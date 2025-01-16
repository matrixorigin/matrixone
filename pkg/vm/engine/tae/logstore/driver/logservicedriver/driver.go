// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logservicedriver

import (
	"context"
	"time"

	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
)

const (
	ReplayReadSize = mpool.MB * 64
	MaxReadSize    = mpool.MB * 64
)

func RetryWithTimeout(timeoutDuration time.Duration, fn func() (shouldReturn bool)) error {
	ctx, cancel := context.WithTimeoutCause(
		context.Background(),
		timeoutDuration,
		moerr.CauseRetryWithTimeout,
	)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return moerr.AttachCause(ctx, ErrRetryTimeOut)
		default:
			if fn() {
				return nil
			}
		}
	}
}

type LogServiceDriver struct {
	*driverInfo
	clientPool      *clientpool
	config          *Config
	currentAppender *driverAppender

	closeCtx        context.Context
	closeCancel     context.CancelFunc
	doAppendLoop    sm.Queue
	appendWaitQueue chan any
	waitAppendLoop  *sm.Loop
	postAppendQueue chan any
	postAppendLoop  *sm.Loop

	appendPool *ants.Pool

	truncateQueue sm.Queue
}

func NewLogServiceDriver(cfg *Config) *LogServiceDriver {
	clientpoolConfig := &clientConfig{
		cancelDuration:        cfg.NewClientDuration,
		recordSize:            cfg.RecordSize,
		clientFactory:         cfg.ClientFactory,
		GetClientRetryTimeOut: cfg.GetClientRetryTimeOut,
		retryDuration:         cfg.RetryTimeout,
	}

	// the tasks submitted to LogServiceDriver.appendPool append entries to logservice,
	// and we hope the task will crash all the tn service if append failed.
	// so, set panic to pool.options.PanicHandler here, or it will only crash
	// the goroutine the append task belongs to.
	pool, _ := ants.NewPool(cfg.ClientMaxCount, ants.WithPanicHandler(func(v interface{}) {
		panic(v)
	}))

	d := &LogServiceDriver{
		clientPool:      newClientPool(cfg.ClientMaxCount, clientpoolConfig),
		config:          cfg,
		currentAppender: newDriverAppender(),
		driverInfo:      newDriverInfo(),
		appendWaitQueue: make(chan any, 10000),
		postAppendQueue: make(chan any, 10000),
		appendPool:      pool,
	}
	d.closeCtx, d.closeCancel = context.WithCancel(context.Background())
	d.doAppendLoop = sm.NewSafeQueue(10000, 10000, d.onAppendRequests)
	d.doAppendLoop.Start()
	d.waitAppendLoop = sm.NewLoop(d.appendWaitQueue, d.postAppendQueue, d.onWaitAppendRequests, 10000)
	d.waitAppendLoop.Start()
	d.postAppendLoop = sm.NewLoop(d.postAppendQueue, nil, d.onAppendDone, 10000)
	d.postAppendLoop.Start()
	d.truncateQueue = sm.NewSafeQueue(10000, 10000, d.onTruncateRequests)
	d.truncateQueue.Start()
	return d
}

func (d *LogServiceDriver) GetMaxClient() int {
	return d.config.ClientMaxCount
}

func (d *LogServiceDriver) Close() error {
	d.clientPool.Close()
	d.closeCancel()
	d.doAppendLoop.Stop()
	d.waitAppendLoop.Stop()
	d.postAppendLoop.Stop()
	d.truncateQueue.Stop()
	close(d.appendWaitQueue)
	close(d.postAppendQueue)
	d.appendPool.Release()
	return nil
}

func (d *LogServiceDriver) Replay(
	ctx context.Context,
	h driver.ApplyHandle,
) (err error) {
	replayer := newReplayer(
		h,
		d,
		ReplayReadSize,
		// driver is mangaging the psn to dsn mapping
		// here the replayer is responsible to provide the all the existing psn to dsn
		// info to the driver
		// a driver is a statemachine.
		// INITed -> REPLAYING -> REPLAYED
		// a driver can be readonly or readwrite
		// for readonly driver, it is always in the REPLAYING state
		// for readwrite driver, it can only serve the write request
		// after the REPLAYED state
		WithReplayerOnScheduled(
			func(psn uint64, dsnRange *common.ClosedIntervals, _ *recordEntry) {
				d.recordPSNInfo(psn, dsnRange)
			},
		),
		WithReplayerOnReplayDone(
			func(replayErr error, dsnStats DSNStats) {
				if replayErr != nil {
					return
				}
				d.resetDSNStats(&dsnStats)
			},
		),
	)

	err = replayer.Replay(ctx)
	return
}

func (d *LogServiceDriver) readFromBackend(
	ctx context.Context, firstPSN uint64, maxSize int,
) (
	nextPSN uint64,
	records []logservice.LogRecord,
	err error,
) {
	var (
		t0         = time.Now()
		cancel     context.CancelFunc
		maxRetry   = 10
		retryTimes = 0
	)

	defer func() {
		if err == nil || retryTimes == 0 {
			return
		}
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		}
		logger(
			"Wal-Read-Backend",
			zap.Uint64("from-psn", firstPSN),
			zap.Int("max-size", maxSize),
			zap.Int("num-records", len(records)),
			zap.Uint64("next-psn", nextPSN),
			zap.Duration("duration", time.Since(t0)),
			zap.Error(err),
		)
	}()

	var client *wrappedClient
	if client, err = d.clientPool.Get(); err != nil {
		return
	}
	defer d.clientPool.Put(client)

	for ; retryTimes < maxRetry; retryTimes++ {
		ctx, cancel = context.WithTimeoutCause(
			ctx,
			d.config.ReadDuration,
			moerr.CauseReadFromLogService,
		)
		if records, nextPSN, err = client.c.Read(
			ctx, firstPSN, uint64(maxSize),
		); err != nil {
			err = moerr.AttachCause(ctx, err)
		}
		cancel()

		if err == nil {
			break
		}
	}

	return
}
