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
	"github.com/matrixorigin/matrixone/pkg/logutil"
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
	readCache
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

	flushtimes  int
	appendtimes int

	// PXU TODO: remove me
	readDuration time.Duration
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
		readCache:       newReadCache(),
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
	logutil.Infof("append%d,flush%d", d.appendtimes, d.flushtimes)
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
	d.PreReplay()
	replayer := newReplayer(h, d, ReplayReadSize)

	defer func() {
		d.resetReadCache()
		d.PostReplay()
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		}
		logger(
			"Wal-Replay-Driver-End",
			zap.Duration("read-cost", d.readDuration),
			zap.Duration("apply-cost", replayer.applyDuration),
			zap.Int("read-count", replayer.readCount),
			zap.Int("internal-count", replayer.internalCount),
			zap.Int("apply-count", replayer.applyCount),
			zap.Error(err),
		)
	}()

	if err = replayer.replay(ctx); err != nil {
		return
	}

	d.onReplay(replayer)

	return
}
