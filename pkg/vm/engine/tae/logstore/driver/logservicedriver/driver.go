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
	"sync/atomic"
	"time"

	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
)

const (
	ReplayReadSize = mpool.MB * 64
	MaxReadSize    = mpool.MB * 64
)

type LogServiceDriver struct {
	*sequenceNumberState

	config Config

	clientPool *clientPool
	committer  *groupCommitter

	commitLoop      sm.Queue
	commitWaitQueue chan any
	waitCommitLoop  *sm.Loop
	postCommitQueue chan any
	truncateQueue   sm.Queue

	ctx    context.Context
	cancel context.CancelFunc

	workers *ants.Pool
}

func NewLogServiceDriver(cfg *Config) *LogServiceDriver {
	// the tasks submitted to LogServiceDriver.workers append entries to logservice,
	// and we hope the task will crash all the tn service if append failed.
	// so, set panic to pool.options.PanicHandler here, or it will only crash
	// the goroutine the append task belongs to.
	pool, _ := ants.NewPool(cfg.ClientMaxCount, ants.WithPanicHandler(func(v interface{}) {
		panic(v)
	}))

	d := &LogServiceDriver{
		clientPool:          newClientPool(cfg),
		config:              *cfg,
		committer:           getCommitter(),
		sequenceNumberState: newSequenceNumberState(),
		commitWaitQueue:     make(chan any, 10000),
		postCommitQueue:     make(chan any, 10000),
		workers:             pool,
	}
	d.ctx, d.cancel = context.WithCancel(context.Background())
	d.commitLoop = sm.NewSafeQueue(10000, 10000, d.onCommitIntents)
	d.commitLoop.Start()
	d.waitCommitLoop = sm.NewLoop(d.commitWaitQueue, nil, d.onWaitCommitted, 10000)
	d.waitCommitLoop.Start()
	d.truncateQueue = sm.NewSafeQueue(10000, 10000, d.onTruncateRequests)
	d.truncateQueue.Start()
	logutil.Info(
		"Wal-Driver-Start",
		zap.String("config", cfg.String()),
	)
	return d
}

func (d *LogServiceDriver) GetMaxClient() int {
	return d.config.ClientMaxCount
}

func (d *LogServiceDriver) Close() error {
	d.clientPool.Close()
	d.cancel()
	d.commitLoop.Stop()
	d.waitCommitLoop.Stop()
	d.truncateQueue.Stop()
	close(d.commitWaitQueue)
	close(d.postCommitQueue)
	d.workers.Release()
	return nil
}

func (d *LogServiceDriver) Replay(
	ctx context.Context,
	h driver.ApplyHandle,
	mode driver.ReplayMode,
) (err error) {

	var replayMode atomic.Int32
	replayMode.Store(int32(mode))

	onLogRecord := func(r logservice.LogRecord) {
		// TODO: handle the config change log record
	}

	onWaitMore := func() bool {
		return replayMode.Load() == int32(driver.ReplayMode_ReplayForever)
	}

	replayer := newReplayer(
		h,
		d,
		ReplayReadSize,
		WithReplayerWaitMore(
			onWaitMore,
		),
		WithReplayerOnLogRecord(
			onLogRecord,
		),

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
			func(psn uint64, e LogEntry) {
				d.recordSequenceNumbers(psn, e.DSNRange())
			},
		),
		WithReplayerOnReplayDone(
			func(replayErr error, dsnStats DSNStats) {
				if replayErr != nil {
					return
				}
				d.initState(&dsnStats)
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
	defer client.Putback()

	for ; retryTimes < d.config.MaxRetryCount; retryTimes++ {
		ctx, cancel = context.WithTimeoutCause(
			ctx,
			d.config.MaxTimeout,
			moerr.CauseReadFromLogService,
		)
		if records, nextPSN, err = client.wrapped.Read(
			ctx, firstPSN, uint64(maxSize),
		); err != nil {
			err = moerr.AttachCause(ctx, err)
		}
		cancel()

		if err == nil {
			break
		}
		time.Sleep(d.config.RetryInterval() * time.Duration(retryTimes+1))
	}

	return
}
