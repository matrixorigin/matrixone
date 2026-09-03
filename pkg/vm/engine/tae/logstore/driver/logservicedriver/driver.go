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
	"errors"
	"sync"
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
	MaxReadBatchSize          = mpool.MB * 64
	defaultDriverCloseTimeout = 4 * time.Minute
)

type replayState struct {
	mode   driver.ReplayMode
	done   bool
	err    error
	waitCh chan struct{}
}

func newReplayState(
	done bool,
	err error,
	mode driver.ReplayMode,
	waitCh chan struct{},
) *replayState {
	if waitCh == nil {
		waitCh = make(chan struct{}, 1)
	}
	return &replayState{
		mode:   mode,
		done:   done,
		waitCh: waitCh,
	}
}

func (s *replayState) Done(err error) {
	s.err = err
	close(s.waitCh)
}

func (s *replayState) WaitC() <-chan struct{} {
	return s.waitCh
}

func (s *replayState) Err() error {
	return s.err
}

type LogServiceDriver struct {
	*sequenceNumberState

	replayState atomic.Pointer[replayState]

	config Config

	clientPool *clientPool
	committer  *groupCommitter

	commitLoop      sm.Queue
	commitWaitQueue chan any
	pendingWait     atomic.Int64
	pendingMu       sync.Mutex
	pendingZero     chan struct{}
	waitCommitLoop  *sm.Loop
	postCommitQueue chan any
	truncateQueue   sm.Queue

	ctx    context.Context
	cancel context.CancelFunc

	workers   *ants.Pool
	closeOnce sync.Once
	closeErr  error

	closeTimeout    time.Duration
	onAppendFailure func(error)
}

func NewLogServiceDriver(cfg *Config) *LogServiceDriver {
	start := time.Now()
	// the tasks submitted to LogServiceDriver.workers append entries to logservice,
	// and we hope the task will crash all the tn service if append failed.
	// so, set panic to pool.options.PanicHandler here, or it will only crash
	// the goroutine the append task belongs to.
	pool, _ := ants.NewPool(cfg.ClientMaxCount,
		ants.WithNonblocking(true),
		ants.WithPanicHandler(func(v interface{}) {
			panic(v)
		}))

	d := &LogServiceDriver{
		clientPool:          newClientPool(cfg),
		committer:           getCommitter(),
		sequenceNumberState: newSequenceNumberState(),
		commitWaitQueue:     make(chan any, 10000),
		postCommitQueue:     make(chan any, 10000),
		workers:             pool,
		closeTimeout:        defaultDriverCloseTimeout,
		onAppendFailure: func(err error) {
			logutil.Fatal(
				"Wal-Cannot-Append",
				zap.Error(err),
			)
		},
	}
	d.pendingZero = make(chan struct{})
	close(d.pendingZero)

	d.config = *cfg
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
		zap.Duration("duration", time.Since(start)),
	)
	return d
}

func (d *LogServiceDriver) GetMaxClient() int {
	return d.config.ClientMaxCount
}

func (d *LogServiceDriver) Close() error {
	d.closeOnce.Do(func() {
		// The deadline covers intake, worker submission and waiter delivery.  In
		// particular, it must begin before SafeQueue.Stop: a callback may be
		// waiting to submit to a saturated worker pool.
		closeTimeout := d.closeTimeout
		if closeTimeout <= 0 {
			closeTimeout = defaultDriverCloseTimeout
		}
		deadline := time.Now().Add(closeTimeout)
		// Stop intake first. safeQueue.Stop waits for the callback currently
		// assembling committers, so no new waiter can be added below.
		stopped := make(chan struct{})
		go func() {
			d.commitLoop.Stop()
			close(stopped)
		}()
		stopTimer := time.NewTimer(time.Until(deadline))
		select {
		case <-stopped:
			if !stopTimer.Stop() {
				select {
				case <-stopTimer.C:
				default:
				}
			}
		case <-stopTimer.C:
			d.closeErr = moerr.NewInternalErrorNoCtx("log service driver commit intake stop timeout")
			return
		}
		// If a worker cannot finish, its committer still owns the durability
		// outcome; do not close its client or synchronously wait past the
		// deadline. The caller must fail-stop and let recovery resolve the
		// unknown commit.
		remaining := time.Until(deadline)
		if remaining <= 0 {
			d.closeErr = moerr.NewInternalErrorNoCtx("log service driver close deadline expired")
			return
		}
		if err := d.workers.ReleaseTimeout(remaining); err != nil {
			d.closeErr = errors.Join(d.closeErr, err)
			return
		}

		// Keep the wait loop alive until every accepted committer has delivered
		// its terminal notification. This preserves the accepted-entry ownership
		// contract during a normal driver shutdown.
		d.pendingMu.Lock()
		pendingZero := d.pendingZero
		d.pendingMu.Unlock()
		waitTimer := time.NewTimer(time.Until(deadline))
		select {
		case <-pendingZero:
			if !waitTimer.Stop() {
				select {
				case <-waitTimer.C:
				default:
				}
			}
		case <-waitTimer.C:
			d.closeErr = moerr.NewInternalErrorNoCtx("log service driver commit wait drain timeout")
			return
		}
		if d.pendingWait.Load() != 0 {
			d.closeErr = moerr.NewInternalErrorNoCtx("log service driver commit wait drain timeout")
			return
		}
		d.waitCommitLoop.Stop()
		d.truncateQueue.Stop()
		d.clientPool.Close()
		d.cancel()
		close(d.commitWaitQueue)
		close(d.postCommitQueue)
	})
	return d.closeErr
}

func (d *LogServiceDriver) addPendingWait() {
	d.pendingMu.Lock()
	if d.pendingWait.Load() == 0 {
		d.pendingZero = make(chan struct{})
	}
	d.pendingWait.Add(1)
	d.pendingMu.Unlock()
}

func (d *LogServiceDriver) donePendingWait() {
	d.pendingMu.Lock()
	remaining := d.pendingWait.Add(-1)
	if remaining == 0 {
		close(d.pendingZero)
	}
	d.pendingMu.Unlock()
}

func (d *LogServiceDriver) Replay(
	ctx context.Context,
	h driver.ApplyHandle,
	modeGetter func() driver.ReplayMode,
	replayOpt *driver.ReplayOption,
) (err error) {
	mode := modeGetter()

	oldState := d.replayState.Load()
	if oldState != nil && oldState.done {
		return moerr.NewInternalErrorNoCtx("replay already done")
	}

	replayState := newReplayState(false, nil, mode, nil)
	if !d.replayState.CompareAndSwap(oldState, replayState) {
		return moerr.NewInternalErrorNoCtx("concurrent replay")
	}

	defer func() {
		doneState := newReplayState(true, err, mode, replayState.waitCh)
		d.replayState.Store(doneState)
		doneState.Done(err)
	}()

	onLogRecord := func(r logservice.LogRecord) {
		// TODO: handle the config change log record
	}

	onWaitMore := func() bool {
		mode = modeGetter()
		return mode == driver.ReplayMode_ReplayForever
	}

	var opts []ReplayOption
	opts = append(opts, WithReplayerOnTruncate(
		d.commitTruncateInfo,
	))
	opts = append(opts, WithReplayerNeedWriteSkip(
		func() bool {
			mode := modeGetter()
			return mode == driver.ReplayMode_ReplayForWrite
		},
	))
	opts = append(opts, WithReplayerWaitMore(onWaitMore))
	opts = append(opts, WithReplayerOnLogRecord(onLogRecord))
	// driver is mangaging the psn to dsn mapping
	// here the replayer is responsible to provide the all the existing psn to dsn
	// info to the driver
	// a driver is a statemachine.
	// INITed -> REPLAYING -> REPLAYED
	// a driver can be readonly or readwrite
	// for readonly driver, it is always in the REPLAYING state
	// for readwrite driver, it can only serve the write request
	// after the REPLAYED state
	opts = append(opts, WithReplayerOnScheduled(
		func(psn uint64, e LogEntry) {
			d.recordSequenceNumbers(psn, e.DSNRange())
		},
	))
	opts = append(opts, WithReplayerOnReplayDone(
		func(replayErr error, dsnStats DSNStats) {
			if replayErr != nil {
				return
			}
			d.initState(&dsnStats)
		},
	))
	if replayOpt != nil {
		opts = append(opts, WithReplayerPollTruncateInterval(replayOpt.PollTruncateInterval))
	}

	replayer := newReplayer(
		h,
		d,
		MaxReadBatchSize,
		opts...,
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

	for {
		ctx, cancel := context.WithTimeoutCause(
			ctx,
			DefaultOneTryTimeout,
			moerr.CauseReadFromLogService,
		)

		if records, nextPSN, err = client.wrapped.Read(
			ctx, firstPSN, uint64(maxSize),
		); err == nil {
			cancel()
			break
		}
		cancel()
		retryTimes++
		if time.Since(t0) > d.config.MaxTimeout {
			break
		}
		time.Sleep(d.config.RetryInterval(retryTimes))
	}

	return
}

func (d *LogServiceDriver) ReplayWaitC() <-chan struct{} {
	state := d.replayState.Load()
	if state == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return state.WaitC()
}

func (d *LogServiceDriver) GetReplayState() *replayState {
	return d.replayState.Load()
}

func (d *LogServiceDriver) GetCfg() *Config {
	return &d.config
}

func (d *LogServiceDriver) canWrite() bool {
	replayState := d.replayState.Load()
	return replayState != nil && replayState.done && replayState.mode == driver.ReplayMode_ReplayForWrite
}
