// Copyright 2023 Matrix Origin
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

package lockservice

import (
	"context"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"go.uber.org/zap"
)

var (
	defaultLazyCheckDuration atomic.Value
	waitTooLong              = time.Minute
)

func init() {
	defaultLazyCheckDuration.Store(time.Second * 5)
}

type lockContext struct {
	ctx              context.Context
	txn              *activeTxn
	waitTxn          pb.WaitTxn
	rows             [][]byte
	opts             LockOptions
	offset           int
	idx              int
	lockedTS         timestamp.Timestamp
	result           pb.Result
	cb               func(pb.Result, error)
	lockFunc         func(*lockContext, bool)
	w                *waiter
	createAt         time.Time
	closed           bool
	rangeLastWaitKey []byte
	lockWaitDeadline time.Time
}

func (l *localLockTable) newLockContext(
	ctx context.Context,
	txn *activeTxn,
	rows [][]byte,
	opts LockOptions,
	cb func(pb.Result, error),
	bind pb.LockTable) *lockContext {
	c := reuse.Alloc[lockContext](nil)
	c.ctx = ctx
	c.txn = txn
	c.rows = rows
	c.waitTxn = txn.toWaitTxn(l.bind.ServiceID, true)
	c.opts = opts
	c.cb = cb
	c.result = pb.Result{LockedOn: bind}
	c.createAt = time.Now()
	if opts.async && opts.LockWaitTimeout > 0 {
		c.lockWaitDeadline = c.createAt.Add(time.Duration(opts.LockWaitTimeout) * time.Second)
	}
	return c
}

func (c lockContext) TypeName() string {
	return "lockservice.lockContext"
}

func (c *lockContext) done(err error) {
	c.cb(c.result, err)
	c.release()
}

func (c *lockContext) release() {
	reuse.Free(c, nil)
}

func (c *lockContext) doLock() {
	if c.lockFunc == nil {
		panic("missing lock")
	}
	c.lockFunc(c, true)
}

func (c *lockContext) getLockWaitTimeout() time.Duration {
	if c.lockWaitDeadline.IsZero() {
		return time.Duration(c.opts.LockWaitTimeout) * time.Second
	}
	return time.Until(c.lockWaitDeadline)
}

type event struct {
	c      *lockContext
	eventC chan *lockContext
}

func (e event) notified() {
	if e.eventC != nil {
		e.eventC <- e.c
	}
}

// waiterEvents is used to handle all notified waiters. And use a pool to retry the lock op,
// to avoid too many goroutine blocked.
type waiterEvents struct {
	logger            *log.MOLogger
	workers           int
	detector          *detector
	eventC            chan *lockContext
	checkOrphanC      chan checkOrphan
	txnHolder         activeTxnHolder
	remoteLockTimeout time.Duration
	unlock            func(ctx context.Context, txnID []byte, commitTS timestamp.Timestamp, mutations ...pb.ExtraMutation) error
	stopper           *stopper.Stopper

	// checkC wakes the handle loop to run check() at a waiter's
	// lockWaitTimeout boundary. checkPending coalesces concurrent timer
	// callbacks so at least one prompt check runs without queueing one
	// signal per waiter.
	checkC       chan struct{}
	checkPending atomic.Bool

	mu struct {
		sync.RWMutex
		blockedWaiters []*waiter
	}
}

func newWaiterEvents(
	workers int,
	detector *detector,
	txnHolder activeTxnHolder,
	remoteLockTimeout time.Duration,
	unlock func(ctx context.Context, txnID []byte, commitTS timestamp.Timestamp, mutations ...pb.ExtraMutation) error,
	logger *log.MOLogger,
) *waiterEvents {
	return &waiterEvents{
		logger:            logger,
		workers:           workers,
		detector:          detector,
		txnHolder:         txnHolder,
		remoteLockTimeout: remoteLockTimeout,
		unlock:            unlock,
		eventC:            make(chan *lockContext, 10000),
		checkOrphanC:      make(chan checkOrphan, 64),
		checkC:            make(chan struct{}, 1),
		stopper:           stopper.NewStopper("waiter-events", stopper.WithLogger(logger.RawLogger())),
	}
}

func (mw *waiterEvents) start() {
	for i := 0; i < mw.workers; i++ {
		if err := mw.stopper.RunTask(mw.handle); err != nil {
			panic(err)
		}
	}
}

func (mw *waiterEvents) close() {
	mw.stopper.Stop()
	close(mw.eventC)
	mw.mu.Lock()
	for _, w := range mw.mu.blockedWaiters {
		w.close("waiterEvents close", mw.logger)
	}
	mw.mu.Unlock()
}

func (mw *waiterEvents) add(c *lockContext) {
	if c.opts.async {
		c.w.event = event{
			eventC: mw.eventC,
			c:      c,
		}
	}
	// Propagate the remaining session-level lock_wait_timeout to the waiter so
	// the check loop enforces one budget across async re-queue cycles. The sync
	// path enforces LockWaitTimeout via context.WithTimeoutCause in doLock.
	c.w.lockWaitTimeout = c.getLockWaitTimeout()
	if c.w.lockWaitTimeout <= 0 && !c.lockWaitDeadline.IsZero() {
		c.w.lockWaitTimeout = time.Nanosecond
	}
	c.w.startWait()
	mw.addToLazyCheckDeadlockC(c.w)

	// Schedule a precise timer only for timeouts at or below the lazy-check
	// interval. Larger timeouts can rely on the periodic check and avoid
	// long-lived timers.
	c.w.stopLockWaitTimer()
	if c.w.lockWaitTimeout > 0 && c.w.lockWaitTimeout <= defaultLazyCheckDuration.Load().(time.Duration) {
		d := c.w.lockWaitTimeout
		c.w.lockWaitTimer.Store(time.AfterFunc(d, mw.wakeCheck))
	}
}

func (mw *waiterEvents) addToLazyCheckDeadlockC(w *waiter) {
	w.ref("addToLazyCheckDeadlockC", mw.logger)
	mw.mu.Lock()
	defer mw.mu.Unlock()
	mw.mu.blockedWaiters = append(mw.mu.blockedWaiters, w)
}

func (mw *waiterEvents) wakeCheck() {
	if !mw.checkPending.CompareAndSwap(false, true) {
		return
	}
	select {
	case mw.checkC <- struct{}{}:
	default:
		// A wake-up is already queued; keep checkPending set so later timers
		// coalesce until the handle loop consumes the signal and runs check().
	}
}

func (mw *waiterEvents) handle(ctx context.Context) {
	timeout := defaultLazyCheckDuration.Load().(time.Duration)
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case c := <-mw.eventC:
			txn := c.txn
			txn.Lock()
			c.doLock()
			txn.Unlock()
		case v := <-mw.checkOrphanC:
			mw.checkOrphan(v)
		case <-mw.checkC:
			mw.checkPending.Store(false)
			// Precise timer fired for at least one waiter's lockWaitTimeout.
			// Run check() immediately instead of waiting for the
			// next coarse lazy-check tick.
			mw.check(timeout)
		case <-timer.C:
			mw.check(timeout)
			timer.Reset(timeout)
		}
	}
}

func (mw *waiterEvents) check(timeout time.Duration) {
	mw.mu.Lock()
	defer mw.mu.Unlock()
	if len(mw.mu.blockedWaiters) == 0 {
		return
	}

	now := time.Now()
	newBlockedWaiters := mw.mu.blockedWaiters[:0]
	for i, w := range mw.mu.blockedWaiters {
		// remove if not in blocking state
		if w.getStatus() != blocking {
			w.close("waiterEvents check", mw.logger)
			mw.mu.blockedWaiters[i] = nil
			continue
		}

		wait := now.Sub(w.waitAt.Load().(time.Time))
		mw.addToOrphanCheck(w, wait)

		// enforce session-level lock_wait_timeout on the async (remote) path.
		// The sync path enforces this via context.WithTimeoutCause in doLock;
		// this gives the async path equivalent timeout enforcement.
		if w.lockWaitTimeout > 0 && wait >= w.lockWaitTimeout {
			mw.logger.Debug("lock wait timeout elapsed, notifying waiter",
				zap.String("txn", w.String()),
				zap.Duration("wait", wait),
				zap.Duration("timeout", w.lockWaitTimeout))
			w.notify(notifyValue{err: ErrLockTimeout}, mw.logger)
			w.close("waiterEvents check timeout", mw.logger)
			mw.mu.blockedWaiters[i] = nil
			continue
		}

		if wait >= timeout {
			mw.addToDeadlockCheck(w)
		}
		newBlockedWaiters = append(newBlockedWaiters, w)
	}
	mw.mu.blockedWaiters = newBlockedWaiters
}

func (mw *waiterEvents) addToDeadlockCheck(w *waiter) error {
	for _, holder := range w.waitFor {
		if err := mw.detector.check(holder, w.txn); err != nil {
			return err
		}
	}
	return nil
}

func (mw *waiterEvents) checkOrphan(v checkOrphan) {
	if mw.txnHolder == nil {
		return
	}

	if v.wait >= waitTooLong {
		lockDetail := ""
		v.lt.mu.RLock()
		lock, ok := v.lt.mu.store.Get(v.key)
		if ok {
			lockDetail = lock.String()
		}
		mw.logger.Warn("wait too long",
			zap.Duration("wait", v.wait),
			zap.String("key", hex.EncodeToString(v.key)),
			zap.String("bind", v.lt.bind.DebugString()),
			zap.String("lock", lockDetail),
			zap.String("txn", hex.EncodeToString(v.txn.TxnID)))
		v.lt.mu.RUnlock()
	}

	holders := func() []pb.WaitTxn {
		var holders []pb.WaitTxn
		v.lt.mu.RLock()
		defer v.lt.mu.RUnlock()

		lock, ok := v.lt.mu.store.Get(v.key)
		if !ok {
			return nil
		}

		for _, v := range lock.holders.txns {
			holders = append(holders, v)
		}
		return holders
	}()
	if len(holders) == 0 {
		return
	}

	for _, h := range holders {
		if !mw.txnHolder.hasRemoteLockBind(h.CreatedOn, v.lt.bind, mw.remoteLockTimeout) {
			if !mw.txnHolder.canUnlockRemoteTxn(h) {
				mw.logger.Warn("found stale remote lock without bind heartbeat, but txn may still commit",
					zap.String("bind", v.lt.bind.DebugString()),
					bytesArrayField("txns", [][]byte{h.TxnID}))
				continue
			}
			mw.logger.Warn("found stale remote lock without bind heartbeat",
				zap.String("bind", v.lt.bind.DebugString()),
				bytesArrayField("txns", [][]byte{h.TxnID}))
			_ = mw.unlock(context.Background(), h.TxnID, timestamp.Timestamp{})
			continue
		}
		// When you have determined that a remote transaction is an orphaned transaction, you
		// can release the lock that the remote transaction has placed on the current cn.
		if !mw.txnHolder.isValidRemoteTxn(h) {
			mw.logger.Warn("found orphans txns",
				bytesArrayField("txns", [][]byte{h.TxnID}))
			// ignore error. If failed will retry until lock removed
			_ = mw.unlock(context.Background(), h.TxnID, timestamp.Timestamp{})
		}
	}
}

func (mw *waiterEvents) addToOrphanCheck(
	w *waiter,
	wait time.Duration,
) {
	ck := *w.conflictKey.Load()
	v := checkOrphan{
		wait: wait,
		key:  ck,
		lt:   w.lt.Load(),
		txn:  w.txn,
	}

	select {
	case mw.checkOrphanC <- v:
	default:
	}
}

type checkOrphan struct {
	wait time.Duration
	key  []byte
	lt   *localLockTable
	txn  pb.WaitTxn
}
