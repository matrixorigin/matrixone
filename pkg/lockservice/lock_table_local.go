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
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"go.uber.org/zap"
)

const (
	eventsWorkers = 4
)

// a localLockTable instance manages the locks on a table
type localLockTable struct {
	bind      pb.LockTable
	fsp       *fixedSlicePool
	clock     clock.Clock
	events    *waiterEvents
	txnHolder activeTxnHolder
	logger    *log.MOLogger

	mu struct {
		sync.RWMutex
		closed           bool
		store            LockStorage
		tableCommittedAt timestamp.Timestamp
		ownerLocalWaits  map[ownerLocalTxnKey][]ownerLocalWaitEdge
	}

	options struct {
		beforeCloseFirstWaiter func(c *lockContext)
		beforeAcquire          func(c *lockContext)
		beforeWait             func(c *lockContext) func()
		afterWait              func(c *lockContext) func()
	}
}

func newLocalLockTable(
	bind pb.LockTable,
	fsp *fixedSlicePool,
	events *waiterEvents,
	clock clock.Clock,
	txnHolder activeTxnHolder,
	logger *log.MOLogger,
) lockTable {
	l := &localLockTable{
		bind:      bind,
		fsp:       fsp,
		clock:     clock,
		events:    events,
		txnHolder: txnHolder,
		logger:    logger,
	}
	l.mu.store = newBtreeBasedStorage()
	l.mu.ownerLocalWaits = make(map[ownerLocalTxnKey][]ownerLocalWaitEdge)
	l.mu.tableCommittedAt, _ = clock.Now()
	return l
}

func (l *localLockTable) lock(
	ctx context.Context,
	txn *activeTxn,
	rows [][]byte,
	opts LockOptions,
	cb func(pb.Result, error)) {
	v2.TxnLocalLockTotalCounter.Inc()

	logLocalLock(l.logger, txn, l.bind.Table, rows, opts)
	c := l.newLockContext(ctx, txn, rows, opts, cb, l.bind)
	if opts.async {
		c.lockFunc = l.doLock
	}
	l.doLock(c, false)
}

func (l *localLockTable) doLock(
	c *lockContext,
	blocked bool) {
	var old *waiter
	var oldOffset int
	var err error
	table := l.bind.Table
	// Session-level SET lock_wait_timeout takes highest priority (passed via
	// pb.LockOptions). The budget only counts time actually spent waiting.
	leftTimeout := time.Duration(c.opts.LockWaitTimeout) * time.Second
	for {
		// blocked used for async callback, waiter is created, and added to wait list.
		// So only need wait notify.
		if !blocked {
			err = l.doAcquireLock(c)
			if err != nil {
				logLocalLockFailed(l.logger, c.txn, table, c.rows, c.opts, err)
				w := c.w
				if w == nil {
					w = old
				}
				l.detachFailedWaiter(c, w)
				c.done(err)
				return
			}
			// no waiter, all locks are added
			if c.w == nil {
				v2.TxnAcquireLockWaitDurationHistogram.Observe(time.Since(c.createAt).Seconds())
				if old != nil {
					old.disableNotify()
					old.close("doLock, no waiter, all locks are added in doLock", l.logger)
				}
				c.txn.clearBlocked(old, l.logger)
				logLocalLockAdded(l.logger, c.txn, l.bind.Table, c.rows, c.opts)
				if c.result.Timestamp.IsEmpty() {
					c.result.Timestamp = c.lockedTS
				}
				c.done(nil)
				return
			}

			if oldOffset != c.offset {
				if old != nil {
					old.disableNotify()
					old.close("doLock, lock next row", l.logger)
				}
				c.txn.clearBlocked(old, l.logger)
			}

			// we handle remote lock on current rpc io read goroutine, so we can not wait here, otherwise
			// the rpc will be blocked.
			if c.opts.async {
				return
			}
		}

		// txn is locked by service.lock or service_remote.lock. We must unlock here. And lock again after
		// wait result. Because during wait, deadlock detection may be triggered, and need call txn.fetchWhoWaitingMe,
		// or other concurrent txn method.
		oldOffset = c.offset
		oldTxnID := c.txn.txnID
		old = c.w
		c.txn.Unlock()

		if l.options.beforeWait != nil {
			l.options.beforeWait(c)()
		}

		waitCtx := c.ctx
		var cancel context.CancelFunc
		if !c.lockWaitDeadline.IsZero() {
			waitCtx, cancel = context.WithDeadlineCause(
				c.ctx,
				c.lockWaitDeadline,
				c.getLockWaitTimeoutErr())
		} else if leftTimeout > 0 {
			waitCtx, cancel = context.WithTimeoutCause(c.ctx, leftTimeout, ErrLockTimeout)
		}
		waitStart := time.Now()
		v := c.w.wait(waitCtx, l.logger)
		l.events.removeBlockedWaiter(c.w)
		lockWaitTimeoutHit := cancel != nil &&
			errors.Is(v.err, context.DeadlineExceeded) &&
			context.Cause(waitCtx) == c.getLockWaitTimeoutErr()
		if cancel != nil {
			cancel()
		}

		if l.options.afterWait != nil {
			l.options.afterWait(c)()
		}

		// Update the remaining lock_wait_timeout budget using only wait time.
		if c.lockWaitDeadline.IsZero() && leftTimeout > 0 {
			waited := time.Since(waitStart)
			if waited < leftTimeout {
				leftTimeout -= waited
			} else {
				leftTimeout = 0
			}
			if lockWaitTimeoutHit {
				// lock_wait_timeout expired: return ErrLockTimeout directly
				// (not errors.Join) so upper layers can recognize it via
				// moerr.IsMoErrCode(err, moerr.ErrLockWaitTimeout).
				v.err = c.getLockWaitTimeoutErr()
			}
		} else if lockWaitTimeoutHit {
			v.err = c.getLockWaitTimeoutErr()
		}

		c.txn.Lock()

		logLocalLockWaitOnResult(l.logger, c.txn, table, c.rows[c.idx], c.opts, c.w, v)

		// txn closed between Unlock and get Lock again
		e := v.err
		if e == nil && (!bytes.Equal(oldTxnID, c.txn.txnID) ||
			!bytes.Equal(c.w.txn.TxnID, oldTxnID)) {
			e = ErrTxnNotFound
		}
		if e != nil ||
			c.txn.deadlockFound {
			c.closed = true
			if e != ErrTxnNotFound {
				c.txn.closeBlockWaiters(l.logger)
			}

			l.detachFailedWaiter(c, c.w)
			c.done(e)
			return
		}

		if c.opts.RetryWait > 0 {
			time.Sleep(time.Duration(c.opts.RetryWait))
		}

		c.w.resetWait(l.logger)
		c.offset = c.idx
		c.result.Timestamp = v.ts
		c.result.HasConflict = true
		c.result.TableDefChanged = v.defChanged
		if !c.result.HasPrevCommit {
			c.result.HasPrevCommit = !v.ts.IsEmpty()
		}
		if c.opts.TableDefChanged {
			c.opts.TableDefChanged = v.defChanged
		}
		blocked = false
	}
}

// detachFailedWaiter releases every ownership edge created when a lock
// request entered the wait state. It is also used after notification: the
// holder may have gone away, but the promoted waiter remains in the queue
// until acquisition succeeds. A deadline failure at that point must remove the
// queue entry, txn blocked reference, event-checker reference and owner-local
// wait edge as one terminal transition.
//
// The caller holds c.txn's mutex. Container removal is idempotent so the same
// path is safe for sync/async waits and for races where holder unlock already
// detached a range waiter.
func (l *localLockTable) detachFailedWaiter(c *lockContext, w *waiter) {
	if w == nil {
		return
	}

	// Stop any concurrent notification before dropping the independent
	// references held by the checker, transaction and lock queue.
	w.disableNotify()
	l.events.removeBlockedWaiter(w)
	c.txn.clearBlocked(w, l.logger)

	// A waiter is allocated before conflict admission. Fast-fail and deadlock
	// detection can reject it before handleLockConflictLocked stores a key, in
	// which case there is no lock-queue ownership to detach.
	var ck []byte
	if conflictKey := w.conflictKey.Load(); conflictKey != nil {
		ck = *conflictKey
	}
	if len(ck) > 0 {
		// The hook deliberately runs outside l.mu. Some tests use it to
		// acquire another lock and exercise concurrent waiter cleanup.
		if c.opts.Granularity == pb.Granularity_Row &&
			l.options.beforeCloseFirstWaiter != nil {
			l.options.beforeCloseFirstWaiter(c)
		}

		l.mu.Lock()
		switch c.opts.Granularity {
		case pb.Granularity_Row:
			// Reload the conflict lock because it may have been deleted and
			// recreated while this request was waiting.
			conflictWith, ok := l.mu.store.Get(ck)
			if ok {
				removed, empty := conflictWith.removeWaiter(w, l.logger)
				if removed {
					l.removeOwnerLocalWaitEdgeLocked(w)
					if empty {
						l.deleteEmptyLockLocked(ck, conflictWith)
					} else {
						l.removeInactiveOwnerLocalWaitEdgesLocked(conflictWith)
					}
				} else {
					l.removeOwnerLocalWaitEdgeLocked(w)
				}
			} else {
				l.removeOwnerLocalWaitEdgeLocked(w)
			}
		case pb.Granularity_Range:
			l.closeRangeWaiterLocked(c, w, true)
		}
		l.mu.Unlock()
	}

	w.close("doLock, detach failed waiter", l.logger)
	if c.w == w {
		c.w = nil
	}
}

func (l *localLockTable) unlock(
	txn *activeTxn,
	ls *cowSlice,
	commitTS timestamp.Timestamp,
	mutations ...pb.ExtraMutation) {
	start := time.Now()
	defer func() {
		v2.TxnUnlockBtreeTotalDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	getMutation := func(key []byte) int {
		for i := range mutations {
			if bytes.Equal(mutations[i].Key, key) {
				return i
			}
		}
		return -1
	}

	logUnlockTableOnLocal(
		l.logger,
		txn,
		l.bind,
	)

	locks := ls.slice()
	defer locks.unref()

	l.mu.Lock()
	v2.TxnUnlockBtreeGetLockDurationHistogram.Observe(time.Since(start).Seconds())

	defer l.mu.Unlock()
	if l.mu.closed {
		return
	}

	b, ok := txn.getHoldLocksLocked(l.bind.Group).tableBinds[l.bind.Table]
	if !ok {
		panic("BUG: missing bind")
	}

	var startKey []byte
	locks.iter(func(key []byte) bool {
		if lock, ok := l.mu.store.Get(key); ok {
			idx := getMutation(key)
			if idx != -1 && mutations[idx].Skip {
				return true
			}

			if lock.isLockRangeStart() {
				startKey = key
				return true
			}

			if !lock.holders.contains(txn.txnID) {
				// A remote proxy retries ReplaceTo after an RPC response is lost.
				// The first request may already have replaced the holder, so make
				// a stale handoff idempotent instead of treating it as a stale
				// ordinary unlock. A replacement can itself finish before the
				// original proxy holder retries, in which case the conditional
				// handoff is also a safe no-op.
				if idx != -1 {
					return true
				}
				// 1. txn1 hold key1 on bind version 0
				// 2. txn1 commit success
				// 3. dn restart
				// 4. txn2 hold key1 on bind version 1
				// 5. txn1 unlock.
				if b.Changed(l.bind) {
					return true
				}

				l.logger.Fatal("BUG: unlock a lock that is not held by the current txn",
					zap.Bool("row", lock.isLockRow()),
					zap.Int("keys-count", locks.len()),
					zap.String("hold-bind", b.DebugString()),
					zap.String("bind", l.bind.DebugString()),
					waitTxnArrayField("holders", lock.holders.getTxnSlice()),
					txnField(txn))
			}
			if len(startKey) > 0 && !lock.isLockRangeEnd() {
				panic("BUG: missing range end key")
			}

			if idx != -1 && len(mutations[idx].ReplaceTo) > 0 {
				replaceTo := mutations[idx].ReplaceTo
				lock.holders.replace(txn.txnID,
					pb.WaitTxn{TxnID: replaceTo, CreatedOn: txn.remoteService})
				// cannot dead lock here, the replaceTo txn was created on the same cn.
				replaceToTxn := l.txnHolder.getActiveTxn(mutations[idx].ReplaceTo, true, txn.remoteService)
				replaceToTxn.Lock()
				_ = replaceToTxn.lockAdded(l.bind.Group, l.bind, [][]byte{key}, l.logger)
				replaceToTxn.Unlock()
				return true
			}

			lockCanRemoved := lock.closeTxn(
				txn,
				notifyValue{ts: commitTS})
			l.removeInactiveOwnerLocalWaitEdgesLocked(lock)
			logLockUnlocked(l.logger, txn, key, lock)

			if lockCanRemoved {
				v2.TxnHoldLockDurationHistogram.Observe(time.Since(lock.createAt).Seconds())
				l.mu.store.Delete(key)
				if len(startKey) > 0 {
					l.mu.store.Delete(startKey)
					startKey = nil
				}
				lock.release()
			}
		}
		return true
	})
	if l.mu.tableCommittedAt.Less(commitTS) {
		l.mu.tableCommittedAt = commitTS
	}
}

func (l *localLockTable) getLock(
	key []byte,
	txn pb.WaitTxn,
	fn func(Lock)) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.mu.closed {
		return
	}
	lock, ok := l.mu.store.Get(key)
	if ok {
		fn(lock)
	}
}

func (l *localLockTable) getLockHolder(ctx context.Context, key []byte) (pb.WaitTxn, bool, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.mu.closed {
		return pb.WaitTxn{}, false, nil
	}
	lock, ok := l.mu.store.Get(key)
	if !ok {
		return pb.WaitTxn{}, false, nil
	}
	var holder pb.WaitTxn
	var found bool
	lock.IterHolders(func(v pb.WaitTxn) bool {
		holder = v
		found = true
		return false
	})
	return holder, found, nil
}

func (l *localLockTable) getBind() pb.LockTable {
	return l.bind
}

func (l *localLockTable) close(reason closeReason) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.mu.closed = true

	l.mu.store.Iter(func(key []byte, lock Lock) bool {
		if lock.isLockRow() || lock.isLockRangeEnd() {
			// if there are waiters in the current lock, just notify
			// the head, and the subsequent waiters will be notified
			// by the previous waiter.
			lock.close(notifyValue{err: ErrLockTableNotFound})
		}
		return true
	})
	clear(l.mu.ownerLocalWaits)
	l.mu.store.Clear()
	logLockTableClosed(l.logger, l.bind, false, reason)
}

func (l *localLockTable) doAcquireLock(c *lockContext) error {
	if l.options.beforeAcquire != nil {
		l.options.beforeAcquire(c)
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	// This is the final admission point after service readiness, binding,
	// bindChangeMu, txn mutex, and the local-table mutex. An uncontended lock
	// must not be created after the absolute budget has expired.
	if err := c.checkLockWaitDeadline(); err != nil {
		return err
	}
	if l.mu.closed {
		return moerr.NewInvalidStateNoCtx("local lock table closed")
	}

	switch c.opts.Granularity {
	case pb.Granularity_Row:
		return l.acquireRowLockLocked(c)
	case pb.Granularity_Range:
		if len(c.rows) == 0 ||
			len(c.rows)%2 != 0 {
			panic("invalid range lock")
		}
		return l.acquireRangeLockLocked(c)
	default:
		panic(fmt.Sprintf("not support lock granularity %d", c.opts.Granularity))
	}
}

func (l *localLockTable) acquireRowLockLocked(c *lockContext) error {
	n := len(c.rows)
	for idx := c.offset; idx < n; idx++ {
		if err := c.checkLockWaitDeadline(); err != nil {
			return err
		}
		row := c.rows[idx]

		key, lock, ok := l.mu.store.Seek(row)
		if ok &&
			(bytes.Equal(key, row) ||
				lock.isLockRangeEnd()) {
			hold, newHolder := lock.tryHold(l.logger, c)
			if hold {
				if c.w != nil {
					l.removeOwnerLocalWaitEdgeLocked(c.w)
					c.w = nil
				}
				// only new holder can added lock into txn.
				// newHolder is false means prev op of txn has already added lock into txn
				if newHolder {
					if updated, changed := lock.setMode(c.opts.Mode); changed {
						l.mu.store.Add(key, updated)
						// Range lock is stored as two entries (start + end) with
						// independent Lock.value bytes. When we update the mode on
						// one end we must also update the paired entry so that
						// subsequent isLockModeAllowed checks on either key see the
						// correct mode.
						l.setModePairedRangeLock(key, updated, c.opts.Mode)
					}
					err := c.txn.lockAdded(l.bind.Group, l.bind, [][]byte{key}, l.logger)
					if err != nil {
						return err
					}
					c.result.NewLockAdd = true
				}
				continue
			}

			// need wait for prev txn closed"
			if c.w == nil {
				c.w = acquireWaiter(c.waitTxn, "acquireRowLockLocked", l.logger)
			}

			c.offset = idx
			return l.handleLockConflictLocked(c, key, lock)
		}
		err := l.addRowLockLocked(c, row)
		if err != nil {
			return err
		}

		// lock added, need create new waiter next time
		c.w = nil
	}

	c.offset = 0
	c.lockedTS = l.mu.tableCommittedAt
	return nil
}

func (l *localLockTable) acquireRangeLockLocked(c *lockContext) error {
	n := len(c.rows)
	for i := c.offset; i < n; i += 2 {
		if err := c.checkLockWaitDeadline(); err != nil {
			return err
		}
		start := c.rows[i]
		end := c.rows[i+1]
		if bytes.Compare(start, end) >= 0 {
			panic(fmt.Sprintf("lock error: start[%v] is greater than end[%v]",
				start, end))
		}

		logLocalLockRange(l.logger, c.txn, l.bind.Table, start, end, c.opts.Mode)
		conflict, conflictWith, err := l.addRangeLockLocked(c, start, end)
		if err != nil {
			return err
		}
		if len(conflict) > 0 {
			if c.w == nil {
				c.w = acquireWaiter(c.waitTxn, "acquireRangeLockLocked", l.logger)
			}

			c.offset = i
			return l.handleLockConflictLocked(c, conflict, conflictWith)
		}

		// lock added, need create new waiter next time
		c.w = nil
	}
	c.offset = 0
	c.lockedTS = l.mu.tableCommittedAt
	return nil
}

func (l *localLockTable) addRowLockLocked(
	c *lockContext,
	row []byte) error {
	lock := newRowLock(l.logger, c)

	// new lock added, use last committed ts to update keys last commit ts.
	lock.waiters.resetCommittedAt(l.mu.tableCommittedAt)

	// we must first add the lock to txn to ensure that the
	// lock can be read when the deadlock is detected.
	err := c.txn.lockAdded(l.bind.Group, l.bind, [][]byte{row}, l.logger)
	if err != nil {
		return err
	}
	c.result.NewLockAdd = true
	l.mu.store.Add(row, lock)
	return nil
}

func (l *localLockTable) handleLockConflictLocked(
	c *lockContext,
	key []byte,
	conflictWith Lock,
) error {
	if c.opts.Policy == pb.WaitPolicy_FastFail {
		return ErrLockConflict
	}
	if err := c.checkLockWaitDeadline(); err != nil {
		return err
	}
	if l.detectOwnerLocalDeadlockLocked(c, conflictWith) {
		return ErrDeadLockDetected
	}

	if c.opts.Granularity == pb.Granularity_Range {
		l.closeRangeWaiterLocked(c, c.w, false)
	}

	c.w.conflictKey.Store(&key)
	c.w.lt.Store(l)
	c.w.waitFor = c.w.waitFor[:0]
	for _, txn := range conflictWith.holders.txns {
		c.w.waitFor = append(c.w.waitFor, txn.TxnID)
	}
	c.result.ConflictKey = key
	if len(c.w.waitFor) > 0 {
		c.result.ConflictTxn = c.w.waitFor[0]
	}
	c.result.Waiters = uint32(conflictWith.waiters.size())
	conflictWith.waiters.iter(func(w *waiter) bool {
		c.result.PrevWaiter = w.txn.TxnID
		return true
	})

	conflictWith.addWaiter(l.logger, c.w)
	// Set waiter to blocking before adding to events.mu.blockedWaiters so
	// waiter_events.check() won't remove it (check removes only non-blocking).
	c.txn.setBlocked(c.w, l.logger)
	l.addOwnerLocalWaitEdgeLocked(c, conflictWith)
	l.events.add(c)

	// find conflict, and wait prev txn completed, and a new
	// waiter added, we need to active deadlock check.
	logLocalLockWaitOn(l.logger, c.txn, l.bind.Table, c.w, key, conflictWith)

	c.rangeLastWaitKey = key
	return nil
}

func (l *localLockTable) closeRangeWaiterLocked(
	c *lockContext,
	w *waiter,
	allowMissing bool,
) {
	if len(c.rangeLastWaitKey) == 0 {
		return
	}

	v, ok := l.mu.store.Get(c.rangeLastWaitKey)
	if ok {
		removed, empty := v.removeWaiter(w, l.logger)
		if removed {
			l.removeOwnerLocalWaitEdgeLocked(w)
			l.removeInactiveOwnerLocalWaitEdgesLocked(v)
			if empty {
				l.deleteEmptyLockLocked(c.rangeLastWaitKey, v)
			}
			c.rangeLastWaitKey = nil
			return
		}
	}
	if allowMissing && !ok {
		// Range unlock notifies all waiters and removes both range entries.
		// A notified waiter that then fails final admission therefore has no
		// queue entry left to detach; only its txn/event references remain.
		l.removeOwnerLocalWaitEdgeLocked(w)
		c.rangeLastWaitKey = nil
		return
	}

	l.logger.Error("missing range last wait key when moving waiter to next conflict",
		zap.Uint64("table", l.bind.Table),
		zap.String("txn", c.txn.txnKey),
		zap.Binary("last-wait-key", c.rangeLastWaitKey),
		zap.Bool("last-wait-key-exists", ok))

	type emptyLock struct {
		key  []byte
		lock Lock
	}
	var emptyLocks []emptyLock
	l.mu.store.Iter(func(key []byte, lock Lock) bool {
		removed, empty := lock.removeWaiter(w, l.logger)
		if removed {
			l.removeOwnerLocalWaitEdgeLocked(w)
			l.removeInactiveOwnerLocalWaitEdgesLocked(lock)
		}
		if removed && empty {
			emptyLocks = append(emptyLocks, emptyLock{
				key:  append([]byte(nil), key...),
				lock: lock,
			})
		}
		return true
	})
	for _, empty := range emptyLocks {
		l.deleteEmptyLockLocked(empty.key, empty.lock)
	}
	c.rangeLastWaitKey = nil
}

// deleteEmptyLockLocked removes the store ownership of an empty lock and
// returns its pooled state exactly once. Range endpoints are two Lock values
// backed by the same holders and waiter queue, so both entries must disappear
// before the shared state can be released.
func (l *localLockTable) deleteEmptyLockLocked(key []byte, lock Lock) {
	if !lock.isEmpty() {
		return
	}
	if lock.isLockRow() ||
		(!lock.isLockRangeStart() && !lock.isLockRangeEnd()) {
		// The second form covers stale/legacy singleton entries without a
		// granularity bit. They have no paired store ownership.
		l.mu.store.Delete(key)
		lock.release()
		return
	}

	pairedKey, pairedLock, ok := l.findPairedRangeLock(key, lock)
	if !ok || pairedLock.holders != lock.holders || pairedLock.waiters != lock.waiters {
		// Do not return shared state to the pools while an unmatched endpoint may
		// still reference it. Leaving the corrupt entry visible is safer than a
		// use-after-reuse and makes the invariant failure observable.
		l.logger.Error("missing paired empty range lock during waiter cleanup",
			zap.Uint64("table", l.bind.Table),
			zap.Binary("key", key))
		return
	}
	l.mu.store.Delete(key)
	l.mu.store.Delete(pairedKey)
	lock.release()
}

func (l *localLockTable) addRangeLockLocked(
	c *lockContext,
	start, end []byte) ([]byte, Lock, error) {

	if c.opts.LockOptions.Mode == pb.LockMode_Shared {
		l1, ok1 := l.mu.store.Get(start)
		l2, ok2 := l.mu.store.Get(end)
		if ok1 && ok2 &&
			l1.isShared() && l2.isShared() &&
			l1.isLockRangeStart() && l2.isLockRangeEnd() {
			hold, newHolder := l1.tryHold(l.logger, c)
			if !hold {
				panic("BUG: must get shared lock")
			}
			hold, _ = l2.tryHold(l.logger, c)
			if !hold {
				panic("BUG: must get shared lock")
			}
			if c.w != nil {
				c.w = nil
			}
			if newHolder {
				err := c.txn.lockAdded(l.bind.Group, l.bind, [][]byte{start, end}, l.logger)
				if err != nil {
					return nil, Lock{}, err
				}
				c.result.NewLockAdd = true
			}
			return nil, Lock{}, nil
		}
	}

	wq := newWaiterQueue()
	wq.init(l.logger)
	mc := newMergeContext(wq)
	defer mc.close()

	var err error
	var conflictWith Lock
	var conflictKey []byte
	var prevStartKey []byte
	rangeStartEncountered := false
	// TODO: remove mem allocate.
	upperBounded := nextKey(end, nil)

	for {
		l.mu.store.Range(
			start,
			nil,
			func(key []byte, keyLock Lock) bool {
				// current txn is not holder, maybe conflict
				if !keyLock.holders.contains(c.txn.txnID) {
					if hasConflictWithLock(key, keyLock, end) {
						conflictWith = keyLock
						conflictKey = key
					}
					return false
				}

				if keyLock.holders.size() > 1 {
					err = ErrMergeRangeLockNotSupport
					return false
				}

				// merge current txn locks
				if keyLock.isLockRangeStart() {
					prevStartKey = key
					rangeStartEncountered = true
					return bytes.Compare(key, end) < 0
				}
				if rangeStartEncountered &&
					!keyLock.isLockRangeEnd() {
					panic("BUG, missing range end key")
				}

				start, end = l.mergeRangeLocked(
					start, end,
					prevStartKey,
					key, keyLock,
					mc,
				)
				prevStartKey = nil
				rangeStartEncountered = false
				return bytes.Compare(key, end) < 0
			})
		if err != nil {
			mc.rollback()
			return nil, Lock{}, err
		}

		if len(conflictKey) > 0 {
			hold, newHolder := conflictWith.tryHold(l.logger, c)
			if hold {
				if c.w != nil {
					c.w = nil
				}

				// only new holder can added lock into txn.
				// newHolder is false means prev op of txn has already added lock into txn
				if newHolder {
					if updated, changed := conflictWith.setMode(c.opts.Mode); changed {
						l.mu.store.Add(conflictKey, updated)
						// Range lock is stored as two entries (start + end) with
						// independent Lock.value bytes. Update the paired entry
						// so both ends reflect the correct mode.
						l.setModePairedRangeLock(conflictKey, updated, c.opts.Mode)
					}
					err = c.txn.lockAdded(l.bind.Group, l.bind, [][]byte{conflictKey}, l.logger)
					if err != nil {
						return nil, Lock{}, err
					}
					c.result.NewLockAdd = true
				}
				conflictWith = Lock{}
				conflictKey = nil
				rangeStartEncountered = false
				continue
			}

			mc.rollback()
			return conflictKey, conflictWith, nil
		}

		if rangeStartEncountered {
			key, keyLock, ok := l.mu.store.Seek(upperBounded)
			if !ok {
				panic("BUG, missing range end key")
			}
			start, end = l.mergeRangeLocked(
				start, end,
				prevStartKey,
				key, keyLock,
				mc,
			)
		}
		break
	}

	mc.commit(l.bind, c.txn, l.mu.store, l.logger)
	startLock, endLock := newRangeLock(l.logger, c)

	wq.resetCommittedAt(l.mu.tableCommittedAt)
	startLock.waiters = wq
	endLock.waiters = wq

	// similar to row lock
	err = c.txn.lockAdded(l.bind.Group, l.bind, [][]byte{start, end}, l.logger)
	if err != nil {
		return nil, Lock{}, err
	}
	c.result.NewLockAdd = true

	l.mu.store.Add(start, startLock)
	l.mu.store.Add(end, endLock)

	if n := len(mc.mergedLocks); n > 0 {
		h := c.txn.getHoldLocksLocked(l.bind.Group)
		v, ok := h.tableKeys[l.bind.Table]
		if ok {
			l.logger.Info("range lock merged",
				zap.Uint64("table", l.bind.OriginTable),
				zap.String("txn", c.txn.txnKey),
				zap.Int("merged", n),
				zap.Int("current", v.mustGet().len()),
			)
		}
	}

	return nil, Lock{}, nil
}

func (l *localLockTable) mergeRangeLocked(
	start, end []byte,
	prevStartKey []byte,
	seekKey []byte,
	seekLock Lock,
	mc *mergeContext,
) ([]byte, []byte) {
	// range lock encountered a row lock
	if seekLock.isLockRow() {
		// 5 + [1, 4] => [1, 4] + [5]
		if bytes.Compare(seekKey, end) > 0 {
			return start, end
		}

		// [1~4] + [1, 4] => [1, 4]
		mc.mergeLocks([][]byte{seekKey})
		mc.mergeWaiter(seekLock.waiters)
		return start, end
	}

	if len(prevStartKey) == 0 {
		prevStartKey = l.mustGetRangeStart(seekKey)
	}

	oldStart, oldEnd := prevStartKey, seekKey

	// no overlap
	if bytes.Compare(oldStart, end) > 0 ||
		bytes.Compare(start, oldEnd) > 0 {
		return start, end
	}

	min, max := oldStart, oldEnd
	if bytes.Compare(min, start) > 0 {
		min = start
	}
	if bytes.Compare(max, end) < 0 {
		max = end
	}

	mc.mergeLocks([][]byte{oldStart, oldEnd})
	mc.mergeWaiter(seekLock.waiters)
	return min, max
}

func (l *localLockTable) mustGetRangeStart(endKey []byte) []byte {
	v, _, ok := l.mu.store.Prev(endKey)
	if !ok {
		panic("missing start key")
	}
	return v
}

// setModePairedRangeLock updates the mode of the paired range lock entry.
// A range lock is stored as two entries (range-start and range-end) with
// independent Lock.value bytes. When setMode updates one end, this helper
// finds and updates the other end so both entries have a consistent mode.
// It is a no-op for row locks.
// setModePairedRangeLock updates the paired range lock entry's mode to keep
// both ends consistent. For range-end it scans backward to find range-start;
// for range-start it scans forward to find range-end.
func (l *localLockTable) setModePairedRangeLock(key []byte, lock Lock, mode pb.LockMode) {
	if lock.isLockRow() {
		return
	}
	pairedKey, pairedLock, ok := l.findPairedRangeLock(key, lock)
	if !ok {
		return
	}
	if updated, changed := pairedLock.setMode(mode); changed {
		l.mu.store.Add(pairedKey, updated)
	}
}

// findPairedRangeLock locates the other end of a range lock pair.
// Between range-start and range-end there may be interleaved row locks
// from other transactions, so we scan until we find the matching entry.
func (l *localLockTable) findPairedRangeLock(key []byte, lock Lock) ([]byte, Lock, bool) {
	if lock.isLockRangeEnd() {
		cur := key
		for {
			prevKey, prevLock, ok := l.mu.store.Prev(cur)
			if !ok {
				return nil, Lock{}, false
			}
			if prevLock.isLockRangeStart() {
				return prevKey, prevLock, true
			}
			cur = prevKey
		}
	}
	// isLockRangeStart: scan forward
	var pairedKey []byte
	var pairedLock Lock
	var found bool
	l.mu.store.Range(
		nextKey(key, nil),
		nil,
		func(k []byte, v Lock) bool {
			if v.isLockRangeEnd() {
				pairedKey, pairedLock, found = k, v, true
				return false
			}
			return true
		},
	)
	return pairedKey, pairedLock, found
}

func nextKey(src, dst []byte) []byte {
	dst = append(dst, src...)
	dst = append(dst, 0)
	return dst
}

var (
	mergePool = sync.Pool{
		New: func() any {
			return &mergeContext{
				mergedLocks: make(map[string]struct{}, 8),
			}
		},
	}
)

type mergeContext struct {
	to            waiterQueue
	mergedWaiters []waiterQueue
	mergedLocks   map[string]struct{}
}

func newMergeContext(to waiterQueue) *mergeContext {
	c := mergePool.Get().(*mergeContext)
	c.to = to
	c.to.beginChange()
	return c
}

func (c *mergeContext) close() {
	for k := range c.mergedLocks {
		delete(c.mergedLocks, k)
	}
	c.to = nil
	c.mergedWaiters = c.mergedWaiters[:0]
	mergePool.Put(c)
}

func (c *mergeContext) mergeWaiter(from waiterQueue) {
	from.moveTo(c.to)
	c.mergedWaiters = append(c.mergedWaiters, from)
}

func (c *mergeContext) mergeLocks(locks [][]byte) {
	for _, v := range locks {
		c.mergedLocks[util.UnsafeBytesToString(v)] = struct{}{}
	}
}

func (c *mergeContext) commit(
	bind pb.LockTable,
	txn *activeTxn,
	s LockStorage,
	logger *log.MOLogger,
) {
	for k := range c.mergedLocks {
		s.Delete(util.UnsafeStringToBytes(k))
	}

	txn.lockRemoved(
		bind.Group,
		bind.Table,
		c.mergedLocks,
	)

	for _, q := range c.mergedWaiters {
		// release ref in merged waiters. The ref is moved to c.to.
		q.iter(func(w *waiter) bool {
			w.close("mergeContext commit", logger)
			return true
		})
		q.reset()
	}
	c.to.commitChange()
}

func (c *mergeContext) rollback() {
	c.to.rollbackChange()
}

func hasConflictWithLock(
	key []byte,
	lock Lock,
	end []byte) bool {
	if lock.isLockRow() {
		// row lock, start <= key <= end
		return bytes.Compare(key, end) <= 0
	}
	if lock.isLockRangeStart() {
		// range start lock, [1, 4] + [2, any]
		return bytes.Compare(key, end) <= 0
	}
	// range end lock, always conflict
	return true
}
