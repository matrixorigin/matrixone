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
	"fmt"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"go.uber.org/zap"
)

const (
	eventsWorkers = 4
)

// a localLockTable instance manages the locks on a table
type localLockTable struct {
	bind   pb.LockTable
	fsp    *fixedSlicePool
	clock  clock.Clock
	events *waiterEvents
	mu     struct {
		sync.RWMutex
		closed           bool
		store            LockStorage
		tableCommittedAt timestamp.Timestamp
	}

	options struct {
		beforeCloseFirstWaiter func(c *lockContext)
	}
}

func newLocalLockTable(
	bind pb.LockTable,
	fsp *fixedSlicePool,
	events *waiterEvents,
	clock clock.Clock) lockTable {
	l := &localLockTable{
		bind:   bind,
		fsp:    fsp,
		clock:  clock,
		events: events,
	}
	l.mu.store = newBtreeBasedStorage()
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

	// FIXME(fagongzi): too many mem alloc in trace
	ctx, span := trace.Debug(ctx, "lockservice.lock.local")
	defer span.End()

	logLocalLock(txn, l.bind.Table, rows, opts)
	c := l.newLockContext(ctx, txn, rows, opts, cb, l.bind)
	if opts.async {
		c.lockFunc = l.doLock
	}
	l.doLock(c, false)
}

func (l *localLockTable) doLock(
	c *lockContext,
	blocked bool) {
	// deadlock detected, return
	if c.txn.deadlockFound {
		c.done(ErrDeadLockDetected)
		return
	}
	var old *waiter
	var err error
	table := l.bind.Table
	for {
		// blocked used for async callback, waiter is created, and added to wait list.
		// So only need wait notify.
		if !blocked {
			err = l.doAcquireLock(c)
			if err != nil {
				logLocalLockFailed(c.txn, table, c.rows, c.opts, err)
				if c.w != nil {
					c.w.disableNotify()
					c.w.close()
				}
				c.done(err)
				return
			}
			// no waiter, all locks are added
			if c.w == nil {
				v2.TxnAcquireLockWaitDurationHistogram.Observe(time.Since(c.createAt).Seconds())
				c.txn.clearBlocked(old)
				logLocalLockAdded(c.txn, l.bind.Table, c.rows, c.opts)
				if c.result.Timestamp.IsEmpty() {
					c.result.Timestamp = c.lockedTS
				}
				c.done(nil)
				return
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
		oldTxnID := c.txn.txnID
		old = c.w
		c.txn.Unlock()
		v := c.w.wait(c.ctx)
		c.txn.Lock()

		logLocalLockWaitOnResult(c.txn, table, c.rows[c.idx], c.opts, c.w, v)

		// txn closed between Unlock and get Lock again
		e := v.err
		if e == nil && (!bytes.Equal(oldTxnID, c.txn.txnID) ||
			!bytes.Equal(c.w.txn.TxnID, oldTxnID)) {
			e = ErrTxnNotFound
		}

		if e != nil {
			c.closed = true
			if len(c.w.conflictKey) > 0 &&
				c.opts.Granularity == pb.Granularity_Row {

				if l.options.beforeCloseFirstWaiter != nil {
					l.options.beforeCloseFirstWaiter(c)
				}

				l.mu.Lock()
				// we must reload conflict lock, because the lock may be deleted
				// by other txn and readd into store. So c.w.conflictWith is
				// invalid.
				conflictWith, ok := l.mu.store.Get(c.w.conflictKey)
				if ok && conflictWith.closeFirstWaiter(c.w) {
					l.mu.store.Delete(c.w.conflictKey)
				}
				l.mu.Unlock()
			}

			c.w.close()
			c.done(e)
			return
		}

		if c.opts.RetryWait > 0 {
			time.Sleep(time.Duration(c.opts.RetryWait))
		}

		c.w.resetWait()
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

func (l *localLockTable) unlock(
	txn *activeTxn,
	ls *cowSlice,
	commitTS timestamp.Timestamp) {
	start := time.Now()
	defer func() {
		v2.TxnUnlockBtreeTotalDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	logUnlockTableOnLocal(
		l.bind.ServiceID,
		txn,
		l.bind)

	locks := ls.slice()
	defer locks.unref()

	l.mu.Lock()
	v2.TxnUnlockBtreeGetLockDurationHistogram.Observe(time.Since(start).Seconds())

	defer l.mu.Unlock()
	if l.mu.closed {
		return
	}

	b, ok := txn.holdBinds[l.bind.Table]
	if !ok {
		panic("BUG: missing bind")
	}

	var startKey []byte
	locks.iter(func(key []byte) bool {
		if lock, ok := l.mu.store.Get(key); ok {
			if lock.isLockRangeStart() {
				startKey = key
				return true
			}

			if !lock.holders.contains(txn.txnID) {
				// 1. txn1 hold key1 on bind version 0
				// 2. txn1 commit success
				// 3. dn restart
				// 4. txn2 hold key1 on bind version 1
				// 5. txn1 unlock.
				if b.Changed(l.bind) {
					return true
				}

				getLogger().Fatal("BUG: unlock a lock that is not held by the current txn",
					zap.Bool("row", lock.isLockRow()),
					zap.Int("keys-count", locks.len()),
					zap.String("hold-bind", b.DebugString()),
					zap.String("bind", l.bind.DebugString()),
					waitTxnArrayField("holders", lock.holders.txns),
					txnField(txn))
			}
			if len(startKey) > 0 && !lock.isLockRangeEnd() {
				panic("BUG: missing range end key")
			}

			lockCanRemoved := lock.closeTxn(
				txn,
				notifyValue{ts: commitTS})
			logLockUnlocked(txn, key, lock)
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

func (l *localLockTable) getBind() pb.LockTable {
	return l.bind
}

func (l *localLockTable) close() {
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
	l.mu.store.Clear()
	logLockTableClosed(l.bind, false)
}

func (l *localLockTable) doAcquireLock(c *lockContext) error {
	l.mu.Lock()
	defer l.mu.Unlock()

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
		row := c.rows[idx]

		key, lock, ok := l.mu.store.Seek(row)
		if ok &&
			(bytes.Equal(key, row) ||
				lock.isLockRangeEnd()) {
			hold, newHolder := lock.tryHold(c)
			if hold {
				if c.w != nil {
					c.w.disableNotify()
					c.w.close()
					c.w = nil
				}
				// only new holder can added lock into txn.
				// newHolder is false means prev op of txn has already added lock into txn
				if newHolder {
					c.txn.lockAdded(l.bind, [][]byte{key})
				}
				continue
			}

			// need wait for prev txn closed
			if c.w == nil {
				c.w = acquireWaiter(c.waitTxn)
			}

			c.offset = idx
			l.handleLockConflictLocked(c, key, lock)
			return nil
		}
		l.addRowLockLocked(c, row)
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
		start := c.rows[i]
		end := c.rows[i+1]
		if bytes.Compare(start, end) >= 0 {
			panic(fmt.Sprintf("lock error: start[%v] is greater than end[%v]",
				start, end))
		}

		logLocalLockRange(c.txn, l.bind.Table, start, end, c.opts.Mode)
		conflict, conflictWith, err := l.addRangeLockLocked(c, start, end)
		if err != nil {
			return err
		}
		if len(conflict) > 0 {
			c.w = acquireWaiter(c.waitTxn)
			c.offset = i
			l.handleLockConflictLocked(c, conflict, conflictWith)
			return nil
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
	row []byte) {
	lock := newRowLock(c)

	// new lock added, use last committed ts to update keys last commit ts.
	lock.waiters.resetCommittedAt(l.mu.tableCommittedAt)

	// we must first add the lock to txn to ensure that the
	// lock can be read when the deadlock is detected.
	c.txn.lockAdded(l.bind, [][]byte{row})
	l.mu.store.Add(row, lock)
}

func (l *localLockTable) handleLockConflictLocked(
	c *lockContext,
	key []byte,
	conflictWith Lock) {
	c.w.conflictKey = key
	c.w.conflictWith = conflictWith
	c.w.lt = l
	c.w.waitFor = c.w.waitFor[:0]
	for _, txn := range conflictWith.holders.txns {
		c.w.waitFor = append(c.w.waitFor, txn.TxnID)
	}
	conflictWith.waiters.iter(func(w *waiter) bool {
		c.w.waitFor = append(c.w.waitFor, w.txn.TxnID)
		return true
	})

	conflictWith.addWaiter(c.w)
	l.events.add(c)

	// find conflict, and wait prev txn completed, and a new
	// waiter added, we need to active deadlock check.
	c.txn.setBlocked(c.w)
	logLocalLockWaitOn(c.txn, l.bind.Table, c.w, key, conflictWith)
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
			hold, newHolder := l1.tryHold(c)
			if !hold {
				panic("BUG: must get shared lock")
			}
			hold, _ = l2.tryHold(c)
			if !hold {
				panic("BUG: must get shared lock")
			}
			if newHolder {
				c.txn.lockAdded(l.bind, [][]byte{start, end})
			}
			return nil, Lock{}, nil
		}
	}

	wq := newWaiterQueue()
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
					c.txn)
				prevStartKey = nil
				rangeStartEncountered = false
				return bytes.Compare(key, end) < 0
			})
		if err != nil {
			mc.rollback()
			return nil, Lock{}, err
		}

		if len(conflictKey) > 0 {
			hold, newHolder := conflictWith.tryHold(c)
			if hold {
				// only new holder can added lock into txn.
				// newHolder is false means prev op of txn has already added lock into txn
				if newHolder {
					c.txn.lockAdded(l.bind, [][]byte{conflictKey})
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
				c.txn)
		}
		break
	}

	mc.commit(l.bind, c.txn, l.mu.store)
	startLock, endLock := newRangeLock(c)

	wq.resetCommittedAt(l.mu.tableCommittedAt)
	startLock.waiters = wq
	endLock.waiters = wq

	// similar to row lock
	c.txn.lockAdded(l.bind, [][]byte{start, end})

	l.mu.store.Add(start, startLock)
	l.mu.store.Add(end, endLock)
	return nil, Lock{}, nil
}

func (l *localLockTable) mergeRangeLocked(
	start, end []byte,
	prevStartKey []byte,
	seekKey []byte,
	seekLock Lock,
	mc *mergeContext,
	txn *activeTxn) ([]byte, []byte) {
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
	s LockStorage) {
	for k := range c.mergedLocks {
		s.Delete(util.UnsafeStringToBytes(k))
	}
	txn.lockRemoved(
		bind.ServiceID,
		bind.Table,
		c.mergedLocks)

	for _, q := range c.mergedWaiters {
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
