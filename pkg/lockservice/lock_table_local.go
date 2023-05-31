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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

const (
	eventsWorkers = 4
)

// a localLockTable instance manages the locks on a table
type localLockTable struct {
	bind     pb.LockTable
	fsp      *fixedSlicePool
	detector *detector
	clock    clock.Clock
	events   *waiterEvents
	mu       struct {
		sync.RWMutex
		closed bool
		store  LockStorage
	}
}

func newLocalLockTable(
	bind pb.LockTable,
	fsp *fixedSlicePool,
	detector *detector,
	events *waiterEvents,
	clock clock.Clock) lockTable {
	l := &localLockTable{
		bind:     bind,
		fsp:      fsp,
		detector: detector,
		clock:    clock,
		events:   events,
	}
	l.mu.store = newBtreeBasedStorage()
	return l
}

func (l *localLockTable) lock(
	ctx context.Context,
	txn *activeTxn,
	rows [][]byte,
	opts LockOptions,
	cb func(pb.Result, error)) {
	// FIXME(fagongzi): too many mem alloc in trace
	ctx, span := trace.Debug(ctx, "lockservice.lock.local")
	defer span.End()

	logLocalLock(l.bind.ServiceID, txn, l.bind.Table, rows, opts)
	c := newLockContext(ctx, txn, rows, opts, cb, l.bind)
	if opts.async {
		c.lockFunc = l.doLock
	}
	l.doLock(c, false)
}

func (l *localLockTable) doLock(
	c lockContext,
	blocked bool) {
	var err error
	table := l.bind.Table
	for {
		// blocked used for async callback, waiter is created, and added to wait list.
		// So only need wait notify.
		if !blocked {
			c, err = l.doAcquireLock(c)
			if err != nil {
				logLocalLockFailed(l.bind.ServiceID, c.txn, table, c.rows, c.opts, err)
				c.done(err)
				return
			}
			// no waiter, all locks are added
			if c.w == nil {
				c.txn.setBlocked(c.txn.txnID, nil, false)
				logLocalLockAdded(l.bind.ServiceID, c.txn, l.bind.Table, c.rows, c.opts)
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

		v := c.w.wait(c.ctx, l.bind.ServiceID)
		logLocalLockWaitOnResult(l.bind.ServiceID, c.txn, table, c.rows[c.idx], c.opts, c.w, err)
		if v.err != nil {
			c.w.close(l.bind.ServiceID, v)
			c.done(v.err)
			return
		}
		c.w.resetWait(l.bind.ServiceID)
		c.offset = c.idx
		c.result.Timestamp = v.ts
		c.result.HasConflict = true
		if !c.result.HasPrevCommit {
			c.result.HasPrevCommit = !v.ts.IsEmpty()
		}
		blocked = false
	}
}

func (l *localLockTable) unlock(
	txn *activeTxn,
	ls *cowSlice,
	commitTS timestamp.Timestamp) {
	logUnlockTableOnLocal(
		l.bind.ServiceID,
		txn,
		l.bind)

	locks := ls.slice()
	defer locks.unref()

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.mu.closed {
		return
	}

	locks.iter(func(key []byte) bool {
		if lock, ok := l.mu.store.Get(key); ok {
			if lock.isLockRow() || lock.isLockRangeEnd() {
				lock.waiter.clearAllNotify(l.bind.ServiceID, "unlock")
				next := lock.waiter.close(l.bind.ServiceID, notifyValue{ts: commitTS})
				logUnlockTableKeyOnLocal(l.bind.ServiceID, txn, l.bind, key, lock, next)
			}
			l.mu.store.Delete(key)
		}
		return true
	})
}

func (l *localLockTable) getLock(txnID, key []byte, fn func(Lock)) {
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
			w := lock.waiter
			w.clearAllNotify(l.bind.ServiceID, "close local")
			// if there are waiters in the current lock, just notify
			// the head, and the subsequent waiters will be notified
			// by the previous waiter.
			w.close(l.bind.ServiceID, notifyValue{err: ErrLockTableNotFound})
		}
		return true
	})
	l.mu.store.Clear()
	logLockTableClosed(l.bind.ServiceID, l.bind, false)
}

func (l *localLockTable) doAcquireLock(c lockContext) (lockContext, error) {
	// The txn lock here to avoid dead lock between doAcquireLock and getLock.
	// The doAcquireLock and getLock operations of the same transaction will be
	// concurrent (deadlock detection), which may lead to a deadlock in mutex.
	c.txn.Lock()
	defer c.txn.Unlock()

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.mu.closed {
		return c, moerr.NewInvalidStateNoCtx("local lock table closed")
	}

	switch c.opts.Granularity {
	case pb.Granularity_Row:
		return l.acquireRowLockLocked(c), nil
	case pb.Granularity_Range:
		if len(c.rows) == 0 ||
			len(c.rows)%2 != 0 {
			panic("invalid range lock")
		}
		return l.acquireRangeLockLocked(c), nil
	default:
		panic(fmt.Sprintf("not support lock granularity %d", c.opts.Granularity))
	}
}

func (l *localLockTable) acquireRowLockLocked(c lockContext) lockContext {
	n := len(c.rows)
	for idx := c.offset; idx < n; idx++ {
		row := c.rows[idx]
		logLocalLockRow(l.bind.ServiceID, c.txn, l.bind.Table, row, c.opts.Mode)
		key, lock, ok := l.mu.store.Seek(row)
		if ok &&
			(bytes.Equal(key, row) ||
				lock.isLockRangeEnd()) {
			// current txn's lock
			if bytes.Equal(c.txn.txnID, lock.txnID) {
				if c.w != nil {
					// txn1 hold lock
					// txn2/op1 added into txn1's waiting list
					// txn2/op2 added into txn2/op1's same txn list
					// txn1 unlock, notify txn2/op1
					// txn2/op3 get lock before txn2/op1 get notify
					if len(c.w.sameTxnWaiters) > 0 {
						c.w.notifySameTxn(l.bind.ServiceID, notifyValue{})
					}
					str := c.w.String()
					if v := c.w.close(l.bind.ServiceID, notifyValue{}); v != nil {
						panic("BUG: waiters should be empty, " + str + "," + v.String() + ", " + fmt.Sprintf("table(%d)  %+v", l.bind.Table, key))
					}
					c.w = nil
				}
				continue
			}
			c.w = getWaiter(l.bind.ServiceID, c.w, c.txn.txnID)
			c.offset = idx
			if c.opts.async {
				l.events.add(c)
			}
			l.handleLockConflictLocked(c.txn, c.w, key, lock)
			return c
		}
		l.addRowLockLocked(c.txn, row, getWaiter(l.bind.ServiceID, c.w, c.txn.txnID), c.opts.Mode)
		// lock added, need create new waiter next time
		c.w = nil
	}
	now, _ := l.clock.Now()
	c.offset = 0
	c.lockedTS = now
	return c
}

func (l *localLockTable) acquireRangeLockLocked(c lockContext) lockContext {
	n := len(c.rows)
	for i := c.offset; i < n; i += 2 {
		start := c.rows[i]
		end := c.rows[i+1]
		if bytes.Compare(start, end) >= 0 {
			panic(fmt.Sprintf("lock error: start[%v] is greater than end[%v]",
				start, end))
		}

		logLocalLockRange(l.bind.ServiceID, c.txn, l.bind.Table, start, end, c.opts.Mode)
		c.w = getWaiter(l.bind.ServiceID, c.w, c.txn.txnID)

		confilct, conflictWith := l.addRangeLockLocked(c.w, c.txn, start, end, c.opts.Mode)
		if len(confilct) > 0 {
			c.w = getWaiter(l.bind.ServiceID, c.w, c.txn.txnID)
			l.handleLockConflictLocked(c.txn, c.w, confilct, conflictWith)
			c.offset = i
			return c
		}

		// lock added, need create new waiter next time
		c.w = nil
	}
	now, _ := l.clock.Now()
	c.offset = 0
	c.lockedTS = now
	return c
}

func (l *localLockTable) addRowLockLocked(
	txn *activeTxn,
	row []byte,
	waiter *waiter,
	mode pb.LockMode) {
	lock := newRowLock(txn.txnID, mode)
	lock.waiter = waiter

	// we must first add the lock to txn to ensure that the
	// lock can be read when the deadlock is detected.
	txn.lockAdded(l.bind.ServiceID, l.bind.Table, [][]byte{row}, true)
	l.mu.store.Add(row, lock)

	// if has same txn's waiters on same key, there must be at wait status.
	waiter.notifySameTxn(l.bind.ServiceID, notifyValue{})
}

func (l *localLockTable) handleLockConflictLocked(
	txn *activeTxn,
	w *waiter,
	key []byte,
	conflictWith Lock) {
	// find conflict, and wait prev txn completed, and a new
	// waiter added, we need to active deadlock check.
	txn.setBlocked(w.txnID, w, true)
	conflictWith.waiter.add(l.bind.ServiceID, w)
	if err := l.detector.check(
		conflictWith.txnID,
		txn.toWaitTxn(
			l.bind.ServiceID,
			true)); err != nil {
		panic("BUG: active dead lock check can not fail")
	}
	logLocalLockWaitOn(l.bind.ServiceID, txn, l.bind.Table, w, key, conflictWith)
}

func getWaiter(
	serviceID string,
	w *waiter,
	txnID []byte) *waiter {
	if w != nil {
		return w
	}
	return acquireWaiter(serviceID, txnID)
}

func (l *localLockTable) addRangeLockLocked(
	w *waiter,
	txn *activeTxn,
	start, end []byte,
	mode pb.LockMode) ([]byte, Lock) {
	w = getWaiter(l.bind.ServiceID, w, txn.txnID)
	mc := newMergeContext(w)
	defer mc.close()

	var conflictWith Lock
	var confilctKey []byte
	var prevStartKey []byte
	rangeStartEncountered := false
	upbound := nextKey(end, nil)
	l.mu.store.Range(
		start,
		upbound,
		func(key []byte, keyLock Lock) bool {
			if !bytes.Equal(keyLock.txnID, txn.txnID) {
				conflictWith = keyLock
				confilctKey = key
				return false
			}

			if keyLock.isLockRangeStart() {
				prevStartKey = key
				rangeStartEncountered = true
				return true
			}
			if rangeStartEncountered &&
				!keyLock.isLockRangeEnd() {
				panic("BUG, missing range end key")
			}

			w, start, end = l.mergeRangeLocked(
				w,
				start, end,
				prevStartKey,
				key, keyLock,
				mc,
				txn)
			prevStartKey = nil
			rangeStartEncountered = false
			return true
		})

	if rangeStartEncountered {
		key, keyLock, ok := l.mu.store.Seek(upbound)
		if !ok {
			panic("BUG, missing range end key")
		}
		w, start, end = l.mergeRangeLocked(
			w,
			start, end,
			prevStartKey,
			key, keyLock,
			mc,
			txn)
	}

	if len(confilctKey) > 0 {
		mc.rollback()
		return confilctKey, conflictWith
	}

	mc.commit(l.bind, txn, l.mu.store)
	startLock, endLock := newRangeLock(txn.txnID, mode)
	startLock.waiter = w
	endLock.waiter = w

	// similar to row lock
	txn.lockAdded(l.bind.ServiceID, l.bind.Table, [][]byte{start, end}, true)
	l.mu.store.Add(start, startLock)
	l.mu.store.Add(end, endLock)

	return nil, Lock{}
}

func (l *localLockTable) mergeRangeLocked(
	w *waiter,
	start, end []byte,
	prevStartKey []byte,
	seekKey []byte,
	seekLock Lock,
	mc *mergeContext,
	txn *activeTxn) (*waiter, []byte, []byte) {
	// range lock encounted a row lock
	if seekLock.isLockRow() {
		// [1+] + [1, 4] => [1, 4]
		mc.mergeLocks([][]byte{seekKey})
		mc.mergeWaiter(l.bind.ServiceID, seekLock.waiter, w)
		return w, start, end
	}

	if len(prevStartKey) == 0 {
		prevStartKey = l.mustGetRangeStart(seekKey)
	}

	oldStart, oldEnd := prevStartKey, seekKey

	// [start, end] < [oldStart, oldEnd]
	// [oldStart, oldEnd] < [start, end]
	if bytes.Compare(end, oldStart) < 0 ||
		bytes.Compare(oldEnd, start) < 0 {
		return w, start, end
	}

	// [oldStart <= start, end <= oldEnd]
	if bytes.Compare(oldStart, start) <= 0 &&
		bytes.Compare(end, oldEnd) <= 0 {
		mc.mergeLocks([][]byte{oldStart, oldEnd})
		mc.mergeWaiter(l.bind.ServiceID, w, seekLock.waiter)
		return seekLock.waiter, oldStart, oldEnd
	}

	// [start <= oldStart, oldEnd <= end]
	if bytes.Compare(start, oldStart) <= 0 &&
		bytes.Compare(oldEnd, end) <= 0 {
		mc.mergeLocks([][]byte{oldStart, oldEnd})
		mc.mergeWaiter(l.bind.ServiceID, seekLock.waiter, w)
		return w, start, end
	}

	// [start, end] intersect [oldStart , oldEnd]
	if between(end, start, oldEnd) &&
		between(oldStart, start, oldEnd) {
		mc.mergeLocks([][]byte{start, end, oldStart, oldEnd})
		mc.mergeWaiter(l.bind.ServiceID, seekLock.waiter, w)
		return w, start, oldEnd
	}

	// [oldStart , oldEnd] intersect [start, end]
	if between(oldEnd, oldStart, end) &&
		between(start, oldStart, end) {
		mc.mergeLocks([][]byte{start, end, oldStart, oldEnd})
		mc.mergeWaiter(l.bind.ServiceID, seekLock.waiter, w)
		return w, oldStart, end
	}

	panic(fmt.Sprintf("BUG: missing some case, new [%+v, %+v], old [%+v, %+v]",
		start, end,
		oldStart, oldEnd))
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

func between(target, start, end []byte) bool {
	return bytes.Compare(target, start) >= 0 &&
		bytes.Compare(target, end) <= 0
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
	origin         *waiter
	waitOnSameKey  []*waiter
	changedWaiters []*waiter
	mergedWaiters  []*waiter
	mergedLocks    map[string]struct{}
}

func newMergeContext(w *waiter) *mergeContext {
	c := mergePool.Get().(*mergeContext)
	c.origin = w
	c.waitOnSameKey = append(c.waitOnSameKey, w.sameTxnWaiters...)
	return c
}

func (c *mergeContext) close() {
	for k := range c.mergedLocks {
		delete(c.mergedLocks, k)
	}
	c.changedWaiters = c.changedWaiters[:0]
	c.mergedWaiters = c.mergedWaiters[:0]
	c.waitOnSameKey = c.waitOnSameKey[:0]
	c.origin = nil
	mergePool.Put(c)
}

func (c *mergeContext) mergeWaiter(serviceID string, from, to *waiter) {
	from.moveTo(serviceID, to)
	c.mergedWaiters = append(c.mergedWaiters, from)
	c.changedWaiters = append(c.changedWaiters, to)
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
		c.mergedLocks,
		true)

	for _, w := range c.changedWaiters {
		w.waiters.commitChange()
	}

	for _, w := range c.mergedWaiters {
		w.waiters.reset()
		w.unref(bind.ServiceID)
	}

	// if has same txn's waiters on same key, there must be at wait status.
	for _, v := range c.waitOnSameKey {
		v.notify("", notifyValue{})
	}
}

func (c *mergeContext) rollback() {
	for _, w := range c.changedWaiters {
		w.waiters.rollbackChange()
	}
}
