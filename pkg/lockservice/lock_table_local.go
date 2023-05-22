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

// a localLockTable instance manages the locks on a table
type localLockTable struct {
	bind     pb.LockTable
	fsp      *fixedSlicePool
	detector *detector
	clock    clock.Clock
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
	clock clock.Clock) lockTable {
	l := &localLockTable{
		bind:     bind,
		fsp:      fsp,
		detector: detector,
		clock:    clock,
	}
	l.mu.store = newBtreeBasedStorage()
	return l
}

func (l *localLockTable) lock(
	ctx context.Context,
	txn *activeTxn,
	rows [][]byte,
	opts LockOptions) (pb.Result, error) {
	// FIXME(fagongzi): too many mem alloc in trace
	ctx, span := trace.Debug(ctx, "lockservice.lock.local")
	defer span.End()

	table := l.bind.Table
	offset := 0
	var w *waiter
	var err error
	var idx int
	var lockedTS timestamp.Timestamp
	result := pb.Result{LockedOn: l.bind}
	logLocalLock(l.bind.ServiceID, txn, table, rows, opts)
	for {
		idx, w, lockedTS, err = l.doAcquireLock(
			txn,
			w,
			offset,
			rows,
			opts)
		if err != nil {
			logLocalLockFailed(l.bind.ServiceID, txn, table, rows, opts, err)
			return result, err
		}
		// no waiter, all locks are added
		if w == nil {
			txn.setBlocked(txn.txnID, nil, false)
			logLocalLockAdded(l.bind.ServiceID, txn, l.bind.Table, rows, opts)
			if result.Timestamp.IsEmpty() {
				result.Timestamp = lockedTS
			}
			return result, nil
		}

		v := w.wait(ctx, l.bind.ServiceID)
		logLocalLockWaitOnResult(l.bind.ServiceID, txn, table, rows[idx], opts, w, err)
		if v.err != nil {
			w.close(l.bind.ServiceID, v)
			return result, v.err
		}
		w.resetWait(l.bind.ServiceID)
		offset = idx
		result.Timestamp = v.ts
		result.HasConflict = true
		if !result.HasPrevCommit {
			result.HasPrevCommit = !v.ts.IsEmpty()
		}
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

func (l *localLockTable) doAcquireLock(
	txn *activeTxn,
	w *waiter,
	offset int,
	rows [][]byte,
	opts LockOptions) (int, *waiter, timestamp.Timestamp, error) {
	// The txn lock here to avoid dead lock between doAcquireLock and getLock.
	// The doAcquireLock and getLock operations of the same transaction will be
	// concurrent (deadlock detection), which may lead to a deadlock in mutex.
	txn.Lock()
	defer txn.Unlock()

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.mu.closed {
		return 0, nil,
			timestamp.Timestamp{},
			moerr.NewInvalidStateNoCtx("local lock table closed")
	}

	switch opts.Granularity {
	case pb.Granularity_Row:
		idx, w, lockedTS := l.acquireRowLockLocked(txn, w, offset, rows, opts.Mode)
		return idx, w, lockedTS, nil
	case pb.Granularity_Range:
		if len(rows) == 0 ||
			len(rows)%2 != 0 {
			panic("invalid range lock")
		}
		idx, w, lockedTS := l.acquireRangeLockLocked(txn, w, offset, rows, opts.Mode)
		return idx, w, lockedTS, nil
	default:
		panic(fmt.Sprintf("not support lock granularity %d", opts))
	}
}

func (l *localLockTable) acquireRowLockLocked(
	txn *activeTxn,
	w *waiter,
	offset int,
	rows [][]byte,
	mode pb.LockMode) (int, *waiter, timestamp.Timestamp) {
	n := len(rows)
	for idx := offset; idx < n; idx++ {
		row := rows[idx]
		logLocalLockRow(l.bind.ServiceID, txn, l.bind.Table, row, mode)
		key, lock, ok := l.mu.store.Seek(row)
		if ok &&
			(bytes.Equal(key, row) ||
				lock.isLockRangeEnd()) {
			// current txn's lock
			if bytes.Equal(txn.txnID, lock.txnID) {
				if w != nil {
					panic("BUG: can not has a waiter on self txn")
				}
				continue
			}
			w = getWaiter(l.bind.ServiceID, w, txn.txnID)
			l.handleLockConflictLocked(txn, w, key, lock)
			return idx, w, timestamp.Timestamp{}
		}
		l.addRowLockLocked(txn, row, getWaiter(l.bind.ServiceID, w, txn.txnID), mode)
		// lock added, need create new waiter next time
		w = nil
	}
	now, _ := l.clock.Now()
	return 0, nil, now
}

func (l *localLockTable) acquireRangeLockLocked(
	txn *activeTxn,
	w *waiter,
	offset int,
	rows [][]byte,
	mode pb.LockMode) (int, *waiter, timestamp.Timestamp) {
	n := len(rows)
	for i := offset; i < n; i += 2 {
		start := rows[i]
		end := rows[i+1]
		if bytes.Compare(start, end) >= 0 {
			panic(fmt.Sprintf("lock error: start[%v] is greater than end[%v]",
				start, end))
		}

		logLocalLockRange(l.bind.ServiceID, txn, l.bind.Table, start, end, mode)
		w = getWaiter(l.bind.ServiceID, w, txn.txnID)

		confilct, conflictWith := l.addRangeLockLocked(w, txn, start, end, mode)
		if len(confilct) > 0 {
			w = getWaiter(l.bind.ServiceID, w, txn.txnID)
			l.handleLockConflictLocked(txn, w, confilct, conflictWith)
			return i, w, timestamp.Timestamp{}
		}

		// lock added, need create new waiter next time
		w = nil
	}
	now, _ := l.clock.Now()
	return 0, nil, now
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
		txn.toWaitTxn(
			l.bind.ServiceID,
			true)); err != nil {
		panic("BUG: active dead lock check can not fail")
	}
	logLocalLockWaitOn(l.bind.ServiceID, txn, l.bind.Table, w, key, conflictWith)
}

func getWaiter(serviceID string, w *waiter, txnID []byte) *waiter {
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
	mc := newMergeContext()
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
	changedWaiters []*waiter
	mergedWaiters  []*waiter
	mergedLocks    map[string]struct{}
}

func newMergeContext() *mergeContext {
	c := mergePool.Get().(*mergeContext)
	return c
}

func (c *mergeContext) close() {
	for k := range c.mergedLocks {
		delete(c.mergedLocks, k)
	}
	c.changedWaiters = c.changedWaiters[:0]
	c.mergedWaiters = c.mergedWaiters[:0]
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
}

func (c *mergeContext) rollback() {
	for _, w := range c.changedWaiters {
		w.waiters.rollbackChange()
	}
}
