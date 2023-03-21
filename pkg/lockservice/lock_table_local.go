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
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

// a localLockTable instance manages the locks on a table
type localLockTable struct {
	bind     pb.LockTable
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
	detector *detector,
	clock clock.Clock) lockTable {
	l := &localLockTable{
		bind:     bind,
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
			txn.setBlocked(txn.txnID, nil)
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
			if w = w.close(l.bind.ServiceID, notifyValue{err: ErrLockTableNotFound}); w != nil {
				w.notify(l.bind.ServiceID, notifyValue{err: ErrLockTableNotFound})
			}
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
			l.handleLockConflict(txn, w, key, lock)
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
	for i := offset; i < n; {
		start := rows[i]
		end := rows[i+1]
		if bytes.Compare(start, end) >= 0 {
			panic(fmt.Sprintf("lock error: start[%v] is greater than end[%v]",
				start, end))
		}
		logLocalLockRange(l.bind.ServiceID, txn, l.bind.Table, start, end, mode)
		key, lock, ok := l.mu.store.Seek(start)
		if ok &&
			bytes.Compare(key, end) <= 0 {
			// current txn's lock
			if bytes.Equal(txn.txnID, lock.txnID) {
				// TODO: range merge, and check start to end conflict
				panic("BUG: current not support")
			}

			w = getWaiter(l.bind.ServiceID, w, txn.txnID)
			l.handleLockConflict(txn, w, key, lock)
			return i, w, timestamp.Timestamp{}
		}

		l.addRangeLockLocked(txn, start, end, getWaiter(l.bind.ServiceID, w, txn.txnID), mode)
		// lock added, need create new waiter next time
		w = nil
		i += 2
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
	txn.lockAdded(l.bind.ServiceID, l.bind.Table, [][]byte{row}, false)
	l.mu.store.Add(row, lock)
}

func (l *localLockTable) addRangeLockLocked(
	txn *activeTxn,
	start, end []byte,
	waiter *waiter,
	mode pb.LockMode) {
	startLock, endLock := newRangeLock(txn.txnID, mode)
	startLock.waiter = waiter
	endLock.waiter = waiter

	// similar to row lock
	txn.lockAdded(l.bind.ServiceID, l.bind.Table, [][]byte{start, end}, false)
	l.mu.store.Add(start, startLock)
	l.mu.store.Add(end, endLock)
}

func (l *localLockTable) handleLockConflict(
	txn *activeTxn,
	w *waiter,
	key []byte,
	conflictWith Lock) {
	// find conflict, and wait prev txn completed, and a new
	// waiter added, we need to active deadlock check.
	txn.setBlocked(w.txnID, w)
	conflictWith.waiter.add(l.bind.ServiceID, w)
	if err := l.detector.check(
		txn.toWaitTxn(
			l.bind.ServiceID,
			false)); err != nil {
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
