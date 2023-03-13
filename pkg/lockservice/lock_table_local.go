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
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

// a localLockTable instance manages the locks on a table
type localLockTable struct {
	bind     pb.LockTable
	detector *detector

	mu struct {
		sync.RWMutex
		closed bool
		store  LockStorage
	}
}

func newLocalLockTable(
	bind pb.LockTable,
	detector *detector) lockTable {
	l := &localLockTable{
		bind:     bind,
		detector: detector,
	}
	l.mu.store = newBtreeBasedStorage()
	return l
}

func (l *localLockTable) lock(
	ctx context.Context,
	txn *activeTxn,
	rows [][]byte,
	opts LockOptions) error {
	// FIXME(fagongzi): too many mem alloc in trace
	ctx, span := trace.Debug(ctx, "lockservice.lock.local")
	defer span.End()

	table := l.bind.Table
	w := acquireWaiter(txn.txnID)
	for {
		logLocalLock(txn, table, rows, opts, w)
		added, err := l.doAcquireLock(
			txn,
			w,
			rows,
			opts)
		if err != nil {
			logLocalLockFailed(txn, table, rows, opts, err)
			return err
		}
		if added {
			txn.setBlocked(w.txnID, nil)
			logLocalLockAdded(txn, l.bind.Table, rows, opts)
			return nil
		}

		err = w.wait(ctx)
		logLocalLockWaitOnResult(txn, table, rows, opts, w, err)
		if err != nil {
			return err
		}
		w.resetWait()
	}
}

func (l *localLockTable) unlock(
	txn *activeTxn,
	ls *cowSlice) {
	logUnlockTableOnLocal(
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
				lock.waiter.clearAllNotify("unlock")
				lock.waiter.close(nil)
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
		w := lock.waiter
		for {
			w.clearAllNotify("close local")
			if w = w.close(ErrLockTableNotFound); w == nil {
				break
			}
		}
		return true
	})
	l.mu.store.Clear()
	logLockTableClosed(l.bind, false)
}

func (l *localLockTable) doAcquireLock(
	txn *activeTxn,
	waiter *waiter,
	rows [][]byte,
	opts LockOptions) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.mu.closed {
		return false, moerr.NewInvalidStateNoCtx("local lock table closed")
	}

	switch opts.Granularity {
	case pb.Granularity_Row:
		added := l.acquireRowLockLocked(txn, waiter, rows[0], opts.Mode)
		return added, nil
	case pb.Granularity_Range:
		if len(rows) != 2 {
			panic("invalid range lock")
		}
		added := l.acquireRangeLockLocked(txn, waiter, rows[0], rows[1], opts.Mode)
		return added, nil
	default:
		panic(fmt.Sprintf("not support lock granularity %d", opts))
	}
}

func (l *localLockTable) acquireRowLockLocked(
	txn *activeTxn,
	w *waiter,
	row []byte,
	mode pb.LockMode) bool {
	key, lock, ok := l.mu.store.Seek(row)
	if ok &&
		(bytes.Equal(key, row) ||
			lock.isLockRangeEnd()) {
		l.handleLockConflict(txn, w, lock)
		return false
	}
	l.addRowLockLocked(txn, row, w, mode)
	return true
}

func (l *localLockTable) acquireRangeLockLocked(
	txn *activeTxn,
	w *waiter,
	start, end []byte,
	mode pb.LockMode) bool {
	if bytes.Compare(start, end) >= 0 {
		panic(fmt.Sprintf("lock error: start[%v] is greater than end[%v]",
			start, end))
	}
	key, lock, ok := l.mu.store.Seek(start)
	if ok &&
		bytes.Compare(key, end) <= 0 {
		l.handleLockConflict(txn, w, lock)
		return false
	}

	l.addRangeLockLocked(txn, start, end, w, mode)
	return true
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
	txn.lockAdded(l.bind.Table, [][]byte{row}, false)
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
	txn.lockAdded(l.bind.Table, [][]byte{start, end}, false)
	l.mu.store.Add(start, startLock)
	l.mu.store.Add(end, endLock)
}

func (l *localLockTable) handleLockConflict(
	txn *activeTxn,
	w *waiter,
	conflictWith Lock) {
	// find conflict, and wait prev txn completed, and a new
	// waiter added, we need to active deadlock check.
	txn.setBlocked(w.txnID, w)
	conflictWith.waiter.add(w)
	if err := l.detector.check(txn.txnID); err != nil {
		panic("BUG: active dead lock check can not fail")
	}
	logLocalLockWaitOn(txn, l.bind.Table, w, conflictWith)
}
