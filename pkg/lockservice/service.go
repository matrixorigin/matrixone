// Copyright 2022 Matrix Origin
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
	"sync"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type service struct {
	mu sync.RWMutex

	// txnID -> tableID -> lockKeys
	// locks sync.Map
	locks map[string]map[uint64][][]byte
	// waitingLocks waitting locks, map[txnid]map[lockKey]*waiter
	waitingLocks map[string]*waiter
	// tableID -> LockStorage
	//seqStorage [32]sync.Map
	seqStorage       map[uint64]LockStorage
	deadlockDetector *detector
}

func NewLockService() LockService {
	l := &service{
		locks:        make(map[string]map[uint64][][]byte),
		seqStorage:   make(map[uint64]LockStorage),
		waitingLocks: make(map[string]*waiter),
	}
	l.deadlockDetector = newDeadlockDetector(l.fetchTxnWaitingList,
		l.abortDeadlockTxn)
	return l
}

func (s *service) Lock(ctx context.Context, tableID uint64, rows [][]byte, txnID []byte, options LockOptions) (bool, error) {
	if err := s.acquireLock(ctx, tableID, rows, txnID, options); err != nil {
		return false, err
	}
	return true, nil
}

func (s *service) Unlock(txnID []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for tableID, keys := range s.locks[unsafeByteSliceToString(txnID)] {
		storage := s.seqStorage[tableID]
		for _, key := range keys {
			if lock, ok := storage.Get(key); ok {
				if lock.isLockRow() || lock.isLockRangeEnd() {
					lock.waiter.close()
				}
				storage.Delete(key)
			}
		}
		delete(s.locks[unsafeByteSliceToString(txnID)], tableID)
	}
	// The deadlock detector will hold the deadlocked transaction that is aborted
	// to avoid the situation where the deadlock detection is interfered with by
	// the abort transaction. When a transaction is unlocked, the deadlock detector
	// needs to be notified to release memory.
	s.deadlockDetector.txnClosed(txnID)
	return nil
}

func (s *service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deadlockDetector.close()
	return nil
}

func (s *service) acquireLock(ctx context.Context, tableID uint64, rows [][]byte, txnID []byte, options LockOptions) error {
	waiter := acquireWaiter(txnID)
	for {
		ok, err := s.doAcquireLock(waiter, tableID, rows, txnID, options)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}

		if err := waiter.wait(ctx); err != nil {
			return err
		}
		waiter.resetWait()
	}
}

func (s *service) doAcquireLock(waiter *waiter, tableID uint64, rows [][]byte, txnID []byte, options LockOptions) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.locks[unsafeByteSliceToString(txnID)]; !ok {
		s.locks[unsafeByteSliceToString(txnID)] = make(map[uint64][][]byte)
	}

	if _, ok := s.seqStorage[tableID]; !ok {
		s.seqStorage[tableID] = newBtreeBasedStorage()
	}

	if options.granularity == Row {
		return s.acquireRowLock(waiter, tableID, rows[0], txnID, options.mode)
	} else {
		return s.acquireRangeLock(waiter, tableID, rows[0], rows[1], txnID, options.mode)
	}
}

func (s *service) acquireRowLock(w *waiter, tableID uint64, row []byte, txnID []byte, mode LockMode) (bool, error) {
	txnKey := unsafeByteSliceToString(txnID)

	storage := s.seqStorage[tableID]
	key, lock, ok := storage.Seek(row)
	if ok && (bytes.Equal(key, row) || lock.isLockRangeEnd()) {
		if err := lock.waiter.add(w); err != nil {
			return false, err
		}

		s.waiterAdded(txnID, txnKey, w)
		return false, nil
	} else {
		addRowLock(storage, txnID, row, w, mode)
	}
	s.locks[txnKey][tableID] = append(s.locks[txnKey][tableID], row)
	return true, nil
}

func (s *service) waiterAdded(txnID []byte, txnKey string, w *waiter) {
	// add txn's waiting lock
	s.waitingLocks[txnKey] = w
	// add to deadlock detector to check dead lock
	if err := s.deadlockDetector.check(txnID); err != nil {
		panic(err)
	}
}

func (s *service) acquireRangeLock(w *waiter, tableID uint64, start, end, txnID []byte, mode LockMode) (bool, error) {
	if bytes.Compare(start, end) >= 0 {
		return false, moerr.NewInternalErrorNoCtx("lock error: start[%v] is greater than end[%v]", start, end)
	}
	txnKey := unsafeByteSliceToString(txnID)
	storage := s.seqStorage[tableID]
	key, lock, ok := storage.Seek(start)
	if ok && bytes.Compare(key, end) <= 0 {
		if err := lock.waiter.add(w); err != nil {
			return false, err
		}
		s.waiterAdded(txnID, txnKey, w)
		return false, nil
	} else {
		addRangeLock(storage, txnID, start, end, w, mode)
	}
	s.locks[txnKey][tableID] = append(s.locks[txnKey][tableID], start, end)
	return true, nil
}

func (s *service) fetchTxnWaitingList(txnID []byte, waiters *waiters) bool {
	txnKey := unsafeByteSliceToString(txnID)

	s.mu.RLock()
	defer s.mu.RUnlock()
	for tableID, lockKeys := range s.locks[txnKey] {
		s := s.seqStorage[tableID]
		for _, lockKey := range lockKeys {
			if lock, ok := s.Get(lockKey); ok {
				hasDeadLock := false
				lock.waiter.waiters.IterTxns(func(id []byte) bool {
					if !waiters.add(id) {
						hasDeadLock = true
						return false
					}
					return true
				})
				if hasDeadLock {
					return false
				}
			}
		}
	}
	return true
}

func (s *service) abortDeadlockTxn(txnID []byte) {
	txnKey := unsafeByteSliceToString(txnID)
	s.mu.RLock()
	defer s.mu.RUnlock()

	// if a deadlock occurs, then the transaction must have a waiter that is
	// currently waiting and has been blocked inside the Lock.
	w, ok := s.waitingLocks[txnKey]
	if !ok {
		panic("BUG: abort a non-waiting txn")
	}
	if !w.notify(ErrDeadlockDetectorClosed) {
		panic("BUG: can not notify deadlock failed")
	}
}

func addRowLock(storage LockStorage, txnID []byte, row []byte, waiter *waiter, mode LockMode) {
	lock := newRowLock(txnID, mode)
	lock.waiter = waiter
	storage.Add(row, lock)
}

func addRangeLock(storage LockStorage, txnID, start, end []byte, waiter *waiter, mode LockMode) {
	lock1, lock2 := newRangeLock(txnID, mode)
	lock1.waiter = waiter
	lock2.waiter = waiter
	storage.Add(start, lock1)
	storage.Add(end, lock2)
}

func unsafeByteSliceToString(key []byte) string {
	return *(*string)(unsafe.Pointer(&key))
}
