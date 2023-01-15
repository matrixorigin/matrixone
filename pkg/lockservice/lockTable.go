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

type lockTable struct {
	mu sync.Mutex

	// txnID -> tableID -> lockKeys
	//locks sync.Map
	locks map[string]map[uint64][][]byte
	// tableID -> LockStorage
	//seqStorage [32]sync.Map
	seqStorage map[uint64]LockStorage
}

func NewLockService() LockService {
	return &lockTable{
		locks:      make(map[string]map[uint64][][]byte),
		seqStorage: make(map[uint64]LockStorage),
	}
}

func (l *lockTable) Lock(ctx context.Context, tableID uint64, rows [][]byte, txnID []byte, options LockOptions) (bool, error) {
	if err := l.acquireLock(ctx, tableID, rows, txnID, options); err != nil {
		return false, err
	}
	return true, nil
}

func (l *lockTable) Unlock(txnID []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for tableID, keys := range l.locks[unsafeByteSliceToString(txnID)] {
		storage := l.seqStorage[tableID]
		for _, key := range keys {
			if lock, ok := storage.Get(key); ok {
				if lock.isLockRow() || lock.isLockRangeEnd() {
					lock.waiter.close()
				}
				storage.Delete(key)
			}
		}
		delete(l.locks[unsafeByteSliceToString(txnID)], tableID)
	}
	return nil
}

func (l *lockTable) acquireLock(ctx context.Context, tableID uint64, rows [][]byte, txnID []byte, options LockOptions) error {
	waiter := acquireWaiter(txnID)
	for {
		ok, err := l.doAcquireLock(waiter, tableID, rows, txnID, options)
		if err != nil {
			waiter.close()
			return err
		}
		if ok {
			return nil
		}

		if err := waiter.wait(ctx); err != nil {
			waiter.close()
			return err
		}
		waiter.resetWait()
	}
}

func (l *lockTable) doAcquireLock(waiter *waiter, tableID uint64, rows [][]byte, txnID []byte, options LockOptions) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.locks[unsafeByteSliceToString(txnID)]; !ok {
		l.locks[unsafeByteSliceToString(txnID)] = make(map[uint64][][]byte)
	}

	if _, ok := l.seqStorage[tableID]; !ok {
		l.seqStorage[tableID] = newBtreeBasedStorage()
	}

	if options.granularity == Row {
		return l.acquireRowLock(waiter, tableID, rows[0], txnID, options.mode)
	} else {
		return l.acquireRangeLock(waiter, tableID, rows[0], rows[1], txnID, options.mode)
	}
}

func (l *lockTable) acquireRowLock(waiter *waiter, tableID uint64, row []byte, txnID []byte, mode LockMode) (bool, error) {
	storage := l.seqStorage[tableID]
	key, lock, ok := storage.Seek(row)
	if ok && (bytes.Equal(key, row) || lock.isLockRangeEnd()) {
		if err := lock.add(waiter); err != nil {
			return false, err
		}
		return false, nil
	} else {
		addRowLock(storage, txnID, row, waiter, mode)
	}
	l.locks[unsafeByteSliceToString(txnID)][tableID] = append(l.locks[unsafeByteSliceToString(txnID)][tableID], row)
	return true, nil
}

func (l *lockTable) acquireRangeLock(waiter *waiter, tableID uint64, start, end, txnID []byte, mode LockMode) (bool, error) {
	if bytes.Compare(start, end) >= 0 {
		return false, moerr.NewInternalErrorNoCtx("lock error: start[%v] is greater than end[%v]", start, end)
	}
	storage := l.seqStorage[tableID]
	key, lock, ok := storage.Seek(start)
	if ok && bytes.Compare(key, end) <= 0 {
		if err := lock.add(waiter); err != nil {
			return false, err
		}
		return false, nil
	} else {
		addRangeLock(storage, txnID, start, end, waiter, mode)
	}
	l.locks[unsafeByteSliceToString(txnID)][tableID] = append(l.locks[unsafeByteSliceToString(txnID)][tableID], start, end)
	return true, nil
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
