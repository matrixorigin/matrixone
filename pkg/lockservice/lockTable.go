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
	if ok := l.acquireLock(ctx, tableID, rows, txnID, options); ok {
		return ok, nil
	} else {
		return false, l.Unlock(ctx, txnID)
	}
}

func (l *lockTable) Unlock(ctx context.Context, txnID []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for tableID, keys := range l.locks[unsafeByteSliceToString(txnID)] {
		storage := l.seqStorage[tableID]
		for _, key := range keys {
			if lock, ok := storage.Get(key); ok {
				lock.waiter.close()
				storage.Delete(key)
				/*
					if w := lock.waiter.close(); w != nil {
						lock.waiter = w
						lock.txnID = w.txnID
						storage.Add(key, lock)
					} else {
						storage.Delete(key)
					}
				*/
			}
		}
		delete(l.locks[unsafeByteSliceToString(txnID)], tableID)
	}
	return nil
}

func (l *lockTable) acquireLock(ctx context.Context, tableID uint64, rows [][]byte, txnID []byte, options LockOptions) bool {
	l.mu.Lock()
	if _, ok := l.locks[unsafeByteSliceToString(txnID)]; !ok {
		l.locks[unsafeByteSliceToString(txnID)] = make(map[uint64][][]byte)
	}

	if _, ok := l.seqStorage[tableID]; !ok {
		l.seqStorage[tableID] = newBtreeBasedStorage()
	}
	l.mu.Unlock()

	if options.granularity == Row {
		return l.acquireRowLock(ctx, tableID, rows[0], txnID, options)
	} else {
		return l.acquireRangeLock(tableID, rows[0], rows[1], txnID, options)
	}
}

func (l *lockTable) acquireRowLock(ctx context.Context, tableID uint64, row []byte, txnID []byte, options LockOptions) bool {
	waiter := acquireWaiter(txnID)
	l.mu.Lock()
	storage := l.seqStorage[tableID]
	key, lock, ok := storage.Seek(row)
	if ok && (bytes.Equal(key, row) || lock.isLockRangeEnd()) {
		if err := lock.add(waiter); err != nil {
			l.mu.Unlock()
			return false
		}
		l.mu.Unlock()
		if err := waiter.wait(ctx); err != nil {
			return false
		}
		l.mu.Lock()
		addRowLock(storage, txnID, row, waiter, options)
	} else {
		addRowLock(storage, txnID, row, waiter, options)
	}
	l.locks[unsafeByteSliceToString(txnID)][tableID] = append(l.locks[unsafeByteSliceToString(txnID)][tableID], row)
	l.mu.Unlock()
	return true
}

func (l *lockTable) acquireRangeLock(tableID uint64, start, end []byte, txnID []byte, options LockOptions) bool {
	return false
}

func addRowLock(storage LockStorage, txnID []byte, row []byte, waiter *waiter, options LockOptions) {
	lock := newRowLock(txnID, options.mode)
	lock.waiter = waiter
	storage.Add(row, lock)
}

func unsafeByteSliceToString(key []byte) string {
	return *(*string)(unsafe.Pointer(&key))
}
