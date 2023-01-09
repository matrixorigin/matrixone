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
)

type lockTable struct {
	mu sync.Mutex

	// txnID -> tableID -> lockKeys
	//locks sync.Map
	locks map[string]map[uint64][][]byte
	// tableID -> LockStorage
	//btrees [32]sync.Map
	btrees map[uint64]LockStorage

	stateChanged chan struct{}
}

func NewLockService() LockService {
	return &lockTable{
		locks:        make(map[string]map[uint64][][]byte),
		btrees:       make(map[uint64]LockStorage),
		stateChanged: make(chan struct{}),
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
	for tableID, keys := range l.locks[string(txnID)] {
		for _, key := range keys {
			if lock, ok := l.btrees[tableID].Get(key); ok {
				if waiter := lock.waiter.close(); waiter != nil {
					lock.waiter = waiter
					l.btrees[tableID].Add(key, lock)
				} else {
					l.btrees[tableID].Delete(key)
				}
			}
		}
		delete(l.locks[string(txnID)], tableID)
	}
	//delete(l.locks, string(txnID))
	return nil
}

func (l *lockTable) acquireLock(ctx context.Context, tableID uint64, rows [][]byte, txnID []byte, options LockOptions) bool {
	l.mu.Lock()
	if _, ok := l.locks[string(txnID)]; !ok {
		l.locks[string(txnID)] = make(map[uint64][][]byte)
	}

	if _, ok := l.btrees[tableID]; !ok {
		l.btrees[tableID] = newBtreeBasedStorage()
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
	key, lock, ok := l.btrees[tableID].Seek(row)
	if ok && (bytes.Equal(key, row) || lock.isLockRangeEnd()) {
		err := lock.add(waiter)
		l.mu.Unlock()
		if err != nil {
			return false
		}
		if err := waiter.wait(ctx); err != nil {
			return false
		}
	} else {
		l.addRowLock(txnID, tableID, row, waiter)
		l.mu.Unlock()
	}
	l.mu.Lock()
	l.locks[string(txnID)][tableID] = append(l.locks[string(txnID)][tableID], row)
	l.mu.Unlock()
	return true
}

func (l *lockTable) acquireRangeLock(tableID uint64, start, end []byte, txnID []byte, options LockOptions) bool {
	return false
}

func (l *lockTable) addRowLock(txnID []byte, tableID uint64, row []byte, waiter *waiter) {
	lock := newRowLock(txnID, Exclusive)
	lock.waiter = waiter
	l.btrees[tableID].Add(row, lock)
}
