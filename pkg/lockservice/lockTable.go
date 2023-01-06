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

	deadlock struct {
		// Send the txnID to checkTxn channel when a new lock is acquired by a txn.
		// checkDeadlock will update entries in dependentQueue related with received txnID.
		checkTxn chan []byte
		// Send a signal to rebuildQueue channel when Unlock is called.
		// refreshDependentQueue will rebuild the queue based on current txn dependent relationship.
		rebuildQueue chan struct{}

		// dependentQueue
	}
}

func NewLockService() LockService {
	return &lockTable{
		locks:        make(map[string]map[uint64][][]byte),
		btrees:       make(map[uint64]LockStorage),
		stateChanged: make(chan struct{}),

		deadlock: struct {
			checkTxn     chan []byte
			rebuildQueue chan struct{}
		}{checkTxn: make(chan []byte), rebuildQueue: make(chan struct{})},
	}
}

func (l *lockTable) Lock(ctx context.Context, tableID uint64, rows [][]byte, txnID []byte, options LockOptions) (ok bool, err error) {
	defer func(txnID []byte) {
		if ok == true {
			l.deadlock.checkTxn <- txnID
		}
	}(txnID)

	if ok = l.tryLock(tableID, rows, txnID, options); ok {
		return ok, nil
	}

	for {
		select {
		case <-ctx.Done():
			return false, l.Unlock(ctx, txnID)
		case <-l.stateChanged:
			if ok = l.tryLock(tableID, rows, txnID, options); ok {
				return ok, nil
			}
		}
	}
}

func (l *lockTable) Unlock(ctx context.Context, txnID []byte) error {
	l.mu.Lock()
	for tableID, keys := range l.locks[string(txnID)] {
		for _, key := range keys {
			l.btrees[tableID].Delete(key)
		}
		if l.btrees[tableID].Len() == 0 {
			delete(l.btrees, tableID)
		}
	}
	delete(l.locks, string(txnID))
	defer l.mu.Unlock()
	l.notifyStateChanged(true)
	return nil
}

func (l *lockTable) tryLock(tableID uint64, rows [][]byte, txnID []byte, options LockOptions) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.locks[string(txnID)]; !ok {
		l.locks[string(txnID)] = make(map[uint64][][]byte)
	}

	if _, ok := l.btrees[tableID]; !ok {
		l.btrees[tableID] = newBtreeBasedStorage()
	}

	if options.granularity == Row {
		return l.tryRowLock(tableID, rows[0], txnID, options)
	} else {
		return l.tryRangeLock(tableID, rows[0], rows[1], txnID, options)
	}
}

func (l *lockTable) tryRowLock(tableID uint64, row []byte, txnID []byte, options LockOptions) bool {

	return false
}

func (l *lockTable) tryRangeLock(tableID uint64, start, end []byte, txnID []byte, options LockOptions) bool {

	return false
}

func (l *lockTable) notifyStateChanged(rebuildDependentQueue bool) {
	l.stateChanged <- struct{}{}
	if rebuildDependentQueue {
		l.deadlock.rebuildQueue <- struct{}{}
	}
}

func (l *lockTable) checkDeadlock() {
	for {
		select {
		case txnID := <-l.deadlock.checkTxn:
			l.updateTxnDependency(txnID)
		case <-l.deadlock.rebuildQueue:
			l.rebuildDependentQueue()
		}
	}
}

func (l *lockTable) updateTxnDependency(txnID []byte) {
}

func (l *lockTable) rebuildDependentQueue() {
}
