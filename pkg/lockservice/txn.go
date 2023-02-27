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
	"sync"
)

var (
	txnPool = sync.Pool{New: func() any {
		return &activeTxn{holdLocks: make(map[uint64]*cowSlice)}
	}}
)

// activeTxn one goroutine write, multi goroutine read
type activeTxn struct {
	sync.RWMutex
	txnID         []byte
	txnKey        string
	fsp           *fixedSlicePool
	blockedWaiter *waiter
	holdLocks     map[uint64]*cowSlice
}

func newActiveTxn(
	txnID []byte,
	txnKey string,
	fsp *fixedSlicePool) *activeTxn {
	txn := txnPool.Get().(*activeTxn)
	txn.Lock()
	defer txn.Unlock()
	txn.txnID = txnID
	txn.txnKey = txnKey
	txn.fsp = fsp
	return txn
}

func (txn *activeTxn) lockAdded(
	table uint64,
	locks [][]byte) {
	txn.Lock()
	defer txn.Unlock()

	v, ok := txn.holdLocks[table]
	if ok {
		v.append(locks)
		return
	}
	txn.holdLocks[table] = newCowSlice(txn.fsp, locks)
}

func (txn *activeTxn) close(lockTableFunc func(uint64) lockTable) {
	txn.Lock()
	defer txn.Unlock()

	for table, cs := range txn.holdLocks {
		l := lockTableFunc(table)
		// TODO(fagongzi): use a deadline context, and retry if has a error
		l.unlock(context.TODO(), cs)
		cs.close()
		delete(txn.holdLocks, table)
	}
	txn.txnID = nil
	txn.txnKey = ""
	txn.blockedWaiter = nil
	txnPool.Put(txn)
}

func (txn *activeTxn) abort(txnID []byte) {
	txn.RLock()
	defer txn.RUnlock()

	// txn already closed
	if !bytes.Equal(txn.txnID, txnID) {
		return
	}

	// if a deadlock occurs, then the transaction must have a waiter that is
	// currently waiting and has been blocked inside the Lock.
	if txn.blockedWaiter == nil {
		panic("BUG: abort a non-waiting txn")
	}

	if !txn.blockedWaiter.notify(ErrDeadlockDetectorClosed) {
		panic("BUG: can not notify deadlock failed")
	}
}

func (txn *activeTxn) fetchWhoWaitingMe(
	txnID []byte,
	waiters *waiters,
	lockTableFunc func(uint64) lockTable) bool {
	txn.RLock()
	defer txn.RUnlock()

	// txn already closed
	if !bytes.Equal(txn.txnID, txnID) {
		return true
	}
	for table, cs := range txn.holdLocks {
		l := lockTableFunc(table)
		locks := cs.slice()
		hasDeadLock := false
		locks.iter(func(lockKey []byte) bool {
			// TODO(fagongzi): use a deadline context, and retry if has a error
			if lock, ok := l.getLock(context.TODO(), lockKey); ok {
				lock.waiter.waiters.iter(func(id []byte) bool {
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
			return true
		})
		locks.unref()
		if hasDeadLock {
			return false
		}
	}
	return true
}

func (txn *activeTxn) setBlocked(w *waiter) {
	txn.Lock()
	defer txn.Unlock()
	txn.blockedWaiter = w
}
