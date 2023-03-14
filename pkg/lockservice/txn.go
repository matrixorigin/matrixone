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
	remoteService string
}

func newActiveTxn(
	txnID []byte,
	txnKey string,
	fsp *fixedSlicePool,
	remoteService string) *activeTxn {
	txn := txnPool.Get().(*activeTxn)
	txn.Lock()
	defer txn.Unlock()
	txn.txnID = txnID
	txn.txnKey = txnKey
	txn.fsp = fsp
	txn.remoteService = remoteService
	return txn
}

func (txn *activeTxn) lockAdded(
	table uint64,
	locks [][]byte,
	locked bool) {

	// only in the lockservice node where the transaction was
	// initiated will it be holds all locks. A remote transaction
	// will only holds locks on the current locktable.
	//
	// Let's consider the correctness of this and assume that transaction
	// t1 is successful in locking against row1, think about the following
	// cases:
	// 1. t1 receives the response and has saved row1, then
	//    everything is fine
	// 2. t1 does not receive a response from remote, deadlock
	//    detection may not be detected, but no problem, t1 will
	//    roll the transaction due to timeout.
	// 3. When t1 remote lock succeeds and saved the lock information
	//    between the deadlock detection module to query, it will miss
	//    the lock information. We use mutex to solve it.

	if !locked {
		txn.Lock()
		defer txn.Unlock()
	}
	defer logTxnLockAdded(txn, locks)
	v, ok := txn.holdLocks[table]
	if ok {
		v.append(locks)
		return
	}
	txn.holdLocks[table] = newCowSlice(txn.fsp, locks)
}

func (txn *activeTxn) close(
	txnID []byte,
	lockTableFunc func(uint64) (lockTable, error)) error {
	txn.Lock()
	defer txn.Unlock()
	if !bytes.Equal(txn.txnID, txnID) {
		return nil
	}

	logTxnReadyToClose(txn)
	// TODO(fagongzi): parallel unlock
	for table, cs := range txn.holdLocks {
		l, err := lockTableFunc(table)
		if err != nil {
			// if a remote transaction, then the corresponding locktable should be local
			// and cannot return an error.
			//
			// or a local transaction holds a lock on remote lock table, but can not get the remote
			// LockTable, it is a bug.
			panic(err)
		}

		logTxnUnlockTable(
			txn,
			table)
		l.unlock(txn, cs)
		logTxnUnlockTableCompleted(
			txn,
			table,
			cs)
		cs.close()
		delete(txn.holdLocks, table)
	}
	txn.txnID = nil
	txn.txnKey = ""
	txn.blockedWaiter = nil
	txn.remoteService = ""
	txnPool.Put(txn)
	return nil
}

func (txn *activeTxn) abort(txnID []byte) {
	txn.RLock()
	defer txn.RUnlock()

	// txn already closed
	if !bytes.Equal(txn.txnID, txnID) {
		return
	}

	if txn.blockedWaiter == nil {
		return
	}
	txn.blockedWaiter.notify(ErrDeadLockDetected)
}

func (txn *activeTxn) fetchWhoWaitingMe(
	txnID []byte,
	waiters func([]byte) bool,
	lockTableFunc func(uint64) (lockTable, error)) bool {
	txn.RLock()
	defer txn.RUnlock()

	// txn already closed
	if !bytes.Equal(txn.txnID, txnID) {
		return true
	}

	// if this is a remote transaction, meaning that all the information is in the
	// remote, we need to execute the logic.
	if txn.isRemoteLocked() {
		panic("can not fetch waiting txn on remote txn")
	}

	for table, cs := range txn.holdLocks {
		l, err := lockTableFunc(table)
		if err != nil {
			// if a remote transaction, then the corresponding locktable should be local
			// and cannot return an error.
			//
			// or a local transaction holds a lock on remote lock table, but can not get
			// the remote LockTable, it is a bug.
			panic(err)
		}

		locks := cs.slice()
		hasDeadLock := false
		locks.iter(func(lockKey []byte) bool {
			l.getLock(
				txnID,
				lockKey,
				func(lock Lock) {
					lock.waiter.waiters.iter(func(id []byte) bool {
						hasDeadLock = !waiters(id)
						return !hasDeadLock
					})
				})
			return !hasDeadLock
		})
		locks.unref()
		if hasDeadLock {
			return false
		}
	}
	return true
}

func (txn *activeTxn) setBlocked(txnID []byte, w *waiter) {
	txn.Lock()
	defer txn.Unlock()
	// txn already closed
	if !bytes.Equal(txn.txnID, txnID) {
		panic("invalid set Blocked")
	}

	txn.blockedWaiter = w
	if w != nil {
		return
	}
}

func (txn *activeTxn) isRemoteLocked() bool {
	return txn.remoteService != ""
}

func (txn *activeTxn) getRemoteService() string {
	txn.RLock()
	defer txn.RUnlock()
	if !txn.isRemoteLocked() {
		return ""
	}
	return txn.remoteService
}
