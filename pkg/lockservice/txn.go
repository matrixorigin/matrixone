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
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/util"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

var (
	txnPool = sync.Pool{New: func() any {
		return &activeTxn{holdLocks: make(map[uint64]*cowSlice)}
	}}
)

// activeTxn one goroutine write, multi goroutine read
type activeTxn struct {
	sync.RWMutex
	txnID          []byte
	txnKey         string
	fsp            *fixedSlicePool
	blockedWaiters []*waiter
	holdLocks      map[uint64]*cowSlice
	remoteService  string
	deadlockFound  bool
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

func (txn *activeTxn) lockRemoved(
	serviceID string,
	table uint64,
	removedLocks map[string]struct{}) {
	v, ok := txn.holdLocks[table]
	if !ok {
		return
	}
	newV := newCowSlice(txn.fsp, nil)
	s := v.slice()
	defer s.unref()
	s.iter(func(v []byte) bool {
		if _, ok := removedLocks[util.UnsafeBytesToString(v)]; !ok {
			newV.append([][]byte{v})
		}
		return true
	})
	v.close()
	txn.holdLocks[table] = newV
}

func (txn *activeTxn) lockAdded(
	serviceID string,
	table uint64,
	locks [][]byte,
	w *waiter) {

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

	defer logTxnLockAdded(serviceID, txn, locks, w)
	v, ok := txn.holdLocks[table]
	if ok {
		v.append(locks)
		return
	}
	txn.holdLocks[table] = newCowSlice(txn.fsp, locks)
}

func (txn *activeTxn) close(
	serviceID string,
	txnID []byte,
	commitTS timestamp.Timestamp,
	lockTableFunc func(uint64) (lockTable, error)) error {
	logTxnReadyToClose(serviceID, txn)
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
			serviceID,
			txn,
			table)
		l.unlock(txn, cs, commitTS)
		logTxnUnlockTableCompleted(
			serviceID,
			txn,
			table,
			cs)
		cs.close()
		delete(txn.holdLocks, table)
	}
	txn.txnID = nil
	txn.txnKey = ""
	txn.blockedWaiters = txn.blockedWaiters[:0]
	txn.remoteService = ""
	txn.deadlockFound = false
	txnPool.Put(txn)
	return nil
}

func (txn *activeTxn) abort(serviceID string, waitTxn pb.WaitTxn) {
	// abort is called by deadlock detection, so it is not necessary to lock
	txn.Lock()
	defer txn.Unlock()

	logAbortDeadLock(serviceID, waitTxn, txn)

	// txn already closed
	if !bytes.Equal(txn.txnID, waitTxn.TxnID) {
		return
	}

	txn.deadlockFound = true
	if len(txn.blockedWaiters) == 0 {
		return
	}
	for _, w := range txn.blockedWaiters {
		w.notify(serviceID, notifyValue{err: ErrDeadLockDetected})
	}
}

func (txn *activeTxn) clearBlocked(txnID []byte) {
	txn.blockedWaiters = txn.blockedWaiters[:0]
}

func (txn *activeTxn) setBlocked(txnID []byte, w *waiter) {
	if w == nil {
		panic("invalid waiter")
	}
	if !w.casStatus("", ready, blocking) {
		panic(fmt.Sprintf("invalid waiter status %d, %s", w.getStatus(), w))
	}
	txn.blockedWaiters = append(txn.blockedWaiters, w)
}

func (txn *activeTxn) isRemoteLocked() bool {
	return txn.remoteService != ""
}

// ============================================================================================================================
// the above methods are called in the Lock and Unlock processes, where txn holds the mutex at the beginning of the process.
// The following methods are called concurrently in processes that are concurrent with the Lock and Unlock processes.
// ============================================================================================================================

func (txn *activeTxn) fetchWhoWaitingMe(
	serviceID string,
	txnID []byte,
	holder activeTxnHolder,
	waiters func(pb.WaitTxn) bool,
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
					lock.waiter.waiters.iter(func(w *waiter) bool {
						wt := w.waitTxn
						if len(wt.TxnID) == 0 {
							if txn := holder.getActiveTxn(w.txnID, false, ""); txn != nil {
								wt = txn.toWaitTxn(serviceID, bytes.Equal(txn.txnID, w.txnID))
							}
						}
						if len(wt.TxnID) == 0 {
							return true
						}
						hasDeadLock = !waiters(wt)
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

func (txn *activeTxn) toWaitTxn(serviceID string, locked bool) pb.WaitTxn {
	if !locked {
		txn.RLock()
		defer txn.RUnlock()
	}

	v := txn.remoteService
	if v == "" {
		v = serviceID
	}
	return pb.WaitTxn{TxnID: txn.txnID, CreatedOn: v}
}

func (txn *activeTxn) getID() []byte {
	txn.RLock()
	defer txn.RUnlock()
	return txn.txnID
}
