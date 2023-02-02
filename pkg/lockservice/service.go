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
	"unsafe"
)

type activeTxnHolder interface {
	getActiveTxn(txnID []byte, create bool) *activeTxn
	deleteActiveTxn(txnID []byte) *activeTxn
}

type mapBasedTxnHolder struct {
	fsp *fixedSlicePool
	mu  struct {
		sync.RWMutex
		activeTxns map[string]*activeTxn
	}
}

func newMapBasedTxnHandler(fsp *fixedSlicePool) activeTxnHolder {
	h := &mapBasedTxnHolder{}
	h.fsp = fsp
	h.mu.activeTxns = make(map[string]*activeTxn)
	return h
}

func (h *mapBasedTxnHolder) getActiveTxn(
	txnID []byte,
	create bool) *activeTxn {
	txnKey := unsafeByteSliceToString(txnID)
	h.mu.RLock()
	if v, ok := h.mu.activeTxns[txnKey]; ok {
		h.mu.RUnlock()
		return v
	}
	h.mu.RUnlock()
	if !create {
		return nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	txn := newActiveTxn(txnID, txnKey, h.fsp)
	h.mu.activeTxns[txnKey] = txn
	return txn
}

func (h *mapBasedTxnHolder) deleteActiveTxn(txnID []byte) *activeTxn {
	txnKey := unsafeByteSliceToString(txnID)
	h.mu.Lock()
	defer h.mu.Unlock()
	v, ok := h.mu.activeTxns[txnKey]
	if ok {
		delete(h.mu.activeTxns, txnKey)
	}
	return v
}

type service struct {
	tables sync.Map // tableid -> *locktable
	// activeTxn        sync.Map // txnid -> *activeTxns
	activeTxns       activeTxnHolder
	fsp              *fixedSlicePool
	deadlockDetector *detector
}

func NewLockService() LockService {
	l := &service{
		// FIXME: config
		fsp: newFixedSlicePool(1024 * 1024),
	}
	l.activeTxns = newMapBasedTxnHandler(l.fsp)
	l.deadlockDetector = newDeadlockDetector(l.fetchTxnWaitingList,
		l.abortDeadlockTxn)
	return l
}

func (s *service) Lock(
	ctx context.Context,
	tableID uint64,
	rows [][]byte,
	txnID []byte,
	options LockOptions) (bool, error) {
	txn := s.activeTxns.getActiveTxn(txnID, true)
	l := s.getLockTable(tableID)
	if err := l.lock(ctx, txn, rows, options); err != nil {
		return false, err
	}
	return true, nil
}

func (s *service) Unlock(txnID []byte) error {
	txn := s.activeTxns.deleteActiveTxn(txnID)
	if txn == nil {
		return nil
	}
	txn.close(s.getLockTable)
	// The deadlock detector will hold the deadlocked transaction that is aborted
	// to avoid the situation where the deadlock detection is interfered with by
	// the abort transaction. When a transaction is unlocked, the deadlock detector
	// needs to be notified to release memory.
	s.deadlockDetector.txnClosed(txnID)
	return nil
}

func (s *service) Close() error {
	s.deadlockDetector.close()
	return nil
}

func (s *service) fetchTxnWaitingList(txnID []byte, waiters *waiters) bool {
	txn := s.activeTxns.getActiveTxn(txnID, false)
	// the active txn closed
	if txn == nil {
		return true
	}

	return txn.fetchWhoWaitingMe(txnID, waiters, s.getLockTable)
}

func (s *service) abortDeadlockTxn(txnID []byte) {
	txn := s.activeTxns.getActiveTxn(txnID, false)
	// the active txn closed
	if txn == nil {
		return
	}
	txn.abort(txnID)
}

func (s *service) getLockTable(tableID uint64) *lockTable {
	if v, ok := s.tables.Load(tableID); ok {
		return v.(*lockTable)
	}

	l := newLockTable(tableID, s.deadlockDetector)
	if v, loaded := s.tables.LoadOrStore(tableID, l); loaded {
		return v.(*lockTable)
	}
	return l
}

func unsafeByteSliceToString(key []byte) string {
	return *(*string)(unsafe.Pointer(&key))
}
