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

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/panjf2000/ants/v2"
)

var (
	parallelUnlockTables = 2
)

type tableLockHolder struct {
	tableKeys  map[uint64]*cowSlice
	tableBinds map[uint64]pb.LockTable
	// tableBindIntents records bind versions touched before a lock attempt
	// finishes, so bind-change fencing also covers failed in-flight attempts.
	tableBindIntents map[uint64]pb.LockTable
}

// activeTxn one goroutine write, multi goroutine read
type activeTxn struct {
	*sync.RWMutex
	txnID          []byte
	txnKey         string
	fsp            *fixedSlicePool
	blockedWaiters []*waiter
	lockHolders    map[uint32]*tableLockHolder
	remoteService  string
	deadlockFound  bool
	bindChanged    bool

	// test-only hook: called before lockAdded; return non-nil to abort
	beforeLockAdded func(txnID []byte, locks [][]byte) error
}

type contextUnlocker interface {
	unlockWithContext(
		context.Context,
		*activeTxn,
		*cowSlice,
		timestamp.Timestamp,
		...pb.ExtraMutation,
	) error
}

func newActiveTxn(
	txnID []byte,
	txnKey string,
	fsp *fixedSlicePool,
	remoteService string) *activeTxn {
	txn := reuse.Alloc[activeTxn](nil)
	txn.Lock()
	defer txn.Unlock()
	txn.txnID = txnID
	txn.txnKey = txnKey
	txn.fsp = fsp
	txn.remoteService = remoteService
	return txn
}

func (txn activeTxn) TypeName() string {
	return "lockservice.activeTxn"
}

func (txn *activeTxn) lockRemoved(
	group uint32,
	table uint64,
	removedLocks map[string]struct{}) {
	h := txn.getHoldLocksLocked(group)
	v, ok := h.tableKeys[table]
	if !ok {
		return
	}
	newV, _ := newCowSlice(txn.fsp, nil)
	s := v.slice()
	defer s.unref()
	s.iter(func(v []byte) bool {
		if _, ok := removedLocks[util.UnsafeBytesToString(v)]; !ok {
			newV.append([][]byte{v})
		}
		return true
	})
	v.close()
	h.tableKeys[table] = newV
}

func (txn *activeTxn) lockAdded(
	group uint32,
	bind pb.LockTable,
	locks [][]byte,
	logger *log.MOLogger,
) error {

	if txn.beforeLockAdded != nil {
		if err := txn.beforeLockAdded(txn.txnID, locks); err != nil {
			return err
		}
	}

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

	defer logTxnLockAdded(logger, txn, locks)
	h := txn.getHoldLocksLocked(group)
	v, ok := h.tableKeys[bind.Table]
	var err error
	if ok {
		return v.append(locks)
	}
	cs, err := newCowSlice(txn.fsp, locks)
	if err != nil {
		return err
	}
	h.tableKeys[bind.Table] = cs
	h.tableBinds[bind.Table] = bind
	return nil
}

func (txn *activeTxn) lockTableBindTouched(bind pb.LockTable) {
	h := txn.getHoldLocksLocked(bind.Group)
	if _, ok := h.tableBindIntents[bind.Table]; !ok {
		h.tableBindIntents[bind.Table] = bind
	}
}

func (txn *activeTxn) close(
	txnID []byte,
	commitTS timestamp.Timestamp,
	lockTableFunc func(uint32, uint64) (lockTable, error),
	logger *log.MOLogger,
	mutations ...pb.ExtraMutation,
) error {
	return txn.closeWithContext(
		context.Background(),
		txnID,
		commitTS,
		lockTableFunc,
		logger,
		mutations...,
	)
}

func (txn *activeTxn) closeWithContext(
	ctx context.Context,
	txnID []byte,
	commitTS timestamp.Timestamp,
	lockTableFunc func(uint32, uint64) (lockTable, error),
	logger *log.MOLogger,
	mutations ...pb.ExtraMutation,
) error {
	return txn.closeWithContextInternal(
		ctx,
		txnID,
		commitTS,
		lockTableFunc,
		logger,
		true,
		mutations...,
	)
}

func (txn *activeTxn) closeWithoutFreeWithContext(
	ctx context.Context,
	txnID []byte,
	commitTS timestamp.Timestamp,
	lockTableFunc func(uint32, uint64) (lockTable, error),
	logger *log.MOLogger,
	mutations ...pb.ExtraMutation,
) error {
	return txn.closeWithContextInternal(
		ctx,
		txnID,
		commitTS,
		lockTableFunc,
		logger,
		false,
		mutations...,
	)
}

func (txn *activeTxn) closeWithContextInternal(
	ctx context.Context,
	txnID []byte,
	commitTS timestamp.Timestamp,
	lockTableFunc func(uint32, uint64) (lockTable, error),
	logger *log.MOLogger,
	release bool,
	mutations ...pb.ExtraMutation,
) error {
	logTxnReadyToClose(logger, txn)

	// cancel all blocked waiters
	txn.cancelBlocks(logger)

	isRemoteTable := txn.remoteService != ""
	canSkipTable := func(isRemoteTable bool, l lockTable) bool {
		if isRemoteTable {
			if _, ok := l.(*remoteLockTable); ok {
				return true
			}
		}
		return false
	}

	n := len(txn.lockHolders)
	// Unknown-commit cleanup can be retried after a bounded remote RPC attempt
	// expires. Keep that path sequential and remove every table only after its
	// unlock succeeds, so a retry never replays a successful local unlock.
	// Normal transaction cleanup retains its existing parallel behavior.
	parallelUnlock := release && n > parallelUnlockTables && ctx.Done() == nil
	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex
	v2.TxnUnlockTableTotalHistogram.Observe(float64(n))
	for group, h := range txn.lockHolders {
		for table, cs := range h.tableKeys {
			l, err := lockTableFunc(group, table)
			if err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return ctxErr
				}
				// if a remote transaction, then the corresponding locktable should be local
				// and cannot return an error.
				//
				// or a local transaction holds a lock on remote lock table, but can not get the remote
				// LockTable, it is a bug.
				panic(err)
			}
			if l == nil || canSkipTable(isRemoteTable, l) {
				if !release {
					txn.removeClosedLockTable(group, table, cs)
				}
				continue
			}

			fn := func(table uint64, cs *cowSlice, l lockTable) func() {
				return func() {
					logTxnUnlockTable(
						logger,
						txn,
						table,
					)
					var err error
					if unlocker, ok := l.(contextUnlocker); ok {
						err = unlocker.unlockWithContext(ctx, txn, cs, commitTS, mutations...)
					} else {
						l.unlock(txn, cs, commitTS, mutations...)
					}
					if err != nil {
						errMu.Lock()
						if firstErr == nil {
							firstErr = err
						}
						errMu.Unlock()
					} else if !release {
						txn.removeClosedLockTable(group, table, cs)
					}
					logTxnUnlockTableCompleted(
						logger,
						txn,
						table,
						cs,
					)
					if parallelUnlock {
						wg.Done()
					}
				}
			}

			if parallelUnlock {
				wg.Add(1)
				ants.Submit(fn(table, cs, l))
			} else {
				fn(table, cs, l)()
			}
		}
	}

	if parallelUnlock {
		wg.Wait()
	}

	if release {
		reuse.Free(txn, nil)
	}
	return firstErr
}

// removeClosedLockTable forgets a successfully released table while an
// unknown-commit cleanup is still retryable. activeTxn is held by the caller.
func (txn *activeTxn) removeClosedLockTable(
	group uint32,
	table uint64,
	cs *cowSlice,
) {
	h, ok := txn.lockHolders[group]
	if !ok || h.tableKeys[table] != cs {
		return
	}
	delete(h.tableKeys, table)
	delete(h.tableBinds, table)
	delete(h.tableBindIntents, table)
	cs.close()
	if len(h.tableKeys) == 0 && len(h.tableBinds) == 0 &&
		len(h.tableBindIntents) == 0 {
		delete(txn.lockHolders, group)
	}
}

func (txn *activeTxn) reset() {
	for g, h := range txn.lockHolders {
		for table, cs := range h.tableKeys {
			cs.close()
			delete(h.tableKeys, table)
		}
		for table := range h.tableBinds {
			delete(h.tableBinds, table)
		}
		for table := range h.tableBindIntents {
			delete(h.tableBindIntents, table)
		}
		delete(txn.lockHolders, g)
	}

	txn.txnID = nil
	txn.txnKey = ""
	txn.blockedWaiters = txn.blockedWaiters[:0]
	txn.remoteService = ""
	txn.deadlockFound = false
	txn.bindChanged = false
}

func (txn *activeTxn) abort(
	waitTxn pb.WaitTxn,
	err error,
	logger *log.MOLogger,
) {
	// abort is called by deadlock detection, so it is not necessary to lock
	txn.Lock()
	defer txn.Unlock()

	logAbortDeadLock(logger, waitTxn, txn)

	// txn already closed
	if !bytes.Equal(txn.txnID, waitTxn.TxnID) {
		return
	}

	txn.deadlockFound = true
	if len(txn.blockedWaiters) == 0 {
		return
	}
	for _, w := range txn.blockedWaiters {
		w.notify(notifyValue{err: err}, logger)
	}
}

func (txn *activeTxn) fenceByBindChanged(bind pb.LockTable, logger *log.MOLogger) bool {
	txn.Lock()
	defer txn.Unlock()

	h, ok := txn.lockHolders[bind.Group]
	if !ok {
		return false
	}
	actual, actualOK := h.tableBinds[bind.Table]
	intent, intentOK := h.tableBindIntents[bind.Table]
	if (!actualOK || !actual.Changed(bind)) &&
		(!intentOK || !intent.Changed(bind)) {
		return false
	}

	txn.bindChanged = true
	for _, w := range txn.blockedWaiters {
		w.notify(notifyValue{err: ErrLockTableBindChanged}, logger)
	}
	return true
}

func (txn *activeTxn) cancelBlocks(
	logger *log.MOLogger,
) {
	for _, w := range txn.blockedWaiters {
		w.notify(notifyValue{err: ErrTxnNotFound}, logger)
		w.close("cancelBlocks", logger)
	}
}

func (txn *activeTxn) clearBlocked(w *waiter, logger *log.MOLogger) {
	newBlockedWaiters := txn.blockedWaiters[:0]
	for _, v := range txn.blockedWaiters {
		if v != w {
			newBlockedWaiters = append(newBlockedWaiters, v)
		} else {
			w.close("clearBlocked", logger)
		}
	}
	txn.blockedWaiters = newBlockedWaiters
}

func (txn *activeTxn) closeBlockWaiters(logger *log.MOLogger) {
	for _, w := range txn.blockedWaiters {
		w.close("closeBlockWaiters", logger)
	}
	txn.blockedWaiters = txn.blockedWaiters[:0]
}

func (txn *activeTxn) setBlocked(
	w *waiter,
	logger *log.MOLogger,
) {
	if w == nil {
		panic("invalid waiter")
	}
	if !w.casStatus(ready, blocking, logger) {
		panic(fmt.Sprintf("invalid waiter status %d, %s", w.getStatus(), w))
	}
	w.ref("activeTxn setBlocked", logger)
	txn.blockedWaiters = append(txn.blockedWaiters, w)
}

func (txn *activeTxn) isRemoteLocked() bool {
	return txn.remoteService != ""
}

func (txn *activeTxn) incLockTableRef(m map[uint32]map[uint64]uint64, serviceID string) {
	txn.RLock()
	defer txn.RUnlock()
	for _, h := range txn.lockHolders {
		for _, l := range h.tableBinds {
			if serviceID == l.ServiceID {
				if _, ok := m[l.Group]; !ok {
					m[l.Group] = make(map[uint64]uint64, 1024)
				}
				m[l.Group][l.Table]++
			}
		}
	}
}

// ============================================================================================================================
// the above methods are called in the Lock and Unlock processes, where txn holds the mutex at the beginning of the process.
// The following methods are called concurrently in processes that are concurrent with the Lock and Unlock processes.
// ============================================================================================================================

func (txn *activeTxn) fetchWhoWaitingMe(
	ctx context.Context,
	serviceID string,
	txnID []byte,
	waiters func(pb.WaitTxn, string) bool,
	lockTableFunc func(context.Context, uint32, uint64) (lockTable, error)) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	txn.RLock()
	// txn already closed
	if !bytes.Equal(txn.txnID, txnID) {
		txn.RUnlock()
		return true, nil
	}
	// if this is a remote transaction, meaning that all the information is in the
	// remote, we need to execute the logic.
	if txn.isRemoteLocked() {
		txn.RUnlock()
		panic("can not fetch waiting txn on remote txn")
	}

	groups := make([]uint32, 0, len(txn.lockHolders))
	tables := make([]uint64, 0, len(txn.lockHolders))
	lockKeys := make([]*fixedSlice, 0, len(txn.lockHolders))
	for g, m := range txn.lockHolders {
		for table, cs := range m.tableKeys {
			tables = append(tables, table)
			lockKeys = append(lockKeys, cs.slice())
			groups = append(groups, g)
		}
	}

	wt := txn.toWaitTxn(serviceID, true)
	txn.RUnlock()

	defer func() {
		for _, cs := range lockKeys {
			cs.unref()
		}
	}()

	for idx, table := range tables {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		l, err := lockTableFunc(ctx, groups[idx], table)
		if err != nil {
			// if a remote transaction, then the corresponding locktable should be local
			// and cannot return an error.
			//
			// or a local transaction holds a lock on remote lock table, but can not get
			// the remote LockTable, it is a bug.
			return false, err
		}
		if l == nil {
			continue
		}

		locks := lockKeys[idx]
		hasDeadLock := false
		var fetchErr error
		waiterAddress := l.getBind().ServiceID
		locks.iter(func(lockKey []byte) bool {
			if err := ctx.Err(); err != nil {
				fetchErr = err
				return false
			}
			if err := l.getLock(
				ctx,
				lockKey,
				wt,
				func(lock Lock) {
					lock.waiters.iter(func(w *waiter) bool {
						// Completed or already-notified waiters can remain in the
						// lock queue until the holder releases the lock. They no
						// longer represent wait-for edges and must not participate
						// in deadlock detection.
						if !w.isBlocking() {
							return true
						}
						hasDeadLock = !waiters(w.txn, waiterAddress)
						return !hasDeadLock
					})
				}); err != nil {
				fetchErr = err
				return false
			}
			return !hasDeadLock
		})
		if fetchErr != nil {
			return false, fetchErr
		}

		if err := ctx.Err(); err != nil {
			return false, err
		}
		if hasDeadLock {
			return false, nil
		}
	}
	return true, nil
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

func (txn *activeTxn) getHoldLocksLocked(group uint32) *tableLockHolder {
	h, ok := txn.lockHolders[group]
	if ok {
		return h
	}
	h = &tableLockHolder{
		tableKeys:        make(map[uint64]*cowSlice),
		tableBinds:       make(map[uint64]pb.LockTable),
		tableBindIntents: make(map[uint64]pb.LockTable),
	}
	txn.lockHolders[group] = h
	return h
}
