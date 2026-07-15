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

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

type localLockTableProxy struct {
	remote    lockTable
	serviceID string
	logger    *log.MOLogger

	mu struct {
		sync.RWMutex
		holders                  map[string]*sharedOps // key: row
		currentHolder            map[string][]byte
		pendingRemoteHolders     map[string][]byte
		pendingLastHolderUnlocks map[string]struct{}
	}
}

func newLockTableProxy(
	serviceID string,
	remote lockTable,
	logger *log.MOLogger,
) lockTable {
	lp := &localLockTableProxy{
		remote:    remote,
		serviceID: serviceID,
		logger:    logger,
	}
	lp.mu.holders = make(map[string]*sharedOps)
	lp.mu.currentHolder = make(map[string][]byte)
	lp.mu.pendingRemoteHolders = make(map[string][]byte)
	lp.mu.pendingLastHolderUnlocks = make(map[string]struct{})
	return lp
}

func (lp *localLockTableProxy) lock(
	ctx context.Context,
	txn *activeTxn,
	rows [][]byte,
	options LockOptions,
	cb func(pb.Result, error)) {
	if options.Mode != pb.LockMode_Shared {
		lp.remote.lock(ctx, txn, rows, options, cb)
		return
	}

	if len(rows) != 1 {
		panic("local lock table proxy can only support on single row")
	}

	lp.mu.Lock()
	key := util.UnsafeBytesToString(rows[0])
	if _, ok := lp.mu.pendingLastHolderUnlocks[key]; ok {
		// The owner may already have applied the last-holder Unlock even
		// though its response was lost. Until the retry confirms that
		// transition, the stale local holder cannot safely represent a remote
		// shared lock. Route new sharers through the owner so they observe any
		// exclusive owner acquired in the meantime.
		lp.mu.Unlock()
		lp.remote.lock(ctx, txn, rows, options, cb)
		return
	}
	v, ok := lp.mu.holders[key]
	if !ok {
		v = &sharedOps{
			rows: rows,
			bind: lp.getBind(),
		}
		lp.mu.holders[key] = v
	}

	first := v.isEmpty()
	r := v.result
	w := v.add(
		lp.serviceID,
		txn,
		cb,
		lp.hasRemoteHolderLocked(key),
		lp.logger)
	if w != nil {
		defer w.close("localLockTableProxy lock", lp.logger)
	}
	lp.mu.Unlock()

	if first {
		lp.remote.lock(
			ctx,
			txn,
			rows,
			options,
			func(r pb.Result, e error) {
				lp.mu.Lock()
				defer lp.mu.Unlock()
				if e == nil {
					lp.mu.currentHolder[key] = v.txns[0].txnID
				}
				v.done(r, e, lp.logger)
			})
		return
	}

	defer func() {
		bind := lp.getBind()
		err := txn.lockAdded(bind.Group, bind, rows, lp.logger)
		cb(r, err)
	}()

	// wait first done
	if w != nil {
		w.wait(ctx, lp.logger)
		return
	}

}

func (lp *localLockTableProxy) unlock(
	txn *activeTxn,
	ls *cowSlice,
	commitTS timestamp.Timestamp,
	mutations ...pb.ExtraMutation) {
	_ = lp.unlockWithContext(context.Background(), txn, ls, commitTS, mutations...)
}

func (lp *localLockTableProxy) unlockWithContext(
	ctx context.Context,
	txn *activeTxn,
	ls *cowSlice,
	commitTS timestamp.Timestamp,
	_ ...pb.ExtraMutation) error {
	rows := ls.slice()
	defer rows.unref()

	type holderUpdate struct {
		row                      string
		replaceWith              []byte
		keepRemoteHolder         bool
		clearPendingRemoteHolder bool
		nextPendingRemoteHolder  []byte
	}

	skipped := 0
	n := rows.len()
	var remoteMutations []pb.ExtraMutation
	var updates []holderUpdate
	lp.mu.Lock()
	defer lp.mu.Unlock()
	rows.iter(func(key []byte) bool {
		row := util.UnsafeBytesToString(key)
		if v, ok := lp.mu.holders[row]; ok {
			isHolder := lp.isRemoteHolderLocked(row, txn.txnID)
			replacement, found := v.lastExcept(txn)
			if !found {
				return true
			}

			// not the holder, no need to unlock
			if !isHolder {
				// A previous holder may have been replaced at the owner before its
				// response was lost. Only the replacement selected by that
				// unacknowledged handoff can be a remote holder too. Its ordinary
				// unlock must conditionally transfer that remote holder instead of
				// being skipped, otherwise the owner retains a finished txn until
				// orphan cleanup.
				if lp.isPendingRemoteHolderLocked(row, txn.txnID) {
					remoteMutations = append(remoteMutations, pb.ExtraMutation{
						Key:       key,
						ReplaceTo: replacement,
					})
					nextPending := replacement
					if bytes.Equal(nextPending, lp.mu.currentHolder[row]) {
						nextPending = nil
					}
					updates = append(updates, holderUpdate{
						row:                      row,
						replaceWith:              lp.mu.currentHolder[row],
						keepRemoteHolder:         true,
						clearPendingRemoteHolder: len(nextPending) == 0,
						nextPendingRemoteHolder:  nextPending,
					})
					return true
				}
				skipped++
				if n > 1 {
					remoteMutations = append(remoteMutations,
						pb.ExtraMutation{
							Key:  key,
							Skip: true,
						})
				}
				updates = append(updates, holderUpdate{
					row:              row,
					replaceWith:      lp.mu.currentHolder[row],
					keepRemoteHolder: true,
				})
				return true
			}

			// Do not publish the replacement locally until the owner has
			// acknowledged ReplaceTo. If the resolver context expires, the
			// source txn stays active and a retry can safely converge both sides.
			// Always send a mutation for the remote holder. Besides carrying a
			// replacement, an empty ReplaceTo is an explicit proxy-handoff
			// marker: if the response is lost after the owner released this last
			// holder, a retry must not fail when another transaction already owns
			// the row.
			remoteReplacement := replacement
			if pending, ok := lp.mu.pendingRemoteHolders[row]; ok {
				// A response-lost handoff has already selected the only remote
				// representative that can be safely retried. Later local sharers
				// must stay behind that representative until the owner confirms a
				// transition; otherwise the proxy and owner can publish different
				// holders for the same shared lock.
				remoteReplacement = pending
			} else if _, ok := lp.mu.pendingLastHolderUnlocks[row]; ok {
				// Preserve the already-selected empty replacement across retries.
				remoteReplacement = nil
			} else if len(remoteReplacement) > 0 {
				lp.mu.pendingRemoteHolders[row] = remoteReplacement
			} else {
				lp.mu.pendingLastHolderUnlocks[row] = struct{}{}
			}
			remoteMutations = append(remoteMutations,
				pb.ExtraMutation{
					Key:       key,
					Skip:      false,
					ReplaceTo: remoteReplacement,
				})
			updates = append(updates, holderUpdate{
				row:                      row,
				replaceWith:              remoteReplacement,
				clearPendingRemoteHolder: true,
			})
		}
		return true
	})

	// all skipped
	var err error
	if unlocker, ok := lp.remote.(contextUnlocker); ok {
		if skipped != rows.len() {
			err = unlocker.unlockWithContext(ctx, txn, ls, commitTS, remoteMutations...)
		}
	} else if skipped != rows.len() {
		lp.remote.unlock(txn, ls, commitTS, remoteMutations...)
	}
	if err != nil {
		return err
	}

	for _, update := range updates {
		v := lp.mu.holders[update.row]
		if v == nil || !v.remove(txn) {
			continue
		}
		if update.keepRemoteHolder || len(update.replaceWith) > 0 {
			lp.mu.currentHolder[update.row] = update.replaceWith
		} else {
			delete(lp.mu.currentHolder, update.row)
		}
		if update.clearPendingRemoteHolder {
			delete(lp.mu.pendingRemoteHolders, update.row)
			delete(lp.mu.pendingLastHolderUnlocks, update.row)
		} else if len(update.nextPendingRemoteHolder) > 0 {
			lp.mu.pendingRemoteHolders[update.row] = update.nextPendingRemoteHolder
		}
	}
	return nil
}

func (lp *localLockTableProxy) isRemoteHolderLocked(
	row string,
	txnID []byte) bool {
	v, ok := lp.mu.currentHolder[row]
	if !ok {
		return false
	}
	return bytes.Equal(txnID, v)
}

func (lp *localLockTableProxy) hasRemoteHolderLocked(row string) bool {
	_, ok := lp.mu.currentHolder[row]
	return ok
}

func (lp *localLockTableProxy) isPendingRemoteHolderLocked(
	row string,
	txnID []byte,
) bool {
	pending, ok := lp.mu.pendingRemoteHolders[row]
	return ok && bytes.Equal(pending, txnID)
}

func (lp *localLockTableProxy) getLock(
	key []byte,
	txn pb.WaitTxn,
	fn func(Lock)) {
	lp.remote.getLock(key, txn, fn)
}

func (lp *localLockTableProxy) getLockHolder(ctx context.Context, key []byte) (pb.WaitTxn, bool, error) {
	return lp.remote.getLockHolder(ctx, key)
}

func (lp *localLockTableProxy) getBind() pb.LockTable {
	return lp.remote.getBind()
}

func (lp *localLockTableProxy) close(reason closeReason) {
	lp.remote.close(reason)
}

type sharedOps struct {
	bind    pb.LockTable
	result  pb.Result
	rows    [][]byte
	txns    []*activeTxn
	waiters []*waiter
	cbs     []func(pb.Result, error)
}

func (s *sharedOps) done(
	r pb.Result,
	err error,
	logger *log.MOLogger,
) {
	for idx, cb := range s.cbs {
		cb(r, err)
		if idx > 0 {
			s.waiters[idx].notify(notifyValue{}, logger)
		}
		s.cbs[idx] = nil
		s.waiters[idx] = nil
	}
	if err != nil {
		s.txns = s.txns[:0]
		s.cbs = s.cbs[:0]
		s.waiters = s.waiters[:0]
	} else {
		s.result = r
	}
}

func (s *sharedOps) isEmpty() bool {
	return len(s.txns) == 0
}

func (s *sharedOps) lastExcept(txn *activeTxn) ([]byte, bool) {
	found := false
	var replacement []byte
	for idx := len(s.txns) - 1; idx >= 0; idx-- {
		if s.txns[idx] == txn {
			found = true
			continue
		}
		if replacement == nil {
			replacement = s.txns[idx].txnID
		}
	}
	return replacement, found
}

func (s *sharedOps) add(
	serviceID string,
	txn *activeTxn,
	cb func(pb.Result, error),
	hasHolder bool,
	logger *log.MOLogger,
) *waiter {
	var w *waiter
	if !hasHolder && !s.isEmpty() {
		v := txn.toWaitTxn(serviceID, true)
		w = acquireWaiter(v, "share ops add", logger)
		w.setStatus(blocking)
	}
	if hasHolder {
		cb = nil
	}

	s.txns = append(s.txns, txn)
	s.cbs = append(s.cbs, cb)
	s.waiters = append(s.waiters, w)
	return w
}

func (s *sharedOps) remove(txn *activeTxn) bool {
	found := false
	newTxns := s.txns[:0]
	newCbs := s.cbs[:0]
	newWaiters := s.waiters[:0]
	for idx, v := range s.txns {
		if v != txn {
			if bytes.Equal(v.txnID, txn.txnID) {
				panic("fatal")
			}
			newTxns = append(newTxns, v)
			newWaiters = append(newWaiters, s.waiters[idx])
			newCbs = append(newCbs, s.cbs[idx])
		} else {
			found = true
		}
	}
	s.txns = newTxns
	s.waiters = newWaiters
	s.cbs = newCbs
	return found
}
