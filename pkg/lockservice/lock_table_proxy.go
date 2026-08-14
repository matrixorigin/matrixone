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
	"errors"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

var errRetryUncachedProxyLock error = moerr.NewInternalErrorNoCtx(
	"retry uncached proxy lock")

type localLockTableProxy struct {
	remote            lockTable
	serviceID         string
	protocolServiceID string
	logger            *log.MOLogger

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
	protocolServiceID string,
	remote lockTable,
	logger *log.MOLogger,
) lockTable {
	lp := &localLockTableProxy{
		remote:            remote,
		serviceID:         serviceID,
		protocolServiceID: protocolServiceID,
		logger:            logger,
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
	// The proxy cache represents exactly one Shared row. Range and multi-row
	// requests must retain the remote lock table's full merge and replacement
	// semantics; routing them through the singleton cache would either panic or
	// lose transaction-bookkeeping replacement.
	if options.Mode != pb.LockMode_Shared || len(rows) != 1 {
		lp.remote.lock(ctx, txn, rows, options, cb)
		return
	}
	if !supportsLockProtocolV20(lp.protocolServiceID) {
		// An existing proxy can outlive a process-wide protocol transition. Stop
		// admitting new cache-only sharers while v20 is unavailable; a direct
		// owner lock/unlock remains compatible with pre-v20 peers and is tracked by
		// remoteUnlockRequired in the transaction ledger.
		lp.remote.lock(ctx, txn, rows, options, cb)
		return
	}

	lp.mu.Lock()
	if err := ctx.Err(); err != nil {
		lp.mu.Unlock()
		cb(pb.Result{}, err)
		return
	}
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
	hasRemoteHolder := lp.hasRemoteHolderLocked(key)
	if !ok {
		v = &sharedOps{}
		lp.mu.holders[key] = v
	} else if !hasRemoteHolder && !v.remoteInFlight {
		// A completed generation can temporarily retain pending followers after
		// its last admitted holder has released the physical lock. Those followers
		// finish against the detached generation and retry at the owner; a new
		// caller must start a fresh generation instead of waiting for a completion
		// notification that has already happened.
		v = &sharedOps{}
		lp.mu.holders[key] = v
	}
	// Mirror localLockTable's re-entrant Shared behavior: once this transaction
	// is a granted local proxy holder, the key is already present in both proxy
	// and transaction bookkeeping. Appending it again only consumes fixed-slice
	// capacity and can manufacture a false lock-upgrade threshold.
	containsTxn, admitted := v.admissionState(txn)
	if hasRemoteHolder && admitted {
		r := v.result
		lp.mu.Unlock()
		cb(r, nil)
		return
	}
	if hasRemoteHolder && !containsTxn {
		// Make transaction bookkeeping durable before publishing this caller as
		// a handoff candidate. Unlock takes the same txn -> proxy lock order, so
		// holding lp.mu across this bounded local update makes admission atomic
		// with representative replacement.
		bind := lp.getBind()
		err := txn.remoteLockAdded(
			bind.Group,
			bind,
			rows,
			options.LockOptions,
			lp.logger,
		)
		if err == nil {
			v.addAdmitted(txn)
			r := v.result
			lp.mu.Unlock()
			cb(r, nil)
			return
		}
		lp.mu.Unlock()
		if moerr.IsMoErrCode(err, moerr.ErrLockNeedUpgrade) {
			// This transaction cannot retain another exact proxy membership, but
			// the negotiated owner snapshot and transaction-scoped remote unlock
			// make a direct, uncached Shared acquisition fully representable.
			lp.remote.lock(ctx, txn, rows, options, cb)
			return
		}
		cb(pb.Result{}, err)
		return
	}
	if containsTxn {
		// This transaction already has a pending admission. It may own the first
		// physical-holder RPC or be an independent follower whose origin ledger is
		// not committed yet. Joining as another holder would make a failed primary
		// admission visible as success; subscribe to this transaction's admission
		// outcome instead.
		txnID := bytes.Clone(txn.txnID)
		txnGeneration := txn.generation
		w := v.addReentrantWaiter(lp.serviceID, txn, lp.logger)
		lp.mu.Unlock()

		defer w.close("localLockTableProxy reentrant lock", lp.logger)
		txn.Unlock()
		value := w.wait(ctx, lp.logger)
		txn.Lock()
		// Completion removes all subscribers before notifying them. Cancellation
		// wins the same waiter-status race but leaves this subscriber registered,
		// so remove it unconditionally before close can return the waiter to its
		// pool. This also covers a transaction generation changing while it waits.
		lp.mu.Lock()
		v.removeReentrantWaiter(w)
		r := v.result
		lp.mu.Unlock()
		if txnGeneration != txn.generation {
			cb(pb.Result{}, ErrTxnNotFound)
			return
		}
		if terminalErr := txn.terminalLockErrorLocked(txnID); terminalErr != nil {
			cb(pb.Result{}, terminalErr)
			return
		}
		if errors.Is(value.err, errRetryUncachedProxyLock) {
			lp.remote.lock(ctx, txn, rows, options, cb)
			return
		}
		if value.err != nil {
			cb(pb.Result{}, value.err)
			return
		}
		cb(r, nil)
		return
	}

	first := v.isEmpty()
	w := v.addPending(
		lp.serviceID,
		txn,
		cb,
		lp.logger)
	if first {
		v.remoteInFlight = true
	}
	if w != nil {
		defer w.close("localLockTableProxy lock", lp.logger)
	}
	lp.mu.Unlock()

	if first {
		options.requireOwnerLocalWaitSnapshot = true
		lp.remote.lock(
			ctx,
			txn,
			rows,
			options,
			func(r pb.Result, e error) {
				lp.mu.Lock()
				defer lp.mu.Unlock()
				if errors.Is(e, errRetryUncachedProxyLock) ||
					(e == nil && !r.NewLockAdd) {
					// The transaction already owned this key at the physical owner.
					// Its effective lock may be Exclusive or a covering range; likewise,
					// a capacity-skewed origin may have only a bounded cleanup route.
					// Neither can safely become a Shared-cache representative. Complete
					// same-transaction callers, but make independent queued transactions
					// retry against the owner under their own identity.
					v.doneUncached(r, lp.logger)
					delete(lp.mu.holders, key)
					return
				}
				if e == nil {
					lp.mu.currentHolder[key] = v.firstPendingTxn().txnID
				}
				v.done(r, e, lp.logger)
				if e != nil && v.isEmpty() {
					delete(lp.mu.holders, key)
				}
			})
		return
	}

	// wait first done
	if w != nil {
		txnID := bytes.Clone(txn.txnID)
		txnGeneration := txn.generation
		txn.Unlock()
		value := w.wait(ctx, lp.logger)
		txn.Lock()
		if txnGeneration != txn.generation {
			lp.mu.Lock()
			lp.removeTxnLocked(key, v, txn, ErrTxnNotFound)
			lp.mu.Unlock()
			cb(pb.Result{}, ErrTxnNotFound)
			return
		}
		if terminalErr := txn.terminalLockErrorLocked(txnID); terminalErr != nil {
			lp.mu.Lock()
			lp.removeTxnLocked(key, v, txn, terminalErr)
			lp.mu.Unlock()
			cb(pb.Result{}, terminalErr)
			return
		}
		if errors.Is(value.err, errRetryUncachedProxyLock) {
			// This follower was never checked under its own transaction ID.
			// Bypass the cache for this attempt instead of serially rebuilding
			// generations that may each prove uncacheable for the same reason.
			lp.remote.lock(ctx, txn, rows, options, cb)
			return
		}
		if value.err != nil {
			lp.mu.Lock()
			lp.removeTxnLocked(key, v, txn, value.err)
			lp.mu.Unlock()
			cb(pb.Result{}, value.err)
			return
		}
	}

	lp.mu.Lock()
	currentGeneration, retained := lp.mu.holders[key]
	if !retained || currentGeneration != v ||
		!lp.hasRemoteHolderLocked(key) {
		// The last admitted representative released the owner lock before this
		// notified follower could commit its local ledger. It was never eligible
		// for that handoff, so remove it from the completed generation and retry
		// directly under its own transaction identity.
		lp.removeTxnLocked(key, v, txn, errRetryUncachedProxyLock)
		lp.mu.Unlock()
		lp.remote.lock(ctx, txn, rows, options, cb)
		return
	}

	bind := lp.getBind()
	err := txn.remoteLockAdded(
		bind.Group,
		bind,
		rows,
		options.LockOptions,
		lp.logger,
	)
	if err != nil {
		admissionErr := err
		if moerr.IsMoErrCode(err, moerr.ErrLockNeedUpgrade) {
			admissionErr = errRetryUncachedProxyLock
		}
		lp.removeTxnLocked(key, v, txn, admissionErr)
		lp.mu.Unlock()
		if moerr.IsMoErrCode(err, moerr.ErrLockNeedUpgrade) {
			lp.remote.lock(ctx, txn, rows, options, cb)
			return
		}
		cb(pb.Result{}, err)
		return
	}
	if !v.admit(txn, lp.logger) {
		panic("BUG: proxy follower disappeared during admission")
	}
	r := v.result
	lp.mu.Unlock()
	cb(r, nil)
}

// removeTxnLocked rolls back one proxy admission without disturbing a newer
// generation for the same row. A completed generation with no physical holder
// is detached even if other notified followers have not resumed yet: they keep
// their sharedOps pointer and retry directly, while new callers can make
// progress through a fresh generation.
func (lp *localLockTableProxy) removeTxnLocked(
	key string,
	v *sharedOps,
	txn *activeTxn,
	admissionErr error,
) {
	v.failPending(txn, admissionErr, lp.logger)
	current, ok := lp.mu.holders[key]
	if !ok || current != v {
		return
	}
	if v.isEmpty() ||
		(!v.remoteInFlight && !lp.hasRemoteHolderLocked(key)) {
		delete(lp.mu.holders, key)
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
	bind := lp.getBind()
	forceRemoteUnlock := txn.isRemoteUnlockRequiredLocked(bind.Group, bind.Table)
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
				if n > 1 && !forceRemoteUnlock {
					// With only proxy ownership, the owner has no transaction
					// ledger for this local sharer and Skip protects the remote
					// representative. Once this txn also acquired directly, the
					// owner ledger is authoritative: a duplicate key can be a real
					// direct holder and must be released rather than skipped.
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
		if skipped != rows.len() || forceRemoteUnlock {
			err = unlocker.unlockWithContext(ctx, txn, ls, commitTS, remoteMutations...)
		}
	} else if skipped != rows.len() || forceRemoteUnlock {
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
		if v.isEmpty() ||
			(!v.remoteInFlight && !lp.hasRemoteHolderLocked(update.row)) {
			delete(lp.mu.holders, update.row)
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
	ctx context.Context,
	key []byte,
	txn pb.WaitTxn,
	fn func(Lock)) error {
	return lp.remote.getLock(ctx, key, txn, fn)
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
	result         pb.Result
	txns           []*activeTxn // durable origin ledger; eligible for handoff
	pending        []proxyPendingTxn
	remoteInFlight bool
}

// proxyPendingTxn keeps owner-RPC completion separate from cache admission.
// The first transaction owns the physical RPC; independent followers share
// that result but become handoff-eligible only after their own origin ledger is
// durable. Same-transaction callers subscribe to that per-transaction outcome.
type proxyPendingTxn struct {
	txn              *activeTxn
	waiter           *waiter
	cb               func(pb.Result, error)
	reentrantWaiters []*waiter
}

func (s *sharedOps) done(
	r pb.Result,
	err error,
	logger *log.MOLogger,
) {
	s.remoteInFlight = false
	if err != nil {
		for idx := range s.pending {
			s.pending[idx].complete(r, err, logger)
		}
		clear(s.pending)
		s.pending = nil
		return
	}
	if len(s.pending) == 0 {
		panic("BUG: proxy owner completion without a pending transaction")
	}

	s.result = r
	// remoteLockTable commits the first transaction's ledger before invoking
	// this callback. Publish only that transaction before waking followers;
	// each follower completes its own admission later.
	first := &s.pending[0]
	s.txns = append(s.txns, first.txn)
	first.complete(r, nil, logger)
	for idx := 1; idx < len(s.pending); idx++ {
		s.pending[idx].completePrimary(r, nil, logger)
	}
	s.removePendingAt(0)
}

// doneUncached completes a successful owner call that cannot safely seed the
// Shared proxy cache. The first transaction and its re-entrant callers already
// own the physical key, but independent queued transactions must retry at the
// owner so its effective lock mode remains authoritative.
func (s *sharedOps) doneUncached(
	r pb.Result,
	logger *log.MOLogger,
) {
	s.remoteInFlight = false
	s.result = r
	for idx := range s.pending {
		err := errRetryUncachedProxyLock
		if idx == 0 {
			err = nil
		}
		s.pending[idx].complete(r, err, logger)
	}
	clear(s.pending)
	s.pending = nil
}

func (s *sharedOps) addReentrantWaiter(
	serviceID string,
	txn *activeTxn,
	logger *log.MOLogger,
) *waiter {
	w := acquireWaiter(
		txn.toWaitTxn(serviceID, true),
		"share ops reentrant add",
		logger,
	)
	w.setStatus(blocking)
	for idx := range s.pending {
		if s.pending[idx].txn != txn {
			continue
		}
		s.pending[idx].reentrantWaiters = append(
			s.pending[idx].reentrantWaiters, w)
		return w
	}
	w.close("sharedOps missing pending admission", logger)
	panic("BUG: reentrant proxy waiter without a pending admission")
}

func (s *sharedOps) removeReentrantWaiter(target *waiter) {
	for idx := range s.pending {
		waiters := s.pending[idx].reentrantWaiters
		for waiterIdx, w := range waiters {
			if w != target {
				continue
			}
			copy(waiters[waiterIdx:], waiters[waiterIdx+1:])
			last := len(waiters) - 1
			waiters[last] = nil
			s.pending[idx].reentrantWaiters = waiters[:last]
			return
		}
	}
}

func (s *sharedOps) isEmpty() bool {
	return len(s.txns) == 0 && len(s.pending) == 0
}

func (s *sharedOps) admissionState(txn *activeTxn) (bool, bool) {
	for _, holder := range s.txns {
		if holder == txn {
			return true, true
		}
	}
	for idx := range s.pending {
		if s.pending[idx].txn == txn {
			return true, false
		}
	}
	return false, false
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

func (s *sharedOps) addPending(
	serviceID string,
	txn *activeTxn,
	cb func(pb.Result, error),
	logger *log.MOLogger,
) *waiter {
	var w *waiter
	if !s.isEmpty() {
		v := txn.toWaitTxn(serviceID, true)
		w = acquireWaiter(v, "share ops add", logger)
		w.setStatus(blocking)
		// The waiting goroutine owns its callback. sharedOps.done only publishes
		// the first remote result through the waiter, so completion and caller
		// cancellation have one waiter-status linearization point.
		cb = nil
	}
	s.pending = append(s.pending, proxyPendingTxn{
		txn:    txn,
		waiter: w,
		cb:     cb,
	})
	return w
}

func (s *sharedOps) addAdmitted(txn *activeTxn) {
	s.txns = append(s.txns, txn)
}

func (s *sharedOps) admit(txn *activeTxn, logger *log.MOLogger) bool {
	for idx := range s.pending {
		if s.pending[idx].txn != txn {
			continue
		}
		s.txns = append(s.txns, txn)
		s.pending[idx].completeReentrant(nil, logger)
		s.removePendingAt(idx)
		return true
	}
	return false
}

func (s *sharedOps) failPending(
	txn *activeTxn,
	err error,
	logger *log.MOLogger,
) bool {
	for idx := range s.pending {
		if s.pending[idx].txn != txn {
			continue
		}
		s.pending[idx].completeReentrant(err, logger)
		s.removePendingAt(idx)
		return true
	}
	return false
}

func (s *sharedOps) firstPendingTxn() *activeTxn {
	if len(s.pending) == 0 {
		panic("BUG: proxy owner RPC without a pending transaction")
	}
	return s.pending[0].txn
}

func (s *sharedOps) removePendingAt(idx int) {
	copy(s.pending[idx:], s.pending[idx+1:])
	last := len(s.pending) - 1
	s.pending[last] = proxyPendingTxn{}
	s.pending = s.pending[:last]
	if len(s.pending) == 0 {
		// Pending callback/waiter capacity is useless in the steady cached state;
		// release it so each row retains only admitted transaction pointers.
		s.pending = nil
	}
}

func (s *sharedOps) remove(txn *activeTxn) bool {
	found := false
	oldLen := len(s.txns)
	newTxns := s.txns[:0]
	for _, v := range s.txns {
		if v != txn {
			if bytes.Equal(v.txnID, txn.txnID) {
				panic("fatal")
			}
			newTxns = append(newTxns, v)
		} else {
			found = true
		}
	}
	clear(s.txns[len(newTxns):oldLen])
	s.txns = newTxns
	return found
}

func (p *proxyPendingTxn) complete(
	r pb.Result,
	err error,
	logger *log.MOLogger,
) {
	p.completePrimary(r, err, logger)
	p.completeReentrant(err, logger)
}

func (p *proxyPendingTxn) completePrimary(
	r pb.Result,
	err error,
	logger *log.MOLogger,
) {
	if p.cb != nil {
		p.cb(r, err)
	} else if p.waiter != nil {
		p.waiter.notify(notifyValue{err: err}, logger)
	}
	p.cb = nil
	p.waiter = nil
}

func (p *proxyPendingTxn) completeReentrant(
	err error,
	logger *log.MOLogger,
) {
	for idx, w := range p.reentrantWaiters {
		w.notify(notifyValue{err: err}, logger)
		p.reentrantWaiters[idx] = nil
	}
	p.reentrantWaiters = nil
}
