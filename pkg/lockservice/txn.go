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
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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

const maxRemoteUnlockBatchSize = 64

type tableLockHolder struct {
	tableKeys  map[uint64]*cowSlice
	tableBinds map[uint64]pb.LockTable
	// Keep uncommon ownership metadata behind one lazy pointer. A holder is
	// allocated for every transaction/group, while these maps are needed only
	// for remote, uncertain, Shared or sharded ownership. Keeping them inline
	// doubles the common holder allocation size and regresses Lock+Unlock.
	extra *tableLockHolderExtra
	// tableBindIntents is common admission bookkeeping and remains inline.
	tableBindIntents map[uint64]pb.LockTable
}

type tableLockHolderExtra struct {
	nonCoarsenableTables map[uint64]struct{}
	// coarsenedTables records authoritative owner tables whose exact Exclusive
	// ownership has already crossed the cumulative key budget and committed as
	// one conservative range. Keeping this state is what makes coarsening
	// monotonic: later compatible batches widen that range directly instead of
	// rebuilding another budget-sized set of row locks first.
	coarsenedTables map[uint64]struct{}
	// ownerLocalWaitSnapshots records tables whose physical owner explicitly
	// negotiated the owner-local transaction wait-for snapshot protocol. Only
	// these tables may use compact origin bookkeeping for deadlock traversal.
	ownerLocalWaitSnapshots map[uint64]struct{}
	// remoteUnlockRequired records tables on which this origin transaction may
	// own locks acquired through a direct owner RPC. localLockTableProxy must not
	// suppress that table-level txnID unlock merely because every retained probe
	// key belongs to its singleton Shared cache.
	remoteUnlockRequired map[uint64]struct{}
	// batchUnlockTables records successful locks whose physical owner explicitly
	// advertised the bounded multi-table unlock protocol. Unknown lock outcomes
	// deliberately stay on the legacy table-scoped cleanup path.
	batchUnlockTables map[uint64]struct{}
	// uncertainLockKeys contains rows recorded only because a remote Lock
	// response failed. They remain in tableKeys for conservative cleanup, but
	// do not prove that this transaction holds the row for deadlock detection.
	uncertainLockKeys map[uint64]map[string]struct{}
}

func (h *tableLockHolder) ensureExtra() *tableLockHolderExtra {
	if h.extra == nil {
		h.extra = &tableLockHolderExtra{}
	}
	return h.extra
}

func (h *tableLockHolder) nonCoarsenableTables() map[uint64]struct{} {
	if h.extra == nil {
		return nil
	}
	return h.extra.nonCoarsenableTables
}

func (h *tableLockHolder) coarsenedTables() map[uint64]struct{} {
	if h.extra == nil {
		return nil
	}
	return h.extra.coarsenedTables
}

func (h *tableLockHolder) ownerLocalWaitSnapshots() map[uint64]struct{} {
	if h.extra == nil {
		return nil
	}
	return h.extra.ownerLocalWaitSnapshots
}

func (h *tableLockHolder) remoteUnlockRequiredTables() map[uint64]struct{} {
	if h.extra == nil {
		return nil
	}
	return h.extra.remoteUnlockRequired
}

func (h *tableLockHolder) batchUnlockSupportedTables() map[uint64]struct{} {
	if h.extra == nil {
		return nil
	}
	return h.extra.batchUnlockTables
}

func (h *tableLockHolder) uncertainLockKeys() map[uint64]map[string]struct{} {
	if h.extra == nil {
		return nil
	}
	return h.extra.uncertainLockKeys
}

func (h *tableLockHolder) clearExtraIfEmpty() {
	if h.extra != nil &&
		len(h.extra.nonCoarsenableTables) == 0 &&
		len(h.extra.coarsenedTables) == 0 &&
		len(h.extra.ownerLocalWaitSnapshots) == 0 &&
		len(h.extra.remoteUnlockRequired) == 0 &&
		len(h.extra.batchUnlockTables) == 0 &&
		len(h.extra.uncertainLockKeys) == 0 {
		h.extra = nil
	}
}

// activeTxn one goroutine write, multi goroutine read
type activeTxn struct {
	*sync.RWMutex
	txnID          []byte
	txnKey         string
	generation     uint64
	fsp            *fixedSlicePool
	blockedWaiters []*waiter
	// asyncLockOps keeps barrier-protected callbacks from outliving and
	// publishing into a recycled transaction generation. lockOpsCtx interrupts
	// their local waits and remote RPCs when the transaction becomes terminal.
	asyncLockOps  sync.WaitGroup
	lockOpsCtx    context.Context
	cancelLockOps context.CancelFunc
	lockHolders   map[uint32]*tableLockHolder
	remoteService string
	deadlockFound bool
	bindChanged   bool
	// closing is read by bind-change fencing without taking txn's mutex. A
	// closing transaction rejects every later Lock and therefore no longer
	// needs that fence; making the state atomic lets the fence avoid waiting on
	// cleanup while holding bindChangeMu.
	closing atomic.Bool

	// test-only hook: called before lockAdded; return non-nil to abort
	beforeLockAdded func(txnID []byte, locks [][]byte) error
}

// beginLockOpLocked admits work into the transaction generation. The caller
// holds txn's mutex and has already rejected closing/deadlock/bind-change
// states, which serializes WaitGroup Add against terminal Wait.
func (txn *activeTxn) beginLockOpLocked(
	parent context.Context,
) (context.Context, func()) {
	if txn.lockOpsCtx == nil {
		txn.lockOpsCtx, txn.cancelLockOps = context.WithCancel(context.Background())
	}
	txn.asyncLockOps.Add(1)
	ctx, cancel := contextWithServiceClose(parent, txn.lockOpsCtx)
	return ctx, func() {
		cancel()
		txn.asyncLockOps.Done()
	}
}

// beginClosingLocked seals admission before canceling and draining current
// work. The caller holds txn's mutex.
func (txn *activeTxn) beginClosingLocked(logger *log.MOLogger) {
	txn.closing.Store(true)
	if txn.cancelLockOps != nil {
		txn.cancelLockOps()
	}
	txn.cancelBlocks(logger)
}

// waitAsyncLockOpsLocked waits for callbacks that may still dereference txn.
// The caller holds txn's mutex before and after this method. closing must be
// set before calling it so no later handler can add another operation.
func (txn *activeTxn) waitAsyncLockOpsLocked(
	txnID []byte,
	generation uint64,
) bool {
	if txn.lockOpsCtx == nil {
		// A synchronous generation never admits work into asyncLockOps. Once
		// closing is set under the transaction mutex, a nil generation context
		// proves the WaitGroup is empty and avoids an unnecessary unlock/relock.
		return txn.generation == generation && bytes.Equal(txn.txnID, txnID)
	}
	txn.Unlock()
	txn.asyncLockOps.Wait()
	txn.Lock()
	return txn.generation == generation && bytes.Equal(txn.txnID, txnID)
}

// terminalLockErrorLocked normalizes every terminal transaction-generation
// state before a Lock result is returned. Context cancellation and waiter
// notification race by design; the durable state under txn's mutex defines
// the public error, independent of which wake-up won that race.
func (txn *activeTxn) terminalLockErrorLocked(txnID []byte) error {
	if !bytes.Equal(txn.txnID, txnID) {
		return ErrTxnNotFound
	}
	if txn.deadlockFound {
		return ErrDeadLockDetected
	}
	if txn.bindChanged {
		return ErrLockTableBindChanged
	}
	if txn.closing.Load() {
		return ErrTxnNotFound
	}
	return nil
}

// preparedTxnLocks is the transaction-bookkeeping half of one range
// representation change. Allocation and failure injection happen while the
// lock store is still untouched; commit itself cannot fail. The caller holds
// both the transaction mutex and the local lock-table mutex.
type preparedTxnLocks struct {
	txn              *activeTxn
	holder           *tableLockHolder
	table            uint64
	bind             pb.LockTable
	old              *cowSlice
	next             *cowSlice
	added            [][]byte
	opts             pb.LockOptions
	logger           *log.MOLogger
	hadUncertainKeys bool
	uncertainKeys    map[string]struct{}
	coarsenedExtra   *tableLockHolderExtra
	coarsenedTables  map[uint64]struct{}
}

// prepareMarkCoarsened builds the next persistent coarsening state before the
// physical lock representation changes. commit must remain allocation-free: it
// linearizes the transaction ledger and lock store while both mutexes are held.
func (p *preparedTxnLocks) prepareMarkCoarsened() bool {
	current := p.holder.coarsenedTables()
	if _, ok := current[p.table]; ok {
		return false
	}
	next := make(map[uint64]struct{}, len(current)+1)
	for table := range current {
		next[table] = struct{}{}
	}
	next[p.table] = struct{}{}
	p.coarsenedTables = next
	if p.holder.extra == nil {
		p.coarsenedExtra = &tableLockHolderExtra{coarsenedTables: next}
	}
	return true
}

func (p *preparedTxnLocks) commit() {
	if p == nil || p.next == nil {
		panic("BUG: invalid prepared transaction locks")
	}
	defer logTxnLockAdded(p.logger, p.txn, p.added)
	p.holder.tableKeys[p.table] = p.next
	p.holder.tableBinds[p.table] = p.bind
	p.txn.markTableNonCoarsenableLocked(p.holder, p.table, p.opts)
	if p.coarsenedTables != nil {
		if p.holder.extra == nil {
			p.holder.extra = p.coarsenedExtra
		} else {
			p.holder.extra.coarsenedTables = p.coarsenedTables
		}
	}
	if p.hadUncertainKeys {
		extra := p.holder.ensureExtra()
		if len(p.uncertainKeys) == 0 {
			delete(extra.uncertainLockKeys, p.table)
			if len(extra.uncertainLockKeys) == 0 {
				extra.uncertainLockKeys = nil
			}
			p.holder.clearExtraIfEmpty()
		} else {
			extra.uncertainLockKeys[p.table] = p.uncertainKeys
		}
	}
	p.next = nil
	if p.old != nil {
		p.old.close()
	}
}

func (p *preparedTxnLocks) close() {
	if p != nil && p.next != nil {
		p.next.close()
		p.next = nil
	}
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

// remoteTxnWaiterFetcher returns the authoritative wait-for snapshot kept by
// the service that owns a remote transaction's physical locks. Origin-side
// table keys are cleanup/proxy routing metadata and may intentionally use a
// different representation, so they cannot be the source of truth.
type remoteTxnWaiterFetcher interface {
	getTxnWaitingList(context.Context, []byte) ([]pb.WaitTxn, error)
}

func asRemoteTxnWaiterFetcher(l lockTable) (remoteTxnWaiterFetcher, bool) {
	if fetcher, ok := l.(remoteTxnWaiterFetcher); ok {
		return fetcher, true
	}
	if proxy, ok := l.(*localLockTableProxy); ok {
		fetcher, ok := proxy.remote.(remoteTxnWaiterFetcher)
		return fetcher, ok
	}
	return nil, false
}

func newActiveTxn(
	txnID []byte,
	txnKey string,
	fsp *fixedSlicePool,
	remoteService string) *activeTxn {
	txn := reuse.Alloc[activeTxn](nil)
	initActiveTxn(txn, txnID, txnKey, fsp, remoteService)
	return txn
}

func initActiveTxn(
	txn *activeTxn,
	txnID []byte,
	txnKey string,
	fsp *fixedSlicePool,
	remoteService string,
) {
	txn.Lock()
	defer txn.Unlock()
	txn.txnID = txnID
	txn.txnKey = txnKey
	txn.generation++
	txn.fsp = fsp
	txn.remoteService = remoteService
}

func (txn *activeTxn) TypeName() string {
	return "lockservice.activeTxn"
}

func (txn *activeTxn) lockAdded(
	group uint32,
	bind pb.LockTable,
	locks [][]byte,
	opts pb.LockOptions,
	logger *log.MOLogger,
) error {
	return txn.addLocks(group, bind, locks, opts, logger, true)
}

// lockAddedForCleanup records locks that a failed remote request may have
// acquired. The rows must be unlocked when the transaction closes, but cannot
// be used as confirmed holder edges by deadlock detection.
func (txn *activeTxn) lockAddedForCleanup(
	group uint32,
	bind pb.LockTable,
	locks [][]byte,
	opts pb.LockOptions,
	logger *log.MOLogger,
) error {
	return txn.addLocks(group, bind, locks, opts, logger, false)
}

func (txn *activeTxn) addLocks(
	group uint32,
	bind pb.LockTable,
	locks [][]byte,
	opts pb.LockOptions,
	logger *log.MOLogger,
	confirmed bool,
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
	var existing map[string]struct{}
	if !confirmed && ok {
		requested := make(map[string]struct{}, len(locks))
		for _, key := range locks {
			requested[string(key)] = struct{}{}
		}
		existing = make(map[string]struct{}, len(requested))
		s := v.slice()
		s.iter(func(key []byte) bool {
			value := util.UnsafeBytesToString(key)
			if _, ok := requested[value]; ok {
				existing[string(key)] = struct{}{}
			}
			return true
		})
		s.unref()
	}

	if ok {
		err = v.append(locks)
	} else {
		var cs *cowSlice
		cs, err = newCowSlice(txn.fsp, locks)
		if err == nil {
			h.tableKeys[bind.Table] = cs
			h.tableBinds[bind.Table] = bind
		}
	}
	if err != nil {
		return err
	}
	// Cumulative coarsening is safe only while the complete retained
	// transaction/table ownership consists of non-sharded Exclusive locks.
	// Transaction bookkeeping intentionally stores keys rather than per-key
	// modes, so conservatively make ineligibility monotonic for this transaction.
	txn.markTableNonCoarsenableLocked(h, bind.Table, opts)
	if confirmed {
		h.markLocksConfirmed(bind.Table, locks)
	} else {
		h.markNewLocksUncertain(bind.Table, locks, existing)
	}
	return nil
}

func (txn *activeTxn) markTableNonCoarsenableLocked(
	h *tableLockHolder,
	table uint64,
	opts pb.LockOptions,
) {
	if opts.Mode == pb.LockMode_Exclusive && opts.Sharding == pb.Sharding_None {
		return
	}
	extra := h.ensureExtra()
	if extra.nonCoarsenableTables == nil {
		extra.nonCoarsenableTables = make(map[uint64]struct{})
	}
	extra.nonCoarsenableTables[table] = struct{}{}
}

func (txn *activeTxn) canCoarsenTableLocked(group uint32, table uint64) bool {
	h, ok := txn.lockHolders[group]
	if !ok {
		return true
	}
	_, disabled := h.nonCoarsenableTables()[table]
	return !disabled
}

func (txn *activeTxn) markRemoteUnlockRequiredLocked(group uint32, table uint64) {
	h := txn.getHoldLocksLocked(group)
	extra := h.ensureExtra()
	if extra.remoteUnlockRequired == nil {
		extra.remoteUnlockRequired = make(map[uint64]struct{})
	}
	extra.remoteUnlockRequired[table] = struct{}{}
}

func (txn *activeTxn) isRemoteUnlockRequiredLocked(group uint32, table uint64) bool {
	h, ok := txn.lockHolders[group]
	if !ok {
		return false
	}
	_, required := h.remoteUnlockRequiredTables()[table]
	return required
}

func (txn *activeTxn) setBatchUnlockSupportedLocked(
	group uint32,
	table uint64,
	supported bool,
) {
	h := txn.getHoldLocksLocked(group)
	if !supported {
		if h.extra != nil {
			delete(h.extra.batchUnlockTables, table)
			h.clearExtraIfEmpty()
		}
		return
	}
	extra := h.ensureExtra()
	if extra.batchUnlockTables == nil {
		extra.batchUnlockTables = make(map[uint64]struct{})
	}
	extra.batchUnlockTables[table] = struct{}{}
}

func (txn *activeTxn) isBatchUnlockSupportedLocked(group uint32, table uint64) bool {
	h, ok := txn.lockHolders[group]
	if !ok {
		return false
	}
	_, supported := h.batchUnlockSupportedTables()[table]
	return supported
}

func (txn *activeTxn) markOwnerLocalWaitSnapshotLocked(group uint32, table uint64) {
	h := txn.getHoldLocksLocked(group)
	extra := h.ensureExtra()
	if extra.ownerLocalWaitSnapshots == nil {
		extra.ownerLocalWaitSnapshots = make(map[uint64]struct{})
	}
	extra.ownerLocalWaitSnapshots[table] = struct{}{}
}

func (txn *activeTxn) hasOwnerLocalWaitSnapshotLocked(group uint32, table uint64) bool {
	h, ok := txn.lockHolders[group]
	if !ok {
		return false
	}
	_, ok = h.ownerLocalWaitSnapshots()[table]
	return ok
}

// prepareLockUpdate builds the complete post-merge ledger before either
// ownership surface changes. keep is evaluated for every currently recorded
// key; the retained keys are followed by added in their physical unlock order.
// The caller holds txn's mutex.
func (txn *activeTxn) prepareLockUpdate(
	group uint32,
	bind pb.LockTable,
	added [][]byte,
	opts pb.LockOptions,
	keep func([]byte) bool,
	logger *log.MOLogger,
) (*preparedTxnLocks, error) {
	if txn.beforeLockAdded != nil {
		if err := txn.beforeLockAdded(txn.txnID, added); err != nil {
			return nil, err
		}
	}

	h := txn.getHoldLocksLocked(group)
	old := h.tableKeys[bind.Table]
	oldUncertain := h.uncertainLockKeys()[bind.Table]
	capacity := len(added)
	if old != nil {
		capacity += old.mustGet().len()
	}
	nextValues := make([][]byte, 0, capacity)
	var nextUncertain map[string]struct{}
	if old != nil {
		current := old.slice()
		current.iter(func(key []byte) bool {
			if keep == nil || keep(key) {
				nextValues = append(nextValues, key)
				if _, ok := oldUncertain[util.UnsafeBytesToString(key)]; ok {
					if nextUncertain == nil {
						nextUncertain = make(map[string]struct{}, len(oldUncertain))
					}
					nextUncertain[string(key)] = struct{}{}
				}
			}
			return true
		})
		current.unref()
	}
	nextValues = append(nextValues, added...)
	for _, key := range added {
		delete(nextUncertain, util.UnsafeBytesToString(key))
	}
	next, err := newCowSlice(txn.fsp, nextValues)
	if err != nil {
		return nil, err
	}
	return &preparedTxnLocks{
		txn:              txn,
		holder:           h,
		table:            bind.Table,
		bind:             bind,
		old:              old,
		next:             next,
		added:            added,
		opts:             opts,
		logger:           logger,
		hadUncertainKeys: len(oldUncertain) > 0,
		uncertainKeys:    nextUncertain,
	}, nil
}

// remoteLockAdded records origin-side routing and local-proxy keys. Unlike the
// authoritative owner ledger, this snapshot is never interpreted as physical
// range pairs during remote unlock or deadlock traversal. Keep the common
// append path unchanged and compact exact duplicates only when the configured
// fixed capacity would otherwise reject a successful remote operation.
func (txn *activeTxn) remoteLockAdded(
	group uint32,
	bind pb.LockTable,
	locks [][]byte,
	opts pb.LockOptions,
	logger *log.MOLogger,
) error {
	if txn.beforeLockAdded != nil {
		if err := txn.beforeLockAdded(txn.txnID, locks); err != nil {
			return err
		}
	}
	defer logTxnLockAdded(logger, txn, locks)

	h := txn.getHoldLocksLocked(group)
	old := h.tableKeys[bind.Table]
	if old == nil {
		cs, err := newCowSlice(txn.fsp, locks)
		if err != nil {
			return err
		}
		h.tableKeys[bind.Table] = cs
		h.tableBinds[bind.Table] = bind
		txn.markTableNonCoarsenableLocked(h, bind.Table, opts)
		h.markLocksConfirmed(bind.Table, locks)
		return nil
	}
	if err := old.append(locks); err == nil {
		txn.markTableNonCoarsenableLocked(h, bind.Table, opts)
		h.markLocksConfirmed(bind.Table, locks)
		return nil
	} else if !moerr.IsMoErrCode(err, moerr.ErrLockNeedUpgrade) {
		return err
	}

	// Compact exact duplicates only after capacity overflow. The ordinary
	// success path above remains allocation-free.
	seen := make(map[string]struct{}, old.mustGet().len()+len(locks))
	values := make([][]byte, 0, old.mustGet().len()+len(locks))
	addDistinct := func(key []byte) {
		value := util.UnsafeBytesToString(key)
		if _, ok := seen[value]; ok {
			return
		}
		seen[string(key)] = struct{}{}
		values = append(values, key)
	}
	current := old.slice()
	current.iter(func(key []byte) bool {
		addDistinct(key)
		return true
	})
	current.unref()
	for _, key := range locks {
		addDistinct(key)
	}
	next, compactErr := newCowSlice(txn.fsp, values)
	if compactErr != nil {
		return compactErr
	}
	h.tableKeys[bind.Table] = next
	h.tableBinds[bind.Table] = bind
	txn.markTableNonCoarsenableLocked(h, bind.Table, opts)
	h.markLocksConfirmed(bind.Table, locks)
	old.close()
	return nil
}

// reconcileLegacyRemoteLocks repairs the exact origin-side probe ledger after
// an acknowledged Lock on an owner without transaction-level snapshots. This
// includes NewLockAdd=false after an earlier successful response was lost.
// Existing keys stay on the allocation-free path; only missing probes are
// appended, and every acknowledged logical key becomes confirmed.
func (txn *activeTxn) reconcileLegacyRemoteLocks(
	group uint32,
	bind pb.LockTable,
	locks [][]byte,
	opts pb.LockOptions,
	logger *log.MOLogger,
) error {
	holder := txn.getHoldLocksLocked(group)
	current := holder.tableKeys[bind.Table]
	if current == nil {
		return txn.remoteLockAdded(group, bind, locks, opts, logger)
	}

	seen := make(map[string]struct{}, current.mustGet().len())
	values := current.slice()
	values.iter(func(key []byte) bool {
		seen[string(key)] = struct{}{}
		return true
	})
	values.unref()
	missing := make([][]byte, 0, len(locks))
	for _, key := range locks {
		value := util.UnsafeBytesToString(key)
		if _, ok := seen[value]; ok {
			continue
		}
		seen[string(key)] = struct{}{}
		missing = append(missing, key)
	}
	if len(missing) > 0 {
		if err := txn.remoteLockAdded(
			group,
			bind,
			missing,
			opts,
			logger,
		); err != nil {
			return err
		}
	}
	holder.markLocksConfirmed(bind.Table, locks)
	return nil
}

// ensureRemoteLockTableTracked is the bounded cleanup fallback for an
// indeterminate remote Lock result. Remote unlock releases all owner-side locks
// by transaction ID; therefore an existing table entry is already sufficient.
// For a first request, one key creates that routing entry without copying an
// arbitrarily large speculative request. On negotiated tables the owner-local
// transaction snapshot provides the authoritative wait graph; legacy tables
// use this only for cleanup after an indeterminate RPC. Allocation failure is
// returned to the caller.
func (txn *activeTxn) ensureRemoteLockTableTracked(
	group uint32,
	bind pb.LockTable,
	locks [][]byte,
	opts pb.LockOptions,
	confirmed bool,
	logger *log.MOLogger,
) error {
	h := txn.getHoldLocksLocked(group)
	if len(locks) == 0 {
		return nil
	}
	old := h.tableKeys[bind.Table]
	if old == nil {
		cs, err := newCowSlice(txn.fsp, locks[:1])
		if err != nil {
			return err
		}
		defer logTxnLockAdded(logger, txn, locks[:1])
		h.tableKeys[bind.Table] = cs
		h.tableBinds[bind.Table] = bind
		txn.markTableNonCoarsenableLocked(h, bind.Table, opts)
		if !confirmed {
			h.markNewLocksUncertain(bind.Table, locks[:1], nil)
		}
		return nil
	}
	txn.markTableNonCoarsenableLocked(h, bind.Table, opts)
	if confirmed {
		h.markLocksConfirmed(bind.Table, locks)
	}
	return nil
}

// coarsenLockRequest enforces the row-lock budget at the owner of the actual
// transaction state. The caller must hold txn's mutex.
//
// The budget is scoped to one transaction and physical lock table, rather than
// one Lock call. That distinction matters because execution sends a large DML
// as many independently sub-threshold batches and a transaction can contain
// multiple statements. Once the retained keys plus the incoming keys exceed
// the budget, one conservative range spanning their observed minimum and
// maximum replaces them. The range can cover gaps, but never keys outside the
// observed bounds as a table lock would.
//
// An Exclusive request can be coarsened only when every retained lock is also
// non-sharded Exclusive; otherwise the replacement could strengthen historical
// Shared ownership or span keys from different physical tables. Shared requests
// deliberately stay exact: the non-overlapping range representation cannot
// replace one transaction's rows across foreign Shared holders without either
// waiting on a compatible lock or broadening another transaction's ownership.
func (txn *activeTxn) coarsenLockRequest(
	group uint32,
	table uint64,
	rows [][]byte,
	opts pb.LockOptions,
	maxLockRowCount int,
) ([][]byte, pb.LockOptions, bool) {
	if len(rows) == 0 ||
		maxLockRowCount <= 0 ||
		opts.Mode != pb.LockMode_Exclusive ||
		opts.Sharding != pb.Sharding_None {
		return rows, opts, false
	}
	switch opts.Granularity {
	case pb.Granularity_Row:
	case pb.Granularity_Range:
		if len(rows)%2 != 0 {
			// Preserve the validation failure in the lock-table layer instead of
			// turning malformed input into a valid but unintended range.
			return rows, opts, false
		}
	default:
		return rows, opts, false
	}

	var held *cowSlice
	heldCount := 0
	alreadyCoarsened := false
	if h, ok := txn.lockHolders[group]; ok {
		if _, disabled := h.nonCoarsenableTables()[table]; disabled {
			return rows, opts, false
		}
		_, alreadyCoarsened = h.coarsenedTables()[table]
		held = h.tableKeys[table]
		if held != nil {
			heldCount = held.mustGet().len()
		}
	}
	// A range needs two endpoints even when a test or deployment configures a
	// smaller row budget.
	effectiveBudget := max(maxLockRowCount, 2)
	naiveCount := heldCount + len(rows)
	if !alreadyCoarsened && naiveCount <= effectiveBudget {
		return rows, opts, false
	}

	var minKey, maxKey []byte
	updateBounds := func(key []byte) {
		if minKey == nil || bytes.Compare(key, minKey) < 0 {
			minKey = key
		}
		if maxKey == nil || bytes.Compare(key, maxKey) > 0 {
			maxKey = key
		}
	}
	if alreadyCoarsened {
		// The first committed budget replacement made the conservative range part
		// of this transaction's ownership semantics. Preserve that representation
		// for every later compatible batch. This bounds both physical locks and
		// merge work instead of cycling between two endpoints and another full
		// budget of exact row locks.
		if held != nil {
			locks := held.slice()
			locks.iter(func(key []byte) bool {
				updateBounds(key)
				return true
			})
			locks.unref()
		}
		for _, row := range rows {
			updateBounds(row)
		}
		if minKey == nil || bytes.Equal(minKey, maxKey) {
			opts.Granularity = pb.Granularity_Row
			if minKey == nil {
				return nil, opts, false
			}
			return [][]byte{bytes.Clone(minKey)}, opts, true
		}
		opts.Granularity = pb.Granularity_Range
		return [][]byte{bytes.Clone(minKey), bytes.Clone(maxKey)}, opts, true
	}

	// Count distinct retained keys only on the budget-crossing path. Ordinary
	// requests keep the zero-allocation fast path above; this set is bounded by
	// effectiveBudget because once one more distinct key is seen, coarsening is
	// already mandatory and only min/max still need to be scanned.
	seen := make(map[string]struct{}, min(naiveCount, effectiveBudget))
	overBudget := false
	add := func(key []byte) bool {
		updateBounds(key)
		if overBudget {
			return false
		}
		value := util.UnsafeBytesToString(key)
		if _, ok := seen[value]; ok {
			return false
		}
		if len(seen) == effectiveBudget {
			overBudget = true
			return false
		}
		seen[value] = struct{}{}
		return true
	}
	if held != nil {
		locks := held.slice()
		locks.iter(func(key []byte) bool {
			add(key)
			return true
		})
		locks.unref()
	}
	newRows := rows
	if opts.Granularity == pb.Granularity_Row {
		newRows = make([][]byte, 0, min(len(rows), effectiveBudget))
	}
	for _, row := range rows {
		if add(row) && opts.Granularity == pb.Granularity_Row {
			newRows = append(newRows, row)
		}
	}
	if !overBudget {
		// Re-entrant row keys and request duplicates add no physical ownership.
		// Remove them on this already-uncommon path so remote-origin bookkeeping
		// also stays aligned with the authoritative owner. Explicit ranges retain
		// their endpoint pairs unchanged.
		return newRows, opts, false
	}

	if bytes.Equal(minKey, maxKey) {
		// Re-entrant calls for one key can duplicate remote-origin bookkeeping.
		// Compact that bookkeeping without inventing an invalid zero-width range.
		opts.Granularity = pb.Granularity_Row
		return [][]byte{bytes.Clone(minKey)}, opts, true
	}
	opts.Granularity = pb.Granularity_Range
	return [][]byte{bytes.Clone(minKey), bytes.Clone(maxKey)}, opts, true
}

// replaceLocks records a committed coarsened range while preserving keys outside
// that range. A Lock call can wait with txn's mutex released, so another call for
// the same transaction may acquire an out-of-range key before this replacement
// commits. Dropping the whole old slice would lose the only cleanup record for
// that key and leak its physical lock. The caller must hold txn's mutex.
// Allocation and the test failure hook run before the old bookkeeping is
// detached, so an error leaves the transaction's ownership state unchanged.
func (txn *activeTxn) replaceLocks(
	group uint32,
	bind pb.LockTable,
	locks [][]byte,
	logger *log.MOLogger,
) error {
	keep := func([]byte) bool { return false }
	if len(locks) == 2 && bytes.Compare(locks[0], locks[1]) < 0 {
		// Range endpoints must stay adjacent in transaction bookkeeping because
		// local unlock interprets a range-start followed by its range-end as one
		// lock. Preserve ownership acquired concurrently outside the planned
		// range; keys inside it are represented by the replacement pair.
		keep = func(key []byte) bool {
			return bytes.Compare(key, locks[0]) < 0 ||
				bytes.Compare(key, locks[1]) > 0
		}
	}
	prepared, err := txn.prepareLockUpdate(
		group,
		bind,
		locks,
		pb.LockOptions{
			Mode:     pb.LockMode_Exclusive,
			Sharding: pb.Sharding_None,
		},
		keep,
		logger,
	)
	if err != nil {
		return err
	}
	prepared.prepareMarkCoarsened()
	prepared.commit()
	return nil
}

func (h *tableLockHolder) markLocksConfirmed(table uint64, locks [][]byte) {
	uncertainByTable := h.uncertainLockKeys()
	uncertain := uncertainByTable[table]
	if len(uncertain) == 0 {
		return
	}
	for _, key := range locks {
		delete(uncertain, util.UnsafeBytesToString(key))
	}
	if len(uncertain) == 0 {
		delete(uncertainByTable, table)
		if len(uncertainByTable) == 0 {
			h.extra.uncertainLockKeys = nil
		}
		h.clearExtraIfEmpty()
	}
}

func (h *tableLockHolder) markNewLocksUncertain(
	table uint64,
	locks [][]byte,
	existing map[string]struct{},
) {
	for _, key := range locks {
		if _, ok := existing[util.UnsafeBytesToString(key)]; ok {
			continue
		}
		extra := h.ensureExtra()
		if extra.uncertainLockKeys == nil {
			extra.uncertainLockKeys = make(map[uint64]map[string]struct{})
		}
		uncertain := extra.uncertainLockKeys[table]
		if uncertain == nil {
			uncertain = make(map[string]struct{})
			extra.uncertainLockKeys[table] = uncertain
		}
		uncertain[string(key)] = struct{}{}
	}
}

func (txn *activeTxn) lockTableBindTouched(bind pb.LockTable) bool {
	h := txn.getHoldLocksLocked(bind.Group)
	if _, ok := h.tableBindIntents[bind.Table]; ok {
		return false
	}
	h.tableBindIntents[bind.Table] = bind
	return true
}

// iterLockTableBindsLocked visits every table touched by the transaction once.
// Acquired binds take precedence over intents because they are authoritative.
// The caller must hold txn's mutex.
func (txn *activeTxn) iterLockTableBindsLocked(
	fn func(group uint32, table uint64, bind pb.LockTable),
) {
	for group, h := range txn.lockHolders {
		for table, bind := range h.tableBinds {
			fn(group, table, bind)
		}
		for table, bind := range h.tableBindIntents {
			if _, ok := h.tableBinds[table]; !ok {
				fn(group, table, bind)
			}
		}
	}
}

func (txn *activeTxn) lockTableBindsLocked() []pb.LockTable {
	binds := make([]pb.LockTable, 0, len(txn.lockHolders))
	txn.iterLockTableBindsLocked(func(_ uint32, _ uint64, bind pb.LockTable) {
		binds = append(binds, bind)
	})
	return binds
}

func (txn *activeTxn) close(
	txnID []byte,
	commitTS timestamp.Timestamp,
	lockTableFunc func(pb.LockTable) (lockTable, error),
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
	lockTableFunc func(pb.LockTable) (lockTable, error),
	logger *log.MOLogger,
	mutations ...pb.ExtraMutation,
) error {
	return txn.closeWithContextInternal(
		ctx,
		txnID,
		commitTS,
		lockTableFunc,
		logger,
		true,  // release
		false, // detachSuccessful
		false, // directLocalUnlock
		mutations...,
	)
}

func (txn *activeTxn) closeWithoutFreeWithContext(
	ctx context.Context,
	txnID []byte,
	commitTS timestamp.Timestamp,
	lockTableFunc func(pb.LockTable) (lockTable, error),
	logger *log.MOLogger,
	mutations ...pb.ExtraMutation,
) error {
	return txn.closeWithContextInternal(
		ctx,
		txnID,
		commitTS,
		lockTableFunc,
		logger,
		false, // release
		true,  // detachSuccessful
		false, // directLocalUnlock
		mutations...,
	)
}

// closeSynchronousWithoutFreeWithContext is the durable synchronous close
// path. Concrete local tables cannot return a retryable error, so their ledgers
// stay attached for the caller's immediate final Free. Uncommon context-aware
// table implementations still detach each success if a later table fails.
func (txn *activeTxn) closeSynchronousWithoutFreeWithContext(
	ctx context.Context,
	txnID []byte,
	commitTS timestamp.Timestamp,
	lockTableFunc func(pb.LockTable) (lockTable, error),
	logger *log.MOLogger,
	mutations ...pb.ExtraMutation,
) error {
	return txn.closeWithContextInternal(
		ctx,
		txnID,
		commitTS,
		lockTableFunc,
		logger,
		false, // release
		false, // detachSuccessful
		true,  // directLocalUnlock
		mutations...,
	)
}

func (txn *activeTxn) closeWithContextInternal(
	ctx context.Context,
	txnID []byte,
	commitTS timestamp.Timestamp,
	lockTableFunc func(pb.LockTable) (lockTable, error),
	logger *log.MOLogger,
	release bool,
	detachSuccessful bool,
	directLocalUnlock bool,
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
			bind, ok := h.tableBinds[table]
			if !ok {
				return moerr.NewInternalErrorNoCtx(
					"transaction lock table is missing its recorded binding")
			}
			l, err := lockTableFunc(bind)
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
				if detachSuccessful {
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
					_, local := l.(*localLockTable)
					if directLocalUnlock && local {
						l.unlock(txn, cs, commitTS, mutations...)
					} else if unlocker, ok := l.(contextUnlocker); ok {
						err = unlocker.unlockWithContext(ctx, txn, cs, commitTS, mutations...)
					} else {
						l.unlock(txn, cs, commitTS, mutations...)
					}
					// The completion log reads cs through slice(). Keep that read
					// before a successful detach releases the cowSlice owner.
					logTxnUnlockTableCompleted(
						logger,
						txn,
						table,
						cs,
					)
					if err != nil {
						errMu.Lock()
						if firstErr == nil {
							firstErr = err
						}
						errMu.Unlock()
					} else if detachSuccessful || (directLocalUnlock && !local) {
						txn.removeClosedLockTable(group, table, cs)
					}
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
	if h.extra != nil {
		delete(h.extra.nonCoarsenableTables, table)
		delete(h.extra.coarsenedTables, table)
		delete(h.extra.remoteUnlockRequired, table)
		delete(h.extra.batchUnlockTables, table)
		delete(h.extra.ownerLocalWaitSnapshots, table)
		delete(h.extra.uncertainLockKeys, table)
		h.clearExtraIfEmpty()
	}
	// Keep the intent until the whole transaction closes. It owns the service
	// drain reference even after this table was successfully released during a
	// retryable, multi-table cleanup.
	cs.close()
	if len(h.tableKeys) == 0 && len(h.tableBinds) == 0 &&
		h.extra == nil &&
		len(h.tableBindIntents) == 0 {
		delete(txn.lockHolders, group)
	}
}

func (txn *activeTxn) reset() {
	if txn.cancelLockOps != nil {
		txn.cancelLockOps()
	}
	txn.lockOpsCtx = nil
	txn.cancelLockOps = nil
	for g, h := range txn.lockHolders {
		for table, cs := range h.tableKeys {
			cs.close()
			delete(h.tableKeys, table)
		}
		for table := range h.tableBinds {
			delete(h.tableBinds, table)
		}
		h.extra = nil
		for table := range h.tableBindIntents {
			delete(h.tableBindIntents, table)
		}
		delete(txn.lockHolders, g)
	}

	txn.txnID = nil
	txn.txnKey = ""
	clear(txn.blockedWaiters)
	txn.blockedWaiters = txn.blockedWaiters[:0]
	txn.remoteService = ""
	txn.deadlockFound = false
	txn.bindChanged = false
	txn.closing.Store(false)
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

	txn.markDeadlockLocked(err, logger)
}

// markDeadlockLocked makes deadlock a transaction-generation terminal state.
// The caller holds txn's mutex.
func (txn *activeTxn) markDeadlockLocked(
	err error,
	logger *log.MOLogger,
) {
	txn.deadlockFound = true
	if txn.cancelLockOps != nil {
		txn.cancelLockOps()
	}
	for _, w := range txn.blockedWaiters {
		w.notify(notifyValue{err: err}, logger)
	}
}

func (txn *activeTxn) fenceByBindChanged(bind pb.LockTable, logger *log.MOLogger) bool {
	txn.Lock()
	defer txn.Unlock()
	return txn.fenceByBindChangedLocked(bind, logger)
}

func (txn *activeTxn) fenceByBindChangedLocked(bind pb.LockTable, logger *log.MOLogger) bool {
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

	txn.markBindChangedLocked(logger)
	return true
}

func (txn *activeTxn) fenceByExactBindLocked(bind pb.LockTable, logger *log.MOLogger) bool {
	if txn.bindChanged {
		return false
	}
	h, ok := txn.lockHolders[bind.Group]
	if !ok {
		return false
	}
	key := makeRemoteBindKey(bind)
	actual, actualOK := h.tableBinds[bind.Table]
	intent, intentOK := h.tableBindIntents[bind.Table]
	actualMatches := actualOK && makeRemoteBindKey(actual) == key
	intentMatches := intentOK && makeRemoteBindKey(intent) == key
	if !actualMatches && !intentMatches {
		return false
	}

	txn.markBindChangedLocked(logger)
	return true
}

// markBindChangedLocked fences every in-flight operation of this transaction,
// not only the operation that observed the generation/protocol transition.
// The caller holds txn's mutex.
func (txn *activeTxn) markBindChangedLocked(logger *log.MOLogger) {
	txn.bindChanged = true
	if txn.cancelLockOps != nil {
		txn.cancelLockOps()
	}
	for _, w := range txn.blockedWaiters {
		w.notify(notifyValue{err: ErrLockTableBindChanged}, logger)
	}
}

func (txn *activeTxn) cancelBlocks(
	logger *log.MOLogger,
) {
	for _, w := range txn.blockedWaiters {
		w.notify(notifyValue{err: ErrTxnNotFound}, logger)
		w.close("cancelBlocks", logger)
	}
	clear(txn.blockedWaiters)
	txn.blockedWaiters = txn.blockedWaiters[:0]
}

func (txn *activeTxn) hasHeldLockTablesLocked() bool {
	for _, holder := range txn.lockHolders {
		if len(holder.tableKeys) > 0 {
			return true
		}
	}
	return false
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
	clear(txn.blockedWaiters[len(newBlockedWaiters):])
	txn.blockedWaiters = newBlockedWaiters
}

func (txn *activeTxn) closeBlockWaiters(logger *log.MOLogger) {
	for _, w := range txn.blockedWaiters {
		w.close("closeBlockWaiters", logger)
	}
	clear(txn.blockedWaiters)
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

func (txn *activeTxn) incLockTableRef(m map[uint32]map[uint64]uint64, serviceID string) {
	txn.RLock()
	defer txn.RUnlock()
	txn.iterLockTableBindsLocked(func(_ uint32, _ uint64, l pb.LockTable) {
		if serviceID == l.ServiceID {
			if _, ok := m[l.Group]; !ok {
				m[l.Group] = make(map[uint64]uint64, 1024)
			}
			m[l.Group][l.Table]++
		}
	})
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
	return txn.fetchWhoWaitingMeInternal(
		ctx,
		serviceID,
		txnID,
		waiters,
		lockTableFunc,
		true,
	)
}

// fetchWhoWaitingMeOnLockTable builds a snapshot using only physical lock
// tables owned by this service. It is the server side of the owner-local RPC;
// keeping remote snapshot recursion disabled is its liveness contract.
func (txn *activeTxn) fetchWhoWaitingMeOnLockTable(
	ctx context.Context,
	serviceID string,
	txnID []byte,
	waiters func(pb.WaitTxn, string) bool,
	lockTableFunc func(context.Context, uint32, uint64) (lockTable, error)) (bool, error) {
	return txn.fetchWhoWaitingMeInternal(
		ctx,
		serviceID,
		txnID,
		waiters,
		lockTableFunc,
		false,
	)
}

func (txn *activeTxn) fetchWhoWaitingMeInternal(
	ctx context.Context,
	serviceID string,
	txnID []byte,
	waiters func(pb.WaitTxn, string) bool,
	lockTableFunc func(context.Context, uint32, uint64) (lockTable, error),
	allowOwnerRPC bool,
) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	txn.RLock()
	// txn already closed
	if !bytes.Equal(txn.txnID, txnID) {
		txn.RUnlock()
		return true, nil
	}
	groups := make([]uint32, 0, len(txn.lockHolders))
	tables := make([]uint64, 0, len(txn.lockHolders))
	lockKeys := make([]*fixedSlice, 0, len(txn.lockHolders))
	uncertainLockKeys := make([]map[string]struct{}, 0, len(txn.lockHolders))
	ownerLocalSnapshots := make([]bool, 0, len(txn.lockHolders))
	for g, m := range txn.lockHolders {
		for table, cs := range m.tableKeys {
			tables = append(tables, table)
			lockKeys = append(lockKeys, cs.slice())
			groups = append(groups, g)
			var uncertainCopy map[string]struct{}
			if uncertain := m.uncertainLockKeys()[table]; len(uncertain) > 0 {
				uncertainCopy = make(map[string]struct{}, len(uncertain))
				for key := range uncertain {
					uncertainCopy[key] = struct{}{}
				}
			}
			uncertainLockKeys = append(uncertainLockKeys, uncertainCopy)
			_, ownerLocalSnapshot := m.ownerLocalWaitSnapshots()[table]
			ownerLocalSnapshots = append(ownerLocalSnapshots, ownerLocalSnapshot)
		}
	}

	wt := txn.toWaitTxn(serviceID, true)
	txn.RUnlock()

	defer func() {
		for _, cs := range lockKeys {
			cs.unref()
		}
	}()

	// Query an owner once only after that owner negotiated the dedicated
	// owner-local method on a successful Lock response. Legacy/mixed-version
	// tables retain exact keys and continue through GetTxnLock below.
	var remoteOwners map[string]struct{}
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
		if allowOwnerRPC && ownerLocalSnapshots[idx] {
			fetcher, ok := asRemoteTxnWaiterFetcher(l)
			if !ok {
				return false, moerr.NewInternalErrorNoCtx(
					"owner-local wait snapshot negotiated for a non-remote lock table")
			}
			owner := l.getBind().ServiceID
			if _, fetched := remoteOwners[owner]; fetched {
				continue
			}
			values, err := fetcher.getTxnWaitingList(ctx, txnID)
			if err != nil {
				if moerr.IsMoErrCode(err, moerr.ErrNotSupported) ||
					moerr.IsMoErrCode(err, moerr.ErrLockTableNotFound) ||
					moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged) {
					txn.Lock()
					if bytes.Equal(txn.txnID, txnID) {
						txn.markBindChangedLocked(nil)
					}
					txn.Unlock()
				}
				return false, err
			}
			if remoteOwners == nil {
				remoteOwners = make(map[string]struct{})
			}
			remoteOwners[owner] = struct{}{}
			for _, value := range values {
				if !waiters(value, value.WaiterAddress) {
					return false, nil
				}
			}
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
			if _, ok := uncertainLockKeys[idx][util.UnsafeBytesToString(lockKey)]; ok {
				return true
			}
			if err := l.getLock(
				ctx,
				lockKey,
				wt,
				func(lock Lock) {
					lock.waiters.iter(func(w *waiter) bool {
						if !w.isBlockingFor(txnID, lock.holders) {
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
