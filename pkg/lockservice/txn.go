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

type tableLockHolder struct {
	tableKeys            map[uint64]*cowSlice
	tableBinds           map[uint64]pb.LockTable
	nonCoarsenableTables map[uint64]struct{}
	// remoteUnlockRequired records tables on which this origin transaction may
	// own locks acquired through a direct owner RPC. localLockTableProxy must not
	// suppress that table-level txnID unlock merely because every retained probe
	// key belongs to its singleton Shared cache.
	remoteUnlockRequired map[uint64]struct{}
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

// preparedTxnLocks is the transaction-bookkeeping half of one range
// representation change. Allocation and failure injection happen while the
// lock store is still untouched; commit itself cannot fail. The caller holds
// both the transaction mutex and the local lock-table mutex.
type preparedTxnLocks struct {
	txn    *activeTxn
	holder *tableLockHolder
	table  uint64
	bind   pb.LockTable
	old    *cowSlice
	next   *cowSlice
	added  [][]byte
	opts   pb.LockOptions
	logger *log.MOLogger
}

func (p *preparedTxnLocks) commit() {
	if p == nil || p.next == nil {
		panic("BUG: invalid prepared transaction locks")
	}
	defer logTxnLockAdded(p.logger, p.txn, p.added)
	p.holder.tableKeys[p.table] = p.next
	p.holder.tableBinds[p.table] = p.bind
	p.txn.markTableNonCoarsenableLocked(p.holder, p.table, p.opts)
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

func (txn *activeTxn) lockAdded(
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
	if h.nonCoarsenableTables == nil {
		h.nonCoarsenableTables = make(map[uint64]struct{})
	}
	h.nonCoarsenableTables[table] = struct{}{}
}

func (txn *activeTxn) canCoarsenTableLocked(group uint32, table uint64) bool {
	h, ok := txn.lockHolders[group]
	if !ok {
		return true
	}
	_, disabled := h.nonCoarsenableTables[table]
	return !disabled
}

func (txn *activeTxn) markRemoteUnlockRequiredLocked(group uint32, table uint64) {
	h := txn.getHoldLocksLocked(group)
	if h.remoteUnlockRequired == nil {
		h.remoteUnlockRequired = make(map[uint64]struct{})
	}
	h.remoteUnlockRequired[table] = struct{}{}
}

func (txn *activeTxn) isRemoteUnlockRequiredLocked(group uint32, table uint64) bool {
	h, ok := txn.lockHolders[group]
	if !ok {
		return false
	}
	_, required := h.remoteUnlockRequired[table]
	return required
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
	capacity := len(added)
	if old != nil {
		capacity += old.mustGet().len()
	}
	nextValues := make([][]byte, 0, capacity)
	if old != nil {
		current := old.slice()
		current.iter(func(key []byte) bool {
			if keep == nil || keep(key) {
				nextValues = append(nextValues, key)
			}
			return true
		})
		current.unref()
	}
	nextValues = append(nextValues, added...)
	next, err := newCowSlice(txn.fsp, nextValues)
	if err != nil {
		return nil, err
	}
	return &preparedTxnLocks{
		txn:    txn,
		holder: h,
		table:  bind.Table,
		bind:   bind,
		old:    old,
		next:   next,
		added:  added,
		opts:   opts,
		logger: logger,
	}, nil
}

// remoteLockAdded records origin-side routing/deadlock probe keys. Unlike the
// authoritative owner ledger, this snapshot is never interpreted as physical
// range pairs during remote unlock. Keep the common append path unchanged and
// compact exact duplicates only when the configured fixed capacity would
// otherwise reject a successful remote operation.
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
		return nil
	}
	if err := old.append(locks); err == nil {
		txn.markTableNonCoarsenableLocked(h, bind.Table, opts)
		return nil
	} else if !moerr.IsMoErrCode(err, moerr.ErrLockNeedUpgrade) {
		return err
	}

	seen := make(map[string]struct{}, len(locks))
	values := make([][]byte, 0, len(locks))
	addDistinct := func(key []byte) {
		value := util.UnsafeBytesToString(key)
		if _, ok := seen[value]; ok {
			return
		}
		seen[value] = struct{}{}
		values = append(values, key)
	}
	if old != nil {
		current := old.slice()
		current.iter(func(key []byte) bool {
			addDistinct(key)
			return true
		})
		current.unref()
	}
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
	if old != nil {
		old.close()
	}
	return nil
}

// ensureRemoteLockTableTracked is the bounded cleanup fallback for an
// indeterminate remote Lock result. Remote unlock releases all owner-side locks
// by transaction ID; therefore an existing table entry is already sufficient.
// For a first request, one key creates that routing entry without attempting an
// old+new union that can exceed fixed capacity. Allocation failure is returned
// to the caller rather than silently dropping the only route to the owner.
func (txn *activeTxn) ensureRemoteLockTableTracked(
	group uint32,
	bind pb.LockTable,
	locks [][]byte,
	opts pb.LockOptions,
	logger *log.MOLogger,
) error {
	h := txn.getHoldLocksLocked(group)
	if h.tableKeys[bind.Table] != nil || len(locks) == 0 {
		if h.tableKeys[bind.Table] != nil {
			txn.markTableNonCoarsenableLocked(h, bind.Table, opts)
		}
		return nil
	}
	cs, err := newCowSlice(txn.fsp, locks[:1])
	if err != nil {
		return err
	}
	defer logTxnLockAdded(logger, txn, locks[:1])
	h.tableKeys[bind.Table] = cs
	h.tableBinds[bind.Table] = bind
	txn.markTableNonCoarsenableLocked(h, bind.Table, opts)
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
	if h, ok := txn.lockHolders[group]; ok {
		if _, disabled := h.nonCoarsenableTables[table]; disabled {
			return rows, opts, false
		}
		held = h.tableKeys[table]
		if held != nil {
			heldCount = held.mustGet().len()
		}
	}
	// A range needs two endpoints even when a test or deployment configures a
	// smaller row budget.
	effectiveBudget := max(maxLockRowCount, 2)
	naiveCount := heldCount + len(rows)
	if naiveCount <= effectiveBudget {
		return rows, opts, false
	}

	var minKey, maxKey []byte
	// Count distinct retained keys only on the budget-crossing path. Ordinary
	// requests keep the zero-allocation fast path above; this set is bounded by
	// effectiveBudget because once one more distinct key is seen, coarsening is
	// already mandatory and only min/max still need to be scanned.
	seen := make(map[string]struct{}, min(naiveCount, effectiveBudget))
	overBudget := false
	add := func(key []byte) bool {
		if minKey == nil || bytes.Compare(key, minKey) < 0 {
			minKey = key
		}
		if maxKey == nil || bytes.Compare(key, maxKey) > 0 {
			maxKey = key
		}
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
	prepared.commit()
	return nil
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
	delete(h.nonCoarsenableTables, table)
	delete(h.remoteUnlockRequired, table)
	// Keep the intent until the whole transaction closes. It owns the service
	// drain reference even after this table was successfully released during a
	// retryable, multi-table cleanup.
	cs.close()
	if len(h.tableKeys) == 0 && len(h.tableBinds) == 0 &&
		len(h.nonCoarsenableTables) == 0 &&
		len(h.remoteUnlockRequired) == 0 &&
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
		for table := range h.nonCoarsenableTables {
			delete(h.nonCoarsenableTables, table)
		}
		for table := range h.remoteUnlockRequired {
			delete(h.remoteUnlockRequired, table)
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
