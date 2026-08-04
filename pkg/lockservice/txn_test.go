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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type retryableUnlockTestTable struct {
	bind      pb.LockTable
	calls     int
	failFirst bool
}

func (l *retryableUnlockTestTable) lock(
	context.Context,
	*activeTxn,
	[][]byte,
	LockOptions,
	func(pb.Result, error),
) {
	panic("unexpected lock")
}

func (l *retryableUnlockTestTable) unlock(
	*activeTxn,
	*cowSlice,
	timestamp.Timestamp,
	...pb.ExtraMutation,
) {
	panic("expected context-aware unlock")
}

func (l *retryableUnlockTestTable) unlockWithContext(
	_ context.Context,
	_ *activeTxn,
	_ *cowSlice,
	_ timestamp.Timestamp,
	_ ...pb.ExtraMutation,
) error {
	l.calls++
	if l.failFirst && l.calls == 1 {
		return context.DeadlineExceeded
	}
	return nil
}

func (l *retryableUnlockTestTable) getLock(context.Context, []byte, pb.WaitTxn, func(Lock)) error {
	panic("unexpected getLock")
}

func (l *retryableUnlockTestTable) getLockHolder(
	context.Context,
	[]byte,
) (pb.WaitTxn, bool, error) {
	return pb.WaitTxn{}, false, errors.New("unexpected getLockHolder")
}

func (l *retryableUnlockTestTable) getBind() pb.LockTable { return l.bind }

func (l *retryableUnlockTestTable) close(closeReason) {}

func TestLockAdded(t *testing.T) {
	reuse.RunReuseTests(func() {
		id := []byte("t1")
		fsp := newFixedSlicePool(2)
		txn := newActiveTxn(id, string(id), fsp, "")
		defer reuse.Free(txn, nil)

		err := txn.lockAdded(0, pb.LockTable{Table: 1}, [][]byte{[]byte("k1")}, getLogger(""))
		assert.NoError(t, err)
		err = txn.lockAdded(0, pb.LockTable{Table: 1}, [][]byte{[]byte("k11")}, getLogger(""))
		assert.NoError(t, err)
		err = txn.lockAdded(0, pb.LockTable{Table: 2}, [][]byte{[]byte("k2"), []byte("k22")}, getLogger(""))
		assert.NoError(t, err)
		assert.Equal(t, 2, len(txn.getHoldLocksLocked(0).tableKeys))

		sp := txn.getHoldLocksLocked(0).tableKeys[1]
		s := sp.slice()
		defer s.unref()
		assert.Equal(t, 2, s.len())

		sp2 := txn.getHoldLocksLocked(0).tableKeys[2]
		s2 := sp2.slice()
		defer s2.unref()
		assert.Equal(t, 2, s2.len())
	})
}

func TestLockAddedThatShouldFail(t *testing.T) {
	reuse.RunReuseTests(func() {
		id := []byte("t1")
		fsp := newFixedSlicePool(2)
		txn := newActiveTxn(id, string(id), fsp, "")
		defer reuse.Free(txn, nil)
		err := txn.lockAdded(0, pb.LockTable{Table: 1}, [][]byte{[]byte("k2"), []byte("k22"), []byte("k222")}, getLogger(""))
		assert.Error(t, err)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrLockNeedUpgrade))
	})
}

func TestCoarsenLockRequestUsesTransactionTableState(t *testing.T) {
	reuse.RunReuseTests(func() {
		fsp := newFixedSlicePool(32)
		txn := newActiveTxn([]byte("t1"), "t1", fsp, "")
		defer reuse.Free(txn, nil)
		bind := pb.LockTable{Group: 1, Table: 10}
		require.NoError(t, txn.lockAdded(
			bind.Group,
			bind,
			[][]byte{[]byte("b"), []byte("d")},
			getLogger(""),
		))

		exclusive := pb.LockOptions{
			Granularity: pb.Granularity_Row,
			Mode:        pb.LockMode_Exclusive,
		}
		rows := [][]byte{[]byte("f")}
		gotRows, gotOpts, replace := txn.coarsenLockRequest(
			bind.Group, bind.Table, rows, exclusive, 3)
		require.False(t, replace)
		require.Equal(t, rows, gotRows)
		require.Equal(t, pb.Granularity_Row, gotOpts.Granularity)

		gotRows, gotOpts, replace = txn.coarsenLockRequest(
			bind.Group,
			bind.Table,
			[][]byte{[]byte("f"), []byte("a")},
			exclusive,
			3,
		)
		require.True(t, replace)
		require.Equal(t, pb.Granularity_Range, gotOpts.Granularity)
		require.Equal(t, [][]byte{[]byte("a"), []byte("f")}, gotRows)

		// Budgets are isolated by physical table and lock group. A first large
		// request is still bounded without consulting planner cardinality.
		gotRows, gotOpts, replace = txn.coarsenLockRequest(
			bind.Group,
			bind.Table+1,
			[][]byte{[]byte("z"), []byte("x"), []byte("y"), []byte("w")},
			exclusive,
			3,
		)
		require.True(t, replace)
		require.Equal(t, pb.Granularity_Range, gotOpts.Granularity)
		require.Equal(t, [][]byte{[]byte("w"), []byte("z")}, gotRows)

		shared := exclusive
		shared.Mode = pb.LockMode_Shared
		sharedRows := [][]byte{[]byte("a"), []byte("z")}
		gotRows, _, replace = txn.coarsenLockRequest(
			bind.Group, bind.Table, sharedRows, shared, 3)
		require.False(t, replace)
		require.Equal(t, sharedRows, gotRows)

		sharded := exclusive
		sharded.Sharding = pb.Sharding_ByRow
		shardedRows := [][]byte{[]byte("z")}
		gotRows, _, replace = txn.coarsenLockRequest(
			bind.Group, bind.Table, shardedRows, sharded, 2)
		require.False(t, replace)
		require.Equal(t, shardedRows, gotRows)

		rangeOptions := exclusive
		rangeOptions.Granularity = pb.Granularity_Range
		gotRows, gotOpts, replace = txn.coarsenLockRequest(
			bind.Group,
			bind.Table,
			[][]byte{[]byte("e"), []byte("h")},
			rangeOptions,
			3,
		)
		require.True(t, replace)
		require.Equal(t, pb.Granularity_Range, gotOpts.Granularity)
		require.Equal(t, [][]byte{[]byte("b"), []byte("h")}, gotRows)

		// Invalid inputs remain invalid so validation stays in the lock-table
		// layer; coarsening must not silently repair them.
		malformedRange := [][]byte{[]byte("e")}
		gotRows, _, replace = txn.coarsenLockRequest(
			bind.Group, bind.Table, malformedRange, rangeOptions, 1)
		require.False(t, replace)
		require.Equal(t, malformedRange, gotRows)
		unsupported := exclusive
		unsupported.Granularity = pb.Granularity(99)
		gotRows, _, replace = txn.coarsenLockRequest(
			bind.Group, bind.Table, rows, unsupported, 1)
		require.False(t, replace)
		require.Equal(t, rows, gotRows)

		// Re-entrant duplicates compact to one row, not an invalid range whose
		// start equals its end.
		duplicates := [][]byte{[]byte("q"), []byte("q"), []byte("q"), []byte("q")}
		gotRows, gotOpts, replace = txn.coarsenLockRequest(
			bind.Group, bind.Table+2, duplicates, exclusive, 3)
		require.True(t, replace)
		require.Equal(t, pb.Granularity_Row, gotOpts.Granularity)
		require.Equal(t, [][]byte{[]byte("q")}, gotRows)
	})
}

func TestReplaceLocksFailurePreservesOwnership(t *testing.T) {
	reuse.RunReuseTests(func() {
		fsp := newFixedSlicePool(8)
		txn := newActiveTxn([]byte("t1"), "t1", fsp, "")
		defer reuse.Free(txn, nil)
		bind := pb.LockTable{Group: 1, Table: 10}
		original := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
		require.NoError(t, txn.lockAdded(bind.Group, bind, original, getLogger("")))

		expectedErr := errors.New("injected bookkeeping failure")
		txn.beforeLockAdded = func([]byte, [][]byte) error { return expectedErr }
		err := txn.replaceLocks(
			bind.Group,
			bind,
			[][]byte{[]byte("a"), []byte("z")},
			getLogger(""),
		)
		require.ErrorIs(t, err, expectedErr)
		locks := txn.lockHolders[bind.Group].tableKeys[bind.Table].slice()
		require.Equal(t, original, locks.all())
		locks.unref()

		txn.beforeLockAdded = nil
		err = txn.replaceLocks(
			bind.Group,
			bind,
			newTestRows(1, 2, 3, 4, 5, 6, 7, 8, 9),
			getLogger(""),
		)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockNeedUpgrade))
		locks = txn.lockHolders[bind.Group].tableKeys[bind.Table].slice()
		require.Equal(t, original, locks.all())
		locks.unref()

		require.NoError(t, txn.replaceLocks(
			bind.Group,
			bind,
			[][]byte{[]byte("a"), []byte("z")},
			getLogger(""),
		))
		locks = txn.lockHolders[bind.Group].tableKeys[bind.Table].slice()
		require.Equal(t, [][]byte{[]byte("a"), []byte("z")}, locks.all())
		locks.unref()
	})
}

func TestLockTableBindTouchedTracksFenceIntentOnly(t *testing.T) {
	reuse.RunReuseTests(func() {
		id := []byte("t1")
		fsp := newFixedSlicePool(2)
		txn := newActiveTxn(id, string(id), fsp, "")
		defer reuse.Free(txn, nil)

		bind := pb.LockTable{Group: 0, Table: 1, ServiceID: "s1", Version: 1}
		assert.True(t, txn.lockTableBindTouched(bind))
		assert.False(t, txn.lockTableBindTouched(bind))

		h := txn.getHoldLocksLocked(bind.Group)
		assert.Empty(t, h.tableBinds)
		assert.Equal(t, bind, h.tableBindIntents[bind.Table])

		refs := make(map[uint32]map[uint64]uint64)
		txn.incLockTableRef(refs, bind.ServiceID)
		assert.Equal(t, uint64(1), refs[bind.Group][bind.Table])

		changed := bind
		changed.Version++
		assert.True(t, txn.fenceByBindChanged(changed, getLogger("")))
		assert.True(t, txn.bindChanged)

		txn.reset()
		assert.Empty(t, txn.lockHolders)
	})
}

func TestFetchWhoWaitingMeSkipsInactiveWaiters(t *testing.T) {
	reuse.RunReuseTests(func() {
		logger := getLogger("")
		bind := pb.LockTable{Group: 0, Table: 1, ServiceID: "owner"}
		key := []byte("key")
		holderID := []byte("holder")

		txn := newActiveTxn(holderID, string(holderID), newFixedSlicePool(2), "")
		defer reuse.Free(txn, nil)
		require.NoError(t, txn.lockAdded(0, bind, [][]byte{key}, logger))

		lt := newLocalLockTable(
			bind,
			nil,
			nil,
			runtime.DefaultRuntime().Clock(),
			nil,
			logger,
		).(*localLockTable)

		holders := newHolders()
		holders.add(pb.WaitTxn{TxnID: holderID, CreatedOn: "origin"})
		waiterQueue := newWaiterQueue()
		waiterQueue.init(logger)

		completedWaiter := acquireWaiter(pb.WaitTxn{TxnID: []byte("completed")}, "test", logger)
		completedWaiter.setStatus(completed)
		defer completedWaiter.close("test", logger)

		notifiedWaiter := acquireWaiter(pb.WaitTxn{TxnID: []byte("notified")}, "test", logger)
		notifiedWaiter.setStatus(notified)
		defer notifiedWaiter.close("test", logger)

		blockingWaiter := acquireWaiter(pb.WaitTxn{TxnID: []byte("blocking")}, "test", logger)
		blockingWaiter.setStatus(blocking)
		defer blockingWaiter.close("test", logger)

		waiterQueue.put(completedWaiter, notifiedWaiter, blockingWaiter)
		defer func() {
			removed, _ := waiterQueue.remove(completedWaiter)
			require.True(t, removed)
			removed, _ = waiterQueue.remove(notifiedWaiter)
			require.True(t, removed)
			removed, _ = waiterQueue.remove(blockingWaiter)
			require.True(t, removed)
		}()

		lt.mu.store.Add(key, Lock{
			value:    flagLockRow | flagLockExclusiveMode,
			createAt: time.Now(),
			holders:  holders,
			waiters:  waiterQueue,
		})

		var waitingTxnIDs [][]byte
		ok, err := txn.fetchWhoWaitingMe(
			context.Background(),
			"origin",
			holderID,
			func(waitTxn pb.WaitTxn, waiterAddress string) bool {
				waitingTxnIDs = append(waitingTxnIDs, waitTxn.TxnID)
				assert.Equal(t, bind.ServiceID, waiterAddress)
				return true
			},
			func(_ context.Context, group uint32, table uint64) (lockTable, error) {
				assert.Equal(t, bind.Group, group)
				assert.Equal(t, bind.Table, table)
				return lt, nil
			},
		)

		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, [][]byte{[]byte("blocking")}, waitingTxnIDs)
	})
}

func TestClose(t *testing.T) {
	reuse.RunReuseTests(func() {
		events := newWaiterEvents(1, nil, nil, time.Second, nil, getLogger(""))
		defer events.close()

		id := []byte("t1")
		fsp := newFixedSlicePool(2)
		txn := newActiveTxn(id, string(id), fsp, "")
		tables := map[uint64]lockTable{
			1: newLocalLockTable(pb.LockTable{Table: 1}, nil, events, runtime.DefaultRuntime().Clock(), nil, getLogger("")),
			2: newLocalLockTable(pb.LockTable{Table: 2}, nil, events, runtime.DefaultRuntime().Clock(), nil, getLogger("")),
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()

		tables[1].lock(ctx, txn, [][]byte{[]byte("k1")}, LockOptions{}, func(r pb.Result, err error) {
			assert.NoError(t, err)
		})

		tables[2].lock(ctx, txn, [][]byte{[]byte("k2")}, LockOptions{}, func(r pb.Result, err error) {
			assert.NoError(t, err)
		})

		txn.close(
			txn.txnID,
			timestamp.Timestamp{},
			func(group uint32, table uint64) (lockTable, error) {
				return tables[table], nil
			},
			getLogger(""),
		)
		assert.Empty(t, txn.txnID)
		assert.Empty(t, txn.txnKey)
		assert.Empty(t, txn.blockedWaiters)
		assert.Empty(t, txn.getHoldLocksLocked(0).tableKeys)
		assert.Empty(t, txn.getHoldLocksLocked(0).tableBinds)
		assert.Equal(t, 0, tables[1].(*localLockTable).mu.store.Len())
		assert.Equal(t, 0, tables[2].(*localLockTable).mu.store.Len())
	})
}

func TestCloseWithoutFreeWithContextRetriesOnlyFailedTables(t *testing.T) {
	reuse.RunReuseTests(func() {
		id := []byte("unknown-commit")
		txn := newActiveTxn(id, string(id), newFixedSlicePool(2), "")
		defer reuse.Free(txn, nil)

		tables := map[uint64]*retryableUnlockTestTable{
			1: {bind: pb.LockTable{Group: 0, Table: 1}},
			2: {bind: pb.LockTable{Group: 0, Table: 2}, failFirst: true},
		}
		require.NoError(t, txn.lockAdded(0, tables[1].bind, [][]byte{[]byte("k1")}, getLogger("")))
		require.NoError(t, txn.lockAdded(0, tables[2].bind, [][]byte{[]byte("k2")}, getLogger("")))

		err := txn.closeWithoutFreeWithContext(
			context.Background(),
			id,
			timestamp.Timestamp{},
			func(_ uint32, table uint64) (lockTable, error) {
				return tables[table], nil
			},
			getLogger(""),
		)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, 1, tables[1].calls)
		require.Equal(t, 1, tables[2].calls)
		holder := txn.getHoldLocksLocked(0)
		require.NotContains(t, holder.tableKeys, uint64(1))
		require.Contains(t, holder.tableKeys, uint64(2))

		require.NoError(t, txn.closeWithoutFreeWithContext(
			context.Background(),
			id,
			timestamp.Timestamp{},
			func(_ uint32, table uint64) (lockTable, error) {
				return tables[table], nil
			},
			getLogger(""),
		))
		require.Equal(t, 1, tables[1].calls, "successful tables must not be replayed")
		require.Equal(t, 2, tables[2].calls)
		require.Empty(t, txn.lockHolders)
	})
}

func TestCloseWithoutFreeWithContextReturnsCanceledLookup(t *testing.T) {
	reuse.RunReuseTests(func() {
		id := []byte("canceled-lookup")
		txn := newActiveTxn(id, string(id), newFixedSlicePool(2), "")
		defer reuse.Free(txn, nil)

		bind := pb.LockTable{Group: 0, Table: 1}
		require.NoError(t, txn.lockAdded(0, bind, [][]byte{[]byte("k1")}, getLogger("")))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := txn.closeWithoutFreeWithContext(
			ctx,
			id,
			timestamp.Timestamp{},
			func(uint32, uint64) (lockTable, error) {
				return nil, ctx.Err()
			},
			getLogger(""),
		)
		require.ErrorIs(t, err, context.Canceled)
		require.Contains(t, txn.getHoldLocksLocked(0).tableKeys, bind.Table,
			"canceled cleanup must retain the table for a later retry")
	})
}
