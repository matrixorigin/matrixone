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

func (l *retryableUnlockTestTable) getLock([]byte, pb.WaitTxn, func(Lock)) {
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

func TestLockTableBindTouchedTracksFenceIntentOnly(t *testing.T) {
	reuse.RunReuseTests(func() {
		id := []byte("t1")
		fsp := newFixedSlicePool(2)
		txn := newActiveTxn(id, string(id), fsp, "")
		defer reuse.Free(txn, nil)

		bind := pb.LockTable{Group: 0, Table: 1, ServiceID: "s1", Version: 1}
		txn.lockTableBindTouched(bind)

		h := txn.getHoldLocksLocked(bind.Group)
		assert.Empty(t, h.tableBinds)
		assert.Equal(t, bind, h.tableBindIntents[bind.Table])

		refs := make(map[uint32]map[uint64]uint64)
		txn.incLockTableRef(refs, bind.ServiceID)
		assert.Empty(t, refs)

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
		ok := txn.fetchWhoWaitingMe(
			"origin",
			holderID,
			func(waitTxn pb.WaitTxn, waiterAddress string) bool {
				waitingTxnIDs = append(waitingTxnIDs, waitTxn.TxnID)
				assert.Equal(t, bind.ServiceID, waiterAddress)
				return true
			},
			func(group uint32, table uint64) (lockTable, error) {
				assert.Equal(t, bind.Group, group)
				assert.Equal(t, bind.Table, table)
				return lt, nil
			},
		)

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
