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

	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

// unlockAfterApplyErrorTable simulates an owner that applies an Unlock before
// the source CN loses the response.
type unlockAfterApplyErrorTable struct {
	lockTable
	err error
}

func (t *unlockAfterApplyErrorTable) unlockWithContext(
	_ context.Context,
	txn *activeTxn,
	locks *cowSlice,
	commitTS timestamp.Timestamp,
	mutations ...pb.ExtraMutation,
) error {
	t.lockTable.unlock(txn, locks, commitTS, mutations...)
	return t.err
}

func TestProxySharedLock(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, s []*service) {
			tableID := uint64(10)
			s1 := s[0]
			s2 := s[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
			defer cancel()

			option := newTestRowSharedOptions()
			rows := newTestRows(1)
			txn1 := newTestTxnID(1)
			txn2 := newTestTxnID(2)
			txn3 := newTestTxnID(3)
			txn4 := newTestTxnID(4)
			// txn5 := newTestTxnID(5)

			s1.cfg.EnableRemoteLocalProxy = true
			_, err := s1.Lock(ctx, tableID, rows, txn1, option)
			require.NoError(t, err, err)
			require.NoError(t, s1.Unlock(ctx, txn1, timestamp.Timestamp{}))

			v := s1.tableGroups.get(0, tableID)
			lt := v.(*localLockTable)

			// s2 will enable shared remote proxy
			s2.cfg.EnableRemoteLocalProxy = true
			_, err = s2.Lock(ctx, tableID, rows, txn2, option)
			require.NoError(t, err)
			checkLock(t, lt, rows[0], [][]byte{txn2}, nil, nil)
			v = s2.tableGroups.get(0, tableID)
			ltp := v.(*localLockTableProxy)
			require.Equal(t, ltp.mu.currentHolder[string(rows[0])], txn2)
			require.Equal(t, 1, len(ltp.mu.holders[string(rows[0])].txns))
			require.Equal(t, ltp.mu.holders[string(rows[0])].txns[0].txnID, txn2)
			require.True(t, ltp.mu.holders[string(rows[0])].cbs[0] == nil)
			require.True(t, ltp.mu.holders[string(rows[0])].waiters[0] == nil)

			_, err = s2.Lock(ctx, tableID, rows, txn3, option)
			require.NoError(t, err)
			checkLock(t, lt, rows[0], [][]byte{txn2}, nil, nil)
			require.Equal(t, ltp.mu.currentHolder[string(rows[0])], txn2)
			require.Equal(t, 2, len(ltp.mu.holders[string(rows[0])].txns))
			require.Equal(t, ltp.mu.holders[string(rows[0])].txns[1].txnID, txn3)
			require.True(t, ltp.mu.holders[string(rows[0])].cbs[1] == nil)
			require.True(t, ltp.mu.holders[string(rows[0])].waiters[1] == nil)

			_, err = s2.Lock(ctx, tableID, rows, txn4, option)
			require.NoError(t, err)
			checkLock(t, lt, rows[0], [][]byte{txn2}, nil, nil)
			require.Equal(t, ltp.mu.currentHolder[string(rows[0])], txn2)
			require.Equal(t, 3, len(ltp.mu.holders[string(rows[0])].txns))
			require.Equal(t, ltp.mu.holders[string(rows[0])].txns[2].txnID, txn4)
			require.True(t, ltp.mu.holders[string(rows[0])].cbs[2] == nil)
			require.True(t, ltp.mu.holders[string(rows[0])].waiters[2] == nil)

			// require.NoError(t, s2.Unlock(ctx, txn2, timestamp.Timestamp{}))
			// checkLock(t, lt, rows[0], [][]byte{txn4}, nil, nil)
			// require.Equal(t, ltp.mu.currentHolder[string(rows[0])], txn4)

			// require.NoError(t, s2.Unlock(ctx, txn4, timestamp.Timestamp{}))
			// checkLock(t, lt, rows[0], [][]byte{txn3}, nil, nil)
			// require.Equal(t, ltp.mu.currentHolder[string(rows[0])], txn3)

			// require.NoError(t, s2.Unlock(ctx, txn3, timestamp.Timestamp{}))
			// checkLock(t, lt, rows[0], [][]byte{}, nil, nil)
			// require.Empty(t, ltp.mu.currentHolder)

			// _, err = s1.Lock(ctx, tableID, rows, txn5, newTestRowExclusiveOptions())
			// require.NoError(t, err, err)
			// require.NoError(t, s1.Unlock(ctx, txn5, timestamp.Timestamp{}))
		},
	)
}

func TestProxySharedUnlock(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, s []*service) {
			tableID := uint64(10)
			s1 := s[0]
			s2 := s[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
			defer cancel()

			option := newTestRowSharedOptions()
			rows := newTestRows(1)
			txn1 := newTestTxnID(1)
			txn2 := newTestTxnID(2)
			txn3 := newTestTxnID(3)
			txn4 := newTestTxnID(3)

			s1.cfg.EnableRemoteLocalProxy = true
			_, err := s1.Lock(ctx, tableID, rows, txn1, option)
			require.NoError(t, err, err)
			require.NoError(t, s1.Unlock(ctx, txn1, timestamp.Timestamp{}))

			v := s1.tableGroups.get(0, tableID)
			lt := v.(*localLockTable)

			// s2 will enable shared remote proxy
			s2.cfg.EnableRemoteLocalProxy = true
			_, err = s2.Lock(ctx, tableID, rows, txn2, option)
			require.NoError(t, err)

			_, err = s2.Lock(ctx, tableID, rows, txn3, option)
			require.NoError(t, err)
			require.NoError(t, s2.Unlock(ctx, txn3, timestamp.Timestamp{}))

			require.NoError(t, s2.Unlock(ctx, txn2, timestamp.Timestamp{}))
			checkLock(t, lt, rows[0], [][]byte{}, nil, nil)

			_, err = s1.Lock(ctx, tableID, rows, txn4, newTestRowExclusiveOptions())
			require.NoError(t, err, err)
			require.NoError(t, s1.Unlock(ctx, txn4, timestamp.Timestamp{}))
		},
	)
}

func TestProxyUnlockCleansBookkeepingAfterContextExpiry(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(_ *lockTableAllocator, services []*service) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			const tableID = uint64(10)
			row := []byte("row")
			seedTxn := []byte("seed")
			timedOutTxn := []byte("timed-out")
			survivorTxn := []byte("survivor")
			s1 := services[0]
			s2 := services[1]

			// Make s1 the table owner, then consolidate two shared holders on
			// s2 behind one remote holder.
			s1.cfg.EnableRemoteLocalProxy = true
			_, err := s1.Lock(ctx, tableID, [][]byte{row}, seedTxn, newTestRowSharedOptions())
			require.NoError(t, err)
			require.NoError(t, s1.Unlock(ctx, seedTxn, timestamp.Timestamp{}))

			s2.cfg.EnableRemoteLocalProxy = true
			_, err = s2.Lock(ctx, tableID, [][]byte{row}, timedOutTxn, newTestRowSharedOptions())
			require.NoError(t, err)
			_, err = s2.Lock(ctx, tableID, [][]byte{row}, survivorTxn, newTestRowSharedOptions())
			require.NoError(t, err)

			proxy := s2.tableGroups.get(0, tableID).(*localLockTableProxy)
			timedOut := s2.activeTxnHolder.getActiveTxn(timedOutTxn, false, "")
			survivor := s2.activeTxnHolder.getActiveTxn(survivorTxn, false, "")
			require.NotNil(t, timedOut)
			require.NotNil(t, survivor)
			s2.unknownCommitResolver.mu.Lock()
			s2.unknownCommitResolver.mu.pending[string(timedOutTxn)] = unknownCommitTxn{
				id: timedOutTxn,
			}
			s2.unknownCommitResolver.mu.Unlock()

			// Expire the resolver context while the unknown-commit resolver is
			// waiting on the source txn mutex. It must leave both proxy and owner
			// on the old holder so a later orphan cleanup cannot release a lock
			// that the proxy still treats as held by survivor.
			timedOut.Lock()
			unlockCtx, unlockCancel := context.WithCancel(context.Background())
			defer unlockCancel()
			unlocked := make(chan error, 1)
			go func() {
				unlocked <- s2.unlockUnknownCommit(
					unlockCtx,
					timedOutTxn,
					timestamp.Timestamp{},
				)
			}()
			require.Eventually(t, func() bool {
				return s2.activeTxnHolder.hasActiveTxn(timedOutTxn)
			}, time.Second, time.Millisecond)
			unlockCancel()
			timedOut.Unlock()

			require.ErrorIs(t, <-unlocked, context.Canceled)

			// The failed handoff cannot publish survivor as the remote holder.
			// Keeping the old source txn active makes CheckActiveTxn report it as
			// live even after the frontend transaction is gone.
			proxy.mu.RLock()
			shared := proxy.mu.holders[string(row)]
			require.NotNil(t, shared)
			require.Len(t, shared.txns, 2)
			require.Same(t, timedOut, shared.txns[0])
			require.Same(t, survivor, shared.txns[1])
			require.Equal(t, timedOutTxn, proxy.mu.currentHolder[string(row)])
			proxy.mu.RUnlock()

			holder, ok, err := s1.tableGroups.get(0, tableID).getLockHolder(ctx, row)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, timedOutTxn, holder.TxnID)

			// Exercise the owner-side orphan path. The unknown-commit resolver
			// still owns the source holder, so the owner must not use
			// CannotCommit to release it before ReplaceTo has been acknowledged.
			owner := s1.tableGroups.get(0, tableID).(*localLockTable)
			s1.activeTxnHolder.keepRemoteLockBindActive(s2.serviceID, owner.bind)
			s1.events.checkOrphan(checkOrphan{
				wait: waitTooLong,
				key:  row,
				lt:   owner,
				txn:  pb.WaitTxn{TxnID: []byte("owner-waiter"), CreatedOn: s1.serviceID},
			})

			holder, ok, err = owner.getLockHolder(ctx, row)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, timedOutTxn, holder.TxnID)
		},
	)
}

func TestProxyUnlockRetryIgnoresNewOwnerAfterLostLastHolderResponse(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(_ *lockTableAllocator, services []*service) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			const tableID = uint64(10)
			row := []byte("row")
			seedTxn := []byte("seed")
			timedOutTxn := []byte("timed-out")
			newOwnerTxn := []byte("new-owner")
			s1 := services[0]
			s2 := services[1]

			// Make s1 the table owner, then acquire one shared proxy holder on s2.
			s1.cfg.EnableRemoteLocalProxy = true
			_, err := s1.Lock(ctx, tableID, [][]byte{row}, seedTxn, newTestRowSharedOptions())
			require.NoError(t, err)
			require.NoError(t, s1.Unlock(ctx, seedTxn, timestamp.Timestamp{}))

			s2.cfg.EnableRemoteLocalProxy = true
			_, err = s2.Lock(ctx, tableID, [][]byte{row}, timedOutTxn, newTestRowSharedOptions())
			require.NoError(t, err)

			proxy := s2.tableGroups.get(0, tableID).(*localLockTableProxy)
			owner := s1.tableGroups.get(0, tableID).(*localLockTable)
			remote := proxy.remote
			proxy.remote = &unlockAfterApplyErrorTable{
				lockTable: owner,
				err:       errors.New("unlock response lost"),
			}

			// The owner executes the first Unlock, but the source does not receive
			// the response and therefore retains the old proxy holder for retry.
			err = s2.unlockUnknownCommit(ctx, timedOutTxn, timestamp.Timestamp{})
			require.EqualError(t, err, "unlock response lost")

			holder, ok, err := owner.getLockHolder(ctx, row)
			require.NoError(t, err)
			require.False(t, ok)
			require.Empty(t, holder.TxnID)

			// A different owner-local transaction may acquire the row before the
			// source retries its already-applied Unlock.
			_, err = s1.Lock(ctx, tableID, [][]byte{row}, newOwnerTxn, newTestRowExclusiveOptions())
			require.NoError(t, err)

			proxy.remote = remote
			require.NoError(t, s2.unlockUnknownCommit(ctx, timedOutTxn, timestamp.Timestamp{}))

			holder, ok, err = owner.getLockHolder(ctx, row)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, newOwnerTxn, holder.TxnID)
			require.NoError(t, s1.Unlock(ctx, newOwnerTxn, timestamp.Timestamp{}))
		},
	)
}
