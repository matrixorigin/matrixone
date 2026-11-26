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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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

// unlockBeforeApplyErrorTable simulates a request that fails before the owner
// receives it.
type unlockBeforeApplyErrorTable struct {
	lockTable
	err error
}

type recordingUnlockTable struct {
	lockTable
	mutations []pb.ExtraMutation
}

type blockingProxyLockTable struct {
	lockTable
	bind    pb.LockTable
	started chan struct{}
	release chan struct{}
	err     error
}

func (t *blockingProxyLockTable) lock(
	ctx context.Context,
	_ *activeTxn,
	_ [][]byte,
	_ LockOptions,
	cb func(pb.Result, error),
) {
	select {
	case t.started <- struct{}{}:
	default:
	}
	select {
	case <-t.release:
		cb(pb.Result{}, t.err)
	case <-ctx.Done():
		cb(pb.Result{}, ctx.Err())
	}
}

func (t *blockingProxyLockTable) getBind() pb.LockTable {
	return t.bind
}

func (t *recordingUnlockTable) unlockWithContext(
	ctx context.Context,
	txn *activeTxn,
	locks *cowSlice,
	commitTS timestamp.Timestamp,
	mutations ...pb.ExtraMutation,
) error {
	t.mutations = append(t.mutations, mutations...)
	if unlocker, ok := t.lockTable.(contextUnlocker); ok {
		return unlocker.unlockWithContext(ctx, txn, locks, commitTS, mutations...)
	}
	t.lockTable.unlock(txn, locks, commitTS, mutations...)
	return nil
}

func (t *unlockAfterApplyErrorTable) unlockWithContext(
	ctx context.Context,
	txn *activeTxn,
	locks *cowSlice,
	commitTS timestamp.Timestamp,
	mutations ...pb.ExtraMutation,
) error {
	if unlocker, ok := t.lockTable.(contextUnlocker); ok {
		if err := unlocker.unlockWithContext(ctx, txn, locks, commitTS, mutations...); err != nil {
			return err
		}
	} else {
		t.lockTable.unlock(txn, locks, commitTS, mutations...)
	}
	return t.err
}

func (t *unlockBeforeApplyErrorTable) unlockWithContext(
	_ context.Context,
	_ *activeTxn,
	_ *cowSlice,
	_ timestamp.Timestamp,
	_ ...pb.ExtraMutation,
) error {
	return t.err
}

func TestProxySharedLockCancellationWhileFirstRemoteLockInFlight(t *testing.T) {
	for _, remoteErr := range []error{nil, errors.New("remote lock failed")} {
		name := "remote-success"
		if remoteErr != nil {
			name = "remote-failure"
		}
		t.Run(name, func(t *testing.T) {
			bind := pb.LockTable{
				Group:     0,
				Table:     1,
				ServiceID: "remote",
				Valid:     true,
			}
			remote := &blockingProxyLockTable{
				bind:    bind,
				started: make(chan struct{}, 1),
				release: make(chan struct{}),
				err:     remoteErr,
			}
			proxy := newLockTableProxy("local", remote, getLogger("")).(*localLockTableProxy)
			rows := [][]byte{[]byte("row")}
			options := LockOptions{LockOptions: newTestRowSharedOptions()}
			firstTxn := newActiveTxn([]byte("first"), "first", newFixedSlicePool(4), "")
			secondTxn := newActiveTxn([]byte("second"), "second", newFixedSlicePool(4), "")

			firstDone := make(chan error, 1)
			go func() {
				firstTxn.Lock()
				defer firstTxn.Unlock()
				proxy.lock(context.Background(), firstTxn, rows, options, func(_ pb.Result, err error) {
					firstDone <- err
				})
			}()
			select {
			case <-remote.started:
			case <-time.After(time.Second):
				t.Fatal("first remote lock did not start")
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var secondCallbacks atomic.Int32
			var secondLockAdded atomic.Int32
			secondTxn.beforeLockAdded = func([]byte, [][]byte) error {
				secondLockAdded.Add(1)
				return nil
			}
			secondDone := make(chan error, 2)
			go func() {
				secondTxn.Lock()
				defer secondTxn.Unlock()
				proxy.lock(ctx, secondTxn, rows, options, func(_ pb.Result, err error) {
					secondCallbacks.Add(1)
					secondDone <- err
				})
			}()

			require.Eventually(t, func() bool {
				proxy.mu.Lock()
				defer proxy.mu.Unlock()
				ops := proxy.mu.holders[string(rows[0])]
				return ops != nil && len(ops.txns) == 2 && ops.waiters[1] != nil
			}, time.Second, time.Millisecond, "second shared lock was not admitted")

			cancel()
			select {
			case err := <-secondDone:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("second shared lock ignored cancellation")
			}
			require.Equal(t, int32(1), secondCallbacks.Load())
			require.Zero(t, secondLockAdded.Load())
			proxy.mu.Lock()
			ops := proxy.mu.holders[string(rows[0])]
			require.Len(t, ops.txns, 1)
			require.Same(t, firstTxn, ops.txns[0])
			proxy.mu.Unlock()

			close(remote.release)
			select {
			case err := <-firstDone:
				if remoteErr == nil {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, remoteErr)
				}
			case <-time.After(time.Second):
				t.Fatal("first remote lock did not finish")
			}
			require.Equal(t, int32(1), secondCallbacks.Load())
			require.Zero(t, secondLockAdded.Load())
			select {
			case err := <-secondDone:
				t.Fatalf("second callback ran more than once: %v", err)
			default:
			}
		})
	}
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

func TestUnknownCommitYieldsWithoutChangingProxyHolder(t *testing.T) {
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

			// The resolver must yield while the source transaction is busy,
			// preserving the proxy and owner holder for its next attempt.
			timedOut.Lock()
			releaseTxn := sync.OnceFunc(timedOut.Unlock)
			defer releaseTxn()
			unlocked := make(chan error, 1)
			go func() {
				unlocked <- s2.unlockUnknownCommit(ctx, timedOutTxn, timestamp.Timestamp{})
			}()
			select {
			case err := <-unlocked:
				require.ErrorIs(t, err, ErrLockTimeout)
			case <-ctx.Done():
				t.Fatal("unknown-commit cleanup waited for the source transaction mutex")
			}
			releaseTxn()

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
				lockTable: remote,
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

func TestProxyLostLastHolderResponseRoutesLateSharerThroughOwner(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(_ *lockTableAllocator, services []*service) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			const tableID = uint64(10)
			row := []byte("row")
			seedTxn := []byte("seed")
			unknownTxn := []byte("unknown")
			exclusiveTxn := []byte("exclusive")
			lateSharerTxn := []byte("late-sharer")
			s1 := services[0]
			s2 := services[1]

			// Make s1 the owner and leave one proxied shared holder on s2.
			s1.cfg.EnableRemoteLocalProxy = true
			_, err := s1.Lock(ctx, tableID, [][]byte{row}, seedTxn, newTestRowSharedOptions())
			require.NoError(t, err)
			require.NoError(t, s1.Unlock(ctx, seedTxn, timestamp.Timestamp{}))

			s2.cfg.EnableRemoteLocalProxy = true
			_, err = s2.Lock(ctx, tableID, [][]byte{row}, unknownTxn, newTestRowSharedOptions())
			require.NoError(t, err)

			proxy := s2.tableGroups.get(0, tableID).(*localLockTableProxy)
			owner := s1.tableGroups.get(0, tableID).(*localLockTable)
			remote := proxy.remote
			proxy.remote = &unlockAfterApplyErrorTable{
				lockTable: remote,
				err:       errors.New("unlock response lost"),
			}

			// The owner removes the last shared holder, but s2 cannot observe the
			// acknowledgement and retains it for an idempotent retry.
			err = s2.unlockUnknownCommit(ctx, unknownTxn, timestamp.Timestamp{})
			require.EqualError(t, err, "unlock response lost")
			_, ok, err := owner.getLockHolder(ctx, row)
			require.NoError(t, err)
			require.False(t, ok)
			proxy.mu.RLock()
			pendingValue, pendingUnlock := proxy.mu.pendingRemoteHolders[string(row)]
			proxy.mu.RUnlock()
			require.True(t, pendingUnlock)
			require.Nil(t, pendingValue)

			// An owner-local exclusive can acquire before the proxy retries.
			_, err = s1.Lock(ctx, tableID, [][]byte{row}, exclusiveTxn, newTestRowExclusiveOptions())
			require.NoError(t, err)

			// A late proxy sharer must reach the owner and wait behind that
			// exclusive instead of using the stale local shared holder.
			proxy.remote = remote
			lateSharerDone := make(chan error, 1)
			go func() {
				_, err := s2.Lock(ctx, tableID, [][]byte{row}, lateSharerTxn, newTestRowSharedOptions())
				lateSharerDone <- err
			}()
			waitWaiters(t, s1, tableID, row, 1)

			// Converging the stale last-holder removal must preserve both the
			// exclusive holder and its queued late sharer.
			require.NoError(t, s2.unlockUnknownCommit(ctx, unknownTxn, timestamp.Timestamp{}))
			holder, ok, err := owner.getLockHolder(ctx, row)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, exclusiveTxn, holder.TxnID)
			proxy.mu.RLock()
			_, pendingUnlock = proxy.mu.pendingRemoteHolders[string(row)]
			proxy.mu.RUnlock()
			require.False(t, pendingUnlock)

			require.NoError(t, s1.Unlock(ctx, exclusiveTxn, timestamp.Timestamp{}))
			select {
			case err := <-lateSharerDone:
				require.NoError(t, err)
			case <-ctx.Done():
				require.NoError(t, ctx.Err())
			}
			holder, ok, err = owner.getLockHolder(ctx, row)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, lateSharerTxn, holder.TxnID)
			require.NoError(t, s2.Unlock(ctx, lateSharerTxn, timestamp.Timestamp{}))
		},
	)
}

func TestProxySurvivorUnlockReleasesLostHandoffHolder(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(_ *lockTableAllocator, services []*service) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			const tableID = uint64(10)
			row := []byte("row")
			seedTxn := []byte("seed")
			unknownTxn := []byte("unknown")
			survivorTxn := []byte("survivor")
			s1 := services[0]
			s2 := services[1]

			s1.cfg.EnableRemoteLocalProxy = true
			_, err := s1.Lock(ctx, tableID, [][]byte{row}, seedTxn, newTestRowSharedOptions())
			require.NoError(t, err)
			require.NoError(t, s1.Unlock(ctx, seedTxn, timestamp.Timestamp{}))

			s2.cfg.EnableRemoteLocalProxy = true
			_, err = s2.Lock(ctx, tableID, [][]byte{row}, unknownTxn, newTestRowSharedOptions())
			require.NoError(t, err)
			_, err = s2.Lock(ctx, tableID, [][]byte{row}, survivorTxn, newTestRowSharedOptions())
			require.NoError(t, err)

			proxy := s2.tableGroups.get(0, tableID).(*localLockTableProxy)
			owner := s1.tableGroups.get(0, tableID).(*localLockTable)
			remote := proxy.remote
			proxy.remote = &unlockAfterApplyErrorTable{
				lockTable: remote,
				err:       errors.New("unlock response lost"),
			}

			// The owner applies unknown -> survivor, but s2 loses the response
			// and therefore still records unknown as its current remote holder.
			err = s2.unlockUnknownCommit(ctx, unknownTxn, timestamp.Timestamp{})
			require.EqualError(t, err, "unlock response lost")
			holder, ok, err := owner.getLockHolder(ctx, row)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, survivorTxn, holder.TxnID)
			proxy.mu.RLock()
			require.Equal(t, unknownTxn, proxy.mu.currentHolder[string(row)])
			require.Equal(t, survivorTxn, proxy.mu.pendingRemoteHolders[string(row)])
			proxy.mu.RUnlock()
			require.True(t, s1.activeTxnHolder.hasActiveTxn(survivorTxn))

			recordingRemote := &recordingUnlockTable{lockTable: remote}
			proxy.remote = recordingRemote
			// A normal survivor Unlock must not be skipped merely because the
			// proxy has not acknowledged the previous handoff. It conditionally
			// transfers the owner back to unknown, allowing the resolver retry to
			// release it instead of leaving survivor at the remote owner.
			require.NoError(t, s2.Unlock(ctx, survivorTxn, timestamp.Timestamp{}))
			require.Len(t, recordingRemote.mutations, 1)
			require.Equal(t, row, recordingRemote.mutations[0].Key)
			require.Equal(t, unknownTxn, recordingRemote.mutations[0].ReplaceTo)
			require.False(t, s1.activeTxnHolder.hasActiveTxn(survivorTxn))
			holder, ok, err = owner.getLockHolder(ctx, row)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, unknownTxn, holder.TxnID)

			require.NoError(t, s2.unlockUnknownCommit(ctx, unknownTxn, timestamp.Timestamp{}))
			_, ok, err = owner.getLockHolder(ctx, row)
			require.NoError(t, err)
			require.False(t, ok)
		},
	)
}

func TestProxySurvivorUnlockHandlesUnappliedHandoff(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(_ *lockTableAllocator, services []*service) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			const tableID = uint64(10)
			row := []byte("row")
			seedTxn := []byte("seed")
			unknownTxn := []byte("unknown")
			survivorTxn := []byte("survivor")
			s1 := services[0]
			s2 := services[1]

			s1.cfg.EnableRemoteLocalProxy = true
			_, err := s1.Lock(ctx, tableID, [][]byte{row}, seedTxn, newTestRowSharedOptions())
			require.NoError(t, err)
			require.NoError(t, s1.Unlock(ctx, seedTxn, timestamp.Timestamp{}))

			s2.cfg.EnableRemoteLocalProxy = true
			_, err = s2.Lock(ctx, tableID, [][]byte{row}, unknownTxn, newTestRowSharedOptions())
			require.NoError(t, err)
			_, err = s2.Lock(ctx, tableID, [][]byte{row}, survivorTxn, newTestRowSharedOptions())
			require.NoError(t, err)

			proxy := s2.tableGroups.get(0, tableID).(*localLockTableProxy)
			owner := s1.tableGroups.get(0, tableID).(*localLockTable)
			remote := proxy.remote
			proxy.remote = &unlockBeforeApplyErrorTable{
				lockTable: remote,
				err:       errors.New("unlock request lost"),
			}

			err = s2.unlockUnknownCommit(ctx, unknownTxn, timestamp.Timestamp{})
			require.EqualError(t, err, "unlock request lost")
			holder, ok, err := owner.getLockHolder(ctx, row)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, unknownTxn, holder.TxnID)

			proxy.remote = remote
			require.NoError(t, s2.Unlock(ctx, survivorTxn, timestamp.Timestamp{}))
			holder, ok, err = owner.getLockHolder(ctx, row)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, unknownTxn, holder.TxnID)

			require.NoError(t, s2.unlockUnknownCommit(ctx, unknownTxn, timestamp.Timestamp{}))
			_, ok, err = owner.getLockHolder(ctx, row)
			require.NoError(t, err)
			require.False(t, ok)
		},
	)
}

func TestProxyRetryPreservesAppliedHandoffRepresentative(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(_ *lockTableAllocator, services []*service) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			const tableID = uint64(10)
			row := []byte("row")
			seedTxn := []byte("seed")
			unknownTxn := []byte("unknown")
			firstReplacementTxn := []byte("first-replacement")
			lateSharerTxn := []byte("late-sharer")
			exclusiveTxn := []byte("exclusive")
			s1 := services[0]
			s2 := services[1]

			s1.cfg.EnableRemoteLocalProxy = true
			_, err := s1.Lock(ctx, tableID, [][]byte{row}, seedTxn, newTestRowSharedOptions())
			require.NoError(t, err)
			require.NoError(t, s1.Unlock(ctx, seedTxn, timestamp.Timestamp{}))

			s2.cfg.EnableRemoteLocalProxy = true
			_, err = s2.Lock(ctx, tableID, [][]byte{row}, unknownTxn, newTestRowSharedOptions())
			require.NoError(t, err)
			_, err = s2.Lock(ctx, tableID, [][]byte{row}, firstReplacementTxn, newTestRowSharedOptions())
			require.NoError(t, err)

			proxy := s2.tableGroups.get(0, tableID).(*localLockTableProxy)
			owner := s1.tableGroups.get(0, tableID).(*localLockTable)
			remote := proxy.remote
			proxy.remote = &unlockAfterApplyErrorTable{
				lockTable: remote,
				err:       errors.New("handoff response lost"),
			}

			// The owner has already moved unknown -> firstReplacement, but the
			// proxy still records unknown and must retry that same representative.
			err = s2.unlockUnknownCommit(ctx, unknownTxn, timestamp.Timestamp{})
			require.EqualError(t, err, "handoff response lost")

			// A later local sharer must not overwrite the representative selected
			// by the unacknowledged handoff.
			proxy.remote = remote
			_, err = s2.Lock(ctx, tableID, [][]byte{row}, lateSharerTxn, newTestRowSharedOptions())
			require.NoError(t, err)
			require.NoError(t, s2.unlockUnknownCommit(ctx, unknownTxn, timestamp.Timestamp{}))

			holder, ok, err := owner.getLockHolder(ctx, row)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, firstReplacementTxn, holder.TxnID)
			proxy.mu.RLock()
			require.Equal(t, firstReplacementTxn, proxy.mu.currentHolder[string(row)])
			require.Empty(t, proxy.mu.pendingRemoteHolders)
			proxy.mu.RUnlock()

			// Let the first replacement finish. The owner must now move to the
			// still-active late sharer, rather than retaining an orphan holder.
			require.NoError(t, s2.Unlock(ctx, firstReplacementTxn, timestamp.Timestamp{}))
			holder, ok, err = owner.getLockHolder(ctx, row)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, lateSharerTxn, holder.TxnID)

			// In the real service, an ordinary live txn is reported by the
			// frontend iterator. Keep the late sharer visible while exercising the
			// owner orphan check for the finished first replacement.
			s2.cfg.TxnIterFunc = func(fn func([]byte) bool) {
				fn(lateSharerTxn)
			}
			s1.activeTxnHolder.keepRemoteLockBindActive(s2.serviceID, owner.bind)
			s1.events.checkOrphan(checkOrphan{
				wait: waitTooLong,
				key:  row,
				lt:   owner,
				txn:  pb.WaitTxn{TxnID: []byte("owner-waiter"), CreatedOn: s1.serviceID},
			})

			type result struct {
				err error
			}
			exclusiveDone := make(chan result, 1)
			// Waiter notification is asynchronous. Give this phase its own budget
			// so setup time or temporary runner starvation cannot consume it.
			exclusiveCtx, exclusiveCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer exclusiveCancel()
			go func() {
				_, err := s1.Lock(exclusiveCtx, tableID, [][]byte{row}, exclusiveTxn, newTestRowExclusiveOptions())
				exclusiveDone <- result{err: err}
			}()
			waitWaiters(t, s1, tableID, row, 1)
			require.Never(t, func() bool {
				select {
				case r := <-exclusiveDone:
					require.NoError(t, r.err)
					return true
				default:
					return false
				}
			}, 100*time.Millisecond, time.Millisecond)

			require.NoError(t, s2.Unlock(ctx, lateSharerTxn, timestamp.Timestamp{}))
			select {
			case r := <-exclusiveDone:
				require.NoError(t, r.err)
			case <-exclusiveCtx.Done():
				require.NoError(t, exclusiveCtx.Err())
			}
			require.NoError(t, s1.Unlock(ctx, exclusiveTxn, timestamp.Timestamp{}))
		},
	)
}

func TestSameOwnerProxyHandoffSurvivesOrdinaryTableRelease(t *testing.T) {
	runLockServiceTests(t, []string{"owner", "source"}, func(_ *lockTableAllocator, services []*service) {
		owner, source := services[0], services[1]
		source.cfg.EnableRemoteLocalProxy = true
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		const ordinary, shared = 990, 991
		row := []byte("same-row")
		rows := [][]byte{row}
		exclusive := newTestRowExclusiveOptions()
		exclusive.Policy = pb.WaitPolicy_FastFail
		for _, table := range []uint64{ordinary, shared} {
			_, err := owner.Lock(ctx, table, rows, []byte("seed"), exclusive)
			require.NoError(t, err)
			require.NoError(t, owner.Unlock(ctx, []byte("seed"), timestamp.Timestamp{}))
		}

		representative, surviving := []byte("representative"), []byte("surviving")
		_, err := source.Lock(ctx, ordinary, rows, representative, exclusive)
		require.NoError(t, err)
		_, err = source.Lock(ctx, shared, rows, representative, newTestRowSharedOptions())
		require.NoError(t, err)
		_, err = source.Lock(ctx, shared, rows, surviving, newTestRowSharedOptions())
		require.NoError(t, err)
		require.NoError(t, source.Unlock(ctx, representative, timestamp.Timestamp{}))

		holder, held, err := owner.GetLockHolder(ctx, shared, row, exclusive)
		require.NoError(t, err)
		require.True(t, held)
		require.Equal(t, surviving, holder.TxnID)
		owner.mu.RLock()
		refs := owner.mu.lockTableRef[0][shared]
		owner.mu.RUnlock()
		require.EqualValues(t, 1, refs)
		_, err = owner.Lock(ctx, shared, rows, []byte("challenger"), exclusive)
		require.Error(t, err)
		require.NoError(t, owner.Unlock(ctx, []byte("challenger"), timestamp.Timestamp{}))

		require.NoError(t, source.Unlock(ctx, surviving, timestamp.Timestamp{}))
		_, err = owner.Lock(ctx, shared, rows, []byte("challenger"), exclusive)
		require.NoError(t, err)
		require.NoError(t, owner.Unlock(ctx, []byte("challenger"), timestamp.Timestamp{}))
	})
}

func TestHandoffStagesAllReplacementLedgersBeforeChangingOwner(t *testing.T) {
	runLockServiceTests(t, []string{"owner"}, func(_ *lockTableAllocator, nodes []*service) {
		s := nodes[0]
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		const table = 1032
		a, b, c := []byte("old"), []byte("replacement-1"), []byte("replacement-2")
		row1, row2, row3 := []byte{1}, []byte{2}, []byte{3}
		opts := newTestRowSharedOptions()
		_, err := s.Lock(ctx, table, [][]byte{row1, row2}, a, opts)
		require.NoError(t, err)
		_, err = s.Lock(ctx, table, [][]byte{row3}, c, opts)
		require.NoError(t, err)
		bind := s.tableGroups.get(0, table).getBind()
		cTxn := s.activeTxnHolder.getActiveTxn(c, false, "")
		request := pb.UnlockRequest{TxnID: a, Mutations: []pb.ExtraMutation{
			{Key: row1, ReplaceTo: b},
			{Key: row2, ReplaceTo: c},
		}}
		check := func(row, expected []byte) {
			holder, ok, err := s.GetLockHolder(ctx, table, row, opts)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, expected, holder.TxnID)
		}

		cTxn.Lock()
		err = s.unlockRemoteTable(ctx, bind, request)
		cTxn.Unlock()
		require.ErrorIs(t, err, ErrLockTimeout)
		require.False(t, s.activeTxnHolder.hasActiveTxn(b), "failed preflight registered an empty holder")
		check(row1, a)
		check(row2, a)

		cTxn.Lock()
		originalPool := cTxn.fsp
		cTxn.fsp = newFixedSlicePool(1)
		cTxn.Unlock()
		err = s.unlockRemoteTable(ctx, bind, request)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockNeedUpgrade), "%v", err)
		require.False(t, s.activeTxnHolder.hasActiveTxn(b), "failed reservation registered an empty holder")
		check(row1, a)
		check(row2, a)

		cTxn.Lock()
		cTxn.fsp = originalPool
		cTxn.Unlock()
		require.NoError(t, s.unlockRemoteTable(ctx, bind, request))
		check(row1, b)
		check(row2, c)
		require.NoError(t, s.unlockRemoteTable(ctx, bind, pb.UnlockRequest{TxnID: b}))
		require.NoError(t, s.unlockRemoteTable(ctx, bind, pb.UnlockRequest{TxnID: c}))
		for _, row := range [][]byte{row1, row2, row3} {
			_, held, err := s.GetLockHolder(ctx, table, row, opts)
			require.NoError(t, err)
			require.False(t, held)
		}
	})
}

type handoffReservationProbe struct {
	Client
	rejected chan struct{}
}

func (c *handoffReservationProbe) Send(ctx context.Context, req *pb.Request) (*pb.Response, error) {
	resp, err := c.Client.Send(ctx, req)
	if req.Method == pb.Method_Unlock && moerr.IsMoErrCode(err, moerr.ErrLockNeedUpgrade) {
		select {
		case c.rejected <- struct{}{}:
		default:
		}
	}
	return resp, err
}

func TestProxyHandoffCapacityAllowsReplacementCleanup(t *testing.T) {
	runLockServiceTests(t, []string{"owner", "source"}, func(_ *lockTableAllocator, nodes []*service) {
		owner, source := nodes[0], nodes[1]
		source.cfg.EnableRemoteLocalProxy = true
		probe := &handoffReservationProbe{Client: source.remote.client, rejected: make(chan struct{}, 1)}
		source.remote.client = probe
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		const table = 1033
		a, b := []byte("old-holder"), []byte("replacement")
		row1, row2 := []byte{1}, []byte{2}
		opts := newTestRowSharedOptions()
		_, err := owner.Lock(ctx, table, [][]byte{row1}, []byte("seed"), opts)
		require.NoError(t, err)
		require.NoError(t, owner.Unlock(ctx, []byte("seed"), timestamp.Timestamp{}))

		_, err = source.Lock(ctx, table, [][]byte{row2}, b, opts)
		require.NoError(t, err)
		_, err = source.Lock(ctx, table, [][]byte{row1}, a, opts)
		require.NoError(t, err)
		_, err = source.Lock(ctx, table, [][]byte{row1}, b, opts)
		require.NoError(t, err)

		bOwner := owner.activeTxnHolder.getActiveTxn(b, false, "")
		bOwner.Lock()
		bOwner.fsp = newFixedSlicePool(1)
		bOwner.Unlock()

		aDone := make(chan error, 1)
		go func() { aDone <- source.Unlock(ctx, a, timestamp.Timestamp{}) }()
		select {
		case <-probe.rejected:
		case <-ctx.Done():
			t.Fatal("handoff did not reach the owner capacity limit")
		}
		bDone := make(chan error, 1)
		go func() { bDone <- source.Unlock(ctx, b, timestamp.Timestamp{}) }()
		for _, done := range []chan error{bDone, aDone} {
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal("proxy blocked cleanup of a replacement after definite rejection")
			}
		}
		for _, row := range [][]byte{row1, row2} {
			_, held, err := owner.GetLockHolder(ctx, table, row, opts)
			require.NoError(t, err)
			require.False(t, held)
		}
	})
}

func TestHandoffReplacementKeepsOriginalTableInstance(t *testing.T) {
	runLockServiceTests(t, []string{"owner"}, func(_ *lockTableAllocator, nodes []*service) {
		owner := nodes[0]
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		const table = 1790
		oldID, replacementID := []byte("old"), []byte("replacement")
		opts := newTestRowSharedOptions()
		rows := newTestRows(1)
		_, err := owner.Lock(ctx, table, rows, oldID, opts)
		require.NoError(t, err)
		old := owner.tableGroups.get(0, table).(*localLockTable)
		oldTxn := owner.activeTxnHolder.getActiveTxn(oldID, false, "")
		adopting, resume := make(chan struct{}), make(chan struct{})
		continueAdoption := sync.OnceFunc(func() { close(resume) })
		defer continueAdoption()
		handedOff := make(chan error, 1)
		go func() {
			oldTxn.Lock()
			defer oldTxn.Unlock()
			keys := oldTxn.lockHolders[0].tableKeys[table]
			handedOff <- old.releaseLocks(oldTxn, keys, timestamp.Timestamp{}, handoffLocalUnlock,
				func(group uint32, table uint64) {
					close(adopting)
					<-resume
					owner.incRef(group, table)
				}, pb.ExtraMutation{Key: rows[0], ReplaceTo: replacementID})
		}()
		select {
		case <-adopting:
		case <-ctx.Done():
			t.Fatal("handoff did not reach reference adoption")
		}
		// Retirement can proceed once the physical handoff releases the table
		// mutex. The new instance must see the replacement's old intent.
		removed, err := owner.CloseRemoteLockTable(0, table, old.bind.Version)
		require.NoError(t, err)
		require.True(t, removed)
		nextRows := newTestRows(2)
		recreated, err := owner.getLockTableWithCreate(ctx, 0, table, nextRows, opts.Sharding)
		require.NoError(t, err)
		require.NotSame(t, old, recreated)
		require.True(t, old.bind.Equal(recreated.getBind()))
		locked := make(chan error, 1)
		go func() {
			_, err := owner.Lock(ctx, table, nextRows, replacementID, opts)
			locked <- err
		}()
		continueAdoption()
		require.NoError(t, <-handedOff)
		require.ErrorIs(t, <-locked, ErrLockTableBindChanged)
		_, held, err := owner.GetLockHolder(ctx, table, nextRows[0], opts)
		require.NoError(t, err)
		require.False(t, held)
		require.NoError(t, owner.Unlock(ctx, oldID, timestamp.Timestamp{}))
		require.NoError(t, owner.Unlock(ctx, replacementID, timestamp.Timestamp{}))
		owner.mu.RLock()
		refs := owner.mu.lockTableRef[0][table]
		owner.mu.RUnlock()
		require.Zero(t, refs)
	})
}

func TestProxyExactRowsUseOwnerThroughUnlock(t *testing.T) {
	const table = uint64(17)
	runLockServiceTests(t, []string{"owner", "client"}, func(_ *lockTableAllocator, ss []*service) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		owner, client := ss[0], ss[1]
		client.cfg.EnableRemoteLocalProxy = true
		shared := newTestRowSharedOptions()
		rows := [][]byte{{1}, {3}}
		proxyTxn, exactTxn := newTestTxnID(1), newTestTxnID(2)
		_, err := owner.Lock(ctx, table, rows[:1], proxyTxn, shared)
		require.NoError(t, err)
		require.NoError(t, owner.Unlock(ctx, proxyTxn, timestamp.Timestamp{}))
		_, err = client.Lock(ctx, table, rows[:1], proxyTxn, shared)
		require.NoError(t, err)
		_, err = client.Lock(ctx, table, rows, exactTxn, shared.WithExactRows())
		require.NoError(t, err)
		// Revisiting a point as an ordinary reader must keep its direct ownership.
		_, err = client.Lock(ctx, table, rows[:1], exactTxn, shared)
		require.NoError(t, err)
		_, err = client.Lock(ctx, table, rows[:1], proxyTxn, shared.WithExactRows())
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry))
		// Restore replays both grants in one transaction. Pin the route on
		// the ordinary request while a different transaction uses the proxy.
		restoreTxn := newTestTxnID(4)
		restoreShared := shared
		restoreShared.BypassLocalProxy = true
		_, err = client.Lock(ctx, table, rows[:1], restoreTxn, restoreShared)
		require.NoError(t, err)
		_, err = client.Lock(ctx, table, rows, restoreTxn, restoreShared.WithExactRows())
		require.NoError(t, err)
		require.NoError(t, client.Unlock(ctx, restoreTxn, timestamp.Timestamp{}))
		require.NoError(t, client.Unlock(ctx, proxyTxn, timestamp.Timestamp{}))
		exclusive := newTestRowExclusiveOptions()
		exclusive.Policy = pb.WaitPolicy_FastFail
		writer := newTestTxnID(3)
		_, err = owner.Lock(ctx, table, rows[:1], writer, exclusive)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockConflict))
		require.NoError(t, owner.Unlock(ctx, writer, timestamp.Timestamp{}))
		require.NoError(t, client.Unlock(ctx, exactTxn, timestamp.Timestamp{}))
		_, err = owner.Lock(ctx, table, rows, writer, exclusive)
		require.NoError(t, err)
		require.NoError(t, owner.Unlock(ctx, writer, timestamp.Timestamp{}))
	})
}
