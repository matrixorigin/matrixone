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
	"sync/atomic"
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
}

func (t *blockingProxyLockTable) lock(
	_ context.Context,
	_ *activeTxn,
	_ [][]byte,
	_ LockOptions,
	cb func(pb.Result, error),
) {
	t.started <- struct{}{}
	<-t.release
	cb(pb.Result{LockedOn: t.bind}, nil)
}

func (t *blockingProxyLockTable) getBind() pb.LockTable { return t.bind }

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

func TestProxyCoalescedTimeoutIsRemovedAndCallbackRunsOnce(t *testing.T) {
	testCases := []struct {
		name       string
		lockBudget bool
	}{
		{name: "caller-deadline"},
		{name: "lock-budget", lockBudget: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			bind := pb.LockTable{Group: 0, Table: 1, OriginTable: 1, ServiceID: "s2", Valid: true}
			remote := &blockingProxyLockTable{
				bind:    bind,
				started: make(chan struct{}, 1),
				release: make(chan struct{}),
			}
			proxy := newLockTableProxy("s1", remote, getLogger("")).(*localLockTableProxy)
			row := []byte("row")
			firstTxnID := []byte("proxy-first")
			waiterTxnID := []byte("proxy-waiter")
			firstTxn := newUnpooledActiveTxnForTest(firstTxnID)
			waiterTxn := newUnpooledActiveTxnForTest(waiterTxnID)
			defer firstTxn.reset()
			defer waiterTxn.reset()

			firstDone := make(chan struct{})
			go func() {
				firstTxn.Lock()
				proxy.lock(
					context.Background(),
					firstTxn,
					[][]byte{row},
					LockOptions{LockOptions: newTestRowSharedOptions()},
					func(pb.Result, error) {})
				firstTxn.Unlock()
				close(firstDone)
			}()
			select {
			case <-remote.started:
			case <-time.After(time.Second):
				require.FailNow(t, "first proxy owner RPC did not start")
			}

			waitCtx := context.Background()
			cancel := func() {}
			options := newTestRowSharedOptions()
			if testCase.lockBudget {
				options.LockWaitTimeout = 60
				options.LockWaitDeadline = time.Now().Add(100 * time.Millisecond).UnixNano()
			} else {
				waitCtx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
			}
			defer cancel()

			var callbackCount atomic.Int32
			var callbackErr error
			waiterTxn.Lock()
			proxy.lock(
				waitCtx,
				waiterTxn,
				[][]byte{row},
				LockOptions{LockOptions: options},
				func(_ pb.Result, err error) {
					callbackCount.Add(1)
					callbackErr = err
				})
			waiterTxn.Unlock()
			if testCase.lockBudget {
				require.ErrorIs(t, callbackErr, ErrLockTimeout)
			} else {
				require.ErrorIs(t, callbackErr, context.DeadlineExceeded)
			}
			require.Equal(t, int32(1), callbackCount.Load())

			close(remote.release)
			select {
			case <-firstDone:
			case <-time.After(time.Second):
				require.FailNow(t, "first proxy owner RPC did not finish")
			}
			require.Equal(t, int32(1), callbackCount.Load(),
				"owner completion must not invoke the timed-out callback again")

			proxy.mu.Lock()
			shared := proxy.mu.holders[string(row)]
			require.NotNil(t, shared)
			require.Len(t, shared.txns, 1)
			require.Same(t, firstTxn, shared.txns[0])
			require.Len(t, shared.waiters, 1)
			require.Nil(t, shared.waiters[0])
			shared.remove(firstTxn)
			delete(proxy.mu.holders, string(row))
			delete(proxy.mu.currentHolder, string(row))
			proxy.mu.Unlock()
		})
	}
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
			_, pending := proxy.mu.pendingLastHolderUnlocks[string(row)]
			proxy.mu.RUnlock()
			require.True(t, pending)

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
			_, pending = proxy.mu.pendingLastHolderUnlocks[string(row)]
			proxy.mu.RUnlock()
			require.False(t, pending)

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
			go func() {
				_, err := s1.Lock(ctx, tableID, [][]byte{row}, exclusiveTxn, newTestRowExclusiveOptions())
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
			case <-ctx.Done():
				require.NoError(t, ctx.Err())
			}
			require.NoError(t, s1.Unlock(ctx, exclusiveTxn, timestamp.Timestamp{}))
		},
	)
}
