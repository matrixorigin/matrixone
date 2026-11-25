// Copyright 2026 Matrix Origin
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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

func TestResolveCommitUnknownWaitsForAllocatorFence(t *testing.T) {
	runLockServiceTests(t, []string{"s1"}, func(allocator *lockTableAllocator, services []*service) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		service := services[0]
		holderTxn := []byte("holder")
		waiterTxn := []byte("waiter")
		key := []byte("key")
		options := newTestRowExclusiveOptions()
		var resolved atomic.Int32

		_, err := service.Lock(ctx, 1, [][]byte{key}, holderTxn, options)
		require.NoError(t, err)
		_, err = allocator.Valid(service.serviceID, holderTxn, nil)
		require.NoError(t, err)
		require.NoError(t, service.ResolveCommitUnknown(
			holderTxn,
			time.Now().Add(time.Hour),
			service.NextCommitSequence(),
			func() { resolved.Add(1) },
		))

		type lockResult struct {
			result pb.Result
			err    error
		}
		resultC := make(chan lockResult, 1)
		go func() {
			result, err := service.Lock(ctx, 1, [][]byte{key}, waiterTxn, options)
			resultC <- lockResult{result: result, err: err}
		}()
		waitWaiters(t, service, 1, key, 1)

		require.Never(t, func() bool {
			select {
			case <-resultC:
				return true
			default:
				return false
			}
		}, 200*time.Millisecond, 10*time.Millisecond)

		allocator.FinishCommit(service.serviceID, holderTxn)

		var result lockResult
		require.Eventually(t, func() bool {
			select {
			case result = <-resultC:
				return true
			default:
				return false
			}
		}, 2*time.Second, 10*time.Millisecond)
		require.NoError(t, result.err)
		require.True(t, result.result.HasConflict)
		require.True(t, result.result.HasPrevCommit)
		require.False(t, result.result.Timestamp.IsEmpty())
		require.Eventually(t, func() bool {
			return resolved.Load() == 1
		}, time.Second, 10*time.Millisecond)
		require.Never(t, func() bool {
			return resolved.Load() > 1
		}, 100*time.Millisecond, 10*time.Millisecond)

		require.NoError(t, service.Unlock(ctx, waiterTxn, timestamp.Timestamp{}))
	})
}

func TestResolveCommitUnknownCompletesWhenTxnAlreadyUnlocked(t *testing.T) {
	runLockServiceTests(t, []string{"s1"}, func(_ *lockTableAllocator, services []*service) {
		service := services[0]
		var resolved atomic.Int32
		require.NoError(t, service.ResolveCommitUnknown(
			[]byte("already-unlocked"),
			time.Now().Add(time.Hour),
			service.NextCommitSequence(),
			func() { resolved.Add(1) },
		))
		require.Eventually(t, func() bool {
			return resolved.Load() == 1
		}, time.Second, 10*time.Millisecond)
		require.False(t, service.unknownCommitResolver.isPending([]byte("already-unlocked")))
	})
}

func TestUnknownCommitPendingActiveCallbackCanReenterClose(t *testing.T) {
	runLockServiceTests(t, []string{"s1"}, func(_ *lockTableAllocator, services []*service) {
		service := services[0]
		var resolved atomic.Int32
		closeResult := make(chan error, 1)

		require.NoError(t, service.ResolveCommitUnknown(
			[]byte("already-unlocked-reentrant-close"),
			time.Now().Add(time.Hour),
			service.NextCommitSequence(),
			func() {
				resolved.Add(1)
				closeResult <- service.Close()
			},
		))

		select {
		case err := <-closeResult:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("pendingActiveTxns callback deadlocked re-entering service Close")
		}
		require.Equal(t, int32(1), resolved.Load())
		require.Never(t, func() bool {
			return resolved.Load() != 1
		}, 100*time.Millisecond, 10*time.Millisecond)
	})
}

func TestUnknownCommitRemoveCallbackCanReenterClose(t *testing.T) {
	runLockServiceTests(t, []string{"s1"}, func(allocator *lockTableAllocator, services []*service) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		service := services[0]
		txnID := []byte("resolved-reentrant-close")
		_, err := service.Lock(
			ctx,
			1,
			[][]byte{[]byte("resolved-reentrant-close-key")},
			txnID,
			newTestRowExclusiveOptions(),
		)
		require.NoError(t, err)
		_, err = allocator.Valid(service.serviceID, txnID, nil)
		require.NoError(t, err)

		var resolved atomic.Int32
		closeResult := make(chan error, 1)
		require.NoError(t, service.ResolveCommitUnknown(
			txnID,
			time.Now().Add(time.Hour),
			service.NextCommitSequence(),
			func() {
				resolved.Add(1)
				closeResult <- service.Close()
			},
		))
		allocator.FinishCommit(service.serviceID, txnID)

		select {
		case err := <-closeResult:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("remove callback deadlocked re-entering service Close")
		}
		require.Equal(t, int32(1), resolved.Load())
		require.Never(t, func() bool {
			return resolved.Load() != 1
		}, 100*time.Millisecond, 10*time.Millisecond)
	})
}

func TestUnknownCommitCleanupDoesNotDeadlockBindFence(t *testing.T) {
	runLockServiceTests(t, []string{"s1"}, func(_ *lockTableAllocator, services []*service) {
		service := services[0]
		tableID := uint64(24766)
		bind := pb.LockTable{
			Group:     0,
			Table:     tableID,
			ServiceID: service.serviceID,
			Version:   1,
			Valid:     true,
		}
		lockTable := &blockingUnlockTestTable{
			retryableUnlockTestTable: retryableUnlockTestTable{bind: bind},
			started:                  make(chan struct{}),
			release:                  make(chan struct{}),
		}

		txnID := []byte("unknown-commit-bind-fence")
		txn := service.activeTxnHolder.getActiveTxn(txnID, true, "")
		txn.Lock()
		require.NoError(t, txn.lockAdded(bind.Group, bind, [][]byte{{1}}, service.logger))
		txn.Unlock()
		service.incRef(bind.Group, bind.Table)

		holder := service.activeTxnHolder.(*mapBasedTxnHolder)
		service.tableGroups.set(bind.Group, bind.Table, lockTable)

		unlockDone := make(chan error, 1)
		go func() {
			unlockDone <- service.unlockUnknownCommit(
				context.Background(),
				txnID,
				timestamp.Timestamp{},
			)
		}()
		<-lockTable.started

		fenceDone := make(chan int, 1)
		changedBind := bind
		changedBind.Version++
		go func() {
			fenceDone <- holder.fenceByBindChanged(changedBind)
		}()
		select {
		case count := <-fenceDone:
			require.Zero(t, count)
		case <-time.After(5 * time.Second):
			close(lockTable.release)
			t.Fatal("bind fencing waited for a closing transaction")
		}
		close(lockTable.release)

		select {
		case err := <-unlockDone:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("unknown-commit cleanup deadlocked with bind fencing")
		}
		require.False(t, holder.hasActiveTxn(txnID))
	})
}

func TestUnknownCommitBatchFencesOnlyNonCommittingTxns(t *testing.T) {
	runLockServiceTests(t, []string{"s1"}, func(allocator *lockTableAllocator, services []*service) {
		service := services[0]
		committingTxn := []byte("committing")
		unstartedTxn := []byte("unstarted")

		_, err := allocator.Valid(service.serviceID, committingTxn, nil)
		require.NoError(t, err)

		committing, fenceTS, ok := service.canUnlockUnknownCommits(
			context.Background(),
			[][]byte{committingTxn, unstartedTxn},
			time.Now().Add(time.Hour),
			service.NextCommitSequence(),
		)
		require.True(t, ok)
		_, ok = committing[string(committingTxn)]
		require.True(t, ok)
		_, ok = committing[string(unstartedTxn)]
		require.False(t, ok)
		require.False(t, fenceTS.IsEmpty())

		_, err = allocator.Valid(service.serviceID, unstartedTxn, nil)
		require.Error(t, err, "the batch fence must reject a late Commit")
		allocator.FinishCommit(service.serviceID, committingTxn)
	})
}

func TestUnknownCommitFenceSurvivesLiveServiceCleanup(t *testing.T) {
	runLockServiceTests(t, []string{"s1"}, func(allocator *lockTableAllocator, services []*service) {
		service := services[0]
		txnID := []byte("late-commit")

		sequence := service.NextCommitSequence()
		_, _, ok := service.canUnlockUnknownCommits(
			context.Background(),
			[][]byte{txnID},
			time.Now().Add(time.Hour),
			sequence,
		)
		require.True(t, ok)

		// The source CN is live but its TxnIterFunc no longer contains the
		// closed operator. That is not enough to prove an already buffered
		// Commit cannot still reach TN.
		allocator.cleanCommitStateOnce(
			context.Background(),
			func(context.Context, string) (bool, [][]byte, error) {
				return true, nil, nil
			},
			time.Hour,
		)

		ctl := allocator.getCtl(service.serviceID)
		require.NotZero(t, ctl.persistentFenceExpiry())
		require.Zero(t, ctl.size(), "persistent fences are compacted per source CN")

		_, err := allocator.Valid(
			service.serviceID,
			txnID,
			nil,
			CommitRequestMeta{Sequence: sequence},
		)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrCannotCommitOrphan))
	})
}

func TestUnknownCommitFencesSurviveSourceIncarnationInvalid(t *testing.T) {
	runLockServiceTests(t, []string{"s1"}, func(allocator *lockTableAllocator, services []*service) {
		service := services[0]
		const fenceCount = 128
		txnIDs := make([][]byte, 0, fenceCount)
		for i := 0; i < fenceCount; i++ {
			txnIDs = append(txnIDs, []byte(fmt.Sprintf("late-commit-%d", i)))
		}

		sequence := service.NextCommitSequence()
		_, _, ok := service.canUnlockUnknownCommits(
			context.Background(),
			txnIDs,
			time.Now().Add(time.Hour),
			sequence,
		)
		require.True(t, ok)

		// A lockservice endpoint mismatch does not drain the TN RPC queue. A
		// Commit decoded before the mismatch can still reach allocator.Valid.
		client := &resetTrackingClient{}
		require.NoError(t, allocator.client.Close())
		allocator.client = client
		allocator.cleanCommitStateOnce(
			context.Background(),
			func(context.Context, string) (bool, [][]byte, error) {
				return false, nil, nil
			},
			time.Hour,
		)

		require.Equal(t, int32(1), client.resets.Load())
		ctl := allocator.getCtl(service.serviceID)
		require.NotZero(t, ctl.persistentFenceExpiry())
		require.Zero(t, ctl.size(), "persistent fences are compacted per source CN")
		_, err := allocator.Valid(
			service.serviceID,
			txnIDs[0],
			nil,
			CommitRequestMeta{Sequence: sequence},
		)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrCannotCommitOrphan))
	})
}

func TestUnknownCommitFenceRejectsOnlyOlderCommitSequences(t *testing.T) {
	runLockServiceTests(t, []string{"s1"}, func(allocator *lockTableAllocator, services []*service) {
		service := services[0]
		unknownDeadline := time.Now().Add(time.Hour)
		unknownSequence := service.NextCommitSequence()
		_, _, ok := service.canUnlockUnknownCommits(
			context.Background(),
			[][]byte{[]byte("unknown")},
			unknownDeadline,
			unknownSequence,
		)
		require.True(t, ok)

		// The unknown Commit's sequence is rejected even if it remains queued.
		_, err := allocator.Valid(
			service.serviceID,
			[]byte("old-commit"),
			nil,
			CommitRequestMeta{
				DeadlineUnixNano: unknownDeadline.UnixNano(),
				Sequence:         unknownSequence,
			},
		)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrCannotCommitOrphan))

		// A newer Commit remains admissible even with a shorter deadline. This
		// avoids converting an unknown outcome into normal-traffic failures for
		// callers that use a tighter context timeout.
		newTxnID := []byte("new-commit")
		_, err = allocator.Valid(
			service.serviceID,
			newTxnID,
			nil,
			CommitRequestMeta{
				DeadlineUnixNano: time.Now().Add(time.Millisecond).UnixNano(),
				Sequence:         service.NextCommitSequence(),
			},
		)
		require.NoError(t, err)
		allocator.FinishCommit(service.serviceID, newTxnID)
	})
}

func TestUnknownCommitFenceKeepsSequenceExpiryPairs(t *testing.T) {
	ctl := &commitCtl{}
	now := time.Now().UnixNano()
	longExpiry := now + int64(time.Hour)
	shortExpiry := now + int64(time.Second)

	ctl.mu.Lock()
	ctl.addPersistentFenceLocked(now, commitFence{
		persist:        true,
		expiresAt:      longExpiry,
		commitSequence: 1,
	})
	ctl.addPersistentFenceLocked(now, commitFence{
		persist:        true,
		expiresAt:      shortExpiry,
		commitSequence: 100,
	})
	require.True(t, ctl.hasPersistentFenceLocked(now, 2))
	// Once the short fence expires, the long-running sequence-1 fence must
	// remain, but it must not continue rejecting still-valid sequences 2..99.
	require.False(t, ctl.hasPersistentFenceLocked(shortExpiry+1, 2))
	require.True(t, ctl.hasPersistentFenceLocked(shortExpiry+1, 1))
	ctl.mu.Unlock()
}

func TestUnknownCommitFenceFrontierCollapsesAtBound(t *testing.T) {
	ctl := &commitCtl{}
	now := time.Now()
	for i := 0; i < maxPersistentFenceFrontierEntries; i++ {
		state := ctl.tryCannotCommit(
			fmt.Sprintf("unknown-%d", i),
			commitFence{
				persist:        true,
				expiresAt:      now.Add(time.Duration(maxPersistentFenceFrontierEntries-i+2) * time.Second).UnixNano(),
				commitSequence: uint64(i + 1),
			},
		)
		require.Equal(t, cannotCommitState, state)
	}
	require.Equal(t, maxPersistentFenceFrontierEntries, ctl.persistentFenceCount())

	state := ctl.tryCannotCommit(
		"overflow",
		commitFence{
			persist:        true,
			expiresAt:      now.Add(time.Second).UnixNano(),
			commitSequence: maxPersistentFenceFrontierEntries + 1,
		},
	)
	require.Equal(t, cannotCommitState, state)
	require.Equal(t, 1, ctl.persistentFenceCount())
	ctl.mu.Lock()
	require.True(t, ctl.hasPersistentFenceLocked(now.UnixNano(), maxPersistentFenceFrontierEntries+1))
	require.False(t, ctl.hasPersistentFenceLocked(now.UnixNano(), maxPersistentFenceFrontierEntries+2))
	ctl.mu.Unlock()
}

func TestUnknownCommitFenceOverflowReleasesSourceTxn(t *testing.T) {
	runLockServiceTests(t, []string{"s1"}, func(allocator *lockTableAllocator, services []*service) {
		service := services[0]
		ctl := allocator.getCtl(service.serviceID)
		// Build the exact frontier directly so the test timeout covers only the
		// resolver RPC and unlock path under test, rather than 1024 setup RPCs.
		seedDeadline := time.Now().Add(time.Hour)
		for i := 0; i < maxPersistentFenceFrontierEntries; i++ {
			// Increasing sequence and decreasing expiry intentionally build the
			// largest exact non-dominated frontier.
			state := ctl.tryCannotCommit(
				fmt.Sprintf("seed-%d", i),
				commitFence{
					persist: true,
					expiresAt: service.unknownCommitFenceExpiry(
						seedDeadline.Add(time.Duration(maxPersistentFenceFrontierEntries-i+3) * time.Second),
					).UnixNano(),
					commitSequence: uint64(i + 1),
				},
			)
			require.Equal(t, cannotCommitState, state)
		}
		require.Equal(t, maxPersistentFenceFrontierEntries, ctl.persistentFenceCount())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		overflowTxn := []byte("overflow")
		_, err := service.Lock(
			ctx,
			1,
			[][]byte{[]byte("overflow-key")},
			overflowTxn,
			newTestRowExclusiveOptions(),
		)
		require.NoError(t, err)
		// Derive this after the RPC-heavy setup so its fence is still live when
		// the resolver submits it.
		overflowDeadline := time.Now().Add(time.Minute)
		require.NoError(t, service.ResolveCommitUnknown(
			overflowTxn,
			overflowDeadline,
			maxPersistentFenceFrontierEntries+1,
			nil,
		))

		require.Eventually(t, func() bool {
			return !service.activeTxnHolder.hasActiveTxn(overflowTxn) &&
				!service.unknownCommitResolver.isPending(overflowTxn)
		}, 2*time.Second, 10*time.Millisecond)

		require.Equal(t, 1, ctl.persistentFenceCount())
	})
}

func TestUnknownCommitFencesStayBoundedForSequentialTxns(t *testing.T) {
	runLockServiceTests(t, []string{"s1"}, func(allocator *lockTableAllocator, services []*service) {
		service := services[0]
		const fenceCount = 128
		deadline := time.Now().Add(time.Hour)

		for i := 0; i < fenceCount; i++ {
			txnID := []byte(fmt.Sprintf("sequential-unknown-%d", i))
			_, _, ok := service.canUnlockUnknownCommits(
				context.Background(),
				[][]byte{txnID},
				deadline,
				service.NextCommitSequence(),
			)
			require.True(t, ok)
		}

		ctl := allocator.getCtl(service.serviceID)
		require.NotZero(t, ctl.persistentFenceExpiry())
		require.Equal(t, 1, ctl.persistentFenceCount())
		require.Zero(t, ctl.size(), "unknown Commit fences must not retain each txn")

		// A normal live-service cleanup must preserve the compact source fence.
		allocator.cleanCommitStateOnce(
			context.Background(),
			func(context.Context, string) (bool, [][]byte, error) {
				return true, nil, nil
			},
			time.Hour,
		)
		require.NotZero(t, ctl.persistentFenceExpiry())
		require.Zero(t, ctl.size())
	})
}

func TestUnknownCommitFencesExpireAfterCommitDeadline(t *testing.T) {
	runLockServiceTests(t, []string{"s1"}, func(allocator *lockTableAllocator, services []*service) {
		service := services[0]
		const fenceCount = 128
		txnIDs := make([][]byte, 0, fenceCount)
		for i := 0; i < fenceCount; i++ {
			txnIDs = append(txnIDs, []byte(fmt.Sprintf("expired-unknown-%d", i)))
		}

		// The expiry is deliberately far in the past so it dominates the
		// allocator's bounded clock-skew grace on every test clock.
		_, _, ok := service.canUnlockUnknownCommits(
			context.Background(),
			txnIDs,
			time.Now().Add(-time.Hour),
			service.NextCommitSequence(),
		)
		require.True(t, ok)

		allocator.cleanCommitStateOnce(
			context.Background(),
			func(context.Context, string) (bool, [][]byte, error) {
				return true, nil, nil
			},
			time.Hour,
		)

		_, exists := allocator.ctl.Load(service.serviceID)
		require.False(t, exists, "expired persistent fences must not grow forever")
	})
}

func TestUnknownCommitResolverCloseCancelsRemoteUnlock(t *testing.T) {
	runLockServiceTests(t, []string{"s1"}, func(_ *lockTableAllocator, services []*service) {
		service := services[0]
		client := &blockingUnlockClient{unlockStarted: make(chan struct{}, 1)}
		bind := pb.LockTable{
			Group:     0,
			Table:     100,
			ServiceID: "unreachable",
			Version:   1,
			Valid:     true,
		}
		service.tableGroups.set(
			bind.Group,
			bind.Table,
			newRemoteLockTable(
				service.serviceID,
				time.Second,
				bind,
				client,
				service.handleBindChanged,
				service.logger,
			),
		)

		txnID := []byte("unknown-commit-remote-unlock")
		txn := service.activeTxnHolder.getActiveTxn(txnID, true, "")
		txn.Lock()
		require.NoError(t, txn.lockAdded(bind.Group, bind, [][]byte{[]byte("key")}, service.logger))
		txn.Unlock()
		resolved := make(chan struct{}, 1)
		callbackCloseErr := make(chan error, 1)
		require.NoError(t, service.ResolveCommitUnknown(
			txnID,
			time.Now().Add(time.Hour),
			service.NextCommitSequence(),
			func() {
				callbackCloseErr <- service.Close()
				resolved <- struct{}{}
			},
		))

		select {
		case <-client.unlockStarted:
		case <-time.After(time.Second):
			require.FailNow(t, "unknown commit resolver did not start remote unlock")
		}

		closed := make(chan error, 1)
		go func() {
			closed <- service.Close()
		}()
		select {
		case err := <-closed:
			require.NoError(t, err)
		case <-time.After(time.Second):
			require.FailNow(t, "service close blocked on remote unknown-commit unlock")
		}
		select {
		case <-resolved:
		case <-time.After(time.Second):
			require.FailNow(t, "service close did not release unknown-commit admission")
		}
		require.NoError(t, <-callbackCloseErr)
	})
}

func TestServiceCloseCancelsOrdinaryRemoteUnlock(t *testing.T) {
	runLockServiceTests(t, []string{"s1"}, func(_ *lockTableAllocator, services []*service) {
		service := services[0]
		client := &blockingUnlockClient{unlockStarted: make(chan struct{}, 1)}
		bind := pb.LockTable{
			Group:     0,
			Table:     101,
			ServiceID: "unreachable",
			Version:   1,
			Valid:     true,
		}
		service.tableGroups.set(
			bind.Group,
			bind.Table,
			newRemoteLockTable(
				service.serviceID,
				time.Second,
				bind,
				client,
				service.handleBindChanged,
				service.logger,
			),
		)

		txnID := []byte("ordinary-remote-unlock")
		txn := service.activeTxnHolder.getActiveTxn(txnID, true, "")
		txn.Lock()
		require.NoError(t, txn.lockAdded(bind.Group, bind, [][]byte{[]byte("key")}, service.logger))
		txn.Unlock()

		unlockDone := make(chan error, 1)
		go func() {
			unlockDone <- service.Unlock(
				context.Background(),
				txnID,
				timestamp.Timestamp{},
			)
		}()
		select {
		case <-client.unlockStarted:
		case <-time.After(time.Second):
			require.FailNow(t, "ordinary remote unlock did not start")
		}

		closeDone := make(chan error, 1)
		go func() {
			closeDone <- service.Close()
		}()
		select {
		case err := <-closeDone:
			require.NoError(t, err)
		case <-time.After(time.Second):
			require.FailNow(t, "service close did not cancel ordinary remote unlock")
		}
		require.ErrorIs(t, <-unlockDone, context.Canceled)
	})
}

type unavailableRollbackOwner struct {
	Client
	unavailable atomic.Bool
}

func (c *unavailableRollbackOwner) Send(ctx context.Context, req *pb.Request) (*pb.Response, error) {
	if req.Method == pb.Method_Unlock && c.unavailable.Load() {
		return nil, moerr.NewBackendClosedNoCtx()
	}
	return c.Client.Send(ctx, req)
}

func TestRollbackCleanupRetainsConfirmedLockUntilOwnerRecovers(t *testing.T) {
	runLockServiceTests(t, []string{"owner", "source"}, func(_ *lockTableAllocator, nodes []*service) {
		owner, source := nodes[0], nodes[1]
		gate := &unavailableRollbackOwner{Client: source.remote.client}
		source.remote.client = gate
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		opts := newTestRowExclusiveOptions()
		const table = 1024
		row := []byte{2}
		rows := [][]byte{row}
		seed, id := []byte("seed"), []byte("rollback-confirmed")
		_, err := owner.Lock(ctx, table, rows, seed, opts)
		require.NoError(t, err)
		require.NoError(t, owner.Unlock(ctx, seed, timestamp.Timestamp{}))
		_, err = source.Lock(ctx, table, rows, id, opts)
		require.NoError(t, err)

		gate.unavailable.Store(true)
		defer gate.unavailable.Store(false)
		resolved := make(chan struct{})
		pending, err := source.ResolveRollbackCleanup(id, func() { close(resolved) })
		require.NoError(t, err)
		require.True(t, pending)
		require.True(t, source.activeTxnHolder.hasActiveTxn(id))
		_, held, err := owner.GetLockHolder(ctx, table, row, opts)
		require.NoError(t, err)
		require.True(t, held)
		select {
		case <-resolved:
			t.Fatal("cleanup completed before physical release")
		default:
		}
		gate.unavailable.Store(false)
		select {
		case <-resolved:
		case <-ctx.Done():
			t.Fatal("owner recovery did not finish rollback cleanup")
		}
		_, held, err = owner.GetLockHolder(ctx, table, row, opts)
		require.NoError(t, err)
		require.False(t, held)
	})
}

func TestRollbackCleanupYieldsWhileTableBusy(t *testing.T) {
	for _, remoteProxy := range []bool{false, true} {
		name := "local"
		if remoteProxy {
			name = "proxy"
		}
		t.Run(name, func(t *testing.T) {
			var visits atomic.Int32
			var armed atomic.Bool
			attempt := make(chan struct{})
			runLockServiceTests(t, []string{"owner", "source"}, func(_ *lockTableAllocator, nodes []*service) {
				owner, source := nodes[0], nodes[1]
				source.cfg.EnableRemoteLocalProxy = remoteProxy
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				const busyTable, healthyTable = 1036, 1037
				busy, healthy, seed := []byte("busy"), []byte("healthy"), []byte("seed")
				row := []byte{1}
				rows := [][]byte{row}
				options := newTestRowExclusiveOptions()
				for _, table := range []uint64{busyTable, healthyTable} {
					if table == busyTable && !remoteProxy {
						continue
					}
					_, err := owner.Lock(ctx, table, rows, seed, options)
					require.NoError(t, err)
				}
				require.NoError(t, owner.Unlock(ctx, seed, timestamp.Timestamp{}))
				busyOptions := options
				if remoteProxy {
					busyOptions = newTestRowSharedOptions()
				}
				_, err := source.Lock(ctx, busyTable, rows, busy, busyOptions)
				require.NoError(t, err)
				_, err = source.Lock(ctx, healthyTable, rows, healthy, options)
				require.NoError(t, err)

				releaseBusy := func() func() {
					switch table := source.tableGroups.get(0, busyTable).(type) {
					case *localLockTable:
						require.False(t, remoteProxy)
						table.mu.Lock()
						return table.mu.Unlock
					case *localLockTableProxy:
						require.True(t, remoteProxy)
						table.mu.Lock()
						return table.mu.Unlock
					default:
						t.Fatalf("unexpected busy table type %T", table)
						return nil
					}
				}()
				// Release before the service fixture closes, including on failure.
				defer func() {
					if releaseBusy != nil {
						releaseBusy()
						releaseBusy = nil
					}
				}()
				busyDone, healthyDone := make(chan struct{}), make(chan struct{})
				armed.Store(true)
				transfer := make(chan error, 1)
				go func() {
					pending, err := source.ResolveRollbackCleanup(busy, func() { close(busyDone) })
					if err == nil && !pending {
						err = fmt.Errorf("busy rollback did not transfer cleanup")
					}
					transfer <- err
				}()
				select {
				case err := <-transfer:
					require.NoError(t, err)
				case <-ctx.Done():
					t.Fatal("foreground rollback waited for the busy table")
				}
				select {
				case <-attempt:
				case <-ctx.Done():
					t.Fatal("resolver did not attempt the busy rollback")
				}

				transferred, err := source.ResolveRollbackCleanup(healthy, func() { close(healthyDone) })
				require.NoError(t, err)
				require.True(t, transferred)
				select {
				case <-healthyDone:
				case <-ctx.Done():
					t.Fatal("busy table blocked the independent rollback")
				}
				_, held, err := owner.GetLockHolder(ctx, healthyTable, row, options)
				require.NoError(t, err)
				require.False(t, held)
				select {
				case <-busyDone:
					t.Fatal("busy rollback acknowledged before releasing its lock")
				default:
				}
				releaseBusy()
				releaseBusy = nil
				select {
				case <-busyDone:
				case <-ctx.Done():
					t.Fatal("busy rollback did not finish after the table became available")
				}
				busyOwner := source
				if remoteProxy {
					busyOwner = owner
				}
				_, held, err = busyOwner.GetLockHolder(ctx, busyTable, row, busyOptions)
				require.NoError(t, err)
				require.False(t, held)
			}, WithWait(func(context.Context) error {
				if armed.Load() && visits.Add(1) == 2 {
					close(attempt)
				}
				return nil
			}))
		})
	}
}

// Record actual busy-table RPCs after they return to the sender.
type rollbackUnlockRecorder struct {
	Client
	table uint64
	done  chan string
}

func (c *rollbackUnlockRecorder) Send(ctx context.Context, req *pb.Request) (*pb.Response, error) {
	resp, err := c.Client.Send(ctx, req)
	if req.Method == pb.Method_Unlock && req.LockTable.Table == c.table {
		select {
		case c.done <- string(req.Unlock.TxnID):
		default:
		}
	}
	return resp, err
}

func TestRollbackBacklogDoesNotBlockAnotherTableOnOwner(t *testing.T) {
	runLockServiceTests(t, []string{"owner", "source"}, func(_ *lockTableAllocator, nodes []*service) {
		owner, source := nodes[0], nodes[1]
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		const busyTable, healthyTable = 1444, 1445
		opts := newTestRowExclusiveOptions()
		seed := []byte("seed")
		for _, table := range []uint64{busyTable, healthyTable} {
			_, err := owner.Lock(ctx, table, newTestRows(0), seed, opts)
			require.NoError(t, err)
		}
		require.NoError(t, owner.Unlock(ctx, seed, timestamp.Timestamp{}))

		recorder := &rollbackUnlockRecorder{
			Client: source.remote.client, table: busyTable, done: make(chan string, 2*defaultHandleWorkers),
		}
		source.remote.client = recorder
		healthy := func(id string) {
			lockCtx, stop := context.WithTimeout(ctx, time.Second)
			defer stop()
			_, err := source.Lock(lockCtx, healthyTable, newTestRows(1), []byte(id), opts)
			require.NoError(t, err, "remote Lock on the healthy table")
			require.NoError(t, source.Unlock(ctx, []byte(id), timestamp.Timestamp{}))
		}
		healthy("before-contention")

		ids := make([][]byte, defaultHandleWorkers)
		for i := range ids {
			ids[i] = []byte(fmt.Sprintf("busy-%d", i))
			_, err := source.Lock(ctx, busyTable, newTestRows(byte(i+1)), ids[i], opts)
			require.NoError(t, err)
		}

		busy := owner.tableGroups.get(0, busyTable).(*localLockTable)
		busy.mu.Lock()
		locked := true
		defer func() {
			if locked {
				busy.mu.Unlock()
			}
		}()

		resolved := make(chan struct{}, len(ids))
		for _, id := range ids {
			scheduled, err := source.ResolveRollbackCleanup(id, func() { resolved <- struct{}{} })
			require.NoError(t, err)
			require.True(t, scheduled)
		}
		seen := make(map[string]struct{}, len(ids))
		for len(seen) < len(ids) {
			select {
			case id := <-recorder.done:
				seen[id] = struct{}{}
			case <-ctx.Done():
				t.Fatalf("only %d of %d rollbacks attempted owner cleanup", len(seen), len(ids))
			}
		}
		require.Empty(t, resolved, "a busy owner acknowledged cleanup before releasing the rows")
		healthy("during-contention")

		busy.mu.Unlock()
		locked = false
		for range ids {
			select {
			case <-resolved:
			case <-ctx.Done():
				t.Fatal("rollback failed to resume when the owner table became available")
			}
		}
		for i := range ids {
			_, held, err := owner.GetLockHolder(ctx, busyTable, []byte{byte(i + 1)}, opts)
			require.NoError(t, err)
			require.False(t, held)
		}
		healthy("after-contention")
	})
}

type blackholedUnlockOwner struct {
	Client
	owner      string
	table      uint64
	refreshes  atomic.Int32
	enabled    atomic.Bool
	attempted  chan (<-chan struct{})
	active     atomic.Int32
	overlapped atomic.Bool
}

func (c *blackholedUnlockOwner) Send(ctx context.Context, req *pb.Request) (*pb.Response, error) {
	if c.enabled.Load() && req.Method == pb.Method_GetBind && req.GetBind.Table == c.table {
		c.refreshes.Add(1)
	}
	if req.Method != pb.Method_Unlock || (c.owner != "" && req.LockTable.ServiceID != c.owner) || !c.enabled.Load() {
		return c.Client.Send(ctx, req)
	}
	if c.active.Add(1) > 1 {
		c.overlapped.Store(true)
	}
	defer c.active.Add(-1)
	select {
	case c.attempted <- ctx.Done():
	default:
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestRollbackBacklogDoesNotBlockHealthyOwner(t *testing.T) {
	runLockServiceTests(t, []string{"slow", "healthy", "source"}, func(_ *lockTableAllocator, nodes []*service) {
		slow, healthy, source := nodes[0], nodes[1], nodes[2]
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		const slowTable, healthyTable = 1442, 1443
		const backlog = 12
		options, healthyID := newTestRowExclusiveOptions(), []byte("healthy")
		slowClient := &blackholedUnlockOwner{
			Client: source.remote.client, owner: slow.serviceID, table: slowTable, attempted: make(chan (<-chan struct{}), backlog+2),
		}
		source.remote.client = slowClient
		for _, target := range []struct {
			service *service
			table   uint64
		}{{slow, slowTable}, {healthy, healthyTable}} {
			seed := []byte("seed")
			_, err := target.service.Lock(ctx, target.table, newTestRows(0), seed, options)
			require.NoError(t, err)
			require.NoError(t, target.service.Unlock(ctx, seed, timestamp.Timestamp{}))
		}

		slowIDs := make([][]byte, backlog)
		for i := range slowIDs {
			slowIDs[i] = []byte(fmt.Sprintf("slow-%d", i))
			_, err := source.Lock(ctx, slowTable, newTestRows(byte(i+1)), slowIDs[i], options)
			require.NoError(t, err)
		}
		_, err := source.Lock(ctx, healthyTable, newTestRows(1), healthyID, options)
		require.NoError(t, err)
		slowClient.enabled.Store(true)
		defer slowClient.enabled.Store(false)
		slowDone := make(chan struct{}, backlog)
		queueSlow := func(id []byte) {
			transferred, err := source.ResolveRollbackCleanup(id, func() { slowDone <- struct{}{} })
			require.NoError(t, err)
			require.True(t, transferred)
		}
		waitForAttempt := func() <-chan struct{} {
			select {
			case expired := <-slowClient.attempted:
				return expired
			case <-ctx.Done():
				t.Fatal("resolver did not send to the blackholed owner")
				return nil
			}
		}
		queueSlow(slowIDs[0])
		waitForAttempt()
		for _, id := range slowIDs[1:] {
			queueSlow(id)
		}
		slowAttempt := waitForAttempt()
		require.Positive(t, slowClient.refreshes.Load(), "a failed send must leave time for bind refresh")

		healthyDone := make(chan struct{})
		transferred, err := source.ResolveRollbackCleanup(healthyID, func() { close(healthyDone) })
		require.NoError(t, err)
		require.True(t, transferred)
		select {
		case <-healthyDone:
		case <-time.After(3 * time.Second):
			t.Fatal("blackholed owner backlog delayed healthy cleanup")
		}
		select {
		case <-slowAttempt:
			t.Fatal("healthy cleanup waited for the slow owner's RPC deadline")
		default:
		}
		require.False(t, slowClient.overlapped.Load())
		require.Empty(t, slowDone)
		_, held, err := healthy.GetLockHolder(ctx, healthyTable, newTestRows(1)[0], options)
		require.NoError(t, err)
		require.False(t, held)

		slowClient.enabled.Store(false)
		for range slowIDs {
			select {
			case <-slowDone:
			case <-ctx.Done():
				t.Fatal("slow cleanup did not resume when the owner recovered")
			}
		}
		for i, id := range slowIDs {
			require.False(t, source.unknownCommitResolver.isPending(id))
			_, held, err := slow.GetLockHolder(ctx, slowTable, newTestRows(byte(i + 1))[0], options)
			require.NoError(t, err)
			require.False(t, held)
		}
	})
}

func TestResolverCloseCancelsActiveOwnerAttempts(t *testing.T) {
	ids := make([]string, 1, 1+resolverWorkers)
	ids[0] = "source"
	for i := range resolverWorkers {
		ids = append(ids, fmt.Sprintf("owner-%d", i))
	}
	runLockServiceTests(t, ids, func(_ *lockTableAllocator, nodes []*service) {
		source := nodes[0]
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		client := &blackholedUnlockOwner{
			Client: source.remote.client, attempted: make(chan (<-chan struct{}), resolverWorkers),
		}
		source.remote.client = client
		options := newTestRowExclusiveOptions()
		for i, owner := range nodes[1:] {
			table := uint64(1450 + i)
			_, err := owner.Lock(ctx, table, newTestRows(1), []byte("seed"), options)
			require.NoError(t, err)
			require.NoError(t, owner.Unlock(ctx, []byte("seed"), timestamp.Timestamp{}))
			_, err = source.Lock(ctx, table, newTestRows(1), []byte(ids[i+1]), options)
			require.NoError(t, err)
		}
		client.enabled.Store(true)
		resolved := make(chan string, resolverWorkers)
		for _, id := range ids[1:] {
			transferred, err := source.ResolveRollbackCleanup([]byte(id), func() { resolved <- id })
			require.NoError(t, err)
			require.True(t, transferred)
		}
		active := make([]<-chan struct{}, 0, resolverWorkers)
		for range resolverWorkers {
			select {
			case expired := <-client.attempted:
				active = append(active, expired)
			case <-ctx.Done():
				t.Fatal("workers did not reach their blocked owners")
			}
		}
		for _, expired := range active {
			select {
			case <-expired:
				t.Fatal("a worker finished before the close barrier")
			default:
			}
		}

		closed := make(chan error, 1)
		go func() { closed <- source.Close() }()
		select {
		case err := <-closed:
			require.NoError(t, err)
		case <-ctx.Done():
			t.Fatal("Close did not join active resolver attempts")
		}
		require.Zero(t, client.active.Load())
		callbacks := make([]string, 0, resolverWorkers)
		for range resolverWorkers {
			select {
			case id := <-resolved:
				callbacks = append(callbacks, id)
			case <-ctx.Done():
				t.Fatal("Close did not return cleanup admission")
			}
		}
		require.ElementsMatch(t, ids[1:], callbacks)
		require.Empty(t, resolved)
	})
}

func TestResolverCompletionPreservesUpdatedFence(t *testing.T) {
	resolver := newUnknownCommitResolver(nil)
	resolver.mu.running = true
	id := []byte("resolution")
	deadline := time.Now().Add(time.Hour)
	completed := make(chan struct{}, 1)
	require.NoError(t, resolver.enqueue(id, deadline, 1, func() { completed <- struct{}{} }))
	old := resolver.mu.pending[string(id)]
	require.True(t, resolver.startAttempt(old))

	require.NoError(t, resolver.enqueue(id, deadline, 2, nil))
	replacement := resolver.mu.pending[string(id)]
	require.False(t, resolver.startAttempt(replacement))
	resolver.finishAttempt(old, nil)
	require.True(t, resolver.isPending(id))
	require.Empty(t, completed)

	require.True(t, resolver.startAttempt(replacement))
	resolver.finishAttempt(replacement, nil)
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("current resolution did not complete")
	}
	require.False(t, resolver.isPending(id))
}

func TestResolverDeferredOwnerRetriesWhenSlotOpens(t *testing.T) {
	resolver := newUnknownCommitResolver(nil)
	resolver.mu.running = true
	require.True(t, resolver.tryOwner("owner", "blocker"))
	ids := []string{"first", "next", "last"}
	for _, id := range ids {
		require.NoError(t, resolver.enqueue([]byte(id), time.Time{}, 0, nil))
		txn := resolver.mu.pending[id]
		require.True(t, resolver.startAttempt(txn))
		resolver.finishAttempt(txn, resolverUnlockDeferred{owner: "owner"})
	}
	_, ready := resolver.nextOwnerAttempt()
	require.False(t, ready)

	resolver.finishOwner("owner", false)
	for _, id := range ids {
		txn, ready := resolver.nextOwnerAttempt()
		require.True(t, ready)
		require.Equal(t, id, string(txn.id))
		_, duplicate := resolver.nextOwnerAttempt()
		require.False(t, duplicate, "only one parked transaction owns the next slot")
		require.False(t, resolver.tryOwner("owner", "fresh"), "a fresh attempt must not overtake the reserved transaction")
		require.True(t, resolver.tryOwner("owner", id))
		resolver.finishOwner("owner", false)
		resolver.finishAttempt(txn, nil)
	}
	require.Empty(t, resolver.mu.owners)
}

type queuedOwnerUnlocks struct {
	Client
	owners  map[string]chan struct{}
	started chan string
}

func (c *queuedOwnerUnlocks) Send(ctx context.Context, req *pb.Request) (*pb.Response, error) {
	permit := c.owners[req.LockTable.ServiceID]
	if req.Method == pb.Method_Unlock && permit != nil {
		select {
		case c.started <- req.LockTable.ServiceID:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		select {
		case <-permit:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return c.Client.Send(ctx, req)
}

func TestResolverSharesReadySlotsWithNewTransactions(t *testing.T) {
	ids := make([]string, 2, 2+resolverWorkers)
	ids[0], ids[1] = "source", "healthy"
	for i := range resolverWorkers {
		ids = append(ids, fmt.Sprintf("queued-owner-%d", i))
	}
	runLockServiceTests(t, ids, func(_ *lockTableAllocator, nodes []*service) {
		source, healthy := nodes[0], nodes[1]
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
		defer cancel()
		client := &queuedOwnerUnlocks{
			Client: source.remote.client, owners: make(map[string]chan struct{}),
			started: make(chan string, resolverWorkers*2),
		}
		source.remote.client = client
		o := newTestRowExclusiveOptions()
		const perOwner = 3
		const healthyTable = 1460
		healthyID := []byte("new-healthy")
		for i, owner := range nodes[1:] {
			table := uint64(healthyTable + i)
			_, err := owner.Lock(ctx, table, newTestRows(0), []byte("seed"), o)
			require.NoError(t, err)
			require.NoError(t, owner.Unlock(ctx, []byte("seed"), timestamp.Timestamp{}))
			if i == 0 {
				_, err = source.Lock(ctx, table, newTestRows(1), healthyID, o)
				require.NoError(t, err)
				continue
			}
			client.owners[owner.serviceID] = make(chan struct{}, 1)
			for key := byte(1); key <= perOwner; key++ {
				id := []byte(fmt.Sprintf("owner-%d-row-%d", i, key))
				_, err = source.Lock(ctx, table, newTestRows(key), id, o)
				require.NoError(t, err)
			}
		}
		resolver := source.unknownCommitResolver
		// Keep each owner occupied while the dispatchers discover the routes,
		// so every owner has a parked backlog before any RPC can proceed.
		for ownerID := range client.owners {
			require.True(t, resolver.tryOwner(ownerID, "blocker"))
		}
		for i := 1; i < len(nodes)-1; i++ {
			for key := byte(1); key <= perOwner; key++ {
				id := []byte(fmt.Sprintf("owner-%d-row-%d", i, key))
				scheduled, err := source.ResolveRollbackCleanup(id, nil)
				require.NoError(t, err)
				require.True(t, scheduled)
			}
		}
		require.Eventually(t, func() bool {
			resolver.mu.Lock()
			defer resolver.mu.Unlock()
			count := 0
			for _, owner := range resolver.mu.owners {
				count += len(owner.waiting)
			}
			return count == resolverWorkers*perOwner
		}, time.Second, time.Millisecond)
		for ownerID := range client.owners {
			resolver.finishOwner(ownerID, false)
		}
		var first string
		for range resolverWorkers {
			select {
			case ownerID := <-client.started:
				first = ownerID
			case <-ctx.Done():
				t.Fatal("owner backlog did not occupy every worker")
			}
		}

		done := make(chan struct{})
		scheduled, err := source.ResolveRollbackCleanup(healthyID, func() { close(done) })
		require.NoError(t, err)
		require.True(t, scheduled)
		client.owners[first] <- struct{}{}
		select {
		case <-done:
		case <-client.started:
			// The next worker can send before a completed transaction's
			// asynchronous callback runs; inspect the actual owner below.
		case <-ctx.Done():
			t.Fatal("healthy owner did not receive cleanup")
		}
		_, held, err := healthy.GetLockHolder(ctx, healthyTable, newTestRows(1)[0], o)
		require.NoError(t, err)
		require.False(t, held)
	})
}
