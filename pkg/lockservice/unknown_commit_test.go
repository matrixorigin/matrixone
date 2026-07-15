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
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		service := services[0]
		now := time.Now()
		for i := 0; i < maxPersistentFenceFrontierEntries; i++ {
			// Increasing sequence and decreasing expiry intentionally build the
			// largest exact non-dominated frontier.
			_, _, ok := service.canUnlockUnknownCommits(
				ctx,
				[][]byte{[]byte(fmt.Sprintf("seed-%d", i))},
				now.Add(time.Duration(maxPersistentFenceFrontierEntries-i+3)*time.Second),
				uint64(i+1),
			)
			require.True(t, ok)
		}

		overflowTxn := []byte("overflow")
		_, err := service.Lock(
			ctx,
			1,
			[][]byte{[]byte("overflow-key")},
			overflowTxn,
			newTestRowExclusiveOptions(),
		)
		require.NoError(t, err)
		require.NoError(t, service.ResolveCommitUnknown(
			overflowTxn,
			now.Add(time.Second),
			maxPersistentFenceFrontierEntries+1,
			nil,
		))

		require.Eventually(t, func() bool {
			return !service.activeTxnHolder.hasActiveTxn(overflowTxn) &&
				!service.unknownCommitResolver.isPending(overflowTxn)
		}, 2*time.Second, 10*time.Millisecond)

		ctl := allocator.getCtl(service.serviceID)
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
		require.NoError(t, service.ResolveCommitUnknown(
			txnID,
			time.Now().Add(time.Hour),
			service.NextCommitSequence(),
			nil,
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
	})
}
