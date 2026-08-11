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
	"sync"
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

func TestUnknownCommitCallbackAdmissionIsBoundedAndSealable(t *testing.T) {
	callbacks := newUnknownCommitCallbacks(1)
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseCallback := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseCallback)
	callback, err := callbacks.admit(func() {
		close(started)
		<-release
	})
	require.NoError(t, err)
	callback.dispatch()
	<-started

	_, err = callbacks.admit(func() {})
	require.ErrorContains(t, err, "capacity exhausted")
	callbacks.seal()
	_, err = callbacks.admit(func() {})
	require.ErrorContains(t, err, "stopping")

	releaseCallback()
	require.Eventually(t, func() bool {
		return len(callbacks.slots) == 0
	}, time.Second, time.Millisecond)
}

func TestUnknownCommitCallbackSaturationKeepsCleanupOwned(t *testing.T) {
	runLockServiceTests(t, []string{"s1"}, func(_ *lockTableAllocator, services []*service) {
		service := services[0]
		service.unknownCommitResolver.callbacks = newUnknownCommitCallbacks(1)
		started := make(chan struct{})
		release := make(chan struct{})
		t.Cleanup(func() {
			select {
			case <-release:
			default:
				close(release)
			}
		})

		require.NoError(t, service.ResolveCommitUnknown(
			[]byte("callback-slot-owner"),
			time.Now().Add(time.Hour),
			service.NextCommitSequence(),
			func() {
				close(started)
				<-release
			},
		))
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("first completion callback did not start")
		}

		saturatedTxn := []byte("callback-slot-saturated")
		var unexpected atomic.Int32
		retainedCallback := func() { unexpected.Add(1) }
		err := service.ResolveCommitUnknown(
			saturatedTxn,
			time.Now().Add(time.Hour),
			service.NextCommitSequence(),
			retainedCallback,
		)
		require.ErrorContains(t, err, "capacity exhausted")
		resolutionDone, scheduled := UnknownCommitResolutionDone(err)
		require.True(t, scheduled)
		// Callback ownership stayed with the caller, but lock-cleanup ownership
		// still transferred. Its terminal signal preserves the caller's admission
		// until cleanup finishes without exceeding the callback execution bound.
		require.Eventually(t, func() bool {
			return !service.unknownCommitResolver.isPending(saturatedTxn)
		}, 5*time.Second, time.Millisecond)
		select {
		case <-resolutionDone:
		case <-time.After(5 * time.Second):
			t.Fatal("saturated callback owner did not observe terminal cleanup")
		}
		require.Zero(t, unexpected.Load())
		retainedCallback()
		require.Equal(t, int32(1), unexpected.Load())

		close(release)
		require.Eventually(t, func() bool {
			return len(service.unknownCommitResolver.callbacks.slots) == 0
		}, time.Second, time.Millisecond)
	})
}

func TestUnknownCommitDuplicateCallbackKeepsNewOwner(t *testing.T) {
	runLockServiceTests(t, []string{"s1"}, func(allocator *lockTableAllocator, services []*service) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		service := services[0]
		txnID := []byte("duplicate-callback")
		_, err := service.Lock(
			ctx,
			1,
			[][]byte{[]byte("duplicate-callback-key")},
			txnID,
			newTestRowExclusiveOptions(),
		)
		require.NoError(t, err)
		_, err = allocator.Valid(service.serviceID, txnID, nil)
		require.NoError(t, err)

		deadline := time.Now().Add(time.Hour)
		var firstCalled atomic.Int32
		var secondCalled atomic.Int32
		require.NoError(t, service.ResolveCommitUnknown(
			txnID,
			deadline,
			service.NextCommitSequence(),
			func() { firstCalled.Add(1) },
		))

		retainedCallback := func() { secondCalled.Add(1) }
		err = service.ResolveCommitUnknown(
			txnID,
			deadline.Add(time.Second),
			service.NextCommitSequence(),
			retainedCallback,
		)
		require.ErrorContains(t, err, "callback already registered")
		resolutionDone, scheduled := UnknownCommitResolutionDone(err)
		require.True(t, scheduled)
		require.Len(t, service.unknownCommitResolver.callbacks.slots, 1)
		require.True(t, service.unknownCommitResolver.isPending(txnID))
		require.Zero(t, firstCalled.Load())
		require.Zero(t, secondCalled.Load())

		allocator.FinishCommit(service.serviceID, txnID)
		select {
		case <-resolutionDone:
		case <-time.After(5 * time.Second):
			t.Fatal("duplicate callback owner did not observe terminal cleanup")
		}
		require.Eventually(t, func() bool {
			return firstCalled.Load() == 1 &&
				len(service.unknownCommitResolver.callbacks.slots) == 0
		}, time.Second, time.Millisecond)
		require.Zero(t, secondCalled.Load())

		// A non-nil error leaves the new callback with its caller. Invoking it after
		// the shared terminal signal models the txn client's retained-owner path.
		retainedCallback()
		require.Equal(t, int32(1), secondCalled.Load())
	})
}

func TestUnknownCommitScheduledSignalCompletesOnClose(t *testing.T) {
	runLockServiceTests(t, []string{"s1"}, func(allocator *lockTableAllocator, services []*service) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		service := services[0]
		service.unknownCommitResolver.callbacks = newUnknownCommitCallbacks(0)
		txnID := []byte("scheduled-signal-close")
		_, err := service.Lock(
			ctx,
			1,
			[][]byte{[]byte("scheduled-signal-close-key")},
			txnID,
			newTestRowExclusiveOptions(),
		)
		require.NoError(t, err)
		_, err = allocator.Valid(service.serviceID, txnID, nil)
		require.NoError(t, err)

		var called atomic.Int32
		retainedCallback := func() { called.Add(1) }
		err = service.ResolveCommitUnknown(
			txnID,
			time.Now().Add(time.Hour),
			service.NextCommitSequence(),
			retainedCallback,
		)
		require.ErrorContains(t, err, "capacity exhausted")
		resolutionDone, scheduled := UnknownCommitResolutionDone(err)
		require.True(t, scheduled)
		require.True(t, service.unknownCommitResolver.isPending(txnID))

		require.NoError(t, service.Close())
		select {
		case <-resolutionDone:
		case <-time.After(time.Second):
			t.Fatal("service Close did not publish terminal cleanup")
		}
		require.Zero(t, called.Load())
		retainedCallback()
		require.Equal(t, int32(1), called.Load())
	})
}

func TestUnknownCommitCallbackAdmissionRollsBackWhenTaskStartFails(t *testing.T) {
	runLockServiceTests(t, []string{"s1"}, func(_ *lockTableAllocator, services []*service) {
		service := services[0]
		service.stopper.Stop()
		var called atomic.Int32
		err := service.ResolveCommitUnknown(
			[]byte("resolver-task-start-failure"),
			time.Now().Add(time.Hour),
			service.NextCommitSequence(),
			func() { called.Add(1) },
		)
		require.Error(t, err)
		require.False(t, service.unknownCommitResolver.isPending(
			[]byte("resolver-task-start-failure")))
		require.Empty(t, service.unknownCommitResolver.callbacks.slots)
		require.NoError(t, service.Close())
		require.Zero(t, called.Load())
	})
}

func TestUnknownCommitBlockingCallbackRacesCloseWithoutSelfWait(t *testing.T) {
	runLockServiceTests(t, []string{"s1"}, func(_ *lockTableAllocator, services []*service) {
		service := services[0]
		started := make(chan struct{})
		release := make(chan struct{})
		var releaseOnce sync.Once
		releaseCallback := func() { releaseOnce.Do(func() { close(release) }) }
		t.Cleanup(releaseCallback)
		callbackCloseResult := make(chan error, 1)

		require.NoError(t, service.ResolveCommitUnknown(
			[]byte("blocking-reentrant-close"),
			time.Now().Add(time.Hour),
			service.NextCommitSequence(),
			func() {
				close(started)
				<-release
				callbackCloseResult <- service.Close()
			},
		))
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("completion callback did not start")
		}

		closeDone := make(chan error, 1)
		go func() { closeDone <- service.Close() }()
		select {
		case err := <-closeDone:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("service close waited for external callback code")
		}

		err := service.ResolveCommitUnknown(
			[]byte("post-close"),
			time.Now().Add(time.Hour),
			1,
			func() {},
		)
		require.ErrorContains(t, err, "lock service is closing")

		releaseCallback()
		select {
		case err := <-callbackCloseResult:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("completion callback deadlocked re-entering Close")
		}
		require.Eventually(t, func() bool {
			return len(service.unknownCommitResolver.callbacks.slots) == 0
		}, time.Second, time.Millisecond)
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
		fenceEntered := make(chan struct{}, 1)
		holder.beforeFenceTxnLock = func(candidate *activeTxn) {
			if candidate == txn {
				select {
				case fenceEntered <- struct{}{}:
				default:
				}
			}
		}
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
		<-fenceEntered
		close(lockTable.release)

		select {
		case err := <-unlockDone:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("unknown-commit cleanup deadlocked with bind fencing")
		}
		select {
		case <-fenceDone:
		case <-time.After(5 * time.Second):
			t.Fatal("bind fencing deadlocked with unknown-commit cleanup")
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
