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

package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/require"
)

func TestCleanupRootMustPrecedeSideEffectsAndUseImmutableNamespace(t *testing.T) {
	ctx := context.Background()
	repository := newMemoryCleanupRootRepository()
	root := CleanupRoot{
		RootID:               "root-1",
		AttemptID:            "attempt-1",
		Mode:                 CleanupModeArchiveRewrite,
		ArchivePrefix:        "archive/root-1/attempt-1",
		BookingPrefix:        "tae/root-1/attempt-1/booking",
		ReservedCleanupBytes: 1,
		State:                CleanupRootRegistered,
		StateVersion:         1,
		CleanupAfter:         time.Now(),
	}
	require.NoError(t, repository.Register(ctx, root))
	guard := NewCleanupRootSideEffectGuard(repository)
	require.NoError(t, guard.EnsureDurable(ctx, "root-1", "attempt-1"))
	require.Error(t, guard.EnsureDurable(ctx, "root-1", "attempt-other"))
	uploading, err := repository.Transition(
		ctx,
		root.RootID,
		root.AttemptID,
		root.ExecutorEpoch,
		CleanupRootRegistered,
		root.StateVersion,
		CleanupRootUploading,
	)
	require.NoError(t, err)
	verified, err := repository.Transition(
		ctx,
		uploading.RootID,
		uploading.AttemptID,
		uploading.ExecutorEpoch,
		CleanupRootUploading,
		uploading.StateVersion,
		CleanupRootVerified,
	)
	require.NoError(t, err)
	require.Error(t, guard.EnsureDurable(
		ctx,
		verified.RootID,
		verified.AttemptID,
	))

	root.ArchivePrefix = "archive/shared"
	require.Error(t, ValidateCleanupRoot(root))
}

func TestCleanupRootStateMachineAndCommitUnknownAreFailClosed(t *testing.T) {
	ctx := context.Background()
	repository := newMemoryCleanupRootRepository()
	root := lifecycleCleanupTestRoot()
	require.NoError(t, repository.Register(ctx, root))

	updated, err := repository.Transition(
		ctx,
		root.RootID,
		root.AttemptID,
		root.ExecutorEpoch,
		CleanupRootRegistered,
		1,
		CleanupRootUploading,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(2), updated.StateVersion)
	_, err = repository.Transition(
		ctx,
		root.RootID,
		root.AttemptID,
		root.ExecutorEpoch+1,
		CleanupRootUploading,
		2,
		CleanupRootVerified,
	)
	require.Error(t, err)

	updated, err = repository.Transition(
		ctx,
		root.RootID,
		root.AttemptID,
		root.ExecutorEpoch,
		CleanupRootUploading,
		2,
		CleanupRootVerified,
	)
	require.NoError(t, err)
	updated, err = repository.Transition(
		ctx,
		root.RootID,
		root.AttemptID,
		root.ExecutorEpoch,
		CleanupRootVerified,
		3,
		CleanupRootFinalizing,
	)
	require.NoError(t, err)
	updated, err = repository.Transition(
		ctx,
		root.RootID,
		root.AttemptID,
		root.ExecutorEpoch,
		CleanupRootFinalizing,
		4,
		CleanupRootCommitUnknown,
	)
	require.NoError(t, err)
	require.Equal(t, CleanupRootCommitUnknown, updated.State)
	require.False(t, CanSweepCleanupRoot(updated))
}

func TestValidateCleanupRootRejectsInvalidOwnershipCombinations(t *testing.T) {
	valid := lifecycleCleanupTestRoot()
	require.NoError(t, ValidateCleanupRoot(valid))

	for _, test := range []struct {
		name   string
		mutate func(*CleanupRoot)
		want   string
	}{
		{
			name: "missing immutable identity",
			mutate: func(root *CleanupRoot) {
				root.RootID = ""
			},
			want: "identity is incomplete",
		},
		{
			name: "missing resource reservation",
			mutate: func(root *CleanupRoot) {
				root.ReservedCleanupBytes = 0
			},
			want: "state identity is incomplete",
		},
		{
			name: "whole archive prefix escapes root",
			mutate: func(root *CleanupRoot) {
				root.ArchivePrefix = "archive/shared"
			},
			want: "Archive prefix is not Root scoped",
		},
		{
			name: "rewrite booking escapes root",
			mutate: func(root *CleanupRoot) {
				root.Mode = CleanupModeArchiveRewrite
				root.BookingPrefix = "tae/shared"
			},
			want: "Booking prefix is not Root scoped",
		},
		{
			name: "completed rewrite retains tae ownership",
			mutate: func(root *CleanupRoot) {
				root.Mode = CleanupModeTTLRewrite
				root.BookingPrefix = "tae/" + root.RootID + "/" + root.AttemptID + "/booking"
				root.TemporaryCleanupDone = true
				root.SegmentID = "live-segment"
			},
			want: "still owns TAE files",
		},
		{
			name: "unknown cleanup mode",
			mutate: func(root *CleanupRoot) {
				root.Mode = "UNKNOWN"
			},
			want: "unknown Lifecycle cleanup mode",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := valid
			test.mutate(&root)
			require.ErrorContains(t, ValidateCleanupRoot(root), test.want)
		})
	}
}

func TestCleanupSweeperWaitsForQuiescenceAndCatchesLatePut(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0)
	repository := newMemoryCleanupRootRepository()
	root := lifecycleCleanupTestRoot()
	root.State = CleanupRootDeletePending
	root.CleanupAfter = now
	require.NoError(t, repository.Register(ctx, root))
	store := newMemoryArchiveStore()
	require.NoError(t, store.Put(ctx, root.ArchivePrefix+"/payload-old", []byte("old")))

	sweeper := CleanupSweeper{
		Roots:            repository,
		Archive:          store,
		QuiescenceWindow: time.Minute,
	}
	require.NoError(t, sweeper.SweepOne(ctx, root.RootID, now))
	require.Empty(t, store.keys())
	current, err := repository.Get(ctx, root.RootID)
	require.NoError(t, err)
	require.Equal(t, CleanupRootDeleting, current.State)
	require.True(t, current.QuiescenceSince.IsZero())

	require.NoError(t, sweeper.SweepOne(ctx, root.RootID, now.Add(30*time.Second)))
	current, err = repository.Get(ctx, root.RootID)
	require.NoError(t, err)
	require.Equal(t, now.Add(30*time.Second), current.QuiescenceSince)

	require.NoError(t, store.Put(ctx, root.ArchivePrefix+"/payload-late", []byte("late")))
	require.NoError(t, sweeper.SweepOne(ctx, root.RootID, now.Add(45*time.Second)))
	current, err = repository.Get(ctx, root.RootID)
	require.NoError(t, err)
	require.True(t, current.QuiescenceSince.IsZero())

	require.NoError(t, sweeper.SweepOne(ctx, root.RootID, now.Add(2*time.Minute)))
	require.NoError(t, sweeper.SweepOne(ctx, root.RootID, now.Add(4*time.Minute)))
	current, err = repository.Get(ctx, root.RootID)
	require.NoError(t, err)
	require.Equal(t, CleanupRootCleaned, current.State)
}

func TestCleanupSweeperRejectsCorruptPersistedNamespaceBeforeDelete(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0)
	repository := newMemoryCleanupRootRepository()
	root := lifecycleCleanupTestRoot()
	root.State = CleanupRootDeletePending
	root.CleanupAfter = now
	require.NoError(t, repository.Register(ctx, root))

	repository.mu.Lock()
	corrupt := repository.roots[root.RootID]
	corrupt.ArchivePrefix = "archive/shared"
	repository.roots[root.RootID] = corrupt
	repository.mu.Unlock()

	store := newMemoryArchiveStore()
	require.NoError(t, store.Put(ctx, "archive/shared/unrelated", []byte("keep")))
	sweeper := CleanupSweeper{
		Roots:            repository,
		Archive:          store,
		QuiescenceWindow: time.Minute,
	}
	require.ErrorContains(
		t,
		sweeper.SweepOne(ctx, root.RootID, now),
		"Root scoped",
	)
	require.Equal(t, []string{"archive/shared/unrelated"}, store.keys())
}

func TestCleanupSweeperRejectsSiblingPrefixBeforeDelete(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0)
	repository := newMemoryCleanupRootRepository()
	root := lifecycleCleanupTestRoot()
	root.State = CleanupRootDeletePending
	root.CleanupAfter = now
	require.NoError(t, repository.Register(ctx, root))
	store := &escapingCleanupStore{
		listed: []string{root.ArchivePrefix + "-sibling/payload"},
	}
	sweeper := CleanupSweeper{
		Roots:            repository,
		Archive:          store,
		QuiescenceWindow: time.Minute,
	}
	require.ErrorContains(
		t,
		sweeper.SweepOne(ctx, root.RootID, now),
		"outside",
	)
	require.Empty(t, store.deleted)
}

func TestCleanupSweeperPublishesPurgeBeforeRootBecomesCleaned(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0)
	repository := newMemoryCleanupRootRepository()
	root := lifecycleCleanupTestRoot()
	root.State = CleanupRootDeleting
	root.CleanupAfter = now
	root.QuiescenceSince = now.Add(-time.Minute)
	require.NoError(t, repository.Register(ctx, root))

	finalizeErr := errors.New("dataset-purge-unavailable")
	finalizeCalls := 0
	sweeper := CleanupSweeper{
		Roots:            repository,
		Archive:          newMemoryArchiveStore(),
		QuiescenceWindow: time.Second,
		FinalizePublication: func(context.Context, CleanupRoot) error {
			finalizeCalls++
			if finalizeCalls == 1 {
				return finalizeErr
			}
			return nil
		},
	}

	require.ErrorIs(t, sweeper.SweepOne(ctx, root.RootID, now), finalizeErr)
	current, err := repository.Get(ctx, root.RootID)
	require.NoError(t, err)
	require.Equal(t, CleanupRootDeleting, current.State)

	require.NoError(t, sweeper.SweepOne(ctx, root.RootID, now.Add(time.Second)))
	current, err = repository.Get(ctx, root.RootID)
	require.NoError(t, err)
	require.Equal(t, CleanupRootCleaned, current.State)
	require.Equal(t, 2, finalizeCalls)
}

func TestCleanupSweeperDeletesRootOwnedBookingAndLiveStaging(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	repository := newMemoryCleanupRootRepository()
	root := lifecycleCleanupTestRoot()
	root.Mode = CleanupModeArchiveRewrite
	root.State = CleanupRootDeletePending
	root.CleanupAfter = now
	root.BookingPrefix = "tae/" + root.RootID + "/" + root.AttemptID + "/booking"
	segmentID := types.Uuid{1, 2, 3}
	root.SegmentID = segmentID.String()
	root.OrdinalUpperBound = 2
	require.NoError(t, repository.Register(ctx, root))

	archive := newMemoryArchiveStore()
	tae := newMemoryArchiveStore()
	require.NoError(t, archive.Put(
		ctx,
		root.ArchivePrefix+"/payload",
		[]byte("archive"),
	))
	require.NoError(t, tae.Put(
		ctx,
		root.BookingPrefix+"/page",
		[]byte("booking"),
	))
	for ordinal := uint16(0); ordinal < 2; ordinal++ {
		require.NoError(t, tae.Put(
			ctx,
			objectio.BuildObjectName(&segmentID, ordinal).String(),
			[]byte("live"),
		))
	}
	sweeper := CleanupSweeper{
		Roots:            repository,
		Archive:          archive,
		TAE:              tae,
		QuiescenceWindow: time.Second,
	}
	require.NoError(t, sweeper.SweepOne(ctx, root.RootID, now))
	require.NoError(t, sweeper.SweepOne(ctx, root.RootID, now.Add(time.Second)))
	require.Empty(t, archive.keys())
	require.Empty(t, tae.keys())
}

func TestCleanupFaultBeforeDeleteRetainsRootOwnedFilesForRetry(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	repository := newMemoryCleanupRootRepository()
	root := lifecycleCleanupTestRoot()
	root.State = CleanupRootDeletePending
	root.CleanupAfter = now
	require.NoError(t, repository.Register(ctx, root))
	store := newMemoryArchiveStore()
	payload := root.ArchivePrefix + "/payload"
	require.NoError(t, store.Put(ctx, payload, []byte("archive")))
	faults := NewProgrammableFaultInjector(map[FaultPoint]FaultAction{
		FaultBeforeCleanupDelete: FailOnHit(1, "cleanup-delete-crash"),
	})
	sweeper := CleanupSweeper{
		Roots:            repository,
		Archive:          store,
		QuiescenceWindow: time.Second,
		Faults:           faults,
	}

	require.ErrorContains(t, sweeper.SweepOne(ctx, root.RootID, now), "cleanup-delete-crash")
	current, err := repository.Get(ctx, root.RootID)
	require.NoError(t, err)
	require.Equal(t, CleanupRootDeleting, current.State)
	require.Equal(t, []string{payload}, store.keys())

	require.NoError(t, sweeper.SweepOne(ctx, root.RootID, now.Add(time.Second)))
	require.Empty(t, store.keys())
}

func TestCleanupFaultBeforeListRetainsRootOwnedFilesForRetry(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	repository := newMemoryCleanupRootRepository()
	root := lifecycleCleanupTestRoot()
	root.State = CleanupRootDeletePending
	root.CleanupAfter = now
	require.NoError(t, repository.Register(ctx, root))
	store := newMemoryArchiveStore()
	payload := root.ArchivePrefix + "/payload"
	require.NoError(t, store.Put(ctx, payload, []byte("archive")))
	faults := NewProgrammableFaultInjector(map[FaultPoint]FaultAction{
		FaultBeforeCleanupList: FailOnHit(1, "cleanup-list-crash"),
	})
	sweeper := CleanupSweeper{
		Roots:            repository,
		Archive:          store,
		QuiescenceWindow: time.Second,
		Faults:           faults,
	}

	require.ErrorContains(t, sweeper.SweepOne(ctx, root.RootID, now), "cleanup-list-crash")
	current, err := repository.Get(ctx, root.RootID)
	require.NoError(t, err)
	require.Equal(t, CleanupRootDeleting, current.State)
	require.Equal(t, []string{payload}, store.keys())

	require.NoError(t, sweeper.SweepOne(ctx, root.RootID, now.Add(time.Second)))
	require.Empty(t, store.keys())
}

func TestCleanupFaultBeforeRootCASLeavesDeletePendingForRetry(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	repository := newMemoryCleanupRootRepository()
	root := lifecycleCleanupTestRoot()
	root.State = CleanupRootDeletePending
	root.CleanupAfter = now
	require.NoError(t, repository.Register(ctx, root))
	faults := NewProgrammableFaultInjector(map[FaultPoint]FaultAction{
		FaultBeforeRootCAS: FailOnHit(1, "cleanup-cas-crash"),
	})
	sweeper := CleanupSweeper{
		Roots:            repository,
		Archive:          newMemoryArchiveStore(),
		QuiescenceWindow: time.Second,
		Faults:           faults,
	}

	require.ErrorContains(t, sweeper.SweepOne(ctx, root.RootID, now), "cleanup-cas-crash")
	current, err := repository.Get(ctx, root.RootID)
	require.NoError(t, err)
	require.Equal(t, CleanupRootDeletePending, current.State)

	require.NoError(t, sweeper.SweepOne(ctx, root.RootID, now.Add(time.Second)))
	current, err = repository.Get(ctx, root.RootID)
	require.NoError(t, err)
	require.Equal(t, CleanupRootDeleting, current.State)
}

func TestCleanupFaultAfterDeleteReconcilesFromProviderState(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	repository := newMemoryCleanupRootRepository()
	root := lifecycleCleanupTestRoot()
	root.State = CleanupRootDeletePending
	root.CleanupAfter = now
	require.NoError(t, repository.Register(ctx, root))
	store := newMemoryArchiveStore()
	payload := root.ArchivePrefix + "/payload"
	require.NoError(t, store.Put(ctx, payload, []byte("archive")))
	faults := NewProgrammableFaultInjector(map[FaultPoint]FaultAction{
		FaultAfterCleanupDelete: FailOnHit(1, "cleanup-delete-response-lost"),
	})
	sweeper := CleanupSweeper{
		Roots:            repository,
		Archive:          store,
		QuiescenceWindow: time.Second,
		Faults:           faults,
	}

	require.ErrorContains(
		t,
		sweeper.SweepOne(ctx, root.RootID, now),
		"cleanup-delete-response-lost",
	)
	current, err := repository.Get(ctx, root.RootID)
	require.NoError(t, err)
	require.Equal(t, CleanupRootDeleting, current.State)
	require.Empty(t, store.keys())

	require.NoError(t, sweeper.SweepOne(ctx, root.RootID, now.Add(time.Second)))
	require.NoError(t, sweeper.SweepOne(ctx, root.RootID, now.Add(3*time.Second)))
	current, err = repository.Get(ctx, root.RootID)
	require.NoError(t, err)
	require.Equal(t, CleanupRootCleaned, current.State)
}

func TestCleanupRootCASResponseLossResumesFromPersistedState(t *testing.T) {
	repository := newMemoryCleanupRootRepository()
	store := newMemoryArchiveStore()
	root := lifecycleCleanupTestRoot()
	root.State = CleanupRootDeletePending
	now := time.Now()
	root.CleanupAfter = now
	require.NoError(t, repository.Register(context.Background(), root))
	faults := NewProgrammableFaultInjector(map[FaultPoint]FaultAction{
		FaultAfterRootCAS: FailOnHit(1, "cleanup-cas-response-lost"),
	})
	sweeper := CleanupSweeper{
		Roots:            repository,
		Archive:          store,
		QuiescenceWindow: time.Second,
		Faults:           faults,
	}
	require.ErrorContains(t,
		sweeper.SweepOne(context.Background(), root.RootID, now),
		"cleanup-cas-response-lost",
	)
	persisted, err := repository.Get(context.Background(), root.RootID)
	require.NoError(t, err)
	require.Equal(t, CleanupRootDeleting, persisted.State)

	require.NoError(t,
		sweeper.SweepOne(context.Background(), root.RootID, now),
	)
	require.NoError(t,
		sweeper.SweepOne(
			context.Background(),
			root.RootID,
			now.Add(2*time.Second),
		),
	)
	persisted, err = repository.Get(context.Background(), root.RootID)
	require.NoError(t, err)
	require.Equal(t, CleanupRootCleaned, persisted.State)
}

func TestCleanupRootFailureMovesBehindDuePage(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(2000, 0)
	repository := newMemoryCleanupRootRepository()
	root := lifecycleCleanupTestRoot()
	root.State = CleanupRootDeleting
	root.CleanupAfter = now.Add(-time.Minute)
	require.NoError(t, repository.Register(ctx, root))

	updated, err := DeferCleanupRoot(
		ctx,
		repository,
		root.RootID,
		now,
		fmt.Errorf("permanent credential failure"),
	)
	require.NoError(t, err)
	require.Equal(t, now.Add(cleanupRetryBackoff), updated.CleanupAfter)
	require.Equal(t, "permanent credential failure", updated.LastError)
	require.Equal(t, root.State, updated.State)
}

func TestPublishedRewritePurgeDoesNotDeleteTransferredLiveObject(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	repository := newMemoryCleanupRootRepository()
	root := lifecycleCleanupTestRoot()
	root.Mode = CleanupModeArchiveRewrite
	root.State = CleanupRootPublished
	root.SegmentID = ""
	root.OrdinalUpperBound = 0
	root.BookingPrefix = ""
	root.TemporaryCleanupDone = true
	require.NoError(t, repository.Register(ctx, root))

	archive := newMemoryArchiveStore()
	tae := newMemoryArchiveStore()
	liveSegmentID := types.Uuid{7, 8, 9}
	liveObject := objectio.BuildObjectName(&liveSegmentID, 0).String()
	require.NoError(t, tae.Put(ctx, liveObject, []byte("TAE-owned live data")))
	require.NoError(t, archive.Put(
		ctx,
		root.ArchivePrefix+"/payload",
		[]byte("archive"),
	))

	root, err := repository.Transition(
		ctx,
		root.RootID,
		root.AttemptID,
		root.ExecutorEpoch,
		CleanupRootPublished,
		root.StateVersion,
		CleanupRootDeletePending,
	)
	require.NoError(t, err)
	root.CleanupAfter = now
	root, err = repository.UpdateCleanup(ctx, root, root.StateVersion)
	require.NoError(t, err)

	sweeper := CleanupSweeper{
		Roots:            repository,
		Archive:          archive,
		TAE:              tae,
		QuiescenceWindow: time.Second,
	}
	require.NoError(t, sweeper.SweepOne(ctx, root.RootID, now))
	require.Contains(t, tae.keys(), liveObject)
}

func TestCleanupPublishedTemporaryDeletesOnlyRootOwnedBooking(t *testing.T) {
	ctx := context.Background()
	repository := newMemoryCleanupRootRepository()
	root := lifecycleCleanupTestRoot()
	root.Mode = CleanupModeArchiveRewrite
	root.State = CleanupRootPublished
	root.BookingPrefix = "tae/" + root.RootID + "/" + root.AttemptID + "/booking"
	root.SegmentID = ""
	root.OrdinalUpperBound = 0
	require.NoError(t, repository.Register(ctx, root))

	store := newMemoryArchiveStore()
	bookingKey := root.BookingPrefix + "/page-000000"
	require.NoError(t, store.Put(ctx, bookingKey, []byte("booking")))
	require.NoError(t, store.Put(ctx, "tae/unrelated/page", []byte("keep")))

	updated, err := CleanupPublishedTemporary(ctx, repository, store, root)
	require.NoError(t, err)
	require.True(t, updated.TemporaryCleanupDone)
	require.Empty(t, updated.BookingPrefix)
	require.Equal(t, []string{"tae/unrelated/page"}, store.keys())

	persisted, err := repository.Get(ctx, root.RootID)
	require.NoError(t, err)
	require.Equal(t, updated, persisted)
	_, err = CleanupPublishedTemporary(ctx, repository, nil, persisted)
	require.NoError(t, err, "completed cleanup is idempotent and no longer needs a store")
}

func lifecycleCleanupTestRoot() CleanupRoot {
	return CleanupRoot{
		RootID:               "root-test",
		AttemptID:            "attempt-test",
		Mode:                 CleanupModeArchiveWhole,
		ExecutorEpoch:        7,
		ArchivePrefix:        "archive/root-test/attempt-test",
		ReservedCleanupBytes: 1,
		State:                CleanupRootRegistered,
		StateVersion:         1,
		CleanupAfter:         time.Unix(1000, 0),
	}
}

type escapingCleanupStore struct {
	listed  []string
	deleted []string
}

func (store *escapingCleanupStore) List(context.Context, string) ([]string, error) {
	return append([]string(nil), store.listed...), nil
}

func (store *escapingCleanupStore) Delete(_ context.Context, key string) error {
	store.deleted = append(store.deleted, key)
	return nil
}
