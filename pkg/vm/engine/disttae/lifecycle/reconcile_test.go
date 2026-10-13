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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fixedCleanupReconcileCatalog struct {
	publication CleanupPublicationState
	ownerExists bool
	cleanup     bool
}

func (catalog fixedCleanupReconcileCatalog) MatchingPublication(
	context.Context,
	CleanupRoot,
	time.Time,
) (CleanupPublicationState, error) {
	return catalog.publication, nil
}

func (catalog fixedCleanupReconcileCatalog) OwnerExists(
	context.Context,
	CleanupRoot,
) (bool, error) {
	return catalog.ownerExists, nil
}

func (catalog fixedCleanupReconcileCatalog) RequestCleanup(
	context.Context,
	CleanupRoot,
	time.Time,
) (bool, error) {
	return catalog.cleanup, nil
}

func TestCleanupReconcilerAbandonsOnlyPreFinalExpiredAttempts(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	repository := newMemoryCleanupRootRepository()
	root := lifecycleCleanupTestRoot()
	root.WorkerDeadline = now.Add(-time.Minute)
	root.State = CleanupRootUploading
	require.NoError(t, repository.Register(ctx, root))

	reconciler := CleanupReconciler{
		Roots: repository,
		Catalog: fixedCleanupReconcileCatalog{
			ownerExists: true,
		},
	}
	updated, err := reconciler.ReconcileOne(ctx, root, now)
	require.NoError(t, err)
	require.Equal(t, CleanupRootDeletePending, updated.State)
}

func TestCleanupReconcilerTreatsExpiredFinalizingAsUnknown(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	repository := newMemoryCleanupRootRepository()
	root := lifecycleCleanupTestRoot()
	root.WorkerDeadline = now.Add(-time.Minute)
	root.State = CleanupRootFinalizing
	require.NoError(t, repository.Register(ctx, root))

	reconciler := CleanupReconciler{
		Roots: repository,
		Catalog: fixedCleanupReconcileCatalog{
			ownerExists: true,
		},
	}
	updated, err := reconciler.ReconcileOne(ctx, root, now)
	require.NoError(t, err)
	require.Equal(t, CleanupRootCommitUnknown, updated.State)
	require.False(t, CanSweepCleanupRoot(updated))
}

func TestCleanupReconcilerMatchingDatasetTransfersLiveOwnership(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	repository := newMemoryCleanupRootRepository()
	root := lifecycleCleanupTestRoot()
	root.Mode = CleanupModeArchiveRewrite
	root.State = CleanupRootCommitUnknown
	root.SegmentID = "019111fd-aed1-70c0-8760-9abadd8f0f4a"
	root.OrdinalUpperBound = 2
	root.BookingPrefix = "booking/" + root.RootID + "/" + root.AttemptID
	require.NoError(t, repository.Register(ctx, root))

	reconciler := CleanupReconciler{
		Roots: repository,
		Catalog: fixedCleanupReconcileCatalog{
			publication: CleanupPublicationPublished,
			ownerExists: true,
		},
	}
	updated, err := reconciler.ReconcileOne(ctx, root, now)
	require.NoError(t, err)
	require.Equal(t, CleanupRootPublished, updated.State)
	require.Empty(t, updated.SegmentID)
	require.Zero(t, updated.OrdinalUpperBound)
	require.False(t, updated.TemporaryCleanupDone)
}

func TestCleanupReconcilerPublishedPurgeMovesToDeletePending(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	repository := newMemoryCleanupRootRepository()
	root := lifecycleCleanupTestRoot()
	root.State = CleanupRootPublished
	root.TemporaryCleanupDone = true
	require.NoError(t, repository.Register(ctx, root))

	reconciler := CleanupReconciler{
		Roots: repository,
		Catalog: fixedCleanupReconcileCatalog{
			publication: CleanupPublicationDeletePending,
			ownerExists: true,
			cleanup:     true,
		},
	}
	updated, err := reconciler.ReconcileOne(ctx, root, now)
	require.NoError(t, err)
	require.Equal(t, CleanupRootDeletePending, updated.State)
}

func TestCleanupReconcilerUnknownOwnerDropRemainsFailClosed(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	repository := newMemoryCleanupRootRepository()
	root := lifecycleCleanupTestRoot()
	root.State = CleanupRootCommitUnknown
	require.NoError(t, repository.Register(ctx, root))

	reconciler := CleanupReconciler{
		Roots: repository,
		Catalog: fixedCleanupReconcileCatalog{
			publication: CleanupPublicationMissing,
			ownerExists: false,
			cleanup:     true,
		},
	}
	updated, err := reconciler.ReconcileOne(ctx, root, now)
	require.NoError(t, err)
	require.Equal(t, CleanupRootCommitUnknown, updated.State)
	require.False(t, CanSweepCleanupRoot(updated))
}
