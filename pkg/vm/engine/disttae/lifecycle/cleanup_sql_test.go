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
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestSQLCleanupRootRepositoryRegistersInSystemAccount(t *testing.T) {
	root := lifecycleSQLCleanupRoot()
	fake := &scriptedLifecycleSQLExecutor{
		t: t,
		steps: []lifecycleSQLStep{{
			contains:  "insert into mo_catalog.mo_lifecycle_cleanup_roots",
			accountID: 0,
			result:    executor.Result{AffectedRows: 1},
		}},
	}
	repository := SQLCleanupRootRepository{Executor: fake}
	require.NoError(t, repository.Register(context.Background(), root))
	require.Equal(t, 1, fake.offset)

	root.RootID = "not-a-uuid"
	require.Error(t, repository.Register(context.Background(), root))
}

func TestDecodeLifecycleCleanupRootPreservesOwnerIdentity(t *testing.T) {
	mp := mpool.MustNewZero()
	root := lifecycleSQLCleanupRoot()
	root.SegmentID = ""
	root.BookingPrefix = ""
	root.OrdinalUpperBound = 0
	root.TemporaryCleanupDone = true
	result := lifecycleCleanupRootResult(t, mp, root)
	roots, err := decodeLifecycleCleanupRoots(result)
	require.NoError(t, err)
	require.Len(t, roots, 1)
	require.Equal(t, root, roots[0])
}

func TestLifecycleSQLUUIDAndTimestampRoundTrip(t *testing.T) {
	id := uuid.NewString()
	encoded, err := lifecycleSQLUUID(id)
	require.NoError(t, err)
	decoded, err := lifecycleUUIDFromHex(encoded)
	require.NoError(t, err)
	require.Equal(t, id, decoded)

	now := time.Now().UTC().Truncate(time.Microsecond)
	decodedTime, err := lifecycleParseSQLTime(
		now.Format(lifecycleSQLTimestampLayout),
	)
	require.NoError(t, err)
	require.Equal(t, now, decodedTime)
}

func TestSQLCleanupRootRepositoryListsPublishedTemporary(t *testing.T) {
	root := lifecycleSQLCleanupRoot()
	root.State = CleanupRootPublished
	root.TemporaryCleanupDone = false
	mp := mpool.MustNewZero()
	fake := &scriptedLifecycleSQLExecutor{
		t: t,
		steps: []lifecycleSQLStep{{
			contains:  "state='PUBLISHED' and temporary_cleanup_done=false",
			accountID: 0,
			result:    lifecycleCleanupRootResult(t, mp, root),
		}},
	}
	repository := SQLCleanupRootRepository{Executor: fake}
	roots, err := repository.ListPublishedTemporary(context.Background(), 8)
	require.NoError(t, err)
	require.Equal(t, []CleanupRoot{root}, roots)
}

func TestSQLCleanupRootRepositoryGetTransitionUpdateAndSweep(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	root := lifecycleSQLCleanupRoot()
	getFake := &scriptedLifecycleSQLExecutor{
		t: t,
		steps: []lifecycleSQLStep{{
			contains:  "where root_id=unhex",
			accountID: 0,
			result:    lifecycleCleanupRootResult(t, mp, root),
		}},
	}
	got, err := (SQLCleanupRootRepository{Executor: getFake}).Get(ctx, root.RootID)
	require.NoError(t, err)
	require.Equal(t, root, got)
	require.Equal(t, 1, getFake.offset)

	transitionFake := &scriptedLifecycleSQLExecutor{
		t: t,
		steps: []lifecycleSQLStep{
			{
				contains:  "where root_id=unhex",
				accountID: 0,
				result:    lifecycleCleanupRootResult(t, mp, root),
			},
			{
				contains:  "set state='VERIFIED',state_version=state_version+1",
				accountID: 0,
				result:    executor.Result{AffectedRows: 1},
			},
		},
	}
	transitioned, err := (SQLCleanupRootRepository{Executor: transitionFake}).Transition(
		ctx,
		root.RootID,
		root.AttemptID,
		root.ExecutorEpoch,
		CleanupRootUploading,
		root.StateVersion,
		CleanupRootVerified,
	)
	require.NoError(t, err)
	require.Equal(t, CleanupRootVerified, transitioned.State)
	require.Equal(t, root.StateVersion+1, transitioned.StateVersion)
	require.Equal(t, 2, transitionFake.offset)

	updateFake := &scriptedLifecycleSQLExecutor{
		t: t,
		steps: []lifecycleSQLStep{{
			contains:  "update mo_catalog.mo_lifecycle_cleanup_roots set",
			accountID: 0,
			result:    executor.Result{AffectedRows: 1},
		}},
	}
	updated, err := (SQLCleanupRootRepository{Executor: updateFake}).UpdateCleanup(
		ctx,
		transitioned,
		transitioned.StateVersion,
	)
	require.NoError(t, err)
	require.Equal(t, transitioned.StateVersion+1, updated.StateVersion)
	require.Equal(t, 1, updateFake.offset)

	sweepRoot := root
	sweepRoot.State = CleanupRootDeletePending
	sweepFake := &scriptedLifecycleSQLExecutor{
		t: t,
		steps: []lifecycleSQLStep{{
			contains:  "state in ('DELETE_PENDING','DELETING')",
			accountID: 0,
			result:    lifecycleCleanupRootResult(t, mp, sweepRoot),
		}},
	}
	due, err := (SQLCleanupRootRepository{Executor: sweepFake}).ListSweepable(
		ctx,
		time.Now().Add(time.Hour),
		8,
	)
	require.NoError(t, err)
	require.Equal(t, []CleanupRoot{sweepRoot}, due)
	require.Equal(t, 1, sweepFake.offset)
}

func TestSQLCleanupRootRepositoryPagesReconcileableRoots(t *testing.T) {
	root := lifecycleSQLCleanupRoot()
	root.State = CleanupRootCommitUnknown
	mp := mpool.MustNewZero()
	fake := &scriptedLifecycleSQLExecutor{
		t: t,
		steps: []lifecycleSQLStep{{
			contains:  "state in ('registered','uploading','verified','finalizing','commit_unknown','published')",
			accountID: 0,
			result:    lifecycleCleanupRootResult(t, mp, root),
		}},
	}
	repository := SQLCleanupRootRepository{Executor: fake}
	roots, next, wrapped, err := repository.ListReconcileable(
		context.Background(),
		"",
		8,
	)
	require.NoError(t, err)
	require.False(t, wrapped)
	require.Equal(t, []CleanupRoot{root}, roots)
	require.Equal(t, root.RootID, next)
}

func TestSQLCleanupRootRepositoryFindsUnresolvedSource(t *testing.T) {
	root := lifecycleSQLCleanupRoot()
	mp := mpool.MustNewZero()
	fake := &scriptedLifecycleSQLExecutor{
		t: t,
		steps: []lifecycleSQLStep{{
			contains:    "and state in ('finalizing','commit_unknown')",
			notContains: "source_set_digest=",
			accountID:   0,
			result: lifecycleStringResult(
				t,
				mp,
				root.RootID,
			),
		}},
	}
	unresolved, err := (SQLCleanupRootRepository{Executor: fake}).
		HasUnresolvedSource(
			context.Background(),
			root.OwnerAccountID,
			root.PhysicalTableID,
			root.SourceSetDigest,
		)
	require.NoError(t, err)
	require.True(t, unresolved)
}

func TestSQLCleanupRootRepositoryRejectsActiveCapacityAtLimit(t *testing.T) {
	mp := mpool.MustNewZero()
	fake := &scriptedLifecycleSQLExecutor{
		t: t,
		steps: []lifecycleSQLStep{{
			contains: `where state in ('registered','uploading','verified','finalizing',
'commit_unknown','delete_pending','deleting')
or (state='published' and temporary_cleanup_done=false)`,
			accountID: 0,
			result:    lifecycleUint64Result(t, mp, 64, 0),
		}},
	}
	err := (SQLCleanupRootRepository{Executor: fake}).
		CheckCreateCapacity(context.Background(), 64, 1<<30, 1)
	require.ErrorContains(t, err, "Cleanup Root capacity")
	require.Equal(t, 1, fake.offset)
}

func TestSQLCleanupRootRepositoryAcceptsBoundedCapacity(t *testing.T) {
	mp := mpool.MustNewZero()
	fake := &scriptedLifecycleSQLExecutor{
		t: t,
		steps: []lifecycleSQLStep{{
			contains:  "sum(reserved_cleanup_bytes)",
			accountID: 0,
			result:    lifecycleUint64Result(t, mp, 4, 128<<20),
		}},
	}
	require.NoError(t, (SQLCleanupRootRepository{Executor: fake}).
		CheckCreateCapacity(context.Background(), 64, 1<<30, 64<<20))
	require.Equal(t, 1, fake.offset)
}

func TestSQLCleanupReconcileCatalogMatchesArchiveDataset(t *testing.T) {
	root := lifecycleSQLCleanupRoot()
	root.State = CleanupRootCommitUnknown
	mp := mpool.MustNewZero()
	fake := &scriptedLifecycleSQLExecutor{
		t: t,
		steps: []lifecycleSQLStep{{
			contains:  "from mo_catalog.mo_lifecycle_datasets",
			accountID: root.OwnerAccountID,
			result: lifecycleStringResult(
				t,
				mp,
				"PUBLISHED",
			),
		}},
	}
	state, err := (SQLCleanupReconcileCatalog{Executor: fake}).
		MatchingPublication(context.Background(), root, time.Now())
	require.NoError(t, err)
	require.Equal(t, CleanupPublicationPublished, state)
}

func TestSQLCleanupReconcileCatalogMatchesTTLReceipt(t *testing.T) {
	root := lifecycleSQLCleanupRoot()
	root.Mode = CleanupModeTTLRewrite
	mp := mpool.MustNewZero()
	fake := &scriptedLifecycleSQLExecutor{
		t: t,
		steps: []lifecycleSQLStep{{
			contains:  "from mo_catalog.mo_lifecycle_ttl_receipts",
			accountID: root.OwnerAccountID,
			result:    lifecycleStringResult(t, mp, "PUBLISHED"),
		}},
	}
	state, err := (SQLCleanupReconcileCatalog{Executor: fake}).
		MatchingPublication(context.Background(), root, time.Now())
	require.NoError(t, err)
	require.Equal(t, CleanupPublicationPublished, state)
	require.Equal(t, 1, fake.offset)
}

func TestSQLCleanupReconcileCatalogTreatsDroppedAccountAsNoPublication(t *testing.T) {
	root := lifecycleSQLCleanupRoot()
	root.State = CleanupRootCommitUnknown
	mp := mpool.MustNewZero()
	fake := &scriptedLifecycleSQLExecutor{
		t: t,
		steps: []lifecycleSQLStep{
			{
				contains:  "from mo_catalog.mo_lifecycle_datasets",
				accountID: root.OwnerAccountID,
				err:       errors.New("account does not exist"),
			},
			{
				contains:  "from mo_catalog.mo_account",
				accountID: 0,
				result:    executor.Result{Mp: mp},
			},
		},
	}
	state, err := (SQLCleanupReconcileCatalog{Executor: fake}).
		MatchingPublication(context.Background(), root, time.Now())
	require.NoError(t, err)
	require.Equal(t, CleanupPublicationMissing, state)
	require.Equal(t, len(fake.steps), fake.offset)
}

func TestSQLCleanupReconcileCatalogRequestsDueDatasetCleanup(t *testing.T) {
	root := lifecycleSQLCleanupRoot()
	root.State = CleanupRootPublished
	root.TemporaryCleanupDone = true
	mp := mpool.MustNewZero()
	fake := &scriptedLifecycleSQLExecutor{
		t: t,
		steps: []lifecycleSQLStep{
			{
				contains:  "select rel_id from mo_catalog.mo_tables",
				accountID: 0,
				result:    executor.Result{Mp: mp},
			},
			{
				contains:  "from mo_catalog.mo_account",
				accountID: 0,
				result:    lifecycleAccountResult(t, mp, uint64(root.OwnerAccountID)),
			},
			{
				contains:    "and restore_lease_id is null",
				notContains: "%!(extra",
				accountID:   root.OwnerAccountID,
				result: executor.Result{
					AffectedRows: 1,
					Mp:           mp,
				},
			},
		},
	}
	cleanup, err := (SQLCleanupReconcileCatalog{Executor: fake}).
		RequestCleanup(context.Background(), root, time.Now())
	require.NoError(t, err)
	require.True(t, cleanup)
}

func TestSQLCleanupReconcileCatalogRequestsCleanupAfterOwnerAccountDrop(t *testing.T) {
	root := lifecycleSQLCleanupRoot()
	root.State = CleanupRootPublished
	mp := mpool.MustNewZero()
	fake := &scriptedLifecycleSQLExecutor{
		t: t,
		steps: []lifecycleSQLStep{
			{
				contains:  "select rel_id from mo_catalog.mo_tables",
				accountID: 0,
				result:    executor.Result{Mp: mp},
			},
			{
				contains:  "from mo_catalog.mo_account",
				accountID: 0,
				result:    executor.Result{Mp: mp},
			},
		},
	}
	cleanup, err := (SQLCleanupReconcileCatalog{Executor: fake}).RequestCleanup(
		context.Background(),
		root,
		time.Now(),
	)
	require.NoError(t, err)
	require.True(t, cleanup)
	require.Equal(t, len(fake.steps), fake.offset)
}

func TestSQLCleanupReconcileCatalogTTLNeedsNoDatasetMutation(t *testing.T) {
	root := lifecycleSQLCleanupRoot()
	root.Mode = CleanupModeTTLRewrite
	catalogAdapter := SQLCleanupReconcileCatalog{Executor: &scriptedLifecycleSQLExecutor{t: t}}
	cleanup, err := catalogAdapter.RequestCleanup(context.Background(), root, time.Now())
	require.NoError(t, err)
	require.True(t, cleanup)
	require.NoError(t, catalogAdapter.FinalizeCleanup(context.Background(), root))
}

func TestSQLCleanupReconcileCatalogFinalizesDatasetAfterPhysicalCleanup(t *testing.T) {
	root := lifecycleSQLCleanupRoot()
	root.State = CleanupRootDeleting
	mp := mpool.MustNewZero()
	fake := &scriptedLifecycleSQLExecutor{
		t: t,
		steps: []lifecycleSQLStep{
			{
				contains:  "from mo_catalog.mo_account",
				accountID: 0,
				result: lifecycleAccountResult(
					t,
					mp,
					uint64(root.OwnerAccountID),
				),
			},
			{
				contains:  "set state='purged'",
				accountID: root.OwnerAccountID,
				result: executor.Result{
					AffectedRows: 1,
					Mp:           mp,
				},
			},
		},
	}
	require.NoError(t, (SQLCleanupReconcileCatalog{Executor: fake}).
		FinalizeCleanup(context.Background(), root))
	require.Equal(t, len(fake.steps), fake.offset)
}

func TestSQLCleanupReconcileCatalogFinalizesFailedArchiveWithoutDataset(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(2000, 0).UTC()
	root := lifecycleSQLCleanupRoot()
	root.State = CleanupRootDeleting
	root.CleanupAfter = now
	root.QuiescenceSince = now.Add(-time.Minute)
	root.TemporaryCleanupDone = true
	root.SegmentID = ""
	root.BookingPrefix = ""
	root.OrdinalUpperBound = 0

	repository := newMemoryCleanupRootRepository()
	require.NoError(t, repository.Register(ctx, root))
	mp := mpool.MustNewZero()
	fake := &scriptedLifecycleSQLExecutor{
		t: t,
		steps: []lifecycleSQLStep{
			{
				contains:  "from mo_catalog.mo_account",
				accountID: 0,
				result: lifecycleAccountResult(
					t,
					mp,
					uint64(root.OwnerAccountID),
				),
			},
			{
				contains:  "set state='purged'",
				accountID: root.OwnerAccountID,
				result: executor.Result{
					AffectedRows: 0,
					Mp:           mp,
				},
			},
			{
				contains:  "select state from mo_catalog.mo_lifecycle_datasets",
				accountID: root.OwnerAccountID,
				result:    executor.Result{Mp: mp},
			},
		},
	}
	sweeper := CleanupSweeper{
		Roots:            repository,
		Archive:          newMemoryArchiveStore(),
		QuiescenceWindow: time.Second,
		FinalizePublication: (SQLCleanupReconcileCatalog{
			Executor: fake,
		}).FinalizeCleanup,
	}

	require.NoError(t, sweeper.SweepOne(ctx, root.RootID, now))
	current, err := repository.Get(ctx, root.RootID)
	require.NoError(t, err)
	require.Equal(t, CleanupRootCleaned, current.State)
	require.Equal(t, len(fake.steps), fake.offset)
}

func TestSQLCleanupReconcileCatalogRejectsPublishedDatasetAfterPhysicalCleanup(t *testing.T) {
	root := lifecycleSQLCleanupRoot()
	root.State = CleanupRootDeleting
	mp := mpool.MustNewZero()
	fake := &scriptedLifecycleSQLExecutor{
		t: t,
		steps: []lifecycleSQLStep{
			{
				contains:  "from mo_catalog.mo_account",
				accountID: 0,
				result: lifecycleAccountResult(
					t,
					mp,
					uint64(root.OwnerAccountID),
				),
			},
			{
				contains:  "set state='purged'",
				accountID: root.OwnerAccountID,
				result: executor.Result{
					AffectedRows: 0,
					Mp:           mp,
				},
			},
			{
				contains:  "select state from mo_catalog.mo_lifecycle_datasets",
				accountID: root.OwnerAccountID,
				result: lifecycleStringResult(
					t,
					mp,
					"PUBLISHED",
				),
			},
		},
	}

	require.ErrorContains(t, (SQLCleanupReconcileCatalog{Executor: fake}).
		FinalizeCleanup(context.Background(), root), "state \"PUBLISHED\"")
	require.Equal(t, len(fake.steps), fake.offset)
}

func lifecycleStringResult(
	t *testing.T,
	mp *mpool.MPool,
	values ...string,
) executor.Result {
	t.Helper()
	value := batch.NewWithSize(1)
	value.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	for _, item := range values {
		require.NoError(t, vector.AppendBytes(
			value.Vecs[0],
			[]byte(item),
			false,
			mp,
		))
	}
	value.SetRowCount(len(values))
	return executor.Result{Batches: []*batch.Batch{value}, Mp: mp}
}

func lifecycleUint64Result(
	t *testing.T,
	mp *mpool.MPool,
	values ...uint64,
) executor.Result {
	t.Helper()
	bat := batch.NewWithSize(len(values))
	for column, value := range values {
		bat.Vecs[column] = vector.NewVec(types.T_uint64.ToType())
		require.NoError(t, vector.AppendFixed(
			bat.Vecs[column],
			value,
			false,
			mp,
		))
	}
	bat.SetRowCount(1)
	return executor.Result{Batches: []*batch.Batch{bat}, Mp: mp}
}

func lifecycleSQLCleanupRoot() CleanupRoot {
	rootID := "2d55f9be-4d3e-4ac7-a58a-1f7995d88f7f"
	attemptID := "e091026d-114b-44f9-81f3-326bf6481446"
	return CleanupRoot{
		RootID:               rootID,
		AttemptID:            attemptID,
		Mode:                 CleanupModeArchiveRewrite,
		OwnerAccountID:       17,
		LogicalTableID:       42,
		PhysicalTableID:      43,
		ExecutorEpoch:        7,
		WorkerDeadline:       time.Date(2026, 7, 31, 1, 2, 3, 4000, time.UTC),
		ArchiveNamespace:     `{"stage_id":9}`,
		CredentialHandle:     "default",
		ArchivePrefix:        "archive/" + rootID + "/" + attemptID,
		ManifestKey:          "manifest",
		ManifestDigest:       [32]byte{1},
		TAENamespace:         "shared",
		SegmentID:            "segment",
		BookingPrefix:        "booking/" + rootID + "/" + attemptID,
		OrdinalUpperBound:    8,
		ReservedCleanupBytes: 1 << 30,
		SourceSetDigest:      [32]byte{2},
		FinalTxnID:           "txn",
		State:                CleanupRootUploading,
		StateVersion:         3,
		CleanupAfter:         time.Date(2026, 8, 1, 1, 2, 3, 4000, time.UTC),
		QuiescenceSince:      time.Date(2026, 8, 1, 2, 0, 0, 0, time.UTC),
		LastListAt:           time.Date(2026, 8, 1, 2, 1, 0, 0, time.UTC),
		LastError:            "provider timeout",
	}
}

func lifecycleCleanupRootResult(
	t *testing.T,
	mp *mpool.MPool,
	root CleanupRoot,
) executor.Result {
	t.Helper()
	value := batch.NewWithSize(27)
	rootUUID := uuid.MustParse(root.RootID)
	attemptUUID := uuid.MustParse(root.AttemptID)
	strings := map[int]string{
		0:  hex.EncodeToString(rootUUID[:]),
		1:  hex.EncodeToString(attemptUUID[:]),
		2:  string(root.Mode),
		7:  root.WorkerDeadline.Format(lifecycleSQLTimestampLayout),
		8:  root.ArchiveNamespace,
		9:  root.CredentialHandle,
		10: root.ArchivePrefix,
		11: root.ManifestKey,
		12: hex.EncodeToString(root.ManifestDigest[:]),
		13: root.TAENamespace,
		14: root.SegmentID,
		15: root.BookingPrefix,
		18: hex.EncodeToString(root.SourceSetDigest[:]),
		19: root.FinalTxnID,
		20: string(root.State),
		22: root.CleanupAfter.Format(lifecycleSQLTimestampLayout),
		24: root.QuiescenceSince.Format(lifecycleSQLTimestampLayout),
		25: root.LastListAt.Format(lifecycleSQLTimestampLayout),
		26: root.LastError,
	}
	numbers := map[int]uint64{
		3:  uint64(root.OwnerAccountID),
		4:  root.LogicalTableID,
		5:  root.PhysicalTableID,
		6:  root.ExecutorEpoch,
		16: uint64(root.OrdinalUpperBound),
		17: root.ReservedCleanupBytes,
		21: root.StateVersion,
	}
	for column := 0; column < len(value.Vecs); column++ {
		if column == 23 {
			value.Vecs[column] = vector.NewVec(types.T_bool.ToType())
			require.NoError(t, vector.AppendFixed(
				value.Vecs[column],
				root.TemporaryCleanupDone,
				false,
				mp,
			))
			continue
		}
		if number, ok := numbers[column]; ok {
			value.Vecs[column] = vector.NewVec(types.T_uint64.ToType())
			require.NoError(t, vector.AppendFixed(
				value.Vecs[column],
				number,
				false,
				mp,
			))
			continue
		}
		value.Vecs[column] = vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(
			value.Vecs[column],
			[]byte(strings[column]),
			false,
			mp,
		))
	}
	value.SetRowCount(1)
	return executor.Result{Batches: []*batch.Batch{value}, Mp: mp}
}
