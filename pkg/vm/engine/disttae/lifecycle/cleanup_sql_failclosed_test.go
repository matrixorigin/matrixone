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
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestSQLCleanupRootRepositoryFailsClosedOnCatalogErrors(t *testing.T) {
	ctx := context.Background()
	root := lifecycleSQLCleanupRoot()
	failure := errors.New("catalog unavailable")
	errorExecutor := executor.NewMemExecutor(func(string) (executor.Result, error) {
		return executor.Result{}, failure
	})

	require.ErrorContains(t, (SQLCleanupRootRepository{}).Register(ctx, root), "executor is nil")
	require.ErrorIs(t, (SQLCleanupRootRepository{Executor: errorExecutor}).Register(ctx, root), failure)
	require.ErrorContains(t, (SQLCleanupRootRepository{Executor: executor.NewMemExecutor(
		func(string) (executor.Result, error) { return executor.Result{}, nil },
	)}).Register(ctx, root), "affected 0 rows")

	_, err := (SQLCleanupRootRepository{}).Get(ctx, root.RootID)
	require.ErrorContains(t, err, "executor is nil")
	_, err = (SQLCleanupRootRepository{Executor: errorExecutor}).Get(ctx, "invalid")
	require.ErrorContains(t, err, "invalid Lifecycle UUID")
	_, err = (SQLCleanupRootRepository{Executor: errorExecutor}).Get(ctx, root.RootID)
	require.ErrorIs(t, err, failure)
	_, err = (SQLCleanupRootRepository{Executor: executor.NewMemExecutor(
		func(string) (executor.Result, error) { return executor.Result{}, nil },
	)}).Get(ctx, root.RootID)
	require.ErrorContains(t, err, "returned 0 rows")

	_, err = (SQLCleanupRootRepository{}).HasUnresolvedSource(ctx, 0, 0, [32]byte{})
	require.ErrorContains(t, err, "query is incomplete")
	_, err = (SQLCleanupRootRepository{Executor: errorExecutor}).HasUnresolvedSource(
		ctx, root.OwnerAccountID, root.PhysicalTableID, root.SourceSetDigest,
	)
	require.ErrorIs(t, err, failure)

	for _, input := range []struct {
		roots int
		bytes uint64
		need  uint64
	}{{0, 1, 1}, {1, 0, 1}, {1, 1, 0}} {
		err = (SQLCleanupRootRepository{}).CheckCreateCapacity(
			ctx, input.roots, input.bytes, input.need,
		)
		require.ErrorContains(t, err, "capacity check is incomplete")
	}
	err = (SQLCleanupRootRepository{Executor: errorExecutor}).CheckCreateCapacity(ctx, 1, 1, 1)
	require.ErrorIs(t, err, failure)
	_, _, err = decodeCleanupCapacity(executor.Result{})
	require.ErrorContains(t, err, "returned 0 rows")

	_, err = (SQLCleanupRootRepository{}).Transition(
		ctx, root.RootID, root.AttemptID, root.ExecutorEpoch,
		CleanupRootUploading, root.StateVersion, CleanupRootCleaned,
	)
	require.ErrorContains(t, err, "invalid Lifecycle Cleanup Root transition")

	mp := mpool.MustNewZero()
	mismatch := root
	mismatch.StateVersion++
	transitionFake := &scriptedLifecycleSQLExecutor{t: t, steps: []lifecycleSQLStep{{
		contains:  "where root_id=unhex",
		accountID: catalog.System_Account,
		result:    lifecycleCleanupRootResult(t, mp, mismatch),
	}}}
	_, err = (SQLCleanupRootRepository{Executor: transitionFake}).Transition(
		ctx, root.RootID, root.AttemptID, root.ExecutorEpoch,
		root.State, root.StateVersion, CleanupRootVerified,
	)
	require.ErrorContains(t, err, "transition CAS failed")

	_, err = (SQLCleanupRootRepository{}).UpdateCleanup(ctx, root, root.StateVersion)
	require.ErrorContains(t, err, "executor is nil")
	invalidRoot := root
	invalidRoot.RootID = "invalid"
	_, err = (SQLCleanupRootRepository{Executor: errorExecutor}).UpdateCleanup(
		ctx, invalidRoot, root.StateVersion,
	)
	require.ErrorContains(t, err, "invalid Lifecycle UUID")
	invalidRoot = root
	invalidRoot.AttemptID = "invalid"
	_, err = (SQLCleanupRootRepository{Executor: errorExecutor}).UpdateCleanup(
		ctx, invalidRoot, root.StateVersion,
	)
	require.ErrorContains(t, err, "invalid Lifecycle UUID")
	_, err = (SQLCleanupRootRepository{Executor: errorExecutor}).UpdateCleanup(ctx, root, root.StateVersion)
	require.ErrorIs(t, err, failure)
	_, err = (SQLCleanupRootRepository{Executor: executor.NewMemExecutor(
		func(string) (executor.Result, error) { return executor.Result{}, nil },
	)}).UpdateCleanup(ctx, root, root.StateVersion)
	require.ErrorContains(t, err, "update CAS failed")

	_, err = (SQLCleanupRootRepository{}).ListSweepable(ctx, time.Time{}, 0)
	require.ErrorContains(t, err, "query is incomplete")
	_, err = (SQLCleanupRootRepository{Executor: errorExecutor}).ListSweepable(ctx, time.Now(), 1)
	require.ErrorIs(t, err, failure)
	_, err = (SQLCleanupRootRepository{}).ListPublishedTemporary(ctx, 0)
	require.ErrorContains(t, err, "query is incomplete")
	_, err = (SQLCleanupRootRepository{Executor: errorExecutor}).ListPublishedTemporary(ctx, 1)
	require.ErrorIs(t, err, failure)
	_, _, _, err = (SQLCleanupRootRepository{}).ListReconcileable(ctx, "", 0)
	require.ErrorContains(t, err, "query is incomplete")
	_, _, _, err = (SQLCleanupRootRepository{Executor: errorExecutor}).ListReconcileable(ctx, "invalid", 1)
	require.ErrorContains(t, err, "invalid Lifecycle UUID")
	_, _, _, err = (SQLCleanupRootRepository{Executor: errorExecutor}).ListReconcileable(ctx, "", 1)
	require.ErrorIs(t, err, failure)
}

func TestDecodeLifecycleCleanupRootsRejectsCorruptPersistentRows(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *mpool.MPool, executor.Result)
	}{
		{"root-id", func(t *testing.T, mp *mpool.MPool, result executor.Result) {
			require.NoError(t, vector.SetBytesAt(result.Batches[0].Vecs[0], 0, []byte("bad"), mp))
		}},
		{"attempt-id", func(t *testing.T, mp *mpool.MPool, result executor.Result) {
			require.NoError(t, vector.SetBytesAt(result.Batches[0].Vecs[1], 0, []byte("bad"), mp))
		}},
		{"number", func(t *testing.T, mp *mpool.MPool, result executor.Result) {
			result.Batches[0].Vecs[3] = vector.NewVec(types.T_varchar.ToType())
			require.NoError(t, vector.AppendBytes(result.Batches[0].Vecs[3], []byte("17"), false, mp))
		}},
		{"worker-deadline", func(t *testing.T, mp *mpool.MPool, result executor.Result) {
			require.NoError(t, vector.SetBytesAt(result.Batches[0].Vecs[7], 0, []byte("bad"), mp))
		}},
		{"cleanup-after", func(t *testing.T, mp *mpool.MPool, result executor.Result) {
			require.NoError(t, vector.SetBytesAt(result.Batches[0].Vecs[22], 0, []byte("bad"), mp))
		}},
		{"bool", func(t *testing.T, mp *mpool.MPool, result executor.Result) {
			result.Batches[0].Vecs[23] = vector.NewVec(types.T_uint64.ToType())
			require.NoError(t, vector.AppendFixed(result.Batches[0].Vecs[23], uint64(1), false, mp))
		}},
		{"manifest-digest", func(t *testing.T, mp *mpool.MPool, result executor.Result) {
			require.NoError(t, vector.SetBytesAt(result.Batches[0].Vecs[12], 0, []byte("bad"), mp))
		}},
		{"source-digest", func(t *testing.T, mp *mpool.MPool, result executor.Result) {
			require.NoError(t, vector.SetBytesAt(result.Batches[0].Vecs[18], 0, []byte("bad"), mp))
		}},
		{"quiescence", func(t *testing.T, mp *mpool.MPool, result executor.Result) {
			require.NoError(t, vector.SetBytesAt(result.Batches[0].Vecs[24], 0, []byte("bad"), mp))
		}},
		{"last-list", func(t *testing.T, mp *mpool.MPool, result executor.Result) {
			require.NoError(t, vector.SetBytesAt(result.Batches[0].Vecs[25], 0, []byte("bad"), mp))
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			result := lifecycleCleanupRootResult(t, mp, lifecycleSQLCleanupRoot())
			test.mutate(t, mp, result)
			_, err := decodeLifecycleCleanupRoots(result)
			require.Error(t, err)
		})
	}

	mp := mpool.MustNewZero()
	wrongColumns := lifecycleStringResult(t, mp, "value")
	_, err := decodeLifecycleCleanupRoots(wrongColumns)
	require.ErrorContains(t, err, "returned 1 columns")
}

func TestSQLCleanupReconcileCatalogFailsClosedOnAmbiguousPublication(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	root := lifecycleSQLCleanupRoot()
	mp := mpool.MustNewZero()
	failure := errors.New("catalog unavailable")

	_, err := (SQLCleanupReconcileCatalog{}).MatchingPublication(ctx, root, now)
	require.ErrorContains(t, err, "executor is nil")
	invalid := root
	invalid.RootID = "invalid"
	_, err = (SQLCleanupReconcileCatalog{Executor: executor.NewMemExecutor(
		func(string) (executor.Result, error) { return executor.Result{}, nil },
	)}).MatchingPublication(ctx, invalid, now)
	require.ErrorContains(t, err, "invalid Lifecycle UUID")
	invalid = root
	invalid.AttemptID = "invalid"
	_, err = (SQLCleanupReconcileCatalog{Executor: executor.NewMemExecutor(
		func(string) (executor.Result, error) { return executor.Result{}, nil },
	)}).MatchingPublication(ctx, invalid, now)
	require.ErrorContains(t, err, "invalid Lifecycle UUID")
	invalid = root
	invalid.Mode = "UNKNOWN"
	_, err = (SQLCleanupReconcileCatalog{Executor: executor.NewMemExecutor(
		func(string) (executor.Result, error) { return executor.Result{}, nil },
	)}).MatchingPublication(ctx, invalid, now)
	require.ErrorContains(t, err, "unknown Lifecycle")

	for _, state := range []string{"DELETE_PENDING", "DELETING", "PURGED"} {
		fake := &scriptedLifecycleSQLExecutor{t: t, steps: []lifecycleSQLStep{{
			contains:  "from mo_catalog.mo_lifecycle_datasets",
			accountID: root.OwnerAccountID,
			result:    lifecycleStringResult(t, mp, state),
		}}}
		publication, err := (SQLCleanupReconcileCatalog{Executor: fake}).MatchingPublication(ctx, root, now)
		require.NoError(t, err)
		require.Equal(t, CleanupPublicationDeletePending, publication)
	}
	for _, states := range [][]string{{"INVALID"}, {"PUBLISHED", "PURGED"}} {
		fake := &scriptedLifecycleSQLExecutor{t: t, steps: []lifecycleSQLStep{{
			contains:  "from mo_catalog.mo_lifecycle_datasets",
			accountID: root.OwnerAccountID,
			result:    lifecycleStringResult(t, mp, states...),
		}}}
		_, err := (SQLCleanupReconcileCatalog{Executor: fake}).MatchingPublication(ctx, root, now)
		require.ErrorContains(t, err, "invalid state")
	}

	queryAndAccountFailure := &scriptedLifecycleSQLExecutor{t: t, steps: []lifecycleSQLStep{
		{contains: "from mo_catalog.mo_lifecycle_datasets", accountID: root.OwnerAccountID, err: failure},
		{contains: "from mo_catalog.mo_account", accountID: catalog.System_Account, err: failure},
	}}
	_, err = (SQLCleanupReconcileCatalog{Executor: queryAndAccountFailure}).MatchingPublication(ctx, root, now)
	require.ErrorContains(t, err, "account lookup failed")

	queryFailure := &scriptedLifecycleSQLExecutor{t: t, steps: []lifecycleSQLStep{
		{contains: "from mo_catalog.mo_lifecycle_datasets", accountID: root.OwnerAccountID, err: failure},
		{contains: "from mo_catalog.mo_account", accountID: catalog.System_Account, result: lifecycleAccountResult(t, mp, uint64(root.OwnerAccountID))},
	}}
	_, err = (SQLCleanupReconcileCatalog{Executor: queryFailure}).MatchingPublication(ctx, root, now)
	require.ErrorIs(t, err, failure)
}

func TestSQLCleanupReconcileCatalogHandlesRetryAndOwnerFailurePaths(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	root := lifecycleSQLCleanupRoot()
	mp := mpool.MustNewZero()
	failure := errors.New("catalog unavailable")

	_, err := (SQLCleanupReconcileCatalog{}).OwnerExists(ctx, root)
	require.ErrorContains(t, err, "executor is nil")
	_, err = (SQLCleanupReconcileCatalog{Executor: executor.NewMemExecutor(
		func(string) (executor.Result, error) { return executor.Result{}, failure },
	)}).OwnerExists(ctx, root)
	require.ErrorIs(t, err, failure)

	_, err = (SQLCleanupReconcileCatalog{}).RequestCleanup(ctx, root, time.Time{})
	require.ErrorContains(t, err, "request is incomplete")
	ownerFailure := &scriptedLifecycleSQLExecutor{t: t, steps: []lifecycleSQLStep{{
		contains: "select rel_id", accountID: catalog.System_Account, err: failure,
	}}}
	_, err = (SQLCleanupReconcileCatalog{Executor: ownerFailure}).RequestCleanup(ctx, root, now)
	require.ErrorIs(t, err, failure)

	accountFailure := &scriptedLifecycleSQLExecutor{t: t, steps: []lifecycleSQLStep{
		{contains: "select rel_id", accountID: catalog.System_Account, result: executor.Result{Mp: mp}},
		{contains: "from mo_catalog.mo_account", accountID: catalog.System_Account, err: failure},
	}}
	_, err = (SQLCleanupReconcileCatalog{Executor: accountFailure}).RequestCleanup(ctx, root, now)
	require.ErrorIs(t, err, failure)

	for _, affected := range []uint64{0, 2} {
		steps := []lifecycleSQLStep{
			{contains: "select rel_id", accountID: catalog.System_Account, result: lifecycleStringResult(t, mp, "owner")},
			{contains: "from mo_catalog.mo_account", accountID: catalog.System_Account, result: lifecycleAccountResult(t, mp, uint64(root.OwnerAccountID))},
			{contains: "set state='delete_pending'", accountID: root.OwnerAccountID, result: executor.Result{AffectedRows: affected, Mp: mp}},
		}
		if affected == 0 {
			steps = append(steps, lifecycleSQLStep{
				contains: "from mo_catalog.mo_lifecycle_datasets", accountID: root.OwnerAccountID,
				result: lifecycleStringResult(t, mp, "DELETE_PENDING"),
			})
		}
		fake := &scriptedLifecycleSQLExecutor{t: t, steps: steps}
		cleanup, err := (SQLCleanupReconcileCatalog{Executor: fake}).RequestCleanup(ctx, root, now)
		if affected == 0 {
			require.NoError(t, err)
			require.True(t, cleanup)
		} else {
			require.ErrorContains(t, err, "updated 2 rows")
		}
	}

	require.ErrorContains(t, (SQLCleanupReconcileCatalog{}).FinalizeCleanup(ctx, root), "executor is nil")
	accountMissing := &scriptedLifecycleSQLExecutor{t: t, steps: []lifecycleSQLStep{{
		contains: "from mo_catalog.mo_account", accountID: catalog.System_Account, result: executor.Result{Mp: mp},
	}}}
	require.NoError(t, (SQLCleanupReconcileCatalog{Executor: accountMissing}).FinalizeCleanup(ctx, root))

	for _, affected := range []uint64{1, 2} {
		fake := &scriptedLifecycleSQLExecutor{t: t, steps: []lifecycleSQLStep{
			{contains: "from mo_catalog.mo_account", accountID: catalog.System_Account, result: lifecycleAccountResult(t, mp, uint64(root.OwnerAccountID))},
			{contains: "set state='purged'", accountID: root.OwnerAccountID, result: executor.Result{AffectedRows: affected, Mp: mp}},
		}}
		err := (SQLCleanupReconcileCatalog{Executor: fake}).FinalizeCleanup(ctx, root)
		if affected == 1 {
			require.NoError(t, err)
		} else {
			require.ErrorContains(t, err, "finalized 2 rows")
		}
	}

	selectPurged := &scriptedLifecycleSQLExecutor{t: t, steps: []lifecycleSQLStep{
		{contains: "from mo_catalog.mo_account", accountID: catalog.System_Account, result: lifecycleAccountResult(t, mp, uint64(root.OwnerAccountID))},
		{contains: "set state='purged'", accountID: root.OwnerAccountID, result: executor.Result{Mp: mp}},
		{contains: "select state", accountID: root.OwnerAccountID, result: lifecycleStringResult(t, mp, "PURGED")},
	}}
	require.NoError(t, (SQLCleanupReconcileCatalog{Executor: selectPurged}).FinalizeCleanup(ctx, root))

	selectFailure := &scriptedLifecycleSQLExecutor{t: t, steps: []lifecycleSQLStep{
		{contains: "from mo_catalog.mo_account", accountID: catalog.System_Account, result: lifecycleAccountResult(t, mp, uint64(root.OwnerAccountID))},
		{contains: "set state='purged'", accountID: root.OwnerAccountID, result: executor.Result{Mp: mp}},
		{contains: "select state", accountID: root.OwnerAccountID, err: failure},
	}}
	require.ErrorIs(t, (SQLCleanupReconcileCatalog{Executor: selectFailure}).FinalizeCleanup(ctx, root), failure)
}

func TestLifecycleCleanupSQLScalarGuards(t *testing.T) {
	require.Equal(t, "null", lifecycleSQLNullableString(""))
	require.Equal(t, "'a''b'", lifecycleSQLNullableString("a'b"))
	require.Equal(t, "null", lifecycleSQLNullableDigest([32]byte{}))
	require.True(t, strings.HasPrefix(lifecycleSQLNullableDigest([32]byte{1}), "unhex('01"))
	require.Equal(t, "null", lifecycleSQLNullableTime(time.Time{}))
	mp := mpool.MustNewZero()
	nullable := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(nullable, nil, true, mp))
	require.Equal(t, "", lifecycleNullableString(nullable, 0))

	parsed, err := lifecycleParseSQLTime("2026-08-09 10:00:00")
	require.NoError(t, err)
	require.Equal(t, 2026, parsed.Year())
	_, err = lifecycleParseSQLTime("invalid")
	require.ErrorContains(t, err, "invalid Lifecycle SQL timestamp")

	_, err = lifecycleUUIDFromHex("invalid")
	require.ErrorContains(t, err, "invalid persisted Lifecycle UUID")
	var digest [32]byte
	require.NoError(t, lifecycleDecodeDigest("", &digest, true))
	require.Error(t, lifecycleDecodeDigest("", &digest, false))
}

func TestSQLCleanupCatalogCASAndPaginationFailuresRemainRetryable(t *testing.T) {
	ctx := context.Background()
	root := lifecycleSQLCleanupRoot()
	mp := mpool.MustNewZero()
	failure := errors.New("catalog unavailable")

	malformedCapacity := lifecycleStringResult(t, mp, "invalid")
	_, _, err := decodeCleanupCapacity(malformedCapacity)
	require.ErrorContains(t, err, "capacity query is invalid")
	err = (SQLCleanupRootRepository{Executor: executor.NewMemExecutor(
		func(string) (executor.Result, error) { return executor.Result{}, nil },
	)}).CheckCreateCapacity(ctx, 1, 1, 1)
	require.ErrorContains(t, err, "returned 0 rows")

	transitionReadFailure := &scriptedLifecycleSQLExecutor{t: t, steps: []lifecycleSQLStep{{
		contains: "where root_id=unhex", accountID: catalog.System_Account, err: failure,
	}}}
	_, err = (SQLCleanupRootRepository{Executor: transitionReadFailure}).Transition(
		ctx, root.RootID, root.AttemptID, root.ExecutorEpoch,
		root.State, root.StateVersion, CleanupRootVerified,
	)
	require.ErrorIs(t, err, failure)

	for _, affected := range []uint64{0, 1} {
		steps := []lifecycleSQLStep{{
			contains: "where root_id=unhex", accountID: catalog.System_Account,
			result: lifecycleCleanupRootResult(t, mp, root),
		}, {
			contains: "set state='verified'", accountID: catalog.System_Account,
			result: executor.Result{AffectedRows: affected, Mp: mp},
		}}
		if affected == 1 {
			steps[1].result = executor.Result{}
			steps[1].err = failure
		}
		fake := &scriptedLifecycleSQLExecutor{t: t, steps: steps}
		_, err := (SQLCleanupRootRepository{Executor: fake}).Transition(
			ctx, root.RootID, root.AttemptID, root.ExecutorEpoch,
			root.State, root.StateVersion, CleanupRootVerified,
		)
		if affected == 0 {
			require.ErrorContains(t, err, "transition CAS failed")
		} else {
			require.ErrorIs(t, err, failure)
		}
	}

	wrap := &scriptedLifecycleSQLExecutor{t: t, steps: []lifecycleSQLStep{
		{
			contains: "and root_id > unhex", accountID: catalog.System_Account,
			result: executor.Result{Mp: mp},
		},
		{
			notContains: "root_id > unhex", accountID: catalog.System_Account,
			result: lifecycleCleanupRootResult(t, mp, root),
		},
	}}
	roots, next, wrapped, err := (SQLCleanupRootRepository{Executor: wrap}).ListReconcileable(
		ctx, root.RootID, 1,
	)
	require.NoError(t, err)
	require.True(t, wrapped)
	require.Len(t, roots, 1)
	require.Equal(t, root.RootID, next)

	wrapFailure := &scriptedLifecycleSQLExecutor{t: t, steps: []lifecycleSQLStep{
		{
			contains: "and root_id > unhex", accountID: catalog.System_Account,
			result: executor.Result{Mp: mp},
		},
		{accountID: catalog.System_Account, err: failure},
	}}
	_, _, _, err = (SQLCleanupRootRepository{Executor: wrapFailure}).ListReconcileable(
		ctx, root.RootID, 1,
	)
	require.ErrorIs(t, err, failure)

	missingPublication := &scriptedLifecycleSQLExecutor{t: t, steps: []lifecycleSQLStep{{
		contains: "from mo_catalog.mo_lifecycle_datasets", accountID: root.OwnerAccountID,
		result: executor.Result{Mp: mp},
	}}}
	publication, err := (SQLCleanupReconcileCatalog{Executor: missingPublication}).MatchingPublication(
		ctx, root, time.Now(),
	)
	require.NoError(t, err)
	require.Equal(t, CleanupPublicationMissing, publication)

	requestUpdateFailure := &scriptedLifecycleSQLExecutor{t: t, steps: []lifecycleSQLStep{
		{contains: "select rel_id", accountID: catalog.System_Account, result: lifecycleStringResult(t, mp, "owner")},
		{contains: "from mo_catalog.mo_account", accountID: catalog.System_Account, result: lifecycleAccountResult(t, mp, uint64(root.OwnerAccountID))},
		{contains: "set state='delete_pending'", accountID: root.OwnerAccountID, err: failure},
	}}
	_, err = (SQLCleanupReconcileCatalog{Executor: requestUpdateFailure}).RequestCleanup(ctx, root, time.Now())
	require.ErrorIs(t, err, failure)

	finalizeAccountFailure := &scriptedLifecycleSQLExecutor{t: t, steps: []lifecycleSQLStep{{
		contains: "from mo_catalog.mo_account", accountID: catalog.System_Account, err: failure,
	}}}
	require.ErrorIs(t,
		(SQLCleanupReconcileCatalog{Executor: finalizeAccountFailure}).FinalizeCleanup(ctx, root),
		failure,
	)
	finalizeUpdateFailure := &scriptedLifecycleSQLExecutor{t: t, steps: []lifecycleSQLStep{
		{contains: "from mo_catalog.mo_account", accountID: catalog.System_Account, result: lifecycleAccountResult(t, mp, uint64(root.OwnerAccountID))},
		{contains: "set state='purged'", accountID: root.OwnerAccountID, err: failure},
	}}
	require.ErrorIs(t,
		(SQLCleanupReconcileCatalog{Executor: finalizeUpdateFailure}).FinalizeCleanup(ctx, root),
		failure,
	)
}
