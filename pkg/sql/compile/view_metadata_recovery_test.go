// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

type deadlineCheckingSQLExecutor struct {
	t             *testing.T
	expectedError error
}

func (e deadlineCheckingSQLExecutor) Exec(ctx context.Context, _ string, _ executor.Options) (executor.Result, error) {
	_, ok := ctx.Deadline()
	require.True(e.t, ok)
	return executor.Result{}, e.expectedError
}

func (deadlineCheckingSQLExecutor) ExecTxn(
	context.Context,
	func(executor.TxnExecutor) error,
	executor.Options,
) error {
	panic("unexpected ExecTxn")
}

func TestRunViewMetadataRecoveryPageIsBoundedAndFair(t *testing.T) {
	remaining := viewMetadataRecoveryPageSize + 7
	discovered, processed := 0, 0
	discoverOne := func() (bool, error) {
		discovered++
		return true, nil
	}
	recoverOne := func() (bool, error) {
		if remaining == 0 {
			return false, nil
		}
		remaining--
		processed++
		return true, nil
	}
	require.NoError(t, runViewMetadataRecoveryPage(context.Background(), discoverOne, recoverOne))
	require.Equal(t, viewMetadataRecoveryPageSize-1, processed)
	require.Equal(t, 8, remaining)
	require.NoError(t, runViewMetadataRecoveryPage(context.Background(), discoverOne, recoverOne))
	require.Equal(t, viewMetadataRecoveryPageSize+7, processed)
	require.Zero(t, remaining)
	require.Equal(t, 2, discovered)
}

func TestRecoveryCompilerContextRestoresValidatedSnapshot(t *testing.T) {
	snapshot := &planpb.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 123}}
	ctx := &recoveryCompilerContext{
		dependencies:    []plan2.ViewDependency{{SnapshotName: "daily", Snapshot: snapshot}},
		legacySnapshots: map[string]*plan2.Snapshot{"daily": snapshot},
	}
	restored, err := ctx.ResolveSnapshotWithSnapshotName("daily")
	require.NoError(t, err)
	require.Equal(t, snapshot, restored)
	require.NotSame(t, snapshot, restored)
	valid, err := ctx.CheckTimeStampValid(123)
	require.NoError(t, err)
	require.True(t, valid)
	valid, err = ctx.CheckTimeStampValid(124)
	require.NoError(t, err)
	require.False(t, valid)
}

func TestViewDependencyNameKeyHonorsLowerCaseTableNames(t *testing.T) {
	require.NotEqual(t,
		viewDependencyNameKey("Quoted Name", 0),
		viewDependencyNameKey("quoted name", 0))
	require.Equal(t,
		viewDependencyNameKey("Quoted Name", 1),
		viewDependencyNameKey("quoted name", 1))
}

func TestViewBindingNameEqualHonorsLowerCaseTableNames(t *testing.T) {
	require.False(t, viewBindingNameEqual("T", "t", 0))
	require.True(t, viewBindingNameEqual("T", "t", 1))
	require.True(t, viewBindingNameEqual("T", "t", 2))
}

func TestSynchronousViewRefreshCountNeverExceedsBudget(t *testing.T) {
	require.Equal(t, 32, synchronousViewRefreshCount(33, 32))
	require.Zero(t, synchronousViewRefreshCount(1, 0))
	require.Zero(t, synchronousViewRefreshCount(1, -1))
	require.Equal(t, 3, synchronousViewRefreshCount(3, 32))
}

func TestViewMetadataLifecycleSkipsRestoreCatalogDDL(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().IsRestore = true
	c := &Compile{proc: proc}
	require.NoError(t, c.persistViewDependencies(nil, "db", nil))
	require.NoError(t, c.refreshViewsAfterRelationMutation("db", "t", 0, 0))
	require.NoError(t, c.enqueueViewsAfterRelationRemoval("db", "t", 0, 0, 0))
	require.NoError(t, c.deleteDroppedViewMetadata(1))
	require.NoError(t, c.deleteDroppedDatabaseViewMetadata(0, 1, "db"))
}

func TestViewDependencyMutationPredicateNeverTreatsUnknownIDAsIdentity(t *testing.T) {
	predicate := viewDependencyMutationPredicate(viewRelationMutation{
		accountID: 7, databaseID: 11, relationID: 13, logicalID: 17,
		databaseName: "Quoted DB", relationName: "Quoted View",
	}, 0, 0)
	require.Contains(t, predicate, "d.source_relation_id<>0")
	require.Contains(t, predicate, "d.source_logical_id<>0")
	require.Equal(t, 1, strings.Count(predicate, viewDependencyNameKey("Quoted DB", 0)))
	require.Equal(t, 1, strings.Count(predicate, viewDependencyNameKey("quoted db", 1)))
}

func TestNextViewRefreshGenerationFencesOlderCatalogVisibility(t *testing.T) {
	next, ok := nextViewRefreshGeneration(5, 7)
	require.True(t, ok)
	require.Equal(t, uint64(8), next)
	next, ok = nextViewRefreshGeneration(9, 7)
	require.True(t, ok)
	require.Equal(t, uint64(10), next)
	_, ok = nextViewRefreshGeneration(^uint64(0), 0)
	require.False(t, ok)
}

func TestNextLegacyViewScanCursorIsMonotonicAndWraps(t *testing.T) {
	cursor := legacyViewScanCursor{
		accountID: 1, databaseName: "old db", relationName: "old view",
		generation: 7, status: catalog.ViewRefreshStatusLegacyScan,
	}
	next, ok := nextLegacyViewScanCursor(cursor, []legacyViewCandidate{{
		accountID: 2, databaseName: "new db", relationName: "new view",
	}})
	require.True(t, ok)
	require.Equal(t, uint64(8), next.generation)
	require.Equal(t, uint32(2), next.accountID)
	require.Equal(t, "new db", next.databaseName)
	require.Equal(t, "new view", next.relationName)
	require.Equal(t, catalog.ViewRefreshStatusLegacyScan, next.status)

	done, ok := nextLegacyViewScanCursor(next, nil)
	require.True(t, ok)
	require.Equal(t, uint64(9), done.generation)
	require.Equal(t, uint32(0), done.accountID)
	require.Empty(t, done.databaseName)
	require.Empty(t, done.relationName)
	require.Equal(t, catalog.ViewRefreshStatusLegacyScan, done.status)

	cursor.generation = ^uint64(0)
	_, ok = nextLegacyViewScanCursor(cursor, nil)
	require.False(t, ok)
}

func TestRunViewMetadataRecoveryPageStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := runViewMetadataRecoveryPage(ctx, func() (bool, error) {
		calls++
		cancel()
		return true, nil
	}, func() (bool, error) { return true, nil })
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, calls)
}

func TestViewMetadataRecoveryExecutorSetsTransactionDeadline(t *testing.T) {
	expectedError := errors.New("stop after deadline check")
	err := RunViewMetadataRecovery(context.Background(), deadlineCheckingSQLExecutor{
		t:             t,
		expectedError: expectedError,
	}, "worker")
	require.ErrorIs(t, err, expectedError)
}
