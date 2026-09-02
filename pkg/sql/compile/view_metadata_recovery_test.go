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
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

type viewMetadataCleanupRecordingExecutor struct {
	sqls            []string
	systemCTELimits []bool
	results         []executor.Result
	failures        map[int]error
}

func enableViewMetadataRefreshForTest(t *testing.T) {
	t.Helper()
	original := viewMetadataRefreshEnabled
	viewMetadataRefreshEnabled = func(string) bool { return true }
	t.Cleanup(func() { viewMetadataRefreshEnabled = original })
}

func setSynchronousViewRefreshBudgetForTest(t *testing.T, budget int) {
	t.Helper()
	original := viewMetadataSynchronousRefreshBudget
	viewMetadataSynchronousRefreshBudget = budget
	t.Cleanup(func() { viewMetadataSynchronousRefreshBudget = original })
}

func (e *viewMetadataCleanupRecordingExecutor) Exec(
	ctx context.Context,
	sql string,
	_ executor.Options,
) (executor.Result, error) {
	e.sqls = append(e.sqls, sql)
	e.systemCTELimits = append(e.systemCTELimits, process.HasSystemCTELimits(ctx))
	call := len(e.sqls)
	if err := e.failures[call]; err != nil {
		return executor.Result{}, err
	}
	if call <= len(e.results) {
		return e.results[call-1], nil
	}
	return executor.Result{}, nil
}

func (e *viewMetadataCleanupRecordingExecutor) ExecTxn(
	ctx context.Context,
	execFunc func(executor.TxnExecutor) error,
	opts executor.Options,
) error {
	return execFunc(executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		return e.Exec(ctx, sql, opts)
	}, nil))
}

type deadlineCheckingSQLExecutor struct {
	t             *testing.T
	expectedError error
}

func TestRelationRemovalUsesOneAtomicRecursiveClosureInvalidation(t *testing.T) {
	proc := testutil.NewProcess(t)
	exec := &viewMetadataCleanupRecordingExecutor{results: []executor.Result{
		{AffectedRows: viewMetadataClosureWritePageSize}, {AffectedRows: 1},
	}}
	installViewMetadataTestExecutor(t, proc, exec)
	c := &Compile{proc: proc, pn: &planpb.Plan{}}
	root := viewRefreshIdentityKey{accountID: 1, databaseID: 2, logicalID: 999}
	require.NoError(t, c.enqueueDependentViewClosure("root predicate", 77, root))
	require.Len(t, exec.sqls, 2)
	require.Equal(t, []bool{true, true}, exec.systemCTELimits)
	enqueueSQL := exec.sqls[0]
	require.Contains(t, enqueueSQL, "with recursive affected")
	require.Contains(t, enqueueSQL, "root predicate")
	require.Equal(t, 2, strings.Count(enqueueSQL, "d.target_relation_id<>0"))
	require.Contains(t, enqueueSQL, " union select ")
	require.NotContains(t, enqueueSQL, "select distinct")
	require.Contains(t, enqueueSQL, "coalesce(r.target_generation+1,77)")
	require.Contains(t, enqueueSQL,
		"not ((a.account_id=1 and a.target_database_id=2 and "+
			"coalesce(nullif(a.target_logical_id,0),a.target_relation_id)=999))")
	require.Contains(t, enqueueSQL, "limit 16 offset 0")
	require.Contains(t, exec.sqls[1], "limit 16 offset 16")
}

func TestExportedViewMetadataRevalidationSQLReturnsDefensiveCopy(t *testing.T) {
	statements := ViewMetadataRequireRevalidationSQL()
	require.Equal(t, viewMetadataRequireRevalidationSQL(), statements)
	statements[0] = "mutated"
	require.NotEqual(t, statements, ViewMetadataRequireRevalidationSQL())
}

func TestSeedMissingViewMetadataUsesOnlyUserViewsWithoutState(t *testing.T) {
	sql := SeedMissingViewMetadataSQL(77)
	require.Contains(t, sql, "t.relkind='v'")
	require.Contains(t, sql, "r.target_relation_id is null")
	require.Contains(t, sql, "77,0,'DISCOVERING'")
	require.Contains(t, sql, "t.reldatabase not in ('")
}

func TestReconcileAccountViewMetadataRemovesOrphansAndSeedsMissingViews(t *testing.T) {
	sqls := ReconcileAccountViewMetadataSQL(42, 77)
	require.Len(t, sqls, 3)
	for _, sql := range sqls[:2] {
		require.Contains(t, sql, "account_id=42")
		require.Contains(t, sql, "target_relation_id<>0")
		require.Contains(t, sql, "not exists")
		require.Contains(t, sql, "t.relkind='v'")
	}
	require.Contains(t, sqls[2], "where t.account_id=42")
	require.Contains(t, sqls[2], "77,0,'DISCOVERING'")
	require.Contains(t, sqls[2], "r.target_relation_id is null")
}

func TestConflictingRecoveryTargetGetsGenerationFencedBackoff(t *testing.T) {
	proc := testutil.NewProcess(t)
	selected := executor.NewMemResult([]types.Type{
		types.T_uint32.ToType(), types.T_uint64.ToType(), types.T_uint64.ToType(),
	}, proc.Mp())
	selected.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendFixedRows(selected, 0, []uint32{7}))
	require.NoError(t, executor.AppendFixedRows(selected, 1, []uint64{11}))
	require.NoError(t, executor.AppendFixedRows(selected, 2, []uint64{13}))
	exec := &viewMetadataCleanupRecordingExecutor{results: []executor.Result{selected.GetResult(), {}}}
	target, found, err := selectPendingViewMetadataTarget(context.Background(), exec, nil)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint32(7), target.accountID)
	require.Equal(t, uint64(11), target.relationID)
	require.Equal(t, uint64(13), target.generation)
	require.NoError(t, deferConflictingViewMetadata(context.Background(), exec,
		viewMetadataRecoveryCommand{
			AccountID: target.accountID, RelationID: target.relationID, Generation: target.generation,
		}))
	require.Contains(t, exec.sqls[1], "next_retry_at=date_add(now(),interval 2 second)")
	require.Contains(t, exec.sqls[1], "attempts=attempts+1")
	require.Contains(t, exec.sqls[1], "account_id=7 and target_relation_id=11 and target_generation=13")
	require.Contains(t, exec.sqls[1], "status in ('PENDING','DISCOVERING')")
}

func TestRunRecoveryContinuesAfterTransactionConflict(t *testing.T) {
	proc := testutil.NewProcess(t)
	discovery := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, proc.Mp())
	discovery.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendStringRows(discovery, 0, []string{`{"result":0}`}))
	selected := executor.NewMemResult([]types.Type{
		types.T_uint32.ToType(), types.T_uint64.ToType(), types.T_uint64.ToType(),
	}, proc.Mp())
	selected.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendFixedRows(selected, 0, []uint32{7}))
	require.NoError(t, executor.AppendFixedRows(selected, 1, []uint64{11}))
	require.NoError(t, executor.AppendFixedRows(selected, 2, []uint64{13}))
	exec := &viewMetadataCleanupRecordingExecutor{
		results:  []executor.Result{{}, discovery.GetResult(), selected.GetResult(), {}, {}, {}},
		failures: map[int]error{4: moerr.NewTxnNeedRetryWithDefChangedNoCtx()},
	}
	require.NoError(t, RunViewMetadataRecovery(context.Background(), exec, "worker"))
	require.GreaterOrEqual(t, len(exec.sqls), 6)
	require.Contains(t, exec.sqls[4], "next_retry_at=date_add(now(),interval 2 second)")
	require.Contains(t, exec.sqls[4], "target_generation=13")
	// The successful durable backoff counts as progress, so the same page asks
	// for another eligible target instead of stopping behind the conflict.
	require.Contains(t, exec.sqls[5], "order by next_retry_at,attempts")
	require.Contains(t, exec.sqls[5], "not (account_id=7 and target_relation_id=11)")
}

func TestRecoveryPropagationOnlyInvalidatesCurrentDependents(t *testing.T) {
	proc := testutil.NewProcess(t)
	exec := &viewMetadataCleanupRecordingExecutor{}
	installViewMetadataTestExecutor(t, proc, exec)
	c := &Compile{proc: proc, pn: &planpb.Plan{}}
	require.NoError(t, c.enqueueCurrentDependentViews(viewRelationMutation{
		accountID: 1, databaseID: 2, relationID: 3, logicalID: 4,
		databaseName: "db", relationName: "v",
	}))
	require.Len(t, exec.sqls, 1)
	require.Contains(t, exec.sqls[0], "coalesce(r.target_generation+1,d.dependency_generation)")
	require.Contains(t, exec.sqls[0], "r.target_relation_id is null or r.status='CURRENT'")
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
	proc := testutil.NewProcess(t)
	liveSnapshot := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, proc.Mp())
	liveSnapshot.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendStringRows(liveSnapshot, 0, []string{"snapshot-id"}))
	exec := &viewMetadataCleanupRecordingExecutor{results: []executor.Result{
		liveSnapshot.GetResult(), {},
	}}
	installViewMetadataTestExecutor(t, proc, exec)
	snapshot := &planpb.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 123}}
	ctx := &recoveryCompilerContext{
		compilerContext: &compilerContext{ctx: proc.Ctx, proc: proc},
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
	require.Equal(t, []string{
		"select snapshot_id from mo_catalog.mo_snapshots where ts=123 limit 1",
		"select snapshot_id from mo_catalog.mo_snapshots where ts=124 limit 1",
	}, exec.sqls)
}

func TestRecoveryCompilerContextRejectsStalePersistedTimestamp(t *testing.T) {
	proc := testutil.NewProcess(t)
	exec := &viewMetadataCleanupRecordingExecutor{}
	installViewMetadataTestExecutor(t, proc, exec)
	ctx := &recoveryCompilerContext{
		compilerContext: &compilerContext{ctx: proc.Ctx, proc: proc},
		dependencies: []plan2.ViewDependency{{
			Snapshot: &planpb.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 456}},
		}},
	}

	valid, err := ctx.CheckTimeStampValid(456)
	require.NoError(t, err)
	require.False(t, valid)
	require.Equal(t,
		[]string{"select snapshot_id from mo_catalog.mo_snapshots where ts=456 limit 1"}, exec.sqls)
}

func TestRecoveryCompilerContextAcceptsLiveTimestampWithoutPersistedDependency(t *testing.T) {
	proc := testutil.NewProcess(t)
	liveSnapshot := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, proc.Mp())
	liveSnapshot.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendStringRows(liveSnapshot, 0, []string{"snapshot-id"}))
	exec := &viewMetadataCleanupRecordingExecutor{results: []executor.Result{liveSnapshot.GetResult()}}
	installViewMetadataTestExecutor(t, proc, exec)
	ctx := &recoveryCompilerContext{
		compilerContext: &compilerContext{ctx: proc.Ctx, proc: proc},
	}

	valid, err := ctx.CheckTimeStampValid(456)
	require.NoError(t, err)
	require.True(t, valid)
	require.Equal(t,
		[]string{"select snapshot_id from mo_catalog.mo_snapshots where ts=456 limit 1"}, exec.sqls)
}

func TestRecoveryCompilerContextPropagatesTimestampCatalogError(t *testing.T) {
	proc := testutil.NewProcess(t)
	expected := errors.New("snapshot catalog unavailable")
	exec := &viewMetadataCleanupRecordingExecutor{failures: map[int]error{1: expected}}
	installViewMetadataTestExecutor(t, proc, exec)
	ctx := &recoveryCompilerContext{
		compilerContext: &compilerContext{ctx: proc.Ctx, proc: proc},
	}

	valid, err := ctx.CheckTimeStampValid(456)
	require.False(t, valid)
	require.ErrorIs(t, err, expected)
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
	require.Zero(t, viewMetadataSynchronousRefreshBudget)
	require.Equal(t, 8, synchronousViewRefreshCount(9, 8))
	require.Zero(t, synchronousViewRefreshCount(1, 0))
	require.Zero(t, synchronousViewRefreshCount(1, -1))
	require.Equal(t, 3, synchronousViewRefreshCount(3, 8))
}

func TestPersistViewDependenciesCoversCatalogWriteModes(t *testing.T) {
	viewData, err := json.Marshal(plan2.ViewData{Dependencies: []plan2.ViewDependency{
		{
			AccountID: 1, DatabaseID: 2, RelationID: 3, LogicalID: 4,
			DatabaseName: "source db", RelationName: "source table", RelationKind: "r",
			LowerCaseTableNames: 1,
		},
		{
			AccountID: 5, DatabaseID: 6, RelationID: 7, LogicalID: 8,
			DatabaseName: "publisher db", RelationName: "publisher table", RelationKind: "r",
			SubscriptionName: "subscription", PublisherAccount: 5,
			Snapshot: &planpb.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 123}},
		},
	}})
	require.NoError(t, err)
	viewDef := &planpb.TableDef{Name: "target view", ViewSql: &planpb.ViewDef{View: string(viewData)}}

	for _, createState := range []bool{true, false} {
		t.Run(fmt.Sprintf("create state %t", createState), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			proc := testutil.NewProcess(t)
			exec := &viewMetadataCleanupRecordingExecutor{}
			if !createState {
				exec.results = []executor.Result{{}, {}, {}, {}, {AffectedRows: 1}}
			}
			installViewMetadataTestExecutor(t, proc, exec)
			relation := mock_frontend.NewMockRelation(ctrl)
			relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(13))
			relation.EXPECT().GetTableDef(gomock.Any()).Return(&planpb.TableDef{LogicalId: 17})
			database := mock_frontend.NewMockDatabase(ctrl)
			database.EXPECT().Relation(gomock.Any(), "target view", gomock.Any()).Return(relation, nil)
			database.EXPECT().GetDatabaseId(gomock.Any()).Return("11")
			c := &Compile{proc: proc, pn: &planpb.Plan{}}

			require.NoError(t, c.persistViewDependenciesWithContext(
				proc.Ctx, database, "target db", viewDef, 19, createState))
			require.Len(t, exec.sqls, 5)
			require.Contains(t, exec.sqls[0], "delete from mo_catalog.mo_view_dependencies")
			require.Contains(t, exec.sqls[1], "source db")
			require.Contains(t, exec.sqls[2], "subscription")
			require.Contains(t, exec.sqls[3], "delete from mo_catalog.mo_view_refresh")
			if createState {
				require.Contains(t, exec.sqls[4], "replace into mo_catalog.mo_view_refresh")
			} else {
				require.Contains(t, exec.sqls[4], "update mo_catalog.mo_view_refresh")
			}
		})
	}
}

func TestPersistViewDependenciesRejectsStaleOrMalformedCatalogState(t *testing.T) {
	t.Run("missing definition", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		_, err := (&Compile{proc: proc, pn: &planpb.Plan{}}).
			persistViewDependencyEdgesWithContext(proc.Ctx, nil, "db", nil, 1)
		require.Error(t, err)
	})

	t.Run("relation lookup", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
		ctrl := gomock.NewController(t)
		database := mock_frontend.NewMockDatabase(ctrl)
		expected := errors.New("relation unavailable")
		database.EXPECT().Relation(gomock.Any(), "view", gomock.Any()).Return(nil, expected)
		_, err := (&Compile{proc: proc, pn: &planpb.Plan{}}).persistViewDependencyEdgesWithContext(
			proc.Ctx, database, "db", &planpb.TableDef{Name: "view", ViewSql: &planpb.ViewDef{}}, 1)
		require.ErrorIs(t, err, expected)
	})

	t.Run("database identity", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
		ctrl := gomock.NewController(t)
		database := mock_frontend.NewMockDatabase(ctrl)
		relation := mock_frontend.NewMockRelation(ctrl)
		database.EXPECT().Relation(gomock.Any(), "view", gomock.Any()).Return(relation, nil)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(13))
		relation.EXPECT().GetTableDef(gomock.Any()).Return(&planpb.TableDef{})
		database.EXPECT().GetDatabaseId(gomock.Any()).Return("invalid")
		_, err := (&Compile{proc: proc, pn: &planpb.Plan{}}).persistViewDependencyEdgesWithContext(
			proc.Ctx, database, "db", &planpb.TableDef{Name: "view", ViewSql: &planpb.ViewDef{}}, 1)
		require.Error(t, err)
	})

	t.Run("persisted view JSON", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
		ctrl := gomock.NewController(t)
		database := mock_frontend.NewMockDatabase(ctrl)
		relation := mock_frontend.NewMockRelation(ctrl)
		database.EXPECT().Relation(gomock.Any(), "view", gomock.Any()).Return(relation, nil)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(13))
		relation.EXPECT().GetTableDef(gomock.Any()).Return(&planpb.TableDef{LogicalId: 17})
		database.EXPECT().GetDatabaseId(gomock.Any()).Return("11")
		_, err := (&Compile{proc: proc, pn: &planpb.Plan{}}).persistViewDependencyEdgesWithContext(
			proc.Ctx, database, "db", &planpb.TableDef{
				Name: "view", ViewSql: &planpb.ViewDef{View: "{"},
			}, 1)
		require.Error(t, err)
	})
}

func TestLoadDependentViewsDecodesCatalogRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	result := executor.NewMemResult([]types.Type{
		types.T_uint32.ToType(), types.T_uint64.ToType(), types.T_uint64.ToType(),
		types.T_uint64.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(),
		types.T_uint64.ToType(),
	}, proc.Mp())
	result.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendFixedRows(result, 0, []uint32{7}))
	require.NoError(t, executor.AppendFixedRows(result, 1, []uint64{11}))
	require.NoError(t, executor.AppendFixedRows(result, 2, []uint64{13}))
	require.NoError(t, executor.AppendFixedRows(result, 3, []uint64{17}))
	require.NoError(t, executor.AppendStringRows(result, 4, []string{"target_db"}))
	require.NoError(t, executor.AppendStringRows(result, 5, []string{"target_view"}))
	require.NoError(t, executor.AppendFixedRows(result, 6, []uint64{19}))

	exec := &viewMetadataCleanupRecordingExecutor{results: []executor.Result{result.GetResult()}}
	installViewMetadataTestExecutor(t, proc, exec)
	c := &Compile{proc: proc, pn: &planpb.Plan{}}
	targets, err := c.loadDependentViews(viewRelationMutation{
		accountID: 7, databaseID: 23, relationID: 29, logicalID: 31,
		databaseName: "source_db", relationName: "source_table",
	}, 37, 41)
	require.NoError(t, err)
	require.Equal(t, []viewRefreshTarget{{
		accountID: 7, databaseID: 11, relationID: 13, logicalID: 17,
		databaseName: "target_db", relationName: "target_view", generation: 19,
	}}, targets)
	require.Len(t, exec.sqls, 1)
	require.Contains(t, exec.sqls[0], "d.source_account_id=7")
	require.Contains(t, exec.sqls[0], "d.source_relation_id in (29,37)")
	require.Contains(t, exec.sqls[0], "d.source_logical_id in (31,41)")
}

func TestLoadViewCatalogOwnershipHandlesPresentAndMissingRows(t *testing.T) {
	t.Run("present", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		result := executor.NewMemResult([]types.Type{
			types.T_uint32.ToType(), types.T_uint32.ToType(), types.T_timestamp.ToType(),
		}, proc.Mp())
		result.NewBatchWithRowCount(1)
		require.NoError(t, executor.AppendFixedRows(result, 0, []uint32{3}))
		require.NoError(t, executor.AppendFixedRows(result, 1, []uint32{5}))
		require.NoError(t, executor.AppendFixedRows(result, 2, []types.Timestamp{7}))
		exec := &viewMetadataCleanupRecordingExecutor{results: []executor.Result{result.GetResult()}}
		installViewMetadataTestExecutor(t, proc, exec)
		ownership, err := (&Compile{proc: proc, pn: &planpb.Plan{}}).loadViewCatalogOwnership(11, 13)
		require.NoError(t, err)
		require.Equal(t, viewCatalogOwnership{creator: 3, owner: 5, createdTime: 7}, ownership)
		require.Contains(t, exec.sqls[0], "account_id=11 and rel_id=13")
	})

	t.Run("missing", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		installViewMetadataTestExecutor(t, proc, &viewMetadataCleanupRecordingExecutor{})
		_, err := (&Compile{proc: proc, pn: &planpb.Plan{}}).loadViewCatalogOwnership(11, 13)
		var changed *viewRefreshIdentityChangedError
		require.ErrorAs(t, err, &changed)
	})
}

func TestEnabledViewMetadataCommandRoutesLifecycleOperations(t *testing.T) {
	for _, tc := range []struct {
		name      string
		parameter string
		results   []executor.Result
		count     int
		contains  string
	}{
		{
			name: "discover", parameter: `{"discover":true}`,
			results: []executor.Result{{}, {}, {}}, count: 1,
			contains: "'LEGACY_SCAN'",
		},
		{
			name: "revalidate", parameter: `{"revalidate":true}`,
			results: []executor.Result{{}, {AffectedRows: 1}}, count: 1,
			contains: "source_relation_kind='REVALIDATE_SCAN'",
		},
		{
			name: "recover target", parameter: `{"worker_id":"worker","account_id":1,"relation_id":2,"generation":3}`,
			results: []executor.Result{{}, {}}, count: 0,
			contains: "and account_id=1 and target_relation_id=2 and target_generation=3",
		},
		{
			name: "legacy worker parameter", parameter: "worker",
			results: []executor.Result{{}, {}}, count: 0,
			contains: "status in ('PENDING','DISCOVERING')",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			enableViewMetadataRefreshForTest(t)
			proc := testutil.NewProcess(t)
			exec := &viewMetadataCleanupRecordingExecutor{results: tc.results}
			installViewMetadataTestExecutor(t, proc, exec)
			count, err := recoverViewMetadataCommand(proc, tc.parameter)
			require.NoError(t, err)
			require.Equal(t, tc.count, count)
			require.NotEmpty(t, exec.sqls)
			require.Equal(t, catalog.ViewMetadataLifecycleGateSQL, exec.sqls[0])
			require.Contains(t, strings.Join(exec.sqls, "\n"), tc.contains)
		})
	}
}

func TestEnabledRelationMutationDurablyEnqueuesClosure(t *testing.T) {
	enableViewMetadataRefreshForTest(t)
	setSynchronousViewRefreshBudgetForTest(t, 1)
	proc := testutil.NewProcess(t)
	proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
	exec := &viewMetadataCleanupRecordingExecutor{}
	installViewMetadataTestExecutor(t, proc, exec)
	ctrl := gomock.NewController(t)
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().SnapshotTS().Return(timestamp.Timestamp{PhysicalTime: 31})
	proc.Base.TxnOperator = txnOperator
	engine := mock_frontend.NewMockEngine(ctrl)
	database := mock_frontend.NewMockDatabase(ctrl)
	relation := mock_frontend.NewMockRelation(ctrl)
	engine.EXPECT().Database(gomock.Any(), "source_db", gomock.Any()).Return(database, nil)
	database.EXPECT().Relation(gomock.Any(), "source_table", gomock.Any()).Return(relation, nil)
	database.EXPECT().GetDatabaseId(gomock.Any()).Return("11")
	relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(13))
	relation.EXPECT().GetTableDef(gomock.Any()).Return(&planpb.TableDef{LogicalId: 17})

	c := &Compile{proc: proc, e: engine, pn: &planpb.Plan{}}
	require.NoError(t, c.refreshViewsAfterRelationMutation("source_db", "source_table", 19, 23))
	require.Len(t, exec.sqls, 2)
	require.Contains(t, exec.sqls[0], "d.source_account_id=7")
	require.Contains(t, exec.sqls[0], "d.source_relation_id in (13,19)")
	require.Contains(t, exec.sqls[0], "d.source_logical_id in (17,23)")
	require.Contains(t, exec.sqls[1], "order by d.account_id,d.target_database_id")
}

func TestEnabledRelationMutationBoundsRetryingSynchronousRefresh(t *testing.T) {
	enableViewMetadataRefreshForTest(t)
	setSynchronousViewRefreshBudgetForTest(t, 1)
	lockStub := gostub.Stub(&lockMoTable,
		func(_ *Compile, _, _ string, _ lock.LockMode) error { return nil })
	t.Cleanup(lockStub.Reset)
	proc := testutil.NewProcess(t)
	proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
	targets := executor.NewMemResult([]types.Type{
		types.T_uint32.ToType(), types.T_uint64.ToType(), types.T_uint64.ToType(),
		types.T_uint64.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(),
		types.T_uint64.ToType(),
	}, proc.Mp())
	targets.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendFixedRows(targets, 0, []uint32{7}))
	require.NoError(t, executor.AppendFixedRows(targets, 1, []uint64{31}))
	require.NoError(t, executor.AppendFixedRows(targets, 2, []uint64{37}))
	require.NoError(t, executor.AppendFixedRows(targets, 3, []uint64{41}))
	require.NoError(t, executor.AppendStringRows(targets, 4, []string{"target_db"}))
	require.NoError(t, executor.AppendStringRows(targets, 5, []string{"target_view"}))
	require.NoError(t, executor.AppendFixedRows(targets, 6, []uint64{43}))
	exec := &viewMetadataCleanupRecordingExecutor{results: []executor.Result{
		{}, targets.GetResult(), {},
	}}
	installViewMetadataTestExecutor(t, proc, exec)
	ctrl := gomock.NewController(t)
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().SnapshotTS().Return(timestamp.Timestamp{PhysicalTime: 47})
	proc.Base.TxnOperator = txnOperator
	storage := mock_frontend.NewMockEngine(ctrl)
	sourceDatabase := mock_frontend.NewMockDatabase(ctrl)
	sourceRelation := mock_frontend.NewMockRelation(ctrl)
	storage.EXPECT().Database(gomock.Any(), "source_db", gomock.Any()).Return(sourceDatabase, nil)
	sourceDatabase.EXPECT().Relation(gomock.Any(), "source_table", gomock.Any()).Return(sourceRelation, nil)
	sourceDatabase.EXPECT().GetDatabaseId(gomock.Any()).Return("11")
	sourceRelation.EXPECT().GetTableID(gomock.Any()).Return(uint64(13))
	sourceRelation.EXPECT().GetTableDef(gomock.Any()).Return(&planpb.TableDef{LogicalId: 17})
	storage.EXPECT().Database(gomock.Any(), "target_db", gomock.Any()).
		Return(nil, errors.New("target temporarily unavailable"))

	c := &Compile{proc: proc, e: storage, pn: &planpb.Plan{}}
	require.NoError(t, c.refreshViewsAfterRelationMutation("source_db", "source_table", 19, 23))
	require.Len(t, exec.sqls, 3)
	require.Contains(t, exec.sqls[2], "d.source_account_id=7")
}

func TestEnabledLifecycleRemovalAndCleanupPaths(t *testing.T) {
	enableViewMetadataRefreshForTest(t)
	proc := testutil.NewProcess(t)
	proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
	ctrl := gomock.NewController(t)
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().SnapshotTS().Return(timestamp.Timestamp{PhysicalTime: 31})
	proc.Base.TxnOperator = txnOperator
	exec := &viewMetadataCleanupRecordingExecutor{}
	installViewMetadataTestExecutor(t, proc, exec)
	c := &Compile{proc: proc, pn: &planpb.Plan{}}

	require.NoError(t, c.enqueueViewsAfterRelationRemoval("source_db", "source_table", 11, 13, 17))
	require.Len(t, exec.sqls, 1)
	require.Contains(t, exec.sqls[0], "d.source_account_id=7")
	require.Contains(t, exec.sqls[0], "not ((a.account_id=7 and a.target_database_id=11")

	exec.sqls = nil
	require.NoError(t, c.deleteDroppedViewMetadata("source_db", 13))
	require.Len(t, exec.sqls, 3)
	require.Equal(t, catalog.ViewMetadataLifecycleGateSQL, exec.sqls[0])
	require.Contains(t, exec.sqls[1], "account_id=7 and target_relation_id=13")
	require.Contains(t, exec.sqls[2], "account_id=7 and target_relation_id=13")

	exec.sqls = nil
	require.NoError(t, c.deleteDroppedDatabaseViewMetadata(7, 11, "source'db"))
	require.Len(t, exec.sqls, 3)
	require.Equal(t, catalog.ViewMetadataLifecycleGateSQL, exec.sqls[0])
	require.Contains(t, exec.sqls[1], "target_database_id=11 or target_database_name='source''db'")
	require.Contains(t, exec.sqls[2], "target_database_id=11 or target_database_name='source''db'")

	exec.sqls = nil
	require.NoError(t, c.enqueueViewsAfterDatabaseRemoval("source_db", 7, 11, 31))
	require.Len(t, exec.sqls, 1)
	require.Contains(t, exec.sqls[0], "d.source_account_id=7 and d.source_database_id=11")
}

func TestRecoveryCompilerContextCatalogAndBindingAdapters(t *testing.T) {
	t.Run("subscription catalog result is cached", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
		result := executor.NewMemResult([]types.Type{
			types.T_int32.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(),
			types.T_varchar.ToType(), types.T_varchar.ToType(),
		}, proc.Mp())
		result.NewBatchWithRowCount(1)
		require.NoError(t, executor.AppendFixedRows(result, 0, []int32{11}))
		require.NoError(t, executor.AppendStringRows(result, 1, []string{"publisher"}))
		require.NoError(t, executor.AppendStringRows(result, 2, []string{"publication"}))
		require.NoError(t, executor.AppendStringRows(result, 3, []string{"published_db"}))
		require.NoError(t, executor.AppendStringRows(result, 4, []string{"table_a,table_b"}))
		exec := &viewMetadataCleanupRecordingExecutor{results: []executor.Result{result.GetResult()}}
		installViewMetadataTestExecutor(t, proc, exec)
		ctx := &recoveryCompilerContext{compilerContext: &compilerContext{
			ctx: proc.Ctx, proc: proc, lower: 1,
		}}
		meta, err := ctx.GetSubscriptionMeta("SUB_DB", nil)
		require.NoError(t, err)
		require.Equal(t, int32(11), meta.AccountId)
		require.Equal(t, "published_db", meta.DbName)
		require.Equal(t, "SUB_DB", meta.SubName)
		cached, err := ctx.GetSubscriptionMeta("sub_db", nil)
		require.NoError(t, err)
		require.Same(t, meta, cached)
		require.Len(t, exec.sqls, 1)
	})

	t.Run("dependency binding uses physical account", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
		ctrl := gomock.NewController(t)
		storage := mock_frontend.NewMockEngine(ctrl)
		expected := errors.New("physical catalog unavailable")
		storage.EXPECT().Database(gomock.Any(), "physical_db", gomock.Any()).Return(nil, expected)
		ctx := &recoveryCompilerContext{
			compilerContext: &compilerContext{ctx: proc.Ctx, proc: proc, engine: storage},
			dependencies: []plan2.ViewDependency{{
				AccountID: 11, DatabaseName: "physical_db", RelationName: "physical_table",
				BindingDatabaseName: "binding_db", BindingRelationName: "binding_table",
				LowerCaseTableNames: 1, RelationID: 13,
			}},
		}
		require.False(t, ctx.DatabaseExists("BINDING_DB", nil))
		storage.EXPECT().Database(gomock.Any(), "physical_db", gomock.Any()).Return(nil, expected)
		_, _, err := ctx.Resolve("BINDING_DB", "BINDING_TABLE", nil)
		require.ErrorIs(t, err, expected)
		storage.EXPECT().Database(gomock.Any(), "physical_db", gomock.Any()).Return(nil, expected)
		_, _, err = ctx.ResolveById(13, nil)
		require.ErrorIs(t, err, expected)
	})

	t.Run("fallback adapters restore top context", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
		ctrl := gomock.NewController(t)
		storage := mock_frontend.NewMockEngine(ctrl)
		expected := errors.New("catalog unavailable")
		storage.EXPECT().Database(gomock.Any(), "db", gomock.Any()).Return(nil, expected)
		ctx := &recoveryCompilerContext{
			compilerContext:          &compilerContext{ctx: proc.Ctx, proc: proc, engine: storage},
			legacySubscriptionLooked: map[string]struct{}{"db": {}},
			legacySubscriptions:      map[string]*planpb.SubscriptionMeta{},
		}
		originalTop := proc.GetTopContext()
		require.False(t, ctx.DatabaseExists("db", nil))
		storage.EXPECT().Database(gomock.Any(), "db", gomock.Any()).Return(nil, expected)
		_, _, err := ctx.Resolve("db", "table", nil)
		require.ErrorIs(t, err, expected)
		require.Equal(t, originalTop, proc.GetTopContext())
		storage.EXPECT().GetNameById(gomock.Any(), gomock.Any(), uint64(19)).Return("", "", expected)
		_, _, err = ctx.ResolveById(19, nil)
		require.ErrorIs(t, err, expected)
	})

	t.Run("stale subscription binding is unavailable", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
		ctx := &recoveryCompilerContext{
			compilerContext: &compilerContext{ctx: proc.Ctx, proc: proc},
			dependencies: []plan2.ViewDependency{{
				AccountID: 11, DatabaseName: "published_db", RelationName: "table",
				SubscriptionName: "sub", RelationID: 13,
			}},
			legacySubscriptionLooked: map[string]struct{}{"sub": {}},
			legacySubscriptions: map[string]*planpb.SubscriptionMeta{
				"sub": {AccountId: 99, DbName: "published_db", SubName: "sub", Tables: "table"},
			},
		}
		_, _, err := ctx.Resolve("sub", "table", nil)
		var unavailable *viewRefreshDependencyUnavailableError
		require.ErrorAs(t, err, &unavailable)
	})
}

func TestUpdateViewRefreshFailurePersistsRetryAndTerminalState(t *testing.T) {
	proc := testutil.NewProcess(t)
	pending := &pendingViewRefresh{viewRefreshTarget: viewRefreshTarget{
		accountID: 7, relationID: 11, generation: 13,
	}, leaseEpoch: 9}
	for _, tc := range []struct {
		name   string
		retry  bool
		status string
		want   string
	}{
		{name: "retry", retry: true, status: viewRefreshStatusPending, want: "interval 256 second"},
		{name: "terminal", status: viewRefreshStatusInvalid, want: "next_retry_at=null"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec := &viewMetadataCleanupRecordingExecutor{}
			require.NoError(t, updateViewRefreshFailure(proc, exec, executor.Options{}, pending,
				tc.status, viewRefreshFailureInfrastructure, tc.retry))
			require.Len(t, exec.sqls, 1)
			require.Contains(t, exec.sqls[0], tc.want)
			require.Contains(t, exec.sqls[0], "target_generation=13 and lease_epoch=9")
		})
	}

	t.Run("catalog error", func(t *testing.T) {
		expected := errors.New("catalog unavailable")
		exec := &viewMetadataCleanupRecordingExecutor{failures: map[int]error{1: expected}}
		err := updateViewRefreshFailure(proc, exec, executor.Options{}, pending,
			viewRefreshStatusPending, viewRefreshFailureInfrastructure, true)
		require.ErrorIs(t, err, expected)
	})
}

func TestRefreshOneViewRejectsStaleCatalogTargetsBeforeReplacement(t *testing.T) {
	target := viewRefreshTarget{
		accountID: 7, databaseID: 11, relationID: 13, logicalID: 17,
		databaseName: "db", relationName: "view", generation: 19,
	}
	source := viewRelationMutation{}

	t.Run("database unavailable", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		ctrl := gomock.NewController(t)
		storage := mock_frontend.NewMockEngine(ctrl)
		expected := errors.New("database unavailable")
		storage.EXPECT().Database(gomock.Any(), "db", gomock.Any()).Return(nil, expected)
		err := (&Compile{proc: proc, e: storage, pn: &planpb.Plan{}}).refreshOneView(target, source)
		require.ErrorIs(t, err, expected)
	})

	t.Run("relation unavailable", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		ctrl := gomock.NewController(t)
		storage := mock_frontend.NewMockEngine(ctrl)
		database := mock_frontend.NewMockDatabase(ctrl)
		expected := errors.New("relation unavailable")
		storage.EXPECT().Database(gomock.Any(), "db", gomock.Any()).Return(database, nil)
		database.EXPECT().Relation(gomock.Any(), "view", gomock.Any()).Return(nil, expected)
		err := (&Compile{proc: proc, e: storage, pn: &planpb.Plan{}}).refreshOneView(target, source)
		require.ErrorIs(t, err, expected)
	})

	t.Run("physical identity changed", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		ctrl := gomock.NewController(t)
		storage := mock_frontend.NewMockEngine(ctrl)
		database := mock_frontend.NewMockDatabase(ctrl)
		relation := mock_frontend.NewMockRelation(ctrl)
		storage.EXPECT().Database(gomock.Any(), "db", gomock.Any()).Return(database, nil)
		database.EXPECT().Relation(gomock.Any(), "view", gomock.Any()).Return(relation, nil)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(99))
		err := (&Compile{proc: proc, e: storage, pn: &planpb.Plan{}}).refreshOneView(target, source)
		require.Error(t, err)
		require.Contains(t, err.Error(), "identity changed")
	})

	t.Run("target is no longer a view", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		ctrl := gomock.NewController(t)
		storage := mock_frontend.NewMockEngine(ctrl)
		database := mock_frontend.NewMockDatabase(ctrl)
		relation := mock_frontend.NewMockRelation(ctrl)
		storage.EXPECT().Database(gomock.Any(), "db", gomock.Any()).Return(database, nil)
		database.EXPECT().Relation(gomock.Any(), "view", gomock.Any()).Return(relation, nil)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(13))
		relation.EXPECT().CopyTableDef(gomock.Any()).Return(nil)
		err := (&Compile{proc: proc, e: storage, pn: &planpb.Plan{}}).refreshOneView(target, source)
		require.Error(t, err)
		require.Contains(t, err.Error(), "no longer a View")
	})

	t.Run("catalog ownership disappeared", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		installViewMetadataTestExecutor(t, proc, &viewMetadataCleanupRecordingExecutor{})
		ctrl := gomock.NewController(t)
		storage := mock_frontend.NewMockEngine(ctrl)
		database := mock_frontend.NewMockDatabase(ctrl)
		relation := mock_frontend.NewMockRelation(ctrl)
		storage.EXPECT().Database(gomock.Any(), "db", gomock.Any()).Return(database, nil)
		database.EXPECT().Relation(gomock.Any(), "view", gomock.Any()).Return(relation, nil)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(13))
		relation.EXPECT().CopyTableDef(gomock.Any()).Return(&planpb.TableDef{
			ViewSql: &planpb.ViewDef{View: `{}`},
		})
		err := (&Compile{proc: proc, e: storage, pn: &planpb.Plan{}}).refreshOneView(target, source)
		var changed *viewRefreshIdentityChangedError
		require.ErrorAs(t, err, &changed)
	})

	t.Run("replacement error after authoritative regeneration", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		ownership := executor.NewMemResult([]types.Type{
			types.T_uint32.ToType(), types.T_uint32.ToType(), types.T_timestamp.ToType(),
		}, proc.Mp())
		ownership.NewBatchWithRowCount(1)
		require.NoError(t, executor.AppendFixedRows(ownership, 0, []uint32{3}))
		require.NoError(t, executor.AppendFixedRows(ownership, 1, []uint32{5}))
		require.NoError(t, executor.AppendFixedRows(ownership, 2, []types.Timestamp{7}))
		installViewMetadataTestExecutor(t, proc, &viewMetadataCleanupRecordingExecutor{
			results: []executor.Result{ownership.GetResult()},
		})
		ctrl := gomock.NewController(t)
		storage := mock_frontend.NewMockEngine(ctrl)
		database := mock_frontend.NewMockDatabase(ctrl)
		relation := mock_frontend.NewMockRelation(ctrl)
		storage.EXPECT().Database(gomock.Any(), "db", gomock.Any()).Return(database, nil)
		database.EXPECT().Relation(gomock.Any(), "view", gomock.Any()).Return(relation, nil)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(13))
		relation.EXPECT().CopyTableDef(gomock.Any()).Return(&planpb.TableDef{
			Name: "view", Version: 23,
			ViewSql: &planpb.ViewDef{View: `{"Stmt":"create view view as select 1","DefaultDatabase":"db"}`},
		})
		expected := errors.New("replace failed")
		relation.EXPECT().AlterTable(gomock.Any(), gomock.Any(), gomock.Any()).Return(expected)
		err := (&Compile{proc: proc, e: storage, pn: &planpb.Plan{}}).refreshOneView(target, source)
		require.ErrorIs(t, err, expected)
	})
}

func TestRegenerateViewRejectsMalformedPersistedEnvironment(t *testing.T) {
	proc := testutil.NewProcess(t)
	originalTop := proc.GetTopContext()
	_, err := regenerateViewUsingPersistedEnvironment(proc, nil,
		defines.AttachAccountId(proc.Ctx, 7), &planpb.TableDef{ViewSql: &planpb.ViewDef{View: "{"}})
	require.Error(t, err)
	require.Equal(t, originalTop, proc.GetTopContext())
}

func TestRegenerateConstantViewUsingPersistedEnvironment(t *testing.T) {
	proc := testutil.NewProcess(t)
	targetContext := defines.AttachAccountId(proc.Ctx, 7)
	regenerated, err := regenerateViewUsingPersistedEnvironment(proc, nil, targetContext,
		&planpb.TableDef{ViewSql: &planpb.ViewDef{
			View: `{"Stmt":"create view view as select 1","DefaultDatabase":"db"}`,
		}})
	require.NoError(t, err)
	require.NotNil(t, regenerated.TableDef.ViewSql)
	require.Empty(t, regenerated.Dependencies)
}

func TestRefreshPendingConstantViewCommitsReplacementAndLifecycleState(t *testing.T) {
	proc := testutil.NewProcess(t)
	ownership := executor.NewMemResult([]types.Type{
		types.T_uint32.ToType(), types.T_uint32.ToType(), types.T_timestamp.ToType(),
	}, proc.Mp())
	ownership.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendFixedRows(ownership, 0, []uint32{3}))
	require.NoError(t, executor.AppendFixedRows(ownership, 1, []uint32{5}))
	require.NoError(t, executor.AppendFixedRows(ownership, 2, []types.Timestamp{7}))
	exec := &viewMetadataCleanupRecordingExecutor{results: []executor.Result{
		{}, ownership.GetResult(), {}, {}, {AffectedRows: 1},
	}}
	installViewMetadataTestExecutor(t, proc, exec)
	ctrl := gomock.NewController(t)
	storage := mock_frontend.NewMockEngine(ctrl)
	database := mock_frontend.NewMockDatabase(ctrl)
	relation := mock_frontend.NewMockRelation(ctrl)
	proc.GetSessionInfo().StorageEngine = storage
	storage.EXPECT().Database(gomock.Any(), "db", gomock.Any()).Return(database, nil)
	database.EXPECT().Relation(gomock.Any(), "view", gomock.Any()).Return(relation, nil).Times(2)
	relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(13)).Times(2)
	currentDef := &planpb.TableDef{
		Name: "view", Version: 23, LogicalId: 17,
		ViewSql: &planpb.ViewDef{View: `{"Stmt":"create view view as select 1","DefaultDatabase":"db"}`},
	}
	relation.EXPECT().CopyTableDef(gomock.Any()).Return(currentDef)
	relation.EXPECT().AlterTable(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	relation.EXPECT().GetTableDef(gomock.Any()).Return(currentDef)
	database.EXPECT().GetDatabaseId(gomock.Any()).Return("11")

	wrote, err := refreshPendingView(proc, &pendingViewRefresh{viewRefreshTarget: viewRefreshTarget{
		accountID: 7, databaseID: 11, relationID: 13, logicalID: 17,
		databaseName: "db", relationName: "view", generation: 19,
	}})
	require.NoError(t, err)
	require.True(t, wrote)
	require.Len(t, exec.sqls, 5)
	require.Contains(t, exec.sqls[0], "r.target_relation_id is null or r.status='CURRENT'")
	require.Contains(t, exec.sqls[4], "completed_generation=19,status='CURRENT'")
}

func TestRefreshOneConstantViewCommitsReplacementAndLifecycleState(t *testing.T) {
	proc := testutil.NewProcess(t)
	ownership := executor.NewMemResult([]types.Type{
		types.T_uint32.ToType(), types.T_uint32.ToType(), types.T_timestamp.ToType(),
	}, proc.Mp())
	ownership.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendFixedRows(ownership, 0, []uint32{3}))
	require.NoError(t, executor.AppendFixedRows(ownership, 1, []uint32{5}))
	require.NoError(t, executor.AppendFixedRows(ownership, 2, []types.Timestamp{7}))
	exec := &viewMetadataCleanupRecordingExecutor{results: []executor.Result{
		ownership.GetResult(), {}, {}, {AffectedRows: 1},
	}}
	installViewMetadataTestExecutor(t, proc, exec)
	ctrl := gomock.NewController(t)
	storage := mock_frontend.NewMockEngine(ctrl)
	database := mock_frontend.NewMockDatabase(ctrl)
	relation := mock_frontend.NewMockRelation(ctrl)
	storage.EXPECT().Database(gomock.Any(), "db", gomock.Any()).Return(database, nil)
	database.EXPECT().Relation(gomock.Any(), "view", gomock.Any()).Return(relation, nil).Times(2)
	relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(13)).Times(2)
	currentDef := &planpb.TableDef{
		Name: "view", Version: 23, LogicalId: 17,
		ViewSql: &planpb.ViewDef{View: `{"Stmt":"create view view as select 1","DefaultDatabase":"db"}`},
	}
	relation.EXPECT().CopyTableDef(gomock.Any()).Return(currentDef)
	relation.EXPECT().AlterTable(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	relation.EXPECT().GetTableDef(gomock.Any()).Return(currentDef)
	database.EXPECT().GetDatabaseId(gomock.Any()).Return("11")

	err := (&Compile{proc: proc, e: storage, pn: &planpb.Plan{}}).refreshOneView(
		viewRefreshTarget{
			accountID: 7, databaseID: 11, relationID: 13, logicalID: 17,
			databaseName: "db", relationName: "view", generation: 19,
		}, viewRelationMutation{accountID: 7, logicalID: 29})
	require.NoError(t, err)
	require.Len(t, exec.sqls, 4)
	require.Contains(t, exec.sqls[3], "completed_generation=19,status='CURRENT'")
}

func TestMarkSynchronousViewRefreshInvalidUsesGenerationFence(t *testing.T) {
	proc := testutil.NewProcess(t)
	exec := &viewMetadataCleanupRecordingExecutor{}
	installViewMetadataTestExecutor(t, proc, exec)
	c := &Compile{proc: proc, pn: &planpb.Plan{}}
	require.NoError(t, c.markSynchronousViewRefreshInvalid(viewRefreshTarget{
		accountID: 7, relationID: 11, generation: 13,
	}, viewRefreshFailureIdentityChanged))
	require.Len(t, exec.sqls, 1)
	require.Contains(t, exec.sqls[0], "status='INVALID'")
	require.Contains(t, exec.sqls[0], "account_id=7 and target_relation_id=11 and target_generation=13")
}

func TestViewMetadataLifecycleSkipsInternalDatabasesBeforeCatalogProbe(t *testing.T) {
	tests := []struct {
		name string
		run  func(*Compile, string) error
	}{
		{
			name: "persist dependencies",
			run: func(c *Compile, databaseName string) error {
				return c.persistViewDependencies(nil, databaseName, nil)
			},
		},
		{
			name: "refresh after mutation",
			run: func(c *Compile, databaseName string) error {
				return c.refreshViewsAfterRelationMutation(databaseName, "relation", 0, 0)
			},
		},
		{
			name: "enqueue after relation removal",
			run: func(c *Compile, databaseName string) error {
				return c.enqueueViewsAfterRelationRemoval(databaseName, "relation", 1, 2, 3)
			},
		},
		{
			name: "delete dropped view metadata",
			run: func(c *Compile, databaseName string) error {
				return c.deleteDroppedViewMetadata(databaseName, 2)
			},
		},
		{
			name: "delete dropped database metadata",
			run: func(c *Compile, databaseName string) error {
				return c.deleteDroppedDatabaseViewMetadata(0, 1, databaseName)
			},
		},
		{
			name: "enqueue after database removal",
			run: func(c *Compile, databaseName string) error {
				return c.enqueueViewsAfterDatabaseRemoval(databaseName, 0, 1, 2)
			},
		},
	}

	for databaseName := range needSkipDbs {
		for _, test := range tests {
			t.Run(databaseName+"/"+test.name, func(t *testing.T) {
				proc := testutil.NewProcess(t)
				exec := &viewMetadataCleanupRecordingExecutor{}
				installUnavailableViewMetadataTestExecutor(t, proc, exec)
				require.NoError(t, test.run(&Compile{proc: proc, pn: &planpb.Plan{}}, databaseName))
				require.Empty(t, exec.sqls)
			})
		}
	}

	t.Run("user database still probes lifecycle", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		exec := &viewMetadataCleanupRecordingExecutor{}
		installUnavailableViewMetadataTestExecutor(t, proc, exec)
		require.NoError(t, (&Compile{proc: proc, pn: &planpb.Plan{}}).
			refreshViewsAfterRelationMutation("user_db", "relation", 0, 0))
		require.Equal(t, viewMetadataRequireRevalidationSQL(), exec.sqls)
	})
}

func TestViewMetadataLifecycleSkipsRestoreCatalogDDL(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().IsRestore = true
	c := &Compile{proc: proc}
	require.NoError(t, c.persistViewDependencies(nil, "db", nil))
	require.NoError(t, c.refreshViewsAfterRelationMutation("db", "t", 0, 0))
	require.NoError(t, c.enqueueViewsAfterRelationRemoval("db", "t", 0, 0, 0))
	require.NoError(t, c.deleteDroppedViewMetadata("db", 1))
	require.NoError(t, c.deleteDroppedDatabaseViewMetadata(0, 1, "db"))
}

func TestTableAndDatabaseRestoreInvalidateAtRelationRemoval(t *testing.T) {
	for _, level := range []tree.CloneLevelType{
		tree.RestoreCloneLevelTable,
		tree.RestoreCloneLevelDatabase,
	} {
		proc := testutil.NewProcess(t)
		proc.GetSessionInfo().IsRestore = true
		proc.Ctx = context.WithValue(proc.Ctx, tree.CloneLevelCtxKey{}, level)
		exec := &viewMetadataCleanupRecordingExecutor{}
		installViewMetadataTestExecutor(t, proc, exec)
		c := &Compile{proc: proc, pn: &planpb.Plan{}}

		require.NoError(t, c.enqueueViewsAfterRelationRemoval("db", "src", 8, 9, 10))
		require.Equal(t, viewMetadataRequireRevalidationSQL(), exec.sqls)
		require.Contains(t, exec.sqls[2], "source_relation_kind='REVALIDATE_REQUIRED'")

		exec.sqls = nil
		require.NoError(t, c.refreshViewsAfterRelationMutation("db", "src", 9, 10))
		require.Empty(t, exec.sqls)
	}
}

func TestBindingLifecycleInvalidationSQLUsesPersistedIdentity(t *testing.T) {
	snapshotSQL := SnapshotViewMetadataInvalidationSQL(
		`{"ExtraInfo":{"Name":"odd snapshot"}}`, 123, true, 7)
	require.Contains(t, snapshotSQL,
		`d.snapshot_data='{"ExtraInfo":{"Name":"odd snapshot"}}'`)
	require.Contains(t, snapshotSQL,
		"json_extract(d.snapshot_data,'$.ExtraInfo.Name') is null and cast(json_unquote(json_extract("+
			"d.snapshot_data,'$.TS.PhysicalTime')) as bigint)=123")
	require.NotContains(t, snapshotSQL, "d.snapshot_name")
	require.Contains(t, catalog.MoViewDependenciesColumns, "snapshot_data")
	require.Contains(t, catalog.MoViewDependenciesDDL, "snapshot_data text")
	require.NotContains(t, catalog.MoViewDependenciesDDL, "snapshot_name")
	dependencyColumns := make(map[string]struct{})
	for _, column := range strings.Split(catalog.MoViewDependenciesColumns, ",") {
		dependencyColumns[column] = struct{}{}
	}
	for _, match := range regexp.MustCompile(`\bd\.([a-z_]+)\b`).FindAllStringSubmatch(snapshotSQL, -1) {
		require.Contains(t, dependencyColumns, match[1], "generated dependency column %s", match[1])
	}
	ddlStatements, err := mysql.Parse(context.Background(), catalog.MoViewDependenciesDDL, 1)
	require.NoError(t, err)
	require.Len(t, ddlStatements, 1)
	ddlStatements[0].Free()
	preservedTimestampSQL := SnapshotViewMetadataInvalidationSQL(
		`{"ExtraInfo":{"Name":"odd snapshot"}}`, 123, false, 7)
	require.NotContains(t, preservedTimestampSQL, "$.TS.PhysicalTime")
	require.Contains(t, preservedTimestampSQL,
		`d.snapshot_data='{"ExtraInfo":{"Name":"odd snapshot"}}'`)
	require.NotContains(t, strings.ToLower(snapshotSQL), " like ")

	publicationSQL := PublicationViewMetadataInvalidationSQL(8, 9, 10)
	require.Contains(t, publicationSQL,
		"d.publisher_account_id=8 and d.source_account_id=8 and d.source_database_id=9")
	accountPublicationSQL := PublicationViewMetadataInvalidationSQL(8, 0, 10)
	require.NotContains(t, accountPublicationSQL, "d.source_database_id=0")

	subscriptionSQL := SubscriptionViewMetadataInvalidationSQL(11, "odd name", 12)
	require.Contains(t, subscriptionSQL,
		"d.account_id=11 and d.subscription_name='odd name'")
	accountSQL := AccountViewMetadataInvalidationSQL(13, 14)
	require.Contains(t, accountSQL, "d.source_account_id=13")
	for _, sql := range []string{
		snapshotSQL, preservedTimestampSQL, publicationSQL,
		accountPublicationSQL, subscriptionSQL, accountSQL,
	} {
		require.Contains(t, sql, "with recursive affected")
		require.Contains(t, sql, " union select ")
		require.NotContains(t, sql, "select distinct")
		require.Contains(t, sql, "replace into mo_catalog.mo_view_refresh")
		statements, err := mysql.Parse(context.Background(), sql, 1)
		require.NoError(t, err)
		require.Len(t, statements, 1)
	}
}

func TestViewMetadataLifecycleBeforeCapabilityActivation(t *testing.T) {
	t.Run("catalog not upgraded", func(t *testing.T) {
		for _, run := range []func(*Compile) error{
			func(c *Compile) error { return c.persistViewDependencies(nil, "db", nil) },
			func(c *Compile) error { return c.refreshViewsAfterRelationMutation("db", "t", 0, 0) },
			func(c *Compile) error { return c.enqueueViewsAfterRelationRemoval("db", "t", 0, 0, 0) },
			func(c *Compile) error { return c.deleteDroppedViewMetadata("db", 1) },
			func(c *Compile) error { return c.deleteDroppedDatabaseViewMetadata(0, 1, "db") },
			func(c *Compile) error { return c.enqueueViewsAfterDatabaseRemoval("db", 0, 1, 1) },
		} {
			proc := testutil.NewProcess(t)
			exec := &viewMetadataCleanupRecordingExecutor{failures: map[int]error{
				1: moerr.NewNoSuchTableNoCtx("mo_catalog", catalog.MO_VIEW_DEPENDENCIES),
			}}
			installUnavailableViewMetadataTestExecutor(t, proc, exec)
			require.NoError(t, run(&Compile{proc: proc, pn: &planpb.Plan{}}))
			require.Equal(t, []string{catalog.ViewMetadataLifecycleGateSQL}, exec.sqls)
		}
	})

	t.Run("catalog ready", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		exec := &viewMetadataCleanupRecordingExecutor{}
		installUnavailableViewMetadataTestExecutor(t, proc, exec)
		require.NoError(t, (&Compile{proc: proc, pn: &planpb.Plan{}}).
			persistViewDependencies(nil, "db", nil))
		require.Equal(t, viewMetadataRequireRevalidationSQL(), exec.sqls)
		require.Contains(t, exec.sqls[2], "source_relation_kind='REVALIDATE_REQUIRED'")
	})

	t.Run("catalog failure", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		testErr := moerr.NewTxnNeedRetryNoCtx()
		exec := &viewMetadataCleanupRecordingExecutor{failures: map[int]error{1: testErr}}
		installUnavailableViewMetadataTestExecutor(t, proc, exec)
		err := (&Compile{proc: proc, pn: &planpb.Plan{}}).persistViewDependencies(nil, "db", nil)
		require.ErrorIs(t, err, testErr)
	})
}

func TestViewMetadataCleanupLocksLifecycleGateBeforeRows(t *testing.T) {
	tests := []struct {
		name string
		run  func(*Compile) error
	}{
		{
			name: "view",
			run:  func(c *Compile) error { return c.deleteDroppedViewMetadata("db", 11) },
		},
		{
			name: "database",
			run: func(c *Compile) error {
				return c.deleteDroppedDatabaseViewMetadata(0, 7, "db")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			exec := &viewMetadataCleanupRecordingExecutor{}
			installViewMetadataTestExecutor(t, proc, exec)
			require.NoError(t, tc.run(&Compile{proc: proc, pn: &planpb.Plan{}}))
			require.Len(t, exec.sqls, 3)
			require.Equal(t, catalog.ViewMetadataLifecycleGateSQL, exec.sqls[0])
			require.Equal(t, viewMetadataRequireRevalidationSQL(), exec.sqls)
			require.Contains(t, exec.sqls[2], "source_relation_kind='REVALIDATE_REQUIRED'")
		})
	}
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

	revalidating := cursor
	revalidating.status = catalog.ViewRefreshStatusRevalidateScan
	revalidated, ok := nextLegacyViewScanCursor(revalidating, []legacyViewCandidate{{
		accountID: 2, databaseName: "new db", relationName: "new view",
	}})
	require.True(t, ok)
	require.Equal(t, catalog.ViewRefreshStatusRevalidateScan, revalidated.status)
	revalidated, ok = nextLegacyViewScanCursor(revalidated, nil)
	require.True(t, ok)
	require.Equal(t, catalog.ViewRefreshStatusActivated, revalidated.status)

	activated, ok := nextLegacyViewScanCursor(revalidated, nil)
	require.True(t, ok)
	require.Equal(t, catalog.ViewRefreshStatusActivated, activated.status)

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

func TestClassifyViewRefreshFailureUsesTypedDisposition(t *testing.T) {
	ctx := context.Background()
	identityCause := errors.New("identity changed")
	dependencyCause := errors.New("dependency unavailable")
	tests := []struct {
		name        string
		err         error
		code        viewRefreshFailureCode
		disposition viewRefreshDisposition
	}{
		{"none", nil, viewRefreshFailureNone, viewRefreshRollbackDDL},
		{"identity", &viewRefreshIdentityChangedError{cause: identityCause}, viewRefreshFailureIdentityChanged, viewRefreshMarkInvalid},
		{"dependency", &viewRefreshDependencyUnavailableError{cause: dependencyCause}, viewRefreshFailureDependencyUnavailable, viewRefreshRetry},
		{"canceled", context.Canceled, viewRefreshFailureCanceled, viewRefreshRetry},
		{"deadline", context.DeadlineExceeded, viewRefreshFailureCanceled, viewRefreshRetry},
		{"transaction", moerr.NewTxnNeedRetryWithDefChangedNoCtx(), viewRefreshFailureTxnConflict, viewRefreshRetry},
		{"missing table", moerr.NewNoSuchTable(ctx, "db", "t"), viewRefreshFailureDependencyUnavailable, viewRefreshRetry},
		{"parser", moerr.NewParseError(ctx, "invalid"), viewRefreshFailurePlannerIncompatible, viewRefreshMarkInvalid},
		{"invalid", moerr.NewInvalidInput(ctx, "invalid"), viewRefreshFailurePermanentlyInvalid, viewRefreshMarkInvalid},
		{"backend", moerr.NewBackendClosedNoCtx(), viewRefreshFailureInfrastructure, viewRefreshRetry},
		{"unknown", errors.New("unknown"), viewRefreshFailureInfrastructure, viewRefreshRetry},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			failure := classifyViewRefreshFailure(tc.err)
			require.Equal(t, tc.code, failure.code)
			require.Equal(t, tc.disposition, failure.disposition)
			if tc.err != nil {
				require.Equal(t, tc.err.Error(), failure.Error())
				require.ErrorIs(t, failure, tc.err)
			}
		})
	}
	require.Equal(t, identityCause.Error(), (&viewRefreshIdentityChangedError{cause: identityCause}).Error())
	require.ErrorIs(t, &viewRefreshIdentityChangedError{cause: identityCause}, identityCause)
	require.Equal(t, dependencyCause.Error(), (&viewRefreshDependencyUnavailableError{cause: dependencyCause}).Error())
	require.ErrorIs(t, &viewRefreshDependencyUnavailableError{cause: dependencyCause}, dependencyCause)
}

func TestViewMetadataRecoveryRejectsUnavailableRuntimeState(t *testing.T) {
	proc := testutil.NewProcess(t)
	canceled, cancel := context.WithCancel(proc.Ctx)
	cancel()
	proc.Ctx = canceled
	_, err := discoverLegacyViewMetadata(proc)
	require.ErrorIs(t, err, context.Canceled)

	proc = testutil.NewProcess(t)
	refreshed, err := refreshPendingView(proc, &pendingViewRefresh{})
	require.False(t, refreshed)
	require.Error(t, err)

	runtime := moruntime.ServiceRuntime(proc.GetService())
	oldExecutor, hadOldExecutor := runtime.GetGlobalVariables(moruntime.InternalSQLExecutor)
	if hadOldExecutor {
		runtime.CompareAndDeleteGlobalVariables(moruntime.InternalSQLExecutor, oldExecutor)
		t.Cleanup(func() {
			runtime.SetGlobalVariables(moruntime.InternalSQLExecutor, oldExecutor)
		})
	}
	_, err = recoverOnePendingViewMetadata(proc, "worker")
	require.Error(t, err)
	_, err = discoverLegacyViewMetadata(proc)
	require.Error(t, err)
	require.NoError(t, lockViewMetadataLifecycleGate(proc))
}

func TestRefreshPendingViewFailsClosedBeforeRegeneration(t *testing.T) {
	lookupErr := moerr.NewInternalErrorNoCtx("catalog lookup failed")
	for _, tc := range []struct {
		name  string
		setup func(*gomock.Controller, *mock_frontend.MockEngine)
		check func(*testing.T, error)
	}{
		{
			name: "database lookup",
			setup: func(_ *gomock.Controller, engine *mock_frontend.MockEngine) {
				engine.EXPECT().Database(gomock.Any(), "db", gomock.Any()).Return(nil, lookupErr)
			},
			check: func(t *testing.T, err error) { require.ErrorIs(t, err, lookupErr) },
		},
		{
			name: "relation lookup",
			setup: func(ctrl *gomock.Controller, engine *mock_frontend.MockEngine) {
				database := mock_frontend.NewMockDatabase(ctrl)
				engine.EXPECT().Database(gomock.Any(), "db", gomock.Any()).Return(database, nil)
				database.EXPECT().Relation(gomock.Any(), "v", gomock.Any()).Return(nil, lookupErr)
			},
			check: func(t *testing.T, err error) { require.ErrorIs(t, err, lookupErr) },
		},
		{
			name: "relation identity changed",
			setup: func(ctrl *gomock.Controller, engine *mock_frontend.MockEngine) {
				database := mock_frontend.NewMockDatabase(ctrl)
				relation := mock_frontend.NewMockRelation(ctrl)
				engine.EXPECT().Database(gomock.Any(), "db", gomock.Any()).Return(database, nil)
				database.EXPECT().Relation(gomock.Any(), "v", gomock.Any()).Return(relation, nil)
				relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(99))
			},
			check: func(t *testing.T, err error) {
				var identityErr *viewRefreshIdentityChangedError
				require.ErrorAs(t, err, &identityErr)
			},
		},
		{
			name: "target is not a View",
			setup: func(ctrl *gomock.Controller, engine *mock_frontend.MockEngine) {
				database := mock_frontend.NewMockDatabase(ctrl)
				relation := mock_frontend.NewMockRelation(ctrl)
				engine.EXPECT().Database(gomock.Any(), "db", gomock.Any()).Return(database, nil)
				database.EXPECT().Relation(gomock.Any(), "v", gomock.Any()).Return(relation, nil)
				relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(3))
				relation.EXPECT().CopyTableDef(gomock.Any()).Return(nil)
			},
			check: func(t *testing.T, err error) { require.Error(t, err) },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			installViewMetadataTestExecutor(t, proc, &viewMetadataCleanupRecordingExecutor{})
			ctrl := gomock.NewController(t)
			engine := mock_frontend.NewMockEngine(ctrl)
			proc.GetSessionInfo().StorageEngine = engine
			tc.setup(ctrl, engine)

			originalTopContext := proc.GetTopContext()
			refreshed, err := refreshPendingView(proc, &pendingViewRefresh{
				viewRefreshTarget: viewRefreshTarget{
					accountID: 1, databaseID: 2, relationID: 3, logicalID: 4,
					databaseName: "db", relationName: "v",
				},
			})
			require.False(t, refreshed)
			tc.check(t, err)
			require.Equal(t, originalTopContext, proc.GetTopContext())
		})
	}
}

func installViewMetadataTestExecutor(
	t *testing.T,
	proc *process.Process,
	exec executor.SQLExecutor,
) {
	t.Helper()
	runtime := moruntime.ServiceRuntime(proc.GetService())
	oldExecutor, hadOldExecutor := runtime.GetGlobalVariables(moruntime.InternalSQLExecutor)
	runtime.SetGlobalVariables(moruntime.InternalSQLExecutor, exec)
	t.Cleanup(func() {
		if hadOldExecutor {
			runtime.SetGlobalVariables(moruntime.InternalSQLExecutor, oldExecutor)
		} else {
			runtime.CompareAndDeleteGlobalVariables(moruntime.InternalSQLExecutor, exec)
		}
	})
}

func installUnavailableViewMetadataTestExecutor(
	t *testing.T,
	proc *process.Process,
	exec executor.SQLExecutor,
) {
	t.Helper()
	runtime := moruntime.ServiceRuntime(proc.GetService())
	oldExecutor, hadOldExecutor := runtime.GetGlobalVariables(moruntime.InternalSQLExecutor)
	runtime.SetGlobalVariables(moruntime.InternalSQLExecutor, exec)
	t.Cleanup(func() {
		if hadOldExecutor {
			runtime.SetGlobalVariables(moruntime.InternalSQLExecutor, oldExecutor)
		} else {
			runtime.CompareAndDeleteGlobalVariables(moruntime.InternalSQLExecutor, exec)
		}
	})
}

func TestViewMetadataRecoveryErrorAndEmptyWorkPaths(t *testing.T) {
	t.Run("initial cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := runViewMetadataRecoveryPage(ctx,
			func() (bool, error) { panic("unexpected discovery") },
			func() (bool, error) { panic("unexpected recovery") })
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("recovery error", func(t *testing.T) {
		expected := errors.New("recover failed")
		err := runViewMetadataRecoveryPage(context.Background(),
			func() (bool, error) { return false, nil },
			func() (bool, error) { return false, expected })
		require.ErrorIs(t, err, expected)
	})

	t.Run("catalog query error", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		expected := errors.New("catalog unavailable")
		exec := &viewMetadataCleanupRecordingExecutor{failures: map[int]error{1: expected}}
		installViewMetadataTestExecutor(t, proc, exec)
		_, err := recoverOnePendingViewMetadata(proc, "worker")
		require.ErrorIs(t, err, expected)
	})

	t.Run("no pending work", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		exec := &viewMetadataCleanupRecordingExecutor{}
		installViewMetadataTestExecutor(t, proc, exec)
		count, err := recoverOnePendingViewMetadata(proc, "worker")
		require.NoError(t, err)
		require.Zero(t, count)
	})

	t.Run("pending work without engine", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		result := executor.NewMemResult([]types.Type{
			types.T_uint32.ToType(), types.T_uint64.ToType(), types.T_uint64.ToType(),
			types.T_uint64.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(),
			types.T_uint64.ToType(), types.T_uint64.ToType(), types.T_varchar.ToType(),
		}, proc.Mp())
		result.NewBatchWithRowCount(1)
		require.NoError(t, executor.AppendFixedRows(result, 0, []uint32{1}))
		require.NoError(t, executor.AppendFixedRows(result, 1, []uint64{2}))
		require.NoError(t, executor.AppendFixedRows(result, 2, []uint64{3}))
		require.NoError(t, executor.AppendFixedRows(result, 3, []uint64{4}))
		require.NoError(t, executor.AppendStringRows(result, 4, []string{"db"}))
		require.NoError(t, executor.AppendStringRows(result, 5, []string{"view"}))
		require.NoError(t, executor.AppendFixedRows(result, 6, []uint64{5}))
		require.NoError(t, executor.AppendFixedRows(result, 7, []uint64{6}))
		require.NoError(t, executor.AppendStringRows(result, 8, []string{viewRefreshStatusPending}))
		exec := &viewMetadataCleanupRecordingExecutor{results: []executor.Result{result.GetResult()}}
		installViewMetadataTestExecutor(t, proc, exec)
		_, err := recoverOnePendingViewMetadata(proc, "worker")
		require.Error(t, err)
	})
}

func TestRecoverViewMetadataCommandRoutesDurableRecoveryModes(t *testing.T) {
	t.Run("capability disabled", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		count, err := recoverViewMetadataCommand(proc, `{"worker_id":"worker"}`)
		require.NoError(t, err)
		require.Zero(t, count)
	})

}

func TestLegacyDiscoveryCursorFailurePaths(t *testing.T) {
	t.Run("initialize cursor", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		exec := &viewMetadataCleanupRecordingExecutor{}
		count, err := discoverLegacyViewPage(proc, exec, executor.Options{})
		require.NoError(t, err)
		require.Equal(t, 1, count)
		require.Len(t, exec.sqls, 2)
	})

	makeCursor := func(t *testing.T, proc *process.Process, status string) executor.Result {
		t.Helper()
		result := executor.NewMemResult([]types.Type{
			types.T_uint32.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(),
			types.T_uint64.ToType(), types.T_varchar.ToType(),
		}, proc.Mp())
		result.NewBatchWithRowCount(1)
		require.NoError(t, executor.AppendFixedRows(result, 0, []uint32{1}))
		require.NoError(t, executor.AppendStringRows(result, 1, []string{"db"}))
		require.NoError(t, executor.AppendStringRows(result, 2, []string{"view"}))
		require.NoError(t, executor.AppendFixedRows(result, 3, []uint64{1}))
		require.NoError(t, executor.AppendStringRows(result, 4, []string{status}))
		return result.GetResult()
	}

	t.Run("invalid cursor state", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		exec := &viewMetadataCleanupRecordingExecutor{results: []executor.Result{makeCursor(t, proc, "invalid")}}
		_, err := discoverLegacyViewPage(proc, exec, executor.Options{})
		require.Error(t, err)
	})

	t.Run("page query error", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		expected := errors.New("page unavailable")
		exec := &viewMetadataCleanupRecordingExecutor{
			results:  []executor.Result{makeCursor(t, proc, catalog.ViewRefreshStatusLegacyScan)},
			failures: map[int]error{2: expected},
		}
		_, err := discoverLegacyViewPage(proc, exec, executor.Options{})
		require.ErrorIs(t, err, expected)
		require.Contains(t, exec.sqls[1], "where t.relkind='v'")
		require.Contains(t, exec.sqls[1],
			"t.reldatabase not in ('information_schema','mo_catalog','mo_debug','mo_task','mysql','system','system_metrics')")
	})

	t.Run("revalidation page enqueues current view", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		page := executor.NewMemResult([]types.Type{
			types.T_uint32.ToType(), types.T_uint64.ToType(), types.T_uint64.ToType(),
			types.T_uint64.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(),
			types.T_varchar.ToType(), types.T_uint64.ToType(), types.T_varchar.ToType(),
		}, proc.Mp())
		page.NewBatchWithRowCount(1)
		require.NoError(t, executor.AppendFixedRows(page, 0, []uint32{1}))
		require.NoError(t, executor.AppendFixedRows(page, 1, []uint64{2}))
		require.NoError(t, executor.AppendFixedRows(page, 2, []uint64{3}))
		require.NoError(t, executor.AppendFixedRows(page, 3, []uint64{4}))
		require.NoError(t, executor.AppendStringRows(page, 4, []string{"db"}))
		require.NoError(t, executor.AppendStringRows(page, 5, []string{"view"}))
		require.NoError(t, executor.AppendStringRows(page, 6, []string{catalog.SystemViewRel}))
		require.NoError(t, executor.AppendFixedRows(page, 7, []uint64{3}))
		require.NoError(t, executor.AppendStringRows(page, 8, []string{viewRefreshStatusCurrent}))
		exec := &viewMetadataCleanupRecordingExecutor{results: []executor.Result{
			makeCursor(t, proc, catalog.ViewRefreshStatusRevalidateScan),
			page.GetResult(), {}, {AffectedRows: 1},
		}}
		count, err := discoverLegacyViewPage(proc, exec, executor.Options{})
		require.NoError(t, err)
		require.Equal(t, 1, count)
		require.Len(t, exec.sqls, 4)
		require.Contains(t, exec.sqls[2], "set target_generation=target_generation+1,status='PENDING'")
		require.Contains(t, exec.sqls[3], "source_relation_kind='REVALIDATE_SCAN'")
	})

	t.Run("empty revalidation page releases tenant markers after cursor CAS", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		exec := &viewMetadataCleanupRecordingExecutor{results: []executor.Result{
			makeCursor(t, proc, catalog.ViewRefreshStatusRevalidateScan),
			{}, {AffectedRows: 1}, {},
		}}
		count, err := discoverLegacyViewPage(proc, exec, executor.Options{})
		require.NoError(t, err)
		require.Equal(t, 1, count)
		require.Len(t, exec.sqls, 4)
		require.Contains(t, exec.sqls[2], "account_id=0")
		require.Contains(t, exec.sqls[2], "source_relation_kind='ACTIVATED'")
		require.Contains(t, exec.sqls[3], "account_id<>0")
		require.Contains(t, exec.sqls[3], "source_relation_kind='REVALIDATE_SCAN'")
	})
}

func TestBeginViewMetadataRevalidationResetsDurableCursor(t *testing.T) {
	proc := testutil.NewProcess(t)
	exec := &viewMetadataCleanupRecordingExecutor{results: []executor.Result{{AffectedRows: 1}}}
	installViewMetadataTestExecutor(t, proc, exec)
	count, err := beginViewMetadataRevalidation(proc)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Contains(t, exec.sqls[0], "source_relation_kind='REVALIDATE_SCAN'")
}

func TestBeginViewMetadataRevalidationPropagatesCatalogError(t *testing.T) {
	proc := testutil.NewProcess(t)
	testErr := moerr.NewInternalErrorNoCtx("catalog unavailable")
	exec := &viewMetadataCleanupRecordingExecutor{failures: map[int]error{1: testErr}}
	installViewMetadataTestExecutor(t, proc, exec)
	count, err := beginViewMetadataRevalidation(proc)
	require.Zero(t, count)
	require.ErrorIs(t, err, testErr)
}

func TestViewMetadataRevalidationActivationIsPersistedAndIdempotent(t *testing.T) {
	markerResult := func() executor.Result {
		proc := testutil.NewProcess(t)
		result := executor.NewMemResult([]types.Type{
			types.T_uint32.ToType(), types.T_varchar.ToType(), types.T_uint64.ToType(),
		}, proc.Mp())
		result.NewBatchWithRowCount(1)
		require.NoError(t, executor.AppendFixedRows(result, 0, []uint32{viewMetadataRevalidationSeedComplete}))
		require.NoError(t, executor.AppendStringRows(result, 1,
			[]string{catalog.ViewRefreshStatusRevalidateRequired}))
		require.NoError(t, executor.AppendFixedRows(result, 2, []uint64{7}))
		return result.GetResult()
	}
	exec := &viewMetadataCleanupRecordingExecutor{results: []executor.Result{
		{}, {}, {}, {}, markerResult(), {}, markerResult(), {},
	}}
	require.NoError(t, RequireViewMetadataRevalidation(context.Background(), exec))
	require.NoError(t, StartViewMetadataRevalidation(context.Background(), exec, "worker"))
	require.Len(t, exec.sqls, 8)
	require.Equal(t, catalog.ViewMetadataLifecycleGateSQL, exec.sqls[0])
	require.Contains(t, exec.sqls[1], "select source_account_id")
	require.Contains(t, exec.sqls[2],
		"where not exists")
	require.Contains(t, exec.sqls[2], "'REVALIDATE_REQUIRED'")
	require.Contains(t, exec.sqls[3],
		"source_relation_kind='REVALIDATE_REQUIRED'")
	require.Contains(t, exec.sqls[3],
		"source_relation_kind in ('LEGACY_SCAN','REVALIDATE_SCAN','ACTIVATED')")
	require.Contains(t, exec.sqls[3], "where account_id=0")
	require.Contains(t, exec.sqls[4], "select source_account_id")
	require.Equal(t, catalog.ViewMetadataLifecycleGateSQL, exec.sqls[5])
	require.Contains(t, exec.sqls[6], "select source_account_id")
	require.Contains(t, exec.sqls[7],
		"source_relation_kind='REVALIDATE_SCAN'")
	require.Contains(t, exec.sqls[7],
		"source_relation_kind='REVALIDATE_REQUIRED'")
	require.Contains(t, exec.sqls[7], "source_account_id=0")
	require.NotContains(t, exec.sqls[7], "where account_id=0")
	require.Contains(t, exec.sqls[7], "where target_relation_id=0")
	for _, sql := range exec.sqls {
		statements, err := mysql.Parse(context.Background(), sql, 1)
		require.NoError(t, err, sql)
		require.Len(t, statements, 1)
		statements[0].Free()
	}
}

func TestRunViewMetadataRecoveryAdvancesRequiredSentinels(t *testing.T) {
	proc := testutil.NewProcess(t)
	required := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, proc.Mp())
	required.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendStringRows(required, 0,
		[]string{catalog.ViewRefreshStatusRevalidateRequired}))
	marker := executor.NewMemResult([]types.Type{
		types.T_uint32.ToType(), types.T_varchar.ToType(), types.T_uint64.ToType(),
	}, proc.Mp())
	marker.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendFixedRows(marker, 0,
		[]uint32{viewMetadataRevalidationSeedComplete}))
	require.NoError(t, executor.AppendStringRows(marker, 1,
		[]string{catalog.ViewRefreshStatusRevalidateRequired}))
	require.NoError(t, executor.AppendFixedRows(marker, 2, []uint64{7}))
	exec := &viewMetadataCleanupRecordingExecutor{results: []executor.Result{
		required.GetResult(), {}, marker.GetResult(), {},
	}}
	require.NoError(t, RunViewMetadataRecovery(context.Background(), exec, "worker"))
	require.Len(t, exec.sqls, 4)
	require.Contains(t, exec.sqls[0], "select source_relation_kind")
	require.Equal(t, catalog.ViewMetadataLifecycleGateSQL, exec.sqls[1])
	require.Contains(t, exec.sqls[3], "where target_relation_id=0")
	require.NotContains(t, exec.sqls[3], "where account_id=0")
}

func TestRequireViewMetadataRevalidationFastReturnsWhenAlreadyRequired(t *testing.T) {
	proc := testutil.NewProcess(t)
	current := executor.NewMemResult([]types.Type{
		types.T_uint32.ToType(), types.T_varchar.ToType(), types.T_uint64.ToType(),
	}, proc.Mp())
	current.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendFixedRows(current, 0,
		[]uint32{viewMetadataRevalidationSeedComplete}))
	require.NoError(t, executor.AppendStringRows(current, 1,
		[]string{catalog.ViewRefreshStatusRevalidateRequired}))
	require.NoError(t, executor.AppendFixedRows(current, 2, []uint64{7}))
	exec := &viewMetadataCleanupRecordingExecutor{results: []executor.Result{{}, current.GetResult()}}
	require.NoError(t, RequireViewMetadataRevalidation(context.Background(), exec))
	require.Len(t, exec.sqls, 2)
	require.Equal(t, catalog.ViewMetadataLifecycleGateSQL, exec.sqls[0])
	require.Contains(t, exec.sqls[1], "for update")
}

func TestSeedViewMetadataRevalidationPageIsBounded(t *testing.T) {
	proc := testutil.NewProcess(t)
	marker := executor.NewMemResult([]types.Type{
		types.T_uint32.ToType(), types.T_varchar.ToType(), types.T_uint64.ToType(),
	}, proc.Mp())
	marker.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendFixedRows(marker, 0, []uint32{0}))
	require.NoError(t, executor.AppendStringRows(marker, 1,
		[]string{catalog.ViewRefreshStatusRevalidateRequired}))
	require.NoError(t, executor.AppendFixedRows(marker, 2, []uint64{9}))
	accounts := executor.NewMemResult([]types.Type{types.T_int32.ToType()}, proc.Mp())
	accounts.NewBatchWithRowCount(viewMetadataRecoveryPageSize)
	ids := make([]int32, viewMetadataRecoveryPageSize)
	for i := range ids {
		ids[i] = int32(i + 1)
	}
	require.NoError(t, executor.AppendFixedRows(accounts, 0, ids))
	results := []executor.Result{marker.GetResult(), accounts.GetResult()}
	results = append(results, make([]executor.Result, viewMetadataRecoveryPageSize+1)...)
	results[len(results)-1].AffectedRows = 1
	exec := &viewMetadataCleanupRecordingExecutor{results: results}
	complete := false
	err := exec.ExecTxn(context.Background(), func(txn executor.TxnExecutor) error {
		var seedErr error
		var active bool
		complete, active, seedErr = seedViewMetadataRevalidationPage(txn)
		require.True(t, active)
		return seedErr
	}, executor.Options{})
	require.NoError(t, err)
	require.False(t, complete)
	require.Len(t, exec.sqls, viewMetadataRecoveryPageSize+3)
	require.Contains(t, exec.sqls[1], fmt.Sprintf("limit %d", viewMetadataRecoveryPageSize))
	require.Contains(t, exec.sqls[len(exec.sqls)-1],
		fmt.Sprintf("set source_account_id=%d", viewMetadataRecoveryPageSize))
}

func TestSeedViewMetadataRevalidationPageRejectsInvalidAccountPage(t *testing.T) {
	tests := []struct {
		name          string
		accountType   types.T
		rowCount      int
		accountIDs    []int32
		unsignedIDs   []uint32
		expectedError string
	}{
		{
			name:          "unexpected account vector type",
			accountType:   types.T_uint32,
			rowCount:      1,
			unsignedIDs:   []uint32{1},
			expectedError: "expected INT",
		},
		{
			name:          "negative account id",
			accountType:   types.T_int32,
			rowCount:      1,
			accountIDs:    []int32{-1},
			expectedError: "account_id is negative",
		},
		{
			name:          "batch row count exceeds vector length",
			accountType:   types.T_int32,
			rowCount:      2,
			accountIDs:    []int32{1},
			expectedError: "rows, length",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			marker := executor.NewMemResult([]types.Type{
				types.T_uint32.ToType(), types.T_varchar.ToType(), types.T_uint64.ToType(),
			}, proc.Mp())
			marker.NewBatchWithRowCount(1)
			require.NoError(t, executor.AppendFixedRows(marker, 0, []uint32{0}))
			require.NoError(t, executor.AppendStringRows(marker, 1,
				[]string{catalog.ViewRefreshStatusRevalidateRequired}))
			require.NoError(t, executor.AppendFixedRows(marker, 2, []uint64{9}))

			accounts := executor.NewMemResult([]types.Type{tc.accountType.ToType()}, proc.Mp())
			accounts.NewBatchWithRowCount(tc.rowCount)
			if tc.accountType == types.T_int32 {
				require.NoError(t, executor.AppendFixedRows(accounts, 0, tc.accountIDs))
			} else {
				require.NoError(t, executor.AppendFixedRows(accounts, 0, tc.unsignedIDs))
			}
			exec := &viewMetadataCleanupRecordingExecutor{
				results: []executor.Result{marker.GetResult(), accounts.GetResult()},
			}

			var complete bool
			var active bool
			var seedErr error
			require.NotPanics(t, func() {
				seedErr = exec.ExecTxn(context.Background(), func(txn executor.TxnExecutor) error {
					var err error
					complete, active, err = seedViewMetadataRevalidationPage(txn)
					return err
				}, executor.Options{})
			})
			require.ErrorContains(t, seedErr, tc.expectedError)
			require.False(t, complete)
			require.True(t, active)
			// The page is decoded before any metadata rows are written.
			require.Len(t, exec.sqls, 2)
		})
	}
}

func TestViewMetadataRevalidationActivationPropagatesCatalogErrors(t *testing.T) {
	testErr := moerr.NewInternalErrorNoCtx("catalog unavailable")

	t.Run("required sentinel insert", func(t *testing.T) {
		exec := &viewMetadataCleanupRecordingExecutor{failures: map[int]error{2: testErr}}
		require.ErrorIs(t, RequireViewMetadataRevalidation(context.Background(), exec), testErr)
		require.Len(t, exec.sqls, 2)
	})

	t.Run("required marker transition", func(t *testing.T) {
		exec := &viewMetadataCleanupRecordingExecutor{failures: map[int]error{3: testErr}}
		require.ErrorIs(t, RequireViewMetadataRevalidation(context.Background(), exec), testErr)
		require.Len(t, exec.sqls, 3)
	})

	t.Run("required marker cursor", func(t *testing.T) {
		exec := &viewMetadataCleanupRecordingExecutor{failures: map[int]error{4: testErr}}
		require.ErrorIs(t, RequireViewMetadataRevalidation(context.Background(), exec), testErr)
		require.Len(t, exec.sqls, 4)
	})

	t.Run("required lifecycle gate", func(t *testing.T) {
		exec := &viewMetadataCleanupRecordingExecutor{failures: map[int]error{1: testErr}}
		require.ErrorIs(t, RequireViewMetadataRevalidation(context.Background(), exec), testErr)
		require.Len(t, exec.sqls, 1)
	})

	t.Run("start scan transition", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		marker := executor.NewMemResult([]types.Type{
			types.T_uint32.ToType(), types.T_varchar.ToType(), types.T_uint64.ToType(),
		}, proc.Mp())
		marker.NewBatchWithRowCount(1)
		require.NoError(t, executor.AppendFixedRows(marker, 0,
			[]uint32{viewMetadataRevalidationSeedComplete}))
		require.NoError(t, executor.AppendStringRows(marker, 1,
			[]string{catalog.ViewRefreshStatusRevalidateRequired}))
		require.NoError(t, executor.AppendFixedRows(marker, 2, []uint64{7}))
		exec := &viewMetadataCleanupRecordingExecutor{
			results:  []executor.Result{{}, marker.GetResult()},
			failures: map[int]error{3: testErr},
		}
		require.ErrorIs(t,
			StartViewMetadataRevalidation(context.Background(), exec, "restarted-worker"), testErr)
		require.Len(t, exec.sqls, 3)
	})

	t.Run("start lifecycle gate", func(t *testing.T) {
		exec := &viewMetadataCleanupRecordingExecutor{failures: map[int]error{1: testErr}}
		require.ErrorIs(t,
			StartViewMetadataRevalidation(context.Background(), exec, "restarted-worker"), testErr)
		require.Len(t, exec.sqls, 1)
	})

	t.Run("start tenant marker cursor", func(t *testing.T) {
		exec := &viewMetadataCleanupRecordingExecutor{failures: map[int]error{2: testErr}}
		require.ErrorIs(t,
			StartViewMetadataRevalidation(context.Background(), exec, "restarted-worker"), testErr)
		require.Len(t, exec.sqls, 2)
	})
}

func TestRecoveryContextMissingSnapshotAndDependencyIdentity(t *testing.T) {
	proc := testutil.NewProcess(t)
	exec := &viewMetadataCleanupRecordingExecutor{}
	installViewMetadataTestExecutor(t, proc, exec)
	ctx := &recoveryCompilerContext{
		compilerContext: &compilerContext{ctx: proc.Ctx, proc: proc},
		legacySnapshots: make(map[string]*plan2.Snapshot),
		dependencies: []plan2.ViewDependency{{
			AccountID: 9, RelationID: 11, LogicalID: 13,
			DatabaseName: "Quoted DB", RelationName: "Quoted Table", LowerCaseTableNames: 1,
		}},
	}

	_, err := ctx.ResolveSnapshotWithSnapshotName("missing")
	require.Error(t, err)

	accountID, err := ctx.ResolveViewDependencyAccount(nil, &planpb.TableDef{TblId: 11}, nil)
	require.NoError(t, err)
	require.Equal(t, uint32(9), accountID)
	accountID, err = ctx.ResolveViewDependencyAccount(
		&planpb.ObjectRef{SchemaName: "quoted db", ObjName: "quoted table"},
		&planpb.TableDef{}, nil)
	require.NoError(t, err)
	require.Equal(t, uint32(9), accountID)
	accountID, err = ctx.ResolveViewDependencyAccount(nil, &planpb.TableDef{}, nil)
	require.NoError(t, err)
	require.Equal(t, uint32(0), accountID)

	ctx.dependencies = nil
	ctx.legacySnapshots["daily"] = &plan2.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 789}}
	valid, err := ctx.CheckTimeStampValid(789)
	require.NoError(t, err)
	require.False(t, valid)
	require.Len(t, exec.sqls, 2)
	require.Contains(t, exec.sqls[1], "where ts=789 limit 1")
}

func TestRecoveryContextRestoresCatalogSnapshot(t *testing.T) {
	proc := testutil.NewProcess(t)
	result := executor.NewMemResult([]types.Type{
		types.T_int64.ToType(), types.T_varchar.ToType(),
		types.T_varchar.ToType(), types.T_uint64.ToType(),
	}, proc.Mp())
	result.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendFixedRows(result, 0, []int64{123456}))
	require.NoError(t, executor.AppendStringRows(result, 1, []string{"account"}))
	require.NoError(t, executor.AppendStringRows(result, 2, []string{"tenant-a"}))
	require.NoError(t, executor.AppendFixedRows(result, 3, []uint64{17}))
	exec := &viewMetadataCleanupRecordingExecutor{results: []executor.Result{result.GetResult()}}
	installViewMetadataTestExecutor(t, proc, exec)
	ctx := &recoveryCompilerContext{
		compilerContext: &compilerContext{ctx: proc.Ctx, proc: proc},
	}

	snapshot, err := ctx.ResolveSnapshotWithSnapshotName("daily")
	require.NoError(t, err)
	require.Equal(t, int64(123456), snapshot.TS.PhysicalTime)
	require.Equal(t, "account", snapshot.ExtraInfo.Level)
	require.Equal(t, uint64(17), snapshot.ExtraInfo.ObjId)
	require.Equal(t, "daily", snapshot.ExtraInfo.Name)
	require.Equal(t, "tenant-a", snapshot.Tenant.TenantName)
	require.Equal(t, uint32(17), snapshot.Tenant.TenantID)
	require.Len(t, exec.sqls, 1)

	cached, err := ctx.ResolveSnapshotWithSnapshotName("daily")
	require.NoError(t, err)
	require.Equal(t, snapshot, cached)
	require.NotSame(t, snapshot, cached)
	require.Len(t, exec.sqls, 1)
}

func TestViewMetadataCleanupPropagatesLifecycleAndRowErrors(t *testing.T) {
	tests := []struct {
		name string
		run  func(*Compile) error
	}{
		{"view", func(c *Compile) error { return c.deleteDroppedViewMetadata("db", 11) }},
		{"database", func(c *Compile) error { return c.deleteDroppedDatabaseViewMetadata(0, 7, "db") }},
	}
	for _, tc := range tests {
		for _, failAt := range []int{1, 2} {
			t.Run(fmt.Sprintf("%s call %d", tc.name, failAt), func(t *testing.T) {
				proc := testutil.NewProcess(t)
				expected := errors.New("catalog failure")
				exec := &viewMetadataCleanupRecordingExecutor{failures: map[int]error{failAt: expected}}
				installViewMetadataTestExecutor(t, proc, exec)
				err := tc.run(&Compile{proc: proc, pn: &planpb.Plan{}})
				require.ErrorIs(t, err, expected)
				require.Len(t, exec.sqls, failAt)
			})
		}
	}
}
