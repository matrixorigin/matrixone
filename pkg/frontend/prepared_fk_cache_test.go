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

package frontend

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/stretchr/testify/require"
)

func TestShouldCachePrepareCompileForeignKeyActions(t *testing.T) {
	makePlan := func(stmtType plan.Query_StatementType, hasForeignKeyAction bool) *plan.Plan {
		return &plan.Plan{
			Plan: &plan.Plan_Query{
				Query: &plan.Query{
					StmtType:            stmtType,
					HasForeignKeyAction: hasForeignKeyAction,
				},
			},
		}
	}

	require.True(t, shouldCachePrepareCompile(nil))
	require.True(t, shouldCachePrepareCompile(&plan.Plan{}))
	require.True(t, shouldCachePrepareCompile(makePlan(plan.Query_UPDATE, false)))
	require.True(t, shouldCachePrepareCompile(makePlan(plan.Query_DELETE, false)))

	require.False(t, shouldCachePrepareCompile(makePlan(plan.Query_UPDATE, true)))
	require.False(t, shouldCachePrepareCompile(makePlan(plan.Query_DELETE, true)))
	require.False(t, shouldCachePrepareCompile(makePlan(plan.Query_INSERT, true)))

	require.True(t, checkNodeCanCache(makePlan(plan.Query_INSERT, false)))
	require.False(t, checkNodeCanCache(makePlan(plan.Query_INSERT, true)))

	require.False(t, shouldRebuildPreparePlan(false, nil))
	require.False(t, shouldRebuildPreparePlan(false, makePlan(plan.Query_INSERT, false)))
	require.True(t, shouldRebuildPreparePlan(false, makePlan(plan.Query_INSERT, true)))
	require.True(t, shouldRebuildPreparePlan(true, makePlan(plan.Query_INSERT, false)))

	subscriptionMetadataPlan := makePlan(plan.Query_SELECT, false)
	subscriptionMetadataPlan.GetQuery().Nodes = []*plan.Node{{
		ViewPath: []*plan.ViewStep{{DatabaseName: "information_schema", ViewName: "statistics", Snapshot: &plan.Snapshot{Tenant: &plan.SnapshotTenant{}}}},
	}}
	require.True(t, shouldRebuildPreparePlan(false, subscriptionMetadataPlan))
	require.False(t, checkNodeCanCache(subscriptionMetadataPlan))
}

func TestTemporalIndexReplansAcrossCacheOwners(t *testing.T) {
	p := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{StmtType: plan.Query_SELECT, TemporalIndexNeedsReplan: true}}}
	require.False(t, checkNodeCanCache(p))
	require.False(t, shouldCachePrepareCompile(p))
	require.True(t, shouldRebuildPreparePlan(false, p))
	p.GetQuery().TemporalIndexNeedsReplan = false
	require.True(t, checkNodeCanCache(p))
	require.True(t, shouldCachePrepareCompile(p))
	require.False(t, shouldRebuildPreparePlan(false, p))
}

func TestTemporalPlanningIntentUsesStatementSQL(t *testing.T) {
	ses, prepareStmt, _, execCtx := newPreparedExecuteEnv(t, 122)
	defer prepareStmt.Close()
	stmt, err := parsers.ParseOne(execCtx.reqCtx, dialect.MYSQL, "select 1", 1)
	require.NoError(t, err)
	defer stmt.Free()
	raw := "select /*+ SET_VAR(query_max_workers=2) */ 1"
	root, intent, ok := temporalPlanningIntentForStatement(ses, ses.GetProc(), raw, "", stmt)
	require.True(t, ok)
	require.Same(t, stmt.(*tree.Select), root)
	require.True(t, intent.ParameterFree)
	require.Equal(t, schedule.WorkerSetMax, intent.Placement.Scheduling.WorkerSet.Mode)
	require.Equal(t, 2, intent.Placement.Scheduling.WorkerSet.MaxWorkers)
	proc := ses.GetProc()
	require.True(t, intent.MatchesProcess(proc))
	require.True(t, temporalPlanningIntentMatchesCompile(&intent, ses, proc, raw, "", stmt))
	require.False(t, temporalPlanningIntentMatchesCompile(&intent, ses, proc, "select 1", "", stmt), "changed scheduling must rebuild")
	require.False(t, temporalPlanningIntentMatchesCompile(&intent, ses, proc, "/*+ SIDECAR */ select 1", "", stmt), "changed offload must rebuild")
	require.False(t, temporalPlanningIntentMatchesCompile(nil, ses, proc, raw, "", stmt))
	oldZone, oldTime := proc.GetSessionInfo().TimeZone, proc.Base.UnixTime
	defer func() { proc.GetSessionInfo().TimeZone, proc.Base.UnixTime = oldZone, oldTime }()
	proc.GetSessionInfo().TimeZone = time.FixedZone("other", 3600)
	require.False(t, intent.MatchesProcess(proc), "zone drift must invalidate the sample")
	proc.GetSessionInfo().TimeZone = oldZone
	proc.Base.UnixTime++
	require.False(t, intent.MatchesProcess(proc), "clock drift must invalidate the sample")
	proc.Base.UnixTime = oldTime
	_, intent, ok = temporalPlanningIntentForStatement(ses, ses.GetProc(), "/*+ SIDECAR */ select 1", "", stmt)
	require.True(t, ok)
	require.True(t, intent.Placement.Sirius)
	_, _, ok = temporalPlanningIntentForStatement(ses, ses.GetProc(), "", "", stmt)
	require.False(t, ok, "missing raw statement SQL cannot authorize a candidate")
	parameterized, err := parsers.ParseOne(execCtx.reqCtx, dialect.MYSQL, "select ?", 1)
	require.NoError(t, err)
	defer parameterized.Free()
	_, _, ok = temporalPlanningIntentForStatement(ses, ses.GetProc(), "select ?", "", parameterized)
	require.False(t, ok)
	root, intent, ok = temporalPlanningIntentForExecution(ses, ses.GetProc(), "select ?", "", parameterized, true)
	require.True(t, ok)
	require.Same(t, parameterized.(*tree.Select), root)
	require.False(t, intent.ParameterFree, "placement is not a parameter diagnostic proof")
}

func TestTemporalPlanningIntentForExplain(t *testing.T) {
	ses, prepared, _, execCtx := newPreparedExecuteEnv(t, 125)
	defer prepared.Close()
	for _, tc := range []struct {
		sql      string
		eligible bool
	}{
		{"explain select 1", true},
		{"explain select ?", false},
		{"explain execute missing", false},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(execCtx.reqCtx, dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			root, _, ok := temporalPlanningIntentForStatement(ses, ses.GetProc(), tc.sql, "", stmt)
			require.Equal(t, tc.eligible, ok)
			if ok {
				require.Same(t, temporalPlanningRoot(stmt), root)
				_, _, ok = temporalPlanningIntentForStatement(ses, ses.GetProc(), "", "", stmt)
				require.False(t, ok, "missing statement-local SQL cannot grant a scope")
			}
		})
	}
}

func TestTemporalIndexPrepareRebuildsAtExecution(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnv(t, 121)
	defer prepareStmt.Close()
	oldPlan := prepareStmt.PreparePlan
	query := oldPlan.GetDcl().GetPrepare().GetPlan().GetQuery()
	require.NotNil(t, query)
	query.TemporalIndexNeedsReplan = true
	_, rebuilt, stmt, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err)
	require.NotNil(t, rebuilt)
	require.NotNil(t, stmt)
	require.NotSame(t, oldPlan, prepareStmt.PreparePlan)
}

func TestPreparedRetrySnapshotsTemporalIntent(t *testing.T) {
	cw := &TxnComputationWrapper{
		paramVals: []any{int64(1)},
		temporalPlanningIntent: &plan2.TemporalPlanningIntent{
			Placement: schedule.TemporalAccessIntent{CurrentCN: schedule.Worker{ID: "cn-a"}},
		},
	}
	retry := cw.preparedExecutionRetry()
	require.Nil(t, retry.temporalIntent, "an unvalidated planning snapshot cannot authorize a retry")
	cw.temporalIntentValidated = true
	retry = cw.preparedExecutionRetry()
	require.NotNil(t, retry.temporalIntent)
	cw.temporalPlanningIntent.Placement.CurrentCN.ID = "cn-b"
	require.Equal(t, "cn-a", retry.temporalIntent.Placement.CurrentCN.ID)
}

func TestTemporalPlanRebuildsOnCompileIntentDrift(t *testing.T) {
	ses, prepared, _, execCtx := newPreparedExecuteEnv(t, 123)
	defer prepared.Close()
	stmt, err := parsers.ParseOne(execCtx.reqCtx, dialect.MYSQL, "select 1", 1)
	require.NoError(t, err)
	defer stmt.Free()
	const sql = "select /*+ SET_VAR(query_max_workers=2) */ 1"
	_, intent, ok := temporalPlanningIntentForStatement(ses, ses.GetProc(), sql, "", stmt)
	require.True(t, ok)
	marked := &plan.Expr{TemporalCandidateFilter: true}
	candidate := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{FilterList: []*plan.Expr{marked}}}}}}
	safe := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{}}}
	builds := 0
	rebuild := func() (*plan2.Plan, error) { builds++; return safe, nil }
	got, validated, err := validateTemporalPlanBeforeCompile(candidate, &intent, ses, ses.GetProc(), sql, "", stmt, rebuild)
	require.NoError(t, err)
	require.Same(t, candidate, got)
	require.True(t, validated)
	require.Zero(t, builds)
	got, validated, err = validateTemporalPlanBeforeCompile(candidate, &intent, ses, ses.GetProc(), "select 1", "", stmt, rebuild)
	require.NoError(t, err)
	require.Same(t, safe, got)
	require.False(t, validated)
	require.Equal(t, 1, builds)
	got, validated, err = validateTemporalPlanBeforeCompile(safe, nil, ses, ses.GetProc(), "", "", stmt, rebuild)
	require.NoError(t, err)
	require.Same(t, safe, got)
	require.False(t, validated)
	require.Equal(t, 1, builds, "candidate-free plans keep their existing scheduling path")
	_, _, err = validateTemporalPlanBeforeCompile(candidate, nil, ses, ses.GetProc(), sql, "", stmt,
		func() (*plan2.Plan, error) { return candidate, nil })
	require.Error(t, err, "an unscoped rebuild must not return a candidate")
}

func TestTemporalSafeRebuildDropsStaleSnapshot(t *testing.T) {
	_, prepared, cw, execCtx := newPreparedExecuteEnv(t, 124)
	defer prepared.Close()
	cw.planGenerationReused = true
	cw.setPlanSnapshotTS(timestamp.Timestamp{PhysicalTime: 123})
	rebuilt, err := cw.rebuildUnscopedTemporalPlan(execCtx)
	require.NoError(t, err)
	require.NotNil(t, rebuilt.GetQuery())
	require.False(t, cw.planGenerationReused)
	if cw.proc.GetTxnOperator() == nil {
		require.False(t, cw.hasPlanSnapshotTS)
	} else {
		require.True(t, cw.hasPlanSnapshotTS)
		require.Equal(t, currentTxnSnapshotTSForProcess(cw.proc), cw.planSnapshotTS)
	}
}

func TestPreparedTemporalDriftRebuildsFromInnerStatement(t *testing.T) {
	ses, prepared, cw, execCtx := newPreparedExecuteEnv(t, 125)
	defer prepared.Close()
	cw.preparedStmt = prepared
	cw.planGenerationReused = true
	cw.setPlanSnapshotTS(timestamp.Timestamp{PhysicalTime: 123})
	writer := execCtx.resper.MysqlRrWr().(*testMysqlWriter)
	writer.makeColumnDefDataFunc = func(_ context.Context, columns []*plan.ColDef) ([][]byte, error) {
		require.Len(t, columns, 1)
		return [][]byte{[]byte("fresh metadata")}, nil
	}
	execCtx.prepareColDef = [][]byte{[]byte("stale metadata")}
	marked := &plan.Expr{TemporalCandidateFilter: true}
	candidate := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{FilterList: []*plan.Expr{marked}}}}}}
	safe, err := cw.validatePreparedTemporalPlan(execCtx, ses, prepared.PrepareStmt, prepared.Sql, candidate)
	require.NoError(t, err)
	require.NotNil(t, safe.GetQuery())
	require.False(t, cw.planGenerationReused)
	require.False(t, cw.temporalIntentValidated)
	require.Equal(t, [][]byte{[]byte("fresh metadata")}, execCtx.prepareColDef, "binary metadata must belong to the replacement plan")
	if cw.proc.GetTxnOperator() == nil {
		require.False(t, cw.hasPlanSnapshotTS)
	} else {
		require.Equal(t, currentTxnSnapshotTSForProcess(cw.proc), cw.planSnapshotTS)
	}
}

func TestCreateCompileTemporalDriftPreservesRequestState(t *testing.T) {
	ses, prepared, cw, execCtx := newPreparedExecuteEnv(t, 126)
	defer prepared.Close()
	_, intent, ok := temporalPlanningIntentForStatement(ses, cw.proc, prepared.Sql, "", prepared.PrepareStmt)
	require.True(t, ok)
	intent.UnixTime++
	marked := &plan.Expr{TemporalCandidateFilter: true}
	candidate := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{FilterList: []*plan.Expr{marked}}}}}}
	top, cancel := context.WithCancel(context.Background())
	defer cancel()
	cw.proc.ReplaceTopCtx(top)
	cw.proc.SetMessageBoard(message.NewMessageBoard())
	oldRequest, oldTop, oldBoard := execCtx.reqCtx, cw.proc.GetTopContext(), cw.proc.GetMessageBoard()
	compiled, err := createCompile(execCtx, ses, cw.proc, ses.GetDatabaseName(), false, prepared.Sql, prepared.Sql,
		nil, prepared.PrepareStmt, candidate, nil, false, nil, false, nil, nil, 0, &intent)
	require.Nil(t, compiled)
	require.ErrorIs(t, err, compile.ErrTemporalAccessDrift)
	require.Same(t, oldRequest, execCtx.reqCtx)
	require.Same(t, oldTop, cw.proc.GetTopContext())
	require.Same(t, oldBoard, cw.proc.GetMessageBoard())
}

func TestInitExecuteStmtParamRebuildsAcrossForeignKeyChecksTransitions(t *testing.T) {
	for i, test := range []struct {
		name string
		from int64
		to   int64
	}{
		{name: "disabled to enabled", from: 0, to: 1},
		{name: "enabled to disabled", from: 1, to: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			ses, prepareStmt, cw, execCtx := newPreparedExecuteEnv(t, uint32(120+i))
			defer prepareStmt.Close()

			require.NoError(t, ses.SetSessionSysVar(execCtx.reqCtx, "foreign_key_checks", test.from))
			oldPlan := prepareStmt.PreparePlan
			oldQuery := oldPlan.GetDcl().GetPrepare().GetPlan().GetQuery()
			require.NotNil(t, oldQuery)
			oldQuery.HasForeignKeyAction = true

			require.NoError(t, ses.SetSessionSysVar(execCtx.reqCtx, "foreign_key_checks", test.to))
			_, rebuiltPlan, rebuiltStmt, _, _, err := initExecuteStmtParam(
				execCtx, ses, cw, nil, prepareStmt.Name,
			)
			require.NoError(t, err)
			require.NotNil(t, rebuiltPlan)
			require.NotNil(t, rebuiltStmt)
			require.NotSame(t, oldPlan, prepareStmt.PreparePlan)
		})
	}
}

func TestShouldCachePrepareCompileRejectsIcebergScan(t *testing.T) {
	p := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{
		NodeType: plan.Node_EXTERNAL_SCAN,
		ExternScan: &plan.ExternScan{
			Type:        int32(plan.ExternType_ICEBERG_TB),
			IcebergScan: &plan.IcebergScan{},
		},
	}}}}}

	require.False(t, shouldCachePrepareCompile(p))
}
