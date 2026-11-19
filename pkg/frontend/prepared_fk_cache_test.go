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

	require.True(t, planReuse(false, nil, preparedPhysicalCompile).reusable)
	require.True(t, planReuse(false, &plan.Plan{}, preparedPhysicalCompile).reusable)
	require.True(t, planReuse(false, makePlan(plan.Query_UPDATE, false), preparedPhysicalCompile).reusable)
	require.True(t, planReuse(false, makePlan(plan.Query_DELETE, false), preparedPhysicalCompile).reusable)

	require.False(t, planReuse(false, makePlan(plan.Query_UPDATE, true), preparedPhysicalCompile).reusable)
	require.False(t, planReuse(false, makePlan(plan.Query_DELETE, true), preparedPhysicalCompile).reusable)
	require.False(t, planReuse(false, makePlan(plan.Query_INSERT, true), preparedPhysicalCompile).reusable)

	require.True(t, checkNodeCanCache(makePlan(plan.Query_INSERT, false)))
	require.False(t, checkNodeCanCache(makePlan(plan.Query_INSERT, true)))

	require.False(t, !planReuse(false, nil, preparedLogicalPlan).reusable)
	require.False(t, !planReuse(false, makePlan(plan.Query_INSERT, false), preparedLogicalPlan).reusable)
	require.True(t, !planReuse(false, makePlan(plan.Query_INSERT, true), preparedLogicalPlan).reusable)
	require.True(t, !planReuse(true, makePlan(plan.Query_INSERT, false), preparedLogicalPlan).reusable)

	subscriptionMetadataPlan := makePlan(plan.Query_SELECT, false)
	subscriptionMetadataPlan.GetQuery().Nodes = []*plan.Node{{
		ViewPath: []*plan.ViewStep{{DatabaseName: "information_schema", ViewName: "statistics", Snapshot: &plan.Snapshot{Tenant: &plan.SnapshotTenant{}}}},
	}}
	require.True(t, !planReuse(false, subscriptionMetadataPlan, preparedLogicalPlan).reusable)
	require.False(t, checkNodeCanCache(subscriptionMetadataPlan))
}

func TestExecutionDependentAccessRejectsOrdinaryCache(t *testing.T) {
	p := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{StmtType: plan.Query_SELECT, ExecutionDependentAccess: true}}}
	require.False(t, checkNodeCanCache(p))
	p.GetQuery().ExecutionDependentAccess = false
	require.True(t, checkNodeCanCache(p))
}

func TestExecutionIntentUsesStatementSQL(t *testing.T) {
	ses, prepareStmt, _, execCtx := newPreparedExecuteEnv(t, 122)
	defer prepareStmt.Close()
	stmt, err := parsers.ParseOne(execCtx.reqCtx, dialect.MYSQL, "select 1 from intent_table", 1)
	require.NoError(t, err)
	defer stmt.Free()
	raw := "select /*+ SET_VAR(query_max_workers=2) */ 1 from intent_table"
	input, intent, ok := planningInputForStatement(ses, ses.GetProc(), raw, "", stmt)
	require.True(t, ok)
	require.NotNil(t, input)
	require.Equal(t, schedule.WorkerSetMax, intent.Placement.Scheduling.WorkerSet.Mode)
	require.Equal(t, 2, intent.Placement.Scheduling.WorkerSet.MaxWorkers)
	proc := ses.GetProc()
	require.True(t, intent.MatchesProcess(proc))
	require.True(t, executionIntentMatchesCompile(&intent, ses, proc, raw, "", stmt))
	require.False(t, executionIntentMatchesCompile(&intent, ses, proc, "select 1 from intent_table", "", stmt), "changed scheduling must rebuild")
	require.False(t, executionIntentMatchesCompile(&intent, ses, proc, "/*+ SIDECAR */ select 1 from intent_table", "", stmt), "changed offload must rebuild")
	require.False(t, executionIntentMatchesCompile(nil, ses, proc, raw, "", stmt))
	oldZone, oldTime := proc.GetSessionInfo().TimeZone, proc.Base.UnixTime
	defer func() { proc.GetSessionInfo().TimeZone, proc.Base.UnixTime = oldZone, oldTime }()
	proc.GetSessionInfo().TimeZone = time.FixedZone("other", 3600)
	require.False(t, intent.MatchesProcess(proc), "zone drift must invalidate the sample")
	proc.GetSessionInfo().TimeZone = oldZone
	proc.Base.UnixTime++
	require.False(t, intent.MatchesProcess(proc), "clock drift must invalidate the sample")
	proc.Base.UnixTime = oldTime
	_, intent, ok = planningInputForStatement(ses, ses.GetProc(), "/*+ SIDECAR */ select 1 from intent_table", "", stmt)
	require.True(t, ok)
	require.True(t, intent.Placement.Sirius)
	_, _, ok = planningInputForStatement(ses, ses.GetProc(), "", "", stmt)
	require.False(t, ok, "missing raw statement SQL cannot authorize a candidate")
	parameterized, err := parsers.ParseOne(execCtx.reqCtx, dialect.MYSQL, "select ?", 1)
	require.NoError(t, err)
	defer parameterized.Free()
	_, _, ok = planningInputForStatement(ses, ses.GetProc(), "select ?", "", parameterized)
	require.False(t, ok)
	require.False(t, executionIntentMatchesCompile(&intent, ses, ses.GetProc(), "select ?", "", parameterized), "compile independently rejects parameterized roots")
}

func TestExecutionIntentForExplain(t *testing.T) {
	ses, prepared, _, execCtx := newPreparedExecuteEnv(t, 125)
	defer prepared.Close()
	for _, tc := range []struct {
		sql      string
		eligible bool
	}{
		{"explain select 1 from intent_table", true},
		{"explain select ?", false},
		{"explain execute missing", false},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(execCtx.reqCtx, dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			input, _, ok := planningInputForStatement(ses, ses.GetProc(), tc.sql, "", stmt)
			require.Equal(t, tc.eligible, ok)
			if ok {
				require.NotNil(t, input)
				_, _, ok = planningInputForStatement(ses, ses.GetProc(), "", "", stmt)
				require.False(t, ok, "missing statement-local SQL cannot grant a scope")
			}
		})
	}
}

func TestTemporalPlanRebuildsOnCompileIntentDrift(t *testing.T) {
	ses, prepared, _, execCtx := newPreparedExecuteEnv(t, 123)
	defer prepared.Close()
	stmt, err := parsers.ParseOne(execCtx.reqCtx, dialect.MYSQL, "select 1 from intent_table", 1)
	require.NoError(t, err)
	defer stmt.Free()
	const sql = "select /*+ SET_VAR(query_max_workers=2) */ 1 from intent_table"
	_, intent, ok := planningInputForStatement(ses, ses.GetProc(), sql, "", stmt)
	require.True(t, ok)
	marked := &plan.Expr{CandidateFilter: true}
	candidate := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{IndexScanInfo: plan.IndexScanInfo{RequiresCapturedExecution: true}, FilterList: []*plan.Expr{marked}}}}}}
	safe := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{}}}
	builds := 0
	rebuild := func() (*plan2.Plan, error) { builds++; return safe, nil }
	got, validated, err := validateCapturedExecutionBeforeCompile(candidate, &intent, ses, ses.GetProc(), sql, "", stmt, rebuild)
	require.NoError(t, err)
	require.Same(t, candidate, got)
	require.True(t, validated)
	require.Zero(t, builds)
	got, validated, err = validateCapturedExecutionBeforeCompile(candidate, &intent, ses, ses.GetProc(), "select 1 from intent_table", "", stmt, rebuild)
	require.NoError(t, err)
	require.Same(t, safe, got)
	require.False(t, validated)
	require.Equal(t, 1, builds)
	got, validated, err = validateCapturedExecutionBeforeCompile(safe, nil, ses, ses.GetProc(), "", "", stmt, rebuild)
	require.NoError(t, err)
	require.Same(t, safe, got)
	require.False(t, validated)
	require.Equal(t, 1, builds, "candidate-free plans keep their existing scheduling path")
	_, _, err = validateCapturedExecutionBeforeCompile(candidate, nil, ses, ses.GetProc(), sql, "", stmt,
		func() (*plan2.Plan, error) { return candidate, nil })
	require.Error(t, err, "an unscoped rebuild must not return a candidate")
}

func TestTemporalSafeRebuildDropsStaleSnapshot(t *testing.T) {
	_, prepared, cw, execCtx := newPreparedExecuteEnv(t, 124)
	defer prepared.Close()
	cw.planGenerationReused = true
	cw.setPlanSnapshotTS(timestamp.Timestamp{PhysicalTime: 123})
	rebuilt, err := cw.rebuildWithoutCapturedAccess(execCtx)
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

func TestCreateCompileTemporalDriftPreservesRequestState(t *testing.T) {
	ses, prepared, cw, execCtx := newPreparedExecuteEnv(t, 126)
	defer prepared.Close()
	const statementSQL = "select 1 from intent_table"
	statement, err := parsers.ParseOne(execCtx.reqCtx, dialect.MYSQL, statementSQL, 1)
	require.NoError(t, err)
	defer statement.Free()
	_, intent, ok := planningInputForStatement(ses, cw.proc, statementSQL, "", statement)
	require.True(t, ok)
	intent.UnixTime++
	marked := &plan.Expr{CandidateFilter: true}
	candidate := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{IndexScanInfo: plan.IndexScanInfo{RequiresCapturedExecution: true}, FilterList: []*plan.Expr{marked}}}}}}
	top, cancel := context.WithCancel(context.Background())
	defer cancel()
	cw.proc.ReplaceTopCtx(top)
	cw.proc.SetMessageBoard(message.NewMessageBoard())
	oldRequest, oldTop, oldBoard := execCtx.reqCtx, cw.proc.GetTopContext(), cw.proc.GetMessageBoard()
	compiled, err := createCompile(execCtx, ses, cw.proc, ses.GetDatabaseName(), false, statementSQL, statementSQL,
		nil, statement, candidate, nil, false, nil, false, nil, nil, 0, &intent)
	require.Nil(t, compiled)
	require.ErrorIs(t, err, compile.ErrExecutionIntentDrift)
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

	require.False(t, planReuse(false, p, preparedPhysicalCompile).reusable)
}
