// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"maps"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	sqlcompile "github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	ivfflatplan "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/plugin/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type frontendBackSchedulingTestEngine struct {
	engine.Engine
	candidates      engine.QueryCandidates
	resolvedNodes   engine.Nodes
	strictNoMatch   bool
	lastPoolRequest engine.QueryCandidatePoolRequest
}

func (e *frontendBackSchedulingTestEngine) DiscoverQueryCandidates(context.Context) (engine.QueryCandidates, error) {
	return e.candidates, nil
}

func (e *frontendBackSchedulingTestEngine) ResolveQueryCandidatePool(
	_ context.Context,
	_ engine.QueryCandidates,
	request engine.QueryCandidatePoolRequest,
) (engine.ResolvedQueryPool, error) {
	e.lastPoolRequest = request
	if e.strictNoMatch && request.FallbackPolicy == engine.QueryPoolFallbackStrict {
		return engine.ResolvedQueryPool{
			RequestedIdentity: request.RequestedPool,
			Identity:          request.RequestedPool,
			Resolution:        engine.QueryPoolResolutionNoMatch,
		}, nil
	}
	return engine.ResolvedQueryPool{
		Nodes:             e.resolvedNodes,
		RequestedIdentity: request.RequestedPool,
		Identity:          request.RequestedPool,
		Resolution:        engine.QueryPoolResolutionExactLabels,
	}, nil
}

func frontendBackIvfSchedulingQuery() *plan.Query {
	return &plan.Query{
		Nodes: []*plan.Node{{
			NodeType: plan.Node_FUNCTION_SCAN,
			TableDef: &plan.TableDef{
				TblFunc: &plan.TableFunction{Name: ivfflatplan.IVFFLATSearchFuncName},
			},
			IndexReaderParam: &plan.IndexReaderParam{OrigFuncName: "l2_distance"},
		}},
		Steps: []int32{0},
	}
}

func TestBindBackExecSession(t *testing.T) {
	clientSessionID := uuid.New()
	backSessionID := uuid.New()
	clientSession := &Session{
		feSessionImpl: feSessionImpl{uuid: clientSessionID},
		tempTables:    make(map[string]string),
		tempTablesRev: make(map[string]string),
	}
	backSes := &backSession{
		feSessionImpl: feSessionImpl{
			uuid:     backSessionID,
			upstream: clientSession,
		},
	}
	proc := &process.Process{Base: &process.BaseProcess{}}

	bindBackExecSession(proc, backSes)

	require.Same(t, backSes, proc.GetSession())
	require.Equal(t, clientSessionID, proc.Base.SessionInfo.SessionId)
	proc.GetSession().AddTempTable("db1", "tmp1", "real_tmp1")
	realName, ok := clientSession.GetTempTable("db1", "tmp1")
	require.True(t, ok)
	require.Equal(t, "real_tmp1", realName)
}

func TestBackSessionInheritsCNLabels(t *testing.T) {
	ses := newFeatureLimitTestSession(t)
	ses.requestLabel = map[string]string{"account": "tp", "role": "tp"}

	backSes := (&backSession{}).initFeSes(ses, nil, "", nil)
	require.Equal(t, ses.requestLabel, backSes.getCNLabels())

	ses.requestLabel["role"] = "ap"
	require.Equal(t, "tp", backSes.getCNLabels()["role"])
	backSes.getCNLabels()["account"] = "other"
	require.Equal(t, "tp", ses.requestLabel["account"])
}

func TestBackSessionInheritsCompleteSchedulingSnapshot(t *testing.T) {
	ses := newFeatureLimitTestSession(t)
	ses.requestLabel = map[string]string{"account": "tp", "role": "tp"}
	ses.SetQueryInProgress(true)
	intent := schedule.SchedulingIntent{
		Explicit:          true,
		RequestedPool:     "tenant:3:tenant|4:role=2:tp",
		PoolFallback:      schedule.PoolFallbackStrict,
		EmptyWorkerPolicy: schedule.EmptyWorkerFail,
		CurrentCNPolicy:   schedule.CurrentCNRequired,
		WorkerSet: schedule.WorkerSetPolicy{
			Mode:             schedule.WorkerSetMax,
			MaxWorkers:       2,
			SelectionKey:     "parent-statement",
			AlgorithmVersion: schedule.WorkerSelectionAlgorithmV1,
		},
	}
	ses.proc.Base.SessionInfo.QuerySchedulingIntent = intent

	backSes := (&backSession{}).initFeSes(ses, nil, "", nil)
	require.True(t, backSes.hasRoutingIntent)
	require.Equal(t, intent, backSes.routingIntent)

	// A generated child SQL hint must not replace the parent statement's
	// snapshot. The routing policy belongs to the initiating statement, not
	// to the generated SQL text.
	childSQL := "select /*+ SET_VAR(query_pool_strict=off) SET_VAR(query_max_workers=99) */ 1"
	require.Equal(t, intent, querySchedulingIntentForStatement(backSes, childSQL))
	require.Equal(t, intent, querySchedulingIntentForStatementWithSQLMode(backSes, childSQL, ""))

	child := (&backSession{}).initFeSes(backSes, nil, "", nil)
	require.True(t, child.hasRoutingIntent)
	require.Equal(t, intent, child.routingIntent)

	ses.requestLabel["role"] = "ap"
	require.Equal(t, "tp", backSes.getCNLabels()["role"])
	require.Equal(t, "tp", child.getCNLabels()["role"])
}

func TestBackSessionSchedulingSnapshotControlsIvfPlacement(t *testing.T) {
	labels := map[string]string{"account": "tp", "role": "tp"}
	intent := schedule.SchedulingIntent{
		Explicit:          true,
		PoolFallback:      schedule.PoolFallbackStrict,
		EmptyWorkerPolicy: schedule.EmptyWorkerFail,
		WorkerSet: schedule.WorkerSetPolicy{
			Mode:             schedule.WorkerSetMax,
			MaxWorkers:       2,
			SelectionKey:     "parent-statement",
			AlgorithmVersion: schedule.WorkerSelectionAlgorithmV1,
		},
	}

	for _, tc := range []struct {
		name            string
		strictNoMatch   bool
		wantReason      string
		wantSatisfied   bool
		wantSelectedIDs []string
	}{
		{
			name:          "strict pool does not widen to shared CN",
			strictNoMatch: true,
			wantReason:    schedule.ReasonNoCandidateCN,
			wantSatisfied: false,
		},
		{
			name:            "worker cap selects only parent pool CNs",
			wantReason:      schedule.ReasonMultiCN,
			wantSatisfied:   true,
			wantSelectedIDs: []string{"cn-a", "cn-c"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ses := newFeatureLimitTestSession(t)
			ses.requestLabel = maps.Clone(labels)
			ses.SetQueryInProgress(true)
			ses.proc.Base.SessionInfo.QuerySchedulingIntent = intent
			backSes := (&backSession{}).initFeSes(ses, nil, "", nil)

			provider := &frontendBackSchedulingTestEngine{
				candidates: engine.QueryCandidates{
					{Service: metadata.CNService{ServiceID: "cn-a", PipelineServiceAddress: "cn-a:6001"}, Mcpu: 4},
					{Service: metadata.CNService{ServiceID: "cn-b", PipelineServiceAddress: "cn-b:6001"}, Mcpu: 4},
					{Service: metadata.CNService{ServiceID: "cn-c", PipelineServiceAddress: "cn-c:6001"}, Mcpu: 4},
				},
				resolvedNodes: engine.Nodes{
					{Id: "cn-a", Addr: "cn-a:6001", Mcpu: 4},
					{Id: "cn-c", Addr: "cn-c:6001", Mcpu: 4},
				},
				strictNoMatch: tc.strictNoMatch,
			}

			process := testutil.NewProcess(t)
			childIntent := querySchedulingIntentForStatement(
				backSes,
				"select /*+ SET_VAR(query_pool_strict=off) SET_VAR(query_max_workers=99) */ 1",
			)
			trace := sqlcompile.PreviewQueryScheduling(sqlcompile.SchedulingPreviewRequest{
				Query:    frontendBackIvfSchedulingQuery(),
				Engine:   provider,
				Process:  process,
				Address:  "local:6001",
				Tenant:   "tenant",
				Username: "user",
				CNLabel:  backSes.getCNLabels(),
				Intent:   childIntent,
			})

			require.Len(t, trace.Attempts, 1)
			require.NotNil(t, trace.Attempts[0].Query)
			query := trace.Attempts[0].Query
			require.Equal(t, tc.wantReason, query.Reason)
			require.Equal(t, tc.wantSatisfied, query.Satisfied)
			require.Equal(t, "strict", query.PoolFallbackPolicy)
			require.Equal(t, "max-workers", query.WorkerSetMode)
			require.Equal(t, 2, query.MaxWorkers)
			require.Equal(t, engine.QueryPoolFallbackStrict, provider.lastPoolRequest.FallbackPolicy)
			require.Equal(t, labels, provider.lastPoolRequest.CNLabel)
			if tc.wantSatisfied {
				selectedIDs := make([]string, 0, len(query.Selected))
				for _, worker := range query.Selected {
					selectedIDs = append(selectedIDs, worker.ID)
				}
				require.ElementsMatch(t, tc.wantSelectedIDs, selectedIDs)
				require.Len(t, selectedIDs, 2)
			}
		})
	}
}

func TestBackSessionInheritsForeignKeyChecks(t *testing.T) {
	ctx := context.Background()
	ses := newFeatureLimitTestSession(t)
	require.NoError(t, ses.SetSessionSysVar(ctx, "foreign_key_checks", int64(0)))
	backSes := &backSession{}
	backSes.upstream = ses

	value, err := backSes.GetSessionSysVar("foreign_key_checks")
	require.NoError(t, err)
	require.Equal(t, int8(0), value)
}

func TestBackSessionInheritsSchedulingSysVars(t *testing.T) {
	ctx := context.Background()
	ses := newFeatureLimitTestSession(t)
	require.NoError(t, ses.SetSessionSysVar(ctx, queryPoolStrict, int64(1)))
	require.NoError(t, ses.SetSessionSysVar(ctx, queryMaxWorkers, int64(3)))

	backSes := (&backSession{}).initFeSes(ses, nil, "", nil)
	backSes.upstream = ses

	strict, err := backSes.GetSessionSysVar(queryPoolStrict)
	require.NoError(t, err)
	require.Equal(t, int8(1), strict)
	maxWorkers, err := backSes.GetSessionSysVar(queryMaxWorkers)
	require.NoError(t, err)
	require.Equal(t, int64(3), maxWorkers)

	intent := querySchedulingIntent(backSes)
	require.True(t, intent.Explicit)
	require.Equal(t, schedule.PoolFallbackStrict, intent.PoolFallback)
	require.Equal(t, schedule.EmptyWorkerFail, intent.EmptyWorkerPolicy)
	require.Equal(t, schedule.WorkerSetMax, intent.WorkerSet.Mode)
	require.Equal(t, 3, intent.WorkerSet.MaxWorkers)
}

func TestBackSessionPropagatesSnapshotThroughDelegatedExec(t *testing.T) {
	ses := newFeatureLimitTestSession(t)
	ses.requestLabel = map[string]string{"account": "tp", "role": "tp"}
	ses.SetQueryInProgress(true)
	intent := schedule.SchedulingIntent{
		Explicit:          true,
		PoolFallback:      schedule.PoolFallbackStrict,
		EmptyWorkerPolicy: schedule.EmptyWorkerFail,
		CurrentCNPolicy:   schedule.CurrentCNRequired,
		WorkerSet: schedule.WorkerSetPolicy{
			Mode:             schedule.WorkerSetMax,
			MaxWorkers:       2,
			SelectionKey:     "parent-statement",
			AlgorithmVersion: schedule.WorkerSelectionAlgorithmV1,
		},
	}
	ses.proc.Base.SessionInfo.QuerySchedulingIntent = intent

	parent := (&backSession{}).initFeSes(ses, nil, "", nil)
	parent.upstream = ses
	delegated := parent.InitBackExec(nil, "", nil)
	defer delegated.Close()

	nested, ok := delegated.(*backExec)
	require.True(t, ok)
	require.Equal(t, parent.getCNLabels(), nested.backSes.getCNLabels())
	require.Equal(t, intent, nested.backSes.routingIntent)
	require.True(t, nested.backSes.hasRoutingIntent)
}

func TestBindBackExecSessionWithoutUpstream(t *testing.T) {
	backSessionID := uuid.New()
	backSes := &backSession{
		feSessionImpl: feSessionImpl{uuid: backSessionID},
	}
	proc := &process.Process{Base: &process.BaseProcess{}}

	bindBackExecSession(proc, backSes)

	require.Nil(t, proc.GetSession())
	require.Equal(t, uuid.Nil, proc.Base.SessionInfo.SessionId)
}

func TestExecInFrontendInBackRequiresUpstreamForPreparedStatements(t *testing.T) {
	ctx := context.Background()
	varExpr := tree.NewVarExpr("sql", false, false, nil)
	prepareVar := tree.NewPrepareVar("stmt", varExpr)

	tests := []struct {
		name string
		stmt tree.Statement
	}{
		{name: "prepare statement", stmt: tree.NewPrepareStmt("stmt", &tree.Select{})},
		{name: "prepare string", stmt: tree.NewPrepareString("stmt", "select 1")},
		{name: "prepare variable", stmt: prepareVar},
		{name: "deallocate", stmt: tree.NewDeallocate("stmt", false)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer test.stmt.Free()
			backSes := &backSession{}
			execCtx := &ExecCtx{reqCtx: ctx, ses: backSes, stmt: test.stmt}

			err := execInFrontendInBack(backSes, execCtx)

			require.ErrorContains(t, err, "requires an upstream session")
		})
	}
}

func TestInstallBackExecStatsInfoPreservesRootAndClaimsOnce(t *testing.T) {
	root := resource.NewRoot(resource.ConnInternal)
	parent := resource.ContextWithRoot(context.Background(), root)
	start := time.Unix(123, 456)
	duration := 17 * time.Millisecond

	firstCtx, firstStats := installBackExecStatsInfo(parent, start, duration)
	secondCtx, secondStats := installBackExecStatsInfo(parent, start, duration)

	if firstStats == secondStats {
		t.Fatal("successive substatements must use distinct StatsInfo values")
	}
	if resource.RootFromContext(firstCtx) != root || resource.RootFromContext(secondCtx) != root {
		t.Fatal("substatement context must preserve the parent resource root")
	}
	if statistic.StatsInfoFromContext(firstCtx) != firstStats {
		t.Fatal("first substatement context does not contain its StatsInfo")
	}
	if statistic.StatsInfoFromContext(secondCtx) != secondStats {
		t.Fatal("second substatement context does not contain its StatsInfo")
	}
	if firstStats.ParseStage.ParseStartTime != start || firstStats.ParseStage.ParseDuration != duration {
		t.Fatal("first StatsInfo parse timing was not installed")
	}
	if secondStats.ParseStage.ParseStartTime != start || secondStats.ParseStage.ParseDuration != duration {
		t.Fatal("second StatsInfo parse timing was not installed")
	}
	if _, ok := firstStats.ClaimRootPhaseResource(); !ok {
		t.Fatal("first StatsInfo claim should succeed")
	}
	if _, ok := firstStats.ClaimRootPhaseResource(); ok {
		t.Fatal("first StatsInfo claim should not succeed twice")
	}
	if _, ok := secondStats.ClaimRootPhaseResource(); !ok {
		t.Fatal("second StatsInfo claim should succeed")
	}
	if _, ok := secondStats.ClaimRootPhaseResource(); ok {
		t.Fatal("second StatsInfo claim should not succeed twice")
	}
}

func TestLegacyCompositeStatsProjection(t *testing.T) {
	ctx, _ := installBackExecStatsInfo(context.Background(), time.Unix(0, 0), 7)
	h := &marshalPlanHandler{isInternalSubStmt: true, query: &plan.Query{Nodes: []*plan.Node{{
		NodeType:    plan.Node_TABLE_SCAN,
		TableDef:    &plan.TableDef{Name: "t"},
		AnalyzeInfo: &plan.AnalyzeInfo{TimeConsumed: 123, InputRows: 9, InputSize: 77},
	}}}}
	stats, details := h.Stats(ctx, nil)
	if stats.GetTimeConsumed() != 130 {
		t.Fatalf("legacy engine projection time = %v, want 130", stats.GetTimeConsumed())
	}
	if details.RowsRead != 9 || details.BytesScan != 77 {
		t.Fatalf("legacy engine projection scan stats = (%d,%d), want (9,77)", details.RowsRead, details.BytesScan)
	}
}

func TestTopLevelStatsDoesNotRunLegacyResourceProjection(t *testing.T) {
	ctx, _ := installBackExecStatsInfo(context.Background(), time.Unix(0, 0), 7)
	h := &marshalPlanHandler{query: &plan.Query{Nodes: []*plan.Node{{
		NodeType:    plan.Node_TABLE_SCAN,
		TableDef:    &plan.TableDef{Name: "t"},
		AnalyzeInfo: &plan.AnalyzeInfo{TimeConsumed: 123, InputRows: 9, InputSize: 77},
	}}}}
	stats, details := h.Stats(ctx, nil)
	if stats.GetTimeConsumed() != 0 {
		t.Fatalf("top-level shadow resource projection time = %v, want 0", stats.GetTimeConsumed())
	}
	if details.RowsRead != 9 || details.BytesScan != 77 {
		t.Fatalf("top-level scan stats = (%d,%d), want (9,77)", details.RowsRead, details.BytesScan)
	}
}
