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
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestTemporalAccessDriftSentinelIdentity(t *testing.T) {
	require.ErrorIs(t, ErrTemporalAccessDrift, ErrTemporalAccessDrift)
	require.ErrorIs(t, fmt.Errorf("compile: %w", ErrTemporalAccessDrift), ErrTemporalAccessDrift)
	require.NotErrorIs(t, moerr.NewInternalErrorNoCtx("different internal error"), ErrTemporalAccessDrift)
}

func TestTemporalCandidatePermissionRequiresProvenanceAndCopy(t *testing.T) {
	helper := func() *plan.Expr {
		return &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(function.TEMPORAL_DATETIME_BOUND, 0)},
			Args: []*plan.Expr{nil, nil, nil, nil, plan2.MakePlan2BoolConstExprWithType(false)},
		}}}
	}
	marked := helper()
	marked.TemporalCandidateFilter = true
	unmarked := helper()
	rowFilter := helper()
	markedRow := plan2.DeepCopyExpr(marked)
	template := &plan.Query{Nodes: []*plan.Node{{BlockFilterList: []*plan.Expr{marked, unmarked}, FilterList: []*plan.Expr{rowFilter, markedRow}}}}
	execution := plan2.DeepCopyQuery(template)
	setTemporalCandidateNarrowing(execution, true)
	getPermission := func(expr *plan.Expr) bool { return expr.GetF().Args[4].GetLit().GetBval() }
	require.True(t, getPermission(execution.Nodes[0].BlockFilterList[0]))
	require.False(t, getPermission(execution.Nodes[0].BlockFilterList[1]))
	require.False(t, getPermission(execution.Nodes[0].FilterList[0]))
	require.True(t, getPermission(execution.Nodes[0].FilterList[1]))
	require.False(t, getPermission(template.Nodes[0].BlockFilterList[0]), "execution must not mutate the reusable template")
	require.False(t, getPermission(template.Nodes[0].FilterList[1]))
	setTemporalCandidateNarrowing(execution, false)
	require.False(t, getPermission(execution.Nodes[0].BlockFilterList[0]))
	require.False(t, getPermission(execution.Nodes[0].FilterList[1]))

	proc := testutil.NewProcess(t)
	defer proc.Free()
	templatePlan := &plan.Plan{Plan: &plan.Plan_Query{Query: template}}
	proc.GetSessionInfo().TimeZone = time.FixedZone("test", 1234)
	fixed := prepareFixedZoneTemporalExecutionPlan(templatePlan, proc, true)
	require.NotSame(t, templatePlan, fixed)
	require.True(t, getPermission(fixed.GetQuery().Nodes[0].BlockFilterList[0]))
	require.False(t, getPermission(templatePlan.GetQuery().Nodes[0].BlockFilterList[0]))
	rowOnly := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{FilterList: []*plan.Expr{plan2.DeepCopyExpr(markedRow)}}}}}}
	rowExecution := prepareFixedZoneTemporalExecutionPlan(rowOnly, proc, true)
	require.NotSame(t, rowOnly, rowExecution)
	require.True(t, getPermission(rowExecution.GetQuery().Nodes[0].FilterList[0]))
	require.False(t, getPermission(rowOnly.GetQuery().Nodes[0].FilterList[0]))
	c := &Compile{proc: proc, addr: "local:6001"}
	intent := &plan2.TemporalPlanningIntent{
		TimeZone: proc.GetSessionInfo().TimeZone, UnixTime: proc.Base.UnixTime,
		Placement: schedule.TemporalAccessIntent{CurrentCN: schedule.Worker{
			ID: proc.GetService(), Addr: c.addr, Route: schedule.WorkerRouteLocal,
		}},
	}
	require.False(t, getPermission(c.prepareTemporalExecutionPlan(context.Background(), templatePlan).GetQuery().Nodes[0].BlockFilterList[0]))
	c.SetValidatedTemporalAccessIntent(intent)
	require.True(t, getPermission(c.prepareTemporalExecutionPlan(context.Background(), templatePlan).GetQuery().Nodes[0].BlockFilterList[0]))
	require.False(t, getPermission(c.prepareTemporalExecutionPlan(WithSiriusOffload(context.Background()), templatePlan).GetQuery().Nodes[0].BlockFilterList[0]))
	c.querySchedulingIntent.Explicit = true
	require.False(t, getPermission(c.prepareTemporalExecutionPlan(context.Background(), templatePlan).GetQuery().Nodes[0].BlockFilterList[0]))
	c.querySchedulingIntent.Explicit = false
	c.temporalCandidateTopology = true
	require.False(t, c.temporalRetryNeedsRebuild())
	proc.Base.UnixTime++
	require.False(t, getPermission(c.prepareTemporalExecutionPlan(context.Background(), templatePlan).GetQuery().Nodes[0].BlockFilterList[0]))
	require.True(t, c.temporalRetryNeedsRebuild(), "retry must replan rather than scan the full index after clock drift")
	c.temporalCandidateTopology = false
	require.False(t, c.temporalRetryNeedsRebuild(), "a published safe generation must not keep forcing temporal rebuilds")
	proc.Base.UnixTime--
	c.pn = templatePlan
	require.ErrorIs(t, c.Reset(nil, time.Time{}, nil, ""), cantCompileForPrepareErr, "candidate topology cannot be reused across executions")
	require.Nil(t, c.temporalAccessIntent)
	folded := plan2.MakePlan2BoolConstExprWithType(true)
	folded.TemporalCandidateFilter = true
	c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{FilterList: []*plan.Expr{folded}}}}}}
	require.ErrorIs(t, c.Reset(nil, time.Time{}, nil, ""), cantCompileForPrepareErr, "folding must not hide candidate ownership")
	c.pn = nil
	c.temporalCandidateTopology = true
	require.ErrorIs(t, c.Reset(nil, time.Time{}, nil, ""), cantCompileForPrepareErr, "the topology remembers ownership even if rewrites remove the filter")
	zone, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	proc.GetSessionInfo().TimeZone = zone
	named := prepareFixedZoneTemporalExecutionPlan(templatePlan, proc, true)
	require.False(t, getPermission(named.GetQuery().Nodes[0].BlockFilterList[0]))
}

func TestTemporalPreflightDiscardPreservesProcess(t *testing.T) {
	ctrl := gomock.NewController(t)
	workspace := mock_frontend.NewMockWorkspace(ctrl)
	workspace.EXPECT().UpdateSnapshotWriteOffset().Times(0)
	txn := mock_frontend.NewMockTxnOperator(ctrl)
	txn.EXPECT().GetWorkspace().Return(workspace).AnyTimes()
	proc := testutil.NewProcess(t)
	proc.Base.TxnOperator = txn
	defer proc.Free()
	board := message.NewMessageBoard()
	proc.SetMessageBoard(board)
	params := vector.NewVec(types.T_text.ToType())
	defer params.Free(proc.Mp())
	proc.SetPrepareParams(params)
	defer proc.SetPrepareParams(nil)
	originalSnapshot := timestamp.Timestamp{PhysicalTime: 123}
	proc.SetPlanSnapshotTS(originalSnapshot)
	labels := map[string]string{"pool": "original"}
	c := NewTemporalCompile("local:6001", "", "select 1", "", "", nil, proc, nil, false, labels, time.Time{})
	require.Same(t, board, proc.GetMessageBoard())
	c.SetPlanSnapshotTS(timestamp.Timestamp{PhysicalTime: 456})
	actualSnapshot, ok := proc.GetPlanSnapshotTS()
	require.True(t, ok)
	require.Equal(t, originalSnapshot, actualSnapshot)
	c.SetValidatedTemporalAccessIntent(&plan2.TemporalPlanningIntent{TimeZone: proc.GetSessionInfo().TimeZone, UnixTime: proc.Base.UnixTime + 1})
	marked := &plan.Expr{TemporalCandidateFilter: true}
	p := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{FilterList: []*plan.Expr{marked}}}}}}
	require.ErrorIs(t, c.Compile(context.Background(), p, nil), ErrTemporalAccessDrift)
	c.DiscardTemporalPreflight()
	require.Same(t, board, proc.GetMessageBoard())
	require.Same(t, params, proc.GetPrepareParams())
	require.Equal(t, map[string]string{"pool": "original"}, labels)
	actualSnapshot, ok = proc.GetPlanSnapshotTS()
	require.True(t, ok)
	require.Equal(t, originalSnapshot, actualSnapshot)
}

func TestTemporalPreflightDiscardPreservesDependentOwner(t *testing.T) {
	parent := testutil.NewProcess(t)
	defer parent.Free()
	budget := process.MustNewExecutionResourceBudget(1<<20, 1<<20)
	defer budget.Close()
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	defer generation.Close()
	require.NoError(t, parent.SetExecutionResourceBudgetForTesting(generation))
	slot, err := vector.NewConstFixed(types.T_int64.ToType(), int64(7), 1, parent.Mp())
	require.NoError(t, err)
	defer slot.Free(parent.Mp())
	child, err := parent.NewDependentProcess(1, []*vector.Vector{slot}, 1)
	require.NoError(t, err)
	defer child.CloseDependentProcess()
	board, used := child.GetMessageBoard(), generation.Used()
	c := NewTemporalCompile("local:6001", "", "select 1", "", "", nil, child, nil, false, nil, time.Time{})
	c.SetValidatedTemporalAccessIntent(&plan2.TemporalPlanningIntent{TimeZone: child.GetSessionInfo().TimeZone, UnixTime: child.Base.UnixTime + 1})
	marked := &plan.Expr{TemporalCandidateFilter: true}
	p := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{FilterList: []*plan.Expr{marked}}}}}}
	require.ErrorIs(t, c.Compile(context.Background(), p, nil), ErrTemporalAccessDrift)
	c.DiscardTemporalPreflight()
	require.Same(t, board, child.GetMessageBoard())
	require.Equal(t, used, generation.Used())
	got, err := child.DependentSlot(1, 0)
	require.NoError(t, err)
	require.Same(t, slot, got)
}

func TestTemporalPreflightWorkspacePanicDoesNotPublishBoard(t *testing.T) {
	ctrl := gomock.NewController(t)
	workspace := mock_frontend.NewMockWorkspace(ctrl)
	workspace.EXPECT().UpdateSnapshotWriteOffset().Do(func() { panic("workspace failure") })
	txn := mock_frontend.NewMockTxnOperator(ctrl)
	txn.EXPECT().GetWorkspace().Return(workspace).AnyTimes()
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.Base.TxnOperator = txn
	proc.GetSessionInfo().TimeZone = time.UTC
	board := message.NewMessageBoard()
	proc.SetMessageBoard(board)
	c := NewTemporalCompile("local:6001", "", "select 1", "", "", nil, proc, nil, false, nil, time.Time{})
	c.SetValidatedTemporalAccessIntent(&plan2.TemporalPlanningIntent{TimeZone: proc.GetSessionInfo().TimeZone, UnixTime: proc.Base.UnixTime,
		Placement: schedule.TemporalAccessIntent{CurrentCN: schedule.Worker{ID: proc.GetService(), Addr: c.addr, Route: schedule.WorkerRouteLocal}}})
	marked := &plan.Expr{TemporalCandidateFilter: true}
	p := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{FilterList: []*plan.Expr{marked}}}}}}
	require.Error(t, c.Compile(context.Background(), p, nil))
	c.Release()
	require.Same(t, board, proc.GetMessageBoard())
}

func TestTemporalRetryDriftUsesSafeBuilderAndPreservesParamsOnFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	workspace := mock_frontend.NewMockWorkspace(ctrl)
	workspace.EXPECT().UpdateSnapshotWriteOffset().Times(3)
	workspace.EXPECT().GetSnapshotWriteOffset().Return(3).Times(3)
	txn := mock_frontend.NewMockTxnOperator(ctrl)
	txn.EXPECT().GetWorkspace().Return(workspace).AnyTimes()
	proc := testutil.NewProcess(t)
	proc.Base.TxnOperator = txn
	proc.ReplaceTopCtx(context.Background())
	params := vector.NewVec(types.T_text.ToType())
	defer params.Free(proc.Mp())
	proc.SetPrepareParams(params)
	labels := map[string]string{"pool": "original"}
	c := NewCompile("local:6001", "", "select 1", "", "", nil, proc, nil, false, labels, time.Time{})
	defer c.Release()
	board := proc.GetMessageBoard()
	intent := &plan2.TemporalPlanningIntent{TimeZone: proc.GetSessionInfo().TimeZone, UnixTime: proc.Base.UnixTime,
		Placement: schedule.TemporalAccessIntent{CurrentCN: schedule.Worker{ID: proc.GetService(), Addr: c.addr, Route: schedule.WorkerRouteLocal}}}
	c.SetValidatedTemporalAccessIntent(intent)
	c.temporalCandidateTopology = true
	marked := &plan.Expr{TemporalCandidateFilter: true}
	candidate := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{FilterList: []*plan.Expr{marked}}}}}}
	c.pn = candidate
	regularBuilds, safeBuilds := 0, 0
	c.SetBuildPlanFunc(func(context.Context) (*plan.Plan, error) {
		regularBuilds++
		proc.Base.UnixTime++
		return candidate, nil
	})
	want := moerr.NewInternalError(context.Background(), "safe rebuild failed")
	c.SetBuildSafeTemporalPlanFunc(func(context.Context) (*plan.Plan, error) {
		safeBuilds++
		return nil, want
	})
	retry, err := c.buildRetryCompile(true)
	require.Nil(t, retry)
	require.ErrorIs(t, err, want)
	require.Equal(t, 1, regularBuilds)
	require.Equal(t, 1, safeBuilds)
	require.Same(t, params, proc.GetPrepareParams())
	require.Same(t, board, proc.GetMessageBoard())
	require.Equal(t, map[string]string{"pool": "original"}, labels)
	retry, err = c.buildRetryCompile(false)
	require.Nil(t, retry)
	require.ErrorIs(t, err, want)
	require.Equal(t, 1, regularBuilds, "already stale retry must use the authorized safe builder directly")
	require.Equal(t, 2, safeBuilds)
	require.Same(t, params, proc.GetPrepareParams())
}

func TestTemporalRetryBuildPanicPreservesCallerResources(t *testing.T) {
	ctrl := gomock.NewController(t)
	workspace := mock_frontend.NewMockWorkspace(ctrl)
	workspace.EXPECT().UpdateSnapshotWriteOffset().Times(2)
	workspace.EXPECT().GetSnapshotWriteOffset().Return(3).Times(2)
	txn := mock_frontend.NewMockTxnOperator(ctrl)
	txn.EXPECT().GetWorkspace().Return(workspace).AnyTimes()
	proc := testutil.NewProcess(t)
	proc.Base.TxnOperator = txn
	proc.ReplaceTopCtx(context.Background())
	params := vector.NewVec(types.T_text.ToType())
	defer params.Free(proc.Mp())
	proc.SetPrepareParams(params)
	labels := map[string]string{"pool": "original"}
	c := NewCompile("local:6001", "", "select 1", "", "", nil, proc, nil, false, labels, time.Time{})
	defer c.Release()
	board := proc.GetMessageBoard()
	c.SetValidatedTemporalAccessIntent(&plan2.TemporalPlanningIntent{TimeZone: proc.GetSessionInfo().TimeZone, UnixTime: proc.Base.UnixTime,
		Placement: schedule.TemporalAccessIntent{CurrentCN: schedule.Worker{ID: proc.GetService(), Addr: c.addr, Route: schedule.WorkerRouteLocal}}})
	want := &struct{ reason string }{"plan build failed"}
	c.SetBuildPlanFunc(func(context.Context) (*plan.Plan, error) { panic(want) })
	require.PanicsWithValue(t, want, func() { _, _ = c.buildRetryCompile(true) })
	require.Same(t, params, proc.GetPrepareParams())
	require.Same(t, board, proc.GetMessageBoard())
	require.Equal(t, map[string]string{"pool": "original"}, labels)
}

func TestTemporalRetryPublishesSafeGeneration(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.ReplaceTopCtx(context.Background())
	c := NewCompile("local:6001", "", "select 1", "", "", nil, proc, nil, false, nil, time.Time{})
	defer c.Release()
	c.SetValidatedTemporalAccessIntent(&plan2.TemporalPlanningIntent{TimeZone: proc.GetSessionInfo().TimeZone, UnixTime: proc.Base.UnixTime,
		Placement: schedule.TemporalAccessIntent{CurrentCN: schedule.Worker{ID: proc.GetService(), Addr: c.addr, Route: schedule.WorkerRouteLocal}}})
	c.temporalCandidateTopology = true
	marked := &plan.Expr{TemporalCandidateFilter: true}
	candidate := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{FilterList: []*plan.Expr{marked}}}}}}
	c.pn = candidate
	c.SetBuildPlanFunc(func(context.Context) (*plan.Plan, error) {
		proc.Base.UnixTime++
		return candidate, nil
	})
	safe := &plan.Plan{Plan: &plan.Plan_Ddl{Ddl: &plan.DataDefinition{DdlType: plan.DataDefinition_CREATE_TABLE}}}
	c.SetBuildSafeTemporalPlanFunc(func(context.Context) (*plan.Plan, error) { return safe, nil })
	retry, err := c.buildRetryCompile(true)
	require.NoError(t, err)
	defer releaseRetryCompile(retry)
	require.Same(t, safe, c.pn)
	require.False(t, c.temporalCandidateTopology)
	require.False(t, c.temporalRetryNeedsRebuild(), "the safe generation must not force further temporal rebuilds")
}

func TestTemporalIndexPlacementChecksEveryScan(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.GetSessionInfo().TimeZone = time.UTC
	c := &Compile{proc: proc, addr: "local:6001", ncpu: 4}
	intent := &plan2.TemporalPlanningIntent{TimeZone: time.UTC, UnixTime: proc.Base.UnixTime, Placement: schedule.TemporalAccessIntent{CurrentCN: schedule.Worker{ID: proc.GetService(), Addr: c.addr, Route: schedule.WorkerRouteLocal}}}
	c.SetValidatedTemporalAccessIntent(intent)
	makeScan := func() *plan.Node {
		return &plan.Node{NodeType: plan.Node_TABLE_SCAN, Stats: &plan.Stats{ForceOneCN: true}, IndexScanInfo: plan.IndexScanInfo{IsIndexScan: true}, FilterList: []*plan.Expr{{TemporalCandidateFilter: true}}}
	}
	template := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{makeScan(), makeScan()}, MaxDop: 1}}}
	oldForce := plan2.GetForceScanOnMultiCN()
	t.Cleanup(func() { plan2.SetForceScanOnMultiCN(oldForce) })
	plan2.SetForceScanOnMultiCN(false)
	require.True(t, c.temporalIndexPlacementAllowed(template), "local placement is independent of the cost model fanout")
	template.GetQuery().Nodes[1].Stats.ForceOneCN = false
	require.False(t, c.temporalIndexPlacementAllowed(template), "every marked scan must retain local placement")
	template.GetQuery().Nodes[1].Stats.ForceOneCN = true
	c.ncpu = 5
	require.True(t, c.temporalIndexPlacementAllowed(template), "ordinary estimated cost is not an execution permission")
	c.ncpu = 4
	c.temporalAccessIntent = nil
	require.False(t, c.temporalIndexPlacementAllowed(template))
	c.SetValidatedTemporalAccessIntent(intent)
	plan2.SetForceScanOnMultiCN(true)
	require.False(t, c.temporalIndexPlacementAllowed(template))
	// Successful preflight owns its decision; a later global flip cannot change
	// helper narrowing. compileQuery and scan placement separately retain local.
	plan2.SetForceScanOnMultiCN(false)
	require.True(t, c.temporalIndexPlacementAllowed(template))
	helper := &plan.Expr{TemporalCandidateFilter: true, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(function.TEMPORAL_DATETIME_BOUND, 0)}, Args: []*plan.Expr{nil, nil, nil, nil, plan2.MakePlan2BoolConstExprWithType(false)}}}}
	template.GetQuery().Nodes[0].BlockFilterList = []*plan.Expr{helper}
	plan2.SetForceScanOnMultiCN(true)
	prepared := c.prepareTemporalExecutionPlan(proc.Ctx, template)
	require.True(t, prepared.GetQuery().Nodes[0].BlockFilterList[0].GetF().Args[4].GetLit().GetBval())
	require.False(t, helper.GetF().Args[4].GetLit().GetBval(), "execution never mutates the template")
}
