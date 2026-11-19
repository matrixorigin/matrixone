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
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestCompileSetupRejectsInvalidBeforeResources(t *testing.T) {
	for _, setup := range [][]CompileSetup{{CompileSetup(255)}, {ImmediateSetup, PreflightSetup}} {
		var caught any
		func() {
			defer func() { caught = recover() }()
			// A nil process would fail if setup validation reached allocation or
			// the workspace. The constructor must reject its own invalid input first.
			NewCompile("", "", "", "", "", nil, nil, nil, false, nil, time.Time{}, setup...)
		}()
		cause, ok := caught.(error)
		require.True(t, ok)
		require.True(t, moerr.IsMoErrCode(cause, moerr.ErrInternal))
	}
}

func TestTemporalAccessDriftSentinelIdentity(t *testing.T) {
	require.ErrorIs(t, ErrExecutionIntentDrift, ErrExecutionIntentDrift)
	require.ErrorIs(t, fmt.Errorf("compile: %w", ErrExecutionIntentDrift), ErrExecutionIntentDrift)
	require.NotErrorIs(t, moerr.NewInternalErrorNoCtx("different internal error"), ErrExecutionIntentDrift)
}

func TestTemporalCandidateExecutionFenceAndReuse(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.GetSessionInfo().TimeZone = time.FixedZone("test", 1234)
	c := &Compile{proc: proc, addr: "local:6001"}
	intent := &plan2.ExecutionIntent{
		TimeZone: proc.GetSessionInfo().TimeZone, UnixTime: proc.Base.UnixTime,
		Placement: schedule.LocalAccessIntent{CurrentCN: schedule.Worker{ID: proc.GetService(), Addr: c.addr, Route: schedule.WorkerRouteLocal}},
	}
	marked := &plan.Expr{CandidateFilter: true}
	template := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{
		NodeType: plan.Node_TABLE_SCAN, Stats: &plan.Stats{ForceOneCN: true}, IndexScanInfo: plan.IndexScanInfo{IsIndexScan: true, RequiresCapturedExecution: true}, BlockFilterList: []*plan.Expr{marked},
	}}}}}
	require.False(t, c.capturedExecutionAllowed(context.Background()))
	c.SetValidatedExecutionIntent(intent)
	require.True(t, c.capturedExecutionAllowed(context.Background()))
	require.False(t, c.capturedExecutionAllowed(WithSiriusOffload(context.Background())))
	c.querySchedulingIntent.Explicit = true
	require.False(t, c.capturedExecutionAllowed(context.Background()))
	c.querySchedulingIntent.Explicit = false
	c.capturedExecutionTopology = true
	require.False(t, c.capturedExecutionRetryNeedsRebuild())
	proc.Base.UnixTime++
	require.False(t, c.capturedExecutionAllowed(context.Background()))
	require.True(t, c.capturedExecutionRetryNeedsRebuild())
	c.capturedExecutionTopology = false
	require.False(t, c.capturedExecutionRetryNeedsRebuild())
	proc.Base.UnixTime--
	c.pn = template
	require.ErrorIs(t, c.Reset(nil, time.Time{}, nil, ""), cantCompileForPrepareErr)
	require.Nil(t, c.executionIntent)
	folded := plan2.MakePlan2BoolConstExprWithType(true)
	folded.CandidateFilter = true
	c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{IndexScanInfo: plan.IndexScanInfo{RequiresCapturedExecution: true}, FilterList: []*plan.Expr{folded}}}}}}
	require.ErrorIs(t, c.Reset(nil, time.Time{}, nil, ""), cantCompileForPrepareErr, "folding must not hide candidate ownership")
	c.pn = nil
	c.capturedExecutionTopology = true
	require.ErrorIs(t, c.Reset(nil, time.Time{}, nil, ""), cantCompileForPrepareErr, "topology remains sticky")
	zone, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	proc.GetSessionInfo().TimeZone = zone
	intent.TimeZone = zone
	c.SetValidatedExecutionIntent(intent)
	require.True(t, c.capturedExecutionAllowed(context.Background()), "the generic captured execution contract does not impose a producer's zone rule")
	proc.GetSessionInfo().TimeZone = time.UTC
	require.False(t, c.capturedExecutionAllowed(context.Background()), "a changed zone still invalidates the captured execution")
}

func TestTemporalCandidateWithoutPhysicalIndexOrIntentFailsPreflight(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.GetSessionInfo().TimeZone = time.UTC
	board := message.NewMessageBoard()
	proc.SetMessageBoard(board)
	c := NewCompile("local:6001", "", "select 1", "", "", nil, proc, nil, false, nil, time.Time{}, PreflightSetup)
	marked := &plan.Expr{CandidateFilter: true}
	node := &plan.Node{IndexScanInfo: plan.IndexScanInfo{RequiresCapturedExecution: true}, FilterList: []*plan.Expr{marked}}
	p := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{node}}}}
	require.ErrorIs(t, c.Compile(context.Background(), p, nil), ErrExecutionIntentDrift)
	c.SetValidatedExecutionIntent(&plan2.ExecutionIntent{TimeZone: time.UTC, UnixTime: proc.Base.UnixTime,
		Placement: schedule.LocalAccessIntent{CurrentCN: schedule.Worker{ID: proc.GetService(), Addr: c.addr, Route: schedule.WorkerRouteLocal}}})
	require.ErrorIs(t, c.Compile(context.Background(), p, nil), ErrExecutionIntentDrift)
	node.NodeType = plan.Node_TABLE_SCAN
	node.IndexScanInfo.IsIndexScan = true
	node.Stats = &plan.Stats{ForceOneCN: true}
	c.SetValidatedExecutionIntent(nil)
	require.ErrorIs(t, c.Compile(context.Background(), p, nil), ErrExecutionIntentDrift)
	node.IndexScanInfo.RequiresCapturedExecution = false
	c.capturedExecutionTopology = true
	require.ErrorIs(t, c.Compile(context.Background(), p, nil), ErrExecutionIntentDrift,
		"a retained topology cannot publish after its captured intent is cleared")
	c.DiscardPreflight()
	require.Same(t, board, proc.GetMessageBoard())
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
	c := NewCompile("local:6001", "", "select 1", "", "", nil, proc, nil, false, labels, time.Time{}, PreflightSetup)
	require.Same(t, board, proc.GetMessageBoard())
	c.SetPlanSnapshotTS(timestamp.Timestamp{PhysicalTime: 456})
	actualSnapshot, ok := proc.GetPlanSnapshotTS()
	require.True(t, ok)
	require.Equal(t, originalSnapshot, actualSnapshot)
	c.SetValidatedExecutionIntent(&plan2.ExecutionIntent{TimeZone: proc.GetSessionInfo().TimeZone, UnixTime: proc.Base.UnixTime + 1})
	marked := &plan.Expr{CandidateFilter: true}
	p := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{IndexScanInfo: plan.IndexScanInfo{RequiresCapturedExecution: true}, FilterList: []*plan.Expr{marked}}}}}}
	require.ErrorIs(t, c.Compile(context.Background(), p, nil), ErrExecutionIntentDrift)
	c.capturedExecutionTopology = true
	p.GetQuery().Nodes[0].FilterList = nil
	p.GetQuery().Nodes[0].IndexScanInfo.RequiresCapturedExecution = false
	require.ErrorIs(t, c.Compile(context.Background(), p, nil), ErrExecutionIntentDrift, "a previously published candidate still requires a valid intent")
	c.DiscardPreflight()
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
	c := NewCompile("local:6001", "", "select 1", "", "", nil, child, nil, false, nil, time.Time{}, PreflightSetup)
	c.SetValidatedExecutionIntent(&plan2.ExecutionIntent{TimeZone: child.GetSessionInfo().TimeZone, UnixTime: child.Base.UnixTime + 1})
	marked := &plan.Expr{CandidateFilter: true}
	p := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{IndexScanInfo: plan.IndexScanInfo{RequiresCapturedExecution: true}, FilterList: []*plan.Expr{marked}}}}}}
	require.ErrorIs(t, c.Compile(context.Background(), p, nil), ErrExecutionIntentDrift)
	c.DiscardPreflight()
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
	c := NewCompile("local:6001", "", "select 1", "", "", nil, proc, nil, false, nil, time.Time{}, PreflightSetup)
	c.SetValidatedExecutionIntent(&plan2.ExecutionIntent{TimeZone: proc.GetSessionInfo().TimeZone, UnixTime: proc.Base.UnixTime,
		Placement: schedule.LocalAccessIntent{CurrentCN: schedule.Worker{ID: proc.GetService(), Addr: c.addr, Route: schedule.WorkerRouteLocal}}})
	marked := &plan.Expr{CandidateFilter: true}
	p := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{NodeType: plan.Node_TABLE_SCAN, Stats: &plan.Stats{ForceOneCN: true}, IndexScanInfo: plan.IndexScanInfo{IsIndexScan: true, RequiresCapturedExecution: true}, FilterList: []*plan.Expr{marked}}}}}}
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
	sequenceRetry := proc.GetSequenceRetryState()
	require.NotNil(t, sequenceRetry)
	sequenceRetry.EnsureWatching(process.WatchIdentity{DBName: "db", TblName: "seq", WantRel: 1})
	deadline := sequenceRetry.AdmitRetry(time.Now().Add(time.Minute))
	board := proc.GetMessageBoard()
	intent := &plan2.ExecutionIntent{TimeZone: proc.GetSessionInfo().TimeZone, UnixTime: proc.Base.UnixTime,
		Placement: schedule.LocalAccessIntent{CurrentCN: schedule.Worker{ID: proc.GetService(), Addr: c.addr, Route: schedule.WorkerRouteLocal}}}
	c.SetValidatedExecutionIntent(intent)
	c.capturedExecutionTopology = true
	marked := &plan.Expr{CandidateFilter: true}
	candidate := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{IndexScanInfo: plan.IndexScanInfo{RequiresCapturedExecution: true}, FilterList: []*plan.Expr{marked}}}}}}
	c.pn = candidate
	regularBuilds, safeBuilds := 0, 0
	c.SetBuildPlanFunc(func(context.Context) (*plan.Plan, error) {
		regularBuilds++
		proc.Base.UnixTime++
		return candidate, nil
	})
	want := moerr.NewInternalError(context.Background(), "safe rebuild failed")
	c.SetBuildFallbackPlanFunc(func(context.Context) (*plan.Plan, error) {
		safeBuilds++
		return nil, want
	})
	retry, err := c.buildRetryCompile(true, false, nil)
	require.Nil(t, retry)
	require.ErrorIs(t, err, want)
	require.Equal(t, 1, regularBuilds)
	require.Equal(t, 1, safeBuilds)
	require.Same(t, params, proc.GetPrepareParams())
	require.Same(t, sequenceRetry, proc.GetSequenceRetryState(), "a failed temporal retry still belongs to the same statement")
	gotDeadline, hasDeadline := proc.GetSequenceRetryState().RetryDeadline()
	require.True(t, hasDeadline)
	require.True(t, deadline.Equal(gotDeadline), "retry must not reset the sticky admission window")
	require.Same(t, board, proc.GetMessageBoard())
	require.Equal(t, map[string]string{"pool": "original"}, labels)
	retry, err = c.buildRetryCompile(false, false, nil)
	require.Nil(t, retry)
	require.ErrorIs(t, err, want)
	require.Equal(t, 1, regularBuilds, "already stale retry must use the authorized safe builder directly")
	require.Equal(t, 2, safeBuilds)
	require.Same(t, params, proc.GetPrepareParams())
	require.Same(t, sequenceRetry, proc.GetSequenceRetryState())
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
	c.SetValidatedExecutionIntent(&plan2.ExecutionIntent{TimeZone: proc.GetSessionInfo().TimeZone, UnixTime: proc.Base.UnixTime,
		Placement: schedule.LocalAccessIntent{CurrentCN: schedule.Worker{ID: proc.GetService(), Addr: c.addr, Route: schedule.WorkerRouteLocal}}})
	want := &struct{ reason string }{"plan build failed"}
	c.SetBuildPlanFunc(func(context.Context) (*plan.Plan, error) { panic(want) })
	require.PanicsWithValue(t, want, func() { _, _ = c.buildRetryCompile(true, false, nil) })
	require.Same(t, params, proc.GetPrepareParams())
	require.Same(t, board, proc.GetMessageBoard())
	require.Equal(t, map[string]string{"pool": "original"}, labels)
}

func TestTemporalRetryPublishesSafeGeneration(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.ReplaceTopCtx(context.Background())
	c := NewCompile("local:6001", "", "select 1", "", "", nil, proc, nil, false, nil, time.Time{})
	defer c.Release()
	sequenceRetry := proc.GetSequenceRetryState()
	require.NotNil(t, sequenceRetry)
	sequenceRetry.EnsureWatching(process.WatchIdentity{DBName: "db", TblName: "seq", WantRel: 1})
	deadline := sequenceRetry.AdmitRetry(time.Now().Add(time.Minute))
	c.SetValidatedExecutionIntent(&plan2.ExecutionIntent{TimeZone: proc.GetSessionInfo().TimeZone, UnixTime: proc.Base.UnixTime,
		Placement: schedule.LocalAccessIntent{CurrentCN: schedule.Worker{ID: proc.GetService(), Addr: c.addr, Route: schedule.WorkerRouteLocal}}})
	c.capturedExecutionTopology = true
	marked := &plan.Expr{CandidateFilter: true}
	candidate := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{IndexScanInfo: plan.IndexScanInfo{RequiresCapturedExecution: true}, FilterList: []*plan.Expr{marked}}}}}}
	c.pn = candidate
	c.SetBuildPlanFunc(func(context.Context) (*plan.Plan, error) {
		proc.Base.UnixTime++
		return candidate, nil
	})
	safe := &plan.Plan{Plan: &plan.Plan_Ddl{Ddl: &plan.DataDefinition{DdlType: plan.DataDefinition_CREATE_TABLE}}}
	c.SetBuildFallbackPlanFunc(func(context.Context) (*plan.Plan, error) { return safe, nil })
	retry, err := c.buildRetryCompile(true, false, nil)
	require.NoError(t, err)
	defer releaseRetryCompile(retry)
	require.Same(t, safe, c.pn)
	require.Same(t, sequenceRetry, retry.proc.GetSequenceRetryState())
	gotDeadline, hasDeadline := retry.proc.GetSequenceRetryState().RetryDeadline()
	require.True(t, hasDeadline)
	require.True(t, deadline.Equal(gotDeadline), "the safe generation retains the statement retry budget")
	require.False(t, c.capturedExecutionTopology)
	require.False(t, c.capturedExecutionRetryNeedsRebuild(), "the safe generation must not force further temporal rebuilds")
}

func TestTemporalIndexPlacementChecksEveryScan(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.GetSessionInfo().TimeZone = time.UTC
	c := &Compile{proc: proc, addr: "local:6001", ncpu: 4}
	intent := &plan2.ExecutionIntent{TimeZone: time.UTC, UnixTime: proc.Base.UnixTime, Placement: schedule.LocalAccessIntent{CurrentCN: schedule.Worker{ID: proc.GetService(), Addr: c.addr, Route: schedule.WorkerRouteLocal}}}
	c.SetValidatedExecutionIntent(intent)
	makeScan := func() *plan.Node {
		return &plan.Node{NodeType: plan.Node_TABLE_SCAN, Stats: &plan.Stats{ForceOneCN: true}, IndexScanInfo: plan.IndexScanInfo{IsIndexScan: true, RequiresCapturedExecution: true}, FilterList: []*plan.Expr{{CandidateFilter: true}}}
	}
	template := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{makeScan(), makeScan()}, MaxDop: 1}}}
	oldForce := plan2.GetForceScanOnMultiCN()
	t.Cleanup(func() { plan2.SetForceScanOnMultiCN(oldForce) })
	plan2.SetForceScanOnMultiCN(false)
	require.True(t, c.capturedExecutionPlacementAllowed(template), "local placement is independent of the cost model fanout")
	template.GetQuery().Nodes[1].Stats.ForceOneCN = false
	require.False(t, c.capturedExecutionPlacementAllowed(template), "every marked scan must retain local placement")
	template.GetQuery().Nodes[1].Stats.ForceOneCN = true
	template.GetQuery().Nodes[1].IndexScanInfo.IsIndexScan = false
	require.False(t, c.capturedExecutionPlacementAllowed(template), "every marked node must be a physical index scan")
	template.GetQuery().Nodes[1].IndexScanInfo.IsIndexScan = true
	c.ncpu = 5
	require.True(t, c.capturedExecutionPlacementAllowed(template), "ordinary estimated cost is not an execution permission")
	c.ncpu = 4
	c.executionIntent = nil
	require.False(t, c.capturedExecutionPlacementAllowed(template))
	c.SetValidatedExecutionIntent(intent)
	plan2.SetForceScanOnMultiCN(true)
	require.False(t, c.capturedExecutionPlacementAllowed(template))
	plan2.SetForceScanOnMultiCN(false)
	require.True(t, c.capturedExecutionPlacementAllowed(template))
}
