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
	"testing"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeorder"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestCompileLargeOffsetUsesExternalOrder(t *testing.T) {
	runLargeOffsetMemory(t, int(mergeTopResidentPlanThreshold)+1)
}

// Scale-sensitive memory acceptance from review 5127191041 belongs outside
// the ordinary UT fixture. The UT above pins the minimum routing boundary.
func BenchmarkReviewLargeOffsetMemory(b *testing.B) {
	for i := 0; i < b.N; i++ {
		runLargeOffsetMemory(b, 4*1024*1024)
	}
}

func runLargeOffsetMemory(t testing.TB, n int) {
	c := newMergeTopFallbackTestCompile(t)
	c.proc.Base.Lim.Size = 160 << 20
	generation, err := c.proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 1<<14)
	require.NoError(t, err)
	account, err := registry.OpenWithController(160<<20, generation)
	require.NoError(t, err)
	var bats []*batch.Batch
	for start := 0; start < n; start += 8192 {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		values := make([]int64, min(8192, n-start))
		for j := range values {
			values[j] = int64(start + j)
		}
		require.NoError(t, vector.AppendFixedList(bat.Vecs[0], values, nil, c.proc.Mp()))
		bat.SetRowCount(len(values))
		bats = append(bats, bat)
	}
	s := newMergeTopFallbackTestScope(c)
	s.Proc = c.proc
	s.RootOp = colexec.NewMockOperator().WithBatchs(bats)
	node := newMergeTopFallbackTestNode(plan2.MakePlan2Uint64ConstExprWithType(1))
	node.Offset = plan2.MakePlan2Uint64ConstExprWithType(uint64(n - 1))
	node.NodeType = plan.Node_SORT
	scopes := c.compileSort(node, []*Scope{s})
	require.Len(t, scopes, 1)
	op := scopes[0].RootOp
	var owners []executionAllocationAccountOwner
	defer func() {
		require.NoError(t, vm.HandleAllOp(op, func(_ vm.Operator, o vm.Operator) error { o.Free(c.proc, false, nil); return nil }))
		for _, owner := range owners {
			require.NoError(t, owner.ClearAllocationAccount(account))
		}
		require.Zero(t, account.Snapshot().Used)
		require.Zero(t, generation.SpillDiskUsed())
		require.Zero(t, generation.SpillFDUsed())
		c.proc.Free()
		require.Zero(t, c.proc.Mp().CurrNB())
	}()
	require.NoError(t, vm.HandleAllOp(op, func(_ vm.Operator, o vm.Operator) error {
		require.NotEqual(t, vm.Top, o.OpType())
		if owner, ok := o.(executionAllocationAccountOwner); ok {
			require.NoError(t, owner.SetAllocationAccount(account))
			owners = append(owners, owner)
		}
		return o.Prepare(c.proc)
	}))
	result, err := vm.Exec(op, c.proc)
	t.Logf("rows=%d peak accounted bytes=%d", n, account.Snapshot().Peak)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, 1, result.Batch.RowCount())
	require.Equal(t, int64(n-1), vector.GetFixedAtWithTypeCheck[int64](result.Batch.Vecs[0], 0))
}

func TestCanUseResidentMergeTop(t *testing.T) {
	dynamicLimit := &plan.Expr{Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}}
	tests := []struct {
		name  string
		limit *plan.Expr
		want  bool
	}{
		{name: "nil", limit: nil},
		{name: "zero", limit: plan2.MakePlan2Uint64ConstExprWithType(0), want: true},
		{
			name:  "threshold",
			limit: plan2.MakePlan2Uint64ConstExprWithType(mergeTopResidentPlanThreshold),
			want:  true,
		},
		{
			name:  "above threshold",
			limit: plan2.MakePlan2Uint64ConstExprWithType(mergeTopResidentPlanThreshold + 1),
		},
		{name: "non uint literal", limit: plan2.MakePlan2Int64ConstExprWithType(1)},
		{name: "dynamic", limit: dynamicLimit},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, canUseResidentMergeTop(test.limit))
		})
	}
}

func TestCompileTopRoutesMultiScopeLimits(t *testing.T) {
	tests := []struct {
		name    string
		limit   *plan.Expr
		stats   *plan.Stats
		ordered bool
	}{
		{
			name:  "resident threshold",
			limit: plan2.MakePlan2Uint64ConstExprWithType(mergeTopResidentPlanThreshold),
			stats: &plan.Stats{Cost: 1_000, Rowsize: 100},
		},
		{
			name:  "resident row threshold with wide candidates",
			limit: plan2.MakePlan2Uint64ConstExprWithType(mergeTopResidentPlanThreshold),
			stats: &plan.Stats{
				Cost:    float64(mergeTopResidentPlanThreshold * 2),
				Rowsize: float64(distributedTopNStreamingThresholdBytes)/(float64(mergeTopResidentPlanThreshold)*2) + 1,
			},
			ordered: true,
		},
		{
			name:    "large literal",
			limit:   plan2.MakePlan2Uint64ConstExprWithType(mergeTopResidentPlanThreshold + 1),
			ordered: true,
		},
		{
			name:    "dynamic",
			limit:   &plan.Expr{Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}},
			ordered: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := newMergeTopFallbackTestCompile(t)
			node := newMergeTopFallbackTestNode(test.limit)
			node.Stats = test.stats
			result := c.compileTop(node, test.limit, newMergeTopFallbackTestScopes(c, 2))
			require.Len(t, result, 1)
			require.Len(t, result[0].PreScopes, 2)
			for _, producer := range result[0].PreScopes {
				connectorOp, ok := producer.RootOp.(*connector.Connector)
				require.True(t, ok)
				localTop, ok := connectorOp.GetOperatorBase().GetChildren(0).(*top.Top)
				require.True(t, ok)
				require.Equal(t, test.ordered, localTop.OrderedOutput)
				require.Same(t, test.limit, localTop.Limit)
			}

			globalTop, ok := result[0].RootOp.(*mergetop.MergeTop)
			require.True(t, ok)
			require.Equal(t, test.ordered, globalTop.OrderedStreams)
			if test.ordered {
				require.Zero(t, globalTop.GetOperatorBase().NumChildren())
			} else {
				require.Equal(t, 1, globalTop.GetOperatorBase().NumChildren())
			}
			require.Same(t, test.limit, globalTop.Limit)
			for _, reg := range result[0].Proc.Reg.MergeReceivers {
				require.Equal(t, test.ordered, reg.OrderedStream)
				if test.ordered {
					require.Equal(t, 1, reg.NilBatchCnt)
					require.Equal(t, 1, cap(reg.Ch2))
				}
			}

			result[0].FreeOperator(c)
			result[0].release()
			c.proc.Free()
		})
	}
}

func TestShouldUseDistributedOrderedTopUsesCandidateBytes(t *testing.T) {
	limitExpr := plan2.MakePlan2Uint64ConstExprWithType(500_000)
	scopes := []*Scope{{NodeInfo: engine.Node{Mcpu: 16}}}
	tests := []struct {
		name     string
		cost     float64
		rowSize  float64
		expected bool
	}{
		{name: "one spill window shape", cost: 1_000_000, rowSize: 146},
		{name: "many spill windows shape", cost: 10_000_000, rowSize: 146, expected: true},
		{name: "missing cardinality is bounded", rowSize: 146, expected: true},
		{name: "missing row size is bounded", cost: 10_000_000, expected: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			node := &plan.Node{Stats: &plan.Stats{Cost: test.cost, Rowsize: test.rowSize}}
			require.Equal(t, test.expected,
				shouldUseDistributedOrderedTop(node, limitExpr, scopes))
		})
	}
	require.True(t, shouldUseDistributedOrderedTop(
		&plan.Node{},
		&plan.Expr{Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}},
		scopes))
	dynamicLimit := &plan.Expr{Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}}
	require.True(t, shouldUseDistributedOrderedTop(
		&plan.Node{Stats: &plan.Stats{Cost: 1_000_000, Rowsize: 146}},
		dynamicLimit,
		scopes))
	require.True(t, shouldUseDistributedOrderedTop(
		&plan.Node{Stats: &plan.Stats{Cost: 10_000_000, Rowsize: 146}},
		dynamicLimit,
		scopes))
	require.False(t, shouldUseDistributedOrderedTop(
		&plan.Node{}, plan2.MakePlan2Uint64ConstExprWithType(0), scopes))

	residentLimit := plan2.MakePlan2Uint64ConstExprWithType(mergeTopResidentPlanThreshold)
	residentCandidates := float64(mergeTopResidentPlanThreshold * 16)
	exactThresholdRowSize := float64(distributedTopNStreamingThresholdBytes) / residentCandidates
	residentNode := newMergeTopFallbackTestNode(residentLimit)
	residentNode.Stats = &plan.Stats{Cost: residentCandidates, Rowsize: exactThresholdRowSize}
	require.False(t, shouldUseDistributedOrderedTop(
		residentNode,
		residentLimit,
		scopes))
	residentNode.Stats.Rowsize++
	require.True(t, shouldUseDistributedOrderedTop(
		residentNode,
		residentLimit,
		scopes))
	require.True(t, shouldUseDistributedOrderedTop(
		&plan.Node{}, residentLimit, scopes))
}

func TestCompileTopKeepsSingleScopeLargeLimitLocal(t *testing.T) {
	c := newMergeTopFallbackTestCompile(t)
	limitExpr := plan2.MakePlan2Uint64ConstExprWithType(mergeTopResidentPlanThreshold + 1)
	node := newMergeTopFallbackTestNode(limitExpr)
	scope := newMergeTopFallbackTestScope(c)

	result := c.compileTop(node, limitExpr, []*Scope{scope})
	require.Len(t, result, 1)
	require.Same(t, scope, result[0])
	localTop, ok := result[0].RootOp.(*top.Top)
	require.True(t, ok)
	require.False(t, localTop.OrderedOutput, "single-worker results have no ordered receiver edge")
	require.Same(t, limitExpr, localTop.Limit)

	result[0].FreeOperator(c)
	result[0].release()
	c.proc.Free()
}

func TestCompileTopStaleStatisticsCannotAdmitVarlenPayload(t *testing.T) {
	for _, k := range []uint64{16383, 16384, 16385} {
		for _, version := range []int64{defines.MORPCVersion52, defines.MORPCLatestVersion} {
			c := newMergeTopFallbackTestCompile(t)
			runtime.ServiceRuntime(c.proc.GetService()).SetGlobalVariables(runtime.MOProtocolVersion, version)
			node := newMergeTopFallbackTestNode(plan2.MakePlan2Uint64ConstExprWithType(k))
			node.Stats = &plan.Stats{Cost: 1, Rowsize: 1}
			node.ProjectList = append(node.ProjectList, &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}})
			result := c.compileTop(node, node.Limit, newMergeTopFallbackTestScopes(c, 2))
			if version >= defines.MORPCVersion53 && k <= mergeTopResidentPlanThreshold {
				gather, ok := result[0].RootOp.(*mergetop.MergeTop)
				require.True(t, ok)
				require.True(t, gather.OrderedStreams)
			} else {
				globalLimit, ok := result[0].RootOp.(*limit.Limit)
				require.True(t, ok)
				_, ok = globalLimit.GetChildren(0).(*mergeorder.MergeOrder)
				require.True(t, ok)
			}
			result[0].FreeOperator(c)
			result[0].release()
			c.proc.Free()
		}
	}
}

func TestResidentTopPayloadProofUsesPhysicalInputSchema(t *testing.T) {
	limitExpr := plan2.MakePlan2Uint64ConstExprWithType(16384)
	node := newMergeTopFallbackTestNode(limitExpr)
	require.True(t, residentTopPayloadFits(node, limitExpr))
	for _, kind := range []plan.Node_NodeType{plan.Node_SORT, plan.Node_TIME_WINDOW} {
		node.NodeType = kind
		// These projections run after Top; a narrow final projection says
		// nothing about the width of retained child payload.
		require.False(t, residentTopPayloadFits(node, limitExpr))
	}
	node.NodeType = plan.Node_PROJECT
	node.ProjectList = nil
	require.False(t, residentTopPayloadFits(node, limitExpr))
	node.ProjectList = []*plan.Expr{{Typ: plan.Type{Id: int32(types.T_any)}}}
	require.False(t, residentTopPayloadFits(node, limitExpr))
	node.ProjectList = []*plan.Expr{{Typ: plan.Type{Id: int32(types.T_tuple)}}}
	require.False(t, residentTopPayloadFits(node, limitExpr))
	node.ProjectList = []*plan.Expr{{Typ: plan.Type{Id: int32(types.T_int64)}}}
	require.False(t, residentTopPayloadFits(node, plan2.MakePlan2Uint64ConstExprWithType(^uint64(0))))
	// Widen the fixed-width payload beyond the resident allowance without
	// allocating rows or relying on optimizer estimates.
	for range 128 {
		node.ProjectList = append(node.ProjectList, node.ProjectList[0])
	}
	require.False(t, residentTopPayloadFits(node, limitExpr))
}

func TestCompileTopFallsBackDuringRollingUpgrade(t *testing.T) {
	c := newMergeTopFallbackTestCompile(t)
	rt := runtime.ServiceRuntime(c.proc.GetService())
	previous, hadPrevious := rt.GetGlobalVariables(runtime.MOProtocolVersion)
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion52)
	t.Cleanup(func() {
		if hadPrevious {
			rt.SetGlobalVariables(runtime.MOProtocolVersion, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion52)
		}
	})

	limitExpr := plan2.MakePlan2Uint64ConstExprWithType(mergeTopResidentPlanThreshold + 1)
	node := newMergeTopFallbackTestNode(limitExpr)
	result := c.compileTop(node, limitExpr, newMergeTopFallbackTestScopes(c, 2))
	require.Len(t, result, 1)
	globalLimit, ok := result[0].RootOp.(*limit.Limit)
	require.True(t, ok)
	_, ok = globalLimit.GetOperatorBase().GetChildren(0).(*mergeorder.MergeOrder)
	require.True(t, ok)
	orderedReg := process.NewPipelineEdge(1, 1)
	orderedReg.OrderedStream = true
	orderedOutput := connector.NewArgument().WithReg(orderedReg)
	_, _, _, _, err := prepareRemoteRunSendingData(
		"select 1 order by 1 limit 1",
		&Scope{RootOp: orderedOutput, Proc: c.proc},
		c.proc,
		nil,
		uuid.Nil,
	)
	require.Error(t, err)
	orderedOutput.Release()

	result[0].FreeOperator(c)
	result[0].release()
	c.proc.Free()
}

func TestCompileTopFallsBackWhenOrderKeyIsNotMaterialized(t *testing.T) {
	c := newMergeTopFallbackTestCompile(t)
	limitExpr := plan2.MakePlan2Uint64ConstExprWithType(mergeTopResidentPlanThreshold + 1)
	node := newMergeTopFallbackTestNode(limitExpr)
	node.OrderBy[0].Expr = plan2.MakePlan2Int64ConstExprWithType(7)

	result := c.compileTop(node, limitExpr, newMergeTopFallbackTestScopes(c, 2))
	require.Len(t, result, 1)
	globalLimit, ok := result[0].RootOp.(*limit.Limit)
	require.True(t, ok)
	_, ok = globalLimit.GetOperatorBase().GetChildren(0).(*mergeorder.MergeOrder)
	require.True(t, ok)

	result[0].FreeOperator(c)
	result[0].release()
	c.proc.Free()
}

func newMergeTopFallbackTestCompile(t testing.TB) *Compile {
	c := NewMockCompile(t)
	enableDistributedOrderedTopForTest(t, c.proc)
	c.anal = &AnalyzeModule{curNodeIdx: 1, isFirst: true}
	c.execType = plan2.ExecTypeAP_ONECN
	c.isPrepare = true
	return c
}

func enableDistributedOrderedTopForTest(t testing.TB, proc *process.Process) {
	t.Helper()
	rt := runtime.ServiceRuntime(proc.GetService())
	previous, hadPrevious := rt.GetGlobalVariables(runtime.MOProtocolVersion)
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
	t.Cleanup(func() {
		if hadPrevious {
			rt.SetGlobalVariables(runtime.MOProtocolVersion, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})
}

func newMergeTopFallbackTestNode(limitExpr *plan.Expr) *plan.Node {
	key := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
	}
	return &plan.Node{
		Limit:       limitExpr,
		ProjectList: []*plan.Expr{key},
		OrderBy: []*plan.OrderBySpec{{
			Expr: key,
		}},
	}
}

func newMergeTopFallbackTestScope(c *Compile) *Scope {
	return &Scope{
		Magic:    Normal,
		NodeInfo: engine.Node{Mcpu: 1},
		Proc:     c.proc.NewNoContextChildProc(0),
		RootOp:   colexec.NewMockOperator(),
	}
}

func newMergeTopFallbackTestScopes(c *Compile, count int) []*Scope {
	scopes := make([]*Scope, count)
	for i := range scopes {
		scopes[i] = newMergeTopFallbackTestScope(c)
	}
	return scopes
}
