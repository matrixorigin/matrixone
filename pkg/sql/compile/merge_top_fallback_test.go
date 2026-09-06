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
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeorder"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

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
		ordered bool
	}{
		{
			name:  "resident threshold",
			limit: plan2.MakePlan2Uint64ConstExprWithType(mergeTopResidentPlanThreshold),
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
	require.False(t, shouldUseDistributedOrderedTop(
		&plan.Node{Stats: &plan.Stats{Cost: 1_000_000, Rowsize: 146}},
		dynamicLimit,
		scopes))
	require.True(t, shouldUseDistributedOrderedTop(
		&plan.Node{Stats: &plan.Stats{Cost: 10_000_000, Rowsize: 146}},
		dynamicLimit,
		scopes))
	require.False(t, shouldUseDistributedOrderedTop(
		&plan.Node{}, plan2.MakePlan2Uint64ConstExprWithType(0), scopes))
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
	require.True(t, localTop.OrderedOutput)
	require.Same(t, limitExpr, localTop.Limit)

	result[0].FreeOperator(c)
	result[0].release()
	c.proc.Free()
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

func newMergeTopFallbackTestCompile(t *testing.T) *Compile {
	c := NewMockCompile(t)
	c.anal = &AnalyzeModule{curNodeIdx: 1, isFirst: true}
	c.execType = plan2.ExecTypeAP_ONECN
	c.isPrepare = true
	return c
}

func newMergeTopFallbackTestNode(limitExpr *plan.Expr) *plan.Node {
	return &plan.Node{
		Limit: limitExpr,
		OrderBy: []*plan.OrderBySpec{{
			Expr: &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}},
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
