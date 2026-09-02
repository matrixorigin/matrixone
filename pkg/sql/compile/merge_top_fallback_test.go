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

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeorder"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
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
		name         string
		limit        *plan.Expr
		wantMergeTop bool
	}{
		{
			name:         "resident threshold",
			limit:        plan2.MakePlan2Uint64ConstExprWithType(mergeTopResidentPlanThreshold),
			wantMergeTop: true,
		},
		{
			name:  "large literal",
			limit: plan2.MakePlan2Uint64ConstExprWithType(mergeTopResidentPlanThreshold + 1),
		},
		{
			name:  "dynamic",
			limit: &plan.Expr{Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}},
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
				require.Same(t, test.limit, localTop.Limit)
			}

			if test.wantMergeTop {
				globalTop, ok := result[0].RootOp.(*mergetop.MergeTop)
				require.True(t, ok)
				require.Same(t, test.limit, globalTop.Limit)
			} else {
				globalLimit, ok := result[0].RootOp.(*limit.Limit)
				require.True(t, ok)
				require.Same(t, test.limit, globalLimit.LimitExpr)
				_, ok = globalLimit.GetOperatorBase().GetChildren(0).(*mergeorder.MergeOrder)
				require.True(t, ok)
			}

			result[0].FreeOperator(c)
			result[0].release()
			c.proc.Free()
		})
	}
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
	require.Same(t, limitExpr, localTop.Limit)

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
