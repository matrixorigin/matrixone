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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestAdaptiveTopPreservesCandidateProjection(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	ctx := NewBindContext(b, nil)
	col := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 10}}}
	expr, err := BindFuncExprImplByPlanExpr(context.Background(), "+", []*plan.Expr{col, makePlan2Int64ConstExprWithType(100)})
	require.NoError(t, err)
	scan := b.appendNode(&plan.Node{NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{10}}, ctx)
	project := b.appendNode(&plan.Node{NodeType: plan.Node_PROJECT, BindingTags: []int32{11}, Children: []int32{scan}, ProjectList: []*plan.Expr{expr}}, ctx)
	root := b.appendNode(&plan.Node{NodeType: plan.Node_ADAPTIVE_TOP, Children: []int32{project}}, ctx)
	b.removeSimpleProjections(root, plan.Node_UNKNOWN, false, map[[2]int32]int{{11, 0}: 1})
	require.Equal(t, project, b.qry.Nodes[root].Children[0])
	require.Equal(t, "+", b.qry.Nodes[project].ProjectList[0].GetF().Func.ObjName)
}

func TestAdaptiveTopRejectsVolatileReplayAndCopiesForceMode(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	ctx := NewBindContext(b, nil)
	randExpr, err := BindFuncExprImplByPlanExpr(context.Background(), "rand", nil)
	require.NoError(t, err)
	scan := b.appendNode(&plan.Node{NodeType: plan.Node_TABLE_SCAN}, ctx)
	projectNode := &plan.Node{NodeType: plan.Node_PROJECT, Children: []int32{scan}, ProjectList: []*plan.Expr{randExpr}}
	project := b.appendNode(projectNode, ctx)
	require.False(t, b.adaptiveIvfReplaySafe(project))

	original := &plan.RankOption{Mode: "auto"}
	v := &vectorSortContext{
		projNode:   projectNode,
		sortNode:   &plan.Node{},
		scanNode:   b.qry.Nodes[scan],
		rankOption: original,
	}
	b.forceAdaptiveVectorRegion(v)
	require.Equal(t, "auto", original.Mode)
	require.Equal(t, "force", v.rankOption.Mode)
	require.Equal(t, "force", v.projNode.RankOption.Mode)
	require.NotSame(t, v.rankOption, v.projNode.RankOption)
}

func TestAdaptiveTopUnsupportedRegionKeepsExactGraph(t *testing.T) {
	for _, shape := range []string{"sort", "membership", "provider"} {
		t.Run(shape, func(t *testing.T) {
			opts := vectorJoinPlanOptions{joinType: plan.Node_SEMI}
			if shape == "provider" {
				opts = vectorJoinPlanOptions{joinType: plan.Node_INNER, providerSingle: true, providerVectorNotNull: true}
			}
			tc := newVectorJoinPlanCase(t, opts)
			v := tc.builder.buildVectorSortContextThroughJoin(tc.projNode)
			require.NotNil(t, v)
			v.projNode.ProjectList = []*plan.Expr{makePlan2Int64ConstExprWithType(1)}
			originalOption := &plan.RankOption{Mode: "auto"}
			v.rankOption = originalOption
			root := tc.projNodeID
			switch shape {
			case "sort":
				v.projNode = nil
				v.hasMembership = false
				v.providerNodeID = -1
				root = v.sortNode.NodeId
			case "provider":
				require.False(t, v.hasMembership)
				require.Equal(t, tc.providerNodeID, v.providerNodeID)
			}
			count := len(tc.builder.qry.Nodes)
			children := append([]int32(nil), tc.builder.qry.Nodes[root].Children...)
			got, err := tc.builder.buildAdaptiveIvfTop(root, v, newVectorJoinIvfIndex(), nil, nil)
			require.NoError(t, err)
			require.Equal(t, root, got)
			require.Len(t, tc.builder.qry.Nodes, count, "must not leave partially rewritten candidates")
			require.Equal(t, children, tc.builder.qry.Nodes[root].Children)
			require.Equal(t, "force", v.sortNode.RankOption.Mode)
			require.Equal(t, "auto", originalOption.Mode, "must not mutate shared rank options")
		})
	}
}
