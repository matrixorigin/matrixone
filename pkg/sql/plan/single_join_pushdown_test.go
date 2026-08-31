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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func singleJoinPushdownEquality(t *testing.T, builder *QueryBuilder, leftTag, rightTag int32) *plan.Expr {
	t.Helper()
	typ := plan.Type{Id: int32(types.T_int64)}
	expr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
		GetColExpr(typ, leftTag, 0),
		GetColExpr(typ, rightTag, 0),
	})
	require.NoError(t, err)
	return expr
}

func newSingleJoinPushdownBuilder(t *testing.T, filterTagIndexes ...int) (*QueryBuilder, int32, []int32) {
	t.Helper()
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	tags := []int32{
		builder.GenNewBindTag(),
		builder.GenNewBindTag(),
		builder.GenNewBindTag(),
		builder.GenNewBindTag(),
	}
	builder.qry.Nodes = []*plan.Node{
		{NodeId: 0, NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{tags[0]}},
		{NodeId: 1, NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{tags[1]}},
		{NodeId: 2, NodeType: plan.Node_JOIN, JoinType: plan.Node_INNER, Children: []int32{0, 1}},
		{NodeId: 3, NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{tags[2]}},
		{NodeId: 4, NodeType: plan.Node_JOIN, JoinType: plan.Node_SEMI, Children: []int32{2, 3}},
		{NodeId: 5, NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{tags[3]}},
		{NodeId: 6, NodeType: plan.Node_JOIN, JoinType: plan.Node_SINGLE, Children: []int32{4, 5}},
		{NodeId: 7, NodeType: plan.Node_FILTER, Children: []int32{6}},
	}
	for _, tagIndex := range filterTagIndexes {
		builder.qry.Nodes[7].FilterList = append(builder.qry.Nodes[7].FilterList,
			singleJoinPushdownEquality(t, builder, tags[tagIndex], tags[3]))
	}
	return builder, 7, tags
}

func TestPushdownUncorrelatedSingleJoinFilterFindsSmallestInput(t *testing.T) {
	t.Run("through semi and inner left", func(t *testing.T) {
		builder, rootID, tags := newSingleJoinPushdownBuilder(t, 0)
		rootID = builder.pushdownUncorrelatedSingleJoinFilters(rootID)

		require.Equal(t, int32(4), rootID)
		require.Equal(t, int32(2), builder.qry.Nodes[4].Children[0])
		require.Equal(t, int32(7), builder.qry.Nodes[2].Children[0])
		require.Equal(t, int32(6), builder.qry.Nodes[7].Children[0])
		require.Equal(t, int32(0), builder.qry.Nodes[6].Children[0])
		require.Equal(t, tags[0], builder.qry.Nodes[0].BindingTags[0])
	})

	t.Run("inner right", func(t *testing.T) {
		builder, rootID, _ := newSingleJoinPushdownBuilder(t, 1)
		rootID = builder.pushdownUncorrelatedSingleJoinFilters(rootID)

		require.Equal(t, int32(4), rootID)
		require.Equal(t, int32(7), builder.qry.Nodes[2].Children[1])
		require.Equal(t, int32(1), builder.qry.Nodes[6].Children[0])
	})

	t.Run("stops at common input", func(t *testing.T) {
		builder, rootID, _ := newSingleJoinPushdownBuilder(t, 0, 1)
		rootID = builder.pushdownUncorrelatedSingleJoinFilters(rootID)

		require.Equal(t, int32(4), rootID)
		require.Equal(t, int32(7), builder.qry.Nodes[4].Children[0])
		require.Equal(t, int32(2), builder.qry.Nodes[6].Children[0])
	})
}

func TestPushdownUncorrelatedSingleJoinFilterKeepsSemanticBarriers(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*QueryBuilder, []int32)
	}{
		{
			name: "correlated single",
			mutate: func(builder *QueryBuilder, tags []int32) {
				builder.qry.Nodes[6].OnList = []*plan.Expr{
					singleJoinPushdownEquality(t, builder, tags[0], tags[3]),
				}
			},
		},
		{
			name: "right single",
			mutate: func(builder *QueryBuilder, _ []int32) {
				builder.qry.Nodes[6].IsRightJoin = true
			},
		},
		{
			name: "volatile filter",
			mutate: func(builder *QueryBuilder, tags []int32) {
				builder.qry.Nodes[7].FilterList = append(builder.qry.Nodes[7].FilterList,
					makeVolatileJoinFilter(t, builder.compCtx.(*MockCompilerContext), &tags[0]))
			},
		},
		{
			name: "outer join",
			mutate: func(builder *QueryBuilder, _ []int32) {
				builder.qry.Nodes[4].JoinType = plan.Node_LEFT
			},
		},
		{
			name: "limit",
			mutate: func(builder *QueryBuilder, _ []int32) {
				builder.qry.Nodes[4].Limit = makePlan2Uint64ConstExprWithType(1)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			builder, rootID, tags := newSingleJoinPushdownBuilder(t, 0)
			test.mutate(builder, tags)

			require.Equal(t, rootID, builder.pushdownUncorrelatedSingleJoinFilters(rootID))
			require.Equal(t, int32(6), builder.qry.Nodes[7].Children[0])
			require.Equal(t, int32(4), builder.qry.Nodes[6].Children[0])
		})
	}
}

func TestPlannerPushesUncorrelatedScalarFilterBelowUnrelatedJoin(t *testing.T) {
	logicPlan, err := runOneStmt(NewMockOptimizer(true), t, `
		select n.n_name
		from tpch.nation n join tpch.region r on n.n_regionkey = r.r_regionkey
		where r.r_regionkey = (
			select r2.r_regionkey from tpch.region r2 where r2.r_name = 'EUROPE'
		)`)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	var scalar *plan.Node
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_SINGLE && len(node.OnList) == 0 {
			scalar = node
			break
		}
	}
	require.NotNil(t, scalar)

	outerScans := make(map[string]int)
	var visit func(int32)
	visit = func(nodeID int32) {
		node := query.Nodes[nodeID]
		if node.NodeType == plan.Node_TABLE_SCAN && node.TableDef != nil {
			outerScans[node.TableDef.Name]++
		}
		for _, childID := range node.Children {
			visit(childID)
		}
	}
	visit(scalar.Children[0])
	require.Equal(t, map[string]int{"region": 1}, outerScans,
		"SINGLE must be placed at the region input, before joining nation")
}
