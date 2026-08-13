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

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func fnExpr(name string, args ...*plan.Expr) *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{ObjName: name},
		Args: args,
	}}}
}

func colExpr() *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{}}}
}

// TestPlanCallsAnyFunc_FindsReachableCalls covers every expression list a node can carry,
// since a MATCH can legally appear in any of them and a missed list would silently persist
// an unusable view.
func TestPlanCallsAnyFunc_FindsReachableCalls(t *testing.T) {
	for _, tc := range []struct {
		name string
		node *plan.Node
	}{
		{"filter", &plan.Node{NodeId: 1, Children: []int32{0}, FilterList: []*plan.Expr{fnExpr("fulltext_match")}}},
		{"project", &plan.Node{NodeId: 1, Children: []int32{0}, ProjectList: []*plan.Expr{fnExpr("fulltext_match_score")}}},
		{"on list", &plan.Node{NodeId: 1, Children: []int32{0}, OnList: []*plan.Expr{fnExpr("fulltext_match")}}},
		{"group by", &plan.Node{NodeId: 1, Children: []int32{0}, GroupBy: []*plan.Expr{fnExpr("fulltext_match")}}},
		{"agg list", &plan.Node{NodeId: 1, Children: []int32{0}, AggList: []*plan.Expr{fnExpr("fulltext_match")}}},
		{"block filter", &plan.Node{NodeId: 1, Children: []int32{0}, BlockFilterList: []*plan.Expr{fnExpr("fulltext_match")}}},
		{"order by", &plan.Node{NodeId: 1, Children: []int32{0},
			OrderBy: []*plan.OrderBySpec{{Expr: fnExpr("fulltext_match_score")}}}},
		// A window's OVER (ORDER BY MATCH(...)) lands in WinSpecList, not OrderBy. It was
		// missed by the first version of this walker, and a CREATE VIEW carrying one
		// persisted an unusable view exactly as #27027 describes.
		{"window spec", &plan.Node{NodeId: 1, Children: []int32{0},
			WinSpecList: []*plan.Expr{fnExpr("fulltext_match_score")}}},
		// The real shape of OVER (ORDER BY MATCH(...)): WinSpecList holds an Expr_W, and
		// the MATCH sits inside its OrderBy. Walking WinSpecList is not enough unless
		// checkExpr descends into the spec -- measured on a real plan, the placeholder is
		// exactly here (Window node, "Order By: fulltext_match(...)").
		{"inside a window spec's ORDER BY", &plan.Node{NodeId: 1, Children: []int32{0},
			WinSpecList: []*plan.Expr{{Expr: &plan.Expr_W{W: &plan.WindowSpec{
				WindowFunc: fnExpr("row_number"),
				OrderBy:    []*plan.OrderBySpec{{Expr: fnExpr("fulltext_match")}},
			}}}}}},
		{"inside a window spec's PARTITION BY", &plan.Node{NodeId: 1, Children: []int32{0},
			WinSpecList: []*plan.Expr{{Expr: &plan.Expr_W{W: &plan.WindowSpec{
				WindowFunc:  fnExpr("row_number"),
				PartitionBy: []*plan.Expr{fnExpr("fulltext_match")},
			}}}}}},
		{"as the window function itself", &plan.Node{NodeId: 1, Children: []int32{0},
			WinSpecList: []*plan.Expr{{Expr: &plan.Expr_W{W: &plan.WindowSpec{
				WindowFunc: fnExpr("fulltext_match_score"),
			}}}}}},
		// A subquery reference nests through its Child expression. Flattening usually
		// removes these before the optimized plan, but the walker must not depend on that.
		{"inside a subquery's child expr", &plan.Node{NodeId: 1, Children: []int32{0},
			FilterList: []*plan.Expr{{Expr: &plan.Expr_Sub{Sub: &plan.SubqueryRef{
				Child:  fnExpr("fulltext_match"),
				NodeId: -1,
			}}}}}},
		{"time window partition by", &plan.Node{NodeId: 1, Children: []int32{0},
			TimeWindowPartitionBy: []*plan.Expr{fnExpr("fulltext_match")}}},
		{"table function args", &plan.Node{NodeId: 1, Children: []int32{0},
			TblFuncExprList: []*plan.Expr{fnExpr("fulltext_match")}}},
		{"fill value", &plan.Node{NodeId: 1, Children: []int32{0},
			FillVal: []*plan.Expr{fnExpr("fulltext_match_score")}}},
		{"limit", &plan.Node{NodeId: 1, Children: []int32{0},
			Limit: fnExpr("fulltext_match_score")}},
		{"nested under a scalar", &plan.Node{NodeId: 1, Children: []int32{0},
			FilterList: []*plan.Expr{fnExpr("not", fnExpr("fulltext_match"))}}},
		{"inside an expr list", &plan.Node{NodeId: 1, Children: []int32{0},
			FilterList: []*plan.Expr{{Expr: &plan.Expr_List{List: &plan.ExprList{
				List: []*plan.Expr{fnExpr("fulltext_match")}}}}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := &plan.Query{
				Steps: []int32{1},
				Nodes: []*plan.Node{{NodeId: 0}, tc.node},
			}
			require.True(t, PlanCallsAnyFunc(query, "fulltext_match", "fulltext_match_score"))
		})
	}
}

// TestPlanCallsAnyFunc_IgnoresUnreachableNodes is the reason this walks from the roots.
// query.Nodes is an append-only arena, so a SUCCESSFUL rewrite leaves its pre-rewrite nodes
// in it: the fulltext rewrite re-parents the scan under a join with the index scan and
// abandons the original FILTER, which still holds the original fulltext_match. Scanning the
// arena reads that dead node -- measured against a real FULLTEXT(body) index, it rejected
// every CREATE VIEW carrying a WHERE MATCH.
//
// Node ids mirror an actual rewritten plan: root 2 -> 5 -> 4 -> {0,3}, orphan FILTER at 1.
func TestPlanCallsAnyFunc_IgnoresUnreachableNodes(t *testing.T) {
	query := &plan.Query{
		Steps: []int32{2},
		Nodes: []*plan.Node{
			{NodeId: 0, NodeType: plan.Node_TABLE_SCAN},
			{NodeId: 1, NodeType: plan.Node_FILTER, Children: []int32{0},
				FilterList: []*plan.Expr{fnExpr("fulltext_match")}},
			{NodeId: 2, NodeType: plan.Node_PROJECT, Children: []int32{5},
				ProjectList: []*plan.Expr{colExpr()}},
			{NodeId: 3, NodeType: plan.Node_FUNCTION_SCAN},
			{NodeId: 4, NodeType: plan.Node_JOIN, Children: []int32{0, 3}},
			{NodeId: 5, NodeType: plan.Node_SORT, Children: []int32{4}},
		},
	}
	require.False(t, PlanCallsAnyFunc(query, "fulltext_match", "fulltext_match_score"),
		"an abandoned pre-rewrite node must not condemn a view the index can serve")
}

func TestPlanCallsAnyFunc_NoMatch(t *testing.T) {
	query := &plan.Query{
		Steps: []int32{1},
		Nodes: []*plan.Node{
			{NodeId: 0},
			{NodeId: 1, Children: []int32{0},
				ProjectList: []*plan.Expr{fnExpr("l2_distance", colExpr(), colExpr())}},
		},
	}
	require.False(t, PlanCallsAnyFunc(query, "fulltext_match", "fulltext_match_score"))
	require.False(t, PlanCallsAnyFunc(nil, "fulltext_match"))
	require.False(t, PlanCallsAnyFunc(query), "no names means nothing to look for")
}

// TestPlanCallsAnyFunc_MalformedGraph: the walk runs inside DDL, so a cyclic or
// out-of-range graph must terminate rather than hang or panic.
func TestPlanCallsAnyFunc_MalformedGraph(t *testing.T) {
	cyclic := &plan.Query{
		Steps: []int32{0},
		Nodes: []*plan.Node{
			{NodeId: 0, Children: []int32{1}},
			{NodeId: 1, Children: []int32{0}},
		},
	}
	require.NotPanics(t, func() {
		require.False(t, PlanCallsAnyFunc(cyclic, "fulltext_match"))
	})

	outOfRange := &plan.Query{
		Steps: []int32{0, 99},
		Nodes: []*plan.Node{{NodeId: 0, Children: []int32{7, -1}}},
	}
	require.NotPanics(t, func() {
		require.False(t, PlanCallsAnyFunc(outOfRange, "fulltext_match"))
	})
}

// TestPlanCallsAnyFunc_FollowsSubqueryNodeEdge: a SubqueryRef points at a whole subtree via
// NodeId, which is not in the parent's Children. Walking Children alone would never reach
// it, so a MATCH living there would be invisible and the unusable view would persist.
func TestPlanCallsAnyFunc_FollowsSubqueryNodeEdge(t *testing.T) {
	query := &plan.Query{
		Steps: []int32{1},
		Nodes: []*plan.Node{
			{NodeId: 0, NodeType: plan.Node_TABLE_SCAN},
			{NodeId: 1, NodeType: plan.Node_PROJECT, Children: []int32{0},
				FilterList: []*plan.Expr{{Expr: &plan.Expr_Sub{Sub: &plan.SubqueryRef{
					NodeId: 2,
				}}}}},
			// Reachable ONLY through the subquery edge above.
			{NodeId: 2, NodeType: plan.Node_PROJECT,
				ProjectList: []*plan.Expr{fnExpr("fulltext_match")}},
		},
	}
	require.True(t, PlanCallsAnyFunc(query, "fulltext_match", "fulltext_match_score"))
}
