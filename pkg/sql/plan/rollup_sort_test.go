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
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestSortRollupPreservesHighNDVTupleDistinct(t *testing.T) {
	for _, tc := range []struct {
		name, algorithm, hints string
		rollup, marked         bool
	}{
		{"session SORT", "SORT", "", true, true},
		{"legacy hint", "COST", "rollupSort=1", true, true},
		{"COST", "COST", "", true, false},
		{"HASH", "HASH", "", true, false},
		{"ordinary GROUP BY", "SORT", "", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := "select a,b,count(distinct b,c) from select_test.bind_select group by a,b"
			if tc.rollup {
				sql += " with rollup"
			}
			q := buildAutoRollupPlanSQLWithControls(t, 2_000_000, sql, tc.algorithm,
				"determineShuffle=2,sharedComputation=1,"+tc.hints, 2, 4, 1_000_000).GetQuery()
			require.Equal(t, tc.marked, planHasSortRollup(q))
			aggregates := 0
			for _, n := range q.Nodes {
				if n.NodeType != planpb.Node_AGG {
					continue
				}
				aggregates++
				if tc.marked {
					require.True(t, IsSortRollupOption(n.ExtraOptions))
					require.Len(t, n.AggList, 1)
					require.NotZero(t, uint64(n.AggList[0].GetF().Func.Obj)&function.Distinct)
					require.Len(t, n.AggList[0].GetF().Args, 2)
					require.Equal(t, planpb.Node_SORT, q.Nodes[n.Children[0]].NodeType)
				}
			}
			if tc.marked {
				require.Equal(t, 1, aggregates)
			}
			if !tc.rollup {
				require.GreaterOrEqual(t, aggregates, 3, "ordinary GROUP BY retains the high-NDV tuple-pair topology")
				for _, n := range q.Nodes {
					for _, agg := range n.AggList {
						require.Zero(t, uint64(agg.GetF().Func.Obj)&function.Distinct)
					}
				}
			}
		})
	}
}

func TestSortRollupOrderFollowsColumnLineage(t *testing.T) {
	typ := planpb.Type{Id: int32(types.T_int64)}
	col := func(tag, pos int32) *Expr { return GetColExpr(typ, tag, pos) }
	for _, tc := range []struct {
		name string
		edit func([]*Node, []*Expr)
		want bool
	}{
		{name: "nested reordering and filter", want: true},
		{name: "sort output projection", want: true, edit: func(nodes []*Node, _ []*Expr) {
			nodes[2].BindingTags = []int32{19}
			nodes[2].ProjectList = []*Expr{col(11, 1), col(11, 0)}
			nodes[3].ProjectList = []*Expr{col(19, 0), col(19, 1)}
		}},
		{name: "different consumer relation", edit: func(_ []*Node, keys []*Expr) { keys[0].GetCol().RelPos = 99 }},
		{name: "different sort relation", edit: func(nodes []*Node, _ []*Expr) { nodes[2].OrderBy[0].Expr.GetCol().RelPos = 99 }},
		{name: "different input relation", edit: func(nodes []*Node, _ []*Expr) { nodes[1].ProjectList[1].GetCol().RelPos = 99 }},
		{name: "missing sort key", edit: func(nodes []*Node, _ []*Expr) { nodes[2].OrderBy = nodes[2].OrderBy[:1] }},
		{name: "wrong key order", edit: func(nodes []*Node, _ []*Expr) {
			nodes[2].OrderBy[0], nodes[2].OrderBy[1] = nodes[2].OrderBy[1], nodes[2].OrderBy[0]
		}},
		{name: "descending", edit: func(nodes []*Node, _ []*Expr) { nodes[2].OrderBy[0].Flag = planpb.OrderBySpec_DESC }},
		{name: "nulls last", edit: func(nodes []*Node, _ []*Expr) { nodes[2].OrderBy[0].Flag = planpb.OrderBySpec_NULLS_LAST }},
		{name: "different type", edit: func(nodes []*Node, _ []*Expr) { nodes[3].ProjectList[0].Typ.Id = int32(types.T_int32) }},
		{name: "different scale", edit: func(nodes []*Node, _ []*Expr) { nodes[3].ProjectList[0].Typ.Scale = 2 }},
		{name: "different width", edit: func(nodes []*Node, _ []*Expr) { nodes[3].ProjectList[0].Typ.Width = 8 }},
		{name: "different equality metadata", edit: func(nodes []*Node, _ []*Expr) { nodes[3].ProjectList[0].Typ.PadSpace = true }},
		{name: "computed projection", edit: func(nodes []*Node, _ []*Expr) { nodes[3].ProjectList[0] = MakePlan2Int64ConstExprWithType(1) }},
		{name: "project has no output binding", edit: func(nodes []*Node, _ []*Expr) { nodes[3].BindingTags = nil }},
		{name: "filter projects", edit: func(nodes []*Node, _ []*Expr) { nodes[4].ProjectList = []*Expr{col(23, 0)} }},
		{name: "sort projects without binding", edit: func(nodes []*Node, _ []*Expr) { nodes[2].ProjectList = []*Expr{col(11, 0), col(11, 1)} }},
		{name: "limit", edit: func(nodes []*Node, _ []*Expr) { nodes[2].Limit = MakePlan2Int64ConstExprWithType(1) }},
		{name: "offset", edit: func(nodes []*Node, _ []*Expr) { nodes[4].Offset = MakePlan2Int64ConstExprWithType(1) }},
		{name: "rank", edit: func(nodes []*Node, _ []*Expr) { nodes[2].RankOption = &planpb.RankOption{} }},
		{name: "local scan order", edit: func(nodes []*Node, _ []*Expr) {
			nodes[0].OrderBy = []*planpb.OrderBySpec{{Expr: col(7, 0)}, {Expr: col(7, 1)}}
			nodes[3].Children = []int32{1}
		}},
		{name: "unsupported join", edit: func(nodes []*Node, _ []*Expr) { nodes[4].NodeType = planpb.Node_JOIN }},
		{name: "cycle", edit: func(nodes []*Node, _ []*Expr) { nodes[4].Children = []int32{4} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nodes := []*Node{
				{NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{7}, TableDef: &TableDef{Cols: []*ColDef{{Name: "a", Typ: typ}, {Name: "b", Typ: typ}}}},
				{NodeType: planpb.Node_PROJECT, BindingTags: []int32{11}, Children: []int32{0}, ProjectList: []*Expr{col(7, 1), col(7, 0)}},
				{NodeType: planpb.Node_SORT, Children: []int32{1}, OrderBy: []*planpb.OrderBySpec{{Expr: col(11, 1)}, {Expr: col(11, 0)}, {Expr: col(11, 1)}}},
				{NodeType: planpb.Node_PROJECT, BindingTags: []int32{23}, Children: []int32{2}, ProjectList: []*Expr{col(11, 1), col(11, 0)}},
				{NodeType: planpb.Node_FILTER, Children: []int32{3}},
				{NodeType: planpb.Node_PROJECT, BindingTags: []int32{37}, Children: []int32{4}, ProjectList: []*Expr{col(23, 1), col(23, 0)}},
			}
			keys := []*Expr{col(37, 1), col(37, 0)}
			if tc.edit != nil {
				tc.edit(nodes, keys)
			}
			original := proto.Clone(&Query{Nodes: nodes})
			builder := &QueryBuilder{qry: &Query{Nodes: nodes}}
			require.Equal(t, tc.want, builder.sortRollupNodeInputOrdered(5, keys))
			require.True(t, proto.Equal(original, &Query{Nodes: nodes}), "order proof must not mutate its producer")
		})
	}
}

func TestSortRollupProjectedInputPlanModes(t *testing.T) {
	for _, tc := range []struct {
		name, source string
		wantSort     bool
	}{
		{"alias", "(select a as x,b as y,c as v from select_test.bind_select order by x,y) d", true},
		{"reordered", "(select c as v,b as y,a as x from select_test.bind_select order by x,y) d", true},
		{"filtered", "(select c as v,b as y,a as x from select_test.bind_select where b > 0 order by x,y) d", true},
		{"nested", "(select e.y,e.v,e.x from (select c as v,b as y,a as x from select_test.bind_select order by x,y) e where e.y > 0) d", true},
		{"wrong order", "(select a as x,b as y,c as v from select_test.bind_select order by y,x) d", false},
		{"partial order", "(select a as x,b as y,c as v from select_test.bind_select order by x) d", false},
		{"unordered", "(select a as x,b as y,c as v from select_test.bind_select) d", false},
		{"computed", "(select a+1 as x,b as y,c as v from select_test.bind_select order by x,y) d", false},
		{"limited", "(select a as x,b as y,c as v from select_test.bind_select order by x,y limit 10) d", false},
		{"subquery filter", "(select e.x,e.y,e.v from (select a as x,b as y,c as v from select_test.bind_select order by x,y) e where exists (select 1 from select_test.bind_select s where s.c=e.v)) d", false},
	} {
		for _, algorithm := range []string{"SORT", "COST", "HASH"} {
			t.Run(tc.name+"/"+algorithm, func(t *testing.T) {
				q := buildAutoRollupPlanSQLWithAlgorithm(t, 100_000,
					"select d.x,d.y,avg(d.v) from "+tc.source+" group by d.x,d.y with rollup", algorithm, 1, 4, 1).GetQuery()
				want := tc.wantSort && algorithm != "HASH"
				require.Equal(t, want, planHasSortRollup(q))
				if want {
					shape := reachableGroupingSetShape(q)
					require.Equal(t, 1, shape.tableScans)
					require.Equal(t, 1, shape.aggregates)
					sorts := 0
					for _, n := range q.Nodes {
						if n.NodeType == planpb.Node_SORT {
							sorts++
						}
					}
					require.Equal(t, 1, sorts, "reuse the existing global order")
				}
			})
		}
	}
}

func TestSortRollupProjectedInputCostUsesBaseNDV(t *testing.T) {
	// x is output slot 2, but comes from base slot 0. Its alias/position must
	// not borrow slot 2's low NDV and admit an expensive high-NDV SORT plan.
	for _, tc := range []struct {
		name     string
		ndv      float64
		wantSort bool
	}{
		{"low NDV", 4, true},
		{"high NDV", 25_000, false},
		{"unknown NDV", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := buildAutoRollupPlanSQLWithAlgorithm(t, 100_000,
				`select d.x,count(*) from
				 (select b,c,a as x from select_test.bind_select order by x) d
				 group by d.x with rollup`, "COST", tc.ndv, 1, 1).GetQuery()
			require.Equal(t, tc.wantSort, planHasSortRollup(q))
		})
	}
}
