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
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestPreparedPaginationTopInputSafe(t *testing.T) {
	col := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
	predicate, err := BindFuncExprImplByPlanExpr(context.Background(), ">", []*planpb.Expr{col, MakePlan2Int64ConstExprWithType(0)})
	require.NoError(t, err)
	div, err := BindFuncExprImplByPlanExpr(context.Background(), "div", []*planpb.Expr{MakePlan2Int64ConstExprWithType(1), col})
	require.NoError(t, err)
	unsafePredicate, err := BindFuncExprImplByPlanExpr(context.Background(), ">", []*planpb.Expr{div, MakePlan2Int64ConstExprWithType(0)})
	require.NoError(t, err)
	param := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
	paramPredicate, err := BindFuncExprImplByPlanExpr(context.Background(), ">", []*planpb.Expr{col, param})
	require.NoError(t, err)

	newQuery := func() (*planpb.Query, *planpb.Node) {
		scan := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, TableDef: &planpb.TableDef{}, ProjectList: []*planpb.Expr{col}, FilterList: []*planpb.Expr{predicate}}
		project := &planpb.Node{NodeType: planpb.Node_PROJECT, Children: []int32{0}, ProjectList: []*planpb.Expr{col}}
		sort := &planpb.Node{NodeType: planpb.Node_SORT, Children: []int32{1}, OrderBy: []*planpb.OrderBySpec{{Expr: col}}}
		return &planpb.Query{StmtType: planpb.Query_SELECT, Nodes: []*planpb.Node{scan, project, sort}}, sort
	}
	tests := []struct {
		name string
		edit func(*planpb.Query, *planpb.Node)
		want bool
	}{
		{name: "total scan and project", want: true},
		{name: "filter node", edit: func(q *planpb.Query, _ *planpb.Node) { q.Nodes[1].NodeType = planpb.Node_FILTER }, want: true},
		{name: "row diagnostic", edit: func(q *planpb.Query, _ *planpb.Node) { q.Nodes[0].FilterList = []*planpb.Expr{unsafePredicate} }},
		{name: "runtime filter conversion", edit: func(q *planpb.Query, _ *planpb.Node) { q.Nodes[0].FilterList = []*planpb.Expr{paramPredicate} }},
		{name: "row diagnostic in projection", edit: func(q *planpb.Query, _ *planpb.Node) { q.Nodes[1].ProjectList = []*planpb.Expr{div} }},
		{name: "blocking or unknown input", edit: func(q *planpb.Query, _ *planpb.Node) { q.Nodes[1].NodeType = planpb.Node_AGG }},
		{name: "filter barrier", edit: func(q *planpb.Query, _ *planpb.Node) { q.Nodes[1].FilterIsBarrier = true }},
		{name: "nested pagination", edit: func(q *planpb.Query, _ *planpb.Node) { q.Nodes[1].Limit = MakePlan2Uint64ConstExprWithType(1) }},
		{name: "materialized input", edit: func(q *planpb.Query, _ *planpb.Node) { q.Nodes[0].SourceStep = []int32{0} }},
		{name: "cycle", edit: func(q *planpb.Query, _ *planpb.Node) { q.Nodes[1].Children[0] = 1 }},
		{name: "invalid child", edit: func(q *planpb.Query, _ *planpb.Node) { q.Nodes[1].Children[0] = 99 }},
		{name: "computed sort key", edit: func(_ *planpb.Query, s *planpb.Node) { s.OrderBy[0] = &planpb.OrderBySpec{Expr: div} }},
		{name: "unproved sort domain", edit: func(_ *planpb.Query, s *planpb.Node) {
			s.OrderBy[0] = &planpb.OrderBySpec{Expr: &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_json)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}}
		}},
		{name: "nonselect", edit: func(q *planpb.Query, _ *planpb.Node) { q.StmtType = planpb.Query_UPDATE }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			query, sort := newQuery()
			if tc.edit != nil {
				tc.edit(query, sort)
			}
			require.Equal(t, tc.want, PreparedPaginationTopInputSafe(query, sort))
		})
	}
}
