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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func groupedMeasureTestBuilder(t *testing.T) (*QueryBuilder, int32) {
	t.Helper()
	b := NewQueryBuilder(plan.Query_SELECT, NewEmptyCompilerContext(newPlanTestProcess(t)), false, false)
	k := plan.Type{Id: int32(types.T_int32), NotNullable: true}
	v := plan.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2}
	label := plan.Type{Id: int32(types.T_varchar), Width: 20}
	bind := func(name string, args ...*plan.Expr) *plan.Expr {
		e, err := BindFuncExprImplByPlanExpr(b.GetContext(), name, args)
		require.NoError(t, err)
		return e
	}
	var facts []*plan.Node
	for range 2 {
		tag := b.genNewBindTag()
		id := b.appendNode(&plan.Node{NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{tag},
			ObjRef:   &plan.ObjectRef{Obj: 1000, ObjName: "facts"},
			TableDef: &plan.TableDef{TblId: 1000, Cols: []*plan.ColDef{{Name: "k", Typ: k}, {Name: "v", Typ: v}}},
			Stats:    &plan.Stats{Outcnt: 10000}}, nil)
		facts = append(facts, b.qry.Nodes[id])
	}
	dimTag := b.genNewBindTag()
	dim := b.appendNode(&plan.Node{NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{dimTag},
		ObjRef: &plan.ObjectRef{Obj: 1001, ObjName: "dimension"},
		TableDef: &plan.TableDef{TblId: 1001, Pkey: &plan.PrimaryKeyDef{Names: []string{"k"}, PkeyColName: "k"},
			Name2ColIndex: map[string]int32{"k": 0}, Cols: []*plan.ColDef{{Name: "k", Typ: k}, {Name: "label", Typ: label}}},
		Stats: &plan.Stats{Outcnt: 100}}, nil)
	qualSum := bind("sum", GetColExpr(v, facts[0].BindingTags[0], 1))
	qual := b.appendNode(&plan.Node{NodeType: plan.Node_AGG, Children: []int32{facts[0].NodeId},
		BindingTags: []int32{b.genNewBindTag(), b.genNewBindTag()},
		GroupBy:     []*plan.Expr{GetColExpr(k, facts[0].BindingTags[0], 0)}, AggList: []*plan.Expr{qualSum},
		FilterList: []*plan.Expr{makePlan2BoolConstExprWithType(true)}, Stats: &plan.Stats{Outcnt: 10}}, nil)
	qualProjectTag := b.genNewBindTag()
	qualProject := b.appendNode(&plan.Node{NodeType: plan.Node_PROJECT, Children: []int32{qual},
		BindingTags: []int32{qualProjectTag}, ProjectList: []*plan.Expr{GetColExpr(k, b.qry.Nodes[qual].BindingTags[0], 0)},
		Stats: &plan.Stats{Outcnt: 10}}, nil)
	semi := b.appendNode(&plan.Node{NodeType: plan.Node_JOIN, JoinType: plan.Node_SEMI, Children: []int32{dim, qualProject},
		OnList: []*plan.Expr{bind("=", GetColExpr(k, dimTag, 0), GetColExpr(k, qualProjectTag, 0))}}, nil)
	join := b.appendNode(&plan.Node{NodeType: plan.Node_JOIN, JoinType: plan.Node_INNER, Children: []int32{semi, facts[1].NodeId},
		OnList: []*plan.Expr{bind("=", GetColExpr(k, dimTag, 0), GetColExpr(k, facts[1].BindingTags[0], 0))}}, nil)
	finalSum := bind("sum", GetColExpr(v, facts[1].BindingTags[0], 1))
	agg := b.appendNode(&plan.Node{NodeType: plan.Node_AGG, Children: []int32{join},
		BindingTags: []int32{b.genNewBindTag(), b.genNewBindTag()},
		GroupBy:     []*plan.Expr{GetColExpr(k, dimTag, 0), GetColExpr(label, dimTag, 1)}, AggList: []*plan.Expr{finalSum}}, nil)
	root := b.appendNode(&plan.Node{NodeType: plan.Node_PROJECT, Children: []int32{agg},
		BindingTags: []int32{b.genNewBindTag()}, ProjectList: []*plan.Expr{
			GetColExpr(k, b.qry.Nodes[agg].BindingTags[0], 0), GetColExpr(finalSum.Typ, b.qry.Nodes[agg].BindingTags[1], 0)}}, nil)
	return b, root
}

func TestGroupedMeasureReuseCarriesExactMeasure(t *testing.T) {
	b, root := groupedMeasureTestBuilder(t)
	oldType := b.qry.Nodes[7].AggList[0].Typ
	b.reuseGroupedMeasures(root)
	projection := b.qry.Nodes[7]
	require.Equal(t, plan.Node_PROJECT, projection.NodeType)
	require.Len(t, projection.ProjectList, 3)
	require.Equal(t, oldType, projection.ProjectList[2].Typ)
	require.Equal(t, int32(2), b.qry.Nodes[root].ProjectList[1].GetCol().ColPos)
	qualProject := b.qry.Nodes[4]
	require.Len(t, qualProject.ProjectList, 2, "expose the already computed measure through the qualifier's projection")
	require.Equal(t, qualProject.BindingTags[0], projection.ProjectList[2].GetCol().RelPos)
	seen := make(map[int32]bool)
	var visit func(int32)
	visit = func(id int32) {
		if seen[id] {
			return
		}
		seen[id] = true
		for _, child := range b.qry.Nodes[id].Children {
			visit(child)
		}
	}
	visit(root)
	require.True(t, seen[0], "retain the qualifying fact scan")
	require.False(t, seen[1], "retire the repeated fact scan")
	require.True(t, seen[2], "missing dimensions still filter qualified groups")
	before := proto.Clone(b.qry)
	b.reuseGroupedMeasures(root)
	require.True(t, proto.Equal(before, b.qry))
}

func TestGroupedMeasureReuseRejectsDifferentBagsAndMultiplicity(t *testing.T) {
	for _, name := range []string{"nonunique", "changed-filter", "different-table", "different-argument", "distinct", "no-key-group", "unsafe-sum", "outer", "having", "shared", "cost", "disabled"} {
		t.Run(name, func(t *testing.T) {
			b, root := groupedMeasureTestBuilder(t)
			fact, dim, join, agg := b.qry.Nodes[1], b.qry.Nodes[2], b.qry.Nodes[6], b.qry.Nodes[7]
			switch name {
			case "nonunique":
				dim.TableDef.Pkey = nil
			case "changed-filter":
				fact.FilterList = []*plan.Expr{makePlan2BoolConstExprWithType(false)}
			case "different-table":
				fact.ObjRef = &plan.ObjectRef{Obj: 2000, ObjName: "other"}
			case "different-argument":
				agg.AggList[0].GetF().Args[0].GetCol().ColPos = 0
			case "distinct":
				fn := agg.AggList[0].GetF()
				fn.Func.Obj = int64(uint64(fn.Func.Obj) | function.Distinct)
			case "no-key-group":
				agg.GroupBy = agg.GroupBy[1:]
			case "unsafe-sum":
				agg.AggList[0].Typ.Width = 10
			case "outer":
				join.JoinType = plan.Node_LEFT
			case "having":
				agg.FilterList = []*plan.Expr{makePlan2BoolConstExprWithType(true)}
			case "shared":
				b.qry.Steps = []int32{root, fact.NodeId}
			case "cost":
				b.qry.Nodes[4].Stats.Outcnt = 10000
			case "disabled":
				b.optimizerHints = &OptimizerHints{aggPushDown: 1}
			}
			before := proto.Clone(b.qry)
			b.reuseGroupedMeasures(root)
			require.True(t, proto.Equal(before, b.qry), "a rejected proposal must keep its original graph")
		})
	}
}

func TestGroupedMeasureSQLReachability(t *testing.T) {
	ctx := &fixedStatsCompilerContext{&statsCacheCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false, newPlanTestProcess(t)), statsCache: NewStatsCache(),
	}}
	for _, name := range []string{"customer", "orders", "lineitem"} {
		table := ctx.tables[name]
		rows := map[string]float64{"customer": 1000, "orders": 10000, "lineitem": 40000}[name]
		s := &statsinfo.StatsInfo{TableCnt: rows, BlockNumber: 1, TableName: name,
			NdvMap: map[string]float64{}, SizeMap: map[string]uint64{}, DataTypeMap: map[string]uint64{}}
		for _, col := range table.Cols {
			s.NdvMap[col.Name], s.SizeMap[col.Name], s.DataTypeMap[col.Name] = rows, uint64(rows*8), uint64(col.Typ.Id)
		}
		if name == "orders" {
			s.NdvMap["o_custkey"] = 1000
		} else if name == "lineitem" {
			s.NdvMap["l_orderkey"] = 10000
		}
		ctx.statsCache.Set(table.TblId, s)
	}
	sql := `select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity)
		from customer, orders, lineitem
		where o_orderkey in (select l_orderkey from lineitem group by l_orderkey having sum(l_quantity)>314)
		and c_custkey=o_custkey and o_orderkey=l_orderkey
		group by c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice
		order by o_totalprice desc,o_orderdate limit 100`
	stmt, err := mysql.ParseOne(ctx.GetContext(), sql, 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)
	p, err := BuildPlan(ctx, stmt, false, nil)
	require.NoError(t, err)
	query := p.GetQuery()
	seen := make(map[int32]bool)
	facts := 0
	var visit func(int32)
	visit = func(id int32) {
		if seen[id] {
			return
		}
		seen[id] = true
		n := query.Nodes[id]
		if n.NodeType == plan.Node_TABLE_SCAN && n.TableDef.Name == "lineitem" {
			facts++
		}
		for _, child := range n.Children {
			visit(child)
		}
	}
	for _, root := range query.Steps {
		visit(root)
	}
	require.Equal(t, 1, facts, "unchanged Q18 must retire its repeated fact scan: %s", query.String())
}
