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

func aggregateMembershipTestBuilder(t *testing.T) (*QueryBuilder, int32) {
	t.Helper()
	ctx := &fixedStatsCompilerContext{&statsCacheCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false, newPlanTestProcess(t)), statsCache: NewStatsCache(),
	}}
	b := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)
	k := plan.Type{Id: int32(types.T_int32), NotNullable: true}
	v := plan.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2}
	text := plan.Type{Id: int32(types.T_varchar), Width: 55}
	bind := func(name string, args ...*plan.Expr) *plan.Expr {
		expr, err := BindFuncExprImplByPlanExpr(b.GetContext(), name, args)
		require.NoError(t, err)
		return expr
	}
	scans := make([]*plan.Node, 0, 3)
	for i, cols := range [][]*plan.ColDef{
		{{Name: "part", Typ: k}, {Name: "supp", Typ: k}},
		{{Name: "part", Typ: k}, {Name: "name", Typ: text}},
		{{Name: "part", Typ: k}, {Name: "supp", Typ: k}, {Name: "v", Typ: v}},
	} {
		tag := b.genNewBindTag()
		def := &plan.TableDef{TblId: uint64(1000 + i), Cols: cols}
		id := b.appendNode(&plan.Node{NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{tag},
			ObjRef: &plan.ObjectRef{Obj: int64(def.TblId)}, TableDef: def,
			Stats: &plan.Stats{Outcnt: 100000, TableCnt: 100000, Selectivity: 1}}, nil)
		b.tag2Table[tag] = def
		ctx.statsCache.Set(def.TblId, &statsinfo.StatsInfo{TableCnt: 100000,
			NdvMap: map[string]float64{"part": 5000, "supp": 2000}})
		scans = append(scans, b.qry.Nodes[id])
	}
	left, domain, fact := scans[0], scans[1], scans[2]
	domain.FilterList = []*plan.Expr{bind("like", GetColExpr(text, domain.BindingTags[0], 1), makePlan2StringConstExprWithType("lime%"))}
	domain.Stats = &plan.Stats{Outcnt: 100, TableCnt: 10000, Selectivity: .01}
	semi := b.appendNode(&plan.Node{NodeType: plan.Node_JOIN, JoinType: plan.Node_SEMI, Children: []int32{left.NodeId, domain.NodeId},
		OnList: []*plan.Expr{bind("=", GetColExpr(k, left.BindingTags[0], 0), GetColExpr(k, domain.BindingTags[0], 0))}}, nil)
	sum := bind("sum", GetColExpr(v, fact.BindingTags[0], 2))
	groupTag, sumTag := b.genNewBindTag(), b.genNewBindTag()
	agg := b.appendNode(&plan.Node{NodeType: plan.Node_AGG, Children: []int32{fact.NodeId}, BindingTags: []int32{groupTag, sumTag},
		GroupBy: []*plan.Expr{GetColExpr(k, fact.BindingTags[0], 0), GetColExpr(k, fact.BindingTags[0], 1)},
		AggList: []*plan.Expr{sum}, Stats: &plan.Stats{Outcnt: 50000}}, nil)
	join := b.appendNode(&plan.Node{NodeType: plan.Node_JOIN, JoinType: plan.Node_LEFT, Children: []int32{semi, agg},
		OnList: []*plan.Expr{
			bind("=", GetColExpr(k, left.BindingTags[0], 0), GetColExpr(k, groupTag, 0)),
			bind("=", GetColExpr(k, left.BindingTags[0], 1), GetColExpr(k, groupTag, 1)),
		}}, nil)
	root := b.appendNode(&plan.Node{NodeType: plan.Node_PROJECT, Children: []int32{join}, BindingTags: []int32{b.genNewBindTag()},
		ProjectList: []*plan.Expr{GetColExpr(k, left.BindingTags[0], 0), GetColExpr(sum.Typ, sumTag, 0)}}, nil)
	return b, root
}

func TestAggregateMembershipRestrictsOnlyFactInput(t *testing.T) {
	b, root := aggregateMembershipTestBuilder(t)
	original := proto.Clone(b.qry.Nodes[1]).(*plan.Node)
	oldOn := DeepCopyExprList(b.qry.Nodes[5].OnList)
	require.True(t, b.restrictAggregateMembership(root))
	agg := b.qry.Nodes[4]
	input := b.qry.Nodes[agg.Children[0]]
	require.Equal(t, plan.Node_JOIN, input.NodeType)
	require.Equal(t, plan.Node_SEMI, input.JoinType)
	require.Equal(t, int32(2), input.Children[0])
	copy := b.qry.Nodes[input.Children[1]]
	require.NotEqual(t, original.BindingTags[0], copy.BindingTags[0])
	require.True(t, proto.Equal(original.ObjRef, copy.ObjRef))
	require.True(t, proto.Equal(original.ScanSnapshot, copy.ScanSnapshot))
	require.True(t, proto.Equal(original, b.qry.Nodes[1]), "the original mandatory membership is retained unchanged")
	require.Equal(t, original.BindingTags[0], b.qry.Nodes[3].OnList[0].GetF().Args[1].GetCol().RelPos)
	require.Equal(t, copy.BindingTags[0], copy.FilterList[0].GetF().Args[0].GetCol().RelPos)
	require.Equal(t, oldOn, b.qry.Nodes[5].OnList, "retain both correlation keys and original scalar consumer")
	before := proto.Clone(b.qry)
	require.False(t, b.restrictAggregateMembership(root))
	require.True(t, proto.Equal(before, b.qry), "an already restricted aggregate is not wrapped again")
}

func TestAggregateMembershipRejectsUnsafeOrUnprofitableDomains(t *testing.T) {
	for _, name := range []string{"negative", "optional", "mixed-type", "no-filter", "barrier", "unknown-ndv", "expensive-domain", "unselective", "distinct", "unsafe-sum", "shared", "disabled"} {
		t.Run(name, func(t *testing.T) {
			b, root := aggregateMembershipTestBuilder(t)
			domain, semi, agg := b.qry.Nodes[1], b.qry.Nodes[3], b.qry.Nodes[4]
			switch name {
			case "negative":
				semi.JoinType = plan.Node_ANTI
			case "optional":
				semi.JoinType = plan.Node_LEFT
			case "mixed-type":
				semi.OnList[0].GetF().Args[1].Typ.Id = int32(types.T_int64)
			case "no-filter":
				domain.FilterList = nil
			case "barrier":
				domain.FilterIsBarrier = true
			case "unknown-ndv":
				b.compCtx.GetStatsCache().Delete(domain.TableDef.TblId)
			case "expensive-domain":
				domain.Stats.TableCnt = 1e12
			case "unselective":
				domain.Stats.Outcnt, domain.Stats.Selectivity = 100000, 1
			case "distinct":
				fn := agg.AggList[0].GetF()
				fn.Func.Obj = int64(uint64(fn.Func.Obj) | function.Distinct)
			case "unsafe-sum":
				agg.AggList[0].Typ.Width = 10
			case "shared":
				b.qry.Steps = []int32{root, domain.NodeId}
			case "disabled":
				b.optimizerHints = &OptimizerHints{aggPushDown: 1}
			}
			before := proto.Clone(b.qry)
			require.False(t, b.restrictAggregateMembership(root))
			require.True(t, proto.Equal(before, b.qry), "rejection must retain the valid original graph")
		})
	}
}

func TestAggregateMembershipSQLReachability(t *testing.T) {
	ctx := &fixedStatsCompilerContext{&statsCacheCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false, newPlanTestProcess(t)), statsCache: NewStatsCache(),
	}}
	for _, name := range []string{"part", "partsupp", "lineitem", "supplier", "nation"} {
		table := ctx.tables[name]
		rows := map[string]float64{"part": 20000, "partsupp": 80000, "lineitem": 6000000, "supplier": 1000, "nation": 25}[name]
		s := &statsinfo.StatsInfo{TableCnt: rows, BlockNumber: 1, TableName: name,
			NdvMap: map[string]float64{}, SizeMap: map[string]uint64{}, DataTypeMap: map[string]uint64{}}
		for _, col := range table.Cols {
			s.NdvMap[col.Name], s.SizeMap[col.Name], s.DataTypeMap[col.Name] = rows, uint64(rows*8), uint64(col.Typ.Id)
		}
		s.NdvMap["p_name"] = 20000
		s.NdvMap["ps_partkey"], s.NdvMap["l_partkey"] = 20000, 20000
		s.NdvMap["ps_suppkey"], s.NdvMap["l_suppkey"] = 1000, 1000
		ctx.statsCache.Set(table.TblId, s)
	}
	sql := `select s_name,s_address from supplier,nation where s_suppkey in
		(select ps_suppkey from partsupp where ps_partkey in
		 (select p_partkey from part where p_name like 'lime%')
		 and ps_availqty > (select .5*sum(l_quantity) from lineitem
		 where l_partkey=ps_partkey and l_suppkey=ps_suppkey
		 and l_shipdate>=date '1993-01-01' and l_shipdate<date '1994-01-01'))
		 and s_nationkey=n_nationkey and n_name='VIETNAM' order by s_name`
	stmt, err := mysql.ParseOne(ctx.GetContext(), sql, 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)
	p, err := BuildPlan(ctx, stmt, false, nil)
	require.NoError(t, err)
	query := p.GetQuery()
	seen := make(map[int32]bool)
	parts := 0
	var visit func(int32)
	visit = func(id int32) {
		if seen[id] {
			return
		}
		seen[id] = true
		n := query.Nodes[id]
		if n.NodeType == plan.Node_TABLE_SCAN && n.TableDef.Name == "part" {
			parts++
		}
		for _, child := range n.Children {
			visit(child)
		}
	}
	for _, root := range query.Steps {
		visit(root)
	}
	require.Equal(t, 2, parts, "unchanged Q20 must copy the part domain below its aggregate: %s", query.String())
}
