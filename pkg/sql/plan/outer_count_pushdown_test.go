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
	"math"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func outerCountTestBuilder(t *testing.T) (*QueryBuilder, int32) {
	t.Helper()
	ctx := &fixedStatsCompilerContext{&statsCacheCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false, newPlanTestProcess(t)), statsCache: NewStatsCache(),
	}}
	b := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)
	keyType := plan.Type{Id: int32(types.T_int32), NotNullable: true}
	valueType := plan.Type{Id: int32(types.T_int64)}
	leftTag, rightTag := b.genNewBindTag(), b.genNewBindTag()
	leftDef := &plan.TableDef{TblId: 1001, Pkey: &plan.PrimaryKeyDef{Names: []string{"k"}, PkeyColName: "k"},
		Name2ColIndex: map[string]int32{"k": 0}, Cols: []*plan.ColDef{{Name: "k", Typ: keyType}}}
	rightDef := &plan.TableDef{TblId: 1002, Name2ColIndex: map[string]int32{"k": 0, "v": 1},
		Cols: []*plan.ColDef{{Name: "k", Typ: keyType}, {Name: "v", Typ: valueType}}}
	left := b.appendNode(&plan.Node{NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{leftTag},
		TableDef: leftDef, Stats: &plan.Stats{Outcnt: 100}}, nil)
	right := b.appendNode(&plan.Node{NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{rightTag},
		TableDef: rightDef, Stats: &plan.Stats{Outcnt: 10000}}, nil)
	b.tag2Table[leftTag], b.tag2Table[rightTag] = leftDef, rightDef
	ctx.statsCache.Set(rightDef.TblId, &statsinfo.StatsInfo{TableCnt: 10000, NdvMap: map[string]float64{"k": 100}})
	on, err := BindFuncExprImplByPlanExpr(b.GetContext(), "=", []*plan.Expr{GetColExpr(keyType, leftTag, 0), GetColExpr(keyType, rightTag, 0)})
	require.NoError(t, err)
	join := b.appendNode(&plan.Node{NodeType: plan.Node_JOIN, JoinType: plan.Node_LEFT,
		Children: []int32{left, right}, OnList: []*plan.Expr{on}}, nil)
	count, err := BindFuncExprImplByPlanExpr(b.GetContext(), "count", []*plan.Expr{GetColExpr(valueType, rightTag, 1)})
	require.NoError(t, err)
	groupTag, countTag := b.genNewBindTag(), b.genNewBindTag()
	agg := b.appendNode(&plan.Node{NodeType: plan.Node_AGG, Children: []int32{join},
		BindingTags: []int32{groupTag, countTag}, GroupingFlag: []bool{true},
		GroupBy: []*plan.Expr{GetColExpr(keyType, leftTag, 0)}, AggList: []*plan.Expr{count}}, nil)
	root := b.appendNode(&plan.Node{NodeType: plan.Node_PROJECT, Children: []int32{agg},
		BindingTags: []int32{b.genNewBindTag()}, ProjectList: []*plan.Expr{GetColExpr(keyType, groupTag, 0), GetColExpr(count.Typ, countTag, 0)}}, nil)
	return b, root
}

func TestOuterCountPushdownPreservesOutputs(t *testing.T) {
	b, root := outerCountTestBuilder(t)
	oldType := b.qry.Nodes[3].AggList[0].Typ
	b.qry.Nodes[2].IsRightJoin = true
	b.pushDownOuterCounts(root)
	restored := b.qry.Nodes[3]
	require.Equal(t, plan.Node_PROJECT, restored.NodeType)
	require.Equal(t, oldType, restored.ProjectList[1].Typ)
	require.Equal(t, "coalesce", restored.ProjectList[1].GetF().Func.ObjName)
	require.Equal(t, int64(0), restored.ProjectList[1].GetF().Args[1].GetLit().GetI64Val())
	require.Equal(t, restored.BindingTags[0], b.qry.Nodes[root].ProjectList[1].GetCol().RelPos)
	require.Equal(t, int32(1), b.qry.Nodes[root].ProjectList[1].GetCol().ColPos)
	join := b.qry.Nodes[2]
	require.False(t, join.IsRightJoin, "orientation must be recosted for the new build")
	partial := b.qry.Nodes[join.Children[1]]
	require.Equal(t, plan.Node_AGG, partial.NodeType)
	require.Equal(t, oldType, partial.AggList[0].Typ)
	require.Equal(t, b.qry.Nodes[1].BindingTags[0], partial.AggList[0].GetF().Args[0].GetCol().RelPos)
	require.Equal(t, partial.BindingTags[0], join.OnList[0].GetF().Args[1].GetCol().RelPos)
	require.NotEqual(t, restored.BindingTags[0], partial.BindingTags[0])
	before := proto.Clone(b.qry)
	b.pushDownOuterCounts(root)
	require.True(t, proto.Equal(before, b.qry), "the rewritten shape is idempotent")
}

func TestOuterCountPushdownRejections(t *testing.T) {
	for _, name := range []string{"nonunique", "distinct", "starcount", "inner", "mixed", "having", "rollup", "small", "high-ndv", "shared", "shared-step", "limit", "unsafe-filter", "residual", "disabled"} {
		t.Run(name, func(t *testing.T) {
			b, root := outerCountTestBuilder(t)
			agg, join, left, right := b.qry.Nodes[3], b.qry.Nodes[2], b.qry.Nodes[0], b.qry.Nodes[1]
			switch name {
			case "nonunique":
				left.TableDef.Pkey = nil
			case "distinct":
				fn := agg.AggList[0].GetF()
				fn.Func.Obj = int64(uint64(fn.Func.Obj) | function.Distinct)
			case "starcount":
				var err error
				agg.AggList[0], err = BindFuncExprImplByPlanExpr(b.GetContext(), "starcount", []*plan.Expr{makePlan2Int64ConstExprWithType(1)})
				require.NoError(t, err)
			case "inner":
				join.JoinType = plan.Node_INNER
			case "mixed":
				agg.AggList = append(agg.AggList, DeepCopyExpr(agg.AggList[0]))
			case "having":
				agg.FilterList = []*plan.Expr{makePlan2BoolConstExprWithType(true)}
			case "rollup":
				agg.GroupingFlag = []bool{false}
			case "small":
				right.Stats.Outcnt = 1
			case "high-ndv":
				left.Stats.Outcnt = 99
			case "shared":
				root = b.appendNode(&plan.Node{NodeType: plan.Node_UNION_ALL, Children: []int32{root, root}}, nil)
			case "shared-step":
				b.qry.Steps = []int32{root, right.NodeId}
			case "limit":
				right.Limit = makePlan2Int64ConstExprWithType(1)
			case "unsafe-filter":
				right.FilterList = []*plan.Expr{GetColExpr(plan.Type{Id: int32(types.T_int64)}, right.BindingTags[0], 1)}
			case "residual":
				pred := DeepCopyExpr(join.OnList[0])
				join.OnList = append(join.OnList, pred)
			case "disabled":
				b.optimizerHints = &OptimizerHints{aggPushDown: 1}
			}
			before := proto.Clone(b.qry)
			b.pushDownOuterCounts(root)
			require.True(t, proto.Equal(before, b.qry), "rejected proposals must not mutate the plan")
		})
	}
}

func TestOuterCountCostBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		rows, groups, preserved float64
		want                    bool
	}{
		{"threshold", 1024, 256, 256, true}, {"tiny", 1023, 100, 100, false},
		{"no-reduction", 1024, 257, 300, false}, {"larger-state", 1024, 100, 99, false},
		{"unknown", 1024, -1, 300, false}, {"nan", math.NaN(), 1, 300, false},
		{"infinite", 1024, math.Inf(1), 300, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, outerCountCostAllows(&plan.Stats{Outcnt: tc.rows}, &plan.Stats{Outcnt: tc.preserved}, tc.groups))
		})
	}
}

func TestOuterCountIntegerSpanBounds(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		oid                  types.T
		groups, lo, hi, want float64
		nullable             bool
	}{
		{"overlapping object NDV", types.T_int32, 3484895, 1, 1499999, 1499999, false},
		{"possible NULL group", types.T_int32, 3484895, 1, 1499999, 1500000, true},
		{"retain smaller estimate", types.T_int16, 10, 0, 100, 10, false},
		{"single value", types.T_uint8, 100, 7, 7, 1, false},
		{"uninitialized zone map", types.T_int32, 100, 0, 0, 100, false},
		{"unsigned full domain", types.T_uint32, 5e9, 0, math.MaxUint32, 4294967296, false},
		{"unsupported precision", types.T_int64, 1e6, 1, 100, 1e6, false},
		{"not integer", types.T_float64, 100, 1, 10, 100, false},
		{"unknown NDV", types.T_int32, -1, 1, 10, -1, false},
		{"invalid range", types.T_int32, 100, 10, 1, 100, false},
		{"fractional bound", types.T_int32, 100, 1.5, 10, 100, false},
		{"out of type domain", types.T_int8, 1000, -129, 127, 1000, false},
		{"negative unsigned", types.T_uint32, 100, -1, 10, 100, false},
		{"NaN", types.T_int32, 100, math.NaN(), 10, 100, false},
		{"infinity", types.T_int32, 100, 1, math.Inf(1), 100, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			typ := plan.Type{Id: int32(tc.oid), NotNullable: !tc.nullable}
			require.Equal(t, tc.want, outerCountIntegerSpanCap(tc.groups, typ, tc.lo, tc.hi))
		})
	}
}

func TestOuterCountSQLReachability(t *testing.T) {
	ctx := &fixedStatsCompilerContext{&statsCacheCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false, newPlanTestProcess(t)), statsCache: NewStatsCache(),
	}}
	for _, name := range []string{"customer", "orders"} {
		table := ctx.tables[name]
		require.NotNil(t, table)
		rows := float64(1000)
		if name == "orders" {
			rows = 10000
		}
		s := &statsinfo.StatsInfo{TableCnt: rows, BlockNumber: 1, TableName: name,
			NdvMap: map[string]float64{}, SizeMap: map[string]uint64{}, DataTypeMap: map[string]uint64{}}
		for _, col := range table.Cols {
			s.NdvMap[col.Name], s.SizeMap[col.Name], s.DataTypeMap[col.Name] = rows, uint64(rows*8), uint64(col.Typ.Id)
		}
		if name == "orders" {
			// An overestimated NDV must not exceed the known INT32 span.
			s.NdvMap["o_custkey"] = 3500
			s.MinValMap = map[string]float64{"o_custkey": 1}
			s.MaxValMap = map[string]float64{"o_custkey": 999}
		}
		ctx.statsCache.Set(table.TblId, s)
	}
	sql := `select c_count, count(*) as custdist from
		(select c_custkey, count(o_orderkey) as c_count from customer
		 left join orders on c_custkey=o_custkey and o_comment not like '%pending%accounts%'
		 group by c_custkey) x group by c_count order by custdist desc, c_count desc`
	stmt, err := mysql.ParseOne(ctx.GetContext(), sql, 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)
	p, err := BuildPlan(ctx, stmt, false, nil)
	require.NoError(t, err)
	query := p.GetQuery()
	found := false
	seen := make(map[int32]bool)
	var visit func(int32)
	visit = func(id int32) {
		if seen[id] {
			return
		}
		seen[id] = true
		n := query.Nodes[id]
		if n.NodeType == plan.Node_AGG {
			for _, agg := range n.AggList {
				fn := agg.GetF()
				if fn == nil || fn.Func == nil || fn.Func.ObjName != "count" {
					continue
				}
				child := query.Nodes[n.Children[0]]
				if child.NodeType == plan.Node_TABLE_SCAN && child.TableDef.Name == "orders" {
					found = true
				}
			}
		}
		for _, child := range n.Children {
			visit(child)
		}
	}
	for _, root := range query.Steps {
		visit(root)
	}
	require.True(t, found, "unchanged Q13 must count ORDERS before the outer join: %s", query.String())
}
