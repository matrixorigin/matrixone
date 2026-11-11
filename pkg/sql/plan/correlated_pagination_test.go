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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestPaginationExpressionSafeDecimalLiteralCast(t *testing.T) {
	cast := func(input *plan.Expr, width, scale int32) *plan.Expr {
		target := plan.Type{Id: int32(types.T_decimal128), Width: width, Scale: scale}
		return &plan.Expr{Typ: target, Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "cast"},
			Args: []*plan.Expr{input, {Typ: target, Expr: &plan.Expr_T{T: &plan.TargetType{}}}},
		}}}
	}
	literal := makePlan2Int64ConstExprWithType(99)
	column := GetColExpr(literal.Typ, 1, 0)
	require.True(t, paginationExpressionSafe(cast(literal, 20, 0)))
	require.True(t, paginationExpressionSafe(cast(literal, 21, 1)))
	require.False(t, paginationExpressionSafe(cast(literal, 19, 0)))
	require.False(t, paginationExpressionSafe(cast(literal, 21, 2)))
	require.False(t, paginationExpressionSafe(cast(column, 38, 0)))
}

func TestPaginationOuterIntegerLiteralCasts(t *testing.T) {
	for _, tc := range []struct {
		name   string
		input  *plan.Expr
		target types.T
		safe   bool
	}{
		{"signed minimum", makePlan2Int64ConstExprWithType(-128), types.T_int8, true},
		{"signed maximum", makePlan2Int64ConstExprWithType(127), types.T_int8, true},
		{"signed underflow", makePlan2Int64ConstExprWithType(-129), types.T_int8, false},
		{"signed overflow", makePlan2Int64ConstExprWithType(128), types.T_int8, false},
		{"unsigned maximum", makePlan2Uint64ConstExprWithType(255), types.T_uint8, true},
		{"unsigned overflow", makePlan2Uint64ConstExprWithType(256), types.T_uint8, false},
		{"negative to unsigned", makePlan2Int64ConstExprWithType(-1), types.T_uint64, false},
		{"unsigned to signed overflow", makePlan2Uint64ConstExprWithType(^uint64(0)), types.T_int64, false},
		{"parameter", &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_P{P: &plan.ParamRef{}}}, types.T_int8, false},
		{"string", makePlan2StringConstExprWithType("bad"), types.T_int8, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := plan.Type{Id: int32(tc.target)}
			expr := &plan.Expr{Typ: target, Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{ObjName: "cast", Obj: function.EncodeOverloadID(int32(function.CAST), 0)},
				Args: []*plan.Expr{tc.input, {Typ: target, Expr: &plan.Expr_T{T: &plan.TargetType{}}}},
			}}}
			require.Equal(t, tc.safe, paginationExpressionSafeWithLiteralCasts(expr, true))
			require.False(t, paginationExpressionSafe(expr), "inner pagination must keep its original cast policy")
		})
	}
}

func TestPaginationPreservableBoundsAndExpressions(t *testing.T) {
	uint64Type := plan.Type{Id: int32(types.T_uint64)}
	param := &plan.Expr{Typ: uint64Type, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}}
	castParam := &plan.Expr{Typ: uint64Type, Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(int32(function.CAST), 0), ObjName: "cast"},
		Args: []*plan.Expr{param, {Typ: uint64Type, Expr: &plan.Expr_T{T: &plan.TargetType{}}}},
	}}}
	for _, tc := range []struct {
		name        string
		expr        *plan.Expr
		ok, literal bool
	}{
		{"absent offset", nil, true, true},
		{"literal", makePlan2Uint64ConstExprWithType(1), true, true},
		{"uint64 parameter", param, true, false},
		{"cast parameter", castParam, true, false},
		{"signed parameter", &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_P{P: &plan.ParamRef{}}}, false, false},
		{"session variable", &plan.Expr{Typ: uint64Type, Expr: &plan.Expr_V{V: &plan.VarRef{Name: "v"}}}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ok, literal := paginationPreservableBound(tc.expr)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.literal, literal)
		})
	}
	random := &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(int32(function.RANDOM_BYTES), 0), ObjName: "random_bytes"},
		Args: []*plan.Expr{makePlan2Int64ConstExprWithType(4)},
	}}}
	require.False(t, paginationPreservableExpr(random))
	require.False(t, paginationPreservableExpr(&plan.Expr{Expr: &plan.Expr_V{V: &plan.VarRef{Name: "v"}}}))
	require.True(t, paginationPreservableExpr(castParam))
}

func TestPaginationAggregateArgNestedCasts(t *testing.T) {
	inputType := plan.Type{Id: int32(types.T_int64)}
	for _, tc := range []struct {
		name       string
		input      *plan.Expr
		alwaysNull bool
	}{
		{"scan column", GetColExpr(inputType, 7, 0), true},
		{"literal", makePlan2Int64ConstExprWithType(1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr := tc.input
			for range 64 {
				expr = &plan.Expr{Typ: inputType, Expr: &plan.Expr_F{F: &plan.Function{
					Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(int32(function.CAST), 0), ObjName: "cast"},
					Args: []*plan.Expr{expr, {Typ: inputType, Expr: &plan.Expr_T{T: &plan.TargetType{}}}},
				}}}
			}
			safe, alwaysNull := paginationAggregateArgOnNull(expr, 7, 1)
			require.True(t, safe)
			require.Equal(t, tc.alwaysNull, alwaysNull)
		})
	}
}

func TestContainsCorrelatedNonEqComparison(t *testing.T) {
	col := GetColExpr(plan.Type{Id: int32(types.T_int64)}, 1, 0)
	corr := &plan.Expr{Expr: &plan.Expr_Corr{Corr: &plan.CorrColRef{Depth: 1}}}
	fn := func(name string, args ...*plan.Expr) *plan.Expr {
		return &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: name}, Args: args}}}
	}
	equal := fn("=", col, corr)
	innerOnly := fn(">", col, makePlan2Int64ConstExprWithType(0))
	correlated := fn("<", col, corr)
	for _, tc := range []struct {
		name  string
		expr  *plan.Expr
		found bool
	}{
		{"inner comparison under or", fn("or", equal, innerOnly), false},
		{"correlated comparison under or", fn("or", equal, correlated), true},
		{"correlated comparison under not and", fn("not", fn("and", equal, correlated)), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.found, containsCorrelatedNonEqComparison(tc.expr))
		})
	}
	deep := correlated
	for range 64 {
		deep = fn("not", deep)
	}
	require.True(t, containsCorrelatedNonEqComparison(deep))
}

func TestCorrelatedPaginationPreserveBoundary(t *testing.T) {
	for _, tc := range []struct {
		name string
		sort bool
	}{
		{"filter boundary is not preserved", false},
		{"sort boundary is preserved", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, false)
			pred := &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_Corr{Corr: &plan.CorrColRef{Depth: 1}}}
			scan := &plan.Node{NodeType: plan.Node_TABLE_SCAN}
			filter := &plan.Node{NodeType: plan.Node_FILTER, Children: []int32{0}, FilterList: []*plan.Expr{pred}}
			builder.qry.Nodes = []*plan.Node{scan, filter}
			root := int32(1)
			if tc.sort {
				builder.qry.Nodes = append(builder.qry.Nodes, &plan.Node{NodeType: plan.Node_SORT, Children: []int32{1}, Limit: makePlan2Uint64ConstExprWithType(1)})
				root = 2
			} else {
				filter.Limit = makePlan2Uint64ConstExprWithType(1)
			}
			ctx := NewBindContext(builder, nil)
			_, _, preserved, err := builder.prepareCorrelatedPagination(0, root, ctx, plan.SubqueryRef_SCALAR)
			require.NoError(t, err)
			if tc.sort {
				require.Equal(t, root, preserved)
			} else {
				require.Equal(t, int32(-1), preserved)
			}
		})
	}
}

func TestCorrelatedPaginationOuterOnlyPreservesLimit(t *testing.T) {
	for _, sql := range []string{
		`select n.n_nationkey,(select abs(r.r_regionkey) from tpch.region r where n.n_nationkey>0 order by r.r_regionkey limit 1) from tpch.nation n`,
		`select n.n_nationkey,(select r.r_regionkey+1 from tpch.region r where n.n_nationkey>0 order by r.r_regionkey limit 1) from tpch.nation n`,
		`select n.n_nationkey,(select r.r_regionkey from tpch.region r where n.n_nationkey>0 and abs(r.r_regionkey)>0 order by r.r_regionkey limit 1) from tpch.nation n`,
		`select n.n_nationkey,(select cast(r.r_name as signed) from tpch.region r where n.n_nationkey>0 order by r.r_regionkey limit 1) from tpch.nation n`,
		`select n.n_nationkey,(select r.r_regionkey from tpch.region r where n.n_nationkey>0 and cast(r.r_name as signed)>0 order by r.r_regionkey limit 1) from tpch.nation n`,
		`select n.n_nationkey,(select r.r_regionkey from tpch.region r where n.n_nationkey>0 order by cast(r.r_name as signed) limit 1 offset 1) from tpch.nation n`,
	} {
		result, err := runOneStmt(NewMockOptimizer(true), t, sql)
		require.NoError(t, err, sql)
		q := result.GetQuery()
		foundLimit := false
		var walk func(int32, bool)
		walk = func(id int32, rightOfJoin bool) {
			node := q.Nodes[id]
			require.NotEqual(t, plan.Node_WINDOW, node.NodeType, sql)
			if rightOfJoin && node.Limit != nil {
				foundLimit = true
			}
			for i, child := range node.Children {
				walk(child, rightOfJoin || node.NodeType == plan.Node_JOIN && i == 1)
			}
		}
		for _, root := range q.Steps {
			walk(root, false)
		}
		require.True(t, foundLimit, "outer-only pagination must remain on the inner input: %s", sql)
	}
}

func TestCorrelatedPaginationPlacement(t *testing.T) {
	cases := []struct {
		name, sql     string
		rankAfterJoin bool
	}{
		{"equality with a different order key", `select n.n_nationkey, (select r.r_regionkey from tpch.region r where r.r_regionkey=n.n_regionkey order by r.r_name desc limit 1) from tpch.nation n`, false},
		{"string equality", `select n.n_nationkey, (select r.r_regionkey from tpch.region r where r.r_name=n.n_name order by r.r_regionkey desc limit 1) from tpch.nation n`, true},
		{"string equality without outer identity", `select n.n_nationkey, (select r.r_regionkey from tpch.region r where r.r_name=n.n_name order by r.r_regionkey desc limit 1) from (select n_nationkey,n_name from tpch.nation union all select n_nationkey,n_name from tpch.nation) n`, false},
		{"widening cast", `select n.n_nationkey, (select cast(r.r_regionkey as bigint) from tpch.region r where r.r_regionkey=n.n_regionkey and r.r_regionkey<=n.n_nationkey order by r.r_regionkey desc limit 1) from tpch.nation n`, true},
		{"range", `select n.n_nationkey, (select r.r_regionkey from tpch.region r where r.r_regionkey=n.n_regionkey and r.r_regionkey<=n.n_nationkey order by r.r_regionkey desc limit 1) from tpch.nation n`, true},
		{"range offset", `select n.n_nationkey, (select r.r_regionkey from tpch.region r where r.r_regionkey=n.n_regionkey and r.r_regionkey<=n.n_nationkey order by r.r_regionkey desc limit 1 offset 1) from tpch.nation n`, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := runOneStmt(NewMockOptimizer(true), t, tc.sql)
			require.NoError(t, err)
			q := result.GetQuery()
			require.NotNil(t, q)
			assertReachablePlanHasNoCorrelatedExpr(t, q)
			visited := make(map[int32]bool)
			var contains func(int32, plan.Node_NodeType) bool
			contains = func(id int32, typ plan.Node_NodeType) bool {
				n := q.Nodes[id]
				if n.NodeType == typ {
					return true
				}
				for _, child := range n.Children {
					if contains(child, typ) {
						return true
					}
				}
				return false
			}
			found := false
			var walk func(int32)
			walk = func(id int32) {
				if visited[id] {
					return
				}
				visited[id] = true
				n := q.Nodes[id]
				require.Nil(t, n.Limit, "reachable plan must not retain a global subquery LIMIT")
				if tc.rankAfterJoin && n.NodeType == plan.Node_WINDOW && contains(n.Children[0], plan.Node_JOIN) {
					found = true
				}
				if !tc.rankAfterJoin && n.NodeType == plan.Node_JOIN && len(n.Children) == 2 && contains(n.Children[1], plan.Node_WINDOW) {
					found = true
				}
				for _, child := range n.Children {
					walk(child)
				}
			}
			for _, id := range q.Steps {
				walk(id)
			}
			require.True(t, found, "rank must be on the correct side of the correlated join")
		})
	}
}

func TestCorrelatedPaginationExtremum(t *testing.T) {
	for _, tc := range []struct {
		name, scalar, aggregate string
		narrowOuter, notNull    bool
	}{
		{"maximum", `select r.r_name from region r where r.r_regionkey=n.n_regionkey order by r.r_name desc limit 1`, "max", false, false},
		{"null safe maximum", `select r.r_name from region r where r.r_regionkey<=>n.n_regionkey order by r.r_name desc limit 1`, "max", false, false},
		{"minimum not null", `select r.r_name from region r where r.r_regionkey=n.n_regionkey order by r.r_name limit 1`, "min", false, true},
		{"minimum nulls first", `select r.r_name from region r where r.r_regionkey=n.n_regionkey order by r.r_name limit 1`, "", false, false},
		{"range maximum", `select r.r_name from region r where r.r_regionkey=n.n_regionkey and r.r_name<=n.n_nationkey order by r.r_name desc limit 1`, "max", true, false},
		{"wide outer range keeps window", `select r.r_name from region r where r.r_regionkey=n.n_regionkey and r.r_name<=n.n_nationkey order by r.r_name desc limit 1`, "", false, false},
		{"offset keeps window", `select r.r_name from region r where r.r_regionkey=n.n_regionkey order by r.r_name desc limit 1 offset 1`, "", false, false},
		{"different value keeps window", `select r.r_regionkey from region r where r.r_regionkey=n.n_regionkey order by r.r_name desc limit 1`, "", false, false},
		{"tie breaker keeps window", `select r.r_name from region r where r.r_regionkey=n.n_regionkey order by r.r_name desc,r.r_regionkey limit 1`, "", false, false},
		{"string value keeps window", `select r.r_comment from region r where r.r_regionkey=n.n_regionkey order by r.r_comment desc limit 1`, "", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opt := NewMockOptimizer(true)
			// Private catalog copies provide nullable integer values and a narrow
			// outer row without adding another service or shared fixture.
			opt.ctxt.tables["region"].Cols[1].Typ = plan.Type{Id: int32(types.T_int32), NotNullable: tc.notNull}
			if tc.narrowOuter {
				for _, col := range opt.ctxt.tables["nation"].Cols {
					if types.T(col.Typ.Id) == types.T_varchar {
						col.Typ = plan.Type{Id: int32(types.T_int32)}
					}
				}
			}
			result, err := runOneStmt(opt, t, "select n.n_nationkey,("+tc.scalar+") from nation n")
			require.NoError(t, err)
			q := result.GetQuery()
			assertReachablePlanHasNoCorrelatedExpr(t, q)
			window, extremum := false, false
			var visit func(int32)
			visit = func(id int32) {
				node := q.Nodes[id]
				window = window || node.NodeType == plan.Node_WINDOW
				for _, agg := range node.AggList {
					extremum = extremum || agg.GetF().GetFunc().GetObjName() == tc.aggregate
				}
				for _, child := range node.Children {
					visit(child)
				}
			}
			for _, root := range q.Steps {
				visit(root)
			}
			require.Equal(t, tc.aggregate == "", window)
			if tc.aggregate != "" {
				require.True(t, extremum)
			}
		})
	}
}

func TestCorrelatedPaginationExtremumPointLookup(t *testing.T) {
	result, err := runOneStmt(NewMockOptimizer(true), t, `select n.n_nationkey,(select r.r_regionkey from region r where r.r_regionkey=n.n_regionkey order by r.r_regionkey desc limit 1) from nation n where n.n_nationkey=0`)
	require.NoError(t, err)
	q := result.GetQuery()
	var visit func(int32)
	visit = func(id int32) {
		node := q.Nodes[id]
		require.NotEqual(t, plan.Node_WINDOW, node.NodeType, "a point lookup must not rank all inner keys")
		for _, child := range node.Children {
			visit(child)
		}
	}
	for _, root := range q.Steps {
		visit(root)
	}
}

func TestCorrelatedPaginationRejectsUnsafeShapes(t *testing.T) {
	cases := []struct{ name, sql string }{
		{"range scalar multiple rows", `select (select r.r_regionkey from tpch.region r where r.r_regionkey<=n.n_nationkey order by r.r_regionkey limit 2) from tpch.nation n`},
		{"arithmetic with equality still guarded", `select (select abs(r.r_regionkey) from tpch.region r where r.r_regionkey=n.n_regionkey order by r.r_regionkey limit 1) from tpch.nation n`},
		{"arithmetic with range still guarded", `select (select abs(r.r_regionkey) from tpch.region r where r.r_regionkey<=n.n_nationkey order by r.r_regionkey limit 1) from tpch.nation n`},
		{"volatile discarded order", `select (select n.n_nationkey from tpch.region r where r.r_regionkey=n.n_regionkey order by random_bytes(16) limit 1) from tpch.nation n`},
		{"erroring inner filter", `select (select r.r_regionkey from tpch.region r where r.r_regionkey=n.n_regionkey and cast(r.r_name as signed)>0 order by r.r_regionkey limit 1) from tpch.nation n`},
		{"erroring group key", `select (select count(*) from tpch.region r where r.r_regionkey=n.n_regionkey group by cast(r.r_name as signed) limit 1) from tpch.nation n`},
		{"erroring join condition", `select (select r.r_regionkey from tpch.region r join tpch.nation i on cast(i.n_name as signed)=r.r_regionkey where r.r_regionkey=n.n_regionkey order by r.r_regionkey limit 1) from tpch.nation n`},
		{"outer projection across aggregate", `select (select n.n_nationkey from tpch.region r where r.r_regionkey=n.n_regionkey having count(*)>=0 limit 1) from tpch.nation n`},
		{"aggregate with disjunction", `select (select count(*) from tpch.region r where r.r_regionkey=n.n_regionkey or r.r_regionkey=n.n_nationkey limit 1) from tpch.nation n`},
		{"narrowing cast", `select (select cast(r.r_regionkey as tinyint) from tpch.region r where r.r_regionkey=n.n_regionkey order by r.r_regionkey limit 1) from tpch.nation n`},
		{"implicit narrowing after widening", `select (select r.r_regionkey from tpch.region r where r.r_regionkey=n.n_regionkey and r.r_regionkey<=cast(n.n_nationkey as bigint) order by r.r_regionkey desc limit 1) from tpch.nation n`},
		{"empty with dynamic offset", `select (select n.n_nationkey limit 0 offset @pagination_offset) from tpch.nation n`},
		{"aggregate can overflow before join", `select n.n_nationkey, (select coalesce(sum(r.r_regionkey), 0) from tpch.region r where r.r_regionkey=n.n_regionkey limit 1) from tpch.nation n`},
		{"prepared empty subquery keeps parameter metadata", `prepare p from 'select (select ? limit 0)'`},
		{"prepared empty quantified operand", `prepare p from 'select ? in (select 1 limit 0)'`},
		{"volatile per outer row", `select (select uuid() from tpch.region r where r.r_regionkey=n.n_regionkey order by r.r_regionkey limit 1) from tpch.nation n`},
		{"repeated outer row identity", `select (select r.r_regionkey from tpch.region r where r.r_regionkey<=n.n_nationkey order by r.r_regionkey limit 1) from (select n_nationkey from tpch.nation union all select n_nationkey from tpch.nation) n`},
		{"string equality and range without outer identity", `select (select r.r_regionkey from tpch.region r where r.r_name=n.n_name and r.r_regionkey<=n.n_nationkey order by r.r_regionkey limit 1) from (select n_nationkey,n_name from tpch.nation union all select n_nationkey,n_name from tpch.nation) n`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := runOneStmt(NewMockOptimizer(true), t, tc.sql)
			require.ErrorContains(t, err, "not yet implemented")
		})
	}
}

// OFFSET must use the first window's unique row number for the second sort;
// sorting twice by the user's tied keys can change which row is offset.
func TestCorrelatedPaginationOffsetUsesFirstRowNumber(t *testing.T) {
	result, err := runOneStmt(NewMockOptimizer(true), t, `select n.n_nationkey, (select r.r_regionkey from tpch.region r where r.r_regionkey=n.n_regionkey and r.r_regionkey<=n.n_nationkey order by r.r_regionkey desc limit 1 offset 1) from tpch.nation n`)
	require.NoError(t, err)
	q := result.GetQuery()
	var lead *plan.Node
	for _, node := range q.Nodes {
		if node.NodeType == plan.Node_WINDOW && len(node.WinSpecList) == 1 && node.WinSpecList[0].GetW().GetName() == "lead" {
			lead = node
			break
		}
	}
	require.NotNil(t, lead)
	require.Len(t, lead.Children, 1)
	require.Len(t, lead.WinSpecList[0].GetW().OrderBy, 1)
	order := lead.WinSpecList[0].GetW().OrderBy[0].Expr.GetCol()
	require.NotNil(t, order)
	partition := q.Nodes[lead.Children[0]]
	require.Equal(t, plan.Node_PARTITION, partition.NodeType)
	require.Len(t, partition.Children, 1)
	require.GreaterOrEqual(t, order.ColPos, int32(0))
	require.Less(t, int(order.ColPos), len(partition.ProjectList))
	forwarded := partition.ProjectList[order.ColPos].GetCol()
	require.NotNil(t, forwarded)
	rank := q.Nodes[partition.Children[0]]
	require.Equal(t, plan.Node_WINDOW, rank.NodeType)
	require.Len(t, rank.WinSpecList, 1)
	require.Equal(t, "row_number", rank.WinSpecList[0].GetW().GetName())
	require.GreaterOrEqual(t, forwarded.ColPos, int32(0))
	require.Less(t, int(forwarded.ColPos), len(rank.ProjectList))
	computed := rank.ProjectList[forwarded.ColPos].GetCol()
	require.NotNil(t, computed)
	require.Equal(t, int32(-1), computed.RelPos)
}

func TestCorrelatedPaginationZeroRetainsTableScan(t *testing.T) {
	result, err := runOneStmt(NewMockOptimizer(true), t, `select n.n_nationkey,(select max(r.r_regionkey) from tpch.region r where r.r_regionkey<=n.n_nationkey limit 0) from tpch.nation n`)
	require.NoError(t, err)
	q := result.GetQuery()
	visited := make(map[int32]bool)
	found := false
	var visit func(int32)
	visit = func(id int32) {
		if visited[id] {
			return
		}
		visited[id] = true
		node := q.Nodes[id]
		if node.NodeType == plan.Node_TABLE_SCAN && node.TableDef.GetName() == "region" {
			found = true
		}
		for _, child := range node.Children {
			visit(child)
		}
	}
	for _, root := range q.Steps {
		visit(root)
	}
	require.True(t, found, "the empty subquery must retain its table scan for authorization and prepare metadata")
}
