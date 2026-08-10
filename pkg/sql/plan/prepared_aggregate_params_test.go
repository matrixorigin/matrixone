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
	"fmt"
	"sort"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func buildPreparedAggregatePlan(t *testing.T, sql string) *planpb.Prepare {
	t.Helper()
	mock := NewMockOptimizer(false)
	logicPlan, err := runOneStmt(mock, t, fmt.Sprintf("prepare stmt1 from '%s'", sql))
	require.NoError(t, err)
	prepare := logicPlan.GetDcl().GetPrepare()
	require.NotNil(t, prepare)
	require.NotNil(t, prepare.GetPlan().GetQuery())
	return prepare
}

func collectParamPositions(expr *planpb.Expr, positions map[int32]struct{}) {
	if expr == nil {
		return
	}
	if param := expr.GetP(); param != nil {
		positions[param.Pos] = struct{}{}
		return
	}
	if function := expr.GetF(); function != nil {
		for _, arg := range function.Args {
			collectParamPositions(arg, positions)
		}
		return
	}
	if window := expr.GetW(); window != nil {
		collectParamPositions(window.WindowFunc, positions)
		for _, item := range window.PartitionBy {
			collectParamPositions(item, positions)
		}
		for _, order := range window.OrderBy {
			if order != nil {
				collectParamPositions(order.Expr, positions)
			}
		}
		if window.Frame != nil {
			if window.Frame.Start != nil {
				collectParamPositions(window.Frame.Start.Val, positions)
			}
			if window.Frame.End != nil {
				collectParamPositions(window.Frame.End.Val, positions)
			}
		}
		return
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			collectParamPositions(item, positions)
		}
	}
}

func collectColumnNames(expr *planpb.Expr, names *[]string) {
	if expr == nil {
		return
	}
	if col := expr.GetCol(); col != nil {
		*names = append(*names, col.Name)
		return
	}
	if function := expr.GetF(); function != nil {
		for _, arg := range function.Args {
			collectColumnNames(arg, names)
		}
		return
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			collectColumnNames(item, names)
		}
	}
}

func preparedParamPositions(prepare *planpb.Prepare) []int32 {
	positions := make(map[int32]struct{})
	for _, node := range prepare.GetPlan().GetQuery().Nodes {
		for _, exprs := range [][]*planpb.Expr{node.ProjectList, node.AggList, node.GroupBy, node.WinSpecList} {
			for _, expr := range exprs {
				collectParamPositions(expr, positions)
			}
		}
	}
	result := make([]int32, 0, len(positions))
	for pos := range positions {
		result = append(result, pos)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func preparedEffectiveParamTypes(t *testing.T, prepare *planpb.Prepare) map[int32]planpb.Type {
	t.Helper()
	result := make(map[int32]planpb.Type)
	var collectExpr func(*planpb.Expr, planpb.Type)
	collectExpr = func(expr *planpb.Expr, inherited planpb.Type) {
		if expr == nil {
			return
		}
		if param := expr.GetP(); param != nil {
			typ := inherited
			if typ.Id == 0 {
				typ = expr.Typ
			}
			if previous, ok := result[param.Pos]; ok {
				require.Equal(t, previous, typ, "parameter %d has inconsistent effective types", param.Pos)
			} else {
				result[param.Pos] = typ
			}
			return
		}
		if function := expr.GetF(); function != nil {
			childType := inherited
			if function.Func != nil && function.Func.ObjName == "cast" {
				childType = expr.Typ
			} else if childType.Id == 0 && types.T(expr.Typ.Id).ToType().IsNumeric() {
				childType = expr.Typ
			}
			for _, arg := range function.Args {
				collectExpr(arg, childType)
			}
			return
		}
		if window := expr.GetW(); window != nil {
			collectExpr(window.WindowFunc, inherited)
			for _, item := range window.PartitionBy {
				collectExpr(item, planpb.Type{})
			}
			for _, order := range window.OrderBy {
				if order != nil {
					collectExpr(order.Expr, planpb.Type{})
				}
			}
		}
	}

	for _, node := range prepare.GetPlan().GetQuery().Nodes {
		for _, exprs := range [][]*planpb.Expr{
			node.ProjectList,
			node.AggList,
			node.GroupBy,
			node.WinSpecList,
		} {
			for _, expr := range exprs {
				collectExpr(expr, planpb.Type{})
			}
		}
	}
	return result
}

func planListContainsParamPos(exprs []*planpb.Expr, pos int32) bool {
	for _, expr := range exprs {
		positions := make(map[int32]struct{})
		collectParamPositions(expr, positions)
		if _, ok := positions[pos]; ok {
			return true
		}
	}
	return false
}

func preparedPlanReusesGroupedColumn(prepare *planpb.Prepare) bool {
	for _, node := range prepare.GetPlan().GetQuery().Nodes {
		if node.NodeType != planpb.Node_AGG || len(node.GroupBy) == 0 || len(node.ProjectList) == 0 {
			continue
		}
		// GroupBinder marks SELECT expressions reused by GROUP BY with the
		// aggregate's synthetic relation (-1), rather than rebinding the scan
		// expression (or wrapping it in any_value).
		if col := node.ProjectList[0].GetCol(); col != nil && col.RelPos == -1 {
			return true
		}
	}
	return false
}

func TestPreparedAggregateParametersAreDiscoveredAndExecutable(t *testing.T) {
	for _, function := range []string{"min", "max", "count", "group_concat"} {
		t.Run(function, func(t *testing.T) {
			prepare := buildPreparedAggregatePlan(t, fmt.Sprintf("select %s(?) from nation", function))
			require.Len(t, prepare.ParamTypes, 1)
			require.Equal(t, []int32{0}, preparedParamPositions(prepare))

			_, err := FillValuesOfParamsInPlan(context.Background(), prepare.Plan, []any{int64(1)})
			require.NoError(t, err)
			_, err = FillValuesOfParamsInPlan(context.Background(), prepare.Plan, nil)
			require.ErrorContains(t, err, "prepare params")
		})
	}
}

func TestPreparedNumericAggregateParameters(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want types.T
	}{
		{name: "direct sum", sql: "select sum(?) from nation", want: types.T_decimal256},
		{name: "direct avg", sql: "select avg(?) from nation", want: types.T_decimal256},
		{name: "window sum", sql: "select sum(?) over () from nation", want: types.T_decimal256},
		{name: "window avg", sql: "select avg(?) over () from nation", want: types.T_decimal256},
		{name: "derived parameter", sql: "select sum(n) from (select ? as n) d", want: types.T_float64},
		{name: "nonrecursive cte parameter", sql: "with c(n) as (select ?) select avg(n) from c", want: types.T_float64},
		{
			name: "recursive cte sum",
			sql:  "with recursive r(n) as (select ? union all select n + 1 from r where n < 2) select sum(n) from r",
			want: types.T_float64,
		},
		{
			name: "recursive cte avg",
			sql:  "with recursive r(n) as (select ? union all select n + 1 from r where n < 2) select avg(n) from r",
			want: types.T_float64,
		},
		{
			name: "recursive cte window",
			sql:  "with recursive r(n) as (select ? union all select n + 1 from r where n < 2) select sum(n) over () from r",
		},
		{
			name: "recursive cte aggregate in window order",
			sql:  "with recursive r(n) as (select ? union all select n + 1 from r where n < 2) select sum(1) over (order by sum(n)) from r",
		},
		{
			name: "recursive cte aggregate in window partition",
			sql:  "with recursive r(n) as (select ? union all select n + 1 from r where n < 2) select avg(1) over (partition by sum(n)) from r",
		},
		{
			name: "recursive cte having",
			sql:  "with recursive r(n) as (select ? union all select n + 1 from r where n < 2) select count(*) from r having sum(n) > 0",
		},
		{
			name: "recursive cte order by",
			sql:  "with recursive r(n) as (select ? union all select n + 1 from r where n < 2) select count(*) from r order by sum(n)",
		},
		{
			name: "recursive cte joined source",
			sql:  "with recursive r(n) as (select ? union all select n + 1 from r where n < 2) select sum(r.n) from r cross join nation",
		},
		{
			name: "recursive cte comma joined source",
			sql:  "with recursive r(n) as (select ? union all select n + 1 from r where n < 2) select sum(r.n) from r, nation",
		},
		{
			name: "recursive cte nested derived source",
			sql:  "with recursive r(n) as (select ? union all select n + 1 from r where n < 2) select sum(d.n) from (select n from r) d",
		},
		{
			name: "recursive cte compatible consumer targets",
			sql:  "with recursive r(n) as (select ? union all select n + 1 from r where n < 2) select sum(r.n), sum(r.n + cast(1 as signed)) from r",
		},
		{
			name: "recursive cte compatible targets across aliases",
			sql:  "with recursive r(n) as (select ? union all select n + 1 from r where n < 2) select sum(a.n), sum(b.n + cast(1 as signed)) from r a cross join r b",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			prepare := buildPreparedAggregatePlan(t, test.sql)
			require.Equal(t, []int32{int32(types.T_any)}, prepare.ParamTypes)
			originalTypes := preparedEffectiveParamTypes(t, prepare)
			want := test.want
			if want == 0 {
				want = types.T_float64
			}
			require.Equal(t, int32(want), originalTypes[0].Id)

			first, err := FillValuesOfParamsInPlan(context.Background(), prepare.Plan, []any{int64(1)})
			require.NoError(t, err)
			second, err := FillValuesOfParamsInPlan(context.Background(), prepare.Plan, []any{"2.5"})
			require.NoError(t, err)
			require.NotSame(t, first, second)
			require.Equal(t, []int32{0}, preparedParamPositions(prepare))
			require.Equal(t, originalTypes, preparedEffectiveParamTypes(t, prepare))
		})
	}
}

func TestPreparedNumericAggregateParameterIdentity(t *testing.T) {
	prepare := buildPreparedAggregatePlan(t, "select sum(?), avg(?) from nation")
	require.Equal(t, []int32{int32(types.T_any), int32(types.T_any)}, prepare.ParamTypes)
	require.Equal(t, []int32{0, 1}, preparedParamPositions(prepare))
	paramTypes := preparedEffectiveParamTypes(t, prepare)
	require.Equal(t, int32(types.T_decimal256), paramTypes[0].Id)
	require.Equal(t, int32(types.T_decimal256), paramTypes[1].Id)

	_, err := FillValuesOfParamsInPlan(context.Background(), prepare.Plan, []any{int64(1), "2.5"})
	require.NoError(t, err)
}

func TestPreparedNumericAggregateDoesNotCoerceStrings(t *testing.T) {
	tests := []string{
		"select sum(n_name) from nation",
		"select avg(n_name) over () from nation",
		"select sum(cast(? as char)) from nation",
		"with recursive r(n) as (select \"x\" union all select n from r where n = \"never\") select sum(n) from r",
		"with recursive r(n) as (select cast(? as char) union all select n from r where n = \"never\") select sum(n) from r",
	}
	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			_, err := runOneStmt(mock, t, fmt.Sprintf("prepare stmt1 from '%s'", sql))
			require.ErrorContains(t, err, "invalid argument aggregate function")
		})
	}
}

func TestPreparedNumericAggregateRespectsExplicitNumericCast(t *testing.T) {
	tests := []string{
		"select sum(cast(? as signed)) from nation",
		"with recursive r(n) as (select cast(? as signed) union all select n + 1 from r where n < 2) select sum(n) from r",
	}
	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			prepare := buildPreparedAggregatePlan(t, sql)
			require.Equal(t, []int32{int32(types.T_any)}, prepare.ParamTypes)
			require.Equal(t, int32(types.T_int64), preparedEffectiveParamTypes(t, prepare)[0].Id)
			_, err := FillValuesOfParamsInPlan(context.Background(), prepare.Plan, []any{int64(1)})
			require.NoError(t, err)
			require.Equal(t, []int32{0}, preparedParamPositions(prepare))
		})
	}
}

func TestPreparedProjectionAndGroupMarkersStayIndependent(t *testing.T) {
	prepare := buildPreparedAggregatePlan(t, "select ? as k, sum(n_nationkey) from nation group by ?")
	require.Len(t, prepare.ParamTypes, 2)
	require.Equal(t, []int32{0, 1}, preparedParamPositions(prepare))

	var projectHasFirst, groupHasSecond bool
	for _, node := range prepare.Plan.GetQuery().Nodes {
		projectHasFirst = projectHasFirst || planListContainsParamPos(node.ProjectList, 0)
		groupHasSecond = groupHasSecond || planListContainsParamPos(node.GroupBy, 1)
	}
	require.True(t, projectHasFirst)
	require.True(t, groupHasSecond)
}

func TestPreparedNestedGroupMarkerStaysIndependent(t *testing.T) {
	prepare := buildPreparedAggregatePlan(t, "select (? + 0) as k, sum(n_nationkey) from nation group by (? + 0)")
	require.Len(t, prepare.ParamTypes, 2)
	require.Equal(t, []int32{0, 1}, preparedParamPositions(prepare))
}

func TestPreparedParameterizedGroupAliasAndOrdinalReuseGroupedColumn(t *testing.T) {
	for _, groupBy := range []string{"x", "1"} {
		t.Run(groupBy, func(t *testing.T) {
			prepare := buildPreparedAggregatePlan(t,
				fmt.Sprintf("select n_nationkey + ? as x, count(*) from nation group by %s", groupBy))
			require.Len(t, prepare.ParamTypes, 1)
			require.Equal(t, []int32{0}, preparedParamPositions(prepare))
			require.True(t, preparedPlanReusesGroupedColumn(prepare))
		})
	}
}

func TestPreparedEqualLookingAggregatesStayIndependent(t *testing.T) {
	prepare := buildPreparedAggregatePlan(t, "select min(?), min(?) from nation")
	require.Len(t, prepare.ParamTypes, 2)
	require.Equal(t, []int32{0, 1}, preparedParamPositions(prepare))
}

func TestPreparedNestedAggregateMarkersStayIndependent(t *testing.T) {
	prepare := buildPreparedAggregatePlan(t, "select min(abs(?)), min(abs(?)) from nation")
	require.Len(t, prepare.ParamTypes, 2)
	require.Equal(t, []int32{0, 1}, preparedParamPositions(prepare))
}

func TestPreparedParameterIdentityDoesNotLeakIntoPlanNames(t *testing.T) {
	prepare := buildPreparedAggregatePlan(t, "select min(?), min(?) from nation")

	var names []string
	for _, node := range prepare.Plan.GetQuery().Nodes {
		for _, exprs := range [][]*planpb.Expr{
			node.ProjectList,
			node.AggList,
			node.GroupBy,
			node.FilterList,
		} {
			for _, expr := range exprs {
				collectColumnNames(expr, &names)
			}
		}
	}
	for _, name := range names {
		require.NotContains(t, name, "?0")
		require.NotContains(t, name, "?1")
	}
}

func TestWindowExpressionKeysRetainParameterOffsets(t *testing.T) {
	windowSpec := func() *tree.WindowSpec {
		return &tree.WindowSpec{
			HasFrame: true,
			Frame: &tree.FrameClause{
				Type:  tree.Rows,
				Start: &tree.FrameBound{Type: tree.CurrentRow},
				End:   &tree.FrameBound{Type: tree.CurrentRow},
			},
		}
	}
	first := testWindowFuncExpr("min", tree.FUNC_TYPE_DEFAULT, windowSpec(), testScalarFuncExpr("abs", tree.NewParamExpr(0)))
	second := testWindowFuncExpr("min", tree.FUNC_TYPE_DEFAULT, windowSpec(), testScalarFuncExpr("abs", tree.NewParamExpr(1)))

	require.NotEqual(t, windowExprAstKey(first), windowExprAstKey(second))
}
