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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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

func TestPreparedBinaryStateMarkersUseVarbinaryDomain(t *testing.T) {
	for _, sql := range []string{
		"select hll_cardinality(?)",
		"select hll_merge_agg(?) from nation",
		"select bitmap_or_agg(?) from nation",
	} {
		t.Run(sql, func(t *testing.T) {
			prepare := buildPreparedAggregatePlan(t, sql)
			require.Equal(t, []int32{0}, preparedParamPositions(prepare))
			name := "hll_cardinality"
			if sql == "select hll_merge_agg(?) from nation" {
				name = "hll_merge_agg"
			} else if sql == "select bitmap_or_agg(?) from nation" {
				name = "bitmap_or_agg"
			}
			fn := findPlanFunctionExpr(prepare.Plan, name)
			require.NotNil(t, fn)
			require.Len(t, fn.GetF().Args, 1)
			arg := fn.GetF().Args[0]
			require.Equal(t, int32(types.T_varbinary), arg.Typ.Id)
			require.Zero(t, arg.Typ.Width, "opaque state cast must not impose the SQL VARBINARY width")
			require.Equal(t, "cast", arg.GetF().GetFunc().GetObjName())
			require.Equal(t, int32(types.T_text), arg.GetF().Args[0].Typ.Id)
		})
	}
}

func TestPreparedPercentileParameters(t *testing.T) {
	for _, sql := range []string{
		"select approx_percentile(n_nationkey, ?) from nation",
		"select approx_percentile(?) within group (order by n_nationkey) from nation",
		"select percentile_cont(?) within group (order by n_nationkey) from nation",
		"select percentile_disc(?) within group (order by n_name) from nation",
	} {
		t.Run(sql, func(t *testing.T) {
			prepare := buildPreparedAggregatePlan(t, sql)
			require.Equal(t, []int32{0}, preparedParamPositions(prepare))
			require.True(t, PreparedPlanHasPercentileParams(prepare.Plan))
		})
	}

	literal := buildPreparedAggregatePlan(t,
		"select percentile_disc(0.5) within group (order by n_name) from nation")
	require.False(t, PreparedPlanHasPercentileParams(literal.Plan))

	_, err := runOneStmt(NewMockOptimizer(false), t,
		"select percentile_disc(n_regionkey) within group (order by n_name) from nation")
	require.ErrorContains(t, err, "non-null constant or parameter")
}

func TestPreparedPercentileParameterExpressions(t *testing.T) {
	for _, sql := range []string{
		"select approx_percentile(n_nationkey, ? / 100.0) from nation",
		"select approx_percentile((? + 5) / 100.0) within group (order by n_nationkey desc) from nation",
		"select percentile_cont(cast(? as double) / 100.0) within group (order by n_nationkey) from nation",
		"select percentile_disc((? + ?) / 100.0) within group (order by n_name) from nation",
		"select percentile_disc(-(-? / 100.0)) within group (order by n_name) over () from nation",
	} {
		t.Run(sql, func(t *testing.T) {
			prepare := buildPreparedAggregatePlan(t, sql)
			require.NotEmpty(t, preparedParamPositions(prepare))
			require.True(t, PreparedPlanHasPercentileParams(prepare.Plan))
		})
	}
}

func TestPreparedPercentilePreservesSupportedDecimalConfigType(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{
			name: NameApproxPercentile,
			sql:  "select approx_percentile(n_nationkey, cast(? as decimal(19,18))) from nation",
		},
		{
			name: NameApproxPercentile,
			sql:  "select approx_percentile(null, cast(? as decimal(19,18)))",
		},
		{
			name: NamePercentileCont,
			sql:  "select percentile_cont(cast(? as decimal(19,18))) within group (order by n_nationkey) from nation",
		},
		{
			name: NamePercentileDisc,
			sql:  "select percentile_disc(cast(? as decimal(19,18))) within group (order by n_nationkey) from nation",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prepare := buildPreparedAggregatePlan(t, tc.sql)
			fn := findAggregateByName(prepare.Plan.GetQuery(), tc.name)
			require.NotNil(t, fn)
			require.Len(t, fn.Args, 2)
			percentile := fn.Args[1]
			require.Equal(t, int32(types.T_decimal128), percentile.Typ.Id)
			require.Equal(t, int32(19), percentile.Typ.Width)
			require.Equal(t, int32(18), percentile.Typ.Scale)
			require.Equal(t, "cast", percentile.GetF().GetFunc().GetObjName())

			filled, err := FillValuesOfParamsInPlan(
				context.Background(), prepare.Plan, []any{"0.500000000000000001"})
			require.NoError(t, err)
			filledFn := findPlanFunctionExpr(filled, tc.name)
			require.NotNil(t, filledFn)
			filledPercentile := filledFn.GetF().Args[1]
			require.Equal(t, int32(types.T_decimal128), filledPercentile.Typ.Id)
			require.Equal(t, int32(19), filledPercentile.Typ.Width)
			require.Equal(t, int32(18), filledPercentile.Typ.Scale)
			require.Equal(t, "cast", filledPercentile.GetF().GetFunc().GetObjName())
			require.Equal(t, int32(types.T_text), filledPercentile.GetF().Args[0].Typ.Id)

			originalFn := findAggregateByName(prepare.Plan.GetQuery(), tc.name)
			require.Equal(t, percentile, originalFn.Args[1],
				"filling one execution must not mutate the cached plan")
		})
	}
}

func TestPreparedPercentileParameterExpressionsRejectRowDependentOrArbitraryFunctions(t *testing.T) {
	for _, sql := range []string{
		"prepare stmt_col from 'select percentile_cont((? + n_regionkey) / 100.0) within group (order by n_nationkey) from nation'",
		"prepare stmt_subquery from 'select percentile_cont((? + (select 1)) / 100.0) within group (order by n_nationkey) from nation'",
		"prepare stmt_function from 'select percentile_cont(abs(?) / 100.0) within group (order by n_nationkey) from nation'",
		"prepare stmt_volatile from 'select percentile_cont((? + rand()) / 100.0) within group (order by n_nationkey) from nation'",
		"prepare stmt_variable from 'select percentile_cont((? + @percentile_offset) / 100.0) within group (order by n_nationkey) from nation'",
		"prepare stmt_string_cast from 'select percentile_cont(cast(? as char) / 100.0) within group (order by n_nationkey) from nation'",
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := runOneStmt(NewMockOptimizer(false), t, sql)
			require.ErrorContains(t, err, "non-null constant or parameter")
		})
	}
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
	}{
		{name: "direct sum", sql: "select sum(?) from nation"},
		{name: "direct avg", sql: "select avg(?) from nation"},
		{name: "window sum", sql: "select sum(?) over () from nation"},
		{name: "window avg", sql: "select avg(?) over () from nation"},
		{name: "derived parameter", sql: "select sum(n) from (select ? as n) d"},
		{name: "nonrecursive cte parameter", sql: "with c(n) as (select ?) select avg(n) from c"},
		{
			name: "recursive cte sum",
			sql:  "with recursive r(n) as (select ? union all select n + 1 from r where n < 2) select sum(n) from r",
		},
		{
			name: "recursive cte avg",
			sql:  "with recursive r(n) as (select ? union all select n + 1 from r where n < 2) select avg(n) from r",
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
			require.Equal(t, int32(types.T_float64), originalTypes[0].Id)

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
	require.Equal(t, int32(types.T_float64), paramTypes[0].Id)
	require.Equal(t, int32(types.T_float64), paramTypes[1].Id)

	_, err := FillValuesOfParamsInPlan(context.Background(), prepare.Plan, []any{int64(1), "2.5"})
	require.NoError(t, err)
}

func TestPreparedJSONAggregateValueNeedsRuntimeSpecialization(t *testing.T) {
	for _, test := range []struct {
		sql  string
		name string
	}{
		{"select json_arrayagg(?) from nation", "json_arrayagg"},
		{"select json_objectagg(''k'', ?) from nation", "json_objectagg"},
	} {
		t.Run(test.name, func(t *testing.T) {
			prepare := buildPreparedAggregatePlan(t, test.sql)
			require.True(t, PreparedPlanNeedsRuntimeSpecialization(prepare.Plan))
			for _, value := range []struct {
				text   string
				typ    types.Type
				isNull bool
			}{
				{text: "123.4500", typ: types.New(types.T_decimal128, 20, 4)},
				{text: `{"a":1}`, typ: types.T_json.ToType()},
				{text: "plain", typ: types.T_text.ToType()},
				{typ: types.New(types.T_decimal128, 20, 4), isNull: true},
			} {
				for _, binary := range []bool{false, true} {
					param := ParamValue{IsBinaryProtocol: binary, RetainParamRef: true}
					if !value.isNull {
						param.Value = value.text
					}
					if binary {
						param.RuntimeType, param.HasRuntimeType = value.typ, true
					} else {
						param.SourceType, param.HasSourceType = value.typ, true
					}
					filled, specialized, err := FillValuesOfParamsInPlanWithSpecialization(
						context.Background(), prepare.Plan, []any{param})
					require.NoError(t, err)
					require.True(t, specialized)
					aggregate := findPlanFunctionExpr(filled, test.name)
					require.NotNil(t, aggregate)
					if !value.isNull {
						require.Equal(t, int32(value.typ.Oid), aggregate.GetF().Args[len(aggregate.GetF().Args)-1].Typ.Id)
					}
					require.NoError(t, RestorePreparedRuntimeParamRefs(context.Background(), filled))
					require.True(t, preparedExprContainsParam(aggregate.GetF().Args[len(aggregate.GetF().Args)-1]),
						"runtime cache must not retain the first EXECUTE value")
				}
			}
		})
	}
}

func TestPreparedJSONAggregateValueWithoutRuntimeMetadata(t *testing.T) {
	for _, tc := range []struct {
		sql string
		fn  string
	}{
		{sql: "select json_arrayagg(?) from nation", fn: "json_arrayagg"},
		{sql: "select json_objectagg(''k'', ?) from nation", fn: "json_objectagg"},
	} {
		t.Run(tc.fn, func(t *testing.T) {
			prepared := buildPreparedAggregatePlan(t, tc.sql)
			preparedAggregate := findPlanFunctionExpr(prepared.Plan, tc.fn)
			require.NotNil(t, preparedAggregate)
			preparedArg := preparedAggregate.GetF().Args
			preparedType := preparedArg[len(preparedArg)-1].Typ.Id

			// A caller without source-type metadata must keep the prepared
			// marker domain, not infer a JSON atom from the text value.
			filled, specialized, err := FillValuesOfParamsInPlanWithSpecialization(
				context.Background(), prepared.Plan,
				[]any{ParamValue{Value: "plain", RetainParamRef: true}})
			require.NoError(t, err)
			require.False(t, specialized, "unknown source type must not invalidate the cached compile")
			aggregate := findPlanFunctionExpr(filled, tc.fn)
			require.NotNil(t, aggregate)
			args := aggregate.GetF().Args
			require.Equal(t, preparedType, args[len(args)-1].Typ.Id)
			require.NoError(t, RestorePreparedRuntimeParamRefs(context.Background(), filled))
			require.True(t, preparedExprContainsParam(args[len(args)-1]))

			// The fallback must not poison the cached template for a later
			// execution that does supply a concrete source type.
			filled, specialized, err = FillValuesOfParamsInPlanWithSpecialization(
				context.Background(), prepared.Plan,
				[]any{ParamValue{
					Value: "123.4500", SourceType: types.New(types.T_decimal128, 20, 4),
					HasSourceType: true, RetainParamRef: true,
				}})
			require.NoError(t, err)
			require.True(t, specialized)
			aggregate = findPlanFunctionExpr(filled, tc.fn)
			require.NotNil(t, aggregate)
			args = aggregate.GetF().Args
			require.Equal(t, int32(types.T_decimal128), args[len(args)-1].Typ.Id)
		})
	}
}

func TestPreparedJSONAggregateKeepsExplicitAndKeyDomains(t *testing.T) {
	for _, sql := range []string{
		"select json_arrayagg(cast(? as decimal(20,4))) from nation",
		"select json_arrayagg(cast(? as json)) from nation",
		"select json_objectagg(''k'', cast(? as decimal(20,4))) from nation",
		"select json_objectagg(''k'', cast(? as json)) from nation",
		"select json_objectagg(?, 1) from nation",
	} {
		t.Run(sql, func(t *testing.T) {
			prepare := buildPreparedAggregatePlan(t, sql)
			require.False(t, PreparedPlanNeedsRuntimeSpecialization(prepare.Plan))
		})
	}
}

func TestSQLPreparedNullRetainsBinarySourceTypeAndRuntimeDomain(t *testing.T) {
	prepare := buildPreparedAggregatePlan(t, "select char_length(?) from nation")
	filled, specialized, err := FillValuesOfParamsInPlanWithSpecialization(
		context.Background(), prepare.Plan, []any{ParamValue{
			Value:               nil,
			SourceType:          types.T_varbinary.ToType(),
			HasSourceType:       true,
			RuntimeStringDomain: types.RuntimeStringText,
		}})
	require.NoError(t, err)
	require.True(t, specialized)

	found := false
	require.NoError(t, planpb.VisitExpressionsInOwner(filled, func(root *planpb.Expr) error {
		return planpb.VisitExprTree(root, func(expr *planpb.Expr) error {
			literal := expr.GetLit()
			if literal != nil && literal.Isnull && expr.Typ.Id == int32(types.T_varbinary) {
				found = true
				require.Equal(t,
					planpb.StringLiteralForm_STRING_LITERAL_TEXT, literal.LiteralForm)
			}
			return nil
		})
	}))
	require.True(t, found, "typed NULL must remain VARBINARY with its explicit text override")
}

func TestPreparedAggregateRuntimeTypeReachesResultProjection(t *testing.T) {
	prepare := buildPreparedAggregatePlan(t, "select sum(?) from nation")

	filled, specialized, err := FillValuesOfParamsInPlanWithSpecialization(
		context.Background(),
		prepare.Plan,
		[]any{ParamValue{
			Value:            "7",
			RuntimeType:      types.T_int64.ToType(),
			HasRuntimeType:   true,
			IsBinaryProtocol: true,
		}},
	)
	require.NoError(t, err)
	require.True(t, specialized)

	columns := GetResultColumnsFromPlan(filled)
	require.Len(t, columns, 1)
	require.Equal(t, int32(types.T_decimal128), columns[0].Typ.Id)
	for _, node := range filled.GetQuery().Nodes {
		for _, expr := range node.ProjectList {
			col := expr.GetCol()
			if col != nil && col.RelPos == -2 {
				require.Equal(t, int32(types.T_decimal128), expr.Typ.Id)
			}
		}
	}
}

func TestPreparedWindowAggregateRuntimeTypeReachesResultProjection(t *testing.T) {
	prepare := buildPreparedAggregatePlan(t, "select sum(?) over () from nation")

	filled, specialized, err := FillValuesOfParamsInPlanWithSpecialization(
		context.Background(),
		prepare.Plan,
		[]any{ParamValue{
			Value:            "7",
			RuntimeType:      types.T_int64.ToType(),
			HasRuntimeType:   true,
			IsBinaryProtocol: true,
		}},
	)
	require.NoError(t, err)
	require.True(t, specialized)

	columns := GetResultColumnsFromPlan(filled)
	require.Len(t, columns, 1)
	require.Equal(t, int32(types.T_decimal128), columns[0].Typ.Id)
	windowSeen := false
	for _, node := range filled.GetQuery().Nodes {
		if node.NodeType != planpb.Node_WINDOW {
			continue
		}
		windowSeen = true
		require.Len(t, node.WinSpecList, 1)
		require.Equal(t, int32(types.T_decimal128), node.WinSpecList[0].Typ.Id)
		for _, expr := range node.ProjectList {
			if col := expr.GetCol(); col != nil && col.RelPos == -1 {
				require.Equal(t, int32(types.T_decimal128), expr.Typ.Id)
			}
		}
	}
	require.True(t, windowSeen)
}

func TestPreparedMaxByRuntimeTypeReachesResultProjection(t *testing.T) {
	for _, name := range []string{"max_by", "max_by_non_null"} {
		t.Run(name, func(t *testing.T) {
			prepare := buildPreparedAggregatePlan(t, fmt.Sprintf("select %s(?, 1, 1) from nation", name))
			require.True(t, PreparedPlanNeedsRuntimeSpecialization(prepare.Plan))
			filled, specialized, err := FillValuesOfParamsInPlanWithSpecialization(
				context.Background(),
				prepare.Plan,
				[]any{ParamValue{
					Value:            "7",
					RuntimeType:      types.T_int64.ToType(),
					HasRuntimeType:   true,
					IsBinaryProtocol: true,
				}},
			)
			require.NoError(t, err)
			require.True(t, specialized)

			columns := GetResultColumnsFromPlan(filled)
			require.Len(t, columns, 1)
			require.Equal(t, int32(types.T_int64), columns[0].Typ.Id)
		})
	}
}

func TestPreparedBitwiseAggregateScansForRuntimeSpecialization(t *testing.T) {
	prepare := buildPreparedAggregatePlan(t, "select bit_and(?) from nation")
	aggregate := findPlanFunctionExpr(prepare.Plan, "bit_and")
	require.NotNil(t, aggregate)
	require.Len(t, aggregate.GetF().Args, 1)
	privateCast := aggregate.GetF().Args[0]
	require.True(t, isBitwiseAggregatePrivateCast(privateCast))
	require.False(t, isExplicitPreparedCast(privateCast))
	require.True(t, PreparedPlanNeedsRuntimeSpecialization(prepare.Plan))
}

func TestPreparedBitwiseAggregateRebindsChangedValueWithinSameDomain(t *testing.T) {
	prepare := buildPreparedAggregatePlan(t, "select bit_and(?) from nation")
	require.True(t, PreparedPlanNeedsRuntimeSpecialization(prepare.Plan))
	decimalType := types.New(types.T_decimal64, 2, 1)

	for _, test := range []struct {
		value string
		want  int64
	}{{value: "2.5", want: 25}, {value: "4.0", want: 40}} {
		filled, specialized, err := FillValuesOfParamsInPlanWithSpecialization(
			context.Background(),
			prepare.Plan,
			[]any{ParamValue{
				Value: test.value, SourceType: decimalType, HasSourceType: true,
				RetainParamRef: true,
			}},
		)
		require.NoError(t, err)
		require.True(t, specialized)

		aggregate := findPlanFunctionExpr(filled, "bit_and")
		require.NotNil(t, aggregate)
		require.Len(t, aggregate.GetF().Args, 1)
		aggregateCast := aggregate.GetF().Args[0]
		require.True(t, isBitwiseAggregatePrivateCast(aggregateCast))
		source := aggregateCast.GetF().Args[0]
		require.Equal(t, int32(types.T_decimal64), source.Typ.Id)
		require.Equal(t, test.want, source.GetLit().GetDecimal64Val().A)
		sourceRef := source.GetLit().GetSrc()
		require.NotNil(t, sourceRef)
		require.NotNil(t, sourceRef.GetP())
		require.Equal(t, int32(0), sourceRef.GetP().GetPos())
	}
}

func TestPreparedBitwiseAggregateProjectionRefreshPreservesPrivateCast(t *testing.T) {
	prepare := buildPreparedAggregatePlan(t,
		"select bit_and(v) from (select max(?) as v from nation) d")
	preparedAggregate := findPlanFunctionExpr(prepare.Plan, "bit_and")
	require.NotNil(t, preparedAggregate)
	require.Len(t, preparedAggregate.GetF().Args, 1)
	preparedCast := preparedAggregate.GetF().Args[0]
	require.True(t, isBitwiseAggregatePrivateCast(preparedCast))
	require.NotNil(t, preparedCast.GetF().Args[0].GetCol(), preparedCast.String())

	for _, test := range []struct {
		name        string
		value       any
		sourceType  types.Type
		isBinary    bool
		wantPrivate bool
		wantSource  types.T
	}{
		{
			name:        "DECIMAL64 fractional input keeps numeric conversion",
			value:       "2.5",
			sourceType:  types.New(types.T_decimal64, 2, 1),
			wantPrivate: true,
			wantSource:  types.T_decimal64,
		},
		{
			name:        "DECIMAL128 keeps unsigned numeric conversion",
			value:       "9223372036854775808",
			sourceType:  types.New(types.T_decimal128, 20, 0),
			wantPrivate: true,
			wantSource:  types.T_decimal128,
		},
		{
			name:       "VARBINARY returns to native byte semantics",
			value:      []byte{0x02},
			sourceType: types.New(types.T_varbinary, 1, 0),
			isBinary:   true,
			wantSource: types.T_varbinary,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			filled, specialized, err := FillValuesOfParamsInPlanWithSpecialization(
				context.Background(), prepare.Plan, []any{ParamValue{
					Value: test.value, SourceType: test.sourceType, HasSourceType: true,
					IsBin: test.isBinary, RetainParamRef: true,
				}},
			)
			require.NoError(t, err)
			require.True(t, specialized)

			filledAggregate := findPlanFunctionExpr(filled, "bit_and")
			require.NotNil(t, filledAggregate)
			require.Len(t, filledAggregate.GetF().Args, 1)
			aggregateArg := filledAggregate.GetF().Args[0]
			require.Equal(t, test.wantPrivate,
				isBitwiseAggregatePrivateCast(aggregateArg), aggregateArg.String())
			if test.wantPrivate {
				require.Equal(t, int32(types.T_int64), aggregateArg.Typ.Id)
				aggregateArg = aggregateArg.GetF().Args[0]
			}
			require.Equal(t, int32(test.wantSource), aggregateArg.Typ.Id)
		})
	}
}

func TestPreparedRuntimeSpecializationCoversResultDomainAggregates(t *testing.T) {
	for _, name := range []string{
		"min", "max", "any_value", "max_by", "max_by_non_null",
	} {
		t.Run(name, func(t *testing.T) {
			require.True(t, preparedRuntimeSpecializationFunction(name))
		})
	}
}

func TestPreparedRuntimeSpecializationCoversBinaryStringSemantics(t *testing.T) {
	for _, name := range []string{
		"ord", "char_length", "character_length",
		"left", "right", "substring", "substr", "mid", "reverse",
		"lower", "lcase", "upper", "ucase", "trim", "ltrim", "rtrim",
		"locate", "instr", "position", "insert", "replace", "lpad", "rpad",
		"substring_index", "split_part", "repeat", "concat", "concat_ws",
		"charset", "collation",
	} {
		require.True(t, preparedRuntimeSpecializationFunction(name), name)
	}

	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare binary_domains from 'select charset(left(?, 1)), char_length(?), ord(?)'")
	require.NoError(t, err)
	preparedPlan := prepared.GetDcl().GetPrepare().Plan
	require.True(t, PreparedPlanNeedsRuntimeSpecialization(preparedPlan))

	binaryParam := ParamValue{
		Value: "\xe4\xbd\xa0", SourceType: types.T_varbinary.ToType(), HasSourceType: true,
	}
	filled, specialized, err := FillValuesOfParamsInPlanWithSpecialization(
		context.Background(), preparedPlan, []any{binaryParam, binaryParam, binaryParam})
	require.NoError(t, err)
	require.True(t, specialized)

	left := findPlanFunctionExpr(filled, "left")
	require.NotNil(t, left)
	require.Equal(t, int32(types.T_varbinary), left.Typ.Id)
	require.Equal(t, uint32(types.CharsetBinary), left.Typ.Charset)
	charLength := findPlanFunctionExpr(filled, "char_length")
	require.NotNil(t, charLength)
	require.Equal(t, int32(types.T_varbinary), charLength.GetF().Args[0].Typ.Id)
	ord := findPlanFunctionExpr(filled, "ord")
	require.NotNil(t, ord)
	require.Equal(t, int32(types.T_varbinary), ord.GetF().Args[0].Typ.Id)
}

func TestPreparedDMLRuntimeSpecializationPreservesWriteParameters(t *testing.T) {
	predicateOnly := buildPreparedAggregatePlan(t,
		"update nation set n_comment = ''x'' where ? = ?")
	require.True(t, PreparedPlanNeedsRuntimeSpecialization(predicateOnly.Plan))

	withWriteParameter := buildPreparedAggregatePlan(t,
		"update nation set n_comment = ? where ? = ?")
	// The predicate still needs execute-time comparison specialization. The
	// write projection is materialized with its original assignment cast so a
	// fresh DML compile cannot change the positional write layout.
	require.True(t, PreparedPlanNeedsRuntimeSpecialization(withWriteParameter.Plan))

	writeExpressionPredicate := buildPreparedAggregatePlan(t,
		"update nation set n_comment = (? = ?) where n_nationkey = 1")
	// A domain-sensitive expression may be nested below the assignment cast;
	// scanning must descend into the write root while preserving its outer
	// positional contract.
	require.True(t, PreparedPlanNeedsRuntimeSpecialization(writeExpressionPredicate.Plan))

	nestedWriteExpression := buildPreparedAggregatePlan(t,
		"update nation set n_comment = (select d.v from (select ? = ? as v) d) where n_nationkey = 1")
	// A derived-table projection is not a positional DML write root. Its
	// marker comparison must therefore remain visible to the specialization
	// scan instead of being preserved as if it were an assignment cast.
	require.True(t, PreparedPlanNeedsRuntimeSpecialization(nestedWriteExpression.Plan))

	columnBoundPredicate := buildPreparedAggregatePlan(t,
		"update nation set n_comment = ? where n_nationkey = ? and n_regionkey = ?")
	// The generic overload scan can still reuse the cached indexed plan; the
	// separate text-comparison scan must select the engine DOUBLE conversion.
	require.False(t, PreparedPlanNeedsRuntimeSpecialization(columnBoundPredicate.Plan))
	require.True(t, PreparedPlanNeedsRuntimeTextComparisonSpecialization(
		columnBoundPredicate.Plan,
		[]types.Type{types.T_text.ToType(), types.T_text.ToType(), types.T_text.ToType()},
	))
	for _, predicate := range []string{
		"n_nationkey = abs(?)",
		"n_nationkey = (? + 0)",
		"n_nationkey in (abs(?), 2)",
		"? between n_nationkey and n_regionkey",
	} {
		columnExpressionPredicate := buildPreparedAggregatePlan(t,
			"update nation set n_comment = n_comment where "+predicate)
		require.True(t, PreparedPlanNeedsRuntimeTextComparisonSpecialization(
			columnExpressionPredicate.Plan, []types.Type{types.T_text.ToType()}), predicate)
	}

	filled, specialized, err := FillValuesOfParamsInPlanWithSpecializationPreservingDMLWrites(
		context.Background(),
		withWriteParameter.Plan,
		[]any{
			ParamValue{Value: "updated", RuntimeType: types.T_varchar.ToType(), HasRuntimeType: true, IsBinaryProtocol: true},
			ParamValue{Value: "1", RuntimeType: types.T_int64.ToType(), HasRuntimeType: true, IsBinaryProtocol: true},
			ParamValue{Value: "1.00", RuntimeType: types.T_text.ToType(), HasRuntimeType: true, IsBinaryProtocol: true},
		},
	)
	require.NoError(t, err)
	require.True(t, specialized)

	writeCastSeen := false
	predicateParamSeen := false
	for _, node := range filled.GetQuery().Nodes {
		for _, expr := range node.ProjectList {
			function := expr.GetF()
			if function == nil || function.Func == nil || function.Func.GetObjName() != "cast_assign" || len(function.Args) == 0 {
				continue
			}
			if literal := function.Args[0].GetLit(); literal != nil && literal.GetSval() == "updated" {
				writeCastSeen = true
			}
		}
		for _, expr := range node.FilterList {
			if expr.GetF() == nil || expr.GetF().Func == nil || expr.GetF().Func.GetObjName() != "=" {
				continue
			}
			for _, arg := range expr.GetF().Args {
				if arg.GetP() != nil {
					predicateParamSeen = true
				}
			}
		}
	}
	require.True(t, writeCastSeen)
	require.False(t, predicateParamSeen)
}

func TestPreparedNtileParameter(t *testing.T) {
	prepare := buildPreparedAggregatePlan(t,
		"select n_nationkey, ntile(?) over (partition by n_regionkey order by n_nationkey) from nation")
	require.Equal(t, []int32{int32(types.T_any)}, prepare.ParamTypes)
	require.Equal(t, []int32{0}, preparedParamPositions(prepare))

	originalTypes := preparedEffectiveParamTypes(t, prepare)
	require.Equal(t, int32(types.T_int64), originalTypes[0].Id)

	first, err := FillValuesOfParamsInPlan(context.Background(), prepare.Plan, []any{int64(2)})
	require.NoError(t, err)
	second, err := FillValuesOfParamsInPlan(context.Background(), prepare.Plan, []any{int64(5)})
	require.NoError(t, err)
	require.NotSame(t, first, second)
	require.Equal(t, []int32{0}, preparedParamPositions(prepare))
	require.Equal(t, originalTypes, preparedEffectiveParamTypes(t, prepare))
}

func TestPreparedLagLeadOffsetParameter(t *testing.T) {
	for _, name := range []string{"lag", "lead"} {
		t.Run(name, func(t *testing.T) {
			prepare := buildPreparedAggregatePlan(t, fmt.Sprintf(
				"select n_nationkey, %s(n_nationkey, ?) over (partition by n_regionkey order by n_nationkey) from nation",
				name,
			))
			require.Equal(t, []int32{int32(types.T_any)}, prepare.ParamTypes)
			require.Equal(t, []int32{0}, preparedParamPositions(prepare))
			require.Equal(t, []int32{0}, PreparedLagLeadParamPositions(prepare.Plan))

			originalTypes := preparedEffectiveParamTypes(t, prepare)
			require.Equal(t, int32(types.T_int64), originalTypes[0].Id)

			for _, valid := range []any{
				int64(0),
				int64(1),
				false,
				true,
				ParamValue{Value: "0", PrepareParamKind: vector.PrepareParamBoolean},
				ParamValue{Value: "1", PrepareParamKind: vector.PrepareParamBoolean},
				ParamValue{Value: "9223372036854775807", PrepareParamKind: vector.PrepareParamInteger},
			} {
				filled, err := FillValuesOfParamsInPlan(context.Background(), prepare.Plan, []any{valid})
				require.NoError(t, err)
				require.NotSame(t, prepare.Plan, filled)
				require.Equal(t, originalTypes, preparedEffectiveParamTypes(t, prepare))
			}

			for _, invalid := range []any{
				int64(-1),
				nil,
				float64(-1.5),
				ParamValue{Value: "-1", PrepareParamKind: vector.PrepareParamInteger},
				ParamValue{Value: "-1.5", PrepareParamKind: vector.PrepareParamFloat},
				ParamValue{Value: "-1.5", PrepareParamKind: vector.PrepareParamDecimal},
				ParamValue{Value: "2", PrepareParamKind: vector.PrepareParamBoolean},
			} {
				_, err := FillValuesOfParamsInPlan(context.Background(), prepare.Plan, []any{invalid})
				require.Error(t, err)
				require.Equal(t, moerr.ER_WRONG_ARGUMENTS, err.(*moerr.Error).MySQLCode())
			}
		})
	}
}

func TestNtileRequiresIntegerArgument(t *testing.T) {
	for _, sql := range []string{
		"select ntile(n_name) over (order by n_nationkey) from nation",
		"select ntile(2.5) over (order by n_nationkey) from nation",
		"select ntile(cast(? as char)) over (order by n_nationkey) from nation",
	} {
		t.Run(sql, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			_, err := runOneStmt(mock, t, fmt.Sprintf("prepare stmt1 from '%s'", sql))
			require.ErrorContains(t, err, "invalid argument function ntile")
		})
	}
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
