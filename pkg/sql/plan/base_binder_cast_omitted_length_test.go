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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

// bindFirstSelectExpr parses sql as a plain select and binds its first
// projection expression with a default binder.
func bindFirstSelectExpr(t *testing.T, sql string) *Expr {
	t.Helper()
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	sel := stmt.(*tree.Select)
	clause, ok := sel.Select.(*tree.SelectClause)
	require.True(t, ok, "expected a plain select clause in %q", sql)
	binder := NewDefaultBinder(t.Context(), nil, nil, Type{}, nil)
	bound, err := binder.BindExpr(clause.Exprs[0].Expr, 0, false)
	require.NoError(t, err)
	return bound
}

// castBoundTarget ensures the bound expression is a cast whose target
// argument carries the expected target oid/width pair, returning the cast
// source expression.
func castBoundTarget(t *testing.T, bound *Expr, wantOid types.T, wantWidth int32) *Expr {
	t.Helper()
	fn := bound.GetF()
	require.NotNil(t, fn)
	require.Equal(t, "cast", fn.Func.GetObjName())
	require.Len(t, fn.Args, 2)
	target := fn.Args[1]
	require.Equal(t, int32(wantOid), target.Typ.Id)
	require.Equal(t, wantWidth, target.Typ.Width)
	return fn.Args[0]
}

// TestCastOmittedCharLengthBindsUnboundedTarget verifies that the expression
// CAST(<string expr> AS CHAR) with omitted length binds to the unbounded
// TEXT target instead of VARCHAR(65535). This is what prevents the cast from
// truncating >64 KiB strings: expression results such as CONCAT can carry
// content longer than the declared width of their vector type.
func TestCastOmittedCharLengthBindsUnboundedTarget(t *testing.T) {
	for _, sql := range []string{
		"select cast(concat(repeat('a', 50000), repeat('b', 50000)) as char)",
		"select convert(concat(repeat('a', 50000), repeat('b', 50000)), char)",
		"select concat(repeat('a', 50000), repeat('b', 50000))::char",
	} {
		bound := bindFirstSelectExpr(t, sql)
		source := castBoundTarget(t, bound, types.T_text, 0)
		require.Equal(t, int32(types.T_text), bound.Typ.Id, sql)
		require.Equal(t, int32(types.T_varchar), source.Typ.Id, sql)
	}
}

// TestCastOmittedCharLengthBindsUnboundedTargetJSON covers the JSON source
// side of the same contract: a JSON value cast to an omitted-length CHAR
// keeps the serialized document intact because the target is unbounded.
func TestCastOmittedCharLengthBindsUnboundedTargetJSON(t *testing.T) {
	bound := bindFirstSelectExpr(t, "select cast(cast('{\"s\": \"a\"}' as json) as char)")
	source := castBoundTarget(t, bound, types.T_text, 0)
	require.Equal(t, int32(types.T_text), bound.Typ.Id)
	require.Equal(t, int32(types.T_json), source.Typ.Id)
}

// TestCastExplicitCharLengthKeepsBoundedTarget verifies that explicit widths
// keep their bounded cast targets, so over-length values truncate exactly as
// before for CAST(expr AS CHAR(N)) and CONVERT(expr, CHAR(N)).
func TestCastExplicitCharLengthKeepsBoundedTarget(t *testing.T) {
	bound := bindFirstSelectExpr(t, "select cast(repeat('a', 100) as char(3))")
	castBoundTarget(t, bound, types.T_char, 3)
	require.Equal(t, int32(types.T_char), bound.Typ.Id)

	convertBound := bindFirstSelectExpr(t, "select convert(repeat('a', 100), char(3))")
	castBoundTarget(t, convertBound, types.T_char, 3)
	require.Equal(t, int32(types.T_char), convertBound.Typ.Id)
}

// TestCastOmittedVarcharLengthKeepsBoundedTarget pins the scope of the
// change: an omitted-length VARCHAR cast keeps the VARCHAR(65535) default.
func TestCastOmittedVarcharLengthKeepsBoundedTarget(t *testing.T) {
	bound := bindFirstSelectExpr(t, "select cast(repeat('a', 100) as varchar)")
	castBoundTarget(t, bound, types.T_varchar, types.MaxVarcharLen)
	require.Equal(t, int32(types.T_varchar), bound.Typ.Id)
}

// TestCanonicalStringLiteralCastAcceptsTextTarget keeps the NAME_CONST
// canonical hex-literal cast whitelist aligned with the omitted-length CHAR
// binding, which now plans a TEXT target instead of VARCHAR.
func TestCanonicalStringLiteralCastAcceptsTextTarget(t *testing.T) {
	makeCast := func(targetOid types.T) *plan.Expr {
		return &plan.Expr{
			Typ: plan.Type{Id: int32(targetOid)},
			Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{ObjName: "cast"},
				Args: []*plan.Expr{
					{Typ: plan.Type{}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{IsBin: true}}},
					{Typ: plan.Type{Id: int32(targetOid)}, Expr: &plan.Expr_T{T: &plan.TargetType{}}},
				},
			}},
		}
	}
	require.True(t, isCanonicalStringLiteralCast(makeCast(types.T_text)))
	require.True(t, isCanonicalStringLiteralCast(makeCast(types.T_varchar)))
	require.False(t, isCanonicalStringLiteralCast(makeCast(types.T_char)))
}

// boundExprFn returns the bound *plan.Function of a whole bound expression.
func boundExprFn(t *testing.T, bound *Expr) *plan.Function {
	t.Helper()
	fn := bound.GetF()
	require.NotNil(t, fn)
	return fn
}

// TestOmittedLengthConsumersKeepStringOverloads verifies that the composed
// SQL from the review of #32336/#32374 keeps its pre-widening STRING
// behavior: each consumer function must bind directly on the TEXT cast
// result instead of tying into its first non-string overload through an
// implicit narrowing cast.
func TestOmittedLengthConsumersKeepStringOverloads(t *testing.T) {
	t.Run("ceil_floor_keep_numeric_string_parsing", func(t *testing.T) {
		// The decimal-string compositions keep binding the direct TEXT
		// overload, whose one-argument exact-integer executor returns the
		// digit-rendering VARCHAR that carries the exact integers and the
		// complete pre-widening FLOAT64 domain; the large-integer spellings
		// bind the same overload and keep their exact values.
		for _, sql := range []string{
			"select ceil(cast('1.2' as char))",
			"select ceil(convert('1.2', char))",
			"select ceil(cast('9007199254740993' as text))",
		} {
			bound := bindFirstSelectExpr(t, sql)
			fn := boundExprFn(t, bound)
			require.Equal(t, "ceil", fn.Func.GetObjName(), sql)
			require.Equal(t, int32(types.T_varchar), bound.Typ.Id, sql)
			require.Equal(t, int32(types.T_text), fn.Args[0].Typ.Id, sql,
				"the TEXT cast result must reach ceil directly, without a narrowing implicit cast")
		}
		for _, sql := range []string{
			"select floor(cast('-1.2' as char))",
			"select floor(convert('-1.2', char))",
			"select floor(cast('9007199254740993' as text))",
		} {
			bound := bindFirstSelectExpr(t, sql)
			fn := boundExprFn(t, bound)
			require.Equal(t, "floor", fn.Func.GetObjName(), sql)
			require.Equal(t, int32(types.T_varchar), bound.Typ.Id, sql)
			require.Equal(t, int32(types.T_text), fn.Args[0].Typ.Id, sql)
		}
	})

	t.Run("ceil_floor_compositions_stay_numeric", func(t *testing.T) {
		// The composed one-argument CEIL/FLOOR results keep the base's
		// FLOAT64 binding for arithmetic, comparisons and IN-lists: without
		// the provenance retype, the digit-rendering VARCHAR return flows
		// through the string-to-int64 implicit cast, whose range rejection
		// broke base-successful compositions such as
		// CEIL(CAST('1e40' AS CHAR)) + 1 (#32336, #32374).
		for _, tc := range []struct {
			sql string
			typ types.T
		}{
			{"select ceil(cast('1e40' as char)) + 1", types.T_float64},
			{"select floor(convert('1e40', char)) - 1", types.T_float64},
			{"select ceil(cast('1e40' as char)) * 10", types.T_float64},
			{"select floor(convert('1e40', char)) / 2", types.T_float64},
			{"select -ceil(cast('1e40' as char))", types.T_float64},
			{"select mod(ceil(cast('20.5' as char)), 2)", types.T_float64},
			{"select ceil(cast('1e40' as char)) > 0", types.T_bool},
			{"select floor(convert('18446744073709552000', char)) <= 1", types.T_bool},
			{"select ceil(cast('1e40' as char)) in (21)", types.T_bool},
			{"select ceil(cast('1e40' as char)) between 1 and 2", types.T_bool},
		} {
			bound := bindFirstSelectExpr(t, tc.sql)
			require.Equal(t, int32(tc.typ), bound.Typ.Id, tc.sql)
		}

		// The retype only follows the CEIL/FLOOR provenance: a plain widened
		// string keeps the string-to-int64 composition the base also used,
		// and an INTERVAL operand keeps this change's string temporaal
		// operator rewrite.
		plain := bindFirstSelectExpr(t, "select cast('999' as char) + 1")
		require.Equal(t, int32(types.T_int64), plain.Typ.Id)

		interval := bindFirstSelectExpr(t,
			"select ceil(cast('2024-01-01' as char)) + interval 1 day")
		intervalFn := boundExprFn(t, interval)
		require.Equal(t, "date_add", intervalFn.Func.GetObjName())
	})

	t.Run("timestampadd_keeps_string_temporal_path", func(t *testing.T) {
		for _, sql := range []string{
			"select timestampadd(hour, 1, convert('2024-01-01 12:34:56', char))",
			"select timestampadd(hour, 1, cast('2024-01-01 12:34:56' as char))",
		} {
			bound := bindFirstSelectExpr(t, sql)
			fn := boundExprFn(t, bound)
			require.Equal(t, "timestampadd", fn.Func.GetObjName(), sql)
			require.Equal(t, int32(types.T_varchar), bound.Typ.Id, sql,
				"the string-input overload returns VARCHAR, matching its VARCHAR/CHAR peers")
			require.Equal(t, int32(types.T_text), fn.Args[2].Typ.Id, sql,
				"the TEXT cast result must reach timestampadd directly, not through a DATE-narrowing implicit cast")
		}
	})

	t.Run("timestamp_keeps_string_temporal_path", func(t *testing.T) {
		for _, sql := range []string{
			"select timestamp(cast('2024-01-01 12:34:56' as char))",
			"select timestamp(convert('2024-01-01 12:34:56', char))",
		} {
			bound := bindFirstSelectExpr(t, sql)
			fn := boundExprFn(t, bound)
			require.Equal(t, "timestamp", fn.Func.GetObjName(), sql)
			require.Equal(t, int32(types.T_timestamp), bound.Typ.Id, sql)
			require.Equal(t, int32(types.T_text), fn.Args[0].Typ.Id, sql,
				"the TEXT cast result must reach timestamp directly, not through a DATE-narrowing implicit cast")
		}
	})

	t.Run("extract_keeps_string_parsing_path", func(t *testing.T) {
		for _, sql := range []string{
			"select extract(minute from cast('10:11:12' as char))",
			"select extract(minute from convert('10:11:12', char))",
		} {
			bound := bindFirstSelectExpr(t, sql)
			fn := boundExprFn(t, bound)
			require.Equal(t, "extract", fn.Func.GetObjName(), sql)
			require.Len(t, fn.Args, 2, sql)
			require.Equal(t, int32(types.T_text), fn.Args[1].Typ.Id, sql,
				"the TEXT cast result must reach extract directly, not tied into the DATETIME overload")
		}
	})

	t.Run("timestampdiff_keeps_string_temporal_path", func(t *testing.T) {
		// DATE-peered mixes must keep the string operand on the string
		// parsing overload; a narrowing cast to DATE would silently drop
		// its time-of-day and change the HOUR difference by whole hours.
		for _, sql := range []string{
			"select timestampdiff(hour, date('2024-01-01'), cast('2024-01-03 01:00:00' as char))",
			"select timestampdiff(hour, date('2024-01-01'), convert('2024-01-03 01:00:00', char))",
		} {
			bound := bindFirstSelectExpr(t, sql)
			fn := boundExprFn(t, bound)
			require.Equal(t, "timestampdiff", fn.Func.GetObjName(), sql)
			require.Equal(t, int32(types.T_int64), bound.Typ.Id, sql)
			require.Equal(t, int32(types.T_date), fn.Args[1].Typ.Id, sql,
				"the DATE peer must stay direct, without an inserted cast")
			require.Equal(t, int32(types.T_text), fn.Args[2].Typ.Id, sql,
				"the TEXT cast result must reach timestampdiff directly, not tied into the DATE overload")
		}
		for _, sql := range []string{
			"select timestampdiff(hour, cast('2024-01-03 01:00:00' as char), date('2024-01-05'))",
			"select timestampdiff(hour, convert('2024-01-03 01:00:00', char), date('2024-01-05'))",
		} {
			bound := bindFirstSelectExpr(t, sql)
			fn := boundExprFn(t, bound)
			require.Equal(t, "timestampdiff", fn.Func.GetObjName(), sql)
			require.Equal(t, int32(types.T_int64), bound.Typ.Id, sql)
			require.Equal(t, int32(types.T_text), fn.Args[1].Typ.Id, sql,
				"the TEXT cast result must reach timestampdiff directly, not tied into the DATE overload")
			require.Equal(t, int32(types.T_date), fn.Args[2].Typ.Id, sql,
				"the DATE peer must stay direct, without an inserted cast")
		}
		// The pure string pair keeps the same treatment for both operands.
		for _, sql := range []string{
			"select timestampdiff(hour, cast('2024-01-01 01:00:00' as char), cast('2024-01-03 00:30:00' as char))",
			"select timestampdiff(hour, convert('2024-01-01 01:00:00', char), convert('2024-01-03 00:30:00', char))",
		} {
			bound := bindFirstSelectExpr(t, sql)
			fn := boundExprFn(t, bound)
			require.Equal(t, "timestampdiff", fn.Func.GetObjName(), sql)
			require.Equal(t, int32(types.T_int64), bound.Typ.Id, sql)
			require.Equal(t, int32(types.T_text), fn.Args[1].Typ.Id, sql)
			require.Equal(t, int32(types.T_text), fn.Args[2].Typ.Id, sql)
		}
	})
}

// TestOmittedLengthStrToDateAcceptsTextFormat verifies the STR_TO_DATE and
// TO_DATE format argument keeps accepting the widened TEXT cast results:
// previously the format-type gate matched only VARCHAR/CHAR and returned an
// invalid-argument error during binding for these compositions.
func TestOmittedLengthStrToDateAcceptsTextFormat(t *testing.T) {
	for _, sql := range []string{
		"select str_to_date('2024-01-02', convert('%Y-%m-%d', char))",
		"select str_to_date('2024-01-02', cast('%Y-%m-%d' as char))",
		"select to_date('2024-01-02', convert('%Y-%m-%d', char))",
		"select to_date('2024-01-02', cast('%Y-%m-%d' as char))",
	} {
		bound := bindFirstSelectExpr(t, sql)
		fn := boundExprFn(t, bound)
		require.Contains(t, []string{"str_to_date", "to_date"}, fn.Func.GetObjName(), sql,
			"both spellings bind the STR_TO_DATE operator family")
		require.Equal(t, int32(types.T_date), bound.Typ.Id, sql,
			"a constant %Y-%m-%d date format must keep inferring the DATE return type")
		require.Len(t, fn.Args, 3, sql)
	}
}

// TestOmittedLengthTextKeepsOperatorRewrites guards the "+" and "-" operator
// rewrites for the widened TEXT cast target (#32336, #32374): the date
// arithmetic rewrite for INTERVAL operands and the string concatenation
// rewrite must keep firing when an operand is the omitted-length
// CAST/CONVERT-to-CHAR result, so compositions keep binding instead of
// failing through the numeric operator tables.
func TestOmittedLengthTextKeepsOperatorRewrites(t *testing.T) {
	t.Run("interval_addition", func(t *testing.T) {
		for _, sql := range []string{
			"select cast('2024-01-01 12:34:56' as char) + interval 1 day",
			"select convert('2024-01-01 12:34:56', char) + interval 1 day",
			"select interval 1 day + cast('2024-01-01 12:34:56' as char)",
			"select interval 1 day + convert('2024-01-01 12:34:56', char)",
		} {
			bound := bindFirstSelectExpr(t, sql)
			require.Equal(t, int32(types.T_text), bound.Typ.Id, sql,
				"the TEXT date_add overload returns TEXT like its VARCHAR peer")
			fn := boundExprFn(t, bound)
			require.Equal(t, "date_add", fn.Func.GetObjName(), sql)
			require.Len(t, fn.Args, 3, sql)
			require.Equal(t, int32(types.T_text), fn.Args[0].Typ.Id, sql,
				"the TEXT cast result must reach date_add through its TEXT overload, with no narrowing cast")
		}
	})

	t.Run("interval_subtraction", func(t *testing.T) {
		for _, sql := range []string{
			"select cast('2024-03-31 12:34:56' as char) - interval 1 day",
			"select convert('2024-03-31 12:34:56', char) - interval 1 day",
		} {
			bound := bindFirstSelectExpr(t, sql)
			require.Equal(t, int32(types.T_text), bound.Typ.Id, sql)
			fn := boundExprFn(t, bound)
			require.Equal(t, "date_sub", fn.Func.GetObjName(), sql)
			require.Len(t, fn.Args, 3, sql)
			require.Equal(t, int32(types.T_text), fn.Args[0].Typ.Id, sql,
				"the TEXT cast result must reach date_sub through its TEXT overload, with no narrowing cast")
		}
	})

	t.Run("string_concatenation", func(t *testing.T) {
		for _, tc := range []struct {
			sql         string
			left, right int32
		}{
			{"select cast('a' as char) + 'b'", int32(types.T_text), int32(types.T_varchar)},
			{"select convert('a', char) + 'b'", int32(types.T_text), int32(types.T_varchar)},
			{"select 'a' + cast('b' as char)", int32(types.T_varchar), int32(types.T_text)},
			{"select 'a' + convert('b', char)", int32(types.T_varchar), int32(types.T_text)},
			{"select cast('a' as char) + cast('b' as char)", int32(types.T_text), int32(types.T_text)},
			{"select convert('a', char) + convert('b', char)", int32(types.T_text), int32(types.T_text)},
		} {
			bound := bindFirstSelectExpr(t, tc.sql)
			require.Equal(t, int32(types.T_varchar), bound.Typ.Id, tc.sql,
				"concat keeps its VARCHAR result for mixed TEXT operands")
			fn := boundExprFn(t, bound)
			require.Equal(t, "concat", fn.Func.GetObjName(), tc.sql)
			require.Len(t, fn.Args, 2, tc.sql)
			require.Equal(t, tc.left, fn.Args[0].Typ.Id, tc.sql,
				"concat keeps the TEXT operand as is, with no narrowing cast")
			require.Equal(t, tc.right, fn.Args[1].Typ.Id, tc.sql,
				"concat keeps the TEXT operand as is, with no narrowing cast")
		}
	})
}

// TestOmittedLengthTextKeepsDateExtractionStringPaths guards the
// DAYOFMONTH/QUARTER/DAYNAME/MONTHNAME/WEEKOFYEAR compositions: the widened
// TEXT cast results must bind each consumer's string overload directly, not
// the first DATE overload through an equal-cost implicit cast whose
// strToDate rejects year-zero and incomplete dates.
func TestOmittedLengthTextKeepsDateExtractionStringPaths(t *testing.T) {
	for _, tc := range []struct {
		sql string
		ret int32
	}{
		{"select dayofmonth(cast('0000-01-01' as char))", int32(types.T_uint8)},
		{"select dayofmonth(convert('2024-00-15', char))", int32(types.T_uint8)},
		{"select quarter(cast('0000-01-01' as char))", int32(types.T_uint8)},
		{"select quarter(convert('0000-01-01', char))", int32(types.T_uint8)},
		{"select dayname(cast('0000-01-01' as char))", int32(types.T_varchar)},
		{"select dayname(convert('0000-01-01', char))", int32(types.T_varchar)},
		{"select monthname(cast('0000-01-01' as char))", int32(types.T_varchar)},
		{"select monthname(convert('0000-01-01', char))", int32(types.T_varchar)},
		{"select weekofyear(cast('0000-01-01' as char))", int32(types.T_int64)},
		{"select weekofyear(convert('0000-01-01', char))", int32(types.T_int64)},
	} {
		bound := bindFirstSelectExpr(t, tc.sql)
		require.Equal(t, tc.ret, bound.Typ.Id, tc.sql)
		fn := boundExprFn(t, bound)
		require.Equal(t, int32(types.T_text), fn.Args[0].Typ.Id, tc.sql,
			"the TEXT cast result must reach the string overload directly, not cast to DATE or DATETIME")
	}
}

// TestOmittedLengthCeilFloorTextRenderOrderByKeepsNumericKey pins the ORDER BY
// semantics of the digit-rendering VARCHAR result of the one-argument TEXT
// CEIL/FLOOR: the sort key is the FLOAT64 value the base route ordered by,
// while the projected display keeps its digit rendering (#32336, #32374).
// Plain widened strings keep their lexical descriptor, and the ENUM/SET
// special-order provenance is unchanged.
func TestOmittedLengthCeilFloorTextRenderOrderByKeepsNumericKey(t *testing.T) {
	for _, sql := range []string{
		"select ceil(cast(v as char)) as c from enum_order_t order by c",
		"select ceil(convert(v, char)) as c from enum_order_t order by c",
		"select v from enum_order_t order by ceil(cast(v as char))",
		"select floor(cast(v as char)) as c from enum_order_t order by c desc",
	} {
		logicPlan, err := runOneStmt(newMySQLSpecialOrderMock(), t, sql)
		require.NoError(t, err, sql)
		requireSingleSortKeyType(t, logicPlan, types.T_float64, sql)

		// The displayed projection keeps the digit-rendering VARCHAR type.
		keptRender := false
		for _, node := range logicPlan.GetQuery().Nodes {
			for _, expr := range node.ProjectList {
				if fn := expr.GetF(); fn != nil &&
					(expr.Typ.Id == int32(types.T_varchar) || expr.Typ.Id == int32(types.T_text)) {
					switch fn.Func.GetObjName() {
					case "ceil", "ceiling", "floor":
						if len(fn.Args) == 1 && fn.Args[0].Typ.Id == int32(types.T_text) {
							keptRender = true
						}
					}
				}
			}
		}
		require.True(t, keptRender, sql,
			"the visible projection must keep the digit-rendering result")
	}

	// Controls: a plain widened string keeps its lexical sort key, and the
	// ENUM special-order provenance is untouched.
	plainPlan, err := runOneStmt(newMySQLSpecialOrderMock(), t,
		"select cast(v as char) as c from enum_order_t order by c")
	require.NoError(t, err)
	requireSingleSortKeyType(t, plainPlan, types.T_text)

	enumPlan, err := runOneStmt(newMySQLSpecialOrderMock(), t,
		"select id, e from enum_order_t order by e")
	require.NoError(t, err)
	requireSingleSortKeyType(t, enumPlan, types.T_enum)
}

// collectBoundFuncs appends every plan function expression named in names
// found anywhere inside one bound expression tree.
func collectBoundFuncs(expr *plan.Expr, names map[string]bool, into *[]*plan.Function) {
	if expr == nil {
		return
	}
	switch impl := expr.Expr.(type) {
	case *plan.Expr_F:
		fn := impl.F
		if fn != nil && fn.Func != nil && names[fn.Func.GetObjName()] {
			*into = append(*into, fn)
		}
		for _, arg := range fn.GetArgs() {
			collectBoundFuncs(arg, names, into)
		}
	case *plan.Expr_List:
		for _, item := range impl.List.GetList() {
			collectBoundFuncs(item, names, into)
		}
	}
}

// collectPlanFuncs walks every expression list of every node of the logic
// plan (projections, aggregates, filters, joins, order-by specs) looking
// for the requested function names.
func collectPlanFuncs(
	t *testing.T, logicPlan *plan.Plan, names map[string]bool,
) []*plan.Function {
	t.Helper()
	var found []*plan.Function
	for _, node := range logicPlan.GetQuery().Nodes {
		for _, expr := range node.ProjectList {
			collectBoundFuncs(expr, names, &found)
		}
		for _, expr := range node.AggList {
			collectBoundFuncs(expr, names, &found)
		}
		for _, expr := range node.FilterList {
			collectBoundFuncs(expr, names, &found)
		}
		for _, expr := range node.OnList {
			collectBoundFuncs(expr, names, &found)
		}
		for _, spec := range node.OrderBy {
			collectBoundFuncs(spec.Expr, names, &found)
		}
		_ = node
	}
	return found
}

func ceilFloorRenderNames() map[string]bool {
	return map[string]bool{"ceil": true, "ceiling": true, "floor": true}
}

// requireRenderOverload asserts the decoded overload id of a plan render
// function expression against the expected value.
func requireRenderOverload(t *testing.T, fn *plan.Function, want int32, sql string) {
	t.Helper()
	_, boundOverload := function.DecodeOverloadID(fn.Func.GetObj())
	require.Equal(t, want, boundOverload, sql)
}

// collectBoundExprs walks a bound plan expression tree collecting every
// function expression whose name is requested. Unlike collectBoundFuncs it
// keeps the surrounding *plan.Expr, so the expression's own result type
// stays available to assertions.
func collectBoundExprs(expr *plan.Expr, names map[string]bool, into *[]*plan.Expr) {
	if expr == nil {
		return
	}
	switch impl := expr.Expr.(type) {
	case *plan.Expr_F:
		fn := impl.F
		if fn != nil && fn.Func != nil && names[fn.Func.GetObjName()] {
			*into = append(*into, expr)
		}
		for _, arg := range fn.GetArgs() {
			collectBoundExprs(arg, names, into)
		}
	case *plan.Expr_List:
		for _, item := range impl.List.GetList() {
			collectBoundExprs(item, names, into)
		}
	}
}

// requireRetypedRenders asserts that the bound expression retypes every
// embedded one-argument CEIL/FLOOR digit render to the given numeric
// target before its consumer sees the display string (at least one such
// cast must exist).
func requireRetypedRenders(t *testing.T, expr *plan.Expr, want types.T, sql string) {
	t.Helper()
	var casts []*plan.Expr
	collectBoundExprs(expr, map[string]bool{"cast": true}, &casts)
	found := false
	for _, cast := range casts {
		fn := cast.GetF()
		// casts carry the target type as a trailing TargetType argument, so
		// only the first argument position is the retyped operand.
		if fn == nil || len(fn.Args) == 0 || !isCeilFloorTextRenderExpr(fn.Args[0]) {
			continue
		}
		require.Equal(t, int32(want), cast.Typ.Id, sql)
		found = true
	}
	require.True(t, found, "the render must be explicitly retyped: "+sql+
		"\nbound expr: "+expr.String())
}

// requirePlanFnResultType binds sql through the full query build, collects
// every function expression named fnName across the plan's projections,
// aggregate lists, filters and order keys, and asserts each one's result
// type. Table-scan projections expose the raw column references, so the
// composed consumer must be pinned by name rather than by node position.
func requirePlanFnResultType(t *testing.T, sql, fnName string, want types.T) {
	t.Helper()
	logicPlan, err := runOneStmt(newMySQLSpecialOrderMock(), t, sql)
	require.NoError(t, err, sql)
	var found []*plan.Expr
	for _, node := range logicPlan.GetQuery().Nodes {
		for _, expr := range node.ProjectList {
			collectBoundExprs(expr, map[string]bool{fnName: true}, &found)
		}
		for _, expr := range node.AggList {
			collectBoundExprs(expr, map[string]bool{fnName: true}, &found)
		}
		for _, expr := range node.FilterList {
			collectBoundExprs(expr, map[string]bool{fnName: true}, &found)
		}
		for _, spec := range node.OrderBy {
			collectBoundExprs(spec.Expr, map[string]bool{fnName: true}, &found)
		}
	}
	require.NotEmpty(t, found, sql)
	for _, expr := range found {
		require.Equal(t, int32(want), expr.Typ.Id, sql)
	}
}

// TestCeilFloorOmittedCharRenderBindsProvenanceMarker pins the provenance
// split of the one-argument CEIL/FLOOR TEXT render (#32336, #32374, PR
// #32438 review): every omitted-length CAST/CONVERT-to-CHAR spelling binds
// the planner-only marker overload 15 — identical executor and result to
// overload 13 — while explicit TEXT casts and TEXT columns keep the
// ordinary overload 13, so the numeric retype of each provenance can keep
// its own base domain (FLOAT64 for the widened CHAR casts, exact-integer
// DECIMAL128 for TEXT).
func TestCeilFloorOmittedCharRenderBindsProvenanceMarker(t *testing.T) {
	for _, tc := range []struct {
		sql        string
		fnName     string
		markerWant int32
	}{
		{"select ceil(cast('1.2' as char))", "ceil", function.CeilTextOmittedCharRenderOverload},
		{"select ceiling(cast('1.2' as char))", "ceiling", function.CeilTextOmittedCharRenderOverload},
		{"select ceil(convert('1.2', char))", "ceil", function.CeilTextOmittedCharRenderOverload},
		{"select floor(cast('1.2' as char))", "floor", function.FloorTextOmittedCharRenderOverload},
		{"select floor(convert('1.2', char))", "floor", function.FloorTextOmittedCharRenderOverload},
		{"select ceil(cast('1' as text))", "ceil", int32(13)},
		{"select ceiling(cast('1' as text))", "ceiling", int32(13)},
		// CONVERT's MySQL type list has no TEXT spelling (CONVERT(x, text)
		// is a syntax error), so explicit TEXT provenance is CAST-only.
		{"select floor(cast('1' as text))", "floor", int32(13)},
	} {
		expr := bindFirstSelectExpr(t, tc.sql)
		fn := boundExprFn(t, expr)
		require.Equal(t, tc.fnName, fn.Func.GetObjName(), tc.sql)
		requireRenderOverload(t, fn, tc.markerWant, tc.sql)
		// Both provenances keep the same display: the digit-rendering
		// VARCHAR result whose single argument is the unbounded TEXT.
		require.Equal(t, int32(types.T_varchar), expr.Typ.Id, tc.sql)
		require.Len(t, fn.Args, 1, tc.sql)
		require.Equal(t, int32(types.T_text), fn.Args[0].Typ.Id, tc.sql)
	}

	// The same provenance split holds for correlated column inputs through
	// the full plan build: a VARCHAR column widened by the omitted-length
	// CAST keeps the marker; a TEXT column keeps the ordinary overload.
	widenedPlan, err := runOneStmt(newMySQLSpecialOrderMock(), t,
		"select ceil(cast(vrt as char)) as c from text_render_t")
	require.NoError(t, err)
	fns := collectPlanFuncs(t, widenedPlan, ceilFloorRenderNames())
	require.NotEmpty(t, fns)
	requireRenderOverload(t, fns[0], function.CeilTextOmittedCharRenderOverload,
		"select ceil(cast(vrt as char)) as c from text_render_t")

	textColPlan, err := runOneStmt(newMySQLSpecialOrderMock(), t,
		"select ceil(vt) as c from text_render_t")
	require.NoError(t, err)
	fns = collectPlanFuncs(t, textColPlan, ceilFloorRenderNames())
	require.NotEmpty(t, fns)
	requireRenderOverload(t, fns[0], 13, "select ceil(vt) as c from text_render_t")
}

// TestCeilFloorTextRenderCompositionsKeepBaseNumericDomains pins the two
// domains the PR #32438 review restored for composed consumers of the
// one-argument CEIL/FLOOR digit render (#32336, #32374):
//
//   - omitted-length CAST/CONVERT-to-CHAR inputs retype to FLOAT64, so the
//     base VARCHAR CeilStr/FloorStr route's compositions bind again,
//     including MAX/MIN/SUM/AVG aggregates and ROUND/POWER/ABS scalars and
//     every finite magnitude above the decimal ceiling;
//
//   - explicit TEXT inputs (TEXT columns, CAST/CONVERT AS TEXT) retype to
//     the exact-integer DECIMAL128: CEIL(CAST('9007199254740993' AS TEXT))
//     = 9007199254740992 keeps comparing through DECIMAL128 and stays
//     false instead of every row collapsing to the same FLOAT64 value.
func TestCeilFloorTextRenderCompositionsKeepBaseNumericDomains(t *testing.T) {
	t.Run("omitted_char_compositions_keep_float64_domain", func(t *testing.T) {
		for _, tc := range []struct {
			sql    string
			fnName string // locate the composed consumer in the full plan
			typ    types.T
		}{
			{"select ceil(cast('1e40' as char)) + 1", "", types.T_float64},
			{"select floor(convert('1e90', char)) * 10", "", types.T_float64},
			{"select round(ceil(cast('1e40' as char)))", "", types.T_float64},
			{"select round(ceil(convert('20.5', char)), 1)", "", types.T_float64},
			{"select truncate(floor(cast('30.7' as char)), 1)", "", types.T_float64},
			{"select abs(ceil(convert('-30.7', char)))", "", types.T_float64},
			{"select power(ceil(cast('20.7' as char)), 2)", "", types.T_float64},
			{"select max(ceil(cast(vrt as char))) from text_render_t", "max", types.T_float64},
			{"select min(floor(convert(vrt, char))) from text_render_t", "min", types.T_float64},
			{"select sum(ceil(cast(vrt as char))) from text_render_t", "sum", types.T_float64},
			{"select avg(floor(convert(vrt, char))) from text_render_t", "avg", types.T_float64},
			{"select ceil(cast(vrt as char)) between 1 and 2 from text_render_t", "between", types.T_bool},
			{"select ceil(cast('1e40' as char)) in (2)", "", types.T_bool},
			{"select floor(convert('2.5', char)) in (3, 4)", "", types.T_bool},
			{"select json_object('v', ceil(convert('2.5', char)))", "", types.T_json},
			{"select json_array(floor(cast('1e40' as char)))", "", types.T_json},
		} {
			if tc.fnName != "" {
				requirePlanFnResultType(t, tc.sql, tc.fnName, tc.typ)
			} else {
				bound := bindFirstSelectExpr(t, tc.sql)
				require.Equal(t, int32(tc.typ), bound.Typ.Id, tc.sql)
			}
		}
	})

	t.Run("explicit_text_compositions_keep_exact_decimal_domain", func(t *testing.T) {
		for _, tc := range []struct {
			sql    string
			fnName string // locate the composed consumer in the full plan
			typ    types.T
		}{
			{"select ceil(cast('9007199254740993' as text)) = 9007199254740992", "", types.T_bool},
			{"select floor(cast('9007199254740993' as text)) = 9007199254740992", "", types.T_bool},
			{"select ceil(cast('9007199254740993' as text)) > 9007199254740992", "", types.T_bool},
			{"select ceil(cast('9007199254740993' as text)) + 1", "", types.T_decimal128},
			{"select floor(cast('9007199254740993' as text)) - 1", "", types.T_decimal128},
			{"select round(ceil(cast('9007199254740993' as text)))", "", types.T_decimal128},
			{"select max(ceil(vt)) from text_render_t", "max", types.T_decimal128},
			{"select min(floor(vt)) from text_render_t", "min", types.T_decimal128},
			{"select sum(ceil(vt)) from text_render_t", "sum", types.T_decimal128},
			{"select abs(floor(vt)) from text_render_t", "abs", types.T_decimal128},
			{"select json_object('v', ceil(vt)) from text_render_t", "json_object", types.T_json},
			{"select ceil(cast('9007199254740993' as text)) in (9007199254740992)", "", types.T_bool},
		} {
			if tc.fnName != "" {
				requirePlanFnResultType(t, tc.sql, tc.fnName, tc.typ)
			} else {
				bound := bindFirstSelectExpr(t, tc.sql)
				require.Equal(t, int32(tc.typ), bound.Typ.Id, tc.sql)
			}
		}

		// The comparison's left operand must carry the DECIMAL128 retype of
		// the render (not FLOAT64): the operator bound re-parses the exact
		// digits, keeping the different integer sides unequal.
		bound := bindFirstSelectExpr(t,
			"select ceil(cast('9007199254740993' as text)) = 9007199254740992")
		cmp := boundExprFn(t, bound)
		require.Equal(t, "=", cmp.Func.GetObjName())
		retype := cmp.Args[0].GetF()
		require.NotNil(t, retype, "the render must be explicitly retyped for the comparison")
		require.Equal(t, "cast", retype.Func.GetObjName())
		require.Equal(t, int32(types.T_decimal128), cmp.Args[0].Typ.Id)

		// The omitted-CHAR comparison retype stays FLOAT64 (base behavior).
		widened := bindFirstSelectExpr(t,
			"select ceil(cast('1e40' as char)) > 0")
		cmp2 := boundExprFn(t, widened)
		require.Equal(t, int32(types.T_float64), cmp2.Args[0].Typ.Id)

		// The base's SUM failure over the VARCHAR render is gone for both
		// provenances and the aggregates see numeric arguments.
		aggPlan, err := runOneStmt(newMySQLSpecialOrderMock(), t,
			"select sum(ceil(cast(vrt as char))) as s, sum(ceil(vt)) as st from text_render_t")
		require.NoError(t, err, "SUM must keep binding over the digit render")
		aggs := collectPlanFuncs(t, aggPlan, map[string]bool{"sum": true})
		require.Len(t, aggs, 2)
		for _, agg := range aggs {
			cast := agg.Args[0].GetF()
			require.NotNil(t, cast, "each aggregate argument must be retyped")
			require.Equal(t, "cast", cast.Func.GetObjName())
		}
		require.Equal(t, int32(types.T_float64), aggs[0].Args[0].Typ.Id,
			"omitted-CHAR renders retype SUM to FLOAT64")
		require.Equal(t, int32(types.T_decimal128), aggs[1].Args[0].Typ.Id,
			"TEXT-column renders retype SUM to the exact DECIMAL128 domain")

		// IN lists keep the same domain split: the explicit-TEXT render
		// retypes to the exact DECIMAL128 before the member comparison
		// binds, while the omitted-CHAR widening keeps the base FLOAT64
		// IN comparison of the VARCHAR CeilStr/FloorStr route.
		inText := bindFirstSelectExpr(t,
			"select ceil(cast('9007199254740993' as text)) in (9007199254740992)")
		require.Equal(t, int32(types.T_bool), inText.Typ.Id)
		requireRetypedRenders(t, inText, types.T_decimal128,
			"select ceil(cast('9007199254740993' as text)) in (9007199254740992)")

		inWidened := bindFirstSelectExpr(t,
			"select ceil(cast('1e40' as char)) in (2)")
		require.Equal(t, int32(types.T_bool), inWidened.Typ.Id)
		requireRetypedRenders(t, inWidened, types.T_float64,
			"select ceil(cast('1e40' as char)) in (2)")

		// JSON builders embed the retyped numeric domain exactly like the
		// base result they replaced: the value positions keep the numeric
		// typing, so neither provenance embeds the digit render as a JSON
		// string, while json_object's key positions keep their string typing.
		jsonText := bindFirstSelectExpr(t,
			"select json_object('v', ceil(cast('9007199254740993' as text)))")
		require.Equal(t, int32(types.T_json), jsonText.Typ.Id)
		requireRetypedRenders(t, jsonText, types.T_decimal128,
			"select json_object('v', ceil(cast('9007199254740993' as text)))")

		jsonWidened := bindFirstSelectExpr(t,
			"select json_array(ceil(cast('1e40' as char)))")
		require.Equal(t, int32(types.T_json), jsonWidened.Typ.Id)
		requireRetypedRenders(t, jsonWidened, types.T_float64,
			"select json_array(ceil(cast('1e40' as char)))")
	})

	t.Run("order_by_keeps_exact_integer_domain_for_text_renders", func(t *testing.T) {
		logicPlan, err := runOneStmt(newMySQLSpecialOrderMock(), t,
			"select ceil(vt) as c from text_render_t order by c")
		require.NoError(t, err)
		requireSingleSortKeyType(t, logicPlan, types.T_decimal128,
			"TEXT renders must sort on the exact DECIMAL128 key")

		keptRender := false
		for _, node := range logicPlan.GetQuery().Nodes {
			for _, expr := range node.ProjectList {
				if fn := expr.GetF(); fn != nil && expr.Typ.Id == int32(types.T_varchar) {
					switch fn.Func.GetObjName() {
					case "ceil", "ceiling", "floor":
						if len(fn.Args) == 1 && fn.Args[0].Typ.Id == int32(types.T_text) {
							keptRender = true
						}
					}
				}
			}
		}
		require.True(t, keptRender, "the visible projection must keep the digit render")
	})
}

// TestCeilFloorOmittedCharWidenMarkerSurvivesQueryBoundaries pins the
// provenance of a one-argument CEIL/FLOOR argument that a derived-table or
// CTE projection exported as a plain column reference (PR #32438 review,
// #32336, #32374): the defining scope recorded the omitted-length
// CAST/CONVERT-to-CHAR widening, so the outer scope must still bind the
// FLOAT64-domain marker overload — keeping every composed
// CEIL(cast(v as char)) + 1 query on its base FLOAT64 route — instead of
// the ordinary TEXT overload whose DECIMAL128(38,0) retype rejects
// magnitudes above 38 integral digits such as 1e90.
func TestCeilFloorOmittedCharWidenMarkerSurvivesQueryBoundaries(t *testing.T) {
	for _, tc := range []struct {
		sql      string
		overload int32
	}{
		{"select ceil(c) + 1 from (select cast(vrt as char) as c from text_render_t) d", function.CeilTextOmittedCharRenderOverload},
		{"select floor(c) + 1 from (select convert(vrt, char) as c from text_render_t) d", function.FloorTextOmittedCharRenderOverload},
		{"with w as (select cast(vrt as char) as c from text_render_t) select ceil(c) from w", function.CeilTextOmittedCharRenderOverload},
		{"select ceil(c2) + 1 from (select c as c2 from (select cast(vrt as char) as c from text_render_t) d1) d2", function.CeilTextOmittedCharRenderOverload},
	} {
		logicPlan, err := runOneStmt(newMySQLSpecialOrderMock(), t, tc.sql)
		require.NoError(t, err, tc.sql)
		fns := collectPlanFuncs(t, logicPlan, ceilFloorRenderNames())
		require.NotEmpty(t, fns, tc.sql)
		requireRenderOverload(t, fns[0], tc.overload, tc.sql)
	}

	// The composed arithmetic keeps the same FLOAT64 domain the base used
	// for the omitted-CHAR widths across the boundary.
	requirePlanFnResultType(t,
		"select ceil(c) + 1 from (select cast(vrt as char) as c from text_render_t) d",
		"+", types.T_float64)

	// Controls: an explicit TEXT cast target and a plain TEXT column lose the
	// omitted-CHAR provenance at the very same boundary, so they keep the
	// ordinary overload 13 and the exact-integer DECIMAL128 composition.
	for _, tc := range []struct{ sql string }{
		{"select ceil(c) + 1 from (select cast(vrt as text) as c from text_render_t) d"},
		{"select ceil(c) + 1 from (select vt as c from text_render_t) d"},
	} {
		logicPlan, err := runOneStmt(newMySQLSpecialOrderMock(), t, tc.sql)
		require.NoError(t, err, tc.sql)
		fns := collectPlanFuncs(t, logicPlan, ceilFloorRenderNames())
		require.NotEmpty(t, fns, tc.sql)
		requireRenderOverload(t, fns[0], 13, tc.sql)
	}
	requirePlanFnResultType(t,
		"select ceil(c) + 1 from (select cast(vrt as text) as c from text_render_t) d",
		"+", types.T_decimal128)
}

// TestCeilFloorTextRenderNumbersSurviveQueryBoundaries pins the numeric
// consumption of the rendered one-argument CEIL/FLOOR results once a
// derived-table or CTE projection exported them as plain column references
// (PR #32438 review, #32336, #32374): MAX/MIN and ORDER BY keep the base's
// numeric comparison and SUM/AVG keep binding, on the exact same base
// domains the direct compositions use — FLOAT64 for the omitted-length
// CAST/CONVERT-to-CHAR provenance and the exact-integer DECIMAL128 for
// TEXT columns and explicit TEXT casts.
func TestCeilFloorTextRenderNumbersSurviveQueryBoundaries(t *testing.T) {
	// Omitted-CHAR provenance composes through FLOAT64 across boundaries.
	for _, tc := range []struct {
		sql    string
		fnName string
	}{
		{"select max(c), min(c) from (select ceil(cast(vrt as char)) as c from text_render_t) d", "max"},
		{"select max(c), min(c) from (select floor(cast(vrt as char)) as c from text_render_t) d", "min"},
		{"select sum(c) from (select ceil(cast(vrt as char)) as c from text_render_t) d", "sum"},
		{"select sum(c) from (select floor(convert(vrt, char)) as c from text_render_t) d", "sum"},
		{"select avg(c) from (select ceil(cast(vrt as char)) as c from text_render_t) d", "avg"},
		{"with w as (select ceil(cast(vrt as char)) as c from text_render_t) select max(c), min(c) from w", "max"},
		{"with w as (select floor(cast(vrt as char)) as c from text_render_t) select sum(c) from w", "sum"},
		{"select max(c) from (select c from (select ceil(cast(vrt as char)) as c from text_render_t) d1) d2", "max"},
	} {
		requirePlanFnResultType(t, tc.sql, tc.fnName, types.T_float64)
	}
	for _, tc := range []struct{ sql string }{
		{"select c from (select ceil(cast(vrt as char)) as c from text_render_t) d order by c"},
		{"select c from (select floor(convert(vrt, char)) as c from text_render_t) d order by c desc"},
		{"with w as (select ceil(cast(vrt as char)) as c from text_render_t) select c from w order by c"},
	} {
		logicPlan, err := runOneStmt(newMySQLSpecialOrderMock(), t, tc.sql)
		require.NoError(t, err, tc.sql)
		requireSingleSortKeyType(t, logicPlan, types.T_float64, tc.sql)
	}

	// TEXT-column renders keep the exact-integer DECIMAL128 domain through
	// the same boundary, so CEIL(CAST('9007199254740993' AS TEXT))-style rows
	// never collapse through FLOAT64 rounding.
	for _, tc := range []struct {
		sql    string
		fnName string
	}{
		{"select max(c), min(c) from (select ceil(vt) as c from text_render_t) d", "max"},
		{"select sum(c) from (select floor(vt) as c from text_render_t) d", "sum"},
	} {
		requirePlanFnResultType(t, tc.sql, tc.fnName, types.T_decimal128)
	}
	logicPlan, err := runOneStmt(newMySQLSpecialOrderMock(), t,
		"select c from (select ceil(vt) as c from text_render_t) d order by c")
	require.NoError(t, err)
	requireSingleSortKeyType(t, logicPlan, types.T_decimal128,
		"TEXT renders must sort on the exact DECIMAL128 key through the boundary")

	// The projected render keeps its digit-rendering VARCHAR display, and
	// consumers of unprovenanced VARCHAR columns keep their ordinary string
	// binding: the boundary retype never widens beyond the recorded domains.
	displayPlan, err := runOneStmt(newMySQLSpecialOrderMock(), t,
		"select c from (select ceil(cast(vrt as char)) as c from text_render_t) d")
	require.NoError(t, err)
	keptRender := false
	for _, node := range displayPlan.GetQuery().Nodes {
		for _, expr := range node.ProjectList {
			if fn := expr.GetF(); fn != nil && expr.Typ.Id == int32(types.T_varchar) {
				switch fn.Func.GetObjName() {
				case "ceil", "ceiling", "floor":
					if len(fn.Args) == 1 && fn.Args[0].Typ.Id == int32(types.T_text) {
						keptRender = true
					}
				}
			}
		}
	}
	require.True(t, keptRender, "the visible projection must keep the digit render")
	requirePlanFnResultType(t,
		"select max(vrt) from (select vrt from text_render_t where id > 0) d",
		"max", types.T_varchar)
}

// TestCeilFloorSetOutputsKeepBaseNumericDomains pins the numeric contract
// of the omitted-CHAR render/widen provenance through set-operation
// outputs (PR #32438 review: the mo-bot P1/P2 findings). Every set branch
// that exports a rendered one-argument CEIL/FLOOR value re-enters its
// base numeric domain at build time — FLOAT64 for the omitted-CHAR
// provenance, exact-integer DECIMAL128 for TEXT inputs — so the ordinary
// common-type rules unify the branches exactly like the base's numeric
// outputs: homogeneous and mixed-domain unions keep the base FLOAT64
// semantics for aggregates, ORDER BY and grouped ORDER BY, and a branch
// through an explicit TEXT render contributes its exact-integer domain
// to the same unification instead of failing the outer aggregate. The
// outer consumer no longer crosses the set boundary through the
// provenance walk, whose identical-domain requirement and step budget
// failed valid mixed-domain and wide UNION queries.
// Control-flow value arms retype the same way before the CASE/IF result
// unification so the composed outputs keep the base's result typing.
func TestCeilFloorSetOutputsKeepBaseNumericDomains(t *testing.T) {
	// P1: numeric aggregates over a homogeneous render union bind through
	// the base domains (SUM/AVGSTRING reject the raw VARCHAR render).
	for _, tc := range []struct{ sql, fnName string }{
		{"select sum(c) from (select ceil(cast(vrt as char)) as c from text_render_t union all select ceil(cast(vrt as char)) as c from text_render_t) d", "sum"},
		{"select max(c) from (select ceil(cast(vrt as char)) as c from text_render_t union all select floor(convert(vrt, char)) as c from text_render_t) d", "max"},
		{"select min(c) from (select floor(cast(vrt as char)) as c from text_render_t union all select ceil(convert(vrt, char)) as c from text_render_t) d", "min"},
		{"select avg(c) from (select ceil(cast(vrt as char)) as c from text_render_t union all select ceil(cast(vrt as char)) as c from text_render_t) d", "avg"},
		// UNION DISTINCT shares the same set-node output schema.
		{"select sum(c) from (select ceil(cast(vrt as char)) as c from text_render_t union select ceil(cast(vrt as char)) as c from text_render_t) d", "sum"},
		// three branches fan out over one chain of set nodes
		{"select sum(c) from (select ceil(cast(vrt as char)) as c from text_render_t union all select ceil(cast(vrt as char)) as c from text_render_t union all select floor(convert(vrt, char)) as c from text_render_t) d", "sum"},
	} {
		requirePlanFnResultType(t, tc.sql, tc.fnName, types.T_float64)
	}

	// P1: ORDER BY over the render union keeps the base numeric key, both
	// plainly and through a GROUP BY over the same set output.
	requireSingleSortKeyType(t, mustBuildPlan(t,
		"select c from (select ceil(cast(vrt as char)) as c from text_render_t union all select ceil(cast(vrt as char)) as c from text_render_t) d order by c",
	), types.T_float64)
	requireSingleSortKeyType(t, mustBuildPlan(t,
		"select c, count(*) from (select ceil(cast(vrt as char)) as c from text_render_t union all select ceil(cast(vrt as char)) as c from text_render_t) d group by c order by c",
	), types.T_float64)

	// A derived chain above the set output still resolves: the branch
	// projection passes the exported column through a nested derived
	// table before the union combines the two arms.
	requirePlanFnResultType(t,
		"select max(c2) from (select c as c2 from (select ceil(cast(vrt as char)) as c from text_render_t) d1 union all select c as c2 from (select ceil(convert(vrt, char)) as c from text_render_t) d2) d0",
		"max", types.T_float64)

	// P2: CEIL over a homogeneous widen union keeps the provenance marker
	// and the base FLOAT64 composition of the outer expression.
	logicPlan, err := runOneStmt(newMySQLSpecialOrderMock(), t,
		"select ceil(c) + 1 from (select cast(vrt as char) as c from text_render_t union all select convert(vrt, char) as c from text_render_t) d")
	require.NoError(t, err)
	fns := collectPlanFuncs(t, logicPlan, ceilFloorRenderNames())
	require.NotEmpty(t, fns)
	requireRenderOverload(t, fns[0], function.CeilTextOmittedCharRenderOverload,
		"a homogeneous omitted-CHAR widen union keeps the provenance marker")
	requirePlanFnResultType(t,
		"select ceil(c) + 1 from (select cast(vrt as char) as c from text_render_t union all select convert(vrt, char) as c from text_render_t) d",
		"+", types.T_float64)

	// P2 control: one explicit TEXT branch breaks the homogeneous
	// provenance, so the outer CEIL keeps the ordinary overload 13 and
	// the exact-integer DECIMAL128 composition (the base lost the
	// omitted-CHAR domain through that set output as well).
	logicPlan, err = runOneStmt(newMySQLSpecialOrderMock(), t,
		"select ceil(c) + 1 from (select cast(vrt as char) as c from text_render_t union all select cast(vrt as text) as c from text_render_t) d")
	require.NoError(t, err)
	fns = collectPlanFuncs(t, logicPlan, ceilFloorRenderNames())
	require.NotEmpty(t, fns)
	requireRenderOverload(t, fns[0], 13,
		"an explicit-TEXT branch keeps the ordinary render overload")
	requirePlanFnResultType(t,
		"select ceil(c) + 1 from (select cast(vrt as char) as c from text_render_t union all select cast(vrt as text) as c from text_render_t) d",
		"+", types.T_decimal128)

	// Heterogeneous render unions unify numerically (mo-bot review fix):
	// every branch re-enters its own base domain before the common-type
	// unification, so the mixed marker/TEXT-render union binds SUM through
	// FLOAT64 instead of failing with a VARCHAR aggregate argument, the
	// way the base's numeric branch outputs unified.
	requirePlanFnResultType(t,
		"select sum(c) from (select ceil(cast(vrt as char)) as c from text_render_t union all select ceil(vt) as c from text_render_t) d",
		"sum", types.T_float64)
	requirePlanFnResultType(t,
		"select sum(c) from (select ceil(cast(vrt as char)) as c from text_render_t union all select floor(cast(vrt as text)) as c from text_render_t) d",
		"sum", types.T_float64)
	// A numeric-literal branch unifies the same way: the base summed
	// FLOAT64 over this shape (the marker render previously lost its
	// domain crossing the set boundary and SUM rejected VARCHAR).
	requirePlanFnResultType(t,
		"select sum(c) from (select ceil(cast(vrt as char)) as c from text_render_t union all select 1 as c) d",
		"sum", types.T_float64)
	// A wide UNION ALL far beyond the historical provenance-walk budget
	// (48 steps) still aggregates: per-branch recovery no longer walks
	// the whole fan-out from the outer consumer.
	var wideUnion strings.Builder
	wideUnion.WriteString("select sum(n) from (")
	for i := 0; i < 56; i++ {
		if i > 0 {
			wideUnion.WriteString(" union all ")
		}
		wideUnion.WriteString("select ceil(cast(vrt as char)) as n from text_render_t")
	}
	wideUnion.WriteString(") d")
	requirePlanFnResultType(t, wideUnion.String(), "sum", types.T_float64)

	// The plain display of a homogeneous render union now carries the
	// unified numeric type directly (the VARCHAR union output the review
	// flagged as a base regression is gone).
	display, err := runOneStmt(newMySQLSpecialOrderMock(), t,
		"select c from (select ceil(cast(vrt as char)) as c from text_render_t union all select ceil(cast(vrt as char)) as c from text_render_t) d order by c")
	require.NoError(t, err)
	requireSingleSortKeyType(t, display, types.T_float64,
		"the union output sorts on the unified numeric key")
	q := display.GetQuery()
	last := q.Nodes[q.Steps[len(q.Steps)-1]]
	for _, e := range last.ProjectList {
		require.Equal(t, int32(types.T_float64), e.Typ.Id,
			"the union display column keeps the unified numeric type")
	}

	// CTAS of a bare render infers the base numeric column (mo-bot P2):
	// the omitted-CHAR provenance creates a DOUBLE column — not the
	// VARCHAR the render display carries — so downstream queries read a
	// stored numeric column like on the base.
	ctas, err := runOneStmt(newMySQLSpecialOrderMock(), t,
		"create table repro_ctas_set as select ceil(cast(vrt as char)) as n from text_render_t")
	require.NoError(t, err)
	col := ctas.GetDdl().GetCreateTable().TableDef.Cols[0]
	require.Equal(t, int32(types.T_float64), col.Typ.Id,
		"CTAS of a bare omitted-CHAR render infers the base DOUBLE column")
	require.Equal(t, "n", col.Name)
	// The explicit-TEXT render creates its established exact-integer
	// DECIMAL128 column.
	ctasText, err := runOneStmt(newMySQLSpecialOrderMock(), t,
		"create table repro_ctas_text as select ceil(vt) as n from text_render_t")
	require.NoError(t, err)
	col = ctasText.GetDdl().GetCreateTable().TableDef.Cols[0]
	require.Equal(t, int32(types.T_decimal128), col.Typ.Id,
		"CTAS of a bare TEXT render infers the exact-integer DECIMAL128 column")

	// Control-flow value arms: the render arm retypes to its own base
	// domain before the CASE/IF result unification, so the composed case
	// output keeps the base numeric typing for numeric consumers.
	for _, tc := range []struct{ sql, fnName string }{
		{"select sum(c) from (select case when id > 0 then ceil(cast(vrt as char)) else floor(cast(vrt as char)) end as c from text_render_t) d", "sum"},
		{"select sum(c) from (select case when id > 0 then ceil(cast(vrt as char)) else 0 end as c from text_render_t) d", "sum"},
		{"select sum(c) from (select if(id > 0, ceil(cast(vrt as char)), 0) as c from text_render_t) d", "sum"},
	} {
		requirePlanFnResultType(t, tc.sql, tc.fnName, types.T_float64)
	}
	// Control: CASE/IF arms of plain TEXT renders keep the established
	// exact-integer DECIMAL128 domain through the same composition.
	requirePlanFnResultType(t,
		"select sum(c) from (select case when id > 0 then ceil(vt) else floor(vt) end as c from text_render_t) d",
		"sum", types.T_decimal128)
}

// mustBuildPlan binds one SQL statement through the mock optimizer and
// fails the test on bind errors.
func mustBuildPlan(t *testing.T, sql string) *plan.Plan {
	t.Helper()
	logicPlan, err := runOneStmt(newMySQLSpecialOrderMock(), t, sql)
	require.NoError(t, err, sql)
	return logicPlan
}
