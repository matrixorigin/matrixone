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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
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
		// overload, whose one-argument exact-integer executor now returns
		// scale-0 DECIMAL128; the large-integer spellings bind the same
		// overload and keep their exact values.
		for _, sql := range []string{
			"select ceil(cast('1.2' as char))",
			"select ceil(convert('1.2', char))",
			"select ceil(cast('9007199254740993' as text))",
		} {
			bound := bindFirstSelectExpr(t, sql)
			fn := boundExprFn(t, bound)
			require.Equal(t, "ceil", fn.Func.GetObjName(), sql)
			require.Equal(t, int32(types.T_decimal128), bound.Typ.Id, sql)
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
			require.Equal(t, int32(types.T_decimal128), bound.Typ.Id, sql)
			require.Equal(t, int32(types.T_text), fn.Args[0].Typ.Id, sql)
		}
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
