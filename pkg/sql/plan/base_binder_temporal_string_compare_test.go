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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func makeTemporalCompareColumn(oid types.T, scale int32, colPos int32) *planpb.Expr {
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(oid), Scale: scale},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			RelPos: 0,
			ColPos: colPos,
		}},
	}
}

func makeTemporalCompareParam(pos int32) *planpb.Expr {
	return &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_text)},
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: pos}},
	}
}

func requireTemporalCompareArgTypes(t *testing.T, expr *planpb.Expr, oid types.T, scale int32) {
	t.Helper()
	fn := expr.GetF()
	require.NotNil(t, fn)
	for _, arg := range fn.Args {
		require.Equal(t, int32(oid), arg.Typ.Id)
		require.Equal(t, scale, arg.Typ.Scale)
	}
}

func requireTimeParamCast(t *testing.T, expr *planpb.Expr, argPos int, scale int32) {
	t.Helper()
	arg := expr.GetF().Args[argPos]
	require.Equal(t, int32(types.T_time), arg.Typ.Id)
	require.Equal(t, scale, arg.Typ.Scale)

	cast := arg.GetF()
	require.NotNil(t, cast)
	require.Equal(t, "cast", cast.Func.GetObjName())
	require.Len(t, cast.Args, 2)
	_, overload := function.DecodeOverloadID(cast.Func.Obj)
	require.Equal(t, int32(2), overload, "TIME comparison must retain its conversion policy")
	require.NotNil(t, cast.Args[0].GetP())
}

func TestTimeColumnStringLiteralComparisonUsesColumnScale(t *testing.T) {
	ctx := context.Background()
	timeCol := makeTemporalCompareColumn(types.T_time, 3, 0)
	stringLit := makePlan2StringConstExprWithType("12:34:56.789456")

	expr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{timeCol, stringLit})
	require.NoError(t, err)
	requireTemporalCompareArgTypes(t, expr, types.T_time, 3)
}

func TestTimeColumnStringLiteralBetweenUsesColumnScale(t *testing.T) {
	ctx := context.Background()
	timeCol := makeTemporalCompareColumn(types.T_time, 3, 0)
	lower := makePlan2StringConstExprWithType("12:34:56.789455")
	upper := makePlan2StringConstExprWithType("12:34:56.789457")

	expr, err := BindFuncExprImplByPlanExpr(ctx, "between", []*planpb.Expr{timeCol, lower, upper})
	require.NoError(t, err)
	requireTemporalCompareArgTypes(t, expr, types.T_time, 3)
}

func TestTimeColumnDirectParamComparisonUsesColumnScale(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name      string
		operator  string
		paramLeft bool
	}{
		{name: "equal", operator: "="},
		{name: "null safe equal", operator: "<=>"},
		{name: "not equal", operator: "!="},
		{name: "less than", operator: "<"},
		{name: "less equal", operator: "<="},
		{name: "greater than", operator: ">"},
		{name: "greater equal", operator: ">="},
		{name: "param left", operator: "<", paramLeft: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			timeCol := makeTemporalCompareColumn(types.T_time, 2, 0)
			param := makeTemporalCompareParam(0)
			args := []*planpb.Expr{timeCol, param}
			paramPos := 1
			if tc.paramLeft {
				args = []*planpb.Expr{param, timeCol}
				paramPos = 0
			}

			expr, err := BindFuncExprImplByPlanExpr(ctx, tc.operator, args)
			require.NoError(t, err)
			requireTemporalCompareArgTypes(t, expr, types.T_time, 2)
			requireTimeParamCast(t, expr, paramPos, 2)
		})
	}
}

func TestDateLikeColumnDirectParamComparisonUsesColumnType(t *testing.T) {
	ctx := context.Background()

	for _, temporalType := range []types.T{types.T_date, types.T_datetime, types.T_timestamp} {
		for _, operator := range []string{"=", "<=>", "!=", "<", "<=", ">", ">="} {
			t.Run(temporalType.String()+"/"+operator, func(t *testing.T) {
				column := makeTemporalCompareColumn(temporalType, 3, 0)
				param := makeTemporalCompareParam(0)

				expr, err := BindFuncExprImplByPlanExpr(ctx, operator, []*planpb.Expr{column, param})
				require.NoError(t, err)
				requireTemporalCompareArgTypes(t, expr, temporalType, 3)

				cast := expr.GetF().Args[1].GetF()
				require.NotNil(t, cast)
				require.Equal(t, "cast", cast.Func.GetObjName())
				require.NotNil(t, cast.Args[0].GetP())
			})
		}
	}
}

func TestTimeColumnDirectParamBetweenUsesColumnScale(t *testing.T) {
	ctx := context.Background()

	makeTimeValue := func() *planpb.Expr {
		value := makePlan2TimeConstExprWithType(0)
		value.Typ.Scale = 2
		return value
	}

	for _, tc := range []struct {
		name     string
		lower    *planpb.Expr
		upper    *planpb.Expr
		paramPos []int
	}{
		{
			name:     "two params",
			lower:    makeTemporalCompareParam(0),
			upper:    makeTemporalCompareParam(1),
			paramPos: []int{1, 2},
		},
		{
			name:     "lower param upper literal",
			lower:    makeTemporalCompareParam(0),
			upper:    makePlan2StringConstExprWithType("13:00:00.24"),
			paramPos: []int{1},
		},
		{
			name:     "lower literal upper param",
			lower:    makePlan2StringConstExprWithType("02:00:00.24"),
			upper:    makeTemporalCompareParam(0),
			paramPos: []int{2},
		},
		{
			name:     "lower time value upper param",
			lower:    makeTimeValue(),
			upper:    makeTemporalCompareParam(0),
			paramPos: []int{2},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			timeCol := makeTemporalCompareColumn(types.T_time, 2, 0)
			expr, err := BindFuncExprImplByPlanExpr(ctx, "between", []*planpb.Expr{timeCol, tc.lower, tc.upper})
			require.NoError(t, err)
			requireTemporalCompareArgTypes(t, expr, types.T_time, 2)
			for _, paramPos := range tc.paramPos {
				requireTimeParamCast(t, expr, paramPos, 2)
			}
		})
	}
}

func TestTimeColumnStringLiteralInUsesColumnScale(t *testing.T) {
	ctx := context.Background()
	timeCol := makeTemporalCompareColumn(types.T_time, 3, 0)
	rightList := &planpb.Expr{
		Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{
			makePlan2StringConstExprWithType("12:34:56.789456"),
		}}},
	}

	expr, err := BindFuncExprImplByPlanExpr(ctx, "in", []*planpb.Expr{timeCol, rightList})
	require.NoError(t, err)
	requireTemporalCompareArgTypes(t, expr, types.T_time, 3)
}

func TestScalarTimeStringComparisonKeepsStringSemantics(t *testing.T) {
	ctx := context.Background()
	timeLit := makePlan2TimeConstExprWithType(0)
	timeLit.Typ.Scale = 6
	stringLit := makePlan2StringConstExprWithType("030405.123456")

	expr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{timeLit, stringLit})
	require.NoError(t, err)
	requireTemporalCompareArgTypes(t, expr, types.T_varchar, 0)
}

func TestTimeColumnDynamicStringComparisonKeepsStringSemantics(t *testing.T) {
	ctx := context.Background()
	timeCol := makeTemporalCompareColumn(types.T_time, 6, 0)
	stringCol := makeTemporalCompareColumn(types.T_varchar, 0, 1)

	expr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{timeCol, stringCol})
	require.NoError(t, err)
	requireTemporalCompareArgTypes(t, expr, types.T_varchar, 0)
}

func TestScalarTimeDirectParamComparisonKeepsStringSemantics(t *testing.T) {
	ctx := context.Background()
	timeLit := makePlan2TimeConstExprWithType(0)
	timeLit.Typ.Scale = 6
	param := makeTemporalCompareParam(0)

	expr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{timeLit, param})
	require.NoError(t, err)
	requireTemporalCompareArgTypes(t, expr, types.T_varchar, 0)
}

// MySQL compares DATE values with strings in the DATETIME domain. A string
// literal with a non-zero time portion must promote the comparison to
// DATETIME(6) so its time portion is not truncated away (issue #31539).
func requireDateStringDatetimeDomain(t *testing.T, expr *planpb.Expr, datePos int) {
	t.Helper()
	fn := expr.GetF()
	require.NotNil(t, fn)
	require.Len(t, fn.Args, 2)
	// The comparison promotes both sides to DATETIME; only the string side's
	// scale matters, because it must keep the literal's fractional seconds.
	require.Equal(t, int32(types.T_datetime), fn.Args[datePos].Typ.Id)
	require.Equal(t, int32(types.T_datetime), fn.Args[1-datePos].Typ.Id)
	require.Equal(t, int32(6), fn.Args[1-datePos].Typ.Scale)
}

func TestDateFractionalStringLiteralComparisonUsesDatetimeDomain(t *testing.T) {
	ctx := context.Background()
	dateLit := makePlan2DateConstExprWithType(0)
	stringLit := makePlan2StringConstExprWithType("2026-01-01 00:00:00.001")

	for _, operator := range []string{"=", "<=>", "!=", "<", "<=", ">", ">="} {
		t.Run(operator, func(t *testing.T) {
			expr, err := BindFuncExprImplByPlanExpr(ctx, operator, []*planpb.Expr{dateLit, stringLit})
			require.NoError(t, err)
			requireDateStringDatetimeDomain(t, expr, 0)
		})
	}

	t.Run("string on the left", func(t *testing.T) {
		expr, err := BindFuncExprImplByPlanExpr(ctx, "<", []*planpb.Expr{stringLit, dateLit})
		require.NoError(t, err)
		requireDateStringDatetimeDomain(t, expr, 1)
	})

	t.Run("date column", func(t *testing.T) {
		dateCol := makeTemporalCompareColumn(types.T_date, 0, 0)
		expr, err := BindFuncExprImplByPlanExpr(ctx, "<", []*planpb.Expr{dateCol, stringLit})
		require.NoError(t, err)
		requireDateStringDatetimeDomain(t, expr, 0)
	})
}

// A string literal whose time portion is zero compares identically in the
// DATE and DATETIME domains, so it keeps the DATE domain (and its column
// fast path). Strings that fail the canonical datetime parse keep the
// DATE-domain error behavior as well.
func TestDateZeroTimeAndInvalidStringLiteralComparisonKeepsDateDomain(t *testing.T) {
	ctx := context.Background()
	dateCol := makeTemporalCompareColumn(types.T_date, 0, 0)

	for name, text := range map[string]string{
		"plain date":        "2026-01-01",
		"midnight":          "2026-01-01 00:00:00",
		"midnight fraction": "2026-01-01 00:00:00.000000",
		"compact date":      "20260101",
		"slash clock":       "2026-01-03 00/00/00",
		"invalid":           "not-a-date",
	} {
		t.Run(name, func(t *testing.T) {
			stringLit := makePlan2StringConstExprWithType(text)
			expr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{dateCol, stringLit})
			require.NoError(t, err)
			requireTemporalCompareArgTypes(t, expr, types.T_date, 0)
		})
	}
}

// A compact datetime string ('YYYYMMDDHHMMSS') parses as a datetime, but the
// DATE cast parser refuses it, so keeping the DATE domain would evaluate the
// comparison as NULL instead of matching MySQL. Such literals promote to the
// DATETIME domain even when their time portion is zero.
func TestDateCompactDatetimeStringLiteralComparisonUsesDatetimeDomain(t *testing.T) {
	ctx := context.Background()
	dateCol := makeTemporalCompareColumn(types.T_date, 0, 0)

	for name, text := range map[string]string{
		"compact midnight":   "20260101000000",
		"compact one second": "20260101000001",
	} {
		t.Run(name, func(t *testing.T) {
			stringLit := makePlan2StringConstExprWithType(text)
			expr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{dateCol, stringLit})
			require.NoError(t, err)
			requireDateStringDatetimeDomain(t, expr, 0)
		})
	}

	t.Run("compact midnight ordering", func(t *testing.T) {
		stringLit := makePlan2StringConstExprWithType("20260101000001")
		expr, err := BindFuncExprImplByPlanExpr(ctx, "<", []*planpb.Expr{dateCol, stringLit})
		require.NoError(t, err)
		requireDateStringDatetimeDomain(t, expr, 0)
	})

	t.Run("compact midnight reversed", func(t *testing.T) {
		stringLit := makePlan2StringConstExprWithType("20260101000000")
		expr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{stringLit, dateCol})
		require.NoError(t, err)
		requireDateStringDatetimeDomain(t, expr, 1)
	})
}

// A binary-introducer (_binary'...') literal carries string content that
// MySQL coerces in temporal comparisons exactly like a character string, so
// it follows the same DATE comparison rule. Binary comparisons and BETWEEN
// share this one eligibility rule, keeping NOT BETWEEN the exact negation
// of BETWEEN (issue #31539 review).
func makeBinaryIntroducerConstExpr(v string) *planpb.Expr {
	expr := makePlan2VarBinaryConstExprWithType(v)
	expr.GetLit().LiteralForm = planpb.StringLiteralForm_STRING_LITERAL_BINARY_INTRODUCER
	return expr
}

func TestDateBinaryIntroducerStringLiteralComparisonUsesDatetimeDomain(t *testing.T) {
	ctx := context.Background()
	dateCol := makeTemporalCompareColumn(types.T_date, 0, 0)

	t.Run("fractional promotes", func(t *testing.T) {
		expr, err := BindFuncExprImplByPlanExpr(ctx, "<", []*planpb.Expr{dateCol, makeBinaryIntroducerConstExpr("2026-01-01 00:00:00.001")})
		require.NoError(t, err)
		requireDateStringDatetimeDomain(t, expr, 0)
	})

	t.Run("noon equality promotes", func(t *testing.T) {
		expr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{dateCol, makeBinaryIntroducerConstExpr("2026-01-01 12:00:00")})
		require.NoError(t, err)
		requireDateStringDatetimeDomain(t, expr, 0)
	})

	t.Run("reversed", func(t *testing.T) {
		expr, err := BindFuncExprImplByPlanExpr(ctx, ">", []*planpb.Expr{makeBinaryIntroducerConstExpr("2026-01-01 00:00:00.001"), dateCol})
		require.NoError(t, err)
		requireDateStringDatetimeDomain(t, expr, 1)
	})

	t.Run("compact datetime promotes", func(t *testing.T) {
		expr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{dateCol, makeBinaryIntroducerConstExpr("20260101000000")})
		require.NoError(t, err)
		requireDateStringDatetimeDomain(t, expr, 0)
	})

	t.Run("zero time keeps date domain", func(t *testing.T) {
		expr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{dateCol, makeBinaryIntroducerConstExpr("2026-01-01 00:00:00")})
		require.NoError(t, err)
		requireTemporalCompareArgTypes(t, expr, types.T_date, 0)
	})

	t.Run("invalid keeps date domain", func(t *testing.T) {
		expr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{dateCol, makeBinaryIntroducerConstExpr("not-a-date")})
		require.NoError(t, err)
		requireTemporalCompareArgTypes(t, expr, types.T_date, 0)
	})
}

func TestDateFractionalStringLiteralBetweenUsesDatetimeDomain(t *testing.T) {
	ctx := context.Background()
	dateCol := makeTemporalCompareColumn(types.T_date, 0, 0)

	t.Run("fractional lower bound", func(t *testing.T) {
		lower := makePlan2StringConstExprWithType("2026-01-01 00:00:00.5")
		upper := makePlan2StringConstExprWithType("2026-01-02")
		expr, err := BindFuncExprImplByPlanExpr(ctx, "between", []*planpb.Expr{dateCol, lower, upper})
		require.NoError(t, err)
		fn := expr.GetF()
		require.NotNil(t, fn)
		require.Len(t, fn.Args, 3)
		require.Equal(t, int32(types.T_datetime), fn.Args[0].Typ.Id)
		require.Equal(t, int32(types.T_datetime), fn.Args[1].Typ.Id)
		require.Equal(t, int32(6), fn.Args[1].Typ.Scale)
		require.Equal(t, int32(types.T_datetime), fn.Args[2].Typ.Id)
		require.Equal(t, int32(6), fn.Args[2].Typ.Scale)
	})

	t.Run("fractional upper bound", func(t *testing.T) {
		lower := makePlan2StringConstExprWithType("2026-01-01")
		upper := makePlan2StringConstExprWithType("2026-01-02 23:59:59")
		expr, err := BindFuncExprImplByPlanExpr(ctx, "between", []*planpb.Expr{dateCol, lower, upper})
		require.NoError(t, err)
		fn := expr.GetF()
		require.NotNil(t, fn)
		require.Equal(t, int32(types.T_datetime), fn.Args[0].Typ.Id)
		require.Equal(t, int32(types.T_datetime), fn.Args[1].Typ.Id)
		require.Equal(t, int32(types.T_datetime), fn.Args[2].Typ.Id)
	})

	t.Run("zero time bounds keep date domain", func(t *testing.T) {
		lower := makePlan2StringConstExprWithType("2026-01-01")
		upper := makePlan2StringConstExprWithType("2026-01-02 00:00:00")
		expr, err := BindFuncExprImplByPlanExpr(ctx, "between", []*planpb.Expr{dateCol, lower, upper})
		require.NoError(t, err)
		fn := expr.GetF()
		require.NotNil(t, fn)
		for _, arg := range fn.Args {
			require.Equal(t, int32(types.T_date), arg.Typ.Id)
		}
	})

	t.Run("compact midnight bounds promote", func(t *testing.T) {
		// The DATE cast parser refuses compact datetimes, so the range would
		// evaluate as NULL in the DATE domain; both bounds are datetime
		// parseable, so the range promotes.
		lower := makePlan2StringConstExprWithType("20260101000000")
		upper := makePlan2StringConstExprWithType("20260103000000")
		expr, err := BindFuncExprImplByPlanExpr(ctx, "between", []*planpb.Expr{dateCol, lower, upper})
		require.NoError(t, err)
		fn := expr.GetF()
		require.NotNil(t, fn)
		require.Len(t, fn.Args, 3)
		for _, arg := range fn.Args {
			require.Equal(t, int32(types.T_datetime), arg.Typ.Id)
		}
		require.Equal(t, int32(6), fn.Args[1].Typ.Scale)
		require.Equal(t, int32(6), fn.Args[2].Typ.Scale)
	})

	t.Run("datetime unparseable bound expands to independent comparisons", func(t *testing.T) {
		// The datetime parser rejects the slash-separated clock, so the
		// range cannot share one DATETIME domain. The positive range expands
		// to the same independent comparisons NOT BETWEEN uses: the noon
		// bound keeps its time portion in the DATETIME domain while the
		// slash-clock bound converts in the DATE domain (the conversion the
		// DATE cast accepts). This keeps BETWEEN the exact negation of NOT
		// BETWEEN and excludes the midnight row, matching MySQL.
		lower := makePlan2StringConstExprWithType("2026-01-01 12:00:00")
		upper := makePlan2StringConstExprWithType("2026-01-03 00/00/00")
		expr, err := BindFuncExprImplByPlanExpr(ctx, "between", []*planpb.Expr{dateCol, lower, upper})
		require.NoError(t, err)
		lowerCmp, upperCmp := requireBetweenExpansion(t, expr)
		requireDateStringDatetimeDomain(t, lowerCmp, 0)
		requireTemporalCompareArgTypes(t, upperCmp, types.T_date, 0)
	})

	t.Run("typed date peer expands to independent comparisons", func(t *testing.T) {
		// A typed DATE bound is not a string literal, so a fused range
		// would truncate the noon bound in its shared DATE domain while NOT
		// BETWEEN preserves it. Expand instead.
		lower := makePlan2StringConstExprWithType("2026-01-01 12:00:00")
		upper := makePlan2DateConstExprWithType(0)
		expr, err := BindFuncExprImplByPlanExpr(ctx, "between", []*planpb.Expr{dateCol, lower, upper})
		require.NoError(t, err)
		lowerCmp, upperCmp := requireBetweenExpansion(t, expr)
		requireDateStringDatetimeDomain(t, lowerCmp, 0)
		requireTemporalCompareArgTypes(t, upperCmp, types.T_date, 0)
	})

	t.Run("promoting upper bound with typed date peer expands", func(t *testing.T) {
		lower := makePlan2DateConstExprWithType(0)
		upper := makePlan2StringConstExprWithType("2026-01-03 12:00:00")
		expr, err := BindFuncExprImplByPlanExpr(ctx, "between", []*planpb.Expr{dateCol, lower, upper})
		require.NoError(t, err)
		lowerCmp, upperCmp := requireBetweenExpansion(t, expr)
		requireTemporalCompareArgTypes(t, lowerCmp, types.T_date, 0)
		requireDateStringDatetimeDomain(t, upperCmp, 0)
	})

	t.Run("parameter bound expands to independent comparisons", func(t *testing.T) {
		// A direct parameter is not a literal, so the range expands; the
		// parameter bound keeps its per-bound DATE-domain conversion while
		// the promoting string bound keeps its time portion.
		lower := makeTemporalCompareParam(0)
		upper := makePlan2StringConstExprWithType("2026-01-03 12:00:00")
		expr, err := BindFuncExprImplByPlanExpr(ctx, "between", []*planpb.Expr{dateCol, lower, upper})
		require.NoError(t, err)
		_, upperCmp := requireBetweenExpansion(t, expr)
		requireDateStringDatetimeDomain(t, upperCmp, 0)
	})

	t.Run("binary introducer bounds promote like character strings", func(t *testing.T) {
		lower := makeBinaryIntroducerConstExpr("2026-01-01 12:00:00")
		upper := makeBinaryIntroducerConstExpr("2026-01-03")
		expr, err := BindFuncExprImplByPlanExpr(ctx, "between", []*planpb.Expr{dateCol, lower, upper})
		require.NoError(t, err)
		fn := expr.GetF()
		require.NotNil(t, fn)
		require.Len(t, fn.Args, 3)
		for _, arg := range fn.Args {
			require.Equal(t, int32(types.T_datetime), arg.Typ.Id)
		}
	})

	t.Run("binary introducer unparseable bound expands", func(t *testing.T) {
		// The noon _binary bound promotes exactly as it would in a
		// standalone comparison; the slash-clock _binary bound keeps the
		// DATE-domain conversion. This is the expansion NOT BETWEEN uses,
		// so the two forms stay exact negations.
		lower := makeBinaryIntroducerConstExpr("2026-01-01 12:00:00")
		upper := makeBinaryIntroducerConstExpr("2026-01-03 00/00/00")
		expr, err := BindFuncExprImplByPlanExpr(ctx, "between", []*planpb.Expr{dateCol, lower, upper})
		require.NoError(t, err)
		lowerCmp, upperCmp := requireBetweenExpansion(t, expr)
		requireDateStringDatetimeDomain(t, lowerCmp, 0)
		requireTemporalCompareArgTypes(t, upperCmp, types.T_date, 0)
	})
}

// requireBetweenExpansion asserts that a positive range expanded to the
// same independent comparisons NOT BETWEEN uses and returns them
// (left >= lower, left <= upper).
func requireBetweenExpansion(t *testing.T, expr *planpb.Expr) (lowerCmp *planpb.Expr, upperCmp *planpb.Expr) {
	t.Helper()
	fn := expr.GetF()
	require.NotNil(t, fn)
	require.Equal(t, "and", fn.Func.GetObjName())
	require.Len(t, fn.Args, 2)
	lowerCmp = fn.Args[0]
	upperCmp = fn.Args[1]
	require.Equal(t, ">=", lowerCmp.GetF().Func.GetObjName())
	require.Equal(t, "<=", upperCmp.GetF().Func.GetObjName())
	return lowerCmp, upperCmp
}

// Scalar TIME comparisons keep MySQL's string semantics and must not be
// promoted by the DATE rule.
func TestScalarTimeShortStringComparisonKeepsStringSemantics(t *testing.T) {
	ctx := context.Background()
	timeLit := makePlan2TimeConstExprWithType(0)
	stringLit := makePlan2StringConstExprWithType("05:00")

	expr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{timeLit, stringLit})
	require.NoError(t, err)
	requireTemporalCompareArgTypes(t, expr, types.T_varchar, 0)
}

// bindVolatileDateExpr binds a DATE-typed expression whose evaluation is
// volatile: CAST(DATE_ADD(DATE '2026-01-01', INTERVAL nextval(...) DAY) AS
// DATE). It carries no evaluation memo because the SELECT projection does
// not duplicate it.
func bindVolatileDateExpr(t *testing.T) *planpb.Expr {
	t.Helper()
	logicPlan, err := runOneStmt(NewMockOptimizer(true, newPlanTestProcess(t)), t,
		"select cast(date_add(date '2026-01-01', interval nextval('date_string_between_seq') day) as date)")
	require.NoError(t, err)
	for _, node := range logicPlan.GetQuery().Nodes {
		for _, expr := range node.ProjectList {
			if expr.Typ.Id == int32(types.T_date) && containsVolatileFunction(expr) {
				require.Zero(t, expr.AuxId)
				return expr
			}
		}
	}
	require.FailNow(t, "no volatile DATE projection")
	return nil
}

// requireSingleNegativeAuxID returns the one evaluation memo (negative
// AuxId) carried anywhere inside expr.
func requireSingleNegativeAuxID(t *testing.T, expr *planpb.Expr) int32 {
	t.Helper()
	ids := map[int32]struct{}{}
	collectNegativeAuxIDs(expr, ids)
	require.Len(t, ids, 1)
	for id := range ids {
		return id
	}
	return 0
}

// The range expansion rebinds its left operand in each comparison, so a
// volatile left operand such as a sequence must not evaluate twice. The
// query binder assigns one evaluation memo (negative AuxId) before the
// expansion runs, and both independent comparisons consume the same
// memoized value (issue #31539 review).
func TestDateStringVolatileBetweenExpandsWithSharedEvaluationMemo(t *testing.T) {
	logicPlan, err := runOneStmt(NewMockOptimizer(true, newPlanTestProcess(t)), t,
		"select cast(date_add(date '2026-01-01', interval nextval('date_string_between_seq') day) as date) "+
			"between '2026-01-01 12:00:00' and date '2026-01-02'")
	require.NoError(t, err)

	var rangeExpr *planpb.Expr
	var visit func(*planpb.Expr)
	visit = func(expr *planpb.Expr) {
		if expr == nil {
			return
		}
		if fn := expr.GetF(); fn != nil {
			if fn.Func.GetObjName() == "and" && rangeExpr == nil {
				rangeExpr = expr
			}
			for _, arg := range fn.Args {
				visit(arg)
			}
		}
	}
	for _, node := range logicPlan.GetQuery().Nodes {
		for _, projection := range node.ProjectList {
			visit(projection)
		}
	}
	require.NotNil(t, rangeExpr, "the DATE/string range must expand to independent comparisons")
	lowerCmp, upperCmp := requireBetweenExpansion(t, rangeExpr)
	lowerMemo := requireSingleNegativeAuxID(t, lowerCmp.GetF().Args[0])
	upperMemo := requireSingleNegativeAuxID(t, upperCmp.GetF().Args[0])
	require.Equal(t, lowerMemo, upperMemo,
		"both comparisons must consume one memoized evaluation of the volatile operand")
}

// A volatile left operand without a binder-issued evaluation memo keeps the
// fused range: the builder-less binder cannot memoize, and expanding would
// evaluate the operand once per comparison.
func TestDateStringVolatileBetweenKeepsFusedRangeWithoutEvaluationMemo(t *testing.T) {
	ctx := context.Background()
	left := bindVolatileDateExpr(t)
	lower := makePlan2StringConstExprWithType("2026-01-01 12:00:00")
	upper := makePlan2DateConstExprWithType(0)

	expr, err := BindFuncExprImplByPlanExpr(ctx, "between", []*planpb.Expr{left, lower, upper})
	require.NoError(t, err)
	fn := expr.GetF()
	require.NotNil(t, fn)
	require.Equal(t, "between", fn.Func.GetObjName(),
		"an unmemoized volatile operand keeps the fused single-evaluation range")
}

// With a binder-issued memo the range still expands, and the deep-copied
// operand in each comparison carries the same memo key, so the executor
// evaluates the volatile source once.
func TestDateStringVolatileBetweenExpansionSharesMemoizedEvaluation(t *testing.T) {
	ctx := context.Background()
	left := bindVolatileDateExpr(t)
	left.AuxId = -7 // binder-issued evaluation memo
	lower := makePlan2StringConstExprWithType("2026-01-01 12:00:00")
	upper := makePlan2DateConstExprWithType(0)

	expr, err := BindFuncExprImplByPlanExpr(ctx, "between", []*planpb.Expr{left, lower, upper})
	require.NoError(t, err)
	lowerCmp, upperCmp := requireBetweenExpansion(t, expr)
	require.Equal(t, int32(-7), requireSingleNegativeAuxID(t, lowerCmp.GetF().Args[0]))
	require.Equal(t, int32(-7), requireSingleNegativeAuxID(t, upperCmp.GetF().Args[0]))
}
