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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	planfunction "github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func makeWideDecimalIntervalExpr(t *testing.T, value string) *plan.Expr {
	t.Helper()
	expr, err := makePlan2DecimalExprWithType(context.Background(), value)
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal256), expr.Typ.Id, expr.String())
	return expr
}

func makeDecimalIntervalCastExpr(
	t *testing.T, source *plan.Expr, width, scale int32,
) *plan.Expr {
	t.Helper()
	expr, err := appendCastBeforeExpr(context.Background(), source, plan.Type{
		Id:          int32(types.T_decimal256),
		Width:       width,
		Scale:       scale,
		NotNullable: true,
	})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal256), expr.Typ.Id, expr.String())
	return expr
}

func makeDecimalIntervalCastExprWithType(
	t *testing.T, source *plan.Expr, id types.T, width, scale int32, explicit bool,
) *plan.Expr {
	t.Helper()
	typ := plan.Type{
		Id:          int32(id),
		Width:       width,
		Scale:       scale,
		NotNullable: true,
	}
	var (
		expr *plan.Expr
		err  error
	)
	if explicit {
		expr, err = appendExplicitCastBeforeExpr(context.Background(), source, typ)
	} else {
		expr, err = appendCastBeforeExpr(context.Background(), source, typ)
	}
	require.NoError(t, err)
	return expr
}

func makeExplicitDecimalIntervalCastExpr(
	t *testing.T, source *plan.Expr, width, scale int32,
) *plan.Expr {
	t.Helper()
	expr, err := appendExplicitCastBeforeExpr(context.Background(), source, plan.Type{
		Id:          int32(types.T_decimal256),
		Width:       width,
		Scale:       scale,
		NotNullable: true,
	})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal256), expr.Typ.Id, expr.String())
	return expr
}

func makeWideDecimalCastIntervalExpr(
	t *testing.T, value string, width, scale int32,
) *plan.Expr {
	t.Helper()
	return makeDecimalIntervalCastExpr(t, makeStringConst(value), width, scale)
}

func TestDecimal256IntervalPreservesFractionalSeconds(t *testing.T) {
	ctx := context.Background()
	dateExpr := makeDatetimeConst("2026-01-01 00:00:00")
	value := "1.25000000000000000000000000000000000000"

	for _, tc := range []struct {
		unit string
		want int64
	}{
		{unit: "SECOND", want: 1250000},
		{unit: "MINUTE", want: 75000000},
		{unit: "HOUR", want: 4500000000},
		{unit: "DAY", want: 108000000000},
	} {
		t.Run("date arithmetic "+tc.unit, func(t *testing.T) {
			args, err := resetDateFunctionArgs(ctx, dateExpr, makeIntervalExpr(
				makeWideDecimalIntervalExpr(t, value), tc.unit))
			require.NoError(t, err)
			require.Equal(t, tc.want, extractInt64Value(args[1]))
			require.Equal(t, int64(types.MicroSecond), extractInt64Value(args[2]))
			require.True(t, args[1].GetLit().GetDecimalLiteralRequiresV82())
		})
	}

	t.Run("interval function", func(t *testing.T) {
		args, err := resetIntervalFunctionArgs(ctx, makeIntervalExpr(
			makeWideDecimalIntervalExpr(t, value), "SECOND"))
		require.NoError(t, err)
		require.Equal(t, int64(1250000), extractInt64FromExpr(args[0]))
		require.Equal(t, int64(types.MicroSecond), extractInt64FromExpr(args[1]))
		require.True(t, args[0].GetLit().GetDecimalLiteralRequiresV82())
	})
}

func TestDecimal256IntervalPreparedConstantKeepsMicroseconds(t *testing.T) {
	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare decimal256_interval from select date_add(cast('2026-01-01 00:00:00' as datetime(6)), interval 1.25000000000000000000000000000000000000 second)")
	require.NoError(t, err)
	plan := prepared.GetDcl().GetPrepare().GetPlan()
	root := plan.GetQuery().Nodes[plan.GetQuery().Steps[len(plan.GetQuery().Steps)-1]]
	require.Len(t, root.ProjectList, 1, prepared.String())
	require.Equal(t, int32(types.T_datetime), root.ProjectList[0].Typ.Id)
	want, err := types.ParseDatetime("2026-01-01 00:00:01.250000", 6)
	require.NoError(t, err)
	require.Equal(t, int64(want),
		root.ProjectList[0].GetLit().GetDatetimeval())
	require.True(t, root.ProjectList[0].GetLit().GetDecimalLiteralRequiresV82())
}

func TestDecimal256IntervalUnarySignPreservesFractionalSeconds(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		want int64
	}{
		{
			name: "date_add unary minus",
			sql: "select date_add(cast('2026-01-01 00:00:00' as datetime(6)), " +
				"interval -1.25000000000000000000000000000000000000 second)",
			want: -1250000,
		},
		{
			name: "date_sub unary minus",
			sql: "select date_sub(cast('2026-01-01 00:00:00' as datetime(6)), " +
				"interval -1.25000000000000000000000000000000000000 second)",
			want: -1250000,
		},
		{
			name: "nested float cast uses float rounding",
			sql: "select date_add(cast('2026-01-01 00:00:00' as datetime(6)), " +
				"interval cast(cast(1.005e0 as decimal(10,2)) as decimal(40,2)) second)",
			want: 1000000,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := runOneStmt(NewMockOptimizer(false), t, tc.sql)
			require.NoError(t, err)
			root := stmt.GetQuery().Nodes[stmt.GetQuery().Steps[len(stmt.GetQuery().Steps)-1]]
			require.Len(t, root.ProjectList, 1)
			fn := root.ProjectList[0].GetF()
			require.NotNil(t, fn, root.ProjectList[0].String())
			require.Len(t, fn.Args, 3, root.ProjectList[0].String())
			require.Equal(t, tc.want, extractInt64FromExpr(fn.Args[1]),
				root.ProjectList[0].String())
			require.Equal(t, int64(types.MicroSecond), extractInt64FromExpr(fn.Args[2]),
				root.ProjectList[0].String())
		})
	}
}

func TestDecimal256IntervalUnaryMinusRejectsNegativeWindowBound(t *testing.T) {
	proc := testutil.NewProc(t)
	source := makeWideDecimalIntervalExpr(t, "0."+strings.Repeat("0", 38)+"1")
	negative := &plan.Expr{
		Typ: source.Typ,
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "unary_minus"},
			Args: []*plan.Expr{source},
		}},
	}

	got, err := resetWindowIntervalExpr(context.Background(), proc,
		makeIntervalExpr(negative, "SECOND"))
	require.Nil(t, got)
	require.ErrorContains(t, err, "frame start or end is negative")
}

func TestDecimalIntervalUsesCastTargetScale(t *testing.T) {
	args, err := resetIntervalFunctionArgs(context.Background(), makeIntervalExpr(
		makeWideDecimalCastIntervalExpr(t, "1.255", 10, 2), "SECOND"))
	require.NoError(t, err)
	require.Equal(t, int64(1260000), extractInt64FromExpr(args[0]))
	require.Equal(t, int64(types.MicroSecond), extractInt64FromExpr(args[1]))
}

func TestDecimal256IntervalHandlesFloatAndNestedCasts(t *testing.T) {
	ctx := context.Background()
	floatCast := makeDecimalIntervalCastExpr(t, makeFloat64Const(1.25), 40, 2)
	args, err := resetIntervalFunctionArgs(ctx, makeIntervalExpr(floatCast, "SECOND"))
	require.NoError(t, err)
	require.Equal(t, int64(1250000), extractInt64FromExpr(args[0]))
	require.Equal(t, int64(types.MicroSecond), extractInt64FromExpr(args[1]))
	require.True(t, args[0].GetLit().GetDecimalLiteralRequiresV82())

	inner := makeWideDecimalCastIntervalExpr(t, "1.25", 40, 2)
	nested := makeDecimalIntervalCastExpr(t, inner, 40, 4)
	args, err = resetIntervalFunctionArgs(ctx, makeIntervalExpr(nested, "SECOND"))
	require.NoError(t, err)
	require.Equal(t, int64(1250000), extractInt64FromExpr(args[0]))
	require.Equal(t, int64(types.MicroSecond), extractInt64FromExpr(args[1]))
	require.True(t, args[0].GetLit().GetDecimalLiteralRequiresV82())

	for _, tc := range []struct {
		name  string
		id    types.T
		width int32
		want  int64
	}{
		{name: "decimal64 follows float conversion", id: types.T_decimal64, width: 10, want: 1000000},
		{name: "decimal128 follows float conversion", id: types.T_decimal128, width: 30, want: 1000000},
		{name: "decimal256 keeps direct float conversion", id: types.T_decimal256, width: 40, want: 1010000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inner := makeDecimalIntervalCastExprWithType(
				t, makeFloat64Const(1.005), tc.id, tc.width, 2, true)
			outer := makeDecimalIntervalCastExpr(t, inner, 40, 2)
			args, err := resetIntervalFunctionArgs(ctx, makeIntervalExpr(outer, "SECOND"))
			require.NoError(t, err)
			require.Equal(t, tc.want, extractInt64FromExpr(args[0]))
			require.Equal(t, int64(types.MicroSecond), extractInt64FromExpr(args[1]))
		})
	}
}

func TestNarrowNormalizedDecimalIntervalPreservesProtocolMarker(t *testing.T) {
	value := strings.Repeat("0", 37) + "1.25"
	dateExpr := makeDatetimeConst("2026-01-01 00:00:00")
	narrow, err := makePlan2DecimalExprWithType(context.Background(), value)
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal64), narrow.Typ.Id, narrow.String())
	require.True(t, decimalIntervalRequiresProtocol(narrow), narrow.String())

	dateArgs, err := resetDateFunctionArgs(context.Background(), dateExpr,
		makeIntervalExpr(narrow, "SECOND"))
	require.NoError(t, err)
	require.Equal(t, int64(1250000), extractInt64FromExpr(dateArgs[1]))
	require.True(t, dateArgs[1].GetLit().GetDecimalLiteralRequiresV82())

	d64, err := types.ParseDecimal64("1.25", 10, 2)
	require.NoError(t, err)
	d128, err := types.ParseDecimal128("1.25", 30, 2)
	require.NoError(t, err)
	for _, direct := range []*plan.Expr{
		MakePlan2Decimal64ExprWithType(d64, &Type{
			Id: int32(types.T_decimal64), Width: 10, Scale: 2, NotNullable: true,
		}),
		MakePlan2Decimal128ExprWithType(d128, &Type{
			Id: int32(types.T_decimal128), Width: 30, Scale: 2, NotNullable: true,
		}),
	} {
		direct.GetLit().DecimalLiteralRequiresV82 = true
		intervalArgs, intervalErr := resetIntervalFunctionArgs(context.Background(),
			makeIntervalExpr(direct, "SECOND"))
		require.NoError(t, intervalErr)
		require.Equal(t, int64(1250000), extractInt64FromExpr(intervalArgs[0]))
		require.True(t, intervalArgs[0].GetLit().GetDecimalLiteralRequiresV82())
	}

	proc := testutil.NewProc(t)
	for _, direct := range []*plan.Expr{
		MakePlan2Decimal64ExprWithType(d64, &Type{
			Id: int32(types.T_decimal64), Width: 10, Scale: 2, NotNullable: true,
		}),
		MakePlan2Decimal128ExprWithType(d128, &Type{
			Id: int32(types.T_decimal128), Width: 30, Scale: 2, NotNullable: true,
		}),
	} {
		direct.GetLit().DecimalLiteralRequiresV82 = true
		window, windowErr := resetWindowIntervalExpr(context.Background(), proc,
			makeIntervalExpr(direct, "SECOND"))
		require.NoError(t, windowErr)
		require.Equal(t, int64(1250000), extractInt64FromExpr(window.GetList().List[0]))
		require.True(t, window.GetList().List[0].GetLit().GetDecimalLiteralRequiresV82())
	}
}

func TestDecimal256IntervalMatchesExplicitDecimalCastToken(t *testing.T) {
	ctx := context.Background()
	value, err := planfunction.ParseExplicitDecimal256CastString("0b10", 40, 2)
	require.NoError(t, err)
	require.Equal(t, "2.00", value.Format(2))

	cast := makeExplicitDecimalIntervalCastExpr(t, makeStringConst("0b10"), 40, 2)
	args, err := resetIntervalFunctionArgs(ctx, makeIntervalExpr(cast, "SECOND"))
	require.NoError(t, err)
	require.Equal(t, int64(2000000), extractInt64FromExpr(args[0]))
	require.Equal(t, int64(types.MicroSecond), extractInt64FromExpr(args[1]))
	require.True(t, args[0].GetLit().GetDecimalLiteralRequiresV82())
}

func TestDecimal256IntervalCoversDecimalCastCarriers(t *testing.T) {
	for _, tc := range []struct {
		name  string
		id    types.T
		width int32
	}{
		{name: "decimal64", id: types.T_decimal64, width: 10},
		{name: "decimal128", id: types.T_decimal128, width: 30},
		{name: "decimal256", id: types.T_decimal256, width: 40},
	} {
		for _, explicit := range []bool{false, true} {
			name := tc.name + " normal"
			if explicit {
				name = tc.name + " explicit"
			}
			t.Run(name, func(t *testing.T) {
				inner := makeDecimalIntervalCastExprWithType(
					t, makeStringConst("1.255"), tc.id, tc.width, 2, explicit)
				outer := makeDecimalIntervalCastExpr(t, inner, 40, 2)
				args, err := resetIntervalFunctionArgs(context.Background(), makeIntervalExpr(outer, "SECOND"))
				require.NoError(t, err)
				require.Equal(t, int64(1260000), extractInt64FromExpr(args[0]))
				require.Equal(t, int64(types.MicroSecond), extractInt64FromExpr(args[1]))
				require.True(t, args[0].GetLit().GetDecimalLiteralRequiresV82())
			})
		}
	}

	invalid := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_decimal256), Width: 40, Scale: 2},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "cast"},
			Args: []*plan.Expr{makeStringConst("not-a-decimal")},
		}},
	}
	_, err := resetIntervalFunctionArgs(context.Background(), makeIntervalExpr(invalid, "SECOND"))
	require.Error(t, err)

	for _, tc := range []struct {
		name string
		typ  plan.Type
	}{
		{name: "decimal64", typ: plan.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2}},
		{name: "decimal128", typ: plan.Type{Id: int32(types.T_decimal128), Width: 30, Scale: 2}},
		{name: "decimal256", typ: plan.Type{Id: int32(types.T_decimal256), Width: 40, Scale: 2}},
	} {
		t.Run(tc.name+" invalid cast", func(t *testing.T) {
			_, ok, err := decimalIntervalCastText("not-a-decimal", tc.typ, false, false)
			require.Error(t, err)
			require.False(t, ok)
		})
	}
}

func TestDecimal256IntervalSourceLiteralForms(t *testing.T) {
	d128, _, err := types.Parse128("1.25")
	require.NoError(t, err)
	for _, tc := range []struct {
		name  string
		value *plan.Literal
		want  string
	}{
		{name: "decimal64", value: &plan.Literal{Value: &plan.Literal_Decimal64Val{
			Decimal64Val: &plan.Decimal64{A: 125},
		}}, want: "1.25"},
		{name: "decimal128", value: &plan.Literal{Value: &plan.Literal_Decimal128Val{
			Decimal128Val: &plan.Decimal128{A: int64(d128.B0_63), B: int64(d128.B64_127)},
		}}, want: "1.25"},
		{name: "string", value: &plan.Literal{Value: &plan.Literal_Sval{Sval: "1.25"}}, want: "1.25"},
		{name: "double", value: &plan.Literal{Value: &plan.Literal_Dval{Dval: 1.25}}, want: "1.25"},
		{name: "float", value: &plan.Literal{Value: &plan.Literal_Fval{Fval: 1.25}}, want: "1.25"},
		{name: "int8", value: &plan.Literal{Value: &plan.Literal_I8Val{I8Val: -8}}, want: "-8"},
		{name: "int16", value: &plan.Literal{Value: &plan.Literal_I16Val{I16Val: -16}}, want: "-16"},
		{name: "int32", value: &plan.Literal{Value: &plan.Literal_I32Val{I32Val: -32}}, want: "-32"},
		{name: "int64", value: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: -64}}, want: "-64"},
		{name: "uint8", value: &plan.Literal{Value: &plan.Literal_U8Val{U8Val: 8}}, want: "8"},
		{name: "uint16", value: &plan.Literal{Value: &plan.Literal_U16Val{U16Val: 16}}, want: "16"},
		{name: "uint32", value: &plan.Literal{Value: &plan.Literal_U32Val{U32Val: 32}}, want: "32"},
		{name: "uint64", value: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 64}}, want: "64"},
		{name: "true", value: &plan.Literal{Value: &plan.Literal_Bval{Bval: true}}, want: "1"},
		{name: "false", value: &plan.Literal{Value: &plan.Literal_Bval{Bval: false}}, want: "0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr := &plan.Expr{
				Typ:  plan.Type{Id: int32(types.T_decimal256), Scale: 2},
				Expr: &plan.Expr_Lit{Lit: tc.value},
			}
			got, ok, err := decimalIntervalText(expr)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, tc.want, got)
		})
	}

	for _, tc := range []struct {
		name string
		text string
		want int64
	}{
		{name: "negative half", text: "-0.0000005", want: -1},
		{name: "negative below half", text: "-0.0000004", want: 0},
		{name: "positive half", text: "0.0000005", want: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr := &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_decimal256), Scale: 7},
				Expr: &plan.Expr_Lit{Lit: &plan.Literal{
					Value: &plan.Literal_Sval{Sval: tc.text},
				}},
			}
			value, negative, handled, err := normalizeDecimalIntervalValue(expr, types.Second)
			require.NoError(t, err)
			require.True(t, handled)
			require.Equal(t, tc.want, value)
			require.Equal(t, tc.text[0] == '-', negative)
		})
	}
}

func TestDecimal256IntervalGuardsAndProtocolPaths(t *testing.T) {
	_, ok := intervalMicrosecondMultiplier(types.Week)
	require.False(t, ok)

	for _, expr := range []*plan.Expr{
		nil,
		{Typ: plan.Type{Id: int32(types.T_decimal256)}, Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{Isnull: true},
		}},
		{Typ: plan.Type{Id: int32(types.T_decimal256)}, Expr: &plan.Expr_F{
			F: &plan.Function{Func: &plan.ObjectRef{ObjName: "lower"}},
		}},
	} {
		_, ok, err := decimalIntervalText(expr)
		require.NoError(t, err)
		require.False(t, ok)
	}

	marked := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			DecimalLiteralRequiresV82: true,
		}},
	}
	unmarked := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{}},
	}
	require.False(t, decimalIntervalRequiresProtocol(nil))
	require.True(t, decimalIntervalRequiresProtocol(marked))
	require.False(t, decimalIntervalRequiresProtocol(unmarked))
	require.False(t, decimalIntervalRequiresProtocol(&plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: "plus"}}},
	}))

	for _, source := range []*plan.Expr{unmarked, marked} {
		value := makeDecimalIntervalValueExpr(source, 7)
		require.Equal(t, int64(7), extractInt64FromExpr(value))
		require.Equal(t, source.GetLit().GetDecimalLiteralRequiresV82(),
			value.GetLit().GetDecimalLiteralRequiresV82())
	}
}

func TestDecimal256IntervalRejectsInt64OverflowAfterRounding(t *testing.T) {
	ctx := context.Background()
	dateExpr := makeDatetimeConst("2026-01-01 00:00:00")
	args, err := resetDateFunctionArgs(ctx, dateExpr, makeIntervalExpr(
		makeWideDecimalIntervalExpr(t, "9223372036854.77580800000000000000000000000000000000"),
		"SECOND"))
	require.NoError(t, err)
	require.Equal(t, "to_interval_microsecond", args[1].GetF().GetFunc().GetObjName(),
		"overflow is decided at execution, after branch selection")
}

func TestDecimal256IntervalRoundsExactlyBeforeWindowValidation(t *testing.T) {
	proc := testutil.NewProc(t)
	ctx := context.Background()

	positive := makeIntervalExpr(
		makeWideDecimalIntervalExpr(t, "1.00000050000000000000000000000000000000"),
		"SECOND")
	got, err := resetWindowIntervalExpr(ctx, proc, positive)
	require.NoError(t, err)
	require.Equal(t, int64(1000001), extractInt64Value(got.GetList().List[0]))
	require.Equal(t, int64(types.MicroSecond), extractInt64Value(got.GetList().List[1]))
	require.True(t, got.GetList().List[0].GetLit().GetDecimalLiteralRequiresV82())

	negative := makeIntervalExpr(
		makeWideDecimalIntervalExpr(t, "-0."+strings.Repeat("0", 38)+"1"),
		"SECOND")
	got, err = resetWindowIntervalExpr(ctx, proc, negative)
	require.Nil(t, got)
	require.ErrorContains(t, err, "frame start or end is negative")
}
