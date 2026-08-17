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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func requireExactDecimalComparisonArgs(t *testing.T, expr *planpb.Expr, scale int32) {
	t.Helper()
	fn := expr.GetF()
	require.NotNil(t, fn)
	require.Len(t, fn.Args, 2)
	for _, arg := range fn.Args {
		require.True(t, types.T(arg.Typ.Id).IsDecimal(), "type id %d", arg.Typ.Id)
		require.Equal(t, scale, arg.Typ.Scale, "type: %+v", arg.Typ)
	}
}

func makeExplicitVarcharLiteralCast(t *testing.T, ctx context.Context, value string) *planpb.Expr {
	t.Helper()
	target := types.T_varchar.ToType()
	target.Width = int32(len(value))
	expr, err := appendExplicitCastBeforeExpr(ctx, makePlan2StringConstExprWithType(value), makePlan2Type(&target))
	require.NoError(t, err)
	return expr
}

func containsVarcharLiteralCast(expr *planpb.Expr) bool {
	if expr == nil {
		return false
	}
	fn := expr.GetF()
	if fn == nil {
		return false
	}
	if fn.Func.GetObjName() == "cast" && types.T(expr.Typ.Id).IsMySQLString() &&
		len(fn.Args) > 0 && fn.Args[0].GetLit() != nil {
		return true
	}
	for _, arg := range fn.Args {
		if containsVarcharLiteralCast(arg) {
			return true
		}
	}
	return false
}

func decimalCastSourceString(expr *planpb.Expr) (string, bool) {
	if expr == nil {
		return "", false
	}
	if literal := expr.GetLit(); literal != nil {
		value, ok := literal.Value.(*planpb.Literal_Sval)
		if ok {
			return value.Sval, true
		}
		return "", false
	}
	fn := expr.GetF()
	if fn == nil {
		return "", false
	}
	for _, arg := range fn.Args {
		if value, ok := decimalCastSourceString(arg); ok {
			return value, true
		}
	}
	return "", false
}

func TestDecimalStringLiteralComparisonsUseExactDecimalTypes(t *testing.T) {
	ctx := context.Background()
	decimalTypes := []struct {
		typ   types.Type
		value string
	}{
		{typ: types.New(types.T_decimal64, 18, 2), value: "9007199254740992.01"},
		{typ: types.New(types.T_decimal128, 20, 4), value: "9007199254740992.0001"},
		{typ: types.New(types.T_decimal256, 40, 4), value: "9007199254740992.0001"},
	}
	operators := []string{"=", "<=>", "!=", "<>", "<", "<=", ">", ">="}

	for _, decimal := range decimalTypes {
		for _, operator := range operators {
			for _, literalLeft := range []bool{false, true} {
				name := fmt.Sprintf("%s/%s/literal_left=%t", decimal.typ.Oid.String(), operator, literalLeft)
				t.Run(name, func(t *testing.T) {
					column := makePreparedDecimalComparisonColumn(decimal.typ)
					literal := makePlan2StringConstExprWithType(decimal.value)
					args := []*planpb.Expr{column, literal}
					if literalLeft {
						args = []*planpb.Expr{literal, column}
					}

					expr, err := BindFuncExprImplByPlanExpr(ctx, operator, args)
					require.NoError(t, err)
					requireExactDecimalComparisonArgs(t, expr, decimal.typ.Scale)
				})
			}
		}
	}
}

func TestDecimalStringLiteralCastUsesExactDecimalType(t *testing.T) {
	ctx := context.Background()
	decimalType := types.New(types.T_decimal128, 20, 4)
	stringCast := makeExplicitVarcharLiteralCast(t, ctx, "9007199254740992.0001")

	expr, err := BindFuncExprImplByPlanExpr(ctx, "<=>", []*planpb.Expr{
		makePreparedDecimalComparisonColumn(decimalType),
		stringCast,
	})
	require.NoError(t, err)
	requireExactDecimalComparisonArgs(t, expr, decimalType.Scale)
	require.True(t, containsVarcharLiteralCast(expr), "the explicit VARCHAR cast must remain in the expression")
}

func TestPreparedDecimalComparisonRepresentativeAccountsForPeerIntegralDomain(t *testing.T) {
	ctx := context.Background()
	peer := types.New(types.T_decimal256, 65, 30)

	for _, value := range []string{"1e-43", "-1e-43"} {
		expr := makePlan2StringConstExprWithType(value)
		expr.ExactDecimalParam = true
		representative, err := preparedDecimalComparisonRepresentative(ctx, expr, &planpb.Expr{Typ: makePlan2Type(&peer)})
		require.NoError(t, err)
		require.NotSame(t, expr, representative)
		require.Equal(t, int32(types.T_decimal256), representative.Typ.Id)
		require.Equal(t, int32(41), representative.Typ.Scale)
		require.True(t, representative.ExactDecimalParam)
	}
}

func TestDecimalComparisonPeerDomainUsesOriginalCastDomain(t *testing.T) {
	ctx := context.Background()
	original := makePreparedDecimalComparisonColumn(types.New(types.T_decimal256, 65, 30))
	outerType := types.New(types.T_decimal256, 76, 43)
	outer, err := appendCastBeforeExpr(ctx, original, makePlan2Type(&outerType))
	require.NoError(t, err)

	integral, scale := decimalComparisonPeerDomain(outer)
	require.Equal(t, int32(35), integral)
	require.Equal(t, int32(30), scale)
}

func TestDecimalStringLiteralComparisonPreservesHigherScale(t *testing.T) {
	ctx := context.Background()
	decimalType := types.New(types.T_decimal128, 20, 4)

	expr, err := BindFuncExprImplByPlanExpr(ctx, "<", []*planpb.Expr{
		makePreparedDecimalComparisonColumn(decimalType),
		makePlan2StringConstExprWithType("1.23456"),
	})
	require.NoError(t, err)
	fn := expr.GetF()
	require.NotNil(t, fn)
	require.Len(t, fn.Args, 2)
	for _, arg := range fn.Args {
		require.True(t, types.T(arg.Typ.Id).IsDecimal(), "type id %d", arg.Typ.Id)
	}
	require.Equal(t, int32(4), fn.Args[0].Typ.Scale)
	require.Equal(t, int32(5), fn.Args[1].Typ.Scale)
}

func TestDecimalStringLiteralUsesMySQLNumericPrefix(t *testing.T) {
	ctx := context.Background()
	decimalType := types.New(types.T_decimal128, 20, 4)
	for _, test := range []struct {
		name  string
		input string
		want  string
	}{
		{name: "hex-looking text", input: "0x10", want: "0"},
		{name: "embedded plus", input: "1+2", want: "1"},
		{name: "embedded space", input: "1 2", want: "1"},
		{name: "scientific suffix", input: "1e2suffix", want: "100"},
		{name: "incomplete exponent", input: "1e+suffix", want: "1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			expr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{
				makePreparedDecimalComparisonColumn(decimalType),
				makePlan2StringConstExprWithType(test.input),
			})
			require.NoError(t, err)
			requireExactDecimalComparisonArgs(t, expr, expr.GetF().Args[0].Typ.Scale)
			value, ok := decimalCastSourceString(expr.GetF().Args[1])
			require.True(t, ok)
			require.Equal(t, test.want, value)
		})
	}
}

func TestDecimalLiteralNaturalTypeUsesMathematicalValue(t *testing.T) {
	for _, test := range []struct {
		name  string
		value string
		oid   types.T
		width int32
		scale int32
	}{
		{name: "positive exponent", value: "1e2", oid: types.T_decimal64, width: 3, scale: 0},
		{name: "uppercase exponent", value: "1E2", oid: types.T_decimal64, width: 3, scale: 0},
		{name: "negative exponent", value: "1e-2", oid: types.T_decimal64, width: 2, scale: 2},
		{name: "leading zeros", value: "0001.20", oid: types.T_decimal64, width: 2, scale: 1},
		{name: "fractional zero", value: "0.000", oid: types.T_decimal64, width: 1, scale: 0},
		{name: "decimal128 boundary", value: "99999999999999999999999999999999999999", oid: types.T_decimal128, width: 38, scale: 0},
		{name: "scale 37 stays decimal128", value: "1e-37", oid: types.T_decimal128, width: 37, scale: 37},
		{name: "scale 38 promotes without rounding", value: "1e-38", oid: types.T_decimal256, width: 38, scale: 38},
		{name: "scale 39 remains exact", value: "1e-39", oid: types.T_decimal256, width: 39, scale: 39},
		{name: "scale 42 remains exact", value: "1e-42", oid: types.T_decimal256, width: 42, scale: 42},
		{name: "scale 43 remains exact", value: "1e-43", oid: types.T_decimal256, width: 43, scale: 43},
		{
			name:  "decimal256 promotion",
			value: "12345678.0000000000000000000000000000001",
			oid:   types.T_decimal256,
			width: 39,
			scale: 31,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			expr, exact, err := makePlan2ExactDecimalStringExprWithType(context.Background(), test.value)
			require.NoError(t, err)
			require.True(t, exact)
			require.Equal(t, int32(test.oid), expr.Typ.Id)
			require.Equal(t, test.width, expr.Typ.Width)
			require.Equal(t, test.scale, expr.Typ.Scale)
		})
	}
}

func TestDecimalStringLiteralBeyond128KeepsExactComparison(t *testing.T) {
	ctx := context.Background()
	value := "12345678.0000000000000000000000000000001"
	columnType := types.New(types.T_decimal128, 38, 30)
	for _, operator := range []string{"=", "<"} {
		for _, literalLeft := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/literal_left=%t", operator, literalLeft), func(t *testing.T) {
				args := []*planpb.Expr{
					makePreparedDecimalComparisonColumn(columnType),
					makePlan2StringConstExprWithType(value),
				}
				if literalLeft {
					args[0], args[1] = args[1], args[0]
				}

				expr, err := BindFuncExprImplByPlanExpr(ctx, operator, args)
				require.NoError(t, err)
				if operator == "=" {
					require.NotNil(t, expr.GetLit())
					require.False(t, expr.GetLit().GetBval())
					return
				}
				require.Len(t, expr.GetF().Args, 2)
				require.Equal(t, int32(types.T_decimal256), expr.GetF().Args[0].Typ.Id, "%+v", expr)
				require.Equal(t, int32(types.T_decimal256), expr.GetF().Args[1].Typ.Id, "%+v", expr)
				castValue, ok := decimalCastSourceString(expr)
				require.True(t, ok)
				require.Equal(t, value, castValue)
			})
		}
	}
}

func TestDecimalNonExactStringExpressionsKeepGenericCoercion(t *testing.T) {
	ctx := context.Background()
	decimalType := types.New(types.T_decimal128, 20, 4)
	controls := []struct {
		name string
		expr *planpb.Expr
	}{
		{name: "non-numeric literal", expr: makePlan2StringConstExprWithType("not-a-number")},
		{name: "null literal", expr: MakePlan2NullTextConstExprWithType("")},
		{name: "raw binary literal", expr: &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetBinary)},
			Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
				IsBin: true, Value: &planpb.Literal_Sval{Sval: "9007199254740992.0001"}}},
		}},
		{name: "varchar column", expr: &planpb.Expr{
			Typ:  planpb.Type{Id: int32(types.T_varchar)},
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 1}},
		}},
	}

	for _, tc := range controls {
		t.Run(tc.name, func(t *testing.T) {
			expr, err := BindFuncExprImplByPlanExpr(ctx, "<=>", []*planpb.Expr{
				makePreparedDecimalComparisonColumn(decimalType),
				tc.expr,
			})
			require.NoError(t, err)
			for _, arg := range expr.GetF().Args {
				require.Equal(t, int32(types.T_float64), arg.Typ.Id)
			}
		})
	}
}

func TestDecimalBinaryStringLiteralUsesExactComparison(t *testing.T) {
	mock := NewMockOptimizer(false)
	decimalType := types.New(types.T_decimal128, 20, 4)
	mock.ctxt.tables["part"].Cols[7].Typ = makePlan2Type(&decimalType)
	for _, test := range []struct {
		name  string
		value string
		exact bool
	}{
		{name: "binary character introducer", value: "_binary'9007199254740992.0001'", exact: true},
		{name: "raw hex", value: "x'39'"},
		{name: "raw bit", value: "b'111001'"},
	} {
		t.Run(test.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(mock, t,
				"select p_partkey from part where p_retailprice = "+test.value)
			require.NoError(t, err)
			expr := findPreparedDecimalComparisonInPlan(logicPlan, "=")
			require.NotNil(t, expr)
			for _, arg := range expr.GetF().Args {
				if test.exact {
					require.True(t, types.T(arg.Typ.Id).IsDecimal(), "type id %d", arg.Typ.Id)
				} else {
					require.Equal(t, int32(types.T_float64), arg.Typ.Id)
				}
			}
		})
	}
}

func TestDecimalStringLiteralNormalizationSupportsDecimal256(t *testing.T) {
	ctx := context.Background()
	decimalType := types.New(types.T_decimal256, 40, 4)
	literal := makePlan2StringConstExprWithType("999999999999999999999999999999999999.0001")
	args := []*planpb.Expr{
		makePreparedDecimalComparisonColumn(decimalType),
		literal,
	}

	err := normalizeDecimalStringLiteralComparisonArgs(ctx, "<", args)
	require.NoError(t, err)
	require.NotSame(t, literal, args[1])
	require.Equal(t, int32(types.T_decimal256), args[1].Typ.Id)
	require.Equal(t, int32(4), args[1].Typ.Scale)
}

func TestDecimalStringLiteralOutOfExactDomainFallsBack(t *testing.T) {
	ctx := context.Background()
	decimalType := types.New(types.T_decimal128, 20, 4)
	for _, value := range []string{
		"1e10000",
		"1e-10000",
		"0." + strings.Repeat("0", 79) + "1",
	} {
		t.Run(value, func(t *testing.T) {
			expr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{
				makePreparedDecimalComparisonColumn(decimalType),
				makePlan2StringConstExprWithType(value),
			})
			require.NoError(t, err)
			for _, arg := range expr.GetF().Args {
				require.Equal(t, int32(types.T_float64), arg.Typ.Id)
			}
		})
	}
}

func TestDecimalStringLiteralCanonicalizesRedundantZeros(t *testing.T) {
	ctx := context.Background()
	decimalType := types.New(types.T_decimal128, 20, 4)
	for _, value := range []string{
		"0e10000",
		"0e-10000",
		strings.Repeat("0", 80) + "1",
		"1.",
		"0." + strings.Repeat("0", 80),
	} {
		t.Run(value, func(t *testing.T) {
			expr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{
				makePreparedDecimalComparisonColumn(decimalType),
				makePlan2StringConstExprWithType(value),
			})
			require.NoError(t, err)
			for _, arg := range expr.GetF().Args {
				require.True(t, types.T(arg.Typ.Id).IsDecimal(), "type id %d", arg.Typ.Id)
			}
		})
	}
}

func TestFoldableDecimalStringExpressionUsesExactComparison(t *testing.T) {
	compilerCtx := NewMockCompilerContext(true)
	lower, err := bindFuncExprAndConstFold(
		compilerCtx.GetContext(),
		compilerCtx.GetProcess(),
		"lower",
		[]*planpb.Expr{makePlan2StringConstExprWithType("9007199254740992.0001")},
	)
	require.NoError(t, err)
	require.Nil(t, lower.GetLit(), "control: LOWER is not folded by the generic binder path")

	expr, err := bindFuncExprAndConstFold(
		compilerCtx.GetContext(),
		compilerCtx.GetProcess(),
		"=",
		[]*planpb.Expr{
			makePreparedDecimalComparisonColumn(types.New(types.T_decimal128, 20, 4)),
			lower,
		},
	)
	require.NoError(t, err)
	for _, arg := range expr.GetF().Args {
		require.True(t, types.T(arg.Typ.Id).IsDecimal(), "type id %d", arg.Typ.Id)
	}
}

func TestSingleInFoldableDecimalStringExpressionUsesExactComparison(t *testing.T) {
	compilerCtx := NewMockCompilerContext(true)
	decimalType := types.New(types.T_decimal128, 20, 4)
	for _, stringLeft := range []bool{false, true} {
		for _, name := range []string{"in", "not_in"} {
			t.Run(fmt.Sprintf("%s/string_left=%t", name, stringLeft), func(t *testing.T) {
				lower, err := bindFuncExprAndConstFold(compilerCtx.GetContext(), compilerCtx.GetProcess(), "lower",
					[]*planpb.Expr{makePlan2StringConstExprWithType("9007199254740992.0001")})
				require.NoError(t, err)
				decimal := makePreparedDecimalComparisonColumn(decimalType)
				left, right := decimal, lower
				if stringLeft {
					left, right = lower, decimal
				}
				expr, err := bindFuncExprAndConstFold(compilerCtx.GetContext(), compilerCtx.GetProcess(), name,
					[]*planpb.Expr{left, {Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{right}}}}})
				require.NoError(t, err)
				require.NotNil(t, expr.GetF())
				for _, arg := range expr.GetF().Args {
					require.True(t, types.T(arg.Typ.Id).IsDecimal(), "type id %d", arg.Typ.Id)
				}
			})
		}
	}
}
