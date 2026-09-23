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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func makePreparedDecimalComparisonColumn(typ types.Type) *planpb.Expr {
	return &planpb.Expr{
		Typ: makePlan2Type(&typ),
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			RelPos: 0,
			ColPos: 0,
		}},
	}
}

func makePreparedDecimalComparisonParam(pos int32) *planpb.Expr {
	return &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_text)},
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: pos}},
	}
}

func requirePreparedDecimalComparisonArgs(
	t *testing.T,
	expr *planpb.Expr,
	want types.Type,
	paramPos int,
) {
	t.Helper()
	fn := expr.GetF()
	require.NotNil(t, fn)
	require.Len(t, fn.Args, 2)
	for _, arg := range fn.Args {
		require.Equal(t, int32(want.Oid), arg.Typ.Id)
		require.Equal(t, want.Width, arg.Typ.Width)
		require.Equal(t, want.Scale, arg.Typ.Scale)
	}

	cast := fn.Args[paramPos].GetF()
	require.NotNil(t, cast)
	require.Equal(t, "cast", cast.Func.GetObjName())
	require.Len(t, cast.Args, 2)
	require.NotNil(t, cast.Args[0].GetP())
}

func TestPreparedDecimalBinaryComparisonsDeriveParamType(t *testing.T) {
	ctx := context.Background()
	decimalTypes := []types.Type{
		types.New(types.T_decimal64, 18, 2),
		types.New(types.T_decimal128, 20, 4),
	}
	operators := []string{"=", "<=>", "<>", "<", "<=", ">", ">="}

	for _, decimalType := range decimalTypes {
		for _, operator := range operators {
			for _, paramLeft := range []bool{false, true} {
				name := fmt.Sprintf("%s/%s/param_left=%t", decimalType.Oid.String(), operator, paramLeft)
				t.Run(name, func(t *testing.T) {
					column := makePreparedDecimalComparisonColumn(decimalType)
					param := makePreparedDecimalComparisonParam(0)
					args := []*planpb.Expr{column, param}
					paramPos := 1
					if paramLeft {
						args = []*planpb.Expr{param, column}
						paramPos = 0
					}

					expr, err := BindFuncExprImplByPlanExpr(ctx, operator, args)
					require.NoError(t, err)
					requirePreparedDecimalComparisonArgs(t, expr, decimalType, paramPos)
				})
			}
		}
	}
}

func TestDecimalStringLiteralComparisonsUseExactCoercion(t *testing.T) {
	ctx := context.Background()
	decimalTypes := []types.Type{
		types.New(types.T_decimal64, 18, 2),
		types.New(types.T_decimal128, 20, 4),
		types.New(types.T_decimal256, 40, 4),
	}
	operators := []string{"=", "<=>", "!=", "<>", "<", "<=", ">", ">="}

	for _, decimalType := range decimalTypes {
		value := "9007199254740992.0001"
		if decimalType.Oid == types.T_decimal64 {
			value = "9007199254740992.01"
		}
		for _, operator := range operators {
			for _, literalLeft := range []bool{false, true} {
				name := fmt.Sprintf("%s/%s/literal_left=%t", decimalType.Oid.String(), operator, literalLeft)
				t.Run(name, func(t *testing.T) {
					args := []*planpb.Expr{
						makePreparedDecimalComparisonColumn(decimalType),
						makePlan2StringConstExprWithType(value),
					}
					if literalLeft {
						args[0], args[1] = args[1], args[0]
					}

					expr, err := BindFuncExprImplByPlanExpr(ctx, operator, args)
					require.NoError(t, err)
					require.NotNil(t, expr.GetF())
					for _, arg := range expr.GetF().Args {
						require.True(t, types.T(arg.Typ.Id).IsDecimal(), "type: %+v", arg.Typ)
						require.Equal(t, decimalType.Scale, arg.Typ.Scale)
					}
				})
			}
		}
	}
}

func TestFoldableDecimalStringComparisonUsesExactCoercion(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	decimalType := types.New(types.T_decimal128, 20, 4)
	makeConcat := func(parts ...string) *planpb.Expr {
		args := make([]*planpb.Expr, len(parts))
		for i, part := range parts {
			args[i] = makePlan2StringConstExprWithType(part)
		}
		expr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "concat", args)
		require.NoError(t, err)
		return expr
	}

	for _, stringLeft := range []bool{false, true} {
		t.Run(fmt.Sprintf("scalar/string_left=%t", stringLeft), func(t *testing.T) {
			decimal := makePreparedDecimalComparisonColumn(decimalType)
			constant := makeConcat("9007199254740992.", "0001")
			left, right := decimal, constant
			if stringLeft {
				left, right = constant, decimal
			}
			expr, err := bindFuncExprAndConstFold(
				ctx.GetContext(), ctx.GetProcess(), "=", []*planpb.Expr{left, right})
			require.NoError(t, err)
			for _, arg := range expr.GetF().Args {
				require.True(t, types.T(arg.Typ.Id).IsDecimal(), "type: %+v", arg.Typ)
			}
		})
	}

	for _, name := range []string{"in", "not_in"} {
		for _, stringLeft := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/string_left=%t", name, stringLeft), func(t *testing.T) {
				decimal := makePreparedDecimalComparisonColumn(decimalType)
				constant := makeConcat("9007199254740992.", "0001")
				left, right := decimal, constant
				if stringLeft {
					left, right = constant, decimal
				}
				expr, err := bindFuncExprAndConstFold(ctx.GetContext(), ctx.GetProcess(), name, []*planpb.Expr{
					left,
					{Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{right}}}},
				})
				require.NoError(t, err)
				operator := "="
				if name == "not_in" {
					operator = "!="
				}
				comparison := findPreparedDecimalComparisonFunction(expr, operator)
				require.NotNil(t, comparison)
				for _, arg := range comparison.GetF().Args {
					require.True(t, types.T(arg.Typ.Id).IsDecimal(), "type: %+v", arg.Typ)
				}
			})
		}
	}

	t.Run("runtime varchar", func(t *testing.T) {
		varcharColumn := &planpb.Expr{
			Typ:  planpb.Type{Id: int32(types.T_varchar)},
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 1}},
		}
		concat, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "concat", []*planpb.Expr{
			varcharColumn,
			makePlan2StringConstExprWithType(""),
		})
		require.NoError(t, err)
		expr, err := bindFuncExprAndConstFold(ctx.GetContext(), ctx.GetProcess(), "=", []*planpb.Expr{
			makePreparedDecimalComparisonColumn(decimalType),
			concat,
		})
		require.NoError(t, err)
		for _, arg := range expr.GetF().Args {
			require.Equal(t, int32(types.T_float64), arg.Typ.Id)
		}
	})

	t.Run("real-time function", func(t *testing.T) {
		realTime, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "charset", []*planpb.Expr{
			makePlan2StringConstExprWithType("100"),
		})
		require.NoError(t, err)
		expr, err := bindFuncExprAndConstFold(ctx.GetContext(), ctx.GetProcess(), "=", []*planpb.Expr{
			makePreparedDecimalComparisonColumn(decimalType),
			realTime,
		})
		require.NoError(t, err)
		for _, arg := range expr.GetF().Args {
			require.Equal(t, int32(types.T_float64), arg.Typ.Id)
		}
	})

	t.Run("folded suffix preserves runtime lexeme", func(t *testing.T) {
		expr, err := bindFuncExprAndConstFold(ctx.GetContext(), ctx.GetProcess(), "=", []*planpb.Expr{
			makePreparedDecimalComparisonColumn(decimalType),
			makeConcat("1e2", "suffix"),
		})
		require.NoError(t, err)
		for _, arg := range expr.GetF().Args {
			require.Equal(t, int32(types.T_float64), arg.Typ.Id)
		}
		value, ok := decimalComparisonStringLiteral(expr)
		require.True(t, ok)
		require.Equal(t, "1e2suffix", value)
	})
}

func TestDecimalStringLiteralComparisonPreservesNaturalScale(t *testing.T) {
	ctx := context.Background()
	expr, err := BindFuncExprImplByPlanExpr(ctx, "<", []*planpb.Expr{
		makePreparedDecimalComparisonColumn(types.New(types.T_decimal128, 20, 4)),
		makePlan2StringConstExprWithType("1.23456"),
	})
	require.NoError(t, err)
	require.NotNil(t, expr.GetF())
	require.Len(t, expr.GetF().Args, 2)
	for _, arg := range expr.GetF().Args {
		require.Equal(t, int32(5), arg.Typ.Scale)
		require.True(t, types.T(arg.Typ.Id).IsDecimal(), "type: %+v", arg.Typ)
	}
}

func TestDecimalStringLiteralPromotesPeerToDecimal256(t *testing.T) {
	ctx := context.Background()
	value := "12345678.0000000000000000000000000000001"
	expr, err := BindFuncExprImplByPlanExpr(ctx, "<", []*planpb.Expr{
		makePreparedDecimalComparisonColumn(types.New(types.T_decimal128, 38, 30)),
		makePlan2StringConstExprWithType(value),
	})
	require.NoError(t, err)
	require.NotNil(t, expr.GetF())
	for _, arg := range expr.GetF().Args {
		require.Equal(t, int32(types.T_decimal256), arg.Typ.Id, "type: %+v", arg.Typ)
		require.Equal(t, int32(31), arg.Typ.Scale)
	}
}

func TestDecimalStringSingleInUsesExactCoercion(t *testing.T) {
	ctx := context.Background()
	decimalType := types.New(types.T_decimal128, 20, 4)
	for _, name := range []string{"in", "not_in"} {
		for _, literalLeft := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/literal_left=%t", name, literalLeft), func(t *testing.T) {
				decimal := makePreparedDecimalComparisonColumn(decimalType)
				literal := makePlan2StringConstExprWithType("9007199254740992.0001")
				left, right := decimal, literal
				if literalLeft {
					left, right = literal, decimal
				}
				expr, err := BindFuncExprImplByPlanExpr(ctx, name, []*planpb.Expr{
					left,
					{Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{right}}}},
				})
				require.NoError(t, err)
				operator := "="
				if name == "not_in" {
					operator = "!="
				}
				comparison := findPreparedDecimalComparisonFunction(expr, operator)
				require.NotNil(t, comparison)
				for _, arg := range comparison.GetF().Args {
					require.True(t, types.T(arg.Typ.Id).IsDecimal(), "type: %+v", arg.Typ)
				}
			})
		}
	}
}

func TestDecimalStringMultiInKeepsRealCoercion(t *testing.T) {
	ctx := context.Background()
	decimalType := types.New(types.T_decimal128, 20, 4)
	for _, name := range []string{"in", "not_in"} {
		t.Run(name, func(t *testing.T) {
			expr, err := BindFuncExprImplByPlanExpr(ctx, name, []*planpb.Expr{
				makePreparedDecimalComparisonColumn(decimalType),
				{Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{
					makePlan2StringConstExprWithType("9007199254740992.0001"),
					makePlan2StringConstExprWithType("9007199254740992.9999"),
				}}}},
			})
			require.NoError(t, err)
			operator := "="
			if name == "not_in" {
				operator = "!="
			}
			comparison := findPreparedDecimalComparisonFunction(expr, operator)
			require.NotNil(t, comparison)
			for _, arg := range comparison.GetF().Args {
				require.Equal(t, int32(types.T_float64), arg.Typ.Id)
			}
		})
	}
}

func TestDecimalBinaryStringDomainsKeepRealCoercion(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	decimalType := types.New(types.T_decimal128, 20, 4)
	value := "9007199254740992.0001"

	binaryIntroducer := makePlan2StringConstExprWithType(value)
	binaryIntroducer.Typ.Charset = uint32(types.CharsetBinary)
	binaryIntroducer.GetLit().LiteralForm = planpb.StringLiteralForm_STRING_LITERAL_BINARY_INTRODUCER

	rawBit := makePlan2StringConstExprWithType(value, true)
	rawBit.GetLit().LiteralForm = planpb.StringLiteralForm_STRING_LITERAL_BIT

	binaryCharset := makePlan2StringConstExprWithType(value)
	binaryCharset.Typ.Charset = uint32(types.CharsetBinary)
	binaryCharset.GetLit().LiteralForm = planpb.StringLiteralForm_STRING_LITERAL_NONE

	makeBinaryCast := func(oid types.T) *planpb.Expr {
		target := types.New(oid, int32(len(value)), 0)
		expr, err := appendExplicitCastBeforeExpr(
			ctx.GetContext(), makePlan2StringConstExprWithType(value), makePlan2Type(&target))
		require.NoError(t, err)
		return expr
	}

	controls := []struct {
		name string
		expr *planpb.Expr
	}{
		{name: "binary introducer", expr: binaryIntroducer},
		{name: "raw hex", expr: makePlan2StringConstExprWithType(value, true)},
		{name: "raw bit", expr: rawBit},
		{name: "binary charset", expr: binaryCharset},
		{name: "static varbinary literal", expr: makePlan2VarBinaryConstExprWithType(value)},
		{name: "cast as binary", expr: makeBinaryCast(types.T_binary)},
		{name: "cast as varbinary", expr: makeBinaryCast(types.T_varbinary)},
		{name: "cast as blob", expr: makeBinaryCast(types.T_blob)},
	}
	for _, tc := range controls {
		t.Run(tc.name, func(t *testing.T) {
			expr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "<=>", []*planpb.Expr{
				makePreparedDecimalComparisonColumn(decimalType),
				DeepCopyExpr(tc.expr),
			})
			require.NoError(t, err)
			for _, arg := range expr.GetF().Args {
				require.Equal(t, int32(types.T_float64), arg.Typ.Id)
			}
		})
	}

	t.Run("folded binary expression", func(t *testing.T) {
		concat, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "concat", []*planpb.Expr{
			makePlan2StringConstExprWithType("9007199254740992."),
			makePlan2StringConstExprWithType("0001"),
		})
		require.NoError(t, err)
		target := types.New(types.T_varbinary, int32(len(value)), 0)
		binaryExpr, err := appendExplicitCastBeforeExpr(ctx.GetContext(), concat, makePlan2Type(&target))
		require.NoError(t, err)
		folded, err := ConstantFold(
			batch.EmptyForConstFoldBatch, DeepCopyExpr(binaryExpr), ctx.GetProcess(), false, true)
		require.NoError(t, err)
		require.NotNil(t, folded.GetLit())
		require.Equal(t, types.StringDomainBinary, types.StaticStringDomain(makeTypeByPlan2Expr(folded)))
		require.Equal(t, planpb.StringLiteralForm_STRING_LITERAL_NONE, folded.GetLit().LiteralForm)

		expr, err := bindFuncExprAndConstFold(ctx.GetContext(), ctx.GetProcess(), "<=>", []*planpb.Expr{
			makePreparedDecimalComparisonColumn(decimalType),
			binaryExpr,
		})
		require.NoError(t, err)
		for _, arg := range expr.GetF().Args {
			require.Equal(t, int32(types.T_float64), arg.Typ.Id)
		}
	})

	t.Run("text override on binary static type", func(t *testing.T) {
		textOverride := makePlan2VarBinaryConstExprWithType(value)
		textOverride.GetLit().LiteralForm = planpb.StringLiteralForm_STRING_LITERAL_TEXT
		expr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "<=>", []*planpb.Expr{
			makePreparedDecimalComparisonColumn(decimalType),
			textOverride,
		})
		require.NoError(t, err)
		for _, arg := range expr.GetF().Args {
			require.True(t, types.T(arg.Typ.Id).IsDecimal(), "type: %+v", arg.Typ)
		}
	})
}

func TestDecimalStringComparisonUsesFinalTextCastValue(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	decimalType := types.New(types.T_decimal128, 20, 4)
	const finalValue = "9007199254740992.0001"
	const sourceValue = finalValue + "x"

	for _, oid := range []types.T{types.T_char, types.T_varchar} {
		t.Run(oid.String(), func(t *testing.T) {
			target := types.New(oid, int32(len(finalValue)), 0)
			stringCast, err := appendExplicitCastBeforeExpr(
				ctx.GetContext(), makePlan2StringConstExprWithType(sourceValue), makePlan2Type(&target))
			require.NoError(t, err)

			unfolded, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "<=>", []*planpb.Expr{
				makePreparedDecimalComparisonColumn(decimalType),
				DeepCopyExpr(stringCast),
			})
			require.NoError(t, err)
			for _, arg := range unfolded.GetF().Args {
				require.Equal(t, int32(types.T_float64), arg.Typ.Id)
			}

			folded, err := ConstantFold(
				batch.EmptyForConstFoldBatch, DeepCopyExpr(stringCast), ctx.GetProcess(), false, true)
			require.NoError(t, err)
			require.NotNil(t, folded.GetLit())
			require.Equal(t, finalValue, folded.GetLit().GetSval())
			require.Equal(t, types.StringDomainText, decimalStringEffectiveDomain(folded))

			expr, err := bindFuncExprAndConstFold(ctx.GetContext(), ctx.GetProcess(), "<=>", []*planpb.Expr{
				makePreparedDecimalComparisonColumn(decimalType),
				stringCast,
			})
			require.NoError(t, err)
			for _, arg := range expr.GetF().Args {
				require.True(t, types.T(arg.Typ.Id).IsDecimal(), "type: %+v", arg.Typ)
			}
			require.True(t, containsExplicitDecimalComparisonStringCast(expr))
		})
	}

	t.Run("remaining suffix", func(t *testing.T) {
		target := types.New(types.T_varchar, int32(len(finalValue)+1), 0)
		stringCast, err := appendExplicitCastBeforeExpr(
			ctx.GetContext(), makePlan2StringConstExprWithType(finalValue+"xy"), makePlan2Type(&target))
		require.NoError(t, err)
		expr, err := bindFuncExprAndConstFold(ctx.GetContext(), ctx.GetProcess(), "<=>", []*planpb.Expr{
			makePreparedDecimalComparisonColumn(decimalType),
			stringCast,
		})
		require.NoError(t, err)
		for _, arg := range expr.GetF().Args {
			require.Equal(t, int32(types.T_float64), arg.Typ.Id)
		}
		value, ok := decimalComparisonStringLiteral(expr)
		require.True(t, ok)
		require.Equal(t, finalValue+"x", value)
	})
}

func containsExplicitDecimalComparisonStringCast(expr *planpb.Expr) bool {
	if expr == nil || expr.GetF() == nil {
		return false
	}
	if isExplicitPreparedCast(expr) && types.T(expr.Typ.Id).IsMySQLString() {
		return true
	}
	for _, arg := range expr.GetF().Args {
		if containsExplicitDecimalComparisonStringCast(arg) {
			return true
		}
	}
	return false
}

func TestDecimalStringLiteralCastUsesExactCoercion(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	target := types.T_varchar.ToType()
	target.Width = 21
	stringCast, err := appendExplicitCastBeforeExpr(
		ctx.GetContext(),
		makePlan2StringConstExprWithType("9007199254740992.0001"),
		makePlan2Type(&target),
	)
	require.NoError(t, err)

	expr, err := bindFuncExprAndConstFold(ctx.GetContext(), ctx.GetProcess(), "<=>", []*planpb.Expr{
		makePreparedDecimalComparisonColumn(types.New(types.T_decimal128, 20, 4)),
		stringCast,
	})
	require.NoError(t, err)
	for _, arg := range expr.GetF().Args {
		require.True(t, types.T(arg.Typ.Id).IsDecimal(), "type: %+v", arg.Typ)
	}
	require.True(t, containsDecimalComparisonStringCast(expr))
	require.True(t, containsExplicitDecimalComparisonStringCast(expr))
}

func containsDecimalComparisonStringCast(expr *planpb.Expr) bool {
	if expr == nil || expr.GetF() == nil {
		return false
	}
	if expr.GetF().Func.GetObjName() == "cast" && types.T(expr.Typ.Id).IsMySQLString() {
		return true
	}
	for _, arg := range expr.GetF().Args {
		if containsDecimalComparisonStringCast(arg) {
			return true
		}
	}
	return false
}

func TestDecimalStringPrefixKeepsOriginalCoercion(t *testing.T) {
	ctx := context.Background()
	for _, value := range []string{"1e2suffix", "0x10", "0x10foo", "\u00a01\u00a0"} {
		t.Run(value, func(t *testing.T) {
			expr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{
				makePreparedDecimalComparisonColumn(types.New(types.T_decimal128, 20, 4)),
				makePlan2StringConstExprWithType(value),
			})
			require.NoError(t, err)
			for _, arg := range expr.GetF().Args {
				require.Equal(t, int32(types.T_float64), arg.Typ.Id)
			}
			actual, ok := decimalComparisonStringLiteral(expr)
			require.True(t, ok)
			require.Equal(t, value, actual)
		})
	}
}

func decimalComparisonStringLiteral(expr *planpb.Expr) (string, bool) {
	if expr == nil {
		return "", false
	}
	if literal := expr.GetLit(); literal != nil {
		value, ok := literal.Value.(*planpb.Literal_Sval)
		if ok {
			return value.Sval, true
		}
	}
	if expr.GetF() != nil {
		for _, arg := range expr.GetF().Args {
			if value, ok := decimalComparisonStringLiteral(arg); ok {
				return value, true
			}
		}
	}
	return "", false
}

func TestExactDecimalStringNaturalType(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value string
		oid   types.T
		width int32
		scale int32
	}{
		{name: "scientific", value: "1e2", oid: types.T_decimal64, width: 3, scale: 0},
		{name: "leading and trailing zeros", value: "0001.20", oid: types.T_decimal64, width: 2, scale: 1},
		{name: "decimal128", value: strings.Repeat("9", 38), oid: types.T_decimal128, width: 38, scale: 0},
		{
			name:  "decimal256",
			value: "12345678.0000000000000000000000000000001",
			oid:   types.T_decimal256,
			width: 39,
			scale: 31,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr, exact, err := makePlan2ExactDecimalStringExprWithType(context.Background(), tc.value)
			require.NoError(t, err)
			require.True(t, exact)
			require.Equal(t, int32(tc.oid), expr.Typ.Id)
			require.Equal(t, tc.width, expr.Typ.Width)
			require.Equal(t, tc.scale, expr.Typ.Scale)
		})
	}
}

func TestDecimalStringLiteralOutOfExactDomainKeepsRealCoercion(t *testing.T) {
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

func TestDecimalStringLiteralWithoutCommonExactDomainKeepsRealCoercion(t *testing.T) {
	ctx := context.Background()
	value := "0." + strings.Repeat("0", 75) + "1"
	expr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{
		makePreparedDecimalComparisonColumn(types.New(types.T_decimal256, 76, 0)),
		makePlan2StringConstExprWithType(value),
	})
	require.NoError(t, err)
	for _, arg := range expr.GetF().Args {
		require.Equal(t, int32(types.T_float64), arg.Typ.Id)
	}
}

func TestDecimalRuntimeStringComparisonsKeepRealCoercion(t *testing.T) {
	ctx := context.Background()
	decimalType := types.New(types.T_decimal128, 20, 4)
	controls := []struct {
		name string
		expr *planpb.Expr
	}{
		{name: "non-numeric literal", expr: makePlan2StringConstExprWithType("not-a-number")},
		{name: "null literal", expr: MakePlan2NullTextConstExprWithType("")},
		{name: "raw binary literal", expr: makePlan2StringConstExprWithType("9007199254740992.0001", true)},
		{name: "varchar column", expr: &planpb.Expr{
			Typ:  planpb.Type{Id: int32(types.T_varchar)},
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 1}},
		}},
	}

	for _, tc := range controls {
		t.Run(tc.name, func(t *testing.T) {
			expr, err := BindFuncExprImplByPlanExpr(ctx, "<=>", []*planpb.Expr{
				makePreparedDecimalComparisonColumn(decimalType),
				DeepCopyExpr(tc.expr),
			})
			require.NoError(t, err)
			for _, arg := range expr.GetF().Args {
				require.Equal(t, int32(types.T_float64), arg.Typ.Id)
			}
		})
	}
}

func findPreparedDecimalComparisonFunction(expr *planpb.Expr, name string) *planpb.Expr {
	if expr == nil {
		return nil
	}
	if fn := expr.GetF(); fn != nil {
		if fn.Func.GetObjName() == name {
			return expr
		}
		for _, arg := range fn.Args {
			if found := findPreparedDecimalComparisonFunction(arg, name); found != nil {
				return found
			}
		}
	}
	return nil
}

func findPreparedDecimalComparisonInPlan(queryPlan *planpb.Plan, name string) *planpb.Expr {
	query := queryPlan.GetQuery()
	if query == nil {
		return nil
	}
	for _, node := range query.Nodes {
		for _, exprs := range [][]*planpb.Expr{
			node.ProjectList,
			node.FilterList,
			node.OnList,
			node.AggList,
			node.GroupBy,
		} {
			for _, expr := range exprs {
				if found := findPreparedDecimalComparisonFunction(expr, name); found != nil {
					return found
				}
			}
		}
	}
	return nil
}

func planExprContainsPreparedDecimalParam(expr *planpb.Expr) bool {
	if expr == nil {
		return false
	}
	if expr.GetP() != nil {
		return true
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if planExprContainsPreparedDecimalParam(arg) {
				return true
			}
		}
	}
	return false
}

func firstPreparedDecimalComparisonLiteral(expr *planpb.Expr) *planpb.Literal {
	if expr == nil {
		return nil
	}
	if literal := expr.GetLit(); literal != nil {
		return literal
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if literal := firstPreparedDecimalComparisonLiteral(arg); literal != nil {
				return literal
			}
		}
	}
	return nil
}

func TestPreparedDecimalComparisonPlannerReplacementAndReuse(t *testing.T) {
	mock := NewMockOptimizer(false)
	decimalType := types.New(types.T_decimal128, 20, 4)
	mock.ctxt.tables["part"].Cols[7].Typ = makePlan2Type(&decimalType)

	logicPlan, err := runOneStmt(
		mock,
		t,
		"prepare decimal_cmp from 'select p_partkey from part where p_retailprice <=> ?'",
	)
	require.NoError(t, err)
	prepare := logicPlan.GetDcl().GetPrepare()
	require.NotNil(t, prepare)
	original := findPreparedDecimalComparisonInPlan(prepare.Plan, "<=>")
	require.NotNil(t, original)
	requirePreparedDecimalComparisonArgs(t, original, decimalType, 1)

	for _, value := range []any{
		nil,
		"9007199254740992.0001",
		"9007199254740993.0001",
	} {
		filled, err := FillValuesOfParamsInPlan(context.Background(), prepare.Plan, []any{value})
		require.NoError(t, err)
		require.NotSame(t, prepare.Plan, filled)

		comparison := findPreparedDecimalComparisonInPlan(filled, "<=>")
		require.NotNil(t, comparison)
		for _, arg := range comparison.GetF().Args {
			require.Equal(t, int32(decimalType.Oid), arg.Typ.Id)
			require.Equal(t, decimalType.Width, arg.Typ.Width)
			require.Equal(t, decimalType.Scale, arg.Typ.Scale)
		}
		require.False(t, planExprContainsPreparedDecimalParam(comparison))

		literal := firstPreparedDecimalComparisonLiteral(comparison.GetF().Args[1])
		require.NotNil(t, literal)
		if value == nil {
			require.True(t, literal.Isnull)
		} else {
			require.False(t, literal.Isnull)
			require.Equal(t, value, literal.GetSval())
		}

		require.True(t, planExprContainsPreparedDecimalParam(original))
	}
}
