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

func TestDecimalStringLiteralComparisonsUseExactDecimalTypes(t *testing.T) {
	ctx := context.Background()
	decimalTypes := []struct {
		typ   types.Type
		value string
	}{
		{typ: types.New(types.T_decimal64, 18, 2), value: "9007199254740992.01"},
		{typ: types.New(types.T_decimal128, 20, 4), value: "9007199254740992.0001"},
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

func TestDecimalNonExactStringExpressionsKeepGenericCoercion(t *testing.T) {
	ctx := context.Background()
	decimalType := types.New(types.T_decimal128, 20, 4)
	controls := []struct {
		name string
		expr *planpb.Expr
	}{
		{name: "non-numeric literal", expr: makePlan2StringConstExprWithType("not-a-number")},
		{name: "null literal", expr: MakePlan2NullTextConstExprWithType("")},
		{name: "binary literal", expr: makePlan2StringConstExprWithType("9007199254740992.0001", true)},
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

func TestDecimalStringLiteralNormalizationSkipsDecimal256(t *testing.T) {
	ctx := context.Background()
	decimalType := types.New(types.T_decimal256, 40, 4)
	literal := makePlan2StringConstExprWithType("999999999999999999999999999999999999.0001")
	args := []*planpb.Expr{
		makePreparedDecimalComparisonColumn(decimalType),
		literal,
	}

	err := normalizeDecimalStringLiteralComparisonArgs(ctx, "<", args)
	require.NoError(t, err)
	require.Same(t, literal, args[1])
}
