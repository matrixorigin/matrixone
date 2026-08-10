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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func mixedStringNumericInList(t *testing.T, ctx context.Context) *planpb.Expr {
	t.Helper()
	decimal, err := makePlan2DecimalExprWithType(ctx, "9.5")
	require.NoError(t, err)

	return &planpb.Expr{
		Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{
			makePlan2Int64ConstExprWithType(7),
			makePlan2StringConstExprWithType("8"),
			decimal,
		}}},
	}
}

func TestMixedStringNumericInKeepsDecimalComparisonExact(t *testing.T) {
	ctx := context.Background()
	expr, err := BindFuncExprImplByPlanExpr(ctx, "in", []*planpb.Expr{
		makePlan2StringConstExprWithType("9.50"), mixedStringNumericInList(t, ctx),
	})
	require.NoError(t, err)

	var equalExpressions []*planpb.Expr
	var visit func(*planpb.Expr)
	visit = func(current *planpb.Expr) {
		if function := current.GetF(); function != nil {
			if function.Func.GetObjName() == "=" {
				equalExpressions = append(equalExpressions, current)
			}
			for _, arg := range function.Args {
				visit(arg)
			}
		}
	}
	visit(expr)
	require.Len(t, equalExpressions, 3)

	float64Comparisons := 0
	for _, equalExpr := range equalExpressions {
		args := equalExpr.GetF().Args
		require.Len(t, args, 2)
		if args[0].Typ.Id == int32(types.T_float64) {
			require.Equal(t, int32(types.T_float64), args[1].Typ.Id)
			float64Comparisons++
		}
	}
	require.Equal(t, 1, float64Comparisons)
}

func TestMixedStringNumericInConstantFoldsToTrue(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	expr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "in", []*planpb.Expr{
		makePlan2StringConstExprWithType("9.50"), mixedStringNumericInList(t, ctx.GetContext()),
	})
	require.NoError(t, err)

	folded, err := ConstantFold(batch.EmptyForConstFoldBatch, expr, ctx.GetProcess(), false, true)
	require.NoError(t, err)
	result, ok := folded.GetLit().Value.(*planpb.Literal_Bval)
	require.True(t, ok)
	require.True(t, result.Bval)
}

func TestMixedStringNumericNotInBindsAndFoldsToFalse(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	expr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "not_in", []*planpb.Expr{
		makePlan2StringConstExprWithType("9.50"), mixedStringNumericInList(t, ctx.GetContext()),
	})
	require.NoError(t, err)

	var notEqualExpressions []*planpb.Expr
	var visit func(*planpb.Expr)
	visit = func(current *planpb.Expr) {
		if function := current.GetF(); function != nil {
			if function.Func.GetObjName() == "!=" {
				notEqualExpressions = append(notEqualExpressions, current)
			}
			for _, arg := range function.Args {
				visit(arg)
			}
		}
	}
	visit(expr)
	require.Len(t, notEqualExpressions, 3)

	float64Comparisons := 0
	for _, notEqualExpr := range notEqualExpressions {
		args := notEqualExpr.GetF().Args
		require.Len(t, args, 2)
		if args[0].Typ.Id == int32(types.T_float64) {
			require.Equal(t, int32(types.T_float64), args[1].Typ.Id)
			float64Comparisons++
		}
	}
	require.Equal(t, 1, float64Comparisons)

	folded, err := ConstantFold(batch.EmptyForConstFoldBatch, expr, ctx.GetProcess(), false, true)
	require.NoError(t, err)
	result, ok := folded.GetLit().Value.(*planpb.Literal_Bval)
	require.True(t, ok)
	require.False(t, result.Bval)
}

func TestDecimalStringLiteralLeftInKeepsExactComparison(t *testing.T) {
	decimalType := types.New(types.T_decimal128, 20, 4)
	for _, operator := range []struct {
		name       string
		comparison string
	}{
		{name: "in", comparison: "="},
		{name: "not_in", comparison: "!="},
	} {
		t.Run(operator.name, func(t *testing.T) {
			column := &planpb.Expr{
				Typ:  makePlan2Type(&decimalType),
				Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}},
			}
			list := &planpb.Expr{
				Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{column}}},
			}

			expr, err := BindFuncExprImplByPlanExpr(context.Background(), operator.name, []*planpb.Expr{
				makePlan2StringConstExprWithType("9007199254740992.0001"),
				list,
			})
			require.NoError(t, err)
			comparison := expr.GetF()
			require.NotNil(t, comparison)
			require.Equal(t, operator.comparison, comparison.Func.GetObjName())
			for _, arg := range comparison.Args {
				require.True(t, types.T(arg.Typ.Id).IsDecimal(), "type id %d", arg.Typ.Id)
			}
		})
	}
}

func TestNumericInStringLiteralKeepsExactNumericComparison(t *testing.T) {
	for _, tc := range []struct {
		name     string
		column   types.Type
		value    string
		expected types.T
	}{
		{
			name:     "int64",
			column:   types.T_int64.ToType(),
			value:    "9223372036854775806",
			expected: types.T_int64,
		},
		{
			name:     "decimal128",
			column:   types.New(types.T_decimal128, 20, 4),
			value:    "9007199254740992.0001",
			expected: types.T_decimal128,
		},
		{
			name:     "decimal256",
			column:   types.New(types.T_decimal256, 40, 0),
			value:    "9999999999999999999999999999999999999998",
			expected: types.T_decimal256,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			left := &planpb.Expr{
				Typ:  makePlan2Type(&tc.column),
				Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}},
			}
			rightList := &planpb.Expr{
				Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{
					makePlan2StringConstExprWithType(tc.value),
				}}},
			}

			expr, err := BindFuncExprImplByPlanExpr(context.Background(), "in", []*planpb.Expr{left, rightList})
			require.NoError(t, err)
			comparison := expr.GetF()
			require.NotNil(t, comparison)
			require.Equal(t, "=", comparison.Func.GetObjName())
			require.Len(t, comparison.Args, 2)
			require.Equal(t, int32(tc.expected), comparison.Args[0].Typ.Id)
			require.Equal(t, int32(tc.expected), comparison.Args[1].Typ.Id)
		})
	}
}
