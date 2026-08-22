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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
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

func TestMixedStringNumericInBindsNumericComparisonsAsFloat64(t *testing.T) {
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
	require.Equal(t, 2, float64Comparisons)
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
	require.Equal(t, 2, float64Comparisons)

	folded, err := ConstantFold(batch.EmptyForConstFoldBatch, expr, ctx.GetProcess(), false, true)
	require.NoError(t, err)
	result, ok := folded.GetLit().Value.(*planpb.Literal_Bval)
	require.True(t, ok)
	require.False(t, result.Bval)
}

func TestPromotedPadSpaceStringInUsesCanonicalKey(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	for _, tc := range []struct {
		name string
		fn   string
	}{
		{name: "in", fn: "in"},
		{name: "not in", fn: "not_in"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			left := makePlan2StringConstExprWithType("MO      ")
			left.Typ.PadSpace = true
			right := &planpb.Expr{
				Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{
					makePlan2StringConstExprWithType("MO"),
					makePlan2StringConstExprWithType("XX"),
				}}},
			}

			expr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), tc.fn, []*planpb.Expr{left, right})
			require.NoError(t, err)
			inFunction := expr.GetF()
			require.NotNil(t, inFunction)
			require.Equal(t, tc.fn, inFunction.Func.ObjName)
			leftCast := inFunction.Args[0].GetF()
			require.NotNil(t, leftCast)
			require.Equal(t, "cast", leftCast.Func.ObjName)
			_, overloadID := function.DecodeOverloadID(leftCast.Func.Obj)
			require.Equal(t, int32(2), overloadID)
		})
	}
}

func TestPromotedPadSpaceComparisonBuiltinsUseCanonicalArguments(t *testing.T) {
	value := "coalesce(cast(n_name as char(8)), cast(n_comment as varchar(8)))"
	for _, tc := range []struct {
		name string
		sql  string
		fn   string
	}{
		{name: "strcmp", sql: "select strcmp(" + value + ", 'MO') from nation", fn: "strcmp"},
		{name: "field", sql: "select field(" + value + ", 'MO', 'XX') from nation", fn: "field"},
		{name: "least", sql: "select least(" + value + ", 'MO') from nation", fn: "least"},
		{name: "greatest", sql: "select greatest(" + value + ", 'MO') from nation", fn: "greatest"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(NewMockOptimizer(true), t, tc.sql)
			require.NoError(t, err)

			var found bool
			var visit func(*planpb.Expr)
			visit = func(expr *planpb.Expr) {
				if expr == nil {
					return
				}
				fn := expr.GetF()
				if fn == nil {
					return
				}
				if fn.Func.ObjName == tc.fn {
					for _, arg := range fn.Args {
						found = found || isCastOverload(arg, 2)
					}
				}
				for _, arg := range fn.Args {
					visit(arg)
				}
			}
			for _, node := range logicPlan.GetQuery().Nodes {
				for _, projection := range node.ProjectList {
					visit(projection)
				}
			}
			require.True(t, found)
		})
	}
}

func TestNumericInStringLiteralKeepsExactNumericComparison(t *testing.T) {
	for _, tc := range []struct {
		name     string
		column   types.T
		value    string
		expected types.T
	}{
		{
			name:     "int64",
			column:   types.T_int64,
			value:    "9223372036854775806",
			expected: types.T_int64,
		},
		{
			name:     "decimal256",
			column:   types.T_decimal256,
			value:    "9999999999999999999999999999999999999998",
			expected: types.T_decimal256,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			left := &planpb.Expr{
				Typ:  planpb.Type{Id: int32(tc.column)},
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
