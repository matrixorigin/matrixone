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
