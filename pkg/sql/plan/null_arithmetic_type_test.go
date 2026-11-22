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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func nullArithmeticOperand() *planpb.Expr {
	return &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_any)},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}},
	}
}

func TestNullDivisionUsesDecimalResultMetadata(t *testing.T) {
	ctx := function.WithDivPrecisionIncrement(context.Background(), 10)
	decimal := types.New(types.T_decimal64, 10, 2)
	column := &planpb.Expr{Typ: decimal.PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
	for _, args := range [][]*planpb.Expr{
		{nullArithmeticOperand(), column},
		{column, nullArithmeticOperand()},
	} {
		expr, err := BindFuncExprImplByPlanExpr(ctx, "/", args)
		require.NoError(t, err)
		require.True(t, expr.GetLit().GetIsnull())
		require.Equal(t, types.T_decimal128, types.T(expr.Typ.Id))
		require.Equal(t, int32(22), expr.Typ.Width)
		require.Equal(t, int32(12), expr.Typ.Scale)
		require.False(t, expr.Typ.NotNullable)
	}
}

func TestNullArithmeticKeepsRebindableLiteral(t *testing.T) {
	value := nullArithmeticOperand()
	value.GetLit().Src = &planpb.Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
	expr, err := BindFuncExprImplByPlanExpr(context.Background(), "+", []*planpb.Expr{value, intArithmeticOperand()})
	require.NoError(t, err)
	require.NotNil(t, expr.GetF(), "a runtime-owned value cannot discard its peer at binding")
}

func TestNullArithmeticUnsupportedPeerCompatibility(t *testing.T) {
	ctx := context.Background()
	uuid := &planpb.Expr{Typ: types.T_uuid.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
	bare, err := BindFuncExprImplByPlanExpr(ctx, "+", []*planpb.Expr{nullArithmeticOperand(), uuid})
	require.NoError(t, err)
	require.True(t, bare.GetLit().GetIsnull())
	require.Equal(t, types.T_any, types.T(bare.Typ.Id))
	typed := nullArithmeticOperand()
	typed.Typ = types.T_int64.ToType().PlanType()
	_, err = BindFuncExprImplByPlanExpr(ctx, "+", []*planpb.Expr{typed, uuid})
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidArg), "%v", err)
}

func intArithmeticOperand() *planpb.Expr {
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_int64)},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
			Value: &planpb.Literal_I64Val{I64Val: 1},
		}},
	}
}

func TestBinaryArithmeticNullLiteralsPreserveResolvedType(t *testing.T) {
	ctx := context.Background()
	for _, op := range []struct {
		name string
		want types.T
	}{
		{"+", types.T_int64}, {"-", types.T_int64}, {"*", types.T_int64},
		{"/", types.T_float64}, {"%", types.T_int64},
	} {
		for _, tc := range []struct {
			name string
			args []*planpb.Expr
		}{
			{
				name: "null_left",
				args: []*planpb.Expr{nullArithmeticOperand(), intArithmeticOperand()},
			},
			{
				name: "null_right",
				args: []*planpb.Expr{intArithmeticOperand(), nullArithmeticOperand()},
			},
			{
				name: "null_both",
				args: []*planpb.Expr{nullArithmeticOperand(), nullArithmeticOperand()},
			},
		} {
			t.Run(op.name+"/"+tc.name, func(t *testing.T) {
				expr, err := BindFuncExprImplByPlanExpr(ctx, op.name, tc.args)
				require.NoError(t, err)
				require.NotNil(t, expr)
				require.NotNil(t, expr.GetLit(), "strict NULL arithmetic must still fold away its executable wrapper")
				require.True(t, expr.GetLit().GetIsnull())
				require.Equal(t, op.want, types.T(expr.Typ.Id))
				require.False(t, expr.Typ.NotNullable)
			})
		}
	}
}
