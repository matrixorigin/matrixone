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

func nullArithmeticOperand() *planpb.Expr {
	return &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_any)},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}},
	}
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
	for _, name := range []string{"+", "-", "*", "/", "%"} {
		for _, tc := range []struct {
			name string
			args []*planpb.Expr
			typ  []types.Type
		}{
			{
				name: "null_left",
				args: []*planpb.Expr{nullArithmeticOperand(), intArithmeticOperand()},
				typ:  []types.Type{types.T_any.ToType(), types.T_int64.ToType()},
			},
			{
				name: "null_right",
				args: []*planpb.Expr{intArithmeticOperand(), nullArithmeticOperand()},
				typ:  []types.Type{types.T_int64.ToType(), types.T_any.ToType()},
			},
			{
				name: "null_both",
				args: []*planpb.Expr{nullArithmeticOperand(), nullArithmeticOperand()},
				typ:  []types.Type{types.T_any.ToType(), types.T_any.ToType()},
			},
		} {
			t.Run(name+"/"+tc.name, func(t *testing.T) {
				resolved, err := function.GetFunctionByName(ctx, name, tc.typ)
				require.NoError(t, err)

				expr, err := BindFuncExprImplByPlanExpr(ctx, name, tc.args)
				require.NoError(t, err)
				require.NotNil(t, expr)
				require.NotNil(t, expr.GetLit(), "strict NULL arithmetic must still fold away its executable wrapper")
				require.True(t, expr.GetLit().GetIsnull())
				require.Equal(t, resolved.GetReturnType().Oid, types.T(expr.Typ.Id))
				require.NotEqual(t, types.T_any, types.T(expr.Typ.Id),
					"NULL must not bypass arithmetic result-type resolution")
			})
		}
	}
}
