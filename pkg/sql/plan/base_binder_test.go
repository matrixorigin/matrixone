// Copyright 2022 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// TestBindFuncExprImplByPlanExpr_PowAlias tests that "pow" is correctly
// remapped to "power" (line ~1781 in base_binder.go:
// case "pow": name = "power").
func TestBindFuncExprImplByPlanExpr_PowAlias(t *testing.T) {
	ctx := context.Background()

	t.Run("pow with two int args", func(t *testing.T) {
		x := makeInt64ConstPlanExpr(2)
		y := makeInt64ConstPlanExpr(10)
		result, err := BindFuncExprImplByPlanExpr(ctx, "pow", []*plan.Expr{x, y})
		require.NoError(t, err)
		require.NotNil(t, result)

		f := result.GetF()
		require.NotNil(t, f, "result should be a function")
		// "pow" is remapped to "power"
		require.Equal(t, "power", f.Func.GetObjName())
	})

	t.Run("power with two int args", func(t *testing.T) {
		x := makeInt64ConstPlanExpr(3)
		y := makeInt64ConstPlanExpr(4)
		result, err := BindFuncExprImplByPlanExpr(ctx, "power", []*plan.Expr{x, y})
		require.NoError(t, err)
		require.NotNil(t, result)

		f := result.GetF()
		require.NotNil(t, f)
		require.Equal(t, "power", f.Func.GetObjName())
	})
}

// TestBindFuncExprImplByPlanExpr_JsonValid tests that json_valid binds
// correctly with string and json inputs.
func TestBindFuncExprImplByPlanExpr_JsonValid(t *testing.T) {
	ctx := context.Background()

	t.Run("json_valid with varchar literal", func(t *testing.T) {
		arg := makePlan2StringConstExprWithType(`{"a":1}`)
		result, err := BindFuncExprImplByPlanExpr(ctx, "json_valid", []*plan.Expr{arg})
		require.NoError(t, err)
		require.NotNil(t, result)

		f := result.GetF()
		require.NotNil(t, f, "should be a function expression")
		require.Equal(t, "json_valid", f.Func.GetObjName())
		require.Equal(t, 1, len(f.Args))
		require.Equal(t, int32(types.T_bool), result.Typ.Id, "return type should be bool")
	})

	t.Run("json_valid with json column ref", func(t *testing.T) {
		arg := &plan.Expr{
			Typ: plan.Type{
				Id:          int32(types.T_json),
				NotNullable: true,
			},
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{ColPos: 0, Name: "a"},
			},
		}
		result, err := BindFuncExprImplByPlanExpr(ctx, "json_valid", []*plan.Expr{arg})
		require.NoError(t, err)
		require.NotNil(t, result)

		f := result.GetF()
		require.NotNil(t, f)
		require.Equal(t, int32(types.T_bool), result.Typ.Id)
	})
}

func TestBindFuncExprImplByAstExpr_IntervalDisambiguation(t *testing.T) {
	builder, bindCtx := genBuilderAndCtx()
	whereBinder := NewWhereBinder(builder, bindCtx)

	t.Run("function style keeps interval builtin", func(t *testing.T) {
		args := []tree.Expr{
			tree.NewNumVal(int64(5), "5", false, tree.P_int64),
			tree.NewNumVal("day", "day", false, tree.P_char),
		}
		result, err := whereBinder.bindFuncExprImplByAstExpr("interval", args, 0)
		require.NoError(t, err)
		require.NotNil(t, result)

		f := result.GetF()
		require.NotNil(t, f, "interval(5, 'day') should bind to the interval builtin")
		require.Equal(t, "interval", f.Func.GetObjName())
		require.Len(t, f.Args, 2)
		require.NotEqual(t, int32(types.T_interval), result.Typ.Id)
	})

	t.Run("interval expression rewrites to interval list", func(t *testing.T) {
		args := []tree.Expr{
			tree.NewNumVal(int64(5), "5", false, tree.P_int64),
			tree.NewTimeUnitExpr("day"),
		}
		result, err := whereBinder.bindFuncExprImplByAstExpr("interval", args, 0)
		require.NoError(t, err)
		require.NotNil(t, result)

		require.Equal(t, int32(types.T_interval), result.Typ.Id)
		list := result.GetList()
		require.NotNil(t, list, "INTERVAL 5 DAY should bind as an interval expression list")
		require.Len(t, list.List, 2)
		require.Equal(t, "day", list.List[1].GetLit().GetSval())
	})
}
