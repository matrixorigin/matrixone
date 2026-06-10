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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
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

func TestBindUnaryMinusUint64MinInt64Boundary(t *testing.T) {
	builder, bindCtx := genBuilderAndCtx()
	whereBinder := NewWhereBinder(builder, bindCtx)

	testCases := []struct {
		name       string
		sql        string
		checkValue func(t *testing.T, expr *plan.Expr)
	}{
		{
			name: "min int64 boundary",
			sql:  "-9223372036854775808",
			checkValue: func(t *testing.T, expr *plan.Expr) {
				require.Equal(t, int32(types.T_int64), expr.Typ.Id)
				require.Equal(t, int64(math.MinInt64), expr.GetLit().GetI64Val())
			},
		},
		{
			name: "below min int64 keeps decimal",
			sql:  "-9223372036854775809",
			checkValue: func(t *testing.T, expr *plan.Expr) {
				require.Equal(t, int32(types.T_decimal128), expr.Typ.Id)
				require.NotNil(t, expr.GetLit().GetDecimal128Val())
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, "select "+tc.sql+" from bind_select", 1)
			require.NoError(t, err)

			selectStmt := stmts[0].(*tree.Select)
			selectClause := selectStmt.Select.(*tree.SelectClause)
			unaryExpr, ok := selectClause.Exprs[0].Expr.(*tree.UnaryExpr)
			require.True(t, ok)

			expr, err := whereBinder.bindUnaryExpr(unaryExpr, 0, false)
			require.NoError(t, err)
			require.NotNil(t, expr.GetLit())
			tc.checkValue(t, expr)
		})
	}
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

func TestBindNameConstConstArgs(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{
			name: "string name and int value",
			sql:  "select name_const('myname', 14)",
		},
		{
			name: "numeric name and negative value",
			sql:  "select name_const(123, -456)",
		},
		{
			name: "parenthesized literals",
			sql:  "select name_const(('myname'), (14))",
		},
		{
			name: "null value",
			sql:  "select name_const('myname', null)",
		},
		{
			name: "decimal value",
			sql:  "select name_const('myname', 12.34)",
		},
		{
			name: "negative decimal value",
			sql:  "select name_const('myname', -12.34)",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, bindNameConstSelect(tc.sql))
		})
	}
}

func TestBindNameConstInvalidArgs(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{
			name: "wrong arg count",
			sql:  "select name_const('myname')",
		},
		{
			name: "null name",
			sql:  "select name_const(null, 1)",
		},
		{
			name: "unary minus name",
			sql:  "select name_const(-123, -456)",
		},
		{
			name: "column name",
			sql:  "select name_const(a, 1) from t",
		},
		{
			name: "column value",
			sql:  "select name_const('myname', a) from t",
		},
		{
			name: "cast function value",
			sql:  "select name_const('myname', cast(14 as signed))",
		},
		{
			name: "foldable function value",
			sql:  "select name_const('myname', abs(-1))",
		},
		{
			name: "non-foldable function value",
			sql:  "select name_const('myname', now())",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, bindNameConstSelect(tc.sql))
		})
	}
}

func bindNameConstSelect(sql string) error {
	stmts, err := parsers.Parse(context.Background(), dialect.MYSQL, sql, 1)
	if err != nil {
		return err
	}
	_, err = BuildPlan(NewMockCompilerContext(true), stmts[0], false)
	return err
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
