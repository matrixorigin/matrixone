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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestPreparedScalarNumericOverloadsUseDoubleDomain(t *testing.T) {
	for _, name := range []string{"abs", "sleep"} {
		t.Run(name, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(false), t,
				fmt.Sprintf("prepare stmt_%s from 'select %s(?)'", name, name))
			require.NoError(t, err)

			fn := findPlanFunctionExpr(p.GetDcl().GetPrepare().Plan, name)
			require.NotNil(t, fn)
			require.Len(t, fn.GetF().Args, 1)
			require.Equal(t, int32(types.T_float64), fn.GetF().Args[0].Typ.Id)
			cast := fn.GetF().Args[0].GetF()
			require.NotNil(t, cast)
			require.Equal(t, "cast", cast.Func.GetObjName())
			require.NotNil(t, cast.Args[0].GetP())
			require.Equal(t, int32(types.T_float64), cast.Args[1].Typ.Id)
		})
	}

	t.Run("ordinary column keeps native overload", func(t *testing.T) {
		p, err := runOneStmt(NewMockOptimizer(false), t,
			"prepare stmt_abs_column from 'select abs(n_regionkey) from nation'")
		require.NoError(t, err)

		fn := findPlanFunctionExpr(p.GetDcl().GetPrepare().Plan, "abs")
		require.NotNil(t, fn)
		require.NotEqual(t, int32(types.T_float64), fn.GetF().Args[0].Typ.Id)
	})

	t.Run("parameter nested in arithmetic", func(t *testing.T) {
		p, err := runOneStmt(NewMockOptimizer(false), t,
			"prepare stmt_abs_expr from 'select abs(? + 0)'")
		require.NoError(t, err)

		fn := findPlanFunctionExpr(p.GetDcl().GetPrepare().Plan, "abs")
		require.NotNil(t, fn)
		require.Equal(t, int32(types.T_float64), fn.GetF().Args[0].Typ.Id)
	})
}

func TestContainsPreparedParamExprVariants(t *testing.T) {
	param := tree.NewParamExpr(0)
	literal := tree.NewNumVal(int64(1), "1", false, tree.P_int64)

	tests := []struct {
		name string
		expr tree.Expr
		want bool
	}{
		{name: "parameter", expr: param, want: true},
		{name: "binary", expr: tree.NewBinaryExpr(tree.PLUS, literal, param), want: true},
		{name: "unary", expr: tree.NewUnaryExpr(tree.UNARY_MINUS, param), want: true},
		{name: "parenthesized", expr: tree.NewParentExpr(param), want: true},
		{name: "function", expr: &tree.FuncExpr{Exprs: tree.Exprs{param}}, want: true},
		{name: "cast", expr: tree.NewCastExpr(param, nil), want: true},
		{name: "bit cast", expr: tree.NewBitCastExpr(param, nil), want: true},
		{name: "case operand", expr: tree.NewCaseExpr(param, nil, literal), want: false},
		{name: "case when condition", expr: tree.NewCaseExpr(nil, []*tree.When{tree.NewWhen(param, literal)}, nil), want: false},
		{name: "case when value", expr: tree.NewCaseExpr(nil, []*tree.When{tree.NewWhen(literal, param)}, nil), want: true},
		{name: "case else", expr: tree.NewCaseExpr(nil, nil, param), want: true},
		{name: "tuple", expr: &tree.Tuple{Exprs: tree.Exprs{literal, param}}, want: true},
		{name: "literal", expr: literal, want: false},
		{name: "empty case", expr: tree.NewCaseExpr(nil, nil, nil), want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, containsPreparedParamExpr(tc.expr))
		})
	}

	require.True(t, containsPreparedParamExprs(tree.Exprs{literal, param}))
	require.False(t, containsPreparedParamExprs(tree.Exprs{literal}))
}

func TestPreparedScalarNumericOverloadsCoverSubqueryAndExactInteger(t *testing.T) {
	ctx := context.Background()

	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_abs_exact from 'select abs(?)'")
	require.NoError(t, err)
	queryPlan := prepared.GetDcl().GetPrepare().Plan
	filled, err := FillValuesOfParamsInPlan(ctx, queryPlan, []any{
		ParamValue{Value: "-9007199254740993", PrepareParamKind: vector.PrepareParamInteger},
	})
	require.NoError(t, err)
	fn := findPlanFunctionExpr(filled, "abs")
	require.NotNil(t, fn)
	require.Equal(t, int32(types.T_int64), fn.Typ.Id)
	require.Equal(t, int32(types.T_int64), fn.GetF().Args[0].Typ.Id)
	require.Equal(t, int64(-9007199254740993), fn.GetF().Args[0].GetLit().GetI64Val())

	prepared, err = runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_abs_subquery from 'select abs((select ?))'")
	require.NoError(t, err)
	fn = findPlanFunctionExpr(prepared.GetDcl().GetPrepare().Plan, "abs")
	require.NotNil(t, fn)
	require.Equal(t, int32(types.T_float64), fn.GetF().Args[0].Typ.Id)

	prepared, err = runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_abs_case_condition from 'select abs(case when ? then n_regionkey else n_regionkey end) from nation'")
	require.NoError(t, err)
	fn = findPlanFunctionExpr(prepared.GetDcl().GetPrepare().Plan, "abs")
	require.NotNil(t, fn)
	// The marker only controls CASE flow. It must not force the BIGINT result
	// branches through the deferred DOUBLE overload.
	require.NotEqual(t, int32(types.T_float64), fn.GetF().Args[0].Typ.Id)

	prepared, err = runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_abs_if_condition from 'select abs(if(?, n_regionkey, n_regionkey)) from nation'")
	require.NoError(t, err)
	fn = findPlanFunctionExpr(prepared.GetDcl().GetPrepare().Plan, "abs")
	require.NotNil(t, fn)
	require.NotEqual(t, int32(types.T_float64), fn.GetF().Args[0].Typ.Id)

	prepared, err = runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_abs_explicit_double from 'select abs(cast(? as double))'")
	require.NoError(t, err)
	queryPlan = prepared.GetDcl().GetPrepare().Plan
	filled, err = FillValuesOfParamsInPlan(ctx, queryPlan, []any{
		ParamValue{Value: "9007199254740993", PrepareParamKind: vector.PrepareParamInteger},
	})
	require.NoError(t, err)
	fn = findPlanFunctionExpr(filled, "abs")
	require.NotNil(t, fn)
	// An explicit DOUBLE cast is a user-requested precision boundary and must
	// not be specialized back to an integer overload.
	require.Equal(t, int32(types.T_float64), fn.Typ.Id)
}

func TestBindFuncExprImplByPlanExpr_CaseDifferentDecimalScale(t *testing.T) {
	ctx := context.Background()

	condExpr := makePlan2BoolConstExprWithType(false)
	thenExpr := makeDecimal128ConstExpr("-58140.00", 23, 2)
	elseExpr := makeDecimal128ConstExpr("-408180.5580000", 38, 7)

	result, err := BindFuncExprImplByPlanExpr(ctx, "case", []*planpb.Expr{condExpr, thenExpr, elseExpr})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, int32(types.T_decimal128), result.Typ.Id)
	require.Equal(t, int32(38), result.Typ.Width)
	require.Equal(t, int32(7), result.Typ.Scale)

	funcExpr := result.GetF()
	require.NotNil(t, funcExpr)
	require.Len(t, funcExpr.Args, 3)

	arg1 := funcExpr.Args[1]
	require.True(t, isCastExpr(arg1), "THEN value should be cast when CASE decimal branch scales differ")
	require.Equal(t, int32(types.T_decimal128), arg1.Typ.Id)
	require.Equal(t, int32(38), arg1.Typ.Width)
	require.Equal(t, int32(7), arg1.Typ.Scale)
	require.False(t, isCastExpr(funcExpr.Args[2]), "ELSE value already has the common decimal scale")
}

func TestBuildPreparedCaseConditionParameter(t *testing.T) {
	ctx := context.Background()
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL,
		"select case when ? then v else -v end from (select 1 as v) t", 1)
	require.NoError(t, err)

	queryPlan, err := BuildPlan(NewMockCompilerContext(true), stmt, true)
	require.NoError(t, err)
	require.NoError(t, NormalizePrepareParamRefs(ctx, queryPlan))

	caseExpr := findPlanFunctionExpr(queryPlan, "case")
	require.NotNil(t, caseExpr)
	require.Len(t, caseExpr.GetF().Args, 3)
	requirePreparedCaseCondition(t, caseExpr.GetF().Args[0], true)

	for _, test := range []struct {
		name    string
		value   any
		wantNil bool
		want    string
	}{
		{name: "true", value: "1", want: "1"},
		{name: "false", value: "0", want: "0"},
		{name: "null", value: nil, wantNil: true},
		{name: "binary true", value: ParamValue{Value: "1", IsBin: true}, want: "1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			filled, err := FillValuesOfParamsInPlan(ctx, queryPlan, []any{test.value})
			require.NoError(t, err)
			filledCase := findPlanFunctionExpr(filled, "case")
			require.NotNil(t, filledCase)
			requirePreparedCaseCondition(t, filledCase.GetF().Args[0], false)

			conditionArg := filledCase.GetF().Args[0].GetF().Args[0]
			if test.wantNil {
				require.True(t, conditionArg.GetLit().GetIsnull())
			} else {
				require.Equal(t, test.want, conditionArg.GetLit().GetSval())
				require.Equal(t, test.name == "binary true", conditionArg.GetLit().GetIsBin())
			}
		})
	}
}

func requirePreparedCaseCondition(t *testing.T, condition *planpb.Expr, hasParam bool) {
	t.Helper()
	require.Equal(t, int32(types.T_bool), condition.Typ.Id)
	cast := condition.GetF()
	require.NotNil(t, cast)
	require.Equal(t, "cast", cast.Func.GetObjName())
	require.NotEmpty(t, cast.Args)
	if hasParam {
		require.NotNil(t, cast.Args[0].GetP())
		require.Equal(t, int32(0), cast.Args[0].GetP().Pos)
	} else {
		require.NotNil(t, cast.Args[0].GetLit())
	}
}

func findPlanFunctionExpr(queryPlan *planpb.Plan, name string) *planpb.Expr {
	var find func(*planpb.Expr) *planpb.Expr
	find = func(expr *planpb.Expr) *planpb.Expr {
		if expr == nil {
			return nil
		}
		if fn := expr.GetF(); fn != nil {
			if fn.Func.GetObjName() == name {
				return expr
			}
			for _, arg := range fn.Args {
				if found := find(arg); found != nil {
					return found
				}
			}
		}
		if list := expr.GetList(); list != nil {
			for _, arg := range list.List {
				if found := find(arg); found != nil {
					return found
				}
			}
		}
		return nil
	}

	if query := queryPlan.GetQuery(); query != nil {
		for _, node := range query.Nodes {
			for _, exprs := range [][]*planpb.Expr{
				node.ProjectList,
				node.FilterList,
				node.OnList,
				node.AggList,
				node.GroupBy,
			} {
				for _, expr := range exprs {
					if found := find(expr); found != nil {
						return found
					}
				}
			}
		}
	}
	return nil
}
