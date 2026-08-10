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

func TestDecimalStringColumnComparisonsKeepMySQLCoercion(t *testing.T) {
	ctx := context.Background()
	decimalType := types.New(types.T_decimal128, 20, 4)
	stringColumn := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_varchar)},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 1}},
	}

	expr, err := BindFuncExprImplByPlanExpr(ctx, "<=>", []*planpb.Expr{
		makePreparedDecimalComparisonColumn(decimalType),
		stringColumn,
	})
	require.NoError(t, err)
	for _, arg := range expr.GetF().Args {
		require.Equal(t, int32(types.T_float64), arg.Typ.Id)
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
			require.True(t, types.T(arg.Typ.Id).IsDecimal())
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

func TestPreparedDecimalComparisonUsesActualStringValueDomain(t *testing.T) {
	mock := NewMockOptimizer(false)
	decimalType := types.New(types.T_decimal128, 20, 4)
	mock.ctxt.tables["part"].Cols[7].Typ = makePlan2Type(&decimalType)

	logicPlan, err := runOneStmt(
		mock,
		t,
		"prepare decimal_cmp_value from 'select p_partkey from part where p_retailprice = ?'",
	)
	require.NoError(t, err)
	prepare := logicPlan.GetDcl().GetPrepare()
	require.NotNil(t, prepare)

	for _, tc := range []struct {
		value     string
		wantValue string
		wantScale int32
	}{
		{value: "0x10", wantValue: "0", wantScale: 0},
		{value: "1+2", wantValue: "1", wantScale: 0},
		{value: "1 2", wantValue: "1", wantScale: 0},
		{value: "9007199254740992.00014", wantValue: "9007199254740992.00014", wantScale: 5},
	} {
		t.Run(tc.value, func(t *testing.T) {
			changed, err := PlanHasExactDecimalComparisonParam(context.Background(), prepare.Plan)
			require.NoError(t, err)
			require.True(t, changed)
			filled, err := FillValuesOfParamsInPlan(context.Background(), prepare.Plan, []any{tc.value})
			require.NoError(t, err)
			comparison := findPreparedDecimalComparisonInPlan(filled, "=")
			if tc.wantScale > decimalType.Scale {
				require.Nil(t, comparison)
				return
			}
			require.NotNil(t, comparison)
			value, ok := decimalCastSourceString(comparison.GetF().Args[1])
			require.True(t, ok)
			require.Equal(t, tc.wantValue, value)
			for _, arg := range comparison.GetF().Args {
				require.True(t, types.T(arg.Typ.Id).IsDecimal())
			}
		})
	}
}
