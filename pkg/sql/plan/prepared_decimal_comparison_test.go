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

func TestDecimalStringComparisonsKeepMySQLCoercion(t *testing.T) {
	ctx := context.Background()
	decimalType := types.New(types.T_decimal128, 20, 4)

	for _, tc := range []struct {
		name  string
		right *planpb.Expr
	}{
		{name: "string literal", right: makePlan2StringConstExprWithType("9007199254740992.0001")},
		{name: "string column", right: &planpb.Expr{
			Typ:  planpb.Type{Id: int32(types.T_varchar)},
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 1}},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr, err := BindFuncExprImplByPlanExpr(ctx, "<=>", []*planpb.Expr{
				makePreparedDecimalComparisonColumn(decimalType),
				DeepCopyExpr(tc.right),
			})
			require.NoError(t, err)
			for _, arg := range expr.GetF().Args {
				require.Equal(t, int32(types.T_float64), arg.Typ.Id)
			}
		})
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
			require.Equal(t, int32(decimalType.Oid), arg.Typ.Id)
			require.Equal(t, decimalType.Width, arg.Typ.Width)
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

func TestPreparedDecimalCommonTypeFunctionsDeriveParamType(t *testing.T) {
	ctx := context.Background()
	decimalTypes := []types.Type{
		types.New(types.T_decimal64, 18, 2),
		types.New(types.T_decimal128, 20, 4),
	}

	for _, decimalType := range decimalTypes {
		for _, name := range []string{"coalesce", "greatest", "least"} {
			for _, paramPos := range []int{0, 1} {
				t.Run(fmt.Sprintf("%s/%s/param_pos=%d", name, decimalType.Oid, paramPos), func(t *testing.T) {
					args := []*planpb.Expr{
						makePreparedDecimalComparisonParam(0),
						makePreparedDecimalComparisonColumn(decimalType),
					}
					if paramPos == 1 {
						args[0], args[1] = args[1], args[0]
					}

					expr, err := BindFuncExprImplByPlanExpr(ctx, name, args)
					require.NoError(t, err)
					require.Equal(t, int32(decimalType.Oid), expr.Typ.Id)
					require.Equal(t, decimalType.Width, expr.Typ.Width)
					require.Equal(t, decimalType.Scale, expr.Typ.Scale)
					require.Len(t, expr.GetF().Args, 2)
					for _, arg := range expr.GetF().Args {
						require.Equal(t, int32(decimalType.Oid), arg.Typ.Id)
						require.Equal(t, decimalType.Width, arg.Typ.Width)
						require.Equal(t, decimalType.Scale, arg.Typ.Scale)
					}
					require.NotNil(t, expr.GetF().Args[paramPos].GetF())
					require.NotNil(t, expr.GetF().Args[paramPos].GetF().Args[0].GetP())
				})
			}
		}
	}
}

func TestPreparedDecimalCommonTypeFunctionsUseAllNumericPeers(t *testing.T) {
	ctx := context.Background()
	decimalIntegral := types.New(types.T_decimal128, 38, 0)
	decimalFractional := types.New(types.T_decimal128, 38, 38)

	for _, name := range []string{"coalesce", "greatest", "least"} {
		t.Run(name, func(t *testing.T) {
			expr, err := BindFuncExprImplByPlanExpr(ctx, name, []*planpb.Expr{
				makePreparedDecimalComparisonParam(0),
				makePreparedDecimalComparisonColumn(decimalIntegral),
				makePreparedDecimalComparisonColumn(decimalFractional),
				makePlan2Int64ConstExprWithType(1),
			})
			require.NoError(t, err)
			require.Equal(t, int32(types.T_decimal256), expr.Typ.Id)
			require.Equal(t, int32(76), expr.Typ.Width)
			require.Equal(t, int32(38), expr.Typ.Scale)
			for _, arg := range expr.GetF().Args {
				require.Equal(t, int32(types.T_decimal256), arg.Typ.Id)
				require.Equal(t, int32(76), arg.Typ.Width)
				require.Equal(t, int32(38), arg.Typ.Scale)
			}
		})
	}
}

func TestPreparedDecimalCommonTypeFunctionsUseMySQLNumericPeers(t *testing.T) {
	ctx := context.Background()
	decimalType := types.New(types.T_decimal128, 20, 4)

	peers := []struct {
		name     string
		typ      types.Type
		wantType types.T
	}{
		{name: "float32", typ: types.T_float32.ToType(), wantType: types.T_float64},
		{name: "float64", typ: types.T_float64.ToType(), wantType: types.T_float64},
		{name: "bool", typ: types.T_bool.ToType(), wantType: types.T_decimal128},
		{name: "year", typ: types.T_year.ToType(), wantType: types.T_decimal128},
	}

	for _, name := range []string{"coalesce", "greatest", "least"} {
		for _, peer := range peers {
			t.Run(name+"/"+peer.name, func(t *testing.T) {
				expr, err := BindFuncExprImplByPlanExpr(ctx, name, []*planpb.Expr{
					makePreparedDecimalComparisonParam(0),
					makePreparedDecimalComparisonColumn(decimalType),
					makePreparedDecimalComparisonColumn(peer.typ),
				})
				require.NoError(t, err)
				require.Equal(t, int32(peer.wantType), expr.Typ.Id)
				for _, arg := range expr.GetF().Args {
					require.Equal(t, int32(peer.wantType), arg.Typ.Id)
					if peer.wantType.IsDecimal() {
						require.Equal(t, expr.Typ.Width, arg.Typ.Width)
						require.Equal(t, expr.Typ.Scale, arg.Typ.Scale)
					}
				}
				if peer.typ.Oid == types.T_bool {
					boolCast := expr.GetF().Args[2].GetF()
					require.NotNil(t, boolCast)
					require.Equal(t, "cast", boolCast.Func.GetObjName())
					require.Len(t, boolCast.Args, 2)
					require.Equal(t, int32(types.T_uint8), boolCast.Args[0].Typ.Id)
					integerCast := boolCast.Args[0].GetF()
					require.NotNil(t, integerCast)
					require.Equal(t, "cast", integerCast.Func.GetObjName())
					require.Len(t, integerCast.Args, 2)
					require.Equal(t, int32(types.T_bool), integerCast.Args[0].Typ.Id)
				}
			})
		}
	}
}

func TestPreparedDecimalCommonTypeFunctionsKeepStringBoundaries(t *testing.T) {
	ctx := context.Background()
	decimalType := types.New(types.T_decimal128, 20, 4)
	stringPeers := []struct {
		name string
		expr *planpb.Expr
	}{
		{name: "real_string", expr: makePlan2StringConstExprWithType("9007199254740992.0001")},
		{name: "enum", expr: makePreparedDecimalComparisonColumn(types.T_enum.ToType())},
	}

	for _, name := range []string{"coalesce", "greatest", "least"} {
		for _, peer := range stringPeers {
			t.Run(name+"/"+peer.name, func(t *testing.T) {
				expr, err := BindFuncExprImplByPlanExpr(ctx, name, []*planpb.Expr{
					makePreparedDecimalComparisonParam(0),
					makePreparedDecimalComparisonColumn(decimalType),
					DeepCopyExpr(peer.expr),
				})
				require.NoError(t, err)
				require.True(t, types.T(expr.Typ.Id).IsMySQLString())
			})
		}
	}
}

func TestPreparedDecimalCommonTypePlannerReplacementAndReuse(t *testing.T) {
	mock := NewMockOptimizer(false)
	decimalType := types.New(types.T_decimal128, 20, 4)
	mock.ctxt.tables["part"].Cols[7].Typ = makePlan2Type(&decimalType)

	logicPlan, err := runOneStmt(
		mock,
		t,
		"prepare decimal_common from 'select p_partkey from part where coalesce(?, p_retailprice) = p_retailprice'",
	)
	require.NoError(t, err)
	prepare := logicPlan.GetDcl().GetPrepare()
	require.NotNil(t, prepare)
	original := findPreparedDecimalComparisonInPlan(prepare.Plan, "coalesce")
	require.NotNil(t, original)
	requirePreparedDecimalComparisonArgs(t, original, decimalType, 0)

	for _, value := range []any{
		nil,
		"9007199254740992.0001",
		nil,
		"9007199254740993.0001",
	} {
		filled, err := FillValuesOfParamsInPlan(context.Background(), prepare.Plan, []any{value})
		require.NoError(t, err)
		require.NotSame(t, prepare.Plan, filled)

		coalesce := findPreparedDecimalComparisonInPlan(filled, "coalesce")
		require.NotNil(t, coalesce)
		for _, arg := range coalesce.GetF().Args {
			require.Equal(t, int32(decimalType.Oid), arg.Typ.Id)
			require.Equal(t, decimalType.Width, arg.Typ.Width)
			require.Equal(t, decimalType.Scale, arg.Typ.Scale)
		}
		require.False(t, planExprContainsPreparedDecimalParam(coalesce))
		require.True(t, planExprContainsPreparedDecimalParam(original))
	}
}
