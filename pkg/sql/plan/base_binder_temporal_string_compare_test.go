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
	"github.com/stretchr/testify/require"
)

func makeTemporalCompareColumn(oid types.T, scale int32, colPos int32) *planpb.Expr {
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(oid), Scale: scale},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			RelPos: 0,
			ColPos: colPos,
		}},
	}
}

func makeTemporalCompareParam(pos int32) *planpb.Expr {
	return &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_text)},
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: pos}},
	}
}

func requireTemporalCompareArgTypes(t *testing.T, expr *planpb.Expr, oid types.T, scale int32) {
	t.Helper()
	fn := expr.GetF()
	require.NotNil(t, fn)
	for _, arg := range fn.Args {
		require.Equal(t, int32(oid), arg.Typ.Id)
		require.Equal(t, scale, arg.Typ.Scale)
	}
}

func requireTimeParamCast(t *testing.T, expr *planpb.Expr, argPos int, scale int32) {
	t.Helper()
	arg := expr.GetF().Args[argPos]
	require.Equal(t, int32(types.T_time), arg.Typ.Id)
	require.Equal(t, scale, arg.Typ.Scale)

	cast := arg.GetF()
	require.NotNil(t, cast)
	require.Equal(t, "cast", cast.Func.GetObjName())
	require.Len(t, cast.Args, 2)
	require.NotNil(t, cast.Args[0].GetP())
}

func TestTimeColumnStringLiteralComparisonUsesColumnScale(t *testing.T) {
	ctx := context.Background()
	timeCol := makeTemporalCompareColumn(types.T_time, 3, 0)
	stringLit := makePlan2StringConstExprWithType("12:34:56.789456")

	expr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{timeCol, stringLit})
	require.NoError(t, err)
	requireTemporalCompareArgTypes(t, expr, types.T_time, 3)
}

func TestTimeColumnStringLiteralBetweenUsesColumnScale(t *testing.T) {
	ctx := context.Background()
	timeCol := makeTemporalCompareColumn(types.T_time, 3, 0)
	lower := makePlan2StringConstExprWithType("12:34:56.789455")
	upper := makePlan2StringConstExprWithType("12:34:56.789457")

	expr, err := BindFuncExprImplByPlanExpr(ctx, "between", []*planpb.Expr{timeCol, lower, upper})
	require.NoError(t, err)
	requireTemporalCompareArgTypes(t, expr, types.T_time, 3)
}

func TestTimeColumnDirectParamComparisonUsesColumnScale(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name      string
		operator  string
		paramLeft bool
	}{
		{name: "equal", operator: "="},
		{name: "null safe equal", operator: "<=>"},
		{name: "not equal", operator: "!="},
		{name: "less than", operator: "<"},
		{name: "less equal", operator: "<="},
		{name: "greater than", operator: ">"},
		{name: "greater equal", operator: ">="},
		{name: "param left", operator: "<", paramLeft: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			timeCol := makeTemporalCompareColumn(types.T_time, 2, 0)
			param := makeTemporalCompareParam(0)
			args := []*planpb.Expr{timeCol, param}
			paramPos := 1
			if tc.paramLeft {
				args = []*planpb.Expr{param, timeCol}
				paramPos = 0
			}

			expr, err := BindFuncExprImplByPlanExpr(ctx, tc.operator, args)
			require.NoError(t, err)
			requireTemporalCompareArgTypes(t, expr, types.T_time, 2)
			requireTimeParamCast(t, expr, paramPos, 2)
		})
	}
}

func TestTimeColumnDirectParamBetweenUsesColumnScale(t *testing.T) {
	ctx := context.Background()

	makeTimeValue := func() *planpb.Expr {
		value := makePlan2TimeConstExprWithType(0)
		value.Typ.Scale = 2
		return value
	}

	for _, tc := range []struct {
		name     string
		lower    *planpb.Expr
		upper    *planpb.Expr
		paramPos []int
	}{
		{
			name:     "two params",
			lower:    makeTemporalCompareParam(0),
			upper:    makeTemporalCompareParam(1),
			paramPos: []int{1, 2},
		},
		{
			name:     "lower param upper literal",
			lower:    makeTemporalCompareParam(0),
			upper:    makePlan2StringConstExprWithType("13:00:00.24"),
			paramPos: []int{1},
		},
		{
			name:     "lower literal upper param",
			lower:    makePlan2StringConstExprWithType("02:00:00.24"),
			upper:    makeTemporalCompareParam(0),
			paramPos: []int{2},
		},
		{
			name:     "lower time value upper param",
			lower:    makeTimeValue(),
			upper:    makeTemporalCompareParam(0),
			paramPos: []int{2},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			timeCol := makeTemporalCompareColumn(types.T_time, 2, 0)
			expr, err := BindFuncExprImplByPlanExpr(ctx, "between", []*planpb.Expr{timeCol, tc.lower, tc.upper})
			require.NoError(t, err)
			requireTemporalCompareArgTypes(t, expr, types.T_time, 2)
			for _, paramPos := range tc.paramPos {
				requireTimeParamCast(t, expr, paramPos, 2)
			}
		})
	}
}

func TestTimeColumnStringLiteralInUsesColumnScale(t *testing.T) {
	ctx := context.Background()
	timeCol := makeTemporalCompareColumn(types.T_time, 3, 0)
	rightList := &planpb.Expr{
		Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{
			makePlan2StringConstExprWithType("12:34:56.789456"),
		}}},
	}

	expr, err := BindFuncExprImplByPlanExpr(ctx, "in", []*planpb.Expr{timeCol, rightList})
	require.NoError(t, err)
	requireTemporalCompareArgTypes(t, expr, types.T_time, 3)
}

func TestScalarTimeStringComparisonKeepsStringSemantics(t *testing.T) {
	ctx := context.Background()
	timeLit := makePlan2TimeConstExprWithType(0)
	timeLit.Typ.Scale = 6
	stringLit := makePlan2StringConstExprWithType("030405.123456")

	expr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{timeLit, stringLit})
	require.NoError(t, err)
	requireTemporalCompareArgTypes(t, expr, types.T_varchar, 0)
}

func TestTimeColumnDynamicStringComparisonKeepsStringSemantics(t *testing.T) {
	ctx := context.Background()
	timeCol := makeTemporalCompareColumn(types.T_time, 6, 0)
	stringCol := makeTemporalCompareColumn(types.T_varchar, 0, 1)

	expr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{timeCol, stringCol})
	require.NoError(t, err)
	requireTemporalCompareArgTypes(t, expr, types.T_varchar, 0)
}

func TestScalarTimeDirectParamComparisonKeepsStringSemantics(t *testing.T) {
	ctx := context.Background()
	timeLit := makePlan2TimeConstExprWithType(0)
	timeLit.Typ.Scale = 6
	param := makeTemporalCompareParam(0)

	expr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{timeLit, param})
	require.NoError(t, err)
	requireTemporalCompareArgTypes(t, expr, types.T_varchar, 0)
}
