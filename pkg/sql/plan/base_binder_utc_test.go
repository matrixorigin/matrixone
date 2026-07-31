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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestBindUTCFunctionReturnTypeScale(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name     string
		function string
		oid      types.T
		fsp      int64
	}{
		{name: "utc time scale zero", function: "utc_time", oid: types.T_time, fsp: 0},
		{name: "utc time scale three", function: "utc_time", oid: types.T_time, fsp: 3},
		{name: "utc time scale six", function: "utc_time", oid: types.T_time, fsp: 6},
		{name: "utc timestamp scale zero", function: "utc_timestamp", oid: types.T_datetime, fsp: 0},
		{name: "utc timestamp scale three", function: "utc_timestamp", oid: types.T_datetime, fsp: 3},
		{name: "utc timestamp scale six", function: "utc_timestamp", oid: types.T_datetime, fsp: 6},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expr, err := BindFuncExprImplByPlanExpr(ctx, test.function, []*Expr{makePlan2Int64ConstExprWithType(test.fsp)})
			require.NoError(t, err)
			require.Equal(t, int32(test.oid), expr.Typ.Id)
			require.Equal(t, int32(test.fsp), expr.Typ.Width)
			require.Equal(t, int32(test.fsp), expr.Typ.Scale)
		})
	}

	for _, test := range []struct {
		name     string
		function string
		oid      types.T
	}{
		{name: "utc time default scale", function: "utc_time", oid: types.T_time},
		{name: "utc timestamp default scale", function: "utc_timestamp", oid: types.T_datetime},
	} {
		t.Run(test.name, func(t *testing.T) {
			expr, err := BindFuncExprImplByPlanExpr(ctx, test.function, nil)
			require.NoError(t, err)
			require.Equal(t, int32(test.oid), expr.Typ.Id)
			require.Zero(t, expr.Typ.Width)
			require.Zero(t, expr.Typ.Scale)
		})
	}
}

func TestBindUTCFunctionRejectsInvalidFractionalSecondPrecision(t *testing.T) {
	ctx := context.Background()
	column := &Expr{
		Typ: Type{Id: int32(types.T_int64)},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			RelPos: 0,
			ColPos: 0,
		}},
	}
	expression, err := BindFuncExprImplByPlanExpr(ctx, "+", []*Expr{
		makePlan2Int64ConstExprWithType(1),
		makePlan2Int64ConstExprWithType(2),
	})
	require.NoError(t, err)

	invalidArgs := []struct {
		name     string
		expr     *Expr
		contains string
	}{
		{name: "column", expr: column, contains: "integer literal between 0 and 6"},
		{name: "expression", expr: expression, contains: "integer literal between 0 and 6"},
		{name: "null", expr: makePlan2NullConstExprWithType(), contains: "integer literal between 0 and 6"},
		{name: "negative", expr: makePlan2Int64ConstExprWithType(-1), contains: "negative precision -1 specified"},
		{name: "above maximum", expr: makePlan2Int64ConstExprWithType(7), contains: "Too-big precision 7 specified"},
		{name: "above int32 maximum", expr: makePlan2Int64ConstExprWithType(2147483648), contains: "Too-big precision 2147483648 specified"},
	}

	for _, function := range []string{"utc_time", "utc_timestamp"} {
		for _, test := range invalidArgs {
			t.Run(function+"/"+test.name, func(t *testing.T) {
				expr, err := BindFuncExprImplByPlanExpr(ctx, function, []*Expr{test.expr})
				require.Nil(t, expr)
				require.ErrorContains(t, err, test.contains)
			})
		}
	}
}

func TestBuildUTCFunctionReturnTypePrecision(t *testing.T) {
	tests := []struct {
		function string
		oid      types.T
	}{
		{function: "utc_time", oid: types.T_time},
		{function: "utc_timestamp", oid: types.T_datetime},
	}

	for _, test := range tests {
		t.Run(test.function, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
				"select "+test.function+"(0), "+test.function+"(3), "+test.function+"(6)", 1)
			require.NoError(t, err)

			pl, err := BuildPlan(NewMockCompilerContext(true), stmt, false)
			require.NoError(t, err)

			var results []*planpb.Expr
			for _, node := range pl.GetQuery().Nodes {
				for _, expr := range node.ProjectList {
					if expr.GetF() != nil && expr.GetF().GetFunc().GetObjName() == test.function {
						results = append(results, expr)
					}
				}
			}
			require.Len(t, results, 3)
			for fsp, result := range results {
				require.Equal(t, int32(test.oid), result.Typ.Id)
				require.Equal(t, int32(fsp*3), result.Typ.Width)
				require.Equal(t, int32(fsp*3), result.Typ.Scale)
			}
		})
	}
}
