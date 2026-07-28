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
		name string
		expr *Expr
	}{
		{name: "column", expr: column},
		{name: "expression", expr: expression},
		{name: "null", expr: makePlan2NullConstExprWithType()},
		{name: "negative", expr: makePlan2Int64ConstExprWithType(-1)},
		{name: "above maximum", expr: makePlan2Int64ConstExprWithType(7)},
	}

	for _, function := range []string{"utc_time", "utc_timestamp"} {
		for _, test := range invalidArgs {
			t.Run(function+"/"+test.name, func(t *testing.T) {
				expr, err := BindFuncExprImplByPlanExpr(ctx, function, []*Expr{test.expr})
				require.Nil(t, expr)
				require.ErrorContains(t, err, "integer literal between 0 and 6")
			})
		}
	}
}
