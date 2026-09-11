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

func TestDecimalRoundingReturnMetadata(t *testing.T) {
	decimalArg := func(typ types.Type) *planpb.Expr {
		return &planpb.Expr{
			Typ:  makePlan2Type(&typ),
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
		}
	}
	digitsArg := func(value int64) *planpb.Expr {
		return makePlan2Int64ConstExprWithType(value)
	}

	for _, test := range []struct {
		name     string
		function string
		input    types.Type
		digits   *int64
		want     types.Type
	}{
		{name: "round decimal64", function: "round", input: types.New(types.T_decimal64, 10, 4), digits: ptrTo(int64(2)), want: types.New(types.T_decimal64, 9, 2)},
		{name: "round decimal128", function: "round", input: types.New(types.T_decimal128, 38, 10), digits: ptrTo(int64(5)), want: types.New(types.T_decimal128, 34, 5)},
		{name: "round decimal256", function: "round", input: types.New(types.T_decimal256, 50, 10), digits: ptrTo(int64(5)), want: types.New(types.T_decimal256, 46, 5)},
		{name: "round keeps input when digits exceeds scale", function: "round", input: types.New(types.T_decimal64, 10, 4), digits: ptrTo(int64(8)), want: types.New(types.T_decimal64, 10, 4)},
		{name: "truncate decimal64", function: "truncate", input: types.New(types.T_decimal64, 10, 4), digits: ptrTo(int64(2)), want: types.New(types.T_decimal64, 8, 2)},
		{name: "truncate to integer", function: "truncate", input: types.New(types.T_decimal64, 10, 4), digits: ptrTo(int64(0)), want: types.New(types.T_decimal64, 6, 0)},
		{name: "truncate decimal128", function: "truncate", input: types.New(types.T_decimal128, 38, 10), digits: ptrTo(int64(5)), want: types.New(types.T_decimal128, 33, 5)},
		{name: "truncate decimal256", function: "truncate", input: types.New(types.T_decimal256, 50, 10), digits: ptrTo(int64(5)), want: types.New(types.T_decimal256, 45, 5)},
		{name: "ceil decimal64", function: "ceil", input: types.New(types.T_decimal64, 10, 4), want: types.New(types.T_decimal64, 7, 0)},
		{name: "floor decimal64", function: "floor", input: types.New(types.T_decimal64, 10, 4), want: types.New(types.T_decimal64, 7, 0)},
		{name: "ceil decimal19 stays decimal", function: "ceil", input: types.New(types.T_decimal128, 19, 0), want: types.New(types.T_decimal128, 19, 0)},
		{name: "floor decimal19 stays decimal", function: "floor", input: types.New(types.T_decimal128, 19, 0), want: types.New(types.T_decimal128, 19, 0)},
		{name: "ceil with digits", function: "ceil", input: types.New(types.T_decimal64, 10, 4), digits: ptrTo(int64(2)), want: types.New(types.T_decimal64, 9, 2)},
		{name: "ceiling with digits", function: "ceiling", input: types.New(types.T_decimal64, 10, 4), digits: ptrTo(int64(2)), want: types.New(types.T_decimal64, 9, 2)},
		{name: "floor with digits", function: "floor", input: types.New(types.T_decimal64, 10, 4), digits: ptrTo(int64(2)), want: types.New(types.T_decimal64, 9, 2)},
		{name: "ceil keeps input when digits exceeds scale", function: "ceil", input: types.New(types.T_decimal64, 10, 4), digits: ptrTo(int64(8)), want: types.New(types.T_decimal64, 10, 4)},
		{name: "ceil decimal256", function: "ceil", input: types.New(types.T_decimal256, 50, 10), want: types.New(types.T_decimal256, 41, 0)},
		{name: "floor decimal256", function: "floor", input: types.New(types.T_decimal256, 50, 10), want: types.New(types.T_decimal256, 41, 0)},
	} {
		t.Run(test.name, func(t *testing.T) {
			args := []*planpb.Expr{decimalArg(test.input)}
			if test.digits != nil {
				args = append(args, digitsArg(*test.digits))
			}
			got, err := BindFuncExprImplByPlanExpr(context.Background(), test.function, args)
			require.NoError(t, err)
			require.Equal(t, makePlan2Type(&test.want), got.Typ)
		})
	}
}
