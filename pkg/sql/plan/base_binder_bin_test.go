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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestBinBinaryNumericLiteralsUseUnsignedNumericPath(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name  string
		value string
		form  plan.StringLiteralForm
	}{
		{name: "hex", value: string([]byte{0x01, 0x00}), form: plan.StringLiteralForm_STRING_LITERAL_HEX},
		{name: "bit", value: string([]byte{0xff}), form: plan.StringLiteralForm_STRING_LITERAL_BIT},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			expr := makePlan2StringConstExprWithType(tc.value, true)
			expr.GetLit().LiteralForm = tc.form
			bound, err := BindFuncExprImplByPlanExpr(ctx, "bin", []*Expr{expr})
			require.NoError(t, err)
			fn := bound.GetF()
			require.NotNil(t, fn)
			require.Len(t, fn.Args, 1)
			require.Equal(t, types.T_uint64, makeTypeByPlan2Expr(fn.Args[0]).Oid)
		})
	}
}

func TestBinStringOperandsKeepPrefixPath(t *testing.T) {
	binaryIntroducer := makePlan2StringConstExprWithType(string([]byte{0x37, 0xff}), true)
	binaryIntroducer.GetLit().LiteralForm = plan.StringLiteralForm_STRING_LITERAL_BINARY_INTRODUCER
	tests := []struct {
		name string
		expr *Expr
	}{
		{name: "text", expr: makePlan2StringConstExprWithType("255")},
		{name: "binary introducer", expr: binaryIntroducer},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bound, err := BindFuncExprImplByPlanExpr(context.Background(), "bin", []*Expr{tc.expr})
			require.NoError(t, err)
			fn := bound.GetF()
			require.NotNil(t, fn)
			require.Len(t, fn.Args, 1)
			require.Equal(t, types.T_varchar, makeTypeByPlan2Expr(fn.Args[0]).Oid)
		})
	}
}

func TestConvBinaryNumericLiteralsUseBitNumericPath(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name  string
		value string
		form  plan.StringLiteralForm
	}{
		{name: "hex", value: string([]byte{0x0f}), form: plan.StringLiteralForm_STRING_LITERAL_HEX},
		{name: "bit", value: string([]byte{0x0f}), form: plan.StringLiteralForm_STRING_LITERAL_BIT},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			expr := makePlan2StringConstExprWithType(tc.value, true)
			expr.GetLit().LiteralForm = tc.form
			fromBase := makePlan2Int64ConstExprWithType(2)
			toBase := makePlan2Int64ConstExprWithType(10)
			bound, err := BindFuncExprImplByPlanExpr(ctx, "conv", []*Expr{expr, fromBase, toBase})
			require.NoError(t, err)
			fn := bound.GetF()
			require.NotNil(t, fn)
			require.Len(t, fn.Args, 3)
			require.Equal(t, types.T_bit, makeTypeByPlan2Expr(fn.Args[0]).Oid)
		})
	}
}

func TestBinaryNumericLiteralWidthBoundaries(t *testing.T) {
	ctx := context.Background()
	for _, name := range []string{"bin", "conv"} {
		t.Run(name, func(t *testing.T) {
			argsFor := func(value string) []*Expr {
				expr := makePlan2StringConstExprWithType(value, true)
				expr.GetLit().LiteralForm = plan.StringLiteralForm_STRING_LITERAL_HEX
				if name == "conv" {
					return []*Expr{
						expr,
						makePlan2Int64ConstExprWithType(2),
						makePlan2Int64ConstExprWithType(10),
					}
				}
				return []*Expr{expr}
			}

			t.Run("empty stays on string path for NULL semantics", func(t *testing.T) {
				bound, err := BindFuncExprImplByPlanExpr(ctx, name, argsFor(""))
				require.NoError(t, err)
				require.Equal(t, types.T_char, makeTypeByPlan2Expr(bound.GetF().Args[0]).Oid)
			})

			t.Run("wide value is bounded to numeric zero", func(t *testing.T) {
				bound, err := BindFuncExprImplByPlanExpr(ctx, name, argsFor(string(make([]byte, 9))))
				require.NoError(t, err)
				require.Equal(t, types.T_uint64, makeTypeByPlan2Expr(bound.GetF().Args[0]).Oid)
			})
		})
	}
}
