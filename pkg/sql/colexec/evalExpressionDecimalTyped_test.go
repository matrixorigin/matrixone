// Copyright 2021 Matrix Origin
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

package colexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestDecimalTypedBinaryAggregate(t *testing.T) {
	for _, tc := range []struct{ op, want string }{{"+", "3.00"}, {"-", "1.00"}, {"*", "2.0000"}} {
		t.Run(tc.op, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			t.Cleanup(proc.Free)
			typ := types.New(types.T_decimal64, 15, 2)
			col := func(i int32) *plan.Expr {
				return &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: i}}}
			}
			a := bindTestCast(t, proc, 0, col(0), types.New(types.T_decimal128, 15, 2))
			b := bindTestCast(t, proc, 0, col(1), types.New(types.T_decimal128, 15, 2))
			expression := bindTestFunction(t, proc, tc.op, a, b)
			ex, err := newTestDecimalAggregateExecutor(proc, expression)
			require.NoError(t, err)
			t.Cleanup(ex.Free)
			typed, ok := ex.(*decimalTypedExecutor)
			require.True(t, ok)
			input := batch.NewWithSize(2)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			for i, value := range []types.Decimal64{200, 100} {
				input.Vecs[i] = vector.NewVec(typ)
				require.NoError(t, vector.AppendFixed(input.Vecs[i], value, false, proc.Mp()))
			}
			input.SetRowCount(1)
			for iteration := 0; iteration < 2; iteration++ {
				out, err := ex.Eval(proc, []*batch.Batch{input}, nil)
				require.NoError(t, err)
				var text string
				if out.GetType().Oid == types.T_decimal64 {
					text = vector.GetFixedAtNoTypeCheck[types.Decimal64](out, 0).Format(out.GetType().Scale)
				} else {
					text = vector.GetFixedAtNoTypeCheck[types.Decimal128](out, 0).Format(out.GetType().Scale)
				}
				require.Equal(t, tc.want, text)
				require.Equal(t, typed.root.resultType, *out.GetType())
				ex.ResetForNextQuery()
			}
			input.Vecs[1].GetNulls().Add(0)
			out, err := ex.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.True(t, out.IsNull(0))
		})
	}
}

func TestDecimalTypedRefusesExplicitCast(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	narrow := types.New(types.T_decimal64, 15, 2)
	col := &plan.Expr{Typ: narrow.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	// Even a numerically lossless explicit cast remains an evaluation boundary.
	cast := bindTestCast(t, proc, 1, col, types.New(types.T_decimal128, 15, 2))
	cast.GetF().SyntaxExplicitCast = true
	expression := bindTestFunction(t, proc, "+", cast, cast)
	ex, err := NewExpressionExecutor(proc, expression)
	require.NoError(t, err)
	t.Cleanup(ex.Free)
	require.Nil(t, prepareDecimalTyped(expression, ex))
}

func TestDecimalTypedRetainsLegacyCastPrecisionBoundary(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	typ := types.New(types.T_decimal64, 18, 2)
	col := &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	cast := bindTestCast(t, proc, 0, col, types.New(types.T_decimal128, 18, 2))
	expression := bindTestFunction(t, proc, "+", cast, cast)
	ex, err := NewExpressionExecutor(proc, expression)
	require.NoError(t, err)
	t.Cleanup(ex.Free)
	require.Nil(t, prepareDecimalTyped(expression, ex))
	require.Nil(t, prepareDecimalChain(expression, ex))
}

func TestDecimalNativeBinaryKeepsExistingKernel(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	typ := types.New(types.T_decimal64, 15, 2)
	col := func(i int32) *plan.Expr {
		return &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: i}}}
	}
	for _, op := range []string{"+", "-", "*"} {
		expression := bindTestFunction(t, proc, op, col(0), col(1))
		generic, err := NewExpressionExecutor(proc, expression)
		require.NoError(t, err)
		t.Cleanup(generic.Free)
		require.Same(t, generic, FuseDecimalAggregateArgument(expression, generic))
	}
}
