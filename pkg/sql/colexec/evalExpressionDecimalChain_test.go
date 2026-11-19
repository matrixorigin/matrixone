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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func decimalChargeTestPlan(t testing.TB, p *process.Process) *plan.Expr {
	inner := mulSubTestPlan(t, p, false)
	sub := inner.GetF().Args[1]
	literal := sub.GetF().Args[0]
	narrow := types.New(types.T_decimal64, 15, 2)
	tax := &plan.Expr{Typ: narrow.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 2}}}
	tax = bindTestCast(t, p, 0, tax, types.New(types.T_decimal128, 15, 2))
	plus := bindTestFunction(t, p, "+", literal, tax)
	a := bindTestCast(t, p, 0, inner, types.New(types.T_decimal256, inner.Typ.Width, inner.Typ.Scale))
	b := bindTestCast(t, p, 0, plus, types.New(types.T_decimal256, plus.Typ.Width, plus.Typ.Scale))
	return bindTestFunction(t, p, "*", a, b)
}
func TestDecimalChainChargeAndFallback(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	planExpr := decimalChargeTestPlan(t, proc)
	ex, err := newTestDecimalAggregateExecutor(proc, planExpr)
	require.NoError(t, err)
	t.Cleanup(ex.Free)
	chain, ok := ex.(*decimalChainExecutor)
	require.True(t, ok)
	require.Len(t, chain.nodes, 4)
	input := batch.NewWithSize(3)
	t.Cleanup(func() { input.Clean(proc.Mp()) })
	values := [][]int64{{10000, 20000, -30000}, {5, 10, 20}, {8, 8, 8}}
	for col := range values {
		input.Vecs[col] = vector.NewVec(types.New(types.T_decimal64, 15, 2))
		for row := 0; row < 257; row++ {
			require.NoError(t, vector.AppendFixed(input.Vecs[col], types.Decimal64(values[col][row%3]), false, proc.Mp()))
		}
	}
	input.SetRowCount(257)
	for generation := 0; generation < 2; generation++ {
		out, err := ex.Eval(proc, []*batch.Batch{input}, nil)
		require.NoError(t, err)
		require.Equal(t, chain.root.resultType, *out.GetType())
		got := append([]types.Decimal256(nil), vector.MustFixedColNoTypeCheck[types.Decimal256](out)...)
		expected := []string{"102.600000", "194.400000", "-259.200000"}
		for i, value := range got {
			require.Equal(t, expected[i%3], value.Format(6))
		}
		generic, err := chain.root.Eval(proc, []*batch.Batch{input}, nil)
		require.NoError(t, err)
		require.Equal(t, vector.MustFixedColNoTypeCheck[types.Decimal256](generic), got)
		for _, leaf := range chain.inputs {
			if !leaf.constant {
				require.Nil(t, leaf.value)
			}
		}
		ex.ResetForNextQuery()
		for _, node := range chain.nodes {
			require.Nil(t, node.scratch)
		}
	}
	input.Vecs[1].GetNulls().Add(256)
	out, err := ex.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.True(t, out.IsNull(256))
	for _, node := range chain.nodes {
		require.Nil(t, node.scratch)
	}
}

func BenchmarkDecimalChain(b *testing.B) {
	for _, n := range []int{256, 8192} {
		name := "caller256"
		if n == 8192 {
			name = "block8192"
		}
		for _, specialized := range []bool{false, true} {
			arm := "generic"
			if specialized {
				arm = "typed"
			}
			b.Run(name+"/"+arm, func(b *testing.B) {
				proc := testutil.NewProcess(b)
				defer proc.Free()
				expression := decimalChargeTestPlan(b, proc)
				generic, err := NewExpressionExecutor(proc, expression)
				require.NoError(b, err)
				ex := generic
				if specialized {
					ex = FuseDecimalAggregateArgument(expression, generic)
					_, ok := ex.(*decimalChainExecutor)
					require.True(b, ok)
				}
				defer ex.Free()
				input := batch.NewWithSize(3)
				defer input.Clean(proc.Mp())
				for col, value := range []types.Decimal64{123456, 5, 8} {
					input.Vecs[col] = vector.NewVec(types.New(types.T_decimal64, 15, 2))
					for row := 0; row < n; row++ {
						require.NoError(b, vector.AppendFixed(input.Vecs[col], value, false, proc.Mp()))
					}
				}
				input.SetRowCount(n)
				_, err = ex.Eval(proc, []*batch.Batch{input}, nil)
				require.NoError(b, err)
				batches := []*batch.Batch{input}
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					_, err = ex.Eval(proc, batches, nil)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func TestDecimalChainAllocationFailureAndRetry(t *testing.T) {
	for _, limit := range []uint64{4 << 10, 16 << 10} {
		t.Run(fmt.Sprintf("limit%d", limit), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			t.Cleanup(proc.Free)
			registry, err := mpool.NewAllocationAccountRegistry(1, 64)
			require.NoError(t, err)
			account, err := registry.Open(limit)
			require.NoError(t, err)
			t.Cleanup(func() {
				require.Zero(t, account.Snapshot().Used)
				account.Seal()
				_, err := registry.Finalize(account)
				require.NoError(t, err)
			})
			selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
			require.NoError(t, err)
			ex, err := newTestDecimalAggregateExecutorWithAllocation(proc, decimalChargeTestPlan(t, proc), selection)
			require.NoError(t, err)
			t.Cleanup(ex.Free)
			chain, ok := ex.(*decimalChainExecutor)
			require.True(t, ok)
			input := batch.NewWithSize(3)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			for col, value := range []types.Decimal64{10000, 5, 8} {
				input.Vecs[col] = vector.NewVec(types.New(types.T_decimal64, 15, 2))
				for i := 0; i < 8192; i++ {
					require.NoError(t, vector.AppendFixed(input.Vecs[col], value, false, proc.Mp()))
				}
			}
			input.SetRowCount(8192)
			before := account.Snapshot().Used
			_, err = ex.Eval(proc, []*batch.Batch{input}, nil)
			require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
			require.Equal(t, before, account.Snapshot().Used)
			for _, node := range chain.nodes {
				require.Nil(t, node.scratch)
			}
			for _, leaf := range chain.inputs {
				require.Nil(t, leaf.value)
			}
			for _, v := range input.Vecs {
				v.SetLength(1)
			}
			input.SetRowCount(1)
			out, err := ex.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, "102.600000", vector.GetFixedAtNoTypeCheck[types.Decimal256](out, 0).Format(6))
			require.Same(t, selection, out.AllocationAccountSelection())
			ex.ResetForNextQuery()
			require.LessOrEqual(t, account.Snapshot().Used, account.Snapshot().Limit)
		})
	}
}

func TestDecimalChainExactScaleAlignment(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	aType, bType := types.New(types.T_decimal64, 15, 1), types.New(types.T_decimal64, 15, 2)
	col := func(i int32, typ types.Type) *plan.Expr {
		return &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: i}}}
	}
	a := bindTestCast(t, proc, 0, col(0, aType), types.New(types.T_decimal128, 16, 2))
	b := bindTestCast(t, proc, 0, col(1, bType), types.New(types.T_decimal128, 15, 2))
	sum := bindTestFunction(t, proc, "+", a, b)
	literal := &plan.Expr{Typ: types.New(types.T_decimal128, 19, 0).PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Decimal128Val{Decimal128Val: &plan.Decimal128{A: 2}}}}}
	expression := bindTestFunction(t, proc, "*", sum, literal)
	ex, err := newTestDecimalAggregateExecutor(proc, expression)
	require.NoError(t, err)
	t.Cleanup(ex.Free)
	chain, ok := ex.(*decimalChainExecutor)
	require.True(t, ok)
	input := batch.NewWithSize(2)
	t.Cleanup(func() { input.Clean(proc.Mp()) })
	for col, typ := range []types.Type{aType, bType} {
		input.Vecs[col] = vector.NewVec(typ)
	}
	require.NoError(t, vector.AppendFixed(input.Vecs[0], types.Decimal64(12), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(input.Vecs[1], types.Decimal64(3), false, proc.Mp()))
	input.SetRowCount(1)
	out, err := ex.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t, "2.46", vector.GetFixedAtNoTypeCheck[types.Decimal128](out, 0).Format(2))
	require.Equal(t, types.T_decimal64, chain.nodes[0].typ.Oid)
}

func TestDecimalChainPreparationBounds(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	expression := decimalChargeTestPlan(t, proc)
	// Add a fifth arithmetic operation. Generic evaluation retains ownership.
	expression = bindTestFunction(t, proc, "+", expression, expression)
	generic, err := NewExpressionExecutor(proc, expression)
	require.NoError(t, err)
	t.Cleanup(generic.Free)
	require.Nil(t, prepareDecimalChain(expression, generic))
	// A narrower intermediate must remain an observable precision boundary.
	expression = decimalChargeTestPlan(t, proc)
	expression.GetF().Args[0].GetF().Args[0].Typ.Width = 1
	generic2, err := NewExpressionExecutor(proc, expression)
	require.NoError(t, err)
	t.Cleanup(generic2.Free)
	require.Nil(t, prepareDecimalChain(expression, generic2))
}

func TestDecimalSingleNodeSkipsWideningCast(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	expression := mulSubTestPlan(t, proc, false).GetF().Args[1]
	ex, err := newTestDecimalAggregateExecutor(proc, expression)
	require.NoError(t, err)
	t.Cleanup(ex.Free)
	chain, ok := ex.(*decimalChainExecutor)
	require.True(t, ok)
	require.Len(t, chain.nodes, 1)
	input := batch.NewWithSize(2)
	t.Cleanup(func() { input.Clean(proc.Mp()) })
	input.Vecs[1] = vector.NewVec(types.New(types.T_decimal64, 15, 2))
	require.NoError(t, vector.AppendFixed(input.Vecs[1], types.Decimal64(5), false, proc.Mp()))
	input.SetRowCount(1)
	out, err := ex.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t, "0.95", vector.GetFixedAtNoTypeCheck[types.Decimal128](out, 0).Format(2))
	require.Equal(t, chain.root.resultType, *out.GetType())
	cast := chain.root.parameterExecutor[1].(*FunctionExpressionExecutor)
	if value := cast.resultVector.GetResultVector(); value != nil {
		require.Zero(t, value.Length())
	}
}
