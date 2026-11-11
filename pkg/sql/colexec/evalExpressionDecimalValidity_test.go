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

package colexec

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func appendValidityDecimal(t *testing.T, proc *process.Process, v *vector.Vector, n int64) {
	t.Helper()
	if v.GetType().Oid == types.T_decimal64 {
		require.NoError(t, vector.AppendFixed(v, types.Decimal64(n), false, proc.Mp()))
		return
	}
	x := types.Decimal128{B0_63: uint64(n)}
	if n < 0 {
		x.B64_127 = math.MaxUint64
	}
	require.NoError(t, vector.AppendFixed(v, x, false, proc.Mp()))
}

func TestDecimal128ValidityRuntimeTransitions(t *testing.T) {
	for _, tc := range []struct {
		name     string
		oid      types.T
		cast     bool
		scale    int32
		constant bool
		want     []string
	}{
		{"widen", types.T_decimal64, true, 2, false, []string{"1.20", "-2.50", "0.00", "12.34"}},
		{"widen-scale", types.T_decimal64, true, 4, false, []string{"1.2000", "-2.5000", "0.0000", "12.3400"}},
		{"multiply64", types.T_decimal64, false, 0, false, []string{"2.4000", "-7.5000", "0.0000", "1.2340"}},
		{"multiply128", types.T_decimal128, false, 0, false, []string{"2.4000", "-7.5000", "0.0000", "1.2340"}},
		{"multiply-broadcast", types.T_decimal128, false, 0, true, []string{"2.4000", "-5.0000", "0.0000", "24.6800"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc, allocation, _ := dependentTestAllocation(t)
			input := batch.NewWithSize(2)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			typ := types.New(tc.oid, 15, 2)
			for side, values := range [][]int64{{120, -250, 0, 1234}, {200, 300, -500, 10}} {
				input.Vecs[side] = vector.NewVec(typ)
				for _, n := range values {
					appendValidityDecimal(t, proc, input.Vecs[side], n)
				}
			}
			input.SetRowCount(4)
			if tc.constant {
				input.Vecs[1].ToConst()
			}
			col := func(i int32) *plan.Expr {
				return &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: i}}}
			}
			expression := bindTestFunction(t, proc, "*", col(0), col(1))
			if tc.cast {
				expression = bindTestCast(t, proc, 0, col(0), types.New(types.T_decimal128, 38, tc.scale))
			}
			executor, err := NewExpressionExecutorWithAllocation(proc, expression, allocation)
			require.NoError(t, err)
			t.Cleanup(executor.Free)
			check := func(out *vector.Vector, excluded int) {
				t.Helper()
				for row, want := range tc.want {
					require.Equal(t, row == excluded, out.IsNull(uint64(row)))
					if row != excluded {
						require.Equal(t, want, vector.GetFixedAtNoTypeCheck[types.Decimal128](out, row).Format(out.GetType().Scale))
					}
				}
			}
			out, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			check(out, -1)
			require.Zero(t, out.GetNulls().GetBitmap().ExternalStorageCapacity())
			input.Vecs[0].SetNull(1)
			out, err = executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			check(out, 1)
			capacity := out.GetNulls().GetBitmap().ExternalStorageCapacity()
			require.Positive(t, capacity)
			input.Vecs[0].GetNulls().Reset()
			out, err = executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			check(out, -1)
			require.Equal(t, capacity, out.GetNulls().GetBitmap().ExternalStorageCapacity())
			out, err = executor.Eval(proc, []*batch.Batch{input}, []bool{true, false, true, true})
			require.NoError(t, err)
			check(out, 1)
			input.Vecs[0].SetNull(1)
			out, err = executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			check(out, 1)
			if tc.constant {
				require.NoError(t, vector.SetConstNull(input.Vecs[1], 4, proc.Mp()))
				out, err = executor.Eval(proc, []*batch.Batch{input}, nil)
				require.NoError(t, err)
				for row := range 4 {
					require.True(t, out.IsNull(uint64(row)))
				}
			}
		})
	}
}

func TestDecimal128ValidityCastBounds(t *testing.T) {
	for _, tc := range []struct {
		name           string
		width, scale   int32
		target         types.Type
		overload       int32
		explicit, skip bool
	}{
		{"legacy-carrier", 18, 2, types.New(types.T_decimal128, 19, 2), 0, false, true},
		{"legacy-insufficient", 18, 2, types.New(types.T_decimal128, 18, 2), 0, false, false},
		{"narrow-width", 15, 2, types.New(types.T_decimal128, 14, 2), 0, false, false},
		{"round-scale", 15, 2, types.New(types.T_decimal128, 15, 1), 0, false, false},
		{"explicit-overload", 15, 2, types.New(types.T_decimal128, 38, 2), 1, true, false},
		{"explicit-syntax", 15, 2, types.New(types.T_decimal128, 38, 2), 0, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc, allocation, _ := dependentTestAllocation(t)
			typ := types.New(types.T_decimal64, tc.width, tc.scale)
			input := batch.NewWithSize(1)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			input.Vecs[0] = vector.NewVec(typ)
			for _, n := range []int64{120, -250} {
				appendValidityDecimal(t, proc, input.Vecs[0], n)
			}
			if tc.name == "legacy-carrier" {
				input.Vecs[0].CleanOnlyData()
				appendValidityDecimal(t, proc, input.Vecs[0], math.MaxInt64)
				appendValidityDecimal(t, proc, input.Vecs[0], math.MinInt64)
			}
			input.SetRowCount(2)
			column := &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
			expression := bindTestCast(t, proc, tc.overload, column, tc.target)
			expression.GetF().SyntaxExplicitCast = tc.explicit
			executor, err := NewExpressionExecutorWithAllocation(proc, expression, allocation)
			require.NoError(t, err)
			t.Cleanup(executor.Free)
			out, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, tc.skip, out.GetNulls().GetBitmap().ExternalStorageCapacity() == 0)
			if tc.name == "legacy-carrier" {
				require.Equal(t, "92233720368547758.07", vector.GetFixedAtNoTypeCheck[types.Decimal128](out, 0).Format(2))
				require.Equal(t, "-92233720368547758.08", vector.GetFixedAtNoTypeCheck[types.Decimal128](out, 1).Format(2))
			}
		})
	}
}

func TestDecimal128ValidityAdmissionFailure(t *testing.T) {
	for _, cast := range []bool{false, true} {
		t.Run(map[bool]string{false: "multiply", true: "cast"}[cast], func(t *testing.T) {
			proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			t.Cleanup(proc.Free)
			registry, err := mpool.NewAllocationAccountRegistry(1, 32)
			require.NoError(t, err)
			account, err := registry.Open(64)
			require.NoError(t, err)
			t.Cleanup(func() {
				require.Zero(t, account.Snapshot().Used)
				_, _, err := registry.CompleteTerminal(account)
				require.NoError(t, err)
			})
			allocation, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerSelection, 1, 2, 3, 4)
			require.NoError(t, err)
			typ := types.New(types.T_decimal64, 15, 2)
			input := batch.NewWithSize(2)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			for side := range 2 {
				input.Vecs[side] = vector.NewVec(typ)
				for range 4 {
					appendValidityDecimal(t, proc, input.Vecs[side], 100)
				}
			}
			input.SetRowCount(4)
			col := func(i int32) *plan.Expr {
				return &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: i}}}
			}
			expression := bindTestFunction(t, proc, "*", col(0), col(1))
			if cast {
				expression = bindTestCast(t, proc, 0, col(0), types.New(types.T_decimal128, 38, 2))
			}
			executor, err := NewExpressionExecutorWithAllocation(proc, expression, allocation)
			require.NoError(t, err)
			t.Cleanup(executor.Free)
			out, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.EqualValues(t, 64, account.Snapshot().Used)
			require.Zero(t, out.GetNulls().GetBitmap().ExternalStorageCapacity())
			input.Vecs[0].SetNull(0)
			_, err = executor.Eval(proc, []*batch.Batch{input}, nil)
			require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
			input.Vecs[0].GetNulls().Reset()
			_, err = executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			for _, v := range input.Vecs {
				appendValidityDecimal(t, proc, v, 100)
			}
			input.SetRowCount(5)
			_, err = executor.Eval(proc, []*batch.Batch{input}, nil)
			require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
		})
	}
}

func TestDecimal128ValidityPreservesMultiplyOverflow(t *testing.T) {
	proc, allocation, _ := dependentTestAllocation(t)
	typ := types.New(types.T_decimal128, 38, 0)
	input := batch.NewWithSize(2)
	t.Cleanup(func() { input.Clean(proc.Mp()) })
	for side, text := range []string{"99999999999999999999999999999999999999", "2"} {
		value, err := types.ParseDecimal128(text, 38, 0)
		require.NoError(t, err)
		input.Vecs[side] = vector.NewVec(typ)
		require.NoError(t, vector.AppendFixed(input.Vecs[side], value, false, proc.Mp()))
	}
	input.SetRowCount(1)
	col := func(i int32) *plan.Expr {
		return &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: i}}}
	}
	expression := bindTestFunction(t, proc, "*", col(0), col(1))
	// Exercise the registered native D128 kernel's error contract. The SQL
	// binder promotes this range to D256; that promotion is outside this
	// result-preparation test and would remove the native overflow case.
	expression.Typ = typ.PlanType()
	executor, err := NewExpressionExecutorWithAllocation(proc, expression, allocation)
	require.NoError(t, err)
	t.Cleanup(executor.Free)
	_, err = executor.Eval(proc, []*batch.Batch{input}, nil)
	require.Error(t, err, "overflow must retain its error, not become a NULL result")
	root := executor.(*FunctionExpressionExecutor)
	require.Zero(t, root.resultVector.GetResultVector().GetNulls().GetBitmap().ExternalStorageCapacity())
}

func TestDecimal128ValidityMetadataFallback(t *testing.T) {
	for _, shape := range []string{"cast-grouping", "multiply-prepared-json"} {
		t.Run(shape, func(t *testing.T) {
			proc, allocation, _ := dependentTestAllocation(t)
			typ := types.New(types.T_decimal64, 15, 2)
			input := batch.NewWithSize(2)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			for side := range 2 {
				input.Vecs[side] = vector.NewVec(typ)
				appendValidityDecimal(t, proc, input.Vecs[side], 100)
			}
			input.SetRowCount(1)
			col := func(i int32) *plan.Expr {
				return &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: i}}}
			}
			expression := bindTestFunction(t, proc, "*", col(0), col(1))
			if shape == "cast-grouping" {
				expression = bindTestCast(t, proc, 0, col(0), types.New(types.T_decimal128, 38, 2))
				input.Vecs[0].GetGrouping().Add(0)
			} else {
				input.Vecs[1].SetPreparedJSONComparisonParam()
			}
			executor, err := NewExpressionExecutorWithAllocation(proc, expression, allocation)
			require.NoError(t, err)
			t.Cleanup(executor.Free)
			out, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Positive(t, out.GetNulls().GetBitmap().ExternalStorageCapacity())
		})
	}
}

func TestDecimal128ValidityQ17Predicate(t *testing.T) {
	proc, allocation, _ := dependentTestAllocation(t)
	quantity := types.New(types.T_decimal64, 15, 2)
	average := aggexec.AvgReturnType([]types.Type{quantity})
	input := batch.NewWithSize(2)
	t.Cleanup(func() { input.Clean(proc.Mp()) })
	input.Vecs[0], input.Vecs[1] = vector.NewVec(quantity), vector.NewVec(average)
	averageValue, err := types.ParseDecimal128("10", average.Width, average.Scale)
	require.NoError(t, err)
	for _, n := range []int64{120, -250, 0, 1234} {
		appendValidityDecimal(t, proc, input.Vecs[0], n)
		require.NoError(t, vector.AppendFixed(input.Vecs[1], averageValue, false, proc.Mp()))
	}
	input.SetRowCount(4)
	col := func(i int32, typ types.Type) *plan.Expr {
		return &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: i}}}
	}
	literal := &plan.Expr{Typ: types.New(types.T_decimal128, 1, 1).PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Decimal128Val{Decimal128Val: &plan.Decimal128{A: 2}}}}}
	widen := bindTestCast(t, proc, 0, col(0, quantity), types.New(types.T_decimal128, 38, 2))
	multiply := bindTestFunction(t, proc, "*", literal, col(1, average))
	expression := bindTestFunction(t, proc, "<", widen, multiply)
	executor, err := NewExpressionExecutorWithAllocation(proc, expression, allocation)
	require.NoError(t, err)
	t.Cleanup(executor.Free)
	out, err := executor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t, []bool{true, true, true, false}, vector.MustFixedColNoTypeCheck[bool](out))
	root := executor.(*FunctionExpressionExecutor)
	for _, child := range root.parameterExecutor {
		value := child.(*FunctionExpressionExecutor).resultVector.GetResultVector()
		require.Equal(t, types.T_decimal128, value.GetType().Oid)
		require.Zero(t, value.GetNulls().GetBitmap().ExternalStorageCapacity())
	}
	input.Vecs[1].SetNull(1)
	out, err = executor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.True(t, out.IsNull(1))
	input.Vecs[1].GetNulls().Reset()
	out, err = executor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.False(t, out.HasNull())
	require.Equal(t, []bool{true, true, true, false}, vector.MustFixedColNoTypeCheck[bool](out))
}
