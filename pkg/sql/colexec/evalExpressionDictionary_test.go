// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package colexec

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

func TestDictionaryFunctionFallbackAndMaskedArguments(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	proc := testutil.NewProcess(t, testutil.WithMPool(mp), testutil.WithFileService(nil))
	typ := types.T_varchar.ToType()
	source := testutil.MakeVarcharVector([]string{"AA", "A LONG STRING BEYOND INLINE STORAGE", "unused"}, []uint64{2}, mp)
	t.Cleanup(func() { source.Free(mp) })
	base, err := vector.FreezeVector(source, mp, nil)
	require.NoError(t, err)
	t.Cleanup(base.Release)
	rows, err := vector.NewRowSelection([]int64{1, 0, 1, 2}, 3, mp, nil)
	require.NoError(t, err)
	t.Cleanup(rows.Release)
	dict, err := vector.NewDictionaryVector(base, rows, mp, nil)
	require.NoError(t, err)
	input := batch.NewWithSize(1)
	input.Vecs[0] = dict
	input.SetRowCount(4)
	t.Cleanup(func() { input.Clean(mp) })
	column := &plan.Expr{Typ: plan.Type{Id: int32(typ.Oid), Width: typ.Width}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	expression := bindTestFunction(t, proc, "lower", column)
	executor, err := NewExpressionExecutor(proc, expression)
	require.NoError(t, err)
	t.Cleanup(executor.Free)
	executor.(*FunctionExpressionExecutor).dictionaryInput = nil // Explicit physical-only compatibility control.
	check := func(mask []bool) {
		t.Helper()
		result, err := executor.Eval(proc, []*batch.Batch{input}, mask)
		require.NoError(t, err)
		require.Equal(t, 4, result.Length())
		require.Equal(t, "a long string beyond inline storage", string(result.GetBytesAt(0)))
		require.Equal(t, "a long string beyond inline storage", string(result.GetBytesAt(2)))
		require.True(t, result.IsNull(3))
		if mask == nil {
			require.Equal(t, "aa", string(result.GetBytesAt(1)))
		} else {
			require.True(t, result.IsNull(1))
		}
		require.True(t, dict.IsDictionary())
		require.Equal(t, "A LONG STRING BEYOND INLINE STORAGE", string(dict.GetBytesAt(0)))
	}
	check(nil)
	root := executor.(*FunctionExpressionExecutor)
	scratch := root.dictionaryScratch[0]
	check(nil)
	require.Same(t, scratch, root.dictionaryScratch[0], "reuse bounded argument storage")
	check([]bool{true, false, true, false})
	executor.ResetForNextQuery()
	require.Nil(t, root.dictionaryScratch)
	check(nil)
	t.Run("logical-string-kernels", func(t *testing.T) {
		for _, name := range []string{"lower", "upper", "length", "char_length"} {
			t.Run(name, func(t *testing.T) {
				direct, err := NewExpressionExecutor(proc, bindTestFunction(t, proc, name, column))
				require.NoError(t, err)
				t.Cleanup(direct.Free)
				value, err := direct.Eval(proc, []*batch.Batch{input}, nil)
				require.NoError(t, err)
				require.True(t, value.IsNull(3))
				if name == "length" || name == "char_length" {
					require.Equal(t, int64(2), vector.GetFixedAtNoTypeCheck[int64](value, 1))
				} else if name == "lower" {
					require.Equal(t, "aa", string(value.GetBytesAt(1)))
				} else {
					require.Equal(t, "AA", string(value.GetBytesAt(1)))
				}
				require.Empty(t, direct.(*FunctionExpressionExecutor).dictionaryScratch)
			})
		}
	})
	t.Run("accounted-build-retention", func(t *testing.T) {
		registry, err := mpool.NewAllocationAccountRegistry(1, 32)
		require.NoError(t, err)
		account, err := registry.Open(1 << 20)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.Zero(t, account.Snapshot().Used)
			_, _, err := registry.CompleteTerminal(account)
			require.NoError(t, err)
		})
		selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
		require.NoError(t, err)
		var retained Batches
		t.Cleanup(func() { retained.Clean(mp) })
		require.NoError(t, retained.CopyIntoBatchesWithAllocation(input, proc, selection))
		require.Len(t, retained.Buf, 1)
		require.Equal(t, 4, retained.Buf[0].RowCount())
		got := retained.Buf[0].Vecs[0]
		require.False(t, got.IsDictionary())
		require.Same(t, selection, got.AllocationAccountSelection())
		require.Equal(t, "A LONG STRING BEYOND INLINE STORAGE", string(got.GetBytesAt(0)))
		require.Equal(t, "AA", string(got.GetBytesAt(1)))
		require.True(t, got.IsNull(3))
		require.True(t, dict.IsDictionary())
	})

}

func TestDictionaryCharComparisonReadsSelectedRows(t *testing.T) {
	for _, oid := range []types.T{types.T_char, types.T_varchar, types.T_text} {
		t.Run(oid.String(), func(t *testing.T) { testDictionaryStringComparison(t, oid) })
	}
}

func testDictionaryStringComparison(t *testing.T, oid types.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	proc := testutil.NewProcess(t, testutil.WithMPool(mp), testutil.WithFileService(nil))
	typ := types.New(oid, 10, 0)
	source := vector.NewVec(typ)
	t.Cleanup(func() { source.Free(mp) })
	first := "AA"
	if oid == types.T_char {
		first = "AA  "
	}
	for i, value := range []string{first, "B", "unused"} {
		require.NoError(t, vector.AppendBytes(source, []byte(value), i == 2, mp))
	}
	base, err := vector.FreezeVector(source, mp, nil)
	require.NoError(t, err)
	t.Cleanup(base.Release)
	rows, err := vector.NewRowSelection([]int64{1, 0, 1, 2}, 3, mp, nil)
	require.NoError(t, err)
	t.Cleanup(rows.Release)
	dict, err := vector.NewDictionaryVector(base, rows, mp, nil)
	require.NoError(t, err)
	input := batch.NewWithSize(1)
	input.Vecs[0] = dict
	input.SetRowCount(4)
	t.Cleanup(func() { input.Clean(mp) })
	column := &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	literal := &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "AA"}}}}
	for _, tc := range []struct {
		op   string
		want []bool
	}{
		{"=", []bool{false, true, false}},
		{"<>", []bool{true, false, true}},
		{"<", []bool{false, false, false}},
		{"<=", []bool{false, true, false}},
		{">", []bool{true, false, true}},
		{">=", []bool{true, true, true}},
	} {
		t.Run(tc.op, func(t *testing.T) {
			executor, err := NewExpressionExecutor(proc, bindTestFunction(t, proc, tc.op, column, literal))
			require.NoError(t, err)
			t.Cleanup(executor.Free)
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			for row, want := range tc.want {
				require.False(t, result.IsNull(uint64(row)))
				require.Equal(t, want, vector.GetFixedAtNoTypeCheck[bool](result, row))
			}
			require.True(t, result.IsNull(3))
			require.Empty(t, executor.(*FunctionExpressionExecutor).dictionaryScratch, "CHAR comparison must read the selected view directly")
			require.True(t, dict.IsDictionary())
		})
	}
}

func TestDictionaryBetweenPreservesNaNNullAndRepeatedRows(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	proc := testutil.NewProcess(t, testutil.WithMPool(mp), testutil.WithFileService(nil))
	typ := types.T_float64.ToType()
	source := vector.NewVec(typ)
	t.Cleanup(func() { source.Free(mp) })
	require.NoError(t, vector.AppendFixedList(source, []float64{math.NaN(), 1, 2, 0}, []bool{false, false, false, true}, mp))
	base, err := vector.FreezeVector(source, mp, nil)
	require.NoError(t, err)
	t.Cleanup(base.Release)
	rows, err := vector.NewRowSelection([]int64{0, 2, 1, 2, 3}, 4, mp, nil)
	require.NoError(t, err)
	t.Cleanup(rows.Release)
	selected, err := vector.NewDictionaryVector(base, rows, mp, nil)
	require.NoError(t, err)
	input := batch.NewWithSize(1)
	input.Vecs[0] = selected
	input.SetRowCount(5)
	t.Cleanup(func() { input.Clean(mp) })
	col := &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	lit := func(v float64) *plan.Expr {
		return &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Dval{Dval: v}}}}
	}
	executor, err := NewExpressionExecutor(proc, bindTestFunction(t, proc, "between", col, lit(1), lit(2)))
	require.NoError(t, err)
	t.Cleanup(executor.Free)
	result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	for row, want := range []bool{false, true, true, true} {
		require.False(t, result.IsNull(uint64(row)))
		require.Equal(t, want, vector.GetFixedAtNoTypeCheck[bool](result, row))
	}
	require.True(t, result.IsNull(4))
	require.Empty(t, executor.(*FunctionExpressionExecutor).dictionaryScratch)
	t.Run("primitive-selected-comparisons", func(t *testing.T) {
		for _, op := range []string{"=", ">="} {
			compare, err := NewExpressionExecutor(proc, bindTestFunction(t, proc, op, col, lit(2)))
			require.NoError(t, err)
			t.Cleanup(compare.Free)
			values, err := compare.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			for row, want := range []bool{false, true, false, true} {
				require.False(t, values.IsNull(uint64(row)))
				require.Equal(t, want, vector.GetFixedAtNoTypeCheck[bool](values, row))
			}
			require.True(t, values.IsNull(4))
			require.Empty(t, compare.(*FunctionExpressionExecutor).dictionaryScratch)
		}
	})
}
