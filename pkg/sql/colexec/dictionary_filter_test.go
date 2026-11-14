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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDictionaryPredicateOriginalRowsAndErrorOrder(t *testing.T) {
	for _, shape := range []string{"original-rows", "skip-divide-zero", "identity", "none", "memo", "single-batch-tag"} {
		t.Run(shape, func(t *testing.T) {
			skipError := shape == "skip-divide-zero"
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			proc := testutil.NewProcess(t, testutil.WithMPool(mp), testutil.WithFileService(nil))
			warnings := &process.WarningProbe{}
			proc.WarningSink = warnings
			typ := types.T_float64.ToType()
			input := batch.NewWithSize(1)
			t.Cleanup(func() { input.Clean(mp) })
			input.Vecs[0] = vector.NewVec(typ)
			values := []float64{1, 2, 3, 4}
			if skipError {
				values = []float64{0, 2}
			}
			require.NoError(t, vector.AppendFixedList(input.Vecs[0], values, nil, mp))
			input.SetRowCount(len(values))
			col := &plan.Expr{Typ: plan.Type{Id: int32(typ.Oid)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
			literal := func(value float64) *plan.Expr {
				return &plan.Expr{Typ: plan.Type{Id: int32(typ.Oid)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Dval{Dval: value}}}}
			}
			predicates := []*plan.Expr{bindTestFunction(t, proc, ">", col, literal(1)), bindTestFunction(t, proc, "<", col, literal(4))}
			want := []int64{1, 2}
			if skipError {
				predicates = []*plan.Expr{bindTestFunction(t, proc, "<>", col, literal(0)), bindTestFunction(t, proc, ">", bindTestFunction(t, proc, "/", literal(10), col), literal(0))}
				want = []int64{1}
			}
			if shape == "identity" {
				predicates = []*plan.Expr{bindTestFunction(t, proc, ">", col, literal(0)), bindTestFunction(t, proc, "<", col, literal(5))}
				want = []int64{}
			} else if shape == "none" {
				predicates = []*plan.Expr{bindTestFunction(t, proc, ">", col, literal(9))}
				want = []int64{}
			}
			if shape == "single-batch-tag" {
				col.GetCol().RelPos = 7
			}
			executors, err := NewExpressionExecutorsFromPlanExpressions(proc, predicates)
			require.NoError(t, err)
			var frame *DictionaryInputFrame
			if shape == "memo" {
				state := &memoExpressionState{executor: executors[1], refs: 1}
				executors[1] = &memoRootExpressionExecutor{executor: &memoExpressionExecutor{state: state}, states: []*memoExpressionState{state}}
				frame = &DictionaryInputFrame{}
				t.Cleanup(func() { frame.Free(mp) })
			}
			t.Cleanup(func() {
				for _, executor := range executors {
					executor.Free()
				}
			})
			var scratch []int64
			var parameter vector.FunctionParameterWrapper[bool]
			got, all, err := SelectPredicateRows(proc, input, nil, executors, nil, &scratch, &parameter, nil, nil, frame)
			require.NoError(t, err)
			require.Equal(t, shape == "identity", all)
			if len(want) == 0 {
				require.Empty(t, got)
			} else {
				require.Equal(t, want, got)
			}
			require.Equal(t, len(values), input.RowCount())
			require.Equal(t, values, vector.MustFixedColNoTypeCheck[float64](input.Vecs[0]))
			require.False(t, warnings.Warned(), "rejected rows must not emit division diagnostics")
			if shape == "memo" {
				vector.MustFixedColNoTypeCheck[float64](input.Vecs[0])[2] = 5
				got, all, err = SelectPredicateRows(proc, input, nil, executors, nil, &scratch, &parameter, nil, nil, frame)
				require.NoError(t, err)
				require.False(t, all)
				require.Equal(t, []int64{1}, got, "memo roots must reevaluate a reused predicate bank")
			}

		})
	}
}

func TestPredicateScratchReusesOnlyReferencedColumns(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	proc := testutil.NewProcess(t, testutil.WithMPool(mp), testutil.WithFileService(nil))
	input := batch.NewWithSize(2)
	t.Cleanup(func() { input.Clean(mp) })
	input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	input.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendFixedList(input.Vecs[0], []int64{10, 20, 30}, nil, mp))
	for _, s := range []string{"unreferenced first payload", "unreferenced second payload", "unreferenced third payload"} {
		require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(s), false, mp))
	}
	input.Vecs[0].GetNulls().Add(1)
	input.Vecs[0].GetGrouping().Add(2)
	input.Vecs[0].SetSorted(true)
	input.SetRowCount(3)
	executor, err := NewExpressionExecutor(proc, &plan.Expr{Typ: types.T_int64.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}})
	require.NoError(t, err)
	t.Cleanup(executor.Free)
	frame := &DictionaryInputFrame{}
	t.Cleanup(func() { frame.Free(mp) })
	frame.Begin(input, nil, mp)
	first, borrowed, err := frame.predicateBatch(proc, input, nil, executor, []int64{2, 0}, nil)
	require.NoError(t, err)
	require.True(t, borrowed)
	require.Nil(t, first.Vecs[1])
	require.Empty(t, frame.predicateVectors)
	require.True(t, first.Vecs[0].IsDictionary())
	saved := first.Vecs[0]
	require.Equal(t, []int64{30, 10}, []int64{vector.GetFixedAtNoTypeCheck[int64](saved, 0), vector.GetFixedAtNoTypeCheck[int64](saved, 1)})
	require.True(t, saved.GetGrouping().Contains(0))
	require.False(t, saved.HasNull())
	require.False(t, saved.GetSorted())
	second, borrowed, err := frame.predicateBatch(proc, input, nil, executor, []int64{1}, nil)
	require.NoError(t, err)
	require.True(t, borrowed)
	require.Same(t, first, second)
	require.Same(t, saved, second.Vecs[0])
	require.True(t, second.Vecs[0].IsNull(0))
	require.False(t, second.Vecs[0].HasGrouping())
	require.True(t, second.Vecs[0].GetSorted())
	require.Equal(t, []int64{10, 20, 30}, vector.MustFixedColNoTypeCheck[int64](input.Vecs[0]))
	frame.End()
	require.Nil(t, second.Vecs[0], "end evaluation clears borrowed bank headers")
	require.Zero(t, saved.Length(), "End retires scoped borrowed headers")
	require.Empty(t, frame.predicateVectors, "logical reads need no owned predicate payload")
}

func TestCompactPredicateRowsOriginalDomain(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	value := vector.NewVec(types.T_bool.ToType())
	t.Cleanup(func() { value.Free(mp) })
	require.NoError(t, vector.AppendFixedList(value, []bool{true, false, true, true}, nil, mp))
	require.Equal(t, []int64{0, 2, 3}, compactPredicateRows(value, 4, nil, false))
	scratch := []int64{8, 2, 8, 5}
	address := &scratch[0]
	got := compactPredicateRows(value, 4, scratch, true)
	require.Equal(t, []int64{8, 8, 5}, got)
	require.Same(t, address, &got[0], "later predicates must compact existing positions")
	value.GetNulls().Add(2)
	require.Equal(t, []int64{0, 3}, compactPredicateRows(value, 4, nil, false))
	base, err := vector.FreezeVector(value, mp, nil)
	require.NoError(t, err)
	t.Cleanup(base.Release)
	rows, err := vector.NewRowSelection([]int64{3, 2, 0, 1}, 4, mp, nil)
	require.NoError(t, err)
	t.Cleanup(rows.Release)
	dictionary, err := vector.NewDictionaryVector(base, rows, mp, nil)
	require.NoError(t, err)
	t.Cleanup(func() { dictionary.Free(mp) })
	require.Equal(t, []int64{0, 2}, compactPredicateRows(dictionary, 4, nil, false))
	scratch = []int64{9, 7, 3, 1}
	require.Equal(t, []int64{9, 3}, compactPredicateRows(dictionary, 4, scratch, true))
	for _, keep := range []bool{false, true} {
		func() {
			constant, err := vector.NewConstFixed(types.T_bool.ToType(), keep, 4, mp)
			require.NoError(t, err)
			defer constant.Free(mp)
			frozen, err := vector.FreezeVector(constant, mp, nil)
			require.NoError(t, err)
			defer frozen.Release()
			selected, err := vector.NewDictionaryVector(frozen, rows, mp, nil)
			require.NoError(t, err)
			defer selected.Free(mp)
			result := compactPredicateRows(selected, 4, nil, false)
			if keep {
				require.Equal(t, []int64{0, 1, 2, 3}, result)
			} else {
				require.Empty(t, result)
			}
		}()
	}
}

func TestPredicateScratchCapacityFallsBackAfterReleasingBanks(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	proc := testutil.NewProcess(t, testutil.WithMPool(mp), testutil.WithFileService(nil))
	registry, err := mpool.NewAllocationAccountRegistry(1, 32)
	require.NoError(t, err)
	account, err := registry.Open(1536)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.Zero(t, account.Snapshot().Used)
		_, _, err := registry.CompleteTerminal(account)
		require.NoError(t, err)
	})
	allocation, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerSelection, 1, 2, 3, 4)
	require.NoError(t, err)
	input := batch.NewWithSize(2)
	t.Cleanup(func() { input.Clean(mp) })
	rows := make([]int64, 128)
	for i := range rows {
		rows[i] = int64(i)
	}
	for i := range input.Vecs {
		input.Vecs[i] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(input.Vecs[i], rows, nil, mp))
	}
	input.SetRowCount(len(rows))
	frame := &DictionaryInputFrame{}
	t.Cleanup(func() { frame.Free(mp) })
	frame.Begin(input, allocation, mp)
	for col := 0; col < 2; col++ {
		executor, err := NewExpressionExecutor(proc, &plan.Expr{Typ: types.T_int64.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: int32(col)}}})
		require.NoError(t, err)
		t.Cleanup(executor.Free)
		// A function without an audited borrow contract uses owned scratch.
		wrapper := &FunctionExpressionExecutor{parameterExecutor: []ExpressionExecutor{executor}}
		output, borrowed, err := frame.predicateBatch(proc, input, nil, wrapper, rows, allocation)
		require.NoError(t, err)
		require.Equal(t, col == 0, borrowed)
		require.Equal(t, rows, vector.MustFixedColNoTypeCheck[int64](output.Vecs[col]))
		if !borrowed {
			output.Clean(mp)
		}
	}
	require.Nil(t, frame.predicateVectors[0], "optional scratch must yield capacity to the valid fallback")
}

func TestPredicateRowScratchRetiresAtNextOwnerOrFree(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	proc := testutil.NewProcess(t, testutil.WithMPool(mp), testutil.WithFileService(nil))
	input := batch.NewWithSize(1)
	t.Cleanup(func() { input.Clean(mp) })
	input.Vecs[0] = vector.NewVec(types.T_bool.ToType())
	require.NoError(t, vector.AppendFixedList(input.Vecs[0], []bool{true, false, true}, nil, mp))
	input.SetRowCount(3)
	executor, err := NewExpressionExecutor(proc, &plan.Expr{Typ: types.T_bool.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}})
	require.NoError(t, err)
	t.Cleanup(executor.Free)
	frame := &DictionaryInputFrame{}
	t.Cleanup(func() { frame.Free(mp) })
	var first, second []int64
	var parameter vector.FunctionParameterWrapper[bool]
	got, all, err := SelectPredicateRows(proc, input, nil, []ExpressionExecutor{executor}, nil, &first, &parameter, nil, nil, frame)
	require.NoError(t, err)
	require.False(t, all)
	require.Equal(t, []int64{0, 2}, got, "End must not expire the returned borrowed mapping")
	require.Same(t, &first, frame.pooledRows)
	got, all, err = SelectPredicateRows(proc, input, nil, []ExpressionExecutor{executor}, nil, &second, &parameter, nil, nil, frame)
	require.NoError(t, err)
	require.False(t, all)
	require.Nil(t, first, "the next callback may retire the prior scratch owner")
	require.Equal(t, []int64{0, 2}, got)
	frame.Free(mp)
	require.Nil(t, second, "Free returns and clears the final pooled slice")
}
