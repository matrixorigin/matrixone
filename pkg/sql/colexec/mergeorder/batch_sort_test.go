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

package mergeorder

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestSortBatchSpillsAndPreservesOrder(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	input := newValuesBatch(proc, []int8{7, 1, 5, 1, 3, 9})
	var got *batch.Batch
	t.Cleanup(func() {
		if got != nil {
			got.Clean(proc.Mp())
		}
		input.Clean(proc.Mp())
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
	analyzer := process.NewAnalyzer(0, false, false, "window-sort-spill")
	fs := []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8)}}

	var err error
	got, err = SortBatch(proc, input, fs, 1, analyzer)
	require.NoError(t, err)
	require.Equal(t, []int8{1, 1, 3, 5, 7, 9}, vector.MustFixedColWithTypeCheck[int8](got.Vecs[0]))
	require.Positive(t, analyzer.GetOpStats().SpillRows)
	require.Positive(t, analyzer.GetOpStats().SpillSize)
}

func TestSortBatchEnforcesSpillResourceAdmission(t *testing.T) {
	fs := []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8)}}
	for _, tc := range []struct {
		name      string
		component process.ExecutionResourceComponent
	}{
		{name: "disk", component: process.ExecutionResourceComponentSpillDisk},
		{name: "file descriptor", component: process.ExecutionResourceComponentSpillFD},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			t.Cleanup(func() {
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			})
			generation, err := proc.GetExecutionResourceBudget()
			require.NoError(t, err)

			var releaseBlocker func()
			switch tc.component {
			case process.ExecutionResourceComponentSpillDisk:
				reservation, reserveErr := generation.ReserveSpillDisk(generation.SpillDiskCap())
				require.NoError(t, reserveErr)
				releaseBlocker = func() {
					if reservation != nil {
						require.True(t, reservation.Release())
					}
				}
			case process.ExecutionResourceComponentSpillFD:
				reservation, reserveErr := generation.ReserveSpillFD(generation.SpillFDCap())
				require.NoError(t, reserveErr)
				releaseBlocker = func() {
					if reservation != nil {
						require.True(t, reservation.Release())
					}
				}
			}
			t.Cleanup(func() {
				releaseBlocker()
				require.Zero(t, generation.SpillDiskUsed())
				require.Zero(t, generation.SpillFDUsed())
			})

			input := newValuesBatch(proc, []int8{3, 1, 2})
			var sorted *batch.Batch
			t.Cleanup(func() {
				if sorted != nil {
					sorted.Clean(proc.Mp())
				}
				input.Clean(proc.Mp())
			})

			before := generation.Snapshot()
			sorted, err = SortBatch(
				proc,
				input,
				fs,
				1,
				process.NewAnalyzer(0, false, false, "batch-sort-resource-admission"),
			)
			require.Nil(t, sorted)
			var resourceErr *process.ExecutionResourceError
			require.True(t, errors.As(err, &resourceErr))
			require.Equal(t, tc.component, resourceErr.Component)
			after := generation.Snapshot()
			require.Equal(t, before.SpillDiskUsed, after.SpillDiskUsed)
			require.Equal(t, before.SpillFDUsed, after.SpillFDUsed)
			require.Equal(t, []int8{3, 1, 2}, vector.MustFixedColWithTypeCheck[int8](input.Vecs[0]))
		})
	}
}

func TestSortBatchFreesSingleBatchExpressionKey(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	input := newValuesBatch(proc, []int8{3, 1, 2})
	key := testutil.NewVector(3, types.T_int8.ToType(), proc.Mp(), false, []int8{1, 2, 3})
	ctr := &container{
		batchList: []*batch.Batch{input},
		orderCols: [][]*vector.Vector{{key}},
	}
	var got *batch.Batch
	t.Cleanup(func() {
		cleanupBatchSortContainer(proc, ctr)
		if got != nil {
			got.Clean(proc.Mp())
		}
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
	analyzer := process.NewAnalyzer(0, false, false, "single-batch-expression-key")
	fs := []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8)}}

	var err error
	got, err = ctr.collectSortedBatch(proc, fs, analyzer)
	require.NoError(t, err)
}

func TestSortBatchEmptyInputIsIndependent(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	input := newValuesBatch(proc, nil)
	fs := []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8)}}
	var sorted *batch.Batch
	t.Cleanup(func() {
		if sorted != nil {
			sorted.Clean(proc.Mp())
		}
		input.Clean(proc.Mp())
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	var err error
	sorted, err = SortBatch(proc, input, fs, 1, process.NewAnalyzer(0, false, false, "batch-sort-empty"))
	require.NoError(t, err)
	require.NotSame(t, input, sorted)
	require.Zero(t, sorted.RowCount())
	require.Len(t, sorted.Vecs, 1)
}

func TestSortBatchCarriesPrecomputedAndExtraVectors(t *testing.T) {
	fs := []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8)}}

	t.Run("precomputed order and extra argument", func(t *testing.T) {
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		input := newPairBatch(proc, []int8{10, 20, 30}, []int64{1000, 2000, 3000})
		input.Attrs = []string{"id", "payload"}
		key := testutil.NewVector(3, types.T_int8.ToType(), proc.Mp(), false, []int8{3, 1, 2})
		extra := testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{100, 200, 300})
		var sorted *batch.Batch
		var sortedKeys, sortedExtra []*vector.Vector
		t.Cleanup(func() {
			if sorted != nil {
				sorted.Clean(proc.Mp())
			}
			for _, vec := range sortedKeys {
				if vec != nil {
					vec.Free(proc.Mp())
				}
			}
			for _, vec := range sortedExtra {
				if vec != nil {
					vec.Free(proc.Mp())
				}
			}
			if input != nil {
				input.Clean(proc.Mp())
			}
			if key != nil {
				key.Free(proc.Mp())
			}
			if extra != nil {
				extra.Free(proc.Mp())
			}
			proc.Free()
			require.Zero(t, proc.Mp().CurrNB())
		})
		analyzer := process.NewAnalyzer(0, false, false, "batch-sort-precomputed-carry")

		var err error
		sorted, sortedKeys, sortedExtra, err = SortBatchWithPrecomputedOrder(
			proc, input, fs, 1, analyzer, []*vector.Vector{key}, []*vector.Vector{extra})
		require.NoError(t, err)
		require.Len(t, sortedKeys, 1)
		require.Len(t, sortedExtra, 1)

		input.Clean(proc.Mp())
		input = nil
		key.Free(proc.Mp())
		key = nil
		extra.Free(proc.Mp())
		extra = nil
		require.Equal(t, []int8{20, 30, 10}, vector.MustFixedColWithTypeCheck[int8](sorted.Vecs[0]))
		require.Equal(t, []int64{2000, 3000, 1000}, vector.MustFixedColWithTypeCheck[int64](sorted.Vecs[1]))
		require.Equal(t, []int8{1, 2, 3}, vector.MustFixedColWithTypeCheck[int8](sortedKeys[0]))
		require.Equal(t, []int64{200, 300, 100}, vector.MustFixedColWithTypeCheck[int64](sortedExtra[0]))
		require.Equal(t, []string{"id", "payload"}, sorted.Attrs)
	})

	t.Run("evaluated extra vector", func(t *testing.T) {
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		input := newValuesBatch(proc, []int8{3, 1, 2})
		extra := testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{30, 10, 20})
		delegatedInput := (*batch.Batch)(nil)
		var sorted, delegated *batch.Batch
		var sortedExtra, delegatedExtra []*vector.Vector
		t.Cleanup(func() {
			if sorted != nil {
				sorted.Clean(proc.Mp())
			}
			if delegated != nil {
				delegated.Clean(proc.Mp())
			}
			for _, vec := range sortedExtra {
				if vec != nil {
					vec.Free(proc.Mp())
				}
			}
			for _, vec := range delegatedExtra {
				if vec != nil {
					vec.Free(proc.Mp())
				}
			}
			if input != nil {
				input.Clean(proc.Mp())
			}
			if delegatedInput != nil {
				delegatedInput.Clean(proc.Mp())
			}
			if extra != nil {
				extra.Free(proc.Mp())
			}
			proc.Free()
			require.Zero(t, proc.Mp().CurrNB())
		})
		analyzer := process.NewAnalyzer(0, false, false, "batch-sort-extra-carry")

		var err error
		sorted, sortedExtra, err = SortBatchWithExtraVectors(
			proc, input, fs, 1, analyzer, []*vector.Vector{extra})
		require.NoError(t, err)
		require.Len(t, sortedExtra, 1)
		input.Clean(proc.Mp())
		input = nil
		extra.Free(proc.Mp())
		extra = nil
		require.Equal(t, []int8{1, 2, 3}, vector.MustFixedColWithTypeCheck[int8](sorted.Vecs[0]))
		require.Equal(t, []int64{10, 20, 30}, vector.MustFixedColWithTypeCheck[int64](sortedExtra[0]))

		delegatedInput = newValuesBatch(proc, []int8{2, 1})
		delegated, delegatedExtra, err = SortBatchWithExtraVectors(
			proc, delegatedInput, fs, 1<<30, analyzer, nil)
		require.NoError(t, err)
		require.Nil(t, delegatedExtra)
		delegatedInput.Clean(proc.Mp())
		delegatedInput = nil
		require.Equal(t, []int8{1, 2}, vector.MustFixedColWithTypeCheck[int8](delegated.Vecs[0]))
	})
}

func TestSortBatchMergesResidentAndSpilledChunks(t *testing.T) {
	fs := []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8)}}

	for _, tc := range []struct {
		name  string
		spill bool
	}{
		{name: "resident", spill: false},
		{name: "spill runs", spill: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			input := newValuesBatch(proc, []int8{3, 1, 4, 2})
			ctr := &container{
				executors:   []colexec.ExpressionExecutor{&countingColumnExecutor{col: 0, maxCalls: 4}},
				batchList:   make([]*batch.Batch, 0, 2),
				orderCols:   make([][]*vector.Vector, 0, 2),
				spillColPos: []int32{0},
			}
			var sorted *batch.Batch
			t.Cleanup(func() {
				if sorted != nil {
					sorted.Clean(proc.Mp())
				}
				if input != nil {
					input.Clean(proc.Mp())
				}
				cleanupBatchSortContainer(proc, ctr)
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			})
			ctr.setSpillThreshold(1 << 30)
			ctr.generateCompares(fs)
			desc, nullsLast := orderFlags(fs)
			analyzer := process.NewAnalyzer(0, false, false, "batch-sort-chunk-merge")

			require.NoError(t, ctr.appendSortedChunk(proc, input, 0, 2, desc, nullsLast, fs, analyzer))
			if tc.spill {
				ctr.setSpillThreshold(int64(ctr.batchList[0].Size()))
			}
			require.NoError(t, ctr.appendSortedChunk(proc, input, 2, 4, desc, nullsLast, fs, analyzer))
			if tc.spill {
				require.True(t, ctr.spilling)
				require.Len(t, ctr.spillRuns, 2)
			} else {
				require.False(t, ctr.spilling)
				require.Len(t, ctr.batchList, 2)
			}

			var err error
			sorted, err = ctr.collectSortedBatch(proc, fs, analyzer)
			require.NoError(t, err)
			input.Clean(proc.Mp())
			input = nil
			require.Equal(t, []int8{1, 2, 3, 4}, vector.MustFixedColWithTypeCheck[int8](sorted.Vecs[0]))
		})
	}
}

func TestSortBatchHonorsExplicitNullPlacement(t *testing.T) {
	fsFor := func(flag plan.OrderBySpec_OrderByFlag) []*plan.OrderBySpec {
		return []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: flag}}
	}

	for _, tc := range []struct {
		name      string
		flag      plan.OrderBySpec_OrderByFlag
		want      []int8
		nullIndex uint64
	}{
		{name: "nulls first", flag: plan.OrderBySpec_DESC | plan.OrderBySpec_NULLS_FIRST, want: []int8{3, 1}, nullIndex: 0},
		{name: "nulls last", flag: plan.OrderBySpec_DESC | plan.OrderBySpec_NULLS_LAST, want: []int8{3, 1}, nullIndex: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			input := newNullableValuesBatch(proc, []int8{2, 1, 3}, []uint64{0})
			var sorted *batch.Batch
			t.Cleanup(func() {
				if sorted != nil {
					sorted.Clean(proc.Mp())
				}
				if input != nil {
					input.Clean(proc.Mp())
				}
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			})
			analyzer := process.NewAnalyzer(0, false, false, "batch-sort-null-placement")
			var err error
			sorted, err = SortBatch(proc, input, fsFor(tc.flag), 1<<30, analyzer)
			require.NoError(t, err)
			input.Clean(proc.Mp())
			input = nil
			got := vector.MustFixedColWithTypeCheck[int8](sorted.Vecs[0])
			if tc.nullIndex == 0 {
				require.Equal(t, tc.want, got[1:])
			} else {
				require.Equal(t, tc.want, got[:len(got)-1])
			}
			require.True(t, sorted.Vecs[0].GetNulls().Contains(tc.nullIndex))
		})
	}
}

func TestSortBatchRejectsInvalidInputs(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	input := newValuesBatch(proc, []int8{3, 1, 2})
	short := testutil.NewVector(2, types.T_int8.ToType(), proc.Mp(), false, []int8{1, 2})
	t.Cleanup(func() {
		short.Free(proc.Mp())
		input.Clean(proc.Mp())
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
	fs := []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8)}}
	validKey := []*vector.Vector{input.Vecs[0]}
	assertInvalid := func(t *testing.T, err error) {
		t.Helper()
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), err)
	}

	t.Run("nil process", func(t *testing.T) {
		_, err := SortBatch(nil, input, fs, 1, process.NewAnalyzer(0, false, false, "invalid"))
		assertInvalid(t, err)
	})
	t.Run("nil input", func(t *testing.T) {
		_, err := SortBatch(proc, nil, fs, 1, process.NewAnalyzer(0, false, false, "invalid"))
		assertInvalid(t, err)
	})
	t.Run("missing order specification", func(t *testing.T) {
		_, err := SortBatch(proc, input, nil, 1, process.NewAnalyzer(0, false, false, "invalid"))
		assertInvalid(t, err)
	})
	t.Run("precomputed key count", func(t *testing.T) {
		_, _, _, err := SortBatchWithPrecomputedOrder(proc, input, fs, 1, process.NewAnalyzer(0, false, false, "invalid"), nil, nil)
		assertInvalid(t, err)
	})
	t.Run("nil precomputed specification", func(t *testing.T) {
		_, _, _, err := SortBatchWithPrecomputedOrder(proc, input, []*plan.OrderBySpec{nil}, 1, process.NewAnalyzer(0, false, false, "invalid"), validKey, nil)
		assertInvalid(t, err)
	})
	t.Run("nil precomputed expression", func(t *testing.T) {
		_, _, _, err := SortBatchWithPrecomputedOrder(proc, input, []*plan.OrderBySpec{{}}, 1, process.NewAnalyzer(0, false, false, "invalid"), validKey, nil)
		assertInvalid(t, err)
	})
	t.Run("nil precomputed key", func(t *testing.T) {
		_, _, _, err := SortBatchWithPrecomputedOrder(proc, input, fs, 1, process.NewAnalyzer(0, false, false, "invalid"), []*vector.Vector{nil}, nil)
		assertInvalid(t, err)
	})
	t.Run("short precomputed key", func(t *testing.T) {
		_, _, _, err := SortBatchWithPrecomputedOrder(proc, input, fs, 1, process.NewAnalyzer(0, false, false, "invalid"), []*vector.Vector{short}, nil)
		assertInvalid(t, err)
	})
	t.Run("nil extra vector", func(t *testing.T) {
		_, _, err := SortBatchWithExtraVectors(proc, input, fs, 1, process.NewAnalyzer(0, false, false, "invalid"), []*vector.Vector{nil})
		assertInvalid(t, err)
	})
	t.Run("short extra vector", func(t *testing.T) {
		_, _, err := SortBatchWithExtraVectors(proc, input, fs, 1, process.NewAnalyzer(0, false, false, "invalid"), []*vector.Vector{short})
		assertInvalid(t, err)
	})

	require.Equal(t, []int8{3, 1, 2}, vector.MustFixedColWithTypeCheck[int8](input.Vecs[0]))
}

func TestSortBatchCancellationAndEvaluationFailureCleanOwnedState(t *testing.T) {
	fs := []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8)}}

	t.Run("cancelled spill admission", func(t *testing.T) {
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		baseCtx := proc.Ctx
		ctx, cancel := context.WithCancel(baseCtx)
		proc.Ctx = ctx
		cancel()
		input := newValuesBatch(proc, []int8{3, 1, 2})
		var result *batch.Batch
		t.Cleanup(func() {
			proc.Ctx = baseCtx
			if result != nil {
				result.Clean(proc.Mp())
			}
			input.Clean(proc.Mp())
			proc.Free()
			require.Zero(t, proc.Mp().CurrNB())
		})
		analyzer := process.NewAnalyzer(0, false, false, "batch-sort-cancel")
		var err error
		result, err = SortBatch(proc, input, fs, 1, analyzer)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, result)
		require.Equal(t, []int8{3, 1, 2}, vector.MustFixedColWithTypeCheck[int8](input.Vecs[0]))
	})

	t.Run("order expression failure", func(t *testing.T) {
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		input := newValuesBatch(proc, []int8{3, 1, 2})
		ctr := &container{
			executors:       []colexec.ExpressionExecutor{&failingExecutor{}},
			spillColPos:     []int32{-1},
			spillKeyIndexes: []int{0},
		}
		t.Cleanup(func() {
			cleanupBatchSortContainer(proc, ctr)
			if input != nil {
				input.Clean(proc.Mp())
			}
			proc.Free()
			require.Zero(t, proc.Mp().CurrNB())
		})
		before := proc.Mp().CurrNB()
		desc, nullsLast := orderFlags(fs)
		err := ctr.appendSortedChunk(proc, input, 0, input.RowCount(), desc, nullsLast, fs, process.NewAnalyzer(0, false, false, "batch-sort-eval-failure"))
		require.Error(t, err)
		require.Equal(t, before, proc.Mp().CurrNB())
		require.Equal(t, []int8{3, 1, 2}, vector.MustFixedColWithTypeCheck[int8](input.Vecs[0]))
	})
}

func cleanupBatchSortContainer(proc *process.Process, ctr *container) {
	if ctr == nil {
		return
	}
	for i, bat := range ctr.batchList {
		if bat == nil {
			continue
		}
		var orderCols []*vector.Vector
		if i < len(ctr.orderCols) {
			orderCols = ctr.orderCols[i]
		}
		freeOrderColumns(proc.Mp(), bat, orderCols)
		bat.Clean(proc.Mp())
	}
	ctr.batchList = nil
	ctr.orderCols = nil
	ctr.cleanupSpill(proc)
	if ctr.buf != nil {
		ctr.buf.Clean(proc.Mp())
		ctr.buf = nil
	}
	for i, executor := range ctr.executors {
		if executor != nil {
			executor.Free()
			ctr.executors[i] = nil
		}
	}
}
