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

package window

import (
	"context"
	"math"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestLocalRankIndexedAgainstOracle(t *testing.T) {
	for _, width := range []int{0, 1, 128, 500} {
		t.Run(fmtRankWidth(width), func(t *testing.T) {
			const n = 384
			values := make([]float64, n)
			nulls := make([]bool, n)
			for i := range values {
				values[i] = float64((i*59)%31 - 15)
				nulls[i] = i%29 == 0
				if i%37 == 0 {
					values[i] = math.NaN()
				}
				if i%41 == 0 {
					values[i] = math.Copysign(0, -1)
				}
				if i%47 == 0 {
					values[i] = math.Inf(1)
				}
			}
			mp := mpool.MustNewZero()
			bat := localRankBatch(mp, n, float64RankVec(t, mp, values, nulls))
			args := []*plan.Expr{newColExprWithType(0, types.T_float64.ToType())}
			name := "cumrank"
			if width != 0 {
				name = "mrank"
				args = append(args, intervalInt64(int64(width)))
			}
			spec := localRankSpec(name, args, -1, types.Type{}, nil)
			got, gotNulls, err := runLocalRankWindow(t, spec, []*batch.Batch{bat}, mp)
			require.NoError(t, err)
			require.Equal(t, nulls, gotNulls)
			for row := range values {
				if nulls[row] {
					continue
				}
				start := 0
				if width != 0 {
					start = max(0, row-width+1)
				}
				want := uint64(1)
				for i := start; i <= row; i++ {
					if !nulls[i] && values[i] < values[row] {
						want++
					}
				}
				require.Equal(t, want, got[row], "row %d", row)
			}
			require.Zero(t, mp.CurrNB())
		})
	}
}

func fmtRankWidth(width int) string {
	if width == 0 {
		return "cumulative"
	}
	return "width_" + strconv.Itoa(width)
}

func TestLocalRankIndexedCrossChunk(t *testing.T) {
	const n = 2*colexec.DefaultBatchSize + 13
	for _, width := range []int{0, 128, colexec.DefaultBatchSize + 1, n + 1} {
		t.Run(fmtRankWidth(width), func(t *testing.T) {
			values := make([]int32, n)
			nulls := make([]bool, n)
			for i := range values {
				values[i], nulls[i] = int32(i), i%71 == 0
			}
			mp := mpool.MustNewZero()
			bat := localRankBatch(mp, n, int32RankVec(t, mp, values, nulls))
			args := []*plan.Expr{newColExprWithType(0, types.T_int32.ToType())}
			name := "cumrank"
			if width != 0 {
				name = "mrank"
				args = append(args, intervalInt64(int64(width)))
			}
			spec := localRankSpec(name, args, -1, types.Type{}, nil)
			got, gotNulls, err := runLocalRankWindow(t, spec, []*batch.Batch{bat}, mp)
			require.NoError(t, err)
			require.Equal(t, nulls, gotNulls)
			for row := range values {
				if nulls[row] {
					continue
				}
				start := 0
				if width != 0 {
					start = max(0, row-width+1)
				}
				want := uint64(row - start + 1)
				// Every position divisible by 71 is NULL.
				want -= uint64(row/71 - (start-1)/71)
				if start == 0 {
					want--
				}
				require.Equal(t, want, got[row], "row %d", row)
			}
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestLocalRankIndexDemandGrowth(t *testing.T) {
	const start, n, chunk = 17, 512, 64
	for _, nullPrefix := range []bool{false, true} {
		for _, width := range []int{0, 128, 300, 1024} {
			t.Run(strconv.FormatBool(nullPrefix)+"/"+fmtRankWidth(width), func(t *testing.T) {
				proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
				defer proc.Free()
				values, nulls := make([]float64, start+n), make([]bool, start+n)
				for i := range values {
					values[i], nulls[i] = float64(i%13-6), i%31 == 0 || (nullPrefix && i < start+128)
					if i%37 == 0 {
						values[i] = math.NaN()
					}
				}
				vec := float64RankVec(t, proc.Mp(), values, nulls)
				defer vec.Free(proc.Mp())
				ctr := &container{}
				defer ctr.freeLocalRankIndex()
				indexedRows := 0
				for row := start; row < start+n; row++ {
					demand := min(start+n, start+((row-start)/chunk+1)*chunk)
					index, err := ctr.rankIndex(proc, vec, start, start+n, row, uint64(width), demand)
					require.NoError(t, err)
					require.LessOrEqual(t, ctr.localRank.horizon-start, 2*(demand-start))
					if row < start+chunk || (nullPrefix && row < start+128) {
						require.Nil(t, index)
					}
					if index == nil {
						continue
					}
					indexedRows++
					got := index.advance(row)
					if nulls[row] {
						continue
					}
					left := start
					if width != 0 {
						left = max(start, row-width+1)
					}
					var want uint64
					for i := left; i <= row; i++ {
						if !nulls[i] && values[i] < values[row] {
							want++
						}
					}
					require.Equal(t, want, got, "row %d", row)
				}
				require.Positive(t, indexedRows, "short/NULL prefixes must not permanently disable indexing")
			})
		}
	}
}

// This private ledger models available query/CN capacity without timing or
// changing global limits. Callers register their vector/operator cleanup later.
func localRankTestBudget(t *testing.T, limit uint64) (*process.Process, *process.ExecutionResourceGeneration, *mpool.AllocationAccount) {
	t.Helper()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	t.Cleanup(proc.Free)
	budget := process.MustNewExecutionResourceBudget(limit, limit)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	t.Cleanup(generation.Close)
	require.NoError(t, proc.SetExecutionResourceBudgetForTesting(generation))
	registry, err := mpool.NewAllocationAccountRegistry(1, 32)
	require.NoError(t, err)
	account, err := registry.OpenWithController(limit, generation)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _, err := registry.CompleteTerminal(account)
		require.NoError(t, err)
		require.Zero(t, generation.Used())
		require.Zero(t, proc.Mp().CurrNB())
	})
	return proc, generation, account
}

func localRankAccountedWindow(t *testing.T, n, width int, withNulls bool, limit uint64) (*Window, *process.Process, *process.ExecutionResourceGeneration) {
	t.Helper()
	proc, generation, account := localRankTestBudget(t, limit)
	selection, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerOrder, 1, 2, 3, 4)
	require.NoError(t, err)
	input, err := vector.NewOffHeapVecWithTypeAndAllocation(types.T_int32.ToType(), selection)
	require.NoError(t, err)
	t.Cleanup(func() {
		if input != nil {
			input.Free(proc.Mp())
		}
	})
	values, nulls := make([]int32, n), make([]bool, n)
	for i := range values {
		values[i], nulls[i] = int32(i), withNulls && i%7 == 0
	}
	require.NoError(t, vector.AppendFixedList(input, values, nulls, proc.Mp()))
	bat := localRankBatch(proc.Mp(), n, input)
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	input = nil // ownership transferred to the mock child
	t.Cleanup(func() { op.Free(proc, false, nil) })
	args, name := []*plan.Expr{newColExprWithType(0, types.T_int32.ToType())}, "cumrank"
	if width != 0 {
		args, name = append(args, intervalInt64(int64(width))), "mrank"
	}
	spec := localRankSpec(name, args, -1, types.Type{}, nil)
	arg := &Window{WinSpecList: []*plan.Expr{spec}, Aggs: []aggexec.AggFuncExecExpression{
		aggexec.MakeAggFunctionExpression(0, false, args, nil),
	}}
	t.Cleanup(func() {
		arg.Free(proc, false, nil)
		require.NoError(t, arg.ClearAllocationAccount(account))
	})
	arg.AppendChild(op)
	require.NoError(t, arg.SetAllocationAccount(account))
	require.NoError(t, arg.Prepare(proc))
	return arg, proc, generation
}

func TestLocalRankIndexOutputBudget(t *testing.T) {
	const n = 300
	for _, width := range []int{0, 128} {
		for _, tc := range []struct {
			name  string
			limit uint64
			nulls bool
		}{
			{"admission_fallback", 10 << 10, false},
			{"output_fallback", 12 << 10, false},
			{"null_output_fallback", 12 << 10, true},
			{"indexed_control", 32 << 10, true},
		} {
			t.Run(fmtRankWidth(width)+"/"+tc.name, func(t *testing.T) {
				arg, proc, _ := localRankAccountedWindow(t, n, width, tc.nulls, tc.limit)
				res, err := vm.Exec(arg, proc)
				require.NoError(t, err)
				require.Equal(t, n, res.Batch.RowCount())
				require.Nil(t, arg.ctr.localRank, "completed partition must release its optional state")
				vec := res.Batch.Vecs[1]
				got := vector.MustFixedColWithTypeCheck[uint64](vec)
				for row := range got {
					isNull := tc.nulls && row%7 == 0
					require.Equal(t, isNull, vec.IsNull(uint64(row)))
					if isNull {
						continue
					}
					left, want := 0, uint64(1)
					if width != 0 {
						left = max(0, row-width+1)
					}
					for i := left; i < row; i++ {
						if !tc.nulls || i%7 != 0 {
							want++
						}
					}
					require.Equal(t, want, got[row], "row %d", row)
				}
			})
		}
	}
}

func TestLocalRankIndexLaterOutputPressure(t *testing.T) {
	const n = 3*colexec.DefaultBatchSize + 13
	for _, width := range []int{0, 128} {
		t.Run(fmtRankWidth(width), func(t *testing.T) {
			arg, proc, generation := localRankAccountedWindow(t, n, width, false, 4<<20)
			for chunk := 1; chunk <= 3; chunk++ {
				res, err := vm.Exec(arg, proc)
				require.NoError(t, err)
				require.Equal(t, colexec.DefaultBatchSize, res.Batch.RowCount())
				require.NotNil(t, arg.ctr.localRank)
				require.LessOrEqual(t, arg.ctr.localRank.horizon, 2*chunk*colexec.DefaultBatchSize)
				if chunk == 1 {
					require.Equal(t, colexec.DefaultBatchSize, arg.ctr.localRank.horizon)
				}
			}
			// Hold all remaining headroom after consuming the previous output.
			// The fourth Call reuses the index and must yield it for its output.
			arg.ctr.cleanOutput(proc.Mp())
			reservation, err := generation.ReserveTransientMemory(generation.Cap() - generation.Used())
			require.NoError(t, err)
			defer reservation.Release()
			res, err := vm.Exec(arg, proc)
			require.NoError(t, err)
			require.Equal(t, 13, res.Batch.RowCount())
			for i, got := range vector.MustFixedColWithTypeCheck[uint64](res.Batch.Vecs[1]) {
				want := uint64(3*colexec.DefaultBatchSize + i + 1)
				if width != 0 {
					want = uint64(width)
				}
				require.Equal(t, want, got)
			}
			require.Nil(t, arg.ctr.localRank)
		})
	}
}

func TestLocalRankIndexResetReuse(t *testing.T) {
	const n = colexec.DefaultBatchSize + 1
	arg, proc, _ := localRankAccountedWindow(t, n, 0, false, 2<<20)
	_, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.NotNil(t, arg.ctr.localRank)
	arg.Reset(proc, false, nil)
	require.Nil(t, arg.ctr.localRank)
	child := arg.GetChildren(0).(*colexec.MockOperator)
	child.Reset(proc, false, nil)
	input := child.GetBatchs()[0]
	values := make([]int32, n)
	for i := range values {
		values[i] = int32(n - i)
	}
	require.NoError(t, vector.AppendFixedList(input.Vecs[0], values, nil, proc.Mp()))
	input.SetRowCount(n)
	require.NoError(t, arg.Prepare(proc))
	rows := 0
	for {
		res, err := vm.Exec(arg, proc)
		require.NoError(t, err)
		if res.Batch == nil {
			break
		}
		rows += res.Batch.RowCount()
		for _, got := range vector.MustFixedColWithTypeCheck[uint64](res.Batch.Vecs[1]) {
			require.Equal(t, uint64(1), got)
		}
	}
	require.Equal(t, n, rows)
	require.Nil(t, arg.ctr.localRank)
}

func TestLocalRankIndexAppendFailure(t *testing.T) {
	for _, name := range []string{"null_retry", "retry_rejected", "canceled", "sealed"} {
		t.Run(name, func(t *testing.T) {
			proc, generation, account := localRankTestBudget(t, 64<<10)
			ctr := &container{}
			require.NoError(t, ctr.allocation.Set(account))
			defer ctr.freeLocalRankIndex()
			vec := int32RankVec(t, proc.Mp(), make([]int32, 256), nil)
			defer vec.Free(proc.Mp())
			_, err := ctr.rankIndex(proc, vec, 0, 256, 0, 0, 256)
			require.NoError(t, err)
			result, err := ctr.allocation.NewVector(types.T_uint64.ToType())
			require.NoError(t, err)
			defer result.Free(proc.Mp())
			if name == "retry_rejected" {
				require.NoError(t, vector.AppendFixedList(result, make([]uint64, 1024), nil, proc.Mp()))
			}
			length := result.Length()
			reservation, err := generation.ReserveTransientMemory(generation.Cap() - generation.Used())
			require.NoError(t, err)
			defer reservation.Release()
			if name == "canceled" {
				ctx, cancel := context.WithCancel(proc.Ctx)
				cancel()
				proc.Ctx = ctx
			} else if name == "sealed" {
				generation.Close()
			}
			err = ctr.appendLocalRankResult(proc, result, 7, name == "null_retry")
			switch name {
			case "null_retry":
				require.NoError(t, err)
				require.Equal(t, length+1, result.Length())
				require.True(t, result.IsNull(uint64(length)))
			case "retry_rejected":
				require.True(t, mpool.IsRetryableAllocationCapacity(err), "%v", err)
			case "canceled":
				require.ErrorIs(t, err, context.Canceled)
			case "sealed":
				require.ErrorIs(t, err, mpool.ErrAllocationAccountSealed)
			}
			if err != nil {
				require.Equal(t, length, result.Length(), "failed append must not publish a row")
			}
			require.Equal(t, name == "null_retry" || name == "retry_rejected", ctr.localRank.fallback)
		})
	}
}

func TestLocalRankIndexSeedCancellation(t *testing.T) {
	for _, width := range []uint64{0, 128} {
		t.Run(strconv.FormatUint(width, 10), func(t *testing.T) {
			proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			// Cumulative cancellation occurs during frequency seeding; width 128
			// completes seeding and cancels during the linear parent-summing pass.
			proc.Ctx = newCountedCancelAfterDoneContext(proc.Ctx, 2)
			index := localRankIndex{width: width, ranks: make([]int, 2500), tree: make([]uint64, 2501)}
			require.ErrorIs(t, index.seed(proc, 2048), context.Canceled)
		})
	}
}

func TestLocalRankIndexedPartitionGenerations(t *testing.T) {
	const n = 256
	mp := mpool.MustNewZero()
	batches := make([]*batch.Batch, 2)
	for part := range batches {
		values, keys := make([]int32, n), make([]int32, n)
		for i := range values {
			values[i], keys[i] = int32(i), int32(part)
			if part == 1 {
				values[i] = int32(n - i)
			}
		}
		batches[part] = localRankBatch(mp, n, int32RankVec(t, mp, values, nil), int32RankVec(t, mp, keys, nil))
	}
	spec := localRankSpec("cumrank", []*plan.Expr{newColExprWithType(0, types.T_int32.ToType())},
		-1, types.Type{}, []*plan.Expr{newColExprWithType(1, types.T_int32.ToType())})
	got, _, err := runLocalRankWindow(t, spec, batches, mp)
	require.NoError(t, err)
	require.Len(t, got, 2*n)
	for i := 0; i < n; i++ {
		require.Equal(t, uint64(i+1), got[i])
		require.Equal(t, uint64(1), got[n+i])
	}
	require.Zero(t, mp.CurrNB())
}

func TestLocalRankIndexAllNull(t *testing.T) {
	const n = 100000
	for _, constant := range []bool{false, true} {
		t.Run(map[bool]string{false: "bitmap", true: "constant"}[constant], func(t *testing.T) {
			mp := mpool.MustNewZero()
			proc := testutil.NewProcessWithMPool(t, "", mp)
			defer proc.Free()
			var vec *vector.Vector
			if constant {
				vec = vector.NewConstNull(types.T_int32.ToType(), n, mp)
			} else {
				nulls := make([]bool, n)
				for i := range nulls {
					nulls[i] = true
				}
				vec = int32RankVec(t, mp, make([]int32, n), nulls)
			}
			defer vec.Free(mp)
			ctr := &container{}
			index, err := ctr.rankIndex(proc, vec, 0, n, 0, 0, n)
			require.NoError(t, err)
			require.Nil(t, index)
			require.True(t, ctr.localRank.fallback)
			require.Zero(t, ctr.localRank.charge)
		})
	}

	t.Run("invalid width still fails for NULL values", func(t *testing.T) {
		mp := mpool.MustNewZero()
		bat := localRankBatch(mp, n, vector.NewConstNull(types.T_int32.ToType(), n, mp), constInt64Vec(t, mp, n, 0))
		spec := localRankSpec("mrank", []*plan.Expr{
			newColExprWithType(0, types.T_int32.ToType()), newColExprWithType(1, types.T_int64.ToType()),
		}, -1, types.Type{}, nil)
		_, _, err := runLocalRankWindow(t, spec, []*batch.Batch{bat}, mp)
		require.Error(t, err)
		require.Zero(t, mp.CurrNB())
	})

	t.Run("cancellation during bitmap inspection", func(t *testing.T) {
		mp := mpool.MustNewZero()
		proc := testutil.NewProcessWithMPool(t, "", mp)
		defer proc.Free()
		proc.Ctx = newCountedCancelAfterDoneContext(proc.Ctx, 2)
		nulls := make([]bool, n)
		for i := range nulls {
			nulls[i] = true
		}
		vec := int32RankVec(t, mp, make([]int32, n), nulls)
		defer vec.Free(mp)
		ctr := &container{}
		index, err := ctr.rankIndex(proc, vec, 0, n, 0, 0, n)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, index)
		require.Nil(t, ctr.localRank)
	})
}

func TestLocalRankIndexRejectsMismatchedGeneration(t *testing.T) {
	mp := mpool.MustNewZero()
	first := testutil.NewProcessWithMPool(t, "first", mp)
	defer first.Free()
	second := testutil.NewProcessWithMPool(t, "second", mp)
	defer second.Free()
	firstBudget := process.MustNewExecutionResourceBudget(1<<20, 1<<20)
	firstGeneration, err := firstBudget.OpenGeneration(1)
	require.NoError(t, err)
	defer firstGeneration.Close()
	secondBudget := process.MustNewExecutionResourceBudget(1<<20, 1<<20)
	secondGeneration, err := secondBudget.OpenGeneration(1)
	require.NoError(t, err)
	defer secondGeneration.Close()
	require.NoError(t, first.SetExecutionResourceBudgetForTesting(firstGeneration))
	require.NoError(t, second.SetExecutionResourceBudgetForTesting(secondGeneration))
	registry, err := mpool.NewAllocationAccountRegistry(1, 32)
	require.NoError(t, err)
	account, err := registry.OpenWithController(1<<20, firstGeneration)
	require.NoError(t, err)
	defer func() { _, _, err := registry.CompleteTerminal(account); require.NoError(t, err) }()
	vec := int32RankVec(t, mp, make([]int32, 300), nil)
	defer vec.Free(mp)
	ctr := &container{}
	require.NoError(t, ctr.allocation.Set(account))
	defer ctr.freeLocalRankIndex()
	index, err := ctr.rankIndex(first, vec, 0, 300, 0, 0, 300)
	require.NoError(t, err)
	require.NotNil(t, index)
	require.Positive(t, firstGeneration.Snapshot().Used)
	index, err = ctr.rankIndex(second, vec, 0, 299, 0, 0, 299)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountMismatch)
	require.Nil(t, index)
	require.Nil(t, ctr.localRank)
	require.Zero(t, firstGeneration.Snapshot().Used)
	require.Zero(t, secondGeneration.Snapshot().Used)
}

func TestLocalRankIndexCapacityAndCancellation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		limit    uint64
		cancelAt int32
		close    bool
	}{
		{name: "capacity_fallback", limit: 1024},
		{name: "accounted_release", limit: 1 << 20},
		{name: "canceled_construction", limit: 1 << 20, cancelAt: 2},
		{name: "canceled_sort", limit: 1 << 20, cancelAt: 4},
		{name: "closed_generation", limit: 1 << 20, close: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc, generation, account := localRankTestBudget(t, tc.limit)
			mp := proc.Mp()
			values := make([]int32, 2500)
			for i := range values {
				values[i] = int32(i % 13)
			}
			vec := int32RankVec(t, mp, values, nil)
			defer vec.Free(mp)
			ctr := &container{}
			require.NoError(t, ctr.allocation.Set(account))
			defer ctr.freeLocalRankIndex()
			if tc.cancelAt != 0 {
				proc.Ctx = newCountedCancelAfterDoneContext(proc.Ctx, tc.cancelAt)
			}
			if tc.close {
				generation.Close()
			}
			index, err := ctr.rankIndex(proc, vec, 0, len(values), 0, 0, len(values))
			switch {
			case tc.cancelAt != 0:
				require.ErrorIs(t, err, context.Canceled)
				require.Nil(t, index)
				require.Nil(t, ctr.localRank)
			case tc.close:
				require.ErrorIs(t, err, mpool.ErrAllocationAccountSealed)
				require.Nil(t, index)
			case tc.limit < 1<<20:
				require.NoError(t, err)
				require.Nil(t, index)
				require.True(t, ctr.localRank.fallback)
			default:
				require.NoError(t, err)
				require.NotNil(t, index)
				require.Positive(t, generation.Snapshot().Used)
			}
			ctr.freeLocalRankIndex()
			require.Zero(t, generation.Snapshot().Used)
		})
	}
}
