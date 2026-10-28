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
	if width == 1 {
		return "width_one"
	}
	if width == 128 {
		return "width_128"
	}
	return "width_larger_than_partition"
}

func TestLocalRankIndexedCrossChunk(t *testing.T) {
	const n = 8205
	for _, width := range []int{0, 128} {
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
			index, err := ctr.rankIndex(proc, vec, 0, n, 0, 0)
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
		index, err := ctr.rankIndex(proc, vec, 0, n, 0, 0)
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
	index, err := ctr.rankIndex(first, vec, 0, 300, 0, 0)
	require.NoError(t, err)
	require.NotNil(t, index)
	require.Positive(t, firstGeneration.Snapshot().Used)
	index, err = ctr.rankIndex(second, vec, 0, 299, 0, 0)
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
			mp := mpool.MustNewZero()
			proc := testutil.NewProcessWithMPool(t, "", mp)
			defer proc.Free()
			budget := process.MustNewExecutionResourceBudget(tc.limit, tc.limit)
			generation, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			defer generation.Close()
			require.NoError(t, proc.SetExecutionResourceBudgetForTesting(generation))
			registry, err := mpool.NewAllocationAccountRegistry(1, 32)
			require.NoError(t, err)
			account, err := registry.OpenWithController(1<<20, generation)
			require.NoError(t, err)
			defer func() { _, _, err := registry.CompleteTerminal(account); require.NoError(t, err) }()
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
			index, err := ctr.rankIndex(proc, vec, 0, len(values), 0, 0)
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
