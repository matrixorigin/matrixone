// Copyright 2021 - 2026 Matrix Origin
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

package objectio

import (
	"context"
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

func TestTopNVectorAppliesDistanceRangeBeforeHeap(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	entries := vector.NewVec(types.New(types.T_array_float32, 1, 0))
	defer entries.Free(mp)
	for _, value := range []float32{1, 2, 3, 4} {
		require.NoError(t, vector.AppendArray(entries, []float32{value}, false, mp))
	}
	require.NoError(t, vector.AppendArray(entries, []float32{float32(math.NaN())}, false, mp))

	top := &IndexReaderTopOp{
		Typ:            types.T_array_float32,
		MetricType:     metric.Metric_L2sqDistance,
		NumVec:         types.ArrayToBytes([]float32{0}),
		Limit:          2,
		LowerBoundType: plan.BoundType_EXCLUSIVE,
		LowerBound:     1,
		UpperBoundType: plan.BoundType_INCLUSIVE,
		UpperBound:     9,
	}
	rows, distances, err := TopNVector(context.Background(), nil, entries, top)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, rows)
	require.Equal(t, []float64{4, 9}, distances)
}

func TestTopNVectorNaNRangeSelectsNothing(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	entries := vector.NewVec(types.New(types.T_array_float32, 1, 0))
	defer entries.Free(mp)
	require.NoError(t, vector.AppendArray(entries, []float32{1}, false, mp))

	top := &IndexReaderTopOp{
		Typ:            types.T_array_float32,
		MetricType:     metric.Metric_L2sqDistance,
		NumVec:         types.ArrayToBytes([]float32{0}),
		Limit:          1,
		UpperBoundType: plan.BoundType_INCLUSIVE,
		UpperBound:     math.NaN(),
	}
	rows, distances, err := TopNVector(context.Background(), nil, entries, top)
	require.NoError(t, err)
	require.Empty(t, rows)
	require.Empty(t, distances)
	require.Empty(t, top.DistHeap)
}

func TestTopNVectorDoesNotMutateSelectedRows(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	entries := vector.NewVec(types.New(types.T_array_float32, 1, 0))
	defer entries.Free(mp)
	require.NoError(t, vector.AppendArray(entries, []float32{1}, false, mp))
	require.NoError(t, vector.AppendArray(entries, []float32(nil), true, mp))
	require.NoError(t, vector.AppendArray(entries, []float32{2}, false, mp))

	selected := []int64{-1, 0, 3, 1, 2}
	wantSelected := append([]int64(nil), selected...)
	top := &IndexReaderTopOp{
		Typ:        types.T_array_float32,
		MetricType: metric.Metric_L2sqDistance,
		NumVec:     types.ArrayToBytes([]float32{0}),
		Limit:      2,
	}
	rows, distances, err := TopNVector(context.Background(), selected, entries, top)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 2}, rows)
	require.Equal(t, []float64{1, 4}, distances)
	require.Equal(t, wantSelected, selected)
}

func TestTopNVectorBoundsDescendingCandidatesByLimit(t *testing.T) {
	const (
		rows  = 4096
		limit = 3
	)

	results := make(vectorTopResultHeap, 0, limit)
	for row := 0; row < rows; row++ {
		// Every successive candidate is better, the adversarial case that used
		// to append all rows before filtering results by the final heap cutoff.
		retainVectorTopResult(&results, limit, vectorTopResult{
			row: int64(row), distance: float64(rows - row), ordinal: row,
		})
	}
	require.Len(t, results, limit)
	require.LessOrEqual(t, cap(results), limit)

	// The bounded collector retains the same winners TopNVector returns for a
	// decreasing distance sequence, and stores no row/distance state for the
	// other 4,093 candidates.
	slices.SortFunc(results, func(left, right vectorTopResult) int {
		return left.ordinal - right.ordinal
	})
	require.Equal(t, []int64{4093, 4094, 4095}, []int64{
		results[0].row, results[1].row, results[2].row,
	})

	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	entries := vector.NewVec(types.New(types.T_array_float32, 1, 0))
	defer entries.Free(mp)
	for row := 0; row < rows; row++ {
		require.NoError(t, vector.AppendArray(entries, []float32{float32(rows - row)}, false, mp))
	}
	top := &IndexReaderTopOp{
		Typ:        types.T_array_float32,
		MetricType: metric.Metric_L2sqDistance,
		NumVec:     types.ArrayToBytes([]float32{0}),
		Limit:      limit,
	}
	winners, _, err := TopNVector(context.Background(), nil, entries, top)
	require.NoError(t, err)
	require.Equal(t, []int64{4093, 4094, 4095}, winners)
}
