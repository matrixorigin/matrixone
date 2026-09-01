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
