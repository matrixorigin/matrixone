// Copyright 2022 Matrix Origin
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

package function

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/stretchr/testify/require"
)

// makeConstArrayVec creates a constant vector holding a single array value repeated length times.
func makeConstArrayVec[T types.RealNumbers](t *testing.T, mp *mpool.MPool, arr []T, length int) *vector.Vector {
	t.Helper()
	b := types.ArrayToBytes[T](arr)
	v, err := vector.NewConstBytes(types.T_array_float32.ToType(), b, length, mp)
	require.NoError(t, err)
	return v
}

// makeConstArrayVec64 is the float64 variant.
func makeConstArrayVec64(t *testing.T, mp *mpool.MPool, arr []float64, length int) *vector.Vector {
	t.Helper()
	b := types.ArrayToBytes[float64](arr)
	v, err := vector.NewConstBytes(types.T_array_float64.ToType(), b, length, mp)
	require.NoError(t, err)
	return v
}

// makeColArrayVec creates a column vector holding one array per row.
func makeColArrayVec[T types.RealNumbers](t *testing.T, mp *mpool.MPool, typ types.Type, rows [][]T) *vector.Vector {
	t.Helper()
	v := vector.NewVec(typ)
	for _, row := range rows {
		require.NoError(t, vector.AppendBytes(v, types.ArrayToBytes[T](row), false, mp))
	}
	return v
}

// approxEqF32 checks float32 equality within a relative/absolute tolerance.
func approxEqF32(a, b float32) bool {
	if a == b {
		return true
	}
	diff := math.Abs(float64(a - b))
	avg := math.Abs(float64(a+b) / 2.0)
	if avg < 1e-9 {
		return diff < 1e-5
	}
	return diff/avg < 1e-4
}

// TestBatchArrayDistanceSync_L2Sq verifies batchArrayDistanceSync with Metric_L2sqDistance
// on a small const-vs-column input (always CPU path).
func TestBatchArrayDistanceSync_L2Sq(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	query := []float32{1, 0, 0}
	rows := [][]float32{{1, 0, 0}, {0, 1, 0}, {0, 0, 1}, {1, 1, 0}}
	N := len(rows)
	// expected: ||query - row||²
	want := []float32{0, 2, 2, 1}

	constVec := makeConstArrayVec[float32](t, mp, query, N)
	colVec := makeColArrayVec[float32](t, mp, types.T_array_float32.ToType(), rows)

	dist, ok, err := batchArrayDistanceSync[float32](
		[]*vector.Vector{constVec, colVec}, N, metric.Metric_L2sqDistance)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, N, len(dist))
	for i, w := range want {
		require.True(t, approxEqF32(dist[i], w), "row %d: got %v want %v", i, dist[i], w)
	}
}

// TestBatchArrayDistanceSync_L2 verifies batchArrayDistanceSync with Metric_L2Distance.
func TestBatchArrayDistanceSync_L2(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	query := []float32{1, 0, 0}
	rows := [][]float32{{1, 0, 0}, {0, 1, 0}, {0, 0, 1}, {1, 1, 0}}
	N := len(rows)
	sqrt2 := float32(math.Sqrt(2))
	want := []float32{0, sqrt2, sqrt2, 1}

	constVec := makeConstArrayVec[float32](t, mp, query, N)
	colVec := makeColArrayVec[float32](t, mp, types.T_array_float32.ToType(), rows)

	dist, ok, err := batchArrayDistanceSync[float32](
		[]*vector.Vector{constVec, colVec}, N, metric.Metric_L2Distance)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, N, len(dist))
	for i, w := range want {
		require.True(t, approxEqF32(dist[i], w), "row %d: got %v want %v", i, dist[i], w)
	}
}

// TestBatchArrayDistanceSync_InnerProduct verifies Metric_InnerProduct.
// The function returns -dot_product (negated for ANN ordering).
func TestBatchArrayDistanceSync_InnerProduct(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	query := []float32{1, 0, 0}
	rows := [][]float32{{1, 0, 0}, {0, 1, 0}, {0, 0, 1}, {1, 1, 0}}
	N := len(rows)
	// -dot(query, row)
	want := []float32{-1, 0, 0, -1}

	constVec := makeConstArrayVec[float32](t, mp, query, N)
	colVec := makeColArrayVec[float32](t, mp, types.T_array_float32.ToType(), rows)

	dist, ok, err := batchArrayDistanceSync[float32](
		[]*vector.Vector{constVec, colVec}, N, metric.Metric_InnerProduct)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, N, len(dist))
	for i, w := range want {
		require.True(t, approxEqF32(dist[i], w), "row %d: got %v want %v", i, dist[i], w)
	}
}

// TestBatchArrayDistanceSync_CosineDistance verifies Metric_CosineDistance.
func TestBatchArrayDistanceSync_CosineDistance(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	query := []float32{1, 0, 0}
	rows := [][]float32{{1, 0, 0}, {0, 1, 0}, {0, 0, 1}, {1, 1, 0}}
	N := len(rows)
	// 1 - cosine_similarity
	oneMinusInvSqrt2 := float32(1.0 - 1.0/math.Sqrt(2))
	want := []float32{0, 1, 1, oneMinusInvSqrt2}

	constVec := makeConstArrayVec[float32](t, mp, query, N)
	colVec := makeColArrayVec[float32](t, mp, types.T_array_float32.ToType(), rows)

	dist, ok, err := batchArrayDistanceSync[float32](
		[]*vector.Vector{constVec, colVec}, N, metric.Metric_CosineDistance)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, N, len(dist))
	for i, w := range want {
		require.True(t, approxEqF32(dist[i], w), "row %d: got %v want %v", i, dist[i], w)
	}
}

// TestBatchArrayDistanceSync_QueryAsSecondArg verifies that the query vector
// can be in ivecs[1] (column in ivecs[0], const in ivecs[1]).
func TestBatchArrayDistanceSync_QueryAsSecondArg(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	query := []float32{1, 0, 0}
	rows := [][]float32{{1, 0, 0}, {0, 1, 0}, {0, 0, 1}, {1, 1, 0}}
	N := len(rows)
	want := []float32{0, 2, 2, 1}

	colVec := makeColArrayVec[float32](t, mp, types.T_array_float32.ToType(), rows)
	constVec := makeConstArrayVec[float32](t, mp, query, N)

	// Note: const is ivecs[1], column is ivecs[0]
	dist, ok, err := batchArrayDistanceSync[float32](
		[]*vector.Vector{colVec, constVec}, N, metric.Metric_L2sqDistance)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, N, len(dist))
	for i, w := range want {
		require.True(t, approxEqF32(dist[i], w), "row %d: got %v want %v", i, dist[i], w)
	}
}

// TestBatchArrayDistanceSync_Float64 verifies that the float64 type uses the CPU pairwise path.
func TestBatchArrayDistanceSync_Float64(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	query := []float64{1, 0, 0}
	rows := [][]float64{{1, 0, 0}, {0, 1, 0}, {0, 0, 1}, {1, 1, 0}}
	N := len(rows)
	want := []float32{0, 2, 2, 1}

	constVec := makeConstArrayVec64(t, mp, query, N)
	colVec := makeColArrayVec[float64](t, mp, types.T_array_float64.ToType(), rows)

	dist, ok, err := batchArrayDistanceSync[float64](
		[]*vector.Vector{constVec, colVec}, N, metric.Metric_L2sqDistance)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, N, len(dist))
	for i, w := range want {
		require.True(t, approxEqF32(dist[i], w), "row %d: got %v want %v", i, dist[i], w)
	}
}

// TestBatchArrayDistanceSync_BothConst verifies that both-const input returns ok=false.
func TestBatchArrayDistanceSync_BothConst(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	b := types.ArrayToBytes[float32]([]float32{1, 0, 0})
	v0, err := vector.NewConstBytes(types.T_array_float32.ToType(), b, 4, mp)
	require.NoError(t, err)
	v1, err := vector.NewConstBytes(types.T_array_float32.ToType(), b, 4, mp)
	require.NoError(t, err)

	_, ok, err := batchArrayDistanceSync[float32](
		[]*vector.Vector{v0, v1}, 4, metric.Metric_L2sqDistance)
	require.NoError(t, err)
	require.False(t, ok, "both-const should return ok=false")
}

// TestBatchArrayDistanceSync_BothCol verifies that column-vs-column input returns ok=false.
func TestBatchArrayDistanceSync_BothCol(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	rows := [][]float32{{1, 0, 0}, {0, 1, 0}}
	v0 := makeColArrayVec[float32](t, mp, types.T_array_float32.ToType(), rows)
	v1 := makeColArrayVec[float32](t, mp, types.T_array_float32.ToType(), rows)

	_, ok, err := batchArrayDistanceSync[float32](
		[]*vector.Vector{v0, v1}, 2, metric.Metric_L2sqDistance)
	require.NoError(t, err)
	require.False(t, ok, "col-vs-col should return ok=false")
}

// TestBatchArrayDistanceSync_NullConst verifies that a null const vector returns ok=false.
func TestBatchArrayDistanceSync_NullConst(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	constVec := vector.NewConstNull(types.T_array_float32.ToType(), 4, mp)
	rows := [][]float32{{1, 0, 0}, {0, 1, 0}, {0, 0, 1}, {1, 1, 0}}
	colVec := makeColArrayVec[float32](t, mp, types.T_array_float32.ToType(), rows)

	_, ok, err := batchArrayDistanceSync[float32](
		[]*vector.Vector{constVec, colVec}, 4, metric.Metric_L2sqDistance)
	require.NoError(t, err)
	require.False(t, ok, "null const should return ok=false")
}

// TestBatchArrayDistanceSync_NullInColumn verifies that a column with nulls returns ok=false.
func TestBatchArrayDistanceSync_NullInColumn(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	query := []float32{1, 0, 0}
	constVec := makeConstArrayVec[float32](t, mp, query, 3)

	typ := types.T_array_float32.ToType()
	colVec := vector.NewVec(typ)
	require.NoError(t, vector.AppendBytes(colVec, types.ArrayToBytes[float32]([]float32{1, 0, 0}), false, mp))
	require.NoError(t, vector.AppendBytes(colVec, nil, true, mp)) // null row
	require.NoError(t, vector.AppendBytes(colVec, types.ArrayToBytes[float32]([]float32{0, 1, 0}), false, mp))

	_, ok, err := batchArrayDistanceSync[float32](
		[]*vector.Vector{constVec, colVec}, 3, metric.Metric_L2sqDistance)
	require.NoError(t, err)
	require.False(t, ok, "column with nulls should return ok=false")
}
