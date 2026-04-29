//go:build gpu

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
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/stretchr/testify/require"
)

// gpuWorkSize returns nX*nY*dim, the work-unit count used to decide GPU vs CPU.
func gpuWorkSize(nX, nY, dim int) uint64 {
	return uint64(nX) * uint64(nY) * uint64(dim)
}

// TestBatchArrayDistanceSync_GPU_L2sq exercises the GPU path for Metric_L2sqDistance.
// GPUThresholdSQL = 4MB/4 = 1,048,576. With dim=512 we need N ≥ 2049; use 2100 for safety.
func TestBatchArrayDistanceSync_GPU_L2sq(t *testing.T) {
	const dim = 512
	const N = 2100
	require.GreaterOrEqual(t,
		gpuWorkSize(1, N, dim), metric.GPUThresholdSQL,
		"work size must exceed GPUThresholdSQL to exercise the GPU path")

	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	rng := rand.New(rand.NewSource(42))
	query := make([]float32, dim)
	for i := range query {
		query[i] = rng.Float32()
	}
	rows := make([][]float32, N)
	for i := range rows {
		rows[i] = make([]float32, dim)
		for j := range rows[i] {
			rows[i][j] = rng.Float32()
		}
	}

	constVec := makeConstArrayVec[float32](t, mp, query, N)
	colVec := makeColArrayVec[float32](t, mp, types.T_array_float32.ToType(), rows)

	gpuDist, ok, err := batchArrayDistanceSync[float32](
		[]*vector.Vector{constVec, colVec}, N, metric.Metric_L2sqDistance)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, N, len(gpuDist))

	// Reference: CPU pairwise
	x := [][]float32{query}
	cpuDist, err := metric.GoPairWiseDistance(x, rows, metric.Metric_L2sqDistance)
	require.NoError(t, err)
	require.Equal(t, N, len(cpuDist))

	for i := range cpuDist {
		require.True(t, approxEqF32(gpuDist[i], cpuDist[i]),
			"row %d: GPU=%v CPU=%v", i, gpuDist[i], cpuDist[i])
	}
}

// TestBatchArrayDistanceSync_GPU_InnerProduct exercises the GPU path for Metric_InnerProduct.
func TestBatchArrayDistanceSync_GPU_InnerProduct(t *testing.T) {
	const dim = 512
	const N = 2100

	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	rng := rand.New(rand.NewSource(7))
	query := make([]float32, dim)
	for i := range query {
		query[i] = rng.Float32()
	}
	rows := make([][]float32, N)
	for i := range rows {
		rows[i] = make([]float32, dim)
		for j := range rows[i] {
			rows[i][j] = rng.Float32()
		}
	}

	constVec := makeConstArrayVec[float32](t, mp, query, N)
	colVec := makeColArrayVec[float32](t, mp, types.T_array_float32.ToType(), rows)

	gpuDist, ok, err := batchArrayDistanceSync[float32](
		[]*vector.Vector{constVec, colVec}, N, metric.Metric_InnerProduct)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, N, len(gpuDist))

	x := [][]float32{query}
	cpuDist, err := metric.GoPairWiseDistance(x, rows, metric.Metric_InnerProduct)
	require.NoError(t, err)
	require.Equal(t, N, len(cpuDist))

	for i := range cpuDist {
		require.True(t, approxEqF32(gpuDist[i], cpuDist[i]),
			"row %d: GPU=%v CPU=%v", i, gpuDist[i], cpuDist[i])
	}
}

// TestBatchArrayDistanceSync_GPU_CosineDistance exercises the GPU path for Metric_CosineDistance.
func TestBatchArrayDistanceSync_GPU_CosineDistance(t *testing.T) {
	const dim = 512
	const N = 2100

	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	rng := rand.New(rand.NewSource(13))
	query := make([]float32, dim)
	for i := range query {
		query[i] = rng.Float32()
	}
	rows := make([][]float32, N)
	for i := range rows {
		rows[i] = make([]float32, dim)
		for j := range rows[i] {
			rows[i][j] = rng.Float32()
		}
	}

	constVec := makeConstArrayVec[float32](t, mp, query, N)
	colVec := makeColArrayVec[float32](t, mp, types.T_array_float32.ToType(), rows)

	gpuDist, ok, err := batchArrayDistanceSync[float32](
		[]*vector.Vector{constVec, colVec}, N, metric.Metric_CosineDistance)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, N, len(gpuDist))

	x := [][]float32{query}
	cpuDist, err := metric.GoPairWiseDistance(x, rows, metric.Metric_CosineDistance)
	require.NoError(t, err)
	require.Equal(t, N, len(cpuDist))

	for i := range cpuDist {
		require.True(t, approxEqF32(gpuDist[i], cpuDist[i]),
			"row %d: GPU=%v CPU=%v", i, gpuDist[i], cpuDist[i])
	}
}

// TestBatchArrayDistanceSync_GPU_L2Distance exercises the GPU path for Metric_L2Distance.
func TestBatchArrayDistanceSync_GPU_L2Distance(t *testing.T) {
	const dim = 512
	const N = 2100

	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	rng := rand.New(rand.NewSource(99))
	query := make([]float32, dim)
	for i := range query {
		query[i] = rng.Float32()
	}
	rows := make([][]float32, N)
	for i := range rows {
		rows[i] = make([]float32, dim)
		for j := range rows[i] {
			rows[i][j] = rng.Float32()
		}
	}

	constVec := makeConstArrayVec[float32](t, mp, query, N)
	colVec := makeColArrayVec[float32](t, mp, types.T_array_float32.ToType(), rows)

	gpuDist, ok, err := batchArrayDistanceSync[float32](
		[]*vector.Vector{constVec, colVec}, N, metric.Metric_L2Distance)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, N, len(gpuDist))

	x := [][]float32{query}
	cpuDist, err := metric.GoPairWiseDistance(x, rows, metric.Metric_L2Distance)
	require.NoError(t, err)
	require.Equal(t, N, len(cpuDist))

	for i := range cpuDist {
		require.True(t, approxEqF32(gpuDist[i], cpuDist[i]),
			"row %d: GPU=%v CPU=%v", i, gpuDist[i], cpuDist[i])
	}
}
