//go:build !gpu

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

package brute_force

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

func TestBruteForce(t *testing.T) {

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	dataset := [][]float32{{1, 2, 3}, {3, 4, 5}}
	query := [][]float32{{1, 2, 3}, {3, 4, 5}}
	dimension := uint(3)
	ncpu := uint(1)
	limit := uint(2)
	elemsz := uint(4) // float32

	idx, err := NewUsearchBruteForceIndex[float32](dataset, dimension, metric.Metric_L2sqDistance, elemsz)
	require.NoError(t, err)

	rt := vectorindex.RuntimeConfig{Limit: limit, NThreads: ncpu}

	keys, distances, err := idx.Search(sqlproc, query, rt)
	require.NoError(t, err)
	fmt.Printf("keys %v, dist %v\n", keys, distances)

}

func TestGoBruteForce(t *testing.T) {

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	dataset := [][]float32{{1, 2, 3}, {3, 4, 5}}
	query := [][]float32{{1, 2, 3}, {3, 4, 5}}
	dimension := uint(3)
	ncpu := uint(1)
	limit := uint(2)
	elemsz := uint(4) // float32

	idx, err := NewGoBruteForceIndex[float32](dataset, dimension, metric.Metric_L2sqDistance, elemsz)
	require.NoError(t, err)

	rt := vectorindex.RuntimeConfig{Limit: limit, NThreads: ncpu}

	keys, distances, err := idx.Search(sqlproc, query, rt)
	require.NoError(t, err)
	fmt.Printf("keys %v, dist %v\n", keys, distances)

}

func runBruteForceConcurrent(t *testing.T, is_usearch bool) {

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)
	dimension := uint(128)
	ncpu := uint(4)
	limit := uint(3)
	elemsz := uint(4) // float32

	dsize := 10000
	dataset := make([][]float32, dsize)
	for i := range dataset {
		dataset[i] = make([]float32, dimension)
		for j := range dataset[i] {
			dataset[i][j] = rand.Float32()
		}
	}

	query := dataset

	var idx cache.VectorIndexSearchIf
	var err error
	if is_usearch {
		idx, err = NewUsearchBruteForceIndex[float32](dataset, dimension, metric.Metric_L2sqDistance, elemsz)
	} else {
		idx, err = NewGoBruteForceIndex[float32](dataset, dimension, metric.Metric_L2sqDistance, elemsz)
	}
	require.NoError(t, err)

	// limit 3
	{
		rt := vectorindex.RuntimeConfig{Limit: limit, NThreads: ncpu}

		anykeys, distances, err := idx.Search(sqlproc, query, rt)
		require.NoError(t, err)

		keys := anykeys.([]int64)
		// fmt.Printf("keys %v, dist %v\n", keys, distances)
		require.Equal(t, int(rt.Limit)*len(query), len(keys))
		for i := range query {
			offset := i * int(rt.Limit)
			require.Equal(t, int64(i), keys[offset])
			require.Equal(t, float64(0), distances[offset])
		}
	}

	// limit 1
	{
		rt := vectorindex.RuntimeConfig{Limit: 1, NThreads: ncpu}

		anykeys, distances, err := idx.Search(sqlproc, query, rt)
		require.NoError(t, err)

		keys := anykeys.([]int64)
		// fmt.Printf("keys %v, dist %v\n", keys, distances)
		require.Equal(t, int(rt.Limit)*len(query), len(keys))
		for i := range query {
			offset := i * int(rt.Limit)
			require.Equal(t, int64(i), keys[offset])
			require.Equal(t, float64(0), distances[offset])
		}
	}

}

func TestGoBruteForceConcurrent(t *testing.T) {
	runBruteForceConcurrent(t, false)
}

func TestUsearchBruteForceConcurrent(t *testing.T) {
	runBruteForceConcurrent(t, true)
}

func TestSearchFloat32(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	dimension := uint(16)
	dsize := 100
	dataset := make([][]float32, dsize)
	for i := range dataset {
		dataset[i] = make([]float32, dimension)
		for j := range dataset[i] {
			dataset[i][j] = rand.Float32()
		}
	}

	qsize := 5
	queries := make([][]float32, qsize)
	for i := range queries {
		queries[i] = make([]float32, dimension)
		for j := range queries[i] {
			queries[i][j] = rand.Float32()
		}
	}

	limit := uint(3)
	rt := vectorindex.RuntimeConfig{Limit: limit, NThreads: 2}
	elemsz := uint(4)

	indices := []struct {
		name string
		fn   func([][]float32, uint, metric.MetricType, uint) (cache.VectorIndexSearchIf, error)
	}{
		{"GoBruteForce", NewGoBruteForceIndex[float32]},
		{"UsearchBruteForce", NewUsearchBruteForceIndex[float32]},
	}

	for _, tc := range indices {
		t.Run(tc.name, func(t *testing.T) {
			idx, err := tc.fn(dataset, dimension, metric.Metric_L2sqDistance, elemsz)
			require.NoError(t, err)

			// 1. Get baseline from standard Search
			keysAny, dists64, err := idx.Search(sqlproc, queries, rt)
			require.NoError(t, err)
			expectedKeys := keysAny.([]int64)

			// 2. Test SearchFloat32
			outKeys := make([]int64, qsize*int(limit))
			outDists := make([]float32, qsize*int(limit))
			err = idx.SearchFloat32(sqlproc, queries, rt, outKeys, outDists)
			require.NoError(t, err)

			// 3. Compare results
			require.Equal(t, expectedKeys, outKeys)
			for i := range dists64 {
				require.InDelta(t, dists64[i], float64(outDists[i]), 1e-5)
			}
		})
	}
}

func TestNewUsearchBruteForceIndexFlattened(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	dimension := uint(3)
	count := uint(2)
	flat := []float32{1, 2, 3, 3, 4, 5}
	elemsz := uint(4)
	limit := uint(2)

	idx, err := NewUsearchBruteForceIndexFlattened[float32](flat, count, dimension, metric.Metric_L2sqDistance, elemsz)
	require.NoError(t, err)
	require.NotNil(t, idx)

	rt := vectorindex.RuntimeConfig{Limit: limit, NThreads: 1}
	query := [][]float32{{1, 2, 3}}
	keys, dists, err := idx.Search(sqlproc, query, rt)
	require.NoError(t, err)
	require.NotNil(t, keys)
	require.Equal(t, 2, len(dists))
}

func TestNewBruteForceIndexHelpers(t *testing.T) {
	dataset := [][]float32{{1, 2, 3}, {3, 4, 5}}
	dimension := uint(3)
	elemsz := uint(4)

	// CPU helper -> Go index
	idx, err := NewBruteForceIndex[float32](dataset, dimension, metric.Metric_L2sqDistance, elemsz, 1)
	require.NoError(t, err)
	require.NotNil(t, idx)

	// Adhoc -> Usearch
	idx2, err := NewAdhocBruteForceIndex[float32](dataset, dimension, metric.Metric_L2sqDistance, elemsz)
	require.NoError(t, err)
	require.NotNil(t, idx2)

	// Adhoc flattened
	flat := []float32{1, 2, 3, 3, 4, 5}
	idx3, err := NewAdhocBruteForceIndexFlattened[float32](flat, 2, dimension, metric.Metric_L2sqDistance, elemsz)
	require.NoError(t, err)
	require.NotNil(t, idx3)

	// Cpu helper directly
	idx4, err := NewCpuBruteForceIndex[float32](dataset, dimension, metric.Metric_L2sqDistance, elemsz)
	require.NoError(t, err)
	require.NotNil(t, idx4)
}

func TestGetUsearchQuantizationFromType(t *testing.T) {
	q, err := GetUsearchQuantizationFromType(float32(0))
	require.NoError(t, err)
	_ = q
	q2, err := GetUsearchQuantizationFromType(float64(0))
	require.NoError(t, err)
	_ = q2
	_, err = GetUsearchQuantizationFromType(int32(0))
	require.Error(t, err)
}

func TestUsearchBruteForceSearchFlattenedQuery(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	dataset := [][]float32{{1, 2, 3}, {3, 4, 5}}
	dimension := uint(3)
	elemsz := uint(4)

	idx, err := NewUsearchBruteForceIndex[float32](dataset, dimension, metric.Metric_L2sqDistance, elemsz)
	require.NoError(t, err)

	// Pass flattened []T as queries (covers the []T branch in Search)
	flat := []float32{1, 2, 3}
	rt := vectorindex.RuntimeConfig{Limit: 1, NThreads: 1}
	keys, dists, err := idx.Search(sqlproc, flat, rt)
	require.NoError(t, err)
	require.NotNil(t, keys)
	require.Equal(t, 1, len(dists))
}

func TestUsearchBruteForceSearchEmptyQuery(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	dataset := [][]float32{{1, 2, 3}, {3, 4, 5}}
	idx, err := NewUsearchBruteForceIndex[float32](dataset, 3, metric.Metric_L2sqDistance, 4)
	require.NoError(t, err)

	rt := vectorindex.RuntimeConfig{Limit: 2, NThreads: 1}
	queries := [][]float32{}
	keys, dists, err := idx.Search(sqlproc, queries, rt)
	require.NoError(t, err)
	require.Nil(t, keys)
	require.Nil(t, dists)
}

func TestUsearchBruteForceSearchBadType(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	dataset := [][]float32{{1, 2, 3}}
	idx, err := NewUsearchBruteForceIndex[float32](dataset, 3, metric.Metric_L2sqDistance, 4)
	require.NoError(t, err)

	rt := vectorindex.RuntimeConfig{Limit: 1, NThreads: 1}
	_, _, err = idx.Search(sqlproc, "wrong type", rt)
	require.Error(t, err)
}

func TestGoBruteForceSearchFloat32_BadType(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	dataset := [][]float32{{1, 2, 3}}
	idx, err := NewGoBruteForceIndex[float32](dataset, 3, metric.Metric_L2sqDistance, 4)
	require.NoError(t, err)

	rt := vectorindex.RuntimeConfig{Limit: 1, NThreads: 1}
	err = idx.SearchFloat32(sqlproc, "wrong type", rt, nil, nil)
	require.Error(t, err)
}

func TestGoBruteForceSearch_LimitZero(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	dataset := [][]float32{{1, 2, 3}}
	queries := [][]float32{{1, 2, 3}}
	idx, err := NewGoBruteForceIndex[float32](dataset, 3, metric.Metric_L2sqDistance, 4)
	require.NoError(t, err)

	rt := vectorindex.RuntimeConfig{Limit: 0, NThreads: 1}
	keys, dists, err := idx.Search(sqlproc, queries, rt)
	require.NoError(t, err)
	require.Equal(t, []int64{}, keys)
	require.Equal(t, []float64{}, dists)

	// SearchFloat32 limit==0 returns nil error w/o writing
	err = idx.SearchFloat32(sqlproc, queries, rt, nil, nil)
	require.NoError(t, err)
}

func TestUsearchBruteForceLifecycle(t *testing.T) {
	dataset := [][]float32{{1, 2, 3}, {3, 4, 5}}
	idx, err := NewUsearchBruteForceIndex[float32](dataset, 3, metric.Metric_L2sqDistance, 4)
	require.NoError(t, err)

	bf := idx.(*UsearchBruteForceIndex[float32])
	require.NoError(t, bf.Load(nil))
	require.NoError(t, bf.UpdateConfig(nil))

	// Destroy with allocator
	bf.Destroy()
	// Calling again is safe
	bf.Destroy()
}

func TestGoBruteForceLifecycle(t *testing.T) {
	dataset := [][]float32{{1, 2, 3}}
	idx, err := NewGoBruteForceIndex[float32](dataset, 3, metric.Metric_L2sqDistance, 4)
	require.NoError(t, err)

	bf := idx.(*GoBruteForceIndex[float32])
	require.NoError(t, bf.Load(nil))
	require.NoError(t, bf.UpdateConfig(nil))
	bf.Destroy()
}

func TestGoBruteForceHeapLogic(t *testing.T) {
	// Generate random dataset
	dsize := 1000
	dimension := uint(16)
	dataset := make([][]float32, dsize)
	for i := range dataset {
		dataset[i] = make([]float32, dimension)
		for j := range dataset[i] {
			dataset[i][j] = rand.Float32()
		}
	}

	qsize := 10
	queries := make([][]float32, qsize)
	for i := range queries {
		queries[i] = make([]float32, dimension)
		for j := range queries[i] {
			queries[i][j] = rand.Float32()
		}
	}

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)
	elemsz := uint(4)

	idx, err := NewGoBruteForceIndex[float32](dataset, dimension, metric.Metric_L2sqDistance, elemsz)
	require.NoError(t, err)

	limits := []uint{1, 5, 50, 1000}

	for _, limit := range limits {
		rt := vectorindex.RuntimeConfig{Limit: limit, NThreads: 2}
		keysAny, dists, err := idx.Search(sqlproc, queries, rt)
		require.NoError(t, err)

		keys := keysAny.([]int64)
		require.Equal(t, int(limit)*qsize, len(keys))
		require.Equal(t, int(limit)*qsize, len(dists))

		// Verify correctness for each query
		for i := 0; i < qsize; i++ {
			type res struct {
				id   int64
				dist float64
			}
			allRes := make([]res, dsize)
			for j := 0; j < dsize; j++ {
				d, _ := metric.L2DistanceSq(queries[i], dataset[j])
				allRes[j] = res{id: int64(j), dist: float64(d)}
			}

			// Sort by distance ascending, then ID ascending for stability
			sort.Slice(allRes, func(a, b int) bool {
				if allRes[a].dist == allRes[b].dist {
					return allRes[a].id < allRes[b].id
				}
				return allRes[a].dist < allRes[b].dist
			})

			// Check top K
			for j := 0; j < int(limit); j++ {
				offset := i*int(limit) + j
				expectedDist := allRes[j].dist
				actualDist := dists[offset]

				require.InDeltaf(t, expectedDist, actualDist, 1e-5, "Distance mismatch at query %d, rank %d (limit %d)", i, j, limit)
			}

			// Check that actual results are sorted
			for j := 1; j < int(limit); j++ {
				offset := i*int(limit) + j
				require.Truef(t, dists[offset] >= dists[offset-1], "Results not sorted at query %d, rank %d", i, j)
			}
		}
	}
}
