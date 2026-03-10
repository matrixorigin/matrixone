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
