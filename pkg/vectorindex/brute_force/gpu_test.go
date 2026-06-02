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

package brute_force

import (
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

func TestGpuBruteForce(t *testing.T) {

	dataset := [][]float32{{1, 2, 3}, {3, 4, 5}}
	query := [][]float32{{1, 2, 3}, {3, 4, 5}}
	dimension := uint(3)
	ncpu := uint(8)
	limit := uint(1)
	elemsz := uint(4) // float32

	idx, err := NewGpuBruteForceIndex[float32](dataset, dimension, metric.Metric_L2sqDistance, elemsz, ncpu)
	require.NoError(t, err)
	defer idx.Destroy()

	err = idx.Load(nil)
	require.NoError(t, err)

	rt := vectorindex.RuntimeConfig{Limit: limit, NThreads: 1}

	var wg sync.WaitGroup

	for n := 0; n < 8; n++ {

		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				keys, distances, err := idx.Search(nil, query, rt)
				require.NoError(t, err)

				keys_i64, ok := keys.([]int64)
				require.Equal(t, ok, true)

				for j, key := range keys_i64 {
					require.Equal(t, key, int64(j))
					require.Equal(t, distances[j], float64(0))
				}
			}
		}()
	}

	wg.Wait()

}

func TestGpuBruteForceConcurrent(t *testing.T) {

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)
	dimension := uint(128)
	ncpu := uint(8)
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

	idx, err := NewGpuBruteForceIndex[float32](dataset, dimension, metric.Metric_L2sqDistance, elemsz, ncpu)
	require.NoError(t, err)
	defer idx.Destroy()

	err = idx.Load(nil)
	require.NoError(t, err)

	// limit 3
	{
		rt := vectorindex.RuntimeConfig{Limit: limit, NThreads: 1}

		anykeys, distances, err := idx.Search(sqlproc, query, rt)
		require.NoError(t, err)

		keys := anykeys.([]int64)
		require.Equal(t, int(rt.Limit)*len(query), len(keys))
		for i := range query {
			offset := i * int(rt.Limit)
			require.Equal(t, int64(i), keys[offset])
			require.Equal(t, float64(0), distances[offset])
		}
	}

	// limit 1
	{
		rt := vectorindex.RuntimeConfig{Limit: 1, NThreads: 1}

		anykeys, distances, err := idx.Search(sqlproc, query, rt)
		require.NoError(t, err)

		keys := anykeys.([]int64)
		require.Equal(t, int(rt.Limit)*len(query), len(keys))
		for i := range query {
			offset := i * int(rt.Limit)
			require.Equal(t, int64(i), keys[offset])
			require.Equal(t, float64(0), distances[offset])
		}
	}
}

func TestGpuSearchFloat32(t *testing.T) {
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
	rt := vectorindex.RuntimeConfig{Limit: limit, NThreads: 1}
	elemsz := uint(4)

	indices := []struct {
		name string
		fn   func([][]float32, uint, metric.MetricType, uint, uint) (cache.VectorIndexSearchIf, error)
	}{
		{"GpuBruteForce", NewGpuBruteForceIndex[float32]},
		{"GpuAdhocBruteForce", func(d [][]float32, dim uint, m metric.MetricType, es uint, nt uint) (cache.VectorIndexSearchIf, error) {
			return NewGpuAdhocBruteForceIndex[float32](d, dim, m, es)
		}},
	}

	for _, tc := range indices {
		t.Run(tc.name, func(t *testing.T) {
			idx, err := tc.fn(dataset, dimension, metric.Metric_L2sqDistance, elemsz, 1)
			require.NoError(t, err)
			defer idx.Destroy()

			err = idx.Load(sqlproc)
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
