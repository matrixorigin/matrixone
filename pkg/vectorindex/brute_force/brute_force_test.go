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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
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

func TestGoBruteForceConcurrent(t *testing.T) {

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

	idx, err := NewGoBruteForceIndex[float32](dataset, dimension, metric.Metric_L2sqDistance, elemsz)
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
