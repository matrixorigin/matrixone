//go:build gpu

// Copyright 2023 Matrix Origin
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

package device

import (
	//"fmt"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	mobf "github.com/matrixorigin/matrixone/pkg/vectorindex/brute_force"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

func TestGpu(t *testing.T) {

	dim := 128
	dsize := 1024
	nlist := 128
	vecs := make([][]float32, dsize)
	for i := range vecs {
		vecs[i] = make([]float32, dim)
		for j := range vecs[i] {
			vecs[i][j] = rand.Float32()
		}
	}

	c, err := NewKMeans[float32](vecs, nlist, 10, 0, metric.Metric_L2Distance, 0, false, 0)
	require.NoError(t, err)

	centers, err := c.Cluster()
	require.NoError(t, err)

	_, ok := centers.([][]float32)
	require.True(t, ok)

	/*
		for k, center := range centroids {
			fmt.Printf("center[%d] = %v\n", k, center)
		}
	*/
}

func TestIVFAndBruteForce(t *testing.T) {

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)
	dimension := uint(128)
	ncpu := uint(1)
	limit := uint(1)
	elemsz := uint(4) // float32

	dsize := 100000
	nlist := 128
	vecs := make([][]float32, dsize)
	for i := range vecs {
		vecs[i] = make([]float32, dimension)
		for j := range vecs[i] {
			vecs[i][j] = rand.Float32()
		}
	}

	c, err := NewKMeans[float32](vecs, nlist, 10, 0, metric.Metric_L2Distance, 0, false, 0)
	require.NoError(t, err)

	centers, err := c.Cluster()
	require.NoError(t, err)

	centroids, ok := centers.([][]float32)
	require.True(t, ok)

	/*
		for k, center := range centroids {
			fmt.Printf("center[%d] = %v\n", k, center)
		}
	*/

	queries := vecs[:8192]
	idx, err := mobf.NewBruteForceIndex[float32](centroids, dimension, metric.Metric_L2sqDistance, elemsz)
	require.NoError(t, err)
	defer idx.Destroy()

	err = idx.Load(nil)
	require.NoError(t, err)

	rt := vectorindex.RuntimeConfig{Limit: limit, NThreads: ncpu}

	var wg sync.WaitGroup

	for n := 0; n < 4; n++ {

		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				keys, distances, err := idx.Search(sqlproc, queries, rt)
				require.NoError(t, err)

				keys_i64, ok := keys.([]int64)
				require.Equal(t, ok, true)

				for j, key := range keys_i64 {
					require.Equal(t, key, int64(j))
					require.Equal(t, distances[j], float64(0))
				}
				// fmt.Printf("keys %v, dist %v\n", keys, distances)
			}
		}()
	}

	wg.Wait()

}
