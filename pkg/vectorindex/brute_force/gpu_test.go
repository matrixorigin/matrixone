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
	//"fmt"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/stretchr/testify/require"
)

func TestGpuBruteForce(t *testing.T) {

	dataset := [][]float32{{1, 2, 3}, {3, 4, 5}}
	query := [][]float32{{1, 2, 3}, {3, 4, 5}}
	dimension := uint(3)
	ncpu := uint(1)
	limit := uint(1)
	elemsz := uint(4) // float32

	idx, err := NewGpuBruteForceIndex[float32](dataset, dimension, metric.Metric_L2sqDistance, elemsz)
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
				keys, distances, err := idx.Search(nil, query, rt)
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
