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
	"fmt"
	"math/rand/v2"
	"runtime"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/cuvs"
	"github.com/stretchr/testify/require"
)

func getCenters(vecs [][]float32, dim int, clusterCnt int, distanceType cuvs.DistanceType, maxIterations int) ([][]float32, error) {
	if len(vecs) == 0 {
		return nil, fmt.Errorf("empty dataset")
	}

	// Flatten vectors
	flattened := make([]float32, len(vecs)*dim)
	for i, v := range vecs {
		copy(flattened[i*dim:(i+1)*dim], v)
	}

	deviceID := 0
	nthread := uint32(1)
	km, err := cuvs.NewGpuKMeans[float32](uint32(clusterCnt), uint32(dim), distanceType, maxIterations, deviceID, nthread)
	if err != nil {
		return nil, err
	}
	defer km.Destroy()
	km.Start()

	_, _, err = km.Fit(flattened, uint64(len(vecs)))
	if err != nil {
		return nil, err
	}

	centroids, err := km.GetCentroids()
	if err != nil {
		return nil, err
	}

	// Reshape centroids
	result := make([][]float32, clusterCnt)
	for i := 0; i < clusterCnt; i++ {
		result[i] = make([]float32, dim)
		copy(result[i], centroids[i*dim:(i+1)*dim])
	}

	return result, nil
}

func Search(datasetvec [][]float32, queriesvec [][]float32, limit uint, distanceType cuvs.DistanceType) (retkeys any, retdistances []float64, err error) {
	if len(datasetvec) == 0 || len(queriesvec) == 0 {
		return nil, nil, nil
	}

	dim := len(datasetvec[0])
	flattenedDataset := make([]float32, len(datasetvec)*dim)
	for i, v := range datasetvec {
		copy(flattenedDataset[i*dim:(i+1)*dim], v)
	}

	flattenedQueries := make([]float32, len(queriesvec)*dim)
	for i, v := range queriesvec {
		copy(flattenedQueries[i*dim:(i+1)*dim], v)
	}

	deviceID := 0
	nthread := uint32(1)
	bf, err := cuvs.NewGpuBruteForce[float32](flattenedDataset, uint64(len(datasetvec)), uint32(dim), distanceType, nthread, deviceID)
	if err != nil {
		return nil, nil, err
	}
	defer bf.Destroy()
	bf.Start()

	err = bf.Build()
	if err != nil {
		return nil, nil, err
	}

	neighbors, distances, err := bf.Search(flattenedQueries, uint64(len(queriesvec)), uint32(dim), uint32(limit))
	if err != nil {
		return nil, nil, err
	}

	retdistances = make([]float64, len(distances))
	for i, d := range distances {
		retdistances[i] = float64(d)
	}

	retkeys = neighbors
	return
}

func TestIssueGpu(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()

		defer wg.Done()

		dimension := uint(128)
		dsize := 100000
		nlist := 128
		vecs := make([][]float32, dsize)
		for i := range vecs {
			vecs[i] = make([]float32, dimension)
			for j := range vecs[i] {
				vecs[i][j] = rand.Float32()
			}
		}

		_, err := getCenters(vecs, int(dimension), nlist, cuvs.L2Expanded, 10)
		require.NoError(t, err)
	}()
	wg.Wait()
}

func TestIssueIvfAndBruteForceForIssue(t *testing.T) {
	var wg1 sync.WaitGroup
	wg1.Add(1)
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()

		defer wg1.Done()

		dimension := uint(128)
		limit := uint(1)
		dsize := 100000
		nlist := 128
		vecs := make([][]float32, dsize)
		for i := range vecs {
			vecs[i] = make([]float32, dimension)
			for j := range vecs[i] {
				vecs[i][j] = rand.Float32()
			}
		}
		queries := vecs[:8192]

		centers, err := getCenters(vecs, int(dimension), nlist, cuvs.L2Expanded, 10)
		require.NoError(t, err)

		fmt.Println("centers DONE")

		var wg sync.WaitGroup

		for n := 0; n < 8; n++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				runtime.LockOSThread()
				defer runtime.UnlockOSThread()

				for i := 0; i < 100; i++ { // Reduced iteration count for faster test run
					_, _, err := Search(centers, queries, limit, cuvs.L2Expanded)
					require.NoError(t, err)
				}
			}()
		}

		wg.Wait()
	}()

	wg1.Wait()
}
