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
	//"os"

	"github.com/stretchr/testify/require"

	cuvs "github.com/rapidsai/cuvs/go"
	"github.com/rapidsai/cuvs/go/brute_force"
	"github.com/rapidsai/cuvs/go/ivf_flat"
)

func getCenters(vecs [][]float32, dim int, clusterCnt int, distanceType cuvs.Distance, maxIterations int) ([][]float32, error) {

	resource, err := cuvs.NewResource(nil)
	if err != nil {
		return nil, err
	}
	defer resource.Close()

	indexParams, err := ivf_flat.CreateIndexParams()
	if err != nil {
		return nil, err
	}
	defer indexParams.Close()

	indexParams.SetNLists(uint32(clusterCnt))
	indexParams.SetMetric(distanceType)
	indexParams.SetKMeansNIters(uint32(maxIterations))
	indexParams.SetKMeansTrainsetFraction(1) // train all sample

	dataset, err := cuvs.NewTensor(vecs)
	if err != nil {
		return nil, err
	}
	defer dataset.Close()

	index, _ := ivf_flat.CreateIndex(indexParams, &dataset)
	defer index.Close()

	if _, err := dataset.ToDevice(&resource); err != nil {
		return nil, err
	}

	centers, err := cuvs.NewTensorOnDevice[float32](&resource, []int64{int64(clusterCnt), int64(dim)})
	if err != nil {
		return nil, err
	}

	if err := ivf_flat.BuildIndex(resource, indexParams, &dataset, index); err != nil {
		return nil, err
	}

	if err := resource.Sync(); err != nil {
		return nil, err
	}

	if err := ivf_flat.GetCenters(index, &centers); err != nil {
		return nil, err
	}

	if _, err := centers.ToHost(&resource); err != nil {
		return nil, err
	}

	if err := resource.Sync(); err != nil {
		return nil, err
	}

	result, err := centers.Slice()
	if err != nil {
		return nil, err
	}

	return result, nil

}

func Search(datasetvec [][]float32, queriesvec [][]float32, limit uint, distanceType cuvs.Distance) (retkeys any, retdistances []float64, err error) {
	//os.Stderr.WriteString(fmt.Sprintf("probe set %d\n", len(queriesvec)))
	//os.Stderr.WriteString("brute force index search start\n")

	resource, err := cuvs.NewResource(nil)
	if err != nil {
		return
	}
	defer resource.Close()

	dataset, err := cuvs.NewTensor(datasetvec)
	if err != nil {
		return
	}
	defer dataset.Close()

	index, err := brute_force.CreateIndex()
	if err != nil {
		return
	}
	defer index.Close()

	queries, err := cuvs.NewTensor(queriesvec)
	if err != nil {
		return
	}
	defer queries.Close()

	neighbors, err := cuvs.NewTensorOnDevice[int64](&resource, []int64{int64(len(queriesvec)), int64(limit)})
	if err != nil {
		return
	}
	defer neighbors.Close()

	distances, err := cuvs.NewTensorOnDevice[float32](&resource, []int64{int64(len(queriesvec)), int64(limit)})
	if err != nil {
		return
	}
	defer distances.Close()

	if _, err = dataset.ToDevice(&resource); err != nil {
		return
	}

	if err = resource.Sync(); err != nil {
		return
	}

	err = brute_force.BuildIndex(resource, &dataset, distanceType, 2.0, index)
	if err != nil {
		//os.Stderr.WriteString(fmt.Sprintf("BruteForceIndex: build index failed %v\n", err))
		//os.Stderr.WriteString(fmt.Sprintf("BruteForceIndex: build index failed centers %v\n", datasetvec))
		return
	}

	if err = resource.Sync(); err != nil {
		return
	}
	//os.Stderr.WriteString("built brute force index\n")

	if _, err = queries.ToDevice(&resource); err != nil {
		return
	}

	//os.Stderr.WriteString("brute force index search Runing....\n")
	err = brute_force.SearchIndex(resource, *index, &queries, &neighbors, &distances)
	if err != nil {
		return
	}
	//os.Stderr.WriteString("brute force index search finished Runing....\n")

	if _, err = neighbors.ToHost(&resource); err != nil {
		return
	}
	//os.Stderr.WriteString("brute force index search neighbour to host done....\n")

	if _, err = distances.ToHost(&resource); err != nil {
		return
	}
	//os.Stderr.WriteString("brute force index search distances to host done....\n")

	if err = resource.Sync(); err != nil {
		return
	}

	//os.Stderr.WriteString("brute force index search return result....\n")
	neighborsSlice, err := neighbors.Slice()
	if err != nil {
		return
	}

	distancesSlice, err := distances.Slice()
	if err != nil {
		return
	}

	//fmt.Printf("flattened %v\n", flatten)
	retdistances = make([]float64, len(distancesSlice)*int(limit))
	for i := range distancesSlice {
		for j, dist := range distancesSlice[i] {
			retdistances[i*int(limit)+j] = float64(dist)
		}
	}

	keys := make([]int64, len(neighborsSlice)*int(limit))
	for i := range neighborsSlice {
		for j, key := range neighborsSlice[i] {
			keys[i*int(limit)+j] = int64(key)
		}
	}
	retkeys = keys
	//os.Stderr.WriteString("brute force index search RETURN NOW....\n")
	return
}

func TestIvfAndBruteForceForIssue(t *testing.T) {

	dimension := uint(128)
	limit := uint(1)
	/*
	ncpu := uint(1)
	elemsz := uint(4) // float32
	*/

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

	centers, err := getCenters(vecs, int(dimension), nlist, cuvs.DistanceL2, 10)
	require.NoError(t, err)

	var wg sync.WaitGroup

	for n := 0; n < 4; n++ {

		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				_, _, err := Search(centers, queries, limit, cuvs.DistanceL2)
				require.NoError(t, err)

				/*
				keys_i64, ok := keys.([]int64)
				require.Equal(t, ok, true)

				for j, key := range keys_i64 {
					require.Equal(t, key, int64(j))
					require.Equal(t, distances[j], float64(0))
				}
				*/
				// fmt.Printf("keys %v, dist %v\n", keys, distances)
			}
		}()
	}

	wg.Wait()

}
