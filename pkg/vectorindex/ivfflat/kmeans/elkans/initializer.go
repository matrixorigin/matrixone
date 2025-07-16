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

package elkans

import (
	"math/rand"
	"runtime"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/kmeans"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

type Initializer interface {
	InitCentroids(vectors any, k int) (centroids any, err error)
}

// var _ Initializer = (*Random)(nil)

// Random initializes the centroids with random centroids from the vector list.
type Random struct {
	rand rand.Rand
}

func NewRandomInitializer() Initializer {
	return &Random{
		rand: *rand.New(rand.NewSource(kmeans.DefaultRandSeed)),
	}
}

func (r *Random) InitCentroids(vectors any, k int) (_centroids any, _err error) {

	switch _vecs := vectors.(type) {
	case [][]float32:
		centroids := make([][]float32, k)
		for i := 0; i < k; i++ {
			randIdx := r.rand.Intn(len(_vecs))
			centroids[i] = _vecs[randIdx]
		}
		return centroids, nil

	case [][]float64:
		centroids := make([][]float64, k)
		for i := 0; i < k; i++ {
			randIdx := r.rand.Intn(len(_vecs))
			centroids[i] = _vecs[randIdx]
		}
		return centroids, nil

	default:
		return nil, moerr.NewInternalErrorNoCtx("InitCentroids type not supported")
	}
}

// KMeansPlusPlus initializes the centroids using kmeans++ algorithm.
// Complexity: O(k*n*k); n = number of vectors, k = number of clusters
// Ref Paper: https://theory.stanford.edu/~sergei/papers/kMeansPP-soda.pdf
// The reason why we have kmeans++ is that it is more stable than random initialization.
// For example, we have 3 clusters.
// Using random, we could get 3 centroids: 1&2 which are close to each other and part of cluster 1. 3 is in the middle of 2&3.
// Using kmeans++, we are sure that 3 centroids are farther away from each other.
type KMeansPlusPlus[T types.RealNumbers] struct {
	rand   rand.Rand
	distFn metric.DistanceFunction[T]
}

func NewKMeansPlusPlusInitializer[T types.RealNumbers](distFn metric.DistanceFunction[T]) Initializer {
	return &KMeansPlusPlus[T]{
		rand:   *rand.New(rand.NewSource(kmeans.DefaultRandSeed)),
		distFn: distFn,
	}
}

func (kpp *KMeansPlusPlus[T]) InitCentroids(_vectors any, k int) (_centroids any, _err error) {

	vectors, ok := _vectors.([][]T)
	if !ok {
		panic("KmeanPlusPlus InitCentroids type not supported")
	}

	numSamples := len(vectors)
	centroids := make([][]T, k)

	// 1. start with a random center
	centroids[0] = vectors[kpp.rand.Intn(numSamples)]

	distances := make([]T, numSamples)
	for j := range distances {
		distances[j] = metric.MaxFloat[T]()
	}

	ncpu := runtime.NumCPU()
	if numSamples < ncpu {
		ncpu = numSamples
	}

	errs := make(chan error, ncpu)
	for nextCentroidIdx := 1; nextCentroidIdx < k; nextCentroidIdx++ {

		// 2. for each data point, compute the min distance to the existing centers
		var totalDistToExistingCenters T
		var wg sync.WaitGroup
		var mutex sync.Mutex

		for n := 0; n < ncpu; n++ {
			wg.Add(1)

			go func(tid int) {
				defer wg.Done()

				for vecIdx := range vectors {

					if vecIdx%ncpu != tid {
						continue
					}

					// this is a deviation from standard kmeans.here we are not using minDistance to all the existing centers.
					// This code was very slow: https://github.com/matrixorigin/matrixone/blob/77ff1452bd56cd93a10e3327632adebdbaf279cb/pkg/sql/plan/function/functionAgg/algos/kmeans/elkans/initializer.go#L81-L86
					// but instead we are using the distance to the last center that was chosen.
					distance, err := kpp.distFn(vectors[vecIdx], centroids[nextCentroidIdx-1])
					if err != nil {
						errs <- err
						return
					}

					distance *= distance
					mutex.Lock()
					if distance < distances[vecIdx] {
						distances[vecIdx] = distance
					}
					totalDistToExistingCenters += distances[vecIdx]
					mutex.Unlock()
				}
			}(n)
		}

		wg.Wait()

		if len(errs) > 0 {
			return nil, <-errs
		}

		// 3. choose the next random center, using a weighted probability distribution
		// where it is chosen with probability proportional to D(x)^2
		// Ref: https://en.wikipedia.org/wiki/K-means%2B%2B#Improved_initialization_algorithm
		target := T(kpp.rand.Float32()) * totalDistToExistingCenters
		for idx, distance := range distances {
			target -= distance
			if target <= 0 {
				centroids[nextCentroidIdx] = vectors[idx]
				break
			}
		}
	}
	return centroids, nil
}
