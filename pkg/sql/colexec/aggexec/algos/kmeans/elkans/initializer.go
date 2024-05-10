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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec/algos/kmeans"
	"gonum.org/v1/gonum/mat"
	"math"
	"math/rand"
)

type Initializer interface {
	InitCentroids(vectors []*mat.VecDense, k int) (centroids []*mat.VecDense)
}

var _ Initializer = (*Random)(nil)

// Random initializes the centroids with random centroids from the vector list.
type Random struct {
	rand rand.Rand
}

func NewRandomInitializer() Initializer {
	return &Random{
		rand: *rand.New(rand.NewSource(kmeans.DefaultRandSeed)),
	}
}

func (r *Random) InitCentroids(vectors []*mat.VecDense, k int) (centroids []*mat.VecDense) {
	centroids = make([]*mat.VecDense, k)
	for i := 0; i < k; i++ {
		randIdx := r.rand.Intn(len(vectors))
		centroids[i] = vectors[randIdx]
	}
	return centroids
}

// KMeansPlusPlus initializes the centroids using kmeans++ algorithm.
// Complexity: O(k*n*k); n = number of vectors, k = number of clusters
// Ref Paper: https://theory.stanford.edu/~sergei/papers/kMeansPP-soda.pdf
// The reason why we have kmeans++ is that it is more stable than random initialization.
// For example, we have 3 clusters.
// Using random, we could get 3 centroids: 1&2 which are close to each other and part of cluster 1. 3 is in the middle of 2&3.
// Using kmeans++, we are sure that 3 centroids are farther away from each other.
type KMeansPlusPlus struct {
	rand   rand.Rand
	distFn kmeans.DistanceFunction
}

func NewKMeansPlusPlusInitializer(distFn kmeans.DistanceFunction) Initializer {
	return &KMeansPlusPlus{
		rand:   *rand.New(rand.NewSource(kmeans.DefaultRandSeed)),
		distFn: distFn,
	}
}

func (kpp *KMeansPlusPlus) InitCentroids(vectors []*mat.VecDense, k int) (centroids []*mat.VecDense) {
	numSamples := len(vectors)
	centroids = make([]*mat.VecDense, k)

	// 1. start with a random center
	centroids[0] = vectors[kpp.rand.Intn(numSamples)]

	distances := make([]float64, numSamples)
	for j := range distances {
		distances[j] = math.MaxFloat64
	}

	for nextCentroidIdx := 1; nextCentroidIdx < k; nextCentroidIdx++ {

		// 2. for each data point, compute the min distance to the existing centers
		var totalDistToExistingCenters float64
		for vecIdx := range vectors {
			// this is a deviation from standard kmeans.here we are not using minDistance to all the existing centers.
			// This code was very slow: https://github.com/matrixorigin/matrixone/blob/77ff1452bd56cd93a10e3327632adebdbaf279cb/pkg/sql/plan/function/functionAgg/algos/kmeans/elkans/initializer.go#L81-L86
			// but instead we are using the distance to the last center that was chosen.
			distance := kpp.distFn(vectors[vecIdx], centroids[nextCentroidIdx-1])
			distance *= distance
			if distance < distances[vecIdx] {
				distances[vecIdx] = distance
			}
			totalDistToExistingCenters += distances[vecIdx]
		}

		// 3. choose the next random center, using a weighted probability distribution
		// where it is chosen with probability proportional to D(x)^2
		// Ref: https://en.wikipedia.org/wiki/K-means%2B%2B#Improved_initialization_algorithm
		target := kpp.rand.Float64() * totalDistToExistingCenters
		for idx, distance := range distances {
			target -= distance
			if target <= 0 {
				centroids[nextCentroidIdx] = vectors[idx]
				break
			}
		}
	}
	return centroids
}
