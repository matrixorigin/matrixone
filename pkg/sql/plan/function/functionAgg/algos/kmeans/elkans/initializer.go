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

// randomInit initializes the centroids with random centroids from the vector list.
// As mentioned in the FAISS discussion here https://github.com/facebookresearch/faiss/issues/268#issuecomment-348184505
// "We have not implemented it in Faiss, because with our former Yael library, which implements both k-means++
// and regular random initialization, we observed that the overhead computational cost was not
// worth the saving (negligible) in all large-scale settings we have considered."
func (km *KMeansClusterer) randomInit() {
	for i := 0; i < km.clusterCnt; i++ {
		randIdx := km.rand.Intn(km.vectorCnt)
		km.Centroids[i] = km.vectorList[randIdx]
	}
}

// kmeansPlusPlusInit initializes the centroids using kmeans++ algorithm.
// Complexity: O(k*n*k); n = number of vectors, k = number of clusters
// Ref Paper: https://theory.stanford.edu/~sergei/papers/kMeansPP-soda.pdf
func (km *KMeansClusterer) kmeansPlusPlusInit() {
	// 1. start with a random center
	km.Centroids[0] = km.vectorList[km.rand.Intn(km.vectorCnt)]

	distances := make([]float64, km.vectorCnt)
	for nextCentroidIdx := 1; nextCentroidIdx < len(km.Centroids); nextCentroidIdx++ {

		// 2. for each data point, compute the min distance to the existing centers
		var totalDistToExistingCenters float64
		for vecIdx := range km.vectorList {
			minDist := km.distFn(km.vectorList[vecIdx], km.Centroids[0])
			for currKnownCentroidIdx := 1; currKnownCentroidIdx < nextCentroidIdx; currKnownCentroidIdx++ {
				dist := km.distFn(km.vectorList[vecIdx], km.Centroids[currKnownCentroidIdx])
				if dist < minDist {
					minDist = dist
				}
			}

			distances[vecIdx] = minDist * minDist
			totalDistToExistingCenters += distances[vecIdx]
		}

		// 3. choose the next random center, using a weighted probability distribution
		// where it is chosen with probability proportional to D(x)^2
		// Ref: https://en.wikipedia.org/wiki/K-means%2B%2B#Improved_initialization_algorithm
		target := km.rand.Float64() * totalDistToExistingCenters
		idx := 0
		for currSum := distances[0]; currSum < target; currSum += distances[idx] {
			idx++
		}
		km.Centroids[nextCentroidIdx] = km.vectorList[idx]
	}
}
