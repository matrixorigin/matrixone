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

package elkans_kmeans

import (
	"errors"
	"math"
	"math/rand"
	"time"
)

type Clusterer interface {
	InitCentroids()
	Cluster() ([][]float64, error)
}

type ElkansKMeansClusterer struct {
	maxIterations  int
	deltaThreshold float64

	// for each of the n vectors, we keep track of the following data
	vectorList  [][]float64
	vectorMetas []vectorMeta
	assignments []int

	// for each of the k centroids, we keep track of the following data
	Centroids       [][]float64
	centroidDist    [][]float64
	minCentroidDist []float64

	distFn     DistanceFunction
	clusterCnt int
	vectorCnt  int
}

type vectorMeta struct {
	lower     []float64
	upper     float64
	recompute bool
}

var _ Clusterer = new(ElkansKMeansClusterer)

func NewElkansKMeans(vectors [][]float64,
	clusterCnt, maxIterations int,
	deltaThreshold float64,
	distanceType DistanceType) (Clusterer, error) {

	err := validateArgs(vectors, clusterCnt, maxIterations, deltaThreshold)
	if err != nil {
		return nil, err
	}

	assignments := make([]int, len(vectors))
	var metas = make([]vectorMeta, len(vectors))
	for i := range metas {
		metas[i] = vectorMeta{
			lower:     make([]float64, clusterCnt),
			upper:     0,
			recompute: true,
		}
	}

	vectorDim := len(vectors[0])
	centroids := make([][]float64, clusterCnt)
	centroidDist := make([][]float64, clusterCnt)
	minCentroidDist := make([]float64, clusterCnt)
	for i := range centroids {
		centroids[i] = make([]float64, vectorDim)
		centroidDist[i] = make([]float64, clusterCnt)
	}

	distanceFunction := findCorrespondingDistanceFunc(distanceType)

	return &ElkansKMeansClusterer{
		maxIterations:  maxIterations,
		deltaThreshold: deltaThreshold,

		vectorList:  vectors,
		assignments: assignments,
		vectorMetas: metas,

		Centroids:       centroids,
		centroidDist:    centroidDist,
		minCentroidDist: minCentroidDist,

		distFn:     distanceFunction,
		clusterCnt: clusterCnt,
		vectorCnt:  len(vectors),
	}, nil
}

func (kmeans *ElkansKMeansClusterer) Cluster() ([][]float64, error) {

	kmeans.InitCentroids() // step 0
	changes := 0

	for iter := 0; ; iter++ {
		kmeans.computeCentroidDistanceMatrix() // step 1

		changes = kmeans.assignData() // step 2 and 3

		newCentroids := kmeans.recalculateCentroids() // step 4

		kmeans.updateBounds(newCentroids) // step 5 and 6

		kmeans.Centroids = newCentroids // step 7

		if iter != 0 && kmeans.isConverged(iter, changes) {
			break
		}
	}
	return kmeans.Centroids, nil
}

func validateArgs(vectorList [][]float64, clusterCnt, maxIterations int, deltaThreshold float64) error {
	if vectorList == nil || len(vectorList) == 0 || len(vectorList[0]) == 0 {
		return errors.New("input vectors is empty")
	}
	if clusterCnt > len(vectorList) {
		return errors.New("cluster count is larger than vector count")
	}
	if maxIterations < 0 {
		return errors.New("max iteration is out of bounds (must be >= 0)")
	}
	if deltaThreshold <= 0.0 || deltaThreshold >= 1.0 {
		return errors.New("delta threshold is out of bounds (must be > 0.0 and < 1.0)")
	}
	return nil
}

func (kmeans *ElkansKMeansClusterer) InitCentroids() {
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < kmeans.clusterCnt; i++ {
		randIdx := random.Intn(kmeans.vectorCnt)
		kmeans.Centroids[i] = kmeans.vectorList[randIdx]
	}
}

func (kmeans *ElkansKMeansClusterer) computeCentroidDistanceMatrix() {

	// step 1.a
	// For all centers c and c', compute 0.5 x d(c, c').
	for i := 0; i < kmeans.clusterCnt-1; i++ {
		for j := i + 1; j < kmeans.clusterCnt; j++ {
			kmeans.centroidDist[i][j] = 0.5 * kmeans.distFn(kmeans.Centroids[i], kmeans.Centroids[j])
			kmeans.centroidDist[j][i] = kmeans.centroidDist[i][j]
		}
	}

	// step 1.b
	//  For all centers c, compute s(c)=0.5 min {d(c, c') | c'!= c}.
	for i := 0; i < kmeans.clusterCnt; i++ {
		currMinDist := math.MaxFloat64
		for j := 0; j < kmeans.clusterCnt; j++ {
			if i == j {
				continue
			}
			currMinDist = math.Min(kmeans.centroidDist[i][j], currMinDist)
		}
		kmeans.minCentroidDist[i] = currMinDist
	}
}

func (kmeans *ElkansKMeansClusterer) assignData() int {

	var currVecUpperBound float64
	var currVecClusterAssignmentIdx int
	changes := 0

	for currVectorIdx, x := range kmeans.vectorList {

		currVecUpperBound = kmeans.vectorMetas[currVectorIdx].upper     // u(x) in the paper
		currVecClusterAssignmentIdx = kmeans.assignments[currVectorIdx] // c(x) in the paper

		// step 2 u(x) <= s(c(x))
		if currVecUpperBound <= kmeans.minCentroidDist[currVecClusterAssignmentIdx] {
			continue
		}

		// step 3
		for nextPossibleCentroidIdx := range kmeans.Centroids {
			if nextPossibleCentroidIdx == currVecClusterAssignmentIdx && // (i)
				currVecUpperBound <= kmeans.vectorMetas[currVectorIdx].lower[nextPossibleCentroidIdx] && // ii)
				currVecUpperBound <= kmeans.centroidDist[currVecClusterAssignmentIdx][nextPossibleCentroidIdx] { // (iii)
				continue
			}

			/* Step 3.a */
			// proactively use the otherwise case
			distToCentroid := currVecUpperBound
			if kmeans.vectorMetas[currVectorIdx].recompute {
				// then recompute the distance to the assigned
				// centroid
				distToCentroid = kmeans.distFn(x, kmeans.Centroids[currVecClusterAssignmentIdx])
				kmeans.vectorMetas[currVectorIdx].upper = distToCentroid
				kmeans.vectorMetas[currVectorIdx].lower[currVecClusterAssignmentIdx] = distToCentroid
				kmeans.vectorMetas[currVectorIdx].recompute = false
			}

			/* Step 3.b */
			if distToCentroid > kmeans.vectorMetas[currVectorIdx].lower[nextPossibleCentroidIdx] ||
				distToCentroid > kmeans.centroidDist[currVecClusterAssignmentIdx][nextPossibleCentroidIdx] {
				// only now compute the distance to the
				// centroid
				dist := kmeans.distFn(x, kmeans.Centroids[nextPossibleCentroidIdx])
				kmeans.vectorMetas[currVectorIdx].lower[nextPossibleCentroidIdx] = dist
				if dist < distToCentroid {
					kmeans.assignments[currVectorIdx] = nextPossibleCentroidIdx
					changes++
				}
			}
		}
	}
	return changes
}

func (kmeans *ElkansKMeansClusterer) recalculateCentroids() [][]float64 {
	clusterElementsSum := make([][]float64, len(kmeans.Centroids))
	clusterElementsCount := make([]int64, len(kmeans.Centroids))

	for j := range kmeans.Centroids {
		clusterElementsSum[j] = make([]float64, len(kmeans.vectorList[0]))
	}

	for i, x := range kmeans.vectorList {
		clusterElementsCount[kmeans.assignments[i]]++
		for j := range x {
			clusterElementsSum[kmeans.assignments[i]][j] += x[j]
		}
	}

	centroids := append([][]float64{}, kmeans.Centroids...)
	for j := range centroids {
		// if no objects are in the same class,
		// reinitialize it to a random vector
		if clusterElementsCount[j] == 0 {
			for l := range centroids[j] {
				centroids[j][l] = 10 * (rand.Float64() - 0.5)
			}
			continue
		}

		for l := range centroids[j] {
			centroids[j][l] = clusterElementsSum[j][l] / float64(clusterElementsCount[j])
		}
	}

	return centroids
}

func (kmeans *ElkansKMeansClusterer) updateBounds(newCentroids [][]float64) {
	for i := range kmeans.vectorList {
		/* Step 5 */
		for j := range kmeans.Centroids {
			// calculate the shift to the new centroid
			shift := kmeans.vectorMetas[i].lower[j] - kmeans.distFn(kmeans.Centroids[j], newCentroids[j])

			// bound the shift at 0 and assign it
			// as the new lower bound
			if shift < 0 {
				shift = 0
			}
			kmeans.vectorMetas[i].lower[j] = shift
		}

		/* Step 6 */
		// reassign the currVecUpperBound bound to account
		// for the centroid shift
		currVecClusterAssignmentIdx := kmeans.assignments[i]
		kmeans.vectorMetas[i].upper += kmeans.distFn(newCentroids[currVecClusterAssignmentIdx], kmeans.Centroids[currVecClusterAssignmentIdx])
		kmeans.vectorMetas[i].recompute = true
	}
}

func (kmeans *ElkansKMeansClusterer) isConverged(i int, changes int) bool {
	vectorCnt := float64(len(kmeans.vectorList))
	if i == kmeans.maxIterations ||
		changes < int(vectorCnt*kmeans.deltaThreshold) ||
		changes == 0 {
		return true
	}
	return false
}
