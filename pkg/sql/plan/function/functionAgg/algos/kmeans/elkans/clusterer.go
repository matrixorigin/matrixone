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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionAgg/algos/kmeans"
	"github.com/matrixorigin/matrixone/pkg/vectorize/moarray"
	"gonum.org/v1/gonum/mat"
	"math"
	"math/rand"
	"sync"
)

// ElkanClusterer is an improved kmeans algorithm which using the triangle inequality to reduce the number of
// distance calculations. As quoted from the paper:
// "The main contribution of this paper is an optimized version of the standard k-means method, with which the number
// of distance computations is in practice closer to `n` than to `nke`, where n is the number of vectors, k is the number
// of centroids, and e is the number of iterations needed until convergence."
//
// However, during each iteration of the algorithm, the lower bounds l(x, c) are updated for all points x and centers c.
// These updates take O(nk) time, so the complexity of the algorithm remains at least O(nke), even though the number
// of distance calculations is roughly O(n) only.
// NOTE that, distance calculation is very expensive for higher dimension vectors.
//
// Ref Paper: https://cdn.aaai.org/ICML/2003/ICML03-022.pdf
type ElkanClusterer struct {

	// for each of the n vectors, we keep track of the following data
	vectorList  []*mat.VecDense
	vectorMetas []vectorMeta
	assignments []int

	// for each of the k centroids, we keep track of the following data
	centroids                   []*mat.VecDense
	halfInterCentroidDistMatrix [][]float64
	minHalfInterCentroidDist    []float64

	// thresholds
	maxIterations  int     // e in paper
	deltaThreshold float64 // used for early convergence. we are not using it right now.

	// counts
	clusterCnt int // k in paper
	vectorCnt  int // n in paper

	distFn   kmeans.DistanceFunction
	initType kmeans.InitType
	rand     *rand.Rand
}

// vectorMeta holds required information for Elkan's kmeans pruning.
// lower is at-least distance of a vector to each of the k centroids. Thus, there are k values for each data point.
// upper is at-most (maximum possible) distance of a vector to its currently assigned or "closest" centroid.
// Hence, there's only one value for each data point.
// recompute is a flag to indicate if the distance to centroids needs to be recomputed. if false,
// the algorithm will rely on the 'upper' bound as an approximation instead of computing the exact distance.
type vectorMeta struct {
	lower     []float64
	upper     float64
	recompute bool
}

var _ kmeans.Clusterer = new(ElkanClusterer)

func NewKMeans(vectors [][]float64, clusterCnt,
	maxIterations int, deltaThreshold float64,
	distanceType kmeans.DistanceType, initType kmeans.InitType,
) (kmeans.Clusterer, error) {

	err := validateArgs(vectors, clusterCnt, maxIterations, deltaThreshold, distanceType, initType)
	if err != nil {
		return nil, err
	}

	gonumVectors, err := moarray.ToGonumVectors[float64](vectors...)
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

	centroidDist := make([][]float64, clusterCnt)
	for i := range centroidDist {
		centroidDist[i] = make([]float64, clusterCnt)
	}
	minCentroidDist := make([]float64, clusterCnt)

	distanceFunction, err := resolveDistanceFn(distanceType)
	if err != nil {
		return nil, err
	}

	return &ElkanClusterer{
		maxIterations:  maxIterations,
		deltaThreshold: deltaThreshold,

		vectorList:  gonumVectors,
		assignments: assignments,
		vectorMetas: metas,

		//centroids will be initialized by InitCentroids()
		halfInterCentroidDistMatrix: centroidDist,
		minHalfInterCentroidDist:    minCentroidDist,

		distFn:     distanceFunction,
		initType:   initType,
		clusterCnt: clusterCnt,
		vectorCnt:  len(vectors),

		rand: rand.New(rand.NewSource(kmeans.DefaultRandSeed)),
	}, nil
}

// Normalize is required for spherical kmeans initialization.
func (km *ElkanClusterer) Normalize() {
	moarray.NormalizeGonumVectors(km.vectorList)
}

// InitCentroids initializes the centroids using initialization algorithms like random or kmeans++.
// Right now, we don't support kmeans++ since it is very slow for large scale clustering.
func (km *ElkanClusterer) InitCentroids() {
	var initializer Initializer
	switch km.initType {
	case kmeans.Random:
		initializer = NewRandomInitializer()
	default:
		initializer = NewRandomInitializer()
	}
	km.centroids = initializer.InitCentroids(km.vectorList, km.clusterCnt)
}

// Cluster returns the final centroids and the error if any.
func (km *ElkanClusterer) Cluster() ([][]float64, error) {
	km.Normalize() // spherical kmeans initialization

	if km.vectorCnt == km.clusterCnt {
		return moarray.ToMoArrays[float64](km.vectorList), nil
	}

	km.InitCentroids() // step 0.1
	km.initBounds()    // step 0.2

	res, err := km.elkansCluster()
	if err != nil {
		return nil, err
	}

	return moarray.ToMoArrays[float64](res), nil
}

func (km *ElkanClusterer) elkansCluster() ([]*mat.VecDense, error) {

	for iter := 0; ; iter++ {
		km.computeCentroidDistances() // step 1

		changes := km.assignData() // step 2 and 3

		newCentroids := km.recalculateCentroids() // step 4

		km.updateBounds(newCentroids) // step 5 and 6

		km.centroids = newCentroids // step 7

		logutil.Debugf("kmeans iter=%d, changes=%d", iter, changes)
		if iter != 0 && km.isConverged(iter, changes) {
			break
		}
	}
	return km.centroids, nil
}

func validateArgs(vectorList [][]float64, clusterCnt,
	maxIterations int, deltaThreshold float64,
	distanceType kmeans.DistanceType, initType kmeans.InitType) error {
	if len(vectorList) == 0 || len(vectorList[0]) == 0 {
		return moerr.NewInternalErrorNoCtx("input vectors is empty")
	}
	if clusterCnt > len(vectorList) {
		return moerr.NewInternalErrorNoCtx("cluster count is larger than vector count")
	}
	if maxIterations < 0 {
		return moerr.NewInternalErrorNoCtx("max iteration is out of bounds (must be >= 0)")
	}
	if deltaThreshold <= 0.0 || deltaThreshold >= 1.0 {
		return moerr.NewInternalErrorNoCtx("delta threshold is out of bounds (must be > 0.0 and < 1.0)")
	}
	if distanceType > 2 {
		return moerr.NewInternalErrorNoCtx("distance type is not supported")
	}
	if initType != 0 {
		return moerr.NewInternalErrorNoCtx("init type is not supported")
	}

	// We need to validate that all vectors have the same dimension.
	// This is already done by moarray.ToGonumVectors, so skipping it here.

	return nil
}

// initBounds initializes the lower bounds, upper bound and assignment for each vector.
func (km *ElkanClusterer) initBounds() {
	// step 0.2
	// Set the lower bound l(x, c)=0 for each point x and center c.
	// Assign each x to its closest initial center c(x)=min{ d(x, c) }, using Lemma 1 to avoid
	// redundant distance calculations. Each time d(x, c) is computed, set l(x, c)=d(x, c).
	// Assign upper bounds u(x)=min_c d(x, c).
	for x := range km.vectorList {
		minDist := math.MaxFloat64
		closestCenter := 0
		for c := range km.centroids {
			dist := km.distFn(km.vectorList[x], km.centroids[c])
			km.vectorMetas[x].lower[c] = dist
			if dist < minDist {
				minDist = dist
				closestCenter = c
			}
		}

		km.vectorMetas[x].upper = minDist
		km.assignments[x] = closestCenter
	}
}

// computeCentroidDistances computes the centroid distances and the min centroid distances.
// NOTE: here we are save 0.5 of centroid distance to avoid 0.5 multiplication in step 3(iii) and 3.b.
func (km *ElkanClusterer) computeCentroidDistances() {

	// step 1.a
	// For all centers c and c', compute 0.5 x d(c, c').
	var wg sync.WaitGroup
	for r := 0; r < km.clusterCnt; r++ {
		for c := r + 1; c < km.clusterCnt; c++ {
			wg.Add(1)
			go func(i, j int) {
				defer wg.Done()
				dist := 0.5 * km.distFn(km.centroids[i], km.centroids[j])
				km.halfInterCentroidDistMatrix[i][j] = dist
				km.halfInterCentroidDistMatrix[j][i] = dist
			}(r, c)
		}
	}
	wg.Wait()

	// step 1.b
	//  For all centers c, compute s(c)=0.5 x min{d(c, c') | c'!= c}.
	for i := 0; i < km.clusterCnt; i++ {
		currMinDist := math.MaxFloat64
		for j := 0; j < km.clusterCnt; j++ {
			if i == j {
				continue
			}
			currMinDist = math.Min(currMinDist, km.halfInterCentroidDistMatrix[i][j])
		}
		km.minHalfInterCentroidDist[i] = currMinDist
	}
}

// assignData assigns each vector to the nearest centroid.
// This is the place where most of the "distance computation skipping" happens.
func (km *ElkanClusterer) assignData() int {

	var distToCurrentCentroid float64 // u(x) in the paper
	var assignedCentroid int          // c(x) in the paper
	changes := 0

	for currVector := range km.vectorList {

		distToCurrentCentroid = km.vectorMetas[currVector].upper
		assignedCentroid = km.assignments[currVector]

		// step 2
		// u(x) <= s(c(x))
		if distToCurrentCentroid <= km.minHalfInterCentroidDist[assignedCentroid] {
			continue
		}

		for c := range km.centroids { // c is nextPossibleCentroidIdx
			// step 3
			// For all remaining points x and centers c such that
			// (i) c != c(x) and
			// (ii) u(x)>l(x, c) and
			// (iii) u(x)> 0.5 x d(c(x), c)
			if c != assignedCentroid &&
				distToCurrentCentroid > km.vectorMetas[currVector].lower[c] &&
				distToCurrentCentroid > km.halfInterCentroidDistMatrix[assignedCentroid][c] {

				//step 3.a - Bounds update
				// If r(x) then compute d(x, c(x)) and assign r(x)= false. Otherwise, d(x, c(x))=u(x).
				dxcx := distToCurrentCentroid // d(x, c(x)) in the paper ie distToCentroid
				if km.vectorMetas[currVector].recompute {
					dxcx = km.distFn(km.vectorList[currVector], km.centroids[assignedCentroid])
					km.vectorMetas[currVector].upper = dxcx
					km.vectorMetas[currVector].lower[assignedCentroid] = dxcx
					km.vectorMetas[currVector].recompute = false
				}

				//step 3.b - Update
				// If d(x, c(x))>l(x, c) or d(x, c(x))> 0.5 d(c(x), c) then
				// Compute d(x, c)
				// If d(x, c)<d(x, c(x)) then assign c(x)=c.
				if dxcx > km.vectorMetas[currVector].lower[c] ||
					dxcx > km.halfInterCentroidDistMatrix[assignedCentroid][c] {

					dxc := km.distFn(km.vectorList[currVector], km.centroids[c]) // d(x,c) in the paper
					km.vectorMetas[currVector].lower[c] = dxc
					if dxc < dxcx {
						km.vectorMetas[currVector].upper = dxc

						assignedCentroid = c
						km.assignments[currVector] = assignedCentroid
						changes++
					}
				}
			}
		}
	}
	return changes
}

// recalculateCentroids calculates the new mean centroids based on the new assignments.
func (km *ElkanClusterer) recalculateCentroids() []*mat.VecDense {
	membersCount := make([]int64, km.clusterCnt)

	newCentroids := make([]*mat.VecDense, km.clusterCnt)
	for c := range newCentroids {
		newCentroids[c] = mat.NewVecDense(km.vectorList[0].Len(), nil)
	}

	// sum of all the members of the cluster
	for x, vec := range km.vectorList {
		cx := km.assignments[x]
		membersCount[cx]++
		newCentroids[cx].AddVec(newCentroids[cx], vec)
	}

	// means of the clusters = sum of all the members of the cluster / number of members in the cluster
	for c := range newCentroids {
		if membersCount[c] == 0 {
			// if the cluster is empty, reinitialize it to a random vector, since you can't find the mean of an empty set
			randVector := make([]float64, km.vectorList[0].Len())
			for l := range randVector {
				randVector[l] = 10 * (km.rand.Float64() - 0.5)
			}
			newCentroids[c] = mat.NewVecDense(km.vectorList[0].Len(), randVector)

			// normalize the random vector
			moarray.NormalizeGonumVector(newCentroids[c])
		} else {
			// find the mean of the cluster members
			// note: we don't need to normalize here, since the vectors are already normalized
			newCentroids[c].ScaleVec(1.0/float64(membersCount[c]), newCentroids[c])
		}

	}

	return newCentroids
}

// updateBounds updates the lower and upper bounds for each vector.
func (km *ElkanClusterer) updateBounds(newCentroid []*mat.VecDense) {

	// compute the centroid shift distance matrix once.
	// d(c', m(c')) in the paper
	centroidShiftDist := make([]float64, km.clusterCnt)
	var wg sync.WaitGroup
	for c := 0; c < km.clusterCnt; c++ {
		wg.Add(1)
		go func(cIdx int) {
			defer wg.Done()
			centroidShiftDist[cIdx] = km.distFn(km.centroids[cIdx], newCentroid[cIdx])
			logutil.Debugf("centroidShiftDist[%d]=%f", cIdx, centroidShiftDist[cIdx])
		}(c)
	}
	wg.Wait()

	// step 5
	//For each point x and center c, assign
	// l(x, c)= max{ l(x, c)-d(c, m(c)), 0 }
	for x := range km.vectorList {
		for c := range km.centroids {
			shift := km.vectorMetas[x].lower[c] - centroidShiftDist[c]
			km.vectorMetas[x].lower[c] = math.Max(shift, 0)
		}

		// step 6
		// For each point x, assign
		// u(x)= u(x) + d(m(c(x)), c(x))
		// r(x)= true
		cx := km.assignments[x]
		km.vectorMetas[x].upper += centroidShiftDist[cx]
		km.vectorMetas[x].recompute = true
	}
}

// isConverged checks if the algorithm has converged.
func (km *ElkanClusterer) isConverged(iter int, changes int) bool {
	if iter == km.maxIterations || changes == 0 {
		return true
	}
	// NOTE: we are not using deltaThreshold right now.
	//if changes < int(float64(km.vectorCnt)*km.deltaThreshold) {
	//	return true
	//}
	return false
}

// SSE returns the sum of squared errors.
func (km *ElkanClusterer) SSE() float64 {
	sse := 0.0
	for i := range km.vectorList {
		distErr := km.distFn(km.vectorList[i], km.centroids[km.assignments[i]])
		sse += math.Pow(distErr, 2)
	}
	return sse
}
