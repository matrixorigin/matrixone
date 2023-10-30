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
	"errors"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionAgg/algos/kmeans"
	"golang.org/x/sync/errgroup"
	"math"
	"math/rand"
	"sync"
)

// KMeansClusterer is an improved kmeans algorithm which using the triangle inequality to reduce the number of
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
// Ref Material :https://www.cse.iitd.ac.in/~rjaiswal/2015/col870/Project/Nipun.pdf
type KMeansClusterer struct {

	// for each of the n vectors, we keep track of the following data
	vectorList  [][]float64
	vectorMetas []vectorMeta
	assignments []int

	// for each of the k centroids, we keep track of the following data
	Centroids                   [][]float64
	halfInterCentroidDistMatrix [][]float64
	minHalfInterCentroidDist    []float64

	// thresholds
	maxIterations  int     // e in paper
	deltaThreshold float64 // used for early convergence

	// counts
	clusterCnt int // k in paper
	vectorCnt  int // n in paper

	distFn kmeans.DistanceFunction
	rand   *rand.Rand
}

// vectorMeta holds info required for Elkan's kmeans optimization.
// lower is the lower bound distance of a vector to `each` of its centroids. Hence, there are k values.
// upper is the upper bound distance of a vector to its `closest` centroid. Hence, only one value.
// recompute is a flag to indicate if the distance to centroids needs to be recomputed. if false, then use upper.
type vectorMeta struct {
	lower     []float64
	upper     float64
	recompute bool
}

var _ kmeans.Clusterer = new(KMeansClusterer)

func NewElkansKMeans(vectors [][]float64,
	clusterCnt, maxIterations int,
	deltaThreshold float64,
	distanceType kmeans.DistanceType) (kmeans.Clusterer, error) {

	err := validateArgs(vectors, clusterCnt, maxIterations, deltaThreshold, distanceType)
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

	distanceFunction, err := resolveDistanceFn(distanceType)
	if err != nil {
		return nil, err
	}

	return &KMeansClusterer{
		maxIterations:  maxIterations,
		deltaThreshold: deltaThreshold,

		vectorList:  vectors,
		assignments: assignments,
		vectorMetas: metas,

		Centroids:                   centroids,
		halfInterCentroidDistMatrix: centroidDist,
		minHalfInterCentroidDist:    minCentroidDist,

		distFn:     distanceFunction,
		clusterCnt: clusterCnt,
		vectorCnt:  len(vectors),

		rand: rand.New(rand.NewSource(kmeans.DefaultRandSeed)),
	}, nil
}

// InitCentroids initializes the centroids using initialization algorithms like kmeans++ or random.
func (kmeans *KMeansClusterer) InitCentroids() {
	// Here we use random initialization, as it is the better suited for large-scale use-case.
	kmeans.randomInit()
}

// Cluster returns the final centroids and the error if any.
func (kmeans *KMeansClusterer) Cluster() ([][]float64, error) {

	if kmeans.vectorCnt == kmeans.clusterCnt {
		return kmeans.vectorList, nil
	}

	kmeans.InitCentroids() // step 0.a
	kmeans.initBounds()    // step 0.b

	return kmeans.elkansCluster()
}

func (kmeans *KMeansClusterer) elkansCluster() ([][]float64, error) {

	for iter := 0; ; iter++ {
		kmeans.computeCentroidDistances() // step 1

		changes := kmeans.assignData() // step 2 and 3

		newCentroids := kmeans.recalculateCentroids() // step 4

		kmeans.updateBounds(newCentroids) // step 5 and 6

		kmeans.Centroids = newCentroids // step 7

		if iter != 0 && kmeans.isConverged(iter, changes) {
			break
		}
	}
	return kmeans.Centroids, nil
}

func validateArgs(vectorList [][]float64, clusterCnt, maxIterations int, deltaThreshold float64, distanceType kmeans.DistanceType) error {
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
	if distanceType < 0 || distanceType > 3 {
		return errors.New("distance type is not supported")
	}

	// validate that all vectors have the same dimension
	vecDim := len(vectorList[0])
	eg := new(errgroup.Group)
	for i := 1; i < len(vectorList); i++ {
		func(idx int) {
			eg.Go(func() error {
				if len(vectorList[idx]) != vecDim {
					return errors.New(fmt.Sprintf("dim mismatch. "+
						"vector[%d] has dimension %d, "+
						"but vector[0] has dimension %d", idx, len(vectorList[idx]), vecDim))
				}
				return nil
			})
		}(i)
	}
	if err := eg.Wait(); err != nil {
		return err
	}

	return nil
}

// initBounds initializes the lower bounds, upper bound and assignment for each vector.
func (kmeans *KMeansClusterer) initBounds() {
	for x := range kmeans.vectorList {
		minDist := math.MaxFloat64
		closestCenter := 0
		for c := 0; c < len(kmeans.Centroids); c++ {
			dist := kmeans.distFn(kmeans.vectorList[x], kmeans.Centroids[c])
			kmeans.vectorMetas[x].lower[c] = dist
			if dist < minDist {
				minDist = dist
				closestCenter = c
			}
		}

		kmeans.vectorMetas[x].upper = minDist
		kmeans.assignments[x] = closestCenter
	}
}

// computeCentroidDistances computes the centroid distances and the min centroid distances.
// NOTE: here we are save 0.5 of centroid distance to avoid 0.5 multiplication in step 3(iii) and 3.b.
func (kmeans *KMeansClusterer) computeCentroidDistances() {

	// step 1.a
	// For all centers c and c', compute 0.5 x d(c, c').
	var wg sync.WaitGroup
	for r := 0; r < kmeans.clusterCnt; r++ {
		for c := r + 1; c < kmeans.clusterCnt; c++ {
			wg.Add(1)
			go func(i, j int) {
				defer wg.Done()
				dist := 0.5 * kmeans.distFn(kmeans.Centroids[i], kmeans.Centroids[j])
				kmeans.halfInterCentroidDistMatrix[i][j] = dist
				kmeans.halfInterCentroidDistMatrix[j][i] = dist
			}(r, c)
		}
	}
	wg.Wait()

	// step 1.b
	//  For all centers c, compute s(c)=0.5 x min{d(c, c') | c'!= c}.
	for i := 0; i < kmeans.clusterCnt; i++ {
		currMinDist := math.MaxFloat64
		for j := 0; j < kmeans.clusterCnt; j++ {
			if i == j {
				continue
			}
			currMinDist = math.Min(currMinDist, kmeans.halfInterCentroidDistMatrix[i][j])
		}
		kmeans.minHalfInterCentroidDist[i] = currMinDist
	}
}

// assignData assigns each vector to the nearest centroid.
// This is the place where most of the pruning happens.
func (kmeans *KMeansClusterer) assignData() int {

	var ux float64 // currVecUpperBound
	var cx int     // currVecClusterAssignmentIdx
	changes := 0

	for x := range kmeans.vectorList { // x is currVectorIdx

		ux = kmeans.vectorMetas[x].upper // u(x) in the paper
		cx = kmeans.assignments[x]       // c(x) in the paper

		// step 2 u(x) <= s(c(x))
		if ux <= kmeans.minHalfInterCentroidDist[cx] {
			continue
		}

		for c := range kmeans.Centroids { // c is nextPossibleCentroidIdx
			// step 3
			// For all remaining points x and centers c such that
			// (i) c != c(x) and
			// (ii) u(x)>l(x, c) and
			// (iii) u(x)> 0.5 x d(c(x), c)
			// NOTE: we proactively use the otherwise case
			if c == cx || // (i)
				ux <= kmeans.vectorMetas[x].lower[c] || // ii)
				ux <= kmeans.halfInterCentroidDistMatrix[cx][c] { // (iii)
				continue
			}

			//step 3.a - Bounds update
			// If r(x) then compute d(x, c(x)) and assign r(x)= false. Otherwise, d(x, c(x))=u(x).
			dxcx := ux // d(x, c(x)) in the paper ie distToCentroid
			if kmeans.vectorMetas[x].recompute {
				dxcx = kmeans.distFn(kmeans.vectorList[x], kmeans.Centroids[cx])
				kmeans.vectorMetas[x].upper = dxcx
				kmeans.vectorMetas[x].lower[cx] = dxcx
				kmeans.vectorMetas[x].recompute = false
			}

			//step 3.b - Update
			// If d(x, c(x))>l(x, c) or d(x, c(x))> 0.5 d(c(x), c) then
			// Compute d(x, c)
			// If d(x, c)<d(x, c(x)) then assign c(x)=c.
			if dxcx > kmeans.vectorMetas[x].lower[c] ||
				dxcx > kmeans.halfInterCentroidDistMatrix[cx][c] {

				dxc := kmeans.distFn(kmeans.vectorList[x], kmeans.Centroids[c]) // d(x,c) in the paper
				kmeans.vectorMetas[x].lower[c] = dxc
				if dxc < dxcx {
					kmeans.vectorMetas[x].upper = dxc

					cx = c
					kmeans.assignments[x] = c
					changes++
				}
			}
		}
	}
	return changes
}

// recalculateCentroids calculates the new mean centroids based on the new assignments.
func (kmeans *KMeansClusterer) recalculateCentroids() [][]float64 {
	clusterMembersCount := make([]int64, len(kmeans.Centroids))
	clusterMembersDimWiseSum := make([][]float64, len(kmeans.Centroids))

	for c := range kmeans.Centroids {
		clusterMembersDimWiseSum[c] = make([]float64, len(kmeans.vectorList[0]))
	}

	for x, vec := range kmeans.vectorList {
		cx := kmeans.assignments[x]
		clusterMembersCount[cx]++
		for dim := range vec {
			clusterMembersDimWiseSum[cx][dim] += vec[dim]
		}
	}

	newCentroids := append([][]float64{}, kmeans.Centroids...)
	for c, newCentroid := range newCentroids {
		memberCnt := float64(clusterMembersCount[c])

		if memberCnt == 0 {
			// if the cluster is empty, reinitialize it to a random vector, since you can't find the mean of an empty set
			for l := range newCentroid {
				newCentroid[l] = 10 * (kmeans.rand.Float64() - 0.5)
			}
		} else {
			// find the mean of the cluster members
			for dim := range newCentroid {
				newCentroid[dim] = clusterMembersDimWiseSum[c][dim] / memberCnt
			}
		}

	}

	return newCentroids
}

// updateBounds updates the lower and upper bounds for each vector.
func (kmeans *KMeansClusterer) updateBounds(newCentroid [][]float64) {

	// compute the centroid shift distance matrix once.
	centroidShiftDist := make([]float64, kmeans.clusterCnt)
	var wg sync.WaitGroup
	for c := 0; c < kmeans.clusterCnt; c++ {
		wg.Add(1)
		go func(cIdx int) {
			defer wg.Done()
			centroidShiftDist[cIdx] = kmeans.distFn(kmeans.Centroids[cIdx], newCentroid[cIdx])
		}(c)
	}
	wg.Wait()

	// step 5
	//For each point x and center c, assign
	// l(x, c)= max{ l(x, c)-d(c, m(c)), 0 }
	for x := range kmeans.vectorList {
		for c := range kmeans.Centroids {
			shift := kmeans.vectorMetas[x].lower[c] - centroidShiftDist[c]
			kmeans.vectorMetas[x].lower[c] = math.Max(shift, 0)
		}

		// step 6
		// For each point x, assign
		// u(x)=u(x)+d(m(c(x)), c(x))
		// r(x)= true
		cx := kmeans.assignments[x] // ie currVecClusterAssignmentIdx
		kmeans.vectorMetas[x].upper += centroidShiftDist[cx]
		kmeans.vectorMetas[x].recompute = true
	}
}

// isConverged checks if the algorithm has converged.
func (kmeans *KMeansClusterer) isConverged(iter int, changes int) bool {
	if iter == kmeans.maxIterations ||
		changes < int(float64(kmeans.vectorCnt)*kmeans.deltaThreshold) ||
		changes == 0 {
		return true
	}
	return false
}
