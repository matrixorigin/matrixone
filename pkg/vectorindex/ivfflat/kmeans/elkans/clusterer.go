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
	"context"
	"math"
	"math/rand"
	"runtime"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/concurrent"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/kmeans"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
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
type ElkanClusterer[T types.RealNumbers] struct {

	// for each of the n vectors, we keep track of the following data
	vectorList  [][]T
	vectorMetas []vectorMeta[T]
	assignments []int

	// for each of the k centroids, we keep track of the following data
	centroids                   [][]T
	halfInterCentroidDistMatrix [][]T
	minHalfInterCentroidDist    []T

	// thresholds
	maxIterations  int     // e in paper
	deltaThreshold float64 // used for early convergence. we are not using it right now.

	// counts
	clusterCnt int // k in paper
	vectorCnt  int // n in paper

	distFn    metric.DistanceFunction[T]
	initType  kmeans.InitType
	rand      *rand.Rand
	normalize bool

	// number of worker threads
	nworker int
}

// vectorMeta holds required information for Elkan's kmeans pruning.
// lower is at-least distance of a vector to each of the k centroids. Thus, there are k values for each data point.
// upper is at-most (maximum possible) distance of a vector to its currently assigned or "closest" centroid.
// Hence, there's only one value for each data point.
// recompute is a flag to indicate if the distance to centroids needs to be recomputed. if false,
// the algorithm will rely on the 'upper' bound as an approximation instead of computing the exact distance.
type vectorMeta[T types.RealNumbers] struct {
	lower     []T
	upper     T
	recompute bool
}

// var _ kmeans.Clusterer = new(ElkanClusterer)

func NewKMeans[T types.RealNumbers](vectors [][]T, clusterCnt,
	maxIterations int, deltaThreshold float64,
	distanceType metric.MetricType, initType kmeans.InitType,
	spherical bool,
	nworker int,
) (kmeans.Clusterer, error) {

	err := validateArgs[T](vectors, clusterCnt, maxIterations, deltaThreshold, distanceType, initType)
	if err != nil {
		return nil, err
	}

	assignments := make([]int, len(vectors))
	var metas = make([]vectorMeta[T], len(vectors))
	for i := range metas {
		metas[i] = vectorMeta[T]{
			lower:     make([]T, clusterCnt),
			upper:     0,
			recompute: true,
		}
	}

	centroidDist := make([][]T, clusterCnt)
	for i := range centroidDist {
		centroidDist[i] = make([]T, clusterCnt)
	}
	minCentroidDist := make([]T, clusterCnt)

	distanceFunction, normalize, err := metric.ResolveKmeansDistanceFn[T](distanceType, spherical)
	if err != nil {
		return nil, err
	}

	if nworker <= 0 {
		nworker = runtime.NumCPU()
	}

	return &ElkanClusterer[T]{
		maxIterations:  maxIterations,
		deltaThreshold: deltaThreshold,

		vectorList:  vectors,
		assignments: assignments,
		vectorMetas: metas,

		//centroids will be initialized by InitCentroids()
		halfInterCentroidDistMatrix: centroidDist,
		minHalfInterCentroidDist:    minCentroidDist,

		distFn:     distanceFunction,
		initType:   initType,
		clusterCnt: clusterCnt,
		vectorCnt:  len(vectors),

		rand:      rand.New(rand.NewSource(kmeans.DefaultRandSeed)),
		normalize: normalize,
		nworker:   nworker,
	}, nil
}

func (km *ElkanClusterer[T]) Close() error {
	return nil
}

// InitCentroids initializes the centroids using initialization algorithms like random or kmeans++.
func (km *ElkanClusterer[T]) InitCentroids(ctx context.Context) error {
	var initializer Initializer
	switch km.initType {
	case kmeans.Random:
		initializer = NewRandomInitializer()
	case kmeans.KmeansPlusPlus:
		initializer = NewKMeansPlusPlusInitializer[T](km.distFn)
	default:
		initializer = NewRandomInitializer()
	}
	anycentroids, err := initializer.InitCentroids(ctx, km.vectorList, km.clusterCnt)
	if err != nil {
		return err
	}

	var ok bool
	km.centroids, ok = anycentroids.([][]T)
	if !ok {
		return moerr.NewInternalErrorNoCtx("InitCentroids not return [][]float32|float64")
	}
	return nil
}

// Cluster returns the final centroids and the error if any.
func (km *ElkanClusterer[T]) Cluster(ctx context.Context) (any, error) {
	if km.normalize {
		for i := range km.vectorList {
			metric.NormalizeL2(km.vectorList[i], km.vectorList[i])
		}
	}

	if km.vectorCnt == km.clusterCnt {
		return km.vectorList, nil
	}

	err := km.InitCentroids(ctx) // step 0.1
	if err != nil {
		return nil, err
	}

	km.initBounds(ctx) // step 0.2

	return km.elkansCluster(ctx)
}

func (km *ElkanClusterer[T]) elkansCluster(ctx context.Context) ([][]T, error) {

	for iter := 0; ; iter++ {
		km.computeCentroidDistances(ctx) // step 1

		changes, err := km.assignData(ctx) // step 2 and 3
		if err != nil {
			return nil, err
		}

		newCentroids := km.recalculateCentroids(ctx) // step 4

		km.updateBounds(ctx, newCentroids) // step 5 and 6

		km.centroids = newCentroids // step 7

		logutil.Debugf("kmeans iter=%d, changes=%d\n", iter, changes)
		if iter != 0 && km.isConverged(iter, changes) {
			break
		}
	}
	return km.centroids, nil
}

func validateArgs[T types.RealNumbers](vectorList [][]T, clusterCnt,
	maxIterations int, deltaThreshold float64,
	distanceType metric.MetricType, initType kmeans.InitType) error {
	if len(vectorList) == 0 || len(vectorList[0]) == 0 {
		return moerr.NewInternalErrorNoCtx("input vectors is empty")
	}
	if clusterCnt > len(vectorList) {
		return moerr.NewInternalErrorNoCtxf("cluster count is larger than vector count %d > %d", clusterCnt, len(vectorList))
	}
	if maxIterations < 0 {
		return moerr.NewInternalErrorNoCtxf("max iteration is out of bounds (must be >= 0)")
	}
	if deltaThreshold <= 0.0 || deltaThreshold >= 1.0 {
		return moerr.NewInternalErrorNoCtx("delta threshold is out of bounds (must be > 0.0 and < 1.0)")
	}
	if distanceType >= metric.Metric_TypeCount {
		return moerr.NewInternalErrorNoCtx("distance type is not supported")
	}
	if initType > 1 {
		return moerr.NewInternalErrorNoCtx("init type is not supported")
	}

	vlen := -1
	for _, v := range vectorList {
		if vlen == -1 {
			vlen = len(v)
		}
		if vlen != len(v) {
			return moerr.NewInternalErrorNoCtx("input vectors not in same dimension")
		}
	}
	// We need to validate that all vectors have the same dimension.
	// This is already done by moarray.ToGonumVectors, so skipping it here.

	if (clusterCnt * clusterCnt) > math.MaxInt {
		return moerr.NewInternalErrorNoCtx("cluster count is too large for int*int")
	}

	return nil
}

// initBounds initializes the lower bounds, upper bound and assignment for each vector.
func (km *ElkanClusterer[T]) initBounds(ctx context.Context) (err error) {
	// step 0.2
	// Set the lower bound l(x, c)=0 for each point x and center c.
	// Assign each x to its closest initial center c(x)=min{ d(x, c) }, using Lemma 1 to avoid
	// redundant distance calculations. Each time d(x, c) is computed, set l(x, c)=d(x, c).
	// Assign upper bounds u(x)=min_c d(x, c).

	ncpu := km.nworker
	if len(km.vectorList) < ncpu {
		ncpu = len(km.vectorList)
	}

	exec := concurrent.NewThreadPoolExecutor(ncpu)
	err = exec.Execute(
		ctx,
		len(km.vectorList),
		func(ctx context.Context, thread_id int, start, end int) (err2 error) {
			subvec := km.vectorList[start:end]
			submetas := km.vectorMetas[start:end]
			subassigns := km.assignments[start:end]

			for x := range subvec {

				if x%100 == 0 && ctx.Err() != nil {
					return ctx.Err()
				}

				minDist := metric.MaxFloat[T]()
				closestCenter := 0
				for c := range km.centroids {
					dist, err2 := km.distFn(subvec[x], km.centroids[c])
					if err2 != nil {
						return err2
					}

					submetas[x].lower[c] = dist
					if dist < minDist {
						minDist = dist
						closestCenter = c
					}
				}

				submetas[x].upper = minDist
				subassigns[x] = closestCenter
			}
			return
		})

	return
}

// computeCentroidDistances computes the centroid distances and the min centroid distances.
// NOTE: here we are save 0.5 of centroid distance to avoid 0.5 multiplication in step 3(iii) and 3.b.
func (km *ElkanClusterer[T]) computeCentroidDistances(ctx context.Context) error {

	// step 1.a
	// For all centers c and c', compute 0.5 x d(c, c').
	ncpu := km.nworker
	if km.clusterCnt < ncpu {
		ncpu = km.clusterCnt
	}

	exec := concurrent.NewThreadPoolExecutor(ncpu)
	err := exec.Execute(
		ctx,
		km.clusterCnt,
		func(ctx context.Context, thread_id int, start, end int) error {
			subcentroids := km.centroids[start:end]

			for x := range subcentroids {

				if x%100 == 0 && ctx.Err() != nil {
					return ctx.Err()
				}

				i := start + x
				for j := i + 1; j < km.clusterCnt; j++ {
					dist, err2 := km.distFn(subcentroids[x], km.centroids[j])
					if err2 != nil {
						return err2
					}
					dist *= 0.5
					km.halfInterCentroidDistMatrix[i][j] = dist
					km.halfInterCentroidDistMatrix[j][i] = dist
				}
			}

			return nil
		})

	if err != nil {
		return err
	}

	// step 1.b
	//  For all centers c, compute s(c)=0.5 x min{d(c, c') | c'!= c}.
	for i := 0; i < km.clusterCnt; i++ {
		currMinDist := T(math.MaxFloat32)
		for j := 0; j < km.clusterCnt; j++ {
			if i == j {
				continue
			}
			currMinDist = T(math.Min(float64(currMinDist), float64(km.halfInterCentroidDistMatrix[i][j])))
		}
		km.minHalfInterCentroidDist[i] = currMinDist
	}

	return nil
}

// assignData assigns each vector to the nearest centroid.
// This is the place where most of the "distance computation skipping" happens.
func (km *ElkanClusterer[T]) assignData(ctx context.Context) (int, error) {
	var changes atomic.Int64
	ncpu := km.nworker
	if len(km.vectorList) < ncpu {
		ncpu = len(km.vectorList)
	}

	exec := concurrent.NewThreadPoolExecutor(ncpu)
	err := exec.Execute(
		ctx,
		len(km.vectorList),
		func(ctx context.Context, thread_id int, start, end int) (err2 error) {
			subvec := km.vectorList[start:end]
			submetas := km.vectorMetas[start:end]
			subassigns := km.assignments[start:end]

			for currVector := range subvec {

				if currVector%100 == 0 && ctx.Err() != nil {
					return ctx.Err()
				}

				// step 2
				// u(x) <= s(c(x))
				if submetas[currVector].upper <= km.minHalfInterCentroidDist[subassigns[currVector]] {
					continue
				}

				for c := range km.centroids { // c is nextPossibleCentroidIdx
					// step 3
					// For all remaining points x and centers c such that
					// (i) c != c(x) and
					// (ii) u(x)>l(x, c) and
					// (iii) u(x)> 0.5 x d(c(x), c)
					if c != subassigns[currVector] &&
						submetas[currVector].upper > submetas[currVector].lower[c] &&
						submetas[currVector].upper > km.halfInterCentroidDistMatrix[subassigns[currVector]][c] {

						//step 3.a - Bounds update
						// If r(x) then compute d(x, c(x)) and assign r(x)= false.
						var dxcx T
						if submetas[currVector].recompute {
							submetas[currVector].recompute = false

							dxcx, err2 = km.distFn(subvec[currVector], km.centroids[subassigns[currVector]])
							if err2 != nil {
								return err2
							}
							submetas[currVector].upper = dxcx
							submetas[currVector].lower[subassigns[currVector]] = dxcx

							if submetas[currVector].upper <= submetas[currVector].lower[c] {
								continue // Pruned by triangle inequality on lower bound.
							}

							if submetas[currVector].upper <= km.halfInterCentroidDistMatrix[subassigns[currVector]][c] {
								continue // Pruned by triangle inequality on cluster distances.
							}

						} else {
							dxcx = submetas[currVector].upper //  Otherwise, d(x, c(x))=u(x).
						}

						//step 3.b - Update
						// If d(x, c(x))>l(x, c) or d(x, c(x))> 0.5 d(c(x), c) then
						// Compute d(x, c)
						// If d(x, c)<d(x, c(x)) then assign c(x)=c.
						if dxcx > submetas[currVector].lower[c] ||
							dxcx > km.halfInterCentroidDistMatrix[subassigns[currVector]][c] {

							dxc, err2 := km.distFn(subvec[currVector], km.centroids[c]) // d(x,c) in the paper
							if err2 != nil {
								return err2
							}
							submetas[currVector].lower[c] = dxc
							if dxc < dxcx {
								submetas[currVector].upper = dxc
								subassigns[currVector] = c
								changes.Add(1)
							}
						}
					}
				}
			}
			return
		})

	if err != nil {
		return 0, err
	}

	return int(changes.Load()), nil
}

// recalculateCentroids calculates the new mean centroids based on the new assignments.
func (km *ElkanClusterer[T]) recalculateCentroids(ctx context.Context) [][]T {
	membersCount := make([]int64, km.clusterCnt)

	newCentroids := make([][]T, km.clusterCnt)
	for c := range newCentroids {
		newCentroids[c] = make([]T, len(km.vectorList[0]))
	}

	// sum of all the members of the cluster
	for x, vec := range km.vectorList {
		cx := km.assignments[x]
		membersCount[cx]++
		for i := range vec {
			newCentroids[cx][i] += vec[i]
		}
	}

	// means of the clusters = sum of all the members of the cluster / number of members in the cluster
	for c := range newCentroids {
		if membersCount[c] == 0 {
			// pick a vector randomly from existing vectors as the new centroid
			//newCentroids[c] = km.vectorList[km.rand.Intn(km.vectorCnt)]

			//// if the cluster is empty, reinitialize it to a random vector, since you can't find the mean of an empty set
			randVector := make([]T, len(km.vectorList[0]))
			for l := range randVector {
				randVector[l] = T(km.rand.Float32())
			}
			newCentroids[c] = randVector

			// normalize the random vector
			if km.normalize {
				metric.NormalizeL2(newCentroids[c], newCentroids[c])
			}
		} else {
			// find the mean of the cluster members
			// note: we don't need to normalize here, since the vectors are already normalized
			metric.ScaleInPlace[T](newCentroids[c], 1.0/T(membersCount[c]))
		}

	}

	return newCentroids
}

// updateBounds updates the lower and upper bounds for each vector.
func (km *ElkanClusterer[T]) updateBounds(ctx context.Context, newCentroid [][]T) (err error) {

	// compute the centroid shift distance matrix once.
	// d(c', m(c')) in the paper
	centroidShiftDist := make([]T, km.clusterCnt)
	for c := 0; c < km.clusterCnt; c++ {
		centroidShiftDist[c], err = km.distFn(km.centroids[c], newCentroid[c])
		if err != nil {
			return err
		}
		//logutil.Debugf("centroidShiftDist[%d]=%f", c, centroidShiftDist[c])
	}

	// step 5
	//For each point x and center c, assign
	// l(x, c)= max{ l(x, c)-d(c, m(c)), 0 }
	for x := range km.vectorList {
		for c := range km.centroids {
			shift := km.vectorMetas[x].lower[c] - centroidShiftDist[c]
			km.vectorMetas[x].lower[c] = T(math.Max(float64(shift), 0))
		}

		// step 6
		// For each point x, assign
		// u(x)= u(x) + d(m(c(x)), c(x))
		// r(x)= true
		cx := km.assignments[x]
		km.vectorMetas[x].upper += centroidShiftDist[cx]
		km.vectorMetas[x].recompute = true
	}

	return nil
}

// isConverged checks if the algorithm has converged.
func (km *ElkanClusterer[T]) isConverged(iter int, changes int) bool {
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
func (km *ElkanClusterer[T]) SSE() (float64, error) {
	sse := 0.0
	for i := range km.vectorList {
		distErr, err := km.distFn(km.vectorList[i], km.centroids[km.assignments[i]])
		if err != nil {
			return 0, err
		}
		sse += math.Pow(float64(distErr), 2)
	}
	return sse, nil
}
