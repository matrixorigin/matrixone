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
	Centroids [][]float64
	d         [][]float64 // centroidDist
	s         []float64   // minCentroidDist

	distFn     DistanceFunction
	clusterCnt int
	vectorCnt  int
}

type vectorMeta struct {
	l []float64 // lower
	u float64   // upper bound
	r bool      // recompute
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
			l: make([]float64, clusterCnt),
			u: 0,
			r: true,
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

		Centroids: centroids,
		d:         centroidDist,
		s:         minCentroidDist,

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
			kmeans.d[i][j] = 0.5 * kmeans.distFn(kmeans.Centroids[i], kmeans.Centroids[j])
			kmeans.d[j][i] = kmeans.d[i][j]
		}
	}

	// step 1.b
	//  For all centers c, compute s(c)=0.5 x min{d(c, c') | c'!= c}.
	for i := 0; i < kmeans.clusterCnt; i++ {
		currMinDist := math.MaxFloat64
		for j := 0; j < kmeans.clusterCnt; j++ {
			if i == j {
				continue
			}
			currMinDist = math.Min(kmeans.d[i][j], currMinDist)
		}
		kmeans.s[i] = currMinDist
	}
}

func (kmeans *ElkansKMeansClusterer) assignData() int {

	var ux float64 // currVecUpperBound
	var cx int     // currVecClusterAssignmentIdx
	changes := 0

	for x := range kmeans.vectorList { // x is currVectorIdx

		ux = kmeans.vectorMetas[x].u // u(x) in the paper
		cx = kmeans.assignments[x]   // c(x) in the paper

		// step 2 u(x) <= s(c(x))
		if ux <= kmeans.s[cx] {
			continue
		}

		for c := range kmeans.Centroids { // c is nextPossibleCentroidIdx
			// step 3
			// For all remaining points x and centers c such that
			// (i) c != c(x) and
			// (ii) u(x)>l(x, c) and
			// (iii) u(x)> 0.5 x d(c(x), c)
			// NOTE: we proactively use the otherwise case
			if c == cx && // (i)
				ux <= kmeans.vectorMetas[x].l[c] && // ii)
				ux <= kmeans.d[cx][c] { // (iii)
				continue
			}

			//step 3.a - Bounds update
			// If r(x) then compute d(x, c(x)) and assign r(x)= false. Otherwise, d(x, c(x))=u(x).
			dxcx := ux // d(x, c(x)) in the paper ie distToCentroid
			if kmeans.vectorMetas[x].r {
				dxcx = kmeans.distFn(kmeans.vectorList[x], kmeans.Centroids[cx])
				kmeans.vectorMetas[x].u = dxcx
				kmeans.vectorMetas[x].l[cx] = dxcx
				kmeans.vectorMetas[x].r = false
			}

			//step 3.b - Update
			// If d(x, c(x))>l(x, c) or d(x, c(x))> 0.5 d(c(x), c) then
			// Compute d(x, c)
			// If d(x, c)<d(x, c(x)) then assign c(x)=c.
			if dxcx > kmeans.vectorMetas[x].l[c] ||
				dxcx > kmeans.d[cx][c] {

				dxc := kmeans.distFn(kmeans.vectorList[x], kmeans.Centroids[c]) // d(x,c) in the paper
				kmeans.vectorMetas[x].l[c] = dxc
				if dxc < dxcx {
					kmeans.assignments[x] = c
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

	for c := range kmeans.Centroids {
		clusterElementsSum[c] = make([]float64, len(kmeans.vectorList[0]))
	}

	for x, vec := range kmeans.vectorList {
		clusterElementsCount[kmeans.assignments[x]]++
		for e := range vec {
			clusterElementsSum[kmeans.assignments[x]][e] += vec[e]
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

func (kmeans *ElkansKMeansClusterer) updateBounds(m [][]float64) {
	// step 5
	//For each point x and center c, assign
	// l(x, c)= max{ l(x, c)-d(c, m(c)), 0 }
	for x := range kmeans.vectorList {
		for c := range kmeans.Centroids {
			shift := kmeans.vectorMetas[x].l[c] - kmeans.distFn(kmeans.Centroids[c], m[c])
			kmeans.vectorMetas[x].l[c] = math.Max(shift, 0)
		}

		// step 6
		// For each point x, assign
		// u(x)=u(x)+d(m(c(x)), c(x))
		// r(x)= true
		cx := kmeans.assignments[x] // ie currVecClusterAssignmentIdx
		kmeans.vectorMetas[x].u += kmeans.distFn(m[cx], kmeans.Centroids[cx])
		kmeans.vectorMetas[x].r = true
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
