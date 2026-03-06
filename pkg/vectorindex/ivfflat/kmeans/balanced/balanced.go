// Copyright 2024 Matrix Origin
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

package balanced

import (
	"context"
	"math"
	"math/rand/v2"
	"runtime"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/concurrent"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/kmeans"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

type BalancedKMeans[T types.RealNumbers] struct {
	vectorList    [][]T
	clusterCnt    int
	maxIterations int
	distFn        metric.DistanceFunction[T]
	normalize     bool
	nworker       int

	centroids   [][]T
	assignments []int
}

var _ kmeans.Clusterer = new(BalancedKMeans[float32])

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

	distanceFunction, normalize, err := metric.ResolveKmeansDistanceFn[T](distanceType, spherical)
	if err != nil {
		return nil, err
	}

	if nworker <= 0 {
		nworker = runtime.NumCPU()
	}

	return &BalancedKMeans[T]{
		vectorList:    vectors,
		clusterCnt:    clusterCnt,
		maxIterations: maxIterations,
		distFn:        distanceFunction,
		normalize:     normalize,
		nworker:       nworker,
		centroids:     make([][]T, clusterCnt),
		assignments:   make([]int, len(vectors)),
	}, nil
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
	if distanceType >= metric.Metric_TypeCount {
		return moerr.NewInternalErrorNoCtx("distance type is not supported")
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
	return nil
}

func (km *BalancedKMeans[T]) InitCentroids(ctx context.Context) error {
	// For balanced divisive k-means, initialization is inherently part of the clustering process.
	return nil
}

func (km *BalancedKMeans[T]) Close() error {
	return nil
}

type pointDiff struct {
	index int
	diff  float64
}

func (km *BalancedKMeans[T]) Cluster(ctx context.Context) (any, error) {
	if km.normalize {
		for i := range km.vectorList {
			metric.NormalizeL2(km.vectorList[i], km.vectorList[i])
		}
	}

	if len(km.vectorList) == km.clusterCnt {
		for i := 0; i < km.clusterCnt; i++ {
			km.centroids[i] = km.vectorList[i]
			km.assignments[i] = i
		}
		return km.centroids, nil
	}

	indices := make([]int, len(km.vectorList))
	for i := range indices {
		indices[i] = i
	}

	exec := concurrent.NewThreadPoolExecutor(km.nworker)
	err := km.bisectBalanced(ctx, indices, km.clusterCnt, 0, exec)
	if err != nil {
		return nil, err
	}

	return km.centroids, nil
}

func (km *BalancedKMeans[T]) bisectBalanced(
	ctx context.Context,
	indices []int,
	k int,
	clusterStart int,
	exec concurrent.ThreadPoolExecutor,
) error {
	if k == 1 {
		km.centroids[clusterStart] = computeMeanFromIndices(km.vectorList, indices)
		if km.normalize {
			metric.NormalizeL2(km.centroids[clusterStart], km.centroids[clusterStart])
		}
		for _, idx := range indices {
			km.assignments[idx] = clusterStart
		}
		return nil
	}

	n := len(indices)
	k1 := k / 2
	k2 := k - k1

	// Proportion of data
	n1 := int((int64(n) * int64(k1)) / int64(k))
	if n1 == 0 {
		n1 = 1
	}
	if n1 == n {
		n1 = n - 1
	}
	n2 := n - n1

	dim := len(km.vectorList[0])
	c1 := make([]T, dim)
	c2 := make([]T, dim)

	// Random initial centers for the bisection
	idx1 := rand.IntN(n)
	idx2 := rand.IntN(n)
	for idx1 == idx2 && n > 1 {
		idx2 = rand.IntN(n)
	}
	copy(c1, km.vectorList[indices[idx1]])
	copy(c2, km.vectorList[indices[idx2]])

	// Local assignments for bisection: 0 for left, 1 for right
	localAssign := make([]int, n)
	diffs := make([]pointDiff, n)

	for iter := 0; iter < km.maxIterations; iter++ {
		err := exec.Execute(ctx, n, func(ctx context.Context, thread_id int, start, end int) error {
			for i := start; i < end; i++ {
				vIdx := indices[i]
				d1, err1 := km.distFn(km.vectorList[vIdx], c1)
				if err1 != nil {
					return err1
				}
				d2, err2 := km.distFn(km.vectorList[vIdx], c2)
				if err2 != nil {
					return err2
				}
				// diff < 0 means closer to c1
				diffs[i] = pointDiff{index: i, diff: float64(d1) - float64(d2)}
			}
			return nil
		})
		if err != nil {
			return err
		}

		sort.Slice(diffs, func(i, j int) bool {
			return diffs[i].diff < diffs[j].diff
		})

		changed := false
		for i := 0; i < n1; i++ {
			localIdx := diffs[i].index
			if iter == 0 || localAssign[localIdx] != 0 {
				localAssign[localIdx] = 0
				changed = true
			}
		}
		for i := n1; i < n; i++ {
			localIdx := diffs[i].index
			if iter == 0 || localAssign[localIdx] != 1 {
				localAssign[localIdx] = 1
				changed = true
			}
		}

		if !changed && iter > 0 {
			break
		}

		c1 = computeMeanFromIndicesAndAssign(km.vectorList, indices, localAssign, 0, dim)
		c2 = computeMeanFromIndicesAndAssign(km.vectorList, indices, localAssign, 1, dim)
	}

	leftIndices := make([]int, 0, n1)
	rightIndices := make([]int, 0, n2)
	for i := 0; i < n; i++ {
		if localAssign[i] == 0 {
			leftIndices = append(leftIndices, indices[i])
		} else {
			rightIndices = append(rightIndices, indices[i])
		}
	}

	err := km.bisectBalanced(ctx, leftIndices, k1, clusterStart, exec)
	if err != nil {
		return err
	}

	err = km.bisectBalanced(ctx, rightIndices, k2, clusterStart+k1, exec)
	if err != nil {
		return err
	}

	return nil
}

func computeMeanFromIndicesAndAssign[T types.RealNumbers](data [][]T, indices []int, assignments []int, target int, dim int) []T {
	m := make([]T, dim)
	count := 0
	for i, a := range assignments {
		if a == target {
			vIdx := indices[i]
			for j := 0; j < dim; j++ {
				m[j] += data[vIdx][j]
			}
			count++
		}
	}
	if count > 0 {
		for j := 0; j < dim; j++ {
			m[j] /= T(count)
		}
	}
	return m
}

func computeMeanFromIndices[T types.RealNumbers](data [][]T, indices []int) []T {
	if len(indices) == 0 {
		return nil
	}
	dim := len(data[0])
	m := make([]T, dim)
	for _, vIdx := range indices {
		for j := 0; j < dim; j++ {
			m[j] += data[vIdx][j]
		}
	}
	for j := 0; j < dim; j++ {
		m[j] /= T(len(indices))
	}
	return m
}

// SSE returns the sum of squared errors.
func (km *BalancedKMeans[T]) SSE() (float64, error) {
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