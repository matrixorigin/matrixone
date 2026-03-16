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
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/concurrent"
	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
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

	// pre-allocated buffers
	indices     []int
	c1          []T
	c2          []T
	diffs       []pointDiff
	localAssign []int

	deallocators []malloc.Deallocator
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

	allocator := malloc.NewCAllocator()
	var deallocators []malloc.Deallocator

	allocSlice := func(size uint64) []byte {
		slice, deallocator, err := allocator.Allocate(size, malloc.NoClear)
		if err != nil {
			panic(err) // OOM
		}
		deallocators = append(deallocators, deallocator)
		return slice
	}

	dim := len(vectors[0])
	numVectors := len(vectors)

	// allocate centroids (outer slice + inner slices)
	centroidsBytes := allocSlice(uint64(clusterCnt) * uint64(util.UnsafeSizeOf[[]T]()))
	centroids := util.UnsafeSliceCastToLength[[]T](centroidsBytes, clusterCnt)
	for i := range centroids {
		innerBytes := allocSlice(uint64(dim) * uint64(util.UnsafeSizeOf[T]()))
		centroids[i] = util.UnsafeSliceCastToLength[T](innerBytes, dim)
	}

	// allocate assignments
	assignmentsBytes := allocSlice(uint64(numVectors) * uint64(util.UnsafeSizeOf[int]()))
	assignments := util.UnsafeSliceCastToLength[int](assignmentsBytes, numVectors)

	// allocate indices
	indicesBytes := allocSlice(uint64(numVectors) * uint64(util.UnsafeSizeOf[int]()))
	indices := util.UnsafeSliceCastToLength[int](indicesBytes, numVectors)

	// allocate c1, c2
	c1Bytes := allocSlice(uint64(dim) * uint64(util.UnsafeSizeOf[T]()))
	c1 := util.UnsafeSliceCastToLength[T](c1Bytes, dim)
	c2Bytes := allocSlice(uint64(dim) * uint64(util.UnsafeSizeOf[T]()))
	c2 := util.UnsafeSliceCastToLength[T](c2Bytes, dim)

	// allocate diffs
	diffsBytes := allocSlice(uint64(numVectors) * uint64(util.UnsafeSizeOf[pointDiff]()))
	diffs := util.UnsafeSliceCastToLength[pointDiff](diffsBytes, numVectors)

	// allocate localAssign
	localAssignBytes := allocSlice(uint64(numVectors) * uint64(util.UnsafeSizeOf[int]()))
	localAssign := util.UnsafeSliceCastToLength[int](localAssignBytes, numVectors)

	return &BalancedKMeans[T]{
		vectorList:    vectors,
		clusterCnt:    clusterCnt,
		maxIterations: maxIterations,
		distFn:        distanceFunction,
		normalize:     normalize,
		nworker:       nworker,
		centroids:     centroids,
		assignments:   assignments,
		indices:       indices,
		c1:            c1,
		c2:            c2,
		diffs:         diffs,
		localAssign:   localAssign,
		deallocators:  deallocators,
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
	for _, d := range km.deallocators {
		d.Deallocate()
	}
	km.deallocators = nil
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
			copy(km.centroids[i], km.vectorList[i])
			km.assignments[i] = i
		}
		return km.centroids, nil
	}

	for i := range km.indices {
		km.indices[i] = i
	}

	rnd := rand.New(rand.NewPCG(uint64(kmeans.DefaultRandSeed), 0))

	exec := concurrent.NewThreadPoolExecutor(km.nworker)
	err := km.bisectBalanced(ctx, km.indices, km.clusterCnt, 0, exec, km.c1, km.c2, km.diffs, km.localAssign, rnd)
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
	c1, c2 []T,
	diffs []pointDiff,
	localAssign []int,
	rnd *rand.Rand,
) error {
	if k == 1 {
		computeMeanFromIndicesInPlace(km.vectorList, indices, km.centroids[clusterStart])
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

	// Random initial centers for the bisection
	idx1 := rnd.IntN(n)
	idx2 := rnd.IntN(n)
	for idx1 == idx2 && n > 1 {
		idx2 = rnd.IntN(n)
	}
	copy(c1, km.vectorList[indices[idx1]])
	copy(c2, km.vectorList[indices[idx2]])

	// use slices for this level of recursion
	curDiffs := diffs[:n]
	curAssign := localAssign[:n]

	// Create the worker function once outside the iteration loop to avoid allocating closures
	workerFn := func(ctx context.Context, thread_id int, start, end int) error {
		for i := start; i < end; i++ {
			if (i-start)%100 == 0 && ctx.Err() != nil {
				return ctx.Err()
			}
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
			curDiffs[i] = pointDiff{index: i, diff: float64(d1) - float64(d2)}
		}
		return nil
	}

	for iter := 0; iter < km.maxIterations; iter++ {
		err := exec.Execute(ctx, n, workerFn)
		if err != nil {
			return err
		}

		slices.SortFunc(curDiffs, func(a, b pointDiff) int {
			if a.diff < b.diff {
				return -1
			} else if a.diff > b.diff {
				return 1
			}
			return 0
		})

		changed := false
		for i := 0; i < n1; i++ {
			localIdx := curDiffs[i].index
			if iter == 0 || curAssign[localIdx] != 0 {
				curAssign[localIdx] = 0
				changed = true
			}
		}
		for i := n1; i < n; i++ {
			localIdx := curDiffs[i].index
			if iter == 0 || curAssign[localIdx] != 1 {
				curAssign[localIdx] = 1
				changed = true
			}
		}

		if !changed && iter > 0 {
			break
		}

		computeMeanFromIndicesAndAssignInPlace(km.vectorList, indices, curAssign, 0, c1)
		computeMeanFromIndicesAndAssignInPlace(km.vectorList, indices, curAssign, 1, c2)
		if km.normalize {
			metric.NormalizeL2(c1, c1)
			metric.NormalizeL2(c2, c2)
		}
	}

	// In-place partition of indices based on curAssign
	left, right := 0, n-1
	for left <= right {
		for left <= right && curAssign[left] == 0 {
			left++
		}
		for left <= right && curAssign[right] == 1 {
			right--
		}
		if left < right {
			indices[left], indices[right] = indices[right], indices[left]
			curAssign[left], curAssign[right] = curAssign[right], curAssign[left]
			left++
			right--
		}
	}

	// We can reuse the buffers for the child calls since they are sequential
	err := km.bisectBalanced(ctx, indices[:n1], k1, clusterStart, exec, c1, c2, diffs, localAssign, rnd)
	if err != nil {
		return err
	}

	err = km.bisectBalanced(ctx, indices[n1:], k2, clusterStart+k1, exec, c1, c2, diffs, localAssign, rnd)
	if err != nil {
		return err
	}

	return nil
}

func computeMeanFromIndicesAndAssignInPlace[T types.RealNumbers](data [][]T, indices []int, assignments []int, target int, out []T) {
	dim := len(out)
	for j := 0; j < dim; j++ {
		out[j] = 0
	}
	count := 0
	for i, a := range assignments {
		if a == target {
			vIdx := indices[i]
			for j := 0; j < dim; j++ {
				out[j] += data[vIdx][j]
			}
			count++
		}
	}
	if count > 0 {
		for j := 0; j < dim; j++ {
			out[j] /= T(count)
		}
	}
}

func computeMeanFromIndicesInPlace[T types.RealNumbers](data [][]T, indices []int, out []T) {
	if len(indices) == 0 {
		return
	}
	dim := len(out)
	for j := 0; j < dim; j++ {
		out[j] = 0
	}
	for _, vIdx := range indices {
		for j := 0; j < dim; j++ {
			out[j] += data[vIdx][j]
		}
	}
	for j := 0; j < dim; j++ {
		out[j] /= T(len(indices))
	}
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
