//go:build gpu

/*
 * Copyright 2021 Matrix Origin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cuvs

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

// MultiGpuIndex manages multiple GpuIndex instances and performs search across all of them using default parameters.
type MultiGpuIndex[T VectorType] struct {
	indices    []GpuIndex[T]
	bruteForce *GpuBruteForce[T]
	dimension  uint32
	metric     DistanceType
}

// NewMultiGpuIndex creates a new MultiGpuIndex instance.
func NewMultiGpuIndex[T VectorType](indices []GpuIndex[T], bruteForce *GpuBruteForce[T], dimension uint32, metric DistanceType) *MultiGpuIndex[T] {
	return &MultiGpuIndex[T]{
		indices:    indices,
		bruteForce: bruteForce,
		dimension:  dimension,
		metric:     metric,
	}
}

// Search performs a K-Nearest Neighbor search across all internal indices asynchronously.
func (mi *MultiGpuIndex[T]) Search(queries []T, numQueries uint64, dimension uint32, limit uint32) ([]int64, []float32, error) {
	return multiGpuSearch(mi.indices, mi.bruteForce, mi.dimension, queries, nil, numQueries, dimension, limit, func(idx GpuIndex[T], q []T, nQ uint64, d uint32, l uint32) (uint64, error) {
		return idx.SearchAsync(q, nQ, d, l)
	}, nil, nil, nil)
}

// SearchFloat32 performs a K-Nearest Neighbor search with float32 queries across all internal indices asynchronously.
func (mi *MultiGpuIndex[T]) SearchFloat32(queries []float32, numQueries uint64, dimension uint32, limit uint32) ([]int64, []float32, error) {
	return multiGpuSearch(mi.indices, mi.bruteForce, mi.dimension, nil, queries, numQueries, dimension, limit, nil, func(idx GpuIndex[T], q []float32, nQ uint64, d uint32, l uint32) (uint64, error) {
		return idx.SearchFloat32Async(q, nQ, d, l)
	}, nil, nil)
}

// Destroy destroys all internal indices.
func (mi *MultiGpuIndex[T]) Destroy() error {
	var firstErr error
	for _, idx := range mi.indices {
		if err := idx.Destroy(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if mi.bruteForce != nil {
		if err := mi.bruteForce.Destroy(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// --- MultiGpuIvfFlat ---

type MultiGpuIvfFlat[T VectorType] struct {
	indices    []*GpuIvfFlat[T]
	bruteForce *GpuBruteForce[T]
	dimension  uint32
	metric     DistanceType
}

func NewMultiGpuIvfFlat[T VectorType](indices []*GpuIvfFlat[T], bruteForce *GpuBruteForce[T], dimension uint32, metric DistanceType) *MultiGpuIvfFlat[T] {
	return &MultiGpuIvfFlat[T]{indices: indices, bruteForce: bruteForce, dimension: dimension, metric: metric}
}

// All MultiIndex paths funnel through multiGpuSearch — every inner index
// (incl. the 1-index and brute-force-only configurations) dispatches via
// SearchAsync + SearchWait. After the C++ side defers SHARDED merge to
// search_wait() (plan: effervescent-hatching-dewdrop.md), there is no
// remaining reason to keep the sync fallbacks here; they bypassed dynamic
// batching and serialized through main_thread_.
func (mi *MultiGpuIvfFlat[T]) Search(queries []T, numQueries uint64, dimension uint32, limit uint32, sp IvfFlatSearchParams) ([]int64, []float32, error) {
	genericIndices := make([]GpuIndex[T], len(mi.indices))
	for i, idx := range mi.indices {
		genericIndices[i] = idx
	}
	return multiGpuSearch(genericIndices, mi.bruteForce, mi.dimension, queries, nil, numQueries, dimension, limit, func(idx GpuIndex[T], q []T, nQ uint64, d uint32, l uint32) (uint64, error) {
		return idx.(*GpuIvfFlat[T]).SearchAsyncWithParams(q, nQ, d, l, sp)
	}, nil, nil, nil)
}

func (mi *MultiGpuIvfFlat[T]) SearchFloat32(queries []float32, numQueries uint64, dimension uint32, limit uint32, sp IvfFlatSearchParams) ([]int64, []float32, error) {
	genericIndices := make([]GpuIndex[T], len(mi.indices))
	for i, idx := range mi.indices {
		genericIndices[i] = idx
	}
	return multiGpuSearch(genericIndices, mi.bruteForce, mi.dimension, nil, queries, numQueries, dimension, limit, nil, func(idx GpuIndex[T], q []float32, nQ uint64, d uint32, l uint32) (uint64, error) {
		return idx.(*GpuIvfFlat[T]).SearchFloat32AsyncWithParams(q, nQ, d, l, sp)
	}, nil, nil)
}

// --- MultiGpuIvfPq ---

type MultiGpuIvfPq[T VectorType] struct {
	indices    []*GpuIvfPq[T]
	bruteForce *GpuBruteForce[T]
	dimension  uint32
	metric     DistanceType
}

func NewMultiGpuIvfPq[T VectorType](indices []*GpuIvfPq[T], bruteForce *GpuBruteForce[T], dimension uint32, metric DistanceType) *MultiGpuIvfPq[T] {
	return &MultiGpuIvfPq[T]{indices: indices, bruteForce: bruteForce, dimension: dimension, metric: metric}
}

func (mi *MultiGpuIvfPq[T]) Search(queries []T, numQueries uint64, dimension uint32, limit uint32, sp IvfPqSearchParams) ([]int64, []float32, error) {
	genericIndices := make([]GpuIndex[T], len(mi.indices))
	for i, idx := range mi.indices {
		genericIndices[i] = idx
	}
	return multiGpuSearch(genericIndices, mi.bruteForce, mi.dimension, queries, nil, numQueries, dimension, limit, func(idx GpuIndex[T], q []T, nQ uint64, d uint32, l uint32) (uint64, error) {
		return idx.(*GpuIvfPq[T]).SearchAsyncWithParams(q, nQ, d, l, sp)
	}, nil, nil, nil)
}

// SearchQuantizeHalf quantizes a vecf16 (half) query to the 1-byte storage type
// T (int8/uint8) via the first index's half-source quantizer, then runs the
// normal native Search([]T) (sharding/overflow/merge reused). Unfiltered.
func (mi *MultiGpuIvfPq[T]) SearchQuantizeHalf(queries []Float16, numQueries uint64, dimension uint32, limit uint32, sp IvfPqSearchParams) ([]int64, []float32, error) {
	if len(mi.indices) == 0 {
		return nil, nil, moerr.NewInternalErrorNoCtx("MultiGpuIvfPq.SearchQuantizeHalf: no main index (small-data f16->int8/uint8 overflow not yet supported)")
	}
	qT, err := mi.indices[0].QuantizeHalf(queries, numQueries, dimension)
	if err != nil {
		return nil, nil, err
	}
	return mi.Search(qT, numQueries, dimension, limit, sp)
}

func (mi *MultiGpuIvfPq[T]) SearchFloat32(queries []float32, numQueries uint64, dimension uint32, limit uint32, sp IvfPqSearchParams) ([]int64, []float32, error) {
	genericIndices := make([]GpuIndex[T], len(mi.indices))
	for i, idx := range mi.indices {
		genericIndices[i] = idx
	}
	return multiGpuSearch(genericIndices, mi.bruteForce, mi.dimension, nil, queries, numQueries, dimension, limit, nil, func(idx GpuIndex[T], q []float32, nQ uint64, d uint32, l uint32) (uint64, error) {
		return idx.(*GpuIvfPq[T]).SearchFloat32AsyncWithParams(q, nQ, d, l, sp)
	}, nil, nil)
}

// --- MultiGpuCagra ---

type MultiGpuCagra[T VectorType] struct {
	indices    []*GpuCagra[T]
	bruteForce *GpuBruteForce[T]
	dimension  uint32
	metric     DistanceType
}

func NewMultiGpuCagra[T VectorType](indices []*GpuCagra[T], bruteForce *GpuBruteForce[T], dimension uint32, metric DistanceType) *MultiGpuCagra[T] {
	return &MultiGpuCagra[T]{indices: indices, bruteForce: bruteForce, dimension: dimension, metric: metric}
}

func (mi *MultiGpuCagra[T]) Search(queries []T, numQueries uint64, dimension uint32, limit uint32, sp CagraSearchParams) ([]int64, []float32, error) {
	genericIndices := make([]GpuIndex[T], len(mi.indices))
	for i, idx := range mi.indices {
		genericIndices[i] = idx
	}
	return multiGpuSearch(genericIndices, mi.bruteForce, mi.dimension, queries, nil, numQueries, dimension, limit, func(idx GpuIndex[T], q []T, nQ uint64, d uint32, l uint32) (uint64, error) {
		return idx.(*GpuCagra[T]).SearchAsyncWithParams(q, nQ, d, l, sp)
	}, nil, nil, nil)
}

// SearchQuantizeHalf quantizes a vecf16 (half) query to the 1-byte storage type
// T (int8/uint8) via the first index's half-source quantizer, then runs the
// normal native Search([]T) (sharding/overflow/merge reused). Unfiltered.
func (mi *MultiGpuCagra[T]) SearchQuantizeHalf(queries []Float16, numQueries uint64, dimension uint32, limit uint32, sp CagraSearchParams) ([]int64, []float32, error) {
	if len(mi.indices) == 0 {
		return nil, nil, moerr.NewInternalErrorNoCtx("MultiGpuCagra.SearchQuantizeHalf: no main index (small-data f16->int8/uint8 overflow not yet supported)")
	}
	qT, err := mi.indices[0].QuantizeHalf(queries, numQueries, dimension)
	if err != nil {
		return nil, nil, err
	}
	return mi.Search(qT, numQueries, dimension, limit, sp)
}

func (mi *MultiGpuCagra[T]) SearchFloat32(queries []float32, numQueries uint64, dimension uint32, limit uint32, sp CagraSearchParams) ([]int64, []float32, error) {
	genericIndices := make([]GpuIndex[T], len(mi.indices))
	for i, idx := range mi.indices {
		genericIndices[i] = idx
	}
	return multiGpuSearch(genericIndices, mi.bruteForce, mi.dimension, nil, queries, numQueries, dimension, limit, nil, func(idx GpuIndex[T], q []float32, nQ uint64, d uint32, l uint32) (uint64, error) {
		return idx.(*GpuCagra[T]).SearchFloat32AsyncWithParams(q, nQ, d, l, sp)
	}, nil, nil)
}

// --- Helper search function ---

// multiGpuSearch dispatches per-index searches asynchronously and merges the
// per-index top-k. The brute-force fallback (if any) is dispatched alongside
// the indexed shards. bfSearchFn / bfSearchF32Fn override the default
// bruteForce.SearchAsync / SearchFloat32Async dispatch — leave them nil for
// unfiltered callers, set them in the filter path so the brute-force fallback
// uses SearchFloatWithFilterAsync.
func multiGpuSearch[T VectorType](
	indices []GpuIndex[T],
	bruteForce *GpuBruteForce[T],
	miDimension uint32,
	queries []T,
	queriesF32 []float32,
	numQueries uint64,
	queryDimension uint32,
	limit uint32,
	searchFn func(GpuIndex[T], []T, uint64, uint32, uint32) (uint64, error),
	searchF32Fn func(GpuIndex[T], []float32, uint64, uint32, uint32) (uint64, error),
	bfSearchFn func(*GpuBruteForce[T], []T, uint64, uint32, uint32) (uint64, error),
	bfSearchF32Fn func(*GpuBruteForce[T], []float32, uint64, uint32, uint32) (uint64, error),
) ([]int64, []float32, error) {
	if queryDimension != miDimension {
		return nil, nil, moerr.NewInternalErrorNoCtx("query dimension mismatch")
	}

	numIndices := len(indices)
	if bruteForce != nil {
		numIndices++
	}

	if numIndices == 0 {
		return nil, nil, moerr.NewInternalErrorNoCtx("no indices in MultiIndex")
	}

	type jobInfo struct {
		index GpuIndex[T]
		jobID uint64
	}
	jobs := make([]jobInfo, 0, numIndices)

	for _, idx := range indices {
		var jobID uint64
		var err error
		if queries != nil {
			jobID, err = searchFn(idx, queries, numQueries, queryDimension, limit)
		} else {
			jobID, err = searchF32Fn(idx, queriesF32, numQueries, queryDimension, limit)
		}
		if err != nil {
			return nil, nil, err
		}
		jobs = append(jobs, jobInfo{index: idx, jobID: jobID})
	}

	if bruteForce != nil {
		var jobID uint64
		var err error
		if queries != nil {
			if bfSearchFn != nil {
				jobID, err = bfSearchFn(bruteForce, queries, numQueries, queryDimension, limit)
			} else {
				jobID, err = bruteForce.SearchAsync(queries, numQueries, queryDimension, limit)
			}
		} else {
			if bfSearchF32Fn != nil {
				jobID, err = bfSearchF32Fn(bruteForce, queriesF32, numQueries, queryDimension, limit)
			} else {
				jobID, err = bruteForce.SearchFloat32Async(queriesF32, numQueries, queryDimension, limit)
			}
		}
		if err != nil {
			return nil, nil, err
		}
		jobs = append(jobs, jobInfo{index: bruteForce, jobID: jobID})
	}

	allNeighbors := make([][]int64, len(jobs))
	allDistances := make([][]float32, len(jobs))

	for i, job := range jobs {
		neighbors, distances, err := job.index.SearchWait(job.jobID, numQueries, limit)
		if err != nil {
			return nil, nil, err
		}
		allNeighbors[i] = neighbors
		allDistances[i] = distances
	}

	n, d := mergeMultiResults(allNeighbors, allDistances, numQueries, limit)
	return n, d, nil
}

// mergeMultiResults does a k-way merge of per-index top-k results into a single
// top-k per query using a max-heap. Empty slots (neighbor == -1) are skipped.
// Shared by both the async-dispatched multiGpuSearch and the synchronous
// filtered search variants.
func mergeMultiResults(allNeighbors [][]int64, allDistances [][]float32, numQueries uint64, limit uint32) ([]int64, []float32) {
	finalNeighbors := make([]int64, numQueries*uint64(limit))
	finalDistances := make([]float32, numQueries*uint64(limit))

	for q := uint64(0); q < numQueries; q++ {
		keysBuf := make([]int64, limit)
		distsBuf := make([]float32, limit)
		heap := vectorindex.NewFastMaxHeap[float32, int64](int(limit), keysBuf, distsBuf)

		for i := 0; i < len(allNeighbors); i++ {
			offset := q * uint64(limit)
			for k := uint32(0); k < limit; k++ {
				idx := offset + uint64(k)
				neighbor := allNeighbors[i][idx]
				if neighbor != -1 {
					heap.Push(neighbor, allDistances[i][idx])
				}
			}
		}

		for k := int(limit) - 1; k >= 0; k-- {
			key, dist, ok := heap.Pop()
			if ok {
				finalNeighbors[q*uint64(limit)+uint64(k)] = key
				finalDistances[q*uint64(limit)+uint64(k)] = dist
			} else {
				finalNeighbors[q*uint64(limit)+uint64(k)] = -1
				finalDistances[q*uint64(limit)+uint64(k)] = 3.402823466e+38 // Max float32
			}
		}
	}

	return finalNeighbors, finalDistances
}

// --- Filtered async search variants ---
//
// Every per-index filtered search is dispatched via SearchFloatWithFilterAsync
// (which returns a job_id) and collected with SearchWait, matching the
// unfiltered SearchFloat32 path. Predicate evaluation, H2D, and GPU work for
// sibling indices overlap on their own worker threads, including the
// brute-force fallback when mi.bruteForce is non-nil.
//
// SHARDED inner indices no longer get routed through main_thread_ — see the
// C++ search_*_with_filter_async branches and plan
// .claude/plans/effervescent-hatching-dewdrop.md.

func (mi *MultiGpuCagra[T]) SearchFloat32WithFilter(queries []float32, numQueries uint64, dimension uint32, limit uint32, sp CagraSearchParams, predsJSON string) ([]int64, []float32, error) {
	genericIndices := make([]GpuIndex[T], len(mi.indices))
	for i, idx := range mi.indices {
		genericIndices[i] = idx
	}
	return multiGpuSearch(genericIndices, mi.bruteForce, mi.dimension, nil, queries, numQueries, dimension, limit, nil, func(idx GpuIndex[T], q []float32, nQ uint64, d uint32, l uint32) (uint64, error) {
		return idx.(*GpuCagra[T]).SearchFloatWithFilterAsync(q, nQ, d, l, sp, predsJSON)
	}, nil, func(bf *GpuBruteForce[T], q []float32, nQ uint64, d uint32, l uint32) (uint64, error) {
		return bf.SearchFloatWithFilterAsync(q, nQ, d, l, predsJSON)
	})
}

func (mi *MultiGpuIvfFlat[T]) SearchFloat32WithFilter(queries []float32, numQueries uint64, dimension uint32, limit uint32, sp IvfFlatSearchParams, predsJSON string) ([]int64, []float32, error) {
	genericIndices := make([]GpuIndex[T], len(mi.indices))
	for i, idx := range mi.indices {
		genericIndices[i] = idx
	}
	return multiGpuSearch(genericIndices, mi.bruteForce, mi.dimension, nil, queries, numQueries, dimension, limit, nil, func(idx GpuIndex[T], q []float32, nQ uint64, d uint32, l uint32) (uint64, error) {
		return idx.(*GpuIvfFlat[T]).SearchFloatWithFilterAsync(q, nQ, d, l, sp, predsJSON)
	}, nil, func(bf *GpuBruteForce[T], q []float32, nQ uint64, d uint32, l uint32) (uint64, error) {
		return bf.SearchFloatWithFilterAsync(q, nQ, d, l, predsJSON)
	})
}

func (mi *MultiGpuIvfPq[T]) SearchFloat32WithFilter(queries []float32, numQueries uint64, dimension uint32, limit uint32, sp IvfPqSearchParams, predsJSON string) ([]int64, []float32, error) {
	genericIndices := make([]GpuIndex[T], len(mi.indices))
	for i, idx := range mi.indices {
		genericIndices[i] = idx
	}
	return multiGpuSearch(genericIndices, mi.bruteForce, mi.dimension, nil, queries, numQueries, dimension, limit, nil, func(idx GpuIndex[T], q []float32, nQ uint64, d uint32, l uint32) (uint64, error) {
		return idx.(*GpuIvfPq[T]).SearchFloatWithFilterAsync(q, nQ, d, l, sp, predsJSON)
	}, nil, func(bf *GpuBruteForce[T], q []float32, nQ uint64, d uint32, l uint32) (uint64, error) {
		return bf.SearchFloatWithFilterAsync(q, nQ, d, l, predsJSON)
	})
}
