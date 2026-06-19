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

type MultiGpuIvfFlat[B VectorType, Q VectorType] struct {
	indices    []*GpuIvfFlat[B, Q]
	bruteForce *GpuBruteForce[B]
	dimension  uint32
	metric     DistanceType
}

func NewMultiGpuIvfFlat[B VectorType, Q VectorType](indices []*GpuIvfFlat[B, Q], bruteForce *GpuBruteForce[B], dimension uint32, metric DistanceType) *MultiGpuIvfFlat[B, Q] {
	return &MultiGpuIvfFlat[B, Q]{indices: indices, bruteForce: bruteForce, dimension: dimension, metric: metric}
}

// All MultiIndex paths funnel through multiGpuSearch — every inner index
// (incl. the 1-index and brute-force-only configurations) dispatches via
// SearchAsync + SearchWait. After the C++ side defers SHARDED merge to
// search_wait() (plan: effervescent-hatching-dewdrop.md), there is no
// remaining reason to keep the sync fallbacks here; they bypassed dynamic
// batching and serialized through main_thread_.
//
// Storage-typed (Q) query path. When an overflow brute force is loaded it is
// base-typed (GpuBruteForce[B]), so it needs a []B query; we only have one when
// B==Q (i.e. F32/F16 storage, where storage type == base type). For the
// quantized combos (B=float/half, Q=int8/uint8) the []Q->[]B assertion fails,
// qB stays nil, and multiGpuSearchBQ's guard returns a "B/Q dispatch mismatch"
// error rather than searching — a storage-typed (already-quantized) query
// cannot be reconstructed into the base-typed query the overflow requires.
// Production code reaches the overflow via the f32 query path (SearchFloat32),
// which is unaffected; callers needing the overflow with a quantized index
// should use SearchFloat32, not this typed entry point.
func (mi *MultiGpuIvfFlat[B, Q]) Search(queries []Q, numQueries uint64, dimension uint32, limit uint32, sp IvfFlatSearchParams) ([]int64, []float32, error) {
	genericIndices := make([]GpuIndex[Q], len(mi.indices))
	for i, idx := range mi.indices {
		genericIndices[i] = idx
	}
	// Reinterpret the native Q query as []B for the base-typed overflow. Only
	// succeeds when B==Q; nil otherwise (see the method doc above).
	var qB []B
	if mi.bruteForce != nil {
		qB, _ = any(queries).([]B)
	}
	return multiGpuSearchBQ(genericIndices, mi.bruteForce, mi.dimension, queries, nil, qB, nil, numQueries, dimension, limit,
		func(idx GpuIndex[Q], q []Q, nQ uint64, d uint32, l uint32) (uint64, error) {
			return idx.(*GpuIvfFlat[B, Q]).SearchAsyncWithParams(q, nQ, d, l, sp)
		}, nil, nil, nil)
}

func (mi *MultiGpuIvfFlat[B, Q]) SearchFloat32(queries []float32, numQueries uint64, dimension uint32, limit uint32, sp IvfFlatSearchParams) ([]int64, []float32, error) {
	genericIndices := make([]GpuIndex[Q], len(mi.indices))
	for i, idx := range mi.indices {
		genericIndices[i] = idx
	}
	// f32 query: indices quantize/cast internally; overflow takes f32 (cast to B).
	return multiGpuSearchBQ(genericIndices, mi.bruteForce, mi.dimension, nil, queries, nil, queries, numQueries, dimension, limit,
		nil, func(idx GpuIndex[Q], q []float32, nQ uint64, d uint32, l uint32) (uint64, error) {
			return idx.(*GpuIvfFlat[B, Q]).SearchFloat32AsyncWithParams(q, nQ, d, l, sp)
		}, nil, nil)
}

// --- MultiGpuIvfPq ---

// MultiGpuIvfPq carries two element types: storage Q (the main cuVS ivf_pq
// indices) and base B (the CDC/overflow brute force). B==Q for a direct index;
// for a quantized index (e.g. vecf16 base -> int8 storage) B is the base type
// (Float16/float32) so the overflow brute force is cuVS-supported and lossless.
type MultiGpuIvfPq[B VectorType, Q VectorType] struct {
	indices    []*GpuIvfPq[B, Q]
	bruteForce *GpuBruteForce[B]
	dimension  uint32
	metric     DistanceType
}

func NewMultiGpuIvfPq[B VectorType, Q VectorType](indices []*GpuIvfPq[B, Q], bruteForce *GpuBruteForce[B], dimension uint32, metric DistanceType) *MultiGpuIvfPq[B, Q] {
	return &MultiGpuIvfPq[B, Q]{indices: indices, bruteForce: bruteForce, dimension: dimension, metric: metric}
}

func (mi *MultiGpuIvfPq[B, Q]) Search(queries []Q, numQueries uint64, dimension uint32, limit uint32, sp IvfPqSearchParams) ([]int64, []float32, error) {
	genericIndices := make([]GpuIndex[Q], len(mi.indices))
	for i, idx := range mi.indices {
		genericIndices[i] = idx
	}
	// Native Q query — the direct (B==Q) path; the overflow takes the same query
	// reinterpreted as []B (B==Q here).
	var qB []B
	if mi.bruteForce != nil {
		qB, _ = any(queries).([]B)
	}
	return multiGpuSearchBQ(genericIndices, mi.bruteForce, mi.dimension, queries, nil, qB, nil, numQueries, dimension, limit,
		func(idx GpuIndex[Q], q []Q, nQ uint64, d uint32, l uint32) (uint64, error) {
			return idx.(*GpuIvfPq[B, Q]).SearchAsyncWithParams(q, nQ, d, l, sp)
		}, nil, nil, nil)
}

// SearchQuantizeHalf searches a vecf16 base index whose storage is int8/uint8:
// the main indices get the half query quantized to Q (via the first index's
// half quantizer), the base-typed (Float16) overflow gets the native half query.
// Both async via the worker pool. Works overflow-only (no main index, small data).
func (mi *MultiGpuIvfPq[B, Q]) SearchQuantizeHalf(queries []Float16, numQueries uint64, dimension uint32, limit uint32, sp IvfPqSearchParams) ([]int64, []float32, error) {
	genericIndices := make([]GpuIndex[Q], len(mi.indices))
	for i, idx := range mi.indices {
		genericIndices[i] = idx
	}
	// The base type B here is Float16 (the vecf16 -> int8/uint8 quantize path);
	// reinterpret the half query as []B for the B-source query quantizer.
	queriesB, _ := any(queries).([]B)
	var qQ []Q
	if len(mi.indices) > 0 {
		qQ = make([]Q, numQueries*uint64(dimension))
		if err := mi.indices[0].QuantizeQuery(queriesB, numQueries, qQ); err != nil {
			return nil, nil, err
		}
	}
	// Overflow is base type B==Float16: search it with the native half query.
	var qB []B
	if mi.bruteForce != nil {
		qB = queriesB
	}
	return multiGpuSearchBQ(genericIndices, mi.bruteForce, mi.dimension, qQ, nil, qB, nil, numQueries, dimension, limit,
		func(idx GpuIndex[Q], q []Q, nQ uint64, d uint32, l uint32) (uint64, error) {
			return idx.(*GpuIvfPq[B, Q]).SearchAsyncWithParams(q, nQ, d, l, sp)
		}, nil, nil, nil)
}

func (mi *MultiGpuIvfPq[B, Q]) SearchFloat32(queries []float32, numQueries uint64, dimension uint32, limit uint32, sp IvfPqSearchParams) ([]int64, []float32, error) {
	genericIndices := make([]GpuIndex[Q], len(mi.indices))
	for i, idx := range mi.indices {
		genericIndices[i] = idx
	}
	// f32 query: indices quantize/cast internally; overflow takes f32 (cast to B).
	return multiGpuSearchBQ(genericIndices, mi.bruteForce, mi.dimension, nil, queries, nil, queries, numQueries, dimension, limit,
		nil, func(idx GpuIndex[Q], q []float32, nQ uint64, d uint32, l uint32) (uint64, error) {
			return idx.(*GpuIvfPq[B, Q]).SearchFloat32AsyncWithParams(q, nQ, d, l, sp)
		}, nil, nil)
}

// --- MultiGpuCagra ---

// MultiGpuCagra carries base type B (overflow) and storage type Q (cagra
// indices) — see MultiGpuIvfPq.
type MultiGpuCagra[B VectorType, Q VectorType] struct {
	indices    []*GpuCagra[B, Q]
	bruteForce *GpuBruteForce[B]
	dimension  uint32
	metric     DistanceType
}

func NewMultiGpuCagra[B VectorType, Q VectorType](indices []*GpuCagra[B, Q], bruteForce *GpuBruteForce[B], dimension uint32, metric DistanceType) *MultiGpuCagra[B, Q] {
	return &MultiGpuCagra[B, Q]{indices: indices, bruteForce: bruteForce, dimension: dimension, metric: metric}
}

func (mi *MultiGpuCagra[B, Q]) Search(queries []Q, numQueries uint64, dimension uint32, limit uint32, sp CagraSearchParams) ([]int64, []float32, error) {
	genericIndices := make([]GpuIndex[Q], len(mi.indices))
	for i, idx := range mi.indices {
		genericIndices[i] = idx
	}
	var qB []B
	if mi.bruteForce != nil {
		qB, _ = any(queries).([]B)
	}
	return multiGpuSearchBQ(genericIndices, mi.bruteForce, mi.dimension, queries, nil, qB, nil, numQueries, dimension, limit,
		func(idx GpuIndex[Q], q []Q, nQ uint64, d uint32, l uint32) (uint64, error) {
			return idx.(*GpuCagra[B, Q]).SearchAsyncWithParams(q, nQ, d, l, sp)
		}, nil, nil, nil)
}

// SearchQuantizeHalf — see MultiGpuIvfPq.SearchQuantizeHalf.
func (mi *MultiGpuCagra[B, Q]) SearchQuantizeHalf(queries []Float16, numQueries uint64, dimension uint32, limit uint32, sp CagraSearchParams) ([]int64, []float32, error) {
	genericIndices := make([]GpuIndex[Q], len(mi.indices))
	for i, idx := range mi.indices {
		genericIndices[i] = idx
	}
	// The base type B here is Float16 (the vecf16 -> int8/uint8 quantize path);
	// reinterpret the half query as []B for the B-source query quantizer.
	queriesB, _ := any(queries).([]B)
	var qQ []Q
	if len(mi.indices) > 0 {
		qQ = make([]Q, numQueries*uint64(dimension))
		if err := mi.indices[0].QuantizeQuery(queriesB, numQueries, qQ); err != nil {
			return nil, nil, err
		}
	}
	var qB []B
	if mi.bruteForce != nil {
		qB = queriesB
	}
	return multiGpuSearchBQ(genericIndices, mi.bruteForce, mi.dimension, qQ, nil, qB, nil, numQueries, dimension, limit,
		func(idx GpuIndex[Q], q []Q, nQ uint64, d uint32, l uint32) (uint64, error) {
			return idx.(*GpuCagra[B, Q]).SearchAsyncWithParams(q, nQ, d, l, sp)
		}, nil, nil, nil)
}

func (mi *MultiGpuCagra[B, Q]) SearchFloat32(queries []float32, numQueries uint64, dimension uint32, limit uint32, sp CagraSearchParams) ([]int64, []float32, error) {
	genericIndices := make([]GpuIndex[Q], len(mi.indices))
	for i, idx := range mi.indices {
		genericIndices[i] = idx
	}
	return multiGpuSearchBQ(genericIndices, mi.bruteForce, mi.dimension, nil, queries, nil, queries, numQueries, dimension, limit,
		nil, func(idx GpuIndex[Q], q []float32, nQ uint64, d uint32, l uint32) (uint64, error) {
			return idx.(*GpuCagra[B, Q]).SearchFloat32AsyncWithParams(q, nQ, d, l, sp)
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

// searchWaiter is the post-submission contract shared by GpuIndex[Q] and
// *GpuBruteForce[B]: once a search job is submitted, collecting its result is
// type-agnostic (jobID -> []int64 neighbors, []float32 distances). This lets
// multiGpuSearchBQ merge index (storage type Q) and overflow (base type B)
// results without the two types leaking into the wait/merge.
type searchWaiter interface {
	SearchWait(jobID uint64, numQueries uint64, limit uint32) ([]int64, []float32, error)
}

// multiGpuSearchBQ is multiGpuSearch with the brute-force overflow typed by the
// BASE type B (f16/f32) independently of the index storage type Q — the [B,Q]
// design. Indices are searched with the []Q (e.g. quantized) query, the
// base-typed overflow with the []B (or f32) query; both are submitted async to
// the worker pool and the post-submission collect/merge is type-agnostic
// (searchWaiter). No extra goroutine. When B==Q this is equivalent to
// multiGpuSearch with the overflow carrying the base type.
func multiGpuSearchBQ[Q VectorType, B VectorType](
	indices []GpuIndex[Q],
	bruteForce *GpuBruteForce[B],
	miDimension uint32,
	queriesQ []Q,
	queriesQF32 []float32,
	queriesB []B,
	queriesBF32 []float32,
	numQueries uint64,
	queryDimension uint32,
	limit uint32,
	idxFn func(GpuIndex[Q], []Q, uint64, uint32, uint32) (uint64, error),
	idxF32Fn func(GpuIndex[Q], []float32, uint64, uint32, uint32) (uint64, error),
	bfFn func(*GpuBruteForce[B], []B, uint64, uint32, uint32) (uint64, error),
	bfF32Fn func(*GpuBruteForce[B], []float32, uint64, uint32, uint32) (uint64, error),
) ([]int64, []float32, error) {
	if queryDimension != miDimension {
		return nil, nil, moerr.NewInternalErrorNoCtx("query dimension mismatch")
	}

	n := len(indices)
	if bruteForce != nil {
		n++
	}
	if n == 0 {
		return nil, nil, moerr.NewInternalErrorNoCtx("no indices in MultiIndex")
	}

	type jobInfo struct {
		w     searchWaiter
		jobID uint64
	}
	jobs := make([]jobInfo, 0, n)

	for _, idx := range indices {
		var jobID uint64
		var err error
		if queriesQ != nil {
			jobID, err = idxFn(idx, queriesQ, numQueries, queryDimension, limit)
		} else {
			jobID, err = idxF32Fn(idx, queriesQF32, numQueries, queryDimension, limit)
		}
		if err != nil {
			return nil, nil, err
		}
		jobs = append(jobs, jobInfo{w: idx, jobID: jobID})
	}

	if bruteForce != nil {
		// Guard against a dispatch mismatch: if the overflow brute force is
		// live but neither a base-typed (B) nor an f32 query was supplied, the
		// async search would submit an empty job (job id 0) and SearchWait(0)
		// would block forever. Fail loudly instead — this means the [B,Q]
		// instantiation disagrees with the decoded query type.
		if len(queriesB) == 0 && len(queriesBF32) == 0 {
			return nil, nil, moerr.NewInternalErrorNoCtx("multiGpuSearchBQ: brute force is loaded but no base/f32 query was provided (B/Q dispatch mismatch)")
		}
		var jobID uint64
		var err error
		if queriesB != nil {
			if bfFn != nil {
				jobID, err = bfFn(bruteForce, queriesB, numQueries, queryDimension, limit)
			} else {
				jobID, err = bruteForce.SearchAsync(queriesB, numQueries, queryDimension, limit)
			}
		} else {
			if bfF32Fn != nil {
				jobID, err = bfF32Fn(bruteForce, queriesBF32, numQueries, queryDimension, limit)
			} else {
				jobID, err = bruteForce.SearchFloat32Async(queriesBF32, numQueries, queryDimension, limit)
			}
		}
		if err != nil {
			return nil, nil, err
		}
		jobs = append(jobs, jobInfo{w: bruteForce, jobID: jobID})
	}

	allNeighbors := make([][]int64, len(jobs))
	allDistances := make([][]float32, len(jobs))
	for i, job := range jobs {
		neighbors, distances, err := job.w.SearchWait(job.jobID, numQueries, limit)
		if err != nil {
			return nil, nil, err
		}
		allNeighbors[i] = neighbors
		allDistances[i] = distances
	}

	n2, d := mergeMultiResults(allNeighbors, allDistances, numQueries, limit)
	return n2, d, nil
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

func (mi *MultiGpuCagra[B, Q]) SearchFloat32WithFilter(queries []float32, numQueries uint64, dimension uint32, limit uint32, sp CagraSearchParams, predsJSON string) ([]int64, []float32, error) {
	genericIndices := make([]GpuIndex[Q], len(mi.indices))
	for i, idx := range mi.indices {
		genericIndices[i] = idx
	}
	return multiGpuSearchBQ(genericIndices, mi.bruteForce, mi.dimension, nil, queries, nil, queries, numQueries, dimension, limit, nil, func(idx GpuIndex[Q], q []float32, nQ uint64, d uint32, l uint32) (uint64, error) {
		return idx.(*GpuCagra[B, Q]).SearchFloatWithFilterAsync(q, nQ, d, l, sp, predsJSON)
	}, nil, func(bf *GpuBruteForce[B], q []float32, nQ uint64, d uint32, l uint32) (uint64, error) {
		return bf.SearchFloatWithFilterAsync(q, nQ, d, l, predsJSON)
	})
}

func (mi *MultiGpuIvfFlat[B, Q]) SearchFloat32WithFilter(queries []float32, numQueries uint64, dimension uint32, limit uint32, sp IvfFlatSearchParams, predsJSON string) ([]int64, []float32, error) {
	genericIndices := make([]GpuIndex[Q], len(mi.indices))
	for i, idx := range mi.indices {
		genericIndices[i] = idx
	}
	return multiGpuSearchBQ(genericIndices, mi.bruteForce, mi.dimension, nil, queries, nil, queries, numQueries, dimension, limit, nil, func(idx GpuIndex[Q], q []float32, nQ uint64, d uint32, l uint32) (uint64, error) {
		return idx.(*GpuIvfFlat[B, Q]).SearchFloatWithFilterAsync(q, nQ, d, l, sp, predsJSON)
	}, nil, func(bf *GpuBruteForce[B], q []float32, nQ uint64, d uint32, l uint32) (uint64, error) {
		return bf.SearchFloatWithFilterAsync(q, nQ, d, l, predsJSON)
	})
}

func (mi *MultiGpuIvfPq[B, Q]) SearchFloat32WithFilter(queries []float32, numQueries uint64, dimension uint32, limit uint32, sp IvfPqSearchParams, predsJSON string) ([]int64, []float32, error) {
	genericIndices := make([]GpuIndex[Q], len(mi.indices))
	for i, idx := range mi.indices {
		genericIndices[i] = idx
	}
	return multiGpuSearchBQ(genericIndices, mi.bruteForce, mi.dimension, nil, queries, nil, queries, numQueries, dimension, limit, nil, func(idx GpuIndex[Q], q []float32, nQ uint64, d uint32, l uint32) (uint64, error) {
		return idx.(*GpuIvfPq[B, Q]).SearchFloatWithFilterAsync(q, nQ, d, l, sp, predsJSON)
	}, nil, func(bf *GpuBruteForce[B], q []float32, nQ uint64, d uint32, l uint32) (uint64, error) {
		return bf.SearchFloatWithFilterAsync(q, nQ, d, l, predsJSON)
	})
}
