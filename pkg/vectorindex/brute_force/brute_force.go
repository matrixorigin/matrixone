// Copyright 2022 Matrix Origin
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

package brute_force

import (
	"context"
	"fmt"
	"runtime"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/concurrent"
	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	usearch "github.com/unum-cloud/usearch/golang"
)

var (
	pool1DI64 = sync.Pool{New: func() any { x := make([]int64, 0); return &x }}
	pool1DF32 = sync.Pool{New: func() any { x := make([]float32, 0); return &x }}
	pool1DF64 = sync.Pool{New: func() any { x := make([]float64, 0); return &x }}
	pool1DInt = sync.Pool{New: func() any { x := make([]int, 0); return &x }}
)

func get1D[T any](pool *sync.Pool, n int) *[]T {
	val := pool.Get()
	if val == nil {
		newSlice := make([]T, n)
		return &newSlice
	}
	v, ok := val.(*[]T)
	if !ok || v == nil {
		newSlice := make([]T, n)
		return &newSlice
	}
	if cap(*v) < n {
		if n > 0 {
			pool.Put(v)
			newSlice := make([]T, n)
			return &newSlice
		}
		*v = (*v)[:0]
		return v
	}
	*v = (*v)[:n]
	return v
}

func put1D[T any](pool *sync.Pool, v *[]T) {
	var zero T
	for i := range *v {
		(*v)[i] = zero
	}
	*v = (*v)[:0]
	pool.Put(v)
}

type UsearchBruteForceIndex[T types.RealNumbers] struct {
	Dataset      *[]T // flattend vector
	Metric       usearch.Metric
	Dimension    uint
	Count        uint
	Quantization usearch.Quantization
	ElementSize  uint
	deallocator  malloc.Deallocator
}

type GoBruteForceIndex[T types.RealNumbers] struct {
	Dataset   [][]T // flattend vector
	Metric    metric.MetricType
	Dimension uint
	Count     uint
}

var _ cache.VectorIndexSearchIf = &UsearchBruteForceIndex[float32]{}
var _ cache.VectorIndexSearchIf = &GoBruteForceIndex[float32]{}

func GetUsearchQuantizationFromType(v any) (usearch.Quantization, error) {
	switch v.(type) {
	case float32:
		return usearch.F32, nil
	case float64:
		return usearch.F64, nil
	default:
		return 0, moerr.NewInternalErrorNoCtx(fmt.Sprintf("usearch not support type %T", v))
	}
}

func NewCpuBruteForceIndex[T types.RealNumbers](dataset [][]T,
	dimension uint,
	m metric.MetricType,
	elemsz uint) (cache.VectorIndexSearchIf, error) {

	return NewGoBruteForceIndex(dataset, dimension, m, elemsz)
}

func NewGoBruteForceIndex[T types.RealNumbers](dataset [][]T,
	dimension uint,
	m metric.MetricType,
	elemsz uint) (cache.VectorIndexSearchIf, error) {

	idx := &GoBruteForceIndex[T]{}
	idx.Metric = m
	idx.Dimension = dimension
	idx.Count = uint(len(dataset))
	idx.Dataset = dataset
	return idx, nil
}

func NewUsearchBruteForceIndex[T types.RealNumbers](dataset [][]T,
	dimension uint,
	m metric.MetricType,
	elemsz uint) (cache.VectorIndexSearchIf, error) {
	var err error

	idx := &UsearchBruteForceIndex[T]{}
	idx.Metric = metric.MetricTypeToUsearchMetric[m]
	idx.Quantization, err = GetUsearchQuantizationFromType(T(0))
	if err != nil {
		return nil, err
	}
	idx.Dimension = dimension
	idx.Count = uint(len(dataset))
	idx.ElementSize = elemsz

	reqSize := int(idx.Count * idx.Dimension)

	allocator := malloc.NewCAllocator()

	var _t T
	switch any(_t).(type) {
	case float32:
		slice, deallocator, err := allocator.Allocate(uint64(reqSize)*4, malloc.NoClear)
		if err != nil {
			return nil, err
		}
		idx.deallocator = deallocator
		f32Slice := util.UnsafeSliceCastToLength[float32](slice, reqSize)
		idx.Dataset = any(&f32Slice).(*[]T)
	case float64:
		slice, deallocator, err := allocator.Allocate(uint64(reqSize)*8, malloc.NoClear)
		if err != nil {
			return nil, err
		}
		idx.deallocator = deallocator
		f64Slice := util.UnsafeSliceCastToLength[float64](slice, reqSize)
		idx.Dataset = any(&f64Slice).(*[]T)
	default:
		// Fallback
		ds := make([]T, reqSize)
		idx.Dataset = &ds
	}

	ds := *idx.Dataset
	for i := 0; i < len(dataset); i++ {
		offset := i * int(dimension)
		copy(ds[offset:], dataset[i])
	}

	return idx, nil
}

func (idx *UsearchBruteForceIndex[T]) Load(sqlproc *sqlexec.SqlProcess) error {
	return nil
}

func (idx *UsearchBruteForceIndex[T]) Search(proc *sqlexec.SqlProcess, _queries any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {
	queries, ok := _queries.([][]T)
	if !ok {
		return nil, nil, moerr.NewInternalErrorNoCtx("queries type invalid")
	}

	var flatten []T
	var queryDeallocator malloc.Deallocator

	reqSize := len(queries) * int(idx.Dimension)
	allocator := malloc.NewCAllocator()
	var _t T
	switch any(_t).(type) {
	case float32:
		slice, dealloc, err2 := allocator.Allocate(uint64(reqSize)*4, malloc.NoClear)
		if err2 != nil {
			return nil, nil, err2
		}
		queryDeallocator = dealloc
		f32Slice := util.UnsafeSliceCastToLength[float32](slice, reqSize)
		flatten = any(f32Slice).([]T)
	case float64:
		slice, dealloc, err2 := allocator.Allocate(uint64(reqSize)*8, malloc.NoClear)
		if err2 != nil {
			return nil, nil, err2
		}
		queryDeallocator = dealloc
		f64Slice := util.UnsafeSliceCastToLength[float64](slice, reqSize)
		flatten = any(f64Slice).([]T)
	}

	for i := 0; i < len(queries); i++ {
		offset := i * int(idx.Dimension)
		copy(flatten[offset:], queries[i])
	}

	if queryDeallocator != nil {
		defer queryDeallocator.Deallocate()
	}
	//fmt.Printf("flattened %v\n", flatten)

	// limit must be less than idx.Count
	limit := rt.Limit
	if limit > idx.Count {
		limit = idx.Count
	}

	keys_ui64, distances_f32, err := usearch.ExactSearchUnsafe(
		util.UnsafePointer(&((*idx.Dataset)[0])),
		util.UnsafePointer(&(flatten[0])),
		uint(idx.Count),
		uint(len(queries)),
		idx.Dimension*idx.ElementSize,
		idx.Dimension*idx.ElementSize,
		idx.Dimension,
		idx.Metric,
		idx.Quantization,
		limit,
		rt.NThreads)

	if err != nil {
		return
	}

	distances = make([]float64, len(distances_f32))
	for i, dist := range distances_f32 {
		distances[i] = float64(dist)
	}

	keys_i64 := make([]int64, len(keys_ui64))
	for i, key := range keys_ui64 {
		keys_i64[i] = int64(key)
	}
	keys = keys_i64

	runtime.KeepAlive(flatten)
	runtime.KeepAlive(idx.Dataset)
	return
}

func (idx *UsearchBruteForceIndex[T]) UpdateConfig(sif cache.VectorIndexSearchIf) error {
	return nil
}

func (idx *UsearchBruteForceIndex[T]) Destroy() {
	if idx.deallocator != nil {
		idx.deallocator.Deallocate()
		idx.deallocator = nil
		idx.Dataset = nil
	} else if idx.Dataset != nil {
		idx.Dataset = nil
	}
}

func (idx *GoBruteForceIndex[T]) Load(sqlproc *sqlexec.SqlProcess) error {
	return nil
}

func (idx *GoBruteForceIndex[T]) UpdateConfig(sif cache.VectorIndexSearchIf) error {
	return nil
}

func (idx *GoBruteForceIndex[T]) Destroy() {
}

func (idx *GoBruteForceIndex[T]) Search(proc *sqlexec.SqlProcess, _queries any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {
	queries, ok := _queries.([][]T)
	if !ok {
		return nil, nil, moerr.NewInternalErrorNoCtx("queries type invalid")
	}

	distfn, err := metric.ResolveDistanceFn[T](idx.Metric)
	if err != nil {
		return nil, nil, err
	}

	nthreads := int(rt.NThreads)
	if nthreads <= 0 {
		nthreads = 1
	}
	nqueries := len(queries)
	ndataset := len(idx.Dataset)
	limit := int(rt.Limit)

	if limit == 0 || nqueries == 0 || ndataset == 0 {
		return make([]int64, nqueries*limit), make([]float64, nqueries*limit), nil
	}

	if limit > ndataset {
		limit = ndataset
	}

	totalReturn := nqueries * limit
	retKeys64 := make([]int64, totalReturn)
	retDistances := make([]float64, totalReturn)

	exec := concurrent.NewThreadPoolExecutor(nthreads)

	// If we have enough queries to keep threads busy, parallelize over queries.
	if nqueries >= nthreads {
		err = exec.Execute(
			proc.GetContext(),
			nqueries,
			func(ctx context.Context, thread_id int, start, end int) (err2 error) {
				// Pre-allocate heap buffers for this thread
				var heapKeysBuf []int64
				var heapDistBuf []T
				if limit > 1 {
					heapKeysBuf = make([]int64, limit)
					heapDistBuf = make([]T, limit)
				}

				for k := start; k < end; k++ {
					q := queries[k]
					if k%100 == 0 && ctx.Err() != nil {
						return ctx.Err()
					}

					if limit == 1 {
						minDist := metric.MaxFloat[T]()
						minIdx := -1
						for j := range idx.Dataset {
							dist, err2 := distfn(q, idx.Dataset[j])
							if err2 != nil {
								return err2
							}
							if dist < minDist {
								minDist = dist
								minIdx = j
							}
						}
						retKeys64[k*limit] = int64(minIdx)
						retDistances[k*limit] = float64(minDist)
						continue
					}

					// Max-heap logic for K > 1
					h := vectorindex.NewFastMaxHeap(limit, heapKeysBuf, heapDistBuf)

					for j := range idx.Dataset {
						dist, err2 := distfn(q, idx.Dataset[j])
						if err2 != nil {
							return err2
						}
						h.Push(int64(j), dist)
					}

					// Extract from heap and place into results in sorted order (smallest first)
					offset := k * limit
					for j := limit - 1; j >= 0; j-- {
						key, dist, ok := h.Pop()
						if !ok {
							retKeys64[offset+j] = -1
							retDistances[offset+j] = 0
							continue
						}
						retKeys64[offset+j] = key
						retDistances[offset+j] = float64(dist)
					}
				}
				return
			})

		if err != nil {
			return nil, nil, err
		}

		return retKeys64, retDistances, nil
	}

	return idx.searchDatasetParallel(proc, queries, nthreads, limit, distfn, retKeys64, retDistances)
}

func (idx *GoBruteForceIndex[T]) searchDatasetParallel(
	proc *sqlexec.SqlProcess,
	queries [][]T,
	nthreads int,
	limit int,
	distfn metric.DistanceFunction[T],
	retKeys64 []int64,
	retDistances []float64,
) ([]int64, []float64, error) {
	nqueries := len(queries)
	ndataset := len(idx.Dataset)
	exec := concurrent.NewThreadPoolExecutor(nthreads)

	// If nqueries < nthreads (e.g. 1 query, 16 threads), parallelize over the dataset.
	// We will collect top-K results from each thread and then merge them.
	// Allocate a flattened block for thread results to minimize allocations
	flatLen := nqueries * nthreads * limit
	var flatKeys []int64
	var flatDists []T
	var pFlatKeys *[]int64
	var pFlatDists *[]T

	var _t T
	switch any(_t).(type) {
	case float32:
		pFlatKeys = get1D[int64](&pool1DI64, flatLen)
		flatKeys = *pFlatKeys

		pFlatDists32 := get1D[float32](&pool1DF32, flatLen)
		flatDists = any(*pFlatDists32).([]T)
		pFlatDists = any(pFlatDists32).(*[]T)
	case float64:
		pFlatKeys = get1D[int64](&pool1DI64, flatLen)
		flatKeys = *pFlatKeys

		pFlatDists64 := get1D[float64](&pool1DF64, flatLen)
		flatDists = any(*pFlatDists64).([]T)
		pFlatDists = any(pFlatDists64).(*[]T)
	}

	defer func() {
		put1D(&pool1DI64, pFlatKeys)
		switch any(_t).(type) {
		case float32:
			put1D(&pool1DF32, any(pFlatDists).(*[]float32))
		case float64:
			put1D(&pool1DF64, any(pFlatDists).(*[]float64))
		}
	}()

	// To track how many valid items each thread produced per query
	pValidCounts := get1D[int](&pool1DInt, nqueries*nthreads)
	validCounts := *pValidCounts
	defer put1D(&pool1DInt, pValidCounts)

	err := exec.Execute(
		proc.GetContext(),
		ndataset,
		func(ctx context.Context, thread_id int, start, end int) (err2 error) {
			var heapKeysBuf []int64
			var heapDistBuf []T
			if limit > 1 {
				heapKeysBuf = make([]int64, limit)
				heapDistBuf = make([]T, limit)
			}

			datasetChunk := idx.Dataset[start:end]

			for k := 0; k < nqueries; k++ {
				q := queries[k]
				if k%100 == 0 && ctx.Err() != nil {
					return ctx.Err()
				}

				baseOffset := (k*nthreads + thread_id) * limit

				if limit == 1 {
					minDist := metric.MaxFloat[T]()
					minIdx := -1
					for j := range datasetChunk {
						dist, err2 := distfn(q, datasetChunk[j])
						if err2 != nil {
							return err2
						}
						if dist < minDist {
							minDist = dist
							minIdx = start + j // Global ID
						}
					}

					// Store local result for this thread
					if minIdx != -1 {
						flatKeys[baseOffset] = int64(minIdx)
						flatDists[baseOffset] = minDist
						validCounts[k*nthreads+thread_id] = 1
					} else {
						validCounts[k*nthreads+thread_id] = 0
					}
					continue
				}

				// Max-heap logic for K > 1
				h := vectorindex.NewFastMaxHeap(limit, heapKeysBuf, heapDistBuf)

				for j := range datasetChunk {
					dist, err2 := distfn(q, datasetChunk[j])
					if err2 != nil {
						return err2
					}
					h.Push(int64(start+j), dist)
				}

				// Extract from local heap directly into the flat array
				validCount := 0
				for j := limit - 1; j >= 0; j-- {
					key, dist, ok := h.Pop()
					if !ok {
						continue
					}
					flatKeys[baseOffset+j] = key
					flatDists[baseOffset+j] = dist
					validCount++
				}

				// Shift valid items to the front of this thread's block if we didn't fill the limit
				if validCount > 0 && validCount < limit {
					shift := limit - validCount
					for j := 0; j < validCount; j++ {
						flatKeys[baseOffset+j] = flatKeys[baseOffset+j+shift]
						flatDists[baseOffset+j] = flatDists[baseOffset+j+shift]
					}
				}

				validCounts[k*nthreads+thread_id] = validCount
			}
			return
		})

	if err != nil {
		return nil, nil, err
	}

	// Merge the thread-local results for each query
	var finalHeapKeysBuf []int64
	var finalHeapDistBuf []T
	if limit > 1 {
		finalHeapKeysBuf = make([]int64, limit)
		finalHeapDistBuf = make([]T, limit)
	}

	for k := 0; k < nqueries; k++ {
		offset := k * limit

		if limit == 1 {
			minDist := metric.MaxFloat[T]()
			minIdx := int64(-1)
			for t := 0; t < nthreads; t++ {
				validCount := validCounts[k*nthreads+t]
				if validCount > 0 {
					baseOffset := (k*nthreads + t) * limit
					if flatDists[baseOffset] < minDist {
						minDist = flatDists[baseOffset]
						minIdx = flatKeys[baseOffset]
					}
				}
			}
			retKeys64[offset] = minIdx
			retDistances[offset] = float64(minDist)
			continue
		}

		h := vectorindex.NewFastMaxHeap(limit, finalHeapKeysBuf, finalHeapDistBuf)
		for t := 0; t < nthreads; t++ {
			validCount := validCounts[k*nthreads+t]
			baseOffset := (k*nthreads + t) * limit
			for j := 0; j < validCount; j++ {
				h.Push(flatKeys[baseOffset+j], flatDists[baseOffset+j])
			}
		}

		for j := limit - 1; j >= 0; j-- {
			key, dist, ok := h.Pop()
			if !ok {
				retKeys64[offset+j] = -1
				retDistances[offset+j] = 0
				continue
			}
			retKeys64[offset+j] = key
			retDistances[offset+j] = float64(dist)
		}
	}

	return retKeys64, retDistances, nil
}
