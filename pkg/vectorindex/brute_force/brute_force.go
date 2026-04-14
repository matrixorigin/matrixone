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

func NewUsearchBruteForceIndexFlattened[T types.RealNumbers](dataset []T,
	count uint,
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
	idx.Count = count
	idx.ElementSize = elemsz
	idx.Dataset = &dataset

	return idx, nil
}

func (idx *UsearchBruteForceIndex[T]) Load(sqlproc *sqlexec.SqlProcess) error {
	return nil
}

func (idx *UsearchBruteForceIndex[T]) SearchFloat32(proc *sqlexec.SqlProcess, _queries any, rt vectorindex.RuntimeConfig, outKeys []int64, outDists []float32) error {
	keys, dists, err := idx.Search(proc, _queries, rt)
	if err != nil {
		return err
	}
	if keys == nil {
		return nil
	}
	copy(outKeys, keys.([]int64))
	for i, d := range dists {
		outDists[i] = float32(d)
	}
	return nil
}

func (idx *UsearchBruteForceIndex[T]) SearchFloat32WithKeyUint32(proc *sqlexec.SqlProcess, _queries any, rt vectorindex.RuntimeConfig, outKeys []uint32, outDists []float32) error {
	return moerr.NewInternalErrorNoCtx("UsearchBruteForceIndex: does not support SearchFloat32WithKeyUint32")
}

func (idx *UsearchBruteForceIndex[T]) Search(proc *sqlexec.SqlProcess, _queries any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {
	var flatten []T
	var queryDeallocator malloc.Deallocator
	var nQueries int

	switch queries := _queries.(type) {
	case []T:
		flatten = queries
		nQueries = len(queries) / int(idx.Dimension)
	case [][]T:
		if len(queries) == 0 {
			return nil, nil, nil
		}
		nQueries = len(queries)
		reqSize := nQueries * int(idx.Dimension)
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

		for i := 0; i < nQueries; i++ {
			offset := i * int(idx.Dimension)
			copy(flatten[offset:], queries[i])
		}
	default:
		return nil, nil, moerr.NewInternalErrorNoCtx("queries type invalid")
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
		uint(nQueries),
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

func (idx *GoBruteForceIndex[T]) SearchFloat32WithKeyUint32(proc *sqlexec.SqlProcess, _queries any, rt vectorindex.RuntimeConfig, outKeys []uint32, outDists []float32) error {
	return moerr.NewInternalErrorNoCtx("GoBruteForceIndex: does not support SearchFloat32WithKeyUint32")
}

func (idx *GoBruteForceIndex[T]) UpdateConfig(sif cache.VectorIndexSearchIf) error {
	return nil
}

func (idx *GoBruteForceIndex[T]) Destroy() {
}

// SearchFloat32 implements VectorIndexSearchIf — writes results directly into caller-provided
// slices, eliminating the intermediate []int64 and []float64 heap allocations of Search.
func (idx *GoBruteForceIndex[T]) SearchFloat32(proc *sqlexec.SqlProcess, _queries any, rt vectorindex.RuntimeConfig, outKeys []int64, outDists []float32) error {
	queries, ok := _queries.([][]T)
	if !ok {
		return moerr.NewInternalErrorNoCtx("queries type invalid")
	}

	distfn, err := metric.ResolveDistanceFn[T](idx.Metric)
	if err != nil {
		return err
	}

	nthreads := rt.NThreads
	nqueries := len(queries)
	limit := int(rt.Limit)

	if limit == 0 {
		return nil
	}

	exec := concurrent.NewThreadPoolExecutor(int(nthreads))
	return exec.Execute(
		proc.GetContext(),
		nqueries,
		func(ctx context.Context, thread_id int, start, end int) error {
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
					outKeys[k] = int64(minIdx)
					outDists[k] = float32(minDist)
					continue
				}

				h := vectorindex.NewFastMaxHeap[T, int64](limit, heapKeysBuf, heapDistBuf)
				for j := range idx.Dataset {
					dist, err2 := distfn(q, idx.Dataset[j])
					if err2 != nil {
						return err2
					}
					h.Push(int64(j), dist)
				}

				offset := k * limit
				for j := limit - 1; j >= 0; j-- {
					key, dist, ok := h.Pop()
					if !ok {
						outKeys[offset+j] = -1
						outDists[offset+j] = 0
						continue
					}
					outKeys[offset+j] = key
					outDists[offset+j] = float32(dist)
				}
			}
			return nil
		})
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

	nthreads := rt.NThreads
	nqueries := len(queries)
	limit := int(rt.Limit)

	if limit == 0 {
		return []int64{}, []float64{}, nil
	}

	totalReturn := nqueries * limit
	retKeys64 := make([]int64, totalReturn)
	retDistances := make([]float64, totalReturn)

	exec := concurrent.NewThreadPoolExecutor(int(nthreads))
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
				h := vectorindex.NewFastMaxHeap[T, int64](limit, heapKeysBuf, heapDistBuf)

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
						// Pad with invalid if not enough data
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
