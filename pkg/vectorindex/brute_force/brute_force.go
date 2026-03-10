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
	"slices"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/concurrent"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	usearch "github.com/unum-cloud/usearch/golang"
	"github.com/viterin/partial"
)

var (
	pool1DF32    = sync.Pool{New: func() any { x := make([]float32, 0); return &x }}
	pool1DF64    = sync.Pool{New: func() any { x := make([]float64, 0); return &x }}
	pool2DResult = sync.Pool{New: func() any { x := make([][]vectorindex.SearchResult, 0); return &x }}
	pool1DResult = sync.Pool{New: func() any { x := make([]vectorindex.SearchResult, 0); return &x }}
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

	switch m {
	case metric.Metric_L1Distance:
		return NewGoBruteForceIndex(dataset, dimension, m, elemsz)
	default:
		return NewUsearchBruteForceIndex(dataset, dimension, m, elemsz)
	}
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
	var _t T
	switch any(_t).(type) {
	case float32:
		p := get1D[float32](&pool1DF32, reqSize)
		idx.Dataset = any(p).(*[]T)
	case float64:
		p := get1D[float64](&pool1DF64, reqSize)
		idx.Dataset = any(p).(*[]T)
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
	var pFlatten *[]T
	if len(queries) == 1 {
		flatten = queries[0]
	} else {
		reqSize := len(queries) * int(idx.Dimension)
		var _t T
		switch any(_t).(type) {
		case float32:
			p := get1D[float32](&pool1DF32, reqSize)
			defer put1D(&pool1DF32, p)
			flatten = any(*p).([]T)
		case float64:
			p := get1D[float64](&pool1DF64, reqSize)
			defer put1D(&pool1DF64, p)
			flatten = any(*p).([]T)
		}

		for i := 0; i < len(queries); i++ {
			offset := i * int(idx.Dimension)
			copy(flatten[offset:], queries[i])
		}
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
	runtime.KeepAlive(pFlatten) // ensures defer hasn't fired before usearch call
	runtime.KeepAlive(idx.Dataset)
	return
}

func (idx *UsearchBruteForceIndex[T]) UpdateConfig(sif cache.VectorIndexSearchIf) error {
	return nil
}

func (idx *UsearchBruteForceIndex[T]) Destroy() {
	if idx.Dataset != nil {
		var _t T
		switch any(_t).(type) {
		case float32:
			p := any(idx.Dataset).(*[]float32)
			put1D(&pool1DF32, p)
		case float64:
			p := any(idx.Dataset).(*[]float64)
			put1D(&pool1DF64, p)
		}
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

	nthreads := rt.NThreads

	// datasize * nqueries
	nqueries := len(queries)
	ndataset := len(idx.Dataset)

	// create distance matric
	pResults := get1D[[]vectorindex.SearchResult](&pool2DResult, nqueries)
	defer put1D(&pool2DResult, pResults)
	results := any(*pResults).([][]vectorindex.SearchResult)

	pFlatResults := get1D[vectorindex.SearchResult](&pool1DResult, nqueries*ndataset)
	defer put1D(&pool1DResult, pFlatResults)
	flatResults := any(*pFlatResults).([]vectorindex.SearchResult)

	for i := range results {
		results[i] = flatResults[i*ndataset : (i+1)*ndataset]
	}

	exec := concurrent.NewThreadPoolExecutor(int(nthreads))
	err = exec.Execute(
		proc.GetContext(),
		nqueries,
		func(ctx context.Context, thread_id int, start, end int) (err2 error) {
			subqueries := queries[start:end:end]
			subresults := results[start:end:end]
			for k, q := range subqueries {
				if k%100 == 0 && ctx.Err() != nil {
					return ctx.Err()
				}

				for j := range idx.Dataset {
					dist, err2 := distfn(q, idx.Dataset[j])
					if err2 != nil {
						return err2
					}
					subresults[k][j].Id = int64(j)
					subresults[k][j].Distance = float64(dist)
				}
			}
			return
		})

	if err != nil {
		return nil, nil, err
	}

	cmpfn := func(a, b vectorindex.SearchResult) int {
		if a.Distance < b.Distance {
			return -1
		} else if a.Distance == b.Distance {
			return 0
		}
		return 1
	}

	// get min
	limit := int(rt.Limit)
	totalReturn := nqueries * limit

	// Revert keys/distances to standard allocation since they are the return values
	retKeys64 := make([]int64, totalReturn)
	retDistances := make([]float64, totalReturn)

	err = exec.Execute(
		proc.GetContext(),
		nqueries,
		func(ctx context.Context, thread_id int, start, end int) (err2 error) {
			subresults := results[start:end:end]
			for j := range subresults {
				if j%100 == 0 && ctx.Err() != nil {
					return ctx.Err()
				}

				if rt.Limit == 1 {
					// min
					first := slices.MinFunc(subresults[j], cmpfn)
					subresults[j][0] = first

				} else {
					// partial sort
					partial.SortFunc(subresults[j], int(rt.Limit), cmpfn)

				}
			}
			return
		})
	if err != nil {
		return nil, nil, err
	}

	for i := 0; i < nqueries; i++ {
		for j := 0; j < limit; j++ {
			retKeys64[i*limit+j] = results[i][j].Id
			retDistances[i*limit+j] = results[i][j].Distance
		}
	}

	return retKeys64, retDistances, nil
}
