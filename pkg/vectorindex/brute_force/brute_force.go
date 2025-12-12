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
	"fmt"
	"runtime"
	"slices"
	"sync"

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

type UsearchBruteForceIndex[T types.RealNumbers] struct {
	Dataset      []T // flattend vector
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

	idx.Dataset = make([]T, idx.Count*idx.Dimension)
	for i := 0; i < len(dataset); i++ {
		offset := i * int(dimension)
		copy(idx.Dataset[offset:], dataset[i])
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
	if len(queries) == 1 {
		flatten = queries[0]
	} else {
		flatten = make([]T, len(queries)*int(idx.Dimension))
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
		util.UnsafePointer(&(idx.Dataset[0])),
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

	errs := make(chan error)
	distfn, err := metric.ResolveDistanceFn[T](idx.Metric)
	if err != nil {
		return nil, nil, err
	}

	nthreads := rt.NThreads
	if nthreads == 0 {
		runtime.NumCPU()
	}

	// datasize * nqueries

	nqueries := len(queries)
	ndataset := len(idx.Dataset)

	// create distance matric
	results := make([]vectorindex.SearchResult, nqueries*ndataset)

	var wg sync.WaitGroup
	for n := 0; n < int(nthreads); n++ {

		wg.Add(1)
		go func(tid int) {
			defer wg.Done()
			for i := 0; i < nqueries; i++ {
				if i%int(nthreads) != tid {
					continue
				}
				for j := 0; j < ndataset; j++ {
					dist, err := distfn(queries[i], idx.Dataset[j])
					if err != nil {
						errs <- err
						return
					}
					results[i*ndataset+j].Id = int64(j)
					results[i*ndataset+j].Distance = float64(dist)
				}
			}

		}(n)
	}

	wg.Wait()

	if len(errs) > 0 {
		return nil, nil, <-errs
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
	keys64 := make([]int64, nqueries*int(rt.Limit))
	distances = make([]float64, nqueries*int(rt.Limit))
	var wg2 sync.WaitGroup
	for n := 0; n < int(nthreads); n++ {
		wg2.Add(1)
		go func(tid int) {
			defer wg2.Done()
			for i := 0; i < nqueries; i++ {
				if i%int(nthreads) != tid {
					continue
				}

				if rt.Limit == 1 {
					// min
					first := slices.MinFunc(results[i*ndataset:(i+1)*ndataset], cmpfn)
					results[i*ndataset] = first

				} else {
					// partial sort
					partial.SortFunc(results[i*ndataset:(i+1)*ndataset], int(rt.Limit), cmpfn)

				}
			}
		}(n)
	}

	wg2.Wait()

	for i := 0; i < nqueries; i++ {
		for j := 0; j < int(rt.Limit); j++ {
			keys64[i*int(rt.Limit)+j] = results[i*ndataset+j].Id
			distances[i*int(rt.Limit)+j] = results[i*ndataset+j].Distance
		}
	}

	return keys64, distances, nil
}
