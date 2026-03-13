//go:build gpu

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
	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/util"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

type GpuBruteForceIndex[T cuvs.VectorType] struct {
	index     *cuvs.GpuBruteForce[T]
	dimension uint
	count     uint
}

var _ cache.VectorIndexSearchIf = &GpuBruteForceIndex[float32]{}

func resolveCuvsDistance(m metric.MetricType) cuvs.DistanceType {
	switch m {
	case metric.Metric_L2sqDistance:
		return cuvs.L2Expanded
	case metric.Metric_L2Distance:
		return cuvs.L2Expanded
	case metric.Metric_InnerProduct:
		return cuvs.InnerProduct
	case metric.Metric_CosineDistance:
		return cuvs.CosineSimilarity
	case metric.Metric_L1Distance:
		return cuvs.L1
	default:
		return cuvs.L2Expanded
	}
}

func NewBruteForceIndex[T types.RealNumbers](dataset [][]T,
	dimension uint,
	m metric.MetricType,
	elemsz uint,
	nthread uint) (cache.VectorIndexSearchIf, error) {

	switch dset := any(dataset).(type) {
	case [][]float64:
		return NewCpuBruteForceIndex[T](dataset, dimension, m, elemsz)
	case [][]float32:
		return NewGpuBruteForceIndex[float32](dset, dimension, m, elemsz, nthread)
	case [][]uint16:
		// Convert [][]uint16 to [][]cuvs.Float16 to pass to NewGpuBruteForceIndex
		f16dset := make([][]cuvs.Float16, len(dset))
		for i, v := range dset {
			f16dset[i] = util.UnsafeSliceCast[cuvs.Float16](v)
		}
		return NewGpuBruteForceIndex[cuvs.Float16](f16dset, dimension, m, elemsz, nthread)
	default:
		return nil, moerr.NewInternalErrorNoCtx("type not supported for BruteForceIndex")
	}
}

func NewGpuBruteForceIndex[T cuvs.VectorType](dataset [][]T,
	dimension uint,
	m metric.MetricType,
	elemsz uint,
	nthread uint) (cache.VectorIndexSearchIf, error) {

	if len(dataset) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("empty dataset")
	}

	dim := int(dimension)
	reqSize := len(dataset) * dim
	var flattened []T

	var _t T
	switch any(_t).(type) {
	case float32:
		allocator := malloc.NewCAllocator()
		slice, deallocator, err := allocator.Allocate(uint64(reqSize*4), malloc.NoClear)
		if err != nil {
			return nil, err
		}
		defer deallocator.Deallocate()
		flattened = any(util.UnsafeSliceCast[float32](slice)).([]T)
	case cuvs.Float16:
		allocator := malloc.NewCAllocator()
		slice, deallocator, err := allocator.Allocate(uint64(reqSize*2), malloc.NoClear)
		if err != nil {
			return nil, err
		}
		defer deallocator.Deallocate()
		flattened = any(util.UnsafeSliceCast[cuvs.Float16](slice)).([]T)
	default:
		ds := make([]T, reqSize)
		flattened = ds
	}

	for i, v := range dataset {
		copy(flattened[i*dim:(i+1)*dim], v)
	}

	deviceID := 0 // Default to device 0
	km, err := cuvs.NewGpuBruteForce[T](flattened, uint64(len(dataset)), uint32(dimension), resolveCuvsDistance(m), uint32(nthread), deviceID)
	if err != nil {
		return nil, err
	}

	km.Start()
	return &GpuBruteForceIndex[T]{
		index:     km,
		dimension: dimension,
		count:     uint(len(dataset)),
	}, nil
}

func (idx *GpuBruteForceIndex[T]) Load(sqlproc *sqlexec.SqlProcess) (err error) {
	if idx.index == nil {
		return moerr.NewInternalErrorNoCtx("GpuBruteForce not initialized")
	}
	return idx.index.Build()
}

func (idx *GpuBruteForceIndex[T]) Search(proc *sqlexec.SqlProcess, _queries any, rt vectorindex.RuntimeConfig) (retkeys any, retdistances []float64, err error) {
	queriesvec, ok := _queries.([][]T)
	if !ok {
		return nil, nil, moerr.NewInternalErrorNoCtx("queries type invalid")
	}

	if len(queriesvec) == 0 {
		return nil, nil, nil
	}

	dim := int(idx.dimension)
	reqSize := len(queriesvec) * dim

	var flattenedQueries []T
	var queryDeallocator malloc.Deallocator

	var _t T
	switch any(_t).(type) {
	case float32:
		allocator := malloc.NewCAllocator()
		slice, dealloc, err2 := allocator.Allocate(uint64(reqSize)*4, malloc.NoClear)
		if err2 != nil {
			return nil, nil, err2
		}
		queryDeallocator = dealloc
		f32Slice := util.UnsafeSliceCastToLength[float32](slice, reqSize)
		flattenedQueries = any(f32Slice).([]T)
	case cuvs.Float16:
		allocator := malloc.NewCAllocator()
		slice, dealloc, err2 := allocator.Allocate(uint64(reqSize)*2, malloc.NoClear)
		if err2 != nil {
			return nil, nil, err2
		}
		queryDeallocator = dealloc
		f16Slice := util.UnsafeSliceCastToLength[cuvs.Float16](slice, reqSize)
		flattenedQueries = any(f16Slice).([]T)
	default:
		// Not pooling other types, although T is likely only float32 for CUVS
		ds := make([]T, reqSize)
		flattenedQueries = ds
	}

	for i, v := range queriesvec {
		copy(flattenedQueries[i*dim:(i+1)*dim], v)
	}

	if queryDeallocator != nil {
		defer queryDeallocator.Deallocate()
	}

	neighbors, distances, err := idx.index.Search(flattenedQueries, uint64(len(queriesvec)), uint32(idx.dimension), uint32(rt.Limit))
	if err != nil {
		return nil, nil, err
	}

	retdistances = make([]float64, len(distances))
	for i, d := range distances {
		retdistances[i] = float64(d)
	}

	retkeys = neighbors
	return
}

func (idx *GpuBruteForceIndex[T]) UpdateConfig(sif cache.VectorIndexSearchIf) error {
	return nil
}

func (idx *GpuBruteForceIndex[T]) Destroy() {
	if idx.index != nil {
		idx.index.Destroy()
	}
}
