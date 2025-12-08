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

package metric

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	cuvs "github.com/rapidsai/cuvs/go"
	usearch "github.com/unum-cloud/usearch/golang"
)

var (
	MetricTypeToCuvsMetric = map[MetricType]cuvs.Distance{
		Metric_L2sqDistance:   cuvs.DistanceSQEuclidean,
		Metric_L2Distance:     cuvs.DistanceSqEuclidean,
		Metric_InnerProduct:   cuvs.DistanceInnerProduct,
		Metric_CosineDistance: cuvs.DistanceCosine,
		Metric_L1Distance:     cuvs.DistanceL1,
	}
)

type GpuVectorSet[T cuvs.TensorNumberType] struct {
	Idxmap    map[int64]int64
	Dataset   *cuvs.Tensor[T]
	Vector    [][]T
	Count     uint
	Dimension uint
	Curr      int64
	Elemsz    uint
}

var _ VectorSetIf[float32] = &GpuVectorSet[float32]{}

func NewVectorSet[T types.RealNumbers](count uint, dimension uint, elemSize uint) VectorSetIf[T] {
	var t T
	switch any(t).(type) {
	case float32:
		c := &GpVectorSet[float32]{Count: count, Dimension: dimension, Elemsz: elemSize}
		c.Idxmap = make(map[int64]int64, count*dimension)
		c.Vector = make([][]float32, count)
		return c
	default:
		c := &VectorSet[T]{Count: count, Dimension: dimension, Elemsz: elemSize}
		c.Idxmap = make(map[int64]int64, count*dimension)
		c.Vector = make([]T, count*dimension)
		return c

	}
}

func (set *GpuVectorSet[T]) GetIndexMap() map[int64]int64 {
	return set.Idxmap
}

func (set *GpuVectorSet[T]) GetCount() uint {
	return set.Count
}

func (set *GpuVectorSet[T]) GetDimension() uint {
	return set.Dimension
}

func (set *GpuVectorSet[T]) GetCurr() int64 {
	return set.Curr
}

func (set *GpuVectorSet[T]) GetElementSize() uint {
	return set.Elemsz
}

func (set *GpuVectorSet[T]) GetVector() any {
	return set.Vector
}

func (set *GpuVectorSet[T]) SetNull(offset int64) {
	set.Idxmap[offset] = -1
}

func (set *GpuVectorSet[T]) AddVector(offset int64, v []T) {
	if v == nil {
		set.Idxmap[offset] = -1
		return
	}

	set.Idxmap[offset] = set.Curr
	start := set.Curr * int64(set.Dimension)
	for i, f := range v {
		set.Vector[start+int64(i)] = f
	}
	set.Curr += 1

}

func (set *GpuVectorSet[T]) Reset(count uint, dimension uint, elemSize uint) {
	newsz := count * dimension
	if cap(set.Vector) < int(newsz) {
		set.Vector = make([]T, count*dimension)
	}
	clear(set.Idxmap)
	set.Count = count
	set.Dimension = dimension
	set.Curr = 0
	set.Elemsz = elemSize
}

/*
func GetUseachQuantizationFromType(v any) (usearch.Quantization, error) {
	switch v.(type) {
	case float32:
		return usearch.F32, nil
	case float64:
		return usearch.F64, nil
	default:
		return 0, moerr.NewInternalErrorNoCtx(fmt.Sprintf("usearch not support type %T", v))
	}
}
*/

func ExactSearch[T types.RealNumbers](_dataset, _queries VectorSetIf[T],
	metric MetricType,
	maxResults uint,
	ncpu uint) (keys []uint64, distances []float32, err error) {

	switch _dataset.(type) {
	case *VectorSet:
		return UsearchExactSearch[T](_dataset, _queries, metric, maxResults, ncpu)
	case *GpuVectorSet:
		var t T
		switch any(t).(type) {
		case float32:
			return GpuExactSearch[float32](_dataset, _queries, metric, maxResults, ncpu)
		default:
			return nil, moerr.NewInternalErrorNoCtx("Not supported type for GPU")
		}
	}
}

func GpuExactSearch[T cuvs.TensorNumberType](_dataset, _queries VectorSetIf[T],
	metric MetricType,
	maxResults uint,
	ncpu uint) (keys []uint64, distances []float32, err error) {

	/*
		usearch_metric := MetricTypeToUsearchMetric[metric]

		quantization, err := GetUseachQuantizationFromType(T(0))
		if err != nil {
			return nil, nil, err
		}
	*/

	resouce, _ := cuvs.NewResource(nil)
	defer resource.Close()

	dataset := _dataset.(*GpuVectorSet[T])
	queries := _queries.(*GpuVectorSet[T])

	dataset_tensor, err := cuvs.NewTensor(dataset.GetVector().([][]T))
	if err != nil {
		return nil, nil, err
	}
	defer dataset_tensor.Close()

	queries_tensor, err := cuvs.NewTensor(queries.GetVector().([][]T))
	if err != nil {
		return nil, nil, err
	}
	defer queries_tensor.Close()

	index, err := CreateIndex()

	return usearch.ExactSearchUnsafe(
		util.UnsafePointer(&(dataset.Vector[0])),
		util.UnsafePointer(&(queries.Vector[0])),
		uint(dataset.Curr),
		uint(queries.Curr),
		dataset.Dimension*dataset.Elemsz,
		queries.Dimension*queries.Elemsz,
		dataset.Dimension,
		usearch_metric,
		quantization,
		maxResults,
		ncpu)
}
