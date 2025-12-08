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
	"fmt"
	"runtime"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	cuvs "github.com/rapidsai/cuvs/go"
	"github.com/rapidsai/cuvs/go/brute_force"
)

type GpuBruteForceIndex[T cuvs.TensorNumberType] struct {
	Resource    *cuvs.Resource
	Dataset     *cusv.Tensor[T]
	Index       *cuvs.BruteForceIndex
	Metric      cuvs.Distance
	Dimension   uint
	Count       uint
	ElementSize uint
}

var _ cache.VectorIndexSearchIf = &GpuBruteForceIndex[float32]{}

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

func NewBruteForceIndex[T types.RealNumbers](dataset [][]T,
	dimension uint,
	m metric.MetricType,
	elemsz uint) (cache.VectorIndexSearchIf, error) {
	var err error

	switch dset := any(dataset).(type) {
	case [][]float64:
		return NewCpuBruteForceIndex[T](dataset, dimension, m, elemsz)
	case [][]float32:
		idx := &GpuBruteForceIndex[float32]{}

		idx.Resource, _ := cuvs.NewResource(nil)

		idx.Dataset, err = cuvs.NewTensor(dset)
		if err != nil {
			return nil, err
		}

		idx.Metric = metric.MetricTypeToCuvsMetric[m]
		idx.Dimension = dimension
		idx.Count = uint(len(dataset))
		idx.ElementSize = elemsz
		return idx, nil
	default:
		return nil, moerr.NewInternalErrorNoCtx("type not supported for BruteForceIndex")
	}

}

func (idx *GpuBruteForceIndex[T]) Load(sqlproc *sqlexec.SqlProcess) (err error) {
	if _, err = idx.Dataset.ToDevice(&idx.Resource); err != nil {
		return err
	}

	idx.Index, err = brute_force.CreateIndex()
	if err != nil {
		return
	}

	err = brute_force.BuildIndex[T](idx.Resource, idx.Dataset, idx.Metric, 0, idx.Index)
	if err != nil {
		return
	}

	if err = idx.Resource.Sync(); err != nil {
		return
	}
}

func (idx *GpuBruteForceIndex[T]) Search(proc *sqlexec.SqlProcess, _queries any, rt vectorindex.RuntimeConfig) (retkeys any, retdistances []float64, err error) {
	queriesvec, ok := _queries.([][]T)
	if !ok {
		return nil, nil, moerr.NewInternalErrorNoCtx("queries type invalid")
	}

	resource, _ := cuvs.NewResource(nil)
	defer resource.Close()

	queries, err := cuvs.NewTensor(queriesvec)
	if err != nil {
		return nil, nil, err
	}
	defer queries.Close()

	neighbors, err := cuvs.NewTensorOnDevice[int64](&resource, []int64{int64(len(queriesvec)), int64(rt.Limit)})
	if err != nil {
		return nil, nil, err
	}
	defer neighbors.Close()

	distances, err := cuvs.NewTensorOnDevice[float32](&resource, []int64{int64(len(queriesvec)), int64(rt.Limit)})
	if err != nil {
		return nil, nil, err
	}
	defer distances.Close()

	if _, err = queries.ToDevice(&resource); err != nil {
		return nil, nil, err
	}

	err = brute_force.SearchIndex(resource, idx.Index, &queries, &neighbors, &distances)
	if err != nil {
		return nil, nil, err
	}

	if _, err = neighbors.ToHost(&resource); err != nil {
		return nil, nil, err
	}

	if _, err = distances.ToHost(&resource); err != nil {
		return nil, nil, err
	}

	if err = resource.Sync(); err != nil {
		return nil, nil, err
	}

	neighborsSlice, err := neighbors.Slice()
	if err != nil {
		return nil, nil, err
	}

	distancesSlice, err := distances.Slice()
	if err != nil {
		return nil, nil, err
	}

	//fmt.Printf("flattened %v\n", flatten)
	retdistances = make([]float64, len(distancesSlice))
	for i, dist := range distancesSlice {
		retdistances[i] = float64(dist)
	}

	retkeys := make([]int64, len(neighborsSlice))
	for i, key := range neighborsSlice {
		retkeys[i] = int64(key)
	}
	return
}

func (idx *GpuBruteForceIndex[T]) UpdateConfig(sif cache.VectorIndexSearchIf) error {
	return nil
}

func (idx *GpuBruteForceIndex[T]) Destroy() {
	if idx.Dataset != nil {
		idx.Dataset.Close()
	}
	if idx.Resource != nil {
		idx.Resource.Close()
	}
}
