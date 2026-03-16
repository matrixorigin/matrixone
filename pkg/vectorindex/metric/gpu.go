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
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/cuvs"
)

var (
	MetricTypeToCuvsMetric = map[MetricType]cuvs.DistanceType{
		Metric_L2sqDistance:   cuvs.L2Expanded,
		Metric_L2Distance:     cuvs.L2Expanded,
		Metric_InnerProduct:   cuvs.InnerProduct,
		Metric_CosineDistance: cuvs.CosineExpanded,
		Metric_L1Distance:     cuvs.L1,
	}
)

func PairWiseDistance[T types.RealNumbers](
	x [][]T,
	y [][]T,
	metric MetricType,
	deviceID int,
) ([]float32, error) {
	nX := len(x)
	nY := len(y)
	if nX == 0 || nY == 0 {
		return nil, nil
	}
	dim := len(x[0])

	cuvsMetric, ok := MetricTypeToCuvsMetric[metric]
	if !ok || nX*nY*dim < 40000*1024 {
		return GoPairWiseDistance(x, y, metric)
	}

	// T must be float32 for cuvs.PairwiseDistance as per VectorType constraint
	// RealNumbers only includes float32/float64. cuvs.VectorType includes float32, Float16, int8, uint8.
	// For now we only support float32 on GPU via this interface if T is float32.
	var zero T
	if any(zero).(interface{}) == any(float32(0)).(interface{}) {
		allocator := malloc.NewCAllocator()

		xf32Slice, xDeallocator, err := allocator.Allocate(uint64(nX*dim*4), malloc.NoClear)
		if err != nil {
			return nil, err
		}
		defer xDeallocator.Deallocate()
		xf32 := util.UnsafeSliceCast[float32](xf32Slice)
		for i, v := range x {
			copy(xf32[i*dim:(i+1)*dim], any(v).([]float32))
		}

		yf32Slice, yDeallocator, err := allocator.Allocate(uint64(nY*dim*4), malloc.NoClear)
		if err != nil {
			return nil, err
		}
		defer yDeallocator.Deallocate()
		yf32 := util.UnsafeSliceCast[float32](yf32Slice)
		for i, v := range y {
			copy(yf32[i*dim:(i+1)*dim], any(v).([]float32))
		}

		res, err := cuvs.PairwiseDistance(xf32, uint64(nX), yf32, uint64(nY), uint32(dim), cuvsMetric, deviceID)
		if err != nil {
			return nil, err
		}

		if metric == Metric_L2Distance {
			for i := range res {
				res[i] = float32(math.Sqrt(float64(res[i])))
			}
		} else if metric == Metric_InnerProduct {
			for i := range res {
				res[i] = -res[i]
			}
		}
		return res, nil
	}

	return GoPairWiseDistance(x, y, metric)
}
