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
	x []T,
	nX int,
	y []T,
	nY int,
	dim int,
	metric MetricType,
	deviceID int,
) ([]float32, error) {
	if nX == 0 || nY == 0 {
		return nil, nil
	}

	cuvsMetric, ok := MetricTypeToCuvsMetric[metric]
	if !ok {
		return GoPairWiseDistance(x, nX, y, nY, dim, metric)
	}

	// T must be float32 for cuvs.PairwiseDistance as per VectorType constraint
	// RealNumbers only includes float32/float64. cuvs.VectorType includes float32, Float16, int8, uint8.
	// For now we only support float32 on GPU via this interface if T is float32.
	var zero T
	if any(zero).(interface{}) == any(float32(0)).(interface{}) {
		xf32 := any(x).([]float32)
		yf32 := any(y).([]float32)

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

	return GoPairWiseDistance(x, nX, y, nY, dim, metric)
}
