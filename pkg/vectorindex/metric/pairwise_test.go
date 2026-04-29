// Copyright 2023 Matrix Origin
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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPairWiseDistance(t *testing.T) {
	nX, nY := 3, 2
	x := [][]float32{
		{1, 0, 0, 0},
		{0, 1, 0, 0},
		{0, 0, 1, 0},
	}
	y := [][]float32{
		{1, 0, 0, 0},
		{0, 1, 1, 0},
	}

	metrics := []MetricType{
		Metric_L2sqDistance,
		Metric_L2Distance,
		Metric_InnerProduct,
		Metric_CosineDistance,
		Metric_L1Distance,
	}

	for _, m := range metrics {
		t.Run(MetricTypeToDistFuncName[m], func(t *testing.T) {
			dist, err := PairWiseDistance(x, y, m)
			require.NoError(t, err)
			require.Equal(t, nX*nY, len(dist))

			// Verify against direct calls
			distFn, err := ResolveDistanceFn[float32](m)
			require.NoError(t, err)

			for i := 0; i < nX; i++ {
				for j := 0; j < nY; j++ {
					expected, err := distFn(x[i], y[j])
					require.NoError(t, err)

					val := dist[i*nY+j]
					if m == Metric_L2Distance {
						require.InDelta(t, math.Sqrt(float64(expected)), float64(val), 1e-5)
					} else {
						require.InDelta(t, float64(expected), float64(val), 1e-5)
					}
				}
			}
		})
	}
}

func TestGoPairWiseDistance(t *testing.T) {
	x := [][]float64{{1, 0}, {0, 1}}
	y := [][]float64{{1, 0}, {1, 1}}

	dist, err := GoPairWiseDistance(x, y, Metric_L2sqDistance)
	require.NoError(t, err)
	require.Equal(t, 4, len(dist))

	// (1,0) to (1,0) -> 0
	require.InDelta(t, 0.0, float64(dist[0]), 1e-5)
	// (1,0) to (1,1) -> 1
	require.InDelta(t, 1.0, float64(dist[1]), 1e-5)
	// (0,1) to (1,0) -> 2
	require.InDelta(t, 2.0, float64(dist[2]), 1e-5)
	// (0,1) to (1,1) -> 1
	require.InDelta(t, 1.0, float64(dist[3]), 1e-5)
}

func TestPairwiseDistanceLaunchWaitCPU_Float32(t *testing.T) {
	x := [][]float32{{1, 0}, {0, 1}}
	y := [][]float32{{1, 0}, {1, 1}}

	dist := make([]float32, 4)
	h, err := PairwiseDistanceLaunchCPU(x, y, Metric_L2sqDistance, dist)
	require.NoError(t, err)
	require.True(t, h.IsValid())

	out, err := PairwiseDistanceWaitCPU(h, Metric_L2sqDistance)
	require.NoError(t, err)
	require.Equal(t, 4, len(out))
	require.InDelta(t, 0.0, float64(out[0]), 1e-5)
	require.InDelta(t, 1.0, float64(out[1]), 1e-5)
	require.InDelta(t, 2.0, float64(out[2]), 1e-5)
	require.InDelta(t, 1.0, float64(out[3]), 1e-5)
}

func TestPairwiseDistanceLaunchWaitCPU_Float64_L2(t *testing.T) {
	x := [][]float64{{1, 0}, {0, 1}}
	y := [][]float64{{1, 0}, {1, 1}}

	// dist with insufficient capacity -> internal allocation path
	dist := make([]float32, 0)
	h, err := PairwiseDistanceLaunchCPU(x, y, Metric_L2Distance, dist)
	require.NoError(t, err)
	require.True(t, h.IsValid())

	out, err := PairwiseDistanceWaitCPU(h, Metric_L2Distance)
	require.NoError(t, err)
	require.Equal(t, 4, len(out))
	// L2 takes sqrt of squared distance
	require.InDelta(t, 0.0, float64(out[0]), 1e-5)
	require.InDelta(t, 1.0, float64(out[1]), 1e-5)
	require.InDelta(t, math.Sqrt(2.0), float64(out[2]), 1e-5)
	require.InDelta(t, 1.0, float64(out[3]), 1e-5)
}

func TestPairwiseDistanceWaitCPU_InvalidHandle(t *testing.T) {
	_, err := PairwiseDistanceWaitCPU(PairwiseJobHandle(0), Metric_L2sqDistance)
	require.Error(t, err)
	// arbitrary handle that wasn't issued
	_, err = PairwiseDistanceWaitCPU(PairwiseJobHandle(0xdeadbeef), Metric_L2sqDistance)
	require.Error(t, err)
}

func TestPairwiseJobHandleIsValid(t *testing.T) {
	require.False(t, PairwiseJobHandle(0).IsValid())
	require.True(t, PairwiseJobHandle(1).IsValid())
}

func TestPairwiseDistanceLaunchCPU_BadMetric(t *testing.T) {
	x := [][]float32{{1, 0}}
	y := [][]float32{{1, 0}}
	_, err := PairwiseDistanceLaunchCPU(x, y, MetricType(9999), nil)
	require.Error(t, err)
}
