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
	nX, nY, dim := 3, 2, 4
	x := []float32{
		1, 0, 0, 0,
		0, 1, 0, 0,
		0, 0, 1, 0,
	}
	y := []float32{
		1, 0, 0, 0,
		0, 1, 1, 0,
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
			dist, err := PairWiseDistance(x, nX, y, nY, dim, m, 0)
			require.NoError(t, err)
			require.Equal(t, nX*nY, len(dist))

			// Verify against direct calls
			distFn, err := ResolveDistanceFn[float32](m)
			require.NoError(t, err)

			for i := 0; i < nX; i++ {
				for j := 0; j < nY; j++ {
					expected, err := distFn(x[i*dim:(i+1)*dim], y[j*dim:(j+1)*dim])
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
	nX, nY, dim := 2, 2, 2
	x := []float64{1, 0, 0, 1}
	y := []float64{1, 0, 1, 1}

	dist, err := GoPairWiseDistance(x, nX, y, nY, dim, Metric_L2sqDistance)
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
