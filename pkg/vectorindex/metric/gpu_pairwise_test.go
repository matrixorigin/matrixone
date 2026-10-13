//go:build gpu

// Copyright 2026 Matrix Origin
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

// TestGpuPairwiseMatchesCpuConvention pins the GPU pairwise path to the same values the
// CPU kernels produce. Only the sign/scale conventions are asserted, not GPU arithmetic:
// both paths must hand back MO's numbers so a query whose batches split across the
// GPUThresholdSQL boundary cannot report two different answers for the same vectors.
//
// Inner product is the case that regressed: cuVS returns the raw dot product and
// gpu_pairwise_distance_wait (cgo/cuvs/distance_c.cpp) already flips its sign, so an
// extra negation on the Go side produced +a·b where MO's inner_product is -a·b. A
// 20000-row query returned BOTH +128 and -128 depending on which batches cleared the
// threshold. minWorkSize 0 (GPUThresholdOverlapped) forces the GPU path regardless of
// how small the input is, so this stays a real GPU test rather than a CPU fallback.
func TestGpuPairwiseMatchesCpuConvention(t *testing.T) {
	x := [][]float32{{1, 2, 3}}
	y := [][]float32{{1, 2, 3}, {4, 5, 6}}

	// dot(x,y0) = 14, dot(x,y1) = 32 -> MO's inner_product is the negation.
	// L2sq: 0 and 27. L2: 0 and sqrt(27).
	for _, tc := range []struct {
		name   string
		metric MetricType
		want   []float64
	}{
		{"inner_product", Metric_InnerProduct, []float64{-14, -32}},
		{"l2sq", Metric_L2sqDistance, []float64{0, 27}},
		{"l2", Metric_L2Distance, []float64{0, math.Sqrt(27)}},
		{"cosine", Metric_CosineDistance, []float64{0, 0.0253681}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gpuDist := make([]float32, len(y))
			h, err := PairwiseDistanceLaunch(x, y, tc.metric, gpuDist, GPUThresholdOverlapped, true)
			require.NoError(t, err)
			gpuOut, err := PairwiseDistanceWait(h, tc.metric)
			require.NoError(t, err)
			require.Len(t, gpuOut, len(y))

			cpuDist := make([]float32, len(y))
			ch, err := PairwiseDistanceLaunchCPU(x, y, tc.metric, cpuDist)
			require.NoError(t, err)
			cpuOut, err := PairwiseDistanceWaitCPU(ch, tc.metric)
			require.NoError(t, err)

			for i := range tc.want {
				require.InDeltaf(t, tc.want[i], float64(gpuOut[i]), 1e-4,
					"GPU %s[%d]", tc.name, i)
				require.InDeltaf(t, float64(cpuOut[i]), float64(gpuOut[i]), 1e-4,
					"GPU and CPU disagree on %s[%d]: cpu=%v gpu=%v",
					tc.name, i, cpuOut[i], gpuOut[i])
			}
		})
	}
}
