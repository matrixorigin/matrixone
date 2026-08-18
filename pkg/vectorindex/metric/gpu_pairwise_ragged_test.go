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

//go:build gpu

package metric

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// raggedPairwiseInput builds a workload above GPUThresholdSQL (so the GPU is actually
// used) in which exactly one y row has the wrong dimension.
func raggedPairwiseInput(dim int, shorten bool) (x, y [][]float32) {
	nY := int(GPUThresholdSQL/uint64(dim)) + 16

	x = [][]float32{make([]float32, dim)}
	for i := range x[0] {
		x[0][i] = 1
	}
	y = make([][]float32, nY)
	for i := range y {
		row := make([]float32, dim)
		for j := range row {
			row[j] = 1
		}
		y[i] = row
	}
	if shorten {
		y[0] = y[0][:dim-1]
	} else {
		y[0] = append(y[0], 1)
	}
	return x, y
}

// TestGpuPairwiseRejectsRaggedRows: gpuPairwiseLaunch copies each row into a dim-sized
// slot of a malloc.NoClear buffer, dim coming from x[0]. A short row therefore left the
// tail of its slot holding unrelated memory and a long row was truncated, and cuVS
// returned a plausible distance with NO error -- measured on hardware, a row one element
// short reported 1 where the true distance is 0, and the value depends on whatever was in
// that memory. The CPU kernel rejects the same input, and the two must not disagree.
func TestGpuPairwiseRejectsRaggedRows(t *testing.T) {
	const dim = 8

	for _, tc := range []struct {
		name    string
		shorten bool
	}{
		{"short row", true},
		{"long row", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x, y := raggedPairwiseInput(dim, tc.shorten)

			dist := make([]float32, len(y))
			_, err := PairwiseDistanceLaunch(x, y, Metric_L2sqDistance, dist, GPUThresholdSQL, true)
			require.Error(t, err, "a ragged row must not reach the GPU flatten")
			require.Contains(t, err.Error(), "vector dimension not matched")

			// The CPU path rejects it identically -- that agreement is the point.
			cpuDist := make([]float32, len(y))
			h, cpuErr := PairwiseDistanceLaunchCPU(x, y, Metric_L2sqDistance, cpuDist)
			if cpuErr == nil {
				_, cpuErr = PairwiseDistanceWaitCPU(h, Metric_L2sqDistance)
			}
			require.Error(t, cpuErr)
			require.Contains(t, cpuErr.Error(), "vector dimension not matched")
		})
	}
}

// TestGpuPairwiseUniformRowsStillDispatch guards the check from over-reaching: a
// well-formed workload above the threshold must still run on the GPU and be correct.
func TestGpuPairwiseUniformRowsStillDispatch(t *testing.T) {
	const dim = 8
	x, y := raggedPairwiseInput(dim, true)
	y[0] = make([]float32, dim) // repair the ragged row: distance from all-ones is dim
	for i := range y[0] {
		y[0][i] = 0
	}

	dist := make([]float32, len(y))
	h, err := PairwiseDistanceLaunch(x, y, Metric_L2sqDistance, dist, GPUThresholdSQL, true)
	require.NoError(t, err)
	out, err := PairwiseDistanceWait(h, Metric_L2sqDistance)
	require.NoError(t, err)
	require.InDelta(t, float32(dim), out[0], 1e-4, "all-zero row against all-ones query")
	require.InDelta(t, float32(0), out[1], 1e-4, "identical row")
}
