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

package table_function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/stretchr/testify/require"
)

// calculatePqDim must agree with cuVS index<IdxT>::calculate_pq_dim
// (cuvs/cpp/src/neighbors/ivf_pq_index.cu:611) or every derived capacity is wrong
// for builds that leave m unset. The wiki_all case is the one that bites: at dim
// 768 cuVS picks 384 while the benchmark template configures 192, so a default-m
// index costs twice as much device memory as a configured one.
func TestCalculatePqDim(t *testing.T) {
	for _, tc := range []struct {
		dim, want uint64
		why       string
	}{
		{768, 384, "wiki_all: halved to 384, already a multiple of 32"},
		{960, 480, "halved to 480, already a multiple of 32"},
		{128, 64, "boundary: >=128 so it halves"},
		{127, 96, "just under the boundary: no halving, round down 127 -> 96"},
		{100, 96, "no halving, round down to a multiple of 32"},
		{64, 64, "no halving, already a multiple of 32"},
		{30, 16, "rounds down to 0, so falls back to the largest power of two <= dim"},
		{1, 1, "degenerate"},
	} {
		require.Equalf(t, tc.want, calculatePqDim(tc.dim), "dim=%d (%s)", tc.dim, tc.why)
	}
}

// pqCodeBytes is what bounds capacity now that the dataset is streamed, so it has
// to count the int64 payload cuVS stores next to each code — omitting it
// under-counts every row by 8 bytes, which at 88M rows is 0.7 GB.
func TestPqCodeBytes(t *testing.T) {
	require.Equal(t, uint64(200), pqCodeBytes(768, 192, 8), "wiki_all 88M: 192 code + 8 index")
	require.Equal(t, uint64(392), pqCodeBytes(768, 0, 8), "m unset -> cuVS default 384, + 8")
	require.Equal(t, uint64(200), pqCodeBytes(768, 192, 0), "bits unset -> 8")
	require.Equal(t, uint64(104), pqCodeBytes(768, 192, 4), "4-bit codes pack two per byte")
	require.Equal(t, uint64(33), pqCodeBytes(768, 100, 2), "200 bits rounds up to 25 bytes, + 8")
}

// The counter-intuitive one: a narrower storage type makes the TRAINSET bigger,
// because cuVS keeps a float32 trainset and, for non-float T, a second copy in T
// (ivf_pq_build.cuh:1288-1307). Sizing this with the storage width under-counts
// f16 by a third and is how a derived default OOMs.
func TestTrainsetBytesPerElem(t *testing.T) {
	require.Equal(t, uint64(4), trainsetBytesPerElem(metric.Quantization_F32))
	require.Equal(t, uint64(6), trainsetBytesPerElem(metric.Quantization_F16), "4 f32 + 2 half")
	require.Equal(t, uint64(5), trainsetBytesPerElem(metric.Quantization_INT8), "4 f32 + 1 int8")
	require.Equal(t, uint64(5), trainsetBytesPerElem(metric.Quantization_UINT8))
	require.Greater(t, trainsetBytesPerElem(metric.Quantization_F16),
		trainsetBytesPerElem(metric.Quantization_F32),
		"narrow storage must cost MORE here, not less")
}

func TestPlanTrainFraction(t *testing.T) {
	t.Run("request honoured when the device can hold it", func(t *testing.T) {
		p := planTrainFraction(1_000_000, 500_000, 1000, 0.2)
		require.Equal(t, int64(200_000), p.Rows)
		require.InDelta(t, 0.2, p.Fraction, 1e-9)
		require.False(t, p.Clamped)
		require.False(t, p.Thin, "200 rows per centroid is plenty")
	})

	t.Run("clamped to what the device can hold", func(t *testing.T) {
		// The wiki_all shape: 20% of 88M cannot be trained on a 20 GB card.
		p := planTrainFraction(88_000_000, 3_280_000, 6000, 0.2)
		require.Equal(t, int64(3_280_000), p.Rows)
		require.InDelta(t, 0.0373, p.Fraction, 1e-4, "20% requested resolves to 3.7%")
		require.True(t, p.Clamped, "the clamp must be visible to the caller, not silent")
		require.False(t, p.Thin, "546 points per centroid is still fine")
	})

	t.Run("cuVS n_lists floor beats the device clamp", func(t *testing.T) {
		// A sample below n_lists cannot seed the centroids at all. cuVS floors it
		// itself (ivf_pq_build.cuh:1257-1260); mirroring that keeps the reported
		// fraction honest rather than promising something cuVS will not do.
		p := planTrainFraction(1_000_000, 100, 5000, 0.001)
		require.Equal(t, int64(5000), p.Rows, "floored at n_lists, not the 1000 requested")
		require.True(t, p.Thin)
	})

	t.Run("thin centroids are flagged, not rejected", func(t *testing.T) {
		// The case the plan cared about: capacity forced down by a split while
		// n_lists stays sized for the whole table.
		p := planTrainFraction(3_000_000, 0, 37_500, 0.2)
		require.Equal(t, int64(600_000), p.Rows)
		require.True(t, p.Thin, "16 points per centroid is under the ~39 floor")
		require.False(t, p.Clamped, "no device measurement, so nothing was clamped")
	})

	t.Run("sample never exceeds the population", func(t *testing.T) {
		p := planTrainFraction(1000, 10_000_000, 100, 1.0)
		require.Equal(t, int64(1000), p.Rows)
		require.InDelta(t, 1.0, p.Fraction, 1e-9)
	})

	t.Run("unmeasured device leaves the request alone", func(t *testing.T) {
		p := planTrainFraction(1_000_000, 0, 1000, 0.1)
		require.Equal(t, int64(100_000), p.Rows)
		require.False(t, p.Clamped)
	})

	t.Run("out-of-range fraction falls back to the whole population", func(t *testing.T) {
		for _, f := range []float64{0, -1, 1.5} {
			p := planTrainFraction(1000, 0, 10, f)
			require.Equalf(t, int64(1000), p.Rows, "fraction %v", f)
		}
	})

	t.Run("zero capacity is inert", func(t *testing.T) {
		require.Equal(t, trainPlan{}, planTrainFraction(0, 100, 10, 0.5))
	})
}
