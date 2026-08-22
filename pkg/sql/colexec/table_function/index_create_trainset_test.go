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

	"github.com/stretchr/testify/require"
)

// planTrainFraction resolves the k-means sample against the capacity and the
// training-row bound the index reported. The per-row costs behind that bound now
// live in C++ (gpu_ivf_pq_t::device_bytes_per_row / trainset_bytes_per_row), so
// what is left to test here is the clamping arithmetic, not the cost model.
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
