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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// CONTRACT: no distance function returns NaN. A magnitude that overflows the
// element domain yields a well-ordered result -- a genuine +/-Inf, a clamped
// finite value, or the NaN-from-cancellation mapped to +Inf -- never an unordered
// NaN that would corrupt a top-k ranking. This holds on every build: these inputs
// are all same-signed, so they overflow the accumulator sequentially too (the
// lane-cancellation NaN that is SIMD-specific is covered in the _simd_test).
//
// Finiteness is NOT turned into an error here: an overflow is reported as an error
// only at the consumer score boundary (moarray / the SQL array-distance builtins /
// index Search's CheckFiniteDists). The kernels return the well-ordered value so
// search ranking stays correct (#29496).
func TestMetricNeverReturnsNaN29496(t *testing.T) {
	const n = 128
	mag := float32(1 << 63)
	ab := make([]types.BF16, n)
	for i := range ab {
		ab[i] = types.BF16FromFloat32(mag)
	}
	bigF32 := make([]float32, n)
	bigF64 := make([]float64, n)
	for i := range bigF32 {
		bigF32[i], bigF64[i] = mag, 1e200
	}

	check := func(t *testing.T, d float64, err error) {
		t.Helper()
		require.NoError(t, err, "kernel must not turn overflow into an error")
		require.Falsef(t, math.IsNaN(d), "distance must never be NaN, got %v", d)
	}

	t.Run("ip_f32", func(t *testing.T) { d, err := InnerProduct[float32](bigF32, bigF32); check(t, float64(d), err) })
	t.Run("ip_f64", func(t *testing.T) { d, err := InnerProduct[float64](bigF64, bigF64); check(t, d, err) })
	for _, m := range []MetricType{Metric_InnerProduct, Metric_CosineDistance, Metric_L2sqDistance, Metric_L1Distance} {
		t.Run("bf16_"+MetricWhat(m), func(t *testing.T) {
			fn, err := ResolveDistanceFn[types.BF16, float64](m)
			require.NoError(t, err)
			d, err := fn(ab, ab)
			check(t, d, err)
		})
	}
	t.Run("spherical_f32", func(t *testing.T) { d, err := SphericalDistance[float32](bigF32, bigF32); check(t, float64(d), err) })

	// Ordinary vectors are unaffected.
	t.Run("finite_ok", func(t *testing.T) {
		a, b := make([]float32, n), make([]float32, n)
		for i := range a {
			a[i], b[i] = 1.5, 2.0
		}
		d, err := InnerProduct[float32](a, b)
		require.NoError(t, err)
		require.False(t, math.IsNaN(float64(d)) || math.IsInf(float64(d), 0))
	})
}
