//go:build (amd64 || arm64) && go1.27 && goexperiment.simd

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

// CONTRACT: a distance function maps a NaN to +Inf, it never returns NaN. The SIMD
// kernels accumulate signed products in several float lanes and reduce at the end,
// so an input whose products are each finite but whose per-lane partial sums
// overflow to +/-Inf before the reduction produces NaN (Inf + -Inf) where a
// sequential scalar sum would stay finite. NaN is unordered and corrupts a top-k
// ranking (it is never evicted and drops a valid candidate), so every NaN-capable
// distance function maps it to +Inf -- the largest distance -- which ranks the
// overflowing candidate last instead. This input (a[i]=mag, b[i] alternating
// +/-mag, dim 32) drives the lanes to NaN on the SIMD build; the scalar build
// cancels in source order and never hits it, which is why this is SIMD-tagged
// (#29496).
func TestMetricMapsLaneCancellationToPosInf29496(t *testing.T) {
	const dim = 32
	mag32 := float32(1 << 63)
	af, bf := make([]float32, dim), make([]float32, dim)
	ad, bd := make([]float64, dim), make([]float64, dim)
	ab, bb := make([]types.BF16, dim), make([]types.BF16, dim)
	mag64 := math.Ldexp(1, 511)
	for i := 0; i < dim; i++ {
		af[i], ad[i], ab[i] = mag32, mag64, types.BF16FromFloat32(mag32)
		if i%2 == 0 {
			bf[i], bd[i], bb[i] = mag32, mag64, types.BF16FromFloat32(mag32)
		} else {
			bf[i], bd[i], bb[i] = -mag32, -mag64, types.BF16FromFloat32(-mag32)
		}
	}
	isPosInf := func(v float64) bool { return math.IsInf(v, 1) }

	t.Run("ip_f32", func(t *testing.T) {
		d, err := InnerProduct[float32](af, bf)
		require.NoError(t, err)
		require.Truef(t, isPosInf(float64(d)), "lane cancellation must map to +Inf, got %v", d)
	})
	t.Run("ip_f64", func(t *testing.T) {
		d, err := InnerProduct[float64](ad, bd)
		require.NoError(t, err)
		require.Truef(t, isPosInf(d), "lane cancellation must map to +Inf, got %v", d)
	})
	t.Run("ip_bf16", func(t *testing.T) {
		fn, err := ResolveDistanceFn[types.BF16, float64](Metric_InnerProduct)
		require.NoError(t, err)
		d, err := fn(ab, bb)
		require.NoError(t, err)
		require.Truef(t, isPosInf(d), "lane cancellation must map to +Inf, got %v", d)
	})
	t.Run("cosine_bf16", func(t *testing.T) {
		fn, err := ResolveDistanceFn[types.BF16, float64](Metric_CosineDistance)
		require.NoError(t, err)
		d, err := fn(ab, bb)
		require.NoError(t, err)
		require.Truef(t, isPosInf(d), "lane cancellation must map to +Inf, got %v", d)
	})
	t.Run("spherical_f32", func(t *testing.T) {
		d, err := SphericalDistance[float32](af, bf)
		require.NoError(t, err)
		require.Truef(t, isPosInf(float64(d)), "lane cancellation must map to +Inf, got %v", d)
	})
}
