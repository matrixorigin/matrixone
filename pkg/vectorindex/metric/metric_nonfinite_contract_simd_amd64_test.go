//go:build amd64 && go1.27 && goexperiment.simd

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

	"simd/archsimd"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// CONTRACT (AVX-512): the SIMD kernels accumulate signed products in several float lanes and
// reduce at the end, so an input whose products are each finite but whose per-lane partial sums
// overflow to +/-Inf before the reduction produces NaN (Inf + -Inf) where the in-order scalar
// reference cancels and stays finite. The metric owner RECOVERS that reference answer: on a
// non-finite SIMD result it recomputes the dot in float64 in source order (the exceptional path,
// mirroring cosineRecomputeF64), so SIMD agrees with the scalar oracle. Only a GENUINE overflow --
// one the in-order f64 sum cannot represent either -- fast-fails to +Inf, which the serve boundary
// rejects. The ordinary fast path is unchanged.
//
// The test exercises the SIMD kernels DIRECTLY, independent of the MO_METRIC_NO_AVX512 override:
// narrow kernels by name (gate-free), and hasAVX512 forced on for the real kernels (dim 128 >= 64).
// Executing AVX-512 on a CPU without it is an illegal instruction, so the test skips when absent.
// The cancellation dot is 0, so inner product recovers 0 and spherical recovers acos(0)/pi = 0.5 --
// exactly what the scalar path returns (#29496 / #29271-review).
func TestMetricRecoversLaneCancellation29496(t *testing.T) {
	if !archsimd.X86.AVX512() {
		t.Skip("AVX-512 not available on this CPU; real kernels are AVX-512-only")
	}
	saved := hasAVX512
	hasAVX512 = true
	defer func() { hasAVX512 = saved }()

	const dim = 128
	mag32 := float32(1 << 63)
	mag64 := math.Ldexp(1, 511)
	af, bf := make([]float32, dim), make([]float32, dim)
	ad, bd := make([]float64, dim), make([]float64, dim)
	ab, bb := make([]types.BF16, dim), make([]types.BF16, dim)
	for i := 0; i < dim; i++ {
		af[i], ad[i], ab[i] = mag32, mag64, types.BF16FromFloat32(mag32)
		if i%2 == 0 {
			bf[i], bd[i], bb[i] = mag32, mag64, types.BF16FromFloat32(mag32)
		} else {
			bf[i], bd[i], bb[i] = -mag32, -mag64, types.BF16FromFloat32(-mag32)
		}
	}

	t.Run("ip_f32", func(t *testing.T) {
		d, err := InnerProduct[float32](af, bf)
		require.NoError(t, err)
		require.InDelta(t, 0.0, float64(d), 1e-9, "lane cancellation must recover the in-order 0, got %v", d)
	})
	t.Run("ip_f64", func(t *testing.T) {
		d, err := InnerProduct[float64](ad, bd)
		require.NoError(t, err)
		require.InDelta(t, 0.0, d, 1e-9, "lane cancellation must recover the in-order 0, got %v", d)
	})
	t.Run("spherical_f32", func(t *testing.T) {
		d, err := SphericalDistance[float32](af, bf)
		require.NoError(t, err)
		require.InDelta(t, 0.5, float64(d), 1e-6, "lane cancellation must recover acos(0)/pi = 0.5, got %v", d)
	})
	t.Run("ip_bf16", func(t *testing.T) {
		d, err := innerProductBF16SIMD(ab, bb)
		require.NoError(t, err)
		require.InDelta(t, 0.0, d, 1e-9, "lane cancellation must recover the in-order 0, got %v", d)
	})

	// Narrow cosine recovers too: the SIMD dot lanes cancel to NaN (cosineDistClamped -> +Inf), then the
	// metric owner recomputes via the scalar reference. The dot cancels to 0, so the cosine distance is 1.
	t.Run("cosine_bf16", func(t *testing.T) {
		d, err := cosineDistanceBF16SIMD(ab, bb)
		require.NoError(t, err)
		require.InDeltaf(t, 1.0, d, 1e-9, "lane cancellation must recover cosine distance 1, got %v", d)
	})
}
