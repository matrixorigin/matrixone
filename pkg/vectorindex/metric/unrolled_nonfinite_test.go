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

// The *Unrolled oracles are the non-SIMD build's kernels AND the SIMD build's recovery reference.
// They sum 8-wide (4-wide for cosine), so a block of same-sign extreme-but-finite products can
// overflow to +/-Inf and two opposite-sign blocks cancel to NaN -- the same artifact as the SIMD
// lanes, just at block width, NOT only on genuine overflow. NaN is unordered and corrupts top-k
// ranking, so each oracle applies the no-NaN finalizer (NaN -> +Inf) itself. This proves it in a
// build-independent way, which covers the non-SIMD build whose wrappers delegate straight to these
// oracles (the build-time scalar hole from the #29496 review, separate from any runtime CPU flag).
func TestUnrolledOraclesNeverReturnNaN29496(t *testing.T) {
	const dim = 64
	// m*m = 2^126 is a finite float32; an 8-wide block sums to 8*2^126 = 2^129 -> +Inf.
	m := float32(1 << 63)
	query := make([]float32, dim)
	block := make([]float32, dim) // +m at 0..7, -m at 32..39: block 0 -> +Inf, block 4 -> -Inf, dot -> NaN
	for i := range query {
		query[i] = m
	}
	for i := 0; i < 8; i++ {
		block[i] = m
		block[32+i] = -m
	}

	t.Run("inner_product", func(t *testing.T) {
		d, err := InnerProductUnrolled[float32](query, block)
		require.NoError(t, err)
		require.Falsef(t, math.IsNaN(float64(d)), "inner product oracle must not return NaN, got %v", d)
		require.Truef(t, math.IsInf(float64(d), 1), "a genuine overflow must saturate to +Inf, got %v", d)
	})
	t.Run("spherical", func(t *testing.T) {
		d, err := SphericalDistanceUnrolled[float32](query, block)
		require.NoError(t, err)
		require.Falsef(t, math.IsNaN(float64(d)), "spherical oracle must not return NaN, got %v", d)
		require.Truef(t, math.IsInf(float64(d), 1), "a NaN dot must map to +Inf, got %v", d)
	})
	t.Run("cosine", func(t *testing.T) {
		d, err := CosineDistanceUnrolled[float32](query, block)
		require.NoError(t, err)
		require.Falsef(t, math.IsNaN(float64(d)), "cosine oracle must not return NaN, got %v", d)
	})
}
