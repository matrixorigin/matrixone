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

package moarray

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestInnerProductFiniteProductCancellation29496 is the public-result oracle for the metric
// owner's finite recovery. The inputs are finite and valid (types.StringToArray accepts +/-2^63):
// every element product and every source-order partial sum is finite, and the dot cancels to 0.
// A NEON SIMD lane sum overflows to +/-Inf before the cross-lane reduction cancels, which the
// scalar builtin would then surface as an overflow error -- a public divergence from the scalar
// path's finite 0. With the metric owner recomputing the in-order reference on a non-finite SIMD
// result, the public result is the finite 0 on every build (#29496).
func TestInnerProductFiniteProductCancellation29496(t *testing.T) {
	const dim = 128
	big := float32(1 << 63)
	q := make([]float32, dim)
	cand := make([]float32, dim)
	for i := range q {
		q[i] = big
		if i%2 == 0 {
			cand[i] = big
		} else {
			cand[i] = -big
		}
	}
	d, err := InnerProduct[float32](q, cand)
	require.NoError(t, err, "finite-product cancellation must not surface as an overflow error")
	require.InDelta(t, 0.0, d, 1e-9, "inner product must recover the in-order 0, got %v", d)
}
