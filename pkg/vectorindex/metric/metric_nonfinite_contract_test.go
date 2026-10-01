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

// CONTRACT: the metric distance kernels DO NOT reject a non-finite result. An
// accumulation that overflows the element domain returns +/-Inf (and NaN once
// opposite-signed SIMD lanes cancel) with a nil error. This test pins that
// contract: if someone adds a finite check INSIDE a kernel (the wrong layer),
// these assertions fail and point them back to the boundary (#29496).
//
// WHY returning NaN/Inf (not an error) is correct here:
//   - These kernels are the innermost hot-path primitive: a brute-force or pairwise
//     search calls them O(rows) times, each over O(dim) elements. A per-call finite
//     branch (or a defensive clamp) is pure overhead on the common path for a case
//     that cannot occur with stored data -- an indexed vector can never hold Inf/NaN
//     (#28688), so a non-finite result only arises from ~1e19+ synthetic magnitudes.
//   - IEEE-754 +/-Inf/NaN IS the correct, information-preserving signal that the
//     accumulation left the element domain; swallowing it in the kernel would hide
//     the condition from the one place that can report it cleanly.
//
// WHEN/WHERE to check finiteness instead -- ONCE, at the consumer boundary, so the
// cost is one pass per batch/row rather than one branch per distance:
//   - batch index Search: CheckFiniteDists over the result slice (usearch/hnsw/
//     ivfpq/cagra, metric/gpu), or riding along the existing pass (the L2 sqrt
//     screen in GoPairWiseDistance);
//   - single scalar result: CheckFiniteDist in moarray and the SQL array-distance
//     builtins (e.g. arrayDistanceNarrow).
//
// Only bf16/f32/f64 can overflow: f16's max magnitude is 65504, so its products
// stay well within the float32 accumulator (hence it is absent here, as in the
// issue's table).
func TestMetricReturnsNonFiniteWithoutChecking29496(t *testing.T) {
	const n = 128
	nonFinite := func(v float64) bool { return math.IsInf(v, 0) || math.IsNaN(v) }

	t.Run("f32", func(t *testing.T) {
		a, b := make([]float32, n), make([]float32, n)
		for i := range a {
			// product 2^126 is finite in float32; the sum of n overflows to +Inf.
			a[i], b[i] = float32(1<<63), float32(1<<63)
		}
		d, err := InnerProduct[float32](a, b)
		require.NoError(t, err, "metric must not reject a non-finite result")
		require.Truef(t, nonFinite(float64(d)), "metric must return the raw non-finite value, got %v", d)
	})
	t.Run("f64", func(t *testing.T) {
		a, b := make([]float64, n), make([]float64, n)
		for i := range a {
			// product 1e308 is finite in float64; the sum of n overflows to +Inf.
			a[i], b[i] = 1e154, 1e154
		}
		d, err := InnerProduct[float64](a, b)
		require.NoError(t, err, "metric must not reject a non-finite result")
		require.Truef(t, nonFinite(d), "metric must return the raw non-finite value, got %v", d)
	})
	t.Run("bf16_via_resolve", func(t *testing.T) {
		a, b := make([]types.BF16, n), make([]types.BF16, n)
		for i := range a {
			a[i], b[i] = types.BF16FromFloat32(float32(1<<63)), types.BF16FromFloat32(float32(1<<63))
		}
		fn, err := ResolveDistanceFn[types.BF16, float64](Metric_InnerProduct)
		require.NoError(t, err)
		d, err := fn(a, b)
		require.NoError(t, err, "ResolveDistanceFn must not reject a non-finite result")
		require.Truef(t, nonFinite(d), "metric must return the raw non-finite value, got %v", d)
	})
}
