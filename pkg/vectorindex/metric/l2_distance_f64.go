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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// l2DistanceF64 computes the L2 distance accumulating the squared sum in float64. Accumulating in
// float64 (not the element type) keeps the intermediate squared sum from overflowing for a float32
// base whose distance is still representable (#29083). The result is cast back to T, so the exposed
// float32 distance stays in its own domain (#29040/#29050).
func l2DistanceF64[T types.RealNumbers](v1, v2 []T) (T, error) {
	if len(v1) != len(v2) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	var sum float64
	for i := range v1 {
		d := float64(v1[i]) - float64(v2[i])
		sum += d * d
	}
	return T(math.Sqrt(sum)), nil
}

// CheckL2Finite rejects a pairwise L2 result that holds a non-finite entry.
//
// The batch kernels accumulate the squared distance in float32 -- the CPU loop and cuVS on the
// GPU -- so a pair whose square exceeds float32 (|diff| above ~1.8e19) loses the result: the CPU
// loop reaches +Inf, and cuVS computes the expanded form ||a||^2 + ||b||^2 - 2ab, whose overflow
// is Inf - Inf = NaN. Neither value is a distance, and both would silently corrupt an ordering,
// so the query fails here instead of returning one.
//
// A stored vector cannot hold NaN or Inf -- those are rejected at the cast boundary (#28688) --
// so a non-finite entry is always this intermediate overflow and never an input value.
func CheckL2Finite(dist []float32) error {
	if AllFiniteF32(dist) {
		return nil
	}
	return moerr.NewInternalErrorNoCtx("l2 distance: vector magnitude is too large, the squared distance overflows the float32 domain")
}

// AllFiniteF32 reports whether every entry is finite. Callers that can answer a non-finite batch
// result some other way -- the SQL functions hand those rows to the per-row kernel, which
// accumulates in float64 -- use this instead of CheckL2Finite.
func AllFiniteF32(dist []float32) bool {
	for i := range dist {
		if !isFiniteF32(dist[i]) {
			return false
		}
	}
	return true
}

// isFiniteF32 is one subtraction and one comparison: x-x is 0 for every finite x, and NaN for
// +Inf, -Inf and NaN alike. It is called once per pairwise result entry, so it must not become a
// call to math.IsInf/math.IsNaN through a float64 conversion.
func isFiniteF32(x float32) bool {
	return x-x == 0
}
