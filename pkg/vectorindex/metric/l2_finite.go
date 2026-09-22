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

// L2FromSquared narrows a squared L2 distance to the distance itself, rejecting a squared sum
// that left T's domain.
//
// The kernels accumulate the square in T, so summing finite elements can overflow T even when the
// distance is representable (a float32 pair 2e19 apart squares past float32 while 2.8e19 is a fine
// float32). sqrt of that overflow is +Inf, which is not a distance and silently corrupts any
// ordering built on it, so it is rejected here.
//
// Accumulating in float64 instead would return the distance, but it costs the float32 kernel its
// AVX-512 form -- 47.7ns vs 457ns per dim-768 pair, and +24% on a balanced k-means build -- and it
// makes the scalar kernel disagree with the batch kernel, which stays in float32. One domain, one
// answer, and an error where that domain runs out.
func L2FromSquared[T types.RealNumbers](sq T) (T, error) {
	if sq-sq != 0 {
		return 0, moerr.NewInternalErrorNoCtx(l2OverflowMsg)
	}
	return T(math.Sqrt(float64(sq))), nil
}

const l2OverflowMsg = "l2 distance: vector magnitude is too large, the squared distance overflows the element domain"

// CheckL2Finite rejects a pairwise L2 result that holds a non-finite entry.
//
// This is L2FromSquared's check applied to a whole pairwise result. The CPU loop reaches +Inf on
// overflow; cuVS computes the expanded form ||a||^2 + ||b||^2 - 2ab, whose overflow is
// Inf - Inf = NaN. Neither is a distance, so the query fails instead of returning one.
//
// A stored vector cannot hold NaN or Inf -- those are rejected at the cast boundary (#28688) --
// so a non-finite entry is always this intermediate overflow and never an input value.
func CheckL2Finite(dist []float32) error {
	if AllFiniteF32(dist) {
		return nil
	}
	return moerr.NewInternalErrorNoCtx(l2OverflowMsg)
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
