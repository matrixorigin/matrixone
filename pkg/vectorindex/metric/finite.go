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

// L2FromSquared narrows a squared L2 distance to the distance itself, rejecting a squared sum that
// left T's domain. The kernels accumulate the square in T, so a float32 pair 2e19 apart overflows
// to +Inf even though its distance 2.8e19 is a representable float32.
//
// The mirror case is NOT rejected: a float32 pair 1e-30 apart squares to 1e-60, which underflows
// to zero and reports the two as identical. Detecting it needs the vectors, and comparing them
// whenever the square is zero costs 447ns against 36ns at dim 768 -- an exact match is the common
// case in vector search, not a degenerate one, and it is exactly the case that walks both vectors
// to the end.
func L2FromSquared[T types.RealNumbers](sq T) (T, error) {
	if _, err := CheckFiniteDist(sq, l2What); err != nil {
		return 0, err
	}
	return T(math.Sqrt(float64(sq))), nil
}

// l2What names every L2 path -- scalar, squared, pairwise, GPU -- in its overflow error, so which
// kernel answered cannot change the error text.
const (
	l2What     = "l2 distance"
	ipWhat     = "inner product"
	cosineWhat = "cosine distance"
	l1What     = "l1 distance"
)

// MetricWhat names a metric in an overflow error.
func MetricWhat(m MetricType) string {
	switch m {
	case Metric_L2Distance, Metric_L2sqDistance:
		return l2What
	case Metric_InnerProduct:
		return ipWhat
	case Metric_CosineDistance:
		return cosineWhat
	case Metric_L1Distance:
		return l1What
	default:
		return "vector distance"
	}
}

// CheckFiniteDist rejects a distance that left T's domain: finite inputs can still produce +Inf
// (a float32 dot product of 1e20-magnitude vectors) or NaN (that +Inf cancelling a -Inf).
// x-x is 0 for every finite x and NaN for +Inf, -Inf and NaN alike.
func CheckFiniteDist[T types.RealNumbers](d T, what string) (T, error) {
	if d-d != 0 {
		return 0, moerr.NewInternalErrorNoCtx(what + nonFiniteMsg)
	}
	return d, nil
}

const nonFiniteMsg = ": vector magnitude is too large, the result overflows the element domain"

// anyNonZero reports whether v holds a value that is not zero. Used where a norm reads 0: that is
// either a genuinely zero vector, which has its own documented convention, or one whose squares
// all underflowed, which has no computable answer.
func anyNonZero[T types.RealNumbers](v []T) bool {
	for _, x := range v {
		if x != 0 {
			return true
		}
	}
	return false
}

// CheckFiniteDists rejects a distance result that holds a non-finite entry. Called where native
// (C, CUDA, usearch) results cross back into Go. A stored vector cannot hold NaN or Inf (#28688),
// so a non-finite entry is always an intermediate overflow and never an input value.
func CheckFiniteDists(dist []float32, what string) error {
	if AllFiniteF32(dist) {
		return nil
	}
	return moerr.NewInternalErrorNoCtx(what + nonFiniteMsg)
}

// CheckFiniteDists64 is CheckFiniteDists for a result read as float64.
func CheckFiniteDists64(dist []float64, what string) error {
	for _, d := range dist {
		if d-d != 0 {
			return moerr.NewInternalErrorNoCtx(what + nonFiniteMsg)
		}
	}
	return nil
}

// AllFiniteF32 reports whether every entry is finite. Callers that answer a non-finite batch
// result some other way use this instead of CheckFiniteDists.
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
