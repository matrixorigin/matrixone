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

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// smallestNormalFloat64 is the smallest positive normal (non-subnormal) float64.
const smallestNormalFloat64 = 2.2250738585072014e-308

// cosineNormsOK reports whether a cosine kernel's own squared norms can be used as they stand.
//
// A squared norm must be a NORMAL number of the accumulation type, not merely positive and finite.
// Subnormals are the case a zero/Inf/NaN test misses: float32 [3e-23,3e-23] squares each element
// to 9e-46, which is subnormal, so the sum keeps only a couple of bits. The denominator is still
// positive and finite, so the kernel used it and answered 0.433316 where the cosine distance is
// 0.292893 -- enough to flip a `< 0.35` predicate.
//
// Zero is deliberately NOT accepted here: it sends a vector whose squares underflowed to the
// float64 recompute, which is what lets float32 [1e-30,0] still answer 0 against itself. A
// genuinely zero vector recomputes to zero too, and the caller's zero-magnitude convention takes
// it from there.
//
// smallestNormal is the caller's own accumulation bound, passed in so no type dispatch runs on the
// hot path.
func cosineNormsOK(normP, normQ, smallestNormal float64) bool {
	return normP >= smallestNormal && normP <= math.MaxFloat64 &&
		normQ >= smallestNormal && normQ <= math.MaxFloat64
}

// smallestNormalOf returns T's own smallest normal value, for the guard above. Only the scalar
// build needs it; the SIMD kernels know their accumulator type statically and pass the constant.
func smallestNormalOf[T types.RealNumbers]() float64 {
	if _, ok := any(*new(T)).(float32); ok {
		return smallestNormalFloat32
	}
	return smallestNormalFloat64
}

// cosineRecomputeF64 re-accumulates the three cosine components in float64. ok is false when even
// float64 overflows (a float64 element whose squared norm exceeds the float64 range): the cosine
// cannot be computed, and the caller errors out rather than returning the Inf/Inf = NaN such a
// pass produces.
func cosineRecomputeF64[T types.RealNumbers](p, q []T) (dot, normP, normQ float64, ok bool) {
	for i := range p {
		a, b := float64(p[i]), float64(q[i])
		dot += a * b
		normP += a * a
		normQ += b * b
	}
	return dot, normP, normQ, isFiniteF64(dot) && recomputedNormOK(normP) && recomputedNormOK(normQ)
}

// recomputedNormOK accepts a float64 squared norm the recompute can stand behind: zero, which the
// caller's zero-magnitude convention handles, or a normal number. A subnormal one cannot be
// improved by recomputing -- float64 is already the widest accumulation here -- so it is rejected
// rather than returned with most of its bits gone.
func recomputedNormOK(norm float64) bool {
	return norm == 0 || (norm >= smallestNormalFloat64 && norm <= math.MaxFloat64)
}

func isFiniteF64(v float64) bool {
	return !math.IsInf(v, 0) && !math.IsNaN(v)
}
