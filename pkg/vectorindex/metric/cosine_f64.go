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

// cosineDenOK reports whether a cosine kernel's own denominator ||p||*||q|| can be used as it
// stands, i.e. whether the T-domain accumulation kept the cosine.
//
// Testing the denominator rather than the two norms tests a value the kernel computes anyway, and
// loses nothing: both norms are sums of squares, so den is 0 exactly when a norm is 0 (sqrt of a
// positive float64 cannot underflow to 0, and the product of two such roots cannot either), +Inf
// when a norm overflowed, and NaN when a norm is NaN -- and NaN fails both comparisons. The
// product itself cannot overflow: sqrt(MaxFloat64)^2 is MaxFloat64. The dot product needs no test
// of its own -- |dot| <= (normP+normQ)/2, so it is finite whenever the norms are.
//
// A zero-magnitude vector also fails this test; the float64 recompute confirms the norm really is
// zero and the caller then applies its own zero-magnitude convention.
//
// Two comparisons, taken once per vector pair, keep the check off the kernel's inner loop.
func cosineDenOK(den float64) bool {
	return den > 0 && den <= math.MaxFloat64
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
	return dot, normP, normQ, isFiniteF64(dot) && isFiniteF64(normP) && isFiniteF64(normQ)
}

func isFiniteF64(v float64) bool {
	return !math.IsInf(v, 0) && !math.IsNaN(v)
}
