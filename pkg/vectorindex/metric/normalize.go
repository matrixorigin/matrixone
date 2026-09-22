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

// NormalizeL2 writes the L2-normalized v1 into normalized. An all-zero vector is copied unchanged.
// The norm is accumulated in float64 regardless of T.
//
// A vector whose squared norm leaves the float64 domain has no computable norm here: squaring a
// float64 element above ~1.3e154 overflows to +Inf (every component would normalize to 0), and
// squaring one below ~1.5e-162 underflows to 0 (the vector would be returned unnormalized). Both
// are rejected rather than returned silently wrong.
func NormalizeL2[T types.RealNumbers](v1 []T, normalized []T) error {
	if len(v1) == 0 {
		return moerr.NewInternalErrorNoCtx("cannot normalize empty vector")
	}
	var sumSquares float64
	for _, val := range v1 {
		sumSquares += float64(val) * float64(val)
	}
	if math.IsInf(sumSquares, 0) || math.IsNaN(sumSquares) {
		return moerr.NewInternalErrorNoCtx("cannot normalize vector: its squared norm overflows the float64 domain")
	}
	norm := math.Sqrt(sumSquares)
	if norm == 0 {
		// A zero norm is either an all-zero vector or one whose squares all underflowed. Telling
		// them apart needs a second pass, but only on this degenerate input -- an all-zero test
		// inside the accumulation loop above would cost every ordinary vector a branch per element.
		for _, val := range v1 {
			if val != 0 {
				return moerr.NewInternalErrorNoCtx("cannot normalize vector: its squared norm underflows the float64 domain")
			}
		}
		copy(normalized, v1)
		return nil
	}
	for i, val := range v1 {
		normalized[i] = T(float64(val) / norm)
	}
	return nil
}
