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

package cuvs

import (
	"fmt"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

// MaxSquaredNorm is the largest squared L2 norm a CAGRA or IVF-PQ index stores. The index
// computes L2 distances in float32; the squared distance of two such vectors is at most
// (||a|| + ||b||)^2 <= 4 * MaxSquaredNorm, the float32 maximum.
const MaxSquaredNorm = float64(math.MaxFloat32) / 4

// CheckStorable returns the reason a CAGRA or IVF-PQ index cannot store vec, a vecf32 or vecf16
// base vector, or nil. toFloat16 reports that the index stores a vecf32 base as float16.
func CheckStorable[T float32 | types.Float16](vec []T, toFloat16 bool) error {
	if len(vec) == 0 {
		return moerr.NewIndexVectorDimensionNoCtx("the vector is empty")
	}
	var sum float64
	for _, e := range vec {
		var f float32
		switch x := any(e).(type) {
		case float32:
			f = x
		case types.Float16:
			f = x.ToFloat32()
		}
		if math.IsNaN(float64(f)) || math.IsInf(float64(f), 0) {
			return moerr.NewIndexVectorNonFiniteNoCtx()
		}
		if toFloat16 && (f >= vectorindex.Float16Overflow || f <= -vectorindex.Float16Overflow) {
			return moerr.NewIndexVectorOutOfRangeNoCtx(fmt.Sprintf("vector element %v cannot be stored as float16", f))
		}
		sum += float64(f) * float64(f)
	}
	if sum > MaxSquaredNorm {
		return moerr.NewIndexVectorOutOfRangeNoCtx(fmt.Sprintf("the squared L2 norm %g exceeds %g", sum, MaxSquaredNorm))
	}
	return nil
}

// CheckQuery returns the reason a CAGRA or IVF-PQ index cannot search for query, the bytes of a
// vector of the base type base, or nil. toFloat16 reports that the index stores a vecf32 base as
// float16.
func CheckQuery(query []byte, base types.T, toFloat16 bool) error {
	if base == types.T_array_float16 {
		return CheckStorable(types.BytesToArray[types.Float16](query), false)
	}
	return CheckStorable(types.BytesToArray[float32](query), toFloat16)
}
