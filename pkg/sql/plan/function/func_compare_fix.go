// Copyright 2021 - 2022 Matrix Origin
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

package function

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// numericCompareWithTypeMismatch handles comparison when two numeric types don't match.
// It converts both operands to float64 for comparison, which is safe for all numeric types.
// This is a fallback for cases where plan-stage type casting was not applied correctly.
func numericCompareWithTypeMismatch(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	cmpFn func(float64, float64) bool,
	selectList *FunctionSelectList,
) error {
	rs := vector.MustFunctionResult[bool](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedColNoTypeCheck[bool](rsVec)

	// Get values as float64 from both vectors
	v1Float64 := getAsFloat64Slice(parameters[0])
	v2Float64 := getAsFloat64Slice(parameters[1])

	c1, c2 := parameters[0].IsConst(), parameters[1].IsConst()

	if c1 && c2 {
		// Both are constants
		null1 := parameters[0].IsConstNull()
		null2 := parameters[1].IsConstNull()
		if null1 || null2 {
			rsVec.GetNulls().AddRange(0, uint64(length))
		} else {
			r := cmpFn(v1Float64[0], v2Float64[0])
			for i := 0; i < length; i++ {
				rss[i] = r
			}
		}
		return nil
	}

	if c1 {
		if parameters[0].IsConstNull() {
			rsVec.GetNulls().AddRange(0, uint64(length))
			return nil
		}
		val1 := v1Float64[0]
		nsp2 := parameters[1].GetNulls()
		for i := 0; i < length; i++ {
			if nsp2.Contains(uint64(i)) {
				rsVec.GetNulls().Add(uint64(i))
			} else {
				rss[i] = cmpFn(val1, v2Float64[i])
			}
		}
		return nil
	}

	if c2 {
		if parameters[1].IsConstNull() {
			rsVec.GetNulls().AddRange(0, uint64(length))
			return nil
		}
		val2 := v2Float64[0]
		nsp1 := parameters[0].GetNulls()
		for i := 0; i < length; i++ {
			if nsp1.Contains(uint64(i)) {
				rsVec.GetNulls().Add(uint64(i))
			} else {
				rss[i] = cmpFn(v1Float64[i], val2)
			}
		}
		return nil
	}

	// Neither is constant
	nsp1 := parameters[0].GetNulls()
	nsp2 := parameters[1].GetNulls()
	for i := 0; i < length; i++ {
		if nsp1.Contains(uint64(i)) || nsp2.Contains(uint64(i)) {
			rsVec.GetNulls().Add(uint64(i))
		} else {
			rss[i] = cmpFn(v1Float64[i], v2Float64[i])
		}
	}
	return nil
}

// getAsFloat64Slice converts a vector to a float64 slice for comparison
func getAsFloat64Slice(v *vector.Vector) []float64 {
	t := v.GetType()
	switch t.Oid {
	case types.T_int8:
		cols := vector.MustFixedColNoTypeCheck[int8](v)
		result := make([]float64, len(cols))
		for i, val := range cols {
			result[i] = float64(val)
		}
		return result
	case types.T_int16:
		cols := vector.MustFixedColNoTypeCheck[int16](v)
		result := make([]float64, len(cols))
		for i, val := range cols {
			result[i] = float64(val)
		}
		return result
	case types.T_int32:
		cols := vector.MustFixedColNoTypeCheck[int32](v)
		result := make([]float64, len(cols))
		for i, val := range cols {
			result[i] = float64(val)
		}
		return result
	case types.T_int64:
		cols := vector.MustFixedColNoTypeCheck[int64](v)
		result := make([]float64, len(cols))
		for i, val := range cols {
			result[i] = float64(val)
		}
		return result
	case types.T_uint8:
		cols := vector.MustFixedColNoTypeCheck[uint8](v)
		result := make([]float64, len(cols))
		for i, val := range cols {
			result[i] = float64(val)
		}
		return result
	case types.T_uint16:
		cols := vector.MustFixedColNoTypeCheck[uint16](v)
		result := make([]float64, len(cols))
		for i, val := range cols {
			result[i] = float64(val)
		}
		return result
	case types.T_uint32:
		cols := vector.MustFixedColNoTypeCheck[uint32](v)
		result := make([]float64, len(cols))
		for i, val := range cols {
			result[i] = float64(val)
		}
		return result
	case types.T_uint64:
		cols := vector.MustFixedColNoTypeCheck[uint64](v)
		result := make([]float64, len(cols))
		for i, val := range cols {
			result[i] = float64(val)
		}
		return result
	case types.T_float32:
		cols := vector.MustFixedColNoTypeCheck[float32](v)
		result := make([]float64, len(cols))
		for i, val := range cols {
			result[i] = float64(val)
		}
		return result
	case types.T_float64:
		return vector.MustFixedColNoTypeCheck[float64](v)
	default:
		// For other types, return empty slice (should not happen for numeric comparisons)
		return nil
	}
}

// shouldUseTypeMismatchPath checks if two vectors have mismatched numeric types
// that require the fallback comparison path
func shouldUseTypeMismatchPath(v1, v2 *vector.Vector) bool {
	t1 := v1.GetType().Oid
	t2 := v2.GetType().Oid
	if t1 == t2 {
		return false
	}
	// Check if both are numeric types
	return isNumericType(t1) && isNumericType(t2)
}

func isNumericType(t types.T) bool {
	switch t {
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64:
		return true
	default:
		return false
	}
}
