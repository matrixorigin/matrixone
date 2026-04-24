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
	"math"
	"math/big"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type numericCompareKind uint8

const (
	numericCompareFinite numericCompareKind = iota
	numericCompareNegInf
	numericComparePosInf
	numericCompareNaN
)

type numericCompareValue struct {
	kind numericCompareKind
	rat  *big.Rat
}

// numericCompareWithTypeMismatch handles comparison when two numeric types don't match.
// It compares both operands via exact rational values to avoid precision loss across type boundaries.
// This is a fallback for cases where plan-stage type casting was not applied correctly.
func numericCompareWithTypeMismatch(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	cmpFn func(int, bool) bool,
	selectList *FunctionSelectList,
) error {
	if usesApproximateNumericCompare(parameters[0], parameters[1]) {
		return numericCompareWithTypeMismatchApprox(parameters, result, proc, length, cmpFn, selectList)
	}

	rs := vector.MustFunctionResult[bool](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedColNoTypeCheck[bool](rsVec)

	v1Cmp := getAsCompareValueSlice(parameters[0])
	v2Cmp := getAsCompareValueSlice(parameters[1])

	c1, c2 := parameters[0].IsConst(), parameters[1].IsConst()

	if c1 && c2 {
		// Both are constants
		null1 := parameters[0].IsConstNull()
		null2 := parameters[1].IsConstNull()
		if null1 || null2 {
			rsVec.GetNulls().AddRange(0, uint64(length))
		} else {
			cmp, comparable := compareNumericValues(v1Cmp[0], v2Cmp[0])
			r := cmpFn(cmp, comparable)
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
		val1 := v1Cmp[0]
		nsp2 := parameters[1].GetNulls()
		for i := 0; i < length; i++ {
			if nsp2.Contains(uint64(i)) {
				rsVec.GetNulls().Add(uint64(i))
			} else {
				cmp, comparable := compareNumericValues(val1, v2Cmp[i])
				rss[i] = cmpFn(cmp, comparable)
			}
		}
		return nil
	}

	if c2 {
		if parameters[1].IsConstNull() {
			rsVec.GetNulls().AddRange(0, uint64(length))
			return nil
		}
		val2 := v2Cmp[0]
		nsp1 := parameters[0].GetNulls()
		for i := 0; i < length; i++ {
			if nsp1.Contains(uint64(i)) {
				rsVec.GetNulls().Add(uint64(i))
			} else {
				cmp, comparable := compareNumericValues(v1Cmp[i], val2)
				rss[i] = cmpFn(cmp, comparable)
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
			cmp, comparable := compareNumericValues(v1Cmp[i], v2Cmp[i])
			rss[i] = cmpFn(cmp, comparable)
		}
	}
	return nil
}

func numericCompareWithTypeMismatchNullSafe(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	cmpFn func(int, bool) bool,
	selectList *FunctionSelectList,
) error {
	if usesApproximateNumericCompare(parameters[0], parameters[1]) {
		return numericCompareWithTypeMismatchNullSafeApprox(parameters, result, proc, length, cmpFn, selectList)
	}

	rs := vector.MustFunctionResult[bool](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedColNoTypeCheck[bool](rsVec)
	rsVec.GetNulls().Reset()

	v1Cmp := getAsCompareValueSlice(parameters[0])
	v2Cmp := getAsCompareValueSlice(parameters[1])

	c1, c2 := parameters[0].IsConst(), parameters[1].IsConst()

	if c1 && c2 {
		null1 := parameters[0].IsConstNull()
		null2 := parameters[1].IsConstNull()
		var r bool
		if null1 || null2 {
			r = null1 && null2
		} else {
			cmp, comparable := compareNumericValues(v1Cmp[0], v2Cmp[0])
			r = cmpFn(cmp, comparable)
		}
		for i := 0; i < length; i++ {
			rss[i] = r
		}
		return nil
	}

	if c1 {
		if parameters[0].IsConstNull() {
			nsp2 := parameters[1].GetNulls()
			for i := 0; i < length; i++ {
				rss[i] = nsp2.Contains(uint64(i))
			}
			return nil
		}
		val1 := v1Cmp[0]
		nsp2 := parameters[1].GetNulls()
		for i := 0; i < length; i++ {
			if nsp2.Contains(uint64(i)) {
				rss[i] = false
			} else {
				cmp, comparable := compareNumericValues(val1, v2Cmp[i])
				rss[i] = cmpFn(cmp, comparable)
			}
		}
		return nil
	}

	if c2 {
		if parameters[1].IsConstNull() {
			nsp1 := parameters[0].GetNulls()
			for i := 0; i < length; i++ {
				rss[i] = nsp1.Contains(uint64(i))
			}
			return nil
		}
		val2 := v2Cmp[0]
		nsp1 := parameters[0].GetNulls()
		for i := 0; i < length; i++ {
			if nsp1.Contains(uint64(i)) {
				rss[i] = false
			} else {
				cmp, comparable := compareNumericValues(v1Cmp[i], val2)
				rss[i] = cmpFn(cmp, comparable)
			}
		}
		return nil
	}

	nsp1 := parameters[0].GetNulls()
	nsp2 := parameters[1].GetNulls()
	for i := 0; i < length; i++ {
		null1 := nsp1.Contains(uint64(i))
		null2 := nsp2.Contains(uint64(i))
		if null1 || null2 {
			rss[i] = null1 && null2
		} else {
			cmp, comparable := compareNumericValues(v1Cmp[i], v2Cmp[i])
			rss[i] = cmpFn(cmp, comparable)
		}
	}
	return nil
}

func numericCompareWithTypeMismatchApprox(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	cmpFn func(int, bool) bool,
	selectList *FunctionSelectList,
) error {
	rs := vector.MustFunctionResult[bool](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedColNoTypeCheck[bool](rsVec)

	v1Float64 := getAsFloat64Slice(parameters[0])
	v2Float64 := getAsFloat64Slice(parameters[1])

	c1, c2 := parameters[0].IsConst(), parameters[1].IsConst()

	if c1 && c2 {
		null1 := parameters[0].IsConstNull()
		null2 := parameters[1].IsConstNull()
		if null1 || null2 {
			rsVec.GetNulls().AddRange(0, uint64(length))
		} else {
			cmp, comparable := compareApproximateNumericValues(v1Float64[0], v2Float64[0])
			r := cmpFn(cmp, comparable)
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
				cmp, comparable := compareApproximateNumericValues(val1, v2Float64[i])
				rss[i] = cmpFn(cmp, comparable)
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
				cmp, comparable := compareApproximateNumericValues(v1Float64[i], val2)
				rss[i] = cmpFn(cmp, comparable)
			}
		}
		return nil
	}

	nsp1 := parameters[0].GetNulls()
	nsp2 := parameters[1].GetNulls()
	for i := 0; i < length; i++ {
		if nsp1.Contains(uint64(i)) || nsp2.Contains(uint64(i)) {
			rsVec.GetNulls().Add(uint64(i))
		} else {
			cmp, comparable := compareApproximateNumericValues(v1Float64[i], v2Float64[i])
			rss[i] = cmpFn(cmp, comparable)
		}
	}
	return nil
}

func numericCompareWithTypeMismatchNullSafeApprox(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	cmpFn func(int, bool) bool,
	selectList *FunctionSelectList,
) error {
	rs := vector.MustFunctionResult[bool](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedColNoTypeCheck[bool](rsVec)
	rsVec.GetNulls().Reset()

	v1Float64 := getAsFloat64Slice(parameters[0])
	v2Float64 := getAsFloat64Slice(parameters[1])

	c1, c2 := parameters[0].IsConst(), parameters[1].IsConst()

	if c1 && c2 {
		null1 := parameters[0].IsConstNull()
		null2 := parameters[1].IsConstNull()
		var r bool
		if null1 || null2 {
			r = null1 && null2
		} else {
			cmp, comparable := compareApproximateNumericValues(v1Float64[0], v2Float64[0])
			r = cmpFn(cmp, comparable)
		}
		for i := 0; i < length; i++ {
			rss[i] = r
		}
		return nil
	}

	if c1 {
		if parameters[0].IsConstNull() {
			nsp2 := parameters[1].GetNulls()
			for i := 0; i < length; i++ {
				rss[i] = nsp2.Contains(uint64(i))
			}
			return nil
		}
		val1 := v1Float64[0]
		nsp2 := parameters[1].GetNulls()
		for i := 0; i < length; i++ {
			if nsp2.Contains(uint64(i)) {
				rss[i] = false
			} else {
				cmp, comparable := compareApproximateNumericValues(val1, v2Float64[i])
				rss[i] = cmpFn(cmp, comparable)
			}
		}
		return nil
	}

	if c2 {
		if parameters[1].IsConstNull() {
			nsp1 := parameters[0].GetNulls()
			for i := 0; i < length; i++ {
				rss[i] = nsp1.Contains(uint64(i))
			}
			return nil
		}
		val2 := v2Float64[0]
		nsp1 := parameters[0].GetNulls()
		for i := 0; i < length; i++ {
			if nsp1.Contains(uint64(i)) {
				rss[i] = false
			} else {
				cmp, comparable := compareApproximateNumericValues(v1Float64[i], val2)
				rss[i] = cmpFn(cmp, comparable)
			}
		}
		return nil
	}

	nsp1 := parameters[0].GetNulls()
	nsp2 := parameters[1].GetNulls()
	for i := 0; i < length; i++ {
		null1 := nsp1.Contains(uint64(i))
		null2 := nsp2.Contains(uint64(i))
		if null1 || null2 {
			rss[i] = null1 && null2
		} else {
			cmp, comparable := compareApproximateNumericValues(v1Float64[i], v2Float64[i])
			rss[i] = cmpFn(cmp, comparable)
		}
	}
	return nil
}

func compareNumericValues(left, right numericCompareValue) (int, bool) {
	if left.kind == numericCompareNaN || right.kind == numericCompareNaN {
		return 0, false
	}
	if left.kind == numericComparePosInf {
		if right.kind == numericComparePosInf {
			return 0, true
		}
		return 1, true
	}
	if left.kind == numericCompareNegInf {
		if right.kind == numericCompareNegInf {
			return 0, true
		}
		return -1, true
	}
	if right.kind == numericComparePosInf {
		return -1, true
	}
	if right.kind == numericCompareNegInf {
		return 1, true
	}
	return left.rat.Cmp(right.rat), true
}

func compareApproximateNumericValues(left, right float64) (int, bool) {
	if math.IsNaN(left) || math.IsNaN(right) {
		return 0, false
	}
	switch {
	case left < right:
		return -1, true
	case left > right:
		return 1, true
	default:
		return 0, true
	}
}

func getAsCompareValueSlice(v *vector.Vector) []numericCompareValue {
	t := v.GetType()
	switch t.Oid {
	case types.T_int8:
		cols := vector.MustFixedColNoTypeCheck[int8](v)
		result := make([]numericCompareValue, len(cols))
		for i, val := range cols {
			result[i] = numericCompareValue{kind: numericCompareFinite, rat: new(big.Rat).SetInt64(int64(val))}
		}
		return result
	case types.T_int16:
		cols := vector.MustFixedColNoTypeCheck[int16](v)
		result := make([]numericCompareValue, len(cols))
		for i, val := range cols {
			result[i] = numericCompareValue{kind: numericCompareFinite, rat: new(big.Rat).SetInt64(int64(val))}
		}
		return result
	case types.T_int32:
		cols := vector.MustFixedColNoTypeCheck[int32](v)
		result := make([]numericCompareValue, len(cols))
		for i, val := range cols {
			result[i] = numericCompareValue{kind: numericCompareFinite, rat: new(big.Rat).SetInt64(int64(val))}
		}
		return result
	case types.T_int64:
		cols := vector.MustFixedColNoTypeCheck[int64](v)
		result := make([]numericCompareValue, len(cols))
		for i, val := range cols {
			result[i] = numericCompareValue{kind: numericCompareFinite, rat: new(big.Rat).SetInt64(val)}
		}
		return result
	case types.T_uint8:
		cols := vector.MustFixedColNoTypeCheck[uint8](v)
		result := make([]numericCompareValue, len(cols))
		for i, val := range cols {
			result[i] = numericCompareValue{kind: numericCompareFinite, rat: new(big.Rat).SetInt64(int64(val))}
		}
		return result
	case types.T_uint16:
		cols := vector.MustFixedColNoTypeCheck[uint16](v)
		result := make([]numericCompareValue, len(cols))
		for i, val := range cols {
			result[i] = numericCompareValue{kind: numericCompareFinite, rat: new(big.Rat).SetInt64(int64(val))}
		}
		return result
	case types.T_uint32:
		cols := vector.MustFixedColNoTypeCheck[uint32](v)
		result := make([]numericCompareValue, len(cols))
		for i, val := range cols {
			result[i] = numericCompareValue{kind: numericCompareFinite, rat: new(big.Rat).SetInt64(int64(val))}
		}
		return result
	case types.T_uint64:
		cols := vector.MustFixedColNoTypeCheck[uint64](v)
		result := make([]numericCompareValue, len(cols))
		for i, val := range cols {
			bi := new(big.Int).SetUint64(val)
			result[i] = numericCompareValue{kind: numericCompareFinite, rat: new(big.Rat).SetInt(bi)}
		}
		return result
	case types.T_float32:
		cols := vector.MustFixedColNoTypeCheck[float32](v)
		result := make([]numericCompareValue, len(cols))
		for i, val := range cols {
			f := float64(val)
			if math.IsNaN(f) {
				result[i] = numericCompareValue{kind: numericCompareNaN}
				continue
			}
			if math.IsInf(f, 1) {
				result[i] = numericCompareValue{kind: numericComparePosInf}
				continue
			}
			if math.IsInf(f, -1) {
				result[i] = numericCompareValue{kind: numericCompareNegInf}
				continue
			}
			r := new(big.Rat).SetFloat64(float64(val))
			if r == nil {
				panic("invalid float32 value for numeric comparison")
			}
			result[i] = numericCompareValue{kind: numericCompareFinite, rat: r}
		}
		return result
	case types.T_float64:
		cols := vector.MustFixedColNoTypeCheck[float64](v)
		result := make([]numericCompareValue, len(cols))
		for i, val := range cols {
			if math.IsNaN(val) {
				result[i] = numericCompareValue{kind: numericCompareNaN}
				continue
			}
			if math.IsInf(val, 1) {
				result[i] = numericCompareValue{kind: numericComparePosInf}
				continue
			}
			if math.IsInf(val, -1) {
				result[i] = numericCompareValue{kind: numericCompareNegInf}
				continue
			}
			r := new(big.Rat).SetFloat64(val)
			if r == nil {
				panic("invalid float64 value for numeric comparison")
			}
			result[i] = numericCompareValue{kind: numericCompareFinite, rat: r}
		}
		return result
	case types.T_decimal64:
		cols := vector.MustFixedColNoTypeCheck[types.Decimal64](v)
		result := make([]numericCompareValue, len(cols))
		for i, val := range cols {
			r, ok := new(big.Rat).SetString(val.Format(t.Scale))
			if !ok {
				panic("invalid decimal64 value for numeric comparison")
			}
			result[i] = numericCompareValue{kind: numericCompareFinite, rat: r}
		}
		return result
	case types.T_decimal128:
		cols := vector.MustFixedColNoTypeCheck[types.Decimal128](v)
		result := make([]numericCompareValue, len(cols))
		for i, val := range cols {
			r, ok := new(big.Rat).SetString(val.Format(t.Scale))
			if !ok {
				panic("invalid decimal128 value for numeric comparison")
			}
			result[i] = numericCompareValue{kind: numericCompareFinite, rat: r}
		}
		return result
	default:
		return nil
	}
}

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
	case types.T_decimal64:
		cols := vector.MustFixedColNoTypeCheck[types.Decimal64](v)
		result := make([]float64, len(cols))
		for i, val := range cols {
			result[i] = types.Decimal64ToFloat64(val, t.Scale)
		}
		return result
	case types.T_decimal128:
		cols := vector.MustFixedColNoTypeCheck[types.Decimal128](v)
		result := make([]float64, len(cols))
		for i, val := range cols {
			result[i] = types.Decimal128ToFloat64(val, t.Scale)
		}
		return result
	default:
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
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128:
		return true
	default:
		return false
	}
}

func usesApproximateNumericCompare(v1, v2 *vector.Vector) bool {
	return isFloatType(v1.GetType().Oid) || isFloatType(v2.GetType().Oid)
}

func isFloatType(t types.T) bool {
	return t == types.T_float32 || t == types.T_float64
}
