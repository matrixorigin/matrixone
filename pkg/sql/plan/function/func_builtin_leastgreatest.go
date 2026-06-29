// Copyright 2021 - 2025 Matrix Origin
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
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// check input types for least and greatest function.
// It requires at least 1 input. NULL arguments (T_any) are allowed and produce
// NULL results per MySQL behavior.
//
// When every non-NULL argument already shares the same type, the call succeeds
// directly. When the non-NULL arguments have different numeric types, they are
// promoted to a common numeric type and compared on that type, matching MySQL's
// implicit numeric promotion (issue #25145). Mixing non-numeric types that do
// not already match is still rejected.
func leastGreatestCheck(_ []overload, inputs []types.Type) checkResult {
	if len(inputs) < 1 {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}

	// Collect the non-NULL (non-T_any) argument types. A NULL literal is typed
	// T_any and always evaluates to NULL regardless of the resolved type.
	nonNull := make([]types.Type, 0, len(inputs))
	for i := range inputs {
		if inputs[i].Oid != types.T_any {
			nonNull = append(nonNull, inputs[i])
		}
	}
	// All arguments are NULL literals: nothing to compare, evaluate as varchar.
	if len(nonNull) == 0 {
		return newCheckResultWithSuccess(0)
	}

	// Fast path: every non-NULL argument already shares the same type Oid. This
	// preserves the original behavior (including any per-type scale handling) for
	// the common case where no promotion is needed.
	baseOid := nonNull[0].Oid
	sameOid := true
	for i := 1; i < len(nonNull); i++ {
		if nonNull[i].Oid != baseOid {
			sameOid = false
			break
		}
	}
	if sameOid {
		return newCheckResultWithSuccess(0)
	}

	// Mixed argument types. MySQL promotes numeric arguments to a common type
	// and compares on that. If the arguments are not all numeric we cannot
	// promote them, so reject the call as before.
	target, ok := leastGreatestCommonNumericType(nonNull)
	if !ok {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}
	castType := make([]types.Type, len(inputs))
	for i := range castType {
		castType[i] = target
	}
	return newCheckResultWithCast(0, castType)
}

// leastGreatestCommonNumericType derives the common type used to compare a set
// of mixed numeric arguments to LEAST/GREATEST. It follows MySQL's promotion
// rules: any floating-point operand makes the result DOUBLE; otherwise any
// DECIMAL operand makes the result a DECIMAL wide enough to hold every operand;
// otherwise the operands are integers and the narrowest integer type that holds
// them all is chosen. It returns (_, false) if any argument is not numeric.
func leastGreatestCommonNumericType(inputs []types.Type) (types.Type, bool) {
	hasFloat, hasDecimal, hasSigned, hasUnsigned := false, false, false, false
	for i := range inputs {
		t := inputs[i]
		switch {
		case t.IsFloat():
			hasFloat = true
		case t.IsDecimal():
			hasDecimal = true
		case t.IsInt():
			hasSigned = true
		case t.IsUInt(), t.Oid == types.T_bit:
			hasUnsigned = true
		default:
			// non-numeric argument: cannot promote
			return types.Type{}, false
		}
	}

	// 1. Any floating-point argument: compare as DOUBLE.
	if hasFloat {
		return types.T_float64.ToType(), true
	}

	// 2. Any DECIMAL argument: widen to a decimal that holds every decimal and
	//    integer operand without losing integral digits or scale, promoting to
	//    DECIMAL128/256 as needed. Fall back to DOUBLE if the required precision
	//    exceeds DECIMAL256.
	if hasDecimal {
		target := types.T_decimal64.ToType()
		for i := range inputs {
			if inputs[i].Oid == types.T_decimal256 {
				target.Oid = types.T_decimal256
				break
			}
			if inputs[i].Oid == types.T_decimal128 {
				target.Oid = types.T_decimal128
			}
		}
		target.Size = int32(target.Oid.TypeLen())
		if !setSafeDecimalWidthAndScaleFromSource(&target, inputs) {
			return types.T_float64.ToType(), true
		}
		return target, true
	}

	// 3. Integer-only operands (including BIT). Choose the narrowest integer type
	//    that holds every operand. Mixing signed and unsigned operands that do
	//    not fit a signed 64-bit integer widens to DECIMAL128 to stay lossless.
	maxWidth := int32(0)
	for i := range inputs {
		w := integerIntegralWidth(inputs[i].Oid)
		if inputs[i].Oid == types.T_bit {
			w = 20 // BIT holds up to a 64-bit unsigned value
		}
		if w > maxWidth {
			maxWidth = w
		}
	}
	switch {
	case hasSigned && hasUnsigned:
		// uint32 (10 digits) and narrower fit losslessly in int64 (19 digits).
		if maxWidth <= 10 {
			return types.T_int64.ToType(), true
		}
		// A uint64/BIT operand alongside a signed operand cannot fit any signed
		// integer; use DECIMAL128, which holds the full unsigned 64-bit range.
		dt := types.T_decimal128.ToType()
		dt.Scale = 0
		dt.Width = 20
		dt.Size = int32(types.T_decimal128.TypeLen())
		return dt, true
	case hasUnsigned:
		return unsignedTypeForWidth(maxWidth), true
	default:
		return signedTypeForWidth(maxWidth), true
	}
}

// signedTypeForWidth returns the narrowest signed integer type whose integral
// width covers w decimal digits.
func signedTypeForWidth(w int32) types.Type {
	switch {
	case w <= 3:
		return types.T_int8.ToType()
	case w <= 5:
		return types.T_int16.ToType()
	case w <= 10:
		return types.T_int32.ToType()
	default:
		return types.T_int64.ToType()
	}
}

// unsignedTypeForWidth returns the narrowest unsigned integer type whose
// integral width covers w decimal digits.
func unsignedTypeForWidth(w int32) types.Type {
	switch {
	case w <= 3:
		return types.T_uint8.ToType()
	case w <= 5:
		return types.T_uint16.ToType()
	case w <= 10:
		return types.T_uint32.ToType()
	default:
		return types.T_uint64.ToType()
	}
}

func leastGreatestFnFixed[T types.FixedSizeTExceptStrType](
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
	compareFn func(v1, v2 T) bool) error {
	rs := vector.MustFunctionResult[T](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedColWithTypeCheck[T](rsVec)
	rsNull := rsVec.GetNulls()
	rsAnyNull := false

	if selectList != nil {
		if selectList.IgnoreAllRow() {
			nulls.AddRange(rsNull, 0, uint64(length))
			return nil
		}
		if !selectList.ShouldEvalAllRow() {
			rsAnyNull = true
			for i := range selectList.SelectList {
				if selectList.Contains(uint64(i)) {
					rsNull.Add(uint64(i))
				}
			}
		}
	}

	for np, pv := range parameters {
		p := vector.GenerateFunctionFixedTypeParameter[T](pv)
		if pv.IsConstNull() {
			nulls.AddRange(rsNull, 0, uint64(length))
			return nil
		}
		if p.WithAnyNullValue() || rsAnyNull {
			nulls.Or(rsNull, pv.GetNulls(), rsNull)
		}

		for i := 0; i < length; i++ {
			if rsNull.Contains(uint64(i)) {
				continue
			}

			v, _ := p.GetValue(uint64(i))
			if np == 0 {
				rss[i] = v
			} else {
				if compareFn(v, rss[i]) {
					rss[i] = v
				}
			}
		}
	}
	return nil
}

func leastGreatestFnVarlen(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
	compareFn func(v1, v2 []byte) bool) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	rsVec := rs.GetResultVector()
	rsNull := rsVec.GetNulls()

	if selectList != nil {
		if selectList.IgnoreAllRow() {
			nulls.AddRange(rsNull, 0, uint64(length))
			return nil
		}
		if !selectList.ShouldEvalAllRow() {
			for i := range selectList.SelectList {
				if selectList.Contains(uint64(i)) {
					rsNull.Add(uint64(i))
				}
			}
		}
	}

	for _, pv := range parameters {
		if pv.IsConstNull() {
			nulls.AddRange(rsNull, 0, uint64(length))
			return nil
		}
	}

	for i := uint64(0); i < uint64(length); i++ {
		var v []byte
		var isNull bool

		if selectList != nil && selectList.ShouldEvalAllRow() {
			if selectList.Contains(i) {
				rs.AppendBytes(nil, true)
				continue
			}
		}

		for _, pv := range parameters {
			if pv.IsNull(i) {
				isNull = true
				break
			} else {
				vv := pv.GetBytesAt(int(i))
				if v == nil {
					v = vv
				} else {
					if compareFn(vv, v) {
						v = vv
					}
				}
			}
		}
		rs.AppendBytes(v, isNull)
	}

	return nil
}

// leastGreatestParamType finds the first non-T_any parameter type.
// If all parameters are T_any (all NULL constants), returns T_varchar.
func leastGreatestParamType(parameters []*vector.Vector) types.Type {
	for _, p := range parameters {
		if p.GetType().Oid != types.T_any {
			return *p.GetType()
		}
	}
	return types.T_varchar.ToType()
}

func leastFn(parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList) error {
	paramType := leastGreatestParamType(parameters)
	switch paramType.Oid {
	case types.T_bool:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 bool) bool {
				return !v1 && v2
			})

	case types.T_bit:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 uint64) bool {
				return v1 < v2
			})

	case types.T_int8:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 int8) bool {
				return v1 < v2
			})
	case types.T_int16:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 int16) bool {
				return v1 < v2
			})
	case types.T_int32:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 int32) bool {
				return v1 < v2
			})
	case types.T_int64:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 int64) bool {
				return v1 < v2
			})
	case types.T_uint8:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 uint8) bool {
				return v1 < v2
			})
	case types.T_uint16:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 uint16) bool {
				return v1 < v2
			})
	case types.T_uint32:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 uint32) bool {
				return v1 < v2
			})
	case types.T_uint64:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 uint64) bool {
				return v1 < v2
			})

	case types.T_uuid:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Uuid) bool {
				return types.CompareUuid(v1, v2) < 0
			})

	case types.T_float32:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 float32) bool {
				return v1 < v2
			})

	case types.T_float64:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 float64) bool {
				return v1 < v2
			})

	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_datalink:
		return leastGreatestFnVarlen(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 []byte) bool {
				return bytes.Compare(v1, v2) < 0
			})

	case types.T_binary, types.T_varbinary:
		return leastGreatestFnVarlen(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 []byte) bool {
				return bytes.Compare(v1, v2) < 0
			})

	case types.T_array_float32:
		return leastGreatestFnVarlen(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 []byte) bool {
				_v1 := types.BytesToArray[float32](v1)
				_v2 := types.BytesToArray[float32](v2)

				return types.ArrayCompare[float32](_v1, _v2) < 0
			})

	case types.T_array_float64:
		return leastGreatestFnVarlen(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 []byte) bool {
				_v1 := types.BytesToArray[float64](v1)
				_v2 := types.BytesToArray[float64](v2)
				return types.ArrayCompare[float64](_v1, _v2) < 0
			})

	case types.T_date:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Date) bool {
				return v1 < v2
			})

	case types.T_datetime:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Datetime) bool {
				return v1 < v2
			})

	case types.T_time:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Time) bool {
				return v1 < v2
			})

	case types.T_timestamp:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Timestamp) bool {
				return v1 < v2
			})

	case types.T_year:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.MoYear) bool {
				return v1 < v2
			})

	case types.T_decimal64:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Decimal64) bool {
				return v1.Compare(v2) < 0
			})

	case types.T_decimal128:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Decimal128) bool {
				return v1.Compare(v2) < 0
			})

	case types.T_decimal256:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Decimal256) bool {
				return v1.Compare(v2) < 0
			})

	case types.T_Rowid:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Rowid) bool {
				return v1.LT(&v2)
			})
	}
	panic("unreached code")
}

func greatestFn(parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList) error {
	paramType := leastGreatestParamType(parameters)
	switch paramType.Oid {
	case types.T_bool:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 bool) bool {
				return v1 && !v2
			})

	case types.T_bit:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 uint64) bool {
				return v1 > v2
			})

	case types.T_int8:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 int8) bool {
				return v1 > v2
			})
	case types.T_int16:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 int16) bool {
				return v1 > v2
			})
	case types.T_int32:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 int32) bool {
				return v1 > v2
			})
	case types.T_int64:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 int64) bool {
				return v1 > v2
			})
	case types.T_uint8:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 uint8) bool {
				return v1 > v2
			})
	case types.T_uint16:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 uint16) bool {
				return v1 > v2
			})
	case types.T_uint32:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 uint32) bool {
				return v1 > v2
			})
	case types.T_uint64:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 uint64) bool {
				return v1 > v2
			})

	case types.T_uuid:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Uuid) bool {
				return types.CompareUuid(v1, v2) > 0
			})

	case types.T_float32:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 float32) bool {
				return v1 > v2
			})

	case types.T_float64:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 float64) bool {
				return v1 > v2
			})

	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_datalink:
		return leastGreatestFnVarlen(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 []byte) bool {
				return bytes.Compare(v1, v2) > 0
			})

	case types.T_binary, types.T_varbinary:
		return leastGreatestFnVarlen(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 []byte) bool {
				return bytes.Compare(v1, v2) > 0
			})

	case types.T_array_float32:
		return leastGreatestFnVarlen(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 []byte) bool {
				_v1 := types.BytesToArray[float32](v1)
				_v2 := types.BytesToArray[float32](v2)
				return types.ArrayCompare[float32](_v1, _v2) > 0
			})

	case types.T_array_float64:
		return leastGreatestFnVarlen(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 []byte) bool {
				_v1 := types.BytesToArray[float64](v1)
				_v2 := types.BytesToArray[float64](v2)
				return types.ArrayCompare[float64](_v1, _v2) > 0
			})

	case types.T_date:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Date) bool {
				return v1 > v2
			})

	case types.T_datetime:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Datetime) bool {
				return v1 > v2
			})

	case types.T_time:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Time) bool {
				return v1 > v2
			})

	case types.T_timestamp:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Timestamp) bool {
				return v1 > v2
			})

	case types.T_year:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.MoYear) bool {
				return v1 > v2
			})

	case types.T_decimal64:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Decimal64) bool {
				return v1.Compare(v2) > 0
			})

	case types.T_decimal128:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Decimal128) bool {
				return v1.Compare(v2) > 0
			})

	case types.T_decimal256:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Decimal256) bool {
				return v1.Compare(v2) > 0
			})

	case types.T_Rowid:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Rowid) bool {
				return v1.GT(&v2)
			})
	}
	panic("unreached code")
}
