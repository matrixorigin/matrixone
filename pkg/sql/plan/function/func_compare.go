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
	"bytes"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

func otherCompareOperatorSupports(typ1, typ2 types.Type) bool {
	if isDatetimeTimestampComparison(typ1, typ2) {
		return true
	}
	if typ1.Oid != typ2.Oid {
		return false
	}
	switch typ1.Oid {
	case types.T_bool:
	case types.T_bit:
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
	case types.T_float32, types.T_float64:
	case types.T_decimal64, types.T_decimal128, types.T_decimal256:
	case types.T_char, types.T_varchar:
	case types.T_date, types.T_datetime:
	case types.T_timestamp, types.T_time:
	case types.T_blob, types.T_text, types.T_datalink:
	case types.T_binary, types.T_varbinary:
	case types.T_json:
	case types.T_uuid:
	case types.T_Rowid:
	case types.T_array_float32, types.T_array_float64:
	case types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8:
	case types.T_year:
	default:
		return false
	}
	return true
}

// jsonOrderingWithStringNotSupported returns true when one operand is JSON and the other
// is a MySQL string type, indicating that ordering comparisons (>, <, >=, <=) are not
// supported for this combination. Only equality/inequality (= and !=) are allowed.
func jsonOrderingWithStringNotSupported(inputs []types.Type) bool {
	if len(inputs) != 2 {
		return false
	}
	return (inputs[0].Oid == types.T_json) != (inputs[1].Oid == types.T_json) &&
		(inputs[0].Oid.IsMySQLString() || inputs[1].Oid.IsMySQLString())
}

func equalAndNotEqualOperatorSupports(typ1, typ2 types.Type) bool {
	if isDatetimeTimestampComparison(typ1, typ2) {
		return true
	}
	if typ1.Oid != typ2.Oid {
		return false
	}
	switch typ1.Oid {
	case types.T_bool:
	case types.T_bit:
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
	case types.T_float32, types.T_float64:
	case types.T_decimal64, types.T_decimal128, types.T_decimal256:
	case types.T_char, types.T_varchar:
	case types.T_date, types.T_datetime:
	case types.T_timestamp, types.T_time:
	case types.T_blob, types.T_text, types.T_datalink:
	case types.T_binary, types.T_varbinary:
	case types.T_json:
	case types.T_uuid:
	case types.T_Rowid:
	case types.T_array_float32, types.T_array_float64:
	case types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8:
	case types.T_enum:
	case types.T_year:
	default:
		return false
	}
	return true
}

func compareDatetimeAndTimestamp(
	parameters []*vector.Vector,
	result *vector.FunctionResult[bool],
	proc *process.Process,
	length int,
	cmp func(left, right types.Timestamp) bool,
	selectList *FunctionSelectList,
) error {
	zone := proc.GetSessionInfo().TimeZone
	if parameters[0].GetType().Oid == types.T_datetime {
		timestampScale := parameters[1].GetType().Scale
		return opBinaryFixedFixedToFixed[types.Datetime, types.Timestamp, bool](
			parameters, result, proc, length,
			func(left types.Datetime, right types.Timestamp) bool {
				return cmp(left.ToTimestamp(zone).TruncateToScale(timestampScale), right)
			}, selectList)
	}
	timestampScale := parameters[0].GetType().Scale
	return opBinaryFixedFixedToFixed[types.Timestamp, types.Datetime, bool](
		parameters, result, proc, length,
		func(left types.Timestamp, right types.Datetime) bool {
			return cmp(left, right.ToTimestamp(zone).TruncateToScale(timestampScale))
		}, selectList)
}

func opBinaryFixedFixedToFixedNullSafe[T types.FixedSizeTExceptStrType](
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	_ *process.Process,
	length int,
	cmpFn func(v1, v2 T) bool,
	selectList *FunctionSelectList,
) error {
	result.UseOptFunctionParamFrame(2)
	rs := vector.MustFunctionResult[bool](result)
	p1 := vector.OptGetParamFromWrapper[T](rs, 0, parameters[0])
	p2 := vector.OptGetParamFromWrapper[T](rs, 1, parameters[1])
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedColNoTypeCheck[bool](rsVec)

	// Result of <=> is never NULL
	rsVec.GetNulls().Reset()

	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)

		if null1 && null2 {
			rss[i] = true
		} else if null1 || null2 {
			rss[i] = false
		} else {
			rss[i] = cmpFn(v1, v2)
		}
	}
	return nil
}

func opBinaryBytesBytesToFixedNullSafe(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	_ *process.Process,
	length int,
	cmpFn func(v1, v2 []byte) bool,
	selectList *FunctionSelectList,
) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])
	rs := vector.MustFunctionResult[bool](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedColNoTypeCheck[bool](rsVec)

	// Result of <=> is never NULL
	rsVec.GetNulls().Reset()

	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		v2, null2 := p2.GetStrValue(i)

		if null1 && null2 {
			rss[i] = true
		} else if null1 || null2 {
			rss[i] = false
		} else {
			rss[i] = cmpFn(v1, v2)
		}
	}
	return nil
}

func compareJsonBytes(left, right []byte) int {
	return bytejson.CompareByteJson(types.DecodeJson(left), types.DecodeJson(right))
}

func float32ComparisonNormalizers(leftScale, rightScale int32) (
	left types.Float32ScaleNormalizer,
	right types.Float32ScaleNormalizer,
	normalize bool,
) {
	left = types.NewFloat32ScaleNormalizer(leftScale)
	if rightScale == leftScale {
		right = left
	} else {
		right = types.NewFloat32ScaleNormalizer(rightScale)
	}
	return left, right, leftScale > 0 || rightScale > 0
}

func nullSafeEqualFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[bool](result)

	switch paramType.Oid {
	case types.T_bool:
		return opBinaryFixedFixedToFixedNullSafe[bool](parameters, rs, proc, length, func(a, b bool) bool {
			return a == b
		}, selectList)
	case types.T_bit:
		return opBinaryFixedFixedToFixedNullSafe[uint64](parameters, rs, proc, length, func(a, b uint64) bool {
			return a == b
		}, selectList)
	case types.T_int8:
		return opBinaryFixedFixedToFixedNullSafe[int8](parameters, rs, proc, length, func(a, b int8) bool {
			return a == b
		}, selectList)
	case types.T_int16:
		return opBinaryFixedFixedToFixedNullSafe[int16](parameters, rs, proc, length, func(a, b int16) bool {
			return a == b
		}, selectList)
	case types.T_int32:
		return opBinaryFixedFixedToFixedNullSafe[int32](parameters, rs, proc, length, func(a, b int32) bool {
			return a == b
		}, selectList)
	case types.T_int64:
		return opBinaryFixedFixedToFixedNullSafe[int64](parameters, rs, proc, length, func(a, b int64) bool {
			return a == b
		}, selectList)
	case types.T_uint8:
		return opBinaryFixedFixedToFixedNullSafe[uint8](parameters, rs, proc, length, func(a, b uint8) bool {
			return a == b
		}, selectList)
	case types.T_uint16:
		return opBinaryFixedFixedToFixedNullSafe[uint16](parameters, rs, proc, length, func(a, b uint16) bool {
			return a == b
		}, selectList)
	case types.T_uint32:
		return opBinaryFixedFixedToFixedNullSafe[uint32](parameters, rs, proc, length, func(a, b uint32) bool {
			return a == b
		}, selectList)
	case types.T_uint64:
		return opBinaryFixedFixedToFixedNullSafe[uint64](parameters, rs, proc, length, func(a, b uint64) bool {
			return a == b
		}, selectList)
	case types.T_uuid:
		return opBinaryFixedFixedToFixedNullSafe[types.Uuid](parameters, rs, proc, length, func(a, b types.Uuid) bool {
			return a == b
		}, selectList)
	case types.T_float32:
		leftNormalizer, rightNormalizer, normalize := float32ComparisonNormalizers(
			paramType.Scale,
			parameters[1].GetType().Scale,
		)
		if normalize {
			return opBinaryFixedFixedToFixedNullSafe[float32](parameters, rs, proc, length, func(a, b float32) bool {
				a = leftNormalizer.Normalize(a)
				b = rightNormalizer.Normalize(b)
				return a == b
			}, selectList)
		}
		return opBinaryFixedFixedToFixedNullSafe[float32](parameters, rs, proc, length, func(a, b float32) bool {
			return a == b
		}, selectList)
	case types.T_float64:
		return opBinaryFixedFixedToFixedNullSafe[float64](parameters, rs, proc, length, func(a, b float64) bool {
			return a == b
		}, selectList)
	case types.T_json:
		return opBinaryBytesBytesToFixedNullSafe(parameters, rs, proc, length, func(a, b []byte) bool {
			return compareJsonBytes(a, b) == 0
		}, selectList)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_binary, types.T_varbinary, types.T_datalink:
		return opBinaryBytesBytesToFixedNullSafe(parameters, rs, proc, length, func(a, b []byte) bool {
			return bytes.Equal(a, b)
		}, selectList)
	case types.T_array_float32:
		return opBinaryBytesBytesToFixedNullSafe(parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float32](v1)
			_v2 := types.BytesToArray[float32](v2)
			return types.ArrayCompare[float32](_v1, _v2) == 0
		}, selectList)
	case types.T_array_float64:
		return opBinaryBytesBytesToFixedNullSafe(parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float64](v1)
			_v2 := types.BytesToArray[float64](v2)
			return types.ArrayCompare[float64](_v1, _v2) == 0
		}, selectList)
	case types.T_array_bf16:
		return opBinaryBytesBytesToFixedNullSafe(parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[types.BF16](v1)
			_v2 := types.BytesToArray[types.BF16](v2)
			return types.ArrayElementCompare[types.BF16](_v1, _v2) == 0
		}, selectList)
	case types.T_array_float16:
		return opBinaryBytesBytesToFixedNullSafe(parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[types.Float16](v1)
			_v2 := types.BytesToArray[types.Float16](v2)
			return types.ArrayElementCompare[types.Float16](_v1, _v2) == 0
		}, selectList)
	case types.T_array_int8:
		return opBinaryBytesBytesToFixedNullSafe(parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[int8](v1)
			_v2 := types.BytesToArray[int8](v2)
			return types.ArrayElementCompare[int8](_v1, _v2) == 0
		}, selectList)
	case types.T_array_uint8:
		return opBinaryBytesBytesToFixedNullSafe(parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[uint8](v1)
			_v2 := types.BytesToArray[uint8](v2)
			return types.ArrayElementCompare[uint8](_v1, _v2) == 0
		}, selectList)
	case types.T_date:
		return opBinaryFixedFixedToFixedNullSafe[types.Date](parameters, rs, proc, length, func(a, b types.Date) bool {
			return a == b
		}, selectList)
	case types.T_datetime:
		return opBinaryFixedFixedToFixedNullSafe[types.Datetime](parameters, rs, proc, length, func(a, b types.Datetime) bool {
			return a == b
		}, selectList)
	case types.T_time:
		return opBinaryFixedFixedToFixedNullSafe[types.Time](parameters, rs, proc, length, func(a, b types.Time) bool {
			return a == b
		}, selectList)
	case types.T_timestamp:
		return opBinaryFixedFixedToFixedNullSafe[types.Timestamp](parameters, rs, proc, length, func(a, b types.Timestamp) bool {
			return a == b
		}, selectList)
	case types.T_decimal64:
		return opBinaryFixedFixedToFixedNullSafe[types.Decimal64](parameters, rs, proc, length, func(a, b types.Decimal64) bool {
			return a == b
		}, selectList)
	case types.T_decimal128:
		return opBinaryFixedFixedToFixedNullSafe[types.Decimal128](parameters, rs, proc, length, func(a, b types.Decimal128) bool {
			return a == b
		}, selectList)
	case types.T_decimal256:
		return opBinaryFixedFixedToFixedNullSafe[types.Decimal256](parameters, rs, proc, length, func(a, b types.Decimal256) bool {
			return a == b
		}, selectList)
	case types.T_Rowid:
		return opBinaryFixedFixedToFixedNullSafe[types.Rowid](parameters, rs, proc, length, func(a, b types.Rowid) bool {
			return a.EQ(&b)
		}, selectList)
	case types.T_enum:
		return opBinaryFixedFixedToFixedNullSafe[types.Enum](parameters, rs, proc, length, func(a, b types.Enum) bool {
			return a == b
		}, selectList)
	case types.T_year:
		return opBinaryFixedFixedToFixedNullSafe[types.MoYear](parameters, rs, proc, length, func(a, b types.MoYear) bool {
			return a == b
		}, selectList)
	}
	panic("unreached code")
}

// should convert to c.Numeric next.
func equalFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[bool](result)
	if isDatetimeTimestampComparison(*paramType, *parameters[1].GetType()) {
		return compareDatetimeAndTimestamp(parameters, rs, proc, length, func(left, right types.Timestamp) bool {
			return left == right
		}, selectList)
	}

	switch paramType.Oid {
	case types.T_bool:
		return opBinaryFixedFixedToFixed[bool, bool, bool](parameters, rs, proc, length, func(a, b bool) bool {
			return a == b
		}, selectList)
	case types.T_bit:
		return opBinaryFixedFixedToFixed[uint64, uint64, bool](parameters, rs, proc, length, func(a, b uint64) bool {
			return a == b
		}, selectList)
	case types.T_int8:
		return opBinaryFixedFixedToFixed[int8, int8, bool](parameters, rs, proc, length, func(a, b int8) bool {
			return a == b
		}, selectList)
	case types.T_int16:
		return opBinaryFixedFixedToFixed[int16, int16, bool](parameters, rs, proc, length, func(a, b int16) bool {
			return a == b
		}, selectList)
	case types.T_int32:
		return opBinaryFixedFixedToFixed[int32, int32, bool](parameters, rs, proc, length, func(a, b int32) bool {
			return a == b
		}, selectList)
	case types.T_int64:
		return opBinaryFixedFixedToFixed[int64, int64, bool](parameters, rs, proc, length, func(a, b int64) bool {
			return a == b
		}, selectList)
	case types.T_uint8:
		return opBinaryFixedFixedToFixed[uint8, uint8, bool](parameters, rs, proc, length, func(a, b uint8) bool {
			return a == b
		}, selectList)
	case types.T_uint16:
		return opBinaryFixedFixedToFixed[uint16, uint16, bool](parameters, rs, proc, length, func(a, b uint16) bool {
			return a == b
		}, selectList)
	case types.T_uint32:
		return opBinaryFixedFixedToFixed[uint32, uint32, bool](parameters, rs, proc, length, func(a, b uint32) bool {
			return a == b
		}, selectList)
	case types.T_uint64:
		return opBinaryFixedFixedToFixed[uint64, uint64, bool](parameters, rs, proc, length, func(a, b uint64) bool {
			return a == b
		}, selectList)
	case types.T_uuid:
		return opBinaryFixedFixedToFixed[types.Uuid, types.Uuid, bool](parameters, rs, proc, length, func(a, b types.Uuid) bool {
			return a == b
		}, selectList)
	case types.T_float32:
		leftNormalizer, rightNormalizer, normalize := float32ComparisonNormalizers(
			paramType.Scale,
			parameters[1].GetType().Scale,
		)
		if normalize {
			return opBinaryFixedFixedToFixed[float32, float32, bool](parameters, rs, proc, length, func(a, b float32) bool {
				a = leftNormalizer.Normalize(a)
				b = rightNormalizer.Normalize(b)
				return a == b
			}, selectList)
		}
		return opBinaryFixedFixedToFixed[float32, float32, bool](parameters, rs, proc, length, func(a, b float32) bool {
			return a == b
		}, selectList)
	case types.T_float64:
		return opBinaryFixedFixedToFixed[float64, float64, bool](parameters, rs, proc, length, func(a, b float64) bool {
			return a == b
		}, selectList)
	case types.T_json:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return compareJsonBytes(a, b) == 0
		}, selectList)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_binary, types.T_varbinary, types.T_datalink:
		if parameters[0].GetArea() == nil && parameters[1].GetArea() == nil && (selectList == nil) {
			return compareVarlenaEqual(parameters, rs, proc, length, selectList)
		}
		return opBinaryStrStrToFixed[bool](parameters, rs, proc, length, func(v1, v2 string) bool {
			return v1 == v2
		}, selectList)
	case types.T_array_float32:
		if parameters[0].GetArea() == nil && parameters[1].GetArea() == nil && (selectList == nil) {
			return compareVarlenaEqual(parameters, rs, proc, length, selectList)
		}
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float32](v1)
			_v2 := types.BytesToArray[float32](v2)

			return types.ArrayCompare[float32](_v1, _v2) == 0
		}, selectList)
	case types.T_array_float64:
		if parameters[0].GetArea() == nil && parameters[1].GetArea() == nil && (selectList == nil) {
			return compareVarlenaEqual(parameters, rs, proc, length, selectList)
		}
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float64](v1)
			_v2 := types.BytesToArray[float64](v2)

			return types.ArrayCompare[float64](_v1, _v2) == 0
		}, selectList)
	case types.T_array_bf16:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			return types.ArrayElementCompare[types.BF16](types.BytesToArray[types.BF16](v1), types.BytesToArray[types.BF16](v2)) == 0
		}, selectList)
	case types.T_array_float16:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			return types.ArrayElementCompare[types.Float16](types.BytesToArray[types.Float16](v1), types.BytesToArray[types.Float16](v2)) == 0
		}, selectList)
	case types.T_array_int8:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			return types.ArrayElementCompare[int8](types.BytesToArray[int8](v1), types.BytesToArray[int8](v2)) == 0
		}, selectList)
	case types.T_array_uint8:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			return types.ArrayElementCompare[uint8](types.BytesToArray[uint8](v1), types.BytesToArray[uint8](v2)) == 0
		}, selectList)
	case types.T_date:
		return opBinaryFixedFixedToFixed[types.Date, types.Date, bool](parameters, rs, proc, length, func(a, b types.Date) bool {
			return a == b
		}, selectList)
	case types.T_datetime:
		return opBinaryFixedFixedToFixed[types.Datetime, types.Datetime, bool](parameters, rs, proc, length, func(a, b types.Datetime) bool {
			return a == b
		}, selectList)
	case types.T_time:
		return opBinaryFixedFixedToFixed[types.Time, types.Time, bool](parameters, rs, proc, length, func(a, b types.Time) bool {
			return a == b
		}, selectList)
	case types.T_timestamp:
		return opBinaryFixedFixedToFixed[types.Timestamp, types.Timestamp, bool](parameters, rs, proc, length, func(a, b types.Timestamp) bool {
			return a == b
		}, selectList)
	case types.T_decimal64:
		return valueDec64Compare(parameters, rs, uint64(length), func(a, b types.Decimal64) bool {
			return a == b
		}, selectList)
	case types.T_decimal128:
		return valueDec128Compare(parameters, rs, uint64(length), func(a, b types.Decimal128) bool {
			return a == b
		}, selectList)
	case types.T_decimal256:
		return valueDec256Compare(parameters, rs, uint64(length), func(a, b types.Decimal256) bool {
			return a == b
		}, selectList)
	case types.T_Rowid:
		return opBinaryFixedFixedToFixed[types.Rowid, types.Rowid, bool](parameters, rs, proc, length, func(a, b types.Rowid) bool {
			return a.EQ(&b)
		}, selectList)
	case types.T_enum:
		return opBinaryFixedFixedToFixed[types.Enum, types.Enum, bool](parameters, rs, proc, length, func(a, b types.Enum) bool {
			return a == b
		}, selectList)
	case types.T_year:
		return opBinaryFixedFixedToFixed[types.MoYear, types.MoYear, bool](parameters, rs, proc, length, func(a, b types.MoYear) bool {
			return a == b
		}, selectList)
	}
	panic("unreached code")
}

type valueDecimalCompareType interface {
	types.Decimal64 | types.Decimal128 | types.Decimal256
}

// valueDecimalCompare compares in the operands' scale without assuming that
// multiplying a coefficient by a power of ten still fits its physical type.
func valueDecimalCompare[T valueDecimalCompareType](
	parameters []*vector.Vector, result *vector.FunctionResult[bool], length uint64,
	cmpFn func(a, b T) bool, scaleValue func(v T) (T, bool),
	negative func(T) bool, zero, one T, delta int32, selectList *FunctionSelectList,
) error {
	if delta == 0 {
		return opBinaryFixedFixedToFixed[T, T, bool](parameters, result, nil, int(length), cmpFn, selectList)
	}

	// On overflow the scaled value has greater magnitude than every value of
	// the same physical type. Preserve that ordering using its original sign.
	less, greater := cmpFn(zero, one), cmpFn(one, zero)
	constantSide := -1
	if delta > 0 && parameters[0].IsConst() {
		constantSide = 0
	} else if delta < 0 && parameters[1].IsConst() {
		constantSide = 1
	}
	if constantSide >= 0 && (selectList == nil || !selectList.IgnoreAllRow() && selectList.ShouldEvalAllRow()) {
		result.UseOptFunctionParamFrame(2)
		constantParam := vector.OptGetParamFromWrapper[T](result, constantSide, parameters[constantSide])
		variableSide := 1 - constantSide
		variableParam := vector.OptGetParamFromWrapper[T](result, variableSide, parameters[variableSide])
		constant, isNull := constantParam.GetValue(0)
		variableMayNull := variableParam.WithAnyNullValue()
		if !isNull && (!variableMayNull || !parameters[variableSide].IsConst()) {
			scaled, ok := scaleValue(constant)
			resultVector := result.GetResultVector()
			values := vector.MustFixedColNoTypeCheck[bool](resultVector)
			if variableMayNull {
				nulls.Or(resultVector.GetNulls(), parameters[variableSide].GetNulls(), resultVector.GetNulls())
			}
			if !ok {
				answer := greater
				if constantSide == 0 && negative(constant) || constantSide == 1 && !negative(constant) {
					answer = less
				}
				for i := uint64(0); i < length; i++ {
					values[i] = answer
				}
			} else if parameters[variableSide].IsConst() {
				value, _ := variableParam.GetValue(0)
				answer := cmpFn(value, scaled)
				if constantSide == 0 {
					answer = cmpFn(scaled, value)
				}
				for i := uint64(0); i < length; i++ {
					values[i] = answer
				}
			} else {
				input := variableParam.UnSafeGetAllValue()
				if variableMayNull {
					for i := uint64(0); i < length; i++ {
						if resultVector.GetNulls().Contains(i) {
							continue
						}
						if constantSide == 0 {
							values[i] = cmpFn(scaled, input[i])
						} else {
							values[i] = cmpFn(input[i], scaled)
						}
					}
				} else if constantSide == 0 {
					for i := uint64(0); i < length; i++ {
						values[i] = cmpFn(scaled, input[i])
					}
				} else {
					for i := uint64(0); i < length; i++ {
						values[i] = cmpFn(input[i], scaled)
					}
				}
			}
			return nil
		}
	}
	// When only the unscaled operand is constant, walk the non-null vector
	// directly and keep its per-value overflow checks.
	unscaledConstant := -1
	if delta > 0 && parameters[1].IsConst() && !parameters[0].IsConst() {
		unscaledConstant = 1
	} else if delta < 0 && parameters[0].IsConst() && !parameters[1].IsConst() {
		unscaledConstant = 0
	}
	if unscaledConstant >= 0 && (selectList == nil || !selectList.IgnoreAllRow() && selectList.ShouldEvalAllRow()) {
		result.UseOptFunctionParamFrame(2)
		constantParam := vector.OptGetParamFromWrapper[T](result, unscaledConstant, parameters[unscaledConstant])
		variableSide := 1 - unscaledConstant
		variableParam := vector.OptGetParamFromWrapper[T](result, variableSide, parameters[variableSide])
		constant, isNull := constantParam.GetValue(0)
		if !isNull && !variableParam.WithAnyNullValue() {
			input := variableParam.UnSafeGetAllValue()
			values := vector.MustFixedColNoTypeCheck[bool](result.GetResultVector())
			if variableSide == 0 {
				for i := uint64(0); i < length; i++ {
					scaled, ok := scaleValue(input[i])
					if ok {
						values[i] = cmpFn(scaled, constant)
					} else if negative(input[i]) {
						values[i] = less
					} else {
						values[i] = greater
					}
				}
			} else {
				for i := uint64(0); i < length; i++ {
					scaled, ok := scaleValue(input[i])
					if ok {
						values[i] = cmpFn(constant, scaled)
					} else if negative(input[i]) {
						values[i] = greater
					} else {
						values[i] = less
					}
				}
			}
			return nil
		}
	}
	compare := func(a, b T) bool {
		if delta > 0 {
			scaled, ok := scaleValue(a)
			if !ok {
				if negative(a) {
					return less
				}
				return greater
			}
			return cmpFn(scaled, b)
		}
		scaled, ok := scaleValue(b)
		if !ok {
			if negative(b) {
				return greater
			}
			return less
		}
		return cmpFn(a, scaled)
	}
	// Preserve the existing one-time scaling fast path when the operand that
	// needs rescaling is constant.
	if delta > 0 && parameters[0].IsConst() {
		value, isNull := vector.GenerateFunctionFixedTypeParameter[T](parameters[0]).GetValue(0)
		if !isNull {
			scaled, ok := scaleValue(value)
			if !ok {
				answer := greater
				if negative(value) {
					answer = less
				}
				compare = func(_, _ T) bool { return answer }
			} else {
				compare = func(_ T, b T) bool { return cmpFn(scaled, b) }
			}
		}
	} else if delta < 0 && parameters[1].IsConst() {
		value, isNull := vector.GenerateFunctionFixedTypeParameter[T](parameters[1]).GetValue(0)
		if !isNull {
			scaled, ok := scaleValue(value)
			if !ok {
				answer := less
				if negative(value) {
					answer = greater
				}
				compare = func(_, _ T) bool { return answer }
			} else {
				compare = func(a T, _ T) bool { return cmpFn(a, scaled) }
			}
		}
	}
	return opBinaryFixedFixedToFixed[T, T, bool](parameters, result, nil, int(length), compare, selectList)
}

// decimal64ScaleUp proves signed multiplication is representable before
// multiplying. Avoid formatting an error for each out-of-range vector element.
func decimal64ScaleUp(delta int32) func(types.Decimal64) (types.Decimal64, bool) {
	if delta > 18 {
		return func(v types.Decimal64) (types.Decimal64, bool) { return v, v == 0 }
	}
	factor := int64(types.Pow10[delta])
	upper, lower := int64(math.MaxInt64)/factor, int64(math.MinInt64)/factor
	return func(v types.Decimal64) (types.Decimal64, bool) {
		n := int64(v)
		if n > upper || n < lower {
			return v, false
		}
		return types.Decimal64(uint64(n * factor)), true
	}
}

func decimalComparisonScaleDelta(parameters []*vector.Vector) int32 {
	return parameters[1].GetType().Scale - parameters[0].GetType().Scale
}

func decimalScaleMagnitude(delta int32) int32 {
	if delta < 0 {
		return -delta
	}
	return delta
}

func valueDec64Compare(
	parameters []*vector.Vector, result *vector.FunctionResult[bool], length uint64,
	cmpFn func(a, b types.Decimal64) bool, selectList *FunctionSelectList,
) error {
	delta := decimalComparisonScaleDelta(parameters)
	if delta == 0 {
		return opBinaryFixedFixedToFixed[types.Decimal64, types.Decimal64, bool](parameters, result, nil, int(length), cmpFn, selectList)
	}
	return valueDecimalCompare(parameters, result, length, cmpFn, decimal64ScaleUp(decimalScaleMagnitude(delta)),
		types.Decimal64.Sign, types.Decimal64(0), types.Decimal64(1), delta, selectList)
}

func valueDec128Compare(
	parameters []*vector.Vector, result *vector.FunctionResult[bool], length uint64,
	cmpFn func(a, b types.Decimal128) bool, selectList *FunctionSelectList,
) error {
	delta := decimalComparisonScaleDelta(parameters)
	if parameters[0].GetType().Oid != types.T_decimal128 || parameters[1].GetType().Oid != types.T_decimal128 {
		// The vector wrapper also converts float and DECIMAL64 inputs; use its
		// resulting scale rather than the source vector's metadata.
		left := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](parameters[0])
		right := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](parameters[1])
		delta = right.GetType().Scale - left.GetType().Scale
	}
	if delta == 0 {
		return opBinaryFixedFixedToFixed[types.Decimal128, types.Decimal128, bool](parameters, result, nil, int(length), cmpFn, selectList)
	}
	magnitude := decimalScaleMagnitude(delta)
	scale := func(value types.Decimal128) (types.Decimal128, bool) {
		scaled, err := value.Scale(magnitude)
		return scaled, err == nil && scaled.Sign() == value.Sign()
	}
	return valueDecimalCompare(parameters, result, length, cmpFn, scale,
		types.Decimal128.Sign, types.Decimal128{}, types.Decimal128{B0_63: 1}, delta, selectList)
}

func valueDec256Compare(
	parameters []*vector.Vector, result *vector.FunctionResult[bool], length uint64,
	cmpFn func(a, b types.Decimal256) bool, selectList *FunctionSelectList,
) error {
	delta := decimalComparisonScaleDelta(parameters)
	if delta == 0 {
		return opBinaryFixedFixedToFixed[types.Decimal256, types.Decimal256, bool](parameters, result, nil, int(length), cmpFn, selectList)
	}
	magnitude := decimalScaleMagnitude(delta)
	scale := func(value types.Decimal256) (types.Decimal256, bool) {
		scaled, err := value.Scale(magnitude)
		return scaled, err == nil && scaled.Sign() == value.Sign()
	}
	return valueDecimalCompare(parameters, result, length, cmpFn, scale,
		types.Decimal256.Sign, types.Decimal256{}, types.Decimal256{B0_63: 1}, delta, selectList)
}

func greatThanFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[bool](result)
	if isDatetimeTimestampComparison(*paramType, *parameters[1].GetType()) {
		return compareDatetimeAndTimestamp(parameters, rs, proc, length, func(left, right types.Timestamp) bool {
			return left > right
		}, selectList)
	}
	switch paramType.Oid {
	case types.T_bit:
		return opBinaryFixedFixedToFixed[uint64, uint64, bool](parameters, rs, proc, length, func(a, b uint64) bool {
			return a > b
		}, selectList)
	case types.T_bool:
		return opBinaryFixedFixedToFixed[bool, bool, bool](parameters, rs, proc, length, func(x, y bool) bool {
			return x && !y
		}, selectList)
	case types.T_int8:
		return opBinaryFixedFixedToFixed[int8, int8, bool](parameters, rs, proc, length, func(a, b int8) bool {
			return a > b
		}, selectList)
	case types.T_int16:
		return opBinaryFixedFixedToFixed[int16, int16, bool](parameters, rs, proc, length, func(a, b int16) bool {
			return a > b
		}, selectList)
	case types.T_int32:
		return opBinaryFixedFixedToFixed[int32, int32, bool](parameters, rs, proc, length, func(a, b int32) bool {
			return a > b
		}, selectList)
	case types.T_int64:
		return opBinaryFixedFixedToFixed[int64, int64, bool](parameters, rs, proc, length, func(a, b int64) bool {
			return a > b
		}, selectList)
	case types.T_uint8:
		return opBinaryFixedFixedToFixed[uint8, uint8, bool](parameters, rs, proc, length, func(a, b uint8) bool {
			return a > b
		}, selectList)
	case types.T_uint16:
		return opBinaryFixedFixedToFixed[uint16, uint16, bool](parameters, rs, proc, length, func(a, b uint16) bool {
			return a > b
		}, selectList)
	case types.T_uint32:
		return opBinaryFixedFixedToFixed[uint32, uint32, bool](parameters, rs, proc, length, func(a, b uint32) bool {
			return a > b
		}, selectList)
	case types.T_uint64:
		return opBinaryFixedFixedToFixed[uint64, uint64, bool](parameters, rs, proc, length, func(a, b uint64) bool {
			return a > b
		}, selectList)
	case types.T_uuid:
		return opBinaryFixedFixedToFixed[types.Uuid, types.Uuid, bool](parameters, rs, proc, length, func(v1, v2 types.Uuid) bool {
			return types.CompareUuid(v1, v2) > 0
		}, selectList)
	case types.T_float32:
		leftNormalizer, rightNormalizer, normalize := float32ComparisonNormalizers(
			paramType.Scale,
			parameters[1].GetType().Scale,
		)
		if normalize {
			return opBinaryFixedFixedToFixed[float32, float32, bool](parameters, rs, proc, length, func(a, b float32) bool {
				a = leftNormalizer.Normalize(a)
				b = rightNormalizer.Normalize(b)
				return a > b
			}, selectList)
		}
		return opBinaryFixedFixedToFixed[float32, float32, bool](parameters, rs, proc, length, func(a, b float32) bool {
			return a > b
		}, selectList)
	case types.T_float64:
		return opBinaryFixedFixedToFixed[float64, float64, bool](parameters, rs, proc, length, func(a, b float64) bool {
			return a > b
		}, selectList)
	case types.T_json:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return compareJsonBytes(a, b) > 0
		}, selectList)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_datalink:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return bytes.Compare(a, b) > 0
		}, selectList)
	case types.T_binary, types.T_varbinary:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return bytes.Compare(a, b) > 0
		}, selectList)
	case types.T_array_float32:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float32](v1)
			_v2 := types.BytesToArray[float32](v2)

			return types.ArrayCompare[float32](_v1, _v2) > 0
		}, selectList)
	case types.T_array_float64:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float64](v1)
			_v2 := types.BytesToArray[float64](v2)

			return types.ArrayCompare[float64](_v1, _v2) > 0
		}, selectList)
	case types.T_array_bf16:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[types.BF16](v1)
			_v2 := types.BytesToArray[types.BF16](v2)
			return types.ArrayElementCompare[types.BF16](_v1, _v2) > 0
		}, selectList)
	case types.T_array_float16:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[types.Float16](v1)
			_v2 := types.BytesToArray[types.Float16](v2)
			return types.ArrayElementCompare[types.Float16](_v1, _v2) > 0
		}, selectList)
	case types.T_array_int8:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[int8](v1)
			_v2 := types.BytesToArray[int8](v2)
			return types.ArrayElementCompare[int8](_v1, _v2) > 0
		}, selectList)
	case types.T_array_uint8:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[uint8](v1)
			_v2 := types.BytesToArray[uint8](v2)
			return types.ArrayElementCompare[uint8](_v1, _v2) > 0
		}, selectList)
	case types.T_date:
		return opBinaryFixedFixedToFixed[types.Date, types.Date, bool](parameters, rs, proc, length, func(a, b types.Date) bool {
			return a > b
		}, selectList)
	case types.T_datetime:
		return opBinaryFixedFixedToFixed[types.Datetime, types.Datetime, bool](parameters, rs, proc, length, func(a, b types.Datetime) bool {
			return a > b
		}, selectList)
	case types.T_time:
		return opBinaryFixedFixedToFixed[types.Time, types.Time, bool](parameters, rs, proc, length, func(a, b types.Time) bool {
			return a > b
		}, selectList)
	case types.T_timestamp:
		return opBinaryFixedFixedToFixed[types.Timestamp, types.Timestamp, bool](parameters, rs, proc, length, func(a, b types.Timestamp) bool {
			return a > b
		}, selectList)
	case types.T_decimal64:
		return valueDec64Compare(parameters, rs, uint64(length), func(a, b types.Decimal64) bool {
			return a.Compare(b) > 0
		}, selectList)
	case types.T_decimal128:
		return valueDec128Compare(parameters, rs, uint64(length), func(a, b types.Decimal128) bool {
			return a.Compare(b) > 0
		}, selectList)
	case types.T_decimal256:
		return valueDec256Compare(parameters, rs, uint64(length), func(a, b types.Decimal256) bool {
			return a.Compare(b) > 0
		}, selectList)
	case types.T_Rowid:
		return opBinaryFixedFixedToFixed[types.Rowid, types.Rowid, bool](parameters, rs, proc, length, func(a, b types.Rowid) bool {
			return a.GT(&b)
		}, selectList)
	case types.T_year:
		return opBinaryFixedFixedToFixed[types.MoYear, types.MoYear, bool](parameters, rs, proc, length, func(a, b types.MoYear) bool {
			return a > b
		}, selectList)
	}
	panic("unreached code")
}

func greatEqualFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[bool](result)
	if isDatetimeTimestampComparison(*paramType, *parameters[1].GetType()) {
		return compareDatetimeAndTimestamp(parameters, rs, proc, length, func(left, right types.Timestamp) bool {
			return left >= right
		}, selectList)
	}
	switch paramType.Oid {
	case types.T_bool:
		return opBinaryFixedFixedToFixed[bool, bool, bool](parameters, rs, proc, length, func(x, y bool) bool {
			return x || !y
		}, selectList)
	case types.T_bit:
		return opBinaryFixedFixedToFixed[uint64, uint64, bool](parameters, rs, proc, length, func(a, b uint64) bool {
			return a >= b
		}, selectList)
	case types.T_int8:
		return opBinaryFixedFixedToFixed[int8, int8, bool](parameters, rs, proc, length, func(a, b int8) bool {
			return a >= b
		}, selectList)
	case types.T_int16:
		return opBinaryFixedFixedToFixed[int16, int16, bool](parameters, rs, proc, length, func(a, b int16) bool {
			return a >= b
		}, selectList)
	case types.T_int32:
		return opBinaryFixedFixedToFixed[int32, int32, bool](parameters, rs, proc, length, func(a, b int32) bool {
			return a >= b
		}, selectList)
	case types.T_int64:
		return opBinaryFixedFixedToFixed[int64, int64, bool](parameters, rs, proc, length, func(a, b int64) bool {
			return a >= b
		}, selectList)
	case types.T_uint8:
		return opBinaryFixedFixedToFixed[uint8, uint8, bool](parameters, rs, proc, length, func(a, b uint8) bool {
			return a >= b
		}, selectList)
	case types.T_uint16:
		return opBinaryFixedFixedToFixed[uint16, uint16, bool](parameters, rs, proc, length, func(a, b uint16) bool {
			return a >= b
		}, selectList)
	case types.T_uint32:
		return opBinaryFixedFixedToFixed[uint32, uint32, bool](parameters, rs, proc, length, func(a, b uint32) bool {
			return a >= b
		}, selectList)
	case types.T_uint64:
		return opBinaryFixedFixedToFixed[uint64, uint64, bool](parameters, rs, proc, length, func(a, b uint64) bool {
			return a >= b
		}, selectList)
	case types.T_uuid:
		return opBinaryFixedFixedToFixed[types.Uuid, types.Uuid, bool](parameters, rs, proc, length, func(v1, v2 types.Uuid) bool {
			return types.CompareUuid(v1, v2) >= 0
		}, selectList)
	case types.T_float32:
		leftNormalizer, rightNormalizer, normalize := float32ComparisonNormalizers(
			paramType.Scale,
			parameters[1].GetType().Scale,
		)
		if normalize {
			return opBinaryFixedFixedToFixed[float32, float32, bool](parameters, rs, proc, length, func(a, b float32) bool {
				a = leftNormalizer.Normalize(a)
				b = rightNormalizer.Normalize(b)
				return a >= b
			}, selectList)
		}
		return opBinaryFixedFixedToFixed[float32, float32, bool](parameters, rs, proc, length, func(a, b float32) bool {
			return a >= b
		}, selectList)
	case types.T_float64:
		return opBinaryFixedFixedToFixed[float64, float64, bool](parameters, rs, proc, length, func(a, b float64) bool {
			return a >= b
		}, selectList)
	case types.T_json:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return compareJsonBytes(a, b) >= 0
		}, selectList)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_datalink:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return bytes.Compare(a, b) >= 0
		}, selectList)
	case types.T_binary, types.T_varbinary:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return bytes.Compare(a, b) >= 0
		}, selectList)
	case types.T_array_float32:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float32](v1)
			_v2 := types.BytesToArray[float32](v2)

			return types.ArrayCompare[float32](_v1, _v2) >= 0
		}, selectList)
	case types.T_array_float64:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float64](v1)
			_v2 := types.BytesToArray[float64](v2)

			return types.ArrayCompare[float64](_v1, _v2) >= 0
		}, selectList)
	case types.T_array_bf16:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[types.BF16](v1)
			_v2 := types.BytesToArray[types.BF16](v2)
			return types.ArrayElementCompare[types.BF16](_v1, _v2) >= 0
		}, selectList)
	case types.T_array_float16:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[types.Float16](v1)
			_v2 := types.BytesToArray[types.Float16](v2)
			return types.ArrayElementCompare[types.Float16](_v1, _v2) >= 0
		}, selectList)
	case types.T_array_int8:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[int8](v1)
			_v2 := types.BytesToArray[int8](v2)
			return types.ArrayElementCompare[int8](_v1, _v2) >= 0
		}, selectList)
	case types.T_array_uint8:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[uint8](v1)
			_v2 := types.BytesToArray[uint8](v2)
			return types.ArrayElementCompare[uint8](_v1, _v2) >= 0
		}, selectList)
	case types.T_date:
		return opBinaryFixedFixedToFixed[types.Date, types.Date, bool](parameters, rs, proc, length, func(a, b types.Date) bool {
			return a >= b
		}, selectList)
	case types.T_datetime:
		return opBinaryFixedFixedToFixed[types.Datetime, types.Datetime, bool](parameters, rs, proc, length, func(a, b types.Datetime) bool {
			return a >= b
		}, selectList)
	case types.T_time:
		return opBinaryFixedFixedToFixed[types.Time, types.Time, bool](parameters, rs, proc, length, func(a, b types.Time) bool {
			return a >= b
		}, selectList)
	case types.T_timestamp:
		return opBinaryFixedFixedToFixed[types.Timestamp, types.Timestamp, bool](parameters, rs, proc, length, func(a, b types.Timestamp) bool {
			return a >= b
		}, selectList)
	case types.T_decimal64:
		return valueDec64Compare(parameters, rs, uint64(length), func(a, b types.Decimal64) bool {
			return a.Compare(b) >= 0
		}, selectList)
	case types.T_decimal128:
		return valueDec128Compare(parameters, rs, uint64(length), func(a, b types.Decimal128) bool {
			return a.Compare(b) >= 0
		}, selectList)
	case types.T_decimal256:
		return valueDec256Compare(parameters, rs, uint64(length), func(a, b types.Decimal256) bool {
			return a.Compare(b) >= 0
		}, selectList)
	case types.T_Rowid:
		return opBinaryFixedFixedToFixed[types.Rowid, types.Rowid, bool](parameters, rs, proc, length, func(a, b types.Rowid) bool {
			return a.GE(&b)
		}, selectList)
	case types.T_year:
		return opBinaryFixedFixedToFixed[types.MoYear, types.MoYear, bool](parameters, rs, proc, length, func(a, b types.MoYear) bool {
			return a >= b
		}, selectList)
	}
	panic("unreached code")
}

func notEqualFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[bool](result)
	if isDatetimeTimestampComparison(*paramType, *parameters[1].GetType()) {
		return compareDatetimeAndTimestamp(parameters, rs, proc, length, func(left, right types.Timestamp) bool {
			return left != right
		}, selectList)
	}
	switch paramType.Oid {
	case types.T_bool:
		return opBinaryFixedFixedToFixed[bool, bool, bool](parameters, rs, proc, length, func(a, b bool) bool {
			return a != b
		}, selectList)
	case types.T_bit:
		return opBinaryStrStrToFixed[bool](parameters, rs, proc, length, func(a, b string) bool {
			return a != b
		}, selectList)
	case types.T_int8:
		return opBinaryFixedFixedToFixed[int8, int8, bool](parameters, rs, proc, length, func(a, b int8) bool {
			return a != b
		}, selectList)
	case types.T_int16:
		return opBinaryFixedFixedToFixed[int16, int16, bool](parameters, rs, proc, length, func(a, b int16) bool {
			return a != b
		}, selectList)
	case types.T_int32:
		return opBinaryFixedFixedToFixed[int32, int32, bool](parameters, rs, proc, length, func(a, b int32) bool {
			return a != b
		}, selectList)
	case types.T_int64:
		return opBinaryFixedFixedToFixed[int64, int64, bool](parameters, rs, proc, length, func(a, b int64) bool {
			return a != b
		}, selectList)
	case types.T_uint8:
		return opBinaryFixedFixedToFixed[uint8, uint8, bool](parameters, rs, proc, length, func(a, b uint8) bool {
			return a != b
		}, selectList)
	case types.T_uint16:
		return opBinaryFixedFixedToFixed[uint16, uint16, bool](parameters, rs, proc, length, func(a, b uint16) bool {
			return a != b
		}, selectList)
	case types.T_uint32:
		return opBinaryFixedFixedToFixed[uint32, uint32, bool](parameters, rs, proc, length, func(a, b uint32) bool {
			return a != b
		}, selectList)
	case types.T_uint64:
		return opBinaryFixedFixedToFixed[uint64, uint64, bool](parameters, rs, proc, length, func(a, b uint64) bool {
			return a != b
		}, selectList)
	case types.T_uuid:
		return opBinaryFixedFixedToFixed[types.Uuid, types.Uuid, bool](parameters, rs, proc, length, func(a, b types.Uuid) bool {
			return a != b
		}, selectList)
	case types.T_float32:
		leftNormalizer, rightNormalizer, normalize := float32ComparisonNormalizers(
			paramType.Scale,
			parameters[1].GetType().Scale,
		)
		if normalize {
			return opBinaryFixedFixedToFixed[float32, float32, bool](parameters, rs, proc, length, func(a, b float32) bool {
				a = leftNormalizer.Normalize(a)
				b = rightNormalizer.Normalize(b)
				return a != b
			}, selectList)
		}
		return opBinaryFixedFixedToFixed[float32, float32, bool](parameters, rs, proc, length, func(a, b float32) bool {
			return a != b
		}, selectList)
	case types.T_float64:
		return opBinaryFixedFixedToFixed[float64, float64, bool](parameters, rs, proc, length, func(a, b float64) bool {
			return a != b
		}, selectList)
	case types.T_json:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return compareJsonBytes(a, b) != 0
		}, selectList)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_datalink:
		return opBinaryStrStrToFixed[bool](parameters, rs, proc, length, func(a, b string) bool {
			return a != b
		}, selectList)
	case types.T_binary, types.T_varbinary:
		return opBinaryStrStrToFixed[bool](parameters, rs, proc, length, func(a, b string) bool {
			return a != b
		}, selectList)
	case types.T_array_float32:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float32](v1)
			_v2 := types.BytesToArray[float32](v2)

			return types.ArrayCompare[float32](_v1, _v2) != 0
		}, selectList)
	case types.T_array_float64:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float64](v1)
			_v2 := types.BytesToArray[float64](v2)

			return types.ArrayCompare[float64](_v1, _v2) != 0
		}, selectList)
	case types.T_array_bf16:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[types.BF16](v1)
			_v2 := types.BytesToArray[types.BF16](v2)
			return types.ArrayElementCompare[types.BF16](_v1, _v2) != 0
		}, selectList)
	case types.T_array_float16:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[types.Float16](v1)
			_v2 := types.BytesToArray[types.Float16](v2)
			return types.ArrayElementCompare[types.Float16](_v1, _v2) != 0
		}, selectList)
	case types.T_array_int8:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[int8](v1)
			_v2 := types.BytesToArray[int8](v2)
			return types.ArrayElementCompare[int8](_v1, _v2) != 0
		}, selectList)
	case types.T_array_uint8:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[uint8](v1)
			_v2 := types.BytesToArray[uint8](v2)
			return types.ArrayElementCompare[uint8](_v1, _v2) != 0
		}, selectList)
	case types.T_date:
		return opBinaryFixedFixedToFixed[types.Date, types.Date, bool](parameters, rs, proc, length, func(a, b types.Date) bool {
			return a != b
		}, selectList)
	case types.T_datetime:
		return opBinaryFixedFixedToFixed[types.Datetime, types.Datetime, bool](parameters, rs, proc, length, func(a, b types.Datetime) bool {
			return a != b
		}, selectList)
	case types.T_time:
		return opBinaryFixedFixedToFixed[types.Time, types.Time, bool](parameters, rs, proc, length, func(a, b types.Time) bool {
			return a != b
		}, selectList)
	case types.T_timestamp:
		return opBinaryFixedFixedToFixed[types.Timestamp, types.Timestamp, bool](parameters, rs, proc, length, func(a, b types.Timestamp) bool {
			return a != b
		}, selectList)
	case types.T_decimal64:
		return valueDec64Compare(parameters, rs, uint64(length), func(a, b types.Decimal64) bool {
			return a != b
		}, selectList)
	case types.T_decimal128:
		return valueDec128Compare(parameters, rs, uint64(length), func(a, b types.Decimal128) bool {
			return a != b
		}, selectList)
	case types.T_decimal256:
		return valueDec256Compare(parameters, rs, uint64(length), func(a, b types.Decimal256) bool {
			return a != b
		}, selectList)
	case types.T_Rowid:
		return opBinaryFixedFixedToFixed[types.Rowid, types.Rowid, bool](parameters, rs, proc, length, func(a, b types.Rowid) bool {
			return !a.EQ(&b)
		}, selectList)
	case types.T_year:
		return opBinaryFixedFixedToFixed[types.MoYear, types.MoYear, bool](parameters, rs, proc, length, func(a, b types.MoYear) bool {
			return a != b
		}, selectList)
	}
	panic("unreached code")
}

func lessThanFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[bool](result)
	if isDatetimeTimestampComparison(*paramType, *parameters[1].GetType()) {
		return compareDatetimeAndTimestamp(parameters, rs, proc, length, func(left, right types.Timestamp) bool {
			return left < right
		}, selectList)
	}
	switch paramType.Oid {
	case types.T_bool:
		return opBinaryFixedFixedToFixed[bool, bool, bool](parameters, rs, proc, length, func(x, y bool) bool {
			return !x && y
		}, selectList)
	case types.T_bit:
		return opBinaryFixedFixedToFixed[uint64, uint64, bool](parameters, rs, proc, length, func(a, b uint64) bool {
			return a < b
		}, selectList)
	case types.T_int8:
		return opBinaryFixedFixedToFixed[int8, int8, bool](parameters, rs, proc, length, func(a, b int8) bool {
			return a < b
		}, selectList)
	case types.T_int16:
		return opBinaryFixedFixedToFixed[int16, int16, bool](parameters, rs, proc, length, func(a, b int16) bool {
			return a < b
		}, selectList)
	case types.T_int32:
		return opBinaryFixedFixedToFixed[int32, int32, bool](parameters, rs, proc, length, func(a, b int32) bool {
			return a < b
		}, selectList)
	case types.T_int64:
		return opBinaryFixedFixedToFixed[int64, int64, bool](parameters, rs, proc, length, func(a, b int64) bool {
			return a < b
		}, selectList)
	case types.T_uint8:
		return opBinaryFixedFixedToFixed[uint8, uint8, bool](parameters, rs, proc, length, func(a, b uint8) bool {
			return a < b
		}, selectList)
	case types.T_uint16:
		return opBinaryFixedFixedToFixed[uint16, uint16, bool](parameters, rs, proc, length, func(a, b uint16) bool {
			return a < b
		}, selectList)
	case types.T_uint32:
		return opBinaryFixedFixedToFixed[uint32, uint32, bool](parameters, rs, proc, length, func(a, b uint32) bool {
			return a < b
		}, selectList)
	case types.T_uint64:
		return opBinaryFixedFixedToFixed[uint64, uint64, bool](parameters, rs, proc, length, func(a, b uint64) bool {
			return a < b
		}, selectList)
	case types.T_uuid:
		return opBinaryFixedFixedToFixed[types.Uuid, types.Uuid, bool](parameters, rs, proc, length, func(v1, v2 types.Uuid) bool {
			return types.CompareUuid(v1, v2) < 0
		}, selectList)
	case types.T_float32:
		leftNormalizer, rightNormalizer, normalize := float32ComparisonNormalizers(
			paramType.Scale,
			parameters[1].GetType().Scale,
		)
		if normalize {
			return opBinaryFixedFixedToFixed[float32, float32, bool](parameters, rs, proc, length, func(a, b float32) bool {
				a = leftNormalizer.Normalize(a)
				b = rightNormalizer.Normalize(b)
				return a < b
			}, selectList)
		}
		return opBinaryFixedFixedToFixed[float32, float32, bool](parameters, rs, proc, length, func(a, b float32) bool {
			return a < b
		}, selectList)
	case types.T_float64:
		return opBinaryFixedFixedToFixed[float64, float64, bool](parameters, rs, proc, length, func(a, b float64) bool {
			return a < b
		}, selectList)
	case types.T_json:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return compareJsonBytes(a, b) < 0
		}, selectList)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_datalink:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return bytes.Compare(a, b) < 0
		}, selectList)
	case types.T_binary, types.T_varbinary:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return bytes.Compare(a, b) < 0
		}, selectList)
	case types.T_array_float32:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float32](v1)
			_v2 := types.BytesToArray[float32](v2)

			return types.ArrayCompare[float32](_v1, _v2) < 0
		}, selectList)
	case types.T_array_float64:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float64](v1)
			_v2 := types.BytesToArray[float64](v2)

			return types.ArrayCompare[float64](_v1, _v2) < 0
		}, selectList)
	case types.T_array_bf16:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[types.BF16](v1)
			_v2 := types.BytesToArray[types.BF16](v2)
			return types.ArrayElementCompare[types.BF16](_v1, _v2) < 0
		}, selectList)
	case types.T_array_float16:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[types.Float16](v1)
			_v2 := types.BytesToArray[types.Float16](v2)
			return types.ArrayElementCompare[types.Float16](_v1, _v2) < 0
		}, selectList)
	case types.T_array_int8:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[int8](v1)
			_v2 := types.BytesToArray[int8](v2)
			return types.ArrayElementCompare[int8](_v1, _v2) < 0
		}, selectList)
	case types.T_array_uint8:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[uint8](v1)
			_v2 := types.BytesToArray[uint8](v2)
			return types.ArrayElementCompare[uint8](_v1, _v2) < 0
		}, selectList)
	case types.T_date:
		return opBinaryFixedFixedToFixed[types.Date, types.Date, bool](parameters, rs, proc, length, func(a, b types.Date) bool {
			return a < b
		}, selectList)
	case types.T_datetime:
		return opBinaryFixedFixedToFixed[types.Datetime, types.Datetime, bool](parameters, rs, proc, length, func(a, b types.Datetime) bool {
			return a < b
		}, selectList)
	case types.T_time:
		return opBinaryFixedFixedToFixed[types.Time, types.Time, bool](parameters, rs, proc, length, func(a, b types.Time) bool {
			return a < b
		}, selectList)
	case types.T_timestamp:
		return opBinaryFixedFixedToFixed[types.Timestamp, types.Timestamp, bool](parameters, rs, proc, length, func(a, b types.Timestamp) bool {
			return a < b
		}, selectList)
	case types.T_decimal64:
		return valueDec64Compare(parameters, rs, uint64(length), func(a, b types.Decimal64) bool {
			return a.Compare(b) < 0
		}, selectList)
	case types.T_decimal128:
		return valueDec128Compare(parameters, rs, uint64(length), func(a, b types.Decimal128) bool {
			return a.Compare(b) < 0
		}, selectList)
	case types.T_decimal256:
		return valueDec256Compare(parameters, rs, uint64(length), func(a, b types.Decimal256) bool {
			return a.Compare(b) < 0
		}, selectList)
	case types.T_Rowid:
		return opBinaryFixedFixedToFixed[types.Rowid, types.Rowid, bool](parameters, rs, proc, length, func(a, b types.Rowid) bool {
			return a.LT(&b)
		}, selectList)
	case types.T_year:
		return opBinaryFixedFixedToFixed[types.MoYear, types.MoYear, bool](parameters, rs, proc, length, func(a, b types.MoYear) bool {
			return a < b
		}, selectList)
	}
	panic("unreached code")
}

func lessEqualFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[bool](result)
	if isDatetimeTimestampComparison(*paramType, *parameters[1].GetType()) {
		return compareDatetimeAndTimestamp(parameters, rs, proc, length, func(left, right types.Timestamp) bool {
			return left <= right
		}, selectList)
	}
	switch paramType.Oid {
	case types.T_bool:
		return opBinaryFixedFixedToFixed[bool, bool, bool](parameters, rs, proc, length, func(x, y bool) bool {
			return !x || y
		}, selectList)
	case types.T_bit:
		return opBinaryFixedFixedToFixed[uint64, uint64, bool](parameters, rs, proc, length, func(a, b uint64) bool {
			return a <= b
		}, selectList)
	case types.T_int8:
		return opBinaryFixedFixedToFixed[int8, int8, bool](parameters, rs, proc, length, func(a, b int8) bool {
			return a <= b
		}, selectList)
	case types.T_int16:
		return opBinaryFixedFixedToFixed[int16, int16, bool](parameters, rs, proc, length, func(a, b int16) bool {
			return a <= b
		}, selectList)
	case types.T_int32:
		return opBinaryFixedFixedToFixed[int32, int32, bool](parameters, rs, proc, length, func(a, b int32) bool {
			return a <= b
		}, selectList)
	case types.T_int64:
		return opBinaryFixedFixedToFixed[int64, int64, bool](parameters, rs, proc, length, func(a, b int64) bool {
			return a <= b
		}, selectList)
	case types.T_uint8:
		return opBinaryFixedFixedToFixed[uint8, uint8, bool](parameters, rs, proc, length, func(a, b uint8) bool {
			return a <= b
		}, selectList)
	case types.T_uint16:
		return opBinaryFixedFixedToFixed[uint16, uint16, bool](parameters, rs, proc, length, func(a, b uint16) bool {
			return a <= b
		}, selectList)
	case types.T_uint32:
		return opBinaryFixedFixedToFixed[uint32, uint32, bool](parameters, rs, proc, length, func(a, b uint32) bool {
			return a <= b
		}, selectList)
	case types.T_uint64:
		return opBinaryFixedFixedToFixed[uint64, uint64, bool](parameters, rs, proc, length, func(a, b uint64) bool {
			return a <= b
		}, selectList)
	case types.T_uuid:
		return opBinaryFixedFixedToFixed[types.Uuid, types.Uuid, bool](parameters, rs, proc, length, func(v1, v2 types.Uuid) bool {
			return types.CompareUuid(v1, v2) <= 0
		}, selectList)
	case types.T_float32:
		leftNormalizer, rightNormalizer, normalize := float32ComparisonNormalizers(
			paramType.Scale,
			parameters[1].GetType().Scale,
		)
		if normalize {
			return opBinaryFixedFixedToFixed[float32, float32, bool](parameters, rs, proc, length, func(a, b float32) bool {
				a = leftNormalizer.Normalize(a)
				b = rightNormalizer.Normalize(b)
				return a <= b
			}, selectList)
		}
		return opBinaryFixedFixedToFixed[float32, float32, bool](parameters, rs, proc, length, func(a, b float32) bool {
			return a <= b
		}, selectList)
	case types.T_float64:
		return opBinaryFixedFixedToFixed[float64, float64, bool](parameters, rs, proc, length, func(a, b float64) bool {
			return a <= b
		}, selectList)
	case types.T_json:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return compareJsonBytes(a, b) <= 0
		}, selectList)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_datalink:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return bytes.Compare(a, b) <= 0
		}, selectList)
	case types.T_binary, types.T_varbinary:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return bytes.Compare(a, b) <= 0
		}, selectList)
	case types.T_array_float32:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float32](v1)
			_v2 := types.BytesToArray[float32](v2)

			return types.ArrayCompare[float32](_v1, _v2) <= 0
		}, selectList)
	case types.T_array_float64:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float64](v1)
			_v2 := types.BytesToArray[float64](v2)

			return types.ArrayCompare[float64](_v1, _v2) <= 0
		}, selectList)
	case types.T_array_bf16:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[types.BF16](v1)
			_v2 := types.BytesToArray[types.BF16](v2)
			return types.ArrayElementCompare[types.BF16](_v1, _v2) <= 0
		}, selectList)
	case types.T_array_float16:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[types.Float16](v1)
			_v2 := types.BytesToArray[types.Float16](v2)
			return types.ArrayElementCompare[types.Float16](_v1, _v2) <= 0
		}, selectList)
	case types.T_array_int8:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[int8](v1)
			_v2 := types.BytesToArray[int8](v2)
			return types.ArrayElementCompare[int8](_v1, _v2) <= 0
		}, selectList)
	case types.T_array_uint8:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[uint8](v1)
			_v2 := types.BytesToArray[uint8](v2)
			return types.ArrayElementCompare[uint8](_v1, _v2) <= 0
		}, selectList)
	case types.T_date:
		return opBinaryFixedFixedToFixed[types.Date, types.Date, bool](parameters, rs, proc, length, func(a, b types.Date) bool {
			return a <= b
		}, selectList)
	case types.T_datetime:
		return opBinaryFixedFixedToFixed[types.Datetime, types.Datetime, bool](parameters, rs, proc, length, func(a, b types.Datetime) bool {
			return a <= b
		}, selectList)
	case types.T_time:
		return opBinaryFixedFixedToFixed[types.Time, types.Time, bool](parameters, rs, proc, length, func(a, b types.Time) bool {
			return a <= b
		}, selectList)
	case types.T_timestamp:
		return opBinaryFixedFixedToFixed[types.Timestamp, types.Timestamp, bool](parameters, rs, proc, length, func(a, b types.Timestamp) bool {
			return a <= b
		}, selectList)
	case types.T_decimal64:
		return valueDec64Compare(parameters, rs, uint64(length), func(a, b types.Decimal64) bool {
			return a.Compare(b) <= 0
		}, selectList)
	case types.T_decimal128:
		return valueDec128Compare(parameters, rs, uint64(length), func(a, b types.Decimal128) bool {
			return a.Compare(b) <= 0
		}, selectList)
	case types.T_decimal256:
		return valueDec256Compare(parameters, rs, uint64(length), func(a, b types.Decimal256) bool {
			return a.Compare(b) <= 0
		}, selectList)
	case types.T_Rowid:
		return opBinaryFixedFixedToFixed[types.Rowid, types.Rowid, bool](parameters, rs, proc, length, func(a, b types.Rowid) bool {
			return a.LE(&b)
		}, selectList)
	case types.T_year:
		return opBinaryFixedFixedToFixed[types.MoYear, types.MoYear, bool](parameters, rs, proc, length, func(a, b types.MoYear) bool {
			return a <= b
		}, selectList)
	}
	panic("unreached code")
}

func operatorOpBitUint64[T1, T2 constraints.Integer](
	parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int,
	fn func(uint64, uint64) uint64) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[T1](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[T2](parameters[1])
	rs := vector.MustFunctionResult[uint64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if err := rs.Append(fn(uint64(v1), uint64(v2)), null1 || null2); err != nil {
			return err
		}
	}
	return nil
}

func operatorOpUint64Fn(
	parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int,
	fn func(uint64, uint64) uint64) error {
	return operatorOpBitUint64[uint64, uint64](parameters, result, proc, length, fn)
}

func operatorOpInt64ToUint64Fn(
	parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int,
	fn func(uint64, uint64) uint64) error {
	return operatorOpBitUint64[int64, int64](parameters, result, proc, length, fn)
}

func operatorOpUint64Int64Fn(
	parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int,
	fn func(uint64, uint64) uint64) error {
	return operatorOpBitUint64[uint64, int64](parameters, result, proc, length, fn)
}

func operatorOpInt64Uint64Fn(
	parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int,
	fn func(uint64, uint64) uint64) error {
	return operatorOpBitUint64[int64, uint64](parameters, result, proc, length, fn)
}

func operatorOpStrFn(
	parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int,
	fn func([]byte, []byte) ([]byte, error)) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])
	rs := vector.MustFunctionResult[types.Varlena](result)
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		v2, null2 := p2.GetStrValue(i)
		if null1 || null2 {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			rv, err := fn(v1, v2)
			if err != nil {
				return err
			}
			if err = rs.AppendBytes(rv, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func operatorOpBitAndUint64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpUint64Fn(parameters, result, proc, length, func(i uint64, i2 uint64) uint64 { return i & i2 })
}

func operatorOpBitAndInt64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpInt64ToUint64Fn(parameters, result, proc, length, func(i uint64, i2 uint64) uint64 { return i & i2 })
}

func operatorOpBitAndUint64Int64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpUint64Int64Fn(parameters, result, proc, length, func(i uint64, i2 uint64) uint64 { return i & i2 })
}

func operatorOpBitAndInt64Uint64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpInt64Uint64Fn(parameters, result, proc, length, func(i uint64, i2 uint64) uint64 { return i & i2 })
}

func operatorOpBitAndStrFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpStrFn(parameters, result, proc, length, func(i []byte, i2 []byte) ([]byte, error) {
		if len(i) != len(i2) {
			return nil, moerr.NewInternalErrorNoCtx("Binary operands of bitwise operators must be of equal length")
		}
		rv := make([]byte, len(i))
		for j := range rv {
			rv[j] = i[j] & i2[j]
		}
		return rv, nil
	})
}

func operatorOpBitXorUint64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpUint64Fn(parameters, result, proc, length, func(i uint64, i2 uint64) uint64 { return i ^ i2 })
}

func operatorOpBitXorInt64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpInt64ToUint64Fn(parameters, result, proc, length, func(i uint64, i2 uint64) uint64 { return i ^ i2 })
}

func operatorOpBitXorUint64Int64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpUint64Int64Fn(parameters, result, proc, length, func(i uint64, i2 uint64) uint64 { return i ^ i2 })
}

func operatorOpBitXorInt64Uint64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpInt64Uint64Fn(parameters, result, proc, length, func(i uint64, i2 uint64) uint64 { return i ^ i2 })
}

func operatorOpBitXorStrFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpStrFn(parameters, result, proc, length, func(i []byte, i2 []byte) ([]byte, error) {
		if len(i) != len(i2) {
			return nil, moerr.NewInternalErrorNoCtx("Binary operands of bitwise operators must be of equal length")
		}
		rv := make([]byte, len(i))
		for j := range rv {
			rv[j] = i[j] ^ i2[j]
		}
		return rv, nil
	})
}

func operatorOpBitOrUint64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpUint64Fn(parameters, result, proc, length, func(i uint64, i2 uint64) uint64 { return i | i2 })
}

func operatorOpBitOrInt64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpInt64ToUint64Fn(parameters, result, proc, length, func(i uint64, i2 uint64) uint64 { return i | i2 })
}

func operatorOpBitOrUint64Int64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpUint64Int64Fn(parameters, result, proc, length, func(i uint64, i2 uint64) uint64 { return i | i2 })
}

func operatorOpBitOrInt64Uint64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpInt64Uint64Fn(parameters, result, proc, length, func(i uint64, i2 uint64) uint64 { return i | i2 })
}

func operatorOpBitOrStrFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpStrFn(parameters, result, proc, length, func(i []byte, i2 []byte) ([]byte, error) {
		if len(i) != len(i2) {
			return nil, moerr.NewInternalErrorNoCtx("Binary operands of bitwise operators must be of equal length")
		}
		rv := make([]byte, len(i))
		for j := range rv {
			rv[j] = i[j] | i2[j]
		}
		return rv, nil
	})
}

func bitShiftLeft(value, shift uint64) uint64 {
	if shift >= 64 {
		return 0
	}
	return value << shift
}

func bitShiftRight(value, shift uint64) uint64 {
	if shift >= 64 {
		return 0
	}
	return value >> shift
}

func operatorOpBitShiftLeftUint64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpUint64Fn(parameters, result, proc, length, bitShiftLeft)
}

func operatorOpBitShiftLeftInt64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpInt64ToUint64Fn(parameters, result, proc, length, bitShiftLeft)
}

func operatorOpBitShiftLeftUint64Int64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpUint64Int64Fn(parameters, result, proc, length, bitShiftLeft)
}

func operatorOpBitShiftLeftInt64Uint64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpInt64Uint64Fn(parameters, result, proc, length, bitShiftLeft)
}

func operatorOpBitShiftRightUint64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpUint64Fn(parameters, result, proc, length, bitShiftRight)
}

func operatorOpBitShiftRightInt64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpInt64ToUint64Fn(parameters, result, proc, length, bitShiftRight)
}

func operatorOpBitShiftRightUint64Int64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpUint64Int64Fn(parameters, result, proc, length, bitShiftRight)
}

func operatorOpBitShiftRightInt64Uint64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpInt64Uint64Fn(parameters, result, proc, length, bitShiftRight)
}
