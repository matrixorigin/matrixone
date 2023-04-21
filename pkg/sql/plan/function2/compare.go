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

package function2

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

func compareOperatorSupports(typ1, typ2 types.Type) bool {
	if typ1.Oid != typ2.Oid {
		return false
	}
	switch typ1.Oid {
	case types.T_bool:
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
	case types.T_float32, types.T_float64:
	case types.T_decimal64, types.T_decimal128:
	case types.T_char, types.T_varchar:
	case types.T_date, types.T_datetime:
	case types.T_timestamp, types.T_time:
	case types.T_blob, types.T_text, types.T_json:
	case types.T_binary, types.T_varbinary:
	case types.T_uuid:
	default:
		return false
	}
	return true
}

// should convert to c.Numeric next.
func equalFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[bool](result)
	switch paramType.Oid {
	case types.T_bool:
		return valueEquals[bool](parameters, rs, uint64(length))
	case types.T_int8:
		return valueEquals[int8](parameters, rs, uint64(length))
	case types.T_int16:
		return valueEquals[int16](parameters, rs, uint64(length))
	case types.T_int32:
		return valueEquals[int32](parameters, rs, uint64(length))
	case types.T_int64:
		return valueEquals[int64](parameters, rs, uint64(length))
	case types.T_uint8:
		return valueEquals[uint8](parameters, rs, uint64(length))
	case types.T_uint16:
		return valueEquals[uint16](parameters, rs, uint64(length))
	case types.T_uint32:
		return valueEquals[uint32](parameters, rs, uint64(length))
	case types.T_uint64:
		return valueEquals[uint64](parameters, rs, uint64(length))
	case types.T_uuid:
		return valueEquals[types.Uuid](parameters, rs, uint64(length))
	case types.T_float32:
		return valueEquals[float32](parameters, rs, uint64(length))
	case types.T_float64:
		return valueEquals[float64](parameters, rs, uint64(length))
	case types.T_char, types.T_varchar, types.T_blob, types.T_json, types.T_text:
		return valueStrEquals(parameters, rs, uint64(length))
	case types.T_binary, types.T_varbinary:
		return valueStrEquals(parameters, rs, uint64(length))
	case types.T_date:
		return valueEquals[types.Date](parameters, rs, uint64(length))
	case types.T_datetime:
		return valueEquals[types.Datetime](parameters, rs, uint64(length))
	case types.T_time:
		return valueEquals[types.Time](parameters, rs, uint64(length))
	case types.T_timestamp:
		return valueEquals[types.Timestamp](parameters, rs, uint64(length))
	case types.T_decimal64:
		return valueDec64Equals(parameters, rs, uint64(length))
	case types.T_decimal128:
		return valueDec128Equals(parameters, rs, uint64(length))
	}
	panic("unreached code")
}

func valueEquals[T bool | constraints.Integer | constraints.Float |
	types.Date | types.Datetime | types.Time | types.Timestamp | types.Uuid](
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64) error {
	col1 := vector.GenerateFunctionFixedTypeParameter[T](params[0])
	col2 := vector.GenerateFunctionFixedTypeParameter[T](params[1])
	for i := uint64(0); i < length; i++ {
		v1, null1 := col1.GetValue(i)
		v2, null2 := col2.GetValue(i)
		if err := result.Append(v1 == v2, null1 || null2); err != nil {
			return err
		}
	}
	return nil
}

func valueStrEquals(
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64) error {
	col1 := vector.GenerateFunctionStrParameter(params[0])
	col2 := vector.GenerateFunctionStrParameter(params[1])
	for i := uint64(0); i < length; i++ {
		v1, null1 := col1.GetStrValue(i)
		v2, null2 := col2.GetStrValue(i)
		if err := result.Append(bytes.Equal(v1, v2), null1 || null2); err != nil {
			return err
		}
	}
	return nil
}

func valueDec64Equals(
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64) error {
	col1 := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](params[0])
	col2 := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](params[1])

	m := col2.GetType().Scale - col1.GetType().Scale
	if m >= 0 {
		for i := uint64(0); i < length; i++ {
			v1, null1 := col1.GetValue(i)
			v2, null2 := col2.GetValue(i)
			if null1 || null2 {
				if err := result.Append(false, true); err != nil {
					return err
				}
			} else {
				x, _ := v1.Scale(m)
				if err := result.Append(x == v2, false); err != nil {
					return err
				}
			}
		}
	} else {
		for i := uint64(0); i < length; i++ {
			v1, null1 := col1.GetValue(i)
			v2, null2 := col2.GetValue(i)
			if null1 || null2 {
				if err := result.Append(false, true); err != nil {
					return err
				}
			} else {
				y, _ := v2.Scale(-m)
				if err := result.Append(v1 == y, false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func valueDec128Equals(
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64) error {
	col1 := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](params[0])
	col2 := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](params[1])

	m := col2.GetType().Scale - col1.GetType().Scale
	if m >= 0 {
		for i := uint64(0); i < length; i++ {
			v1, null1 := col1.GetValue(i)
			v2, null2 := col2.GetValue(i)
			if null1 || null2 {
				if err := result.Append(false, true); err != nil {
					return err
				}
			} else {
				x, _ := v1.Scale(m)
				if err := result.Append(x == v2, false); err != nil {
					return err
				}
			}
		}
	} else {
		for i := uint64(0); i < length; i++ {
			v1, null1 := col1.GetValue(i)
			v2, null2 := col2.GetValue(i)
			if null1 || null2 {
				if err := result.Append(false, true); err != nil {
					return err
				}
			} else {
				y, _ := v2.Scale(-m)
				if err := result.Append(v1 == y, false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func greatThanFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[bool](result)
	switch paramType.Oid {
	case types.T_bool:
		return valueBoolGreatThan(parameters, rs, uint64(length))
	case types.T_int8:
		return valueGreatThan1[int8](parameters, rs, uint64(length))
	case types.T_int16:
		return valueGreatThan1[int16](parameters, rs, uint64(length))
	case types.T_int32:
		return valueGreatThan1[int32](parameters, rs, uint64(length))
	case types.T_int64:
		return valueGreatThan1[int64](parameters, rs, uint64(length))
	case types.T_uint8:
		return valueGreatThan1[uint8](parameters, rs, uint64(length))
	case types.T_uint16:
		return valueGreatThan1[uint16](parameters, rs, uint64(length))
	case types.T_uint32:
		return valueGreatThan1[uint32](parameters, rs, uint64(length))
	case types.T_uint64:
		return valueGreatThan1[uint64](parameters, rs, uint64(length))
	case types.T_uuid:
		return valueUuidGreatThan(parameters, rs, uint64(length))
	case types.T_float32:
		return valueGreatThan1[float32](parameters, rs, uint64(length))
	case types.T_float64:
		return valueGreatThan1[float64](parameters, rs, uint64(length))
	case types.T_char, types.T_varchar, types.T_blob, types.T_json, types.T_text:
		return valueStrGreatThan(parameters, rs, uint64(length))
	case types.T_binary, types.T_varbinary:
		return valueStrGreatThan(parameters, rs, uint64(length))
	case types.T_date:
		return valueGreatThan1[types.Date](parameters, rs, uint64(length))
	case types.T_datetime:
		return valueGreatThan1[types.Datetime](parameters, rs, uint64(length))
	case types.T_time:
		return valueGreatThan1[types.Time](parameters, rs, uint64(length))
	case types.T_timestamp:
		return valueGreatThan1[types.Timestamp](parameters, rs, uint64(length))
	case types.T_decimal64:
		return valueDec64GreatThan(parameters, rs, uint64(length))
	case types.T_decimal128:
		return valueDec128GreatThan(parameters, rs, uint64(length))
	}
	panic("unreached code")
}

func valueGreatThan1[T constraints.Integer | constraints.Float |
	types.Date | types.Datetime | types.Time | types.Timestamp](
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64) error {
	col1 := vector.GenerateFunctionFixedTypeParameter[T](params[0])
	col2 := vector.GenerateFunctionFixedTypeParameter[T](params[1])
	for i := uint64(0); i < length; i++ {
		v1, null1 := col1.GetValue(i)
		v2, null2 := col2.GetValue(i)
		if err := result.Append(v1 >= v2, null1 || null2); err != nil {
			return err
		}
	}
	return nil
}

func valueBoolGreatThan(
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64) error {
	f := func(x, y bool) bool {
		return x && !y
	}

	col1 := vector.GenerateFunctionFixedTypeParameter[bool](params[0])
	col2 := vector.GenerateFunctionFixedTypeParameter[bool](params[1])
	for i := uint64(0); i < length; i++ {
		v1, null1 := col1.GetValue(i)
		v2, null2 := col2.GetValue(i)
		if err := result.Append(f(v1, v2), null1 || null2); err != nil {
			return err
		}
	}
	return nil
}

func valueUuidGreatThan(
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64) error {
	col1 := vector.GenerateFunctionFixedTypeParameter[types.Uuid](params[0])
	col2 := vector.GenerateFunctionFixedTypeParameter[types.Uuid](params[1])
	for i := uint64(0); i < length; i++ {
		v1, null1 := col1.GetValue(i)
		v2, null2 := col2.GetValue(i)
		if err := result.Append(types.CompareUuid(v1, v2) > 0, null1 || null2); err != nil {
			return err
		}
	}
	return nil
}

func valueDec64GreatThan(
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64) error {
	col1 := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](params[0])
	col2 := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](params[1])

	m := col2.GetType().Scale - col1.GetType().Scale
	if m >= 0 {
		for i := uint64(0); i < length; i++ {
			v1, null1 := col1.GetValue(i)
			v2, null2 := col2.GetValue(i)
			if null1 || null2 {
				if err := result.Append(false, true); err != nil {
					return err
				}
			} else {
				x, _ := v1.Scale(m)
				if err := result.Append(x.Compare(v2) > 0, false); err != nil {
					return err
				}
			}
		}
	} else {
		for i := uint64(0); i < length; i++ {
			v1, null1 := col1.GetValue(i)
			v2, null2 := col2.GetValue(i)
			if null1 || null2 {
				if err := result.Append(false, true); err != nil {
					return err
				}
			} else {
				y, _ := v2.Scale(-m)
				if err := result.Append(v1.Compare(y) > 0, false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func valueDec128GreatThan(
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64) error {
	col1 := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](params[0])
	col2 := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](params[1])

	m := col2.GetType().Scale - col1.GetType().Scale
	if m >= 0 {
		for i := uint64(0); i < length; i++ {
			v1, null1 := col1.GetValue(i)
			v2, null2 := col2.GetValue(i)
			if null1 || null2 {
				if err := result.Append(false, true); err != nil {
					return err
				}
			} else {
				x, _ := v1.Scale(m)
				if err := result.Append(x.Compare(v2) > 0, false); err != nil {
					return err
				}
			}
		}
	} else {
		for i := uint64(0); i < length; i++ {
			v1, null1 := col1.GetValue(i)
			v2, null2 := col2.GetValue(i)
			if null1 || null2 {
				if err := result.Append(false, true); err != nil {
					return err
				}
			} else {
				y, _ := v2.Scale(-m)
				if err := result.Append(v1.Compare(y) > 0, false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func valueStrGreatThan(
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64) error {
	col1 := vector.GenerateFunctionStrParameter(params[0])
	col2 := vector.GenerateFunctionStrParameter(params[1])
	for i := uint64(0); i < length; i++ {
		v1, null1 := col1.GetStrValue(i)
		v2, null2 := col2.GetStrValue(i)
		if null1 || null2 {
			if err := result.Append(false, true); err != nil {
				return err
			}
		} else {
			if err := result.Append(bytes.Compare(v1, v2) > 0, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func greatEqualFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[bool](result)
	switch paramType.Oid {
	case types.T_bool:
		return valueBoolGreatEqual(parameters, rs, uint64(length))
	case types.T_int8:
		return valueGreatEqual1[int8](parameters, rs, uint64(length))
	case types.T_int16:
		return valueGreatEqual1[int16](parameters, rs, uint64(length))
	case types.T_int32:
		return valueGreatEqual1[int32](parameters, rs, uint64(length))
	case types.T_int64:
		return valueGreatEqual1[int64](parameters, rs, uint64(length))
	case types.T_uint8:
		return valueGreatEqual1[uint8](parameters, rs, uint64(length))
	case types.T_uint16:
		return valueGreatEqual1[uint16](parameters, rs, uint64(length))
	case types.T_uint32:
		return valueGreatEqual1[uint32](parameters, rs, uint64(length))
	case types.T_uint64:
		return valueGreatEqual1[uint64](parameters, rs, uint64(length))
	case types.T_uuid:
		return valueUuidGreatEqual(parameters, rs, uint64(length))
	case types.T_float32:
		return valueGreatEqual1[float32](parameters, rs, uint64(length))
	case types.T_float64:
		return valueGreatEqual1[float64](parameters, rs, uint64(length))
	case types.T_char, types.T_varchar, types.T_blob, types.T_json, types.T_text:
		return valueStrGreatEqual(parameters, rs, uint64(length))
	case types.T_binary, types.T_varbinary:
		return valueStrGreatEqual(parameters, rs, uint64(length))
	case types.T_date:
		return valueGreatEqual1[types.Date](parameters, rs, uint64(length))
	case types.T_datetime:
		return valueGreatEqual1[types.Datetime](parameters, rs, uint64(length))
	case types.T_time:
		return valueGreatEqual1[types.Time](parameters, rs, uint64(length))
	case types.T_timestamp:
		return valueGreatEqual1[types.Timestamp](parameters, rs, uint64(length))
	case types.T_decimal64:
		return valueDec64GreatEqual(parameters, rs, uint64(length))
	case types.T_decimal128:
		return valueDec128GreatEqual(parameters, rs, uint64(length))
	}
	panic("unreached code")
}

func valueGreatEqual1[T constraints.Integer | constraints.Float |
	types.Date | types.Datetime | types.Time | types.Timestamp](
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64) error {
	col1 := vector.GenerateFunctionFixedTypeParameter[T](params[0])
	col2 := vector.GenerateFunctionFixedTypeParameter[T](params[1])
	for i := uint64(0); i < length; i++ {
		v1, null1 := col1.GetValue(i)
		v2, null2 := col2.GetValue(i)
		if err := result.Append(v1 >= v2, null1 || null2); err != nil {
			return err
		}
	}
	return nil
}

func valueBoolGreatEqual(
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64) error {
	f := func(x, y bool) bool {
		return x || !y
	}

	col1 := vector.GenerateFunctionFixedTypeParameter[bool](params[0])
	col2 := vector.GenerateFunctionFixedTypeParameter[bool](params[1])
	for i := uint64(0); i < length; i++ {
		v1, null1 := col1.GetValue(i)
		v2, null2 := col2.GetValue(i)
		if err := result.Append(f(v1, v2), null1 || null2); err != nil {
			return err
		}
	}
	return nil
}

func valueUuidGreatEqual(
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64) error {
	col1 := vector.GenerateFunctionFixedTypeParameter[types.Uuid](params[0])
	col2 := vector.GenerateFunctionFixedTypeParameter[types.Uuid](params[1])
	for i := uint64(0); i < length; i++ {
		v1, null1 := col1.GetValue(i)
		v2, null2 := col2.GetValue(i)
		if err := result.Append(types.CompareUuid(v1, v2) >= 0, null1 || null2); err != nil {
			return err
		}
	}
	return nil
}

func valueDec64GreatEqual(
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64) error {
	col1 := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](params[0])
	col2 := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](params[1])

	m := col2.GetType().Scale - col1.GetType().Scale
	if m >= 0 {
		for i := uint64(0); i < length; i++ {
			v1, null1 := col1.GetValue(i)
			v2, null2 := col2.GetValue(i)
			if null1 || null2 {
				if err := result.Append(false, true); err != nil {
					return err
				}
			} else {
				x, _ := v1.Scale(m)
				if err := result.Append(x.Compare(v2) >= 0, false); err != nil {
					return err
				}
			}
		}
	} else {
		for i := uint64(0); i < length; i++ {
			v1, null1 := col1.GetValue(i)
			v2, null2 := col2.GetValue(i)
			if null1 || null2 {
				if err := result.Append(false, true); err != nil {
					return err
				}
			} else {
				y, _ := v2.Scale(-m)
				if err := result.Append(v1.Compare(y) >= 0, false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func valueDec128GreatEqual(
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64) error {
	col1 := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](params[0])
	col2 := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](params[1])

	m := col2.GetType().Scale - col1.GetType().Scale
	if m >= 0 {
		for i := uint64(0); i < length; i++ {
			v1, null1 := col1.GetValue(i)
			v2, null2 := col2.GetValue(i)
			if null1 || null2 {
				if err := result.Append(false, true); err != nil {
					return err
				}
			} else {
				x, _ := v1.Scale(m)
				if err := result.Append(x.Compare(v2) >= 0, false); err != nil {
					return err
				}
			}
		}
	} else {
		for i := uint64(0); i < length; i++ {
			v1, null1 := col1.GetValue(i)
			v2, null2 := col2.GetValue(i)
			if null1 || null2 {
				if err := result.Append(false, true); err != nil {
					return err
				}
			} else {
				y, _ := v2.Scale(-m)
				if err := result.Append(v1.Compare(y) >= 0, false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func valueStrGreatEqual(
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64) error {
	col1 := vector.GenerateFunctionStrParameter(params[0])
	col2 := vector.GenerateFunctionStrParameter(params[1])
	for i := uint64(0); i < length; i++ {
		v1, null1 := col1.GetStrValue(i)
		v2, null2 := col2.GetStrValue(i)
		if null1 || null2 {
			if err := result.Append(false, true); err != nil {
				return err
			}
		} else {
			if err := result.Append(bytes.Compare(v1, v2) >= 0, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func notEqualFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[bool](result)
	switch paramType.Oid {
	case types.T_bool:
		return valueNotEquals[bool](parameters, rs, uint64(length))
	case types.T_int8:
		return valueNotEquals[int8](parameters, rs, uint64(length))
	case types.T_int16:
		return valueNotEquals[int16](parameters, rs, uint64(length))
	case types.T_int32:
		return valueNotEquals[int32](parameters, rs, uint64(length))
	case types.T_int64:
		return valueNotEquals[int64](parameters, rs, uint64(length))
	case types.T_uint8:
		return valueNotEquals[uint8](parameters, rs, uint64(length))
	case types.T_uint16:
		return valueNotEquals[uint16](parameters, rs, uint64(length))
	case types.T_uint32:
		return valueNotEquals[uint32](parameters, rs, uint64(length))
	case types.T_uint64:
		return valueNotEquals[uint64](parameters, rs, uint64(length))
	case types.T_uuid:
		return valueNotEquals[types.Uuid](parameters, rs, uint64(length))
	case types.T_float32:
		return valueNotEquals[float32](parameters, rs, uint64(length))
	case types.T_float64:
		return valueNotEquals[float64](parameters, rs, uint64(length))
	case types.T_char, types.T_varchar, types.T_blob, types.T_json, types.T_text:
		return valueStrNotEquals(parameters, rs, uint64(length))
	case types.T_binary, types.T_varbinary:
		return valueStrNotEquals(parameters, rs, uint64(length))
	case types.T_date:
		return valueNotEquals[types.Date](parameters, rs, uint64(length))
	case types.T_datetime:
		return valueNotEquals[types.Datetime](parameters, rs, uint64(length))
	case types.T_time:
		return valueNotEquals[types.Time](parameters, rs, uint64(length))
	case types.T_timestamp:
		return valueNotEquals[types.Timestamp](parameters, rs, uint64(length))
	case types.T_decimal64:
		return valueDec64NotEquals(parameters, rs, uint64(length))
	case types.T_decimal128:
		return valueDec128NotEquals(parameters, rs, uint64(length))
	}
	panic("unreached code")
}

func valueNotEquals[T bool | constraints.Integer | constraints.Float |
	types.Date | types.Datetime | types.Time | types.Timestamp | types.Uuid](
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64) error {
	col1 := vector.GenerateFunctionFixedTypeParameter[T](params[0])
	col2 := vector.GenerateFunctionFixedTypeParameter[T](params[1])
	for i := uint64(0); i < length; i++ {
		v1, null1 := col1.GetValue(i)
		v2, null2 := col2.GetValue(i)
		if err := result.Append(v1 != v2, null1 || null2); err != nil {
			return err
		}
	}
	return nil
}

func valueStrNotEquals(
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64) error {
	col1 := vector.GenerateFunctionStrParameter(params[0])
	col2 := vector.GenerateFunctionStrParameter(params[1])
	for i := uint64(0); i < length; i++ {
		v1, null1 := col1.GetStrValue(i)
		v2, null2 := col2.GetStrValue(i)
		if err := result.Append(!bytes.Equal(v1, v2), null1 || null2); err != nil {
			return err
		}
	}
	return nil
}

func valueDec64NotEquals(
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64) error {
	col1 := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](params[0])
	col2 := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](params[1])

	m := col2.GetType().Scale - col1.GetType().Scale
	if m >= 0 {
		for i := uint64(0); i < length; i++ {
			v1, null1 := col1.GetValue(i)
			v2, null2 := col2.GetValue(i)
			if null1 || null2 {
				if err := result.Append(false, true); err != nil {
					return err
				}
			} else {
				x, _ := v1.Scale(m)
				if err := result.Append(x != v2, false); err != nil {
					return err
				}
			}
		}
	} else {
		for i := uint64(0); i < length; i++ {
			v1, null1 := col1.GetValue(i)
			v2, null2 := col2.GetValue(i)
			if null1 || null2 {
				if err := result.Append(false, true); err != nil {
					return err
				}
			} else {
				y, _ := v2.Scale(-m)
				if err := result.Append(v1 != y, false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func valueDec128NotEquals(
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64) error {
	col1 := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](params[0])
	col2 := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](params[1])

	m := col2.GetType().Scale - col1.GetType().Scale
	if m >= 0 {
		for i := uint64(0); i < length; i++ {
			v1, null1 := col1.GetValue(i)
			v2, null2 := col2.GetValue(i)
			if null1 || null2 {
				if err := result.Append(false, true); err != nil {
					return err
				}
			} else {
				x, _ := v1.Scale(m)
				if err := result.Append(x != v2, false); err != nil {
					return err
				}
			}
		}
	} else {
		for i := uint64(0); i < length; i++ {
			v1, null1 := col1.GetValue(i)
			v2, null2 := col2.GetValue(i)
			if null1 || null2 {
				if err := result.Append(false, true); err != nil {
					return err
				}
			} else {
				y, _ := v2.Scale(-m)
				if err := result.Append(v1 != y, false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func lessThanFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[bool](result)
	switch paramType.Oid {
	case types.T_bool:
		return valueBoolLessThan(parameters, rs, uint64(length))
	case types.T_int8:
		return valueLessThan1[int8](parameters, rs, uint64(length))
	case types.T_int16:
		return valueLessThan1[int16](parameters, rs, uint64(length))
	case types.T_int32:
		return valueLessThan1[int32](parameters, rs, uint64(length))
	case types.T_int64:
		return valueLessThan1[int64](parameters, rs, uint64(length))
	case types.T_uint8:
		return valueLessThan1[uint8](parameters, rs, uint64(length))
	case types.T_uint16:
		return valueLessThan1[uint16](parameters, rs, uint64(length))
	case types.T_uint32:
		return valueLessThan1[uint32](parameters, rs, uint64(length))
	case types.T_uint64:
		return valueLessThan1[uint64](parameters, rs, uint64(length))
	case types.T_uuid:
		return valueUuidLessThan(parameters, rs, uint64(length))
	case types.T_float32:
		return valueLessThan1[float32](parameters, rs, uint64(length))
	case types.T_float64:
		return valueLessThan1[float64](parameters, rs, uint64(length))
	case types.T_char, types.T_varchar, types.T_blob, types.T_json, types.T_text:
		return valueStrLessThan(parameters, rs, uint64(length))
	case types.T_binary, types.T_varbinary:
		return valueStrLessThan(parameters, rs, uint64(length))
	case types.T_date:
		return valueLessThan1[types.Date](parameters, rs, uint64(length))
	case types.T_datetime:
		return valueLessThan1[types.Datetime](parameters, rs, uint64(length))
	case types.T_time:
		return valueLessThan1[types.Time](parameters, rs, uint64(length))
	case types.T_timestamp:
		return valueLessThan1[types.Timestamp](parameters, rs, uint64(length))
	case types.T_decimal64:
		return valueDec64LessThan(parameters, rs, uint64(length))
	case types.T_decimal128:
		return valueDec128LessThan(parameters, rs, uint64(length))
	}
	panic("unreached code")
}

func valueLessThan1[T constraints.Integer | constraints.Float |
	types.Date | types.Datetime | types.Time | types.Timestamp](
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64) error {
	col1 := vector.GenerateFunctionFixedTypeParameter[T](params[0])
	col2 := vector.GenerateFunctionFixedTypeParameter[T](params[1])
	for i := uint64(0); i < length; i++ {
		v1, null1 := col1.GetValue(i)
		v2, null2 := col2.GetValue(i)
		if err := result.Append(v1 < v2, null1 || null2); err != nil {
			return err
		}
	}
	return nil
}

func valueBoolLessThan(
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64) error {
	f := func(x, y bool) bool {
		return !x && y
	}

	col1 := vector.GenerateFunctionFixedTypeParameter[bool](params[0])
	col2 := vector.GenerateFunctionFixedTypeParameter[bool](params[1])
	for i := uint64(0); i < length; i++ {
		v1, null1 := col1.GetValue(i)
		v2, null2 := col2.GetValue(i)
		if err := result.Append(f(v1, v2), null1 || null2); err != nil {
			return err
		}
	}
	return nil
}

func valueUuidLessThan(
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64) error {
	col1 := vector.GenerateFunctionFixedTypeParameter[types.Uuid](params[0])
	col2 := vector.GenerateFunctionFixedTypeParameter[types.Uuid](params[1])
	for i := uint64(0); i < length; i++ {
		v1, null1 := col1.GetValue(i)
		v2, null2 := col2.GetValue(i)
		if err := result.Append(types.CompareUuid(v1, v2) < 0, null1 || null2); err != nil {
			return err
		}
	}
	return nil
}

func valueDec64LessThan(
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64) error {
	col1 := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](params[0])
	col2 := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](params[1])

	m := col2.GetType().Scale - col1.GetType().Scale
	if m >= 0 {
		for i := uint64(0); i < length; i++ {
			v1, null1 := col1.GetValue(i)
			v2, null2 := col2.GetValue(i)
			if null1 || null2 {
				if err := result.Append(false, true); err != nil {
					return err
				}
			} else {
				x, _ := v1.Scale(m)
				if err := result.Append(x.Compare(v2) < 0, false); err != nil {
					return err
				}
			}
		}
	} else {
		for i := uint64(0); i < length; i++ {
			v1, null1 := col1.GetValue(i)
			v2, null2 := col2.GetValue(i)
			if null1 || null2 {
				if err := result.Append(false, true); err != nil {
					return err
				}
			} else {
				y, _ := v2.Scale(-m)
				if err := result.Append(v1.Compare(y) < 0, false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func valueDec128LessThan(
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64) error {
	col1 := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](params[0])
	col2 := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](params[1])

	m := col2.GetType().Scale - col1.GetType().Scale
	if m >= 0 {
		for i := uint64(0); i < length; i++ {
			v1, null1 := col1.GetValue(i)
			v2, null2 := col2.GetValue(i)
			if null1 || null2 {
				if err := result.Append(false, true); err != nil {
					return err
				}
			} else {
				x, _ := v1.Scale(m)
				if err := result.Append(x.Compare(v2) < 0, false); err != nil {
					return err
				}
			}
		}
	} else {
		for i := uint64(0); i < length; i++ {
			v1, null1 := col1.GetValue(i)
			v2, null2 := col2.GetValue(i)
			if null1 || null2 {
				if err := result.Append(false, true); err != nil {
					return err
				}
			} else {
				y, _ := v2.Scale(-m)
				if err := result.Append(v1.Compare(y) < 0, false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func valueStrLessThan(
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64) error {
	col1 := vector.GenerateFunctionStrParameter(params[0])
	col2 := vector.GenerateFunctionStrParameter(params[1])
	for i := uint64(0); i < length; i++ {
		v1, null1 := col1.GetStrValue(i)
		v2, null2 := col2.GetStrValue(i)
		if null1 || null2 {
			if err := result.Append(false, true); err != nil {
				return err
			}
		} else {
			if err := result.Append(bytes.Compare(v1, v2) < 0, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func lessEqualFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[bool](result)
	switch paramType.Oid {
	case types.T_bool:
		return valueBoolLessEqual(parameters, rs, uint64(length))
	case types.T_int8:
		return valueLessEqual[int8](parameters, rs, uint64(length))
	case types.T_int16:
		return valueLessEqual[int16](parameters, rs, uint64(length))
	case types.T_int32:
		return valueLessEqual[int32](parameters, rs, uint64(length))
	case types.T_int64:
		return valueLessEqual[int64](parameters, rs, uint64(length))
	case types.T_uint8:
		return valueLessEqual[uint8](parameters, rs, uint64(length))
	case types.T_uint16:
		return valueLessEqual[uint16](parameters, rs, uint64(length))
	case types.T_uint32:
		return valueLessEqual[uint32](parameters, rs, uint64(length))
	case types.T_uint64:
		return valueLessEqual[uint64](parameters, rs, uint64(length))
	case types.T_uuid:
		return valueUuidLessEqual(parameters, rs, uint64(length))
	case types.T_float32:
		return valueLessEqual[float32](parameters, rs, uint64(length))
	case types.T_float64:
		return valueLessEqual[float64](parameters, rs, uint64(length))
	case types.T_char, types.T_varchar, types.T_blob, types.T_json, types.T_text:
		return valueStrLessEqual(parameters, rs, uint64(length))
	case types.T_binary, types.T_varbinary:
		return valueStrLessEqual(parameters, rs, uint64(length))
	case types.T_date:
		return valueLessEqual[types.Date](parameters, rs, uint64(length))
	case types.T_datetime:
		return valueLessEqual[types.Datetime](parameters, rs, uint64(length))
	case types.T_time:
		return valueLessEqual[types.Time](parameters, rs, uint64(length))
	case types.T_timestamp:
		return valueLessEqual[types.Timestamp](parameters, rs, uint64(length))
	case types.T_decimal64:
		return valueDec64LessEqual(parameters, rs, uint64(length))
	case types.T_decimal128:
		return valueDec128LessEqual(parameters, rs, uint64(length))
	}
	panic("unreached code")
}

func valueLessEqual[T constraints.Integer | constraints.Float |
	types.Date | types.Datetime | types.Time | types.Timestamp](
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64) error {
	col1 := vector.GenerateFunctionFixedTypeParameter[T](params[0])
	col2 := vector.GenerateFunctionFixedTypeParameter[T](params[1])
	for i := uint64(0); i < length; i++ {
		v1, null1 := col1.GetValue(i)
		v2, null2 := col2.GetValue(i)
		if err := result.Append(v1 <= v2, null1 || null2); err != nil {
			return err
		}
	}
	return nil
}

func valueBoolLessEqual(
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64) error {
	f := func(x, y bool) bool {
		return !x || y
	}

	col1 := vector.GenerateFunctionFixedTypeParameter[bool](params[0])
	col2 := vector.GenerateFunctionFixedTypeParameter[bool](params[1])
	for i := uint64(0); i < length; i++ {
		v1, null1 := col1.GetValue(i)
		v2, null2 := col2.GetValue(i)
		if err := result.Append(f(v1, v2), null1 || null2); err != nil {
			return err
		}
	}
	return nil
}

func valueUuidLessEqual(
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64) error {
	col1 := vector.GenerateFunctionFixedTypeParameter[types.Uuid](params[0])
	col2 := vector.GenerateFunctionFixedTypeParameter[types.Uuid](params[1])
	for i := uint64(0); i < length; i++ {
		v1, null1 := col1.GetValue(i)
		v2, null2 := col2.GetValue(i)
		if err := result.Append(types.CompareUuid(v1, v2) <= 0, null1 || null2); err != nil {
			return err
		}
	}
	return nil
}

func valueDec64LessEqual(
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64) error {
	col1 := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](params[0])
	col2 := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](params[1])

	m := col2.GetType().Scale - col1.GetType().Scale
	if m >= 0 {
		for i := uint64(0); i < length; i++ {
			v1, null1 := col1.GetValue(i)
			v2, null2 := col2.GetValue(i)
			if null1 || null2 {
				if err := result.Append(false, true); err != nil {
					return err
				}
			} else {
				x, _ := v1.Scale(m)
				if err := result.Append(x.Compare(v2) <= 0, false); err != nil {
					return err
				}
			}
		}
	} else {
		for i := uint64(0); i < length; i++ {
			v1, null1 := col1.GetValue(i)
			v2, null2 := col2.GetValue(i)
			if null1 || null2 {
				if err := result.Append(false, true); err != nil {
					return err
				}
			} else {
				y, _ := v2.Scale(-m)
				if err := result.Append(v1.Compare(y) <= 0, false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func valueDec128LessEqual(
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64) error {
	col1 := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](params[0])
	col2 := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](params[1])

	m := col2.GetType().Scale - col1.GetType().Scale
	if m >= 0 {
		for i := uint64(0); i < length; i++ {
			v1, null1 := col1.GetValue(i)
			v2, null2 := col2.GetValue(i)
			if null1 || null2 {
				if err := result.Append(false, true); err != nil {
					return err
				}
			} else {
				x, _ := v1.Scale(m)
				if err := result.Append(x.Compare(v2) <= 0, false); err != nil {
					return err
				}
			}
		}
	} else {
		for i := uint64(0); i < length; i++ {
			v1, null1 := col1.GetValue(i)
			v2, null2 := col2.GetValue(i)
			if null1 || null2 {
				if err := result.Append(false, true); err != nil {
					return err
				}
			} else {
				y, _ := v2.Scale(-m)
				if err := result.Append(v1.Compare(y) <= 0, false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func valueStrLessEqual(
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64) error {
	col1 := vector.GenerateFunctionStrParameter(params[0])
	col2 := vector.GenerateFunctionStrParameter(params[1])
	for i := uint64(0); i < length; i++ {
		v1, null1 := col1.GetStrValue(i)
		v2, null2 := col2.GetStrValue(i)
		if null1 || null2 {
			if err := result.Append(false, true); err != nil {
				return err
			}
		} else {
			if err := result.Append(bytes.Compare(v1, v2) <= 0, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func operatorOpInt64Fn(
	parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int,
	fn func(int64, int64) int64) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[int64](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[int64](parameters[1])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if err := rs.Append(fn(v1, v2), null1 || null2); err != nil {
			return err
		}
	}
	return nil
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

func operatorOpBitAndInt64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return operatorOpInt64Fn(parameters, result, proc, length, func(i int64, i2 int64) int64 {
		return i & i2
	})
}

func operatorOpBitAndStrFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
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

func operatorOpBitXorInt64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return operatorOpInt64Fn(parameters, result, proc, length, func(i int64, i2 int64) int64 {
		return i ^ i2
	})
}

func operatorOpBitXorStrFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
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

func operatorOpBitOrInt64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return operatorOpInt64Fn(parameters, result, proc, length, func(i int64, i2 int64) int64 {
		return i | i2
	})
}

func operatorOpBitOrStrFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
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

func operatorOpBitShiftLeftInt64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return operatorOpInt64Fn(parameters, result, proc, length, func(i int64, i2 int64) int64 {
		if i2 < 0 {
			return 0
		}
		return i << i2
	})
}

func operatorOpBitShiftRightInt64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return operatorOpInt64Fn(parameters, result, proc, length, func(i int64, i2 int64) int64 {
		if i2 < 0 {
			return 0
		}
		return i >> i2
	})
}
