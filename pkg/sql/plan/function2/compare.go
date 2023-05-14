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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
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
		return valueCompare[bool](parameters, rs, uint64(length), func(a, b bool) bool {
			return a == b
		})
	case types.T_int8:
		return valueCompare[int8](parameters, rs, uint64(length), func(a, b int8) bool {
			return a == b
		})
	case types.T_int16:
		return valueCompare[int16](parameters, rs, uint64(length), func(a, b int16) bool {
			return a == b
		})
	case types.T_int32:
		return valueCompare[int32](parameters, rs, uint64(length), func(a, b int32) bool {
			return a == b
		})
	case types.T_int64:
		return valueCompare[int64](parameters, rs, uint64(length), func(a, b int64) bool {
			return a == b
		})
	case types.T_uint8:
		return valueCompare[uint8](parameters, rs, uint64(length), func(a, b uint8) bool {
			return a == b
		})
	case types.T_uint16:
		return valueCompare[uint16](parameters, rs, uint64(length), func(a, b uint16) bool {
			return a == b
		})
	case types.T_uint32:
		return valueCompare[uint32](parameters, rs, uint64(length), func(a, b uint32) bool {
			return a == b
		})
	case types.T_uint64:
		return valueCompare[uint64](parameters, rs, uint64(length), func(a, b uint64) bool {
			return a == b
		})
	case types.T_uuid:
		return valueCompare[types.Uuid](parameters, rs, uint64(length), func(a, b types.Uuid) bool {
			return a == b
		})
	case types.T_float32:
		return valueCompare[float32](parameters, rs, uint64(length), func(a, b float32) bool {
			return a == b
		})
	case types.T_float64:
		return valueCompare[float64](parameters, rs, uint64(length), func(a, b float64) bool {
			return a == b
		})
	case types.T_char, types.T_varchar, types.T_blob, types.T_json, types.T_text:
		return valueStrCompare(parameters, rs, uint64(length), bytes.Equal)
	case types.T_binary, types.T_varbinary:
		return valueStrCompare(parameters, rs, uint64(length), bytes.Equal)
	case types.T_date:
		return valueCompare[types.Date](parameters, rs, uint64(length), func(a, b types.Date) bool {
			return a == b
		})
	case types.T_datetime:
		return valueCompare[types.Datetime](parameters, rs, uint64(length), func(a, b types.Datetime) bool {
			return a == b
		})
	case types.T_time:
		return valueCompare[types.Time](parameters, rs, uint64(length), func(a, b types.Time) bool {
			return a == b
		})
	case types.T_timestamp:
		return valueCompare[types.Timestamp](parameters, rs, uint64(length), func(a, b types.Timestamp) bool {
			return a == b
		})
	case types.T_decimal64:
		return valueDec64Compare(parameters, rs, uint64(length), func(a, b types.Decimal64) bool {
			return a == b
		})
	case types.T_decimal128:
		return valueDec128Compare(parameters, rs, uint64(length), func(a, b types.Decimal128) bool {
			return a == b
		})
	}
	panic("unreached code")
}

func valueCompare[T bool | constraints.Integer | constraints.Float |
	types.Date | types.Datetime | types.Time | types.Timestamp | types.Uuid](
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64,
	cmpFn func(a, b T) bool) error {
	col1 := vector.GenerateFunctionFixedTypeParameter[T](params[0])
	col2 := vector.GenerateFunctionFixedTypeParameter[T](params[1])

	if params[0].IsConst() {
		v1, null1 := col1.GetValue(0)
		if null1 {
			for i := uint64(0); i < length; i++ {
				if err := result.Append(false, true); err != nil {
					return err
				}
			}
		} else {
			for i := uint64(0); i < length; i++ {
				v2, null2 := col2.GetValue(i)
				if null2 {
					if err := result.Append(false, true); err != nil {
						return err
					}
				} else {
					if err := result.Append(cmpFn(v1, v2), false); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}

	if params[1].IsConst() {
		v2, null2 := col2.GetValue(0)
		if null2 {
			for i := uint64(0); i < length; i++ {
				if err := result.Append(false, true); err != nil {
					return err
				}
			}
		} else {
			for i := uint64(0); i < length; i++ {
				v1, null1 := col1.GetValue(i)
				if null1 {
					if err := result.Append(false, true); err != nil {
						return err
					}
				} else {
					if err := result.Append(cmpFn(v1, v2), false); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}

	for i := uint64(0); i < length; i++ {
		v1, null1 := col1.GetValue(i)
		v2, null2 := col2.GetValue(i)
		if err := result.Append(cmpFn(v1, v2), null1 || null2); err != nil {
			return err
		}
	}
	return nil
}

func valueStrCompare(
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64,
	cmpFn func(a, b []byte) bool) error {
	col1 := vector.GenerateFunctionStrParameter(params[0])
	col2 := vector.GenerateFunctionStrParameter(params[1])

	if params[0].IsConst() {
		v1, null1 := col1.GetStrValue(0)
		if null1 {
			for i := uint64(0); i < length; i++ {
				if err := result.Append(false, true); err != nil {
					return err
				}
			}
		} else {
			for i := uint64(0); i < length; i++ {
				v2, null2 := col2.GetStrValue(i)
				if null2 {
					if err := result.Append(false, true); err != nil {
						return err
					}
				} else {
					if err := result.Append(cmpFn(v1, v2), false); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}

	if params[1].IsConst() {
		v2, null2 := col2.GetStrValue(0)
		if null2 {
			for i := uint64(0); i < length; i++ {
				if err := result.Append(false, true); err != nil {
					return err
				}
			}
		} else {
			for i := uint64(0); i < length; i++ {
				v1, null1 := col1.GetStrValue(i)
				if null1 {
					if err := result.Append(false, true); err != nil {
						return err
					}
				} else {
					if err := result.Append(cmpFn(v1, v2), false); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}

	for i := uint64(0); i < length; i++ {
		v1, null1 := col1.GetStrValue(i)
		v2, null2 := col2.GetStrValue(i)
		if null1 || null2 {
			if err := result.Append(false, true); err != nil {
				return err
			}
		} else {
			if err := result.Append(cmpFn(v1, v2), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func valueDec64Compare(
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64,
	cmpFn func(a, b types.Decimal64) bool) error {
	col1 := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](params[0])
	col2 := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](params[1])

	m := col2.GetType().Scale - col1.GetType().Scale

	rsVec := result.GetResultVector()
	rss := vector.MustFixedCol[bool](rsVec)

	if params[0].IsConst() && params[1].IsConst() {
		v1, null1 := col1.GetValue(0)
		v2, null2 := col2.GetValue(0)
		if null1 || null2 {
			nulls.AddRange(rsVec.GetNulls(), 0, length)
		} else {
			if m >= 0 {
				x, _ := v1.Scale(m)
				for i := uint64(0); i < length; i++ {
					rss[i] = cmpFn(x, v2)
				}
			} else {
				y, _ := v2.Scale(-m)
				for i := uint64(0); i < length; i++ {
					rss[i] = cmpFn(v1, y)
				}
			}
		}
		return nil
	}

	if params[0].IsConst() {
		v1, null1 := col1.GetValue(0)
		if null1 {
			nulls.AddRange(rsVec.GetNulls(), 0, length)
		} else {
			if m >= 0 {
				x, _ := v1.Scale(m)
				if col2.WithAnyNullValue() {
					rsVec.GetNulls().Or(params[1].GetNulls())
					for i := uint64(0); i < length; i++ {
						v2, null2 := col2.GetValue(i)
						if null2 {
							continue
						}
						rss[i] = cmpFn(x, v2)
					}
				} else {
					for i := uint64(0); i < length; i++ {
						v2, _ := col2.GetValue(i)
						rss[i] = cmpFn(x, v2)
					}
				}
			} else {
				if col2.WithAnyNullValue() {
					rsVec.GetNulls().Or(params[1].GetNulls())
					for i := uint64(0); i < length; i++ {
						v2, null2 := col2.GetValue(i)
						if null2 {
							continue
						}
						y, _ := v2.Scale(-m)
						rss[i] = cmpFn(v1, y)
					}
				} else {
					for i := uint64(0); i < length; i++ {
						v2, _ := col2.GetValue(i)
						y, _ := v2.Scale(-m)
						rss[i] = cmpFn(v1, y)
					}
				}
			}
		}

		return nil
	}

	if params[1].IsConst() {
		v2, null2 := col2.GetValue(0)
		if null2 {
			nulls.AddRange(rsVec.GetNulls(), 0, length)
		} else {
			if m >= 0 {
				if col1.WithAnyNullValue() {
					rsVec.GetNulls().Or(params[1].GetNulls())
					for i := uint64(0); i < length; i++ {
						v1, null1 := col1.GetValue(i)
						if null1 {
							continue
						}
						x, _ := v1.Scale(m)
						rss[i] = cmpFn(x, v1)
					}
				} else {
					for i := uint64(0); i < length; i++ {
						v1, _ := col1.GetValue(i)
						x, _ := v1.Scale(m)
						rss[i] = cmpFn(x, v2)
					}
				}
			} else {
				y, _ := v2.Scale(-m)
				if col1.WithAnyNullValue() {
					rsVec.GetNulls().Or(params[1].GetNulls())
					for i := uint64(0); i < length; i++ {
						v1, null1 := col1.GetValue(i)
						if null1 {
							continue
						}
						rss[i] = cmpFn(v1, y)
					}
				} else {
					for i := uint64(0); i < length; i++ {
						v1, _ := col1.GetValue(i)
						rss[i] = cmpFn(v1, y)
					}
				}
			}
		}
		return nil
	}

	if col1.WithAnyNullValue() || col1.WithAnyNullValue() {
		nulls.Or(params[0].GetNulls(), params[1].GetNulls(), rsVec.GetNulls())

		if m >= 0 {
			for i := uint64(0); i < length; i++ {
				if rsVec.GetNulls().Contains(i) {
					continue
				}
				v1, _ := col1.GetValue(i)
				v2, _ := col2.GetValue(i)
				x, _ := v1.Scale(m)
				rss[i] = cmpFn(x, v2)
			}
		} else {
			for i := uint64(0); i < length; i++ {
				if rsVec.GetNulls().Contains(i) {
					continue
				}
				v1, _ := col1.GetValue(i)
				v2, _ := col2.GetValue(i)
				y, _ := v2.Scale(-m)
				rss[i] = cmpFn(v1, y)
			}
		}
		return nil
	}

	if m >= 0 {
		for i := uint64(0); i < length; i++ {
			v1, _ := col1.GetValue(i)
			v2, _ := col2.GetValue(i)
			x, _ := v1.Scale(m)
			rss[i] = cmpFn(x, v2)
		}
	} else {
		for i := uint64(0); i < length; i++ {
			v1, _ := col1.GetValue(i)
			v2, _ := col2.GetValue(i)
			y, _ := v2.Scale(-m)
			rss[i] = cmpFn(v1, y)
		}
	}
	return nil
}

func valueDec128Compare(
	params []*vector.Vector, result *vector.FunctionResult[bool], length uint64,
	cmpFn func(a, b types.Decimal128) bool) error {
	col1 := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](params[0])
	col2 := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](params[1])

	m := col2.GetType().Scale - col1.GetType().Scale

	rsVec := result.GetResultVector()
	rss := vector.MustFixedCol[bool](rsVec)

	if params[0].IsConst() && params[1].IsConst() {
		v1, null1 := col1.GetValue(0)
		v2, null2 := col2.GetValue(0)
		if null1 || null2 {
			nulls.AddRange(rsVec.GetNulls(), 0, length)
		} else {
			if m >= 0 {
				x, _ := v1.Scale(m)
				for i := uint64(0); i < length; i++ {
					rss[i] = cmpFn(x, v2)
				}
			} else {
				y, _ := v2.Scale(-m)
				for i := uint64(0); i < length; i++ {
					rss[i] = cmpFn(v1, y)
				}
			}
		}
		return nil
	}

	if params[0].IsConst() {
		v1, null1 := col1.GetValue(0)
		if null1 {
			nulls.AddRange(rsVec.GetNulls(), 0, length)
		} else {
			if m >= 0 {
				x, _ := v1.Scale(m)
				if col2.WithAnyNullValue() {
					rsVec.GetNulls().Or(params[1].GetNulls())
					for i := uint64(0); i < length; i++ {
						v2, null2 := col2.GetValue(i)
						if null2 {
							continue
						}
						rss[i] = cmpFn(x, v2)
					}
				} else {
					for i := uint64(0); i < length; i++ {
						v2, _ := col2.GetValue(i)
						rss[i] = cmpFn(x, v2)
					}
				}
			} else {
				if col2.WithAnyNullValue() {
					rsVec.GetNulls().Or(params[1].GetNulls())
					for i := uint64(0); i < length; i++ {
						v2, null2 := col2.GetValue(i)
						if null2 {
							continue
						}
						y, _ := v2.Scale(-m)
						rss[i] = cmpFn(v1, y)
					}
				} else {
					for i := uint64(0); i < length; i++ {
						v2, _ := col2.GetValue(i)
						y, _ := v2.Scale(-m)
						rss[i] = cmpFn(v1, y)
					}
				}
			}
		}

		return nil
	}

	if params[1].IsConst() {
		v2, null2 := col2.GetValue(0)
		if null2 {
			nulls.AddRange(rsVec.GetNulls(), 0, length)
		} else {
			if m >= 0 {
				if col1.WithAnyNullValue() {
					rsVec.GetNulls().Or(params[1].GetNulls())
					for i := uint64(0); i < length; i++ {
						v1, null1 := col1.GetValue(i)
						if null1 {
							continue
						}
						x, _ := v1.Scale(m)
						rss[i] = cmpFn(x, v1)
					}
				} else {
					for i := uint64(0); i < length; i++ {
						v1, _ := col1.GetValue(i)
						x, _ := v1.Scale(m)
						rss[i] = cmpFn(x, v2)
					}
				}
			} else {
				y, _ := v2.Scale(-m)
				if col1.WithAnyNullValue() {
					rsVec.GetNulls().Or(params[1].GetNulls())
					for i := uint64(0); i < length; i++ {
						v1, null1 := col1.GetValue(i)
						if null1 {
							continue
						}
						rss[i] = cmpFn(v1, y)
					}
				} else {
					for i := uint64(0); i < length; i++ {
						v1, _ := col1.GetValue(i)
						rss[i] = cmpFn(v1, y)
					}
				}
			}
		}
		return nil
	}

	if col1.WithAnyNullValue() || col1.WithAnyNullValue() {
		nulls.Or(params[0].GetNulls(), params[1].GetNulls(), rsVec.GetNulls())

		if m >= 0 {
			for i := uint64(0); i < length; i++ {
				if rsVec.GetNulls().Contains(i) {
					continue
				}
				v1, _ := col1.GetValue(i)
				v2, _ := col2.GetValue(i)
				x, _ := v1.Scale(m)
				rss[i] = cmpFn(x, v2)
			}
		} else {
			for i := uint64(0); i < length; i++ {
				if rsVec.GetNulls().Contains(i) {
					continue
				}
				v1, _ := col1.GetValue(i)
				v2, _ := col2.GetValue(i)
				y, _ := v2.Scale(-m)
				rss[i] = cmpFn(v1, y)
			}
		}
		return nil
	}

	if m >= 0 {
		for i := uint64(0); i < length; i++ {
			v1, _ := col1.GetValue(i)
			v2, _ := col2.GetValue(i)
			x, _ := v1.Scale(m)
			rss[i] = cmpFn(x, v2)
		}
	} else {
		for i := uint64(0); i < length; i++ {
			v1, _ := col1.GetValue(i)
			v2, _ := col2.GetValue(i)
			y, _ := v2.Scale(-m)
			rss[i] = cmpFn(v1, y)
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
		return valueCompare[int8](parameters, rs, uint64(length), func(a, b int8) bool {
			return a > b
		})
	case types.T_int16:
		return valueCompare[int16](parameters, rs, uint64(length), func(a, b int16) bool {
			return a > b
		})
	case types.T_int32:
		return valueCompare[int32](parameters, rs, uint64(length), func(a, b int32) bool {
			return a > b
		})
	case types.T_int64:
		return valueCompare[int64](parameters, rs, uint64(length), func(a, b int64) bool {
			return a > b
		})
	case types.T_uint8:
		return valueCompare[uint8](parameters, rs, uint64(length), func(a, b uint8) bool {
			return a > b
		})
	case types.T_uint16:
		return valueCompare[uint16](parameters, rs, uint64(length), func(a, b uint16) bool {
			return a > b
		})
	case types.T_uint32:
		return valueCompare[uint32](parameters, rs, uint64(length), func(a, b uint32) bool {
			return a > b
		})
	case types.T_uint64:
		return valueCompare[uint64](parameters, rs, uint64(length), func(a, b uint64) bool {
			return a > b
		})
	case types.T_uuid:
		return valueUuidGreatThan(parameters, rs, uint64(length))
	case types.T_float32:
		return valueCompare[float32](parameters, rs, uint64(length), func(a, b float32) bool {
			return a > b
		})
	case types.T_float64:
		return valueCompare[float64](parameters, rs, uint64(length), func(a, b float64) bool {
			return a > b
		})
	case types.T_char, types.T_varchar, types.T_blob, types.T_json, types.T_text:
		return valueStrCompare(parameters, rs, uint64(length), func(a, b []byte) bool {
			return bytes.Compare(a, b) > 0
		})
	case types.T_binary, types.T_varbinary:
		return valueStrCompare(parameters, rs, uint64(length), func(a, b []byte) bool {
			return bytes.Compare(a, b) > 0
		})
	case types.T_date:
		return valueCompare[types.Date](parameters, rs, uint64(length), func(a, b types.Date) bool {
			return a > b
		})
	case types.T_datetime:
		return valueCompare[types.Datetime](parameters, rs, uint64(length), func(a, b types.Datetime) bool {
			return a > b
		})
	case types.T_time:
		return valueCompare[types.Time](parameters, rs, uint64(length), func(a, b types.Time) bool {
			return a > b
		})
	case types.T_timestamp:
		return valueCompare[types.Timestamp](parameters, rs, uint64(length), func(a, b types.Timestamp) bool {
			return a > b
		})
	case types.T_decimal64:
		return valueDec64Compare(parameters, rs, uint64(length), func(a, b types.Decimal64) bool {
			return a.Compare(b) > 0
		})
	case types.T_decimal128:
		return valueDec128Compare(parameters, rs, uint64(length), func(a, b types.Decimal128) bool {
			return a.Compare(b) > 0
		})
	}
	panic("unreached code")
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

func greatEqualFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[bool](result)
	switch paramType.Oid {
	case types.T_bool:
		return valueBoolGreatEqual(parameters, rs, uint64(length))
	case types.T_int8:
		return valueCompare[int8](parameters, rs, uint64(length), func(a, b int8) bool {
			return a >= b
		})
	case types.T_int16:
		return valueCompare[int16](parameters, rs, uint64(length), func(a, b int16) bool {
			return a >= b
		})
	case types.T_int32:
		return valueCompare[int32](parameters, rs, uint64(length), func(a, b int32) bool {
			return a >= b
		})
	case types.T_int64:
		return valueCompare[int64](parameters, rs, uint64(length), func(a, b int64) bool {
			return a >= b
		})
	case types.T_uint8:
		return valueCompare[uint8](parameters, rs, uint64(length), func(a, b uint8) bool {
			return a >= b
		})
	case types.T_uint16:
		return valueCompare[uint16](parameters, rs, uint64(length), func(a, b uint16) bool {
			return a >= b
		})
	case types.T_uint32:
		return valueCompare[uint32](parameters, rs, uint64(length), func(a, b uint32) bool {
			return a >= b
		})
	case types.T_uint64:
		return valueCompare[uint64](parameters, rs, uint64(length), func(a, b uint64) bool {
			return a >= b
		})
	case types.T_uuid:
		return valueUuidGreatEqual(parameters, rs, uint64(length))
	case types.T_float32:
		return valueCompare[float32](parameters, rs, uint64(length), func(a, b float32) bool {
			return a >= b
		})
	case types.T_float64:
		return valueCompare[float64](parameters, rs, uint64(length), func(a, b float64) bool {
			return a >= b
		})
	case types.T_char, types.T_varchar, types.T_blob, types.T_json, types.T_text:
		return valueStrCompare(parameters, rs, uint64(length), func(a, b []byte) bool {
			return bytes.Compare(a, b) >= 0
		})
	case types.T_binary, types.T_varbinary:
		return valueStrCompare(parameters, rs, uint64(length), func(a, b []byte) bool {
			return bytes.Compare(a, b) >= 0
		})
	case types.T_date:
		return valueCompare[types.Date](parameters, rs, uint64(length), func(a, b types.Date) bool {
			return a >= b
		})
	case types.T_datetime:
		return valueCompare[types.Datetime](parameters, rs, uint64(length), func(a, b types.Datetime) bool {
			return a >= b
		})
	case types.T_time:
		return valueCompare[types.Time](parameters, rs, uint64(length), func(a, b types.Time) bool {
			return a >= b
		})
	case types.T_timestamp:
		return valueCompare[types.Timestamp](parameters, rs, uint64(length), func(a, b types.Timestamp) bool {
			return a >= b
		})
	case types.T_decimal64:
		return valueDec64Compare(parameters, rs, uint64(length), func(a, b types.Decimal64) bool {
			return a.Compare(b) >= 0
		})
	case types.T_decimal128:
		return valueDec128Compare(parameters, rs, uint64(length), func(a, b types.Decimal128) bool {
			return a.Compare(b) >= 0
		})
	}
	panic("unreached code")
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

func notEqualFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[bool](result)
	switch paramType.Oid {
	case types.T_bool:
		return valueCompare[bool](parameters, rs, uint64(length), func(a, b bool) bool {
			return a != b
		})
	case types.T_int8:
		return valueCompare[int8](parameters, rs, uint64(length), func(a, b int8) bool {
			return a != b
		})
	case types.T_int16:
		return valueCompare[int16](parameters, rs, uint64(length), func(a, b int16) bool {
			return a != b
		})
	case types.T_int32:
		return valueCompare[int32](parameters, rs, uint64(length), func(a, b int32) bool {
			return a != b
		})
	case types.T_int64:
		return valueCompare[int64](parameters, rs, uint64(length), func(a, b int64) bool {
			return a != b
		})
	case types.T_uint8:
		return valueCompare[uint8](parameters, rs, uint64(length), func(a, b uint8) bool {
			return a != b
		})
	case types.T_uint16:
		return valueCompare[uint16](parameters, rs, uint64(length), func(a, b uint16) bool {
			return a != b
		})
	case types.T_uint32:
		return valueCompare[uint32](parameters, rs, uint64(length), func(a, b uint32) bool {
			return a != b
		})
	case types.T_uint64:
		return valueCompare[uint64](parameters, rs, uint64(length), func(a, b uint64) bool {
			return a != b
		})
	case types.T_uuid:
		return valueCompare[types.Uuid](parameters, rs, uint64(length), func(a, b types.Uuid) bool {
			return a != b
		})
	case types.T_float32:
		return valueCompare[float32](parameters, rs, uint64(length), func(a, b float32) bool {
			return a != b
		})
	case types.T_float64:
		return valueCompare[float64](parameters, rs, uint64(length), func(a, b float64) bool {
			return a != b
		})
	case types.T_char, types.T_varchar, types.T_blob, types.T_json, types.T_text:
		return valueStrCompare(parameters, rs, uint64(length), func(a, b []byte) bool {
			return !bytes.Equal(a, b)
		})
	case types.T_binary, types.T_varbinary:
		return valueStrCompare(parameters, rs, uint64(length), func(a, b []byte) bool {
			return !bytes.Equal(a, b)
		})
	case types.T_date:
		return valueCompare[types.Date](parameters, rs, uint64(length), func(a, b types.Date) bool {
			return a != b
		})
	case types.T_datetime:
		return valueCompare[types.Datetime](parameters, rs, uint64(length), func(a, b types.Datetime) bool {
			return a != b
		})
	case types.T_time:
		return valueCompare[types.Time](parameters, rs, uint64(length), func(a, b types.Time) bool {
			return a != b
		})
	case types.T_timestamp:
		return valueCompare[types.Timestamp](parameters, rs, uint64(length), func(a, b types.Timestamp) bool {
			return a != b
		})
	case types.T_decimal64:
		return valueDec64Compare(parameters, rs, uint64(length), func(a, b types.Decimal64) bool {
			return a != b
		})
	case types.T_decimal128:
		return valueDec128Compare(parameters, rs, uint64(length), func(a, b types.Decimal128) bool {
			return a != b
		})
	}
	panic("unreached code")
}

func lessThanFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[bool](result)
	switch paramType.Oid {
	case types.T_bool:
		return valueBoolLessThan(parameters, rs, uint64(length))
	case types.T_int8:
		return valueCompare[int8](parameters, rs, uint64(length), func(a, b int8) bool {
			return a < b
		})
	case types.T_int16:
		return valueCompare[int16](parameters, rs, uint64(length), func(a, b int16) bool {
			return a < b
		})
	case types.T_int32:
		return valueCompare[int32](parameters, rs, uint64(length), func(a, b int32) bool {
			return a < b
		})
	case types.T_int64:
		return valueCompare[int64](parameters, rs, uint64(length), func(a, b int64) bool {
			return a < b
		})
	case types.T_uint8:
		return valueCompare[uint8](parameters, rs, uint64(length), func(a, b uint8) bool {
			return a < b
		})
	case types.T_uint16:
		return valueCompare[uint16](parameters, rs, uint64(length), func(a, b uint16) bool {
			return a < b
		})
	case types.T_uint32:
		return valueCompare[uint32](parameters, rs, uint64(length), func(a, b uint32) bool {
			return a < b
		})
	case types.T_uint64:
		return valueCompare[uint64](parameters, rs, uint64(length), func(a, b uint64) bool {
			return a < b
		})
	case types.T_uuid:
		return valueUuidLessThan(parameters, rs, uint64(length))
	case types.T_float32:
		return valueCompare[float32](parameters, rs, uint64(length), func(a, b float32) bool {
			return a < b
		})
	case types.T_float64:
		return valueCompare[float64](parameters, rs, uint64(length), func(a, b float64) bool {
			return a < b
		})
	case types.T_char, types.T_varchar, types.T_blob, types.T_json, types.T_text:
		return valueStrCompare(parameters, rs, uint64(length), func(a, b []byte) bool {
			return bytes.Compare(a, b) < 0
		})
	case types.T_binary, types.T_varbinary:
		return valueStrCompare(parameters, rs, uint64(length), func(a, b []byte) bool {
			return bytes.Compare(a, b) < 0
		})
	case types.T_date:
		return valueCompare[types.Date](parameters, rs, uint64(length), func(a, b types.Date) bool {
			return a < b
		})
	case types.T_datetime:
		return valueCompare[types.Datetime](parameters, rs, uint64(length), func(a, b types.Datetime) bool {
			return a < b
		})
	case types.T_time:
		return valueCompare[types.Time](parameters, rs, uint64(length), func(a, b types.Time) bool {
			return a < b
		})
	case types.T_timestamp:
		return valueCompare[types.Timestamp](parameters, rs, uint64(length), func(a, b types.Timestamp) bool {
			return a < b
		})
	case types.T_decimal64:
		return valueDec64Compare(parameters, rs, uint64(length), func(a, b types.Decimal64) bool {
			return a.Compare(b) < 0
		})
	case types.T_decimal128:
		return valueDec128Compare(parameters, rs, uint64(length), func(a, b types.Decimal128) bool {
			return a.Compare(b) < 0
		})
	}
	panic("unreached code")
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

func lessEqualFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[bool](result)
	switch paramType.Oid {
	case types.T_bool:
		return valueBoolLessEqual(parameters, rs, uint64(length))
	case types.T_int8:
		return valueCompare[int8](parameters, rs, uint64(length), func(a, b int8) bool {
			return a <= b
		})
	case types.T_int16:
		return valueCompare[int16](parameters, rs, uint64(length), func(a, b int16) bool {
			return a <= b
		})
	case types.T_int32:
		return valueCompare[int32](parameters, rs, uint64(length), func(a, b int32) bool {
			return a <= b
		})
	case types.T_int64:
		return valueCompare[int64](parameters, rs, uint64(length), func(a, b int64) bool {
			return a <= b
		})
	case types.T_uint8:
		return valueCompare[uint8](parameters, rs, uint64(length), func(a, b uint8) bool {
			return a <= b
		})
	case types.T_uint16:
		return valueCompare[uint16](parameters, rs, uint64(length), func(a, b uint16) bool {
			return a <= b
		})
	case types.T_uint32:
		return valueCompare[uint32](parameters, rs, uint64(length), func(a, b uint32) bool {
			return a <= b
		})
	case types.T_uint64:
		return valueCompare[uint64](parameters, rs, uint64(length), func(a, b uint64) bool {
			return a <= b
		})
	case types.T_uuid:
		return valueUuidLessEqual(parameters, rs, uint64(length))
	case types.T_float32:
		return valueCompare[float32](parameters, rs, uint64(length), func(a, b float32) bool {
			return a <= b
		})
	case types.T_float64:
		return valueCompare[float64](parameters, rs, uint64(length), func(a, b float64) bool {
			return a <= b
		})
	case types.T_char, types.T_varchar, types.T_blob, types.T_json, types.T_text:
		return valueStrCompare(parameters, rs, uint64(length), func(a, b []byte) bool {
			return bytes.Compare(a, b) <= 0
		})
	case types.T_binary, types.T_varbinary:
		return valueStrCompare(parameters, rs, uint64(length), func(a, b []byte) bool {
			return bytes.Compare(a, b) <= 0
		})
	case types.T_date:
		return valueCompare[types.Date](parameters, rs, uint64(length), func(a, b types.Date) bool {
			return a <= b
		})
	case types.T_datetime:
		return valueCompare[types.Datetime](parameters, rs, uint64(length), func(a, b types.Datetime) bool {
			return a <= b
		})
	case types.T_time:
		return valueCompare[types.Time](parameters, rs, uint64(length), func(a, b types.Time) bool {
			return a <= b
		})
	case types.T_timestamp:
		return valueCompare[types.Timestamp](parameters, rs, uint64(length), func(a, b types.Timestamp) bool {
			return a <= b
		})
	case types.T_decimal64:
		return valueDec64Compare(parameters, rs, uint64(length), func(a, b types.Decimal64) bool {
			return a.Compare(b) <= 0
		})
	case types.T_decimal128:
		return valueDec128Compare(parameters, rs, uint64(length), func(a, b types.Decimal128) bool {
			return a.Compare(b) <= 0
		})
	}
	panic("unreached code")
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
