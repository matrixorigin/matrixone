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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/moarray"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func otherCompareOperatorSupports(typ1, typ2 types.Type) bool {
	if typ1.Oid != typ2.Oid {
		return false
	}
	switch typ1.Oid {
	case types.T_bool:
	case types.T_bit:
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
	case types.T_float32, types.T_float64:
	case types.T_decimal64, types.T_decimal128:
	case types.T_char, types.T_varchar:
	case types.T_date, types.T_datetime:
	case types.T_timestamp, types.T_time:
	case types.T_blob, types.T_text, types.T_datalink:
	case types.T_binary, types.T_varbinary:
	case types.T_uuid:
	case types.T_Rowid:
	case types.T_array_float32, types.T_array_float64:
	default:
		return false
	}
	return true
}

func equalAndNotEqualOperatorSupports(typ1, typ2 types.Type) bool {
	if typ1.Oid != typ2.Oid {
		return false
	}
	switch typ1.Oid {
	case types.T_bool:
	case types.T_bit:
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
	case types.T_float32, types.T_float64:
	case types.T_decimal64, types.T_decimal128:
	case types.T_char, types.T_varchar:
	case types.T_date, types.T_datetime:
	case types.T_timestamp, types.T_time:
	case types.T_blob, types.T_text, types.T_datalink:
	case types.T_binary, types.T_varbinary:
	case types.T_json:
	case types.T_uuid:
	case types.T_Rowid:
	case types.T_array_float32, types.T_array_float64:
	case types.T_enum:
	default:
		return false
	}
	return true
}

// should convert to c.Numeric next.
func equalFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[bool](result)
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
		return opBinaryFixedFixedToFixed[float32, float32, bool](parameters, rs, proc, length, func(a, b float32) bool {
			return a == b
		}, selectList)
	case types.T_float64:
		return opBinaryFixedFixedToFixed[float64, float64, bool](parameters, rs, proc, length, func(a, b float64) bool {
			return a == b
		}, selectList)
	case types.T_char, types.T_varchar, types.T_blob, types.T_json, types.T_text, types.T_binary, types.T_varbinary, types.T_datalink:
		if parameters[0].GetArea() == nil && parameters[1].GetArea() == nil {
			return compareVarlenaEqual(parameters, rs, proc, length, selectList)
		}
		return opBinaryStrStrToFixed[bool](parameters, rs, proc, length, func(v1, v2 string) bool {
			return v1 == v2
		}, selectList)
	case types.T_array_float32:
		if parameters[0].GetArea() == nil && parameters[1].GetArea() == nil {
			return compareVarlenaEqual(parameters, rs, proc, length, selectList)
		}
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float32](v1)
			_v2 := types.BytesToArray[float32](v2)

			return moarray.Compare[float32](_v1, _v2) == 0
		}, selectList)
	case types.T_array_float64:
		if parameters[0].GetArea() == nil && parameters[1].GetArea() == nil {
			return compareVarlenaEqual(parameters, rs, proc, length, selectList)
		}
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float64](v1)
			_v2 := types.BytesToArray[float64](v2)

			return moarray.Compare[float64](_v1, _v2) == 0
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
	case types.T_Rowid:
		return opBinaryFixedFixedToFixed[types.Rowid, types.Rowid, bool](parameters, rs, proc, length, func(a, b types.Rowid) bool {
			return a.Equal(b)
		}, selectList)
	case types.T_enum:
		return opBinaryFixedFixedToFixed[types.Enum, types.Enum, bool](parameters, rs, proc, length, func(a, b types.Enum) bool {
			return a == b
		}, selectList)
	}
	panic("unreached code")
}

func valueDec64Compare(
	parameters []*vector.Vector, result *vector.FunctionResult[bool], length uint64,
	cmpFn func(a, b types.Decimal64) bool, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](parameters[1])

	m := p2.GetType().Scale - p1.GetType().Scale

	rsVec := result.GetResultVector()
	rss := vector.MustFixedCol[bool](rsVec)

	c1, c2 := parameters[0].IsConst(), parameters[1].IsConst()
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
	if c1 && c2 {
		v1, null1 := p1.GetValue(0)
		v2, null2 := p2.GetValue(0)
		if null1 || null2 {
			nulls.AddRange(rsNull, 0, length)
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

	if c1 {
		v1, null1 := p1.GetValue(0)
		if null1 {
			nulls.AddRange(rsNull, 0, length)
		} else {
			if m >= 0 {
				x, _ := v1.Scale(m)
				if p2.WithAnyNullValue() || rsAnyNull {
					nulls.Or(rsNull, parameters[1].GetNulls(), rsNull)
					for i := uint64(0); i < length; i++ {
						if rsNull.Contains(i) {
							continue
						}
						v2, _ := p2.GetValue(i)
						rss[i] = cmpFn(x, v2)
					}
				} else {
					for i := uint64(0); i < length; i++ {
						v2, _ := p2.GetValue(i)
						rss[i] = cmpFn(x, v2)
					}
				}
			} else {
				if p2.WithAnyNullValue() || rsAnyNull {
					nulls.Or(rsNull, parameters[1].GetNulls(), rsNull)
					for i := uint64(0); i < length; i++ {
						if rsNull.Contains(i) {
							continue
						}
						v2, _ := p2.GetValue(i)
						y, _ := v2.Scale(-m)
						rss[i] = cmpFn(v1, y)
					}
				} else {
					scaleMy := -m
					for i := uint64(0); i < length; i++ {
						v2, _ := p2.GetValue(i)
						y, _ := v2.Scale(scaleMy)
						rss[i] = cmpFn(v1, y)
					}
				}
			}
		}

		return nil
	}

	if c2 {
		v2, null2 := p2.GetValue(0)
		if null2 {
			nulls.AddRange(rsNull, 0, length)
		} else {
			if m >= 0 {
				if p1.WithAnyNullValue() || rsAnyNull {
					nulls.Or(rsNull, parameters[0].GetNulls(), rsNull)
					for i := uint64(0); i < length; i++ {
						if rsNull.Contains(i) {
							continue
						}
						v1, _ := p1.GetValue(i)
						x, _ := v1.Scale(m)
						rss[i] = cmpFn(x, v2)
					}
				} else {
					for i := uint64(0); i < length; i++ {
						v1, _ := p1.GetValue(i)
						x, _ := v1.Scale(m)
						rss[i] = cmpFn(x, v2)
					}
				}
			} else {
				y, _ := v2.Scale(-m)
				if p1.WithAnyNullValue() || rsAnyNull {
					nulls.Or(rsNull, parameters[0].GetNulls(), rsNull)
					for i := uint64(0); i < length; i++ {
						if rsNull.Contains(i) {
							continue
						}
						v1, _ := p1.GetValue(i)
						rss[i] = cmpFn(v1, y)
					}
				} else {
					for i := uint64(0); i < length; i++ {
						v1, _ := p1.GetValue(i)
						rss[i] = cmpFn(v1, y)
					}
				}
			}
		}
		return nil
	}

	if p1.WithAnyNullValue() || p2.WithAnyNullValue() || rsAnyNull {
		nulls.Or(rsNull, parameters[0].GetNulls(), rsNull)
		nulls.Or(rsNull, parameters[1].GetNulls(), rsNull)
		if m >= 0 {
			for i := uint64(0); i < length; i++ {
				if rsNull.Contains(i) {
					continue
				}
				v1, _ := p1.GetValue(i)
				v2, _ := p2.GetValue(i)
				x, _ := v1.Scale(m)
				rss[i] = cmpFn(x, v2)
			}
		} else {
			scaleMy := -m
			for i := uint64(0); i < length; i++ {
				if rsNull.Contains(i) {
					continue
				}
				v1, _ := p1.GetValue(i)
				v2, _ := p2.GetValue(i)
				y, _ := v2.Scale(scaleMy)
				rss[i] = cmpFn(v1, y)
			}
		}
		return nil
	}

	if m >= 0 {
		for i := uint64(0); i < length; i++ {
			v1, _ := p1.GetValue(i)
			v2, _ := p2.GetValue(i)
			x, _ := v1.Scale(m)
			rss[i] = cmpFn(x, v2)
		}
	} else {
		scaleMy := -m
		for i := uint64(0); i < length; i++ {
			v1, _ := p1.GetValue(i)
			v2, _ := p2.GetValue(i)
			y, _ := v2.Scale(scaleMy)
			rss[i] = cmpFn(v1, y)
		}
	}
	return nil
}

func valueDec128Compare(
	parameters []*vector.Vector, result *vector.FunctionResult[bool], length uint64,
	cmpFn func(a, b types.Decimal128) bool, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](parameters[1])

	m := p2.GetType().Scale - p1.GetType().Scale

	rsVec := result.GetResultVector()
	rss := vector.MustFixedCol[bool](rsVec)

	c1, c2 := parameters[0].IsConst(), parameters[1].IsConst()
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
	if c1 && c2 {
		v1, null1 := p1.GetValue(0)
		v2, null2 := p2.GetValue(0)
		if null1 || null2 {
			nulls.AddRange(rsNull, 0, length)
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

	if c1 {
		v1, null1 := p1.GetValue(0)
		if null1 {
			nulls.AddRange(rsNull, 0, length)
		} else {
			if m >= 0 {
				x, _ := v1.Scale(m)
				if p2.WithAnyNullValue() || rsAnyNull {
					nulls.Or(rsNull, parameters[1].GetNulls(), rsNull)
					for i := uint64(0); i < length; i++ {
						if rsNull.Contains(i) {
							continue
						}
						v2, _ := p2.GetValue(i)
						rss[i] = cmpFn(x, v2)
					}
				} else {
					for i := uint64(0); i < length; i++ {
						v2, _ := p2.GetValue(i)
						rss[i] = cmpFn(x, v2)
					}
				}
			} else {
				if p2.WithAnyNullValue() {
					nulls.Or(rsNull, parameters[1].GetNulls(), rsNull)
					for i := uint64(0); i < length; i++ {
						if rsNull.Contains(i) {
							continue
						}
						v2, _ := p2.GetValue(i)
						y, _ := v2.Scale(-m)
						rss[i] = cmpFn(v1, y)
					}
				} else {
					scaleMy := -m
					for i := uint64(0); i < length; i++ {
						v2, _ := p2.GetValue(i)
						y, _ := v2.Scale(scaleMy)
						rss[i] = cmpFn(v1, y)
					}
				}
			}
		}

		return nil
	}

	if c2 {
		v2, null2 := p2.GetValue(0)
		if null2 {
			nulls.AddRange(rsNull, 0, length)
		} else {
			if m >= 0 {
				if p1.WithAnyNullValue() || rsAnyNull {
					nulls.Or(rsNull, parameters[0].GetNulls(), rsNull)
					for i := uint64(0); i < length; i++ {
						if rsNull.Contains(i) {
							continue
						}
						v1, _ := p1.GetValue(i)
						x, _ := v1.Scale(m)
						rss[i] = cmpFn(x, v2)
					}
				} else {
					for i := uint64(0); i < length; i++ {
						v1, _ := p1.GetValue(i)
						x, _ := v1.Scale(m)
						rss[i] = cmpFn(x, v2)
					}
				}
			} else {
				y, _ := v2.Scale(-m)
				if p1.WithAnyNullValue() || rsAnyNull {
					nulls.Or(rsNull, parameters[0].GetNulls(), rsNull)
					for i := uint64(0); i < length; i++ {
						if rsNull.Contains(i) {
							continue
						}
						v1, _ := p1.GetValue(i)
						rss[i] = cmpFn(v1, y)
					}
				} else {
					for i := uint64(0); i < length; i++ {
						v1, _ := p1.GetValue(i)
						rss[i] = cmpFn(v1, y)
					}
				}
			}
		}
		return nil
	}

	if p1.WithAnyNullValue() || p2.WithAnyNullValue() || rsAnyNull {
		nulls.Or(rsNull, parameters[0].GetNulls(), rsNull)
		nulls.Or(rsNull, parameters[1].GetNulls(), rsNull)
		if m >= 0 {
			for i := uint64(0); i < length; i++ {
				if rsNull.Contains(i) {
					continue
				}
				v1, _ := p1.GetValue(i)
				v2, _ := p2.GetValue(i)
				x, _ := v1.Scale(m)
				rss[i] = cmpFn(x, v2)
			}
		} else {
			scaleMy := -m
			for i := uint64(0); i < length; i++ {
				if rsNull.Contains(i) {
					continue
				}
				v1, _ := p1.GetValue(i)
				v2, _ := p2.GetValue(i)
				y, _ := v2.Scale(scaleMy)
				rss[i] = cmpFn(v1, y)
			}
		}
		return nil
	}

	if m >= 0 {
		for i := uint64(0); i < length; i++ {
			v1, _ := p1.GetValue(i)
			v2, _ := p2.GetValue(i)
			x, _ := v1.Scale(m)
			rss[i] = cmpFn(x, v2)
		}
	} else {
		scaleMy := -m
		for i := uint64(0); i < length; i++ {
			v1, _ := p1.GetValue(i)
			v2, _ := p2.GetValue(i)
			y, _ := v2.Scale(scaleMy)
			rss[i] = cmpFn(v1, y)
		}
	}
	return nil
}

func greatThanFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[bool](result)
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
		return opBinaryFixedFixedToFixed[float32, float32, bool](parameters, rs, proc, length, func(a, b float32) bool {
			return a > b
		}, selectList)
	case types.T_float64:
		return opBinaryFixedFixedToFixed[float64, float64, bool](parameters, rs, proc, length, func(a, b float64) bool {
			return a > b
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

			return moarray.Compare[float32](_v1, _v2) > 0
		}, selectList)
	case types.T_array_float64:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float64](v1)
			_v2 := types.BytesToArray[float64](v2)

			return moarray.Compare[float64](_v1, _v2) > 0
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
	case types.T_Rowid:
		return opBinaryFixedFixedToFixed[types.Rowid, types.Rowid, bool](parameters, rs, proc, length, func(a, b types.Rowid) bool {
			return a.Great(b)
		}, selectList)
	}
	panic("unreached code")
}

func greatEqualFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[bool](result)
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
		return opBinaryFixedFixedToFixed[float32, float32, bool](parameters, rs, proc, length, func(a, b float32) bool {
			return a >= b
		}, selectList)
	case types.T_float64:
		return opBinaryFixedFixedToFixed[float64, float64, bool](parameters, rs, proc, length, func(a, b float64) bool {
			return a >= b
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

			return moarray.Compare[float32](_v1, _v2) >= 0
		}, selectList)
	case types.T_array_float64:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float64](v1)
			_v2 := types.BytesToArray[float64](v2)

			return moarray.Compare[float64](_v1, _v2) >= 0
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
	case types.T_Rowid:
		return opBinaryFixedFixedToFixed[types.Rowid, types.Rowid, bool](parameters, rs, proc, length, func(a, b types.Rowid) bool {
			return a.Ge(b)
		}, selectList)
	}
	panic("unreached code")
}

func notEqualFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[bool](result)
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
		return opBinaryFixedFixedToFixed[float32, float32, bool](parameters, rs, proc, length, func(a, b float32) bool {
			return a != b
		}, selectList)
	case types.T_float64:
		return opBinaryFixedFixedToFixed[float64, float64, bool](parameters, rs, proc, length, func(a, b float64) bool {
			return a != b
		}, selectList)
	case types.T_char, types.T_varchar, types.T_blob, types.T_json, types.T_text, types.T_datalink:
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

			return moarray.Compare[float32](_v1, _v2) != 0
		}, selectList)
	case types.T_array_float64:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float64](v1)
			_v2 := types.BytesToArray[float64](v2)

			return moarray.Compare[float64](_v1, _v2) != 0
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
	case types.T_Rowid:
		return opBinaryFixedFixedToFixed[types.Rowid, types.Rowid, bool](parameters, rs, proc, length, func(a, b types.Rowid) bool {
			return a.NotEqual(b)
		}, selectList)
	}
	panic("unreached code")
}

func lessThanFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[bool](result)
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
		return opBinaryFixedFixedToFixed[float32, float32, bool](parameters, rs, proc, length, func(a, b float32) bool {
			return a < b
		}, selectList)
	case types.T_float64:
		return opBinaryFixedFixedToFixed[float64, float64, bool](parameters, rs, proc, length, func(a, b float64) bool {
			return a < b
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

			return moarray.Compare[float32](_v1, _v2) < 0
		}, selectList)
	case types.T_array_float64:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float64](v1)
			_v2 := types.BytesToArray[float64](v2)

			return moarray.Compare[float64](_v1, _v2) < 0
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
	case types.T_Rowid:
		return opBinaryFixedFixedToFixed[types.Rowid, types.Rowid, bool](parameters, rs, proc, length, func(a, b types.Rowid) bool {
			return a.Less(b)
		}, selectList)
	}
	panic("unreached code")
}

func lessEqualFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[bool](result)
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
		return opBinaryFixedFixedToFixed[float32, float32, bool](parameters, rs, proc, length, func(a, b float32) bool {
			return a <= b
		}, selectList)
	case types.T_float64:
		return opBinaryFixedFixedToFixed[float64, float64, bool](parameters, rs, proc, length, func(a, b float64) bool {
			return a <= b
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

			return moarray.Compare[float32](_v1, _v2) <= 0
		}, selectList)
	case types.T_array_float64:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float64](v1)
			_v2 := types.BytesToArray[float64](v2)

			return moarray.Compare[float64](_v1, _v2) <= 0
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
	case types.T_Rowid:
		return opBinaryFixedFixedToFixed[types.Rowid, types.Rowid, bool](parameters, rs, proc, length, func(a, b types.Rowid) bool {
			return a.Le(b)
		}, selectList)
	}
	panic("unreached code")
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

func operatorOpBitAndInt64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpInt64Fn(parameters, result, proc, length, func(i int64, i2 int64) int64 {
		return i & i2
	})
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

func operatorOpBitXorInt64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpInt64Fn(parameters, result, proc, length, func(i int64, i2 int64) int64 {
		return i ^ i2
	})
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

func operatorOpBitOrInt64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpInt64Fn(parameters, result, proc, length, func(i int64, i2 int64) int64 {
		return i | i2
	})
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

func operatorOpBitShiftLeftInt64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpInt64Fn(parameters, result, proc, length, func(i int64, i2 int64) int64 {
		if i2 < 0 {
			return 0
		}
		return i << i2
	})
}

func operatorOpBitShiftRightInt64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpInt64Fn(parameters, result, proc, length, func(i int64, i2 int64) int64 {
		if i2 < 0 {
			return 0
		}
		return i >> i2
	})
}
