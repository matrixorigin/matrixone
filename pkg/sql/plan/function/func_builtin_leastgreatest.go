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

// check input types for least and greatest function
// it should be at least 1 input, and all inputs should be the same type.
func leastGreatestCheck(_ []overload, inputs []types.Type) checkResult {
	if len(inputs) < 1 {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}
	for i := 1; i < len(inputs); i++ {
		if inputs[i].Oid != inputs[0].Oid {
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		}
	}
	return newCheckResultWithSuccess(0)
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

func leastFn(parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList) error {
	paramType := parameters[0].GetType()
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
	paramType := parameters[0].GetType()
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
