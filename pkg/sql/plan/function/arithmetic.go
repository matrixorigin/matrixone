// Copyright 2021 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"math"
)

func plusOperatorSupports(typ1, typ2 types.Type) bool {
	if typ1.Oid != typ2.Oid {
		return false
	}
	switch typ1.Oid {
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
	case types.T_float32, types.T_float64:
	case types.T_decimal64, types.T_decimal128:
	case types.T_array_int8, types.T_array_int16, types.T_array_int32, types.T_array_int64, types.T_array_float32, types.T_array_float64:
	default:
		return false
	}
	return true
}

func minusOperatorSupports(typ1, typ2 types.Type) bool {
	if typ1.Oid != typ2.Oid {
		return false
	}
	switch typ1.Oid {
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
	case types.T_float32, types.T_float64:
	case types.T_decimal64, types.T_decimal128:
	case types.T_date, types.T_datetime:
	case types.T_array_int8, types.T_array_int16, types.T_array_int32, types.T_array_int64, types.T_array_float32, types.T_array_float64:
	default:
		return false
	}
	return true
}

func multiOperatorSupports(typ1, typ2 types.Type) bool {
	if typ1.Oid != typ2.Oid {
		return false
	}
	switch typ1.Oid {
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
	case types.T_float32, types.T_float64:
	case types.T_decimal64, types.T_decimal128:
	case types.T_array_int8, types.T_array_int16, types.T_array_int32, types.T_array_int64, types.T_array_float32, types.T_array_float64:
	default:
		return false
	}
	return true
}

func divOperatorSupports(typ1, typ2 types.Type) bool {
	if typ1.Oid != typ2.Oid {
		return false
	}
	switch typ1.Oid {
	case types.T_float32, types.T_float64:
	case types.T_decimal64, types.T_decimal128:
	case types.T_array_float32, types.T_array_float64:
	default:
		return false
	}
	return true
}

func integerDivOperatorSupports(typ1, typ2 types.Type) bool {
	if typ1.Oid != typ2.Oid {
		return false
	}
	switch typ1.Oid {
	case types.T_float32, types.T_float64:
	default:
		return false
	}
	return true
}

func modOperatorSupports(typ1, typ2 types.Type) bool {
	if typ1.Oid != typ2.Oid {
		return false
	}
	switch typ1.Oid {
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
	case types.T_float32, types.T_float64:
	case types.T_decimal128, types.T_decimal64:
	default:
		return false
	}
	return true
}

func plusFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	paramType := parameters[0].GetType()
	switch paramType.Oid {
	case types.T_uint8:
		return opBinaryFixedFixedToFixed[uint8, uint8, uint8](parameters, result, proc, length, func(v1, v2 uint8) uint8 {
			return v1 + v2
		})
	case types.T_uint16:
		return opBinaryFixedFixedToFixed[uint16, uint16, uint16](parameters, result, proc, length, func(v1, v2 uint16) uint16 {
			return v1 + v2
		})
	case types.T_uint32:
		return opBinaryFixedFixedToFixed[uint32, uint32, uint32](parameters, result, proc, length, func(v1, v2 uint32) uint32 {
			return v1 + v2
		})
	case types.T_uint64:
		return opBinaryFixedFixedToFixed[uint64, uint64, uint64](parameters, result, proc, length, func(v1, v2 uint64) uint64 {
			return v1 + v2
		})
	case types.T_int8:
		return opBinaryFixedFixedToFixed[int8, int8, int8](parameters, result, proc, length, func(v1, v2 int8) int8 {
			return v1 + v2
		})
	case types.T_int16:
		return opBinaryFixedFixedToFixed[int16, int16, int16](parameters, result, proc, length, func(v1, v2 int16) int16 {
			return v1 + v2
		})
	case types.T_int32:
		return opBinaryFixedFixedToFixed[int32, int32, int32](parameters, result, proc, length, func(v1, v2 int32) int32 {
			return v1 + v2
		})
	case types.T_int64:
		return opBinaryFixedFixedToFixed[int64, int64, int64](parameters, result, proc, length, func(v1, v2 int64) int64 {
			return v1 + v2
		})
	case types.T_float32:
		return opBinaryFixedFixedToFixed[float32, float32, float32](parameters, result, proc, length, func(v1, v2 float32) float32 {
			return v1 + v2
		})
	case types.T_float64:
		return opBinaryFixedFixedToFixed[float64, float64, float64](parameters, result, proc, length, func(v1, v2 float64) float64 {
			return v1 + v2
		})
	case types.T_decimal64:
		return decimalArith[types.Decimal64](parameters, result, proc, length, func(v1, v2 types.Decimal64, scale1, scale2 int32) (types.Decimal64, error) {
			r, _, err := v1.Add(v2, scale1, scale2)
			return r, err
		})
	case types.T_decimal128:
		return decimalArith[types.Decimal128](parameters, result, proc, length, func(v1, v2 types.Decimal128, scale1, scale2 int32) (types.Decimal128, error) {
			r, _, err := v1.Add(v2, scale1, scale2)
			return r, err
		})

	case types.T_array_int8:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, plusFnArray[int8])
	case types.T_array_int16:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, plusFnArray[int16])
	case types.T_array_int32:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, plusFnArray[int32])
	case types.T_array_int64:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, plusFnArray[int64])
	case types.T_array_float32:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, plusFnArray[float32])
	case types.T_array_float64:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, plusFnArray[float64])
	}
	panic("unreached code")
}

func minusFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	paramType := parameters[0].GetType()
	switch paramType.Oid {
	case types.T_uint8:
		return opBinaryFixedFixedToFixed[uint8, uint8, uint8](parameters, result, proc, length, func(v1, v2 uint8) uint8 {
			return v1 - v2
		})
	case types.T_uint16:
		return opBinaryFixedFixedToFixed[uint16, uint16, uint16](parameters, result, proc, length, func(v1, v2 uint16) uint16 {
			return v1 - v2
		})
	case types.T_uint32:
		return opBinaryFixedFixedToFixed[uint32, uint32, uint32](parameters, result, proc, length, func(v1, v2 uint32) uint32 {
			return v1 - v2
		})
	case types.T_uint64:
		return opBinaryFixedFixedToFixed[uint64, uint64, uint64](parameters, result, proc, length, func(v1, v2 uint64) uint64 {
			return v1 - v2
		})
	case types.T_int8:
		return opBinaryFixedFixedToFixed[int8, int8, int8](parameters, result, proc, length, func(v1, v2 int8) int8 {
			return v1 - v2
		})
	case types.T_int16:
		return opBinaryFixedFixedToFixed[int16, int16, int16](parameters, result, proc, length, func(v1, v2 int16) int16 {
			return v1 - v2
		})
	case types.T_int32:
		return opBinaryFixedFixedToFixed[int32, int32, int32](parameters, result, proc, length, func(v1, v2 int32) int32 {
			return v1 - v2
		})
	case types.T_int64:
		return opBinaryFixedFixedToFixed[int64, int64, int64](parameters, result, proc, length, func(v1, v2 int64) int64 {
			return v1 - v2
		})
	case types.T_float32:
		return opBinaryFixedFixedToFixed[float32, float32, float32](parameters, result, proc, length, func(v1, v2 float32) float32 {
			return v1 - v2
		})
	case types.T_float64:
		return opBinaryFixedFixedToFixed[float64, float64, float64](parameters, result, proc, length, func(v1, v2 float64) float64 {
			return v1 - v2
		})
	case types.T_decimal64:
		return decimalArith[types.Decimal64](parameters, result, proc, length, func(v1, v2 types.Decimal64, scale1, scale2 int32) (types.Decimal64, error) {
			r, _, err := v1.Sub(v2, scale1, scale2)
			return r, err
		})
	case types.T_decimal128:
		return decimalArith[types.Decimal128](parameters, result, proc, length, func(v1, v2 types.Decimal128, scale1, scale2 int32) (types.Decimal128, error) {
			r, _, err := v1.Sub(v2, scale1, scale2)
			return r, err
		})
	case types.T_date:
		return opBinaryFixedFixedToFixed[types.Date, types.Date, int64](parameters, result, proc, length, func(v1, v2 types.Date) int64 {
			return int64(v1 - v2)
		})
	case types.T_datetime:
		return opBinaryFixedFixedToFixed[types.Datetime, types.Datetime, int64](parameters, result, proc, length, func(v1, v2 types.Datetime) int64 {
			return v1.DatetimeMinusWithSecond(v2)
		})

	case types.T_array_int8:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, minusFnArray[int8])
	case types.T_array_int16:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, minusFnArray[int16])
	case types.T_array_int32:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, minusFnArray[int32])
	case types.T_array_int64:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, minusFnArray[int64])
	case types.T_array_float32:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, minusFnArray[float32])
	case types.T_array_float64:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, minusFnArray[float64])
	}
	panic("unreached code")
}

func multiFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	paramType := parameters[0].GetType()
	switch paramType.Oid {
	case types.T_uint8:
		return opBinaryFixedFixedToFixed[uint8, uint8, uint8](parameters, result, proc, length, func(v1, v2 uint8) uint8 {
			return v1 * v2
		})
	case types.T_uint16:
		return opBinaryFixedFixedToFixed[uint16, uint16, uint16](parameters, result, proc, length, func(v1, v2 uint16) uint16 {
			return v1 * v2
		})
	case types.T_uint32:
		return opBinaryFixedFixedToFixed[uint32, uint32, uint32](parameters, result, proc, length, func(v1, v2 uint32) uint32 {
			return v1 * v2
		})
	case types.T_uint64:
		return opBinaryFixedFixedToFixed[uint64, uint64, uint64](parameters, result, proc, length, func(v1, v2 uint64) uint64 {
			return v1 * v2
		})
	case types.T_int8:
		return opBinaryFixedFixedToFixed[int8, int8, int8](parameters, result, proc, length, func(v1, v2 int8) int8 {
			return v1 * v2
		})
	case types.T_int16:
		return opBinaryFixedFixedToFixed[int16, int16, int16](parameters, result, proc, length, func(v1, v2 int16) int16 {
			return v1 * v2
		})
	case types.T_int32:
		return opBinaryFixedFixedToFixed[int32, int32, int32](parameters, result, proc, length, func(v1, v2 int32) int32 {
			return v1 * v2
		})
	case types.T_int64:
		return opBinaryFixedFixedToFixed[int64, int64, int64](parameters, result, proc, length, func(v1, v2 int64) int64 {
			return v1 * v2
		})
	case types.T_float32:
		return opBinaryFixedFixedToFixed[float32, float32, float32](parameters, result, proc, length, func(v1, v2 float32) float32 {
			return v1 * v2
		})
	case types.T_float64:
		return opBinaryFixedFixedToFixed[float64, float64, float64](parameters, result, proc, length, func(v1, v2 float64) float64 {
			return v1 * v2
		})
	case types.T_decimal64:
		return decimalArith2(parameters, result, proc, length, func(x, y types.Decimal128, scale1, scale2 int32) (types.Decimal128, error) {
			rt, _, err := x.Mul(y, scale1, scale2)
			return rt, err
		})
	case types.T_decimal128:
		return decimalArith[types.Decimal128](parameters, result, proc, length, func(v1, v2 types.Decimal128, scale1, scale2 int32) (types.Decimal128, error) {
			r, _, err := v1.Mul(v2, scale1, scale2)
			return r, err
		})

	case types.T_array_int8:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, multiFnArray[int8])
	case types.T_array_int16:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, multiFnArray[int16])
	case types.T_array_int32:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, multiFnArray[int32])
	case types.T_array_int64:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, multiFnArray[int64])
	case types.T_array_float32:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, multiFnArray[float32])
	case types.T_array_float64:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, multiFnArray[float64])
	}
	panic("unreached code")
}

func divFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	paramType := parameters[0].GetType()
	switch paramType.Oid {
	case types.T_float32:
		return specialTemplateForDivFunction[float32, float32](parameters, result, proc, length, func(v1, v2 float32) float32 {
			return v1 / v2
		})
	case types.T_float64:
		return specialTemplateForDivFunction[float64, float64](parameters, result, proc, length, func(v1, v2 float64) float64 {
			return v1 / v2
		})
	case types.T_decimal64:
		return decimalArith2(parameters, result, proc, length, func(x, y types.Decimal128, scale1, scale2 int32) (types.Decimal128, error) {
			rt, _, err := x.Div(y, scale1, scale2)
			return rt, err
		})
	case types.T_decimal128:
		return decimalArith[types.Decimal128](parameters, result, proc, length, func(v1, v2 types.Decimal128, scale1, scale2 int32) (types.Decimal128, error) {
			r, _, err := v1.Div(v2, scale1, scale2)
			return r, err
		})
	//TODO: Check if int8 would be casted to float32 or float64.
	// My understanding: https://stackoverflow.com/a/34504552/1609570
	case types.T_array_float32:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, divFnArray[float32])
	case types.T_array_float64:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, divFnArray[float64])
	}
	panic("unreached code")
}

func integerDivFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	paramType := parameters[0].GetType()
	if paramType.Oid == types.T_float32 {
		return specialTemplateForDivFunction[float32, int64](parameters, result, proc, length, func(v1, v2 float32) int64 {
			return int64(v1 / v2)
		})
	}
	if paramType.Oid == types.T_float64 {
		return specialTemplateForDivFunction[float64, int64](parameters, result, proc, length, func(v1, v2 float64) int64 {
			return int64(v1 / v2)
		})
	}
	panic("unreached code")
}

func modFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	paramType := parameters[0].GetType()
	switch paramType.Oid {
	case types.T_uint8:
		return specialTemplateForModFunction[uint8](parameters, result, proc, length, func(v1, v2 uint8) uint8 {
			return v1 % v2
		})
	case types.T_uint16:
		return specialTemplateForModFunction[uint16](parameters, result, proc, length, func(v1, v2 uint16) uint16 {
			return v1 % v2
		})
	case types.T_uint32:
		return specialTemplateForModFunction[uint32](parameters, result, proc, length, func(v1, v2 uint32) uint32 {
			return v1 % v2
		})
	case types.T_uint64:
		return specialTemplateForModFunction[uint64](parameters, result, proc, length, func(v1, v2 uint64) uint64 {
			return v1 % v2
		})
	case types.T_int8:
		return specialTemplateForModFunction[int8](parameters, result, proc, length, func(v1, v2 int8) int8 {
			return v1 % v2
		})
	case types.T_int16:
		return specialTemplateForModFunction[int16](parameters, result, proc, length, func(v1, v2 int16) int16 {
			return v1 % v2
		})
	case types.T_int32:
		return specialTemplateForModFunction[int32](parameters, result, proc, length, func(v1, v2 int32) int32 {
			return v1 % v2
		})
	case types.T_int64:
		return specialTemplateForModFunction[int64](parameters, result, proc, length, func(v1, v2 int64) int64 {
			return v1 % v2
		})
	case types.T_float32:
		return specialTemplateForModFunction[float32](parameters, result, proc, length, func(v1, v2 float32) float32 {
			return float32(math.Mod(float64(v1), float64(v2)))
		})
	case types.T_float64:
		return specialTemplateForModFunction[float64](parameters, result, proc, length, func(v1, v2 float64) float64 {
			return math.Mod(v1, v2)
		})
	case types.T_decimal64:
		return decimalArith[types.Decimal64](parameters, result, proc, length, func(v1, v2 types.Decimal64, scale1, scale2 int32) (types.Decimal64, error) {
			r, _, err := v1.Mod(v2, scale1, scale2)
			return r, err
		})
	case types.T_decimal128:
		return decimalArith[types.Decimal128](parameters, result, proc, length, func(v1, v2 types.Decimal128, scale1, scale2 int32) (types.Decimal128, error) {
			r, _, err := v1.Mod(v2, scale1, scale2)
			return r, err
		})
	}
	panic("unreached code")
}

func plusFnArray[T types.RealNumbers](v1, v2 []byte) ([]byte, error) {
	if len(v1) != len(v2) {
		return nil, moerr.NewInternalErrorNoCtx("Dimensions should be same")
	}

	_v1 := types.BytesToArray[T](v1)
	_v2 := types.BytesToArray[T](v2)

	r := make([]T, len(v1))
	for i := 0; i < len(v1); i++ {
		r[i] = _v1[i] + _v2[i]
	}

	return types.ArrayToBytes[T](r), nil
}

func minusFnArray[T types.RealNumbers](v1, v2 []byte) ([]byte, error) {
	if len(v1) != len(v2) {
		return nil, moerr.NewInternalErrorNoCtx("Dimensions should be same")
	}

	_v1 := types.BytesToArray[T](v1)
	_v2 := types.BytesToArray[T](v2)

	r := make([]T, len(v1))
	for i := 0; i < len(v1); i++ {
		r[i] = _v1[i] - _v2[i]
	}

	return types.ArrayToBytes[T](r), nil
}

func multiFnArray[T types.RealNumbers](v1, v2 []byte) ([]byte, error) {
	if len(v1) != len(v2) {
		return nil, moerr.NewInternalErrorNoCtx("Dimensions should be same")
	}

	_v1 := types.BytesToArray[T](v1)
	_v2 := types.BytesToArray[T](v2)

	r := make([]T, len(v1))
	for i := 0; i < len(v1); i++ {
		r[i] = _v1[i] * _v2[i]
	}

	return types.ArrayToBytes[T](r), nil
}

func divFnArray[T types.RealNumbers](v1, v2 []byte) ([]byte, error) {
	if len(v1) != len(v2) {
		return nil, moerr.NewInternalErrorNoCtx("Dimensions should be same")
	}

	_v1 := types.BytesToArray[T](v1)
	_v2 := types.BytesToArray[T](v2)

	r := make([]T, len(v1))
	for i := 0; i < len(v1); i++ {
		r[i] = _v1[i] / _v2[i]
	}

	return types.ArrayToBytes[T](r), nil
}
