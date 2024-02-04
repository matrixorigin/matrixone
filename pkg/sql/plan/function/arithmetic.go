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
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/moarray"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func plusOperatorSupportsVectorScalar(typ1, typ2 types.Type) bool {

	if (typ1.Oid.IsArrayRelate() && typ2.IsNumeric()) || // Vec + Scalar
		(typ1.IsNumeric() && typ2.Oid.IsArrayRelate()) { // Scalar + Vec
		return true
	}
	return false
}

func plusOperatorSupports(typ1, typ2 types.Type) bool {
	if typ1.Oid != typ2.Oid {
		return false
	}
	switch typ1.Oid {
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64, types.T_bit:
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
	case types.T_float32, types.T_float64:
	case types.T_decimal64, types.T_decimal128:
	case types.T_array_float32, types.T_array_float64:
	default:
		return false
	}
	return true
}

func minusOperatorSupportsVectorScalar(typ1, typ2 types.Type) bool {
	if typ1.Oid.IsArrayRelate() && typ2.IsNumeric() { // Vec - Scalar
		return true
	}
	return false
}

func minusOperatorSupports(typ1, typ2 types.Type) bool {
	if typ1.Oid != typ2.Oid {
		return false
	}
	switch typ1.Oid {
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64, types.T_bit:
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
	case types.T_float32, types.T_float64:
	case types.T_decimal64, types.T_decimal128:
	case types.T_date, types.T_datetime:
	case types.T_array_float32, types.T_array_float64:
	default:
		return false
	}
	return true
}
func multiOperatorSupportsVectorScalar(typ1, typ2 types.Type) bool {
	if (typ1.Oid.IsArrayRelate() && typ2.IsNumeric()) || // Vec * Scalar
		(typ1.IsNumeric() && typ2.Oid.IsArrayRelate()) { // Scalar * Vec
		return true
	}
	return false
}
func multiOperatorSupports(typ1, typ2 types.Type) bool {
	if typ1.Oid != typ2.Oid {
		return false
	}
	switch typ1.Oid {
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64, types.T_bit:
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
	case types.T_float32, types.T_float64:
	case types.T_decimal64, types.T_decimal128:
	case types.T_array_float32, types.T_array_float64:
	default:
		return false
	}
	return true
}

func divOperatorSupportsVectorScalar(typ1, typ2 types.Type) bool {
	if typ1.Oid.IsArrayRelate() && typ2.IsNumeric() { // Vec / Scalar
		return true
	}
	return false
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
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64, types.T_bit:
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
	case types.T_float32, types.T_float64:
	case types.T_decimal128, types.T_decimal64:
	default:
		return false
	}
	return true
}

func vectorScalarOp[T types.RealNumbers](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, op string) (err error) {
	rs := vector.MustFunctionResult[types.Varlena](result)
	vs := vector.GenerateFunctionStrParameter(ivecs[0])
	num := vector.GenerateFunctionFixedTypeParameter[T](ivecs[1])

	for i := uint64(0); i < uint64(length); i++ {
		vec, null1 := vs.GetStrValue(i)
		sca, null2 := num.GetValue(i)

		if null1 || null2 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			out, err := moarray.ScalarOp[T](types.BytesToArray[T](vec), op, float64(sca))
			if err != nil {
				return err
			}

			if err = rs.AppendBytes(types.ArrayToBytes[T](out), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func plusFnVectorScalar(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	vectorIdx, scalarIdx := 0, 1
	if parameters[1].GetType().Oid.IsArrayRelate() {
		vectorIdx, scalarIdx = 1, 0
	}

	vectorAndScalarParams := []*vector.Vector{parameters[vectorIdx], parameters[scalarIdx]}
	if parameters[vectorIdx].GetType().Oid == types.T_array_float32 {
		return vectorScalarOp[float32](vectorAndScalarParams, result, proc, length, "+")
	} else {
		return vectorScalarOp[float64](vectorAndScalarParams, result, proc, length, "+")
	}
}

func plusFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	paramType := parameters[0].GetType()
	switch paramType.Oid {
	case types.T_bit:
		return opBinaryFixedFixedToFixed[uint64, uint64, uint64](parameters, result, proc, length, func(v1, v2 uint64) uint64 {
			return v1 + v2
		})
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
		return decimal128ArithArray(parameters, result, proc, length, decimal128AddArray)

	case types.T_array_float32:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, plusFnArray[float32])
	case types.T_array_float64:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, plusFnArray[float64])
	}
	panic("unreached code")
}

func minusFnVectorScalar(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	vectorIdx, scalarIdx := 0, 1
	vectorAndScalarParams := []*vector.Vector{parameters[vectorIdx], parameters[scalarIdx]}
	if parameters[vectorIdx].GetType().Oid == types.T_array_float32 {
		return vectorScalarOp[float32](vectorAndScalarParams, result, proc, length, "-")
	} else {
		return vectorScalarOp[float64](vectorAndScalarParams, result, proc, length, "-")
	}
}

func minusFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	paramType := parameters[0].GetType()
	switch paramType.Oid {
	case types.T_bit:
		return opBinaryFixedFixedToFixed[uint64, uint64, uint64](parameters, result, proc, length, func(v1, v2 uint64) uint64 {
			return v1 - v2
		})
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
		return decimal128ArithArray(parameters, result, proc, length, decimal128SubArray)

	case types.T_date:
		return opBinaryFixedFixedToFixed[types.Date, types.Date, int64](parameters, result, proc, length, func(v1, v2 types.Date) int64 {
			return int64(v1 - v2)
		})
	case types.T_datetime:
		return opBinaryFixedFixedToFixed[types.Datetime, types.Datetime, int64](parameters, result, proc, length, func(v1, v2 types.Datetime) int64 {
			return v1.DatetimeMinusWithSecond(v2)
		})
	case types.T_array_float32:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, minusFnArray[float32])
	case types.T_array_float64:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, minusFnArray[float64])
	}
	panic("unreached code")
}

func multiFnVectorScalar(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	vectorIdx, scalarIdx := 0, 1
	if parameters[1].GetType().Oid.IsArrayRelate() {
		vectorIdx, scalarIdx = 1, 0
	}

	vectorAndScalarParams := []*vector.Vector{parameters[vectorIdx], parameters[scalarIdx]}
	if parameters[vectorIdx].GetType().Oid == types.T_array_float32 {
		return vectorScalarOp[float32](vectorAndScalarParams, result, proc, length, "*")
	} else {
		return vectorScalarOp[float64](vectorAndScalarParams, result, proc, length, "*")
	}
}

func multiFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	paramType := parameters[0].GetType()
	switch paramType.Oid {
	case types.T_bit:
		return opBinaryFixedFixedToFixed[uint64, uint64, uint64](parameters, result, proc, length, func(v1, v2 uint64) uint64 {
			return v1 * v2
		})
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
		return decimal128ArithArray(parameters, result, proc, length, decimal128MultiArray)

	case types.T_array_float32:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, multiFnArray[float32])
	case types.T_array_float64:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, multiFnArray[float64])
	}
	panic("unreached code")
}

func divFnVectorScalar(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	vectorIdx, scalarIdx := 0, 1
	vectorAndScalarParams := []*vector.Vector{parameters[vectorIdx], parameters[scalarIdx]}
	if parameters[vectorIdx].GetType().Oid == types.T_array_float32 {
		return vectorScalarOp[float32](vectorAndScalarParams, result, proc, length, "/")
	} else {
		return vectorScalarOp[float64](vectorAndScalarParams, result, proc, length, "/")
	}
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
	case types.T_bit:
		return specialTemplateForModFunction[uint64](parameters, result, proc, length, func(v1, v2 uint64) uint64 {
			return v1 % v2
		})
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

	_v1 := types.BytesToArray[T](v1)
	_v2 := types.BytesToArray[T](v2)

	r, err := moarray.Add(_v1, _v2)
	if err != nil {
		return nil, err
	}

	return types.ArrayToBytes[T](r), nil
}

func minusFnArray[T types.RealNumbers](v1, v2 []byte) ([]byte, error) {

	_v1 := types.BytesToArray[T](v1)
	_v2 := types.BytesToArray[T](v2)

	r, err := moarray.Subtract(_v1, _v2)
	if err != nil {
		return nil, err
	}

	return types.ArrayToBytes[T](r), nil
}

func multiFnArray[T types.RealNumbers](v1, v2 []byte) ([]byte, error) {

	_v1 := types.BytesToArray[T](v1)
	_v2 := types.BytesToArray[T](v2)

	r, err := moarray.Multiply(_v1, _v2)
	if err != nil {
		return nil, err
	}

	return types.ArrayToBytes[T](r), nil
}

func divFnArray[T types.RealNumbers](v1, v2 []byte) ([]byte, error) {

	_v1 := types.BytesToArray[T](v1)
	_v2 := types.BytesToArray[T](v2)

	r, err := moarray.Divide(_v1, _v2)
	if err != nil {
		return nil, err
	}

	return types.ArrayToBytes[T](r), nil
}

func decimal128ScaleArray(v, rs []types.Decimal128, len int, n int32) error {
	for i := 0; i < len; i++ {
		rs[i] = v[i]
		err := rs[i].ScaleInplace(n)
		if err != nil {
			return err
		}
	}
	return nil
}

func decimal128ScaleArrayWithNulls(v, rs []types.Decimal128, len int, n int32, null1, null2 *nulls.Nulls) error {
	for i := 0; i < len; i++ {
		if null1.Contains(uint64(i)) || null2.Contains(uint64(i)) {
			continue
		}
		rs[i] = v[i]
		err := rs[i].ScaleInplace(n)
		if err != nil {
			return err
		}
	}
	return nil
}

func decimal128AddArray(v1, v2, rs []types.Decimal128, scale1, scale2 int32, null1, null2 *nulls.Nulls) error {
	len1 := len(v1)
	len2 := len(v2)
	var err error
	if null1.IsEmpty() && null2.IsEmpty() {
		if len1 == len2 {
			// all vector, or all constant
			if scale1 > scale2 {
				err = decimal128ScaleArray(v2, rs, len2, scale1-scale2)
				if err != nil {
					return err
				}
				for i := 0; i < len1; i++ {
					err = rs[i].AddInplace(&v1[i])
					if err != nil {
						return err
					}
				}
			} else if scale1 < scale2 {
				err = decimal128ScaleArray(v1, rs, len1, scale2-scale1)
				if err != nil {
					return err
				}
				for i := 0; i < len1; i++ {
					err = rs[i].AddInplace(&v2[i])
					if err != nil {
						return err
					}
				}
			} else {
				for i := 0; i < len1; i++ {
					rs[i] = v1[i]
					err = rs[i].AddInplace(&v2[i])
					if err != nil {
						return err
					}
				}
			}
		} else {
			if len1 == 1 {
				// v1 constant, v2 vector
				if scale1 > scale2 {
					err = decimal128ScaleArray(v2, rs, len2, scale1-scale2)
					if err != nil {
						return err
					}
					for i := 0; i < len2; i++ {
						err = rs[i].AddInplace(&v1[0])
						if err != nil {
							return err
						}
					}
				} else if scale1 < scale2 {
					err = decimal128ScaleArray(v1, rs, len1, scale2-scale1)
					if err != nil {
						return err
					}
					tmp := rs[0]
					for i := 0; i < len2; i++ {
						rs[i] = tmp
						err = rs[i].AddInplace(&v2[i])
						if err != nil {
							return err
						}
					}
				} else {
					tmp := v1[0]
					for i := 0; i < len2; i++ {
						rs[i] = tmp
						err = rs[i].AddInplace(&v2[i])
						if err != nil {
							return err
						}
					}
				}
			} else {
				// v1 vector, v2 constant
				if scale1 > scale2 {
					err = decimal128ScaleArray(v2, rs, len2, scale1-scale2)
					if err != nil {
						return err
					}
					tmp := rs[0]
					for i := 0; i < len1; i++ {
						rs[i] = tmp
						err = rs[i].AddInplace(&v1[i])
						if err != nil {
							return err
						}
					}
				} else if scale1 < scale2 {
					err = decimal128ScaleArray(v1, rs, len1, scale2-scale1)
					if err != nil {
						return err
					}
					for i := 0; i < len1; i++ {
						err = rs[i].AddInplace(&v2[0])
						if err != nil {
							return err
						}
					}
				} else {
					tmp := v2[0]
					for i := 0; i < len1; i++ {
						rs[i] = tmp
						err = rs[i].AddInplace(&v1[i])
						if err != nil {
							return err
						}
					}
				}
			}
		}
	} else {
		if len1 == len2 {
			// all vector, or all constant
			if scale1 > scale2 {
				err = decimal128ScaleArrayWithNulls(v2, rs, len2, scale1-scale2, null1, null2)
				if err != nil {
					return err
				}
				for i := 0; i < len1; i++ {
					if null1.Contains(uint64(i)) || null2.Contains(uint64(i)) {
						continue
					}
					err = rs[i].AddInplace(&v1[i])
					if err != nil {
						return err
					}
				}
			} else if scale1 < scale2 {
				err = decimal128ScaleArrayWithNulls(v1, rs, len1, scale2-scale1, null1, null2)
				if err != nil {
					return err
				}
				for i := 0; i < len1; i++ {
					if null1.Contains(uint64(i)) || null2.Contains(uint64(i)) {
						continue
					}
					err = rs[i].AddInplace(&v2[i])
					if err != nil {
						return err
					}
				}
			} else {
				for i := 0; i < len1; i++ {
					if null1.Contains(uint64(i)) || null2.Contains(uint64(i)) {
						continue
					}
					rs[i] = v1[i]
					err = rs[i].AddInplace(&v2[i])
					if err != nil {
						return err
					}
				}
			}
		} else {
			if len1 == 1 {
				// v1 constant, v2 vector
				if null1.Contains(0) {
					return nil
				}
				if scale1 > scale2 {
					err = decimal128ScaleArrayWithNulls(v2, rs, len2, scale1-scale2, null1, null2)
					if err != nil {
						return err
					}
					for i := 0; i < len2; i++ {
						if null2.Contains(uint64(i)) {
							continue
						}
						err = rs[i].AddInplace(&v1[0])
						if err != nil {
							return err
						}
					}
				} else if scale1 < scale2 {
					err = decimal128ScaleArray(v1, rs, len1, scale2-scale1)
					if err != nil {
						return err
					}
					tmp := rs[0]
					for i := 0; i < len2; i++ {
						if null2.Contains(uint64(i)) {
							continue
						}
						rs[i] = tmp
						err = rs[i].AddInplace(&v2[i])
						if err != nil {
							return err
						}
					}
				} else {
					tmp := v1[0]
					for i := 0; i < len2; i++ {
						if null2.Contains(uint64(i)) {
							continue
						}
						rs[i] = tmp
						err = rs[i].AddInplace(&v2[i])
						if err != nil {
							return err
						}
					}
				}
			} else {
				// v1 vector, v2 constant
				if null2.Contains(0) {
					return nil
				}
				if scale1 > scale2 {
					err = decimal128ScaleArray(v2, rs, len2, scale1-scale2)
					if err != nil {
						return err
					}
					tmp := rs[0]
					for i := 0; i < len1; i++ {
						if null1.Contains(uint64(i)) {
							continue
						}
						rs[i] = tmp
						err = rs[i].AddInplace(&v1[i])
						if err != nil {
							return err
						}
					}
				} else if scale1 < scale2 {
					err = decimal128ScaleArrayWithNulls(v1, rs, len1, scale2-scale1, null1, null2)
					if err != nil {
						return err
					}
					for i := 0; i < len1; i++ {
						if null1.Contains(uint64(i)) {
							continue
						}
						err = rs[i].AddInplace(&v2[0])
						if err != nil {
							return err
						}
					}
				} else {
					tmp := v2[0]
					for i := 0; i < len1; i++ {
						if null1.Contains(uint64(i)) {
							continue
						}
						rs[i] = tmp
						err = rs[i].AddInplace(&v1[i])
						if err != nil {
							return err
						}
					}
				}
			}
		}
	}

	return nil
}

func decimal128SubArray(v1, v2, rs []types.Decimal128, scale1, scale2 int32, null1, null2 *nulls.Nulls) error {
	len1 := len(v1)
	len2 := len(v2)
	var err error

	if null1.IsEmpty() && null2.IsEmpty() {
		if len1 == len2 {
			// all vector, or all constant
			if scale1 > scale2 {
				err = decimal128ScaleArray(v2, rs, len2, scale1-scale2)
				if err != nil {
					return err
				}
				for i := 0; i < len1; i++ {
					rs[i].MinusInplace()
					err = rs[i].AddInplace(&v1[i])
					if err != nil {
						return err
					}
				}
			} else if scale1 < scale2 {
				err = decimal128ScaleArray(v1, rs, len1, scale2-scale1)
				if err != nil {
					return err
				}
				for i := 0; i < len1; i++ {
					rs[i].MinusInplace()
					err = rs[i].AddInplace(&v2[i])
					if err != nil {
						return err
					}
					rs[i].MinusInplace()
				}
			} else {
				for i := 0; i < len1; i++ {
					rs[i] = v2[i]
					rs[i].MinusInplace()
					err = rs[i].AddInplace(&v1[i])
					if err != nil {
						return err
					}
				}
			}
		} else {
			if len1 == 1 {
				// v1 constant, v2 vector
				if scale1 > scale2 {
					err = decimal128ScaleArray(v2, rs, len2, scale1-scale2)
					if err != nil {
						return err
					}
					for i := 0; i < len2; i++ {
						rs[i].MinusInplace()
						err = rs[i].AddInplace(&v1[0])
						if err != nil {
							return err
						}
					}
				} else if scale1 < scale2 {
					err = decimal128ScaleArray(v1, rs, len1, scale2-scale1)
					if err != nil {
						return err
					}
					tmp := rs[0]
					tmp.MinusInplace()
					for i := 0; i < len2; i++ {
						rs[i] = tmp
						err = rs[i].AddInplace(&v2[i])
						rs[i].MinusInplace()
						if err != nil {
							return err
						}
					}
				} else {
					tmp := v1[0]
					tmp.MinusInplace()
					for i := 0; i < len2; i++ {
						rs[i] = tmp
						err = rs[i].AddInplace(&v2[i])
						rs[i].MinusInplace()
						if err != nil {
							return err
						}
					}
				}
			} else {
				// v1 vector, v2 constant
				if scale1 > scale2 {
					err = decimal128ScaleArray(v2, rs, len2, scale1-scale2)
					if err != nil {
						return err
					}
					tmp := rs[0]
					tmp.MinusInplace()
					for i := 0; i < len1; i++ {
						rs[i] = tmp
						err = rs[i].AddInplace(&v1[i])
						if err != nil {
							return err
						}
					}
				} else if scale1 < scale2 {
					err = decimal128ScaleArray(v1, rs, len1, scale2-scale1)
					if err != nil {
						return err
					}
					for i := 0; i < len1; i++ {
						rs[i].MinusInplace()
						err = rs[i].AddInplace(&v2[0])
						if err != nil {
							return err
						}
						rs[i].MinusInplace()
					}
				} else {
					tmp := v2[0]
					tmp.MinusInplace()
					for i := 0; i < len1; i++ {
						rs[i] = tmp
						err = rs[i].AddInplace(&v1[i])
						if err != nil {
							return err
						}
					}
				}
			}
		}
	} else {
		if len1 == len2 {
			// all vector, or all constant
			if scale1 > scale2 {
				err = decimal128ScaleArrayWithNulls(v2, rs, len2, scale1-scale2, null1, null2)
				if err != nil {
					return err
				}
				for i := 0; i < len1; i++ {
					if null1.Contains(uint64(i)) || null2.Contains(uint64(i)) {
						continue
					}
					rs[i].MinusInplace()
					err = rs[i].AddInplace(&v1[i])
					if err != nil {
						return err
					}
				}
			} else if scale1 < scale2 {
				err = decimal128ScaleArrayWithNulls(v1, rs, len1, scale2-scale1, null1, null2)
				if err != nil {
					return err
				}
				for i := 0; i < len1; i++ {
					if null1.Contains(uint64(i)) || null2.Contains(uint64(i)) {
						continue
					}
					rs[i].MinusInplace()
					err = rs[i].AddInplace(&v2[i])
					if err != nil {
						return err
					}
					rs[i].MinusInplace()
				}
			} else {
				for i := 0; i < len1; i++ {
					if null1.Contains(uint64(i)) || null2.Contains(uint64(i)) {
						continue
					}
					rs[i] = v2[i]
					rs[i].MinusInplace()
					err = rs[i].AddInplace(&v1[i])
					if err != nil {
						return err
					}
				}
			}
		} else {
			if len1 == 1 {
				// v1 constant, v2 vector
				if null1.Contains(0) {
					return nil
				}
				if scale1 > scale2 {
					err = decimal128ScaleArrayWithNulls(v2, rs, len2, scale1-scale2, null1, null2)
					if err != nil {
						return err
					}
					for i := 0; i < len2; i++ {
						if null2.Contains(uint64(i)) {
							continue
						}
						rs[i].MinusInplace()
						err = rs[i].AddInplace(&v1[0])
						if err != nil {
							return err
						}
					}
				} else if scale1 < scale2 {
					err = decimal128ScaleArray(v1, rs, len1, scale2-scale1)
					if err != nil {
						return err
					}
					tmp := rs[0]
					tmp.MinusInplace()
					for i := 0; i < len2; i++ {
						if null2.Contains(uint64(i)) {
							continue
						}
						rs[i] = tmp
						err = rs[i].AddInplace(&v2[i])
						rs[i].MinusInplace()
						if err != nil {
							return err
						}
					}
				} else {
					tmp := v1[0]
					tmp.MinusInplace()
					for i := 0; i < len2; i++ {
						if null2.Contains(uint64(i)) {
							continue
						}
						rs[i] = tmp
						err = rs[i].AddInplace(&v2[i])
						rs[i].MinusInplace()
						if err != nil {
							return err
						}
					}
				}
			} else {
				// v1 vector, v2 constant
				if null2.Contains(0) {
					return nil
				}
				if scale1 > scale2 {
					err = decimal128ScaleArray(v2, rs, len2, scale1-scale2)
					if err != nil {
						return err
					}
					tmp := rs[0]
					tmp.MinusInplace()
					for i := 0; i < len1; i++ {
						if null1.Contains(uint64(i)) {
							continue
						}
						rs[i] = tmp
						err = rs[i].AddInplace(&v1[i])
						if err != nil {
							return err
						}
					}
				} else if scale1 < scale2 {
					err = decimal128ScaleArrayWithNulls(v1, rs, len1, scale2-scale1, null1, null2)
					if err != nil {
						return err
					}
					for i := 0; i < len1; i++ {
						if null1.Contains(uint64(i)) {
							continue
						}
						rs[i].MinusInplace()
						err = rs[i].AddInplace(&v2[0])
						if err != nil {
							return err
						}
						rs[i].MinusInplace()
					}
				} else {
					tmp := v2[0]
					tmp.MinusInplace()
					for i := 0; i < len1; i++ {
						if null1.Contains(uint64(i)) {
							continue
						}
						rs[i] = tmp
						err = rs[i].AddInplace(&v1[i])
						if err != nil {
							return err
						}
					}
				}
			}
		}
	}
	return nil
}

func decimal128MultiArray(v1, v2, rs []types.Decimal128, scale1, scale2 int32, null1, null2 *nulls.Nulls) error {
	len1 := len(v1)
	len2 := len(v2)
	var err error

	if null1.IsEmpty() && null2.IsEmpty() {
		var scale int32 = 12
		if scale1 > scale {
			scale = scale1
		}
		if scale2 > scale {
			scale = scale2
		}
		if scale1+scale2 < scale {
			scale = scale1 + scale2
		}
		scale = scale - scale1 - scale2

		if len1 == len2 {
			for i := 0; i < len1; i++ {
				rs[i] = v1[i]
				err = rs[i].MulInplace(&v2[i], scale, scale1, scale2)
				if err != nil {
					return err
				}
			}
		} else {
			if len1 == 1 {
				for i := 0; i < len2; i++ {
					rs[i] = v1[0]
					err = rs[i].MulInplace(&v2[i], scale, scale1, scale2)
					if err != nil {
						return err
					}
				}
			} else {
				for i := 0; i < len1; i++ {
					rs[i] = v1[i]
					err = rs[i].MulInplace(&v2[0], scale, scale1, scale2)
					if err != nil {
						return err
					}
				}
			}
		}
	} else {
		var scale int32 = 12
		if scale1 > scale {
			scale = scale1
		}
		if scale2 > scale {
			scale = scale2
		}
		if scale1+scale2 < scale {
			scale = scale1 + scale2
		}
		scale = scale - scale1 - scale2

		if len1 == len2 {
			for i := 0; i < len1; i++ {
				if null1.Contains(uint64(i)) || null2.Contains(uint64(i)) {
					continue
				}
				rs[i] = v1[i]
				err = rs[i].MulInplace(&v2[i], scale, scale1, scale2)
				if err != nil {
					return err
				}
			}
		} else {
			if len1 == 1 {
				if null1.Contains(0) {
					return nil
				}
				for i := 0; i < len2; i++ {
					if null2.Contains(uint64(i)) {
						continue
					}
					rs[i] = v1[0]
					err = rs[i].MulInplace(&v2[i], scale, scale1, scale2)
					if err != nil {
						return err
					}
				}
			} else {
				if null2.Contains(0) {
					return nil
				}
				for i := 0; i < len1; i++ {
					if null1.Contains(uint64(i)) {
						continue
					}
					rs[i] = v1[i]
					err = rs[i].MulInplace(&v2[0], scale, scale1, scale2)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}
