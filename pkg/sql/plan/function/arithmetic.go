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
	"context"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
	case types.T_decimal64, types.T_decimal128, types.T_decimal256:
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
	case types.T_decimal64, types.T_decimal128, types.T_decimal256:
	case types.T_date, types.T_datetime:
	case types.T_array_float32, types.T_array_float64:
	case types.T_year:
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
	case types.T_decimal64, types.T_decimal128, types.T_decimal256:
	case types.T_array_float32, types.T_array_float64:
	case types.T_year:
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
	case types.T_decimal64, types.T_decimal128, types.T_decimal256:
	case types.T_array_float32, types.T_array_float64:
	case types.T_year:
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
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128, types.T_decimal256:
		return true
	default:
		return false
	}
}

func modOperatorSupports(typ1, typ2 types.Type) bool {
	if typ1.Oid != typ2.Oid {
		return false
	}
	switch typ1.Oid {
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64, types.T_bit:
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
	case types.T_float32, types.T_float64:
	case types.T_decimal128, types.T_decimal64, types.T_decimal256:
	default:
		return false
	}
	return true
}

func vectorScalarOp[T types.RealNumbers](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, op string, selectList *FunctionSelectList) (err error) {
	rs := vector.MustFunctionResult[types.Varlena](result)
	vs := vector.GenerateFunctionStrParameter(ivecs[0])
	num := vector.GenerateFunctionFixedTypeParameter[T](ivecs[1])

	if selectList == nil || !selectList.ShouldEvalAllRow() {
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
	} else {
		for i := uint64(0); i < uint64(length); i++ {
			if selectList.Contains(i) {
				if err = rs.AppendBytes(nil, true); err != nil {
					return err
				}
				continue
			}
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
	}

	return nil
}

func plusFnVectorScalar(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	vectorIdx, scalarIdx := 0, 1
	if parameters[1].GetType().Oid.IsArrayRelate() {
		vectorIdx, scalarIdx = 1, 0
	}

	vectorAndScalarParams := []*vector.Vector{parameters[vectorIdx], parameters[scalarIdx]}
	if parameters[vectorIdx].GetType().Oid == types.T_array_float32 {
		return vectorScalarOp[float32](vectorAndScalarParams, result, proc, length, "+", selectList)
	} else {
		return vectorScalarOp[float64](vectorAndScalarParams, result, proc, length, "+", selectList)
	}
}

func plusFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	// Check result type first, as it may differ from parameter types after type conversion
	resultType := result.GetResultVector().GetType()

	// If result type is decimal128, use decimal128 handler
	// This handles cases like decimal64 + float64 where both are converted to decimal128
	if resultType.Oid == types.T_decimal128 {
		// If inputs have no nulls, ensure result nulls are cleared after computation.
		inputHasNull := func(vec *vector.Vector) bool {
			ns := vec.GetNulls()
			return ns != nil && !ns.IsEmpty()
		}
		noNullInput := !inputHasNull(parameters[0]) && !inputHasNull(parameters[1])

		if err := decimal128ArithArray(parameters, result, proc, length, decimal128AddArray, selectList); err != nil {
			return err
		}
		if noNullInput {
			result.GetResultVector().GetNulls().Reset()
		}
		return nil
	}

	// Result-type driven: handles cross-type promotions where inputs may not
	// yet be decimal256 but the planner chose decimal256 as the result type
	// (e.g. decimal128 + int64 → decimal256).  The paramType-based switch
	// below covers the case where inputs are already decimal256.
	if resultType.Oid == types.T_decimal256 {
		if err := decimalArith[types.Decimal256](parameters, result, proc, length, func(v1, v2 types.Decimal256, scale1, scale2 int32) (types.Decimal256, error) {
			r, _, err := v1.Add(v2, scale1, scale2)
			return r, err
		}, selectList, false); err != nil {
			return err
		}
		return nil
	}

	paramType := parameters[0].GetType()

	switch paramType.Oid {
	case types.T_bit:
		return opBinaryFixedFixedToFixed[uint64, uint64, uint64](parameters, result, proc, length, func(v1, v2 uint64) uint64 {
			return v1 + v2
		}, selectList)
	case types.T_uint8:
		return opBinaryFixedFixedToFixed[uint8, uint8, uint8](parameters, result, proc, length, func(v1, v2 uint8) uint8 {
			return v1 + v2
		}, selectList)
	case types.T_uint16:
		return opBinaryFixedFixedToFixed[uint16, uint16, uint16](parameters, result, proc, length, func(v1, v2 uint16) uint16 {
			return v1 + v2
		}, selectList)
	case types.T_uint32:
		return opBinaryFixedFixedToFixed[uint32, uint32, uint32](parameters, result, proc, length, func(v1, v2 uint32) uint32 {
			return v1 + v2
		}, selectList)
	case types.T_uint64:
		return opBinaryFixedFixedToFixed[uint64, uint64, uint64](parameters, result, proc, length, func(v1, v2 uint64) uint64 {
			return v1 + v2
		}, selectList)
	case types.T_int8:
		return opBinaryFixedFixedToFixed[int8, int8, int8](parameters, result, proc, length, func(v1, v2 int8) int8 {
			return v1 + v2
		}, selectList)
	case types.T_int16:
		return opBinaryFixedFixedToFixed[int16, int16, int16](parameters, result, proc, length, func(v1, v2 int16) int16 {
			return v1 + v2
		}, selectList)
	case types.T_int32:
		return opBinaryFixedFixedToFixed[int32, int32, int32](parameters, result, proc, length, func(v1, v2 int32) int32 {
			return v1 + v2
		}, selectList)
	case types.T_int64:
		return opBinaryFixedFixedToFixed[int64, int64, int64](parameters, result, proc, length, func(v1, v2 int64) int64 {
			return v1 + v2
		}, selectList)
	case types.T_float32:
		return opBinaryFixedFixedToFixedWithErrorCheck[float32, float32, float32](parameters, result, proc, length, func(v1, v2 float32) (float32, error) {
			return addFloat32WithOverflowCheck(proc.Ctx, v1, v2)
		}, selectList)
	case types.T_float64:
		return opBinaryFixedFixedToFixedWithErrorCheck[float64, float64, float64](parameters, result, proc, length, func(v1, v2 float64) (float64, error) {
			return addFloat64WithOverflowCheck(proc.Ctx, v1, v2)
		}, selectList)
	case types.T_decimal64:
		return decimalArith[types.Decimal64](parameters, result, proc, length, func(v1, v2 types.Decimal64, scale1, scale2 int32) (types.Decimal64, error) {
			r, _, err := v1.Add(v2, scale1, scale2)
			return r, err
		}, selectList, false)
	case types.T_decimal128:
		return decimal128ArithArray(parameters, result, proc, length, decimal128AddArray, selectList)
	case types.T_decimal256:
		return decimalArith[types.Decimal256](parameters, result, proc, length, func(v1, v2 types.Decimal256, scale1, scale2 int32) (types.Decimal256, error) {
			r, _, err := v1.Add(v2, scale1, scale2)
			return r, err
		}, selectList, false)

	case types.T_array_float32:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, plusFnArray[float32], selectList)
	case types.T_array_float64:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, plusFnArray[float64], selectList)
	}
	panic("unreached code")
}

func minusFnVectorScalar(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	vectorIdx, scalarIdx := 0, 1
	vectorAndScalarParams := []*vector.Vector{parameters[vectorIdx], parameters[scalarIdx]}
	if parameters[vectorIdx].GetType().Oid == types.T_array_float32 {
		return vectorScalarOp[float32](vectorAndScalarParams, result, proc, length, "-", selectList)
	} else {
		return vectorScalarOp[float64](vectorAndScalarParams, result, proc, length, "-", selectList)
	}
}

func minusFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	paramType := parameters[0].GetType()
	switch paramType.Oid {
	case types.T_bit:
		return opBinaryFixedFixedToFixed[uint64, uint64, uint64](parameters, result, proc, length, func(v1, v2 uint64) uint64 {
			return v1 - v2
		}, selectList)
	case types.T_uint8:
		return opBinaryFixedFixedToFixed[uint8, uint8, uint8](parameters, result, proc, length, func(v1, v2 uint8) uint8 {
			return v1 - v2
		}, selectList)
	case types.T_uint16:
		return opBinaryFixedFixedToFixed[uint16, uint16, uint16](parameters, result, proc, length, func(v1, v2 uint16) uint16 {
			return v1 - v2
		}, selectList)
	case types.T_uint32:
		return opBinaryFixedFixedToFixed[uint32, uint32, uint32](parameters, result, proc, length, func(v1, v2 uint32) uint32 {
			return v1 - v2
		}, selectList)
	case types.T_uint64:
		return opBinaryFixedFixedToFixed[uint64, uint64, uint64](parameters, result, proc, length, func(v1, v2 uint64) uint64 {
			return v1 - v2
		}, selectList)
	case types.T_int8:
		return opBinaryFixedFixedToFixed[int8, int8, int8](parameters, result, proc, length, func(v1, v2 int8) int8 {
			return v1 - v2
		}, selectList)
	case types.T_int16:
		return opBinaryFixedFixedToFixed[int16, int16, int16](parameters, result, proc, length, func(v1, v2 int16) int16 {
			return v1 - v2
		}, selectList)
	case types.T_int32:
		return opBinaryFixedFixedToFixed[int32, int32, int32](parameters, result, proc, length, func(v1, v2 int32) int32 {
			return v1 - v2
		}, selectList)
	case types.T_int64:
		return opBinaryFixedFixedToFixed[int64, int64, int64](parameters, result, proc, length, func(v1, v2 int64) int64 {
			return v1 - v2
		}, selectList)
	case types.T_float32:
		return opBinaryFixedFixedToFixed[float32, float32, float32](parameters, result, proc, length, func(v1, v2 float32) float32 {
			return v1 - v2
		}, selectList)
	case types.T_float64:
		return opBinaryFixedFixedToFixed[float64, float64, float64](parameters, result, proc, length, func(v1, v2 float64) float64 {
			return v1 - v2
		}, selectList)
	case types.T_decimal64:
		return decimalArith[types.Decimal64](parameters, result, proc, length, func(v1, v2 types.Decimal64, scale1, scale2 int32) (types.Decimal64, error) {
			r, _, err := v1.Sub(v2, scale1, scale2)
			return r, err
		}, selectList, false)
	case types.T_decimal128:
		return decimal128ArithArray(parameters, result, proc, length, decimal128SubArray, selectList)
	case types.T_decimal256:
		return decimalArith[types.Decimal256](parameters, result, proc, length, func(v1, v2 types.Decimal256, scale1, scale2 int32) (types.Decimal256, error) {
			r, _, err := v1.Sub(v2, scale1, scale2)
			return r, err
		}, selectList, false)

	case types.T_date:
		return opBinaryFixedFixedToFixed[types.Date, types.Date, int64](parameters, result, proc, length, func(v1, v2 types.Date) int64 {
			return int64(v1 - v2)
		}, selectList)
	case types.T_datetime:
		return opBinaryFixedFixedToFixed[types.Datetime, types.Datetime, int64](parameters, result, proc, length, func(v1, v2 types.Datetime) int64 {
			return v1.DatetimeMinusWithSecond(v2)
		}, selectList)
	case types.T_array_float32:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, minusFnArray[float32], selectList)
	case types.T_array_float64:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, minusFnArray[float64], selectList)
	case types.T_year:
		return opBinaryFixedFixedToFixed[types.MoYear, types.MoYear, int64](parameters, result, proc, length, func(v1, v2 types.MoYear) int64 {
			return int64(v1 - v2)
		}, selectList)
	}
	panic("unreached code")
}

func multiFnVectorScalar(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	vectorIdx, scalarIdx := 0, 1
	if parameters[1].GetType().Oid.IsArrayRelate() {
		vectorIdx, scalarIdx = 1, 0
	}

	vectorAndScalarParams := []*vector.Vector{parameters[vectorIdx], parameters[scalarIdx]}
	if parameters[vectorIdx].GetType().Oid == types.T_array_float32 {
		return vectorScalarOp[float32](vectorAndScalarParams, result, proc, length, "*", selectList)
	} else {
		return vectorScalarOp[float64](vectorAndScalarParams, result, proc, length, "*", selectList)
	}
}

func multiFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	paramType := parameters[0].GetType()
	switch paramType.Oid {
	case types.T_bit:
		return opBinaryFixedFixedToFixed[uint64, uint64, uint64](parameters, result, proc, length, func(v1, v2 uint64) uint64 {
			return v1 * v2
		}, selectList)
	case types.T_uint8:
		return opBinaryFixedFixedToFixed[uint8, uint8, uint8](parameters, result, proc, length, func(v1, v2 uint8) uint8 {
			return v1 * v2
		}, selectList)
	case types.T_uint16:
		return opBinaryFixedFixedToFixed[uint16, uint16, uint16](parameters, result, proc, length, func(v1, v2 uint16) uint16 {
			return v1 * v2
		}, selectList)
	case types.T_uint32:
		return opBinaryFixedFixedToFixed[uint32, uint32, uint32](parameters, result, proc, length, func(v1, v2 uint32) uint32 {
			return v1 * v2
		}, selectList)
	case types.T_uint64:
		return opBinaryFixedFixedToFixed[uint64, uint64, uint64](parameters, result, proc, length, func(v1, v2 uint64) uint64 {
			return v1 * v2
		}, selectList)
	case types.T_int8:
		return opBinaryFixedFixedToFixed[int8, int8, int8](parameters, result, proc, length, func(v1, v2 int8) int8 {
			return v1 * v2
		}, selectList)
	case types.T_int16:
		return opBinaryFixedFixedToFixed[int16, int16, int16](parameters, result, proc, length, func(v1, v2 int16) int16 {
			return v1 * v2
		}, selectList)
	case types.T_int32:
		return opBinaryFixedFixedToFixed[int32, int32, int32](parameters, result, proc, length, func(v1, v2 int32) int32 {
			return v1 * v2
		}, selectList)
	case types.T_int64:
		return opBinaryFixedFixedToFixed[int64, int64, int64](parameters, result, proc, length, func(v1, v2 int64) int64 {
			return v1 * v2
		}, selectList)
	case types.T_float32:
		return opBinaryFixedFixedToFixed[float32, float32, float32](parameters, result, proc, length, func(v1, v2 float32) float32 {
			return v1 * v2
		}, selectList)
	case types.T_float64:
		return opBinaryFixedFixedToFixed[float64, float64, float64](parameters, result, proc, length, func(v1, v2 float64) float64 {
			return v1 * v2
		}, selectList)
	case types.T_decimal64:
		return decimalArith2(parameters, result, proc, length, func(x, y types.Decimal128, scale1, scale2 int32) (types.Decimal128, error) {
			rt, _, err := x.Mul(y, scale1, scale2)
			return rt, err
		}, selectList, false)
	case types.T_decimal128:
		return decimal128ArithArray(parameters, result, proc, length, decimal128MultiArray, selectList)
	case types.T_decimal256:
		return decimalArith[types.Decimal256](parameters, result, proc, length, func(v1, v2 types.Decimal256, scale1, scale2 int32) (types.Decimal256, error) {
			r, _, err := v1.Mul(v2, scale1, scale2)
			return r, err
		}, selectList, false)

	case types.T_array_float32:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, multiFnArray[float32], selectList)
	case types.T_array_float64:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, multiFnArray[float64], selectList)
	case types.T_year:
		return opBinaryFixedFixedToFixed[types.MoYear, types.MoYear, int64](parameters, result, proc, length, func(v1, v2 types.MoYear) int64 {
			return int64(v1) * int64(v2)
		}, selectList)
	}
	panic("unreached code")
}

func divFnVectorScalar(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	vectorIdx, scalarIdx := 0, 1
	vectorAndScalarParams := []*vector.Vector{parameters[vectorIdx], parameters[scalarIdx]}
	if parameters[vectorIdx].GetType().Oid == types.T_array_float32 {
		return vectorScalarOp[float32](vectorAndScalarParams, result, proc, length, "/", selectList)
	} else {
		return vectorScalarOp[float64](vectorAndScalarParams, result, proc, length, "/", selectList)
	}
}

func divFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	paramType := parameters[0].GetType()
	switch paramType.Oid {
	case types.T_float32:
		return specialTemplateForDivFunction[float32, float32](parameters, result, proc, length, func(v1, v2 float32) float32 {
			return v1 / v2
		}, selectList)
	case types.T_float64:
		return specialTemplateForDivFunction[float64, float64](parameters, result, proc, length, func(v1, v2 float64) float64 {
			return v1 / v2
		}, selectList)
	case types.T_decimal64:
		return decimalArith2(parameters, result, proc, length, func(x, y types.Decimal128, scale1, scale2 int32) (types.Decimal128, error) {
			rt, _, err := x.Div(y, scale1, scale2)
			return rt, err
		}, selectList, true)
	case types.T_decimal128:
		return decimalArith[types.Decimal128](parameters, result, proc, length, func(v1, v2 types.Decimal128, scale1, scale2 int32) (types.Decimal128, error) {
			r, _, err := v1.Div(v2, scale1, scale2)
			return r, err
		}, selectList, true)
	case types.T_decimal256:
		return decimalArith[types.Decimal256](parameters, result, proc, length, func(v1, v2 types.Decimal256, scale1, scale2 int32) (types.Decimal256, error) {
			r, _, err := v1.Div(v2, scale1, scale2)
			return r, err
		}, selectList, true)
	case types.T_array_float32:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, divFnArray[float32], selectList)
	case types.T_array_float64:
		return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length, divFnArray[float64], selectList)
	case types.T_year:
		return opBinaryFixedFixedToFixed[types.MoYear, types.MoYear, float64](parameters, result, proc, length, func(v1, v2 types.MoYear) float64 {
			if v2 == 0 {
				return math.NaN()
			}
			return float64(v1) / float64(v2)
		}, selectList)
	}
	panic("unreached code")
}

func integerDivFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	paramType := parameters[0].GetType()
	switch paramType.Oid {
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
		return integerDivSigned(parameters, result, proc, length, selectList)
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		return integerDivUnsigned(parameters, result, proc, length, selectList)
	case types.T_float32:
		return specialTemplateForDivFunction[float32, int64](parameters, result, proc, length, func(v1, v2 float32) int64 {
			return int64(v1 / v2)
		}, selectList)
	case types.T_float64:
		return specialTemplateForDivFunction[float64, int64](parameters, result, proc, length, func(v1, v2 float64) int64 {
			return int64(v1 / v2)
		}, selectList)
	case types.T_decimal64:
		return decimalIntegerDiv[types.Decimal64](parameters, result, proc, length, selectList)
	case types.T_decimal128:
		return decimalIntegerDiv[types.Decimal128](parameters, result, proc, length, selectList)
	case types.T_decimal256:
		return decimalIntegerDiv[types.Decimal256](parameters, result, proc, length, selectList)
	}
	panic("unreached code")
}

// integerDivSigned handles DIV for signed integer types
func integerDivSigned(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if length == 0 {
		return nil
	}

	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[int64](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedColNoTypeCheck[int64](rsVec)
	rsNull := rsVec.GetNulls()
	shouldError := checkDivisionByZeroBehavior(proc, selectList)

	switch paramType.Oid {
	case types.T_int8:
		p1 := vector.GenerateFunctionFixedTypeParameter[int8](parameters[0])
		p2 := vector.GenerateFunctionFixedTypeParameter[int8](parameters[1])
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetValue(i)
			v2, null2 := p2.GetValue(i)
			if null1 || null2 || v2 == 0 {
				if v2 == 0 && !null2 && shouldError {
					return moerr.NewDivByZeroNoCtx()
				}
				rsNull.Add(i)
			} else {
				rss[i] = int64(v1) / int64(v2)
			}
		}
	case types.T_int16:
		p1 := vector.GenerateFunctionFixedTypeParameter[int16](parameters[0])
		p2 := vector.GenerateFunctionFixedTypeParameter[int16](parameters[1])
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetValue(i)
			v2, null2 := p2.GetValue(i)
			if null1 || null2 || v2 == 0 {
				if v2 == 0 && !null2 && shouldError {
					return moerr.NewDivByZeroNoCtx()
				}
				rsNull.Add(i)
			} else {
				rss[i] = int64(v1) / int64(v2)
			}
		}
	case types.T_int32:
		p1 := vector.GenerateFunctionFixedTypeParameter[int32](parameters[0])
		p2 := vector.GenerateFunctionFixedTypeParameter[int32](parameters[1])
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetValue(i)
			v2, null2 := p2.GetValue(i)
			if null1 || null2 || v2 == 0 {
				if v2 == 0 && !null2 && shouldError {
					return moerr.NewDivByZeroNoCtx()
				}
				rsNull.Add(i)
			} else {
				rss[i] = int64(v1) / int64(v2)
			}
		}
	case types.T_int64:
		p1 := vector.GenerateFunctionFixedTypeParameter[int64](parameters[0])
		p2 := vector.GenerateFunctionFixedTypeParameter[int64](parameters[1])
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetValue(i)
			v2, null2 := p2.GetValue(i)
			if null1 || null2 || v2 == 0 {
				if v2 == 0 && !null2 && shouldError {
					return moerr.NewDivByZeroNoCtx()
				}
				rsNull.Add(i)
			} else {
				rss[i] = v1 / v2
			}
		}
	}
	return nil
}

// integerDivUnsigned handles DIV for unsigned integer types
func integerDivUnsigned(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if length == 0 {
		return nil
	}

	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[int64](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedColNoTypeCheck[int64](rsVec)
	rsNull := rsVec.GetNulls()
	shouldError := checkDivisionByZeroBehavior(proc, selectList)

	switch paramType.Oid {
	case types.T_uint8:
		p1 := vector.GenerateFunctionFixedTypeParameter[uint8](parameters[0])
		p2 := vector.GenerateFunctionFixedTypeParameter[uint8](parameters[1])
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetValue(i)
			v2, null2 := p2.GetValue(i)
			if null1 || null2 || v2 == 0 {
				if v2 == 0 && !null2 && shouldError {
					return moerr.NewDivByZeroNoCtx()
				}
				rsNull.Add(i)
			} else {
				rss[i] = int64(v1) / int64(v2)
			}
		}
	case types.T_uint16:
		p1 := vector.GenerateFunctionFixedTypeParameter[uint16](parameters[0])
		p2 := vector.GenerateFunctionFixedTypeParameter[uint16](parameters[1])
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetValue(i)
			v2, null2 := p2.GetValue(i)
			if null1 || null2 || v2 == 0 {
				if v2 == 0 && !null2 && shouldError {
					return moerr.NewDivByZeroNoCtx()
				}
				rsNull.Add(i)
			} else {
				rss[i] = int64(v1) / int64(v2)
			}
		}
	case types.T_uint32:
		p1 := vector.GenerateFunctionFixedTypeParameter[uint32](parameters[0])
		p2 := vector.GenerateFunctionFixedTypeParameter[uint32](parameters[1])
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetValue(i)
			v2, null2 := p2.GetValue(i)
			if null1 || null2 || v2 == 0 {
				if v2 == 0 && !null2 && shouldError {
					return moerr.NewDivByZeroNoCtx()
				}
				rsNull.Add(i)
			} else {
				rss[i] = int64(v1) / int64(v2)
			}
		}
	case types.T_uint64:
		p1 := vector.GenerateFunctionFixedTypeParameter[uint64](parameters[0])
		p2 := vector.GenerateFunctionFixedTypeParameter[uint64](parameters[1])
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetValue(i)
			v2, null2 := p2.GetValue(i)
			if null1 || null2 || v2 == 0 {
				if v2 == 0 && !null2 && shouldError {
					return moerr.NewDivByZeroNoCtx()
				}
				rsNull.Add(i)
			} else {
				quotient := v1 / v2
				// MySQL 8.0: if DIV result exceeds BIGINT range, error occurs
				if quotient > math.MaxInt64 {
					return moerr.NewOutOfRangeNoCtx("BIGINT", "")
				}
				rss[i] = int64(quotient)
			}
		}
	}
	return nil
}

func modFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	paramType := parameters[0].GetType()
	switch paramType.Oid {
	case types.T_bit:
		return specialTemplateForModFunction[uint64](parameters, result, proc, length, func(v1, v2 uint64) uint64 {
			return v1 % v2
		}, selectList)
	case types.T_uint8:
		return specialTemplateForModFunction[uint8](parameters, result, proc, length, func(v1, v2 uint8) uint8 {
			return v1 % v2
		}, selectList)
	case types.T_uint16:
		return specialTemplateForModFunction[uint16](parameters, result, proc, length, func(v1, v2 uint16) uint16 {
			return v1 % v2
		}, selectList)
	case types.T_uint32:
		return specialTemplateForModFunction[uint32](parameters, result, proc, length, func(v1, v2 uint32) uint32 {
			return v1 % v2
		}, selectList)
	case types.T_uint64:
		return specialTemplateForModFunction[uint64](parameters, result, proc, length, func(v1, v2 uint64) uint64 {
			return v1 % v2
		}, selectList)
	case types.T_int8:
		return specialTemplateForModFunction[int8](parameters, result, proc, length, func(v1, v2 int8) int8 {
			return v1 % v2
		}, selectList)
	case types.T_int16:
		return specialTemplateForModFunction[int16](parameters, result, proc, length, func(v1, v2 int16) int16 {
			return v1 % v2
		}, selectList)
	case types.T_int32:
		return specialTemplateForModFunction[int32](parameters, result, proc, length, func(v1, v2 int32) int32 {
			return v1 % v2
		}, selectList)
	case types.T_int64:
		return specialTemplateForModFunction[int64](parameters, result, proc, length, func(v1, v2 int64) int64 {
			return v1 % v2
		}, selectList)
	case types.T_float32:
		return specialTemplateForModFunction[float32](parameters, result, proc, length, func(v1, v2 float32) float32 {
			return float32(math.Mod(float64(v1), float64(v2)))
		}, selectList)
	case types.T_float64:
		return specialTemplateForModFunction[float64](parameters, result, proc, length, func(v1, v2 float64) float64 {
			return math.Mod(v1, v2)
		}, selectList)
	case types.T_decimal64:
		return decimalArith[types.Decimal64](parameters, result, proc, length, func(v1, v2 types.Decimal64, scale1, scale2 int32) (types.Decimal64, error) {
			r, _, err := v1.Mod(v2, scale1, scale2)
			return r, err
		}, selectList, true)
	case types.T_decimal128:
		return decimalArith[types.Decimal128](parameters, result, proc, length, func(v1, v2 types.Decimal128, scale1, scale2 int32) (types.Decimal128, error) {
			r, _, err := v1.Mod(v2, scale1, scale2)
			return r, err
		}, selectList, true)
	case types.T_decimal256:
		return decimalArith[types.Decimal256](parameters, result, proc, length, func(v1, v2 types.Decimal256, scale1, scale2 int32) (types.Decimal256, error) {
			r, _, err := v1.Mod(v2, scale1, scale2)
			return r, err
		}, selectList, true)
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

func decimal128ScaleArrayWithNulls(v, rs []types.Decimal128, len int, n int32, rsnull *nulls.Nulls) error {
	for i := 0; i < len; i++ {
		rs[i] = v[i]
		if rsnull.Contains(uint64(i)) {
			continue
		}
		err := rs[i].ScaleInplace(n)
		if err != nil {
			return err
		}
	}
	return nil
}

func decimal128AddArray(v1, v2, rs []types.Decimal128, scale1, scale2 int32, rsnull *nulls.Nulls) error {
	len1 := len(v1)
	len2 := len(v2)
	var err error
	if rsnull.IsEmpty() {
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
				err = decimal128ScaleArrayWithNulls(v2, rs, len2, scale1-scale2, rsnull)
				if err != nil {
					return err
				}
				for i := 0; i < len1; i++ {
					if rsnull.Contains(uint64(i)) {
						continue
					}
					err = rs[i].AddInplace(&v1[i])
					if err != nil {
						return err
					}
				}
			} else if scale1 < scale2 {
				err = decimal128ScaleArrayWithNulls(v1, rs, len1, scale2-scale1, rsnull)
				if err != nil {
					return err
				}
				for i := 0; i < len1; i++ {
					if rsnull.Contains(uint64(i)) {
						continue
					}
					err = rs[i].AddInplace(&v2[i])
					if err != nil {
						return err
					}
				}
			} else {
				for i := 0; i < len1; i++ {
					if rsnull.Contains(uint64(i)) {
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
				if scale1 > scale2 {
					err = decimal128ScaleArrayWithNulls(v2, rs, len2, scale1-scale2, rsnull)
					if err != nil {
						return err
					}
					for i := 0; i < len2; i++ {
						if rsnull.Contains(uint64(i)) {
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
						if rsnull.Contains(uint64(i)) {
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
						if rsnull.Contains(uint64(i)) {
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
				if scale1 > scale2 {
					err = decimal128ScaleArray(v2, rs, len2, scale1-scale2)
					if err != nil {
						return err
					}
					tmp := rs[0]
					for i := 0; i < len1; i++ {
						if rsnull.Contains(uint64(i)) {
							continue
						}
						rs[i] = tmp
						err = rs[i].AddInplace(&v1[i])
						if err != nil {
							return err
						}
					}
				} else if scale1 < scale2 {
					err = decimal128ScaleArrayWithNulls(v1, rs, len1, scale2-scale1, rsnull)
					if err != nil {
						return err
					}
					for i := 0; i < len1; i++ {
						if rsnull.Contains(uint64(i)) {
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
						if rsnull.Contains(uint64(i)) {
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

func decimal128SubArray(v1, v2, rs []types.Decimal128, scale1, scale2 int32, rsnull *nulls.Nulls) error {
	len1 := len(v1)
	len2 := len(v2)
	var err error

	if rsnull.IsEmpty() {
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
				err = decimal128ScaleArrayWithNulls(v2, rs, len2, scale1-scale2, rsnull)
				if err != nil {
					return err
				}
				for i := 0; i < len1; i++ {
					if rsnull.Contains(uint64(i)) {
						continue
					}
					rs[i].MinusInplace()
					err = rs[i].AddInplace(&v1[i])
					if err != nil {
						return err
					}
				}
			} else if scale1 < scale2 {
				err = decimal128ScaleArrayWithNulls(v1, rs, len1, scale2-scale1, rsnull)
				if err != nil {
					return err
				}
				for i := 0; i < len1; i++ {
					if rsnull.Contains(uint64(i)) {
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
					if rsnull.Contains(uint64(i)) {
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
				if scale1 > scale2 {
					err = decimal128ScaleArrayWithNulls(v2, rs, len2, scale1-scale2, rsnull)
					if err != nil {
						return err
					}
					for i := 0; i < len2; i++ {
						if rsnull.Contains(uint64(i)) {
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
						if rsnull.Contains(uint64(i)) {
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
						if rsnull.Contains(uint64(i)) {
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
				if scale1 > scale2 {
					err = decimal128ScaleArray(v2, rs, len2, scale1-scale2)
					if err != nil {
						return err
					}
					tmp := rs[0]
					tmp.MinusInplace()
					for i := 0; i < len1; i++ {
						if rsnull.Contains(uint64(i)) {
							continue
						}
						rs[i] = tmp
						err = rs[i].AddInplace(&v1[i])
						if err != nil {
							return err
						}
					}
				} else if scale1 < scale2 {
					err = decimal128ScaleArrayWithNulls(v1, rs, len1, scale2-scale1, rsnull)
					if err != nil {
						return err
					}
					for i := 0; i < len1; i++ {
						if rsnull.Contains(uint64(i)) {
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
						if rsnull.Contains(uint64(i)) {
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

func decimal128MultiArray(v1, v2, rs []types.Decimal128, scale1, scale2 int32, rsnull *nulls.Nulls) error {
	len1 := len(v1)
	len2 := len(v2)
	var err error

	if rsnull.IsEmpty() {
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
				if rsnull.Contains(uint64(i)) {
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
				for i := 0; i < len2; i++ {
					if rsnull.Contains(uint64(i)) {
						continue
					}
					rs[i] = v1[0]
					err = rs[i].MulInplace(&v2[i], scale, scale1, scale2)
					if err != nil {
						return err
					}
				}
			} else {
				for i := 0; i < len1; i++ {
					if rsnull.Contains(uint64(i)) {
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

// decimalIntegerDiv performs integer division for decimal types (DIV operator)
func decimalIsZero[T templateDec](v T) bool {
	switch d := any(v).(type) {
	case types.Decimal64:
		return d == 0
	case types.Decimal128:
		return d.B0_63 == 0 && d.B64_127 == 0
	case types.Decimal256:
		return d.B0_63 == 0 && d.B64_127 == 0 && d.B128_191 == 0 && d.B192_255 == 0
	default:
		panic("unsupported decimal type")
	}
}

func decimal128ToInt64(v types.Decimal128) (int64, error) {
	if v.Sign() {
		if v.B64_127 != ^uint64(0) {
			return 0, moerr.NewOutOfRangeNoCtx("BIGINT", "")
		}
		if v.B0_63 < 0x8000000000000000 {
			return 0, moerr.NewOutOfRangeNoCtx("BIGINT", "")
		}
		negated := v.Minus()
		return -int64(negated.B0_63), nil
	}

	if v.B64_127 != 0 {
		return 0, moerr.NewOutOfRangeNoCtx("BIGINT", "")
	}
	if v.B0_63 > 0x7FFFFFFFFFFFFFFF {
		return 0, moerr.NewOutOfRangeNoCtx("BIGINT", "")
	}
	return int64(v.B0_63), nil
}

func decimal256ToInt64(v types.Decimal256) (int64, error) {
	if v.Sign() {
		if v.B64_127 != ^uint64(0) || v.B128_191 != ^uint64(0) || v.B192_255 != ^uint64(0) {
			return 0, moerr.NewOutOfRangeNoCtx("BIGINT", "")
		}
		if v.B0_63 < 0x8000000000000000 {
			return 0, moerr.NewOutOfRangeNoCtx("BIGINT", "")
		}
		negated := v.Minus()
		return -int64(negated.B0_63), nil
	}

	if v.B64_127 != 0 || v.B128_191 != 0 || v.B192_255 != 0 {
		return 0, moerr.NewOutOfRangeNoCtx("BIGINT", "")
	}
	if v.B0_63 > 0x7FFFFFFFFFFFFFFF {
		return 0, moerr.NewOutOfRangeNoCtx("BIGINT", "")
	}
	return int64(v.B0_63), nil
}

func decimalIntegerDiv[T templateDec](parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[T](parameters[1])
	rs := vector.MustFunctionResult[int64](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedColNoTypeCheck[int64](rsVec)
	rsNull := rsVec.GetNulls()

	scale1 := p1.GetType().Scale
	scale2 := p2.GetType().Scale
	shouldError := checkDivisionByZeroBehavior(proc, selectList)

	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			rsNull.Add(i)
			continue
		}

		if decimalIsZero(v2) {
			if shouldError {
				return moerr.NewDivByZeroNoCtx()
			}
			rsNull.Add(i)
			continue
		}

		var err error
		switch any(v1).(type) {
		case types.Decimal128:
			d1 := any(v1).(types.Decimal128)
			d2 := any(v2).(types.Decimal128)
			// For integer DIV: align scales then use truncating integer division.
			if scale1 != scale2 {
				if scale1 < scale2 {
					d1, err = d1.Scale(scale2 - scale1)
				} else {
					d2, err = d2.Scale(scale1 - scale2)
				}
				if err != nil {
					return err
				}
			}
			n1, convErr := decimal128ToInt64(d1)
			if convErr != nil {
				return convErr
			}
			n2, convErr := decimal128ToInt64(d2)
			if convErr != nil {
				return convErr
			}
			rss[i] = n1 / n2
		case types.Decimal64:
			d1Val := any(v1).(types.Decimal64)
			d2Val := any(v2).(types.Decimal64)
			n1 := int64(d1Val)
			n2 := int64(d2Val)
			if scale1 != scale2 {
				if scale1 < scale2 {
					for s := scale1; s < scale2; s++ {
						n1 *= 10
					}
				} else {
					for s := scale2; s < scale1; s++ {
						n2 *= 10
					}
				}
			}
			if n2 == 0 {
				rsNull.Add(i)
				continue
			}
			rss[i] = n1 / n2
		case types.Decimal256:
			d1 := any(v1).(types.Decimal256)
			d2 := any(v2).(types.Decimal256)
			divResult, resultScale, divErr := d1.Div(d2, scale1, scale2)
			if divErr != nil {
				return divErr
			}
			if resultScale > 0 {
				divResult, divErr = divResult.Scale(-resultScale)
				if divErr != nil {
					return divErr
				}
			}
			rss[i], err = decimal256ToInt64(divResult)
		default:
			panic("unsupported decimal type")
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func addFloat64WithOverflowCheck(ctx context.Context, v1, v2 float64) (float64, error) {
	result := v1 + v2
	if math.IsInf(result, 0) {
		return 0, moerr.NewOutOfRangef(ctx, "float64", "DOUBLE value is out of range in '(%v + %v)'", v1, v2)
	}
	return result, nil
}

func addFloat32WithOverflowCheck(ctx context.Context, v1, v2 float32) (float32, error) {
	result := v1 + v2
	if math.IsInf(float64(result), 0) {
		return 0, moerr.NewOutOfRangef(ctx, "float32", "FLOAT value is out of range in '(%v + %v)'", v1, v2)
	}
	return result, nil
}
