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

package function2

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function2/function2Util"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
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
		return optimizedTypeArith1[uint8, uint8](parameters, result, proc, length, func(v1, v2 uint8) uint8 {
			return v1 + v2
		})
	case types.T_uint16:
		return optimizedTypeArith1[uint16, uint16](parameters, result, proc, length, func(v1, v2 uint16) uint16 {
			return v1 + v2
		})
	case types.T_uint32:
		return optimizedTypeArith1[uint32, uint32](parameters, result, proc, length, func(v1, v2 uint32) uint32 {
			return v1 + v2
		})
	case types.T_uint64:
		return optimizedTypeArith1[uint64, uint64](parameters, result, proc, length, func(v1, v2 uint64) uint64 {
			return v1 + v2
		})
	case types.T_int8:
		return optimizedTypeArith1[int8, int8](parameters, result, proc, length, func(v1, v2 int8) int8 {
			return v1 + v2
		})
	case types.T_int16:
		return optimizedTypeArith1[int16, int16](parameters, result, proc, length, func(v1, v2 int16) int16 {
			return v1 + v2
		})
	case types.T_int32:
		return optimizedTypeArith1[int32, int32](parameters, result, proc, length, func(v1, v2 int32) int32 {
			return v1 + v2
		})
	case types.T_int64:
		return optimizedTypeArith1[int64, int64](parameters, result, proc, length, func(v1, v2 int64) int64 {
			return v1 + v2
		})
	case types.T_float32:
		return optimizedTypeArith1[float32, float32](parameters, result, proc, length, func(v1, v2 float32) float32 {
			return v1 + v2
		})
	case types.T_float64:
		return optimizedTypeArith1[float64, float64](parameters, result, proc, length, func(v1, v2 float64) float64 {
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
	}
	panic("unreached code")
}

func minusFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	paramType := parameters[0].GetType()
	switch paramType.Oid {
	case types.T_uint8:
		return optimizedTypeArith1[uint8, uint8](parameters, result, proc, length, func(v1, v2 uint8) uint8 {
			return v1 - v2
		})
	case types.T_uint16:
		return optimizedTypeArith1[uint16, uint16](parameters, result, proc, length, func(v1, v2 uint16) uint16 {
			return v1 - v2
		})
	case types.T_uint32:
		return optimizedTypeArith1[uint32, uint32](parameters, result, proc, length, func(v1, v2 uint32) uint32 {
			return v1 - v2
		})
	case types.T_uint64:
		return optimizedTypeArith1[uint64, uint64](parameters, result, proc, length, func(v1, v2 uint64) uint64 {
			return v1 - v2
		})
	case types.T_int8:
		return optimizedTypeArith1[int8, int8](parameters, result, proc, length, func(v1, v2 int8) int8 {
			return v1 - v2
		})
	case types.T_int16:
		return optimizedTypeArith1[int16, int16](parameters, result, proc, length, func(v1, v2 int16) int16 {
			return v1 - v2
		})
	case types.T_int32:
		return optimizedTypeArith1[int32, int32](parameters, result, proc, length, func(v1, v2 int32) int32 {
			return v1 - v2
		})
	case types.T_int64:
		return optimizedTypeArith1[int64, int64](parameters, result, proc, length, func(v1, v2 int64) int64 {
			return v1 - v2
		})
	case types.T_float32:
		return optimizedTypeArith1[float32, float32](parameters, result, proc, length, func(v1, v2 float32) float32 {
			return v1 - v2
		})
	case types.T_float64:
		return optimizedTypeArith1[float64, float64](parameters, result, proc, length, func(v1, v2 float64) float64 {
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
		return optimizedTypeArith1[types.Date, int64](parameters, result, proc, length, func(v1, v2 types.Date) int64 {
			return int64(v1 - v2)
		})
	case types.T_datetime:
		return optimizedTypeArith1[types.Datetime, int64](parameters, result, proc, length, func(v1, v2 types.Datetime) int64 {
			return v1.DatetimeMinusWithSecond(v2)
		})
	}
	panic("unreached code")
}

func multiFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	paramType := parameters[0].GetType()
	switch paramType.Oid {
	case types.T_uint8:
		return optimizedTypeArith1[uint8, uint8](parameters, result, proc, length, func(v1, v2 uint8) uint8 {
			return v1 * v2
		})
	case types.T_uint16:
		return optimizedTypeArith1[uint16, uint16](parameters, result, proc, length, func(v1, v2 uint16) uint16 {
			return v1 * v2
		})
	case types.T_uint32:
		return optimizedTypeArith1[uint32, uint32](parameters, result, proc, length, func(v1, v2 uint32) uint32 {
			return v1 * v2
		})
	case types.T_uint64:
		return optimizedTypeArith1[uint64, uint64](parameters, result, proc, length, func(v1, v2 uint64) uint64 {
			return v1 * v2
		})
	case types.T_int8:
		return optimizedTypeArith1[int8, int8](parameters, result, proc, length, func(v1, v2 int8) int8 {
			return v1 * v2
		})
	case types.T_int16:
		return optimizedTypeArith1[int16, int16](parameters, result, proc, length, func(v1, v2 int16) int16 {
			return v1 * v2
		})
	case types.T_int32:
		return optimizedTypeArith1[int32, int32](parameters, result, proc, length, func(v1, v2 int32) int32 {
			return v1 * v2
		})
	case types.T_int64:
		return optimizedTypeArith1[int64, int64](parameters, result, proc, length, func(v1, v2 int64) int64 {
			return v1 * v2
		})
	case types.T_float32:
		return optimizedTypeArith1[float32, float32](parameters, result, proc, length, func(v1, v2 float32) float32 {
			return v1 * v2
		})
	case types.T_float64:
		return optimizedTypeArith1[float64, float64](parameters, result, proc, length, func(v1, v2 float64) float64 {
			return v1 * v2
		})
	case types.T_decimal64:
		return decimalArith2(parameters, result, proc, length, func(v1, v2 types.Decimal64, scale1, scale2 int32) (types.Decimal128, error) {
			x, y := function2Util.ConvertD64ToD128(v1), function2Util.ConvertD64ToD128(v2)
			rt, _, err := x.Mul(y, scale1, scale2)
			return rt, err
		})
	case types.T_decimal128:
		return decimalArith[types.Decimal128](parameters, result, proc, length, func(v1, v2 types.Decimal128, scale1, scale2 int32) (types.Decimal128, error) {
			r, _, err := v1.Mul(v2, scale1, scale2)
			return r, err
		})
	}
	panic("unreached code")
}

func divFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	paramType := parameters[0].GetType()
	switch paramType.Oid {
	case types.T_float32:
		return floatDiv[float32](parameters, result, uint64(length))
	case types.T_float64:
		return floatDiv[float64](parameters, result, uint64(length))
	case types.T_decimal64:
		return decimalArith2(parameters, result, proc, length, func(v1, v2 types.Decimal64, scale1, scale2 int32) (types.Decimal128, error) {
			x, y := function2Util.ConvertD64ToD128(v1), function2Util.ConvertD64ToD128(v2)
			rt, _, err := x.Div(y, scale1, scale2)
			return rt, err
		})
	case types.T_decimal128:
		return decimalArith[types.Decimal128](parameters, result, proc, length, func(v1, v2 types.Decimal128, scale1, scale2 int32) (types.Decimal128, error) {
			r, _, err := v1.Div(v2, scale1, scale2)
			return r, err
		})
	}
	panic("unreached code")
}

func floatDiv[T float32 | float64](parameters []*vector.Vector, result vector.FunctionResultWrapper, length uint64) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[T](parameters[1])
	rs := vector.MustFunctionResult[T](result)

	for i := uint64(0); i < length; i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if v2 == 0 {
				return moerr.NewDivByZeroNoCtx()
			}
			if err := rs.Append(v1/v2, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func integerDivFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	paramType := parameters[0].GetType()
	if paramType.Oid == types.T_float32 {
		return floatIntegerDiv[float32](parameters, result, uint64(length))
	}
	if paramType.Oid == types.T_float64 {
		return floatIntegerDiv[float64](parameters, result, uint64(length))
	}
	panic("unreached code")
}

func floatIntegerDiv[T float32 | float64](parameters []*vector.Vector, result vector.FunctionResultWrapper, length uint64) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[T](parameters[1])
	rs := vector.MustFunctionResult[int64](result)

	for i := uint64(0); i < length; i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if v2 == 0 {
				return moerr.NewDivByZeroNoCtx()
			}
			if err := rs.Append(int64(v1/v2), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func modFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	paramType := parameters[0].GetType()
	switch paramType.Oid {
	case types.T_uint8:
		return intMod[uint8](parameters, result, uint64(length))
	case types.T_uint16:
		return intMod[uint16](parameters, result, uint64(length))
	case types.T_uint32:
		return intMod[uint32](parameters, result, uint64(length))
	case types.T_uint64:
		return intMod[uint64](parameters, result, uint64(length))
	case types.T_int8:
		return intMod[int8](parameters, result, uint64(length))
	case types.T_int16:
		return intMod[int16](parameters, result, uint64(length))
	case types.T_int32:
		return intMod[int32](parameters, result, uint64(length))
	case types.T_int64:
		return intMod[int64](parameters, result, uint64(length))
	case types.T_float32:
		return floatMod[float32](parameters, result, uint64(length))
	case types.T_float64:
		return floatMod[float64](parameters, result, uint64(length))
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

func intMod[T constraints.Integer](parameters []*vector.Vector, result vector.FunctionResultWrapper, length uint64) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[T](parameters[1])
	rs := vector.MustFunctionResult[T](result)
	for i := uint64(0); i < length; i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if v2 == 0 {
				if err := rs.Append(v1, false); err != nil {
					return err
				}
			} else {
				if err := rs.Append(v1%v2, false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func floatMod[T constraints.Float](parameters []*vector.Vector, result vector.FunctionResultWrapper, length uint64) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[T](parameters[1])
	rs := vector.MustFunctionResult[T](result)
	for i := uint64(0); i < length; i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if v2 == 0 {
				if err := rs.Append(v1, false); err != nil {
					return err
				}
			} else {
				if err := rs.Append(T(math.Mod(float64(v1), float64(v2))), false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
