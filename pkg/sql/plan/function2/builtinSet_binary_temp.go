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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vectorize/floor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
	"math"
	"strconv"
)

func AddFaultPoint(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	for i := 0; i < 5; i++ {
		if ivecs[i].IsConstNull() || !ivecs[i].IsConst() {
			return moerr.NewInvalidArg(proc.Ctx, "AddFaultPoint", "not scalar")
		}
	}

	name, _ := vector.GenerateFunctionStrParameter(ivecs[0]).GetStrValue(0)
	freq, _ := vector.GenerateFunctionStrParameter(ivecs[1]).GetStrValue(0)
	action, _ := vector.GenerateFunctionStrParameter(ivecs[2]).GetStrValue(0)
	iarg, _ := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[3]).GetValue(0)
	sarg, _ := vector.GenerateFunctionStrParameter(ivecs[4]).GetStrValue(0)

	rs := vector.MustFunctionResult[bool](result)

	if err = fault.AddFaultPoint(proc.Ctx, string(name), string(freq), string(action), iarg, string(sarg)); err != nil {
		return err
	}
	if err = rs.Append(true, false); err != nil {
		return
	}
	return nil
}

type mathMultiT interface {
	constraints.Integer | constraints.Float | types.Decimal64 | types.Decimal128
}

type mathMultiFun[T mathMultiT] func(T, int64) T

func generalMathMulti[T mathMultiT](funcName string, ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, cb mathMultiFun[T]) (err error) {
	digits := int64(0)
	if len(ivecs) > 1 {
		if !ivecs[1].IsConst() || ivecs[1].GetType().Oid != types.T_int64 {
			return moerr.NewInvalidArg(proc.Ctx, fmt.Sprintf("the second argument of the %s", funcName), "not const")
		}
		digits = vector.MustFixedCol[int64](ivecs[1])[0]
	}

	rs := vector.MustFunctionResult[T](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[T](ivecs[0])
	var t T
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err = rs.Append(t, true); err != nil {
				return err
			}
		} else {
			if err = rs.Append(cb(v, digits), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func CeilStr(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	digits := int64(0)
	if len(ivecs) > 1 {
		if !ivecs[1].IsConst() || ivecs[1].GetType().Oid != types.T_int64 {
			return moerr.NewInvalidArg(proc.Ctx, fmt.Sprintf("the second argument of the %s", "ceil"), "not const")
		}
		digits = vector.MustFixedCol[int64](ivecs[1])[0]
	}

	rs := vector.MustFunctionResult[float64](result)
	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err = rs.Append(0, true); err != nil {
				return err
			}
		} else {
			floatVal, err := strconv.ParseFloat(string(v), 64)
			if err != nil {
				return err
			}
			if err = rs.Append(ceilFloat64(floatVal, digits), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func ceilInt64(x, digits int64) int64 {
	switch {
	case digits >= 0:
		return x
	case digits > -floor.MaxInt64digits:
		scale := int64(floor.ScaleTable[-digits])
		t := x % scale
		s := x
		if t != 0 {
			s -= t
			if s >= 0 && x > 0 {
				x = (s + scale) / scale * scale
			} else {
				x = s
			}
		}
	case digits <= -floor.MaxInt64digits:
		x = 0
	}
	return x
}

func ceilUint64(x uint64, digits int64) uint64 {
	switch {
	case digits >= 0:
		return x
	case digits > -floor.MaxUint64digits:
		scale := floor.ScaleTable[-digits]
		t := x % scale
		s := x
		if t != 0 {
			s -= t
			x = (s + scale) / scale * scale
		}
	case digits <= -floor.MaxUint64digits:
		x = 0
	}
	return x
}

func ceilFloat64(x float64, digits int64) float64 {
	if digits == 0 {
		return math.Ceil(x)
	}
	scale := math.Pow10(int(digits))
	value := x * scale
	x = math.Ceil(value) / scale
	return x
}

func CeilUint64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	return generalMathMulti("ceil", ivecs, result, proc, length, ceilUint64)
}

func CeilInt64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	return generalMathMulti("ceil", ivecs, result, proc, length, ceilInt64)
}

func CeilFloat64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	return generalMathMulti("ceil", ivecs, result, proc, length, ceilFloat64)
}

func ceilDecimal64(x types.Decimal64, digits int64, scale int32) types.Decimal64 {
	if digits > 19 {
		digits = 19
	}
	if digits < -18 {
		digits = -18
	}
	return x.Ceil(scale, int32(digits))
}

func ceilDecimal128(x types.Decimal128, digits int64, scale int32) types.Decimal128 {
	if digits > 39 {
		digits = 19
	}
	if digits < -38 {
		digits = -38
	}
	return x.Ceil(scale, int32(digits))
}

func CeilDecimal64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	scale := ivecs[0].GetType().Scale
	if len(ivecs) > 1 {
		digit := vector.MustFixedCol[int64](ivecs[1])
		if len(digit) > 0 && int32(digit[0]) <= scale-18 {
			return moerr.NewOutOfRange(proc.Ctx, "decimal64", "ceil(decimal64(18,%v),%v)", scale, digit[0])
		}
	}
	cb := func(x types.Decimal64, digits int64) types.Decimal64 {
		return ceilDecimal64(x, digits, scale)
	}
	return generalMathMulti("ceil", ivecs, result, proc, length, cb)
}

func CeilDecimal128(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	scale := ivecs[0].GetType().Scale
	if len(ivecs) > 1 {
		digit := vector.MustFixedCol[int64](ivecs[1])
		if len(digit) > 0 && int32(digit[0]) <= scale-38 {
			return moerr.NewOutOfRange(proc.Ctx, "decimal128", "ceil(decimal128(38,%v),%v)", scale, digit[0])
		}
	}
	cb := func(x types.Decimal128, digits int64) types.Decimal128 {
		return ceilDecimal128(x, digits, scale)
	}
	return generalMathMulti("ceil", ivecs, result, proc, length, cb)
}
