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
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vectorize/floor"
	"github.com/matrixorigin/matrixone/pkg/vectorize/format"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
	"math"
	"strconv"
	"time"
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

var MaxUint8digits = numOfDigits(math.MaxUint8)
var MaxUint16digits = numOfDigits(math.MaxUint16)
var MaxUint32digits = numOfDigits(math.MaxUint32)
var MaxUint64digits = numOfDigits(math.MaxUint64) // 20
var MaxInt8digits = numOfDigits(math.MaxInt8)
var MaxInt16digits = numOfDigits(math.MaxInt16)
var MaxInt32digits = numOfDigits(math.MaxInt32)
var MaxInt64digits = numOfDigits(math.MaxInt64) // 19

func numOfDigits(value uint64) int64 {
	digits := int64(0)
	for value > 0 {
		value /= 10
		digits++
	}
	return digits
}

// ScaleTable is a lookup array for digits
var ScaleTable = [...]uint64{
	1,
	10,
	100,
	1000,
	10000,
	100000,
	1000000,
	10000000,
	100000000,
	1000000000,
	10000000000,
	100000000000,
	1000000000000,
	10000000000000,
	100000000000000,
	1000000000000000,
	10000000000000000,
	100000000000000000,
	1000000000000000000,
	10000000000000000000, // 1 followed by 19 zeros, maxUint64 number has 20 digits, so the max scale is 1 followed by 19 zeroes
}

func floorUint64(x uint64, digits int64) uint64 {
	switch {
	case digits >= 0:
		return x
	case digits > -MaxUint64digits:
		scale := ScaleTable[-digits]
		x = x / scale * scale
	case digits <= -MaxUint64digits:
		x = 0
	}
	return x
}

func FloorUInt64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	return generalMathMulti("floor", ivecs, result, proc, length, floorUint64)
}

func floorInt64(x int64, digits int64) int64 {
	switch {
	case digits >= 0:
		return x
	case digits > -MaxInt64digits:
		scale := int64(ScaleTable[-digits])
		value := x
		if value < 0 {
			value -= scale - 1
		}
		x = value / scale * scale
	case digits <= -MaxInt64digits:
		x = 0
	}
	return x
}

func FloorInt64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	return generalMathMulti("floor", ivecs, result, proc, length, floorInt64)
}

func floorFloat64(x float64, digits int64) float64 {
	if digits == 0 {
		return math.Floor(x)
	}
	scale := math.Pow10(int(digits))
	value := x * scale
	return math.Floor(value) / scale
}

func FloorFloat64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	return generalMathMulti("floor", ivecs, result, proc, length, floorFloat64)
}

func floorDecimal64(x types.Decimal64, digits int64, scale int32) types.Decimal64 {
	if digits > 19 {
		digits = 19
	}
	if digits < -18 {
		digits = -18
	}
	return x.Floor(scale, int32(digits))
}

func floorDecimal128(x types.Decimal128, digits int64, scale int32) types.Decimal128 {
	if digits > 39 {
		digits = 39
	}
	if digits < -38 {
		digits = -38
	}
	return x.Floor(scale, int32(digits))
}

func FloorDecimal64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	scale := ivecs[0].GetType().Scale
	if len(ivecs) > 1 {
		digit := vector.MustFixedCol[int64](ivecs[1])
		if len(digit) > 0 && int32(digit[0]) <= scale-18 {
			return moerr.NewOutOfRange(proc.Ctx, "decimal64", "floor(decimal64(18,%v),%v)", scale, digit[0])
		}
	}
	cb := func(x types.Decimal64, digits int64) types.Decimal64 {
		return floorDecimal64(x, digits, scale)
	}

	return generalMathMulti("floor", ivecs, result, proc, length, cb)
}

func FloorDecimal128(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	scale := ivecs[0].GetType().Scale
	if len(ivecs) > 1 {
		digit := vector.MustFixedCol[int64](ivecs[1])
		if len(digit) > 0 && int32(digit[0]) <= scale-38 {
			return moerr.NewOutOfRange(proc.Ctx, "decimal128", "floor(decimal128(38,%v),%v)", scale, digit[0])
		}
	}
	cb := func(x types.Decimal128, digits int64) types.Decimal128 {
		return floorDecimal128(x, digits, scale)
	}

	return generalMathMulti("floor", ivecs, result, proc, length, cb)
}

func FloorStr(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
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
			if err = rs.Append(floorFloat64(floatVal, digits), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func roundUint64(x uint64, digits int64) uint64 {
	switch {
	case digits >= 0:
		return x
	case digits > -MaxUint64digits: // round algorithm contributed by @ffftian
		scale := ScaleTable[-digits]
		step1 := x / scale * scale
		step2 := x % scale
		if step2 >= scale/2 {
			x = step1 + scale
			if x < step1 {
				panic(moerr.NewOutOfRangeNoCtx("uint64", "ROUND"))
			}
		} else {
			x = step1
		}
	case digits <= -MaxUint64digits:
		x = 0
	}
	return x
}

func RoundUint64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	return generalMathMulti("round", ivecs, result, proc, length, roundUint64)
}

func roundInt64(x int64, digits int64) int64 {
	switch {
	case digits >= 0:
		return x
	case digits > -MaxInt64digits:
		scale := int64(ScaleTable[-digits]) // round algorithm contributed by @ffftian
		if x > 0 {
			step1 := x / scale * scale
			step2 := x % scale
			if step2 >= scale/2 {
				x = step1 + scale
				if x < step1 {
					panic(moerr.NewOutOfRangeNoCtx("int64", "ROUND"))
				}
			} else {
				x = step1
			}
		} else if x < 0 {
			step1 := x / scale * scale
			step2 := x % scale // module operation with negative numbers, the result is negative
			if step2 <= scale/2 {
				x = step1 - scale
				if x > step1 {
					panic(moerr.NewOutOfRangeNoCtx("int64", "ROUND"))
				}
			} else {
				x = step1
			}
		} else {
			x = 0
		}
	case digits <= MaxInt64digits:
		x = 0
	}
	return x
}

func RoundInt64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	return generalMathMulti("round", ivecs, result, proc, length, roundInt64)
}

func roundFloat64(x float64, digits int64) float64 {
	if digits == 0 {
		x = math.RoundToEven(x)
	} else if digits >= 308 { // the range of float64
	} else if digits <= -308 {
		x = 0
	} else {
		var abs_digits uint64
		if digits < 0 {
			abs_digits = uint64(-digits)
		} else {
			abs_digits = uint64(digits)
		}
		var tmp = math.Pow(10.0, float64(abs_digits))

		if digits > 0 {
			var value_mul_tmp = x * tmp
			x = math.RoundToEven(value_mul_tmp) / tmp
		} else {
			var value_div_tmp = x / tmp
			x = math.RoundToEven(value_div_tmp) * tmp
		}
	}
	return x
}

func RoundFloat64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	return generalMathMulti("round", ivecs, result, proc, length, roundFloat64)
}

func roundDecimal64(x types.Decimal64, digits int64, scale int32) types.Decimal64 {
	if digits > 19 {
		digits = 19
	}
	if digits < -18 {
		digits = -18
	}
	return x.Round(scale, int32(digits))
}

func roundDecimal128(x types.Decimal128, digits int64, scale int32) types.Decimal128 {
	if digits > 39 {
		digits = 39
	}
	if digits < -38 {
		digits = -38
	}
	return x.Round(scale, int32(digits))
}

func RoundDecimal64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	scale := ivecs[0].GetType().Scale
	cb := func(x types.Decimal64, digits int64) types.Decimal64 {
		return roundDecimal64(x, digits, scale)
	}
	return generalMathMulti("round", ivecs, result, proc, length, cb)
}

func RoundDecimal128(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	scale := ivecs[0].GetType().Scale
	cb := func(x types.Decimal128, digits int64) types.Decimal128 {
		return roundDecimal128(x, digits, scale)
	}
	return generalMathMulti("round", ivecs, result, proc, length, cb)
}

type NormalType interface {
	constraints.Integer | constraints.Float | bool | types.Date | types.Datetime |
		types.Decimal64 | types.Decimal128 | types.Timestamp | types.Uuid
}

func CoalesceGeneral[T NormalType](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[T](result)
	vecs := make([]vector.FunctionParameterWrapper[T], len(ivecs))
	for i := range ivecs {
		vecs[i] = vector.GenerateFunctionFixedTypeParameter[T](ivecs[i])
	}
	var t T
	for i := uint64(0); i < uint64(length); i++ {
		isFill := false
		for j := range vecs {
			v, null := vecs[j].GetValue(i)
			if null {
				continue
			}
			if err = rs.Append(v, false); err != nil {
				return err
			}
			isFill = true
			break
		}
		if !isFill {
			if err = rs.Append(t, true); err != nil {
				return err
			}
		}
	}
	return nil
}

func CoalesceStr(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[types.Varlena](result)
	vecs := make([]vector.FunctionParameterWrapper[types.Varlena], len(ivecs))
	for i := range ivecs {
		vecs[i] = vector.GenerateFunctionStrParameter(ivecs[i])
	}
	for i := uint64(0); i < uint64(length); i++ {
		isFill := false
		for j := range vecs {
			v, null := vecs[j].GetStrValue(i)
			if null {
				continue
			}
			if err = rs.AppendBytes(v, false); err != nil {
				return err
			}
			isFill = true
			break
		}
		if !isFill {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		}
	}
	return nil
}

func ConcatWs(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[types.Varlena](result)
	vecs := make([]vector.FunctionParameterWrapper[types.Varlena], len(ivecs))
	for i := range ivecs {
		vecs[i] = vector.GenerateFunctionStrParameter(ivecs[i])
	}
	for i := uint64(0); i < uint64(length); i++ {
		sp, null := vecs[0].GetStrValue(i)
		if null {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		allNull := true
		var str string
		for j := 1; j < len(vecs); j++ {
			v, null := vecs[j].GetStrValue(i)
			if null {
				continue
			}
			if len(str) > 0 {
				str += string(sp)
			}
			str += string(v)
			allNull = false
		}
		if allNull {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		if err = rs.AppendBytes([]byte(str), false); err != nil {
			return err
		}
	}
	return nil
}

func doDateAdd(start types.Date, diff int64, unit int64) (types.Date, error) {
	err := types.JudgeIntervalNumOverflow(diff, types.IntervalType(unit))
	if err != nil {
		return 0, err
	}
	dt, success := start.ToDatetime().AddInterval(diff, types.IntervalType(unit), types.DateType)
	if success {
		return dt.ToDate(), nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("date", "")
	}
}

func doTimeAdd(start types.Time, diff int64, unit int64) (types.Time, error) {
	err := types.JudgeIntervalNumOverflow(diff, types.IntervalType(unit))
	if err != nil {
		return 0, err
	}
	t, success := start.AddInterval(diff, types.IntervalType(unit))
	if success {
		return t, nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("time", "")
	}
}

func doDatetimeAdd(start types.Datetime, diff int64, unit int64) (types.Datetime, error) {
	err := types.JudgeIntervalNumOverflow(diff, types.IntervalType(unit))
	if err != nil {
		return 0, err
	}
	dt, success := start.AddInterval(diff, types.IntervalType(unit), types.DateTimeType)
	if success {
		return dt, nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("datetime", "")
	}
}

func doDateStringAdd(startStr string, diff int64, unit int64) (types.Datetime, error) {
	err := types.JudgeIntervalNumOverflow(diff, types.IntervalType(unit))
	if err != nil {
		return 0, err
	}
	start, err := types.ParseDatetime(startStr, 6)
	if err != nil {
		return 0, err
	}
	dt, success := start.AddInterval(diff, types.IntervalType(unit), types.DateType)
	if success {
		return dt, nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("datetime", "")
	}
}

func doTimestampAdd(loc *time.Location, start types.Timestamp, diff int64, unit int64) (types.Timestamp, error) {
	err := types.JudgeIntervalNumOverflow(diff, types.IntervalType(unit))
	if err != nil {
		return 0, err
	}
	dt, success := start.ToDatetime(loc).AddInterval(diff, types.IntervalType(unit), types.DateTimeType)
	if success {
		return dt.ToTimestamp(loc), nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("timestamp", "")
	}
}

func DateAdd(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[types.Date](result)
	unit, _ := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[2]).GetValue(0)

	starts := vector.GenerateFunctionFixedTypeParameter[types.Date](ivecs[0])
	diffs := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := starts.GetValue(i)
		v2, null2 := diffs.GetValue(i)

		if null1 || null2 {
			if err = rs.Append(v1, true); err != nil {
				return err
			}
		} else {
			val, err := doDateAdd(v1, v2, unit)
			if err != nil {
				return err
			}
			if err = rs.Append(val, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func DatetimeAdd(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[types.Datetime](result)
	unit, _ := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[2]).GetValue(0)

	starts := vector.GenerateFunctionFixedTypeParameter[types.Datetime](ivecs[0])
	diffs := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := starts.GetValue(i)
		v2, null2 := diffs.GetValue(i)

		if null1 || null2 {
			if err = rs.Append(v1, true); err != nil {
				return err
			}
		} else {
			val, err := doDatetimeAdd(v1, v2, unit)
			if err != nil {
				return err
			}
			if err = rs.Append(val, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func DateStringAdd(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[types.Datetime](result)
	unit, _ := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[2]).GetValue(0)

	var d types.Datetime
	starts := vector.GenerateFunctionStrParameter(ivecs[0])
	diffs := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := starts.GetStrValue(i)
		v2, null2 := diffs.GetValue(i)

		if null1 || null2 {
			if err = rs.Append(d, true); err != nil {
				return err
			}
		} else {
			val, err := doDateStringAdd(string(v1), v2, unit)
			if err != nil {
				return err
			}
			if err = rs.Append(val, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func TimestampAdd(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[types.Timestamp](result)
	unit, _ := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[2]).GetValue(0)

	starts := vector.GenerateFunctionFixedTypeParameter[types.Timestamp](ivecs[0])
	diffs := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := starts.GetValue(i)
		v2, null2 := diffs.GetValue(i)

		if null1 || null2 {
			if err = rs.Append(v1, true); err != nil {
				return err
			}
		} else {
			val, err := doTimestampAdd(proc.SessionInfo.TimeZone, v1, v2, unit)
			if err != nil {
				return err
			}
			if err = rs.Append(val, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func TimeAdd(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[types.Time](result)
	unit, _ := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[2]).GetValue(0)

	starts := vector.GenerateFunctionFixedTypeParameter[types.Time](ivecs[0])
	diffs := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := starts.GetValue(i)
		v2, null2 := diffs.GetValue(i)

		if null1 || null2 {
			if err = rs.Append(v1, true); err != nil {
				return err
			}
		} else {
			val, err := doTimeAdd(v1, v2, unit)
			if err != nil {
				return err
			}
			if err = rs.Append(val, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func Serial(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) (err error) {
	return nil
}

func DateFormat(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	if !ivecs[1].IsConst() {
		return moerr.NewInvalidArg(proc.Ctx, "date format format", "not constant")
	}

	rs := vector.MustFunctionResult[types.Varlena](result)

	dates := vector.GenerateFunctionFixedTypeParameter[types.Datetime](ivecs[0])
	formats := vector.GenerateFunctionStrParameter(ivecs[1])

	for i := uint64(0); i < uint64(length); i++ {
		d, null1 := dates.GetValue(i)
		f, null2 := formats.GetStrValue(i)

		if null1 || null2 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			str, err := datetimeFormat(proc.Ctx, d, string(f))
			if err != nil {
				return err
			}
			if err = rs.AppendBytes([]byte(str), false); err != nil {
				return err
			}
		}
	}
	return nil
}

// datetimeFormat: format the datetime value according to the format string.
func datetimeFormat(ctx context.Context, datetime types.Datetime, format string) (string, error) {
	var buf bytes.Buffer
	inPatternMatch := false
	for _, b := range format {
		if inPatternMatch {
			if err := makeDateFormat(ctx, datetime, b, &buf); err != nil {
				return "", err
			}
			inPatternMatch = false
			continue
		}

		// It's not in pattern match now.
		if b == '%' {
			inPatternMatch = true
		} else {
			buf.WriteRune(b)
		}
	}
	return buf.String(), nil
}

// makeDateFormat: Get the format string corresponding to the date according to a single format character
func makeDateFormat(ctx context.Context, t types.Datetime, b rune, buf *bytes.Buffer) error {
	switch b {
	case 'b':
		m := t.Month()
		if m == 0 || m > 12 {
			return moerr.NewInvalidInput(ctx, "invalud date format for month '%d'", m)
		}
		buf.WriteString(MonthNames[m-1][:3])
	case 'M':
		m := t.Month()
		if m == 0 || m > 12 {
			return moerr.NewInvalidInput(ctx, "invalud date format for month '%d'", m)
		}
		buf.WriteString(MonthNames[m-1])
	case 'm':
		buf.WriteString(FormatIntByWidth(int(t.Month()), 2))
	case 'c':
		buf.WriteString(strconv.FormatInt(int64(t.Month()), 10))
	case 'D':
		buf.WriteString(strconv.FormatInt(int64(t.Day()), 10))
		buf.WriteString(AbbrDayOfMonth(int(t.Day())))
	case 'd':
		buf.WriteString(FormatIntByWidth(int(t.Day()), 2))
	case 'e':
		buf.WriteString(strconv.FormatInt(int64(t.Day()), 10))
	case 'f':
		fmt.Fprintf(buf, "%06d", t.MicroSec())
	case 'j':
		fmt.Fprintf(buf, "%03d", t.DayOfYear())
	case 'H':
		buf.WriteString(FormatIntByWidth(int(t.Hour()), 2))
	case 'k':
		buf.WriteString(strconv.FormatInt(int64(t.Hour()), 10))
	case 'h', 'I':
		tt := t.Hour()
		if tt%12 == 0 {
			buf.WriteString("12")
		} else {
			buf.WriteString(FormatIntByWidth(int(tt%12), 2))
		}
	case 'i':
		buf.WriteString(FormatIntByWidth(int(t.Minute()), 2))
	case 'l':
		tt := t.Hour()
		if tt%12 == 0 {
			buf.WriteString("12")
		} else {
			buf.WriteString(strconv.FormatInt(int64(tt%12), 10))
		}
	case 'p':
		hour := t.Hour()
		if hour/12%2 == 0 {
			buf.WriteString("AM")
		} else {
			buf.WriteString("PM")
		}
	case 'r':
		h := t.Hour()
		h %= 24
		switch {
		case h == 0:
			fmt.Fprintf(buf, "%02d:%02d:%02d AM", 12, t.Minute(), t.Sec())
		case h == 12:
			fmt.Fprintf(buf, "%02d:%02d:%02d PM", 12, t.Minute(), t.Sec())
		case h < 12:
			fmt.Fprintf(buf, "%02d:%02d:%02d AM", h, t.Minute(), t.Sec())
		default:
			fmt.Fprintf(buf, "%02d:%02d:%02d PM", h-12, t.Minute(), t.Sec())
		}
	case 'S', 's':
		buf.WriteString(FormatIntByWidth(int(t.Sec()), 2))
	case 'T':
		fmt.Fprintf(buf, "%02d:%02d:%02d", t.Hour(), t.Minute(), t.Sec())
	case 'U':
		w := t.Week(0)
		buf.WriteString(FormatIntByWidth(w, 2))
	case 'u':
		w := t.Week(1)
		buf.WriteString(FormatIntByWidth(w, 2))
	case 'V':
		w := t.Week(2)
		buf.WriteString(FormatIntByWidth(w, 2))
	case 'v':
		_, w := t.YearWeek(3)
		buf.WriteString(FormatIntByWidth(w, 2))
	case 'a':
		weekday := t.DayOfWeek()
		buf.WriteString(AbbrevWeekdayName[weekday])
	case 'W':
		buf.WriteString(t.DayOfWeek().String())
	case 'w':
		buf.WriteString(strconv.FormatInt(int64(t.DayOfWeek()), 10))
	case 'X':
		year, _ := t.YearWeek(2)
		if year < 0 {
			buf.WriteString(strconv.FormatUint(uint64(math.MaxUint32), 10))
		} else {
			buf.WriteString(FormatIntByWidth(year, 4))
		}
	case 'x':
		year, _ := t.YearWeek(3)
		if year < 0 {
			buf.WriteString(strconv.FormatUint(uint64(math.MaxUint32), 10))
		} else {
			buf.WriteString(FormatIntByWidth(year, 4))
		}
	case 'Y':
		buf.WriteString(FormatIntByWidth(int(t.Year()), 4))
	case 'y':
		str := FormatIntByWidth(int(t.Year()), 4)
		buf.WriteString(str[2:])
	default:
		buf.WriteRune(b)
	}
	return nil
}

// FormatIntByWidth: Formatintwidthn is used to format ints with width parameter n. Insufficient numbers are filled with 0.
func FormatIntByWidth(num, n int) string {
	numStr := strconv.FormatInt(int64(num), 10)
	if len(numStr) >= n {
		return numStr
	}
	padBytes := make([]byte, n-len(numStr))
	for i := range padBytes {
		padBytes[i] = '0'
	}
	return string(padBytes) + numStr
}

// AbbrDayOfMonth: Get the abbreviation of month of day
func AbbrDayOfMonth(day int) string {
	var str string
	switch day {
	case 1, 21, 31:
		str = "st"
	case 2, 22:
		str = "nd"
	case 3, 23:
		str = "rd"
	default:
		str = "th"
	}
	return str
}

func doDateSub(start types.Date, diff int64, unit int64) (types.Date, error) {
	err := types.JudgeIntervalNumOverflow(diff, types.IntervalType(unit))
	if err != nil {
		return 0, err
	}
	dt, success := start.ToDatetime().AddInterval(-diff, types.IntervalType(unit), types.DateType)
	if success {
		return dt.ToDate(), nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("date", "")
	}
}

func doTimeSub(start types.Time, diff int64, unit int64) (types.Time, error) {
	err := types.JudgeIntervalNumOverflow(diff, types.IntervalType(unit))
	if err != nil {
		return 0, err
	}
	t, success := start.AddInterval(-diff, types.IntervalType(unit))
	if success {
		return t, nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("time", "")
	}
}

func doDatetimeSub(start types.Datetime, diff int64, unit int64) (types.Datetime, error) {
	err := types.JudgeIntervalNumOverflow(diff, types.IntervalType(unit))
	if err != nil {
		return 0, err
	}
	dt, success := start.AddInterval(-diff, types.IntervalType(unit), types.DateTimeType)
	if success {
		return dt, nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("datetime", "")
	}
}

func doDateStringSub(startStr string, diff int64, unit int64) (types.Datetime, error) {
	err := types.JudgeIntervalNumOverflow(diff, types.IntervalType(unit))
	if err != nil {
		return 0, err
	}
	start, err := types.ParseDatetime(startStr, 6)
	if err != nil {
		return 0, err
	}
	dt, success := start.AddInterval(-diff, types.IntervalType(unit), types.DateType)
	if success {
		return dt, nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("datetime", "")
	}
}

func doTimestampSub(loc *time.Location, start types.Timestamp, diff int64, unit int64) (types.Timestamp, error) {
	err := types.JudgeIntervalNumOverflow(diff, types.IntervalType(unit))
	if err != nil {
		return 0, err
	}
	dt, success := start.ToDatetime(loc).AddInterval(-diff, types.IntervalType(unit), types.DateTimeType)
	if success {
		return dt.ToTimestamp(loc), nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("timestamp", "")
	}
}

func DateSub(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[types.Date](result)
	unit, _ := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[2]).GetValue(0)

	starts := vector.GenerateFunctionFixedTypeParameter[types.Date](ivecs[0])
	diffs := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := starts.GetValue(i)
		v2, null2 := diffs.GetValue(i)

		if null1 || null2 {
			if err = rs.Append(v1, true); err != nil {
				return err
			}
		} else {
			val, err := doDateSub(v1, v2, unit)
			if err != nil {
				return err
			}
			if err = rs.Append(val, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func DatetimeSub(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[types.Datetime](result)
	unit, _ := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[2]).GetValue(0)

	starts := vector.GenerateFunctionFixedTypeParameter[types.Datetime](ivecs[0])
	diffs := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := starts.GetValue(i)
		v2, null2 := diffs.GetValue(i)

		if null1 || null2 {
			if err = rs.Append(v1, true); err != nil {
				return err
			}
		} else {
			val, err := doDatetimeSub(v1, v2, unit)
			if err != nil {
				return err
			}
			if err = rs.Append(val, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func DateStringSub(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[types.Datetime](result)
	unit, _ := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[2]).GetValue(0)

	var d types.Datetime
	starts := vector.GenerateFunctionStrParameter(ivecs[0])
	diffs := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := starts.GetStrValue(i)
		v2, null2 := diffs.GetValue(i)

		if null1 || null2 {
			if err = rs.Append(d, true); err != nil {
				return err
			}
		} else {
			val, err := doDateStringSub(string(v1), v2, unit)
			if err != nil {
				return err
			}
			if err = rs.Append(val, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func TimestampSub(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[types.Timestamp](result)
	unit, _ := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[2]).GetValue(0)

	starts := vector.GenerateFunctionFixedTypeParameter[types.Timestamp](ivecs[0])
	diffs := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := starts.GetValue(i)
		v2, null2 := diffs.GetValue(i)

		if null1 || null2 {
			if err = rs.Append(v1, true); err != nil {
				return err
			}
		} else {
			val, err := doTimestampSub(proc.SessionInfo.TimeZone, v1, v2, unit)
			if err != nil {
				return err
			}
			if err = rs.Append(val, false); err != nil {
				return err
			}
		}
	}
	return nil
}

type number interface {
	constraints.Unsigned | constraints.Signed | constraints.Float
}

func FieldNumber[T number](ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[uint64](result)

	fs := make([]vector.FunctionParameterWrapper[T], len(ivecs))
	for i := range ivecs {
		fs[i] = vector.GenerateFunctionFixedTypeParameter[T](ivecs[i])
	}

	nums := make([]uint64, length)

	for j := 1; j < len(ivecs); j++ {

		for i := uint64(0); i < uint64(length); i++ {

			v1, null1 := fs[0].GetValue(i)
			v2, null2 := fs[j].GetValue(i)

			if (nums[i] != 0) || (null1 || null2) {
				continue
			}

			if v1 == v2 {
				nums[i] = uint64(j)
			}

		}
	}

	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.Append(nums[i], false); err != nil {
			return err
		}
	}

	return nil
}

func FieldString(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[uint64](result)

	fs := make([]vector.FunctionParameterWrapper[types.Varlena], len(ivecs))
	for i := range ivecs {
		fs[i] = vector.GenerateFunctionStrParameter(ivecs[i])
	}

	nums := make([]uint64, length)

	for j := 1; j < len(ivecs); j++ {

		for i := uint64(0); i < uint64(length); i++ {

			v1, null1 := fs[0].GetStrValue(i)
			v2, null2 := fs[j].GetStrValue(i)

			if (nums[i] != 0) || (null1 || null2) {
				continue
			}

			if string(v1) == string(v2) {
				nums[i] = uint64(j)
			}

		}
	}

	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.Append(nums[i], false); err != nil {
			return err
		}
	}

	return nil
}

func FormatWith2Args(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[types.Varlena](result)

	vs1 := vector.GenerateFunctionStrParameter(ivecs[0])
	vs2 := vector.GenerateFunctionStrParameter(ivecs[1])

	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := vs1.GetStrValue(i)
		v2, null2 := vs2.GetStrValue(i)

		if null1 || null2 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			val, err := format.GetNumberFormat(string(v1), string(v2), "en_US")
			if err != nil {
				return err
			}
			if err = rs.AppendBytes([]byte(val), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func FormatWith3Args(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[types.Varlena](result)

	vs1 := vector.GenerateFunctionStrParameter(ivecs[0])
	vs2 := vector.GenerateFunctionStrParameter(ivecs[1])
	vs3 := vector.GenerateFunctionStrParameter(ivecs[2])

	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := vs1.GetStrValue(i)
		v2, null2 := vs2.GetStrValue(i)
		v3, null3 := vs3.GetStrValue(i)

		if null1 || null2 || null3 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			val, err := format.GetNumberFormat(string(v1), string(v2), string(v3))
			if err != nil {
				return err
			}
			if err = rs.AppendBytes([]byte(val), false); err != nil {
				return err
			}
		}
	}
	return nil
}

const (
	max_unix_timestamp_int = 32536771199
)

func FromUnixTimeInt64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[types.Datetime](result)
	vs := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[0])

	var d types.Datetime
	for i := uint64(0); i < uint64(length); i++ {
		v, null := vs.GetValue(i)

		if null || (v < 0 || v > max_unix_timestamp_int) {
			if err = rs.Append(d, true); err != nil {
				return err
			}
		} else {
			if err = rs.Append(types.DatetimeFromUnix(proc.SessionInfo.TimeZone, v), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func FromUnixTimeUint64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[types.Datetime](result)
	vs := vector.GenerateFunctionFixedTypeParameter[uint64](ivecs[0])

	var d types.Datetime
	for i := uint64(0); i < uint64(length); i++ {
		v, null := vs.GetValue(i)

		if null || (v < 0 || v > max_unix_timestamp_int) {
			if err = rs.Append(d, true); err != nil {
				return err
			}
		} else {
			if err = rs.Append(types.DatetimeFromUnix(proc.SessionInfo.TimeZone, int64(v)), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func splitDecimalToIntAndFrac(f float64) (int64, int64) {
	intPart := int64(f)
	nano := (f - float64(intPart)) * math.Pow10(9)
	fracPart := int64(nano)
	return intPart, fracPart
}

func FromUnixTimeFloat64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[types.Datetime](result)
	vs := vector.GenerateFunctionFixedTypeParameter[float64](ivecs[0])

	var d types.Datetime
	for i := uint64(0); i < uint64(length); i++ {
		v, null := vs.GetValue(i)

		if null || (v < 0 || v > max_unix_timestamp_int) {
			if err = rs.Append(d, true); err != nil {
				return err
			}
		} else {
			x, y := splitDecimalToIntAndFrac(v)
			if err = rs.Append(types.DatetimeFromUnixWithNsec(proc.SessionInfo.TimeZone, x, y), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func FromUnixTimeInt64Format(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	if !ivecs[1].IsConst() {
		return moerr.NewInvalidArg(proc.Ctx, "from_unixtime format", "not constant")
	}

	rs := vector.MustFunctionResult[types.Varlena](result)
	vs := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[0])
	formatMask, null1 := vector.GenerateFunctionStrParameter(ivecs[1]).GetStrValue(0)
	f := string(formatMask)

	for i := uint64(0); i < uint64(length); i++ {
		v, null := vs.GetValue(i)

		if null || (v < 0 || v > max_unix_timestamp_int) || null1 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			r := types.DatetimeFromUnix(proc.SessionInfo.TimeZone, v)
			formatStr, err := datetimeFormat(proc.Ctx, r, f)
			if err != nil {
				return err
			}
			if err = rs.AppendBytes([]byte(formatStr), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func FromUnixTimeUint64Format(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	if !ivecs[1].IsConst() {
		return moerr.NewInvalidArg(proc.Ctx, "from_unixtime format", "not constant")
	}

	rs := vector.MustFunctionResult[types.Varlena](result)
	vs := vector.GenerateFunctionFixedTypeParameter[uint64](ivecs[0])
	formatMask, null1 := vector.GenerateFunctionStrParameter(ivecs[1]).GetStrValue(0)
	f := string(formatMask)

	for i := uint64(0); i < uint64(length); i++ {
		v, null := vs.GetValue(i)

		if null || (v < 0 || v > max_unix_timestamp_int) || null1 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			r := types.DatetimeFromUnix(proc.SessionInfo.TimeZone, int64(v))
			formatStr, err := datetimeFormat(proc.Ctx, r, f)
			if err != nil {
				return err
			}
			if err = rs.AppendBytes([]byte(formatStr), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func FromUnixTimeFloat64Format(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	if !ivecs[1].IsConst() {
		return moerr.NewInvalidArg(proc.Ctx, "from_unixtime format", "not constant")
	}

	rs := vector.MustFunctionResult[types.Varlena](result)
	vs := vector.GenerateFunctionFixedTypeParameter[float64](ivecs[0])
	formatMask, null1 := vector.GenerateFunctionStrParameter(ivecs[1]).GetStrValue(0)
	f := string(formatMask)

	for i := uint64(0); i < uint64(length); i++ {
		v, null := vs.GetValue(i)

		if null || (v < 0 || v > max_unix_timestamp_int) || null1 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			x, y := splitDecimalToIntAndFrac(v)
			r := types.DatetimeFromUnixWithNsec(proc.SessionInfo.TimeZone, x, y)
			formatStr, err := datetimeFormat(proc.Ctx, r, f)
			if err != nil {
				return err
			}
			if err = rs.AppendBytes([]byte(formatStr), false); err != nil {
				return err
			}
		}
	}
	return nil
}

// Slice from left to right, starting from 0
func getSliceFromLeft(s string, offset int64) string {
	sourceRune := []rune(s)
	elemsize := int64(len(sourceRune))
	if offset > elemsize {
		return ""
	}
	substrRune := sourceRune[offset:]
	return string(substrRune)
}

// Cut slices from right to left, starting from 1
func getSliceFromRight(s string, offset int64) string {
	sourceRune := []rune(s)
	elemsize := int64(len(sourceRune))
	if offset > elemsize {
		return ""
	}
	substrRune := sourceRune[elemsize-offset:]
	return string(substrRune)
}

func SubStringWith2Args[T number](ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[types.Varlena](result)
	vs := vector.GenerateFunctionStrParameter(ivecs[0])
	starts := vector.GenerateFunctionFixedTypeParameter[T](ivecs[1])

	for i := uint64(0); i < uint64(length); i++ {
		v, null1 := vs.GetStrValue(i)
		s, null2 := starts.GetValue(i)

		if null1 || null2 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			var r string
			start := int64(s)
			if start > 0 {
				r = getSliceFromLeft(string(v), start-1)
			} else if start < 0 {
				r = getSliceFromRight(string(v), -start)
			} else {
				r = ""
			}
			if err = rs.AppendBytes([]byte(r), false); err != nil {
				return err
			}
		}
	}
	return nil
}

// Cut the slice with length from left to right, starting from 0
func getSliceFromLeftWithLength(s string, offset int64, length int64) string {
	if offset < 0 {
		return ""
	}
	return getSliceOffsetLen(s, offset, length)
}

func getSliceOffsetLen(s string, offset int64, length int64) string {
	sourceRune := []rune(s)
	elemsize := int64(len(sourceRune))
	if offset < 0 {
		offset += elemsize
		if offset < 0 {
			return ""
		}
	}
	if offset >= elemsize {
		return ""
	}

	if length <= 0 {
		return ""
	} else {
		end := offset + length
		if end > elemsize {
			end = elemsize
		}
		substrRune := sourceRune[offset:end]
		return string(substrRune)
	}
}

// From right to left, cut the slice with length from 1
func getSliceFromRightWithLength(s string, offset int64, length int64) string {
	return getSliceOffsetLen(s, -offset, length)
}

func SubStringWith3Args[T1, T2 number](ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[types.Varlena](result)
	vs := vector.GenerateFunctionStrParameter(ivecs[0])
	starts := vector.GenerateFunctionFixedTypeParameter[T1](ivecs[1])
	lens := vector.GenerateFunctionFixedTypeParameter[T2](ivecs[2])

	for i := uint64(0); i < uint64(length); i++ {
		v, null1 := vs.GetStrValue(i)
		s, null2 := starts.GetValue(i)
		l, null3 := lens.GetValue(i)

		if null1 || null2 || null3 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			var r string
			start := int64(s)
			lt := int64(l)
			if start > 0 {
				r = getSliceFromLeftWithLength(string(v), start-1, lt)
			} else if start < 0 {
				r = getSliceFromRightWithLength(string(v), -start, lt)
			} else {
				r = ""
			}
			if err = rs.AppendBytes([]byte(r), false); err != nil {
				return err
			}
		}
	}
	return nil
}
