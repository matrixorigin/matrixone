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
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vectorize/floor"
	"github.com/matrixorigin/matrixone/pkg/vectorize/format"
	"github.com/matrixorigin/matrixone/pkg/vectorize/instr"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
	"math"
	"strconv"
	"strings"
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

func generalMathMulti[T mathMultiT](funcName string, ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int,
	cb mathMultiFun[T]) (err error) {
	digits := int64(0)
	if len(ivecs) > 1 {
		if ivecs[1].IsConstNull() || !ivecs[1].IsConst() {
			return moerr.NewInvalidArg(proc.Ctx, fmt.Sprintf("the second argument of the %s", funcName), "not const")
		}
		digits = vector.MustFixedCol[int64](ivecs[1])[0]
	}

	return opUnaryFixedToFixed[T, T](ivecs, result, proc, length, func(x T) T {
		return cb(x, digits)
	})
}

func CeilStr(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	digits := int64(0)
	if len(ivecs) > 1 {
		if !ivecs[1].IsConst() || ivecs[1].GetType().Oid != types.T_int64 {
			return moerr.NewInvalidArg(proc.Ctx, fmt.Sprintf("the second argument of the %s", "ceil"), "not const")
		}
		digits = vector.MustFixedCol[int64](ivecs[1])[0]
	}

	return opUnaryStrToFixedWithErrorCheck[float64](ivecs, result, proc, length, func(v string) (float64, error) {
		floatVal, err1 := strconv.ParseFloat(v, 64)
		if err1 != nil {
			return 0, err1
		}
		return ceilFloat64(floatVal, digits), nil
	})
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

var MaxUint64digits = numOfDigits(math.MaxUint64) // 20
var MaxInt64digits = numOfDigits(math.MaxInt64)   // 19

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

func coalesceCheck(overloads []overload, inputs []types.Type) checkResult {
	if len(inputs) > 0 {
		minIndex := -1
		minOid := types.T(0)
		minCost := math.MaxInt
		overloadRequire := make([]types.T, len(inputs))
		for i, over := range overloads {
			requireOid := over.args[0]
			for j := range overloadRequire {
				overloadRequire[j] = requireOid
			}

			sta, cos := tryToMatch(inputs, overloadRequire)
			if sta == matchFailed {
				continue
			} else if sta == matchDirectly {
				return newCheckResultWithSuccess(i)
			} else {
				if cos < minCost {
					minIndex = i
					minCost = cos
					minOid = requireOid
				}
			}
		}
		if minIndex == -1 {
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		}
		castType := make([]types.Type, len(inputs))
		for i := range castType {
			if minOid == inputs[i].Oid {
				castType[i] = inputs[i]
			} else {
				castType[i] = minOid.ToType()
				setTargetScaleFromSource(&inputs[i], &castType[i])
			}
		}

		if minOid.IsDecimal() || minOid.IsDateRelate() {
			setMaxScaleForAll(castType)
		}
		return newCheckResultWithCast(minIndex, castType)
	}
	return newCheckResultWithFailure(failedFunctionParametersWrong)
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

func concatWsCheck(overloads []overload, inputs []types.Type) checkResult {
	// all args should be string or can cast to string type.
	if len(inputs) > 1 {
		ret := make([]types.Type, len(inputs))
		shouldConvert := false

		for i, t := range inputs {
			if t.Oid.IsMySQLString() {
				ret[i] = t
				continue
			}
			if can, _ := fixedImplicitTypeCast(t, types.T_varchar); can {
				shouldConvert = true
				ret[i] = types.T_varchar.ToType()
			} else {
				return newCheckResultWithFailure(failedFunctionParametersWrong)
			}
		}
		if shouldConvert {
			return newCheckResultWithCast(0, ret)
		}
		return newCheckResultWithSuccess(0)
	}
	return newCheckResultWithFailure(failedFunctionParametersWrong)
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
		canSp := false
		var str string
		for j := 1; j < len(vecs); j++ {
			v, null := vecs[j].GetStrValue(i)
			if null {
				continue
			}
			if canSp {
				str += string(sp)
			}
			canSp = true
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

func doDateAdd(start types.Date, diff int64, iTyp types.IntervalType) (types.Date, error) {
	err := types.JudgeIntervalNumOverflow(diff, iTyp)
	if err != nil {
		return 0, err
	}
	dt, success := start.ToDatetime().AddInterval(diff, iTyp, types.DateType)
	if success {
		return dt.ToDate(), nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("date", "")
	}
}

func doTimeAdd(start types.Time, diff int64, iTyp types.IntervalType) (types.Time, error) {
	err := types.JudgeIntervalNumOverflow(diff, iTyp)
	if err != nil {
		return 0, err
	}
	t, success := start.AddInterval(diff, iTyp)
	if success {
		return t, nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("time", "")
	}
}

func doDatetimeAdd(start types.Datetime, diff int64, iTyp types.IntervalType) (types.Datetime, error) {
	err := types.JudgeIntervalNumOverflow(diff, iTyp)
	if err != nil {
		return 0, err
	}
	dt, success := start.AddInterval(diff, iTyp, types.DateTimeType)
	if success {
		return dt, nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("datetime", "")
	}
}

func doDateStringAdd(startStr string, diff int64, iTyp types.IntervalType) (types.Datetime, error) {
	err := types.JudgeIntervalNumOverflow(diff, iTyp)
	if err != nil {
		return 0, err
	}
	start, err := types.ParseDatetime(startStr, 6)
	if err != nil {
		return 0, err
	}
	dt, success := start.AddInterval(diff, iTyp, types.DateType)
	if success {
		return dt, nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("datetime", "")
	}
}

func doTimestampAdd(loc *time.Location, start types.Timestamp, diff int64, iTyp types.IntervalType) (types.Timestamp, error) {
	err := types.JudgeIntervalNumOverflow(diff, iTyp)
	if err != nil {
		return 0, err
	}
	dt, success := start.ToDatetime(loc).AddInterval(diff, iTyp, types.DateTimeType)
	if success {
		return dt.ToTimestamp(loc), nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("timestamp", "")
	}
}

func DateAdd(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	unit, _ := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[2]).GetValue(0)
	iTyp := types.IntervalType(unit)

	return opBinaryFixedFixedToFixedWithErrorCheck[types.Date, int64, types.Date](ivecs, result, proc, length, func(v1 types.Date, v2 int64) (types.Date, error) {
		return doDateAdd(v1, v2, iTyp)
	})
}

func DatetimeAdd(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	unit, _ := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[2]).GetValue(0)
	scale := ivecs[0].GetType().Scale
	iTyp := types.IntervalType(unit)
	if iTyp == types.MicroSecond {
		scale = 6
	}
	rs := vector.MustFunctionResult[types.Datetime](result)
	rs.TempSetType(types.New(types.T_datetime, 0, scale))

	return opBinaryFixedFixedToFixedWithErrorCheck[types.Datetime, int64, types.Datetime](ivecs, result, proc, length, func(v1 types.Datetime, v2 int64) (types.Datetime, error) {
		return doDatetimeAdd(v1, v2, iTyp)
	})
}

func DateStringAdd(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	unit, _ := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[2]).GetValue(0)
	iTyp := types.IntervalType(unit)
	rs := vector.MustFunctionResult[types.Datetime](result)
	rs.TempSetType(types.New(types.T_datetime, 0, 6))

	return opBinaryStrFixedToFixedWithErrorCheck[int64, types.Datetime](ivecs, result, proc, length, func(v1 string, v2 int64) (types.Datetime, error) {
		return doDateStringAdd(v1, v2, iTyp)
	})
}

func TimestampAdd(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[types.Timestamp](result)
	unit, _ := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[2]).GetValue(0)
	scale := ivecs[0].GetType().Scale
	iTyp := types.IntervalType(unit)
	switch iTyp {
	case types.MicroSecond:
		scale = 6
	}
	rs.TempSetType(types.New(types.T_timestamp, 0, scale))

	return opBinaryFixedFixedToFixedWithErrorCheck[types.Timestamp, int64, types.Timestamp](ivecs, result, proc, length, func(v1 types.Timestamp, v2 int64) (types.Timestamp, error) {
		return doTimestampAdd(proc.SessionInfo.TimeZone, v1, v2, iTyp)
	})
}

func TimeAdd(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[types.Time](result)
	unit, _ := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[2]).GetValue(0)
	scale := ivecs[0].GetType().Scale
	iTyp := types.IntervalType(unit)
	switch iTyp {
	case types.MicroSecond:
		scale = 6
	}
	rs.TempSetType(types.New(types.T_time, 0, scale))

	return opBinaryFixedFixedToFixedWithErrorCheck[types.Time, int64, types.Time](ivecs, result, proc, length, func(v1 types.Time, v2 int64) (types.Time, error) {
		return doTimeAdd(v1, v2, iTyp)
	})
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

var (
	// WeekdayNames lists names of weekdays, which are used in builtin function `date_format`.
	WeekdayNames = []string{
		"Monday",
		"Tuesday",
		"Wednesday",
		"Thursday",
		"Friday",
		"Saturday",
		"Sunday",
	}

	// MonthNames lists names of months, which are used in builtin function `date_format`.
	MonthNames = []string{
		"January",
		"February",
		"March",
		"April",
		"May",
		"June",
		"July",
		"August",
		"September",
		"October",
		"November",
		"December",
	}

	// AbbrevWeekdayName lists Abbreviation of week names, which are used int builtin function 'date_format'
	AbbrevWeekdayName = []string{
		"Sun",
		"Mon",
		"Tue",
		"Wed",
		"Thu",
		"Fri",
		"Sat",
	}
)

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

func doDateSub(start types.Date, diff int64, iTyp types.IntervalType) (types.Date, error) {
	err := types.JudgeIntervalNumOverflow(diff, iTyp)
	if err != nil {
		return 0, err
	}
	dt, success := start.ToDatetime().AddInterval(-diff, iTyp, types.DateType)
	if success {
		return dt.ToDate(), nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("date", "")
	}
}

//func doTimeSub(start types.Time, diff int64, unit int64) (types.Time, error) {
//	err := types.JudgeIntervalNumOverflow(diff, types.IntervalType(unit))
//	if err != nil {
//		return 0, err
//	}
//	t, success := start.AddInterval(-diff, types.IntervalType(unit))
//	if success {
//		return t, nil
//	} else {
//		return 0, moerr.NewOutOfRangeNoCtx("time", "")
//	}
//}

func doDatetimeSub(start types.Datetime, diff int64, iTyp types.IntervalType) (types.Datetime, error) {
	err := types.JudgeIntervalNumOverflow(diff, iTyp)
	if err != nil {
		return 0, err
	}
	dt, success := start.AddInterval(-diff, iTyp, types.DateTimeType)
	if success {
		return dt, nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("datetime", "")
	}
}

func doDateStringSub(startStr string, diff int64, iTyp types.IntervalType) (types.Datetime, error) {
	err := types.JudgeIntervalNumOverflow(diff, iTyp)
	if err != nil {
		return 0, err
	}
	start, err := types.ParseDatetime(startStr, 6)
	if err != nil {
		return 0, err
	}
	dt, success := start.AddInterval(-diff, iTyp, types.DateType)
	if success {
		return dt, nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("datetime", "")
	}
}

func doTimestampSub(loc *time.Location, start types.Timestamp, diff int64, iTyp types.IntervalType) (types.Timestamp, error) {
	err := types.JudgeIntervalNumOverflow(diff, iTyp)
	if err != nil {
		return 0, err
	}
	dt, success := start.ToDatetime(loc).AddInterval(-diff, iTyp, types.DateTimeType)
	if success {
		return dt.ToTimestamp(loc), nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("timestamp", "")
	}
}

func DateSub(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	unit, _ := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[2]).GetValue(0)
	iTyp := types.IntervalType(unit)

	return opBinaryFixedFixedToFixedWithErrorCheck[types.Date, int64, types.Date](ivecs, result, proc, length, func(v1 types.Date, v2 int64) (types.Date, error) {
		return doDateSub(v1, v2, iTyp)
	})
}

func DatetimeSub(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	unit, _ := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[2]).GetValue(0)
	scale := ivecs[0].GetType().Scale
	iTyp := types.IntervalType(unit)
	if iTyp == types.MicroSecond {
		scale = 6
	}
	rs := vector.MustFunctionResult[types.Datetime](result)
	rs.TempSetType(types.New(types.T_datetime, 0, scale))

	return opBinaryFixedFixedToFixedWithErrorCheck[types.Datetime, int64, types.Datetime](ivecs, result, proc, length, func(v1 types.Datetime, v2 int64) (types.Datetime, error) {
		return doDatetimeSub(v1, v2, iTyp)
	})
}

func DateStringSub(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[types.Datetime](result)
	unit, _ := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[2]).GetValue(0)

	var d types.Datetime
	starts := vector.GenerateFunctionStrParameter(ivecs[0])
	diffs := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])
	rs.TempSetType(types.New(types.T_datetime, 0, 6))
	iTyp := types.IntervalType(unit)
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := starts.GetStrValue(i)
		v2, null2 := diffs.GetValue(i)

		if null1 || null2 {
			if err = rs.Append(d, true); err != nil {
				return err
			}
		} else {
			val, err := doDateStringSub(functionUtil.QuickBytesToStr(v1), v2, iTyp)
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
	unit, _ := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[2]).GetValue(0)
	iTyp := types.IntervalType(unit)

	scale := ivecs[0].GetType().Scale
	rs := vector.MustFunctionResult[types.Timestamp](result)
	rs.TempSetType(types.New(types.T_timestamp, 0, scale))

	return opBinaryFixedFixedToFixedWithErrorCheck[types.Timestamp, int64, types.Timestamp](ivecs, result, proc, length, func(v1 types.Timestamp, v2 int64) (types.Timestamp, error) {
		return doTimestampSub(proc.SessionInfo.TimeZone, v1, v2, iTyp)
	})
}

type number interface {
	constraints.Unsigned | constraints.Signed | constraints.Float
}

func fieldCheck(overloads []overload, inputs []types.Type) checkResult {
	tc := func(inputs []types.Type, t types.T) bool {
		for _, input := range inputs {
			if input.Oid != t && input.Oid != types.T_any {
				return false
			}
		}
		return true
	}
	if len(inputs) < 2 {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}
	returnType := [...]types.T{types.T_varchar, types.T_char, types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64, types.T_float32, types.T_float64}
	for i, r := range returnType {
		if tc(inputs, r) {
			if i < 2 {
				return newCheckResultWithSuccess(0)
			} else {
				return newCheckResultWithSuccess(i - 1)
			}
		}
	}
	castTypes := make([]types.T, len(inputs))
	targetTypes := make([]types.Type, len(inputs))
	for j := 0; j < len(inputs); j++ {
		castTypes[j] = types.T_float64
		targetTypes[j] = types.T_float64.ToType()
	}
	c, _ := tryToMatch(inputs, castTypes)
	if c == matchFailed {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}
	if c == matchByCast {
		return newCheckResultWithCast(10, targetTypes)
	}
	return newCheckResultWithSuccess(10)
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

			if strings.EqualFold(functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v2)) {
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

func formatCheck(overloads []overload, inputs []types.Type) checkResult {
	if len(inputs) > 1 {
		// if the first param's type is time type. return failed.
		if inputs[0].Oid.IsDateRelate() {
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		}
		return fixedTypeMatch(overloads, inputs)
	}
	return newCheckResultWithFailure(failedFunctionParametersWrong)
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

	val := ""
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := vs1.GetStrValue(i)
		v2, null2 := vs2.GetStrValue(i)
		v3, null3 := vs3.GetStrValue(i)

		if null1 || null2 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			if null3 {
				val, err = format.GetNumberFormat(string(v1), string(v2), "en_US")
			} else {
				val, err = format.GetNumberFormat(string(v1), string(v2), string(v3))
			}
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

		if null || v > max_unix_timestamp_int {
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
	rs.TempSetType(types.New(types.T_datetime, 0, 6))
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

		if null1 || null || v > max_unix_timestamp_int {
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

func SubStringWith2Args(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[types.Varlena](result)
	vs := vector.GenerateFunctionStrParameter(ivecs[0])
	starts := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])

	for i := uint64(0); i < uint64(length); i++ {
		v, null1 := vs.GetStrValue(i)
		s, null2 := starts.GetValue(i)

		if null1 || null2 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			var r string
			if s > 0 {
				r = getSliceFromLeft(functionUtil.QuickBytesToStr(v), s-1)
			} else if s < 0 {
				r = getSliceFromRight(functionUtil.QuickBytesToStr(v), -s)
			} else {
				r = ""
			}
			if err = rs.AppendBytes(functionUtil.QuickStrToBytes(r), false); err != nil {
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

func SubStringWith3Args(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[types.Varlena](result)
	vs := vector.GenerateFunctionStrParameter(ivecs[0])
	starts := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])
	lens := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[2])

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
			if s > 0 {
				r = getSliceFromLeftWithLength(functionUtil.QuickBytesToStr(v), s-1, l)
			} else if s < 0 {
				r = getSliceFromRightWithLength(functionUtil.QuickBytesToStr(v), -s, l)
			} else {
				r = ""
			}
			if err = rs.AppendBytes(functionUtil.QuickStrToBytes(r), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func subStrIndex(str, delim string, count int64) (string, error) {
	// if the length of delim is 0, return empty string
	if len(delim) == 0 {
		return "", nil
	}
	// if the count is 0, return empty string
	if count == 0 {
		return "", nil
	}

	partions := strings.Split(str, delim)
	start, end := int64(0), int64(len(partions))

	if count > 0 {
		//is count is positive, reset the end position
		if count < end {
			end = count
		}
	} else {
		count = -count

		// -count overflows max int64, return the whole string.
		if count < 0 {
			return str, nil
		}

		//if count is negative, reset the start postion
		if count < end {
			start = end - count
		}
	}
	subPartions := partions[start:end]
	return strings.Join(subPartions, delim), nil
}

func getCount[T number](typ types.Type, val T) int64 {
	var r int64
	switch typ.Oid {
	case types.T_float64:
		v := float64(val)
		if v > float64(math.MaxInt64) {
			r = math.MaxInt64
		} else if v < float64(math.MinInt64) {
			r = math.MinInt64
		} else {
			r = int64(v)
		}
	case types.T_uint64:
		v := uint64(val)
		if v > uint64(math.MaxInt64) {
			r = math.MaxInt64
		} else {
			r = int64(v)
		}
	default:
		r = int64(val)
	}
	return r
}

func SubStrIndex[T number](ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[types.Varlena](result)
	vs := vector.GenerateFunctionStrParameter(ivecs[0])
	delims := vector.GenerateFunctionStrParameter(ivecs[1])
	counts := vector.GenerateFunctionFixedTypeParameter[T](ivecs[2])
	typ := counts.GetType()

	for i := uint64(0); i < uint64(length); i++ {
		v, null1 := vs.GetStrValue(i)
		d, null2 := delims.GetStrValue(i)
		c, null3 := counts.GetValue(i)

		if null1 || null2 || null3 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			r, err := subStrIndex(string(v), string(d), getCount(typ, c))
			if err != nil {
				return err
			}

			if err = rs.AppendBytes([]byte(r), false); err != nil {
				return err
			}
		}
	}
	return nil
}

// STARTSWITH

func StartsWith(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) (err error) {
	p1 := vector.GenerateFunctionStrParameter(ivecs[0])
	p2 := vector.GenerateFunctionStrParameter(ivecs[1])
	rs := vector.MustFunctionResult[uint8](result)

	//TODO: ignoring 4 switch cases: Original code:https://github.com/m-schen/matrixone/blob/0c480ca11b6302de26789f916a3e2faca7f79d47/pkg/sql/plan/function/builtin/binary/startswith.go#L36
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		v2, null2 := p2.GetStrValue(i)
		if null1 || null2 {
			if err = rs.Append(0, true); err != nil {
				return err
			}
		} else {
			res := hasPrefix(v1, v2)
			if err = rs.Append(res, false); err != nil {
				return err
			}
		}
	}
	return nil
}
func hasPrefix(b1, b2 []byte) uint8 {
	if len(b1) >= len(b2) && bytes.Equal(b1[:len(b2)], b2) {
		return 1
	}
	return 0
}

// ENDSWITH

func EndsWith(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) (err error) {
	p1 := vector.GenerateFunctionStrParameter(ivecs[0])
	p2 := vector.GenerateFunctionStrParameter(ivecs[1])
	rs := vector.MustFunctionResult[uint8](result)

	//TODO: ignoring 4 switch cases: Original code:https://github.com/m-schen/matrixone/blob/0c480ca11b6302de26789f916a3e2faca7f79d47/pkg/sql/plan/function/builtin/binary/endswith.go#L43
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		v2, null2 := p2.GetStrValue(i)
		if null1 || null2 {
			if err = rs.Append(0, true); err != nil {
				return err
			}
		} else {
			res := isEqualSuffix(string(v1), string(v2))
			if err = rs.Append(res, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func isEqualSuffix(b1, b2 string) uint8 {
	if len(b1) >= len(b2) && bytes.Equal([]byte(b1)[len(b1)-len(b2):], []byte(b2)) {
		return 1
	}
	return 0
}

// EXTRACT

func ExtractFromDate(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	if !ivecs[0].IsConst() {
		return moerr.NewInternalError(proc.Ctx, "invalid input for extract")
	}
	p1 := vector.GenerateFunctionStrParameter(ivecs[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[types.Date](ivecs[1])
	rs := vector.MustFunctionResult[uint32](result)

	v1, null1 := p1.GetStrValue(0)
	if null1 {
		for i := uint64(0); i < uint64(length); i++ {
			if err = rs.Append(0, true); err != nil {
				return err
			}
		}
		return nil
	}

	unit := functionUtil.QuickBytesToStr(v1)
	for i := uint64(0); i < uint64(length); i++ {
		v2, null2 := p2.GetValue(i)
		if null2 {
			if err = rs.Append(0, true); err != nil {
				return err
			}
		} else {
			res, _ := extractFromDate(unit, v2)
			if err = rs.Append(res, false); err != nil {
				return err
			}
		}
	}
	return nil
}

var validDateUnit = map[string]struct{}{
	"year":       {},
	"month":      {},
	"day":        {},
	"year_month": {},
	"quarter":    {},
}

func extractFromDate(unit string, d types.Date) (uint32, error) {
	if _, ok := validDateUnit[unit]; !ok {
		return 0, moerr.NewInternalErrorNoCtx("invalid unit")
	}
	var result uint32
	switch unit {
	case "day":
		result = uint32(d.Day())
	case "week":
		result = uint32(d.WeekOfYear2())
	case "month":
		result = uint32(d.Month())
	case "quarter":
		result = d.Quarter()
	case "year_month":
		result = d.YearMonth()
	case "year":
		result = uint32(d.Year())
	}
	return result, nil
}

func ExtractFromDatetime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	if !ivecs[0].IsConst() {
		return moerr.NewInternalError(proc.Ctx, "invalid input")
	}

	p1 := vector.GenerateFunctionStrParameter(ivecs[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[types.Datetime](ivecs[1])
	rs := vector.MustFunctionResult[types.Varlena](result)

	v1, null1 := p1.GetStrValue(0)
	if null1 {
		for i := uint64(0); i < uint64(length); i++ {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		}
		return nil
	}
	unit := functionUtil.QuickBytesToStr(v1)
	for i := uint64(0); i < uint64(length); i++ {
		v2, null2 := p2.GetValue(i)
		if null2 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			res, _ := extractFromDatetime(unit, v2)
			if err = rs.AppendBytes(functionUtil.QuickStrToBytes(res), false); err != nil {
				return err
			}
		}
	}

	return nil
}

var validDatetimeUnit = map[string]struct{}{
	"microsecond":        {},
	"second":             {},
	"minute":             {},
	"hour":               {},
	"day":                {},
	"week":               {},
	"month":              {},
	"quarter":            {},
	"year":               {},
	"second_microsecond": {},
	"minute_microsecond": {},
	"minute_second":      {},
	"hour_microsecond":   {},
	"hour_second":        {},
	"hour_minute":        {},
	"day_microsecond":    {},
	"day_second":         {},
	"day_minute":         {},
	"day_hour":           {},
	"year_month":         {},
}

func extractFromDatetime(unit string, d types.Datetime) (string, error) {
	if _, ok := validDatetimeUnit[unit]; !ok {
		return "", moerr.NewInternalErrorNoCtx("invalid unit")
	}
	var value string
	switch unit {
	case "microsecond":
		value = fmt.Sprintf("%d", int(d.MicroSec()))
	case "second":
		value = fmt.Sprintf("%02d", int(d.Sec()))
	case "minute":
		value = fmt.Sprintf("%02d", int(d.Minute()))
	case "hour":
		value = fmt.Sprintf("%02d", int(d.Hour()))
	case "day":
		value = fmt.Sprintf("%02d", int(d.ToDate().Day()))
	case "week":
		value = fmt.Sprintf("%02d", int(d.ToDate().WeekOfYear2()))
	case "month":
		value = fmt.Sprintf("%02d", int(d.ToDate().Month()))
	case "quarter":
		value = fmt.Sprintf("%d", int(d.ToDate().Quarter()))
	case "year":
		value = fmt.Sprintf("%04d", int(d.ToDate().Year()))
	case "second_microsecond":
		value = d.SecondMicrosecondStr()
	case "minute_microsecond":
		value = d.MinuteMicrosecondStr()
	case "minute_second":
		value = d.MinuteSecondStr()
	case "hour_microsecond":
		value = d.HourMicrosecondStr()
	case "hour_second":
		value = d.HourSecondStr()
	case "hour_minute":
		value = d.HourMinuteStr()
	case "day_microsecond":
		value = d.DayMicrosecondStr()
	case "day_second":
		value = d.DaySecondStr()
	case "day_minute":
		value = d.DayMinuteStr()
	case "day_hour":
		value = d.DayHourStr()
	case "year_month":
		value = d.ToDate().YearMonthStr()
	}
	return value, nil
}

func ExtractFromTime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	if !ivecs[0].IsConst() {
		return moerr.NewInternalError(proc.Ctx, "invalid input for extract")
	}

	p1 := vector.GenerateFunctionStrParameter(ivecs[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[types.Time](ivecs[1])
	rs := vector.MustFunctionResult[types.Varlena](result)

	v1, null1 := p1.GetStrValue(0)
	if null1 {
		for i := uint64(0); i < uint64(length); i++ {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		}
		return nil
	}
	unit := functionUtil.QuickBytesToStr(v1)
	for i := uint64(0); i < uint64(length); i++ {
		v2, null2 := p2.GetValue(i)
		if null2 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			res, _ := extractFromTime(unit, v2)
			if err = rs.AppendBytes(functionUtil.QuickStrToBytes(res), false); err != nil {
				return err
			}
		}
	}

	return nil
}

var validTimeUnit = map[string]struct{}{
	"microsecond":        {},
	"second":             {},
	"minute":             {},
	"hour":               {},
	"second_microsecond": {},
	"minute_microsecond": {},
	"minute_second":      {},
	"hour_microsecond":   {},
	"hour_second":        {},
	"hour_minute":        {},
	"day_microsecond":    {},
	"day_second":         {},
	"day_minute":         {},
	"day_hour":           {},
}

func extractFromTime(unit string, t types.Time) (string, error) {
	if _, ok := validTimeUnit[unit]; !ok {
		return "", moerr.NewInternalErrorNoCtx("invalid unit")
	}
	var value string
	switch unit {
	case "microsecond":
		value = fmt.Sprintf("%d", int(t))
	case "second":
		value = fmt.Sprintf("%02d", int(t.Sec()))
	case "minute":
		value = fmt.Sprintf("%02d", int(t.Minute()))
	case "hour", "day_hour":
		value = fmt.Sprintf("%02d", int(t.Hour()))
	case "second_microsecond":
		microSec := fmt.Sprintf("%0*d", 6, int(t.MicroSec()))
		value = fmt.Sprintf("%2d%s", int(t.Sec()), microSec)
	case "minute_microsecond":
		microSec := fmt.Sprintf("%0*d", 6, int(t.MicroSec()))
		value = fmt.Sprintf("%2d%2d%s", int(t.Minute()), int(t.Sec()), microSec)
	case "minute_second":
		value = fmt.Sprintf("%2d%2d", int(t.Minute()), int(t.Sec()))
	case "hour_microsecond", "day_microsecond":
		microSec := fmt.Sprintf("%0*d", 6, int(t.MicroSec()))
		value = fmt.Sprintf("%2d%2d%2d%s", int(t.Hour()), int(t.Minute()), int(t.Sec()), microSec)
	case "hour_second", "day_second":
		value = fmt.Sprintf("%2d%2d%2d", int(t.Hour()), int(t.Minute()), int(t.Sec()))
	case "hour_minute", "day_minute":
		value = fmt.Sprintf("%2d%2d", int(t.Hour()), int(t.Minute()))
	}
	return value, nil
}

func ExtractFromVarchar(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	if !ivecs[0].IsConst() {
		return moerr.NewInternalError(proc.Ctx, "invalid input for extract")
	}

	p1 := vector.GenerateFunctionStrParameter(ivecs[0])
	p2 := vector.GenerateFunctionStrParameter(ivecs[1])
	rs := vector.MustFunctionResult[types.Varlena](result)

	v1, null1 := p1.GetStrValue(0)
	if null1 {
		for i := uint64(0); i < uint64(length); i++ {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		}
		return nil
	}
	unit := functionUtil.QuickBytesToStr(v1)
	scale := p2.GetType().Scale
	for i := uint64(0); i < uint64(length); i++ {
		v2, null2 := p2.GetStrValue(i)
		if null2 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			res, err := extractFromVarchar(unit, functionUtil.QuickBytesToStr(v2), scale)
			if err != nil {
				return err
			}
			if err = rs.AppendBytes(functionUtil.QuickStrToBytes(res), false); err != nil {
				return err
			}
		}
	}

	return nil
}

func extractFromVarchar(unit string, t string, scale int32) (string, error) {
	var result string
	if len(t) == 0 {
		result = t
	} else if value, err := types.ParseDatetime(t, scale); err == nil {
		result, err = extractFromDatetime(unit, value)
		if err != nil {
			return "", err
		}
	} else if value, err := types.ParseTime(t, scale); err == nil {
		result, err = extractFromTime(unit, value)
		if err != nil {
			return "", err
		}
	} else {
		return "", moerr.NewInternalErrorNoCtx("invalid input")
	}

	return result, nil
}

// FINDINSET

func FindInSet(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) (err error) {
	p1 := vector.GenerateFunctionStrParameter(ivecs[0])
	p2 := vector.GenerateFunctionStrParameter(ivecs[1])
	rs := vector.MustFunctionResult[uint64](result)

	//TODO: ignoring 4 switch cases: Original code:https://github.com/m-schen/matrixone/blob/0c480ca11b6302de26789f916a3e2faca7f79d47/pkg/sql/plan/function/builtin/binary/findinset.go#L45
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		v2, null2 := p2.GetStrValue(i)
		if null1 || null2 {
			if err = rs.Append(0, true); err != nil {
				return err
			}
		} else {
			res := findInStrList(string(v1), string(v2))
			if err = rs.Append(res, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func findInStrList(str, strlist string) uint64 {
	for j, s := range strings.Split(strlist, ",") {
		if s == str {
			return uint64(j + 1)
		}
	}
	return 0
}

// INSTR

func Instr(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) (err error) {
	p1 := vector.GenerateFunctionStrParameter(ivecs[0])
	p2 := vector.GenerateFunctionStrParameter(ivecs[1])
	rs := vector.MustFunctionResult[int64](result)

	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		v2, null2 := p2.GetStrValue(i)
		if null1 || null2 {
			if err = rs.Append(0, true); err != nil {
				return err
			}
		} else {
			str1 := functionUtil.QuickBytesToStr(v1)
			str2 := functionUtil.QuickBytesToStr(v2)

			res := instr.Single(str1, str2)
			if err = rs.Append(res, false); err != nil {
				return err
			}
		}
	}
	return nil
}

// LEFT

func Left(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) (err error) {
	p1 := vector.GenerateFunctionStrParameter(ivecs[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])
	rs := vector.MustFunctionResult[types.Varlena](result)

	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			//TODO: Ignoring 4 switch cases: https://github.com/m-schen/matrixone/blob/0c480ca11b6302de26789f916a3e2faca7f79d47/pkg/sql/plan/function/builtin/binary/left.go#L38
			res := evalLeft(functionUtil.QuickBytesToStr(v1), v2)
			if err = rs.AppendBytes(functionUtil.QuickStrToBytes(res), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func evalLeft(str string, length int64) string {
	runeStr := []rune(str)
	leftLength := int(length)
	if strLength := len(runeStr); leftLength > strLength {
		leftLength = strLength
	} else if leftLength < 0 {
		leftLength = 0
	}
	return string(runeStr[:leftLength])
}

//POW

func Power(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) (err error) {
	p1 := vector.GenerateFunctionFixedTypeParameter[float64](ivecs[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[float64](ivecs[1])
	rs := vector.MustFunctionResult[float64](result)

	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			if err = rs.Append(0, true); err != nil {
				return err
			}
		} else {
			//TODO: Ignoring 4 switch cases:https://github.com/m-schen/matrixone/blob/0c480ca11b6302de26789f916a3e2faca7f79d47/pkg/sql/plan/function/builtin/binary/power.go#L36
			res := math.Pow(v1, v2)
			if err = rs.Append(res, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func TimeDiff[T types.Time | types.Datetime](ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) (err error) {
	p1 := vector.GenerateFunctionFixedTypeParameter[T](ivecs[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[T](ivecs[1])
	rs := vector.MustFunctionResult[types.Time](result)

	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			if err = rs.Append(0, true); err != nil {
				return err
			}
		} else {
			res, _ := timeDiff(v1, v2)
			if err = rs.Append(res, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func timeDiff[T types.Time | types.Datetime](v1, v2 T) (types.Time, error) {
	tmpTime := int64(v1 - v2)
	// different sign need to check overflow
	if (int64(v1)>>63)^(int64(v2)>>63) != 0 {
		if (tmpTime>>63)^(int64(v1)>>63) != 0 {
			// overflow
			isNeg := int64(v1) < 0
			return types.TimeFromClock(isNeg, types.MaxHourInTime, 59, 59, 0), nil
		}
	}

	// same sign don't need to check overflow
	time := types.Time(tmpTime)
	hour, _, _, _, isNeg := time.ClockFormat()
	if !types.ValidTime(uint64(hour), 0, 0) {
		return types.TimeFromClock(isNeg, types.MaxHourInTime, 59, 59, 0), nil
	}
	return time, nil
}

// TIMESTAMPDIFF

func TimestampDiff(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) (err error) {
	p1 := vector.GenerateFunctionStrParameter(ivecs[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[types.Datetime](ivecs[1])
	p3 := vector.GenerateFunctionFixedTypeParameter[types.Datetime](ivecs[2])
	rs := vector.MustFunctionResult[int64](result)

	//TODO: ignoring maxLen: Original code:https://github.com/m-schen/matrixone/blob/d2921c8ea5ecd9f38ad224159d3c62543894e807/pkg/sql/plan/function/builtin/multi/timestampdiff.go#L35
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		v2, null2 := p2.GetValue(i)
		v3, null3 := p3.GetValue(i)
		if null1 || null2 || null3 {
			if err = rs.Append(0, true); err != nil {
				return err
			}
		} else {
			res, _ := v3.DateTimeDiffWithUnit(functionUtil.QuickBytesToStr(v1), v2)
			if err = rs.Append(res, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func Replace(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) (err error) {
	p1 := vector.GenerateFunctionStrParameter(ivecs[0])
	p2 := vector.GenerateFunctionStrParameter(ivecs[1])
	p3 := vector.GenerateFunctionStrParameter(ivecs[2])
	rs := vector.MustFunctionResult[types.Varlena](result)

	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		v2, null2 := p2.GetStrValue(i)
		v3, null3 := p3.GetStrValue(i)

		// TODO: Ignoring maxLen. https://github.com/m-schen/matrixone/blob/5f91a015a3d7aae5721ba94b097db13c3dcbf294/pkg/sql/plan/function/builtin/multi/replace.go#L35
		if null1 || null2 || null3 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			//FIXME: Ignoring the complex logic. https://github.com/m-schen/matrixone/blob/5f91a015a3d7aae5721ba94b097db13c3dcbf294/pkg/vectorize/regular/regular_replace.go#L182
			// FIXME: This is wrong. I haven't handled Arrays. Hence it will fail.
			v1Str := functionUtil.QuickBytesToStr(v1)
			v2Str := functionUtil.QuickBytesToStr(v2)
			var res string
			if v2Str == "" {
				res = v1Str
			} else {
				res = strings.ReplaceAll(v1Str, v2Str, functionUtil.QuickBytesToStr(v3))
			}

			if err = rs.AppendBytes(functionUtil.QuickStrToBytes(res), false); err != nil {
				return err
			}
		}
	}
	return nil
}

//TRIM

func Trim(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {

	p1 := vector.GenerateFunctionStrParameter(ivecs[0])
	p2 := vector.GenerateFunctionStrParameter(ivecs[1])
	p3 := vector.GenerateFunctionStrParameter(ivecs[2])
	rs := vector.MustFunctionResult[types.Varlena](result)

	for i := uint64(0); i < uint64(length); i++ {

		v1, null1 := p1.GetStrValue(i)
		src, null2 := p2.GetStrValue(i)
		cut, null3 := p3.GetStrValue(i)

		if null1 || null2 || null3 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {

			v1Str := strings.ToLower(string(v1))
			var res string
			switch v1Str {
			case "both":
				res = trimBoth(string(cut), string(src))
			case "leading":
				res = trimLeading(string(cut), string(src))
			case "trailing":
				res = trimTrailing(string(cut), string(src))
			default:
				return moerr.NewNotSupported(proc.Ctx, "trim type %s", v1Str)
			}

			if err = rs.AppendBytes([]byte(res), false); err != nil {
				return err
			}
		}

	}
	return nil
}

func trimBoth(src, cuts string) string {
	if len(cuts) == 0 {
		return src
	}
	return trimLeading(trimTrailing(src, cuts), cuts)
}

func trimLeading(src, cuts string) string {
	if len(cuts) == 0 {
		return src
	}
	for strings.HasPrefix(src, cuts) {
		src = src[len(cuts):]
	}
	return src
}

func trimTrailing(src, cuts string) string {
	if len(cuts) == 0 {
		return src
	}
	for strings.HasSuffix(src, cuts) {
		src = src[:len(src)-len(cuts)]
	}
	return src
}

// JSON_EXTRACT
func jsonExtractCheckFn(overloads []overload, inputs []types.Type) checkResult {
	if len(inputs) > 1 {
		ts := make([]types.Type, 0, len(inputs))
		allMatch := true
		for _, input := range inputs {
			if input.Oid == types.T_json || input.Oid.IsMySQLString() {
				ts = append(ts, input)
			} else {
				if canCast, _ := fixedImplicitTypeCast(input, types.T_varchar); canCast {
					ts = append(ts, types.T_varchar.ToType())
					allMatch = false
				} else {
					return newCheckResultWithFailure(failedFunctionParametersWrong)
				}
			}
		}
		if allMatch {
			return newCheckResultWithSuccess(0)
		}
		return newCheckResultWithCast(0, ts)
	}
	return newCheckResultWithFailure(failedFunctionParametersWrong)
}

type computeFn func([]byte, []*bytejson.Path) (*bytejson.ByteJson, error)

func computeJson(json []byte, paths []*bytejson.Path) (*bytejson.ByteJson, error) {
	bj := types.DecodeJson(json)
	return bj.Query(paths), nil
}
func computeString(json []byte, paths []*bytejson.Path) (*bytejson.ByteJson, error) {
	bj, err := types.ParseSliceToByteJson(json)
	if err != nil {
		return nil, err
	}
	return bj.Query(paths), nil
}

func JsonExtract(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	jsonVec := parameters[0]
	var fn computeFn
	switch jsonVec.GetType().Oid {
	case types.T_json:
		fn = computeJson
	default:
		fn = computeString
	}
	jsonWrapper := vector.GenerateFunctionStrParameter(jsonVec)
	pathWrapers := make([]vector.FunctionParameterWrapper[types.Varlena], len(parameters)-1)
	rs := vector.MustFunctionResult[types.Varlena](result)
	paths := make([]*bytejson.Path, len(parameters)-1)
	for i := 0; i < len(parameters)-1; i++ {
		pathWrapers[i] = vector.GenerateFunctionStrParameter(parameters[i+1])
	}
	for i := uint64(0); i < uint64(length); i++ {
		jsonBytes, jIsNull := jsonWrapper.GetStrValue(i)
		if jIsNull {
			err := rs.AppendBytes(nil, true)
			if err != nil {
				return err
			}
			continue
		}
		skip := false
		for j := 0; j < len(parameters)-1; j++ {
			pathBytes, pIsNull := pathWrapers[j].GetStrValue(i)
			if pIsNull {
				skip = true
				break
			}
			p, err := types.ParseStringToPath(string(pathBytes))
			if err != nil {
				return err
			}
			paths[j] = &p
		}
		if skip {
			err := rs.AppendBytes(nil, true)
			if err != nil {
				return err
			}
			continue
		}
		out, err := fn(jsonBytes, paths)
		if err != nil {
			return err
		}
		if out.IsNull() {
			err := rs.AppendBytes(nil, true)
			if err != nil {
				return err
			}
			continue
		}
		dt, _ := out.Marshal()
		err = rs.AppendBytes(dt, false)
		if err != nil {
			return err
		}
	}
	return nil
}

// SPLIT PART

func SplitPart(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	p1 := vector.GenerateFunctionStrParameter(ivecs[0])
	p2 := vector.GenerateFunctionStrParameter(ivecs[1])
	p3 := vector.GenerateFunctionFixedTypeParameter[uint32](ivecs[2])
	rs := vector.MustFunctionResult[types.Varlena](result)

	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		v2, null2 := p2.GetStrValue(i)
		v3, null3 := p3.GetValue(i)
		if null1 || null2 || null3 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {

			if v3 == 0 {
				err = moerr.NewInvalidInput(proc.Ctx, "split_part: field contains non-positive integer")
				return
			}

			res, isNull := SplitSingle(string(v1), string(v2), v3)
			if isNull {
				if err = rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				if err = rs.AppendBytes([]byte(res), false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func SplitSingle(str, sep string, cnt uint32) (string, bool) {
	expectedLen := int(cnt + 1)
	strSlice := strings.SplitN(str, sep, expectedLen)
	if len(strSlice) < int(cnt) || strSlice[cnt-1] == "" {
		return "", true
	}
	return strSlice[cnt-1], false
}
