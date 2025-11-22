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
	"crypto/aes"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	fj "github.com/matrixorigin/matrixone/pkg/sql/plan/function/fault"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/vectorize/floor"
	"github.com/matrixorigin/matrixone/pkg/vectorize/format"
	"github.com/matrixorigin/matrixone/pkg/vectorize/instr"
	"github.com/matrixorigin/matrixone/pkg/vectorize/moarray"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

func doFaultPoint(
	proc *process.Process,
	sql string,
) (ret bool, err error) {

	var (
		sqlRet [][]interface{}

		cnCnt int
		tnCnt int = 1

		podResp []fj.PodResponse
	)

	if sqlRet, err = proc.GetSessionInfo().SqlHelper.ExecSqlWithCtx(proc.Ctx, sql); err != nil {
		return false, err
	}

	if err = json.Unmarshal(sqlRet[0][0].([]byte), &podResp); err != nil {
		return false, err
	}

	clusterservice.GetMOCluster(proc.GetService()).GetCNService(
		clusterservice.Selector{}, func(cn metadata.CNService) bool {
			if cn.GetWorkState() == metadata.WorkState_Working {
				cnCnt++
			}
			return true
		})

	for i := range podResp {
		ok := len(podResp[i].ErrorStr) == 0
		if podResp[i].PodType == "CN" {
			if ok {
				cnCnt--
			}
		} else if ok {
			tnCnt--
		}
	}

	if cnCnt <= 0 && tnCnt <= 0 {
		ret = true
	}

	logutil.Info("FaultPoint",
		zap.String("sql", sql),
		zap.String("sqlRet", string(sqlRet[0][0].([]byte))))

	return ret, nil
}

func AddFaultPoint(
	ivecs []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) (err error) {

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

	sql := fmt.Sprintf(
		"select fault_inject('all.', 'add_fault_point', '%s#%s#%s#%d#%s#%s');",
		name, freq, action, iarg, sarg, "false",
	)

	var (
		rs = vector.MustFunctionResult[bool](result)

		finalVal bool
	)

	// this call may come from UT
	if proc.GetSessionInfo() == nil || proc.GetSessionInfo().SqlHelper == nil {
		if err = fault.AddFaultPoint(proc.Ctx, string(name), string(freq), string(action), iarg, string(sarg), false); err != nil {
			return err
		}
		finalVal = true

	} else {
		if finalVal, err = doFaultPoint(proc, sql); err != nil {
			return err
		}
	}

	if err = rs.Append(finalVal, false); err != nil {
		return
	}

	return nil
}

type mathMultiT interface {
	constraints.Integer | constraints.Float | types.Decimal64 | types.Decimal128
}

type mathMultiFun[T mathMultiT] func(T, int64) T

func generalMathMulti[T mathMultiT](funcName string, ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int,
	cb mathMultiFun[T], selectList *FunctionSelectList) (err error) {
	digits := int64(0)
	if len(ivecs) > 1 {
		if ivecs[1].IsConstNull() || !ivecs[1].IsConst() {
			return moerr.NewInvalidArg(proc.Ctx, fmt.Sprintf("the second argument of the %s", funcName), "not const")
		}
		digits = vector.MustFixedColWithTypeCheck[int64](ivecs[1])[0]
	}

	return opUnaryFixedToFixed[T, T](ivecs, result, proc, length, func(x T) T {
		return cb(x, digits)
	}, selectList)
}

func CeilStr(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	digits := int64(0)
	if len(ivecs) > 1 {
		if !ivecs[1].IsConst() || ivecs[1].GetType().Oid != types.T_int64 {
			return moerr.NewInvalidArg(proc.Ctx, fmt.Sprintf("the second argument of the %s", "ceil"), "not const")
		}
		digits = vector.MustFixedColWithTypeCheck[int64](ivecs[1])[0]
	}

	return opUnaryStrToFixedWithErrorCheck[float64](ivecs, result, proc, length, func(v string) (float64, error) {
		floatVal, err1 := strconv.ParseFloat(v, 64)
		if err1 != nil {
			return 0, err1
		}
		return ceilFloat64(floatVal, digits), nil
	}, selectList)
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

func CeilUint64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	return generalMathMulti("ceil", ivecs, result, proc, length, ceilUint64, selectList)
}

func CeilInt64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	return generalMathMulti("ceil", ivecs, result, proc, length, ceilInt64, selectList)
}

func CeilFloat64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	return generalMathMulti("ceil", ivecs, result, proc, length, ceilFloat64, selectList)
}

func ceilDecimal64(x types.Decimal64, digits int64, scale int32, isConst bool) types.Decimal64 {
	if digits > 19 {
		digits = 19
	}
	if digits < -18 {
		digits = -18
	}
	return x.Ceil(scale, int32(digits), isConst)
}

func ceilDecimal128(x types.Decimal128, digits int64, scale int32, isConst bool) types.Decimal128 {
	if digits > 39 {
		digits = 19
	}
	if digits < -38 {
		digits = -38
	}
	return x.Ceil(scale, int32(digits), isConst)
}

func CeilDecimal64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	scale := ivecs[0].GetType().Scale
	if len(ivecs) > 1 {
		digit := vector.MustFixedColWithTypeCheck[int64](ivecs[1])
		if len(digit) > 0 && int32(digit[0]) <= scale-18 {
			return moerr.NewOutOfRangef(proc.Ctx, "decimal64", "ceil(decimal64(18,%v),%v)", scale, digit[0])
		}
	}
	cb := func(x types.Decimal64, digits int64) types.Decimal64 {
		return ceilDecimal64(x, digits, scale, result.GetResultVector().GetType().Scale != scale)
	}
	return generalMathMulti("ceil", ivecs, result, proc, length, cb, selectList)
}

func CeilDecimal128(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	scale := ivecs[0].GetType().Scale
	if len(ivecs) > 1 {
		digit := vector.MustFixedColWithTypeCheck[int64](ivecs[1])
		if len(digit) > 0 && int32(digit[0]) <= scale-38 {
			return moerr.NewOutOfRangef(proc.Ctx, "decimal128", "ceil(decimal128(38,%v),%v)", scale, digit[0])
		}
	}
	cb := func(x types.Decimal128, digits int64) types.Decimal128 {
		return ceilDecimal128(x, digits, scale, result.GetResultVector().GetType().Scale != scale)
	}
	return generalMathMulti("ceil", ivecs, result, proc, length, cb, selectList)
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

func FloorUInt64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	return generalMathMulti("floor", ivecs, result, proc, length, floorUint64, selectList)
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

func FloorInt64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	return generalMathMulti("floor", ivecs, result, proc, length, floorInt64, selectList)
}

func floorFloat64(x float64, digits int64) float64 {
	if digits == 0 {
		return math.Floor(x)
	}
	scale := math.Pow10(int(digits))
	value := x * scale
	return math.Floor(value) / scale
}

func FloorFloat64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	return generalMathMulti("floor", ivecs, result, proc, length, floorFloat64, selectList)
}

func floorDecimal64(x types.Decimal64, digits int64, scale int32, isConst bool) types.Decimal64 {
	if digits > 19 {
		digits = 19
	}
	if digits < -18 {
		digits = -18
	}
	return x.Floor(scale, int32(digits), isConst)
}

func floorDecimal128(x types.Decimal128, digits int64, scale int32, isConst bool) types.Decimal128 {
	if digits > 39 {
		digits = 39
	}
	if digits < -38 {
		digits = -38
	}
	return x.Floor(scale, int32(digits), isConst)
}

func FloorDecimal64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	scale := ivecs[0].GetType().Scale
	if len(ivecs) > 1 {
		digit := vector.MustFixedColWithTypeCheck[int64](ivecs[1])
		if len(digit) > 0 && int32(digit[0]) <= scale-18 {
			return moerr.NewOutOfRangef(proc.Ctx, "decimal64", "floor(decimal64(18,%v),%v)", scale, digit[0])
		}
	}
	cb := func(x types.Decimal64, digits int64) types.Decimal64 {
		return floorDecimal64(x, digits, scale, result.GetResultVector().GetType().Scale != scale)
	}

	return generalMathMulti("floor", ivecs, result, proc, length, cb, selectList)
}

func FloorDecimal128(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	scale := ivecs[0].GetType().Scale
	if len(ivecs) > 1 {
		digit := vector.MustFixedColWithTypeCheck[int64](ivecs[1])
		if len(digit) > 0 && int32(digit[0]) <= scale-38 {
			return moerr.NewOutOfRangef(proc.Ctx, "decimal128", "floor(decimal128(38,%v),%v)", scale, digit[0])
		}
	}
	cb := func(x types.Decimal128, digits int64) types.Decimal128 {
		return floorDecimal128(x, digits, scale, result.GetResultVector().GetType().Scale != scale)
	}

	return generalMathMulti("floor", ivecs, result, proc, length, cb, selectList)
}

func FloorStr(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	digits := int64(0)
	if len(ivecs) > 1 {
		if !ivecs[1].IsConst() || ivecs[1].GetType().Oid != types.T_int64 {
			return moerr.NewInvalidArg(proc.Ctx, fmt.Sprintf("the second argument of the %s", "ceil"), "not const")
		}
		digits = vector.MustFixedColWithTypeCheck[int64](ivecs[1])[0]
	}

	rs := vector.MustFunctionResult[float64](result)
	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	rsVec := rs.GetResultVector()
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

	if ivec.WithAnyNullValue() || rsAnyNull {
		nulls.Or(rsNull, ivecs[0].GetNulls(), rsNull)
		for i := uint64(0); i < uint64(length); i++ {
			if rsNull.Contains(i) {
				if err = rs.Append(0, true); err != nil {
					return err
				}
			}
			v, _ := ivec.GetStrValue(i)
			floatVal, err := strconv.ParseFloat(string(v), 64)
			if err != nil {
				return err
			}
			if err = rs.Append(floorFloat64(floatVal, digits), false); err != nil {
				return err
			}
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		v, _ := ivec.GetStrValue(i)
		floatVal, err := strconv.ParseFloat(string(v), 64)
		if err != nil {
			return err
		}
		if err = rs.Append(floorFloat64(floatVal, digits), false); err != nil {
			return err
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

func RoundUint64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	return generalMathMulti("round", ivecs, result, proc, length, roundUint64, selectList)
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

func RoundInt64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	return generalMathMulti("round", ivecs, result, proc, length, roundInt64, selectList)
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

func RoundFloat64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	return generalMathMulti("round", ivecs, result, proc, length, roundFloat64, selectList)
}

// TRUNCATE function implementations
// TRUNCATE truncates a number to D decimal places without rounding
func truncateUint64(x uint64, digits int64) uint64 {
	switch {
	case digits >= 0:
		return x
	case digits > -MaxUint64digits:
		scale := ScaleTable[-digits]
		x = x / scale * scale // truncate without rounding
	case digits <= -MaxUint64digits:
		x = 0
	}
	return x
}

func TruncateUint64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	return generalMathMulti("truncate", ivecs, result, proc, length, truncateUint64, selectList)
}

func truncateInt64(x int64, digits int64) int64 {
	switch {
	case digits >= 0:
		return x
	case digits > -MaxInt64digits:
		scale := int64(ScaleTable[-digits])
		// Truncate towards zero (just divide and multiply, no rounding)
		x = x / scale * scale
	case digits <= -MaxInt64digits:
		x = 0
	}
	return x
}

func TruncateInt64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	return generalMathMulti("truncate", ivecs, result, proc, length, truncateInt64, selectList)
}

func truncateFloat64(x float64, digits int64) float64 {
	if digits == 0 {
		x = math.Trunc(x)
	} else if digits >= 308 { // the range of float64
		// No truncation needed
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
			// Truncate to D decimal places: multiply, truncate, divide
			var value_mul_tmp = x * tmp
			x = math.Trunc(value_mul_tmp) / tmp
		} else {
			// Truncate to -D digits before decimal point
			var value_div_tmp = x / tmp
			x = math.Trunc(value_div_tmp) * tmp
		}
	}
	return x
}

func TruncateFloat64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	return generalMathMulti("truncate", ivecs, result, proc, length, truncateFloat64, selectList)
}

func truncateDecimal64(x types.Decimal64, digits int64, scale int32, isConst bool) types.Decimal64 {
	if digits > 19 {
		digits = 19
	}
	if digits < -18 {
		digits = -18
	}
	// Truncate to D decimal places (towards zero)
	// Similar to Floor but works correctly for both positive and negative
	if int32(digits) >= scale {
		return x
	}
	k := scale - int32(digits)
	if k > 18 {
		k = 18
	}
	// Remove the fractional part beyond D digits
	y, _, _ := x.Mod(types.Decimal64(1), k, 0)
	x, _ = x.Sub64(y)
	if isConst {
		if int32(digits) < 0 {
			k = scale
		}
		x, _ = x.Scale(-k)
	}
	return x
}

func TruncateDecimal64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	scale := ivecs[0].GetType().Scale
	cb := func(x types.Decimal64, digits int64) types.Decimal64 {
		return truncateDecimal64(x, digits, scale, result.GetResultVector().GetType().Scale != scale)
	}
	return generalMathMulti("truncate", ivecs, result, proc, length, cb, selectList)
}

func truncateDecimal128(x types.Decimal128, digits int64, scale int32, isConst bool) types.Decimal128 {
	if digits > 39 {
		digits = 39
	}
	if digits < -38 {
		digits = -38
	}
	// Truncate to D decimal places (towards zero)
	if int32(digits) >= scale {
		return x
	}
	k := scale - int32(digits)
	if k > 38 {
		k = 38
	}
	// Remove the fractional part beyond D digits
	y, _, _ := x.Mod(types.Decimal128{B0_63: 1, B64_127: 0}, k, 0)
	x, _ = x.Sub128(y)
	if isConst {
		if int32(digits) < 0 {
			k = scale
		}
		x, _ = x.Scale(-k)
	}
	return x
}

func TruncateDecimal128(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	scale := ivecs[0].GetType().Scale
	cb := func(x types.Decimal128, digits int64) types.Decimal128 {
		return truncateDecimal128(x, digits, scale, result.GetResultVector().GetType().Scale != scale)
	}
	return generalMathMulti("truncate", ivecs, result, proc, length, cb, selectList)
}

func roundDecimal64(x types.Decimal64, digits int64, scale int32, isConst bool) types.Decimal64 {
	if digits > 19 {
		digits = 19
	}
	if digits < -18 {
		digits = -18
	}
	return x.Round(scale, int32(digits), isConst)
}

func roundDecimal128(x types.Decimal128, digits int64, scale int32, isConst bool) types.Decimal128 {
	if digits > 39 {
		digits = 39
	}
	if digits < -38 {
		digits = -38
	}
	return x.Round(scale, int32(digits), isConst)
}

func RoundDecimal64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	scale := ivecs[0].GetType().Scale
	cb := func(x types.Decimal64, digits int64) types.Decimal64 {
		return roundDecimal64(x, digits, scale, result.GetResultVector().GetType().Scale != scale)
	}
	return generalMathMulti("round", ivecs, result, proc, length, cb, selectList)
}

func RoundDecimal128(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	scale := ivecs[0].GetType().Scale
	cb := func(x types.Decimal128, digits int64) types.Decimal128 {
		return roundDecimal128(x, digits, scale, result.GetResultVector().GetType().Scale != scale)
	}
	return generalMathMulti("round", ivecs, result, proc, length, cb, selectList)
}

type NormalType interface {
	bool |
		constraints.Float |
		types.Date |
		types.Datetime |
		types.Decimal64 |
		types.Decimal128 |
		types.Timestamp |
		types.Uuid |
		constraints.Integer
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
				SetTargetScaleFromSource(&inputs[i], &castType[i])
			}
		}

		if minOid.IsDecimal() || minOid.IsDateRelate() {
			setMaxScaleForAll(castType)
		}
		return newCheckResultWithCast(minIndex, castType)
	}
	return newCheckResultWithFailure(failedFunctionParametersWrong)
}

func CoalesceGeneral[T NormalType](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
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

func CoalesceStr(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) (err error) {
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

func ConcatWs(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) (err error) {
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

func TSToTimestamp(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Timestamp](result)
	from := vector.GenerateFunctionFixedTypeParameter[types.TS](ivecs[0])
	scale := int32(6)
	if len(ivecs) == 2 && !ivecs[1].IsConstNull() {
		scale = int32(vector.MustFixedColWithTypeCheck[int64](ivecs[1])[0])
	}
	rs.TempSetType(types.New(types.T_timestamp, 0, scale))
	for i := 0; i < length; i++ {
		tsVal, null := from.GetValue(uint64(i))
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		physical := tsVal.Physical()
		seconds := int64(physical / 1e9)
		nanos := int64(physical % 1e9)
		t := time.Unix(seconds, nanos).UTC()
		timeStr := t.Format("2006-01-02 15:04:05.999999")

		zone := time.Local
		if proc != nil {
			zone = proc.GetSessionInfo().TimeZone
		}
		val, err := types.ParseTimestamp(zone, timeStr, scale)
		if err != nil {
			return err
		}

		if err = rs.Append(val, false); err != nil {
			return err
		}

	}

	return nil
}

func ConvertTz(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	dates := vector.GenerateFunctionFixedTypeParameter[types.Datetime](ivecs[0])
	fromTzs := vector.GenerateFunctionStrParameter(ivecs[1])
	toTzs := vector.GenerateFunctionStrParameter(ivecs[2])
	rs := vector.MustFunctionResult[types.Varlena](result)

	var fromLoc, toLoc *time.Location
	if ivecs[1].IsConst() && !ivecs[1].IsConstNull() {
		fromTz, _ := fromTzs.GetStrValue(0)
		fromLoc = convertTimezone(string(fromTz))
	}
	if ivecs[2].IsConst() && !ivecs[2].IsConstNull() {
		toTz, _ := toTzs.GetStrValue(0)
		toLoc = convertTimezone(string(toTz))
	}

	for i := uint64(0); i < uint64(length); i++ {
		date, null1 := dates.GetValue(i)
		fromTz, null2 := fromTzs.GetStrValue(i)
		toTz, null3 := toTzs.GetStrValue(i)

		if null1 || null2 || null3 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
			return nil
		} else if len(fromTz) == 0 || len(toTz) == 0 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
			return nil
		} else {
			if !ivecs[1].IsConst() {
				fromLoc = convertTimezone(string(fromTz))
			}
			if !ivecs[2].IsConst() {
				toLoc = convertTimezone(string(toTz))
			}
			if fromLoc == nil || toLoc == nil {
				if err = rs.AppendBytes(nil, true); err != nil {
					return err
				}
				return nil
			}
			maxStartTime := time.Date(9999, 12, 31, 23, 59, 59, 0, fromLoc)
			maxEndTime := time.Date(9999, 12, 31, 23, 59, 59, 0, toLoc)
			minStartTime := time.Date(0001, 1, 1, 0, 0, 0, 0, fromLoc)
			minEndTime := time.Date(0001, 1, 1, 0, 0, 0, 0, toLoc)
			startTime := date.ConvertToGoTime(fromLoc)
			if startTime.After(maxStartTime) { // if startTime > maxTime, return maxTime
				if err = rs.AppendBytes([]byte(maxStartTime.Format(time.DateTime)), false); err != nil {
					return err
				}
			} else if startTime.Before(minStartTime) { // if startTime < minTime, return minTime
				if err = rs.AppendBytes([]byte(minStartTime.Format(time.DateTime)), false); err != nil {
					return err
				}
			} else { // if minTime <= startTime <= maxTime
				endTime := startTime.In(toLoc)
				if endTime.After(maxEndTime) || endTime.Before(minEndTime) { // if endTime > maxTime or endTime < maxTime, return startTime
					if err = rs.AppendBytes([]byte(startTime.Format(time.DateTime)), false); err != nil {
						return err
					}
				} else {
					if err = rs.AppendBytes([]byte(endTime.Format(time.DateTime)), false); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

func convertTimezone(tz string) *time.Location {
	loc, err := time.LoadLocation(tz)
	if err != nil && tz[0] != '+' && tz[0] != '-' {
		return nil
	}
	// convert from timezone offset to location
	if tz[0] == '+' || tz[0] == '-' {
		parts := strings.Split(tz, ":")
		if len(parts) != 2 {
			return nil
		}
		hours, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil
		} else if hours < -13 || hours > 14 { // timezone should be in [-13, 14]
			return nil
		}
		minutes, err := strconv.Atoi(parts[1])
		if tz[0] == '-' {
			minutes = -minutes
		}
		if err != nil {
			return nil
		}

		offset := time.Duration(hours)*time.Hour + time.Duration(minutes)*time.Minute
		loc = time.FixedZone("GMT", int(offset.Seconds()))
	}

	return loc
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

func Truncate(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	diff, _ := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1]).GetValue(0)
	unit, _ := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[2]).GetValue(0)
	num, err := getIntervalNum(diff, unit, proc)
	if err != nil {
		return err
	}
	t := types.Datetime(num)

	ivec := vector.GenerateFunctionFixedTypeParameter[types.Datetime](ivecs[0])
	rs := vector.MustFunctionResult[types.Datetime](result)

	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			return moerr.NewNotSupported(proc.Ctx, "now args of MO_WIN_TRUNCATE can not be NULL")
		}
		if err = rs.Append(v-v%t, false); err != nil {
			return err
		}
	}

	return nil
}

func getIntervalNum(diff, unit int64, proc *process.Process) (int64, error) {
	var num int64
	iTyp := types.IntervalType(unit)
	err := types.JudgeIntervalNumOverflow(diff, iTyp)
	if err != nil {
		return num, err
	}
	switch iTyp {
	case types.Second:
		num = diff * types.MicroSecsPerSec
	case types.Minute:
		num = diff * types.SecsPerMinute * types.MicroSecsPerSec
	case types.Hour:
		num = diff * types.SecsPerHour * types.MicroSecsPerSec
	case types.Day:
		num = diff * types.SecsPerDay * types.MicroSecsPerSec
	default:
		return num, moerr.NewNotSupported(proc.Ctx, "now support SECOND, MINUTE, HOUR, DAY as the time unit")
	}
	return num, nil
}

func getSecondNum(diff, unit int64, proc *process.Process) (int64, error) {
	var num int64
	iTyp := types.IntervalType(unit)
	err := types.JudgeIntervalNumOverflow(diff, iTyp)
	if err != nil {
		return num, err
	}
	switch iTyp {
	case types.Second:
		num = diff
	case types.Minute:
		num = diff * types.SecsPerMinute
	case types.Hour:
		num = diff * types.SecsPerHour
	case types.Day:
		num = diff * types.SecsPerDay
	default:
		return num, moerr.NewNotSupported(proc.Ctx, "now support SECOND, MINUTE, HOUR, DAY as the time unit")
	}
	return num, nil
}

func Divisor(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	diff1, _ := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[0]).GetValue(0)
	unit1, _ := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1]).GetValue(0)
	num1, err := getSecondNum(diff1, unit1, proc)
	if err != nil {
		return err
	}
	diff2, _ := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[2]).GetValue(0)
	unit2, _ := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[3]).GetValue(0)
	num2, err := getSecondNum(diff2, unit2, proc)
	if err != nil {
		return err
	}

	if num2 > num1 {
		return moerr.NewInvalidInput(proc.Ctx, "sliding value should be smaller than the interval value")
	}

	rs := vector.MustFunctionResult[int64](result)

	gcd := func(a, b int64) int64 {
		for b != 0 {
			a, b = b, a%b
		}
		return a
	}
	if err = rs.Append(gcd(num1, num2), false); err != nil {
		return err
	}
	return nil
}

func DateAdd(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	unit, _ := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[2]).GetValue(0)
	iTyp := types.IntervalType(unit)

	return opBinaryFixedFixedToFixedWithErrorCheck[types.Date, int64, types.Date](ivecs, result, proc, length, func(v1 types.Date, v2 int64) (types.Date, error) {
		return doDateAdd(v1, v2, iTyp)
	}, selectList)
}

func DatetimeAdd(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
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
	}, selectList)
}

func DateStringAdd(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	unit, _ := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[2]).GetValue(0)
	iTyp := types.IntervalType(unit)
	rs := vector.MustFunctionResult[types.Datetime](result)
	rs.TempSetType(types.New(types.T_datetime, 0, 6))

	return opBinaryStrFixedToFixedWithErrorCheck[int64, types.Datetime](ivecs, result, proc, length, func(v1 string, v2 int64) (types.Datetime, error) {
		return doDateStringAdd(v1, v2, iTyp)
	}, selectList)
}

func TimestampAdd(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
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
		return doTimestampAdd(proc.GetSessionInfo().TimeZone, v1, v2, iTyp)
	}, selectList)
}

func TimeAdd(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
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
	}, selectList)
}

// TimestampAddDate: TIMESTAMPADD(unit, interval, date)
// Parameters: ivecs[0] = unit (string), ivecs[1] = interval (int64), ivecs[2] = date (Date)
func TimestampAddDate(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	if !ivecs[0].IsConst() {
		return moerr.NewInvalidArg(proc.Ctx, "timestampadd unit", "not constant")
	}

	unitStr, _ := vector.GenerateFunctionStrParameter(ivecs[0]).GetStrValue(0)
	iTyp, err := types.IntervalTypeOf(functionUtil.QuickBytesToStr(unitStr))
	if err != nil {
		return err
	}

	rs := vector.MustFunctionResult[types.Date](result)
	dates := vector.GenerateFunctionFixedTypeParameter[types.Date](ivecs[2])
	intervals := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])

	for i := uint64(0); i < uint64(length); i++ {
		date, null1 := dates.GetValue(i)
		interval, null2 := intervals.GetValue(i)
		if null1 || null2 {
			if err = rs.Append(types.Date(0), true); err != nil {
				return err
			}
		} else {
			resultDate, err := doDateAdd(date, interval, iTyp)
			if err != nil {
				return err
			}
			if err = rs.Append(resultDate, false); err != nil {
				return err
			}
		}
	}
	return nil
}

// TimestampAddDatetime: TIMESTAMPADD(unit, interval, datetime)
// Parameters: ivecs[0] = unit (string), ivecs[1] = interval (int64), ivecs[2] = datetime (Datetime)
func TimestampAddDatetime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	if !ivecs[0].IsConst() {
		return moerr.NewInvalidArg(proc.Ctx, "timestampadd unit", "not constant")
	}

	unitStr, _ := vector.GenerateFunctionStrParameter(ivecs[0]).GetStrValue(0)
	iTyp, err := types.IntervalTypeOf(functionUtil.QuickBytesToStr(unitStr))
	if err != nil {
		return err
	}

	scale := ivecs[2].GetType().Scale
	if iTyp == types.MicroSecond {
		scale = 6
	}
	rs := vector.MustFunctionResult[types.Datetime](result)
	rs.TempSetType(types.New(types.T_datetime, 0, scale))

	datetimes := vector.GenerateFunctionFixedTypeParameter[types.Datetime](ivecs[2])
	intervals := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])

	for i := uint64(0); i < uint64(length); i++ {
		dt, null1 := datetimes.GetValue(i)
		interval, null2 := intervals.GetValue(i)
		if null1 || null2 {
			if err = rs.Append(types.Datetime(0), true); err != nil {
				return err
			}
		} else {
			resultDt, err := doDatetimeAdd(dt, interval, iTyp)
			if err != nil {
				return err
			}
			if err = rs.Append(resultDt, false); err != nil {
				return err
			}
		}
	}
	return nil
}

// TimestampAddTimestamp: TIMESTAMPADD(unit, interval, timestamp)
// Parameters: ivecs[0] = unit (string), ivecs[1] = interval (int64), ivecs[2] = timestamp (Timestamp)
func TimestampAddTimestamp(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	if !ivecs[0].IsConst() {
		return moerr.NewInvalidArg(proc.Ctx, "timestampadd unit", "not constant")
	}

	unitStr, _ := vector.GenerateFunctionStrParameter(ivecs[0]).GetStrValue(0)
	iTyp, err := types.IntervalTypeOf(functionUtil.QuickBytesToStr(unitStr))
	if err != nil {
		return err
	}

	scale := ivecs[2].GetType().Scale
	if iTyp == types.MicroSecond {
		scale = 6
	}
	rs := vector.MustFunctionResult[types.Timestamp](result)
	rs.TempSetType(types.New(types.T_timestamp, 0, scale))

	timestamps := vector.GenerateFunctionFixedTypeParameter[types.Timestamp](ivecs[2])
	intervals := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])
	loc := proc.GetSessionInfo().TimeZone
	if loc == nil {
		loc = time.Local
	}

	for i := uint64(0); i < uint64(length); i++ {
		ts, null1 := timestamps.GetValue(i)
		interval, null2 := intervals.GetValue(i)
		if null1 || null2 {
			if err = rs.Append(types.Timestamp(0), true); err != nil {
				return err
			}
		} else {
			resultTs, err := doTimestampAdd(loc, ts, interval, iTyp)
			if err != nil {
				return err
			}
			if err = rs.Append(resultTs, false); err != nil {
				return err
			}
		}
	}
	return nil
}

// TimestampAddString: TIMESTAMPADD(unit, interval, datetime_string)
// Parameters: ivecs[0] = unit (string), ivecs[1] = interval (int64), ivecs[2] = datetime_string (string)
func TimestampAddString(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	if !ivecs[0].IsConst() {
		return moerr.NewInvalidArg(proc.Ctx, "timestampadd unit", "not constant")
	}

	unitStr, _ := vector.GenerateFunctionStrParameter(ivecs[0]).GetStrValue(0)
	iTyp, err := types.IntervalTypeOf(functionUtil.QuickBytesToStr(unitStr))
	if err != nil {
		return err
	}

	rs := vector.MustFunctionResult[types.Datetime](result)
	rs.TempSetType(types.New(types.T_datetime, 0, 6))

	dateStrings := vector.GenerateFunctionStrParameter(ivecs[2])
	intervals := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])

	for i := uint64(0); i < uint64(length); i++ {
		dateStr, null1 := dateStrings.GetStrValue(i)
		interval, null2 := intervals.GetValue(i)
		if null1 || null2 {
			if err = rs.Append(types.Datetime(0), true); err != nil {
				return err
			}
		} else {
			resultDt, err := doDateStringAdd(functionUtil.QuickBytesToStr(dateStr), interval, iTyp)
			if err != nil {
				return err
			}
			if err = rs.Append(resultDt, false); err != nil {
				return err
			}
		}
	}
	return nil
}

// Conv: CONV(N, from_base, to_base) - Converts numbers between different number bases
// N can be a string or numeric type
// from_base and to_base must be integers between 2 and 36
// Returns a string representation of N in to_base
func Conv(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	rs := vector.MustFunctionResult[types.Varlena](result)

	// Get base parameters (must be constants)
	if !ivecs[1].IsConst() || !ivecs[2].IsConst() {
		return moerr.NewInvalidArg(proc.Ctx, "conv bases", "not constant")
	}

	fromBaseVec := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])
	toBaseVec := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[2])

	fromBase, null1 := fromBaseVec.GetValue(0)
	toBase, null2 := toBaseVec.GetValue(0)

	if null1 || null2 {
		// If bases are NULL, return NULL for all rows
		for i := uint64(0); i < uint64(length); i++ {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		}
		return nil
	}

	// Validate base ranges (2-36)
	if fromBase < 2 || fromBase > 36 || toBase < 2 || toBase > 36 {
		return moerr.NewInvalidInputf(proc.Ctx, "conv base must be between 2 and 36, got from_base=%d, to_base=%d", fromBase, toBase)
	}

	// Handle different input types for N
	// MySQL behavior:
	// - For numeric types: always treat as base 10, regardless of from_base
	// - For string types: parse according to from_base
	inputType := ivecs[0].GetType()
	switch inputType.Oid {
	case types.T_char, types.T_varchar, types.T_text:
		return convString(ivecs[0], fromBase, toBase, rs, length, selectList)
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
		// Numeric types are always treated as base 10
		return convInt64Direct(ivecs[0], toBase, rs, length, selectList)
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		// Numeric types are always treated as base 10
		return convUint64Direct(ivecs[0], toBase, rs, length, selectList)
	case types.T_float32, types.T_float64:
		// Numeric types are always treated as base 10
		return convFloat64Direct(ivecs[0], toBase, rs, length, selectList)
	default:
		// For other types, try to convert to string first
		return convString(ivecs[0], fromBase, toBase, rs, length, selectList)
	}
}

func convString(nVec *vector.Vector, fromBase, toBase int64, rs *vector.FunctionResult[types.Varlena], length int, selectList *FunctionSelectList) error {
	nParam := vector.GenerateFunctionStrParameter(nVec)

	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && selectList.Contains(i) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		nStr, null := nParam.GetStrValue(i)
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		// Parse the number from from_base
		// strconv.ParseInt can handle bases 2-36
		val, err := strconv.ParseInt(strings.TrimSpace(string(nStr)), int(fromBase), 64)
		if err != nil {
			// If parsing as signed int fails, try unsigned
			uval, uerr := strconv.ParseUint(strings.TrimSpace(string(nStr)), int(fromBase), 64)
			if uerr != nil {
				// Return NULL if parsing fails (MySQL behavior)
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
				continue
			}
			// Convert unsigned to string in to_base
			result := strconv.FormatUint(uval, int(toBase))
			if err := rs.AppendBytes([]byte(result), false); err != nil {
				return err
			}
		} else {
			// Convert signed int to string in to_base
			// For negative numbers, MySQL returns the unsigned representation
			if val < 0 {
				uval := uint64(val)
				result := strconv.FormatUint(uval, int(toBase))
				if err := rs.AppendBytes([]byte(result), false); err != nil {
					return err
				}
			} else {
				result := strconv.FormatInt(val, int(toBase))
				if err := rs.AppendBytes([]byte(result), false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func convInt64Direct(nVec *vector.Vector, toBase int64, rs *vector.FunctionResult[types.Varlena], length int, selectList *FunctionSelectList) error {
	nParam := vector.GenerateFunctionFixedTypeParameter[int64](nVec)

	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && selectList.Contains(i) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		n, null := nParam.GetValue(i)
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		// Convert int64 to string in to_base
		// For negative numbers, MySQL returns the unsigned representation
		var result string
		if n < 0 {
			uval := uint64(n)
			result = strconv.FormatUint(uval, int(toBase))
		} else {
			result = strconv.FormatInt(n, int(toBase))
		}
		if err := rs.AppendBytes([]byte(result), false); err != nil {
			return err
		}
	}
	return nil
}

func convUint64Direct(nVec *vector.Vector, toBase int64, rs *vector.FunctionResult[types.Varlena], length int, selectList *FunctionSelectList) error {
	nParam := vector.GenerateFunctionFixedTypeParameter[uint64](nVec)

	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && selectList.Contains(i) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		n, null := nParam.GetValue(i)
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		// Convert uint64 to string in to_base
		result := strconv.FormatUint(n, int(toBase))
		if err := rs.AppendBytes([]byte(result), false); err != nil {
			return err
		}
	}
	return nil
}

func convFloat64Direct(nVec *vector.Vector, toBase int64, rs *vector.FunctionResult[types.Varlena], length int, selectList *FunctionSelectList) error {
	nParam := vector.GenerateFunctionFixedTypeParameter[float64](nVec)

	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && selectList.Contains(i) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		n, null := nParam.GetValue(i)
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		// Convert float64 to int64 first (truncate), then to string in to_base
		val := int64(n)
		var result string
		if val < 0 {
			uval := uint64(val)
			result = strconv.FormatUint(uval, int(toBase))
		} else {
			result = strconv.FormatInt(val, int(toBase))
		}
		if err := rs.AppendBytes([]byte(result), false); err != nil {
			return err
		}
	}
	return nil
}

// AddTime: ADDTIME(expr1, expr2) - Adds expr2 (time) to expr1 (time or datetime)
// expr1 can be TIME, DATETIME, or TIMESTAMP
// expr2 is a TIME expression (can include days like '1 1:1:1.000002')
// Returns TIME if expr1 is TIME, DATETIME if expr1 is DATETIME/TIMESTAMP
func AddTime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	// Determine result type based on first argument
	inputType := ivecs[0].GetType()
	switch inputType.Oid {
	case types.T_time:
		return addTimeToTime(ivecs, result, proc, length, selectList)
	case types.T_datetime:
		return addTimeToDatetime(ivecs, result, proc, length, selectList)
	case types.T_timestamp:
		return addTimeToTimestamp(ivecs, result, proc, length, selectList)
	case types.T_char, types.T_varchar, types.T_text:
		// Try to parse as datetime first, then time
		return addTimeToString(ivecs, result, proc, length, selectList)
	default:
		return moerr.NewInvalidInputf(proc.Ctx, "addtime: unsupported type %v", inputType.Oid)
	}
}

func addTimeToTime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	times1 := vector.GenerateFunctionFixedTypeParameter[types.Time](ivecs[0])
	time2Param := vector.GenerateFunctionStrParameter(ivecs[1])
	rs := vector.MustFunctionResult[types.Time](result)

	// Determine scale from input
	scale := int32(ivecs[0].GetType().Scale)
	if scale2 := int32(ivecs[1].GetType().Scale); scale2 > scale {
		scale = scale2
	}
	rs.TempSetType(types.New(types.T_time, 0, scale))

	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && selectList.Contains(i) {
			if err := rs.Append(types.Time(0), true); err != nil {
				return err
			}
			continue
		}

		time1, null1 := times1.GetValue(i)
		time2Str, null2 := time2Param.GetStrValue(i)

		if null1 || null2 {
			if err := rs.Append(types.Time(0), true); err != nil {
				return err
			}
			continue
		}

		// Parse time2 string
		time2, err := types.ParseTime(functionUtil.QuickBytesToStr(time2Str), scale)
		if err != nil {
			if err := rs.Append(types.Time(0), true); err != nil {
				return err
			}
			continue
		}

		// Add time2 to time1 (both are in microseconds)
		resultTime := types.Time(int64(time1) + int64(time2))

		// Validate result
		h := resultTime.Hour()
		if h < 0 {
			h = -h
		}
		if !types.ValidTime(uint64(h), 0, 0) {
			if err := rs.Append(types.Time(0), true); err != nil {
				return err
			}
			continue
		}

		if err := rs.Append(resultTime, false); err != nil {
			return err
		}
	}
	return nil
}

func addTimeToDatetime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	datetimes := vector.GenerateFunctionFixedTypeParameter[types.Datetime](ivecs[0])
	time2Param := vector.GenerateFunctionStrParameter(ivecs[1])
	rs := vector.MustFunctionResult[types.Datetime](result)

	// Determine scale from input
	scale := int32(ivecs[0].GetType().Scale)
	if scale2 := int32(ivecs[1].GetType().Scale); scale2 > scale {
		scale = scale2
	}
	rs.TempSetType(types.New(types.T_datetime, 0, scale))

	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && selectList.Contains(i) {
			if err := rs.Append(types.Datetime(0), true); err != nil {
				return err
			}
			continue
		}

		dt, null1 := datetimes.GetValue(i)
		time2Str, null2 := time2Param.GetStrValue(i)

		if null1 || null2 {
			if err := rs.Append(types.Datetime(0), true); err != nil {
				return err
			}
			continue
		}

		// Parse time2 string
		time2, err := types.ParseTime(functionUtil.QuickBytesToStr(time2Str), scale)
		if err != nil {
			if err := rs.Append(types.Datetime(0), true); err != nil {
				return err
			}
			continue
		}

		// Add time2 to datetime (both are in microseconds)
		resultDt := types.Datetime(int64(dt) + int64(time2))

		if err := rs.Append(resultDt, false); err != nil {
			return err
		}
	}
	return nil
}

func addTimeToTimestamp(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	timestamps := vector.GenerateFunctionFixedTypeParameter[types.Timestamp](ivecs[0])
	time2Param := vector.GenerateFunctionStrParameter(ivecs[1])
	rs := vector.MustFunctionResult[types.Timestamp](result)
	loc := proc.GetSessionInfo().TimeZone

	// Determine scale from input
	scale := int32(ivecs[0].GetType().Scale)
	if scale2 := int32(ivecs[1].GetType().Scale); scale2 > scale {
		scale = scale2
	}
	rs.TempSetType(types.New(types.T_timestamp, 0, scale))

	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && selectList.Contains(i) {
			if err := rs.Append(types.Timestamp(0), true); err != nil {
				return err
			}
			continue
		}

		ts, null1 := timestamps.GetValue(i)
		time2Str, null2 := time2Param.GetStrValue(i)

		if null1 || null2 {
			if err := rs.Append(types.Timestamp(0), true); err != nil {
				return err
			}
			continue
		}

		// Parse time2 string
		time2, err := types.ParseTime(functionUtil.QuickBytesToStr(time2Str), scale)
		if err != nil {
			if err := rs.Append(types.Timestamp(0), true); err != nil {
				return err
			}
			continue
		}

		// Convert timestamp to datetime, add time, convert back
		dt := ts.ToDatetime(loc)
		resultDt := types.Datetime(int64(dt) + int64(time2))
		resultTs := resultDt.ToTimestamp(loc)

		if err := rs.Append(resultTs, false); err != nil {
			return err
		}
	}
	return nil
}

func addTimeToString(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	// Try to parse as datetime first
	dtParam := vector.GenerateFunctionStrParameter(ivecs[0])
	time2Param := vector.GenerateFunctionStrParameter(ivecs[1])
	rs := vector.MustFunctionResult[types.Datetime](result)

	scale := int32(6) // Use max scale for string inputs
	rs.TempSetType(types.New(types.T_datetime, 0, scale))

	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && selectList.Contains(i) {
			if err := rs.Append(types.Datetime(0), true); err != nil {
				return err
			}
			continue
		}

		dtStr, null1 := dtParam.GetStrValue(i)
		time2Str, null2 := time2Param.GetStrValue(i)

		if null1 || null2 {
			if err := rs.Append(types.Datetime(0), true); err != nil {
				return err
			}
			continue
		}

		// Try to parse as datetime
		dt, err := types.ParseDatetime(functionUtil.QuickBytesToStr(dtStr), scale)
		if err != nil {
			// If parsing as datetime fails, try as time
			time1, err2 := types.ParseTime(functionUtil.QuickBytesToStr(dtStr), scale)
			if err2 != nil {
				if err := rs.Append(types.Datetime(0), true); err != nil {
					return err
				}
				continue
			}
			// Convert time to datetime (using today's date)
			dt = time1.ToDatetime(scale)
		}

		// Parse time2 string
		time2, err := types.ParseTime(functionUtil.QuickBytesToStr(time2Str), scale)
		if err != nil {
			if err := rs.Append(types.Datetime(0), true); err != nil {
				return err
			}
			continue
		}

		// Add time2 to datetime
		resultDt := types.Datetime(int64(dt) + int64(time2))

		if err := rs.Append(resultDt, false); err != nil {
			return err
		}
	}
	return nil
}

// SubTime: SUBTIME(expr1, expr2) - Returns expr1 - expr2 expressed as a time value.
// expr1 can be TIME, DATETIME, or TIMESTAMP
// expr2 is a TIME expression (can include days like '1 1:1:1.000002')
// Returns TIME if expr1 is TIME, DATETIME if expr1 is DATETIME/TIMESTAMP
func SubTime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	// Determine result type based on first argument
	inputType := ivecs[0].GetType()
	switch inputType.Oid {
	case types.T_time:
		return subTimeFromTime(ivecs, result, proc, length, selectList)
	case types.T_datetime:
		return subTimeFromDatetime(ivecs, result, proc, length, selectList)
	case types.T_timestamp:
		return subTimeFromTimestamp(ivecs, result, proc, length, selectList)
	case types.T_char, types.T_varchar, types.T_text:
		// Try to parse as datetime first, then time
		return subTimeFromString(ivecs, result, proc, length, selectList)
	default:
		return moerr.NewInvalidInputf(proc.Ctx, "subtime: unsupported type %v", inputType.Oid)
	}
}

func subTimeFromTime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	times1 := vector.GenerateFunctionFixedTypeParameter[types.Time](ivecs[0])
	time2Param := vector.GenerateFunctionStrParameter(ivecs[1])
	rs := vector.MustFunctionResult[types.Time](result)

	// Determine scale from input
	scale := int32(ivecs[0].GetType().Scale)
	if scale2 := int32(ivecs[1].GetType().Scale); scale2 > scale {
		scale = scale2
	}
	rs.TempSetType(types.New(types.T_time, 0, scale))

	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && selectList.Contains(i) {
			if err := rs.Append(types.Time(0), true); err != nil {
				return err
			}
			continue
		}

		time1, null1 := times1.GetValue(i)
		time2Str, null2 := time2Param.GetStrValue(i)

		if null1 || null2 {
			if err := rs.Append(types.Time(0), true); err != nil {
				return err
			}
			continue
		}

		// Parse time2 string
		time2, err := types.ParseTime(functionUtil.QuickBytesToStr(time2Str), scale)
		if err != nil {
			if err := rs.Append(types.Time(0), true); err != nil {
				return err
			}
			continue
		}

		// Subtract time2 from time1 (both are in microseconds)
		resultTime := types.Time(int64(time1) - int64(time2))

		// Validate result
		h := resultTime.Hour()
		if h < 0 {
			h = -h
		}
		if !types.ValidTime(uint64(h), 0, 0) {
			if err := rs.Append(types.Time(0), true); err != nil {
				return err
			}
			continue
		}

		if err := rs.Append(resultTime, false); err != nil {
			return err
		}
	}
	return nil
}

func subTimeFromDatetime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	datetimes := vector.GenerateFunctionFixedTypeParameter[types.Datetime](ivecs[0])
	time2Param := vector.GenerateFunctionStrParameter(ivecs[1])
	rs := vector.MustFunctionResult[types.Datetime](result)

	// Determine scale from input
	scale := int32(ivecs[0].GetType().Scale)
	if scale2 := int32(ivecs[1].GetType().Scale); scale2 > scale {
		scale = scale2
	}
	rs.TempSetType(types.New(types.T_datetime, 0, scale))

	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && selectList.Contains(i) {
			if err := rs.Append(types.Datetime(0), true); err != nil {
				return err
			}
			continue
		}

		dt, null1 := datetimes.GetValue(i)
		time2Str, null2 := time2Param.GetStrValue(i)

		if null1 || null2 {
			if err := rs.Append(types.Datetime(0), true); err != nil {
				return err
			}
			continue
		}

		// Parse time2 string
		time2, err := types.ParseTime(functionUtil.QuickBytesToStr(time2Str), scale)
		if err != nil {
			if err := rs.Append(types.Datetime(0), true); err != nil {
				return err
			}
			continue
		}

		// Subtract time2 from datetime (both are in microseconds)
		resultDt := types.Datetime(int64(dt) - int64(time2))

		if err := rs.Append(resultDt, false); err != nil {
			return err
		}
	}
	return nil
}

func subTimeFromTimestamp(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	timestamps := vector.GenerateFunctionFixedTypeParameter[types.Timestamp](ivecs[0])
	time2Param := vector.GenerateFunctionStrParameter(ivecs[1])
	rs := vector.MustFunctionResult[types.Timestamp](result)
	loc := proc.GetSessionInfo().TimeZone

	// Determine scale from input
	scale := int32(ivecs[0].GetType().Scale)
	if scale2 := int32(ivecs[1].GetType().Scale); scale2 > scale {
		scale = scale2
	}
	rs.TempSetType(types.New(types.T_timestamp, 0, scale))

	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && selectList.Contains(i) {
			if err := rs.Append(types.Timestamp(0), true); err != nil {
				return err
			}
			continue
		}

		ts, null1 := timestamps.GetValue(i)
		time2Str, null2 := time2Param.GetStrValue(i)

		if null1 || null2 {
			if err := rs.Append(types.Timestamp(0), true); err != nil {
				return err
			}
			continue
		}

		// Parse time2 string
		time2, err := types.ParseTime(functionUtil.QuickBytesToStr(time2Str), scale)
		if err != nil {
			if err := rs.Append(types.Timestamp(0), true); err != nil {
				return err
			}
			continue
		}

		// Convert timestamp to datetime, subtract time, convert back
		dt := ts.ToDatetime(loc)
		resultDt := types.Datetime(int64(dt) - int64(time2))
		resultTs := resultDt.ToTimestamp(loc)

		if err := rs.Append(resultTs, false); err != nil {
			return err
		}
	}
	return nil
}

func subTimeFromString(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	// Try to parse as datetime first
	dtParam := vector.GenerateFunctionStrParameter(ivecs[0])
	time2Param := vector.GenerateFunctionStrParameter(ivecs[1])
	rs := vector.MustFunctionResult[types.Datetime](result)

	scale := int32(6) // Use max scale for string inputs
	rs.TempSetType(types.New(types.T_datetime, 0, scale))

	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && selectList.Contains(i) {
			if err := rs.Append(types.Datetime(0), true); err != nil {
				return err
			}
			continue
		}

		dtStr, null1 := dtParam.GetStrValue(i)
		time2Str, null2 := time2Param.GetStrValue(i)

		if null1 || null2 {
			if err := rs.Append(types.Datetime(0), true); err != nil {
				return err
			}
			continue
		}

		// Try to parse as datetime
		dt, err := types.ParseDatetime(functionUtil.QuickBytesToStr(dtStr), scale)
		if err != nil {
			// If parsing as datetime fails, try as time
			time1, err2 := types.ParseTime(functionUtil.QuickBytesToStr(dtStr), scale)
			if err2 != nil {
				if err := rs.Append(types.Datetime(0), true); err != nil {
					return err
				}
				continue
			}
			// Convert time to datetime (using today's date)
			dt = time1.ToDatetime(scale)
		}

		// Parse time2 string
		time2, err := types.ParseTime(functionUtil.QuickBytesToStr(time2Str), scale)
		if err != nil {
			if err := rs.Append(types.Datetime(0), true); err != nil {
				return err
			}
			continue
		}

		// Subtract time2 from datetime
		resultDt := types.Datetime(int64(dt) - int64(time2))

		if err := rs.Append(resultDt, false); err != nil {
			return err
		}
	}
	return nil
}

func DateFormat(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	if !ivecs[1].IsConst() {
		return moerr.NewInvalidArg(proc.Ctx, "date format format", "not constant")
	}

	rs := vector.MustFunctionResult[types.Varlena](result)

	dates := vector.GenerateFunctionFixedTypeParameter[types.Datetime](ivecs[0])
	formats := vector.GenerateFunctionStrParameter(ivecs[1])
	fmt, null2 := formats.GetStrValue(0)

	var dateFmtOperator DateFormatFunc
	switch string(fmt) {
	case "%d/%m/%Y":
		dateFmtOperator = date_format_combine_pattern1
	case "%Y%m%d":
		dateFmtOperator = date_format_combine_pattern2
	case "%Y":
		dateFmtOperator = date_format_combine_pattern3
	case "%Y-%m-%d":
		dateFmtOperator = date_format_combine_pattern4
	case "%Y-%m-%d %H:%i:%s", "%Y-%m-%d %T":
		dateFmtOperator = date_format_combine_pattern5
	case "%Y/%m/%d":
		dateFmtOperator = date_format_combine_pattern6
	case "%Y/%m/%d %H:%i:%s", "%Y/%m/%d %T":
		dateFmtOperator = date_format_combine_pattern7
	default:
		dateFmtOperator = datetimeFormat
	}

	//format := "%b %D %M"   -> []func{func1,func2,  func3}
	var buf bytes.Buffer
	for i := uint64(0); i < uint64(length); i++ {
		d, null1 := dates.GetValue(i)
		if null1 || null2 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			buf.Reset()
			if err = dateFmtOperator(proc.Ctx, d, string(fmt), &buf); err != nil {
				return err
			}
			if err = rs.AppendBytes(buf.Bytes(), false); err != nil {
				return err
			}
		}
	}
	return nil
}

type DateFormatFunc func(ctx context.Context, datetime types.Datetime, format string, buf *bytes.Buffer) error

// DATE_FORMAT       datetime
// handle '%d/%m/%Y' ->	 22/04/2021
func date_format_combine_pattern1(ctx context.Context, t types.Datetime, format string, buf *bytes.Buffer) error {
	month := int(t.Month())
	day := int(t.Day())
	year := int(t.Year())

	// Convert numbers to strings using bitwise operations and append them to a buffer
	buf.Grow(10) // Pre allocate sufficient buffer size

	// Date and month conversion
	buf.WriteByte(byte('0' + (day / 10 % 10)))
	buf.WriteByte(byte('0' + (day % 10)))
	buf.WriteByte('/')
	buf.WriteByte(byte('0' + (month / 10 % 10)))
	buf.WriteByte(byte('0' + (month % 10)))
	buf.WriteByte('/')

	// Year conversion
	buf.WriteByte(byte('0' + (year / 1000 % 10)))
	buf.WriteByte(byte('0' + (year / 100 % 10)))
	buf.WriteByte(byte('0' + (year / 10 % 10)))
	buf.WriteByte(byte('0' + (year % 10)))
	return nil
}

// handle '%Y%m%d' ->   20210422
func date_format_combine_pattern2(ctx context.Context, t types.Datetime, format string, buf *bytes.Buffer) error {
	year := t.Year()
	month := int(t.Month())
	day := int(t.Day())

	// Convert numbers to strings using bitwise operations and append them to a buffer
	buf.Grow(8) // Pre allocate sufficient buffer size

	// Year conversion
	buf.WriteByte(byte('0' + (year / 1000 % 10)))
	buf.WriteByte(byte('0' + (year / 100 % 10)))
	buf.WriteByte(byte('0' + (year / 10 % 10)))
	buf.WriteByte(byte('0' + (year % 10)))

	// Month conversion
	buf.WriteByte(byte('0' + (month / 10)))
	buf.WriteByte(byte('0' + (month % 10)))

	// date conversion
	buf.WriteByte(byte('0' + (day / 10)))
	buf.WriteByte(byte('0' + (day % 10)))
	return nil
}

// handle '%Y'  ->   2021
func date_format_combine_pattern3(ctx context.Context, t types.Datetime, format string, buf *bytes.Buffer) error {
	year := t.Year()
	// Year conversion
	buf.WriteByte(byte('0' + (year / 1000 % 10)))
	buf.WriteByte(byte('0' + (year / 100 % 10)))
	buf.WriteByte(byte('0' + (year / 10 % 10)))
	buf.WriteByte(byte('0' + (year % 10)))
	return nil
}

// %Y-%m-%d	               2021-04-22
func date_format_combine_pattern4(ctx context.Context, t types.Datetime, format string, buf *bytes.Buffer) error {
	year := t.Year()
	month := int(t.Month())
	day := int(t.Day())

	// Convert numbers to strings using bitwise operations and append them to a buffer
	buf.Grow(10) // Pre allocate sufficient buffer size

	// Year conversion
	buf.WriteByte(byte('0' + (year / 1000 % 10)))
	buf.WriteByte(byte('0' + (year / 100 % 10)))
	buf.WriteByte(byte('0' + (year / 10 % 10)))
	buf.WriteByte(byte('0' + (year % 10)))

	// Month conversion
	buf.WriteByte('-')
	buf.WriteByte(byte('0' + (month / 10)))
	buf.WriteByte(byte('0' + (month % 10)))

	// date conversion
	buf.WriteByte('-')
	buf.WriteByte(byte('0' + (day / 10)))
	buf.WriteByte(byte('0' + (day % 10)))
	return nil
}

// handle '%Y-%m-%d %H:%i:%s'  ->   2004-04-03 13:11:10
// handle ' %Y-%m-%d %T'   ->   2004-04-03 13:11:10
func date_format_combine_pattern5(ctx context.Context, t types.Datetime, format string, buf *bytes.Buffer) error {
	year := int(t.Year())
	month := int(t.Month())
	day := int(t.Day())
	hour := int(t.Hour())
	minute := int(t.Minute())
	sec := int(t.Sec())

	// Convert numbers to strings using bitwise operations and append them to a buffer
	buf.Grow(19) // Pre allocate sufficient buffer size

	// Year conversion
	buf.WriteByte(byte('0' + (year / 1000 % 10)))
	buf.WriteByte(byte('0' + (year / 100 % 10)))
	buf.WriteByte(byte('0' + (year / 10 % 10)))
	buf.WriteByte(byte('0' + (year % 10)))

	buf.WriteByte('-')

	// Month conversion
	buf.WriteByte(byte('0' + (month / 10)))
	buf.WriteByte(byte('0' + (month % 10)))

	buf.WriteByte('-')

	// date conversion
	buf.WriteByte(byte('0' + (day / 10)))
	buf.WriteByte(byte('0' + (day % 10)))

	buf.WriteByte(' ')

	// Hour conversion
	buf.WriteByte(byte('0' + (hour / 10)))
	buf.WriteByte(byte('0' + (hour % 10)))

	buf.WriteByte(':')

	// Minute conversion
	buf.WriteByte(byte('0' + (minute / 10)))
	buf.WriteByte(byte('0' + (minute % 10)))

	buf.WriteByte(':')

	// Second conversion
	buf.WriteByte(byte('0' + (sec / 10)))
	buf.WriteByte(byte('0' + (sec % 10)))
	return nil
}

// handle '%Y/%m/%d'  ->   2010/01/07
func date_format_combine_pattern6(ctx context.Context, t types.Datetime, format string, buf *bytes.Buffer) error {
	year := t.Year()
	month := int(t.Month())
	day := int(t.Day())

	// Convert numbers to strings using bitwise operations and append them to a buffer
	buf.Grow(10) // Pre allocate sufficient buffer size

	// Year conversion
	buf.WriteByte(byte('0' + (year / 1000 % 10)))
	buf.WriteByte(byte('0' + (year / 100 % 10)))
	buf.WriteByte(byte('0' + (year / 10 % 10)))
	buf.WriteByte(byte('0' + (year % 10)))

	// Month conversion
	buf.WriteByte('/')
	buf.WriteByte(byte('0' + (month / 10)))
	buf.WriteByte(byte('0' + (month % 10)))

	// date conversion
	buf.WriteByte('/')
	buf.WriteByte(byte('0' + (day / 10)))
	buf.WriteByte(byte('0' + (day % 10)))
	return nil
}

// handle '%Y/%m/%d %H:%i:%s'   ->    2010/01/07 23:12:34
// handle '%Y/%m/%d %T'   ->    2010/01/07 23:12:34
func date_format_combine_pattern7(ctx context.Context, t types.Datetime, format string, buf *bytes.Buffer) error {
	year := int(t.Year())
	month := int(t.Month())
	day := int(t.Day())
	hour := int(t.Hour())
	minute := int(t.Minute())
	sec := int(t.Sec())

	// Convert numbers to strings using bitwise operations and append them to a buffer
	buf.Grow(19) // Pre allocate sufficient buffer size

	// Year conversion
	buf.WriteByte(byte('0' + (year / 1000 % 10)))
	buf.WriteByte(byte('0' + (year / 100 % 10)))
	buf.WriteByte(byte('0' + (year / 10 % 10)))
	buf.WriteByte(byte('0' + (year % 10)))

	buf.WriteByte('/')

	// Month conversion
	buf.WriteByte(byte('0' + (month / 10)))
	buf.WriteByte(byte('0' + (month % 10)))

	buf.WriteByte('/')

	// date conversion
	buf.WriteByte(byte('0' + (day / 10)))
	buf.WriteByte(byte('0' + (day % 10)))

	buf.WriteByte(' ')

	// Hour conversion
	buf.WriteByte(byte('0' + (hour / 10)))
	buf.WriteByte(byte('0' + (hour % 10)))

	buf.WriteByte(':')

	// Minute conversion
	buf.WriteByte(byte('0' + (minute / 10)))
	buf.WriteByte(byte('0' + (minute % 10)))

	buf.WriteByte(':')

	// Second conversion
	buf.WriteByte(byte('0' + (sec / 10)))
	buf.WriteByte(byte('0' + (sec % 10)))
	return nil
}

// datetimeFormat: format the datetime value according to the format string.
func datetimeFormat(ctx context.Context, datetime types.Datetime, format string, buf *bytes.Buffer) error {
	inPatternMatch := false
	for _, b := range format {
		if inPatternMatch {
			if err := makeDateFormat(ctx, datetime, b, buf); err != nil {
				return err
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
	return nil
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
			return moerr.NewInvalidInputf(ctx, "invalud date format for month '%d'", m)
		}
		buf.WriteString(MonthNames[m-1][:3])
	case 'M':
		m := t.Month()
		if m == 0 || m > 12 {
			return moerr.NewInvalidInputf(ctx, "invalud date format for month '%d'", m)
		}
		buf.WriteString(MonthNames[m-1])
	case 'm':
		FormatInt2BufByWidth(int(t.Month()), 2, buf)
		//buf.WriteString(FormatIntByWidth(int(t.Month()), 2))
	case 'c':
		buf.WriteString(strconv.FormatInt(int64(t.Month()), 10))
	case 'D':
		buf.WriteString(strconv.FormatInt(int64(t.Day()), 10))
		buf.WriteString(AbbrDayOfMonth(int(t.Day())))
	case 'd':
		FormatInt2BufByWidth(int(t.Day()), 2, buf)
		//buf.WriteString(FormatIntByWidth(int(t.Day()), 2))
	case 'e':
		buf.WriteString(strconv.FormatInt(int64(t.Day()), 10))
	case 'f':
		fmt.Fprintf(buf, "%06d", t.MicroSec())
	case 'j':
		fmt.Fprintf(buf, "%03d", t.DayOfYear())
	case 'H':
		FormatInt2BufByWidth(int(t.Hour()), 2, buf)
		//buf.WriteString(FormatIntByWidth(int(t.Hour()), 2))
	case 'k':
		buf.WriteString(strconv.FormatInt(int64(t.Hour()), 10))
	case 'h', 'I':
		tt := t.Hour()
		if tt%12 == 0 {
			buf.WriteString("12")
		} else {
			FormatInt2BufByWidth(int(tt%12), 2, buf)
			//buf.WriteString(FormatIntByWidth(int(tt%12), 2))
		}
	case 'i':
		FormatInt2BufByWidth(int(t.Minute()), 2, buf)
		//buf.WriteString(FormatIntByWidth(int(t.Minute()), 2))
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
		FormatInt2BufByWidth(int(t.Sec()), 2, buf)
		//buf.WriteString(FormatIntByWidth(int(t.Sec()), 2))
	case 'T':
		fmt.Fprintf(buf, "%02d:%02d:%02d", t.Hour(), t.Minute(), t.Sec())
	case 'U':
		w := t.Week(0)
		FormatInt2BufByWidth(w, 2, buf)
		//buf.WriteString(FormatIntByWidth(w, 2))
	case 'u':
		w := t.Week(1)
		FormatInt2BufByWidth(w, 2, buf)
		//buf.WriteString(FormatIntByWidth(w, 2))
	case 'V':
		w := t.Week(2)
		FormatInt2BufByWidth(w, 2, buf)
		//buf.WriteString(FormatIntByWidth(w, 2))
	case 'v':
		_, w := t.YearWeek(3)
		FormatInt2BufByWidth(w, 2, buf)
		//buf.WriteString(FormatIntByWidth(w, 2))
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
			FormatInt2BufByWidth(year, 4, buf)
			//buf.WriteString(FormatIntByWidth(year, 4))
		}
	case 'x':
		year, _ := t.YearWeek(3)
		if year < 0 {
			buf.WriteString(strconv.FormatUint(uint64(math.MaxUint32), 10))
		} else {
			FormatInt2BufByWidth(year, 4, buf)
			//buf.WriteString(FormatIntByWidth(year, 4))
		}
	case 'Y':
		FormatInt2BufByWidth(int(t.Year()), 4, buf)
		//buf.WriteString(FormatIntByWidth(int(t.Year()), 4))
	case 'y':
		str := FormatIntByWidth(int(t.Year()), 4)
		buf.WriteString(str[2:])
	default:
		buf.WriteRune(b)
	}
	return nil
}

// TimeFormat: format the time value according to the format string.
// TIME_FORMAT only supports time-related format specifiers: %H, %h, %I, %i, %k, %l, %S, %s, %f, %p, %r, %T
func TimeFormat(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	if !ivecs[1].IsConst() {
		return moerr.NewInvalidArg(proc.Ctx, "time format format", "not constant")
	}

	rs := vector.MustFunctionResult[types.Varlena](result)

	times := vector.GenerateFunctionFixedTypeParameter[types.Time](ivecs[0])
	formats := vector.GenerateFunctionStrParameter(ivecs[1])
	fmt, null2 := formats.GetStrValue(0)

	var buf bytes.Buffer
	for i := uint64(0); i < uint64(length); i++ {
		t, null1 := times.GetValue(i)
		if null1 || null2 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			buf.Reset()
			if err = timeFormat(proc.Ctx, t, string(fmt), &buf); err != nil {
				return err
			}
			if err = rs.AppendBytes(buf.Bytes(), false); err != nil {
				return err
			}
		}
	}
	return nil
}

// timeFormat: Get the format string corresponding to the time according to format specifiers
// Only supports time-related format specifiers: %H, %h, %I, %i, %k, %l, %S, %s, %f, %p, %r, %T
func timeFormat(ctx context.Context, t types.Time, format string, buf *bytes.Buffer) error {
	hour, minute, sec, msec, _ := t.ClockFormat()
	inPatternMatch := false
	for _, b := range format {
		if inPatternMatch {
			if err := makeTimeFormat(ctx, hour, minute, sec, msec, b, buf); err != nil {
				return err
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
	return nil
}

// makeTimeFormat: Get the format string corresponding to the time according to a single format character
// Only supports time-related format specifiers
func makeTimeFormat(ctx context.Context, hour uint64, minute, sec uint8, msec uint64, b rune, buf *bytes.Buffer) error {
	switch b {
	case 'f':
		fmt.Fprintf(buf, "%06d", msec)
	case 'H':
		FormatInt2BufByWidth(int(hour), 2, buf)
	case 'k':
		buf.WriteString(strconv.FormatUint(hour, 10))
	case 'h', 'I':
		tt := hour % 24
		if tt%12 == 0 {
			buf.WriteString("12")
		} else {
			FormatInt2BufByWidth(int(tt%12), 2, buf)
		}
	case 'i':
		FormatInt2BufByWidth(int(minute), 2, buf)
	case 'l':
		tt := hour % 24
		if tt%12 == 0 {
			buf.WriteString("12")
		} else {
			buf.WriteString(strconv.FormatUint(tt%12, 10))
		}
	case 'p':
		h := hour % 24
		if h/12%2 == 0 {
			buf.WriteString("AM")
		} else {
			buf.WriteString("PM")
		}
	case 'r':
		h := hour % 24
		switch {
		case h == 0:
			fmt.Fprintf(buf, "%02d:%02d:%02d AM", 12, minute, sec)
		case h == 12:
			fmt.Fprintf(buf, "%02d:%02d:%02d PM", 12, minute, sec)
		case h < 12:
			fmt.Fprintf(buf, "%02d:%02d:%02d AM", h, minute, sec)
		default:
			fmt.Fprintf(buf, "%02d:%02d:%02d PM", h-12, minute, sec)
		}
	case 'S', 's':
		FormatInt2BufByWidth(int(sec), 2, buf)
	case 'T':
		fmt.Fprintf(buf, "%02d:%02d:%02d", hour%24, minute, sec)
	default:
		// For unsupported format specifiers, just write the character as-is
		// This matches MySQL behavior where non-time format specifiers are ignored
		buf.WriteRune(b)
	}
	return nil
}

// FormatIntByWidth: Formatintwidthn is used to format ints with width parameter n. Insufficient numbers are filled with 0.
func FormatIntByWidth(num, n int) string {
	numStr := strconv.Itoa(num)
	if len(numStr) >= n {
		return numStr
	}

	pad := strings.Repeat("0", n-len(numStr))
	var builder strings.Builder
	builder.Grow(n)
	builder.WriteString(pad)
	builder.WriteString(numStr)
	return builder.String()
}

func FormatInt2BufByWidth(num, n int, buf *bytes.Buffer) {
	numStr := strconv.Itoa(num)
	if len(numStr) >= n {
		buf.WriteString(numStr)
		return
	}

	pad := strings.Repeat("0", n-len(numStr))
	buf.WriteString(pad)
	buf.WriteString(numStr)
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

func doTimeSub(start types.Time, diff int64, iTyp types.IntervalType) (types.Time, error) {
	err := types.JudgeIntervalNumOverflow(diff, iTyp)
	if err != nil {
		return 0, err
	}
	t, success := start.AddInterval(-diff, iTyp)
	if success {
		return t, nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("time", "")
	}
}

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

func DateSub(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	unit, _ := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[2]).GetValue(0)
	iTyp := types.IntervalType(unit)

	return opBinaryFixedFixedToFixedWithErrorCheck[types.Date, int64, types.Date](ivecs, result, proc, length, func(v1 types.Date, v2 int64) (types.Date, error) {
		return doDateSub(v1, v2, iTyp)
	}, selectList)
}

func DatetimeSub(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
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
	}, selectList)
}

func DateStringSub(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) (err error) {
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

func TimestampSub(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	unit, _ := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[2]).GetValue(0)
	iTyp := types.IntervalType(unit)

	scale := ivecs[0].GetType().Scale
	if iTyp == types.MicroSecond {
		scale = 6
	}
	rs := vector.MustFunctionResult[types.Timestamp](result)
	rs.TempSetType(types.New(types.T_timestamp, 0, scale))

	return opBinaryFixedFixedToFixedWithErrorCheck[types.Timestamp, int64, types.Timestamp](ivecs, result, proc, length, func(v1 types.Timestamp, v2 int64) (types.Timestamp, error) {
		return doTimestampSub(proc.GetSessionInfo().TimeZone, v1, v2, iTyp)
	}, selectList)
}

func TimeSub(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	rs := vector.MustFunctionResult[types.Time](result)
	unit, _ := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[2]).GetValue(0)
	scale := ivecs[0].GetType().Scale
	iTyp := types.IntervalType(unit)
	if iTyp == types.MicroSecond {
		scale = 6
	}
	rs.TempSetType(types.New(types.T_time, 0, scale))

	return opBinaryFixedFixedToFixedWithErrorCheck[types.Time, int64, types.Time](ivecs, result, proc, length, func(v1 types.Time, v2 int64) (types.Time, error) {
		return doTimeSub(v1, v2, iTyp)
	}, selectList)
}

type number interface {
	constraints.Unsigned | constraints.Signed | constraints.Float
}

func fieldCheck(overloads []overload, inputs []types.Type) checkResult {
	tc := func(inputs []types.Type, t types.T) bool {
		for _, input := range inputs {
			if (input.Oid == types.T_char && t == types.T_varchar) || (input.Oid == types.T_varchar && t == types.T_char) {
				continue
			}
			if input.Oid != t && input.Oid != types.T_any {
				return false
			}
		}
		return true
	}
	if len(inputs) < 2 {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}
	returnType := [...]types.T{
		types.T_varchar, types.T_char,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_bit,
	}
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

func FieldNumber[T number](ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) (err error) {
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

func FieldString(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) (err error) {
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

func eltCheck(overloads []overload, inputs []types.Type) checkResult {
	if len(inputs) < 2 {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}

	shouldCast := false
	castTypes := make([]types.Type, len(inputs))

	// First argument must be numeric (int64)
	if !inputs[0].Oid.IsInteger() && inputs[0].Oid != types.T_any {
		c, _ := tryToMatch([]types.Type{inputs[0]}, []types.T{types.T_int64})
		if c == matchFailed {
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		}
		if c == matchByCast {
			shouldCast = true
			castTypes[0] = types.T_int64.ToType()
		} else {
			castTypes[0] = inputs[0]
		}
	} else {
		castTypes[0] = inputs[0]
	}

	// Rest arguments must be strings
	for i := 1; i < len(inputs); i++ {
		if !inputs[i].Oid.IsMySQLString() && inputs[i].Oid != types.T_any {
			c, _ := tryToMatch([]types.Type{inputs[i]}, []types.T{types.T_varchar})
			if c == matchFailed {
				return newCheckResultWithFailure(failedFunctionParametersWrong)
			}
			if c == matchByCast {
				shouldCast = true
				castTypes[i] = types.T_varchar.ToType()
			} else {
				castTypes[i] = inputs[i]
			}
		} else {
			castTypes[i] = inputs[i]
		}
	}

	if shouldCast {
		return newCheckResultWithCast(0, castTypes)
	}
	return newCheckResultWithSuccess(0)
}

// Elt: ELT(N, str1, str2, str3, ...) - Returns str1 if N = 1, str2 if N = 2, and so on.
// Returns NULL if N is less than 1, greater than the number of strings, or NULL.
func Elt(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Varlena](result)

	// First argument is the index N
	nParam := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[0])

	// Rest arguments are strings
	strParams := make([]vector.FunctionParameterWrapper[types.Varlena], len(ivecs)-1)
	for i := 1; i < len(ivecs); i++ {
		strParams[i-1] = vector.GenerateFunctionStrParameter(ivecs[i])
	}

	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && selectList.Contains(i) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		n, null := nParam.GetValue(i)
		if null || n < 1 || n > int64(len(strParams)) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		// Get the string at position n (1-based, so index is n-1)
		str, null := strParams[n-1].GetStrValue(i)
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		if err := rs.AppendBytes(str, false); err != nil {
			return err
		}
	}
	return nil
}

func makeSetCheck(overloads []overload, inputs []types.Type) checkResult {
	// MAKE_SET(bits, str1, str2, ...)
	// Minimum 2 arguments (bits + at least one string)
	if len(inputs) < 2 {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}

	shouldCast := false
	castTypes := make([]types.Type, len(inputs))

	// First argument (bits) must be numeric
	isNumeric := inputs[0].Oid.IsInteger() || inputs[0].Oid.IsFloat() || inputs[0].Oid == types.T_decimal64 || inputs[0].Oid == types.T_decimal128 || inputs[0].Oid == types.T_bit
	if !isNumeric && inputs[0].Oid != types.T_any {
		c, _ := tryToMatch([]types.Type{inputs[0]}, []types.T{types.T_int64})
		if c == matchFailed {
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		}
		if c == matchByCast {
			shouldCast = true
			castTypes[0] = types.T_int64.ToType()
		} else {
			castTypes[0] = inputs[0]
		}
	} else {
		castTypes[0] = inputs[0]
	}

	// Rest arguments must be strings
	for i := 1; i < len(inputs); i++ {
		if !inputs[i].Oid.IsMySQLString() && inputs[i].Oid != types.T_any {
			c, _ := tryToMatch([]types.Type{inputs[i]}, []types.T{types.T_varchar})
			if c == matchFailed {
				return newCheckResultWithFailure(failedFunctionParametersWrong)
			}
			if c == matchByCast {
				shouldCast = true
				castTypes[i] = types.T_varchar.ToType()
			} else {
				castTypes[i] = inputs[i]
			}
		} else {
			castTypes[i] = inputs[i]
		}
	}

	if shouldCast {
		return newCheckResultWithCast(0, castTypes)
	}
	return newCheckResultWithSuccess(0)
}

// MakeSet: MAKE_SET(bits, str1, str2, ...) - Returns a set value (a string containing substrings separated by ',' characters) consisting of the strings that have the corresponding bit in bits set.
func MakeSet(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Varlena](result)

	// First argument: bits (numeric) - handle different numeric types
	bitsType := ivecs[0].GetType().Oid
	var getBitsValue func(uint64) (uint64, bool)

	// Create appropriate parameter wrapper based on type (once, outside loop)
	switch bitsType {
	case types.T_int8:
		param := vector.GenerateFunctionFixedTypeParameter[int8](ivecs[0])
		getBitsValue = func(i uint64) (uint64, bool) {
			val, null := param.GetValue(i)
			if null {
				return 0, true
			}
			return uint64(val), false
		}
	case types.T_int16:
		param := vector.GenerateFunctionFixedTypeParameter[int16](ivecs[0])
		getBitsValue = func(i uint64) (uint64, bool) {
			val, null := param.GetValue(i)
			if null {
				return 0, true
			}
			return uint64(val), false
		}
	case types.T_int32:
		param := vector.GenerateFunctionFixedTypeParameter[int32](ivecs[0])
		getBitsValue = func(i uint64) (uint64, bool) {
			val, null := param.GetValue(i)
			if null {
				return 0, true
			}
			return uint64(val), false
		}
	case types.T_int64:
		param := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[0])
		getBitsValue = func(i uint64) (uint64, bool) {
			val, null := param.GetValue(i)
			if null {
				return 0, true
			}
			return uint64(val), false
		}
	case types.T_uint8:
		param := vector.GenerateFunctionFixedTypeParameter[uint8](ivecs[0])
		getBitsValue = func(i uint64) (uint64, bool) {
			val, null := param.GetValue(i)
			if null {
				return 0, true
			}
			return uint64(val), false
		}
	case types.T_uint16:
		param := vector.GenerateFunctionFixedTypeParameter[uint16](ivecs[0])
		getBitsValue = func(i uint64) (uint64, bool) {
			val, null := param.GetValue(i)
			if null {
				return 0, true
			}
			return uint64(val), false
		}
	case types.T_uint32:
		param := vector.GenerateFunctionFixedTypeParameter[uint32](ivecs[0])
		getBitsValue = func(i uint64) (uint64, bool) {
			val, null := param.GetValue(i)
			if null {
				return 0, true
			}
			return uint64(val), false
		}
	case types.T_uint64:
		param := vector.GenerateFunctionFixedTypeParameter[uint64](ivecs[0])
		getBitsValue = func(i uint64) (uint64, bool) {
			val, null := param.GetValue(i)
			if null {
				return 0, true
			}
			return val, false
		}
	case types.T_float32:
		param := vector.GenerateFunctionFixedTypeParameter[float32](ivecs[0])
		getBitsValue = func(i uint64) (uint64, bool) {
			val, null := param.GetValue(i)
			if null {
				return 0, true
			}
			return uint64(int64(val)), false
		}
	case types.T_float64:
		param := vector.GenerateFunctionFixedTypeParameter[float64](ivecs[0])
		getBitsValue = func(i uint64) (uint64, bool) {
			val, null := param.GetValue(i)
			if null {
				return 0, true
			}
			return uint64(int64(val)), false
		}
	default:
		// Fallback to int64
		param := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[0])
		getBitsValue = func(i uint64) (uint64, bool) {
			val, null := param.GetValue(i)
			if null {
				return 0, true
			}
			return uint64(val), false
		}
	}

	// Rest arguments are strings
	strParams := make([]vector.FunctionParameterWrapper[types.Varlena], len(ivecs)-1)
	for i := 1; i < len(ivecs); i++ {
		strParams[i-1] = vector.GenerateFunctionStrParameter(ivecs[i])
	}

	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && selectList.Contains(i) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		// Extract bits value using the appropriate getter
		bitsUint, nullBits := getBitsValue(i)
		if nullBits {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		// Build the result string by checking each bit position
		var parts []string
		for j := 0; j < len(strParams); j++ {
			// Check if bit j is set (0-based, so bit 0 corresponds to str1, bit 1 to str2, etc.)
			if (bitsUint>>uint(j))&1 == 1 {
				str, null := strParams[j].GetStrValue(i)
				if !null {
					parts = append(parts, functionUtil.QuickBytesToStr(str))
				}
			}
		}

		// Join with comma separator
		resultStr := strings.Join(parts, ",")
		if err := rs.AppendBytes([]byte(resultStr), false); err != nil {
			return err
		}
	}
	return nil
}

func exportSetCheck(overloads []overload, inputs []types.Type) checkResult {
	// EXPORT_SET(bits, on, off[, separator[, number_of_bits]])
	// Minimum 3 arguments, maximum 5 arguments
	if len(inputs) < 3 || len(inputs) > 5 {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}

	shouldCast := false
	castTypes := make([]types.Type, len(inputs))

	// First argument (bits) must be numeric
	// Check if it's a numeric type (integer, float, or decimal)
	isNumeric := inputs[0].Oid.IsInteger() || inputs[0].Oid.IsFloat() || inputs[0].Oid == types.T_decimal64 || inputs[0].Oid == types.T_decimal128 || inputs[0].Oid == types.T_bit
	if !isNumeric && inputs[0].Oid != types.T_any {
		c, _ := tryToMatch([]types.Type{inputs[0]}, []types.T{types.T_int64})
		if c == matchFailed {
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		}
		if c == matchByCast {
			shouldCast = true
			castTypes[0] = types.T_int64.ToType()
		} else {
			castTypes[0] = inputs[0]
		}
	} else {
		castTypes[0] = inputs[0]
	}

	// Second and third arguments (on, off) must be strings
	for i := 1; i <= 2; i++ {
		if !inputs[i].Oid.IsMySQLString() && inputs[i].Oid != types.T_any {
			c, _ := tryToMatch([]types.Type{inputs[i]}, []types.T{types.T_varchar})
			if c == matchFailed {
				return newCheckResultWithFailure(failedFunctionParametersWrong)
			}
			if c == matchByCast {
				shouldCast = true
				castTypes[i] = types.T_varchar.ToType()
			} else {
				castTypes[i] = inputs[i]
			}
		} else {
			castTypes[i] = inputs[i]
		}
	}

	// Optional fourth argument (separator) must be string
	if len(inputs) >= 4 {
		if !inputs[3].Oid.IsMySQLString() && inputs[3].Oid != types.T_any {
			c, _ := tryToMatch([]types.Type{inputs[3]}, []types.T{types.T_varchar})
			if c == matchFailed {
				return newCheckResultWithFailure(failedFunctionParametersWrong)
			}
			if c == matchByCast {
				shouldCast = true
				castTypes[3] = types.T_varchar.ToType()
			} else {
				castTypes[3] = inputs[3]
			}
		} else {
			castTypes[3] = inputs[3]
		}
	}

	// Optional fifth argument (number_of_bits) must be numeric
	if len(inputs) >= 5 {
		if !inputs[4].Oid.IsInteger() && inputs[4].Oid != types.T_any {
			c, _ := tryToMatch([]types.Type{inputs[4]}, []types.T{types.T_int64})
			if c == matchFailed {
				return newCheckResultWithFailure(failedFunctionParametersWrong)
			}
			if c == matchByCast {
				shouldCast = true
				castTypes[4] = types.T_int64.ToType()
			} else {
				castTypes[4] = inputs[4]
			}
		} else {
			castTypes[4] = inputs[4]
		}
	}

	if shouldCast {
		return newCheckResultWithCast(0, castTypes)
	}
	return newCheckResultWithSuccess(0)
}

// ExportSet: EXPORT_SET(bits, on, off[, separator[, number_of_bits]]) - Returns a string such that for every bit set in the value bits, you get an on string and for every bit not set, you get an off string.
func ExportSet(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Varlena](result)

	// First argument: bits (numeric) - handle different numeric types
	bitsType := ivecs[0].GetType().Oid
	var bitsUint uint64
	var nullBits bool

	// Create appropriate parameter wrapper based on type (once, outside loop)
	var getBitsValue func(uint64) (uint64, bool)
	switch bitsType {
	case types.T_int8:
		param := vector.GenerateFunctionFixedTypeParameter[int8](ivecs[0])
		getBitsValue = func(i uint64) (uint64, bool) {
			val, null := param.GetValue(i)
			if null {
				return 0, true
			}
			return uint64(val), false
		}
	case types.T_int16:
		param := vector.GenerateFunctionFixedTypeParameter[int16](ivecs[0])
		getBitsValue = func(i uint64) (uint64, bool) {
			val, null := param.GetValue(i)
			if null {
				return 0, true
			}
			return uint64(val), false
		}
	case types.T_int32:
		param := vector.GenerateFunctionFixedTypeParameter[int32](ivecs[0])
		getBitsValue = func(i uint64) (uint64, bool) {
			val, null := param.GetValue(i)
			if null {
				return 0, true
			}
			return uint64(val), false
		}
	case types.T_int64:
		param := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[0])
		getBitsValue = func(i uint64) (uint64, bool) {
			val, null := param.GetValue(i)
			if null {
				return 0, true
			}
			return uint64(val), false
		}
	case types.T_uint8:
		param := vector.GenerateFunctionFixedTypeParameter[uint8](ivecs[0])
		getBitsValue = func(i uint64) (uint64, bool) {
			val, null := param.GetValue(i)
			if null {
				return 0, true
			}
			return uint64(val), false
		}
	case types.T_uint16:
		param := vector.GenerateFunctionFixedTypeParameter[uint16](ivecs[0])
		getBitsValue = func(i uint64) (uint64, bool) {
			val, null := param.GetValue(i)
			if null {
				return 0, true
			}
			return uint64(val), false
		}
	case types.T_uint32:
		param := vector.GenerateFunctionFixedTypeParameter[uint32](ivecs[0])
		getBitsValue = func(i uint64) (uint64, bool) {
			val, null := param.GetValue(i)
			if null {
				return 0, true
			}
			return uint64(val), false
		}
	case types.T_uint64:
		param := vector.GenerateFunctionFixedTypeParameter[uint64](ivecs[0])
		getBitsValue = func(i uint64) (uint64, bool) {
			val, null := param.GetValue(i)
			if null {
				return 0, true
			}
			return val, false
		}
	case types.T_float32:
		param := vector.GenerateFunctionFixedTypeParameter[float32](ivecs[0])
		getBitsValue = func(i uint64) (uint64, bool) {
			val, null := param.GetValue(i)
			if null {
				return 0, true
			}
			return uint64(int64(val)), false
		}
	case types.T_float64:
		param := vector.GenerateFunctionFixedTypeParameter[float64](ivecs[0])
		getBitsValue = func(i uint64) (uint64, bool) {
			val, null := param.GetValue(i)
			if null {
				return 0, true
			}
			return uint64(int64(val)), false
		}
	default:
		// Fallback to int64
		param := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[0])
		getBitsValue = func(i uint64) (uint64, bool) {
			val, null := param.GetValue(i)
			if null {
				return 0, true
			}
			return uint64(val), false
		}
	}

	// Second argument: on (string)
	onParam := vector.GenerateFunctionStrParameter(ivecs[1])

	// Third argument: off (string)
	offParam := vector.GenerateFunctionStrParameter(ivecs[2])

	// Optional fourth argument: separator (string, default ',')
	var separatorParam vector.FunctionParameterWrapper[types.Varlena]
	separatorProvided := len(ivecs) > 3
	if separatorProvided {
		separatorParam = vector.GenerateFunctionStrParameter(ivecs[3])
	}

	// Optional fifth argument: number_of_bits (int64, default 64)
	var numberOfBitsParam vector.FunctionParameterWrapper[int64]
	numberOfBitsProvided := len(ivecs) > 4
	if numberOfBitsProvided {
		numberOfBitsParam = vector.GenerateFunctionFixedTypeParameter[int64](ivecs[4])
	}

	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && selectList.Contains(i) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		// Extract bits value using the appropriate getter
		bitsUint, nullBits = getBitsValue(i)

		if nullBits {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		on, nullOn := onParam.GetStrValue(i)
		off, nullOff := offParam.GetStrValue(i)
		if nullOn || nullOff {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		// Get separator (default ',')
		separator := ","
		if separatorProvided && !ivecs[3].IsConstNull() {
			sep, nullSep := separatorParam.GetStrValue(i)
			if !nullSep {
				separator = functionUtil.QuickBytesToStr(sep)
			}
		}

		// Get number_of_bits (default 64, max 64)
		numberOfBits := int64(64)
		if numberOfBitsProvided && !ivecs[4].IsConstNull() {
			nBits, nullNBits := numberOfBitsParam.GetValue(i)
			if !nullNBits {
				if nBits < 1 {
					numberOfBits = 1
				} else if nBits > 64 {
					numberOfBits = 64
				} else {
					numberOfBits = nBits
				}
			}
		}

		// Build the result string
		var parts []string
		for j := int64(0); j < numberOfBits; j++ {
			if (bitsUint>>uint(j))&1 == 1 {
				parts = append(parts, functionUtil.QuickBytesToStr(on))
			} else {
				parts = append(parts, functionUtil.QuickBytesToStr(off))
			}
		}

		resultStr := strings.Join(parts, separator)
		if err := rs.AppendBytes([]byte(resultStr), false); err != nil {
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

func FormatWith2Args(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) (err error) {
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

// GetFormat returns a format string based on the type and locale.
// GET_FORMAT({DATE|TIME|DATETIME}, {'EUR'|'USA'|'JIS'|'ISO'|'INTERNAL'})
func GetFormat(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) (err error) {
	rs := vector.MustFunctionResult[types.Varlena](result)

	vs1 := vector.GenerateFunctionStrParameter(ivecs[0]) // type: DATE, TIME, or DATETIME
	vs2 := vector.GenerateFunctionStrParameter(ivecs[1]) // locale: EUR, USA, JIS, ISO, INTERNAL

	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := vs1.GetStrValue(i)
		v2, null2 := vs2.GetStrValue(i)

		if null1 || null2 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		typeStr := strings.ToUpper(functionUtil.QuickBytesToStr(v1))
		localeStr := strings.ToUpper(functionUtil.QuickBytesToStr(v2))

		var formatStr string
		switch typeStr {
		case "DATE":
			switch localeStr {
			case "USA":
				formatStr = "%m.%d.%Y"
			case "EUR":
				formatStr = "%d.%m.%Y"
			case "JIS":
				formatStr = "%Y-%m-%d"
			case "ISO":
				formatStr = "%Y-%m-%d"
			case "INTERNAL":
				formatStr = "%Y%m%d"
			default:
				// Invalid locale, return NULL
				if err = rs.AppendBytes(nil, true); err != nil {
					return err
				}
				continue
			}
		case "TIME":
			switch localeStr {
			case "USA":
				formatStr = "%h:%i:%s %p"
			case "EUR":
				formatStr = "%H.%i.%s"
			case "JIS":
				formatStr = "%H:%i:%s"
			case "ISO":
				formatStr = "%H:%i:%s"
			case "INTERNAL":
				formatStr = "%H%i%s"
			default:
				// Invalid locale, return NULL
				if err = rs.AppendBytes(nil, true); err != nil {
					return err
				}
				continue
			}
		case "DATETIME":
			switch localeStr {
			case "USA":
				formatStr = "%Y-%m-%d %H.%i.%s"
			case "EUR":
				formatStr = "%Y-%m-%d %H.%i.%s"
			case "JIS":
				formatStr = "%Y-%m-%d %H:%i:%s"
			case "ISO":
				formatStr = "%Y-%m-%d %H:%i:%s"
			case "INTERNAL":
				formatStr = "%Y%m%d%H%i%s"
			default:
				// Invalid locale, return NULL
				if err = rs.AppendBytes(nil, true); err != nil {
					return err
				}
				continue
			}
		default:
			// Invalid type, return NULL
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		if err = rs.AppendBytes([]byte(formatStr), false); err != nil {
			return err
		}
	}
	return nil
}

func FormatWith3Args(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) (err error) {
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
	maxUnixTimestampInt = 32536771199
)

func FromUnixTimeInt64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	rs := vector.MustFunctionResult[types.Datetime](result)
	vs := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[0])

	var d types.Datetime
	for i := uint64(0); i < uint64(length); i++ {
		v, null := vs.GetValue(i)

		if null || (v < 0 || v > maxUnixTimestampInt) {
			if err = rs.Append(d, true); err != nil {
				return err
			}
		} else {
			if err = rs.Append(types.DatetimeFromUnix(proc.GetSessionInfo().TimeZone, v), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func FromUnixTimeUint64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	rs := vector.MustFunctionResult[types.Datetime](result)
	vs := vector.GenerateFunctionFixedTypeParameter[uint64](ivecs[0])

	var d types.Datetime
	for i := uint64(0); i < uint64(length); i++ {
		v, null := vs.GetValue(i)

		if null || v > maxUnixTimestampInt {
			if err = rs.Append(d, true); err != nil {
				return err
			}
		} else {
			if err = rs.Append(types.DatetimeFromUnix(proc.GetSessionInfo().TimeZone, int64(v)), false); err != nil {
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

func FromUnixTimeFloat64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	rs := vector.MustFunctionResult[types.Datetime](result)
	vs := vector.GenerateFunctionFixedTypeParameter[float64](ivecs[0])
	rs.TempSetType(types.New(types.T_datetime, 0, 6))
	var d types.Datetime
	for i := uint64(0); i < uint64(length); i++ {
		v, null := vs.GetValue(i)

		if null || (v < 0 || v > maxUnixTimestampInt) {
			if err = rs.Append(d, true); err != nil {
				return err
			}
		} else {
			x, y := splitDecimalToIntAndFrac(v)
			if err = rs.Append(types.DatetimeFromUnixWithNsec(proc.GetSessionInfo().TimeZone, x, y), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func FromUnixTimeInt64Format(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	if !ivecs[1].IsConst() {
		return moerr.NewInvalidArg(proc.Ctx, "from_unixtime format", "not constant")
	}

	rs := vector.MustFunctionResult[types.Varlena](result)
	vs := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[0])
	formatMask, null1 := vector.GenerateFunctionStrParameter(ivecs[1]).GetStrValue(0)
	f := string(formatMask)

	var buf bytes.Buffer
	for i := uint64(0); i < uint64(length); i++ {
		v, null := vs.GetValue(i)

		if null || (v < 0 || v > maxUnixTimestampInt) || null1 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			buf.Reset()
			r := types.DatetimeFromUnix(proc.GetSessionInfo().TimeZone, v)
			if err = datetimeFormat(proc.Ctx, r, f, &buf); err != nil {
				return err
			}
			if err = rs.AppendBytes(buf.Bytes(), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func FromUnixTimeUint64Format(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	if !ivecs[1].IsConst() {
		return moerr.NewInvalidArg(proc.Ctx, "from_unixtime format", "not constant")
	}

	rs := vector.MustFunctionResult[types.Varlena](result)
	vs := vector.GenerateFunctionFixedTypeParameter[uint64](ivecs[0])
	formatMask, null1 := vector.GenerateFunctionStrParameter(ivecs[1]).GetStrValue(0)
	f := string(formatMask)

	var buf bytes.Buffer
	for i := uint64(0); i < uint64(length); i++ {
		v, null := vs.GetValue(i)

		if null1 || null || v > maxUnixTimestampInt {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			buf.Reset()
			r := types.DatetimeFromUnix(proc.GetSessionInfo().TimeZone, int64(v))
			if err = datetimeFormat(proc.Ctx, r, f, &buf); err != nil {
				return err
			}
			if err = rs.AppendBytes(buf.Bytes(), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func FromUnixTimeFloat64Format(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	if !ivecs[1].IsConst() {
		return moerr.NewInvalidArg(proc.Ctx, "from_unixtime format", "not constant")
	}

	rs := vector.MustFunctionResult[types.Varlena](result)
	vs := vector.GenerateFunctionFixedTypeParameter[float64](ivecs[0])
	formatMask, null1 := vector.GenerateFunctionStrParameter(ivecs[1]).GetStrValue(0)
	f := string(formatMask)

	var buf bytes.Buffer
	for i := uint64(0); i < uint64(length); i++ {
		v, null := vs.GetValue(i)

		if null || (v < 0 || v > maxUnixTimestampInt) || null1 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			buf.Reset()
			x, y := splitDecimalToIntAndFrac(v)
			r := types.DatetimeFromUnixWithNsec(proc.GetSessionInfo().TimeZone, x, y)
			if err = datetimeFormat(proc.Ctx, r, f, &buf); err != nil {
				return err
			}
			if err = rs.AppendBytes(buf.Bytes(), false); err != nil {
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

func StrCmp(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	return opBinaryStrStrToFixedWithErrorCheck[int8](ivecs, result, proc, length, strcmp, nil)
}

func strcmp(s1, s2 string) (int8, error) {
	if s1 == s2 {
		return 0, nil
	}
	if s1 < s2 {
		return -1, nil
	}
	return 1, nil
}

func SubStringWith2Args(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) (err error) {
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

func SubStringWith3Args(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
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
	case types.T_uint64, types.T_bit:
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

func SubStrIndex[T number](ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) (err error) {
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

func StartsWith(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	return opBinaryBytesBytesToFixed[bool](ivecs, result, proc, length, bytes.HasPrefix, selectList)
}

func EndsWith(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	return opBinaryBytesBytesToFixed[bool](ivecs, result, proc, length, bytes.HasSuffix, selectList)
}

// https://dev.mysql.com/doc/refman/8.0/en/encryption-functions.html#function_sha2
func SHA2Func(args []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) (err error) {
	res := vector.MustFunctionResult[types.Varlena](result)
	strs := vector.GenerateFunctionStrParameter(args[0])
	shaTypes := vector.GenerateFunctionFixedTypeParameter[int64](args[1])

	for i := uint64(0); i < uint64(length); i++ {
		str, isnull1 := strs.GetStrValue(i)
		shaType, isnull2 := shaTypes.GetValue(i)

		if isnull1 || isnull2 || !isSha2Family(shaType) {
			if err = res.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			var checksum []byte

			switch shaType {
			case 0, 256:
				sum256 := sha256.Sum256(str)
				checksum = sum256[:]
			case 224:
				sum224 := sha256.Sum224(str)
				checksum = sum224[:]
			case 384:
				sum384 := sha512.Sum384(str)
				checksum = sum384[:]
			case 512:
				sum512 := sha512.Sum512(str)
				checksum = sum512[:]
			default:
				panic("unexpected err happened in sha2 function")
			}
			checksum = []byte(hex.EncodeToString(checksum))
			if err = res.AppendBytes(checksum, false); err != nil {
				return err
			}
		}

	}

	return nil
}

// any one of 224 256 384 512 0 is valid
func isSha2Family(len int64) bool {
	return len == 0 || len == 224 || len == 256 || len == 384 || len == 512
}

func ExtractFromDate(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	extractFromDate := func(unit string, d types.Date) (uint32, error) {
		var r uint32
		switch unit {
		case "day":
			r = uint32(d.Day())
		case "week":
			r = uint32(d.WeekOfYear2())
		case "month":
			r = uint32(d.Month())
		case "quarter":
			r = d.Quarter()
		case "year_month":
			r = d.YearMonth()
		case "year":
			r = uint32(d.Year())
		default:
			return 0, moerr.NewInternalErrorNoCtx("invalid unit")
		}
		return r, nil
	}

	if !ivecs[0].IsConst() {
		return moerr.NewInternalError(proc.Ctx, "invalid input for extract")
	}

	return opBinaryStrFixedToFixedWithErrorCheck[types.Date, uint32](ivecs, result, proc, length, extractFromDate, selectList)
}

func ExtractFromDatetime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
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

// YearWeekDate: YEARWEEK(date, mode) - Returns year and week for a date as YYYYWW format.
// If mode is not provided, defaults to 0.
func YearWeekDate(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[int64](result)
	dates := vector.GenerateFunctionFixedTypeParameter[types.Date](ivecs[0])

	// Get mode (default 0 if not provided)
	mode := 0
	if len(ivecs) > 1 && !ivecs[1].IsConstNull() {
		mode = int(vector.MustFixedColWithTypeCheck[int64](ivecs[1])[0])
		// Clamp mode to valid range [0, 7]
		if mode < 0 {
			mode = 0
		} else if mode > 7 {
			mode = 7
		}
	}

	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && selectList.Contains(i) {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		date, null := dates.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		year, week := date.YearWeek(mode)
		// YEARWEEK returns year*100 + week
		result := int64(year)*100 + int64(week)
		if err := rs.Append(result, false); err != nil {
			return err
		}
	}
	return nil
}

// YearWeekDatetime: YEARWEEK(datetime, mode) - Returns year and week for a datetime as YYYYWW format.
func YearWeekDatetime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[int64](result)
	datetimes := vector.GenerateFunctionFixedTypeParameter[types.Datetime](ivecs[0])

	// Get mode (default 0 if not provided)
	mode := 0
	if len(ivecs) > 1 && !ivecs[1].IsConstNull() {
		mode = int(vector.MustFixedColWithTypeCheck[int64](ivecs[1])[0])
		// Clamp mode to valid range [0, 7]
		if mode < 0 {
			mode = 0
		} else if mode > 7 {
			mode = 7
		}
	}

	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && selectList.Contains(i) {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		dt, null := datetimes.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		year, week := dt.YearWeek(mode)
		// YEARWEEK returns year*100 + week
		result := int64(year)*100 + int64(week)
		if err := rs.Append(result, false); err != nil {
			return err
		}
	}
	return nil
}

// YearWeekTimestamp: YEARWEEK(timestamp, mode) - Returns year and week for a timestamp as YYYYWW format.
func YearWeekTimestamp(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[int64](result)
	timestamps := vector.GenerateFunctionFixedTypeParameter[types.Timestamp](ivecs[0])
	loc := proc.GetSessionInfo().TimeZone
	if loc == nil {
		loc = time.Local
	}

	// Get mode (default 0 if not provided)
	mode := 0
	if len(ivecs) > 1 && !ivecs[1].IsConstNull() {
		mode = int(vector.MustFixedColWithTypeCheck[int64](ivecs[1])[0])
		// Clamp mode to valid range [0, 7]
		if mode < 0 {
			mode = 0
		} else if mode > 7 {
			mode = 7
		}
	}

	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && selectList.Contains(i) {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		ts, null := timestamps.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		dt := ts.ToDatetime(loc)
		year, week := dt.YearWeek(mode)
		// YEARWEEK returns year*100 + week
		result := int64(year)*100 + int64(week)
		if err := rs.Append(result, false); err != nil {
			return err
		}
	}
	return nil
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

func ExtractFromTime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
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
		value = fmt.Sprintf("%d", int(t.MicroSec()))
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

func ExtractFromTimestamp(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	if !ivecs[0].IsConst() {
		return moerr.NewInternalError(proc.Ctx, "invalid input for extract")
	}

	p1 := vector.GenerateFunctionStrParameter(ivecs[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[types.Timestamp](ivecs[1])
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
	zone := proc.GetSessionInfo().TimeZone
	for i := uint64(0); i < uint64(length); i++ {
		v2, null2 := p2.GetValue(i)
		if null2 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			// Convert TIMESTAMP to DATETIME with full precision (scale=6) to preserve microseconds
			dt := v2.ToDatetime(zone)
			res, _ := extractFromDatetime(unit, dt)
			if err = rs.AppendBytes(functionUtil.QuickStrToBytes(res), false); err != nil {
				return err
			}
		}
	}

	return nil
}

func ExtractFromVarchar(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
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

func FindInSet(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	findInStrList := func(str, strlist string) uint64 {
		for j, s := range strings.Split(strlist, ",") {
			if s == str {
				return uint64(j + 1)
			}
		}
		return 0
	}

	return opBinaryStrStrToFixed[uint64](ivecs, result, proc, length, findInStrList, nil)
}

func Instr(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	return opBinaryStrStrToFixed[int64](ivecs, result, proc, length, instr.Single, nil)
}

func Left(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) (err error) {
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

func Right(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) (err error) {
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
			res := evalRight(functionUtil.QuickBytesToStr(v1), v2)
			if err = rs.AppendBytes(functionUtil.QuickStrToBytes(res), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func evalRight(str string, length int64) string {
	runeStr := []rune(str)
	rightLength := int(length)
	strLength := len(runeStr)

	if rightLength <= 0 {
		return ""
	}
	if rightLength >= strLength {
		return str
	}
	// Return the last rightLength characters
	return string(runeStr[strLength-rightLength:])
}

func Power(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) (err error) {
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

func TimeDiff[T types.Time | types.Datetime](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	return opBinaryFixedFixedToFixedWithErrorCheck[T, T, types.Time](ivecs, result, proc, length, timeDiff[T], selectList)
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
	tt := types.Time(tmpTime)
	hour, _, _, _, isNeg := tt.ClockFormat()
	if !types.ValidTime(uint64(hour), 0, 0) {
		return types.TimeFromClock(isNeg, types.MaxHourInTime, 59, 59, 0), nil
	}
	return tt, nil
}

// TimeDiffString: TIMEDIFF with string inputs - parses strings as TIME or DATETIME and returns the difference as TIME
func TimeDiffString(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	expr1Param := vector.GenerateFunctionStrParameter(ivecs[0])
	expr2Param := vector.GenerateFunctionStrParameter(ivecs[1])
	rs := vector.MustFunctionResult[types.Time](result)

	scale := int32(6) // Use max scale for string inputs
	rs.TempSetType(types.New(types.T_time, 0, scale))

	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && selectList.Contains(i) {
			if err := rs.Append(types.Time(0), true); err != nil {
				return err
			}
			continue
		}

		expr1Str, null1 := expr1Param.GetStrValue(i)
		expr2Str, null2 := expr2Param.GetStrValue(i)

		if null1 || null2 {
			if err := rs.Append(types.Time(0), true); err != nil {
				return err
			}
			continue
		}

		// Parse expr1 - try datetime first, then time
		var dt1 types.Datetime
		var err1 error
		expr1StrVal := functionUtil.QuickBytesToStr(expr1Str)
		dt1, err1 = types.ParseDatetime(expr1StrVal, scale)
		if err1 != nil {
			// If parsing as datetime fails, try as time
			time1, err2 := types.ParseTime(expr1StrVal, scale)
			if err2 != nil {
				if err := rs.Append(types.Time(0), true); err != nil {
					return err
				}
				continue
			}
			// Convert time to datetime (using today's date)
			dt1 = time1.ToDatetime(scale)
		}

		// Parse expr2 - try datetime first, then time
		var dt2 types.Datetime
		var err2 error
		expr2StrVal := functionUtil.QuickBytesToStr(expr2Str)
		dt2, err2 = types.ParseDatetime(expr2StrVal, scale)
		if err2 != nil {
			// If parsing as datetime fails, try as time
			time2, err3 := types.ParseTime(expr2StrVal, scale)
			if err3 != nil {
				if err := rs.Append(types.Time(0), true); err != nil {
					return err
				}
				continue
			}
			// Convert time to datetime (using today's date)
			dt2 = time2.ToDatetime(scale)
		}

		// Calculate difference: expr1 - expr2
		resultTime, err := timeDiff[types.Datetime](dt1, dt2)
		if err != nil {
			if err := rs.Append(types.Time(0), true); err != nil {
				return err
			}
			continue
		}

		if err := rs.Append(resultTime, false); err != nil {
			return err
		}
	}
	return nil
}

func TimestampDiff(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) (err error) {
	p1 := vector.GenerateFunctionStrParameter(ivecs[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[types.Datetime](ivecs[1])
	p3 := vector.GenerateFunctionFixedTypeParameter[types.Datetime](ivecs[2])
	rs := vector.MustFunctionResult[int64](result)

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

func MakeDateString(
	ivecs []*vector.Vector,
	result vector.FunctionResultWrapper,
	_ *process.Process,
	length int,
	selectList *FunctionSelectList,
) error {
	p1 := vector.GenerateFunctionStrParameter(ivecs[0])
	p2 := vector.GenerateFunctionStrParameter(ivecs[1])
	rs := vector.MustFunctionResult[types.Varlena](result)
	for i := uint64(0); i < uint64(length); i++ {
		yearStr, null1 := p1.GetStrValue(i)
		dayStr, null2 := p2.GetStrValue(i)

		if null1 || null2 {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			// null
			yearStrStr := functionUtil.QuickBytesToStr(yearStr)
			year, err := strconv.ParseInt(yearStrStr, 10, 64)
			if err != nil {
				yearFloat, err := strconv.ParseFloat(yearStrStr, 64)
				if err != nil {
					year = castBinaryArrayToInt(yearStr)
				} else {
					year = int64(yearFloat)
				}
			}
			day, err := strconv.ParseInt(functionUtil.QuickBytesToStr(dayStr), 10, 64)
			if err != nil {
				// parse as float64
				dayFloat, err := strconv.ParseFloat(functionUtil.QuickBytesToStr(dayStr), 64)
				if err != nil {
					day = castBinaryArrayToInt(dayStr)
				} else {
					day = int64(dayFloat)
				}
			}
			if day <= 0 || year < 0 || year > 9999 {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
				continue
			}

			if year < 70 {
				year += 2000
			} else if year < 100 {
				year += 1900
			}

			resDt := types.MakeDate(int32(year), 1, int32(day))

			if resDt.Year() > 9999 || resDt <= 0 {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
				continue
			}

			if err := rs.AppendBytes([]byte(resDt.String()), false); err != nil {
				return err
			}
		}
	}
	return nil
}

// makeTimeFromInt64: Helper function to create Time from int64 values
func makeTimeFromInt64(hour, minute, second int64, rs *vector.FunctionResult[types.Time], i uint64) error {
	// MySQL allows hour to be in range [0, 838] (TIME type range)
	// minute and second should be in range [0, 59]
	// If values are out of range, MySQL returns NULL
	if hour < 0 || hour > 838 {
		return rs.Append(types.Time(0), true)
	}

	if minute < 0 || minute > 59 || second < 0 || second > 59 {
		return rs.Append(types.Time(0), true)
	}

	// Create Time value using TimeFromClock
	// hour can be up to 838, so we use uint64 for hour
	timeValue := types.TimeFromClock(false, uint64(hour), uint8(minute), uint8(second), 0)

	// Validate the resulting time
	h := timeValue.Hour()
	if h < 0 {
		h = -h
	}
	if !types.ValidTime(uint64(h), 0, 0) {
		return rs.Append(types.Time(0), true)
	}

	return rs.Append(timeValue, false)
}

// MakeTime: MAKETIME(hour, minute, second) - Returns a time value calculated from the hour, minute, and second arguments.
func MakeTime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Time](result)

	// Check the types of input vectors and create appropriate parameter wrappers
	hourType := ivecs[0].GetType().Oid
	minuteType := ivecs[1].GetType().Oid
	secondType := ivecs[2].GetType().Oid

	// Create parameter wrappers based on types (these can be reused for all rows)
	var getHourValue func(uint64) (int64, bool)
	var getMinuteValue func(uint64) (int64, bool)
	var getSecondValue func(uint64) (int64, bool)

	// Setup hour parameter extractor
	switch hourType {
	case types.T_int8:
		hourParam := vector.GenerateFunctionFixedTypeParameter[int8](ivecs[0])
		getHourValue = func(i uint64) (int64, bool) {
			val, null := hourParam.GetValue(i)
			return int64(val), null
		}
	case types.T_int16:
		hourParam := vector.GenerateFunctionFixedTypeParameter[int16](ivecs[0])
		getHourValue = func(i uint64) (int64, bool) {
			val, null := hourParam.GetValue(i)
			return int64(val), null
		}
	case types.T_int32:
		hourParam := vector.GenerateFunctionFixedTypeParameter[int32](ivecs[0])
		getHourValue = func(i uint64) (int64, bool) {
			val, null := hourParam.GetValue(i)
			return int64(val), null
		}
	case types.T_int64:
		hourParam := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[0])
		getHourValue = func(i uint64) (int64, bool) {
			val, null := hourParam.GetValue(i)
			return val, null
		}
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		hourParam := vector.GenerateFunctionFixedTypeParameter[uint64](ivecs[0])
		getHourValue = func(i uint64) (int64, bool) {
			val, null := hourParam.GetValue(i)
			return int64(val), null
		}
	case types.T_float32, types.T_float64:
		hourParam := vector.GenerateFunctionFixedTypeParameter[float64](ivecs[0])
		getHourValue = func(i uint64) (int64, bool) {
			val, null := hourParam.GetValue(i)
			return int64(val), null // Truncate decimal part
		}
	default:
		return moerr.NewInvalidArgNoCtx("MAKETIME hour parameter", hourType)
	}

	// Setup minute parameter extractor
	switch minuteType {
	case types.T_int8:
		minuteParam := vector.GenerateFunctionFixedTypeParameter[int8](ivecs[1])
		getMinuteValue = func(i uint64) (int64, bool) {
			val, null := minuteParam.GetValue(i)
			return int64(val), null
		}
	case types.T_int16:
		minuteParam := vector.GenerateFunctionFixedTypeParameter[int16](ivecs[1])
		getMinuteValue = func(i uint64) (int64, bool) {
			val, null := minuteParam.GetValue(i)
			return int64(val), null
		}
	case types.T_int32:
		minuteParam := vector.GenerateFunctionFixedTypeParameter[int32](ivecs[1])
		getMinuteValue = func(i uint64) (int64, bool) {
			val, null := minuteParam.GetValue(i)
			return int64(val), null
		}
	case types.T_int64:
		minuteParam := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])
		getMinuteValue = func(i uint64) (int64, bool) {
			val, null := minuteParam.GetValue(i)
			return val, null
		}
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		minuteParam := vector.GenerateFunctionFixedTypeParameter[uint64](ivecs[1])
		getMinuteValue = func(i uint64) (int64, bool) {
			val, null := minuteParam.GetValue(i)
			return int64(val), null
		}
	case types.T_float32, types.T_float64:
		minuteParam := vector.GenerateFunctionFixedTypeParameter[float64](ivecs[1])
		getMinuteValue = func(i uint64) (int64, bool) {
			val, null := minuteParam.GetValue(i)
			return int64(val), null // Truncate decimal part
		}
	default:
		return moerr.NewInvalidArgNoCtx("MAKETIME minute parameter", minuteType)
	}

	// Setup second parameter extractor
	switch secondType {
	case types.T_int8:
		secondParam := vector.GenerateFunctionFixedTypeParameter[int8](ivecs[2])
		getSecondValue = func(i uint64) (int64, bool) {
			val, null := secondParam.GetValue(i)
			return int64(val), null
		}
	case types.T_int16:
		secondParam := vector.GenerateFunctionFixedTypeParameter[int16](ivecs[2])
		getSecondValue = func(i uint64) (int64, bool) {
			val, null := secondParam.GetValue(i)
			return int64(val), null
		}
	case types.T_int32:
		secondParam := vector.GenerateFunctionFixedTypeParameter[int32](ivecs[2])
		getSecondValue = func(i uint64) (int64, bool) {
			val, null := secondParam.GetValue(i)
			return int64(val), null
		}
	case types.T_int64:
		secondParam := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[2])
		getSecondValue = func(i uint64) (int64, bool) {
			val, null := secondParam.GetValue(i)
			return val, null
		}
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		secondParam := vector.GenerateFunctionFixedTypeParameter[uint64](ivecs[2])
		getSecondValue = func(i uint64) (int64, bool) {
			val, null := secondParam.GetValue(i)
			return int64(val), null
		}
	case types.T_float32, types.T_float64:
		secondParam := vector.GenerateFunctionFixedTypeParameter[float64](ivecs[2])
		getSecondValue = func(i uint64) (int64, bool) {
			val, null := secondParam.GetValue(i)
			return int64(val), null // Truncate decimal part
		}
	default:
		return moerr.NewInvalidArgNoCtx("MAKETIME second parameter", secondType)
	}

	// Process all rows
	for i := uint64(0); i < uint64(length); i++ {
		hourInt, null1 := getHourValue(i)
		minuteInt, null2 := getMinuteValue(i)
		secondInt, null3 := getSecondValue(i)

		if null1 || null2 || null3 {
			if err := rs.Append(types.Time(0), true); err != nil {
				return err
			}
			continue
		}

		if err := makeTimeFromInt64(hourInt, minuteInt, secondInt, rs, i); err != nil {
			return err
		}
	}
	return nil
}

// PeriodAdd: PERIOD_ADD(P, N) - Adds N months to period P (in the format YYMM or YYYYMM). Returns a value in the format YYYYMM.
func PeriodAdd(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[int64](result)

	// P can be int64, uint64, or float64 (period in YYMM or YYYYMM format)
	// N can be int64, uint64, or float64 (number of months to add)
	periodType := ivecs[0].GetType().Oid
	monthsType := ivecs[1].GetType().Oid

	// Create parameter extractors based on types
	var getPeriodValue func(uint64) (int64, bool)
	var getMonthsValue func(uint64) (int64, bool)

	// Setup period parameter extractor
	switch periodType {
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
		periodParam := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[0])
		getPeriodValue = func(i uint64) (int64, bool) {
			val, null := periodParam.GetValue(i)
			return val, null
		}
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		periodParam := vector.GenerateFunctionFixedTypeParameter[uint64](ivecs[0])
		getPeriodValue = func(i uint64) (int64, bool) {
			val, null := periodParam.GetValue(i)
			return int64(val), null
		}
	case types.T_float32, types.T_float64:
		periodParam := vector.GenerateFunctionFixedTypeParameter[float64](ivecs[0])
		getPeriodValue = func(i uint64) (int64, bool) {
			val, null := periodParam.GetValue(i)
			return int64(val), null // Truncate decimal part
		}
	default:
		return moerr.NewInvalidArgNoCtx("PERIOD_ADD period parameter", periodType)
	}

	// Setup months parameter extractor
	switch monthsType {
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
		monthsParam := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])
		getMonthsValue = func(i uint64) (int64, bool) {
			val, null := monthsParam.GetValue(i)
			return val, null
		}
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		monthsParam := vector.GenerateFunctionFixedTypeParameter[uint64](ivecs[1])
		getMonthsValue = func(i uint64) (int64, bool) {
			val, null := monthsParam.GetValue(i)
			return int64(val), null
		}
	case types.T_float32, types.T_float64:
		monthsParam := vector.GenerateFunctionFixedTypeParameter[float64](ivecs[1])
		getMonthsValue = func(i uint64) (int64, bool) {
			val, null := monthsParam.GetValue(i)
			return int64(val), null // Truncate decimal part
		}
	default:
		return moerr.NewInvalidArgNoCtx("PERIOD_ADD months parameter", monthsType)
	}

	for i := uint64(0); i < uint64(length); i++ {
		period, null1 := getPeriodValue(i)
		months, null2 := getMonthsValue(i)

		if null1 || null2 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		// Parse period P (YYMM or YYYYMM format) using helper function
		year, month, err := parsePeriod(period)
		if err != nil {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		// Validate year and month
		if year < 0 || year > 9999 || month < 1 || month > 12 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		// Add months to the date
		// Create a date from year and month (use day 1)
		date := types.DateFromCalendar(year, month, 1)
		dt := date.ToDatetime()

		// Add months using AddInterval
		resultDt, success := dt.AddInterval(months, types.Month, types.DateTimeType)
		if !success {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		// Extract year and month from result
		resultDate := resultDt.ToDate()
		resultYear := resultDate.Year()
		resultMonth := resultDate.Month()

		// Format as YYYYMM
		resultPeriod := int64(resultYear)*100 + int64(resultMonth)

		if err := rs.Append(resultPeriod, false); err != nil {
			return err
		}
	}
	return nil
}

// parsePeriod parses a period value (YYMM or YYYYMM format) and returns year and month.
// Returns (year, month, error). If period is invalid, returns (0, 0, error).
func parsePeriod(period int64) (int32, uint8, error) {
	// Handle negative periods (MySQL returns NULL for negative periods)
	if period < 0 {
		return 0, 0, moerr.NewInvalidArgNoCtx("PERIOD", "negative period")
	}

	periodStr := fmt.Sprintf("%d", period)
	periodLen := len(periodStr)

	// Pad 3-digit periods to 4 digits (e.g., 802 -> 0802)
	if periodLen == 3 {
		periodStr = "0" + periodStr
		periodLen = 4
	}

	var year int32
	var month uint8

	if periodLen == 4 {
		// YYMM format (e.g., 0802)
		yy, err := strconv.ParseInt(periodStr[:2], 10, 32)
		if err != nil {
			return 0, 0, err
		}
		mm, err := strconv.ParseInt(periodStr[2:], 10, 32)
		if err != nil || mm < 1 || mm > 12 {
			return 0, 0, moerr.NewInvalidArgNoCtx("PERIOD", "invalid month")
		}
		// Convert YY to YYYY: 00-69 -> 2000-2069, 70-99 -> 1970-1999
		if yy >= 0 && yy <= 69 {
			year = int32(2000 + yy)
		} else if yy >= 70 && yy <= 99 {
			year = int32(1900 + yy)
		} else {
			return 0, 0, moerr.NewInvalidArgNoCtx("PERIOD", "invalid year")
		}
		month = uint8(mm)
	} else if periodLen == 6 {
		// YYYYMM format (e.g., 200802)
		yyyy, err := strconv.ParseInt(periodStr[:4], 10, 32)
		if err != nil {
			return 0, 0, err
		}
		mm, err := strconv.ParseInt(periodStr[4:], 10, 32)
		if err != nil || mm < 1 || mm > 12 {
			return 0, 0, moerr.NewInvalidArgNoCtx("PERIOD", "invalid month")
		}
		year = int32(yyyy)
		month = uint8(mm)
	} else {
		// Invalid period format (not 3, 4, or 6 digits)
		return 0, 0, moerr.NewInvalidArgNoCtx("PERIOD", "invalid format")
	}

	// Validate year range
	if year < 0 || year > 9999 {
		return 0, 0, moerr.NewInvalidArgNoCtx("PERIOD", "year out of range")
	}

	return year, month, nil
}

// PeriodDiff: PERIOD_DIFF(P1, P2) - Returns the number of months between periods P1 and P2.
// P1 and P2 should be in the format YYMM or YYYYMM.
func PeriodDiff(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[int64](result)

	// P1 and P2 can be int64, uint64, or float64 (period in YYMM or YYYYMM format)
	period1Type := ivecs[0].GetType().Oid
	period2Type := ivecs[1].GetType().Oid

	// Create parameter extractors based on types
	var getPeriod1Value func(uint64) (int64, bool)
	var getPeriod2Value func(uint64) (int64, bool)

	// Setup period1 parameter extractor
	switch period1Type {
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
		period1Param := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[0])
		getPeriod1Value = func(i uint64) (int64, bool) {
			val, null := period1Param.GetValue(i)
			return val, null
		}
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		period1Param := vector.GenerateFunctionFixedTypeParameter[uint64](ivecs[0])
		getPeriod1Value = func(i uint64) (int64, bool) {
			val, null := period1Param.GetValue(i)
			return int64(val), null
		}
	case types.T_float32, types.T_float64:
		period1Param := vector.GenerateFunctionFixedTypeParameter[float64](ivecs[0])
		getPeriod1Value = func(i uint64) (int64, bool) {
			val, null := period1Param.GetValue(i)
			return int64(val), null // Truncate decimal part
		}
	default:
		return moerr.NewInvalidArgNoCtx("PERIOD_DIFF period1 parameter", period1Type)
	}

	// Setup period2 parameter extractor
	switch period2Type {
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
		period2Param := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])
		getPeriod2Value = func(i uint64) (int64, bool) {
			val, null := period2Param.GetValue(i)
			return val, null
		}
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		period2Param := vector.GenerateFunctionFixedTypeParameter[uint64](ivecs[1])
		getPeriod2Value = func(i uint64) (int64, bool) {
			val, null := period2Param.GetValue(i)
			return int64(val), null
		}
	case types.T_float32, types.T_float64:
		period2Param := vector.GenerateFunctionFixedTypeParameter[float64](ivecs[1])
		getPeriod2Value = func(i uint64) (int64, bool) {
			val, null := period2Param.GetValue(i)
			return int64(val), null // Truncate decimal part
		}
	default:
		return moerr.NewInvalidArgNoCtx("PERIOD_DIFF period2 parameter", period2Type)
	}

	for i := uint64(0); i < uint64(length); i++ {
		period1, null1 := getPeriod1Value(i)
		period2, null2 := getPeriod2Value(i)

		if null1 || null2 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		// Parse period1
		year1, month1, err1 := parsePeriod(period1)
		if err1 != nil {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		// Parse period2
		year2, month2, err2 := parsePeriod(period2)
		if err2 != nil {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		// Calculate difference in months: (year1 - year2) * 12 + (month1 - month2)
		diffMonths := (year1-year2)*12 + int32(month1) - int32(month2)

		if err := rs.Append(int64(diffMonths), false); err != nil {
			return err
		}
	}
	return nil
}

// SecToTime: SEC_TO_TIME(seconds) - Returns the seconds argument, converted to hours, minutes, and seconds, as a TIME value.
func SecToTime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Time](result)

	// seconds can be int64, uint64, or float64
	secondsType := ivecs[0].GetType().Oid

	// Create parameter extractor based on type
	var getSecondsValue func(uint64) (int64, bool)

	switch secondsType {
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
		secondsParam := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[0])
		getSecondsValue = func(i uint64) (int64, bool) {
			val, null := secondsParam.GetValue(i)
			return val, null
		}
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		secondsParam := vector.GenerateFunctionFixedTypeParameter[uint64](ivecs[0])
		getSecondsValue = func(i uint64) (int64, bool) {
			val, null := secondsParam.GetValue(i)
			return int64(val), null
		}
	case types.T_float32, types.T_float64:
		secondsParam := vector.GenerateFunctionFixedTypeParameter[float64](ivecs[0])
		getSecondsValue = func(i uint64) (int64, bool) {
			val, null := secondsParam.GetValue(i)
			return int64(val), null // Truncate decimal part
		}
	default:
		return moerr.NewInvalidArgNoCtx("SEC_TO_TIME seconds parameter", secondsType)
	}

	// MySQL TIME range: -838:59:59 to 838:59:59
	// In seconds: -3020399 to 3020399
	const maxTimeSeconds = 3020399 // 838*3600 + 59*60 + 59
	const minTimeSeconds = -3020399

	for i := uint64(0); i < uint64(length); i++ {
		seconds, null := getSecondsValue(i)

		if null {
			if err := rs.Append(types.Time(0), true); err != nil {
				return err
			}
			continue
		}

		// Check if seconds is within valid TIME range
		if seconds > maxTimeSeconds || seconds < minTimeSeconds {
			if err := rs.Append(types.Time(0), true); err != nil {
				return err
			}
			continue
		}

		// Convert seconds to hours, minutes, and seconds
		// Handle negative values
		isNegative := seconds < 0
		if isNegative {
			seconds = -seconds
		}

		hours := seconds / 3600
		remainingSeconds := seconds % 3600
		minutes := remainingSeconds / 60
		secs := remainingSeconds % 60

		// Check if hours exceed MySQL TIME limit (838:59:59)
		// MySQL TIME range is -838:59:59 to 838:59:59
		if hours > 838 {
			if err := rs.Append(types.Time(0), true); err != nil {
				return err
			}
			continue
		}

		// Create TIME value using TimeFromClock
		// isNegative: true if the time should be negative
		timeValue := types.TimeFromClock(isNegative, uint64(hours), uint8(minutes), uint8(secs), 0)

		// Validate the resulting time
		h := timeValue.Hour()
		if h < 0 {
			h = -h
		}
		if !types.ValidTime(uint64(h), uint64(timeValue.Minute()), uint64(timeValue.Sec())) {
			if err := rs.Append(types.Time(0), true); err != nil {
				return err
			}
			continue
		}

		if err := rs.Append(timeValue, false); err != nil {
			return err
		}
	}
	return nil
}

func Replace(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) (err error) {
	p1 := vector.GenerateFunctionStrParameter(ivecs[0])
	p2 := vector.GenerateFunctionStrParameter(ivecs[1])
	p3 := vector.GenerateFunctionStrParameter(ivecs[2])
	rs := vector.MustFunctionResult[types.Varlena](result)

	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		v2, null2 := p2.GetStrValue(i)
		v3, null3 := p3.GetStrValue(i)

		if null1 || null2 || null3 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
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

func Insert(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) (err error) {
	p1 := vector.GenerateFunctionStrParameter(ivecs[0])              // str
	p2 := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1]) // pos
	p3 := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[2]) // len
	p4 := vector.GenerateFunctionStrParameter(ivecs[3])              // newstr
	rs := vector.MustFunctionResult[types.Varlena](result)

	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		v2, null2 := p2.GetValue(i)
		v3, null3 := p3.GetValue(i)
		v4, null4 := p4.GetStrValue(i)

		if null1 || null2 || null3 || null4 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			str := functionUtil.QuickBytesToStr(v1)
			pos := v2
			replaceLen := v3
			newstr := functionUtil.QuickBytesToStr(v4)

			// Convert to runes for proper character handling
			runes := []rune(str)
			strLen := int64(len(runes))

			// MySQL INSERT behavior:
			// - If pos <= 0 or pos > string length, return original string
			// - If replaceLen <= 0, insert newstr at position pos without removing anything
			// - Otherwise, replace replaceLen characters starting at pos with newstr
			// - Position is 1-based

			var result string
			if pos <= 0 || pos > strLen {
				// Invalid position, return original string
				result = str
			} else if replaceLen <= 0 {
				// Insert without removing
				posIdx := int(pos - 1) // Convert to 0-based index
				if posIdx >= len(runes) {
					result = str + newstr
				} else {
					result = string(runes[:posIdx]) + newstr + string(runes[posIdx:])
				}
			} else {
				// Replace replaceLen characters starting at pos
				posIdx := int(pos - 1) // Convert to 0-based index
				endIdx := posIdx + int(replaceLen)
				if endIdx > len(runes) {
					endIdx = len(runes)
				}
				if posIdx >= len(runes) {
					result = str + newstr
				} else {
					result = string(runes[:posIdx]) + newstr + string(runes[endIdx:])
				}
			}

			if err = rs.AppendBytes(functionUtil.QuickStrToBytes(result), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func Trim(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
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
				return moerr.NewNotSupportedf(proc.Ctx, "trim type %s", v1Str)
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

// SPLIT PART

func SplitPart(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
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
	if len(strSlice) < int(cnt) {
		return "", true
	}
	return strSlice[cnt-1], false
}

func InnerProductArray[T types.RealNumbers](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opBinaryBytesBytesToFixedWithErrorCheck[float64](ivecs, result, proc, length, func(v1, v2 []byte) (out float64, err error) {
		_v1 := types.BytesToArray[T](v1)
		_v2 := types.BytesToArray[T](v2)

		return moarray.InnerProduct[T](_v1, _v2)
	}, selectList)
}

func CosineSimilarityArray[T types.RealNumbers](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opBinaryBytesBytesToFixedWithErrorCheck[float64](ivecs, result, proc, length, func(v1, v2 []byte) (out float64, err error) {
		_v1 := types.BytesToArray[T](v1)
		_v2 := types.BytesToArray[T](v2)
		return moarray.CosineSimilarity[T](_v1, _v2)
	}, selectList)
}

func L2DistanceArray[T types.RealNumbers](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opBinaryBytesBytesToFixedWithErrorCheck[float64](ivecs, result, proc, length, func(v1, v2 []byte) (out float64, err error) {
		_v1 := types.BytesToArray[T](v1)
		_v2 := types.BytesToArray[T](v2)
		return moarray.L2Distance[T](_v1, _v2)
	}, selectList)
}

func L2DistanceSqArray[T types.RealNumbers](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opBinaryBytesBytesToFixedWithErrorCheck[float64](ivecs, result, proc, length, func(v1, v2 []byte) (out float64, err error) {
		_v1 := types.BytesToArray[T](v1)
		_v2 := types.BytesToArray[T](v2)
		return moarray.L2DistanceSq[T](_v1, _v2)
	}, selectList)
}

func CosineDistanceArray[T types.RealNumbers](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opBinaryBytesBytesToFixedWithErrorCheck[float64](ivecs, result, proc, length, func(v1, v2 []byte) (out float64, err error) {
		_v1 := types.BytesToArray[T](v1)
		_v2 := types.BytesToArray[T](v2)
		return moarray.CosineDistance[T](_v1, _v2)
	}, selectList)
}

func castBinaryArrayToInt(array []uint8) int64 {
	var result int64
	for i, value := range array {
		result += int64(value) << uint(8*(len(array)-i-1))
	}
	return result
}

// generateAESKey generates a 16-byte AES key from the input key using SHA1
// MySQL's AES_ENCRYPT uses SHA1 hash of the key, taking the first 16 bytes
func generateAESKey(key []byte) []byte {
	hash := sha1.Sum(key)
	return hash[:16] // AES-128 requires 16-byte key
}

// pkcs7Padding adds PKCS7 padding to the data
func pkcs7Padding(data []byte, blockSize int) []byte {
	padding := blockSize - len(data)%blockSize
	padtext := make([]byte, padding)
	for i := range padtext {
		padtext[i] = byte(padding)
	}
	return append(data, padtext...)
}

// pkcs7Unpadding removes PKCS7 padding from the data
func pkcs7Unpadding(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, moerr.NewInvalidInputNoCtx("invalid padding")
	}
	padding := int(data[len(data)-1])
	if padding > len(data) || padding == 0 {
		return nil, moerr.NewInvalidInputNoCtx("invalid padding")
	}
	// Verify padding
	for i := len(data) - padding; i < len(data); i++ {
		if data[i] != byte(padding) {
			return nil, moerr.NewInvalidInputNoCtx("invalid padding")
		}
	}
	return data[:len(data)-padding], nil
}

// encryptECB encrypts data using AES-128-ECB mode
func encryptECB(plaintext, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	// Add PKCS7 padding
	padded := pkcs7Padding(plaintext, aes.BlockSize)

	// Encrypt each block independently (ECB mode)
	ciphertext := make([]byte, len(padded))
	for i := 0; i < len(padded); i += aes.BlockSize {
		block.Encrypt(ciphertext[i:i+aes.BlockSize], padded[i:i+aes.BlockSize])
	}

	return ciphertext, nil
}

// decryptECB decrypts data using AES-128-ECB mode
func decryptECB(ciphertext, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	// Check that ciphertext length is a multiple of block size
	if len(ciphertext)%aes.BlockSize != 0 {
		return nil, moerr.NewInvalidInputNoCtx("invalid ciphertext length")
	}

	// Decrypt each block independently (ECB mode)
	plaintext := make([]byte, len(ciphertext))
	for i := 0; i < len(ciphertext); i += aes.BlockSize {
		block.Decrypt(plaintext[i:i+aes.BlockSize], ciphertext[i:i+aes.BlockSize])
	}

	// Remove PKCS7 padding
	return pkcs7Unpadding(plaintext)
}

// AESEncrypt: AES_ENCRYPT(str, key_str) - Encrypts a string using AES encryption
func AESEncrypt(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	strParam := vector.GenerateFunctionStrParameter(ivecs[0])
	keyParam := vector.GenerateFunctionStrParameter(ivecs[1])

	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && selectList.Contains(i) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		str, nullStr := strParam.GetStrValue(i)
		key, nullKey := keyParam.GetStrValue(i)

		if nullStr || nullKey {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		// Generate AES key from the key string
		aesKey := generateAESKey(key)

		// Encrypt using AES-128-ECB
		ciphertext, err := encryptECB(str, aesKey)
		if err != nil {
			// On error, return NULL (MySQL behavior)
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		if err := rs.AppendBytes(ciphertext, false); err != nil {
			return err
		}
	}

	return nil
}

// AESDecrypt: AES_DECRYPT(crypt_str, key_str) - Decrypts an encrypted string using AES decryption
func AESDecrypt(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	cryptParam := vector.GenerateFunctionStrParameter(ivecs[0])
	keyParam := vector.GenerateFunctionStrParameter(ivecs[1])

	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && selectList.Contains(i) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		crypt, nullCrypt := cryptParam.GetStrValue(i)
		key, nullKey := keyParam.GetStrValue(i)

		if nullCrypt || nullKey {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		// Generate AES key from the key string
		aesKey := generateAESKey(key)

		// Decrypt using AES-128-ECB
		plaintext, err := decryptECB(crypt, aesKey)
		if err != nil {
			// On error, return NULL (MySQL behavior)
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		if err := rs.AppendBytes(plaintext, false); err != nil {
			return err
		}
	}

	return nil
}
