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
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
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

	logutil.Debug("FaultPoint",
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
	constraints.Integer | constraints.Float | types.Decimal64 | types.Decimal128 | types.Decimal256
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

func ceilDecimal256(x types.Decimal256, digits int64, scale int32, isConst bool) types.Decimal256 {
	if digits > 65 {
		digits = 65
	}
	if digits < -65 {
		digits = -65
	}
	return x.Ceil(scale, int32(digits), isConst)
}

func CeilDecimal256(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	scale := ivecs[0].GetType().Scale
	if len(ivecs) > 1 {
		digit := vector.MustFixedColWithTypeCheck[int64](ivecs[1])
		if len(digit) > 0 && int32(digit[0]) <= scale-65 {
			return moerr.NewOutOfRangef(proc.Ctx, "decimal256", "ceil(decimal256(65,%v),%v)", scale, digit[0])
		}
	}
	cb := func(x types.Decimal256, digits int64) types.Decimal256 {
		return ceilDecimal256(x, digits, scale, result.GetResultVector().GetType().Scale != scale)
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

func floorDecimal256(x types.Decimal256, digits int64, scale int32, isConst bool) types.Decimal256 {
	if digits > 65 {
		digits = 65
	}
	if digits < -65 {
		digits = -65
	}
	return x.Floor(scale, int32(digits), isConst)
}

func FloorDecimal256(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	scale := ivecs[0].GetType().Scale
	if len(ivecs) > 1 {
		digit := vector.MustFixedColWithTypeCheck[int64](ivecs[1])
		if len(digit) > 0 && int32(digit[0]) <= scale-65 {
			return moerr.NewOutOfRangef(proc.Ctx, "decimal256", "floor(decimal256(65,%v),%v)", scale, digit[0])
		}
	}
	cb := func(x types.Decimal256, digits int64) types.Decimal256 {
		return floorDecimal256(x, digits, scale, result.GetResultVector().GetType().Scale != scale)
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

func truncateDecimal256(x types.Decimal256, digits int64, scale int32, isConst bool) types.Decimal256 {
	if digits > 65 {
		digits = 65
	}
	if digits < -65 {
		digits = -65
	}
	if int32(digits) >= scale {
		return x
	}
	k := scale - int32(digits)
	if k > 65 {
		k = 65
	}
	y, _, _ := x.Mod(types.Decimal256{B0_63: 1}, k, 0)
	x, _ = x.Sub256(y)
	if isConst {
		if int32(digits) < 0 {
			k = scale
		}
		x, _ = x.Scale(-k)
	}
	return x
}

func TruncateDecimal256(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	scale := ivecs[0].GetType().Scale
	cb := func(x types.Decimal256, digits int64) types.Decimal256 {
		return truncateDecimal256(x, digits, scale, result.GetResultVector().GetType().Scale != scale)
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

func roundDecimal256(x types.Decimal256, digits int64, scale int32, isConst bool) types.Decimal256 {
	if digits > 65 {
		digits = 65
	}
	if digits < -65 {
		digits = -65
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

func RoundDecimal256(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	scale := ivecs[0].GetType().Scale
	cb := func(x types.Decimal256, digits int64) types.Decimal256 {
		return roundDecimal256(x, digits, scale, result.GetResultVector().GetType().Scale != scale)
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
		types.Decimal256 |
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

func decimal256ToInt64ForUnix(v types.Decimal256) (int64, error) {
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

func decimal256UnixTimeParts(v types.Decimal256, scale int32) (sec int64, nsec int64, ok bool, err error) {
	if v.Sign() {
		return 0, 0, false, nil
	}

	whole, err := v.ScaleTruncate(-scale)
	if err != nil {
		return 0, 0, false, nil
	}
	sec, err = decimal256ToInt64ForUnix(whole)
	if err != nil {
		return 0, 0, false, nil
	}
	if sec < 0 || sec > maxUnixTimestampInt {
		return 0, 0, false, nil
	}

	if scale <= 0 {
		return sec, 0, true, nil
	}

	wholeScaled, err := whole.Scale(scale)
	if err != nil {
		return 0, 0, false, nil
	}
	frac, err := v.Sub256(wholeScaled)
	if err != nil {
		return 0, 0, false, err
	}
	if frac.B0_63 == 0 && frac.B64_127 == 0 && frac.B128_191 == 0 && frac.B192_255 == 0 {
		return sec, 0, true, nil
	}

	if scale > 9 {
		frac, err = frac.ScaleTruncate(-(scale - 9))
	} else if scale < 9 {
		frac, err = frac.Scale(9 - scale)
	}
	if err != nil {
		return 0, 0, false, err
	}
	nsec, err = decimal256ToInt64ForUnix(frac)
	if err != nil {
		return 0, 0, false, err
	}
	if sec == maxUnixTimestampInt && nsec > 0 {
		return 0, 0, false, nil
	}
	return sec, nsec, true, nil
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

func FromUnixTimeDecimal256(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	rs := vector.MustFunctionResult[types.Datetime](result)
	vs := vector.GenerateFunctionFixedTypeParameter[types.Decimal256](ivecs[0])
	scale := ivecs[0].GetType().Scale
	rs.TempSetType(types.New(types.T_datetime, 0, 6))
	var d types.Datetime
	for i := uint64(0); i < uint64(length); i++ {
		v, null := vs.GetValue(i)
		sec, nsec, ok, convErr := decimal256UnixTimeParts(v, scale)
		if convErr != nil {
			return convErr
		}

		if null || !ok {
			if err = rs.Append(d, true); err != nil {
				return err
			}
		} else {
			if err = rs.Append(types.DatetimeFromUnixWithNsec(proc.GetSessionInfo().TimeZone, sec, nsec), false); err != nil {
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

func FromUnixTimeDecimal256Format(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	if !ivecs[1].IsConst() {
		return moerr.NewInvalidArg(proc.Ctx, "from_unixtime format", "not constant")
	}

	rs := vector.MustFunctionResult[types.Varlena](result)
	vs := vector.GenerateFunctionFixedTypeParameter[types.Decimal256](ivecs[0])
	scale := ivecs[0].GetType().Scale
	formatMask, null1 := vector.GenerateFunctionStrParameter(ivecs[1]).GetStrValue(0)
	f := string(formatMask)

	var buf bytes.Buffer
	for i := uint64(0); i < uint64(length); i++ {
		v, null := vs.GetValue(i)
		sec, nsec, ok, convErr := decimal256UnixTimeParts(v, scale)
		if convErr != nil {
			return convErr
		}

		if null || !ok || null1 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			buf.Reset()
			r := types.DatetimeFromUnixWithNsec(proc.GetSessionInfo().TimeZone, sec, nsec)
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

func StDistance(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opBinaryBytesBytesToFixedWithErrorCheck[float64](ivecs, result, proc, length, func(v1, v2 []byte) (float64, error) {
		return geometryDistance(v1, v2)
	}, selectList)
}

func StContains(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opBinaryBytesBytesToFixedWithErrorCheck[bool](ivecs, result, proc, length, func(v1, v2 []byte) (bool, error) {
		return geometryContains(v1, v2)
	}, selectList)
}

func StWithin(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opBinaryBytesBytesToFixedWithErrorCheck[bool](ivecs, result, proc, length, func(v1, v2 []byte) (bool, error) {
		return geometryWithin(v1, v2)
	}, selectList)
}

func StIntersects(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opBinaryBytesBytesToFixedWithErrorCheck[bool](ivecs, result, proc, length, func(v1, v2 []byte) (bool, error) {
		return geometryIntersects(v1, v2)
	}, selectList)
}

func StDisjoint(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opBinaryBytesBytesToFixedWithErrorCheck[bool](ivecs, result, proc, length, func(v1, v2 []byte) (bool, error) {
		return geometryDisjoint(v1, v2)
	}, selectList)
}

func StTouches(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opBinaryBytesBytesToFixedWithErrorCheck[bool](ivecs, result, proc, length, func(v1, v2 []byte) (bool, error) {
		return geometryTouches(v1, v2)
	}, selectList)
}

func StCrosses(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opBinaryBytesBytesToFixedWithErrorCheck[bool](ivecs, result, proc, length, func(v1, v2 []byte) (bool, error) {
		return geometryCrosses(v1, v2)
	}, selectList)
}

func StOverlaps(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opBinaryBytesBytesToFixedWithErrorCheck[bool](ivecs, result, proc, length, func(v1, v2 []byte) (bool, error) {
		return geometryOverlaps(v1, v2)
	}, selectList)
}

func StEquals(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opBinaryBytesBytesToFixedWithErrorCheck[bool](ivecs, result, proc, length, func(v1, v2 []byte) (bool, error) {
		return geometryEquals(v1, v2)
	}, selectList)
}

func StCovers(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opBinaryBytesBytesToFixedWithErrorCheck[bool](ivecs, result, proc, length, func(v1, v2 []byte) (bool, error) {
		return geometryCovers(v1, v2)
	}, selectList)
}

func StCoveredBy(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opBinaryBytesBytesToFixedWithErrorCheck[bool](ivecs, result, proc, length, func(v1, v2 []byte) (bool, error) {
		return geometryCoveredBy(v1, v2)
	}, selectList)
}

type geometryPoint2D struct {
	x float64
	y float64
}

type geometryPolygon2D struct {
	outer []geometryPoint2D
	holes [][]geometryPoint2D
}

type geometryParamInterval struct {
	start float64
	end   float64
}

const (
	stDistanceSupportedPairsError       = "ST_DISTANCE only supports POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, or MULTIPOLYGON inputs"
	stTouchesSupportedPairsError        = "ST_TOUCHES only supports POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, or GEOMETRYCOLLECTION inputs"
	stOverlapsSupportedPairsError       = "ST_OVERLAPS only supports POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, or GEOMETRYCOLLECTION inputs"
	stEqualsSupportedPairsError         = "ST_EQUALS only supports POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, or GEOMETRYCOLLECTION inputs"
	stCrossesSupportedPairsError        = "ST_CROSSES only supports POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, or GEOMETRYCOLLECTION inputs"
	differentGeometrySRIDsErrorTemplate = "Binary geometry function %s given two geometries of different srids: %d and %d, which should have been identical."
)

func isSimpleGeometryType(typeName string) bool {
	switch typeName {
	case "POINT", "LINESTRING", "POLYGON":
		return true
	default:
		return false
	}
}

func isIntersectsSupportedGeometryType(typeName string) bool {
	return isSimpleGeometryType(typeName) || typeName == "MULTIPOINT" || typeName == "MULTILINESTRING" || typeName == "MULTIPOLYGON" || typeName == "GEOMETRYCOLLECTION"
}

func isDistanceSupportedGeometryType(typeName string) bool {
	return isSimpleGeometryType(typeName) || typeName == "MULTIPOINT" || typeName == "MULTILINESTRING" || typeName == "MULTIPOLYGON"
}

func geometrySRIDFromPayload(payload []byte) (uint32, error) {
	_, srid, sridDefined, err := decodeGeometryPayload(payload)
	if err != nil {
		return 0, err
	}
	if !sridDefined {
		return 0, nil
	}
	return srid, nil
}

func ensureMatchingGeometrySRID(functionName string, left, right []byte) error {
	leftSRID, err := geometrySRIDFromPayload(left)
	if err != nil {
		return err
	}
	rightSRID, err := geometrySRIDFromPayload(right)
	if err != nil {
		return err
	}
	if leftSRID != rightSRID {
		return moerr.NewInvalidInputNoCtxf(differentGeometrySRIDsErrorTemplate, functionName, leftSRID, rightSRID)
	}
	return nil
}

func geometryDistance(left, right []byte) (float64, error) {
	leftType, err := geometryTypeNameFromPayload(left)
	if err != nil {
		return 0, err
	}
	rightType, err := geometryTypeNameFromPayload(right)
	if err != nil {
		return 0, err
	}
	if isDistanceSupportedGeometryType(leftType) && isDistanceSupportedGeometryType(rightType) {
		if err := ensureMatchingGeometrySRID("ST_DISTANCE", left, right); err != nil {
			return 0, err
		}
	}
	if leftType == "MULTIPOINT" || leftType == "MULTILINESTRING" || leftType == "MULTIPOLYGON" {
		return multiGeometryDistance(left, right)
	}
	if rightType == "MULTIPOINT" || rightType == "MULTILINESTRING" || rightType == "MULTIPOLYGON" {
		return multiGeometryDistance(right, left)
	}

	switch leftType {
	case "POINT":
		x, y, err := parsePointXYFromPayload(left)
		if err != nil {
			return 0, err
		}
		leftPoint := geometryPoint2D{x: x, y: y}
		switch rightType {
		case "POINT":
			rightX, rightY, err := parsePointXYFromPayload(right)
			if err != nil {
				return 0, err
			}
			return math.Hypot(x-rightX, y-rightY), nil
		case "LINESTRING":
			rightLine, err := lineStringGeometryPointsFromPayload(right)
			if err != nil {
				return 0, err
			}
			return pointDistanceToLineString(leftPoint, rightLine)
		case "POLYGON":
			rightPolygon, err := polygonGeometryFromPayload(right)
			if err != nil {
				return 0, err
			}
			return pointDistanceToPolygonGeometry(leftPoint, rightPolygon)
		default:
			return 0, moerr.NewInvalidInputNoCtx(stDistanceSupportedPairsError)
		}
	case "LINESTRING":
		leftLine, err := lineStringGeometryPointsFromPayload(left)
		if err != nil {
			return 0, err
		}
		switch rightType {
		case "POINT":
			x, y, err := parsePointXYFromPayload(right)
			if err != nil {
				return 0, err
			}
			return pointDistanceToLineString(geometryPoint2D{x: x, y: y}, leftLine)
		case "LINESTRING":
			rightLine, err := lineStringGeometryPointsFromPayload(right)
			if err != nil {
				return 0, err
			}
			return lineStringDistanceToLineString(leftLine, rightLine)
		case "POLYGON":
			rightPolygon, err := polygonGeometryFromPayload(right)
			if err != nil {
				return 0, err
			}
			return lineStringDistanceToPolygonGeometry(leftLine, rightPolygon)
		default:
			return 0, moerr.NewInvalidInputNoCtx(stDistanceSupportedPairsError)
		}
	case "POLYGON":
		switch rightType {
		case "POINT":
			x, y, err := parsePointXYFromPayload(right)
			if err != nil {
				return 0, err
			}
			leftPolygonGeometry, err := polygonGeometryFromPayload(left)
			if err != nil {
				return 0, err
			}
			return pointDistanceToPolygonGeometry(geometryPoint2D{x: x, y: y}, leftPolygonGeometry)
		case "LINESTRING":
			rightLine, err := lineStringGeometryPointsFromPayload(right)
			if err != nil {
				return 0, err
			}
			leftPolygonGeometry, err := polygonGeometryFromPayload(left)
			if err != nil {
				return 0, err
			}
			return lineStringDistanceToPolygonGeometry(rightLine, leftPolygonGeometry)
		case "POLYGON":
			leftPolygon, err := polygonGeometryFromPayload(left)
			if err != nil {
				return 0, err
			}
			rightPolygon, err := polygonGeometryFromPayload(right)
			if err != nil {
				return 0, err
			}
			return polygonDistanceToPolygonGeometry(leftPolygon, rightPolygon)
		default:
			return 0, moerr.NewInvalidInputNoCtx(stDistanceSupportedPairsError)
		}
	default:
		return 0, moerr.NewInvalidInputNoCtx(stDistanceSupportedPairsError)
	}
}

func pointDistanceToLineString(point geometryPoint2D, line []geometryPoint2D) (float64, error) {
	if len(line) < 2 {
		return 0, moerr.NewInvalidInputNoCtx("invalid linestring payload")
	}
	minDistance := pointDistanceToLineSegment(point, line[0], line[1])
	for i := 1; i < len(line)-1; i++ {
		minDistance = math.Min(minDistance, pointDistanceToLineSegment(point, line[i], line[i+1]))
	}
	return minDistance, nil
}

func lineStringDistanceToLineString(left, right []geometryPoint2D) (float64, error) {
	if len(left) < 2 || len(right) < 2 {
		return 0, moerr.NewInvalidInputNoCtx("invalid linestring payload")
	}
	minDistance := lineSegmentDistance(left[0], left[1], right[0], right[1])
	for i := 0; i < len(left)-1; i++ {
		for j := 0; j < len(right)-1; j++ {
			minDistance = math.Min(minDistance, lineSegmentDistance(left[i], left[i+1], right[j], right[j+1]))
		}
	}
	return minDistance, nil
}

func lineStringDistanceToPolygon(line, polygon []geometryPoint2D) (float64, error) {
	if len(line) < 2 {
		return 0, moerr.NewInvalidInputNoCtx("invalid linestring payload")
	}
	if len(polygon) < 3 {
		return 0, moerr.NewInvalidInputNoCtx("invalid polygon payload")
	}
	if lineStringIntersectsPolygon(line, polygon) {
		return 0, nil
	}

	minDistance := lineSegmentDistance(line[0], line[1], polygon[0], polygon[1])
	for i := 0; i < len(line)-1; i++ {
		for j := 0; j < len(polygon); j++ {
			next := (j + 1) % len(polygon)
			minDistance = math.Min(minDistance, lineSegmentDistance(line[i], line[i+1], polygon[j], polygon[next]))
		}
	}
	return minDistance, nil
}

func lineStringDistanceToPolygonGeometry(line []geometryPoint2D, polygon geometryPolygon2D) (float64, error) {
	if len(line) < 2 {
		return 0, moerr.NewInvalidInputNoCtx("invalid linestring payload")
	}
	if err := validatePolygonGeometry(polygon); err != nil {
		return 0, err
	}
	if lineStringIntersectsPolygonGeometry(line, polygon) {
		return 0, nil
	}

	minDistance := lineSegmentDistance(line[0], line[1], polygon.outer[0], polygon.outer[1])
	updateDistance := func(ring []geometryPoint2D) {
		for i := 0; i < len(line)-1; i++ {
			for j := 0; j < len(ring); j++ {
				next := (j + 1) % len(ring)
				minDistance = math.Min(minDistance, lineSegmentDistance(line[i], line[i+1], ring[j], ring[next]))
			}
		}
	}
	updateDistance(polygon.outer)
	for _, hole := range polygon.holes {
		updateDistance(hole)
	}
	return minDistance, nil
}

func validatePolygonGeometry(polygon geometryPolygon2D) error {
	if len(polygon.outer) < 3 {
		return moerr.NewInvalidInputNoCtx("invalid polygon payload")
	}
	for _, hole := range polygon.holes {
		if len(hole) < 3 {
			return moerr.NewInvalidInputNoCtx("invalid polygon payload")
		}
	}
	return nil
}

func polygonGeometryRings(polygon geometryPolygon2D) [][]geometryPoint2D {
	rings := make([][]geometryPoint2D, 0, 1+len(polygon.holes))
	rings = append(rings, polygon.outer)
	rings = append(rings, polygon.holes...)
	return rings
}

func polygonDistanceToPolygon(left, right []geometryPoint2D) (float64, error) {
	if len(left) < 3 || len(right) < 3 {
		return 0, moerr.NewInvalidInputNoCtx("invalid polygon payload")
	}
	if polygonIntersectsPolygon(left, right) {
		return 0, nil
	}

	minDistance := lineSegmentDistance(left[0], left[1], right[0], right[1])
	for i := 0; i < len(left); i++ {
		leftNext := (i + 1) % len(left)
		for j := 0; j < len(right); j++ {
			rightNext := (j + 1) % len(right)
			minDistance = math.Min(minDistance, lineSegmentDistance(left[i], left[leftNext], right[j], right[rightNext]))
		}
	}
	return minDistance, nil
}

func polygonDistanceToPolygonGeometry(left, right geometryPolygon2D) (float64, error) {
	if err := validatePolygonGeometry(left); err != nil {
		return 0, err
	}
	if err := validatePolygonGeometry(right); err != nil {
		return 0, err
	}
	if polygonIntersectsPolygonGeometry(left, right) {
		return 0, nil
	}

	leftRings := polygonGeometryRings(left)
	rightRings := polygonGeometryRings(right)
	minDistance := lineSegmentDistance(left.outer[0], left.outer[1], right.outer[0], right.outer[1])
	for _, leftRing := range leftRings {
		for i := 0; i < len(leftRing); i++ {
			leftNext := (i + 1) % len(leftRing)
			for _, rightRing := range rightRings {
				for j := 0; j < len(rightRing); j++ {
					rightNext := (j + 1) % len(rightRing)
					minDistance = math.Min(minDistance, lineSegmentDistance(leftRing[i], leftRing[leftNext], rightRing[j], rightRing[rightNext]))
				}
			}
		}
	}
	return minDistance, nil
}

func pointDistanceToPolygon(point geometryPoint2D, polygon []geometryPoint2D) (float64, error) {
	if len(polygon) < 3 {
		return 0, moerr.NewInvalidInputNoCtx("invalid polygon payload")
	}
	if pointIntersectsPolygon(point, polygon) {
		return 0, nil
	}

	minDistance := pointDistanceToLineSegment(point, polygon[0], polygon[1])
	for i := 1; i < len(polygon); i++ {
		next := (i + 1) % len(polygon)
		minDistance = math.Min(minDistance, pointDistanceToLineSegment(point, polygon[i], polygon[next]))
	}
	return minDistance, nil
}

func pointDistanceToPolygonGeometry(point geometryPoint2D, polygon geometryPolygon2D) (float64, error) {
	if len(polygon.outer) < 3 {
		return 0, moerr.NewInvalidInputNoCtx("invalid polygon payload")
	}
	for _, hole := range polygon.holes {
		if len(hole) < 3 {
			return 0, moerr.NewInvalidInputNoCtx("invalid polygon payload")
		}
	}
	if pointIntersectsPolygonGeometry(point, polygon) {
		return 0, nil
	}

	minDistance := pointDistanceToPolygonRing(point, polygon.outer)
	for _, hole := range polygon.holes {
		minDistance = math.Min(minDistance, pointDistanceToPolygonRing(point, hole))
	}
	return minDistance, nil
}

func pointDistanceToPolygonRing(point geometryPoint2D, ring []geometryPoint2D) float64 {
	minDistance := pointDistanceToLineSegment(point, ring[0], ring[1])
	for i := 1; i < len(ring); i++ {
		next := (i + 1) % len(ring)
		minDistance = math.Min(minDistance, pointDistanceToLineSegment(point, ring[i], ring[next]))
	}
	return minDistance
}

func pointDistanceToLineSegment(point, start, end geometryPoint2D) float64 {
	dx := end.x - start.x
	dy := end.y - start.y
	if dx == 0 && dy == 0 {
		return math.Hypot(point.x-start.x, point.y-start.y)
	}

	projection := ((point.x-start.x)*dx + (point.y-start.y)*dy) / (dx*dx + dy*dy)
	if projection <= 0 {
		return math.Hypot(point.x-start.x, point.y-start.y)
	}
	if projection >= 1 {
		return math.Hypot(point.x-end.x, point.y-end.y)
	}

	closestX := start.x + projection*dx
	closestY := start.y + projection*dy
	return math.Hypot(point.x-closestX, point.y-closestY)
}

func lineSegmentDistance(a, b, c, d geometryPoint2D) float64 {
	if lineSegmentsIntersect(a, b, c, d) {
		return 0
	}

	minDistance := pointDistanceToLineSegment(a, c, d)
	minDistance = math.Min(minDistance, pointDistanceToLineSegment(b, c, d))
	minDistance = math.Min(minDistance, pointDistanceToLineSegment(c, a, b))
	minDistance = math.Min(minDistance, pointDistanceToLineSegment(d, a, b))
	return minDistance
}

func geometryContains(container, target []byte) (bool, error) {
	containerType, err := geometryTypeNameFromPayload(container)
	if err != nil {
		return false, err
	}
	targetType, err := geometryTypeNameFromPayload(target)
	if err != nil {
		return false, err
	}
	if isContainsSupportedGeometryType(containerType) && isContainsSupportedGeometryType(targetType) {
		if err := ensureMatchingGeometrySRID("ST_CONTAINS", container, target); err != nil {
			return false, err
		}
	}
	return geometryContainsImpl(container, target, containerType, targetType)
}

func geometryContainsImpl(container, target []byte, containerType, targetType string) (bool, error) {
	if containerType == "GEOMETRYCOLLECTION" || targetType == "GEOMETRYCOLLECTION" {
		return geometryCollectionContains(container, target, containerType, targetType)
	}
	switch containerType {
	case "POINT", "MULTIPOINT":
		if !isPointGeometryType(targetType) {
			if isContainsSupportedGeometryType(targetType) {
				return false, nil
			}
			return false, moerr.NewInvalidInputNoCtx("ST_CONTAINS only supports POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, or GEOMETRYCOLLECTION inputs")
		}
		containerPoints, err := pointGeometryItems(container, containerType)
		if err != nil {
			return false, err
		}
		targetPoints, err := pointGeometryItems(target, targetType)
		if err != nil {
			return false, err
		}
		return pointCollectionCoveredByPointCollection(targetPoints, containerPoints), nil
	case "LINESTRING", "MULTILINESTRING":
		containerLines, err := lineGeometryItems(container, containerType)
		if err != nil {
			return false, err
		}
		switch targetType {
		case "POINT", "MULTIPOINT":
			targetPoints, err := pointGeometryItems(target, targetType)
			if err != nil {
				return false, err
			}
			return pointCollectionContainedByLineCollection(targetPoints, containerLines), nil
		case "LINESTRING", "MULTILINESTRING":
			targetLines, err := lineGeometryItems(target, targetType)
			if err != nil {
				return false, err
			}
			return lineCollectionCoveredByLineCollection(targetLines, containerLines), nil
		case "POLYGON", "MULTIPOLYGON":
			return false, nil
		default:
			return false, moerr.NewInvalidInputNoCtx("ST_CONTAINS only supports POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, or GEOMETRYCOLLECTION inputs")
		}
	case "POLYGON", "MULTIPOLYGON":
		containerPolygons, err := polygonGeometryItems(container, containerType)
		if err != nil {
			return false, err
		}
		switch targetType {
		case "POINT", "MULTIPOINT":
			targetPoints, err := pointGeometryItems(target, targetType)
			if err != nil {
				return false, err
			}
			return pointCollectionContainedByPolygonCollection(targetPoints, containerPolygons), nil
		case "LINESTRING", "MULTILINESTRING":
			targetLines, err := lineGeometryItems(target, targetType)
			if err != nil {
				return false, err
			}
			return lineCollectionContainedByPolygonCollection(targetLines, containerPolygons), nil
		case "POLYGON", "MULTIPOLYGON":
			targetPolygons, err := polygonGeometryItems(target, targetType)
			if err != nil {
				return false, err
			}
			return polygonCollectionCoveredByPolygonCollection(targetPolygons, containerPolygons)
		default:
			return false, moerr.NewInvalidInputNoCtx("ST_CONTAINS only supports POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, or GEOMETRYCOLLECTION inputs")
		}
	default:
		return false, moerr.NewInvalidInputNoCtx("ST_CONTAINS only supports POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, or GEOMETRYCOLLECTION inputs")
	}
}

func geometryWithin(candidate, container []byte) (bool, error) {
	candidateType, err := geometryTypeNameFromPayload(candidate)
	if err != nil {
		return false, err
	}
	containerType, err := geometryTypeNameFromPayload(container)
	if err != nil {
		return false, err
	}
	if isContainsSupportedGeometryType(candidateType) && isContainsSupportedGeometryType(containerType) {
		if err := ensureMatchingGeometrySRID("ST_WITHIN", candidate, container); err != nil {
			return false, err
		}
	}
	if !isContainsSupportedGeometryType(candidateType) || !isContainsSupportedGeometryType(containerType) {
		return false, moerr.NewInvalidInputNoCtx("ST_WITHIN only supports POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, or GEOMETRYCOLLECTION inputs")
	}
	if candidateType == "GEOMETRYCOLLECTION" || containerType == "GEOMETRYCOLLECTION" {
		return geometryContainsImpl(container, candidate, containerType, candidateType)
	}

	switch candidateType {
	case "POINT", "MULTIPOINT":
		switch containerType {
		case "POINT", "MULTIPOINT", "LINESTRING", "POLYGON", "MULTILINESTRING", "MULTIPOLYGON":
			return geometryContainsImpl(container, candidate, containerType, candidateType)
		default:
			return false, moerr.NewInvalidInputNoCtx("ST_WITHIN only supports POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, or GEOMETRYCOLLECTION inputs")
		}
	case "LINESTRING", "MULTILINESTRING":
		switch containerType {
		case "POINT", "MULTIPOINT":
			return false, nil
		case "LINESTRING", "POLYGON", "MULTILINESTRING", "MULTIPOLYGON":
			return geometryContainsImpl(container, candidate, containerType, candidateType)
		default:
			return false, moerr.NewInvalidInputNoCtx("ST_WITHIN only supports POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, or GEOMETRYCOLLECTION inputs")
		}
	case "POLYGON", "MULTIPOLYGON":
		switch containerType {
		case "POINT", "MULTIPOINT", "LINESTRING", "MULTILINESTRING":
			return false, nil
		case "POLYGON", "MULTIPOLYGON":
			return geometryContainsImpl(container, candidate, containerType, candidateType)
		default:
			return false, moerr.NewInvalidInputNoCtx("ST_WITHIN only supports POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, or GEOMETRYCOLLECTION inputs")
		}
	default:
		return false, moerr.NewInvalidInputNoCtx("ST_WITHIN only supports POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, or GEOMETRYCOLLECTION inputs")
	}
}

func geometryIntersects(left, right []byte) (bool, error) {
	leftType, err := geometryTypeNameFromPayload(left)
	if err != nil {
		return false, err
	}
	rightType, err := geometryTypeNameFromPayload(right)
	if err != nil {
		return false, err
	}
	if isIntersectsSupportedGeometryType(leftType) && isIntersectsSupportedGeometryType(rightType) {
		if err := ensureMatchingGeometrySRID("ST_INTERSECTS", left, right); err != nil {
			return false, err
		}
	}
	return geometryIntersectsImpl(left, right, leftType, rightType)
}

func geometryIntersectsImpl(left, right []byte, leftType, rightType string) (bool, error) {
	if leftType == "GEOMETRYCOLLECTION" {
		return geometryCollectionIntersects(left, right, leftType, rightType)
	}
	if rightType == "GEOMETRYCOLLECTION" {
		return geometryCollectionIntersects(right, left, rightType, leftType)
	}
	if leftType == "MULTIPOINT" || leftType == "MULTILINESTRING" || leftType == "MULTIPOLYGON" {
		return multiGeometryIntersects(left, right)
	}
	if rightType == "MULTIPOINT" || rightType == "MULTILINESTRING" || rightType == "MULTIPOLYGON" {
		return multiGeometryIntersects(right, left)
	}

	switch leftType {
	case "POINT":
		x, y, err := parsePointXYFromPayload(left)
		if err != nil {
			return false, err
		}
		leftPoint := geometryPoint2D{x: x, y: y}
		switch rightType {
		case "POINT":
			rightX, rightY, err := parsePointXYFromPayload(right)
			if err != nil {
				return false, err
			}
			return sameGeometryPoint(leftPoint, geometryPoint2D{x: rightX, y: rightY}), nil
		case "LINESTRING":
			rightPoints, err := lineStringGeometryPointsFromPayload(right)
			if err != nil {
				return false, err
			}
			return pointIntersectsLineString(leftPoint, rightPoints), nil
		case "POLYGON":
			rightPolygon, err := polygonGeometryFromPayload(right)
			if err != nil {
				return false, err
			}
			return pointIntersectsPolygonGeometry(leftPoint, rightPolygon), nil
		default:
			return false, moerr.NewInvalidInputNoCtx("ST_INTERSECTS only supports POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, or GEOMETRYCOLLECTION inputs")
		}
	case "LINESTRING":
		leftPoints, err := lineStringGeometryPointsFromPayload(left)
		if err != nil {
			return false, err
		}
		switch rightType {
		case "POINT":
			x, y, err := parsePointXYFromPayload(right)
			if err != nil {
				return false, err
			}
			return pointIntersectsLineString(geometryPoint2D{x: x, y: y}, leftPoints), nil
		case "LINESTRING":
			rightPoints, err := lineStringGeometryPointsFromPayload(right)
			if err != nil {
				return false, err
			}
			return lineStringIntersectsLineString(leftPoints, rightPoints), nil
		case "POLYGON":
			rightPolygon, err := polygonGeometryFromPayload(right)
			if err != nil {
				return false, err
			}
			return lineStringIntersectsPolygonGeometry(leftPoints, rightPolygon), nil
		default:
			return false, moerr.NewInvalidInputNoCtx("ST_INTERSECTS only supports POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, or GEOMETRYCOLLECTION inputs")
		}
	case "POLYGON":
		switch rightType {
		case "POINT":
			x, y, err := parsePointXYFromPayload(right)
			if err != nil {
				return false, err
			}
			leftPolygon, err := polygonGeometryFromPayload(left)
			if err != nil {
				return false, err
			}
			return pointIntersectsPolygonGeometry(geometryPoint2D{x: x, y: y}, leftPolygon), nil
		case "LINESTRING":
			leftPolygon, err := polygonGeometryFromPayload(left)
			if err != nil {
				return false, err
			}
			rightPoints, err := lineStringGeometryPointsFromPayload(right)
			if err != nil {
				return false, err
			}
			return lineStringIntersectsPolygonGeometry(rightPoints, leftPolygon), nil
		case "POLYGON":
			leftPolygon, err := polygonGeometryFromPayload(left)
			if err != nil {
				return false, err
			}
			rightPolygon, err := polygonGeometryFromPayload(right)
			if err != nil {
				return false, err
			}
			return polygonIntersectsPolygonGeometry(leftPolygon, rightPolygon), nil
		default:
			return false, moerr.NewInvalidInputNoCtx("ST_INTERSECTS only supports POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, or GEOMETRYCOLLECTION inputs")
		}
	default:
		return false, moerr.NewInvalidInputNoCtx("ST_INTERSECTS only supports POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, or GEOMETRYCOLLECTION inputs")
	}
}

func geometryDisjoint(left, right []byte) (bool, error) {
	leftType, err := geometryTypeNameFromPayload(left)
	if err != nil {
		return false, err
	}
	rightType, err := geometryTypeNameFromPayload(right)
	if err != nil {
		return false, err
	}
	if isIntersectsSupportedGeometryType(leftType) && isIntersectsSupportedGeometryType(rightType) {
		if err := ensureMatchingGeometrySRID("ST_DISJOINT", left, right); err != nil {
			return false, err
		}
	}
	intersects, err := geometryIntersectsImpl(left, right, leftType, rightType)
	if err != nil {
		return false, err
	}
	return !intersects, nil
}

func geometryTouches(left, right []byte) (bool, error) {
	leftType, err := geometryTypeNameFromPayload(left)
	if err != nil {
		return false, err
	}
	rightType, err := geometryTypeNameFromPayload(right)
	if err != nil {
		return false, err
	}
	if isTouchesSupportedGeometryType(leftType) && isTouchesSupportedGeometryType(rightType) {
		if err := ensureMatchingGeometrySRID("ST_TOUCHES", left, right); err != nil {
			return false, err
		}
	}
	if !isTouchesSupportedGeometryType(leftType) || !isTouchesSupportedGeometryType(rightType) {
		return false, moerr.NewInvalidInputNoCtx(stTouchesSupportedPairsError)
	}
	if leftType == "MULTIPOINT" || leftType == "MULTILINESTRING" || leftType == "MULTIPOLYGON" ||
		leftType == "GEOMETRYCOLLECTION" || rightType == "MULTIPOINT" || rightType == "MULTILINESTRING" ||
		rightType == "MULTIPOLYGON" || rightType == "GEOMETRYCOLLECTION" {
		return multiGeometryTouches(left, right, leftType, rightType)
	}

	switch leftType {
	case "POINT":
		x, y, err := parsePointXYFromPayload(left)
		if err != nil {
			return false, err
		}
		leftPoint := geometryPoint2D{x: x, y: y}
		switch rightType {
		case "POINT":
			if _, _, err := parsePointXYFromPayload(right); err != nil {
				return false, err
			}
			return false, nil
		case "LINESTRING":
			rightPoints, err := lineStringGeometryPointsFromPayload(right)
			if err != nil {
				return false, err
			}
			return pointTouchesLineString(leftPoint, rightPoints), nil
		case "POLYGON":
			rightPolygon, err := polygonGeometryFromPayload(right)
			if err != nil {
				return false, err
			}
			return pointOnPolygonBoundaryGeometry(rightPolygon, leftPoint.x, leftPoint.y), nil
		default:
			return false, moerr.NewInvalidInputNoCtx(stTouchesSupportedPairsError)
		}
	case "LINESTRING":
		leftPoints, err := lineStringGeometryPointsFromPayload(left)
		if err != nil {
			return false, err
		}
		switch rightType {
		case "POINT":
			x, y, err := parsePointXYFromPayload(right)
			if err != nil {
				return false, err
			}
			return pointTouchesLineString(geometryPoint2D{x: x, y: y}, leftPoints), nil
		case "LINESTRING":
			rightPoints, err := lineStringGeometryPointsFromPayload(right)
			if err != nil {
				return false, err
			}
			return lineStringTouchesLineString(leftPoints, rightPoints), nil
		case "POLYGON":
			rightPolygon, err := polygonGeometryFromPayload(right)
			if err != nil {
				return false, err
			}
			return lineStringTouchesPolygonGeometry(leftPoints, rightPolygon), nil
		default:
			return false, moerr.NewInvalidInputNoCtx(stTouchesSupportedPairsError)
		}
	case "POLYGON":
		switch rightType {
		case "POINT":
			x, y, err := parsePointXYFromPayload(right)
			if err != nil {
				return false, err
			}
			leftPolygon, err := polygonGeometryFromPayload(left)
			if err != nil {
				return false, err
			}
			return pointOnPolygonBoundaryGeometry(leftPolygon, x, y), nil
		case "LINESTRING":
			leftPolygon, err := polygonGeometryFromPayload(left)
			if err != nil {
				return false, err
			}
			rightPoints, err := lineStringGeometryPointsFromPayload(right)
			if err != nil {
				return false, err
			}
			return lineStringTouchesPolygonGeometry(rightPoints, leftPolygon), nil
		case "POLYGON":
			leftPolygon, err := polygonGeometryFromPayload(left)
			if err != nil {
				return false, err
			}
			rightPolygon, err := polygonGeometryFromPayload(right)
			if err != nil {
				return false, err
			}
			return polygonTouchesPolygonGeometry(left, right, leftPolygon, rightPolygon)
		default:
			return false, moerr.NewInvalidInputNoCtx(stTouchesSupportedPairsError)
		}
	default:
		return false, moerr.NewInvalidInputNoCtx(stTouchesSupportedPairsError)
	}
}

func geometryCrosses(left, right []byte) (bool, error) {
	leftType, err := geometryTypeNameFromPayload(left)
	if err != nil {
		return false, err
	}
	rightType, err := geometryTypeNameFromPayload(right)
	if err != nil {
		return false, err
	}
	if isCrossesSupportedGeometryType(leftType) && isCrossesSupportedGeometryType(rightType) {
		if err := ensureMatchingGeometrySRID("ST_CROSSES", left, right); err != nil {
			return false, err
		}
	}
	if !isCrossesSupportedGeometryType(leftType) || !isCrossesSupportedGeometryType(rightType) {
		return false, moerr.NewInvalidInputNoCtx(stCrossesSupportedPairsError)
	}
	if leftType == "GEOMETRYCOLLECTION" || rightType == "GEOMETRYCOLLECTION" {
		return geometryCollectionCrosses(left, right, leftType, rightType)
	}

	switch leftType {
	case "POINT", "MULTIPOINT":
		if isPolygonGeometryType(rightType) || isPointGeometryType(rightType) {
			return false, nil
		}
		leftPoints, err := pointGeometryItems(left, leftType)
		if err != nil {
			return false, err
		}
		rightLines, err := lineGeometryItems(right, rightType)
		if err != nil {
			return false, err
		}
		return pointCollectionCrossesLineCollection(leftPoints, rightLines), nil
	case "LINESTRING", "MULTILINESTRING":
		leftLines, err := lineGeometryItems(left, leftType)
		if err != nil {
			return false, err
		}
		switch {
		case isPointGeometryType(rightType):
			rightPoints, err := pointGeometryItems(right, rightType)
			if err != nil {
				return false, err
			}
			return pointCollectionCrossesLineCollection(rightPoints, leftLines), nil
		case isLinearGeometryType(rightType):
			rightLines, err := lineGeometryItems(right, rightType)
			if err != nil {
				return false, err
			}
			return lineCollectionCrossesLineCollection(leftLines, rightLines), nil
		case isPolygonGeometryType(rightType):
			rightPolygons, err := polygonGeometryItems(right, rightType)
			if err != nil {
				return false, err
			}
			return lineCollectionCrossesPolygonCollection(leftLines, rightPolygons), nil
		default:
			return false, moerr.NewInvalidInputNoCtx(stCrossesSupportedPairsError)
		}
	case "POLYGON", "MULTIPOLYGON":
		if isPointGeometryType(rightType) || isPolygonGeometryType(rightType) {
			return false, nil
		}
		leftPolygons, err := polygonGeometryItems(left, leftType)
		if err != nil {
			return false, err
		}
		rightLines, err := lineGeometryItems(right, rightType)
		if err != nil {
			return false, err
		}
		return lineCollectionCrossesPolygonCollection(rightLines, leftPolygons), nil
	default:
		return false, moerr.NewInvalidInputNoCtx(stCrossesSupportedPairsError)
	}
}

func geometryOverlaps(left, right []byte) (bool, error) {
	leftType, err := geometryTypeNameFromPayload(left)
	if err != nil {
		return false, err
	}
	rightType, err := geometryTypeNameFromPayload(right)
	if err != nil {
		return false, err
	}
	if isOverlapsSupportedGeometryType(leftType) && isOverlapsSupportedGeometryType(rightType) {
		if err := ensureMatchingGeometrySRID("ST_OVERLAPS", left, right); err != nil {
			return false, err
		}
	}
	if !isOverlapsSupportedGeometryType(leftType) || !isOverlapsSupportedGeometryType(rightType) {
		return false, moerr.NewInvalidInputNoCtx(stOverlapsSupportedPairsError)
	}
	if leftType == "GEOMETRYCOLLECTION" || rightType == "GEOMETRYCOLLECTION" {
		return geometryCollectionOverlaps(left, right, leftType, rightType)
	}
	if isPointGeometryType(leftType) && isPointGeometryType(rightType) {
		leftPoints, err := pointGeometryItems(left, leftType)
		if err != nil {
			return false, err
		}
		rightPoints, err := pointGeometryItems(right, rightType)
		if err != nil {
			return false, err
		}
		return pointCollectionOverlapsPointCollection(leftPoints, rightPoints), nil
	}
	if isPointGeometryType(leftType) || isPointGeometryType(rightType) {
		return false, nil
	}
	if isLinearGeometryType(leftType) && isLinearGeometryType(rightType) {
		return linearGeometryOverlaps(left, right, leftType, rightType)
	}
	if isPolygonGeometryType(leftType) && isPolygonGeometryType(rightType) {
		return polygonGeometryOverlaps(left, right, leftType, rightType)
	}
	return false, nil
}

func geometryEquals(left, right []byte) (bool, error) {
	leftType, err := geometryTypeNameFromPayload(left)
	if err != nil {
		return false, err
	}
	rightType, err := geometryTypeNameFromPayload(right)
	if err != nil {
		return false, err
	}
	if isEqualsSupportedGeometryType(leftType) && isEqualsSupportedGeometryType(rightType) {
		if err := ensureMatchingGeometrySRID("ST_EQUALS", left, right); err != nil {
			return false, err
		}
	}
	if leftType == "GEOMETRYCOLLECTION" || rightType == "GEOMETRYCOLLECTION" {
		return geometryCollectionEquals(left, right, leftType, rightType)
	}

	switch leftType {
	case "POINT", "MULTIPOINT":
		if rightType != leftType {
			if isEqualsSupportedGeometryType(rightType) {
				return false, nil
			}
			return false, moerr.NewInvalidInputNoCtx(stEqualsSupportedPairsError)
		}
		leftPoints, err := pointGeometryItems(left, leftType)
		if err != nil {
			return false, err
		}
		rightPoints, err := pointGeometryItems(right, rightType)
		if err != nil {
			return false, err
		}
		return pointCollectionCoveredByPointCollection(leftPoints, rightPoints) &&
			pointCollectionCoveredByPointCollection(rightPoints, leftPoints), nil
	case "LINESTRING", "MULTILINESTRING":
		if rightType != leftType {
			if isEqualsSupportedGeometryType(rightType) {
				return false, nil
			}
			return false, moerr.NewInvalidInputNoCtx(stEqualsSupportedPairsError)
		}
		leftLines, err := lineGeometryItems(left, leftType)
		if err != nil {
			return false, err
		}
		rightLines, err := lineGeometryItems(right, rightType)
		if err != nil {
			return false, err
		}
		return lineCollectionCoveredByLineCollection(leftLines, rightLines) &&
			lineCollectionCoveredByLineCollection(rightLines, leftLines), nil
	case "POLYGON", "MULTIPOLYGON":
		if rightType != leftType {
			if isEqualsSupportedGeometryType(rightType) {
				return false, nil
			}
			return false, moerr.NewInvalidInputNoCtx(stEqualsSupportedPairsError)
		}
		leftPolygons, err := polygonGeometryItems(left, leftType)
		if err != nil {
			return false, err
		}
		rightPolygons, err := polygonGeometryItems(right, rightType)
		if err != nil {
			return false, err
		}
		leftCovered, err := polygonCollectionCoveredByPolygonCollection(leftPolygons, rightPolygons)
		if err != nil {
			return false, err
		}
		if !leftCovered {
			return false, nil
		}
		rightCovered, err := polygonCollectionCoveredByPolygonCollection(rightPolygons, leftPolygons)
		if err != nil {
			return false, err
		}
		return rightCovered, nil
	default:
		return false, moerr.NewInvalidInputNoCtx(stEqualsSupportedPairsError)
	}
}

func geometryCovers(container, target []byte) (bool, error) {
	containerType, err := geometryTypeNameFromPayload(container)
	if err != nil {
		return false, err
	}
	targetType, err := geometryTypeNameFromPayload(target)
	if err != nil {
		return false, err
	}
	if isCoversSupportedGeometryType(containerType) && isCoversSupportedGeometryType(targetType) {
		if err := ensureMatchingGeometrySRID("ST_COVERS", container, target); err != nil {
			return false, err
		}
	}
	if !isCoversSupportedGeometryType(containerType) || !isCoversSupportedGeometryType(targetType) {
		return false, moerr.NewInvalidInputNoCtx("ST_COVERS only supports POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, or GEOMETRYCOLLECTION inputs")
	}
	return geometryCoversImpl(container, target, containerType, targetType)
}

func geometryCoversImpl(container, target []byte, containerType, targetType string) (bool, error) {
	if containerType == "GEOMETRYCOLLECTION" || targetType == "GEOMETRYCOLLECTION" {
		return geometryCollectionCovers(container, target, containerType, targetType)
	}
	switch containerType {
	case "POINT", "MULTIPOINT":
		if !isPointGeometryType(targetType) {
			if isCoversSupportedGeometryType(targetType) {
				return false, nil
			}
			return false, moerr.NewInvalidInputNoCtx("ST_COVERS only supports POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, or GEOMETRYCOLLECTION inputs")
		}
		containerPoints, err := pointGeometryItems(container, containerType)
		if err != nil {
			return false, err
		}
		targetPoints, err := pointGeometryItems(target, targetType)
		if err != nil {
			return false, err
		}
		return pointCollectionCoveredByPointCollection(targetPoints, containerPoints), nil
	case "LINESTRING", "MULTILINESTRING":
		containerLines, err := lineGeometryItems(container, containerType)
		if err != nil {
			return false, err
		}
		switch targetType {
		case "POINT", "MULTIPOINT":
			targetPoints, err := pointGeometryItems(target, targetType)
			if err != nil {
				return false, err
			}
			return pointCollectionCoveredByLineCollection(targetPoints, containerLines), nil
		case "LINESTRING", "MULTILINESTRING":
			targetLines, err := lineGeometryItems(target, targetType)
			if err != nil {
				return false, err
			}
			return lineCollectionCoveredByLineCollection(targetLines, containerLines), nil
		case "POLYGON", "MULTIPOLYGON":
			return false, nil
		default:
			return false, moerr.NewInvalidInputNoCtx("ST_COVERS only supports POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, or GEOMETRYCOLLECTION inputs")
		}
	case "POLYGON", "MULTIPOLYGON":
		containerPolygons, err := polygonGeometryItems(container, containerType)
		if err != nil {
			return false, err
		}
		switch targetType {
		case "POINT", "MULTIPOINT":
			targetPoints, err := pointGeometryItems(target, targetType)
			if err != nil {
				return false, err
			}
			return pointCollectionCoveredByPolygonCollection(targetPoints, containerPolygons), nil
		case "LINESTRING", "MULTILINESTRING":
			targetLines, err := lineGeometryItems(target, targetType)
			if err != nil {
				return false, err
			}
			return lineCollectionCoveredByPolygonCollection(targetLines, containerPolygons), nil
		case "POLYGON", "MULTIPOLYGON":
			targetPolygons, err := polygonGeometryItems(target, targetType)
			if err != nil {
				return false, err
			}
			return polygonCollectionCoveredByPolygonCollection(targetPolygons, containerPolygons)
		default:
			return false, moerr.NewInvalidInputNoCtx("ST_COVERS only supports POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, or GEOMETRYCOLLECTION inputs")
		}
	default:
		return false, moerr.NewInvalidInputNoCtx("ST_COVERS only supports POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, or GEOMETRYCOLLECTION inputs")
	}
}

func geometryCoveredBy(candidate, container []byte) (bool, error) {
	candidateType, err := geometryTypeNameFromPayload(candidate)
	if err != nil {
		return false, err
	}
	containerType, err := geometryTypeNameFromPayload(container)
	if err != nil {
		return false, err
	}
	if isCoversSupportedGeometryType(candidateType) && isCoversSupportedGeometryType(containerType) {
		if err := ensureMatchingGeometrySRID("ST_COVEREDBY", candidate, container); err != nil {
			return false, err
		}
	}
	if !isCoversSupportedGeometryType(candidateType) || !isCoversSupportedGeometryType(containerType) {
		return false, moerr.NewInvalidInputNoCtx("ST_COVEREDBY only supports POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, or GEOMETRYCOLLECTION inputs")
	}
	if candidateType == "GEOMETRYCOLLECTION" || containerType == "GEOMETRYCOLLECTION" {
		return geometryCoversImpl(container, candidate, containerType, candidateType)
	}

	switch candidateType {
	case "POINT", "MULTIPOINT":
		switch containerType {
		case "POINT", "MULTIPOINT", "LINESTRING", "POLYGON", "MULTILINESTRING", "MULTIPOLYGON":
			return geometryCoversImpl(container, candidate, containerType, candidateType)
		default:
			return false, moerr.NewInvalidInputNoCtx("ST_COVEREDBY only supports POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, or GEOMETRYCOLLECTION inputs")
		}
	case "LINESTRING", "MULTILINESTRING":
		switch containerType {
		case "POINT", "MULTIPOINT":
			return false, nil
		case "LINESTRING", "POLYGON", "MULTILINESTRING", "MULTIPOLYGON":
			return geometryCoversImpl(container, candidate, containerType, candidateType)
		default:
			return false, moerr.NewInvalidInputNoCtx("ST_COVEREDBY only supports POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, or GEOMETRYCOLLECTION inputs")
		}
	case "POLYGON", "MULTIPOLYGON":
		switch containerType {
		case "POINT", "MULTIPOINT", "LINESTRING", "MULTILINESTRING":
			return false, nil
		case "POLYGON", "MULTIPOLYGON":
			return geometryCoversImpl(container, candidate, containerType, candidateType)
		default:
			return false, moerr.NewInvalidInputNoCtx("ST_COVEREDBY only supports POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, or GEOMETRYCOLLECTION inputs")
		}
	default:
		return false, moerr.NewInvalidInputNoCtx("ST_COVEREDBY only supports POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, or GEOMETRYCOLLECTION inputs")
	}
}

func polygonGeometryFromPayload(payload []byte) (geometryPolygon2D, error) {
	typeName, err := geometryTypeNameFromPayload(payload)
	if err != nil {
		return geometryPolygon2D{}, err
	}
	if typeName != "POLYGON" {
		return geometryPolygon2D{}, moerr.NewInvalidInputNoCtx("geometry is not a POLYGON")
	}

	wkt, _, _, err := decodeGeometryPayload(payload)
	if err != nil {
		return geometryPolygon2D{}, err
	}
	return polygonGeometryFromText(wkt)
}

func polygonGeometryFromText(wkt string) (geometryPolygon2D, error) {
	openIdx := strings.IndexByte(wkt, '(')
	closeIdx := strings.LastIndexByte(wkt, ')')
	if openIdx < 0 || closeIdx <= openIdx {
		return geometryPolygon2D{}, moerr.NewInvalidInputNoCtx("invalid polygon payload")
	}

	content := strings.TrimSpace(wkt[openIdx+1 : closeIdx])
	if content == "" {
		return geometryPolygon2D{}, moerr.NewInvalidInputNoCtx("invalid polygon payload")
	}

	rings := splitTopLevelGeometryItems(content)
	if len(rings) == 0 {
		return geometryPolygon2D{}, moerr.NewInvalidInputNoCtx("invalid polygon payload")
	}

	outer, err := parsePolygonTextRing(rings[0])
	if err != nil {
		return geometryPolygon2D{}, err
	}
	polygon := geometryPolygon2D{outer: outer}
	if len(rings) == 1 {
		return polygon, nil
	}

	polygon.holes = make([][]geometryPoint2D, 0, len(rings)-1)
	for _, ring := range rings[1:] {
		hole, err := parsePolygonTextRing(ring)
		if err != nil {
			return geometryPolygon2D{}, err
		}
		polygon.holes = append(polygon.holes, hole)
	}
	return polygon, nil
}

func parsePolygonTextRing(ring string) ([]geometryPoint2D, error) {
	ring = strings.TrimSpace(ring)
	if len(ring) < 2 || ring[0] != '(' || ring[len(ring)-1] != ')' {
		return nil, moerr.NewInvalidInputNoCtx("invalid polygon payload")
	}
	return parsePolygonRingPoints(ring[1 : len(ring)-1])
}

func pointIntersectsLineString(point geometryPoint2D, line []geometryPoint2D) bool {
	for i := 0; i < len(line)-1; i++ {
		if pointOnSegment(point.x, point.y, line[i], line[i+1]) {
			return true
		}
	}
	return false
}

func pointTouchesLineString(point geometryPoint2D, line []geometryPoint2D) bool {
	if len(line) == 0 || sameGeometryPoint(line[0], line[len(line)-1]) {
		return false
	}
	return sameGeometryPoint(point, line[0]) || sameGeometryPoint(point, line[len(line)-1])
}

func lineStringTouchesLineString(left, right []geometryPoint2D) bool {
	touched := false
	for i := 0; i < len(left)-1; i++ {
		for j := 0; j < len(right)-1; j++ {
			if !lineSegmentsIntersect(left[i], left[i+1], right[j], right[j+1]) {
				continue
			}
			if collinearSegmentsOverlapWithLength(left[i], left[i+1], right[j], right[j+1]) {
				return false
			}
			points := segmentIntersectionPoints(left[i], left[i+1], right[j], right[j+1])
			if len(points) == 0 {
				return false
			}
			hasBoundaryTouch := false
			for _, point := range points {
				if lineStringPointIsBoundary(left, point) || lineStringPointIsBoundary(right, point) {
					hasBoundaryTouch = true
					continue
				}
				return false
			}
			if hasBoundaryTouch {
				touched = true
			}
		}
	}
	return touched
}

func pointCrossesLineString(point geometryPoint2D, line []geometryPoint2D) bool {
	if !pointIntersectsLineString(point, line) {
		return false
	}
	return !lineStringPointIsBoundary(line, point)
}

func lineStringCrossesLineString(left, right []geometryPoint2D) bool {
	for i := 0; i < len(left)-1; i++ {
		for j := 0; j < len(right)-1; j++ {
			if !lineSegmentsIntersect(left[i], left[i+1], right[j], right[j+1]) {
				continue
			}
			if collinearSegmentsOverlapWithLength(left[i], left[i+1], right[j], right[j+1]) {
				return false
			}
			points := segmentIntersectionPoints(left[i], left[i+1], right[j], right[j+1])
			if len(points) == 0 {
				return true
			}
			for _, point := range points {
				if lineStringPointIsBoundary(left, point) || lineStringPointIsBoundary(right, point) {
					continue
				}
				return true
			}
		}
	}
	return false
}

func lineStringCrossesPolygonGeometry(line []geometryPoint2D, polygon geometryPolygon2D) bool {
	if !lineStringIntersectsPolygonGeometry(line, polygon) {
		return false
	}

	hasInside := false
	hasOutside := false
	for i := 0; i < len(line)-1; i++ {
		if sameGeometryPoint(line[i], line[i+1]) {
			continue
		}
		segmentInside, segmentOutside, _, boundaryOverlap := lineSegmentPolygonLocationFlagsGeometry(line[i], line[i+1], polygon)
		if boundaryOverlap {
			return false
		}
		hasInside = hasInside || segmentInside
		hasOutside = hasOutside || segmentOutside
	}
	return hasInside && hasOutside
}

func lineSegmentPolygonLocationFlagsGeometry(start, end geometryPoint2D, polygon geometryPolygon2D) (bool, bool, bool, bool) {
	params := []float64{0, 1}
	boundaryTouched := false
	collectRingParams := func(ring []geometryPoint2D) bool {
		for i := 0; i < len(ring); i++ {
			next := (i + 1) % len(ring)
			if !lineSegmentsIntersect(start, end, ring[i], ring[next]) {
				continue
			}
			boundaryTouched = true
			if collinearSegmentsOverlapWithLength(start, end, ring[i], ring[next]) {
				return true
			}
			points := segmentIntersectionPoints(start, end, ring[i], ring[next])
			if len(points) == 0 {
				point, ok := segmentIntersectionPoint(start, end, ring[i], ring[next])
				if !ok {
					continue
				}
				params = append(params, segmentParameter(start, end, point))
				continue
			}
			for _, point := range points {
				params = append(params, segmentParameter(start, end, point))
			}
		}
		return false
	}
	if collectRingParams(polygon.outer) {
		return false, false, true, true
	}
	for _, hole := range polygon.holes {
		if collectRingParams(hole) {
			return false, false, true, true
		}
	}

	sort.Float64s(params)
	params = dedupeSegmentParameters(params)

	hasInside := false
	hasOutside := false
	for i := 0; i < len(params)-1; i++ {
		if sameGeometryCoordinate(params[i], params[i+1]) {
			continue
		}
		point := interpolateSegmentPoint(start, end, (params[i]+params[i+1])/2)
		if pointInPolygonGeometry(polygon, point.x, point.y) {
			hasInside = true
			continue
		}
		if !pointOnPolygonBoundaryGeometry(polygon, point.x, point.y) {
			hasOutside = true
		}
	}
	return hasInside, hasOutside, boundaryTouched, false
}

func segmentIntersectionPoint(a, b, c, d geometryPoint2D) (geometryPoint2D, bool) {
	denominator := (a.x-b.x)*(c.y-d.y) - (a.y-b.y)*(c.x-d.x)
	if sameGeometryCoordinate(denominator, 0) {
		return geometryPoint2D{}, false
	}

	leftCross := a.x*b.y - a.y*b.x
	rightCross := c.x*d.y - c.y*d.x
	x := (leftCross*(c.x-d.x) - (a.x-b.x)*rightCross) / denominator
	y := (leftCross*(c.y-d.y) - (a.y-b.y)*rightCross) / denominator
	return geometryPoint2D{x: x, y: y}, true
}

func segmentParameter(start, end, point geometryPoint2D) float64 {
	dx := end.x - start.x
	dy := end.y - start.y
	if math.Abs(dx) >= math.Abs(dy) {
		if sameGeometryCoordinate(dx, 0) {
			return 0
		}
		return (point.x - start.x) / dx
	}
	if sameGeometryCoordinate(dy, 0) {
		return 0
	}
	return (point.y - start.y) / dy
}

func dedupeSegmentParameters(params []float64) []float64 {
	if len(params) == 0 {
		return nil
	}
	deduped := make([]float64, 0, len(params))
	for _, param := range params {
		switch {
		case param < 0 && sameGeometryCoordinate(param, 0):
			param = 0
		case param > 1 && sameGeometryCoordinate(param, 1):
			param = 1
		}
		if len(deduped) > 0 && sameGeometryCoordinate(deduped[len(deduped)-1], param) {
			continue
		}
		deduped = append(deduped, param)
	}
	return deduped
}

func interpolateSegmentPoint(start, end geometryPoint2D, param float64) geometryPoint2D {
	return geometryPoint2D{
		x: start.x + (end.x-start.x)*param,
		y: start.y + (end.y-start.y)*param,
	}
}

func hasLineStringLinearOverlap(left, right []geometryPoint2D) bool {
	for i := 0; i < len(left)-1; i++ {
		for j := 0; j < len(right)-1; j++ {
			if collinearSegmentsOverlapWithLength(left[i], left[i+1], right[j], right[j+1]) {
				return true
			}
		}
	}
	return false
}

func lineStringCoveredByPolygonGeometry(line []geometryPoint2D, polygon geometryPolygon2D) bool {
	for _, point := range line {
		if !pointIntersectsPolygonGeometry(point, polygon) {
			return false
		}
	}
	for i := 0; i < len(line)-1; i++ {
		if sameGeometryPoint(line[i], line[i+1]) {
			continue
		}
		_, hasOutside, _, _ := lineSegmentPolygonLocationFlagsGeometry(line[i], line[i+1], polygon)
		if hasOutside {
			return false
		}
	}
	return true
}

func polygonRingCoveredByPolygonGeometry(ring []geometryPoint2D, polygon geometryPolygon2D) bool {
	for _, point := range ring {
		if !pointIntersectsPolygonGeometry(point, polygon) {
			return false
		}
	}
	for i := 0; i < len(ring); i++ {
		next := (i + 1) % len(ring)
		if sameGeometryPoint(ring[i], ring[next]) {
			continue
		}
		_, hasOutside, _, _ := lineSegmentPolygonLocationFlagsGeometry(ring[i], ring[next], polygon)
		if hasOutside {
			return false
		}
	}
	return true
}

func polygonCoveredByPolygonGeometry(candidatePayload []byte, candidate, container geometryPolygon2D) (bool, error) {
	if err := validatePolygonGeometry(candidate); err != nil {
		return false, err
	}
	if err := validatePolygonGeometry(container); err != nil {
		return false, err
	}

	candidateInterior, err := polygonInteriorPointFromPayload(candidatePayload)
	if err != nil {
		return false, err
	}
	if !pointIntersectsPolygonGeometry(candidateInterior, container) {
		return false, nil
	}

	for _, ring := range polygonGeometryRings(candidate) {
		if !polygonRingCoveredByPolygonGeometry(ring, container) {
			return false, nil
		}
	}
	for _, hole := range container.holes {
		holeInterior, err := polygonInteriorPointFromRing(hole)
		if err != nil {
			return false, err
		}
		if pointInPolygonGeometry(candidate, holeInterior.x, holeInterior.y) {
			return false, nil
		}
	}
	return true, nil
}

func segmentOverlapParameterInterval(start, end, otherStart, otherEnd geometryPoint2D) (geometryParamInterval, bool) {
	if geometryOrientation(start, end, otherStart) != 0 || geometryOrientation(start, end, otherEnd) != 0 {
		return geometryParamInterval{}, false
	}
	if !lineSegmentsIntersect(start, end, otherStart, otherEnd) {
		return geometryParamInterval{}, false
	}

	startParam := segmentParameter(start, end, otherStart)
	endParam := segmentParameter(start, end, otherEnd)
	interval := geometryParamInterval{
		start: math.Max(0, math.Min(startParam, endParam)),
		end:   math.Min(1, math.Max(startParam, endParam)),
	}
	if interval.end-interval.start <= 1e-9 {
		return geometryParamInterval{}, false
	}
	return interval, true
}

func parameterIntervalsCoverSegment(intervals []geometryParamInterval) bool {
	if len(intervals) == 0 {
		return false
	}

	sort.Slice(intervals, func(i, j int) bool {
		if sameGeometryCoordinate(intervals[i].start, intervals[j].start) {
			return intervals[i].end < intervals[j].end
		}
		return intervals[i].start < intervals[j].start
	})

	coveredEnd := intervals[0].end
	if intervals[0].start > 1e-9 {
		return false
	}
	if coveredEnd >= 1-1e-9 {
		return true
	}

	for _, interval := range intervals[1:] {
		if interval.start-coveredEnd > 1e-9 {
			return false
		}
		if interval.end > coveredEnd {
			coveredEnd = interval.end
			if coveredEnd >= 1-1e-9 {
				return true
			}
		}
	}
	return coveredEnd >= 1-1e-9
}

func lineStringTouchesPolygonGeometry(line []geometryPoint2D, polygon geometryPolygon2D) bool {
	if !lineStringIntersectsPolygonGeometry(line, polygon) {
		return false
	}

	touchedBoundary := false
	for _, point := range line {
		if pointInPolygonGeometry(polygon, point.x, point.y) {
			return false
		}
		if pointOnPolygonBoundaryGeometry(polygon, point.x, point.y) {
			touchedBoundary = true
		}
	}
	for i := 0; i < len(line)-1; i++ {
		if sameGeometryPoint(line[i], line[i+1]) {
			continue
		}
		segmentInside, _, segmentBoundaryTouched, _ := lineSegmentPolygonLocationFlagsGeometry(line[i], line[i+1], polygon)
		if segmentInside {
			return false
		}
		if segmentBoundaryTouched {
			touchedBoundary = true
		}
	}
	return touchedBoundary
}

func polygonTouchesPolygonGeometry(leftPayload, rightPayload []byte, left, right geometryPolygon2D) (bool, error) {
	if err := validatePolygonGeometry(left); err != nil {
		return false, err
	}
	if err := validatePolygonGeometry(right); err != nil {
		return false, err
	}
	if !polygonIntersectsPolygonGeometry(left, right) {
		return false, nil
	}

	leftInterior, err := polygonInteriorPointFromPayload(leftPayload)
	if err != nil {
		return false, err
	}
	if pointInPolygonGeometry(right, leftInterior.x, leftInterior.y) {
		return false, nil
	}

	rightInterior, err := polygonInteriorPointFromPayload(rightPayload)
	if err != nil {
		return false, err
	}
	if pointInPolygonGeometry(left, rightInterior.x, rightInterior.y) {
		return false, nil
	}

	touched := false
	for _, ring := range polygonGeometryRings(left) {
		for i := 0; i < len(ring); i++ {
			next := (i + 1) % len(ring)
			if sameGeometryPoint(ring[i], ring[next]) {
				continue
			}
			segmentInside, _, segmentBoundaryTouched, _ := lineSegmentPolygonLocationFlagsGeometry(ring[i], ring[next], right)
			if segmentInside {
				return false, nil
			}
			if segmentBoundaryTouched {
				touched = true
			}
		}
	}
	for _, ring := range polygonGeometryRings(right) {
		for i := 0; i < len(ring); i++ {
			next := (i + 1) % len(ring)
			if sameGeometryPoint(ring[i], ring[next]) {
				continue
			}
			segmentInside, _, segmentBoundaryTouched, _ := lineSegmentPolygonLocationFlagsGeometry(ring[i], ring[next], left)
			if segmentInside {
				return false, nil
			}
			if segmentBoundaryTouched {
				touched = true
			}
		}
	}
	return touched, nil
}

func polygonOverlapsPolygonGeometry(leftPayload, rightPayload []byte, left, right geometryPolygon2D) (bool, error) {
	if !polygonIntersectsPolygonGeometry(left, right) {
		return false, nil
	}
	touches, err := polygonTouchesPolygonGeometry(leftPayload, rightPayload, left, right)
	if err != nil {
		return false, err
	}
	if touches {
		return false, nil
	}
	leftCovered, err := polygonCoveredByPolygonGeometry(leftPayload, left, right)
	if err != nil {
		return false, err
	}
	if leftCovered {
		return false, nil
	}
	rightCovered, err := polygonCoveredByPolygonGeometry(rightPayload, right, left)
	if err != nil {
		return false, err
	}
	if rightCovered {
		return false, nil
	}
	return true, nil
}

func polygonInteriorPointFromPayload(payload []byte) (geometryPoint2D, error) {
	pointPayload, err := pointOnSurfaceFromPayload(payload)
	if err != nil {
		return geometryPoint2D{}, err
	}
	x, y, err := parsePointXYFromPayload(pointPayload)
	if err != nil {
		return geometryPoint2D{}, err
	}
	return geometryPoint2D{x: x, y: y}, nil
}

func polygonInteriorPointFromRing(ring []geometryPoint2D) (geometryPoint2D, error) {
	payload := encodeGeometryPayload("POLYGON("+polygonRingText(ring)+")", 0, false)
	return polygonInteriorPointFromPayload(payload)
}

func polygonRingText(ring []geometryPoint2D) string {
	var builder strings.Builder
	builder.WriteByte('(')
	writePoint := func(point geometryPoint2D) {
		builder.WriteString(strconv.FormatFloat(point.x, 'g', -1, 64))
		builder.WriteByte(' ')
		builder.WriteString(strconv.FormatFloat(point.y, 'g', -1, 64))
	}
	for i, point := range ring {
		if i > 0 {
			builder.WriteByte(',')
		}
		writePoint(point)
	}
	if len(ring) > 0 && !sameGeometryPoint(ring[0], ring[len(ring)-1]) {
		builder.WriteByte(',')
		writePoint(ring[0])
	}
	builder.WriteByte(')')
	return builder.String()
}

func lineStringPointIsBoundary(line []geometryPoint2D, point geometryPoint2D) bool {
	if len(line) == 0 || sameGeometryPoint(line[0], line[len(line)-1]) {
		return false
	}
	return sameGeometryPoint(point, line[0]) || sameGeometryPoint(point, line[len(line)-1])
}

func segmentIntersectionPoints(a, b, c, d geometryPoint2D) []geometryPoint2D {
	points := make([]geometryPoint2D, 0, 4)
	if pointOnSegment(a.x, a.y, c, d) {
		points = appendUniqueGeometryPoints(points, a)
	}
	if pointOnSegment(b.x, b.y, c, d) {
		points = appendUniqueGeometryPoints(points, b)
	}
	if pointOnSegment(c.x, c.y, a, b) {
		points = appendUniqueGeometryPoints(points, c)
	}
	if pointOnSegment(d.x, d.y, a, b) {
		points = appendUniqueGeometryPoints(points, d)
	}
	return points
}

func appendUniqueGeometryPoints(points []geometryPoint2D, point geometryPoint2D) []geometryPoint2D {
	for _, existing := range points {
		if sameGeometryPoint(existing, point) {
			return points
		}
	}
	return append(points, point)
}

func collinearSegmentsOverlapWithLength(a, b, c, d geometryPoint2D) bool {
	if geometryOrientation(a, b, c) != 0 || geometryOrientation(a, b, d) != 0 {
		return false
	}
	if math.Abs(a.x-b.x) >= math.Abs(a.y-b.y) {
		overlap := math.Min(math.Max(a.x, b.x), math.Max(c.x, d.x)) - math.Max(math.Min(a.x, b.x), math.Min(c.x, d.x))
		return overlap > 1e-9
	}
	overlap := math.Min(math.Max(a.y, b.y), math.Max(c.y, d.y)) - math.Max(math.Min(a.y, b.y), math.Min(c.y, d.y))
	return overlap > 1e-9
}

func pointIntersectsPolygon(point geometryPoint2D, polygon []geometryPoint2D) bool {
	return pointOnPolygonBoundary(polygon, point.x, point.y) || pointInPolygon(polygon, point.x, point.y)
}

func pointIntersectsPolygonGeometry(point geometryPoint2D, polygon geometryPolygon2D) bool {
	if pointOnPolygonBoundaryGeometry(polygon, point.x, point.y) {
		return true
	}
	if !pointInPolygon(polygon.outer, point.x, point.y) {
		return false
	}
	for _, hole := range polygon.holes {
		if pointInPolygon(hole, point.x, point.y) {
			return false
		}
	}
	return true
}

func lineStringIntersectsLineString(left, right []geometryPoint2D) bool {
	for i := 0; i < len(left)-1; i++ {
		for j := 0; j < len(right)-1; j++ {
			if lineSegmentsIntersect(left[i], left[i+1], right[j], right[j+1]) {
				return true
			}
		}
	}
	return false
}

func lineStringIntersectsPolygon(line []geometryPoint2D, polygon []geometryPoint2D) bool {
	for _, point := range line {
		if pointIntersectsPolygon(point, polygon) {
			return true
		}
	}
	for i := 0; i < len(line)-1; i++ {
		for j := 0; j < len(polygon); j++ {
			next := (j + 1) % len(polygon)
			if lineSegmentsIntersect(line[i], line[i+1], polygon[j], polygon[next]) {
				return true
			}
		}
	}
	return false
}

func lineStringIntersectsPolygonGeometry(line []geometryPoint2D, polygon geometryPolygon2D) bool {
	for _, point := range line {
		if pointIntersectsPolygonGeometry(point, polygon) {
			return true
		}
	}
	checkRing := func(ring []geometryPoint2D) bool {
		for i := 0; i < len(line)-1; i++ {
			for j := 0; j < len(ring); j++ {
				next := (j + 1) % len(ring)
				if lineSegmentsIntersect(line[i], line[i+1], ring[j], ring[next]) {
					return true
				}
			}
		}
		return false
	}
	if checkRing(polygon.outer) {
		return true
	}
	for _, hole := range polygon.holes {
		if checkRing(hole) {
			return true
		}
	}
	return false
}

func polygonIntersectsPolygon(left, right []geometryPoint2D) bool {
	for i := 0; i < len(left); i++ {
		leftNext := (i + 1) % len(left)
		for j := 0; j < len(right); j++ {
			rightNext := (j + 1) % len(right)
			if lineSegmentsIntersect(left[i], left[leftNext], right[j], right[rightNext]) {
				return true
			}
		}
	}
	for _, point := range left {
		if pointIntersectsPolygon(point, right) {
			return true
		}
	}
	for _, point := range right {
		if pointIntersectsPolygon(point, left) {
			return true
		}
	}
	return false
}

func polygonIntersectsPolygonGeometry(left, right geometryPolygon2D) bool {
	for _, ring := range polygonGeometryRings(left) {
		if lineStringIntersectsPolygonGeometry(ring, right) {
			return true
		}
	}
	for _, ring := range polygonGeometryRings(right) {
		if lineStringIntersectsPolygonGeometry(ring, left) {
			return true
		}
	}
	return false
}

func multiGeometryIntersects(collection, other []byte) (bool, error) {
	count, err := geometryCountFromPayload(collection)
	if err != nil {
		return false, err
	}
	for i := int64(1); i <= count; i++ {
		item, err := geometryNFromPayload(collection, i)
		if err != nil {
			return false, err
		}
		intersects, err := geometryIntersects([]byte(item), other)
		if err != nil {
			return false, err
		}
		if intersects {
			return true, nil
		}
	}
	return false, nil
}

func geometryCollectionIntersects(collection, other []byte, collectionType, otherType string) (bool, error) {
	items, err := geometryPayloadItems(collection, collectionType)
	if err != nil {
		return false, err
	}
	for _, item := range items {
		itemType, err := geometryTypeNameFromPayload(item)
		if err != nil {
			return false, err
		}
		intersects, err := geometryIntersectsImpl(item, other, itemType, otherType)
		if err != nil {
			return false, err
		}
		if intersects {
			return true, nil
		}
	}
	return false, nil
}

func geometryCollectionContains(container, target []byte, containerType, targetType string) (bool, error) {
	if targetType == "GEOMETRYCOLLECTION" {
		targetItems, err := geometryPayloadItems(target, targetType)
		if err != nil {
			return false, err
		}
		for _, item := range targetItems {
			itemType, err := geometryTypeNameFromPayload(item)
			if err != nil {
				return false, err
			}
			contains, err := geometryContainsImpl(container, item, containerType, itemType)
			if err != nil {
				return false, err
			}
			if !contains {
				return false, nil
			}
		}
		return true, nil
	}

	containerItems, err := geometryPayloadItems(container, containerType)
	if err != nil {
		return false, err
	}
	for _, item := range containerItems {
		itemType, err := geometryTypeNameFromPayload(item)
		if err != nil {
			return false, err
		}
		contains, err := geometryContainsImpl(item, target, itemType, targetType)
		if err != nil {
			return false, err
		}
		if contains {
			return true, nil
		}
	}
	return false, nil
}

func geometryCollectionCovers(container, target []byte, containerType, targetType string) (bool, error) {
	if targetType == "GEOMETRYCOLLECTION" {
		targetItems, err := geometryPayloadItems(target, targetType)
		if err != nil {
			return false, err
		}
		for _, item := range targetItems {
			itemType, err := geometryTypeNameFromPayload(item)
			if err != nil {
				return false, err
			}
			covers, err := geometryCoversImpl(container, item, containerType, itemType)
			if err != nil {
				return false, err
			}
			if !covers {
				return false, nil
			}
		}
		return true, nil
	}

	containerItems, err := geometryPayloadItems(container, containerType)
	if err != nil {
		return false, err
	}
	for _, item := range containerItems {
		itemType, err := geometryTypeNameFromPayload(item)
		if err != nil {
			return false, err
		}
		covers, err := geometryCoversImpl(item, target, itemType, targetType)
		if err != nil {
			return false, err
		}
		if covers {
			return true, nil
		}
	}
	return false, nil
}

func geometryCollectionOverlaps(left, right []byte, leftType, rightType string) (bool, error) {
	leftItems, err := geometryCollectionTopLevelItems(left, leftType)
	if err != nil {
		return false, err
	}
	rightItems, err := geometryCollectionTopLevelItems(right, rightType)
	if err != nil {
		return false, err
	}
	for _, leftItem := range leftItems {
		for _, rightItem := range rightItems {
			overlaps, err := geometryOverlaps(leftItem, rightItem)
			if err != nil {
				return false, err
			}
			if overlaps {
				return true, nil
			}
		}
	}
	return false, nil
}

func geometryCollectionEquals(left, right []byte, leftType, rightType string) (bool, error) {
	if leftType == "GEOMETRYCOLLECTION" {
		if _, err := geometryPayloadItems(left, leftType); err != nil {
			return false, err
		}
	}
	if rightType == "GEOMETRYCOLLECTION" {
		if _, err := geometryPayloadItems(right, rightType); err != nil {
			return false, err
		}
	}
	if leftType != "GEOMETRYCOLLECTION" || rightType != "GEOMETRYCOLLECTION" {
		if isEqualsSupportedGeometryType(leftType) && isEqualsSupportedGeometryType(rightType) {
			return false, nil
		}
		return false, moerr.NewInvalidInputNoCtx(stEqualsSupportedPairsError)
	}

	leftItems, err := geometryPayloadItems(left, leftType)
	if err != nil {
		return false, err
	}
	rightItems, err := geometryPayloadItems(right, rightType)
	if err != nil {
		return false, err
	}
	if len(leftItems) != len(rightItems) {
		return false, nil
	}

	matchedRight := make([]bool, len(rightItems))
	for _, leftItem := range leftItems {
		matched := false
		for idx, rightItem := range rightItems {
			if matchedRight[idx] {
				continue
			}
			equal, err := geometryEquals(leftItem, rightItem)
			if err != nil {
				return false, err
			}
			if !equal {
				continue
			}
			matchedRight[idx] = true
			matched = true
			break
		}
		if !matched {
			return false, nil
		}
	}
	return true, nil
}

func geometryCollectionCrosses(left, right []byte, leftType, rightType string) (bool, error) {
	leftItems, err := geometryCollectionTopLevelItems(left, leftType)
	if err != nil {
		return false, err
	}
	rightItems, err := geometryCollectionTopLevelItems(right, rightType)
	if err != nil {
		return false, err
	}
	for _, leftItem := range leftItems {
		for _, rightItem := range rightItems {
			crosses, err := geometryCrosses(leftItem, rightItem)
			if err != nil {
				return false, err
			}
			if crosses {
				return true, nil
			}
		}
	}
	return false, nil
}

func geometryCollectionTopLevelItems(payload []byte, typeName string) ([][]byte, error) {
	if typeName != "GEOMETRYCOLLECTION" {
		return [][]byte{payload}, nil
	}
	return geometryPayloadItems(payload, typeName)
}

func isTouchesSupportedGeometryType(typeName string) bool {
	return isSimpleGeometryType(typeName) || typeName == "MULTIPOINT" || typeName == "MULTILINESTRING" || typeName == "MULTIPOLYGON" || typeName == "GEOMETRYCOLLECTION"
}

func isCrossesSupportedGeometryType(typeName string) bool {
	return isSimpleGeometryType(typeName) || typeName == "MULTIPOINT" || typeName == "MULTILINESTRING" || typeName == "MULTIPOLYGON" || typeName == "GEOMETRYCOLLECTION"
}

func isOverlapsSupportedGeometryType(typeName string) bool {
	return isSimpleGeometryType(typeName) || typeName == "MULTIPOINT" || typeName == "MULTILINESTRING" || typeName == "MULTIPOLYGON" || typeName == "GEOMETRYCOLLECTION"
}

func isEqualsSupportedGeometryType(typeName string) bool {
	return isSimpleGeometryType(typeName) || typeName == "MULTIPOINT" || typeName == "MULTILINESTRING" || typeName == "MULTIPOLYGON" || typeName == "GEOMETRYCOLLECTION"
}

func isPointGeometryType(typeName string) bool {
	return typeName == "POINT" || typeName == "MULTIPOINT"
}

func isLinearGeometryType(typeName string) bool {
	return typeName == "LINESTRING" || typeName == "MULTILINESTRING"
}

func isPolygonGeometryType(typeName string) bool {
	return typeName == "POLYGON" || typeName == "MULTIPOLYGON"
}

func isCoversSupportedGeometryType(typeName string) bool {
	return isSimpleGeometryType(typeName) || typeName == "MULTIPOINT" || typeName == "MULTILINESTRING" || typeName == "MULTIPOLYGON" || typeName == "GEOMETRYCOLLECTION"
}

func isContainsSupportedGeometryType(typeName string) bool {
	return isSimpleGeometryType(typeName) || typeName == "MULTIPOINT" || typeName == "MULTILINESTRING" || typeName == "MULTIPOLYGON" || typeName == "GEOMETRYCOLLECTION"
}

func geometryPayloadItems(payload []byte, typeName string) ([][]byte, error) {
	if typeName != "MULTIPOINT" && typeName != "MULTILINESTRING" && typeName != "MULTIPOLYGON" && typeName != "GEOMETRYCOLLECTION" {
		return [][]byte{payload}, nil
	}
	count, err := geometryCountFromPayload(payload)
	if err != nil {
		return nil, err
	}
	items := make([][]byte, 0, int(count))
	for i := int64(1); i <= count; i++ {
		item, err := geometryNFromPayload(payload, i)
		if err != nil {
			return nil, err
		}
		items = append(items, []byte(item))
	}
	return items, nil
}

func pointGeometryItems(payload []byte, typeName string) ([]geometryPoint2D, error) {
	items, err := geometryPayloadItems(payload, typeName)
	if err != nil {
		return nil, err
	}
	points := make([]geometryPoint2D, 0, len(items))
	for _, item := range items {
		x, y, err := parsePointXYFromPayload(item)
		if err != nil {
			return nil, err
		}
		points = append(points, geometryPoint2D{x: x, y: y})
	}
	return points, nil
}

func pointCollectionCoveredByPointCollection(candidate, container []geometryPoint2D) bool {
	for _, candidatePoint := range candidate {
		covered := false
		for _, containerPoint := range container {
			if sameGeometryPoint(candidatePoint, containerPoint) {
				covered = true
				break
			}
		}
		if !covered {
			return false
		}
	}
	return true
}

func pointCollectionOverlapsPointCollection(left, right []geometryPoint2D) bool {
	hasSharedPoint := false
	leftCoveredByRight := true
	for _, leftPoint := range left {
		covered := false
		for _, rightPoint := range right {
			if sameGeometryPoint(leftPoint, rightPoint) {
				covered = true
				hasSharedPoint = true
				break
			}
		}
		if !covered {
			leftCoveredByRight = false
		}
	}
	if !hasSharedPoint || leftCoveredByRight {
		return false
	}
	return !pointCollectionCoveredByPointCollection(right, left)
}

func pointCollectionCoveredByLineCollection(candidate []geometryPoint2D, container [][]geometryPoint2D) bool {
	for _, candidatePoint := range candidate {
		if !pointIntersectsLineCollection(candidatePoint, container) {
			return false
		}
	}
	return true
}

func pointCollectionContainedByLineCollection(candidate []geometryPoint2D, container [][]geometryPoint2D) bool {
	for _, candidatePoint := range candidate {
		if !pointCrossesLineCollection(candidatePoint, container) {
			return false
		}
	}
	return true
}

func pointCollectionCrossesLineCollection(points []geometryPoint2D, lines [][]geometryPoint2D) bool {
	for _, point := range points {
		if pointCrossesLineCollection(point, lines) {
			return true
		}
	}
	return false
}

func multiGeometryTouches(left, right []byte, leftType, rightType string) (bool, error) {
	leftItems, err := geometryPayloadItems(left, leftType)
	if err != nil {
		return false, err
	}
	rightItems, err := geometryPayloadItems(right, rightType)
	if err != nil {
		return false, err
	}
	touched := false
	for _, leftItem := range leftItems {
		for _, rightItem := range rightItems {
			intersects, err := geometryIntersects(leftItem, rightItem)
			if err != nil {
				return false, err
			}
			if !intersects {
				continue
			}
			itemTouches, err := geometryTouches(leftItem, rightItem)
			if err != nil {
				return false, err
			}
			if !itemTouches {
				return false, nil
			}
			touched = true
		}
	}
	return touched, nil
}

func lineGeometryItems(payload []byte, typeName string) ([][]geometryPoint2D, error) {
	items, err := geometryPayloadItems(payload, typeName)
	if err != nil {
		return nil, err
	}
	lines := make([][]geometryPoint2D, 0, len(items))
	for _, item := range items {
		line, err := lineStringGeometryPointsFromPayload(item)
		if err != nil {
			return nil, err
		}
		lines = append(lines, line)
	}
	return lines, nil
}

type polygonGeometryItem struct {
	payload []byte
	shape   geometryPolygon2D
}

func polygonGeometryItems(payload []byte, typeName string) ([]polygonGeometryItem, error) {
	items, err := geometryPayloadItems(payload, typeName)
	if err != nil {
		return nil, err
	}
	polygons := make([]polygonGeometryItem, 0, len(items))
	for _, item := range items {
		polygon, err := polygonGeometryFromPayload(item)
		if err != nil {
			return nil, err
		}
		polygons = append(polygons, polygonGeometryItem{
			payload: item,
			shape:   polygon,
		})
	}
	return polygons, nil
}

func lineCollectionHasLinearOverlap(left, right [][]geometryPoint2D) bool {
	for _, leftLine := range left {
		for _, rightLine := range right {
			if hasLineStringLinearOverlap(leftLine, rightLine) {
				return true
			}
		}
	}
	return false
}

func pointIntersectsLineCollection(point geometryPoint2D, lines [][]geometryPoint2D) bool {
	for _, line := range lines {
		if pointIntersectsLineString(point, line) {
			return true
		}
	}
	return false
}

func pointCrossesLineCollection(point geometryPoint2D, lines [][]geometryPoint2D) bool {
	for _, line := range lines {
		if pointCrossesLineString(point, line) {
			return true
		}
	}
	return false
}

func lineSegmentCoveredByLineCollection(start, end geometryPoint2D, lines [][]geometryPoint2D) bool {
	intervals := make([]geometryParamInterval, 0, len(lines))
	for _, line := range lines {
		for i := 0; i < len(line)-1; i++ {
			interval, ok := segmentOverlapParameterInterval(start, end, line[i], line[i+1])
			if !ok {
				continue
			}
			intervals = append(intervals, interval)
		}
	}
	return parameterIntervalsCoverSegment(intervals)
}

func lineCollectionCoveredByLineCollection(candidate, container [][]geometryPoint2D) bool {
	for _, line := range candidate {
		for i := 0; i < len(line)-1; i++ {
			if sameGeometryPoint(line[i], line[i+1]) {
				continue
			}
			if !lineSegmentCoveredByLineCollection(line[i], line[i+1], container) {
				return false
			}
		}
	}
	return true
}

func lineCollectionCrossesLineCollection(left, right [][]geometryPoint2D) bool {
	if lineCollectionHasLinearOverlap(left, right) {
		return false
	}
	for _, leftLine := range left {
		for _, rightLine := range right {
			if lineStringCrossesLineString(leftLine, rightLine) {
				return true
			}
		}
	}
	return false
}

func pointIntersectsPolygonCollection(point geometryPoint2D, polygons []polygonGeometryItem) bool {
	for _, polygon := range polygons {
		if pointIntersectsPolygonGeometry(point, polygon.shape) {
			return true
		}
	}
	return false
}

func lineCollectionCrossesPolygonCollection(lines [][]geometryPoint2D, polygons []polygonGeometryItem) bool {
	for _, line := range lines {
		for _, polygon := range polygons {
			if lineStringCrossesPolygonGeometry(line, polygon.shape) {
				return true
			}
		}
	}
	return false
}

func pointInPolygonCollection(point geometryPoint2D, polygons []polygonGeometryItem) bool {
	for _, polygon := range polygons {
		if pointInPolygonGeometry(polygon.shape, point.x, point.y) {
			return true
		}
	}
	return false
}

func pointCollectionCoveredByPolygonCollection(candidate []geometryPoint2D, container []polygonGeometryItem) bool {
	for _, candidatePoint := range candidate {
		if !pointIntersectsPolygonCollection(candidatePoint, container) {
			return false
		}
	}
	return true
}

func pointCollectionContainedByPolygonCollection(candidate []geometryPoint2D, container []polygonGeometryItem) bool {
	for _, candidatePoint := range candidate {
		if !pointInPolygonCollection(candidatePoint, container) {
			return false
		}
	}
	return true
}

func lineCollectionCoveredByPolygonCollection(lines [][]geometryPoint2D, polygons []polygonGeometryItem) bool {
	for _, line := range lines {
		covered := false
		for _, polygon := range polygons {
			if lineStringCoveredByPolygonGeometry(line, polygon.shape) {
				covered = true
				break
			}
		}
		if !covered {
			return false
		}
	}
	return true
}

func lineCollectionContainedByPolygonCollection(lines [][]geometryPoint2D, polygons []polygonGeometryItem) bool {
	for _, line := range lines {
		contained := false
		for _, polygon := range polygons {
			if lineStringCoveredByPolygonGeometry(line, polygon.shape) && !lineStringTouchesPolygonGeometry(line, polygon.shape) {
				contained = true
				break
			}
		}
		if !contained {
			return false
		}
	}
	return true
}

func linearGeometryOverlaps(left, right []byte, leftType, rightType string) (bool, error) {
	leftLines, err := lineGeometryItems(left, leftType)
	if err != nil {
		return false, err
	}
	rightLines, err := lineGeometryItems(right, rightType)
	if err != nil {
		return false, err
	}
	if !lineCollectionHasLinearOverlap(leftLines, rightLines) {
		return false, nil
	}
	if lineCollectionCoveredByLineCollection(leftLines, rightLines) || lineCollectionCoveredByLineCollection(rightLines, leftLines) {
		return false, nil
	}
	return true, nil
}

func polygonCollectionCoveredByPolygonCollection(candidate, container []polygonGeometryItem) (bool, error) {
	for _, candidatePolygon := range candidate {
		covered := false
		for _, containerPolygon := range container {
			itemCovered, err := polygonCoveredByPolygonGeometry(candidatePolygon.payload, candidatePolygon.shape, containerPolygon.shape)
			if err != nil {
				return false, err
			}
			if itemCovered {
				covered = true
				break
			}
		}
		if !covered {
			return false, nil
		}
	}
	return true, nil
}

func polygonGeometryOverlaps(left, right []byte, leftType, rightType string) (bool, error) {
	leftPolygons, err := polygonGeometryItems(left, leftType)
	if err != nil {
		return false, err
	}
	rightPolygons, err := polygonGeometryItems(right, rightType)
	if err != nil {
		return false, err
	}
	hasOverlap := false
	for _, leftPolygon := range leftPolygons {
		for _, rightPolygon := range rightPolygons {
			overlaps, err := polygonOverlapsPolygonGeometry(leftPolygon.payload, rightPolygon.payload, leftPolygon.shape, rightPolygon.shape)
			if err != nil {
				return false, err
			}
			if overlaps {
				hasOverlap = true
				break
			}
		}
		if hasOverlap {
			break
		}
	}
	if !hasOverlap {
		return false, nil
	}
	leftCovered, err := polygonCollectionCoveredByPolygonCollection(leftPolygons, rightPolygons)
	if err != nil {
		return false, err
	}
	if leftCovered {
		return false, nil
	}
	rightCovered, err := polygonCollectionCoveredByPolygonCollection(rightPolygons, leftPolygons)
	if err != nil {
		return false, err
	}
	if rightCovered {
		return false, nil
	}
	return true, nil
}

func multiGeometryDistance(collection, other []byte) (float64, error) {
	count, err := geometryCountFromPayload(collection)
	if err != nil {
		return 0, err
	}
	minDistance := math.MaxFloat64
	for i := int64(1); i <= count; i++ {
		item, err := geometryNFromPayload(collection, i)
		if err != nil {
			return 0, err
		}
		distance, err := geometryDistance([]byte(item), other)
		if err != nil {
			return 0, err
		}
		minDistance = math.Min(minDistance, distance)
	}
	return minDistance, nil
}

func parsePolygonRingPoints(content string) ([]geometryPoint2D, error) {
	items := splitTopLevelGeometryItems(content)
	if len(items) < 3 {
		return nil, moerr.NewInvalidInputNoCtx("invalid polygon payload")
	}

	points := make([]geometryPoint2D, 0, len(items))
	for _, item := range items {
		x, y, err := parseCoordinatePairWithError(item, "invalid polygon payload")
		if err != nil {
			return nil, err
		}
		points = append(points, geometryPoint2D{x: x, y: y})
	}

	if len(points) > 1 && sameGeometryPoint(points[0], points[len(points)-1]) {
		points = points[:len(points)-1]
	}
	if len(points) < 3 {
		return nil, moerr.NewInvalidInputNoCtx("invalid polygon payload")
	}
	return points, nil
}

func pointInPolygon(points []geometryPoint2D, px, py float64) bool {
	if pointOnPolygonBoundary(points, px, py) {
		return false
	}

	inside := false
	j := len(points) - 1
	for i := 0; i < len(points); i++ {
		xi, yi := points[i].x, points[i].y
		xj, yj := points[j].x, points[j].y
		if (yi > py) != (yj > py) {
			crossX := (xj-xi)*(py-yi)/(yj-yi) + xi
			if px < crossX {
				inside = !inside
			}
		}
		j = i
	}
	return inside
}

func pointInPolygonGeometry(polygon geometryPolygon2D, px, py float64) bool {
	if pointOnPolygonBoundaryGeometry(polygon, px, py) {
		return false
	}
	if !pointInPolygon(polygon.outer, px, py) {
		return false
	}
	for _, hole := range polygon.holes {
		if pointInPolygon(hole, px, py) || pointOnPolygonBoundary(hole, px, py) {
			return false
		}
	}
	return true
}

func pointOnPolygonBoundary(points []geometryPoint2D, px, py float64) bool {
	j := len(points) - 1
	for i := 0; i < len(points); i++ {
		if pointOnSegment(px, py, points[j], points[i]) {
			return true
		}
		j = i
	}
	return false
}

func pointOnPolygonBoundaryGeometry(polygon geometryPolygon2D, px, py float64) bool {
	if pointOnPolygonBoundary(polygon.outer, px, py) {
		return true
	}
	for _, hole := range polygon.holes {
		if pointOnPolygonBoundary(hole, px, py) {
			return true
		}
	}
	return false
}

func pointOnSegment(px, py float64, start, end geometryPoint2D) bool {
	const epsilon = 1e-9

	cross := (px-start.x)*(end.y-start.y) - (py-start.y)*(end.x-start.x)
	if math.Abs(cross) > epsilon {
		return false
	}
	if px < math.Min(start.x, end.x)-epsilon || px > math.Max(start.x, end.x)+epsilon {
		return false
	}
	if py < math.Min(start.y, end.y)-epsilon || py > math.Max(start.y, end.y)+epsilon {
		return false
	}
	return true
}

func sameGeometryPoint(a, b geometryPoint2D) bool {
	const epsilon = 1e-9
	return math.Abs(a.x-b.x) <= epsilon && math.Abs(a.y-b.y) <= epsilon
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
