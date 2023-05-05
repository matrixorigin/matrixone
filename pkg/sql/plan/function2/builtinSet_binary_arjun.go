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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function2/function2Util"
	"github.com/matrixorigin/matrixone/pkg/vectorize/datediff"
	"github.com/matrixorigin/matrixone/pkg/vectorize/instr"
	"github.com/matrixorigin/matrixone/pkg/vectorize/timediff"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"math"
	"strings"
)

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
	} else {
		v1str := string(v1)
		for i := uint64(0); i < uint64(length); i++ {
			v2, null2 := p2.GetValue(i)
			if null2 {
				if err = rs.Append(0, true); err != nil {
					return err
				}
			} else {
				res, _ := extractFromDate(v1str, v2)
				if err = rs.Append(res, false); err != nil {
					return err
				}
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
	} else {
		v1str := string(v1)
		for i := uint64(0); i < uint64(length); i++ {
			v2, null2 := p2.GetValue(i)
			if null2 {
				if err = rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				res, _ := extractFromDatetime(v1str, v2)
				if err = rs.AppendBytes(function2Util.QuickStrToBytes(res), false); err != nil {
					return err
				}
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
	} else {
		v1str := string(v1)
		for i := uint64(0); i < uint64(length); i++ {
			v2, null2 := p2.GetValue(i)
			if null2 {
				if err = rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				res, _ := extractFromTime(v1str, v2)
				if err = rs.AppendBytes(function2Util.QuickStrToBytes(res), false); err != nil {
					return err
				}
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
	} else {
		v1str := string(v1)
		p2scale := p2.GetType().Scale
		for i := uint64(0); i < uint64(length); i++ {
			v2, null2 := p2.GetStrValue(i)
			if null2 {
				if err = rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				res, err := extractFromVarchar(v1str, function2Util.QuickBytesToStr(v2), p2scale)
				if err != nil {
					return err
				}
				if err = rs.AppendBytes(function2Util.QuickStrToBytes(res), false); err != nil {
					return err
				}
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

	//TODO: ignoring maxLen: Original code:https://github.com/m-schen/matrixone/blob/0c480ca11b6302de26789f916a3e2faca7f79d47/pkg/sql/plan/function/builtin/binary/instr.go#L32
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		v2, null2 := p2.GetStrValue(i)
		if null1 || null2 {
			if err = rs.Append(0, true); err != nil {
				return err
			}
		} else {
			str1 := function2Util.QuickBytesToStr(v1)
			str2 := function2Util.QuickBytesToStr(v2)

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
			res := evalLeft(function2Util.QuickBytesToStr(v1), v2)
			if err = rs.AppendBytes(function2Util.QuickStrToBytes(res), false); err != nil {
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

// TIMEDIFF

func TimeDiff[T timediff.DiffT](ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) (err error) {

	p1 := vector.GenerateFunctionFixedTypeParameter[T](ivecs[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[T](ivecs[1])
	rs := vector.MustFunctionResult[types.Time](result)

	//TODO: ignoring scale: Original code: https://github.com/m-schen/matrixone/blob/a4b3a641c3daaa10972f17db091c0eb88554c5c2/pkg/sql/plan/function/builtin/binary/timediff.go#L33
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

func timeDiff[T timediff.DiffT](v1, v2 T) (types.Time, error) {
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
			res, _ := datediff.TimeStampDiff(function2Util.QuickBytesToStr(v1), v2, v3)
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
			v1Str := function2Util.QuickBytesToStr(v1)
			v2Str := function2Util.QuickBytesToStr(v2)
			var res string
			if v2Str == "" {
				res = v1Str
			} else {
				res = strings.ReplaceAll(v1Str, v2Str, function2Util.QuickBytesToStr(v3))
			}

			if err = rs.AppendBytes(function2Util.QuickStrToBytes(res), false); err != nil {
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
