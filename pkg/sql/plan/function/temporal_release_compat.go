// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// The 4.2 catalog stores encoded overload identities and physical result types.
// Keep those executors available for DEFAULT/ON UPDATE and generated/check
// expressions. New SQL binds appended overloads; compatibility costs stay off
// the new execution path, and reading metadata never rewrites shared catalog state.
func legacyExtract(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if !ivecs[0].IsConst() {
		return moerr.NewInternalError(proc.Ctx, "invalid input for extract")
	}
	units := vector.GenerateFunctionStrParameter(ivecs[0])
	unitBytes, unitNull := units.GetStrValue(0)
	unit := string(unitBytes)
	var valueAt func(uint64) (string, bool, error)
	switch ivecs[1].GetType().Oid {
	case types.T_datetime:
		values := vector.GenerateFunctionFixedTypeParameter[types.Datetime](ivecs[1])
		valueAt = func(i uint64) (string, bool, error) {
			v, null := values.GetValue(i)
			if null {
				return "", true, nil
			}
			s, err := legacyFormatExtractDatetime(unit, v)
			return s, false, err
		}
	case types.T_timestamp:
		values := vector.GenerateFunctionFixedTypeParameter[types.Timestamp](ivecs[1])
		zone := proc.GetSessionInfo().TimeZone
		valueAt = func(i uint64) (string, bool, error) {
			v, null := values.GetValue(i)
			if null {
				return "", true, nil
			}
			s, err := legacyFormatExtractDatetime(unit, v.ToDatetime(zone))
			return s, false, err
		}
	case types.T_time:
		values := vector.GenerateFunctionFixedTypeParameter[types.Time](ivecs[1])
		valueAt = func(i uint64) (string, bool, error) {
			v, null := values.GetValue(i)
			if null {
				return "", true, nil
			}
			s, err := legacyFormatExtractTime(unit, v)
			return s, false, err
		}
	default:
		values := vector.GenerateFunctionStrParameter(ivecs[1])
		scale := ivecs[1].GetType().Scale
		if scale == 0 {
			scale = 6
		}
		valueAt = func(i uint64) (string, bool, error) {
			v, null := values.GetStrValue(i)
			if null {
				return "", true, nil
			}
			s, err := legacyFormatExtractVarchar(unit, string(v), scale)
			return s, false, err
		}
	}
	rs := vector.MustFunctionResult[types.Varlena](result)
	for i := uint64(0); i < uint64(length); i++ {
		if unitNull || functionRowSkipped(selectList, i) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		s, null, err := valueAt(i)
		if err != nil {
			return err
		}
		if err := rs.AppendBytes([]byte(s), null); err != nil {
			return err
		}
	}
	return nil
}

func legacyAddTimeString(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return legacyStringTimeArithmetic(ivecs, result, length, selectList, false)
}

func legacySubTimeString(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return legacyStringTimeArithmetic(ivecs, result, length, selectList, true)
}

// Preserve the release's DATETIME result, including its TIME-to-today rule.
// New string arithmetic uses VARCHAR and never reaches this compatibility path.
func legacyStringTimeArithmetic(ivecs []*vector.Vector, result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList, subtract bool) error {
	first := vector.GenerateFunctionStrParameter(ivecs[0])
	second := vector.GenerateFunctionStrParameter(ivecs[1])
	rs := vector.MustFunctionResult[types.Datetime](result)
	for i := uint64(0); i < uint64(length); i++ {
		a, an := first.GetStrValue(i)
		b, bn := second.GetStrValue(i)
		var value types.Datetime
		null := an || bn || functionRowSkipped(selectList, i)
		if !null {
			dt, err := types.ParseDatetime(string(a), 6)
			if err != nil {
				tm, timeErr := types.ParseTime(string(a), 6)
				if timeErr != nil {
					null = true
				} else {
					dt = tm.ToDatetime(6)
				}
			}
			tm, timeErr := types.ParseTime(string(b), 6)
			null = null || dt == types.ZeroDatetime || timeErr != nil
			if !null {
				if subtract {
					value = dt - types.Datetime(tm)
				} else {
					value = dt + types.Datetime(tm)
				}
			}
		}
		if err := rs.Append(value, null); err != nil {
			return err
		}
	}
	return nil
}

func legacyExtractFromDate(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	extractFromDate := func(unit string, d types.Date) (uint32, error) {
		var r uint32
		switch unit {
		case "day":
			r = uint32(d.Day())
		case "week":
			if d != types.ZeroDate {
				r = uint32(d.WeekOfYear2())
			}
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

func legacyFormatExtractDatetime(unit string, d types.Datetime) (string, error) {
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
		if d == types.ZeroDatetime {
			value = "00"
		} else {
			value = fmt.Sprintf("%02d", int(d.ToDate().WeekOfYear2()))
		}
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
	default:
		return "", moerr.NewInternalErrorNoCtx("invalid unit")
	}
	return value, nil
}

func legacyFormatExtractTime(unit string, t types.Time) (string, error) {
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
	default:
		return "", moerr.NewInternalErrorNoCtx("invalid unit")
	}
	return value, nil
}

func legacyFormatExtractVarchar(unit string, t string, scale int32) (string, error) {
	var result string
	if len(t) == 0 {
		result = t
	} else if extractUnitPrefersTime(unit) {
		if value, err := types.ParseTime(t, scale); err == nil {
			result, err = legacyFormatExtractTime(unit, value)
			if err != nil {
				return "", err
			}
		} else if value, err := types.ParseDatetime(t, scale); err == nil {
			result, err = legacyFormatExtractDatetime(unit, value)
			if err != nil {
				return "", err
			}
		} else {
			return "", moerr.NewInternalErrorNoCtx("invalid input")
		}
	} else if value, err := types.ParseDatetime(t, scale); err == nil {
		result, err = legacyFormatExtractDatetime(unit, value)
		if err != nil {
			return "", err
		}
	} else if value, err := types.ParseTime(t, scale); err == nil {
		result, err = legacyFormatExtractTime(unit, value)
		if err != nil {
			return "", err
		}
	} else {
		return "", moerr.NewInternalErrorNoCtx("invalid input")
	}

	return result, nil
}
