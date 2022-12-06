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

package binary

import (
	"strings"
	"unicode"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	// MaxFsp is the maximum digit of fractional seconds part.
	MaxFsp = 6
)

// Convert the string to date type value according to the format string
func StrToDate(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	dateVector := vectors[0]
	formatVector := vectors[1]

	resultType := types.T_date.ToType()
	if !formatVector.IsScalar() {
		return nil, moerr.NewInvalidArgNoCtx("to_date format", "not constant")
	}
	if dateVector.IsScalarNull() || formatVector.IsScalarNull() {
		return proc.AllocScalarNullVector(resultType), nil
	}
	// get the format string.
	formatMask := formatVector.GetString(0)

	if dateVector.IsScalar() {
		datestr := dateVector.GetString(0)
		ctx := make(map[string]int)
		time := NewGeneralTime()
		success := strToDate(time, datestr, formatMask, ctx)
		if !success {
			// should be null
			return proc.AllocScalarNullVector(resultType), nil
		} else {
			if types.ValidDate(int32(time.year), time.month, time.day) {
				resCol := types.FromCalendar(int32(time.year), time.month, time.day)
				return vector.NewConstFixed[types.Date](resultType, 1, resCol, proc.Mp()), nil
			} else {
				// should be null
				return proc.AllocScalarNullVector(resultType), nil
			}
		}
	} else {
		datestrs := vector.MustStrCols(dateVector)
		rNsp := nulls.NewWithSize(len(datestrs))
		resCol, err := CalcStrToDate(datestrs, formatMask, dateVector.Nsp, rNsp)
		if err != nil {
			return nil, err
		}
		resultVector := vector.NewWithFixed[types.Date](resultType, resCol, rNsp, proc.Mp())
		nulls.Set(resultVector.Nsp, dateVector.Nsp)
		return resultVector, nil
	}
}

// Convert the string to datetime type value according to the format string
func StrToDateTime(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	dateVector := vectors[0]
	formatVector := vectors[1]

	resultType := types.T_datetime.ToType()
	if !formatVector.IsScalar() {
		return nil, moerr.NewInvalidArgNoCtx("to_date format", "not constant")
	}
	if dateVector.IsScalarNull() || formatVector.IsScalarNull() {
		return proc.AllocScalarNullVector(resultType), nil
	}
	// get the format string.
	formatMask := formatVector.GetString(0)

	if dateVector.IsScalar() {
		datetimestr := dateVector.GetString(0)
		ctx := make(map[string]int)
		time := NewGeneralTime()
		success := strToDate(time, datetimestr, formatMask, ctx)
		if !success {
			// should be null
			return proc.AllocScalarNullVector(resultType), nil
		} else {
			if types.ValidDatetime(int32(time.year), time.month, time.day) && types.ValidTimeInDay(time.hour, time.minute, time.second) {
				resCol := types.FromClock(int32(time.year), time.month, time.day, time.hour, time.minute, time.second, time.microsecond)
				return vector.NewConstFixed[types.Datetime](resultType, 1, resCol, proc.Mp()), nil
			} else {
				// should be null
				return proc.AllocScalarNullVector(resultType), nil
			}
		}
	} else {
		datetimestrs := vector.MustStrCols(dateVector)
		rNsp := nulls.NewWithSize(len(datetimestrs))
		resCol, err := CalcStrToDatetime(datetimestrs, formatMask, dateVector.Nsp, rNsp)
		if err != nil {
			return nil, err
		}
		resultVector := vector.NewWithFixed[types.Datetime](resultType, resCol, nulls.NewWithSize(len(resCol)), proc.Mp())
		nulls.Set(resultVector.Nsp, dateVector.Nsp)
		return resultVector, nil
	}
}

// // Convert the string to time type value according to the format string,such as '09:30:17'
func StrToTime(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	dateVector := vectors[0]
	formatVector := vectors[1]

	resultType := types.T_time.ToType()
	if !formatVector.IsScalar() {
		return nil, moerr.NewInvalidArgNoCtx("to_date format", "not constant")
	}
	if dateVector.IsScalarNull() || formatVector.IsScalarNull() {
		return proc.AllocScalarNullVector(resultType), nil
	}
	// get the format string.
	formatMask := formatVector.GetString(0)

	if dateVector.IsScalar() {
		timestr := dateVector.GetString(0)

		ctx := make(map[string]int)
		time := NewGeneralTime()
		success := strToDate(time, timestr, formatMask, ctx)
		if !success {
			// should be null
			return proc.AllocScalarNullVector(resultType), nil
		} else {
			if types.ValidTime(uint64(time.hour), uint64(time.minute), uint64(time.second)) {
				resCol := types.FromTimeClock(false, uint64(time.hour), time.minute, time.second, time.microsecond)
				return vector.NewConstFixed[types.Time](resultType, 1, resCol, proc.Mp()), nil
			} else {
				// should be null
				return proc.AllocScalarNullVector(resultType), nil
			}
		}
	} else {
		timestrs := vector.MustStrCols(dateVector)
		rNsp := nulls.NewWithSize(len(timestrs))
		resCol, err := CalcStrToTime(timestrs, formatMask, dateVector.Nsp, rNsp)
		if err != nil {
			return nil, err
		}
		resultVector := vector.NewWithFixed[types.Time](resultType, resCol, nulls.NewWithSize(len(resCol)), proc.Mp())
		nulls.Set(resultVector.Nsp, dateVector.Nsp)
		return resultVector, nil
	}
}

func CalcStrToDatetime(timestrs []string, format string, ns *nulls.Nulls, rNsp *nulls.Nulls) ([]types.Datetime, error) {
	res := make([]types.Datetime, len(timestrs))
	time := NewGeneralTime()
	for idx, timestr := range timestrs {
		if nulls.Contains(ns, uint64(idx)) {
			continue
		}
		success := CoreStrToDate(time, timestr, format)
		if !success {
			// should be null
			nulls.Add(rNsp, uint64(idx))
		} else {
			if types.ValidDatetime(int32(time.year), time.month, time.day) && types.ValidTimeInDay(time.hour, time.minute, time.second) {
				res[idx] = types.FromClock(int32(time.year), time.month, time.day, time.hour, time.minute, time.second, time.microsecond)
			} else {
				// should be null
				nulls.Add(rNsp, uint64(idx))
			}
		}
		time.ResetTime()
	}
	return res, nil
}

func CalcStrToDate(timestrs []string, format string, ns *nulls.Nulls, rNsp *nulls.Nulls) ([]types.Date, error) {
	res := make([]types.Date, len(timestrs))
	time := NewGeneralTime()
	for idx, timestr := range timestrs {
		if nulls.Contains(ns, uint64(idx)) {
			continue
		}
		success := CoreStrToDate(time, timestr, format)
		if !success {
			// should be null
			nulls.Add(rNsp, uint64(idx))
		} else {
			if types.ValidDate(int32(time.year), time.month, time.day) {
				res[idx] = types.FromCalendar(int32(time.year), time.month, time.day)
			} else {
				// should be null
				nulls.Add(rNsp, uint64(idx))
			}
		}
		time.ResetTime()
	}
	return res, nil
}

func CalcStrToTime(timestrs []string, format string, ns *nulls.Nulls, rNsp *nulls.Nulls) ([]types.Time, error) {
	res := make([]types.Time, len(timestrs))
	time := NewGeneralTime()
	for idx, timestr := range timestrs {
		if nulls.Contains(ns, uint64(idx)) {
			continue
		}
		success := CoreStrToDate(time, timestr, format)
		if !success {
			// should be null
			nulls.Add(rNsp, uint64(idx))
		} else {
			if types.ValidTime(uint64(time.hour), uint64(time.minute), uint64(time.second)) {
				res[idx] = types.FromTimeClock(false, uint64(time.hour), time.minute, time.second, time.microsecond)
			} else {
				// should be null
				nulls.Add(rNsp, uint64(idx))
			}
		}
		time.ResetTime()
	}
	return res, nil
}

func CoreStrToDate(t *GeneralTime, date string, format string) bool {
	ctx := make(map[string]int)
	success := strToDate(t, date, format, ctx)
	if !success {
		return false
	}
	if err := checkMysqlTime(t, ctx); err != nil {
		return false
	}
	return true
}

// strToDate converts date string according to format,
// the value will be stored in argument ctx. the second return value is true when success
func strToDate(t *GeneralTime, date string, format string, ctx map[string]int) (success bool) {
	date = trimWhiteSpace(date)
	format = trimWhiteSpace(format)

	token, formatRemain, succ := nextFormatToken(format)
	if !succ {
		return false
	}

	if token == "" {
		if len(date) != 0 {
			// Extra characters at the end of date are ignored
			return true
		}
		// Normal case. Both token and date are empty now.
		return true
	}

	if len(date) == 0 {
		ctx[token] = 0
		return true
	}

	dateRemain, succ := matchDateWithToken(t, date, token, ctx)
	if !succ {
		return false
	}

	return strToDate(t, dateRemain, formatRemain, ctx)
}

// checkMysqlTime fixes the Time use the values in the context.
func checkMysqlTime(t *GeneralTime, ctx map[string]int) error {
	if valueAMorPm, ok := ctx["%p"]; ok {
		if _, ok := ctx["%H"]; ok {
			return moerr.NewInternalErrorNoCtx("Truncated incorrect %-.64s value: '%-.128s'", "time", t)
		}
		if t.getHour() == 0 {
			return moerr.NewInternalErrorNoCtx("Truncated incorrect %-.64s value: '%-.128s'", "time", t)
		}
		if t.getHour() == 12 {
			// 12 is a special hour.
			switch valueAMorPm {
			case timeOfAM:
				t.setHour(0)
			case timeOfPM:
				t.setHour(12)
			}
			return nil
		}
		if valueAMorPm == timeOfPM {
			t.setHour(t.getHour() + 12)
		}
	} else {
		if _, ok := ctx["%h"]; ok && t.getHour() == 12 {
			t.setHour(0)
		}
	}
	return nil
}

// Judge the return value type of the str_to_date function according to the value of the fromat parameter
func JudgmentToDateReturnType(format string) (tp types.T, fsp int) {
	isTime, isDate := GetTimeFormatType(format)
	if isTime && !isDate {
		tp = types.T_time
	} else if !isTime && isDate {
		tp = types.T_date
	} else {
		tp = types.T_datetime
	}
	if strings.Contains(format, "%f") {
		fsp = MaxFsp
	}
	return tp, MaxFsp
}

// GetTimeFormatType checks the type(Time, Date or Datetime) of a format string.
func GetTimeFormatType(format string) (isTime, isDate bool) {
	format = trimWhiteSpace(format)
	var token string
	var succ bool
	for {
		token, format, succ = nextFormatToken(format)
		if len(token) == 0 {
			break
		}
		if !succ {
			isTime, isDate = false, false
			break
		}
		if len(token) >= 2 && token[0] == '%' {
			switch token[1] {
			case 'h', 'H', 'i', 'I', 's', 'S', 'k', 'l', 'f', 'r', 'T':
				isTime = true
			case 'y', 'Y', 'm', 'M', 'c', 'b', 'D', 'd', 'e':
				isDate = true
			}
		}
		if isTime && isDate {
			break
		}
	}
	return
}

// trim spaces in strings
func trimWhiteSpace(input string) string {
	for i, c := range input {
		if !unicode.IsSpace(c) {
			return input[i:]
		}
	}
	return ""
}

// nextFormatToken takes next one format control token from the string.
// such as: format "%d %H %m" will get token "%d" and the remain is " %H %m".
func nextFormatToken(format string) (token string, remain string, success bool) {
	if len(format) == 0 {
		return "", "", true
	}

	// Just one character.
	if len(format) == 1 {
		if format[0] == '%' {
			return "", "", false
		}
		return format, "", true
	}

	// More than one character.
	if format[0] == '%' {
		return format[:2], format[2:], true
	}

	return format[:1], format[1:], true
}
