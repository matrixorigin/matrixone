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
	"unicode"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const strToDateMaxFsp = 6

func normalizeStrToDateScale(scale int32) int32 {
	if scale < 0 {
		return 0
	}
	if scale > strToDateMaxFsp {
		return strToDateMaxFsp
	}
	return scale
}

func builtInStrToDate(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])

	rs := vector.MustFunctionResult[types.Date](result)

	time := NewGeneralTime()
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		v2, null2 := p2.GetStrValue(i)
		if null1 || null2 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			time.ResetTime()

			success := coreStrToDate(proc.Ctx, time, functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v2))
			if success {
				if types.ValidDate(int32(time.year), time.month, time.day) {
					value := types.DateFromCalendar(int32(time.year), time.month, time.day)
					if err := rs.Append(value, false); err != nil {
						return err
					}
					continue
				}
			}
			if err := rs.Append(0, true); err != nil {
				return err
			}
		}
	}

	return nil
}

func builtInStrToDatetime(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])

	rs := vector.MustFunctionResult[types.Datetime](result)

	time := NewGeneralTime()
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		v2, null2 := p2.GetStrValue(i)
		if null1 || null2 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			time.ResetTime()

			success := coreStrToDate(proc.Ctx, time, functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v2))
			if success {
				if types.ValidDatetime(int32(time.year), time.month, time.day) && types.ValidTimeInDay(time.hour, time.minute, time.second) {
					value := types.DatetimeFromClock(int32(time.year), time.month, time.day, time.hour, time.minute, time.second, time.microsecond)
					if err := rs.Append(value, false); err != nil {
						return err
					}
					continue
				}
			}
			if err := rs.Append(0, true); err != nil {
				return err
			}
		}
	}

	return nil
}

func builtInStrToTime(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])

	rs := vector.MustFunctionResult[types.Time](result)

	time := NewGeneralTime()
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		v2, null2 := p2.GetStrValue(i)
		if null1 || null2 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			time.ResetTime()

			success := coreStrToDate(proc.Ctx, time, functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v2))
			if success {
				hours := uint64(time.day)*24 + uint64(time.hour)
				if types.ValidTime(hours, uint64(time.minute), uint64(time.second)) {
					value := types.TimeFromClock(false, hours, time.minute, time.second, time.microsecond)
					if types.IsMySQLTime(value) {
						if err := rs.Append(value, false); err != nil {
							return err
						}
						continue
					}
				}
			}
			if err := rs.Append(0, true); err != nil {
				return err
			}
		}
	}

	return nil
}

func coreStrToDate(cctx context.Context, t *GeneralTime, date string, format string) bool {
	ctx := make(map[string]int)
	success := strToDate2(cctx, t, date, format, ctx)
	if !success {
		return false
	}
	if err := checkMysqlTime(cctx, t, ctx); err != nil {
		return false
	}
	return true
}

// strToDate converts date string according to format,
// the value will be stored in argument ctx. the second return value is true when success
func strToDate2(cctx context.Context, t *GeneralTime, date string, format string, ctx map[string]int) (success bool) {
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
		if _, parsed := ctx[token]; !parsed {
			ctx[token] = 0
		}
		return true
	}

	dateRemain, succ := matchDateWithToken(t, date, token, ctx)
	if !succ {
		return false
	}

	return strToDate2(cctx, t, dateRemain, formatRemain, ctx)
}

// checkMysqlTime fixes the Time use the values in the context.
func checkMysqlTime(cctx context.Context, t *GeneralTime, ctx map[string]int) error {
	if valueAMorPm, ok := ctx["%p"]; ok {
		if _, ok := ctx["%H"]; ok {
			return moerr.NewInternalErrorf(cctx, "Truncated incorrect %-.64s value: '%-.128s'", "time", t)
		}
		if t.getHour() == 0 {
			return moerr.NewInternalErrorf(cctx, "Truncated incorrect %-.64s value: '%-.128s'", "time", t)
		}
		if t.getHour() == 12 {
			// 12 is a special hour.
			switch valueAMorPm {
			case timeOfAM:
				t.setHour(0)
			case timeOfPM:
				t.setHour(12)
			}
		} else if valueAMorPm == timeOfPM {
			t.setHour(t.getHour() + 12)
		}
	} else {
		if _, ok := ctx["%h"]; ok && t.getHour() == 12 {
			t.setHour(0)
		}
	}
	if !resolveStrToDateCalendar(t, ctx) {
		return moerr.NewInvalidInput(cctx, "invalid STR_TO_DATE calendar fields")
	}
	return nil
}

// resolveStrToDateCalendar applies derived dates after all format fields have
// been parsed. MySQL gives week dates precedence over day-of-year, and both
// take precedence over directly parsed month and day.
func resolveStrToDateCalendar(t *GeneralTime, ctx map[string]int) bool {
	year := int(t.year)
	ordinal, hasOrdinal := ctx["%j"]
	if hasOrdinal && ordinal != 0 {
		if !setStrToDateOrdinal(t, year, ordinal) {
			return false
		}
		year = int(t.year)
	}
	week, hasWeek := ctx["week"]
	weekday, hasWeekday := ctx["weekday"]
	if !hasWeek || !hasWeekday {
		return true
	}
	mode := ctx["week_mode"]
	weekYear, hasWeekYear := ctx["week_year"]
	if mode >= 2 {
		if !hasWeekYear || ctx["week_year_mode"] != mode {
			return false
		}
		year = weekYear
	} else if hasWeekYear {
		return false
	}
	if year < 0 || year > 9999 {
		return false
	}
	jan1 := int(types.DayOfWeekFromCalendar(int32(year), 1, 1))
	first := 0
	if mode == 0 || mode == 2 {
		// Week 1 begins on the first Sunday of the calendar year.
		first = (7 - jan1) % 7
	} else {
		// ISO week 1 is the Monday in the week containing January 4.
		first = 3 - (jan1+3+6)%7
	}
	dayOffset := first + (week-1)*7
	if mode == 0 || mode == 2 {
		dayOffset += weekday
	} else {
		dayOffset += (weekday + 6) % 7
	}
	return setStrToDateOrdinal(t, year, dayOffset+1)
}

// MySQL allows day-of-year values through 999 and week 53 to cross a year
// boundary. Normalize with its year-zero rule (year zero is not leap).
func setStrToDateOrdinal(t *GeneralTime, year, ordinal int) bool {
	for ordinal <= 0 {
		year--
		if year < 0 {
			return false
		}
		ordinal += strToDateDaysInYear(year)
	}
	for ordinal > strToDateDaysInYear(year) {
		ordinal -= strToDateDaysInYear(year)
		year++
		if year > 9999 {
			return false
		}
	}
	monthDays := [...]int{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	if strToDateDaysInYear(year) == 366 {
		monthDays[1] = 29
	}
	for month, days := range monthDays {
		if ordinal <= days {
			// MySQL's day-number conversion reserves year zero for the
			// zero date. A later week directive may still replace it.
			if year == 0 {
				t.setYear(0)
				t.setMonth(0)
				t.setDay(0)
				return true
			}
			t.setYear(uint16(year))
			t.setMonth(uint8(month + 1))
			t.setDay(uint8(ordinal))
			return true
		}
		ordinal -= days
	}
	return false
}

func strToDateDaysInYear(year int) int {
	if year != 0 && year%4 == 0 && (year%100 != 0 || year%400 == 0) {
		return 366
	}
	return 365
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
