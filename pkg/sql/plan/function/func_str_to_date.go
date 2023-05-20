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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"unicode"
)

func builtInStrToDate(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	if !parameters[1].IsConst() {
		return moerr.NewInvalidArg(proc.Ctx, "to_date format", "not constant")
	}

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

			timestr := string(v1)
			format := string(v2)
			success := coreStrToDate(proc.Ctx, time, timestr, format)
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

func builtInStrToDatetime(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	if !parameters[1].IsConst() {
		return moerr.NewInvalidArg(proc.Ctx, "to_date format", "not constant")
	}

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

			timestr := string(v1)
			format := string(v2)
			success := coreStrToDate(proc.Ctx, time, timestr, format)
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

func builtInStrToTime(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	if !parameters[1].IsConst() {
		return moerr.NewInvalidArg(proc.Ctx, "to_date format", "not constant")
	}

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

			timestr := string(v1)
			format := string(v2)
			success := coreStrToDate(proc.Ctx, time, timestr, format)
			if success {
				if types.ValidTime(uint64(time.hour), uint64(time.minute), uint64(time.second)) {
					value := types.TimeFromClock(false, uint64(time.hour), time.minute, time.second, time.microsecond)
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
		ctx[token] = 0
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
			return moerr.NewInternalError(cctx, "Truncated incorrect %-.64s value: '%-.128s'", "time", t)
		}
		if t.getHour() == 0 {
			return moerr.NewInternalError(cctx, "Truncated incorrect %-.64s value: '%-.128s'", "time", t)
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
