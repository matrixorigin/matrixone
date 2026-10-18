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
	"bytes"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Incomplete calendars exist only while evaluating a string. They must never
// be encoded as a DATE/DATETIME: those types represent complete dates or zero.
type incompleteCalendar struct {
	year       int32
	month, day uint8
	clock      types.Datetime // surrogate used exclusively for validated clock fields
}

func parseIncompleteCalendar(value string) (incompleteCalendar, bool) {
	s := strings.TrimSpace(value)
	// A complete ISO calendar cannot need raw zero-component handling. Leave
	// calendar/clock validation to the caller's authoritative parser, avoiding
	// a second component parse on the ordinary VARCHAR date path.
	if len(s) >= 10 && s[4] == '-' && s[7] == '-' &&
		s[5] >= '0' && s[5] <= '9' && s[6] >= '0' && s[6] <= '9' && (s[5] != '0' || s[6] != '0') &&
		s[8] >= '0' && s[8] <= '9' && s[9] >= '0' && s[9] <= '9' && (s[8] != '0' || s[9] != '0') {
		return incompleteCalendar{}, false
	}
	dateText, clockText := s, ""
	if len(s) >= 14 && isAllDigits(s[:14]) {
		dateText = s[:8]
		clockText = " " + s[8:10] + ":" + s[10:12] + ":" + s[12:]
	} else if sep := strings.IndexAny(s, " T"); sep >= 0 {
		dateText, clockText = s[:sep], s[sep:]
	}
	year, month, day, err := types.ParseDateCastComponents(dateText)
	if err != nil || (month != 0 && day != 0) || (year == 0 && month == 0 && day == 0) {
		return incompleteCalendar{}, false
	}
	// Parse only the clock against a valid surrogate. Calendar directives below
	// always read the original components, never the surrogate's calendar.
	clock, err := types.ParseDatetime("2000-01-01"+clockText, 6)
	if err != nil || clock.ToDate() != types.DateFromCalendar(2000, 1, 1) {
		return incompleteCalendar{}, false
	}
	return incompleteCalendar{year: year, month: month, day: day, clock: clock}, true
}

func appendIncompleteCalendarWarning(proc *process.Process, value string) {
	if proc == nil {
		return
	}
	if sink, ok := proc.GetWarningSink().(warningDiagnosticAppender); ok {
		sink.AppendWarningDiagnostic(moerr.ER_TRUNCATED_WRONG_VALUE,
			fmt.Sprintf("Incorrect datetime value: '%-.128s'", value))
	}
}

// These helpers implement the raw-field formatting contract of MySQL 8.0.45
// make_date_time/calc_daynr/calc_week. In particular, week arithmetic has a
// uint32 days intermediate; normalized date arithmetic gives different results
// for zero components. No validated-calendar consumer uses these helpers.
func rawCalendarDayNumber(year, month, day int) int {
	if year == 0 && month == 0 {
		return 0
	}
	n := 365*year + 31*(month-1) + day
	if month <= 2 {
		year--
	} else {
		n -= (month*4 + 23) / 10
	}
	return n + year/4 - ((year/100+1)*3)/4
}

func rawCalendarYearDays(year uint32) uint32 {
	if year%4 == 0 && (year%100 != 0 || (year%400 == 0 && year != 0)) {
		return 366
	}
	return 365
}

func (c incompleteCalendar) week(mode int, qualified bool) (uint32, uint32) {
	behavior := mode & 7
	if behavior&1 == 0 {
		behavior ^= 4
	}
	if qualified {
		behavior |= 2
	}
	monday, weekYear, firstWeekday := behavior&1 != 0, behavior&2 != 0, behavior&4 != 0
	daynr := rawCalendarDayNumber(int(c.year), int(c.month), int(c.day))
	first := rawCalendarDayNumber(int(c.year), 1, 1)
	weekday := (first + 5) % 7
	if !monday {
		weekday = (weekday + 1) % 7
	}
	year := uint32(c.year)
	if c.month == 1 && int(c.day) <= 7-weekday {
		if !weekYear && ((firstWeekday && weekday != 0) || (!firstWeekday && weekday >= 4)) {
			return year, 0
		}
		weekYear = true
		year--
		days := rawCalendarYearDays(year)
		first -= int(days)
		weekday = (weekday + 53*7 - int(days)) % 7
	}
	var days uint32
	if (firstWeekday && weekday != 0) || (!firstWeekday && weekday >= 4) {
		days = uint32(daynr - (first + 7 - weekday))
	} else {
		days = uint32(daynr - (first - weekday))
	}
	if weekYear && days >= 52*7 {
		weekday = (weekday + int(rawCalendarYearDays(year))) % 7
		if (!firstWeekday && weekday < 4) || (firstWeekday && weekday == 0) {
			return year + 1, 1
		}
	}
	return year, days/7 + 1
}

func (c incompleteCalendar) formatDirective(locale *temporalLocale, b rune, buf *bytes.Buffer) (bool, error) {
	switch b {
	case 'Y':
		FormatInt2BufByWidth(int(c.year), 4, buf)
	case 'y':
		FormatInt2BufByWidth(int(c.year)%100, 2, buf)
	case 'm':
		FormatInt2BufByWidth(int(c.month), 2, buf)
	case 'c':
		fmt.Fprint(buf, c.month)
	case 'd':
		FormatInt2BufByWidth(int(c.day), 2, buf)
	case 'e':
		fmt.Fprint(buf, c.day)
	case 'D':
		fmt.Fprintf(buf, "%d%s", c.day, AbbrDayOfMonth(int(c.day)))
	case 'M', 'b':
		if c.month == 0 {
			return true, nil
		}
		if b == 'M' {
			buf.WriteString(locale.localizedMonth(int(c.month)))
		} else {
			buf.WriteString(locale.localizedMonthAbbrev(int(c.month)))
		}
	case 'W', 'a', 'w':
		if c.year == 0 && c.month == 0 {
			return true, nil
		}
		weekday := (rawCalendarDayNumber(int(c.year), int(c.month), int(c.day)) + 6) % 7
		if b == 'w' {
			fmt.Fprint(buf, weekday)
		} else if b == 'W' {
			buf.WriteString(locale.localizedWeekday(weekday))
		} else {
			buf.WriteString(locale.localizedWeekdayAbbrev(weekday))
		}
	case 'j':
		n := rawCalendarDayNumber(int(c.year), int(c.month), int(c.day)) - rawCalendarDayNumber(int(c.year), 1, 1) + 1
		FormatInt2BufByWidth(n, 3, buf)
	case 'U', 'u', 'V', 'v', 'X', 'x':
		mode := 0
		switch b {
		case 'u':
			mode = 1
		case 'V', 'X':
			mode = 2
		case 'v', 'x':
			mode = 3
		}
		year, week := c.week(mode, b == 'X' || b == 'x')
		if b == 'X' || b == 'x' {
			fmt.Fprintf(buf, "%04d", year)
		} else {
			fmt.Fprintf(buf, "%02d", week)
		}
	default:
		return makeDateFormatWithLocale(locale, nil, c.clock, b, buf)
	}
	return false, nil
}

func stringCalendarTotal(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList, seconds bool) error {
	truncateFractional, err := process.ResolveTimeTruncateFractional(proc)
	if err != nil {
		return err
	}
	source := vector.GenerateFunctionStrParameter(ivecs[0])
	rs := vector.MustFunctionResult[int64](result)
	unit, offset := intervalUnitDAY, int64(ADZeroDays)
	if seconds {
		unit, offset = intervalUnitSECOND, ADZeroSeconds
	}
	for i := uint64(0); i < uint64(length); i++ {
		if functionRowSkipped(selectList, i) {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		value, null := source.GetStrValue(i)
		if null || len(value) == 0 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		text := functionUtil.QuickBytesToStr(value)
		parseText := text
		if truncateFractional {
			parseText = truncateTemporalFractionForParse(text, 6)
		}
		if _, partial := parseIncompleteCalendar(parseText); partial {
			appendIncompleteCalendarWarning(proc, text)
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		dt, err := types.ParseDatetime(parseText, 6)
		if err != nil {
			return err
		}
		if dt == types.ZeroDatetime {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		n := DateTimeDiff(unit, types.DatetimeEpoch, dt) + offset
		if err := rs.Append(n, false); err != nil {
			return err
		}
	}
	return nil
}
func stringToDays(v []*vector.Vector, r vector.FunctionResultWrapper, p *process.Process, n int, s *FunctionSelectList) error {
	return stringCalendarTotal(v, r, p, n, s, false)
}
func stringToSeconds(v []*vector.Vector, r vector.FunctionResultWrapper, p *process.Process, n int, s *FunctionSelectList) error {
	return stringCalendarTotal(v, r, p, n, s, true)
}

func stringCalendarIndex(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList, week bool) error {
	source := vector.GenerateFunctionStrParameter(ivecs[0])
	var modes vector.FunctionParameterWrapper[int64]
	defaultMode := 0
	if week {
		if len(ivecs) > 1 {
			modes = vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])
		} else {
			var err error
			defaultMode, err = getDefaultWeekFormatMode(proc)
			if err != nil {
				return err
			}
		}
	}
	var weeks *vector.FunctionResult[int32]
	var days *vector.FunctionResult[int64]
	if week {
		weeks = vector.MustFunctionResult[int32](result)
	} else {
		days = vector.MustFunctionResult[int64](result)
	}
	appendValue := func(n int, null bool) error {
		if week {
			return weeks.Append(int32(n), null)
		}
		return days.Append(int64(n), null)
	}
	for i := uint64(0); i < uint64(length); i++ {
		if functionRowSkipped(selectList, i) {
			if err := appendValue(0, true); err != nil {
				return err
			}
			continue
		}
		value, null := source.GetStrValue(i)
		if null {
			if err := appendValue(0, true); err != nil {
				return err
			}
			continue
		}
		text := functionUtil.QuickBytesToStr(value)
		if _, partial := parseIncompleteCalendar(text); partial {
			appendIncompleteCalendarWarning(proc, text)
			if err := appendValue(0, true); err != nil {
				return err
			}
			continue
		}
		date, err := types.ParseDateCast(text)
		if err != nil || date == types.ZeroDate {
			if err := appendValue(0, true); err != nil {
				return err
			}
			continue
		}
		n := int(date.DayOfWeek()) + 1
		if week {
			mode := defaultMode
			if modes != nil {
				mode = weekModeAt(modes, i)
			}
			n = date.Week(mode)
		}
		if err := appendValue(n, false); err != nil {
			return err
		}
	}
	return nil
}
func stringDayOfWeek(v []*vector.Vector, r vector.FunctionResultWrapper, p *process.Process, n int, s *FunctionSelectList) error {
	return stringCalendarIndex(v, r, p, n, s, false)
}
func stringWeek(v []*vector.Vector, r vector.FunctionResultWrapper, p *process.Process, n int, s *FunctionSelectList) error {
	return stringCalendarIndex(v, r, p, n, s, true)
}

// Mixed DATEDIFF overloads keep typed operands typed. Only string operands
// inspect raw components and own their rejection diagnostic.
func stringDateDiff(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	operand := func(v *vector.Vector) func(uint64) (types.Date, bool) {
		if v.GetType().Oid == types.T_date {
			p := vector.GenerateFunctionFixedTypeParameter[types.Date](v)
			return p.GetValue
		}
		p := vector.GenerateFunctionStrParameter(v)
		return func(i uint64) (types.Date, bool) {
			value, null := p.GetStrValue(i)
			if null {
				return 0, true
			}
			text := functionUtil.QuickBytesToStr(value)
			if _, partial := parseIncompleteCalendar(text); partial {
				appendIncompleteCalendarWarning(proc, text)
				return 0, true
			}
			date, err := types.ParseDateCast(text)
			return date, err != nil
		}
	}
	left, right := operand(ivecs[0]), operand(ivecs[1])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		if functionRowSkipped(selectList, i) || ivecs[0].IsNull(i) || ivecs[1].IsNull(i) {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		a, an := left(i)
		b, bn := right(i)
		if an || bn || a == types.ZeroDate || b == types.ZeroDate {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		if err := rs.Append(int64(a-b), false); err != nil {
			return err
		}
	}
	return nil
}
