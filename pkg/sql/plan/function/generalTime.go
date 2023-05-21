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
	"fmt"
	"strings"
	"unicode"
)

// GeneralTime is the internal struct type for Time.
type GeneralTime struct {
	year        uint16
	month       uint8
	day         uint8
	hour        uint8
	minute      uint8
	second      uint8
	microsecond uint32
}

func NewGeneralTime() *GeneralTime {
	return &GeneralTime{}
}

func FromDate(year int, month int, day int, hour int, minute int, second int, microsecond int) GeneralTime {
	return GeneralTime{
		year:        uint16(year),
		month:       uint8(month),
		day:         uint8(day),
		hour:        uint8(hour),
		minute:      uint8(minute),
		second:      uint8(second),
		microsecond: uint32(microsecond),
	}
}

// Reset GeneralTime to initialization state
func (t *GeneralTime) ResetTime() {
	*t = GeneralTime{}
}

// String implements fmt.Stringer.
func (t GeneralTime) String() string {
	return fmt.Sprintf("{%d %d %d %d %d %d %d}", t.getYear(), t.getMonth(), t.getDay(), t.getHour(), t.getMinute(), t.getSecond(), t.getMicrosecond())
}

func (t GeneralTime) getYear() uint16 {
	return t.year
}

func (t *GeneralTime) setYear(year uint16) {
	t.year = year
}

func (t GeneralTime) getMonth() uint8 {
	return t.month
}

func (t *GeneralTime) setMonth(month uint8) {
	t.month = month
}

func (t GeneralTime) getDay() uint8 {
	return t.day
}

func (t *GeneralTime) setDay(day uint8) {
	t.day = day
}

func (t GeneralTime) getHour() uint8 {
	return t.hour
}

func (t *GeneralTime) setHour(hour uint8) {
	t.hour = hour
}

func (t GeneralTime) getMinute() uint8 {
	return t.minute
}

func (t *GeneralTime) setMinute(minute uint8) {
	t.minute = minute
}

// Minute returns the minute value.
func (t GeneralTime) Minute() int {
	return int(t.getMinute())
}

func (t GeneralTime) getSecond() uint8 {
	return t.second
}

func (t *GeneralTime) setSecond(second uint8) {
	t.second = second
}

func (t GeneralTime) getMicrosecond() uint32 {
	return t.microsecond
}

func (t *GeneralTime) setMicrosecond(microsecond uint32) {
	t.microsecond = microsecond
}

// The month represents one month of the year (January=1,...).
type Month int

const (
	January Month = 1 + iota
	February
	March
	April
	May
	June
	July
	August
	September
	October
	November
	December
)

var monthAbbrev = map[string]Month{
	"jan": January,
	"feb": February,
	"mar": March,
	"apr": April,
	"may": May,
	"jun": June,
	"jul": July,
	"aug": August,
	"sep": September,
	"oct": October,
	"nov": November,
	"dec": December,
}

type dateFormatParser func(t *GeneralTime, date string, ctx map[string]int) (remain string, succ bool)

var dateFormatParserTable = map[string]dateFormatParser{
	"%b": abbreviatedMonth,      // Abbreviated month name (Jan..Dec)
	"%c": monthNumeric,          // Month, numeric (0..12)
	"%d": dayOfMonthNumeric,     // Day of the month, numeric (0..31)
	"%e": dayOfMonthNumeric,     // Day of the month, numeric (0..31)
	"%f": microSeconds,          // Microseconds (000000..999999)
	"%h": hour12Numeric,         // Hour (01..12)
	"%H": hour24Numeric,         // Hour (00..23)
	"%I": hour12Numeric,         // Hour (01..12)
	"%i": minutesNumeric,        // Minutes, numeric (00..59)
	"%j": dayOfYearNumeric,      // Day of year (001..366)
	"%k": hour24Numeric,         // Hour (0..23)
	"%l": hour12Numeric,         // Hour (1..12)
	"%M": fullNameMonth,         // Month name (January..December)
	"%m": monthNumeric,          // Month, numeric (00..12)
	"%p": isAMOrPM,              // AM or PM
	"%r": time12Hour,            // Time, 12-hour (hh:mm:ss followed by AM or PM)
	"%s": secondsNumeric,        // Seconds (00..59)
	"%S": secondsNumeric,        // Seconds (00..59)
	"%T": time24Hour,            // Time, 24-hour (hh:mm:ss)
	"%Y": yearNumericFourDigits, // Year, numeric, four digits
	"%#": skipAllNums,           // Skip all numbers
	"%.": skipAllPunct,          // Skip all punctation characters
	"%@": skipAllAlpha,          // Skip all alpha characters
	"%y": yearNumericTwoDigits,  // Year, numeric (two digits)
	// "%a": abbreviatedWeekday,         // Abbreviated weekday name (Sun..Sat)
	// "%D": dayOfMonthWithSuffix,       // Day of the month with English suffix (0th, 1st, 2nd, 3rd)
	// "%U": weekMode0,                  // Week (00..53), where Sunday is the first day of the week; WEEK() mode 0
	// "%u": weekMode1,                  // Week (00..53), where Monday is the first day of the week; WEEK() mode 1
	// "%V": weekMode2,                  // Week (01..53), where Sunday is the first day of the week; WEEK() mode 2; used with %X
	// "%v": weekMode3,                  // Week (01..53), where Monday is the first day of the week; WEEK() mode 3; used with %x
	// "%W": weekdayName,                // Weekday name (Sunday..Saturday)
	// "%w": dayOfWeek,                  // Day of the week (0=Sunday..6=Saturday)
	// "%X": yearOfWeek,                 // Year for the week where Sunday is the first day of the week, numeric, four digits; used with %V
	// "%x": yearOfWeek,                 // Year for the week, where Monday is the first day of the week, numeric, four digits; used with %v
}

func matchDateWithToken(t *GeneralTime, date string, token string, ctx map[string]int) (remain string, succ bool) {
	if parse, ok := dateFormatParserTable[token]; ok {
		return parse(t, date, ctx)
	}

	if strings.HasPrefix(date, token) {
		return date[len(token):], true
	}
	return date, false
}

// Try to parse digits with number of `limit` starting from `input`
// Return <number, n chars to step forward> if success.
// Return <_, 0> if fail.
func parseNDigits(input string, limit int) (number int, step int) {
	if limit <= 0 {
		return 0, 0
	}

	var num uint64 = 0
	step = 0
	for ; step < len(input) && step < limit && '0' <= input[step] && input[step] <= '9'; step++ {
		num = num*10 + uint64(input[step]-'0')
	}
	return int(num), step
}

// Seconds (00..59)
func secondsNumeric(t *GeneralTime, input string, _ map[string]int) (string, bool) {
	v, step := parseNDigits(input, 2)
	if step <= 0 || v >= 60 {
		return input, false
	}
	t.setSecond(uint8(v))
	return input[step:], true
}

// Minutes, numeric (00..59)
func minutesNumeric(t *GeneralTime, input string, _ map[string]int) (string, bool) {
	v, step := parseNDigits(input, 2)
	if step <= 0 || v >= 60 {
		return input, false
	}
	t.setMinute(uint8(v))
	return input[step:], true
}

type parseState int32

const (
	parseStateNormal    parseState = 1
	parseStateFail      parseState = 2
	parseStateEndOfLine parseState = 3
)

func parseSep(input string) (string, parseState) {
	input = trimWhiteSpace(input)
	if len(input) == 0 {
		return input, parseStateEndOfLine
	}
	if input[0] != ':' {
		return input, parseStateFail
	}
	if input = trimWhiteSpace(input[1:]); len(input) == 0 {
		return input, parseStateEndOfLine
	}
	return input, parseStateNormal
}

// Time, 12-hour (hh:mm:ss followed by AM or PM)
func time12Hour(t *GeneralTime, input string, _ map[string]int) (string, bool) {
	tryParse := func(input string) (string, parseState) {
		// hh:mm:ss AM
		/// Note that we should update `t` as soon as possible, or we
		/// can not get correct result for incomplete input like "12:13"
		/// that is shorter than "hh:mm:ss"
		hour, step := parseNDigits(input, 2) // 1..12
		if step <= 0 || hour > 12 || hour == 0 {
			return input, parseStateFail
		}
		// Handle special case: 12:34:56 AM -> 00:34:56
		// For PM, we will add 12 it later
		if hour == 12 {
			hour = 0
		}
		t.setHour(uint8(hour))

		// ':'
		var state parseState
		if input, state = parseSep(input[step:]); state != parseStateNormal {
			return input, state
		}

		minute, step := parseNDigits(input, 2) // 0..59
		if step <= 0 || minute > 59 {
			return input, parseStateFail
		}
		t.setMinute(uint8(minute))

		// ':'
		if input, state = parseSep(input[step:]); state != parseStateNormal {
			return input, state
		}

		second, step := parseNDigits(input, 2) // 0..59
		if step <= 0 || second > 59 {
			return input, parseStateFail
		}
		t.setSecond(uint8(second))

		input = trimWhiteSpace(input[step:])
		if len(input) == 0 {
			// No "AM"/"PM" suffix, it is ok
			return input, parseStateEndOfLine
		} else if len(input) < 2 {
			// some broken char, fail
			return input, parseStateFail
		}

		switch {
		case hasCaseInsensitivePrefix(input, "AM"):
			t.setHour(uint8(hour))
		case hasCaseInsensitivePrefix(input, "PM"):
			t.setHour(uint8(hour + 12))
		default:
			return input, parseStateFail
		}

		return input[2:], parseStateNormal
	}

	remain, state := tryParse(input)
	if state == parseStateFail {
		return input, false
	}
	return remain, true
}

// Time, 24-hour (hh:mm:ss)
func time24Hour(t *GeneralTime, input string, _ map[string]int) (string, bool) {
	tryParse := func(input string) (string, parseState) {
		// hh:mm:ss
		/// Note that we should update `t` as soon as possible, or we
		/// can not get correct result for incomplete input like "12:13"
		/// that is shorter than "hh:mm:ss"
		hour, step := parseNDigits(input, 2) // 0..23
		if step <= 0 || hour > 23 {
			return input, parseStateFail
		}
		t.setHour(uint8(hour))

		// ':'
		var state parseState
		if input, state = parseSep(input[step:]); state != parseStateNormal {
			return input, state
		}

		minute, step := parseNDigits(input, 2) // 0..59
		if step <= 0 || minute > 59 {
			return input, parseStateFail
		}
		t.setMinute(uint8(minute))

		// ':'
		if input, state = parseSep(input[step:]); state != parseStateNormal {
			return input, state
		}

		second, step := parseNDigits(input, 2) // 0..59
		if step <= 0 || second > 59 {
			return input, parseStateFail
		}
		t.setSecond(uint8(second))
		return input[step:], parseStateNormal
	}

	remain, state := tryParse(input)
	if state == parseStateFail {
		return input, false
	}
	return remain, true
}

const (
	timeOfAM = 1 + iota
	timeOfPM
)

// judege AM or PM
func isAMOrPM(_ *GeneralTime, input string, ctx map[string]int) (string, bool) {
	if len(input) < 2 {
		return input, false
	}

	s := strings.ToLower(input[:2])
	switch s {
	case "am":
		ctx["%p"] = timeOfAM
	case "pm":
		ctx["%p"] = timeOfPM
	default:
		return input, false
	}
	return input[2:], true
}

// Day of the month, numeric (0..31)
func dayOfMonthNumeric(t *GeneralTime, input string, _ map[string]int) (string, bool) {
	v, step := parseNDigits(input, 2) // 0..31
	if step <= 0 || v > 31 {
		return input, false
	}
	t.setDay(uint8(v))
	return input[step:], true
}

// Hour (00..23)
func hour24Numeric(t *GeneralTime, input string, ctx map[string]int) (string, bool) {
	v, step := parseNDigits(input, 2) // 0..23
	if step <= 0 || v > 23 {
		return input, false
	}
	t.setHour(uint8(v))
	ctx["%H"] = v
	return input[step:], true
}

// Hour result (01..12)
func hour12Numeric(t *GeneralTime, input string, ctx map[string]int) (string, bool) {
	v, step := parseNDigits(input, 2) // 1..12
	if step <= 0 || v > 12 || v == 0 {
		return input, false
	}
	t.setHour(uint8(v))
	ctx["%h"] = v
	return input[step:], true
}

// Microseconds (000000..999999)
func microSeconds(t *GeneralTime, input string, _ map[string]int) (string, bool) {
	v, step := parseNDigits(input, 6)
	if step <= 0 {
		t.setMicrosecond(0)
		return input, true
	}
	for i := step; i < 6; i++ {
		v *= 10
	}
	t.setMicrosecond(uint32(v))
	return input[step:], true
}

// Year, numeric, four digits
func yearNumericFourDigits(t *GeneralTime, input string, ctx map[string]int) (string, bool) {
	return yearNumericNDigits(t, input, ctx, 4)
}

// Year, numeric (two digits)
func yearNumericTwoDigits(t *GeneralTime, input string, ctx map[string]int) (string, bool) {
	return yearNumericNDigits(t, input, ctx, 2)
}

func yearNumericNDigits(t *GeneralTime, input string, _ map[string]int, n int) (string, bool) {
	year, step := parseNDigits(input, n)
	if step <= 0 {
		return input, false
	} else if step <= 2 {
		year = adjustYear(year)
	}
	t.setYear(uint16(year))
	return input[step:], true
}

// adjustYear adjusts year according to y.
// link to: https://dev.mysql.com/doc/refman/8.0/en/two-digit-years.html
func adjustYear(y int) int {
	if y >= 0 && y <= 69 {
		y = 2000 + y
	} else if y >= 70 && y <= 99 {
		y = 1900 + y
	}
	return y
}

// Day of year (001..366)
func dayOfYearNumeric(_ *GeneralTime, input string, ctx map[string]int) (string, bool) {
	// MySQL declares that "%j" should be "Day of year (001..366)". But actually,
	// it accepts a number that is up to three digits, which range is [1, 999].
	v, step := parseNDigits(input, 3)
	if step <= 0 || v == 0 {
		return input, false
	}
	ctx["%j"] = v
	return input[step:], true
}

// Abbreviated month name (Jan..Dec)
func abbreviatedMonth(t *GeneralTime, input string, _ map[string]int) (string, bool) {
	if len(input) >= 3 {
		monthName := strings.ToLower(input[:3])
		if month, ok := monthAbbrev[monthName]; ok {
			t.setMonth(uint8(month))
			return input[len(monthName):], true
		}
	}
	return input, false
}

func hasCaseInsensitivePrefix(input, prefix string) bool {
	if len(input) < len(prefix) {
		return false
	}
	return strings.EqualFold(input[:len(prefix)], prefix)
}

// Month name (January..December)
func fullNameMonth(t *GeneralTime, input string, _ map[string]int) (string, bool) {
	for i, month := range MonthNames {
		if hasCaseInsensitivePrefix(input, month) {
			t.setMonth(uint8(i + 1))
			return input[len(month):], true
		}
	}
	return input, false
}

// Month, numeric (0..12)
func monthNumeric(t *GeneralTime, input string, _ map[string]int) (string, bool) {
	v, step := parseNDigits(input, 2) // 1..12
	if step <= 0 || v > 12 {
		return input, false
	}
	t.setMonth(uint8(v))
	return input[step:], true
}

// DateFSP gets fsp from date string.
func DateFSP(date string) (fsp int) {
	i := strings.LastIndex(date, ".")
	if i != -1 {
		fsp = len(date) - i - 1
	}
	return
}

// Skip all numbers
func skipAllNums(_ *GeneralTime, input string, _ map[string]int) (string, bool) {
	retIdx := 0
	for i, ch := range input {
		if unicode.IsNumber(ch) {
			retIdx = i + 1
		} else {
			break
		}
	}
	return input[retIdx:], true
}

// Skip all punctation characters
func skipAllPunct(_ *GeneralTime, input string, _ map[string]int) (string, bool) {
	retIdx := 0
	for i, ch := range input {
		if unicode.IsPunct(ch) {
			retIdx = i + 1
		} else {
			break
		}
	}
	return input[retIdx:], true
}

// Skip all alpha characters
func skipAllAlpha(_ *GeneralTime, input string, _ map[string]int) (string, bool) {
	retIdx := 0
	for i, ch := range input {
		if unicode.IsLetter(ch) {
			retIdx = i + 1
		} else {
			break
		}
	}
	return input[retIdx:], true
}
