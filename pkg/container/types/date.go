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

package types

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	daysPer400Years   = 365*400 + 97
	daysPer100Years   = 365*100 + 24
	daysPer4Years     = 365*4 + 1
	DateToBytesLength = 10
)

type Weekday uint8

const (
	Sunday Weekday = iota
	Monday
	Tuesday
	Wednesday
	Thursday
	Friday
	Saturday
)

// String returns the English name of the day ("Sunday", "Monday", ...).
func (d Weekday) String() string {
	if d <= Saturday {
		return longDayNames[d]
	}
	return "%Weekday(" + strconv.FormatUint(uint64(d), 10) + ")"
}

var unixEpochMicroSecs = int64(DatetimeFromClock(1970, 1, 1, 0, 0, 0, 0))
var unixEpochDays = int32(DateFromCalendar(1970, 1, 1))

var (
	leapYearMonthDays = []uint8{31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	flatYearMonthDays = []uint8{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
)

const (
	MaxDateYear    = 9999
	MinDateYear    = 1
	MaxMonthInYear = 12
	MinMonthInYear = 1
	ZeroDate       = Date(-1)
)

type TimeType int32

const (
	DateType      = 0
	DateTimeType  = 1
	TimeStampType = 2
)

func isDigit(c byte) bool {
	return '0' <= c && c <= '9'
}

func isAllDigit(s string) bool {
	for i := range s {
		if !isDigit(s[i]) {
			return false
		}
	}
	return true
}

// ParseDateCastComponents parses the date portion of a string accepted by
// ParseDateCast without validating the calendar components.
//
// It preserves ParseDateCast's input grammar so callers that need to inspect
// incomplete dates can use the same normalization as normal date casts.
func ParseDateCastComponents(s string) (int32, uint8, uint8, error) {
	s = strings.TrimSpace(s)
	year, month, day, _, err := parseDateCastComponents(s)
	return year, month, day, err
}

// parseDateCastComponents parses an already-trimmed date string. Keeping the
// normalization outside this helper lets ParseDateCast avoid repeating it.
func parseDateCastComponents(s string) (int32, uint8, uint8, bool, error) {
	if isZeroDatetimeString(s) {
		return 0, 0, 0, true, nil
	}
	if len(s) < 7 && isAllDigit(s) {
		return 0, 0, 0, false, moerr.NewInvalidArgNoCtx("parsedate", s)
	}
	var year, month, day int64
	if len(s) == 7 && isAllDigit(s) {
		year = int64(s[0]-'0')*100 + int64(s[1]-'0')*10 + int64(s[2]-'0')
		month = int64(s[3]-'0')*10 + int64(s[4]-'0')
		day = int64(s[5]-'0')*10 + int64(s[6]-'0')
	} else if len(s) == 8 && isAllDigit(s) {
		year = int64(s[0]-'0')*1000 + int64(s[1]-'0')*100 + int64(s[2]-'0')*10 + int64(s[3]-'0')
		month = int64(s[4]-'0')*10 + int64(s[5]-'0')
		day = int64(s[6]-'0')*10 + int64(s[7]-'0')
	} else {
		const (
			start uint8 = iota
			yearState
			monthState
			dayState
			hourState
			minuteState
			secondState
			msState
			end
		)
		var state = start
		var yearLen, monthLen, dayLen, hourLen, minuteLen, secondLen int
		var hour, minute, second uint8
		var hasTime bool
		for i := 0; i < len(s); i++ {
			switch state {
			case start:
				if !isDigit(s[i]) {
					return 0, 0, 0, false, moerr.NewInvalidArgNoCtx("parsedate", s)
				}
				state = yearState
				year = int64(s[i] - '0')
				yearLen = 1
			case yearState:
				if isDigit(s[i]) {
					if yearLen >= 4 {
						return 0, 0, 0, false, moerr.NewInvalidArgNoCtx("parsedate", s)
					}
					year = year*10 + int64(s[i]-'0')
					yearLen++
				} else if isDateDelimiter(s[i]) {
					state = monthState
					if yearLen == 0 {
						return 0, 0, 0, false, moerr.NewInvalidArgNoCtx("parsedate", s)
					}
				} else {
					return 0, 0, 0, false, moerr.NewInvalidArgNoCtx("parsedate", s)
				}
			case monthState:
				if isDigit(s[i]) {
					month = month*10 + int64(s[i]-'0')
					monthLen++
					if monthLen >= 3 {
						return 0, 0, 0, false, moerr.NewInvalidArgNoCtx("parsedate", s)
					}
				} else if isDateDelimiter(s[i]) {
					if monthLen == 0 {
						return 0, 0, 0, false, moerr.NewInvalidArgNoCtx("parsedate", s)
					}
					state = dayState
				} else {
					return 0, 0, 0, false, moerr.NewInvalidArgNoCtx("parsedate", s)
				}
			case dayState:
				if isDigit(s[i]) {
					day = day*10 + int64(s[i]-'0')
					dayLen++
					if dayLen >= 3 {
						return 0, 0, 0, false, moerr.NewInvalidArgNoCtx("parsedate", s)
					}
				} else if s[i] == ' ' || s[i] == 'T' {
					if dayLen == 0 {
						return 0, 0, 0, false, moerr.NewInvalidArgNoCtx("parsedate", s)
					}
					if i == len(s)-1 {
						return 0, 0, 0, false, moerr.NewInvalidArgNoCtx("parsedate", s)
					}
					state = hourState
					hasTime = true
				} else {
					return 0, 0, 0, false, moerr.NewInvalidArgNoCtx("parsedate", s)
				}
				if i == len(s)-1 {
					state = end
				}
			case hourState:
				if s[i] == ' ' {
					continue
				}
				if isDigit(s[i]) {
					hourLen++
					if hourLen >= 3 {
						return 0, 0, 0, false, moerr.NewInvalidArgNoCtx("parsedate", s)
					}
					hour = hour*10 + uint8(s[i]-'0')
				} else if isTimeDelimiter(s[i]) {
					if hourLen == 0 {
						return 0, 0, 0, false, moerr.NewInvalidArgNoCtx("parsedate", s)
					}
					state = minuteState
				} else {
					return 0, 0, 0, false, moerr.NewInvalidArgNoCtx("parsedate", s)
				}
			case minuteState:
				if isDigit(s[i]) {
					minuteLen++
					if minuteLen >= 3 {
						return 0, 0, 0, false, moerr.NewInvalidArgNoCtx("parsedate", s)
					}
					minute = minute*10 + uint8(s[i]-'0')
					if i == len(s)-1 {
						s += ":00"
					}
				} else if isTimeDelimiter(s[i]) {
					if minuteLen == 0 {
						return 0, 0, 0, false, moerr.NewInvalidArgNoCtx("parsedate", s)
					}
					if i == len(s)-1 {
						s += "00"
					}
					state = secondState
				} else {
					return 0, 0, 0, false, moerr.NewInvalidArgNoCtx("parsedate", s)
				}
			case secondState:
				if isDigit(s[i]) {
					secondLen++
					if secondLen >= 3 {
						return 0, 0, 0, false, moerr.NewInvalidArgNoCtx("parsedate", s)
					}
					second = second*10 + uint8(s[i]-'0')
				} else if s[i] == '.' {
					if secondLen == 0 {
						return 0, 0, 0, false, moerr.NewInvalidArgNoCtx("parsedate", s)
					}
					state = msState
				} else {
					return 0, 0, 0, false, moerr.NewInvalidArgNoCtx("parsedate", s)
				}
				if i == len(s)-1 {
					state = end
				}
			case msState:
				if isAllDigit(s[i:]) {
					state = end
					i = len(s)
				} else {
					return 0, 0, 0, false, moerr.NewInvalidArgNoCtx("parsedate", s)
				}
			}
		}
		if state != end {
			return 0, 0, 0, false, moerr.NewInvalidArgNoCtx("parsedate", s)
		}
		year = normalizeDateCastYear(year, yearLen)
		if hasTime && !ValidTimeInDay(hour, minute, second) {
			return 0, 0, 0, false, moerr.NewInvalidArgNoCtx("parsedate", s)
		}
	}
	if year > MaxDateYear || month > MaxMonthInYear || day > 31 {
		return 0, 0, 0, false, moerr.NewInvalidArgNoCtx("parsedate", s)
	}
	return int32(year), uint8(month), uint8(day), false, nil
}

// isDateDelimiter matches MySQL's deprecated punctuation-delimited date forms.
func isDateDelimiter(c byte) bool {
	return c >= '!' && c <= '/' || c >= ':' && c <= '@' ||
		c >= '[' && c <= '`' || c >= '{' && c <= '~'
}

// isTimeDelimiter matches MySQL's relaxed punctuation-delimited time fields.
func isTimeDelimiter(c byte) bool {
	return isDateDelimiter(c)
}

func normalizeDateCastYear(year int64, length int) int64 {
	if length != 2 {
		return year
	}
	if year <= 69 {
		return year + 2000
	}
	return year + 1900
}

func parseFixedDateCast(s string) (Date, bool) {
	if len(s) < 19 || s[4] != '-' || s[7] != '-' || (s[10] != ' ' && s[10] != 'T') ||
		s[13] != ':' || s[16] != ':' || !isAllDigit(s[:4]) || !isAllDigit(s[5:7]) ||
		!isAllDigit(s[8:10]) || !isAllDigit(s[11:13]) || !isAllDigit(s[14:16]) || !isAllDigit(s[17:19]) {
		return 0, false
	}
	if len(s) > 19 && (s[19] != '.' || len(s) == 20 || !isAllDigit(s[20:])) {
		return 0, false
	}

	year := int32(s[0]-'0')*1000 + int32(s[1]-'0')*100 + int32(s[2]-'0')*10 + int32(s[3]-'0')
	month := uint8(s[5]-'0')*10 + uint8(s[6]-'0')
	day := uint8(s[8]-'0')*10 + uint8(s[9]-'0')
	hour := uint8(s[11]-'0')*10 + uint8(s[12]-'0')
	minute := uint8(s[14]-'0')*10 + uint8(s[15]-'0')
	second := uint8(s[17]-'0')*10 + uint8(s[18]-'0')
	if !ValidDate(year, month, day) || !ValidTimeInDay(hour, minute, second) {
		return 0, false
	}
	return DateFromCalendar(year, month, day), true
}

// rewrite ParseDateCast, don't use regexp, that's too slow
// the format we need to support:
// 1.yyyy-mm-dd hh:mm:ss.ms or yyyy-mm-dd hh:mm: or yyyy-mm-dd hh:mm
// 2.yyyy-mm-dd
// 3.yyyymmdd
func ParseDateCast(s string) (Date, error) {
	s = strings.TrimSpace(s)
	if len(s) > 0 && s[0] == '0' && isZeroDatetimeString(s) {
		return ZeroDate, nil
	}

	if len(s) == 7 && isAllDigit(s) {
		year := int32(s[0]-'0')*100 + int32(s[1]-'0')*10 + int32(s[2]-'0')
		month := uint8(s[3]-'0')*10 + uint8(s[4]-'0')
		day := uint8(s[5]-'0')*10 + uint8(s[6]-'0')
		if ValidDate(year, month, day) {
			return DateFromCalendar(year, month, day), nil
		}
	} else if len(s) == 8 && isAllDigit(s) {
		year := int32(s[0]-'0')*1000 + int32(s[1]-'0')*100 + int32(s[2]-'0')*10 + int32(s[3]-'0')
		month := uint8(s[4]-'0')*10 + uint8(s[5]-'0')
		day := uint8(s[6]-'0')*10 + uint8(s[7]-'0')
		if ValidDate(year, month, day) {
			return DateFromCalendar(year, month, day), nil
		}
	} else if len(s) == 10 && s[4] == '-' && s[7] == '-' &&
		isAllDigit(s[:4]) && isAllDigit(s[5:7]) && isAllDigit(s[8:]) {
		year := int32(s[0]-'0')*1000 + int32(s[1]-'0')*100 + int32(s[2]-'0')*10 + int32(s[3]-'0')
		month := uint8(s[5]-'0')*10 + uint8(s[6]-'0')
		day := uint8(s[8]-'0')*10 + uint8(s[9]-'0')
		if ValidDate(year, month, day) {
			return DateFromCalendar(year, month, day), nil
		}
	}
	if date, ok := parseFixedDateCast(s); ok {
		return date, nil
	}

	year, month, day, isZero, err := parseDateCastComponents(s)
	if err != nil {
		return -1, err
	}
	if isZero {
		return ZeroDate, nil
	}
	if ValidDate(year, month, day) {
		return DateFromCalendar(year, month, day), nil
	}
	return -1, moerr.NewInvalidArgNoCtx("parsedate", s)
}

// date[0001-01-01 to 9999-12-31]
func ValidDate(year int32, month, day uint8) bool {
	return year >= MinDateYear && ValidCalendarDate(year, month, day)
}

// ValidCalendarDate validates a calendar value accepted by MySQL VARCHAR
// temporal functions. It intentionally includes year 0, which is not a
// storable DATE value in MatrixOne. MySQL treats year 0 as a non-leap year.
func ValidCalendarDate(year int32, month, day uint8) bool {
	if year < 0 || year > MaxDateYear || month < MinMonthInYear || month > MaxMonthInYear || day == 0 {
		return false
	}
	if year != 0 && isLeap(year) {
		return day <= leapYearMonthDays[month-1]
	}
	return day <= flatYearMonthDays[month-1]
}

func (d Date) String() string {
	y, m, day, _ := d.Calendar(true)
	return fmt.Sprintf("%04d-%02d-%02d", y, m, day)
}

// ToBytes converts Date to bytes like Data.String().
func (d Date) ToBytes(dst []byte) []byte {
	y, m, day, _ := d.Calendar(true)
	//1. year to upper, lower
	yUpper, yLower := y/100, y%100
	//2. yUpper & yLower to chars. four bytes
	dst = append(dst, hundredToChars[yUpper][:2]...)
	dst = append(dst, hundredToChars[yLower][:2]...)
	dst = append(dst, '-')
	dst = append(dst, hundredToChars[m][:2]...)
	dst = append(dst, '-')
	dst = append(dst, hundredToChars[day][:2]...)
	return dst
}

// 1~99 to two chars
var hundredToChars = [100][2]byte{
	{'0', '0'},
	{'0', '1'},
	{'0', '2'},
	{'0', '3'},
	{'0', '4'},
	{'0', '5'},
	{'0', '6'},
	{'0', '7'},
	{'0', '8'},
	{'0', '9'},
	{'1', '0'},
	{'1', '1'},
	{'1', '2'},
	{'1', '3'},
	{'1', '4'},
	{'1', '5'},
	{'1', '6'},
	{'1', '7'},
	{'1', '8'},
	{'1', '9'},
	{'2', '0'},
	{'2', '1'},
	{'2', '2'},
	{'2', '3'},
	{'2', '4'},
	{'2', '5'},
	{'2', '6'},
	{'2', '7'},
	{'2', '8'},
	{'2', '9'},
	{'3', '0'},
	{'3', '1'},
	{'3', '2'},
	{'3', '3'},
	{'3', '4'},
	{'3', '5'},
	{'3', '6'},
	{'3', '7'},
	{'3', '8'},
	{'3', '9'},
	{'4', '0'},
	{'4', '1'},
	{'4', '2'},
	{'4', '3'},
	{'4', '4'},
	{'4', '5'},
	{'4', '6'},
	{'4', '7'},
	{'4', '8'},
	{'4', '9'},
	{'5', '0'},
	{'5', '1'},
	{'5', '2'},
	{'5', '3'},
	{'5', '4'},
	{'5', '5'},
	{'5', '6'},
	{'5', '7'},
	{'5', '8'},
	{'5', '9'},
	{'6', '0'},
	{'6', '1'},
	{'6', '2'},
	{'6', '3'},
	{'6', '4'},
	{'6', '5'},
	{'6', '6'},
	{'6', '7'},
	{'6', '8'},
	{'6', '9'},
	{'7', '0'},
	{'7', '1'},
	{'7', '2'},
	{'7', '3'},
	{'7', '4'},
	{'7', '5'},
	{'7', '6'},
	{'7', '7'},
	{'7', '8'},
	{'7', '9'},
	{'8', '0'},
	{'8', '1'},
	{'8', '2'},
	{'8', '3'},
	{'8', '4'},
	{'8', '5'},
	{'8', '6'},
	{'8', '7'},
	{'8', '8'},
	{'8', '9'},
	{'9', '0'},
	{'9', '1'},
	{'9', '2'},
	{'9', '3'},
	{'9', '4'},
	{'9', '5'},
	{'9', '6'},
	{'9', '7'},
	{'9', '8'},
	{'9', '9'},
}

// Today Holds number of days since January 1, year 1 in Gregorian calendar
func Today(loc *time.Location) Date {
	return Now(loc).ToDate()
}

const dayInfoTableMinYear = 1924
const dayInfoTableMaxYear = 2099
const dayInfoTableYears = dayInfoTableMaxYear - dayInfoTableMinYear + 1
const dayInfoTableSize = dayInfoTableYears*365 + (dayInfoTableMaxYear-dayInfoTableMinYear)/4 + 1
const dayNumOfTableEpoch = 702360 // the day number of "1924-01-01"

type dayInfo struct {
	year uint16
	//month uint8
	//week  uint8
}

var dayInfoTable [dayInfoTableSize]dayInfo

// this init function takes a bit of build time
func init() {
	yearNow := uint16(1924)
	i := int32(0)
	for yearIndex := 0; yearIndex < dayInfoTableYears; yearIndex++ {
		if yearIndex%4 == 0 { // this is a leap year
			for j := 0; j < 366; j++ {
				dayInfoTable[i].year = yearNow
				i++
			}
		} else {
			for j := 0; j < 365; j++ {
				dayInfoTable[i].year = yearNow
				i++
			}
		}
		yearNow++
	}
}

// Year takes a date and returns an uint16 number as the year of this date
func (d Date) Year() uint16 {
	if d == ZeroDate {
		return 0
	}
	dayNum := int32(d)
	insideDayInfoTable := dayNum >= dayNumOfTableEpoch && dayNum < dayNumOfTableEpoch+dayInfoTableSize
	if insideDayInfoTable {
		return dayInfoTable[dayNum-dayNumOfTableEpoch].year
	}
	// Account for 400 year cycles.
	n := d / daysPer400Years
	y := 400 * n
	d -= daysPer400Years * n

	// Cut off 100-year cycles.
	// The last cycle has one extra leap year, so on the last day
	// of that year, day / daysPer100Years will be 4 instead of 3.
	// Cut it back down to 3 by subtracting n>>2.
	n = d / daysPer100Years
	n -= n >> 2
	y += 100 * n
	d -= daysPer100Years * n

	// Cut off 4-year cycles.
	// The last cycle has a missing leap year, which does not
	// affect the computation.
	n = d / daysPer4Years
	y += 4 * n
	d -= daysPer4Years * n

	// Cut off years within a 4-year cycle.
	// The last year is a leap year, so on the last day of that year,
	// day / 365 will be 4 instead of 3. Cut it back down to 3
	// by subtracting n>>2.
	n = d / 365
	n -= n >> 2
	y += n

	year := uint16(y) + 1

	return year
}

func (d Date) YearMonth() uint32 {
	year, month, _, _ := d.Calendar(true)
	yearStr := fmt.Sprintf("%04d", year)
	monthStr := fmt.Sprintf("%02d", month)
	result, _ := strconv.ParseUint(yearStr+monthStr, 10, 32)
	return uint32(result)
}

func (d Date) YearMonthStr() string {
	year, month, _, _ := d.Calendar(true)
	yearStr := fmt.Sprintf("%04d", year)
	monthStr := fmt.Sprintf("%02d", month)
	return yearStr + monthStr
}

var monthToQuarter = map[uint8]uint32{
	1:  1,
	2:  1,
	3:  1,
	4:  2,
	5:  2,
	6:  2,
	7:  3,
	8:  3,
	9:  3,
	10: 4,
	11: 4,
	12: 4,
}

func (d Date) Quarter() uint32 {
	_, month, _, _ := d.Calendar(true)
	return monthToQuarter[month]
}

func (d Date) Calendar(full bool) (year int32, month, day uint8, yday uint16) {
	if d == ZeroDate {
		return 0, 0, 0, 0
	}
	// Account for 400 year cycles.
	n := d / daysPer400Years
	y := 400 * n
	d -= daysPer400Years * n

	// Cut off 100-year cycles.
	// The last cycle has one extra leap year, so on the last day
	// of that year, day / daysPer100Years will be 4 instead of 3.
	// Cut it back down to 3 by subtracting n>>2.
	n = d / daysPer100Years
	n -= n >> 2
	y += 100 * n
	d -= daysPer100Years * n

	// Cut off 4-year cycles.
	// The last cycle has a missing leap year, which does not
	// affect the computation.
	n = d / daysPer4Years
	y += 4 * n
	d -= daysPer4Years * n

	// Cut off years within a 4-year cycle.
	// The last year is a leap year, so on the last day of that year,
	// day / 365 will be 4 instead of 3. Cut it back down to 3
	// by subtracting n>>2.
	n = d / 365
	n -= n >> 2
	y += n
	d -= 365 * n

	year = int32(y) + 1
	yday = uint16(d + 1)

	if !full {
		return
	}

	if isLeap(year) {
		// Leap year
		switch {
		case d > 31+29-1:
			// After leap day; pretend it wasn't there.
			d--
		case d == 31+29-1:
			// Leap day.
			month = 2
			day = 29
			return
		}
	}

	// Estimate month on assumption that every month has 31 days.
	// The estimate may be too low by at most one month, so adjust.
	month = uint8(d / 31)
	// Check bounds: daysBefore array has 13 elements (0-12), so month+1 must be <= 12
	// If month is too large, the date is invalid (out of valid datetime range)
	if month+1 >= uint8(len(daysBefore)) {
		// Return invalid date (year=0) to indicate invalid date
		// ValidDatetime will catch this and return false
		return 0, 0, 0, 0
	}
	end := daysBefore[month+1]
	var begin uint16
	if uint16(d) >= end {
		month++
		// Check bounds again after increment
		if month+1 >= uint8(len(daysBefore)) {
			return 0, 0, 0, 0
		}
		begin = end
	} else {
		begin = daysBefore[month]
	}

	month++ // because January is 1
	day = uint8(uint16(d) - begin + 1)
	return year, month, day, yday
}

// daysBefore[m] counts the number of days in a non-leap year
// before month m begins. There is an entry for m=12, counting
// the number of days before January of next year (365).

var daysBefore = [...]uint16{
	0,
	31,
	31 + 28,
	31 + 28 + 31,
	31 + 28 + 31 + 30,
	31 + 28 + 31 + 30 + 31,
	31 + 28 + 31 + 30 + 31 + 30,
	31 + 28 + 31 + 30 + 31 + 30 + 31,
	31 + 28 + 31 + 30 + 31 + 30 + 31 + 31,
	31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30,
	31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31,
	31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30,
	31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30 + 31,
}

func DateFromCalendar(year int32, month, day uint8) Date {
	// Compute days since the absolute epoch.
	d := daysSinceEpoch(year - 1)

	// Add in days before this month.
	d += int32(daysBefore[month-1])
	if isLeap(year) && month >= 3 {
		d++ // February 29
	}

	// Add in days before today.
	d += int32(day - 1)

	return Date(d)
}

func daysSinceEpoch(year int32) int32 {
	// Add in days from 400-year cycles.
	n := year / 400
	year -= 400 * n
	d := daysPer400Years * n

	// Add in 100-year cycles.
	n = year / 100
	year -= 100 * n
	d += daysPer100Years * n

	// Add in 4-year cycles.
	n = year / 4
	year -= 4 * n
	d += daysPer4Years * n

	// Add in non-leap years.
	n = year
	d += 365 * n

	return d
}

// DayOfWeek return the day of the week counting from Sunday
func (d Date) DayOfWeek() Weekday {
	// January 1, year 1 in Gregorian calendar, was a Monday.
	return Weekday((d + 1) % 7)
}

// DayOfWeek2 return the day of the week counting from Monday
func (d Date) DayOfWeek2() Weekday {
	// January 1, year 1 in Gregorian calendar, was a Monday.
	return Weekday(d % 7)
}

// DayOfYear return day of year (001..366)
func (d Date) DayOfYear() uint16 {
	_, _, _, yday := d.Calendar(false)
	return yday
}

func (d Date) WeekOfYear() (year int32, week uint8) {
	// According to the rule that the first calendar week of a calendar year is
	// the week including the first Thursday of that year, and that the last one is
	// the week immediately preceding the first calendar week of the next calendar year.
	// See https://www.iso.org/obp/ui#iso:std:iso:8601:-1:ed-1:v1:en:term:3.1.1.23 for details.

	// weeks start with Monday
	// Monday Tuesday Wednesday Thursday Friday Saturday Sunday
	// 1      2       3         4        5      6        7
	// +3     +2      +1        0        -1     -2       -3
	// the offset to Thursday
	delta := 4 - int32(d.DayOfWeek())
	// handle Sunday
	if delta == 4 {
		delta = -3
	}
	// find the Thursday of the calendar week
	d = Date(int32(d) + delta)
	year, _, _, yday := d.Calendar(false)
	return year, uint8((yday-1)/7 + 1)
}

func (d Date) WeekOfYear2() uint8 {
	// According to the rule that the first calendar week of a calendar year is
	// the week including the first Thursday of that year, and that the last one is
	// the week immediately preceding the first calendar week of the next calendar year.
	// See https://www.iso.org/obp/ui#iso:std:iso:8601:-1:ed-1:v1:en:term:3.1.1.23 for details.

	// weeks start with Monday
	// Monday Tuesday Wednesday Thursday Friday Saturday Sunday
	// 1      2       3         4        5      6        7
	// +3     +2      +1        0        -1     -2       -3
	// the offset to Thursday
	delta := 4 - int32(d.DayOfWeek())
	// handle Sunday
	if delta == 4 {
		delta = -3
	}
	// find the Thursday of the calendar week
	d = Date(int32(d) + delta)
	_, _, _, yday := d.Calendar(false)
	return uint8((yday-1)/7 + 1)
}

type WeekBehaviour uint

const (
	// WeekMondayFirst: set Monday as first day of week; otherwise Sunday is first day of week
	WeekMondayFirst WeekBehaviour = 1

	// WeekYear: If set, Week is in range 1-53, otherwise Week is in range 0-53.
	//	Week 0 is returned for the the last week of the previous year (for
	// a date at start of january) In this case one can get 53 for the
	// first week of next year.  This flag ensures that the week is
	// relevant for the given year. Note that this flag is only
	// releveant if WEEK_JANUARY is not set.
	WeekYear = 2

	//WeekFirstWeekday: If not set, Weeks are numbered according to ISO 8601:1988.
	// If set, the week that contains the first 'first-day-of-week' is week 1.
	// ISO 8601:1988 means that if the week containing January 1 has
	// four or more days in the new year, then it is week 1;
	// Otherwise it is the last week of the previous year, and the next week is week 1.
	WeekFirstWeekday = 4
)

func (v WeekBehaviour) bitAnd(flag WeekBehaviour) bool {
	return (v & flag) != 0
}

func weekMode(mode int) WeekBehaviour {
	weekFormat := WeekBehaviour(mode & 7)
	if (weekFormat & WeekMondayFirst) == 0 {
		weekFormat ^= WeekFirstWeekday
	}
	return weekFormat
}

// Week (00..53), where Sunday is the first day of the week; WEEK() mode 0
// Week (00..53), where Monday is the first day of the week; WEEK() mode 1
func (d Date) Week(mode int) int {
	if d.Month() == 0 || d.Day() == 0 {
		return 0
	}
	_, week := calcWeekFromCalendar(int(d.Year()), int(d.Month()), int(d.Day()), weekMode(mode))
	return week
}

// DayOfWeekFromCalendar returns the weekday for a validated calendar date,
// including MySQL's year-0 date values.
func DayOfWeekFromCalendar(year int32, month, day uint8) Weekday {
	weekday := calcWeekday(calcDaynr(int(year), int(month), int(day)), false)
	return Weekday((weekday + 1) % 7)
}

// WeekFromCalendar returns WEEK(year-month-day, mode) for a validated calendar
// date, including MySQL's year-0 date values.
func WeekFromCalendar(year int32, month, day uint8, mode int) int {
	_, week := calcWeekFromCalendar(int(year), int(month), int(day), weekMode(mode))
	return week
}

// YearWeek returns year and week.
func (d Date) YearWeek(mode int) (year int, week int) {
	behavior := weekMode(mode) | WeekYear
	return calcWeek(d, behavior)
}

// calcWeek calculates week and year for the date.
func calcWeek(d Date, wb WeekBehaviour) (year int, week int) {
	return calcWeekFromCalendar(int(d.Year()), int(d.Month()), int(d.Day()), wb)
}

func calcWeekFromCalendar(ty, tm, td int, wb WeekBehaviour) (year int, week int) {
	var days int
	daynr := calcDaynr(ty, tm, td)
	firstDaynr := calcDaynr(ty, 1, 1)
	mondayFirst := wb.bitAnd(WeekMondayFirst)
	weekYear := wb.bitAnd(WeekYear)
	firstWeekday := wb.bitAnd(WeekFirstWeekday)

	weekday := calcWeekday(firstDaynr, !mondayFirst)

	year = ty

	if tm == 1 && td <= 7-weekday {
		if !weekYear &&
			((firstWeekday && weekday != 0) || (!firstWeekday && weekday >= 4)) {
			week = 0
			return
		}
		weekYear = true
		year--
		days = calcDaysInYear(year)
		firstDaynr -= days
		weekday = (weekday + 53*7 - days) % 7
	}

	if (firstWeekday && weekday != 0) ||
		(!firstWeekday && weekday >= 4) {
		days = daynr - (firstDaynr + 7 - weekday)
	} else {
		days = daynr - (firstDaynr - weekday)
	}

	if weekYear && days >= 52*7 {
		weekday = (weekday + calcDaysInYear(year)) % 7
		if (!firstWeekday && weekday < 4) ||
			(firstWeekday && weekday == 0) {
			year++
			week = 1
			return
		}
	}
	week = days/7 + 1
	return
}

// calcWeekday calculates weekday from daynr, returns 0 for Monday, 1 for Tuesday
func calcWeekday(daynr int, sundayFirstDayOfWeek bool) int {
	daynr += 5
	if sundayFirstDayOfWeek {
		daynr++
	}
	return daynr % 7
}

// Calculate nr of day since year 0 in new date-system (from 1615).
func calcDaynr(year, month, day int) int {
	if year == 0 && month == 0 {
		return 0
	}

	delsum := 365*year + 31*(month-1) + day
	if month <= 2 {
		year--
	} else {
		delsum -= (month*4 + 23) / 10
	}
	temp := ((year/100 + 1) * 3) / 4
	return delsum + year/4 - temp
}

// calcDaysInYear calculates days in one year, it works with 0 <= year <= 99.
func calcDaysInYear(year int) int {
	if (year&3) == 0 && (year%100 != 0 || (year%400 == 0 && (year != 0))) {
		return 366
	}
	return 365
}

func isLeap(year int32) bool {
	return year%4 == 0 && (year%100 != 0 || year%400 == 0)
}

func (d Date) ToDatetime() Datetime {
	if d == ZeroDate {
		return ZeroDatetime
	}
	return Datetime(int64(d) * SecsPerDay * MicroSecsPerSec)
}

func (d Date) ToTime() Time {
	return Time(0)
}

func (d Date) ToTimestamp(loc *time.Location) Timestamp {
	if d == ZeroDate {
		return ZeroTimestamp
	}
	year, mon, day, _ := d.Calendar(true)
	t := time.Date(int(year), time.Month(mon), int(day), 0, 0, 0, 0, loc)
	return Timestamp(t.UnixMicro() + unixEpochMicroSecs)
}

func (d Date) Month() uint8 {
	_, month, _, _ := d.Calendar(true)
	return month
}

func LastDay(year int32, month uint8) uint8 {
	if isLeap(year) {
		return leapYearMonthDays[month-1]
	}
	return flatYearMonthDays[month-1]
}

func (d Date) Day() uint8 {
	_, _, day, _ := d.Calendar(true)
	return day
}

func (d Date) DaysSinceUnixEpoch() int32 {
	return int32(d) - unixEpochDays
}

func DaysFromUnixEpochToDate(days int32) Date {
	return Date(days + unixEpochDays)
}

func GetUnixEpochSecs() int64 {
	return unixEpochMicroSecs
}

func MakeDate(year int32, month uint8, day int32) Date {
	// Compute days since the absolute epoch.
	d := daysSinceEpoch(year - 1)

	// Add in days before this month.
	d += int32(daysBefore[month-1])
	if isLeap(year) && month >= 3 {
		d++ // February 29
	}

	// Add in days before today.
	d += day - 1

	return Date(d)
}
