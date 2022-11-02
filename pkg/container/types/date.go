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
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	daysPer400Years = 365*400 + 97
	daysPer100Years = 365*100 + 24
	daysPer4Years   = 365*4 + 1
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
	if Sunday <= d && d <= Saturday {
		return longDayNames[d]
	}
	return "%Weekday(" + strconv.FormatUint(uint64(d), 10) + ")"
}

var unixEpoch = int64(FromClock(1970, 1, 1, 0, 0, 0, 0))

var (
	leapYearMonthDays = []uint8{31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	flatYearMonthDays = []uint8{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
)

const (
	MaxDateYear    = 9999
	MinDateYear    = 1
	MaxMonthInYear = 12
	MinMonthInYear = 1
)

type TimeType int32

const (
	DateType      = 0
	DateTimeType  = 1
	TimeStampType = 2
)

// ParseDate will parse a string to be a Date
// Support Format:
// `yyyy-mm-dd`
// `yyyy-mm-d`
// `yyyy-m-dd`
// `yyyy-m-d`
// `yyyymmdd`
func ParseDate(s string) (Date, error) {
	var y int32
	var m, d uint8

	if len(s) < 8 {
		return -1, moerr.NewInvalidArg("parsedate", s)
	}

	y = int32(s[0]-'0')*1000 + int32(s[1]-'0')*100 + int32(s[2]-'0')*10 + int32(s[3]-'0')
	if s[4] == '-' {
		if len(s) < 8 || len(s) > 10 {
			return -1, moerr.NewInvalidArg("parsedate", s)
		}
		if len(s) == 8 {
			if s[6] != '-' {
				return -1, moerr.NewInvalidArg("parsedate", s)
			}
			m = s[5] - '0'
			d = s[7] - '0'
		} else if len(s) == 9 {
			if s[6] == '-' {
				m = s[5] - '0'
				d = (s[7]-'0')*10 + (s[8] - '0')
			} else if s[7] == '-' {
				m = (s[5]-'0')*10 + (s[6] - '0')
				d = s[8] - '0'
			} else {
				return -1, moerr.NewInvalidArg("parsedate", s)
			}
		} else {
			if s[7] != '-' {
				return -1, moerr.NewInvalidArg("parsedate", s)
			}
			m = (s[5]-'0')*10 + (s[6] - '0')
			d = (s[8]-'0')*10 + (s[9] - '0')
		}
	} else {
		if len(s) != 8 {
			return -1, moerr.NewInvalidArg("parsedate", s)
		}
		m = (s[4]-'0')*10 + (s[5] - '0')
		d = (s[6]-'0')*10 + (s[7] - '0')
	}

	if ValidDate(y, m, d) {
		return FromCalendar(y, m, d), nil
	}
	return -1, moerr.NewInvalidArg("parsedate", s)
}

// ParseDateCast will parse a string to be a Date (this is used for cast string to date,it's different from above)
// Support Format: we exchange '.' with '-' anyway.
// `yyyy-mm-dd`
// `yyyymmdd`
// `yyyy-mm.dd`
// `yyyy-mm.dd hh`
// `yyyy-mm.dd hh:mm:ss`
// `yyyy-mm.dd hh:mm:ss.(msc)`
func ParseDateCast(s string) (Date, error) {
	var y int32
	var m, d uint8
	//special case
	flag_spcial, _ := regexp.MatchString("^[0-9]{4}[.|-]{1}[0-9]{2}$", s)
	if flag_spcial {
		return -1, moerr.NewInvalidArg("parsedate", s)
	}
	//if it's pure number series like yyyymmdd,it must be 8 or 6 digits, otherwise there will be obfuscate
	flag1, _ := regexp.MatchString("^[0-9]{4}[0-9]{1,2}[0-9]{1,2}$", s)
	//the reg rule test: here refers to https://regex101.com/r/NlaiAo/1
	flag2, _ := regexp.MatchString("^[0-9]{4}[./-]{0,1}[0-9]{1,2}[./-]{0,1}[0-9]{1,2}([ ](([0-9]{1,2})|([0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}(\\.[0-9]*){0,1}))){0,1}$", s)
	if !flag2 {
		return -1, moerr.NewInvalidArg("parsedate", s)
	}
	y = int32(s[0]-'0')*1000 + int32(s[1]-'0')*100 + int32(s[2]-'0')*10 + int32(s[3]-'0')
	if flag1 {
		if len(s) == 8 {
			m = (s[4]-'0')*10 + (s[5] - '0')
			d = (s[6]-'0')*10 + (s[7] - '0')
		} else if len(s) == 6 {
			m = (s[4] - '0')
			d = (s[5] - '0')
		} else {
			return -1, moerr.NewInvalidArg("parsedate", s)
		}
	} else {
		// if len(s) < 8 {
		// 	return -1, errIncorrectDateValue
		// }
		strs := strings.Split(s, " ")
		strs = strings.FieldsFunc(strs[0], func(r rune) bool {
			return r == '.' || r == '-'
		})
		v, _ := strconv.ParseUint(strs[1], 10, 8)
		m = uint8(v)
		v, _ = strconv.ParseUint(strs[2], 10, 8)
		d = uint8(v)
		// if s[4] == '-' {
		// 	if len(s) != 10 && len(s) != 13 || (s[7] != '-' && s[7] != '.') {
		// 		return -1, errIncorrectDateValue
		// 	}
		// 	m = (s[5]-'0')*10 + (s[6] - '0')
		// 	d = (s[8]-'0')*10 + (s[9] - '0')
		// } else {
		// 	if len(s) != 8 {
		// 		return -1, errIncorrectDateValue
		// 	}
		// 	m = (s[4]-'0')*10 + (s[5] - '0')
		// 	d = (s[6]-'0')*10 + (s[7] - '0')
		// }
	}

	if ValidDate(y, m, d) {
		return FromCalendar(y, m, d), nil
	}
	return -1, moerr.NewInvalidArg("parsedate", s)
}

// date[0001-01-01 to 9999-12-31]
func ValidDate(year int32, month, day uint8) bool {
	if year >= MinDateYear && year <= MaxDateYear {
		if MinMonthInYear <= month && month <= MaxMonthInYear {
			if day > 0 {
				if isLeap(year) {
					return day <= leapYearMonthDays[month-1]
				} else {
					return day <= flatYearMonthDays[month-1]
				}
			}
		}
	}
	return false
}

func (d Date) String() string {
	y, m, day, _ := d.Calendar(true)
	return fmt.Sprintf("%04d-%02d-%02d", y, m, day)
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
	end := daysBefore[month+1]
	var begin uint16
	if uint16(d) >= end {
		month++
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

func FromCalendar(year int32, month, day uint8) Date {
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

// DayOfWeek return the day of the week of the date
func (d Date) DayOfWeek() Weekday {
	// January 1, year 1 in Gregorian calendar, was a Monday.
	return Weekday((d + 1) % 7)
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

type weekBehaviour uint

const (
	// WeekMondayFirst: set Monday as first day of week; otherwise Sunday is first day of week
	WeekMondayFirst weekBehaviour = 1

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

func (v weekBehaviour) bitAnd(flag weekBehaviour) bool {
	return (v & flag) != 0
}

func weekMode(mode int) weekBehaviour {
	weekFormat := weekBehaviour(mode & 7)
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
	_, week := calcWeek(d, weekMode(mode))
	return week
}

// YearWeek returns year and week.
func (d Date) YearWeek(mode int) (year int, week int) {
	behavior := weekMode(mode) | WeekYear
	return calcWeek(d, behavior)
}

// calcWeek calculates week and year for the date.
func calcWeek(d Date, wb weekBehaviour) (year int, week int) {
	var days int
	ty, tm, td := int(d.Year()), int(d.Month()), int(d.Day())
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
	return Datetime(int64(d) * secsPerDay * microSecsPerSec)
}

func (d Date) ToTime() Time {
	return Time(0)
}

func (d Date) ToTimestamp(loc *time.Location) Timestamp {
	year, mon, day, _ := d.Calendar(true)
	t := time.Date(int(year), time.Month(mon), int(day), 0, 0, 0, 0, loc)
	return Timestamp(t.UnixMicro() + unixEpoch)
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
