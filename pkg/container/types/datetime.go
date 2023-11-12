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
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	SecsPerMinute       = 60
	SecsPerHour         = 60 * SecsPerMinute
	SecsPerDay          = 24 * SecsPerHour
	SecsPerWeek         = 7 * SecsPerDay
	NanoSecsPerSec      = 1000000000 // 10^9
	MicroSecsPerSec     = 1000000    // 10^6
	MillisecsPerSec     = 1000       // 10^3
	nanoSecsPerMicroSec = 1000
	microSecsPerDay     = SecsPerDay * MicroSecsPerSec
	MaxDatetimeYear     = 9999
	MinDatetimeYear     = 1

	minHourInDay, maxHourInDay           = 0, 23
	minMinuteInHour, maxMinuteInHour     = 0, 59
	minSecondInMinute, maxSecondInMinute = 0, 59
)

var (
	scaleVal = []Datetime{1000000, 100000, 10000, 1000, 100, 10, 1}
)

const (
	// ZeroDatetime is the zero value for date Time '0000-01-01 00:00:00'.
	ZeroDatetime = Datetime(0)
)

// The Datetime type holds number of microseconds since January 1, year 1 in Gregorian calendar

func (dt Datetime) String() string {
	y, m, d, _ := dt.ToDate().Calendar(true)
	hour, minute, sec := dt.Clock()
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", y, m, d, hour, minute, sec)
}

func (dt Datetime) String2(scale int32) string {
	y, m, d, _ := dt.ToDate().Calendar(true)
	hour, minute, sec := dt.Clock()
	if scale > 0 {
		msec := int64(dt) % MicroSecsPerSec
		msecInstr := fmt.Sprintf("%06d\n", msec)
		msecInstr = msecInstr[:scale]

		return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d"+"."+msecInstr, y, m, d, hour, minute, sec)
	}
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", y, m, d, hour, minute, sec)
}

// ParseDatetime will parse a string to be a Datetime
// Support Format:
// 1. all the Date value
// 2. yyyy-mm-dd hh:mm:ss(.msec)
// now support mm/dd/hh/mm/ss can be single number
// 3. yyyymmddhhmmss(.msec)
// during parsing, the Datetime value will be rounded(away from zero) to the predefined scale, for example:
// Datetime(3) input string   					parsing result
//
//	"1999-09-09 11:11:11.1234"		"1999-09-09 11:11:11.123"
//	"1999-09-09 11:11:11.1235"		"1999-09-09 11:11:11.124"
//	"1999-09-09 11:11:11.9994"      "1999-09-09 11:11:11.999"
//	"1999-09-09 11:11:11.9995"      "1999-09-09 11:11:12.000"
//	"1999-09-09 11:11"              "1999-09-09 11:11:00.000"
//	"1999-09-09 11:11:"             "1999-09-09 11:11:00.000"
func ParseDatetime(s string, scale int32) (Datetime, error) {
	s = strings.TrimSpace(s)
	if len(s) < 14 {
		if d, err := ParseDateCast(s); err == nil {
			return d.ToDatetime(), nil
		}
		return -1, moerr.NewInvalidInputNoCtx("invalid datetime value %s", s)
	}
	var year int32
	var month, day, hour, minute, second uint8
	var msec uint32 = 0
	var carry uint32 = 0
	var err error

	if s[4] == '-' || s[4] == '/' {
		var num int64
		var unum uint64
		strArr := strings.Split(s, " ")
		if len(strArr) != 2 {
			return -1, moerr.NewInvalidInputNoCtx("invalid datetime value %s", s)
		}
		// solve year/month/day
		front := strings.Split(strArr[0], s[4:5])
		if len(front) != 3 {
			return -1, moerr.NewInvalidInputNoCtx("invalid datetime value %s", s)
		}
		num, err = strconv.ParseInt(front[0], 10, 32)
		if err != nil {
			return -1, moerr.NewInvalidInputNoCtx("invalid datetime value %s", s)
		}
		year = int32(num)
		unum, err = strconv.ParseUint(front[1], 10, 8)
		if err != nil {
			return -1, moerr.NewInvalidInputNoCtx("invalid datetime value %s", s)
		}
		month = uint8(unum)
		unum, err = strconv.ParseUint(front[2], 10, 8)
		if err != nil {
			return -1, moerr.NewInvalidInputNoCtx("invalid datetime value %s", s)
		}
		day = uint8(unum)

		if !ValidDate(year, month, day) {
			return -1, moerr.NewInvalidInputNoCtx("invalid datetime value %s", s)
		}

		middleAndBack := strings.Split(strArr[1], ".")
		// solve hour/minute/second
		middle := strings.Split(middleAndBack[0], ":")
		if len(middle) != 3 {
			if len(middle) == 2 {
				middle = append(middle, "00")
			} else {
				return -1, moerr.NewInvalidInputNoCtx("invalid datetime value %s", s)
			}
		}
		if len(middle) == 3 && middle[2] == "" {
			middle[2] = "00"
		}
		unum, err = strconv.ParseUint(middle[0], 10, 8)
		if err != nil {
			return -1, moerr.NewInvalidInputNoCtx("invalid datetime value %s", s)
		}
		hour = uint8(unum)
		unum, err = strconv.ParseUint(middle[1], 10, 8)
		if err != nil {
			return -1, moerr.NewInvalidInputNoCtx("invalid datetime value %s", s)
		}
		minute = uint8(unum)
		unum, err = strconv.ParseUint(middle[2], 10, 8)
		if err != nil {
			return -1, moerr.NewInvalidInputNoCtx("invalid datetime value %s", s)
		}
		second = uint8(unum)
		if !ValidTimeInDay(hour, minute, second) {
			return -1, moerr.NewInvalidInputNoCtx("invalid datetime value %s", s)
		}
		// solve microsecond
		if len(middleAndBack) == 2 {
			msec, carry, err = getMsec(middleAndBack[1], scale)
			if err != nil {
				return -1, moerr.NewInvalidInputNoCtx("invalid datetime value %s", s)
			}
		} else if len(middleAndBack) > 2 {
			return -1, moerr.NewInvalidInputNoCtx("invalid datetime value %s", s)
		}
	} else {
		year = int32(s[0]-'0')*1000 + int32(s[1]-'0')*100 + int32(s[2]-'0')*10 + int32(s[3]-'0')
		month = (s[4]-'0')*10 + (s[5] - '0')
		day = (s[6]-'0')*10 + (s[7] - '0')
		hour = (s[8]-'0')*10 + (s[9] - '0')
		minute = (s[10]-'0')*10 + (s[11] - '0')
		second = (s[12]-'0')*10 + (s[13] - '0')
		if len(s) > 14 {
			if len(s) > 15 && s[14] == '.' {
				msecStr := s[15:]
				msec, carry, err = getMsec(msecStr, scale)
				if err != nil {
					return -1, moerr.NewInvalidInputNoCtx("invalid datetime value %s", s)
				}
			} else {
				return -1, moerr.NewInvalidInputNoCtx("invalid datetime value %s", s)
			}
		}
	}
	if !ValidDate(year, month, day) {
		return -1, moerr.NewInvalidInputNoCtx("invalid datetime value %s", s)
	}
	result := DatetimeFromClock(year, month, day, hour, minute, second+uint8(carry), msec)
	y, m, d, _ := result.ToDate().Calendar(true)
	if !ValidDate(y, m, d) {
		return -1, moerr.NewInvalidInputNoCtx("invalid datetime value %s", s)
	}
	return result, nil
}

// validTimeInDay return true if hour, minute and second can be a time during a day
func ValidTimeInDay(h, m, s uint8) bool {
	if h < minHourInDay || h > maxHourInDay {
		return false
	}
	if m < minMinuteInHour || m > maxMinuteInHour {
		return false
	}
	if s < minSecondInMinute || s > maxSecondInMinute {
		return false
	}
	return true
}

func (dt Datetime) UnixTimestamp(loc *time.Location) int64 {
	return dt.ConvertToGoTime(loc).Unix()
}

func DatetimeFromUnix(loc *time.Location, ts int64) Datetime {
	t := time.Unix(ts, 0).In(loc)
	_, offset := t.Zone()
	return Datetime((ts+int64(offset))*MicroSecsPerSec + unixEpochMicroSecs)
}

func DatetimeFromUnixWithNsec(loc *time.Location, sec int64, nsec int64) Datetime {
	t := time.Unix(sec, nsec).In(loc)
	_, offset := t.Zone()
	msec := math.Round(float64(nsec) / 1000)
	return Datetime((sec+int64(offset))*MicroSecsPerSec + int64(msec) + unixEpochMicroSecs)
}

func Now(loc *time.Location) Datetime {
	now := time.Now().In(loc)
	_, offset := now.Zone()
	return Datetime(now.UnixMicro() + int64(offset)*MicroSecsPerSec + unixEpochMicroSecs)
}

func UTC() Datetime {
	return Datetime(time.Now().UnixMicro() + unixEpochMicroSecs)
}

func (dt Datetime) ToDate() Date {
	return Date((dt.sec()) / SecsPerDay)
}

// We need to truncate the part after scale position when cast
// between different scale.
func (dt Datetime) ToTime(scale int32) Time {
	if scale == 6 {
		return Time(dt % microSecsPerDay)
	}

	// truncate the date part
	ms := dt % microSecsPerDay

	base := ms / scaleVal[scale]
	if ms%scaleVal[scale]/scaleVal[scale+1] >= 5 { // check carry
		base += 1
	}

	return Time(base * scaleVal[scale])
}

func (dt Datetime) Clock() (hour, minute, sec int8) {
	t := (dt.sec()) % SecsPerDay
	hour = int8(t / SecsPerHour)
	minute = int8(t % SecsPerHour / SecsPerMinute)
	sec = int8(t % SecsPerMinute)
	return
}

func (dt Datetime) Sec() int8 {
	_, _, sec := dt.Clock()
	return sec
}

func (dt Datetime) Minute() int8 {
	_, minute, _ := dt.Clock()
	return minute
}

func (dt Datetime) Hour() int8 {
	hour, _, _ := dt.Clock()
	return hour
}

func DatetimeFromClock(year int32, month, day, hour, minute, sec uint8, msec uint32) Datetime {
	days := DateFromCalendar(year, month, day)
	secs := int64(days)*SecsPerDay + int64(hour)*SecsPerHour + int64(minute)*SecsPerMinute + int64(sec)
	return Datetime(secs*MicroSecsPerSec + int64(msec))
}

func (dt Datetime) ConvertToGoTime(loc *time.Location) time.Time {
	year, mon, day, _ := dt.ToDate().Calendar(true)
	hour, minute, sec := dt.Clock()
	nsec := dt.MicroSec() * 1000
	return time.Date(int(year), time.Month(mon), int(day), int(hour), int(minute), int(sec), int(nsec), loc)
}

func (dt Datetime) AddDateTime(addMonth, addYear int64, timeType TimeType) (Datetime, bool) {
	// corner case: mysql: date_add('2022-01-31',interval 1 month) -> 2022-02-28
	// only in the month year year-month
	oldDate := dt.ToDate()
	y, m, d, _ := oldDate.Calendar(true)
	year := int64(y) + addYear + addMonth/12
	month := int64(m) + addMonth%12
	if month <= 0 {
		year--
		month += 12
	}
	if month > 12 {
		year++
		month -= 12
	}

	y = int32(year)
	m = uint8(month)

	lastDay := LastDay(y, m)
	if lastDay < d {
		d = lastDay
	}

	switch timeType {
	case DateType:
		if !ValidDate(y, m, d) {
			return 0, false
		}
	case DateTimeType, TimeStampType:
		if !ValidDatetime(y, m, d) {
			return 0, false
		}
	}
	newDate := DateFromCalendar(y, m, d)
	return dt + Datetime(newDate-oldDate)*SecsPerDay*MicroSecsPerSec, true
}

// AddInterval now date or datetime use the function to add/sub date,
// we need a bool arg to tell isDate/isDatetime
// date/datetime have different regions, so we don't use same valid function
// return type bool means the if the date/datetime is valid
func (dt Datetime) AddInterval(nums int64, its IntervalType, timeType TimeType) (Datetime, bool) {
	var addMonth, addYear int64
	switch its {
	case Second:
		nums *= MicroSecsPerSec
	case Minute:
		nums *= MicroSecsPerSec * SecsPerMinute
	case Hour:
		nums *= MicroSecsPerSec * SecsPerHour
	case Day:
		nums *= MicroSecsPerSec * SecsPerDay
	case Week:
		nums *= MicroSecsPerSec * SecsPerWeek
	case Month:
		addMonth = nums
		return dt.AddDateTime(addMonth, addYear, timeType)
	case Quarter:
		addMonth = 3 * nums
		return dt.AddDateTime(addMonth, addYear, timeType)
	case Year:
		addYear = nums
		return dt.AddDateTime(addMonth, addYear, timeType)
	}

	newDate := dt + Datetime(nums)
	y, m, d, _ := newDate.ToDate().Calendar(true)
	if !ValidDatetime(y, m, d) {
		return 0, false
	}
	return newDate, true
}

func (dt Datetime) DateTimeDiffWithUnit(its string, secondDt Datetime) (int64, error) {
	switch its {
	case "microsecond":
		return int64(dt - secondDt), nil
	case "second":
		return (dt - secondDt).sec(), nil
	case "minute":
		return int64(dt-secondDt) / (MicroSecsPerSec * SecsPerMinute), nil
	case "hour":
		return int64(dt-secondDt) / (MicroSecsPerSec * SecsPerHour), nil
	case "day":
		return int64(dt-secondDt) / (MicroSecsPerSec * SecsPerDay), nil
	case "week":
		return int64(dt-secondDt) / (MicroSecsPerSec * SecsPerWeek), nil
	case "month":
		return dt.ConvertToMonth(secondDt), nil
	case "quarter":
		return dt.ConvertToMonth(secondDt) / 3, nil
	case "year":
		return dt.ConvertToMonth(secondDt) / 12, nil
	}
	return 0, moerr.NewInvalidInputNoCtx("invalid time_stamp_unit input")
}

func (dt Datetime) DatetimeMinusWithSecond(secondDt Datetime) int64 {
	return int64((dt - secondDt) / MicroSecsPerSec)
}

func (dt Datetime) ConvertToMonth(secondDt Datetime) int64 {

	dayDiff := int64(dt.ToDate().Day()) - int64(secondDt.ToDate().Day())
	monthDiff := (int64(dt.ToDate().Year())-int64(secondDt.ToDate().Year()))*12 + int64(dt.ToDate().Month()) - int64(secondDt.ToDate().Month())

	if dayDiff >= 0 {
		return monthDiff
	} else {
		return monthDiff - 1
	}
}

func (dt Datetime) MicroSec() int64 {
	return int64(dt) % MicroSecsPerSec
}

func (dt Datetime) sec() int64 {
	return int64(dt) / MicroSecsPerSec
}

func (dt Datetime) Year() uint16 {
	return dt.ToDate().Year()
}

func (dt Datetime) Month() uint8 {
	return dt.ToDate().Month()
}

func (dt Datetime) Day() uint8 {
	return dt.ToDate().Day()
}

func (dt Datetime) WeekOfYear() (int32, uint8) {
	return dt.ToDate().WeekOfYear()
}

func (dt Datetime) DayOfYear() uint16 {
	return dt.ToDate().DayOfYear()
}

func (dt Datetime) DayOfWeek() Weekday {
	return dt.ToDate().DayOfWeek()
}

func (dt Datetime) DayOfWeek2() Weekday {
	return dt.ToDate().DayOfWeek2()
}

func (dt Datetime) Week(mode int) int {
	return dt.ToDate().Week(mode)
}

// YearWeek returns year and week.
func (dt Datetime) YearWeek(mode int) (year int, week int) {
	return dt.ToDate().YearWeek(mode)
}

func (dt Datetime) ToTimestamp(loc *time.Location) Timestamp {
	return Timestamp(dt.ConvertToGoTime(loc).UnixMicro() + unixEpochMicroSecs)
}

func (dt Datetime) SecondMicrosecondStr() string {
	result := fmt.Sprintf("%02d", dt.Sec()) + "." + fmt.Sprintf("%06d", dt.MicroSec())
	return result
}

func (dt Datetime) MinuteMicrosecondStr() string {
	result := fmt.Sprintf("%02d", dt.Minute()) + ":" + fmt.Sprintf("%02d", dt.Sec()) + "." + fmt.Sprintf("%06d", dt.MicroSec())
	return result
}

func (dt Datetime) MinuteSecondStr() string {
	result := fmt.Sprintf("%02d", dt.Minute()) + ":" + fmt.Sprintf("%02d", dt.Sec())
	return result
}

func (dt Datetime) HourMicrosecondStr() string {
	result := fmt.Sprintf("%2d", dt.Hour()) + ":" + fmt.Sprintf("%02d", dt.Minute()) + ":" + fmt.Sprintf("%02d", dt.Sec()) + "." + fmt.Sprintf("%06d", dt.MicroSec())
	return result
}

func (dt Datetime) HourSecondStr() string {
	result := fmt.Sprintf("%2d", dt.Hour()) + ":" + fmt.Sprintf("%02d", dt.Minute()) + ":" + fmt.Sprintf("%02d", dt.Sec())
	return result
}

func (dt Datetime) HourMinuteStr() string {
	result := fmt.Sprintf("%2d", dt.Hour()) + ":" + fmt.Sprintf("%02d", dt.Minute())
	return result
}

func (dt Datetime) DayMicrosecondStr() string {
	result := fmt.Sprintf("%02d", dt.Day()) + " " + dt.HourMicrosecondStr()
	return result
}

func (dt Datetime) DaySecondStr() string {
	result := fmt.Sprintf("%02d", dt.Day()) + " " + dt.HourSecondStr()
	return result
}

func (dt Datetime) DayMinuteStr() string {
	result := fmt.Sprintf("%02d", dt.Day()) + " " + dt.HourMinuteStr()
	return result
}

func (dt Datetime) DayHourStr() string {
	result := fmt.Sprintf("%02d", dt.Day()) + " " + fmt.Sprintf("%02d", dt.Hour())
	return result
}

func (dt Datetime) YearMonthStr() string {
	result := fmt.Sprintf("%04d", dt.Year()) + " " + fmt.Sprintf("%02d", dt.Month())
	return result
}

// date[0001-01-01 00:00:00 to 9999-12-31 23:59:59]
func ValidDatetime(year int32, month, day uint8) bool {
	if year >= MinDatetimeYear && year <= MaxDatetimeYear {
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

func (dt Datetime) SecsSinceUnixEpoch() int64 {
	return (int64(dt) - unixEpochMicroSecs) / MicroSecsPerSec
}

func (dt Datetime) ToDecimal64() Decimal64 {
	return Decimal64(int64(dt) - unixEpochMicroSecs)
}

func (dt Datetime) ToDecimal128() Decimal128 {
	return Decimal128{uint64(int64(dt) - unixEpochMicroSecs), 0}
}
