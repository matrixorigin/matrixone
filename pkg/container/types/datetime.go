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
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

const (
	secsPerMinute = 60
	secsPerHour   = 60 * secsPerMinute
	secsPerDay    = 24 * secsPerHour
	secsPerWeek   = 7 * secsPerDay
)

// The higher 44 bits holds number of seconds since January 1, year 1 in Gregorian
// calendar, and lower 20 bits holds number of microseconds

func (a Datetime) String() string {
	y, m, d, _ := a.ToDate().Calendar(true)
	hour, minute, sec := a.Clock()
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", y, m, d, hour, minute, sec)
}

const (
	tsMask         = ^uint64(0) >> 1
	hasMonotonic   = 1 << 63
	unixToInternal = (1969*365 + 1969/4 - 1969/100 + 1969/400) * secsPerDay
	wallToInternal = (1884*365 + 1884/4 - 1884/100 + 1884/400) * secsPerDay

	minHourInDay, maxHourInDay = 0, 23
	minMinuteInHour, maxMinuteInHour = 0, 59
	minSecondInMinute, maxSecondInMinute = 0, 59
)

var (
	errIncorrectDatetimeValue = errors.New(errno.DataException, "Incorrect datetime value")

	// reg1 reg2 are regexps to match `year-month-day hh:mm:ss.msec`
	reg1 = regexp.MustCompile(`^(?P<year>[0-9]+)[-](?P<month>[0-9]+)[-](?P<day>[0-9]+)$`)
	reg2 = regexp.MustCompile(`^(?P<hour>[0-9]+)[:](?P<minute>[0-9]+)[:](?P<second>[0-9]+)(?P<msec>[.][0-9]+)?$`)

	nowDatetime = "now()"
	nowDatetimeLength = len(nowDatetime)

	// regSimple is the regexp to match `yearMonthDayHourMinuteSecond.Sec` and some datetime like that
	regSimple = regexp.MustCompile(`^(?P<part1>[0-9]+)(?P<part2>[.][0-9]+)?$`)

	// format1 format2 format3 format4 are valid length cases for datetime-format used in ParseDatetime
	format1 = 4 + 2 + 2 + 2 + 2 + 2 // full year + month + day + hour + minute + second
	format2 = 3 + 2 + 2 + 2 + 2 + 2
	format3 = 2 + 2 + 2 + 2 + 2 + 2
	format4 = 1 + 2 + 2 + 2 + 2 + 2 // shortest year + month + day + hour + minute + second
)

// ParseDatetime parse a format string to be a Datetime
// support Format:
//	All Datetime
//	Datetime + day time(hh:mm:ss or hh:mm:ss.[0, 9]*)
func ParseDatetime(s string) (Datetime, error) {
	var year int32
	var month, day, hour, minute, second uint8
	var msec uint32 = 0
	var ymdhms [6]string // strings of year, month, day, hour, minute, second

	if len(s) < shortestDateFormat {
		return -1, errIncorrectDatetimeValue
	}

	//if len(s) == nowDatetimeLength && s == nowDatetime { // special case
	//	return Now(), nil
	//}

	{ // match which string without hour-minute-second part.
		if d, err := ParseDate(s); err == nil {
			return d.ToTime(), nil
		}
	}

	datetimeParts := strings.Split(s, " ")
	switch len(datetimeParts) {
	case 1: // possible to be `yearMonthDayHourMinuteSecond.Msec`
		{
			match := regSimple.FindStringSubmatch(datetimeParts[0])
			switch len(match) {
			case 3:
				if len(match[1]) <= format1 && len(match[1]) >= format4 {
					if len(match[2]) > 1 {
						if ms, err := strconv.ParseUint(match[2][1:], 10, 32); err != nil {
							return -1, errIncorrectDatetimeValue
						} else {
							msec = uint32(ms)
						}
					}
					switch len(match[1]) {
					case format1:
						ymdhms[0], ymdhms[1], ymdhms[2] = match[1][0:4], match[1][4:6], match[1][6:8]
						ymdhms[3], ymdhms[4], ymdhms[5] = match[1][8:10], match[1][10:12], match[1][12:]
					case format2:
						ymdhms[0], ymdhms[1], ymdhms[2] = thisCenturyStr0 + match[1][0:3], match[1][3:5], match[1][5:7]
						ymdhms[3], ymdhms[4], ymdhms[5] = match[1][7:9], match[1][9:11], match[1][11:]
					case format3:
						ymdhms[0], ymdhms[1], ymdhms[2] = thisCenturyStr1 + match[1][0:2], match[1][2:4], match[1][4:6]
						ymdhms[3], ymdhms[4], ymdhms[5] = match[1][6:8], match[1][8:10], match[1][10:]
					case format4:
						ymdhms[0], ymdhms[1], ymdhms[2] = thisCenturyStr2 + match[1][0:1], match[1][1:3], match[1][3:5]
						ymdhms[3], ymdhms[4], ymdhms[5] = match[1][5:7], match[1][7:9], match[1][9:]
					}
				} else {
					return -1, errIncorrectDatetimeValue
				}
			default:
				return -1, errIncorrectDatetimeValue
			}
		}
	case 2:	// only possible to be `year-month-day hh:mm:ss.[0,9]*`
		{ // to match year-month-day
			match := reg1.FindStringSubmatch(datetimeParts[0])
			switch len(match) {
			case 4:
				ymdhms[0], ymdhms[1], ymdhms[2] = match[1], match[2], match[3]
			default:
				return -1, errIncorrectDatetimeValue
			}
		}
		{ // to match hh:mm:ss.[0-9]*
			match := reg2.FindStringSubmatch(datetimeParts[1])
			switch len(match) {
			case 5:
				ymdhms[3], ymdhms[4], ymdhms[5] = match[1], match[2], match[3]
				if len(match[4]) > 1 { // with fractional part, and match[4] is '.xx'
					if ms, err := strconv.ParseUint(match[4][1:], 10, 32); err != nil {
						return -1, errIncorrectDatetimeValue
					} else {
						msec = uint32(ms)
					}
				}
			default:
				return -1, errIncorrectDatetimeValue
			}
		}
	default:
		return -1, errIncorrectDatetimeValue
	}

	{ // year
		y, err := strconv.ParseInt(ymdhms[0], 10, 32)
		if err != nil {
			return -1, errIncorrectDatetimeValue
		}
		year = int32(y)
	}
	{ // month
		m, err := strconv.ParseUint(ymdhms[1], 10, 8)
		if err != nil {
			return -1, errIncorrectDatetimeValue
		}
		month = uint8(m)
	}
	{ // day
		d, err := strconv.ParseUint(ymdhms[2], 10, 8)
		if err != nil {
			return -1, errIncorrectDatetimeValue
		}
		day = uint8(d)
	}
	{ // hour
		h, err := strconv.ParseUint(ymdhms[3], 10, 8)
		if err != nil {
			return -1, errIncorrectDatetimeValue
		}
		hour = uint8(h)
	}
	{ // minute
		m, err := strconv.ParseUint(ymdhms[4], 10, 8)
		if err != nil {
			return -1, errIncorrectDatetimeValue
		}
		minute = uint8(m)
	}
	{ // second
		s, err := strconv.ParseUint(ymdhms[5], 10, 8)
		if err != nil {
			return -1, errIncorrectDatetimeValue
		}
		second = uint8(s)
	}
	if !validDate(year, month, day) || !validTimeInDay(hour, minute, second) {
		return -1, errIncorrectDatetimeValue
	}
	return FromClock(year, month, day, hour, minute, second, msec), nil
}

// validTimeInDay return true if hour, minute and second can be a time during a day
func validTimeInDay(h, m, s uint8) bool {
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

func Now() Datetime {
	t := time.Now()
	wall := *(*uint64)(unsafe.Pointer(&t))
	ext := *(*int64)(unsafe.Pointer(uintptr(unsafe.Pointer(&t)) + unsafe.Sizeof(wall)))
	var sec, nsec int64
	if wall&hasMonotonic != 0 {
		sec = int64(wall<<1>>31) + wallToInternal
		nsec = int64(wall << 34 >> 34)
	} else {
		sec = ext
		nsec = int64(wall)
	}
	return Datetime((sec << 20) + nsec/1000)
}

func (dt Datetime) ToDate() Date {
	return Date((dt.sec() + localTZ) / secsPerDay)
}

func (dt Datetime) Clock() (hour, min, sec int8) {
	t := (dt.sec() + localTZ) % secsPerDay
	hour = int8(t / secsPerHour)
	min = int8(t % secsPerHour / secsPerMinute)
	sec = int8(t % secsPerMinute)
	return
}

func FromClock(year int32, month, day, hour, min, sec uint8, msec uint32) Datetime {
	days := FromCalendar(year, month, day)
	secs := int64(days)*secsPerDay + int64(hour)*secsPerHour + int64(min)*secsPerMinute + int64(sec) - localTZ
	return Datetime((secs << 20) + int64(msec))
}

func (dt Datetime) sec() int64 {
	return int64(dt) >> 20
}
