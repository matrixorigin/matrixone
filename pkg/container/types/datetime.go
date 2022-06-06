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
	gotime "time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
)

const (
	secsPerMinute = 60
	secsPerHour   = 60 * secsPerMinute
	secsPerDay    = 24 * secsPerHour
	//secsPerWeek   = 7 * secsPerDay
	microSecondBitMask = 0xfffff
)

// The higher 44 bits holds number of seconds since January 1, year 1 in Gregorian
// calendar, and lower 20 bits holds number of microseconds

func (dt Datetime) String() string {
	// when datetime have microsecond, we print it, default precision is 6
	y, m, d, _ := dt.ToDate().Calendar(true)
	hour, minute, sec := dt.Clock()
	msec := int64(dt) & microSecondBitMask
	if msec > 0 {
		msecInstr := fmt.Sprintf("%06d", msec)
		return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d"+"."+msecInstr, y, m, d, hour, minute, sec)
	}
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", y, m, d, hour, minute, sec)
}

const (
	//tsMask         = ^uint64(0) >> 1
	hasMonotonic = 1 << 63
	//unixToInternal = (1969*365 + 1969/4 - 1969/100 + 1969/400) * secsPerDay
	wallToInternal = (1884*365 + 1884/4 - 1884/100 + 1884/400) * secsPerDay

	minHourInDay, maxHourInDay           = 0, 23
	minMinuteInHour, maxMinuteInHour     = 0, 59
	minSecondInMinute, maxSecondInMinute = 0, 59
)

var (
	errIncorrectDatetimeValue = errors.New(errno.DataException, "Incorrect datetime value")
)

// ParseDatetime will parse a string to be a Datetime
// Support Format:
// 1. all the Date value
// 2. yyyy-mm-dd hh:mm:ss(.msec)
// 3. yyyymmddhhmmss(.msec)
// Notice: 2022-01-01 00:00:00.1 and 20220101000000.1 should parse to 100000 microsecond instead of 1 microsecond
// I call the situation is microsecond number bug
func ParseDatetime(s string) (Datetime, error) {
	if len(s) < 14 {
		if d, err := ParseDate(s); err == nil {
			return d.ToTime(), nil
		}
		return -1, errIncorrectDatetimeValue
	}
	var year int32
	var month, day, hour, minute, second uint8
	var msec uint32 = 0

	year = int32(s[0]-'0')*1000 + int32(s[1]-'0')*100 + int32(s[2]-'0')*10 + int32(s[3]-'0')
	if s[4] == '-' {
		if len(s) < 19 {
			return -1, errIncorrectDatetimeValue
		}
		month = (s[5]-'0')*10 + (s[6] - '0')
		if s[7] != '-' {
			return -1, errIncorrectDatetimeValue
		}
		day = (s[8]-'0')*10 + (s[9] - '0')
		if s[10] != ' ' {
			return -1, errIncorrectDatetimeValue
		}
		if !validDate(year, month, day) {
			return -1, errIncorrectDatetimeValue
		}
		hour = (s[11]-'0')*10 + (s[12] - '0')
		if s[13] != ':' {
			return -1, errIncorrectDatetimeValue
		}
		minute = (s[14]-'0')*10 + (s[15] - '0')
		if s[16] != ':' {
			return -1, errIncorrectDatetimeValue
		}
		second = (s[17]-'0')*10 + (s[18] - '0')
		if !validTimeInDay(hour, minute, second) {
			return -1, errIncorrectDatetimeValue
		}
		if len(s) > 19 {
			if len(s) > 20 && s[19] == '.' {
				m, err := strconv.ParseUint(s[20:], 10, 32)
				if err != nil {
					return -1, errIncorrectDatetimeValue
				}
				// fix microsecond number bug
				m = m * uint64(math.Pow10(26-len(s)))
				msec = uint32(m)
			} else {
				return -1, errIncorrectDatetimeValue
			}
		}
	} else {
		month = (s[4]-'0')*10 + (s[5] - '0')
		day = (s[6]-'0')*10 + (s[7] - '0')
		hour = (s[8]-'0')*10 + (s[9] - '0')
		minute = (s[10]-'0')*10 + (s[11] - '0')
		second = (s[12]-'0')*10 + (s[13] - '0')
		if len(s) > 14 {
			if len(s) > 15 && s[14] == '.' {
				m, err := strconv.ParseUint(s[15:], 10, 32)
				if err != nil {
					return -1, errIncorrectDatetimeValue
				}
				// fix microsecond number bug
				m = m * uint64(math.Pow10(21-len(s)))
				msec = uint32(m)
			} else {
				return -1, errIncorrectDatetimeValue
			}
		}
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

// UTC turn local datetime to utc datetime
func (dt Datetime) UTC() Datetime {
	return Datetime((dt.sec() - localTZ) << 20)
}

func (dt Datetime) UnixTimestamp() int64 {
	return dt.sec() - unixEpoch
}

func FromUnix(time int64) Datetime {
	return Datetime((time + unixEpoch) << 20)
}

func Now() Datetime {
	t := gotime.Now()
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
	return Date((dt.sec()) / secsPerDay)
}

func (dt Datetime) Clock() (hour, min, sec int8) {
	t := (dt.sec()) % secsPerDay
	hour = int8(t / secsPerHour)
	min = int8(t % secsPerHour / secsPerMinute)
	sec = int8(t % secsPerMinute)
	return
}

func FromClock(year int32, month, day, hour, min, sec uint8, msec uint32) Datetime {
	days := FromCalendar(year, month, day)
	secs := int64(days)*secsPerDay + int64(hour)*secsPerHour + int64(min)*secsPerMinute + int64(sec)
	return Datetime((secs << 20) + int64(msec))
}

func (dt Datetime) ConvertToGoTime() gotime.Time {
	y, m, d, _ := dt.ToDate().Calendar(true)
	msec := dt.microSec()
	hour, min, sec := dt.Clock()
	return gotime.Date(int(y), gotime.Month(m), int(d), int(hour), int(min), int(sec), int(msec*1000), startupTime.Location())
}

func (dt Datetime) AddDateTime(date gotime.Time, addMsec, addSec, addMin, addHour, addDay, addMonth, addYear int64) Datetime {
	date = date.Add(gotime.Duration(addMsec) * gotime.Microsecond)
	date = date.Add(gotime.Duration(addSec) * gotime.Second)
	date = date.Add(gotime.Duration(addMin) * gotime.Minute)
	date = date.Add(gotime.Duration(addHour) * gotime.Hour)
	// corner case: mysql: date_add('2022-01-31',interval 1 month) -> 2022-02-28
	// only in the month year year-month
	if addMonth != 0 || addYear != 0 {
		originDay := date.Day()
		newDate := date.AddDate(int(addYear), int(addMonth), int(addDay))
		newDay := newDate.Day()
		if originDay != newDay {
			maxDay := LastDay(uint16(newDate.Year()), uint8(newDate.Month()-1))
			addDay = int64(maxDay) - int64(originDay)
		}
	}
	date = date.AddDate(int(addYear), int(addMonth), int(addDay))
	return FromClock(int32(date.Year()), uint8(date.Month()), uint8(date.Day()), uint8(date.Hour()), uint8(date.Minute()), uint8(date.Second()), uint32(date.Nanosecond()/1000))
}

func (dt Datetime) AddInterval(nums int64, its IntervalType) Datetime {
	goTime := dt.ConvertToGoTime()
	var addMsec, addSec, addMin, addHour, addDay, addMonth, addYear int64
	switch its {
	case MicroSecond:
		addMsec += nums
	case Second:
		addSec += nums
	case Minute:
		addMin += nums
	case Hour:
		addHour += nums
	case Day:
		addDay += nums
	case Week:
		addDay += 7 * nums
	case Month:
		addMonth += nums
	case Quarter:
		addMonth += 3 * nums
	case Year:
		addYear += nums
	}
	return dt.AddDateTime(goTime, addMsec, addSec, addMin, addHour, addDay, addMonth, addYear)
}

func (dt Datetime) microSec() int64 {
	return int64(dt) << 44 >> 44
}

func (dt Datetime) sec() int64 {
	return int64(dt) >> 20
}

func (dt Datetime) Year() uint16 {
	return dt.ToDate().Year()
}

func (dt Datetime) Month() uint8 {
	return dt.ToDate().Month()
}
