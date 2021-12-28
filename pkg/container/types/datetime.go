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
	"strconv"
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

	year = int32(s[0]-'0') * 1000 + int32(s[1]-'0') * 100 + int32(s[2]-'0') * 10 + int32(s[3]-'0')
	if s[4] == '-' {
		if len(s) < 19 {
			return -1, errIncorrectDatetimeValue
		}
		month = (s[5]-'0') * 10 + (s[6]-'0')
		if s[7] != '-' {
			return -1, errIncorrectDatetimeValue
		}
		day = (s[8]-'0') * 10 + (s[9]-'0')
		if s[10] != ' ' {
			return -1, errIncorrectDatetimeValue
		}
		if !validDate(year, month, day) {
			return -1, errIncorrectDatetimeValue
		}
		hour = (s[11]-'0') * 10 + (s[12]-'0')
		if s[13] != ':' {
			return -1, errIncorrectDatetimeValue
		}
		minute = (s[14]-'0') * 10 + (s[15]-'0')
		if s[16] != ':' {
			return -1, errIncorrectDatetimeValue
		}
		second = (s[17]-'0') * 10 + (s[18]-'0')
		if !validTimeInDay(hour, minute, second) {
			return -1, errIncorrectDatetimeValue
		}
		if len(s) > 19 {
			if len(s) > 20 && s[19] == '.' {
				m, err := strconv.ParseUint(s[20:], 10, 32)
				if err != nil {
					return -1, errIncorrectDatetimeValue
				}
				msec = uint32(m)
			} else {
				return -1, errIncorrectDatetimeValue
			}
		}
	} else {
		month = (s[4]-'0') * 10 + (s[5]-'0')
		day = (s[6]-'0') * 10 + (s[7]-'0')
		hour = (s[8]-'0') * 10 + (s[9]-'0')
		minute = (s[10]-'0') * 10 + (s[11]-'0')
		second = (s[12]-'0') * 10 + (s[13]-'0')
		if len(s) > 14 {
			if len(s) > 15 && s[14] == '.' {
				m, err := strconv.ParseUint(s[15:], 10, 32)
				if err != nil {
					return -1, errIncorrectDatetimeValue
				}
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

func (dt Datetime) Year() uint16 {
	return dt.ToDate().Year()
}
