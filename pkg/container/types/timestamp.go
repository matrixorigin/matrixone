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

// timestamp data type:
// Question 1: When should I use Datetime, and when should I use Timestamp?
// 		Well, during insertion, the Datetime value will be stored as is(we have some bugs in here now),
//		but for timestamp, the timestamp value passed in will be converted to a UTC timestamp, that is,
//		the value passed in subtract by the server's local Time Zone
// 		so, during retrieval, if the server's time zone is the same as the time zone when the timestamp value got inserted,
//		the timestamp valued retrieved is the same value as the inserted, but if these two timezones are different, you
// 		will get different timestamp value.
//      for example:     		insertion timezone	insertion value					retrieval timezone  retrieval value
// 								UTC+8 				2022-05-01 11:11:11				UTC+9				2022-05-01 12:11:11
//
// So, if your application is geo-distributed cross different timezones, using TIMESTAMP could save you trouble
// you may otherwise encounter by using DATETIME
//
// Internal representation:
// timestamp values are represented using a 64bit integer, the higher 40 bits stores the secs since January 1, year 1, local time zone, in Gregorian
// calendar, and lower 20 bits hold the number of microseconds
// the default fractional seconds precision(fsp) for TIMESTAMP is 6, as SQL standard requires.

package types

import (
	"fmt"
	"strconv"
	gotime "time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
)

const microSecondsDigits = 6

var TimestampMinValue Timestamp
var TimestampMaxValue Timestamp
var (
	ErrInvalidTimestampAddInterval = errors.New(errno.DataException, "Beyond the range of timestamp")
)

// the range for TIMESTAMP values is '1970-01-01 00:00:01.000000' to '2038-01-19 03:14:07.999999'.
func init() {
	TimestampMinValue = FromClockUTC(1970, 1, 1, 0, 0, 1, 0)
	TimestampMaxValue = FromClockUTC(2038, 1, 19, 3, 14, 07, 999999)
}

func (ts Timestamp) String() string {
	dt := Datetime(int64(ts) + localTZ<<20)
	y, m, d, _ := dt.ToDate().Calendar(true)
	hour, minute, sec := dt.Clock()
	msec := int64(ts) & 0xfffff // the lower 20 bits of timestamp stores the microseconds value
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d", y, m, d, hour, minute, sec, msec)
}

// String2 stringify timestamp, including its fractional seconds precision part(fsp)
func (ts Timestamp) String2(precision int32) string {
	dt := Datetime(int64(ts) + localTZ<<20)
	y, m, d, _ := dt.ToDate().Calendar(true)
	hour, minute, sec := dt.Clock()
	if precision > 0 {
		msec := int64(dt) & 0xfffff
		msecInstr := fmt.Sprintf("%06d\n", msec)
		msecInstr = msecInstr[:precision]

		return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d"+"."+msecInstr, y, m, d, hour, minute, sec)
	}

	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", y, m, d, hour, minute, sec)
}

var (
	errIncorrectTimestampValue = errors.New(errno.DataException, "Incorrect timestamp value")
	errTimestampOutOfRange     = errors.New(errno.DataException, "timestamp out of range")
)

// this scaleTable stores the corresponding microseconds value for a precision
var scaleTable = [...]uint32{1000000, 100000, 10000, 1000, 100, 10, 1}

var OneSecInMicroSeconds = uint32(1000000)

func getMsec(msecStr string, precision int32) (uint32, uint32, error) {
	msecs := uint32(0)
	carry := uint32(0)
	msecCarry := uint32(0)
	if len(msecStr) > int(precision) {
		if msecStr[precision] >= '5' && msecStr[precision] <= '9' {
			msecCarry = 1
		} else if msecStr[precision] >= '0' && msecStr[precision] <= '4' {
			msecCarry = 0
		} else {
			return 0, 0, ErrIncorrectDatetimeValue
		}
		msecStr = msecStr[:precision]
	} else if len(msecStr) < int(precision) {
		lengthMsecStr := len(msecStr)
		padZeros := int(precision) - lengthMsecStr
		for i := 0; i < padZeros; i++ {
			msecStr = msecStr + string('0')
		}
	}
	if len(msecStr) == 0 { // this means the precision is 0
		return 0, msecCarry, nil
	}
	m, err := strconv.ParseUint(msecStr, 10, 32)
	if err != nil {
		return 0, 0, errIncorrectTimestampValue
	}
	msecs = (uint32(m) + msecCarry) * scaleTable[precision]
	if msecs == OneSecInMicroSeconds {
		carry = 1
		msecs = 0
	}
	return msecs, carry, nil
}

func (d Date) ToTimeUTC() Timestamp {
	return Timestamp((int64(d)*secsPerDay)<<20 - (localTZ << 20))
}

// ParseTimestamp will parse a string to be a Timestamp
// Support Format:
// 1. all the Date value
// 2. yyyy-mm-dd hh:mm:ss(.msec)
// 3. yyyymmddhhmmss(.msec)
func ParseTimestamp(s string, precision int32) (Timestamp, error) {
	if len(s) < 14 {
		if d, err := ParseDate(s); err == nil {
			return d.ToTimeUTC(), nil
		}
		return -1, errIncorrectTimestampValue
	}
	var year int32
	var month, day, hour, minute, second uint8
	var msec uint32 = 0
	var carry uint32 = 0
	var err error

	year = int32(s[0]-'0')*1000 + int32(s[1]-'0')*100 + int32(s[2]-'0')*10 + int32(s[3]-'0')
	if s[4] == '-' {
		if len(s) < 19 {
			return -1, errIncorrectTimestampValue
		}
		month = (s[5]-'0')*10 + (s[6] - '0')
		if s[7] != '-' {
			return -1, errIncorrectTimestampValue
		}
		day = (s[8]-'0')*10 + (s[9] - '0')
		if s[10] != ' ' {
			return -1, errIncorrectTimestampValue
		}
		if !validDate(year, month, day) {
			return -1, errIncorrectTimestampValue
		}
		hour = (s[11]-'0')*10 + (s[12] - '0')
		if s[13] != ':' {
			return -1, errIncorrectTimestampValue
		}
		minute = (s[14]-'0')*10 + (s[15] - '0')
		if s[16] != ':' {
			return -1, errIncorrectTimestampValue
		}
		second = (s[17]-'0')*10 + (s[18] - '0')
		if !validTimeInDay(hour, minute, second) {
			return -1, errIncorrectTimestampValue
		}
		if len(s) > 19 {
			// for a timestamp string like "2020-01-01 11:11:11.123"
			// the microseconds part .123 should be interpreted as 123000 microseconds, so we need to pad zeros
			if len(s) > 20 && s[19] == '.' {
				msecStr := s[20:]
				msec, carry, err = getMsec(msecStr, precision)
				if err != nil {
					return -1, errIncorrectTimestampValue
				}
			} else {
				return -1, errIncorrectTimestampValue
			}
		}
	} else {
		month = (s[4]-'0')*10 + (s[5] - '0')
		day = (s[6]-'0')*10 + (s[7] - '0')
		hour = (s[8]-'0')*10 + (s[9] - '0')
		minute = (s[10]-'0')*10 + (s[11] - '0')
		second = (s[12]-'0')*10 + (s[13] - '0')
		if len(s) > 14 {
			// for a timestamp string like "20200101111111.123"
			// the microseconds part .123 should be interpreted as 123000 microseconds, so we need to pad zeros
			if len(s) > 15 && s[14] == '.' {
				msecStr := s[15:]
				msec, carry, err = getMsec(msecStr, precision)
				if err != nil {
					return -1, errIncorrectTimestampValue
				}
			} else {
				return -1, errIncorrectTimestampValue
			}
		}
	}

	result := FromClockUTC(year, month, day, hour, minute, second+uint8(carry), msec)
	if result > TimestampMaxValue || result < TimestampMinValue {
		return -1, errTimestampOutOfRange
	}

	return result, nil
}

func TimestampToDatetime(xs []Timestamp, rs []Datetime) ([]Datetime, error) {
	localTZAligned := localTZ << 20
	xsInInt64 := *(*[]int64)(unsafe.Pointer(&xs))
	rsInInt64 := *(*[]int64)(unsafe.Pointer(&rs))
	for i, x := range xsInInt64 {
		rsInInt64[i] = x + localTZAligned
	}
	return rs, nil
}

// FromClockUTC gets the utc time value in Timestamp
func FromClockUTC(year int32, month, day, hour, min, sec uint8, msec uint32) Timestamp {
	days := FromCalendar(year, month, day)
	secs := int64(days)*secsPerDay + int64(hour)*secsPerHour + int64(min)*secsPerMinute + int64(sec) - localTZ
	return Timestamp((secs << 20) + int64(msec))
}

func NowUTC() Timestamp {
	t := gotime.Now()
	t.UTC()
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
	return Timestamp((sec << 20) + nsec/1000)
}

func ValidTimestamp(timestamp Timestamp) bool {
	return timestamp > TimestampMinValue && timestamp < TimestampMaxValue
}
