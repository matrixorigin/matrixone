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
	"unsafe"
)

const microSecondsDigits = 6

func (ts Timestamp) String() string {
	dt := Datetime(int64(ts) + localTZ)
	y, m, d, _ := dt.ToDate().Calendar(true)
	hour, minute, sec := dt.Clock()
	msec := int64(ts) & 0xfffff // the lower 20 bits of timestamp stores the microseconds value
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d", y, m, d, hour, minute, sec, msec)
}

// String2 stringify timestamp, including its fractional seconds precision part(fsp)
func (ts Timestamp) String2(precision int32) string {
	dt := Datetime(int64(ts))
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

// ParseTimestamp will parse a string to be a Timestamp
// Support Format:
// 1. all the Date value
// 2. yyyy-mm-dd hh:mm:ss(.msec)
// 3. yyyymmddhhmmss(.msec)
func ParseTimestamp(s string, precision int32) (Timestamp, error) {
	if len(s) < 14 {
		if d, err := ParseDate(s); err == nil {
			return Timestamp(d.ToTime()), nil
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
			// for a timestamp string like "2020-01-01 11:11:11.123"
			// the microseconds part .123 should be interpreted as 123000 microseconds, so we need to pad zeros
			if len(s) > 20 && s[19] == '.' {
				msecStr := s[20:]
				lengthMsecStr := len(msecStr)
				if lengthMsecStr > int(precision) {
					msecStr = msecStr[:precision]
				}
				padZeros := microSecondsDigits - len(msecStr)
				for i := 0; i < padZeros; i++ {
					msecStr = msecStr + string('0')
				}
				fmt.Println("len(s) > 19, msecStr is", msecStr)
				m, err := strconv.ParseUint(msecStr, 10, 32)
				if err != nil {
					return -1, errIncorrectDatetimeValue
				}
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
			// for a timestamp string like "20200101111111.123"
			// the microseconds part .123 should be interpreted as 123000 microseconds, so we need to pad zeros
			if len(s) > 15 && s[14] == '.' {
				msecStr := s[15:]
				lengthMsecStr := len(msecStr)
				if lengthMsecStr > microSecondsDigits {
					msecStr = msecStr[:microSecondsDigits]
				} else {
					padZeros := microSecondsDigits - lengthMsecStr
					for i := 0; i < padZeros; i++ {
						msecStr = msecStr + string('0')
					}
				}
				m, err := strconv.ParseUint(msecStr, 10, 32)
				if err != nil {
					return -1, errIncorrectDatetimeValue
				}
				msec = uint32(m)
			} else {
				return -1, errIncorrectDatetimeValue
			}
		}
	}
	result := FromClockUTC(year, month, day, hour, minute, second, msec)

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

// FromClock2 gets the utc time value in Timestamp
func FromClockUTC(year int32, month, day, hour, min, sec uint8, msec uint32) Timestamp {
	days := FromCalendar(year, month, day)
	secs := int64(days)*secsPerDay + int64(hour)*secsPerHour + int64(min)*secsPerMinute + int64(sec) - localTZ
	return Timestamp((secs << 20) + int64(msec))
}
