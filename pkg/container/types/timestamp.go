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
// timestamp values are represented using a 64bit integer, which stores the microsecs since January 1, year 1, local time zone, in Gregorian calendar
// the default fractional seconds scale(fsp) for TIMESTAMP is 6, as SQL standard requires.

package types

import (
	"fmt"
	"strconv"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

var (
	FillString = []string{"", "0", "00", "000", "0000", "00000", "000000", "0000000"}
)

//const microSecondsDigits = 6

var TimestampMinValue Timestamp
var TimestampMaxValue Timestamp

// the range for TIMESTAMP values is '1970-01-01 00:00:01.000000' to '2038-01-19 03:14:07.999999'.
func init() {
	TimestampMinValue = FromClockUTC(1970, 1, 1, 0, 0, 1, 0)
	TimestampMaxValue = FromClockUTC(9999, 12, 31, 23, 59, 59, 999999)
}

func (ts Timestamp) String() string {
	dt := Datetime(int64(ts))
	y, m, d, _ := dt.ToDate().Calendar(true)
	hour, minute, sec := dt.Clock()
	msec := int64(ts) % microSecsPerSec
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d UTC", y, m, d, hour, minute, sec, msec)
}

// String2 stringify timestamp, including its fractional seconds precision part(fsp)
func (ts Timestamp) String2(loc *time.Location, scale int32) string {
	t := time.UnixMicro(int64(ts) - unixEpochMicroSecs).In(loc)
	y, m, d := t.Date()
	hour, minute, sec := t.Clock()
	if scale > 0 {
		msec := t.Nanosecond() / 1000
		msecInstr := fmt.Sprintf("%06d\n", msec)
		msecInstr = msecInstr[:scale]

		return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d"+"."+msecInstr, y, m, d, hour, minute, sec)
	}

	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", y, m, d, hour, minute, sec)
}

func (ts Timestamp) Unix() int64 {
	return (int64(ts) - unixEpochMicroSecs) / microSecsPerSec
}

func (ts Timestamp) UnixToFloat() float64 {
	return float64(int64(ts)-unixEpochMicroSecs) / microSecsPerSec
}

func (ts Timestamp) UnixToDecimal128() (Decimal128, error) {
	a, err := Decimal128_FromStringWithScale(fmt.Sprintf("%d", int64(ts)-unixEpochMicroSecs), 64, 6)
	if err != nil {
		return a, err
	}
	return a.DivInt64(microSecsPerSec), nil
}

// this scaleTable stores the corresponding microseconds value for a scale
var scaleTable = [...]uint32{1000000, 100000, 10000, 1000, 100, 10, 1}

var OneSecInMicroSeconds = uint32(1000000)

func getMsec(msecStr string, scale int32) (uint32, uint32, error) {
	msecs := uint32(0)
	carry := uint32(0)
	msecCarry := uint32(0)
	if len(msecStr) > int(scale) {
		if msecStr[scale] >= '5' && msecStr[scale] <= '9' {
			msecCarry = 1
		} else if msecStr[scale] >= '0' && msecStr[scale] <= '4' {
			msecCarry = 0
		} else {
			return 0, 0, moerr.NewInvalidArgNoCtx("get ms", msecStr)
		}
		msecStr = msecStr[:scale]
	} else if len(msecStr) < int(scale) {
		lengthMsecStr := len(msecStr)
		padZeros := int(scale) - lengthMsecStr
		msecStr = msecStr + FillString[padZeros]
	}
	if len(msecStr) == 0 { // this means the scale is 0
		return 0, msecCarry, nil
	}
	m, err := strconv.ParseUint(msecStr, 10, 32)
	if err != nil {
		return 0, 0, moerr.NewInvalidArgNoCtx("get ms", msecStr)
	}
	msecs = (uint32(m) + msecCarry) * scaleTable[scale]
	if msecs == OneSecInMicroSeconds {
		carry = 1
		msecs = 0
	}
	return msecs, carry, nil
}

// ParseTimestamp will parse a string to be a Timestamp
// Support Format:
// 1. all the Date value
// 2. yyyy-mm-dd hh:mm:ss(.msec)
// 3. yyyymmddhhmmss(.msec)
func ParseTimestamp(loc *time.Location, s string, scale int32) (Timestamp, error) {
	dt, err := ParseDatetime(s, scale)
	if err != nil {
		return -1, moerr.NewInvalidArgNoCtx("parse timestamp", s)
	}

	result := dt.ToTimestamp(loc)
	//for issue5305, do not do this check
	//according to mysql, timestamp function actually return a datetime value
	/*
		if result < TimestampMinValue {
			return -1, moerr.NewInvalidArgNoCtx("parse timestamp", s)
		}
	*/

	return result, nil
}

type unsafeLoc struct {
	name string
	zone []struct {
		name   string
		offset int
		isDST  bool
	}
	tx []struct {
		when         int64
		index        uint8
		isstd, isutc bool
	}
	extend string
}

func TimestampToDatetime(loc *time.Location, xs []Timestamp, rs []Datetime) ([]Datetime, error) {
	xsInInt64 := *(*[]int64)(unsafe.Pointer(&xs))
	rsInInt64 := *(*[]int64)(unsafe.Pointer(&rs))

	locPtr := (*unsafeLoc)(unsafe.Pointer(loc))
	if len(locPtr.zone) == 1 {
		offset := int64(locPtr.zone[0].offset) * microSecsPerSec
		for i, x := range xsInInt64 {
			rsInInt64[i] = x + offset
		}
	} else {
		for i, x := range xsInInt64 {
			t := time.UnixMicro(x - unixEpochMicroSecs).In(loc)
			_, offset := t.Zone()
			rsInInt64[i] = x + int64(offset)*microSecsPerSec
		}
	}
	return rs, nil
}

func (ts Timestamp) ToDatetime(loc *time.Location) Datetime {
	t := time.UnixMicro(int64(ts) - unixEpochMicroSecs).In(loc)
	_, offset := t.Zone()
	return Datetime(ts) + Datetime(offset)*microSecsPerSec
}

// FromClockUTC gets the utc time value in Timestamp
func FromClockUTC(year int32, month, day, hour, minute, sec uint8, msec uint32) Timestamp {
	days := DateFromCalendar(year, month, day)
	secs := int64(days)*secsPerDay + int64(hour)*secsPerHour + int64(minute)*secsPerMinute + int64(sec)
	return Timestamp(secs*microSecsPerSec + int64(msec))
}

// FromClockZone gets the local time value in Timestamp
func FromClockZone(loc *time.Location, year int32, month, day, hour, minute, sec uint8, msec uint32) Timestamp {
	t := time.Date(int(year), time.Month(month), int(day), int(hour), int(minute), int(sec), int(msec*1000), loc)
	return Timestamp(t.UnixMicro() + unixEpochMicroSecs)
}

func CurrentTimestamp() Timestamp {
	return Timestamp(time.Now().UnixMicro() + unixEpochMicroSecs)
}

func ValidTimestamp(timestamp Timestamp) bool {
	return timestamp > TimestampMinValue
}

func UnixToTimestamp(ts int64) Timestamp {
	return Timestamp(ts*microSecsPerSec + unixEpochMicroSecs)
}

func UnixMicroToTimestamp(ts int64) Timestamp {
	return Timestamp(ts + unixEpochMicroSecs)
}
func UnixNanoToTimestamp(ts int64) Timestamp {
	return Timestamp(ts/nanoSecsPerMicroSec + unixEpochMicroSecs)
}
