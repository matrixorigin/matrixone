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
	minHourInTime, maxHourInTime = 0, 838
)

var (
	FillString = []string{"", "0", "00", "000", "0000", "00000", "000000", "0000000"}
)

// no msec part
func (t Time) String() string {
	h, m, s, _, isNeg := t.ClockFormat()
	if isNeg {
		return fmt.Sprintf("-%02d:%02d:%02d", h, m, s)
	}
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
}

func (t Time) String2(precision int32) string {
	var symbol string
	h, m, s, ms, isNeg := t.ClockFormat()
	if isNeg {
		symbol = "-"
	}
	if precision > 0 {
		msecInstr := fmt.Sprintf("%06d\n", ms)
		msecInstr = msecInstr[:precision]
		return fmt.Sprintf("%s%02d:%02d:%02d"+"."+msecInstr, symbol, h, m, s)
	}
	return fmt.Sprintf("%s%02d:%02d:%02d", symbol, h, m, s)
}

// The Time type holds number of microseconds for hh:mm:ss(.msec)

// ParseTime will parse a string to a Time type
// Support Format:
// * yyyy-mm-dd hh:mm:ss(.msec)
// * yyyymmddhhmmss(.msec)
// * (-)hh:mm:ss(.msec) and (-)hhh:mm:ss(.msec)
// * (-)hhmmss(.msec) and (-)hhhmmss(.msec)
// * (-)hh:mm and (-)hhh:mm

// During parsing, if the input length of msec is larger than predefined
// precision, it will be rounded
// eg.
//
//	  Time(3) input string   	parsing result
//	"11:11:11.1234"			"11:11:11.123"
//	"11:11:11.1235"			"11:11:11.124"
//	"11:11:11.9994"      		"11:11:11.999"
//	"11:11:11.9995"      		"11:11:12.000"
//	"-11:11:11.1235"		"-11:11:11.124"
//	"-11:11:11.9995"      		"-11:11:12.000"
func ParseTime(s string, precision int32) (Time, error) {
	s = strings.TrimSpace(s)

	// seperate to date&time and msec parts
	strs := strings.Split(s, ".")
	timeString := strs[0]
	isNegative := false

	// handle date&time part
	// If the input string have date, make sure it is valid.
	if len(timeString) >= 14 {
		// The minest length of yyyy-mm-dd hh:mm:ss or yyyymmddhhmmss
		// must greater than 14 and it can be handled like Datetime
		dt, err := ParseDatetime(s, precision)
		if err != nil {
			return -1, moerr.NewInvalidInput("invalid time value %s", s)
		}
		return dt.ToTime(precision), nil
	} else {
		// if length is less than 14 it must be format:
		// (-)hh:mm:ss(.msec) / (-)hhh:mm:ss(.msec)
		// (-)hhmmss(.msec) / (-)hhhmmss(.msec)
		if s[0] == '-' {
			isNegative = true
			timeString = timeString[1:]
		}
	}

	// handle time part
	var hour, minute, sec uint64
	var msec uint32 = 0
	var carry uint32 = 0
	var err error
	timeArr := strings.Split(timeString, ":")
	switch len(timeArr) {
	case 1: // s/ss/mss/mmss/hmmss/hhmmss/hhhmmss
		if len(timeArr[0]) > 7 {
			return -1, moerr.NewInvalidInput("invalid time value %s", s)
		}
		// fill 0 to make sure its length is 7
		// eg.
		//	1 => 0000001 => (000)(00)(01) => 00:00:01
		//	123 => 0000123 => (000)(01)(23) => 00:01:23
		//	12345 => 0012345 => (001)(23)(45) => 01:23:45
		n := 7 - len(timeArr[0])
		parseString := FillString[n] + timeArr[0]
		if hour, err = strconv.ParseUint(parseString[0:3], 10, 32); err != nil {
			return -1, moerr.NewInvalidInput("invalid time value %s", s)
		}
		if minute, err = strconv.ParseUint(parseString[3:5], 10, 8); err != nil {
			return -1, moerr.NewInvalidInput("invalid time value %s", s)
		}
		if sec, err = strconv.ParseUint(parseString[5:7], 10, 8); err != nil {
			return -1, moerr.NewInvalidInput("invalid time value %s", s)
		}

	case 2: // hh:ss/ hhh:ss
		if hour, err = strconv.ParseUint(timeArr[0], 10, 32); err != nil {
			return -1, moerr.NewInvalidInput("invalid time value %s", s)
		}
		if minute, err = strconv.ParseUint(timeArr[1], 10, 8); err != nil {
			return -1, moerr.NewInvalidInput("invalid time value %s", s)
		}
		sec = 0
	case 3: // hh:mm:ss / hhh:mm:ss
		if hour, err = strconv.ParseUint(timeArr[0], 10, 32); err != nil {
			return -1, moerr.NewInvalidInput("invalid time value %s", s)
		}
		if minute, err = strconv.ParseUint(timeArr[1], 10, 8); err != nil {
			return -1, moerr.NewInvalidInput("invalid time value %s", s)
		}
		if sec, err = strconv.ParseUint(timeArr[2], 10, 8); err != nil {
			return -1, moerr.NewInvalidInput("invalid time value %s", s)
		}
	default:
		return -1, moerr.NewInvalidInput("invalid time value %s", s)
	}

	if !validTime(hour, minute, sec) {
		return -1, moerr.NewInvalidInput("invalid time value %s", s)
	}

	// handle msec part
	if len(strs) > 1 {
		msec, carry, err = getMsec(strs[1], precision)
		if err != nil {
			return -1, moerr.NewInvalidInput("invalid time value %s", s)
		}
	}

	return FromTimeClock(isNegative, int32(hour), uint8(minute), uint8(sec+uint64(carry)), msec), nil
}

func FromTimeClock(isNegative bool, hour int32, minute, sec uint8, msec uint32) Time {
	secs := int64(hour)*secsPerHour + int64(minute)*secsPerMinute + int64(sec)
	t := secs*microSecsPerSec + int64(msec)
	if isNegative {
		return Time(-t)
	}
	return Time(t)
}

// ClockFormat: symbol part/hour part/minute part/second part/msecond part
func (t Time) ClockFormat() (hour int32, minute, sec int8, msec int64, isNeg bool) {
	if t < 0 {
		isNeg = true
		t = -t
	}
	ts := t.sec()
	h := int32(ts / secsPerHour)
	m := int8(ts % secsPerHour / secsPerMinute)
	s := int8(ts % secsPerMinute)
	ms := int64(t % microSecsPerSec)

	return h, m, s, ms, isNeg
}

func (t Time) MicroSec() int64 {
	return int64(t) % microSecsPerSec
}

func (t Time) Sec() int8 {
	s := int8((t.sec()) % secsPerMinute)
	return s
}

func (t Time) Minute() int8 {
	m := int8((t.sec()) % secsPerHour / secsPerMinute)
	return m
}

func (t Time) Hour() int32 {
	h := int32((t.sec()) / secsPerHour)
	return h
}

// TODO: Get Today date from local time?
func (t Time) ToDate() Date {
	return Today(time.UTC)
}

// We need to truncate the part after precision position when cast
// between different precision.
func (t Time) ToDatetime(precision int32) Datetime {
	// TODO: Get today date from local time?
	d := Today(time.UTC)
	dt := d.ToDatetime()
	if precision == 6 {
		return Datetime(int64(dt) + int64(t))
	}

	newTime := Datetime(int64(dt) + int64(t))
	base := newTime / precisionVal[precision]
	if newTime%precisionVal[precision]/precisionVal[precision+1] >= 5 { // check carry
		base += 1
	}
	return base * precisionVal[precision]
}

// AddInterval now date or time use the function to add/sub date,
// return type bool means the if the time is valid
func (t Time) AddInterval(nums int64, its IntervalType) (Time, bool) {
	switch its {
	case Second:
		nums *= microSecsPerSec
	case Minute:
		nums *= microSecsPerSec * secsPerMinute
	case Hour:
		nums *= microSecsPerSec * secsPerHour
	}
	newTime := t + Time(nums)

	// valid
	h := newTime.Hour()
	if h < 0 {
		h = -h
	}
	if !validTime(uint64(h), 0, 0) {
		return 0, false
	}
	return newTime, true
}

func (t Time) ConvertToInterval(its string) (int64, error) {
	switch its {
	case "microsecond":
		return int64(t), nil
	case "second":
		return int64(t) / microSecsPerSec, nil
	case "minute":
		return int64(t) / (microSecsPerSec * secsPerMinute), nil
	case "hour":
		return int64(t) / (microSecsPerSec * secsPerHour), nil
	}
	return 0, moerr.NewInvalidInput("invalid time input")
}

func (t Time) sec() int64 {
	return int64(t) / microSecsPerSec
}

func validTime(h, m, s uint64) bool {
	if h < minHourInTime || h > maxHourInTime {
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
