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
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	// Time range is [-2562047787:59:59.999999,2562047787:59:59.999999]
	// This is the max hour that int64 with solution msec can present
	// (9223372036854775807(max int64)/1000000(msec) - 1)/3600(sec per hour) - 1 = 2562047787
	MinHourInTime, MaxHourInTime     = 0, 2562047787
	MinInputIntTime, MaxInputIntTime = -25620477875959, 25620477875959
)

// no msec part
// Format: hh:mm:ss
func (t Time) String() string {
	h, m, s, _, isNeg := t.ClockFormat()
	if isNeg {
		return fmt.Sprintf("-%02d:%02d:%02d", h, m, s)
	}
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
}

// Format: hh:mm:ss.msec
func (t Time) String2(scale int32) string {
	var symbol string
	h, m, s, ms, isNeg := t.ClockFormat()
	if isNeg {
		symbol = "-"
	}
	if scale > 0 {
		msecInstr := fmt.Sprintf("%06d\n", ms)
		msecInstr = msecInstr[:scale]
		return fmt.Sprintf("%s%02d:%02d:%02d"+"."+msecInstr, symbol, h, m, s)
	}
	return fmt.Sprintf("%s%02d:%02d:%02d", symbol, h, m, s)
}

// Format: hhmmss.msec
// TODO: add the carry when truncate
func (t Time) NumericString(scale int32) string {
	var symbol string
	h, m, s, ms, isNeg := t.ClockFormat()
	if isNeg {
		symbol = "-"
	}
	if scale > 0 {
		msecInstr := fmt.Sprintf("%06d\n", ms)
		msecInstr = msecInstr[:scale]
		return fmt.Sprintf("%s%02d%02d%02d"+"."+msecInstr, symbol, h, m, s)
	}
	return fmt.Sprintf("%s%02d%02d%02d", symbol, h, m, s)
}

// The Time type holds number of microseconds for hh:mm:ss(.msec)

// ParseTime will parse a string to a Time type
// Support Format:
// * yyyy-mm-dd hh:mm:ss(.msec)
// * (-)hh:mm:ss(.msec)
// * (-)hh:mm and
// * (-)hhmmss(.msec)

// During parsing, if the input length of msec is larger than predefined
// scale, it will be rounded
// eg.
//
//	Time(3) input string   		parsing result
//	"11:11:11.1234"			"11:11:11.123"
//	"11:11:11.1235"			"11:11:11.124"
//	"11:11:11.9994"      		"11:11:11.999"
//	"11:11:11.9995"      		"11:11:12.000"
//	"-11:11:11.1235"		"-11:11:11.124"
//	"-11:11:11.9995"      		"-11:11:12.000"
func ParseTime(s string, scale int32) (Time, error) {
	s = strings.TrimSpace(s)

	// seperate to date&time and msec parts
	strs := strings.Split(s, ".")
	timeString := strs[0]
	isNegative := false
	day := uint64(0)

	// handle date&time part
	// If the input string have date, make sure it is valid.
	if isDateType(timeString) {
		// The date type format must be "yyyy-mm-dd hh:mm:ss" and
		// it can be handled like Datetime
		dt, err := ParseDatetime(s, scale)
		if err != nil {
			return -1, moerr.NewInvalidInputNoCtx("invalid time value %s", s)
		}
		return dt.ToTime(scale), nil
	} else {
		// empty string "" equals to 00:00:00
		if len(timeString) == 0 {
			return Time(0), nil
		}

		if s[0] == '-' {
			isNegative = true
			timeString = timeString[1:]
		}

		timeParts := strings.Split(s, " ")
		if len(timeParts) > 1 {
			day, _ = strconv.ParseUint(timeParts[0], 10, 64)
			if day > MaxHourInTime/maxHourInDay {
				return -1, moerr.NewInvalidInputNoCtx("invalid time value %s", s)
			}
			timeString = timeParts[1]
		}
	}

	// handle time part
	var hour, minute, sec uint64
	var msec uint32 = 0
	var carry uint32 = 0
	var err error
	timeArr := strings.Split(timeString, ":")
	switch len(timeArr) {
	case 1: // s/ss/mss/mmss/hmmss/hhmmss/...hhhmmss
		l := len(timeArr[0])
		// The max length of the input is hhhhhhhhhhmmss
		// Because the max hour and int64 with solution
		// msec can present is 2562047787
		if l > 14 {
			return -1, moerr.NewInvalidInputNoCtx("invalid time value %s", s)
		}

		parsingString := timeArr[0]
		if l <= 2 {
			// l <= 2: s/ss
			if sec, err = strconv.ParseUint(parsingString[0:l], 10, 8); err != nil {
				return -1, moerr.NewInvalidInputNoCtx("invalid time value %s", s)
			}
		} else if l <= 4 {
			// 2 < l <= 4: mss/mmss
			// m is the length of minute part
			minuLen := l - 2
			if minute, err = strconv.ParseUint(parsingString[0:minuLen], 10, 8); err != nil {
				return -1, moerr.NewInvalidInputNoCtx("invalid time value %s", s)
			}
			if sec, err = strconv.ParseUint(parsingString[minuLen:l], 10, 8); err != nil {
				return -1, moerr.NewInvalidInputNoCtx("invalid time value %s", s)
			}
		} else {
			// l > 4: hh...hhmmss
			// hourLen is the length of hour part
			hourLen := l - 4
			if hour, err = strconv.ParseUint(parsingString[0:hourLen], 10, 64); err != nil {
				return -1, moerr.NewInvalidInputNoCtx("invalid time value %s", s)
			}
			if minute, err = strconv.ParseUint(parsingString[hourLen:hourLen+2], 10, 8); err != nil {
				return -1, moerr.NewInvalidInputNoCtx("invalid time value %s", s)
			}
			if sec, err = strconv.ParseUint(parsingString[hourLen+2:l], 10, 8); err != nil {
				return -1, moerr.NewInvalidInputNoCtx("invalid time value %s", s)
			}
		}
	case 2: // h:mm / hh:mm / hh...hh:mm
		if hour, err = strconv.ParseUint(timeArr[0], 10, 64); err != nil {
			return -1, moerr.NewInvalidInputNoCtx("invalid time value %s", s)
		}
		if minute, err = strconv.ParseUint(timeArr[1], 10, 8); err != nil {
			return -1, moerr.NewInvalidInputNoCtx("invalid time value %s", s)
		}
		sec = 0
	case 3: // h:mm:ss / hh:mm:ss / hh...hh:mm:ss
		if hour, err = strconv.ParseUint(timeArr[0], 10, 64); err != nil {
			return -1, moerr.NewInvalidInputNoCtx("invalid time value %s", s)
		}
		if minute, err = strconv.ParseUint(timeArr[1], 10, 8); err != nil {
			return -1, moerr.NewInvalidInputNoCtx("invalid time value %s", s)
		}
		if sec, err = strconv.ParseUint(timeArr[2], 10, 8); err != nil {
			return -1, moerr.NewInvalidInputNoCtx("invalid time value %s", s)
		}
	default:
		return -1, moerr.NewInvalidInputNoCtx("invalid time value %s", s)
	}

	if !ValidTime(hour, minute, sec) {
		return -1, moerr.NewInvalidInputNoCtx("invalid time value %s", s)
	}

	// handle msec part
	if len(strs) > 1 {
		msec, carry, err = getMsec(strs[1], scale)
		if err != nil {
			return -1, moerr.NewInvalidInputNoCtx("invalid time value %s", s)
		}
	}

	if day > MaxHourInTime/maxHourInDay-hour {
		return -1, moerr.NewInvalidInputNoCtx("invalid time value %s", s)
	}

	return TimeFromClock(isNegative, hour+day*24, uint8(minute), uint8(sec+uint64(carry)), msec), nil
}

// Numeric 112233/112233.4444 should be treate like string and then
// cast into time type
// The integrity numeric is int64 while numeric with decimal is Decimal128
// The number befre the decimal point is the hh:mm:ss part while
// decimal part is the msec.
// eg.
//
//	111111112233 -> "111111112233" -> "11111111:22:33"
//	112233 -> "112233" -> "11:22:33"
//	123 -> "000123" -> "00:01:23"
//	112233.444 -> "112233.444" -> "11:22:33.444"

func ParseInt64ToTime(input int64, scale int32) (Time, error) {
	if input < MinInputIntTime || input > MaxInputIntTime {
		return -1, moerr.NewInvalidInputNoCtx("invalid time value %d", input)
	}
	s := strconv.FormatInt(input, 10)
	return ParseTime(s, scale)
}

func ParseDecimal64ToTime(input Decimal64, scale1, scale2 int32) (Time, error) {
	input, _ = input.Scale(scale2 - scale1)
	s := input.Format(scale2)
	return ParseTime(s, scale2)
}

func ParseDecimal128ToTime(input Decimal128, scale1, scale2 int32) (Time, error) {
	input, _ = input.Scale(scale2 - scale1)
	s := input.Format(scale2)
	return ParseTime(s, scale2)
}

func (t Time) ToInt64() int64 {
	h, m, s, _, isNeg := t.ClockFormat()
	trans := int64(h*10000) + int64(m)*100 + int64(s)
	if isNeg {
		trans = -trans
	}

	return trans
}

func (t Time) ToDecimal64(ctx context.Context, width, scale int32) (Decimal64, error) {
	tToStr := t.NumericString(scale)
	ret, err := ParseDecimal64(tToStr, width, scale)
	if err != nil {
		return ret, moerr.NewInternalError(ctx, "exsit time cant't cast to decimal64")
	}

	return ret, nil
}

func (t Time) ToDecimal128(ctx context.Context, width, scale int32) (Decimal128, error) {
	tToStr := t.NumericString(scale)
	ret, err := ParseDecimal128(tToStr, width, scale)
	if err != nil {
		return ret, moerr.NewInternalError(ctx, "exsit time cant't cast to decimal128")
	}

	return ret, nil
}

func TimeFromClock(isNegative bool, hour uint64, minute, sec uint8, msec uint32) Time {
	secs := int64(hour)*secsPerHour + int64(minute)*secsPerMinute + int64(sec)
	t := secs*microSecsPerSec + int64(msec)
	if isNegative {
		return Time(-t)
	}
	return Time(t)
}

// ClockFormat: symbol part/hour part/minute part/second part/msecond part
func (t Time) ClockFormat() (hour uint64, minute, sec uint8, msec uint64, isNeg bool) {
	if t < 0 {
		isNeg = true
		t = -t
	}
	ts := t.sec()
	h := uint64(ts / secsPerHour)
	m := uint8(ts % secsPerHour / secsPerMinute)
	s := uint8(ts % secsPerMinute)
	ms := uint64(t % microSecsPerSec)

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

func (t Time) Hour() int64 {
	h := (t.sec()) / secsPerHour
	return h
}

// TODO: Get Today date from local time zone setting?
func (t Time) ToDate() Date {
	return Today(time.UTC)
}

// We need to truncate the part after scale position when cast
// between different scale.
func (t Time) ToDatetime(scale int32) Datetime {
	// TODO: Get today date from local time zone setting?
	d := Today(time.UTC)
	dt := d.ToDatetime()
	if scale == 6 {
		return Datetime(int64(dt) + int64(t))
	}

	// TODO: add the valid check
	newTime := Datetime(int64(dt) + int64(t))
	base := newTime / scaleVal[scale]
	if newTime%scaleVal[scale]/scaleVal[scale+1] >= 5 { // check carry
		base += 1
	}
	return base * scaleVal[scale]
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
	if !ValidTime(uint64(h), 0, 0) {
		return 0, false
	}
	return newTime, true
}

func (t Time) ConvertToInterval(ctx context.Context, its string) (int64, error) {
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
	return 0, moerr.NewInvalidInput(ctx, "invalid time input")
}

func (t Time) sec() int64 {
	return int64(t) / microSecsPerSec
}

func ValidTime(h, m, s uint64) bool {
	if h < MinHourInTime || h > MaxHourInTime {
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

func isDateType(s string) bool {
	strArr := strings.Split(s, " ")
	if len(strArr) > 1 {
		if _, err := strconv.ParseUint(strArr[0], 10, 64); err != nil {
			return true
		}
	}

	return false
}
