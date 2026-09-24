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
	invalidDatetimeDayMicros             = int64(SecsPerDay) * MicroSecsPerSec
	// Valid DATETIME values, including dates before the epoch, are well above
	// this range. Keeping invalid calendar values below MinInt64 plus their
	// packed payload makes the tag unambiguous for arbitrary Datetime values.
	invalidDatetimeEncodingBase = int64(math.MinInt64)
)

var (
	scaleVal = []Datetime{1000000, 100000, 10000, 1000, 100, 10, 1}
)

const (
	// DatetimeEpoch is the internal epoch for 0001-01-01 00:00:00.
	DatetimeEpoch = Datetime(0)
	// ZeroDatetime represents MySQL's 0000-00-00 00:00:00 value.
	ZeroDatetime = Datetime(-1)
)

// The Datetime type holds number of microseconds since January 1, year 1 in Gregorian calendar

func (dt Datetime) String() string {
	if dt == ZeroDatetime {
		return "0000-00-00 00:00:00"
	}
	y, m, d, _ := dt.ToDate().Calendar(true)
	hour, minute, sec := dt.Clock()
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", y, m, d, hour, minute, sec)
}

func (dt Datetime) String2(scale int32) string {
	if dt == ZeroDatetime {
		if scale > 0 {
			return "0000-00-00 00:00:00." + strings.Repeat("0", int(scale))
		}
		return "0000-00-00 00:00:00"
	}
	y, m, d, _ := dt.ToDate().Calendar(true)
	hour, minute, sec := dt.Clock()

	if scale > 0 {
		msec := dt.MicroSec()
		// Format microseconds as 6 digits (max precision we store)
		msecInstr := fmt.Sprintf("%06d", msec)
		// For scale > 6, pad with zeros to the right (e.g., scale 9: "000001" -> "000001000")
		if scale > 6 {
			// Pad to 9 digits by appending zeros
			for len(msecInstr) < 9 {
				msecInstr = msecInstr + "0"
			}
			// Truncate to requested scale (max 9)
			if scale > 9 {
				scale = 9
			}
			msecInstr = msecInstr[:scale]
		} else {
			// For scale <= 6, truncate from the right
			msecInstr = msecInstr[:scale]
		}

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
	return parseDatetime(s, scale, false)
}

// ParseDatetimeWithInvalidDates preserves in-range calendar fields under
// ALLOW_INVALID_DATES while keeping time-field validation unchanged.
func ParseDatetimeWithInvalidDates(s string, scale int32) (Datetime, error) {
	return parseDatetime(s, scale, true)
}

func parseDatetime(s string, scale int32, allowInvalidDates bool) (Datetime, error) {
	s = strings.TrimSpace(s)
	if isZeroDatetimeString(s) {
		return ZeroDatetime, nil
	}
	if len(s) < 14 {
		var d Date
		var err error
		if allowInvalidDates {
			d, err = ParseDateCastWithInvalidDates(s)
		} else {
			d, err = ParseDateCast(s)
		}
		if err == nil {
			return d.ToDatetime(), nil
		}
		return -1, moerr.NewInvalidInputNoCtxf("invalid datetime value %s", s)
	}
	var year int32
	var month, day, hour, minute, second uint8
	var msec uint32 = 0
	var carry uint32 = 0
	var err error

	if s[4] == '-' || s[4] == '/' || s[4] == ':' {
		var unum uint64
		dateSep := s[4]

		// Fast path: standard zero-padded format "yyyy-mm-dd hh:mm:ss[.f...]"
		// or ISO 8601 "yyyy-mm-ddThh:mm:ss[.f...]" with fixed-width fields.
		// Separators at known positions; no slice allocations needed.
		if len(s) >= 19 && s[7] == dateSep && (s[10] == ' ' || s[10] == 'T') &&
			s[13] == ':' && s[16] == ':' && (len(s) == 19 || s[19] == '.') {
			var num int64
			num, err = strconv.ParseInt(s[0:4], 10, 32)
			if err != nil {
				return -1, moerr.NewInvalidInputNoCtxf("invalid datetime value %s", s)
			}
			year = int32(num)
			unum, err = strconv.ParseUint(s[5:7], 10, 8)
			if err != nil {
				return -1, moerr.NewInvalidInputNoCtxf("invalid datetime value %s", s)
			}
			month = uint8(unum)
			unum, err = strconv.ParseUint(s[8:10], 10, 8)
			if err != nil {
				return -1, moerr.NewInvalidInputNoCtxf("invalid datetime value %s", s)
			}
			day = uint8(unum)
			if !dateFieldsValidForParse(year, month, day, allowInvalidDates) {
				return -1, moerr.NewInvalidInputNoCtxf("invalid datetime value %s", s)
			}
			unum, err = strconv.ParseUint(s[11:13], 10, 8)
			if err != nil {
				return -1, moerr.NewInvalidInputNoCtxf("invalid datetime value %s", s)
			}
			hour = uint8(unum)
			unum, err = strconv.ParseUint(s[14:16], 10, 8)
			if err != nil {
				return -1, moerr.NewInvalidInputNoCtxf("invalid datetime value %s", s)
			}
			minute = uint8(unum)
			unum, err = strconv.ParseUint(s[17:19], 10, 8)
			if err != nil {
				return -1, moerr.NewInvalidInputNoCtxf("invalid datetime value %s", s)
			}
			second = uint8(unum)
			if !ValidTimeInDay(hour, minute, second) {
				return -1, moerr.NewInvalidInputNoCtxf("invalid datetime value %s", s)
			}
			if len(s) == 19 {
				// nothing
			} else if s[19] == '.' {
				msec, carry, err = getMsec(s[20:], scale)
				if err != nil {
					return -1, moerr.NewInvalidInputNoCtxf("invalid datetime value %s", s)
				}
			} else {
				return -1, moerr.NewInvalidInputNoCtxf("invalid datetime value %s", s)
			}
		} else {
			// Slow path: variable-width fields. Use IndexByte instead of
			// strings.Split to avoid []string allocations.
			dtSepIdx := strings.IndexByte(s, ' ')
			if dtSepIdx < 0 {
				dtSepIdx = strings.IndexByte(s, 'T')
			}
			if dtSepIdx < 0 || dtSepIdx == len(s)-1 {
				return -1, moerr.NewInvalidInputNoCtxf("invalid datetime value %s", s)
			}
			dateStr := s[:dtSepIdx]
			timeStr := s[dtSepIdx+1:]

			// Parse date: find second occurrence of dateSep
			p2 := 5 + strings.IndexByte(dateStr[5:], dateSep)
			if p2 < 5 || p2 >= len(dateStr)-1 {
				return -1, moerr.NewInvalidInputNoCtxf("invalid datetime value %s", s)
			}
			var num int64
			num, err = strconv.ParseInt(dateStr[:4], 10, 32)
			if err != nil {
				return -1, moerr.NewInvalidInputNoCtxf("invalid datetime value %s", s)
			}
			year = int32(num)
			unum, err = strconv.ParseUint(dateStr[5:p2], 10, 8)
			if err != nil {
				return -1, moerr.NewInvalidInputNoCtxf("invalid datetime value %s", s)
			}
			month = uint8(unum)
			unum, err = strconv.ParseUint(dateStr[p2+1:], 10, 8)
			if err != nil {
				return -1, moerr.NewInvalidInputNoCtxf("invalid datetime value %s", s)
			}
			day = uint8(unum)
			if !dateFieldsValidForParse(year, month, day, allowInvalidDates) {
				return -1, moerr.NewInvalidInputNoCtxf("invalid datetime value %s", s)
			}

			// Parse time: split off microseconds
			dotIdx := strings.IndexByte(timeStr, '.')
			hmsStr := timeStr
			if dotIdx >= 0 {
				hmsStr = timeStr[:dotIdx]
			}
			c1 := strings.IndexByte(hmsStr, ':')
			if c1 < 0 {
				return -1, moerr.NewInvalidInputNoCtxf("invalid datetime value %s", s)
			}
			c2 := c1 + 1 + strings.IndexByte(hmsStr[c1+1:], ':')
			var secStr string
			if c2 <= c1 {
				// only h:mm — treat seconds as "00"
				if len(hmsStr)-c1-1 == 0 {
					return -1, moerr.NewInvalidInputNoCtxf("invalid datetime value %s", s)
				}
				secStr = "00"
			} else {
				secStr = hmsStr[c2+1:]
				if secStr == "" {
					secStr = "00"
				}
			}
			unum, err = strconv.ParseUint(hmsStr[:c1], 10, 8)
			if err != nil {
				return -1, moerr.NewInvalidInputNoCtxf("invalid datetime value %s", s)
			}
			hour = uint8(unum)
			end := c2
			if c2 <= c1 {
				end = len(hmsStr)
			}
			unum, err = strconv.ParseUint(hmsStr[c1+1:end], 10, 8)
			if err != nil {
				return -1, moerr.NewInvalidInputNoCtxf("invalid datetime value %s", s)
			}
			minute = uint8(unum)
			unum, err = strconv.ParseUint(secStr, 10, 8)
			if err != nil {
				return -1, moerr.NewInvalidInputNoCtxf("invalid datetime value %s", s)
			}
			second = uint8(unum)
			if !ValidTimeInDay(hour, minute, second) {
				return -1, moerr.NewInvalidInputNoCtxf("invalid datetime value %s", s)
			}
			if dotIdx >= 0 {
				if dotIdx == len(timeStr)-1 {
					return -1, moerr.NewInvalidInputNoCtxf("invalid datetime value %s", s)
				}
				msec, carry, err = getMsec(timeStr[dotIdx+1:], scale)
				if err != nil {
					return -1, moerr.NewInvalidInputNoCtxf("invalid datetime value %s", s)
				}
			}
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
					return -1, moerr.NewInvalidInputNoCtxf("invalid datetime value %s", s)
				}
			} else {
				return -1, moerr.NewInvalidInputNoCtxf("invalid datetime value %s", s)
			}
		}
	}
	if !dateFieldsValidForParse(year, month, day, allowInvalidDates) {
		return -1, moerr.NewInvalidInputNoCtxf("invalid datetime value %s", s)
	}
	if carry != 0 {
		second++
		if second == 60 {
			second = 0
			minute++
			if minute == 60 {
				minute = 0
				hour++
				if hour == 24 {
					hour = 0
					year, month, day = nextDateFieldsAfterRounding(year, month, day, allowInvalidDates)
				}
			}
		}
	}
	if !dateFieldsValidForParse(year, month, day, allowInvalidDates) {
		return -1, moerr.NewInvalidInputNoCtxf("invalid datetime value %s", s)
	}
	result := DatetimeFromClockAllowInvalid(year, month, day, hour, minute, second, msec)
	y, m, d, _ := result.ToDate().Calendar(true)
	if !allowInvalidDates && !ValidDate(y, m, d) {
		return -1, moerr.NewInvalidInputNoCtxf("invalid datetime value %s", s)
	}
	return result, nil
}

func nextDateFieldsAfterRounding(year int32, month, day uint8, allowInvalidDates bool) (int32, uint8, uint8) {
	if allowInvalidDates && !ValidDate(year, month, day) {
		if day < 31 {
			return year, month, day + 1
		}
		if month < MaxMonthInYear {
			return year, month + 1, 1
		}
		return year + 1, MinMonthInYear, 1
	}
	next := DateFromCalendar(year, month, day) + 1
	nextYear, nextMonth, nextDay, _ := next.Calendar(true)
	return nextYear, nextMonth, nextDay
}

func dateFieldsValidForParse(year int32, month, day uint8, allowInvalidDates bool) bool {
	if allowInvalidDates {
		return year >= MinDatetimeYear && year <= MaxDatetimeYear &&
			month >= MinMonthInYear && month <= MaxMonthInYear && day > 0 && day <= 31
	}
	return ValidDate(year, month, day)
}

func isZeroDatetimeString(s string) bool {
	base := s
	if dot := strings.IndexByte(s, '.'); dot >= 0 {
		base = s[:dot]
		fraction := s[dot+1:]
		if fraction == "" || !isAllDigit(fraction) {
			return false
		}
		for i := range fraction {
			if fraction[i] != '0' {
				return false
			}
		}
	}
	switch base {
	case "0000-00-00", "00000000", "0000-00-00 00:00:00", "0000-00-00T00:00:00", "00000000000000":
		return true
	default:
		return false
	}
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
	if dt == ZeroDatetime {
		return -1
	}
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
	if dt == ZeroDatetime {
		return ZeroDate
	}
	if isEncodedInvalidDatetime(dt) {
		year, month, day, _, _, _, _ := decodeInvalidDatetime(dt)
		return DateFromCalendarAllowInvalid(year, month, day)
	}
	return Date(dt.sec() / SecsPerDay)
}

// We need to truncate the part after scale position when cast
// between different scale.
func (dt Datetime) ToTime(scale int32) Time {
	// Get today's date (the base date used when converting TIME to DATETIME)
	todayDate := Today(time.UTC)
	todayDatetime := todayDate.ToDatetime()

	// Get the date portion of the datetime
	datePart := dt.ToDate()
	baseDatetime := datePart.ToDatetime()

	// Calculate time difference from the base date
	// If the datetime's date is today or tomorrow (within 1 day), use today as base
	// This preserves times > 24 hours that came from ADDTIME with TIME inputs
	var timeDiff int64
	dateDiff := int64(datePart) - int64(todayDate)
	if dateDiff >= 0 && dateDiff <= 1 {
		// Date is today or tomorrow, calculate from today's date
		timeDiff = int64(dt) - int64(todayDatetime)
	} else {
		// Date is far from today, use the date portion of the datetime
		timeDiff = int64(dt) - int64(baseDatetime)
	}

	// Time type only supports up to 6 digits of microsecond precision
	// For scale > 6, use scale 6 (full microsecond precision)
	if scale >= 6 {
		return Time(timeDiff)
	}

	// truncate the time part
	scaleValInt := int64(scaleVal[scale])
	base := timeDiff / scaleValInt
	if scale < 6 && timeDiff%scaleValInt/int64(scaleVal[scale+1]) >= 5 { // check carry
		base += 1
	}

	return Time(base * scaleValInt)
}

// TruncateToScale truncates a datetime to the given scale (0-6).
// Scale represents fractional seconds precision:
//   - 0: seconds (no fractional part)
//   - 1-5: fractional seconds with corresponding precision
//   - 6: microseconds (full precision, no truncation)
//   - >6: treated as scale 6 (full precision)
func (dt Datetime) TruncateToScale(scale int32) Datetime {
	if dt == ZeroDatetime {
		return ZeroDatetime
	}
	if isEncodedInvalidDatetime(dt) {
		year, month, day, hour, minute, second, micro := decodeInvalidDatetime(dt)
		if scale >= 6 {
			return dt
		}
		divisor := int64(scaleVal[scale])
		base := micro / divisor
		if micro%divisor/int64(scaleVal[scale+1]) >= 5 {
			base++
		}
		roundedMicro := base * divisor
		if roundedMicro >= MicroSecsPerSec {
			roundedMicro = 0
			second++
			if second == 60 {
				second = 0
				minute++
				if minute == 60 {
					minute = 0
					hour++
					if hour == 24 {
						hour = 0
						year, month, day = nextDateFieldsAfterRounding(year, month, day, true)
					}
				}
			}
		}
		return DatetimeFromClockAllowInvalid(year, month, day, hour, minute, second, uint32(roundedMicro))
	}
	// For scale >= 6, return full precision (no truncation)
	if scale >= 6 {
		return dt
	}

	// Get the date part (seconds since epoch)
	secPart := (dt / MicroSecsPerSec) * MicroSecsPerSec
	// Get the fractional part (microseconds within the second)
	microPart := dt % MicroSecsPerSec

	divisor := scaleVal[scale]
	base := microPart / divisor
	// Round up if the next digit >= 5
	if scale < 6 && microPart%divisor/scaleVal[scale+1] >= 5 {
		base += 1
	}

	return secPart + base*divisor
}

func (dt Datetime) Clock() (hour, minute, sec int8) {
	if dt == ZeroDatetime {
		return 0, 0, 0
	}
	if isEncodedInvalidDatetime(dt) {
		_, _, _, h, m, s, _ := decodeInvalidDatetime(dt)
		return int8(h), int8(m), int8(s)
	}
	t := dt.sec() % SecsPerDay
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

func isEncodedInvalidDatetime(dt Datetime) bool {
	maxPayload := int64(MaxDatetimeYear*10000+12*100+31)*invalidDatetimeDayMicros + invalidDatetimeDayMicros - 1
	if int64(dt) < invalidDatetimeEncodingBase+1 || int64(dt) > invalidDatetimeEncodingBase+maxPayload {
		return false
	}
	year, month, day, hour, minute, sec, msec := decodeInvalidDatetime(dt)
	return year >= MinDatetimeYear && year <= MaxDatetimeYear &&
		month >= MinMonthInYear && month <= MaxMonthInYear &&
		day > 0 && day <= 31 && !ValidDate(year, month, day) &&
		hour <= maxHourInDay && minute <= maxMinuteInHour &&
		sec <= maxSecondInMinute && msec < MicroSecsPerSec
}

// IsTaggedInvalid reports whether dt uses the ALLOW_INVALID_DATES tagged
// representation. Storage codecs use this to select a format version that a
// pre-tag reader cannot silently reinterpret.
func (dt Datetime) IsTaggedInvalid() bool {
	return isEncodedInvalidDatetime(dt)
}

func encodeInvalidDatetime(year int32, month, day, hour, minute, sec uint8, msec uint32) Datetime {
	date := int64(year)*10000 + int64(month)*100 + int64(day)
	clock := (int64(hour)*SecsPerHour+int64(minute)*SecsPerMinute+int64(sec))*MicroSecsPerSec + int64(msec)
	return Datetime(invalidDatetimeEncodingBase + date*invalidDatetimeDayMicros + clock)
}

func decodeInvalidDatetime(dt Datetime) (year int32, month, day, hour, minute, sec uint8, msec int64) {
	packed := int64(dt) - invalidDatetimeEncodingBase
	date := packed / invalidDatetimeDayMicros
	clock := packed % invalidDatetimeDayMicros
	year = int32(date / 10000)
	month = uint8((date / 100) % 100)
	day = uint8(date % 100)
	hour = uint8(clock / (SecsPerHour * MicroSecsPerSec))
	clock %= SecsPerHour * MicroSecsPerSec
	minute = uint8(clock / (SecsPerMinute * MicroSecsPerSec))
	clock %= SecsPerMinute * MicroSecsPerSec
	sec = uint8(clock / MicroSecsPerSec)
	msec = clock % MicroSecsPerSec
	return
}

// DatetimeFromClockAllowInvalid uses the legacy representation for valid
// dates and a fixed-width packed representation for calendar-invalid dates.
func DatetimeFromClockAllowInvalid(year int32, month, day, hour, minute, sec uint8, msec uint32) Datetime {
	if ValidDate(year, month, day) {
		return DatetimeFromClock(year, month, day, hour, minute, sec, msec)
	}
	return encodeInvalidDatetime(year, month, day, hour, minute, sec, msec)
}

// DatetimeOrderKey is the unsigned, order-preserving calendar/time key used
// by tuple and index encodings. It is separate from Datetime's physical
// microsecond scalar so tagged invalid dates retain their SQL order.
func DatetimeOrderKey(dt Datetime) uint64 {
	if dt == ZeroDatetime {
		return 0
	}
	dateKey := DateOrderKey(dt.ToDate())
	clockKey := (uint64(dt.Hour())*SecsPerHour+
		uint64(dt.Minute())*SecsPerMinute+uint64(dt.Sec()))*MicroSecsPerSec +
		uint64(dt.MicroSec())
	return dateKey*uint64(microSecsPerDay) + clockKey
}

// DatetimeFromOrderKey reverses DatetimeOrderKey, including invalid calendar
// fields and fractional seconds.
func DatetimeFromOrderKey(key uint64) Datetime {
	if key == 0 {
		return ZeroDatetime
	}
	dateKey := key / uint64(microSecsPerDay)
	clockKey := key % uint64(microSecsPerDay)
	date := DateFromOrderKey(dateKey)
	year, month, day, _ := date.Calendar(true)
	hour := uint8(clockKey / (SecsPerHour * MicroSecsPerSec))
	clockKey %= SecsPerHour * MicroSecsPerSec
	minute := uint8(clockKey / (SecsPerMinute * MicroSecsPerSec))
	clockKey %= SecsPerMinute * MicroSecsPerSec
	second := uint8(clockKey / MicroSecsPerSec)
	micro := uint32(clockKey % MicroSecsPerSec)
	return DatetimeFromClockAllowInvalid(year, month, day, hour, minute, second, micro)
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
	hour, minute, second := dt.Clock()
	micro := dt.MicroSec()
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
	if isEncodedInvalidDatetime(dt) {
		return DatetimeFromClock(y, m, d, uint8(hour), uint8(minute), uint8(second), uint32(micro)), true
	}
	return dt + Datetime(newDate-oldDate)*SecsPerDay*MicroSecsPerSec, true
}

// AddInterval now date or datetime use the function to add/sub date,
// we need a bool arg to tell isDate/isDatetime
// date/datetime have different regions, so we don't use same valid function
// return type bool means the if the date/datetime is valid
func (dt Datetime) AddInterval(nums int64, its IntervalType, timeType TimeType) (Datetime, bool) {
	if isEncodedInvalidDatetime(dt) {
		year, month, day, hour, minute, second, micro := decodeInvalidDatetime(dt)
		normalized := DatetimeFromClock(year, month, day, hour, minute, second, uint32(micro))
		return normalized.AddInterval(nums, its, timeType)
	}
	var addMonth, addYear int64
	switch its {
	case MicroSecond:
		if (nums > 0 && int64(dt) > math.MaxInt64-nums) || (nums < 0 && int64(dt) < math.MinInt64-nums) {
			return 0, false
		}
		newDate := dt + Datetime(nums)
		// Datetime zero is 0001-01-01 00:00:00. Values below it can be
		// misread as an in-range calendar date after integer division truncates
		// toward zero, so reject the encoded domain before calendar conversion.
		if newDate < 0 {
			return 0, false
		}
		y, m, d, _ := newDate.ToDate().Calendar(true)
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
		return newDate, true
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
	case Year_Month:
		// Year_Month should be treated as Month (nums already represents months after NormalizeInterval)
		// This handles the case where Year_Month type is directly passed without NormalizeInterval conversion
		addMonth = nums
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
	first := normalizedDatetimeForArithmetic(dt)
	second := normalizedDatetimeForArithmetic(secondDt)
	switch its {
	case "microsecond":
		return int64(first - second), nil
	case "second":
		return (first - second).sec(), nil
	case "minute":
		return int64(first-second) / (MicroSecsPerSec * SecsPerMinute), nil
	case "hour":
		return int64(first-second) / (MicroSecsPerSec * SecsPerHour), nil
	case "day":
		return int64(first-second) / (MicroSecsPerSec * SecsPerDay), nil
	case "week":
		return int64(first-second) / (MicroSecsPerSec * SecsPerWeek), nil
	case "month":
		return first.ConvertToMonth(second), nil
	case "quarter":
		return first.ConvertToMonth(second) / 3, nil
	case "year":
		return first.ConvertToMonth(second) / 12, nil
	}
	return 0, moerr.NewInvalidInputNoCtx("invalid time_stamp_unit input")
}

func (dt Datetime) DatetimeMinusWithSecond(secondDt Datetime) int64 {
	return int64((normalizedDatetimeForArithmetic(dt) - normalizedDatetimeForArithmetic(secondDt)) / MicroSecsPerSec)
}

func normalizedDatetimeForArithmetic(dt Datetime) Datetime {
	if !isEncodedInvalidDatetime(dt) {
		return dt
	}
	year, month, day, hour, minute, second, micro := decodeInvalidDatetime(dt)
	return DatetimeFromClock(year, month, day, hour, minute, second, uint32(micro))
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
	if dt == ZeroDatetime {
		return 0
	}
	if isEncodedInvalidDatetime(dt) {
		_, _, _, _, _, _, msec := decodeInvalidDatetime(dt)
		return msec
	}
	return int64(dt) % MicroSecsPerSec
}

func (dt Datetime) sec() int64 {
	if isEncodedInvalidDatetime(dt) {
		year, month, day, hour, minute, second, _ := decodeInvalidDatetime(dt)
		date := DateFromCalendar(year, month, day)
		return int64(date)*SecsPerDay + int64(hour)*SecsPerHour + int64(minute)*SecsPerMinute + int64(second)
	}
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
	if dt == ZeroDatetime {
		return ZeroTimestamp
	}
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
