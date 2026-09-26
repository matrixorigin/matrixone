// Copyright 2021 - 2022 Matrix Origin
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
	"math"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

/*
 * Interval in MySQL is not a real type, that is, it cannot be stored in
 * a table as column.   We still treat it as if it is a type so that we
 * can use it in some functions.
 */

type IntervalType int8

const IntervalNumMAX = int64(^uint64(0) >> 21)

const (
	IntervalTypeInvalid IntervalType = iota
	MicroSecond
	Second
	Minute
	Hour
	Day
	Week
	Month
	Quarter
	Year
	Second_MicroSecond
	Minute_MicroSecond
	Minute_Second
	Hour_MicroSecond
	Hour_Second
	Hour_Minute
	Day_MicroSecond
	Day_Second
	Day_Minute
	Day_Hour
	Year_Month
	IntervalTypeMax
)

func (it IntervalType) String() string {
	switch it {
	case MicroSecond:
		return "MICROSECOND"
	case Second:
		return "SECOND"
	case Minute:
		return "MINUTE"
	case Hour:
		return "HOUR"
	case Day:
		return "DAY"
	case Week:
		return "WEEK"
	case Month:
		return "MONTH"
	case Quarter:
		return "QUARTER"
	case Year:
		return "YEAR"
	case Second_MicroSecond:
		return "SECOND_MICROSECOND"
	case Minute_MicroSecond:
		return "MINUTE_MICROSECOND"
	case Minute_Second:
		return "MINUTE_SECOND"
	case Hour_MicroSecond:
		return "HOUR_MICROSECOND"
	case Hour_Second:
		return "HOUR_SECOND"
	case Hour_Minute:
		return "HOUR_MINUTE"
	case Day_MicroSecond:
		return "DAY_MICROSECOND"
	case Day_Second:
		return "DAY_SECOND"
	case Day_Minute:
		return "DAY_MINUTE"
	case Day_Hour:
		return "DAY_HOUR"
	case Year_Month:
		return "YEAR_MONTH"
	}
	return "INVALID_INTERVAL_TYPE"
}

func IntervalTypeOf(s string) (IntervalType, error) {
	for i := 1; i < int(IntervalTypeMax); i++ {
		if IntervalType(i).String() == strings.ToUpper(s) {
			return IntervalType(i), nil
		}
	}
	return IntervalTypeMax, moerr.NewInvalidInputNoCtxf("invalid interval type '%s'", s)
}

type intervalNumberField struct{ start, end int }

// Interval fields have at most five positions, plus one fractional field.
// Keep spans rather than accumulating every token into int64: a long final
// fraction is valid even when its decimal digits cannot fit into an integer.
func splitIntervalNumberFields(s string, maxFields int) ([6]intervalNumberField, int, error) {
	var fields [6]intervalNumberField
	count := 0
	inDigits := false
	for i := 0; i < len(s); i++ {
		if s[i] >= '0' && s[i] <= '9' {
			if !inDigits {
				if count == maxFields {
					return fields, 0, moerr.NewInvalidInputNoCtxf("invalid time interval value '%s'", s)
				}
				fields[count].start = i
				count++
				inDigits = true
			}
			fields[count-1].end = i + 1
		} else {
			inDigits = false
		}
	}
	return fields, count, nil
}

func intervalFieldValue(s string, field intervalNumberField) (int64, error) {
	value, err := strconv.ParseInt(s[field.start:field.end], 10, 64)
	if err != nil {
		return 0, moerr.NewInvalidInputNoCtxf("invalid time interval value '%s'", s)
	}
	return value, nil
}

// Multiply a decimal fraction by the final microsecond unit using long
// multiplication. The carry after the first digit is the integer part; its
// remainder is the first discarded decimal digit for half-away rounding.
func scaledIntervalFraction(s string, field intervalNumberField, multiplier int64) int64 {
	var carry, remainder int64
	for i := field.end - 1; i >= field.start; i-- {
		product := int64(s[i]-'0')*multiplier + carry
		carry, remainder = product/10, product%10
	}
	if remainder >= 5 {
		carry++
	}
	return carry
}

func conv(a []int64, mul []int64, rt IntervalType) (int64, IntervalType, error) {
	if len(a) != len(mul) {
		return 0, IntervalTypeInvalid, moerr.NewInternalErrorNoCtx("conv intervaltype has jagged array input")
	}

	var largerThanZero bool
	for _, num := range a {
		if num > 0 || num < 0 {
			largerThanZero = num > 0
		}
	}
	var ret int64
	var curMul int64 = 1

	for i := len(a) - 1; i >= 0; i-- {
		if mul[i] <= 0 || curMul > math.MaxInt64/mul[i] {
			return 0, IntervalTypeInvalid, moerr.NewInvalidInputNoCtxf("interval type, bad value '%d'", a[i])
		}
		curMul *= mul[i]
		if a[i] > math.MaxInt64/curMul || a[i] < math.MinInt64/curMul {
			return 0, IntervalTypeInvalid, moerr.NewInvalidInputNoCtxf("interval type, bad value '%d'", a[i])
		}
		term := a[i] * curMul
		if (term > 0 && ret > math.MaxInt64-term) || (term < 0 && ret < math.MinInt64-term) {
			return 0, IntervalTypeInvalid, moerr.NewInvalidInputNoCtxf("interval type, bad value '%d'", a[i])
		}
		ret += term
	}
	if largerThanZero && ret < 0 {
		return 0, IntervalTypeInvalid, moerr.NewInvalidInputNoCtxf("interval type, bad value '%d'", ret)
	} else if !largerThanZero && ret > 0 {
		return 0, IntervalTypeInvalid, moerr.NewInvalidInputNoCtxf("interval type, bad value '%d'", ret)
	}

	return ret, rt, nil
}

func NormalizeInterval(s string, it IntervalType) (ret int64, rettype IntervalType, err error) {
	return normalizeInterval(s, it, nil)
}

// NormalizeIntervalWithOverflow retains the error kind for consumers whose
// diagnostics distinguish a numeric overflow from invalid interval syntax.
// The ordinary NormalizeInterval error and result contract remains unchanged.
func NormalizeIntervalWithOverflow(s string, it IntervalType) (ret int64, rettype IntervalType, overflow bool, err error) {
	ret, rettype, err = normalizeInterval(s, it, &overflow)
	return
}

func normalizeInterval(s string, it IntervalType, overflow *bool) (ret int64, rettype IntervalType, err error) {
	s = strings.TrimSpace(s)
	maxLen := typeMaxLength(it)
	fields, count, err := splitIntervalNumberFields(s, maxLen+1)
	if err != nil {
		return 0, IntervalTypeInvalid, err
	}
	invalid := func() (int64, IntervalType, error) {
		return 0, IntervalTypeInvalid, moerr.NewInvalidInputNoCtxf("invalid time interval value '%s'", s)
	}
	if count == 0 && overflow != nil {
		return invalid()
	}
	negative := strings.HasPrefix(s, "-")
	microsecondFields := isxxxMicrosecondType(it)
	multiplier := int64(0)
	if !microsecondFields {
		switch it {
		case Second, Minute_Second, Hour_Second, Day_Second:
			multiplier = MicroSecsPerSec
		case Minute:
			multiplier = MicroSecsPerSec * SecsPerMinute
		case Hour:
			multiplier = MicroSecsPerSec * SecsPerHour
		case Day:
			multiplier = MicroSecsPerSec * SecsPerDay
		}
	}

	prefixCount := count
	var fractional int64
	hasFraction := false
	if microsecondFields {
		if count > maxLen {
			return invalid()
		}
		if count > 0 {
			prefixCount--
			fractional = scaledIntervalFraction(s, fields[count-1], MicroSecsPerSec)
			hasFraction = true
		}
	} else if count == maxLen+1 && multiplier != 0 {
		dot := strings.LastIndexByte(s, '.')
		if dot < 0 || fields[count-1].start != dot+1 || fields[count-1].end != len(s) {
			return invalid()
		}
		prefixCount--
		fractional = scaledIntervalFraction(s, fields[count-1], multiplier)
		hasFraction = true
	} else if count > maxLen {
		return invalid()
	}

	vals := make([]int64, 0, maxLen)
	for i := 0; i < prefixCount; i++ {
		field, fieldErr := intervalFieldValue(s, fields[i])
		if fieldErr != nil {
			if overflow != nil {
				*overflow = true // digit-only field exceeded int64
			}
			return 0, IntervalTypeInvalid, fieldErr
		}
		vals = append(vals, field)
	}
	if hasFraction {
		if microsecondFields {
			vals = append(vals, fractional)
		} else if maxLen == 1 {
			whole := vals[0]
			if whole > (math.MaxInt64-fractional)/multiplier {
				if overflow != nil {
					*overflow = true
				}
				return invalid()
			}
			value := whole*multiplier + fractional
			if negative {
				value = -value
			}
			return value, MicroSecond, nil
		} else {
			last := len(vals) - 1
			if vals[last] > (math.MaxInt64-fractional)/MicroSecsPerSec {
				if overflow != nil {
					*overflow = true
				}
				return invalid()
			}
			vals[last] = vals[last]*MicroSecsPerSec + fractional
		}
	}
	// As in the existing interval grammar, a leading minus negates every field.
	// Keep the lexical sign separately so -0.0000005 can round to -1 microsecond.
	if negative {
		for i := range vals {
			vals[i] = -vals[i]
		}
	}

	// For composite interval types, if we have fewer values than expected,
	// pad with zeros. The interpretation depends on the interval type:
	// - For microsecond types (Day_MicroSecond, Hour_MicroSecond, etc.):
	//   If we have 2 values, the first is the second-to-last unit (e.g., second for day_microsecond),
	//   and the last is the microsecond part.
	// - For other types, pad from the left (missing higher-order units default to 0)
	typeMaxLen := maxLen
	if len(vals) < typeMaxLen && typeMaxLen > 1 {
		padded := make([]int64, typeMaxLen)
		if isxxxMicrosecondType(it) {
			// For microsecond types, if we have exactly 2 values, they represent
			// the second-to-last unit and the microsecond part
			// e.g., '1.02' day_microsecond -> [1, 20000] -> [0, 0, 0, 1, 20000]
			if len(vals) == 2 {
				padded[typeMaxLen-2] = vals[0] // second-to-last position
				padded[typeMaxLen-1] = vals[1] // last position (microsecond)
			} else if len(vals) == 1 {
				// Single value goes to the last position (microsecond)
				padded[typeMaxLen-1] = vals[0]
			} else {
				// More than 2 values: pad from the left
				copy(padded[typeMaxLen-len(vals):], vals)
			}
		} else {
			// For non-microsecond types, pad from the left
			copy(padded[typeMaxLen-len(vals):], vals)
		}
		vals = padded
	}

	switch it {
	case MicroSecond, Second, Minute, Hour, Day,
		Week, Month, Quarter, Year:
		ret, rettype, err = conv(vals, []int64{1}, it)

	case Second_MicroSecond:
		ret, rettype, err = conv(vals, []int64{1000000, 1}, MicroSecond)

	case Minute_MicroSecond:
		ret, rettype, err = conv(vals, []int64{60, 1000000, 1}, MicroSecond)

	case Minute_Second:
		if hasFraction {
			ret, rettype, err = conv(vals, []int64{60 * MicroSecsPerSec, 1}, MicroSecond)
		} else {
			ret, rettype, err = conv(vals, []int64{60, 1}, Second)
		}

	case Hour_MicroSecond:
		ret, rettype, err = conv(vals, []int64{60, 60, 1000000, 1}, MicroSecond)

	case Hour_Second:
		if hasFraction {
			ret, rettype, err = conv(vals, []int64{60, 60 * MicroSecsPerSec, 1}, MicroSecond)
		} else {
			ret, rettype, err = conv(vals, []int64{60, 60, 1}, Second)
		}

	case Hour_Minute:
		ret, rettype, err = conv(vals, []int64{60, 1}, Minute)

	case Day_MicroSecond:
		ret, rettype, err = conv(vals, []int64{24, 60, 60, 1000000, 1}, MicroSecond)

	case Day_Second:
		if hasFraction {
			ret, rettype, err = conv(vals, []int64{24, 60, 60 * MicroSecsPerSec, 1}, MicroSecond)
		} else {
			ret, rettype, err = conv(vals, []int64{24, 60, 60, 1}, Second)
		}

	case Day_Minute:
		ret, rettype, err = conv(vals, []int64{24, 60, 1}, Minute)

	case Day_Hour:
		ret, rettype, err = conv(vals, []int64{24, 1}, Hour)
	case Year_Month:
		ret, rettype, err = conv(vals, []int64{12, 1}, Month)
	}
	if err != nil && overflow != nil {
		*overflow = true // checked field multiplication or sum in conv
	}
	return
}

func isxxxMicrosecondType(it IntervalType) bool {
	return it == Second_MicroSecond || it == Minute_MicroSecond || it == Hour_MicroSecond || it == Day_MicroSecond
}

func typeMaxLength(it IntervalType) int {
	switch it {
	case MicroSecond, Second, Minute, Hour, Day,
		Week, Month, Quarter, Year:
		return 1

	case Second_MicroSecond:
		return 2

	case Minute_MicroSecond:
		return 3

	case Minute_Second:
		return 2

	case Hour_MicroSecond:
		return 4

	case Hour_Second:
		return 3

	case Hour_Minute:
		return 2

	case Day_MicroSecond:
		return 5

	case Day_Second:
		return 4

	case Day_Minute:
		return 3

	case Day_Hour:
		return 2

	case Year_Month:
		return 2
	}
	return 0
}

// UnitIsDayOrLarger if interval type unit is day or larger, we return true
// else return false
// use to judge a string whether it needs to become date/datetime type when we use date_add/sub(str string, interval type)
func UnitIsDayOrLarger(it IntervalType) bool {
	return it == Day || it == Week || it == Month || it == Quarter || it == Year || it == Year_Month
}

func JudgeIntervalNumOverflow(num int64, it IntervalType) error {
	if it == MicroSecond {
		return nil
	} else if num > int64(IntervalNumMAX) {
		return moerr.NewInvalidArgNoCtx("interval", num)
	} else if num < -int64(IntervalNumMAX) {
		return moerr.NewInvalidArgNoCtx("interval", num)
	}
	return nil
}
