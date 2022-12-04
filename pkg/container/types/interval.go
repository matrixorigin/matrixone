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
	return IntervalTypeMax, moerr.NewInvalidInputNoCtx("invalid interval type '%s'", s)
}

// parseInts parse integer from string s.   This is used to handle interval values,
// when interval type is Second_MicroSecond Minute_MicroSecond Hour_MicroSecond Day_MicroSecond
// we should set second parameter true, other set false
// the example: when the s is "1:1"
// when we use Second_MicroSecond(...), we should parse to 1 second and 100000 microsecond.
// when we use Minute_Second(...), we just parse to 1 minute and 1 second.
// so we use method to solve this: we count the length of the num, use 1e(6 - length) * ret[len(ret) - 1]
// for example: when the s is "1:001"
// the last number length is 3, so the last number should be 1e(6 - 3) * 1 = 1000
// so there are a few strange things.
//  1. Only takes 0-9, may have leading 0, still means decimal instead oct.
//  2. 1-1 is parsed out as 1, 1 '-' is delim, so is '+', '.' etc.
//  3. we will not support int32 overflow.
func parseInts(s string, isxxxMicrosecond bool, typeMaxLength int) ([]int64, error) {
	ret := make([]int64, 0)
	numLength := 0
	cur := -1
	for _, c := range s {
		if c >= rune('0') && c <= rune('9') {
			if cur < 0 {
				cur = len(ret)
				ret = append(ret, int64(c-rune('0')))
				numLength++
			} else {
				ret[cur] = 10*ret[cur] + int64(c-rune('0'))
				numLength++
				if ret[cur] < 0 {
					return nil, moerr.NewInvalidInputNoCtx("invalid time interval value '%s'", s)
				}
			}
		} else {
			if cur >= 0 {
				cur = -1
				numLength = 0
			}
		}
	}
	if isxxxMicrosecond {
		if len(ret) == typeMaxLength {
			ret[len(ret)-1] *= int64(math.Pow10(6 - numLength))
		}
	}
	// parse "-1:1"
	for _, c := range s {
		if c == ' ' {
			continue
		} else if c == '-' {
			for i := range ret {
				ret[i] = -ret[i]
			}
			break
		} else {
			break
		}
	}
	return ret, nil
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
		curMul = curMul * mul[i]
		ret += int64(a[i]) * curMul
	}
	if largerThanZero && ret < 0 {
		return 0, IntervalTypeInvalid, moerr.NewInvalidInputNoCtx("interval type, bad value '%d'", ret)
	} else if !largerThanZero && ret > 0 {
		return 0, IntervalTypeInvalid, moerr.NewInvalidInputNoCtx("interval type, bad value '%d'", ret)
	}

	return ret, rt, nil
}

func NormalizeInterval(s string, it IntervalType) (ret int64, rettype IntervalType, err error) {
	vals, err := parseInts(s, isxxxMicrosecondType(it), typeMaxLength(it))
	if err != nil {
		return
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
		ret, rettype, err = conv(vals, []int64{60, 1}, Second)

	case Hour_MicroSecond:
		ret, rettype, err = conv(vals, []int64{60, 60, 1000000, 1}, MicroSecond)

	case Hour_Second:
		ret, rettype, err = conv(vals, []int64{60, 60, 1}, Second)

	case Hour_Minute:
		ret, rettype, err = conv(vals, []int64{60, 1}, Minute)

	case Day_MicroSecond:
		ret, rettype, err = conv(vals, []int64{24, 60, 60, 1000000, 1}, MicroSecond)

	case Day_Second:
		ret, rettype, err = conv(vals, []int64{24, 60, 60, 1}, Second)

	case Day_Minute:
		ret, rettype, err = conv(vals, []int64{24, 60, 1}, Minute)

	case Day_Hour:
		ret, rettype, err = conv(vals, []int64{24, 1}, Hour)
	case Year_Month:
		ret, rettype, err = conv(vals, []int64{12, 1}, Month)
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
	} else if -num > int64(IntervalNumMAX) {
		return moerr.NewInvalidArgNoCtx("interval", num)
	}
	return nil
}
