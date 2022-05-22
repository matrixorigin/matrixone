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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
)

/*
 * Interval in MySQL is not a real type, that is, it cannot be stored in
 * a table as column.   We still treat it as if it is a type so that we
 * can use it in some functions.
 */

type IntervalType int8

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
	return IntervalTypeMax, errors.New(errno.DataException, "Invalud Interval Type")
}

//
// parseInts parse integer from string s.   This is used to handle interval values,
// so there are a few strange things.
//	1. Only takes 0-9, may have leading 0, still means decimal instead oct.
//  2. 1-1 is parsed out as 1, 1 '-' is delim, so is '+', '.' etc.
//	3. we will not support int32 overflow.
//
func parseInts(s string) ([]int32, error) {
	ret := make([]int32, 0)
	cur := -1
	for _, c := range s {
		if c >= rune('0') && c <= rune('9') {
			if cur < 0 {
				cur = len(ret)
				ret = append(ret, c-rune('0'))
			} else {
				ret[cur] = 10*ret[cur] + c - rune('0')
				if ret[cur] < 0 {
					return nil, errors.New(errno.DataException, "Invalid string interval value")
				}
			}
		} else {
			if cur >= 0 {
				cur = -1
			}
		}
	}
	return ret, nil
}

func conv(a []int32, mul []int64, rt IntervalType) (int64, IntervalType, error) {
	if len(a) > len(mul) {
		return 0, IntervalTypeInvalid, errors.New(errno.DataException, "Invalid interval format")
	}

	var ret int64
	var curMul int64 = 1

	for i := len(a) - 1; i >= 0; i-- {
		curMul = curMul * mul[i]
		ret += int64(a[i]) * curMul
	}
	if ret < 0 {
		return 0, IntervalTypeInvalid, errors.New(errno.DataException, "Interval value overflow")
	}
	return ret, rt, nil
}

func NormalizeInterval(s string, it IntervalType) (ret int64, rettype IntervalType, err error) {
	vals, err := parseInts(s)
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
