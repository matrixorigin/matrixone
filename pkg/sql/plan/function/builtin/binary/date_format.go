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

package binary

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// function overview:
// date_format() function used to formated the date value according to the format string. If either argument is NULL, the function returns NULL.
// Input parameter type: date type: datatime, format type: string(constant)
// return type: string
// reference linking:https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-format

var (
	// WeekdayNames lists names of weekdays, which are used in builtin function `date_format`.
	WeekdayNames = []string{
		"Monday",
		"Tuesday",
		"Wednesday",
		"Thursday",
		"Friday",
		"Saturday",
		"Sunday",
	}

	// MonthNames lists names of months, which are used in builtin function `date_format`.
	MonthNames = []string{
		"January",
		"February",
		"March",
		"April",
		"May",
		"June",
		"July",
		"August",
		"September",
		"October",
		"November",
		"December",
	}

	// AbbrevWeekdayName lists Abbreviation of week names, which are used int builtin function 'date_format'
	AbbrevWeekdayName = []string{
		"Sun",
		"Mon",
		"Tue",
		"Wed",
		"Thu",
		"Fri",
		"Sat",
	}
)

// DateFromat: Formats the date value according to the format string. If either argument is NULL, the function returns NULL.
func DateFormat(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	dateVector := vectors[0]
	formatVector := vectors[1]

	rtyp := types.T_varchar.ToType()
	if !formatVector.IsConst() {
		return nil, moerr.NewInvalidArg(proc.Ctx, "date format format", "not constant")
	}

	if dateVector.IsConstNull() || formatVector.IsConstNull() {
		rvec := vector.NewConstNull(rtyp, dateVector.Length(), proc.Mp())
		return rvec, nil
	}

	// get the format string.
	formatMask := string(formatVector.GetStringAt(0))

	if dateVector.IsConst() {
		// XXX Null handling maybe broken.
		datetimes := vector.MustFixedCol[types.Datetime](dateVector)
		resCol, err := CalcDateFromat(proc.Ctx, datetimes, formatMask, dateVector.GetNulls())
		if err != nil {
			return nil, err
		}
		return vector.NewConstBytes(rtyp, []byte(resCol[0]), dateVector.Length(), proc.Mp()), nil
	} else {
		datetimes := vector.MustFixedCol[types.Datetime](dateVector)
		resCol, err := CalcDateFromat(proc.Ctx, datetimes, formatMask, dateVector.GetNulls())
		if err != nil {
			return nil, err
		}
		rvec := vector.NewVec(rtyp)
		nulls.Set(rvec.GetNulls(), dateVector.GetNulls())
		vector.AppendStringList(rvec, resCol, nil, proc.Mp())
		return rvec, nil
	}
}

// CalcDateFromat: DateFromat is used to formating the datetime values according to the format string.
func CalcDateFromat(ctx context.Context, datetimes []types.Datetime, format string, ns *nulls.Nulls) ([]string, error) {
	res := make([]string, len(datetimes))
	for idx, datetime := range datetimes {
		if nulls.Contains(ns, uint64(idx)) {
			continue
		}
		formatStr, err := datetimeFormat(ctx, datetime, format)
		if err != nil {
			return nil, err
		}
		res[idx] = formatStr
	}
	return res, nil
}

// datetimeFormat: format the datetime value according to the format string.
func datetimeFormat(ctx context.Context, datetime types.Datetime, format string) (string, error) {
	var buf bytes.Buffer
	inPatternMatch := false
	for _, b := range format {
		if inPatternMatch {
			if err := makeDateFormat(ctx, datetime, b, &buf); err != nil {
				return "", err
			}
			inPatternMatch = false
			continue
		}

		// It's not in pattern match now.
		if b == '%' {
			inPatternMatch = true
		} else {
			buf.WriteRune(b)
		}
	}
	return buf.String(), nil
}

// makeDateFormat: Get the format string corresponding to the date according to a single format character
func makeDateFormat(ctx context.Context, t types.Datetime, b rune, buf *bytes.Buffer) error {
	switch b {
	case 'b':
		m := t.Month()
		if m == 0 || m > 12 {
			return moerr.NewInvalidInput(ctx, "invalud date format for month '%d'", m)
		}
		buf.WriteString(MonthNames[m-1][:3])
	case 'M':
		m := t.Month()
		if m == 0 || m > 12 {
			return moerr.NewInvalidInput(ctx, "invalud date format for month '%d'", m)
		}
		buf.WriteString(MonthNames[m-1])
	case 'm':
		buf.WriteString(FormatIntByWidth(int(t.Month()), 2))
	case 'c':
		buf.WriteString(strconv.FormatInt(int64(t.Month()), 10))
	case 'D':
		buf.WriteString(strconv.FormatInt(int64(t.Day()), 10))
		buf.WriteString(AbbrDayOfMonth(int(t.Day())))
	case 'd':
		buf.WriteString(FormatIntByWidth(int(t.Day()), 2))
	case 'e':
		buf.WriteString(strconv.FormatInt(int64(t.Day()), 10))
	case 'f':
		fmt.Fprintf(buf, "%06d", t.MicroSec())
	case 'j':
		fmt.Fprintf(buf, "%03d", t.DayOfYear())
	case 'H':
		buf.WriteString(FormatIntByWidth(int(t.Hour()), 2))
	case 'k':
		buf.WriteString(strconv.FormatInt(int64(t.Hour()), 10))
	case 'h', 'I':
		tt := t.Hour()
		if tt%12 == 0 {
			buf.WriteString("12")
		} else {
			buf.WriteString(FormatIntByWidth(int(tt%12), 2))
		}
	case 'i':
		buf.WriteString(FormatIntByWidth(int(t.Minute()), 2))
	case 'l':
		tt := t.Hour()
		if tt%12 == 0 {
			buf.WriteString("12")
		} else {
			buf.WriteString(strconv.FormatInt(int64(tt%12), 10))
		}
	case 'p':
		hour := t.Hour()
		if hour/12%2 == 0 {
			buf.WriteString("AM")
		} else {
			buf.WriteString("PM")
		}
	case 'r':
		h := t.Hour()
		h %= 24
		switch {
		case h == 0:
			fmt.Fprintf(buf, "%02d:%02d:%02d AM", 12, t.Minute(), t.Sec())
		case h == 12:
			fmt.Fprintf(buf, "%02d:%02d:%02d PM", 12, t.Minute(), t.Sec())
		case h < 12:
			fmt.Fprintf(buf, "%02d:%02d:%02d AM", h, t.Minute(), t.Sec())
		default:
			fmt.Fprintf(buf, "%02d:%02d:%02d PM", h-12, t.Minute(), t.Sec())
		}
	case 'S', 's':
		buf.WriteString(FormatIntByWidth(int(t.Sec()), 2))
	case 'T':
		fmt.Fprintf(buf, "%02d:%02d:%02d", t.Hour(), t.Minute(), t.Sec())
	case 'U':
		w := t.Week(0)
		buf.WriteString(FormatIntByWidth(w, 2))
	case 'u':
		w := t.Week(1)
		buf.WriteString(FormatIntByWidth(w, 2))
	case 'V':
		w := t.Week(2)
		buf.WriteString(FormatIntByWidth(w, 2))
	case 'v':
		_, w := t.YearWeek(3)
		buf.WriteString(FormatIntByWidth(w, 2))
	case 'a':
		weekday := t.DayOfWeek()
		buf.WriteString(AbbrevWeekdayName[weekday])
	case 'W':
		buf.WriteString(t.DayOfWeek().String())
	case 'w':
		buf.WriteString(strconv.FormatInt(int64(t.DayOfWeek()), 10))
	case 'X':
		year, _ := t.YearWeek(2)
		if year < 0 {
			buf.WriteString(strconv.FormatUint(uint64(math.MaxUint32), 10))
		} else {
			buf.WriteString(FormatIntByWidth(year, 4))
		}
	case 'x':
		year, _ := t.YearWeek(3)
		if year < 0 {
			buf.WriteString(strconv.FormatUint(uint64(math.MaxUint32), 10))
		} else {
			buf.WriteString(FormatIntByWidth(year, 4))
		}
	case 'Y':
		buf.WriteString(FormatIntByWidth(int(t.Year()), 4))
	case 'y':
		str := FormatIntByWidth(int(t.Year()), 4)
		buf.WriteString(str[2:])
	default:
		buf.WriteRune(b)
	}
	return nil
}

// FormatIntByWidth: Formatintwidthn is used to format ints with width parameter n. Insufficient numbers are filled with 0.
func FormatIntByWidth(num, n int) string {
	numStr := strconv.FormatInt(int64(num), 10)
	if len(numStr) >= n {
		return numStr
	}
	padBytes := make([]byte, n-len(numStr))
	for i := range padBytes {
		padBytes[i] = '0'
	}
	return string(padBytes) + numStr
}

// AbbrDayOfMonth: Get the abbreviation of month of day
func AbbrDayOfMonth(day int) string {
	var str string
	switch day {
	case 1, 21, 31:
		str = "st"
	case 2, 22:
		str = "nd"
	case 3, 23:
		str = "rd"
	default:
		str = "th"
	}
	return str
}
