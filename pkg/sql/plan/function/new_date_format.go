// Copyright 2021 - 2024 Matrix Origin
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

package function

import (
	"bytes"
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"math"
	"strconv"
)

type DateFormatFunc func(ctx context.Context, t types.Datetime, b rune, buf *bytes.Buffer) error

var funcs [52]DateFormatFunc

//func init() {
//	startIdx := 'a'
//	funcs['b'-startIdx] = date_format_pattern_b
//	funcs['M'-startIdx] = date_format_pattern_M
//	funcs['m'-startIdx] = date_format_pattern_m
//	funcs['c'-startIdx] = date_format_pattern_c
//	funcs['D'-startIdx] = date_format_pattern_D
//	funcs['d'-startIdx] = date_format_pattern_d
//	funcs['e'-startIdx] = date_format_pattern_e
//	funcs['f'-startIdx] = date_format_pattern_f
//	funcs['j'-startIdx] = date_format_pattern_j
//	funcs['H'-startIdx] = date_format_pattern_H
//	funcs['h'-startIdx] = date_format_pattern_hI
//	funcs['k'-startIdx] = date_format_pattern_k
//	funcs['I'-startIdx] = date_format_pattern_hI
//	funcs['i'-startIdx] = date_format_pattern_i
//	funcs['l'-startIdx] = date_format_pattern_l
//	funcs['p'-startIdx] = date_format_pattern_p
//	funcs['r'-startIdx] = date_format_pattern_r
//	funcs['S'-startIdx] = date_format_pattern_Ss
//	funcs['s'-startIdx] = date_format_pattern_Ss
//	funcs['T'-startIdx] = date_format_pattern_T
//	funcs['U'-startIdx] = date_format_pattern_U
//	funcs['u'-startIdx] = date_format_pattern_u
//	funcs['V'-startIdx] = date_format_pattern_V
//	funcs['v'-startIdx] = date_format_pattern_v
//	funcs['a'-startIdx] = date_format_pattern_a
//	funcs['W'-startIdx] = date_format_pattern_W
//	funcs['w'-startIdx] = date_format_pattern_w
//	funcs['X'-startIdx] = date_format_pattern_X
//	funcs['x'-startIdx] = date_format_pattern_x
//	funcs['Y'-startIdx] = date_format_pattern_Y
//	funcs['y'-startIdx] = date_format_pattern_y
//}

func date_format_pattern_b(ctx context.Context, t types.Datetime, _ rune, buf *bytes.Buffer) error {
	m := t.Month()
	if m == 0 || m > 12 {
		return moerr.NewInvalidInput(ctx, "invalud date format for month '%d'", m)
	}
	buf.WriteString(MonthNames[m-1][:3])
	return nil
}

func date_format_pattern_M(ctx context.Context, t types.Datetime, _ rune, buf *bytes.Buffer) error {
	m := t.Month()
	if m == 0 || m > 12 {
		return moerr.NewInvalidInput(ctx, "invalud date format for month '%d'", m)
	}
	buf.WriteString(MonthNames[m-1])
	return nil
}

func date_format_pattern_m(ctx context.Context, t types.Datetime, _ rune, buf *bytes.Buffer) error {
	buf.WriteString(FormatIntByWidth(int(t.Month()), 2))
	return nil
}

func date_format_pattern_c(ctx context.Context, t types.Datetime, _ rune, buf *bytes.Buffer) error {
	buf.WriteString(strconv.FormatInt(int64(t.Month()), 10))
	return nil
}

func date_format_pattern_D(ctx context.Context, t types.Datetime, _ rune, buf *bytes.Buffer) error {
	buf.WriteString(strconv.FormatInt(int64(t.Day()), 10))
	buf.WriteString(AbbrDayOfMonth(int(t.Day())))
	return nil
}

func date_format_pattern_d(ctx context.Context, t types.Datetime, _ rune, buf *bytes.Buffer) error {
	buf.WriteString(FormatIntByWidth(int(t.Day()), 2))
	return nil
}

func date_format_pattern_e(ctx context.Context, t types.Datetime, _ rune, buf *bytes.Buffer) error {
	buf.WriteString(strconv.FormatInt(int64(t.Day()), 10))
	return nil
}

func date_format_pattern_f(ctx context.Context, t types.Datetime, _ rune, buf *bytes.Buffer) error {
	fmt.Fprintf(buf, "%06d", t.MicroSec())
	return nil
}

func date_format_pattern_j(ctx context.Context, t types.Datetime, _ rune, buf *bytes.Buffer) error {
	fmt.Fprintf(buf, "%03d", t.DayOfYear())
	return nil
}

func date_format_pattern_H(ctx context.Context, t types.Datetime, b rune, buf *bytes.Buffer) error {
	buf.WriteString(FormatIntByWidth(int(t.Hour()), 2))
	return nil
}

func date_format_pattern_k(ctx context.Context, t types.Datetime, _ rune, buf *bytes.Buffer) error {
	buf.WriteString(strconv.FormatInt(int64(t.Hour()), 10))
	return nil
}

func date_format_pattern_hI(ctx context.Context, t types.Datetime, _ rune, buf *bytes.Buffer) error {
	tt := t.Hour()
	if tt%12 == 0 {
		buf.WriteString("12")
	} else {
		buf.WriteString(FormatIntByWidth(int(tt%12), 2))
	}
	return nil
}

func date_format_pattern_i(ctx context.Context, t types.Datetime, _ rune, buf *bytes.Buffer) error {
	buf.WriteString(FormatIntByWidth(int(t.Minute()), 2))
	return nil
}

func date_format_pattern_l(ctx context.Context, t types.Datetime, _ rune, buf *bytes.Buffer) error {
	tt := t.Hour()
	if tt%12 == 0 {
		buf.WriteString("12")
	} else {
		buf.WriteString(strconv.FormatInt(int64(tt%12), 10))
	}
	return nil
}

func date_format_pattern_p(ctx context.Context, t types.Datetime, _ rune, buf *bytes.Buffer) error {
	hour := t.Hour()
	if hour/12%2 == 0 {
		buf.WriteString("AM")
	} else {
		buf.WriteString("PM")
	}
	return nil
}

func date_format_pattern_r(ctx context.Context, t types.Datetime, _ rune, buf *bytes.Buffer) error {
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
	return nil
}

func date_format_pattern_Ss(ctx context.Context, t types.Datetime, _ rune, buf *bytes.Buffer) error {
	buf.WriteString(FormatIntByWidth(int(t.Sec()), 2))
	return nil
}

func date_format_pattern_T(ctx context.Context, t types.Datetime, _ rune, buf *bytes.Buffer) error {
	fmt.Fprintf(buf, "%02d:%02d:%02d", t.Hour(), t.Minute(), t.Sec())
	return nil
}

func date_format_pattern_U(ctx context.Context, t types.Datetime, _ rune, buf *bytes.Buffer) error {
	w := t.Week(0)
	buf.WriteString(FormatIntByWidth(w, 2))
	return nil
}

func date_format_pattern_u(ctx context.Context, t types.Datetime, _ rune, buf *bytes.Buffer) error {
	w := t.Week(1)
	buf.WriteString(FormatIntByWidth(w, 2))
	return nil
}

func date_format_pattern_V(ctx context.Context, t types.Datetime, _ rune, buf *bytes.Buffer) error {
	w := t.Week(2)
	buf.WriteString(FormatIntByWidth(w, 2))
	return nil
}

func date_format_pattern_v(ctx context.Context, t types.Datetime, _ rune, buf *bytes.Buffer) error {
	_, w := t.YearWeek(3)
	buf.WriteString(FormatIntByWidth(w, 2))
	return nil
}

func date_format_pattern_a(ctx context.Context, t types.Datetime, _ rune, buf *bytes.Buffer) error {
	weekday := t.DayOfWeek()
	buf.WriteString(AbbrevWeekdayName[weekday])
	return nil
}

func date_format_pattern_W(ctx context.Context, t types.Datetime, _ rune, buf *bytes.Buffer) error {
	buf.WriteString(t.DayOfWeek().String())
	return nil
}

func date_format_pattern_w(ctx context.Context, t types.Datetime, _ rune, buf *bytes.Buffer) error {
	buf.WriteString(strconv.FormatInt(int64(t.DayOfWeek()), 10))
	return nil
}

func date_format_pattern_X(ctx context.Context, t types.Datetime, _ rune, buf *bytes.Buffer) error {
	year, _ := t.YearWeek(2)
	if year < 0 {
		buf.WriteString(strconv.FormatUint(uint64(math.MaxUint32), 10))
	} else {
		buf.WriteString(FormatIntByWidth(year, 4))
	}
	return nil
}

func date_format_pattern_x(ctx context.Context, t types.Datetime, _ rune, buf *bytes.Buffer) error {
	year, _ := t.YearWeek(3)
	if year < 0 {
		buf.WriteString(strconv.FormatUint(uint64(math.MaxUint32), 10))
	} else {
		buf.WriteString(FormatIntByWidth(year, 4))
	}
	return nil
}

func date_format_pattern_Y(ctx context.Context, t types.Datetime, _ rune, buf *bytes.Buffer) error {
	buf.WriteString(FormatIntByWidth(int(t.Year()), 4))
	return nil
}

func date_format_pattern_y(ctx context.Context, t types.Datetime, b rune, buf *bytes.Buffer) error {
	str := FormatIntByWidth(int(t.Year()), 4)
	buf.WriteString(str[2:])
	return nil
}

func date_format_default_pattern(ctx context.Context, t types.Datetime, b rune, buf *bytes.Buffer) error {
	buf.WriteRune(b)
	return nil
}
