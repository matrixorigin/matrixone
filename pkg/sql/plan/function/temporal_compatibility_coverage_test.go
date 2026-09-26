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

package function

import (
	"bytes"
	"context"
	"math"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// These cases exercise the compatibility branches added for runtime temporal
// formats and for the typed/string temporal arithmetic matrix.  The BVT cases
// validate the public SQL contract; this test keeps the changed execution
// branches covered by the unit-test coverage gate as well.
func TestTemporalCompatibilityExecutionMatrix(t *testing.T) {
	proc := newTmpProcess(t)

	t.Run("str_to_date duration days", func(t *testing.T) {
		input := NewFunctionTestInput(types.T_varchar.ToType(), []string{
			"1 12:34:56", "31 12:34:56", "32 12:34:56", "34 22:59:59", "35 12:34:56", "99 23:59:59", "1 12:34:56.123456", "bad",
		}, nil)
		format := NewFunctionTestInput(types.T_varchar.ToType(), []string{
			"%d %H:%i:%s", "%e %H:%i:%s", "%d %H:%i:%s", "%d %H:%i:%s", "%d %H:%i:%s", "%d %H:%i:%s", "%d %H:%i:%s.%f", "%d %H:%i:%s",
		}, nil)
		want := NewFunctionTestResult(types.T_time.ToTypeWithScale(6), false,
			[]types.Time{
				types.TimeFromClock(false, 36, 34, 56, 0), types.TimeFromClock(false, 756, 34, 56, 0),
				types.TimeFromClock(false, 780, 34, 56, 0), types.TimeFromClock(false, 838, 59, 59, 0),
				0, 0, types.TimeFromClock(false, 36, 34, 56, 123456), 0,
			}, []bool{false, false, false, false, true, true, false, true})
		caseDef := NewFunctionTestCase(proc, []FunctionTestInput{input, format}, want, builtInStrToTime)
		ok, info := caseDef.Run()
		require.True(t, ok, info)
	})

	t.Run("dynamic str_to_date keeps datetime domain", func(t *testing.T) {
		input := NewFunctionTestInput(types.T_varchar.ToType(), []string{
			"12:34:56.123456", "2024-02-29", "2024-02-29 12:34:56.123456",
			"bad", "2024-02-29", "12:34:56",
		}, []bool{false, false, false, false, true, false})
		format := NewFunctionTestInput(types.T_varchar.ToType(), []string{
			"%H:%i:%s.%f", "%Y-%m-%d", "%Y-%m-%d %H:%i:%s.%f",
			"%Y-%m-%d", "%Y-%m-%d", "%Y-%m-%d",
		}, nil)
		midnight, err := types.ParseDatetime("2024-02-29 00:00:00", 6)
		require.NoError(t, err)
		full, err := types.ParseDatetime("2024-02-29 12:34:56.123456", 6)
		require.NoError(t, err)
		want := NewFunctionTestResult(types.New(types.T_datetime, 0, 6), false,
			[]types.Datetime{0, midnight, full, 0, 0, 0},
			[]bool{true, false, false, true, true, true})
		caseDef := NewFunctionTestCase(proc, []FunctionTestInput{input, format}, want, builtInStrToDatetime)
		ok, info := caseDef.Run()
		require.True(t, ok, info)
	})

	t.Run("typed time arithmetic", func(t *testing.T) {
		left := NewFunctionTestInput(types.T_time.ToType(), []types.Time{
			types.TimeFromClock(false, 12, 34, 56, 0),
			types.TimeFromClock(true, 1, 0, 0, 0),
		}, nil)
		right := NewFunctionTestInput(types.T_varchar.ToType(), []string{
			"1:02:03.100000",
			"2:00:00",
		}, nil)
		for _, tc := range []struct {
			name string
			fn   fEvalFn
			want []types.Time
		}{
			{name: "add", fn: AddTime, want: []types.Time{
				types.TimeFromClock(false, 13, 36, 59, 0),
				types.TimeFromClock(false, 1, 0, 0, 0),
			}},
			{name: "sub", fn: SubTime, want: []types.Time{
				types.TimeFromClock(false, 11, 32, 53, 0),
				types.TimeFromClock(true, 3, 0, 0, 0),
			}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				caseDef := NewFunctionTestCase(proc, []FunctionTestInput{left, right},
					NewFunctionTestResult(types.T_time.ToType(), false, tc.want, []bool{false, false}), tc.fn)
				ok, info := caseDef.Run()
				require.True(t, ok, info)
			})
		}
	})

	t.Run("datetime arithmetic and invalid operands", func(t *testing.T) {
		dt, err := types.ParseDatetime("2024-02-29 12:34:56.123456", 6)
		require.NoError(t, err)
		left := NewFunctionTestInput(types.T_datetime.ToTypeWithScale(6), []types.Datetime{dt, types.ZeroDatetime}, []bool{false, false})
		right := NewFunctionTestInput(types.T_varchar.ToType(), []string{"01:02:03.100000", "bad"}, []bool{false, false})
		want, err := types.ParseDatetime("2024-02-29 13:36:59.223456", 6)
		require.NoError(t, err)
		for _, tc := range []struct {
			name string
			fn   fEvalFn
			vals []types.Datetime
			null []bool
		}{
			{"add", AddTime, []types.Datetime{want, 0}, []bool{false, true}},
			{"sub", SubTime, []types.Datetime{dt - types.Datetime(3723100000), 0}, []bool{false, true}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				caseDef := NewFunctionTestCase(proc, []FunctionTestInput{left, right},
					NewFunctionTestResult(types.T_datetime.ToTypeWithScale(6), false, tc.vals, tc.null), tc.fn)
				ok, info := caseDef.Run()
				require.True(t, ok, info)
			})
		}
	})

	t.Run("format and extract domains", func(t *testing.T) {
		dt, err := types.ParseDatetime("2024-02-29 12:34:56.123456", 6)
		require.NoError(t, err)
		date, err := types.ParseDatetime("2024-02-29 00:00:00", 0)
		require.NoError(t, err)
		tm := types.TimeFromClock(false, 12, 34, 56, 123456)
		for _, tc := range []struct {
			name   string
			fn     fEvalFn
			in     FunctionTestInput
			format string
			want   string
		}{
			{name: "date format date", fn: DateFormat, in: NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{date}, nil), format: "%Y-%m-%d", want: "2024-02-29"},
			{name: "date format datetime", fn: DateFormat, in: NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{dt}, nil), format: "%Y-%m-%d %H:%i:%s", want: "2024-02-29 12:34:56"},
			{name: "time format", fn: TimeFormat, in: NewFunctionTestInput(types.T_time.ToTypeWithScale(6), []types.Time{tm}, nil), format: "%H:%i:%s.%f", want: "12:34:56.123456"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				format := NewFunctionTestConstInput(types.T_varchar.ToType(), []string{tc.format}, nil)
				caseDef := NewFunctionTestCase(proc, []FunctionTestInput{tc.in, format},
					NewFunctionTestResult(types.T_varchar.ToType(), false, []string{tc.want}, []bool{false}), tc.fn)
				ok, info := caseDef.Run()
				require.True(t, ok, info)
			})
		}
	})
}

func TestTemporalCompatibilityHelperDomains(t *testing.T) {
	dt, err := types.ParseDatetime("2024-02-29 12:34:56.123456", 6)
	require.NoError(t, err)
	tm := types.TimeFromClock(false, 12, 34, 56, 123456)
	for _, unit := range []string{
		"microsecond", "second", "minute", "hour", "day", "week", "month", "quarter", "year",
		"second_microsecond", "minute_microsecond", "minute_second", "hour_microsecond", "hour_second", "hour_minute",
		"day_microsecond", "day_second", "day_minute", "day_hour", "year_month",
	} {
		t.Run("datetime/"+unit, func(t *testing.T) {
			value, err := extractNumericFromDatetime(unit, dt)
			require.NoError(t, err)
			if want, ok := map[string]int64{
				"year": 2024, "month": 2, "day": 29, "hour": 12,
				"minute_second": 3456, "hour_second": 123456,
				"day_second": 29123456, "day_microsecond": 29123456123456,
				"year_month": 202402,
			}[unit]; ok {
				require.Equal(t, want, value)
			}
		})
		if unit == "day" || unit == "week" || unit == "month" || unit == "quarter" || unit == "year" || unit == "year_month" {
			continue
		}
		t.Run("time/"+unit, func(t *testing.T) {
			value, err := extractNumericFromTime(unit, tm)
			require.NoError(t, err)
			if want, ok := map[string]int64{
				"hour_second": 123456, "minute_second": 3456,
				"day_microsecond": 123456123456, "day_hour": 12,
			}[unit]; ok {
				require.Equal(t, want, value)
			}
		})
	}
	for _, tc := range []struct {
		value types.Time
		unit  string
		want  int64
	}{
		{types.TimeFromClock(false, 1, 2, 3, 4), "hour_second", 10203},
		{types.TimeFromClock(false, 1, 2, 3, 4), "minute_second", 203},
		{types.TimeFromClock(false, 1, 2, 3, 4), "hour_microsecond", 10203000004},
		{types.TimeFromClock(true, 0, 2, 3, 0), "hour_second", -203},
		{types.TimeFromClock(true, 0, 0, 0, 4), "second_microsecond", -4},
	} {
		got, err := extractNumericFromTime(tc.unit, tc.value)
		require.NoError(t, err)
		require.Equal(t, tc.want, got)
	}

	for _, format := range []string{"%d/%m/%Y", "%Y%m%d", "%Y", "%Y-%m-%d", "%Y-%m-%d %H:%i:%s", "%Y/%m/%d", "%Y/%m/%d %H:%i:%s"} {
		t.Run("date-format/"+format, func(t *testing.T) {
			var buf bytes.Buffer
			operator := dateFormatOperator(format)
			isNull, err := operator(context.Background(), dt, format, &buf)
			require.NoError(t, err)
			require.False(t, isNull)
			require.NotEmpty(t, buf.String())
		})
	}

	for _, unit := range []string{"microsecond", "second", "minute", "hour", "day", "week", "month", "quarter", "year"} {
		require.Equal(t, unit == "microsecond" || unit == "second" || unit == "minute" || unit == "hour", extractUnitPrefersTime(unit))
	}
	_, overflow, err := doTimeAdd(tm, 1, types.Second)
	require.NoError(t, err)
	require.False(t, overflow)
	_, overflow, err = doTimeAdd(tm, int64(types.MaxHourInTime+1), types.Hour)
	require.NoError(t, err)
	require.True(t, overflow)
}

func TestTimeIntervalOverflowContract(t *testing.T) {
	max := types.TimeFromClock(false, 838, 59, 59, 0)
	for _, tc := range []struct {
		name string
		fn   fEvalFn
		diff int64
	}{
		{"date_add", TimeAdd, 1},
		{"date_sub_negative", TimeSub, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := newTmpProcess(t)
			warnings := &numericWarningSession{}
			proc.WarningSink = warnings
			input := NewFunctionTestInput(types.T_time.ToType(), []types.Time{max - types.Time(types.MicroSecsPerSec), max, types.TimeFromClock(false, 12, 0, 0, 0)}, nil)
			interval := NewFunctionTestInput(types.T_int64.ToType(), []int64{tc.diff, tc.diff, tc.diff}, nil)
			unit := NewFunctionTestConstInput(types.T_int64.ToType(), []int64{int64(types.Second)}, nil)
			want := NewFunctionTestResult(types.T_time.ToType(), false,
				[]types.Time{max, 0, types.TimeFromClock(false, 12, 0, 1, 0)},
				[]bool{false, true, false})
			caseDef := NewFunctionTestCase(proc, []FunctionTestInput{input, interval, unit}, want, tc.fn)
			ok, info := caseDef.Run()
			require.True(t, ok, info)
			require.Equal(t, []numericWarning{{code: moerr.ER_DATETIME_FUNCTION_OVERFLOW, msg: "Datetime function: time field overflow"}}, warnings.warnings)
		})
	}
	for _, tc := range []struct {
		start    types.Time
		diff     int64
		subtract bool
		overflow bool
	}{
		{0, math.MinInt64, true, true},
		{0, math.MinInt64, false, true},
		{-max, 1, true, true},
		{-max, -1, false, true},
		{max, -1, true, true},
		{max, 0, false, false},
	} {
		_, overflow, err := doTimeInterval(tc.start, tc.diff, types.MicroSecond, tc.subtract)
		require.NoError(t, err)
		require.Equal(t, tc.overflow, overflow, "%+v", tc)
	}
}

func TestRawTimeIntervalDistinguishesNullInvalidAndOverflow(t *testing.T) {
	proc := newTmpProcess(t)
	warnings := &numericWarningSession{}
	proc.WarningSink = warnings
	noon := types.TimeFromClock(false, 12, 0, 0, 0)
	max := types.TimeFromClock(false, 838, 59, 59, 0)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_time.ToType(), []types.Time{noon, 0, noon, noon, max},
			[]bool{false, true, false, false, false}),
		NewFunctionTestInput(types.T_varchar.ToType(),
			[]string{"10000000000000.0", "10000000000000.0", "bad", "1.5", "1.5"}, nil),
		NewFunctionTestConstInput(types.T_int64.ToType(), []int64{int64(types.Second)}, nil),
	}
	want := NewFunctionTestResult(types.T_time.ToTypeWithScale(6), false,
		[]types.Time{0, 0, 0, types.TimeFromClock(false, 12, 0, 1, 500000), 0},
		[]bool{true, true, true, false, true})
	caseDef := NewFunctionTestCase(proc, inputs, want, TimeAddRaw)
	ok, info := caseDef.Run()
	require.True(t, ok, info)
	require.Equal(t, []numericWarning{
		{code: moerr.ER_DATETIME_FUNCTION_OVERFLOW, msg: "Datetime function: time field overflow"},
		{code: moerr.ER_DATETIME_FUNCTION_OVERFLOW, msg: "Datetime function: time field overflow"},
	}, warnings.warnings)
}

func TestRawNumericMicrosecondRoundingAndBounds(t *testing.T) {
	for _, tc := range []struct {
		text  string
		value int64
		state timeIntervalState
	}{
		{"1.5", 2, timeIntervalValid},
		{"-1.5", -2, timeIntervalValid},
		{"0.49", 0, timeIntervalValid},
		{"-0.5", -1, timeIntervalValid},
		{"9223372036854775807.4", math.MaxInt64, timeIntervalValid},
		{"9223372036854775807.5", 0, timeIntervalOverflow},
		{"-9223372036854775808.4", math.MinInt64, timeIntervalValid},
		{"-9223372036854775808.5", 0, timeIntervalOverflow},
	} {
		got := roundedRawMicroseconds(tc.text)
		require.Equal(t, tc.state, got.state, tc.text)
		if tc.state == timeIntervalValid {
			require.Equal(t, tc.value, got.value, tc.text)
		}
	}
}

func TestDynamicIntervalUnitContract(t *testing.T) {
	proc := newTmpProcess(t)
	for _, tc := range []struct {
		unit types.IntervalType
		want int64
	}{
		{types.Second, types.MicroSecsPerSec},
		{types.Minute, types.MicroSecsPerSec * types.SecsPerMinute},
		{types.Hour, types.MicroSecsPerSec * types.SecsPerHour},
		{types.Day, types.MicroSecsPerSec * types.SecsPerDay},
	} {
		inputs := []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"1", "bad"}, nil),
			NewFunctionTestConstInput(types.T_int64.ToType(), []int64{int64(tc.unit)}, nil),
		}
		want := NewFunctionTestResult(types.T_int64.ToType(), false,
			[]int64{tc.want, 0}, []bool{false, true})
		caseDef := NewFunctionTestCase(proc, inputs, want, ToIntervalMicrosecond)
		ok, info := caseDef.Run()
		require.True(t, ok, "unit=%v: %s", tc.unit, info)
	}
	legacy := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"1"}, nil),
		NewFunctionTestConstInput(types.T_int64.ToType(), []int64{int64(types.Second)}, nil),
	}
	legacyCase := NewFunctionTestCase(proc, legacy,
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1}, nil), ToInterval)
	ok, info := legacyCase.Run()
	require.True(t, ok, info)
}

func TestScalarDoubleIntervalMatchesLiteralRounding(t *testing.T) {
	proc := newTmpProcess(t)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_float64.ToType(), []float64{34410126.8315485, -34410126.8315485}, nil),
		NewFunctionTestConstInput(types.T_int64.ToType(), []int64{int64(types.Second)}, nil),
	}
	want := NewFunctionTestResult(types.T_int64.ToType(), false,
		[]int64{34410126831548, -34410126831548}, nil)
	caseDef := NewFunctionTestCase(proc, inputs, want, ToIntervalMicrosecond)
	ok, info := caseDef.Run()
	require.True(t, ok, info)
}

var temporalExtractBenchmarkSink int64

func BenchmarkTemporalNumericExtract(b *testing.B) {
	dates := make([]types.Date, 4096)
	times := make([]types.Time, 4096)
	for i := range dates {
		dates[i] = types.DateFromCalendar(2000, 1, 1) + types.Date(i*37)
		times[i] = types.TimeFromClock(i%2 == 0, uint64(i%839), uint8(i%60), uint8((i*7)%60), uint32(i%1000000))
	}
	b.Run("date_year", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			v, err := extractNumericFromDatetime("year", dates[i&4095].ToDatetime())
			if err != nil {
				b.Fatal(err)
			}
			temporalExtractBenchmarkSink = v
		}
	})
	b.Run("time_hour_microsecond", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			v, err := extractNumericFromTime("hour_microsecond", times[i&4095])
			if err != nil {
				b.Fatal(err)
			}
			temporalExtractBenchmarkSink = v
		}
	})
}

func TestTemporalCompatibilityPeriodAndUnixDomains(t *testing.T) {
	proc := newTmpProcess(t)
	format := NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"%Y-%m-%d %H:%i:%s"}, nil)
	for _, tc := range []struct {
		name  string
		in    FunctionTestInput
		fn    fEvalFn
		want  []string
		nulls []bool
	}{
		{"unix-int", NewFunctionTestInput(types.T_int64.ToType(), []int64{0, 1, -1}, []bool{false, false, true}), FromUnixTimeInt64Format, []string{"1970-01-01 00:00:00", "1970-01-01 00:00:01", ""}, []bool{false, false, true}},
		{"unix-uint", NewFunctionTestInput(types.T_uint64.ToType(), []uint64{0, 1}, nil), FromUnixTimeUint64Format, []string{"1970-01-01 00:00:00", "1970-01-01 00:00:01"}, nil},
		{"unix-float", NewFunctionTestInput(types.T_float64.ToType(), []float64{0, 1.5}, nil), FromUnixTimeFloat64Format, []string{"1970-01-01 00:00:00", "1970-01-01 00:00:01"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := NewFunctionTestResult(types.T_varchar.ToType(), false, tc.want, tc.nulls)
			caseDef := NewFunctionTestCase(proc, []FunctionTestInput{tc.in, format}, result, tc.fn)
			ok, info := caseDef.Run()
			require.True(t, ok, info)
		})
	}

	for _, tc := range []struct {
		name string
		fn   fEvalFn
	}{
		{"period-add-signed", PeriodAdd},
		{"period-add-unsigned", PeriodAdd},
		{"period-add-float", PeriodAdd},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var period FunctionTestInput
			var months FunctionTestInput
			switch tc.name {
			case "period-add-signed":
				period = NewFunctionTestInput(types.T_int64.ToType(), []int64{200801, 9912}, nil)
				months = NewFunctionTestInput(types.T_int64.ToType(), []int64{2, 1}, nil)
			case "period-add-unsigned":
				period = NewFunctionTestInput(types.T_uint64.ToType(), []uint64{200801}, nil)
				months = NewFunctionTestInput(types.T_uint64.ToType(), []uint64{2}, nil)
			default:
				period = NewFunctionTestInput(types.T_int64.ToType(), []int64{200801}, nil)
				months = NewFunctionTestInput(types.T_float64.ToType(), []float64{2.9}, nil)
			}
			want := []int64{200803}
			if tc.name == "period-add-signed" {
				want = []int64{200803, 200001}
			}
			caseDef := NewFunctionTestCase(proc, []FunctionTestInput{period, months},
				NewFunctionTestResult(types.T_int64.ToType(), false, want, nil), tc.fn)
			ok, info := caseDef.Run()
			require.True(t, ok, info)
		})
	}

	periodDiff := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{200802, 200801}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{200703, 200801}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{11, 0}, nil), PeriodDiff)
	ok, info := periodDiff.Run()
	require.True(t, ok, info)
}

func TestTemporalCompatibilityErrorAndBoundaryHelpers(t *testing.T) {
	dt, err := types.ParseDatetime("2024-02-29 12:34:56.123456", 6)
	require.NoError(t, err)
	date, err := types.ParseDateCast("2024-02-29")
	require.NoError(t, err)
	tm := types.TimeFromClock(false, 12, 34, 56, 123456)

	// Exercise the invalid and overflow branches used by DATE_SUB/SUBTIME.
	_, err = doDateSub(types.ZeroDate, 1, types.Day)
	require.Error(t, err)
	_, err = doDateSub(date, math.MaxInt64, types.Day)
	require.Error(t, err)
	_, err = doDateStringSub("12:34:56", 1, types.Day)
	require.Error(t, err)
	_, err = doDateStringSub("not-a-date", 1, types.Day)
	require.Error(t, err)
	_, err = doDatetimeSub(types.ZeroDatetime, 1, types.Day)
	require.Error(t, err)
	_, err = doDatetimeSub(dt, math.MaxInt64, types.Day)
	require.Error(t, err)
	_, overflow, err := doTimeSub(tm, math.MaxInt64, types.Second)
	require.NoError(t, err)
	require.True(t, overflow)
	_, overflow, err = doTimeSub(types.TimeFromClock(false, 0, 0, 0, 0), int64(types.MaxHourInTime+1), types.Hour)
	require.NoError(t, err)
	require.True(t, overflow)

	// These parsers deliberately distinguish calendar strings from durations.
	_, _, kind, err := parseTemporalString("12:34:56", 6)
	require.NoError(t, err)
	require.Equal(t, temporalStringTime, kind)
	_, _, kind, err = parseTemporalString("2024-02-29 12:34:56", 6)
	require.NoError(t, err)
	require.Equal(t, temporalStringDateTime, kind)
	for _, input := range []string{"2024-2-29 12:34:56", "20240229123456.123456", "2024.2.29", "2024@2@29"} {
		_, _, kind, err = parseTemporalString(input, 6)
		require.NoError(t, err)
		require.Equal(t, temporalStringDateTime, kind)
		_, err = parseTimeOperand(input, 6)
		require.Error(t, err)
	}
	_, _, kind, err = parseTemporalString("2024-2-30 12:34:56", 6)
	require.Error(t, err)
	require.Equal(t, temporalStringInvalid, kind)
	_, duration, kind, err := parseTemporalString("00000123456", 6)
	require.NoError(t, err)
	require.Equal(t, temporalStringTime, kind)
	require.Equal(t, types.TimeFromClock(false, 12, 34, 56, 0), duration)
	_, err = parseTimeOperand("00000123456", 6)
	require.NoError(t, err)
	_, duration, kind, err = parseTemporalString("1234.5", 6)
	require.NoError(t, err)
	require.Equal(t, temporalStringTime, kind)
	require.Equal(t, types.TimeFromClock(false, 0, 12, 34, 500000), duration)
	_, err = parseTimeOperand("1234.5", 6)
	require.NoError(t, err)
	_, err = parseTimeOperand("2024-02-29", 6)
	require.Error(t, err)
	_, err = parseTimeOperand("bad", 6)
	require.Error(t, err)

	for _, tc := range []struct {
		value int64
		want  bool
	}{
		{0, false}, {int64(types.MySQLTimeMaxForScale(0)) / types.MicroSecsPerSec, false},
		{int64(types.MySQLTimeMaxForScale(0))/types.MicroSecsPerSec + 1, true},
		{-int64(types.MySQLTimeMaxForScale(0))/types.MicroSecsPerSec - 1, true},
	} {
		_, warning := secToTimeFromInt64(tc.value)
		require.Equal(t, tc.want, warning)
	}
	_, warning := secToTimeFromUint64(uint64(types.MySQLTimeMaxForScale(0))/types.MicroSecsPerSec + 1)
	require.True(t, warning)
	_, warning, _ = secToTimeFromFloat64(math.NaN())
	require.True(t, warning)
	_, warning, clampedFlag := secToTimeFromFloat64(math.Inf(1))
	require.False(t, warning)
	require.True(t, clampedFlag)

	for _, tc := range []struct {
		value int64
		want  int64
	}{
		{200802, 2008*12 + 2},
		{9912, 1999*12 + 12},
	} {
		year, month, err := parsePeriod(tc.value)
		require.NoError(t, err)
		require.Equal(t, tc.want, int64(year)*12+int64(month))
	}
	_, _, err = parsePeriod(13)
	require.Error(t, err)
	_, _, err = parsePeriod(200813)
	require.Error(t, err)

	require.Equal(t, types.MySQLTimeFunctionMaxForScale(6), signedMySQLTimeFunctionMax(false))
	require.Equal(t, -types.MySQLTimeFunctionMaxForScale(6), signedMySQLTimeFunctionMax(true))
	diff, err := timeDiff(types.TimeFromClock(false, 838, 59, 59, 0), -tm)
	require.NoError(t, err)
	require.Equal(t, types.MySQLTimeMaxForScale(6), diff)
	for _, tc := range []struct {
		first, second types.Time
		subtract      bool
		want          types.Time
	}{
		{types.TimeFromClock(false, 2_000_000_000, 0, 0, 0), types.TimeFromClock(false, 2_000_000_000, 0, 0, 0), false, types.MySQLTimeMaxForScale(6)},
		{types.TimeFromClock(false, 2_000_000_000, 0, 0, 0), types.TimeFromClock(true, 2_000_000_000, 0, 0, 0), true, types.MySQLTimeMaxForScale(6)},
		{types.TimeFromClock(true, 2_000_000_000, 0, 0, 0), types.TimeFromClock(true, 2_000_000_000, 0, 0, 0), false, -types.MySQLTimeMaxForScale(6)},
	} {
		got, _, truncated := timeArithmeticResult(tc.first, tc.second, tc.subtract, 6)
		require.True(t, truncated, "first=%d second=%d subtract=%t got=%d", tc.first, tc.second, tc.subtract, got)
		require.Equal(t, tc.want, got)
	}
}

func TestTemporalCompatibilitySelectListAndNullBranches(t *testing.T) {
	proc := newTmpProcess(t)

	// The interval kernels have separate all-row, masked-row, NULL, and
	// invalid-interval paths. Keep those paths covered independently of the
	// SQL planner's vector selection.
	tm := types.TimeFromClock(false, 1, 2, 3, 0)
	for _, tc := range []struct {
		name string
		fn   fEvalFn
	}{
		{"time-add", TimeAdd},
		{"time-sub", TimeSub},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := NewFunctionTestInput(types.T_time.ToType(), []types.Time{tm, tm, tm, tm}, []bool{false, true, false, false})
			interval := NewFunctionTestInput(types.T_int64.ToType(), []int64{1, 1, math.MaxInt64, 1}, []bool{false, false, false, false})
			unit := NewFunctionTestConstInput(types.T_int64.ToType(), []int64{int64(types.Second)}, nil)
			result := NewFunctionTestResult(types.T_time.ToType(), false,
				[]types.Time{types.TimeFromClock(false, 1, 2, 4, 0), 0, 0, 0},
				[]bool{false, true, true, true})
			if tc.name == "time-sub" {
				result = NewFunctionTestResult(types.T_time.ToType(), false,
					[]types.Time{types.TimeFromClock(false, 1, 2, 2, 0), 0, 0, 0},
					[]bool{false, true, true, true})
			}
			caseDef := NewFunctionTestCase(proc, []FunctionTestInput{input, interval, unit}, result, tc.fn).
				WithSelectList(&FunctionSelectList{AnyNull: true, SelectList: []bool{true, true, true, false}})
			ok, info := caseDef.Run()
			require.True(t, ok, info)
		})
	}

	format := NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"%Y"}, nil)
	for _, tc := range []struct {
		name string
		in   FunctionTestInput
		fn   fEvalFn
	}{
		{"unix-int", NewFunctionTestInput(types.T_int64.ToType(), []int64{0, -1, maxUnixTimestampInt + 1}, []bool{false, false, false}), FromUnixTimeInt64Format},
		{"unix-uint", NewFunctionTestInput(types.T_uint64.ToType(), []uint64{0, maxUnixTimestampInt + 1, maxUnixTimestampInt + 1}, nil), FromUnixTimeUint64Format},
		{"unix-float", NewFunctionTestInput(types.T_float64.ToType(), []float64{0, math.NaN(), math.NaN()}, nil), FromUnixTimeFloat64Format},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"1970", "", ""}, []bool{false, true, true})
			caseDef := NewFunctionTestCase(proc, []FunctionTestInput{tc.in, format}, result, tc.fn).
				WithSelectList(&FunctionSelectList{AnyNull: true, SelectList: []bool{true, false, true}})
			ok, info := caseDef.Run()
			require.True(t, ok, info)
		})
	}
}

// Exercise the row-level null, invalid-input, timestamp, and string-domain
// branches which are not reached by the compact compatibility matrix above.
// These branches are part of the changed temporal execution contract and must
// remain covered by the PR coverage gate.
func TestTemporalCompatibilityAdditionalExecutionBranches(t *testing.T) {
	proc := newTmpProcess(t)

	t.Run("string add and sub domains", func(t *testing.T) {
		left := NewFunctionTestInput(types.T_varchar.ToType(), []string{
			"12:00:00", "2024-02-29 12:00:00", "bad", "838:59:59",
		}, nil)
		right := NewFunctionTestInput(types.T_varchar.ToType(), []string{
			"01:00:00", "01:00:00", "01:00:00", "00:00:01",
		}, nil)

		for _, tc := range []struct {
			name string
			fn   fEvalFn
			want []string
			null []bool
		}{
			{name: "add", fn: AddTime, want: []string{"13:00:00", "2024-02-29 13:00:00", "", "838:59:59"}, null: []bool{false, false, true, false}},
			{name: "sub", fn: SubTime, want: []string{"11:00:00", "2024-02-29 11:00:00", "", "838:59:58"}, null: []bool{false, false, true, false}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				caseDef := NewFunctionTestCase(proc, []FunctionTestInput{left, right},
					NewFunctionTestResult(types.T_varchar.ToType(), false, tc.want, tc.null), tc.fn)
				ok, info := caseDef.Run()
				require.True(t, ok, info)
			})
		}
	})

	t.Run("typed timestamp null and invalid branches", func(t *testing.T) {
		dt, err := types.ParseDatetime("2024-02-29 12:00:00", 0)
		require.NoError(t, err)
		ts := dt.ToTimestamp(time.UTC)
		left := NewFunctionTestInput(types.T_timestamp.ToType(), []types.Timestamp{ts, types.ZeroTimestamp, ts}, []bool{false, false, false})
		right := NewFunctionTestInput(types.T_varchar.ToType(), []string{"01:00:00", "01:00:00", "bad"}, nil)
		want := (dt + types.Datetime(3600*types.MicroSecsPerSec)).ToTimestamp(time.UTC)
		caseDef := NewFunctionTestCase(proc, []FunctionTestInput{left, right},
			NewFunctionTestResult(types.T_timestamp.ToType(), false, []types.Timestamp{want, 0, 0}, []bool{false, true, true}), AddTime)
		ok, info := caseDef.Run()
		require.True(t, ok, info)
	})

	t.Run("time format dynamic and null calendar fields", func(t *testing.T) {
		values := NewFunctionTestInput(types.T_time.ToTypeWithScale(6), []types.Time{
			types.TimeFromClock(false, 0, 1, 2, 3),
			types.TimeFromClock(false, 13, 14, 15, 160000),
			0,
		}, []bool{false, false, true})
		formats := NewFunctionTestInput(types.T_varchar.ToType(), []string{"%H:%i:%s.%f", "%W", "%H"}, []bool{false, false, false})
		caseDef := NewFunctionTestCase(proc, []FunctionTestInput{values, formats},
			NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"00:01:02.000003", "", ""}, []bool{false, true, true}), TimeFormat)
		ok, info := caseDef.Run()
		require.True(t, ok, info)
	})
}

func TestTemporalCompatibilityAdditionalHelperBranches(t *testing.T) {
	dt, err := types.ParseDatetime("2024-02-29 12:34:56.123456", 6)
	require.NoError(t, err)
	tm := types.TimeFromClock(false, 13, 14, 15, 160000)

	// Cover the string extractor's time-first, date-first, zero-date, empty,
	// and invalid fallbacks directly as well as through the vector wrappers.
	for _, tc := range []struct {
		unit  string
		value string
		want  int64
		err   bool
	}{
		{unit: "microsecond", value: "12:34:56.123456", want: 123456},
		{unit: "year", value: "2024-02-29", want: 2024},
		{unit: "week", value: "0000-00-00 00:00:00", want: 0},
		{unit: "hour", value: "", want: 0},
		{unit: "hour", value: "not-a-temporal", err: true},
	} {
		got, gotErr := extractNumericFromVarchar(tc.unit, tc.value, 6)
		if tc.err {
			require.Error(t, gotErr)
		} else {
			require.NoError(t, gotErr)
			require.Equal(t, tc.want, got)
		}
	}

	// Exercise every TIME_FORMAT branch, including calendar-only specifiers
	// which must return NULL for a TIME value.
	for _, format := range []string{"%f", "%H", "%k", "%h", "%I", "%i", "%l", "%p", "%r", "%S", "%s", "%T", "%Y", "%W", "literal"} {
		var buf bytes.Buffer
		isNull, err := timeFormat(context.Background(), tm, format, &buf)
		require.NoError(t, err)
		if format == "%W" {
			require.True(t, isNull)
		} else {
			require.False(t, isNull)
			require.NotEmpty(t, buf.String())
		}
	}

	appendTimeRangeWarning(nil, tm, 6)
	require.True(t, validDatetimeResult(dt))
	require.False(t, validDatetimeResult(types.DatetimeEpoch-1))
	require.False(t, validDatetimeResult(types.DatetimeFromClock(types.MaxDatetimeYear, 12, 31, 23, 59, 59, 999999)+1))
	require.Equal(t, 7, normalizeWeekMode(-1))
	require.Equal(t, 1, normalizeWeekMode(9))
	defaultMode, err := getDefaultWeekFormatMode(nil)
	require.NoError(t, err)
	require.Zero(t, defaultMode)

	// Trigger both the normal and overflow paths in TIMEDIFF's generic helper.
	got, err := timeDiff(tm, types.TimeFromClock(false, 12, 0, 0, 0))
	require.NoError(t, err)
	require.Equal(t, types.TimeFromClock(false, 1, 14, 15, 160000), got)
	got, err = timeDiff(types.Time(math.MaxInt64), types.Time(-math.MaxInt64))
	require.NoError(t, err)
	require.Equal(t, types.MySQLTimeMaxForScale(6), got)
}

// The eight-digit overlap is decided by an accepted calendar parse, not by
// width alone. These exact values intentionally retain MatrixOne's compact
// calendar precedence even where MySQL's ADDTIME treats the text as a TIME.
func TestTemporalCompatibilityCompactOperandMatrix(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  string
	}{
		{"1-1-1", "0001-01-01 00:00:00"},
		{"24-2-29", "2024-02-29 00:00:00"},
		{"123-2-3", "0123-02-03 00:00:00"},
		{"69-1-1", "2069-01-01 00:00:00"},
		{"70-1-1", "1970-01-01 00:00:00"},
		{"24/2/29", "2024-02-29 00:00:00"},
	} {
		t.Run("short-calendar/"+tc.input, func(t *testing.T) {
			dt, _, kind, err := parseTemporalString(tc.input, 6)
			require.NoError(t, err)
			require.Equal(t, temporalStringDateTime, kind)
			require.Equal(t, tc.want, dt.String2(0))
			_, err = parseTimeOperand(tc.input, 6)
			require.Error(t, err)
		})
	}
	for _, clock := range []string{
		"10:11:12", "01:02:03", "12:34.56", "01:02.03",
		"1234:56:07", "1234:56.7", "2024:02:29",
	} {
		t.Run("clock/"+clock, func(t *testing.T) {
			_, got, kind, err := parseTemporalString(clock, 6)
			require.NoError(t, err)
			require.Equal(t, temporalStringTime, kind)
			want, err := types.ParseTime(clock, 6)
			require.NoError(t, err)
			require.Equal(t, want, got)
			operand, err := parseTimeOperand(clock, 6)
			require.NoError(t, err)
			require.Equal(t, want, operand)
		})
	}
	for _, tc := range []struct {
		input string
		kind  temporalStringKind
		want  types.Time
	}{
		{"1234", temporalStringTime, types.TimeFromClock(false, 0, 12, 34, 0)},
		{"0001234", temporalStringTime, types.TimeFromClock(false, 0, 12, 34, 0)},
		{"00001234", temporalStringTime, types.TimeFromClock(false, 0, 12, 34, 0)},
		{"00001234.5", temporalStringTime, types.TimeFromClock(false, 0, 12, 34, 500000)},
		{"00001234.123456", temporalStringTime, types.TimeFromClock(false, 0, 12, 34, 123456)},
		{"00000000.5", temporalStringTime, types.TimeFromClock(false, 0, 0, 0, 500000)},
		{"00010101.5", temporalStringTime, types.TimeFromClock(false, 1, 1, 1, 500000)},
		{"00000101", temporalStringTime, types.TimeFromClock(false, 0, 1, 1, 0)},
		{"08385959", temporalStringTime, types.TimeFromClock(false, 838, 59, 59, 0)},
		{"00000123456", temporalStringTime, types.TimeFromClock(false, 12, 34, 56, 0)},
		{"-00001234", temporalStringTime, types.TimeFromClock(true, 0, 12, 34, 0)},
		{"00010101", temporalStringDateTime, 0},
		{"00000000", temporalStringDateTime, 0},
		{"20240229", temporalStringDateTime, 0},
		{"2024.2.29", temporalStringDateTime, 0},
		{"20240229123456.123456", temporalStringDateTime, 0},
	} {
		t.Run(tc.input, func(t *testing.T) {
			_, got, kind, err := parseTemporalString(tc.input, 6)
			require.NoError(t, err)
			require.Equal(t, tc.kind, kind)
			if kind == temporalStringTime {
				require.Equal(t, tc.want, got)
				duration, err := parseTimeOperand(tc.input, 6)
				require.NoError(t, err)
				require.Equal(t, tc.want, duration)
			} else {
				_, err = parseTimeOperand(tc.input, 6)
				require.Error(t, err)
			}
		})
	}
	for _, input := range []string{
		"20240230", "00000000001234", "2024-2-30 12:34:56", "24-2-30",
		"00001234.bad", "00001299", "00008360", "12:99.56",
	} {
		t.Run("invalid/"+input, func(t *testing.T) {
			_, _, kind, err := parseTemporalString(input, 6)
			require.Error(t, err)
			require.Equal(t, temporalStringInvalid, kind)
			_, err = parseTimeOperand(input, 6)
			require.Error(t, err)
		})
	}
}

// A zero calendar does not imply a zero clock. Exercise every EXTRACT field
// family against its exact numeric value, including the day-prefixed clocks.
func TestTemporalCompatibilityZeroCalendarClockMatrix(t *testing.T) {
	clock := "0000-00-00 12:34:56.123456"
	for _, tc := range []struct {
		unit string
		want int64
	}{
		{"year", 0}, {"month", 0}, {"day", 0}, {"quarter", 0},
		{"week", 0}, {"year_month", 0},
		{"hour", 12}, {"minute", 34}, {"second", 56}, {"microsecond", 123456},
		{"second_microsecond", 56123456}, {"minute_microsecond", 3456123456},
		{"minute_second", 3456}, {"hour_microsecond", 123456123456},
		{"hour_second", 123456}, {"hour_minute", 1234},
		{"day_microsecond", 123456123456}, {"day_second", 123456},
		{"day_minute", 1234}, {"day_hour", 12},
	} {
		t.Run(tc.unit, func(t *testing.T) {
			got, err := extractNumericFromVarchar(tc.unit, clock, 6)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
			for _, zero := range []string{"0000-00-00", "0000-00-00 00:00:00.000000"} {
				got, err = extractNumericFromVarchar(tc.unit, zero, 6)
				require.NoError(t, err)
				require.Zero(t, got)
			}
		})
	}
	for _, value := range []string{
		"0000-00-00 24:00:00", "0000-00-00 12:60:00",
		"0000-00-00 12:34:60", "0000-00-00 12:34:56.bad",
		"0000-00-00 12:34:56tail",
	} {
		_, err := extractNumericFromVarchar("hour", value, 6)
		require.Error(t, err, value)
	}
	got, err := extractNumericFromVarchar("second", "0000-00-00 12:34:56.9999999", 6)
	require.NoError(t, err)
	require.Equal(t, int64(57), got)
	for _, unit := range []string{"hour", "second_microsecond", "day_second"} {
		got, err := extractNumericFromVarchar(unit, "0000-00-00 12.34.56.123456", 6)
		require.NoError(t, err)
		switch unit {
		case "hour":
			require.Equal(t, int64(12), got)
		case "second_microsecond":
			require.Equal(t, int64(56123456), got)
		case "day_second":
			require.Equal(t, int64(123456), got)
		}
	}
	for _, tc := range []struct {
		clock string
		want  int64
	}{
		{"12.34", 0},
		{"12:34.56", 56000000},
		{"12.34.56.123456", 56123456},
	} {
		got, err := extractNumericFromVarchar("second_microsecond", "0000-00-00 "+tc.clock, 6)
		require.NoError(t, err, tc.clock)
		require.Equal(t, tc.want, got, tc.clock)
	}

	for _, tc := range []struct {
		unit string
		want int64
	}{
		{"year", 2024}, {"month", 2}, {"day", 29}, {"year_month", 202402},
		{"hour", 12}, {"second_microsecond", 56123456},
		{"day_hour", 2912}, {"day_minute", 291234},
		{"day_second", 29123456}, {"day_microsecond", 29123456123456},
	} {
		got, err := extractNumericFromVarchar(tc.unit, "2024-02-29 12:34:56.123456", 6)
		require.NoError(t, err, tc.unit)
		require.Equal(t, tc.want, got, tc.unit)
	}
}
