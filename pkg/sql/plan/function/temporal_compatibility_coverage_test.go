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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// These cases exercise the compatibility branches added for runtime temporal
// formats and for the typed/string temporal arithmetic matrix.  The BVT cases
// validate the public SQL contract; this test keeps the changed execution
// branches covered by the unit-test coverage gate as well.
func TestTemporalCompatibilityExecutionMatrix(t *testing.T) {
	proc := newTmpProcess(t)

	t.Run("dynamic str_to_date domains", func(t *testing.T) {
		input := NewFunctionTestInput(types.T_varchar.ToType(), []string{
			"12:34:56.123456", "2024-02-29", "2024-02-29 12:34:56.123456",
			"bad", "2024-02-29", "12:34:56",
		}, []bool{false, false, false, false, true, false})
		format := NewFunctionTestInput(types.T_varchar.ToType(), []string{
			"%H:%i:%s.%f", "%Y-%m-%d", "%Y-%m-%d %H:%i:%s.%f",
			"%Y-%m-%d", "%Y-%m-%d", "%Y-%m-%d",
		}, nil)
		want := NewFunctionTestResult(types.New(types.T_varchar, 29, 6), false,
			[]string{"12:34:56.123456", "2024-02-29", "2024-02-29 12:34:56.123456", "", "", ""},
			[]bool{false, false, false, true, true, true})
		caseDef := NewFunctionTestCase(proc, []FunctionTestInput{input, format}, want, builtInStrToDateDynamic)
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
	for _, tc := range []struct {
		format         string
		isTime, isDate bool
		scale          int
	}{
		{"%H:%i:%s.%f", true, false, 6},
		{"%Y-%m-%d", false, true, 0},
		{"%Y-%m-%d %H:%i:%s.%f", true, true, 6},
		{"%W, %M %d, %Y", false, true, 0},
		{"%r", true, false, 0},
	} {
		t.Run(tc.format, func(t *testing.T) {
			isTime, isDate, scale := dynamicStrToDateFormatType(tc.format)
			require.Equal(t, tc.isTime, isTime)
			require.Equal(t, tc.isDate, isDate)
			require.Equal(t, tc.scale, scale)
		})
	}

	dt, err := types.ParseDatetime("2024-02-29 12:34:56.123456", 6)
	require.NoError(t, err)
	tm := types.TimeFromClock(false, 12, 34, 56, 123456)
	for _, unit := range []string{
		"microsecond", "second", "minute", "hour", "day", "week", "month", "quarter", "year",
		"second_microsecond", "minute_microsecond", "minute_second", "hour_microsecond", "hour_second", "hour_minute",
	} {
		t.Run("datetime/"+unit, func(t *testing.T) {
			value, err := extractFromDatetime(unit, dt)
			require.NoError(t, err)
			require.NotEmpty(t, value)
		})
		if unit == "day" || unit == "week" || unit == "month" || unit == "quarter" || unit == "year" {
			continue
		}
		t.Run("time/"+unit, func(t *testing.T) {
			value, err := extractFromTime(unit, tm)
			require.NoError(t, err)
			require.NotEmpty(t, value)
		})
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
	_, err = doTimeAdd(tm, 1, types.Second)
	require.NoError(t, err)
	_, err = doTimeAdd(tm, int64(types.MaxHourInTime+1), types.Hour)
	require.Error(t, err)
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
	_, err = doTimeSub(tm, math.MaxInt64, types.Second)
	require.Error(t, err)
	clamped, err := doTimeSub(types.TimeFromClock(false, 0, 0, 0, 0), int64(types.MaxHourInTime+1), types.Hour)
	require.NoError(t, err)
	require.Equal(t, -types.MySQLTimeMaxForScale(6), clamped)

	// These parsers deliberately distinguish calendar strings from durations.
	_, _, kind, err := parseTemporalString("12:34:56", 6)
	require.NoError(t, err)
	require.Equal(t, temporalStringTime, kind)
	_, _, kind, err = parseTemporalString("2024-02-29 12:34:56", 6)
	require.NoError(t, err)
	require.Equal(t, temporalStringDateTime, kind)
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
	require.Equal(t, types.Time(int64(types.TimeFromClock(false, 838, 59, 59, 0))-int64(-tm)), diff)
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
