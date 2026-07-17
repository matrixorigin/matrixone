// Copyright 2026 Matrix Origin
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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestCastZeroTemporalToNumericUsesZero(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().TimeZone = time.FixedZone("UTC+8", 8*60*60)
	zeroDecimal, err := types.ParseDecimal128("0.000000", 20, 6)
	require.NoError(t, err)

	for _, tc := range []struct {
		name   string
		inputs []FunctionTestInput
		expect FunctionTestResult
	}{
		{
			name: "zero date to signed",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_date.ToType(), []types.Date{types.ZeroDate}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{}, nil),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, nil),
		},
		{
			name: "zero datetime to decimal",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_datetime.ToTypeWithScale(6), []types.Datetime{types.ZeroDatetime}, nil),
				NewFunctionTestInput(types.New(types.T_decimal128, 20, 6), []types.Decimal128{}, nil),
			},
			expect: NewFunctionTestResult(types.New(types.T_decimal128, 20, 6), false, []types.Decimal128{zeroDecimal}, nil),
		},
		{
			name: "zero timestamp to decimal",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_timestamp.ToTypeWithScale(6), []types.Timestamp{types.ZeroTimestamp}, nil),
				NewFunctionTestInput(types.New(types.T_decimal128, 20, 6), []types.Decimal128{}, nil),
			},
			expect: NewFunctionTestResult(types.New(types.T_decimal128, 20, 6), false, []types.Decimal128{zeroDecimal}, nil),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, NewCast)
			succeed, info := fcTC.Run()
			require.True(t, succeed, info)
		})
	}
}

func TestCastZeroTemporalStringsProducesNonNullSentinels(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().TimeZone = time.FixedZone("UTC+8", 8*60*60)

	for _, tc := range []struct {
		name   string
		inputs []FunctionTestInput
		expect FunctionTestResult
	}{
		{
			name: "string to zero date",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"0000-00-00"}, nil),
				NewFunctionTestInput(types.T_date.ToType(), []types.Date{}, nil),
			},
			expect: NewFunctionTestResult(types.T_date.ToType(), false, []types.Date{types.ZeroDate}, nil),
		},
		{
			name: "zero datetime string to zero date",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"0000-00-00 00:00:00"}, nil),
				NewFunctionTestInput(types.T_date.ToType(), []types.Date{}, nil),
			},
			expect: NewFunctionTestResult(types.T_date.ToType(), false, []types.Date{types.ZeroDate}, nil),
		},
		{
			name: "string to zero datetime",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"0000-00-00 00:00:00"}, nil),
				NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{}, nil),
			},
			expect: NewFunctionTestResult(types.T_datetime.ToType(), false, []types.Datetime{types.ZeroDatetime}, nil),
		},
		{
			name: "string to zero timestamp",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"0000-00-00 00:00:00"}, nil),
				NewFunctionTestInput(types.T_timestamp.ToType(), []types.Timestamp{}, nil),
			},
			expect: NewFunctionTestResult(types.T_timestamp.ToType(), false, []types.Timestamp{types.ZeroTimestamp}, nil),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, NewCast)
			succeed, info := fcTC.Run()
			require.True(t, succeed, info)
		})
	}
}

func TestUnixTimestampZeroValueReturnsNull(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().TimeZone = time.UTC
	varcharInput := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"0000-00-00 00:00:00"}, nil),
	}

	for _, tc := range []struct {
		name   string
		inputs []FunctionTestInput
		expect FunctionTestResult
		fn     fEvalFn
	}{
		{
			name: "typed timestamp",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_timestamp.ToType(), []types.Timestamp{types.ZeroTimestamp}, nil),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, []bool{true}),
			fn:     builtInUnixTimestamp,
		},
		{
			name:   "varchar int64",
			inputs: varcharInput,
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, []bool{true}),
			fn:     builtInUnixTimestampVarcharToInt64,
		},
		{
			name:   "varchar float64",
			inputs: varcharInput,
			expect: NewFunctionTestResult(types.T_float64.ToType(), false, []float64{0}, []bool{true}),
			fn:     builtInUnixTimestampVarcharToFloat64,
		},
		{
			name:   "varchar decimal128",
			inputs: varcharInput,
			expect: NewFunctionTestResult(types.New(types.T_decimal128, 38, 6), false, []types.Decimal128{{}}, []bool{true}),
			fn:     builtInUnixTimestampVarcharToDecimal128,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, tc.fn)
			succeed, info := fcTC.Run()
			require.True(t, succeed, info)
		})
	}
}

func TestZeroTemporalIntervalAndDayNumberFunctionsReturnNull(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().TimeZone = time.UTC

	for _, tc := range []struct {
		name    string
		fn      func() error
		isError func(error) bool
	}{
		{
			name: "date add",
			fn: func() error {
				_, err := doDateAdd(types.ZeroDate, 1, types.Day)
				return err
			},
			isError: isDateOverflowMaxError,
		},
		{
			name: "datetime add",
			fn: func() error {
				_, err := doDatetimeAdd(types.ZeroDatetime, 1, types.Day)
				return err
			},
			isError: isDatetimeOverflowMaxError,
		},
		{
			name: "timestamp add",
			fn: func() error {
				_, err := doTimestampAdd(time.UTC, types.ZeroTimestamp, 1, types.Day)
				return err
			},
			isError: isDatetimeOverflowMaxError,
		},
		{
			name: "date sub",
			fn: func() error {
				_, err := doDateSub(types.ZeroDate, 1, types.Day)
				return err
			},
			isError: isDatetimeOverflowMaxError,
		},
		{
			name: "datetime sub",
			fn: func() error {
				_, err := doDatetimeSub(types.ZeroDatetime, 1, types.Day)
				return err
			},
			isError: isDatetimeOverflowMaxError,
		},
		{
			name: "timestamp sub",
			fn: func() error {
				_, err := doTimestampSub(time.UTC, types.ZeroTimestamp, 1, types.Day)
				return err
			},
			isError: isDatetimeOverflowMaxError,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.True(t, tc.isError(tc.fn()))
		})
	}

	for _, tc := range []struct {
		name string
		fn   fEvalFn
	}{
		{name: "to days", fn: builtInToDays},
		{name: "to seconds", fn: builtInToSeconds},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fcTC := NewFunctionTestCase(
				proc,
				[]FunctionTestInput{
					NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{types.ZeroDatetime}, nil),
				},
				NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, []bool{true}),
				tc.fn,
			)
			succeed, info := fcTC.Run()
			require.True(t, succeed, info)
		})
	}
}

func TestZeroTemporalIntervalOperatorsReturnNull(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().TimeZone = time.UTC
	intervalType := int64(types.Day)

	for _, tc := range []struct {
		name   string
		inputs []FunctionTestInput
		expect FunctionTestResult
		fn     fEvalFn
	}{
		{
			name: "date sub",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_date.ToType(), []types.Date{types.ZeroDate}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{intervalType}, nil),
			},
			expect: NewFunctionTestResult(types.T_date.ToType(), false, []types.Date{0}, []bool{true}),
			fn:     DateSub,
		},
		{
			name: "timestamp add",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_timestamp.ToType(), []types.Timestamp{types.ZeroTimestamp}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{intervalType}, nil),
			},
			expect: NewFunctionTestResult(types.T_timestamp.ToType(), false, []types.Timestamp{0}, []bool{true}),
			fn:     TimestampAdd,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, tc.fn)
			succeed, info := fcTC.Run()
			require.True(t, succeed, info)
		})
	}
}

func TestCompleteDateFunctionsReturnNullForZeroTemporal(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().TimeZone = time.UTC

	for _, tc := range []struct {
		name   string
		input  FunctionTestInput
		expect FunctionTestResult
		fn     fEvalFn
	}{
		{name: "dayofyear date", input: NewFunctionTestInput(types.T_date.ToType(), []types.Date{types.ZeroDate}, nil), expect: NewFunctionTestResult(types.T_uint16.ToType(), false, []uint16{0}, []bool{true}), fn: DayOfYear},
		{name: "week date", input: NewFunctionTestInput(types.T_date.ToType(), []types.Date{types.ZeroDate}, nil), expect: NewFunctionTestResult(types.T_uint8.ToType(), false, []uint8{0}, []bool{true}), fn: DateToWeek},
		{name: "week datetime", input: NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{types.ZeroDatetime}, nil), expect: NewFunctionTestResult(types.T_uint8.ToType(), false, []uint8{0}, []bool{true}), fn: DatetimeToWeek},
		{name: "weekofyear date", input: NewFunctionTestInput(types.T_date.ToType(), []types.Date{types.ZeroDate}, nil), expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, []bool{true}), fn: DateToWeekOfYear},
		{name: "weekofyear datetime", input: NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{types.ZeroDatetime}, nil), expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, []bool{true}), fn: DatetimeToWeekOfYear},
		{name: "weekofyear timestamp", input: NewFunctionTestInput(types.T_timestamp.ToType(), []types.Timestamp{types.ZeroTimestamp}, nil), expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, []bool{true}), fn: TimestampToWeekOfYear},
		{name: "yearweek date", input: NewFunctionTestInput(types.T_date.ToType(), []types.Date{types.ZeroDate}, nil), expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, []bool{true}), fn: YearWeekDate},
		{name: "yearweek datetime", input: NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{types.ZeroDatetime}, nil), expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, []bool{true}), fn: YearWeekDatetime},
		{name: "yearweek timestamp", input: NewFunctionTestInput(types.T_timestamp.ToType(), []types.Timestamp{types.ZeroTimestamp}, nil), expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, []bool{true}), fn: YearWeekTimestamp},
		{name: "yearweek string", input: NewFunctionTestInput(types.T_varchar.ToType(), []string{"0000-00-00"}, nil), expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, []bool{true}), fn: YearWeekString},
		{name: "weekday date", input: NewFunctionTestInput(types.T_date.ToType(), []types.Date{types.ZeroDate}, nil), expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, []bool{true}), fn: DateToWeekday},
		{name: "weekday datetime", input: NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{types.ZeroDatetime}, nil), expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, []bool{true}), fn: DatetimeToWeekday},
		{name: "dayofweek date", input: NewFunctionTestInput(types.T_date.ToType(), []types.Date{types.ZeroDate}, nil), expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, []bool{true}), fn: DateToDayOfWeek},
		{name: "dayofweek datetime", input: NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{types.ZeroDatetime}, nil), expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, []bool{true}), fn: DatetimeToDayOfWeek},
		{name: "dayofweek timestamp", input: NewFunctionTestInput(types.T_timestamp.ToType(), []types.Timestamp{types.ZeroTimestamp}, nil), expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, []bool{true}), fn: TimestampToDayOfWeek},
		{name: "dayname date const", input: NewFunctionTestConstInput(types.T_date.ToType(), []types.Date{types.ZeroDate, types.ZeroDate}, nil), expect: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"", ""}, []bool{true, true}), fn: DateToDayName},
		{name: "dayname datetime", input: NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{types.ZeroDatetime}, nil), expect: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{""}, []bool{true}), fn: DatetimeToDayName},
		{name: "dayname timestamp", input: NewFunctionTestInput(types.T_timestamp.ToType(), []types.Timestamp{types.ZeroTimestamp}, nil), expect: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{""}, []bool{true}), fn: TimestampToDayName},
		{name: "monthname date", input: NewFunctionTestInput(types.T_date.ToType(), []types.Date{types.ZeroDate}, nil), expect: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{""}, []bool{true}), fn: DateToMonthName},
		{name: "monthname datetime", input: NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{types.ZeroDatetime}, nil), expect: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{""}, []bool{true}), fn: DatetimeToMonthName},
		{name: "monthname timestamp", input: NewFunctionTestInput(types.T_timestamp.ToType(), []types.Timestamp{types.ZeroTimestamp}, nil), expect: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{""}, []bool{true}), fn: TimestampToMonthName},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fcTC := NewFunctionTestCase(proc, []FunctionTestInput{tc.input}, tc.expect, tc.fn)
			succeed, info := fcTC.Run()
			require.True(t, succeed, info)
		})
	}
}

func TestPartialDateFunctionsKeepZeroForZeroTemporal(t *testing.T) {
	proc := testutil.NewProcess(t)

	for _, tc := range []struct {
		name   string
		input  FunctionTestInput
		expect FunctionTestResult
		fn     fEvalFn
	}{
		{name: "dayofmonth date", input: NewFunctionTestInput(types.T_date.ToType(), []types.Date{types.ZeroDate}, nil), expect: NewFunctionTestResult(types.T_uint8.ToType(), false, []uint8{0}, nil), fn: DateToDay},
		{name: "dayofmonth datetime", input: NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{types.ZeroDatetime}, nil), expect: NewFunctionTestResult(types.T_uint8.ToType(), false, []uint8{0}, nil), fn: DatetimeToDay},
		{name: "month date", input: NewFunctionTestInput(types.T_date.ToType(), []types.Date{types.ZeroDate}, nil), expect: NewFunctionTestResult(types.T_uint8.ToType(), false, []uint8{0}, nil), fn: DateToMonth},
		{name: "month datetime", input: NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{types.ZeroDatetime}, nil), expect: NewFunctionTestResult(types.T_uint8.ToType(), false, []uint8{0}, nil), fn: DatetimeToMonth},
		{name: "quarter date", input: NewFunctionTestInput(types.T_date.ToType(), []types.Date{types.ZeroDate}, nil), expect: NewFunctionTestResult(types.T_uint8.ToType(), false, []uint8{0}, nil), fn: DateToQuarter},
		{name: "quarter datetime", input: NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{types.ZeroDatetime}, nil), expect: NewFunctionTestResult(types.T_uint8.ToType(), false, []uint8{0}, nil), fn: DatetimeToQuarter},
		{name: "year date", input: NewFunctionTestInput(types.T_date.ToType(), []types.Date{types.ZeroDate}, nil), expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, nil), fn: DateToYear},
		{name: "year datetime", input: NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{types.ZeroDatetime}, nil), expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, nil), fn: DatetimeToYear},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fcTC := NewFunctionTestCase(proc, []FunctionTestInput{tc.input}, tc.expect, tc.fn)
			succeed, info := fcTC.Run()
			require.True(t, succeed, info)
		})
	}
}

func TestDateDiffZeroTemporalReturnsNull(t *testing.T) {
	proc := testutil.NewProcess(t)
	valid := types.DateFromCalendar(2024, 1, 1)
	fcTC := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_date.ToType(), []types.Date{types.ZeroDate, valid}, nil),
			NewFunctionTestInput(types.T_date.ToType(), []types.Date{valid, types.ZeroDate}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0, 0}, []bool{true, true}),
		builtInDateDiff,
	)
	succeed, info := fcTC.Run()
	require.True(t, succeed, info)
}

func TestTimestampDiffZeroTemporalReturnsNull(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().TimeZone = time.UTC
	validDate := types.DateFromCalendar(2024, 1, 1)
	validDatetime := validDate.ToDatetime()
	validTimestamp := validDatetime.ToTimestamp(time.UTC)
	units := NewFunctionTestInput(types.T_varchar.ToType(), []string{"day", "day"}, nil)
	expect := NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0, 0}, []bool{true, true})

	for _, tc := range []struct {
		name   string
		inputs []FunctionTestInput
		fn     fEvalFn
	}{
		{
			name: "datetime",
			inputs: []FunctionTestInput{
				units,
				NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{types.ZeroDatetime, validDatetime}, nil),
				NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{validDatetime, types.ZeroDatetime}, nil),
			},
			fn: TimestampDiff,
		},
		{
			name: "date",
			inputs: []FunctionTestInput{
				units,
				NewFunctionTestInput(types.T_date.ToType(), []types.Date{types.ZeroDate, validDate}, nil),
				NewFunctionTestInput(types.T_date.ToType(), []types.Date{validDate, types.ZeroDate}, nil),
			},
			fn: TimestampDiffDate,
		},
		{
			name: "timestamp",
			inputs: []FunctionTestInput{
				units,
				NewFunctionTestInput(types.T_timestamp.ToType(), []types.Timestamp{types.ZeroTimestamp, validTimestamp}, nil),
				NewFunctionTestInput(types.T_timestamp.ToType(), []types.Timestamp{validTimestamp, types.ZeroTimestamp}, nil),
			},
			fn: TimestampDiffTimestamp,
		},
		{
			name: "string",
			inputs: []FunctionTestInput{
				units,
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"0000-00-00 00:00:00", "2024-01-01 00:00:00"}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"2024-01-01 00:00:00", "0000-00-00 00:00:00"}, nil),
			},
			fn: TimestampDiffString,
		},
		{
			name: "date string",
			inputs: []FunctionTestInput{
				units,
				NewFunctionTestInput(types.T_date.ToType(), []types.Date{types.ZeroDate, validDate}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"2024-01-01 00:00:00", "0000-00-00 00:00:00"}, nil),
			},
			fn: TimestampDiffDateString,
		},
		{
			name: "string date",
			inputs: []FunctionTestInput{
				units,
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"0000-00-00 00:00:00", "2024-01-01 00:00:00"}, nil),
				NewFunctionTestInput(types.T_date.ToType(), []types.Date{validDate, types.ZeroDate}, nil),
			},
			fn: TimestampDiffStringDate,
		},
		{
			name: "timestamp date",
			inputs: []FunctionTestInput{
				units,
				NewFunctionTestInput(types.T_timestamp.ToType(), []types.Timestamp{types.ZeroTimestamp, validTimestamp}, nil),
				NewFunctionTestInput(types.T_date.ToType(), []types.Date{validDate, types.ZeroDate}, nil),
			},
			fn: TimestampDiffTimestampDate,
		},
		{
			name: "date timestamp",
			inputs: []FunctionTestInput{
				units,
				NewFunctionTestInput(types.T_date.ToType(), []types.Date{types.ZeroDate, validDate}, nil),
				NewFunctionTestInput(types.T_timestamp.ToType(), []types.Timestamp{validTimestamp, types.ZeroTimestamp}, nil),
			},
			fn: TimestampDiffDateTimestamp,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fcTC := NewFunctionTestCase(proc, tc.inputs, expect, tc.fn)
			succeed, info := fcTC.Run()
			require.True(t, succeed, info)
		})
	}
}
