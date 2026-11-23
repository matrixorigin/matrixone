// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestTemporalCastIntegerLiveMode(t *testing.T) {
	for _, source := range []types.T{types.T_datetime, types.T_time, types.T_timestamp} {
		for _, target := range []types.T{types.T_int64, types.T_uint64} {
			t.Run(source.String()+"/"+target.String(), func(t *testing.T) {
				proc := testutil.NewProcess(t)
				proc.GetSessionInfo().TimeZone = time.FixedZone("UTC+8", 8*3600)
				dt := types.DatetimeFromClock(2024, 2, 29, 23, 59, 59, 500000)
				var values any
				rounded, truncated := int64(20240301000000), int64(20240229235959)
				switch source {
				case types.T_datetime:
					values = []types.Datetime{dt, 0, types.Datetime(math.MaxInt64)}
				case types.T_timestamp:
					values = []types.Timestamp{dt.ToTimestamp(proc.GetSessionInfo().TimeZone), 0, types.Timestamp(math.MaxInt64)}
				case types.T_time:
					values, rounded, truncated = []types.Time{-500000, 0, types.Time(math.MinInt64)}, -1, 0
				}
				mode, calls := "", 0
				var resolveErr error
				proc.SetResolveVariableFunc(func(name string, _, _ bool) (any, error) {
					require.Equal(t, "sql_mode", name)
					calls++
					return mode, resolveErr
				})
				var empty any = []int64{}
				if target == types.T_uint64 {
					empty = []uint64{}
				}
				test := NewFunctionTestCase(proc, []FunctionTestInput{
					NewFunctionTestInput(source.ToTypeWithScale(6), values, []bool{false, true, false}),
					NewFunctionTestInput(target.ToType(), empty, nil),
				}, NewFunctionTestResult(target.ToType(), false, empty, []bool{false, true, true}), NewTemporalCastInteger).
					WithSelectList(&FunctionSelectList{AnyNull: true, SelectList: []bool{true, true, false}})
				defer test.Free()
				for _, tc := range []struct {
					mode string
					want int64
				}{{"", rounded}, {"TIME_TRUNCATE_FRACTIONAL", truncated}, {"", rounded}} {
					mode = tc.mode
					test.expected.wanted = []int64{tc.want, 0, 0}
					if target == types.T_uint64 {
						test.expected.wanted = []uint64{uint64(tc.want), 0, 0}
					}
					ok, info := test.Run()
					require.True(t, ok, info)
				}
				require.Equal(t, 3, calls, "resolve the live mode once per execution, not per row")
				resolveErr = errors.New("sql_mode unavailable")
				require.NoError(t, test.result.PreExtendAndReset(test.fnLength))
				require.ErrorIs(t, NewTemporalCastInteger(test.parameters, test.result, proc, test.fnLength, nil), resolveErr)
			})
		}
	}
}

func TestTemporalCastIntegerRejectsInvalidShape(t *testing.T) {
	require.Error(t, NewTemporalCastInteger(nil, nil, nil, 0, nil))
	proc := testutil.NewProcess(t)
	for _, inputs := range [][]FunctionTestInput{
		{NewFunctionTestInput(types.T_varchar.ToType(), []string{"x"}, nil), NewFunctionTestInput(types.T_int64.ToType(), []int64{}, nil)},
		{NewFunctionTestInput(types.T_time.ToType(), []types.Time{0}, nil), NewFunctionTestInput(types.T_int32.ToType(), []int32{}, nil)},
	} {
		test := NewFunctionTestCase(proc, inputs, NewFunctionTestResult(inputs[1].typ, true, nil, nil), NewTemporalCastInteger)
		ok, info := test.RunAndFree()
		require.True(t, ok, info)
	}
}

func TestExplicitTemporalIntegerCast(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input FunctionTestInput
		want  []int64
	}{
		{"date", NewFunctionTestInput(types.T_date.ToType(), []types.Date{types.DateFromCalendar(2024, 2, 29), types.ZeroDate}, nil), []int64{20240229, 0}},
		{"datetime", NewFunctionTestInput(types.T_datetime.ToTypeWithScale(6), []types.Datetime{
			types.DatetimeFromClock(2024, 2, 29, 23, 59, 59, 499999),
			types.DatetimeFromClock(2024, 2, 29, 23, 59, 59, 500000),
			types.DatetimeFromClock(2024, 12, 31, 23, 59, 59, 999999),
			types.DatetimeFromClock(9999, 12, 31, 23, 59, 59, 999999),
			types.ZeroDatetime,
		}, nil), []int64{20240229235959, 20240301000000, 20250101000000, 99991231000000, 0}},
		{"timestamp", NewFunctionTestInput(types.T_timestamp.ToTypeWithScale(6), []types.Timestamp{
			types.DatetimeFromClock(2024, 2, 29, 23, 59, 59, 500000).ToTimestamp(time.UTC),
			types.DatetimeFromClock(2024, 1, 2, 12, 59, 59, 499999).ToTimestamp(time.UTC),
			types.DatetimeFromClock(2024, 1, 2, 12, 59, 59, 500000).ToTimestamp(time.UTC),
			types.ZeroTimestamp,
		}, nil), []int64{20240301000000, 20240102125959, 20240102130000, 0}},
		{"time", NewFunctionTestInput(types.T_time.ToTypeWithScale(6), []types.Time{
			types.TimeFromClock(false, 12, 59, 59, 499999),
			types.TimeFromClock(false, 12, 59, 59, 500000),
			types.TimeFromClock(true, 0, 0, 0, 499999),
			types.TimeFromClock(true, 0, 0, 0, 500000),
			types.TimeFromClock(true, 12, 0, 0, 500000),
			types.TimeFromClock(false, 838, 59, 59, 999999),
			types.TimeFromClock(true, 838, 59, 59, 999999),
			types.TimeFromClock(false, 839, 0, 0, 500000),
			0,
		}, nil), []int64{125959, 130000, 0, -1, -120001, 8385959, -8385959, 8390001, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.GetSessionInfo().TimeZone = time.UTC
			for _, target := range []types.T{types.T_int64, types.T_uint64} {
				t.Run(target.String(), func(t *testing.T) {
					_, err := GetFunctionByName(context.Background(), "cast", []types.Type{tc.input.typ, target.ToType()})
					require.NoError(t, err)
					var empty, expected any = []int64{}, tc.want
					if target == types.T_uint64 {
						values := make([]uint64, len(tc.want))
						for i, value := range tc.want {
							values[i] = uint64(value)
						}
						empty, expected = []uint64{}, values
					}
					test := NewFunctionTestCase(proc, []FunctionTestInput{tc.input, NewFunctionTestInput(target.ToType(), empty, nil)}, NewFunctionTestResult(target.ToType(), false, expected, nil), NewExplicitCast)
					ok, info := test.RunAndFree()
					require.True(t, ok, info)
				})
			}
		})
	}
}

func TestExplicitTemporalIntegerCastSelection(t *testing.T) {
	for _, oid := range []types.T{types.T_date, types.T_datetime, types.T_timestamp, types.T_time} {
		t.Run(oid.String(), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.GetSessionInfo().TimeZone = time.FixedZone("UTC+8", 8*3600)
			proc.GetSessionInfo().SqlMode = "TIME_TRUNCATE_FRACTIONAL"
			dt := types.DatetimeFromClock(2024, 2, 29, 23, 59, 59, 500000)
			var values any
			var want int64
			switch oid {
			case types.T_date:
				values, want = []types.Date{types.DateFromCalendar(2024, 2, 29), types.ZeroDate, types.ZeroDate}, 20240229
			case types.T_datetime:
				values, want = []types.Datetime{dt, types.Datetime(math.MaxInt64), types.ZeroDatetime}, 20240301000000
			case types.T_timestamp:
				values, want = []types.Timestamp{dt.ToTimestamp(time.UTC), types.Timestamp(math.MaxInt64), types.ZeroTimestamp}, 20240301080000
			case types.T_time:
				values, want = []types.Time{types.TimeFromClock(true, 0, 0, 0, 500000), types.Time(math.MinInt64), 0}, -1
			}
			for _, target := range []types.T{types.T_int64, types.T_uint64} {
				var empty, expected any = []int64{}, []int64{want, 0, 0}
				if target == types.T_uint64 {
					empty, expected = []uint64{}, []uint64{uint64(want), 0, 0}
				}
				inputs := []FunctionTestInput{NewFunctionTestInput(oid.ToTypeWithScale(6), values, []bool{false, false, true}), NewFunctionTestInput(target.ToType(), empty, nil)}
				test := NewFunctionTestCase(proc, inputs, NewFunctionTestResult(target.ToType(), false, expected, []bool{false, true, true}), NewExplicitCast).WithSelectList(&FunctionSelectList{AnyNull: true, SelectList: []bool{true, false, true}})
				ok, info := test.RunAndFree()
				require.True(t, ok, info)
				// A single physical value is broadcast to all logical rows.
				inputs[0].isConst = true
				if target == types.T_int64 {
					expected = []int64{want, want, want}
				} else {
					expected = []uint64{uint64(want), uint64(want), uint64(want)}
				}
				test = NewFunctionTestCase(proc, inputs, NewFunctionTestResult(target.ToType(), false, expected, nil), NewExplicitCast)
				ok, info = test.RunAndFree()
				require.True(t, ok, info)
				inputs[0].nullList = []bool{true}
				test = NewFunctionTestCase(proc, inputs, NewFunctionTestResult(target.ToType(), false, expected, []bool{true, true, true}), NewExplicitCast)
				ok, info = test.RunAndFree()
				require.True(t, ok, info)
				// Neither an invalid masked value nor an empty batch is evaluated.
				inputs[0].isConst = false
				test = NewFunctionTestCase(proc, inputs, NewFunctionTestResult(target.ToType(), false, expected, []bool{true, true, true}), NewExplicitCast).WithSelectList(&FunctionSelectList{AllNull: true})
				ok, info = test.RunAndFree()
				require.True(t, ok, info)
				test = NewFunctionTestCase(proc, inputs, NewFunctionTestResult(target.ToType(), false, empty, nil), NewExplicitCast)
				test.fnLength = 0
				ok, info = test.RunAndFree()
				require.True(t, ok, info)
			}
		})
	}
}

func TestTemporalIntegerCastRoleIsolation(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().TimeZone = time.UTC
	dt := types.DatetimeFromClock(2024, 2, 29, 23, 59, 59, 500000)
	for _, tc := range []struct {
		input FunctionTestInput
		want  int64
	}{
		{NewFunctionTestInput(types.T_datetime.ToTypeWithScale(6), []types.Datetime{dt}, nil), 20240229235959},
		{NewFunctionTestInput(types.T_timestamp.ToTypeWithScale(6), []types.Timestamp{dt.ToTimestamp(time.UTC)}, nil), 20240229235959},
		{NewFunctionTestInput(types.T_date.ToType(), []types.Date{dt.ToDate()}, nil), 20240229},
		{NewFunctionTestInput(types.T_time.ToTypeWithScale(6), []types.Time{types.TimeFromClock(false, 12, 59, 59, 500000)}, nil), 125959},
	} {
		for _, fn := range []executeLogicOfOverload{NewCast, NewAssignCast, NewComparisonCast, NewSetOperationCast} {
			for _, target := range []types.T{types.T_int64, types.T_uint64} {
				var empty, want any = []int64{}, []int64{tc.want}
				if target == types.T_uint64 {
					empty, want = []uint64{}, []uint64{uint64(tc.want)}
				}
				test := NewFunctionTestCase(proc, []FunctionTestInput{tc.input, NewFunctionTestInput(target.ToType(), empty, nil)}, NewFunctionTestResult(target.ToType(), false, want, nil), fn)
				ok, info := test.RunAndFree()
				require.True(t, ok, info)
			}
		}
	}
	decimalType := types.New(types.T_decimal128, 20, 6)
	want, err := types.ParseDecimal128("20240229235959.500000", 20, 6)
	require.NoError(t, err)
	test := NewFunctionTestCase(proc, []FunctionTestInput{NewFunctionTestInput(types.T_datetime.ToTypeWithScale(6), []types.Datetime{dt}, nil), NewFunctionTestInput(decimalType, []types.Decimal128{}, nil)}, NewFunctionTestResult(decimalType, false, []types.Decimal128{want}, nil), NewExplicitCast)
	ok, info := test.RunAndFree()
	require.True(t, ok, info)
}
