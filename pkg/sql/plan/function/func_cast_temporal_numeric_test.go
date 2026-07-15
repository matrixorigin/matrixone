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
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestCastTemporalNumericUsesMysqlPackedValue(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().TimeZone = time.UTC

	dateValue := types.DateFromCalendar(2024, 1, 2)
	datetimeValue, err := types.ParseDatetime("2024-01-02 03:04:05.123456", 6)
	require.NoError(t, err)
	timestampValue := datetimeValue.ToTimestamp(time.UTC)
	dateDecimal, err := types.ParseDecimal128("20240102", 20, 0)
	require.NoError(t, err)
	dateDecimal64, err := types.ParseDecimal64("20240102.000000", 14, 6)
	require.NoError(t, err)
	datetimeDecimal, err := types.ParseDecimal128("20240102030405.123456", 20, 6)
	require.NoError(t, err)
	yearDecimal, err := types.ParseDecimal128("2024", 20, 0)
	require.NoError(t, err)
	yearDecimal64, err := types.ParseDecimal64("2024", 4, 0)
	require.NoError(t, err)

	testCases := []tcTemp{
		{
			info: "date to signed uses YYYYMMDD",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_date.ToType(), []types.Date{dateValue}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{}, nil),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{20240102}, nil),
		},
		{
			info: "date to decimal uses YYYYMMDD",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_date.ToType(), []types.Date{dateValue}, nil),
				NewFunctionTestInput(types.New(types.T_decimal128, 20, 0), []types.Decimal128{}, nil),
			},
			expect: NewFunctionTestResult(types.New(types.T_decimal128, 20, 0), false, []types.Decimal128{dateDecimal}, nil),
		},
		{
			info: "date to decimal64 preserves target scale",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_date.ToType(), []types.Date{dateValue}, nil),
				NewFunctionTestInput(types.New(types.T_decimal64, 14, 6), []types.Decimal64{}, nil),
			},
			expect: NewFunctionTestResult(types.New(types.T_decimal64, 14, 6), false, []types.Decimal64{dateDecimal64}, nil),
		},
		{
			info: "datetime to signed uses YYYYMMDDHHMMSS",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{datetimeValue}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{}, nil),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{20240102030405}, nil),
		},
		{
			info: "datetime to decimal preserves fractional seconds",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{datetimeValue}, nil),
				NewFunctionTestInput(types.New(types.T_decimal128, 20, 6), []types.Decimal128{}, nil),
			},
			expect: NewFunctionTestResult(types.New(types.T_decimal128, 20, 6), false, []types.Decimal128{datetimeDecimal}, nil),
		},
		{
			info: "timestamp to signed uses session time zone calendar value",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_timestamp.ToType(), []types.Timestamp{timestampValue}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{}, nil),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{20240102030405}, nil),
		},
		{
			info: "timestamp to decimal preserves fractional seconds in session time zone",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_timestamp.ToType(), []types.Timestamp{timestampValue}, nil),
				NewFunctionTestInput(types.New(types.T_decimal128, 20, 6), []types.Decimal128{}, nil),
			},
			expect: NewFunctionTestResult(types.New(types.T_decimal128, 20, 6), false, []types.Decimal128{datetimeDecimal}, nil),
		},
		{
			info: "year to decimal uses year value",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_year.ToType(), []types.MoYear{2024}, nil),
				NewFunctionTestInput(types.New(types.T_decimal128, 20, 0), []types.Decimal128{}, nil),
			},
			expect: NewFunctionTestResult(types.New(types.T_decimal128, 20, 0), false, []types.Decimal128{yearDecimal}, nil),
		},
		{
			info: "year to decimal64 uses year value",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_year.ToType(), []types.MoYear{2024}, nil),
				NewFunctionTestInput(types.New(types.T_decimal64, 4, 0), []types.Decimal64{}, nil),
			},
			expect: NewFunctionTestResult(types.New(types.T_decimal64, 4, 0), false, []types.Decimal64{yearDecimal64}, nil),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, NewCast)
			succeed, info := fcTC.Run()
			require.True(t, succeed, info)
		})
	}
}

func TestTemporalNumericTypeCheckUsesDecimal128ForDateTimeAndTimestamp(t *testing.T) {
	for _, tc := range []struct {
		name  string
		left  types.Type
		right types.Type
		want  types.T
	}{
		{
			name:  "datetime plus int uses decimal128",
			left:  types.T_datetime.ToTypeWithScale(6),
			right: types.T_int64.ToType(),
			want:  types.T_decimal128,
		},
		{
			name:  "timestamp plus int uses decimal128",
			left:  types.T_timestamp.ToTypeWithScale(6),
			right: types.T_int64.ToType(),
			want:  types.T_decimal128,
		},
		{
			name:  "time plus int stays decimal64",
			left:  types.T_time.ToType(),
			right: types.T_int64.ToType(),
			want:  types.T_decimal64,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			has, left, right := fixedTypeCastRule1(tc.left, tc.right)
			require.True(t, has)
			require.Equal(t, tc.want, left.Oid)
			require.Equal(t, tc.want, right.Oid)
			if tc.left.Oid.IsDateRelate() && tc.left.Scale > 0 {
				require.Equal(t, tc.left.Scale, left.Scale)
			}
		})
	}
}

func TestTemporalNumericCastOverloadSupportsDateAndYearDecimal(t *testing.T) {
	for _, tc := range []struct {
		name   string
		source types.T
		target types.T
	}{
		{name: "date to decimal64", source: types.T_date, target: types.T_decimal64},
		{name: "date to decimal128", source: types.T_date, target: types.T_decimal128},
		{name: "year to decimal64", source: types.T_year, target: types.T_decimal64},
		{name: "year to decimal128", source: types.T_year, target: types.T_decimal128},
		{name: "decimal128 to timestamp", source: types.T_decimal128, target: types.T_timestamp},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.True(t, IfTypeCastSupported(tc.source, tc.target))
			_, err := GetFunctionByName(context.Background(), "cast", []types.Type{
				tc.source.ToType(), tc.target.ToType(),
			})
			require.NoError(t, err)
		})
	}
}

func TestCastDecimal128PackedDatetimeToTimestamp(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().TimeZone = time.UTC

	packed, err := types.ParseDecimal128("20220102000101", 20, 0)
	require.NoError(t, err)
	expected, err := types.ParseTimestamp(time.UTC, "2022-01-02 00:01:01", 0)
	require.NoError(t, err)

	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.New(types.T_decimal128, 20, 0), []types.Decimal128{packed}, nil),
			NewFunctionTestInput(types.T_timestamp.ToType(), []types.Timestamp{}, nil),
		},
		NewFunctionTestResult(types.T_timestamp.ToType(), false, []types.Timestamp{expected}, nil),
		NewCast,
	)
	succeed, info := tc.Run()
	require.True(t, succeed, info)
}

func TestTimestampNumericCastUsesSessionTimeZone(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().TimeZone = time.FixedZone("+08:00", 8*60*60)

	datetimeValue, err := types.ParseDatetime("2024-01-02 03:04:05.123456", 6)
	require.NoError(t, err)
	timestampValue := datetimeValue.ToTimestamp(time.UTC)
	expected, err := types.ParseDecimal128("20240102110405.123456", 20, 6)
	require.NoError(t, err)

	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_timestamp.ToTypeWithScale(6), []types.Timestamp{timestampValue}, nil),
			NewFunctionTestInput(types.New(types.T_decimal128, 20, 6), []types.Decimal128{}, nil),
		},
		NewFunctionTestResult(types.New(types.T_decimal128, 20, 6), false, []types.Decimal128{expected}, nil),
		NewCast,
	)
	succeed, info := tc.Run()
	require.True(t, succeed, info)
}

func TestTemporalNumericCastRejectsPackedValueOutsideInt32(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().TimeZone = time.UTC

	datetimeValue, err := types.ParseDatetime("2024-01-02 03:04:05", 0)
	require.NoError(t, err)
	timestampValue := datetimeValue.ToTimestamp(time.UTC)

	for _, tc := range []struct {
		name  string
		input FunctionTestInput
	}{
		{
			name:  "datetime",
			input: NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{datetimeValue}, nil),
		},
		{
			name:  "timestamp",
			input: NewFunctionTestInput(types.T_timestamp.ToType(), []types.Timestamp{timestampValue}, nil),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fcTC := NewFunctionTestCase(proc,
				[]FunctionTestInput{
					tc.input,
					NewFunctionTestInput(types.T_int32.ToType(), []int32{}, nil),
				},
				NewFunctionTestResult(types.T_int32.ToType(), true, nil, nil),
				NewCast,
			)
			succeed, info := fcTC.Run()
			require.True(t, succeed, info)
		})
	}
}
