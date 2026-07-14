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

func TestCastTemporalNumericUsesMysqlPackedValue(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().TimeZone = time.UTC

	dateValue := types.DateFromCalendar(2024, 1, 2)
	datetimeValue, err := types.ParseDatetime("2024-01-02 03:04:05.123456", 6)
	require.NoError(t, err)
	timestampValue := datetimeValue.ToTimestamp(time.UTC)
	dateDecimal, err := types.ParseDecimal128("20240102", 20, 0)
	require.NoError(t, err)
	datetimeDecimal, err := types.ParseDecimal128("20240102030405.123456", 20, 6)
	require.NoError(t, err)
	yearDecimal, err := types.ParseDecimal128("2024", 20, 0)
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
