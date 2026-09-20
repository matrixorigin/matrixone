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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// These cases exercise the compatibility branches added for runtime temporal
// formats and for the typed/string temporal arithmetic matrix.  The BVT cases
// validate the public SQL contract; this test keeps the changed execution
// branches covered by the unit-test coverage gate as well.
func TestTemporalCompatibilityExecutionMatrix(t *testing.T) {
	proc := testutil.NewProcess(t)

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
