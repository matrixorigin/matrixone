// Copyright 2023 Matrix Origin
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

package function2

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"testing"
)

// TIMEDIFF

func initTimeDiffInTimeTestCase() []tcTemp {
	//Test Set 1
	t11, _ := types.ParseTime("22:22:22", 6)
	t12, _ := types.ParseTime("11:11:11", 6)
	r1, _ := types.ParseTime("11:11:11", 6)

	t21, _ := types.ParseTime("22:22:22", 6)
	t22, _ := types.ParseTime("-11:11:11", 6)
	r2, _ := types.ParseTime("33:33:33", 6)

	t31, _ := types.ParseTime("-22:22:22", 6)
	t32, _ := types.ParseTime("11:11:11", 6)
	r3, _ := types.ParseTime("-33:33:33", 6)

	t41, _ := types.ParseTime("-22:22:22", 6)
	t42, _ := types.ParseTime("-11:11:11", 6)
	r4, _ := types.ParseTime("-11:11:11", 6)

	//Test Set 2
	t51, _ := types.ParseTime("11:11:11", 6)
	t52, _ := types.ParseTime("22:22:22", 6)
	r5, _ := types.ParseTime("-11:11:11", 6)

	t61, _ := types.ParseTime("11:11:11", 6)
	t62, _ := types.ParseTime("-22:22:22", 6)
	r6, _ := types.ParseTime("33:33:33", 6)

	t71, _ := types.ParseTime("-11:11:11", 6)
	t72, _ := types.ParseTime("22:22:22", 6)
	r7, _ := types.ParseTime("-33:33:33", 6)

	t81, _ := types.ParseTime("-11:11:11", 6)
	t82, _ := types.ParseTime("-22:22:22", 6)
	r8, _ := types.ParseTime("11:11:11", 6)

	//Test Set 3
	t91, _ := types.ParseTime("-2562047787:59:59", 6)
	t92, _ := types.ParseTime("-2562047787:59:59", 6)
	r9, _ := types.ParseTime("00:00:00", 6)

	t101, _ := types.ParseTime("2562047787:59:59", 6)
	t102, _ := types.ParseTime("2562047787:59:59", 6)
	r10, _ := types.ParseTime("00:00:00", 6)

	return []tcTemp{
		{
			info: "test timediff time 1",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_time.ToType(), []types.Time{t11, t21, t31, t41, t51, t61, t71, t81, t91, t101}, []bool{}),
				testutil.NewFunctionTestInput(types.T_time.ToType(), []types.Time{t12, t22, t32, t42, t52, t62, t72, t82, t92, t102}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_time.ToType(), false, []types.Time{r1, r2, r3, r4, r5, r6, r7, r8, r9, r10}, []bool{}),
		},
	}
}

func TestTimeDiffInTime(t *testing.T) {
	testCases := initTimeDiffInTimeTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, TimeDiff[types.Time])
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initTimeDiffInDatetimeTestCase() []tcTemp {
	// Test case 1
	t11, _ := types.ParseDatetime("2012-12-12 22:22:22", 6)
	t12, _ := types.ParseDatetime("2012-12-12 11:11:11", 6)
	r1, _ := types.ParseTime("11:11:11", 0)

	// Test case 2
	t21, _ := types.ParseDatetime("2012-12-12 11:11:11", 6)
	t22, _ := types.ParseDatetime("2012-12-12 22:22:22", 6)
	r2, _ := types.ParseTime("-11:11:11", 0)

	// Test case 3
	t31, _ := types.ParseDatetime("2012-12-12 22:22:22", 6)
	t32, _ := types.ParseDatetime("2000-12-12 11:11:11", 6)
	r3, _ := types.ParseTime("105203:11:11", 0)

	// Test case 4
	t41, _ := types.ParseDatetime("2000-12-12 11:11:11", 6)
	t42, _ := types.ParseDatetime("2012-12-12 22:22:22", 6)
	r4, _ := types.ParseTime("-105203:11:11", 0)

	// Test case 5
	t51, _ := types.ParseDatetime("2012-12-12 22:22:22", 6)
	t52, _ := types.ParseDatetime("2012-10-10 11:11:11", 6)
	r5, _ := types.ParseTime("1523:11:11", 0)

	// Test case 6
	t61, _ := types.ParseDatetime("2012-10-10 11:11:11", 6)
	t62, _ := types.ParseDatetime("2012-12-12 22:22:22", 6)
	r6, _ := types.ParseTime("-1523:11:11", 0)

	// Test case 7
	t71, _ := types.ParseDatetime("2012-12-12 22:22:22", 6)
	t72, _ := types.ParseDatetime("2012-12-10 11:11:11", 6)
	r7, _ := types.ParseTime("59:11:11", 0)

	// Test case 8
	t81, _ := types.ParseDatetime("2012-12-10 11:11:11", 6)
	t82, _ := types.ParseDatetime("2012-12-12 22:22:22", 6)
	r8, _ := types.ParseTime("-59:11:11", 0)

	// Test case 9
	t91, _ := types.ParseDatetime("2012-12-10 11:11:11", 6)
	t92, _ := types.ParseDatetime("2012-12-10 11:11:11", 6)
	r9, _ := types.ParseTime("00:00:00", 0)

	return []tcTemp{
		{
			info: "test Datetimediff Datetime 1",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{t11, t21, t31, t41, t51, t61, t71, t81, t91}, []bool{}),
				testutil.NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{t12, t22, t32, t42, t52, t62, t72, t82, t92}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_time.ToType(), false, []types.Time{r1, r2, r3, r4, r5, r6, r7, r8, r9}, []bool{}),
		},
	}
}

func TestTimeDiffInDateTime(t *testing.T) {
	testCases := initTimeDiffInDatetimeTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, TimeDiff[types.Datetime])
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// TIMESTAMPDIFF

func initTimestampDiffTestCase() []tcTemp {
	cases := []struct {
		name   string
		inputs []string
		want   int64
	}{
		{
			name:   "TEST01",
			inputs: []string{"2017-12-01 12:15:12", "2018-01-01 7:18:20", "microsecond"},
			want:   2660588000000,
		},
		{
			name:   "TEST02",
			inputs: []string{"2017-12-01 12:15:12", "2018-01-01 7:18:20", "second"},
			want:   2660588,
		},
		{
			name:   "TEST03",
			inputs: []string{"2017-12-01 12:15:12", "2018-01-01 7:18:20", "minute"},
			want:   44343,
		},
		{
			name:   "TEST04",
			inputs: []string{"2017-12-01 12:15:12", "2018-01-01 7:18:20", "hour"},
			want:   739,
		},
		{
			name:   "TEST05",
			inputs: []string{"2017-12-01 12:15:12", "2018-01-01 7:18:20", "day"},
			want:   30,
		},
		{
			name:   "TEST06",
			inputs: []string{"2017-12-01 12:15:12", "2018-01-08 12:15:12", "week"},
			want:   5,
		},
		{
			name:   "TEST07",
			inputs: []string{"2017-11-01 12:15:12", "2018-01-01 12:15:12", "month"},
			want:   2,
		},
		{
			name:   "TEST08",
			inputs: []string{"2017-01-01 12:15:12", "2018-01-01 12:15:12", "quarter"},
			want:   4,
		},
		{
			name:   "TEST09",
			inputs: []string{"2017-01-01 12:15:12", "2018-01-01 12:15:12", "year"},
			want:   1,
		},
		{
			name:   "TEST10",
			inputs: []string{"2018-01-01 7:18:20", "2017-12-01 12:15:12", "microsecond"},
			want:   -2660588000000,
		},
		{
			name:   "TEST11",
			inputs: []string{"2018-01-01 7:18:20", "2017-12-01 12:15:12", "second"},
			want:   -2660588,
		},
		{
			name:   "TEST12",
			inputs: []string{"2018-01-01 7:18:20", "2017-12-01 12:15:12", "minute"},

			want: -44343,
		},
		{
			name:   "TEST13",
			inputs: []string{"2018-01-01 7:18:20", "2017-12-01 12:15:12", "hour"},

			want: -739,
		},
		{
			name:   "TEST14",
			inputs: []string{"2018-01-01 7:18:20", "2017-12-01 12:15:12", "day"},

			want: -30,
		},
		{
			name:   "TEST15",
			inputs: []string{"2018-01-08 12:15:12", "2017-12-01 12:15:12", "week"},
			want:   -5,
		},
		{
			name:   "TEST16",
			inputs: []string{"2018-01-01 12:15:12", "2017-11-01 12:15:12", "month"},
			want:   -2,
		},
		{
			name:   "TEST17",
			inputs: []string{"2018-01-01 12:15:12", "2017-01-01 12:15:12", "quarter"},
			want:   -4,
		},
		{
			name:   "TEST18",
			inputs: []string{"2018-01-01 12:15:12", "2017-01-01 12:15:12", "year"},
			want:   -1,
		},
	}

	var testInputs []tcTemp
	for _, c := range cases {

		i1 := c.inputs[2]
		i2, _ := types.ParseDatetime(c.inputs[0], 6)
		i3, _ := types.ParseDatetime(c.inputs[1], 6)

		o := c.want

		testInputs = append(testInputs, tcTemp{

			info: "test TimestampDiff " + c.name,
			inputs: []testutil.FunctionTestInput{
				// Create a input entry <String, Datetime1, Datetime2>
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{i1}, []bool{}),
				testutil.NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{i2}, []bool{}),
				testutil.NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{i3}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false, []int64{o}, []bool{}),
		})
	}

	return testInputs
}

func TestTimestampDiff(t *testing.T) {
	testCases := initTimestampDiffTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, TimestampDiff)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}
