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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
	"time"
)

func initAddFaultPointTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test space",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"a"}, []bool{false}),
				testutil.NewFunctionTestConstInput(types.T_varchar.ToType(), []string{":5::"}, []bool{false}),
				testutil.NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"return"}, []bool{false}),
				testutil.NewFunctionTestConstInput(types.T_int64.ToType(), []int64{0}, []bool{false}),
				testutil.NewFunctionTestConstInput(types.T_varchar.ToType(), []string{""}, []bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_bool.ToType(), true,
				[]bool{true},
				[]bool{false}),
		},
	}
}

func TestAddFaultPoint(t *testing.T) {
	testCases := initAddFaultPointTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, AddFaultPoint)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initCeilTestCase() []tcTemp {
	rfs := []float64{1, -1, -2, math.MinInt64 + 1, math.MinInt64 + 2, -100, -1, 1,
		0, 2, 5, 9, 17, 33, 65, math.MaxInt64, math.MaxFloat64, 0}
	fs := []float64{math.SmallestNonzeroFloat64, -1.2, -2.3, math.MinInt64 + 1, math.MinInt64 + 2, -100.2, -1.3, 0.9, 0,
		1.5, 4.4, 8.5, 16.32, 32.345, 64.09, math.MaxInt64, math.MaxFloat64, 0}
	bs := make([]bool, len(fs))
	return []tcTemp{
		{
			info: "test ceil",
			typ:  types.T_uint64,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{1, 4, 8, 16, 32, math.MaxUint64, 0},
					[]bool{false, false, false, false, false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{1, 4, 8, 16, 32, math.MaxUint64, 0},
				[]bool{false, false, false, false, false, false, false}),
		},
		{
			info: "test ceil",
			typ:  types.T_int64,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{math.MinInt64 + 1, math.MinInt64 + 2, -100, -1, 0, 1, 4, 8, 16, 32, 64, math.MaxInt64, 0},
					[]bool{false, false, false, false, false, false, false, false, false, false, false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{math.MinInt64 + 1, math.MinInt64 + 2, -100, -1, 0, 1, 4, 8, 16, 32, 64, math.MaxInt64, 0},
				[]bool{false, false, false, false, false, false, false, false, false, false, false, false, false}),
		},
		{
			info: "test ceil",
			typ:  types.T_float64,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(),
					fs,
					bs),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				rfs,
				bs),
		},
	}
}

func TestCeil(t *testing.T) {
	testCases := initCeilTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		var fcTC testutil.FunctionTestCase
		switch tc.typ {
		case types.T_uint64:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, CeilUint64)
		case types.T_int64:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, CeilInt64)
		case types.T_float64:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, CeilFloat64)
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initFloorTestCase() []tcTemp {
	rfs := []float64{0, -2, -3, math.MinInt64 + 1, math.MinInt64 + 2, -101, -2, 0,
		0, 1, 4, 8, 16, 32, 64, math.MaxInt64, math.MaxFloat64, 0}
	fs := []float64{math.SmallestNonzeroFloat64, -1.2, -2.3, math.MinInt64 + 1, math.MinInt64 + 2, -100.2, -1.3, 0.9, 0,
		1.5, 4.4, 8.5, 16.32, 32.345, 64.09, math.MaxInt64, math.MaxFloat64, 0}
	bs := make([]bool, len(fs))
	return []tcTemp{
		{
			info: "test floor uint64",
			typ:  types.T_uint64,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{1, 4, 8, 16, 32, math.MaxUint64, 0},
					[]bool{false, false, false, false, false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{1, 4, 8, 16, 32, math.MaxUint64, 0},
				[]bool{false, false, false, false, false, false, false}),
		},
		{
			info: "test floor int64",
			typ:  types.T_int64,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{math.MinInt64 + 1, math.MinInt64 + 2, -100, -1, 0, 1, 4, 8, 16, 32, 64, math.MaxInt64, 0},
					[]bool{false, false, false, false, false, false, false, false, false, false, false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{math.MinInt64 + 1, math.MinInt64 + 2, -100, -1, 0, 1, 4, 8, 16, 32, 64, math.MaxInt64, 0},
				[]bool{false, false, false, false, false, false, false, false, false, false, false, false, false}),
		},
		{
			info: "test floor floa64",
			typ:  types.T_float64,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(),
					fs,
					bs),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				rfs,
				bs),
		},
	}
}

func TestFloor(t *testing.T) {
	testCases := initFloorTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		var fcTC testutil.FunctionTestCase
		switch tc.typ {
		case types.T_uint64:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, FloorUInt64)
		case types.T_int64:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, FloorInt64)
		case types.T_float64:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, FloorFloat64)
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initRoundTestCase() []tcTemp {
	rfs := []float64{0, -1, -2, math.MinInt64 + 1, math.MinInt64 + 2, -100, -1, 1,
		0, 2, 4, 8, 16, 32, 64, math.MaxInt64, math.MaxFloat64, 0}
	fs := []float64{math.SmallestNonzeroFloat64, -1.2, -2.3, math.MinInt64 + 1, math.MinInt64 + 2, -100.2, -1.3, 0.9, 0,
		1.5, 4.4, 8.5, 16.32, 32.345, 64.09, math.MaxInt64, math.MaxFloat64, 0}
	bs := make([]bool, len(fs))
	return []tcTemp{
		{
			info: "test round uint64",
			typ:  types.T_uint64,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{1, 4, 8, 16, 32, math.MaxUint64, 0},
					[]bool{false, false, false, false, false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{1, 4, 8, 16, 32, math.MaxUint64, 0},
				[]bool{false, false, false, false, false, false, false}),
		},
		{
			info: "test round int64",
			typ:  types.T_int64,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{math.MinInt64 + 1, math.MinInt64 + 2, -100, -1, 0, 1, 4, 8, 16, 32, 64, math.MaxInt64, 0},
					[]bool{false, false, false, false, false, false, false, false, false, false, false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{math.MinInt64 + 1, math.MinInt64 + 2, -100, -1, 0, 1, 4, 8, 16, 32, 64, math.MaxInt64, 0},
				[]bool{false, false, false, false, false, false, false, false, false, false, false, false, false}),
		},
		{
			info: "test round floa64",
			typ:  types.T_float64,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(),
					fs,
					bs),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				rfs,
				bs),
		},
	}
}

func TestRound(t *testing.T) {
	testCases := initRoundTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		var fcTC testutil.FunctionTestCase
		switch tc.typ {
		case types.T_uint64:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, RoundUint64)
		case types.T_int64:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, RoundInt64)
		case types.T_float64:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, RoundFloat64)
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initCoalesceTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test Coalesce int64",
			typ:  types.T_int64,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1, 4, 8, 16, 32, 0},
					[]bool{true, true, true, true, true, true}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1, 4, 8, 16, 32, 0},
					[]bool{false, false, false, false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{1, 4, 8, 16, 32, 0},
				[]bool{false, false, false, false, false, false}),
		},
		{
			info: "test Coalesce varchar",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"hello", "", "world"},
					[]bool{false, true, false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"hello", "-", "world"},
					[]bool{true, false, true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"hello", "-", "world"},
				[]bool{false, false, false}),
		},
	}
}

func TestCoalesce(t *testing.T) {
	testCases := initCoalesceTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		var fcTC testutil.FunctionTestCase
		switch tc.typ {
		case types.T_varchar:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, CoalesceStr)
		case types.T_int64:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, CoalesceGeneral[int64])
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initConcatWsTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test ConcatWs",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"---", "---", "---"},
					[]bool{false, false, false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"a", "b", "c"},
					[]bool{false, false, false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"a", "b", "c"},
					[]bool{false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"a---a", "b---b", "c---c"},
				[]bool{false, false, false}),
		},
	}
}

func TestConcatWs(t *testing.T) {
	testCases := initConcatWsTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, ConcatWs)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initDateAddTestCase() []tcTemp {
	d1, _ := types.ParseDatetime("2022-01-01", 6)
	r1, _ := types.ParseDatetime("2022-01-02", 6)
	return []tcTemp{
		{
			info: "test DateAdd",
			typ:  types.T_date,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{d1.ToDate()},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{int64(types.Day)},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_date.ToType(), false,
				[]types.Date{r1.ToDate()},
				[]bool{false}),
		},
		{
			info: "test DatetimeAdd",
			typ:  types.T_datetime,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d1},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{int64(types.Day)},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_datetime.ToType(), false,
				[]types.Datetime{r1},
				[]bool{false}),
		},
		{
			info: "test DatetimeAdd",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"2022-01-01"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{int64(types.Day)},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_datetime.ToType(), false,
				[]types.Datetime{r1},
				[]bool{false}),
		},
	}
}

func TestDateAdd(t *testing.T) {
	testCases := initDateAddTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		var fcTC testutil.FunctionTestCase
		switch tc.typ {
		case types.T_date:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, DateAdd)
		case types.T_datetime:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, DatetimeAdd)
		case types.T_varchar:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, DateStringAdd)
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initFormatTestCase() []tcTemp {
	format := `%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y %%`

	d1, _ := types.ParseDatetime("2010-01-07 23:12:34.12345", 6)
	r1 := `Jan January 01 1 7th 07 7 007 23 11 12 PM 11:12:34 PM 23:12:34 34 123450 01 01 01 01 Thu Thursday 4 2010 2010 2010 10 %`

	d2, _ := types.ParseDatetime("2012-12-21 23:12:34.123456", 6)
	r2 := "Dec December 12 12 21st 21 21 356 23 11 12 PM 11:12:34 PM 23:12:34 34 123456 51 51 51 51 Fri Friday 5 2012 2012 2012 12 %"

	d3, _ := types.ParseDatetime("0001-01-01 00:00:00.123456", 6)
	r3 := `Jan January 01 1 1st 01 1 001 0 12 00 AM 12:00:00 AM 00:00:00 00 123456 00 01 53 01 Mon Monday 1 0000 0001 0001 01 %`

	d4, _ := types.ParseDatetime("2016-09-3 00:59:59.123456", 6)
	r4 := `Sep September 09 9 3rd 03 3 247 0 12 59 AM 12:59:59 AM 00:59:59 59 123456 35 35 35 35 Sat Saturday 6 2016 2016 2016 16 %`

	return []tcTemp{
		{
			info: "test format",
			typ:  types.T_datetime,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d1},
					[]bool{false}),
				testutil.NewFunctionTestConstInput(types.T_varchar.ToType(),
					[]string{format},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{r1},
				[]bool{false}),
		},
		{
			info: "test format",
			typ:  types.T_datetime,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d2},
					[]bool{false}),
				testutil.NewFunctionTestConstInput(types.T_varchar.ToType(),
					[]string{format},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{r2},
				[]bool{false}),
		},
		{
			info: "test format",
			typ:  types.T_datetime,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d3},
					[]bool{false}),
				testutil.NewFunctionTestConstInput(types.T_varchar.ToType(),
					[]string{format},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{r3},
				[]bool{false}),
		},
		{
			info: "test format",
			typ:  types.T_datetime,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d4},
					[]bool{false}),
				testutil.NewFunctionTestConstInput(types.T_varchar.ToType(),
					[]string{format},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{r4},
				[]bool{false}),
		},
	}
}

func TestFormat(t *testing.T) {
	testCases := initFormatTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, DateFormat)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initDateSubTestCase() []tcTemp {
	d1, _ := types.ParseDatetime("2022-01-01", 6)
	r1, _ := types.ParseDatetime("2021-12-31", 6)
	return []tcTemp{
		{
			info: "test DateAdd",
			typ:  types.T_date,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{d1.ToDate()},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{int64(types.Day)},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_date.ToType(), false,
				[]types.Date{r1.ToDate()},
				[]bool{false}),
		},
		{
			info: "test DatetimeAdd",
			typ:  types.T_datetime,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d1},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{int64(types.Day)},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_datetime.ToType(), false,
				[]types.Datetime{r1},
				[]bool{false}),
		},
		{
			info: "test DatetimeAdd",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"2022-01-01"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{int64(types.Day)},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_datetime.ToType(), false,
				[]types.Datetime{r1},
				[]bool{false}),
		},
	}
}

func TestDateSub(t *testing.T) {
	testCases := initDateSubTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		var fcTC testutil.FunctionTestCase
		switch tc.typ {
		case types.T_date:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, DateSub)
		case types.T_datetime:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, DatetimeSub)
		case types.T_varchar:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, DateStringSub)
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initFieldTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test Field",
			typ:  types.T_uint64,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{0},
					[]bool{true}),
				testutil.NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{1},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{1},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{0},
				[]bool{false}),
		},
		{
			info: "test Field",
			typ:  types.T_uint64,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{1},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{2},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{1},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{2},
				[]bool{false}),
		},
		{
			info: "test Field",
			typ:  types.T_uint64,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{1},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{2},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{3},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{0},
				[]bool{false}),
		},

		{
			info: "test Field",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"hello"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""},
					[]bool{true}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"hello"},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{2},
				[]bool{false}),
		},
	}
}

func TestField(t *testing.T) {
	testCases := initFieldTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		var fcTC testutil.FunctionTestCase
		switch tc.typ {
		case types.T_uint64:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, FieldNumber[uint64])
		case types.T_varchar:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, FieldString)
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initFormat2Or3TestCase() []tcTemp {
	return []tcTemp{
		{
			info: "2",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{"12332.123456"}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{"4"}, []bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"12,332.1235"}, []bool{false}),
		},
		{
			info: "2",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{"12332.1"}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{"4"}, []bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"12,332.1000"}, []bool{false}),
		},
		{
			info: "2",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{"12332.2"}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{"0"}, []bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"12,332"}, []bool{false}),
		},
		{
			info: "2",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{"-.12334.2"}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{"2"}, []bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"-0.12"}, []bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{"12332.123456"}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{"4"}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{"ar_SA"}, []bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"12332.1235"}, []bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{"12332.1"}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{"4"}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{"ar_SA"}, []bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"12332.1000"}, []bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{"12332.2"}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{"0"}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{"ar_SA"}, []bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"12332"}, []bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{"-.12334.2"}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{"2"}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{"ar_SA"}, []bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"-0.12"}, []bool{false}),
		},
	}
}

func TestFormat2Or3(t *testing.T) {
	testCases := initFormat2Or3TestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		var fcTC testutil.FunctionTestCase
		switch tc.info {
		case "2":
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, FormatWith2Args)
		case "3":
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, FormatWith3Args)
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initFromUnixTimeTestCase() []tcTemp {
	d1, _ := types.ParseDatetime("1970-01-01 00:00:00", 6)
	d2, _ := types.ParseDatetime("2016-01-01 00:00:00", 6)
	d3, _ := types.ParseDatetime("2016-01-01 00:00:00.999999", 6)
	return []tcTemp{
		{
			info: "test from unix time int64",
			typ:  types.T_int64,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{0},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_datetime.ToType(), false,
				[]types.Datetime{d1},
				[]bool{false}),
		},
		{
			info: "test from unix time uint64",
			typ:  types.T_uint64,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{1451606400},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_datetime.ToType(), false,
				[]types.Datetime{d2},
				[]bool{false}),
		},
		{
			info: "test from unix time float64",
			typ:  types.T_float64,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1451606400.999999},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_datetime.ToType(), false,
				[]types.Datetime{d3},
				[]bool{false}),
		},
		{
			info: "test from unix time float64",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{0},
					[]bool{false}),
				testutil.NewFunctionTestConstInput(types.T_varchar.ToType(),
					[]string{"%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %x %Y %y"},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"Jan January 01 1 1st 01 1 001 0 12 00 AM 12:00:00 AM 00:00:00 00 000000 01 1970 1970 70"},
				[]bool{false}),
		},
	}
}

func newTmpProcess() *process.Process {
	return newProcessWithMPool(mpool.MustNewZero())
}

func newProcessWithMPool(mp *mpool.MPool) *process.Process {
	process := testutil.NewProcessWithMPool(mp)
	process.SessionInfo.TimeZone = time.FixedZone("UTC0", 0)
	return process
}

func TestFromUnixTime(t *testing.T) {
	testCases := initFromUnixTimeTestCase()

	// do the test work.
	proc := newTmpProcess()
	for _, tc := range testCases {
		var fcTC testutil.FunctionTestCase
		switch tc.typ {
		case types.T_int64:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, FromUnixTimeInt64)
		case types.T_uint64:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, FromUnixTimeUint64)
		case types.T_float64:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, FromUnixTimeFloat64)
		case types.T_varchar:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, FromUnixTimeInt64Format)
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initSubStrTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "2",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abcdefghijklmn"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{5},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"efghijklmn"},
				[]bool{false}),
		},
		{
			info: "2",
			typ:  types.T_blob,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_blob.ToType(),
					[]string{"abcdefghijklmn"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{7},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_blob.ToType(), false,
				[]string{"ghijklmn"},
				[]bool{false}),
		},
		{
			info: "2",
			typ:  types.T_text,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_text.ToType(),
					[]string{"abcdefghijklmn"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{11},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_text.ToType(), false,
				[]string{"klmn"},
				[]bool{false}),
		},

		{
			info: "2",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abcdefghijklmn"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{16},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{""},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abcdefghijklmn"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{5},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{6},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"efghij"},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abcdefghijklmn"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{6},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-8},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{""},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abcdefghijklmn"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{6},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-9},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{""},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abcdefghijklmn"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-4},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{4},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"klmn"},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abcdefghijklmn"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-14},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{14},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"abcdefghijklmn"},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abcdefghijklmn"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-14},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{10},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"abcdefghij"},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abcdefghijklmn"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-12},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{2},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"cd"},
				[]bool{false}),
		},
	}
}

func TestSubStr(t *testing.T) {
	testCases := initSubStrTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		var fcTC testutil.FunctionTestCase
		switch tc.info {
		case "2":
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, SubStringWith2Args)
		case "3":
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, SubStringWith3Args)
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initSubStrIndexTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"www.mysql.com"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"."},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{0},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{""},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"www.mysql.com"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"."},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"www"},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"www.mysql.com"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"."},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{2},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"www.mysql"},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"www.mysql.com"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"."},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{3},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"www.mysql.com"},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"www.mysql.com"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"."},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-3},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"www.mysql.com"},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"www.mysql.com"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"."},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-2},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"mysql.com"},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"www.mysql.com"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"."},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-1},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"com"},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"xyz"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abc"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{223372036854775808},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"xyz"},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aaa.bbb.ccc.ddd.eee"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"."},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{9223372036854775807},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"aaa.bbb.ccc.ddd.eee"},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aaa.bbb.ccc.ddd.eee"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"."},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-9223372036854775808},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"aaa.bbb.ccc.ddd.eee"},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aaa.bbb.ccc.ddd.eee"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"."},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{int64(922337203685477580)},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"aaa.bbb.ccc.ddd.eee"},
				[]bool{false}),
		},
	}
}

func TestSubStrIndex(t *testing.T) {
	testCases := initSubStrIndexTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, SubStrIndex[int64])
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}
