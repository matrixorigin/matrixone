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

package function

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
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
		{
			info: "test Coalesce vecf32",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(),
					[][]float32{{1, 2, 3}, {}, {4, 5, 6}},
					[]bool{false, true, false}),
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(),
					[][]float32{{1, 2, 3}, {0, 0, 0}, {4, 5, 6}},
					[]bool{true, false, true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{{1, 2, 3}, {0, 0, 0}, {4, 5, 6}},
				[]bool{false, false, false}),
		},
		{
			info: "test Coalesce vecf64",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float64.ToType(),
					[][]float64{{1, 2, 3}, {}, {4, 5, 6}},
					[]bool{false, true, false}),
				testutil.NewFunctionTestInput(types.T_array_float64.ToType(),
					[][]float64{{1, 2, 3}, {0, 0, 0}, {4, 5, 6}},
					[]bool{true, false, true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_array_float64.ToType(), false,
				[][]float64{{1, 2, 3}, {0, 0, 0}, {4, 5, 6}},
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

func initConvertTzTestCase() []tcTemp {
	d1, _ := types.ParseDatetime("2023-01-01 00:00:00", 6)
	r1 := "2022-12-31 13:07:00"
	d2, _ := types.ParseDatetime("2022-01-01 00:00:00", 6)
	r2 := "2021-12-31 16:00:00"
	d3, _ := types.ParseDatetime("9999-12-31 23:00:00", 6)
	r3 := "9999-12-31 23:00:00"
	d4, _ := types.ParseDatetime("9999-12-31 22:00:00", 6)
	r4 := "9999-12-31 22:00:00"
	d5, _ := types.ParseDatetime("9999-12-31 10:00:00", 6)
	r5 := "9999-12-31 18:00:00"
	return []tcTemp{
		{ // select convert_tz('2023-01-01 00:00:00', '+08:21', '-02:32');
			info: "test ConvertTz correct1",
			typ:  types.T_datetime,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d1},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"+08:21"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"-02:32"},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{r1},
				[]bool{false}),
		},
		{ // select convert_tz('2022-01-01 00:00:00', 'Asia/Shanghai', '+00:00');
			info: "test ConvertTz correct2",
			typ:  types.T_datetime,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d2},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"Asia/Shanghai"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"+00:00"},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{r2},
				[]bool{false}),
		},
		{ // select convert_tz('2022-01-01 00:00:00', 'Europe/London', 'Asia/Shanghai');
			info: "test ConvertTz correct3",
			typ:  types.T_datetime,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d2},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"Asia/Shanghai"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"Europe/London"},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{r2},
				[]bool{false}),
		},
		{ // select convert_tz('9999-12-31 23:00:00', '-02:00', '+11:00');
			info: "test ConvertTz out of range1",
			typ:  types.T_datetime,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d3},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"-02:00"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"+11:00"},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{r3},
				[]bool{false}),
		},
		{ // select convert_tz('9999-12-31 22:00:00', 'Europe/London', 'Asia/Shanghai');
			info: "test ConvertTz out of range2",
			typ:  types.T_datetime,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d4},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"Europe/London"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"Asia/Shanghai"},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{r4},
				[]bool{false}),
		},
		{ // select convert_tz('9999-12-31 10:00:00', 'Europe/London', 'Asia/Shanghai');
			info: "test ConvertTz not out of range",
			typ:  types.T_datetime,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d5},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"Europe/London"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"Asia/Shanghai"},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{r5},
				[]bool{false}),
		},
		{
			info: "test ConvertTz err1",
			typ:  types.T_datetime,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d1},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"ABC"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"GMT"},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{""},
				[]bool{true}),
		},
		{
			info: "test ConvertTz err2",
			typ:  types.T_datetime,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d1},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"+00:00"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"ABC"},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{""},
				[]bool{true}),
		},
		{
			info: "test ConvertTz err3",
			typ:  types.T_datetime,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d1},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"+00:00:00"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"+08:00"},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{""},
				[]bool{true}),
		},
		{
			info: "test ConvertTz err4",
			typ:  types.T_datetime,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d1},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"+00:ws"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"+08:00"},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{""},
				[]bool{true}),
		},
		{
			info: "test ConvertTz err5",
			typ:  types.T_datetime,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d1},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"+00:00"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"+18:00"},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{""},
				[]bool{true}),
		},
		{
			info: "test ConvertTz when tz is empty",
			typ:  types.T_datetime,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d3},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{""},
				[]bool{true}),
		},
	}
}

func TestConvertTz(t *testing.T) {
	testCases := initConvertTzTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, ConvertTz)
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

	bb := make([]bool, 10)
	return []tcTemp{
		{
			info: "test timediff time 1",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_time.ToType(), []types.Time{t11, t21, t31, t41, t51, t61, t71, t81, t91, t101}, bb),
				testutil.NewFunctionTestInput(types.T_time.ToType(), []types.Time{t12, t22, t32, t42, t52, t62, t72, t82, t92, t102}, bb),
			},
			expect: testutil.NewFunctionTestResult(types.T_time.ToType(), false, []types.Time{r1, r2, r3, r4, r5, r6, r7, r8, r9, r10}, bb),
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
	// FIXME: Migrating the testcases as it was from the original functions code. May refactor it later. Original code: https://github.com/m-schen/matrixone/blob/0c480ca11b6302de26789f916a3e2faca7f79d47/pkg/sql/plan/function/builtin/multi/timestampdiff_test.go#L35
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

	var testInputs = make([]tcTemp, 0, len(cases))
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

// StartsWith

func initStartsWithTestCase() []tcTemp {
	// FIXME: Migrating the testcases as it was from the original functions code. May refactor it later. Original code:https://github.com/m-schen/matrixone/blob/0c480ca11b6302de26789f916a3e2faca7f79d47/pkg/sql/plan/function/builtin/binary/startswith_test.go#L28
	var charVecBase = []string{"-123", "123", "+123", "8", ""}
	var charVecBase2 = []string{"-", "+", "1", ""}
	var nsp1, nsp2 []uint64
	var origVecs = make([]testutil.FunctionTestInput, 2)
	n1, n2 := len(charVecBase), len(charVecBase2)
	inputVec := make([]string, n1*n2)
	inputVec2 := make([]string, len(inputVec))
	for i := 0; i < len(inputVec); i++ {
		inputVec[i] = charVecBase[i/n2]
		inputVec2[i] = charVecBase2[i%n2]
		if (i / n2) == (n1 - 1) {
			nsp1 = append(nsp1, uint64(i))
		}
		if (i % n2) == (n2 - 1) {
			nsp2 = append(nsp2, uint64(i))
		}
	}

	makeFunctionTestInputEndsWith := func(values []string, nsp []uint64) testutil.FunctionTestInput {
		totalCount := len(values)
		strs := make([]string, totalCount)
		nulls := make([]bool, totalCount)
		for i := 0; i < totalCount; i++ {
			strs[i] = values[i]
		}
		for i := 0; i < len(nsp); i++ {
			idx := nsp[i]
			nulls[idx] = true
		}
		return testutil.NewFunctionTestInput(types.T_varchar.ToType(), strs, nulls)
	}

	origVecs[0] = makeFunctionTestInputEndsWith(inputVec, nsp1)
	origVecs[1] = makeFunctionTestInputEndsWith(inputVec2, nsp2)

	return []tcTemp{
		{
			info: "test StartsWith",
			inputs: []testutil.FunctionTestInput{
				origVecs[0],
				origVecs[1],
			},
			expect: testutil.NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, false, true, false, false, true, true, false, true, false, true, false, false, false, true, false, false, false, true},
				[]bool{false, false, false, true, false, false, false, true, false, false, false, true, false, false, false, true, true, true, true, true}),
		},
	}
}

func TestStartsWith(t *testing.T) {
	testCases := initStartsWithTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, StartsWith)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// EndsWith

func initEndsWithTestCase() []tcTemp {
	// FIXME: Migrating the testcases as it was from the original functions code. May refactor it later. Original code:https://github.com/m-schen/matrixone/blob/0c480ca11b6302de26789f916a3e2faca7f79d47/pkg/sql/plan/function/builtin/binary/endswith_test.go#L29
	var charVecBase = []string{"123-", "321", "123+", "8", ""}
	var charVecBase2 = []string{"-", "+", "1", ""}
	var nsp1, nsp2 []uint64
	var origVecs = make([]testutil.FunctionTestInput, 2)
	n1, n2 := len(charVecBase), len(charVecBase2)
	inputVec := make([]string, n1*n2)
	inputVec2 := make([]string, len(inputVec))
	for i := 0; i < len(inputVec); i++ {
		inputVec[i] = charVecBase[i/n2]
		inputVec2[i] = charVecBase2[i%n2]
		if (i / n2) == (n1 - 1) {
			nsp1 = append(nsp1, uint64(i))
		}
		if (i % n2) == (n2 - 1) {
			nsp2 = append(nsp2, uint64(i))
		}
	}

	makeFunctionTestInputEndsWith := func(values []string, nsp []uint64) testutil.FunctionTestInput {
		totalCount := len(values)
		strs := make([]string, totalCount)
		nulls := make([]bool, totalCount)
		for i := 0; i < totalCount; i++ {
			strs[i] = values[i]
		}
		for i := 0; i < len(nsp); i++ {
			idx := nsp[i]
			nulls[idx] = true
		}
		return testutil.NewFunctionTestInput(types.T_varchar.ToType(), strs, nulls)
	}

	origVecs[0] = makeFunctionTestInputEndsWith(inputVec, nsp1)
	origVecs[1] = makeFunctionTestInputEndsWith(inputVec2, nsp2)

	return []tcTemp{
		{
			info: "test EndsWith",
			inputs: []testutil.FunctionTestInput{
				origVecs[0],
				origVecs[1],
			},
			expect: testutil.NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, false, true, false, false, true, true, false, true, false, true, false, false, false, true, false, false, false, true},
				[]bool{false, false, false, true, false, false, false, true, false, false, false, true, false, false, false, true, true, true, true, true}),
		},
	}
}

func TestEndsWith(t *testing.T) {
	testCases := initEndsWithTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, EndsWith)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// FindInSet

func initFindInSetTestCase() []tcTemp {

	return []tcTemp{
		{
			info: "test findinset",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{
					"abc",
					"xyz",
					"z",
					"abc", //TODO: Ignoring the scalar checks. Please fix. Original code:https://github.com/m-schen/matrixone/blob/0c480ca11b6302de26789f916a3e2faca7f79d47/pkg/sql/plan/function/builtin/binary/findinset_test.go#L67
					"abc",
					"abc",
					"",
					"abc",
				},
					[]bool{
						false,
						false,
						false,
						false,
						false,
						false,
						true,
						false,
					}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{
					"abc,def",
					"dec,xyz,abc",
					"a,e,c,z",
					"abc,def",
					"abc,def",
					"abc,def",
					"abc",
					"",
				},
					[]bool{
						false,
						false,
						false,
						false,
						false,
						false,
						false,
						true,
					}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{
					1,
					2,
					4,
					1,
					1,
					1,
					0,
					0,
				},
				[]bool{
					false,
					false,
					false,
					false,
					false,
					false,
					true,
					true,
				},
			),
		},
	}
}

func TestFindInSet(t *testing.T) {
	testCases := initFindInSetTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, FindInSet)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// INSTR
func initInstrTestCase() []tcTemp {
	cases := []struct {
		strs    []string
		substrs []string
		wants   []int64
	}{
		{
			strs:    []string{"abc", "abc", "abc", "abc", "abc"},
			substrs: []string{"bc", "b", "abc", "a", "dca"},
			wants:   []int64{2, 2, 1, 1, 0},
		},
		{
			strs:    []string{"abc", "abc", "abc", "abc", "abc"},
			substrs: []string{"", "", "a", "b", "c"},
			wants:   []int64{1, 1, 1, 2, 3},
		},
		//TODO: @m-schen. Please fix these. Original code: https://github.com/m-schen/matrixone/blob/0c480ca11b6302de26789f916a3e2faca7f79d47/pkg/sql/plan/function/builtin/binary/instr_test.go#L43
		//{
		//	strs:    []string{"abc", "abc", "abc", "abc", "abc"},
		//	substrs: []string{"bc"},
		//	wants:   []int64{2, 2, 2, 2, 2},
		//},
		//{
		//	strs:    []string{"abc"},
		//	substrs: []string{"bc", "b", "abc", "a", "dca"},
		//	wants:   []int64{2, 2, 1, 1, 0},
		//},
	}

	var testInputs = make([]tcTemp, 0, len(cases))
	for _, c := range cases {

		testInputs = append(testInputs, tcTemp{

			info: "test instr ",
			inputs: []testutil.FunctionTestInput{
				// Create a input entry <strs, substrs>
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), c.strs, []bool{}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), c.substrs, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false, c.wants, []bool{}),
		})
	}

	return testInputs

}

func TestInstr(t *testing.T) {
	testCases := initInstrTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, Instr)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// Left
func initLeftTestCase() []tcTemp {
	cases := []struct {
		s    string
		len  int64
		want string
	}{
		{
			"abcde",
			3,
			"abc",
		},
		{
			"abcde",
			0,
			"",
		},
		{
			"abcde",
			-1,
			"",
		},
		{
			"abcde",
			100,
			"abcde",
		},
		{
			"foobarbar",
			5,
			"fooba",
		},

		// TestLeft1
		{
			"是都方式快递费",
			3,
			"是都方",
		},
		{
			"ｱｲｳｴｵ",
			3,
			"ｱｲｳ",
		},
		{
			"ｱｲｳｴｵ ",
			3,
			"ｱｲｳ",
		},
		{
			"ｱｲｳｴｵ  ",
			3,
			"ｱｲｳ",
		},
		{
			"ｱｲｳｴｵ   ",
			3,
			"ｱｲｳ",
		},
		{
			"あいうえお",
			3,
			"あいう",
		},
		{
			"あいうえお ",
			3,
			"あいう",
		},
		{
			"あいうえお  ",
			3,
			"あいう",
		},
		{
			"あいうえお   ",
			3,
			"あいう",
		},
		{
			"龔龖龗龞龡",
			3,
			"龔龖龗",
		},
		{
			"龔龖龗龞龡 ",
			3,
			"龔龖龗",
		},
		{
			"龔龖龗龞龡  ",
			3,
			"龔龖龗",
		},
		{
			"龔龖龗龞龡   ",
			3,
			"龔龖龗",
		},
		{
			"2017-06-15    ",
			8,
			"2017-06-",
		},
		{
			"2019-06-25    ",
			8,
			"2019-06-",
		},
		{
			"    2019-06-25  ",
			8,
			"    2019",
		},
		{
			"   2019-06-25   ",
			8,
			"   2019-",
		},
		{
			"    2012-10-12   ",
			8,
			"    2012",
		},
		{
			"   2004-04-24.   ",
			8,
			"   2004-",
		},
		{
			"   2008-12-04.  ",
			8,
			"   2008-",
		},
		{
			"    2012-03-23.   ",
			8,
			"    2012",
		},
		{
			"    2013-04-30  ",
			8,
			"    2013",
		},
		{
			"  1994-10-04  ",
			8,
			"  1994-1",
		},
		{
			"   2018-06-04  ",
			8,
			"   2018-",
		},
		{
			" 2012-10-12  ",
			8,
			" 2012-10",
		},
		{
			"1241241^&@%#^*^!@#&*(!&    ",
			12,
			"1241241^&@%#",
		},
		{
			" 123 ",
			2,
			" 1",
		},
	}

	var testInputs = make([]tcTemp, 0, len(cases))
	for _, c := range cases {

		testInputs = append(testInputs, tcTemp{

			//TODO: Avoiding TestLeft2. Original code: https://github.com/m-schen/matrixone/blob/0c480ca11b6302de26789f916a3e2faca7f79d47/pkg/sql/plan/function/builtin/binary/left_test.go#L247
			info: "test left ",
			inputs: []testutil.FunctionTestInput{
				// Create a input entry <str, int>
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{c.s}, []bool{}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{c.len}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false, []string{c.want}, []bool{}),
		})
	}

	return testInputs

}

func TestLeft(t *testing.T) {
	testCases := initLeftTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, Left)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// POWER
func initPowerTestCase() []tcTemp {
	cases := []struct {
		left  float64
		right float64
		want  float64
	}{
		{1, 2, 1},
		{2, 2, 4},
		{3, 2, 9},
		{3, 3, 27},
		{4, 2, 16},
		{4, 3, 64},
		{4, 0.5, 2},
		{5, 2, 25},
		{6, 2, 36},
		{7, 2, 49},
		{8, 2, 64},
		{0.5, 2, 0.25},
		{1.5, 2, 2.25},
		{2.5, 2, 6.25},
		{3.5, 2, 12.25},
		{4.5, 2, 20.25},
		{5.5, 2, 30.25},
	}

	var testInputs = make([]tcTemp, 0, len(cases))
	for _, c := range cases {

		testInputs = append(testInputs, tcTemp{
			info: "test pow ",
			inputs: []testutil.FunctionTestInput{
				// Create a input entry <float64, float64>
				testutil.NewFunctionTestInput(types.T_float64.ToType(), []float64{c.left}, []bool{}),
				testutil.NewFunctionTestInput(types.T_float64.ToType(), []float64{c.right}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false, []float64{c.want}, []bool{}),
		})
	}

	return testInputs
}

func TestPower(t *testing.T) {
	testCases := initPowerTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, Power)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// SQRT
func initSqrtTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test sqrt regular",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1, 4, 2, 10, 25, 10000, 0, 0, 1.41},
					[]bool{false, false, false, false, false, false, true, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{1, 2, 1.4142135623730951, 3.1622776601683795, 5, 100, 0, 0, 1.1874342087037917},
				[]bool{false, false, false, false, false, false, true, false, false, true}),
		},
		{
			info: "test sqrt error",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(), []float64{-2}, nil),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), true, nil, nil),
		},
	}
}

func TestSqrt(t *testing.T) {
	testCases := initSqrtTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInSqrt)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initSqrtArrayTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test sqrt float32 array",
			typ:  types.T_array_float32,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(),
					[][]float32{{4, 9, 16}, {0, 25, 49}},
					[]bool{false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_array_float64.ToType(), false,
				//NOTE: SQRT(vecf32) --> vecf64
				[][]float64{{2, 3, 4}, {0, 5, 7}},
				[]bool{false, false}),
		},
		{
			info: "test sqrt float64 array",
			typ:  types.T_array_float64,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float64.ToType(),
					[][]float64{{4, 9, 16}, {0, 25, 49}},
					[]bool{false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_array_float64.ToType(), false,
				[][]float64{{2, 3, 4}, {0, 5, 7}},
				[]bool{false, false}),
		},
	}
}

func TestSqrtArray(t *testing.T) {
	testCases := initSqrtArrayTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		var fcTC testutil.FunctionTestCase
		switch tc.typ {
		case types.T_array_float32:
			fcTC = testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInSqrtArray[float32])
		case types.T_array_float64:
			fcTC = testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInSqrtArray[float64])
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// Inner Product
func initInnerProductArrayTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test InnerProduct float32 array",
			typ:  types.T_array_float32,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{14, 77},
				[]bool{false, false}),
		},
		{
			info: "test InnerProduct float64 array",
			typ:  types.T_array_float64,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
				testutil.NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{14, 77},
				[]bool{false, false}),
		},
	}
}

func TestInnerProductArray(t *testing.T) {
	testCases := initInnerProductArrayTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		var fcTC testutil.FunctionTestCase
		switch tc.typ {
		case types.T_array_float32:
			fcTC = testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, InnerProductArray[float32])
		case types.T_array_float64:
			fcTC = testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, InnerProductArray[float64])
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// Cosine Similarity
func initCosineSimilarityArrayTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test CosineSimilarity float32 array",
			typ:  types.T_array_float32,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{1, 1},
				[]bool{false, false}),
		},
		{
			info: "test CosineSimilarity float64 array",
			typ:  types.T_array_float64,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
				testutil.NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{1, 1},
				[]bool{false, false}),
		},
	}
}

func TestCosineSimilarityArray(t *testing.T) {
	testCases := initCosineSimilarityArrayTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		var fcTC testutil.FunctionTestCase
		switch tc.typ {
		case types.T_array_float32:
			fcTC = testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, CosineSimilarityArray[float32])
		case types.T_array_float64:
			fcTC = testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, CosineSimilarityArray[float64])
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// L2 Distance
func initL2DistanceArrayTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test L2Distance float32 array",
			typ:  types.T_array_float32,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{10, 20, 30}, {40, 50, 60}}, []bool{false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{33.67491648096547, 78.9746794865291},
				[]bool{false, false}),
		},
		{
			info: "test L2Distance float64 array",
			typ:  types.T_array_float64,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
				testutil.NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{10, 20, 30}, {40, 50, 60}}, []bool{false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{33.67491648096547, 78.9746794865291},
				[]bool{false, false}),
		},
	}
}

func TestL2DistanceArray(t *testing.T) {
	testCases := initL2DistanceArrayTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		var fcTC testutil.FunctionTestCase
		switch tc.typ {
		case types.T_array_float32:
			fcTC = testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, L2DistanceArray[float32])
		case types.T_array_float64:
			fcTC = testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, L2DistanceArray[float64])
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// Cosine Distance
func initCosineDistanceArrayTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test CosineDistance float32 array",
			typ:  types.T_array_float32,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{10, 20, 30}, {5, 6, 7}}, []bool{false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0, 0.0003542540112345671},
				[]bool{false, false}),
		},
		{
			info: "test CosineDistance float64 array",
			typ:  types.T_array_float64,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
				testutil.NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{10, 20, 30}, {5, 6, 7}}, []bool{false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0, 0.0003542540112345671},
				[]bool{false, false}),
		},
	}
}

func TestCosineDistanceArray(t *testing.T) {
	testCases := initCosineDistanceArrayTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		var fcTC testutil.FunctionTestCase
		switch tc.typ {
		case types.T_array_float32:
			fcTC = testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, CosineDistanceArray[float32])
		case types.T_array_float64:
			fcTC = testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, CosineDistanceArray[float64])
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// Extract
func initExtractTestCase() []tcTemp {
	MakeDates := func(values ...string) []types.Date {
		ds := make([]types.Date, len(values))
		for i, s := range values {
			if len(s) == 0 {
				ds[i] = types.Date(0)
			} else {
				d, err := types.ParseDateCast(s)
				if err != nil {
					panic(err)
				}
				ds[i] = d
			}
		}
		return ds
	}

	MakeDateTimes := func(values ...string) []types.Datetime {
		ds := make([]types.Datetime, len(values))
		for i, s := range values {
			if len(s) == 0 {
				ds[i] = types.Datetime(0)
			} else {
				d, err := types.ParseDatetime(s, 6)
				if err != nil {
					panic(err)
				}
				ds[i] = d
			}
		}
		return ds
	}

	return []tcTemp{
		{
			info: "test extractFromDate year",
			typ:  types.T_date,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"year", "year", "year", "year"}, []bool{false, false, false, false}),
				testutil.NewFunctionTestInput(types.T_date.ToType(), MakeDates("2020-01-01", "2021-02-03", "2024-03-04", ""), []bool{false, false, false, true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{2020, 2021, 2024, 1},
				[]bool{false, false, false, true}),
			//TODO: Comments migrated from original code: https://github.com/m-schen/matrixone/blob/0c480ca11b6302de26789f916a3e2faca7f79d47/pkg/sql/plan/function/builtin/binary/extract_test.go#L39
			// XXX why?  This seems to be wrong.  ExtractFromDate "" should error out,
			// but if it does not, we tested the result is 1 in prev check.
			// it should not be null.
			// require.True(t, nulls.Contains(outputVector.GetNulls(), uint64(3)))
		},
		{
			info: "test extractFromDate month",
			typ:  types.T_date,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"month", "month", "month", "month"}, []bool{false, false, false, false}),
				testutil.NewFunctionTestInput(types.T_date.ToType(), MakeDates("2020-01-01", "2021-02-03", "2024-03-04", ""), []bool{false, false, false, true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{1, 2, 3, 1},
				[]bool{false, false, false, true}),
			//TODO: Comments migrated from original code: https://github.com/m-schen/matrixone/blob/0c480ca11b6302de26789f916a3e2faca7f79d47/pkg/sql/plan/function/builtin/binary/extract_test.go#L39
			// XXX same as above.
			// require.True(t, nulls.Contains(outputVector.GetNulls(), uint64(3)))
		},
		{
			info: "test extractFromDate day",
			typ:  types.T_date,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"day", "day", "day", "day"}, []bool{}),
				testutil.NewFunctionTestInput(types.T_date.ToType(), MakeDates("2020-01-01", "2021-02-03", "2024-03-04", ""), []bool{false, false, false, true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{1, 3, 4, 1},
				[]bool{false, false, false, true}),
			//TODO: Comments migrated from original code: https://github.com/m-schen/matrixone/blob/0c480ca11b6302de26789f916a3e2faca7f79d47/pkg/sql/plan/function/builtin/binary/extract_test.go#L39
			// XXX Same
			// require.True(t, nulls.Contains(outputVector.GetNulls(), uint64(3)))
		},
		{
			info: "test extractFromDate year_month",
			typ:  types.T_date,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"year_month", "year_month", "year_month", "year_month"}, []bool{}),
				testutil.NewFunctionTestInput(types.T_date.ToType(), MakeDates("2020-01-01", "2021-02-03", "2024-03-04", ""), []bool{false, false, false, true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{202001, 202102, 202403, 101},
				[]bool{false, false, false, true}),
			//TODO: Comments migrated from original code: https://github.com/m-schen/matrixone/blob/0c480ca11b6302de26789f916a3e2faca7f79d47/pkg/sql/plan/function/builtin/binary/extract_test.go#L39
			// XXX same
			// require.True(t, nulls.Contains(outputVector.GetNulls(), uint64(3)))
		},
		{
			info: "test extractFromDateTime year",
			typ:  types.T_datetime,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"year", "year", "year", "year"}, []bool{}),
				testutil.NewFunctionTestInput(types.T_datetime.ToType(), MakeDateTimes("2020-01-01 11:12:13.0006", "2006-01-02 15:03:04.1234", "2024-03-04 12:13:14", ""), []bool{false, false, false, true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"2020", "2006", "2024", ""},
				[]bool{false, false, false, true}),
		},
	}
}

func TestExtract(t *testing.T) {
	testCases := initExtractTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		var fcTC testutil.FunctionTestCase
		switch tc.typ {
		case types.T_date:
			fcTC = testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, ExtractFromDate)
		case types.T_datetime:
			fcTC = testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, ExtractFromDatetime)
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// REPLACE

func initReplaceTestCase() []tcTemp {
	cases := []struct {
		info   string
		input  [][]string
		expect []string
	}{
		{
			info: "Single string case1",
			input: [][]string{
				{"abc"},
				{"a"},
				{"d"},
			},

			expect: []string{"dbc"},
		},

		{
			info: "Single string case2",
			input: [][]string{
				{".*.*.*"},
				{".*"},
				{"n"},
			},

			expect: []string{"nnn"},
		},

		{
			info: "Single string case3",
			input: [][]string{
				{"当时明月 在 当时"},
				{"当时"},
				{"此时"},
			},

			expect: []string{"此时明月 在 此时"},
		},

		{
			info: "Single string case4",
			input: [][]string{
				{"123"},
				{""},
				{"n"},
			},

			expect: []string{"123"},
		},
		//FIXME: Didn't implement the ReplaceWithArrays. Hence multi string case fails.

		//{
		//	info: "Multi string case1",
		//	input: [][]string{
		//		[]string{"firststring", "secondstring"},
		//		[]string{"st"},
		//		[]string{"re"},
		//	},
		//
		//	expect: []string{"firrerering", "secondrering"},
		//},
		//{
		//	info: "Multi string case2",
		//	input: [][]string{
		//		[]string{"Oneinput"},
		//		[]string{"n"},
		//		[]string{"e", "b"},
		//	},
		//
		//	expect: []string{"Oeeieput", "Obeibput"},
		//},
		//
		//{
		//	info: "Multi string case3",
		//	input: [][]string{
		//		[]string{"aaabbb"},
		//		[]string{"a", "b"},
		//		[]string{"n"},
		//	},
		//
		//	expect: []string{"nnnbbb", "aaannn"},
		//},
		//
		//{
		//	info: "Multi string case4",
		//	input: [][]string{
		//		[]string{"Matrix", "Origin"},
		//		[]string{"a", "i"},
		//		[]string{"b", "d"},
		//	},
		//
		//	expect: []string{"Mbtrix", "Ordgdn"},
		//},
		//
		//{
		//	info: "Scalar case1",
		//	input: [][]string{
		//		[]string{"cool"},
		//		[]string{"o"},
		//		[]string{"a"},
		//	},
		//
		//	expect: []string{"caal"},
		//},
	}

	var testInputs = make([]tcTemp, 0, len(cases))
	for _, c := range cases {

		testInputs = append(testInputs, tcTemp{
			info: "test replace " + c.info,
			inputs: []testutil.FunctionTestInput{
				// Create a input entry <[]string, []string, []string>
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), c.input[0], []bool{}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), c.input[1], []bool{}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), c.input[2], []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false, c.expect, []bool{}),
		})
	}

	return testInputs
}

func TestReplace(t *testing.T) {
	testCases := initReplaceTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, Replace)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// TRIM

func initTrimTestCase() []tcTemp {
	cases := []struct {
		mode     string
		input    string
		trimWord string
		output   string
		info     string
	}{

		{
			mode:     "both",
			input:    "   hello world   ",
			trimWord: " ",
			output:   "hello world",
		},
		{
			mode:     "leading",
			input:    "   hello world   ",
			trimWord: " ",
			output:   "hello world   ",
		},
		{
			mode:     "trailing",
			input:    "   hello world   ",
			trimWord: " ",
			output:   "   hello world",
		},
		{
			mode:     "both",
			input:    "   hello world   ",
			trimWord: "h",
			output:   "   hello world   ",
		},
		{
			mode:     "trailing",
			input:    "   hello world",
			trimWord: "d",
			output:   "   hello worl",
		},
		{
			mode:     "leading",
			input:    "hello world   ",
			trimWord: "h",
			output:   "ello world   ",
		},
		{
			mode:     "both",
			input:    "嗷嗷0k七七",
			trimWord: "七",
			output:   "嗷嗷0k",
		},
		{
			mode:     "leading",
			input:    "嗷嗷0k七七",
			trimWord: "七",
			output:   "嗷嗷0k七七",
		},
		{
			mode:     "trailing",
			input:    "嗷嗷0k七七",
			trimWord: "七",
			output:   "嗷嗷0k",
		},
		{
			mode:     "both",
			input:    "嗷嗷0k七七",
			trimWord: "k七七",
			output:   "嗷嗷0",
		},
		{
			mode:     "leading",
			input:    "嗷嗷0k七七",
			trimWord: "",
			output:   "嗷嗷0k七七",
		},
		{
			mode:     "trailing",
			input:    "",
			trimWord: "嗷嗷0k七七",
			output:   "",
		},
	}

	var testInputs = make([]tcTemp, 0, len(cases))
	for _, c := range cases {

		testInputs = append(testInputs, tcTemp{

			info: "test trim ",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{c.mode}, []bool{}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{c.trimWord}, []bool{}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{c.input}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false, []string{c.output}, []bool{}),
		})
	}

	return testInputs

}

func TestTrim(t *testing.T) {
	testCases := initTrimTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, Trim)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// JSON EXTRACT

func initJsonExtractTestCase() []tcTemp {

	var (
		cases = []struct {
			index        int
			json         string
			path         string
			want         string
			pathNullList []bool
		}{
			{
				index: 0,
				json:  `{"a":1,"b":2,"c":3}`,
				path:  `$.a`,
				want:  `1`,
			},
			{
				index: 1,
				json:  `{"a":1,"b":2,"c":3}`,
				path:  `$.b`,
				want:  `2`,
			},
			{
				index: 2,
				json:  `{"a":{"q":[1,2,3]}}`,
				path:  `$.a.q[1]`,
				want:  `2`,
			},
			{
				index: 3,
				json:  `[{"a":1,"b":2,"c":3},{"a":4,"b":5,"c":6}]`,
				path:  `$[1].a`,
				want:  `4`,
			},
			{
				index: 4,
				json:  `{"a":{"q":[{"a":1},{"a":2},{"a":3}]}}`,
				path:  `$.a.q[1]`,
				want:  `{"a":2}`,
			},
			{
				index: 5,
				json:  `{"a":{"q":[{"a":1},{"a":2},{"a":3}]}}`,
				path:  `$.a.q`,
				want:  `[{"a":1},{"a":2},{"a":3}]`,
			},
			{
				index: 6,
				json:  `[1,2,3]`,
				path:  "$[*]",
				want:  "[1,2,3]",
			},
			{
				index: 7,
				json:  `{"a":[1,2,3,{"b":4}]}`,
				path:  "$.a[3].b",
				want:  "4",
			},
			//{
			//	index: 8,
			//	json:  `{"a":[1,2,3,{"b":4}]}`,
			//	path:  "$.a[3].c",
			//	want:  "null",
			//},
			{
				index: 9,
				json:  `{"a":[1,2,3,{"b":4}],"c":5}`,
				path:  "$.*",
				want:  `[[1,2,3,{"b":4}],5]`,
			},
			{
				index: 10,
				json:  `{"a":[1,2,3,{"a":4}]}`,
				path:  "$**.a",
				want:  `[[1,2,3,{"a":4}],4]`,
			},
			{
				index: 11,
				json:  `{"a":[1,2,3,{"a":4}]}`,
				path:  "$.a[*].a",
				want:  `4`,
			},
			//{
			//	index:        12,
			//	json:         `{"a":[1,2,3,{"a":4}]}`,
			//	pathNullList: []bool{true},
			//	want:         "null",
			//},
		}
	)

	var testInputs = make([]tcTemp, 0, len(cases))
	for _, c := range cases {

		want := make([]string, 1)
		if c.want != "null" {
			bj, _ := types.ParseStringToByteJson(c.want)
			dt, _ := bj.Marshal()
			want[0] = string(dt)
		}

		testInputs = append(testInputs, tcTemp{

			info: "test json_extract " + fmt.Sprint(c.index),
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{c.json}, []bool{}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{c.path}, c.pathNullList),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false, want, []bool{}),
		})
	}

	return testInputs
}

func TestJsonExtract(t *testing.T) {
	testCases := initJsonExtractTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, JsonExtract)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// SPLIT PART

func initSplitPart() []tcTemp {

	//TODO: Need to validate testcases: https://github.com/m-schen/matrixone/blob/3b58fe39a4c233739a8d3b9cd4fcd562fa2a1568/pkg/sql/plan/function/builtin/multi/split_part_test.go#L50
	// I have skipped the scalar testcases. Please add if it is relevant.
	return []tcTemp{
		{
			info: "test split_part",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{"a,b,c"}, []bool{}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{","}, []bool{}),
				testutil.NewFunctionTestInput(types.T_uint32.ToType(), []uint32{1}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"a"}, []bool{}),
		},
		{
			info: "test split_part Error",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{"a,b,c"}, []bool{}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{","}, []bool{}),
				testutil.NewFunctionTestInput(types.T_uint32.ToType(), []uint32{0}, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), true, []string{"a"}, []bool{}),
		},
	}

}

func TestSplitPart(t *testing.T) {
	testCases := initSplitPart()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, SplitPart)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}
