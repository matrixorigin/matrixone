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
	"math"
	"testing"
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
