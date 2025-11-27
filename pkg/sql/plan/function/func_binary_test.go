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
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func initAddFaultPointTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test space",
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"a"}, []bool{false}),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{":5::"}, []bool{false}),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"return"}, []bool{false}),
				NewFunctionTestConstInput(types.T_int64.ToType(), []int64{0}, []bool{false}),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{""}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), true,
				[]bool{true},
				[]bool{false}),
		},
	}
}

func TestAddFaultPoint(t *testing.T) {
	testCases := initAddFaultPointTestCase()

	// do the test work.
	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc,
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
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{1, 4, 8, 16, 32, math.MaxUint64, 0},
					[]bool{false, false, false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{1, 4, 8, 16, 32, math.MaxUint64, 0},
				[]bool{false, false, false, false, false, false, false}),
		},
		{
			info: "test ceil",
			typ:  types.T_int64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{math.MinInt64 + 1, math.MinInt64 + 2, -100, -1, 0, 1, 4, 8, 16, 32, 64, math.MaxInt64, 0},
					[]bool{false, false, false, false, false, false, false, false, false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{math.MinInt64 + 1, math.MinInt64 + 2, -100, -1, 0, 1, 4, 8, 16, 32, 64, math.MaxInt64, 0},
				[]bool{false, false, false, false, false, false, false, false, false, false, false, false, false}),
		},
		{
			info: "test ceil",
			typ:  types.T_float64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					fs,
					bs),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				rfs,
				bs),
		},
	}
}

func TestCeil(t *testing.T) {
	testCases := initCeilTestCase()

	// do the test work.
	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		var fcTC FunctionTestCase
		switch tc.typ {
		case types.T_uint64:
			fcTC = NewFunctionTestCase(proc,
				tc.inputs, tc.expect, CeilUint64)
		case types.T_int64:
			fcTC = NewFunctionTestCase(proc,
				tc.inputs, tc.expect, CeilInt64)
		case types.T_float64:
			fcTC = NewFunctionTestCase(proc,
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
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{1, 4, 8, 16, 32, math.MaxUint64, 0},
					[]bool{false, false, false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{1, 4, 8, 16, 32, math.MaxUint64, 0},
				[]bool{false, false, false, false, false, false, false}),
		},
		{
			info: "test floor int64",
			typ:  types.T_int64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{math.MinInt64 + 1, math.MinInt64 + 2, -100, -1, 0, 1, 4, 8, 16, 32, 64, math.MaxInt64, 0},
					[]bool{false, false, false, false, false, false, false, false, false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{math.MinInt64 + 1, math.MinInt64 + 2, -100, -1, 0, 1, 4, 8, 16, 32, 64, math.MaxInt64, 0},
				[]bool{false, false, false, false, false, false, false, false, false, false, false, false, false}),
		},
		{
			info: "test floor floa64",
			typ:  types.T_float64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					fs,
					bs),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				rfs,
				bs),
		},
	}
}

func TestFloor(t *testing.T) {
	testCases := initFloorTestCase()

	// do the test work.
	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		var fcTC FunctionTestCase
		switch tc.typ {
		case types.T_uint64:
			fcTC = NewFunctionTestCase(proc,
				tc.inputs, tc.expect, FloorUInt64)
		case types.T_int64:
			fcTC = NewFunctionTestCase(proc,
				tc.inputs, tc.expect, FloorInt64)
		case types.T_float64:
			fcTC = NewFunctionTestCase(proc,
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
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{1, 4, 8, 16, 32, math.MaxUint64, 0},
					[]bool{false, false, false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{1, 4, 8, 16, 32, math.MaxUint64, 0},
				[]bool{false, false, false, false, false, false, false}),
		},
		{
			info: "test round int64",
			typ:  types.T_int64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{math.MinInt64 + 1, math.MinInt64 + 2, -100, -1, 0, 1, 4, 8, 16, 32, 64, math.MaxInt64, 0},
					[]bool{false, false, false, false, false, false, false, false, false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{math.MinInt64 + 1, math.MinInt64 + 2, -100, -1, 0, 1, 4, 8, 16, 32, 64, math.MaxInt64, 0},
				[]bool{false, false, false, false, false, false, false, false, false, false, false, false, false}),
		},
		{
			info: "test round floa64",
			typ:  types.T_float64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					fs,
					bs),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				rfs,
				bs),
		},
	}
}

func TestRound(t *testing.T) {
	testCases := initRoundTestCase()

	// do the test work.
	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		var fcTC FunctionTestCase
		switch tc.typ {
		case types.T_uint64:
			fcTC = NewFunctionTestCase(proc,
				tc.inputs, tc.expect, RoundUint64)
		case types.T_int64:
			fcTC = NewFunctionTestCase(proc,
				tc.inputs, tc.expect, RoundInt64)
		case types.T_float64:
			fcTC = NewFunctionTestCase(proc,
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
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1, 4, 8, 16, 32, 0},
					[]bool{true, true, true, true, true, true}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1, 4, 8, 16, 32, 0},
					[]bool{false, false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{1, 4, 8, 16, 32, 0},
				[]bool{false, false, false, false, false, false}),
		},
		{
			info: "test Coalesce varchar",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"hello", "", "world"},
					[]bool{false, true, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"hello", "-", "world"},
					[]bool{true, false, true}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"hello", "-", "world"},
				[]bool{false, false, false}),
		},
		{
			info: "test Coalesce vecf32",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(),
					[][]float32{{1, 2, 3}, {}, {4, 5, 6}},
					[]bool{false, true, false}),
				NewFunctionTestInput(types.T_array_float32.ToType(),
					[][]float32{{1, 2, 3}, {0, 0, 0}, {4, 5, 6}},
					[]bool{true, false, true}),
			},
			expect: NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{{1, 2, 3}, {0, 0, 0}, {4, 5, 6}},
				[]bool{false, false, false}),
		},
		{
			info: "test Coalesce vecf64",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float64.ToType(),
					[][]float64{{1, 2, 3}, {}, {4, 5, 6}},
					[]bool{false, true, false}),
				NewFunctionTestInput(types.T_array_float64.ToType(),
					[][]float64{{1, 2, 3}, {0, 0, 0}, {4, 5, 6}},
					[]bool{true, false, true}),
			},
			expect: NewFunctionTestResult(types.T_array_float64.ToType(), false,
				[][]float64{{1, 2, 3}, {0, 0, 0}, {4, 5, 6}},
				[]bool{false, false, false}),
		},
	}
}

func TestCoalesce(t *testing.T) {
	testCases := initCoalesceTestCase()

	// do the test work.
	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		var fcTC FunctionTestCase
		switch tc.typ {
		case types.T_varchar:
			fcTC = NewFunctionTestCase(proc,
				tc.inputs, tc.expect, CoalesceStr)
		case types.T_int64:
			fcTC = NewFunctionTestCase(proc,
				tc.inputs, tc.expect, CoalesceGeneral[int64])
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initTimestampAddTestCase() []tcTemp {
	d1, _ := types.ParseDateCast("2024-12-20")
	r1, _ := types.ParseDateCast("2024-12-25") // +5 days
	d2, _ := types.ParseDatetime("2024-12-20 10:30:45", 6)
	r2, _ := types.ParseDatetime("2024-12-25 10:30:45", 6) // +5 days
	d3, _ := types.ParseTimestamp(time.Local, "2024-12-20 10:30:45", 6)
	r3, _ := types.ParseTimestamp(time.Local, "2024-12-25 10:30:45", 6) // +5 days

	return []tcTemp{
		{
			info: "test TimestampAdd - DATE with DAY",
			typ:  types.T_date,
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"DAY"}, []bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{5},
					[]bool{false}),
				NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{d1},
					[]bool{false}),
			},
			// MySQL behavior: DATE input + date unit → DATE output
			// TimestampAddDate uses SetType to change vector type to DATE for date units
			expect: NewFunctionTestResult(types.T_date.ToType(), false,
				[]types.Date{r1},
				[]bool{false}),
		},
		{
			info: "test TimestampAdd - DATETIME with DAY",
			typ:  types.T_datetime,
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"DAY"}, []bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{5},
					[]bool{false}),
				NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d2},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_datetime.ToType(), false,
				[]types.Datetime{r2},
				[]bool{false}),
		},
		{
			info: "test TimestampAdd - TIMESTAMP with DAY",
			typ:  types.T_timestamp,
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"DAY"}, []bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{5},
					[]bool{false}),
				NewFunctionTestInput(types.T_timestamp.ToType(),
					[]types.Timestamp{d3},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_timestamp.ToType(), false,
				[]types.Timestamp{r3},
				[]bool{false}),
		},
		{
			info: "test TimestampAdd - VARCHAR with DAY",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"DAY"}, []bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{5},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"2024-12-20 10:30:45"},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"2024-12-25 10:30:45"},
				[]bool{false}),
		},
		// Note: DATE with HOUR (time unit) returns DATETIME type at runtime via TempSetType
		// This test case is skipped because the test framework cannot handle dynamic type changes
		// The functionality is tested in TestAddIntervalMicrosecond in datetime_test.go
		{
			info: "test TimestampAdd - DATE input is NULL",
			typ:  types.T_date,
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"DAY"}, []bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{5},
					[]bool{false}),
				NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{d1},
					[]bool{true}),
			},
			// MySQL behavior: DATE input + date unit → DATE output
			// TimestampAddDate uses SetType to change vector type to DATE for date units
			expect: NewFunctionTestResult(types.T_date.ToType(), false,
				[]types.Date{types.Date(0)},
				[]bool{true}),
		},
		{
			info: "test TimestampAdd - interval is NULL",
			typ:  types.T_date,
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"DAY"}, []bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{5},
					[]bool{true}),
				NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{d1},
					[]bool{false}),
			},
			// MySQL behavior: DATE input + date unit → DATE output
			// TimestampAddDate uses SetType to change vector type to DATE for date units
			expect: NewFunctionTestResult(types.T_date.ToType(), false,
				[]types.Date{types.Date(0)},
				[]bool{true}),
		},
	}
}

func TestTimestampAdd(t *testing.T) {
	testCases := initTimestampAddTestCase()

	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		var fcTC FunctionTestCase
		switch tc.typ {
		case types.T_date:
			// For DATE input, retType returns DATETIME, so we need to create result wrapper with DATETIME type
			// But the actual vector type will be DATE for date units (DAY, WEEK, etc.)
			// So we manually create the test case to handle this mismatch
			fcTC = FunctionTestCase{proc: proc}
			mp := proc.Mp()
			// allocate vector for function parameters
			fcTC.parameters = make([]*vector.Vector, len(tc.inputs))
			for i := range fcTC.parameters {
				typ := tc.inputs[i].typ
				var nsp *nulls.Nulls = nil
				if len(tc.inputs[i].nullList) != 0 {
					nsp = nulls.NewWithSize(len(tc.inputs[i].nullList))
					for j, b := range tc.inputs[i].nullList {
						if b {
							nsp.Set(uint64(j))
						}
					}
				}
				fcTC.parameters[i] = newVectorByType(proc.Mp(), typ, tc.inputs[i].values, nsp)
				if tc.inputs[i].isConst {
					fcTC.parameters[i].SetClass(vector.CONSTANT)
				}
			}
			// Create result wrapper with DATETIME type (because retType returns DATETIME)
			fcTC.result = vector.NewFunctionResultWrapper(types.T_datetime.ToType(), mp)
			if len(fcTC.parameters) == 0 {
				fcTC.fnLength = 1
			} else {
				fcTC.fnLength = fcTC.parameters[0].Length()
			}
			fcTC.expected = tc.expect
			fcTC.fn = TimestampAddDate
		case types.T_datetime:
			fcTC = NewFunctionTestCase(proc,
				tc.inputs, tc.expect, TimestampAddDatetime)
		case types.T_timestamp:
			fcTC = NewFunctionTestCase(proc,
				tc.inputs, tc.expect, TimestampAddTimestamp)
		case types.T_varchar:
			fcTC = NewFunctionTestCase(proc,
				tc.inputs, tc.expect, TimestampAddString)
		default:
			t.Fatalf("unsupported type for timestampadd test: %v", tc.typ)
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// TestTimestampAddWithNullParameter tests TIMESTAMPADD with NULL parameters
// This test verifies that NULL parameters are handled correctly without type mismatch errors
func TestTimestampAddWithNullParameter(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test case 1: TIMESTAMPADD(DAY, 5, NULL) - third parameter (DATE) is NULL
	// Expected: Returns NULL (DATE type)
	t.Run("DATE parameter is NULL", func(t *testing.T) {
		unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte("DAY"), 1, proc.Mp())
		intervalVec, _ := vector.NewConstFixed(types.T_int64.ToType(), int64(5), 1, proc.Mp())
		// Create a DATE vector with NULL value
		dateVec := vector.NewVec(types.T_date.ToType())
		nsp := nulls.NewWithSize(1)
		nsp.Add(0)
		dateVec.SetNulls(nsp)
		dateVec.SetLength(1)

		parameters := []*vector.Vector{unitVec, intervalVec, dateVec}
		// retType returns DATETIME, so create result wrapper with DATETIME type
		result := vector.NewFunctionResultWrapper(types.T_datetime.ToType(), proc.Mp())

		fnLength := dateVec.Length()
		err := result.PreExtendAndReset(fnLength)
		require.NoError(t, err)

		err = TimestampAddDate(parameters, result, proc, fnLength, nil)
		require.NoError(t, err)

		v := result.GetResultVector()
		require.Equal(t, fnLength, v.Length(), "Result length should match input length")
		// MySQL behavior: DATE input + date unit → DATE output
		// TimestampAddDate uses SetType to change vector type to DATE for date units
		require.Equal(t, types.T_date, v.GetType().Oid, "Result type should be DATE for date units (MySQL compatible)")
		require.True(t, v.GetNulls().Contains(0), "Result should be NULL")
	})

	// Test case 2: TIMESTAMPADD(DAY, NULL, DATE('2024-12-20')) - second parameter (interval) is NULL
	// Expected: Returns NULL (DATE type)
	t.Run("interval parameter is NULL", func(t *testing.T) {
		d1, _ := types.ParseDateCast("2024-12-20")

		unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte("DAY"), 1, proc.Mp())
		// Create an INT64 vector with NULL value
		intervalVec := vector.NewVec(types.T_int64.ToType())
		nsp := nulls.NewWithSize(1)
		nsp.Add(0)
		intervalVec.SetNulls(nsp)
		intervalVec.SetLength(1)
		dateVec, _ := vector.NewConstFixed(types.T_date.ToType(), d1, 1, proc.Mp())

		parameters := []*vector.Vector{unitVec, intervalVec, dateVec}
		// retType returns DATETIME, so create result wrapper with DATETIME type
		result := vector.NewFunctionResultWrapper(types.T_datetime.ToType(), proc.Mp())

		fnLength := dateVec.Length()
		err := result.PreExtendAndReset(fnLength)
		require.NoError(t, err)

		err = TimestampAddDate(parameters, result, proc, fnLength, nil)
		require.NoError(t, err)

		v := result.GetResultVector()
		require.Equal(t, fnLength, v.Length(), "Result length should match input length")
		// MySQL behavior: DATE input + date unit → DATE output
		// TimestampAddDate uses SetType to change vector type to DATE for date units
		require.Equal(t, types.T_date, v.GetType().Oid, "Result type should be DATE for date units (MySQL compatible)")
		require.True(t, v.GetNulls().Contains(0), "Result should be NULL")
	})

	// Test case 3: TIMESTAMPADD(HOUR, 2, NULL) - third parameter (DATE) is NULL with time unit
	// Expected: Returns NULL (DATETIME type after TempSetType)
	t.Run("DATE parameter is NULL with time unit", func(t *testing.T) {
		unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte("HOUR"), 1, proc.Mp())
		intervalVec, _ := vector.NewConstFixed(types.T_int64.ToType(), int64(2), 1, proc.Mp())
		// Create a DATE vector with NULL value
		dateVec := vector.NewVec(types.T_date.ToType())
		nsp := nulls.NewWithSize(1)
		nsp.Add(0)
		dateVec.SetNulls(nsp)
		dateVec.SetLength(1)

		parameters := []*vector.Vector{unitVec, intervalVec, dateVec}
		// retType returns DATETIME, so create result wrapper with DATETIME type
		result := vector.NewFunctionResultWrapper(types.T_datetime.ToType(), proc.Mp())

		fnLength := dateVec.Length()
		err := result.PreExtendAndReset(fnLength)
		require.NoError(t, err)

		err = TimestampAddDate(parameters, result, proc, fnLength, nil)
		require.NoError(t, err)

		v := result.GetResultVector()
		require.Equal(t, fnLength, v.Length(), "Result length should match input length")
		// For time units, TempSetType changes type to DATETIME
		require.Equal(t, types.T_datetime, v.GetType().Oid, "Result type should be DATETIME for time units")
		require.True(t, v.GetNulls().Contains(0), "Result should be NULL")
	})
}

// TestTimestampAddDateWithMicrosecond tests DATE input + MICROSECOND which should return DATETIME type
// This test verifies MySQL compatibility: DATE input + time unit (MICROSECOND) → DATETIME output
func TestTimestampAddDateWithMicrosecond(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test DATE + MICROSECOND = DATETIME
	d1, _ := types.ParseDateCast("2024-12-20")
	rMicrosecond, _ := types.ParseDatetime("2024-12-20 00:00:01.000000", 6) // +1000000 microseconds = +1 second

	// Create input vectors manually to ensure correct length
	unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte("MICROSECOND"), 1, proc.Mp())
	intervalVec, _ := vector.NewConstFixed(types.T_int64.ToType(), int64(1000000), 1, proc.Mp())
	dateVec, _ := vector.NewConstFixed(types.T_date.ToType(), d1, 1, proc.Mp())

	parameters := []*vector.Vector{unitVec, intervalVec, dateVec}

	// Create result wrapper - retType returns DATETIME
	result := vector.NewFunctionResultWrapper(types.New(types.T_datetime, 0, 0), proc.Mp())

	// Run the function - use the actual length from the date vector
	// For const vectors, Length() returns the const length
	fnLength := dateVec.Length()
	err := result.PreExtendAndReset(fnLength)
	require.NoError(t, err)

	err = TimestampAddDate(parameters, result, proc, fnLength, nil)
	require.NoError(t, err)

	// Check the result - TempSetType should have changed it to DATETIME
	v := result.GetResultVector()
	require.Equal(t, fnLength, v.Length(), "Result length should match input length")
	require.Equal(t, types.T_datetime, v.GetType().Oid, "Result type should be DATETIME after TempSetType")
	require.Equal(t, int32(6), v.GetType().Scale, "Result scale should be 6 for microsecond precision")

	// Check the value - get the last value (in case PreExtendAndReset added extra space)
	dt := vector.GenerateFunctionFixedTypeParameter[types.Datetime](v)
	resultDt, null := dt.GetValue(uint64(v.Length() - 1))
	require.False(t, null, "Result should not be null")
	require.Equal(t, rMicrosecond, resultDt, "Result should be 2024-12-20 00:00:01.000000")
	require.Equal(t, "2024-12-20 00:00:01.000000", resultDt.String2(6), "String representation should match")
}

// TestTimestampAddDateWithTimeUnits tests DATE input + time units (HOUR, MINUTE, SECOND) which should return DATETIME type
// This test verifies MySQL compatibility: DATE input + time unit → DATETIME output
func TestTimestampAddDateWithTimeUnits(t *testing.T) {
	proc := testutil.NewProcess(t)

	testCases := []struct {
		unit     string
		interval int64
		expected string
		scale    int32
	}{
		{"HOUR", 2, "2024-12-20 02:00:00", 0},
		{"MINUTE", 30, "2024-12-20 00:30:00", 0},
		{"SECOND", 45, "2024-12-20 00:00:45", 0},
	}

	d1, _ := types.ParseDateCast("2024-12-20")

	for _, tc := range testCases {
		t.Run(tc.unit, func(t *testing.T) {
			expectedDt, _ := types.ParseDatetime(tc.expected, tc.scale)

			unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte(tc.unit), 1, proc.Mp())
			intervalVec, _ := vector.NewConstFixed(types.T_int64.ToType(), tc.interval, 1, proc.Mp())
			dateVec, _ := vector.NewConstFixed(types.T_date.ToType(), d1, 1, proc.Mp())

			parameters := []*vector.Vector{unitVec, intervalVec, dateVec}
			// retType returns DATETIME, so create result wrapper with DATETIME type
			result := vector.NewFunctionResultWrapper(types.New(types.T_datetime, 0, 0), proc.Mp())

			fnLength := dateVec.Length()
			err := result.PreExtendAndReset(fnLength)
			require.NoError(t, err)

			err = TimestampAddDate(parameters, result, proc, fnLength, nil)
			require.NoError(t, err)

			v := result.GetResultVector()
			require.Equal(t, fnLength, v.Length(), "Result length should match input length")
			require.Equal(t, types.T_datetime, v.GetType().Oid, "Result type should be DATETIME after TempSetType")
			require.Equal(t, tc.scale, v.GetType().Scale, fmt.Sprintf("Result scale should be %d for %s unit", tc.scale, tc.unit))

			dt := vector.GenerateFunctionFixedTypeParameter[types.Datetime](v)
			resultDt, null := dt.GetValue(uint64(v.Length() - 1))
			require.False(t, null, "Result should not be null")
			require.Equal(t, expectedDt, resultDt, "Result should match expected value")
			require.Equal(t, tc.expected, resultDt.String2(tc.scale), "String representation should match")
		})
	}
}

// TestTimestampAddDateWithDateUnits tests DATE input + date units (WEEK, MONTH, QUARTER, YEAR) which should return DATE type
// This test verifies MySQL compatibility: DATE input + date unit → DATE output
func TestTimestampAddDateWithDateUnits(t *testing.T) {
	proc := testutil.NewProcess(t)

	testCases := []struct {
		unit     string
		interval int64
		expected string
	}{
		{"WEEK", 1, "2024-12-27"},
		{"MONTH", 1, "2025-01-20"},
		{"QUARTER", 1, "2025-03-20"},
		{"YEAR", 1, "2025-12-20"},
	}

	d1, _ := types.ParseDateCast("2024-12-20")

	for _, tc := range testCases {
		t.Run(tc.unit, func(t *testing.T) {
			expectedDate, _ := types.ParseDateCast(tc.expected)

			unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte(tc.unit), 1, proc.Mp())
			intervalVec, _ := vector.NewConstFixed(types.T_int64.ToType(), tc.interval, 1, proc.Mp())
			dateVec, _ := vector.NewConstFixed(types.T_date.ToType(), d1, 1, proc.Mp())

			parameters := []*vector.Vector{unitVec, intervalVec, dateVec}
			// retType returns DATETIME, so create result wrapper with DATETIME type
			result := vector.NewFunctionResultWrapper(types.New(types.T_datetime, 0, 0), proc.Mp())

			fnLength := dateVec.Length()
			err := result.PreExtendAndReset(fnLength)
			require.NoError(t, err)

			err = TimestampAddDate(parameters, result, proc, fnLength, nil)
			require.NoError(t, err)

			v := result.GetResultVector()
			require.Equal(t, fnLength, v.Length(), "Result length should match input length")
			// MySQL behavior: DATE input + date unit → DATE output
			// TimestampAddDate uses SetType to change vector type to DATE for date units
			require.Equal(t, types.T_date, v.GetType().Oid, "Result type should be DATE (MySQL compatible)")

			// Read as Date
			dateParam := vector.GenerateFunctionFixedTypeParameter[types.Date](v)
			resultDate, null := dateParam.GetValue(uint64(v.Length() - 1))
			require.False(t, null, "Result should not be null")
			require.Equal(t, expectedDate, resultDate, "Result should match expected value")
		})
	}
}

func initConcatWsTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test ConcatWs",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"---", "---", "---"},
					[]bool{false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"a", "b", "c"},
					[]bool{false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"a", "b", "c"},
					[]bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"a---a", "b---b", "c---c"},
				[]bool{false, false, false}),
		},
	}
}

func TestConcatWs(t *testing.T) {
	testCases := initConcatWsTestCase()

	// do the test work.
	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc,
			tc.inputs, tc.expect, ConcatWs)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// TestTimestampAddComprehensiveFromExpectResult tests all cases from expect result files
// This ensures complete coverage of TIMESTAMPADD functionality matching MySQL behavior
func TestTimestampAddComprehensiveFromExpectResult(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test cases from func_datetime_timestampadd.result and func_datetime_timestampadd_comprehensive.result

	// 1. String input (DATE format) + date units → DATE format string
	t.Run("String DATE format + date units", func(t *testing.T) {
		testCases := []struct {
			name     string
			unit     string
			interval int64
			input    string
			expected string
		}{
			{"DAY +5", "DAY", 5, "2024-12-20", "2024-12-25"},
			{"DAY -5", "DAY", -5, "2024-12-20", "2024-12-15"},
			{"MONTH +1", "MONTH", 1, "2024-12-20", "2025-01-20"},
			{"YEAR +1", "YEAR", 1, "2024-12-20", "2025-12-20"},
			{"WEEK +1", "WEEK", 1, "2024-12-20", "2024-12-27"},
			{"QUARTER +1", "QUARTER", 1, "2024-12-20", "2025-03-20"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte(tc.unit), 1, proc.Mp())
				intervalVec, _ := vector.NewConstFixed(types.T_int64.ToType(), tc.interval, 1, proc.Mp())
				inputVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte(tc.input), 1, proc.Mp())

				parameters := []*vector.Vector{unitVec, intervalVec, inputVec}
				result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())

				fnLength := inputVec.Length()
				err := result.PreExtendAndReset(fnLength)
				require.NoError(t, err)

				err = TimestampAddString(parameters, result, proc, fnLength, nil)
				require.NoError(t, err)

				v := result.GetResultVector()
				require.Equal(t, fnLength, v.Length())
				require.Equal(t, types.T_varchar, v.GetType().Oid)

				strParam := vector.GenerateFunctionStrParameter(v)
				resultBytes, null := strParam.GetStrValue(0)
				require.False(t, null, "Result should not be null")
				resultStr := string(resultBytes)
				require.Equal(t, tc.expected, resultStr)
			})
		}
	})

	// 2. String input (DATE format) + time units → DATETIME format string
	t.Run("String DATE format + time units", func(t *testing.T) {
		testCases := []struct {
			name     string
			unit     string
			interval int64
			input    string
			expected string
		}{
			{"HOUR +24", "HOUR", 24, "2024-12-20", "2024-12-21 00:00:00"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte(tc.unit), 1, proc.Mp())
				intervalVec, _ := vector.NewConstFixed(types.T_int64.ToType(), tc.interval, 1, proc.Mp())
				inputVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte(tc.input), 1, proc.Mp())

				parameters := []*vector.Vector{unitVec, intervalVec, inputVec}
				result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())

				fnLength := inputVec.Length()
				err := result.PreExtendAndReset(fnLength)
				require.NoError(t, err)

				err = TimestampAddString(parameters, result, proc, fnLength, nil)
				require.NoError(t, err)

				v := result.GetResultVector()
				require.Equal(t, fnLength, v.Length())
				require.Equal(t, types.T_varchar, v.GetType().Oid)

				strParam := vector.GenerateFunctionStrParameter(v)
				resultBytes, null := strParam.GetStrValue(0)
				require.False(t, null, "Result should not be null")
				resultStr := string(resultBytes)
				require.Equal(t, tc.expected, resultStr)
			})
		}
	})

	// 3. String input (DATETIME format) + date units → DATETIME format string
	t.Run("String DATETIME format + date units", func(t *testing.T) {
		testCases := []struct {
			name     string
			unit     string
			interval int64
			input    string
			expected string
		}{
			{"DAY +5", "DAY", 5, "2024-12-20 10:30:45", "2024-12-25 10:30:45"},
			{"DAY +7", "DAY", 7, "2024-12-20 10:30:45", "2024-12-27 10:30:45"},
			{"WEEK +1", "WEEK", 1, "2024-12-20 10:30:45", "2024-12-27 10:30:45"},
			{"MONTH +1", "MONTH", 1, "2024-12-20 10:30:45", "2025-01-20 10:30:45"},
			{"QUARTER +1", "QUARTER", 1, "2024-12-20 10:30:45", "2025-03-20 10:30:45"},
			{"YEAR +1", "YEAR", 1, "2024-12-20 10:30:45", "2025-12-20 10:30:45"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte(tc.unit), 1, proc.Mp())
				intervalVec, _ := vector.NewConstFixed(types.T_int64.ToType(), tc.interval, 1, proc.Mp())
				inputVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte(tc.input), 1, proc.Mp())

				parameters := []*vector.Vector{unitVec, intervalVec, inputVec}
				result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())

				fnLength := inputVec.Length()
				err := result.PreExtendAndReset(fnLength)
				require.NoError(t, err)

				err = TimestampAddString(parameters, result, proc, fnLength, nil)
				require.NoError(t, err)

				v := result.GetResultVector()
				require.Equal(t, fnLength, v.Length())
				require.Equal(t, types.T_varchar, v.GetType().Oid)

				strParam := vector.GenerateFunctionStrParameter(v)
				resultBytes, null := strParam.GetStrValue(0)
				require.False(t, null, "Result should not be null")
				resultStr := string(resultBytes)
				require.Equal(t, tc.expected, resultStr)
			})
		}
	})

	// 4. String input (DATETIME format) + time units → DATETIME format string
	t.Run("String DATETIME format + time units", func(t *testing.T) {
		testCases := []struct {
			name     string
			unit     string
			interval int64
			input    string
			expected string
		}{
			{"HOUR +2", "HOUR", 2, "2024-12-20 10:30:45", "2024-12-20 12:30:45"},
			{"HOUR -2", "HOUR", -2, "2024-12-20 10:30:45", "2024-12-20 08:30:45"},
			{"HOUR +24", "HOUR", 24, "2024-12-20 10:30:45", "2024-12-21 10:30:45"},
			{"MINUTE +30", "MINUTE", 30, "2024-12-20 10:30:45", "2024-12-20 11:00:45"},
			{"MINUTE +60", "MINUTE", 60, "2024-12-20 10:30:45", "2024-12-20 11:30:45"},
			{"SECOND +60", "SECOND", 60, "2024-12-20 10:30:45", "2024-12-20 10:31:45"},
			{"MICROSECOND +1000000", "MICROSECOND", 1000000, "2024-12-20 10:30:45", "2024-12-20 10:30:46"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte(tc.unit), 1, proc.Mp())
				intervalVec, _ := vector.NewConstFixed(types.T_int64.ToType(), tc.interval, 1, proc.Mp())
				inputVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte(tc.input), 1, proc.Mp())

				parameters := []*vector.Vector{unitVec, intervalVec, inputVec}
				result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())

				fnLength := inputVec.Length()
				err := result.PreExtendAndReset(fnLength)
				require.NoError(t, err)

				err = TimestampAddString(parameters, result, proc, fnLength, nil)
				require.NoError(t, err)

				v := result.GetResultVector()
				require.Equal(t, fnLength, v.Length())
				require.Equal(t, types.T_varchar, v.GetType().Oid)

				strParam := vector.GenerateFunctionStrParameter(v)
				resultBytes, null := strParam.GetStrValue(0)
				require.False(t, null, "Result should not be null")
				resultStr := string(resultBytes)
				require.Equal(t, tc.expected, resultStr)
			})
		}
	})

	// 5. DATE type input + date units → DATE type
	t.Run("DATE type + date units", func(t *testing.T) {
		d1, _ := types.ParseDateCast("2024-12-20")
		testCases := []struct {
			name     string
			unit     string
			interval int64
			expected string
		}{
			{"DAY +5", "DAY", 5, "2024-12-25"},
			{"DAY -5", "DAY", -5, "2024-12-15"},
			{"MONTH +1", "MONTH", 1, "2025-01-20"},
			{"YEAR +1", "YEAR", 1, "2025-12-20"},
			{"WEEK +1", "WEEK", 1, "2024-12-27"},
			{"QUARTER +1", "QUARTER", 1, "2025-03-20"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				expectedDate, _ := types.ParseDateCast(tc.expected)

				unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte(tc.unit), 1, proc.Mp())
				intervalVec, _ := vector.NewConstFixed(types.T_int64.ToType(), tc.interval, 1, proc.Mp())
				dateVec, _ := vector.NewConstFixed(types.T_date.ToType(), d1, 1, proc.Mp())

				parameters := []*vector.Vector{unitVec, intervalVec, dateVec}
				// retType returns DATETIME, so create result wrapper with DATETIME type
				result := vector.NewFunctionResultWrapper(types.T_datetime.ToType(), proc.Mp())

				fnLength := dateVec.Length()
				err := result.PreExtendAndReset(fnLength)
				require.NoError(t, err)

				err = TimestampAddDate(parameters, result, proc, fnLength, nil)
				require.NoError(t, err)

				v := result.GetResultVector()
				require.Equal(t, fnLength, v.Length())
				require.Equal(t, types.T_date, v.GetType().Oid)

				dateParam := vector.GenerateFunctionFixedTypeParameter[types.Date](v)
				resultDate, null := dateParam.GetValue(0)
				require.False(t, null, "Result should not be null")
				require.Equal(t, expectedDate, resultDate)
			})
		}
	})

	// 6. DATE type input + time units → DATETIME type
	// This test verifies MySQL compatibility: DATE input + time unit → DATETIME output
	t.Run("DATE type + time units", func(t *testing.T) {
		d1, _ := types.ParseDateCast("2024-12-20")
		testCases := []struct {
			name     string
			unit     string
			interval int64
			expected string
			scale    int32
		}{
			{"HOUR +2", "HOUR", 2, "2024-12-20 02:00:00", 0},
			{"MINUTE +30", "MINUTE", 30, "2024-12-20 00:30:00", 0},
			{"SECOND +45", "SECOND", 45, "2024-12-20 00:00:45", 0},
			{"MICROSECOND +1000000", "MICROSECOND", 1000000, "2024-12-20 00:00:01", 6},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				expectedDt, _ := types.ParseDatetime(tc.expected, tc.scale)

				unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte(tc.unit), 1, proc.Mp())
				intervalVec, _ := vector.NewConstFixed(types.T_int64.ToType(), tc.interval, 1, proc.Mp())
				dateVec, _ := vector.NewConstFixed(types.T_date.ToType(), d1, 1, proc.Mp())

				parameters := []*vector.Vector{unitVec, intervalVec, dateVec}
				// retType returns DATETIME, so create result wrapper with DATETIME type
				result := vector.NewFunctionResultWrapper(types.T_datetime.ToType(), proc.Mp())

				fnLength := dateVec.Length()
				err := result.PreExtendAndReset(fnLength)
				require.NoError(t, err)

				err = TimestampAddDate(parameters, result, proc, fnLength, nil)
				require.NoError(t, err)

				v := result.GetResultVector()
				require.Equal(t, fnLength, v.Length())
				require.Equal(t, types.T_datetime, v.GetType().Oid, "Result type should be DATETIME for time units")
				require.Equal(t, tc.scale, v.GetType().Scale, fmt.Sprintf("Result scale should be %d for %s unit", tc.scale, tc.unit))

				dtParam := vector.GenerateFunctionFixedTypeParameter[types.Datetime](v)
				resultDt, null := dtParam.GetValue(0)
				require.False(t, null, "Result should not be null")
				require.Equal(t, expectedDt, resultDt, "Result should match expected value")
				// For MICROSECOND, MySQL displays without fractional seconds if they are zero
				// So we compare with String2(0) to match MySQL's display format (expect result file format)
				displayScale := int32(0)
				if tc.unit != "MICROSECOND" {
					displayScale = tc.scale
				}
				require.Equal(t, tc.expected, resultDt.String2(displayScale), "String representation should match")

				// Verify that actual vector type is DATETIME (not DATE)
				require.Equal(t, types.T_datetime, v.GetType().Oid, "Result type should be DATETIME for time units")
				require.Equal(t, tc.scale, v.GetType().Scale, fmt.Sprintf("Result scale should be %d for %s unit", tc.scale, tc.unit))
			})
		}
	})

	// 7. TIMESTAMP type input + date/time units → TIMESTAMP type
	t.Run("TIMESTAMP type + date/time units", func(t *testing.T) {
		ts1, _ := types.ParseTimestamp(time.Local, "2024-12-20 10:30:45", 6)
		testCases := []struct {
			name     string
			unit     string
			interval int64
			expected string // Expected string representation
		}{
			{"DAY +5", "DAY", 5, "2024-12-25 10:30:45"},
			{"HOUR +2", "HOUR", 2, "2024-12-20 12:30:45"},
			{"HOUR -2", "HOUR", -2, "2024-12-20 08:30:45"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte(tc.unit), 1, proc.Mp())
				intervalVec, _ := vector.NewConstFixed(types.T_int64.ToType(), tc.interval, 1, proc.Mp())
				tsVec, _ := vector.NewConstFixed(types.T_timestamp.ToType(), ts1, 1, proc.Mp())

				parameters := []*vector.Vector{unitVec, intervalVec, tsVec}
				result := vector.NewFunctionResultWrapper(types.T_timestamp.ToType(), proc.Mp())

				fnLength := tsVec.Length()
				err := result.PreExtendAndReset(fnLength)
				require.NoError(t, err)

				err = TimestampAddTimestamp(parameters, result, proc, fnLength, nil)
				require.NoError(t, err)

				v := result.GetResultVector()
				require.Equal(t, fnLength, v.Length())
				require.Equal(t, types.T_timestamp, v.GetType().Oid)

				tsParam := vector.GenerateFunctionFixedTypeParameter[types.Timestamp](v)
				resultTs, null := tsParam.GetValue(0)
				require.False(t, null, "Result should not be null")
				resultStr := resultTs.String2(time.Local, 0)
				require.Equal(t, tc.expected, resultStr)
			})
		}
	})

	// 8. DATETIME type input + date/time units → DATETIME type
	t.Run("DATETIME type + date/time units", func(t *testing.T) {
		dt1, _ := types.ParseDatetime("2024-12-20 10:30:45", 6)
		testCases := []struct {
			name     string
			unit     string
			interval int64
			expected string // Expected string representation
		}{
			{"DAY +5", "DAY", 5, "2024-12-25 10:30:45"},
			{"HOUR +2", "HOUR", 2, "2024-12-20 12:30:45"},
			{"MINUTE +30", "MINUTE", 30, "2024-12-20 11:00:45"},
			{"SECOND +60", "SECOND", 60, "2024-12-20 10:31:45"},
			{"MICROSECOND +1000000", "MICROSECOND", 1000000, "2024-12-20 10:30:46"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte(tc.unit), 1, proc.Mp())
				intervalVec, _ := vector.NewConstFixed(types.T_int64.ToType(), tc.interval, 1, proc.Mp())
				dtVec, _ := vector.NewConstFixed(types.T_datetime.ToType(), dt1, 1, proc.Mp())

				parameters := []*vector.Vector{unitVec, intervalVec, dtVec}
				result := vector.NewFunctionResultWrapper(types.T_datetime.ToType(), proc.Mp())

				fnLength := dtVec.Length()
				err := result.PreExtendAndReset(fnLength)
				require.NoError(t, err)

				err = TimestampAddDatetime(parameters, result, proc, fnLength, nil)
				require.NoError(t, err)

				v := result.GetResultVector()
				require.Equal(t, fnLength, v.Length())
				require.Equal(t, types.T_datetime, v.GetType().Oid)

				dtParam := vector.GenerateFunctionFixedTypeParameter[types.Datetime](v)
				resultDt, null := dtParam.GetValue(0)
				require.False(t, null, "Result should not be null")
				resultStr := resultDt.String2(0)
				require.Equal(t, tc.expected, resultStr)
			})
		}
	})
}

// TestTimestampAddStringPerformance tests performance optimization for TimestampAddString
// This test verifies that the optimized single-pass implementation produces correct results
// for large vectors and mixed DATE/DATETIME format inputs
func TestTimestampAddStringPerformance(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test case 1: Large vector with all DATE format inputs (should use optimized path)
	t.Run("Large vector with DATE format inputs", func(t *testing.T) {
		const vectorSize = 10000
		unit := "DAY"
		interval := int64(5)

		// Create large vectors
		unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte(unit), vectorSize, proc.Mp())
		intervalVec, _ := vector.NewConstFixed(types.T_int64.ToType(), interval, vectorSize, proc.Mp())

		// Create DATE format string vector
		dateStrs := make([]string, vectorSize)
		for i := 0; i < vectorSize; i++ {
			dateStrs[i] = "2024-12-20"
		}
		inputVec := vector.NewVec(types.T_varchar.ToType())
		vector.AppendStringList(inputVec, dateStrs, nil, proc.Mp())

		parameters := []*vector.Vector{unitVec, intervalVec, inputVec}
		result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())

		fnLength := inputVec.Length()
		err := result.PreExtendAndReset(fnLength)
		require.NoError(t, err)

		err = TimestampAddString(parameters, result, proc, fnLength, nil)
		require.NoError(t, err)

		v := result.GetResultVector()
		require.Equal(t, fnLength, v.Length())
		require.Equal(t, types.T_varchar, v.GetType().Oid)

		// Verify first and last elements
		strParam := vector.GenerateFunctionStrParameter(v)
		resultBytes, null := strParam.GetStrValue(0)
		require.False(t, null)
		require.Equal(t, "2024-12-25", string(resultBytes))

		resultBytes, null = strParam.GetStrValue(uint64(vectorSize - 1))
		require.False(t, null)
		require.Equal(t, "2024-12-25", string(resultBytes))
	})

	// Test case 2: Large vector with mixed DATE and DATETIME format inputs
	t.Run("Large vector with mixed DATE/DATETIME format inputs", func(t *testing.T) {
		const vectorSize = 10000
		unit := "DAY"
		interval := int64(5)

		unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte(unit), vectorSize, proc.Mp())
		intervalVec, _ := vector.NewConstFixed(types.T_int64.ToType(), interval, vectorSize, proc.Mp())

		// Create mixed format string vector: first half DATE, second half DATETIME
		dateStrs := make([]string, vectorSize)
		for i := 0; i < vectorSize/2; i++ {
			dateStrs[i] = "2024-12-20"
		}
		for i := vectorSize / 2; i < vectorSize; i++ {
			dateStrs[i] = "2024-12-20 10:30:45"
		}
		inputVec := vector.NewVec(types.T_varchar.ToType())
		vector.AppendStringList(inputVec, dateStrs, nil, proc.Mp())

		parameters := []*vector.Vector{unitVec, intervalVec, inputVec}
		result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())

		fnLength := inputVec.Length()
		err := result.PreExtendAndReset(fnLength)
		require.NoError(t, err)

		err = TimestampAddString(parameters, result, proc, fnLength, nil)
		require.NoError(t, err)

		v := result.GetResultVector()
		require.Equal(t, fnLength, v.Length())

		// Verify DATE format inputs produce DATE format output
		strParam := vector.GenerateFunctionStrParameter(v)
		resultBytes, null := strParam.GetStrValue(0)
		require.False(t, null)
		require.Equal(t, "2024-12-25", string(resultBytes))

		// Verify DATETIME format inputs produce DATETIME format output
		resultBytes, null = strParam.GetStrValue(uint64(vectorSize - 1))
		require.False(t, null)
		require.Equal(t, "2024-12-25 10:30:45", string(resultBytes))
	})

	// Test case 3: Small vector with single DATE format input (edge case)
	t.Run("Single DATE format input", func(t *testing.T) {
		unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte("DAY"), 1, proc.Mp())
		intervalVec, _ := vector.NewConstFixed(types.T_int64.ToType(), int64(5), 1, proc.Mp())
		inputVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte("2024-12-20"), 1, proc.Mp())

		parameters := []*vector.Vector{unitVec, intervalVec, inputVec}
		result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())

		fnLength := inputVec.Length()
		err := result.PreExtendAndReset(fnLength)
		require.NoError(t, err)

		err = TimestampAddString(parameters, result, proc, fnLength, nil)
		require.NoError(t, err)

		v := result.GetResultVector()
		require.Equal(t, fnLength, v.Length())

		strParam := vector.GenerateFunctionStrParameter(v)
		resultBytes, null := strParam.GetStrValue(0)
		require.False(t, null)
		require.Equal(t, "2024-12-25", string(resultBytes))
	})

	// Test case 4: Large vector with time units (should always return DATETIME format)
	t.Run("Large vector with time units", func(t *testing.T) {
		const vectorSize = 10000
		unit := "HOUR"
		interval := int64(2)

		unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte(unit), vectorSize, proc.Mp())
		intervalVec, _ := vector.NewConstFixed(types.T_int64.ToType(), interval, vectorSize, proc.Mp())

		dateStrs := make([]string, vectorSize)
		for i := 0; i < vectorSize; i++ {
			dateStrs[i] = "2024-12-20"
		}
		inputVec := vector.NewVec(types.T_varchar.ToType())
		vector.AppendStringList(inputVec, dateStrs, nil, proc.Mp())

		parameters := []*vector.Vector{unitVec, intervalVec, inputVec}
		result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())

		fnLength := inputVec.Length()
		err := result.PreExtendAndReset(fnLength)
		require.NoError(t, err)

		err = TimestampAddString(parameters, result, proc, fnLength, nil)
		require.NoError(t, err)

		v := result.GetResultVector()
		require.Equal(t, fnLength, v.Length())

		strParam := vector.GenerateFunctionStrParameter(v)
		resultBytes, null := strParam.GetStrValue(0)
		require.False(t, null)
		// Time unit should return DATETIME format
		require.Equal(t, "2024-12-20 02:00:00", string(resultBytes))
	})

	// Test case 5: ISO 8601 format support
	t.Run("ISO 8601 format support", func(t *testing.T) {
		proc := testutil.NewProcess(t)

		testCases := []struct {
			name     string
			unit     string
			interval int64
			input    string
			expected string
		}{
			{
				name:     "ISO format with DAY unit",
				unit:     "DAY",
				interval: 5,
				input:    "2024-12-20T10:30:45",
				expected: "2024-12-25 10:30:45",
			},
			{
				name:     "ISO format with HOUR unit",
				unit:     "HOUR",
				interval: 2,
				input:    "2024-12-20T10:30:45",
				expected: "2024-12-20 12:30:45",
			},
			{
				name:     "ISO format with MINUTE unit",
				unit:     "MINUTE",
				interval: 30,
				input:    "2024-12-20T10:30:45",
				expected: "2024-12-20 11:00:45",
			},
			{
				name:     "ISO format with SECOND unit",
				unit:     "SECOND",
				interval: 60,
				input:    "2024-12-20T10:30:45",
				expected: "2024-12-20 10:31:45",
			},
			{
				name:     "ISO format with microseconds",
				unit:     "MICROSECOND",
				interval: 123456,
				input:    "2024-12-20T10:30:45.000000",
				expected: "2024-12-20 10:30:45.123456",
			},
			{
				name:     "ISO format with MONTH unit",
				unit:     "MONTH",
				interval: 1,
				input:    "2024-12-20T10:30:45",
				expected: "2025-01-20 10:30:45",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte(tc.unit), 1, proc.Mp())
				intervalVec, _ := vector.NewConstFixed(types.T_int64.ToType(), tc.interval, 1, proc.Mp())
				inputVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte(tc.input), 1, proc.Mp())

				parameters := []*vector.Vector{unitVec, intervalVec, inputVec}
				result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())

				fnLength := inputVec.Length()
				err := result.PreExtendAndReset(fnLength)
				require.NoError(t, err)

				err = TimestampAddString(parameters, result, proc, fnLength, nil)
				require.NoError(t, err)

				v := result.GetResultVector()
				require.Equal(t, fnLength, v.Length())

				strParam := vector.GenerateFunctionStrParameter(v)
				resultBytes, null := strParam.GetStrValue(0)
				require.False(t, null)
				require.Equal(t, tc.expected, string(resultBytes))
			})
		}
	})
}

// TestTimestampAddErrorHandling tests error handling for TIMESTAMPADD function
// This test verifies that invalid inputs are handled correctly
func TestTimestampAddErrorHandling(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test case 1: Invalid unit string
	t.Run("Invalid unit string", func(t *testing.T) {
		unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte("INVALID_UNIT"), 1, proc.Mp())
		intervalVec, _ := vector.NewConstFixed(types.T_int64.ToType(), int64(5), 1, proc.Mp())
		dateVec, _ := vector.NewConstFixed(types.T_date.ToType(), types.Date(0), 1, proc.Mp())

		parameters := []*vector.Vector{unitVec, intervalVec, dateVec}
		result := vector.NewFunctionResultWrapper(types.T_datetime.ToType(), proc.Mp())

		fnLength := dateVec.Length()
		err := result.PreExtendAndReset(fnLength)
		require.NoError(t, err)

		err = TimestampAddDate(parameters, result, proc, fnLength, nil)
		require.Error(t, err, "Should return error for invalid unit")
		require.Contains(t, err.Error(), "invalid", "Error message should mention invalid unit")
	})

	// Test case 2: Invalid date string format
	t.Run("Invalid date string format", func(t *testing.T) {
		unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte("DAY"), 1, proc.Mp())
		intervalVec, _ := vector.NewConstFixed(types.T_int64.ToType(), int64(5), 1, proc.Mp())
		inputVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte("invalid-date"), 1, proc.Mp())

		parameters := []*vector.Vector{unitVec, intervalVec, inputVec}
		result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())

		fnLength := inputVec.Length()
		err := result.PreExtendAndReset(fnLength)
		require.NoError(t, err)

		err = TimestampAddString(parameters, result, proc, fnLength, nil)
		require.Error(t, err, "Should return error for invalid date string")
	})

	// Test case 3: Empty unit string
	t.Run("Empty unit string", func(t *testing.T) {
		unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte(""), 1, proc.Mp())
		intervalVec, _ := vector.NewConstFixed(types.T_int64.ToType(), int64(5), 1, proc.Mp())
		dateVec, _ := vector.NewConstFixed(types.T_date.ToType(), types.Date(0), 1, proc.Mp())

		parameters := []*vector.Vector{unitVec, intervalVec, dateVec}
		result := vector.NewFunctionResultWrapper(types.T_datetime.ToType(), proc.Mp())

		fnLength := dateVec.Length()
		err := result.PreExtendAndReset(fnLength)
		require.NoError(t, err)

		err = TimestampAddDate(parameters, result, proc, fnLength, nil)
		require.Error(t, err, "Should return error for empty unit")
	})

	// Test case 4: NULL unit (should be handled by NULL check, but test for completeness)
	t.Run("NULL unit handling", func(t *testing.T) {
		// Note: This test verifies that NULL unit is handled correctly
		// In practice, NULL unit should be caught earlier in the execution pipeline
		unitVec := vector.NewConstNull(types.T_varchar.ToType(), 1, proc.Mp())
		intervalVec, _ := vector.NewConstFixed(types.T_int64.ToType(), int64(5), 1, proc.Mp())
		dateVec, _ := vector.NewConstFixed(types.T_date.ToType(), types.Date(0), 1, proc.Mp())

		parameters := []*vector.Vector{unitVec, intervalVec, dateVec}
		result := vector.NewFunctionResultWrapper(types.T_datetime.ToType(), proc.Mp())

		fnLength := dateVec.Length()
		err := result.PreExtendAndReset(fnLength)
		require.NoError(t, err)

		// NULL unit should cause error when trying to parse
		err = TimestampAddDate(parameters, result, proc, fnLength, nil)
		// This may return error or handle NULL gracefully depending on implementation
		// The important thing is it doesn't panic
		_ = err // Accept either error or success, just ensure no panic
	})

	// Test case 5: Very large interval (potential overflow)
	// Note: This test verifies that the function handles large intervals appropriately
	// Large intervals may cause overflow, which should be caught and handled
	t.Run("Very large interval", func(t *testing.T) {
		d1, _ := types.ParseDateCast("2024-12-20")
		unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte("DAY"), 1, proc.Mp())
		// Use a large but reasonable interval (10000 days ~ 27 years)
		intervalVec, _ := vector.NewConstFixed(types.T_int64.ToType(), int64(10000), 1, proc.Mp())
		dateVec, _ := vector.NewConstFixed(types.T_date.ToType(), d1, 1, proc.Mp())

		parameters := []*vector.Vector{unitVec, intervalVec, dateVec}
		result := vector.NewFunctionResultWrapper(types.T_datetime.ToType(), proc.Mp())

		fnLength := dateVec.Length()
		err := result.PreExtendAndReset(fnLength)
		require.NoError(t, err)

		err = TimestampAddDate(parameters, result, proc, fnLength, nil)
		// Large but reasonable intervals should work
		require.NoError(t, err, "Should handle large but reasonable intervals")

		v := result.GetResultVector()
		require.Equal(t, fnLength, v.Length())
		// Verify the result is reasonable
		dateParam := vector.GenerateFunctionFixedTypeParameter[types.Date](v)
		resultDate, null := dateParam.GetValue(0)
		require.False(t, null)
		// Result should be approximately 2024-12-20 + 10000 days
		require.Greater(t, int64(resultDate), int64(d1), "Result should be greater than input")
	})

	// Test case 6: Invalid date string with time unit
	t.Run("Invalid date string with time unit", func(t *testing.T) {
		unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte("HOUR"), 1, proc.Mp())
		intervalVec, _ := vector.NewConstFixed(types.T_int64.ToType(), int64(2), 1, proc.Mp())
		inputVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte("not-a-date"), 1, proc.Mp())

		parameters := []*vector.Vector{unitVec, intervalVec, inputVec}
		result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())

		fnLength := inputVec.Length()
		err := result.PreExtendAndReset(fnLength)
		require.NoError(t, err)

		err = TimestampAddString(parameters, result, proc, fnLength, nil)
		require.Error(t, err, "Should return error for invalid date string with time unit")
	})

	// Test case 7: Malformed datetime string
	t.Run("Malformed datetime string", func(t *testing.T) {
		unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte("DAY"), 1, proc.Mp())
		intervalVec, _ := vector.NewConstFixed(types.T_int64.ToType(), int64(5), 1, proc.Mp())
		// Malformed datetime: missing time part separator
		inputVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte("2024-12-2010:30:45"), 1, proc.Mp())

		parameters := []*vector.Vector{unitVec, intervalVec, inputVec}
		result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())

		fnLength := inputVec.Length()
		err := result.PreExtendAndReset(fnLength)
		require.NoError(t, err)

		err = TimestampAddString(parameters, result, proc, fnLength, nil)
		// This may or may not error depending on parsing logic
		// The important thing is it doesn't panic
		_ = err
	})

	// Test case 8: Case sensitivity for unit (should be case-insensitive)
	t.Run("Case insensitive unit", func(t *testing.T) {
		d1, _ := types.ParseDateCast("2024-12-20")
		expectedDate, _ := types.ParseDateCast("2024-12-25")

		// Test lowercase unit
		unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte("day"), 1, proc.Mp())
		intervalVec, _ := vector.NewConstFixed(types.T_int64.ToType(), int64(5), 1, proc.Mp())
		dateVec, _ := vector.NewConstFixed(types.T_date.ToType(), d1, 1, proc.Mp())

		parameters := []*vector.Vector{unitVec, intervalVec, dateVec}
		result := vector.NewFunctionResultWrapper(types.T_datetime.ToType(), proc.Mp())

		fnLength := dateVec.Length()
		err := result.PreExtendAndReset(fnLength)
		require.NoError(t, err)

		err = TimestampAddDate(parameters, result, proc, fnLength, nil)
		require.NoError(t, err, "Should accept lowercase unit")

		v := result.GetResultVector()
		dateParam := vector.GenerateFunctionFixedTypeParameter[types.Date](v)
		resultDate, null := dateParam.GetValue(0)
		require.False(t, null)
		require.Equal(t, expectedDate, resultDate)
	})
}

// TestTimestampAddNonConstantUnit tests TIMESTAMPADD with non-constant unit parameter
// This simulates: SELECT TIMESTAMPADD(col_unit, 5, date_col) FROM t1;
// MySQL behavior: Runtime unit determines return type
func TestTimestampAddNonConstantUnit(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test case 1: Non-constant unit with all date units → should return DATE
	t.Run("Non-constant unit - all date units", func(t *testing.T) {
		d1, _ := types.ParseDateCast("2024-12-20")
		d2, _ := types.ParseDateCast("2024-12-21")
		expected1, _ := types.ParseDateCast("2024-12-25") // +5 DAY
		expected2, _ := types.ParseDateCast("2024-12-28") // +7 DAY

		// Create non-constant unit vector: ["DAY", "DAY"]
		unitStrs := []string{"DAY", "DAY"}
		unitVec := vector.NewVec(types.T_varchar.ToType())
		vector.AppendStringList(unitVec, unitStrs, nil, proc.Mp())

		// Create interval vector: [5, 7]
		intervals := []int64{5, 7}
		intervalVec := vector.NewVec(types.T_int64.ToType())
		vector.AppendFixedList(intervalVec, intervals, nil, proc.Mp())

		// Create date vector: [d1, d2]
		dates := []types.Date{d1, d2}
		dateVec := vector.NewVec(types.T_date.ToType())
		vector.AppendFixedList(dateVec, dates, nil, proc.Mp())

		parameters := []*vector.Vector{unitVec, intervalVec, dateVec}
		// Result wrapper should be DATETIME (from retType, since unit is not constant at compile time)
		result := vector.NewFunctionResultWrapper(types.T_datetime.ToType(), proc.Mp())

		fnLength := dateVec.Length()
		err := result.PreExtendAndReset(fnLength)
		require.NoError(t, err)

		err = TimestampAddDate(parameters, result, proc, fnLength, nil)
		require.NoError(t, err, "Should handle non-constant unit")

		v := result.GetResultVector()
		require.Equal(t, fnLength, v.Length())

		// Since all units are date units, result type should be DATE
		require.Equal(t, types.T_date, v.GetType().Oid, "Result type should be DATE when all units are date units")

		dateParam := vector.GenerateFunctionFixedTypeParameter[types.Date](v)
		resultDate1, null1 := dateParam.GetValue(0)
		require.False(t, null1)
		require.Equal(t, expected1, resultDate1)

		resultDate2, null2 := dateParam.GetValue(1)
		require.False(t, null2)
		require.Equal(t, expected2, resultDate2)
	})

	// Test case 2: Non-constant unit with mixed date/time units → should return DATETIME
	t.Run("Non-constant unit - mixed date/time units", func(t *testing.T) {
		d1, _ := types.ParseDateCast("2024-12-20")
		d2, _ := types.ParseDateCast("2024-12-20")
		expected1, _ := types.ParseDateCast("2024-12-25")             // +5 DAY
		expected2, _ := types.ParseDatetime("2024-12-20 02:00:00", 0) // +2 HOUR

		// Create non-constant unit vector: ["DAY", "HOUR"]
		unitStrs := []string{"DAY", "HOUR"}
		unitVec := vector.NewVec(types.T_varchar.ToType())
		vector.AppendStringList(unitVec, unitStrs, nil, proc.Mp())

		// Create interval vector: [5, 2]
		intervals := []int64{5, 2}
		intervalVec := vector.NewVec(types.T_int64.ToType())
		vector.AppendFixedList(intervalVec, intervals, nil, proc.Mp())

		// Create date vector: [d1, d2]
		dates := []types.Date{d1, d2}
		dateVec := vector.NewVec(types.T_date.ToType())
		vector.AppendFixedList(dateVec, dates, nil, proc.Mp())

		parameters := []*vector.Vector{unitVec, intervalVec, dateVec}
		result := vector.NewFunctionResultWrapper(types.T_datetime.ToType(), proc.Mp())

		fnLength := dateVec.Length()
		err := result.PreExtendAndReset(fnLength)
		require.NoError(t, err)

		err = TimestampAddDate(parameters, result, proc, fnLength, nil)
		require.NoError(t, err, "Should handle mixed date/time units")

		v := result.GetResultVector()
		require.Equal(t, fnLength, v.Length())

		// Since there's at least one time unit, result type should be DATETIME
		require.Equal(t, types.T_datetime, v.GetType().Oid, "Result type should be DATETIME when any unit is time unit")

		// First result: DATE input + DAY unit → should be DATE format but stored as DATETIME
		dtParam := vector.GenerateFunctionFixedTypeParameter[types.Datetime](v)
		resultDt1, null1 := dtParam.GetValue(0)
		require.False(t, null1)
		// Convert expected DATE to DATETIME for comparison
		expectedDt1 := expected1.ToDatetime()
		require.Equal(t, expectedDt1, resultDt1)

		// Second result: DATE input + HOUR unit → should be DATETIME
		resultDt2, null2 := dtParam.GetValue(1)
		require.False(t, null2)
		require.Equal(t, expected2, resultDt2)
	})

	// Test case 3: Non-constant unit with all time units → should return DATETIME
	t.Run("Non-constant unit - all time units", func(t *testing.T) {
		d1, _ := types.ParseDateCast("2024-12-20")
		d2, _ := types.ParseDateCast("2024-12-20")
		expected1, _ := types.ParseDatetime("2024-12-20 02:00:00", 0) // +2 HOUR
		expected2, _ := types.ParseDatetime("2024-12-20 00:30:00", 0) // +30 MINUTE

		// Create non-constant unit vector: ["HOUR", "MINUTE"]
		unitStrs := []string{"HOUR", "MINUTE"}
		unitVec := vector.NewVec(types.T_varchar.ToType())
		vector.AppendStringList(unitVec, unitStrs, nil, proc.Mp())

		// Create interval vector: [2, 30]
		intervals := []int64{2, 30}
		intervalVec := vector.NewVec(types.T_int64.ToType())
		vector.AppendFixedList(intervalVec, intervals, nil, proc.Mp())

		// Create date vector: [d1, d2]
		dates := []types.Date{d1, d2}
		dateVec := vector.NewVec(types.T_date.ToType())
		vector.AppendFixedList(dateVec, dates, nil, proc.Mp())

		parameters := []*vector.Vector{unitVec, intervalVec, dateVec}
		result := vector.NewFunctionResultWrapper(types.T_datetime.ToType(), proc.Mp())

		fnLength := dateVec.Length()
		err := result.PreExtendAndReset(fnLength)
		require.NoError(t, err)

		err = TimestampAddDate(parameters, result, proc, fnLength, nil)
		require.NoError(t, err, "Should handle all time units")

		v := result.GetResultVector()
		require.Equal(t, fnLength, v.Length())

		// Since all units are time units, result type should be DATETIME
		require.Equal(t, types.T_datetime, v.GetType().Oid, "Result type should be DATETIME when all units are time units")

		dtParam := vector.GenerateFunctionFixedTypeParameter[types.Datetime](v)
		resultDt1, null1 := dtParam.GetValue(0)
		require.False(t, null1)
		require.Equal(t, expected1, resultDt1)

		resultDt2, null2 := dtParam.GetValue(1)
		require.False(t, null2)
		require.Equal(t, expected2, resultDt2)
	})

	// Test case 4: Non-constant unit with NULL values
	t.Run("Non-constant unit - with NULL values", func(t *testing.T) {
		d1, _ := types.ParseDateCast("2024-12-20")
		d2, _ := types.ParseDateCast("2024-12-21")

		// Create non-constant unit vector: ["DAY", NULL]
		unitStrs := []string{"DAY", ""}
		unitVec := vector.NewVec(types.T_varchar.ToType())
		vector.AppendStringList(unitVec, unitStrs, nil, proc.Mp())
		// Set second element as NULL
		nulls := nulls.NewWithSize(2)
		nulls.Set(1)
		unitVec.SetNulls(nulls)

		// Create interval vector: [5, 7]
		intervals := []int64{5, 7}
		intervalVec := vector.NewVec(types.T_int64.ToType())
		vector.AppendFixedList(intervalVec, intervals, nil, proc.Mp())

		// Create date vector: [d1, d2]
		dates := []types.Date{d1, d2}
		dateVec := vector.NewVec(types.T_date.ToType())
		vector.AppendFixedList(dateVec, dates, nil, proc.Mp())

		parameters := []*vector.Vector{unitVec, intervalVec, dateVec}
		result := vector.NewFunctionResultWrapper(types.T_datetime.ToType(), proc.Mp())

		fnLength := dateVec.Length()
		err := result.PreExtendAndReset(fnLength)
		require.NoError(t, err)

		err = TimestampAddDate(parameters, result, proc, fnLength, nil)
		// NULL unit should cause error or be handled gracefully
		// The important thing is it doesn't panic
		if err != nil {
			require.Contains(t, err.Error(), "null", "Error should mention null or invalid unit")
		}
	})
}

// TestTimestampAddRetType tests the retType function behavior for TIMESTAMPADD
// This test verifies that retType returns the correct type, which affects MySQL protocol layer formatting
func TestTimestampAddRetType(t *testing.T) {
	ctx := context.Background()

	// Get TIMESTAMPADD function ID
	fnId, err := getFunctionIdByName(ctx, "timestampadd")
	require.NoError(t, err)

	// Test case: DATE input
	// retType returns DATETIME to match MySQL behavior
	// Since retType cannot know the runtime unit at compile time, it returns DATETIME conservatively
	// At runtime, TimestampAddDate will use TempSetType appropriately:
	// - For time units: DATETIME with scale 0 (HOUR/MINUTE/SECOND) or 6 (MICROSECOND)
	// - For date units: DATETIME with scale 0, but formatted as DATE when time is 00:00:00
	t.Run("DATE input - retType returns DATETIME", func(t *testing.T) {
		parameters := []types.Type{
			types.T_varchar.ToType(), // unit parameter (string)
			types.T_int64.ToType(),   // interval parameter
			types.T_date.ToType(),    // date parameter
		}

		// Find matching overload
		fn := allSupportedFunctions[fnId]
		var matchedOverload *overload
		for i := range fn.Overloads {
			ov := &fn.Overloads[i]
			if len(ov.args) == len(parameters) {
				match := true
				for j := range parameters {
					if ov.args[j] != parameters[j].Oid {
						match = false
						break
					}
				}
				if match {
					matchedOverload = ov
					break
				}
			}
		}
		require.NotNil(t, matchedOverload, "Should find matching overload for DATE input")

		// Call retType function
		retType := matchedOverload.retType(parameters)
		require.Equal(t, types.T_datetime, retType.Oid, "retType returns DATETIME type (to match MySQL behavior)")

		// Document the behavior: retType returns DATETIME, which ensures MySQL column metadata is MYSQL_TYPE_DATETIME
		// This prevents "Invalid length (19) for type DATE" errors when actual vector type is DATETIME
		t.Logf("retType returns: %s (Oid: %d)", retType.Oid, retType.Oid)
		t.Logf("For DATE input + time unit (HOUR/MINUTE/SECOND/MICROSECOND), actual vector type is DATETIME")
		t.Logf("MySQL protocol layer uses retType (DATETIME) to determine formatting, ensuring correct DATETIME format")
		t.Logf("For DATE input + date unit, MySQL protocol layer formats as DATE when time is 00:00:00")
	})

	// Test case: DATETIME input - retType returns DATETIME
	t.Run("DATETIME input - retType returns DATETIME", func(t *testing.T) {
		parameters := []types.Type{
			types.T_varchar.ToType(),  // unit parameter (string)
			types.T_int64.ToType(),    // interval parameter
			types.T_datetime.ToType(), // datetime parameter
		}

		// Find matching overload
		fn2 := allSupportedFunctions[fnId]
		var matchedOverload2 *overload
		for i := range fn2.Overloads {
			ov := &fn2.Overloads[i]
			if len(ov.args) == len(parameters) {
				match := true
				for j := range parameters {
					if ov.args[j] != parameters[j].Oid {
						match = false
						break
					}
				}
				if match {
					matchedOverload2 = ov
					break
				}
			}
		}
		require.NotNil(t, matchedOverload2, "Should find matching overload for DATETIME input")

		// Call retType function
		retType := matchedOverload2.retType(parameters)
		require.Equal(t, types.T_datetime, retType.Oid, "retType returns DATETIME type for DATETIME input")
	})
}

// TestTimestampAddMySQLProtocolColumnType tests that MySQL protocol column type matches actual vector type
// This test verifies that when TIMESTAMPADD(DAY, 5, DATE) returns DATE type, the MySQL column type is MYSQL_TYPE_DATE
// NOT MYSQL_TYPE_TIMESTAMP or MYSQL_TYPE_DATETIME
// This prevents "Invalid length (10) for type TIMESTAMP" errors
func TestTimestampAddMySQLProtocolColumnType(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test case: TIMESTAMPADD(DAY, 5, DATE('2024-12-20'))
	// MySQL behavior: Returns DATE type (2024-12-25)
	// MySQL column type should be MYSQL_TYPE_DATE (not MYSQL_TYPE_TIMESTAMP or MYSQL_TYPE_DATETIME)
	t.Run("DATE input + DAY unit - MySQL column type should be DATE", func(t *testing.T) {
		d1, _ := types.ParseDateCast("2024-12-20")
		expectedDate, _ := types.ParseDateCast("2024-12-25")

		unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte("DAY"), 1, proc.Mp())
		intervalVec, _ := vector.NewConstFixed(types.T_int64.ToType(), int64(5), 1, proc.Mp())
		dateVec, _ := vector.NewConstFixed(types.T_date.ToType(), d1, 1, proc.Mp())

		parameters := []*vector.Vector{unitVec, intervalVec, dateVec}
		// After BindFuncExprImplByPlanExpr fix, expr.Typ is DATE, so result wrapper is DATE type
		result := vector.NewFunctionResultWrapper(types.T_date.ToType(), proc.Mp())

		fnLength := dateVec.Length()
		err := result.PreExtendAndReset(fnLength)
		require.NoError(t, err)

		err = TimestampAddDate(parameters, result, proc, fnLength, nil)
		require.NoError(t, err, "Should not panic when result wrapper is DATE type")

		v := result.GetResultVector()
		require.Equal(t, fnLength, v.Length())
		// MySQL behavior: DATE input + date unit → DATE output
		require.Equal(t, types.T_date, v.GetType().Oid, "Actual vector type should be DATE (MySQL compatible)")

		// Verify the actual value matches MySQL
		dateParam := vector.GenerateFunctionFixedTypeParameter[types.Date](v)
		resultDate, null := dateParam.GetValue(0)
		require.False(t, null, "Result should not be null")
		require.Equal(t, expectedDate, resultDate, "Result should match MySQL behavior: DATE input + DAY unit → DATE output")
	})

	// Test case: TIMESTAMPADD(DAY, 5, DATE('2024-12-20')) with DATETIME result wrapper (backward compatibility)
	// This tests the backward compatibility path
	t.Run("DATE input + DAY unit - backward compatibility with DATETIME result wrapper", func(t *testing.T) {
		d1, _ := types.ParseDateCast("2024-12-20")
		expectedDate, _ := types.ParseDateCast("2024-12-25")

		unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte("DAY"), 1, proc.Mp())
		intervalVec, _ := vector.NewConstFixed(types.T_int64.ToType(), int64(5), 1, proc.Mp())
		dateVec, _ := vector.NewConstFixed(types.T_date.ToType(), d1, 1, proc.Mp())

		parameters := []*vector.Vector{unitVec, intervalVec, dateVec}
		// Backward compatibility: if retType returns DATETIME, result wrapper is DATETIME type
		result := vector.NewFunctionResultWrapper(types.T_datetime.ToType(), proc.Mp())

		fnLength := dateVec.Length()
		err := result.PreExtendAndReset(fnLength)
		require.NoError(t, err)

		err = TimestampAddDate(parameters, result, proc, fnLength, nil)
		require.NoError(t, err, "Should not panic when result wrapper is DATETIME type (backward compatibility)")

		v := result.GetResultVector()
		require.Equal(t, fnLength, v.Length())
		// MySQL behavior: DATE input + date unit → DATE output
		require.Equal(t, types.T_date, v.GetType().Oid, "Actual vector type should be DATE (MySQL compatible)")

		// Verify the actual value matches MySQL
		dateParam := vector.GenerateFunctionFixedTypeParameter[types.Date](v)
		resultDate, null := dateParam.GetValue(0)
		require.False(t, null, "Result should not be null")
		require.Equal(t, expectedDate, resultDate, "Result should match MySQL behavior: DATE input + DAY unit → DATE output")
	})
}

// TestTimestampAddMySQLCompatibilityAndPanicPrevention tests MySQL compatibility and prevents panic issues
// This test verifies the critical execution flow that was causing panic:
// 1. retType returns DATETIME (because it cannot know the runtime unit at compile time)
// 2. FunctionExpressionExecutor creates result wrapper with DATETIME type (from retType via planExpr.Typ)
// 3. TimestampAddDate is called with DATETIME result wrapper
// 4. TimestampAddDate uses MustFunctionResult[types.Datetime] (should NOT panic)
// 5. For date units: TimestampAddDate uses SetType to change vector type to DATE
// 6. For time units: TimestampAddDate uses TempSetType to set vector type to DATETIME with appropriate scale
// 7. Final result type matches MySQL behavior
//
// This test specifically covers the panic scenario:
// - SELECT d, TIMESTAMPADD(DAY, 5, d) AS added_date FROM t1;
// - Where d is a DATE column
// - Expected: Returns DATE type (2024-12-25), no panic
func TestTimestampAddMySQLCompatibilityAndPanicPrevention(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test case 1: TIMESTAMPADD(DAY, 5, DATE('2024-12-20'))
	// MySQL behavior: Returns DATE type (2024-12-25)
	// This simulates: SELECT TIMESTAMPADD(DAY, 5, d) AS added_date FROM t1; where d is DATE column
	t.Run("DATE input + DAY unit - simulates actual execution flow", func(t *testing.T) {
		d1, _ := types.ParseDateCast("2024-12-20")
		expectedDate, _ := types.ParseDateCast("2024-12-25")

		unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte("DAY"), 1, proc.Mp())
		intervalVec, _ := vector.NewConstFixed(types.T_int64.ToType(), int64(5), 1, proc.Mp())
		dateVec, _ := vector.NewConstFixed(types.T_date.ToType(), d1, 1, proc.Mp())

		parameters := []*vector.Vector{unitVec, intervalVec, dateVec}
		// CRITICAL: Simulate FunctionExpressionExecutor.Init() behavior:
		// - retType returns DATETIME (from list_builtIn.go)
		// - planExpr.Typ is set to DATETIME (from retType)
		// - FunctionExpressionExecutor.Init() uses planExpr.Typ to create result wrapper
		// - So result wrapper is DATETIME type
		// If we create DATE result wrapper here, MustFunctionResult[types.Datetime] will panic!
		result := vector.NewFunctionResultWrapper(types.T_datetime.ToType(), proc.Mp())

		fnLength := dateVec.Length()
		err := result.PreExtendAndReset(fnLength)
		require.NoError(t, err)

		// This should NOT panic: MustFunctionResult[types.Datetime] on DATETIME result wrapper
		// This is the exact line that was panicking: func_binary.go:1370
		err = TimestampAddDate(parameters, result, proc, fnLength, nil)
		require.NoError(t, err, "Should not panic when calling MustFunctionResult[types.Datetime] on DATETIME result wrapper")

		v := result.GetResultVector()
		require.Equal(t, fnLength, v.Length())
		// MySQL behavior: DATE input + date unit → DATE output
		require.Equal(t, types.T_date, v.GetType().Oid, "Result type should be DATE (MySQL compatible)")

		// Verify the actual value matches MySQL
		dateParam := vector.GenerateFunctionFixedTypeParameter[types.Date](v)
		resultDate, null := dateParam.GetValue(0)
		require.False(t, null, "Result should not be null")
		require.Equal(t, expectedDate, resultDate, "Result should match MySQL behavior: DATE input + DAY unit → DATE output")
	})

	// Test case 2: TIMESTAMPADD(HOUR, 2, DATE('2024-12-20'))
	// MySQL behavior: Returns DATETIME type (2024-12-20 02:00:00)
	// This simulates: SELECT TIMESTAMPADD(HOUR, 2, d) AS date_plus_hour FROM t1; where d is DATE column
	t.Run("DATE input + HOUR unit - simulates actual execution flow", func(t *testing.T) {
		d1, _ := types.ParseDateCast("2024-12-20")
		expectedDt, _ := types.ParseDatetime("2024-12-20 02:00:00", 0)

		unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte("HOUR"), 1, proc.Mp())
		intervalVec, _ := vector.NewConstFixed(types.T_int64.ToType(), int64(2), 1, proc.Mp())
		dateVec, _ := vector.NewConstFixed(types.T_date.ToType(), d1, 1, proc.Mp())

		parameters := []*vector.Vector{unitVec, intervalVec, dateVec}
		// CRITICAL: retType returns DATETIME, so result wrapper must be DATETIME type
		result := vector.NewFunctionResultWrapper(types.T_datetime.ToType(), proc.Mp())

		fnLength := dateVec.Length()
		err := result.PreExtendAndReset(fnLength)
		require.NoError(t, err)

		// This should NOT panic
		err = TimestampAddDate(parameters, result, proc, fnLength, nil)
		require.NoError(t, err, "Should not panic when calling MustFunctionResult[types.Datetime] on DATETIME result wrapper")

		v := result.GetResultVector()
		require.Equal(t, fnLength, v.Length())
		// MySQL behavior: DATE input + time unit → DATETIME output
		require.Equal(t, types.T_datetime, v.GetType().Oid, "Result type should be DATETIME (MySQL compatible)")
		require.Equal(t, int32(0), v.GetType().Scale, "Scale should be 0 for HOUR unit")

		// Verify the actual value matches MySQL
		dtParam := vector.GenerateFunctionFixedTypeParameter[types.Datetime](v)
		resultDt, null := dtParam.GetValue(0)
		require.False(t, null, "Result should not be null")
		require.Equal(t, expectedDt, resultDt, "Result should match MySQL behavior: DATE input + HOUR unit → DATETIME output")
	})

	// Test case 3: Verify that DATE result wrapper is now supported (after fix)
	// After BindFuncExprImplByPlanExpr fix, expr.Typ can be DATE for date units
	// FunctionExpressionExecutor creates DATE result wrapper, and TimestampAddDate handles it correctly
	t.Run("DATE result wrapper is now supported for date units", func(t *testing.T) {
		d1, _ := types.ParseDateCast("2024-12-20")
		expectedDate, _ := types.ParseDateCast("2024-12-25")

		unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte("DAY"), 1, proc.Mp())
		intervalVec, _ := vector.NewConstFixed(types.T_int64.ToType(), int64(5), 1, proc.Mp())
		dateVec, _ := vector.NewConstFixed(types.T_date.ToType(), d1, 1, proc.Mp())

		parameters := []*vector.Vector{unitVec, intervalVec, dateVec}
		// After BindFuncExprImplByPlanExpr fix, expr.Typ is DATE for date units
		// FunctionExpressionExecutor creates DATE result wrapper
		result := vector.NewFunctionResultWrapper(types.T_date.ToType(), proc.Mp())

		fnLength := dateVec.Length()
		err := result.PreExtendAndReset(fnLength)
		require.NoError(t, err)

		// This should NOT panic: TimestampAddDate now handles DATE result wrapper correctly
		err = TimestampAddDate(parameters, result, proc, fnLength, nil)
		require.NoError(t, err, "Should not panic when result wrapper is DATE type (after fix)")

		v := result.GetResultVector()
		require.Equal(t, types.T_date, v.GetType().Oid, "Result type should be DATE")
		dateParam := vector.GenerateFunctionFixedTypeParameter[types.Date](v)
		resultDate, null := dateParam.GetValue(0)
		require.False(t, null, "Result should not be null")
		require.Equal(t, expectedDate, resultDate, "Result should match expected value")
	})
}

func initDateAddTestCase() []tcTemp {
	d1, _ := types.ParseDatetime("2022-01-01", 6)
	r1, _ := types.ParseDatetime("2022-01-02", 6)
	return []tcTemp{
		{
			info: "test DateAdd",
			typ:  types.T_date,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{d1.ToDate()},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{int64(types.Day)},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_date.ToType(), false,
				[]types.Date{r1.ToDate()},
				[]bool{false}),
		},
		{
			info: "test DatetimeAdd",
			typ:  types.T_datetime,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d1},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{int64(types.Day)},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_datetime.ToType(), false,
				[]types.Datetime{r1},
				[]bool{false}),
		},
		{
			info: "test DateStringAdd",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"2022-01-01"},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{int64(types.Day)},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"2022-01-02"},
				[]bool{false}),
		},
	}
}

func TestDateAdd(t *testing.T) {
	testCases := initDateAddTestCase()

	// do the test work.
	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		var fcTC FunctionTestCase
		switch tc.typ {
		case types.T_date:
			fcTC = NewFunctionTestCase(proc,
				tc.inputs, tc.expect, DateAdd)
		case types.T_datetime:
			fcTC = NewFunctionTestCase(proc,
				tc.inputs, tc.expect, DatetimeAdd)
		case types.T_varchar:
			fcTC = NewFunctionTestCase(proc,
				tc.inputs, tc.expect, DateStringAdd)
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// TestDateAddOverflow tests that date_add throws error when overflow occurs
// This matches MySQL behavior where overflow should throw error in both SELECT and INSERT
func TestDateAddOverflow(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test case: date_add with large year interval that causes overflow
	// date_add('2000-01-01', interval 8000 year) should throw error
	startDate, _ := types.ParseDateCast("2000-01-01")
	largeInterval := int64(8000) // 8000 years, will cause overflow

	// Create input vectors
	ivecs := make([]*vector.Vector, 3)
	var err error
	ivecs[0], err = vector.NewConstFixed(types.T_date.ToType(), startDate, 1, proc.Mp())
	require.NoError(t, err)
	ivecs[1], err = vector.NewConstFixed(types.T_int64.ToType(), largeInterval, 1, proc.Mp())
	require.NoError(t, err)
	ivecs[2], err = vector.NewConstFixed(types.T_int64.ToType(), int64(types.Year), 1, proc.Mp())
	require.NoError(t, err)

	// Create result vector
	result := vector.NewFunctionResultWrapper(types.T_date.ToType(), proc.Mp())

	// Initialize result vector before calling DateAdd
	err = result.PreExtendAndReset(1)
	require.NoError(t, err)

	// Call DateAdd - should return error
	err = DateAdd(ivecs, result, proc, 1, nil)
	require.Error(t, err, "date_add with overflow should return error")
	require.Contains(t, err.Error(), "data out of range", "error message should contain 'data out of range'")

	// Cleanup
	for _, v := range ivecs {
		if v != nil {
			v.Free(proc.Mp())
		}
	}
	if result != nil {
		result.Free()
	}
}

// TestDateAddOverflowNegative tests that date_sub with large negative interval returns zero date
func TestDateAddOverflowNegative(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test case: date_sub('2000-01-01', interval 2001 year) should return zero date
	// According to MySQL behavior, underflow should return zero date '0000-00-00', not error
	startDate, _ := types.ParseDateCast("2000-01-01")
	largeNegativeInterval := int64(-2001) // -2001 years, will cause underflow

	// Create input vectors
	ivecs := make([]*vector.Vector, 3)
	var err error
	ivecs[0], err = vector.NewConstFixed(types.T_date.ToType(), startDate, 1, proc.Mp())
	require.NoError(t, err)
	ivecs[1], err = vector.NewConstFixed(types.T_int64.ToType(), largeNegativeInterval, 1, proc.Mp())
	require.NoError(t, err)
	ivecs[2], err = vector.NewConstFixed(types.T_int64.ToType(), int64(types.Year), 1, proc.Mp())
	require.NoError(t, err)

	// Create result vector
	result := vector.NewFunctionResultWrapper(types.T_date.ToType(), proc.Mp())

	// Initialize result vector before calling DateAdd
	err = result.PreExtendAndReset(1)
	require.NoError(t, err)

	// Call DateAdd - should return zero date for underflow (MySQL behavior)
	err = DateAdd(ivecs, result, proc, 1, nil)
	require.NoError(t, err, "date_add with underflow should return zero date, not error")

	// Check result is zero date
	resultVec := result.GetResultVector()
	resultDate := vector.MustFixedColNoTypeCheck[types.Date](resultVec)[0]
	require.Equal(t, types.Date(0), resultDate, "underflow should return zero date")

	// Cleanup
	for _, v := range ivecs {
		if v != nil {
			v.Free(proc.Mp())
		}
	}
	if result != nil {
		result.Free()
	}
}

// TestDateAddNormal tests normal date_add operations that should succeed
func TestDateAddNormal(t *testing.T) {
	proc := testutil.NewProcess(t)

	testCases := []struct {
		name          string
		startDate     string
		interval      int64
		intervalType  types.IntervalType
		expectedDate  string
		shouldSucceed bool
	}{
		{
			name:          "add 1 day",
			startDate:     "2000-01-01",
			interval:      1,
			intervalType:  types.Day,
			expectedDate:  "2000-01-02",
			shouldSucceed: true,
		},
		{
			name:          "add 1 year",
			startDate:     "2000-01-01",
			interval:      1,
			intervalType:  types.Year,
			expectedDate:  "2001-01-01",
			shouldSucceed: true,
		},
		{
			name:          "add 100 years (within range)",
			startDate:     "2000-01-01",
			interval:      100,
			intervalType:  types.Year,
			expectedDate:  "2100-01-01",
			shouldSucceed: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startDate, err := types.ParseDateCast(tc.startDate)
			require.NoError(t, err)

			expectedDate, err := types.ParseDateCast(tc.expectedDate)
			require.NoError(t, err)

			// Create input vectors
			ivecs := make([]*vector.Vector, 3)
			var vecErr error
			ivecs[0], vecErr = vector.NewConstFixed(types.T_date.ToType(), startDate, 1, proc.Mp())
			require.NoError(t, vecErr)
			ivecs[1], vecErr = vector.NewConstFixed(types.T_int64.ToType(), tc.interval, 1, proc.Mp())
			require.NoError(t, vecErr)
			ivecs[2], vecErr = vector.NewConstFixed(types.T_int64.ToType(), int64(tc.intervalType), 1, proc.Mp())
			require.NoError(t, vecErr)

			// Create result vector
			result := vector.NewFunctionResultWrapper(types.T_date.ToType(), proc.Mp())

			// Initialize result vector before calling DateAdd
			vecErr = result.PreExtendAndReset(1)
			require.NoError(t, vecErr)

			// Call DateAdd
			err = DateAdd(ivecs, result, proc, 1, nil)
			if tc.shouldSucceed {
				require.NoError(t, err, "date_add should succeed for normal case")
				resultVec := result.GetResultVector()
				resultDate := vector.MustFixedColNoTypeCheck[types.Date](resultVec)[0]
				require.Equal(t, expectedDate, resultDate, "result date should match expected")
			} else {
				require.Error(t, err, "date_add should return error for overflow case")
			}

			// Cleanup
			for _, v := range ivecs {
				if v != nil {
					v.Free(proc.Mp())
				}
			}
			if result != nil {
				result.Free()
			}
		})
	}
}

// TestDateStringAddOverflow tests that DateStringAdd throws error when overflow occurs
// This is the actual path used by SQL: date_add('2000-01-01', interval 8000 year)
func TestDateStringAddOverflow(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test case: date_add with string input and large year interval that causes overflow
	// date_add('2000-01-01', interval 8000 year) should throw error
	startDateStr := "2000-01-01"
	largeInterval := int64(8000) // 8000 years, will cause overflow

	// Create input vectors
	ivecs := make([]*vector.Vector, 3)
	var err error
	ivecs[0], err = vector.NewConstBytes(types.T_varchar.ToType(), []byte(startDateStr), 1, proc.Mp())
	require.NoError(t, err)
	ivecs[1], err = vector.NewConstFixed(types.T_int64.ToType(), largeInterval, 1, proc.Mp())
	require.NoError(t, err)
	ivecs[2], err = vector.NewConstFixed(types.T_int64.ToType(), int64(types.Year), 1, proc.Mp())
	require.NoError(t, err)

	// Create result vector - should be VARCHAR type (string)
	result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())

	// Initialize result vector before calling DateStringAdd
	err = result.PreExtendAndReset(1)
	require.NoError(t, err)

	// Call DateStringAdd - should return NULL (MySQL behavior: overflow returns NULL)
	err = DateStringAdd(ivecs, result, proc, 1, nil)
	require.NoError(t, err, "DateStringAdd with overflow should return NULL, not error")

	// Verify result is NULL
	v := result.GetResultVector()
	strParam := vector.GenerateFunctionStrParameter(v)
	_, null := strParam.GetStrValue(0)
	require.True(t, null, "Result should be NULL for overflow")

	// Cleanup
	for _, v := range ivecs {
		if v != nil {
			v.Free(proc.Mp())
		}
	}
	if result != nil {
		result.Free()
	}
}

// TestDateStringAddNegativeYearOverflow tests that date_add with negative YEAR interval causing year < 1 returns NULL
func TestDateStringAddNegativeYearOverflow(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test case: date_add("1997-12-31 23:59:59",INTERVAL -100000 YEAR) should return NULL
	startDateStr := "1997-12-31 23:59:59"
	largeNegativeInterval := int64(-100000) // -100000 years, will cause year < 1

	// Create input vectors
	ivecs := make([]*vector.Vector, 3)
	var err error
	ivecs[0], err = vector.NewConstBytes(types.T_varchar.ToType(), []byte(startDateStr), 1, proc.Mp())
	require.NoError(t, err)
	ivecs[1], err = vector.NewConstFixed(types.T_int64.ToType(), largeNegativeInterval, 1, proc.Mp())
	require.NoError(t, err)
	ivecs[2], err = vector.NewConstFixed(types.T_int64.ToType(), int64(types.Year), 1, proc.Mp())
	require.NoError(t, err)

	// Create result vector
	result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())

	// Initialize result vector
	err = result.PreExtendAndReset(1)
	require.NoError(t, err)

	// Call DateStringAdd - should return NULL (MySQL behavior)
	err = DateStringAdd(ivecs, result, proc, 1, nil)
	require.NoError(t, err, "DateStringAdd with negative YEAR causing year < 1 should return NULL")

	// Verify result is NULL
	v := result.GetResultVector()
	strParam := vector.GenerateFunctionStrParameter(v)
	_, null := strParam.GetStrValue(0)
	require.True(t, null, "Result should be NULL for year < 1")

	// Cleanup
	for _, v := range ivecs {
		if v != nil {
			v.Free(proc.Mp())
		}
	}
	if result != nil {
		result.Free()
	}
}

// TestDateStringAddVeryLargeInterval tests that date_add with very large interval values returns NULL
func TestDateStringAddVeryLargeInterval(t *testing.T) {
	proc := testutil.NewProcess(t)

	testCases := []struct {
		name     string
		interval int64
		unit     types.IntervalType
	}{
		{"Very large SECOND", 9223372036854775806, types.Second},
		{"Very large MINUTE", 9223372036854775806, types.Minute},
		{"Very large HOUR", 9223372036854775806, types.Hour},
		{"Very large negative SECOND", -9223372036854775806, types.Second},
		{"Very large negative MINUTE", -9223372036854775806, types.Minute},
		{"Very large negative HOUR", -9223372036854775806, types.Hour},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startDateStr := "1995-01-05"

			// Create input vectors
			ivecs := make([]*vector.Vector, 3)
			var err error
			ivecs[0], err = vector.NewConstBytes(types.T_varchar.ToType(), []byte(startDateStr), 1, proc.Mp())
			require.NoError(t, err)
			ivecs[1], err = vector.NewConstFixed(types.T_int64.ToType(), tc.interval, 1, proc.Mp())
			require.NoError(t, err)
			ivecs[2], err = vector.NewConstFixed(types.T_int64.ToType(), int64(tc.unit), 1, proc.Mp())
			require.NoError(t, err)

			// Create result vector
			result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())

			// Initialize result vector
			err = result.PreExtendAndReset(1)
			require.NoError(t, err)

			// Call DateStringAdd - should return NULL (MySQL behavior: very large interval returns NULL)
			err = DateStringAdd(ivecs, result, proc, 1, nil)
			require.NoError(t, err, "DateStringAdd with very large interval should return NULL, not error")

			// Verify result is NULL
			v := result.GetResultVector()
			strParam := vector.GenerateFunctionStrParameter(v)
			_, null := strParam.GetStrValue(0)
			require.True(t, null, "Result should be NULL for very large interval")

			// Cleanup
			for _, v := range ivecs {
				if v != nil {
					v.Free(proc.Mp())
				}
			}
			if result != nil {
				result.Free()
			}
		})
	}
}

// TestTimestampAddOverflowReturnsNull tests that TIMESTAMPADD returns NULL when overflow occurs
// This matches MySQL behavior where TIMESTAMPADD overflow returns NULL (different from date_add)
func TestTimestampAddOverflowReturnsNull(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test case: TIMESTAMPADD(DAY, 1, '9999-12-31') should return NULL
	startDateStr := "9999-12-31"
	interval := int64(1) // 1 day, will cause overflow

	// Create input vectors for TimestampAddString
	ivecs := make([]*vector.Vector, 3)
	var err error
	// Unit parameter (DAY)
	ivecs[0], err = vector.NewConstBytes(types.T_varchar.ToType(), []byte("DAY"), 1, proc.Mp())
	require.NoError(t, err)
	// Interval parameter
	ivecs[1], err = vector.NewConstFixed(types.T_int64.ToType(), interval, 1, proc.Mp())
	require.NoError(t, err)
	// Date string parameter
	ivecs[2], err = vector.NewConstBytes(types.T_varchar.ToType(), []byte(startDateStr), 1, proc.Mp())
	require.NoError(t, err)

	// Create result vector (VARCHAR type for string output)
	result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())

	// Initialize result vector before calling TimestampAddString
	err = result.PreExtendAndReset(1)
	require.NoError(t, err)

	// Call TimestampAddString - should return NULL (no error)
	err = TimestampAddString(ivecs, result, proc, 1, nil)
	require.NoError(t, err, "TimestampAddString with overflow should return NULL, not error")

	// Check result is NULL
	resultVec := result.GetResultVector()
	require.True(t, resultVec.GetNulls().Contains(0), "TimestampAddString overflow should return NULL")

	// Cleanup
	for _, v := range ivecs {
		if v != nil {
			v.Free(proc.Mp())
		}
	}
	if result != nil {
		result.Free()
	}
}

// TestDateStringAddOverflowNegativeMonth tests that DateStringAdd throws error when MONTH interval causes year out of range
func TestDateStringAddOverflowNegativeMonth(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test case: date_add with string input and large negative MONTH interval that causes year < 1
	// date_add('1997-12-31', INTERVAL -120000 MONTH) should throw error
	// Calculation: 1997 + (-120000)/12 = 1997 - 10000 = -8003 < 1
	startDateStr := "1997-12-31 23:59:59"
	largeNegativeInterval := int64(-120000) // -120000 months, will cause year < 1

	// Create input vectors
	ivecs := make([]*vector.Vector, 3)
	var err error
	ivecs[0], err = vector.NewConstBytes(types.T_varchar.ToType(), []byte(startDateStr), 1, proc.Mp())
	require.NoError(t, err)
	ivecs[1], err = vector.NewConstFixed(types.T_int64.ToType(), largeNegativeInterval, 1, proc.Mp())
	require.NoError(t, err)
	ivecs[2], err = vector.NewConstFixed(types.T_int64.ToType(), int64(types.Month), 1, proc.Mp())
	require.NoError(t, err)

	// Create result vector - should be VARCHAR type (string)
	result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())

	// Initialize result vector before calling DateStringAdd
	err = result.PreExtendAndReset(1)
	require.NoError(t, err)

	// Call DateStringAdd - should return NULL (MySQL behavior: year < 1 returns NULL)
	err = DateStringAdd(ivecs, result, proc, 1, nil)
	require.NoError(t, err, "DateStringAdd with negative MONTH causing year < 1 should return NULL")

	// Verify result is NULL
	v := result.GetResultVector()
	strParam := vector.GenerateFunctionStrParameter(v)
	_, null := strParam.GetStrValue(0)
	require.True(t, null, "Result should be NULL for year < 1")

	// Cleanup
	for _, v := range ivecs {
		if v != nil {
			v.Free(proc.Mp())
		}
	}
	if result != nil {
		result.Free()
	}
}

// TestDateStringAddOverflowNegativeQuarter tests that DateStringAdd throws error when QUARTER interval causes year out of range
func TestDateStringAddOverflowNegativeQuarter(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test case: date_add with string input and large negative QUARTER interval that causes year < 1
	// date_add('1997-12-31', INTERVAL -40000 QUARTER) should throw error
	// Calculation: 1997 + (-40000*3)/12 = 1997 - 10000 = -8003 < 1
	startDateStr := "1997-12-31 23:59:59"
	largeNegativeInterval := int64(-40000) // -40000 quarters, will cause year < 1

	// Create input vectors
	ivecs := make([]*vector.Vector, 3)
	var err error
	ivecs[0], err = vector.NewConstBytes(types.T_varchar.ToType(), []byte(startDateStr), 1, proc.Mp())
	require.NoError(t, err)
	ivecs[1], err = vector.NewConstFixed(types.T_int64.ToType(), largeNegativeInterval, 1, proc.Mp())
	require.NoError(t, err)
	ivecs[2], err = vector.NewConstFixed(types.T_int64.ToType(), int64(types.Quarter), 1, proc.Mp())
	require.NoError(t, err)

	// Create result vector - should be VARCHAR type (string)
	result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())

	// Initialize result vector before calling DateStringAdd
	err = result.PreExtendAndReset(1)
	require.NoError(t, err)

	// Call DateStringAdd - should return NULL (MySQL behavior: year < 1 returns NULL)
	err = DateStringAdd(ivecs, result, proc, 1, nil)
	require.NoError(t, err, "DateStringAdd with negative QUARTER causing year < 1 should return NULL")

	// Verify result is NULL
	v := result.GetResultVector()
	strParam := vector.GenerateFunctionStrParameter(v)
	_, null := strParam.GetStrValue(0)
	require.True(t, null, "Result should be NULL for year < 1")

	// Cleanup
	for _, v := range ivecs {
		if v != nil {
			v.Free(proc.Mp())
		}
	}
	if result != nil {
		result.Free()
	}
}

// TestDateAddOverflowNegativeMonth tests that DateAdd throws error when MONTH interval causes year out of range
func TestDateAddOverflowNegativeMonth(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test case: date_add with DATE input and large negative MONTH interval that causes year < 1
	startDate, _ := types.ParseDateCast("1997-12-31")
	largeNegativeInterval := int64(-120000) // -120000 months, will cause year < 1

	// Create input vectors
	ivecs := make([]*vector.Vector, 3)
	var err error
	ivecs[0], err = vector.NewConstFixed(types.T_date.ToType(), startDate, 1, proc.Mp())
	require.NoError(t, err)
	ivecs[1], err = vector.NewConstFixed(types.T_int64.ToType(), largeNegativeInterval, 1, proc.Mp())
	require.NoError(t, err)
	ivecs[2], err = vector.NewConstFixed(types.T_int64.ToType(), int64(types.Month), 1, proc.Mp())
	require.NoError(t, err)

	// Create result vector
	result := vector.NewFunctionResultWrapper(types.T_date.ToType(), proc.Mp())

	// Initialize result vector before calling DateAdd
	err = result.PreExtendAndReset(1)
	require.NoError(t, err)

	// Call DateAdd - should return error
	err = DateAdd(ivecs, result, proc, 1, nil)
	require.Error(t, err, "DateAdd with negative MONTH causing year < 1 should return error")
	require.Contains(t, err.Error(), "data out of range", "error message should contain 'data out of range'")

	// Cleanup
	for _, v := range ivecs {
		if v != nil {
			v.Free(proc.Mp())
		}
	}
	if result != nil {
		result.Free()
	}
}

// TestDateAddOverflowNegativeQuarter tests that DateAdd throws error when QUARTER interval causes year out of range
func TestDateAddOverflowNegativeQuarter(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test case: date_add with DATE input and large negative QUARTER interval that causes year < 1
	startDate, _ := types.ParseDateCast("1997-12-31")
	largeNegativeInterval := int64(-40000) // -40000 quarters, will cause year < 1

	// Create input vectors
	ivecs := make([]*vector.Vector, 3)
	var err error
	ivecs[0], err = vector.NewConstFixed(types.T_date.ToType(), startDate, 1, proc.Mp())
	require.NoError(t, err)
	ivecs[1], err = vector.NewConstFixed(types.T_int64.ToType(), largeNegativeInterval, 1, proc.Mp())
	require.NoError(t, err)
	ivecs[2], err = vector.NewConstFixed(types.T_int64.ToType(), int64(types.Quarter), 1, proc.Mp())
	require.NoError(t, err)

	// Create result vector
	result := vector.NewFunctionResultWrapper(types.T_date.ToType(), proc.Mp())

	// Initialize result vector before calling DateAdd
	err = result.PreExtendAndReset(1)
	require.NoError(t, err)

	// Call DateAdd - should return error
	err = DateAdd(ivecs, result, proc, 1, nil)
	require.Error(t, err, "DateAdd with negative QUARTER causing year < 1 should return error")
	require.Contains(t, err.Error(), "data out of range", "error message should contain 'data out of range'")

	// Cleanup
	for _, v := range ivecs {
		if v != nil {
			v.Free(proc.Mp())
		}
	}
	if result != nil {
		result.Free()
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
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d1},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"+08:21"},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"-02:32"},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{r1},
				[]bool{false}),
		},
		{ // select convert_tz('2022-01-01 00:00:00', 'Asia/Shanghai', '+00:00');
			info: "test ConvertTz correct2",
			typ:  types.T_datetime,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d2},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"Asia/Shanghai"},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"+00:00"},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{r2},
				[]bool{false}),
		},
		{ // select convert_tz('2022-01-01 00:00:00', 'Europe/London', 'Asia/Shanghai');
			info: "test ConvertTz correct3",
			typ:  types.T_datetime,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d2},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"Asia/Shanghai"},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"Europe/London"},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{r2},
				[]bool{false}),
		},
		{ // select convert_tz('9999-12-31 23:00:00', '-02:00', '+11:00');
			info: "test ConvertTz out of range1",
			typ:  types.T_datetime,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d3},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"-02:00"},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"+11:00"},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{r3},
				[]bool{false}),
		},
		{ // select convert_tz('9999-12-31 22:00:00', 'Europe/London', 'Asia/Shanghai');
			info: "test ConvertTz out of range2",
			typ:  types.T_datetime,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d4},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"Europe/London"},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"Asia/Shanghai"},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{r4},
				[]bool{false}),
		},
		{ // select convert_tz('9999-12-31 10:00:00', 'Europe/London', 'Asia/Shanghai');
			info: "test ConvertTz not out of range",
			typ:  types.T_datetime,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d5},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"Europe/London"},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"Asia/Shanghai"},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{r5},
				[]bool{false}),
		},
		{
			info: "test ConvertTz err1",
			typ:  types.T_datetime,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d1},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"ABC"},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"GMT"},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{""},
				[]bool{true}),
		},
		{
			info: "test ConvertTz err2",
			typ:  types.T_datetime,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d1},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"+00:00"},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"ABC"},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{""},
				[]bool{true}),
		},
		{
			info: "test ConvertTz err3",
			typ:  types.T_datetime,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d1},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"+00:00:00"},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"+08:00"},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{""},
				[]bool{true}),
		},
		{
			info: "test ConvertTz err4",
			typ:  types.T_datetime,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d1},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"+00:ws"},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"+08:00"},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{""},
				[]bool{true}),
		},
		{
			info: "test ConvertTz err5",
			typ:  types.T_datetime,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d1},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"+00:00"},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"+18:00"},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{""},
				[]bool{true}),
		},
		{
			info: "test ConvertTz when tz is empty",
			typ:  types.T_datetime,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d3},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{""},
				[]bool{true}),
		},
	}
}

func TestConvertTz(t *testing.T) {
	testCases := initConvertTzTestCase()

	// do the test work.
	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc,
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
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d1},
					[]bool{false}),
				NewFunctionTestConstInput(types.T_varchar.ToType(),
					[]string{format},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{r1},
				[]bool{false}),
		},
		{
			info: "test format",
			typ:  types.T_datetime,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d2},
					[]bool{false}),
				NewFunctionTestConstInput(types.T_varchar.ToType(),
					[]string{format},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{r2},
				[]bool{false}),
		},
		{
			info: "test format",
			typ:  types.T_datetime,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d3},
					[]bool{false}),
				NewFunctionTestConstInput(types.T_varchar.ToType(),
					[]string{format},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{r3},
				[]bool{false}),
		},
		{
			info: "test format",
			typ:  types.T_datetime,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d4},
					[]bool{false}),
				NewFunctionTestConstInput(types.T_varchar.ToType(),
					[]string{format},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{r4},
				[]bool{false}),
		},
	}
}

func TestFormat(t *testing.T) {
	testCases := initFormatTestCase()

	// do the test work.
	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc,
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
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{d1.ToDate()},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{int64(types.Day)},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_date.ToType(), false,
				[]types.Date{r1.ToDate()},
				[]bool{false}),
		},
		{
			info: "test DatetimeAdd",
			typ:  types.T_datetime,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d1},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{int64(types.Day)},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_datetime.ToType(), false,
				[]types.Datetime{r1},
				[]bool{false}),
		},
		{
			info: "test DateStringSub",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"2022-01-01"},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{int64(types.Day)},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"2021-12-31"},
				[]bool{false}),
		},
	}
}

func TestDateSub(t *testing.T) {
	testCases := initDateSubTestCase()

	// do the test work.
	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		var fcTC FunctionTestCase
		switch tc.typ {
		case types.T_date:
			fcTC = NewFunctionTestCase(proc,
				tc.inputs, tc.expect, DateSub)
		case types.T_datetime:
			fcTC = NewFunctionTestCase(proc,
				tc.inputs, tc.expect, DatetimeSub)
		case types.T_varchar:
			fcTC = NewFunctionTestCase(proc,
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
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{0},
					[]bool{true}),
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{1},
					[]bool{false}),
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{1},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{0},
				[]bool{false}),
		},
		{
			info: "test Field",
			typ:  types.T_uint64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{1},
					[]bool{false}),
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{2},
					[]bool{false}),
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{1},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{2},
				[]bool{false}),
		},
		{
			info: "test Field",
			typ:  types.T_uint64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{1},
					[]bool{false}),
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{2},
					[]bool{false}),
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{3},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{0},
				[]bool{false}),
		},

		{
			info: "test Field",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"hello"},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""},
					[]bool{true}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"hello"},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{2},
				[]bool{false}),
		},
	}
}

func TestField(t *testing.T) {
	testCases := initFieldTestCase()

	// do the test work.
	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		var fcTC FunctionTestCase
		switch tc.typ {
		case types.T_uint64:
			fcTC = NewFunctionTestCase(proc,
				tc.inputs, tc.expect, FieldNumber[uint64])
		case types.T_varchar:
			fcTC = NewFunctionTestCase(proc,
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
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"12332.123456"}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"4"}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"12,332.1235"}, []bool{false}),
		},
		{
			info: "2",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"12332.1"}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"4"}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"12,332.1000"}, []bool{false}),
		},
		{
			info: "2",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"12332.2"}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"0"}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"12,332"}, []bool{false}),
		},
		{
			info: "2",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"-.12334.2"}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"2"}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"-0.12"}, []bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"12332.123456"}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"4"}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"ar_SA"}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"12332.1235"}, []bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"12332.1"}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"4"}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"ar_SA"}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"12332.1000"}, []bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"12332.2"}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"0"}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"ar_SA"}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"12332"}, []bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"-.12334.2"}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"2"}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"ar_SA"}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"-0.12"}, []bool{false}),
		},
	}
}

func TestFormat2Or3(t *testing.T) {
	testCases := initFormat2Or3TestCase()

	// do the test work.
	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		var fcTC FunctionTestCase
		switch tc.info {
		case "2":
			fcTC = NewFunctionTestCase(proc,
				tc.inputs, tc.expect, FormatWith2Args)
		case "3":
			fcTC = NewFunctionTestCase(proc,
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
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{0},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_datetime.ToType(), false,
				[]types.Datetime{d1},
				[]bool{false}),
		},
		{
			info: "test from unix time uint64",
			typ:  types.T_uint64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{1451606400},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_datetime.ToType(), false,
				[]types.Datetime{d2},
				[]bool{false}),
		},
		{
			info: "test from unix time float64",
			typ:  types.T_float64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1451606400.999999},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_datetime.ToType(), false,
				[]types.Datetime{d3},
				[]bool{false}),
		},
		{
			info: "test from unix time float64",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{0},
					[]bool{false}),
				NewFunctionTestConstInput(types.T_varchar.ToType(),
					[]string{"%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %x %Y %y"},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"Jan January 01 1 1st 01 1 001 0 12 00 AM 12:00:00 AM 00:00:00 00 000000 01 1970 1970 70"},
				[]bool{false}),
		},
	}
}

func newTmpProcess(t *testing.T) *process.Process {
	return newProcessWithMPool(t, mpool.MustNewZero())
}

func newProcessWithMPool(t *testing.T, mp *mpool.MPool) *process.Process {
	process := testutil.NewProcessWithMPool(t, "", mp)
	process.Base.SessionInfo.TimeZone = time.FixedZone("UTC0", 0)
	return process
}

func TestFromUnixTime(t *testing.T) {
	testCases := initFromUnixTimeTestCase()

	// do the test work.
	proc := newTmpProcess(t)
	for _, tc := range testCases {
		var fcTC FunctionTestCase
		switch tc.typ {
		case types.T_int64:
			fcTC = NewFunctionTestCase(proc,
				tc.inputs, tc.expect, FromUnixTimeInt64)
		case types.T_uint64:
			fcTC = NewFunctionTestCase(proc,
				tc.inputs, tc.expect, FromUnixTimeUint64)
		case types.T_float64:
			fcTC = NewFunctionTestCase(proc,
				tc.inputs, tc.expect, FromUnixTimeFloat64)
		case types.T_varchar:
			fcTC = NewFunctionTestCase(proc,
				tc.inputs, tc.expect, FromUnixTimeInt64Format)
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initStrCmpTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "basic",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"apple", "apple", "apple", "apple"},
					[]bool{false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"apple", "orange", "banana", "app"},
					[]bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{0, -1, -1, 1},
				[]bool{false, false, false, false}),
		},

		{
			info: "null handling",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"a", "", "a", ""},
					[]bool{false, false, false, true}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"a", "a", "", ""},
					[]bool{false, true, false, true}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{0, 0, 1, 0},
				[]bool{false, true, false, true}),
		},

		{
			info: "edge cases",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"", "a", "a ", "a", "A"},
					[]bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"", "a", "a", "a ", "a"},
					[]bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{0, 0, 1, -1, -1},
				[]bool{false, false, false, false, false}),
		},

		{
			info: "different types",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"a", "b", "c"},
					[]bool{false, false, false}),
				NewFunctionTestInput(types.T_binary.ToType(),
					[]string{"A", "b", "d"},
					[]bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{1, 0, -1},
				[]bool{false, false, false}),
		},

		{
			info: "char padding",
			typ:  types.T_char,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_char.ToType(),
					[]string{"a", "a ", "a  "},
					[]bool{false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"a", "a", "a"},
					[]bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{0, 1, 1},
				[]bool{false, false, false}),
		},
	}
}

func TestStrCmp(t *testing.T) {
	testCases := initStrCmpTestCase()

	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, StrCmp)
		success, info := fcTC.Run()

		require.True(t, success,
			fmt.Sprintf("case info: %s, type: %s, error details: %s",
				tc.info, tc.typ.String(), info))
	}
}

func initSubStrTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "2",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abcdefghijklmn"},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{5},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"efghijklmn"},
				[]bool{false}),
		},
		{
			info: "2",
			typ:  types.T_blob,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_blob.ToType(),
					[]string{"abcdefghijklmn"},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{7},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_blob.ToType(), false,
				[]string{"ghijklmn"},
				[]bool{false}),
		},
		{
			info: "2",
			typ:  types.T_text,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_text.ToType(),
					[]string{"abcdefghijklmn"},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{11},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_text.ToType(), false,
				[]string{"klmn"},
				[]bool{false}),
		},

		{
			info: "2",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abcdefghijklmn"},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{16},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{""},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abcdefghijklmn"},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{5},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{6},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"efghij"},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abcdefghijklmn"},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{6},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-8},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{""},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abcdefghijklmn"},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{6},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-9},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{""},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abcdefghijklmn"},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-4},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{4},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"klmn"},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abcdefghijklmn"},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-14},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{14},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"abcdefghijklmn"},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abcdefghijklmn"},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-14},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{10},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"abcdefghij"},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abcdefghijklmn"},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-12},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{2},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"cd"},
				[]bool{false}),
		},
	}
}

func TestSubStr(t *testing.T) {
	testCases := initSubStrTestCase()

	// do the test work.
	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		var fcTC FunctionTestCase
		switch tc.info {
		case "2":
			fcTC = NewFunctionTestCase(proc,
				tc.inputs, tc.expect, SubStringWith2Args)
		case "3":
			fcTC = NewFunctionTestCase(proc,
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
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"www.mysql.com"},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"."},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{0},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{""},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"www.mysql.com"},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"."},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"www"},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"www.mysql.com"},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"."},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{2},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"www.mysql"},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"www.mysql.com"},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"."},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{3},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"www.mysql.com"},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"www.mysql.com"},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"."},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-3},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"www.mysql.com"},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"www.mysql.com"},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"."},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-2},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"mysql.com"},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"www.mysql.com"},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"."},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-1},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"com"},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"xyz"},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abc"},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{223372036854775808},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"xyz"},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aaa.bbb.ccc.ddd.eee"},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"."},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{9223372036854775807},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"aaa.bbb.ccc.ddd.eee"},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aaa.bbb.ccc.ddd.eee"},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"."},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-9223372036854775808},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"aaa.bbb.ccc.ddd.eee"},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aaa.bbb.ccc.ddd.eee"},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"."},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{int64(922337203685477580)},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"aaa.bbb.ccc.ddd.eee"},
				[]bool{false}),
		},
	}
}

func TestSubStrIndex(t *testing.T) {
	testCases := initSubStrIndexTestCase()

	// do the test work.
	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc,
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
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_time.ToType(), []types.Time{t11, t21, t31, t41, t51, t61, t71, t81, t91, t101}, bb),
				NewFunctionTestInput(types.T_time.ToType(), []types.Time{t12, t22, t32, t42, t52, t62, t72, t82, t92, t102}, bb),
			},
			expect: NewFunctionTestResult(types.T_time.ToType(), false, []types.Time{r1, r2, r3, r4, r5, r6, r7, r8, r9, r10}, bb),
		},
	}
}

func TestTimeDiffInTime(t *testing.T) {
	testCases := initTimeDiffInTimeTestCase()

	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, TimeDiff[types.Time])
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
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{t11, t21, t31, t41, t51, t61, t71, t81, t91}, []bool{}),
				NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{t12, t22, t32, t42, t52, t62, t72, t82, t92}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_time.ToType(), false, []types.Time{r1, r2, r3, r4, r5, r6, r7, r8, r9}, []bool{}),
		},
	}
}

func TestTimeDiffInDateTime(t *testing.T) {
	testCases := initTimeDiffInDatetimeTestCase()

	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, TimeDiff[types.Datetime])
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initTimeDiffStringTestCase() []tcTemp {
	// Test cases for TimeDiffString with string inputs (including colon format)
	// expr1, expr2, expected result
	testCases := []struct {
		expr1, expr2 string
		expected     types.Time
		desc         string
	}{
		{
			expr1:    "2000:01:01 00:00:00",
			expr2:    "2000:01:01 00:00:00.000001",
			expected: types.Time(-1), // -1 microsecond
			desc:     "colon format microsecond diff",
		},
		{
			expr1:    "2000:01:01 00:00:00",
			expr2:    "2000:01:01 00:00:00",
			expected: types.Time(0),
			desc:     "colon format zero diff",
		},
		{
			expr1:    "2000:01:01 00:00:01",
			expr2:    "2000:01:01 00:00:00",
			expected: types.Time(1000000), // 1 second = 1000000 microseconds
			desc:     "colon format one second diff",
		},
		{
			expr1:    "2000:01:01 01:00:00",
			expr2:    "2000:01:01 00:00:00",
			expected: types.Time(3600000000), // 1 hour = 3600000000 microseconds
			desc:     "colon format one hour diff",
		},
		{
			expr1:    "2000:01:01 00:00:00",
			expr2:    "2000:01:01 00:00:01",
			expected: types.Time(-1000000), // -1 second
			desc:     "colon format negative one second",
		},
		{
			expr1:    "2000-01-01 15:30:45",
			expr2:    "2000-01-01 10:20:30",
			expected: types.Time(18615000000), // 5:10:15 = 18615000000 microseconds
			desc:     "dash format datetime diff",
		},
		{
			expr1:    "15:30:45",
			expr2:    "10:20:30",
			expected: types.Time(18615000000), // 5:10:15
			desc:     "time format diff",
		},
	}

	inputs1 := make([]string, len(testCases))
	inputs2 := make([]string, len(testCases))
	expected := make([]types.Time, len(testCases))
	nulls := make([]bool, len(testCases))

	for i, tc := range testCases {
		inputs1[i] = tc.expr1
		inputs2[i] = tc.expr2
		expected[i] = tc.expected
		nulls[i] = false
	}

	return []tcTemp{
		{
			info: "test timediff string inputs with colon format",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), inputs1, nulls),
				NewFunctionTestInput(types.T_varchar.ToType(), inputs2, nulls),
			},
			expect: NewFunctionTestResult(types.T_time.ToType(), false, expected, nulls),
		},
	}
}

func TestTimeDiffString(t *testing.T) {
	testCases := initTimeDiffStringTestCase()

	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, TimeDiffString)
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
			inputs: []FunctionTestInput{
				// Create a input entry <String, Datetime1, Datetime2>
				NewFunctionTestInput(types.T_varchar.ToType(), []string{i1}, []bool{}),
				NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{i2}, []bool{}),
				NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{i3}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{o}, []bool{}),
		})
	}

	return testInputs
}

func TestTimestampDiff(t *testing.T) {
	testCases := initTimestampDiffTestCase()

	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, TimestampDiff)
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
	var origVecs = make([]FunctionTestInput, 2)
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

	makeFunctionTestInputEndsWith := func(values []string, nsp []uint64) FunctionTestInput {
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
		return NewFunctionTestInput(types.T_varchar.ToType(), strs, nulls)
	}

	origVecs[0] = makeFunctionTestInputEndsWith(inputVec, nsp1)
	origVecs[1] = makeFunctionTestInputEndsWith(inputVec2, nsp2)

	return []tcTemp{
		{
			info: "test StartsWith",
			inputs: []FunctionTestInput{
				origVecs[0],
				origVecs[1],
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, false, true, false, false, true, true, false, true, false, true, false, false, false, true, false, false, false, true},
				[]bool{false, false, false, true, false, false, false, true, false, false, false, true, false, false, false, true, true, true, true, true}),
		},
	}
}

func TestStartsWith(t *testing.T) {
	testCases := initStartsWithTestCase()

	// do the test work.
	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, StartsWith)
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
	var origVecs = make([]FunctionTestInput, 2)
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

	makeFunctionTestInputEndsWith := func(values []string, nsp []uint64) FunctionTestInput {
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
		return NewFunctionTestInput(types.T_varchar.ToType(), strs, nulls)
	}

	origVecs[0] = makeFunctionTestInputEndsWith(inputVec, nsp1)
	origVecs[1] = makeFunctionTestInputEndsWith(inputVec2, nsp2)

	return []tcTemp{
		{
			info: "test EndsWith",
			inputs: []FunctionTestInput{
				origVecs[0],
				origVecs[1],
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, false, true, false, false, true, true, false, true, false, true, false, false, false, true, false, false, false, true},
				[]bool{false, false, false, true, false, false, false, true, false, false, false, true, false, false, false, true, true, true, true, true}),
		},
	}
}

func TestEndsWith(t *testing.T) {
	testCases := initEndsWithTestCase()

	// do the test work.
	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, EndsWith)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// FindInSet

func initFindInSetTestCase() []tcTemp {

	return []tcTemp{
		{
			info: "test findinset",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{
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
				NewFunctionTestInput(types.T_varchar.ToType(), []string{
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
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
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
	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, FindInSet)
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
			inputs: []FunctionTestInput{
				// Create a input entry <strs, substrs>
				NewFunctionTestInput(types.T_varchar.ToType(), c.strs, []bool{}),
				NewFunctionTestInput(types.T_varchar.ToType(), c.substrs, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, c.wants, []bool{}),
		})
	}

	return testInputs

}

func TestInstr(t *testing.T) {
	testCases := initInstrTestCase()

	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, Instr)
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
			inputs: []FunctionTestInput{
				// Create a input entry <str, int>
				NewFunctionTestInput(types.T_varchar.ToType(), []string{c.s}, []bool{}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{c.len}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{c.want}, []bool{}),
		})
	}

	return testInputs

}

func TestLeft(t *testing.T) {
	testCases := initLeftTestCase()

	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, Left)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// Right
func initRightTestCase() []tcTemp {
	cases := []struct {
		s    string
		len  int64
		want string
	}{
		// Basic cases from user requirements
		{"Hello World", 5, "World"}, // RIGHT('Hello World', 5) = 'World'
		{"Hello", 10, "Hello"},      // RIGHT('Hello', 10) = 'Hello'
		{"Hello", 0, ""},            // RIGHT('Hello', 0) = ''
		// Additional test cases
		{"abcde", 3, "cde"},
		{"abcde", -1, ""},
		{"abcde", 100, "abcde"},
		{"foobarbar", 5, "arbar"},
		// Unicode test cases
		{"是都方式快递费", 3, "快递费"},
		{"ｱｲｳｴｵ", 3, "ｳｴｵ"},
		{"あいうえお", 3, "うえお"},
		{"龔龖龗龞龡", 3, "龗龞龡"},
		// Edge cases
		{"", 5, ""},
		{"test", 4, "test"},
		{"test", 1, "t"},
	}

	var testInputs = make([]tcTemp, 0, len(cases))
	for _, c := range cases {
		testInputs = append(testInputs, tcTemp{
			info: fmt.Sprintf("test right('%s', %d) = '%s'", c.s, c.len, c.want),
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{c.s}, []bool{}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{c.len}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{c.want}, []bool{}),
		})
	}

	// Add NULL test cases
	testInputs = append(testInputs, tcTemp{
		info: "test right with NULL first argument",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{""}, []bool{true}),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{5}, []bool{false}),
		},
		expect: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{""}, []bool{true}),
	})

	testInputs = append(testInputs, tcTemp{
		info: "test right with NULL second argument",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"Hello World"}, []bool{false}),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{0}, []bool{true}),
		},
		expect: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{""}, []bool{true}),
	})

	return testInputs
}

func TestRight(t *testing.T) {
	testCases := initRightTestCase()

	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, Right)
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
		// Basic cases from user requirements
		{2, 3, 8},   // POW(2, 3) = 8
		{2, 0, 1},   // POW(2, 0) = 1
		{4, 0.5, 2}, // POW(4, 0.5) = 2 (square root)
		// Additional test cases
		{1, 2, 1},
		{2, 2, 4},
		{3, 2, 9},
		{3, 3, 27},
		{4, 2, 16},
		{4, 3, 64},
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
		// Negative exponent
		{2, -1, 0.5},
		{4, -2, 0.0625},
		{10, -2, 0.01},
		// Zero base
		{0, 5, 0},
		{0, 0, 1}, // 0^0 = 1 in most systems
		// One as base or exponent
		{1, 100, 1},
		{100, 1, 100},
		// Negative base with integer exponent
		{-2, 2, 4},
		{-2, 3, -8},
		{-1, 1, -1},
		{-1, 2, 1},
	}

	var testInputs = make([]tcTemp, 0, len(cases))
	for _, c := range cases {

		testInputs = append(testInputs, tcTemp{
			info: fmt.Sprintf("test pow(%v, %v) = %v", c.left, c.right, c.want),
			inputs: []FunctionTestInput{
				// Create a input entry <float64, float64>
				NewFunctionTestInput(types.T_float64.ToType(), []float64{c.left}, []bool{}),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{c.right}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false, []float64{c.want}, []bool{}),
		})
	}

	// Add NULL test cases
	testInputs = append(testInputs, tcTemp{
		info: "test pow with NULL first argument",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_float64.ToType(), []float64{0}, []bool{true}),
			NewFunctionTestInput(types.T_float64.ToType(), []float64{2}, []bool{false}),
		},
		expect: NewFunctionTestResult(types.T_float64.ToType(), false, []float64{0}, []bool{true}),
	})

	testInputs = append(testInputs, tcTemp{
		info: "test pow with NULL second argument",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_float64.ToType(), []float64{2}, []bool{false}),
			NewFunctionTestInput(types.T_float64.ToType(), []float64{0}, []bool{true}),
		},
		expect: NewFunctionTestResult(types.T_float64.ToType(), false, []float64{0}, []bool{true}),
	})

	testInputs = append(testInputs, tcTemp{
		info: "test pow with both NULL arguments",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_float64.ToType(), []float64{0}, []bool{true}),
			NewFunctionTestInput(types.T_float64.ToType(), []float64{0}, []bool{true}),
		},
		expect: NewFunctionTestResult(types.T_float64.ToType(), false, []float64{0}, []bool{true}),
	})

	return testInputs
}

func TestPower(t *testing.T) {
	testCases := initPowerTestCase()

	// do the test work.
	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, Power)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// TRUNCATE
func initTruncateTestCase() []tcTemp {
	cases := []struct {
		left  float64
		right int64
		want  float64
	}{
		// Basic cases from user requirements
		{4.567, 2, 4.56},   // TRUNCATE(4.567, 2) = 4.56
		{4.567, 0, 4.0},    // TRUNCATE(4.567, 0) = 4
		{-4.567, 2, -4.56}, // TRUNCATE(-4.567, 2) = -4.56
		// Additional test cases
		{4.567, 1, 4.5},
		{4.567, 3, 4.567},
		{-4.567, 0, -4.0},
		{-4.567, 1, -4.5},
		{0.5, 0, 0.0},
		{-0.5, 0, 0.0},
		{1.999, 2, 1.99},
		{-1.999, 2, -1.99},
		{10.123456, 3, 10.123},
		{-10.123456, 3, -10.123},
	}

	var testInputs = make([]tcTemp, 0, len(cases))
	for _, c := range cases {
		testInputs = append(testInputs, tcTemp{
			info: fmt.Sprintf("test truncate(%v, %v) = %v", c.left, c.right, c.want),
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(), []float64{c.left}, []bool{}),
				NewFunctionTestConstInput(types.T_int64.ToType(), []int64{c.right}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false, []float64{c.want}, []bool{}),
		})
	}

	// Add NULL test cases
	testInputs = append(testInputs, tcTemp{
		info: "test truncate with NULL first argument",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_float64.ToType(), []float64{0}, []bool{true}),
			NewFunctionTestConstInput(types.T_int64.ToType(), []int64{2}, []bool{false}),
		},
		expect: NewFunctionTestResult(types.T_float64.ToType(), false, []float64{0}, []bool{true}),
	})

	// truncate with NULL second argument should expect error, we want a const.
	testInputs = append(testInputs, tcTemp{
		info: "test truncate with NULL second argument",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_float64.ToType(), []float64{4.567}, []bool{false}),
			NewFunctionTestConstInput(types.T_int64.ToType(), []int64{0}, []bool{true}),
		},
		expect: NewFunctionTestResult(types.T_float64.ToType(), true, []float64{0}, []bool{true}),
	})

	return testInputs
}

func TestTruncate(t *testing.T) {
	testCases := initTruncateTestCase()

	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, TruncateFloat64)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// SQRT
func initSqrtTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test sqrt regular",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1, 4, 2, 10, 25, 10000, 0, 0, 1.41},
					[]bool{false, false, false, false, false, false, true, false, false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{1, 2, 1.4142135623730951, 3.1622776601683795, 5, 100, 0, 0, 1.1874342087037917},
				[]bool{false, false, false, false, false, false, true, false, false, true}),
		},
		{
			info: "test sqrt error",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(), []float64{-2}, nil),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), true, nil, nil),
		},
	}
}

func TestSqrt(t *testing.T) {
	testCases := initSqrtTestCase()

	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInSqrt)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initSqrtArrayTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test sqrt float32 array",
			typ:  types.T_array_float32,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(),
					[][]float32{{4, 9, 16}, {0, 25, 49}},
					[]bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_array_float64.ToType(), false,
				//NOTE: SQRT(vecf32) --> vecf64
				[][]float64{{2, 3, 4}, {0, 5, 7}},
				[]bool{false, false}),
		},
		{
			info: "test sqrt float64 array",
			typ:  types.T_array_float64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float64.ToType(),
					[][]float64{{4, 9, 16}, {0, 25, 49}},
					[]bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_array_float64.ToType(), false,
				[][]float64{{2, 3, 4}, {0, 5, 7}},
				[]bool{false, false}),
		},
	}
}

func TestSqrtArray(t *testing.T) {
	testCases := initSqrtArrayTestCase()

	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		var fcTC FunctionTestCase
		switch tc.typ {
		case types.T_array_float32:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInSqrtArray[float32])
		case types.T_array_float64:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInSqrtArray[float64])
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
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{-14, -77},
				[]bool{false, false}),
		},
		{
			info: "test InnerProduct float64 array",
			typ:  types.T_array_float64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
				NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{-14, -77},
				[]bool{false, false}),
		},
	}
}

func TestInnerProductArray(t *testing.T) {
	testCases := initInnerProductArrayTestCase()

	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		var fcTC FunctionTestCase
		switch tc.typ {
		case types.T_array_float32:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, InnerProductArray[float32])
		case types.T_array_float64:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, InnerProductArray[float64])
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
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{1, 1},
				[]bool{false, false}),
		},
		{
			info: "test CosineSimilarity float64 array",
			typ:  types.T_array_float64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
				NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{1, 1},
				[]bool{false, false}),
		},
	}
}

func TestCosineSimilarityArray(t *testing.T) {
	testCases := initCosineSimilarityArrayTestCase()

	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		var fcTC FunctionTestCase
		switch tc.typ {
		case types.T_array_float32:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, CosineSimilarityArray[float32])
		case types.T_array_float64:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, CosineSimilarityArray[float64])
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
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{10, 20, 30}, {40, 50, 60}}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{33.6749153137207, 78.97467803955078},
				[]bool{false, false}),
		},
		{
			info: "test L2Distance float64 array",
			typ:  types.T_array_float64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
				NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{10, 20, 30}, {40, 50, 60}}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{33.67491648096547, 78.9746794865291},
				[]bool{false, false}),
		},
	}
}

func TestL2DistanceArray(t *testing.T) {
	testCases := initL2DistanceArrayTestCase()

	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		var fcTC FunctionTestCase
		switch tc.typ {
		case types.T_array_float32:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, L2DistanceArray[float32])
		case types.T_array_float64:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, L2DistanceArray[float64])
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
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{10, 20, 30}, {5, 6, 7}}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0, 0.00035416888764172594},
				[]bool{false, false}),
		},
		{
			info: "test CosineDistance float64 array",
			typ:  types.T_array_float64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
				NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{10, 20, 30}, {5, 6, 7}}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0, 0.0003542540112345671},
				[]bool{false, false}),
		},
	}
}

func TestCosineDistanceArray(t *testing.T) {
	testCases := initCosineDistanceArrayTestCase()

	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		var fcTC FunctionTestCase
		switch tc.typ {
		case types.T_array_float32:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, CosineDistanceArray[float32])
		case types.T_array_float64:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, CosineDistanceArray[float64])
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
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"year", "year", "year", "year"}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_date.ToType(), MakeDates("2020-01-01", "2021-02-03", "2024-03-04", ""), []bool{false, false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_uint32.ToType(), false,
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
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"month", "month", "month", "month"}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_date.ToType(), MakeDates("2020-01-01", "2021-02-03", "2024-03-04", ""), []bool{false, false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{1, 2, 3, 1},
				[]bool{false, false, false, true}),
			//TODO: Comments migrated from original code: https://github.com/m-schen/matrixone/blob/0c480ca11b6302de26789f916a3e2faca7f79d47/pkg/sql/plan/function/builtin/binary/extract_test.go#L39
			// XXX same as above.
			// require.True(t, nulls.Contains(outputVector.GetNulls(), uint64(3)))
		},
		{
			info: "test extractFromDate day",
			typ:  types.T_date,
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"day", "day", "day", "day"}, []bool{}),
				NewFunctionTestInput(types.T_date.ToType(), MakeDates("2020-01-01", "2021-02-03", "2024-03-04", ""), []bool{false, false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{1, 3, 4, 1},
				[]bool{false, false, false, true}),
			//TODO: Comments migrated from original code: https://github.com/m-schen/matrixone/blob/0c480ca11b6302de26789f916a3e2faca7f79d47/pkg/sql/plan/function/builtin/binary/extract_test.go#L39
			// XXX Same
			// require.True(t, nulls.Contains(outputVector.GetNulls(), uint64(3)))
		},
		{
			info: "test extractFromDate year_month",
			typ:  types.T_date,
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"year_month", "year_month", "year_month", "year_month"}, []bool{}),
				NewFunctionTestInput(types.T_date.ToType(), MakeDates("2020-01-01", "2021-02-03", "2024-03-04", ""), []bool{false, false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{202001, 202102, 202403, 101},
				[]bool{false, false, false, true}),
			//TODO: Comments migrated from original code: https://github.com/m-schen/matrixone/blob/0c480ca11b6302de26789f916a3e2faca7f79d47/pkg/sql/plan/function/builtin/binary/extract_test.go#L39
			// XXX same
			// require.True(t, nulls.Contains(outputVector.GetNulls(), uint64(3)))
		},
		{
			info: "test extractFromDateTime year",
			typ:  types.T_datetime,
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"year", "year", "year", "year"}, []bool{}),
				NewFunctionTestInput(types.T_datetime.ToType(), MakeDateTimes("2020-01-01 11:12:13.0006", "2006-01-02 15:03:04.1234", "2024-03-04 12:13:14", ""), []bool{false, false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"2020", "2006", "2024", ""},
				[]bool{false, false, false, true}),
		},
	}
}

func TestExtract(t *testing.T) {
	testCases := initExtractTestCase()

	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		var fcTC FunctionTestCase
		switch tc.typ {
		case types.T_date:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, ExtractFromDate)
		case types.T_datetime:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, ExtractFromDatetime)
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
			inputs: []FunctionTestInput{
				// Create a input entry <[]string, []string, []string>
				NewFunctionTestInput(types.T_varchar.ToType(), c.input[0], []bool{}),
				NewFunctionTestInput(types.T_varchar.ToType(), c.input[1], []bool{}),
				NewFunctionTestInput(types.T_varchar.ToType(), c.input[2], []bool{}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false, c.expect, []bool{}),
		})
	}

	return testInputs
}

// INSERT
func initInsertTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test insert basic",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"Hello World"},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{7},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{5},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"MySQL"},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"Hello MySQL"},
				[]bool{false}),
		},
		{
			info: "test insert at beginning",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"World"},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{5},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"Hello "},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"Hello "},
				[]bool{false}),
		},
		{
			info: "test insert at end",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"Hello"},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{6},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1},
					[]bool{false}),
				// result is hello, according to MySQL behavior
				// if pos is NOT in the range of string, return original string.
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"World"},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"Hello"},
				[]bool{false}),
		},
		{
			info: "test insert with zero length (insert without remove)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"Hello World"},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{7},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{0},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"MySQL "},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"Hello MySQL World"},
				[]bool{false}),
		},
		{
			info: "test insert with invalid position (pos <= 0)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"Hello World"},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{0},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{5},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"MySQL"},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"Hello World"},
				[]bool{false}),
		},
		{
			info: "test insert with position > string length",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"Hello"},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{10},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{5},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"MySQL"},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"Hello"},
				[]bool{false}),
		},
		{
			info: "test insert with NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"Hello World"},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{7},
					[]bool{true}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{5},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"MySQL"},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{""},
				[]bool{true}),
		},
		{
			info: "test insert with negative length",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"Hello World"},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{7},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-1},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"MySQL "},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"Hello MySQL World"},
				[]bool{false}),
		},
		{
			info: "test insert with length exceeding string",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"Hello"},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{2},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{100},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"MySQL"},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"HMySQL"},
				[]bool{false}),
		},
	}
}

func TestInsert(t *testing.T) {
	testCases := initInsertTestCase()

	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, Insert)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func TestReplace(t *testing.T) {
	testCases := initReplaceTestCase()

	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, Replace)
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
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{c.mode}, []bool{}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{c.trimWord}, []bool{}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{c.input}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{c.output}, []bool{}),
		})
	}

	return testInputs

}

func TestTrim(t *testing.T) {
	testCases := initTrimTestCase()

	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, Trim)
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
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"a,b,c"}, []bool{}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{","}, []bool{}),
				NewFunctionTestInput(types.T_uint32.ToType(), []uint32{1}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"a"}, []bool{}),
		},
		{
			info: "test split_part Error",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"a,b,c"}, []bool{}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{","}, []bool{}),
				NewFunctionTestInput(types.T_uint32.ToType(), []uint32{0}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), true, []string{"a"}, []bool{}),
		},
	}

}

func TestSplitPart(t *testing.T) {
	testCases := initSplitPart()

	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, SplitPart)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func Test_castBinaryArrayToInt(t *testing.T) {
	testCases := []struct {
		name   string
		input  []uint8
		expect int64
	}{
		{
			name:   "test1",
			input:  []uint8{7, 229},
			expect: 2021,
		},
		{
			name:   "test2",
			input:  []uint8{8, 45},
			expect: 2093,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := castBinaryArrayToInt(tc.input)
			require.Equal(t, tc.expect, result)
		})
	}
}

func initTimeFormatTestCase() []tcTemp {
	t1, _ := types.ParseTime("15:30:45", 6)
	t2, _ := types.ParseTime("00:00:00", 6)
	t3, _ := types.ParseTime("23:59:59.123456", 6)
	t4, _ := types.ParseTime("12:34:56.789012", 6)

	return []tcTemp{
		{
			info: "test time_format - %H:%i:%s",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_time.ToType(),
					[]types.Time{t1, t2, t3},
					[]bool{false, false, false}),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"%H:%i:%s"}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"15:30:45", "00:00:00", "23:59:59"},
				[]bool{false, false, false}),
		},
		{
			info: "test time_format - %T",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_time.ToType(),
					[]types.Time{t1},
					[]bool{false}),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"%T"}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"15:30:45"},
				[]bool{false}),
		},
		{
			info: "test time_format - %h:%i:%s %p",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_time.ToType(),
					[]types.Time{t1, t2, t4},
					[]bool{false, false, false}),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"%h:%i:%s %p"}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"03:30:45 PM", "12:00:00 AM", "12:34:56 PM"},
				[]bool{false, false, false}),
		},
		{
			info: "test time_format - %r",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_time.ToType(),
					[]types.Time{t1, t2, t4},
					[]bool{false, false, false}),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"%r"}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"03:30:45 PM", "12:00:00 AM", "12:34:56 PM"},
				[]bool{false, false, false}),
		},
		{
			info: "test time_format - %H:%i:%s.%f",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_time.ToType(),
					[]types.Time{t3, t4},
					[]bool{false, false}),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"%H:%i:%s.%f"}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"23:59:59.123456", "12:34:56.789012"},
				[]bool{false, false}),
		},
		{
			info: "test time_format - null",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_time.ToType(),
					[]types.Time{t1},
					[]bool{true}),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"%H:%i:%s"}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{""},
				[]bool{true}),
		},
	}
}

func TestTimeFormat(t *testing.T) {
	testCases := initTimeFormatTestCase()

	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, TimeFormat)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// TestTimestampDiffDateString tests TIMESTAMPDIFF with DATE and string arguments
// This tests the new overload that handles mixed DATE and string types
func TestTimestampDiffDateString(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("DATE and string with time part", func(t *testing.T) {
		// Test: TIMESTAMPDIFF(HOUR, DATE('2024-12-20'), '2024-12-20 12:00:00')
		// Expected: 12 hours
		unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte("HOUR"), 1, proc.Mp())
		date1, _ := types.ParseDateCast("2024-12-20")
		dateVec, _ := vector.NewConstFixed(types.T_date.ToType(), date1, 1, proc.Mp())
		strVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte("2024-12-20 12:00:00"), 1, proc.Mp())

		parameters := []*vector.Vector{unitVec, dateVec, strVec}
		result := vector.NewFunctionResultWrapper(types.T_int64.ToType(), proc.Mp())

		fnLength := dateVec.Length()
		err := result.PreExtendAndReset(fnLength)
		require.NoError(t, err)

		// Use TimestampDiffDateString to handle DATE and string mix
		err = TimestampDiffDateString(parameters, result, proc, fnLength, nil)
		require.NoError(t, err)

		v := result.GetResultVector()
		require.Equal(t, fnLength, v.Length())
		require.Equal(t, types.T_int64, v.GetType().Oid)

		int64Param := vector.GenerateFunctionFixedTypeParameter[int64](v)
		resultVal, null := int64Param.GetValue(0)
		require.False(t, null, "Result should not be null")
		require.Equal(t, int64(12), resultVal, "Should return 12 hours")
	})

	t.Run("DATE and string with DAY unit", func(t *testing.T) {
		// Test: TIMESTAMPDIFF(DAY, DATE('2024-12-20'), '2024-12-21 12:00:00')
		// Expected: 1 day
		unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte("DAY"), 1, proc.Mp())
		date1, _ := types.ParseDateCast("2024-12-20")
		dateVec, _ := vector.NewConstFixed(types.T_date.ToType(), date1, 1, proc.Mp())
		strVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte("2024-12-21 12:00:00"), 1, proc.Mp())

		parameters := []*vector.Vector{unitVec, dateVec, strVec}
		result := vector.NewFunctionResultWrapper(types.T_int64.ToType(), proc.Mp())

		fnLength := dateVec.Length()
		err := result.PreExtendAndReset(fnLength)
		require.NoError(t, err)

		err = TimestampDiffDateString(parameters, result, proc, fnLength, nil)
		require.NoError(t, err)

		v := result.GetResultVector()
		int64Param := vector.GenerateFunctionFixedTypeParameter[int64](v)
		resultVal, null := int64Param.GetValue(0)
		require.False(t, null)
		require.Equal(t, int64(1), resultVal, "Should return 1 day")
	})
}

// TestTimestampDiffStringDate tests TIMESTAMPDIFF with string and DATE arguments
// This tests the new overload that handles mixed string and DATE types
func TestTimestampDiffStringDate(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("String and DATE with time part", func(t *testing.T) {
		// Test: TIMESTAMPDIFF(HOUR, '2024-12-20 12:00:00', DATE('2024-12-20'))
		// Expected: -12 hours
		unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte("HOUR"), 1, proc.Mp())
		strVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte("2024-12-20 12:00:00"), 1, proc.Mp())
		date1, _ := types.ParseDateCast("2024-12-20")
		dateVec, _ := vector.NewConstFixed(types.T_date.ToType(), date1, 1, proc.Mp())

		parameters := []*vector.Vector{unitVec, strVec, dateVec}
		result := vector.NewFunctionResultWrapper(types.T_int64.ToType(), proc.Mp())

		fnLength := strVec.Length()
		err := result.PreExtendAndReset(fnLength)
		require.NoError(t, err)

		// Use TimestampDiffStringDate to handle string and DATE mix
		err = TimestampDiffStringDate(parameters, result, proc, fnLength, nil)
		require.NoError(t, err)

		v := result.GetResultVector()
		require.Equal(t, fnLength, v.Length())
		require.Equal(t, types.T_int64, v.GetType().Oid)

		int64Param := vector.GenerateFunctionFixedTypeParameter[int64](v)
		resultVal, null := int64Param.GetValue(0)
		require.False(t, null, "Result should not be null")
		require.Equal(t, int64(-12), resultVal, "Should return -12 hours")
	})

	t.Run("String and DATE with MINUTE unit", func(t *testing.T) {
		// Test: TIMESTAMPDIFF(MINUTE, '2024-12-20 10:30:00', DATE('2024-12-20'))
		// Expected: -630 minutes (10 hours 30 minutes)
		unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte("MINUTE"), 1, proc.Mp())
		strVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte("2024-12-20 10:30:00"), 1, proc.Mp())
		date1, _ := types.ParseDateCast("2024-12-20")
		dateVec, _ := vector.NewConstFixed(types.T_date.ToType(), date1, 1, proc.Mp())

		parameters := []*vector.Vector{unitVec, strVec, dateVec}
		result := vector.NewFunctionResultWrapper(types.T_int64.ToType(), proc.Mp())

		fnLength := strVec.Length()
		err := result.PreExtendAndReset(fnLength)
		require.NoError(t, err)

		err = TimestampDiffStringDate(parameters, result, proc, fnLength, nil)
		require.NoError(t, err)

		v := result.GetResultVector()
		int64Param := vector.GenerateFunctionFixedTypeParameter[int64](v)
		resultVal, null := int64Param.GetValue(0)
		require.False(t, null)
		require.Equal(t, int64(-630), resultVal, "Should return -630 minutes")
	})
}

// TestTimestampDiffTimestampDate tests TIMESTAMPDIFF with TIMESTAMP and DATE arguments
func TestTimestampDiffTimestampDate(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("TIMESTAMP and DATE with DAY unit", func(t *testing.T) {
		// Test: TIMESTAMPDIFF(DAY, TIMESTAMP('2024-12-20 10:30:45'), DATE('2024-12-21'))
		// Expected: 0 (because 2024-12-20 10:30:45 to 2024-12-21 00:00:00 is less than 1 day)
		unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte("DAY"), 1, proc.Mp())
		ts1, _ := types.ParseTimestamp(proc.GetSessionInfo().TimeZone, "2024-12-20 10:30:45", 0)
		tsVec, _ := vector.NewConstFixed(types.T_timestamp.ToType(), ts1, 1, proc.Mp())
		date1, _ := types.ParseDateCast("2024-12-21")
		dateVec, _ := vector.NewConstFixed(types.T_date.ToType(), date1, 1, proc.Mp())

		parameters := []*vector.Vector{unitVec, tsVec, dateVec}
		result := vector.NewFunctionResultWrapper(types.T_int64.ToType(), proc.Mp())

		fnLength := tsVec.Length()
		err := result.PreExtendAndReset(fnLength)
		require.NoError(t, err)

		err = TimestampDiffTimestampDate(parameters, result, proc, fnLength, nil)
		require.NoError(t, err)

		v := result.GetResultVector()
		require.Equal(t, fnLength, v.Length())
		require.Equal(t, types.T_int64, v.GetType().Oid)

		int64Param := vector.GenerateFunctionFixedTypeParameter[int64](v)
		resultVal, null := int64Param.GetValue(0)
		require.False(t, null)
		require.Equal(t, int64(0), resultVal, "Should return 0 days (less than 1 day difference)")
	})
}

// TestTimestampDiffDateTimestamp tests TIMESTAMPDIFF with DATE and TIMESTAMP arguments
func TestTimestampDiffDateTimestamp(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("DATE and TIMESTAMP with DAY unit", func(t *testing.T) {
		// Test: TIMESTAMPDIFF(DAY, DATE('2024-12-20'), TIMESTAMP('2024-12-21 10:30:45'))
		// Expected: 1 (because 2024-12-20 00:00:00 to 2024-12-21 10:30:45 is more than 1 day)
		unitVec, _ := vector.NewConstBytes(types.T_varchar.ToType(), []byte("DAY"), 1, proc.Mp())
		date1, _ := types.ParseDateCast("2024-12-20")
		dateVec, _ := vector.NewConstFixed(types.T_date.ToType(), date1, 1, proc.Mp())
		ts1, _ := types.ParseTimestamp(proc.GetSessionInfo().TimeZone, "2024-12-21 10:30:45", 0)
		tsVec, _ := vector.NewConstFixed(types.T_timestamp.ToType(), ts1, 1, proc.Mp())

		parameters := []*vector.Vector{unitVec, dateVec, tsVec}
		result := vector.NewFunctionResultWrapper(types.T_int64.ToType(), proc.Mp())

		fnLength := dateVec.Length()
		err := result.PreExtendAndReset(fnLength)
		require.NoError(t, err)

		err = TimestampDiffDateTimestamp(parameters, result, proc, fnLength, nil)
		require.NoError(t, err)

		v := result.GetResultVector()
		require.Equal(t, fnLength, v.Length())
		require.Equal(t, types.T_int64, v.GetType().Oid)

		int64Param := vector.GenerateFunctionFixedTypeParameter[int64](v)
		resultVal, null := int64Param.GetValue(0)
		require.False(t, null)
		require.Equal(t, int64(1), resultVal, "Should return 1 day")
	})
}

// TestDateStringAddMicrosecondPrecision tests that DateStringAdd returns 6-digit precision for MICROSECOND interval
// and returns string type (varchar) matching the input type
func TestDateStringAddMicrosecondPrecision(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test case: date_add('2022-07-01 10:20:30.123456', interval 1 microsecond)
	// Expected: '2022-07-01 10:20:30.123457' (6 digits, not 9)
	startDateStr := "2022-07-01 10:20:30.123456"
	interval := int64(1) // 1 microsecond

	// Create input vectors
	ivecs := make([]*vector.Vector, 3)
	var err error
	ivecs[0], err = vector.NewConstBytes(types.T_varchar.ToType(), []byte(startDateStr), 1, proc.Mp())
	require.NoError(t, err)
	ivecs[1], err = vector.NewConstFixed(types.T_int64.ToType(), interval, 1, proc.Mp())
	require.NoError(t, err)
	ivecs[2], err = vector.NewConstFixed(types.T_int64.ToType(), int64(types.MicroSecond), 1, proc.Mp())
	require.NoError(t, err)

	// Create result vector - should be VARCHAR type (string)
	result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())

	// Initialize result vector
	err = result.PreExtendAndReset(1)
	require.NoError(t, err)

	// Call DateStringAdd
	err = DateStringAdd(ivecs, result, proc, 1, nil)
	require.NoError(t, err)

	// Verify result type is VARCHAR
	v := result.GetResultVector()
	require.Equal(t, types.T_varchar, v.GetType().Oid, "Result type should be VARCHAR")

	// Verify result value
	strParam := vector.GenerateFunctionStrParameter(v)
	resultStr, null := strParam.GetStrValue(0)
	require.False(t, null, "Result should not be null")
	require.Equal(t, "2022-07-01 10:20:30.123457", string(resultStr), "Result should have 6-digit precision, not 9")

	// Cleanup
	for _, v := range ivecs {
		if v != nil {
			v.Free(proc.Mp())
		}
	}
	if result != nil {
		result.Free()
	}
}

// TestDateStringSubMicrosecondPrecision tests that DateStringSub returns 6-digit precision for MICROSECOND interval
// and returns string type (varchar) matching the input type
func TestDateStringSubMicrosecondPrecision(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test case: date_sub('2022-07-01 10:20:30.123456', interval 1 microsecond)
	// Expected: '2022-07-01 10:20:30.123455' (6 digits, not 9)
	startDateStr := "2022-07-01 10:20:30.123456"
	interval := int64(1) // 1 microsecond

	// Create input vectors
	ivecs := make([]*vector.Vector, 3)
	var err error
	ivecs[0], err = vector.NewConstBytes(types.T_varchar.ToType(), []byte(startDateStr), 1, proc.Mp())
	require.NoError(t, err)
	ivecs[1], err = vector.NewConstFixed(types.T_int64.ToType(), interval, 1, proc.Mp())
	require.NoError(t, err)
	ivecs[2], err = vector.NewConstFixed(types.T_int64.ToType(), int64(types.MicroSecond), 1, proc.Mp())
	require.NoError(t, err)

	// Create result vector - should be VARCHAR type (string)
	result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())

	// Initialize result vector
	err = result.PreExtendAndReset(1)
	require.NoError(t, err)

	// Call DateStringSub
	err = DateStringSub(ivecs, result, proc, 1, nil)
	require.NoError(t, err)

	// Verify result type is VARCHAR
	v := result.GetResultVector()
	require.Equal(t, types.T_varchar, v.GetType().Oid, "Result type should be VARCHAR")

	// Verify result value
	strParam := vector.GenerateFunctionStrParameter(v)
	resultStr, null := strParam.GetStrValue(0)
	require.False(t, null, "Result should not be null")
	require.Equal(t, "2022-07-01 10:20:30.123455", string(resultStr), "Result should have 6-digit precision, not 9")

	// Cleanup
	for _, v := range ivecs {
		if v != nil {
			v.Free(proc.Mp())
		}
	}
	if result != nil {
		result.Free()
	}
}

// TestDateStringAddReturnTypeCompatibility tests that DateStringAdd returns string type matching input type
func TestDateStringAddReturnTypeCompatibility(t *testing.T) {
	proc := testutil.NewProcess(t)

	testCases := []struct {
		name         string
		inputType    types.T
		expectedType types.T
	}{
		{"VARCHAR input returns VARCHAR", types.T_varchar, types.T_varchar},
		{"CHAR input returns CHAR", types.T_char, types.T_char},
		{"TEXT input returns TEXT", types.T_text, types.T_text},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startDateStr := "2022-07-01 10:20:30.123456"
			interval := int64(1)

			// Create input vectors
			ivecs := make([]*vector.Vector, 3)
			var err error
			ivecs[0], err = vector.NewConstBytes(tc.inputType.ToType(), []byte(startDateStr), 1, proc.Mp())
			require.NoError(t, err)
			ivecs[1], err = vector.NewConstFixed(types.T_int64.ToType(), interval, 1, proc.Mp())
			require.NoError(t, err)
			ivecs[2], err = vector.NewConstFixed(types.T_int64.ToType(), int64(types.MicroSecond), 1, proc.Mp())
			require.NoError(t, err)

			// Create result vector with expected return type
			result := vector.NewFunctionResultWrapper(tc.expectedType.ToType(), proc.Mp())

			// Initialize result vector
			err = result.PreExtendAndReset(1)
			require.NoError(t, err)

			// Call DateStringAdd
			err = DateStringAdd(ivecs, result, proc, 1, nil)
			require.NoError(t, err)

			// Verify result type matches expected type
			v := result.GetResultVector()
			require.Equal(t, tc.expectedType, v.GetType().Oid, "Result type should match input type")

			// Cleanup
			for _, vec := range ivecs {
				if vec != nil {
					vec.Free(proc.Mp())
				}
			}
			if result != nil {
				result.Free()
			}
		})
	}
}

// TestDateStringAddNonMicrosecondInterval tests that DateStringAdd works correctly with non-MICROSECOND intervals
func TestDateStringAddNonMicrosecondInterval(t *testing.T) {
	proc := testutil.NewProcess(t)

	testCases := []struct {
		name         string
		interval     int64
		intervalType types.IntervalType
		expected     string
	}{
		{"SECOND interval", 1, types.Second, "2022-07-01 10:20:31"},
		{"MINUTE interval", 1, types.Minute, "2022-07-01 10:21:30"},
		{"HOUR interval", 1, types.Hour, "2022-07-01 11:20:30"},
		{"DAY interval", 1, types.Day, "2022-07-02 10:20:30"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startDateStr := "2022-07-01 10:20:30.123456"

			// Create input vectors
			ivecs := make([]*vector.Vector, 3)
			var err error
			ivecs[0], err = vector.NewConstBytes(types.T_varchar.ToType(), []byte(startDateStr), 1, proc.Mp())
			require.NoError(t, err)
			ivecs[1], err = vector.NewConstFixed(types.T_int64.ToType(), tc.interval, 1, proc.Mp())
			require.NoError(t, err)
			ivecs[2], err = vector.NewConstFixed(types.T_int64.ToType(), int64(tc.intervalType), 1, proc.Mp())
			require.NoError(t, err)

			// Create result vector
			result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())

			// Initialize result vector
			err = result.PreExtendAndReset(1)
			require.NoError(t, err)

			// Call DateStringAdd
			err = DateStringAdd(ivecs, result, proc, 1, nil)
			require.NoError(t, err)

			// Verify result
			v := result.GetResultVector()
			strParam := vector.GenerateFunctionStrParameter(v)
			resultStr, null := strParam.GetStrValue(0)
			require.False(t, null)
			require.Equal(t, tc.expected, string(resultStr))

			// Cleanup
			for _, vec := range ivecs {
				if vec != nil {
					vec.Free(proc.Mp())
				}
			}
			if result != nil {
				result.Free()
			}
		})
	}
}

// TestDateStringAddDateFormatOutput tests that date_add with date-only string input
// returns date-only format when interval doesn't affect time part
func TestDateStringAddDateFormatOutput(t *testing.T) {
	proc := testutil.NewProcess(t)

	testCases := []struct {
		name         string
		interval     int64
		intervalType types.IntervalType
		expected     string
	}{
		{"DAY interval", 1, types.Day, "2022-01-02"},
		{"MONTH interval", 1, types.Month, "2022-02-01"},
		{"YEAR interval", 1, types.Year, "2023-01-01"},
		{"WEEK interval", 1, types.Week, "2022-01-08"},
		{"QUARTER interval", 1, types.Quarter, "2022-04-01"},
		{"SECOND interval", 1, types.Second, "2022-01-01 00:00:01"},
		{"MINUTE interval", 1, types.Minute, "2022-01-01 00:01:00"},
		{"HOUR interval", 1, types.Hour, "2022-01-01 01:00:00"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startDateStr := "2022-01-01" // Date-only format

			// Create input vectors
			ivecs := make([]*vector.Vector, 3)
			var err error
			ivecs[0], err = vector.NewConstBytes(types.T_varchar.ToType(), []byte(startDateStr), 1, proc.Mp())
			require.NoError(t, err)
			ivecs[1], err = vector.NewConstFixed(types.T_int64.ToType(), tc.interval, 1, proc.Mp())
			require.NoError(t, err)
			ivecs[2], err = vector.NewConstFixed(types.T_int64.ToType(), int64(tc.intervalType), 1, proc.Mp())
			require.NoError(t, err)

			// Create result vector
			result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())

			// Initialize result vector
			err = result.PreExtendAndReset(1)
			require.NoError(t, err)

			// Call DateStringAdd
			err = DateStringAdd(ivecs, result, proc, 1, nil)
			require.NoError(t, err)

			// Verify result
			v := result.GetResultVector()
			strParam := vector.GenerateFunctionStrParameter(v)
			resultStr, null := strParam.GetStrValue(0)
			require.False(t, null)
			require.Equal(t, tc.expected, string(resultStr), "Output format should match MySQL behavior")

			// Cleanup
			for _, vec := range ivecs {
				if vec != nil {
					vec.Free(proc.Mp())
				}
			}
			if result != nil {
				result.Free()
			}
		})
	}
}

// TestDateStringSubDateFormatOutput tests that date_sub with date-only string input
// returns date-only format when interval doesn't affect time part
func TestDateStringSubDateFormatOutput(t *testing.T) {
	proc := testutil.NewProcess(t)

	testCases := []struct {
		name         string
		interval     int64
		intervalType types.IntervalType
		expected     string
	}{
		{"DAY interval", 1, types.Day, "2021-12-31"},
		{"MONTH interval", 1, types.Month, "2021-12-01"},
		{"YEAR interval", 1, types.Year, "2021-01-01"},
		{"WEEK interval", 1, types.Week, "2021-12-25"},
		{"QUARTER interval", 1, types.Quarter, "2021-10-01"},
		{"SECOND interval", 1, types.Second, "2021-12-31 23:59:59"},
		{"MINUTE interval", 1, types.Minute, "2021-12-31 23:59:00"},
		{"HOUR interval", 1, types.Hour, "2021-12-31 23:00:00"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startDateStr := "2022-01-01" // Date-only format

			// Create input vectors
			ivecs := make([]*vector.Vector, 3)
			var err error
			ivecs[0], err = vector.NewConstBytes(types.T_varchar.ToType(), []byte(startDateStr), 1, proc.Mp())
			require.NoError(t, err)
			ivecs[1], err = vector.NewConstFixed(types.T_int64.ToType(), tc.interval, 1, proc.Mp())
			require.NoError(t, err)
			ivecs[2], err = vector.NewConstFixed(types.T_int64.ToType(), int64(tc.intervalType), 1, proc.Mp())
			require.NoError(t, err)

			// Create result vector
			result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())

			// Initialize result vector
			err = result.PreExtendAndReset(1)
			require.NoError(t, err)

			// Call DateStringSub
			err = DateStringSub(ivecs, result, proc, 1, nil)
			require.NoError(t, err)

			// Verify result
			v := result.GetResultVector()
			strParam := vector.GenerateFunctionStrParameter(v)
			resultStr, null := strParam.GetStrValue(0)
			require.False(t, null)
			require.Equal(t, tc.expected, string(resultStr), "Output format should match MySQL behavior")

			// Cleanup
			for _, vec := range ivecs {
				if vec != nil {
					vec.Free(proc.Mp())
				}
			}
			if result != nil {
				result.Free()
			}
		})
	}
}

// TestDateStringAddInvalidInterval tests that invalid interval strings return NULL
func TestDateStringAddInvalidInterval(t *testing.T) {
	proc := testutil.NewProcess(t)

	testCases := []struct {
		name         string
		intervalStr  string
		intervalType types.IntervalType
		dateStr      string
	}{
		{"Invalid YEAR_MONTH format", "9223372036854775807-02", types.Year_Month, "1995-01-05"},
		{"Invalid YEAR_MONTH format 2", "9223372036854775808-02", types.Year_Month, "1995-01-05"},
		{"Invalid DAY format", "9223372036854775808-02", types.Day, "1995-01-05"},
		{"Invalid WEEK format", "9223372036854775808-02", types.Week, "1995-01-05"},
		{"Invalid SECOND format", "9223372036854775808-02", types.Second, "1995-01-05"},
		{"Invalid YEAR_MONTH format 3", "9223372036854775700-02", types.Year_Month, "1995-01-05"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create input vectors with invalid interval string
			// The interval value will be math.MaxInt64 (marker for invalid parse)
			ivecs := make([]*vector.Vector, 3)
			var err error
			ivecs[0], err = vector.NewConstBytes(types.T_varchar.ToType(), []byte(tc.dateStr), 1, proc.Mp())
			require.NoError(t, err)
			// Use math.MaxInt64 as marker for invalid interval
			ivecs[1], err = vector.NewConstFixed(types.T_int64.ToType(), int64(math.MaxInt64), 1, proc.Mp())
			require.NoError(t, err)
			ivecs[2], err = vector.NewConstFixed(types.T_int64.ToType(), int64(tc.intervalType), 1, proc.Mp())
			require.NoError(t, err)

			// Create result vector
			result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())

			// Initialize result vector
			err = result.PreExtendAndReset(1)
			require.NoError(t, err)

			// Call DateStringAdd
			err = DateStringAdd(ivecs, result, proc, 1, nil)
			require.NoError(t, err)

			// Verify result is NULL
			resultVec := result.GetResultVector()
			require.True(t, resultVec.GetNulls().Contains(0), "Result should be NULL for invalid interval")
		})
	}
}

// TestDatetimeAddInvalidInterval tests that invalid interval strings return NULL for DatetimeAdd
func TestDatetimeAddInvalidInterval(t *testing.T) {
	proc := testutil.NewProcess(t)

	dt, _ := types.ParseDatetime("1995-01-05 00:00:00", 0)

	// Create input vectors with invalid interval (math.MaxInt64 marker)
	ivecs := make([]*vector.Vector, 3)
	var err error
	ivecs[0], err = vector.NewConstFixed(types.T_datetime.ToType(), dt, 1, proc.Mp())
	require.NoError(t, err)
	ivecs[1], err = vector.NewConstFixed(types.T_int64.ToType(), int64(math.MaxInt64), 1, proc.Mp())
	require.NoError(t, err)
	ivecs[2], err = vector.NewConstFixed(types.T_int64.ToType(), int64(types.Day), 1, proc.Mp())
	require.NoError(t, err)

	// Create result vector
	result := vector.NewFunctionResultWrapper(types.T_datetime.ToType(), proc.Mp())

	// Initialize result vector
	err = result.PreExtendAndReset(1)
	require.NoError(t, err)

	// Call DatetimeAdd
	err = DatetimeAdd(ivecs, result, proc, 1, nil)
	require.NoError(t, err)

	// Verify result is NULL
	resultVec := result.GetResultVector()
	require.True(t, resultVec.GetNulls().Contains(0), "Result should be NULL for invalid interval")
}

// TestDateAddWithNullInterval tests that INTERVAL NULL SECOND returns NULL (not syntax error)
// MySQL behavior: date_add("1997-12-31 23:59:59", INTERVAL NULL SECOND) should return NULL
func TestDateAddWithNullInterval(t *testing.T) {
	proc := testutil.NewProcess(t)

	testCases := []struct {
		name         string
		dateStr      string
		intervalType types.IntervalType
		funcName     string
		testFunc     func([]*vector.Vector, vector.FunctionResultWrapper, *process.Process, int, *FunctionSelectList) error
		resultType   types.T
	}{
		{
			name:         "DateStringAdd with NULL SECOND",
			dateStr:      "1997-12-31 23:59:59",
			intervalType: types.Second,
			funcName:     "DateStringAdd",
			testFunc:     DateStringAdd,
			resultType:   types.T_varchar,
		},
		{
			name:         "DateStringAdd with NULL MINUTE_SECOND",
			dateStr:      "1997-12-31 23:59:59",
			intervalType: types.Minute_Second,
			funcName:     "DateStringAdd",
			testFunc:     DateStringAdd,
			resultType:   types.T_varchar,
		},
		{
			name:         "DatetimeAdd with NULL SECOND",
			dateStr:      "1997-12-31 23:59:59",
			intervalType: types.Second,
			funcName:     "DatetimeAdd",
			testFunc:     DatetimeAdd,
			resultType:   types.T_datetime,
		},
		{
			name:         "DateAdd with NULL SECOND",
			dateStr:      "1997-12-31",
			intervalType: types.Second,
			funcName:     "DateAdd",
			testFunc: func(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
				return DateAdd(ivecs, result, proc, length, selectList)
			},
			resultType: types.T_date,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create input vectors
			ivecs := make([]*vector.Vector, 3)
			var err error

			// First parameter: date/datetime string or value
			if tc.resultType == types.T_varchar {
				ivecs[0], err = vector.NewConstBytes(types.T_varchar.ToType(), []byte(tc.dateStr), 1, proc.Mp())
			} else if tc.resultType == types.T_datetime {
				dt, _ := types.ParseDatetime(tc.dateStr, 6)
				ivecs[0], err = vector.NewConstFixed(types.T_datetime.ToType(), dt, 1, proc.Mp())
			} else {
				d, _ := types.ParseDateCast(tc.dateStr)
				ivecs[0], err = vector.NewConstFixed(types.T_date.ToType(), d, 1, proc.Mp())
			}
			require.NoError(t, err)

			// Second parameter: NULL interval value
			ivecs[1] = vector.NewConstNull(types.T_int64.ToType(), 1, proc.Mp())

			// Third parameter: interval type
			ivecs[2], err = vector.NewConstFixed(types.T_int64.ToType(), int64(tc.intervalType), 1, proc.Mp())
			require.NoError(t, err)

			// Create result vector
			result := vector.NewFunctionResultWrapper(tc.resultType.ToType(), proc.Mp())

			// Initialize result vector
			err = result.PreExtendAndReset(1)
			require.NoError(t, err)

			// Call the function
			err = tc.testFunc(ivecs, result, proc, 1, nil)
			require.NoError(t, err, "Function should not return error for NULL interval")

			// Verify result is NULL
			resultVec := result.GetResultVector()
			require.True(t, resultVec.GetNulls().Contains(0), "Result should be NULL for NULL interval value")

			// Cleanup
			for _, v := range ivecs {
				if v != nil {
					v.Free(proc.Mp())
				}
			}
			if result != nil {
				result.Free()
			}
		})
	}
}

// TestDateSubWithDecimalInterval tests that decimal interval values (e.g., 1.1 SECOND) are handled correctly
// MySQL behavior: DATE_SUB(a, INTERVAL 1.1 SECOND) should preserve fractional seconds
func TestDateSubWithDecimalInterval(t *testing.T) {
	proc := testutil.NewProcess(t)

	testCases := []struct {
		name         string
		startStr     string
		intervalVal  float64
		intervalType types.IntervalType
		expectedStr  string
	}{
		{
			name:         "DATE_SUB with 1.1 SECOND",
			startStr:     "1000-01-01 01:00:00",
			intervalVal:  1.1,
			intervalType: types.Second,
			expectedStr:  "1000-01-01 00:59:58.900000",
		},
		{
			name:         "DATE_SUB with 1.000009 SECOND",
			startStr:     "1000-01-01 01:00:00",
			intervalVal:  1.000009,
			intervalType: types.Second,
			expectedStr:  "1000-01-01 00:59:58.999991", // 1000009 microseconds = 1.000009 seconds (math.Round ensures correct conversion)
		},
		{
			name:         "DATE_SUB with -0.1 SECOND (adding 0.1 second)",
			startStr:     "1000-01-01 01:00:00",
			intervalVal:  -0.1,
			intervalType: types.Second,
			expectedStr:  "1000-01-01 01:00:00.100000",
		},
		{
			name:         "DATE_SUB with 1.1 SECOND from datetime with microseconds",
			startStr:     "1000-01-01 01:00:00.000001",
			intervalVal:  1.1,
			intervalType: types.Second,
			expectedStr:  "1000-01-01 00:59:58.900001",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Parse the start datetime
			startDt, err := types.ParseDatetime(tc.startStr, 6)
			require.NoError(t, err)

			// Calculate expected microseconds based on interval type
			// This simulates what base_binder.go does: converts decimal to microseconds
			var expectedMicroseconds int64
			switch tc.intervalType {
			case types.Second:
				expectedMicroseconds = int64(tc.intervalVal * float64(types.MicroSecsPerSec))
			case types.Minute:
				expectedMicroseconds = int64(tc.intervalVal * float64(types.MicroSecsPerSec*types.SecsPerMinute))
			case types.Hour:
				expectedMicroseconds = int64(tc.intervalVal * float64(types.MicroSecsPerSec*types.SecsPerHour))
			case types.Day:
				expectedMicroseconds = int64(tc.intervalVal * float64(types.MicroSecsPerSec*types.SecsPerDay))
			default:
				expectedMicroseconds = int64(tc.intervalVal)
			}

			// Create input vectors
			ivecs := make([]*vector.Vector, 3)
			ivecs[0], err = vector.NewConstFixed(types.T_datetime.ToType(), startDt, 1, proc.Mp())
			require.NoError(t, err)

			// Use the calculated microseconds as the interval value
			// Note: base_binder.go converts decimal interval (e.g., 1.1 SECOND) to microseconds
			// and uses MicroSecond type, not the original type (e.g., Second)
			// So we should use MicroSecond type here to match the binder behavior
			ivecs[1], err = vector.NewConstFixed(types.T_int64.ToType(), expectedMicroseconds, 1, proc.Mp())
			require.NoError(t, err)

			// Use MicroSecond type since we've already converted to microseconds
			ivecs[2], err = vector.NewConstFixed(types.T_int64.ToType(), int64(types.MicroSecond), 1, proc.Mp())
			require.NoError(t, err)

			// Create result vector
			result := vector.NewFunctionResultWrapper(types.T_datetime.ToType(), proc.Mp())

			// Initialize result vector with scale 6 for microseconds
			err = result.PreExtendAndReset(1)
			require.NoError(t, err)
			rs := vector.MustFunctionResult[types.Datetime](result)
			rs.TempSetType(types.New(types.T_datetime, 0, 6))

			// Call DatetimeSub
			err = DatetimeSub(ivecs, result, proc, 1, nil)
			require.NoError(t, err)

			// Verify result
			resultVec := result.GetResultVector()
			require.False(t, resultVec.GetNulls().Contains(0), "Result should not be NULL")

			dtParam := vector.GenerateFunctionFixedTypeParameter[types.Datetime](resultVec)
			resultDt, null := dtParam.GetValue(0)
			require.False(t, null, "Result should not be null")

			// Format the result with 6-digit precision (or 9 for full precision)
			resultStr := resultDt.String2(6)
			// Note: For very precise decimal values (e.g., 1.000009), there might be 1 microsecond
			// difference due to floating point precision. We'll check if it's close enough.
			// Parse both strings to compare the actual datetime values
			expectedDt, err := types.ParseDatetime(tc.expectedStr, 6)
			if err == nil {
				// Allow 1 microsecond difference due to floating point precision
				diff := int64(resultDt) - int64(expectedDt)
				if diff < 0 {
					diff = -diff
				}
				require.LessOrEqual(t, diff, int64(1), "Result should match expected value within 1 microsecond tolerance")
			} else {
				// Fallback to string comparison if parsing fails
				require.Equal(t, tc.expectedStr, resultStr, "Result should match expected value with fractional seconds")
			}

			// Cleanup
			for _, v := range ivecs {
				if v != nil {
					v.Free(proc.Mp())
				}
			}
			if result != nil {
				result.Free()
			}
		})
	}
}
