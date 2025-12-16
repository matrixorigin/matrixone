// Copyright 2021 - 2025 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func initLeastTestCase() []tcTemp {
	return []tcTemp{
		// Test int8
		{
			info: "test least int8",
			typ:  types.T_int8,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{1, 5, -3, 10, -5},
					[]bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{2, 3, -1, 5, -10},
					[]bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{3, 1, 0, 8, -2},
					[]bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{1, 1, -3, 5, -10},
				[]bool{false, false, false, false, false}),
		},
		// Test int64
		{
			info: "test least int64",
			typ:  types.T_int64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{100, -50, 0, 200, -100},
					[]bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{50, -30, -10, 150, -200},
					[]bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{50, -50, -10, 150, -200},
				[]bool{false, false, false, false, false}),
		},
		// Test uint64
		{
			info: "test least uint64",
			typ:  types.T_uint64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{100, 50, 200, 10, 5},
					[]bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{50, 30, 150, 20, 15},
					[]bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{50, 30, 150, 10, 5},
				[]bool{false, false, false, false, false}),
		},
		// Test float64
		{
			info: "test least float64",
			typ:  types.T_float64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1.5, -2.3, 0.0, 10.5, -5.7},
					[]bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{2.5, -1.3, 0.5, 8.5, -10.7},
					[]bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{1.5, -2.3, 0.0, 8.5, -10.7},
				[]bool{false, false, false, false, false}),
		},
		// Test bool
		{
			info: "test least bool",
			typ:  types.T_bool,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, true, false},
					[]bool{false, false, false, false}),
				NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{false, true, false, true},
					[]bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, false, false, false},
				[]bool{false, false, false, false}),
		},
		// Test varchar
		{
			info: "test least varchar",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"apple", "banana", "cherry", "date"},
					[]bool{false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"zebra", "ant", "dog", "cat"},
					[]bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"apple", "ant", "cherry", "cat"},
				[]bool{false, false, false, false}),
		},
		// Test date
		{
			info: "test least date",
			typ:  types.T_date,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{types.Date(1000), types.Date(2000), types.Date(1500)},
					[]bool{false, false, false}),
				NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{types.Date(1200), types.Date(1800), types.Date(1400)},
					[]bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_date.ToType(), false,
				[]types.Date{types.Date(1000), types.Date(1800), types.Date(1400)},
				[]bool{false, false, false}),
		},
		// Test with nulls
		{
			info: "test least with nulls",
			typ:  types.T_int64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{10, 20, 30},
					[]bool{false, true, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{5, 15, 25},
					[]bool{false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{5, 15, 25},
				[]bool{false, true, true}),
		},
		// Test single argument
		{
			info: "test least single argument",
			typ:  types.T_int64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{42, 100, -50},
					[]bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{42, 100, -50},
				[]bool{false, false, false}),
		},
		// Test three arguments
		{
			info: "test least three arguments",
			typ:  types.T_int64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{10, 20, 30},
					[]bool{false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{5, 15, 25},
					[]bool{false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{8, 12, 35},
					[]bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{5, 12, 25},
				[]bool{false, false, false}),
		},
	}
}

func TestLeast(t *testing.T) {
	testCases := initLeastTestCase()
	proc := testutil.NewProcess(t)

	for _, tc := range testCases {
		var fcTC FunctionTestCase
		switch tc.typ {
		case types.T_int8:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, leastFn)
		case types.T_int64:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, leastFn)
		case types.T_uint64:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, leastFn)
		case types.T_float64:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, leastFn)
		case types.T_bool:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, leastFn)
		case types.T_varchar:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, leastFn)
		case types.T_date:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, leastFn)
		default:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, leastFn)
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info '%s'", tc.info, info))
	}
}

func initGreatestTestCase() []tcTemp {
	return []tcTemp{
		// Test int8
		{
			info: "test greatest int8",
			typ:  types.T_int8,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{1, 5, -3, 10, -5},
					[]bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{2, 3, -1, 5, -10},
					[]bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{3, 1, 0, 8, -2},
					[]bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{3, 5, 0, 10, -2},
				[]bool{false, false, false, false, false}),
		},
		// Test int64
		{
			info: "test greatest int64",
			typ:  types.T_int64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{100, -50, 0, 200, -100},
					[]bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{50, -30, -10, 150, -200},
					[]bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{100, -30, 0, 200, -100},
				[]bool{false, false, false, false, false}),
		},
		// Test uint64
		{
			info: "test greatest uint64",
			typ:  types.T_uint64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{100, 50, 200, 10, 5},
					[]bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{50, 30, 150, 20, 15},
					[]bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{100, 50, 200, 20, 15},
				[]bool{false, false, false, false, false}),
		},
		// Test float64
		{
			info: "test greatest float64",
			typ:  types.T_float64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1.5, -2.3, 0.0, 10.5, -5.7},
					[]bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{2.5, -1.3, 0.5, 8.5, -10.7},
					[]bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{2.5, -1.3, 0.5, 10.5, -5.7},
				[]bool{false, false, false, false, false}),
		},
		// Test bool
		{
			info: "test greatest bool",
			typ:  types.T_bool,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, true, false},
					[]bool{false, false, false, false}),
				NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{false, true, false, true},
					[]bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, true, true, true},
				[]bool{false, false, false, false}),
		},
		// Test varchar
		{
			info: "test greatest varchar",
			typ:  types.T_varchar,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"apple", "banana", "cherry", "date"},
					[]bool{false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"zebra", "ant", "dog", "cat"},
					[]bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"zebra", "banana", "dog", "date"},
				[]bool{false, false, false, false}),
		},
		// Test date
		{
			info: "test greatest date",
			typ:  types.T_date,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{types.Date(1000), types.Date(2000), types.Date(1500)},
					[]bool{false, false, false}),
				NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{types.Date(1200), types.Date(1800), types.Date(1400)},
					[]bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_date.ToType(), false,
				[]types.Date{types.Date(1200), types.Date(2000), types.Date(1500)},
				[]bool{false, false, false}),
		},
		// Test with nulls
		{
			info: "test greatest with nulls",
			typ:  types.T_int64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{10, 20, 30},
					[]bool{false, true, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{5, 15, 25},
					[]bool{false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{10, 15, 25},
				[]bool{false, true, true}),
		},
		// Test single argument
		{
			info: "test greatest single argument",
			typ:  types.T_int64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{42, 100, -50},
					[]bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{42, 100, -50},
				[]bool{false, false, false}),
		},
		// Test three arguments
		{
			info: "test greatest three arguments",
			typ:  types.T_int64,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{10, 20, 30},
					[]bool{false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{5, 15, 25},
					[]bool{false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{8, 12, 35},
					[]bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{10, 20, 35},
				[]bool{false, false, false}),
		},
	}
}

func TestGreatest(t *testing.T) {
	testCases := initGreatestTestCase()
	proc := testutil.NewProcess(t)

	for _, tc := range testCases {
		var fcTC FunctionTestCase
		switch tc.typ {
		case types.T_int8:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, greatestFn)
		case types.T_int64:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, greatestFn)
		case types.T_uint64:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, greatestFn)
		case types.T_float64:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, greatestFn)
		case types.T_bool:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, greatestFn)
		case types.T_varchar:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, greatestFn)
		case types.T_date:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, greatestFn)
		default:
			fcTC = NewFunctionTestCase(proc, tc.inputs, tc.expect, greatestFn)
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}
