// Copyright 2021 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

type tcTemp struct {
	typ    types.T
	info   string
	inputs []testutil.FunctionTestInput
	expect testutil.FunctionTestResult
}

func initAbsTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test abs int64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-23, 9999999, -11, -99999, 9999999, -11, -99999, 9999999, -11, -99999, 9999999, -11, -99999},
					[]bool{false, false, false, false, false, false, false, false, false, false, false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{23, 9999999, 11, 99999, 9999999, 11, 99999, 9999999, 11, 99999, 9999999, 11, 99999},
				[]bool{false, false, false, false, false, false, false, false, false, false, false, false, false}),
		},
	}
}

func TestAbs(t *testing.T) {
	testCases := initAbsTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, AbsInt64)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func BenchmarkAbsInt64(b *testing.B) {
	testCases := initAbsTestCase()
	proc := testutil.NewProcess()

	b.StartTimer()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, AbsInt64)
		_ = fcTC.BenchMarkRun()
	}
	b.StopTimer()
}

func initAbsArrayTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test abs float32 array",
			typ:  types.T_array_float32,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(),
					[][]float32{{-4, 9999999, -99999}, {0, -25, 49}},
					[]bool{false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{{4, 9999999, 99999}, {0, 25, 49}},
				[]bool{false, false}),
		},
		{
			info: "test abs float64 array",
			typ:  types.T_array_float64,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float64.ToType(),
					[][]float64{{-4, 9999999, -99999}, {0, -25, 49}},
					[]bool{false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_array_float64.ToType(), false,
				[][]float64{{4, 9999999, 99999}, {0, 25, 49}},
				[]bool{false, false}),
		},
	}
}

func TestAbsArray(t *testing.T) {
	testCases := initAbsArrayTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		var fcTC testutil.FunctionTestCase
		switch tc.typ {
		case types.T_array_float32:
			fcTC = testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, AbsArray[float32])
		case types.T_array_float64:
			fcTC = testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, AbsArray[float64])
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initNormalizeL2ArrayTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test normalize_l2 float32 array",
			typ:  types.T_array_float32,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(),
					[][]float32{
						{},
						{1, 2, 3, 4},
						{-1, 2, 3, 4},
						{10, 3.333333333333333, 4, 5},
						{1, 2, 3.6666666666666665, 4.666666666666666}},
					[]bool{true, false, false, false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{
					{},
					{0.18257418, 0.36514837, 0.5477226, 0.73029673},
					{-0.18257418, 0.36514837, 0.5477226, 0.73029673},
					{0.8108108, 0.27027026, 0.32432434, 0.4054054},
					{0.1576765, 0.315353, 0.5781472, 0.73582363},
				},
				[]bool{true, false, false, false, false, false}),
		},
		{
			info: "test normalize_l2 float64 array",
			typ:  types.T_array_float64,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float64.ToType(),
					[][]float64{
						{},
						{1, 2, 3, 4},
						{-1, 2, 3, 4},
						{10, 3.333333333333333, 4, 5},
						{1, 2, 3.6666666666666665, 4.666666666666666},
					},
					[]bool{true, false, false, false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_array_float64.ToType(), false,
				[][]float64{
					{},
					{0.18257418583505536, 0.3651483716701107, 0.5477225575051661, 0.7302967433402214},
					{-0.18257418583505536, 0.3651483716701107, 0.5477225575051661, 0.7302967433402214},
					{0.8108108108108107, 0.27027027027027023, 0.3243243243243243, 0.4054054054054054},
					{0.15767649936829103, 0.31535299873658207, 0.5781471643504004, 0.7358236637186913},
				},
				[]bool{true, false, false, false, false, false}),
		},
	}
}

func TestNormalizeL2Array(t *testing.T) {
	testCases := initNormalizeL2ArrayTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		var fcTC testutil.FunctionTestCase
		switch tc.typ {
		case types.T_array_float32:
			fcTC = testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, NormalizeL2Array[float32])
		case types.T_array_float64:
			fcTC = testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, NormalizeL2Array[float64])
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initSummationArrayTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test summation float32 array",
			typ:  types.T_array_float32,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(),
					[][]float32{{1, 2, 3}, {4, 5, 6}},
					[]bool{false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{6, 15},
				[]bool{false, false}),
		},
		{
			info: "test summation float64 array",
			typ:  types.T_array_float64,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float64.ToType(),
					[][]float64{{1, 2, 3}, {4, 5, 6}},
					[]bool{false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{6, 15},
				[]bool{false, false}),
		},
	}
}

func TestSummationArray(t *testing.T) {
	testCases := initSummationArrayTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		var fcTC testutil.FunctionTestCase
		switch tc.typ {
		case types.T_array_float32:
			fcTC = testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, SummationArray[float32])
		case types.T_array_float64:
			fcTC = testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, SummationArray[float64])
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initL1NormArrayTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test L1Norm float32 array",
			typ:  types.T_array_float32,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(),
					[][]float32{{1, 2, 3}, {4, 5, 6}},
					[]bool{false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{6, 15},
				[]bool{false, false}),
		},
		{
			info: "test L1Norm float64 array",
			typ:  types.T_array_float64,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float64.ToType(),
					[][]float64{{1, 2, 3}, {4, 5, 6}},
					[]bool{false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{6, 15},
				[]bool{false, false}),
		},
	}
}

func TestL1NormArray(t *testing.T) {
	testCases := initL1NormArrayTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		var fcTC testutil.FunctionTestCase
		switch tc.typ {
		case types.T_array_float32:
			fcTC = testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, L1NormArray[float32])
		case types.T_array_float64:
			fcTC = testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, L1NormArray[float64])
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initL2NormArrayTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test L2Norm float32 array",
			typ:  types.T_array_float32,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(),
					[][]float32{{1, 2, 3}, {4, 5, 6}},
					[]bool{false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{3.741657386773941, 8.774964387392123},
				[]bool{false, false}),
		},
		{
			info: "test L2Norm float64 array",
			typ:  types.T_array_float64,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float64.ToType(),
					[][]float64{{1, 2, 3}, {4, 5, 6}},
					[]bool{false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{3.741657386773941, 8.774964387392124},
				[]bool{false, false}),
		},
	}
}

func TestL2NormArray(t *testing.T) {
	testCases := initL2NormArrayTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		var fcTC testutil.FunctionTestCase
		switch tc.typ {
		case types.T_array_float32:
			fcTC = testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, L2NormArray[float32])
		case types.T_array_float64:
			fcTC = testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, L2NormArray[float64])
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initSubVectorTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "2",
			typ:  types.T_array_float32,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, []bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{{1, 2, 3}},
				[]bool{false}),
		},
		{
			info: "2",
			typ:  types.T_array_float32,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{2}, []bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{{2, 3}},
				[]bool{false}),
		},
		{
			info: "2",
			typ:  types.T_array_float32,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{3}, []bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{{3}},
				[]bool{false}),
		},
		{
			info: "2",
			typ:  types.T_array_float32,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{-1}, []bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{{3}},
				[]bool{false}),
		},
		{
			info: "2",
			typ:  types.T_array_float32,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{-2}, []bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{{2, 3}},
				[]bool{false}),
		},
		{
			info: "2",
			typ:  types.T_array_float32,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{-3}, []bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{{1, 2, 3}},
				[]bool{false}),
		},
		{
			info: "2",
			typ:  types.T_array_float32,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{0}, []bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{{}},
				[]bool{false}),
		},
		{
			info: "2",
			typ:  types.T_array_float64,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{1, 2, 3}}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, []bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_array_float64.ToType(), false,
				[][]float64{{1, 2, 3}},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_array_float32,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, []bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{{1}},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_array_float32,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{2}, []bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{{1, 2}},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_array_float32,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{3}, []bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{{1, 2, 3}},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_array_float32,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{4}, []bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{{1, 2, 3}},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_array_float32,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{-2}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{2}, []bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{{2, 3}},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_array_float32,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{-3}, []bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{2}, []bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{{1, 2}},
				[]bool{false}),
		},
	}
}

func TestSubVector(t *testing.T) {
	testCases := initSubVectorTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		var fcTC testutil.FunctionTestCase
		switch tc.typ {
		case types.T_array_float32:
			switch tc.info {
			case "2":
				fcTC = testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, SubVectorWith2Args[float32])
			case "3":
				fcTC = testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, SubVectorWith3Args[float32])
			}
		case types.T_array_float64:
			switch tc.info {
			case "2":
				fcTC = testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, SubVectorWith2Args[float64])
			case "3":
				fcTC = testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, SubVectorWith3Args[float64])
			}
		}

		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initAsciiStringTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test Ascii string",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"-23", "9999999", "-11"},
					[]bool{false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{45, 57, 45},
				[]bool{false, false, false}),
		},
	}
}

func TestAsciiString(t *testing.T) {
	testCases := initAsciiStringTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, AsciiString)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initAsciiIntTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test Ascii Int",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{11},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{49},
				[]bool{false}),
		},
	}
}

func TestAsciiInt(t *testing.T) {
	testCases := initAsciiIntTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, AsciiInt[int64])
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initAsciiUintTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test Ascii Int",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{11},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{49},
				[]bool{false}),
		},
	}
}

func TestAsciiUint(t *testing.T) {
	testCases := initAsciiUintTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, AsciiUint[uint64])
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initBinTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test Bin Int",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{2, 4, 6, 8, 16, 32, 64, 128},
					[]bool{false, false, false, false, false, false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"10", "100", "110", "1000", "10000", "100000", "1000000", "10000000"},
				[]bool{false, false, false, false, false, false, false, false}),
		},
	}
}

func TestBin(t *testing.T) {
	testCases := initBinTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, Bin[uint8])
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initBinFloatTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test Bin Int",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{2.1111, 4.4261264, 6.1151275, 8.48484, 16.266, 32.3338787, 64.0000000, 128.26454},
					[]bool{false, false, false, false, false, false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"10", "100", "110", "1000", "10000", "100000", "1000000", "10000000"},
				[]bool{false, false, false, false, false, false, false, false}),
		},
	}
}

func TestBinFloat(t *testing.T) {
	testCases := initBinFloatTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, BinFloat[float32])
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initBitLengthFuncTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test BitLengthFunc",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{0},
				[]bool{false}),
		},
		{
			info: "test BitLengthFunc",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""},
					[]bool{true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{},
				[]bool{true}),
		},
		{
			info: "test BitLengthFunc",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"matrix", "origin", "=", "mo", " ", "\t", ""},
					[]bool{false, false, false, false, false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{48, 48, 8, 16, 8, 8, 0},
				[]bool{false, false, false, false, false, false, false}),
		},
	}
}

func TestBitLengthFunc(t *testing.T) {
	testCases := initBitLengthFuncTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, BitLengthFunc)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initCurrentDateTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test current date",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{types.Date(111)},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_date.ToType(), false,
				[]types.Date{types.Date(111)},
				[]bool{false}),
		},
	}
}

func TestCurrentDate(t *testing.T) {
	testCases := initCurrentDateTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, CurrentDate)
		s, _ := fcTC.Run()
		require.Equal(t, s, false)
	}
}

func initDateToDateTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test date to date",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{types.Date(20040403), types.Date(20211003), types.Date(20200823)},
					[]bool{false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_date.ToType(), false,
				[]types.Date{types.Date(20040403), types.Date(20211003), types.Date(20200823)},
				[]bool{false, false, false}),
		},
	}
}

func TestDateToDate(t *testing.T) {
	testCases := initDateToDateTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, DateToDate)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initDatetimeToDateTestCase() []tcTemp {
	t, _ := types.ParseDatetime("2020-10-10 11:11:11", 6)
	return []tcTemp{
		{
			info: "test datetime to date",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{t},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_date.ToType(), false,
				[]types.Date{t.ToDate()},
				[]bool{false}),
		},
	}
}

func TestDatetimeToDate(t *testing.T) {
	testCases := initDatetimeToDateTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, DatetimeToDate)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initTimeToDateTestCase() []tcTemp {
	t, _ := types.ParseTime("2020-10-10 11:11:11", 6)
	return []tcTemp{
		{
			info: "test datetime to date",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_time.ToType(),
					[]types.Time{t},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_date.ToType(), false,
				[]types.Date{t.ToDate()},
				[]bool{false}),
		},
	}
}

func TestTimeToDate(t *testing.T) {
	testCases := initTimeToDateTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, TimeToDate)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initDateStringToDateTestCase() []tcTemp {
	t, _ := types.ParseDatetime("2020-10-10 11:11:11", 6)
	return []tcTemp{
		{
			info: "test date string to date",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"2020-10-10"},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_date.ToType(), false,
				[]types.Date{t.ToDate()},
				[]bool{false}),
		},
	}
}

func TestDateStringToDate(t *testing.T) {
	testCases := initDateStringToDateTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, DateStringToDate)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initDateToDayTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test date to date",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{types.Date(20040403), types.Date(20211003), types.Date(20200823)},
					[]bool{false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{5, 6, 23},
				[]bool{false, false, false}),
		},
	}
}

func TestDateToDay(t *testing.T) {
	testCases := initDateToDayTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, DateToDay)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initDatetimeToDayTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test date to date",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{types.Datetime(20040403), types.Datetime(20211003), types.Datetime(20200823)},
					[]bool{false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{1, 1, 1},
				[]bool{false, false, false}),
		},
	}
}

func TestDatetimeToDay(t *testing.T) {
	testCases := initDatetimeToDayTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, DatetimeToDay)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initDayOfYearTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test date of Year",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{types.Date(20040403), types.Date(20211003), types.Date(20200823)},
					[]bool{false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{278, 311, 358},
				[]bool{false, false, false}),
		},
	}
}

func TestDayOfYear(t *testing.T) {
	testCases := initDayOfYearTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, DayOfYear)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initEmptyTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test date of Year",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_char.ToType(),
					[]string{"", "sdfsdf", ""},
					[]bool{false, false, true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, false},
				[]bool{false, false, true}),
		},
	}
}

func TestEmpty(t *testing.T) {
	testCases := initEmptyTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, Empty)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initJsonQuoteTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test json quote",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"key:v", "sdfsdf", ""},
					[]bool{false, false, true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_json.ToType(), false,
				[]string{"\f\u0005key:v", "\f\u0006sdfsdf", ""},
				[]bool{false, false, true}),
		},
	}
}

func TestJsonQuote(t *testing.T) {
	testCases := initJsonQuoteTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, JsonQuote)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initJsonUnquoteTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test json unquote",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`"hello"`, `"world"`, `""`},
					[]bool{false, false, true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"hello", "world", ""},
				[]bool{false, false, true}),
		},
	}
}

func TestJsonUnquote(t *testing.T) {
	testCases := initJsonUnquoteTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, JsonUnquote)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func TestLoadFile(t *testing.T) {
	dir := t.TempDir()
	proc := testutil.NewProc()
	ctx := context.Background()
	filepath := dir + "test"
	fs, readPath, err := fileservice.GetForETL(ctx, proc.FileService, filepath)
	assert.Nil(t, err)
	err = fs.Write(ctx, fileservice.IOVector{
		FilePath: readPath,
		Entries: []fileservice.IOEntry{
			{
				Offset: 0,
				Size:   4,
				Data:   []byte("1234"),
			},
			{
				Offset: 4,
				Size:   4,
				Data:   []byte("5678"),
			},
		},
	})
	assert.Nil(t, err)

	testCases := []tcTemp{
		{
			info: "test load file",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{filepath},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"12345678"},
				[]bool{false}),
		},
	}

	// do the test work.
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, LoadFile)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initMoMemoryUsageTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test mo memory usage",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""},
					[]bool{true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{""},
				[]bool{true}),
		},
	}
}

func TestMoMemoryUsage(t *testing.T) {
	testCases := initMoMemoryUsageTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, MoMemUsage)
		_, _ = fcTC.Run()
		require.Error(t, moerr.NewInvalidInput(proc.Ctx, "mo mem usage can only take scalar input"))
	}
}

func TestMoEnableMemoryUsage(t *testing.T) {
	testCases := initMoMemoryUsageTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, MoEnableMemUsageDetail)
		_, _ = fcTC.Run()
		require.Error(t, moerr.NewInvalidInput(proc.Ctx, "mo mem usage can only take scalar input"))
	}
}

func TestMoDisableMemoryUsage(t *testing.T) {
	testCases := initMoMemoryUsageTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, MoDisableMemUsageDetail)
		_, _ = fcTC.Run()
		require.Error(t, moerr.NewInvalidInput(proc.Ctx, "mo mem usage can only take scalar input"))
	}
}

func initSpaceTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test space",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{1, 2, 3, 0},
					[]bool{false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{" ", "  ", "   ", ""},
				[]bool{false, false, false}),
		},
	}
}

func TestSpace(t *testing.T) {
	testCases := initSpaceTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, SpaceNumber[uint64])
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initToTimeCase() []tcTemp {
	d1, _ := types.ParseDatetime("2022-01-01", 6)
	d2, _ := types.ParseDatetime("2022-01-01 16:22:44", 6)
	//d3, scale, _ := types.Parse128("20221212112233.4444")
	//d3, _ = d3.Scale(3 - scale)
	return []tcTemp{
		{
			info: "test to time",
			typ:  types.T_date,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{d1.ToDate()},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_time.ToType(), false,
				[]types.Time{types.TimeFromClock(false, 0, 0, 0, 0)},
				[]bool{false}),
		},
		{
			info: "test to time",
			typ:  types.T_datetime,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d2},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_time.ToType(), false,
				[]types.Time{types.TimeFromClock(false, 16, 22, 44, 0)},
				[]bool{false}),
		},
		{
			info: "test to time",
			typ:  types.T_int64,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{20221212112233},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_time.ToType(), false,
				[]types.Time{types.TimeFromClock(false, 2022121211, 22, 33, 0)},
				[]bool{false}),
		},
		{
			info: "test to time",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"2022-01-01 16:22:44.1235"},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_time.ToType(), false,
				[]types.Time{types.TimeFromClock(false, 16, 22, 44, 123500)},
				[]bool{false}),
		},
		//{
		//	info: "test to time",
		//	typ:  types.T_decimal128,
		//	inputs: []testutil.FunctionTestInput{
		//		testutil.NewFunctionTestInput(types.T_decimal128.ToType(),
		//			[]types.Decimal128{d3},
		//			[]bool{false}),
		//	},
		//	expect: testutil.NewFunctionTestResult(types.T_time.ToType(), false,
		//		[]types.Time{types.TimeFromClock(false, 2022121211, 22, 33, 444000)},
		//		[]bool{false}),
		//},
	}
}

func TestToTime(t *testing.T) {
	testCases := initToTimeCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		var fcTC testutil.FunctionTestCase
		switch tc.typ {
		case types.T_date:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, DateToTime)
		case types.T_datetime:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, DatetimeToTime)
		case types.T_int64:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, Int64ToTime)
		case types.T_varchar:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, DateStringToTime)
			//case types.T_decimal128:
			//	fcTC = testutil.NewFunctionTestCase(proc,
			//		tc.inputs, tc.expect, Decimal128ToTime)
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initToTimestampCase() []tcTemp {
	d1, _ := types.ParseDatetime("2022-01-01", 6)
	d2, _ := types.ParseDatetime("2022-01-01 00:00:00", 6)

	return []tcTemp{
		{
			info: "test to timestamp",
			typ:  types.T_date,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{d1.ToDate()},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_timestamp.ToType(), false,
				[]types.Timestamp{types.FromClockZone(time.Local, 2022, 1, 1, 0, 0, 0, 0)},
				[]bool{false}),
		},
		{
			info: "test to timestamp",
			typ:  types.T_datetime,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d2},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_timestamp.ToType(), false,
				[]types.Timestamp{types.FromClockZone(time.Local, 2022, 1, 1, 0, 0, 0, 0)},
				[]bool{false}),
		},
		{
			info: "test to timestamp",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"2022-01-01"},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_timestamp.ToType(), false,
				[]types.Timestamp{types.FromClockZone(time.Local, 2022, 1, 1, 0, 0, 0, 0)},
				[]bool{false}),
		},
	}
}

func TestToTimeStamp(t *testing.T) {
	testCases := initToTimestampCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		var fcTC testutil.FunctionTestCase
		switch tc.typ {
		case types.T_date:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, DateToTimestamp)
		case types.T_datetime:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, DatetimeToTimestamp)
		case types.T_varchar:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, DateStringToTimestamp)
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func TestValues(t *testing.T) {
	testCases := []tcTemp{
		{
			info: "values(col_int8)",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{-23}, []bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{-23}, []bool{false}),
		},
		{
			info: "values(col_uint8)",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{23, 24, 25}, []bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{23, 24, 25}, []bool{false}),
		},
	}

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, Values)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initHourTestCase() []tcTemp {
	d1, _ := types.ParseDatetime("2004-04-03 10:20:00", 6)
	d2, _ := types.ParseTimestamp(time.Local, "2004-08-03 01:01:37", 6)

	return []tcTemp{
		{
			info: "test hour",
			typ:  types.T_datetime,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d1},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{10},
				[]bool{false}),
		},
		{
			info: "test hour",
			typ:  types.T_timestamp,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_timestamp.ToType(),
					[]types.Timestamp{d2},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{1},
				[]bool{false}),
		},
	}
}

func TestHour(t *testing.T) {
	testCases := initHourTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		var fcTC testutil.FunctionTestCase
		switch tc.typ {
		case types.T_datetime:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, DatetimeToHour)
		case types.T_timestamp:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, TimestampToHour)
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initMinuteTestCase() []tcTemp {
	d1, _ := types.ParseDatetime("2004-04-03 10:20:00", 6)
	d2, _ := types.ParseTimestamp(time.Local, "2004-08-03 01:01:37", 6)

	return []tcTemp{
		{
			info: "test hour",
			typ:  types.T_datetime,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d1},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{20},
				[]bool{false}),
		},
		{
			info: "test hour",
			typ:  types.T_timestamp,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_timestamp.ToType(),
					[]types.Timestamp{d2},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{1},
				[]bool{false}),
		},
	}
}

func TestMinute(t *testing.T) {
	testCases := initMinuteTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		var fcTC testutil.FunctionTestCase
		switch tc.typ {
		case types.T_datetime:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, DatetimeToMinute)
		case types.T_timestamp:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, TimestampToMinute)
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initSecondTestCase() []tcTemp {
	d1, _ := types.ParseDatetime("2004-04-03 10:20:00", 6)
	d2, _ := types.ParseTimestamp(time.Local, "2004-01-03 23:15:08", 6)

	return []tcTemp{
		{
			info: "test hour",
			typ:  types.T_datetime,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d1},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{0},
				[]bool{false}),
		},
		{
			info: "test hour",
			typ:  types.T_timestamp,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_timestamp.ToType(),
					[]types.Timestamp{d2},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{8},
				[]bool{false}),
		},
	}
}

func TestSecond(t *testing.T) {
	testCases := initSecondTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		var fcTC testutil.FunctionTestCase
		switch tc.typ {
		case types.T_datetime:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, DatetimeToSecond)
		case types.T_timestamp:
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, TimestampToSecond)
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initBinaryTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test hour",
			typ:  types.T_datetime,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"hello"},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_binary.ToType(), false,
				[]string{"hello"},
				[]bool{false}),
		},
	}
}

func TestBinary(t *testing.T) {
	testCases := initBinaryTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, Binary)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initHexStringTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test hex string",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"a", "", "255", ""},
					[]bool{false, false, false, true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"61", "", "323535", ""},
				[]bool{false, false, false, true}),
		},
		{
			//TODO: Verify the multi-row case: original code:https://github.com/m-schen/matrixone/blob/d2f81f4b9d843ecb749fa0277332b4150e1fd87f/pkg/sql/plan/function/builtin/unary/hex_test.go#L58
			info: "test hex string - multirow",
			inputs: []testutil.FunctionTestInput{testutil.NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"Hello", "Gopher!"},
				[]bool{false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"48656c6c6f", "476f7068657221"},
				[]bool{false, false}),
		},
		{
			info: "test encode - string to hex",
			inputs: []testutil.FunctionTestInput{testutil.NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"", "abc", "a\nb", `a\nb`, "a\"b"},
				[]bool{false, false, false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"", "616263", "610a62", "615c6e62", "612262"},
				[]bool{false, false, false, false, false}),
		},
	}
}

func TestHexString(t *testing.T) {
	testCases := initHexStringTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, HexString)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initHexInt64TestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test hex int64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{255, 231323423423421, 0}, []bool{false, false, true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"FF", "D2632E7B3BBD", ""},
				[]bool{false, false, true}),
		},
		{
			//TODO: Verify the multi-row case. Original code: https://github.com/m-schen/matrixone/blob/d2f81f4b9d843ecb749fa0277332b4150e1fd87f/pkg/sql/plan/function/builtin/unary/hex_test.go#L116
			info: "test hex int64 - multirow",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int64.ToType(), []int64{123, 234, 345}, []bool{false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"7B", "EA", "159"},
				[]bool{false, false, false}),
		},
	}
}

func TestHexInt64(t *testing.T) {
	testCases := initHexInt64TestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, HexInt64)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// HexArray
func initHexArrayTestCase() []tcTemp {

	arrayF32Cases := []struct {
		info  string
		data  [][]float32
		wants []string
	}{
		{
			info: "test encode - array_float32 to hex",
			data: [][]float32{
				{0.34881967306137085, 0.0028086076490581036, 0.5752133727073669},
				{0.95072953, 0.54392913, 0.30788785},
				{0.98972348, 0.61145728, 0.27879944},
				{0.37520402, 0.13316834, 0.94819581},
			},
			wants: []string{
				"7e98b23e9e10383b2f41133f",
				"0363733ff13e0b3f7aa39d3e",
				"855e7d3f77881c3fcdbe8e3e",
				"be1ac03e485d083ef6bc723f",
			},
		},
	}

	arrayF64Cases := []struct {
		info  string
		data  [][]float64
		wants []string
	}{
		{
			info: "test encode - array_float64 to hex",
			data: [][]float64{
				{0.34881967306137085, 0.0028086076490581036, 0.5752133727073669},
			},
			wants: []string{
				"000000c00f53d63f000000c01302673f000000e02568e23f",
			},
		},
	}

	var testInputs = make([]tcTemp, 0, len(arrayF32Cases)+len(arrayF64Cases))

	for _, c := range arrayF32Cases {

		testInputs = append(testInputs, tcTemp{
			info: c.info,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float32.ToType(), c.data, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_text.ToType(), false, c.wants, []bool{}),
		})
	}

	for _, c := range arrayF64Cases {

		testInputs = append(testInputs, tcTemp{
			info: c.info,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_array_float64.ToType(), c.data, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_text.ToType(), false, c.wants, []bool{}),
		})
	}

	return testInputs

}

func TestHexArray(t *testing.T) {
	testCases := initHexArrayTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, HexArray)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initMd5TestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test encode - string to md5",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"", "abc", "abcd", "abc\b", "abc\"d", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
					[]bool{false, false, false, false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_blob.ToType(), false,
				[]string{
					"d41d8cd98f00b204e9800998ecf8427e",
					"900150983cd24fb0d6963f7d28e17f72",
					"e2fc714c4727ee9395f324cd2e7f331f",
					"c7fa18a56de1b25123523e8475ceb311",
					"0671c72bd761b6ab47f5385798998780",
					"5eca9bd3eb07c006cd43ae48dfde7fd3",
				},
				[]bool{false, false, false, false, false, false}),
		},
	}
}

func TestMd5(t *testing.T) {
	testCases := initMd5TestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, Md5)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initUnhexTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test unhex",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"616263", "610a62", "615c6e62", "612262", "e4bda0e5a5bd", "invalid", "", ""},
					[]bool{false, false, false, false, false, false, false, true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_blob.ToType(), false,
				[]string{"abc", "a\nb", `a\nb`, "a\"b", "你好", "", "", ""},
				[]bool{false, false, false, false, false, true, false, true}),
		},
		{
			info: "test encode - hex to blob",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"", "616263", "610a62", "615c6e62", "612262"},
					[]bool{false, false, false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_blob.ToType(), false,
				[]string{"", "abc", "a\nb", `a\nb`, "a\"b"},
				[]bool{false, false, false, false, false}),
		},
		{
			info: "test encode -  hex to blob(array_float32)",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{
						"7e98b23e9e10383b2f41133f",
						"0363733ff13e0b3f7aa39d3e",
						"855e7d3f77881c3fcdbe8e3e",
						"be1ac03e485d083ef6bc723f",
					},
					[]bool{false, false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_blob.ToType(), false, []string{
				functionUtil.QuickBytesToStr(types.ArrayToBytes([]float32{0.34881967306137085, 0.0028086076490581036, 0.5752133727073669})),
				functionUtil.QuickBytesToStr(types.ArrayToBytes([]float32{0.95072953, 0.54392913, 0.30788785})),
				functionUtil.QuickBytesToStr(types.ArrayToBytes([]float32{0.98972348, 0.61145728, 0.27879944})),
				functionUtil.QuickBytesToStr(types.ArrayToBytes([]float32{0.37520402, 0.13316834, 0.94819581})),
			}, []bool{false, false, false, false}),
		},
		{
			info: "test encode - hex to blob(array_float64)",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{
					"000000c00f53d63f000000c01302673f000000e02568e23f",
				}, []bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_blob.ToType(), false, []string{
				functionUtil.QuickBytesToStr(types.ArrayToBytes([]float64{0.34881967306137085, 0.0028086076490581036, 0.5752133727073669})),
			}, []bool{false}),
		},
	}
}

func TestUnhex(t *testing.T) {
	testCases := initUnhexTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, Unhex)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// ToBase64
func initToBase64TestCase() []tcTemp {
	regularCases := []struct {
		info  string
		data  []string
		wants []string
	}{
		{
			info:  "test encode - string to base64",
			data:  []string{"", "abc", "a\nb", `a\nb`, "a\"b"},
			wants: []string{"", "YWJj", "YQpi", "YVxuYg==", "YSJi"},
		},
	}

	var testInputs = make([]tcTemp, 0, len(regularCases))
	for _, c := range regularCases {

		testInputs = append(testInputs, tcTemp{
			info: c.info,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), c.data, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_text.ToType(), false, c.wants, []bool{}),
		})
	}

	return testInputs

}

func TestToBase64(t *testing.T) {
	testCases := initToBase64TestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, ToBase64)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// FromBase64
func initFromBase64TestCase() []tcTemp {
	regularCases := []struct {
		info  string
		data  []string
		wants []string
	}{

		{
			info:  "test encode - base64 to blob",
			data:  []string{"", "YWJj", "YQpi", "YSJi"},
			wants: []string{"", "abc", "a\nb", "a\"b"},
		},
	}

	var testInputs = make([]tcTemp, 0, len(regularCases))
	for _, c := range regularCases {

		testInputs = append(testInputs, tcTemp{
			info: c.info,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(), c.data, []bool{}),
			},
			expect: testutil.NewFunctionTestResult(types.T_blob.ToType(), false, c.wants, []bool{}),
		})
	}

	return testInputs

}

func TestFromBase64(t *testing.T) {
	testCases := initFromBase64TestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, FromBase64)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initBlobLengthTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test length blob",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_blob.ToType(),
					//TODO: verify: Passing String instead of []byte. Original Code: https://github.com/m-schen/matrixone/blob/d2f81f4b9d843ecb749fa0277332b4150e1fd87f/pkg/sql/plan/function/builtin/unary/length_test.go#L117
					[]string{"12345678", ""},
					[]bool{false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{8, 0},
				[]bool{false, false}),
		},
	}
}

func TestBlobLength(t *testing.T) {
	testCases := initBlobLengthTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, Length)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initLengthTestCase() []tcTemp {
	return []tcTemp{
		{
			//TODO: verify if makevector can be represented like this. Original Code: https://github.com/m-schen/matrixone/blob/d2f81f4b9d843ecb749fa0277332b4150e1fd87f/pkg/sql/plan/function/builtin/unary/length_test.go#L51
			info:   "test length varchar",
			inputs: []testutil.FunctionTestInput{testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{"abcdefghijklm"}, []bool{false})},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false, []int64{13}, []bool{false}),
		},
		{
			//TODO: verify if makevector can be represented like this. Original Code: https://github.com/m-schen/matrixone/blob/d2f81f4b9d843ecb749fa0277332b4150e1fd87f/pkg/sql/plan/function/builtin/unary/length_test.go#L58
			info:   "test length char",
			inputs: []testutil.FunctionTestInput{testutil.NewFunctionTestInput(types.T_char.ToType(), []string{"abcdefghijklm"}, []bool{false})},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false, []int64{13}, []bool{false}),
		},
		{
			//TODO: Previously T_Text was not added. Original code: https://github.com/m-schen/matrixone/blob/d2f81f4b9d843ecb749fa0277332b4150e1fd87f/pkg/sql/plan/function/builtin/unary/length_test.go#L71
			info:   "test length text",
			inputs: []testutil.FunctionTestInput{testutil.NewFunctionTestInput(types.T_text.ToType(), []string{"abcdefghijklm"}, []bool{false})},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false, []int64{13}, []bool{false}),
		},
	}
}

func TestLength(t *testing.T) {
	testCases := initLengthTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, Length)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// LengthUTF8

func initLengthUTF8TestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test lengthutf8",
			inputs: []testutil.FunctionTestInput{testutil.NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{
					"abc",
					"",
					"   ",
					"中国123",
					"abc😄",
					"中国中国中国中国中国中国中国中国中国中国1234",
					"中国中国中国中国中国中国中国中国中国中国1234😄ggg!",
					"你好",
					"français",
					"にほんご",
					"Español",
					"123456",
					"андрей",
					"\\",
					string(rune(0x0c)),
					string('"'),
					string('\a'),
					string('\b'),
					string('\t'),
					string('\n'),
					string('\r'),
					string(rune(0x10)),
					"你好",
					"再见",
					"今天",
					"日期时间",
					"明天",
					"\n\t\r\b" + string(rune(0)) + "\\_\\%\\",
				},
				[]bool{
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
				})},
			expect: testutil.NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{
					3,
					0,
					3,
					5,
					4,
					24,
					29,
					2,
					8,
					4,
					7,
					6,
					6,
					1,
					1,
					1,
					1,
					1,
					1,
					1,
					1,
					1,
					2,
					2,
					2,
					4,
					2,
					10,
				},
				[]bool{
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
				}),
		},
	}
}

func TestLengthUTF8(t *testing.T) {
	testCases := initLengthUTF8TestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, LengthUTF8)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// Ltrim

func initLtrimTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test ltrim",
			inputs: []testutil.FunctionTestInput{testutil.NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{" 123", "  123", "123 ", " 8 ", " 8 a ", ""},
				[]bool{false, false, false, false, false, true},
			)},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"123", "123", "123 ", "8 ", "8 a ", ""},
				[]bool{false, false, false, false, false, true},
			),
		},
	}
}

func TestLtrim(t *testing.T) {
	testCases := initLtrimTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, Ltrim)

		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// Rtrim

func initRtrimTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test rtrim",
			inputs: []testutil.FunctionTestInput{testutil.NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{
					"barbar   ",
					"MySQL",
					"a",
					"  20.06 ",
					"  right  ",
					"你好  ",
					"2017-06-15   ",
					"2017-06-15        ",

					"ｱｲｳｴｵ",
					"ｱｲｳｴｵ ",
					"ｱｲｳｴｵ  ",
					"ｱｲｳｴｵ   ",
					"ｱｲｳｴｵ ",
					"ｱｲｳｴｵ    ",
					"ｱｲｳｴｵ      ",
					"あいうえお",
					"あいうえお ",
					"あいうえお  ",
					"あいうえお   ",
					"あいうえお",
					"あいうえお ",
					"あいうえお   ",
					"龔龖龗龞龡",
					"龔龖龗龞龡 ",
					"龔龖龗龞龡  ",
					"龔龖龗龞龡   ",
					"龔龖龗龞龡",
					"龔龖龗龞龡 ",
					"龔龖龗龞龡   ",
					"ｱｲｳｴｵ",
					"ｱｲｳｴｵ ",
					"ｱｲｳｴｵ  ",
					"ｱｲｳｴｵ   ",
					"ｱｲｳｴｵ",
					"ｱｲｳｴｵ  ",
					"ｱｲｳｴｵ    ",
					"あいうえお",
					"あいうえお ",
					"あいうえお  ",
					"あいうえお   ",
					"あいうえお",
					"あいうえお  ",
					"あいうえお     ",
					"龔龖龗龞龡",
					"龔龖龗龞龡 ",
					"龔龖龗龞龡  ",
					"龔龖龗龞龡   ",
					"龔龖龗龞龡",
					"龔龖龗龞龡 ",
					"龔龖龗龞龡  ",
					"ｱｲｳｴｵ",
					"ｱｲｳｴｵ ",
					"ｱｲｳｴｵ  ",
					"ｱｲｳｴｵ   ",
					"ｱｲｳｴｵ",
					"ｱｲｳｴｵ ",
					"ｱｲｳｴｵ  ",
					"あいうえお",
					"あいうえお ",
					"あいうえお  ",
					"あいうえお   ",
					"あいうえお",
					"あいうえお  ",
					"あいうえお    ",
					"龔龖龗龞龡",
					"龔龖龗龞龡 ",
					"龔龖龗龞龡  ",
					"龔龖龗龞龡   ",
					"龔龖龗龞龡",
					"龔龖龗龞龡  ",
					"龔龖龗龞龡      ",
					"2017-06-15    ",
					"2019-06-25    ",
					"    2019-06-25  ",
					"   2019-06-25   ",
					"    2012-10-12   ",
					"   2004-04-24.   ",
					"   2008-12-04.  ",
					"    2012-03-23.   ",
					"    2013-04-30  ",
					"  1994-10-04  ",
					"   2018-06-04  ",
					" 2012-10-12  ",
					"1241241^&@%#^*^!@#&*(!&    ",
					" 123 ",
				},
				[]bool{
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
				})},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{
					"barbar",
					"MySQL",
					"a",
					"  20.06",
					"  right",
					"你好",
					"2017-06-15",
					"2017-06-15",

					"ｱｲｳｴｵ",
					"ｱｲｳｴｵ",
					"ｱｲｳｴｵ",
					"ｱｲｳｴｵ",
					"ｱｲｳｴｵ",
					"ｱｲｳｴｵ",
					"ｱｲｳｴｵ",
					"あいうえお",
					"あいうえお",
					"あいうえお",
					"あいうえお",
					"あいうえお",
					"あいうえお",
					"あいうえお",
					"龔龖龗龞龡",
					"龔龖龗龞龡",
					"龔龖龗龞龡",
					"龔龖龗龞龡",
					"龔龖龗龞龡",
					"龔龖龗龞龡",
					"龔龖龗龞龡",
					"ｱｲｳｴｵ",
					"ｱｲｳｴｵ",
					"ｱｲｳｴｵ",
					"ｱｲｳｴｵ",
					"ｱｲｳｴｵ",
					"ｱｲｳｴｵ",
					"ｱｲｳｴｵ",
					"あいうえお",
					"あいうえお",
					"あいうえお",
					"あいうえお",
					"あいうえお",
					"あいうえお",
					"あいうえお",
					"龔龖龗龞龡",
					"龔龖龗龞龡",
					"龔龖龗龞龡",
					"龔龖龗龞龡",
					"龔龖龗龞龡",
					"龔龖龗龞龡",
					"龔龖龗龞龡",
					"ｱｲｳｴｵ",
					"ｱｲｳｴｵ",
					"ｱｲｳｴｵ",
					"ｱｲｳｴｵ",
					"ｱｲｳｴｵ",
					"ｱｲｳｴｵ",
					"ｱｲｳｴｵ",
					"あいうえお",
					"あいうえお",
					"あいうえお",
					"あいうえお",
					"あいうえお",
					"あいうえお",
					"あいうえお",
					"龔龖龗龞龡",
					"龔龖龗龞龡",
					"龔龖龗龞龡",
					"龔龖龗龞龡",
					"龔龖龗龞龡",
					"龔龖龗龞龡",
					"龔龖龗龞龡",
					"2017-06-15",
					"2019-06-25",
					"    2019-06-25",
					"   2019-06-25",
					"    2012-10-12",
					"   2004-04-24.",
					"   2008-12-04.",
					"    2012-03-23.",
					"    2013-04-30",
					"  1994-10-04",
					"   2018-06-04",
					" 2012-10-12",
					"1241241^&@%#^*^!@#&*(!&",
					" 123",
				},
				[]bool{
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
				}),
		},
	}
}

func TestRtrim(t *testing.T) {
	testCases := initRtrimTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, Rtrim)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// Reverse

func initReverseTestCase() []tcTemp {
	return []tcTemp{
		{
			//TODO: How to handle ScalarNulls. Original code: https://github.com/m-schen/matrixone/blob/6715c45c2f6e2b15808b10a21fafc17d03a8ae0b/pkg/sql/plan/function/builtin/unary/reverse_test.go#L75
			info: "test reverse",
			inputs: []testutil.FunctionTestInput{testutil.NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{
					"abc",
					"abcd",
					"hello",
					"ｱｲｳｴｵ",
					"あいうえお",
					"龔龖龗龞龡",
					"你好",
					"再 见",
					"bcd",
					"def",
					"xyz",
					"1a1",
					"2012",
					"@($)@($#)_@(#",
					"2023-04-24",
					"10:03:23.021412",
					"sdfad  ",
				},
				[]bool{
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
				})},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{
					"cba",
					"dcba",
					"olleh",
					"ｵｴｳｲｱ",
					"おえういあ",
					"龡龞龗龖龔",
					"好你",
					"见 再",
					"dcb",
					"fed",
					"zyx",
					"1a1",
					"2102",
					"#(@_)#$(@)$(@",
					"42-40-3202",
					"214120.32:30:01",
					"  dafds",
				},
				[]bool{
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
					false,
				}),
		},
	}
}

func TestReverse(t *testing.T) {
	testCases := initReverseTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, Reverse)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

// Oct

func initOctUint8TestCase() []tcTemp {
	e1, _, _ := types.Parse128("14")
	e2, _, _ := types.Parse128("143")
	e3, _, _ := types.Parse128("144")
	e4, _, _ := types.Parse128("377")

	return []tcTemp{
		{
			info: "test oct uint8",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{12, 99, 100, 255},
					[]bool{false, false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{e1, e2, e3, e4},
				[]bool{false, false, false, false}),
		},
	}
}

func TestOctUint8(t *testing.T) {
	testCases := initOctUint8TestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, Oct[uint8])
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initOctUint16TestCase() []tcTemp {
	e1, _, _ := types.Parse128("14")
	e2, _, _ := types.Parse128("143")
	e3, _, _ := types.Parse128("144")
	e4, _, _ := types.Parse128("377")
	e5, _, _ := types.Parse128("2000")
	e6, _, _ := types.Parse128("23420")
	e7, _, _ := types.Parse128("177777")

	return []tcTemp{
		{
			info: "test oct uint16",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{12, 99, 100, 255, 1024, 10000, 65535},
					[]bool{false, false, false, false, false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{e1, e2, e3, e4, e5, e6, e7},
				[]bool{false, false, false, false, false, false, false}),
		},
	}
}

func TestOctUint16(t *testing.T) {
	testCases := initOctUint16TestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, Oct[uint16])
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initOctUint32TestCase() []tcTemp {
	e1, _, _ := types.Parse128("14")
	e2, _, _ := types.Parse128("143")
	e3, _, _ := types.Parse128("144")
	e4, _, _ := types.Parse128("377")
	e5, _, _ := types.Parse128("2000")
	e6, _, _ := types.Parse128("23420")
	e7, _, _ := types.Parse128("177777")
	e8, _, _ := types.Parse128("37777777777")

	return []tcTemp{
		{
			info: "test oct uint32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{12, 99, 100, 255, 1024, 10000, 65535, 4294967295},
					[]bool{false, false, false, false, false, false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{e1, e2, e3, e4, e5, e6, e7, e8},
				[]bool{false, false, false, false, false, false, false, false}),
		},
	}
}

func TestOctUint32(t *testing.T) {
	testCases := initOctUint32TestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, Oct[uint32])
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initOctUint64TestCase() []tcTemp {
	e1, _, _ := types.Parse128("14")
	e2, _, _ := types.Parse128("143")
	e3, _, _ := types.Parse128("144")
	e4, _, _ := types.Parse128("377")
	e5, _, _ := types.Parse128("2000")
	e6, _, _ := types.Parse128("23420")
	e7, _, _ := types.Parse128("177777")
	e8, _, _ := types.Parse128("37777777777")
	e9, _, _ := types.Parse128("1777777777777777777777")

	return []tcTemp{
		{
			info: "test oct uint64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{12, 99, 100, 255, 1024, 10000, 65535, 4294967295, 18446744073709551615},
					[]bool{false, false, false, false, false, false, false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{e1, e2, e3, e4, e5, e6, e7, e8, e9},
				[]bool{false, false, false, false, false, false, false, false, false}),
		},
	}
}

func TestOctUint64(t *testing.T) {
	testCases := initOctUint64TestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, Oct[uint64])
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initOctInt8TestCase() []tcTemp {
	e1, _, _ := types.Parse128("1777777777777777777600")
	e2, _, _ := types.Parse128("1777777777777777777777")
	e3, _, _ := types.Parse128("177")

	return []tcTemp{
		{
			info: "test oct int8",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{-128, -1, 127},
					[]bool{false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{e1, e2, e3},
				[]bool{false, false, false}),
		},
	}
}

func TestOctInt8(t *testing.T) {
	testCases := initOctInt8TestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, Oct[int8])
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initOctInt16TestCase() []tcTemp {
	e1, _, _ := types.Parse128("1777777777777777700000")

	return []tcTemp{
		{
			info: "test oct int16",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{-32768},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{e1},
				[]bool{false}),
		},
	}
}

func TestOctInt16(t *testing.T) {
	testCases := initOctInt16TestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, Oct[int16])
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initOctInt32TestCase() []tcTemp {
	e1, _, _ := types.Parse128("1777777777760000000000")

	return []tcTemp{
		{
			info: "test oct int32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{-2147483648},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{e1},
				[]bool{false}),
		},
	}
}

func TestOctInt32(t *testing.T) {
	testCases := initOctInt32TestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, Oct[int32])
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initOctInt64TestCase() []tcTemp {
	e1, _, _ := types.Parse128("1000000000000000000000")

	return []tcTemp{
		{
			info: "test oct int64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-9223372036854775808},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{e1},
				[]bool{false}),
		},
	}
}

func TestOctInt64(t *testing.T) {
	testCases := initOctInt64TestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, Oct[int64])
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
	//TODO: I am excluding scalar testcase, as per our last discussion on WeCom: https://github.com/m-schen/matrixone/blob/0a48ec5488caff6fd918ad558ebe054eba745be8/pkg/sql/plan/function/builtin/unary/oct_test.go#L176
	//TODO: Previous OctFloat didn't have testcase. Should we add new testcases?
}

// Month

func initDateToMonthTestCase() []tcTemp {
	d1, _ := types.ParseDateCast("2004-04-03")
	d2, _ := types.ParseDateCast("2004-08-03")
	d3, _ := types.ParseDateCast("2004-01-03")
	return []tcTemp{
		{
			info: "test date to month",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{d1, d2, d3},
					[]bool{false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{4, 8, 1},
				[]bool{false, false, false}),
		},
	}
}

func TestDateToMonth(t *testing.T) {
	testCases := initDateToMonthTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, DateToMonth)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initDateTimeToMonthTestCase() []tcTemp {
	d1, _ := types.ParseDatetime("2004-04-03 13:11:10", 6)
	d2, _ := types.ParseDatetime("1999-08-05 11:01:02", 6)
	d3, _ := types.ParseDatetime("2004-01-03 23:15:08", 6)
	return []tcTemp{
		{
			info: "test datetime to month",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d1, d2, d3},
					[]bool{false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{4, 8, 1},
				[]bool{false, false, false}),
		},
	}
}

func TestDateTimeToMonth(t *testing.T) {
	testCases := initDateTimeToMonthTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, DatetimeToMonth)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initDateStringToMonthTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test datestring to month",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"2014-04-03", "2009-11-03", "2012-07-03", "2012-02-03 18:23:15"},
					[]bool{false, false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{4, 11, 7, 2},
				[]bool{false, false, false, false}),
		},
	}
}

func TestDateStringToMonth(t *testing.T) {
	testCases := initDateStringToMonthTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, DateStringToMonth)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
	//TODO: Ignoring Scalar Nulls: Original code: https://github.com/m-schen/matrixone/blob/823b5524f1c6eb189ee9652013bdf86b99e5571e/pkg/sql/plan/function/builtin/unary/month_test.go#L150
}

// Year

func initDateToYearTestCase() []tcTemp {
	d1, _ := types.ParseDateCast("2004-04-03")
	d2, _ := types.ParseDateCast("2014-08-03")
	d3, _ := types.ParseDateCast("2008-01-03")
	return []tcTemp{
		{
			info: "test date to year",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{d1, d2, d3},
					[]bool{false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{2004, 2014, 2008},
				[]bool{false, false, false}),
		},
	}
}

func TestDateToYear(t *testing.T) {
	testCases := initDateToYearTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, DateToYear)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initDateTimeToYearTestCase() []tcTemp {
	d1, _ := types.ParseDatetime("2004-04-03 13:11:10", 6)
	d2, _ := types.ParseDatetime("1999-08-05 11:01:02", 6)
	d3, _ := types.ParseDatetime("2004-01-03 23:15:08", 6)
	return []tcTemp{
		{
			info: "test datetime to year",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d1, d2, d3},
					[]bool{false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{2004, 1999, 2004},
				[]bool{false, false, false}),
		},
	}
}

func TestDateTimeToYear(t *testing.T) {
	testCases := initDateTimeToYearTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, DatetimeToYear)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initDateStringToYearTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test datestring to year",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"2014-04-03", "2009-11-03", "2012-07-03", "2012-02-03 18:23:15"},
					[]bool{false, false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{2014, 2009, 2012, 2012},
				[]bool{false, false, false, false}),
		},
	}
}

func TestDateStringToYear(t *testing.T) {
	testCases := initDateStringToYearTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, DateStringToYear)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
	//TODO: Ignoring Scalar Nulls: Original code:https://github.com/m-schen/matrixone/blob/e8259e975b2c256e529bf26c0ac278fe0df8e97c/pkg/sql/plan/function/builtin/unary/year_test.go#L150
}

// Week

func initDateToWeekTestCase() []tcTemp {
	d11, _ := types.ParseDateCast("2003-12-30")
	d12, _ := types.ParseDateCast("2004-01-02")
	d13, _ := types.ParseDateCast("2004-12-31")
	d14, _ := types.ParseDateCast("2005-01-01")

	d21, _ := types.ParseDateCast("2001-02-16")
	d22, _ := types.ParseDateCast("2012-06-18")
	d23, _ := types.ParseDateCast("2015-09-25")
	d24, _ := types.ParseDateCast("2022-12-05")
	return []tcTemp{
		{
			info: "test date to week - first and last week",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{d11, d12, d13, d14},
					[]bool{false, false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{1, 1, 53, 53},
				[]bool{false, false, false, false}),
		},
		{
			info: "test date to week - normal",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{d21, d22, d23, d24},
					[]bool{false, false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{7, 25, 39, 49},
				[]bool{false, false, false, false}),
		},
		{
			info: "test date to week - null",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{d11},
					[]bool{true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{0},
				[]bool{true}),
		},
		//TODO: Ignoring Scalar Nulls: Original code:https://github.com/m-schen/matrixone/blob/749eb739130decdbbf3dcc3dd5b21f656620edd9/pkg/sql/plan/function/builtin/unary/week_test.go#L52
	}
}

func TestDateToWeek(t *testing.T) {
	testCases := initDateToWeekTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, DateToWeek)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initDateTimeToWeekTestCase() []tcTemp {
	d11, _ := types.ParseDatetime("2003-12-30 13:11:10", 6)
	d12, _ := types.ParseDatetime("2004-01-02 19:22:10", 6)
	d13, _ := types.ParseDatetime("2004-12-31 00:00:00", 6)
	d14, _ := types.ParseDatetime("2005-01-01 04:05:06", 6)

	d21, _ := types.ParseDatetime("2001-02-16 13:11:10", 6)
	d22, _ := types.ParseDatetime("2012-06-18 19:22:10", 6)
	d23, _ := types.ParseDatetime("2015-09-25 00:00:00", 6)
	d24, _ := types.ParseDatetime("2022-12-05 04:05:06", 6)
	return []tcTemp{
		{
			info: "test datetime to week - first and last week",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d11, d12, d13, d14},
					[]bool{false, false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{1, 1, 53, 53},
				[]bool{false, false, false, false}),
		},
		{
			info: "test datetime to week - normal",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d21, d22, d23, d24},
					[]bool{false, false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{7, 25, 39, 49},
				[]bool{false, false, false, false}),
		},
		{
			info: "test datetime to week - null",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d11},
					[]bool{true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{1},
				[]bool{true}),
		},
	}
}

func TestDateTimeToWeek(t *testing.T) {
	testCases := initDateTimeToWeekTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, DatetimeToWeek)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
	//TODO: Ignoring Scalar Nulls: Original code:https://github.com/m-schen/matrixone/blob/749eb739130decdbbf3dcc3dd5b21f656620edd9/pkg/sql/plan/function/builtin/unary/week_test.go#L114
}

// Week day

func initDateToWeekdayTestCase() []tcTemp {
	d11, _ := types.ParseDateCast("2004-04-03")
	d12, _ := types.ParseDateCast("2021-10-03")
	d13, _ := types.ParseDateCast("2020-08-23")
	return []tcTemp{
		{
			info: "test date to weekday - first and last weekday",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{d11, d12, d13},
					[]bool{false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{5, 6, 6},
				[]bool{false, false, false}),
		},
	}
}

func TestDateToWeekday(t *testing.T) {
	testCases := initDateToWeekdayTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, DateToWeekday)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initDateTimeToWeekdayTestCase() []tcTemp {
	d11, _ := types.ParseDatetime("2004-04-03 13:11:10", 6)
	d12, _ := types.ParseDatetime("2021-10-03 15:24:18", 6)
	d13, _ := types.ParseDatetime("2020-08-23 21:53:09", 6)

	return []tcTemp{
		{
			info: "test datetime to weekday - first and last weekday",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d11, d12, d13},
					[]bool{false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{5, 6, 6},
				[]bool{false, false, false}),
		},
	}
}

func TestDateTimeToWeekday(t *testing.T) {
	testCases := initDateTimeToWeekdayTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, DatetimeToWeekday)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initPiTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test pi",
			//TODO: Validate if T_int8 is ok
			inputs: []testutil.FunctionTestInput{testutil.NewFunctionTestInput(types.T_int8.ToType(), []int8{0}, []bool{false})},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false, []float64{math.Pi}, []bool{false}),
		},
	}
}

func TestPi(t *testing.T) {
	testCases := initPiTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, Pi)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initUTCTimestampTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test UTCTimestamp",
			//TODO: Validate: Original Code: https://github.com/m-schen/matrixone/blob/9a29d4656c2c6be66885270a2a50664d3ba2a203/pkg/sql/plan/function/builtin/multi/utctimestamp_test.go#L24
			inputs: []testutil.FunctionTestInput{testutil.NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{})},
			expect: testutil.NewFunctionTestResult(types.T_datetime.ToType(), false, []types.Datetime{}, []bool{}),
		},
	}
}

func TestUTCTimestamp(t *testing.T) {
	testCases := initUTCTimestampTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, UTCTimestamp)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func TestSleep(t *testing.T) {
	testCases := []tcTemp{
		{
			info: "sleep uint64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint64.ToType(), []uint64{1}, []bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false, []uint8{0}, []bool{false}),
		},
		{
			info: "sleep uint64 with null",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint64.ToType(), []uint64{1, 0, 1}, []bool{false, true, true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), true, []uint8{0, 0, 0}, []bool{false, true, true}),
		},
	}

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, Sleep[uint64])
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}

	testCases2 := []tcTemp{
		{
			info: "sleep float64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(), []float64{0.1, 0.2}, []bool{false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false, []uint8{0, 0}, []bool{false, false}),
		},
		{
			info: "sleep float64 with null",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(), []float64{0.1, 0, 0.1}, []bool{false, true, true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), true, []uint8{0, 0, 0}, []bool{false, true, true}),
		},
		{
			info: "sleep float64 with null",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(), []float64{0.1, -1.0, 0.1}, []bool{false, false, true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), true, []uint8{0, 0, 0}, []bool{false, true, true}),
		},
	}

	for _, tc := range testCases2 {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, Sleep[float64])
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initBitCastTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test Decode Int8",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.New(types.T_varbinary, 10, 0),
					[]string{"a", "s", ""},
					[]bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{0},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{97, 115, 0},
				[]bool{false, false, true}),
		},
		{
			info: "test Decode Int16",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.New(types.T_varbinary, 10, 0),
					[]string{"as", "df", ""},
					[]bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{0},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{29537, 26212, 0},
				[]bool{false, false, true}),
		},
		{
			info: "test Decode Int32",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.New(types.T_varbinary, 10, 0),
					[]string{"asdf", "jkl;", ""},
					[]bool{false, false, true}),
				testutil.NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{0},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{1717859169, 996961130, 0},
				[]bool{false, false, true}),
		},
		{
			info: "test Decode Int64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.New(types.T_varbinary, 10, 0),
					[]string{"asdfjkl;", ""},
					[]bool{false, true}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{0},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{4281915450451063649, 0},
				[]bool{false, true}),
		},
	}
}

func TestBitCast(t *testing.T) {
	testCases := initBitCastTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, BitCast)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initSHA1TestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test sha1",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abc", "", ""},
					[]bool{false, false, true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"a9993e364706816aba3e25717850c26c9cd0d89d", "da39a3ee5e6b4b0d3255bfef95601890afd80709", ""},
				[]bool{false, false, true}),
		},
	}
}

func TestSHA1(t *testing.T) {
	testCases := initSHA1TestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, SHA1Func)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}
