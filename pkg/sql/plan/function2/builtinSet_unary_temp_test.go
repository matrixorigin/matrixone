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

package function2

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

type tcTemp struct {
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

func TestJsonQuoteOfYear(t *testing.T) {
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
