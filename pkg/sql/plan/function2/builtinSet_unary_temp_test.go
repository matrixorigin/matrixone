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
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
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
	fs, readPath, err := fileservice.GetForETL(proc.FileService, filepath)
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
