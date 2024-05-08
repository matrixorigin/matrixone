// Copyright 2024 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"testing"
)

func initFormatTestCase1() []tcTemp {
	format := `%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y %%`

	d1, _ := types.ParseDatetime("2010-01-07 23:12:34.12345", 6)
	r1 := `Jan January 01 1 7th 07 7 007 23 11 12 PM 11:12:34 PM 23:12:34 34 123450 01 01 01 01 Thu Thursday 4 2010 2010 2010 10 %`

	d2, _ := types.ParseDatetime("2012-12-21 23:12:34.123456", 6)
	r2 := "Dec December 12 12 21st 21 21 356 23 11 12 PM 11:12:34 PM 23:12:34 34 123456 51 51 51 51 Fri Friday 5 2012 2012 2012 12 %"

	d3, _ := types.ParseDatetime("0001-01-01 00:00:00.123456", 6)
	r3 := `Jan January 01 1 1st 01 1 001 0 12 00 AM 12:00:00 AM 00:00:00 00 123456 00 01 53 01 Mon Monday 1 0000 0001 0001 01 %`

	d4, _ := types.ParseDatetime("2016-09-3 00:59:59.123456", 6)
	r4 := `Sep September 09 9 3rd 03 3 247 0 12 59 AM 12:59:59 AM 00:59:59 59 123456 35 35 35 35 Sat Saturday 6 2016 2016 2016 16 %`

	cases := make([]tcTemp, 100)
	for i := range cases {
		values := make([]types.Datetime, 8192)
		nullList := make([]bool, 8192)
		for j := range values {
			mod := j % 4
			if mod == 0 {
				values[j] = d1
				nullList[j] = false
			} else if mod == 1 {
				values[j] = d2
				nullList[j] = false
			} else if mod == 2 {
				values[j] = d3
				nullList[j] = false
			} else {
				values[j] = d4
				nullList[j] = false
			}
		}
		inputs := make([]testutil.FunctionTestInput, 2)
		inputs[0] = testutil.NewFunctionTestInput(types.T_datetime.ToType(), values, nullList)
		inputs[1] = testutil.NewFunctionTestConstInput(types.T_varchar.ToType(), []string{format}, []bool{false})

		results := make([]string, 8192)
		rnullList := make([]bool, 8192)
		for j := range values {
			mod := j % 4
			if mod == 0 {
				results[j] = r1
				nullList[j] = false
			} else if mod == 1 {
				results[j] = r2
				nullList[j] = false
			} else if mod == 2 {
				results[j] = r3
				nullList[j] = false
			} else {
				results[j] = r4
				nullList[j] = false
			}
		}

		expect := testutil.NewFunctionTestResult(types.T_varchar.ToType(), false, results, rnullList)

		cases[i] = tcTemp{
			info:   "test format",
			typ:    types.T_datetime,
			inputs: inputs,
			expect: expect,
		}
	}
	return cases
}

// BenchmarkDateFormat1-4   	1000000000	         2.462 ns/op
func BenchmarkDateFormat1(b *testing.B) {
	//b.N = 1000000000
	testCases := initFormatTestCase1()
	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, DateFormat)
		fcTC.BenchMarkRun()
		_, _ = fcTC.Run()
	}
}

func initFormatTestCase2() []tcTemp {
	format := `%Y,%m,%d %H:%i:%s`

	d1, _ := types.ParseDatetime("2010-01-07 23:12:34.12345", 6)
	r1 := `2010,01,07 23:12:34`

	d2, _ := types.ParseDatetime("2012-12-21 23:12:34.123456", 6)
	r2 := "2012,12,21 23:12:34"

	d3, _ := types.ParseDatetime("2021-01-01 00:00:00.123456", 6)
	r3 := `2021,01,01 00:00:00`

	d4, _ := types.ParseDatetime("2016-09-3 00:59:59.123456", 6)
	r4 := `2016,09,03 00:59:59`

	cases := make([]tcTemp, 100)
	for i := range cases {
		values := make([]types.Datetime, 8192)
		nullList := make([]bool, 8192)
		for j := range values {
			mod := j % 4
			if mod == 0 {
				values[j] = d1
				nullList[j] = false
			} else if mod == 1 {
				values[j] = d2
				nullList[j] = false
			} else if mod == 2 {
				values[j] = d3
				nullList[j] = false
			} else {
				values[j] = d4
				nullList[j] = false
			}
		}
		inputs := make([]testutil.FunctionTestInput, 2)
		inputs[0] = testutil.NewFunctionTestInput(types.T_datetime.ToType(), values, nullList)
		inputs[1] = testutil.NewFunctionTestConstInput(types.T_varchar.ToType(), []string{format}, []bool{false})

		results := make([]string, 8192)
		rnullList := make([]bool, 8192)
		for j := range values {
			mod := j % 4
			if mod == 0 {
				results[j] = r1
				nullList[j] = false
			} else if mod == 1 {
				results[j] = r2
				nullList[j] = false
			} else if mod == 2 {
				results[j] = r3
				nullList[j] = false
			} else {
				results[j] = r4
				nullList[j] = false
			}
		}

		expect := testutil.NewFunctionTestResult(types.T_varchar.ToType(), false, results, rnullList)

		cases[i] = tcTemp{
			info:   "test format",
			typ:    types.T_datetime,
			inputs: inputs,
			expect: expect,
		}
	}
	return cases
}

// BenchmarkDateFormat2-4   	1000000000	         0.3115 ns/op
func BenchmarkDateFormat2(b *testing.B) {
	//b.N = 1000000000
	testCases := initFormatTestCase2()
	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, DateFormat)
		fcTC.BenchMarkRun()
		_, _ = fcTC.Run()
	}
}

func initFormatTestCase3() []tcTemp {
	format := `%Y-%m-%d`

	d1, _ := types.ParseDatetime("2010-01-07 23:12:34.12345", 6)
	r1 := `2010-01-07`

	d2, _ := types.ParseDatetime("2012-12-21 23:12:34.123456", 6)
	r2 := "2012-12-21"

	d3, _ := types.ParseDatetime("2021-01-01 00:00:00.123456", 6)
	r3 := `2021-01-01`

	d4, _ := types.ParseDatetime("2016-09-3 00:59:59.123456", 6)
	r4 := `2016-09-03`

	cases := make([]tcTemp, 100)
	for i := range cases {
		values := make([]types.Datetime, 8192)
		nullList := make([]bool, 8192)
		for j := range values {
			mod := j % 4
			if mod == 0 {
				values[j] = d1
				nullList[j] = false
			} else if mod == 1 {
				values[j] = d2
				nullList[j] = false
			} else if mod == 2 {
				values[j] = d3
				nullList[j] = false
			} else {
				values[j] = d4
				nullList[j] = false
			}
		}
		inputs := make([]testutil.FunctionTestInput, 2)
		inputs[0] = testutil.NewFunctionTestInput(types.T_datetime.ToType(), values, nullList)
		inputs[1] = testutil.NewFunctionTestConstInput(types.T_varchar.ToType(), []string{format}, []bool{false})

		results := make([]string, 8192)
		rnullList := make([]bool, 8192)
		for j := range values {
			mod := j % 4
			if mod == 0 {
				results[j] = r1
				nullList[j] = false
			} else if mod == 1 {
				results[j] = r2
				nullList[j] = false
			} else if mod == 2 {
				results[j] = r3
				nullList[j] = false
			} else {
				results[j] = r4
				nullList[j] = false
			}
		}

		expect := testutil.NewFunctionTestResult(types.T_varchar.ToType(), false, results, rnullList)

		cases[i] = tcTemp{
			info:   "test format",
			typ:    types.T_datetime,
			inputs: inputs,
			expect: expect,
		}
	}
	return cases
}

// BenchmarkDateFormat3-4   	1000000000	         0.1523 ns/op
func BenchmarkDateFormat3(b *testing.B) {
	//b.N = 1000000000
	testCases := initFormatTestCase3()
	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, DateFormat)
		fcTC.BenchMarkRun()
		_, _ = fcTC.Run()
	}
}

func initFormatTestCase4() []tcTemp {
	format := `%Y/%m/%d`

	d1, _ := types.ParseDatetime("2010-01-07 23:12:34.12345", 6)
	r1 := `2010/01/07`

	d2, _ := types.ParseDatetime("2012-12-21 23:12:34.123456", 6)
	r2 := "2012/12/21"

	d3, _ := types.ParseDatetime("2021-01-01 00:00:00.123456", 6)
	r3 := `2021/01/01`

	d4, _ := types.ParseDatetime("2016-09-3 00:59:59.123456", 6)
	r4 := `2016/09/03`

	cases := make([]tcTemp, 100)
	for i := range cases {
		values := make([]types.Datetime, 8192)
		nullList := make([]bool, 8192)
		for j := range values {
			mod := j % 4
			if mod == 0 {
				values[j] = d1
				nullList[j] = false
			} else if mod == 1 {
				values[j] = d2
				nullList[j] = false
			} else if mod == 2 {
				values[j] = d3
				nullList[j] = false
			} else {
				values[j] = d4
				nullList[j] = false
			}
		}
		inputs := make([]testutil.FunctionTestInput, 2)
		inputs[0] = testutil.NewFunctionTestInput(types.T_datetime.ToType(), values, nullList)
		inputs[1] = testutil.NewFunctionTestConstInput(types.T_varchar.ToType(), []string{format}, []bool{false})

		results := make([]string, 8192)
		rnullList := make([]bool, 8192)
		for j := range values {
			mod := j % 4
			if mod == 0 {
				results[j] = r1
				nullList[j] = false
			} else if mod == 1 {
				results[j] = r2
				nullList[j] = false
			} else if mod == 2 {
				results[j] = r3
				nullList[j] = false
			} else {
				results[j] = r4
				nullList[j] = false
			}
		}

		expect := testutil.NewFunctionTestResult(types.T_varchar.ToType(), false, results, rnullList)

		cases[i] = tcTemp{
			info:   "test format",
			typ:    types.T_datetime,
			inputs: inputs,
			expect: expect,
		}
	}
	return cases
}

// BenchmarkDateFormat4-4   	1000000000	         0.1541 ns/op
func BenchmarkDateFormat4(b *testing.B) {
	//b.N = 1000000000
	testCases := initFormatTestCase4()
	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, DateFormat)
		fcTC.BenchMarkRun()
		_, _ = fcTC.Run()
	}
}

func initFormatTestCase5() []tcTemp {
	format := `%Y-%m-%d %H:%i:%s`

	d1, _ := types.ParseDatetime("2010-01-07 23:12:34.12345", 6)
	r1 := `2010-01-07 23:12:34`

	d2, _ := types.ParseDatetime("2012-12-21 23:12:34.123456", 6)
	r2 := "2012-12-21 23:12:34"

	d3, _ := types.ParseDatetime("2021-01-01 00:00:00.123456", 6)
	r3 := `2021-01-01 00:00:00`

	d4, _ := types.ParseDatetime("2016-09-3 00:59:59.123456", 6)
	r4 := `2016-09-03 00:59:59`

	cases := make([]tcTemp, 100)
	for i := range cases {
		values := make([]types.Datetime, 8192)
		nullList := make([]bool, 8192)
		for j := range values {
			mod := j % 4
			if mod == 0 {
				values[j] = d1
				nullList[j] = false
			} else if mod == 1 {
				values[j] = d2
				nullList[j] = false
			} else if mod == 2 {
				values[j] = d3
				nullList[j] = false
			} else {
				values[j] = d4
				nullList[j] = false
			}
		}
		inputs := make([]testutil.FunctionTestInput, 2)
		inputs[0] = testutil.NewFunctionTestInput(types.T_datetime.ToType(), values, nullList)
		inputs[1] = testutil.NewFunctionTestConstInput(types.T_varchar.ToType(), []string{format}, []bool{false})

		results := make([]string, 8192)
		rnullList := make([]bool, 8192)
		for j := range values {
			mod := j % 4
			if mod == 0 {
				results[j] = r1
				nullList[j] = false
			} else if mod == 1 {
				results[j] = r2
				nullList[j] = false
			} else if mod == 2 {
				results[j] = r3
				nullList[j] = false
			} else {
				results[j] = r4
				nullList[j] = false
			}
		}

		expect := testutil.NewFunctionTestResult(types.T_varchar.ToType(), false, results, rnullList)

		cases[i] = tcTemp{
			info:   "test format",
			typ:    types.T_datetime,
			inputs: inputs,
			expect: expect,
		}
	}
	return cases
}

// BenchmarkDateFormat5-4   	1000000000	         0.2022 ns/op
func BenchmarkDateFormat5(b *testing.B) {
	//b.N = 1000000000
	testCases := initFormatTestCase5()
	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, DateFormat)
		fcTC.BenchMarkRun()
		_, _ = fcTC.Run()
	}
}

func initFormatTestCase6() []tcTemp {
	format := `%Y/%m/%d %H:%i:%s`

	d1, _ := types.ParseDatetime("2010-01-07 23:12:34.12345", 6)
	r1 := `2010/01/07 23:12:34`

	d2, _ := types.ParseDatetime("2012-12-21 23:12:34.123456", 6)
	r2 := "2012/12/21 23:12:34"

	d3, _ := types.ParseDatetime("2021-01-01 00:00:00.123456", 6)
	r3 := `2021/01/01 00:00:00`

	d4, _ := types.ParseDatetime("2016-09-3 00:59:59.123456", 6)
	r4 := `2016/09/03 00:59:59`

	cases := make([]tcTemp, 100)
	for i := range cases {
		values := make([]types.Datetime, 8192)
		nullList := make([]bool, 8192)
		for j := range values {
			mod := j % 4
			if mod == 0 {
				values[j] = d1
				nullList[j] = false
			} else if mod == 1 {
				values[j] = d2
				nullList[j] = false
			} else if mod == 2 {
				values[j] = d3
				nullList[j] = false
			} else {
				values[j] = d4
				nullList[j] = false
			}
		}
		inputs := make([]testutil.FunctionTestInput, 2)
		inputs[0] = testutil.NewFunctionTestInput(types.T_datetime.ToType(), values, nullList)
		inputs[1] = testutil.NewFunctionTestConstInput(types.T_varchar.ToType(), []string{format}, []bool{false})

		results := make([]string, 8192)
		rnullList := make([]bool, 8192)
		for j := range values {
			mod := j % 4
			if mod == 0 {
				results[j] = r1
				nullList[j] = false
			} else if mod == 1 {
				results[j] = r2
				nullList[j] = false
			} else if mod == 2 {
				results[j] = r3
				nullList[j] = false
			} else {
				results[j] = r4
				nullList[j] = false
			}
		}

		expect := testutil.NewFunctionTestResult(types.T_varchar.ToType(), false, results, rnullList)

		cases[i] = tcTemp{
			info:   "test format",
			typ:    types.T_datetime,
			inputs: inputs,
			expect: expect,
		}
	}
	return cases
}

// BenchmarkDateFormat6-4   	1000000000	         0.1927 ns/op
func BenchmarkDateFormat6(b *testing.B) {
	//b.N = 1000000000
	testCases := initFormatTestCase6()
	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, DateFormat)
		fcTC.BenchMarkRun()
		_, _ = fcTC.Run()
	}
}

//DATE_FORMAT string      Formatted date
// %d/%m/%Y	               22/04/2021
// %Y%m%d	               20210422
// %Y                      2021
// %Y-%m-%d	               2021-04-22
// %Y-%m-%d %H:%i:%s       2004-04-03 13:11:10
// %Y/%m/%d                2010/01/07
// %Y/%m/%d %H:%i:%s       2010/01/07 23:12:34

func TestDateFormat(t *testing.T) {
	testCases1 := initFormatTestCase1()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases1 {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, DateFormat)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}

	testCases2 := initFormatTestCase2()
	// do the test work.
	for _, tc := range testCases2 {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, DateFormat)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}

	testCases3 := initFormatTestCase3()
	// do the test work.
	for _, tc := range testCases3 {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, DateFormat)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}

	testCases4 := initFormatTestCase4()
	// do the test work.
	for _, tc := range testCases4 {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, DateFormat)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}

	testCases5 := initFormatTestCase5()
	// do the test work.
	for _, tc := range testCases5 {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, DateFormat)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}

	testCases6 := initFormatTestCase6()
	// do the test work.
	for _, tc := range testCases6 {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, DateFormat)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}
