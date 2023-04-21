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

// Hex

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

// Length

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
