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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestPrefixEq(t *testing.T) {
	tcs := []tcTemp{
		{
			info: "& test prefix_eq",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa12", "bb22", "aa33", "aa44"}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa"}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, true, true}, []bool{false, false, false, false}),
		},
		{
			info: "& test prefix_eq with null",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa12", "bb22", "aa33", "aa44"}, []bool{false, false, true, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa"}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, false, true}, []bool{false, false, true, false}),
		},
		{
			info: "& test prefix_eq with empty prefix",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa12", "bb22", "cc33", "dd44"}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, true, true, true}, []bool{false, false, false, false}),
		},
		{
			info: "& test prefix_eq with single char prefix",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"a1", "a2", "b3", "b4"}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"a"}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, true, false, false}, []bool{false, false, false, false}),
		},
		{
			info: "& test prefix_eq all no match",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"xx11", "yy22", "zz33", "ww44"}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa"}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, false, false, false}, []bool{false, false, false, false}),
		},
		{
			info: "& test prefix_eq with exact match",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa", "bb", "cc", "dd"}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa"}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, false, false}, []bool{false, false, false, false}),
		},
	}

	proc := testutil.NewProcess(t)
	for _, tc := range tcs {
		fcTC := NewFunctionTestCase(proc,
			tc.inputs, tc.expect, PrefixEq)
		s, info := fcTC.Run()
		require.True(t, s, info, tc.info)
	}
}

func TestPrefixIn(t *testing.T) {
	tcs := []tcTemp{
		{
			info: "& test prefix_in",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa12", "ab23", "bb34"}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa", "bb", "ss"}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, true}, []bool{false, false, false}),
		},
		{
			info: "& test prefix_in with null",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa12", "ab23", "bb34"}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa", "bb", "ss"}, []bool{false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, false}, []bool{false, false, true}),
		},
		{
			info: "& test prefix_in with multiple prefixes match",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abc123", "def456", "abx789", "defx000"}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"ab", "def"}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, true, true, true}, []bool{false, false, false, false}),
		},
		{
			info: "& test prefix_in no match",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"xyz", "uvw", "rst", "opq"}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa", "bb", "cc"}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, false, false, false}, []bool{false, false, false, false}),
		},
		{
			info: "& test prefix_in with single prefix",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"a123", "b456", "a789"}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"a"}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, true}, []bool{false, false, false}),
		},
	}

	proc := testutil.NewProcess(t)
	for _, tc := range tcs {
		fcTC := NewFunctionTestCase(proc,
			tc.inputs, tc.expect, newImplPrefixIn().doPrefixIn)
		s, info := fcTC.Run()
		require.True(t, s, info, tc.info)
	}
}

func TestPrefixBetween(t *testing.T) {
	tcs := []tcTemp{
		{
			info: "& test prefix_between",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa11", "aa22", "bb22", "ss34"}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa"}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"kk"}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, true, true, false}, []bool{false, false, false, false}),
		},
		{
			info: "& test prefix_between with null",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa11", "aa22", "bb22", "ss34"}, []bool{false, true, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa"}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"kk"}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, true, false}, []bool{false, true, false, false}),
		},
		{
			info: "& test prefix_between with equal bounds",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa11", "aa22", "bb22", "ss34"}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa"}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa"}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, true, false, false}, []bool{false, false, false, false}),
		},
		{
			info: "& test prefix_between all out of range",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"zz11", "zz22", "zz33", "zz44"}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa"}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"mm"}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, false, false, false}, []bool{false, false, false, false}),
		},
		{
			info: "& test prefix_between all in range",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"mm11", "mm22", "mm33", "mm44"}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa"}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"zz"}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, true, true, true}, []bool{false, false, false, false}),
		},
	}

	proc := testutil.NewProcess(t)
	for _, tc := range tcs {
		fcTC := NewFunctionTestCase(proc,
			tc.inputs, tc.expect, PrefixBetween)
		s, info := fcTC.Run()
		require.True(t, s, info, tc.info)
	}
}

func TestPrefixInRange(t *testing.T) {
	tcs := []tcTemp{
		{
			info: "& test prefix_in_range []",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa11", "aa22", "bb22", "ss34"}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa"}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"kk"}, []bool{false}),
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{0}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, true, true, false}, []bool{false, false, false, false}),
		},
		{
			info: "& test prefix_in_range [] with null",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa11", "aa22", "bb22", "ss34"}, []bool{false, true, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa"}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"kk"}, []bool{false}),
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{0}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, true, false}, []bool{false, true, false, false}),
		},
		{
			info: "& test prefix_in_range ()",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa11", "aa22", "bb22", "ss34"}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa"}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"kk"}, []bool{false}),
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{3}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, false, true, false}, []bool{false, false, false, false}),
		},
		{
			info: "& test prefix_in_range () with null",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa11", "aa22", "bb22", "ss34"}, []bool{false, true, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa"}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"kk"}, []bool{false}),
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{3}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, false, true, false}, []bool{false, true, false, false}),
		},
		{
			info: "& test prefix_in_range (] flag=1",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa11", "aa22", "bb22", "ss34"}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa"}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"kk"}, []bool{false}),
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{1}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, false, true, false}, []bool{false, false, false, false}),
		},
		{
			info: "& test prefix_in_range [) flag=2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa11", "aa22", "bb22", "ss34"}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa"}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"kk"}, []bool{false}),
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{2}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, true, true, false}, []bool{false, false, false, false}),
		},
		{
			info: "& test prefix_in_range with equal bounds",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa11", "aa22", "bb22", "ss34"}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa"}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa"}, []bool{false}),
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{0}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, true, false, false}, []bool{false, false, false, false}),
		},
	}

	proc := testutil.NewProcess(t)
	for _, tc := range tcs {
		fcTC := NewFunctionTestCase(proc,
			tc.inputs, tc.expect, PrefixInRange)
		s, info := fcTC.Run()
		require.True(t, s, info, tc.info)
	}
}

func TestPrefixEqEmptyInput(t *testing.T) {
	tcs := []tcTemp{
		{
			info: "& test prefix_eq all nulls",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa12", "bb22", "cc33", "dd44"}, []bool{true, true, true, true}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa"}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, false, false, false}, []bool{true, true, true, true}),
		},
	}

	proc := testutil.NewProcess(t)
	for _, tc := range tcs {
		fcTC := NewFunctionTestCase(proc,
			tc.inputs, tc.expect, PrefixEq)
		s, info := fcTC.Run()
		require.True(t, s, info, tc.info)
	}
}

func TestPrefixInAdvanced(t *testing.T) {
	tcs := []tcTemp{
		{
			info: "& test prefix_in with overlapping prefixes",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abc", "abcd", "abcde", "xyz"}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abc", "abcd"}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, true, true, false}, []bool{false, false, false, false}),
		},
		{
			info: "& test prefix_in multiple nulls",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa12", "bb22", "cc33"}, []bool{true, true, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa", "bb", "cc"}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, false, true}, []bool{true, true, false}),
		},
	}

	proc := testutil.NewProcess(t)
	for _, tc := range tcs {
		fcTC := NewFunctionTestCase(proc,
			tc.inputs, tc.expect, newImplPrefixIn().doPrefixIn)
		s, info := fcTC.Run()
		require.True(t, s, info, tc.info)
	}
}

func TestPrefixBetweenReversed(t *testing.T) {
	tcs := []tcTemp{
		{
			info: "& test prefix_between reversed bounds",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa11", "mm22", "zz33", "ss34"}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"zz"}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa"}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, false, false, false}, []bool{false, false, false, false}),
		},
	}

	proc := testutil.NewProcess(t)
	for _, tc := range tcs {
		fcTC := NewFunctionTestCase(proc,
			tc.inputs, tc.expect, PrefixBetween)
		s, info := fcTC.Run()
		require.True(t, s, info, tc.info)
	}
}

func TestPrefixInRangeAdvanced(t *testing.T) {
	tcs := []tcTemp{
		{
			info: "& test prefix_in_range reversed bounds with flag=0",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"mm11", "mm22", "mm33"}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"zz"}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa"}, []bool{false}),
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{0}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, false, false}, []bool{false, false, false}),
		},
		{
			info: "& test prefix_in_range all match with flag=2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"mm11", "mm22", "mm33"}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa"}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"zz"}, []bool{false}),
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{2}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, true, true}, []bool{false, false, false}),
		},
		{
			info: "& test prefix_in_range with multiple nulls",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"aa11", "bb22", "cc33"}, []bool{true, false, true}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"a"}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"c"}, []bool{false}),
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{0}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, true, false}, []bool{true, false, true}),
		},
	}

	proc := testutil.NewProcess(t)
	for _, tc := range tcs {
		fcTC := NewFunctionTestCase(proc,
			tc.inputs, tc.expect, PrefixInRange)
		s, info := fcTC.Run()
		require.True(t, s, info, tc.info)
	}
}
