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
			info: "& test prefix_in",
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
			info: "& test prefix_in with null",
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
	}

	proc := testutil.NewProcess(t)
	for _, tc := range tcs {
		fcTC := NewFunctionTestCase(proc,
			tc.inputs, tc.expect, PrefixBetween)
		s, info := fcTC.Run()
		require.True(t, s, info, tc.info)
	}
}
