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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func initJsonValidTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "json_valid with varchar input",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"a":1}`, `[1,2,3]`, `"hello"`, `hello`, `true`, `null`, `42`, ``},
					[]bool{false, false, false, false, false, false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, true, true, false, true, true, true, false},
				[]bool{false, false, false, false, false, false, false, true}),
		},
	}
}

func TestJsonLength(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test 1 arg: array, object, scalar
	t.Run("root level", func(t *testing.T) {
		tc := tcTemp{
			info: "json_length with varchar input",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`[1,2,3]`, `{"a":1,"b":2}`, `"hello"`, `{}`, `[]`},
					[]bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{3, 2, 1, 0, 0},
				[]bool{false, false, false, false, false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, JsonLength)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	// Test 2 args: with path (missing path returns NULL)
	t.Run("with path", func(t *testing.T) {
		tc := tcTemp{
			info: "json_length with path",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"a":[1,2,3]}`, `{"a":[1,2,3]}`, `{"a":1}`, `[1,2,3]`},
					[]bool{false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`$.a`, `$.b`, `$.b`, `$[0]`},
					[]bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{3, 0, 0, 1},
				[]bool{false, true, true, false}), // missing path → NULL
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, JsonLength)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	// Test null input
	t.Run("null input", func(t *testing.T) {
		tc := tcTemp{
			info: "json_length with null",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`[1]`, `[1]`, ``},
					[]bool{false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{1, 1, 0},
				[]bool{false, false, true}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, JsonLength)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})
}

func TestJsonKeys(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("object root", func(t *testing.T) {
		tc := tcTemp{
			info: "json_keys with object",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"a":1,"b":2}`, `{}`},
					[]bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{`["a","b"]`, `[]`},
				[]bool{false, false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, JsonKeys)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	t.Run("not an object", func(t *testing.T) {
		tc := tcTemp{
			info: "json_keys with array/scalar",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`[1,2]`, `"hello"`, `42`},
					[]bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{``, ``, ``},
				[]bool{true, true, true}), // all NULL
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, JsonKeys)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})
}

func TestJsonValid(t *testing.T) {
	testCases := initJsonValidTestCase()

	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc,
			tc.inputs, tc.expect, JsonValid)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}
