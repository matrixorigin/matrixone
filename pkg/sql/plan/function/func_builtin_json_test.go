// Copyright 2025 Matrix Origin
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

func TestJsonLengthCheckFn(t *testing.T) {
	// Valid: 1 arg (json)
	ret := jsonLengthCheckFn(nil, []types.Type{types.T_json.ToType()})
	require.Equal(t, succeedMatched, ret.status)

	// Valid: 1 arg (varchar)
	ret = jsonLengthCheckFn(nil, []types.Type{types.T_varchar.ToType()})
	require.Equal(t, succeedMatched, ret.status)

	// Valid: 2 args (json + varchar path)
	ret = jsonLengthCheckFn(nil, []types.Type{types.T_json.ToType(), types.T_varchar.ToType()})
	require.Equal(t, succeedMatched, ret.status)

	// Valid: 2 args with cast (int -> varchar cast for first arg)
	ret = jsonLengthCheckFn(nil, []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()})
	require.Equal(t, succeedWithCast, ret.status)

	// Invalid: 0 args
	ret = jsonLengthCheckFn(nil, []types.Type{})
	require.Equal(t, failedFunctionParametersWrong, ret.status)

	// Invalid: first arg cannot cast to varchar
	ret = jsonLengthCheckFn(nil, []types.Type{types.T_geometry.ToType()})
	require.Equal(t, failedFunctionParametersWrong, ret.status)

	// Valid: 2 args with second arg castable to varchar (succeedWithCast on path)
	ret = jsonLengthCheckFn(nil, []types.Type{types.T_json.ToType(), types.T_int64.ToType()})
	require.Equal(t, succeedWithCast, ret.status)

	// Invalid: second arg cannot cast to varchar
	ret = jsonLengthCheckFn(nil, []types.Type{types.T_json.ToType(), types.T_geometry.ToType()})
	require.Equal(t, failedFunctionParametersWrong, ret.status)

	// Invalid: 3 args
	ret = jsonLengthCheckFn(nil, []types.Type{types.T_json.ToType(), types.T_varchar.ToType(), types.T_int64.ToType()})
	require.Equal(t, failedFunctionParametersWrong, ret.status)
}

func TestJsonLengthOperator(t *testing.T) {
	proc := testutil.NewProcess(t)

	// json_length with object: {"a":1,"b":2} → 2
	{
		tc := tcTemp{
			info: "json_length with object: {\"a\":1,\"b\":2} -> 2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"a":1,"b":2}`},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{2},
				[]bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, jsonLength)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// json_length with empty object: {} → 0
	{
		tc := tcTemp{
			info: "json_length with empty object: {} -> 0",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{}`},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{0},
				[]bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, jsonLength)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// json_length with array: [1,2,3] → 3
	{
		tc := tcTemp{
			info: "json_length with array: [1,2,3] -> 3",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`[1,2,3]`},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{3},
				[]bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, jsonLength)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// json_length with empty array: [] → 0
	{
		tc := tcTemp{
			info: "json_length with empty array: [] -> 0",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`[]`},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{0},
				[]bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, jsonLength)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// json_length with scalar string: "hello" → 1
	{
		tc := tcTemp{
			info: "json_length with scalar string: \"hello\" -> 1",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`"hello"`},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{1},
				[]bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, jsonLength)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// json_length with scalar number: 42 → 1
	{
		tc := tcTemp{
			info: "json_length with scalar number: 42 -> 1",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`42`},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{1},
				[]bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, jsonLength)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// json_length with boolean true: true → 1
	{
		tc := tcTemp{
			info: "json_length with boolean true: true -> 1",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`true`},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{1},
				[]bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, jsonLength)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// json_length with boolean false: false → 1
	{
		tc := tcTemp{
			info: "json_length with boolean false: false -> 1",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`false`},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{1},
				[]bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, jsonLength)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// json_length with JSON null: null → NULL
	{
		tc := tcTemp{
			info: "json_length with JSON null: null -> NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`null`},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{0},
				[]bool{true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, jsonLength)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// json_length with SQL NULL → NULL
	{
		tc := tcTemp{
			info: "json_length with SQL NULL -> NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""},
					[]bool{true}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{0},
				[]bool{true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, jsonLength)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// json_length with path extraction: {"a":{"b":[1,2,3]}}, $.a.b → 3
	{
		tc := tcTemp{
			info: "json_length with path extraction: {\"a\":{\"b\":[1,2,3]}}, $.a.b -> 3",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"a":{"b":[1,2,3]}}`},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`$.a.b`},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{3},
				[]bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, jsonLength)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// json_length with path not found: {"a":1}, $.x → NULL
	{
		tc := tcTemp{
			info: "json_length with path not found: {\"a\":1}, $.x -> NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"a":1}`},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`$.x`},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{0},
				[]bool{true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, jsonLength)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// json_length with invalid JSON string → error
	{
		tc := tcTemp{
			info: "json_length with invalid JSON string -> error",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`not-json`},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), true,
				nil,
				nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, jsonLength)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
	// json_length with wildcard path: {"a":{"x":1,"y":2}}, $.a.* → 2
	{
		tc := tcTemp{
			info: "json_length with wildcard path: {\"a\":{\"x\":1,\"y\":2}}, $.a.* -> 2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"a":{"x":1,"y":2}}`},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`$.a.*`},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{2},
				[]bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, jsonLength)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}
