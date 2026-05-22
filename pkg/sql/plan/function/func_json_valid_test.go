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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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

func TestJsonPretty(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("scalar", func(t *testing.T) {
		tc := tcTemp{
			info: "json_pretty scalar",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"123", `"hello"`, "true", "null"},
					[]bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"123", `"hello"`, "true", "null"},
				[]bool{false, false, false, false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, JsonPretty)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	t.Run("empty_array", func(t *testing.T) {
		tc := tcTemp{
			info: "json_pretty empty array",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"[]"},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"[]"},
				[]bool{false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, JsonPretty)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	t.Run("empty_object", func(t *testing.T) {
		tc := tcTemp{
			info: "json_pretty empty object",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"{}"},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"{}"},
				[]bool{false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, JsonPretty)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	t.Run("null_input", func(t *testing.T) {
		tc := tcTemp{
			info: "json_pretty null",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"a":1}`, ""},
					[]bool{false, true}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"{\n  \"a\": 1\n}", ""},
				[]bool{false, true}), // null → NULL
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, JsonPretty)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})
}

func TestJsonSchemaValid(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("valid document", func(t *testing.T) {
		tc := tcTemp{
			info: "json_schema_valid valid",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"type":"object","properties":{"lat":{"type":"number","minimum":-90,"maximum":90}},"required":["lat"]}`, `{"type":"object","properties":{"lat":{"type":"number","minimum":-90,"maximum":90}},"required":["lat"]}`},
					[]bool{false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"lat":60}`, `{"lat":60}`},
					[]bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, true},
				[]bool{false, false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, JsonSchemaValid)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	t.Run("invalid document", func(t *testing.T) {
		tc := tcTemp{
			info: "json_schema_valid invalid",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"type":"object","properties":{"lat":{"type":"number","minimum":-90,"maximum":90}},"required":["lat"]}`, `{"type":"object","properties":{"lat":{"type":"number","minimum":-90,"maximum":90}},"required":["lat"]}`},
					[]bool{false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{}`, `{"lat":100}`},
					[]bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, false}, // missing required lat; lat > max
				[]bool{false, false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, JsonSchemaValid)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	t.Run("null inputs", func(t *testing.T) {
		tc := tcTemp{
			info: "json_schema_valid null",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"type":"object"}`, ``, `{"type":"object"}`},
					[]bool{false, true, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{}`, `{}`, ``},
					[]bool{false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, false},
				[]bool{false, true, true}), // null → null
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, JsonSchemaValid)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	t.Run("constant schema must be object", func(t *testing.T) {
		tc := tcTemp{
			info: "json_schema_valid constant boolean schema",
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_varchar.ToType(),
					[]string{`true`},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`42`},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), true, []bool{false}, []bool{false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, JsonSchemaValid)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	t.Run("invalid schema compile error", func(t *testing.T) {
		tc := tcTemp{
			info: "json_schema_valid invalid pattern",
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_varchar.ToType(),
					[]string{`{"type":"string","pattern":"["}`},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`"abc"`},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), true, []bool{false}, []bool{false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, JsonSchemaValid)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	t.Run("constant invalid schema with null document", func(t *testing.T) {
		tc := tcTemp{
			info: "json_schema_valid invalid const schema null document",
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_varchar.ToType(),
					[]string{`{"type":"string","pattern":"["}`},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{``},
					[]bool{true}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false, []bool{false}, []bool{true}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, JsonSchemaValid)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	t.Run("constant invalid schema report with null document", func(t *testing.T) {
		tc := tcTemp{
			info: "json_schema_validation_report invalid const schema null document",
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_varchar.ToType(),
					[]string{`{"type":"string","pattern":"["}`},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{``},
					[]bool{true}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{``}, []bool{true}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, JsonSchemaValidationReport)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})
}

func TestJsonValue(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("string value", func(t *testing.T) {
		tc := tcTemp{
			info: "json_value string",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"fname":"Joe","lname":"Palmer"}`},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`$.fname`},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"Joe"},
				[]bool{false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, JsonValue)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	t.Run("number value", func(t *testing.T) {
		tc := tcTemp{
			info: "json_value number",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"item":"shoes","price":"49.95"}`},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`$.price`},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"49.95"},
				[]bool{false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, JsonValue)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	t.Run("missing path", func(t *testing.T) {
		tc := tcTemp{
			info: "json_value missing",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"a":1}`},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`$.missing`},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{""},
				[]bool{true}), // NULL
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, JsonValue)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	t.Run("null input", func(t *testing.T) {
		tc := tcTemp{
			info: "json_value null",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"a":1}`, ``},
					[]bool{false, true}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`$.a`, `$.a`},
					[]bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"1", ""},
				[]bool{false, true}), // null → NULL
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, JsonValue)
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

func TestJsonFunctionsRespectSelectList(t *testing.T) {
	proc := testutil.NewProcess(t)
	selectList := &FunctionSelectList{
		AnyNull:    true,
		SelectList: []bool{false, true},
	}

	t.Run("json_keys", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`not json`, `{"a":1}`},
					[]bool{false, false}),
			},
			types.T_varchar.ToType(), JsonKeys, selectList)

		require.True(t, vec.IsNull(0))
		v, null := vector.GenerateFunctionStrParameter(vec).GetStrValue(1)
		require.False(t, null)
		require.Equal(t, `["a"]`, string(v))
	})

	t.Run("json_keys_with_path", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`not json`, `{"a":{"b":1}}`},
					[]bool{false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`bad path`, `$.a`},
					[]bool{false, false}),
			},
			types.T_varchar.ToType(), JsonKeys, selectList)

		require.True(t, vec.IsNull(0))
		v, null := vector.GenerateFunctionStrParameter(vec).GetStrValue(1)
		require.False(t, null)
		require.Equal(t, `["b"]`, string(v))
	})

	t.Run("json_pretty", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`not json`, `{"a":1}`},
					[]bool{false, false}),
			},
			types.T_varchar.ToType(), JsonPretty, selectList)

		require.True(t, vec.IsNull(0))
		v, null := vector.GenerateFunctionStrParameter(vec).GetStrValue(1)
		require.False(t, null)
		require.Equal(t, "{\n  \"a\": 1\n}", string(v))
	})

	t.Run("json_value", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`not json`, `{"a":1}`},
					[]bool{false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`bad path`, `$.a`},
					[]bool{false, false}),
			},
			types.T_varchar.ToType(), JsonValue, selectList)

		require.True(t, vec.IsNull(0))
		v, null := vector.GenerateFunctionStrParameter(vec).GetStrValue(1)
		require.False(t, null)
		require.Equal(t, "1", string(v))
	})

	t.Run("json_schema_valid", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`not json`, `{"type":"number"}`},
					[]bool{false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`not json`, `42`},
					[]bool{false, false}),
			},
			types.T_bool.ToType(), JsonSchemaValid, selectList)

		require.True(t, vec.IsNull(0))
		v, null := vector.GenerateFunctionFixedTypeParameter[bool](vec).GetValue(1)
		require.False(t, null)
		require.True(t, v)
	})

	t.Run("json_schema_validation_report", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`not json`, `{"type":"number"}`},
					[]bool{false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`not json`, `42`},
					[]bool{false, false}),
			},
			types.T_varchar.ToType(), JsonSchemaValidationReport, selectList)

		require.True(t, vec.IsNull(0))
		v, null := vector.GenerateFunctionStrParameter(vec).GetStrValue(1)
		require.False(t, null)
		require.Equal(t, `{"valid": true}`, string(v))
	})

	t.Run("json_array", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1, 2},
					[]bool{false, false}),
			},
			types.T_json.ToType(), newOpBuiltInJsonArray().jsonArray, selectList)

		require.True(t, vec.IsNull(0))
		require.False(t, vec.IsNull(1))
	})

	t.Run("json_object", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{``, `a`},
					[]bool{true, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1, 2},
					[]bool{false, false}),
			},
			types.T_json.ToType(), newOpBuiltInJsonObject().jsonObject, selectList)

		require.True(t, vec.IsNull(0))
		require.False(t, vec.IsNull(1))
	})
}

func runJsonFunctionWithSelectList(t *testing.T, proc *process.Process, inputs []FunctionTestInput, retType types.Type, fn fEvalFn, selectList *FunctionSelectList) *vector.Vector {
	t.Helper()
	fcTC := NewFunctionTestCase(proc, inputs, NewFunctionTestResult(retType, false, nil, nil), fn)
	require.NoError(t, fcTC.result.PreExtendAndReset(fcTC.fnLength))
	require.NoError(t, fcTC.fn(fcTC.parameters, fcTC.result, fcTC.proc, fcTC.fnLength, selectList))
	return fcTC.result.GetResultVector()
}
