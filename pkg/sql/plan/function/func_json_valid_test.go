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
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
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
			expect: NewFunctionTestResult(types.T_json.ToType(), false,
				[]string{mustJsonBinaryString(t, `["a","b"]`), mustJsonBinaryString(t, `[]`)},
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
			expect: NewFunctionTestResult(types.T_json.ToType(), false,
				[]string{``, ``, ``},
				[]bool{true, true, true}), // all NULL
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, JsonKeys)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	t.Run("nested composition returns json", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"a":1,"b":2}`},
					[]bool{false}),
			},
			types.T_json.ToType(), JsonKeys, nil)
		require.Equal(t, `["a", "b"]`, jsonVectorRowString(t, vec, 0))

		composed := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"a":1,"b":2}`},
					[]bool{false}),
			},
			types.T_json.ToType(), func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
				keysVec := runJsonFunctionWithSelectList(t, proc,
					[]FunctionTestInput{
						NewFunctionTestInput(types.T_varchar.ToType(),
							[]string{`{"a":1,"b":2}`},
							[]bool{false}),
					},
					types.T_json.ToType(), JsonKeys, nil)
				return newOpBuiltInJsonArray().jsonArray([]*vector.Vector{keysVec}, result, proc, length, selectList)
			}, nil)
		require.Equal(t, `[["a", "b"]]`, jsonVectorRowString(t, composed, 0))
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
			expect: NewFunctionTestResult(types.T_json.ToType(), false, []string{``}, []bool{true}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, JsonSchemaValidationReport)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})
}

func TestJsonSchemaMixedOverloads(t *testing.T) {
	ctx := context.Background()
	for _, fn := range []string{"json_schema_valid", "json_schema_validation_report"} {
		for _, args := range [][]types.Type{
			{types.T_varchar.ToType(), types.T_json.ToType()},
			{types.T_json.ToType(), types.T_varchar.ToType()},
		} {
			res, err := GetFunctionByName(ctx, fn, args)
			require.NoError(t, err)
			_, shouldCast := res.ShouldDoImplicitTypeCast()
			require.False(t, shouldCast)
		}
	}

	proc := testutil.NewProcess(t)
	t.Run("valid varchar schema json document", func(t *testing.T) {
		tc := tcTemp{
			info: "json_schema_valid varchar json",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"type":"number"}`},
					[]bool{false}),
				NewFunctionTestInput(types.T_json.ToType(),
					[]string{mustJsonBinaryString(t, `42`)},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false, []bool{true}, []bool{false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, JsonSchemaValid)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	t.Run("valid json schema varchar document", func(t *testing.T) {
		tc := tcTemp{
			info: "json_schema_valid json varchar",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(),
					[]string{mustJsonBinaryString(t, `{"type":"object","required":["a"]}`)},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{}`},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false, []bool{false}, []bool{false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, JsonSchemaValid)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	t.Run("report varchar schema json document", func(t *testing.T) {
		tc := tcTemp{
			info: "json_schema_validation_report varchar json",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"type":"number"}`},
					[]bool{false}),
				NewFunctionTestInput(types.T_json.ToType(),
					[]string{mustJsonBinaryString(t, `42`)},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_json.ToType(), false, []string{mustJsonBinaryString(t, `{"valid": true}`)}, []bool{false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, JsonSchemaValidationReport)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	t.Run("report json schema varchar document", func(t *testing.T) {
		tc := tcTemp{
			info: "json_schema_validation_report json varchar",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(),
					[]string{mustJsonBinaryString(t, `{"type":"object","required":["a"]}`)},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"a":1}`},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_json.ToType(), false, []string{mustJsonBinaryString(t, `{"valid": true}`)}, []bool{false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, JsonSchemaValidationReport)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})
}

func TestJsonSchemaValidationReportReturnTypeAndContract(t *testing.T) {
	ctx := context.Background()
	for _, args := range [][]types.Type{
		{types.T_varchar.ToType(), types.T_varchar.ToType()},
		{types.T_json.ToType(), types.T_json.ToType()},
		{types.T_varchar.ToType(), types.T_json.ToType()},
		{types.T_json.ToType(), types.T_varchar.ToType()},
	} {
		res, err := GetFunctionByName(ctx, "json_schema_validation_report", args)
		require.NoError(t, err)
		require.Equal(t, types.T_json, res.GetReturnType().Oid)
	}

	proc := testutil.NewProcess(t)
	vec := runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`{"type":"object","required":["name"]}`},
				[]bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`{}`},
				[]bool{false}),
		},
		types.T_json.ToType(), JsonSchemaValidationReport, nil)
	require.Equal(t, `{"document-location": "$", "reason": "name is required", "schema-failed-keyword": "required", "schema-location": "#/required", "valid": false}`, jsonVectorRowString(t, vec, 0))
}

func TestJsonKeysReturnType(t *testing.T) {
	ctx := context.Background()
	for _, args := range [][]types.Type{
		{types.T_json.ToType()},
		{types.T_varchar.ToType()},
		{types.T_json.ToType(), types.T_varchar.ToType()},
		{types.T_varchar.ToType(), types.T_varchar.ToType()},
	} {
		res, err := GetFunctionByName(ctx, "json_keys", args)
		require.NoError(t, err)
		require.Equal(t, types.T_json, res.GetReturnType().Oid)
	}
}

func TestJsonSchemaRefKeywordDetection(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("property named ref is allowed", func(t *testing.T) {
		tc := tcTemp{
			info: "json_schema_valid property named ref",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"type":"object","properties":{"$ref":{"type":"string"}},"required":["$ref"]}`},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"$ref":"ok"}`},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false, []bool{true}, []bool{false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, JsonSchemaValid)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	t.Run("enum value with ref key is allowed", func(t *testing.T) {
		tc := tcTemp{
			info: "json_schema_valid enum ref value",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"enum":[{"$ref":"literal"}]}`},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"$ref":"literal"}`},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false, []bool{true}, []bool{false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, JsonSchemaValid)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	t.Run("schema ref keyword is rejected", func(t *testing.T) {
		tc := tcTemp{
			info: "json_schema_valid ref keyword",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"properties":{"a":{"$ref":"#/$defs/a"}},"$defs":{"a":{"type":"string"}}}`},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"a":"ok"}`},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), true, []bool{false}, []bool{false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, JsonSchemaValid)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})
}

func TestJsonConstructorTypeChecking(t *testing.T) {
	ctx := context.Background()
	_, err := GetFunctionByName(ctx, "json_array", []types.Type{types.T_datalink.ToType()})
	require.Error(t, err)

	_, err = GetFunctionByName(ctx, "json_object", []types.Type{types.T_varchar.ToType(), types.T_datalink.ToType()})
	require.Error(t, err)

	_, err = GetFunctionByName(ctx, "json_array", []types.Type{types.T_timestamp.ToType()})
	require.NoError(t, err)

	_, err = GetFunctionByName(ctx, "json_object", []types.Type{types.T_varchar.ToType(), types.T_timestamp.ToType()})
	require.NoError(t, err)
}

func TestJsonConstructorsTimestampUseSessionTimeZone(t *testing.T) {
	proc := testutil.NewProcess(t)
	loc := time.FixedZone("UTC+8", 8*60*60)
	proc.GetSessionInfo().TimeZone = loc

	ts, err := types.ParseTimestamp(loc, "2022-01-01 01:02:03.123456", 6)
	require.NoError(t, err)
	tsType := types.T_timestamp.ToType()
	tsType.Scale = 6

	t.Run("json_array", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(tsType, []types.Timestamp{ts}, []bool{false}),
			},
			types.T_json.ToType(), newOpBuiltInJsonArray().jsonArray, nil)
		require.Equal(t, `["2022-01-01 01:02:03.123456"]`, jsonVectorRowString(t, vec, 0))
	})

	t.Run("json_object", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"ts"}, []bool{false}),
				NewFunctionTestInput(tsType, []types.Timestamp{ts}, []bool{false}),
			},
			types.T_json.ToType(), newOpBuiltInJsonObject().jsonObject, nil)
		require.Equal(t, `{"ts": "2022-01-01 01:02:03.123456"}`, jsonVectorRowString(t, vec, 0))
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

	t.Run("object and array matches return null", func(t *testing.T) {
		tc := tcTemp{
			info: "json_value non scalar",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"a":[1]}`, `{"a":{"b":1}}`, `{"a":true}`},
					[]bool{false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`$.a`, `$.a`, `$.a`},
					[]bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"", "", "true"},
				[]bool{true, true, false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, JsonValue)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	t.Run("reject non simple path", func(t *testing.T) {
		tc := tcTemp{
			info: "json_value reject wildcard path",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"a":[1,2],"b":[3,4]}`},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`$.*`},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), true, nil, nil),
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

func TestJsonExtractFloat64UnsignedInteger(t *testing.T) {
	proc := testutil.NewProcess(t)
	decimalArray, err := bytejson.CreateByteJSON([]any{newTypedByteJson(bytejson.TpCodeDecimal, "123.45")})
	require.NoError(t, err)
	decimalArrayData, err := decimalArray.Marshal()
	require.NoError(t, err)

	vec := runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_json.ToType(),
				[]string{
					mustJsonBinaryString(t, `[18446744073709551615]`),
					mustJsonBinaryString(t, `[9223372036854775808]`),
					string(decimalArrayData),
				},
				[]bool{false, false, false}),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{`$[0]`}, []bool{false}),
		},
		types.T_float64.ToType(), newOpBuiltInJsonExtract().jsonExtractFloat64, nil)

	p := vector.GenerateFunctionFixedTypeParameter[float64](vec)
	v, null := p.GetValue(0)
	require.False(t, null)
	require.Equal(t, float64(^uint64(0)), v)
	v, null = p.GetValue(1)
	require.False(t, null)
	require.Equal(t, float64(uint64(1)<<63), v)
	v, null = p.GetValue(2)
	require.False(t, null)
	require.Equal(t, 123.45, v)
}

func TestJsonExtractFloat64IgnoreAllRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	selectList := &FunctionSelectList{AllNull: true}
	vec := runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`not json`, `still not json`},
				[]bool{false, false}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`bad path`, `also bad path`},
				[]bool{false, false}),
		},
		types.T_float64.ToType(), newOpBuiltInJsonExtract().jsonExtractFloat64, selectList)

	require.True(t, vec.IsNull(0))
	require.True(t, vec.IsNull(1))
}

func TestJsonExtractConstNullPath(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("json_extract", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`{"a":1}`}, []bool{false}),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{""}, []bool{true}),
			},
			types.T_json.ToType(), newOpBuiltInJsonExtract().jsonExtract, nil)
		require.True(t, vec.IsNull(0))
	})

	t.Run("json_extract_string", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`{"a":"x"}`}, []bool{false}),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{""}, []bool{true}),
			},
			types.T_varchar.ToType(), newOpBuiltInJsonExtract().jsonExtractString, nil)
		require.True(t, vec.IsNull(0))
	})

	t.Run("json_extract_float64", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`{"a":1}`}, []bool{false}),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{""}, []bool{true}),
			},
			types.T_float64.ToType(), newOpBuiltInJsonExtract().jsonExtractFloat64, nil)
		require.True(t, vec.IsNull(0))
	})
}

func TestJsonExtractPreservesJSONNull(t *testing.T) {
	proc := testutil.NewProcess(t)

	tests := []struct {
		name string
		typ  types.Type
		docs []string
	}{
		{
			name: "varchar input",
			typ:  types.T_varchar.ToType(),
			docs: []string{`{"a":null}`, `{}`, ``},
		},
		{
			name: "json input",
			typ:  types.T_json.ToType(),
			docs: []string{
				mustJsonBinaryString(t, `{"a":null}`),
				mustJsonBinaryString(t, `{}`),
				``,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			vec := runJsonFunctionWithSelectList(t, proc,
				[]FunctionTestInput{
					NewFunctionTestInput(test.typ, test.docs, []bool{false, false, true}),
					NewFunctionTestInput(types.T_varchar.ToType(),
						[]string{"$.a", "$.a", "$.a"}, []bool{false, false, false}),
				},
				types.T_json.ToType(), newOpBuiltInJsonExtract().jsonExtract, nil)

			require.False(t, vec.IsNull(0))
			require.Equal(t, "null", jsonVectorRowString(t, vec, 0))
			require.True(t, vec.IsNull(1))
			require.True(t, vec.IsNull(2))
		})
	}
}

func TestJsonExtractIgnoreAllRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	vec := runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`not json`, `still not json`}, []bool{false, false}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`bad path`, `also bad path`}, []bool{false, false}),
		},
		types.T_json.ToType(), newOpBuiltInJsonExtract().jsonExtract,
		&FunctionSelectList{AllNull: true})

	require.True(t, vec.IsNull(0))
	require.True(t, vec.IsNull(1))
}

func TestJsonExtractMultiplePathsPreserveJSONNull(t *testing.T) {
	proc := testutil.NewProcess(t)
	vec := runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`{"a":null,"b":1}`, `{"a":null}`}, []bool{false, false}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"$.a", "$.a"}, []bool{false, false}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"$.b", "$.missing"}, []bool{false, false}),
		},
		types.T_json.ToType(), newOpBuiltInJsonExtract().jsonExtract, nil)

	require.False(t, vec.IsNull(0))
	require.Equal(t, "[null, 1]", jsonVectorRowString(t, vec, 0))
	require.False(t, vec.IsNull(1))
	require.Equal(t, "[null]", jsonVectorRowString(t, vec, 1))
}

func TestJsonExtractWildcardPreservesJSONNull(t *testing.T) {
	proc := testutil.NewProcess(t)
	tests := []struct {
		name string
		typ  types.Type
		doc  string
	}{
		{name: "varchar input", typ: types.T_varchar.ToType(), doc: `{"items":[null,1]}`},
		{name: "json input", typ: types.T_json.ToType(), doc: mustJsonBinaryString(t, `{"items":[null,1]}`)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			vec := runJsonFunctionWithSelectList(t, proc,
				[]FunctionTestInput{
					NewFunctionTestInput(test.typ, []string{test.doc}, []bool{false}),
					NewFunctionTestInput(types.T_varchar.ToType(), []string{"$.items[*]"}, []bool{false}),
				},
				types.T_json.ToType(), newOpBuiltInJsonExtract().jsonExtract, nil)

			require.False(t, vec.IsNull(0))
			require.Equal(t, "[null, 1]", jsonVectorRowString(t, vec, 0))
		})
	}
}

func TestJsonExtractAutowrapsJSONNullIndexZero(t *testing.T) {
	proc := testutil.NewProcess(t)
	tests := []struct {
		name string
		doc  string
		path string
	}{
		{name: "root null", doc: `null`, path: `$[0]`},
		{name: "nested null", doc: `{"a":null}`, path: `$.a[0]`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			vec := runJsonFunctionWithSelectList(t, proc,
				[]FunctionTestInput{
					NewFunctionTestInput(types.T_varchar.ToType(), []string{test.doc}, []bool{false}),
					NewFunctionTestInput(types.T_varchar.ToType(), []string{test.path}, []bool{false}),
				},
				types.T_json.ToType(), newOpBuiltInJsonExtract().jsonExtract, nil)

			require.False(t, vec.IsNull(0))
			require.Equal(t, "null", jsonVectorRowString(t, vec, 0))
		})
	}
}

func TestJsonExtractPreservesSingleMatchMultiValuePathArray(t *testing.T) {
	proc := testutil.NewProcess(t)
	tests := []struct {
		name     string
		doc      string
		path     string
		expected string
	}{
		{name: "scalar range", doc: `1`, path: `$[0 to 0]`, expected: `[1]`},
		{name: "index wildcard", doc: `[null]`, path: `$[*]`, expected: `[null]`},
		{name: "range", doc: `[null]`, path: `$[0 to 0]`, expected: `[null]`},
		{name: "key wildcard", doc: `{"a":null}`, path: `$.*`, expected: `[null]`},
		{name: "recursive descent", doc: `{"a":null}`, path: `$**.a`, expected: `[null]`},
		{name: "empty object range", doc: `{}`, path: `$[0 to 0]`, expected: `[{}]`},
		{name: "object last range", doc: `{"a":1,"b":2}`, path: `$[last to last]`, expected: `[{"a":1,"b":2}]`},
		{name: "object last range then key", doc: `{"a":null,"b":2}`, path: `$[last to last].a`, expected: `[null]`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			vec := runJsonFunctionWithSelectList(t, proc,
				[]FunctionTestInput{
					NewFunctionTestInput(types.T_varchar.ToType(), []string{test.doc}, []bool{false}),
					NewFunctionTestInput(types.T_varchar.ToType(), []string{test.path}, []bool{false}),
				},
				types.T_json.ToType(), newOpBuiltInJsonExtract().jsonExtract, nil)

			require.False(t, vec.IsNull(0))
			require.JSONEq(t, test.expected, jsonVectorRowString(t, vec, 0))
		})
	}
}

func TestJsonExtractConstNullPathAfterNonSimplePath(t *testing.T) {
	proc := testutil.NewProcess(t)
	op := newOpBuiltInJsonExtract()

	errTC := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{`[1,2]`}, []bool{false}),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{`$[*]`}, []bool{false}),
		},
		NewFunctionTestResult(types.T_varchar.ToType(), true, nil, nil), op.jsonExtractString)
	s, info := errTC.Run()
	require.True(t, s, info)

	vec := runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{`{"a":"x"}`}, []bool{false}),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{""}, []bool{true}),
		},
		types.T_varchar.ToType(), op.jsonExtractString, nil)
	require.True(t, vec.IsNull(0))
}

func TestJsonExtractStringTypedScalars(t *testing.T) {
	proc := testutil.NewProcess(t)

	tests := []struct {
		name string
		tp   bytejson.TpCode
		val  string
	}{
		{"date", bytejson.TpCodeDate, "2024-01-02"},
		{"time", bytejson.TpCodeTime, "03:04:05.123"},
		{"datetime", bytejson.TpCodeDatetime, "2024-01-02 03:04:05"},
		{"blob", bytejson.TpCodeBlob, "aGVsbG8="},
	}

	jsonInputs := make([]string, len(tests))
	paths := make([]string, len(tests))
	want := make([]string, len(tests))
	nulls := make([]bool, len(tests))
	for i, tt := range tests {
		bj, err := bytejson.CreateByteJSON([]any{newTypedByteJson(tt.tp, tt.val)})
		require.NoError(t, err, tt.name)
		data, err := bj.Marshal()
		require.NoError(t, err, tt.name)
		jsonInputs[i] = string(data)
		paths[i] = "$[0]"
		want[i] = tt.val
	}

	tc := tcTemp{
		info: "json_extract_string typed JSON scalars",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_json.ToType(), jsonInputs, nulls),
			NewFunctionTestInput(types.T_varchar.ToType(), paths, nulls),
		},
		expect: NewFunctionTestResult(types.T_varchar.ToType(), false, want, nulls),
	}
	fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, newOpBuiltInJsonExtract().jsonExtractString)
	s, info := fcTC.Run()
	require.True(t, s, info)
}

func TestJsonModifyContinuesAfterNullRows(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("null document", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"", `{"a":1}`},
					[]bool{true, false}),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"$.a"}, []bool{false}),
				NewFunctionTestConstInput(types.T_int64.ToType(), []int64{2}, []bool{false}),
			},
			types.T_json.ToType(), newOpBuiltInJsonSet().buildJsonSet, nil)
		require.True(t, vec.IsNull(0))
		require.Equal(t, `{"a": 2}`, jsonVectorRowString(t, vec, 1))
	})

	t.Run("null path", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"a":1}`, `{"a":1}`},
					[]bool{false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"", "$.a"}, []bool{true, false}),
				NewFunctionTestConstInput(types.T_int64.ToType(), []int64{2}, []bool{false}),
			},
			types.T_json.ToType(), newOpBuiltInJsonSet().buildJsonSet, nil)
		require.True(t, vec.IsNull(0))
		require.Equal(t, `{"a": 2}`, jsonVectorRowString(t, vec, 1))
	})
}

func TestJsonRemove(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("removes object array and nested values", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"a":1,"b":2}`, `["a",["b","c"],"d"]`, `{"a":{"b":1,"c":2},"d":3}`},
					[]bool{false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"$.a", "$[1]", "$.a.b"},
					[]bool{false, false, false}),
			},
			types.T_json.ToType(), newOpBuiltInJsonRemove().buildJsonRemove, nil)

		require.Equal(t, `{"b": 2}`, jsonVectorRowString(t, vec, 0))
		require.Equal(t, `["a", "d"]`, jsonVectorRowString(t, vec, 1))
		require.Equal(t, `{"a": {"c": 2}, "d": 3}`, jsonVectorRowString(t, vec, 2))
	})

	t.Run("missing path is noop and paths are applied left to right", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"a":1}`, `["a","b","c","d"]`},
					[]bool{false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"$.b", "$[1]"},
					[]bool{false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"$.a.b", "$[1]"},
					[]bool{false, false}),
			},
			types.T_json.ToType(), newOpBuiltInJsonRemove().buildJsonRemove, nil)

		require.Equal(t, `{"a": 1}`, jsonVectorRowString(t, vec, 0))
		require.Equal(t, `["a", "d"]`, jsonVectorRowString(t, vec, 1))
	})

	t.Run("mysql array index boundaries", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`[1,2,3]`, `[1,2,3]`, `[1,2,3]`, `[1,2,3]`},
					[]bool{false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"$[0]", "$[1]", "$[2]", "$[3]"},
					[]bool{false, false, false, false}),
			},
			types.T_json.ToType(), newOpBuiltInJsonRemove().buildJsonRemove, nil)

		require.Equal(t, `[2, 3]`, jsonVectorRowString(t, vec, 0))
		require.Equal(t, `[1, 3]`, jsonVectorRowString(t, vec, 1))
		require.Equal(t, `[1, 2]`, jsonVectorRowString(t, vec, 2))
		require.Equal(t, `[1, 2, 3]`, jsonVectorRowString(t, vec, 3))
	})

	t.Run("json typed input", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(),
					[]string{mustJsonBinaryString(t, `{"a":1,"b":[2,3]}`)},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"$.b[0]"}, []bool{false}),
			},
			types.T_json.ToType(), newOpBuiltInJsonRemove().buildJsonRemove, nil)

		require.Equal(t, `{"a": 1, "b": [3]}`, jsonVectorRowString(t, vec, 0))
	})

	t.Run("array index zero autowrap", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"a":1,"b":2}`, `{"a":1,"b":2}`, `1`},
					[]bool{false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"$.a[0]", "$[0].a", "$[0]"},
					[]bool{false, false, false}),
			},
			types.T_json.ToType(), newOpBuiltInJsonRemove().buildJsonRemove, nil)

		require.Equal(t, `{"b": 2}`, jsonVectorRowString(t, vec, 0))
		require.Equal(t, `{"b": 2}`, jsonVectorRowString(t, vec, 1))
		require.Equal(t, `1`, jsonVectorRowString(t, vec, 2))
	})

	t.Run("mysql nested and multi path examples", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{
						`[ { "a": true }, { "b": false }, { "c": null }, { "a": null } ]`,
						`{"a":"foo","b":[true,{"c":123}]}`,
						`{"a":"foo","b":[true,{"c":123,"d":456}]}`,
						`{"a":"foo","b":[true,{"c":123,"d":456}]}`,
						`123`,
					},
					[]bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"$[0].a", "$.b[1]", "$.b[1].c", "$.b[1].e", "$.a"},
					[]bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"$[2].c", "$.missing", "$.missing", "$.missing", "$.missing"},
					[]bool{false, false, false, false, false}),
			},
			types.T_json.ToType(), newOpBuiltInJsonRemove().buildJsonRemove, nil)

		require.Equal(t, `[{}, {"b": false}, {}, {"a": null}]`, jsonVectorRowString(t, vec, 0))
		require.Equal(t, `{"a": "foo", "b": [true]}`, jsonVectorRowString(t, vec, 1))
		require.Equal(t, `{"a": "foo", "b": [true, {"d": 456}]}`, jsonVectorRowString(t, vec, 2))
		require.Equal(t, `{"a": "foo", "b": [true, {"c": 123, "d": 456}]}`, jsonVectorRowString(t, vec, 3))
		require.Equal(t, `123`, jsonVectorRowString(t, vec, 4))
	})

	t.Run("mysql deep autowrap", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{
						`{}`,
						`[]`,
						`{"a":{"b":1,"c":2}}`,
						`[{"a":[{"b":1,"c":2},123]},456]`,
					},
					[]bool{false, false, false, false}),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"$[0][0].a[0][0].b"}, []bool{false}),
			},
			types.T_json.ToType(), newOpBuiltInJsonRemove().buildJsonRemove, nil)

		require.Equal(t, `{}`, jsonVectorRowString(t, vec, 0))
		require.Equal(t, `[]`, jsonVectorRowString(t, vec, 1))
		require.Equal(t, `{"a": {"c": 2}}`, jsonVectorRowString(t, vec, 2))
		require.Equal(t, `[{"a": [{"c": 2}, 123]}, 456]`, jsonVectorRowString(t, vec, 3))
	})

	t.Run("quoted star is object key", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"*":1,"a":2}`},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`$."*"`}, []bool{false}),
			},
			types.T_json.ToType(), newOpBuiltInJsonRemove().buildJsonRemove, nil)

		require.Equal(t, `{"a": 2}`, jsonVectorRowString(t, vec, 0))
	})
}

func TestJsonRemoveNullAndInvalidPaths(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("null document and null path return null", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"", `{"a":1}`, `{"a":1}`},
					[]bool{true, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"$.a", "", "$.a"},
					[]bool{false, true, false}),
			},
			types.T_json.ToType(), newOpBuiltInJsonRemove().buildJsonRemove, nil)

		require.True(t, vec.IsNull(0))
		require.True(t, vec.IsNull(1))
		require.Equal(t, `{}`, jsonVectorRowString(t, vec, 2))
	})

	t.Run("null path after prior path returns null", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`[1,{"a":true,"b":false,"c":null},5]`, `[1,{"a":true,"b":false,"c":null},5]`},
					[]bool{false, false}),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"$[1]"}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"$[2]", ""},
					[]bool{false, true}),
			},
			types.T_json.ToType(), newOpBuiltInJsonRemove().buildJsonRemove, nil)

		require.Equal(t, `[1, 5]`, jsonVectorRowString(t, vec, 0))
		require.True(t, vec.IsNull(1))
	})

	t.Run("json null document remains json null", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`null`, `null`},
					[]bool{false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"$[0]", "$.missing"},
					[]bool{false, false}),
			},
			types.T_json.ToType(), newOpBuiltInJsonRemove().buildJsonRemove, nil)

		require.False(t, vec.IsNull(0))
		require.Equal(t, `null`, jsonVectorRowString(t, vec, 0))
		require.False(t, vec.IsNull(1))
		require.Equal(t, `null`, jsonVectorRowString(t, vec, 1))
	})

	for _, pathStr := range []string{"$", "$.*", "$**.a"} {
		t.Run(fmt.Sprintf("rejects path %s", pathStr), func(t *testing.T) {
			tc := tcTemp{
				info: "json_remove rejects invalid modify path",
				inputs: []FunctionTestInput{
					NewFunctionTestInput(types.T_varchar.ToType(), []string{`{"a":1}`}, []bool{false}),
					NewFunctionTestInput(types.T_varchar.ToType(), []string{pathStr}, []bool{false}),
				},
				expect: NewFunctionTestResult(types.T_json.ToType(), true, nil, nil),
			}
			fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, newOpBuiltInJsonRemove().buildJsonRemove)
			s, info := fcTC.Run()
			require.True(t, s, info)
		})
	}
}

func TestJsonRemoveCheckFn(t *testing.T) {
	ctx := context.Background()
	_, err := GetFunctionByName(ctx, "json_remove", []types.Type{
		types.T_json.ToType(),
		types.T_varchar.ToType(),
	})
	require.NoError(t, err)

	_, err = GetFunctionByName(ctx, "json_remove", []types.Type{
		types.T_json.ToType(),
	})
	require.Error(t, err)

	_, err = GetFunctionByName(ctx, "json_remove", []types.Type{
		types.T_json.ToType(),
		types.T_json.ToType(),
	})
	require.Error(t, err)

	_, err = GetFunctionByName(ctx, "json_remove", []types.Type{
		types.T_int64.ToType(),
		types.T_varchar.ToType(),
	})
	require.Error(t, err)

	_, err = GetFunctionByName(ctx, "json_remove", []types.Type{
		types.T_varchar.ToType(),
		types.T_int64.ToType(),
	})
	require.Error(t, err)

	_, err = GetFunctionByName(ctx, "json_remove", []types.Type{
		types.T_any.ToType(),
		types.T_varchar.ToType(),
	})
	require.NoError(t, err)

	_, err = GetFunctionByName(ctx, "json_remove", []types.Type{
		types.T_varchar.ToType(),
		types.T_any.ToType(),
	})
	require.NoError(t, err)
}

func TestJsonRemoveIgnoreAllRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	selectList := &FunctionSelectList{AllNull: true}
	vec := runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`not json`, `still not json`},
				[]bool{false, false}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`bad path`, `also bad path`},
				[]bool{false, false}),
		},
		types.T_json.ToType(), newOpBuiltInJsonRemove().buildJsonRemove, selectList)

	require.True(t, vec.IsNull(0))
	require.True(t, vec.IsNull(1))
}

func TestJsonMergeCheckFn(t *testing.T) {
	ctx := context.Background()
	for _, fn := range []string{"json_merge_patch", "json_merge_preserve"} {
		_, err := GetFunctionByName(ctx, fn, []types.Type{
			types.T_json.ToType(),
			types.T_varchar.ToType(),
		})
		require.NoError(t, err, fn)

		_, err = GetFunctionByName(ctx, fn, []types.Type{types.T_json.ToType()})
		require.Error(t, err, fn)

		_, err = GetFunctionByName(ctx, fn, []types.Type{
			types.T_int64.ToType(),
			types.T_varchar.ToType(),
		})
		require.Error(t, err, fn)

		_, err = GetFunctionByName(ctx, fn, []types.Type{
			types.T_any.ToType(),
			types.T_varchar.ToType(),
		})
		require.NoError(t, err, fn)
	}
}

func TestJsonMerge(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("patch follows RFC 7396 and preserves MySQL null state", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"a":1,"nested":{"keep":1,"remove":2}}`, ``, `{"a":1}`, `{"a":1}`},
					[]bool{false, true, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"b":2,"nested":{"remove":null,"add":3}}`, `[1,2]`, ``, ``},
					[]bool{false, false, true, true}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{}`, `[1,2]`, `{"b":2}`, `2`},
					[]bool{false, false, false, false}),
			},
			types.T_json.ToType(), newOpBuiltInJsonMerge().buildJsonMergePatch, nil)

		require.Equal(t, `{"a": 1, "b": 2, "nested": {"add": 3, "keep": 1}}`, jsonVectorRowString(t, vec, 0))
		require.Equal(t, `[1, 2]`, jsonVectorRowString(t, vec, 1))
		require.True(t, vec.IsNull(2))
		require.Equal(t, `2`, jsonVectorRowString(t, vec, 3))
	})

	t.Run("preserve merges recursively and SQL null remains null", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(),
					[]string{mustJsonBinaryString(t, `{"a":{"x":1},"array":[1]}`), mustJsonBinaryString(t, `{"a":1}`)},
					[]bool{false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"a":{"y":2},"array":[2],"value":null}`, ``},
					[]bool{false, true}),
			},
			types.T_json.ToType(), newOpBuiltInJsonMerge().buildJsonMergePreserve, nil)

		require.Equal(t, `{"a": {"x": 1, "y": 2}, "array": [1, 2], "value": null}`, jsonVectorRowString(t, vec, 0))
		require.True(t, vec.IsNull(1))
	})

	t.Run("json literal null is a non-null JSON result", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`{"a":"foo"}`}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`null`}, []bool{false}),
			},
			types.T_json.ToType(), newOpBuiltInJsonMerge().buildJsonMergePatch, nil)

		require.False(t, vec.IsNull(0))
		require.Equal(t, `null`, jsonVectorRowString(t, vec, 0))
	})
}

func TestJsonMergeDepthValidation(t *testing.T) {
	proc := testutil.NewProcess(t)
	overDepthArray := `[` + nestedJSONMergeObject(100, `1`) + `]`
	overDepthObject := nestedJSONMergeObject(101, `1`)

	t.Run("final non-object replacement is rejected", func(t *testing.T) {
		testCase := NewFunctionTestCase(
			proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`{}`}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{overDepthArray}, nil),
			},
			NewFunctionTestResult(types.T_json.ToType(), true, nil, nil),
			newOpBuiltInJsonMerge().buildJsonMergePatch,
		)
		require.NoError(t, testCase.result.PreExtendAndReset(testCase.fnLength))
		err := testCase.fn(
			testCase.parameters,
			testCase.result,
			testCase.proc,
			testCase.fnLength,
			nil,
		)
		require.ErrorContains(t, err, "json document nesting depth exceeds 100")
	})

	tests := []struct {
		name   string
		fn     fEvalFn
		inputs []FunctionTestInput
	}{
		{
			name: "patch rejects over-depth target before scalar replacement",
			fn:   newOpBuiltInJsonMerge().buildJsonMergePatch,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{overDepthObject}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`1`}, nil),
			},
		},
		{
			name: "patch validates object while target is unknown",
			fn:   newOpBuiltInJsonMerge().buildJsonMergePatch,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{``}, []bool{true}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{overDepthObject}, nil),
			},
		},
		{
			name: "preserve validates argument before following SQL null",
			fn:   newOpBuiltInJsonMerge().buildJsonMergePreserve,
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{overDepthObject}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{``}, []bool{true}),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testCase := NewFunctionTestCase(
				proc,
				tt.inputs,
				NewFunctionTestResult(types.T_json.ToType(), true, nil, nil),
				tt.fn,
			)
			require.NoError(t, testCase.result.PreExtendAndReset(testCase.fnLength))
			err := testCase.fn(
				testCase.parameters,
				testCase.result,
				testCase.proc,
				testCase.fnLength,
				nil,
			)
			require.ErrorContains(t, err, "json document nesting depth exceeds 100")
		})
	}
}

func TestJsonMergeIgnoreAllRowsMaterializesLength(t *testing.T) {
	proc := testutil.NewProcess(t)
	vec := runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{`not json`, `still not json`}, []bool{false, false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{`not json`, `still not json`}, []bool{false, false}),
		},
		types.T_json.ToType(), newOpBuiltInJsonMerge().buildJsonMergePatch, &FunctionSelectList{AllNull: true})

	require.Equal(t, 2, vec.Length())
	require.True(t, vec.IsNull(0))
	require.True(t, vec.IsNull(1))
}

func TestJsonSetCheckFn(t *testing.T) {
	ctx := context.Background()
	_, err := GetFunctionByName(ctx, "json_set", []types.Type{
		types.T_json.ToType(),
		types.T_varchar.ToType(),
		types.T_json.ToType(),
	})
	require.NoError(t, err)

	_, err = GetFunctionByName(ctx, "json_set", []types.Type{
		types.T_json.ToType(),
		types.T_varchar.ToType(),
		types.T_int64.ToType(),
		types.T_varchar.ToType(),
	})
	require.Error(t, err)

	_, err = GetFunctionByName(ctx, "json_set", []types.Type{
		types.T_json.ToType(),
		types.T_json.ToType(),
		types.T_int64.ToType(),
	})
	require.Error(t, err)
}

func TestJsonContainsCheckFn(t *testing.T) {
	ctx := context.Background()

	_, err := GetFunctionByName(ctx, "json_contains", []types.Type{
		types.T_json.ToType(),
		types.T_json.ToType(),
	})
	require.NoError(t, err)

	_, err = GetFunctionByName(ctx, "json_contains", []types.Type{
		types.T_varchar.ToType(),
		types.T_varchar.ToType(),
		types.T_varchar.ToType(),
	})
	require.NoError(t, err)

	_, err = GetFunctionByName(ctx, "json_contains", []types.Type{
		types.T_json.ToType(),
	})
	require.Error(t, err)

	_, err = GetFunctionByName(ctx, "json_contains", []types.Type{
		types.T_json.ToType(),
		types.T_json.ToType(),
		types.T_json.ToType(),
	})
	require.Error(t, err)
}

func TestJsonContainsPathCheckFn(t *testing.T) {
	ctx := context.Background()

	_, err := GetFunctionByName(ctx, "json_contains_path", []types.Type{
		types.T_json.ToType(),
		types.T_varchar.ToType(),
		types.T_varchar.ToType(),
	})
	require.NoError(t, err)

	_, err = GetFunctionByName(ctx, "json_contains_path", []types.Type{
		types.T_varchar.ToType(),
		types.T_varchar.ToType(),
		types.T_varchar.ToType(),
		types.T_varchar.ToType(),
	})
	require.NoError(t, err)

	_, err = GetFunctionByName(ctx, "json_contains_path", []types.Type{
		types.T_json.ToType(),
		types.T_varchar.ToType(),
	})
	require.Error(t, err)

	_, err = GetFunctionByName(ctx, "json_contains_path", []types.Type{
		types.T_json.ToType(),
		types.T_json.ToType(),
		types.T_varchar.ToType(),
	})
	require.Error(t, err)
}

func TestJsonContains(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("with path", func(t *testing.T) {
		tc := tcTemp{
			info: "json_contains with path",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{
						`{"tags":["x","y"]}`,
						`{"a":1,"b":2,"c":{"d":4}}`,
						`{"a":1,"b":2,"c":{"d":4}}`,
						`{"a":1,"b":2,"c":{"d":4}}`,
						`{"a":1,"b":2,"c":{"d":4}}`,
						`{"a":1}`,
						`{"a":null}`,
						`{"a":1}`,
						`{"a":1}`,
					},
					[]bool{false, false, false, false, false, false, false, true, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`"x"`, `1`, `1`, `{"d":4}`, `{"d":4}`, `1`, `null`, `1`, `1`},
					[]bool{false, false, false, false, false, false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`$.tags`, `$.a`, `$.b`, `$.c`, `$.a`, `$.missing`, `$.a`, `$.a`, `$.a[0]`},
					[]bool{false, false, false, false, false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{1, 1, 0, 1, 0, 0, 1, 0, 1},
				[]bool{false, false, false, false, false, true, false, true, false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, newOpBuiltInJsonContains().jsonContains)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	t.Run("without path", func(t *testing.T) {
		tc := tcTemp{
			info: "json_contains without path",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`[1,2,3]`, `{"a":1,"b":{"c":2}}`, `["a","b"]`, `[{"a":1},{"b":2}]`, `true`},
					[]bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`[1,3]`, `{"b":{"c":2}}`, `"c"`, `{"b":2}`, `true`},
					[]bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{1, 1, 0, 1, 1},
				[]bool{false, false, false, false, false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, newOpBuiltInJsonContains().jsonContains)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	t.Run("mysql json_no_table containment regressions", func(t *testing.T) {
		notNulls := make([]bool, 21)
		tc := tcTemp{
			info: "json_contains mysql json_no_table containment regressions",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{
						`[1,[2.0,3.0]]`,
						`[1,2,[3,[4,5]],6,7]`,
						`[1,3,5]`,
						`[{"b":4,"a":7}]`,
						`[{"b":4,"a":7},5]`,
						`[{"b":4,"a":7},5.0]`,
						`[null,1,[2,3],true,false]`,
						`[null,1,[2,3],true,false]`,
						`[true,false]`,
						`[1,2]`,
						`[1,2,[4]]`,
						`[1,2,[4,5]]`,
						`[1,2,[4,5]]`,
						`[]`,
						`[]`,
						`[]`,
						`[]`,
						`[]`,
						`{}`,
						`{}`,
						`{"a":1}`,
					},
					notNulls),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{
						`[2.0]`,
						`5`,
						`[5,3,1,5]`,
						`[{"a":7},{"b":4}]`,
						`[5,{"a":7,"b":4}]`,
						`[5,{"a":7.0E0,"b":4}]`,
						`[null,1,[3],false]`,
						`[null,1,[4],false]`,
						`[[true]]`,
						`[[1]]`,
						`{"b":2}`,
						`[1,2,3,4,5,6,7,8,9]`,
						`[111111111111111111]`,
						`{"a":1}`,
						`[1,2,3,4,5]`,
						`[1,2,3,4,{"a":1}]`,
						`{"a":[1,2,3,4,5]}`,
						`[]`,
						`{}`,
						`{"a":1}`,
						`{"a":1,"b":2}`,
					},
					notNulls),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0},
				notNulls),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, newOpBuiltInJsonContains().jsonContains)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	t.Run("numeric cross type", func(t *testing.T) {
		tc := tcTemp{
			info: "json_contains numeric cross type",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`1`, `{"a":1}`, `[1,2,3]`, `-1`, `[1,2.0,3]`},
					[]bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`1.0`, `{"a":1.0}`, `[1.0,3.0]`, `-1.0`, `[1.0,2,3.0]`},
					[]bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{1, 1, 1, 1, 1},
				[]bool{false, false, false, false, false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, newOpBuiltInJsonContains().jsonContains)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	t.Run("numeric exact equality", func(t *testing.T) {
		tc := tcTemp{
			info: "json_contains numeric exact equality",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{
						`1`,
						`9007199254740993`,
						`[1]`,
						`{"a":1}`,
						`0`,
						`18446744073709551615`,
						`18446744073709551615`,
					},
					[]bool{false, false, false, false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{
						`1.000000009`,
						`9007199254740992.0`,
						`[1.000000009]`,
						`{"a":1.000000009}`,
						`-0.0`,
						`18446744073709551615.0`,
						`18446744073709551614.0`,
					},
					[]bool{false, false, false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{0, 0, 0, 0, 1, 0, 0},
				[]bool{false, false, false, false, false, false, false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, newOpBuiltInJsonContains().jsonContains)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	t.Run("typed json input", func(t *testing.T) {
		tc := tcTemp{
			info: "json_contains typed json input",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(),
					[]string{
						mustJsonBinaryString(t, `{"a":1,"b":[1,2,3]}`),
						mustJsonBinaryString(t, `[1,2,3]`),
						mustJsonBinaryString(t, `{"a":false}`),
					},
					[]bool{false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`1`, `[1.0,3.0]`, `false`},
					[]bool{false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`$.a[0]`, `$`, `$.a[0]`},
					[]bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{1, 1, 1},
				[]bool{false, false, false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, newOpBuiltInJsonContains().jsonContains)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	t.Run("strict scalar type matching", func(t *testing.T) {
		tc := tcTemp{
			info: "json_contains strict scalar type matching",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`"1"`, `1`, `true`, `{"a":"1"}`, `{"a":1}`, `{"a":false}`, `{"a":false}`},
					[]bool{false, false, false, false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`1`, `"1"`, `1`, `1`, `"1"`, `false`, `true`},
					[]bool{false, false, false, false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`$`, `$`, `$`, `$.a`, `$.a`, `$.a`, `$.a`},
					[]bool{false, false, false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{0, 0, 0, 0, 0, 1, 0},
				[]bool{false, false, false, false, false, false, false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, newOpBuiltInJsonContains().jsonContains)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	t.Run("scalar autowrap path variants", func(t *testing.T) {
		tc := tcTemp{
			info: "json_contains scalar autowrap path variants",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`1`, `1`, `{"a":true}`, `{"a":"x"}`, `{"a":[{"b":1}]}`},
					[]bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`1`, `1`, `true`, `"x"`, `1`},
					[]bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`$[0]`, `$[1]`, `$.a[0]`, `$.a[0]`, `$.a[0].b[0]`},
					[]bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{1, 0, 1, 1, 1},
				[]bool{false, true, false, false, false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, newOpBuiltInJsonContains().jsonContains)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})
}

func TestJsonContainsErrors(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("invalid candidate json", func(t *testing.T) {
		tc := tcTemp{
			info: "json_contains invalid candidate",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`{"a":"x"}`}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`x`}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`$.a`}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), true, nil, nil),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, newOpBuiltInJsonContains().jsonContains)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	t.Run("wildcard path", func(t *testing.T) {
		tc := tcTemp{
			info: "json_contains wildcard path",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`[1,2]`}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`1`}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`$[*]`}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), true, nil, nil),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, newOpBuiltInJsonContains().jsonContains)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})
}

func TestJsonContainsPath(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := tcTemp{
		info: "json_contains_path matches json null and short circuits later null paths",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`{"a":null}`, `{}`, `{"a":1}`, `{"a":1}`, `{"a":1}`, `{"a":1}`},
				[]bool{false, false, false, false, false, false}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`all`, `all`, `one`, `all`, `one`, `all`},
				[]bool{false, false, false, false, false, false}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`$.a`, `$.a`, `$.a`, `$.missing`, `$.missing`, `$.a`},
				[]bool{false, false, false, false, false, false}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`$`, `$.missing`, ``, ``, ``, ``},
				[]bool{false, false, true, true, true, true}),
		},
		expect: NewFunctionTestResult(types.T_int64.ToType(), false,
			[]int64{1, 0, 1, 0, 0, 0},
			[]bool{false, false, false, false, true, true}),
	}
	fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, newOpBuiltInJsonContainsPath().jsonContainsPath)
	s, info := fcTC.Run()
	require.True(t, s, info)
}

func TestJsonContainsPathMySQLRegressionSemantics(t *testing.T) {
	proc := testutil.NewProcess(t)
	complexDoc := `{"a":true,"b":[1,2,{"c":[4,5,{"d":[6,7,8,9,10]}]}]}`
	tc := tcTemp{
		info: "json_contains_path supports MySQL all one wildcard and recursive path cases",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{complexDoc, complexDoc, `{"a":true,"b":[1,2]}`, `{"a":1,"b":2}`},
				[]bool{false, false, false, false}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`all`, `one`, `ALL`, `oNe`},
				[]bool{false, false, false, false}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`$**[1]`, `$.c`, `$.b[0]`, `$.*`},
				[]bool{false, false, false, false}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`$.b[0]`, `$**[1]`, `$.b[1]`, `$.missing`},
				[]bool{false, false, false, false}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`$.c`, `$.b[0]`, `$`, `$.b`},
				[]bool{false, false, false, false}),
		},
		expect: NewFunctionTestResult(types.T_int64.ToType(), false,
			[]int64{0, 1, 1, 1},
			[]bool{false, false, false, false}),
	}
	fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, newOpBuiltInJsonContainsPath().jsonContainsPath)
	s, info := fcTC.Run()
	require.True(t, s, info)

	typedJSON := tcTemp{
		info: "json_contains_path accepts typed json documents",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_json.ToType(),
				[]string{mustJsonBinaryString(t, `{"a":null,"b":[1,2]}`)}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{`all`}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{`$.a`}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{`$.b[1]`}, []bool{false}),
		},
		expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1}, []bool{false}),
	}
	fcTC = NewFunctionTestCase(proc, typedJSON.inputs, typedJSON.expect, newOpBuiltInJsonContainsPath().jsonContainsPath)
	s, info = fcTC.Run()
	require.True(t, s, info)
}

func TestJsonContainsPathEvaluationOrder(t *testing.T) {
	proc := testutil.NewProcess(t)
	tests := []struct {
		name   string
		inputs []FunctionTestInput
		expect FunctionTestResult
	}{
		{
			name: "one skips a later invalid path after a hit",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`{"a":1}`}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`one`}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`$.a`}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`$[`}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1}, []bool{false}),
		},
		{
			name: "all skips a later invalid path after a miss",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`{"a":1}`}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`all`}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`$.missing`}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`$[`}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, []bool{false}),
		},
		{
			name: "invalid document is evaluated before a later null path",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`{"a":`}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`one`}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{``}, []bool{true}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), true, nil, nil),
		},
		{
			name: "invalid mode is evaluated before a later null path",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`{"a":1}`}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`invalid`}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{``}, []bool{true}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), true, nil, nil),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fcTC := NewFunctionTestCase(proc, tt.inputs, tt.expect, newOpBuiltInJsonContainsPath().jsonContainsPath)
			s, info := fcTC.Run()
			require.True(t, s, info)
		})
	}
}

func TestJsonContainsPathConstantCacheInvalidation(t *testing.T) {
	proc := testutil.NewProcess(t)
	op := newOpBuiltInJsonContainsPath()

	modeOne, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("one"), 1, proc.Mp())
	require.NoError(t, err)
	mode, isNull, err := op.getMode(modeOne, vector.GenerateFunctionStrParameter(modeOne), 0, proc)
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, jsonContainsPathOne, mode)

	modeAll, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("all"), 1, proc.Mp())
	require.NoError(t, err)
	mode, isNull, err = op.getMode(modeAll, vector.GenerateFunctionStrParameter(modeAll), 0, proc)
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, jsonContainsPathAll, mode)

	op.pathCache = make([]jsonContainsPathPathCache, 1)
	pathA, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("$.a"), 1, proc.Mp())
	require.NoError(t, err)
	parsedA, isNull, err := op.getPath(pathA, vector.GenerateFunctionStrParameter(pathA), 0, 0, proc)
	require.NoError(t, err)
	require.False(t, isNull)

	pathB, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("$.b"), 1, proc.Mp())
	require.NoError(t, err)
	parsedB, isNull, err := op.getPath(pathB, vector.GenerateFunctionStrParameter(pathB), 0, 0, proc)
	require.NoError(t, err)
	require.False(t, isNull)
	require.NotSame(t, parsedA, parsedB)
	require.Equal(t, `$.b`, op.pathCache[0].text)
}

func TestJsonContainsSelectList(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("ignore all rows", func(t *testing.T) {
		selectList := &FunctionSelectList{AllNull: true}
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`not json`, `still not json`},
					[]bool{false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`not json`, `still not json`},
					[]bool{false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`bad path`, `$[*]`},
					[]bool{false, false}),
			},
			types.T_int64.ToType(), newOpBuiltInJsonContains().jsonContains, selectList)

		require.Equal(t, 2, vec.Length())
		require.True(t, vec.IsNull(0))
		require.True(t, vec.IsNull(1))
	})

	t.Run("skip selected rows", func(t *testing.T) {
		selectList := &FunctionSelectList{AnyNull: true, SelectList: []bool{false, true}}
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`not json`, `{"tags":["x","y"]}`},
					[]bool{false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`not json`, `"x"`},
					[]bool{false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`bad path`, `$.tags`},
					[]bool{false, false}),
			},
			types.T_int64.ToType(), newOpBuiltInJsonContains().jsonContains, selectList)

		require.True(t, vec.IsNull(0))
		v, null := vector.GenerateFunctionFixedTypeParameter[int64](vec).GetValue(1)
		require.False(t, null)
		require.Equal(t, int64(1), v)
	})

	t.Run("null path row continues", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"a":1}`, `{"a":1}`},
					[]bool{false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`1`, `1`},
					[]bool{false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"", `$.a`},
					[]bool{true, false}),
			},
			types.T_int64.ToType(), newOpBuiltInJsonContains().jsonContains, nil)

		require.True(t, vec.IsNull(0))
		v, null := vector.GenerateFunctionFixedTypeParameter[int64](vec).GetValue(1)
		require.False(t, null)
		require.Equal(t, int64(1), v)
	})
}

func TestJsonSetValueTypes(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("json value is embedded", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`{"a":0}`}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"$.a"}, []bool{false}),
				NewFunctionTestInput(types.T_json.ToType(), []string{mustJsonBinaryString(t, `{"b":[true,2]}`)}, []bool{false}),
			},
			types.T_json.ToType(), newOpBuiltInJsonSet().buildJsonSet, nil)
		require.Equal(t, `{"a": {"b": [true, 2]}}`, jsonVectorRowString(t, vec, 0))
	})

	t.Run("json-looking varchar remains string", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`{"a":0}`}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"$.a"}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`{"b":1}`}, []bool{false}),
			},
			types.T_json.ToType(), newOpBuiltInJsonSet().buildJsonSet, nil)
		require.Equal(t, `{"a": "{\"b\":1}"}`, jsonVectorRowString(t, vec, 0))
	})

	t.Run("typed scalar values", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"a":0}`, `{"a":0}`, `{"a":0}`},
					[]bool{false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"$.a", "$.a", "$.a"},
					[]bool{false, false, false}),
				NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, true},
					[]bool{false, false, true}),
			},
			types.T_json.ToType(), newOpBuiltInJsonSet().buildJsonSet, nil)
		require.Equal(t, `{"a": true}`, jsonVectorRowString(t, vec, 0))
		require.Equal(t, `{"a": false}`, jsonVectorRowString(t, vec, 1))
		require.Equal(t, `{"a": null}`, jsonVectorRowString(t, vec, 2))
	})

	t.Run("typed numeric values", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"a":0}`, `{"a":0}`},
					[]bool{false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"$.a", "$.a"},
					[]bool{false, false}),
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{^uint64(0), 1},
					[]bool{false, false}),
			},
			types.T_json.ToType(), newOpBuiltInJsonSet().buildJsonSet, nil)
		require.Equal(t, `{"a": 18446744073709551615}`, jsonVectorRowString(t, vec, 0))
		require.Equal(t, `{"a": 1}`, jsonVectorRowString(t, vec, 1))

		vec = runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`{"a":0}`}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"$.a"}, []bool{false}),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{1.5}, []bool{false}),
			},
			types.T_json.ToType(), newOpBuiltInJsonSet().buildJsonSet, nil)
		require.Equal(t, `{"a": 1.5}`, jsonVectorRowString(t, vec, 0))
	})
}

func TestJsonSetIgnoreAllRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	selectList := &FunctionSelectList{AllNull: true}
	vec := runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`not json`, `still not json`},
				[]bool{false, false}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`bad path`, `also bad path`},
				[]bool{false, false}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`bad value`, `also bad value`},
				[]bool{false, false}),
		},
		types.T_json.ToType(), newOpBuiltInJsonSet().buildJsonSet, selectList)

	require.True(t, vec.IsNull(0))
	require.True(t, vec.IsNull(1))
}

func TestJsonInsertCheckFn(t *testing.T) {
	ctx := context.Background()
	_, err := GetFunctionByName(ctx, "json_insert", []types.Type{
		types.T_json.ToType(),
		types.T_varchar.ToType(),
		types.T_json.ToType(),
	})
	require.NoError(t, err)

	_, err = GetFunctionByName(ctx, "json_insert", []types.Type{
		types.T_json.ToType(),
		types.T_varchar.ToType(),
		types.T_int64.ToType(),
		types.T_varchar.ToType(),
	})
	require.Error(t, err)

	_, err = GetFunctionByName(ctx, "json_insert", []types.Type{
		types.T_json.ToType(),
		types.T_json.ToType(),
		types.T_int64.ToType(),
	})
	require.Error(t, err)
}

func TestJsonInsertValueTypes(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("json value is embedded", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`{"a":0}`}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"$.b"}, []bool{false}),
				NewFunctionTestInput(types.T_json.ToType(), []string{mustJsonBinaryString(t, `{"c":[true,2]}`)}, []bool{false}),
			},
			types.T_json.ToType(), newOpBuiltInJsonSet().buildJsonInsert, nil)
		require.Equal(t, `{"a": 0, "b": {"c": [true, 2]}}`, jsonVectorRowString(t, vec, 0))
	})

	t.Run("json-looking varchar remains string", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`{"a":0}`}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"$.b"}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`{"c":1}`}, []bool{false}),
			},
			types.T_json.ToType(), newOpBuiltInJsonSet().buildJsonInsert, nil)
		require.Equal(t, `{"a": 0, "b": "{\"c\":1}"}`, jsonVectorRowString(t, vec, 0))
	})

	t.Run("typed scalar values", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`{"a":0}`, `{"a":0}`, `{"a":0}`},
					[]bool{false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"$.b", "$.b", "$.b"},
					[]bool{false, false, false}),
				NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, true},
					[]bool{false, false, true}),
			},
			types.T_json.ToType(), newOpBuiltInJsonSet().buildJsonInsert, nil)
		require.Equal(t, `{"a": 0, "b": true}`, jsonVectorRowString(t, vec, 0))
		require.Equal(t, `{"a": 0, "b": false}`, jsonVectorRowString(t, vec, 1))
		require.Equal(t, `{"a": 0, "b": null}`, jsonVectorRowString(t, vec, 2))
	})
}

func TestJsonInsertExistingJsonNullNoop(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("object field", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`{"a":null}`}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"$.a"}, []bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, []bool{false}),
			},
			types.T_json.ToType(), newOpBuiltInJsonSet().buildJsonInsert, nil)
		require.Equal(t, `{"a": null}`, jsonVectorRowString(t, vec, 0))
	})

	t.Run("array element", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{`[null]`}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"$[0]"}, []bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, []bool{false}),
			},
			types.T_json.ToType(), newOpBuiltInJsonSet().buildJsonInsert, nil)
		require.Equal(t, `[null]`, jsonVectorRowString(t, vec, 0))
	})
}

func TestJsonInsertRejectsNonSimplePath(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := tcTemp{
		info: "json_insert rejects wildcard path",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{`{"a":1}`}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"$.*"}, []bool{false}),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{2}, []bool{false}),
		},
		expect: NewFunctionTestResult(types.T_json.ToType(), true, nil, nil),
	}
	fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, newOpBuiltInJsonSet().buildJsonInsert)
	s, info := fcTC.Run()
	require.True(t, s, info)
}

func TestJsonInsertIgnoreAllRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	selectList := &FunctionSelectList{AllNull: true}
	vec := runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`not json`, `still not json`},
				[]bool{false, false}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`bad path`, `also bad path`},
				[]bool{false, false}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`bad value`, `also bad value`},
				[]bool{false, false}),
		},
		types.T_json.ToType(), newOpBuiltInJsonSet().buildJsonInsert, selectList)

	require.True(t, vec.IsNull(0))
	require.True(t, vec.IsNull(1))
}

func TestJsonTypeIgnoreAllRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	selectList := &FunctionSelectList{AllNull: true}
	vec := runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`not json`, `still not json`},
				[]bool{false, false}),
		},
		types.T_varchar.ToType(), newOpBuiltInJsonType().jsonType, selectList)

	require.True(t, vec.IsNull(0))
	require.True(t, vec.IsNull(1))
}

func TestJsonArrayIgnoreAllRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	selectList := &FunctionSelectList{AllNull: true}
	vec := runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_json.ToType(),
				[]string{``, ``},
				[]bool{false, false}),
		},
		types.T_json.ToType(), newOpBuiltInJsonArray().jsonArray, selectList)

	require.True(t, vec.IsNull(0))
	require.True(t, vec.IsNull(1))
}

func TestJsonObjectIgnoreAllRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	selectList := &FunctionSelectList{AllNull: true}
	vec := runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_json.ToType(),
				[]string{``, ``},
				[]bool{false, false}),
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{1, 2},
				[]bool{false, false}),
		},
		types.T_json.ToType(), newOpBuiltInJsonObject().jsonObject, selectList)

	require.True(t, vec.IsNull(0))
	require.True(t, vec.IsNull(1))
}

func TestJsonKeysIgnoreAllRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	selectList := &FunctionSelectList{AllNull: true}

	t.Run("root", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`not json`, `still not json`},
					[]bool{false, false}),
			},
			types.T_varchar.ToType(), JsonKeys, selectList)

		require.True(t, vec.IsNull(0))
		require.True(t, vec.IsNull(1))
	})

	t.Run("with path", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`not json`, `still not json`},
					[]bool{false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`bad path`, `still bad path`},
					[]bool{false, false}),
			},
			types.T_json.ToType(), JsonKeys, selectList)

		require.True(t, vec.IsNull(0))
		require.True(t, vec.IsNull(1))
	})
}

func TestJsonPrettyIgnoreAllRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	selectList := &FunctionSelectList{AllNull: true}
	vec := runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`not json`, `still not json`},
				[]bool{false, false}),
		},
		types.T_varchar.ToType(), JsonPretty, selectList)

	require.True(t, vec.IsNull(0))
	require.True(t, vec.IsNull(1))
}

func TestJsonValueIgnoreAllRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	selectList := &FunctionSelectList{AllNull: true}
	vec := runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`not json`, `still not json`},
				[]bool{false, false}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`bad path`, `still bad path`},
				[]bool{false, false}),
		},
		types.T_varchar.ToType(), JsonValue, selectList)

	require.True(t, vec.IsNull(0))
	require.True(t, vec.IsNull(1))
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
			types.T_json.ToType(), JsonKeys, selectList)

		require.True(t, vec.IsNull(0))
		require.Equal(t, `["a"]`, jsonVectorRowString(t, vec, 1))
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
			types.T_json.ToType(), JsonKeys, selectList)

		require.True(t, vec.IsNull(0))
		require.Equal(t, `["b"]`, jsonVectorRowString(t, vec, 1))
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

	t.Run("json_type", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`not json`, `{"a":1}`},
					[]bool{false, false}),
			},
			types.T_varchar.ToType(), newOpBuiltInJsonType().jsonType, selectList)

		require.True(t, vec.IsNull(0))
		v, null := vector.GenerateFunctionStrParameter(vec).GetStrValue(1)
		require.False(t, null)
		require.Equal(t, "OBJECT", string(v))
	})

	t.Run("json_extract", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`not json`, `{"a":1}`},
					[]bool{false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`bad path`, `$.a`},
					[]bool{false, false}),
			},
			types.T_json.ToType(), newOpBuiltInJsonExtract().jsonExtract, selectList)

		require.True(t, vec.IsNull(0))
		require.Equal(t, `1`, jsonVectorRowString(t, vec, 1))
	})

	t.Run("json_extract_string", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`not json`, `{"a":"x"}`},
					[]bool{false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`bad path`, `$.a`},
					[]bool{false, false}),
			},
			types.T_varchar.ToType(), newOpBuiltInJsonExtract().jsonExtractString, selectList)

		require.True(t, vec.IsNull(0))
		v, null := vector.GenerateFunctionStrParameter(vec).GetStrValue(1)
		require.False(t, null)
		require.Equal(t, "x", string(v))
	})

	t.Run("json_extract_float64", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`not json`, `{"a":1.5}`},
					[]bool{false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`bad path`, `$.a`},
					[]bool{false, false}),
			},
			types.T_float64.ToType(), newOpBuiltInJsonExtract().jsonExtractFloat64, selectList)

		require.True(t, vec.IsNull(0))
		v, null := vector.GenerateFunctionFixedTypeParameter[float64](vec).GetValue(1)
		require.False(t, null)
		require.Equal(t, 1.5, v)
	})

	t.Run("json_set", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`not json`, `{"a":1}`},
					[]bool{false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{`bad path`, `$.a`},
					[]bool{false, false}),
				NewFunctionTestConstInput(types.T_int64.ToType(), []int64{2}, []bool{false}),
			},
			types.T_json.ToType(), newOpBuiltInJsonSet().buildJsonSet, selectList)

		require.True(t, vec.IsNull(0))
		require.Equal(t, `{"a": 2}`, jsonVectorRowString(t, vec, 1))
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
			types.T_json.ToType(), JsonSchemaValidationReport, selectList)

		require.True(t, vec.IsNull(0))
		require.Equal(t, `{"valid": true}`, jsonVectorRowString(t, vec, 1))
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

func mustJsonBinaryString(t *testing.T, raw string) string {
	t.Helper()
	bj, err := types.ParseStringToByteJson(raw)
	require.NoError(t, err)
	data, err := bj.Marshal()
	require.NoError(t, err)
	return string(data)
}

func nestedJSONMergeObject(depth int, leaf string) string {
	var builder strings.Builder
	for range depth {
		builder.WriteString(`{"a":`)
	}
	builder.WriteString(leaf)
	for range depth {
		builder.WriteByte('}')
	}
	return builder.String()
}

func jsonVectorRowString(t *testing.T, vec *vector.Vector, row uint64) string {
	t.Helper()
	data, null := vector.GenerateFunctionStrParameter(vec).GetStrValue(row)
	require.False(t, null)
	bj := types.DecodeJson(data)
	out, err := bj.MarshalJSON()
	require.NoError(t, err)
	return string(out)
}
