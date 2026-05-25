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
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{`{"valid": true}`}, []bool{false}),
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
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{`{"valid": true}`}, []bool{false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, JsonSchemaValidationReport)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})
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
			types.T_varchar.ToType(), JsonKeys, selectList)

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

func mustJsonBinaryString(t *testing.T, raw string) string {
	t.Helper()
	bj, err := types.ParseStringToByteJson(raw)
	require.NoError(t, err)
	data, err := bj.Marshal()
	require.NoError(t, err)
	return string(data)
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
