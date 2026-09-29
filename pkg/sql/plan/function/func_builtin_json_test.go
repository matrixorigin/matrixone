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
	"context"
	"encoding/binary"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
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

// ============================================================================
// newTypedByteJson
// ============================================================================

func TestNewTypedByteJson(t *testing.T) {
	tests := []struct {
		name string
		tp   bytejson.TpCode
		s    string
	}{
		{"date", bytejson.TpCodeDate, "2024-01-15"},
		{"time", bytejson.TpCodeTime, "14:30:00"},
		{"datetime", bytejson.TpCodeDatetime, "2024-01-15 14:30:00"},
		{"decimal", bytejson.TpCodeDecimal, "123.456"},
		{"blob", bytejson.TpCodeBlob, "hello"},
		{"empty_string", bytejson.TpCodeString, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bj := newTypedByteJson(tt.tp, tt.s)
			require.Equal(t, tt.tp, bj.Type)

			// Verify uvarint-prefixed length encoding
			l, n := binary.Uvarint(bj.Data)
			require.Equal(t, int(l), len(tt.s), "uvarint length mismatch")
			require.Equal(t, tt.s, string(bj.Data[n:]), "data mismatch")
		})
	}
}

func TestGeometryToByteJSON(t *testing.T) {
	value, err := geometryToByteJSON(context.Background(), encodeGeometryPayload("POINT(1 2)", 0, false))
	require.NoError(t, err)
	require.Equal(t, bytejson.TpCodeObject, value.Type)
	require.Equal(t, `{"coordinates": [1, 2], "type": "Point"}`, value.String())

	_, err = geometryToByteJSON(context.Background(), []byte{1})
	require.ErrorContains(t, err, "invalid geometry payload")
}

func TestJsonObjectKeysPreserveExistingConversion(t *testing.T) {
	proc := testutil.NewProcess(t)
	timeValue, err := types.ParseTime("04:05:06", 0)
	require.NoError(t, err)
	datetimeValue, err := types.ParseDatetime("2024-02-03 04:05:06.12", 2)
	require.NoError(t, err)
	timestampValue, err := types.ParseTimestamp(jsonSessionTimeZone(proc), "2024-02-03 04:05:06.12", 2)
	require.NoError(t, err)

	for _, tc := range []struct {
		name string
		key  FunctionTestInput
		want string
	}{
		{
			name: "time keeps declared scale",
			key: NewFunctionTestInput(types.New(types.T_time, 0, 0),
				[]types.Time{timeValue}, []bool{false}),
			want: `{"04:05:06": 1}`,
		},
		{
			name: "binary keeps legacy base64 key",
			key: NewFunctionTestInput(types.T_binary.ToType(),
				[]string{"\x00\xff"}, []bool{false}),
			want: `{"AP8=": 1}`,
		},
		{
			name: "datetime keeps declared scale",
			key: NewFunctionTestInput(types.New(types.T_datetime, 0, 2),
				[]types.Datetime{datetimeValue}, []bool{false}),
			want: `{"2024-02-03 04:05:06.12": 1}`,
		},
		{
			name: "timestamp keeps session timezone and declared scale",
			key: NewFunctionTestInput(types.New(types.T_timestamp, 0, 2),
				[]types.Timestamp{timestampValue}, []bool{false}),
			want: `{"2024-02-03 04:05:06.12": 1}`,
		},
		{
			name: "year remains a member name",
			key: NewFunctionTestInput(types.T_year.ToType(),
				[]types.MoYear{2024}, []bool{false}),
			want: `{"2024": 1}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vec := runJsonFunctionWithSelectList(t, proc,
				[]FunctionTestInput{
					tc.key,
					NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, []bool{false}),
				},
				types.T_json.ToType(), newOpBuiltInJsonObject().jsonObject, nil)
			require.Equal(t, tc.want, jsonVectorRowString(t, vec, 0))
		})
	}
}

func TestJsonConstructorBinaryValuesUseAdmittedProtocolVersion(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	original, hadOriginal := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadOriginal {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, original)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	tests := []struct {
		name   string
		typ    types.Type
		append func(*vector.Vector, *mpool.MPool) error
	}{
		{
			name: "binary", typ: types.T_binary.ToType(),
			append: func(v *vector.Vector, mp *mpool.MPool) error {
				return vector.AppendBytes(v, []byte("binary"), false, mp)
			},
		},
		{
			name: "varbinary", typ: types.T_varbinary.ToType(),
			append: func(v *vector.Vector, mp *mpool.MPool) error {
				return vector.AppendBytes(v, []byte("varbinary"), false, mp)
			},
		},
		{
			name: "blob", typ: types.T_blob.ToType(),
			append: func(v *vector.Vector, mp *mpool.MPool) error {
				return vector.AppendBytes(v, []byte("blob"), false, mp)
			},
		},
		{
			name: "bit", typ: types.New(types.T_bit, 9, 0),
			append: func(v *vector.Vector, mp *mpool.MPool) error {
				return vector.AppendFixed(v, uint64(0x101), false, mp)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			v := vector.NewVec(tc.typ)
			require.NoError(t, tc.append(v, mp))
			defer v.Free(mp)

			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion51)
			_, err := newOpBuiltInJsonArray().convertToAny(proc, v, 0, jsonSessionProtocolVersion(proc))
			require.ErrorContains(t, err, "MORPC protocol version 52")

			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion52)
			value, err := newOpBuiltInJsonArray().convertToAny(proc, v, 0, jsonSessionProtocolVersion(proc))
			require.NoError(t, err)
			_, err = bytejson.CreateByteJSON(value)
			require.NoError(t, err)
		})
	}
}

func TestJsonConstructorPreparedMetadata(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, tc := range []struct {
		name        string
		typ         types.T
		binary      bool
		kind        vector.PrepareParamKind
		input, want string
	}{
		{"binary", types.T_binary, true, vector.PrepareParamNone, "ab", `"base64:type254:YWI="`},
		{"varbinary", types.T_varbinary, true, vector.PrepareParamNone, "ab", `"base64:type15:YWI="`},
		{"blob", types.T_blob, true, vector.PrepareParamNone, "ab", `"base64:type252:YWI="`},
		{"float32", types.T_float32, false, vector.PrepareParamFloat, "0.1", "0.10000000149011612"},
		{"kind_only", types.T_any, false, vector.PrepareParamInteger, "42", "42"},
		{"text", types.T_any, false, vector.PrepareParamNone, "ab", `"ab"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := vector.NewVec(types.T_text.ToType())
			defer v.Free(proc.Mp())
			require.NoError(t, vector.AppendBytes(v, []byte(tc.input), false, proc.Mp()))
			v.SetPrepareParamType(tc.typ)
			v.SetPrepareParamKind(tc.kind)
			v.SetIsBinaryString(tc.binary)
			value, err := newOpBuiltInJsonArray().convertToAny(proc, v, 0, defines.MORPCVersion52)
			require.NoError(t, err)
			got, err := bytejson.CreateByteJSON(value)
			require.NoError(t, err)
			require.Equal(t, tc.want, got.String())
			if tc.binary {
				key, err := newOpBuiltInJsonObject().convertKeyToAny(proc, newOpBuiltInJsonArray(), v, 0, defines.MORPCVersion51)
				require.NoError(t, err)
				require.Equal(t, tc.input, key)
			}
		})
	}
}

func TestJsonObjectBitKeyPreservesLegacyNameAndTaggedValue(t *testing.T) {
	proc := testutil.NewProcess(t)
	vec := runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.New(types.T_bit, 9, 0), []uint64{0x101}, []bool{false}),
			NewFunctionTestInput(types.New(types.T_bit, 4, 0), []uint64{0xa}, []bool{false}),
		},
		types.T_json.ToType(), newOpBuiltInJsonObject().jsonObject, nil)

	got := jsonVectorRowString(t, vec, 0)
	require.Equal(t, `{"AQE=": "base64:type16:Cg=="}`, got)

	document, err := bytejson.ParseFromString(got)
	require.NoError(t, err)
	path, err := types.ParseStringToPath(`$."AQE="`)
	require.NoError(t, err)
	member, exists := document.QuerySimpleExist(&path)
	require.True(t, exists)
	require.Equal(t, `"base64:type16:Cg=="`, member.String())
}

func TestJsonContainsNumericEqualDecimalAndFloat(t *testing.T) {
	scientificFloat, err := bytejson.CreateByteJSON(1e20)
	require.NoError(t, err)
	fractionalFloat, err := bytejson.CreateByteJSON(0.1)
	require.NoError(t, err)

	tests := []struct {
		name      string
		target    bytejson.ByteJson
		candidate bytejson.ByteJson
		expect    bool
	}{
		{
			name:      "decimal equals scientific float",
			target:    newTypedByteJson(bytejson.TpCodeDecimal, "100000000000000000000"),
			candidate: scientificFloat,
			expect:    true,
		},
		{
			name:      "scientific float equals decimal",
			target:    scientificFloat,
			candidate: newTypedByteJson(bytejson.TpCodeDecimal, "100000000000000000000"),
			expect:    true,
		},
		{
			name:      "decimal differs from nearby scientific float",
			target:    newTypedByteJson(bytejson.TpCodeDecimal, "100000000000000000001"),
			candidate: scientificFloat,
			expect:    false,
		},
		{
			name:      "decimal equals fractional float",
			target:    newTypedByteJson(bytejson.TpCodeDecimal, "0.1"),
			candidate: fractionalFloat,
			expect:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expect, jsonContainsNumericEqual(tt.target, tt.candidate))
		})
	}
}

func TestJsonContainsBinaryUsesRawPayloadAndSubtype(t *testing.T) {
	legacyBlob := newTypedByteJson(bytejson.TpCodeBlob, "AA==")
	rawBlob := newTypedByteJson(bytejson.TpCodeOpaque, string([]byte{0x00}))
	bit := newTypedByteJson(bytejson.TpCodeBit, string([]byte{0x00}))

	require.True(t, jsonContainsScalar(legacyBlob, rawBlob))
	require.True(t, jsonContainsScalar(rawBlob, legacyBlob))
	require.False(t, jsonContainsScalar(bit, rawBlob))
}

// ============================================================================
// computeString / computeStringSimple (use raw JSON text, not internal encoding)
// ============================================================================

func makePath(t *testing.T, s string) *bytejson.Path {
	p, err := types.ParseStringToPath(s)
	require.NoError(t, err)
	return &p
}

func TestComputeString_ValidJson(t *testing.T) {
	raw := []byte(`{"a": 1, "b": "hello"}`)
	paths := []*bytejson.Path{makePath(t, "$.a")}

	bj, err := computeString(raw, paths)
	require.NoError(t, err)
	require.NotEqual(t, bytejson.Null, bj)
	require.Equal(t, bytejson.TpCodeInt64, bj.Type)
}

func TestComputeString_InvalidJson(t *testing.T) {
	raw := []byte(`not json`)
	paths := []*bytejson.Path{makePath(t, "$.a")}

	bj, err := computeString(raw, paths)
	require.Error(t, err)
	require.Equal(t, bytejson.Null, bj)
}

func TestComputeString_EmptyArray(t *testing.T) {
	raw := []byte(`[]`)
	paths := []*bytejson.Path{makePath(t, "$[0]")}

	bj, err := computeString(raw, paths)
	require.NoError(t, err)
	require.Equal(t, bytejson.Null, bj)
}

func TestComputeString_StringValue(t *testing.T) {
	raw := []byte(`{"a": "hello"}`)
	paths := []*bytejson.Path{makePath(t, "$.a")}

	bj, err := computeString(raw, paths)
	require.NoError(t, err)
	require.NotEqual(t, bytejson.Null, bj)
	require.Equal(t, bytejson.TpCodeString, bj.Type)
}

func TestComputeStringSimple_ValidJson(t *testing.T) {
	raw := []byte(`{"a": 1, "b": "hello"}`)
	paths := []*bytejson.Path{makePath(t, "$.a")}

	bj, err := computeStringSimple(raw, paths)
	require.NoError(t, err)
	require.NotEqual(t, bytejson.Null, bj)
	require.Equal(t, bytejson.TpCodeInt64, bj.Type)
}

func TestComputeStringSimple_InvalidJson(t *testing.T) {
	raw := []byte(`not json`)
	paths := []*bytejson.Path{makePath(t, "$.a")}

	bj, err := computeStringSimple(raw, paths)
	require.Error(t, err)
	require.Equal(t, bytejson.Null, bj)
}

func TestComputeStringSimple_SimplePath(t *testing.T) {
	raw := []byte(`{"a": {"b": 99}}`)
	paths := []*bytejson.Path{makePath(t, "$.a.b")}

	bj, err := computeStringSimple(raw, paths)
	require.NoError(t, err)
	require.NotEqual(t, bytejson.Null, bj)
	require.Equal(t, bytejson.TpCodeInt64, bj.Type)
}

// ============================================================================
// computeStringJsonSet / computeStringJsonInsert / computeStringJsonReplace
// ============================================================================

func makeByteJsonInt(v int64) bytejson.ByteJson {
	bj, _ := types.ParseSliceToByteJson([]byte(itoa(v)))
	return bj
}

func itoa(v int64) string {
	if v < 0 {
		return "-" + itoa(-v)
	}
	if v < 10 {
		return string([]byte{byte('0' + v)})
	}
	return itoa(v/10) + itoa(v%10)
}

func TestComputeStringJsonSet_NewKey(t *testing.T) {
	raw := []byte(`{"a": 1}`)
	paths := []*bytejson.Path{makePath(t, "$.b")}
	newVal := []bytejson.ByteJson{makeByteJsonInt(42)}

	bj, err := computeStringJsonSet(raw, paths, newVal)
	require.NoError(t, err)
	require.NotEqual(t, bytejson.Null, bj)
}

func TestComputeStringJsonSet_OverwriteKey(t *testing.T) {
	raw := []byte(`{"a": 1}`)
	paths := []*bytejson.Path{makePath(t, "$.a")}
	newVal := []bytejson.ByteJson{makeByteJsonInt(99)}

	bj, err := computeStringJsonSet(raw, paths, newVal)
	require.NoError(t, err)
	require.NotEqual(t, bytejson.Null, bj)
}

func TestComputeStringJsonInsert_NewKey(t *testing.T) {
	raw := []byte(`{"a": 1}`)
	paths := []*bytejson.Path{makePath(t, "$.b")}
	newVal := []bytejson.ByteJson{makeByteJsonInt(42)}

	bj, err := computeStringJsonInsert(raw, paths, newVal)
	require.NoError(t, err)
	require.NotEqual(t, bytejson.Null, bj)
}

func TestComputeStringJsonInsert_ExistingKey(t *testing.T) {
	raw := []byte(`{"a": 1}`)
	paths := []*bytejson.Path{makePath(t, "$.a")}
	newVal := []bytejson.ByteJson{makeByteJsonInt(99)}

	// INSERT should NOT overwrite existing key
	bj, err := computeStringJsonInsert(raw, paths, newVal)
	require.NoError(t, err)
	require.NotEqual(t, bytejson.Null, bj)
}

func TestComputeStringJsonReplace_ExistingKey(t *testing.T) {
	raw := []byte(`{"a": 1}`)
	paths := []*bytejson.Path{makePath(t, "$.a")}
	newVal := []bytejson.ByteJson{makeByteJsonInt(99)}

	bj, err := computeStringJsonReplace(raw, paths, newVal)
	require.NoError(t, err)
	require.NotEqual(t, bytejson.Null, bj)
}

func TestComputeStringJsonReplace_MissingKey(t *testing.T) {
	raw := []byte(`{"a": 1}`)
	paths := []*bytejson.Path{makePath(t, "$.missing")}
	newVal := []bytejson.ByteJson{makeByteJsonInt(99)}

	// REPLACE does nothing for missing key
	bj, err := computeStringJsonReplace(raw, paths, newVal)
	require.NoError(t, err)
	require.NotEqual(t, bytejson.Null, bj)
}

// ============================================================================
// getPaths
// ============================================================================

func TestGetPaths_AllConst(t *testing.T) {
	paths := []*bytejson.Path{
		makePath(t, "$.a"),
		makePath(t, "$.b"),
	}
	op := &opBuiltInJsonExtract{
		allConst: true,
		npath:    2,
		paths:    paths,
	}

	// allConst=true returns full paths slice regardless of index
	result := op.getPaths(0)
	require.Equal(t, paths, result)

	result = op.getPaths(999)
	require.Equal(t, paths, result)
}

func TestGetPaths_NonConst(t *testing.T) {
	p0 := makePath(t, "$.a")
	p1 := makePath(t, "$.b")
	p2 := makePath(t, "$.c")
	p3 := makePath(t, "$.d")

	paths := []*bytejson.Path{p0, p1, p2, p3}
	op := &opBuiltInJsonExtract{
		allConst: false,
		npath:    2,
		paths:    paths,
	}

	// Row 0: paths[0:2]
	result := op.getPaths(0)
	require.Equal(t, []*bytejson.Path{p0, p1}, result)

	// Row 1: paths[2:4]
	result = op.getPaths(1)
	require.Equal(t, []*bytejson.Path{p2, p3}, result)
}
