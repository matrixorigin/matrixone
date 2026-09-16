// Copyright 2026 Matrix Origin
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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestJsonDepthRegistrationAndTypeCheck(t *testing.T) {
	ctx := context.Background()
	for _, typ := range []types.Type{
		types.T_json.ToType(),
		types.T_char.ToType(),
		types.T_varchar.ToType(),
		types.T_text.ToType(),
		types.T_any.ToType(),
	} {
		resolved, err := GetFunctionByName(ctx, "json_depth", []types.Type{typ})
		require.NoError(t, err, typ)
		require.Equal(t, int32(JSON_DEPTH), resolved.fid)
		require.Equal(t, int32(0), resolved.overloadId)
		require.Equal(t, types.T_int64, resolved.retType.Oid)
		if typ.Oid == types.T_any {
			require.True(t, resolved.needCast)
			require.Equal(t, types.T_varchar, resolved.targetTypes[0].Oid)
		}
	}

	for _, typ := range []types.Type{
		types.T_bool.ToType(),
		types.T_int64.ToType(),
		types.T_float64.ToType(),
		types.T_binary.ToType(),
		types.T_varbinary.ToType(),
		types.T_blob.ToType(),
		types.T_date.ToType(),
		types.T_geometry.ToType(),
		types.T_array_float32.ToType(),
		types.NewWithCharset(types.T_varchar, 16, 0, types.CharsetBinary),
	} {
		_, err := GetFunctionByName(ctx, "json_depth", []types.Type{typ})
		require.Error(t, err, typ)
	}
	_, err := GetFunctionByName(ctx, "json_depth", nil)
	require.Error(t, err)
}

func TestJsonDepthTextAndTypedJSON(t *testing.T) {
	proc := testutil.NewProcess(t)
	texts := []string{
		`null`, `true`, `0`, `"叶"`, `[]`, `{}`,
		`[null, 1, {"值": [false, "文字"]}]`,
		`{"a": [1]}`,
		`[{"a": 1}, [2, [3]]]`,
	}
	want := []int64{1, 1, 1, 1, 1, 1, 4, 3, 4}

	for _, typ := range []types.Type{types.T_varchar.ToType(), types.T_char.ToType(), types.T_text.ToType()} {
		t.Run(typ.String(), func(t *testing.T) {
			fc := NewFunctionTestCase(proc,
				[]FunctionTestInput{NewFunctionTestInput(typ, texts, nil)},
				NewFunctionTestResult(types.T_int64.ToType(), false, want, nil),
				JsonDepth)
			succeed, info := fc.Run()
			require.True(t, succeed, info)
		})
	}

	encoded := make([]string, len(texts))
	for i, raw := range texts {
		encoded[i] = mustJsonBinaryString(t, raw)
	}
	fc := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_json.ToType(), encoded, nil)},
		NewFunctionTestResult(types.T_int64.ToType(), false, want, nil),
		JsonDepth)
	succeed, info := fc.Run()
	require.True(t, succeed, info)

	fc = NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), []string{"", `1`}, []bool{true, false})},
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0, 1}, []bool{true, false}),
		JsonDepth)
	succeed, info = fc.Run()
	require.True(t, succeed, info)
}

func TestJsonDepthRejectsMalformedTypeAndBinaryDomain(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, input := range []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{`not-json`}, nil),
		NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, nil),
		NewFunctionTestInput(types.T_bool.ToType(), []bool{true}, nil),
		NewFunctionTestInput(types.T_binary.ToType(), []string{`1`}, nil),
	} {
		fc := NewFunctionTestCase(proc,
			[]FunctionTestInput{input},
			NewFunctionTestResult(types.T_int64.ToType(), true, nil, nil),
			JsonDepth)
		succeed, info := fc.Run()
		require.True(t, succeed, info)
	}

	fc := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.NewWithCharset(types.T_varchar, 16, 0, types.CharsetBinary), []string{`1`}, nil)},
		NewFunctionTestResult(types.T_int64.ToType(), true, nil, nil),
		JsonDepth)
	succeed, info := fc.Run()
	require.True(t, succeed, info)

	input := vector.NewVec(types.T_varchar.ToType())
	defer input.Free(proc.Mp())
	require.NoError(t, vector.AppendBytes(input, []byte(`1`), false, proc.Mp()))
	input.SetIsBinaryString(true)
	result := vector.NewFunctionResultWrapper(types.T_int64.ToType(), proc.Mp())
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(1))
	require.Error(t, JsonDepth([]*vector.Vector{input}, result, proc, 1, nil))
}

func TestJsonDepthRejectsEmptyTypedPayloadWithoutPanic(t *testing.T) {
	proc := testutil.NewProcess(t)
	// Public T_json append paths reject malformed payloads. Build the
	// representation as text, then change only the test vector metadata so the
	// executor's fail-closed boundary is exercised directly.
	input := vector.NewVec(types.T_varchar.ToType())
	defer input.Free(proc.Mp())
	require.NoError(t, vector.AppendBytes(input, []byte{}, false, proc.Mp()))
	input.SetType(types.T_json.ToType())

	result := vector.NewFunctionResultWrapper(types.T_int64.ToType(), proc.Mp())
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(1))

	var got error
	require.NotPanics(t, func() {
		got = JsonDepth([]*vector.Vector{input}, result, proc, 1, nil)
	})
	require.ErrorContains(t, got, "invalid argument json_depth, bad value invalid JSON document")
}

func TestJsonDepthPreparedProvenanceAndSelectList(t *testing.T) {
	proc := testutil.NewProcess(t)

	input := vector.NewVec(types.T_varchar.ToType())
	defer input.Free(proc.Mp())
	require.NoError(t, vector.AppendBytes(input, []byte(`1`), false, proc.Mp()))
	input.SetPrepareParamKind(vector.PrepareParamInteger)
	input.SetPrepareParamType(types.T_int64)
	result := vector.NewFunctionResultWrapper(types.T_int64.ToType(), proc.Mp())
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(1))
	require.Error(t, JsonDepth([]*vector.Vector{input}, result, proc, 1, nil))

	input.ResetWithSameType()
	require.NoError(t, vector.AppendBytes(input, []byte(`1`), false, proc.Mp()))
	input.SetPrepareParamKind(vector.PrepareParamNone)
	input.SetPrepareParamType(types.T_varchar)
	require.NoError(t, result.PreExtendAndReset(1))
	require.NoError(t, JsonDepth([]*vector.Vector{input}, result, proc, 1, nil))
	value, isNull := vector.GenerateFunctionFixedTypeParameter[int64](result.GetResultVector()).GetValue(0)
	require.False(t, isNull)
	require.Equal(t, int64(1), value)

	input.ResetWithSameType()
	require.NoError(t, vector.AppendBytes(input, []byte(`{"a":[1]}`), false, proc.Mp()))
	input.SetPrepareParamKind(vector.PrepareParamNone)
	input.SetPrepareParamType(types.T_any)
	require.NoError(t, result.PreExtendAndReset(1))
	require.NoError(t, JsonDepth([]*vector.Vector{input}, result, proc, 1, nil))
	value, isNull = vector.GenerateFunctionFixedTypeParameter[int64](result.GetResultVector()).GetValue(0)
	require.False(t, isNull)
	require.Equal(t, int64(3), value)

	input.ResetWithSameType()
	require.NoError(t, vector.AppendBytes(input, []byte(`1`), true, proc.Mp()))
	input.SetPrepareParamKind(vector.PrepareParamInteger)
	input.SetPrepareParamType(types.T_int64)
	require.NoError(t, result.PreExtendAndReset(1))
	require.NoError(t, JsonDepth([]*vector.Vector{input}, result, proc, 1, nil))
	_, isNull = vector.GenerateFunctionFixedTypeParameter[int64](result.GetResultVector()).GetValue(0)
	require.True(t, isNull)

	vec := runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{`not-json`, `{"a":[1]}`}, nil),
		}, types.T_int64.ToType(), JsonDepth,
		&FunctionSelectList{AnyNull: true, SelectList: []bool{false, true}})
	require.True(t, vec.IsNull(0))
	value, isNull = vector.GenerateFunctionFixedTypeParameter[int64](vec).GetValue(1)
	require.False(t, isNull)
	require.Equal(t, int64(3), value)
}

func TestJsonDepthLimitAndErrorRecovery(t *testing.T) {
	proc := testutil.NewProcess(t)
	limit := bytejson.JSONDocumentMaxNestingDepth
	atLimit := strings.Repeat(`{"a":`, limit) + `1` + strings.Repeat(`}`, limit)
	tooDeep := strings.Repeat(`{"a":`, limit+1) + `1` + strings.Repeat(`}`, limit+1)

	fc := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), []string{atLimit}, nil)},
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{int64(limit + 1)}, nil),
		JsonDepth)
	succeed, info := fc.Run()
	require.True(t, succeed, info)

	fc = NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), []string{tooDeep}, nil)},
		NewFunctionTestResult(types.T_int64.ToType(), true, nil, nil),
		JsonDepth)
	succeed, info = fc.Run()
	require.True(t, succeed, info)

	input := vector.NewVec(types.T_varchar.ToType())
	defer input.Free(proc.Mp())
	result := vector.NewFunctionResultWrapper(types.T_int64.ToType(), proc.Mp())
	defer result.Free()
	run := func(raw string) error {
		input.ResetWithSameType()
		require.NoError(t, vector.AppendBytes(input, []byte(raw), false, proc.Mp()))
		require.NoError(t, result.PreExtendAndReset(1))
		return JsonDepth([]*vector.Vector{input}, result, proc, 1, nil)
	}
	require.NoError(t, run(`{"a":[1]}`))
	require.Error(t, run(`not-json`))
	require.NoError(t, run(`{"a": {"b": 1}}`))
	value, isNull := vector.GenerateFunctionFixedTypeParameter[int64](result.GetResultVector()).GetValue(0)
	require.False(t, isNull)
	require.Equal(t, int64(3), value)
}
