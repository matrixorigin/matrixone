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
	"encoding/binary"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestJsonStorageRegistrationAndTypeCheck(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		fid  int32
	}{
		{name: "json_storage_size", fid: JSON_STORAGE_SIZE},
		{name: "json_storage_free", fid: JSON_STORAGE_FREE},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, typ := range []types.Type{
				types.T_json.ToType(),
				types.T_char.ToType(),
				types.T_varchar.ToType(),
				types.T_text.ToType(),
				types.T_binary.ToType(),
				types.T_varbinary.ToType(),
				types.T_blob.ToType(),
				types.T_any.ToType(),
			} {
				resolved, err := GetFunctionByName(ctx, tc.name, []types.Type{typ})
				require.NoError(t, err, typ)
				require.Equal(t, tc.fid, resolved.fid)
				require.Equal(t, types.T_int64, resolved.retType.Oid)
			}

			for _, typ := range []types.Type{
				types.T_bool.ToType(), types.T_bit.ToType(),
				types.T_int8.ToType(), types.T_int16.ToType(),
				types.T_int32.ToType(), types.T_int64.ToType(),
				types.T_uint8.ToType(), types.T_uint16.ToType(),
				types.T_uint32.ToType(), types.T_uint64.ToType(),
				types.T_float32.ToType(), types.T_float64.ToType(),
				types.T_decimal64.ToType(), types.T_decimal128.ToType(),
				types.T_decimal256.ToType(), types.T_year.ToType(),
				types.T_date.ToType(), types.T_time.ToType(),
				types.T_datetime.ToType(), types.T_timestamp.ToType(),
				types.T_enum.ToType(), types.T_uuid.ToType(),
				types.T_geometry.ToType(), types.T_geometry32.ToType(),
				types.T_array_float32.ToType(), types.T_array_float64.ToType(),
			} {
				_, err := GetFunctionByName(ctx, tc.name, []types.Type{typ})
				require.Error(t, err, typ)
			}

			_, err := GetFunctionByName(ctx, tc.name, nil)
			require.Error(t, err)
			_, err = GetFunctionByName(ctx, tc.name, []types.Type{types.T_json.ToType(), types.T_json.ToType()})
			require.Error(t, err)
		})
	}
}

func TestJsonStorageSize(t *testing.T) {
	proc := testutil.NewProcess(t)
	texts := []string{"null", "true", `""`, `"x"`, `[]`, `{}`, `[1,2,3]`, `{"a":1}`}
	want := []int64{2, 2, 2, 3, 9, 9, 48, 29}

	t.Run("empty batch", func(t *testing.T) {
		fc := NewFunctionTestCase(proc,
			[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), []string{}, nil)},
			NewFunctionTestResult(types.T_int64.ToType(), false, []int64{}, []bool{}),
			JsonStorageSize)
		succeed, info := fc.Run()
		require.True(t, succeed, info)
		require.Zero(t, fc.GetResultVectorDirectly().Length())
	})

	t.Run("varchar", func(t *testing.T) {
		fc := NewFunctionTestCase(proc,
			[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), texts, nil)},
			NewFunctionTestResult(types.T_int64.ToType(), false, want, nil),
			JsonStorageSize)
		succeed, info := fc.Run()
		require.True(t, succeed, info)
	})
	for _, typ := range []types.Type{
		types.T_char.ToType(), types.T_text.ToType(),
		types.T_binary.ToType(), types.T_varbinary.ToType(), types.T_blob.ToType(),
	} {
		t.Run(typ.String(), func(t *testing.T) {
			fc := NewFunctionTestCase(proc,
				[]FunctionTestInput{NewFunctionTestInput(typ, texts, nil)},
				NewFunctionTestResult(types.T_int64.ToType(), false, want, nil),
				JsonStorageSize)
			succeed, info := fc.Run()
			require.True(t, succeed, info)
		})
	}

	encoded := make([]string, len(texts))
	for i, text := range texts {
		encoded[i] = mustJsonBinaryString(t, text)
	}
	t.Run("json", func(t *testing.T) {
		fc := NewFunctionTestCase(proc,
			[]FunctionTestInput{NewFunctionTestInput(types.T_json.ToType(), encoded, nil)},
			NewFunctionTestResult(types.T_int64.ToType(), false, want, nil),
			JsonStorageSize)
		succeed, info := fc.Run()
		require.True(t, succeed, info)
	})

	t.Run("null propagation and const", func(t *testing.T) {
		fc := NewFunctionTestCase(proc,
			[]FunctionTestInput{NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"", ""}, []bool{true, true})},
			NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0, 0}, []bool{true, true}),
			JsonStorageSize)
		succeed, info := fc.Run()
		require.True(t, succeed, info)
	})

	t.Run("select list skips malformed row", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"not-json", `"x"`}, nil),
			}, types.T_int64.ToType(), JsonStorageSize,
			&FunctionSelectList{AnyNull: true, SelectList: []bool{false, true}})
		require.True(t, vec.IsNull(0))
		value, isNull := vector.GenerateFunctionFixedTypeParameter[int64](vec).GetValue(1)
		require.False(t, isNull)
		require.Equal(t, int64(3), value)
	})
}

func TestJsonStorageFree(t *testing.T) {
	proc := testutil.NewProcess(t)
	texts := []string{"null", "true", `"x"`, `[]`, `{"a":1}`}
	want := []int64{0, 0, 0, 0, 0}
	for _, typ := range []types.Type{types.T_varchar.ToType(), types.T_json.ToType()} {
		values := texts
		if typ.Oid == types.T_json {
			values = make([]string, len(texts))
			for i, text := range texts {
				values[i] = mustJsonBinaryString(t, text)
			}
		}
		fc := NewFunctionTestCase(proc,
			[]FunctionTestInput{NewFunctionTestInput(typ, values, nil)},
			NewFunctionTestResult(types.T_int64.ToType(), false, want, nil),
			JsonStorageFree)
		succeed, info := fc.Run()
		require.True(t, succeed, "%s: %s", typ, info)
	}

	t.Run("const null", func(t *testing.T) {
		fc := NewFunctionTestCase(proc,
			[]FunctionTestInput{NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"", ""}, []bool{true, true})},
			NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0, 0}, []bool{true, true}),
			JsonStorageFree)
		succeed, info := fc.Run()
		require.True(t, succeed, info)
	})

	t.Run("select list skips malformed row", func(t *testing.T) {
		vec := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"not-json", `"x"`}, nil),
			}, types.T_int64.ToType(), JsonStorageFree,
			&FunctionSelectList{AnyNull: true, SelectList: []bool{false, true}})
		require.True(t, vec.IsNull(0))
		value, isNull := vector.GenerateFunctionFixedTypeParameter[int64](vec).GetValue(1)
		require.False(t, isNull)
		require.Zero(t, value)
	})

	t.Run("empty batch", func(t *testing.T) {
		fc := NewFunctionTestCase(proc,
			[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), []string{}, nil)},
			NewFunctionTestResult(types.T_int64.ToType(), false, []int64{}, []bool{}),
			JsonStorageFree)
		succeed, info := fc.Run()
		require.True(t, succeed, info)
		require.Zero(t, fc.GetResultVectorDirectly().Length())
	})
}

func TestJsonStorageRejectsInvalidJSON(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, fn := range []fEvalFn{JsonStorageSize, JsonStorageFree} {
		fc := NewFunctionTestCase(proc,
			[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), []string{"not-json"}, nil)},
			NewFunctionTestResult(types.T_int64.ToType(), true, nil, nil), fn)
		succeed, info := fc.Run()
		require.True(t, succeed, info)
	}
}

func TestJsonStorageSizeNumericTextUsesByteJsonMarshal(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	texts := []string{
		"-1",
		"0",
		"18446744073709551615",
		"1.2300",
		"-0.000",
		"1e+20",
	}
	want := make([]int64, len(texts))
	for i, text := range texts {
		bj, err := types.ParseStringToByteJson(text)
		require.NoError(t, err, text)
		encoded, err := bj.Marshal()
		require.NoError(t, err, text)
		want[i] = int64(len(encoded))
	}

	fc := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), texts, nil)},
		NewFunctionTestResult(types.T_int64.ToType(), false, want, nil),
		JsonStorageSize)
	succeed, info := fc.Run()
	require.True(t, succeed, info)
}

func TestJsonStorageSizeUsesPersistedSpecialByteJson(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	newBinaryValue := func(tp bytejson.TpCode, payload []byte) bytejson.ByteJson {
		data := binary.AppendUvarint(nil, uint64(len(payload)))
		data = append(data, payload...)
		return bytejson.ByteJson{Type: tp, Data: data}
	}
	opaque := newBinaryValue(bytejson.TpCodeOpaque, []byte{0x01, 0x02})
	bit := newBinaryValue(bytejson.TpCodeBit, []byte{0x03})
	nested, err := bytejson.CreateByteJSON([]any{opaque, bit})
	require.NoError(t, err)

	input := vector.NewVec(types.T_json.ToType())
	defer input.Free(proc.Mp())
	values := []bytejson.ByteJson{opaque, bit, nested}
	want := make([]int64, len(values))
	for i, value := range values {
		encoded, marshalErr := value.Marshal()
		require.NoError(t, marshalErr)
		want[i] = int64(len(encoded))
		require.NoError(t, vector.AppendByteJson(input, value, false, proc.Mp()))
		require.Equal(t, encoded, input.GetBytesAt(i), "row %d must use persisted bytes", i)
	}

	result := vector.NewFunctionResultWrapper(types.T_int64.ToType(), proc.Mp())
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(len(values)))
	require.NoError(t, JsonStorageSize([]*vector.Vector{input}, result, proc, len(values), nil))
	got := vector.GenerateFunctionFixedTypeParameter[int64](result.GetResultVector())
	for i, expected := range want {
		value, isNull := got.GetValue(uint64(i))
		require.False(t, isNull)
		require.Equal(t, expected, value, "row %d", i)
	}
}

func TestJsonStorageRejectsPreparedNonStringDomains(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	for _, fn := range []fEvalFn{JsonStorageSize, JsonStorageFree} {
		fc := NewFunctionTestCase(proc,
			[]FunctionTestInput{NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, nil)},
			NewFunctionTestResult(types.T_int64.ToType(), true, nil, nil), fn)
		succeed, info := fc.Run()
		require.True(t, succeed, info)
	}
	for _, fn := range []fEvalFn{JsonStorageSize, JsonStorageFree} {
		for _, tc := range []struct {
			name string
			typ  types.T
			kind vector.PrepareParamKind
		}{
			{name: "integer", typ: types.T_int64, kind: vector.PrepareParamInteger},
			{name: "unsigned", typ: types.T_uint64, kind: vector.PrepareParamInteger},
			{name: "float", typ: types.T_float64, kind: vector.PrepareParamFloat},
			{name: "decimal", typ: types.T_decimal128, kind: vector.PrepareParamDecimal},
			{name: "boolean", typ: types.T_bool, kind: vector.PrepareParamBoolean},
			{name: "enum", typ: types.T_enum, kind: vector.PrepareParamNone},
			{name: "date", typ: types.T_date, kind: vector.PrepareParamNone},
			{name: "uuid", typ: types.T_uuid, kind: vector.PrepareParamNone},
			{name: "geometry", typ: types.T_geometry, kind: vector.PrepareParamNone},
		} {
			t.Run(tc.name, func(t *testing.T) {
				input := vector.NewVec(types.T_varchar.ToType())
				defer input.Free(proc.Mp())
				require.NoError(t, vector.AppendBytes(input, []byte("1"), false, proc.Mp()))
				input.SetPrepareParamKind(tc.kind)
				input.SetPrepareParamType(tc.typ)
				result := vector.NewFunctionResultWrapper(types.T_int64.ToType(), proc.Mp())
				defer result.Free()
				require.NoError(t, result.PreExtendAndReset(1))
				require.Error(t, fn([]*vector.Vector{input}, result, proc, 1, nil), tc.name)
			})
		}
	}

	fc := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), []string{"1"}, nil)},
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{9}, nil),
		JsonStorageSize)
	fc.parameters[0].SetPrepareParamKind(vector.PrepareParamNone)
	fc.parameters[0].SetPrepareParamType(types.T_varchar)
	succeed, info := fc.Run()
	require.True(t, succeed, info)
}

func TestJsonStorageErrorRecoveryAfterInvalidText(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	for _, tc := range []struct {
		name string
		fn   fEvalFn
		want int64
	}{
		{name: "size", fn: JsonStorageSize, want: 4},
		{name: "free", fn: JsonStorageFree, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := vector.NewVec(types.T_varchar.ToType())
			defer input.Free(proc.Mp())
			result := vector.NewFunctionResultWrapper(types.T_int64.ToType(), proc.Mp())
			defer result.Free()
			run := func(text string) error {
				input.ResetWithSameType()
				require.NoError(t, vector.AppendBytes(input, []byte(text), false, proc.Mp()))
				require.NoError(t, result.PreExtendAndReset(1))
				return tc.fn([]*vector.Vector{input}, result, proc, 1, nil)
			}

			require.NoError(t, run(`{"a":1}`))
			require.Error(t, run(`{"a":`))
			require.NoError(t, run(`"ok"`))
			value, isNull := vector.GenerateFunctionFixedTypeParameter[int64](result.GetResultVector()).GetValue(0)
			require.False(t, isNull)
			require.Equal(t, tc.want, value)
		})
	}
}
