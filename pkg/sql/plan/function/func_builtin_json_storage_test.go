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
	"testing"

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
				types.T_bool.ToType(), types.T_int64.ToType(),
				types.T_date.ToType(), types.T_geometry.ToType(),
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
