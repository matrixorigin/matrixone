// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	stdcrc32 "hash/crc32"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestCRC32AcceptsScalarArgumentsWithoutChangingStringDomains(t *testing.T) {
	proc := testutil.NewProcess(t)

	for _, tc := range []struct {
		name string
		typ  types.Type
	}{
		{name: "bool", typ: types.T_bool.ToType()},
		{name: "signed integer", typ: types.T_int64.ToType()},
		{name: "unsigned integer", typ: types.T_uint64.ToType()},
		{name: "decimal", typ: types.New(types.T_decimal64, 8, 2)},
		{name: "double", typ: types.T_float64.ToType()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolved, err := GetFunctionByName(proc.Ctx, "crc32", []types.Type{tc.typ})
			require.NoError(t, err)
			require.True(t, resolved.needCast)
			require.Len(t, resolved.targetTypes, 1)
			require.Equal(t, types.T_varchar, resolved.targetTypes[0].Oid)
		})
	}

	for _, typ := range []types.T{
		types.T_char,
		types.T_varchar,
		types.T_text,
		types.T_binary,
		types.T_varbinary,
		types.T_blob,
		types.T_json,
		types.T_array_float32,
	} {
		t.Run(typ.String(), func(t *testing.T) {
			resolved, err := GetFunctionByName(proc.Ctx, "crc32", []types.Type{typ.ToType()})
			require.NoError(t, err)
			require.False(t, resolved.needCast)
		})
	}
}

func TestCRC32Uint64ResultWrapper(t *testing.T) {
	proc := testutil.NewProcess(t)
	input := NewFunctionTestInput(types.T_varchar.ToType(), []string{"hello", ""}, []bool{false, false})
	result := NewFunctionTestResult(types.T_uint64.ToType(), false,
		[]uint64{907060870, 0}, []bool{false, false})
	caseData := NewFunctionTestCase(proc, []FunctionTestInput{input}, result, newCrc32ExecContext().builtInCrc32)
	succeed, info := caseData.RunAndFree()
	require.True(t, succeed, info)
}
func TestCRC32PreservesTextAndBinaryBytes(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	textCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"x", `{"b":2,"a":1}`, ""}, nil),
		},
		NewFunctionTestResult(types.T_uint32.ToType(), false,
			[]uint32{2363233923, 3030484594, 0}, nil),
		newCrc32ExecContext().builtInCrc32,
	)
	succeed, info := textCase.RunAndFree()
	require.True(t, succeed, info)

	binaryCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varbinary.ToType(), []string{string([]byte{0x00, 0xff}), string([]byte{0x00}), string([]byte{0xff}), ""}, []bool{false, false, false, false}),
		},
		NewFunctionTestResult(types.T_uint32.ToType(), false,
			[]uint32{1826356594, 3523407757, 4278190080, 0}, nil),
		newCrc32ExecContext().builtInCrc32,
	)
	succeed, info = binaryCase.RunAndFree()
	require.True(t, succeed, info)
}

func TestCRC32ExecutionIdentityPreservesLegacyJSON(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	encoded := makeJSONEncodedFromText(t, []string{`{"t1":"a"}`}, nil)
	f, err := GetFunctionById(proc.Ctx, EncodeOverloadID(CRC32, CRC32LegacyOverload))
	require.NoError(t, err)
	run := NewFunctionTestCase(proc, []FunctionTestInput{NewFunctionTestInput(types.T_json.ToType(), encoded, nil)}, NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{3719146973}, nil), f.newOp())
	ok, info := run.RunAndFree()
	require.True(t, ok, info)
	_, err = GetFunctionById(proc.Ctx, EncodeOverloadID(CRC32, 1))
	require.Error(t, err)
	for _, tc := range []struct {
		typ      types.T
		overload int32
	}{
		{types.T_json, CRC32LegacyOverload}, {types.T_any, CRC32LegacyOverload},
		{types.T_varchar, CRC32LegacyOverload}, {types.T_varbinary, CRC32LegacyOverload}, {types.T_int64, CRC32LegacyOverload},
	} {
		resolved, err := GetFunctionByName(proc.Ctx, "crc32", []types.Type{tc.typ.ToType()})
		require.NoError(t, err)
		require.Equal(t, EncodeOverloadID(CRC32, tc.overload), resolved.GetEncodedOverloadID())
	}
}

func TestCRC32LegacyJSONShapesAndReuse(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	values := []string{`{"b":2,"a":1}`, `[1,true,"x"]`, `1`, `"x"`, `true`, `false`, `null`, `"` + strings.Repeat("x", 4096) + `"`, ""}
	nulls := []bool{false, false, false, false, false, false, false, false, true}
	encoded := makeJSONEncodedFromText(t, values, nulls)
	want := make([]uint32, len(encoded))
	for i := range encoded {
		want[i] = stdcrc32.ChecksumIEEE([]byte(encoded[i]))
	}
	ctx := newCrc32ExecContext()
	for i := 0; i < 2; i++ {
		c := NewFunctionTestCase(proc, []FunctionTestInput{NewFunctionTestInput(types.T_json.ToType(), encoded, nulls)}, NewFunctionTestResult(types.T_uint32.ToType(), false, want, nulls), ctx.builtInCrc32)
		ok, info := c.RunAndFree()
		require.True(t, ok, info)
	}
}
