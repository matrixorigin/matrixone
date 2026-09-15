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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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

func TestCRC32JSONUsesSerializedText(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	jsonTexts := []string{
		`{"b":2,"a":1}`,
		`[1,true,"x"]`,
		`1`,
		`"x"`,
		`true`,
		`false`,
		`null`,
		"",
	}
	nulls := []bool{false, false, false, false, false, false, false, true}
	encoded := makeJSONEncodedFromText(t, jsonTexts, nulls)

	// These checksums are for the independent visible JSON bytes:
	// {"a": 1, "b": 2}, [1, true, "x"], 1, "x", true, false, null.
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_json.ToType(), encoded, nulls),
		},
		NewFunctionTestResult(types.T_uint32.ToType(), false,
			[]uint32{733321759, 3071756745, 2212294583, 4128176518, 4261170317, 734881840, 634125391, 0},
			[]bool{false, false, false, false, false, false, false, true}),
		newCrc32ExecContext().builtInCrc32,
	)
	succeed, info := testCase.Run()
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
	succeed, info := textCase.Run()
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
	succeed, info = binaryCase.Run()
	require.True(t, succeed, info)
}

func TestCRC32JSONConstSelectionAndContextReuse(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	constJSON := makeJSONEncodedFromText(t, []string{`"x"`, `"x"`, `"x"`}, nil)
	constCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestConstInput(types.T_json.ToType(), constJSON, nil),
		},
		NewFunctionTestResult(types.T_uint32.ToType(), false,
			[]uint32{4128176518, 4128176518, 4128176518}, nil),
		newCrc32ExecContext().builtInCrc32,
	)
	succeed, info := constCase.Run()
	require.True(t, succeed, info)

	constNullCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestConstInput(types.T_json.ToType(), []string{"", "", ""}, []bool{true, true, true}),
		},
		NewFunctionTestResult(types.T_uint32.ToType(), false,
			[]uint32{0, 0, 0}, []bool{true, true, true}),
		newCrc32ExecContext().builtInCrc32,
	)
	succeed, info = constNullCase.Run()
	require.True(t, succeed, info)

	// The masked row is deliberately not a valid stored JSON value. A
	// selection must prevent the executor from touching it.
	selectionValues := makeJSONEncodedFromText(t, []string{"1", "true", `"x"`}, nil)
	selectionCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_json.ToType(), selectionValues, nil),
		},
		NewFunctionTestResult(types.T_uint32.ToType(), false,
			[]uint32{0, 4261170317, 4128176518}, []bool{true, false, false}),
		newCrc32ExecContext().builtInCrc32,
	).WithSelectList(&FunctionSelectList{AnyNull: true, SelectList: []bool{false, true, true}})
	stored, null := vector.GenerateFunctionStrParameter(selectionCase.parameters[0]).GetStrValue(0)
	require.False(t, null)
	stored[0] = 0xff
	succeed, info = selectionCase.Run()
	require.True(t, succeed, info)

	// One executor context is reused across batches. Reset must prevent the
	// checksum from carrying bytes from a prior invocation.
	contextFn := newCrc32ExecContext().builtInCrc32
	for _, test := range []struct {
		name string
		text string
		want uint32
	}{
		{name: "first", text: `1`, want: 2212294583},
		{name: "second", text: `"x"`, want: 4128176518},
		{name: "third", text: `null`, want: 634125391},
	} {
		t.Run(test.name, func(t *testing.T) {
			encoded := makeJSONEncodedFromText(t, []string{test.text}, nil)
			caseRun := NewFunctionTestCase(
				proc,
				[]FunctionTestInput{NewFunctionTestInput(types.T_json.ToType(), encoded, nil)},
				NewFunctionTestResult(types.T_uint32.ToType(), false, []uint32{test.want}, nil),
				contextFn,
			)
			succeed, info := caseRun.Run()
			require.True(t, succeed, info)
		})
	}
}

func TestCRC32JSONLongSerializedValues(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	payloadLengths := []int{65532, 65533, 65534, 131070}
	jsonTexts := make([]string, len(payloadLengths))
	for i, payloadLength := range payloadLengths {
		jsonTexts[i] = `"` + strings.Repeat("x", payloadLength) + `"`
	}
	encoded := makeJSONEncodedFromText(t, jsonTexts, nil)

	// The expected values are CRC32 checksums of the quoted JSON strings at
	// serialized lengths 65534, 65535, 65536, and 131072 respectively.
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_json.ToType(), encoded, nil)},
		NewFunctionTestResult(types.T_uint32.ToType(), false,
			[]uint32{3992308327, 2483886726, 1132015800, 3633420607}, nil),
		newCrc32ExecContext().builtInCrc32,
	)
	succeed, info := testCase.Run()
	require.True(t, succeed, info)
}

func TestCRC32JSONPropagatesMarshalError(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	// Start with an admitted JSON value, then corrupt its stored type code to
	// reach MarshalJSON's error path without bypassing vector admission.
	encoded := makeJSONEncodedFromText(t, []string{`1`}, nil)
	input := newVectorByType(proc.Mp(), types.T_json.ToType(), encoded, nil)
	defer input.Free(proc.Mp())
	stored, null := vector.GenerateFunctionStrParameter(input).GetStrValue(0)
	require.False(t, null)
	stored[0] = 0xff

	result := vector.NewFunctionResultWrapper(types.T_uint32.ToType(), proc.Mp())
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(1))
	err := newCrc32ExecContext().builtInCrc32([]*vector.Vector{input}, result, proc, 1, nil)
	require.Error(t, err)
}
