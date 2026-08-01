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
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestCastGeometryToSubtype(t *testing.T) {
	proc := testutil.NewProcess(t)

	testCases := []tcTemp{
		{
			info: "point fits point column",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"POINT"}, []bool{false}),
				NewFunctionTestInput(types.T_geometry.ToType(), []string{"POINT(1 2)"}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_geometry.ToType(), false, []string{"POINT(1 2)"}, []bool{false}),
		},
		{
			info: "generic geometry column accepts point",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"GEOMETRY"}, []bool{false}),
				NewFunctionTestInput(types.T_geometry.ToType(), []string{"POINT(1 2)"}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_geometry.ToType(), false, []string{"POINT(1 2)"}, []bool{false}),
		},
		{
			info: "null payload propagates",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"POINT"}, []bool{false}),
				NewFunctionTestInput(types.T_geometry.ToType(), []string{"POINT(1 2)"}, []bool{true}),
			},
			expect: NewFunctionTestResult(types.T_geometry.ToType(), false, []string{""}, []bool{true}),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, CastGeometryToSubtype)
			succeed, info := tcc.Run()
			require.True(t, succeed, info)
		})
	}
}

func TestCastValueToIndexConstDefinition(t *testing.T) {
	proc := testutil.NewProcess(t)
	descriptor := "zero,one,two,three,four,five,six,seven"
	values := []string{"seven", "zero", "THREE", "2", "four", "five", "six", "one"}
	want := []types.Enum{8, 1, 4, 2, 5, 6, 7, 2}
	typeEnums := make([]string, 0, len(values)*8)
	enumValues := make([]string, 0, len(values)*8)
	expected := make([]types.Enum, 0, len(values)*8)
	for range 8 {
		typeEnums = append(typeEnums, descriptor, descriptor, descriptor, descriptor, descriptor, descriptor, descriptor, descriptor)
		enumValues = append(enumValues, values...)
		expected = append(expected, want...)
	}
	inputs := []FunctionTestInput{
		NewFunctionTestConstInput(types.T_varchar.ToType(), typeEnums, nil),
		NewFunctionTestInput(types.T_varchar.ToType(), enumValues, nil),
	}
	expect := NewFunctionTestResult(types.T_enum.ToType(), false, expected, nil)

	tcc := NewFunctionTestCase(proc, inputs, expect, CastValueToIndex)
	succeed, info := tcc.Run()
	require.True(t, succeed, info)
}

func TestEnumValueIndexPreservesParseEnumSemantics(t *testing.T) {
	for _, definition := range []string{
		"low,LOW,2,high,high",
		"K,K,Σ,ς,σ",
	} {
		index := buildEnumValueIndex(definition)
		require.NotNil(t, index)
		for _, input := range []string{
			"low", "LOW", "LoW", "2", "0x2", "K", "k", "Σ", "ς", "σ",
			"missing", "0", "65537", "18446744073709551615",
		} {
			want, wantErr := types.ParseEnum(definition, input)
			got, gotErr := index.parse(input)
			require.Equal(t, want, got, "definition=%q input=%q", definition, input)
			if wantErr == nil {
				require.NoError(t, gotErr, "definition=%q input=%q", definition, input)
			} else {
				require.EqualError(t, gotErr, wantErr.Error(), "definition=%q input=%q", definition, input)
			}
		}
	}

	require.Nil(t, buildEnumValueIndex(""))
	largeDefinition := strings.Repeat("a,", 1023) + "a"
	require.Nil(t, buildEnumValueIndexForBatch(largeDefinition, 8))
	require.Nil(t, buildEnumValueIndexForBatch("a,b,c,d,e,f,g", 1024))
	require.Nil(t, buildEnumValueIndexForBatch("a,b,c,d,e,f,g,h", enumValueIndexMinRows-1))
	require.NotNil(t, buildEnumValueIndexForBatch("a,b,c,d,e,f,g,h", enumValueIndexMinRows))
}

func BenchmarkEnumValueIndex(b *testing.B) {
	for _, labelCount := range []int{8, 32, 64, 256, 512, 1024} {
		labels := make([]string, labelCount)
		for i := range labels {
			labels[i] = "value" + strconv.Itoa(i)
		}
		definition := strings.Join(labels, ",")
		batchSize := enumValueIndexMinRows
		inputs := []struct {
			name  string
			value string
		}{
			{name: "exact", value: labels[len(labels)-1]},
			{name: "case-insensitive", value: strings.ToUpper(labels[len(labels)-1])},
			{name: "numeric", value: strconv.Itoa(labelCount)},
		}
		for _, input := range inputs {
			prefix := strconv.Itoa(labelCount) + "-labels/" + strconv.Itoa(batchSize) + "-rows/" + input.name
			b.Run(prefix+"/linear", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					for row := 0; row < batchSize; row++ {
						if _, err := types.ParseEnum(definition, input.value); err != nil {
							b.Fatal(err)
						}
					}
				}
			})
			b.Run(prefix+"/indexed", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					index := buildEnumValueIndexForBatch(definition, batchSize)
					if index == nil {
						b.Fatal("batch should use the enum value index")
					}
					for row := 0; row < batchSize; row++ {
						if _, err := index.parse(input.value); err != nil {
							b.Fatal(err)
						}
					}
				}
			})
		}
	}
}

func TestCastGeometryToSubtypeRejectMismatch(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"POINT"}, []bool{false}),
		NewFunctionTestInput(types.T_geometry.ToType(), []string{"LINESTRING(0 0,1 1)"}, []bool{false}),
	}
	expect := NewFunctionTestResult(types.T_geometry.ToType(), false, []string{""}, []bool{false})

	tcc := NewFunctionTestCase(proc, inputs, expect, CastGeometryToSubtype)
	succeed, info := tcc.Run()
	require.False(t, succeed)
	require.Contains(t, info, "cannot store LINESTRING in POINT column")
}

// SRID mismatch is now rejected at bind time (see funcCastForGeometryType in
// the plan package), not by the runtime cast_geometry_to_subtype, which only
// validates the subtype and normalizes the stored bytes to WKB.

func TestCastGeometryToSubtypeRejectNonFinite(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"POINT"}, []bool{false}),
		NewFunctionTestInput(types.T_geometry.ToType(), []string{"POINT(NaN 1)"}, []bool{false}),
	}
	expect := NewFunctionTestResult(types.T_geometry.ToType(), false, []string{""}, []bool{false})

	tcc := NewFunctionTestCase(proc, inputs, expect, CastGeometryToSubtype)
	succeed, info := tcc.Run()
	require.False(t, succeed)
	require.Contains(t, info, "invalid geometry payload")
}

func TestCastGeometryToSubtypeRejectMalformedStructure(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"POINT"}, []bool{false}),
		NewFunctionTestInput(types.T_geometry.ToType(), []string{"POINT(1"}, []bool{false}),
	}
	expect := NewFunctionTestResult(types.T_geometry.ToType(), false, []string{""}, []bool{false})

	tcc := NewFunctionTestCase(proc, inputs, expect, CastGeometryToSubtype)
	succeed, info := tcc.Run()
	require.False(t, succeed)
	require.Contains(t, info, "invalid geometry payload")
}

func TestCastGeometryToSubtypeRejectTooManyPoints(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.SetResolveVariableFunc(func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
		if varName == "max_points_in_geometry" {
			return int64(3), nil
		}
		return nil, nil
	})
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"LINESTRING"}, []bool{false}),
		NewFunctionTestInput(types.T_geometry.ToType(), []string{"LINESTRING(0 0,1 1,2 2,3 3)"}, []bool{false}),
	}
	expect := NewFunctionTestResult(types.T_geometry.ToType(), false, []string{""}, []bool{false})

	tcc := NewFunctionTestCase(proc, inputs, expect, CastGeometryToSubtype)
	succeed, info := tcc.Run()
	require.False(t, succeed)
	require.Contains(t, info, "max_points_in_geometry=3")
}

func TestCastJsonToArray(t *testing.T) {
	proc := testutil.NewProcess(t)
	jsonTexts := []string{`["red","blue",null]`, `[[1,2],[3,null]]`, `[127]`}
	encoded := makeJSONEncodedFromText(t, jsonTexts, nil)

	testCases := []tcTemp{
		{
			info: "varchar array accepts string elements",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"array(varchar(20))"}, []bool{false}),
				NewFunctionTestInput(types.T_json.ToType(), encoded[:1], []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_json.ToType(), false, encoded[:1], []bool{false}),
		},
		{
			info: "nested integer array accepts nested arrays",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"array(array(int))"}, []bool{false}),
				NewFunctionTestInput(types.T_json.ToType(), encoded[1:2], []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_json.ToType(), false, encoded[1:2], []bool{false}),
		},
		{
			info: "integer alias array accepts matching elements",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"array(int1)"}, []bool{false}),
				NewFunctionTestInput(types.T_json.ToType(), encoded[2:3], []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_json.ToType(), false, encoded[2:3], []bool{false}),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, CastJsonToArray)
			succeed, info := tcc.Run()
			require.True(t, succeed, info)
		})
	}
}

func TestCastJsonToArrayRejectsNonArray(t *testing.T) {
	proc := testutil.NewProcess(t)
	encoded := makeJSONEncodedFromText(t, []string{`{"tag":"red"}`}, nil)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"array(varchar(20))"}, []bool{false}),
		NewFunctionTestInput(types.T_json.ToType(), encoded, []bool{false}),
	}
	expect := NewFunctionTestResult(types.T_json.ToType(), false, []string{""}, []bool{false})

	tcc := NewFunctionTestCase(proc, inputs, expect, CastJsonToArray)
	succeed, info := tcc.Run()
	require.False(t, succeed)
	require.Contains(t, info, "cannot store JSON OBJECT in array(varchar(20)) column")
}

func TestCastJsonToArrayRejectsIncompatibleElement(t *testing.T) {
	proc := testutil.NewProcess(t)
	testCases := []struct {
		name      string
		arrayType string
		jsonText  string
	}{
		{name: "string in int array", arrayType: "array(int)", jsonText: `["1"]`},
		{name: "number in varchar array", arrayType: "array(varchar(20))", jsonText: `[1]`},
		{name: "varchar width", arrayType: "array(varchar(3))", jsonText: `["toolong"]`},
		{name: "varbinary byte width", arrayType: "array(varbinary(3))", jsonText: "[\"\u00e9\u00e9\"]"},
		{name: "scalar in nested array", arrayType: "array(array(int))", jsonText: `[[1],2]`},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			encoded := makeJSONEncodedFromText(t, []string{tc.jsonText}, nil)
			inputs := []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{tc.arrayType}, []bool{false}),
				NewFunctionTestInput(types.T_json.ToType(), encoded, []bool{false}),
			}
			expect := NewFunctionTestResult(types.T_json.ToType(), false, []string{""}, []bool{false})

			tcc := NewFunctionTestCase(proc, inputs, expect, CastJsonToArray)
			succeed, info := tcc.Run()
			require.False(t, succeed)
			require.Contains(t, info, "cannot store JSON value with incompatible element type in "+tc.arrayType+" column")
		})
	}
}

func TestTypedArrayBinaryCompatibilityUsesRawPayloadLength(t *testing.T) {
	spec := typedArrayElementSpec{name: "varbinary", width: 1}

	require.True(t, typedArrayElementCompatible(
		spec, newTypedByteJson(bytejson.TpCodeOpaque, string([]byte{0x00}))))
	require.True(t, typedArrayElementCompatible(
		spec, newTypedByteJson(bytejson.TpCodeBit, string([]byte{0x01}))))
	require.True(t, typedArrayElementCompatible(
		spec, newTypedByteJson(bytejson.TpCodeBlob, "AA==")))
}
