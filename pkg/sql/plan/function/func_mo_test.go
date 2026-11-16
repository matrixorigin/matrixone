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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestCastIndexToValueDisplaysEnumErrorMemberAsEmptyString(t *testing.T) {
	proc := testutil.NewProcess(t)
	testCase := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"a,b,"}, nil),
			NewFunctionTestInput(types.T_enum.ToType(), []types.Enum{0, 1, 3}, nil),
		},
		NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"", "a", ""}, nil),
		CastIndexToValue,
	)
	succeed, info := testCase.Run()
	require.True(t, succeed, info)
}

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

func TestInsertIgnoreAdjustsMySQLSpecialTypeValues(t *testing.T) {
	ignoreProc := testutil.NewProcess(t)
	ignoreProc.SetStmtProfile(&process.StmtProfile{})
	ignoreProc.GetStmtProfile().SetStatementRuntimeProfile("Insert", "DML", true)

	run := func(name string, inputs []FunctionTestInput, expect FunctionTestResult, fn func([]*vector.Vector, vector.FunctionResultWrapper, *process.Process, int, *FunctionSelectList) error) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			tcc := NewFunctionTestCase(ignoreProc, inputs, expect, fn)
			succeed, info := tcc.Run()
			require.True(t, succeed, info)
		})
	}

	run("enum labels use error member", []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"a,b", "a,b"}, nil),
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"bad", "b"}, nil),
	}, NewFunctionTestResult(types.T_enum.ToType(), false, []types.Enum{0, 2}, nil), CastValueToIndex)

	run("enum numeric values use error member", []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"a,b", "a,b", "a,b"}, nil),
		NewFunctionTestInput(types.T_uint16.ToType(), []uint16{9, 1, 1}, []bool{false, false, true}),
	}, NewFunctionTestResult(types.T_enum.ToType(), false, []types.Enum{0, 1, 0}, []bool{false, false, true}), CastIndexValueToIndex)

	run("set labels retain valid members", []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"x,y,z", "x,y,z", "x,y,z"}, nil),
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"x,bad", "bad", "x"}, []bool{false, false, true}),
	}, NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{1, 0, 0}, []bool{false, false, true}), CastSetValueToIndex)

	run("set numeric values retain declared bits", []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"x,y,z", "x,y,z", "x,y,z"}, nil),
		NewFunctionTestInput(types.T_uint64.ToType(), []uint64{99, 4, 1}, []bool{false, false, true}),
	}, NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{3, 4, 0}, []bool{false, false, true}), CastSetIndexValueToIndex)

	require.Equal(t, uint64(7), setMemberBitmap("x,y,z"))
	require.False(t, statementIgnore(nil))
}

func TestEnumValueIndexPreservesParseEnumSemantics(t *testing.T) {
	for _, definition := range []string{
		"low,LOW,2,high,high",
		"K,K,Σ,ς,σ",
	} {
		index := buildEnumMemberIndex(strings.Split(definition, ","))
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

	require.Nil(t, buildEnumValueIndexForBatch(nil, 1024))
	largeDefinition := strings.Repeat("a,", 1023) + "a"
	require.Nil(t, buildEnumValueIndexForBatch(strings.Split(largeDefinition, ","), 8))
	require.Nil(t, buildEnumValueIndexForBatch(strings.Split("a,b,c,d,e,f,g", ","), 1024))
	require.Nil(t, buildEnumValueIndexForBatch(strings.Split("a,b,c,d,e,f,g,h", ","), enumValueIndexMinRows-1))
	require.NotNil(t, buildEnumValueIndexForBatch(strings.Split("a,b,c,d,e,f,g,h", ","), enumValueIndexMinRows))
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
					index := buildEnumValueIndexForBatch(strings.Split(definition, ","), batchSize)
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

func TestEnumEncodedConversion(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, constant := range []bool{false, true} {
		domain := NewFunctionTestInput(types.T_varchar.ToType(), []string{"3#3:a,b0:1:c", "3#3:a,b0:1:c", "3#3:a,b0:1:c"}, nil)
		tag := NewFunctionTestInput(types.T_int64.ToType(), []int64{1, 1, 1}, nil)
		if constant {
			domain = NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"3#3:a,b0:1:c", "3#3:a,b0:1:c", "3#3:a,b0:1:c"}, nil)
			tag = NewFunctionTestConstInput(types.T_int64.ToType(), []int64{1, 1, 1}, nil)
		}
		for _, tc := range []struct {
			value FunctionTestInput
			want  FunctionTestResult
			fn    fEvalFn
		}{
			{NewFunctionTestInput(types.T_varchar.ToType(), []string{"a,b", "", "C"}, nil), NewFunctionTestResult(types.T_enum.ToType(), false, []types.Enum{1, 2, 3}, nil), CastValueToIndex},
			{NewFunctionTestInput(types.T_uint16.ToType(), []uint16{1, 2, 3}, nil), NewFunctionTestResult(types.T_enum.ToType(), false, []types.Enum{1, 2, 3}, nil), CastIndexValueToIndex},
			{NewFunctionTestInput(types.T_enum.ToType(), []types.Enum{0, 1, 2}, nil), NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"", "a,b", ""}, nil), CastIndexToValue},
		} {
			tc := NewFunctionTestCase(proc, []FunctionTestInput{domain, tc.value, tag}, tc.want, tc.fn)
			ok, info := tc.Run()
			require.True(t, ok, info)
		}
	}
	tc := NewFunctionTestCase(proc, []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"2#3:a,b1:c"}, nil),
		NewFunctionTestInput(types.T_enum.ToType(), []types.Enum{1}, nil),
	}, NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"2#3:a"}, nil), CastIndexToValue)
	ok, info := tc.Run()
	require.True(t, ok, info)
}

func TestEnumDisplayToIndex(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.SetStmtProfile(&process.StmtProfile{})
	proc.GetStmtProfile().SetStatementRuntimeProfile("Insert", "DML", true)
	typ := types.T_enum.ToType()
	for _, tc := range []struct {
		name    string
		domains []string
		tags    []int64
		values  []string
		nulls   []bool
		want    []types.Enum
	}{
		{
			name:    "exact bytes and null",
			domains: []string{"4#1:a1:A0:3:a,b", "4#1:a1:A0:3:a,b", "4#1:a1:A0:3:a,b", "4#1:a1:A0:3:a,b", "4#1:a1:A0:3:a,b"},
			tags:    []int64{1, 1, 1, 1, 1},
			values:  []string{"a", "A", "", "a,b", ""},
			nulls:   []bool{false, false, false, false, true},
			want:    []types.Enum{1, 2, 3, 4, 0},
		},
		{
			name:    "changing and legacy domains",
			domains: []string{"a,b", "a,b", "", "1#0:"},
			tags:    []int64{0, 0, 0, 1},
			values:  []string{"", "b", "", ""},
			want:    []types.Enum{0, 2, 0, 1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			check := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), tc.domains, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), tc.values, tc.nulls),
				NewFunctionTestInput(types.T_int64.ToType(), tc.tags, nil),
			}, NewFunctionTestResult(typ, false, tc.want, tc.nulls), EnumDisplayToIndex)
			ok, info := check.RunAndFree()
			require.True(t, ok, info)
		})
	}

	// Exercise the indexed batch path and retain the first ordinal for exact
	// duplicates without conflating distinct spellings that fold identically.
	const count = 64
	domains, tags := make([]string, count), make([]int64, count)
	values, want := make([]string, count), make([]types.Enum, count)
	for i := range count {
		domains[i] = "a,A,,b,c,d,a,f"
		values[i] = []string{"a", "A", ""}[i%3]
		want[i] = []types.Enum{1, 2, 3}[i%3]
	}
	check := NewFunctionTestCase(proc, []FunctionTestInput{
		NewFunctionTestConstInput(types.T_varchar.ToType(), domains, nil),
		NewFunctionTestInput(types.T_varchar.ToType(), values, nil),
		NewFunctionTestConstInput(types.T_int64.ToType(), tags, nil),
	}, NewFunctionTestResult(typ, false, want, nil), EnumDisplayToIndex)
	ok, info := check.RunAndFree()
	require.True(t, ok, info)

	for _, tc := range []struct {
		domain string
		tag    int64
		value  string
	}{
		{"a,b", 0, "missing"},
		{"1#9:x", 1, ""},
	} {
		check := NewFunctionTestCase(proc, []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{tc.domain}, nil),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{tc.value}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{tc.tag}, nil),
		}, NewFunctionTestResult(typ, true, nil, nil), EnumDisplayToIndex)
		ok, info := check.RunAndFree()
		require.True(t, ok, info)
	}
}

func TestEnumChangingDomains(t *testing.T) {
	proc := testutil.NewProcess(t)
	// The same payload has different members under the two encoding tags.
	domains := []string{"b,a", "a,b,c", ",x,", "2#3:a,b1:c", "2#3:a,b1:c", "1#0:", "a,b", "1#9:broken"}
	tags := []int64{0, 0, 0, 0, 1, 1, 0, 1}
	domainNulls := []bool{false, false, false, false, false, false, true, false}
	tagNulls := []bool{false, false, false, false, false, false, false, true}
	resultNulls := []bool{false, false, false, false, false, false, true, true}
	for _, encoded := range []bool{false, true} {
		rows := 4
		if encoded {
			rows = len(domains)
		}
		for _, tc := range []struct {
			value FunctionTestInput
			want  FunctionTestResult
			fn    fEvalFn
		}{
			{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"a", "a", "", "b1:c", "a,b", "", "x", "x"}[:rows], nil),
				NewFunctionTestResult(types.T_enum.ToType(), false, []types.Enum{2, 1, 1, 2, 1, 1, 0, 0}[:rows], resultNulls[:rows]),
				CastValueToIndex,
			},
			{
				NewFunctionTestInput(types.T_uint16.ToType(), []uint16{2, 3, 3, 2, 1, 1, 1, 1}[:rows], nil),
				NewFunctionTestResult(types.T_enum.ToType(), false, []types.Enum{2, 3, 3, 2, 1, 1, 0, 0}[:rows], resultNulls[:rows]),
				CastIndexValueToIndex,
			},
			{
				NewFunctionTestInput(types.T_enum.ToType(), []types.Enum{2, 3, 3, 2, 1, 0, 1, 1}[:rows], nil),
				NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"a", "c", "", "b1:c", "a,b", "", "", ""}[:rows], resultNulls[:rows]),
				CastIndexToValue,
			},
		} {
			inputs := []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), domains[:rows], domainNulls[:rows]), tc.value,
			}
			if encoded {
				inputs = append(inputs, NewFunctionTestInput(types.T_int64.ToType(), tags[:rows], tagNulls[:rows]))
			}
			check := NewFunctionTestCase(proc, inputs, tc.want, tc.fn)
			ok, info := check.Run()
			require.True(t, ok, info)
		}
	}
}

func TestEnumEncodingIgnoreAndNull(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.SetStmtProfile(&process.StmtProfile{})
	proc.GetStmtProfile().SetStatementRuntimeProfile("Insert", "DML", true)
	for _, tc := range []struct {
		domain string
		tag    int64
	}{
		{"2#1:a", 1}, {"a,b", 257}, {"1#0:", -1},
		{strings.Repeat(",", types.MaxEnumLen), 0},
	} {
		for _, op := range []struct {
			fn     fEvalFn
			input  FunctionTestInput
			output types.Type
		}{
			{CastValueToIndex, NewFunctionTestInput(types.T_varchar.ToType(), []string{"bad"}, nil), types.T_enum.ToType()},
			{CastIndexValueToIndex, NewFunctionTestInput(types.T_uint16.ToType(), []uint16{0}, nil), types.T_enum.ToType()},
			{CastIndexToValue, NewFunctionTestInput(types.T_enum.ToType(), []types.Enum{0}, nil), types.T_varchar.ToType()},
		} {
			check := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{tc.domain}, nil), op.input,
				NewFunctionTestInput(types.T_int64.ToType(), []int64{tc.tag}, nil),
			}, NewFunctionTestResult(op.output, true, nil, nil), op.fn)
			ok, info := check.Run()
			require.True(t, ok, info)
		}
	}
	check := NewFunctionTestCase(proc, []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"1#0:", "1#0:", "1#0:"}, nil),
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"", "bad", ""}, []bool{false, false, true}),
		NewFunctionTestInput(types.T_int64.ToType(), []int64{1, 1, 1}, nil),
	}, NewFunctionTestResult(types.T_enum.ToType(), false, []types.Enum{1, 0, 0}, []bool{false, false, true}), CastValueToIndex)
	ok, info := check.Run()
	require.True(t, ok, info)
}

func TestCasePreservesEnumStorage(t *testing.T) {
	typ := types.T_enum.ToType()
	typ.SetEnumEncoding(1)
	matched, err := GetFunctionByName(t.Context(), "case", []types.Type{types.T_bool.ToType(), typ, typ})
	require.NoError(t, err)
	require.Equal(t, types.T_enum, matched.GetReturnType().Oid)
	proc := testutil.NewProcess(t)
	check := NewFunctionTestCase(proc, []FunctionTestInput{
		NewFunctionTestInput(types.T_bool.ToType(), []bool{true, false}, nil),
		NewFunctionTestInput(typ, []types.Enum{0, 0}, nil),
		NewFunctionTestInput(typ, []types.Enum{2, 2}, nil),
	}, NewFunctionTestResult(typ, false, []types.Enum{0, 2}, nil), caseFn)
	ok, info := check.Run()
	require.True(t, ok, info)
}

func TestFramedEnumIndexedBatch(t *testing.T) {
	proc := testutil.NewProcess(t)
	payload := "8#3:a,b1:c0:1:e1:f1:g1:h1:i"
	values := []string{"i", "A,B", "", "2", "0x8", "c", "e", "h"}
	ordinals := []types.Enum{8, 1, 3, 2, 8, 2, 4, 7}
	var domains, inputs []string
	var tags []int64
	var expected []types.Enum
	for range 8 {
		for _, value := range values {
			domains = append(domains, payload)
			inputs = append(inputs, value)
			tags = append(tags, 1)
		}
		expected = append(expected, ordinals...)
	}
	check := NewFunctionTestCase(proc, []FunctionTestInput{
		NewFunctionTestConstInput(types.T_varchar.ToType(), domains, nil),
		NewFunctionTestInput(types.T_varchar.ToType(), inputs, nil),
		NewFunctionTestConstInput(types.T_int64.ToType(), tags, nil),
	}, NewFunctionTestResult(types.T_enum.ToType(), false, expected, nil), CastValueToIndex)
	ok, info := check.Run()
	require.True(t, ok, info)
	require.Nil(t, buildEnumValueIndexForBatch(types.EnumMembers{"a,b,c,d,e", "f,g,h,i"}, 64))
}
