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
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func encodeCatalogTypeMetadata(t *testing.T, column types.Type) string {
	t.Helper()
	encoded, err := column.Marshal()
	require.NoError(t, err)
	return string(encoded)
}

// TestInternalColumnMetadataEnumSetOverloadsResolve pins the overload
// resolution: the persisted two-argument metadata queries bind the
// label-aware overload, while the historical one-argument form keeps the
// overload frozen for persisted definitions written by older binaries.
func TestInternalColumnMetadataEnumSetOverloadsResolve(t *testing.T) {
	for _, name := range []string{
		"internal_char_length",
		"internal_char_size",
		"internal_numeric_precision",
		"internal_numeric_scale",
	} {
		t.Run(name, func(t *testing.T) {
			declared := []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType()}
			resolved, err := GetFunctionByName(context.Background(), name, declared)
			require.NoError(t, err, "two-argument label-aware metadata overload must resolve")
			require.Equal(t, types.T_int64.ToType(), resolved.retType)

			historical, err := GetFunctionByName(context.Background(), name, declared[:1])
			require.NoError(t, err, "historical one-argument overload must keep resolving")
			require.Equal(t, int32(0), historical.overloadId)
		})
	}
}

type internalMetadataRow struct {
	name      string
	column    types.Type
	labels    string
	nullLabel bool
	want      int64
	missing   bool
}

func runInternalMetadataOverload(t *testing.T, fid int32, rows []internalMetadataRow) {
	t.Helper()
	op := allSupportedFunctions[fid].Overloads[len(allSupportedFunctions[fid].Overloads)-1].newOp()
	proc := testutil.NewProcess(t)
	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			inputs := []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{encodeCatalogTypeMetadata(t, tc.column)}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{tc.labels}, []bool{tc.nullLabel}),
			}
			result := NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{tc.want}, []bool{tc.missing})
			testCase := NewFunctionTestCase(proc, inputs, result, op)
			ok, info := testCase.RunAndFree()
			require.True(t, ok, tc.name+": "+info)
		})
	}
}

// runInternalMetadataSingleArg evaluates the historical one-argument
// overload; every expectation here is the pre-repair behavior that persisted
// definitions from older binaries must keep observing.
func runInternalMetadataSingleArg(t *testing.T, fid int32, rows []internalMetadataRow) {
	t.Helper()
	op := allSupportedFunctions[fid].Overloads[0].newOp()
	proc := testutil.NewProcess(t)
	for _, tc := range rows {
		t.Run("historical/"+tc.name, func(t *testing.T) {
			inputs := []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{encodeCatalogTypeMetadata(t, tc.column)}, []bool{false}),
			}
			result := NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{tc.want}, []bool{tc.missing})
			testCase := NewFunctionTestCase(proc, inputs, result, op)
			ok, info := testCase.RunAndFree()
			require.True(t, ok, tc.name+": "+info)
		})
	}
}

func TestInternalCharLengthDeclaredEnumSetLabels(t *testing.T) {
	enumColumn := types.T_enum.ToType()
	setColumn := types.T_uint64.ToType()

	runInternalMetadataOverload(t, INTERNAL_CHAR_LENGTH, []internalMetadataRow{
		{name: "enum longest member", column: enumColumn, labels: "a,bb", want: 2},
		{name: "enum longest member first", column: enumColumn, labels: "bb,a", want: 2},
		{name: "enum single character members", column: enumColumn, labels: "z,a", want: 1},
		{name: "enum multibyte members", column: enumColumn, labels: "中,长长", want: 2},
		{name: "enum empty member", column: enumColumn, labels: "a,", want: 1},
		{name: "enum empty domain is declared zero length", column: enumColumn, labels: "", want: 0},
		{name: "enum null labels stay null", column: enumColumn, nullLabel: true, want: 0, missing: true},
		{name: "set all members with commas", column: setColumn, labels: "a,bb", want: 4},
		{name: "set two single-character members", column: setColumn, labels: "z,a", want: 3},
		{name: "set multibyte members", column: setColumn, labels: "中,长长", want: 4},
		{name: "set single member", column: setColumn, labels: "a", want: 1},
		{name: "set empty member encoding", column: setColumn, labels: ",", want: 0},
		{name: "set empty plus member", column: setColumn, labels: ",a", want: 2},
		{name: "set without declared labels", column: setColumn, want: 0, missing: true},
		// Ordinary columns must keep their metadata when asked through the
		// label-aware overload, including a bare BIGINT UNSIGNED that shares
		// the SET storage identity.
		{name: "unsigned integer keeps null length", column: setColumn, labels: "", want: 0, missing: true},
		{name: "varchar keeps declared width", column: types.New(types.T_varchar, 32, 0), labels: "", want: 32},
		{name: "varchar ignores null labels", column: types.New(types.T_varchar, 32, 0), nullLabel: true, want: 32},
		// Label storage is shared by geometry SRID annotations and JSON
		// array declarations; neither may fabricate character lengths.
		{name: "geometry srid labels stay null", column: types.T_geometry.ToType(), labels: "SRID=0", want: 0, missing: true},
		{name: "json array labels stay null", column: types.T_json.ToType(), labels: "array(int)", want: 0, missing: true},
	})

	runInternalMetadataSingleArg(t, INTERNAL_CHAR_LENGTH, []internalMetadataRow{
		{name: "enum", column: enumColumn, labels: "a,bb", want: 0, missing: true},
		{name: "set", column: setColumn, labels: "a,bb", want: 0, missing: true},
		{name: "unsigned integer", column: setColumn, want: 0, missing: true},
		{name: "varchar", column: types.New(types.T_varchar, 32, 0), want: 32},
		{name: "text", column: types.T_text.ToType(), want: int64(types.MaxStringSize)},
	})
}

func TestInternalCharSizeDeclaredEnumSetLabels(t *testing.T) {
	enumColumn := types.T_enum.ToType()
	setColumn := types.T_uint64.ToType()

	runInternalMetadataOverload(t, INTERNAL_CHAR_SIZE, []internalMetadataRow{
		{name: "enum octet length", column: enumColumn, labels: "a,bb", want: 8},
		{name: "enum octet length single character", column: enumColumn, labels: "z,a", want: 4},
		{name: "enum octet length multibyte", column: enumColumn, labels: "中,长长", want: 8},
		{name: "enum empty domain declares zero octet length", column: enumColumn, labels: "", want: 0},
		{name: "enum null labels stay null", column: enumColumn, nullLabel: true, want: 0, missing: true},
		{name: "set octet length", column: setColumn, labels: "a,bb", want: 16},
		{name: "set octet length two members", column: setColumn, labels: "z,a", want: 12},
		{name: "set octet length multibyte", column: setColumn, labels: "中,长长", want: 16},
		{name: "set without declared labels", column: setColumn, want: 0, missing: true},
		{name: "unsigned integer keeps null size", column: setColumn, labels: "", want: 0, missing: true},
		{name: "varchar keeps charset-scaled size", column: types.New(types.T_varchar, 32, 0), labels: "", want: 128},
		{name: "varbinary keeps octet width", column: types.New(types.T_varbinary, 16, 0), labels: "", want: 16},
		{name: "geometry srid labels stay null", column: types.T_geometry.ToType(), labels: "SRID=0", want: 0, missing: true},
	})

	runInternalMetadataSingleArg(t, INTERNAL_CHAR_SIZE, []internalMetadataRow{
		{name: "enum", column: enumColumn, labels: "a,bb", want: 0, missing: true},
		{name: "set", column: setColumn, labels: "a,bb", want: 0, missing: true},
		{name: "unsigned integer", column: setColumn, want: 0, missing: true},
		{name: "varchar", column: types.New(types.T_varchar, 32, 0), want: 128},
		{name: "text", column: types.T_text.ToType(), want: int64(types.MaxStringSize)},
	})
}

func TestInternalNumericMetadataDeclaredEnumSetLabels(t *testing.T) {
	enumColumn := types.T_enum.ToType()
	setColumn := types.T_uint64.ToType()
	mediumInt := types.New(types.T_int32, 24, 0)
	decimal := types.New(types.T_decimal64, 20, 6)
	floatMD := types.New(types.T_float64, 8, 2)

	runInternalMetadataOverload(t, INTERNAL_NUMERIC_PRECISION, []internalMetadataRow{
		{name: "set is not numeric metadata", column: setColumn, labels: "a,bb", want: 0, missing: true},
		{name: "set without declared labels keeps integer identity", column: setColumn, labels: "", want: 20},
		{name: "enum stays null", column: enumColumn, labels: "a,bb", want: 0, missing: true},
		{name: "enum empty domain stays null", column: enumColumn, labels: "", want: 0, missing: true},
		{name: "mediumint keeps 7 digits", column: mediumInt, labels: "", want: 7},
		{name: "decimal keeps declared precision", column: decimal, labels: "", want: 20},
		{name: "float keeps display precision", column: floatMD, labels: "", want: 8},
		{name: "bit keeps declared bits", column: types.New(types.T_bit, 8, 0), labels: "", want: 8},
		{name: "bool keeps 3 digits", column: types.T_bool.ToType(), labels: "", want: 3},
	})

	runInternalMetadataOverload(t, INTERNAL_NUMERIC_SCALE, []internalMetadataRow{
		{name: "set is not numeric metadata", column: setColumn, labels: "a,bb", want: 0, missing: true},
		{name: "set without declared labels keeps integer identity", column: setColumn, labels: "", want: 0},
		{name: "enum stays null", column: enumColumn, labels: "a,bb", want: 0, missing: true},
		{name: "enum empty domain stays null", column: enumColumn, labels: "", want: 0, missing: true},
		{name: "decimal keeps declared scale", column: decimal, labels: "", want: 6},
		{name: "float keeps display scale", column: floatMD, labels: "", want: 2},
		{name: "bool keeps scale zero", column: types.T_bool.ToType(), labels: "", want: 0},
	})

	runInternalMetadataSingleArg(t, INTERNAL_NUMERIC_PRECISION, []internalMetadataRow{
		{name: "set", column: setColumn, labels: "a,bb", want: 20},
		{name: "enum", column: enumColumn, labels: "a,bb", want: 0, missing: true},
		{name: "unsigned integer", column: setColumn, want: 20},
	})

	runInternalMetadataSingleArg(t, INTERNAL_NUMERIC_SCALE, []internalMetadataRow{
		{name: "set", column: setColumn, labels: "a,bb", want: 0},
		{name: "enum", column: enumColumn, labels: "a,bb", want: 0, missing: true},
		{name: "unsigned integer", column: setColumn, want: 0},
	})
}

// TestInternalDeclaredLabelEncodingBoundaryOracle pins the section-2.3
// boundary for the colliding ENUM declaration class - the pre-existing
// attr_enum encoding defect (strings.Join with no escaping) keeps its
// separately-designed follow-up; the corrected metadata interprets the
// persisted string under its documented format, the comma-separated member
// list as stored. ENUM('a,b') and ENUM('a','b') both persist "a,b" and both
// report the two-member lengths with no numeric metadata, the unambiguous
// control ENUM('ab','c') reports exactly 2/8, and SET stays unambiguous by
// construction through its declaration-time comma rejection.
func TestInternalDeclaredLabelEncodingBoundaryOracle(t *testing.T) {
	enumColumn := types.T_enum.ToType()

	runInternalMetadataOverload(t, INTERNAL_CHAR_LENGTH, []internalMetadataRow{
		{name: "colliding ENUM('a,b') reports the boundary value", column: enumColumn, labels: "a,b", want: 1},
		{name: "collision twin ENUM('a','b') reports exactly MySQL", column: enumColumn, labels: "a,b", want: 1},
		{name: "control ENUM('ab','c') reports exactly MySQL", column: enumColumn, labels: "ab,c", want: 2},
	})
	runInternalMetadataOverload(t, INTERNAL_CHAR_SIZE, []internalMetadataRow{
		{name: "colliding ENUM('a,b') reports the boundary value", column: enumColumn, labels: "a,b", want: 4},
		{name: "collision twin ENUM('a','b') reports exactly MySQL", column: enumColumn, labels: "a,b", want: 4},
		{name: "control ENUM('ab','c') reports exactly MySQL", column: enumColumn, labels: "ab,c", want: 8},
	})
	runInternalMetadataOverload(t, INTERNAL_NUMERIC_PRECISION, []internalMetadataRow{
		{name: "colliding ENUM('a,b') keeps numeric metadata null", column: enumColumn, labels: "a,b", want: 0, missing: true},
		{name: "control ENUM('ab','c') keeps numeric metadata null", column: enumColumn, labels: "ab,c", want: 0, missing: true},
	})
	runInternalMetadataOverload(t, INTERNAL_NUMERIC_SCALE, []internalMetadataRow{
		{name: "colliding ENUM('a,b') keeps numeric metadata null", column: enumColumn, labels: "a,b", want: 0, missing: true},
		{name: "control ENUM('ab','c') keeps numeric metadata null", column: enumColumn, labels: "ab,c", want: 0, missing: true},
	})

	_, err := types.NormalizeSetValues([]string{"a,b"})
	require.ErrorContains(t, err, "set member \"a,b\" contains ','",
		"SET('a,b') must stay rejected at declaration, keeping the SET encoding unambiguous")
	normalized, err := types.NormalizeSetValues([]string{"a", "bb"})
	require.NoError(t, err)
	require.Equal(t, []string{"a", "bb"}, normalized)
}
