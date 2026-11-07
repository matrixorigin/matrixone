// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// TestJSONStringComparisonOverloadIdentity pins the plan-level contract of
// issue #30783: a JSON value compared with SQL character data keeps the JSON
// domain identity instead of an implicit text cast or a rejected statement,
// in both operand orders and for every ordering or equality operator.
func TestJSONStringComparisonOverloadIdentity(t *testing.T) {
	names := []string{"=", "<=>", "!=", ">", ">=", "<", "<="}
	for _, name := range names {
		for _, args := range [][]types.Type{
			{types.T_json.ToType(), types.T_varchar.ToType()},
			{types.T_varchar.ToType(), types.T_json.ToType()},
			{types.T_json.ToType(), types.T_char.ToType()},
			{types.T_text.ToType(), types.T_json.ToType()},
		} {
			got, err := GetFunctionByName(context.Background(), name, args)
			require.NoError(t, err, "operator %s", name)
			_, overloadIndex := DecodeOverloadID(got.GetEncodedOverloadID())
			require.Equal(t, int32(1), overloadIndex, "operator %s identity overload", name)
			_, shouldCast := got.ShouldDoImplicitTypeCast()
			require.False(t, shouldCast, "operator %s must not cast", name)
		}
	}
	// The JSON/character identity is reserved for character text. A JSON value
	// with a binary-string peer rejects with the invalid-argument operator
	// error in both operand orders (round-23 boundary repair); only the
	// legacy-era rebinding restores the predecessor's forward cast-row leg for
	// DUMP/LOAD verification of persisted folded defaults.
	for _, args := range [][]types.Type{
		{types.T_json.ToType(), types.T_binary.ToType()},
		{types.T_binary.ToType(), types.T_json.ToType()},
	} {
		_, err := GetFunctionByName(context.Background(), "=", args)
		require.Error(t, err, "binary/JSON equality rejects in both operand orders")
	}
}

// jsonScalarForms builds the JSON operand with one row per SQL form: number 3,
// string "str", true, false, JSON literal null, -1, 0.5 and a SQL NULL row.
func jsonScalarForms(t *testing.T) []string {
	t.Helper()
	forms := []string{`3`, `"str"`, `true`, `false`, `null`, `-1`, `0.5`}
	encoded := make([]string, 0, len(forms)+1)
	for _, f := range forms {
		encoded = append(encoded, mustJsonBinaryString(t, f))
	}
	return append(encoded, "")
}

func jsonFormsNulls(rows []string) []bool {
	nulls := make([]bool, len(rows))
	nulls[len(nulls)-1] = true
	return nulls
}

// TestJSONStringComparisonExecutorContract runs the comparison executors on the
// mixed JSON/character identity. The SQL string compares as a JSON string
// scalar under MySQL's type-major order: numbers rank below strings, strings
// below booleans, and JSON literal null ranks below everything.
func TestJSONStringComparisonExecutorContract(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })

	jsonRows := jsonScalarForms(t)
	jsonNulls := jsonFormsNulls(jsonRows)
	// result row order: 3, "str", true, false, null, -1, 0.5, SQL NULL
	cases := []struct {
		name     string
		op       executeLogicOfOverload
		peer     []string
		expected []bool
		// expected null rows (for operators that can yield SQL NULL)
		resultNulls []bool
	}{
		{
			name:     "json = '3'",
			op:       equalFn,
			peer:     []string{"3", "3", "3", "3", "3", "3", "3", "3"},
			expected: []bool{false, false, false, false, false, false, false, false},
			resultNulls: []bool{false, false, false,
				false, false, false, false, true},
		},
		{
			name:     "json <=> '3'",
			op:       nullSafeEqualFn,
			peer:     []string{"3", "3", "3", "3", "3", "3", "3", "3"},
			expected: []bool{false, false, false, false, false, false, false, false},
			resultNulls: []bool{false, false, false,
				false, false, false, false, false},
		},
		{
			name:     "json != '3'",
			op:       notEqualFn,
			peer:     []string{"3", "3", "3", "3", "3", "3", "3", "3"},
			expected: []bool{true, true, true, true, true, true, true, true},
			resultNulls: []bool{false, false, false,
				false, false, false, false, true},
		},
		{
			name: "json > '1'",
			op:   greatThanFn,
			// number ranks below string; "str" > "1" lexically; booleans
			// rank above strings; JSON null ranks below the string peer.
			peer: []string{"1", "1", "1", "1", "1", "1", "1", "1"},
			expected: []bool{false, true, true, true,
				false, false, false, false},
			resultNulls: []bool{false, false, false, false,
				false, false, false, true},
		},
		{
			name: "json < '1'",
			op:   lessThanFn,
			peer: []string{"1", "1", "1", "1", "1", "1", "1", "1"},
			expected: []bool{true, false, false, false,
				true, true, true, false},
			resultNulls: []bool{false, false, false, false,
				false, false, false, true},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			jsonInput := NewFunctionTestInput(types.T_json.ToType(), jsonRows, jsonNulls)
			peerInput := NewFunctionTestInput(types.T_varchar.ToType(), tc.peer, nil)
			expected := NewFunctionTestResult(types.T_bool.ToType(), false, tc.expected, tc.resultNulls)
			fcTC := NewFunctionTestCase(proc,
				[]FunctionTestInput{jsonInput, peerInput}, expected, tc.op)
			s, info := fcTC.RunAndFree()
			require.True(t, s, "case %s, err info %s", tc.name, info)
		})
	}

	reversed := []struct {
		name     string
		op       executeLogicOfOverload
		expected []bool
	}{
		{"'3' = json", equalFn, []bool{false, false, false, false, false, false, false, false}},
		{"'1' < json", lessThanFn, []bool{false, true, true, true, false, false, false, false}},
	}
	for _, tc := range reversed {
		t.Run(tc.name, func(t *testing.T) {
			peerInput := NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"3", "1", "1", "1", "1", "1", "1", "1"}, nil)
			jsonInput := NewFunctionTestInput(types.T_json.ToType(), jsonRows, jsonNulls)
			expected := NewFunctionTestResult(types.T_bool.ToType(), false, tc.expected,
				[]bool{false, false, false, false, false, false, false, true})
			fcTC := NewFunctionTestCase(proc,
				[]FunctionTestInput{peerInput, jsonInput}, expected, tc.op)
			s, info := fcTC.RunAndFree()
			require.True(t, s, "case %s, err info %s", tc.name, info)
		})
	}
}

// TestJSONLiteralNullRanksBelowNumericPeer pins #30770 at the component level:
// JSON literal null compares by type rank with numeric peers instead of
// collapsing to SQL NULL. SQL NULL propagation stays vector-level.
func TestJSONLiteralNullRanksBelowNumericPeer(t *testing.T) {
	parse := func(s string) bytejson.ByteJson {
		value, err := types.ParseStringToByteJson(s)
		require.NoError(t, err)
		return value
	}
	nullJSON := parse("null")
	for _, typ := range []types.T{types.T_int8, types.T_int16, types.T_int32,
		types.T_int64, types.T_uint8, types.T_uint64, types.T_float32,
		types.T_float64, types.T_decimal64, types.T_decimal128, types.T_decimal256} {
		peer, err := makeJSONNumericPeer(context.Background(), parse("1"), typ, vector.PrepareParamNone)
		require.NoError(t, err)
		comparison, isNull, err := compareJSONNumeric(context.Background(), nullJSON, &peer)
		require.NoError(t, err)
		require.False(t, isNull, "JSON literal null must not surface as SQL NULL")
		require.Equal(t, -1, comparison, "JSON null must rank below %s", typ)
	}
	// Control: a numeric JSON value keeps its numeric comparison.
	peer, err := makeJSONNumericPeer(context.Background(), parse("1"), types.T_int64, vector.PrepareParamNone)
	require.NoError(t, err)
	comparison, isNull, err := compareJSONNumeric(context.Background(), parse("3"), &peer)
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, 1, comparison)
}

// TestCompareJSONScalarWithBooleanLiteralNullRanksBelow keeps the boolean peer
// symmetric with the numeric one: JSON null is a value below the boolean
// category, never SQL NULL.
func TestCompareJSONScalarWithBooleanLiteralNullRanksBelow(t *testing.T) {
	nullJSON, err := types.ParseStringToByteJson("null")
	require.NoError(t, err)
	comparison, jsonNull, err := compareJSONScalarWithBoolean(context.Background(), nullJSON, false)
	require.NoError(t, err)
	require.False(t, jsonNull)
	require.Equal(t, -1, comparison)

	comparison, jsonNull, err = compareJSONScalarWithBoolean(context.Background(), nullJSON, true)
	require.NoError(t, err)
	require.False(t, jsonNull)
	require.Equal(t, -1, comparison)
}

// TestPreparedCharacterParamUsesStringScalarDomain pins the prepared half of
// the literal/prepared consistency for #30783: a character parameter compares
// through the same JSON type ranks as the string literal, and JSON literal null
// parameters stay comparable values (#30770).
func TestPreparedCharacterParamUsesStringScalarDomain(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })

	parse := func(s string) bytejson.ByteJson {
		value, err := types.ParseStringToByteJson(s)
		require.NoError(t, err)
		return value
	}

	// A character parameter is the JSON peer of the same string literal.
	comparison, jsonNull, paramNull, err := comparePreparedJSONScalarsAsType(
		proc, parse(`"3"`), newTypedByteJson(bytejson.TpCodeString, "3"), types.T_varchar)
	require.NoError(t, err)
	require.False(t, jsonNull)
	require.False(t, paramNull)
	require.Equal(t, 0, comparison)

	// A JSON number never equals the JSON string parameter.
	comparison, jsonNull, paramNull, err = comparePreparedJSONScalarsAsType(
		proc, parse(`3`), newTypedByteJson(bytejson.TpCodeString, "3"), types.T_varchar)
	require.NoError(t, err)
	require.False(t, jsonNull)
	require.False(t, paramNull)
	require.Equal(t, -1, comparison)

	// JSON literal null ranks below the parameter's string value.
	comparison, jsonNull, paramNull, err = comparePreparedJSONScalarsAsType(
		proc, parse(`null`), newTypedByteJson(bytejson.TpCodeString, "null"), types.T_varchar)
	require.NoError(t, err)
	require.False(t, jsonNull)
	require.False(t, paramNull)
	require.Equal(t, -1, comparison)

	// An untyped text parameter uses the same JSON string scalar domain.
	comparison, jsonNull, paramNull, err = comparePreparedJSONScalars(
		proc, parse(`null`), newTypedByteJson(bytejson.TpCodeString, "null"), vector.PrepareParamNone)
	require.NoError(t, err)
	require.False(t, jsonNull)
	require.False(t, paramNull)
	require.Equal(t, -1, comparison)

	// A JSON-typed parameter carries a null document that stays comparable.
	comparison, jsonNull, paramNull, err = comparePreparedJSONScalarsAsType(
		proc, parse(`null`), parse(`null`), types.T_json)
	require.NoError(t, err)
	require.False(t, jsonNull)
	require.False(t, paramNull)
	require.Equal(t, 0, comparison)
}

// TestCoalesceStrSerializesJSONBranchAsDocumentText pins #30934 at the
// executor level: a JSON branch selected by COALESCE serializes with CAST AS
// CHAR text semantics, keeping a JSON string's quotes so an INSERT SELECT back
// into a JSON column preserves the scalar type.
func TestCoalesceStrSerializesJSONBranchAsDocumentText(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })

	jsonRows := []string{
		mustJsonBinaryString(t, `"2"`),
		mustJsonBinaryString(t, `"a b"`),
		mustJsonBinaryString(t, `null`),
		mustJsonBinaryString(t, `{"x":1}`),
		mustJsonBinaryString(t, `123`),
		mustJsonBinaryString(t, `1.0`),
		"",
	}
	jsonInput := NewFunctionTestInput(types.T_json.ToType(), jsonRows, jsonFormsNulls(jsonRows))
	textInput := NewFunctionTestInput(types.T_varchar.ToType(),
		[]string{"fallback", "fallback", "fallback", "fallback", "fallback", "fallback", "fallback"}, nil)
	expected := NewFunctionTestResult(types.T_varchar.ToType(), false,
		[]string{`"2"`, `"a b"`, `null`, `{"x": 1}`, `123`, `1.0`, "fallback"},
		nil)
	fcTC := NewFunctionTestCase(proc,
		[]FunctionTestInput{jsonInput, textInput}, expected, CoalesceStr)
	s, info := fcTC.RunAndFree()
	require.True(t, s, "coalesce json/text info %s", info)

	// Both-null rows still yield SQL NULL.
	jsonNulls := []bool{false, true}
	jsonInput = NewFunctionTestInput(types.T_json.ToType(),
		[]string{mustJsonBinaryString(t, `"k"`), ""}, jsonNulls)
	textInput = NewFunctionTestInput(types.T_varchar.ToType(), []string{"x", ""}, jsonNulls)
	expected = NewFunctionTestResult(types.T_varchar.ToType(), false,
		[]string{`"k"`, ""}, []bool{false, true})
	fcTC = NewFunctionTestCase(proc,
		[]FunctionTestInput{jsonInput, textInput}, expected, CoalesceStr)
	s, info = fcTC.RunAndFree()
	require.True(t, s, "coalesce both-null info %s", info)
}

// TestCoalesceStrJSONResultKeepsRawPayload pins the all-JSON COALESCE
// overload: its result vector keeps the internal JSON encoding, so branch
// payloads pass through unchanged. Document-text serialization belongs to the
// text-typed result only; marshaling into a JSON result would store uncoercible
// text inside a JSON vector.
func TestCoalesceStrJSONResultKeepsRawPayload(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })

	leftRows := []string{
		mustJsonBinaryString(t, `42`),
		"", "", "",
	}
	leftNulls := []bool{false, true, true, true}
	rightRows := []string{
		"",
		mustJsonBinaryString(t, `"2"`),
		mustJsonBinaryString(t, `null`),
		"",
	}
	rightNulls := []bool{true, false, false, true}
	expectedRows := []string{
		mustJsonBinaryString(t, `42`),
		mustJsonBinaryString(t, `"2"`),
		mustJsonBinaryString(t, `null`),
		"",
	}
	expectedNulls := []bool{false, false, false, true}
	left := NewFunctionTestInput(types.T_json.ToType(), leftRows, leftNulls)
	right := NewFunctionTestInput(types.T_json.ToType(), rightRows, rightNulls)
	expected := NewFunctionTestResult(types.T_json.ToType(), false, expectedRows, expectedNulls)
	fcTC := NewFunctionTestCase(proc,
		[]FunctionTestInput{left, right}, expected, CoalesceStr)
	s, info := fcTC.RunAndFree()
	require.True(t, s, "coalesce json-result info %s", info)
}

// TestJSONDocumentOrderingStaysRanked pins the pre-existing JSON/JSON
// ordering contract: two JSON values order by MySQL scalar precedence and the
// plain comparison operators resolve the pair without rejection or any new
// cast domain. JSON/character comparisons gaining their own identity domain
// (#31121) must not disturb this older pair: the retained
// func_json_extract_order.sql matrix and MySQL 8.0 both rank JSON documents in
// both operand orders.
func TestJSONDocumentOrderingStaysRanked(t *testing.T) {
	// Resolution must stay successful for every plain operator.
	for _, name := range []string{"=", "!=", "<=>", ">", ">=", "<", "<="} {
		_, err := GetFunctionByName(context.Background(), name,
			[]types.Type{types.T_json.ToType(), types.T_json.ToType()})
		require.NoError(t, err, "operator %s json/json resolution", name)
	}

	proc := testutil.NewProcess(t)
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })

	// Pairs: ("a" vs "b"), ("b" vs "a"), (1 vs "a"), (true vs "a"),
	// (JSON literal null vs 0), ("a" vs SQL NULL).
	leftRows := []string{
		mustJsonBinaryString(t, `"a"`),
		mustJsonBinaryString(t, `"b"`),
		mustJsonBinaryString(t, `1`),
		mustJsonBinaryString(t, `true`),
		mustJsonBinaryString(t, `null`),
		mustJsonBinaryString(t, `"a"`),
	}
	rightRows := []string{
		mustJsonBinaryString(t, `"b"`),
		mustJsonBinaryString(t, `"a"`),
		mustJsonBinaryString(t, `"a"`),
		mustJsonBinaryString(t, `"a"`),
		mustJsonBinaryString(t, `0`),
		mustJsonBinaryString(t, `"b"`),
	}
	rightNulls := []bool{false, false, false, false, false, true}
	resultNulls := []bool{false, false, false, false, false, true}

	cases := []struct {
		name     string
		op       executeLogicOfOverload
		expected []bool
	}{
		{"json < json", lessThanFn, []bool{true, false, true, false, true, false}},
		{"json > json", greatThanFn, []bool{false, true, false, true, false, false}},
		{"json <= json", lessEqualFn, []bool{true, false, true, false, true, false}},
		{"json >= json", greatEqualFn, []bool{false, true, false, true, false, false}},
		{"json = json", equalFn, []bool{false, false, false, false, false, false}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			left := NewFunctionTestInput(types.T_json.ToType(), leftRows, nil)
			right := NewFunctionTestInput(types.T_json.ToType(), rightRows, rightNulls)
			expected := NewFunctionTestResult(types.T_bool.ToType(), false, tc.expected, resultNulls)
			fcTC := NewFunctionTestCase(proc,
				[]FunctionTestInput{left, right}, expected, tc.op)
			s, info := fcTC.RunAndFree()
			require.True(t, s, "case %s info %s", tc.name, info)
		})
	}
}
