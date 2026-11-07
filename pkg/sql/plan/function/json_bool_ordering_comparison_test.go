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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// TestJSONBooleanComparisonOverloadIdentity pins the plan-level contract of
// the #31121/#30770 JSON literal-null ranking: a mixed JSON/BOOL pair binds
// the direct comparison identity in both operand orders and for every
// ordering and equality operator. The pre-#31121 binding cast both operands
// to BOOL, which folded a JSON literal null into SQL NULL through the SQL
// boolean cast before the comparison could see it.
func TestJSONBooleanComparisonOverloadIdentity(t *testing.T) {
	names := []string{"=", "<=>", "!=", ">", ">=", "<", "<="}
	for _, name := range names {
		for _, args := range [][]types.Type{
			{types.T_json.ToType(), types.T_bool.ToType()},
			{types.T_bool.ToType(), types.T_json.ToType()},
		} {
			label := "operator " + name + " " + args[0].Oid.String() + "," + args[1].Oid.String()
			got, err := GetFunctionByName(context.Background(), name, args)
			require.NoError(t, err, label)
			_, overloadIndex := DecodeOverloadID(got.GetEncodedOverloadID())
			require.Equal(t, int32(0), overloadIndex, label+" direct identity overload")
			_, shouldCast := got.ShouldDoImplicitTypeCast()
			require.False(t, shouldCast, label+" must not fold through the SQL boolean cast")
		}
	}
	// The JSON/BOOL identity does not leak into same-domain pairs.
	got, err := GetFunctionByName(context.Background(), "<", []types.Type{types.T_json.ToType(), types.T_json.ToType()})
	require.NoError(t, err)
	_, shouldCast := got.ShouldDoImplicitTypeCast()
	require.False(t, shouldCast)
}

// JSON scalar rows shared with the mixed-domain executor contract tests:
// number 3, string "str", true, false, JSON literal null, -1, 0.5 and a SQL
// NULL row. The boolean kernel ranks a JSON literal null, strings, objects
// and arrays below the boolean category (MySQL 8.0.44 receipts), compares
// JSON literal true/false by boolean value, and folds JSON numbers by the
// established zero/nonzero rule pinned by the upstream func_json_extract
// rows (cast(1.20 as decimal) = true, 1e100/1e-300 = true).
func jsonBooleanOrderingRows(t *testing.T) ([]string, []bool) {
	t.Helper()
	rows := jsonScalarForms(t)
	return rows, jsonFormsNulls(rows)
}

// TestJSONBooleanOrderingExecutorContract runs the ordering and equality
// executors over the mixed JSON/BOOL identity. Expected values are pinned by
// the MySQL 8.0.44 receipts (string category below the boolean peer) and by
// the upstream-pinned zero/nonzero fold for JSON numbers.
func TestJSONBooleanOrderingExecutorContract(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })

	jsonRows, jsonNulls := jsonBooleanOrderingRows(t)
	boolRows := []bool{true, true, true, true, true, true, true, true}
	// row order: 3, "str", true, false, null, -1, 0.5, SQL NULL
	cases := []struct {
		name     string
		op       executeLogicOfOverload
		operand  []bool
		expected []bool
		// expected SQL NULL rows
		resultNulls []bool
	}{
		{
			name: "json < TRUE",
			op:   lessThanFn,
			expected: []bool{false, true, false, true, true,
				false, false, false},
			resultNulls: []bool{false, false, false, false, false,
				false, false, true},
		},
		{
			name: "json <= TRUE",
			op:   lessEqualFn,
			expected: []bool{true, true, true, true, true,
				true, true, false},
			resultNulls: []bool{false, false, false, false, false,
				false, false, true},
		},
		{
			name: "json > TRUE",
			op:   greatThanFn,
			expected: []bool{false, false, false, false, false,
				false, false, false},
			resultNulls: []bool{false, false, false, false, false,
				false, false, true},
		},
		{
			name: "json >= TRUE",
			op:   greatEqualFn,
			expected: []bool{true, false, true, false, false,
				true, true, false},
			resultNulls: []bool{false, false, false, false, false,
				false, false, true},
		},
		{
			name: "json = TRUE",
			op:   equalFn,
			expected: []bool{true, false, true, false, false,
				true, true, false},
			resultNulls: []bool{false, false, false, false, false,
				false, false, true},
		},
		{
			name: "json != TRUE",
			op:   notEqualFn,
			expected: []bool{false, true, false, true, true,
				false, false, false},
			resultNulls: []bool{false, false, false, false, false,
				false, false, true},
		},
		{
			name: "json <=> TRUE",
			op:   nullSafeEqualFn,
			expected: []bool{true, false, true, false, false,
				true, true, false},
			resultNulls: []bool{false, false, false, false, false,
				false, false, false},
		},
		{
			name: "json < FALSE",
			op:   lessThanFn,
			operand: []bool{false, false, false, false, false,
				false, false, false},
			expected: []bool{false, true, false, false, true,
				false, false, false},
			resultNulls: []bool{false, false, false, false, false,
				false, false, true},
		},
		{
			name: "json = FALSE",
			op:   equalFn,
			operand: []bool{false, false, false, false, false,
				false, false, false},
			expected: []bool{false, false, false, true, false,
				false, false, false},
			resultNulls: []bool{false, false, false, false, false,
				false, false, true},
		},
		{
			name: "json <=> FALSE",
			op:   nullSafeEqualFn,
			operand: []bool{false, false, false, false, false,
				false, false, false},
			expected: []bool{false, false, false, true, false,
				false, false, false},
			resultNulls: []bool{false, false, false, false, false,
				false, false, false},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			operand := tc.operand
			if operand == nil {
				operand = boolRows
			}
			jsonInput := NewFunctionTestInput(types.T_json.ToType(), jsonRows, jsonNulls)
			boolInput := NewFunctionTestInput(types.T_bool.ToType(), operand, nil)
			expected := NewFunctionTestResult(types.T_bool.ToType(), false, tc.expected, tc.resultNulls)
			fcTC := NewFunctionTestCase(proc,
				[]FunctionTestInput{jsonInput, boolInput}, expected, tc.op)
			s, info := fcTC.RunAndFree()
			require.True(t, s, "case %s, err info %s", tc.name, info)
		})
	}

	// The reversed orientation exercises the sign flip: the kernel orients
	// the JSON operand, so a boolean left operand mirrors every predicate.
	reversed := []struct {
		name     string
		op       executeLogicOfOverload
		operand  []bool
		expected []bool
	}{
		{"TRUE > json", greatThanFn, boolRows,
			[]bool{false, true, false, true, true, false, false, false}},
		{"TRUE >= json", greatEqualFn, boolRows,
			[]bool{true, true, true, true, true, true, true, false}},
		{"TRUE < json", lessThanFn, boolRows,
			[]bool{false, false, false, false, false, false, false, false}},
		{"TRUE <= json", lessEqualFn, boolRows,
			[]bool{true, false, true, false, false, true, true, false}},
		{"FALSE > json", greatThanFn, []bool{false, false, false, false, false, false, false, false},
			// JSON numbers fold to their zero/nonzero boolean value, literal
			// null and the string category rank below the boolean peer.
			[]bool{false, true, false, false, true, false, false, false}},
	}
	for _, tc := range reversed {
		t.Run(tc.name, func(t *testing.T) {
			boolInput := NewFunctionTestInput(types.T_bool.ToType(), tc.operand, nil)
			jsonInput := NewFunctionTestInput(types.T_json.ToType(), jsonRows, jsonNulls)
			expected := NewFunctionTestResult(types.T_bool.ToType(), false, tc.expected,
				[]bool{false, false, false, false, false, false, false, true})
			fcTC := NewFunctionTestCase(proc,
				[]FunctionTestInput{boolInput, jsonInput}, expected, tc.op)
			s, info := fcTC.RunAndFree()
			require.True(t, s, "case %s, err info %s", tc.name, info)
		})
	}
}

// TestJSONBooleanCategoryRankingMySQLReceipt pins the whole mixed JSON/BOOL
// category ranking with fresh MySQL 8.0.44 receipts: JSON strings, objects
// and arrays rank below the boolean category in every operator and operand
// order, exactly like a JSON literal null (review-artifacts
// mysql-r21-bool-table.txt, mysql-r21-bool-category-cells.txt). The
// pre-#31121 cast semantics error on these rows through boolean-text
// parsing or array/object scalar casts, which is the heterogeneous-abort
// class the parent issue tracks.
func TestJSONBooleanCategoryRankingMySQLReceipt(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })

	rows := []string{
		mustJsonBinaryString(t, `"x"`),
		mustJsonBinaryString(t, `"true"`),
		mustJsonBinaryString(t, `{"a":1}`),
		mustJsonBinaryString(t, `[1,2]`),
	}
	trues := []bool{true, true, true, true}
	falses := []bool{false, false, false, false}

	for _, tc := range []struct {
		name     string
		op       executeLogicOfOverload
		jsonLeft bool
		operand  []bool
		expected []bool
	}{
		{"row < TRUE", lessThanFn, true, trues, []bool{true, true, true, true}},
		{"row <= TRUE", lessEqualFn, true, trues, []bool{true, true, true, true}},
		{"row > TRUE", greatThanFn, true, trues, []bool{false, false, false, false}},
		{"row >= TRUE", greatEqualFn, true, trues, []bool{false, false, false, false}},
		{"row = TRUE", equalFn, true, trues, []bool{false, false, false, false}},
		{"row != TRUE", notEqualFn, true, trues, []bool{true, true, true, true}},
		{"row <=> TRUE", nullSafeEqualFn, true, trues, []bool{false, false, false, false}},
		{"row < FALSE", lessThanFn, true, falses, []bool{true, true, true, true}},
		{"row = FALSE", equalFn, true, falses, []bool{false, false, false, false}},
		{"row > FALSE", greatThanFn, true, falses, []bool{false, false, false, false}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			jsonInput := NewFunctionTestInput(types.T_json.ToType(), rows, nil)
			boolInput := NewFunctionTestInput(types.T_bool.ToType(), tc.operand, nil)
			inputs := []FunctionTestInput{jsonInput, boolInput}
			if !tc.jsonLeft {
				inputs = []FunctionTestInput{boolInput, jsonInput}
			}
			expected := NewFunctionTestResult(types.T_bool.ToType(), false, tc.expected, nil)
			fcTC := NewFunctionTestCase(proc, inputs, expected, tc.op)
			s, info := fcTC.RunAndFree()
			require.True(t, s, "case %s, err info %s", tc.name, info)
		})
	}

	// The reversed orientation keeps the ranking a mirror: TRUE > "x" and
	// FALSE > "x" are true, and the boolean peer never lands below the
	// string/object/array categories (receipt mysql-r21-bool-table.txt:
	// T_gt_strx and T_le_sstrings rows).
	for _, tc := range []struct {
		name     string
		op       executeLogicOfOverload
		operand  []bool
		expected []bool
	}{
		{"TRUE > row", greatThanFn, trues, []bool{true, true, true, true}},
		{"TRUE < row", lessThanFn, trues, []bool{false, false, false, false}},
		{"TRUE <= \"true\"", lessEqualFn, trues, []bool{false, false, false, false}},
		{"TRUE >= \"true\"", greatEqualFn, trues, []bool{true, true, true, true}},
		{"FALSE > row", greatThanFn, falses, []bool{true, true, true, true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			boolInput := NewFunctionTestInput(types.T_bool.ToType(), tc.operand, nil)
			jsonInput := NewFunctionTestInput(types.T_json.ToType(), rows, nil)
			expected := NewFunctionTestResult(types.T_bool.ToType(), false, tc.expected, nil)
			fcTC := NewFunctionTestCase(proc,
				[]FunctionTestInput{boolInput, jsonInput}, expected, tc.op)
			s, info := fcTC.RunAndFree()
			require.True(t, s, "case %s, err info %s", tc.name, info)
		})
	}
}

// TestJSONBooleanLegacyEraEqualityFolds pins the DUMP era's pre-#31121 fold
// for the mixed JSON/BOOL equality family. Only that family reaches the
// JSON/BOOL kernel in the era; the era binder restores the (BOOL, BOOL)
// cast pair for ordering (TestJSONBooleanLegacyEraOrderingCastRoute). The
// predecessor kernel kept a JSON string as a category of its own, so a
// document carrying the text "true" never equalled TRUE (the historical
// counterexample the local review re-opened), and a JSON literal null
// folded into the SQL NULL contract on every predicate.
func TestJSONBooleanLegacyEraEqualityFolds(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })
	proc.Ctx = WithLegacyJSONScalarSemantics(proc.Ctx)

	rows := []string{
		mustJsonBinaryString(t, "null"),
		mustJsonBinaryString(t, "\"true\""),
		mustJsonBinaryString(t, "true"),
	}
	trues := []bool{true, true, true}
	for _, tc := range []struct {
		name     string
		op       executeLogicOfOverload
		expected []bool
		nulls    []bool
	}{
		{"json = TRUE", equalFn, []bool{false, false, true}, []bool{true, false, false}},
		{"json != TRUE", notEqualFn, []bool{false, true, false}, []bool{true, false, false}},
		{"json <=> TRUE", nullSafeEqualFn, []bool{false, false, true}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			jsonInput := NewFunctionTestInput(types.T_json.ToType(), rows, nil)
			boolInput := NewFunctionTestInput(types.T_bool.ToType(), trues, nil)
			expected := NewFunctionTestResult(types.T_bool.ToType(), false, tc.expected, tc.nulls)
			fcTC := NewFunctionTestCase(proc,
				[]FunctionTestInput{jsonInput, boolInput}, expected, tc.op)
			s, info := fcTC.RunAndFree()
			require.True(t, s, "case %s, err info %s", tc.name, info)
		})
	}
	// The reversed orientation keeps the same era folds: equality predicates
	// are sign-insensitive under the orientation flip.
	for _, tc := range []struct {
		name     string
		op       executeLogicOfOverload
		expected []bool
		nulls    []bool
	}{
		{"TRUE = json", equalFn, []bool{false, false, true}, []bool{true, false, false}},
		{"TRUE <=> json", nullSafeEqualFn, []bool{false, false, true}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			jsonInput := NewFunctionTestInput(types.T_json.ToType(), rows, nil)
			boolInput := NewFunctionTestInput(types.T_bool.ToType(), trues, nil)
			expected := NewFunctionTestResult(types.T_bool.ToType(), false, tc.expected, tc.nulls)
			fcTC := NewFunctionTestCase(proc,
				[]FunctionTestInput{boolInput, jsonInput}, expected, tc.op)
			s, info := fcTC.RunAndFree()
			require.True(t, s, "case %s, err info %s", tc.name, info)
		})
	}
}

// TestJSONBooleanLegacyEraOrderingCastRoute pins the binder restoration for
// the pre-#31121 mixed JSON/BOOL ordering family: a predecessor catalog
// resolved these predicates through the generic comparison cast rule, which
// admitted (BOOL, BOOL) with an implicit CAST of the JSON operand, so
// verification of an old archive folds through the shared SQL comparison
// executors instead of the JSON-domain kernel. The era keeps the historical
// mixed JSON/string ordering rejection and leaves the mixed JSON/BOOL
// equality admission untouched (TestJSONBooleanLegacyEraEqualityFolds pins
// its kernel values); its persisted-defaults leg runs against archives
// authored by the predecessor production sources in
// pkg/sql/plan/table_dump_json_scalar_semantics_test.go and
// pkg/frontend/table_dump_json_scalar_test.go.
func TestJSONBooleanLegacyEraOrderingCastRoute(t *testing.T) {
	jsonTyp := types.T_json.ToType()
	boolTyp := types.T_bool.ToType()
	currentDirect := newCheckResultWithSuccess(0)
	for _, id := range []int32{GREAT_THAN, GREAT_EQUAL, LESS_THAN, LESS_EQUAL} {
		for _, orientation := range [][2]types.Type{{jsonTyp, boolTyp}, {boolTyp, jsonTyp}} {
			legacy, ok := legacyJSONScalarCheck(id, nil,
				[]types.Type{orientation[0], orientation[1]}, currentDirect)
			require.True(t, ok, "ordering id %d must rebind under the era", id)
			require.Equal(t, succeedWithCast, legacy.status, "ordering id %d", id)
			require.Equal(t, 0, legacy.idx, "ordering id %d", id)
			require.Len(t, legacy.finalType, 2, "ordering id %d", id)
			require.Equal(t, types.T_bool, legacy.finalType[0].Oid, "ordering id %d", id)
			require.Equal(t, types.T_bool, legacy.finalType[1].Oid, "ordering id %d", id)
		}
	}
	legacy, ok := legacyJSONScalarCheck(GREAT_THAN, nil,
		[]types.Type{jsonTyp, types.T_varchar.ToType()}, newCheckResultWithSuccess(1))
	require.True(t, ok, "mixed JSON/string ordering must rebind under the era")
	require.Equal(t, failedFunctionParametersWrong, legacy.status)
	for _, id := range []int32{EQUAL, NOT_EQUAL, NULL_SAFE_EQUAL} {
		kept, ok := legacyJSONScalarCheck(id, nil, []types.Type{jsonTyp, boolTyp}, currentDirect)
		require.False(t, ok, "equality id %d keeps its current direct admission in the era", id)
		require.Equal(t, currentDirect, kept, "equality id %d result is untouched", id)
	}
}

// TestJSONBooleanOrderingZoneMapAndNullability refuses persistence-key and
// non-null inference for the direct mixed JSON/BOOL comparisons: the JSON
// summary domain does not encode the SQL boolean one, and a physically
// non-NULL JSON value can still carry a comparable JSON literal null.
func TestJSONBooleanOrderingZoneMapAndNullability(t *testing.T) {
	forEachOperatorID := func(yield func(id int32, name string)) {
		for _, entry := range []struct {
			id   int32
			name string
		}{
			{EQUAL, "="}, {NULL_SAFE_EQUAL, "<=>"}, {NOT_EQUAL, "!="},
			{GREAT_THAN, ">"}, {GREAT_EQUAL, ">="},
			{LESS_THAN, "<"}, {LESS_EQUAL, "<="},
		} {
			yield(entry.id, entry.name)
		}
	}
	jsonTyp := plan.Type{Id: int32(types.T_json), NotNullable: true}
	boolTyp := plan.Type{Id: int32(types.T_bool), NotNullable: true}
	reversedTyp := [2]plan.Type{boolTyp, jsonTyp}
	for _, orientation := range [][2]plan.Type{{jsonTyp, boolTyp}, reversedTyp} {
		args := []*plan.Expr{{Typ: orientation[0]}, {Typ: orientation[1]}}
		forEachOperatorID(func(id int32, name string) {
			label := name + " mixed JSON/BOOL"
			require.False(t, CanUseZoneMapComparison(EncodeOverloadID(id, 0), args),
				"zone-map admission for %s", label)
			// The null-safe comparison never produces SQL NULL, so it may
			// infer a non-NULL result; every other mixed operator can fold a
			// physically non-NULL JSON literal null into SQL NULL.
			if id == NULL_SAFE_EQUAL {
				require.True(t, DeduceNotNullable(EncodeOverloadID(id, 0), args),
					"null-safe result is always non-NULL for %s", label)
				return
			}
			require.False(t, DeduceNotNullable(EncodeOverloadID(id, 0), args),
				"non-null inference for %s", label)
		})
	}
	// Same-domain pairs keep their own history.
	for _, args := range [][]*plan.Expr{
		{{Typ: jsonTyp}, {Typ: jsonTyp}},
		{{Typ: boolTyp}, {Typ: boolTyp}},
	} {
		forEachOperatorID(func(id int32, name string) {
			if id == NULL_SAFE_EQUAL {
				return
			}
			require.True(t, CanUseZoneMapComparison(EncodeOverloadID(id, 0), args),
				"same-domain admission for %s", name)
		})
	}
}
