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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// TestJSONBooleanBetweenOverloadIdentity pins the plan-level contract of the
// #31121 review fix: the mixed JSON/BOOL BETWEEN binds the direct comparison
// identity for every admitted triple, exactly like the binary ordering
// operators the NOT BETWEEN lowering produces. The historical check folded
// the JSON operand into BOOL through the generic cast rule, so a JSON string
// such as "true" became indistinguishable from the JSON boolean true and the
// positive and negated range forms could select the same non-null row.
func TestJSONBooleanBetweenOverloadIdentity(t *testing.T) {
	jsonTyp := types.T_json.ToType()
	boolTyp := types.T_bool.ToType()
	shapes := [][]types.Type{
		{jsonTyp, boolTyp, boolTyp},
		{boolTyp, jsonTyp, boolTyp},
		{boolTyp, boolTyp, jsonTyp},
		{jsonTyp, jsonTyp, boolTyp},
		{jsonTyp, boolTyp, jsonTyp},
		{boolTyp, jsonTyp, jsonTyp},
	}
	for _, args := range shapes {
		got, err := GetFunctionByName(context.Background(), "between", args)
		require.NoError(t, err)
		_, overloadIndex := DecodeOverloadID(got.GetEncodedOverloadID())
		require.Equal(t, int32(0), overloadIndex, "direct identity overload")
		_, shouldCast := got.ShouldDoImplicitTypeCast()
		require.False(t, shouldCast,
			"mixed JSON/BOOL BETWEEN triple must not fold through the SQL boolean cast")
	}
	// Homogeneous and out-of-domain shapes keep their historical domains: the
	// all-BOOL triple keeps its native admission and a character bound keeps
	// the same ordering rejection as the binary legs.
	got, err := GetFunctionByName(context.Background(), "between", []types.Type{boolTyp, boolTyp, boolTyp})
	require.NoError(t, err)
	_, shouldCast := got.ShouldDoImplicitTypeCast()
	require.False(t, shouldCast, "the native BOOL BETWEEN keeps its direct overload")
	_, err = GetFunctionByName(context.Background(), "between",
		[]types.Type{jsonTyp, types.T_varchar.ToType(), types.T_varchar.ToType()})
	require.Error(t, err, "JSON BETWEEN with SQL string bounds keeps the ordering rejection")
	_, err = GetFunctionByName(context.Background(), "between",
		[]types.Type{jsonTyp, types.T_int64.ToType(), types.T_int64.ToType()})
	require.NoError(t, err, "JSON BETWEEN with integer bounds keeps the numeric cast domain")
}

// betweenJSONBooleanCase is one executor contract case: the expression column
// and both bounds are full-length columns (the bound columns repeat their
// constant or per-row values), so every row exercises its own leg pair.
type betweenJSONBooleanCase struct {
	name     string
	inputs   []FunctionTestInput
	expected []bool
	nulls    []bool
}

func runBetweenJSONBooleanCase(t *testing.T, proc *process.Process, tc betweenJSONBooleanCase) *vector.Vector {
	t.Helper()
	result := runJsonFunctionWithSelectList(t, proc, tc.inputs,
		types.T_bool.ToType(), betweenImpl, nil)
	values := vector.MustFixedColNoTypeCheck[bool](result)
	require.Equal(t, len(tc.expected), result.Length(), "case %s", tc.name)
	require.Equal(t, tc.expected, values, "case %s", tc.name)
	for row := 0; row < len(tc.expected); row++ {
		rowIsNull := len(tc.nulls) > 0 && tc.nulls[row]
		require.Equal(t, rowIsNull, result.IsNull(uint64(row)),
			"case %s row %d NULL mismatch", tc.name, row)
	}
	return result
}

// assertBetweenMatchesLoweredLegs proves the two invariants of the mixed
// domain fix: the positive form equals its lowered legs
// `lo <= x AND x <= hi`, and the negated form equals the complement of the
// positive form on every non-null row, using the same binary kernels the
// `x < lo OR x > hi` NOT BETWEEN rewrite binds.
func assertBetweenJSONBooleanMatchesLoweredLegs(
	t *testing.T, proc *process.Process, tc betweenJSONBooleanCase, between *vector.Vector,
) {
	t.Helper()
	x, lo, hi := tc.inputs[0], tc.inputs[1], tc.inputs[2]
	lower := runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{lo, x}, types.T_bool.ToType(), lessEqualFn, nil)
	lowerValues := vector.MustFixedColNoTypeCheck[bool](lower)
	upper := runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{x, hi}, types.T_bool.ToType(), lessEqualFn, nil)
	upperValues := vector.MustFixedColNoTypeCheck[bool](upper)
	// The NOT BETWEEN rewrite binds `x < lo OR x > hi`, so the negated legs
	// take the expression column as their left operand.
	negLower := runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{x, lo}, types.T_bool.ToType(), lessThanFn, nil)
	negLowerValues := vector.MustFixedColNoTypeCheck[bool](negLower)
	negUpper := runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{x, hi}, types.T_bool.ToType(), greatThanFn, nil)
	negUpperValues := vector.MustFixedColNoTypeCheck[bool](negUpper)
	betweenValues := vector.MustFixedColNoTypeCheck[bool](between)
	for row := range tc.expected {
		if between.IsNull(uint64(row)) {
			continue
		}
		require.Equal(t, lowerValues[row] && upperValues[row], betweenValues[row],
			"case %s row %d: BETWEEN must equal its lowered AND legs", tc.name, row)
		negated := negLowerValues[row] || negUpperValues[row]
		require.Equal(t, negated, !betweenValues[row],
			"case %s row %d: NOT BETWEEN must complement BETWEEN", tc.name, row)
	}
}

// TestJSONBooleanBetweenExecutorContract runs betweenImpl over the mixed
// JSON/BOOL scalar domain with the shared scalar rows
// (3, "str", true, false, JSON literal null, -1, 0.5, SQL NULL) and the
// receipt category rows. Expected values follow the pinned ranking that the
// binary ordering kernels already carry: a JSON string, object or array and a
// JSON literal null rank below the boolean peer, JSON true/false compare by
// boolean value, and JSON numbers keep the zero/nonzero boolean fold.
func TestJSONBooleanBetweenExecutorContract(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })

	jsonRows, jsonNulls := jsonBooleanOrderingRows(t)
	rowCount := len(jsonRows)
	trues := make([]bool, rowCount)
	falses := make([]bool, rowCount)
	for i := range trues {
		trues[i] = true
	}
	jsonTrue := make([]string, rowCount)
	jsonFalse := make([]string, rowCount)
	jsonNullLiteral := make([]string, rowCount)
	for i := range jsonTrue {
		jsonTrue[i] = mustJsonBinaryString(t, `true`)
		jsonFalse[i] = mustJsonBinaryString(t, `false`)
		jsonNullLiteral[i] = mustJsonBinaryString(t, `null`)
	}

	// All rows report values; a SQL NULL row keeps the false storage value
	// and only raises the null bit, so every expected vector is uniform false
	// or explicit.
	caseNulls := func() []bool {
		nulls := make([]bool, rowCount)
		nulls[rowCount-1] = true
		return nulls
	}

	cases := []betweenJSONBooleanCase{
		{
			// Row order: 3, "str", true, false, literal null, -1, 0.5, SQL NULL.
			name: "row between TRUE and TRUE",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(), jsonRows, jsonNulls),
				NewFunctionTestInput(types.T_bool.ToType(), trues, nil),
				NewFunctionTestInput(types.T_bool.ToType(), trues, nil),
			},
			// Numbers fold to their boolean value, so 3, -1 and 0.5 equal TRUE;
			// the JSON string "str" ranks below the boolean peer, so the
			// review's row class is false here and true through NOT BETWEEN only.
			expected: []bool{true, false, true, false, false, true, true, false},
			nulls:    caseNulls(),
		},
		{
			name: "row between FALSE and TRUE",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(), jsonRows, jsonNulls),
				NewFunctionTestInput(types.T_bool.ToType(), falses, nil),
				NewFunctionTestInput(types.T_bool.ToType(), trues, nil),
			},
			expected: []bool{true, false, true, true, false, true, true, false},
			nulls:    caseNulls(),
		},
		{
			name: "row between TRUE and FALSE (empty range)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(), jsonRows, jsonNulls),
				NewFunctionTestInput(types.T_bool.ToType(), trues, nil),
				NewFunctionTestInput(types.T_bool.ToType(), falses, nil),
			},
			expected: repeatBool(false, rowCount),
			nulls:    caseNulls(),
		},
		{
			name: "row between FALSE and FALSE",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(), jsonRows, jsonNulls),
				NewFunctionTestInput(types.T_bool.ToType(), falses, nil),
				NewFunctionTestInput(types.T_bool.ToType(), falses, nil),
			},
			expected: []bool{false, false, false, true, false, false, false, false},
			nulls:    caseNulls(),
		},
		{
			// An expanded upper bound keeps its own JSON domain: JSON numbers
			// and every category below a JSON string rank below that string,
			// while the JSON literal true ranks above the string.
			name: "row between TRUE and JSON string \"true\"",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(), jsonRows, jsonNulls),
				NewFunctionTestInput(types.T_bool.ToType(), trues, nil),
				NewFunctionTestInput(types.T_json.ToType(),
					repeatString(mustJsonBinaryString(t, `"true"`), rowCount), nil),
			},
			expected: []bool{true, false, false, false, false, true, true, false},
			nulls:    caseNulls(),
		},
		{
			// Reversed orientation: a boolean expression column between two
			// JSON bounds. The JSON literal false/true pair is the complete
			// boolean range.
			name: "TRUE between JSON false and JSON true",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bool.ToType(), trues, nil),
				NewFunctionTestInput(types.T_json.ToType(), jsonFalse, nil),
				NewFunctionTestInput(types.T_json.ToType(), jsonTrue, nil),
			},
			expected: repeatBool(true, rowCount),
			nulls:    nil,
		},
		{
			// A JSON literal null lower bound is a value that ranks below
			// both boolean peers, so every boolean row stays inside the range.
			name: "TRUE between JSON null and JSON true",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bool.ToType(), trues, nil),
				NewFunctionTestInput(types.T_json.ToType(), jsonNullLiteral, nil),
				NewFunctionTestInput(types.T_json.ToType(), jsonTrue, nil),
			},
			expected: repeatBool(true, rowCount),
			nulls:    nil,
		},
		{
			// SQL NULL bounds propagate NULL through the strict operator,
			// exactly like the lowered legs.
			name: "row between NULL and TRUE",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(), jsonRows, jsonNulls),
				NewFunctionTestInput(types.T_bool.ToType(), trues, repeatBool(true, rowCount)),
				NewFunctionTestInput(types.T_bool.ToType(), trues, nil),
			},
			expected: repeatBool(false, rowCount),
			nulls:    repeatBool(true, rowCount),
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			result := runBetweenJSONBooleanCase(t, proc, tc)
			assertBetweenJSONBooleanMatchesLoweredLegs(t, proc, tc, result)
		})
	}

	// The boolean expression column itself alternates so both peer values
	// exercise the reversed JSON/BOOL legs, including a JSON string upper
	// bound that excludes both boolean values on every row.
	boolColumn := make([]bool, rowCount)
	for i := range boolColumn {
		boolColumn[i] = i%2 == 0
	}
	t.Run("BOOL rows between TRUE and JSON string \"true\"", func(t *testing.T) {
		stringUpper := NewFunctionTestInput(types.T_json.ToType(),
			repeatString(mustJsonBinaryString(t, `"true"`), rowCount), nil)
		result := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_bool.ToType(), boolColumn, nil),
				NewFunctionTestInput(types.T_bool.ToType(), trues, nil),
				stringUpper,
			}, types.T_bool.ToType(), betweenImpl, nil)
		values := vector.MustFixedColNoTypeCheck[bool](result)
		require.Equal(t, repeatBool(false, rowCount), values,
			"the JSON string category ranks below both boolean values")
		for row := 0; row < rowCount; row++ {
			require.False(t, result.IsNull(uint64(row)))
		}
	})

	// The MySQL 8.0.44 receipt categories: JSON strings, objects and arrays
	// rank below the boolean peer, so they stay inside a range whose lower
	// bound is a JSON literal null (the lowest category) and whose upper
	// bound is the SQL boolean TRUE. This pins the newly admitted cross shape
	// (JSON expression, JSON lower bound, BOOL upper bound).
	receiptRows := []string{
		mustJsonBinaryString(t, `"x"`),
		mustJsonBinaryString(t, `"true"`),
		mustJsonBinaryString(t, `{"a":1}`),
		mustJsonBinaryString(t, `[1,2]`),
	}
	t.Run("receipt category rows between JSON null and TRUE", func(t *testing.T) {
		result := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(), receiptRows, nil),
				NewFunctionTestInput(types.T_json.ToType(),
					repeatString(mustJsonBinaryString(t, `null`), len(receiptRows)), nil),
				NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, true, true, true}, nil),
			}, types.T_bool.ToType(), betweenImpl, nil)
		values := vector.MustFixedColNoTypeCheck[bool](result)
		require.Equal(t, []bool{true, true, true, true}, values,
			"every string/object/array row ranks between the JSON null lower bound and TRUE")
	})
}

func repeatBool(value bool, count int) []bool {
	rows := make([]bool, count)
	if !value {
		return rows
	}
	for i := range rows {
		rows[i] = true
	}
	return rows
}

func repeatString(value string, count int) []string {
	rows := make([]string, count)
	for i := range rows {
		rows[i] = value
	}
	return rows
}

// TestJSONBooleanBetweenLegacyEraCastRoute pins the DUMP era restoration for
// predecessor persisted BETWEEN checks: the mixed triples rebind through the
// historical cast tail with the implicit CAST(JSON AS BOOL) legs, while the
// cross-shape triples keep the historical rejection (their cast targets
// disagreed), so an archived CHECK constraint authored by the released
// source verifies cell for cell.
func TestJSONBooleanBetweenLegacyEraCastRoute(t *testing.T) {
	jsonTyp := types.T_json.ToType()
	boolTyp := types.T_bool.ToType()
	currentDirect := newCheckResultWithSuccess(0)
	for _, args := range [][]types.Type{
		{jsonTyp, boolTyp, boolTyp},
		{boolTyp, jsonTyp, jsonTyp},
		{boolTyp, boolTyp, jsonTyp},
		{boolTyp, jsonTyp, boolTyp},
	} {
		label := "between " + args[0].Oid.String() + "," + args[1].Oid.String() + "," + args[2].Oid.String()
		legacy, ok := legacyJSONScalarCheck(BETWEEN, nil, args, currentDirect)
		require.True(t, ok, "%s must rebind under the era", label)
		require.Equal(t, succeedWithCast, legacy.status, label)
		require.Equal(t, 0, legacy.idx, label)
		require.Len(t, legacy.finalType, 3, label)
		for _, typ := range legacy.finalType {
			require.Equal(t, types.T_bool, typ.Oid, label)
		}
	}
	for _, args := range [][]types.Type{
		{jsonTyp, boolTyp, jsonTyp},
		{jsonTyp, jsonTyp, boolTyp},
	} {
		label := "between " + args[0].Oid.String() + "," + args[1].Oid.String() + "," + args[2].Oid.String()
		legacy, ok := legacyJSONScalarCheck(BETWEEN, nil, args, currentDirect)
		require.True(t, ok, "%s stays involved in the era", label)
		require.Equal(t, failedFunctionParametersWrong, legacy.status,
			"%s keeps the historical cast-target rejection", label)
	}
	kept, ok := legacyJSONScalarCheck(BETWEEN, nil,
		[]types.Type{boolTyp, boolTyp, boolTyp}, currentDirect)
	require.False(t, ok, "the native BOOL between keeps its current check")
	require.Equal(t, currentDirect, kept)
}

// TestJSONBooleanBetweenMetadataProjections pins the conservative metadata
// consumers for the new uncast BETWEEN shape: a mixed-domain range cannot be
// answered by a bytewise zone-map or index range, and NOT NULL cannot be
// inferred from operand declarations alone, exactly like the mixed binary
// comparisons.
func TestJSONBooleanBetweenMetadataProjections(t *testing.T) {
	jsonTyp := types.T_json.ToType()
	boolTyp := types.T_bool.ToType()
	got, err := GetFunctionByName(context.Background(), "between",
		[]types.Type{jsonTyp, boolTyp, boolTyp})
	require.NoError(t, err)
	overloadID := got.GetEncodedOverloadID()
	mixedArgs := []*plan.Expr{
		{Typ: plan.Type{Id: int32(types.T_json)}},
		{Typ: plan.Type{Id: int32(types.T_bool), NotNullable: true}},
		{Typ: plan.Type{Id: int32(types.T_bool), NotNullable: true}},
	}
	mixedArgs[0].Typ.NotNullable = true
	require.False(t, CanUseZoneMapComparison(overloadID, mixedArgs),
		"the JSON/BOOL ranking has no bytewise zone-map answer")
	eval := GetZoneMapEvaluation(&plan.Function{
		Func: &plan.ObjectRef{Obj: overloadID},
		Args: mixedArgs,
	})
	require.Equal(t, ZoneMapUnsupported, eval)
	require.False(t, deduceNotNullable(context.Background(), overloadID, mixedArgs),
		"the mixed JSON/BOOL BETWEEN must not infer NOT NULL from operand declarations")
	sameArgs := []*plan.Expr{
		{Typ: plan.Type{Id: int32(types.T_bool), NotNullable: true}},
		{Typ: plan.Type{Id: int32(types.T_bool), NotNullable: true}},
		{Typ: plan.Type{Id: int32(types.T_bool), NotNullable: true}},
	}
	gotBool, err := GetFunctionByName(context.Background(), "between",
		[]types.Type{boolTyp, boolTyp, boolTyp})
	require.NoError(t, err)
	require.True(t, deduceNotNullable(context.Background(), gotBool.GetEncodedOverloadID(), sameArgs),
		"the native BOOL BETWEEN keeps its NOT NULL inference")
}
