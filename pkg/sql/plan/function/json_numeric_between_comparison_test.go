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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// TestJSONNumericBetweenOverloadIdentity pins the plan-level contract of the
// #31121 review round 3 fix: the mixed JSON/numeric BETWEEN binds the direct
// comparison identity for every admitted triple and numeric peer family,
// exactly like the binary ordering operators the NOT BETWEEN lowering
// produces. The historical check folded the JSON operand through the
// comparison cast rule (CAST(JSON AS BIGINT) for integer peers), so a JSON
// literal null became SQL NULL and a JSON string was parsed as numeric text
// in the positive range while the lowered negated range ranked both by the
// JSON scalar category: the two forms could select different non-null rows.
func TestJSONNumericBetweenOverloadIdentity(t *testing.T) {
	jsonTyp := types.T_json.ToType()
	shapes := [][]types.Type{
		{types.T_int64.ToType(), types.T_int64.ToType(), jsonTyp},
		{types.T_int64.ToType(), jsonTyp, types.T_int64.ToType()},
		{jsonTyp, types.T_int64.ToType(), types.T_int64.ToType()},
		{jsonTyp, jsonTyp, types.T_int64.ToType()},
		{jsonTyp, types.T_int64.ToType(), jsonTyp},
		{types.T_uint32.ToType(), jsonTyp, types.T_float64.ToType()},
		{types.T_int8.ToType(), jsonTyp, types.T_decimal64.ToType()},
		{jsonTyp, types.New(types.T_decimal128, types.T_decimal128.ToType().Width, 3), types.T_float32.ToType()},
	}
	for _, args := range shapes {
		got, err := GetFunctionByName(context.Background(), "between", args)
		require.NoError(t, err, "mixed JSON/numeric BETWEEN triple %v must resolve", args)
		_, overloadIndex := DecodeOverloadID(got.GetEncodedOverloadID())
		require.Equal(t, int32(0), overloadIndex, "direct identity overload")
		_, shouldCast := got.ShouldDoImplicitTypeCast()
		require.False(t, shouldCast,
			"mixed JSON/numeric BETWEEN triple %v must not fold through the SQL numeric cast", args)
	}
	// Homogeneous and out-of-domain shapes keep their historical domains: an
	// all-numeric triple keeps the fixed numeric alignment, a character bound
	// keeps the ordering rejection, and a BOOL/numeric mixture keeps the
	// historical cast-target disagreement.
	numeric, err := GetFunctionByName(context.Background(), "between",
		[]types.Type{types.T_int64.ToType(), types.T_int64.ToType(), types.T_int64.ToType()})
	require.NoError(t, err)
	_, shouldCast := numeric.ShouldDoImplicitTypeCast()
	require.False(t, shouldCast, "the all-numeric BETWEEN keeps its aligned direct overload")
	_, err = GetFunctionByName(context.Background(), "between",
		[]types.Type{jsonTyp, types.T_varchar.ToType(), types.T_varchar.ToType()})
	require.Error(t, err, "JSON BETWEEN with SQL string bounds keeps the ordering rejection")
	_, err = GetFunctionByName(context.Background(), "between",
		[]types.Type{jsonTyp, types.T_bool.ToType(), types.T_int64.ToType()})
	require.Error(t, err,
		"the cross JSON/BOOL/numeric triple keeps its historical cast-target rejection")
}

// betweenJSONNumericCase is one executor contract case: every operand is a
// full-length column (bounds repeat their constant or per-row values), so
// each row exercises its own leg pair.
type betweenJSONNumericCase struct {
	name     string
	inputs   []FunctionTestInput
	expected []bool
	nulls    []bool
}

func runBetweenJSONNumericCase(t *testing.T, proc *process.Process, tc betweenJSONNumericCase) *vector.Vector {
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

// assertBetweenJSONNumericMatchesLoweredLegs proves the invariant of the fix
// for legs whose binaries pair naturally: the positive form equals the tri-state
// SQL AND of its lowered legs `lo <= x AND x <= hi`, and the negated form
// equals the tri-state complement `x < lo OR x > hi`, on every row including
// NULL rows (SQL NULL operands leave their leg unknown; a definitive leg
// decides the row). JSON, int64 and float64 operands pair through the raw
// kernels, with an int64 column beside a float64 operand widened to its
// DOUBLE adapter column first - the coercion the ordinary comparison binds
// for that pair. The heterogeneous decimal or unsigned peer-width cases pin
// their row values directly instead, because the raw binary kernel pair
// they would need also carries the binder's cast alignment.
func assertBetweenJSONNumericMatchesLoweredLegs(
	t *testing.T, proc *process.Process, tc betweenJSONNumericCase, between *vector.Vector,
) {
	t.Helper()
	for _, input := range tc.inputs {
		if oid := input.typ.Oid; oid != types.T_json && oid != types.T_int64 && oid != types.T_float64 {
			return
		}
	}
	// widenBesideFloat mirrors the ordinary comparison's DOUBLE coercion: an
	// int64 leg operand paired with a float64 operand compares as its
	// float64 adapter, so a BIGINT subject at 2^53+1 equals the DOUBLE 2^53
	// bound in both spellings.
	widenBesideFloat := func(candidate, other FunctionTestInput) FunctionTestInput {
		if candidate.typ.Oid != types.T_int64 || other.typ.Oid != types.T_float64 {
			return candidate
		}
		rows, _ := candidate.values.([]int64)
		widened := make([]float64, len(rows))
		for i, value := range rows {
			widened[i] = float64(value)
		}
		return NewFunctionTestInput(types.T_float64.ToType(), widened, candidate.nullList)
	}
	x, lo, hi := tc.inputs[0], tc.inputs[1], tc.inputs[2]
	lowerLeft, lowerRight := widenBesideFloat(lo, x), widenBesideFloat(x, lo)
	upperLeft, upperRight := widenBesideFloat(x, hi), widenBesideFloat(hi, x)
	lower := runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{lowerLeft, lowerRight}, types.T_bool.ToType(), lessEqualFn, nil)
	lowerValues := vector.MustFixedColNoTypeCheck[bool](lower)
	upper := runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{upperLeft, upperRight}, types.T_bool.ToType(), lessEqualFn, nil)
	upperValues := vector.MustFixedColNoTypeCheck[bool](upper)
	// The NOT BETWEEN rewrite binds `x < lo OR x > hi`, so the negated legs
	// take the expression column as their left operand.
	negLower := runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{lowerRight, lowerLeft}, types.T_bool.ToType(), lessThanFn, nil)
	negLowerValues := vector.MustFixedColNoTypeCheck[bool](negLower)
	negUpper := runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{upperLeft, upperRight}, types.T_bool.ToType(), greatThanFn, nil)
	negUpperValues := vector.MustFixedColNoTypeCheck[bool](negUpper)
	betweenValues := vector.MustFixedColNoTypeCheck[bool](between)
	for row := range tc.expected {
		// Tri-state equality against the lowered SQL AND: a definitive leg
		// decides the row even when the other leg is unknown, exactly what
		// the BETWEEN executor must reproduce. This covers NULL rows too,
		// not only non-null answers.
		lowerUnknown := lower.IsNull(uint64(row))
		upperUnknown := upper.IsNull(uint64(row))
		switch {
		case !lowerUnknown && !lowerValues[row], !upperUnknown && !upperValues[row]:
			require.False(t, between.IsNull(uint64(row)),
				"case %s row %d: a definitive false leg must answer the row FALSE", tc.name, row)
			require.False(t, betweenValues[row],
				"case %s row %d: BETWEEN must equal its lowered AND legs", tc.name, row)
		case lowerUnknown || upperUnknown:
			require.True(t, between.IsNull(uint64(row)),
				"case %s row %d: undecided legs must leave the row NULL", tc.name, row)
		default:
			require.False(t, between.IsNull(uint64(row)), "case %s row %d", tc.name, row)
			require.True(t, betweenValues[row],
				"case %s row %d: BETWEEN must equal its lowered AND legs", tc.name, row)
		}
		// The tri-state complement through the NOT BETWEEN lowering: a true
		// negated leg decides the row, an unknown otherwise unknown leg
		// keeps it NULL.
		negLowerUnknown := negLower.IsNull(uint64(row))
		negUpperUnknown := negUpper.IsNull(uint64(row))
		switch {
		case !negLowerUnknown && negLowerValues[row], !negUpperUnknown && negUpperValues[row]:
			require.False(t, between.IsNull(uint64(row)), "case %s row %d", tc.name, row)
			require.False(t, betweenValues[row],
				"case %s row %d: NOT BETWEEN must complement BETWEEN", tc.name, row)
		case negLowerUnknown || negUpperUnknown:
			require.True(t, between.IsNull(uint64(row)),
				"case %s row %d: an unknown negated leg keeps the row NULL", tc.name, row)
		default:
			require.False(t, between.IsNull(uint64(row)), "case %s row %d", tc.name, row)
			require.True(t, betweenValues[row],
				"case %s row %d: NOT BETWEEN must complement BETWEEN", tc.name, row)
		}
	}
}

// TestJSONNumericBetweenExecutorContract runs betweenImpl over the mixed
// JSON/numeric scalar domain with the shared scalar rows
// (3, "str", true, false, JSON literal null, -1, 0.5, SQL NULL). Expected
// values follow the ranking the binary ordering kernels already carry:
// JSON numbers compare numerically with the numeric peer, a JSON literal null
// type-ranks below the numeric peer (the review's coherence row: it must
// answer both range forms from the same domain), and strings, booleans,
// objects and arrays rank above numbers.
func TestJSONNumericBetweenExecutorContract(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })

	jsonRows := jsonScalarForms(t)
	jsonNulls := jsonFormsNulls(jsonRows)
	rowCount := len(jsonRows)
	// row order: 3, "str", true, false, null, -1, 0.5, SQL NULL
	lowerZero := repeatInt64(0, rowCount)
	upperThree := repeatInt64(3, rowCount)
	upperOne := repeatInt64(1, rowCount)

	caseNulls := func() []bool {
		nulls := make([]bool, rowCount)
		nulls[rowCount-1] = true
		return nulls
	}

	cases := []betweenJSONNumericCase{
		{
			name: "row between 0 and 3",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(), jsonRows, jsonNulls),
				NewFunctionTestInput(types.T_int64.ToType(), lowerZero, nil),
				NewFunctionTestInput(types.T_int64.ToType(), upperThree, nil),
			},
			// JSON numbers stay numerically inside the inclusive range; every
			// other category sits outside.
			expected: []bool{true, false, false, false, false, false, true, false},
			nulls:    caseNulls(),
		},
		{
			// The reviewer's exact range: a JSON literal null is a value
			// ranking below the 0 lower bound, so the plain and negated forms
			// now answer from the same domain instead of SQL NULL vs true.
			name: "row between 0 and 1",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(), jsonRows, jsonNulls),
				NewFunctionTestInput(types.T_int64.ToType(), lowerZero, nil),
				NewFunctionTestInput(types.T_int64.ToType(), upperOne, nil),
			},
			expected: []bool{false, false, false, false, false, false, true, false},
			nulls:    caseNulls(),
		},
		{
			name: "row between 3 and 0 (inverted range)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(), jsonRows, jsonNulls),
				NewFunctionTestInput(types.T_int64.ToType(), upperThree, nil),
				NewFunctionTestInput(types.T_int64.ToType(), lowerZero, nil),
			},
			expected: repeatBool(false, rowCount),
			nulls:    caseNulls(),
		},
		{
			// A JSON lower bound beside the int64 upper bound keeps each leg
			// in its own domain: the JSON/JSON leg ranks by the trusted byte
			// ordering while the JSON/numeric leg ranks the numeric peer.
			name: "row between JSON 0 and 3",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(), jsonRows, jsonNulls),
				NewFunctionTestInput(types.T_json.ToType(),
					repeatString(mustJsonBinaryString(t, `0`), rowCount), nil),
				NewFunctionTestInput(types.T_int64.ToType(), upperThree, nil),
			},
			expected: []bool{true, false, false, false, false, false, true, false},
			nulls:    caseNulls(),
		},
		{
			// A SQL NULL lower bound leaves its leg unknown while the other
			// leg decides, exactly like the lowered AND: the "str" and the
			// JSON boolean rows rank above the 3 upper bound, a definitive
			// FALSE, so those rows answer FALSE instead of NULL, while the
			// number rows whose upper leg can still hold stay NULL.
			name: "row between NULL and 3",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(), jsonRows, jsonNulls),
				NewFunctionTestInput(types.T_int64.ToType(), upperThree, repeatBool(true, rowCount)),
				NewFunctionTestInput(types.T_int64.ToType(), upperThree, nil),
			},
			expected: repeatBool(false, rowCount),
			nulls:    []bool{true, false, false, false, true, true, true, true},
		},
		{
			// Reversed orientation: an int64 expression column between the
			// shared JSON rows and a 5 upper bound. The JSON literal null row
			// ranks below the numeric expression, so it stays inside the
			// range from the same domain the lowered negative form uses.
			name: "int64 rows between JSON bounds and 5",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{3, 5, 0, 1, 2, 2, 4, 0}, nil),
				NewFunctionTestInput(types.T_json.ToType(), jsonRows, jsonNulls),
				NewFunctionTestInput(types.T_int64.ToType(), repeatInt64(5, rowCount), nil),
			},
			expected: []bool{true, false, false, false, true, true, true, false},
			nulls:    caseNulls(),
		},
		{
			// Heterogeneous numeric peers: the numeric/numeric leg (float32
			// against int64) compares through the same exact numeric taxonomy
			// the JSON ranking domain already uses.
			name: "float32 rows between JSON bounds and 5",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{3.0, 1.0, 2.0, 2.0, 4.0, 4.0, 4.0, 0.0}, nil),
				NewFunctionTestInput(types.T_json.ToType(), jsonRows, jsonNulls),
				NewFunctionTestInput(types.T_int64.ToType(), repeatInt64(5, rowCount), nil),
			},
			expected: []bool{true, false, false, false, true, true, true, false},
			nulls:    caseNulls(),
		},
		{
			// Decimal peers keep their parsed exact domain: a decimal64 range
			// at scale 2 admits the -1 and 0.5 JSON numbers and rejects every
			// other category, including the JSON literal null row.
			name: "row between decimal64 bounds",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(), jsonRows, jsonNulls),
				NewFunctionTestInput(types.New(types.T_decimal64, 10, 2), decimal64ScaleTwo(t, rowCount, -150), nil),
				NewFunctionTestInput(types.New(types.T_decimal64, 10, 2), decimal64ScaleTwo(t, rowCount, 250), nil),
			},
			expected: []bool{false, false, false, false, false, true, true, false},
			nulls:    caseNulls(),
		},
		{
			// SQL NULL upper bounds keep the SQL AND tri-state (the review
			// round 5 prepared-range fix): row 0's lower leg json(1) <= 0 is
			// a decisive FALSE, so the row answers FALSE instead of NULL;
			// row 3 keeps both legs undecided enough to stay NULL.
			name: "int64 rows between JSON 1 and SQL NULL upper bounds",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(), []int64{0, 5, 3, 2}, nil),
				NewFunctionTestInput(types.T_json.ToType(),
					repeatString(mustJsonBinaryString(t, `1`), 4), nil),
				NewFunctionTestInput(types.T_int64.ToType(), repeatInt64(9, 4),
					[]bool{true, false, false, true}),
			},
			expected: []bool{false, true, true, false},
			nulls:    []bool{false, false, false, true},
		},
		{
			// The mirrored tri-state through the lower bound: row 1's upper
			// leg 5 <= json(3) is a decisive FALSE beside an unknown lower
			// leg, so the row answers FALSE instead of NULL.
			name: "int64 rows between SQL NULL and JSON lower bounds",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(), []int64{0, 5, 1, 7}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), repeatInt64(1, 4),
					[]bool{true, true, false, false}),
				NewFunctionTestInput(types.T_json.ToType(),
					repeatString(mustJsonBinaryString(t, `3`), 4), nil),
			},
			expected: []bool{false, false, true, false},
			nulls:    []bool{true, false, false, false},
		},
		{
			// The precision boundary of the DOUBLE coercion (the review
			// round 5 prepared-range fix): the numeric/numeric leg compares
			// in the DOUBLE domain exactly like the ordinary BIGINT/DOUBLE
			// comparison it mirrors, so the BIGINT 2^53+1 subject rounds
			// into the DOUBLE 2^53 upper bound and stays in range, while
			// 2^53+3 rounds away. bytejson's exact taxonomy would have
			// excluded the 2^53+1 row and disagreed with the lowered
			// `x <= hi` leg.
			name: "BIGINT rows keep the DOUBLE coercion beside a float64 upper bound",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{9007199254740992, 9007199254740993, 9007199254740991, 9007199254740995}, nil),
				NewFunctionTestInput(types.T_json.ToType(),
					repeatString(mustJsonBinaryString(t, `0`), 4), nil),
				NewFunctionTestInput(types.T_float64.ToType(),
					repeatFloat64(9007199254740992, 4), nil),
			},
			expected: []bool{true, true, true, false},
			nulls:    nil,
		},
		{
			// The DOUBLE coercion also holds for the unsigned peer: the
			// uint64 lower bound 2^63+1 rounds down to the DOUBLE 2^63, so the
			// 2^63 float64 subject passes the lower leg under the ordinary
			// comparison's coercion while bytejson's exact taxonomy would
			// exclude it and disagree with the lowered `lo <= x` leg.
			name: "uint64 lower bound keeps the DOUBLE coercion beside a float64 subject",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{9223372036854775808.0, 2.0}, nil),
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{9223372036854775809, 9223372036854775809}, nil),
				NewFunctionTestInput(types.T_json.ToType(),
					repeatString(mustJsonBinaryString(t, `1e30`), 2), nil),
			},
			expected: []bool{true, false},
			nulls:    nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := runBetweenJSONNumericCase(t, proc, tc)
			assertBetweenJSONNumericMatchesLoweredLegs(t, proc, tc, result)
		})
	}

	// The unsigned peer domain: a uint64 lower bound at its maximum carries
	// the whole exact unsigned domain against an int64 expression, so no
	// representable row can satisfy its lower leg - exactly like the lowered
	// `x < lo` leg both forms share. The decisive false lower leg also
	// answers the SQL NULL JSON row FALSE instead of NULL under the SQL AND
	// tri-state (the review round 5 fix).
	t.Run("int64 rows cannot pass a uint64 maximum lower bound", func(t *testing.T) {
		result := runJsonFunctionWithSelectList(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{3, 3, 3, 3, 3, 3, 3, 3}, nil),
				NewFunctionTestInput(types.T_uint64.ToType(),
					repeatUint64(18446744073709551615, rowCount), nil),
				NewFunctionTestInput(types.T_json.ToType(), jsonRows, jsonNulls),
			}, types.T_bool.ToType(), betweenImpl, nil)
		values := vector.MustFixedColNoTypeCheck[bool](result)
		require.Equal(t, repeatBool(false, rowCount), values, "the uint64 lower bound excludes every int64 row")
		for row := 0; row < rowCount; row++ {
			require.False(t, result.IsNull(uint64(row)),
				"row %d: the decisive lower leg answers even beside the SQL NULL JSON row", row)
		}
	})
}

// TestJSONNumericBetweenLegacyEraFold pins the DUMP era restoration for the
// mixed JSON/numeric range: a JSON literal null folds back into the SQL NULL
// contract in the era (compareJSONNumeric's legacy out), so an archived
// predecessor CHECK expression keeps its historical NULL answer row for row
// while every other kind keeps the era-independent ranking around it.
func TestJSONNumericBetweenLegacyEraFold(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })
	proc.Ctx = WithLegacyJSONScalarSemantics(proc.Ctx)

	jsonRows := jsonScalarForms(t)
	jsonNulls := jsonFormsNulls(jsonRows)
	rowCount := len(jsonRows)
	result := runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_json.ToType(), jsonRows, jsonNulls),
			NewFunctionTestInput(types.T_int64.ToType(), repeatInt64(0, rowCount), nil),
			NewFunctionTestInput(types.T_int64.ToType(), repeatInt64(3, rowCount), nil),
		}, types.T_bool.ToType(), betweenImpl, nil)
	// row order: 3, "str", true, false, null, -1, 0.5, SQL NULL
	values := vector.MustFixedColNoTypeCheck[bool](result)
	require.Equal(t, []bool{true, false, false, false}, values[:4],
		"every non-null kind above the range keeps the ranked answer")
	require.True(t, result.IsNull(uint64(4)),
		"the era folds the JSON literal null row into the SQL NULL contract")
	require.Equal(t, []bool{false, true, false}, values[5:8],
		"-1 and 0.5 keep their numeric answers and the SQL NULL row stays NULL")
	require.True(t, result.IsNull(uint64(7)))
}

// TestJSONNumericBetweenLegacyEraCastRoute pins the DUMP era rebind for
// predecessor persisted mixed JSON/numeric BETWEEN expressions. The
// historical cast table only casts a JSON operand that sits on the left of
// its pair: the forward mixed triples rebind through the historical cast
// tail with the implicit CAST(JSON AS BIGINT) legs, while the reversed
// triple kept the historical parameters-wrong rejection (the binder-era
// column-bound form went through the comparison-legs reconstruction, so no
// persisted reverse-shaped ternary CHECK exists to rebind). Either way an
// archived CHECK verifies cell for cell.
func TestJSONNumericBetweenLegacyEraCastRoute(t *testing.T) {
	jsonTyp := types.T_json.ToType()
	intTyp := types.T_int64.ToType()
	currentDirect := newCheckResultWithSuccess(0)
	for _, args := range [][]types.Type{
		{jsonTyp, intTyp, intTyp},
	} {
		label := "between " + args[0].Oid.String() + "," + args[1].Oid.String() + "," + args[2].Oid.String()
		legacy, ok := legacyJSONScalarCheck(BETWEEN, nil, args, currentDirect)
		require.True(t, ok, "%s must rebind under the era", label)
		require.Equal(t, succeedWithCast, legacy.status, label)
		require.Equal(t, 0, legacy.idx, label)
		require.Len(t, legacy.finalType, 3, label)
		for _, typ := range legacy.finalType {
			require.Equal(t, types.T_int64, typ.Oid, label)
		}
	}
	for _, args := range [][]types.Type{
		{intTyp, jsonTyp, intTyp},
		{jsonTyp, intTyp, jsonTyp},
	} {
		label := "between " + args[0].Oid.String() + "," + args[1].Oid.String() + "," + args[2].Oid.String()
		legacy, ok := legacyJSONScalarCheck(BETWEEN, nil, args, currentDirect)
		require.True(t, ok, "%s stays involved in the era", label)
		require.Equal(t, failedFunctionParametersWrong, legacy.status,
			"%s keeps the historical rejection", label)
	}
	kept, ok := legacyJSONScalarCheck(BETWEEN, nil,
		[]types.Type{intTyp, intTyp, intTyp}, currentDirect)
	require.False(t, ok, "the native numeric between keeps its current check")
	require.Equal(t, currentDirect, kept)
}

// TestJSONNumericBetweenMetadataProjections pins the conservative metadata
// consumers for the new uncast BETWEEN shape: a mixed JSON/numeric range
// cannot be answered by a bytewise zone-map or index range, and NOT NULL
// cannot be inferred from operand declarations alone, exactly like the mixed
// binary comparisons.
func TestJSONNumericBetweenMetadataProjections(t *testing.T) {
	jsonTyp := types.T_json.ToType()
	got, err := GetFunctionByName(context.Background(), "between",
		[]types.Type{jsonTyp, types.T_int64.ToType(), types.T_int64.ToType()})
	require.NoError(t, err)
	overloadID := got.GetEncodedOverloadID()
	mixedArgs := []*plan.Expr{
		{Typ: plan.Type{Id: int32(types.T_json), NotNullable: true}},
		{Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true}},
		{Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true}},
	}
	require.False(t, CanUseZoneMapComparison(overloadID, mixedArgs),
		"the JSON/numeric ranking has no bytewise zone-map answer")
	eval := GetZoneMapEvaluation(&plan.Function{
		Func: &plan.ObjectRef{Obj: overloadID},
		Args: mixedArgs,
	})
	require.Equal(t, ZoneMapUnsupported, eval)
	require.False(t, deduceNotNullable(context.Background(), overloadID, mixedArgs),
		"the mixed JSON/numeric BETWEEN must not infer NOT NULL from operand declarations")
	numeric, err := GetFunctionByName(context.Background(), "between",
		[]types.Type{types.T_int64.ToType(), types.T_int64.ToType(), types.T_int64.ToType()})
	require.NoError(t, err)
	sameArgs := []*plan.Expr{
		{Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true}},
		{Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true}},
		{Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true}},
	}
	require.True(t, deduceNotNullable(context.Background(), numeric.GetEncodedOverloadID(), sameArgs),
		"the native numeric BETWEEN keeps its NOT NULL inference")
}

func repeatInt64(value int64, count int) []int64 {
	rows := make([]int64, count)
	for i := range rows {
		rows[i] = value
	}
	return rows
}

func repeatFloat64(value float64, count int) []float64 {
	rows := make([]float64, count)
	for i := range rows {
		rows[i] = value
	}
	return rows
}

func repeatUint64(value uint64, count int) []uint64 {
	rows := make([]uint64, count)
	for i := range rows {
		rows[i] = value
	}
	return rows
}

// decimal64ScaleTwo repeats one exact decimal64 value at scale 2, built from
// its integral coefficient (e.g. -150 encodes -1.50).
func decimal64ScaleTwo(t *testing.T, count int, coefficient int64) []types.Decimal64 {
	t.Helper()
	scaled := float64(coefficient) / 100
	value, err := types.Decimal64FromFloat64(scaled, 10, 2)
	require.NoError(t, err)
	rows := make([]types.Decimal64, count)
	for i := range rows {
		rows[i] = value
	}
	return rows
}
