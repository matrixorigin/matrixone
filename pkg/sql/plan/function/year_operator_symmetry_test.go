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

// TestYearOperatorSymmetry pins issue #31902: YEAR on the right of an
// arithmetic/comparison operator must bind the same way YEAR on the left
// already does (MySQL treats YEAR as a numeric year value in both orders).
// Also YEAR + YEAR must bind (MINUS/MULTI already did), and '/' must bind
// integral/YEAR in both orders via the rule-2 FLOAT64 cast targets, while
// 'div' must bind the same pairs as well: for signed integral x YEAR it
// resolves the exact INT64 coercion targets from integerDivExactTypes (see
// TestYearIntegerDivExact) instead of the rule-2 FLOAT64 corridor.
func TestYearOperatorSymmetry(t *testing.T) {
	ctx := context.Background()
	year := types.T_year.ToType()
	ops := []string{"+", "-", "*", "/", "div", "=", "<>", ">", "<", ">=", "<="}

	// Column INT is T_int32; TINYINT is T_int8. Only T_int64 x YEAR existed.
	for _, num := range []types.Type{
		types.T_int8.ToType(),
		types.T_int16.ToType(),
		types.T_int32.ToType(),
		types.T_int64.ToType(),
		types.T_uint8.ToType(),
		types.T_uint16.ToType(),
		types.T_uint32.ToType(),
		types.T_uint64.ToType(),
	} {
		for _, name := range ops {
			for _, args := range [][]types.Type{{year, num}, {num, year}} {
				_, err := GetFunctionByName(ctx, name, args)
				require.NoError(t, err, "%s %s %s", args[0].Oid, name, args[1].Oid)
			}
		}
	}

	_, err := GetFunctionByName(ctx, "+", []types.Type{year, year})
	require.NoError(t, err, "YEAR + YEAR")
	_, err = GetFunctionByName(ctx, "-", []types.Type{year, year})
	require.NoError(t, err, "YEAR - YEAR")
	_, err = GetFunctionByName(ctx, "div", []types.Type{year, year})
	require.NoError(t, err, "YEAR div YEAR")
}

// Test_PlusFn_Year executes plusFn with T_year operands so the YEAR plus
// path added for issue #31902 is exercised by unit tests (kernel coverage).
func Test_PlusFn_Year(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select year_col1 + year_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_year.ToType(),
					[]types.MoYear{2024, 1900, 0}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_year.ToType(),
					[]types.MoYear{1, 120, 25}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{2025, 2020, 25}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.RunAndFree()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "select year_col1 + year_col2 with NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_year.ToType(),
					[]types.MoYear{2024, 2000}, []bool{false, true}),
				NewFunctionTestInput(types.T_year.ToType(),
					[]types.MoYear{1, 5}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{2025, 0}, []bool{false, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.RunAndFree()
		require.True(t, succeed, tc.info, info)
	}
}

// TestYearIntegerDivExact pins the #31902 follow-up: integral DIV YEAR and
// YEAR DIV YEAR (pairs that did not bind before) must resolve the exact INT64
// coercion targets produced by integerDivExactTypes instead of the rule-2
// FLOAT64 corridor, which rounds BIGINT dividends before division. YEAR DIV
// integral already bound in released versions through the rule-2 FLOAT64
// targets; it must keep exactly those targets so stored generated columns and
// other persisted expressions verify against the released cast tree.
func TestYearIntegerDivExact(t *testing.T) {
	year := types.T_year.ToType()
	exact := [][2]types.Type{
		{types.T_int8.ToType(), year},
		{types.T_int16.ToType(), year},
		{types.T_int32.ToType(), year},
		{types.T_int64.ToType(), year},
		{year, year},
	}
	for _, c := range exact {
		result := legacyIntegerDivCheck([]types.Type{c[0], c[1]})
		require.Equal(t, succeedWithCast, result.status, "%s div %s", c[0].Oid, c[1].Oid)
		require.Len(t, result.finalType, 2)
		require.Equal(t, types.T_int64, result.finalType[0].Oid, "%s div %s", c[0].Oid, c[1].Oid)
		require.Equal(t, types.T_int64, result.finalType[1].Oid, "%s div %s", c[0].Oid, c[1].Oid)
	}

	released := []types.Type{
		types.T_int8.ToType(), types.T_int16.ToType(), types.T_int32.ToType(), types.T_int64.ToType(),
		types.T_uint8.ToType(), types.T_uint16.ToType(), types.T_uint32.ToType(), types.T_uint64.ToType(),
	}
	for _, num := range released {
		result := legacyIntegerDivCheck([]types.Type{year, num})
		require.Equal(t, succeedWithCast, result.status, "T_year div %s", num.Oid)
		require.Len(t, result.finalType, 2)
		require.Equal(t, types.T_float64, result.finalType[0].Oid, "T_year div %s", num.Oid)
		require.Equal(t, types.T_float64, result.finalType[1].Oid, "T_year div %s", num.Oid)
	}

	// Released rule-1 coercion for YEAR <op> BIGINT UNSIGNED stays INT64; the
	// new reverse pair uses the lossless DECIMAL128 domain so u > y does not
	// overflow for u above MaxInt64.
	has, l, r := fixedTypeCastRule1(year, types.T_uint64.ToType())
	require.True(t, has)
	require.Equal(t, types.T_int64, l.Oid)
	require.Equal(t, types.T_int64, r.Oid)
	has, l, r = fixedTypeCastRule1(types.T_uint64.ToType(), year)
	require.True(t, has)
	require.Equal(t, types.T_decimal128, l.Oid)
	require.Equal(t, types.T_decimal128, r.Oid)
}

// Test_IntegerDivFn_YearBoundary pins the INT64-domain division boundary from
// #31902: once INT64 x YEAR DIV binds the exact INT64 coercion targets, the
// YEAR operand is evaluated in the INT64 domain. The FLOAT64 corridor would
// round 9007199254741999 to 9007199254742000 before dividing and report
// 4503599627371, while the exact INT64 division reports 4503599627370.
func Test_IntegerDivFn_YearBoundary(t *testing.T) {
	proc := testutil.NewProcess(t)

	tc := tcTemp{
		info: "select 9007199254741999 div 2000, 1 div 2000, 2024 div null",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{9007199254741999, 1, 2024}, []bool{false, false, false}),
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{2000, 2000, 2000}, []bool{false, false, true}),
		},
		expect: NewFunctionTestResult(types.T_int64.ToType(), false,
			[]int64{4503599627370, 0, 0}, []bool{false, false, true}),
	}
	tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
	succeed, info := tcc.RunAndFree()
	require.True(t, succeed, tc.info, info)
}

// TestYearIntegerDivUnsignedMixed pins the mixed unsigned/YEAR integer DIV
// pair for #31902: unsigned x YEAR keeps the unsigned domain via
// integerDivUnsignedMixedTypes ((UINT64, INT64)) instead of the FLOAT64
// corridor, so 9007199254741999 DIV 2000 stays exact.
func TestYearIntegerDivUnsignedMixed(t *testing.T) {
	year := types.T_year.ToType()
	for _, u := range []types.Type{types.T_uint8.ToType(), types.T_uint32.ToType(), types.T_uint64.ToType()} {
		result := legacyIntegerDivCheck([]types.Type{u, year})
		require.Equal(t, succeedWithCast, result.status, "%s div T_year", u.Oid)
		require.Len(t, result.finalType, 2, "%s div T_year", u.Oid)
		require.Equal(t, types.T_uint64, result.finalType[0].Oid, "%s div T_year", u.Oid)
		require.Equal(t, types.T_int64, result.finalType[1].Oid, "%s div T_year", u.Oid)
	}
}
