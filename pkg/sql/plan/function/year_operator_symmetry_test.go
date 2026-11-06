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
// Also YEAR + YEAR must bind (MINUS/MULTI already did).
func TestYearOperatorSymmetry(t *testing.T) {
	ctx := context.Background()
	year := types.T_year.ToType()
	ops := []string{"+", "-", "*", "=", "<>", ">", "<", ">=", "<="}

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
