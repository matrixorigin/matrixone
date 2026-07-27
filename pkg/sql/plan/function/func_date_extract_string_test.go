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

func TestDateExtractFunctionsAcceptVarchar(t *testing.T) {
	ctx := context.Background()
	for _, name := range []string{"dayofmonth", "dayname", "monthname", "quarter", "weekofyear"} {
		_, err := GetFunctionByName(ctx, name, []types.Type{types.T_varchar.ToType()})
		require.NoErrorf(t, err, "%s should accept VARCHAR", name)
	}

	proc := testutil.NewProcess(t)
	input := NewFunctionTestInput(types.T_varchar.ToType(), []string{"2024-01-15"}, nil)
	for _, tc := range []struct {
		name   string
		fn     fEvalFn
		expect FunctionTestResult
	}{
		{
			name: "dayofmonth",
			fn:   DateStringToDay,
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{15}, nil),
		},
		{
			name: "dayname",
			fn:   DateStringToDayName,
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"Monday"}, nil),
		},
		{
			name: "monthname",
			fn:   DateStringToMonthName,
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"January"}, nil),
		},
		{
			name: "quarter",
			fn:   DateStringToQuarter,
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{1}, nil),
		},
		{
			name: "weekofyear",
			fn:   DateStringToWeekOfYear,
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{3}, nil),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ftc := NewFunctionTestCase(proc, []FunctionTestInput{input}, tc.expect, tc.fn)
			success, info := ftc.Run()
			require.True(t, success, info)
		})
	}

	boundaryInput := NewFunctionTestInput(types.T_varchar.ToType(), []string{"2005-01-01"}, nil)
	boundaryExpect := NewFunctionTestResult(types.T_int64.ToType(), false, []int64{53}, nil)
	boundary := NewFunctionTestCase(proc, []FunctionTestInput{boundaryInput}, boundaryExpect, DateStringToWeekOfYear)
	success, info := boundary.Run()
	require.True(t, success, info)
}

func TestDateExtractStringFunctionsIgnoreAllRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	input := NewFunctionTestInput(types.T_varchar.ToType(),
		[]string{"2024-01-15", "not-a-date"}, nil)

	for _, tc := range []struct {
		name       string
		resultType types.Type
		fn         fEvalFn
	}{
		{
			name:       "dayofmonth",
			resultType: types.T_uint8.ToType(),
			fn:         DateStringToDay,
		},
		{
			name:       "dayname",
			resultType: types.T_varchar.ToType(),
			fn:         DateStringToDayName,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ftc := NewFunctionTestCase(proc, []FunctionTestInput{input},
				NewFunctionTestResult(tc.resultType, false, nil, nil), tc.fn)
			require.NoError(t, ftc.result.PreExtendAndReset(ftc.fnLength))
			require.NoError(t, tc.fn(ftc.parameters, ftc.result, ftc.proc, ftc.fnLength,
				&FunctionSelectList{AllNull: true}))

			result := ftc.result.GetResultVector()
			require.Equal(t, ftc.fnLength, result.Length())
			for i := uint64(0); i < uint64(ftc.fnLength); i++ {
				require.Truef(t, result.GetNulls().Contains(i), "row %d should be NULL", i)
			}
		})
	}
}
