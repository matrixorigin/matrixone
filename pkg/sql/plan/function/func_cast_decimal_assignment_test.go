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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestSameTypeDecimalAssignmentChecksDeclaredPrecision(t *testing.T) {
	proc := testutil.NewProcess(t)
	typ := types.New(types.T_decimal64, 5, 2)
	source := vector.NewVec(typ)
	defer source.Free(proc.Mp())
	require.NoError(t, vector.AppendFixed(source, types.Decimal64(100000), false, proc.Mp()))
	target := vector.NewConstNull(typ, 1, proc.Mp())
	defer target.Free(proc.Mp())
	result := vector.NewFunctionResultWrapper(typ, proc.Mp())
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(1))
	require.Error(t, NewStrictCast([]*vector.Vector{source, target}, result, proc, 1, nil))
	proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
		if name == "sql_mode" {
			return "STRICT_TRANS_TABLES", nil
		}
		return nil, nil
	})
	require.NoError(t, result.PreExtendAndReset(1))
	require.Error(t, NewAssignCast([]*vector.Vector{source, target}, result, proc, 1, nil))
}

func TestDecimalAssignmentRangeAdjustsOutsideStrictMode(t *testing.T) {
	for _, tc := range []struct {
		name, sqlMode string
		ignore        bool
		wantError     bool
	}{
		{"strict", "STRICT_TRANS_TABLES", false, true},
		{"nonstrict", "", false, false},
		{"ignore", "STRICT_TRANS_TABLES", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
				if name == "sql_mode" {
					return tc.sqlMode, nil
				}
				return nil, nil
			})
			warnings := &numericWarningSession{}
			proc.WarningSink = warnings
			sourceType := types.New(types.T_decimal64, 7, 2)
			targetType := types.New(types.T_decimal64, 5, 2)
			source := vector.NewVec(sourceType)
			defer source.Free(proc.Mp())
			require.NoError(t, vector.AppendFixed(source, types.Decimal64(123456), false, proc.Mp()))
			target := vector.NewConstNull(targetType, 1, proc.Mp())
			defer target.Free(proc.Mp())
			result := vector.NewFunctionResultWrapper(targetType, proc.Mp())
			defer result.Free()
			require.NoError(t, result.PreExtendAndReset(1))
			fn := NewAssignCast
			if tc.ignore {
				fn = NewAssignIgnoreCast
			}
			err := fn([]*vector.Vector{source, target}, result, proc, 1, nil)
			if tc.wantError {
				require.Error(t, err)
				require.Empty(t, warnings.warnings)
				return
			}
			require.NoError(t, err)
			require.Equal(t, types.Decimal64(99999), vector.GetFixedAtNoTypeCheck[types.Decimal64](result.GetResultVector(), 0))
			require.Len(t, warnings.warnings, 1)
		})
	}
}

func TestQuotedDecimalAssignmentRangeAdjustsOutsideStrictMode(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
		if name == "sql_mode" {
			return "", nil
		}
		return nil, nil
	})
	warnings := &numericWarningSession{}
	proc.WarningSink = warnings
	targetType := types.New(types.T_decimal64, 5, 2)
	fc := NewFunctionTestCase(proc, []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"1234.56"}, nil),
		NewFunctionTestInput(targetType, []types.Decimal64{0}, nil),
	}, NewFunctionTestResult(targetType, false, []types.Decimal64{99999}, nil), NewAssignCast)
	ok, info := fc.RunAndFree()
	require.True(t, ok, info)
	require.Len(t, warnings.warnings, 1)
}
