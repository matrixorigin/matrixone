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

// DECIMAL values in a boolean context (AND/OR/NOT/XOR/CASE WHEN/WHERE) are
// implicitly cast to BOOL. MySQL treats any non-zero value as TRUE and keeps
// NULL as NULL.
func TestCastDecimalToBool(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	texts := []string{"0.5", "0", "-2.25", "0", "0.001", "-0.001"}
	nulls := []bool{false, false, false, true, false, false}
	expected := []bool{true, false, true, false, true, true}

	run := func(t *testing.T, oid types.T, width, scale int32, values any) {
		t.Helper()
		from := types.New(oid, width, scale)
		require.True(t, IfTypeCastSupported(oid, types.T_bool))
		_, err := GetFunctionByName(context.Background(), "cast", []types.Type{from, types.T_bool.ToType()})
		require.NoError(t, err)
		inputs := []FunctionTestInput{
			NewFunctionTestInput(from, values, nulls),
			NewFunctionTestInput(types.T_bool.ToType(), []bool{}, []bool{}),
		}
		expect := NewFunctionTestResult(types.T_bool.ToType(), false, expected, nulls)
		tc := NewFunctionTestCase(proc, inputs, expect, NewCast)
		succeed, info := tc.RunAndFree()
		require.True(t, succeed, info)
	}

	t.Run("decimal64", func(t *testing.T) {
		vs := make([]types.Decimal64, len(texts))
		for i, s := range texts {
			v, err := types.ParseDecimal64(s, 10, 3)
			require.NoError(t, err)
			vs[i] = v
		}
		run(t, types.T_decimal64, 10, 3, vs)
	})
	t.Run("decimal128", func(t *testing.T) {
		vs := make([]types.Decimal128, len(texts))
		for i, s := range texts {
			v, err := types.ParseDecimal128(s, 38, 20)
			require.NoError(t, err)
			vs[i] = v
		}
		run(t, types.T_decimal128, 38, 20, vs)
	})
	t.Run("decimal256", func(t *testing.T) {
		vs := make([]types.Decimal256, len(texts))
		for i, s := range texts {
			v, err := types.ParseDecimal256(s, 65, 30)
			require.NoError(t, err)
			vs[i] = v
		}
		run(t, types.T_decimal256, 65, 30, vs)
	})
}
