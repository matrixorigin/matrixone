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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestExactIntegerSlashResolver(t *testing.T) {
	for _, test := range []struct {
		name      string
		left      types.Type
		right     types.Type
		precision int32
	}{
		{name: "tinyint", left: types.T_int8.ToType(), right: types.T_int8.ToType(), precision: 7},
		{name: "bigint", left: types.T_int64.ToType(), right: types.T_int64.ToType(), precision: 23},
		{name: "unsigned bigint", left: types.T_uint64.ToType(), right: types.T_int64.ToType(), precision: 24},
	} {
		t.Run(test.name, func(t *testing.T) {
			resolved, err := GetFunctionByName(
				context.Background(), "/", []types.Type{test.left, test.right})
			require.NoError(t, err)
			require.Equal(t, int32(2), resolved.overloadId)
			targets, needCast := resolved.ShouldDoImplicitTypeCast()
			require.True(t, needCast)
			require.Equal(t, types.T_decimal128, targets[0].Oid)
			require.Equal(t, int32(0), targets[0].Scale)
			require.Equal(t, types.T_decimal128, targets[1].Oid)
			require.Equal(t, types.New(types.T_decimal128, test.precision, 4), resolved.GetReturnType())
		})
	}

	decimal := types.New(types.T_decimal128, 19, 0)
	resolved, err := GetFunctionByName(context.Background(), "/", []types.Type{decimal, decimal})
	require.NoError(t, err)
	require.Equal(t, int32(0), resolved.overloadId, "ordinary decimal division stays on the generic path")
}

func TestExactIntegerSlashExecutionPreservesBigintPrecision(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	inputType := types.New(types.T_decimal128, 19, 0)
	resultType := types.New(types.T_decimal128, 23, 4)
	left := []types.Decimal128{
		{B0_63: 9007199254740993},
		{B0_63: math.MaxInt64},
		{B0_63: 10},
	}
	right := []types.Decimal128{{B0_63: 1}, {B0_63: 1}, {B0_63: 3}}
	want := make([]types.Decimal128, 3)
	for i, value := range []string{
		"9007199254740993.0000",
		"9223372036854775807.0000",
		"3.3333",
	} {
		var err error
		want[i], err = types.ParseDecimal128(value, resultType.Width, resultType.Scale)
		require.NoError(t, err)
	}

	testCase := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(inputType, left, nil),
			NewFunctionTestInput(inputType, right, nil),
		},
		NewFunctionTestResult(resultType, false, want, nil),
		exactIntegerDivFn,
	)
	succeeded, message := testCase.Run()
	require.True(t, succeeded, message)
}

func TestResolveNumericBinaryTypesKeepsIntegerSlashDomain(t *testing.T) {
	resolved, ok := resolveNumericBinaryTypes(
		numericOpDiv, types.T_int64.ToType(), types.T_any.ToType(), nil)
	require.True(t, ok)
	require.Equal(t, types.T_int64, resolved.left.Oid)
	require.Equal(t, types.T_int64, resolved.right.Oid)
	require.Equal(t, types.New(types.T_decimal128, 23, 4), resolved.result)
}
