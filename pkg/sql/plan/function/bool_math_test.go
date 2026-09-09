// Copyright 2021 - 2022 Matrix Origin
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

func TestMathFunctionsAcceptBoolInNumericContext(t *testing.T) {
	ctx := context.Background()
	boolType := types.T_bool.ToType()
	intType := types.T_int64.ToType()
	floatType := types.T_float64.ToType()

	for _, name := range []string{
		"abs", "sign", "ceil", "floor", "round", "truncate",
	} {
		resolved, err := GetFunctionByName(ctx, name, []types.Type{boolType})
		require.NoError(t, err, name)
		require.True(t, resolved.needCast, name)
		require.Equal(t, intType.Oid, resolved.targetTypes[0].Oid, name)
	}

	for _, name := range []string{
		"sqrt", "acos", "asin", "atan", "degrees", "radians", "cos",
		"cot", "exp", "ln", "log", "log2", "log10", "sin", "sinh", "tan",
	} {
		resolved, err := GetFunctionByName(ctx, name, []types.Type{boolType})
		require.NoError(t, err, name)
		require.True(t, resolved.needCast, name)
		require.Equal(t, floatType.Oid, resolved.targetTypes[0].Oid, name)
	}

	for _, tc := range []struct {
		name string
		args []types.Type
		want []types.T
	}{
		{name: "atan", args: []types.Type{boolType, boolType}, want: []types.T{types.T_float64, types.T_float64}},
		{name: "atan2", args: []types.Type{boolType, boolType}, want: []types.T{types.T_float64, types.T_float64}},
		{name: "log", args: []types.Type{boolType, boolType}, want: []types.T{types.T_float64, types.T_float64}},
		{name: "power", args: []types.Type{boolType, boolType}, want: []types.T{types.T_float64, types.T_float64}},
		{name: "ceil", args: []types.Type{boolType, boolType}, want: []types.T{types.T_int64, types.T_int64}},
		{name: "floor", args: []types.Type{boolType, boolType}, want: []types.T{types.T_int64, types.T_int64}},
		{name: "round", args: []types.Type{boolType, boolType}, want: []types.T{types.T_int64, types.T_int64}},
		{name: "truncate", args: []types.Type{boolType, boolType}, want: []types.T{types.T_int64, types.T_int64}},
	} {
		resolved, err := GetFunctionByName(ctx, tc.name, tc.args)
		require.NoError(t, err, tc.name)
		require.True(t, resolved.needCast, tc.name)
		require.Len(t, resolved.targetTypes, len(tc.want), tc.name)
		for i, want := range tc.want {
			require.Equal(t, want, resolved.targetTypes[i].Oid, tc.name)
		}
	}
}

func TestMathPreparedMarkersKeepNumericTargetsForBooleanValues(t *testing.T) {
	ctx := context.Background()
	floatType := types.T_float64.ToType()
	anyType := types.T_any.ToType()
	boolType := types.T_bool.ToType()

	for _, tc := range []struct {
		name     string
		prepared []types.Type
		bound    []types.Type
		want     []types.T
	}{
		{name: "sin", prepared: []types.Type{anyType}, bound: []types.Type{boolType}, want: []types.T{types.T_float64}},
		{name: "power", prepared: []types.Type{anyType, anyType}, bound: []types.Type{boolType, boolType}, want: []types.T{types.T_float64, types.T_float64}},
		{name: "round", prepared: []types.Type{anyType, anyType}, bound: []types.Type{boolType, boolType}, want: []types.T{types.T_uint64, types.T_int64}},
	} {
		prepared, err := GetFunctionByName(ctx, tc.name, tc.prepared)
		require.NoError(t, err, tc.name)
		require.True(t, prepared.needCast, tc.name)
		require.Len(t, prepared.targetTypes, len(tc.want), tc.name)
		for i, want := range tc.want {
			require.Equal(t, want, prepared.targetTypes[i].Oid, tc.name)
		}

		bound, err := GetFunctionByName(ctx, tc.name, tc.bound)
		require.NoError(t, err, tc.name)
		require.True(t, bound.needCast, tc.name)
		if tc.name == "sin" {
			require.Equal(t, floatType.Oid, prepared.targetTypes[0].Oid, tc.name)
			require.Equal(t, bound.targetTypes[0].Oid, prepared.targetTypes[0].Oid, tc.name)
		}
	}
}

func TestMathBoolCastTargetsPreserveValuesAndNulls(t *testing.T) {
	proc := testutil.NewProcess(t)
	caseTest := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_bool.ToType(), []bool{true, false, true}, []bool{false, true, false}),
			NewFunctionTestInput(types.T_float64.ToType(), []float64{}, nil),
		},
		NewFunctionTestResult(types.T_float64.ToType(), false, []float64{1, 0, 1}, []bool{false, true, false}),
		NewCast)

	succeed, info := caseTest.Run()
	require.True(t, succeed, info)
}

func TestFixedTypeMatchWithBoolNumericCastKeepsNonNumericFallback(t *testing.T) {
	overloads := []overload{{args: []types.T{types.T_varchar}}}
	result := fixedTypeMatchWithBoolNumericCast(overloads, []types.Type{types.T_bool.ToType()})

	require.Equal(t, succeedWithCast, result.status)
	require.Equal(t, types.T_varchar, result.finalType[0].Oid)
}
