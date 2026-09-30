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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestPreparedBooleanFloatCast(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, target := range []types.T{types.T_float32, types.T_float64} {
		t.Run(target.String(), func(t *testing.T) {
			var empty, want any = []float64{}, []float64{1, 0, 0, 1, 0, 0}
			if target == types.T_float32 {
				empty, want = []float32{}, []float32{1, 0, 0, 1, 0, 0}
			}
			tc := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(types.T_text.ToType(), []string{"true", "false", "true", "1", "0", "bad"}, []bool{false, false, false, false, false, true}),
				NewFunctionTestInput(target.ToType(), empty, nil),
			}, NewFunctionTestResult(target.ToType(), false, want, []bool{false, false, false, false, false, true}), NewCast)
			tc.parameters[0].SetPrepareParamKinds([]vector.PrepareParamKind{
				vector.PrepareParamBoolean, vector.PrepareParamBoolean, vector.PrepareParamNone,
				vector.PrepareParamBoolean, vector.PrepareParamBoolean, vector.PrepareParamBoolean,
			})
			ok, info := tc.Run()
			require.True(t, ok, info)
		})
	}
}

func TestPreparedBooleanFloatCastInactiveRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	input := newVectorByType(proc.Mp(), types.T_text.ToType(), []string{"true", "invalid"}, nil)
	defer input.Free(proc.Mp())
	input.SetPrepareParamKind(vector.PrepareParamBoolean)
	result := vector.NewFunctionResultWrapper(types.T_float64.ToType(), proc.Mp()).(*vector.FunctionResult[float64])
	defer result.Free()
	for _, mode := range []SQLCompatibilityMode{SQLCompatibilityMySQL, SQLCompatibilityMatrixOne} {
		require.NoError(t, result.PreExtendAndReset(2))
		err := strToFloat(context.Background(), mode, vector.GenerateFunctionStrParameter(input), result, 64, 2,
			&FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}})
		require.NoError(t, err)
		require.Equal(t, float64(1), vector.GetFixedAtNoTypeCheck[float64](result.GetResultVector(), 0))
		require.True(t, result.GetResultVector().GetNulls().Contains(1))

		require.NoError(t, result.PreExtendAndReset(2))
		require.NoError(t, strToFloat(context.Background(), mode, vector.GenerateFunctionStrParameter(input), result, 64, 2,
			&FunctionSelectList{AllNull: true}))
		require.True(t, result.GetResultVector().GetNulls().Contains(0))
		require.True(t, result.GetResultVector().GetNulls().Contains(1))

		require.NoError(t, result.PreExtendAndReset(2))
		require.Error(t, strToFloat(context.Background(), mode, vector.GenerateFunctionStrParameter(input), result, 64, 2, nil))
	}
}

func TestMathFunctionsAcceptBoolInNumericContext(t *testing.T) {
	ctx := context.Background()
	boolType := types.T_bool.ToType()
	intType := types.T_int64.ToType()
	floatType := types.T_float64.ToType()

	for _, name := range []string{
		"abs", "sign", "round", "truncate",
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

func TestMathBooleanCeilFloorKeepExistingStringFallback(t *testing.T) {
	for _, name := range []string{"ceil", "floor"} {
		resolved, err := GetFunctionByName(context.Background(), name, []types.Type{types.T_bool.ToType()})
		require.NoError(t, err, name)
		require.True(t, resolved.needCast, name)
		require.Equal(t, types.T_varchar, resolved.targetTypes[0].Oid, name)
		require.Equal(t, types.T_float64, resolved.retType.Oid, name)
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
	for _, tc := range []struct {
		name       string
		targetType types.Type
		target     any
		want       any
	}{
		{name: "int64", targetType: types.T_int64.ToType(), target: []int64{}, want: []int64{1, 0, 1}},
		{name: "float32", targetType: types.T_float32.ToType(), target: []float32{}, want: []float32{1, 0, 1}},
		{name: "float64", targetType: types.T_float64.ToType(), target: []float64{}, want: []float64{1, 0, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			caseTest := NewFunctionTestCase(proc,
				[]FunctionTestInput{
					NewFunctionTestInput(types.T_bool.ToType(), []bool{true, false, true}, []bool{false, true, false}),
					NewFunctionTestInput(tc.targetType, tc.target, nil),
				},
				NewFunctionTestResult(tc.targetType, false, tc.want, []bool{false, true, false}),
				NewCast)

			succeed, info := caseTest.Run()
			require.True(t, succeed, info)
		})
	}
}

func TestMathBoolCastsHonorMaskedRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	input := newVectorByType(proc.Mp(), types.T_bool.ToType(), []bool{true, false}, nil)
	target := newVectorByType(proc.Mp(), types.T_float64.ToType(), []float64{0, 0}, nil)
	result := vector.NewFunctionResultWrapper(types.T_float64.ToType(), proc.Mp())
	defer input.Free(proc.Mp())
	defer target.Free(proc.Mp())
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(2))
	require.NoError(t, NewCast([]*vector.Vector{input, target}, result, proc, 2,
		&FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}}))

	got := result.GetResultVector()
	require.Equal(t, float64(1), vector.GetFixedAtNoTypeCheck[float64](got, 0))
	require.False(t, got.GetNulls().Contains(0))
	require.True(t, got.GetNulls().Contains(1))
}

func TestMathBooleanValuesReachMathExecutors(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := proc.Ctx

	castInput := func(t *testing.T, values []bool, targetType types.Type, targetValues []float64) (*vector.Vector, func()) {
		t.Helper()
		input := newVectorByType(proc.Mp(), types.T_bool.ToType(), values, nil)
		target := newVectorByType(proc.Mp(), targetType, targetValues, nil)
		castResult := vector.NewFunctionResultWrapper(targetType, proc.Mp())
		require.NoError(t, castResult.PreExtendAndReset(len(values)))
		require.NoError(t, NewCast([]*vector.Vector{input, target}, castResult, proc, len(values), nil))
		return castResult.GetResultVector(), func() {
			castResult.Free()
			input.Free(proc.Mp())
			target.Free(proc.Mp())
		}
	}

	sin, err := GetFunctionByName(ctx, "sin", []types.Type{types.T_bool.ToType()})
	require.NoError(t, err)
	sinInput, releaseSinInput := castInput(t, []bool{true, false}, types.T_float64.ToType(), []float64{0, 0})
	defer releaseSinInput()
	sinOutput, err := RunFunctionDirectly(proc, sin.GetEncodedOverloadID(), []*vector.Vector{sinInput}, 2)
	require.NoError(t, err)
	require.Equal(t, []float64{0.8414709848078965, 0}, vector.MustFixedColNoTypeCheck[float64](sinOutput))
	sinOutput.Free(proc.Mp())

	power, err := GetFunctionByName(ctx, "power", []types.Type{types.T_bool.ToType(), types.T_bool.ToType()})
	require.NoError(t, err)
	left, releaseLeft := castInput(t, []bool{true, false}, types.T_float64.ToType(), []float64{0, 0})
	right, releaseRight := castInput(t, []bool{false, true}, types.T_float64.ToType(), []float64{0, 0})
	defer releaseLeft()
	defer releaseRight()
	powerOutput, err := RunFunctionDirectly(proc, power.GetEncodedOverloadID(), []*vector.Vector{left, right}, 2)
	require.NoError(t, err)
	require.Equal(t, []float64{1, 0}, vector.MustFixedColNoTypeCheck[float64](powerOutput))
	powerOutput.Free(proc.Mp())
}

func TestFixedTypeMatchWithBoolNumericCastKeepsNonNumericFallback(t *testing.T) {
	overloads := []overload{{args: []types.T{types.T_varchar}}}
	result := fixedTypeMatchWithBoolNumericCast(overloads, []types.Type{types.T_bool.ToType()})

	require.Equal(t, succeedWithCast, result.status)
	require.Equal(t, types.T_varchar, result.finalType[0].Oid)
}
