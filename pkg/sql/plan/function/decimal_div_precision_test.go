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
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestDecimalDivisionTypeUsesPrecisionIncrement(t *testing.T) {
	decimal10Scale2 := types.New(types.T_decimal64, 10, 2)
	decimal50Scale10 := types.New(types.T_decimal256, 50, 10)
	integer := types.T_int64.ToType()

	tests := []struct {
		name      string
		increment int32
		inputs    []types.Type
		want      types.Type
		wantCasts []types.Type
	}{
		{
			name:      "zero increment",
			increment: 0,
			inputs:    []types.Type{decimal10Scale2, decimal10Scale2},
			want:      types.New(types.T_decimal128, 12, 2),
		},
		{
			name:      "default increment",
			increment: 4,
			inputs:    []types.Type{decimal10Scale2, decimal10Scale2},
			want:      types.New(types.T_decimal128, 16, 6),
		},
		{
			name:      "larger increment",
			increment: 10,
			inputs:    []types.Type{decimal10Scale2, decimal10Scale2},
			want:      types.New(types.T_decimal128, 22, 12),
		},
		{
			name:      "precision promotes physical decimal",
			increment: 30,
			inputs:    []types.Type{decimal10Scale2, decimal10Scale2},
			want:      types.New(types.T_decimal256, 42, 30),
			wantCasts: []types.Type{
				types.New(types.T_decimal256, 10, 2),
				types.New(types.T_decimal256, 10, 2),
			},
		},
		{
			name:      "decimal256 precision and scale caps",
			increment: 30,
			inputs:    []types.Type{decimal50Scale10, integer},
			want:      types.New(types.T_decimal256, 65, 30),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := WithDivPrecisionIncrement(context.Background(), test.increment)
			resolved, err := GetFunctionByName(ctx, "/", test.inputs)
			require.NoError(t, err)
			require.Equal(t, test.want, resolved.GetReturnType())
			casts, shouldCast := resolved.ShouldDoImplicitTypeCast()
			if test.wantCasts != nil {
				require.True(t, shouldCast)
				require.Equal(t, test.wantCasts, casts)
			}
		})
	}

	resolved, err := GetFunctionByName(
		WithDivPrecisionIncrement(context.Background(), 30),
		"/",
		[]types.Type{types.T_float64.ToType(), types.T_float64.ToType()},
	)
	require.NoError(t, err)
	require.Equal(t, types.T_float64.ToType(), resolved.GetReturnType())
}

func TestDecimalDivisionExecutionUsesBoundResultScale(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputType := types.New(types.T_decimal64, 10, 2)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(inputType, []types.Decimal64{100}, nil),
		NewFunctionTestInput(inputType, []types.Decimal64{300}, nil),
	}

	for _, test := range []struct {
		name string
		typ  types.Type
		want types.Decimal128
	}{
		{
			name: "scale 2",
			typ:  types.New(types.T_decimal128, 12, 2),
			want: types.Decimal128{B0_63: 33},
		},
		{
			name: "scale 6",
			typ:  types.New(types.T_decimal128, 16, 6),
			want: types.Decimal128{B0_63: 333333},
		},
		{
			name: "scale 12",
			typ:  types.New(types.T_decimal128, 22, 12),
			want: types.Decimal128{B0_63: 333333333333},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			caseUnderTest := NewFunctionTestCase(
				proc,
				inputs,
				NewFunctionTestResult(test.typ, false, []types.Decimal128{test.want}, nil),
				divFn,
			)
			passed, info := caseUnderTest.Run()
			require.True(t, passed, info)
			require.Equal(t, test.typ, *caseUnderTest.GetResultVectorDirectly().GetType())
		})
	}
}

func TestDecimal256DivisionExecutionUsesBoundResultScale(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputType := types.New(types.T_decimal256, 10, 2)
	one, err := types.ParseDecimal256("1.00", inputType.Width, inputType.Scale)
	require.NoError(t, err)
	three, err := types.ParseDecimal256("3.00", inputType.Width, inputType.Scale)
	require.NoError(t, err)
	resultType := types.New(types.T_decimal256, 42, 30)
	want, err := types.ParseDecimal256("0.333333333333333333333333333333", resultType.Width, resultType.Scale)
	require.NoError(t, err)

	caseUnderTest := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(inputType, []types.Decimal256{one}, nil),
			NewFunctionTestInput(inputType, []types.Decimal256{three}, nil),
		},
		NewFunctionTestResult(resultType, false, []types.Decimal256{want}, nil),
		divFn,
	)
	passed, info := caseUnderTest.Run()
	require.True(t, passed, info)
}

func TestDecimal256DivisionAvoidsScaledNumeratorOverflow(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputType := types.New(types.T_decimal256, 65, 0)
	value, err := types.ParseDecimal256(
		"99999999999999999999999999999999999999999999999999999999999999999",
		inputType.Width,
		inputType.Scale,
	)
	require.NoError(t, err)
	third, err := types.ParseDecimal256(
		"33333333333333333333333333333333333333333333333333333333333333333",
		inputType.Width,
		inputType.Scale,
	)
	require.NoError(t, err)
	resultType := types.New(types.T_decimal256, 65, 30)
	wantOne, err := types.ParseDecimal256("1.000000000000000000000000000000", resultType.Width, resultType.Scale)
	require.NoError(t, err)
	wantThird, err := types.ParseDecimal256("0.333333333333333333333333333333", resultType.Width, resultType.Scale)
	require.NoError(t, err)

	caseUnderTest := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(inputType, []types.Decimal256{value, third}, nil),
			NewFunctionTestInput(inputType, []types.Decimal256{value, value}, nil),
		},
		NewFunctionTestResult(resultType, false, []types.Decimal256{wantOne, wantThird}, nil),
		divFn,
	)
	passed, info := caseUnderTest.Run()
	require.True(t, passed, info)
}

func TestDecimalDivisionScaleCapBelowInputScale(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("decimal128", func(t *testing.T) {
		leftType := types.New(types.T_decimal128, 38, 37)
		rightType := types.New(types.T_decimal128, 1, 0)
		resultType := types.New(types.T_decimal128, 38, 30)
		left, err := types.ParseDecimal128("0.1000000000000000000000000000000000000", leftType.Width, leftType.Scale)
		require.NoError(t, err)
		right, err := types.ParseDecimal128("2", rightType.Width, rightType.Scale)
		require.NoError(t, err)
		want, err := types.ParseDecimal128("0.050000000000000000000000000000", resultType.Width, resultType.Scale)
		require.NoError(t, err)
		caseUnderTest := NewFunctionTestCase(
			proc,
			[]FunctionTestInput{
				NewFunctionTestInput(leftType, []types.Decimal128{left}, nil),
				NewFunctionTestInput(rightType, []types.Decimal128{right}, nil),
			},
			NewFunctionTestResult(resultType, false, []types.Decimal128{want}, nil),
			divFn,
		)
		passed, info := caseUnderTest.Run()
		require.True(t, passed, info)
	})

	t.Run("decimal256", func(t *testing.T) {
		leftType := types.New(types.T_decimal256, 65, 40)
		rightType := types.New(types.T_decimal256, 1, 0)
		resultType := types.New(types.T_decimal256, 65, 30)
		left, err := types.ParseDecimal256("0.1000000000000000000000000000000000000000", leftType.Width, leftType.Scale)
		require.NoError(t, err)
		right, err := types.ParseDecimal256("2", rightType.Width, rightType.Scale)
		require.NoError(t, err)
		want, err := types.ParseDecimal256("0.050000000000000000000000000000", resultType.Width, resultType.Scale)
		require.NoError(t, err)
		caseUnderTest := NewFunctionTestCase(
			proc,
			[]FunctionTestInput{
				NewFunctionTestInput(leftType, []types.Decimal256{left}, nil),
				NewFunctionTestInput(rightType, []types.Decimal256{right}, nil),
			},
			NewFunctionTestResult(resultType, false, []types.Decimal256{want}, nil),
			divFn,
		)
		passed, info := caseUnderTest.Run()
		require.True(t, passed, info)
	})
}
