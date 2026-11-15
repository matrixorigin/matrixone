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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestDecimalComparisonScaleOverflowPreservesOrder(t *testing.T) {
	t.Run("decimal64", func(t *testing.T) {
		max := types.Decimal64(9999999999999999)
		checkDecimalScaleComparison(t, types.T_decimal64, 16, max, max.Minus(), types.Decimal64(0))
	})
	t.Run("decimal128", func(t *testing.T) {
		max := types.Decimal128{B0_63: ^uint64(0), B64_127: ^uint64(0) >> 1}
		checkDecimalScaleComparison(t, types.T_decimal128, 38, max, max.Minus(), types.Decimal128{})
	})
	t.Run("decimal256", func(t *testing.T) {
		max := types.Decimal256{B0_63: ^uint64(0), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0) >> 1}
		checkDecimalScaleComparison(t, types.T_decimal256, 76, max, max.Minus(), types.Decimal256{})
	})
}

func checkDecimalScaleComparison[T valueDecimalCompareType](t *testing.T, oid types.T, width int32, positive, negative, zero T) {
	t.Helper()
	proc := testutil.NewProcess(t)
	defer proc.Free()
	wide := types.New(oid, width, 0)
	fine := types.New(oid, width, 6)
	values := []T{positive, negative, zero, zero}
	zeroes := []T{zero, zero, zero, zero}
	nulls := []bool{false, false, false, true}
	for _, tc := range []struct {
		name        string
		left, right FunctionTestInput
		fn          fEvalFn
		want        []bool
		wantNull    []bool
	}{
		{"vector and const", NewFunctionTestInput(wide, values, nulls), NewFunctionTestConstInput(fine, zeroes, nil), greatThanFn, []bool{true, false, false, false}, nulls},
		{"const and vector", NewFunctionTestConstInput(fine, zeroes, nil), NewFunctionTestInput(wide, values, nulls), greatThanFn, []bool{false, true, false, false}, nulls},
		{"two vectors", NewFunctionTestInput(wide, values, nulls), NewFunctionTestInput(fine, zeroes, nil), lessEqualFn, []bool{false, true, true, false}, nulls},
		{"same scale", NewFunctionTestInput(wide, values, nulls), NewFunctionTestInput(wide, zeroes, nil), greatThanFn, []bool{true, false, false, false}, nulls},
		{"scaled left vector no null", NewFunctionTestInput(wide, values, nil), NewFunctionTestConstInput(fine, zeroes, nil), greatThanFn, []bool{true, false, false, false}, nil},
		{"scaled right vector no null", NewFunctionTestConstInput(fine, zeroes, nil), NewFunctionTestInput(wide, values, nil), greatThanFn, []bool{false, true, false, false}, nil},
		{"scaled left positive constant", NewFunctionTestConstInput(wide, []T{positive, positive, positive, positive}, nil), NewFunctionTestInput(fine, zeroes, nil), greatThanFn, []bool{true, true, true, true}, nil},
		{"scaled left negative constant", NewFunctionTestConstInput(wide, []T{negative, negative, negative, negative}, nil), NewFunctionTestInput(fine, zeroes, nil), greatThanFn, []bool{false, false, false, false}, nil},
		{"scaled right positive constant", NewFunctionTestInput(fine, zeroes, nil), NewFunctionTestConstInput(wide, []T{positive, positive, positive, positive}, nil), greatThanFn, []bool{false, false, false, false}, nil},
		{"scaled right negative constant", NewFunctionTestInput(fine, zeroes, nil), NewFunctionTestConstInput(wide, []T{negative, negative, negative, negative}, nil), greatThanFn, []bool{true, true, true, true}, nil},
		{"both constants", NewFunctionTestConstInput(fine, zeroes, nil), NewFunctionTestConstInput(wide, []T{negative, negative, negative, negative}, nil), greatThanFn, []bool{true, true, true, true}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := NewFunctionTestCase(proc, []FunctionTestInput{tc.left, tc.right},
				NewFunctionTestResult(types.T_bool.ToType(), false, tc.want, tc.wantNull), tc.fn)
			ok, info := fc.RunAndFree()
			require.True(t, ok, info)
		})
	}
	fc := NewFunctionTestCase(proc, []FunctionTestInput{
		NewFunctionTestInput(wide, values, nil), NewFunctionTestConstInput(fine, zeroes, nil),
	}, NewFunctionTestResult(types.T_bool.ToType(), false, nil, nil), greatThanFn)
	defer fc.Free()
	require.NoError(t, fc.result.PreExtendAndReset(fc.fnLength))
	require.NoError(t, fc.fn(fc.parameters, fc.result, proc, fc.fnLength, &FunctionSelectList{
		AnyNull: true, SelectList: []bool{true, false, true, false},
	}))
	result := vector.GenerateFunctionFixedTypeParameter[bool](fc.result.GetResultVector())
	value, isNull := result.GetValue(0)
	require.False(t, isNull)
	require.True(t, value)
	_, isNull = result.GetValue(1)
	require.True(t, isNull)
	_, isNull = result.GetValue(3)
	require.True(t, isNull)
	require.NoError(t, fc.result.PreExtendAndReset(fc.fnLength))
	require.NoError(t, fc.fn(fc.parameters, fc.result, proc, fc.fnLength, &FunctionSelectList{AllNull: true}))
	for i := 0; i < fc.fnLength; i++ {
		_, isNull = result.GetValue(uint64(i))
		require.True(t, isNull)
	}
}

func TestDecimal64ComparisonDetectsSilentSignFlip(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	coarse := types.New(types.T_decimal64, 16, 0)
	fine := types.New(types.T_decimal64, 18, 18)
	positive := types.Decimal64(10)
	negative := positive.Minus()
	values := []types.Decimal64{positive, negative}
	zeroes := []types.Decimal64{0, 0}
	for _, tc := range []struct {
		name        string
		left, right FunctionTestInput
		want        []bool
	}{
		{"scaled left", NewFunctionTestInput(coarse, values, nil), NewFunctionTestConstInput(fine, zeroes, nil), []bool{true, false}},
		{"scaled right", NewFunctionTestConstInput(fine, zeroes, nil), NewFunctionTestInput(coarse, values, nil), []bool{false, true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := NewFunctionTestCase(proc, []FunctionTestInput{tc.left, tc.right},
				NewFunctionTestResult(types.T_bool.ToType(), false, tc.want, nil), greatThanFn)
			ok, info := fc.RunAndFree()
			require.True(t, ok, info)
		})
	}
}

func TestDecimalComparisonUsesLosslessCommonDomain(t *testing.T) {
	left := types.New(types.T_decimal64, 16, 2)
	right := types.New(types.T_decimal64, 16, 6)
	for _, name := range []string{"=", "<=>", "in_range"} {
		t.Run(name, func(t *testing.T) {
			inputs := []types.Type{left, right}
			if name == "in_range" {
				inputs = append(inputs, right, types.T_uint8.ToType())
			}
			fn, err := GetFunctionByName(context.Background(), name, inputs)
			require.NoError(t, err)
			targets, cast := fn.ShouldDoImplicitTypeCast()
			require.True(t, cast)
			operands := len(inputs)
			if name == "in_range" {
				operands--
			}
			for _, target := range targets[:operands] {
				require.Equal(t, types.T_decimal128, target.Oid)
				require.EqualValues(t, 20, target.Width)
				require.EqualValues(t, 6, target.Scale)
			}
		})
	}
}

func TestDecimal64ScaleUpThresholds(t *testing.T) {
	for _, delta := range []int32{1, 6, 18} {
		factor := int64(types.Pow10[delta])
		lower, upper := int64(math.MinInt64)/factor, int64(math.MaxInt64)/factor
		scale := decimal64ScaleUp(delta)
		for _, n := range []int64{lower - 1, lower, 0, upper, upper + 1} {
			got, ok := scale(types.Decimal64(uint64(n)))
			wantOK := n >= lower && n <= upper
			require.Equal(t, wantOK, ok, "delta=%d value=%d", delta, n)
			if wantOK {
				require.Equal(t, n*factor, int64(got), "delta=%d value=%d", delta, n)
			}
		}
	}
	scale := decimal64ScaleUp(19)
	for _, n := range []int64{-1, 0, 1} {
		_, ok := scale(types.Decimal64(uint64(n)))
		require.Equal(t, n == 0, ok, "delta=19 value=%d", n)
	}
}

func TestDecimalConstantComparisonReusesResultAcrossNullAndSelection(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	fine := types.New(types.T_decimal64, 16, 6)
	coarse := types.New(types.T_decimal64, 16, 2)
	zeroes := []types.Decimal64{0, 0, 0, 0}
	negative := types.Decimal64(123).Minus()
	constant := []types.Decimal64{negative, negative, negative, negative}
	newCase := func(leftNulls, rightNulls []bool) FunctionTestCase {
		return NewFunctionTestCase(proc, []FunctionTestInput{
			NewFunctionTestInput(fine, zeroes, leftNulls),
			NewFunctionTestConstInput(coarse, constant, rightNulls),
		}, NewFunctionTestResult(types.T_bool.ToType(), false, nil, nil), greatThanFn)
	}
	plain := newCase(nil, nil)
	defer plain.Free()
	nullable := newCase([]bool{false, true, false, false}, nil)
	defer nullable.Free()
	nullConstant := newCase(nil, []bool{true, true, true, true})
	defer nullConstant.Free()

	check := func(params []*vector.Vector, selected *FunctionSelectList, want []int) {
		t.Helper()
		require.NoError(t, plain.result.PreExtendAndReset(4))
		require.NoError(t, greatThanFn(params, plain.result, proc, 4, selected))
		result := vector.GenerateFunctionFixedTypeParameter[bool](plain.result.GetResultVector())
		for i, expected := range want {
			got, isNull := result.GetValue(uint64(i))
			if expected < 0 {
				require.True(t, isNull, "row %d", i)
			} else {
				require.False(t, isNull, "row %d", i)
				require.Equal(t, expected == 1, got, "row %d", i)
			}
		}
	}
	check(plain.parameters, nil, []int{1, 1, 1, 1})
	check(nullable.parameters, nil, []int{1, -1, 1, 1})
	check(plain.parameters, &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false, true, false}}, []int{1, -1, 1, -1})
	check(plain.parameters, &FunctionSelectList{AllNull: true}, []int{-1, -1, -1, -1})
	check(nullConstant.parameters, nil, []int{-1, -1, -1, -1})
	check(plain.parameters, nil, []int{1, 1, 1, 1})
}

func TestDecimalComparisonRepresentableConstantFastPath(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	coarse := types.New(types.T_decimal64, 16, 2)
	fine := types.New(types.T_decimal64, 16, 6)
	fineValues := []types.Decimal64{1220000, 1230000, 1240000}
	constant := []types.Decimal64{123, 123, 123}
	for _, tc := range []struct {
		name        string
		left, right FunctionTestInput
		want        []bool
	}{
		{"constant right", NewFunctionTestInput(fine, fineValues, nil), NewFunctionTestConstInput(coarse, constant, nil), []bool{false, false, true}},
		{"constant left", NewFunctionTestConstInput(coarse, constant, nil), NewFunctionTestInput(fine, fineValues, nil), []bool{true, false, false}},
		{"both constants", NewFunctionTestConstInput(coarse, constant, nil), NewFunctionTestConstInput(fine, fineValues, nil), []bool{true, true, true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := NewFunctionTestCase(proc, []FunctionTestInput{tc.left, tc.right},
				NewFunctionTestResult(types.T_bool.ToType(), false, tc.want, nil), greatThanFn)
			ok, info := fc.RunAndFree()
			require.True(t, ok, info)
		})
	}
}

func TestDecimalComparisonMixedPhysicalVectorFrameReuse(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	leftType := types.New(types.T_decimal128, 20, 2)
	rightType := types.New(types.T_decimal64, 10, 2)
	left := []types.Decimal128{{B0_63: 100}, {B0_63: 200}, {B0_63: 300}}
	right := []types.Decimal64{150, 150, 150}
	fc := NewFunctionTestCase(proc, []FunctionTestInput{
		NewFunctionTestInput(leftType, left, nil),
		NewFunctionTestConstInput(rightType, right, nil),
	}, NewFunctionTestResult(types.T_bool.ToType(), false, []bool{false, true, true}, nil), greatThanFn)
	defer fc.Free()
	for i := 0; i < 2; i++ {
		ok, info := fc.Run()
		require.True(t, ok, "batch %d: %s", i, info)
	}
}

func TestDecimalComparisonConvertedFloatScale(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	leftType := types.New(types.T_decimal128, 20, 0)
	left := []types.Decimal128{{B0_63: 1}, {B0_63: 1}, {B0_63: 1}}
	for _, tc := range []struct {
		name  string
		right FunctionTestInput
	}{
		{"float32", NewFunctionTestConstInput(types.T_float32.ToType(), []float32{0.5, 0.5, 0.5}, nil)},
		{"float64", NewFunctionTestConstInput(types.T_float64.ToType(), []float64{0.5, 0.5, 0.5}, nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(leftType, left, nil), tc.right,
			}, NewFunctionTestResult(types.T_bool.ToType(), false, []bool{true, true, true}, nil), greatThanFn)
			defer fc.Free()
			for i := 0; i < 2; i++ {
				ok, info := fc.Run()
				require.True(t, ok, "batch %d: %s", i, info)
			}
		})
	}
}
