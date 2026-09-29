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

func TestDecimalCastChecksNarrowedPrecision(t *testing.T) {
	proc := testutil.NewProcess(t)
	decimal256Overflow, err := types.ParseDecimal256("100000000000000000000000000000000000000.00", 41, 2)
	require.NoError(t, err)
	require.Equal(t, "100000000000000000000000000000000000000.00", decimal256Overflow.Format(2))

	for _, test := range []struct {
		name   string
		source types.Type
		target types.Type
		values any
	}{
		{
			name:   "decimal64",
			source: types.New(types.T_decimal64, 6, 2),
			target: types.New(types.T_decimal64, 5, 2),
			values: []types.Decimal64{99999, 100000, types.Decimal64(99999).Minus(), types.Decimal64(100000).Minus()},
		},
		{
			name:   "decimal128",
			source: types.New(types.T_decimal128, 20, 2),
			target: types.New(types.T_decimal128, 19, 2),
			values: []types.Decimal128{
				{B0_63: 9999999999999999999},
				{B0_63: 10000000000000000000},
			},
		},
		{
			name:   "decimal256",
			source: types.New(types.T_decimal256, 41, 2),
			target: types.New(types.T_decimal256, 40, 2),
			values: []types.Decimal256{decimal256Overflow},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := NewFunctionTestInput(test.source, test.values, nil)
			target := NewFunctionTestInput(test.target, test.values, nil)
			testCase := NewFunctionTestCase(proc, []FunctionTestInput{input, target},
				NewFunctionTestResult(test.target, true, nil, nil), NewCast)
			succeed, info := testCase.Run()
			require.True(t, succeed, info)
		})
	}
}

func TestDecimalCastChecksScaleGrowthAgainstTargetPrecision(t *testing.T) {
	proc := testutil.NewProcess(t)
	decimal256Overflow, err := types.ParseDecimal256("99.99", 4, 2)
	require.NoError(t, err)

	for _, test := range []struct {
		name   string
		source types.Type
		target types.Type
		values any
	}{
		{
			name:   "decimal64",
			source: types.New(types.T_decimal64, 4, 2),
			target: types.New(types.T_decimal64, 5, 4),
			values: []types.Decimal64{9999},
		},
		{
			name:   "decimal128",
			source: types.New(types.T_decimal128, 4, 2),
			target: types.New(types.T_decimal128, 5, 4),
			values: []types.Decimal128{{B0_63: 9999}},
		},
		{
			name:   "decimal256",
			source: types.New(types.T_decimal256, 4, 2),
			target: types.New(types.T_decimal256, 5, 4),
			values: []types.Decimal256{decimal256Overflow},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			testCase := NewFunctionTestCase(proc,
				[]FunctionTestInput{
					NewFunctionTestInput(test.source, test.values, nil),
					NewFunctionTestInput(test.target, test.values, nil),
				},
				NewFunctionTestResult(test.target, true, nil, nil), NewCast)
			succeed, info := testCase.Run()
			require.True(t, succeed, info)
		})
	}

	for _, test := range []struct {
		name   string
		source FunctionTestInput
		target types.Type
		zero   any
	}{
		{
			name:   "decimal64_to_decimal128",
			source: NewFunctionTestInput(types.New(types.T_decimal64, 4, 2), []types.Decimal64{9999}, nil),
			target: types.New(types.T_decimal128, 5, 4),
			zero:   []types.Decimal128{},
		},
		{
			name:   "decimal64_to_decimal256",
			source: NewFunctionTestInput(types.New(types.T_decimal64, 4, 2), []types.Decimal64{9999}, nil),
			target: types.New(types.T_decimal256, 5, 4),
			zero:   []types.Decimal256{},
		},
		{
			name: "decimal128_to_decimal256",
			source: NewFunctionTestInput(types.New(types.T_decimal128, 4, 2),
				[]types.Decimal128{{B0_63: 9999}}, nil),
			target: types.New(types.T_decimal256, 5, 4),
			zero:   []types.Decimal256{},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			testCase := NewFunctionTestCase(proc,
				[]FunctionTestInput{test.source, NewFunctionTestInput(test.target, test.zero, nil)},
				NewFunctionTestResult(test.target, true, nil, nil), NewCast)
			succeed, info := testCase.Run()
			require.True(t, succeed, info)
		})
	}

	legal := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.New(types.T_decimal64, 3, 2), []types.Decimal64{999, 0}, []bool{false, true}),
			NewFunctionTestInput(types.New(types.T_decimal64, 5, 4), []types.Decimal64{}, nil),
		},
		NewFunctionTestResult(types.New(types.T_decimal64, 5, 4), false,
			[]types.Decimal64{99900, 0}, []bool{false, true}), NewCast)
	succeed, info := legal.Run()
	require.True(t, succeed, info)
}

func TestDecimalCastSafeScaleGrowth(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, tc := range []struct {
		name   string
		source types.Type
		target types.Type
		values any
		want   any
	}{
		{"decimal64", types.New(types.T_decimal64, 12, 2), types.New(types.T_decimal64, 14, 4),
			[]types.Decimal64{1234, types.Decimal64(1234).Minus(), 0, 0},
			[]types.Decimal64{123400, types.Decimal64(123400).Minus(), 0, 0}},
		{"decimal128", types.New(types.T_decimal128, 20, 2), types.New(types.T_decimal128, 22, 4),
			[]types.Decimal128{{B0_63: 1234}, (types.Decimal128{B0_63: 1234}).Minus(), {}, {}},
			[]types.Decimal128{{B0_63: 123400}, (types.Decimal128{B0_63: 123400}).Minus(), {}, {}}},
		{"decimal256", types.New(types.T_decimal256, 40, 2), types.New(types.T_decimal256, 42, 4),
			[]types.Decimal256{{B0_63: 1234}, (types.Decimal256{B0_63: 1234}).Minus(), {}, {}},
			[]types.Decimal256{{B0_63: 123400}, (types.Decimal256{B0_63: 123400}).Minus(), {}, {}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testCase := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(tc.source, tc.values, []bool{false, false, false, true}),
				NewFunctionTestInput(tc.target, tc.want, nil),
			}, NewFunctionTestResult(tc.target, false, tc.want, []bool{false, false, false, true}), NewCast)
			succeed, info := testCase.Run()
			require.True(t, succeed, info)
		})
	}
	for _, tc := range []struct {
		from, to types.Type
		want     bool
	}{
		{types.New(types.T_decimal64, 12, 2), types.New(types.T_decimal64, 14, 4), true},
		{types.New(types.T_decimal64, 12, 2), types.New(types.T_decimal64, 13, 4), false},
		{types.New(types.T_decimal128, 38, 36), types.New(types.T_decimal128, 38, 38), false},
		{types.New(types.T_decimal128, 20, 4), types.New(types.T_decimal128, 22, 2), false},
		{types.New(types.T_decimal256, 76, 0), types.New(types.T_decimal256, 76, 1), false},
		{types.New(types.T_decimal64, 0, 2), types.New(types.T_decimal64, 14, 4), false},
		{types.New(types.T_decimal64, 12, 2), types.New(types.T_decimal64, 19, 4), false},
	} {
		require.Equal(t, tc.want, canWidenDecimalScale(tc.from, tc.to), "%v -> %v", tc.from, tc.to)
	}
}

func TestDecimalCastCrossWidthSafeScaleGrowth(t *testing.T) {
	proc := testutil.NewProcess(t)
	max64 := types.Decimal64(999999999999999999)
	max128, err := types.ParseDecimal128("9999999999999999.9900", 20, 4)
	require.NoError(t, err)
	wide128, err := types.ParseDecimal128("999999999999999999999999999999999999.99", 38, 2)
	require.NoError(t, err)
	wide256, err := types.ParseDecimal256("999999999999999999999999999999999999.9900", 40, 4)
	require.NoError(t, err)
	largeScale, err := types.ParseDecimal256("12.34", 66, 30)
	require.NoError(t, err)
	for _, tc := range []struct {
		name   string
		source types.Type
		target types.Type
		values any
		want   any
	}{
		{"64-to-128", types.New(types.T_decimal64, 18, 2), types.New(types.T_decimal128, 20, 4),
			[]types.Decimal64{1234, types.Decimal64(1234).Minus(), max64, 0},
			[]types.Decimal128{{B0_63: 123400}, (types.Decimal128{B0_63: 123400}).Minus(), max128, {}}},
		{"64-to-256", types.New(types.T_decimal64, 18, 2), types.New(types.T_decimal256, 20, 4),
			[]types.Decimal64{1234, types.Decimal64(1234).Minus(), max64, 0},
			[]types.Decimal256{{B0_63: 123400}, (types.Decimal256{B0_63: 123400}).Minus(),
				types.Decimal256FromDecimal128(max128), {}}},
		{"128-to-256", types.New(types.T_decimal128, 38, 2), types.New(types.T_decimal256, 40, 4),
			[]types.Decimal128{{B0_63: 1234}, (types.Decimal128{B0_63: 1234}).Minus(), wide128, {}},
			[]types.Decimal256{{B0_63: 123400}, (types.Decimal256{B0_63: 123400}).Minus(), wide256, {}}},
		{"64-to-256-large-scale", types.New(types.T_decimal64, 18, 2), types.New(types.T_decimal256, 66, 30),
			[]types.Decimal64{1234, 0, 0, 0}, []types.Decimal256{largeScale, {}, {}, {}}},
		{"128-to-256-large-scale", types.New(types.T_decimal128, 38, 2), types.New(types.T_decimal256, 66, 30),
			[]types.Decimal128{{B0_63: 1234}, {}, {}, {}}, []types.Decimal256{largeScale, {}, {}, {}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(tc.source, tc.values, []bool{false, false, false, true}),
				NewFunctionTestInput(tc.target, tc.want, nil),
			}, NewFunctionTestResult(tc.target, false, tc.want, []bool{false, false, false, true}), NewCast)
			ok, info := fc.Run()
			require.True(t, ok, info)
		})
	}
	for _, tc := range []struct {
		from, to types.Type
		want     bool
	}{
		{types.New(types.T_decimal64, 18, 2), types.New(types.T_decimal128, 20, 4), true},
		{types.New(types.T_decimal64, 18, 2), types.New(types.T_decimal256, 20, 4), true},
		{types.New(types.T_decimal128, 38, 2), types.New(types.T_decimal256, 40, 4), true},
		{types.New(types.T_decimal64, 18, 2), types.New(types.T_decimal128, 19, 4), false},
		{types.New(types.T_decimal128, 38, 2), types.New(types.T_decimal256, 39, 4), false},
		{types.New(types.T_decimal128, 38, 2), types.New(types.T_decimal64, 18, 4), false},
	} {
		require.Equal(t, tc.want, canWidenDecimalScale(tc.from, tc.to), "%v -> %v", tc.from, tc.to)
	}
	// The 64->128 implementation also has a dedicated constant-vector branch.
	for _, value := range []types.Decimal64{1234, types.Decimal64(1234).Minus()} {
		want, err := (types.Decimal128{B0_63: uint64(value), B64_127: uint64(int64(value) >> 63)}).Scale(2)
		require.NoError(t, err)
		fc := NewFunctionTestCase(proc, []FunctionTestInput{
			NewFunctionTestConstInput(types.New(types.T_decimal64, 18, 2), []types.Decimal64{value}, nil),
			NewFunctionTestInput(types.New(types.T_decimal128, 20, 4), []types.Decimal128{want}, nil),
		}, NewFunctionTestResult(types.New(types.T_decimal128, 20, 4), false, []types.Decimal128{want}, nil), NewCast)
		ok, info := fc.Run()
		require.True(t, ok, info)
	}
}

func BenchmarkDecimalSafeScaleGrowth(b *testing.B) {
	proc := testutil.NewProcess(b)
	values64 := make([]types.Decimal64, 256)
	values128 := make([]types.Decimal128, 256)
	values256 := make([]types.Decimal256, 256)
	for i := range values64 {
		values64[i] = 1234
		values128[i] = types.Decimal128{B0_63: 1234}
		values256[i] = types.Decimal256{B0_63: 1234}
	}
	for _, tc := range []struct {
		name         string
		source       types.Type
		target       types.Type
		values       any
		targetValues any
	}{
		{"decimal64", types.New(types.T_decimal64, 12, 2), types.New(types.T_decimal64, 14, 4), values64, values64},
		{"decimal128", types.New(types.T_decimal128, 20, 2), types.New(types.T_decimal128, 22, 4), values128, values128},
		{"decimal256", types.New(types.T_decimal256, 40, 2), types.New(types.T_decimal256, 42, 4), values256, values256},
		{"64-to-128", types.New(types.T_decimal64, 12, 2), types.New(types.T_decimal128, 14, 4), values64, values128},
		{"64-to-256", types.New(types.T_decimal64, 12, 2), types.New(types.T_decimal256, 14, 4), values64, values256},
		{"128-to-256", types.New(types.T_decimal128, 20, 2), types.New(types.T_decimal256, 22, 4), values128, values256},
	} {
		b.Run(tc.name, func(b *testing.B) {
			fc := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(tc.source, tc.values, nil),
				NewFunctionTestInput(tc.target, tc.targetValues, nil),
			}, NewFunctionTestResult(tc.target, false, nil, nil), NewCast)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := fc.result.PreExtendAndReset(fc.fnLength); err != nil {
					b.Fatal(err)
				}
				if _, err := fc.DebugRun(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestDecimalScaleReductionRoundsOnce(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, tc := range []struct {
		name   string
		source types.Type
		target types.Type
		values any
		want   any
	}{
		{"decimal128", types.New(types.T_decimal128, 38, 38), types.New(types.T_decimal128, 38, 36),
			[]types.Decimal128{{B0_63: 49}, (types.Decimal128{B0_63: 49}).Minus(), {B0_63: 50}, (types.Decimal128{B0_63: 50}).Minus()},
			[]types.Decimal128{{}, {}, {B0_63: 1}, (types.Decimal128{B0_63: 1}).Minus()}},
		{"decimal128 long scale reduction", types.New(types.T_decimal128, 38, 38), types.New(types.T_decimal128, 5, 0),
			[]types.Decimal128{{B0_63: 49}, (types.Decimal128{B0_63: 49}).Minus()},
			[]types.Decimal128{{}, {}}},
		{"decimal128 to decimal64", types.New(types.T_decimal128, 38, 38), types.New(types.T_decimal64, 18, 17),
			[]types.Decimal128{{B0_63: 49}, (types.Decimal128{B0_63: 49}).Minus()},
			[]types.Decimal64{0, 0}},
		{"decimal128 to decimal256", types.New(types.T_decimal128, 38, 38), types.New(types.T_decimal256, 40, 36),
			[]types.Decimal128{{B0_63: 49}, (types.Decimal128{B0_63: 49}).Minus()},
			[]types.Decimal256{{}, {}}},
		{"decimal256", types.New(types.T_decimal256, 38, 38), types.New(types.T_decimal256, 38, 36),
			[]types.Decimal256{{B0_63: 49}, (types.Decimal256{B0_63: 49}).Minus(), {B0_63: 50}, (types.Decimal256{B0_63: 50}).Minus()},
			[]types.Decimal256{{}, {}, {B0_63: 1}, (types.Decimal256{B0_63: 1}).Minus()}},
		{"decimal256 to decimal128", types.New(types.T_decimal256, 38, 38), types.New(types.T_decimal128, 38, 36),
			[]types.Decimal256{{B0_63: 49}, (types.Decimal256{B0_63: 49}).Minus()},
			[]types.Decimal128{{}, {}}},
		{"decimal256 to decimal64", types.New(types.T_decimal256, 38, 38), types.New(types.T_decimal64, 18, 17),
			[]types.Decimal256{{B0_63: 49}, (types.Decimal256{B0_63: 49}).Minus()},
			[]types.Decimal64{0, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			caseRun := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(tc.source, tc.values, nil),
				NewFunctionTestInput(tc.target, tc.want, nil),
			}, NewFunctionTestResult(tc.target, false, tc.want, nil), NewCast)
			succeed, info := caseRun.Run()
			require.True(t, succeed, info)
		})
	}
}

func TestDecimal128WideningCastDoesNotRetypeSource(t *testing.T) {
	proc := testutil.NewProcess(t)
	sourceType := types.New(types.T_decimal128, 19, 2)
	targetType := types.New(types.T_decimal128, 20, 2)
	value := types.Decimal128{B0_63: 12345}

	testCase := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(sourceType, []types.Decimal128{value, {}}, []bool{false, true}),
			NewFunctionTestInput(targetType, []types.Decimal128{}, nil),
		},
		NewFunctionTestResult(targetType, false, []types.Decimal128{value, {}}, []bool{false, true}), NewCast)
	require.NoError(t, testCase.result.PreExtendAndReset(testCase.fnLength))
	result, err := testCase.DebugRun()
	require.NoError(t, err)
	require.Equal(t, sourceType, *testCase.parameters[0].GetType())
	require.Equal(t, targetType, *result.GetType())
	require.Equal(t, []types.Decimal128{value, {}}, vector.MustFixedColWithTypeCheck[types.Decimal128](result))
}
