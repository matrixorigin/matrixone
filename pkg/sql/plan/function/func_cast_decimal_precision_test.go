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
