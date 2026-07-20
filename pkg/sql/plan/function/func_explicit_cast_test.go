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
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestExplicitCastStringIntegerOverflow(t *testing.T) {
	proc := testutil.NewProcess(t)
	tests := []struct {
		name   string
		inputs []FunctionTestInput
		expect FunctionTestResult
	}{
		{
			name: "signed",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"999999999999999999999999", "9223372036854775807", "9223372036854775809", "-999999999999999999999999"}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{}, nil),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{-1, math.MaxInt64, math.MinInt64 + 1, math.MinInt64}, nil),
		},
		{
			name: "unsigned",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"999999999999999999999999", "18446744073709551615"}, nil),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, nil),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{math.MaxUint64, math.MaxUint64}, nil),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testCase := NewFunctionTestCase(proc, test.inputs, test.expect, NewExplicitCast)
			succeed, info := testCase.Run()
			require.True(t, succeed, info)
		})
	}
}

func TestExplicitCastNegativeIntegerToUnsigned(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_int64.ToType(), []int64{-1, 0, 1}, nil),
		NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, nil),
	}
	expect := NewFunctionTestResult(types.T_uint64.ToType(), false,
		[]uint64{math.MaxUint64, 0, 1}, nil)
	testCase := NewFunctionTestCase(proc, inputs, expect, NewExplicitCast)
	succeed, info := testCase.Run()
	require.True(t, succeed, info)
}

func TestExplicitCastUnsignedIntegerToSigned(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_uint64.ToType(), []uint64{math.MaxUint64, 0, 1}, nil),
		NewFunctionTestInput(types.T_int64.ToType(), []int64{}, nil),
	}
	expect := NewFunctionTestResult(types.T_int64.ToType(), false, []int64{-1, 0, 1}, nil)
	testCase := NewFunctionTestCase(proc, inputs, expect, NewExplicitCast)
	succeed, info := testCase.Run()
	require.True(t, succeed, info)
}

func TestExplicitCastStringDecimalOverflow(t *testing.T) {
	proc := testutil.NewProcess(t)
	target := types.New(types.T_decimal64, 6, 2)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(),
			[]string{"9999999.99", "-9999999.99", "9999.99"}, nil),
		NewFunctionTestInput(target, []types.Decimal64{}, nil),
	}
	expect := NewFunctionTestResult(target, false,
		[]types.Decimal64{999999, types.Decimal64(999999).Minus(), 999999}, nil)
	testCase := NewFunctionTestCase(proc, inputs, expect, NewExplicitCast)
	succeed, info := testCase.Run()
	require.True(t, succeed, info)
}

func TestExplicitCastStringDecimal128Overflow(t *testing.T) {
	proc := testutil.NewProcess(t)
	target := types.New(types.T_decimal128, 20, 2)
	max, err := types.ParseDecimal128("999999999999999999.99", 20, 2)
	require.NoError(t, err)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(),
			[]string{"999999999999999999999999.99", "-999999999999999999999999.99"}, nil),
		NewFunctionTestInput(target, []types.Decimal128{}, nil),
	}
	expect := NewFunctionTestResult(target, false, []types.Decimal128{max, max.Minus()}, nil)
	testCase := NewFunctionTestCase(proc, inputs, expect, NewExplicitCast)
	succeed, info := testCase.Run()
	require.True(t, succeed, info)
}

func TestExplicitCastOverflowHelperBoundaries(t *testing.T) {
	value, err := parseSignedExplicitCastString("255", 8)
	require.NoError(t, err)
	require.Equal(t, int64(-1), value)
	value, err = parseSignedExplicitCastString("129", 8)
	require.NoError(t, err)
	require.Equal(t, int64(-127), value)
	value, err = parseSignedExplicitCastString("-129", 8)
	require.NoError(t, err)
	require.Equal(t, int64(-128), value)

	decimal64, err := clampDecimal64CastString("0xFFFFFF", 6, 2)
	require.NoError(t, err)
	require.Equal(t, types.Decimal64(999999), decimal64)
	_, err = clampDecimal64CastString("0xGG", 6, 2)
	require.Error(t, err)
	_, err = clampDecimal64CastString("1", 0, 0)
	require.Error(t, err)

	decimal128, err := clampDecimal128CastString("999", 2, 2)
	require.NoError(t, err)
	want, err := types.ParseDecimal128("0.99", 2, 2)
	require.NoError(t, err)
	require.Equal(t, want, decimal128)
	_, err = clampDecimal128CastString("1", 2, 3)
	require.Error(t, err)
}

func TestGetFunctionByNameWithOverload(t *testing.T) {
	args := []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()}
	got, err := GetFunctionByNameWithOverload(context.Background(), "cast", args, 1)
	require.NoError(t, err)
	_, overload := DecodeOverloadID(got.GetEncodedOverloadID())
	require.Equal(t, int32(1), overload)

	_, err = GetFunctionByNameWithOverload(context.Background(), "cast", args, 99)
	require.Error(t, err)
	_, err = GetFunctionByNameWithOverload(context.Background(), "cast", []types.Type{types.T_json.ToType()}, 1)
	require.Error(t, err)
}

func TestOrdinaryCastOverflowRemainsStrict(t *testing.T) {
	_, err := parseSignedCastString("999999999999999999999999", 64)
	require.Error(t, err)
	_, err = parseUnsignedCastString("999999999999999999999999", 64)
	require.Error(t, err)
	_, err = parseDecimal64CastString("9999999.99", 6, 2)
	require.Error(t, err)

	_, err = parseSignedExplicitCastString("not-a-number", 64)
	require.Error(t, err)
	_, err = parseUnsignedExplicitCastString("not-a-number", 64)
	require.Error(t, err)
	_, err = clampDecimal64CastString("not-a-number", 6, 2)
	require.Error(t, err)
}
