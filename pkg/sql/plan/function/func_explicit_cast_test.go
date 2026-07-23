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
	"strings"
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
					[]string{"999999999999999999999999", "18446744073709551615", "-1"}, nil),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, nil),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{math.MaxUint64, math.MaxUint64, math.MaxUint64}, nil),
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

func TestExplicitCastFloatToUnsigned(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_float64.ToType(), []float64{-1.0, 0, 1.4, 1.5, 0},
			[]bool{false, false, false, false, true}),
		NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, nil),
	}
	expect := NewFunctionTestResult(types.T_uint64.ToType(), false,
		[]uint64{math.MaxUint64, 0, 1, 2, 0}, []bool{false, false, false, false, true})
	testCase := NewFunctionTestCase(proc, inputs, expect, NewExplicitCast)
	succeed, info := testCase.Run()
	require.True(t, succeed, info)
}

func TestExplicitCastFloatToSigned(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_float64.ToType(), []float64{-1.0, 0, 1.4, 1.5, 0},
			[]bool{false, false, false, false, true}),
		NewFunctionTestInput(types.T_int64.ToType(), []int64{}, nil),
	}
	expect := NewFunctionTestResult(types.T_int64.ToType(), false, []int64{-1, 0, 1, 2, 0},
		[]bool{false, false, false, false, true})
	testCase := NewFunctionTestCase(proc, inputs, expect, NewExplicitCast)
	succeed, info := testCase.Run()
	require.True(t, succeed, info)
}

func TestExplicitCastFloatRoundingToEven(t *testing.T) {
	proc := testutil.NewProcess(t)
	values64 := []float64{0.5, 1.5, 2.5, -0.5, -1.5, -2.5}
	values32 := []float32{0.5, 1.5, 2.5, -0.5, -1.5, -2.5}
	sources := []struct {
		name  string
		input FunctionTestInput
	}{
		{name: "float32", input: NewFunctionTestInput(types.T_float32.ToType(), values32, nil)},
		{name: "float64", input: NewFunctionTestInput(types.T_float64.ToType(), values64, nil)},
	}
	for _, source := range sources {
		t.Run(source.name+" to signed", func(t *testing.T) {
			inputs := []FunctionTestInput{source.input, NewFunctionTestInput(types.T_int64.ToType(), []int64{}, nil)}
			expect := NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0, 2, 2, 0, -2, -2}, nil)
			testCase := NewFunctionTestCase(proc, inputs, expect, NewExplicitCast)
			succeed, info := testCase.Run()
			require.True(t, succeed, info)
		})
		t.Run(source.name+" to unsigned", func(t *testing.T) {
			inputs := []FunctionTestInput{source.input, NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, nil)}
			expect := NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{0, 2, 2, 0, math.MaxUint64 - 1, math.MaxUint64 - 1}, nil)
			testCase := NewFunctionTestCase(proc, inputs, expect, NewExplicitCast)
			succeed, info := testCase.Run()
			require.True(t, succeed, info)
		})
	}
}

func TestExplicitCastFloatOverflowErrors(t *testing.T) {
	proc := testutil.NewProcess(t)
	tests := []struct {
		name   string
		value  float64
		target types.Type
		zero   any
	}{
		{name: "signed finite overflow", value: 9.223372036854776e18, target: types.T_int64.ToType(), zero: []int64{0}},
		{name: "unsigned finite underflow", value: -1.8446744073709552e19, target: types.T_uint64.ToType(), zero: []uint64{0}},
		{name: "signed infinity", value: math.Inf(1), target: types.T_int64.ToType(), zero: []int64{0}},
		{name: "unsigned nan", value: math.NaN(), target: types.T_uint64.ToType(), zero: []uint64{0}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			inputs := []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(), []float64{test.value}, nil),
				NewFunctionTestInput(test.target, test.zero, nil),
			}
			expect := NewFunctionTestResult(test.target, true, test.zero, []bool{false})
			testCase := NewFunctionTestCase(proc, inputs, expect, NewExplicitCast)
			succeed, info := testCase.Run()
			require.True(t, succeed, info)
		})
	}
}

func TestExplicitCastDecimalsToUnsigned(t *testing.T) {
	proc := testutil.NewProcess(t)
	decimal64, err := types.ParseDecimal64("-1.00", 10, 2)
	require.NoError(t, err)
	decimal128, err := types.ParseDecimal128("-1.00", 20, 2)
	require.NoError(t, err)
	decimal256, err := types.ParseDecimal256("-1.00", 40, 2)
	require.NoError(t, err)
	tests := []struct {
		name   string
		inputs []FunctionTestInput
	}{
		{
			name: "decimal64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.New(types.T_decimal64, 10, 2), []types.Decimal64{decimal64, 0}, []bool{false, true}),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, nil),
			},
		},
		{
			name: "decimal128",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.New(types.T_decimal128, 20, 2), []types.Decimal128{decimal128, {}}, []bool{false, true}),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, nil),
			},
		},
		{
			name: "decimal256",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.New(types.T_decimal256, 40, 2), []types.Decimal256{decimal256, {}}, []bool{false, true}),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, nil),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expect := NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{math.MaxUint64, 0}, []bool{false, true})
			testCase := NewFunctionTestCase(proc, test.inputs, expect, NewExplicitCast)
			succeed, info := testCase.Run()
			require.True(t, succeed, info)
		})
	}
}

func TestExplicitCastDecimalsToSigned(t *testing.T) {
	proc := testutil.NewProcess(t)
	decimal64, err := types.ParseDecimal64("-1.00", 10, 2)
	require.NoError(t, err)
	decimal128, err := types.ParseDecimal128("-1.00", 20, 2)
	require.NoError(t, err)
	decimal256, err := types.ParseDecimal256("-1.00", 40, 2)
	require.NoError(t, err)
	tests := []struct {
		name   string
		inputs []FunctionTestInput
	}{
		{name: "decimal64", inputs: []FunctionTestInput{
			NewFunctionTestInput(types.New(types.T_decimal64, 10, 2), []types.Decimal64{decimal64, 0}, []bool{false, true}),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{}, nil),
		}},
		{name: "decimal128", inputs: []FunctionTestInput{
			NewFunctionTestInput(types.New(types.T_decimal128, 20, 2), []types.Decimal128{decimal128, {}}, []bool{false, true}),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{}, nil),
		}},
		{name: "decimal256", inputs: []FunctionTestInput{
			NewFunctionTestInput(types.New(types.T_decimal256, 40, 2), []types.Decimal256{decimal256, {}}, []bool{false, true}),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{}, nil),
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expect := NewFunctionTestResult(types.T_int64.ToType(), false, []int64{-1, 0}, []bool{false, true})
			testCase := NewFunctionTestCase(proc, test.inputs, expect, NewExplicitCast)
			succeed, info := testCase.Run()
			require.True(t, succeed, info)
		})
	}
}

func TestExplicitCastDecimalRoundingToIntegers(t *testing.T) {
	proc := testutil.NewProcess(t)
	values := []string{"-2.5", "-1.6", "-1.5", "-1.4", "-0.5", "0.5", "1.4", "1.5", "1.6", "2.5"}
	decimal64 := make([]types.Decimal64, len(values)+1)
	decimal128 := make([]types.Decimal128, len(values)+1)
	decimal256 := make([]types.Decimal256, len(values)+1)
	for i, value := range values {
		var err error
		decimal64[i], err = types.ParseDecimal64(value, 10, 1)
		require.NoError(t, err)
		decimal128[i], err = types.ParseDecimal128(value, 20, 1)
		require.NoError(t, err)
		decimal256[i], err = types.ParseDecimal256(value, 40, 1)
		require.NoError(t, err)
	}
	nulls := make([]bool, len(values)+1)
	nulls[len(values)] = true
	tests := []struct {
		name  string
		input FunctionTestInput
	}{
		{name: "decimal64", input: NewFunctionTestInput(types.New(types.T_decimal64, 10, 1), decimal64, nulls)},
		{name: "decimal128", input: NewFunctionTestInput(types.New(types.T_decimal128, 20, 1), decimal128, nulls)},
		{name: "decimal256", input: NewFunctionTestInput(types.New(types.T_decimal256, 40, 1), decimal256, nulls)},
	}
	for _, test := range tests {
		t.Run(test.name+" to signed", func(t *testing.T) {
			inputs := []FunctionTestInput{test.input, NewFunctionTestInput(types.T_int64.ToType(), []int64{}, nil)}
			expect := NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{-3, -2, -2, -1, -1, 1, 1, 2, 2, 3, 0}, nulls)
			testCase := NewFunctionTestCase(proc, inputs, expect, NewExplicitCast)
			succeed, info := testCase.Run()
			require.True(t, succeed, info)
		})
		t.Run(test.name+" to unsigned", func(t *testing.T) {
			inputs := []FunctionTestInput{test.input, NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, nil)}
			expect := NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{math.MaxUint64 - 2, math.MaxUint64 - 1, math.MaxUint64 - 1, math.MaxUint64,
					math.MaxUint64, 1, 1, 2, 2, 3, 0}, nulls)
			testCase := NewFunctionTestCase(proc, inputs, expect, NewExplicitCast)
			succeed, info := testCase.Run()
			require.True(t, succeed, info)
		})
	}
}

func TestExplicitCastDecimalRoundingAtSignedBoundaries(t *testing.T) {
	proc := testutil.NewProcess(t)
	values := []string{
		"9223372036854775807.4",
		"9223372036854775807.5",
		"-9223372036854775808.4",
		"-9223372036854775808.5",
	}
	decimal128 := make([]types.Decimal128, len(values))
	decimal256 := make([]types.Decimal256, len(values))
	for i, value := range values {
		var err error
		decimal128[i], err = types.ParseDecimal128(value, 20, 1)
		require.NoError(t, err)
		decimal256[i], err = types.ParseDecimal256(value, 40, 1)
		require.NoError(t, err)
	}
	tests := []struct {
		name  string
		input FunctionTestInput
	}{
		{name: "decimal128", input: NewFunctionTestInput(types.New(types.T_decimal128, 20, 1), decimal128, nil)},
		{name: "decimal256", input: NewFunctionTestInput(types.New(types.T_decimal256, 40, 1), decimal256, nil)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			inputs := []FunctionTestInput{test.input, NewFunctionTestInput(types.T_int64.ToType(), []int64{}, nil)}
			expect := NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{math.MaxInt64, math.MaxInt64, math.MinInt64, math.MinInt64}, nil)
			testCase := NewFunctionTestCase(proc, inputs, expect, NewExplicitCast)
			succeed, info := testCase.Run()
			require.True(t, succeed, info)
		})
	}
}

func TestExplicitCastDecimalPositiveOverflowToSigned(t *testing.T) {
	proc := testutil.NewProcess(t)
	decimal128, err := types.ParseDecimal128("18446744073709551615", 20, 0)
	require.NoError(t, err)
	decimal256, err := types.ParseDecimal256("18446744073709551615", 40, 0)
	require.NoError(t, err)
	tests := []struct {
		name   string
		inputs []FunctionTestInput
	}{
		{name: "decimal128", inputs: []FunctionTestInput{
			NewFunctionTestInput(types.New(types.T_decimal128, 20, 0), []types.Decimal128{decimal128}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{}, nil),
		}},
		{name: "decimal256", inputs: []FunctionTestInput{
			NewFunctionTestInput(types.New(types.T_decimal256, 40, 0), []types.Decimal256{decimal256}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{}, nil),
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expect := NewFunctionTestResult(types.T_int64.ToType(), false, []int64{math.MaxInt64}, nil)
			testCase := NewFunctionTestCase(proc, test.inputs, expect, NewExplicitCast)
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
			[]string{"9999999.99", "-9999999.99", "-10000", "9999.99"}, nil),
		NewFunctionTestInput(target, []types.Decimal64{}, nil),
	}
	expect := NewFunctionTestResult(target, false,
		[]types.Decimal64{999999, types.Decimal64(999999).Minus(), types.Decimal64(999999).Minus(), 999999}, nil)
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

func TestExplicitCastStringDecimal256Overflow(t *testing.T) {
	proc := testutil.NewProcess(t)
	target := types.New(types.T_decimal256, 65, 2)
	max, err := types.ParseDecimal256(strings.Repeat("9", 63)+".99", 65, 2)
	require.NoError(t, err)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(),
			[]string{strings.Repeat("9", 69), "-" + strings.Repeat("9", 69)}, nil),
		NewFunctionTestInput(target, []types.Decimal256{}, nil),
	}
	expect := NewFunctionTestResult(target, false, []types.Decimal256{max, max.Minus()}, nil)
	testCase := NewFunctionTestCase(proc, inputs, expect, NewExplicitCast)
	succeed, info := testCase.Run()
	require.True(t, succeed, info)
}

func TestExplicitCastStringDecimalRejectsNonFinite(t *testing.T) {
	proc := testutil.NewProcess(t)
	tests := []struct {
		name   string
		target types.Type
		zero   any
	}{
		{name: "decimal64", target: types.New(types.T_decimal64, 6, 2), zero: []types.Decimal64{0}},
		{name: "decimal128", target: types.New(types.T_decimal128, 20, 2), zero: []types.Decimal128{{}}},
		{name: "decimal256", target: types.New(types.T_decimal256, 65, 2), zero: []types.Decimal256{{}}},
	}
	for _, test := range tests {
		for _, value := range []string{"Inf", "+Inf", "-Inf"} {
			t.Run(test.name+" "+value, func(t *testing.T) {
				inputs := []FunctionTestInput{
					NewFunctionTestInput(types.T_varchar.ToType(), []string{value}, nil),
					NewFunctionTestInput(test.target, test.zero, nil),
				}
				expect := NewFunctionTestResult(test.target, true, test.zero, nil)
				testCase := NewFunctionTestCase(proc, inputs, expect, NewExplicitCast)
				succeed, info := testCase.Run()
				require.True(t, succeed, info)
			})
		}
	}
}

func TestExplicitCastNumericDecimalOverflow(t *testing.T) {
	proc := testutil.NewProcess(t)
	decimal64Source, err := types.ParseDecimal64("10000.00", 7, 2)
	require.NoError(t, err)
	decimal128Source, err := types.ParseDecimal128("1000000000000000000.00", 21, 2)
	require.NoError(t, err)
	decimal256Source, err := types.ParseDecimal256(strings.Repeat("9", 69), 70, 0)
	require.NoError(t, err)

	target64 := types.New(types.T_decimal64, 6, 2)
	max64 := types.Decimal64(999999)
	target128 := types.New(types.T_decimal128, 20, 2)
	max128, err := types.ParseDecimal128("999999999999999999.99", 20, 2)
	require.NoError(t, err)
	target256 := types.New(types.T_decimal256, 65, 2)
	max256, err := types.ParseDecimal256(strings.Repeat("9", 63)+".99", 65, 2)
	require.NoError(t, err)

	tests := []struct {
		name   string
		inputs []FunctionTestInput
		expect FunctionTestResult
	}{
		{
			name: "signed integer column to decimal64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(), []int64{10000, -10000, 9999, 0}, []bool{false, false, false, true}),
				NewFunctionTestInput(target64, []types.Decimal64{}, nil),
			},
			expect: NewFunctionTestResult(target64, false,
				[]types.Decimal64{max64, max64.Minus(), 999900, 0}, []bool{false, false, false, true}),
		},
		{
			name: "unsigned integer column to decimal64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{10000, 9999}, nil),
				NewFunctionTestInput(target64, []types.Decimal64{}, nil),
			},
			expect: NewFunctionTestResult(target64, false, []types.Decimal64{max64, 999900}, nil),
		},
		{
			name: "float column to decimal128",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(), []float64{1e20, -1e20, 1.25}, nil),
				NewFunctionTestInput(target128, []types.Decimal128{}, nil),
			},
			expect: NewFunctionTestResult(target128, false,
				[]types.Decimal128{max128, max128.Minus(), {B0_63: 125}}, nil),
		},
		{
			name: "decimal64 column to decimal64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.New(types.T_decimal64, 7, 2), []types.Decimal64{decimal64Source}, nil),
				NewFunctionTestInput(target64, []types.Decimal64{}, nil),
			},
			expect: NewFunctionTestResult(target64, false, []types.Decimal64{max64}, nil),
		},
		{
			name: "decimal128 column to decimal128",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.New(types.T_decimal128, 21, 2), []types.Decimal128{decimal128Source}, nil),
				NewFunctionTestInput(target128, []types.Decimal128{}, nil),
			},
			expect: NewFunctionTestResult(target128, false, []types.Decimal128{max128}, nil),
		},
		{
			name: "decimal256 column to decimal256",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.New(types.T_decimal256, 70, 0), []types.Decimal256{decimal256Source}, nil),
				NewFunctionTestInput(target256, []types.Decimal256{}, nil),
			},
			expect: NewFunctionTestResult(target256, false, []types.Decimal256{max256}, nil),
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

func TestExplicitCastFloatToDecimalPreservesInRangeValues(t *testing.T) {
	proc := testutil.NewProcess(t)
	rounded, err := types.Decimal64FromFloat64(1.235, 6, 2)
	require.NoError(t, err)
	large, err := types.ParseDecimal64("100000000000000000", 18, 0)
	require.NoError(t, err)
	tests := []struct {
		name   string
		value  float64
		target types.Type
		want   types.Decimal64
	}{
		{name: "fractional rounding", value: 1.235, target: types.New(types.T_decimal64, 6, 2), want: rounded},
		{name: "large finite fallback", value: 1e17, target: types.New(types.T_decimal64, 18, 0), want: large},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			inputs := []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(), []float64{test.value}, nil),
				NewFunctionTestInput(test.target, []types.Decimal64{}, nil),
			}
			expect := NewFunctionTestResult(test.target, false, []types.Decimal64{test.want}, nil)
			testCase := NewFunctionTestCase(proc, inputs, expect, NewExplicitCast)
			succeed, info := testCase.Run()
			require.True(t, succeed, info)
		})
	}
}

func TestExplicitCastOverflowHelperBoundaries(t *testing.T) {
	value64, err := decimalInt64Explicit("18446744073709551615")
	require.NoError(t, err)
	require.Equal(t, int64(math.MaxInt64), value64)
	value64, err = decimalInt64Explicit("-999999999999999999999999")
	require.NoError(t, err)
	require.Equal(t, int64(math.MinInt64), value64)
	_, err = decimalInt64Explicit("not-a-number")
	require.Error(t, err)

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

	signed, err := parseSignedExplicitCastString("not-a-number", 64)
	require.NoError(t, err)
	require.Zero(t, signed)
	unsigned, err := parseUnsignedExplicitCastString("not-a-number", 64)
	require.NoError(t, err)
	require.Zero(t, unsigned)
	_, err = clampDecimal64CastString("not-a-number", 6, 2)
	require.Error(t, err)
}

func TestMatrixOneExtendedIntegerTargetsRemainStrict(t *testing.T) {
	proc := testutil.NewProcess(t)
	tests := []struct {
		name   string
		value  string
		target types.Type
		zero   any
	}{
		{name: "tinyint upper", value: "128", target: types.T_int8.ToType(), zero: []int8{0}},
		{name: "tinyint lower", value: "-129", target: types.T_int8.ToType(), zero: []int8{0}},
		{name: "smallint upper", value: "32768", target: types.T_int16.ToType(), zero: []int16{0}},
		{name: "unsigned tinyint upper", value: "256", target: types.T_uint8.ToType(), zero: []uint8{0}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			inputs := []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{test.value}, nil),
				NewFunctionTestInput(test.target, test.zero, nil),
			}
			expect := NewFunctionTestResult(test.target, true, test.zero, []bool{false})
			testCase := NewFunctionTestCase(proc, inputs, expect, NewCast)
			succeed, info := testCase.Run()
			require.True(t, succeed, info)
		})
	}
}

func TestExplicitStringIntegerPrefixMatchesMySQL(t *testing.T) {
	for _, test := range []struct {
		input string
		want  int64
	}{
		{input: "7e0", want: 7},
		{input: "7e+2", want: 7},
		{input: "1.5e0", want: 1},
		{input: "-1.5", want: -1},
		{input: "  +42suffix", want: 42},
		{input: ".5", want: 0},
		{input: "0x10", want: 0},
	} {
		t.Run(test.input, func(t *testing.T) {
			got, err := parseSignedExplicitCastString(test.input, 64)
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}

	got, err := parseUnsignedExplicitCastString("7e2", 64)
	require.NoError(t, err)
	require.Equal(t, uint64(7), got)
}

func TestAssignmentStringIntegerRoundingMatchesMySQL(t *testing.T) {
	for _, test := range []struct {
		input string
		want  int64
	}{
		{input: "1.5", want: 2},
		{input: "-1.5", want: -2},
		{input: "2.5", want: 3},
		{input: "7e2", want: 700},
		{input: "7E2", want: 700},
		{input: ".5", want: 1},
		{input: "-.5", want: -1},
		{input: "9.007199254740993e15", want: 9007199254740993},
	} {
		t.Run(test.input, func(t *testing.T) {
			got, err := parseSignedAssignmentCastString(test.input, 64)
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}

func TestImplicitStringIntegerConversionMatchesMySQL(t *testing.T) {
	for _, test := range []struct {
		input string
		want  int64
	}{
		{input: "1.5", want: 1},
		{input: "-1.5", want: -1},
		{input: "7e2", want: 700},
		{input: "7E+2", want: 700},
		{input: "1.5e1", want: 15},
	} {
		t.Run(test.input, func(t *testing.T) {
			got, err := parseSignedImplicitCastString(test.input, 64)
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}
