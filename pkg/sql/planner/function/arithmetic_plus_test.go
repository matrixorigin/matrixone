// Copyright 2021 Matrix Origin
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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// Test_PlusFn_Int8 tests int8 addition with normal values, nulls, and overflow detection
func Test_PlusFn_Int8(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test normal int8 addition
	{
		tc := tcTemp{
			info: "select int8_col1 + int8_col2 with normal values",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{10, -5, 0, 50, -60}, []bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{5, -10, 0, 30, -20}, []bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{15, -15, 0, 80, -80}, []bool{false, false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test int8 addition with NULL values
	{
		tc := tcTemp{
			info: "select int8_col1 + int8_col2 with NULL values",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{10, 20, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{5, 0, 0}, []bool{false, true, false}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{15, 0, 0}, []bool{false, true, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test int8 overflow: should return error
	{
		tc := tcTemp{
			info: "select int8_col1 + int8_col2 with overflow",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{127}, []bool{false}),
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{1}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), true,
				[]int8{0}, []bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_PlusFn_Int16 tests int16 addition
func Test_PlusFn_Int16(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select int16_col1 + int16_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{1000, -500, 0, 16000}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{500, -200, 0, 16000}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{1500, -700, 0, 32000}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_PlusFn_Int32 tests int32 addition
func Test_PlusFn_Int32(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select int32_col1 + int32_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{100000, -50000, 0, 1000000000}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{50000, -20000, 0, 1000000000}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{150000, -70000, 0, 2000000000}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_PlusFn_Int64 tests int64 addition
func Test_PlusFn_Int64(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select int64_col1 + int64_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1000000000, -500000000, 0, 4000000000000000000}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{500000000, -200000000, 0, 4000000000000000000}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{1500000000, -700000000, 0, 8000000000000000000}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_PlusFn_UInt8 tests uint8 addition with overflow detection
func Test_PlusFn_UInt8(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test normal uint8 addition
	{
		tc := tcTemp{
			info: "select uint8_col1 + uint8_col2 with normal values",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{100, 50, 0, 128}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{50, 25, 0, 127}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{150, 75, 0, 255}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test uint8 overflow: should return error
	{
		tc := tcTemp{
			info: "select uint8_col1 + uint8_col2 with overflow",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{255}, []bool{false}),
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{1}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_uint8.ToType(), true,
				[]uint8{0}, []bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_PlusFn_UInt16 tests uint16 addition
func Test_PlusFn_UInt16(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select uint16_col1 + uint16_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{30000, 1000, 0, 32767}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{30000, 500, 0, 32768}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{60000, 1500, 0, 65535}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_PlusFn_UInt32 tests uint32 addition
func Test_PlusFn_UInt32(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select uint32_col1 + uint32_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{2000000000, 1000000, 0, 2147483647}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{2000000000, 500000, 0, 2147483648}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{4000000000, 1500000, 0, 4294967295}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_PlusFn_UInt64 tests uint64 addition
func Test_PlusFn_UInt64(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select uint64_col1 + uint64_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{9000000000000000000, 1000000000, 0, 9223372036854775807}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{9000000000000000000, 500000000, 0, 9223372036854775808}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{18000000000000000000, 1500000000, 0, 18446744073709551615}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_PlusFn_Bit tests bit type addition
func Test_PlusFn_Bit(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select bit_col1 + bit_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bit.ToType(),
					[]uint64{128, 64, 0, 100}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_bit.ToType(),
					[]uint64{64, 32, 0, 200}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{192, 96, 0, 300}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_PlusFn_Float32 tests float32 addition
func Test_PlusFn_Float32(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select float32_col1 + float32_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{10.5, -5.25, 0.0, 3.14159, 1.0}, []bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{5.25, -2.75, 0.0, 2.71828, 2.5}, []bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{15.75, -8.0, 0.0, 5.85987, 3.5}, []bool{false, false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test float32 with NULL values
	{
		tc := tcTemp{
			info: "select float32_col1 + float32_col2 with NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{10.5, 0.0}, []bool{false, true}),
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{5.25, 1.0}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{15.75, 0.0}, []bool{false, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_PlusFn_Float64 tests float64 addition
func Test_PlusFn_Float64(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select float64_col1 + float64_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{10.5, -5.25, 0.0, 3.14159265359, 1.23456789}, []bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{5.25, -2.75, 0.0, 2.71828182846, 9.87654321}, []bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{15.75, -8.0, 0.0, 5.85987448205, 11.1111111}, []bool{false, false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test float64 with NULL values
	{
		tc := tcTemp{
			info: "select float64_col1 + float64_col2 with NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{10.5, 0.0, 0.0}, []bool{false, true, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{5.25, 1.0, 0.0}, []bool{false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{15.75, 0.0, 0.0}, []bool{false, true, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_PlusFn_Decimal64 tests decimal64 addition
func Test_PlusFn_Decimal64(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select decimal64_col1 + decimal64_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_decimal64.ToType(),
					[]types.Decimal64{1000, 500, 250}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_decimal64.ToType(),
					[]types.Decimal64{100, 200, 250}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_decimal64.ToType(), false,
				[]types.Decimal64{1100, 700, 500}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test decimal64 with NULL values
	{
		tc := tcTemp{
			info: "select decimal64_col1 + decimal64_col2 with NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_decimal64.ToType(),
					[]types.Decimal64{1000, 0}, []bool{false, true}),
				NewFunctionTestInput(types.T_decimal64.ToType(),
					[]types.Decimal64{100, 200}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_decimal64.ToType(), false,
				[]types.Decimal64{1100, 0}, []bool{false, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_PlusFn_Decimal128 tests decimal128 addition
func Test_PlusFn_Decimal128(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select decimal128_col1 + decimal128_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_decimal128.ToType(),
					[]types.Decimal128{
						{B0_63: 10000, B64_127: 0},
						{B0_63: 5000, B64_127: 0},
						{B0_63: 2500, B64_127: 0},
					},
					[]bool{false, false, false}),
				NewFunctionTestInput(types.T_decimal128.ToType(),
					[]types.Decimal128{
						{B0_63: 1000, B64_127: 0},
						{B0_63: 2000, B64_127: 0},
						{B0_63: 2500, B64_127: 0},
					},
					[]bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{
					{B0_63: 11000, B64_127: 0},
					{B0_63: 7000, B64_127: 0},
					{B0_63: 5000, B64_127: 0},
				},
				[]bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_PlusFn_ArrayFloat32 tests array_float32 addition
func Test_PlusFn_ArrayFloat32(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select array_float32_col1 + array_float32_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(),
					[][]float32{
						{1.0, 2.0, 3.0},
						{10.0, 20.0, 30.0},
					},
					[]bool{false, false}),
				NewFunctionTestInput(types.T_array_float32.ToType(),
					[][]float32{
						{0.5, 1.0, 1.5},
						{5.0, 10.0, 15.0},
					},
					[]bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{
					{1.5, 3.0, 4.5},
					{15.0, 30.0, 45.0},
				},
				[]bool{false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test array_float32 with NULL
	{
		tc := tcTemp{
			info: "select array_float32_col1 + array_float32_col2 with NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(),
					[][]float32{
						{1.0, 2.0, 3.0},
						{},
					},
					[]bool{false, true}),
				NewFunctionTestInput(types.T_array_float32.ToType(),
					[][]float32{
						{0.5, 1.0, 1.5},
						{5.0, 10.0, 15.0},
					},
					[]bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{
					{1.5, 3.0, 4.5},
					{},
				},
				[]bool{false, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_PlusFn_ArrayFloat64 tests array_float64 addition
func Test_PlusFn_ArrayFloat64(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select array_float64_col1 + array_float64_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float64.ToType(),
					[][]float64{
						{1.0, 2.0, 3.0},
						{10.0, 20.0, 30.0},
						{100.5, 200.5, 300.5},
					},
					[]bool{false, false, false}),
				NewFunctionTestInput(types.T_array_float64.ToType(),
					[][]float64{
						{0.5, 1.0, 1.5},
						{5.0, 10.0, 15.0},
						{50.25, 100.25, 150.25},
					},
					[]bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_array_float64.ToType(), false,
				[][]float64{
					{1.5, 3.0, 4.5},
					{15.0, 30.0, 45.0},
					{150.75, 300.75, 450.75},
				},
				[]bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test array_float64 with NULL
	{
		tc := tcTemp{
			info: "select array_float64_col1 + array_float64_col2 with NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float64.ToType(),
					[][]float64{
						{1.0, 2.0, 3.0},
						{},
						{100.5, 200.5, 300.5},
					},
					[]bool{false, true, false}),
				NewFunctionTestInput(types.T_array_float64.ToType(),
					[][]float64{
						{0.5, 1.0, 1.5},
						{5.0, 10.0, 15.0},
						{},
					},
					[]bool{false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_array_float64.ToType(), false,
				[][]float64{
					{1.5, 3.0, 4.5},
					{},
					{},
				},
				[]bool{false, true, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_PlusFn_EdgeCases tests various edge cases
func Test_PlusFn_EdgeCases(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test adding zero (identity)
	{
		tc := tcTemp{
			info: "select int64_col + 0",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{100, -50, 0}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{0, 0, 0}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{100, -50, 0}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test commutative property (a + b = b + a)
	{
		tc := tcTemp{
			info: "test commutative property of addition",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{7, 13, 29}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{13, 7, 3}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{20, 20, 32}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test adding opposite signs
	{
		tc := tcTemp{
			info: "select positive + negative",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{100, 50, 200}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-50, -50, -200}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{50, 0, 0}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test special float values
	{
		tc := tcTemp{
			info: "select float64 with special values",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{math.MaxFloat64 / 2, -1.0, 1.0}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{math.MaxFloat64 / 2, 1.0, -1.0}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{math.MaxFloat64, 0.0, 0.0}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}
