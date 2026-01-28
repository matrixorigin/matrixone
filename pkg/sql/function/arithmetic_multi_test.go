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

// Test_MultiFn_Int8 tests int8 multiplication with normal values, nulls, and overflow detection
func Test_MultiFn_Int8(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test normal int8 multiplication
	{
		tc := tcTemp{
			info: "select int8_col1 * int8_col2 with normal values",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{5, -4, 0, 10, -10}, []bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{3, 2, 5, 0, -1}, []bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{15, -8, 0, 0, 10}, []bool{false, false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, multiFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test int8 multiplication with NULL values
	{
		tc := tcTemp{
			info: "select int8_col1 * int8_col2 with NULL values",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{5, 10, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{3, 0, 5}, []bool{false, true, false}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{15, 0, 0}, []bool{false, true, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, multiFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test int8 overflow: should return error
	{
		tc := tcTemp{
			info: "select int8_col1 * int8_col2 with overflow",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{64}, []bool{false}),
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{2}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), true,
				[]int8{0}, []bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, multiFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_MultiFn_Int16 tests int16 multiplication
func Test_MultiFn_Int16(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select int16_col1 * int16_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{100, -50, 0, 256}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{5, 4, 10, 2}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{500, -200, 0, 512}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, multiFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_MultiFn_Int32 tests int32 multiplication
func Test_MultiFn_Int32(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select int32_col1 * int32_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{1000, -500, 0, 65536}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{50, 40, 100, 2}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{50000, -20000, 0, 131072}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, multiFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_MultiFn_Int64 tests int64 multiplication
func Test_MultiFn_Int64(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select int64_col1 * int64_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1000000, -500000, 0, 123456789}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{500, 400, 1000, 2}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{500000000, -200000000, 0, 246913578}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, multiFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_MultiFn_UInt8 tests uint8 multiplication with overflow detection
func Test_MultiFn_UInt8(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test normal uint8 multiplication
	{
		tc := tcTemp{
			info: "select uint8_col1 * uint8_col2 with normal values",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{10, 5, 0, 15}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{2, 3, 10, 2}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{20, 15, 0, 30}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, multiFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test uint8 overflow: should return error
	{
		tc := tcTemp{
			info: "select uint8_col1 * uint8_col2 with overflow",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{128}, []bool{false}),
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{2}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_uint8.ToType(), true,
				[]uint8{0}, []bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, multiFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_MultiFn_UInt16 tests uint16 multiplication
func Test_MultiFn_UInt16(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select uint16_col1 * uint16_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{1000, 500, 0, 256}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{5, 4, 10, 2}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{5000, 2000, 0, 512}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, multiFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_MultiFn_UInt32 tests uint32 multiplication
func Test_MultiFn_UInt32(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select uint32_col1 * uint32_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{100000, 50000, 0, 65536}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{50, 40, 100, 2}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{5000000, 2000000, 0, 131072}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, multiFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_MultiFn_UInt64 tests uint64 multiplication
func Test_MultiFn_UInt64(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select uint64_col1 * uint64_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{1000000, 500000, 0, 123456789}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{500, 400, 1000, 2}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{500000000, 200000000, 0, 246913578}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, multiFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_MultiFn_Bit tests bit type multiplication
func Test_MultiFn_Bit(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select bit_col1 * bit_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bit.ToType(),
					[]uint64{10, 5, 0, 256}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_bit.ToType(),
					[]uint64{2, 3, 10, 2}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{20, 15, 0, 512}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, multiFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_MultiFn_Float32 tests float32 multiplication
func Test_MultiFn_Float32(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select float32_col1 * float32_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{2.5, -3.0, 0.0, 1.5, 10.0}, []bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{4.0, 2.0, 5.5, 2.0, 0.1}, []bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{10.0, -6.0, 0.0, 3.0, 1.0}, []bool{false, false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, multiFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test float32 with NULL values
	{
		tc := tcTemp{
			info: "select float32_col1 * float32_col2 with NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{2.5, 0.0}, []bool{false, true}),
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{4.0, 5.5}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{10.0, 0.0}, []bool{false, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, multiFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test float32 with special values
	{
		tc := tcTemp{
			info: "select float32_col1 * float32_col2 with special values",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{1.0, -1.0, 2.0}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{0.0, 0.0, math.MaxFloat32 / 2}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{0.0, 0.0, math.MaxFloat32}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, multiFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_MultiFn_Float64 tests float64 multiplication
func Test_MultiFn_Float64(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select float64_col1 * float64_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{2.5, -3.0, 0.0, 1.5, 10.0}, []bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{4.0, 2.0, 5.5, 2.0, 0.1}, []bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{10.0, -6.0, 0.0, 3.0, 1.0}, []bool{false, false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, multiFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test float64 with NULL values
	{
		tc := tcTemp{
			info: "select float64_col1 * float64_col2 with NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{2.5, 0.0, 0.0}, []bool{false, true, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{4.0, 5.5, 0.0}, []bool{false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{10.0, 0.0, 0.0}, []bool{false, true, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, multiFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test float64 precision
	{
		tc := tcTemp{
			info: "select float64_col1 * float64_col2 with high precision",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{3.14159265359, 2.71828182846, 1.41421356237}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{2.0, 3.0, 1.41421356237}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{6.28318530718, 8.15484548538, 2.0}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, multiFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_MultiFn_Decimal64 tests decimal64 multiplication
func Test_MultiFn_Decimal64(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select decimal64_col1 * decimal64_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_decimal64.ToType(),
					[]types.Decimal64{100, 50, 25}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_decimal64.ToType(),
					[]types.Decimal64{10, 20, 4}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{
					{B0_63: 1000, B64_127: 0},
					{B0_63: 1000, B64_127: 0},
					{B0_63: 100, B64_127: 0},
				},
				[]bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, multiFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test decimal64 with NULL values
	{
		tc := tcTemp{
			info: "select decimal64_col1 * decimal64_col2 with NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_decimal64.ToType(),
					[]types.Decimal64{100, 0}, []bool{false, true}),
				NewFunctionTestInput(types.T_decimal64.ToType(),
					[]types.Decimal64{10, 20}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{
					{B0_63: 1000, B64_127: 0},
					{B0_63: 0, B64_127: 0},
				},
				[]bool{false, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, multiFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_MultiFn_Decimal128 tests decimal128 multiplication
func Test_MultiFn_Decimal128(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select decimal128_col1 * decimal128_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_decimal128.ToType(),
					[]types.Decimal128{
						{B0_63: 1000, B64_127: 0},
						{B0_63: 500, B64_127: 0},
						{B0_63: 250, B64_127: 0},
					},
					[]bool{false, false, false}),
				NewFunctionTestInput(types.T_decimal128.ToType(),
					[]types.Decimal128{
						{B0_63: 10, B64_127: 0},
						{B0_63: 20, B64_127: 0},
						{B0_63: 4, B64_127: 0},
					},
					[]bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{
					{B0_63: 10000, B64_127: 0},
					{B0_63: 10000, B64_127: 0},
					{B0_63: 1000, B64_127: 0},
				},
				[]bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, multiFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test decimal128 with NULL values
	{
		tc := tcTemp{
			info: "select decimal128_col1 * decimal128_col2 with NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_decimal128.ToType(),
					[]types.Decimal128{
						{B0_63: 1000, B64_127: 0},
						{B0_63: 0, B64_127: 0},
					},
					[]bool{false, true}),
				NewFunctionTestInput(types.T_decimal128.ToType(),
					[]types.Decimal128{
						{B0_63: 10, B64_127: 0},
						{B0_63: 20, B64_127: 0},
					},
					[]bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{
					{B0_63: 10000, B64_127: 0},
					{B0_63: 0, B64_127: 0},
				},
				[]bool{false, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, multiFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_MultiFn_ArrayFloat32 tests array_float32 multiplication (element-wise)
func Test_MultiFn_ArrayFloat32(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select array_float32_col1 * array_float32_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(),
					[][]float32{
						{1.0, 2.0, 3.0},
						{10.0, 20.0, 30.0},
					},
					[]bool{false, false}),
				NewFunctionTestInput(types.T_array_float32.ToType(),
					[][]float32{
						{2.0, 3.0, 4.0},
						{0.5, 0.25, 0.1},
					},
					[]bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{
					{2.0, 6.0, 12.0},
					{5.0, 5.0, 3.0},
				},
				[]bool{false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, multiFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test array_float32 with NULL
	{
		tc := tcTemp{
			info: "select array_float32_col1 * array_float32_col2 with NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(),
					[][]float32{
						{1.0, 2.0, 3.0},
						{},
					},
					[]bool{false, true}),
				NewFunctionTestInput(types.T_array_float32.ToType(),
					[][]float32{
						{2.0, 3.0, 4.0},
						{0.5, 0.25, 0.1},
					},
					[]bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{
					{2.0, 6.0, 12.0},
					{},
				},
				[]bool{false, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, multiFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_MultiFn_ArrayFloat64 tests array_float64 multiplication (element-wise)
func Test_MultiFn_ArrayFloat64(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select array_float64_col1 * array_float64_col2",
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
						{2.0, 3.0, 4.0},
						{0.5, 0.25, 0.1},
						{2.0, 2.0, 2.0},
					},
					[]bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_array_float64.ToType(), false,
				[][]float64{
					{2.0, 6.0, 12.0},
					{5.0, 5.0, 3.0},
					{201.0, 401.0, 601.0},
				},
				[]bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, multiFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test array_float64 with NULL
	{
		tc := tcTemp{
			info: "select array_float64_col1 * array_float64_col2 with NULL",
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
						{2.0, 3.0, 4.0},
						{0.5, 0.25, 0.1},
						{},
					},
					[]bool{false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_array_float64.ToType(), false,
				[][]float64{
					{2.0, 6.0, 12.0},
					{},
					{},
				},
				[]bool{false, true, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, multiFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_MultiFn_EdgeCases tests various edge cases
func Test_MultiFn_EdgeCases(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test multiplying by one (identity)
	{
		tc := tcTemp{
			info: "select int64_col * 1",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{100, -50, 0}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1, 1, 1}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{100, -50, 0}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, multiFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test multiplying by zero
	{
		tc := tcTemp{
			info: "select int64_col * 0",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{100, -50, 123456789}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{0, 0, 0}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{0, 0, 0}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, multiFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test multiplying by negative one (negation)
	{
		tc := tcTemp{
			info: "select int64_col * -1",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{100, -50, 0}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-1, -1, -1}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{-100, 50, 0}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, multiFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test commutative property (a * b = b * a)
	{
		tc := tcTemp{
			info: "test commutative property of multiplication",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{7, 13, 29}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{13, 7, 3}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{91, 91, 87}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, multiFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}
