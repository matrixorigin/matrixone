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

// Test_MinusFn_Int8 tests int8 subtraction with normal values, nulls, and overflow detection
func Test_MinusFn_Int8(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test normal int8 subtraction
	{
		tc := tcTemp{
			info: "select int8_col1 - int8_col2 with normal values",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{10, 0, -5, 127, -128}, []bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{5, 0, -10, 1, -1}, []bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{5, 0, 5, 126, -127}, []bool{false, false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, minusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test int8 subtraction with NULL values
	{
		tc := tcTemp{
			info: "select int8_col1 - int8_col2 with NULL values",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{10, 20, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{5, 0, 0}, []bool{false, true, false}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{5, 0, 0}, []bool{false, true, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, minusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test int8 overflow: should return error
	{
		tc := tcTemp{
			info: "select int8_col1 - int8_col2 with overflow",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{-128}, []bool{false}),
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{1}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), true,
				[]int8{0}, []bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, minusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_MinusFn_Int16 tests int16 subtraction
func Test_MinusFn_Int16(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select int16_col1 - int16_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{1000, -500, 32767, 0}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{500, -200, 1, 0}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{500, -300, 32766, 0}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, minusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_MinusFn_Int32 tests int32 subtraction
func Test_MinusFn_Int32(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select int32_col1 - int32_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{100000, -50000, 2147483647, 0}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{50000, -20000, 1, 0}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{50000, -30000, 2147483646, 0}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, minusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_MinusFn_Int64 tests int64 subtraction
func Test_MinusFn_Int64(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select int64_col1 - int64_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1000000000, -500000000, 9223372036854775807, 0}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{500000000, -200000000, 1, 0}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{500000000, -300000000, 9223372036854775806, 0}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, minusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_MinusFn_UInt8 tests uint8 subtraction with underflow detection
func Test_MinusFn_UInt8(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test normal uint8 subtraction
	{
		tc := tcTemp{
			info: "select uint8_col1 - uint8_col2 with normal values",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{255, 100, 10, 5}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{1, 50, 5, 5}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{254, 50, 5, 0}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, minusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test uint8 underflow: should return error
	{
		tc := tcTemp{
			info: "select uint8_col1 - uint8_col2 with underflow",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{0}, []bool{false}),
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{1}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_uint8.ToType(), true,
				[]uint8{0}, []bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, minusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_MinusFn_UInt16 tests uint16 subtraction
func Test_MinusFn_UInt16(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select uint16_col1 - uint16_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{65535, 1000, 500}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{1, 500, 500}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{65534, 500, 0}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, minusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_MinusFn_UInt32 tests uint32 subtraction
func Test_MinusFn_UInt32(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select uint32_col1 - uint32_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{4294967295, 1000000, 500000}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{1, 500000, 500000}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{4294967294, 500000, 0}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, minusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_MinusFn_UInt64 tests uint64 subtraction
func Test_MinusFn_UInt64(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select uint64_col1 - uint64_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{18446744073709551615, 1000000000, 500000000}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{1, 500000000, 500000000}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{18446744073709551614, 500000000, 0}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, minusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_MinusFn_Bit tests bit type subtraction
func Test_MinusFn_Bit(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select bit_col1 - bit_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bit.ToType(),
					[]uint64{255, 128, 64}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_bit.ToType(),
					[]uint64{1, 64, 64}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{254, 64, 0}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, minusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_MinusFn_Float32 tests float32 subtraction
func Test_MinusFn_Float32(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select float32_col1 - float32_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{10.5, -5.25, 0.0, 3.14159, math.MaxFloat32}, []bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{5.25, -2.75, 0.0, 1.0, 1.0}, []bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{5.25, -2.5, 0.0, 2.14159, math.MaxFloat32 - 1.0}, []bool{false, false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, minusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test float32 with NULL values
	{
		tc := tcTemp{
			info: "select float32_col1 - float32_col2 with NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{10.5, 0.0}, []bool{false, true}),
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{5.25, 1.0}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{5.25, 0.0}, []bool{false, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, minusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_MinusFn_Float64 tests float64 subtraction
func Test_MinusFn_Float64(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select float64_col1 - float64_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{10.5, -5.25, 0.0, 3.14159265359, math.MaxFloat64}, []bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{5.25, -2.75, 0.0, 1.0, 1.0}, []bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{5.25, -2.5, 0.0, 2.14159265359, math.MaxFloat64 - 1.0}, []bool{false, false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, minusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test float64 with NULL values
	{
		tc := tcTemp{
			info: "select float64_col1 - float64_col2 with NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{10.5, 0.0, 0.0}, []bool{false, true, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{5.25, 1.0, 0.0}, []bool{false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{5.25, 0.0, 0.0}, []bool{false, true, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, minusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_MinusFn_Decimal64 tests decimal64 subtraction
func Test_MinusFn_Decimal64(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select decimal64_col1 - decimal64_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_decimal64.ToType(),
					[]types.Decimal64{1000, 500, 250}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_decimal64.ToType(),
					[]types.Decimal64{100, 200, 250}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_decimal64.ToType(), false,
				[]types.Decimal64{900, 300, 0}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, minusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test decimal64 with NULL values
	{
		tc := tcTemp{
			info: "select decimal64_col1 - decimal64_col2 with NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_decimal64.ToType(),
					[]types.Decimal64{1000, 0}, []bool{false, true}),
				NewFunctionTestInput(types.T_decimal64.ToType(),
					[]types.Decimal64{100, 200}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_decimal64.ToType(), false,
				[]types.Decimal64{900, 0}, []bool{false, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, minusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_MinusFn_Decimal128 tests decimal128 subtraction
func Test_MinusFn_Decimal128(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select decimal128_col1 - decimal128_col2",
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
					{B0_63: 9000, B64_127: 0},
					{B0_63: 3000, B64_127: 0},
					{B0_63: 0, B64_127: 0},
				},
				[]bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, minusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_MinusFn_Date tests date subtraction (returns int64 - number of days difference)
func Test_MinusFn_Date(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select date_col1 - date_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{100, 50, 30}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{90, 40, 30}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{10, 10, 0}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, minusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test date subtraction with NULL
	{
		tc := tcTemp{
			info: "select date_col1 - date_col2 with NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{100, 0}, []bool{false, true}),
				NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{90, 40}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{10, 0}, []bool{false, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, minusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_MinusFn_Datetime tests datetime subtraction (returns int64 - number of seconds difference)
func Test_MinusFn_Datetime(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		// Create some datetime values
		dt1 := types.Datetime(1000000)
		dt2 := types.Datetime(500000)
		dt3 := types.Datetime(250000)
		dt4 := types.Datetime(200000)

		tc := tcTemp{
			info: "select datetime_col1 - datetime_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{dt1, dt3}, []bool{false, false}),
				NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{dt2, dt4}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{dt1.DatetimeMinusWithSecond(dt2), dt3.DatetimeMinusWithSecond(dt4)}, []bool{false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, minusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_MinusFn_ArrayFloat32 tests array_float32 subtraction
func Test_MinusFn_ArrayFloat32(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select array_float32_col1 - array_float32_col2",
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
					{0.5, 1.0, 1.5},
					{5.0, 10.0, 15.0},
				},
				[]bool{false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, minusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test array_float32 with NULL
	{
		tc := tcTemp{
			info: "select array_float32_col1 - array_float32_col2 with NULL",
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
					{0.5, 1.0, 1.5},
					{},
				},
				[]bool{false, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, minusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_MinusFn_ArrayFloat64 tests array_float64 subtraction
func Test_MinusFn_ArrayFloat64(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select array_float64_col1 - array_float64_col2",
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
					{0.5, 1.0, 1.5},
					{5.0, 10.0, 15.0},
					{50.25, 100.25, 150.25},
				},
				[]bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, minusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test array_float64 with NULL
	{
		tc := tcTemp{
			info: "select array_float64_col1 - array_float64_col2 with NULL",
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
					{0.5, 1.0, 1.5},
					{},
					{},
				},
				[]bool{false, true, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, minusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_MinusFn_EdgeCases tests various edge cases
func Test_MinusFn_EdgeCases(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test subtracting zero
	{
		tc := tcTemp{
			info: "select int64_col - 0",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{100, -50, 0}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{0, 0, 0}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{100, -50, 0}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, minusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test subtracting from itself (result should be zero)
	{
		tc := tcTemp{
			info: "select int64_col - int64_col",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{100, -50, 0}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{100, -50, 0}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{0, 0, 0}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, minusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}
