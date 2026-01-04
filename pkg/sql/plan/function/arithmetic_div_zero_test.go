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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// Test division by zero returns NULL
func TestDivisionByZeroReturnsNull(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Integer division by zero (integers are converted to decimal)
	{
		tc := tcTemp{
			info: "10.0 / 0.0 should return NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{10.0}, []bool{false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{0.0}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0}, []bool{true}), // NULL
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, divFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Float division by zero
	{
		tc := tcTemp{
			info: "10.5 / 0.0 should return NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{10.5}, []bool{false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{0.0}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0}, []bool{true}), // NULL
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, divFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Modulo by zero
	{
		tc := tcTemp{
			info: "10 % 0 should return NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{10}, []bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{0}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{0}, []bool{true}), // NULL
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, modFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test integer DIV operator
func TestIntegerDivOperator(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Signed integer DIV
	{
		tc := tcTemp{
			info: "10 DIV 3 = 3",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{10, -10, 10}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{3, 3, -3}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{3, -3, -3}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Unsigned integer DIV
	{
		tc := tcTemp{
			info: "10 DIV 3 = 3 (unsigned)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{10, 20, 100}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{3, 7, 9}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{3, 2, 11}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Float DIV
	{
		tc := tcTemp{
			info: "10.5 DIV 3.0 = 3",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{10.5, 20.9, 15.1}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{3.0, 7.0, 4.0}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{3, 2, 3}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Decimal64 DIV
	{
		typ := types.T_decimal64.ToType()
		typ.Scale = 2
		tc := tcTemp{
			info: "DECIMAL(10.00) DIV DECIMAL(3.00) = 3",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(typ,
					[]types.Decimal64{1000, 2100}, []bool{false, false}), // 10.00, 21.00
				NewFunctionTestInput(typ,
					[]types.Decimal64{300, 700}, []bool{false, false}), // 3.00, 7.00
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{3, 3}, []bool{false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// DIV by zero returns NULL
	{
		tc := tcTemp{
			info: "10 DIV 0 should return NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{10}, []bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{0}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{0}, []bool{true}), // NULL
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test type support
func TestIntegerDivOperatorSupports(t *testing.T) {
	tests := []struct {
		name     string
		t1       types.Type
		t2       types.Type
		expected bool
	}{
		// All integer types
		{"INT8-INT8", types.T_int8.ToType(), types.T_int8.ToType(), true},
		{"INT16-INT16", types.T_int16.ToType(), types.T_int16.ToType(), true},
		{"INT32-INT32", types.T_int32.ToType(), types.T_int32.ToType(), true},
		{"INT64-INT64", types.T_int64.ToType(), types.T_int64.ToType(), true},
		{"UINT8-UINT8", types.T_uint8.ToType(), types.T_uint8.ToType(), true},
		{"UINT16-UINT16", types.T_uint16.ToType(), types.T_uint16.ToType(), true},
		{"UINT32-UINT32", types.T_uint32.ToType(), types.T_uint32.ToType(), true},
		{"UINT64-UINT64", types.T_uint64.ToType(), types.T_uint64.ToType(), true},
		// Float types
		{"FLOAT32-FLOAT32", types.T_float32.ToType(), types.T_float32.ToType(), true},
		{"FLOAT64-FLOAT64", types.T_float64.ToType(), types.T_float64.ToType(), true},
		// Decimal types
		{"DECIMAL64-DECIMAL64", types.T_decimal64.ToType(), types.T_decimal64.ToType(), true},
		{"DECIMAL128-DECIMAL128", types.T_decimal128.ToType(), types.T_decimal128.ToType(), true},
		// Mixed types (should not be directly supported)
		{"INT64-UINT64", types.T_int64.ToType(), types.T_uint64.ToType(), false},
		{"INT64-FLOAT64", types.T_int64.ToType(), types.T_float64.ToType(), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := integerDivOperatorSupports(tt.t1, tt.t2)
			require.Equal(t, tt.expected, result)
		})
	}
}

// Test boundary values
func TestIntegerDivBoundary(t *testing.T) {
	proc := testutil.NewProcess(t)

	// INT64 min/max - overflow case
	{
		tc := tcTemp{
			info: "INT64 boundary: MIN_INT64 DIV -1 (overflow)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-9223372036854775808}, []bool{false}), // MIN_INT64
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-1}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{-9223372036854775808}, []bool{false}), // Overflow wraps around
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// UINT64 max
	{
		tc := tcTemp{
			info: "UINT64 boundary: MAX_UINT64 DIV 1",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{18446744073709551615}, []bool{false}), // MAX_UINT64
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{1}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{18446744073709551615}, []bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Decimal128 with large scale
	{
		typ := types.T_decimal128.ToType()
		typ.Scale = 10
		d1, _ := types.Decimal128FromFloat64(123456789.123456789, 38, 10)
		d2, _ := types.Decimal128FromFloat64(3.0, 38, 10)
		tc := tcTemp{
			info: "DECIMAL128 with scale 10",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(typ,
					[]types.Decimal128{d1}, []bool{false}),
				NewFunctionTestInput(typ,
					[]types.Decimal128{d2}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{41152263}, []bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Very small divisor
	{
		tc := tcTemp{
			info: "1 DIV 1 = 1",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1}, []bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{1}, []bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test NULL handling
func TestIntegerDivNullHandling(t *testing.T) {
	proc := testutil.NewProcess(t)

	// NULL dividend
	{
		tc := tcTemp{
			info: "NULL DIV 3 = NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{0}, []bool{true}), // NULL
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{3}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{0}, []bool{true}), // NULL
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// NULL divisor
	{
		tc := tcTemp{
			info: "10 DIV NULL = NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{10}, []bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{0}, []bool{true}), // NULL
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{0}, []bool{true}), // NULL
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Both NULL
	{
		tc := tcTemp{
			info: "NULL DIV NULL = NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{0}, []bool{true}), // NULL
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{0}, []bool{true}), // NULL
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{0}, []bool{true}), // NULL
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}
