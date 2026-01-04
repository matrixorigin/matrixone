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
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// TestIntegerDivSmallTypes tests DIV operator with INT8/INT16/INT32/UINT8/UINT16/UINT32
func TestIntegerDivSmallTypes(t *testing.T) {
	proc := testutil.NewProcess(t)

	testCases := []tcTemp{
		// INT8
		{
			info: "INT8: 10 DIV 3 = 3",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(), []int8{10}, []bool{false}),
				NewFunctionTestInput(types.T_int8.ToType(), []int8{3}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{3}, []bool{false}),
		},
		{
			info: "INT8: 10 DIV 0 = NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(), []int8{10}, []bool{false}),
				NewFunctionTestInput(types.T_int8.ToType(), []int8{0}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, []bool{true}),
		},
		{
			info: "INT8: NULL DIV 3 = NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(), []int8{0}, []bool{true}),
				NewFunctionTestInput(types.T_int8.ToType(), []int8{3}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, []bool{true}),
		},
		// INT16
		{
			info: "INT16: 100 DIV 7 = 14",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int16.ToType(), []int16{100}, []bool{false}),
				NewFunctionTestInput(types.T_int16.ToType(), []int16{7}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{14}, []bool{false}),
		},
		{
			info: "INT16: 100 DIV 0 = NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int16.ToType(), []int16{100}, []bool{false}),
				NewFunctionTestInput(types.T_int16.ToType(), []int16{0}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, []bool{true}),
		},
		// INT32
		{
			info: "INT32: 1000 DIV 13 = 76",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int32.ToType(), []int32{1000}, []bool{false}),
				NewFunctionTestInput(types.T_int32.ToType(), []int32{13}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{76}, []bool{false}),
		},
		{
			info: "INT32: 1000 DIV 0 = NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int32.ToType(), []int32{1000}, []bool{false}),
				NewFunctionTestInput(types.T_int32.ToType(), []int32{0}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, []bool{true}),
		},
		// UINT8
		{
			info: "UINT8: 20 DIV 3 = 6",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint8.ToType(), []uint8{20}, []bool{false}),
				NewFunctionTestInput(types.T_uint8.ToType(), []uint8{3}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{6}, []bool{false}),
		},
		{
			info: "UINT8: 20 DIV 0 = NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint8.ToType(), []uint8{20}, []bool{false}),
				NewFunctionTestInput(types.T_uint8.ToType(), []uint8{0}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, []bool{true}),
		},
		// UINT16
		{
			info: "UINT16: 200 DIV 9 = 22",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint16.ToType(), []uint16{200}, []bool{false}),
				NewFunctionTestInput(types.T_uint16.ToType(), []uint16{9}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{22}, []bool{false}),
		},
		{
			info: "UINT16: 200 DIV 0 = NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint16.ToType(), []uint16{200}, []bool{false}),
				NewFunctionTestInput(types.T_uint16.ToType(), []uint16{0}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, []bool{true}),
		},
		// UINT32
		{
			info: "UINT32: 2000 DIV 17 = 117",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint32.ToType(), []uint32{2000}, []bool{false}),
				NewFunctionTestInput(types.T_uint32.ToType(), []uint32{17}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{117}, []bool{false}),
		},
		{
			info: "UINT32: 2000 DIV 0 = NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint32.ToType(), []uint32{2000}, []bool{false}),
				NewFunctionTestInput(types.T_uint32.ToType(), []uint32{0}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, []bool{true}),
		},
	}

	for _, tc := range testCases {
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

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

	// Unsigned integer DIV (returns int64, not uint64)
	{
		tc := tcTemp{
			info: "10 DIV 3 = 3 (unsigned)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{10, 20, 100}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{3, 7, 9}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{3, 2, 11}, []bool{false, false, false}),
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

	// UINT64 max (returns int64, not uint64)
	{
		tc := tcTemp{
			info: "UINT64 boundary: MAX_UINT64 DIV 1",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{18446744073709551615}, []bool{false}), // MAX_UINT64
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{1}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{-1}, []bool{false}), // Overflow to int64
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

// TestDivisionByZeroStrictMode tests that the cache is properly initialized and reset
func TestDivisionByZeroStrictMode(t *testing.T) {
	// This test verifies:
	// 1. DivByZeroErrorMode is initialized to -1 (not 0)
	// 2. Cache is reset when SetStmtProfile is called
	// 3. Behavior changes based on statement type and sql_mode

	proc := testutil.NewProcess(t)
	defer proc.Free()

	// Verify initial state: should be -1 (not initialized)
	initialCache := atomic.LoadInt32(&proc.Base.DivByZeroErrorMode)
	require.Equal(t, int32(-1), initialCache, "DivByZeroErrorMode should be initialized to -1")

	// Test 1: First call should compute and cache
	// Without StmtProfile, should default to "return NULL" (cache = 0)
	shouldError := checkDivisionByZeroBehavior(proc, nil)
	require.False(t, shouldError, "Without StmtProfile, should return NULL")
	require.Equal(t, int32(0), atomic.LoadInt32(&proc.Base.DivByZeroErrorMode), "Should cache 0 (return NULL)")

	// Test 2: SetStmtProfile should reset cache to -1
	proc.SetStmtProfile(&process.StmtProfile{})
	require.Equal(t, int32(-1), atomic.LoadInt32(&proc.Base.DivByZeroErrorMode), "SetStmtProfile should reset cache to -1")

	// Test 3: After reset, next call should recompute
	shouldError = checkDivisionByZeroBehavior(proc, nil)
	require.False(t, shouldError, "After reset, should recompute and return NULL")
	require.Equal(t, int32(0), atomic.LoadInt32(&proc.Base.DivByZeroErrorMode), "Should cache 0 again")

	// Test 4: Verify cache is used on subsequent calls (no recomputation)
	// We can't easily test this without mocking, but the atomic load in checkDivisionByZeroBehavior
	// will return early if cache is set, which we've verified above
}

// TestIntegerDivConstantVector tests DIV with constant vectors (column DIV constant, constant DIV column)
func TestIntegerDivConstantVector(t *testing.T) {
	proc := testutil.NewProcess(t)

	testCases := []tcTemp{
		// Column DIV Constant (INT8)
		{
			info: "INT8 column DIV constant: [10,20,30] DIV 3",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(), []int8{10, 20, 30}, []bool{false, false, false}),
				NewFunctionTestConstInput(types.T_int8.ToType(), []int8{3}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{3, 6, 10}, []bool{false, false, false}),
		},
		// Constant DIV Column (INT16)
		{
			info: "INT16 constant DIV column: 100 DIV [2,4,5]",
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_int16.ToType(), []int16{100}, []bool{false}),
				NewFunctionTestInput(types.T_int16.ToType(), []int16{2, 4, 5}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{50, 25, 20}, []bool{false, false, false}),
		},
		// Column DIV Constant 0 (INT32)
		{
			info: "INT32 column DIV 0: [10,20,30] DIV 0",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int32.ToType(), []int32{10, 20, 30}, []bool{false, false, false}),
				NewFunctionTestConstInput(types.T_int32.ToType(), []int32{0}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0, 0, 0}, []bool{true, true, true}),
		},
		// Constant DIV Column with 0 (UINT8)
		{
			info: "UINT8 constant DIV column with 0: 100 DIV [2,0,5]",
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_uint8.ToType(), []uint8{100}, []bool{false}),
				NewFunctionTestInput(types.T_uint8.ToType(), []uint8{2, 0, 5}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{50, 0, 20}, []bool{false, true, false}),
		},
		// Constant DIV Constant (UINT16)
		{
			info: "UINT16 constant DIV constant: 200 DIV 7",
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_uint16.ToType(), []uint16{200}, []bool{false}),
				NewFunctionTestConstInput(types.T_uint16.ToType(), []uint16{7}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{28}, []bool{false}),
		},
		// Large batch with constant (UINT32)
		{
			info: "UINT32 large batch DIV constant: [1000,2000,3000,4000] DIV 100",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint32.ToType(), []uint32{1000, 2000, 3000, 4000}, []bool{false, false, false, false}),
				NewFunctionTestConstInput(types.T_uint32.ToType(), []uint32{100}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{10, 20, 30, 40}, []bool{false, false, false, false}),
		},
	}

	for _, tc := range testCases {
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// TestDecimal128NegativeDivision tests negative Decimal128 DIV operations
func TestDecimal128NegativeDivision(t *testing.T) {
	proc := testutil.NewProcess(t)

	typ := types.T_decimal128.ToType()
	typ.Scale = 2

	d1, _ := types.ParseDecimal128("-12.00", 38, 2) // -12 / 3 = -4 exactly
	d2, _ := types.ParseDecimal128("3.00", 38, 2)
	d3, _ := types.ParseDecimal128("-20.00", 38, 2) // -20 / 4 = -5 exactly
	d4, _ := types.ParseDecimal128("4.00", 38, 2)
	d5, _ := types.ParseDecimal128("-98.00", 38, 2) // -98 / 7 = -14 exactly
	d6, _ := types.ParseDecimal128("7.00", 38, 2)
	d7, _ := types.ParseDecimal128("98.00", 38, 2) // 98 / -7 = -14 exactly
	d8, _ := types.ParseDecimal128("-7.00", 38, 2)
	d9, _ := types.ParseDecimal128("-98.00", 38, 2) // -98 / -7 = 14 exactly

	testCases := []tcTemp{
		{
			info: "Negative DECIMAL128: -12.00 DIV 3.00 = -4",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(typ, []types.Decimal128{d1}, []bool{false}),
				NewFunctionTestInput(typ, []types.Decimal128{d2}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{-4}, []bool{false}),
		},
		{
			info: "Negative DECIMAL128: -20.00 DIV 4.00 = -5",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(typ, []types.Decimal128{d3}, []bool{false}),
				NewFunctionTestInput(typ, []types.Decimal128{d4}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{-5}, []bool{false}),
		},
		{
			info: "Negative DECIMAL128: -98.00 DIV 7.00 = -14",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(typ, []types.Decimal128{d5}, []bool{false}),
				NewFunctionTestInput(typ, []types.Decimal128{d6}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{-14}, []bool{false}),
		},
		{
			info: "Positive DIV Negative DECIMAL128: 98.00 DIV -7.00 = -14",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(typ, []types.Decimal128{d7}, []bool{false}),
				NewFunctionTestInput(typ, []types.Decimal128{d8}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{-14}, []bool{false}),
		},
		{
			info: "Negative DIV Negative DECIMAL128: -98.00 DIV -7.00 = 14",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(typ, []types.Decimal128{d9}, []bool{false}),
				NewFunctionTestInput(typ, []types.Decimal128{d8}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{14}, []bool{false}),
		},
	}

	for _, tc := range testCases {
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// TestDecimal64NegativeDivision tests negative Decimal64 DIV operations
func TestDecimal64NegativeDivision(t *testing.T) {
	proc := testutil.NewProcess(t)

	typ := types.T_decimal64.ToType()
	typ.Scale = 2

	d1, _ := types.ParseDecimal64("-12.00", 10, 2)
	d2, _ := types.ParseDecimal64("3.00", 10, 2)
	d3, _ := types.ParseDecimal64("-20.00", 10, 2)
	d4, _ := types.ParseDecimal64("4.00", 10, 2)
	d5, _ := types.ParseDecimal64("-98.00", 10, 2)
	d6, _ := types.ParseDecimal64("7.00", 10, 2)
	d7, _ := types.ParseDecimal64("98.00", 10, 2)
	d8, _ := types.ParseDecimal64("-7.00", 10, 2)

	testCases := []tcTemp{
		{
			info: "Negative DECIMAL64: -12.00 DIV 3.00 = -4",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(typ, []types.Decimal64{d1}, []bool{false}),
				NewFunctionTestInput(typ, []types.Decimal64{d2}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{-4}, []bool{false}),
		},
		{
			info: "Negative DECIMAL64: -20.00 DIV 4.00 = -5",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(typ, []types.Decimal64{d3}, []bool{false}),
				NewFunctionTestInput(typ, []types.Decimal64{d4}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{-5}, []bool{false}),
		},
		{
			info: "Negative DECIMAL64: -98.00 DIV 7.00 = -14",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(typ, []types.Decimal64{d5}, []bool{false}),
				NewFunctionTestInput(typ, []types.Decimal64{d6}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{-14}, []bool{false}),
		},
		{
			info: "Positive DIV Negative DECIMAL64: 98.00 DIV -7.00 = -14",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(typ, []types.Decimal64{d7}, []bool{false}),
				NewFunctionTestInput(typ, []types.Decimal64{d8}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{-14}, []bool{false}),
		},
		{
			info: "Negative DIV Negative DECIMAL64: -98.00 DIV -7.00 = 14",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(typ, []types.Decimal64{d5}, []bool{false}),
				NewFunctionTestInput(typ, []types.Decimal64{d8}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{14}, []bool{false}),
		},
	}

	for _, tc := range testCases {
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}
