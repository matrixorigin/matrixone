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

// Test_BuiltInToUpper tests UPPER/TO_UPPER function
// This function calls string transformation templates
func Test_BuiltInToUpper(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select upper(varchar_col)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"hello", "World", "MATRIX", "MaTrIx", "123abc", ""},
					[]bool{false, false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"HELLO", "WORLD", "MATRIX", "MATRIX", "123ABC", ""},
				[]bool{false, false, false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInToUpper)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test with NULL
	{
		tc := tcTemp{
			info: "select upper(varchar_col) with NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"hello", "", "world"},
					[]bool{false, true, false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"HELLO", "", "WORLD"},
				[]bool{false, true, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInToUpper)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test with special characters
	{
		tc := tcTemp{
			info: "select upper with special characters",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"hello@world", "test_123", "a-b-c"},
					[]bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"HELLO@WORLD", "TEST_123", "A-B-C"},
				[]bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInToUpper)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_BuiltInToLower tests LOWER/TO_LOWER function
func Test_BuiltInToLower(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select lower(varchar_col)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"HELLO", "World", "matrix", "MaTrIx", "123ABC", ""},
					[]bool{false, false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"hello", "world", "matrix", "matrix", "123abc", ""},
				[]bool{false, false, false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInToLower)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test with NULL
	{
		tc := tcTemp{
			info: "select lower(varchar_col) with NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"HELLO", "", "WORLD"},
					[]bool{false, true, false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"hello", "", "world"},
				[]bool{false, true, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInToLower)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test with special characters
	{
		tc := tcTemp{
			info: "select lower with special characters",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"HELLO@WORLD", "TEST_123", "A-B-C"},
					[]bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"hello@world", "test_123", "a-b-c"},
				[]bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInToLower)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_AbsUInt64 tests ABS function for uint64 (identity function)
func Test_AbsUInt64(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select abs(uint64_col)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{100, 0, 999, 18446744073709551615},
					[]bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{100, 0, 999, 18446744073709551615},
				[]bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, AbsUInt64)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_AbsFloat64 tests ABS function for float64
func Test_AbsFloat64(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select abs(float64_col)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1.5, -2.5, 0.0, 100.123, -100.123, -999.999},
					[]bool{false, false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{1.5, 2.5, 0.0, 100.123, 100.123, 999.999},
				[]bool{false, false, false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, AbsFloat64)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test with NULL
	{
		tc := tcTemp{
			info: "select abs(float64_col) with NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{-1.5, 0.0},
					[]bool{false, true}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{1.5, 0.0},
				[]bool{false, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, AbsFloat64)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_AbsDecimal64 tests ABS function for decimal64
func Test_AbsDecimal64(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select abs(decimal64_col)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_decimal64.ToType(),
					[]types.Decimal64{
						types.Decimal64(100),
						types.Decimal64(100).Minus(),
						types.Decimal64(0),
						types.Decimal64(999).Minus(),
					},
					[]bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_decimal64.ToType(), false,
				[]types.Decimal64{
					types.Decimal64(100),
					types.Decimal64(100),
					types.Decimal64(0),
					types.Decimal64(999),
				},
				[]bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, AbsDecimal64)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_AbsDecimal128 tests ABS function for decimal128
func Test_AbsDecimal128(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select abs(decimal128_col)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_decimal128.ToType(),
					[]types.Decimal128{
						{B0_63: 100, B64_127: 0},
						types.Decimal128{B0_63: 100, B64_127: 0}.Minus(),
						{B0_63: 0, B64_127: 0},
						types.Decimal128{B0_63: 999, B64_127: 0}.Minus(),
					},
					[]bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{
					{B0_63: 100, B64_127: 0},
					{B0_63: 100, B64_127: 0},
					{B0_63: 0, B64_127: 0},
					{B0_63: 999, B64_127: 0},
				},
				[]bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, AbsDecimal128)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}
