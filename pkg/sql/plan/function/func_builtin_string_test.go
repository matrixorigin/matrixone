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
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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
		succeed, info := tcc.RunAndFree()
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
		succeed, info := tcc.RunAndFree()
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
		succeed, info := tcc.RunAndFree()
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
		succeed, info := tcc.RunAndFree()
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
		succeed, info := tcc.RunAndFree()
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
		succeed, info := tcc.RunAndFree()
		require.True(t, succeed, tc.info, info)
	}

	t.Run("bytes oracle and result ownership", func(t *testing.T) {
		values := [][]byte{
			{}, []byte("already lower"), []byte("MiXeD"),
			bytes.Repeat([]byte("a"), types.VarlenaInlineSize+1),
			bytes.Repeat([]byte("A"), types.VarlenaInlineSize),
			bytes.Repeat([]byte("A"), types.VarlenaInlineSize+1),
			bytes.Repeat([]byte("AbC"), 16), []byte("ASCII prefix ȺİKÄ中😀"),
			{0xff, 'A', 0xc3, 0xa9, 0xc0},
			bytes.Repeat([]byte("A"), (64<<10)-1), []byte("SmAlL"),
			bytes.Repeat([]byte("A"), 64<<10), bytes.Repeat([]byte("a"), 64<<10),
			bytes.Repeat([]byte("中"), (64<<10)/3),
			bytes.Repeat([]byte("A"), (64<<10)+1), []byte("AgAiN"),
		}
		source := testutil.MakeVarlenaVector(values, nil, types.T_text.ToType(), proc.Mp())
		defer source.Free(proc.Mp())
		result := vector.NewFunctionResultWrapper(types.T_text.ToType(), proc.Mp())
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(len(values)))
		require.NoError(t, builtInToLower([]*vector.Vector{source}, result, proc, len(values), nil))
		for row, value := range values {
			require.True(t, bytes.Equal(value, source.GetBytesAt(row)), "input row %d changed", row)
			require.True(t, bytes.Equal(bytes.ToLower(value), result.GetResultVector().GetBytesAt(row)), "result row %d differs", row)
		}
		for row := range values {
			stored := source.GetBytesAt(row)
			for i := range stored {
				stored[i] = '?'
			}
		}
		for row, value := range values {
			require.True(t, bytes.Equal(bytes.ToLower(value), result.GetResultVector().GetBytesAt(row)), "result row %d aliases reused input", row)
		}
	})

	t.Run("reset after fallback", func(t *testing.T) {
		source := testutil.MakeVarlenaVector([][]byte{[]byte("ABC"), []byte("DEF")}, nil, types.T_varchar.ToType(), proc.Mp())
		defer source.Free(proc.Mp())
		source.GetNulls().Add(1)
		require.NoError(t, source.SetRuntimeStringDomainsWithMP([]types.RuntimeStringDomain{types.RuntimeStringBinary, types.RuntimeStringInherit}, proc.Mp()))
		result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(2))
		require.NoError(t, builtInToLower([]*vector.Vector{source}, result, proc, 2,
			&FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}}))
		require.Equal(t, []byte("ABC"), result.GetResultVector().GetBytesAt(0))
		require.True(t, result.GetResultVector().IsNull(1))
		require.Equal(t, types.RuntimeStringBinary, result.GetResultVector().GetRuntimeStringDomainAt(0))

		next := testutil.MakeVarlenaVector([][]byte{[]byte("ALPHA"), []byte("beta")}, nil, types.T_varchar.ToType(), proc.Mp())
		defer next.Free(proc.Mp())
		require.NoError(t, result.PreExtendAndReset(2))
		require.NoError(t, builtInToLower([]*vector.Vector{next}, result, proc, 2, nil))
		for row, want := range []string{"alpha", "beta"} {
			require.False(t, result.GetResultVector().IsNull(uint64(row)))
			require.Equal(t, want, string(result.GetResultVector().GetBytesAt(row)))
			require.Equal(t, types.RuntimeStringInherit, result.GetResultVector().GetRuntimeStringDomainAt(row))
		}
	})
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
		succeed, info := tcc.RunAndFree()
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
		succeed, info := tcc.RunAndFree()
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
		succeed, info := tcc.RunAndFree()
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
		succeed, info := tcc.RunAndFree()
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
		succeed, info := tcc.RunAndFree()
		require.True(t, succeed, tc.info, info)
	}
}
