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

func Test_IntegerDivFn_Int8(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := tcTemp{
		info: "select int8_col1 DIV int8_col2",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_int8.ToType(),
				[]int8{10, 25, -10, 7}, []bool{false, false, false, false}),
			NewFunctionTestInput(types.T_int8.ToType(),
				[]int8{3, 7, 3, 2}, []bool{false, false, false, false}),
		},
		expect: NewFunctionTestResult(types.T_int64.ToType(), false,
			[]int64{3, 3, -3, 3}, []bool{false, false, false, false}),
	}
	tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
	succeed, info := tcc.Run()
	require.True(t, succeed, tc.info, info)
}

func Test_IntegerDivFn_Int16(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := tcTemp{
		info: "select int16_col1 DIV int16_col2",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_int16.ToType(),
				[]int16{100, 250, -100}, []bool{false, false, false}),
			NewFunctionTestInput(types.T_int16.ToType(),
				[]int16{3, 7, 9}, []bool{false, false, false}),
		},
		expect: NewFunctionTestResult(types.T_int64.ToType(), false,
			[]int64{33, 35, -11}, []bool{false, false, false}),
	}
	tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
	succeed, info := tcc.Run()
	require.True(t, succeed, tc.info, info)
}

func Test_IntegerDivFn_Int32(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := tcTemp{
		info: "select int32_col1 DIV int32_col2",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_int32.ToType(),
				[]int32{1000, -2500}, []bool{false, false}),
			NewFunctionTestInput(types.T_int32.ToType(),
				[]int32{7, 13}, []bool{false, false}),
		},
		expect: NewFunctionTestResult(types.T_int64.ToType(), false,
			[]int64{142, -192}, []bool{false, false}),
	}
	tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
	succeed, info := tcc.Run()
	require.True(t, succeed, tc.info, info)
}

func Test_IntegerDivFn_Int64(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Normal division
	{
		tc := tcTemp{
			info: "select int64_col1 DIV int64_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1000000, -50000, 7}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{7, 13, 2}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{142857, -3846, 3}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Division by zero returns NULL
	{
		tc := tcTemp{
			info: "select int64_col DIV 0",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{100}, []bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{0}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{0}, []bool{true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// NULL input
	{
		tc := tcTemp{
			info: "select int64_col DIV int64_col with NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{10, 0}, []bool{false, true}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{3, 5}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{3, 0}, []bool{false, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func Test_IntegerDivFn_Uint8(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := tcTemp{
		info: "select uint8_col1 DIV uint8_col2",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_uint8.ToType(),
				[]uint8{10, 25, 100, 7}, []bool{false, false, false, false}),
			NewFunctionTestInput(types.T_uint8.ToType(),
				[]uint8{3, 7, 9, 2}, []bool{false, false, false, false}),
		},
		expect: NewFunctionTestResult(types.T_int64.ToType(), false,
			[]int64{3, 3, 11, 3}, []bool{false, false, false, false}),
	}
	tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
	succeed, info := tcc.Run()
	require.True(t, succeed, tc.info, info)
}

func Test_IntegerDivFn_Uint16(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := tcTemp{
		info: "select uint16_col1 DIV uint16_col2",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_uint16.ToType(),
				[]uint16{1000, 500}, []bool{false, false}),
			NewFunctionTestInput(types.T_uint16.ToType(),
				[]uint16{7, 13}, []bool{false, false}),
		},
		expect: NewFunctionTestResult(types.T_int64.ToType(), false,
			[]int64{142, 38}, []bool{false, false}),
	}
	tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
	succeed, info := tcc.Run()
	require.True(t, succeed, tc.info, info)
}

func Test_IntegerDivFn_Uint32(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := tcTemp{
		info: "select uint32_col1 DIV uint32_col2",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_uint32.ToType(),
				[]uint32{100000, 50000}, []bool{false, false}),
			NewFunctionTestInput(types.T_uint32.ToType(),
				[]uint32{7, 13}, []bool{false, false}),
		},
		expect: NewFunctionTestResult(types.T_int64.ToType(), false,
			[]int64{14285, 3846}, []bool{false, false}),
	}
	tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
	succeed, info := tcc.Run()
	require.True(t, succeed, tc.info, info)
}

func Test_IntegerDivFn_Uint64(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Normal division
	{
		tc := tcTemp{
			info: "select uint64_col1 DIV uint64_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{1000000, 50000}, []bool{false, false}),
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{7, 13}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{142857, 3846}, []bool{false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Division by zero returns NULL
	{
		tc := tcTemp{
			info: "select uint64_col DIV 0",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{100}, []bool{false}),
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{0}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{0}, []bool{true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Uint64 overflow (result > MaxInt64) should error
	{
		tc := tcTemp{
			info: "select large_uint64 DIV 1 overflow",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{^uint64(0)}, []bool{false}), // MaxUint64
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{1}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{0}, []bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		_, _ = tcc.Run() // Expected to return error (overflow)
	}
}

func Test_IntegerDivFn_Decimal64(t *testing.T) {
	proc := testutil.NewProcess(t)

	d64Type := types.T_decimal64.ToType()
	d64Type.Scale = 2

	// 10.50 DIV 3.00 = 3
	v1, _ := types.Decimal64FromFloat64(10.50, 18, 2)
	v2, _ := types.Decimal64FromFloat64(3.00, 18, 2)

	tc := tcTemp{
		info: "select decimal64_col1 DIV decimal64_col2",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(d64Type, []types.Decimal64{v1}, []bool{false}),
			NewFunctionTestInput(d64Type, []types.Decimal64{v2}, []bool{false}),
		},
		expect: NewFunctionTestResult(types.T_int64.ToType(), false,
			[]int64{3}, []bool{false}),
	}
	tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
	succeed, info := tcc.Run()
	require.True(t, succeed, tc.info, info)
}

func Test_IntegerDivFn_Decimal128(t *testing.T) {
	proc := testutil.NewProcess(t)

	d128Type := types.T_decimal128.ToType()
	d128Type.Scale = 2

	v1, _ := types.Decimal128FromFloat64(100.50, 38, 2)
	v2, _ := types.Decimal128FromFloat64(7.00, 38, 2)

	tc := tcTemp{
		info: "select decimal128_col1 DIV decimal128_col2",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(d128Type, []types.Decimal128{v1}, []bool{false}),
			NewFunctionTestInput(d128Type, []types.Decimal128{v2}, []bool{false}),
		},
		expect: NewFunctionTestResult(types.T_int64.ToType(), false,
			[]int64{14}, []bool{false}),
	}
	tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
	succeed, info := tcc.Run()
	require.True(t, succeed, tc.info, info)
}

func Test_IntegerDivFn_Decimal64_DivByZero(t *testing.T) {
	proc := testutil.NewProcess(t)

	d64Type := types.T_decimal64.ToType()
	d64Type.Scale = 2

	v1, _ := types.Decimal64FromFloat64(10.50, 18, 2)
	v2 := types.Decimal64(0) // zero

	tc := tcTemp{
		info: "select decimal64_col DIV 0",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(d64Type, []types.Decimal64{v1}, []bool{false}),
			NewFunctionTestInput(d64Type, []types.Decimal64{v2}, []bool{false}),
		},
		expect: NewFunctionTestResult(types.T_int64.ToType(), false,
			[]int64{0}, []bool{true}), // NULL for div by zero
	}
	tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
	succeed, info := tcc.Run()
	require.True(t, succeed, tc.info, info)
}

func Test_IntegerDivFn_Decimal128_DivByZero(t *testing.T) {
	proc := testutil.NewProcess(t)

	d128Type := types.T_decimal128.ToType()
	d128Type.Scale = 2

	v1, _ := types.Decimal128FromFloat64(10.50, 38, 2)
	v2 := types.Decimal128{B0_63: 0, B64_127: 0} // zero

	tc := tcTemp{
		info: "select decimal128_col DIV 0",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(d128Type, []types.Decimal128{v1}, []bool{false}),
			NewFunctionTestInput(d128Type, []types.Decimal128{v2}, []bool{false}),
		},
		expect: NewFunctionTestResult(types.T_int64.ToType(), false,
			[]int64{0}, []bool{true}), // NULL for div by zero
	}
	tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
	succeed, info := tcc.Run()
	require.True(t, succeed, tc.info, info)
}

func Test_DecimalIsZero(t *testing.T) {
	require.True(t, decimalIsZero(types.Decimal64(0)))
	require.False(t, decimalIsZero(types.Decimal64(100)))

	require.True(t, decimalIsZero(types.Decimal128{B0_63: 0, B64_127: 0}))
	require.False(t, decimalIsZero(types.Decimal128{B0_63: 1, B64_127: 0}))
	require.False(t, decimalIsZero(types.Decimal128{B0_63: 0, B64_127: 1}))
}

func Test_Decimal128ToInt64(t *testing.T) {
	// Positive value
	val, err := decimal128ToInt64(types.Decimal128{B0_63: 42, B64_127: 0})
	require.NoError(t, err)
	require.Equal(t, int64(42), val)

	// Zero
	val, err = decimal128ToInt64(types.Decimal128{B0_63: 0, B64_127: 0})
	require.NoError(t, err)
	require.Equal(t, int64(0), val)

	// Negative value: -5 in two's complement Decimal128
	neg5 := types.Decimal128{B0_63: ^uint64(4), B64_127: ^uint64(0)} // -5
	val, err = decimal128ToInt64(neg5)
	require.NoError(t, err)
	require.Equal(t, int64(-5), val)

	// Overflow: too large positive
	_, err = decimal128ToInt64(types.Decimal128{B0_63: 0, B64_127: 1})
	require.Error(t, err)

	// Overflow: positive value > MaxInt64
	_, err = decimal128ToInt64(types.Decimal128{B0_63: 0x8000000000000000, B64_127: 0})
	require.Error(t, err)
}

func Test_IntegerDivOperatorSupports(t *testing.T) {
	// Supported same-type pairs
	for _, oid := range []types.T{
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
	} {
		require.True(t, integerDivOperatorSupports(oid.ToType(), oid.ToType()),
			"should support %v DIV %v", oid, oid)
	}

	// Unsupported: different types
	require.False(t, integerDivOperatorSupports(types.T_int32.ToType(), types.T_int64.ToType()))

	// Unsupported: string types
	require.False(t, integerDivOperatorSupports(types.T_varchar.ToType(), types.T_varchar.ToType()))
}
