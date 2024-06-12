// Copyright 2022 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/constraints"
)

func Test_Operator_Unary_Tilde(t *testing.T) {
	proc := testutil.NewProcess()
	tc := tcTemp{
		info: "select unary_tilde(num) with num = 5, -5, null",
		inputs: []testutil.FunctionTestInput{
			testutil.NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{5, -5, 0}, []bool{false, false, true}),
		},
		expect: testutil.NewFunctionTestResult(types.T_uint64.ToType(), false,
			[]uint64{18446744073709551610, 4, 0}, []bool{false, false, true}),
	}
	tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, ToperatorUnaryTilde[int64])
	succeed, info := tcc.Run()
	require.True(t, succeed, tc.info, info)
}

func Test_Operator_Unary_Minus(t *testing.T) {
	proc := testutil.NewProcess()
	{
		tc := tcTemp{
			info: "select -(num) with num = 5, -5, null",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{5, -5, 0}, []bool{false, false, true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{-5, 5, 0}, []bool{false, false, true}),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, ToperatorUnaryMinus[int64])
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "select -(decimal64) with num = 123, 234, 345, null",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_decimal64.ToType(),
					[]types.Decimal64{123, 234, 345, 0}, []bool{false, false, false, true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_decimal64.ToType(), false,
				[]types.Decimal64{types.Decimal64(123).Minus(), types.Decimal64(234).Minus(), types.Decimal64(345).Minus(), types.Decimal64(0)}, []bool{false, false, false, true}),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, ToperatorUnaryMinusDecimal64)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "select -(decimal128) with num = 123, 234, 345, null",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_decimal128.ToType(),
					[]types.Decimal128{
						{B0_63: 123, B64_127: 0},
						{B0_63: 234, B64_127: 0},
						{B0_63: 345, B64_127: 0},
						{B0_63: 0, B64_127: 0},
					},
					[]bool{false, false, false, true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{
					types.Decimal128{B0_63: 123, B64_127: 0}.Minus(),
					types.Decimal128{B0_63: 234, B64_127: 0}.Minus(),
					types.Decimal128{B0_63: 345, B64_127: 0}.Minus(),
					{B0_63: 0, B64_127: 0},
				},
				[]bool{false, false, false, true}),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, ToperatorUnaryMinusDecimal128)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func Test_Operator_Unary_Plus(t *testing.T) {
	proc := testutil.NewProcess()
	{
		tc := tcTemp{
			info: "select +(num) with num = 5, -5, null",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{5, -5, 0}, []bool{false, false, true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{5, -5, 0}, []bool{false, false, true}),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, ToperatorUnaryPlus[int64])
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func Test_Operator_Is(t *testing.T) {
	proc := testutil.NewProcess()
	{
		tc := tcTemp{
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, false}, []bool{false, false, true}),
				testutil.NewFunctionTestConstInput(types.T_bool.ToType(),
					[]bool{true}, nil),
			},
			expect: testutil.NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, false}, nil),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, ToperatorOpIs)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, false}, []bool{false, false, true}),
				testutil.NewFunctionTestConstInput(types.T_bool.ToType(),
					[]bool{false}, nil),
			},
			expect: testutil.NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, true, false}, nil),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, ToperatorOpIs)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func Test_Operator_Is_Not(t *testing.T) {
	proc := testutil.NewProcess()
	{
		tc := tcTemp{
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, true, false, false, true}, nil),
				testutil.NewFunctionTestConstInput(types.T_bool.ToType(),
					[]bool{true}, nil),
			},
			expect: testutil.NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, true, false, true, true, false}, nil),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, ToperatorOpIsNot)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, true, false, false, true}, nil),
				testutil.NewFunctionTestConstInput(types.T_bool.ToType(),
					[]bool{false}, nil),
			},
			expect: testutil.NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, true, false, false, true}, nil),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, ToperatorOpIsNot)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{false}, []bool{true}),
				testutil.NewFunctionTestConstInput(types.T_bool.ToType(),
					[]bool{false}, nil),
			},
			expect: testutil.NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true}, nil),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, ToperatorOpIsNot)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func Test_Operator_Is_True(t *testing.T) {
	proc := testutil.NewProcess()
	{
		tc := tcTemp{
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, false}, []bool{false, false, true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, false}, nil),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, ToperatorIsTrue)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func Test_Operator_Is_Not_True(t *testing.T) {
	proc := testutil.NewProcess()
	{
		tc := tcTemp{
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, false}, []bool{false, false, true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, true, true}, nil),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, ToperatorIsNotTrue)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func Test_Operator_Is_False(t *testing.T) {
	proc := testutil.NewProcess()
	{
		tc := tcTemp{
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, false}, []bool{false, false, true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, true, false}, nil),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, ToperatorIsFalse)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func Test_Operator_Is_Not_False(t *testing.T) {
	proc := testutil.NewProcess()
	{
		tc := tcTemp{
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, false}, []bool{false, false, true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, true}, nil),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, ToperatorIsNotFalse)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func Test_Operator_Is_Null(t *testing.T) {
	proc := testutil.NewProcess()
	{
		tc := tcTemp{
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, false}, []bool{false, false, true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, false, true}, nil),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, ToperatorOpIsNull)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func Test_Operator_Is_Not_Null(t *testing.T) {
	proc := testutil.NewProcess()
	{
		tc := tcTemp{
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, false}, []bool{false, false, true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, true, false}, nil),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, ToperatorOpIsNotNull)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func Test_Operator_And(t *testing.T) {
	proc := testutil.NewProcess()
	{
		tc := tcTemp{
			inputs: []testutil.FunctionTestInput{
				// 3 true + 3 false + 3 null
				testutil.NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, true, true, false, false, false, false, false, false},
					[]bool{false, false, false, false, false, false, true, true, true},
				),

				// 3 loop of `true, false, null`
				testutil.NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, false, true, false, false, true, false, false},
					[]bool{false, false, true, false, false, true, false, false, true},
				),
			},
			expect: testutil.NewFunctionTestResult(
				types.T_bool.ToType(), false,
				[]bool{true, false, false, false, false, false, false, false, false},
				[]bool{false, false, true, false, false, false, true, false, true}),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, TopMultiAnd)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func Test_Operator_Or(t *testing.T) {
	proc := testutil.NewProcess()
	{
		tc := tcTemp{
			inputs: []testutil.FunctionTestInput{
				// 3 true + 3 false + 3 null
				testutil.NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, true, true, false, false, false, false, false, false},
					[]bool{false, false, false, false, false, false, true, true, true},
				),

				// 3 loop of `true, false, null`
				testutil.NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, false, true, false, false, true, false, false},
					[]bool{false, false, true, false, false, true, false, false, true},
				),
			},
			expect: testutil.NewFunctionTestResult(
				types.T_bool.ToType(), false,
				[]bool{true, true, true, true, false, false, true, false, false},
				[]bool{false, false, false, false, false, true, false, true, true}),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, TopMultiOr)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func Test_Operator_Xor(t *testing.T) {
	proc := testutil.NewProcess()
	{
		tc := tcTemp{
			inputs: []testutil.FunctionTestInput{
				// 3 true + 3 false + 3 null
				testutil.NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, true, true, false, false, false, false, false, false},
					[]bool{false, false, false, false, false, false, true, true, true},
				),

				// 3 loop of `true, false, null`
				testutil.NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, false, true, false, false, true, false, false},
					[]bool{false, false, true, false, false, true, false, false, true},
				),
			},
			expect: testutil.NewFunctionTestResult(
				types.T_bool.ToType(), false,
				[]bool{false, true, false, true, false, false, false, false, false},
				[]bool{false, false, true, false, false, true, true, true, true}),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, TxorFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func ToperatorUnaryTilde[T constraints.Integer](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, _ any) (err error) {
	return operatorUnaryTilde[T](ivecs, result, proc, length, nil)
}

func ToperatorUnaryMinus[T constraints.Signed | constraints.Float](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, _ any) (err error) {
	return operatorUnaryMinus[T](ivecs, result, proc, length, nil)
}

func ToperatorUnaryMinusDecimal64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, _ any) (err error) {
	return operatorUnaryMinusDecimal64(ivecs, result, proc, length, nil)
}

func ToperatorUnaryMinusDecimal128(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, _ any) (err error) {
	return operatorUnaryMinusDecimal128(ivecs, result, proc, length, nil)
}

func ToperatorUnaryPlus[T constraints.Integer | constraints.Float | types.Decimal64 | types.Decimal128](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, _ any) (err error) {
	return operatorUnaryPlus[T](ivecs, result, proc, length, nil)
}

func ToperatorOpIs(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, _ any) (err error) {
	return operatorOpIs(ivecs, result, proc, length, nil)
}

func ToperatorOpIsNot(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, _ any) (err error) {
	return operatorOpIsNot(ivecs, result, proc, length, nil)
}

func ToperatorIsTrue(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, _ any) (err error) {
	return operatorIsTrue(ivecs, result, proc, length, nil)
}

func ToperatorIsNotTrue(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, _ any) (err error) {
	return operatorIsNotTrue(ivecs, result, proc, length, nil)
}

func ToperatorIsFalse(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, _ any) (err error) {
	return operatorIsFalse(ivecs, result, proc, length, nil)
}

func ToperatorIsNotFalse(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, _ any) (err error) {
	return operatorIsNotFalse(ivecs, result, proc, length, nil)
}

func ToperatorOpIsNull(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, _ any) (err error) {
	return operatorOpIsNull(ivecs, result, proc, length, nil)
}

func ToperatorOpIsNotNull(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, _ any) (err error) {
	return operatorOpIsNotNull(ivecs, result, proc, length, nil)
}

func TopMultiAnd(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, _ any) (err error) {
	return opMultiAnd(ivecs, result, proc, length, nil)
}

func TopMultiOr(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, _ any) (err error) {
	return opMultiOr(ivecs, result, proc, length, nil)
}

func TxorFn(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, _ any) (err error) {
	return xorFn(ivecs, result, proc, length, nil)
}
