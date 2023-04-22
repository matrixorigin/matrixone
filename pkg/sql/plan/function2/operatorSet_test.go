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

package function2

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"testing"
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
	tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, operatorUnaryTilde[int64])
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
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, operatorUnaryMinus[int64])
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
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, operatorUnaryMinusDecimal64)
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
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, operatorUnaryMinusDecimal128)
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
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, operatorUnaryPlus[int64])
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}
