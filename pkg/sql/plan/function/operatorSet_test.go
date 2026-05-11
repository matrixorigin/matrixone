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
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func Test_Operator_Unary_Tilde(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := tcTemp{
		info: "select unary_tilde(num) with num = 5, -5, null",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{5, -5, 0}, []bool{false, false, true}),
		},
		expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
			[]uint64{18446744073709551610, 4, 0}, []bool{false, false, true}),
	}
	tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, operatorUnaryTilde[int64])
	succeed, info := tcc.Run()
	require.True(t, succeed, tc.info, info)
}

func Test_Operator_Unary_Minus(t *testing.T) {
	proc := testutil.NewProcess(t)
	{
		tc := tcTemp{
			info: "select -(num) with num = 5, -5, null",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{5, -5, 0}, []bool{false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{-5, 5, 0}, []bool{false, false, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, operatorUnaryMinus[int64])
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "select -(decimal64) with num = 123, 234, 345, null",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_decimal64.ToType(),
					[]types.Decimal64{123, 234, 345, 0}, []bool{false, false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_decimal64.ToType(), false,
				[]types.Decimal64{types.Decimal64(123).Minus(), types.Decimal64(234).Minus(), types.Decimal64(345).Minus(), types.Decimal64(0)}, []bool{false, false, false, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, operatorUnaryMinusDecimal64)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "select -(decimal128) with num = 123, 234, 345, null",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_decimal128.ToType(),
					[]types.Decimal128{
						{B0_63: 123, B64_127: 0},
						{B0_63: 234, B64_127: 0},
						{B0_63: 345, B64_127: 0},
						{B0_63: 0, B64_127: 0},
					},
					[]bool{false, false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{
					types.Decimal128{B0_63: 123, B64_127: 0}.Minus(),
					types.Decimal128{B0_63: 234, B64_127: 0}.Minus(),
					types.Decimal128{B0_63: 345, B64_127: 0}.Minus(),
					{B0_63: 0, B64_127: 0},
				},
				[]bool{false, false, false, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, operatorUnaryMinusDecimal128)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func Test_Operator_Unary_Plus(t *testing.T) {
	proc := testutil.NewProcess(t)
	{
		tc := tcTemp{
			info: "select +(num) with num = 5, -5, null",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{5, -5, 0}, []bool{false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{5, -5, 0}, []bool{false, false, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, operatorUnaryPlus[int64])
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func Test_Operator_Is(t *testing.T) {
	proc := testutil.NewProcess(t)
	{
		tc := tcTemp{
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, false}, []bool{false, false, true}),
				NewFunctionTestConstInput(types.T_bool.ToType(),
					[]bool{true}, nil),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, false}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, operatorOpIs)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, false}, []bool{false, false, true}),
				NewFunctionTestConstInput(types.T_bool.ToType(),
					[]bool{false}, nil),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, true, false}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, operatorOpIs)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func Test_Operator_Is_Not(t *testing.T) {
	proc := testutil.NewProcess(t)
	{
		tc := tcTemp{
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, true, false, false, true}, nil),
				NewFunctionTestConstInput(types.T_bool.ToType(),
					[]bool{true}, nil),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, true, false, true, true, false}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, operatorOpIsNot)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, true, false, false, true}, nil),
				NewFunctionTestConstInput(types.T_bool.ToType(),
					[]bool{false}, nil),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, true, false, false, true}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, operatorOpIsNot)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{false}, []bool{true}),
				NewFunctionTestConstInput(types.T_bool.ToType(),
					[]bool{false}, nil),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, operatorOpIsNot)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func Test_Operator_Is_True(t *testing.T) {
	proc := testutil.NewProcess(t)
	{
		tc := tcTemp{
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, false}, []bool{false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, false}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, operatorIsTrue)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func Test_Operator_Is_Not_True(t *testing.T) {
	proc := testutil.NewProcess(t)
	{
		tc := tcTemp{
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, false}, []bool{false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, true, true}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, operatorIsNotTrue)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func Test_Operator_Is_False(t *testing.T) {
	proc := testutil.NewProcess(t)
	{
		tc := tcTemp{
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, false}, []bool{false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, true, false}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, operatorIsFalse)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func Test_Operator_Is_Not_False(t *testing.T) {
	proc := testutil.NewProcess(t)
	{
		tc := tcTemp{
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, false}, []bool{false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, true}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, operatorIsNotFalse)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func Test_Operator_Is_Null(t *testing.T) {
	proc := testutil.NewProcess(t)
	{
		tc := tcTemp{
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, false}, []bool{false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, false, true}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, operatorOpIsNull)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func Test_Operator_Is_Not_Null(t *testing.T) {
	proc := testutil.NewProcess(t)
	{
		tc := tcTemp{
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, false}, []bool{false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, true, false}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, operatorOpIsNotNull)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func Test_Operator_And(t *testing.T) {
	proc := testutil.NewProcess(t)
	{
		tc := tcTemp{
			inputs: []FunctionTestInput{
				// 3 true + 3 false + 3 null
				NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, true, true, false, false, false, false, false, false},
					[]bool{false, false, false, false, false, false, true, true, true},
				),

				// 3 loop of `true, false, null`
				NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, false, true, false, false, true, false, false},
					[]bool{false, false, true, false, false, true, false, false, true},
				),
			},
			expect: NewFunctionTestResult(
				types.T_bool.ToType(), false,
				[]bool{true, false, false, false, false, false, false, false, false},
				[]bool{false, false, true, false, false, false, true, false, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, opMultiAnd)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func Test_Operator_Or(t *testing.T) {
	proc := testutil.NewProcess(t)
	{
		tc := tcTemp{
			inputs: []FunctionTestInput{
				// 3 true + 3 false + 3 null
				NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, true, true, false, false, false, false, false, false},
					[]bool{false, false, false, false, false, false, true, true, true},
				),

				// 3 loop of `true, false, null`
				NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, false, true, false, false, true, false, false},
					[]bool{false, false, true, false, false, true, false, false, true},
				),
			},
			expect: NewFunctionTestResult(
				types.T_bool.ToType(), false,
				[]bool{true, true, true, true, false, false, true, false, false},
				[]bool{false, false, false, false, false, true, false, true, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, opMultiOr)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func Test_Operator_Xor(t *testing.T) {
	proc := testutil.NewProcess(t)
	{
		tc := tcTemp{
			inputs: []FunctionTestInput{
				// 3 true + 3 false + 3 null
				NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, true, true, false, false, false, false, false, false},
					[]bool{false, false, false, false, false, false, true, true, true},
				),

				// 3 loop of `true, false, null`
				NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, false, true, false, false, true, false, false},
					[]bool{false, false, true, false, false, true, false, false, true},
				),
			},
			expect: NewFunctionTestResult(
				types.T_bool.ToType(), false,
				[]bool{false, true, false, true, false, false, false, false, false},
				[]bool{false, false, true, false, false, true, true, true, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, xorFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test for issue #19998: IF with mixed string and numeric types
// This tests that IF(condition, 'string', number) returns varchar type
func Test_IffCheck_MixedTypes(t *testing.T) {
	// Test case 1: IF(condition, string, int) should return varchar
	{
		inputs := []types.Type{
			types.T_bool.ToType(),
			types.T_varchar.ToType(),
			types.T_int32.ToType(),
		}
		result := iffCheck(nil, inputs)
		require.True(t, result.status != failedFunctionParametersWrong, "iffCheck should accept mixed string/int types")
		require.True(t, len(result.finalType) > 0, "should have final types")
		// The return type should be varchar or char (string types first in retOperatorIffSupports after fix)
		require.True(t, result.finalType[1].Oid.IsMySQLString(), "return type should be string type for mixed types, got: %v", result.finalType[1].Oid)
		require.True(t, result.finalType[2].Oid.IsMySQLString(), "return type should be string type for mixed types, got: %v", result.finalType[2].Oid)
	}

	// Test case 2: IF(condition, int, string) should also return varchar
	{
		inputs := []types.Type{
			types.T_bool.ToType(),
			types.T_int32.ToType(),
			types.T_varchar.ToType(),
		}
		result := iffCheck(nil, inputs)
		require.True(t, result.status != failedFunctionParametersWrong, "iffCheck should accept mixed int/string types")
		require.True(t, len(result.finalType) > 0, "should have final types")
		require.True(t, result.finalType[1].Oid.IsMySQLString(), "return type should be string type for mixed types, got: %v", result.finalType[1].Oid)
		require.True(t, result.finalType[2].Oid.IsMySQLString(), "return type should be string type for mixed types, got: %v", result.finalType[2].Oid)
	}

	// Test case 3: IF(condition, int, int) should return int
	{
		inputs := []types.Type{
			types.T_bool.ToType(),
			types.T_int32.ToType(),
			types.T_int32.ToType(),
		}
		result := iffCheck(nil, inputs)
		require.True(t, result.status != failedFunctionParametersWrong, "iffCheck should accept same int types")
	}
}
