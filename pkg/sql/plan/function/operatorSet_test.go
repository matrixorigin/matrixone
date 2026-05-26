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

func Test_CaseCheck_MixedStringNumeric(t *testing.T) {
	inputs := []types.Type{
		types.T_bool.ToType(),
		types.New(types.T_varchar, 11, 0),
		types.T_int32.ToType(),
	}
	result := caseCheck(nil, inputs)
	require.Equal(t, succeedWithCast, result.status)
	require.Len(t, result.finalType, 3)
	require.Equal(t, types.T_bool.ToType(), result.finalType[0])
	require.True(t, result.finalType[1].Oid.IsMySQLString())
	require.True(t, result.finalType[2].Oid.IsMySQLString())
	require.Equal(t, int32(types.MaxVarBinaryLen), result.finalType[1].Width)
	require.Equal(t, int32(types.MaxVarBinaryLen), result.finalType[2].Width)
}

func Test_CaseCheck_DifferentDecimalScale(t *testing.T) {
	inputs := []types.Type{
		types.T_bool.ToType(),
		types.New(types.T_decimal128, 23, 2),
		types.New(types.T_decimal128, 38, 7),
	}

	result := caseCheck(nil, inputs)
	require.Equal(t, succeedWithCast, result.status)
	require.Len(t, result.finalType, len(inputs))
	require.Equal(t, types.T_bool.ToType(), result.finalType[0])
	require.Equal(t, types.T_decimal128, result.finalType[1].Oid)
	require.Equal(t, int32(38), result.finalType[1].Width)
	require.Equal(t, int32(7), result.finalType[1].Scale)
	require.Equal(t, types.T_decimal128, result.finalType[2].Oid)
	require.Equal(t, int32(38), result.finalType[2].Width)
	require.Equal(t, int32(7), result.finalType[2].Scale)
}

func Test_CaseCheck_MixedDecimalFamilyScale(t *testing.T) {
	inputs := []types.Type{
		types.T_bool.ToType(),
		types.New(types.T_decimal64, 18, 6),
		types.New(types.T_decimal128, 20, 0),
	}

	result := caseCheck(nil, inputs)
	require.Equal(t, succeedWithCast, result.status)
	require.Len(t, result.finalType, len(inputs))
	require.Equal(t, types.T_bool.ToType(), result.finalType[0])
	require.Equal(t, types.T_decimal128, result.finalType[1].Oid)
	require.Equal(t, int32(26), result.finalType[1].Width)
	require.Equal(t, int32(6), result.finalType[1].Scale)
	require.Equal(t, types.T_decimal128, result.finalType[2].Oid)
	require.Equal(t, int32(26), result.finalType[2].Width)
	require.Equal(t, int32(6), result.finalType[2].Scale)
}

func Test_CaseCheck_DecimalWithIntegerPromotesForIntegralWidth(t *testing.T) {
	inputs := []types.Type{
		types.T_bool.ToType(),
		types.New(types.T_decimal64, 18, 0),
		types.T_int64.ToType(),
	}

	result := caseCheck(nil, inputs)
	require.Equal(t, succeedWithCast, result.status)
	require.Len(t, result.finalType, len(inputs))
	require.Equal(t, types.T_bool.ToType(), result.finalType[0])
	require.Equal(t, types.T_decimal128, result.finalType[1].Oid)
	require.Equal(t, int32(20), result.finalType[1].Width)
	require.Equal(t, int32(0), result.finalType[1].Scale)
	require.Equal(t, types.T_decimal128, result.finalType[2].Oid)
	require.Equal(t, int32(20), result.finalType[2].Width)
	require.Equal(t, int32(0), result.finalType[2].Scale)
}

func Test_CaseCheck_Decimal64PromotesForRequiredWidth(t *testing.T) {
	inputs := []types.Type{
		types.T_bool.ToType(),
		types.New(types.T_decimal64, 18, 0),
		types.New(types.T_decimal64, 18, 2),
	}

	result := caseCheck(nil, inputs)
	require.Equal(t, succeedWithCast, result.status)
	require.Len(t, result.finalType, len(inputs))
	require.Equal(t, types.T_bool.ToType(), result.finalType[0])
	require.Equal(t, types.T_decimal128, result.finalType[1].Oid)
	require.Equal(t, int32(20), result.finalType[1].Width)
	require.Equal(t, int32(2), result.finalType[1].Scale)
	require.Equal(t, types.T_decimal128, result.finalType[2].Oid)
	require.Equal(t, int32(20), result.finalType[2].Width)
	require.Equal(t, int32(2), result.finalType[2].Scale)
}

func Test_CaseCheck_Decimal64PromotesForLargeScale(t *testing.T) {
	inputs := []types.Type{
		types.T_bool.ToType(),
		types.New(types.T_decimal64, 18, 0),
		types.New(types.T_decimal64, 18, 18),
	}

	result := caseCheck(nil, inputs)
	require.Equal(t, succeedWithCast, result.status)
	require.Len(t, result.finalType, len(inputs))
	require.Equal(t, types.T_bool.ToType(), result.finalType[0])
	require.Equal(t, types.T_decimal128, result.finalType[1].Oid)
	require.Equal(t, int32(36), result.finalType[1].Width)
	require.Equal(t, int32(18), result.finalType[1].Scale)
	require.Equal(t, types.T_decimal128, result.finalType[2].Oid)
	require.Equal(t, int32(36), result.finalType[2].Width)
	require.Equal(t, int32(18), result.finalType[2].Scale)
}

func Test_CaseCheck_Decimal128PromotesToDecimal256(t *testing.T) {
	inputs := []types.Type{
		types.T_bool.ToType(),
		types.New(types.T_decimal128, 38, 0),
		types.New(types.T_decimal128, 38, 38),
	}

	result := caseCheck(nil, inputs)
	require.Equal(t, succeedWithCast, result.status)
	require.Len(t, result.finalType, len(inputs))
	require.Equal(t, types.T_bool.ToType(), result.finalType[0])
	require.Equal(t, types.T_decimal256, result.finalType[1].Oid)
	require.Equal(t, int32(76), result.finalType[1].Width)
	require.Equal(t, int32(38), result.finalType[1].Scale)
	require.Equal(t, types.T_decimal256, result.finalType[2].Oid)
	require.Equal(t, int32(76), result.finalType[2].Width)
	require.Equal(t, int32(38), result.finalType[2].Scale)
}

func Test_CaseCheck_Decimal256OverflowFails(t *testing.T) {
	inputs := []types.Type{
		types.T_bool.ToType(),
		types.New(types.T_decimal256, 76, 0),
		types.New(types.T_decimal256, 76, 76),
	}

	result := caseCheck(nil, inputs)
	require.Equal(t, failedFunctionParametersWrong, result.status)
}

func Test_IffCheck_DifferentDecimalScale(t *testing.T) {
	inputs := []types.Type{
		types.T_bool.ToType(),
		types.New(types.T_decimal128, 23, 2),
		types.New(types.T_decimal128, 38, 7),
	}

	result := iffCheck(nil, inputs)
	require.Equal(t, succeedWithCast, result.status)
	require.Len(t, result.finalType, len(inputs))
	require.Equal(t, types.T_bool.ToType(), result.finalType[0])
	require.Equal(t, types.T_decimal128, result.finalType[1].Oid)
	require.Equal(t, int32(38), result.finalType[1].Width)
	require.Equal(t, int32(7), result.finalType[1].Scale)
	require.Equal(t, types.T_decimal128, result.finalType[2].Oid)
	require.Equal(t, int32(38), result.finalType[2].Width)
	require.Equal(t, int32(7), result.finalType[2].Scale)
}

func Test_IffCheck_MixedDecimalFamilyScale(t *testing.T) {
	inputs := []types.Type{
		types.T_bool.ToType(),
		types.New(types.T_decimal64, 18, 6),
		types.New(types.T_decimal128, 20, 0),
	}

	result := iffCheck(nil, inputs)
	require.Equal(t, succeedWithCast, result.status)
	require.Len(t, result.finalType, len(inputs))
	require.Equal(t, types.T_bool.ToType(), result.finalType[0])
	require.Equal(t, types.T_decimal128, result.finalType[1].Oid)
	require.Equal(t, int32(26), result.finalType[1].Width)
	require.Equal(t, int32(6), result.finalType[1].Scale)
	require.Equal(t, types.T_decimal128, result.finalType[2].Oid)
	require.Equal(t, int32(26), result.finalType[2].Width)
	require.Equal(t, int32(6), result.finalType[2].Scale)
}

func Test_IffCheck_DecimalWithIntegerPromotesForIntegralWidth(t *testing.T) {
	inputs := []types.Type{
		types.T_bool.ToType(),
		types.New(types.T_decimal64, 18, 0),
		types.T_int64.ToType(),
	}

	result := iffCheck(nil, inputs)
	require.Equal(t, succeedWithCast, result.status)
	require.Len(t, result.finalType, len(inputs))
	require.Equal(t, types.T_bool.ToType(), result.finalType[0])
	require.Equal(t, types.T_decimal128, result.finalType[1].Oid)
	require.Equal(t, int32(20), result.finalType[1].Width)
	require.Equal(t, int32(0), result.finalType[1].Scale)
	require.Equal(t, types.T_decimal128, result.finalType[2].Oid)
	require.Equal(t, int32(20), result.finalType[2].Width)
	require.Equal(t, int32(0), result.finalType[2].Scale)
}

func Test_IffCheck_Decimal64PromotesForLargeScale(t *testing.T) {
	inputs := []types.Type{
		types.T_bool.ToType(),
		types.New(types.T_decimal64, 18, 0),
		types.New(types.T_decimal64, 18, 18),
	}

	result := iffCheck(nil, inputs)
	require.Equal(t, succeedWithCast, result.status)
	require.Len(t, result.finalType, len(inputs))
	require.Equal(t, types.T_bool.ToType(), result.finalType[0])
	require.Equal(t, types.T_decimal128, result.finalType[1].Oid)
	require.Equal(t, int32(36), result.finalType[1].Width)
	require.Equal(t, int32(18), result.finalType[1].Scale)
	require.Equal(t, types.T_decimal128, result.finalType[2].Oid)
	require.Equal(t, int32(36), result.finalType[2].Width)
	require.Equal(t, int32(18), result.finalType[2].Scale)
}

func Test_CoalesceCheck_MixedStringNumeric(t *testing.T) {
	overloads := []overload{
		{args: []types.T{types.T_varchar}},
		{args: []types.T{types.T_char}},
	}
	inputs := []types.Type{
		types.New(types.T_varchar, 7, 0),
		types.T_int32.ToType(),
		types.New(types.T_char, 13, 0),
		types.T_float64.ToType(),
	}
	result := coalesceCheck(overloads, inputs)
	require.Equal(t, succeedWithCast, result.status)
	require.Equal(t, 0, result.idx)
	require.Len(t, result.finalType, len(inputs))
	for _, typ := range result.finalType {
		require.True(t, typ.Oid.IsMySQLString())
		require.Equal(t, int32(types.MaxVarBinaryLen), typ.Width)
	}
}

func Test_CaseFn_Decimal256Execution(t *testing.T) {
	proc := testutil.NewProcess(t)
	d1, err := types.ParseDecimal256("111111111111111111111111111111111111111", 76, 0)
	require.NoError(t, err)
	d2, err := types.ParseDecimal256("999999999999999999999999999999999999999", 76, 0)
	require.NoError(t, err)
	retType := types.New(types.T_decimal256, 76, 0)
	// CASE WHEN TRUE THEN d1 ELSE d2 → row0=d1; CASE WHEN FALSE THEN d1 ELSE d2 → row1=d2
	tc := tcTemp{
		info: "caseFn decimal256: CASE WHEN c THEN d1 ELSE d2",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_bool.ToType(), []bool{true, false}, nil),
			NewFunctionTestInput(retType, []types.Decimal256{d1, d1}, nil),
			NewFunctionTestInput(retType, []types.Decimal256{d2, d2}, nil),
		},
		expect: NewFunctionTestResult(retType, false,
			[]types.Decimal256{d1, d2}, nil),
	}
	tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, caseFn)
	succeed, info := tcc.Run()
	require.True(t, succeed, tc.info, info)
}

func Test_IffFn_Decimal256Execution(t *testing.T) {
	proc := testutil.NewProcess(t)
	d1, err := types.ParseDecimal256("123456789012345678901234567890123456789", 76, 0)
	require.NoError(t, err)
	d2, err := types.ParseDecimal256("987654321098765432109876543210987654321", 76, 0)
	require.NoError(t, err)
	retType := types.New(types.T_decimal256, 76, 0)
	// IFF(TRUE, d1, d2) → d1; IFF(FALSE, d1, d2) → d2
	tc := tcTemp{
		info: "iffFn decimal256: IFF(c, d1, d2)",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_bool.ToType(), []bool{true, false}, nil),
			NewFunctionTestInput(retType, []types.Decimal256{d1, d1}, nil),
			NewFunctionTestInput(retType, []types.Decimal256{d2, d2}, nil),
		},
		expect: NewFunctionTestResult(retType, false,
			[]types.Decimal256{d1, d2}, nil),
	}
	tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, iffFn)
	succeed, info := tcc.Run()
	require.True(t, succeed, tc.info, info)
}

func Test_CaseWhen_WithNullAndStringComparison(t *testing.T) {
	// Test CASE WHEN with NULL value compared to string
	// This should not error, matching MySQL behavior
	proc := testutil.NewProcess(t)

	// Test: CASE 1/0 WHEN 'a' THEN 'true' ELSE 'false' END
	// 1/0 returns NULL, NULL = 'a' should return NULL (false in bool context)
	// So result should be 'false' (from ELSE clause)
	tc := tcTemp{
		info: "CASE NULL WHEN 'a' THEN 'true' ELSE 'false' END",
		inputs: []FunctionTestInput{
			// Condition: NULL = 'a' -> false
			NewFunctionTestInput(types.T_bool.ToType(),
				[]bool{false}, []bool{false}),
			// THEN value
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"true"}, []bool{false}),
			// ELSE value
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"false"}, []bool{false}),
		},
		expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
			[]string{"false"}, []bool{false}),
	}

	tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, strCaseFn)
	succeed, info := tcc.Run()
	require.True(t, succeed, tc.info, info)
}
