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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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

func TestIffCheck_PreservesSupportedConditionTypes(t *testing.T) {
	tests := []struct {
		name string
		cond types.Type
		want types.Type
	}{
		{"null", types.T_any.ToType(), types.T_bool.ToType()},
		{"int", types.T_int64.ToType(), types.T_int64.ToType()},
		{"float", types.T_float64.ToType(), types.T_float64.ToType()},
		{"decimal", types.New(types.T_decimal128, 20, 4), types.New(types.T_decimal128, 20, 4)},
		{"bit", types.T_bit.ToType(), types.T_bit.ToType()},
		{"varchar", types.T_varchar.ToType(), types.T_varchar.ToType()},
		{"binary", types.T_binary.ToType(), types.T_binary.ToType()},
		{"varbinary", types.T_varbinary.ToType(), types.T_varbinary.ToType()},
		{"blob", types.T_blob.ToType(), types.T_blob.ToType()},
		{"text", types.T_text.ToType(), types.T_text.ToType()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := iffCheck(nil, []types.Type{
				tt.cond,
				types.T_int64.ToType(),
				types.T_int64.ToType(),
			})
			require.NotEqual(t, failedFunctionParametersWrong, result.status)
			if result.status == succeedWithCast {
				require.Equal(t, tt.want, result.finalType[0])
			}
		})
	}
}

func TestIffCheck_PreservesVectorResultTypes(t *testing.T) {
	for _, typ := range []types.Type{
		types.New(types.T_array_float32, 3, 0),
		types.New(types.T_array_float64, 3, 0),
		types.New(types.T_array_bf16, 3, 0),
		types.New(types.T_array_float16, 3, 0),
		types.New(types.T_array_int8, 3, 0),
		types.New(types.T_array_uint8, 3, 0),
	} {
		t.Run(typ.Oid.String(), func(t *testing.T) {
			result := iffCheck(nil, []types.Type{
				types.T_bool.ToType(),
				typ,
				typ,
			})
			require.Equal(t, succeedMatched, result.status)
		})
	}
}

func TestIffCheck_VectorCommonType(t *testing.T) {
	vecf32 := types.New(types.T_array_float32, 2, 0)
	vecf64 := types.New(types.T_array_float64, 2, 0)
	for _, branches := range [][]types.Type{{vecf32, vecf64}, {vecf64, vecf32}} {
		result := iffCheck(nil, []types.Type{types.T_bool.ToType(), branches[0], branches[1]})
		require.Equal(t, succeedWithCast, result.status)
		require.Equal(t, types.T_array_float64, result.finalType[1].Oid)
		require.Equal(t, int32(2), result.finalType[1].Width)
		require.Equal(t, result.finalType[1], result.finalType[2])
	}

	result := iffCheck(nil, []types.Type{
		types.T_bool.ToType(),
		types.New(types.T_array_float32, 2, 0),
		types.New(types.T_array_float32, 3, 0),
	})
	require.Equal(t, failedFunctionParametersWrong, result.status)

	result = iffCheck(nil, []types.Type{types.T_bool.ToType(), vecf64, types.T_any.ToType()})
	require.Equal(t, succeedWithCast, result.status)
	require.Equal(t, vecf64, result.finalType[1])
	require.Equal(t, vecf64, result.finalType[2])

	vectorTypes := []types.T{
		types.T_array_float32,
		types.T_array_float64,
		types.T_array_bf16,
		types.T_array_float16,
		types.T_array_int8,
		types.T_array_uint8,
	}
	for i := 0; i < len(vectorTypes); i++ {
		for j := i + 1; j < len(vectorTypes); j++ {
			for _, branches := range [][2]types.T{
				{vectorTypes[i], vectorTypes[j]},
				{vectorTypes[j], vectorTypes[i]},
			} {
				result = iffCheck(nil, []types.Type{
					types.T_bool.ToType(),
					types.New(branches[0], 2, 0),
					types.New(branches[1], 2, 0),
				})
				if (branches[0] == types.T_array_float32 && branches[1] == types.T_array_float64) ||
					(branches[0] == types.T_array_float64 && branches[1] == types.T_array_float32) {
					require.Equal(t, succeedWithCast, result.status)
					require.Equal(t, types.T_array_float64, result.finalType[1].Oid)
					continue
				}
				require.Equal(t, failedFunctionParametersWrong, result.status,
					"%s and %s must not pick an order-dependent lossy type", branches[0], branches[1])
			}
		}
	}
}

func TestIffConditionTruthyAt(t *testing.T) {
	proc := testutil.NewProcess(t)

	stringVec := newVectorByType(proc.Mp(), types.T_varchar.ToType(),
		[]string{
			"0", "  -2.5 ", "+0x10", "0b1010", "NaN", "1abc", "bad", "",
			".5xyz", "1e2foo", "1eabc", "-0suffix", "   ",
		}, nil)
	for row, want := range []bool{
		false, true, true, true, true, true, false, false,
		true, true, true, false, false,
	} {
		got, err := IffConditionTruthyAt(stringVec, uint64(row), SQLCompatibilityMySQL)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	for _, row := range []uint64{5, 6, 7, 8, 9, 10, 11, 12} {
		_, err := IffConditionTruthyAt(stringVec, row, SQLCompatibilityMatrixOne)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	}

	binaryVec := newVectorByType(proc.Mp(), types.T_varbinary.ToType(),
		[]string{"\x00", "16", "\x10", "0x10\x00"}, nil)
	binaryVec.SetIsBin(true)
	for row, want := range []bool{false, true, true, true} {
		got, err := IffConditionTruthyAt(binaryVec, uint64(row), SQLCompatibilityMySQL)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}

	intVec := newVectorByType(proc.Mp(), types.T_int64.ToType(), []int64{0, -1, 9}, nil)
	intVec.GetNulls().Add(2)
	for row, want := range []bool{false, true, false} {
		got, err := IffConditionTruthyAt(intVec, uint64(row), SQLCompatibilityMySQL)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}

	floatVec := newVectorByType(proc.Mp(), types.T_float64.ToType(), []float64{0, -0.1, 1}, nil)
	floatVec.GetNulls().Add(2)
	for row, want := range []bool{false, true, false} {
		got, err := IffConditionTruthyAt(floatVec, uint64(row), SQLCompatibilityMySQL)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}

	decimalVec := newVectorByType(proc.Mp(), types.New(types.T_decimal64, 10, 1), []types.Decimal64{0, 1, 1}, nil)
	decimalVec.GetNulls().Add(2)
	for row, want := range []bool{false, true, false} {
		got, err := IffConditionTruthyAt(decimalVec, uint64(row), SQLCompatibilityMySQL)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}

	bitVec := newVectorByType(proc.Mp(), types.T_bit.ToType(), []uint64{0, 1, 1}, nil)
	bitVec.GetNulls().Add(2)
	for row, want := range []bool{false, true, false} {
		got, err := IffConditionTruthyAt(bitVec, uint64(row), SQLCompatibilityMySQL)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}

func TestIffFn_StringCondition(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"0", "0x10", "  -2.5 "}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{10, 11, 12}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{20, 21, 22}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{20, 11, 12}, nil), iffFn)
	succeed, info := tc.Run()
	require.True(t, succeed, info)
}

func TestIffFn_VectorResults(t *testing.T) {
	proc := testutil.NewProcess(t)
	tests := []struct {
		name   string
		typ    types.Type
		values any
		want   any
	}{
		{
			name:   "vecf32",
			typ:    types.T_array_float32.ToType(),
			values: [][]float32{{1, 2, 3}, {4, 5, 6}},
			want:   [][]float32{{1, 2, 3}, {8, 9, 10}},
		},
		{
			name:   "vecf64",
			typ:    types.T_array_float64.ToType(),
			values: [][]float64{{1, 2, 3}, {4, 5, 6}},
			want:   [][]float64{{1, 2, 3}, {8, 9, 10}},
		},
		{
			name:   "vecbf16",
			typ:    types.T_array_bf16.ToType(),
			values: [][]types.BF16{{1, 2, 3}, {4, 5, 6}},
			want:   [][]types.BF16{{1, 2, 3}, {8, 9, 10}},
		},
		{
			name:   "vecf16",
			typ:    types.T_array_float16.ToType(),
			values: [][]types.Float16{{1, 2, 3}, {4, 5, 6}},
			want:   [][]types.Float16{{1, 2, 3}, {8, 9, 10}},
		},
		{
			name:   "vecint8",
			typ:    types.T_array_int8.ToType(),
			values: [][]int8{{-128, 2, 127}, {4, 5, 6}},
			want:   [][]int8{{-128, 2, 127}, {8, 9, 10}},
		},
		{
			name:   "vecuint8",
			typ:    types.T_array_uint8.ToType(),
			values: [][]uint8{{0, 2, 255}, {4, 5, 6}},
			want:   [][]uint8{{0, 2, 255}, {8, 9, 10}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			falseValues := any(nil)
			switch tt.typ.Oid {
			case types.T_array_float32:
				falseValues = [][]float32{{7, 8, 9}, {8, 9, 10}}
			case types.T_array_float64:
				falseValues = [][]float64{{7, 8, 9}, {8, 9, 10}}
			case types.T_array_bf16:
				falseValues = [][]types.BF16{{7, 8, 9}, {8, 9, 10}}
			case types.T_array_float16:
				falseValues = [][]types.Float16{{7, 8, 9}, {8, 9, 10}}
			case types.T_array_int8:
				falseValues = [][]int8{{7, 8, 9}, {8, 9, 10}}
			case types.T_array_uint8:
				falseValues = [][]uint8{{7, 8, 9}, {8, 9, 10}}
			}
			tc := NewFunctionTestCase(proc,
				[]FunctionTestInput{
					NewFunctionTestInput(types.T_bool.ToType(), []bool{true, false}, nil),
					NewFunctionTestInput(tt.typ, tt.values, nil),
					NewFunctionTestInput(tt.typ, falseValues, nil),
				},
				NewFunctionTestResult(tt.typ, false, tt.want, nil), iffFn)
			succeed, info := tc.Run()
			require.True(t, succeed, info)
		})
	}
}

func TestIffFn_DecimalConditionBatch(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.New(types.T_decimal64, 10, 1), []types.Decimal64{0, 1, 1}, []bool{false, false, true}),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{10, 11, 12}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{20, 21, 22}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{20, 11, 22}, nil), iffFn)
	succeed, info := tc.Run()
	require.True(t, succeed, info)
}

func TestIffFn_SkipsInactiveRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"0x10", "bad", "0"}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{10, 11, 12}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{20, 21, 22}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, nil, nil), iffFn)

	require.NoError(t, tc.result.PreExtendAndReset(tc.fnLength))
	err := iffFn(tc.parameters, tc.result, proc, tc.fnLength,
		&FunctionSelectList{AnyNull: true, SelectList: []bool{true, false, true}})
	require.NoError(t, err)
	result := tc.result.GetResultVector()
	require.Equal(t, tc.fnLength, result.Length())
	require.Equal(t, int64(10), vector.GetFixedAtNoTypeCheck[int64](result, 0))
	require.True(t, result.GetNulls().Contains(1))
	require.Equal(t, int64(22), vector.GetFixedAtNoTypeCheck[int64](result, 2))

	require.NoError(t, tc.result.PreExtendAndReset(tc.fnLength))
	err = iffFn(tc.parameters, tc.result, proc, tc.fnLength, &FunctionSelectList{AllNull: true})
	require.NoError(t, err)
	result = tc.result.GetResultVector()
	require.Equal(t, tc.fnLength, result.Length())
	for i := uint64(0); i < uint64(tc.fnLength); i++ {
		require.True(t, result.GetNulls().Contains(i))
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
	require.Equal(t, int32(19), result.finalType[1].Width)
	require.Equal(t, int32(0), result.finalType[1].Scale)
	require.Equal(t, types.T_decimal128, result.finalType[2].Oid)
	require.Equal(t, int32(19), result.finalType[2].Width)
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

func Test_SetSafeDecimalWidthAndScaleFromSourceBranches(t *testing.T) {
	nonDecimal := types.T_varchar.ToType()
	originalNonDecimal := nonDecimal
	require.True(t, setSafeDecimalWidthAndScaleFromSource(&nonDecimal, []types.Type{
		types.New(types.T_decimal64, 18, 2),
	}))
	require.Equal(t, originalNonDecimal, nonDecimal)

	noDecimalOrInteger := types.New(types.T_decimal64, 18, 0)
	originalNoDecimalOrInteger := noDecimalOrInteger
	require.True(t, setSafeDecimalWidthAndScaleFromSource(&noDecimalOrInteger, []types.Type{
		types.T_bool.ToType(),
		types.T_varchar.ToType(),
	}))
	require.Equal(t, originalNoDecimalOrInteger, noDecimalOrInteger)

	decimalScaleLargerThanWidth := types.New(types.T_decimal64, 18, 0)
	require.True(t, setSafeDecimalWidthAndScaleFromSource(&decimalScaleLargerThanWidth, []types.Type{
		types.New(types.T_decimal64, 2, 5),
	}))
	require.Equal(t, types.T_decimal64, decimalScaleLargerThanWidth.Oid)
	require.Equal(t, int32(5), decimalScaleLargerThanWidth.Width)
	require.Equal(t, int32(5), decimalScaleLargerThanWidth.Scale)
}

func Test_IntegerIntegralWidth(t *testing.T) {
	tests := []struct {
		oid  types.T
		want int32
	}{
		{types.T_int8, 3},
		{types.T_uint8, 3},
		{types.T_int16, 5},
		{types.T_uint16, 5},
		{types.T_int32, 10},
		{types.T_uint32, 10},
		{types.T_int64, 19},
		{types.T_uint64, 20},
		{types.T_varchar, 0},
	}

	for _, tt := range tests {
		require.Equal(t, tt.want, integerIntegralWidth(tt.oid))
	}
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
	require.Equal(t, int32(19), result.finalType[1].Width)
	require.Equal(t, int32(0), result.finalType[1].Scale)
	require.Equal(t, types.T_decimal128, result.finalType[2].Oid)
	require.Equal(t, int32(19), result.finalType[2].Width)
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

func Test_IffCheck_Decimal256OverflowFails(t *testing.T) {
	inputs := []types.Type{
		types.T_bool.ToType(),
		types.New(types.T_decimal256, 76, 0),
		types.New(types.T_decimal256, 76, 76),
	}

	result := iffCheck(nil, inputs)
	require.Equal(t, failedFunctionParametersWrong, result.status)
}

func Test_CaseCheck_TextStringBranchesStayText(t *testing.T) {
	inputs := []types.Type{
		types.T_bool.ToType(),
		types.T_text.ToType(),
		types.New(types.T_varchar, 255, 0),
	}
	result := caseCheck(nil, inputs)
	require.Equal(t, succeedWithCast, result.status)
	require.Len(t, result.finalType, len(inputs))
	require.Equal(t, types.T_bool, result.finalType[0].Oid)
	require.Equal(t, types.T_text, result.finalType[1].Oid)
	require.Equal(t, types.T_text, result.finalType[2].Oid)
}

func Test_IffCheck_TextStringBranchesStayText(t *testing.T) {
	inputs := []types.Type{
		types.T_bool.ToType(),
		types.T_text.ToType(),
		types.New(types.T_varchar, 255, 0),
	}
	result := iffCheck(nil, inputs)
	require.Equal(t, succeedWithCast, result.status)
	require.Len(t, result.finalType, len(inputs))
	require.Equal(t, types.T_bool, result.finalType[0].Oid)
	require.Equal(t, types.T_text, result.finalType[1].Oid)
	require.Equal(t, types.T_text, result.finalType[2].Oid)
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

func Test_CoalesceCheck_TextStringBranchesStayText(t *testing.T) {
	overloads := []overload{
		{args: []types.T{types.T_varchar}},
		{args: []types.T{types.T_char}},
		{args: []types.T{types.T_text}},
	}
	inputs := []types.Type{
		types.T_text.ToType(),
		types.New(types.T_varchar, 255, 0),
		types.New(types.T_char, 1, 0),
	}
	result := coalesceCheck(overloads, inputs)
	require.Equal(t, succeedWithCast, result.status)
	require.Equal(t, 2, result.idx)
	require.Len(t, result.finalType, len(inputs))
	for _, typ := range result.finalType {
		require.Equal(t, types.T_text, typ.Oid)
	}
}

// issue #24565: COALESCE over decimal branches with different scales must align
// scale/width across all branches, otherwise the result inherits the first
// branch's scale while carrying another branch's raw value (magnified result).
func Test_CoalesceCheck_DecimalScaleAlignment(t *testing.T) {
	overloads := []overload{
		{args: []types.T{types.T_decimal64}},
		{args: []types.T{types.T_decimal128}},
	}
	inputs := []types.Type{
		types.New(types.T_decimal128, 23, 2),
		types.New(types.T_decimal128, 38, 7),
	}
	result := coalesceCheck(overloads, inputs)
	require.Equal(t, succeedWithCast, result.status)
	require.Equal(t, 1, result.idx) // decimal128 overload
	require.Len(t, result.finalType, len(inputs))
	for _, typ := range result.finalType {
		require.Equal(t, types.T_decimal128, typ.Oid)
		require.Equal(t, int32(7), typ.Scale)
		require.Equal(t, int32(38), typ.Width)
	}
}

func Test_CoalesceCheck_DecimalAligned_NoCast(t *testing.T) {
	overloads := []overload{
		{args: []types.T{types.T_decimal64}},
		{args: []types.T{types.T_decimal128}},
	}
	inputs := []types.Type{
		types.New(types.T_decimal128, 20, 4),
		types.New(types.T_decimal128, 20, 4),
	}
	result := coalesceCheck(overloads, inputs)
	// Already aligned -> no alignment cast needed, plain direct match.
	require.Equal(t, succeedMatched, result.status)
	require.Equal(t, 1, result.idx)
}

// issue #24565 review: when the combined integral width + scale overflows
// decimal128, coalesce must promote the common type to decimal256 instead of
// keeping decimal128 (which would drop integer capacity and overflow on cast).
func Test_CoalesceCheck_DecimalPromoteToDecimal256(t *testing.T) {
	overloads := []overload{
		{args: []types.T{types.T_decimal64}},
		{args: []types.T{types.T_decimal128}},
		{args: []types.T{types.T_decimal256}},
	}
	inputs := []types.Type{
		types.New(types.T_decimal128, 38, 0),  // 38 integral digits
		types.New(types.T_decimal128, 38, 38), // 38 fractional digits
	}
	result := coalesceCheck(overloads, inputs)
	require.Equal(t, succeedWithCast, result.status)
	require.Equal(t, 2, result.idx) // decimal256 overload
	require.Len(t, result.finalType, len(inputs))
	for _, typ := range result.finalType {
		require.Equal(t, types.T_decimal256, typ.Oid)
		require.Equal(t, int32(38), typ.Scale)
		require.Equal(t, int32(76), typ.Width) // 38 integral + 38 scale
	}
}

// required precision overflows decimal256 -> coalesce fails instead of
// silently truncating.
func Test_CoalesceCheck_DecimalOverflowFails(t *testing.T) {
	overloads := []overload{
		{args: []types.T{types.T_decimal128}},
		{args: []types.T{types.T_decimal256}},
	}
	inputs := []types.Type{
		types.New(types.T_decimal256, 76, 0),
		types.New(types.T_decimal256, 76, 76), // 76 integral + 76 scale = 152 > 76
	}
	result := coalesceCheck(overloads, inputs)
	require.Equal(t, failedFunctionParametersWrong, result.status)
}

// the aligned decimal type has no matching overload -> coalesce fails.
func Test_CoalesceCheck_DecimalNoOverloadFails(t *testing.T) {
	overloads := []overload{
		{args: []types.T{types.T_decimal64}},
		{args: []types.T{types.T_decimal128}},
	} // no decimal256 overload
	inputs := []types.Type{
		types.New(types.T_decimal128, 38, 0),
		types.New(types.T_decimal128, 38, 38), // requires decimal256
	}
	result := coalesceCheck(overloads, inputs)
	require.Equal(t, failedFunctionParametersWrong, result.status)
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
