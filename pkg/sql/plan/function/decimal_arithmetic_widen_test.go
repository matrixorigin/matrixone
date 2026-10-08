// Copyright 2026 Matrix Origin
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
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestDecimal128ArithmeticWidensToDecimal256(t *testing.T) {
	for _, test := range []struct {
		name     string
		operator string
		input    types.Type
		want     types.Type
	}{
		{name: "add 38 digits", operator: "+", input: types.New(types.T_decimal128, 38, 0), want: types.New(types.T_decimal256, 39, 0)},
		{name: "subtract scaled 38 digits", operator: "-", input: types.New(types.T_decimal128, 38, 18), want: types.New(types.T_decimal256, 39, 18)},
		{name: "multiply 20 digits", operator: "*", input: types.New(types.T_decimal128, 20, 0), want: types.New(types.T_decimal256, 40, 0)},
	} {
		t.Run(test.name, func(t *testing.T) {
			resolved, err := GetFunctionByName(context.Background(), test.operator, []types.Type{test.input, test.input})
			require.NoError(t, err)
			require.True(t, resolved.needCast)
			require.Equal(t, []types.Type{
				types.New(types.T_decimal256, test.input.Width, test.input.Scale),
				types.New(types.T_decimal256, test.input.Width, test.input.Scale),
			}, resolved.targetTypes)
			require.Equal(t, test.want, resolved.retType)
		})
	}
}

func TestDecimal128ArithmeticKeepsExistingFastPathWhenResultFits(t *testing.T) {
	input := types.New(types.T_decimal128, 19, 0)
	resolved, err := GetFunctionByName(context.Background(), "*", []types.Type{input, input})
	require.NoError(t, err)
	require.NotEqual(t, types.T_decimal256, resolved.targetTypes[0].Oid)
	require.Equal(t, types.New(types.T_decimal128, 38, 0), resolved.retType)
}

func TestMixedDecimalArithmeticWidensFromOriginalDomains(t *testing.T) {
	decimal38 := types.New(types.T_decimal128, 38, 0)
	decimal20 := types.New(types.T_decimal128, 20, 0)
	decimal18 := types.New(types.T_decimal64, 18, 0)
	for _, test := range []struct {
		name       string
		operator   string
		inputs     []types.Type
		wantTarget []types.Type
		wantResult types.Type
	}{
		{
			name:       "decimal38 plus signed bigint",
			operator:   "+",
			inputs:     []types.Type{decimal38, types.T_int64.ToType()},
			wantTarget: []types.Type{types.New(types.T_decimal256, 38, 0), types.New(types.T_decimal256, 19, 0)},
			wantResult: types.New(types.T_decimal256, 39, 0),
		},
		{
			name:       "decimal38 minus unsigned bigint",
			operator:   "-",
			inputs:     []types.Type{decimal38, types.T_uint64.ToType()},
			wantTarget: []types.Type{types.New(types.T_decimal256, 38, 0), types.New(types.T_decimal256, 20, 0)},
			wantResult: types.New(types.T_decimal256, 39, 0),
		},
		{
			name:       "decimal38 times unsigned bigint",
			operator:   "*",
			inputs:     []types.Type{decimal38, types.T_uint64.ToType()},
			wantTarget: []types.Type{types.New(types.T_decimal256, 38, 0), types.New(types.T_decimal256, 20, 0)},
			wantResult: types.New(types.T_decimal256, 58, 0),
		},
		{
			name:       "decimal64 plus decimal128",
			operator:   "+",
			inputs:     []types.Type{decimal18, decimal38},
			wantTarget: []types.Type{types.New(types.T_decimal256, 18, 0), types.New(types.T_decimal256, 38, 0)},
			wantResult: types.New(types.T_decimal256, 39, 0),
		},
		{
			name:       "bit64 plus decimal38",
			operator:   "+",
			inputs:     []types.Type{types.T_bit.ToType(), decimal38},
			wantTarget: []types.Type{types.New(types.T_decimal256, 20, 0), types.New(types.T_decimal256, 38, 0)},
			wantResult: types.New(types.T_decimal256, 39, 0),
		},
		{
			name:       "decimal38 plus bit64",
			operator:   "+",
			inputs:     []types.Type{decimal38, types.T_bit.ToType()},
			wantTarget: []types.Type{types.New(types.T_decimal256, 38, 0), types.New(types.T_decimal256, 20, 0)},
			wantResult: types.New(types.T_decimal256, 39, 0),
		},
		{
			name:       "bit64 times decimal38",
			operator:   "*",
			inputs:     []types.Type{types.T_bit.ToType(), decimal38},
			wantTarget: []types.Type{types.New(types.T_decimal256, 20, 0), types.New(types.T_decimal256, 38, 0)},
			wantResult: types.New(types.T_decimal256, 58, 0),
		},
		{
			name:       "decimal38 times bit64",
			operator:   "*",
			inputs:     []types.Type{decimal38, types.T_bit.ToType()},
			wantTarget: []types.Type{types.New(types.T_decimal256, 38, 0), types.New(types.T_decimal256, 20, 0)},
			wantResult: types.New(types.T_decimal256, 58, 0),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			resolved, err := GetFunctionByName(context.Background(), test.operator, test.inputs)
			require.NoError(t, err)
			require.True(t, resolved.needCast)
			require.Equal(t, test.wantTarget, resolved.targetTypes)
			require.Equal(t, test.wantResult, resolved.retType)
		})
	}

	resolved, err := GetFunctionByName(context.Background(), "*", []types.Type{decimal20, decimal18})
	require.NoError(t, err)
	require.NotEqual(t, types.T_decimal256, resolved.targetTypes[0].Oid)
	require.Equal(t, types.New(types.T_decimal128, 38, 0), resolved.retType)
}

// Published precision is owned by the common arithmetic wrapper, independently
// of kernel dispatch. Inputs below are legal in their declared SQL domains.
func TestDecimal256ArithmeticPublishedPrecision(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	whole := "-57896044618658097711785492504343953926634992332820282019728792003"
	fraction := "-0.956564819968"
	max65 := strings.Repeat("9", 65)
	for _, tc := range []struct {
		name                            string
		fn                              executeLogicOfOverload
		leftType, rightType, resultType types.Type
		left, right, want               []string
		leftNulls, wantNulls            []bool
		leftConst, rightConst           bool
		selection                       *FunctionSelectList
		rawWant                         []types.Decimal256
		wantErr                         string
	}{
		{name: "multiply 65 digits", fn: multiFn,
			leftType: types.New(types.T_decimal256, 38, 0), rightType: types.New(types.T_decimal256, 27, 0), resultType: types.New(types.T_decimal256, 65, 0),
			left: []string{strings.Repeat("9", 38)}, right: []string{strings.Repeat("9", 27)}, want: []string{"99999999999999999999999999899999999999000000000000000000000000001"}},
		{name: "multiply overflow", fn: multiFn,
			leftType: types.New(types.T_decimal256, 38, 0), rightType: types.New(types.T_decimal256, 28, 0), resultType: types.New(types.T_decimal256, 65, 0),
			left: []string{strings.Repeat("9", 38)}, right: []string{strings.Repeat("9", 28)}, wantErr: "exceeds DECIMAL(65,0)"},
		{name: "negative boundary succeeds", fn: plusFn,
			leftType: types.New(types.T_decimal256, 65, 0), rightType: types.New(types.T_decimal256, 65, 0), resultType: types.New(types.T_decimal256, 65, 0),
			left: []string{"-" + max65}, right: []string{"0"}, want: []string{"-" + max65}},
		{name: "positive boundary overflows", fn: plusFn,
			leftType: types.New(types.T_decimal256, 65, 0), rightType: types.New(types.T_decimal256, 65, 0), resultType: types.New(types.T_decimal256, 65, 0),
			left: []string{max65}, right: []string{"1"}, wantErr: "exceeds DECIMAL(65,0)"},
		{name: "negative boundary overflows", fn: minusFn,
			leftType: types.New(types.T_decimal256, 65, 0), rightType: types.New(types.T_decimal256, 65, 0), resultType: types.New(types.T_decimal256, 65, 0),
			left: []string{"-" + max65}, right: []string{"1"}, wantErr: "exceeds DECIMAL(65,0)"},
		{name: "minimum vector vector", fn: plusFn,
			leftType: types.New(types.T_decimal256, 65, 0), rightType: types.New(types.T_decimal256, 65, 12), resultType: types.New(types.T_decimal256, 65, 12),
			left: []string{"0", whole}, right: []string{"0", fraction}, wantErr: "exceeds DECIMAL(65,12)"},
		{name: "minimum scalar vector", fn: plusFn, leftConst: true,
			leftType: types.New(types.T_decimal256, 65, 12), rightType: types.New(types.T_decimal256, 65, 0), resultType: types.New(types.T_decimal256, 65, 12),
			left: []string{fraction}, right: []string{"0", whole}, wantErr: "exceeds DECIMAL(65,12)"},
		{name: "minimum vector scalar", fn: minusFn, rightConst: true,
			leftType: types.New(types.T_decimal256, 65, 0), rightType: types.New(types.T_decimal256, 65, 12), resultType: types.New(types.T_decimal256, 65, 12),
			left: []string{"0", whole}, right: []string{"0.956564819968"}, wantErr: "exceeds DECIMAL(65,12)"},
		{name: "physical carrier preserves minimum", fn: plusFn,
			leftType: types.New(types.T_decimal256, 65, 0), rightType: types.New(types.T_decimal256, 65, 12), resultType: types.New(types.T_decimal256, 76, 12),
			left: []string{"0", whole}, right: []string{"0", fraction}, rawWant: []types.Decimal256{{}, {B192_255: uint64(1) << 63}}},
		{name: "minimum NULL row ignored", fn: plusFn,
			leftType: types.New(types.T_decimal256, 65, 0), rightType: types.New(types.T_decimal256, 65, 12), resultType: types.New(types.T_decimal256, 65, 12),
			left: []string{"1", whole}, right: []string{"0", fraction}, leftNulls: []bool{false, true}, want: []string{"1", "0"}, wantNulls: []bool{false, true}},
		{name: "minimum filtered row ignored", fn: plusFn,
			leftType: types.New(types.T_decimal256, 65, 0), rightType: types.New(types.T_decimal256, 65, 12), resultType: types.New(types.T_decimal256, 65, 12),
			left: []string{"1", whole}, right: []string{"0", fraction}, selection: &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}}, want: []string{"1", "0"}, wantNulls: []bool{false, true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parse := func(values []string, typ types.Type) []types.Decimal256 {
				out := make([]types.Decimal256, len(values))
				for i, value := range values {
					var err error
					out[i], err = types.ParseDecimal256(value, typ.Width, typ.Scale)
					require.NoError(t, err)
				}
				return out
			}
			left := NewFunctionTestInput(tc.leftType, parse(tc.left, tc.leftType), tc.leftNulls)
			left.isConst = tc.leftConst
			right := NewFunctionTestInput(tc.rightType, parse(tc.right, tc.rightType), nil)
			right.isConst = tc.rightConst
			want := tc.rawWant
			if want == nil {
				want = parse(tc.want, tc.resultType)
			}
			fc := NewFunctionTestCase(proc, []FunctionTestInput{left, right},
				NewFunctionTestResult(tc.resultType, tc.wantErr != "", want, tc.wantNulls), tc.fn).WithSelectList(tc.selection)
			defer fc.Free()
			fc.fnLength = max(len(tc.left), len(tc.right))
			if tc.wantErr != "" {
				_, err := fc.DebugRun()
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "error: %v", err)
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			ok, info := fc.Run()
			require.True(t, ok, info)
			require.Equal(t, tc.resultType, *fc.GetResultVectorDirectly().GetType())
		})
	}
}

func TestDecimal256MultiplyPreservesWideSignedOperand(t *testing.T) {
	proc := testutil.NewProcess(t)
	leftType := types.New(types.T_decimal256, 19, 0)
	rightType := types.New(types.T_decimal256, 32, 16)
	resultType := types.New(types.T_decimal256, 51, 16)
	left, err := types.ParseDecimal256("1235467899687894561", leftType.Width, leftType.Scale)
	require.NoError(t, err)
	for _, test := range []struct {
		name     string
		right    string
		expected string
	}{
		{
			name:     "positive",
			right:    "2733892455124775.7851878942123454",
			expected: "3377636369505588272411432821735887.9920303797133694",
		},
		{
			name:     "negative",
			right:    "-2733892455124775.7851878942123454",
			expected: "-3377636369505588272411432821735887.9920303797133694",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			right, parseErr := types.ParseDecimal256(test.right, rightType.Width, rightType.Scale)
			require.NoError(t, parseErr)
			expected, parseErr := types.ParseDecimal256(test.expected, resultType.Width, resultType.Scale)
			require.NoError(t, parseErr)
			require.False(t, d256AllFitInt64([]types.Decimal256{right}, 1))
			require.False(t, d256AllFitInt32([]types.Decimal256{right}, 1))

			for _, input := range []struct {
				name  string
				right FunctionTestInput
			}{
				{
					name:  "literal scalar",
					right: NewFunctionTestConstInput(rightType, []types.Decimal256{right}, nil),
				},
				{
					name:  "typed column",
					right: NewFunctionTestInput(rightType, []types.Decimal256{right, right}, nil),
				},
			} {
				t.Run(input.name, func(t *testing.T) {
					testCase := NewFunctionTestCase(proc,
						[]FunctionTestInput{
							NewFunctionTestInput(leftType, []types.Decimal256{left, left}, nil),
							input.right,
						},
						NewFunctionTestResult(resultType, false, []types.Decimal256{expected, expected}, nil),
						multiFn)
					succeeded, info := testCase.RunAndFree()
					require.True(t, succeeded, info)
				})
			}
		})
	}
}
