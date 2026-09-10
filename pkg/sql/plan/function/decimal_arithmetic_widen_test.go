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

func TestDecimal256MultiplyHonorsPublishedPrecision(t *testing.T) {
	proc := testutil.NewProcess(t)
	leftType := types.New(types.T_decimal256, 38, 0)
	resultType := types.New(types.T_decimal256, 65, 0)
	left, err := types.ParseDecimal256(strings.Repeat("9", 38), leftType.Width, leftType.Scale)
	require.NoError(t, err)

	for _, test := range []struct {
		name      string
		rightSize int
		wantErr   bool
	}{
		{name: "65 digit boundary succeeds", rightSize: 27},
		{name: "66 digit result overflows", rightSize: 28, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			rightType := types.New(types.T_decimal256, int32(test.rightSize), 0)
			right, parseErr := types.ParseDecimal256(strings.Repeat("9", test.rightSize), rightType.Width, rightType.Scale)
			require.NoError(t, parseErr)
			want, mulErr := left.Mul256(right)
			require.NoError(t, mulErr)

			var expected any = []types.Decimal256{want}
			if test.wantErr {
				expected = nil
			}
			testCase := NewFunctionTestCase(proc,
				[]FunctionTestInput{
					NewFunctionTestInput(leftType, []types.Decimal256{left}, nil),
					NewFunctionTestInput(rightType, []types.Decimal256{right}, nil),
				},
				NewFunctionTestResult(resultType, test.wantErr, expected, nil),
				multiFn)
			if test.wantErr {
				require.NoError(t, testCase.result.PreExtendAndReset(testCase.fnLength))
				evalErr := multiFn(testCase.parameters, testCase.result, proc, testCase.fnLength, nil)
				require.ErrorContains(t, evalErr, "exceeds DECIMAL(65,0)")
				return
			}
			succeeded, info := testCase.Run()
			require.True(t, succeeded, info)
		})
	}
}
