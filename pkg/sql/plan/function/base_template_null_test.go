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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestUnaryFixedToStrConstNullPreservesResultCardinality(t *testing.T) {
	proc := testutil.NewProcess(t)
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestConstInput(
				types.T_uint64.ToType(),
				[]uint64{0, 0},
				[]bool{true, true},
			),
		},
		NewFunctionTestResult(
			types.T_varchar.ToType(),
			false,
			[]string{"", ""},
			[]bool{true, true},
		),
		InetNtoa,
	)

	succeed, info := testCase.Run()
	require.True(t, succeed, info)

	resultBatch := batch.NewWithSize(1)
	resultBatch.Vecs[0] = testCase.GetResultVectorDirectly()
	resultBatch.SetRowCount(2)
	require.NoError(t, resultBatch.Shuffle([]int64{1, 0}, proc.Mp()))
	require.Equal(t, 2, resultBatch.RowCount())
	require.True(t, resultBatch.Vecs[0].IsNull(0))
	require.True(t, resultBatch.Vecs[0].IsNull(1))
}

func TestBinaryStrFixedToStrMixedNullPreservesRowPositions(t *testing.T) {
	proc := testutil.NewProcess(t)
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(
				types.T_varchar.ToType(),
				[]string{"a", "", "c"},
				[]bool{false, true, false},
			),
			NewFunctionTestConstInput(
				types.T_int64.ToType(),
				[]int64{2, 2, 2},
				nil,
			),
		},
		NewFunctionTestResult(
			types.T_varchar.ToType(),
			false,
			[]string{"a2", "", "c2"},
			[]bool{false, true, false},
		),
		func(
			parameters []*vector.Vector,
			result vector.FunctionResultWrapper,
			proc *process.Process,
			length int,
			selectList *FunctionSelectList,
		) error {
			return opBinaryStrFixedToStrWithErrorCheck[int64](
				parameters,
				result,
				proc,
				length,
				func(value string, suffix int64) (string, error) {
					return fmt.Sprintf("%s%d", value, suffix), nil
				},
				selectList,
			)
		},
	)

	succeed, info := testCase.Run()
	require.True(t, succeed, info)
}

func TestVarlenaTemplatesIgnoreAllRowsPreserveResultCardinality(t *testing.T) {
	proc := testutil.NewProcess(t)
	stringInput := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"a", "b"}, nil),
	}
	fixedInput := []FunctionTestInput{
		NewFunctionTestInput(types.T_uint64.ToType(), []uint64{1, 2}, nil),
	}
	stringFixedInputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"a", "b"}, nil),
		NewFunctionTestInput(types.T_int64.ToType(), []int64{1, 2}, nil),
	}
	stringInputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"a", "b"}, nil),
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"c", "d"}, nil),
	}

	tests := []struct {
		name       string
		inputs     []FunctionTestInput
		resultType types.Type
		fn         fEvalFn
	}{
		{
			name:   "binary string fixed to string",
			inputs: stringFixedInputs,
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
				return opBinaryStrFixedToStrWithErrorCheck[int64](parameters, result, proc, length,
					func(value string, suffix int64) (string, error) { return value, nil }, selectList)
			},
		},
		{
			name:   "binary bytes to bytes",
			inputs: stringInputs,
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
				return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length,
					func(left, right []byte) ([]byte, error) { return left, nil }, selectList)
			},
		},
		{
			name:   "unary bytes to bytes",
			inputs: stringInput,
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
				return opUnaryBytesToBytes(parameters, result, proc, length, func(value []byte) []byte { return value }, selectList)
			},
		},
		{
			name:   "unary bytes to string",
			inputs: stringInput,
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
				return opUnaryBytesToStr(parameters, result, proc, length, func(value []byte) string { return string(value) }, selectList)
			},
		},
		{
			name:   "unary string to string",
			inputs: stringInput,
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
				return opUnaryStrToStr(parameters, result, proc, length, func(value string) string { return value }, selectList)
			},
		},
		{
			name:   "unary fixed to string",
			inputs: fixedInput,
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
				return opUnaryFixedToStr[uint64](parameters, result, proc, length, func(value uint64) string { return fmt.Sprint(value) }, selectList)
			},
		},
		{
			name:   "unary fixed to string with error",
			inputs: fixedInput,
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
				return opUnaryFixedToStrWithErrorCheck[uint64](parameters, result, proc, length,
					func(value uint64) (string, error) { return fmt.Sprint(value), nil }, selectList)
			},
		},
		{
			name:   "unary string to bytes with error",
			inputs: stringInput,
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
				return opUnaryStrToBytesWithErrorCheck(parameters, result, proc, length,
					func(value string) ([]byte, error) { return []byte(value), nil }, selectList)
			},
		},
		{
			name:   "unary bytes to bytes with error",
			inputs: stringInput,
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
				return opUnaryBytesToBytesWithErrorCheck(parameters, result, proc, length,
					func(value []byte) ([]byte, error) { return value, nil }, selectList)
			},
		},
		{
			name:   "unary bytes to bytes with null on error",
			inputs: stringInput,
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
				return opUnaryBytesToBytesWithNullOnError(parameters, result, proc, length,
					func(value []byte) ([]byte, error) { return value, nil }, selectList)
			},
		},
		{
			name:   "unary bytes to string with error",
			inputs: stringInput,
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
				return opUnaryBytesToStrWithErrorCheck(parameters, result, proc, length,
					func(value []byte) (string, error) { return string(value), nil }, selectList)
			},
		},
		{name: "inet6_aton", inputs: stringInput, resultType: types.T_varbinary.ToType(), fn: Inet6Aton},
		{name: "inet6_ntoa", inputs: stringInput, fn: Inet6Ntoa},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resultType := test.resultType
			if resultType.Oid == types.T_any {
				resultType = types.T_varchar.ToType()
			}
			testCase := NewFunctionTestCase(
				proc,
				test.inputs,
				NewFunctionTestResult(resultType, false, nil, nil),
				test.fn,
			)
			require.NoError(t, testCase.result.PreExtendAndReset(testCase.fnLength))
			require.NoError(t, test.fn(
				testCase.parameters,
				testCase.result,
				testCase.proc,
				testCase.fnLength,
				&FunctionSelectList{AllNull: true},
			))

			result := testCase.result.GetResultVector()
			require.Equal(t, testCase.fnLength, result.Length())
			for row := uint64(0); row < uint64(testCase.fnLength); row++ {
				require.Truef(t, result.IsNull(row), "row %d should be NULL", row)
			}
		})
	}
}

func TestVarlenaConstErrorAndInvalidInputsPreserveResultCardinality(t *testing.T) {
	proc := testutil.NewProcess(t)
	tests := []struct {
		name       string
		input      FunctionTestInput
		resultType types.Type
		fn         fEvalFn
	}{
		{
			name:  "null on error template",
			input: NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"bad", "bad"}, nil),
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
				return opUnaryBytesToBytesWithNullOnError(parameters, result, proc, length,
					func([]byte) ([]byte, error) { return nil, fmt.Errorf("invalid") }, selectList)
			},
		},
		{
			name:       "invalid inet6_aton",
			input:      NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"invalid", "invalid"}, nil),
			resultType: types.T_varbinary.ToType(),
			fn:         Inet6Aton,
		},
		{
			name:  "invalid inet6_ntoa",
			input: NewFunctionTestConstInput(types.T_varbinary.ToType(), []string{"bad", "bad"}, nil),
			fn:    Inet6Ntoa,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resultType := test.resultType
			if resultType.Oid == types.T_any {
				resultType = types.T_varchar.ToType()
			}
			testCase := NewFunctionTestCase(
				proc,
				[]FunctionTestInput{test.input},
				NewFunctionTestResult(
					resultType,
					false,
					[]string{"", ""},
					[]bool{true, true},
				),
				test.fn,
			)

			succeed, info := testCase.Run()
			require.True(t, succeed, info)
		})
	}
}
