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
	tests := []struct {
		name     string
		inputs   []FunctionTestInput
		expected []string
	}{
		{
			name: "left constant",
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"a", "a", "a"}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{1, 0, 3}, []bool{false, true, false}),
			},
			expected: []string{"a1", "", "a3"},
		},
		{
			name: "right constant",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"a", "", "c"}, []bool{false, true, false}),
				NewFunctionTestConstInput(types.T_int64.ToType(), []int64{2, 2, 2}, nil),
			},
			expected: []string{"a2", "", "c2"},
		},
		{
			name: "both vectors",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"a", "", "c"}, []bool{false, true, false}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{1, 2, 3}, nil),
			},
			expected: []string{"a1", "", "c3"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testCase := NewFunctionTestCase(
				proc,
				test.inputs,
				NewFunctionTestResult(
					types.T_varchar.ToType(),
					false,
					test.expected,
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
		})
	}
}

func TestVarlenaConstNullTemplatesPreserveResultCardinality(t *testing.T) {
	proc := testutil.NewProcess(t)
	constNullString := NewFunctionTestConstInput(
		types.T_varchar.ToType(), []string{"", ""}, []bool{true, true})
	constString := NewFunctionTestConstInput(
		types.T_varchar.ToType(), []string{"a", "a"}, nil)
	stringVector := NewFunctionTestInput(
		types.T_varchar.ToType(), []string{"a", "b"}, nil)
	constNullFixed := NewFunctionTestConstInput(
		types.T_int64.ToType(), []int64{0, 0}, []bool{true, true})
	constFixed := NewFunctionTestConstInput(
		types.T_int64.ToType(), []int64{1, 1}, nil)
	fixedVector := NewFunctionTestInput(
		types.T_int64.ToType(), []int64{1, 2}, nil)

	tests := []struct {
		name       string
		inputs     []FunctionTestInput
		resultType types.Type
		fn         fEvalFn
	}{
		{
			name:   "binary string fixed both constant",
			inputs: []FunctionTestInput{constNullString, constFixed},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
				return opBinaryStrFixedToStrWithErrorCheck[int64](parameters, result, proc, length,
					func(value string, suffix int64) (string, error) { return fmt.Sprintf("%s%d", value, suffix), nil }, selectList)
			},
		},
		{
			name:   "binary string fixed left constant",
			inputs: []FunctionTestInput{constNullString, fixedVector},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
				return opBinaryStrFixedToStrWithErrorCheck[int64](parameters, result, proc, length,
					func(value string, suffix int64) (string, error) { return fmt.Sprintf("%s%d", value, suffix), nil }, selectList)
			},
		},
		{
			name:   "binary string fixed right constant",
			inputs: []FunctionTestInput{stringVector, constNullFixed},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
				return opBinaryStrFixedToStrWithErrorCheck[int64](parameters, result, proc, length,
					func(value string, suffix int64) (string, error) { return fmt.Sprintf("%s%d", value, suffix), nil }, selectList)
			},
		},
		{
			name:   "binary bytes both constant",
			inputs: []FunctionTestInput{constNullString, constString},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
				return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length,
					func(left, right []byte) ([]byte, error) { return append(left, right...), nil }, selectList)
			},
		},
		{
			name:   "binary bytes left constant",
			inputs: []FunctionTestInput{constNullString, stringVector},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
				return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length,
					func(left, right []byte) ([]byte, error) { return append(left, right...), nil }, selectList)
			},
		},
		{
			name:   "binary bytes right constant",
			inputs: []FunctionTestInput{stringVector, constNullString},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
				return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length,
					func(left, right []byte) ([]byte, error) { return append(left, right...), nil }, selectList)
			},
		},
		{
			name:   "unary bytes to bytes",
			inputs: []FunctionTestInput{constNullString},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
				return opUnaryBytesToBytes(parameters, result, proc, length, func(value []byte) []byte { return value }, selectList)
			},
		},
		{
			name:   "unary bytes to string",
			inputs: []FunctionTestInput{constNullString},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
				return opUnaryBytesToStr(parameters, result, proc, length, func(value []byte) string { return string(value) }, selectList)
			},
		},
		{
			name:   "unary string to string",
			inputs: []FunctionTestInput{constNullString},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
				return opUnaryStrToStr(parameters, result, proc, length, func(value string) string { return value }, selectList)
			},
		},
		{
			name:   "unary fixed to string with error",
			inputs: []FunctionTestInput{constNullFixed},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
				return opUnaryFixedToStrWithErrorCheck[int64](parameters, result, proc, length,
					func(value int64) (string, error) { return fmt.Sprint(value), nil }, selectList)
			},
		},
		{
			name:   "unary string to bytes with error",
			inputs: []FunctionTestInput{constNullString},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
				return opUnaryStrToBytesWithErrorCheck(parameters, result, proc, length,
					func(value string) ([]byte, error) { return []byte(value), nil }, selectList)
			},
		},
		{
			name:   "unary bytes to bytes with error",
			inputs: []FunctionTestInput{constNullString},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
				return opUnaryBytesToBytesWithErrorCheck(parameters, result, proc, length,
					func(value []byte) ([]byte, error) { return value, nil }, selectList)
			},
		},
		{
			name:   "unary bytes to bytes with null on error",
			inputs: []FunctionTestInput{constNullString},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
				return opUnaryBytesToBytesWithNullOnError(parameters, result, proc, length,
					func(value []byte) ([]byte, error) { return value, nil }, selectList)
			},
		},
		{
			name:   "unary bytes to string with error",
			inputs: []FunctionTestInput{constNullString},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
				return opUnaryBytesToStrWithErrorCheck(parameters, result, proc, length,
					func(value []byte) (string, error) { return string(value), nil }, selectList)
			},
		},
		{name: "inet6_aton", inputs: []FunctionTestInput{constNullString}, resultType: types.T_varbinary.ToType(), fn: Inet6Aton},
		{name: "inet6_ntoa", inputs: []FunctionTestInput{constNullString}, fn: Inet6Ntoa},
		{
			name:   "try_jq",
			inputs: []FunctionTestInput{constNullString, constString},
			fn:     newOpBuiltInJq().tryJq,
		},
		{name: "mo_tuple_expr", inputs: []FunctionTestInput{constNullString}, fn: MoTupleExpr},
		{
			name:       "load_file",
			inputs:     []FunctionTestInput{constNullString},
			resultType: types.T_text.ToType(),
			fn:         LoadFile,
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
				test.inputs,
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
	wasmInputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"not-constant-a", "not-constant-b"}, nil),
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"fn-a", "fn-b"}, nil),
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"arg-a", "arg-b"}, nil),
	}
	onnxInputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varbinary.ToType(), []string{"model-a", "model-b"}, nil),
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"input-a", "input-b"}, nil),
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"shape-a", "shape-b"}, nil),
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"shape-a", "shape-b"}, nil),
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
		{name: "try_jq", inputs: stringInputs, fn: newOpBuiltInJq().tryJq},
		{name: "mo_tuple_expr", inputs: stringInput, fn: MoTupleExpr},
		{name: "load_file", inputs: stringInput, resultType: types.T_text.ToType(), fn: LoadFile},
		{name: "try_wasm", inputs: wasmInputs, fn: newOpBuiltInWasm().tryWasm},
		{name: "onnx_run", inputs: onnxInputs, resultType: types.T_json.ToType(), fn: newOpOnnxRun().onnxRun},
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

func TestMoTupleExprMixedNullPreservesRowPositions(t *testing.T) {
	proc := testutil.NewProcess(t)
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(
				types.T_varchar.ToType(),
				[]string{"", "", "", "invalid"},
				[]bool{false, true, false, false},
			),
		},
		NewFunctionTestResult(
			types.T_varchar.ToType(),
			false,
			[]string{"()", "", "()", ""},
			[]bool{false, true, false, true},
		),
		MoTupleExpr,
	)

	succeed, info := testCase.Run()
	require.True(t, succeed, info)

	require.NoError(t, testCase.result.PreExtendAndReset(testCase.fnLength))
	require.NoError(t, MoTupleExpr(
		testCase.parameters,
		testCase.result,
		proc,
		testCase.fnLength,
		&FunctionSelectList{AnyNull: true, SelectList: []bool{false, true, true, true}},
	))
	result := testCase.GetResultVectorDirectly()
	require.Equal(t, 4, result.Length())
	parameter := vector.GenerateFunctionStrParameter(result)
	for row, wantNull := range []bool{true, true, false, true} {
		value, isNull := parameter.GetStrValue(uint64(row))
		require.Equalf(t, wantNull, isNull, "row %d null state", row)
		if !isNull {
			require.Equal(t, "()", string(value))
		}
	}
}
