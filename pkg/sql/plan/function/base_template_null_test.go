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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestTernaryStrFixedStrToFixedPreservesNullAndSelectionSemantics(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() {
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
	eval := func(
		parameters []*vector.Vector,
		result vector.FunctionResultWrapper,
		proc *process.Process,
		length int,
	) error {
		return opTernaryStrFixedStrToFixed[uint64, uint64](
			parameters,
			result,
			proc,
			length,
			func(value string, bitmap uint64, definition string) uint64 {
				return bitmap + uint64(len(value)+len(definition))
			},
		)
	}

	t.Run("all operands constant", func(t *testing.T) {
		caseData := NewFunctionTestCase(
			proc,
			[]FunctionTestInput{
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"a"}, nil),
				NewFunctionTestConstInput(types.T_uint64.ToType(), []uint64{2}, nil),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"xyz"}, nil),
			},
			NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{6}, []bool{false}),
			eval,
		)
		succeed, info := caseData.RunAndFree()
		require.True(t, succeed, info)
	})

	t.Run("operand nulls are merged by row", func(t *testing.T) {
		caseData := NewFunctionTestCase(
			proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"a", "b", "c"}, []bool{false, true, false}),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{2, 3, 4}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"x", "y", "z"}, []bool{true, false, false}),
			},
			NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{0, 0, 0}, []bool{true, true, true}),
			eval,
		)
		succeed, info := caseData.RunAndFree()
		require.True(t, succeed, info)
	})

	t.Run("constant null operand", func(t *testing.T) {
		caseData := NewFunctionTestCase(
			proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"a", "b"}, nil),
				NewFunctionTestConstInput(types.T_uint64.ToType(), []uint64{0}, []bool{true}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"x", "y"}, nil),
			},
			NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{0, 0}, []bool{true, true}),
			eval,
		)
		succeed, info := caseData.RunAndFree()
		require.True(t, succeed, info)
	})
}

func TestUnaryFixedToStrConstNullPreservesResultCardinality(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() {
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
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
	defer testCase.Free()

	succeed, info := testCase.Run()
	require.True(t, succeed, info)

	resultBatch := batch.NewWithSize(1)
	resultBatch.Vecs[0] = testCase.GetResultVectorDirectly()
	resultBatch.SetRowCount(2)
	require.NoError(t, resultBatch.Permute([]int64{1, 0}, proc.Mp()))
	require.Equal(t, 2, resultBatch.RowCount())
	require.True(t, resultBatch.Vecs[0].IsNull(0))
	require.True(t, resultBatch.Vecs[0].IsNull(1))
}

func TestBinaryStrFixedToStrMixedNullPreservesRowPositions(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() {
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
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
				) error {
					return opBinaryStrFixedToStrWithErrorCheck[int64](
						parameters,
						result,
						proc,
						length,
						func(value string, suffix int64) (string, error) {
							return fmt.Sprintf("%s%d", value, suffix), nil
						},
					)
				},
			)

			succeed, info := testCase.RunAndFree()
			require.True(t, succeed, info)
		})
	}
}

func TestVarlenaConstResultsShareNonInlinePayload(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() {
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
	payload := strings.Repeat("x", types.VarlenaInlineSize+17)
	constString := NewFunctionTestConstInput(
		types.T_varchar.ToType(), []string{"input", "input", "input"}, nil)
	constFixed := NewFunctionTestConstInput(
		types.T_int64.ToType(), []int64{1, 1, 1}, nil)

	tests := []struct {
		name   string
		inputs []FunctionTestInput
		fn     executeLogicOfOverload
	}{
		{
			name:   "binary string fixed to string",
			inputs: []FunctionTestInput{constString, constFixed},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
				return opBinaryStrFixedToStrWithErrorCheck[int64](parameters, result, proc, length,
					func(string, int64) (string, error) { return payload, nil })
			},
		},
		{
			name:   "binary bytes to bytes",
			inputs: []FunctionTestInput{constString, constString},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
				return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length,
					func([]byte, []byte) ([]byte, error) { return []byte(payload), nil })
			},
		},
		{
			name:   "unary bytes to bytes",
			inputs: []FunctionTestInput{constString},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
				return opUnaryBytesToBytes(parameters, result, proc, length,
					func([]byte) []byte { return []byte(payload) })
			},
		},
		{
			name:   "unary fixed to string",
			inputs: []FunctionTestInput{constFixed},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
				return opUnaryFixedToStr[int64](parameters, result, proc, length,
					func(int64) string { return payload })
			},
		},
		{
			name:   "unary fixed to string with null on error",
			inputs: []FunctionTestInput{constFixed},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
				return opUnaryFixedToStrWithNullOnError[int64](parameters, result, proc, length,
					func(int64) (string, error) { return payload, nil })
			},
		},
		{
			name:   "unary fixed to string with error",
			inputs: []FunctionTestInput{constFixed},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
				return opUnaryFixedToStrWithErrorCheck[int64](parameters, result, proc, length,
					func(int64) (string, error) { return payload, nil })
			},
		},
		{
			name:   "unary string to bytes with error",
			inputs: []FunctionTestInput{constString},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
				return opUnaryStrToBytesWithErrorCheck(parameters, result, proc, length,
					func(string) ([]byte, error) { return []byte(payload), nil })
			},
		},
		{
			name:   "unary bytes to bytes with error",
			inputs: []FunctionTestInput{constString},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
				return opUnaryBytesToBytesWithErrorCheck(parameters, result, proc, length,
					func([]byte) ([]byte, error) { return []byte(payload), nil })
			},
		},
		{
			name:   "unary bytes to bytes with null on error",
			inputs: []FunctionTestInput{constString},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
				return opUnaryBytesToBytesWithNullOnError(parameters, result, proc, length,
					func([]byte) ([]byte, error) { return []byte(payload), nil })
			},
		},
		{
			name:   "unary bytes to string with error",
			inputs: []FunctionTestInput{constString},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
				return opUnaryBytesToStrWithErrorCheck(parameters, result, proc, length,
					func([]byte) (string, error) { return payload, nil })
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testCase := NewFunctionTestCase(
				proc,
				test.inputs,
				NewFunctionTestResult(types.T_varchar.ToType(), false, nil, nil),
				test.fn,
			)
			defer testCase.Free()
			require.NoError(t, testCase.result.PreExtendAndReset(testCase.fnLength))
			require.NoError(t, test.fn(
				testCase.parameters,
				testCase.result,
				testCase.proc,
				testCase.fnLength,
			))

			result := testCase.result.GetResultVector()
			require.Equal(t, testCase.expected.typ, *result.GetType())
			require.Equal(t, 3, result.Length())
			require.Len(t, result.GetArea(), len(payload))
			parameter := vector.GenerateFunctionStrParameter(result)
			for row, wantNull := range []bool{false, false, false} {
				value, isNull := parameter.GetStrValue(uint64(row))
				require.Equalf(t, wantNull, isNull, "row %d null state", row)
				if !wantNull {
					require.Equal(t, payload, string(value))
				}
			}
		})
	}
}

func TestVarlenaConstNullTemplatesPreserveResultCardinality(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() {
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
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
		fn         executeLogicOfOverload
	}{
		{
			name:   "binary string fixed both constant",
			inputs: []FunctionTestInput{constNullString, constFixed},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
				return opBinaryStrFixedToStrWithErrorCheck[int64](parameters, result, proc, length,
					func(value string, suffix int64) (string, error) { return fmt.Sprintf("%s%d", value, suffix), nil })
			},
		},
		{
			name:   "binary string fixed left constant",
			inputs: []FunctionTestInput{constNullString, fixedVector},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
				return opBinaryStrFixedToStrWithErrorCheck[int64](parameters, result, proc, length,
					func(value string, suffix int64) (string, error) { return fmt.Sprintf("%s%d", value, suffix), nil })
			},
		},
		{
			name:   "binary string fixed right constant",
			inputs: []FunctionTestInput{stringVector, constNullFixed},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
				return opBinaryStrFixedToStrWithErrorCheck[int64](parameters, result, proc, length,
					func(value string, suffix int64) (string, error) { return fmt.Sprintf("%s%d", value, suffix), nil })
			},
		},
		{
			name:   "binary bytes both constant",
			inputs: []FunctionTestInput{constNullString, constString},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
				return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length,
					func(left, right []byte) ([]byte, error) { return append(left, right...), nil })
			},
		},
		{
			name:   "binary bytes left constant",
			inputs: []FunctionTestInput{constNullString, stringVector},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
				return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length,
					func(left, right []byte) ([]byte, error) { return append(left, right...), nil })
			},
		},
		{
			name:   "binary bytes right constant",
			inputs: []FunctionTestInput{stringVector, constNullString},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
				return opBinaryBytesBytesToBytesWithErrorCheck(parameters, result, proc, length,
					func(left, right []byte) ([]byte, error) { return append(left, right...), nil })
			},
		},
		{
			name:   "unary bytes to bytes",
			inputs: []FunctionTestInput{constNullString},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
				return opUnaryBytesToBytes(parameters, result, proc, length, func(value []byte) []byte { return value })
			},
		},
		{
			name:   "unary fixed to string with error",
			inputs: []FunctionTestInput{constNullFixed},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
				return opUnaryFixedToStrWithErrorCheck[int64](parameters, result, proc, length,
					func(value int64) (string, error) { return fmt.Sprint(value), nil })
			},
		},
		{
			name:   "unary string to bytes with error",
			inputs: []FunctionTestInput{constNullString},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
				return opUnaryStrToBytesWithErrorCheck(parameters, result, proc, length,
					func(value string) ([]byte, error) { return []byte(value), nil })
			},
		},
		{
			name:   "unary bytes to bytes with error",
			inputs: []FunctionTestInput{constNullString},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
				return opUnaryBytesToBytesWithErrorCheck(parameters, result, proc, length,
					func(value []byte) ([]byte, error) { return value, nil })
			},
		},
		{
			name:   "unary bytes to bytes with null on error",
			inputs: []FunctionTestInput{constNullString},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
				return opUnaryBytesToBytesWithNullOnError(parameters, result, proc, length,
					func(value []byte) ([]byte, error) { return value, nil })
			},
		},
		{
			name:   "unary bytes to string with error",
			inputs: []FunctionTestInput{constNullString},
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
				return opUnaryBytesToStrWithErrorCheck(parameters, result, proc, length,
					func(value []byte) (string, error) { return string(value), nil })
			},
		},
		{name: "inet6_aton", inputs: []FunctionTestInput{constNullString}, resultType: types.NewWithCharset(types.T_varbinary, 16, 0, types.CharsetBinary), fn: Inet6Aton},
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

			succeed, info := testCase.RunAndFree()
			require.True(t, succeed, info)
		})
	}
}

func TestVarlenaConstErrorAndInvalidInputsPreserveResultCardinality(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() {
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
	tests := []struct {
		name       string
		input      FunctionTestInput
		resultType types.Type
		fn         executeLogicOfOverload
	}{
		{
			name:  "null on error template",
			input: NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"bad", "bad"}, nil),
			fn: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
				return opUnaryBytesToBytesWithNullOnError(parameters, result, proc, length,
					func([]byte) ([]byte, error) { return nil, fmt.Errorf("invalid") })
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

			succeed, info := testCase.RunAndFree()
			require.True(t, succeed, info)
		})
	}
}

func TestMoTupleExprMixedNullPreservesRowPositions(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() {
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
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
	defer testCase.Free()

	succeed, info := testCase.Run()
	require.True(t, succeed, info)
}

func TestMoTupleExprConstNonInlineResultSharesPayload(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() {
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
	payload := strings.Repeat("t", types.VarlenaInlineSize+17)
	packer := types.NewPacker()
	defer packer.Close()
	packer.EncodeStringType([]byte(payload))
	encoded := string(packer.GetBuf())
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{NewFunctionTestConstInput(
			types.T_varchar.ToType(), []string{encoded, encoded, encoded}, nil)},
		NewFunctionTestResult(types.T_varchar.ToType(), false, nil, nil),
		MoTupleExpr,
	)
	defer testCase.Free()
	require.NoError(t, testCase.result.PreExtendAndReset(testCase.fnLength))
	require.NoError(t, MoTupleExpr(
		testCase.parameters,
		testCase.result,
		proc,
		testCase.fnLength,
	))

	result := testCase.GetResultVectorDirectly()
	require.Equal(t, testCase.expected.typ, *result.GetType())
	require.Equal(t, 3, result.Length())
	require.Len(t, result.GetArea(), len(payload))
	parameter := vector.GenerateFunctionStrParameter(result)
	for row, wantNull := range []bool{false, false, false} {
		value, isNull := parameter.GetStrValue(uint64(row))
		require.Equalf(t, wantNull, isNull, "row %d null state", row)
		if !wantNull {
			require.Equal(t, payload, string(value))
		}
	}
}
