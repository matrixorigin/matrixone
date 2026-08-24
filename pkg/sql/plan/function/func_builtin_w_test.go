// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestWasmVarlenaProducerMatrix(t *testing.T) {
	proc := testutil.NewProcess(t)
	wasmPath, err := filepath.Abs("../../../../test/distributed/resources/plugin/cat.wasm")
	require.NoError(t, err)
	wasmURL := "file://" + wasmPath

	op := newOpBuiltInWasm()
	t.Cleanup(func() {
		require.NoError(t, op.Close())
	})

	t.Run("zero rows", func(t *testing.T) {
		testCase := NewFunctionTestCase(
			proc,
			[]FunctionTestInput{
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{}, nil),
			},
			NewFunctionTestResult(types.T_varchar.ToType(), false, nil, nil),
			op.tryWasm,
		)
		require.NoError(t, testCase.result.PreExtendAndReset(0))
		require.NoError(t, op.tryWasm(testCase.parameters, testCase.result, proc, 0, nil))
		require.Zero(t, testCase.GetResultVectorDirectly().Length())
	})
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestConstInput(types.T_varchar.ToType(),
				[]string{wasmURL, wasmURL, wasmURL, wasmURL, wasmURL}, nil),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"cat", "cat", "cat", "missing", "cat"},
				[]bool{false, false, true, false, false}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"a", "b", "c", "d", "e"},
				[]bool{false, false, false, false, true}),
		},
		NewFunctionTestResult(types.T_varchar.ToType(), false, nil, nil),
		op.tryWasm,
	)
	require.NoError(t, testCase.result.PreExtendAndReset(testCase.fnLength))
	require.NoError(t, op.tryWasm(
		testCase.parameters,
		testCase.result,
		proc,
		testCase.fnLength,
		&FunctionSelectList{AnyNull: true, SelectList: []bool{false, true, true, true, true}},
	))

	result := testCase.GetResultVectorDirectly()
	require.Equal(t, 5, result.Length())
	parameter := vector.GenerateFunctionStrParameter(result)
	for row, want := range []struct {
		value  string
		isNull bool
	}{
		{isNull: true},
		{value: "b"},
		{isNull: true},
		{isNull: true},
		{isNull: true},
	} {
		value, isNull := parameter.GetStrValue(uint64(row))
		require.Equalf(t, want.isNull, isNull, "row %d null state", row)
		if !isNull {
			require.Equalf(t, want.value, string(value), "row %d value", row)
		}
	}

	t.Run("setup errors propagate", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			isTry bool
		}{
			{name: "wasm"},
			{name: "try_wasm", isTry: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				evalFn := op.wasm
				if tc.isTry {
					evalFn = op.tryWasm
				}
				testCase := NewFunctionTestCase(
					proc,
					[]FunctionTestInput{
						NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"file:///missing.wasm"}, nil),
						NewFunctionTestInput(types.T_varchar.ToType(), []string{"cat"}, nil),
						NewFunctionTestInput(types.T_varchar.ToType(), []string{"a"}, nil),
					},
					NewFunctionTestResult(types.T_varchar.ToType(), false, nil, nil),
					evalFn,
				)
				require.NoError(t, testCase.result.PreExtendAndReset(1))
				require.Error(t, evalFn(testCase.parameters, testCase.result, proc, 1, nil))
				require.Zero(t, testCase.GetResultVectorDirectly().Length())
			})
		}
	})

	t.Run("strict call error propagates", func(t *testing.T) {
		testCase := NewFunctionTestCase(
			proc,
			[]FunctionTestInput{
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{wasmURL}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"missing"}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"a"}, nil),
			},
			NewFunctionTestResult(types.T_varchar.ToType(), false, nil, nil),
			op.wasm,
		)
		require.NoError(t, testCase.result.PreExtendAndReset(1))
		require.Error(t, op.wasm(testCase.parameters, testCase.result, proc, 1, nil))
		require.Zero(t, testCase.GetResultVectorDirectly().Length())
	})

	t.Run("invalid image closes prior plugin", func(t *testing.T) {
		invalidPath := filepath.Join(t.TempDir(), "invalid.wasm")
		require.NoError(t, os.WriteFile(invalidPath, []byte("not wasm"), 0o600))
		invalidURL := "file://" + invalidPath
		testCase := NewFunctionTestCase(
			proc,
			[]FunctionTestInput{
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{invalidURL}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"cat"}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"a"}, nil),
			},
			NewFunctionTestResult(types.T_varchar.ToType(), false, nil, nil),
			op.tryWasm,
		)
		require.NoError(t, testCase.result.PreExtendAndReset(1))
		require.Error(t, op.tryWasm(testCase.parameters, testCase.result, proc, 1, nil))
		require.Zero(t, testCase.GetResultVectorDirectly().Length())
		require.Nil(t, op.plugin)
	})
}

func TestWasmRegistrationLifecycle(t *testing.T) {
	for _, functionID := range []int32{WASM, TRY_WASM} {
		ov, err := GetFunctionById(context.TODO(), encodeOverloadID(functionID, 0))
		require.NoError(t, err)
		require.True(t, ov.CannotFold())

		evalFn, resetFn, freeFn, retainedBytesFn := ov.GetExecuteMethod()
		require.NotNil(t, evalFn)
		require.NotNil(t, resetFn)
		require.NotNil(t, freeFn)
		require.Nil(t, retainedBytesFn)
		require.NoError(t, resetFn())
		require.NoError(t, freeFn())
	}
}
