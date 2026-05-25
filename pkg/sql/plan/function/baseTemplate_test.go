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
	"bytes"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestOpUnaryFixedToFixedWithNullOnError(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("const error turns all rows null", func(t *testing.T) {
		tc := tcTemp{
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_float64.ToType(), []float64{-1}, nil),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0}, []bool{true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
			return opUnaryFixedToFixedWithNullOnError[float64, float64](parameters, result, proc, length, func(v float64) (float64, error) {
				if v < 0 {
					return 0, errors.New("neg")
				}
				return v + 1, nil
			}, selectList)
		})
		succeed, info := tcc.Run()
		require.True(t, succeed, info)
	})

	t.Run("row error only nulls failing row", func(t *testing.T) {
		tc := tcTemp{
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(), []float64{1, -1, 3}, nil),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{2, 0, 4}, []bool{false, true, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
			return opUnaryFixedToFixedWithNullOnError[float64, float64](parameters, result, proc, length, func(v float64) (float64, error) {
				if v < 0 {
					return 0, errors.New("neg")
				}
				return v + 1, nil
			}, selectList)
		})
		succeed, info := tcc.Run()
		require.True(t, succeed, info)
	})
}

func TestOpBinaryFixedFixedToFixedWithNullOnError(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("const lhs with row error", func(t *testing.T) {
		tc := tcTemp{
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_float64.ToType(), []float64{10}, nil),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{2, 0, 5}, nil),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{5, 0, 2}, []bool{false, true, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
			return opBinaryFixedFixedToFixedWithNullOnError[float64, float64, float64](parameters, result, proc, length, func(v1, v2 float64) (float64, error) {
				if v2 == 0 {
					return 0, errors.New("div0")
				}
				return v1 / v2, nil
			}, selectList)
		})
		succeed, info := tcc.Run()
		require.True(t, succeed, info)
	})
}

func TestOpUnaryBytesToBytesWithNullOnError(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("error row becomes null", func(t *testing.T) {
		tc := tcTemp{
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"ok", "bad", "go"}, nil),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"OK", "", "GO"}, []bool{false, true, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
			return opUnaryBytesToBytesWithNullOnError(parameters, result, proc, length, func(v []byte) ([]byte, error) {
				if string(v) == "bad" {
					return nil, errors.New("bad")
				}
				return bytes.ToUpper(v), nil
			}, selectList)
		})
		succeed, info := tcc.Run()
		require.True(t, succeed, info)
	})

	t.Run("ignore all rows", func(t *testing.T) {
		vec := runJsonFunctionWithSelectListForBaseTemplate(t, proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"bad", "still bad"}, nil),
			},
			types.T_varchar.ToType(),
			func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
				return opUnaryBytesToBytesWithNullOnError(parameters, result, proc, length, func(v []byte) ([]byte, error) {
					return nil, errors.New("bad")
				}, selectList)
			},
			&FunctionSelectList{AllNull: true},
		)
		require.True(t, vec.IsNull(0))
		require.True(t, vec.IsNull(1))
	})
}

func runJsonFunctionWithSelectListForBaseTemplate(t *testing.T, proc *process.Process, inputs []FunctionTestInput, retType types.Type, fn fEvalFn, selectList *FunctionSelectList) *vector.Vector {
	t.Helper()
	fcTC := NewFunctionTestCase(proc, inputs, NewFunctionTestResult(retType, false, nil, nil), fn)
	require.NoError(t, fcTC.result.PreExtendAndReset(fcTC.fnLength))
	require.NoError(t, fcTC.fn(fcTC.parameters, fcTC.result, fcTC.proc, fcTC.fnLength, selectList))
	return fcTC.result.GetResultVector()
}
