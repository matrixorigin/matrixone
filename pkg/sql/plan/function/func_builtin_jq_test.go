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
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestJqConstResultPreservesCardinalityMaskAndReuse(t *testing.T) {
	proc := testutil.NewProcess(t)
	op := newOpBuiltInJq()
	inputs := []FunctionTestInput{
		NewFunctionTestConstInput(
			types.T_varchar.ToType(),
			[]string{`{"foo":128}`, `{"foo":128}`, `{"foo":128}`},
			nil,
		),
		NewFunctionTestConstInput(
			types.T_varchar.ToType(),
			[]string{".foo", ".foo", ".foo"},
			nil,
		),
	}
	testCase := NewFunctionTestCase(
		proc,
		inputs,
		NewFunctionTestResult(
			types.T_varchar.ToType(),
			false,
			[]string{"128", "128", "128"},
			nil,
		),
		op.tryJq,
	)

	succeed, info := testCase.Run()
	require.True(t, succeed, info)

	// Reuse the result wrapper and operator with one masked row. Reuse must be
	// equivalent to a fresh result, and every logical row must still have one
	// physical varlena descriptor.
	require.NoError(t, testCase.result.PreExtendAndReset(testCase.fnLength))
	require.NoError(t, op.tryJq(
		testCase.parameters,
		testCase.result,
		proc,
		testCase.fnLength,
		&FunctionSelectList{AnyNull: true, SelectList: []bool{true, false, true}},
	))

	result := testCase.GetResultVectorDirectly()
	require.Equal(t, 3, result.Length())
	param := vector.GenerateFunctionStrParameter(result)
	for row, wantNull := range []bool{false, true, false} {
		value, isNull := param.GetStrValue(uint64(row))
		require.Equalf(t, wantNull, isNull, "row %d null state", row)
		if !wantNull {
			require.Equal(t, "128", string(value))
		}
	}

	resultBatch := batch.NewWithSize(1)
	resultBatch.Vecs[0] = result
	resultBatch.SetRowCount(3)
	require.NoError(t, resultBatch.Shuffle([]int64{2, 1, 0}, proc.Mp()))
	require.Equal(t, 3, resultBatch.RowCount())
}

func TestJqConstNonInlineResultSharesPayload(t *testing.T) {
	proc := testutil.NewProcess(t)
	op := newOpBuiltInJq()
	payload := strings.Repeat("j", types.VarlenaInlineSize+17)
	jsonPayload := strconv.Quote(payload)
	result, err := evalJqForTest(t, proc, op, []FunctionTestInput{
		NewFunctionTestConstInput(
			types.T_varchar.ToType(), []string{jsonPayload, jsonPayload, jsonPayload}, nil),
		NewFunctionTestConstInput(
			types.T_varchar.ToType(), []string{".", ".", "."}, nil),
	}, &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false, true}}, false)
	require.NoError(t, err)
	requireJqResult(t, result,
		[]string{jsonPayload, "", jsonPayload}, []bool{false, true, false})
	require.Len(t, result.GetArea(), len(jsonPayload))
}

func evalJqForTest(
	t *testing.T,
	proc *process.Process,
	op *opBuiltInJq,
	inputs []FunctionTestInput,
	selectList *FunctionSelectList,
	isTry bool,
) (*vector.Vector, error) {
	t.Helper()
	testCase := NewFunctionTestCase(
		proc,
		inputs,
		NewFunctionTestResult(types.T_varchar.ToType(), false, nil, nil),
		op.jq,
	)
	require.NoError(t, testCase.result.PreExtendAndReset(testCase.fnLength))
	fn := op.jq
	if isTry {
		fn = op.tryJq
	}
	err := fn(testCase.parameters, testCase.result, proc, testCase.fnLength, selectList)
	return testCase.GetResultVectorDirectly(), err
}

func requireJqResult(t *testing.T, result *vector.Vector, values []string, nulls []bool) {
	t.Helper()
	require.Equal(t, len(values), result.Length())
	parameter := vector.GenerateFunctionStrParameter(result)
	for i := range values {
		value, isNull := parameter.GetStrValue(uint64(i))
		require.Equalf(t, nulls[i], isNull, "row %d null state", i)
		if !isNull {
			require.Equalf(t, values[i], string(value), "row %d value", i)
		}
	}
}

func TestJqVarlenaProducerMatrix(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("zero rows", func(t *testing.T) {
		op := newOpBuiltInJq()
		result, err := evalJqForTest(t, proc, op, []FunctionTestInput{
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{}, nil),
		}, nil, false)
		require.NoError(t, err)
		require.Zero(t, result.Length())
	})

	t.Run("both constant try error", func(t *testing.T) {
		op := newOpBuiltInJq()
		result, err := evalJqForTest(t, proc, op, []FunctionTestInput{
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"invalid", "invalid"}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{".", "."}, nil),
		}, nil, true)
		require.NoError(t, err)
		requireJqResult(t, result, []string{"", ""}, []bool{true, true})
	})

	t.Run("one constant null and try compile error", func(t *testing.T) {
		op := newOpBuiltInJq()
		result, err := evalJqForTest(t, proc, op, []FunctionTestInput{
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"", ""}, []bool{true, false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{".", "."}, nil),
		}, nil, true)
		require.NoError(t, err)
		requireJqResult(t, result, []string{"", ""}, []bool{true, true})

		result, err = evalJqForTest(t, proc, op, []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{`{"foo":1}`, `{"foo":2}`}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"", ""}, []bool{true, false}),
		}, nil, true)
		require.NoError(t, err)
		requireJqResult(t, result, []string{"", ""}, []bool{true, true})

		result, err = evalJqForTest(t, proc, op, []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{`{"foo":1}`, `{"foo":2}`}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"(", "("}, nil),
		}, nil, true)
		require.NoError(t, err)
		requireJqResult(t, result, []string{"", ""}, []bool{true, true})
	})

	t.Run("constant json vector query", func(t *testing.T) {
		op := newOpBuiltInJq()
		result, err := evalJqForTest(t, proc, op, []FunctionTestInput{
			NewFunctionTestConstInput(types.T_varchar.ToType(),
				[]string{`{"foo":1}`, `{"foo":1}`, `{"foo":1}`, `{"foo":1}`}, nil),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{".foo", "(", ".foo", ".foo"}, []bool{false, false, true, false}),
		}, &FunctionSelectList{AnyNull: true, SelectList: []bool{true, true, true, false}}, true)
		require.NoError(t, err)
		requireJqResult(t, result,
			[]string{"1", "", "", ""}, []bool{false, true, true, true})
	})

	t.Run("vector json constant query", func(t *testing.T) {
		op := newOpBuiltInJq()
		result, err := evalJqForTest(t, proc, op, []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`{"foo":1}`, "invalid", `{"foo":1}`, `{"foo":1}`},
				[]bool{false, false, true, false}),
			NewFunctionTestConstInput(types.T_varchar.ToType(),
				[]string{".foo", ".foo", ".foo", ".foo"}, nil),
		}, &FunctionSelectList{AnyNull: true, SelectList: []bool{true, true, true, false}}, true)
		require.NoError(t, err)
		requireJqResult(t, result,
			[]string{"1", "", "", ""}, []bool{false, true, true, true})

		// A try_jq row error must reset the reusable encoder before the next
		// evaluation on the same operator.
		result, err = evalJqForTest(t, proc, op, []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{`{"foo":2}`}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{".foo"}, nil),
		}, nil, true)
		require.NoError(t, err)
		requireJqResult(t, result, []string{"2"}, []bool{false})
	})

	t.Run("both vectors", func(t *testing.T) {
		op := newOpBuiltInJq()
		result, err := evalJqForTest(t, proc, op, []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`{"foo":1}`, `{"foo":2}`, "invalid", `{"foo":4}`}, nil),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{".foo", "(", ".foo", ".foo"}, []bool{false, false, true, false}),
		}, &FunctionSelectList{AnyNull: true, SelectList: []bool{true, true, true, false}}, true)
		require.NoError(t, err)
		requireJqResult(t, result,
			[]string{"1", "", "", ""}, []bool{false, true, true, true})
	})

	t.Run("strict compile error", func(t *testing.T) {
		op := newOpBuiltInJq()
		result, err := evalJqForTest(t, proc, op, []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{`{"foo":1}`, `{"foo":2}`}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"(", "("}, nil),
		}, nil, false)
		require.Error(t, err)
		require.Zero(t, result.Length())
	})
}
