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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
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
