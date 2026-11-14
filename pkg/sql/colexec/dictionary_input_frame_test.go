// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package colexec

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDictionaryInputFrameSharesAndRefreshesColumns(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	proc := testutil.NewProcess(t, testutil.WithMPool(mp), testutil.WithFileService(nil))
	source := testutil.MakeVarcharVector([]string{"AA", "BB", "ignored"}, []uint64{2}, mp)
	t.Cleanup(func() { source.Free(mp) })
	base, err := vector.FreezeVector(source, mp, nil)
	require.NoError(t, err)
	t.Cleanup(base.Release)
	rows, err := vector.NewRowSelection([]int64{1, 0, 1, 2}, 3, mp, nil)
	require.NoError(t, err)
	t.Cleanup(rows.Release)
	dict, err := vector.NewDictionaryVector(base, rows, mp, nil)
	require.NoError(t, err)
	input := batch.NewWithSize(1)
	input.Vecs[0] = dict
	input.SetRowCount(4)
	t.Cleanup(func() { input.Clean(mp) })
	column := &plan.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	expressions := []*plan.Expr{bindTestFunction(t, proc, "lower", column), bindTestFunction(t, proc, "upper", column)}
	executors, err := NewExpressionExecutorsFromPlanExpressions(proc, expressions)
	require.NoError(t, err)
	t.Cleanup(func() {
		for _, e := range executors {
			e.Free()
		}
	})
	for _, e := range executors {
		e.(*FunctionExpressionExecutor).dictionaryInput = nil
	} // Exercise shared contiguous-input requests.
	var frame DictionaryInputFrame
	t.Cleanup(func() { frame.Free(mp) })
	flatInput := batch.NewWithSize(1)
	flatInput.Vecs[0] = source // Borrowed; source retains its existing cleanup owner.
	flatInput.SetRowCount(source.Length())
	frame.Begin(flatInput, nil, mp)
	frame.Bind(executors)
	for _, executor := range executors {
		_, err := executor.Eval(proc, []*batch.Batch{flatInput}, nil)
		require.NoError(t, err)
		require.Nil(t, executor.(*FunctionExpressionExecutor).dictionaryState,
			"flat evaluation must not allocate optional dictionary state")
	}
	frame.End()
	frame.Begin(input, nil, mp)
	frame.Bind(executors)
	for _, e := range executors {
		_, err := e.Eval(proc, []*batch.Batch{input}, nil)
		require.NoError(t, err)
		root := e.(*FunctionExpressionExecutor)
		require.Same(t, frame.flat[0], root.dictionaryState.parameters[0])
		require.Nil(t, root.dictionaryState.scratch[0])
		// Resizing private argument storage must preserve its borrowed frame.
		parameters, err := root.normalizeDictionaryParameters([]*vector.Vector{dict, dict}, false)
		require.NoError(t, err)
		require.Same(t, &frame, root.dictionaryState.frame)
		require.Same(t, frame.flat[0], parameters[0])
		require.Same(t, parameters[0], parameters[1])
	}
	require.Equal(t, "BB", string(frame.flat[0].GetBytesAt(0)))
	require.Equal(t, uint64(1), frame.Gathers, "siblings must not repeat the same gather")
	saved := frame.flat[0]
	frame.End()
	require.Nil(t, frame.inputs[0])
	require.False(t, frame.ready[0])
	t.Run("direct-pattern-substring-and-null", func(t *testing.T) {
		text := &plan.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "B%"}}}}
		one := &plan.Expr{Typ: types.T_int64.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}}}
		for _, tc := range []struct {
			name string
			args []*plan.Expr
		}{
			{"like", []*plan.Expr{column, text}},
			{"substring", []*plan.Expr{column, one, one}},
			{"isnull", []*plan.Expr{column}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				executor, err := NewExpressionExecutor(proc, bindTestFunction(t, proc, tc.name, tc.args...))
				require.NoError(t, err)
				t.Cleanup(executor.Free)
				result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
				require.NoError(t, err)
				if tc.name == "substring" {
					for row, want := range []string{"B", "A", "B"} {
						require.Equal(t, want, string(result.GetBytesAt(row)))
					}
					require.True(t, result.IsNull(3))
				} else if tc.name == "like" {
					for row, want := range []bool{true, false, true} {
						require.Equal(t, want, vector.GetFixedAtNoTypeCheck[bool](result, row))
					}
					require.True(t, result.IsNull(3))
				} else {
					for row, want := range []bool{false, false, false, true} {
						require.Equal(t, want, vector.GetFixedAtNoTypeCheck[bool](result, row))
						require.False(t, result.IsNull(uint64(row)))
					}
				}
				require.Nil(t, executor.(*FunctionExpressionExecutor).dictionaryState)
			})
		}
	})
	// Reuse exactly the same header address for a new logical generation.
	require.NoError(t, dict.EnsureFlat(mp))
	require.NoError(t, vector.SetBytesAt(dict, 0, []byte("CC"), mp))
	frame.Begin(input, nil, mp)
	flat, found, err := frame.materialize(dict, nil, mp)
	require.NoError(t, err)
	require.True(t, found)
	require.Same(t, saved, flat)
	require.Equal(t, "CC", string(flat.GetBytesAt(0)))
	frame.End()
	var nextFrame DictionaryInputFrame
	t.Cleanup(func() { nextFrame.Free(mp) })
	nextFrame.Begin(flatInput, nil, mp)
	nextFrame.Bind(executors)
	for _, executor := range executors {
		require.Same(t, &nextFrame, executor.(*FunctionExpressionExecutor).dictionaryState.frame)
	}
	var detached *DictionaryInputFrame
	detached.Bind(executors)
	for _, executor := range executors {
		require.Nil(t, executor.(*FunctionExpressionExecutor).dictionaryState.frame)
		executor.ResetForNextQuery()
		require.Nil(t, executor.(*FunctionExpressionExecutor).dictionaryState)
	}
	nextFrame.End()
}
