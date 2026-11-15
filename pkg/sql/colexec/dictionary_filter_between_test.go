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

package colexec

import (
	"context"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func dateSelectionFixture(t testing.TB, values []types.Date, lower, upper types.Date) (*process.Process, *batch.Batch, *FunctionExpressionExecutor, *vector.AllocationAccountSelection) {
	t.Helper()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })
	registry, err := mpool.NewAllocationAccountRegistry(1, 128)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.Zero(t, account.Snapshot().Used)
		_, _, err := registry.CompleteTerminal(account)
		require.NoError(t, err)
	})
	allocation, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerSelection, 1, 2, 3, 4)
	require.NoError(t, err)
	typ := types.T_date.ToType()
	input := batch.NewWithSize(1)
	t.Cleanup(func() { input.Clean(proc.Mp()) })
	input.Vecs[0] = vector.NewVec(typ)
	require.NoError(t, vector.AppendFixedList(input.Vecs[0], values, nil, proc.Mp()))
	input.SetRowCount(len(values))
	literal := func(value types.Date) *plan.Expr {
		return &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Dateval{Dateval: int32(value)}}}}
	}
	fn, err := function.GetFunctionByName(t.Context(), "between", []types.Type{typ, typ, typ})
	require.NoError(t, err)
	expression := &plan.Expr{Typ: fn.GetReturnType().PlanType(), Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{ObjName: "between", Obj: fn.GetEncodedOverloadID()},
		Args: []*plan.Expr{{Typ: typ.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}, literal(lower), literal(upper)},
	}}}
	executor, err := NewExpressionExecutorWithAllocation(proc, expression, allocation)
	require.NoError(t, err)
	t.Cleanup(executor.Free)
	return proc, input, executor.(*FunctionExpressionExecutor), allocation
}

func TestDateBetweenSelection(t *testing.T) {
	for _, tc := range []struct {
		name         string
		lower, upper types.Date
		want         []int64
	}{
		{"inclusive", 0, 2, []int64{1, 2, 3}},
		{"all", math.MinInt32, math.MaxInt32, []int64{0, 1, 2, 3, 4, 5, 6}},
		{"none", 7, 8, nil},
		{"inverted", 2, 1, nil},
		{"minimum", math.MinInt32, math.MinInt32, []int64{5}},
		{"maximum", math.MaxInt32, math.MaxInt32, []int64{6}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			values := []types.Date{-1, 0, 1, 2, 3, math.MinInt32, math.MaxInt32}
			proc, input, executor, allocation := dateSelectionFixture(t, values, tc.lower, tc.upper)
			scratch := make([]int64, 0, len(values))
			rows, admitted, err := selectBetweenRows(proc, input, nil, executor, scratch)
			require.NoError(t, err)

			require.True(t, admitted)
			require.Equal(t, tc.want, append([]int64(nil), rows...))
			frame := &DictionaryInputFrame{}
			t.Cleanup(func() { frame.Free(proc.Mp()) })
			var parameter vector.FunctionParameterWrapper[bool]
			rows, all, err := SelectPredicateRows(proc, input, nil, []ExpressionExecutor{executor}, allocation, &scratch, &parameter, nil, nil, frame)
			require.NoError(t, err)
			require.Equal(t, tc.name == "all", all)
			if !all {
				require.Equal(t, tc.want, append([]int64(nil), rows...))
			}
			require.Nil(t, executor.resultVector.GetResultVector(), "the direct path must not create a Boolean result")
			ordinary, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			var expected []int64
			for row := range values {
				if !ordinary.IsNull(uint64(row)) && vector.GetFixedAtNoTypeCheck[bool](ordinary, row) {
					expected = append(expected, int64(row))
				}
			}
			require.Equal(t, tc.want, expected, "ordinary Eval is an independent kernel oracle")
			require.Equal(t, values, vector.MustFixedColNoTypeCheck[types.Date](input.Vecs[0]))
		})
	}
}

func TestDateBetweenSelectionDeclinesUnprovedShapes(t *testing.T) {
	for _, shape := range []string{"null-input", "grouping", "constant-input", "dictionary", "binary-metadata", "parameter-metadata", "string-source", "null-bound", "dynamic-bound", "wrong-type", "unloaded", "short-domain", "empty", "oversized", "memo", "callback"} {
		t.Run(shape, func(t *testing.T) {
			proc, input, executor, allocation := dateSelectionFixture(t, []types.Date{0, 1, 2, 3}, 1, 2)
			var root ExpressionExecutor = executor
			var loaded []int
			source := input.Vecs[0]
			switch shape {
			case "null-input":
				source.GetNulls().Add(1)
			case "grouping":
				source.GetGrouping().Add(1)
			case "constant-input":
				source.ToConst()
			case "dictionary":
				scope := &vector.DictionarySelectionScope{}
				t.Cleanup(func() { scope.Free(proc.Mp()) })
				outputs := make([]*vector.Vector, 1)
				admitted, err := scope.Bind(input.Vecs, outputs, []int{0}, []int64{3, 1, 1}, 4, proc.Mp(), allocation)
				require.NoError(t, err)
				require.True(t, admitted)
				input = &batch.Batch{Vecs: outputs}
				input.SetRowCount(3)
			case "binary-metadata":
				source.SetIsBinaryString(true)
			case "parameter-metadata":
				source.SetPrepareParamType(types.T_date)
			case "string-source":
				require.NoError(t, source.SetStringSource(types.StringSourceLiteral))
			case "null-bound":
				bound := executor.parameterExecutor[1].(*FixedVectorExpressionExecutor)
				null, err := newExpressionConstNull(types.T_date.ToType(), 1, allocation)
				require.NoError(t, err)
				previous := bound.resultVector
				bound.resultVector = null
				previous.Free(proc.Mp())
			case "dynamic-bound":
				saved := executor.parameterExecutor[1]
				executor.parameterExecutor[1] = &ParamExpressionExecutor{}
				defer func() { executor.parameterExecutor[1] = saved }()
			case "wrong-type":
				source.SetType(types.T_int32.ToType())
			case "unloaded":
				loaded = []int{}
			case "short-domain":
				input.SetRowCount(5)
			case "empty":
				input.SetRowCount(0)
			case "oversized":
				input.SetRowCount(DefaultBatchSize + 1)
			case "memo":
				root = &memoRootExpressionExecutor{executor: executor}
			case "callback":
				executor.resetFn = func() error { return nil }
			}
			scratch := []int64{8, 9}
			rows, admitted, err := selectBetweenRows(proc, input, loaded, root, scratch)
			require.NoError(t, err)
			require.False(t, admitted)
			require.Equal(t, []int64{8, 9}, rows, "decline cannot modify the caller's row domain")
			require.Same(t, &scratch[0], &rows[0])
		})
	}
}

func TestDateBetweenSelectionReuseAndCancellation(t *testing.T) {
	proc, input, executor, allocation := dateSelectionFixture(t, []types.Date{0, 1, 2, 3}, 1, 2)
	frame := &DictionaryInputFrame{}
	t.Cleanup(func() { frame.Free(proc.Mp()) })
	executors := []ExpressionExecutor{executor}
	var rows []int64
	var parameter vector.FunctionParameterWrapper[bool]
	for pass := range 3 {
		if pass == 1 {
			input.Vecs[0].GetNulls().Add(1)
		} else {
			input.Vecs[0].GetNulls().Clear()
		}
		beforeCalls := 0
		got, all, err := SelectPredicateRows(proc, input, nil, executors, allocation, &rows, &parameter, nil, func(i, n int) {
			beforeCalls++
			require.Zero(t, i)
			require.Equal(t, 4, n)
		}, frame)
		require.NoError(t, err)
		require.False(t, all)
		require.Equal(t, 1, beforeCalls)
		want := []int64{1, 2}
		if pass == 1 {
			want = []int64{2}
		}
		require.Equal(t, want, got)
	}
	ctx, cancel := context.WithCancel(proc.Ctx)
	cancel()
	proc.Ctx = ctx
	saved := []int64{7, 8}
	got, admitted, err := selectBetweenRows(proc, input, nil, executor, saved)
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, admitted)
	require.Equal(t, saved, got)
}

func TestDateBetweenSelectionObservesCurrentExecutor(t *testing.T) {
	proc, input, first, allocation := dateSelectionFixture(t, []types.Date{0, 1, 2, 3}, 1, 2)
	_, _, replacement, _ := dateSelectionFixture(t, []types.Date{0, 1, 2, 3}, 2, 3)
	frame := &DictionaryInputFrame{}
	t.Cleanup(func() { frame.Free(proc.Mp()) })
	var scratch []int64
	var parameter vector.FunctionParameterWrapper[bool]
	executors := []ExpressionExecutor{first}
	rows, all, err := SelectPredicateRows(proc, input, nil, executors, allocation, &scratch, &parameter, nil, func(int, int) {
		executors[0] = replacement
	}, frame)
	require.NoError(t, err)
	require.False(t, all)
	require.Equal(t, []int64{2, 3}, rows)
	replacement.ResetForNextQuery()
	rows, all, err = SelectPredicateRows(proc, input, nil, executors, allocation, &scratch, &parameter, nil, nil, frame)
	require.NoError(t, err)
	require.False(t, all)
	require.Equal(t, []int64{2, 3}, rows)

	// Multiple predicates must keep the ordered ordinary-executor path.
	executors = []ExpressionExecutor{first, replacement}
	rows, all, err = SelectPredicateRows(proc, input, nil, executors, allocation, &scratch, &parameter, nil, nil, frame)
	require.NoError(t, err)
	require.False(t, all)
	require.Equal(t, []int64{2}, rows)
	require.Positive(t, first.resultVector.GetResultVector().Allocated())
}

func BenchmarkDateBetweenSelection(b *testing.B) {
	values := make([]types.Date, DefaultBatchSize)
	for i := range values {
		values[i] = types.Date(i % 7)
	}
	proc, input, executor, allocation := dateSelectionFixture(b, values, 1, 2)
	frame := &DictionaryInputFrame{}
	b.Cleanup(func() { frame.Free(proc.Mp()) })
	var rows []int64
	var parameter vector.FunctionParameterWrapper[bool]
	executors := []ExpressionExecutor{executor}
	_, _, err := SelectPredicateRows(proc, input, nil, executors, allocation, &rows, &parameter, nil, nil, frame)
	require.NoError(b, err)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, _, err := SelectPredicateRows(proc, input, nil, executors, allocation, &rows, &parameter, nil, nil, frame); err != nil {
			b.Fatal(err)
		}
	}
}
