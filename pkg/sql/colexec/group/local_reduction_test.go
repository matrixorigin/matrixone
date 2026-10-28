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

package group

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestLocalReductionKeepsWideLowCardinalityKeys(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	child := colexec.NewMockOperator()
	t.Cleanup(func() {
		child.Free(proc, false, nil)
		require.Zero(t, proc.Mp().CurrNB())
	})
	value := strings.Repeat("x", 64<<10)
	for range 2 {
		input := batch.NewWithSize(1)
		child.WithBatchs([]*batch.Batch{input})
		input.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendStringList(input.Vecs[0], []string{value, value}, nil, proc.Mp()))
		input.SetRowCount(2)
	}
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_varchar)}, nil)
	g.NeedEval, g.LocalReductionMem = false, 128<<10
	t.Cleanup(func() { g.Free(proc, false, nil) })
	g.AppendChild(child)
	require.NoError(t, g.Prepare(proc))
	result, err := vm.Exec(g, proc)
	require.NoError(t, err)
	require.Equal(t, 1, result.Batch.RowCount())
	require.Equal(t, value, result.Batch.Vecs[0].GetStringAt(0))
	require.Greater(t, g.ctr.memUsed(), g.LocalReductionMem,
		"payload exceeds the target while the useful probe table stays small")
	require.Less(t, g.ctr.hr.Hash.Size(), g.LocalReductionMem)
	require.Zero(t, g.OpAnalyzer.GetOpStats().ExtraStats["GroupLocalReductionFlushes"])
	end, err := vm.Exec(g, proc)
	require.NoError(t, err)
	require.Nil(t, end.Batch)
}

func TestLocalReductionResamplesAndCompletesKeys(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	child := colexec.NewMockOperator()
	t.Cleanup(func() {
		child.Free(proc, false, nil)
		require.Zero(t, proc.Mp().CurrNB())
	})
	type key struct {
		id   int64
		text string
		null bool
	}
	want := make(map[key]bool)
	// A distinct prefix selects bypass. After eight batches, a duplicate-only
	// sample resumes reduction; another distinct sample selects bypass again.
	for pass := range 12 {
		input := batch.NewWithSize(2)
		child.WithBatchs([]*batch.Batch{input})
		input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		input.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
		sources := make([]types.StringSource, 4)
		for row := range 4 {
			id := int64(1)
			if pass == 0 {
				id = int64(row)
			} else if pass == 10 {
				id = int64(row + 4)
			}
			k := key{id: id, null: id%3 == 0}
			if !k.null {
				k.text = fmt.Sprintf("key-%d", id)
			}
			want[k] = true
			require.NoError(t, vector.AppendFixed(input.Vecs[0], -id, false, proc.Mp()))
			require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(k.text), k.null, proc.Mp()))
			sources[row] = types.StringSourceUserVariable
			if pass == 0 {
				sources[row] = types.StringSourceLiteral
			}
		}
		require.NoError(t, input.Vecs[1].SetStringSourcesWithMP(sources, proc.Mp()))
		input.SetRowCount(4)
	}
	keys := []*plan.Expr{colExpr(0, types.T_int64), colExpr(1, types.T_varchar)}
	abs, err := function.GetFunctionByName(proc.Ctx, "abs", []types.Type{types.T_int64.ToType()})
	require.NoError(t, err)
	computedKey := &plan.Expr{
		Typ: keys[0].Typ,
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: abs.GetEncodedOverloadID(), ObjName: "abs"},
			Args: []*plan.Expr{keys[0]},
		}},
	}
	// Borrow one executor-owned computed vector and one child-owned column.
	// The final Group must see the same positive keys across every mode switch.
	partial := newGroupOp(proc, []*plan.Expr{computedKey, keys[1]}, nil)
	partial.NeedEval, partial.LocalReductionMem, partial.SpillMem = false, 1, 64<<20
	allocation := installGroupTestAllocation(t, partial, proc, 64<<20)
	t.Cleanup(func() {
		defer finalizeGroupTestAllocation(t, partial, allocation)
		partial.Free(proc, false, nil)
		require.Zero(t, allocation.account.Snapshot().Used)
	})
	partial.AppendChild(child)
	require.NoError(t, partial.Prepare(proc))
	final := newGroupOp(proc, keys, nil)
	t.Cleanup(func() { final.Free(proc, false, nil) })
	final.AppendChild(partial)
	require.NoError(t, final.Prepare(proc))
	got := make(map[key]bool)
	for {
		result, err := vm.Exec(final, proc)
		require.NoError(t, err)
		if result.Batch == nil {
			break
		}
		ids := vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[0])
		text := result.Batch.Vecs[1]
		for row := range result.Batch.RowCount() {
			k := key{id: ids[row], null: text.GetNulls().Contains(uint64(row))}
			if !k.null {
				k.text = text.GetStringAt(row)
			}
			require.False(t, got[k], "duplicate final key: %+v", k)
			got[k] = true
			if k.id == 1 {
				require.Equal(t, types.StringSourceExpression, text.GetStringSourceAt(row),
					"provenance must merge across reduced and borrowed batches")
			}
		}
	}
	require.Equal(t, want, got)
	stats := partial.OpAnalyzer.GetOpStats()
	require.Equal(t, int64(3), stats.ExtraStats["GroupLocalReductionFlushes"])
	require.Equal(t, int64(36), stats.ExtraStats["GroupLocalBypassRows"])
	require.Zero(t, stats.SpillRows)
	require.Zero(t, final.OpAnalyzer.GetOpStats().SpillRows)
}

type localReductionControlledChild struct {
	*colexec.MockOperator
	calls int
	err   error
}

type localReductionCheckedExpression struct {
	colexec.ExpressionExecutor
	after func() error
}

func (expr *localReductionCheckedExpression) Eval(
	proc *process.Process, input []*batch.Batch, selectList []bool,
) (*vector.Vector, error) {
	vec, err := expr.ExpressionExecutor.Eval(proc, input, selectList)
	if err == nil {
		err = expr.after()
	}
	return vec, err
}

func (child *localReductionControlledChild) Call(proc *process.Process) (vm.CallResult, error) {
	child.calls++
	if child.calls == 2 {
		if child.err != nil {
			return vm.CancelResult, child.err
		}
		return vm.CallResult{Status: vm.ExecYield}, nil
	}
	result, err := child.MockOperator.Call(proc)
	if child.calls == 3 {
		result.Status = vm.ExecStop
	}
	return result, err
}

func TestLocalReductionPreservesInputControlSignals(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	child := &localReductionControlledChild{MockOperator: colexec.NewMockOperator()}
	t.Cleanup(func() {
		child.Free(proc, false, nil)
		require.Zero(t, proc.Mp().CurrNB())
	})
	for _, values := range [][]int32{{1, 2, 3, 4}, {5, 6}} {
		input := batch.NewWithSize(1)
		child.WithBatchs([]*batch.Batch{input})
		input.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
		input.SetRowCount(len(values))
	}
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, nil)
	g.NeedEval, g.LocalReductionMem = false, 1
	t.Cleanup(func() { g.Free(proc, false, nil) })
	g.AppendChild(child)
	require.NoError(t, g.Prepare(proc))
	prefix, err := vm.Exec(g, proc)
	require.NoError(t, err)
	require.Equal(t, 4, prefix.Batch.RowCount())
	yielded, err := vm.Exec(g, proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecYield, yielded.Status)
	require.Nil(t, yielded.Batch)
	resumed, err := vm.Exec(g, proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecNext, resumed.Status)
	require.Equal(t, []int32{5, 6}, vector.MustFixedColNoTypeCheck[int32](resumed.Batch.Vecs[0]))
	for range 2 {
		end, err := vm.Exec(g, proc)
		require.NoError(t, err)
		require.Nil(t, end.Batch)
	}
	require.Equal(t, 4, child.calls, "EOF must remain terminal")
}

func TestLocalReductionBypassFailureAndReuse(t *testing.T) {
	for _, mode := range []string{"cancel-batch", "cancel-eof", "child-error", "expression-error", "cancel-expression"} {
		t.Run(mode, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			t.Cleanup(proc.Free)
			base := proc.Ctx
			ctx, cancel := context.WithCancel(base)
			t.Cleanup(cancel)
			proc.Ctx = ctx
			child := colexec.NewMockOperator()
			t.Cleanup(func() {
				child.Free(proc, false, nil)
				require.Zero(t, proc.Mp().CurrNB())
			})
			makeInput := func() *batch.Batch {
				input := batch.NewWithSize(1)
				child.WithBatchs([]*batch.Batch{input})
				input.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3, 4}, nil, proc.Mp())
				input.SetRowCount(4)
				return input
			}
			makeInput()
			if mode == "cancel-batch" || mode == "expression-error" || mode == "cancel-expression" {
				makeInput()
				if mode == "cancel-batch" {
					child.WithBatchCallback(func(index int) {
						if index == 1 {
							cancel()
						}
					})
				}
			} else if mode == "cancel-eof" {
				child.WithEndOfDataCallback(cancel)
			}
			g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, nil)
			g.NeedEval, g.LocalReductionMem = false, 1
			allocation := installGroupTestAllocation(t, g, proc, 64<<20)
			t.Cleanup(func() {
				defer finalizeGroupTestAllocation(t, g, allocation)
				g.Free(proc, false, nil)
			})
			wantErr := context.Canceled
			if mode == "child-error" {
				wantErr = errors.New("local input failed")
				g.AppendChild(&localReductionControlledChild{MockOperator: child, err: wantErr})
			} else {
				g.AppendChild(child)
			}
			require.NoError(t, g.Prepare(proc))
			prefix, err := vm.Exec(g, proc)
			require.NoError(t, err)
			require.Equal(t, 4, prefix.Batch.RowCount())
			if mode == "expression-error" || mode == "cancel-expression" {
				after := func() error {
					cancel()
					return nil
				}
				if mode == "expression-error" {
					wantErr = errors.New("local key expression failed")
					after = func() error { return wantErr }
				}
				g.ctr.groupByEvaluate.Executor[0] = &localReductionCheckedExpression{
					ExpressionExecutor: g.ctr.groupByEvaluate.Executor[0], after: after,
				}
			}
			failed, err := vm.Exec(g, proc)
			require.ErrorIs(t, err, wantErr)
			require.Nil(t, failed.Batch)
			g.Reset(proc, true, err)
			require.Zero(t, allocation.account.Snapshot().Used)
			require.Zero(t, g.ctr.localReduction.bypassRows)
			require.Nil(t, g.ctr.localReduction.output.Vecs)

			child.Free(proc, false, nil)
			proc.Ctx = base
			makeInput()
			g.Children = nil
			g.AppendChild(child)
			require.NoError(t, g.Prepare(proc))
			reused, err := vm.Exec(g, proc)
			require.NoError(t, err)
			require.Equal(t, 4, reused.Batch.RowCount())
			end, err := vm.Exec(g, proc)
			require.NoError(t, err)
			require.Nil(t, end.Batch)
		})
	}
}
