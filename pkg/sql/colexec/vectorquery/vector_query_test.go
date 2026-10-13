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

package vectorquery

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func newSelectorTest(t *testing.T, k uint64) (*VectorQuery, *merge.Merge, *process.Process) {
	t.Helper()
	proc := testutil.NewProcess(t)
	proc.Reg.MergeReceivers = make([]*process.WaitRegister, 3)
	for i := range proc.Reg.MergeReceivers {
		proc.Reg.MergeReceivers[i] = &process.WaitRegister{}
		proc.Reg.MergeReceivers[i].ResetForReuse(8, 1)
	}
	child := merge.NewArgument().WithPartial(0, 0)
	op := NewArgument()
	op.Source = materialized.NewSource(2)
	require.NoError(t, op.Source.Begin(proc.Mp()))
	op.LimitExpr = &plan.Expr{Typ: plan.Type{Id: int32(types.T_uint64)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: k}}}}
	op.AppendChild(child)
	t.Cleanup(func() {
		// 错误可能发生在读取 EOF 前，所有测试 producer 都已发布终态。
		child.Reset(proc, false, nil)
		op.Free(proc, false, nil)
		op.Source.Close()
		child.Release()
		op.Release()
		require.Zero(t, proc.Mp().CurrNB())
	})
	require.NoError(t, child.Prepare(proc))
	require.NoError(t, op.Prepare(proc))
	return op, child, proc
}

func vectorBatch(t *testing.T, proc *process.Process, rows int, isNull bool) *batch.Batch {
	t.Helper()
	b := batch.NewWithSize(1)
	b.Vecs[0] = vector.NewVec(types.T_array_float32.ToType())
	for i := 0; i < rows; i++ {
		require.NoError(t, vector.AppendBytes(b.Vecs[0], types.ArrayToBytes([]float32{1, 2, 3}), isNull, proc.Mp()))
	}
	b.SetRowCount(rows)
	return b
}

func TestSelectorBranches(t *testing.T) {
	sentinel := errors.New("producer cleanup failed")
	for _, tc := range []struct {
		name                                  string
		rows                                  int
		null                                  bool
		limit                                 uint64
		want                                  []int
		providerErr, completionErr, resultErr bool
	}{
		{name: "non_null", rows: 1, limit: 2, want: []int{0, 1}},
		{name: "null", rows: 1, null: true, limit: 2, want: []int{0, 2}},
		{name: "empty", limit: 2, want: []int{0, 2}},
		{name: "limit_zero", rows: 1, limit: 0},
		{name: "provider_error", limit: 2, want: []int{0}, providerErr: true},
		{name: "error_after_eof", rows: 1, limit: 2, want: []int{0}, completionErr: true},
		{name: "result_error", rows: 1, limit: 2, want: []int{0, 1}, resultErr: true},
		{name: "proof_violation", rows: 2, limit: 2, want: []int{0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op, _, proc := newSelectorTest(t, tc.limit)
			var started, waited []int
			op.SetBranches(func(i int) error {
				started = append(started, i)
				reg := proc.Reg.MergeReceivers[i]
				if i == 0 {
					if tc.rows > 0 {
						reg.Ch2 <- process.NewPipelineSignalToDirectly(vectorBatch(t, proc, tc.rows, tc.null), nil, proc.Mp())
					}
					if tc.providerErr {
						reg.Ch2 <- process.NewErrorSignal(sentinel)
					} else {
						reg.Ch2 <- process.NewEndSignal()
					}
				} else if tc.resultErr {
					reg.Ch2 <- process.NewErrorSignal(sentinel)
				} else {
					// 真正结果来自选中分支，不能返回 provider 的控制批。
					b := batch.NewWithSize(1)
					b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
					require.NoError(t, vector.AppendFixed(b.Vecs[0], int64(42), false, proc.Mp()))
					b.SetRowCount(1)
					reg.Ch2 <- process.NewPipelineSignalToDirectly(b, nil, proc.Mp())
					reg.Ch2 <- process.NewEndSignal()
				}
				return nil
			}, func(i int) error {
				waited = append(waited, i)
				if i == 0 && tc.completionErr {
					return sentinel
				}
				return nil
			})
			result, err := op.Call(proc)
			require.Equal(t, tc.want, started)
			if tc.providerErr || tc.completionErr || tc.resultErr {
				require.ErrorIs(t, err, sentinel)
			} else if tc.rows > 1 {
				require.ErrorContains(t, err, "single-row proof")
			} else if tc.limit == 0 {
				require.NoError(t, err)
				require.Nil(t, result.Batch)
				require.Empty(t, waited)
			} else {
				require.NoError(t, err)
				require.NotNil(t, result.Batch)
				require.Equal(t, int64(42), vector.GetFixedAtNoTypeCheck[int64](result.Batch.Vecs[0], 0))
				// 第一次结果返回前，provider 已真正完成，而非仅见 EOF。
				require.Equal(t, []int{0}, waited)
				result, err = op.Call(proc)
				require.NoError(t, err)
				require.Nil(t, result.Batch)
				require.Equal(t, []int{0, tc.want[1]}, waited)
			}
			require.Equal(t, vm.ExecStop, result.Status)
		})
	}
}

func TestSelectorCancellationAndStartFailure(t *testing.T) {
	t.Run("cancel_before_start", func(t *testing.T) {
		op, _, proc := newSelectorTest(t, 1)
		ctx, cancel := context.WithCancelCause(proc.Ctx)
		proc.Ctx = ctx
		sentinel := errors.New("cancel before activation")
		cancel(sentinel)
		_, err := op.Call(proc)
		require.ErrorIs(t, err, sentinel)
	})
	t.Run("cancel_waiting_provider", func(t *testing.T) {
		op, child, proc := newSelectorTest(t, 1)
		ctx, cancel := context.WithCancelCause(proc.Ctx)
		defer cancel(nil)
		proc.Ctx = ctx
		require.NoError(t, child.Prepare(proc))
		entered := make(chan struct{})
		op.SetBranches(func(i int) error { close(entered); return nil }, func(i int) error { return nil })
		done := make(chan error, 1)
		go func() { _, err := op.Call(proc); done <- err }()
		<-entered
		sentinel := errors.New("cancel active provider")
		cancel(sentinel)
		// 模拟 producer 在观察取消后发布终态，清理不依赖超时。
		proc.Reg.MergeReceivers[0].Ch2 <- process.NewAbortSignal(sentinel)
		select {
		case err := <-done:
			require.ErrorIs(t, err, sentinel)
		case <-time.After(5 * time.Second):
			t.Fatal("selector did not observe cancellation")
		}
	})
	t.Run("start_failure", func(t *testing.T) {
		op, _, proc := newSelectorTest(t, 1)
		sentinel := errors.New("submit failed")
		op.SetBranches(func(i int) error {
			proc.Reg.MergeReceivers[i].Ch2 <- process.NewErrorSignal(sentinel)
			return sentinel
		}, func(int) error { t.Fatal("failed activation cannot wait"); return nil })
		_, err := op.Call(proc)
		require.ErrorIs(t, err, sentinel)
	})
}

func TestSelectorGenerationReuse(t *testing.T) {
	op, child, proc := newSelectorTest(t, 1)
	for iteration, isNull := range []bool{false, true, false, true} {
		if iteration > 0 {
			child.Reset(proc, false, nil)
			op.Reset(proc, false, nil)
			op.Source.ReleaseReader(0)
			op.Source.ReleaseReader(1)
			require.NoError(t, op.Source.Begin(proc.Mp()))
			for _, reg := range proc.Reg.MergeReceivers {
				reg.ResetForReuse(8, 1)
			}
			require.NoError(t, child.Prepare(proc))
			require.NoError(t, op.Prepare(proc))
		}
		var started []int
		op.SetBranches(func(i int) error {
			started = append(started, i)
			if i == 0 {
				proc.Reg.MergeReceivers[i].Ch2 <- process.NewPipelineSignalToDirectly(vectorBatch(t, proc, 1, isNull), nil, proc.Mp())
			}
			proc.Reg.MergeReceivers[i].Ch2 <- process.NewEndSignal()
			return nil
		}, func(int) error { return nil })
		result, err := op.Call(proc)
		require.NoError(t, err)
		require.Equal(t, vm.ExecStop, result.Status)
		expected := 1
		if isNull {
			expected = 2
		}
		require.Equal(t, []int{0, expected}, started)
	}
	var buf bytes.Buffer
	op.String(&buf)
	require.Equal(t, "vector_query", buf.String())
	require.Equal(t, "vector_query", op.TypeName())
	require.Equal(t, vm.VectorQuery, op.OpType())
	require.Same(t, &op.OperatorBase, op.GetOperatorBase())
	b := batch.NewWithSize(0)
	projected, err := op.ExecProjection(proc, b)
	require.NoError(t, err)
	require.Same(t, b, projected)
}

func TestSelectorRejectsInvalidControl(t *testing.T) {
	t.Run("missing_lifecycle", func(t *testing.T) {
		op, _, proc := newSelectorTest(t, 1)
		_, err := op.Call(proc)
		require.ErrorContains(t, err, "lifecycle")
	})
	t.Run("malformed_batch", func(t *testing.T) {
		op, _, proc := newSelectorTest(t, 1)
		op.SetBranches(func(i int) error {
			b := batch.NewWithSize(0)
			b.SetRowCount(1)
			proc.Reg.MergeReceivers[i].Ch2 <- process.NewPipelineSignalToDirectly(b, nil, proc.Mp())
			proc.Reg.MergeReceivers[i].Ch2 <- process.NewEndSignal()
			return nil
		}, func(int) error { return nil })
		_, err := op.Call(proc)
		require.ErrorContains(t, err, "provider batch")
	})
	t.Run("invalid_generation", func(t *testing.T) {
		op, _, proc := newSelectorTest(t, 0)
		_, err := op.Call(proc)
		require.NoError(t, err)
		require.ErrorContains(t, op.Prepare(proc), "generation")
	})
}
