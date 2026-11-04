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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type fixture struct {
	op            *VectorQuery
	child         *merge.Merge
	proc          *process.Process
	account       *mpool.AllocationAccount
	starts, waits []int
}

func newFixture(t *testing.T, limit uint64) *fixture {
	t.Helper()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })
	registry, err := mpool.NewAllocationAccountRegistry(1, 1<<16)
	require.NoError(t, err)
	account, err := registry.Open(math.MaxInt64)
	require.NoError(t, err)
	op := NewArgument()
	op.Source = materialized.NewSource(2)
	op.LimitExpr = plan2.MakePlan2Uint64ConstExprWithType(limit)
	child := merge.NewArgument().WithPartial(0, 0)
	op.AppendChild(child)
	f := &fixture{op: op, child: child, proc: proc, account: account}
	f.begin(t)
	t.Cleanup(func() {
		child.Reset(proc, false, nil)
		op.Free(proc, false, nil)
		child.Free(proc, false, nil)
		op.Source.Close()
		op.Release()
		child.Release()
		snapshot, _, err := registry.CompleteTerminal(account)
		require.NoError(t, err)
		require.Zero(t, snapshot.Used)
	})
	return f
}

func (f *fixture) begin(t *testing.T) {
	t.Helper()
	f.proc.Reg.MergeReceivers = []*process.WaitRegister{{}, {}, {}}
	for _, reg := range f.proc.Reg.MergeReceivers {
		reg.ResetForReuse(4, 1)
	}
	require.NoError(t, f.op.Source.Begin(f.proc.Mp(), materialized.SpillConfig{AllocationAccount: f.account}))
	f.starts, f.waits = nil, nil
}

func (f *fixture) send(t *testing.T, branch int, values []int64, null bool) {
	t.Helper()
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	for _, v := range values {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], v, null, f.proc.Mp()))
	}
	bat.SetRowCount(len(values))
	require.True(t, process.SendPipelineSignalWithContext(f.proc.Ctx, f.proc.Reg.MergeReceivers[branch], process.NewPipelineSignalToDirectly(bat, nil, f.proc.Mp())))
}

func (f *fixture) terminal(t *testing.T, branch int, err error) {
	t.Helper()
	signal := process.NewEndSignal()
	if err != nil {
		signal = process.NewErrorSignal(err)
	}
	require.True(t, process.SendPipelineSignalWithContext(context.Background(), f.proc.Reg.MergeReceivers[branch], signal))
}

func (f *fixture) install(t *testing.T, values []int64, null bool, failAt string) {
	t.Helper()
	failure := errors.New("injected provider failure")
	f.op.SetBranchStarter(func(branch int) error {
		f.starts = append(f.starts, branch)
		if branch == 0 {
			if failAt == "start" {
				f.terminal(t, branch, failure)
				return failure
			}
			if values != nil {
				f.send(t, 0, values, null)
			}
			if failAt == "read" {
				f.terminal(t, branch, failure)
				return nil
			}
		} else {
			require.Equal(t, []int{0}, f.waits, "the provider must finish before any result branch starts")
			f.send(t, branch, []int64{int64(branch)}, false)
		}
		f.terminal(t, branch, nil)
		return nil
	})
	f.op.SetBranchWaiter(func(branch int) error {
		f.waits = append(f.waits, branch)
		if branch == 0 && failAt == "completion" {
			return failure
		}
		return nil
	})
}

func TestScalarVectorQuerySelectionAndReuse(t *testing.T) {
	f := newFixture(t, 2)
	for _, tc := range []struct {
		name   string
		values []int64
		null   bool
		branch int
	}{
		{"nonnull", []int64{1}, false, 1},
		{"empty", nil, false, 2},
		{"null", []int64{0}, true, 2},
		{"new_vector", []int64{9}, false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f.install(t, tc.values, tc.null, "")
			require.NoError(t, vm.Prepare(f.op, f.proc))
			result, err := vm.Exec(f.op, f.proc)
			require.NoError(t, err)
			require.Equal(t, int64(tc.branch), vector.GetFixedAtNoTypeCheck[int64](result.Batch.Vecs[0], 0))
			result, err = vm.Exec(f.op, f.proc)
			require.NoError(t, err)
			require.Equal(t, vm.ExecStop, result.Status)
			require.Equal(t, []int{0, tc.branch}, f.starts)
			require.Equal(t, f.starts, f.waits)
			_, err = vm.Exec(f.op, f.proc)
			require.NoError(t, err)
			require.Len(t, f.starts, 2)
			f.child.Reset(f.proc, false, nil)
			f.op.Reset(f.proc, false, nil)
			f.op.Source.Close()
			f.begin(t)
		})
	}
}

func TestScalarVectorQueryNoStartAndFailures(t *testing.T) {
	t.Run("zero_limit", func(t *testing.T) {
		f := newFixture(t, 0)
		f.install(t, []int64{1}, false, "")
		require.NoError(t, vm.Prepare(f.op, f.proc))
		result, err := vm.Exec(f.op, f.proc)
		require.NoError(t, err)
		require.Equal(t, vm.ExecStop, result.Status)
		require.Empty(t, f.starts)
	})
	for _, at := range []string{"start", "read", "completion", "cardinality"} {
		t.Run(at, func(t *testing.T) {
			f := newFixture(t, 2)
			values := []int64{1}
			if at == "cardinality" {
				values = []int64{1, 2}
			}
			f.install(t, values, false, at)
			require.NoError(t, vm.Prepare(f.op, f.proc))
			result, err := vm.Exec(f.op, f.proc)
			require.Error(t, err)
			require.Nil(t, result.Batch)
			require.Equal(t, []int{0}, f.starts)
		})
	}
	t.Run("cancel", func(t *testing.T) {
		f := newFixture(t, 2)
		f.install(t, []int64{1}, false, "")
		require.NoError(t, vm.Prepare(f.op, f.proc))
		ctx, cancel := context.WithCancel(f.proc.Ctx)
		f.proc.Ctx = ctx
		cancel()
		_, err := f.op.Call(f.proc)
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, f.starts)
	})
}

func TestScalarVectorQueryOperatorContract(t *testing.T) {
	f := newFixture(t, 1)
	var out bytes.Buffer
	f.op.String(&out)
	require.Equal(t, "vector_query", out.String())
	require.Equal(t, "vector_query", f.op.TypeName())
	require.Equal(t, vm.VectorQuery, f.op.OpType())
	require.Same(t, &f.op.OperatorBase, f.op.GetOperatorBase())
	require.True(t, f.op.DeferFirstBranch())
	bat, err := f.op.ExecProjection(f.proc, batch.EmptyForConstFoldBatch)
	require.NoError(t, err)
	require.Same(t, batch.EmptyForConstFoldBatch, bat)
}
