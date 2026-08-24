// Copyright 2024 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package unionall

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	arg   *UnionAll
	types []types.Type
	proc  *process.Process
}

type cancelOnNthDoneContext struct {
	done  chan struct{}
	nth   int32
	calls atomic.Int32
	once  sync.Once
}

func (c *cancelOnNthDoneContext) Deadline() (time.Time, bool) { return time.Time{}, false }

func (c *cancelOnNthDoneContext) Done() <-chan struct{} {
	if c.calls.Add(1) == c.nth {
		c.once.Do(func() { close(c.done) })
	}
	return c.done
}

func (c *cancelOnNthDoneContext) Err() error {
	select {
	case <-c.done:
		return context.Canceled
	default:
		return nil
	}
}

func (c *cancelOnNthDoneContext) Value(any) any { return nil }

func newSequentialUnionAllTest(t *testing.T) (*UnionAll, *merge.Merge, *process.Process) {
	t.Helper()
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.Reg.MergeReceivers = []*process.WaitRegister{{}, {}}
	for _, reg := range proc.Reg.MergeReceivers {
		reg.ResetForReuse(2, 1)
	}
	mergeOp := merge.NewArgument().WithPartial(0, 1)
	arg := NewArgument().WithSequentialBranches(2)
	arg.AppendChild(mergeOp)
	require.NoError(t, vm.Prepare(arg, proc))
	return arg, mergeOp, proc
}

func sendUnionAllTestSignal(
	t *testing.T,
	proc *process.Process,
	branch int,
	signal process.PipelineSignal,
) {
	t.Helper()
	require.True(t, process.SendPipelineSignalWithContext(
		context.Background(), proc.Reg.MergeReceivers[branch], signal))
}

func TestSequentialUnionAllActivatesNextBranchAfterExhaustion(t *testing.T) {
	arg, mergeOp, proc := newSequentialUnionAllTest(t)
	left := newBatch([]types.Type{types.T_int8.ToType()}, proc, 1)
	right := newBatch([]types.Type{types.T_int8.ToType()}, proc, 1)
	sendUnionAllTestSignal(t, proc, 0, process.NewPipelineSignalToDirectly(left, nil, proc.Mp()))
	sendUnionAllTestSignal(t, proc, 0, process.NewEndSignal())

	starts := 0
	arg.SetBranchStarter(func(branch int) error {
		require.Equal(t, 1, branch)
		starts++
		sendUnionAllTestSignal(t, proc, 1, process.NewPipelineSignalToDirectly(right, nil, proc.Mp()))
		sendUnionAllTestSignal(t, proc, 1, process.NewEndSignal())
		return nil
	})

	result, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.Same(t, left, result.Batch)
	require.Zero(t, starts)

	result, err = vm.Exec(arg, proc)
	require.NoError(t, err)
	require.Same(t, right, result.Batch)
	require.Equal(t, 1, starts)

	result, err = vm.Exec(arg, proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)

	mergeOp.Reset(proc, false, nil)
	arg.Reset(proc, false, nil)
	mergeOp.Free(proc, false, nil)
	arg.Free(proc, false, nil)
	mergeOp.Release()
	arg.Release()
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestSequentialUnionAllEarlyCleanupIgnoresUnstartedBranch(t *testing.T) {
	arg, mergeOp, proc := newSequentialUnionAllTest(t)
	left := newBatch([]types.Type{types.T_int8.ToType()}, proc, 1)
	sendUnionAllTestSignal(t, proc, 0, process.NewPipelineSignalToDirectly(left, nil, proc.Mp()))
	sendUnionAllTestSignal(t, proc, 0, process.NewEndSignal())

	starts := 0
	arg.SetBranchStarter(func(int) error {
		starts++
		return nil
	})
	result, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.Same(t, left, result.Batch)
	require.Zero(t, starts)

	// Model LIMIT stopping after the first batch. Cleanup must drain only the
	// active left receiver instead of waiting for a terminal signal from the
	// right branch, which was never started.
	done := make(chan struct{})
	go func() {
		mergeOp.Reset(proc, false, nil)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("merge cleanup waited for the unstarted UNION ALL branch")
	}
	require.Zero(t, starts)

	arg.Reset(proc, false, nil)
	mergeOp.Free(proc, false, nil)
	arg.Free(proc, false, nil)
	mergeOp.Release()
	arg.Release()
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestSequentialUnionAllBranchStartFailureIsDrained(t *testing.T) {
	arg, mergeOp, proc := newSequentialUnionAllTest(t)
	sendUnionAllTestSignal(t, proc, 0, process.NewEndSignal())
	wantErr := errors.New("start right branch")
	arg.SetBranchStarter(func(branch int) error {
		require.Equal(t, 1, branch)
		sendUnionAllTestSignal(t, proc, 1, process.NewErrorSignal(wantErr))
		return wantErr
	})

	_, err := vm.Exec(arg, proc)
	require.ErrorIs(t, err, wantErr)
	// The right receiver was installed before branch startup. Its terminal
	// startup error is therefore drainable during cleanup instead of remaining
	// buffered across a prepared-statement generation.
	mergeOp.Reset(proc, true, err)

	arg.Reset(proc, true, err)
	mergeOp.Free(proc, true, err)
	arg.Free(proc, true, err)
	mergeOp.Release()
	arg.Release()
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestSequentialUnionAllMissingStarterDoesNotActivateNextReceiver(t *testing.T) {
	arg, mergeOp, proc := newSequentialUnionAllTest(t)
	sendUnionAllTestSignal(t, proc, 0, process.NewEndSignal())

	_, err := vm.Exec(arg, proc)
	require.ErrorContains(t, err, "branch starter is not installed")
	// The missing callback is detected before the merge switches to the
	// unstarted receiver, so cleanup has no absent producer to wait for.
	mergeOp.Reset(proc, true, err)

	arg.Reset(proc, true, err)
	mergeOp.Free(proc, true, err)
	arg.Free(proc, true, err)
	mergeOp.Release()
	arg.Release()
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestSequentialUnionAllCancellationDoesNotActivateNextReceiver(t *testing.T) {
	arg, mergeOp, proc := newSequentialUnionAllTest(t)
	cancelCtx := &cancelOnNthDoneContext{done: make(chan struct{}), nth: 2}
	proc.Ctx = cancelCtx
	require.NoError(t, vm.Prepare(arg, proc))
	starts := 0
	arg.SetBranchStarter(func(int) error {
		starts++
		return nil
	})

	result, err := arg.Call(proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, vm.ExecStop, result.Status)
	require.Equal(t, int32(2), cancelCtx.calls.Load())
	require.Zero(t, starts)
	require.Zero(t, len(proc.Reg.MergeReceivers[1].Ch2))
	mergeOp.DisableReceiverWaitForStartFailure(proc)
	mergeOp.Reset(proc, true, err)

	arg.Reset(proc, true, err)
	mergeOp.Free(proc, true, err)
	arg.Free(proc, true, err)
	mergeOp.Release()
	arg.Release()
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestSequentialUnionAllReuseRestartsAtFirstBranch(t *testing.T) {
	arg, mergeOp, proc := newSequentialUnionAllTest(t)
	first := newBatch([]types.Type{types.T_int8.ToType()}, proc, 1)
	sendUnionAllTestSignal(t, proc, 0, process.NewPipelineSignalToDirectly(first, nil, proc.Mp()))
	sendUnionAllTestSignal(t, proc, 0, process.NewEndSignal())
	starts := 0
	arg.SetBranchStarter(func(int) error {
		starts++
		return nil
	})

	result, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.Same(t, first, result.Batch)
	mergeOp.Reset(proc, false, nil)
	arg.Reset(proc, false, nil)
	require.Zero(t, starts)

	for _, reg := range proc.Reg.MergeReceivers {
		reg.ResetForReuse(2, 1)
	}
	require.NoError(t, vm.Prepare(arg, proc))
	second := newBatch([]types.Type{types.T_int8.ToType()}, proc, 1)
	sendUnionAllTestSignal(t, proc, 0, process.NewPipelineSignalToDirectly(second, nil, proc.Mp()))
	sendUnionAllTestSignal(t, proc, 0, process.NewEndSignal())

	result, err = vm.Exec(arg, proc)
	require.NoError(t, err)
	require.Same(t, second, result.Batch)
	require.Zero(t, starts, "reuse must not resume at the previously active branch")
	mergeOp.Reset(proc, false, nil)
	arg.Reset(proc, false, nil)

	mergeOp.Free(proc, false, nil)
	arg.Free(proc, false, nil)
	mergeOp.Release()
	arg.Release()
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func newTestCase(t *testing.T, m *mpool.MPool, ts []types.Type) testCase {
	return testCase{
		types: ts,
		proc:  testutil.NewProcessWithMPool(t, "", m),
		arg:   &UnionAll{},
	}
}

func genTestCases(t *testing.T) []testCase {
	return []testCase{
		newTestCase(t, mpool.MustNewZero(), []types.Type{types.T_int8.ToType()}),
		newTestCase(t, mpool.MustNewZero(), []types.Type{types.T_int8.ToType()}),
		newTestCase(t, mpool.MustNewZero(), []types.Type{types.T_int8.ToType(), types.T_int64.ToType()}),
	}
}

func newBatch(ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}
func TestUnionall(t *testing.T) {
	for _, tc := range genTestCases(t) {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		bats := []*batch.Batch{
			newBatch(tc.types, tc.proc, 10),
			newBatch(tc.types, tc.proc, 10),
			batch.EmptyBatch,
		}
		resetChildren(tc.arg, bats)
		_, _ = vm.Exec(tc.arg, tc.proc)
		tc.arg.GetChildren(0).Free(tc.proc, false, nil)
		tc.arg.Reset(tc.proc, false, nil)

		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		bats = []*batch.Batch{
			newBatch(tc.types, tc.proc, 10),
			newBatch(tc.types, tc.proc, 10),
			batch.EmptyBatch,
		}
		resetChildren(tc.arg, bats)
		_, _ = vm.Exec(tc.arg, tc.proc)
		tc.arg.Free(tc.proc, false, nil)
		tc.arg.GetChildren(0).Free(tc.proc, false, nil)
		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func resetChildren(arg *UnionAll, bats []*batch.Batch) {
	op := colexec.NewMockOperator().WithBatchs(bats)
	arg.Children = nil
	arg.AppendChild(op)
}
