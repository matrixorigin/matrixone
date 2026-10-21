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

package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Use only APIs present in stock 4.1.6 so these same tests can demonstrate the
// regression there. The queue state, not a sleep, forces the cleanup failure.
func TestPipelineCleanupDeliversEndAfterBackpressure(t *testing.T) {
	shortTerminalSendTimeout(t)
	for _, sender := range terminalTestSenders() {
		t.Run(sender.name, func(t *testing.T) {
			mp := mpool.MustNewZeroNoFixed()
			t.Cleanup(func() { mpool.DeleteMPool(mp) })
			proc := terminalTestProcess(t, mp, context.Background())
			sourceMP := mpool.MustNewZeroNoFixed()
			t.Cleanup(func() { mpool.DeleteMPool(sourceMP) })
			first := terminalTestBatch(t, sourceMP, 1)
			second := terminalTestBatch(t, sourceMP, 2)
			regs := terminalTestRegisters(sender.receivers, 2)
			op := sender.newOperator(regs)
			t.Cleanup(op.Release)
			op.GetOperatorBase().AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{first, second}))
			cleanup := terminalTestCleanup(t, proc, op)
			require.NoError(t, vm.Prepare(op, proc))

			for range 2 {
				result, err := vm.Exec(op, proc)
				require.NoError(t, err)
				require.Equal(t, 1, result.Batch.RowCount())
			}
			for _, reg := range regs {
				require.Equal(t, cap(reg.Ch2), len(reg.Ch2), "data notification queue must be full before cleanup")
			}
			require.Positive(t, mp.CurrNB())

			// The consumer is deliberately absent until actual pipeline cleanup
			// has returned. Both spool slots and each notification queue are full.
			cleanup(nil)
			require.Positive(t, mp.CurrNB(), "cleanup must preserve queued batch memory")

			for _, reg := range regs {
				consumer, consumerProc := terminalTestConsumer(t, reg)
				for _, want := range []int64{1, 2} {
					result, err := vm.Exec(consumer, consumerProc)
					require.NoError(t, err)
					require.NotNil(t, result.Batch)
					require.Equal(t, []int64{want}, vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[0]))
				}
				result, err := vm.Exec(consumer, consumerProc)
				require.NoError(t, consumerProc.Ctx.Err(), "End was lost: receiver required cancellation after buffered data drained")
				require.NoError(t, err)
				require.Nil(t, result.Batch)
				require.Equal(t, vm.ExecStop, result.Status)
			}
			require.Zero(t, mp.CurrNB(), "last receiver must release all copied off-heap batches")
		})
	}
}

func TestPipelineCleanupPreservesSpoolAllocationFailure(t *testing.T) {
	shortTerminalSendTimeout(t)
	for _, sender := range terminalTestSenders() {
		t.Run(sender.name, func(t *testing.T) {
			// One pending row fits; copying the next row deterministically
			// exceeds this pool. No global allocator hook or OS OOM is needed.
			mp, err := mpool.NewMPool("pipeline-terminal-allocation-failure", 1<<20, mpool.NoFixed)
			require.NoError(t, err)
			t.Cleanup(func() { mpool.DeleteMPool(mp) })
			proc := terminalTestProcess(t, mp, context.Background())
			sourceMP := mpool.MustNewZeroNoFixed()
			t.Cleanup(func() { mpool.DeleteMPool(sourceMP) })
			first := terminalTestBatch(t, sourceMP, 1)
			tooLarge := batch.NewWithSize(1)
			t.Cleanup(func() { tooLarge.Clean(sourceMP) })
			tooLarge.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
			require.NoError(t, vector.AppendBytes(tooLarge.Vecs[0], make([]byte, 2<<20), false, sourceMP))
			tooLarge.SetRowCount(1)
			regs := terminalTestRegisters(sender.receivers, 1)
			op := sender.newOperator(regs)
			t.Cleanup(op.Release)
			op.GetOperatorBase().AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{first, tooLarge}))
			cleanup := terminalTestCleanup(t, proc, op)
			require.NoError(t, vm.Prepare(op, proc))
			_, err = vm.Exec(op, proc)
			require.NoError(t, err)
			for _, reg := range regs {
				require.Equal(t, cap(reg.Ch2), len(reg.Ch2))
			}
			require.Positive(t, mp.CurrNB())

			_, allocationErr := vm.Exec(op, proc)
			require.Error(t, allocationErr)
			require.True(t, moerr.IsMoErrCode(allocationErr, moerr.ErrInternal))
			require.Contains(t, allocationErr.Error(), "mpool out of space")
			cleanup(allocationErr)

			for _, reg := range regs {
				consumer, consumerProc := terminalTestConsumer(t, reg)
				result, err := vm.Exec(consumer, consumerProc)
				require.NoError(t, consumerProc.Ctx.Err(), "failed cleanup must wake the receiver independently of query cancellation")
				require.Nil(t, result.Batch)
				require.Same(t, allocationErr, err, "receiver must observe the actual failed allocation")
			}
			require.Zero(t, mp.CurrNB(), "failed admission and pending batches must be reclaimed")
		})
	}
}

type terminalTestSender struct {
	name        string
	receivers   int
	newOperator func([]*process.WaitRegister) vm.Operator
}

func terminalTestSenders() []terminalTestSender {
	return []terminalTestSender{
		{name: "connector", receivers: 1, newOperator: func(regs []*process.WaitRegister) vm.Operator {
			return connector.NewArgument().WithReg(regs[0])
		}},
		{name: "broadcast_dispatch", receivers: 2, newOperator: func(regs []*process.WaitRegister) vm.Operator {
			op := dispatch.NewArgument()
			op.FuncId = dispatch.SendToAllLocalFunc
			op.LocalRegs = regs
			return op
		}},
	}
}

func shortTerminalSendTimeout(t *testing.T) {
	t.Helper()
	old := process.PipelineSignalSendTimeout
	oldWait := process.PipelineCleanupWaitTimeout
	// A full queue is the trigger. This only bounds the stock implementation's
	// cleanup sends so the same negative test does not need a 30+30 second wait.
	process.PipelineSignalSendTimeout = time.Nanosecond
	process.PipelineCleanupWaitTimeout = time.Second
	t.Cleanup(func() {
		process.PipelineSignalSendTimeout = old
		process.PipelineCleanupWaitTimeout = oldWait
	})
}

func terminalTestCleanup(t *testing.T, proc *process.Process, op vm.Operator) func(error) {
	t.Helper()
	p := New(0, nil, op)
	cleaned := false
	cleanup := func(err error) {
		if !cleaned {
			p.Cleanup(proc, err != nil, true, err)
			cleaned = true
		}
	}
	t.Cleanup(func() { cleanup(moerr.NewInternalErrorNoCtx("test interrupted before pipeline cleanup")) })
	return cleanup
}

func terminalTestRegisters(count, capacity int) []*process.WaitRegister {
	regs := make([]*process.WaitRegister, count)
	for i := range regs {
		regs[i] = &process.WaitRegister{Ch2: make(chan process.PipelineSignal, capacity)}
	}
	return regs
}

func terminalTestProcess(t *testing.T, mp *mpool.MPool, ctx context.Context) *process.Process {
	t.Helper()
	proc := process.NewTopProcess(ctx, mp, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	proc.BuildPipelineContext(ctx)
	t.Cleanup(func() { proc.Cancel(nil) })
	return proc
}

func terminalTestConsumer(t *testing.T, reg *process.WaitRegister) (*merge.Merge, *process.Process) {
	t.Helper()
	// Keep the consumer independent of the producer context that Cleanup cancels.
	// A deadline is a hang guard; successful terminal delivery must beat cancel.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	proc := terminalTestProcess(t, mp, ctx)
	proc.Reg.MergeReceivers = []*process.WaitRegister{reg}
	op := merge.NewArgument()
	t.Cleanup(func() {
		op.Reset(proc, false, nil)
		op.Release()
	})
	require.NoError(t, op.Prepare(proc))
	return op, proc
}

func terminalTestBatch(t *testing.T, mp *mpool.MPool, value int64) *batch.Batch {
	t.Helper()
	bat := batch.NewWithSize(1)
	t.Cleanup(func() { bat.Clean(mp) })
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], value, false, mp))
	bat.SetRowCount(1)
	return bat
}
