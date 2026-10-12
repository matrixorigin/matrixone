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
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type deadlineDeleteFS struct {
	fileservice.FileService
	deadlines       []time.Time
	recovered       bool
	deadlineCeiling time.Time
}

func (fs *deadlineDeleteFS) Delete(ctx context.Context, names ...string) error {
	d, _ := ctx.Deadline()
	fs.deadlines = append(fs.deadlines, d)
	if fs.recovered {
		return fs.FileService.Delete(ctx, names...)
	}
	// Fail quickly if a regression reintroduces a ten-minute writer budget.
	if d.After(fs.deadlineCeiling) {
		return errors.New("writer restarted cleanup budget")
	}
	<-ctx.Done()
	return ctx.Err()
}

type cleanupWritersOperator struct {
	*colexec.MockOperator
	writers []*colexec.CNS3Writer
	calls   []writerCleanupCall
}

type writerCleanupCall struct {
	kind        string
	failed      bool
	err         error
	cause       error
	deadline    time.Time
	hasDeadline bool
}

func (op *cleanupWritersOperator) record(proc *process.Process, kind string, failed bool, err error) {
	d, ok := process.PipelineCleanupDeadline(proc.Ctx)
	op.calls = append(op.calls, writerCleanupCall{kind, failed, err, context.Cause(proc.Ctx), d, ok})
}

func (op *cleanupWritersOperator) Reset(proc *process.Process, failed bool, err error) {
	op.record(proc, "Reset", failed, err)
	for _, writer := range op.writers {
		_ = writer.ResetWithCleanup(proc.Ctx, true)
	}
}

func (op *cleanupWritersOperator) Free(proc *process.Process, failed bool, err error) {
	op.record(proc, "Free", failed, err)
	for _, writer := range op.writers {
		_ = writer.CloseWithCleanup(proc.Ctx, true)
	}
}

func TestPipelineCleanupPreservesExecutionProvenanceAndBudget(t *testing.T) {
	businessErr := moerr.NewInternalErrorNoCtx("execution failed before registration")
	for _, tc := range []struct {
		name    string
		failed  bool
		err     error
		wantErr error
	}{
		{"success", false, nil, nil},
		{"failure-without-cause", true, nil, process.ErrPipelineTerminalWithoutCause},
		{"business-error", true, businessErr, businessErr},
		{"cancellation", true, context.Canceled, context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProc(t)
			t.Cleanup(proc.Free)
			baseFS, err := colexec.GetSharedFSFromProc(proc)
			require.NoError(t, err)
			fs := &deadlineDeleteFS{FileService: baseFS, recovered: true}
			writer := colexec.NewCNS3DataWriter(proc.Mp(), fs, &plan.TableDef{
				Pkey: &plan.PrimaryKeyDef{}, Cols: []*plan.ColDef{{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}}},
			}, 1, false)
			t.Cleanup(func() { require.NoError(t, writer.CloseWithCleanup(context.Background(), true)) })
			bat := &batch.Batch{Attrs: []string{"a"}, Vecs: []*vector.Vector{testutil.MakeInt64Vector([]int64{1}, nil, proc.Mp())}}
			bat.SetRowCount(1)
			t.Cleanup(func() { bat.Clean(proc.Mp()) })
			op := &cleanupWritersOperator{MockOperator: colexec.NewMockOperator(), writers: []*colexec.CNS3Writer{writer}}
			p := New(0, nil, op)
			var previousDeadline time.Time
			// Reuse the same operator/writer after prepared Reset, then perform
			// Reset and Free in a fresh execution context.
			for execution := 0; execution < 2; execution++ {
				proc.ResetQueryContext()
				ctx := proc.Base.GetContextBase().BuildQueryCtx(proc.GetTopContext())
				proc.BuildPipelineContext(ctx)
				_, ok := process.PipelineCleanupDeadline(proc.Ctx)
				require.False(t, ok, "prepared reuse must begin with a fresh budget")
				require.NoError(t, writer.Write(proc.Ctx, bat))
				info, err := writer.SyncAndFillBlockInfoBat(proc.Ctx)
				require.NoError(t, err)
				require.NotZero(t, info.Vecs[1].Length())
				stats := objectio.ObjectStats(info.Vecs[1].GetBytesAt(0))
				name := stats.ObjectName().String()
				_, err = baseFS.StatFile(context.Background(), name)
				require.NoError(t, err)
				op.calls, fs.deadlines = nil, nil
				cancel := proc.Cancel
				var deadlineAtCancel time.Time
				var budgetAtCancel bool
				proc.Cancel = func(cause error) {
					deadlineAtCancel, budgetAtCancel = process.PipelineCleanupDeadline(proc.Ctx)
					cancel(cause)
				}
				prepared := execution == 0
				p.Cleanup(proc, tc.failed, prepared, tc.err)
				proc.Cancel = cancel
				require.True(t, budgetAtCancel, "budget must precede cancellation")
				d, ok := process.PipelineCleanupDeadline(proc.Ctx)
				require.True(t, ok)
				require.Equal(t, d, deadlineAtCancel)
				if execution > 0 {
					require.True(t, d.After(previousDeadline), "a later execution must get a new deadline")
				}
				previousDeadline = d
				wantCalls := []string{"Reset"}
				if !prepared {
					wantCalls = append(wantCalls, "Free")
				}
				require.Len(t, op.calls, len(wantCalls))
				wantCause := tc.wantErr
				if wantCause == nil {
					wantCause = process.ErrPipelineStopped
				}
				for i, call := range op.calls {
					require.Equal(t, wantCalls[i], call.kind)
					require.Equal(t, tc.failed, call.failed)
					require.True(t, call.err == tc.wantErr, "execution error identity must survive cleanup")
					require.True(t, call.cause == wantCause, "cancellation must retain its separate cause")
					require.True(t, call.hasDeadline)
					require.Equal(t, d, call.deadline)
				}
				require.NotEmpty(t, fs.deadlines)
				for _, deadline := range fs.deadlines {
					require.Equal(t, d, deadline, "nested writer cleanup must share the execution budget")
				}
				_, err = baseFS.StatFile(context.Background(), name)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), "cleanup must physically delete unregistered output")
				require.False(t, writer.PendingUnpublishedCleanup())
			}
		})
	}
}

func TestPipelineCleanupSharesBudgetAcrossWritersAndFree(t *testing.T) {
	old := process.PipelineCleanupTimeout
	process.PipelineCleanupTimeout = 40 * time.Millisecond
	t.Cleanup(func() { process.PipelineCleanupTimeout = old })
	for _, rootOnly := range []bool{false, true} {
		proc := testutil.NewProc(t)
		proc.BuildPipelineContext(proc.Ctx)
		_, hasDeadline := proc.Ctx.Deadline()
		require.False(t, hasDeadline, "no request deadline may mask budget restarts")
		baseFS, err := colexec.GetSharedFSFromProc(proc)
		require.NoError(t, err)
		fs := &deadlineDeleteFS{FileService: baseFS}
		op := &cleanupWritersOperator{MockOperator: colexec.NewMockOperator()}
		bat := &batch.Batch{Attrs: []string{"a"}, Vecs: []*vector.Vector{testutil.MakeInt64Vector([]int64{1}, nil, proc.Mp())}}
		bat.SetRowCount(1)
		for i := 0; i < 4; i++ {
			writer := colexec.NewCNS3DataWriter(proc.Mp(), fs, &plan.TableDef{Pkey: &plan.PrimaryKeyDef{}, Cols: []*plan.ColDef{{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}}}}, 1, false)
			require.NoError(t, writer.Write(proc.Ctx, bat))
			info, err := writer.SyncAndFillBlockInfoBat(proc.Ctx)
			require.NoError(t, err)
			info.Clean(proc.Mp())
			op.writers = append(op.writers, writer)
		}
		p := New(0, nil, op)
		start := time.Now()
		fs.deadlineCeiling = start.Add(time.Second)
		if rootOnly {
			p.CleanRootOperator(proc, true, false, context.Canceled)
		} else {
			p.Cleanup(proc, true, false, context.Canceled)
		}
		require.Less(t, time.Since(start), time.Second)
		d, ok := process.PipelineCleanupDeadline(proc.Ctx)
		require.True(t, ok)
		require.NotEmpty(t, fs.deadlines)
		for _, deadline := range fs.deadlines {
			require.Equal(t, d, deadline, "Reset and Free must never restart the budget")
		}
		// Teardown leaves obligations intact. A new background attempt gets its
		// own budget rather than inheriting the expired execution deadline.
		fs.recovered = true
		for _, writer := range op.writers {
			require.NoError(t, writer.CloseWithCleanup(context.Background(), true))
		}
		bat.Clean(proc.Mp())
		proc.Free()
	}
}

type resetOrderOperator struct {
	*colexec.MockOperator
	name       string
	order      *[]string
	resetFirst bool
}

var _ vm.Operator = (*resetOrderOperator)(nil)

func (op *resetOrderOperator) Reset(*process.Process, bool, error) {
	*op.order = append(*op.order, op.name)
}

func (op *resetOrderOperator) ResetBeforeChildren() bool {
	return op.resetFirst
}

func newResetOrderOperator(name string, order *[]string, resetFirst bool) *resetOrderOperator {
	return &resetOrderOperator{
		MockOperator: colexec.NewMockOperator(),
		name:         name,
		order:        order,
		resetFirst:   resetFirst,
	}
}

func TestResetOperatorTreeHonorsChildOwnerOrdering(t *testing.T) {
	var order []string
	root := newResetOrderOperator("root", &order, false)
	owner := newResetOrderOperator("owner", &order, true)
	leaf := newResetOrderOperator("leaf", &order, false)
	root.AppendChild(owner)
	owner.AppendChild(leaf)

	resetDone := make(map[vm.Operator]struct{})
	resetChildOwners(root, resetDone, nil, true, context.Canceled)
	resetOperatorTree(root, nil, resetDone, nil, true, context.Canceled)

	want := []string{"owner", "leaf", "root"}
	if len(order) != len(want) {
		t.Fatalf("unexpected reset order %v", order)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("unexpected reset order %v", order)
		}
	}
}

func TestCleanupInOrderReturnsWhenMergeEndSignalIsMissing(t *testing.T) {
	oldCleanupWaitTimeout := process.PipelineCleanupWaitTimeout
	oldSignalSendTimeout := process.PipelineSignalSendTimeout
	process.PipelineCleanupWaitTimeout = time.Second
	process.PipelineSignalSendTimeout = time.Second
	t.Cleanup(func() {
		process.PipelineCleanupWaitTimeout = oldCleanupWaitTimeout
		process.PipelineSignalSendTimeout = oldSignalSendTimeout
	})

	proc := process.NewTopProcess(context.Background(), mpool.MustNewZeroNoFixed(), nil, nil, nil, nil, nil, nil, nil, nil, nil)
	proc.BuildPipelineContext(context.Background())

	reg := &process.WaitRegister{Ch2: make(chan process.PipelineSignal, 1)}
	proc.Reg.MergeReceivers = []*process.WaitRegister{reg}

	mergeOp := merge.NewArgument()
	t.Cleanup(mergeOp.Release)

	connectorOp := connector.NewArgument().WithReg(reg)
	t.Cleanup(connectorOp.Release)
	connectorOp.AppendChild(mergeOp)
	if err := connectorOp.Prepare(proc); err != nil {
		t.Fatal(err)
	}

	p := New(0, nil, connectorOp)

	done := make(chan struct{})
	start := time.Now()
	go func() {
		p.Cleanup(proc, true, true, nil)
		close(done)
	}()

	select {
	case <-done:
		if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
			t.Fatalf("pipeline cleanup did not take the sender/receiver fast cleanup path, elapsed %s", elapsed)
		}
	case <-time.After(time.Second):
		t.Fatal("pipeline cleanup did not return after the merge cleanup timeout")
	}
}
