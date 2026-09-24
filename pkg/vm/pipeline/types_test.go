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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
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
	if fs.recovered {
		return fs.FileService.Delete(ctx, names...)
	}
	d, _ := ctx.Deadline()
	fs.deadlines = append(fs.deadlines, d)
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
}

func (op *cleanupWritersOperator) Reset(proc *process.Process, _ bool, _ error) {
	for _, writer := range op.writers {
		_ = writer.ResetWithCleanup(proc.Ctx, true)
	}
}

func (op *cleanupWritersOperator) Free(proc *process.Process, _ bool, _ error) {
	for _, writer := range op.writers {
		_ = writer.CloseWithCleanup(proc.Ctx, true)
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
