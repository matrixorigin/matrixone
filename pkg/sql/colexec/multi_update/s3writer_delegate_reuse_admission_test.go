// Copyright 2025 Matrix Origin
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

package multi_update

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func (s *issue25590WorkspaceStub) hookCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hookCalls
}

// issue25590ReuseProcRound builds ONE cached-plan execution's entry state:
// a fresh proc whose transaction operator resolves this round's workspace
// stub — exactly the way every new statement execution enters an
// already-cached plan with the CURRENT transaction's workspace.
func issue25590ReuseProcRound(t *testing.T) (*process.Process, *issue25590WorkspaceStub) {
	t.Helper()
	_, ctrl, proc := prepareTestCtx(t, true)
	stub := &issue25590WorkspaceStub{
		MockWorkspace: mock_frontend.NewMockWorkspace(ctrl),
	}
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().GetWorkspace().Return(stub).AnyTimes()
	proc.Base.TxnOperator = txnOp
	return proc, stub
}

// issue25590S3UpdateOp builds the cached-plan S3 MultiUpdate operator: one
// main-table update context with the UpdateWriteS3 action whose Prepare owns
// the persistent s3WriterDelegate across executions.
func issue25590S3UpdateOp() *MultiUpdate {
	_, tableDef := getTestMainTable()
	tableDef.Cols[0].Typ = pbplan.Type{Id: int32(types.T_int64)}
	updateCtx := &MultiUpdateCtx{
		TableDef:   tableDef,
		InsertCols: []int{0, 1, 2, 3},
		DeleteCols: []int{0, 1},
	}
	return &MultiUpdate{
		Action:         UpdateWriteS3,
		MultiUpdateCtx: []*MultiUpdateCtx{updateCtx},
	}
}

// issue25590SpillOneObjectViaSinkers walks the PRODUCTION persistence shape
// of one execution against the delegate's persistent insert sinkers:
// append's lazily-created sinkers stage the input batches, flushTail's Sync
// spills them (TailSizeCap 0), and the object crossing the persistence edge
// consults the sinker's capacity hook — under whichever workspace the hook
// currently names.
func issue25590SpillOneObjectViaSinkers(
	t *testing.T, update *MultiUpdate, proc *process.Process,
) error {
	t.Helper()
	writer := update.ctr.s3Writer
	if err := writer.ensureInsertSinkers(proc); err != nil {
		return err
	}
	bats, _ := prepareTestInsertBatchs(proc.Mp(), 2, colexec.DefaultBatchSize, false, false)
	defer func() {
		for _, bat := range bats {
			bat.Clean(proc.Mp())
		}
	}()
	for _, s3w := range writer.insertSinkers {
		if s3w == nil {
			continue
		}
		for _, bat := range bats {
			if err := s3w.Write(proc.Ctx, bat); err != nil {
				return err
			}
		}
		if _, err := s3w.Sync(proc.Ctx); err != nil {
			return err
		}
	}
	return nil
}

// TestMultiUpdatePrepareReuseRebindsPersistentSinkerCapacity pins the
// round-35 review's confirmed ownership defect: a cached TP plan's
// UpdateWriteS3 operator keeps its s3WriterDelegate — and, with it, the
// persistence hooks of ALREADY-CREATED insert sinkers — alive across
// executions, but before the fix each sinker's per-object capacity
// admission bound only at construction, so the FIRST transaction's
// workspace hook outlived its transaction: every next execution's spilled
// object reserved against the FINALIZED first workspace, whose terminal
// boundaries can never release or convert a later statement's
// reservation — repeated reuses permanently consumed engine-wide
// admission capacity until unrelated persistence was refused. Prepare
// must rebind every persistent sinker's admission to the CURRENT
// statement's workspace on reuse — before any next spill runs — and the
// rebound hook must still GATE the persistence: a refusal from the
// current workspace aborts the spill before anything persists.
func TestMultiUpdatePrepareReuseRebindsPersistentSinkerCapacity(t *testing.T) {
	proc1, stub1 := issue25590ReuseProcRound(t)
	proc2, stub2 := issue25590ReuseProcRound(t)
	_, ctrl3, proc3 := prepareTestCtx(t, true)
	stub3 := &issue25590WorkspaceStub{
		MockWorkspace: mock_frontend.NewMockWorkspace(ctrl3),
		refuse: func(string) error {
			return moerr.NewInternalErrorNoCtx("round-35 capacity refused")
		},
	}
	txnOp3 := mock_frontend.NewMockTxnOperator(ctrl3)
	txnOp3.EXPECT().GetWorkspace().Return(stub3).AnyTimes()
	proc3.Base.TxnOperator = txnOp3

	update := issue25590S3UpdateOp()

	// Execution 1: the cached plan runs its first statement under
	// workspace 1; the first spill creates the persistent sinker and
	// binds that workspace's hook.
	require.NoError(t, update.Prepare(proc1))
	require.NoError(t, issue25590SpillOneObjectViaSinkers(t, update, proc1))
	require.True(t, update.ctr.s3Writer.insertSinkers[0].HasUncommittedCapacityHook(),
		"the first execution's lazy sinker creation bound its own workspace's hook")
	require.Equal(t, 1, stub1.hookCallCount(),
		"execution 1's object reserved under workspace 1")
	// The between-executions pipeline reset keeps the writer, its
	// pools and its already-created sinkers.
	require.NoError(t, update.ctr.s3Writer.reset(proc1))

	// Execution 2: the SAME plan runs a different statement under
	// workspace 2. Prepare rebinds the persistent sinkers' admission to
	// the CURRENT workspace before any next spill.
	require.NoError(t, update.Prepare(proc2))
	require.NoError(t, issue25590SpillOneObjectViaSinkers(t, update, proc2))
	require.Equal(t, 1, stub2.hookCallCount(),
		"the reused sinker reserved execution 2's object under the CURRENT workspace")
	require.Equal(t, 1, stub1.hookCallCount(),
		"the finalized first workspace received NO second-execution reservation (the pre-fix shape stranded it there)")
	require.NoError(t, update.ctr.s3Writer.reset(proc2))
	sinkerNamesAfterExec2 := len(update.ctr.s3Writer.insertSinkers[0].PersistedNames())

	// Execution 3: the CURRENT workspace's bound governs the reused
	// sinker's persistence, not merely counts on the right owner — a
	// saturated current workspace refuses the spill with nothing new
	// persisted behind it.
	require.NoError(t, update.Prepare(proc3))
	require.ErrorContains(t,
		issue25590SpillOneObjectViaSinkers(t, update, proc3),
		"round-35 capacity refused",
		"the current workspace's admission refusal propagates out of the reused sinker's Sync")
	require.Equal(t, 1, stub3.hookCallCount(),
		"the refused spill consulted the CURRENT workspace's hook")
	require.Equal(t, sinkerNamesAfterExec2,
		len(update.ctr.s3Writer.insertSinkers[0].PersistedNames()),
		"the refused spill persisted nothing — no object joined the sinker's tracked references")
	require.Equal(t, 1, stub2.hookCallCount(),
		"workspace 2 heard nothing further after finalizing")

	require.NoError(t, update.ctr.s3Writer.free(proc3))
}

// TestMultiUpdatePrepareReuseWithoutOwnerClearsStaleHook pins the rebind's
// compatibility rule — the same rule construction-time binding follows: a
// reused plan whose next execution enters a workspace WITHOUT the
// SinkerOwner surface must end up UNHOOKED, never still wearing the
// finalized first transaction's hook — the pre-fix reuse left exactly
// that stale hook in place — and the unhooked writer keeps the pre-fix
// success behavior.
func TestMultiUpdatePrepareReuseWithoutOwnerClearsStaleHook(t *testing.T) {
	proc1, stub1 := issue25590ReuseProcRound(t)
	_, ctrl2, proc2 := prepareTestCtx(t, true)
	txnOp2 := mock_frontend.NewMockTxnOperator(ctrl2)
	txnOp2.EXPECT().GetWorkspace().
		Return(mock_frontend.NewMockWorkspace(ctrl2)).AnyTimes()
	proc2.Base.TxnOperator = txnOp2

	update := issue25590S3UpdateOp()

	require.NoError(t, update.Prepare(proc1))
	require.NoError(t, update.ctr.s3Writer.ensureInsertSinkers(proc1))
	require.True(t, update.ctr.s3Writer.insertSinkers[0].HasUncommittedCapacityHook())
	require.NoError(t, update.ctr.s3Writer.reset(proc1))

	require.NoError(t, update.Prepare(proc2))
	require.False(t, update.ctr.s3Writer.insertSinkers[0].HasUncommittedCapacityHook(),
		"an owner-less workspace removes the stale first hook — the fresh-construction compat shape, never the finalized workspace's")
	require.NoError(t, issue25590SpillOneObjectViaSinkers(t, update, proc2),
		"the unhooked reused writer keeps the pre-fix success behavior")
	require.Equal(t, 0, stub1.hookCallCount(),
		"the finalized first workspace's hook consulted nothing further")

	require.NoError(t, update.ctr.s3Writer.free(proc2))
}
