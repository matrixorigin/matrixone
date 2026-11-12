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

package disttae

import (
	"context"
	"errors"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
)

type workspaceCleanupFS struct {
	fileservice.FileService
	mu      sync.Mutex
	err     error
	deleted []string
	entered chan struct{}
	resume  chan struct{}
}

func (f *workspaceCleanupFS) Delete(ctx context.Context, names ...string) error {
	if f.entered != nil {
		close(f.entered)
		<-f.resume
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.deleted = append(f.deleted, names...)
	return nil
}

func TestBackportWorkspaceCleanupRefusesRepublication(t *testing.T) {
	e := &Engine{}
	owner := &Transaction{engine: e}
	require.NoError(t, e.admitUnpublishedPendingRecordedNames(owner, []string{"object"}))
	e.enqueueUnpublishedPendingFrom(owner, []string{"object"})
	fs := &workspaceCleanupFS{entered: make(chan struct{}), resume: make(chan struct{})}
	t.Cleanup(func() {
		select {
		case <-fs.resume:
		default:
			close(fs.resume)
		}
	})
	e.fs = fs
	done := make(chan error, 1)
	go func() { done <- e.deleteQueuedUnpublishedPendingOnce(context.Background()) }()
	<-fs.entered
	require.Error(t, e.admitUnpublishedPendingRecordedNames(&Transaction{engine: e}, []string{"object"}))
	close(fs.resume)
	require.NoError(t, <-done)
	require.NoError(t, e.admitUnpublishedPendingRecordedNames(&Transaction{engine: e}, []string{"new-object"}))
}

func TestBackportWorkspaceCleanupFailureRetainsOwnership(t *testing.T) {
	fs := &workspaceCleanupFS{err: errors.New("delete refused")}
	e := &Engine{fs: fs}
	owner := &Transaction{engine: e}
	require.NoError(t, e.admitUnpublishedPendingRecordedNames(owner, []string{"object"}))
	e.enqueueUnpublishedPendingFrom(owner, []string{"object"})
	require.Error(t, e.deleteQueuedUnpublishedPendingOnce(context.Background()))
	e.unpublishedPending.mu.Lock()
	require.Equal(t, []string{"object"}, e.unpublishedPending.names)
	e.unpublishedPending.mu.Unlock()
	fs.err = nil
	require.NoError(t, e.deleteQueuedUnpublishedPendingOnce(context.Background()))
	require.Empty(t, e.unpublishedPending.names)
	require.Equal(t, []string{"object"}, fs.deleted)
}

func TestBackportWorkspaceCleanupSharedClaims(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(map[bool]string{false: "both rollback", true: "one committed"}[committed], func(t *testing.T) {
			e := &Engine{}
			a, b := &Transaction{engine: e}, &Transaction{engine: e}
			require.NoError(t, e.admitUnpublishedPendingRecordedNames(a, []string{"shared"}))
			require.NoError(t, e.admitUnpublishedPendingRecordedNames(b, []string{"shared"}))
			require.EqualValues(t, 1, e.unpublishedPending.live)
			if committed {
				e.releaseUnpublishedPendingPublished(a, []string{"shared"})
			} else {
				e.enqueueUnpublishedPendingFrom(a, []string{"shared"})
			}
			require.Empty(t, e.unpublishedPending.names)
			require.EqualValues(t, 1, e.unpublishedPending.live)
			e.enqueueUnpublishedPendingFrom(b, []string{"shared"})
			require.Zero(t, e.unpublishedPending.live)
			if committed {
				require.Empty(t, e.unpublishedPending.names)
			} else {
				require.Equal(t, []string{"shared"}, e.unpublishedPending.names)
			}
		})
	}
}

func TestBackportWorkspaceCleanupSubmitFailureKeepsQueue(t *testing.T) {
	pool, err := ants.NewPool(1)
	require.NoError(t, err)
	pool.Release()
	e := &Engine{gcPool: pool, fs: &workspaceCleanupFS{}}
	e.enqueueUnpublishedPending([]string{"object"})
	e.waitUnpublishedPendingDrains(time.Second)
	e.unpublishedPending.mu.Lock()
	defer e.unpublishedPending.mu.Unlock()
	require.Equal(t, []string{"object"}, e.unpublishedPending.names)
	require.False(t, e.unpublishedPending.drainScheduled)
}

func TestBackportWorkspaceCleanupCloseWaitsForProducer(t *testing.T) {
	e := &Engine{fs: &workspaceCleanupFS{}}
	release, err := e.registerOrphanHandoffProducer()
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- e.closeWorkspaceGC(time.Now().Add(time.Second)) }()
	select {
	case <-done:
		t.Fatal("close lost active producer")
	default:
	}
	e.enqueueUnpublishedPending([]string{"late-object"})
	release()
	require.NoError(t, <-done)
	require.Empty(t, e.unpublishedPending.names)
	require.Error(t, e.reserveUnpublishedPendingObject("after-close"))
}

func TestBackportWorkspaceCleanupReservedWriterFencesClose(t *testing.T) {
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	e := txn.engine
	fs := &workspaceCleanupFS{}
	e.fs = fs
	require.NoError(t, e.reserveUnpublishedPendingObject("paused-writer"))
	txn.noteUnpublishedDumpReservation("paused-writer")
	done := make(chan error, 1)
	go func() { done <- e.closeWorkspaceGC(time.Now().Add(2 * time.Second)) }()
	require.Eventually(t, e.workspaceGCClosed.Load, time.Second, time.Millisecond)
	select {
	case <-done:
		t.Fatal("close returned before admitted persistence settled")
	default:
	}
	require.Error(t, e.reserveUnpublishedPendingObject("new-writer"))
	// A failed writer converts its reservation into a retained orphan, but that
	// intermediate txn-owned state still fences Close until the queue takes it.
	txn.retainDefiniteOrphansLocked([]string{"paused-writer"}, "paused writer failed")
	e.gcMu.Lock()
	producers := e.gcProducers
	e.gcMu.Unlock()
	require.EqualValues(t, 1, producers)
	select {
	case <-done:
		t.Fatal("conversion dropped the producer before handoff")
	default:
	}
	require.NoError(t, txn.handoffPendingObjectCleanupLocked(true))
	require.NoError(t, <-done)
	require.Equal(t, []string{"paused-writer"}, fs.deleted)
	require.Empty(t, e.unpublishedPending.producerNames)
	require.Zero(t, e.gcProducers)
	require.Zero(t, e.unpublishedPending.inFlight)
	require.Zero(t, e.unpublishedPending.live)
	// Registration after shutdown cannot create a new unfenced cleanup producer.
	release, err := e.registerOrphanHandoffProducer()
	require.Error(t, err)
	require.Nil(t, release)
	require.Zero(t, e.gcProducers)
}

func TestBackportWorkspaceUnknownCommitProtectsSharedObject(t *testing.T) {
	ctx := context.Background()
	colexec.NewServer("")
	fs := newCleanFS(t)
	e := &Engine{fs: fs, cloneTxnCache: newCloneTxnCache()}
	budget := process.MustNewHashBuildBudget(16<<20, 8<<20)
	parent, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	t.Cleanup(parent.Close)
	makeTxn := func() *Transaction {
		op, closeOp := client.NewTestTxnOperator(ctx)
		t.Cleanup(closeOp)
		txn := &Transaction{op: op, proc: testutil.NewProc(t), engine: e, cnObjsSummary: make(map[types.Objectid]Summary), batchSelectList: make(map[*batch.Batch][]int64), deletedBlocks: &deletedBlocks{offsets: make(map[types.Blockid][]int64)}}
		require.NoError(t, txn.proc.SetHashBuildBudgetForTesting(parent))
		op.AddWorkspace(txn)
		return txn
	}
	a, b := makeTxn(), makeTxn()
	colexec.NewServer("")
	stats := mockStatsList(t, 1)
	name := stats[0].ObjectName().String()
	require.NoError(t, writeObjectToFS(ctx, fs, name))
	for _, txn := range []*Transaction{a, b} {
		metadata := cloneObjectStatsBatchForTest(t, txn.proc.Mp(), stats...)
		require.NoError(t, txn.writeFileLockedWithAutoIncrEpochKnown(INSERT, 1, 7, 42, "db", "tbl", name, metadata, DNStore{}, 0, false))
		metadata.Clean(txn.proc.Mp())
		require.Positive(t, txn.workspaceAccount.Snapshot().Used)
	}
	a.SetCloneTxn(1)
	e.cloneTxnCache.AddTxnLocalSharedFile(a.op.Txn().ID, name)
	a.FinalizeCommitWithUnknownResult(ctx)
	require.True(t, a.removed)
	require.Zero(t, a.workspaceAccount.Snapshot().Used)
	require.EqualValues(t, 1, e.unpublishedPending.live)
	require.True(t, objectExistsInFS(ctx, fs, name))
	require.NoError(t, b.Rollback(ctx))
	require.Zero(t, b.workspaceAccount.Snapshot().Used)
	require.Zero(t, e.unpublishedPending.live)
	require.Empty(t, e.unpublishedPending.names)
	require.NoError(t, e.deleteQueuedUnpublishedPendingOnce(ctx))
	require.True(t, objectExistsInFS(ctx, fs, name), "unknown commit may have published this object")
}

// GC has entered its public path, but has not acquired a producer permit.
// Close wins admission; the resumed caller must fail without transferring its
// original claim or appending an orphan after the successful close.
func TestBackportWorkspaceCleanupCloseWinsGCAdmission(t *testing.T) {
	ctx := context.Background()
	fs := newCleanFS(t)
	e := &Engine{fs: fs}
	const name = "gc-entry-before-admission"
	require.NoError(t, writeObjectToFS(ctx, fs, name))
	source := batch.NewWithSize(0)
	t.Cleanup(func() { source.Clean(nil) })
	txn := &Transaction{engine: e, writes: []Entry{{typ: INSERT, fileName: name, bat: source}}}
	require.NoError(t, e.admitUnpublishedPendingRecordedNames(txn, []string{name}))
	entered, resume := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(resume) }) }
	previousHook := gcFilesBeforeAdmit
	gcFilesBeforeAdmit = func() { close(entered); <-resume }
	done := make(chan error, 1)
	exited := make(chan struct{})
	go func() { defer close(exited); done <- gcFilesToOwner(txn, cloneGCIntermediate, name) }()
	t.Cleanup(func() { release(); <-exited; gcFilesBeforeAdmit = previousHook })
	<-entered
	require.NoError(t, e.closeWorkspaceGC(time.Now().Add(time.Second)))
	require.True(t, e.workspaceGCClosed.Load())
	release()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("GC admission did not return after close")
	}
	require.Len(t, txn.writes, 1)
	require.Same(t, source, txn.writes[0].bat)
	require.False(t, txn.removed)
	e.unpublishedPending.mu.Lock()
	_, retained := e.unpublishedPending.ownedNames[name][txn]
	queued := len(e.unpublishedPending.names)
	live := e.unpublishedPending.live
	e.unpublishedPending.mu.Unlock()
	require.True(t, retained, "rejected cleanup must leave the original claim with the transaction")
	require.Zero(t, queued, "no enqueue after Close succeeded")
	require.EqualValues(t, 1, live)
	require.Zero(t, e.gcProducers)
	require.True(t, objectExistsInFS(ctx, fs, name))
}
