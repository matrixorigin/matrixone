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

package disttae

// D1' controls (issue #25590 design r11, closing the round-24 finding
// F1): a CLONE-COMMITTED-BORROWED name — an object this clone
// transaction borrowed from committed state, exactly the class gcFiles'
// filter protects at every scope (cloneTxnCache.IsSharedFile) — must
// never enter the orphan machinery: not through either retain funnel,
// not through the handoff's moved split. TXN-LOCAL shared names are a
// different class: they keep the r21/r22 definite-orphan semantics —
// a failed preparation's retain still OWNS them, and the terminal
// rollback's handoff plus the queue drain still deletes them physically.

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
)

// newCloneBorrowTxnForR11 builds the lite clone fixture the
// table_meta_reader tests established (memory FS, gc pool, clone cache,
// SetCloneTxn) plus the queue members the orphan machinery touches, and
// registers the first stats object as the committed-borrowed witness,
// the second as the txn-local share and the third as a genuinely fresh
// name.
func newCloneBorrowTxnForR11(
	t *testing.T,
) (*Transaction, fileservice.FileService, objectio.ObjectStats, string, string) {
	t.Helper()
	ctx := context.Background()
	fs := newCleanFS(t)

	gcPool, err := ants.NewPool(2)
	require.NoError(t, err)
	t.Cleanup(func() { gcPool.Release() })

	txnOp, closeFn := client.NewTestTxnOperator(ctx)
	t.Cleanup(closeFn)

	txn := &Transaction{
		engine: &Engine{
			cloneTxnCache: newCloneTxnCache(),
			gcPool:        gcPool,
			fs:            fs,
		},
		op: txnOp,
	}
	txnOp.AddWorkspace(txn)
	txn.SetCloneTxn(1)

	stats := mockStatsList(t, 3)
	borrowed := stats[0]
	txnLocalName := stats[1].ObjectName().String()
	freshName := stats[2].ObjectName().String()
	borrowedName := borrowed.ObjectName().String()
	require.NoError(t, writeObjectToFS(ctx, fs, borrowedName))
	require.NoError(t, writeObjectToFS(ctx, fs, txnLocalName))
	require.NoError(t, writeObjectToFS(ctx, fs, freshName))

	txn.engine.cloneTxnCache.AddSharedFile(txn.op.Txn().ID, borrowedName)
	txn.engine.cloneTxnCache.AddTxnLocalSharedFile(txn.op.Txn().ID, txnLocalName)
	return txn, fs, borrowed, txnLocalName, freshName
}

// Control C at both retain funnels: the committed-borrowed witness
// never lands on pendingObjectCleanup through either funnel, while the
// genuinely fresh name in the SAME batch still lands — both directions
// pinned in one batch per funnel.
func TestIssue25590CloneBorrowedNeverEntersRetainFunnelsR24ControlC(t *testing.T) {
	ctx := context.Background()
	txn, fs, borrowed, txnLocalName, freshName := newCloneBorrowTxnForR11(t)
	borrowedName := borrowed.ObjectName().String()

	// The definite-orphan funnel (a failed record preparation).
	txn.Lock()
	txn.retainDefiniteOrphansLocked([]string{borrowedName, freshName},
		"r11 D1' control C: the definite funnel")
	txn.Unlock()
	require.Equal(t, []string{freshName}, txn.pendingObjectCleanupNames(),
		"D1' control C: the committed-borrowed witness never lands through the definite funnel while the fresh sibling lands")

	// The publication-in-flight funnel (the r5 exported retainer surface
	// the operator teardowns serve).
	txn.Lock()
	txn.pendingObjectCleanup = nil
	txn.Unlock()
	txn.RetainUnrecordedS3Objects([]string{borrowedName, freshName},
		"r11 D1' control C: the publication-in-flight funnel")
	require.Equal(t, []string{freshName}, txn.pendingObjectCleanupNames(),
		"D1' control C: the committed-borrowed witness never lands through the r5 funnel either")

	// The witness is untouched physically at this point — the batch
	// split under test, not an empty-input or write accident.
	require.True(t, objectExistsInFS(ctx, fs, borrowedName))
	_ = txnLocalName

	// Boundary: a NON-clone transaction keeps the base semantics — the
	// same class exists on the engine's cache, but the predicate is
	// scoped to clone transactions, so an ordinary transaction's
	// un-recorded persisted name still flows.
	plainOp, plainClose := client.NewTestTxnOperator(ctx)
	t.Cleanup(plainClose)
	plainTxn := &Transaction{engine: txn.engine, op: plainOp}
	plainOp.AddWorkspace(plainTxn)
	txn.engine.cloneTxnCache.AddSharedFile(plainOp.Txn().ID, borrowedName)
	plainTxn.Lock()
	plainTxn.retainDefiniteOrphansLocked([]string{borrowedName},
		"r11 D1' boundary: a non-clone transaction is outside the class")
	plainTxn.Unlock()
	require.Equal(t, []string{borrowedName}, plainTxn.pendingObjectCleanupNames(),
		"a non-clone transaction carries no borrowed class, so its name flows as before")
	// Release the boundary transaction's list and its cache entry so
	// nothing leaks into the other controls' assertions.
	plainTxn.Lock()
	plainTxn.pendingObjectCleanup = nil
	plainTxn.Unlock()
	txn.engine.cloneTxnCache.DeleteTxn(plainOp.Txn().ID)
}

// Control C at the handoff: the moved split applies the SAME predicate,
// so even a name seeded directly on the transaction's list (a
// pre-predicate path) is dropped without ever reaching the engine's
// queue, while the txn-local and fresh siblings in the same list move.
// This is the designed defense-in-depth second gate of the
// one-predicate contract.
func TestIssue25590CloneBorrowedNeverHandedOffToQueueR24ControlC(t *testing.T) {
	txn, _, borrowed, txnLocalName, freshName := newCloneBorrowTxnForR11(t)

	// Seed the list directly — the handoff cannot assume the retain
	// funnels were the only writer (the exact scenario the round-24
	// review demanded the moved split cover).
	txn.Lock()
	txn.pendingObjectCleanup = []pendingObjectCleanupItem{
		{name: borrowed.ObjectName().String()},
		{name: txnLocalName},
		{name: freshName},
	}
	txn.handoffPendingObjectCleanupLocked(true)
	txn.Unlock()

	require.ElementsMatch(t, []string{txnLocalName, freshName},
		issue25590QueuedNames(txn.engine),
		"D1' control C: the handoff's moved split drops the borrowed name and moves exactly the txn-local and fresh siblings")
	require.Empty(t, txn.pendingObjectCleanupNames(),
		"the quiesced terminal handoff drains the transaction's list")
}

// Control A: the intermediate retain — the failed-preparation class a
// live statement's teardown performs — still OWNS the txn-local shared
// name (it lands, so a mid-statement leak is impossible), while the
// committed-borrowed witness in the SAME batch never lands. Pinning
// both halves in one batch is what keeps the exemption from drifting
// into the txn-local class.
func TestIssue25590CloneTxnLocalInterRetainOwnedWhileBorrowedSiblingNeverLandsR24ControlA(t *testing.T) {
	txn, _, borrowed, txnLocalName, _ := newCloneBorrowTxnForR11(t)
	borrowedName := borrowed.ObjectName().String()

	txn.RetainUnrecordedS3Objects([]string{borrowedName, txnLocalName},
		"r11 D1' control A: the intermediate retain")
	require.Equal(t, []string{txnLocalName}, txn.pendingObjectCleanupNames(),
		"D1' control A: the txn-local share stays owned while the borrowed sibling never lands")

	// The definite funnel shows the same split for the other class.
	txn.Lock()
	txn.pendingObjectCleanup = nil
	txn.retainDefiniteOrphansLocked([]string{borrowedName, txnLocalName},
		"r11 D1' control A: the definite variant")
	txn.Unlock()
	require.Equal(t, []string{txnLocalName}, txn.pendingObjectCleanupNames(),
		"D1' control A: the definite funnel splits the batch the same way")
}

// Control B: the owned txn-local name is physically deleted by the
// terminal drain AFTER the rollback terminal's handoff — the existing
// guarantee of TestCloneTxnRollbackGCDeletesTxnLocalSharedObjects re-
// proven THROUGH the orphan-machinery path (funnel -> terminal handoff
// -> queue -> drain) beside the untouched existing test.
func TestIssue25590CloneTxnLocalDeletedByRollbackTerminalDrainR24ControlB(t *testing.T) {
	ctx := context.Background()
	txn, fs, _, txnLocalName, _ := newCloneBorrowTxnForR11(t)

	txn.Lock()
	txn.retainDefiniteOrphansLocked([]string{txnLocalName},
		"r11 D1' control B: the failed preparation owns the txn-local share")
	txn.Unlock()
	require.Equal(t, []string{txnLocalName}, txn.pendingObjectCleanupNames(),
		"control B setup: the txn-local share landed on the pending list")

	// The exact shared terminal helper txn.Rollback runs.
	txn.Lock()
	txn.finalizePendingObjectCleanup()
	txn.Unlock()
	require.Equal(t, []string{txnLocalName},
		issue25590QueuedNames(txn.engine),
		"the rollback terminal handed the owned txn-local name to the engine queue")

	require.NoError(t, txn.engine.deleteQueuedUnpublishedPendingOnce(ctx))
	require.Eventually(t, func() bool {
		return !objectExistsInFS(ctx, fs, txnLocalName)
	}, time.Second, 10*time.Millisecond,
		"D1' control B: the terminal drain physically deletes the txn-local share after rollback")
	require.Empty(t, issue25590QueuedNames(txn.engine))
}

// Control D: end-to-end preservation and readability — after the
// failed-preparation retain, the rollback handoff, the queue drain AND
// the clone rollback-scope gc sweep, the committed-borrowed witness
// still exists physically, its bytes read back, and its share binding
// still resolves; the genuinely fresh sibling is gone. The sweep leg
// pins the untouched gcFiles filter beside the new machinery so the
// two owners cannot diverge.
func TestIssue25590CloneBorrowedSurvivesRollbackDrainAndStaysReadableR24ControlD(t *testing.T) {
	ctx := context.Background()
	txn, fs, borrowed, _, freshName := newCloneBorrowTxnForR11(t)
	borrowedName := borrowed.ObjectName().String()

	// The failed-preparation class retains the batch; only the fresh
	// sibling flows (control C's landing half).
	txn.Lock()
	txn.retainDefiniteOrphansLocked([]string{borrowedName, freshName},
		"r11 D1' control D: the failed preparation's retain")
	txn.Unlock()

	// The clone rollback-scope sweep must ALSO leave the borrowed
	// witness alone (the untouched gcFiles filter semantics). The txn
	// writes entry carries the witness's stats, the exact vehicle the
	// sweep collects.
	mp := mpool.MustNewZero()
	bat := cloneObjectStatsBatchForTest(t, mp, borrowed)
	defer func() {
		bat.Clean(mp)
		require.Zero(t, mp.CurrNB())
	}()
	txn.writes = append(txn.writes, Entry{
		typ:      INSERT,
		fileName: "clone-meta",
		bat:      bat,
	})
	require.NoError(t, txn.gcObjsByIdxRange(0, 0, cloneGCTxnRollback))

	// The rollback terminal's handoff moves exactly the fresh sibling.
	txn.Lock()
	require.Equal(t, []string{freshName}, txn.pendingObjectCleanupNames(),
		"control D setup: only the fresh sibling stayed on the transaction's list")
	txn.finalizePendingObjectCleanup()
	txn.Unlock()
	require.Equal(t, []string{freshName}, issue25590QueuedNames(txn.engine),
		"the rollback terminal queued exactly the fresh sibling")
	require.NoError(t, txn.engine.deleteQueuedUnpublishedPendingOnce(ctx))

	// Physical survival and source readability after rollback and
	// drain: the witness exists, reads back its bytes, and the share
	// binding still resolves for this transaction.
	require.True(t, objectExistsInFS(ctx, fs, borrowedName),
		"D1' control D: the committed-borrowed witness survives the failed preparation, rollback, sweep and drain")
	ioVec := fileservice.IOVector{
		FilePath: borrowedName,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len("test data"))}},
	}
	require.NoError(t, fs.Read(ctx, &ioVec))
	require.Equal(t, []byte("test data"), ioVec.Entries[0].Data,
		"D1' control D: the witness's bytes read back — the source table stays readable")
	require.True(t, txn.engine.cloneTxnCache.IsSharedFile(txn.op.Txn().ID, borrowedName),
		"D1' control D: the share binding still resolves the borrow")

	// The fresh sibling is physically gone through the same terminal,
	// so the survival above is not a drain failure.
	require.Eventually(t, func() bool {
		return !objectExistsInFS(ctx, fs, freshName)
	}, time.Second, 10*time.Millisecond,
		"D1' control D: the genuinely fresh name was collected")
	require.Empty(t, issue25590QueuedNames(txn.engine))
}
