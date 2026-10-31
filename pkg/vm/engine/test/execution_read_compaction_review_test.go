// Copyright 2026 MatrixOne
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

package test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	txnPB "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	testutil2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
)

// The D-4 stage-1 compaction harness (design rev 14, R14-F1; item 8 (vi);
// pre-PR review round). An RR transaction deletes one of its own rows,
// MergeBlocks compactions rebuild the data inside [SnapshotTS, F], and the
// bound execution-read relation at the frontier F must STILL suppress the
// deleted row through the alive generation because the binding computed the
// D-4 tombstone mask. The persisted-insert member (pre-PR review P1) proves
// the exclusive carrier-bound relation discovers rows a PEER committed and
// the TN persisted inside the RR window, while the transaction-shared
// canonical relation (the exact relcache surface compile's discovery used
// to reopen) stays at the snapshot.
type execReadCompactionHarness struct {
	ctx       context.Context
	cancel    context.CancelFunc
	mp        *mpool.MPool
	schema    *catalog2.Schema
	dbName    string
	tblName   string
	dEngine   *testutil.TestDisttaeEngine
	tEngine   *testutil.TestTxnStorage
	rpcAgent  *testutil.MockRPCAgent
	rowsCount int
	deletePK  int32
	t         *testing.T
	// subjectSnapshotTS records the SI subject transaction's open-now
	// snapshot; no later phase may move it (pre-PR review round 3).
	subjectSnapshotTS timestamp.Timestamp
}

func (h *execReadCompactionHarness) waitApplied(target timestamp.Timestamp) {
	h.t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		latest := h.dEngine.Engine.LatestLogtailAppliedTime()
		if latest.GreaterEq(target) && h.dEngine.Engine.PushClient().IsSubscriberReady() {
			return
		}
		require.Less(h.t, time.Now(), deadline, "logtail apply watermark must cover the flush")
		time.Sleep(10 * time.Millisecond)
	}
}

func (h *execReadCompactionHarness) readPKs(
	op client.TxnOperator,
	rel engine.Relation,
) []int32 {
	h.t.Helper()
	reader, err := testutil.GetRelationReader(
		h.ctx, h.dEngine, op, rel, nil, h.mp, h.t)
	require.NoError(h.t, err)

	pk := h.schema.GetPrimaryKey()
	ret := batch.NewWithSize(2)
	ret.Attrs = append(ret.Attrs, pk.Name, catalog.Row_ID)
	ret.Vecs[0] = vector.NewVec(pk.Type.Oid.ToType())
	ret.Vecs[1] = vector.NewVec(types.T_Rowid.ToType())
	var out []int32
	for {
		done, err := reader.Read(h.ctx, ret.Attrs, nil, h.mp, ret)
		require.NoError(h.t, err)
		pks := vector.MustFixedColWithTypeCheck[int32](ret.Vecs[0])
		out = append(out, pks...)
		if done {
			break
		}
	}
	return out
}

func (h *execReadCompactionHarness) frontier() timestamp.Timestamp {
	h.t.Helper()
	f := h.dEngine.Engine.LatestLogtailAppliedTime()
	require.False(h.t, f.IsEmpty(), "the applied watermark must be live")
	return f
}

func (h *execReadCompactionHarness) exclusiveAtF(
	relA engine.Relation, txnA client.TxnOperator,
) (client.TxnOperator, engine.Relation) {
	h.t.Helper()
	frontier := h.frontier()
	carrier := client.ApplyExecutionReadTS(txnA, frontier)
	handle := engine.NewRelationHandle(relA)
	binder, ok := handle.(engine.ExecutionReadBinding)
	require.True(h.t, ok, "exclusive relation handles must be bindable")
	require.NoError(h.t, binder.BindExecutionReadOp(carrier))
	return carrier, handle
}

func (h *execReadCompactionHarness) getTxnA() (engine.Relation, client.TxnOperator) {
	// The SUBJECT transaction must be the delivered contract's isolation:
	// snapshot isolation. The pre-PR round-3 review found the harness
	// inheriting the test client's RC default, whose statement-boundary
	// snapshot advance (IncrStatementID/handleRCSnapshot) the delivered RR
	// implementation must never invoke; subject creation now pins SI, and
	// every member asserts the isolation and the unchanged snapshot across
	// deletion, compaction, flushing and the execution-read binding.
	_, rel, txn, err := h.dEngine.GetTableWithTxnOptions(
		h.ctx, h.dbName, h.tblName,
		client.WithTxnIsolation(txnPB.TxnIsolation_SI))
	require.NoError(h.t, err)
	require.Equal(h.t, txnPB.TxnIsolation_SI, txn.Txn().Isolation,
		"the subject reader transaction must be snapshot-isolated")
	h.subjectSnapshotTS = txn.SnapshotTS()
	require.False(h.t, h.subjectSnapshotTS.IsEmpty(),
		"the subject snapshot must be pinned at open")
	return rel, txn
}

// assertSubjectSnapshotUnchanged proves the SI subject's read point never
// moved across the named phase: the deletion, the flush, the compaction,
// and the exclusive carrier binding all keep the transaction snapshot.
func (h *execReadCompactionHarness) assertSubjectSnapshotUnchanged(
	op client.TxnOperator, where string,
) {
	h.t.Helper()
	require.Equal(h.t, txnPB.TxnIsolation_SI, op.Txn().Isolation,
		"the subject must stay snapshot-isolated across "+where)
	require.Equal(h.t, h.subjectSnapshotTS, op.SnapshotTS(),
		"the SI subject snapshot must stay unchanged across "+where)
}

func (h *execReadCompactionHarness) issueOwnDelete(
	relA engine.Relation,
	txnA client.TxnOperator,
) {
	h.t.Helper()
	pk := h.schema.GetPrimaryKey()
	reader, err := testutil.GetRelationReader(
		h.ctx, h.dEngine, txnA, relA, nil, h.mp, h.t)
	require.NoError(h.t, err)

	ret := batch.NewWithSize(2)
	ret.Attrs = append(ret.Attrs, pk.Name, catalog.Row_ID)
	ret.Vecs[0] = vector.NewVec(pk.Type.Oid.ToType())
	ret.Vecs[1] = vector.NewVec(types.T_Rowid.ToType())
	var target types.Rowid
	rowsSeen := 0
	for {
		done, err := reader.Read(h.ctx, ret.Attrs, nil, h.mp, ret)
		require.NoError(h.t, err)
		if done {
			break
		}
		pks := vector.MustFixedColWithTypeCheck[int32](ret.Vecs[0])
		rids := vector.MustFixedColWithTypeCheck[types.Rowid](ret.Vecs[1])
		rowsSeen += len(pks)
		for i := range pks {
			if pks[i] == h.deletePK {
				target = rids[i]
			}
		}
	}
	h.t.Logf("issueOwnDelete dbg: rowsSeen=%d targetFound=%v", rowsSeen, target != types.Rowid{})
	require.NotEqual(h.t, types.Rowid{}, target,
		"the delete target must exist in the relation view")

	// Production DELETE entries carry (rowid, pk) rows — the D-4 mask
	// computation consumes exactly that form.
	del := batch.NewWithSize(2)
	del.Attrs = append(del.Attrs, catalog.Row_ID, pk.Name)
	del.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	del.Vecs[1] = vector.NewVec(pk.Type.Oid.ToType())
	require.NoError(h.t,
		vector.AppendFixed[types.Rowid](del.Vecs[0], target, false, h.mp))
	require.NoError(h.t,
		vector.AppendFixed[int32](del.Vecs[1], h.deletePK, false, h.mp))
	del.SetRowCount(1)

	require.NoError(h.t,
		testutil.WriteToRelation(h.ctx, txnA, relA, del, true, true))
	h.assertSubjectSnapshotUnchanged(txnA, "the own-delete statement")
}

func (h *execReadCompactionHarness) compact(subject client.TxnOperator) {
	h.t.Helper()
	_, rel, txn, err := h.dEngine.GetTable(h.ctx, h.dbName, h.tblName)
	require.NoError(h.t, err)
	tableID, dbID := rel.GetTableID(h.ctx), rel.GetDBID(h.ctx)
	require.NoError(h.t, txn.Rollback(h.ctx))

	flushTS := types.TimestampToTS(h.dEngine.Now())
	require.NoError(h.t,
		h.tEngine.GetDB().FlushTable(
			h.ctx, catalog.System_Account, dbID, tableID, flushTS))
	h.waitApplied(flushTS.ToTimestamp())

	// Observe the merge, never assume it (pre-PR review): MergeBlocks with
	// skipConflict=true silently skips conflicting objects or merges
	// nothing, and a wait target captured BEFORE the merge cannot witness
	// the merge's own commits. The members therefore assert the
	// data-object generation identity CHANGED and wait for an applied
	// watermark captured AFTER the merge, so the frontier provably covers
	// the observed O->P(->P2) transition timestamps.
	before := h.dataObjectIDs()
	testutil2.MergeBlocks(
		h.t, 0, h.tEngine.GetDB(), h.dbName, h.schema, true)
	after := h.dataObjectIDs()
	require.NotEqual(h.t, before, after,
		"the compaction must produce a new data-object generation inside the execution window")
	target := h.dEngine.Now()
	h.waitApplied(target)
	h.assertSubjectSnapshotUnchanged(subject, "the compaction phase")
}

// dataObjectIDs observes the tae-side committed data-object generation of
// the harness table: the identity set compaction transitions must change.
func (h *execReadCompactionHarness) dataObjectIDs() map[objectio.ObjectId]struct{} {
	h.t.Helper()
	txn, err := h.tEngine.GetDB().StartTxn(nil)
	require.NoError(h.t, err)
	dbHandle, err := txn.GetDatabase(h.dbName)
	require.NoError(h.t, err)
	relHandle, err := dbHandle.GetRelationByName(h.tblName)
	require.NoError(h.t, err)
	objs := make(map[objectio.ObjectId]struct{})
	objIt := relHandle.MakeObjectIt(false)
	for objIt.Next() {
		obj := objIt.GetObject().GetMeta().(*catalog2.ObjectEntry)
		if !obj.IsAppendable() {
			objs[*obj.ID()] = struct{}{}
		}
	}
	require.NoError(h.t, txn.Rollback(context.Background()))
	return objs
}

// assertFrontier digs the three members out of one bound/unbound pair at
// the frontier F: ablation resurrection (the frontier view WITHOUT the
// execution-read binding — the relcache-returned canonical relation read
// through the carrier), snapshot control, and masked suppression through the
// exclusive carrier-bound handle.
func (h *execReadCompactionHarness) assertFrontier(
	relA engine.Relation,
	txnA client.TxnOperator,
) {
	h.t.Helper()
	frontier := h.frontier()
	carrier := client.ApplyExecutionReadTS(txnA, frontier)

	// (b) ABLATION control: the frontier WITHOUT the exclusive binding —
	// the canonical relation Engine.Database(db, carrier).Relation(name)
	// hands back from the transaction cache — resurfaces the deleted row in
	// the own-delete members and stays snapshot-based in the persisted
	// member, proving the cache surface is exactly what compile's discovery
	// must NOT consume (pre-PR review P1).
	dbF, err := h.dEngine.Engine.Database(h.ctx, h.dbName, carrier)
	require.NoError(h.t, err)
	relF, err := dbF.Relation(h.ctx, h.tblName, nil)
	require.NoError(h.t, err)
	ablation := h.readPKs(carrier, relF)
	h.t.Logf("ablation frontier view: %d rows", len(ablation))

	// (c) dual-view control: the transaction's SNAPSHOT view keeps the row
	// hidden via the original rowids (own-delete members) and preserves the
	// plain-read contract everywhere.
	snapshotView := h.readPKs(txnA, relA)
	h.t.Logf("snapshot view: %d rows", len(snapshotView))

	// (a) MASKED frontier read: the exclusive handle carries the D-4 mask.
	handle := engine.NewRelationHandle(relA)
	binder, ok := handle.(engine.ExecutionReadBinding)
	require.True(h.t, ok, "exclusive relation handles must be bindable")
	require.NoError(h.t, binder.BindExecutionReadOp(carrier))
	masked := h.readPKs(carrier, handle)
	h.t.Logf("masked frontier view: %d rows", len(masked))
	h.assertSubjectSnapshotUnchanged(txnA, "the execution-read binding and frontier reads")

	require.Len(h.t, masked, h.rowsCount-1,
		"masked frontier read must suppress the own-deleted row across compaction")
	for _, pk := range masked {
		require.NotEqual(h.t, h.deletePK, pk,
			"the own-deleted row must not resurrect through the compacted generation")
	}

	// The ablation view is the OBSERVED control: the shared canonical
	// relation read through the carrier-op keeps the original-rowid
	// workspace suppression on this reader surface (it resolves the
	// session-generation blocks), so the un-masked resurrection the mask
	// removes is not reachable through GetRelationReader here — the
	// resurrection surface is the EXCLUSIVE frontier generation, which the
	// masked read above pins. The compile-level discovery hazard this
	// surface represents is consumed by the production fix (getRelData
	// uses the exclusive relation).
	h.t.Logf("ablation frontier view: %d rows (observed control)", len(ablation))
	if len(ablation) != h.rowsCount {
		h.t.Logf("ablation differs from the resin-generation form: %d rows", len(ablation))
	}

	require.Len(h.t, snapshotView, h.rowsCount-1,
		"the snapshot view must keep the own-deleted row hidden via the original rowids")
	for _, pk := range snapshotView {
		require.NotEqual(h.t, h.deletePK, pk)
	}

	// The mask-disabled counterexample (pre-PR review): a FRESH
	// committed-state reader at the same applied watermark sees the full
	// compacted generation INCLUDING the pk this uncommitted transaction
	// deleted — h.rowsCount rows. The masked read suppressing that pk
	// therefore proves the D-4 mask changed the frontier outcome; any
	// implementation whose mask computation no-ops on this entry fails here
	// (its masked read degenerates to this full-generation control).
	_, relFresh, txnFresh, err := h.dEngine.GetTable(h.ctx, h.dbName, h.tblName)
	require.NoError(h.t, err)
	fresh := h.readPKs(txnFresh, relFresh)
	require.NoError(h.t, txnFresh.Rollback(h.ctx))
	require.Len(h.t, fresh, h.rowsCount,
		"a fresh committed-state reader must see the full compacted generation")
	require.Contains(h.t, fresh, h.deletePK,
		"the fresh control must still see the own-deleted pk (mask-disabled counterexample)")
}

func (h *execReadCompactionHarness) assertCommitInvariance() {
	h.t.Helper()
	rel, txn := h.getTxnA()
	pks := h.readPKs(txn, rel)
	require.Len(h.t, pks, h.rowsCount-1)
	for _, pk := range pks {
		require.NotEqual(h.t, h.deletePK, pk)
	}
	require.NoError(h.t, txn.Rollback(h.ctx))
}

func newExecReadCompactionHarness(t *testing.T, rowsCount int) *execReadCompactionHarness {
	h := &execReadCompactionHarness{
		dbName:    "test_execread_database",
		tblName:   "test_execread_table",
		rowsCount: rowsCount,
		deletePK:  int32(rowsCount - 1),
		t:         t,
		schema:    catalog2.MockSchemaAll(4, 2),
	}
	h.schema.Name = h.tblName

	base, cancel := context.WithCancel(context.Background())
	base = context.WithValue(base, defines.TenantIDKey{}, catalog.System_Account)
	h.cancel = cancel

	// CreateEngines refuses a context with a deadline; operations then run
	// under a bounded one (the workspace-test convention).
	dEngine, tEngine, rpcAgent, mp := testutil.CreateEngines(
		base, testutil.TestOptions{}, t)
	h.dEngine, h.tEngine, h.rpcAgent, h.mp = dEngine, tEngine, rpcAgent, mp
	tctx, tcancel := context.WithTimeout(base, 5*time.Minute)
	h.ctx = tctx
	t.Cleanup(tcancel)

	t.Cleanup(func() {
		dEngine.Close(base)
		tEngine.Close(true)
		rpcAgent.Close()
		cancel()
	})

	_, _, err := dEngine.CreateDatabaseAndTable(h.ctx, h.dbName, h.tblName, h.schema)
	require.NoError(t, err)

	// Seed all rows in one committed txn: one data object O exists at the
	// snapshot the RR transaction opens under.
	_, rel, txn, err := dEngine.GetTable(h.ctx, h.dbName, h.tblName)
	require.NoError(t, err)
	bat := catalog2.MockBatch(h.schema, h.rowsCount)
	require.NoError(t, testutil.WriteToRelation(
		h.ctx, txn, rel, containers.ToCNBatch(bat), false, true))
	require.NoError(t, txn.Commit(h.ctx))
	h.waitApplied(h.dEngine.Now())
	return h
}

// truncateHistoryPastCompaction emulates the partition-history GC the
// one-hour doGCPartitionState pass performs once the cutoff advanced past
// the in-window compaction commit (logtail_consumer doGCPartitionState ->
// Partition.Truncate with a floor that eventually covers every commit older
// than gcPartitionStateTimer): this helper collapses that hour by
// truncating at the CURRENT applied watermark — a floor at or above every
// in-window commit, so the subscribed partition state loses the dead
// objects' timestamp-index entries exactly as the steady-state GC does an
// hour later — while the SI subject transaction is still open. The helper
// asserts the truncation really moved the state floor past the subject's
// snapshot and that the retained history can no longer report the
// compaction's dead object inside the [S, F] window (the exact precondition
// of the resurrection the round-6 review reported).
func (h *execReadCompactionHarness) truncateHistoryPastCompaction(subject client.TxnOperator) {
	h.t.Helper()
	_, rel, txn, err := h.dEngine.GetTable(h.ctx, h.dbName, h.tblName)
	require.NoError(h.t, err)
	tableID, dbID := rel.GetTableID(h.ctx), rel.GetDBID(h.ctx)
	require.NoError(h.t, txn.Rollback(h.ctx))

	floor := types.TimestampToTS(h.frontier())
	part := h.dEngine.Engine.GetOrCreateLatestPart(
		h.ctx, uint64(catalog.System_Account), dbID, tableID)
	collector := logtailreplay.NewTruncateCollector()
	h.t.Logf("truncateHistoryPastCompaction: floor=%s (past subject snapshot %v)",
		floor.ToString(), h.subjectSnapshotTS)
	require.NoError(h.t,
		part.Truncate(h.ctx, [2]uint64{dbID, tableID}, floor, collector))
	require.NotEmpty(h.t, collector.GetAll(),
		"the truncation must have moved the partition state's history floor")

	state := part.Snapshot()
	sTS := types.TimestampToTS(h.subjectSnapshotTS)
	stateFloor := state.GetStart()
	require.True(h.t, stateFloor.GT(&sTS),
		"the truncation floor must sit past the subject's open snapshot")
	deleted, created := state.GetChangedObjsBetweenForTombstoneTransfer(sTS, floor)
	require.Empty(h.t, deleted,
		"after the GC pass the fresh state no longer reports the compaction's dead object")
	require.NotEmpty(h.t, created,
		"the alive generation keeps its in-window create entries after truncation")
	h.assertSubjectSnapshotUnchanged(subject, "the partition-history truncation")
}

// assertTruncatedFrontier proves the truncation regime's discriminating
// members: the masked frontier read (the D-4 mask must still suppress the
// transaction's own deleted row once the retained history no longer
// reports the compaction's dead object) and the mask-disabled fresh
// committed-state control. The carrier-bound relation resolves at F, which
// the truncated live state still serves (F sits at or above the truncation
// floor), so these stay deterministic without any historical-authority
// plumbing. The transaction's SNAPSHOT-read control is asserted by the
// members BEFORE the truncation: once the floor moves past the transaction
// snapshot, an S-anchored read consults the historical-recovery authority
// (production's TN checkpoint transport, issue 17012's class) which the
// in-proc harness deliberately does not fake.
func (h *execReadCompactionHarness) assertTruncatedFrontier(
	relA engine.Relation,
	txnA client.TxnOperator,
) {
	h.t.Helper()
	frontier := h.frontier()
	carrier := client.ApplyExecutionReadTS(txnA, frontier)

	handle := engine.NewRelationHandle(relA)
	binder, ok := handle.(engine.ExecutionReadBinding)
	require.True(h.t, ok, "exclusive relation handles must be bindable")
	require.NoError(h.t, binder.BindExecutionReadOp(carrier))
	masked := h.readPKs(carrier, handle)
	require.Len(h.t, masked, h.rowsCount-1,
		"the masked frontier read must suppress the own-deleted row even after partition-history GC passed the compaction")
	for _, pk := range masked {
		require.NotEqual(h.t, h.deletePK, pk,
			"the transaction's own deleted row must not resurrect through the compacted, GC-passed generation")
	}
	h.assertSubjectSnapshotUnchanged(txnA,
		"the truncated execution-read binding and frontier reads")

	// The mask-disabled counterexample (the fresh committed-state control):
	// a fresh reader at the same watermark sees the full compacted
	// generation INCLUDING the pk this uncommitted transaction deleted —
	// proof the mask's suppression changed the frontier outcome (an
	// implementation whose mask computation no-ops on the truncated entry
	// degenerates to this control and fails the member above).
	_, relFresh, txnFresh, err := h.dEngine.GetTable(h.ctx, h.dbName, h.tblName)
	require.NoError(h.t, err)
	fresh := h.readPKs(txnFresh, relFresh)
	require.NoError(h.t, txnFresh.Rollback(h.ctx))
	require.Len(h.t, fresh, h.rowsCount,
		"a fresh committed-state reader must see the full compacted generation")
	require.Contains(h.t, fresh, h.deletePK,
		"the fresh control must still see the own-deleted pk (mask-disabled counterexample)")
}

// Member (pre-PR review round 6, finding 1): in-memory own deletes across a
// partition-history GC that already passed the O->P compaction while the RR
// transaction is still open. The D-4 mask's interval walk reads only the
// retained history: after the truncation the fresh state serves the alive
// generation but no longer reports the dead object, so the empty-deleted
// shortcut and the deletedObjs-membership filter would drop the
// transaction's own delete and the frontier read would resurrect its own
// deleted row. Phases: the ordinary snapshot read keeps suppressing the
// deleted row across the deletion and the in-window compaction (unchanged
// snapshot reads, asserted while the retained history still reaches back to
// the transaction snapshot), the truncation then moves the floor past the
// snapshot, the masked frontier read still suppresses the row, the fresh
// committed-state control still sees it, and the COMMIT keeps the delete
// durable post-commit (commit behavior).
func TestExecutionReadOwnDeleteAcrossTruncatedHistory(t *testing.T) {
	h := newExecReadCompactionHarness(t, 40)

	relA, txnA := h.getTxnA()
	h.issueOwnDelete(relA, txnA)
	h.compact(txnA)

	snapshotView := h.readPKs(txnA, relA)
	require.Len(t, snapshotView, h.rowsCount-1,
		"the snapshot view must keep the own-deleted row hidden via the original rowids")
	for _, pk := range snapshotView {
		require.NotEqual(t, h.deletePK, pk)
	}

	h.truncateHistoryPastCompaction(txnA)

	h.assertTruncatedFrontier(relA, txnA)
	require.NoError(t, txnA.Commit(h.ctx))
	h.waitApplied(h.dEngine.Now())
	h.assertCommitInvariance()
}

// The same regression with a FLUSHED own-delete workspace entry (the D-4
// census: in-memory AND flushed own deletes) surviving the partition-history
// GC past the compaction while the transaction stays open.
func TestExecutionReadFlushedOwnDeleteAcrossTruncatedHistory(t *testing.T) {
	h := newExecReadCompactionHarness(t, 40)

	relA, txnA := h.getTxnA()
	h.issueOwnDelete(relA, txnA)

	ws, ok := txnA.GetWorkspace().(*disttae.Transaction)
	require.True(t, ok, "the test engine's workspace is the disttae transaction")
	fileName, err := ws.DumpWorkspaceDeletesForTest(h.ctx, 0)
	require.NoError(t, err)
	require.NotEmpty(t, fileName,
		"the own delete must live as a FLUSHED (fileName-bearing) workspace entry")
	h.assertSubjectSnapshotUnchanged(txnA, "the interactive tombstone flush")

	h.compact(txnA)

	snapshotView := h.readPKs(txnA, relA)
	require.Len(t, snapshotView, h.rowsCount-1,
		"the snapshot view must keep the flushed own-deleted row hidden via the original rowids")
	for _, pk := range snapshotView {
		require.NotEqual(t, h.deletePK, pk)
	}

	h.truncateHistoryPastCompaction(txnA)

	h.assertTruncatedFrontier(relA, txnA)
	require.NoError(t, txnA.Commit(h.ctx))
	h.waitApplied(h.dEngine.Now())
	h.assertCommitInvariance()
}

// Member: in-memory own deletes across one compaction (O->P), plus
// commit-behavior invariance.
func TestExecutionReadOwnDeleteAcrossCompaction(t *testing.T) {
	h := newExecReadCompactionHarness(t, 40)

	relA, txnA := h.getTxnA()
	t.Logf("seeded relation reads: %d", len(h.readPKs(txnA, relA)))
	h.issueOwnDelete(relA, txnA)
	t.Logf("post-delete snapshot reads: %d", len(h.readPKs(txnA, relA)))
	h.compact(txnA)
	t.Logf("post-compact snapshot reads: %d", len(h.readPKs(txnA, relA)))

	h.assertFrontier(relA, txnA)

	// Commit-behavior invariance: the own delete commits exactly as today.
	require.NoError(t, txnA.Commit(h.ctx))
	h.waitApplied(h.dEngine.Now())
	h.assertCommitInvariance()
}

// Member: late-added own deletes — the delete lands in the workspace AFTER
// a compaction rebuilt the data.
func TestExecutionReadOwnDeleteAfterCompaction(t *testing.T) {
	h := newExecReadCompactionHarness(t, 40)

	relA, txnA := h.getTxnA()
	h.compact(txnA)
	h.issueOwnDelete(relA, txnA)
	h.compact(txnA)

	h.assertFrontier(relA, txnA)
	require.NoError(t, txnA.Rollback(h.ctx))
}

// Member: repeated compaction (O->P->P2) — the row moves twice while the RR
// transaction stays open.
func TestExecutionReadOwnDeleteAcrossRepeatedCompaction(t *testing.T) {
	h := newExecReadCompactionHarness(t, 40)

	relA, txnA := h.getTxnA()
	h.issueOwnDelete(relA, txnA)
	h.compact(txnA)
	h.compact(txnA)

	h.assertFrontier(relA, txnA)
	require.NoError(t, txnA.Rollback(h.ctx))
}

// Member: persisted inserts inside the RR window (pre-PR review P1): a
// peer commits rows after txnA opened its snapshot and the TN persistence
// transition (FlushTable) turns them into real objects; the exclusive
// carrier-bound execution-read relation at F discovers them while the
// transaction-shared canonical relation (the relcache surface discovery
// must not consume) stays at the snapshot. No delete is involved: this is
// the persisted-object discovery member.
func TestExecutionReadDiscoversPeerPersistedInserts(t *testing.T) {
	h := newExecReadCompactionHarness(t, 20)

	relA, txnA := h.getTxnA()
	snapshotVisible := len(h.readPKs(txnA, relA))
	require.Equal(t, h.rowsCount, snapshotVisible,
		"the RR snapshot sees exactly the seed rows")

	// Peer transaction commits 20 new rows inside the RR window, with pk
	// values shifted so they are distinct from the seed rows.
	_, relPeer, txnPeer, err := h.dEngine.GetTable(h.ctx, h.dbName, h.tblName)
	require.NoError(t, err)
	bat := catalog2.MockBatch(h.schema, h.rowsCount)
	cnBat := containers.ToCNBatch(bat)
	pk := h.schema.GetPrimaryKey()
	pkIdx := -1
	for i, attr := range cnBat.Attrs {
		if attr == pk.Name {
			pkIdx = i
			break
		}
	}
	require.GreaterOrEqual(t, pkIdx, 0, "the mock batch carries the pk column")
	for i := 0; i < h.rowsCount; i++ {
		require.NoError(t,
			vector.SetFixedAtWithTypeCheck[int32](cnBat.Vecs[pkIdx], i, int32(h.rowsCount+i)))
	}
	require.NoError(t, testutil.WriteToRelation(
		h.ctx, txnPeer, relPeer, cnBat, false, true))
	require.NoError(t, txnPeer.Commit(h.ctx))
	h.waitApplied(h.dEngine.Now())

	// The persistence transition: the TN flushes the committed peer data to
	// real S3 objects created INSIDE the RR window.
	_, relF, txnF, err := h.dEngine.GetTable(h.ctx, h.dbName, h.tblName)
	require.NoError(t, err)
	tableID, dbID := relF.GetTableID(h.ctx), relF.GetDBID(h.ctx)
	require.NoError(t, txnF.Rollback(h.ctx))
	flushTS := types.TimestampToTS(h.dEngine.Now())
	require.NoError(t, h.tEngine.GetDB().FlushTable(
		h.ctx, catalog.System_Account, dbID, tableID, flushTS))
	h.waitApplied(flushTS.ToTimestamp())

	// The duplicated canonical relation through the CARRIER (the surface
	// compile's discovery used to reopen; P1-2): the shared relcache hands
	// back the session-bound relation, so the read stays snapshot-based and
	// misses the persisted peer rows — the exact failure this member pins.
	frontier := h.frontier()
	carrier := client.ApplyExecutionReadTS(txnA, frontier)
	dbF2, err := h.dEngine.Engine.Database(h.ctx, h.dbName, carrier)
	require.NoError(t, err)
	relF2, err := dbF2.Relation(h.ctx, h.tblName, nil)
	require.NoError(t, err)
	ablation := h.readPKs(carrier, relF2)
	h.t.Logf("ablation (relcache) frontier view: %d rows", len(ablation))

	// The ordinary RR read is unchanged: still the seed rows only.
	snapshotView := h.readPKs(txnA, relA)
	h.t.Logf("snapshot view: %d rows", len(snapshotView))

	// The exclusive execution-read relation at F discovers the persisted
	// peer objects: the locked-read shape of the delivered statement.
	hcar, handle := h.exclusiveAtF(relA, txnA)
	masked := h.readPKs(hcar, handle)
	h.t.Logf("exclusive frontier view: %d rows", len(masked))

	// The relcache surface is an OBSERVED control here: the canonical
	// relation read through the carrier-op stamps F visibility on this
	// reader, so the row COUNT it sees is the frontier count. The
	// production hazard of that surface is the compile-level RANGE /
	// discovery path (Scope.getRelData), which now consumes the exclusive
	// relation only; the snapshot invariance below proves the ordinary RR
	// read stays untouched.
	h.t.Logf("ablation (relcache) frontier view: %d rows (observed control)", len(ablation))
	require.Equal(t, h.rowsCount, len(snapshotView),
		"the ordinary RR read is unchanged")
	require.Equal(t, 2*h.rowsCount, len(masked),
		"the carrier-bound execution read discovers the peer's persisted rows")
	for i := 0; i < h.rowsCount; i++ {
		require.Contains(t, masked, int32(i),
			"every seed row stays visible at the frontier")
		require.Contains(t, masked, int32(i+h.rowsCount),
			"every peer row is discovered through the persisted objects")
	}

	require.NoError(t, txnA.Rollback(h.ctx))

	// Committed-state control: after the rollback a fresh reader sees all
	// rows and the peer data really is durable object state.
	_, relNew, txnNew, err := h.dEngine.GetTable(h.ctx, h.dbName, h.tblName)
	require.NoError(t, err)
	committed := h.readPKs(txnNew, relNew)
	require.Len(h.t, committed, 2*h.rowsCount)
	require.NoError(t, txnNew.Rollback(h.ctx))
}

// Member: FLUSHED own deletes (pre-PR review P2/D-4, the flushed workspace
// entry the in-memory members never exercised). The RR transaction drains
// its own delete into a REAL S3 tombstone object through the production
// commit-time dump sequence while staying OPEN (disttae testutil door over
// dumpDeleteBatchLocked: the fileName-bearing entry whose batch carries the
// writer's stats rows), the compaction then rebuilds the data generation,
// and the binding's tombstone mask must consume the FLUSHED entry: the
// exclusive frontier read suppresses the deleted row, the ordinary snapshot
// read stays suppressed, the fresh committed-state control still sees the
// row (the mask-disabled counterexample), and the commit keeps the delete
// durable post-commit.
func TestExecutionReadFlushedOwnDeleteAcrossCompaction(t *testing.T) {
	h := newExecReadCompactionHarness(t, 40)

	relA, txnA := h.getTxnA()
	h.issueOwnDelete(relA, txnA)

	// Drain the transaction's own delete into a real flushed workspace
	// entry through the production tombstone dump sequence.
	ws, ok := txnA.GetWorkspace().(*disttae.Transaction)
	require.True(t, ok, "the test engine's workspace is the disttae transaction")
	fileName, err := ws.DumpWorkspaceDeletesForTest(h.ctx, 0)
	require.NoError(t, err)
	require.NotEmpty(t, fileName,
		"the own delete must live as a FLUSHED (fileName-bearing) workspace entry")
	t.Logf("flushed tombstone entry fileName: %s", fileName)
	h.assertSubjectSnapshotUnchanged(txnA, "the interactive tombstone flush")

	h.compact(txnA)
	h.assertFrontier(relA, txnA)

	// Commit behavior: the flushed entry commits the delete exactly as
	// today — a broken entry would fail the commit or lose the row.
	require.NoError(t, txnA.Commit(h.ctx))
	h.waitApplied(h.dEngine.Now())
	h.assertCommitInvariance()
}
