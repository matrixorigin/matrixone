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

// Object-route carry-journal integration batteries for the r42-approved
// D142/D143/D145/D147 contracts. A persisted tombstone's object-route
// transfer round journals its re-key (effective-only, budget-admitted)
// BEFORE the fallible sinker write, and the new test-only fault point
// fj/cn/transfer_staged_write_fails deterministically produces the
// design's "carry committed to memory and journal, durable write failed"
// shape:
//
//   D142: on the windowed boundary route the rollback unwinds the
//   journal's exact inverse, so every map and the staged link cells read
//   the original identity again while the tail cut discards any
//   published tail; the retried boundary carries onto the fresh mint and
//   the adoption settles retirement and journal together (D145), leaving
//   the settled prefix in txn.retiredTransfers for the close drain
//   (D147).
//   D143: the AdvanceSnapshot route (maintWindow=nil) journals — and
//   unwinds — identically: an earlier table's completed round
//   (publication plus retirement) followed by a later table's post-carry
//   write failure resurrects only the ACTIVE records; fresh same-seq
//   work unwinds while settled adoptees stay put (D145).
//
// The fixtures extend the D139 boundary harness
// (TestPkMoveDumpRollbackPreservesCompletedTransferRekeys) with a real
// persisted tombstone object so the object transfer route runs for real.

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// armRCBoundaryFaultFreq arms one RC-boundary fault point with an explicit
// frequency (fault's start:end:skip:prob form — "2:2::" fires the second
// trigger only) and returns its disarm; the disarm is also cleanup-
// scheduled so a failed assertion cannot leak an armed global fault.
func armRCBoundaryFaultFreq(t *testing.T, key, freq string) (disarm func()) {
	t.Helper()
	fault.Enable()
	require.NoError(t, fault.AddFaultPoint(
		context.Background(), key, freq, "echo", 0, "", false))
	disarmed := false
	disarm = func() {
		if disarmed {
			return
		}
		disarmed = true
		_, _ = fault.RemoveFaultPoint(context.Background(), key)
		fault.Disable()
	}
	t.Cleanup(func() { disarm() })
	return disarm
}

// objectRouteTable holds one user table's seeded generations:
// old-generation identity O (metadata-only, deleted at a generation
// boundary), the real replacement data object created at that boundary
// carrying one row of the table's pk, the REAL persisted tombstone
// object naming O, and the tombstone's expected post-transfer identity
// N in the replacement generation.
type objectRouteTable struct {
	tbl        *txnTable
	tableID    uint64
	tableName  string
	pk         int64
	tombRid    types.Rowid
	newTombRid types.Rowid
	tombStats  objectio.ObjectStats
	newStats   []objectio.ObjectStats
}

// writePersistedTombstone writes one REAL one-row tombstone object
// (rowid O, pk) and returns its stats.
func writePersistedTombstone(
	t *testing.T, proc *process.Process, rid types.Rowid, pk int64,
) objectio.ObjectStats {
	t.Helper()
	mp := proc.Mp()
	sharedFS, err := colexec.GetSharedFSFromProc(proc)
	require.NoError(t, err)
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], rid, false, mp))
	bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[1], pk, false, mp))
	bat.SetRowCount(1)
	w := colexec.NewCNS3TombstoneWriterForService(
		proc.GetService(), mp, sharedFS, types.T_int64.ToType(), -1)
	require.NoError(t, w.Write(context.Background(), bat))
	stats, wErr := w.Sync(context.Background())
	require.NoError(t, wErr)
	w.Close()
	bat.Clean(mp)
	require.Len(t, stats, 1)
	return stats[0]
}

// seedObjectRouteTable plants one user table's generations and persisted
// tombstone: the old data generation is metadata-only (nothing ever
// reads it back, exactly as in the D139 fixture) while the replacement
// generation and the tombstone object are real files. snapGen/gen mark
// the old generation's create/delete timestamps (created at snapGen,
// deleted at gen, with the replacement created at gen).
func seedObjectRouteTable(
	t *testing.T, proc *process.Process, fx *r28Fixture, txn *Transaction,
	tableID uint64, tableName string, pk int64, snapGen int64, gen int64,
) *objectRouteTable {
	t.Helper()
	ctx := context.Background()

	oldID := types.NewObjectid()
	oldStats := objectio.NewObjectStatsWithObjectID(&oldID, false, true, false)
	require.NoError(t, objectio.SetObjectStatsSize(oldStats, 1024))
	require.NoError(t, objectio.SetObjectStatsRowCnt(oldStats, 1))
	require.NoError(t, objectio.SetObjectStatsBlkCnt(oldStats, 1))

	var tbl *txnTable
	if tableID == 42 {
		tbl = txn.tableOps.existAndActive(genTableKey(1, "tbl", 7, "db"))
	} else {
		db := txn.databaseOps.existAndActive(genDatabaseKey(1, "db"))
		require.NotNil(t, db)
		txn.tableOps.addCreateTable(
			genTableKey(1, tableName, 7, "db"),
			0,
			&txnTable{
				accountId: 1,
				tableId:   tableID,
				tableName: tableName,
				db:        db,
				tableDef: &pbplan.TableDef{
					DbName: "db",
					Name:   tableName,
					Cols:   fx.cols,
					Pkey:   &pbplan.PrimaryKeyDef{PkeyColName: "pk"},
				},
			},
		)
		tbl = txn.tableOps.existAndActive(genTableKey(1, tableName, 7, "db"))
		tbl.proc.Store(proc)
		tbl.ensureSeqnumsAndTypesExpectRowid()
	}
	require.NotNil(t, tbl)
	if tbl.extraInfo == nil {
		// transferTombstoneObjects passes tbl.extraInfo.AutoIncrEpoch to
		// writeFileLockedWithAutoIncrEpoch when it publishes the
		// replacement; the seeded fixture tables carry no schema extra.
		tbl.extraInfo = &api.SchemaExtra{}
	}

	newStats := fx.writeObj(t, tbl, []int64{pk})
	require.Len(t, newStats, 1)

	part := txn.engine.GetOrCreateLatestPart(ctx, 1, 7, tableID)
	mut, done := part.MutateState()
	require.NoError(t, mut.HandleObjectEntry(ctx, nil, objectio.ObjectEntry{
		ObjectStats: *oldStats,
		CreateTime:  types.BuildTS(snapGen, 0),
		DeleteTime:  types.BuildTS(gen, 0),
	}, false))
	require.NoError(t, mut.HandleObjectEntry(ctx, nil, objectio.ObjectEntry{
		ObjectStats: newStats[0],
		CreateTime:  types.BuildTS(gen, 0),
	}, false))
	done()

	oldBlk := objectio.NewBlockidWithObjectID(&oldID, 0)
	tombRid := types.NewRowid(&oldBlk, 0)
	newBlk := objectio.NewBlockidWithObjectID(newStats[0].ObjectName().ObjectId(), 0)
	newTombRid := types.NewRowid(&newBlk, 0)

	tombStats := writePersistedTombstone(t, proc, tombRid, pk)
	return &objectRouteTable{
		tbl:        tbl,
		tableID:    tableID,
		tableName:  tableName,
		pk:         pk,
		tombRid:    tombRid,
		newTombRid: newTombRid,
		tombStats:  tombStats,
		newStats:   newStats,
	}
}

// fileDeleteEntryFor stages one file-backed DELETE entry carrying the
// persisted tombstone's stats and stashes the stats for read visibility,
// exactly the shape a flushed tombstone takes in production.
func fileDeleteEntryFor(
	t *testing.T, proc *process.Process, txn *Transaction,
	tableID uint64, tableName string, stats objectio.ObjectStats,
) Entry {
	t.Helper()
	mp := proc.Mp()
	bat := colexec.AllocCNS3ResultBat(true)
	require.NoError(t, vector.AppendBytes(
		bat.Vecs[0], stats.Marshal(), false, mp))
	bat.SetRowCount(bat.Vecs[0].Length())
	txn.StashFlushedTombstones(stats)
	return Entry{
		typ:                DELETE,
		accountId:          1,
		databaseId:         7,
		tableId:            tableID,
		databaseName:       "db",
		tableName:          tableName,
		fileName:           stats.ObjectName().String(),
		bat:                bat,
		pkCheckPos:         1,
		pkCheckReady:       true,
		autoIncrEpochKnown: true,
	}
}

// stagedLinkFor stages one link row whose old_row_id names the table's
// old tombstone identity; seed keeps every link row's Row_ID distinct so
// a same-table merge fold at a boundary keeps every row.
func stagedLinkFor(
	t *testing.T, proc *process.Process, tableID uint64,
	tombRid types.Rowid, seed int64,
) Entry {
	t.Helper()
	e := pkMovePruneLinkEntry(t, proc.Mp(), 1,
		[]uint64{tableID},
		[]*types.Rowid{&tombRid},
		[]*types.Rowid{nil},
		[]uint64{700})
	vector.MustFixedColNoTypeCheck[types.Rowid](e.bat.Vecs[0])[0] =
		types.BuildTestRowid(9, seed)
	return e
}

// linkCellsForTable returns every staged link row's old_row_id cell for
// one user table, order-agnostic against merge-fold and adoption layout
// moves (the boundaries fold same-table INSERT batches and compact husk
// slots). Each returned slice is a copy.
func linkCellsForTable(t *testing.T, txn *Transaction, tableID uint64) [][]byte {
	t.Helper()
	var cells [][]byte
	for i := range txn.writes {
		we := &txn.writes[i]
		if !isPkMoveLinkTableInsert(we.databaseName, we.tableName) ||
			we.typ != INSERT || we.bat == nil || we.bat.RowCount() == 0 {
			continue
		}
		tblIdx, oldRowIdx := -1, -1
		for j, attr := range we.bat.Attrs {
			switch attr {
			case catalog.DataBranchUpdateLinkColTableID:
				tblIdx = j
			case catalog.DataBranchUpdateLinkColOldRow:
				oldRowIdx = j
			}
		}
		require.NotEqual(t, -1, tblIdx)
		require.NotEqual(t, -1, oldRowIdx)
		tids := vector.MustFixedColNoTypeCheck[uint64](we.bat.Vecs[tblIdx])
		for r := 0; r < we.bat.RowCount() && r < len(tids); r++ {
			if tids[r] != tableID {
				continue
			}
			cells = append(cells, append([]byte(nil),
				we.bat.Vecs[oldRowIdx].GetBytesAt(r)...))
		}
	}
	return cells
}

// resurrectedDeleteEntryStats reports whether some live file-backed
// DELETE entry carries the named object stats — the shape the statement
// rollback's resurrection re-appends for the ACTIVE retirement record.
func resurrectedDeleteEntryStats(txn *Transaction, name string) bool {
	for i := range txn.writes {
		e := &txn.writes[i]
		if e.typ != DELETE || e.bat == nil || e.fileName == "" {
			continue
		}
		for j := 0; j < e.bat.Vecs[0].Length(); j++ {
			ss := objectio.ObjectStats(e.bat.Vecs[0].GetBytesAt(j))
			if ss.ObjectName().String() == name {
				return true
			}
		}
	}
	return false
}

// TestPkMoveObjectRoutePostCarryWriteFailureRoundTrip pins D142 on the
// windowed boundary route: a persisted tombstone's object transfer round
// journals its provenance re-key, then the injected sinker-write failure
// aborts the boundary; the statement rollback's journal walker restores
// the pre-boundary identities EXACTLY (a windowed carry composes with
// the boundary dump's own journal entry in strict reverse order), and
// the retried boundary carries, publishes, retires, and the adoption
// settles both ledgers (D145) — after which the close drain consumes the
// settled prefix (D147).
func TestPkMoveObjectRoutePostCarryWriteFailureRoundTrip(t *testing.T) {
	proc, _, base := newR29SharedProc(t)
	fx := newR28Fixture(t, proc)
	ctx := context.Background()

	txn, _ := fx.newTxnTbl(t)
	txn.cn_flushed_s3_tombstone_object_stats_list = new(sync.Map)
	snap := timestamp.Timestamp{PhysicalTime: 100}
	swapRCOperatorWithSnapshotSourceForTest(t, txn,
		func() timestamp.Timestamp { return snap })
	txn.cnObjsSummary = make(map[types.Objectid]Summary)
	txn.deletedBlocks = &deletedBlocks{offsets: map[types.Blockid][]int64{}}
	txn.tableOps.addCreatedInTxn(42, 0)
	txn.engine.partitions = make(map[[2]uint64]*logtailreplay.Partition)

	rt := seedObjectRouteTable(t, proc, fx, txn, 42, "tbl", 7, 90, 150)

	raw := Entry{
		typ:          INSERT,
		accountId:    1,
		databaseId:   7,
		tableId:      42,
		databaseName: "db",
		tableName:    "tbl",
		bat:          newInsertBatchWithRowIDForTest(t, proc, []int64{7}),
		pkCheckPos:   1,
	}
	wsRid := vector.MustFixedColNoTypeCheck[types.Rowid](raw.bat.Vecs[0])[0]
	fileDel := fileDeleteEntryFor(t, proc, txn, 42, "tbl", rt.tombStats)
	link := stagedLinkFor(t, proc, 42, rt.tombRid, 1)

	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"the empty pre-SQL1 boundary must succeed")
	txn.appendWorkspaceEntryLocked(raw)
	txn.appendWorkspaceEntryLocked(fileDel)
	txn.appendWorkspaceEntryLocked(link)
	rawBytes := blkEntryBytes(raw.bat)
	txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{wsRid: rt.tombRid}
	txn.EndStatement()

	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"SQL1's closing boundary is size-gated: the entries stay staged")
	require.Len(t, txn.writes, 3)
	txn.EndStatement()

	// SQL2's closing boundary: thresholds ungated, the snapshot crosses
	// the seeded generation change, and the one-shot sinker-write
	// injection fails the object route AFTER its carry journaled.
	txn.writeWorkspaceThreshold = 0
	txn.engine.config.extraWorkspaceThreshold = 0
	txn.engine.config.writeWorkspaceThreshold = 0
	before := fsInventory(t, base)
	snap = timestamp.Timestamp{PhysicalTime: 200}
	disarm := armRCBoundaryFault(t, objectio.FJ_CNTransferStagedWriteFailed)
	txn.StartStatement()
	err := txn.IncrStatementID(ctx, false)
	require.Error(t, err,
		"the injected sinker-write failure must abort the boundary")
	require.Contains(t, err.Error(), "transfer staged write failed by injection")
	disarm()

	// The carry REALLY ran before the failure: the dump translated the
	// origin key onto the spill identity, the object route re-keyed the
	// origin VALUE and the staged link cell onto the transferred
	// identity, and the journal holds exactly the two entries in
	// chronological order — the dump translation first, the object
	// carry second, both stamped with the failed statement's seq.
	txn.Lock()
	require.Len(t, txn.pkMoveRowidOrigins, 1)
	var spillKey types.Rowid
	for k, v := range txn.pkMoveRowidOrigins {
		require.NotEqual(t, wsRid, k,
			"the dump translated the origin ledger onto the spill identity")
		require.Equal(t, rt.newTombRid, v,
			"the object-route carry re-keyed the ledger's value onto the transferred identity")
		spillKey = k
	}
	require.Len(t, txn.pkMoveCarryJournal, 2)
	require.Equal(t, pkMoveCarryDump, txn.pkMoveCarryJournal[0].kind)
	require.Equal(t, []types.Rowid{wsRid}, txn.pkMoveCarryJournal[0].oldIds)
	require.Equal(t, []types.Rowid{spillKey}, txn.pkMoveCarryJournal[0].newIds)
	require.Equal(t, pkMoveCarryObject, txn.pkMoveCarryJournal[1].kind)
	require.Equal(t, []types.Rowid{rt.tombRid}, txn.pkMoveCarryJournal[1].oldIds)
	require.Equal(t, []types.Rowid{rt.newTombRid}, txn.pkMoveCarryJournal[1].newIds)
	require.Equal(t, txn.pkMoveCarryJournal[0].seq, txn.pkMoveCarryJournal[1].seq)
	require.Equal(t, txn.statementID, txn.pkMoveCarryJournal[0].seq)
	require.Empty(t, txn.retiredTransfers,
		"the failed round never reached retirement: the write precedes it")
	txn.Unlock()
	require.Len(t, txn.writes, 4,
		"raw, the staged tombstone-file entry, the link, and the spill tail")
	require.Equal(t, [][]byte{rt.newTombRid[:]}, linkCellsForTable(t, txn, 42),
		"the staged link cell was carried onto the transferred identity")

	// The frontend-equivalent statement rollback: the walker's strict
	// reverse unwind restores the object carry first, then the dump
	// translation — every map and the staged link cell read the
	// pre-boundary identity exactly, agreeing with the durable side
	// whose write failed (nothing ever replaced the tombstone object).
	spillName := singleNewDigestName(t, before, fsInventory(t, base))
	txn.tableCache = new(sync.Map)
	require.NoError(t, txn.RollbackLastStatement(ctx))
	txn.Lock()
	require.Equal(t, map[types.Rowid]types.Rowid{wsRid: rt.tombRid},
		txn.pkMoveRowidOrigins,
		"D142: the rolled-back boundary restored the pre-carry ledger exactly")
	require.Empty(t, txn.pkMoveCarryJournal)
	txn.Unlock()
	require.Len(t, txn.writes, 3)
	require.Same(t, raw.bat, txn.writes[0].bat)
	require.Equal(t, rawBytes, blkEntryBytes(txn.writes[0].bat),
		"the acknowledged raw entry survives byte-identical")
	require.Equal(t, [][]byte{rt.tombRid[:]}, linkCellsForTable(t, txn, 42),
		"D142: the staged link cell names the original identity again")
	require.Empty(t, txn.retiredTransfers)
	waitFileGone(t, base, spillName)
	require.Equal(t, before, fsInventory(t, base),
		"the failed boundary wrote nothing durable: the inventory is untouched")
	txn.EndStatement()

	// Retry with the injection lifted: the dump re-spills, the transfer
	// re-runs, PUBLISHES the replacement tombstone object, carries the
	// re-key onto it, retires the consumed input entry, and the
	// adoption walk settles the retirement record and drops the journal
	// in the same boundary (D145).
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"the retried boundary re-dumps, transfers, and adopts cleanly")
	txn.Lock()
	require.Len(t, txn.pkMoveRowidOrigins, 1)
	for k, v := range txn.pkMoveRowidOrigins {
		require.NotEqual(t, wsRid, k)
		require.Equal(t, rt.newTombRid, v,
			"the retried boundary's ledger resolves the transferred identity")
	}
	require.Empty(t, txn.pkMoveCarryJournal,
		"D145: the adoption dropped the retried statement's journaled carries")
	require.Len(t, txn.retiredTransfers, 1,
		"D147: the retirement record stays enumerated as the settled prefix")
	require.Equal(t, 1, txn.retiredTransfersSettled)
	require.Len(t, txn.retiredTransfers[0].entries, 1)
	require.Equal(t, retireStatNames(rt.tombStats),
		retireStatNames(txn.retiredTransfers[0].entries[0].stats...))
	require.False(t, stashOf(txn, rt.tombStats),
		"the retired input left the read-visible stash")
	txn.Unlock()
	require.Equal(t, [][]byte{rt.newTombRid[:]}, linkCellsForTable(t, txn, 42),
		"the staged link cell names the committed transferred identity")
	txn.EndStatement()

	// The retried boundary added exactly its spill and the replacement
	// tombstone object; the retired input tombstone object awaits the
	// close drain (unpublished files never leak, D147).
	afterAdopt := fsInventory(t, base)
	require.Len(t, afterAdopt, len(before)+2)
	require.Contains(t, afterAdopt, rt.tombStats.ObjectName().String(),
		"the consumed input tombstone still awaits the close drain")

	// Commit-terminal close: the drain enumerates the settled prefix too
	// (D147) — the consumed input file is reclaimed exactly once.
	txn.gcRetiredTransferFiles()
	require.Nil(t, txn.retiredTransfers)
	require.Zero(t, txn.retiredTransfersSettled)
	waitFileGone(t, base, rt.tombStats.ObjectName().String())
	finalInv := fsInventory(t, base)
	require.Len(t, finalInv, len(before)+1,
		"exactly the spill and the replacement minus the reclaimed input survive")
	for name := range afterAdopt {
		if name != rt.tombStats.ObjectName().String() {
			require.Contains(t, finalInv, name)
		}
	}
}

// TestPkMoveObjectRouteAdvanceSnapshotPostCarryFailureRoundTrip pins the
// D143 no-window route (a): an AdvanceSnapshot-driven object transfer
// journals its carry on a route that has NO boundary maintenance window
// at all; the injected post-carry write failure fails the statement, and
// RollbackLastStatement unwinds the journal — restoring the original
// identities — exactly as on the windowed route. The retry publishes,
// retires, and the following boundary's adoption settles both ledgers.
func TestPkMoveObjectRouteAdvanceSnapshotPostCarryFailureRoundTrip(t *testing.T) {
	proc, _, base := newR29SharedProc(t)
	fx := newR28Fixture(t, proc)
	ctx := context.Background()

	txn, _ := fx.newTxnTbl(t)
	txn.cn_flushed_s3_tombstone_object_stats_list = new(sync.Map)
	snap := timestamp.Timestamp{PhysicalTime: 100}
	swapRCOperatorWithSnapshotSourceForTest(t, txn,
		func() timestamp.Timestamp { return snap })
	txn.cnObjsSummary = make(map[types.Objectid]Summary)
	txn.deletedBlocks = &deletedBlocks{offsets: map[types.Blockid][]int64{}}
	txn.tableOps.addCreatedInTxn(42, 0)
	txn.engine.partitions = make(map[[2]uint64]*logtailreplay.Partition)

	rt := seedObjectRouteTable(t, proc, fx, txn, 42, "tbl", 7, 90, 150)

	raw := Entry{
		typ:          INSERT,
		accountId:    1,
		databaseId:   7,
		tableId:      42,
		databaseName: "db",
		tableName:    "tbl",
		bat:          newInsertBatchWithRowIDForTest(t, proc, []int64{7}),
		pkCheckPos:   1,
	}
	wsRid := vector.MustFixedColNoTypeCheck[types.Rowid](raw.bat.Vecs[0])[0]
	fileDel := fileDeleteEntryFor(t, proc, txn, 42, "tbl", rt.tombStats)
	link := stagedLinkFor(t, proc, 42, rt.tombRid, 1)

	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"the empty pre-SQL1 boundary must succeed")
	txn.appendWorkspaceEntryLocked(raw)
	txn.appendWorkspaceEntryLocked(fileDel)
	txn.appendWorkspaceEntryLocked(link)
	txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{wsRid: rt.tombRid}
	txn.EndStatement()

	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"SQL1's closing boundary acknowledges the staged statement's writes")
	require.Len(t, txn.writes, 3)
	txn.EndStatement()

	// Thresholds stay huge throughout: no dump ever runs on this route,
	// so the journal holds ONLY the object-route carry. The
	// AdvanceSnapshot of statement 2 crosses the seeded generation
	// change; the injected write failure aborts it AFTER the carry.
	before := fsInventory(t, base)
	snap = timestamp.Timestamp{PhysicalTime: 200}
	txn.StartStatement()
	disarm := armRCBoundaryFault(t, objectio.FJ_CNTransferStagedWriteFailed)
	err := txn.AdvanceSnapshot(ctx, snap)
	require.Error(t, err,
		"the injected sinker-write failure must abort the AdvanceSnapshot")
	require.Contains(t, err.Error(), "transfer staged write failed by injection")
	disarm()

	txn.Lock()
	require.Equal(t, map[types.Rowid]types.Rowid{wsRid: rt.newTombRid},
		txn.pkMoveRowidOrigins,
		"the object-route carry ran before the injected failure")
	require.Len(t, txn.pkMoveCarryJournal, 1,
		"no window: only the object-route carry journaled")
	require.Equal(t, pkMoveCarryObject, txn.pkMoveCarryJournal[0].kind)
	require.Equal(t, []types.Rowid{rt.tombRid}, txn.pkMoveCarryJournal[0].oldIds)
	require.Equal(t, []types.Rowid{rt.newTombRid}, txn.pkMoveCarryJournal[0].newIds)
	require.Equal(t, txn.statementID, txn.pkMoveCarryJournal[0].seq)
	require.Empty(t, txn.retiredTransfers)
	txn.Unlock()
	require.Equal(t, [][]byte{rt.newTombRid[:]}, linkCellsForTable(t, txn, 42))
	require.Len(t, txn.writes, 3,
		"the failed round published nothing: no tail appended")

	// The statement rollback unwinds the journaled carry exactly:
	// identities, the staged link cell, and the durable side (untouched
	// inventory) agree again; the roll-back restores the transfer
	// window so the retry consumes the same generation change.
	txn.tableCache = new(sync.Map)
	require.NoError(t, txn.RollbackLastStatement(ctx))
	txn.Lock()
	require.Equal(t, map[types.Rowid]types.Rowid{wsRid: rt.tombRid},
		txn.pkMoveRowidOrigins,
		"D143(a): the rolled-back statement restored the pre-carry ledger")
	require.Empty(t, txn.pkMoveCarryJournal)
	txn.Unlock()
	require.Equal(t, [][]byte{rt.tombRid[:]}, linkCellsForTable(t, txn, 42),
		"D143(a): the staged link cell names the original identity again")
	require.Len(t, txn.writes, 3)
	require.Equal(t, before, fsInventory(t, base))
	txn.EndStatement()

	// Retry: the disarmed AdvanceSnapshot re-transfers the restored
	// window, publishes the replacement, carries the re-key, and
	// retires the input; the ensuing boundary's adoption settles the
	// fresh record and drops the journal.
	txn.StartStatement()
	require.NoError(t, txn.AdvanceSnapshot(ctx, snap),
		"the retried AdvanceSnapshot transfers cleanly")
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"the statement's closing boundary adopts the transfer round")
	txn.Lock()
	require.Empty(t, txn.pkMoveCarryJournal)
	require.Len(t, txn.retiredTransfers, 1)
	require.Equal(t, 1, txn.retiredTransfersSettled)
	require.Equal(t, map[types.Rowid]types.Rowid{wsRid: rt.newTombRid},
		txn.pkMoveRowidOrigins,
		"the AdvanceSnapshot route keeps the workspace identity key (no dump)")
	require.False(t, stashOf(txn, rt.tombStats))
	txn.Unlock()
	require.Equal(t, [][]byte{rt.newTombRid[:]}, linkCellsForTable(t, txn, 42))
	txn.EndStatement()

	// Round-trip close: the drain reclaims the consumed input exactly
	// once from the settled prefix.
	afterAdopt := fsInventory(t, base)
	require.Len(t, afterAdopt, len(before)+1,
		"the AdvanceSnapshot route dumps nothing: only the replacement lands")
	txn.gcRetiredTransferFiles()
	require.Nil(t, txn.retiredTransfers)
	require.Zero(t, txn.retiredTransfersSettled)
	waitFileGone(t, base, rt.tombStats.ObjectName().String())
	require.Len(t, fsInventory(t, base), len(before),
		"input reclaimed: the replacement replaced it byte-for-row")
}

// TestPkMoveObjectRoutePeerFailureUnwindsFreshOnly pins D143(b) with
// D145: outside any window, an earlier table's object round COMPLETES —
// replacement published, input retired, provenance re-keyed — before a
// peer table's round journals its carry and fails at the injected
// sinker write. The failed statement's rollback must resurrect exactly
// the ACTIVE retirement (returning its files and its reservation),
// unwind BOTH fresh carries in strict reverse order, and leave any
// previously settled record untouched: eligibility is ledger
// membership, never the numeric seq the two statements happen to share.
func TestPkMoveObjectRoutePeerFailureUnwindsFreshOnly(t *testing.T) {
	proc, _, base := newR29SharedProc(t)
	fx := newR28Fixture(t, proc)
	ctx := context.Background()

	txn, _ := fx.newTxnTbl(t)
	txn.cn_flushed_s3_tombstone_object_stats_list = new(sync.Map)
	snap := timestamp.Timestamp{PhysicalTime: 100}
	swapRCOperatorWithSnapshotSourceForTest(t, txn,
		func() timestamp.Timestamp { return snap })
	txn.cnObjsSummary = make(map[types.Objectid]Summary)
	txn.deletedBlocks = &deletedBlocks{offsets: map[types.Blockid][]int64{}}
	txn.tableOps.addCreatedInTxn(42, 0)
	txn.tableOps.addCreatedInTxn(43, 0)
	txn.engine.partitions = make(map[[2]uint64]*logtailreplay.Partition)

	rt1 := seedObjectRouteTable(t, proc, fx, txn, 42, "tbl", 7, 90, 150)
	rt2 := seedObjectRouteTable(t, proc, fx, txn, 43, "tbl2", 8, 90, 150)
	sql1Input1, sql1Input2 := rt1.tombStats, rt2.tombStats

	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"the empty pre-SQL1 boundary must succeed")
	txn.appendWorkspaceEntryLocked(
		fileDeleteEntryFor(t, proc, txn, 42, "tbl", rt1.tombStats))
	txn.appendWorkspaceEntryLocked(stagedLinkFor(t, proc, 42, rt1.tombRid, 1))
	txn.appendWorkspaceEntryLocked(
		fileDeleteEntryFor(t, proc, txn, 43, "tbl2", rt2.tombStats))
	txn.appendWorkspaceEntryLocked(stagedLinkFor(t, proc, 43, rt2.tombRid, 2))
	// The provenance gate opens on ANY pk-move bookkeeping; the staged
	// link rows' owning tables are the cheapest sufficient membership,
	// matching the fill hook's pass-4 bookkeeping in production.
	txn.pkMoveLinkTables = map[pkMoveAcctTbl]struct{}{
		{account: 1, table: 42}: {},
		{account: 1, table: 43}: {},
	}
	txn.EndStatement()

	// SQL1: settle an adopted round first — both tables transfer
	// cleanly through AdvanceSnapshot and the statement's closing
	// boundary adopts: their retirements become the SETTLED prefix and
	// their carries leave the journal. This is the ledger the second
	// statement's rollback must never resurrect or re-resurrect.
	preSQL1Inv := fsInventory(t, base)
	snap = timestamp.Timestamp{PhysicalTime: 200}
	txn.StartStatement()
	require.NoError(t, txn.AdvanceSnapshot(ctx, snap),
		"SQL1: both tables transfer cleanly")
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"SQL1's closing boundary adopts both rounds")
	adoptedInv := fsInventory(t, base)
	require.Len(t, adoptedInv, len(preSQL1Inv)+2,
		"SQL1 published exactly the two replacements; the consumed inputs await the drain")

	txn.Lock()
	require.Len(t, txn.retiredTransfers, 2,
		"both tables retired their consumed inputs")
	require.Equal(t, 2, txn.retiredTransfersSettled,
		"D145: the adoption settled both records as the prefix")
	require.Empty(t, txn.pkMoveCarryJournal)
	txn.Unlock()
	settledCell1 := linkCellsForTable(t, txn, 42)
	settledCell2 := linkCellsForTable(t, txn, 43)
	require.Equal(t, [][]byte{rt1.newTombRid[:]}, settledCell1)
	require.Equal(t, [][]byte{rt2.newTombRid[:]}, settledCell2)
	txn.EndStatement()

	// SQL2 with two FRESH rounds is impossible on unchanged snapshots
	// (the transfer window moved past SQL1's generation change), so roll
	// the generation forward once more: rt1 and rt2 each gain a second
	// deleted-generation tombstone pair by re-driving the same seeding
	// with later timestamps.
	for i, rt := range []*objectRouteTable{rt1, rt2} {
		oldID2 := types.NewObjectid()
		oldStats2 := objectio.NewObjectStatsWithObjectID(&oldID2, false, true, false)
		require.NoError(t, objectio.SetObjectStatsSize(oldStats2, 1024))
		require.NoError(t, objectio.SetObjectStatsRowCnt(oldStats2, 1))
		require.NoError(t, objectio.SetObjectStatsBlkCnt(oldStats2, 1))
		newStats2 := fx.writeObj(t, rt.tbl, []int64{rt.pk + int64(i) + 20})
		require.Len(t, newStats2, 1)
		part := txn.engine.GetOrCreateLatestPart(ctx, 1, 7, rt.tableID)
		mut, done := part.MutateState()
		require.NoError(t, mut.HandleObjectEntry(ctx, nil, objectio.ObjectEntry{
			ObjectStats: *oldStats2,
			CreateTime:  types.BuildTS(150, 0),
			DeleteTime:  types.BuildTS(260, 0),
		}, false))
		require.NoError(t, mut.HandleObjectEntry(ctx, nil, objectio.ObjectEntry{
			ObjectStats: newStats2[0],
			CreateTime:  types.BuildTS(260, 0),
		}, false))
		done()
		oldBlk2 := objectio.NewBlockidWithObjectID(&oldID2, 0)
		rt.tombRid = types.NewRowid(&oldBlk2, 0)
		newBlk2 := objectio.NewBlockidWithObjectID(newStats2[0].ObjectName().ObjectId(), 0)
		rt.newTombRid = types.NewRowid(&newBlk2, 0)
		rt.tombStats = writePersistedTombstone(t, proc, rt.tombRid, rt.pk+int64(i)+20)
		rt.newStats = newStats2
	}

	// Statement 2 stages the fresh tombstone pairs with provenance; the
	// statement's own closing boundary ACKNOWLEDGES them (the transfer
	// window (200,200] is empty), so the failing statement 3 rolls back
	// none of their staged work.
	txn.StartStatement()
	txn.appendWorkspaceEntryLocked(
		fileDeleteEntryFor(t, proc, txn, 42, "tbl", rt1.tombStats))
	txn.appendWorkspaceEntryLocked(stagedLinkFor(t, proc, 42, rt1.tombRid, 3))
	txn.appendWorkspaceEntryLocked(
		fileDeleteEntryFor(t, proc, txn, 43, "tbl2", rt2.tombStats))
	txn.appendWorkspaceEntryLocked(stagedLinkFor(t, proc, 43, rt2.tombRid, 4))
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"SQL2's closing boundary acknowledges the fresh stage (empty window)")
	txn.EndStatement()

	// Statement 3 drives ONE AdvanceSnapshot with the seam armed on the
	// SECOND round only ("2:2::"): whatever table order the map yields,
	// exactly one table publishes plus retires and exactly one journals
	// then fails.
	before2 := fsInventory(t, base)
	baseLen := len(txn.writes)
	snap = timestamp.Timestamp{PhysicalTime: 300}
	txn.StartStatement()
	disarm := armRCBoundaryFaultFreq(
		t, objectio.FJ_CNTransferStagedWriteFailed, "2:2::")
	err := txn.AdvanceSnapshot(ctx, snap)
	require.Error(t, err,
		"the second round's injected sinker-write failure must abort")
	require.Contains(t, err.Error(), "transfer staged write failed by injection")
	disarm()

	txn.Lock()
	require.Len(t, txn.retiredTransfers, 3,
		"the settled pair stays; exactly one fresh record reached the ledger")
	require.Equal(t, 2, txn.retiredTransfersSettled,
		"the fresh record is ACTIVE: the adoption never ran for statement 3")
	var freshTable uint64
	switch retireStatNames(txn.retiredTransfers[2].entries[0].stats...)[0] {
	case retireStatNames(rt1.tombStats)[0]:
		freshTable = 42
	case retireStatNames(rt2.tombStats)[0]:
		freshTable = 43
	default:
		t.Fatalf("the fresh record must carry one of the two tables' input: %v",
			retireStatNames(txn.retiredTransfers[2].entries[0].stats...))
	}
	require.Len(t, txn.pkMoveCarryJournal, 2,
		"both fresh rounds journaled their carries: completed round AND failed round")
	for _, e := range txn.pkMoveCarryJournal {
		require.Equal(t, pkMoveCarryObject, e.kind)
		require.Len(t, e.oldIds, 1)
		require.Equal(t, txn.statementID, e.seq)
	}
	txn.Unlock()
	require.Len(t, txn.writes, baseLen+1,
		"only the completed round's published replacement appended")
	require.Equal(t, [][]byte{rt1.newTombRid[:]}, linkCellsForTable(t, txn, 42)[1:],
		"the fresh carry re-keyed table 42's fresh link cell")
	require.Equal(t, [][]byte{rt2.newTombRid[:]}, linkCellsForTable(t, txn, 43)[1:],
		"the fresh carry re-keyed table 43's fresh link cell")
	require.Equal(t, settledCell1, linkCellsForTable(t, txn, 42)[:1],
		"D145: the settled cell stays at the adopted identity")
	require.Equal(t, settledCell2, linkCellsForTable(t, txn, 43)[:1])

	// The rollback resurrects ONLY the active record: the fresh input
	// entry returns live (its file survived — it was acknowledged with
	// statement 2), the completed round's published tail is cut and
	// GC'd, both fresh carries unwind, and the settled prefix keeps its
	// post-adoption state untouched.
	repName := singleNewDigestName(t, before2, fsInventory(t, base))
	txn.tableCache = new(sync.Map)
	require.NoError(t, txn.RollbackLastStatement(ctx))
	txn.Lock()
	require.Len(t, txn.retiredTransfers, 2,
		"only the settled prefix remains after the resurrection")
	require.Equal(t, 2, txn.retiredTransfersSettled)
	require.Empty(t, txn.pkMoveCarryJournal)
	txn.Unlock()
	freshStats := rt2.tombStats
	if freshTable == 42 {
		freshStats = rt1.tombStats
	}
	require.True(t, resurrectedDeleteEntryStats(txn, freshStats.ObjectName().String()),
		"the active record's superseded input returned to press")
	require.Equal(t, settledCell1, linkCellsForTable(t, txn, 42)[:1],
		"settled record's carried state untouched by the fresh rollback")
	require.Equal(t, settledCell2, linkCellsForTable(t, txn, 43)[:1])
	require.Equal(t, [][]byte{rt1.tombRid[:]}, linkCellsForTable(t, txn, 42)[1:],
		"the fresh carry on table 42 unwound to the original identity")
	require.Equal(t, [][]byte{rt2.tombRid[:]}, linkCellsForTable(t, txn, 43)[1:],
		"the fresh carry on table 43 unwound to the original identity")
	waitFileGone(t, base, repName)
	require.Equal(t, before2, fsInventory(t, base),
		"the completed round's published tail rolled back with its file")
	txn.EndStatement()

	// Retry cleanly: both fresh tombstones (the resurrected input and
	// the still-staged peer input) transfer and the closing boundary
	// adopts; the settled prefix GROWS by the two fresh records and the
	// close drain consumes every record's file exactly once.
	txn.StartStatement()
	require.NoError(t, txn.AdvanceSnapshot(ctx, snap),
		"the retried AdvanceSnapshot transfers both fresh inputs cleanly")
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"the closing boundary adopts the retried rounds")
	txn.Lock()
	require.Len(t, txn.retiredTransfers, 4)
	require.Equal(t, 4, txn.retiredTransfersSettled,
		"D145: the retried statement's records settled at adoption")
	require.Empty(t, txn.pkMoveCarryJournal)
	txn.Unlock()
	require.Equal(t, [][]byte{rt1.newTombRid[:]}, linkCellsForTable(t, txn, 42)[1:])
	require.Equal(t, [][]byte{rt2.newTombRid[:]}, linkCellsForTable(t, txn, 43)[1:])
	require.Equal(t, settledCell1, linkCellsForTable(t, txn, 42)[:1])
	require.Equal(t, settledCell2, linkCellsForTable(t, txn, 43)[:1])
	txn.EndStatement()
	require.Len(t, fsInventory(t, base), len(before2)+2,
		"two fresh replacements published; their inputs await the close drain")

	txn.gcRetiredTransferFiles()
	require.Nil(t, txn.retiredTransfers)
	require.Zero(t, txn.retiredTransfersSettled)
	for _, stats := range []objectio.ObjectStats{
		sql1Input1, sql1Input2, rt1.tombStats, rt2.tombStats,
	} {
		waitFileGone(t, base, stats.ObjectName().String())
	}
}
