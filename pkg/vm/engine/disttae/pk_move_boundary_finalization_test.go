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

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// --- r29 review: boundary-finalization ownership proofs for D123/D124/D125.
//
// These tests pin the ownership contract the r29 review demanded:
//
//   - D123 (r29 F1): a completed compaction wave's destructive tail —
//     original-entry release, superseded-object reclamation, and the
//     consumed deletion ledger's drain — stays STAGED until every
//     fallible step of the enclosing boundary succeeded. A boundary whose
//     wave completed but whose dump then failed must roll back leaving
//     the acknowledged originals, siblings, and pending deletions intact,
//     and the retry must reach exactly the committed row set.
//
//   - D124 (r29 F2): pending deletions carry the statement id that
//     deleted them. A failed statement boundary keeps the finished
//     statements' deletions without any manual repair — proven through
//     the PRODUCTION deleteBatch diversion, which is the only path that
//     stages them in service.
//
//   - D125 (r29 F3): an unpublished replacement whose in-writer reclaim
//     AND the failed-wave cleanup both failed transfers to the
//     transaction-retained inventory, and the transaction close's GC
//     drain reclaims it — no double failure can leak an ownerless file.
//
// --- r30 review: surviving mutations of the staged state (D126/D127/D128).
//
// The r30 review attacked the seams between staging and effect:
//
//   - D126 (r30 F1): original-entry release is by batch IDENTITY, never
//     by a positional index recorded at wave time. The boundary dump
//     splices a dumped entry out of txn.writes and appends its spilled
//     replacement at the tail, and the workspace-merge compaction drops
//     nil slots in place — both move the staged original before adoption.
//     The two tests here dump/merge AHEAD of and BEHIND the original and
//     assert exact surviving rows plus file ownership.
//
//   - D127 (r30 F2): the rollback cut widens only after EVERY fallible
//     RC step succeeded. An injected snapshot-refresh or tombstone-
//     transfer failure after a completed wave must roll back the
//     boundary's replacement through the pre-maintenance cut, and the
//     retry must reach exactly the committed row set.
//
//   - D128 (r30 F3): close-time reclamation whose submission or delete
//     fails keeps its cleanup owner. The durable orphan registry is
//     populated BEFORE the fallible attempt and released only by a
//     confirmed deletion; a later sweep (the piggyback body, called
//     directly here) recovers and the filesystem inventory returns to
//     the pre-wave state exactly.

// boundaryFailFS wraps the shared store with deterministic failure knobs
// for the r29 proofs. The file is self-contained so the r29 RED tree —
// era-22 production code plus era-22 test files — compiles and runs these
// tests against it unchanged.
type boundaryFailFS struct {
	fileservice.FileService

	mu sync.Mutex
	// passWrites lets that many armed writes through untouched; failWrites
	// then fails that many writes with a transient error WITHOUT
	// persisting. Arm the pair so a known wave persist passes and the
	// boundary dump's persist fails after it.
	passWrites int
	failWrites int
	// failDeletes fails that many next Delete calls transiently.
	failDeletes int
	// deleteBarrier, when non-nil, blocks every Delete right AFTER its
	// attempt is recorded until the channel is closed — the D130
	// coalescing proof's vehicle for holding one sweep in flight while
	// overlapping triggers and registrations arrive.
	deleteBarrier chan struct{}
	// deleteGate/deleteGateNames/deleteGateErr are the targeted r32
	// variant of the barrier: a Delete naming any gated path blocks
	// right AFTER its attempt is recorded until the gate closes, then
	// returns deleteGateErr instead of touching the store. Per-name
	// aiming keeps concurrent ungated deletes (a draining sweep, an
	// engine-close pass) deterministic fail-injection-free.
	deleteGate      chan struct{}
	deleteGateNames map[string]struct{}
	deleteGateErr   error
	// writeCancelFn arms the commit-ambiguous shape: the next Write is
	// performed, fn runs, and the write reports context.Canceled (the
	// D122 vehicle).
	writeCancelFn context.CancelFunc

	writes  []string
	deletes []boundaryDeleteRecord
}

type boundaryDeleteRecord struct {
	files    []string
	ctxErr   error
	injected bool
}

func (fs *boundaryFailFS) Write(ctx context.Context, vector fileservice.IOVector) error {
	fs.mu.Lock()
	fs.writes = append(fs.writes, vector.FilePath)
	if fs.writeCancelFn != nil {
		fn := fs.writeCancelFn
		fs.writeCancelFn = nil
		fs.mu.Unlock()
		if err := fs.FileService.Write(ctx, vector); err != nil {
			return err
		}
		fn()
		return context.Canceled
	}
	if fs.passWrites > 0 {
		fs.passWrites--
		fs.mu.Unlock()
		return fs.FileService.Write(ctx, vector)
	}
	if fs.failWrites > 0 {
		fs.failWrites--
		fs.mu.Unlock()
		return context.DeadlineExceeded
	}
	fs.mu.Unlock()
	return fs.FileService.Write(ctx, vector)
}

func (fs *boundaryFailFS) Delete(ctx context.Context, filePaths ...string) error {
	fs.mu.Lock()
	rec := boundaryDeleteRecord{
		files:  append([]string(nil), filePaths...),
		ctxErr: ctx.Err(),
	}
	inject := fs.failDeletes > 0
	if inject {
		fs.failDeletes--
	}
	rec.injected = inject
	fs.deletes = append(fs.deletes, rec)
	barrier := fs.deleteBarrier
	gate := fs.deleteGate
	gateErr := fs.deleteGateErr
	if gate != nil {
		hit := false
		for _, p := range filePaths {
			if _, ok := fs.deleteGateNames[p]; ok {
				hit = true
				break
			}
		}
		if !hit {
			gate = nil
			gateErr = nil
		}
	}
	fs.mu.Unlock()

	if barrier != nil {
		<-barrier
	}
	if gate != nil {
		<-gate
		if gateErr != nil {
			return gateErr
		}
	}
	if inject {
		return context.DeadlineExceeded
	}
	return fs.FileService.Delete(ctx, filePaths...)
}

func (fs *boundaryFailFS) deleteAttempts() []boundaryDeleteRecord {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return append([]boundaryDeleteRecord(nil), fs.deletes...)
}

// newR29SharedProc mirrors newR28SharedProc with the r29 failure wrapper.
func newR29SharedProc(t *testing.T) (*process.Process, *boundaryFailFS, fileservice.FileService) {
	t.Helper()
	ctx := context.Background()
	local, err := fileservice.NewLocalFS(
		ctx, defines.LocalFileServiceName, t.TempDir(), fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	base, err := fileservice.NewLocalFS(
		ctx, defines.SharedFileServiceName, t.TempDir(), fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	etl, err := fileservice.NewLocalETLFS(defines.ETLFileServiceName, t.TempDir())
	require.NoError(t, err)
	inj := &boundaryFailFS{FileService: base}
	group, err := fileservice.NewFileServices("", local, inj, etl)
	require.NoError(t, err)
	t.Cleanup(func() { group.Close(context.Background()) })
	proc := testutil.NewProcess(t, testutil.WithFileService(group))
	if proc.Ctx == nil {
		proc.Ctx = ctx
	}
	return proc, inj, base
}

// TestPkMoveCompletedCompactionSurvivesDumpFailure pins r29 review F1
// (D123): the complete maintenance boundary — compaction success
// FOLLOWED by a failure inside the boundary dump — rolls back to the
// pre-boundary state exactly: the acknowledged original keeps its batch
// and file, the sibling tail write stays byte-stable, the drained
// deletion ledger returns, and the retried boundary commits exactly the
// re-keyed two-row replacement plus the tail write. The dump failure is
// the REAL production kind: the origin-keyed tail batch has no usable pk
// column position, so the dump-shadow capture errors BEFORE any
// workspace mutation below it. On the pre-r29 shape the completed wave
// released the original batch and reclaimed its file inline, so the
// first post-failure assertion fails with the original destroyed while
// its replacement rolls away.
func TestPkMoveCompletedCompactionSurvivesDumpFailure(t *testing.T) {
	proc, _, base := newR29SharedProc(t)
	fx := newR28Fixture(t, proc)
	ctx := context.Background()

	txn, tbl := fx.newTxnTbl(t)
	expectMockBoundaryOps(txn)

	// SQL1: one flushed entry carrying the three-row original object.
	origStats := fx.writeObj(t, tbl, []int64{1, 2, 3})
	require.Len(t, origStats, 1)
	origName := origStats[0].ObjectName().String()
	e0 := fx.flushedEntry(t, origStats[0].ObjectLocation().String(), origStats[0])
	objID := *origStats[0].ObjectName().ObjectId()
	blk := objectio.NewBlockidWithObjectID(origStats[0].ObjectName().ObjectId(), 0)
	txn.cnObjsSummary = map[types.Objectid]Summary{
		objID: {
			objBat:     e0.bat,
			accountId:  1,
			databaseId: 7,
			tableId:    42,
			tbName:     "tbl",
			dbName:     "db",
		},
	}

	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"the empty pre-SQL1 boundary must succeed")
	txn.appendWorkspaceEntryLocked(e0)
	e0Bytes := blkEntryBytes(txn.writes[0].bat)
	txn.EndStatement()

	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"SQL1's clean closing boundary must succeed")

	// SQL2 executes: the original's row 0 dies with the survivor
	// provenance-named (a healthy, dirty, provenance-relevant wave), and
	// a raw tail insert carries a second durable-origin entry. The tail
	// batch deliberately lacks a usable pk column position, so the
	// boundary dump's shadow capture fails on it — the REAL dump error
	// landing AFTER the completed wave (r29 F1's shape) with no FS
	// injection at all.
	survivor := types.NewRowid(&blk, 1)
	e1 := Entry{
		typ:          INSERT,
		accountId:    1,
		databaseId:   7,
		tableId:      42,
		databaseName: "db",
		tableName:    "tbl",
		bat:          newInsertBatchWithRowIDForTest(t, proc, []int64{7}),
	}
	e1Rid := vector.MustFixedColNoTypeCheck[types.Rowid](e1.bat.Vecs[0])[0]
	txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{
		survivor: types.BuildTestRowid(30, 2),
		e1Rid:    types.BuildTestRowid(30, 8),
	}
	txn.deletedBlocks = &deletedBlocks{
		offsets: map[types.Blockid][]int64{blk: {0}},
	}
	txn.appendWorkspaceEntryLocked(e1)
	e1Bytes := blkEntryBytes(txn.writes[1].bat)
	// The boundary dump must actually RUN — with the era fixture's huge
	// thresholds it would return at the size gate and no error could
	// land after the completed wave, which is exactly the hole.
	txn.writeWorkspaceThreshold = 0
	txn.engine.config.extraWorkspaceThreshold = 0
	txn.engine.config.writeWorkspaceThreshold = 0
	before := fsInventory(t, base)
	txn.EndStatement()

	txn.StartStatement()
	err := txn.IncrStatementID(ctx, false)
	require.Error(t, err,
		"a failure inside the boundary dump must abort the whole boundary")
	require.Contains(t, err.Error(), "pk-move dump shadow",
		"the failure is the dump shadow capture on the origin-keyed tail batch")

	// F1 pin: the completed wave published its replacement, but the
	// acknowledged original must NOT be released — the boundary never
	// became irrevocable — and the dump's own destructive splice never
	// ran (the shadow capture happened before it).
	require.Len(t, txn.writes, 3,
		"the completed wave published exactly the replacement next to the originals")
	require.NotNil(t, txn.writes[0].bat,
		"D123: the completed compaction's original survives the later dump failure")
	require.NotNil(t, txn.writes[1].bat,
		"SQL2's tail write survives the failed dump")
	rep1 := txn.writes[2]
	require.Equal(t, INSERT, rep1.typ)
	require.NotEmpty(t, rep1.fileName)
	rep1Name := singleNewDigestName(t, before, fsInventory(t, base))

	// The frontend-equivalent rollback drops the boundary's own tail —
	// the published replacement — and discards the staged finalization:
	// the drained ledger returns for the retry, the originals stay
	// byte-stable, and the replacement's file is reclaimed by the
	// boundary-statement range GC.
	txn.tableCache = new(sync.Map)
	require.NoError(t, txn.RollbackLastStatement(ctx))
	require.Len(t, txn.writes, 2,
		"only the failed boundary's own appends roll away")
	require.Same(t, e0.bat, txn.writes[0].bat)
	require.Same(t, e1.bat, txn.writes[1].bat)
	require.Equal(t, e0Bytes, blkEntryBytes(txn.writes[0].bat),
		"SQL1's flushed entry is byte-stable across compaction success + dump failure + rollback")
	require.Equal(t, e1Bytes, blkEntryBytes(txn.writes[1].bat),
		"SQL2's tail write is byte-stable across the failed boundary")
	require.Equal(t, 1, txn.deletedBlocks.size(),
		"D124: the discarded finalization restores the wave-consumed ledger for the retry")
	txn.Lock()
	require.Contains(t, txn.pkMoveRowidOrigins, survivor,
		"D123: the discard restores the pre-carry provenance identity re-minted away by the failed boundary")
	txn.Unlock()
	waitFileGone(t, base, rep1Name)
	require.Contains(t, fsInventory(t, base), origName,
		"the superseded original's file was never reclaimed")
	txn.EndStatement()

	// Retry on the untouched state with the shadow vehicle disarmed: the
	// restored ledger flows into a healthy wave, the dump spills the tail
	// write, and only the irrevocable adoption releases the original and
	// reclaims its file.
	delete(txn.pkMoveRowidOrigins, e1Rid)
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"the retried boundary compacts and dumps the untouched staged state")
	require.Len(t, txn.writes, 2,
		"D129 finalized layout: the adoption released the original, husked the spilled slot, and compacted both — [rep, spill]")
	require.NotNil(t, txn.writes[0].bat,
		"only the adopted boundary releases the original batch — the replacement leads the finalized layout")
	require.NotEmpty(t, txn.writes[1].fileName,
		"the retried dump re-spilled the tail write at the tail")

	// The exact committed row set: rows {2,3} of the original through the
	// replacement, and the tail write's row {7} through the dump spill.
	var rep2, e1Dump *Entry
	for i := range txn.writes {
		w := &txn.writes[i]
		if w.typ != INSERT || w.bat == nil || len(w.bat.Attrs) == 0 ||
			w.bat.Attrs[0] != catalog.BlockMeta_BlockInfo {
			continue
		}
		col, area := vector.MustVarlenaRawData(w.bat.Vecs[1])
		rows := uint32(0)
		for j := range col {
			st := objectio.ObjectStats(col[j].GetByteSlice(area))
			rows += st.Rows()
		}
		switch rows {
		case 2:
			rep2 = w
		case 1:
			e1Dump = w
		}
	}
	require.NotNil(t, rep2,
		"the retried wave published exactly the two-row replacement")
	require.NotNil(t, e1Dump,
		"the retried dump spilled exactly the one-row tail write")
	repCol, repArea := vector.MustVarlenaRawData(rep2.bat.Vecs[1])
	require.Len(t, repCol, 1)
	repStats := objectio.ObjectStats(repCol[0].GetByteSlice(repArea))
	rep2Name := repStats.ObjectName().String()
	rep2Obj := *repStats.ObjectName().ObjectId()
	txn.Lock()
	require.Len(t, txn.pkMoveRowidOrigins, 1)
	for wsRid, originRid := range txn.pkMoveRowidOrigins {
		require.Equal(t, rep2Obj, *wsRid.BorrowObjectID(),
			"the survivor's provenance re-minted onto the retried replacement")
		require.Equal(t, types.BuildTestRowid(30, 2), originRid)
	}
	txn.Unlock()
	waitFileGone(t, base, origName)
	after := fsInventory(t, base)
	require.Contains(t, after, rep2Name)
	require.NotContains(t, after, rep1Name)
	txn.EndStatement()

	// Trailing boundary: the finalized state compact-merges cleanly.
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false))
	txn.EndStatement()
}

// TestPkMoveFailedBoundaryRetainsProductionDivertedDeletions pins r29
// review F2 (D124) through the PRODUCTION delete path: a delete of a row
// living in an uncommitted CN object diverts into the pending-deletion
// ledger tagged with the executing statement. A failed closing boundary
// and its frontend-equivalent rollback must keep that ledger entry
// without any manual repair, and the retry must publish exactly the
// delete's effect. On the pre-r29 shape the failed wave's deferred clean
// (and the rollback's unconditional clean) wiped the ledger, so this
// fails first at the ledger-size pin and the retry would publish the
// unchanged original — resurrecting the deleted row.
func TestPkMoveFailedBoundaryRetainsProductionDivertedDeletions(t *testing.T) {
	proc, _, base := newR29SharedProc(t)
	fx := newR28Fixture(t, proc)
	ctx := context.Background()

	txn, tbl := fx.newTxnTbl(t)
	expectMockBoundaryOps(txn)
	txn.deletedBlocks = &deletedBlocks{offsets: map[types.Blockid][]int64{}}
	txn.batchSelectList = make(map[*batch.Batch][]int64)

	// SQL1: one flushed entry carrying the three-row original object,
	// summary-registered and provenance-named on its row 1.
	origStats := fx.writeObj(t, tbl, []int64{1, 2, 3})
	require.Len(t, origStats, 1)
	e0 := fx.flushedEntry(t, origStats[0].ObjectLocation().String(), origStats[0])
	objID := *origStats[0].ObjectName().ObjectId()
	blk := objectio.NewBlockidWithObjectID(origStats[0].ObjectName().ObjectId(), 0)
	txn.cnObjsSummary = map[types.Objectid]Summary{
		objID: {
			objBat:     e0.bat,
			accountId:  1,
			databaseId: 7,
			tableId:    42,
			tbName:     "tbl",
			dbName:     "db",
		},
	}
	survivor := types.NewRowid(&blk, 1)
	txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{
		survivor: types.BuildTestRowid(30, 2),
	}
	// The wave-failure vehicle: sever the table pk so the rewrite
	// worker's provenance shadow genuinely errors.
	savedName := tbl.tableDef.Pkey.PkeyColName
	tbl.tableDef.Pkey.PkeyColName = "no_such_column"
	restore := func() { tbl.tableDef.Pkey.PkeyColName = savedName }
	t.Cleanup(restore)

	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"the empty pre-SQL1 boundary must succeed")
	txn.appendWorkspaceEntryLocked(e0)
	e0Bytes := blkEntryBytes(txn.writes[0].bat)
	txn.EndStatement()

	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"SQL1's clean closing boundary must succeed")

	// The finishing statement deletes the original's row 0 through the
	// PRODUCTION delete path: the row lives in an uncommitted CN object,
	// so deleteBatch diverts it into the statement-tagged ledger.
	deadRid := types.NewRowid(&blk, 0)
	server := colexec.MustGetServer(txn.engine.service)
	server.PutCnSegment(txn.op.Txn().ID, 42, deadRid.BorrowSegmentID(),
		colexec.TxnWorkspaceUnCommitType)
	del := batch.NewWithSize(2)
	del.SetAttributes([]string{catalog.Row_ID, "pk"})
	del.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixed(del.Vecs[0], deadRid, false, fx.mp))
	del.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(del.Vecs[1], int64(1), false, fx.mp))
	del.SetRowCount(1)
	defer del.Clean(fx.mp)
	out, err := txn.deleteBatch(del, 7, 42)
	require.NoError(t, err)
	require.Zero(t, out.RowCount(),
		"the diverted delete leaves no tombstone row behind")
	require.Equal(t, 1, txn.deletedBlocks.size(),
		"the production delete path staged exactly the pending deletion")
	before := fsInventory(t, base)
	txn.EndStatement()

	// The closing boundary dies inside the rewrite worker's shadow.
	txn.StartStatement()
	err = txn.IncrStatementID(ctx, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "pk-move rewrite shadow")
	require.Len(t, txn.writes, 1,
		"a failed wave publishes nothing")
	// F2 pin: the failed wave consumed NOTHING.
	require.Equal(t, 1, txn.deletedBlocks.size(),
		"D124: the failed boundary must not drain the pending-deletion ledger")

	// The rolled-back statement is the failed BOUNDARY itself; the
	// deletion belongs to the finished statement and must survive.
	txn.tableCache = new(sync.Map)
	require.NoError(t, txn.RollbackLastStatement(ctx))
	require.Equal(t, 1, txn.deletedBlocks.size(),
		"D124: the boundary rollback keeps the finished statement's deletions")
	require.Len(t, txn.writes, 1)
	require.Same(t, e0.bat, txn.writes[0].bat)
	require.Equal(t, e0Bytes, blkEntryBytes(txn.writes[0].bat))
	require.Equal(t, before, fsInventory(t, base))
	txn.EndStatement()

	// Retry with the injection lifted and WITHOUT repairing any internal
	// state: the production-staged deletion flows into the retried wave
	// on its own, and the adopted boundary commits exactly the re-keyed
	// two-row replacement.
	restore()
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false))
	require.Len(t, txn.writes, 2,
		"original (released) plus the published replacement")
	require.Nil(t, txn.writes[0].bat)
	rep := txn.writes[1]
	require.Equal(t, INSERT, rep.typ)
	require.NotNil(t, rep.bat)
	repCol, repArea := vector.MustVarlenaRawData(rep.bat.Vecs[1])
	require.Len(t, repCol, 1)
	repStats := objectio.ObjectStats(repCol[0].GetByteSlice(repArea))
	require.EqualValues(t, 2, repStats.Rows(),
		"the exact committed row set: the production delete is not resurrected")
	require.Equal(t, 0, txn.deletedBlocks.size(),
		"the adopted boundary consumed the ledger")
	txn.EndStatement()

	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false))
	txn.EndStatement()
}

// TestPkMoveFailedWaveOrphansReclaimedAtTxnClose pins r29 review F3
// (D125): when the rewrite writer's own reclaim fails AND the failed
// wave's cleanup pass fails too, the unpublished replacement transfers to
// the transaction-retained inventory before the local ownership
// disappears, and the transaction close reclaims it through the GC
// drain. On the pre-r29 shape the second failure only logged a warning —
// nothing owned the file — so the orphan survives the close and the
// waitFileGone assertion times out with exactly two delete attempts.
func TestPkMoveFailedWaveOrphansReclaimedAtTxnClose(t *testing.T) {
	proc, inj, base := newR29SharedProc(t)
	fx := newR28Fixture(t, proc)
	ctx := context.Background()

	txn, tbl := fx.newTxnTbl(t)
	txn.deletedBlocks = &deletedBlocks{offsets: map[types.Blockid][]int64{}}
	origStats := fx.writeObj(t, tbl, []int64{1, 2, 3})
	require.Len(t, origStats, 1)
	txn.appendWorkspaceEntryLocked(fx.flushedEntry(t,
		origStats[0].ObjectLocation().String(), origStats[0]))
	objID := *origStats[0].ObjectName().ObjectId()
	blk := objectio.NewBlockidWithObjectID(origStats[0].ObjectName().ObjectId(), 0)
	txn.cnObjsSummary = map[types.Objectid]Summary{
		objID: {
			objBat:     txn.writes[0].bat,
			accountId:  1,
			databaseId: 7,
			tableId:    42,
			tbName:     "tbl",
			dbName:     "db",
		},
	}
	survivor := types.NewRowid(&blk, 1)
	txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{
		survivor: types.BuildTestRowid(30, 2),
	}
	txn.deletedBlocks = &deletedBlocks{
		offsets: map[types.Blockid][]int64{blk: {0}},
	}
	before := fsInventory(t, base)

	// Commit-ambiguous persist + BOTH cleanup attempts failing: the
	// writer's own reclaim (D122 defer) misses, then the failed-wave
	// cleanup misses too.
	reqCtx, cancel := context.WithCancel(ctx)
	inj.writeCancelFn = cancel
	inj.setFailDeletes(2)

	txn.Lock()
	err := txn.compactDeletionOnObjsLocked(reqCtx)
	txn.Unlock()
	require.Error(t, err)
	require.Contains(t, err.Error(), "context canceled")

	orphanName := singleNewDigestName(t, before, fsInventory(t, base))
	recs := inj.deleteAttempts()
	require.Len(t, recs, 2,
		"the writer defer's reclaim and the failed-wave cleanup are the two failures")
	require.True(t, recs[0].injected, "attempt 1: the writer defer's reclaim")
	require.True(t, recs[1].injected, "attempt 2: the failed-wave cleanup")
	require.Len(t, txn.writes, 1,
		"the failed wave published nothing")
	require.NotNil(t, txn.writes[0].bat)

	// The transaction closes: the close-time GC drain owns the retained
	// inventory of last resort and reclaims the doubly failed orphan.
	txn.delTransaction()
	waitFileGone(t, base, orphanName)
	recs = inj.deleteAttempts()
	require.Len(t, recs, 3,
		"the transaction close issues the third reclaim through the GC drain")
	require.False(t, recs[2].injected,
		"the knobs were exhausted — the close-time reclaim really deletes")
	require.NoError(t, recs[2].ctxErr,
		"the close-time reclaim runs on a cancellation-free context")
	require.Eventually(t, func() bool {
		return len(durableOrphanGCNames(txn.engine)) == 0
	}, 10*time.Second, 5*time.Millisecond,
		"D128: the confirmed close-time deletion releases every retained name")
}

// swapRCOperatorForTest replaces the fixture's SI operator with a
// read-committed one so statement boundaries exercise the real RC
// maintenance legs. The snapshot refresh itself stays infallible
// (UpdateSnapshot returns nil) — the r30 F2 tests arm the specific
// failure injection each one pins.
func swapRCOperatorForTest(t *testing.T, txn *Transaction) {
	t.Helper()
	ctrl := gomock.NewController(t)
	op := mock_frontend.NewMockTxnOperator(ctrl)
	op.EXPECT().Txn().Return(txnpb.TxnMeta{
		ID:        []byte("txn-test-rc"),
		Isolation: txnpb.TxnIsolation_RC,
	}).AnyTimes()
	op.EXPECT().Status().Return(txnpb.TxnStatus_Active).AnyTimes()
	op.EXPECT().GetWorkspace().Return(txn).AnyTimes()
	op.EXPECT().SnapshotTS().Return(timestamp.Timestamp{PhysicalTime: 100}).AnyTimes()
	op.EXPECT().UpdateSnapshot(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	txn.op = op
	expectMockBoundaryOps(txn)
}

// swapRCOperatorWithSnapshotSourceForTest behaves like
// swapRCOperatorForTest but lets the test advance the operator's
// snapshot timestamp between statements, so the RC transfer's
// (lastTransferred, SnapshotTS] window covers generation changes the
// test seeds between boundaries. The created database's operator is
// re-pointed too: relation resolution flows through the created-on-txn
// database handle, which captured the fixture's original operator.
func swapRCOperatorWithSnapshotSourceForTest(
	t *testing.T, txn *Transaction, snapshot func() timestamp.Timestamp) {
	t.Helper()
	ctrl := gomock.NewController(t)
	op := mock_frontend.NewMockTxnOperator(ctrl)
	op.EXPECT().Txn().Return(txnpb.TxnMeta{
		ID:        []byte("txn-test-rc"),
		Isolation: txnpb.TxnIsolation_RC,
	}).AnyTimes()
	op.EXPECT().Status().Return(txnpb.TxnStatus_Active).AnyTimes()
	op.EXPECT().GetWorkspace().Return(txn).AnyTimes()
	op.EXPECT().SnapshotTS().DoAndReturn(
		func() timestamp.Timestamp { return snapshot() }).AnyTimes()
	op.EXPECT().UpdateSnapshot(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	op.EXPECT().IsSnapOp().Return(false).AnyTimes()
	txn.op = op
	expectMockBoundaryOps(txn)
	if db := txn.databaseOps.existAndActive(genDatabaseKey(1, "db")); db != nil {
		db.op = op
	}
}

// armRCBoundaryFault enables one named RC-boundary fault point and
// returns its disarm; the disarm is also cleanup-scheduled so a failed
// assertion cannot leak an armed global fault into a sibling test.
func armRCBoundaryFault(t *testing.T, key string) (disarm func()) {
	t.Helper()
	fault.Enable()
	rm, err := objectio.SimpleInject(key)
	require.NoError(t, err)
	disarmed := false
	disarm = func() {
		if disarmed {
			return
		}
		disarmed = true
		rm()
		fault.Disable()
	}
	t.Cleanup(func() { disarm() })
	return disarm
}

// TestPkMoveAdoptionReleasesOriginalAfterPrecedingDump pins r30 review
// F1 shape (a) (D126): dumpable entries PRECEDE the staged original. For
// [raw A, original O] the completed wave appends O' at the tail and the
// boundary dump then splices A out ahead of O, appending its spill at
// the tail: [O, O', A']. A positional release recorded at wave time
// (O sat at index 1) names O' instead — destroying the replacement the
// adoption is about to finalize, while the acknowledged original keeps
// its batch and its file stays referenced. r31 D129 makes the windowed
// dump append-only and folds husk-then-compact into the adoption, so
// the finalized layout is [O', A']; a pre-D129 tree fails at the
// finalized-layout length pin.
func TestPkMoveAdoptionReleasesOriginalAfterPrecedingDump(t *testing.T) {
	proc, _, base := newR29SharedProc(t)
	fx := newR28Fixture(t, proc)
	ctx := context.Background()

	txn, tbl := fx.newTxnTbl(t)
	expectMockBoundaryOps(txn)

	origStats := fx.writeObj(t, tbl, []int64{1, 2, 3})
	require.Len(t, origStats, 1)
	origName := origStats[0].ObjectName().String()
	blk := objectio.NewBlockidWithObjectID(origStats[0].ObjectName().ObjectId(), 0)

	// SQL1: a raw dumpable insert FIRST, then the flushed original.
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"the empty pre-SQL1 boundary must succeed")
	raw := Entry{
		typ:          INSERT,
		accountId:    1,
		databaseId:   7,
		tableId:      42,
		databaseName: "db",
		tableName:    "tbl",
		bat:          newInsertBatchWithRowIDForTest(t, proc, []int64{7}),
	}
	txn.appendWorkspaceEntryLocked(raw)
	e0 := fx.flushedEntry(t, origStats[0].ObjectLocation().String(), origStats[0])
	txn.appendWorkspaceEntryLocked(e0)
	txn.EndStatement()

	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"SQL1's closing boundary keeps both entries — the dump is still size-gated")

	// SQL2 dirties the original the healthy way: row 0 dies, the row-1
	// survivor is provenance-named, and the boundary dump must actually
	// RUN so the preceding raw entry spills ahead of the original.
	survivor := types.NewRowid(&blk, 1)
	objID := *origStats[0].ObjectName().ObjectId()
	txn.cnObjsSummary = map[types.Objectid]Summary{
		objID: {
			objBat:     e0.bat,
			accountId:  1,
			databaseId: 7,
			tableId:    42,
			tbName:     "tbl",
			dbName:     "db",
		},
	}
	txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{
		survivor: types.BuildTestRowid(30, 2),
	}
	txn.deletedBlocks = &deletedBlocks{
		offsets: map[types.Blockid][]int64{blk: {0}},
	}
	txn.writeWorkspaceThreshold = 0
	txn.engine.config.extraWorkspaceThreshold = 0
	txn.engine.config.writeWorkspaceThreshold = 0
	before := fsInventory(t, base)
	txn.EndStatement()

	// The SUCCESSFUL boundary: the wave stages O for release, appends O',
	// the windowed dump (r31 D129) spills A to the tail while keeping A's
	// raw slot live, and the adoption releases O, husks A's slot, and
	// compacts both husks — the finalized layout is [O', A'].
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"the healthy wave plus the preceding dump must adopt")
	require.Len(t, txn.writes, 2,
		"D129: the adoption released the original, husked the spilled slot, and compacted both husks")

	// F1a pin: the ORIGINAL — not its successor — was released, and the
	// surviving replacement leads the finalized layout.
	require.NotNil(t, txn.writes[0].bat,
		"D126+D129: identity release found exactly the original under the dump's reorder; a positional release destroys the replacement here")
	require.NotEmpty(t, txn.writes[0].fileName,
		"the published replacement survives its own adoption")
	repCol, repArea := vector.MustVarlenaRawData(txn.writes[0].bat.Vecs[1])
	require.Len(t, repCol, 1)
	repStats := objectio.ObjectStats(repCol[0].GetByteSlice(repArea))
	require.EqualValues(t, 2, repStats.Rows(),
		"exact surviving rows: {2,3} of the original")

	require.NotEmpty(t, txn.writes[1].fileName,
		"the preceding raw entry spilled to its own object at the tail")
	aCol, aArea := vector.MustVarlenaRawData(txn.writes[1].bat.Vecs[1])
	require.Len(t, aCol, 1)
	aStats := objectio.ObjectStats(aCol[0].GetByteSlice(aArea))
	require.EqualValues(t, 1, aStats.Rows(),
		"exact surviving rows: the spilled {7}")

	waitFileGone(t, base, origName)
	after := fsInventory(t, base)
	require.Contains(t, after, repStats.ObjectName().String())
	require.Contains(t, after, aStats.ObjectName().String())
	require.Len(t, after, len(before)+1,
		"file ownership: original reclaimed, replacement and spill added — nothing else")

	// The carry re-minted the provenance onto the SURVIVING replacement.
	repObj := *repStats.ObjectName().ObjectId()
	txn.Lock()
	require.Len(t, txn.pkMoveRowidOrigins, 1)
	for wsRid, originRid := range txn.pkMoveRowidOrigins {
		require.Equal(t, repObj, *wsRid.BorrowObjectID(),
			"the survivor's provenance names the replacement the adoption kept")
		require.Equal(t, types.BuildTestRowid(30, 2), originRid)
	}
	txn.Unlock()
	txn.EndStatement()

	// Trailing boundary: the finalized state rides through cleanly.
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false))
	txn.EndStatement()
}

// TestPkMoveAdoptionReleasesOriginalAcrossWorkspaceMerge pins r30 review
// F1 shape (b) (D126): the workspace-merge compaction drops a nil slot
// BELOW the staged original, moving it before adoption. A same-table
// pair sits below the original; the appended 31st mergeable entry trips
// the foo quorum, the pair folds, and the fold's nil slot would let an
// in-place compaction shift the original down by one — a positional
// release recorded at wave time then names the original's neighbour
// instead. r31 D129 defers the compaction to the adoption, which
// releases by identity first and compacts second; the pre-D129 tree
// fails at the finalized-layout length pin.
func TestPkMoveAdoptionReleasesOriginalAcrossWorkspaceMerge(t *testing.T) {
	proc, _, base := newR29SharedProc(t)
	fx := newR28Fixture(t, proc)
	ctx := context.Background()

	txn, tbl := fx.newTxnTbl(t)
	expectMockBoundaryOps(txn)

	origStats := fx.writeObj(t, tbl, []int64{1, 2, 3})
	require.Len(t, origStats, 1)
	origName := origStats[0].ObjectName().String()
	blk := objectio.NewBlockidWithObjectID(origStats[0].ObjectName().ObjectId(), 0)

	// SQL1: a same-table raw pair first, then 26 distinct-table fillers,
	// the flushed original, and a distinct tail — 29 merge-eligible
	// entries, one short of the compaction quorum, so SQL1's boundary
	// changes nothing.
	rawEntry := func(tableID uint64, pk int64) Entry {
		return Entry{
			typ:          INSERT,
			accountId:    1,
			databaseId:   7,
			tableId:      tableID,
			databaseName: "db",
			tableName:    "dummy",
			bat:          newInsertBatchWithRowIDForTest(t, proc, []int64{pk}),
		}
	}
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"the empty pre-SQL1 boundary must succeed")
	txn.appendWorkspaceEntryLocked(rawEntry(42, 5)) // @0 merges below the original
	txn.appendWorkspaceEntryLocked(rawEntry(42, 6)) // @1 fold source — the nil slot
	for i := 0; i < 26; i++ {
		txn.appendWorkspaceEntryLocked(rawEntry(uint64(1001+i), int64(101+i)))
	}
	e0 := fx.flushedEntry(t, origStats[0].ObjectLocation().String(), origStats[0])
	txn.appendWorkspaceEntryLocked(e0)                // @28 the staged original
	txn.appendWorkspaceEntryLocked(rawEntry(1099, 9)) // @29 the shifted neighbour
	txn.EndStatement()

	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"SQL1's closing boundary must not compact: 29 eligible entries stay one short")
	require.Len(t, txn.writes, 30)
	require.Same(t, e0.bat, txn.writes[28].bat,
		"the original still sits at its staged position")

	// SQL2 dirties the original the healthy way and appends the 31st
	// mergeable entry, tripping the quorum at the boundary. The dump
	// stays size-gated: F1b isolates the merge compaction alone.
	survivor := types.NewRowid(&blk, 1)
	objID := *origStats[0].ObjectName().ObjectId()
	txn.cnObjsSummary = map[types.Objectid]Summary{
		objID: {
			objBat:     e0.bat,
			accountId:  1,
			databaseId: 7,
			tableId:    42,
			tbName:     "tbl",
			dbName:     "db",
		},
	}
	txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{
		survivor: types.BuildTestRowid(30, 2),
	}
	txn.deletedBlocks = &deletedBlocks{
		offsets: map[types.Blockid][]int64{blk: {0}},
	}
	txn.appendWorkspaceEntryLocked(rawEntry(1098, 11))
	before := fsInventory(t, base)
	txn.EndStatement()

	// The SUCCESSFUL boundary: the windowed wave stages O (at 28) for
	// release and appends O' at the tail; the quorum fires and folds the
	// pair (the source slot at 1 goes nil), and — r31 D129 — nothing
	// compacts mid-window: the adoption releases O, then compacts the
	// fold husk and the released original, finalizing [pair, fillers,
	// neighbour, tail write, replacement]. A positional release recorded
	// at wave time would still destroy a neighbour; identity release
	// tracks O wherever the window leaves it.
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"the healthy wave plus the quorum compaction must adopt")
	require.Len(t, txn.writes, 30,
		"D129: adoption compacted the fold husk and the released original — pair fold, fillers, neighbour, tail write, replacement")
	require.Equal(t, 2, txn.writes[0].bat.RowCount(),
		"the pair folded into its first entry")

	// F1b pin: the ORIGINAL was released by identity across the deferred
	// compaction; the pre-merge neighbour, SQL2's tail write, and the
	// replacement stand exactly where the finalized layout puts them.
	neighbourIdx, tailIdx, repIdx := 27, 28, 29
	require.Equal(t, uint64(1099), txn.writes[neighbourIdx].tableId,
		"D129: the pre-merge neighbour survived the finalized layout untouched")
	require.NotNil(t, txn.writes[neighbourIdx].bat,
		"a positional release would have destroyed this neighbour instead")
	require.Equal(t, uint64(1098), txn.writes[tailIdx].tableId,
		"SQL2's tail write rides the finalized layout untouched")
	require.NotEmpty(t, txn.writes[repIdx].fileName,
		"D126+D129: the published replacement survives its own adoption — only the original was released")
	repCol, repArea := vector.MustVarlenaRawData(txn.writes[repIdx].bat.Vecs[1])
	require.Len(t, repCol, 1)
	repStats := objectio.ObjectStats(repCol[0].GetByteSlice(repArea))
	require.EqualValues(t, 2, repStats.Rows(),
		"exact surviving rows: {2,3} of the original")

	waitFileGone(t, base, origName)
	after := fsInventory(t, base)
	require.Contains(t, after, repStats.ObjectName().String())
	require.Len(t, after, len(before),
		"file ownership: original reclaimed, replacement added — net zero change")
	require.NotContains(t, after, origName)

	// The carry re-minted the provenance onto the SURVIVING replacement.
	repObj := *repStats.ObjectName().ObjectId()
	txn.Lock()
	require.Len(t, txn.pkMoveRowidOrigins, 1)
	for wsRid, originRid := range txn.pkMoveRowidOrigins {
		require.Equal(t, repObj, *wsRid.BorrowObjectID(),
			"the survivor's provenance names the replacement the adoption kept")
		require.Equal(t, types.BuildTestRowid(30, 2), originRid)
	}
	txn.Unlock()
	txn.EndStatement()

	// Trailing boundary: the finalized state rides through cleanly.
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false))
	txn.EndStatement()
}

// runRCBoundaryFailure drives the shared r30 review F2 (D127) shape on a
// READ-COMMITTED transaction: a healthy completed wave followed by one
// injected failure inside the RC maintenance. The rollback cut must
// truncate the published replacement through the pre-maintenance offset;
// widening it before the RC legs leaves the replacement cohabiting with
// the restored originals on retry. The RED tree fails at the
// post-rollback workspace-length pin.
func runRCBoundaryFailure(t *testing.T, faultKey string) {
	proc, _, base := newR29SharedProc(t)
	fx := newR28Fixture(t, proc)
	ctx := context.Background()

	txn, tbl := fx.newTxnTbl(t)
	swapRCOperatorForTest(t, txn)

	origStats := fx.writeObj(t, tbl, []int64{1, 2, 3})
	require.Len(t, origStats, 1)
	origName := origStats[0].ObjectName().String()
	e0 := fx.flushedEntry(t, origStats[0].ObjectLocation().String(), origStats[0])
	objID := *origStats[0].ObjectName().ObjectId()
	blk := objectio.NewBlockidWithObjectID(origStats[0].ObjectName().ObjectId(), 0)
	txn.cnObjsSummary = map[types.Objectid]Summary{
		objID: {
			objBat:     e0.bat,
			accountId:  1,
			databaseId: 7,
			tableId:    42,
			tbName:     "tbl",
			dbName:     "db",
		},
	}

	// Boundary #0 and #1 run the REAL RC maintenance: the snapshot
	// refresh consults the operator and the tombstone transfer no-ops on
	// an INSERT-only workspace.
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"the empty pre-SQL1 RC boundary must succeed")
	txn.appendWorkspaceEntryLocked(e0)
	e0Bytes := blkEntryBytes(txn.writes[0].bat)
	txn.EndStatement()

	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"SQL1's closing RC boundary must succeed")
	require.Len(t, txn.transfer.statements, 2,
		"each successful RC boundary captured its placeholder")

	// SQL2 dirties the original the healthy way; the failure then lands
	// INSIDE the RC maintenance, after the wave published the
	// replacement — exactly r30 F2's object-rewrite-success shape.
	survivor := types.NewRowid(&blk, 1)
	txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{
		survivor: types.BuildTestRowid(30, 2),
	}
	txn.deletedBlocks = &deletedBlocks{
		offsets: map[types.Blockid][]int64{blk: {0}},
	}
	before := fsInventory(t, base)
	txn.EndStatement()

	disarm := armRCBoundaryFault(t, faultKey)
	txn.StartStatement()
	err := txn.IncrStatementID(ctx, false)
	require.Error(t, err,
		"the injected RC-maintenance failure must abort the whole boundary")
	require.Contains(t, err.Error(), "injection",
		"the failure is the armed RC boundary injection")
	disarm()
	require.Len(t, txn.writes, 2,
		"the wave completed before the failure — the replacement was published")
	rep1Name := singleNewDigestName(t, before, fsInventory(t, base))

	// The frontend-equivalent rollback: the cut must hold the
	// PRE-maintenance offset, so the boundary's whole tail — the
	// published replacement — truncates and its file is reclaimed.
	txn.tableCache = new(sync.Map)
	require.NoError(t, txn.RollbackLastStatement(ctx))
	require.Len(t, txn.writes, 1,
		"D127: the tentative cut removes the failed boundary's replacement; a pre-RC widened cut keeps it")
	require.Same(t, e0.bat, txn.writes[0].bat)
	require.Equal(t, e0Bytes, blkEntryBytes(txn.writes[0].bat),
		"SQL1's flushed entry is byte-stable across the failed RC boundary")
	require.Equal(t, 1, txn.deletedBlocks.size(),
		"D124: the discarded finalization restores the wave-consumed ledger for the retry")
	require.Len(t, txn.transfer.statements, 2,
		"the failed boundary's transfer placeholder popped")
	txn.Lock()
	require.Contains(t, txn.pkMoveRowidOrigins, survivor,
		"D123: the discard restores the pre-carry provenance identity")
	txn.Unlock()
	waitFileGone(t, base, rep1Name)
	require.Contains(t, fsInventory(t, base), origName,
		"the superseded original's file was never reclaimed")
	txn.EndStatement()

	// Retry on the untouched state with the injection lifted: the
	// restored ledger flows into a healthy wave, the RC legs run for
	// real, and only the irrevocable adoption releases the original.
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"the retried boundary compacts and refreshes on the untouched staged state")
	require.Len(t, txn.writes, 2,
		"original (released) plus the published replacement")
	require.Nil(t, txn.writes[0].bat,
		"only the adopted boundary releases the original batch")
	rep := txn.writes[1]
	require.Equal(t, INSERT, rep.typ)
	require.NotNil(t, rep.bat)
	repCol, repArea := vector.MustVarlenaRawData(rep.bat.Vecs[1])
	require.Len(t, repCol, 1)
	repStats := objectio.ObjectStats(repCol[0].GetByteSlice(repArea))
	require.EqualValues(t, 2, repStats.Rows(),
		"the exact committed row set: no double-published survivors")
	repObj := *repStats.ObjectName().ObjectId()
	txn.Lock()
	require.Len(t, txn.pkMoveRowidOrigins, 1)
	for wsRid, originRid := range txn.pkMoveRowidOrigins {
		require.Equal(t, repObj, *wsRid.BorrowObjectID(),
			"the survivor's provenance re-minted onto the retried replacement")
		require.Equal(t, types.BuildTestRowid(30, 2), originRid)
	}
	txn.Unlock()
	require.Len(t, txn.transfer.statements, 3,
		"the retried boundary captured its own placeholder")
	waitFileGone(t, base, origName)
	txn.EndStatement()

	// Trailing boundary: the finalized state rides the RC legs cleanly.
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false))
	txn.EndStatement()
}

// TestPkMoveRCSnapshotFailureKeepsRollbackCutTentative pins r30 review
// F2 (D127) on the snapshot-refresh leg: the injected failure lands
// BEFORE handleRCSnapshot runs, after the workspace merge and dump
// succeeded.
func TestPkMoveRCSnapshotFailureKeepsRollbackCutTentative(t *testing.T) {
	runRCBoundaryFailure(t, objectio.FJ_CNIncrStmtRCSnapshotFailed)
}

// TestPkMoveRCTransferFailureKeepsRollbackCutTentative pins r30 review
// F2 (D127) on the tombstone-transfer leg: the real snapshot refresh ran
// successfully, the failure lands immediately before the transfer —
// every fallible step must precede the widened cut, whichever one fails.
func TestPkMoveRCTransferFailureKeepsRollbackCutTentative(t *testing.T) {
	runRCBoundaryFailure(t, objectio.FJ_CNIncrStmtRCTransferFailed)
}

// TestPkMoveCloseCleanupFailureKeepsOrphanOwnedUntilRecovered pins r30
// review F3 (D128) on the storage leg: the close-time GC drain's delete
// fails too. Ownership transfers to the durable orphan registry BEFORE
// the fallible attempt and is released only by a confirmed deletion, so
// the thrice-failed orphan is neither lost nor ownerless: a later sweep
// (the piggyback body, called directly here) recovers it and the
// filesystem inventory returns to the pre-wave state exactly. The RED
// tree fails at the retained-inventory pin.
func TestPkMoveCloseCleanupFailureKeepsOrphanOwnedUntilRecovered(t *testing.T) {
	proc, inj, base := newR29SharedProc(t)
	fx := newR28Fixture(t, proc)
	ctx := context.Background()

	txn, tbl := fx.newTxnTbl(t)
	txn.deletedBlocks = &deletedBlocks{offsets: map[types.Blockid][]int64{}}
	origStats := fx.writeObj(t, tbl, []int64{1, 2, 3})
	require.Len(t, origStats, 1)
	txn.appendWorkspaceEntryLocked(fx.flushedEntry(t,
		origStats[0].ObjectLocation().String(), origStats[0]))
	objID := *origStats[0].ObjectName().ObjectId()
	blk := objectio.NewBlockidWithObjectID(origStats[0].ObjectName().ObjectId(), 0)
	txn.cnObjsSummary = map[types.Objectid]Summary{
		objID: {
			objBat:     txn.writes[0].bat,
			accountId:  1,
			databaseId: 7,
			tableId:    42,
			tbName:     "tbl",
			dbName:     "db",
		},
	}
	survivor := types.NewRowid(&blk, 1)
	txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{
		survivor: types.BuildTestRowid(30, 2),
	}
	txn.deletedBlocks = &deletedBlocks{
		offsets: map[types.Blockid][]int64{blk: {0}},
	}
	before := fsInventory(t, base)

	// Commit-ambiguous persist plus THREE failing deletes: the writer's
	// own reclaim (D122 defer), the failed-wave cleanup, AND the close-
	// time GC drain's attempt (r30 F3's terminal failure).
	reqCtx, cancel := context.WithCancel(ctx)
	inj.writeCancelFn = cancel
	inj.setFailDeletes(3)

	txn.Lock()
	err := txn.compactDeletionOnObjsLocked(reqCtx)
	txn.Unlock()
	require.Error(t, err)
	require.Contains(t, err.Error(), "context canceled")

	orphanName := singleNewDigestName(t, before, fsInventory(t, base))
	recs := inj.deleteAttempts()
	require.Len(t, recs, 2,
		"the writer defer's reclaim and the failed-wave cleanup consumed the first two knobs")
	require.True(t, recs[0].injected)
	require.True(t, recs[1].injected)

	// The transaction closes: the close-time drain registers the name
	// BEFORE its own attempt, so the third failure cannot orphan it.
	txn.delTransaction()
	require.Equal(t, []string{orphanName}, durableOrphanGCNames(txn.engine),
		"D128: the failed close-time delete retains cleanup ownership; pre-D128 nothing did")
	require.Contains(t, fsInventory(t, base), orphanName,
		"the thrice-failed orphan is still unpublished garbage on disk")
	require.Eventually(t, func() bool {
		return len(inj.deleteAttempts()) == 3
	}, 10*time.Second, 5*time.Millisecond,
		"the close-time drain issued the third reclaim")
	recs = inj.deleteAttempts()
	require.True(t, recs[2].injected,
		"the third knob consumed exactly the close-time attempt")

	// Recovery: a later GC admission retries the sweep — the piggyback
	// body is called directly here as the deterministic probe.
	retryDurableOrphanGC(txn.engine)
	waitFileGone(t, base, orphanName)
	require.Eventually(t, func() bool {
		return len(durableOrphanGCNames(txn.engine)) == 0
	}, 10*time.Second, 5*time.Millisecond,
		"the confirmed deletion releases the retained ownership")
	recs = inj.deleteAttempts()
	require.Len(t, recs, 4)
	require.False(t, recs[3].injected,
		"the knobs were exhausted — the recovered sweep really deletes")
	require.NoError(t, recs[3].ctxErr)
	require.Equal(t, before, fsInventory(t, base),
		"filesystem inventory returns to the pre-wave state exactly")
}

// TestPkMoveGCAdmissionFailureKeepsOrphanOwnedUntilRecovered pins r30
// review F3 (D128) on the SUBMISSION leg: the close-time drain cannot
// even admit its task (no GC pool). Ownership was registered before the
// attempt, so nothing is deleted, nothing is lost, and a later sweep
// with a live pool recovers the orphan. The RED tree fails at the
// retained-inventory pin.
func TestPkMoveGCAdmissionFailureKeepsOrphanOwnedUntilRecovered(t *testing.T) {
	proc, inj, base := newR29SharedProc(t)
	fx := newR28Fixture(t, proc)
	ctx := context.Background()

	txn, tbl := fx.newTxnTbl(t)
	txn.deletedBlocks = &deletedBlocks{offsets: map[types.Blockid][]int64{}}
	origStats := fx.writeObj(t, tbl, []int64{1, 2, 3})
	require.Len(t, origStats, 1)
	txn.appendWorkspaceEntryLocked(fx.flushedEntry(t,
		origStats[0].ObjectLocation().String(), origStats[0]))
	objID := *origStats[0].ObjectName().ObjectId()
	blk := objectio.NewBlockidWithObjectID(origStats[0].ObjectName().ObjectId(), 0)
	txn.cnObjsSummary = map[types.Objectid]Summary{
		objID: {
			objBat:     txn.writes[0].bat,
			accountId:  1,
			databaseId: 7,
			tableId:    42,
			tbName:     "tbl",
			dbName:     "db",
		},
	}
	survivor := types.NewRowid(&blk, 1)
	txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{
		survivor: types.BuildTestRowid(30, 2),
	}
	txn.deletedBlocks = &deletedBlocks{
		offsets: map[types.Blockid][]int64{blk: {0}},
	}
	before := fsInventory(t, base)

	// Same D122/D125 double failure as the r29 proof: the writer's own
	// reclaim and the failed-wave cleanup both die.
	reqCtx, cancel := context.WithCancel(ctx)
	inj.writeCancelFn = cancel
	inj.setFailDeletes(2)

	txn.Lock()
	err := txn.compactDeletionOnObjsLocked(reqCtx)
	txn.Unlock()
	require.Error(t, err)
	require.Contains(t, err.Error(), "context canceled")

	orphanName := singleNewDigestName(t, before, fsInventory(t, base))
	recs := inj.deleteAttempts()
	require.Len(t, recs, 2,
		"the writer defer's reclaim and the failed-wave cleanup failed")
	require.True(t, recs[0].injected)
	require.True(t, recs[1].injected)

	// Admission failure: the transaction closes with no GC pool at all.
	pool := txn.engine.gcPool
	txn.engine.gcPool = nil
	txn.delTransaction()
	require.Equal(t, []string{orphanName}, durableOrphanGCNames(txn.engine),
		"D128: ownership registered before the admission failure; pre-D128 nothing owned the file")
	require.Contains(t, fsInventory(t, base), orphanName,
		"the unpublished orphan is still on disk")
	require.Len(t, inj.deleteAttempts(), 2,
		"the unavailable pool issued no delete attempt")

	// Recovery: a live pool admits the retained sweep — the piggyback
	// body is called directly here as the deterministic probe.
	txn.engine.gcPool = pool
	retryDurableOrphanGC(txn.engine)
	waitFileGone(t, base, orphanName)
	require.Eventually(t, func() bool {
		return len(durableOrphanGCNames(txn.engine)) == 0
	}, 10*time.Second, 5*time.Millisecond,
		"the confirmed deletion releases the retained ownership")
	recs = inj.deleteAttempts()
	require.Len(t, recs, 3)
	require.False(t, recs[2].injected)
	require.NoError(t, recs[2].ctxErr)
	require.Equal(t, before, fsInventory(t, base),
		"filesystem inventory returns to the pre-wave state exactly")
}

// twoNewDigestNames returns the sorted names present in after but not
// before, requiring exactly want of them — the multi-file counterpart of
// singleNewDigestName for the D129 dump-spill shapes.
func twoNewDigestNames(t *testing.T, before, after map[string]uint64, want int) []string {
	t.Helper()
	extras := make([]string, 0, want)
	for name := range after {
		if _, ok := before[name]; !ok {
			extras = append(extras, name)
		}
	}
	sort.Strings(extras)
	require.Len(t, extras, want,
		"the failed boundary published the replacement and its spill file — nothing else")
	return extras
}

// runRCBoundaryFailureWithPrecedingDump combines r31 review F1's two
// attacking surfaces: D127's injected RC-maintenance failure lands AFTER
// the windowed dump spilled a preceding acknowledged raw entry. The
// pre-D129 tree reshaped the acknowledged region against the recorded
// cut — the dump spliced A out and appended its spill at the tail — so
// the positional rollback GC-deleted A's only surviving copy (the spill)
// while the original's replacement O' escaped the cut; D129 keeps the
// window append-only, so the cut truncates exactly [O', A'] and A's raw
// slot survives byte-identical. The RED tree fails at the failed-window
// layout pin (the era-24 dump removed/relocated the raw slot) or, on the
// sham D127-only shape, at the rollback layout: A would stand alone with
// its file already deleted by the cut covering the spill.
func runRCBoundaryFailureWithPrecedingDump(t *testing.T, faultKey string) {
	proc, _, base := newR29SharedProc(t)
	fx := newR28Fixture(t, proc)
	ctx := context.Background()

	txn, tbl := fx.newTxnTbl(t)
	swapRCOperatorForTest(t, txn)

	origStats := fx.writeObj(t, tbl, []int64{1, 2, 3})
	require.Len(t, origStats, 1)
	origName := origStats[0].ObjectName().String()
	blk := objectio.NewBlockidWithObjectID(origStats[0].ObjectName().ObjectId(), 0)
	objID := *origStats[0].ObjectName().ObjectId()

	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"the empty pre-SQL1 boundary must succeed")

	// SQL1: raw dumpable insert A FIRST, then the flushed original O.
	raw := Entry{
		typ:          INSERT,
		accountId:    1,
		databaseId:   7,
		tableId:      42,
		databaseName: "db",
		tableName:    "tbl",
		bat:          newInsertBatchWithRowIDForTest(t, proc, []int64{7}),
	}
	txn.appendWorkspaceEntryLocked(raw)
	e0 := fx.flushedEntry(t, origStats[0].ObjectLocation().String(), origStats[0])
	txn.appendWorkspaceEntryLocked(e0)
	aBytes := blkEntryBytes(raw.bat)
	e0Bytes := blkEntryBytes(e0.bat)
	txn.EndStatement()

	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"SQL1's closing boundary keeps both entries — the dump is still size-gated")
	require.Len(t, txn.transfer.statements, 2)

	// SQL2 dirties the original the healthy way and ungates the dump.
	survivor := types.NewRowid(&blk, 1)
	txn.cnObjsSummary = map[types.Objectid]Summary{
		objID: {
			objBat:     e0.bat,
			accountId:  1,
			databaseId: 7,
			tableId:    42,
			tbName:     "tbl",
			dbName:     "db",
		},
	}
	txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{
		survivor: types.BuildTestRowid(30, 2),
	}
	txn.deletedBlocks = &deletedBlocks{
		offsets: map[types.Blockid][]int64{blk: {0}},
	}
	txn.writeWorkspaceThreshold = 0
	txn.engine.config.extraWorkspaceThreshold = 0
	txn.engine.config.writeWorkspaceThreshold = 0
	before := fsInventory(t, base)
	txn.EndStatement()

	// The armed failure boundary, combining both r31 F1 surfaces: the
	// wave appends O', the WINDOWED dump spills A to the tail (A's slot
	// stays live), and the injected RC-maintenance fault then aborts the
	// boundary before any adoption.
	disarm := armRCBoundaryFault(t, faultKey)
	txn.StartStatement()
	err := txn.IncrStatementID(ctx, false)
	require.Error(t, err,
		"the injected RC-maintenance failure must abort the whole boundary")
	require.Contains(t, err.Error(), "injection")
	disarm()
	require.Len(t, txn.writes, 4,
		"D129: the window kept the acknowledged slots live; the tail is exactly [O', A']")
	require.Same(t, raw.bat, txn.writes[0].bat,
		"the preceding raw entry kept its live batch in place through the failed window")
	require.Equal(t, aBytes, blkEntryBytes(txn.writes[0].bat),
		"the preceding raw entry stayed byte-identical while the dump spilled it")
	require.Same(t, e0.bat, txn.writes[1].bat,
		"the original kept its batch — no release ran inside the failed window")
	twoNew := twoNewDigestNames(t, before, fsInventory(t, base), 2)
	rep1Name, spill1Name := twoNew[0], twoNew[1]

	// The frontend-equivalent rollback: the cut names the pre-maintenance
	// end=2, so ONLY the boundary's tail truncates — spill file and
	// replacement file both reclaimed — and A's live slot plus the
	// original survive with their content untouched.
	txn.tableCache = new(sync.Map)
	require.NoError(t, txn.RollbackLastStatement(ctx))
	require.Len(t, txn.writes, 2,
		"D129: the cut truncated exactly the tail; a pre-window cut over the reshaped layout would drop A's only surviving copy")
	require.Same(t, raw.bat, txn.writes[0].bat)
	require.Equal(t, aBytes, blkEntryBytes(txn.writes[0].bat),
		"the acknowledged raw entry survives the rollback byte-identical")
	require.Same(t, e0.bat, txn.writes[1].bat)
	require.Equal(t, e0Bytes, blkEntryBytes(txn.writes[1].bat))
	require.Equal(t, 1, txn.deletedBlocks.size(),
		"D124: the discarded finalization restores the wave-consumed ledger for the retry")
	require.Len(t, txn.transfer.statements, 2,
		"the failed boundary's transfer placeholder popped")
	txn.Lock()
	require.Contains(t, txn.pkMoveRowidOrigins, survivor)
	txn.Unlock()
	waitFileGone(t, base, rep1Name)
	waitFileGone(t, base, spill1Name)
	require.Equal(t, before, fsInventory(t, base),
		"file ownership returns to the pre-boundary state exactly — A's spill was not the copy the cut destroyed")
	txn.EndStatement()

	// Retry on the untouched acknowledged state with the injection
	// lifted: the healthy wave re-publishes the replacement, the windowed
	// dump re-spills A, the RC legs run for real, and the adoption
	// releases the original and husks A's slot — finalizing [O', A'].
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"the retried boundary adopts the finalized layout")
	require.Len(t, txn.writes, 2,
		"replacement plus the re-spilled raw entry")
	rep := txn.writes[0]
	require.NotNil(t, rep.bat)
	require.NotEmpty(t, rep.fileName)
	repCol, repArea := vector.MustVarlenaRawData(rep.bat.Vecs[1])
	require.Len(t, repCol, 1)
	repStats := objectio.ObjectStats(repCol[0].GetByteSlice(repArea))
	require.EqualValues(t, 2, repStats.Rows(),
		"exact committed rows: {2,3} — no double-published survivors")
	spill := txn.writes[1]
	require.NotNil(t, spill.bat)
	require.NotEmpty(t, spill.fileName)
	sCol, sArea := vector.MustVarlenaRawData(spill.bat.Vecs[1])
	require.Len(t, sCol, 1)
	sStats := objectio.ObjectStats(sCol[0].GetByteSlice(sArea))
	require.EqualValues(t, 1, sStats.Rows(),
		"the retried dump re-spilled exactly A's row {7} once")
	repObj := *repStats.ObjectName().ObjectId()
	txn.Lock()
	require.Len(t, txn.pkMoveRowidOrigins, 1)
	for wsRid, originRid := range txn.pkMoveRowidOrigins {
		require.Equal(t, repObj, *wsRid.BorrowObjectID(),
			"the survivor's provenance re-minted onto the retried replacement")
		require.Equal(t, types.BuildTestRowid(30, 2), originRid)
	}
	txn.Unlock()
	require.Len(t, txn.transfer.statements, 3,
		"the retried boundary captured its own placeholder")
	waitFileGone(t, base, origName)
	after := fsInventory(t, base)
	require.Contains(t, after, repStats.ObjectName().String())
	require.Contains(t, after, sStats.ObjectName().String())
	require.Len(t, after, len(before)+1,
		"file ownership after retry: original reclaimed, replacement and spill added — nothing else")
	txn.EndStatement()

	// Trailing boundary: the finalized state rides the RC legs cleanly.
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false))
	txn.EndStatement()
}

// TestPkMoveRCSnapshotFailureSurvivesPrecedingDump pins r31 review F1 on
// the snapshot-refresh leg with the preceding-dump layout (D129).
func TestPkMoveRCSnapshotFailureSurvivesPrecedingDump(t *testing.T) {
	runRCBoundaryFailureWithPrecedingDump(t, objectio.FJ_CNIncrStmtRCSnapshotFailed)
}

// TestPkMoveRCTransferFailureSurvivesPrecedingDump pins r31 review F1 on
// the tombstone-transfer leg with the preceding-dump layout (D129).
func TestPkMoveRCTransferFailureSurvivesPrecedingDump(t *testing.T) {
	runRCBoundaryFailureWithPrecedingDump(t, objectio.FJ_CNIncrStmtRCTransferFailed)
}

// runRCBoundaryFailureWithQuorumMerge combines the same RC-maintenance
// failure with the workspace-merge quorum fold: 30 same-table raw
// entries fold mid-window, husk 29 slots across the acknowledged region,
// and the fold itself dumps, spilling to the tail. The pre-D129 tree
// compacted the husks in place (sliding the original) and pre-husked the
// dumped slot against the recorded cut; D129 defers both, so rollback
// restores exactly the fold, the husk compact runs only after the cut,
// and no positional record ever aliases a neighbour. The RED tree fails
// at the failed-window layout pin.
func runRCBoundaryFailureWithQuorumMerge(t *testing.T, faultKey string) {
	proc, _, base := newR29SharedProc(t)
	fx := newR28Fixture(t, proc)
	ctx := context.Background()

	txn, tbl := fx.newTxnTbl(t)
	swapRCOperatorForTest(t, txn)

	origStats := fx.writeObj(t, tbl, []int64{1, 2, 3})
	require.Len(t, origStats, 1)
	origName := origStats[0].ObjectName().String()
	blk := objectio.NewBlockidWithObjectID(origStats[0].ObjectName().ObjectId(), 0)
	objID := *origStats[0].ObjectName().ObjectId()

	rawEntry := func(pk int64) Entry {
		return Entry{
			typ:          INSERT,
			accountId:    1,
			databaseId:   7,
			tableId:      42,
			databaseName: "db",
			tableName:    "tbl",
			bat:          newInsertBatchWithRowIDForTest(t, proc, []int64{pk}),
		}
	}

	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"the empty pre-SQL1 boundary must succeed")

	// SQL1: 29 same-table raw entries, then the flushed original — 29
	// merge-eligible entries, one short of the compaction quorum.
	for i := 0; i < 29; i++ {
		txn.appendWorkspaceEntryLocked(rawEntry(int64(101 + i)))
	}
	e0 := fx.flushedEntry(t, origStats[0].ObjectLocation().String(), origStats[0])
	txn.appendWorkspaceEntryLocked(e0)
	e0Bytes := blkEntryBytes(e0.bat)
	txn.EndStatement()

	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"SQL1's closing boundary must not fold: 29 eligible entries stay one short")
	require.Len(t, txn.writes, 30)
	require.Len(t, txn.transfer.statements, 2)

	// SQL2: the 30th same-table raw trips the quorum, and the original
	// is dirtied the healthy way; the ungated dump spills the fold.
	survivor := types.NewRowid(&blk, 1)
	txn.cnObjsSummary = map[types.Objectid]Summary{
		objID: {
			objBat:     e0.bat,
			accountId:  1,
			databaseId: 7,
			tableId:    42,
			tbName:     "tbl",
			dbName:     "db",
		},
	}
	txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{
		survivor: types.BuildTestRowid(30, 2),
	}
	txn.deletedBlocks = &deletedBlocks{
		offsets: map[types.Blockid][]int64{blk: {0}},
	}
	txn.appendWorkspaceEntryLocked(rawEntry(200))
	txn.writeWorkspaceThreshold = 0
	txn.engine.config.extraWorkspaceThreshold = 0
	txn.engine.config.writeWorkspaceThreshold = 0
	before := fsInventory(t, base)
	txn.EndStatement()

	// The armed failure boundary: the wave appends O' (@31), the quorum
	// folds the 30 same-table raws into slot 0 (husks @1..28 and @30,
	// compaction deferred by D129), the windowed dump spills the fold to
	// the tail (@32), and the injected RC fault aborts the boundary.
	disarm := armRCBoundaryFault(t, faultKey)
	txn.StartStatement()
	err := txn.IncrStatementID(ctx, false)
	require.Error(t, err,
		"the injected RC-maintenance failure must abort the whole boundary")
	require.Contains(t, err.Error(), "injection")
	disarm()
	require.Len(t, txn.writes, 33,
		"D129: mid-window layout [fold, 28 husks, original, husk, replacement, fold spill]")
	require.Equal(t, 30, txn.writes[0].bat.RowCount(),
		"the 30 same-table raws folded into the first slot")
	require.Nil(t, txn.writes[1].bat,
		"the fold husk kept its slot — no in-place compaction inside the window")
	require.Same(t, e0.bat, txn.writes[29].bat,
		"the original kept its batch at the exact acknowledged position")
	require.Nil(t, txn.writes[30].bat,
		"SQL2's raw folded — its husk kept its slot")
	require.NotNil(t, txn.writes[31].bat,
		"the published replacement stands in the boundary tail")
	require.NotEmpty(t, txn.writes[32].fileName,
		"the fold's spill-file entry appended at the tail")
	twoNew := twoNewDigestNames(t, before, fsInventory(t, base), 2)
	rep1Name, spill1Name := twoNew[0], twoNew[1]

	// Rollback: the cut names the pre-maintenance end=31, truncating the
	// replacement and the fold spill and reclaiming both files; the
	// retained fold husk compact runs only after the cut, so nothing the
	// cut named ever moves.
	txn.tableCache = new(sync.Map)
	require.NoError(t, txn.RollbackLastStatement(ctx))
	require.Len(t, txn.writes, 2,
		"D129: tail cut plus husk compact restores [fold, original]")
	require.Equal(t, 30, txn.writes[0].bat.RowCount(),
		"the acknowledged fold survives the rollback with every row")
	require.Same(t, e0.bat, txn.writes[1].bat)
	require.Equal(t, e0Bytes, blkEntryBytes(txn.writes[1].bat),
		"the original survives byte-identical at its slot")
	require.Equal(t, 1, txn.deletedBlocks.size(),
		"D124: the discarded finalization restores the wave-consumed ledger")
	require.Len(t, txn.transfer.statements, 2)
	txn.Lock()
	require.Contains(t, txn.pkMoveRowidOrigins, survivor)
	txn.Unlock()
	waitFileGone(t, base, rep1Name)
	waitFileGone(t, base, spill1Name)
	require.Equal(t, before, fsInventory(t, base),
		"file ownership returns to the pre-boundary state exactly")
	txn.EndStatement()

	// Retry: the healthy wave re-publishes, the fold re-spills, the
	// adoption releases the original and husks the dumped fold slot —
	// finalizing [O', foldA'].
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"the retried boundary adopts the finalized layout")
	require.Len(t, txn.writes, 2,
		"the published replacement plus the re-spilled fold")
	rep := txn.writes[0]
	require.NotNil(t, rep.bat)
	require.NotEmpty(t, rep.fileName)
	repCol, repArea := vector.MustVarlenaRawData(rep.bat.Vecs[1])
	require.Len(t, repCol, 1)
	repStats := objectio.ObjectStats(repCol[0].GetByteSlice(repArea))
	require.EqualValues(t, 2, repStats.Rows(),
		"exact committed rows: {2,3} — the original's survivors")
	spill := txn.writes[1]
	require.NotNil(t, spill.bat)
	require.NotEmpty(t, spill.fileName)
	sCol, sArea := vector.MustVarlenaRawData(spill.bat.Vecs[1])
	require.Len(t, sCol, 1)
	sStats := objectio.ObjectStats(sCol[0].GetByteSlice(sArea))
	require.EqualValues(t, 30, sStats.Rows(),
		"the retried dump re-spilled exactly the 30 folded rows once")
	repObj := *repStats.ObjectName().ObjectId()
	txn.Lock()
	require.Len(t, txn.pkMoveRowidOrigins, 1)
	for wsRid, originRid := range txn.pkMoveRowidOrigins {
		require.Equal(t, repObj, *wsRid.BorrowObjectID(),
			"the survivor's provenance re-minted onto the retried replacement")
		require.Equal(t, types.BuildTestRowid(30, 2), originRid)
	}
	txn.Unlock()
	require.Len(t, txn.transfer.statements, 3)
	waitFileGone(t, base, origName)
	after := fsInventory(t, base)
	require.Contains(t, after, repStats.ObjectName().String())
	require.Contains(t, after, sStats.ObjectName().String())
	require.Len(t, after, len(before)+1,
		"file ownership after retry: original reclaimed, replacement and fold spill added — nothing else")
	txn.EndStatement()

	// Trailing boundary: the finalized state rides the RC legs cleanly.
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false))
	txn.EndStatement()
}

// TestPkMoveRCSnapshotFailureSurvivesQuorumMerge pins r31 review F1 on
// the snapshot-refresh leg with the quorum-merge layout (D129).
func TestPkMoveRCSnapshotFailureSurvivesQuorumMerge(t *testing.T) {
	runRCBoundaryFailureWithQuorumMerge(t, objectio.FJ_CNIncrStmtRCSnapshotFailed)
}

// TestPkMoveRCTransferFailureSurvivesQuorumMerge pins r31 review F1 on
// the tombstone-transfer leg with the quorum-merge layout (D129).
func TestPkMoveRCTransferFailureSurvivesQuorumMerge(t *testing.T) {
	runRCBoundaryFailureWithQuorumMerge(t, objectio.FJ_CNIncrStmtRCTransferFailed)
}

// TestPkMoveOrphanGCAdmissionOverCapacityKeepsOwnership pins r31 review
// F2's retention bound (D130): names past the admission capacity get one
// immediate synchronous reclaim attempt by the registering caller, and
// only a still-failing name is retained past the bound — ownership never
// dies silently. The pre-D130 registry had no capacity at all, so it
// issued no attempt at the admission and retained everything quietly;
// the RED tree fails at the relief-attempt pin.
func TestPkMoveOrphanGCAdmissionOverCapacityKeepsOwnership(t *testing.T) {
	proc, inj, base := newR29SharedProc(t)
	fx := newR28Fixture(t, proc)

	txn, tbl := fx.newTxnTbl(t)

	savedCapacity := durableOrphanGC.capacity
	durableOrphanGC.capacity = 3
	t.Cleanup(func() { durableOrphanGC.capacity = savedCapacity })

	before := fsInventory(t, base)
	names := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		stats := fx.writeObj(t, tbl, []int64{int64(501 + i)})
		require.Len(t, stats, 1)
		names = append(names, stats[0].ObjectName().String())
	}

	// Three names fit the pinned capacity; N4 and N5 overflow and get the
	// synchronous relief attempt, which the armed knob fails — so all
	// five must still be owned.
	inj.setFailDeletes(1)
	registerDurableOrphanGC(txn.engine.fs, names...)

	recs := inj.deleteAttempts()
	require.Len(t, recs, 1,
		"D130: exactly the overflow pair got the synchronous admission-relief attempt")
	require.True(t, recs[0].injected)
	require.ElementsMatch(t, names[3:], recs[0].files,
		"the relief attempt named exactly the overflow pair")
	require.ElementsMatch(t, names, durableOrphanGCNames(txn.engine),
		"ownership: all five names retained — two past the bound, none dropped")
	for _, name := range names {
		require.Contains(t, fsInventory(t, base), name,
			"the still-failing orphan stays unpublished garbage on disk")
	}

	// Recovery: an ordinary retry sweep with the knob healed reclaims
	// every retained name and empties the registry.
	retryDurableOrphanGC(txn.engine)
	require.Eventually(t, func() bool {
		return len(durableOrphanGCNames(txn.engine)) == 0
	}, 10*time.Second, 5*time.Millisecond,
		"the healed sweep deregisters every confirmed deletion")
	for _, name := range names {
		waitFileGone(t, base, name)
	}
	require.Equal(t, before, fsInventory(t, base),
		"filesystem inventory returns to the pre-registration state exactly")
}

// TestPkMoveOrphanGCSweepBudgetBoundsRetryWork pins r31 review F2's
// retry-work bound (D130): one sweep submits at most sweepNameBudget
// names, oldest registration first, so a large retained inventory drains
// in bounded steps instead of one unbounded resubmission. The pre-D130
// sweep submitted the whole inventory in one batch; the RED tree fails
// at the first-batch pin.
func TestPkMoveOrphanGCSweepBudgetBoundsRetryWork(t *testing.T) {
	proc, inj, base := newR29SharedProc(t)
	fx := newR28Fixture(t, proc)

	txn, tbl := fx.newTxnTbl(t)

	savedBudget := durableOrphanGC.sweepNameBudget
	durableOrphanGC.sweepNameBudget = 3
	t.Cleanup(func() { durableOrphanGC.sweepNameBudget = savedBudget })

	before := fsInventory(t, base)
	names := make([]string, 0, 7)
	for i := 0; i < 7; i++ {
		stats := fx.writeObj(t, tbl, []int64{int64(601 + i)})
		require.Len(t, stats, 1)
		names = append(names, stats[0].ObjectName().String())
	}
	registerDurableOrphanGC(txn.engine.fs, names...)

	// No failures armed: the inventory drains 3 + 3 + 1, FIFO.
	wantBatches := [][]string{names[0:3], names[3:6], names[6:7]}
	for i, want := range wantBatches {
		retryDurableOrphanGC(txn.engine)
		require.Eventually(t, func() bool {
			return len(inj.deleteAttempts()) == i+1
		}, 10*time.Second, 5*time.Millisecond,
			"D130: sweep %d completes its bounded batch", i+1)
		recs := inj.deleteAttempts()
		require.Equal(t, want, recs[i].files,
			"sweep %d submitted exactly %d names, oldest registration first", i+1, len(want))
		require.False(t, recs[i].injected)
		for _, name := range want {
			waitFileGone(t, base, name)
		}
	}
	require.Eventually(t, func() bool {
		return len(durableOrphanGCNames(txn.engine)) == 0
	}, 10*time.Second, 5*time.Millisecond,
		"the bounded sweeps converge: nothing retained, nothing left behind")
	recs := inj.deleteAttempts()
	require.Len(t, recs, 3,
		"D130: no sweep ever resubmitted drained names — exactly three bounded batches total")
	require.Equal(t, before, fsInventory(t, base),
		"filesystem inventory returns to the pre-registration state exactly")
}

// TestPkMoveOrphanGCSweepCoalescesOverlappingTriggers pins r31 review
// F2's single-flight rule (D130): a trigger arriving while a sweep is in
// flight cannot submit a duplicate batch over the same inventory; it
// records pending work, and the completing sweep chains exactly one
// follow-up. The barrier holds the first sweep's delete open while the
// overlapping trigger and the late registration arrive. The pre-D130
// registry resubmitted the whole inventory per trigger; the RED tree
// fails at the still-one-attempt pin after the overlapping trigger.
func TestPkMoveOrphanGCSweepCoalescesOverlappingTriggers(t *testing.T) {
	proc, inj, base := newR29SharedProc(t)
	fx := newR28Fixture(t, proc)

	txn, tbl := fx.newTxnTbl(t)

	barrier := make(chan struct{})
	var closeOnce sync.Once
	inj.deleteBarrier = barrier
	t.Cleanup(func() {
		closeOnce.Do(func() { close(barrier) })
		inj.deleteBarrier = nil
	})

	before := fsInventory(t, base)
	names := make([]string, 0, 4)
	for i := 0; i < 4; i++ {
		stats := fx.writeObj(t, tbl, []int64{int64(701 + i)})
		require.Len(t, stats, 1)
		names = append(names, stats[0].ObjectName().String())
	}

	// Sweep 1 admits [R1..R3] and blocks inside the recorded delete.
	registerDurableOrphanGC(txn.engine.fs, names[:3]...)
	retryDurableOrphanGC(txn.engine)
	require.Eventually(t, func() bool {
		return len(inj.deleteAttempts()) == 1
	}, 10*time.Second, 5*time.Millisecond,
		"the first sweep reached its blocked delete")

	// The overlapping trigger must NOT submit a duplicate batch; the late
	// registration just joins the retained inventory.
	retryDurableOrphanGC(txn.engine)
	registerDurableOrphanGC(txn.engine.fs, names[3])
	require.Never(t, func() bool {
		return len(inj.deleteAttempts()) > 1
	}, 200*time.Millisecond, 10*time.Millisecond,
		"D130: the overlapping trigger coalesced — no duplicate submission while sweep 1 is in flight")

	// Release the barrier: sweep 1's delete succeeds, the pending flag
	// chains exactly one follow-up, and that follow-up reclaims R4.
	closeOnce.Do(func() { close(barrier) })
	require.Eventually(t, func() bool {
		return len(inj.deleteAttempts()) == 2
	}, 10*time.Second, 5*time.Millisecond,
		"the completed sweep chains exactly one follow-up for the pending work")
	recs := inj.deleteAttempts()
	require.Equal(t, names[:3], recs[0].files,
		"sweep 1 carried exactly the pre-barrier inventory")
	require.Equal(t, []string{names[3]}, recs[1].files,
		"the chained follow-up carried only the late registration")
	for _, name := range names {
		waitFileGone(t, base, name)
	}
	require.Eventually(t, func() bool {
		return len(durableOrphanGCNames(txn.engine)) == 0
	}, 10*time.Second, 5*time.Millisecond,
		"every name confirmed-deleted — the registry released the storage entry")
	require.Equal(t, before, fsInventory(t, base),
		"filesystem inventory returns to the pre-registration state exactly")
}

// TestPkMoveOrphanGCOwnershipSurvivesEngineClose pins r31 review F3's
// close handoff (D131): a failed deletion followed by Engine.Close must
// not strand the retained file behind a dead engine key. The closing
// engine runs one synchronous terminal pass (which this test fails by
// injection), ownership stays keyed by the storage — never the engine —
// and a replacement engine over the same storage drives the recovery
// sweep to confirmed deletion and registry release. The pre-D131
// registry keyed ownership by *Engine and Close ran no pass, so the
// recovery's wait times out: the RED tree fails at the close-pass pin
// and cannot reclaim through the replacement.
func TestPkMoveOrphanGCOwnershipSurvivesEngineClose(t *testing.T) {
	proc, inj, base := newR29SharedProc(t)
	fx := newR28Fixture(t, proc)

	txn, tbl := fx.newTxnTbl(t)

	before := fsInventory(t, base)
	stats := fx.writeObj(t, tbl, []int64{801})
	require.Len(t, stats, 1)
	orphanName := stats[0].ObjectName().String()

	registerDurableOrphanGC(txn.engine.fs, orphanName)
	require.Equal(t, []string{orphanName}, durableOrphanGCNames(txn.engine))

	// The terminal close pass takes its one failing attempt: the file
	// stays, owned under the storage key, with the engine gone.
	inj.setFailDeletes(1)
	txn.engine.Close()
	txn.engine.gcPool = nil // already released by Close; keep the fixture cleanup honest

	recs := inj.deleteAttempts()
	require.Len(t, recs, 1,
		"D131: the closing engine ran its synchronous terminal pass")
	require.True(t, recs[0].injected,
		"the armed failure consumed the close pass's attempt")
	require.Equal(t, []string{orphanName}, recs[0].files)
	require.Equal(t, []string{orphanName}, durableOrphanGCNames(txn.engine),
		"D131: ownership survived the engine's death — keyed by the storage, not the engine")
	require.Contains(t, fsInventory(t, base), orphanName,
		"the unconfirmed file is still unpublished garbage on disk")

	// A replacement engine over the same storage drives the recovery
	// sweep: confirmed deletion, registry release, exact inventory.
	replacement := &Engine{fs: txn.engine.fs}
	replacement.gcPool, _ = ants.NewPool(2)
	t.Cleanup(func() { replacement.gcPool.Release() })
	retryDurableOrphanGC(replacement)
	require.Eventually(t, func() bool {
		return len(inj.deleteAttempts()) == 2
	}, 10*time.Second, 5*time.Millisecond,
		"the replacement engine swept the dead engine's retained inventory")
	waitFileGone(t, base, orphanName)
	require.Eventually(t, func() bool {
		return len(durableOrphanGCNames(replacement)) == 0
	}, 10*time.Second, 5*time.Millisecond,
		"the confirmed deletion released the storage-keyed registry entry")
	recs = inj.deleteAttempts()
	require.False(t, recs[1].injected,
		"the knob was exhausted — the recovery sweep really deletes")
	require.Equal(t, before, fsInventory(t, base),
		"filesystem inventory returns to the pre-registration state exactly")
}

// pinOrphanGCBounds pins the registry's capacity knobs to small
// deterministic values for one test and restores them on cleanup.
func pinOrphanGCBounds(t *testing.T, capacity, hardCapacity, reserveReclaimRounds int) {
	t.Helper()
	savedCapacity := durableOrphanGC.capacity
	savedHard := durableOrphanGC.hardCapacity
	savedRounds := durableOrphanGC.reserveReclaimRounds
	durableOrphanGC.capacity = capacity
	durableOrphanGC.hardCapacity = hardCapacity
	durableOrphanGC.reserveReclaimRounds = reserveReclaimRounds
	t.Cleanup(func() {
		durableOrphanGC.capacity = savedCapacity
		durableOrphanGC.hardCapacity = savedHard
		durableOrphanGC.reserveReclaimRounds = savedRounds
	})
}

// orphanGCEntryState snapshots one storage's registry bookkeeping for
// assertions (callers hold no lock).
func orphanGCEntryState(fs fileservice.FileService) (alive bool, retained, inflight, reserved, pending int, sweepActive bool) {
	durableOrphanGC.mu.Lock()
	defer durableOrphanGC.mu.Unlock()
	ent := durableOrphanGC.entries[fs]
	if ent == nil {
		return false, 0, 0, 0, 0, false
	}
	return true, len(ent.set), len(ent.inflight), ent.reserved, ent.pendingAdmissions, ent.sweepActive
}

// TestPkMoveOrphanGCAdmissionSurvivesInventoryDrain pins r32 review F1
// (D132): an overflow admission whose synchronous reclaim I/O is still
// pending must hold its registry entry alive while a concurrent sweep
// drains the existing inventory — otherwise the sweep's completion
// releases the entry and the admission's post-failure re-registration
// dereferences a nil entry (a panic inside transaction cleanup). The
// per-name delete gate holds the admission's reclaim blocked until the
// drain has fully finished, making the pre-D132 race deterministic: on
// the pre-D132 shape the entry is gone by gate-open time and the
// admission panics in its re-registration.
func TestPkMoveOrphanGCAdmissionSurvivesInventoryDrain(t *testing.T) {
	proc, inj, base := newR29SharedProc(t)
	fx := newR28Fixture(t, proc)

	txn, tbl := fx.newTxnTbl(t)

	savedCapacity := durableOrphanGC.capacity
	durableOrphanGC.capacity = 2
	t.Cleanup(func() { durableOrphanGC.capacity = savedCapacity })

	before := fsInventory(t, base)
	names := make([]string, 0, 4)
	for i := 0; i < 4; i++ {
		stats := fx.writeObj(t, tbl, []int64{int64(901 + i)})
		require.Len(t, stats, 1)
		names = append(names, stats[0].ObjectName().String())
	}

	// Gate the overflow pair's reclaim: the admission blocks inside its
	// Delete until this test opens the gate, then fails deterministically.
	gate := make(chan struct{})
	var gateOnce sync.Once
	inj.setDeleteGate(gate, map[string]struct{}{
		names[2]: {}, names[3]: {},
	}, context.DeadlineExceeded)
	disarmGate := func() {
		gateOnce.Do(func() { close(gate) })
		// clear under the fixture lock so a straggler sweep worker synchronizes
		inj.setDeleteGate(nil, nil, nil)
	}
	t.Cleanup(disarmGate) // also called explicitly at the recovery below

	// Fill the pinned capacity with the pre-existing inventory.
	registerDurableOrphanGC(txn.engine.fs, names[:2]...)

	// The overflow admission runs concurrently; the pin (D132) lands
	// before its reclaim I/O starts.
	admDone := make(chan any, 1)
	go func() {
		defer func() { admDone <- recover() }()
		registerDurableOrphanGC(txn.engine.fs, names[2:]...)
	}()
	require.Eventually(t, func() bool {
		for _, rec := range inj.deleteAttempts() {
			for _, f := range rec.files {
				if f == names[2] {
					return true
				}
			}
		}
		return false
	}, 10*time.Second, 5*time.Millisecond,
		"the overflow admission reached its gated reclaim attempt")

	// While the admission is mid-I/O, an ordinary sweep drains the whole
	// pre-existing inventory: on the pre-D132 shape this completion
	// dropped the entry under the pending admission.
	retryDurableOrphanGC(txn.engine)
	waitFileGone(t, base, names[0])
	waitFileGone(t, base, names[1])
	require.Eventually(t, func() bool {
		return len(durableOrphanGCNames(txn.engine)) == 0
	}, 10*time.Second, 5*time.Millisecond,
		"the sweep drained the existing inventory while the admission was pending")

	// D132: the pending admission pinned its entry across the drain...
	alive, retained, _, _, pending, _ := orphanGCEntryState(txn.engine.fs)
	require.True(t, alive,
		"D132: a pending admission pins its registry entry across the drain")
	require.Equal(t, 0, retained, "the drain really emptied the inventory")
	require.Equal(t, 1, pending,
		"D132: the admission's pin is visible until its I/O completes")

	// ... so its failed reclaim re-registers into a live entry instead of
	// dereferencing nil.
	gateOnce.Do(func() { close(gate) })
	require.Nil(t, <-admDone,
		"the overflow admission completed without panicking")
	require.ElementsMatch(t, names[2:], durableOrphanGCNames(txn.engine),
		"ownership: both overflow names retained after the failed reclaim")
	for _, name := range names[2:] {
		require.Contains(t, fsInventory(t, base), name,
			"the still-failing orphan stays unpublished garbage on disk")
	}

	// Recovery: heal the store; one retry sweep reclaims the retained
	// pair and releases the storage entry.
	// clear the armed gate under the fixture lock; the sweep below is
	// fail-injection-free and reclaims the pair deterministically
	disarmGate()
	retryDurableOrphanGC(txn.engine)
	require.Eventually(t, func() bool {
		return len(durableOrphanGCNames(txn.engine)) == 0
	}, 10*time.Second, 5*time.Millisecond,
		"the healed sweep deregisters every confirmed deletion")
	for _, name := range names[2:] {
		waitFileGone(t, base, name)
	}
	alive, _, _, _, _, _ = orphanGCEntryState(txn.engine.fs)
	require.False(t, alive, "the empty shell was released")
	require.Equal(t, before, fsInventory(t, base),
		"filesystem inventory returns to the pre-registration state exactly")
}

// TestPkMoveOrphanGCHardBoundBackpressureProof pins r32 review F2
// (D133) at the registry contract: while storage keeps failing, every
// reservation past the hard ceiling runs only the bounded reclaim
// rounds and then fails the producer with the stable exhaustion error —
// retained+inflight+reserved never grows per attempt, so sustained
// failure backpressures production instead of growing retained state
// indefinitely. Recovery then proves ownership was really preserved:
// healed reclaim rounds delete and deregister the whole inventory,
// oldest registration first, before booking the new reservation.
func TestPkMoveOrphanGCHardBoundBackpressureProof(t *testing.T) {
	proc, inj, base := newR29SharedProc(t)
	fx := newR28Fixture(t, proc)

	txn, tbl := fx.newTxnTbl(t)

	// Soft capacity 2, hard ceiling 4, two reclaim rounds per attempt.
	pinOrphanGCBounds(t, 2, 4, 2)

	before := fsInventory(t, base)
	names := make([]string, 0, 4)
	for i := 0; i < 4; i++ {
		stats := fx.writeObj(t, tbl, []int64{int64(911 + i)})
		require.Len(t, stats, 1)
		names = append(names, stats[0].ObjectName().String())
	}

	// The soft capacity admits two retained names; one reservation books
	// the third slot of the pinned hard ceiling.
	registerDurableOrphanGC(txn.engine.fs, names[:2]...)
	require.NoError(t, reserveDurableOrphanOwnership(txn.engine.fs, 1),
		"headroom below the hard ceiling books immediately")

	// Sustained storage failure: every Delete call fails until healed.
	inj.setFailDeletes(1 << 30)
	t.Cleanup(func() { inj.setFailDeletes(0) })

	// D133's backpressure: reserving past the ceiling under sustained
	// failure runs exactly the bounded reclaim rounds, confirms no
	// headroom, and fails the producer — it never books.
	err := reserveDurableOrphanOwnership(txn.engine.fs, 2)
	require.Error(t, err,
		"D133: exhaustion applies backpressure to the producer")
	require.Contains(t, err.Error(), "durable orphan ownership exhausted",
		"the stable exhaustion error names the contract")
	recs := inj.deleteAttempts()
	require.Len(t, recs, 2,
		"exactly the two bounded reclaim rounds attempted storage — no unbounded loop")
	for _, rec := range recs {
		require.True(t, rec.injected)
		require.Equal(t, names[:2], rec.files,
			"each reclaim round selected the oldest retained batch")
	}

	// Sustained failure cannot grow retained state indefinitely: the
	// overflow admissions retain past the soft bound because ownership
	// never dies (D130) — and their two names convert the outstanding
	// reservation name-for-name (D133's anonymous accounting)...
	registerDurableOrphanGC(txn.engine.fs, names[2:]...)
	alive, retained, inflight, reserved, _, _ := orphanGCEntryState(txn.engine.fs)
	require.True(t, alive)
	require.Equal(t, 4, retained)
	require.Equal(t, 0, inflight)
	require.Equal(t, 0, reserved,
		"D133: the overflow pair converted the booked reservation 1:1 — used stays at the ceiling")
	// ... but every further reservation past the ceiling keeps failing
	// and never books: the registry's total is pinned at its ceiling
	// shape no matter how long storage keeps failing.
	attempts := len(inj.deleteAttempts())
	for i := 0; i < 3; i++ {
		require.Error(t, reserveDurableOrphanOwnership(txn.engine.fs, 1),
			"every reservation past the ceiling fails while storage fails")
	}
	_, retained, _, reserved, _, _ = orphanGCEntryState(txn.engine.fs)
	require.Equal(t, 4, retained, "no per-attempt retained growth")
	require.Equal(t, 0, reserved, "failed reservations never booked")
	require.Equal(t, attempts+6, len(inj.deleteAttempts()),
		"each failed reservation ran exactly its two bounded reclaim rounds")
	for _, name := range names {
		require.Contains(t, fsInventory(t, base), name,
			"every owned file is preserved on disk through the failing phase")
	}

	// Recovery: one healed reclaim round deletes and deregisters the
	// whole inventory — oldest registration first — then books.
	inj.setFailDeletes(0)
	require.NoError(t, reserveDurableOrphanOwnership(txn.engine.fs, 2),
		"healed reclaim rounds free headroom for the reservation")
	recs = inj.deleteAttempts()
	require.Equal(t, names, recs[len(recs)-1].files,
		"the successful reclaim round carried the whole inventory, oldest first")
	require.False(t, recs[len(recs)-1].injected,
		"the knob was healed — the recovery round really deletes")
	for _, name := range names {
		waitFileGone(t, base, name)
	}
	alive, retained, _, reserved, _, _ = orphanGCEntryState(txn.engine.fs)
	require.True(t, alive)
	require.Equal(t, 0, retained,
		"recovery reclaimed every owned file")
	require.Equal(t, 2, reserved,
		"the healed reservation booked exactly its request after the reclaim")
	releaseDurableOrphanReservation(txn.engine.fs, 2)
	alive, _, _, _, _, _ = orphanGCEntryState(txn.engine.fs)
	require.False(t, alive, "the empty shell was released")
	require.Equal(t, before, fsInventory(t, base),
		"filesystem inventory returns to the pre-registration state exactly")
}

// TestPkMoveBoundaryFailsBeforePersistWhenOrphanOwnershipExhausted pins
// r32 review F2 (D133) at the producing statement: when the registry is
// saturated and storage keeps failing, the compaction wave's pre-flight
// reservation fails the boundary BEFORE any replacement file is
// persisted — sustained failure stops production rather than growing
// the orphan inventory. The failed statement rolls back to the
// pre-boundary state exactly, and the healed retry frees headroom,
// compacts, and commits the exact rows.
func TestPkMoveBoundaryFailsBeforePersistWhenOrphanOwnershipExhausted(t *testing.T) {
	proc, inj, base := newR29SharedProc(t)
	fx := newR28Fixture(t, proc)
	ctx := context.Background()

	txn, tbl := fx.newTxnTbl(t)
	swapRCOperatorForTest(t, txn)

	// Soft capacity 1, hard ceiling 2, two reclaim rounds per attempt.
	pinOrphanGCBounds(t, 1, 2, 2)

	// SQL1: one flushed entry carrying the three-row original object.
	origStats := fx.writeObj(t, tbl, []int64{1, 2, 3})
	require.Len(t, origStats, 1)
	origName := origStats[0].ObjectName().String()
	e0 := fx.flushedEntry(t, origStats[0].ObjectLocation().String(), origStats[0])
	objID := *origStats[0].ObjectName().ObjectId()
	blk := objectio.NewBlockidWithObjectID(origStats[0].ObjectName().ObjectId(), 0)
	txn.cnObjsSummary = map[types.Objectid]Summary{
		objID: {
			objBat:     e0.bat,
			accountId:  1,
			databaseId: 7,
			tableId:    42,
			tbName:     "tbl",
			dbName:     "db",
		},
	}

	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"the empty pre-SQL1 RC boundary must succeed")
	txn.appendWorkspaceEntryLocked(e0)
	e0Bytes := blkEntryBytes(txn.writes[0].bat)
	txn.EndStatement()

	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"SQL1's closing RC boundary must succeed")

	// SQL2 dirties the original (survivor provenance-named), and one
	// garbage object pre-occupies the registry: retained 1 + a booked
	// reservation 1 fills the pinned hard ceiling.
	survivor := types.NewRowid(&blk, 1)
	txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{
		survivor: types.BuildTestRowid(30, 2),
	}
	txn.deletedBlocks = &deletedBlocks{
		offsets: map[types.Blockid][]int64{blk: {0}},
	}
	garbageStats := fx.writeObj(t, tbl, []int64{77})
	require.Len(t, garbageStats, 1)
	garbageName := garbageStats[0].ObjectName().String()
	before := fsInventory(t, base)
	txn.EndStatement()

	registerDurableOrphanGC(txn.engine.fs, garbageName)
	require.NoError(t, reserveDurableOrphanOwnership(txn.engine.fs, 1),
		"the saturation reservation books the ceiling's last slot")

	// Sustained storage failure: the boundary's pre-flight reservation
	// cannot free headroom and must fail the statement BEFORE the wave
	// persists any replacement.
	inj.setFailDeletes(1 << 30)
	t.Cleanup(func() { inj.setFailDeletes(0) })
	txn.StartStatement()
	err := txn.IncrStatementID(ctx, false)
	require.Error(t, err,
		"D133: the saturated registry backpressures the producing statement")
	require.Contains(t, err.Error(), "durable orphan ownership exhausted",
		"the stable exhaustion error surfaces at the statement")
	require.Equal(t, before, fsInventory(t, base),
		"D133: the boundary failed BEFORE persist — no replacement file exists")
	require.Len(t, txn.writes, 1,
		"the workspace is untouched by the pre-flight failure")

	// The frontend-equivalent rollback restores the staged statement
	// exactly: the original stays byte-stable, the deletion ledger
	// returns for the retry, and the registry kept its saturation shape.
	txn.tableCache = new(sync.Map)
	require.NoError(t, txn.RollbackLastStatement(ctx))
	require.Len(t, txn.writes, 1)
	require.Same(t, e0.bat, txn.writes[0].bat)
	require.Equal(t, e0Bytes, blkEntryBytes(txn.writes[0].bat))
	require.Equal(t, 1, txn.deletedBlocks.size(),
		"the deletion ledger survived for the retry")
	alive, retained, _, reserved, _, _ := orphanGCEntryState(txn.engine.fs)
	require.True(t, alive)
	require.Equal(t, 1, retained)
	require.Equal(t, 1, reserved,
		"the failed statement booked nothing — no growth per attempt")
	txn.EndStatement()

	// Heal and retry: the boundary's reservation reclaims the garbage
	// first, the wave compacts, and only the adoption releases the
	// original.
	inj.setFailDeletes(0)
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"the retried boundary reclaims headroom and compacts")
	require.Len(t, txn.writes, 2,
		"original (released) plus the published replacement")
	require.Nil(t, txn.writes[0].bat,
		"only the adopted boundary releases the original batch")
	rep := txn.writes[1]
	require.NotNil(t, rep.bat)
	repCol, repArea := vector.MustVarlenaRawData(rep.bat.Vecs[1])
	require.Len(t, repCol, 1)
	repStats := objectio.ObjectStats(repCol[0].GetByteSlice(repArea))
	require.EqualValues(t, 2, repStats.Rows(),
		"the exact committed row set: the original's two survivors")
	waitFileGone(t, base, origName)
	waitFileGone(t, base, garbageName)
	txn.EndStatement()

	// The wave's reservation released on success; only the test's
	// saturation reservation remains — releasing it empties the registry.
	releaseDurableOrphanReservation(txn.engine.fs, 1)
	alive, _, _, _, _, _ = orphanGCEntryState(txn.engine.fs)
	require.False(t, alive, "every reservation reconciled — the registry released the storage")
	final := fsInventory(t, base)
	require.NotContains(t, final, origName)
	require.NotContains(t, final, garbageName)
	require.Contains(t, final, repStats.ObjectName().String())
	require.Len(t, final, len(before)-1,
		"original and garbage reclaimed, replacement added — nothing else")
}

// TestPkMoveOrphanGCCloseSkipsActiveSweep pins r32 review F3's
// coordination clause (D134): the engine-close terminal pass runs
// around an already-active sweep owned by ANOTHER live engine over the
// same storage — it never waits for that sweep, never deletes the
// sweep's in-flight names, and never touches that engine's flight
// state. Leftovers stay owned for the sweep's owner. On the pre-D134
// shape the close pass re-snapshotted the whole inventory and blocked
// inside the gated in-flight names, holding Engine.Close open behind
// the peer's sweep.
func TestPkMoveOrphanGCCloseSkipsActiveSweep(t *testing.T) {
	proc, inj, base := newR29SharedProc(t)
	fx := newR28Fixture(t, proc)

	txn, tbl := fx.newTxnTbl(t)

	before := fsInventory(t, base)
	names := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		stats := fx.writeObj(t, tbl, []int64{int64(921 + i)})
		require.Len(t, stats, 1)
		names = append(names, stats[0].ObjectName().String())
	}

	// Gate the peer sweep's names only: its Delete blocks after
	// recording until the gate opens, then really deletes.
	gate := make(chan struct{})
	var gateOnce sync.Once
	inj.setDeleteGate(gate,
		map[string]struct{}{names[0]: {}, names[1]: {}}, nil)
	t.Cleanup(func() {
		gateOnce.Do(func() { close(gate) })
		// clear the gate under the fixture lock (see setDeleteGate)
		inj.setDeleteGate(nil, nil, nil)
	})

	// The peer engine over the same storage starts its sweep; the gated
	// delete holds it in flight.
	registerDurableOrphanGC(txn.engine.fs, names[:2]...)
	peer := &Engine{fs: txn.engine.fs}
	peer.gcPool, _ = ants.NewPool(2)
	t.Cleanup(func() { peer.gcPool.Release() })
	retryDurableOrphanGC(peer)
	require.Eventually(t, func() bool {
		return len(inj.deleteAttempts()) == 1
	}, 10*time.Second, 5*time.Millisecond,
		"the peer sweep reached its gated delete")

	// A late registration lands behind the active sweep (not in-flight).
	registerDurableOrphanGC(txn.engine.fs, names[2])

	// This engine closes DURING the peer's flight: the terminal pass
	// must skip the in-flight pair, reclaim the late name, and return —
	// never waiting on the peer's gate.
	closeDone := make(chan struct{})
	go func() {
		defer close(closeDone)
		_ = txn.engine.Close()
	}()
	require.Eventually(t, func() bool {
		select {
		case <-closeDone:
			return true
		default:
			return false
		}
	}, 10*time.Second, 5*time.Millisecond,
		"D134: the closing engine never waits for another engine's active sweep")
	txn.engine.gcPool = nil // already released by Close; keep the fixture cleanup honest

	recs := inj.deleteAttempts()
	require.Len(t, recs, 2,
		"the peer sweep and the close pass made exactly one attempt each")
	require.Equal(t, names[:2], recs[0].files,
		"the peer sweep carried exactly the pre-close inventory")
	require.Equal(t, []string{names[2]},
		recs[1].files,
		"D134: the close pass deleted around the in-flight names — only the late registration")
	require.False(t, recs[1].injected)
	waitFileGone(t, base, names[2])
	_, retained, inflight, _, _, sweepActive := orphanGCEntryState(txn.engine.fs)
	require.Equal(t, 2, retained, "the in-flight pair stays owned")
	require.Equal(t, 2, inflight,
		"D134: the peer sweep's in-flight bookkeeping was never touched")
	require.True(t, sweepActive,
		"D134: the peer's flight state survived the close pass")
	require.ElementsMatch(t, names[:2], durableOrphanGCNames(txn.engine))
	for _, name := range names[:2] {
		require.Contains(t, fsInventory(t, base), name,
			"the peer-owned names were never deleted by the close pass")
	}

	// The peer's gate opens: its own completion reclaims the pair and
	// releases the storage entry — ownership handoff complete.
	gateOnce.Do(func() { close(gate) })
	waitFileGone(t, base, names[0])
	waitFileGone(t, base, names[1])
	require.Eventually(t, func() bool {
		return len(durableOrphanGCNames(txn.engine)) == 0
	}, 10*time.Second, 5*time.Millisecond,
		"the peer sweep's completion deregisters every confirmed deletion")
	alive, _, _, _, _, _ := orphanGCEntryState(txn.engine.fs)
	require.False(t, alive, "the empty shell was released")
	require.Equal(t, before, fsInventory(t, base),
		"filesystem inventory returns to the pre-registration state exactly")
}

// TestPkMoveOrphanGCCloseLeavesLateRegistrationsOwned pins r32 review
// F3's boundedness clause (D134): the engine-close terminal pass is ONE
// flight over a FINITE snapshot — names another live engine registers
// DURING the pass stay owned for that engine's later sweeps instead of
// being re-snapshotted into the close pass's own work. On the pre-D134
// loop shape, every re-snapshot absorbed the late registrations, so
// steady registrations from a peer held Engine.Close open indefinitely;
// this test's bounded proxy asserts the close pass deletes exactly its
// snapshot and never touches the late names.
func TestPkMoveOrphanGCCloseLeavesLateRegistrationsOwned(t *testing.T) {
	proc, inj, base := newR29SharedProc(t)
	fx := newR28Fixture(t, proc)

	txn, tbl := fx.newTxnTbl(t)

	before := fsInventory(t, base)
	closeNames := make([]string, 0, 4)
	for i := 0; i < 4; i++ {
		stats := fx.writeObj(t, tbl, []int64{int64(931 + i)})
		require.Len(t, stats, 1)
		closeNames = append(closeNames, stats[0].ObjectName().String())
	}
	lateNames := make([]string, 0, 6)
	for i := 0; i < 6; i++ {
		stats := fx.writeObj(t, tbl, []int64{int64(941 + i)})
		require.Len(t, stats, 1)
		lateNames = append(lateNames, stats[0].ObjectName().String())
	}

	// Gate the close snapshot's names: the pass blocks inside its own
	// delete after recording, letting the late registrations land
	// deterministically mid-pass.
	gate := make(chan struct{})
	var gateOnce sync.Once
	inj.setDeleteGate(
		gate,
		map[string]struct{}{
			closeNames[0]: {}, closeNames[1]: {}, closeNames[2]: {}, closeNames[3]: {},
		}, nil)
	t.Cleanup(func() {
		gateOnce.Do(func() { close(gate) })
		inj.setDeleteGate(nil, nil, nil)
	})

	registerDurableOrphanGC(txn.engine.fs, closeNames...)
	closeDone := make(chan struct{})
	go func() {
		defer close(closeDone)
		_ = txn.engine.Close()
	}()
	require.Eventually(t, func() bool {
		return len(inj.deleteAttempts()) == 1
	}, 10*time.Second, 5*time.Millisecond,
		"the close pass reached its gated delete over the finite snapshot")
	require.Equal(t, closeNames, inj.deleteAttempts()[0].files,
		"the snapshot is the FIFO registration order, one bounded batch")

	// A peer engine registers new orphans while the close pass is
	// definitively mid-flight (blocked inside its own delete).
	peer := &Engine{fs: txn.engine.fs}
	peer.gcPool, _ = ants.NewPool(2)
	t.Cleanup(func() { peer.gcPool.Release() })
	registerDurableOrphanGC(peer.fs, lateNames...)

	// Open the gate: the ONE bounded flight finishes and Engine.Close
	// returns — the late names were never part of its snapshot.
	gateOnce.Do(func() { close(gate) })
	require.Eventually(t, func() bool {
		select {
		case <-closeDone:
			return true
		default:
			return false
		}
	}, 10*time.Second, 5*time.Millisecond,
		"D134: registrations during the pass cannot hold Engine.Close open")
	txn.engine.gcPool = nil // already released by Close; keep the fixture cleanup honest

	require.Len(t, inj.deleteAttempts(), 1,
		"D134: the close pass deleted exactly its finite snapshot — no re-snapshot loop")
	require.False(t, inj.deleteAttempts()[0].injected)
	require.ElementsMatch(t, lateNames, durableOrphanGCNames(txn.engine),
		"D134: late registrations stay owned under the storage key for the live engine")
	for _, name := range lateNames {
		require.Contains(t, fsInventory(t, base), name,
			"the close pass never touched a name outside its snapshot")
	}
	for _, name := range closeNames {
		waitFileGone(t, base, name)
	}

	// The live peer engine sweeps the leftovers to confirmed deletion
	// and the storage entry releases.
	retryDurableOrphanGC(peer)
	require.Eventually(t, func() bool {
		return len(inj.deleteAttempts()) == 2
	}, 10*time.Second, 5*time.Millisecond,
		"the live engine's sweep carried only the late registrations")
	require.Equal(t, lateNames, inj.deleteAttempts()[1].files,
		"the peer sweep carried exactly the names the close pass left")
	for _, name := range lateNames {
		waitFileGone(t, base, name)
	}
	require.Eventually(t, func() bool {
		return len(durableOrphanGCNames(peer)) == 0
	}, 10*time.Second, 5*time.Millisecond,
		"every confirmed deletion deregistered")
	alive, _, _, _, _, _ := orphanGCEntryState(txn.engine.fs)
	require.False(t, alive, "the empty shell was released")
	require.Equal(t, before, fsInventory(t, base),
		"filesystem inventory returns to the pre-registration state exactly")
}

// setFailDeletes arms the fixture's transient-delete counter under the
// fixture lock: async sweep workers read failDeletes inside Delete
// holding fs.mu, so an unsynchronized test-side write races with them
// (e26 race gate: the armed knob reset in t.Cleanup against a
// piggyback sweep still in flight).
func (fs *boundaryFailFS) setFailDeletes(n int) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.failDeletes = n
}

// setDeleteGate installs (or clears, when gate == nil) the per-name
// delete gate under the fixture lock for the same reason as
// setFailDeletes.
func (fs *boundaryFailFS) setDeleteGate(gate chan struct{}, names map[string]struct{}, gateErr error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.deleteGate = gate
	fs.deleteGateNames = names
	fs.deleteGateErr = gateErr
}

// pinOrphanGCReliefBatch pins the synchronous relief chunk size for the
// r33 mixed-result proof, restoring the production default afterwards.
func pinOrphanGCReliefBatch(t *testing.T, n int) {
	t.Helper()
	saved := durableOrphanGC.reliefBatchSize
	durableOrphanGC.reliefBatchSize = n
	t.Cleanup(func() { durableOrphanGC.reliefBatchSize = saved })
}

// TestPkMoveOrphanGCOverflowSuccessSettlesTransferredReservations pins
// r33 review F1's settlement contract (D135) on the all-success
// terminal: an overflow name whose direct synchronous reclaim CONFIRMS
// settles its transferred reservation exactly once, just like a name
// retention adopts. r32's overflow coverage exercised only failed
// direct reclaim; on the RED semantics the confirmed name settles
// nothing, leaving one phantom reservation that pins the drained
// registry entry and taxes every later reservation.
func TestPkMoveOrphanGCOverflowSuccessSettlesTransferredReservations(t *testing.T) {
	proc, inj, base := newR29SharedProc(t)
	fx := newR28Fixture(t, proc)
	txn, tbl := fx.newTxnTbl(t)

	// Soft capacity 1, hard ceiling 2: the first name retains under the
	// bound, the second overflows and takes the direct relief attempt —
	// which succeeds with nothing armed.
	pinOrphanGCBounds(t, 1, 2, 1)

	before := fsInventory(t, base)
	names := make([]string, 0, 2)
	for i := 0; i < 2; i++ {
		stats := fx.writeObj(t, tbl, []int64{int64(901 + i)})
		require.Len(t, stats, 1)
		names = append(names, stats[0].ObjectName().String())
	}

	// Two booked reservations, exactly the in-contract discipline: the
	// retained name converts one at admission and the confirmed overflow
	// name settles the other (D135).
	require.NoError(t, reserveDurableOrphanOwnership(txn.engine.fs, 2),
		"both reservations book under the empty hard ceiling")
	registerDurableOrphanGC(txn.engine.fs, names...)

	recs := inj.deleteAttempts()
	require.Len(t, recs, 1, "exactly the overflow name took the relief attempt")
	require.Equal(t, []string{names[1]}, recs[0].files)
	require.False(t, recs[0].injected, "the relief really deleted")
	waitFileGone(t, base, names[1])

	alive, retained, inflight, reserved, _, _ := orphanGCEntryState(txn.engine.fs)
	require.True(t, alive, "the retained name keeps the entry")
	require.Equal(t, 1, retained)
	require.Equal(t, 0, inflight)
	require.Equal(t, 0, reserved,
		"D135: the confirmed overflow deletion settled its reservation exactly once")

	// With no phantom slot the healed sweep deregisters the retained
	// name and maybeReleaseEntryLocked frees the whole shell; on the
	// RED semantics the phantom reservation keeps the entry alive
	// forever.
	retryDurableOrphanGC(txn.engine)
	require.Eventually(t, func() bool {
		a, _, _, _, _, _ := orphanGCEntryState(txn.engine.fs)
		return !a
	}, 10*time.Second, 5*time.Millisecond,
		"D135: with every reservation settled the drained entry released its shell")
	waitFileGone(t, base, names[0])

	// Admission capacity is restored for later producers: the same
	// reservation that booked at the start books again on the settled
	// registry — the RED tree exhausts here with an empty inventory.
	require.NoError(t, reserveDurableOrphanOwnership(txn.engine.fs, 2),
		"D135: the settled registry admits new production at full headroom")
	releaseDurableOrphanReservation(txn.engine.fs, 2)
	alive, _, _, _, _, _ = orphanGCEntryState(txn.engine.fs)
	require.False(t, alive, "the returned reservation empties the registry again")
	require.Equal(t, before, fsInventory(t, base),
		"filesystem inventory returns to the pre-registration state exactly")
}

// TestPkMoveOrphanGCOverflowMixedResultSettlesExactlyOnce pins r33
// review F1's settlement contract (D135) on the mixed terminal: one
// relief chunk fails injected while the other chunks confirm, so the
// three booked reservations reconcile as one retained-adoption
// conversion plus two confirmed-deletion settles — exactly zero left
// over, never the RED residual that counts only the re-registered
// failure.
func TestPkMoveOrphanGCOverflowMixedResultSettlesExactlyOnce(t *testing.T) {
	proc, inj, base := newR29SharedProc(t)
	fx := newR28Fixture(t, proc)
	txn, tbl := fx.newTxnTbl(t)

	// Soft capacity 1 admits the first name; the three overflow names
	// take one relief attempt EACH under the pinned chunk size, so the
	// mixed result is deterministic.
	pinOrphanGCBounds(t, 1, 4, 1)
	pinOrphanGCReliefBatch(t, 1)

	before := fsInventory(t, base)
	names := make([]string, 0, 4)
	for i := 0; i < 4; i++ {
		stats := fx.writeObj(t, tbl, []int64{int64(911 + i)})
		require.Len(t, stats, 1)
		names = append(names, stats[0].ObjectName().String())
	}

	// Three reservations for the three overflow-bound names.
	require.NoError(t, reserveDurableOrphanOwnership(txn.engine.fs, 3))

	// The first relief chunk's delete fails injected; chunks two and
	// three confirm.
	inj.setFailDeletes(1)
	t.Cleanup(func() { inj.setFailDeletes(0) })
	registerDurableOrphanGC(txn.engine.fs, names...)

	recs := inj.deleteAttempts()
	require.Len(t, recs, 3,
		"the pinned one-bound relief issued exactly three single-name attempts")
	require.Equal(t, []string{names[1]}, recs[0].files)
	require.True(t, recs[0].injected, "the first attempt failed injected")
	require.False(t, recs[1].injected)
	require.False(t, recs[2].injected)

	alive, retained, _, reserved, _, _ := orphanGCEntryState(txn.engine.fs)
	require.True(t, alive)
	require.Equal(t, 2, retained,
		"the capacity name plus the failed relief name stay owned")
	require.Equal(t, 0, reserved,
		"D135: two confirmed settles plus one retained conversion reconcile the three bookings exactly")
	waitFileGone(t, base, names[2])
	waitFileGone(t, base, names[3])

	// Healed, the retained pair drains and the shell releases — no
	// phantom reservation holds it (the RED semantics leave one booked
	// for the two confirmed names).
	retryDurableOrphanGC(txn.engine)
	require.Eventually(t, func() bool {
		a, _, _, _, _, _ := orphanGCEntryState(txn.engine.fs)
		return !a
	}, 10*time.Second, 5*time.Millisecond,
		"D135: the mixed-result settlement left nothing pinning the entry")
	for _, name := range names {
		waitFileGone(t, base, name)
	}

	require.NoError(t, reserveDurableOrphanOwnership(txn.engine.fs, 4),
		"D135: full hard-ceiling headroom restored after exact settlement")
	releaseDurableOrphanReservation(txn.engine.fs, 4)
	require.Equal(t, before, fsInventory(t, base),
		"filesystem inventory returns to the pre-registration state exactly")
}

// boundaryReadFailFS extends the r29 wrapper with the r34 read gate
// (review F2's post-Sync shape): the dump's shadow read-back runs
// through the shared filesystem after Sync persisted the output, and a
// storage error there is the terminal the r29 dump-failure test — which
// fails the shadow CAPTURE before any persistence — cannot express.
// failReads fails that many next reads with readErr (default
// context.DeadlineExceeded); passReads lets that many through untouched
// first. Reads and ReadCache share one counter so the gate fires on
// whichever entry point the resolve chooses, never on both.
type boundaryReadFailFS struct {
	*boundaryFailFS

	mu        sync.Mutex
	passReads int
	failReads int
	readErr   error
	reads     []string
}

func (fs *boundaryReadFailFS) setReadGate(pass, fail int, err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.passReads = pass
	fs.failReads = fail
	fs.readErr = err
}

func (fs *boundaryReadFailFS) gatedReadErr(name string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.reads = append(fs.reads, name)
	if fs.passReads > 0 {
		fs.passReads--
		return nil
	}
	if fs.failReads > 0 {
		fs.failReads--
		if fs.readErr != nil {
			return fs.readErr
		}
		return context.DeadlineExceeded
	}
	return nil
}

func (fs *boundaryReadFailFS) Read(ctx context.Context, vector *fileservice.IOVector) error {
	if err := fs.gatedReadErr(vector.FilePath); err != nil {
		return err
	}
	return fs.boundaryFailFS.FileService.Read(ctx, vector)
}

func (fs *boundaryReadFailFS) ReadCache(ctx context.Context, vector *fileservice.IOVector) error {
	if err := fs.gatedReadErr(vector.FilePath); err != nil {
		return err
	}
	return fs.boundaryFailFS.FileService.ReadCache(ctx, vector)
}

// newR34SharedProc mirrors newR29SharedProc with the read-gated wrapper.
func newR34SharedProc(t *testing.T) (*process.Process, *boundaryReadFailFS, fileservice.FileService) {
	t.Helper()
	ctx := context.Background()
	local, err := fileservice.NewLocalFS(
		ctx, defines.LocalFileServiceName, t.TempDir(), fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	base, err := fileservice.NewLocalFS(
		ctx, defines.SharedFileServiceName, t.TempDir(), fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	etl, err := fileservice.NewLocalETLFS(defines.ETLFileServiceName, t.TempDir())
	require.NoError(t, err)
	inj := &boundaryReadFailFS{boundaryFailFS: &boundaryFailFS{FileService: base}}
	group, err := fileservice.NewFileServices("", local, inj, etl)
	require.NoError(t, err)
	t.Cleanup(func() { group.Close(context.Background()) })
	proc := testutil.NewProcess(t, testutil.WithFileService(group))
	if proc.Ctx == nil {
		proc.Ctx = ctx
	}
	return proc, inj, base
}

// TestPkMoveDumpOriginLedgerRestoredOnBoundaryRollback pins r34 review
// F1 (D137): a boundary dump translates the durable-origin ledger onto
// spill identities, and a later failure of the same boundary's RC
// maintenance must roll the boundary back WITH that translation — even
// when no dirty-object wave ran, so no pending finalization exists to
// restore it. On the pre-D137 tree the ledger stays keyed by the dead
// spill identity (the surviving raw batch keeps its pre-translation
// one), and a later key move misses its origin and replays DELETE+INSERT.
// The preceding-dump r31 tests always installed provenance on a
// compacted object, so the wave's compensating snapshot masked this
// hole; here the origin names a raw workspace row and nothing else.
func TestPkMoveDumpOriginLedgerRestoredOnBoundaryRollback(t *testing.T) {
	proc, _, base := newR29SharedProc(t)
	fx := newR28Fixture(t, proc)
	ctx := context.Background()

	txn, _ := fx.newTxnTbl(t)
	swapRCOperatorForTest(t, txn)
	txn.cnObjsSummary = make(map[types.Objectid]Summary)
	txn.deletedBlocks = &deletedBlocks{offsets: map[types.Blockid][]int64{}}

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
	origin := types.BuildTestRowid(30, 8)

	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"the empty pre-SQL1 boundary must succeed")
	txn.appendWorkspaceEntryLocked(raw)
	aBytes := blkEntryBytes(raw.bat)
	// SQL1's payload update names the raw row's durable origin — no
	// deletions anywhere, so the closing boundary has no wave to lean
	// on. The boundary is size-gated, so nothing spills yet.
	txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{wsRid: origin}
	txn.EndStatement()

	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"SQL1's closing boundary is size-gated: the raw entry stays in memory")
	require.Len(t, txn.writes, 1)
	require.Len(t, txn.transfer.statements, 2)

	// SQL2's closing boundary: thresholds ungated, the dump runs with the
	// origin ledger live, and the injected RC-snapshot failure aborts the
	// boundary AFTER the translation swapped the ledger onto the spill
	// identity.
	txn.writeWorkspaceThreshold = 0
	txn.engine.config.extraWorkspaceThreshold = 0
	txn.engine.config.writeWorkspaceThreshold = 0
	before := fsInventory(t, base)
	txn.EndStatement()

	disarm := armRCBoundaryFault(t, objectio.FJ_CNIncrStmtRCSnapshotFailed)
	txn.StartStatement()
	err := txn.IncrStatementID(ctx, false)
	require.Error(t, err,
		"the injected RC-snapshot failure must abort the boundary")
	require.Contains(t, err.Error(), "injection")
	disarm()

	// The translation really ran before the failure — without this the
	// restore assertions below cannot discriminate D137 from a dump that
	// never translated at all.
	txn.Lock()
	require.Len(t, txn.pkMoveRowidOrigins, 1)
	for k, v := range txn.pkMoveRowidOrigins {
		require.NotEqual(t, wsRid, k,
			"the dump translated the origin ledger onto the spill identity")
		require.Equal(t, origin, v)
	}
	txn.Unlock()
	require.Len(t, txn.writes, 2,
		"D129: the acknowledged raw slot stays live; the spill is the boundary tail")

	// The frontend-equivalent statement rollback: the tail cut deletes
	// the spill file and D137 restores the staged pre-translation
	// ledger. On the pre-D137 tree the ledger stays keyed by the dead
	// spill identity — provenance for a later key move is lost here.
	spillName := singleNewDigestName(t, before, fsInventory(t, base))
	txn.tableCache = new(sync.Map)
	require.NoError(t, txn.RollbackLastStatement(ctx))
	txn.Lock()
	require.Equal(t, map[types.Rowid]types.Rowid{wsRid: origin}, txn.pkMoveRowidOrigins,
		"D137: the rolled-back boundary restored the pre-translation origin ledger exactly")
	txn.Unlock()
	require.Len(t, txn.writes, 1)
	require.Same(t, raw.bat, txn.writes[0].bat)
	require.Equal(t, aBytes, blkEntryBytes(txn.writes[0].bat),
		"the acknowledged raw entry survives the rollback byte-identical")
	require.Len(t, txn.transfer.statements, 2)
	waitFileGone(t, base, spillName)
	require.Equal(t, before, fsInventory(t, base),
		"the rollback's tail-cut GC reclaimed the dead spill file")
	txn.EndStatement()

	// Retry the boundary with the injection lifted: the dump re-spills
	// the untouched raw entry, re-shadows it from the restored ledger,
	// and the adoption finalizes the layout.
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"the retried boundary adopts the finalized layout")
	require.Len(t, txn.writes, 1,
		"the raw slot was husked into exactly the re-spilled file entry")
	require.NotNil(t, txn.writes[0].bat)
	require.NotEmpty(t, txn.writes[0].fileName)
	txn.Lock()
	require.Len(t, txn.pkMoveRowidOrigins, 1)
	for liveRid, originRid := range txn.pkMoveRowidOrigins {
		require.NotEqual(t, wsRid, liveRid,
			"the retried dump re-minted the workspace identity again")
		require.Equal(t, origin, originRid,
			"D137: a later key move's old-side lookup (pk_move_link_fixup.go's origins read) still resolves the durable origin")
	}
	txn.Unlock()
	require.Len(t, txn.transfer.statements, 3,
		"the retried boundary captured its own transfer placeholder")
	spillRetryName := singleNewDigestName(t, before, fsInventory(t, base))
	require.Contains(t, fsInventory(t, base), spillRetryName,
		"the finalized spill is the only new persisted file")
}

// TestPkMoveDumpRegistrationFailureReclaimsPersistedOutput pins r34
// review F2 (D138) on the immediate-cleanup terminal: the dump persists
// its output at Sync, the shadow's read-back then fails on storage, and
// the dump itself reclaims the unregistered files through a
// cancellation-independent deletion before returning the error — nothing
// enters failedWaveOrphans, no reservation books, and the filesystem is
// byte-identical to before the boundary. On the pre-D138 tree the
// persisted object stayed on disk owned by nothing.
func TestPkMoveDumpRegistrationFailureReclaimsPersistedOutput(t *testing.T) {
	proc, inj, base := newR34SharedProc(t)
	fx := newR28Fixture(t, proc)
	ctx := context.Background()

	txn, _ := fx.newTxnTbl(t)
	expectMockBoundaryOps(txn)
	txn.cnObjsSummary = make(map[types.Objectid]Summary)
	txn.deletedBlocks = &deletedBlocks{offsets: map[types.Blockid][]int64{}}

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
	origin := types.BuildTestRowid(30, 8)

	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false))
	txn.appendWorkspaceEntryLocked(raw)
	aBytes := blkEntryBytes(raw.bat)
	txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{wsRid: origin}
	txn.writeWorkspaceThreshold = 0
	txn.engine.config.extraWorkspaceThreshold = 0
	txn.engine.config.writeWorkspaceThreshold = 0
	before := fsInventory(t, base)
	txn.EndStatement()

	// The dump's Sync must succeed (only Write traffic), then the FIRST
	// read of the persisted object — the shadow's meta read-back — fails.
	inj.setReadGate(0, 1, nil)
	txn.StartStatement()
	err := txn.IncrStatementID(ctx, false)
	require.Error(t, err,
		"the gated read-back failure aborts the boundary dump")
	require.ErrorContains(t, err, "context deadline exceeded")

	// D138: the dump reclaimed its persisted-but-unregistered output
	// itself, on a cancellation-independent context, before returning.
	require.Equal(t, before, fsInventory(t, base),
		"D138: the unregistered dump output was reclaimed before the dump returned its error")
	require.Empty(t, txn.failedWaveOrphans,
		"immediate cleanup reconciled the whole inventory — nothing retained")
	alive, _, _, reserved, _, _ := orphanGCEntryState(txn.engine.fs)
	require.False(t, alive, "the healthy registry never saw this failure")
	require.Equal(t, 0, reserved, "no reservation traffic on the immediate-cleanup path")
	recs := inj.deleteAttempts()
	require.Len(t, recs, 1, "exactly one reclaim attempt ran")
	require.False(t, recs[0].injected, "the reclaim really deleted")
	for _, name := range recs[0].files {
		require.NotContains(t, before, name,
			"the reclaimed names were the boundary dump's own output")
	}

	// The workspace itself is untouched: the raw slot is live, the
	// ledger was never translated (the resolve failed first), and a
	// statement rollback plus retry completes cleanly. Nothing was ever
	// registered, so the failed boundary appended no tail either.
	require.Len(t, txn.writes, 1,
		"the failed registration appended no tail; the raw slot lives on")
	txn.tableCache = new(sync.Map)
	require.NoError(t, txn.RollbackLastStatement(ctx))
	txn.Lock()
	require.Equal(t, map[types.Rowid]types.Rowid{wsRid: origin}, txn.pkMoveRowidOrigins,
		"the failed resolve never translated the origin ledger")
	txn.Unlock()
	require.Len(t, txn.writes, 1)
	require.Same(t, raw.bat, txn.writes[0].bat)
	require.Equal(t, aBytes, blkEntryBytes(txn.writes[0].bat))
	txn.EndStatement()

	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"the retried boundary re-dumps and registers cleanly")
	require.Len(t, txn.writes, 1)
	require.NotEmpty(t, txn.writes[0].fileName)
	txn.Lock()
	require.Len(t, txn.pkMoveRowidOrigins, 1)
	for liveRid, originRid := range txn.pkMoveRowidOrigins {
		require.NotEqual(t, wsRid, liveRid)
		require.Equal(t, origin, originRid)
	}
	txn.Unlock()
	spillName := singleNewDigestName(t, before, fsInventory(t, base))
	require.Contains(t, fsInventory(t, base), spillName,
		"exactly the retried dump's registered output persists")
	txn.EndStatement()
}

// TestPkMoveDumpRegistrationFailureRetainsThenDrainsOrphans pins r34
// review F2 (D138) on the retention terminal: when the dump's own
// immediate reclaim cannot reconcile the inventory, the leftovers are
// retained in failedWaveOrphans with a reservation booked for exactly
// the retained count, and the transaction close's GC drain converts
// them name-for-name under D133/D135's exactly-once settlement — the
// file is reclaimed by the durable sweep, the reservations settle to
// zero, the entry releases its shell, a full-headroom reserve books
// again, and the filesystem is byte-identical to the start. On the
// pre-D138 tree nothing owned the persisted output at all.
func TestPkMoveDumpRegistrationFailureRetainsThenDrainsOrphans(t *testing.T) {
	proc, inj, base := newR34SharedProc(t)
	fx := newR28Fixture(t, proc)
	ctx := context.Background()

	txn, _ := fx.newTxnTbl(t)
	expectMockBoundaryOps(txn)
	txn.cnObjsSummary = make(map[types.Objectid]Summary)
	txn.deletedBlocks = &deletedBlocks{offsets: map[types.Blockid][]int64{}}

	// Ample ceilings: booking the single retained name must succeed, so
	// the retention's reservation reconciles name-for-name.
	pinOrphanGCBounds(t, 4, 8, 1)

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
	origin := types.BuildTestRowid(30, 8)

	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false))
	txn.appendWorkspaceEntryLocked(raw)
	txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{wsRid: origin}
	txn.writeWorkspaceThreshold = 0
	txn.engine.config.extraWorkspaceThreshold = 0
	txn.engine.config.writeWorkspaceThreshold = 0
	before := fsInventory(t, base)
	txn.EndStatement()

	// The shadow's read-back fails after Sync, and the dump's own
	// immediate reclaim delete fails too (injected).
	inj.setReadGate(0, 1, nil)
	inj.setFailDeletes(1)
	t.Cleanup(func() { inj.setFailDeletes(0) })
	txn.StartStatement()
	err := txn.IncrStatementID(ctx, false)
	require.Error(t, err,
		"the gated read-back failure aborts the boundary dump")
	require.ErrorContains(t, err, "context deadline exceeded")

	// D138 retention: exactly the persisted name, with exactly one
	// booked reservation, and the file survives on disk.
	orphanName := singleNewDigestName(t, before, fsInventory(t, base))
	require.Equal(t, []string{orphanName}, txn.failedWaveOrphans,
		"D138: the unreclaimable orphan was retained for the close drain with its reservation booked")
	recs := inj.deleteAttempts()
	require.Len(t, recs, 1, "the immediate reclaim ran before retention")
	require.True(t, recs[0].injected, "its deletion was the injected failure")
	require.Equal(t, []string{orphanName}, recs[0].files)
	alive, retained, _, reserved, _, _ := orphanGCEntryState(txn.engine.fs)
	require.True(t, alive, "the booking keeps the registry entry")
	require.Equal(t, 0, retained, "nothing registered yet")
	require.Equal(t, 1, reserved, "exactly the retained name's reservation is booked")

	// The statement rollback restores the pre-boundary workspace; the
	// retry then succeeds WITHOUT the retained file colliding — the
	// orphan stays owned by the close drain, unreachable to user scans.
	txn.tableCache = new(sync.Map)
	require.NoError(t, txn.RollbackLastStatement(ctx))
	require.Len(t, txn.writes, 1)
	txn.EndStatement()
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"the retried boundary re-dumps and registers cleanly")
	require.Len(t, txn.writes, 1)
	require.NotEmpty(t, txn.writes[0].fileName)
	txn.EndStatement()

	// Close-time ownership of last resort: the drain hands the retained
	// name to the durable registry (name-for-name conversion of the
	// booked reservation) and triggers the reclaim sweep.
	txn.gcFailedWaveOrphans()
	require.Empty(t, txn.failedWaveOrphans,
		"the close drain took the retained inventory")
	alive, retained, _, reserved, _, _ = orphanGCEntryState(txn.engine.fs)
	require.True(t, alive)
	require.Equal(t, 1, retained, "D133: the drain converted the retained name into a registration")
	require.Equal(t, 0, reserved,
		"D135: the conversion settled the booked reservation exactly once")

	// The healed sweep reclaims the orphan; with every reservation
	// settled and no inventory left, the entry releases its shell.
	retryDurableOrphanGC(txn.engine)
	require.Eventually(t, func() bool {
		a, _, _, _, _, _ := orphanGCEntryState(txn.engine.fs)
		return !a
	}, 10*time.Second, 5*time.Millisecond,
		"D138: once the retained orphan was deleted the drained entry released its shell")
	waitFileGone(t, base, orphanName)

	// Full headroom returns for later producers, and the filesystem is
	// back to the start plus exactly the retried dump's registered
	// output.
	require.NoError(t, reserveDurableOrphanOwnership(txn.engine.fs, 8),
		"D138: the settled registry admits new production at full headroom")
	releaseDurableOrphanReservation(txn.engine.fs, 8)
	alive, _, _, _, _, _ = orphanGCEntryState(txn.engine.fs)
	require.False(t, alive)
	retryName := singleNewDigestName(t, before, fsInventory(t, base))
	require.Contains(t, fsInventory(t, base), retryName,
		"the only file added across the whole exercise is the retried dump's registered output")
}

// TestPkMoveDumpRollbackPreservesCompletedTransferRekeys pins r35 review
// F1 (D139): the boundary dump translates the origin ledger onto the
// spill identity ({W:O} -> {S:O}); the same boundary's in-memory
// tombstone transfer then COMPLETES — the retained delete tombstone's
// cell is re-minted in place onto the committed identity N and the
// carry re-keys the ledger's value ({S:O} -> {S:N}) — and only then
// does the object-transfer leg fail. The boundary rollback rewinds the
// transfer timestamps and placeholders but not the mutated tombstone
// cells, so D137's wholesale snapshot restore would pitch the ledger
// back to {W:O}: the tombstone naming its committed identity N and the
// ledger naming the bygone O, the retried transfer skipping the
// already-minted N, and a later move anchoring to an identity the
// committed stream never shows departing. D139 instead applies the
// journaled dump translation's inverse to the CURRENT ledger: the
// rollback must yield {W:N} exactly, the tombstone stays
// byte-identical naming N, and the retried boundary re-dumps and
// adopts with the ledger resolving N under its re-minted spill
// identity. On the pre-D139 tree this test fails at the rollback
// assertion, which observes the r34 {W:O} against the surviving
// N-naming tombstone.
func TestPkMoveDumpRollbackPreservesCompletedTransferRekeys(t *testing.T) {
	proc, _, base := newR29SharedProc(t)
	fx := newR28Fixture(t, proc)
	ctx := context.Background()

	txn, tbl := fx.newTxnTbl(t)
	snap := timestamp.Timestamp{PhysicalTime: 100}
	swapRCOperatorWithSnapshotSourceForTest(t, txn,
		func() timestamp.Timestamp { return snap })
	txn.cnObjsSummary = make(map[types.Objectid]Summary)
	txn.deletedBlocks = &deletedBlocks{offsets: map[types.Blockid][]int64{}}

	// The transfer resolves tombstone re-mints through the created-in-
	// transaction partition branch: the old generation is metadata-only
	// (nothing ever reads it back), the replacement generation is a real
	// object whose pk column the transfer reads to resolve N.
	txn.tableOps.addCreatedInTxn(42, 0)
	txn.engine.partitions = make(map[[2]uint64]*logtailreplay.Partition)

	oldID := types.NewObjectid()
	oldStats := objectio.NewObjectStatsWithObjectID(&oldID, false, true, false)
	require.NoError(t, objectio.SetObjectStatsSize(oldStats, 1024))
	require.NoError(t, objectio.SetObjectStatsRowCnt(oldStats, 1))
	require.NoError(t, objectio.SetObjectStatsBlkCnt(oldStats, 1))
	newStats := fx.writeObj(t, tbl, []int64{7})
	require.Len(t, newStats, 1)

	part := txn.engine.GetOrCreateLatestPart(ctx, 1, 7, 42)
	mut, done := part.MutateState()
	require.NoError(t, mut.HandleObjectEntry(ctx, nil, objectio.ObjectEntry{
		ObjectStats: *oldStats,
		CreateTime:  types.BuildTS(90, 0),
		DeleteTime:  types.BuildTS(150, 0),
	}, false))
	require.NoError(t, mut.HandleObjectEntry(ctx, nil, objectio.ObjectEntry{
		ObjectStats: newStats[0],
		CreateTime:  types.BuildTS(150, 0),
	}, false))
	done()

	oldBlk := objectio.NewBlockidWithObjectID(&oldID, 0)
	tombRid := types.NewRowid(&oldBlk, 0)
	newBlk := objectio.NewBlockidWithObjectID(newStats[0].ObjectName().ObjectId(), 0)
	newTombRid := types.NewRowid(&newBlk, 0)

	// The payload-update shape: the old generation's tombstone O with
	// pk 7, the replacement row W with the same pk, and provenance
	// {W:O} linking the workspace row to its durable departing
	// generation.
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
	delBat := batch.NewWithSize(2)
	delBat.SetAttributes([]string{catalog.Row_ID, "pk"})
	delBat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixed(delBat.Vecs[0], tombRid, false, proc.Mp()))
	delBat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(delBat.Vecs[1], int64(7), false, proc.Mp()))
	delBat.SetRowCount(1)
	del := Entry{
		typ:          DELETE,
		accountId:    1,
		databaseId:   7,
		tableId:      42,
		databaseName: "db",
		tableName:    "tbl",
		bat:          delBat,
		pkCheckPos:   1,
	}

	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"the empty pre-SQL1 boundary must succeed")
	txn.appendWorkspaceEntryLocked(raw)
	txn.appendWorkspaceEntryLocked(del)
	rawBytes := blkEntryBytes(raw.bat)
	txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{wsRid: tombRid}
	txn.EndStatement()

	// SQL1's closing boundary is size-gated: the snapshot has not yet
	// advanced over the seeded generation change, so both the dump and
	// the transfer stay out of the way.
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"SQL1's closing boundary is size-gated: raw and tombstone stay in memory")
	require.Len(t, txn.writes, 2)
	require.Len(t, txn.transfer.statements, 2)
	txn.EndStatement()

	// SQL2's closing boundary: thresholds ungated, the snapshot crosses
	// the seeded (90<--150-->200) generation change, and the one-shot
	// object-leg injection fails the boundary after the dump translated
	// the ledger AND the in-memory transfer leg completed its carry.
	txn.writeWorkspaceThreshold = 0
	txn.engine.config.extraWorkspaceThreshold = 0
	txn.engine.config.writeWorkspaceThreshold = 0
	before := fsInventory(t, base)
	snap = timestamp.Timestamp{PhysicalTime: 200}
	disarm := armRCBoundaryFault(t, objectio.FJ_CNIncrStmtRCTransferObjectsFailed)
	txn.StartStatement()
	err := txn.IncrStatementID(ctx, false)
	require.Error(t, err,
		"the injected object-transfer failure must abort the boundary")
	require.Contains(t, err.Error(), "injection")
	require.Contains(t, err.Error(), "object transfer")
	disarm()

	// The dump AND the in-memory transfer really completed before the
	// failure — without this the rollback cannot discriminate D139 from
	// a boundary that never carried anything. {W:O} dumped to {S:O}
	// and the completed carry produced {S:N} while the retained
	// tombstone cell was re-minted onto N in place.
	txn.Lock()
	require.Len(t, txn.pkMoveRowidOrigins, 1)
	for k, v := range txn.pkMoveRowidOrigins {
		require.NotEqual(t, wsRid, k,
			"the dump translated the origin ledger onto the spill identity")
		require.Equal(t, newTombRid, v,
			"the completed in-memory transfer re-keyed the ledger's value onto N")
	}
	txn.Unlock()
	require.Len(t, txn.writes, 3,
		"D129: raw and tombstone stay live; the spill is the boundary tail")
	require.Equal(t, newTombRid,
		vector.MustFixedColNoTypeCheck[types.Rowid](txn.writes[1].bat.Vecs[0])[0],
		"transferTombstoneRowidsAndCarryPkMove re-minted the retained tombstone in place")

	// The frontend-equivalent statement rollback: the tail cut deletes
	// the spill file, and D139 applies the journaled translation's
	// inverse to the CURRENT ledger — the dump's own contribution
	// unwinds while the completed carry's re-minted value survives. On
	// the pre-D139 tree the staged-snapshot restore pitches the ledger
	// back to {W:O} against the N-naming tombstone.
	spillName := singleNewDigestName(t, before, fsInventory(t, base))
	txn.tableCache = new(sync.Map)
	require.NoError(t, txn.RollbackLastStatement(ctx))
	txn.Lock()
	require.Equal(t, map[types.Rowid]types.Rowid{wsRid: newTombRid}, txn.pkMoveRowidOrigins,
		"D139: the rolled-back boundary preserved the completed transfer's re-minted value")
	txn.Unlock()
	require.Len(t, txn.writes, 2)
	require.Same(t, raw.bat, txn.writes[0].bat)
	require.Equal(t, rawBytes, blkEntryBytes(txn.writes[0].bat),
		"the acknowledged raw entry survives the rollback byte-identical")
	require.Equal(t, newTombRid,
		vector.MustFixedColNoTypeCheck[types.Rowid](txn.writes[1].bat.Vecs[0])[0],
		"the retained tombstone's committed identity survives the rollback")
	require.Len(t, txn.transfer.statements, 2)
	waitFileGone(t, base, spillName)
	require.Equal(t, before, fsInventory(t, base),
		"the rollback's tail-cut GC reclaimed the dead spill file")
	txn.EndStatement()

	// Retry the boundary with the injection lifted: the dump re-spills
	// the untouched raw entry, the shadow re-derives from the restored
	// ledger, and the retried transfer skips the already-minted N — its
	// object was never deleted — so the adoption finalizes with the
	// provenance resolving N under the re-minted spill identity.
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"the retried boundary re-dumps and adopts cleanly")
	require.Len(t, txn.writes, 2)
	txn.Lock()
	require.Len(t, txn.pkMoveRowidOrigins, 1)
	for k, v := range txn.pkMoveRowidOrigins {
		require.NotEqual(t, wsRid, k,
			"the retried dump re-minted the provenance key onto the new spill identity")
		require.Equal(t, newTombRid, v,
			"D139: the retried boundary's ledger resolves the re-minted identity N")
	}
	txn.Unlock()
	var tomb *Entry
	for i := range txn.writes {
		if txn.writes[i].typ == DELETE && txn.writes[i].fileName == "" {
			tomb = &txn.writes[i]
		}
	}
	require.NotNil(t, tomb, "the in-memory tombstone entry survives the adoption")
	require.Equal(t, newTombRid,
		vector.MustFixedColNoTypeCheck[types.Rowid](tomb.bat.Vecs[0])[0],
		"the retried transfer skipped the already-minted N — no double re-key")
	retryName := singleNewDigestName(t, before, fsInventory(t, base))
	require.Contains(t, fsInventory(t, base), retryName,
		"exactly the retried dump's registered output persists")
	txn.EndStatement()
}

// TestPkMoveDumpAdmissionRefusesBeforePersistenceAtSaturation pins r35
// review F2 (D140): bounded producer admission (D133) reaches the dump
// at the persistence choke point. With the registry saturated — a peer
// producer holding a reservation and a stuck retained name failing
// sustained deletes — the dump's first object is refused by the
// unpublished-name observer before a single byte persists: the boundary
// fails with the D133 exhaustion error, the filesystem stays
// byte-identical, and nothing enters failedWaveOrphans. Repeated
// retries fail identically without growing any state; sustained
// reclamation failure backpressures the producing statement instead of
// compounding retained inventory the way D138's post-persistence
// booking-then-append did. Once reclamation heals, the retry's
// reservation rounds reclaim the stuck name, admission succeeds, the
// dump registers, and the healthy terminal releases its booking — the
// peer's reservation is exact at every terminal. On the pre-D140 tree
// there is no observer at all, so the first refusal assertion sees a
// completed boundary instead of the exhaustion error.
func TestPkMoveDumpAdmissionRefusesBeforePersistenceAtSaturation(t *testing.T) {
	proc, inj, base := newR29SharedProc(t)
	fx := newR28Fixture(t, proc)
	ctx := context.Background()

	txn, _ := fx.newTxnTbl(t)
	expectMockBoundaryOps(txn)
	txn.cnObjsSummary = make(map[types.Objectid]Summary)
	txn.deletedBlocks = &deletedBlocks{offsets: map[types.Blockid][]int64{}}

	// Saturation: set=[stuck], reserved=1(peer) under hardCapacity=2 —
	// used == 2 == hard, and the only headroom-freeing mechanism
	// (reclaiming the stuck name) keeps failing its deletes.
	pinOrphanGCBounds(t, 2, 2, 1)
	inj.setFailDeletes(1 << 20)
	t.Cleanup(func() { inj.setFailDeletes(0) })
	require.NoError(t, reserveDurableOrphanOwnership(txn.engine.fs, 1),
		"the stuck name's producer booked before persisting it")
	registerDurableOrphanGC(txn.engine.fs, "stuck-orphan-for-e29")
	require.NoError(t, reserveDurableOrphanOwnership(txn.engine.fs, 1),
		"the peer producer holds its reservation")
	alive, retained, _, reserved, _, _ := orphanGCEntryState(txn.engine.fs)
	require.True(t, alive)
	require.Equal(t, 1, retained, "the stuck name is retained")
	require.Equal(t, 1, reserved, "the peer's reservation is booked")

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
	origin := types.BuildTestRowid(30, 8)

	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false))
	txn.appendWorkspaceEntryLocked(raw)
	txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{wsRid: origin}
	txn.writeWorkspaceThreshold = 0
	txn.engine.config.extraWorkspaceThreshold = 0
	txn.engine.config.writeWorkspaceThreshold = 0
	before := fsInventory(t, base)
	txn.EndStatement()

	assertRefusal := func() {
		txn.StartStatement()
		err := txn.IncrStatementID(ctx, false)
		require.ErrorContains(t, err, "durable orphan ownership exhausted",
			"D140: producer admission refuses BEFORE persistence at the hard bound")
		require.Equal(t, before, fsInventory(t, base),
			"no byte of dump output exists — production stopped before writing")
		require.Empty(t, txn.failedWaveOrphans,
			"nothing was persisted, so nothing needs retention")
		alive, retained, _, reserved, _, _ := orphanGCEntryState(txn.engine.fs)
		require.True(t, alive)
		require.Equal(t, 1, retained, "the stuck name survives the admission rounds")
		require.Equal(t, 1, reserved, "the peer's reservation is untouched")
		txn.tableCache = new(sync.Map)
		require.NoError(t, txn.RollbackLastStatement(ctx))
		txn.EndStatement()
	}
	assertRefusal()
	assertRefusal()

	// Heal: the retry's reservation rounds reclaim the stuck name and
	// free headroom, admission succeeds, the dump registers its output,
	// and the healthy terminal releases its booking. Exactly the peer's
	// reservation remains.
	inj.setFailDeletes(0)
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"healed reclamation frees headroom; the retried dump completes")
	txn.EndStatement()
	alive, retained, _, reserved, _, _ = orphanGCEntryState(txn.engine.fs)
	require.True(t, alive, "the peer's reservation keeps the shell")
	require.Equal(t, 0, retained, "the admission rounds reclaimed the stuck name")
	require.Equal(t, 1, reserved,
		"exactly the peer's reservation survives the healthy terminal")
	retryName := singleNewDigestName(t, before, fsInventory(t, base))
	require.Contains(t, fsInventory(t, base), retryName,
		"exactly the healed retry's registered output persists")

	releaseDurableOrphanReservation(txn.engine.fs, 1)
	alive, _, _, _, _, _ = orphanGCEntryState(txn.engine.fs)
	require.False(t, alive, "the peer's own release collects the shell")
}

// TestPkMoveDumpRetentionConvertsOnlyItsOwnBookings pins the retention
// terminal of r35 review F2 (D140): headroom exactly one beyond the
// peer's booking, so the dump's object books the LAST slot at the
// persistence choke point; the sustained read-back failure then fails
// the shadow resolve and the dump's own reclaim fails its deletes,
// retaining the orphan with its admission booking in place. Retries
// refuse admission before persisting anything (the registry's reserved
// total stays at peer-1 plus ours-1) until the transaction close's
// drain registers the orphan and converts it against OUR booking —
// never anonymously against the peer's — after which the healed
// admission rounds reclaim it. The peer's reservation survives every
// terminal byte-exact until the peer itself releases it. On the
// pre-D140 tree the first retry persists a second orphan instead of
// refusing, so the refusal assertion sees the read-back error instead
// of the exhaustion error.
func TestPkMoveDumpRetentionConvertsOnlyItsOwnBookings(t *testing.T) {
	proc, inj, base := newR34SharedProc(t)
	fx := newR28Fixture(t, proc)
	ctx := context.Background()

	txn, _ := fx.newTxnTbl(t)
	expectMockBoundaryOps(txn)
	txn.cnObjsSummary = make(map[types.Objectid]Summary)
	txn.deletedBlocks = &deletedBlocks{offsets: map[types.Blockid][]int64{}}

	// Capacity fits the one orphan; the hard bound fits exactly the
	// peer's booking plus the dump's own.
	pinOrphanGCBounds(t, 2, 2, 1)
	require.NoError(t, reserveDurableOrphanOwnership(txn.engine.fs, 1),
		"the peer producer holds its reservation")
	inj.setFailDeletes(1 << 20)
	t.Cleanup(func() { inj.setFailDeletes(0) })

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
	origin := types.BuildTestRowid(30, 8)

	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false))
	txn.appendWorkspaceEntryLocked(raw)
	txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{wsRid: origin}
	txn.writeWorkspaceThreshold = 0
	txn.engine.config.extraWorkspaceThreshold = 0
	txn.engine.config.writeWorkspaceThreshold = 0
	before := fsInventory(t, base)
	txn.EndStatement()

	// Sustained: the shadow resolve's read-back fails, and the dump's
	// own reclaim delete fails too.
	inj.setReadGate(0, 1<<20, nil)

	// Boundary 1: the dump books the last slot at the choke point,
	// persists, the read-back fails, the immediate reclaim fails — the
	// orphan is retained WITH its pre-placed admission booking.
	txn.StartStatement()
	err := txn.IncrStatementID(ctx, false)
	require.ErrorContains(t, err, "context deadline exceeded",
		"the gated read-back failure aborts the boundary dump")
	orphanName := singleNewDigestName(t, before, fsInventory(t, base))
	require.Equal(t, []string{orphanName}, txn.failedWaveOrphans,
		"D140: the unreclaimable orphan is retained with its admission booking in place")
	_, retained, _, reserved, _, _ := orphanGCEntryState(txn.engine.fs)
	require.Equal(t, 0, retained, "nothing is registered yet")
	require.Equal(t, 2, reserved,
		"the peer's booking plus the dump's own admission booking")
	txn.tableCache = new(sync.Map)
	require.NoError(t, txn.RollbackLastStatement(ctx))
	txn.EndStatement()

	// Repeated boundary retries refuse admission BEFORE persisting:
	// reserved total stays at peer-1 plus ours-1 and no new file ever
	// appears.
	assertRefusal := func() {
		txn.StartStatement()
		err := txn.IncrStatementID(ctx, false)
		require.ErrorContains(t, err, "durable orphan ownership exhausted",
			"D140: saturated admission refuses the retry before persistence")
		require.Equal(t, orphanName,
			singleNewDigestName(t, before, fsInventory(t, base)),
			"only the boundary-1 orphan exists — the retry persisted nothing")
		require.Equal(t, []string{orphanName}, txn.failedWaveOrphans)
		_, retained, _, reserved, _, _ := orphanGCEntryState(txn.engine.fs)
		require.Equal(t, 0, retained)
		require.Equal(t, 2, reserved, "peer-1 plus ours-1, unchanged")
		txn.tableCache = new(sync.Map)
		require.NoError(t, txn.RollbackLastStatement(ctx))
		txn.EndStatement()
	}
	assertRefusal()
	assertRefusal()

	// Transaction close: the drain registers the orphan and converts it
	// against OUR booking — the anonymous-conversion hazard of D138's
	// unbooked retention cannot touch the peer.
	txn.gcFailedWaveOrphans()
	require.Empty(t, txn.failedWaveOrphans,
		"the close drain took the retained inventory")
	alive, retained, _, reserved, _, _ := orphanGCEntryState(txn.engine.fs)
	require.True(t, alive)
	require.Equal(t, 1, retained, "D133: the drain registered the retained name")
	require.Equal(t, 1, reserved,
		"D135: the conversion settled exactly our booking — the peer's survives")
	// The drain launches the reclaim sweep asynchronously; with the
	// sustained delete failure it returns the name to the retained set
	// immediately. Wait for quiescence so later admission rounds can
	// select it deterministically instead of seeing it held inflight.
	require.Eventually(t, func() bool {
		_, _, inflight, _, _, _ := orphanGCEntryState(txn.engine.fs)
		return inflight == 0
	}, 10*time.Second, 5*time.Millisecond,
		"the failing drain sweep went quiescent")

	// Still failing: one more retry refuses before persisting — the
	// registered orphan pins headroom and its reclaim keeps failing.
	assertRefusalAfterDrain := func() {
		txn.StartStatement()
		err := txn.IncrStatementID(ctx, false)
		require.ErrorContains(t, err, "durable orphan ownership exhausted",
			"D140: the registration pins headroom; admission still refuses pre-persistence")
		require.Equal(t, orphanName,
			singleNewDigestName(t, before, fsInventory(t, base)),
			"the registered orphan is the only inventory change")
		_, retained, _, reserved, _, _ := orphanGCEntryState(txn.engine.fs)
		require.Equal(t, 1, retained)
		require.Equal(t, 1, reserved, "the peer's booking survives the drained retention")
		txn.tableCache = new(sync.Map)
		require.NoError(t, txn.RollbackLastStatement(ctx))
		txn.EndStatement()
	}
	assertRefusalAfterDrain()

	// Heal: admission rounds reclaim the orphan; the retry books,
	// persists, and registers; the healthy terminal releases its
	// booking. Exactly the peer's reservation remains.
	inj.setReadGate(0, 0, nil)
	inj.setFailDeletes(0)
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"healed reclamation frees headroom; the retried dump completes")
	txn.EndStatement()
	retryName := singleNewDigestName(t, before, fsInventory(t, base))
	require.Contains(t, fsInventory(t, base), retryName,
		"the healed retry's registered output replaces the reclaimed orphan")
	require.NotEqual(t, orphanName, retryName)
	_, retained, _, reserved, _, _ = orphanGCEntryState(txn.engine.fs)
	require.Equal(t, 0, retained, "the admission rounds reclaimed the orphan")
	require.Equal(t, 1, reserved,
		"exactly the peer's booking survives every terminal")

	// The peer's own release collects the shell — asynchronously
	// launched sweeps may still be quiescing, so wait for it.
	releaseDurableOrphanReservation(txn.engine.fs, 1)
	require.Eventually(t, func() bool {
		a, _, _, _, _, _ := orphanGCEntryState(txn.engine.fs)
		return !a
	}, 10*time.Second, 5*time.Millisecond,
		"the peer's own release collects the shell once every name is gone")
}

// --- r57 review F1: boundary adoption locates staged originals once ---
//
// applyPendingBoundaryFinalizationLocked re-located every staged original
// by rescanning txn.writes from the start per staged identity, so a wave
// staging N originals among W workspace entries cost a quadratic
// 1+2+...+N identity comparisons under txn.Lock. The pre-fix shape is
// pinned here by the SAME assertions the fixed tree must satisfy — the
// behavior is identical (D126 identity release, first-match ordering,
// loud miss), only the counted work moves from ~1.5*N^2 comparisons to
// exactly one build visit per workspace slot plus one consult per staged
// identity.
func TestBoundaryAdoptionLocatesStagedOriginalsOnce(t *testing.T) {
	proc := testutil.NewProc(t)

	const (
		// N originals, interleaved unrelated/original/replacement per the
		// r57 action: W = 3N slots, S staged consults, so ONE index pass
		// plus the consults scans exactly W+S = 4N identities.
		N = 64
		W = 3 * N
	)

	// stagedLayout builds a transaction whose workspace interleaves N
	// unrelated in-memory inserts, N file-backed originals (all staged
	// for release), and N file-backed replacements, then stages the
	// originals — plus every bat identity whose slot is listed in dup —
	// as one adoption tail. Returns the slots the originals occupy, the
	// slots whose batches must survive VERBATIM, and the staged
	// originals' accounted byte sizes for the exact refund sum.
	stagedLayout := func(t *testing.T, foreignStaged bool, dup ...int) (
		txn *Transaction,
		origSlots []int,
		survSlots map[int]*batch.Batch,
		origSizes []uint64,
		foreign *batch.Batch,
		staged int,
	) {
		t.Helper()
		txn = newTransactionWithActivePKTableForTest(t, "pk")
		survSlots = make(map[int]*batch.Batch, 2*N)

		appendEntry := func(fileName string, pk int64) (int, *batch.Batch) {
			slot := len(txn.writes)
			bat := newInsertBatchWithRowIDForTest(t, proc, []int64{pk})
			txn.writes = append(txn.writes, Entry{
				typ:          INSERT,
				accountId:    1,
				databaseId:   7,
				tableId:      42,
				databaseName: "db",
				tableName:    "tbl",
				fileName:     fileName,
				bat:          bat,
			})
			txn.accountWorkspaceEntryLocked(&txn.writes[slot])
			return slot, bat
		}

		var releaseBats []*batch.Batch
		for i := 0; i < N; i++ {
			uSlot, uBat := appendEntry("", int64(100000+i)) // unrelated: survives
			survSlots[uSlot] = uBat
			oSlot, oBat := appendEntry("obj", int64(200000+i)) // original: staged
			origSlots = append(origSlots, oSlot)
			origSizes = append(origSizes, uint64(oBat.Size()))
			releaseBats = append(releaseBats, oBat)
			rSlot, rBat := appendEntry("repl", int64(300000+i)) // replacement: survives
			survSlots[rSlot] = rBat
		}
		for _, d := range dup {
			// A duplicated staged identity stands in for a staging bug:
			// adoption must still release exactly once (r27 D115).
			releaseBats = append(releaseBats, txn.writes[origSlots[d]].bat)
		}
		if foreignStaged {
			// A staged identity that never entered the workspace: the
			// staging contract breach stays loud and releases nothing.
			foreign = newInsertBatchWithRowIDForTest(t, proc, []int64{400000})
			releaseBats = append(releaseBats, foreign)
		}
		staged = len(releaseBats)
		txn.pendingBoundaryFinalization = &boundaryFinalization{
			releaseBats: releaseBats,
		}
		return txn, origSlots, survSlots, origSizes, foreign, staged
	}

	// adoptAndCheck drives one adoption and pins everything it owes:
	// exactly-once release of every UNIQUE staged original, verbatim
	// survivors, the exact byte refund, a balanced workspace ledger, and
	// the locator's work bounded by one index pass plus one consult per
	// staged identity (expectScans). Pre-fix the same expectations fail
	// with the quadratic rescan count.
	adoptAndCheck := func(
		t *testing.T,
		txn *Transaction,
		origSlots []int,
		survSlots map[int]*batch.Batch,
		origSizes []uint64,
		staged, expectReleases, expectScans int,
	) {
		t.Helper()

		txn.Lock()
		require.NoError(t, txn.checkWorkspaceAccountingLocked())
		var refund uint64
		for _, s := range origSizes {
			refund += s
		}
		sizeBefore := txn.workspaceSize

		txn.applyPendingBoundaryFinalizationLocked()

		require.Equal(t, expectScans, txn.boundaryAdoptScans,
			"one adoption must scan exactly one identity per workspace slot plus one per staged original")
		require.Equal(t, expectReleases, txn.boundaryAdoptReleases,
			"exactly-once cleanup: every unique staged original is released once and only once")
		require.Len(t, txn.writes, W,
			"adoption husks in place — no slot moves during the release (D126)")
		for i, slot := range origSlots {
			require.Nil(t, txn.writes[slot].bat,
				"original %d released: bat husked", i)
			require.Zero(t, txn.writes[slot].accountedSize,
				"original %d released: accounting zeroed", i)
		}
		live := 0
		for slot, want := range survSlots {
			require.Same(t, want, txn.writes[slot].bat,
				"slot %d survives adoption byte-identical", slot)
			live++
		}
		require.Equal(t, 2*N, live, "every unrelated and replacement entry survives")
		require.Equal(t, sizeBefore-refund, txn.workspaceSize,
			"the adoption refunds exactly the released originals' accounted bytes")
		require.NoError(t, txn.checkWorkspaceAccountingLocked(),
			"the workspace ledger stays balanced across adoption")
		require.Nil(t, txn.pendingBoundaryFinalization,
			"the staged tail is consumed by its adoption")
		txn.Unlock()
	}

	t.Run("many originals interleaved with unrelated and replacement entries adopt in one locator pass", func(t *testing.T) {
		txn, origSlots, survSlots, origSizes, _, staged := stagedLayout(t, false)
		adoptAndCheck(t, txn, origSlots, survSlots, origSizes, staged,
			N,        // every unique staged original released once
			W+staged, // one build visit per slot plus one consult per staged
		)

		// A second adoption with nothing staged is a true no-op: the
		// counters reset at entry and stay at zero.
		txn.Lock()
		txn.applyPendingBoundaryFinalizationLocked()
		require.Zero(t, txn.boundaryAdoptScans)
		require.Zero(t, txn.boundaryAdoptReleases)
		txn.Unlock()
	})

	t.Run("a duplicated staged identity is released exactly once", func(t *testing.T) {
		txn, origSlots, survSlots, origSizes, _, staged := stagedLayout(t, false, 5)
		adoptAndCheck(t, txn, origSlots, survSlots, origSizes, staged,
			N, // the duplicate consult must not double-release original 5
			W+staged,
		)
	})

	t.Run("an absent staged identity stays loud and releases nothing", func(t *testing.T) {
		txn, origSlots, survSlots, origSizes, foreign, staged := stagedLayout(t, true)
		adoptAndCheck(t, txn, origSlots, survSlots, origSizes, staged,
			N, // the foreign identity releases nothing beyond the originals
			W+staged,
		)
		require.Equal(t, 1, foreign.RowCount(),
			"the foreign batch was never a workspace batch and stays untouched")
	})
}
