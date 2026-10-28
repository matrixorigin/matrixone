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
	"bytes"
	"context"
	"hash/fnv"
	"sync"
	"testing"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// --- r28 review: boundary-atomicity proofs for D120/D121/D122 ---------
//
// These tests pin the statement-boundary ownership contract of the
// pk-move provenance stack:
//
//   - D120 (r28 F1): the rollback boundary is ESTABLISHED before any
//     fallible maintenance, so a failed compaction boundary rolls back
//     only the boundary's own tail appends; the finishing statement —
//     whose provenance undo history the boundary preamble retired —
//     keeps its acknowledged writes, lineage bindings and origin ledger
//     through the frontend-equivalent statement rollback, retry and a
//     trailing boundary.
//
//   - D121 (r28 F2): clean-object carries publish inside the SAME atomic
//     wave as the dirty replacements, and the rollback range GC never
//     deletes a file still referenced by an entry outside the cut range.
//
//   - D122 (r28 F3): replacement-file reclamation survives request
//     cancellation and transient delete failures — every unpublished
//     file keeps a cleanup owner from the writer defer through the
//     failed-wave cleanup.

// injectFS wraps the shared store with deterministic failure knobs for
// the r28 F3 ownership proofs. Deletes reject a cancelled context like
// the production object stores the r28 review cites, can fail a bounded
// number of times transiently, and a write can be armed to persist and
// then report a commit-ambiguous cancellation.
type injectFS struct {
	fileservice.FileService

	mu sync.Mutex
	// writeCancelOnce: the next Write is performed, then fn is invoked
	// and the write reports context.Canceled — the commit-ambiguous
	// shape that leaves a persisted object behind a failed Sync.
	writeCancelOnce context.CancelFunc
	// failDeleteTimes: that many next Delete calls fail transiently.
	failDeleteTimes int
	// ctxCheckDelete mirrors the production object stores: Delete
	// rejects a cancelled context immediately.
	ctxCheckDelete bool

	deletes []deleteRecord
}

func (fs *injectFS) recordDeleteLocked(rec deleteRecord, injected bool) {
	rec.injected = injected
	fs.deletes = append(fs.deletes, rec)
}

type deleteRecord struct {
	files    []string
	ctxErr   error
	injected bool
}

func (fs *injectFS) Write(ctx context.Context, vector fileservice.IOVector) error {
	fs.mu.Lock()
	fn := fs.writeCancelOnce
	if fn != nil {
		fs.writeCancelOnce = nil
	}
	fs.mu.Unlock()

	if err := fs.FileService.Write(ctx, vector); err != nil {
		return err
	}
	if fn != nil {
		fn()
		return context.Canceled
	}
	return nil
}

func (fs *injectFS) Delete(ctx context.Context, filePaths ...string) error {
	fs.mu.Lock()
	rec := deleteRecord{files: append([]string(nil), filePaths...), ctxErr: ctx.Err()}
	inject := fs.failDeleteTimes > 0
	if inject {
		fs.failDeleteTimes--
	}
	fs.recordDeleteLocked(rec, inject)
	fs.mu.Unlock()

	if fs.ctxCheckDelete && ctx.Err() != nil {
		return ctx.Err()
	}
	if inject {
		return context.DeadlineExceeded
	}
	return fs.FileService.Delete(ctx, filePaths...)
}

func (fs *injectFS) deleteAttempts() []deleteRecord {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return append([]deleteRecord(nil), fs.deletes...)
}

// newR28SharedProc builds a process whose shared file service is an
// injectFS over a real local store, returning the store for direct
// inventory oracles.
func newR28SharedProc(t *testing.T) (*process.Process, *injectFS, fileservice.FileService) {
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
	inj := &injectFS{FileService: base, ctxCheckDelete: true}
	group, err := fileservice.NewFileServices("", local, inj, etl)
	require.NoError(t, err)
	t.Cleanup(func() { group.Close(context.Background()) })
	proc := testutil.NewProcess(t, testutil.WithFileService(group))
	if proc.Ctx == nil {
		proc.Ctx = ctx
	}
	return proc, inj, base
}

// r28Fixture bundles the harness for the boundary atomicity proofs: a
// transaction with a live pk table, huge dump thresholds (the boundary
// must reach compaction in mergeTxnWorkspaceLocked without any spill
// machinery), and a live ants GC pool so a rollback-side GC request that
// should never fire is observable as a REAL deletion instead of an
// engine-unavailable error.
type r28Fixture struct {
	proc *process.Process
	mp   *mpool.MPool
	cols []*pbplan.ColDef
}

func newR28Fixture(t *testing.T, proc *process.Process) *r28Fixture {
	t.Helper()
	colexec.NewServer("")
	return &r28Fixture{
		proc: proc,
		mp:   proc.Mp(),
		cols: []*pbplan.ColDef{
			{Name: "pk", Typ: pbplan.Type{Id: int32(types.T_int64)}, Seqnum: 0},
			{Name: catalog.Row_ID, Typ: pbplan.Type{Id: int32(types.T_Rowid)}, Seqnum: 1},
		},
	}
}

func (f *r28Fixture) newTxnTbl(t *testing.T) (*Transaction, *txnTable) {
	t.Helper()
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	txn.proc = f.proc
	txn.writeWorkspaceThreshold = 1 << 60
	txn.commitWorkspaceThreshold = 1 << 60
	txn.tnStores = []DNStore{{}}
	txn.engine.gcPool, _ = ants.NewPool(2)
	t.Cleanup(func() {
		if txn.engine.gcPool != nil {
			txn.engine.gcPool.Release()
		}
	})
	tbl := txn.tableOps.existAndActive(genTableKey(1, "tbl", 7, "db"))
	require.NotNil(t, tbl)
	tbl.tableDef.Cols = f.cols
	tbl.tableDef.DbName = "db"
	tbl.tableDef.Name = "tbl"
	tbl.proc.Store(f.proc)
	sharedFS, err := colexec.GetSharedFSFromProc(f.proc)
	require.NoError(t, err)
	txn.engine.fs = sharedFS
	tbl.ensureSeqnumsAndTypesExpectRowid()
	return txn, tbl
}

func (f *r28Fixture) writeObj(t *testing.T, tbl *txnTable, pks []int64) []objectio.ObjectStats {
	t.Helper()
	sharedFS, err := colexec.GetSharedFSFromProc(f.proc)
	require.NoError(t, err)
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(bat.Vecs[0], pks, nil, f.mp))
	bat.SetRowCount(len(pks))
	w := colexec.NewCNS3DataWriterForService(
		f.proc.GetService(), f.mp, sharedFS, tbl.tableDef, -1, false)
	require.NoError(t, w.Write(context.Background(), bat))
	stats, wErr := w.Sync(context.Background())
	require.NoError(t, wErr)
	w.Close()
	bat.Clean(f.mp)
	return stats
}

// flushedEntry builds one flushed INSERT entry in the exact BlockMeta
// layout the boundary scan consumes, carrying the given object stats rows
// (one block per object, in order).
func (f *r28Fixture) flushedEntry(
	t *testing.T, fileName string, stats ...objectio.ObjectStats) Entry {
	t.Helper()
	blkBat := colexec.AllocCNS3ResultBat(false)
	for i := range stats {
		blk := objectio.NewBlockidWithObjectID(stats[i].ObjectName().ObjectId(), 0)
		blkInfoBuf := new(bytes.Buffer)
		_, infoErr := (&objectio.BlockInfo{BlockID: blk}).MarshalWithBuf(blkInfoBuf)
		require.NoError(t, infoErr)
		require.NoError(t, vector.AppendBytes(blkBat.Vecs[0], blkInfoBuf.Bytes(), false, f.mp))
		require.NoError(t, vector.AppendBytes(blkBat.Vecs[1], stats[i].Marshal(), false, f.mp))
	}
	blkBat.SetRowCount(len(stats))
	return Entry{
		typ:          INSERT,
		accountId:    1,
		databaseId:   7,
		tableId:      42,
		databaseName: "db",
		tableName:    "tbl",
		bat:          blkBat,
		fileName:     fileName,
	}
}

// stageDirtyOn marks block 0 of stats row 0 deleted with the row-1
// survivor provenance-named, registering the summary against objBat, and
// severs the table pk definition so the rewrite worker's provenance
// shadow hard-errors. The returned restore re-arms a healthy wave.
func (f *r28Fixture) stageDirtyOn(
	t *testing.T, txn *Transaction, tbl *txnTable,
	stats objectio.ObjectStats, objBat *batch.Batch,
) (types.Blockid, func()) {
	t.Helper()
	blk := objectio.NewBlockidWithObjectID(stats.ObjectName().ObjectId(), 0)
	survivor := types.NewRowid(&blk, 1)
	txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{
		survivor: types.BuildTestRowid(30, 2),
	}
	objId := *stats.ObjectName().ObjectId()
	if txn.cnObjsSummary == nil {
		txn.cnObjsSummary = make(map[types.Objectid]Summary)
	}
	txn.cnObjsSummary[objId] = Summary{
		objBat:     objBat,
		accountId:  1,
		databaseId: 7,
		tableId:    42,
		tbName:     "tbl",
		dbName:     "db",
	}
	txn.deletedBlocks = &deletedBlocks{
		offsets: map[types.Blockid][]int64{blk: {0}},
	}
	savedName := tbl.tableDef.Pkey.PkeyColName
	tbl.tableDef.Pkey.PkeyColName = "no_such_column"
	restore := func() { tbl.tableDef.Pkey.PkeyColName = savedName }
	t.Cleanup(restore)
	require.NotNil(t, txn.pkMoveRewriteMigrationRelevantLocked(),
		"the provenance-named survivor must make this wave provenance-relevant")
	return blk, restore
}

// fsInventory walks the whole shared store and returns path -> content
// digest: the ownership oracle for file reclaim proofs.
func fsInventory(t *testing.T, fs fileservice.FileService) map[string]uint64 {
	t.Helper()
	ctx := context.Background()
	digests := make(map[string]uint64)
	var walk func(dir string)
	walk = func(dir string) {
		for entry, lerr := range fs.List(ctx, dir) {
			require.NoError(t, lerr)
			name := entry.Name
			if dir != "" {
				name = dir + "/" + entry.Name
			}
			if entry.IsDir {
				walk(name)
				continue
			}
			ioVec := fileservice.IOVector{
				FilePath: name,
				Entries:  []fileservice.IOEntry{{Offset: 0, Size: -1}},
			}
			require.NoError(t, fs.Read(ctx, &ioVec))
			require.Len(t, ioVec.Entries, 1)
			h := fnv.New64a()
			_, _ = h.Write(ioVec.Entries[0].Data)
			digests[name] = h.Sum64()
		}
	}
	walk("")
	return digests
}

// waitFileGone waits for the async GC pool to evict name, keeping later
// inventory assertions deterministic.
func waitFileGone(t *testing.T, fs fileservice.FileService, name string) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, err := fs.StatFile(context.Background(), name)
		return err != nil
	}, 10*time.Second, 5*time.Millisecond, "the GC pool must evict %s", name)
}

// singleNewDigestName returns the single name present in after but not
// in before, failing the test unless exactly one such name exists.
func singleNewDigestName(t *testing.T, before, after map[string]uint64) string {
	t.Helper()
	var extra []string
	for name := range after {
		if _, ok := before[name]; !ok {
			extra = append(extra, name)
		}
	}
	require.Len(t, extra, 1, "exactly one new file must appear, got %v", extra)
	return extra[0]
}

// blkEntryBytes serializes an entry's content for byte-stability
// comparisons across failure/rollback cycles.
func blkEntryBytes(bat *batch.Batch) [][]byte {
	var out [][]byte
	for _, vec := range bat.Vecs {
		if vec.GetType().IsVarlen() {
			col, area := vector.MustVarlenaRawData(vec)
			for i := range col {
				out = append(out, append([]byte(nil), col[i].GetByteSlice(area)...))
			}
			continue
		}
		out = append(out, []byte(vec.String()))
	}
	return out
}

func expectMockBoundaryOps(txn *Transaction) {
	mockOp := txn.op.(*mock_frontend.MockTxnOperator)
	mockOp.EXPECT().EnterIncrStmt().AnyTimes()
	mockOp.EXPECT().ExitIncrStmt().AnyTimes()
	mockOp.EXPECT().EnterRollbackStmt().AnyTimes()
	mockOp.EXPECT().ExitRollbackStmt().AnyTimes()
}

// TestPkMoveFailedBoundaryKeepsFinishingStatementAlive pins r28 review F1
// (D120): a maintenance failure at statement S's closing boundary rolls
// back only the boundary's own tail; S's acknowledged writes, lineage
// bindings and origin ledger stay intact through the frontend-equivalent
// statement rollback, the retried boundary and a trailing boundary.
//
// The pre-r28 ordering retired S's provenance undo history and only then
// ran the fallible merge/dump, so RollbackLastStatement computed the cut
// point with the UNADVANCED statement id and truncated S itself: on that
// shape this test fails at the post-rollback require.Len(writes, 2).
func TestPkMoveFailedBoundaryKeepsFinishingStatementAlive(t *testing.T) {
	proc, _, base := newR28SharedProc(t)
	fx := newR28Fixture(t, proc)
	ctx := context.Background()

	txn, tbl := fx.newTxnTbl(t)
	expectMockBoundaryOps(txn)

	// SQL1: a flushed INSERT entry — the earlier acknowledged statement.
	objStats := fx.writeObj(t, tbl, []int64{1, 2, 3})
	require.Len(t, objStats, 1)
	origName := objStats[0].ObjectName().String()

	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"the empty pre-SQL1 boundary must succeed")
	require.Equal(t, 1, txn.statementID)
	require.Equal(t, []int{0}, txn.offsets)

	e0 := fx.flushedEntry(t, objStats[0].ObjectLocation().String(), objStats[0])
	txn.appendWorkspaceEntryLocked(e0)
	txn.EndStatement()

	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"SQL1's clean closing boundary must succeed")
	require.Equal(t, 2, txn.statementID)
	require.Equal(t, []int{0, 1}, txn.offsets)
	require.Len(t, txn.writes, 1)

	// SQL2 executes: an in-memory INSERT of its own (the tail write whose
	// survival pins D120), a lineage binding tagged with its own
	// statement id, and the staged dirty wave with the severed-pk shadow
	// injection.
	e1 := Entry{
		typ:          INSERT,
		accountId:    1,
		databaseId:   7,
		tableId:      42,
		databaseName: "db",
		tableName:    "tbl",
		bat:          newInsertBatchWithRowIDForTest(t, proc, []int64{7}),
	}
	txn.appendWorkspaceEntryLocked(e1)

	acctTbl := pkMoveAcctTbl{account: 1, table: 42}
	boundOrigin := types.BuildTestRowid(30, 9)
	txn.pkMoveKeyLineage = map[pkMoveAcctTbl]map[string][]pkMoveLineageBind{
		acctTbl: {
			"K": {
				{origin: boundOrigin, wm: len(txn.writes), stmt: txn.statementID},
			},
		},
	}

	origBlk, restore := fx.stageDirtyOn(t, txn, tbl, objStats[0], txn.writes[0].bat)
	before := fsInventory(t, base)
	e0Bytes := blkEntryBytes(txn.writes[0].bat)
	e1Bytes := blkEntryBytes(txn.writes[1].bat)
	txn.EndStatement()

	// SQL3 arrives: SQL2's closing boundary must fail inside the rewrite
	// worker's provenance shadow.
	txn.StartStatement()
	err := txn.IncrStatementID(ctx, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "pk-move rewrite shadow")

	// Before any rollback, the failure must have left NOTHING published:
	// the staged state of both acknowledged statements is byte-stable.
	require.Len(t, txn.writes, 2,
		"a failed boundary must not publish carries or replacements")
	require.NotNil(t, txn.writes[0].bat)
	require.NotNil(t, txn.writes[1].bat)
	require.Equal(t, before, fsInventory(t, base),
		"a failed boundary must leave the shared store untouched")

	// The frontend reacts to the boundary error with a statement rollback
	// (default mo_rollback_txn_on_error=0). D120: the cut point is the
	// establishment offset, so ONLY the boundary's own appends would go —
	// there are none — and SQL2's tail survives verbatim.
	txn.tableCache = new(sync.Map)
	require.NoError(t, txn.RollbackLastStatement(ctx))
	require.Equal(t, 2, txn.statementID)
	require.Equal(t, []int{0, 1}, txn.offsets)
	require.Len(t, txn.writes, 2,
		"D120: the finishing statement's acknowledged writes must survive the failed boundary's rollback")
	require.Same(t, e0.bat, txn.writes[0].bat)
	require.Same(t, e1.bat, txn.writes[1].bat)
	require.Equal(t, e0Bytes, blkEntryBytes(txn.writes[0].bat),
		"SQL1's flushed entry must be byte-stable across the failed boundary")
	require.Equal(t, e1Bytes, blkEntryBytes(txn.writes[1].bat),
		"SQL2's tail write must be byte-stable across the failed boundary's rollback")

	txn.Lock()
	origin, ok := txn.lookupPkMoveKeyLineageLocked(acctTbl, "K")
	txn.Unlock()
	require.True(t, ok, "SQL2's lineage binding must survive the failed boundary")
	require.Equal(t, boundOrigin, origin)
	survivor := types.NewRowid(&origBlk, 1)
	require.Contains(t, txn.pkMoveRowidOrigins, survivor)
	require.Equal(t, types.BuildTestRowid(30, 2), txn.pkMoveRowidOrigins[survivor],
		"the staged durable origin must be intact for the retry")
	txn.EndStatement()

	// Retry the closing boundary on the SAME staged data with the
	// injection lifted: everything statement-2 staged must flow into a
	// successful compaction. No ledger repair happens here (r29 review
	// F2, D124): the failed boundary never consumed the staged deletion
	// marks, and the statement rollback dropped none of them, so the
	// retry consumes exactly the marks stageDirtyOn recorded.
	restore()
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"the retried boundary must compact the untouched staged state")
	require.Equal(t, 3, txn.statementID)
	require.Len(t, txn.writes, 3,
		"SQL1 entry (released), SQL2 tail insert, and the published replacement")
	require.Nil(t, txn.writes[0].bat,
		"a fully successful wave releases the original batch")
	require.Equal(t, e1Bytes, blkEntryBytes(txn.writes[1].bat),
		"SQL2's tail write rides the successful boundary untouched")
	rep := txn.writes[2]
	require.Equal(t, INSERT, rep.typ)
	require.NotNil(t, rep.bat)
	require.NotEmpty(t, rep.fileName)

	// The replacement re-mints the survivor's identity: the origin ledger
	// must re-key onto the replacement object.
	repCol, repArea := vector.MustVarlenaRawData(rep.bat.Vecs[1])
	require.Equal(t, 1, len(repCol))
	repStats := objectio.ObjectStats(repCol[0].GetByteSlice(repArea))
	repObj := *repStats.ObjectName().ObjectId()
	txn.Lock()
	require.Len(t, txn.pkMoveRowidOrigins, 1)
	for wsRid, originRid := range txn.pkMoveRowidOrigins {
		require.NotEqual(t, survivor, wsRid,
			"the stale workspace identity must be re-minted")
		require.Equal(t, repObj, *wsRid.BorrowObjectID(),
			"the survivor's provenance must name the replacement object")
		require.Equal(t, types.BuildTestRowid(30, 2), originRid,
			"the durable origin value survives the re-key")
	}
	txn.Unlock()
	txn.EndStatement()

	// A trailing boundary on the healthy transaction stays clean. The
	// successful wave scheduled the superseded original for async GC:
	// once it lands, the inventory = before - original + replacement,
	// with every earlier file's content untouched.
	waitFileGone(t, base, origName)
	after := fsInventory(t, base)
	beforeKept := make(map[string]uint64, len(before))
	for name, digest := range before {
		if name != origName {
			beforeKept[name] = digest
		}
	}
	newName := singleNewDigestName(t, beforeKept, after)
	require.Equal(t, len(beforeKept)+1, len(after))
	for name, digest := range beforeKept {
		require.Equal(t, digest, after[name],
			"no earlier surviving file may be rewritten by the boundary cycle")
	}
	require.NotEmpty(t, newName)
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false))
	txn.EndStatement()
}

// TestPkMoveCompactionCleanSiblingSurvivesFailedWave pins r28 review F2
// (D121): a clean object sharing a flushed batch with a dirty one must
// not be re-registered before the wave succeeds, and the failed wave's
// statement rollback must never reclaim files the retained original
// entries still reference.
//
// On the pre-r28 shape the carry registered immediately during the scan,
// so the first require.Len(writes, 1) fails; had it not, the rollback's
// range GC (live pool) would delete the clean sibling's file while the
// original entry still names it.
func TestPkMoveCompactionCleanSiblingSurvivesFailedWave(t *testing.T) {
	proc, _, base := newR28SharedProc(t)
	fx := newR28Fixture(t, proc)
	ctx := context.Background()

	txn, tbl := fx.newTxnTbl(t)
	expectMockBoundaryOps(txn)

	// SQL1: ONE flushed entry carrying clean objA and dirty objB.
	statsA := fx.writeObj(t, tbl, []int64{10, 11})
	statsB := fx.writeObj(t, tbl, []int64{20, 21, 22})
	require.Len(t, statsA, 1)
	require.Len(t, statsB, 1)
	nameA := statsA[0].ObjectName().String()
	nameB := statsB[0].ObjectName().String()

	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false))
	txn.appendWorkspaceEntryLocked(fx.flushedEntry(t,
		statsA[0].ObjectLocation().String(), statsA[0], statsB[0]))
	txn.EndStatement()
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false))
	require.Len(t, txn.writes, 1)

	// SQL2's closing boundary: objB is dirty (row 0 deleted, survivor
	// provenance-named), objA is a clean sibling of the same scanned
	// batch. The severed pk injects the worker failure.
	_, restore := fx.stageDirtyOn(t, txn, tbl, statsB[0], txn.writes[0].bat)
	objIdA := *statsA[0].ObjectName().ObjectId()
	txn.cnObjsSummary[objIdA] = Summary{
		objBat: txn.writes[0].bat, accountId: 1, databaseId: 7, tableId: 42,
		tbName: "tbl", dbName: "db",
	}
	before := fsInventory(t, base)
	txn.EndStatement()

	txn.StartStatement()
	err := txn.IncrStatementID(ctx, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "pk-move rewrite shadow")

	require.Len(t, txn.writes, 1,
		"D121: a clean-object carry must NOT be registered before the wave succeeds")
	require.NotNil(t, txn.writes[0].bat,
		"the original clean+dirty batch must survive the failed wave")
	require.Equal(t, before, fsInventory(t, base),
		"the failed wave leaves every file — including the clean sibling — untouched")

	// The frontend-equivalent statement rollback must reclaim NOTHING the
	// original entry still references: objA's file is named by the
	// retained original batch, so the range-GC exclusion keeps it alive.
	txn.tableCache = new(sync.Map)
	require.NoError(t, txn.RollbackLastStatement(ctx))
	require.Len(t, txn.writes, 1)
	require.NotNil(t, txn.writes[0].bat)
	require.Equal(t, before, fsInventory(t, base),
		"rollback must not delete the clean sibling's file while the original entry references it")

	// Retry with the injection lifted: the carry for objA and the
	// replacement for objB publish atomically, carry first. The staged
	// deletion marks survived the failed boundary and its rollback (r29
	// review F2, D124), so the retry re-consumes them without repair.
	restore()
	txn.EndStatement()
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false))
	require.Len(t, txn.writes, 3,
		"original (released) + clean carry + dirty replacement")
	require.Nil(t, txn.writes[0].bat)

	carry := txn.writes[1]
	require.Equal(t, INSERT, carry.typ)
	require.NotNil(t, carry.bat)
	require.Equal(t, nameA, carry.fileName,
		"the clean carry re-registers objA's own file")
	require.Equal(t, 1, carry.bat.RowCount())

	repB := txn.writes[2]
	require.Equal(t, INSERT, repB.typ)
	require.NotNil(t, repB.bat)
	require.NotEmpty(t, repB.fileName)
	require.NotEqual(t, nameB, repB.fileName)
	txn.EndStatement()

	// Read/commit verification: the superseded dirty original is evicted
	// by async GC; everything else keeps its content, the replacement
	// adds exactly one file, and a trailing boundary is clean.
	waitFileGone(t, base, nameB)
	after := fsInventory(t, base)
	require.Contains(t, after, nameA,
		"the clean sibling's file must still be readable after the whole cycle")
	beforeKept := make(map[string]uint64, len(before))
	for name, digest := range before {
		if name != nameB {
			beforeKept[name] = digest
		}
	}
	newName := singleNewDigestName(t, beforeKept, after)
	require.Equal(t, len(beforeKept)+1, len(after))
	for name, digest := range beforeKept {
		require.Equal(t, digest, after[name],
			"the clean sibling and every other earlier file keep their content")
	}
	require.NotEmpty(t, newName)
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false))
	txn.EndStatement()
}

// TestPkMoveFailedWaveCleanupOwnsWriterOrphans pins r28 review F3 (D122),
// deletion-failure half: when the writer's own reclaim of a persisted
// replacement fails transiently, the exact inventory transfers through
// the compaction slot into the failed-wave cleanup, so no unpublished
// file is left without a cleanup owner. On the pre-r28 shape the writer
// discarded the inventory on failure and nothing owned the file, so the
// inventory assertion fails with the replacement left behind.
func TestPkMoveFailedWaveCleanupOwnsWriterOrphans(t *testing.T) {
	ctx := context.Background()
	proc, inj, base := newR28SharedProc(t)
	fx := newR28Fixture(t, proc)

	txn, tbl := fx.newTxnTbl(t)
	origStats := fx.writeObj(t, tbl, []int64{1, 2, 3})
	require.Len(t, origStats, 1)
	txn.appendWorkspaceEntryLocked(fx.flushedEntry(t,
		origStats[0].ObjectLocation().String(), origStats[0]))
	_, restore := fx.stageDirtyOn(t, txn, tbl, origStats[0], txn.writes[0].bat)
	// The healthy table definition is irrelevant here: the failure is
	// injected at the writer's own persist, before any shadow work.
	restore()
	before := fsInventory(t, base)

	// Commit-ambiguous persist + a transiently failing first delete: the
	// writer's own reclaim (D122 defer, cancellation-decoupled) fails
	// once, so the exact inventory must transfer through the compaction
	// slot into the failed-wave cleanup, which finishes the reclaim.
	reqCtx, cancel := context.WithCancel(ctx)
	inj.writeCancelOnce = cancel
	inj.failDeleteTimes = 1

	txn.Lock()
	err := txn.compactDeletionOnObjsLocked(reqCtx)
	txn.Unlock()
	require.Error(t, err)
	require.Contains(t, err.Error(), "context canceled")

	require.Len(t, txn.writes, 1)
	require.NotNil(t, txn.writes[0].bat)
	require.Equal(t, before, fsInventory(t, base),
		"D122: every unpublished replacement file is reclaimed — first by the writer defer, then by the failed-wave cleanup")

	records := inj.deleteAttempts()
	require.GreaterOrEqual(t, len(records), 2,
		"the failed in-writer delete and the failed-wave cleanup delete must both appear")
	require.True(t, records[0].injected,
		"the first delete is the writer defer's transiently failing reclaim")
	for _, rec := range records[1:] {
		require.False(t, rec.injected)
		require.NoError(t, rec.ctxErr,
			"the failed-wave cleanup must run on a background context")
	}
}

// TestPkMoveWriterReclaimContracts pins the writer-level interface
// contracts D122 relies on. On the pre-fix production shape — the defer
// calling DeletePersisted with the request context and discarding the
// returned inventory — these are exactly the leak mechanics: a cancelled
// or transiently failing delete leaves the files in place with no owner.
func TestPkMoveWriterReclaimContracts(t *testing.T) {
	ctx := context.Background()

	writeTwoRowObj := func(t *testing.T, proc *process.Process, fx *r28Fixture,
		tbl *txnTable, pks []int64) *colexec.CNS3Writer {
		t.Helper()
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(bat.Vecs[0], pks, nil, fx.mp))
		bat.SetRowCount(len(pks))
		sharedFS, err := colexec.GetSharedFSFromProc(proc)
		require.NoError(t, err)
		w := colexec.NewCNS3DataWriterForService(
			proc.GetService(), fx.mp, sharedFS, tbl.tableDef, -1, false)
		require.NoError(t, w.Write(ctx, bat))
		_, err = w.Sync(ctx)
		require.NoError(t, err)
		bat.Clean(fx.mp)
		return w
	}

	t.Run("a cancelled context defeats the writer reclaim and keeps the files", func(t *testing.T) {
		proc, _, base := newR28SharedProc(t)
		fx := newR28Fixture(t, proc)
		_, tbl := fx.newTxnTbl(t)

		w := writeTwoRowObj(t, proc, fx, tbl, []int64{5, 6})
		before := fsInventory(t, base)

		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		files, dErr := w.DeletePersisted(cancelled)
		require.Error(t, dErr,
			"pinning the pre-D122 hazard: the request context reaches the store delete")
		require.NotEmpty(t, files,
			"the failed reclaim returns the exact ownership snapshot")
		require.Equal(t, before, fsInventory(t, base),
			"under the cancelled context the files remain — the leak D122 closes in the defer")

		// The same reclaim with a cancellation-free context succeeds, so
		// the hazard is the context, not the store.
		retry, rErr := w.DeletePersisted(context.Background())
		require.NoError(t, rErr)
		require.NotEmpty(t, retry)
		w.Close()
		require.Empty(t, fsInventory(t, base))
	})

	t.Run("a transient delete failure retains the ownership snapshot for retry", func(t *testing.T) {
		proc, inj, base := newR28SharedProc(t)
		fx := newR28Fixture(t, proc)
		_, tbl := fx.newTxnTbl(t)

		w := writeTwoRowObj(t, proc, fx, tbl, []int64{7, 8})
		before := fsInventory(t, base)
		require.NotEmpty(t, before)

		inj.failDeleteTimes = 1
		files, dErr := w.DeletePersisted(context.Background())
		require.Error(t, dErr, "the transient delete failure must surface")
		require.NotEmpty(t, files,
			"the exact ownership snapshot is returned for the caller to retain")

		retry, rErr := w.DeletePersisted(context.Background())
		require.NoError(t, rErr)
		require.NotEmpty(t, retry)
		w.Close()
		require.Empty(t, fsInventory(t, base),
			"the retained snapshot lets the caller finish the reclaim")
	})
}

// TestRollbackLastStatementSurfacesCarryUnwindFailure pins the r43
// review's third finding (F3) on the full statement-rollback path:
// RollbackLastStatement joined the carry-journal unwind failure into
// pkMoveErr and then OVERWROTE the accumulator with
// rollbackPkMoveStmtStateLocked's result, so a failed staged-cell
// inverse vanished behind the (successfully empty) statement-undo
// replay and the frontend heard success while a stale re-minted
// identity stayed staged. Pre-fix this test fails at the
// require.ErrorIs below: the swallowed unwind returns nil.
func TestRollbackLastStatementSurfacesCarryUnwindFailure(t *testing.T) {
	proc, _, _ := newR28SharedProc(t)
	fx := newR28Fixture(t, proc)
	ctx := context.Background()

	txn, _ := fx.newTxnTbl(t)
	expectMockBoundaryOps(txn)

	o, n := pkMoveJournalRid(60), pkMoveJournalRid(61)
	probe := pkMoveJournalRid(90)

	// Statement 1's staged state exactly as its completed carry round
	// left it: the staged link's old-row cell holds the re-minted id n,
	// the origin ledger value carries n, and the sequenced journal
	// entry records the round.
	entry := pkMovePruneLinkEntry(t, fx.mp, 7,
		[]uint64{42},
		[]*types.Rowid{&n},
		[]*types.Rowid{nil},
		[]uint64{700})
	txn.appendWorkspaceEntryLocked(entry)
	txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{probe: n}
	txn.pkMoveCarryJournal = []pkMoveCarryJournalEntry{{
		seq:    1,
		kind:   pkMoveCarryObject,
		oldIds: []types.Rowid{o},
		newIds: []types.Rowid{n},
	}}
	txn.statementID = 1
	// The cut lands before the unwind: offsets[0] retains the staged
	// link entry wholesale so the inverse has a live cell to restore.
	txn.offsets = []int{1}

	// Fail the unwind's first staged-cell inverse. With no recorded
	// statement-undo work, rollbackPkMoveStmtStateLocked returns nil —
	// exactly the accumulator-overwrite shape that hid the unwind
	// failure before F3.
	arm, _ := armPkMoveOverwriteFailAfter(t, 0)
	arm()
	txn.tableCache = new(sync.Map)
	txn.deletedBlocks = &deletedBlocks{offsets: map[types.Blockid][]int64{}}
	require.ErrorIs(t, txn.RollbackLastStatement(ctx), errPkMoveInjected,
		"a failed carry unwind must reach the statement-rollback caller instead of vanishing behind the statement-undo replay")
	require.Empty(t, txn.pkMoveCarryJournal,
		"the consumed entry leaves the journal even on failure: no later retry may replay the inverse onto already-restored maps")
}
