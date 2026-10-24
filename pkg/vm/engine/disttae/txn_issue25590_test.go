// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"fmt"
	"os"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	colexecdeletion "github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	colexecinsert "github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
)

// Issue 25590: repeatedly updating the same primary-key row as separate
// statements in one explicit transaction degrades super-linearly, because
// every superseded in-txn row leaves a fully-shrunk (zero-row) workspace
// entry in txn.writes. Reads, statement merging, tombstone collection and
// workspace-resident delete resolution all rescan txn.writes per statement,
// so len(txn.writes) must stay proportional to the live working set instead
// of growing one inert entry per repeated mutation.

const (
	issue25590DBID    = uint64(7)
	issue25590TableID = uint64(42)
)

// newIssue25590TxnForTest builds a Transaction that behaves like the one
// NewTransaction builds for real statements: workspace-segment rowid
// allocator, delete/selection bookkeeping, and dump thresholds far above
// the tiny working set so statement boundaries never spill to S3. stmts is
// the exact number of IncrStatementID calls the test will make.
func newIssue25590TxnForTest(t *testing.T, stmts int) (*Transaction, *process.Process) {
	t.Helper()
	op := newTxnOperatorForTest(t)
	op.EXPECT().EnterIncrStmt().Times(stmts)
	op.EXPECT().ExitIncrStmt().Times(stmts)
	op.EXPECT().EnterRollbackStmt().AnyTimes()
	op.EXPECT().ExitRollbackStmt().AnyTimes()

	proc := newProcForTest(t)
	txn := &Transaction{
		proc:            proc,
		op:              op,
		engine:          &Engine{},
		tablesInVain:    make(map[uint64]int),
		batchSelectList: make(map[*batch.Batch][]int64),
		deletedBlocks:   &deletedBlocks{offsets: make(map[types.Blockid][]int64)},
		tableCache:      new(sync.Map),
		tableOps:        newTableOps(),
		databaseOps:     newDbOps(),
	}
	txn.currentRowId.SetSegment(colexec.TxnWorkspaceSegment)
	txn.writeWorkspaceThreshold = 1 << 40
	txn.commitWorkspaceThreshold = 1 << 40
	return txn, proc
}

func newProcForTest(t *testing.T) *process.Process {
	return testutil.NewProc(t)
}

// issue25590InsertBatch allocates a one-row INSERT batch carrying a fresh
// workspace rowid, the way WriteBatch does for raw CN inserts.
func issue25590InsertBatch(t *testing.T, txn *Transaction, v int64) *batch.Batch {
	t.Helper()
	rowidVec, err := txn.batchAllocNewRowIds(1)
	require.NoError(t, err)
	defer rowidVec.Free(txn.proc.Mp())

	bat := batch.NewWithSize(2)
	bat.Attrs = []string{objectio.PhysicalAddr_Attr, "v"}
	rowids := vector.MustFixedColNoTypeCheck[types.Rowid](rowidVec)
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], rowids[0], false, txn.proc.Mp()))
	bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[1], v, false, txn.proc.Mp()))
	bat.SetRowCount(1)
	return bat
}

// runIssue25590Statements executes n UPDATE-shaped statements against the
// workspace. Each statement mirrors the production statement lifecycle the
// frontend/compile drive for a user statement:
//
//	StartStatement, IncrStatementID (admission: merges the previous
//	statement's delete markings and records the boundary),
//	UpdateSnapshotWriteOffset (compile.go txnOffsetOfCompile publishes the
//	boundary BEFORE statement execution, and txnTable.Write dumps from that
//	offset), execution (delete of the previous in-txn version via
//	deleteTableWrites — exactly what txnTable.Delete does for a row inserted
//
// by an earlier statement — followed by the next version's insert), then
//
//	EndStatement.
//
// The final pending markings are applied by a closing admission exactly the
// way production does it: Commit calls IncrStatementID(ctx, true) first, and
// any next statement would admit the same way (txn.go Transaction.Commit).
//
// prevBat holds the previous statement's INSERT entry so the delete marks
// reach it the same way deleteTableWrites' workspace scan finds it.
func runIssue25590Statements(
	t *testing.T,
	txn *Transaction,
	start int64,
	n int,
	recording []time.Duration,
	prevBat *batch.Batch,
) *batch.Batch {
	t.Helper()
	ctx := context.Background()
	admit := func() {
		txn.StartStatement()
		require.NoError(t, txn.IncrStatementID(ctx, false))
		txn.UpdateSnapshotWriteOffset()
	}
	for i := 0; i < n; i++ {
		newV := start + int64(i)
		begin := time.Now()

		admit()

		if prevBat != nil {
			prevRowids := vector.MustFixedColWithTypeCheck[types.Rowid](prevBat.GetVector(0))
			prevRowid := prevRowids[0]
			deleteBlkId := map[types.Blockid]bool{prevRowid.CloneBlockID(): true}
			rows := map[types.Rowid]uint8{prevRowid: 0}
			sels := vector.GetSels()
			txn.deleteTableWrites(issue25590DBID, issue25590TableID, sels, deleteBlkId,
				prevRowid.GetRowOffset(), prevRowid.GetRowOffset(), rows)
			vector.PutSels(sels)
		}

		insert := issue25590InsertBatch(t, txn, newV)
		txn.appendWorkspaceEntryLocked(Entry{
			typ:          INSERT,
			databaseId:   issue25590DBID,
			tableId:      issue25590TableID,
			databaseName: "db",
			tableName:    "tbl",
			bat:          insert,
		})
		prevBat = insert

		txn.EndStatement()

		if recording != nil {
			recording[i] = time.Since(begin)
		}
	}

	// Closing admission: applies the last statement's markings, the way
	// Commit (or the next statement's admission) does in production.
	admit()
	txn.EndStatement()

	return prevBat
}

// liveIssue25590Rows returns the sum of live INSERT rows currently held for
// the table by the workspace; superseded versions must have been shrunk away.
func liveIssue25590Rows(t *testing.T, txn *Transaction) (rows int64, emptyEntries int) {
	t.Helper()
	for i := range txn.writes {
		e := &txn.writes[i]
		if e.bat == nil {
			continue
		}
		require.Equal(t, issue25590DBID, e.databaseId)
		require.Equal(t, issue25590TableID, e.tableId)
		if e.bat.RowCount() == 0 {
			emptyEntries++
			continue
		}
		rows += int64(e.bat.RowCount())
	}
	return rows, emptyEntries
}

// TestIssue25590RepeatedSameRowUpdateKeepsWorkspaceBounded drives the issue's
// workload at the workspace level: one long transaction whose statements
// repeatedly supersede the same row. The structural invariant is
// len(txn.writes): with the fix it stays proportional to the live working
// set (O(1) here); without it the workspace grows one empty entry per
// statement and every statement-boundary/read scan degrades with history.
func issue25590StmtCount(t *testing.T, dflt int) int {
	t.Helper()
	if v := os.Getenv("ISSUE25590_STMTS"); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil && n > 0 {
			return n
		}
	}
	return dflt
}

func TestIssue25590RepeatedSameRowUpdateKeepsWorkspaceBounded(t *testing.T) {
	stmts := issue25590StmtCount(t, 2000)
	// every loop statement admits once, plus the closing admission
	txn, proc := newIssue25590TxnForTest(t, stmts+1)
	defer func() {
		for i := range txn.writes {
			if txn.writes[i].bat != nil {
				txn.writes[i].bat.Clean(proc.Mp())
			}
		}
	}()

	durations := make([]time.Duration, stmts)
	runIssue25590Statements(t, txn, 1, stmts, durations, nil)

	logIssue25590Timing(t, "workspace-level update loop", durations)

	liveRows, emptyEntries := liveIssue25590Rows(t, txn)
	t.Logf("after %d same-row update statements: workspace entries=%d, live rows=%d, zero-row entries=%d",
		stmts, len(txn.writes), liveRows, emptyEntries)
	require.Equal(t, int64(1), liveRows,
		"exactly one live version of the row may remain")
	require.Empty(t, txn.batchSelectList,
		"statement boundaries must consume all delete selections")
	require.Zero(t, emptyEntries,
		"superseded versions must not leave zero-row entries behind")

	// The workspace must stay bounded by the live working set, not by the
	// number of statements that touched it. Live entries: the one live
	// INSERT. Allow a small constant for bookkeeping entries.
	require.LessOrEqual(t, len(txn.writes), 4,
		"txn.writes must stay proportional to the live working set, got %d entries after %d same-row updates",
		len(txn.writes), stmts)

	// No nil slots either: retiring must also compact them out.
	for i := range txn.writes {
		require.NotNil(t, txn.writes[i].bat,
			"retired entries must be compacted out of the workspace")
	}

	// The surviving entry must hold the newest version.
	var lastV int64
	for i := range txn.writes {
		if e := txn.writes[i]; e.bat != nil && e.bat.RowCount() > 0 {
			lastV = vector.MustFixedColWithTypeCheck[int64](e.bat.GetVector(1))[0]
		}
	}
	require.Equal(t, int64(stmts), lastV, "the live version must be the newest write")
}

// TestIssue25590WorkspaceReadsSeeOnlyLiveVersion keeps the deleted-block and
// selection bookkeeping honest across the production admission order: the
// reader-visible state (the ForEachTableWrites window the workspace reader
// machinery uses) must expose exactly the newest version.
func TestIssue25590WorkspaceReadsSeeOnlyLiveVersion(t *testing.T) {
	const stmts = 50
	// every loop statement admits once, plus the closing admission
	txn, proc := newIssue25590TxnForTest(t, stmts+1)
	defer func() {
		for i := range txn.writes {
			if txn.writes[i].bat != nil {
				txn.writes[i].bat.Clean(proc.Mp())
			}
		}
	}()

	runIssue25590Statements(t, txn, 1, stmts, nil, nil)

	liveRows, emptyEntries := liveIssue25590Rows(t, txn)
	t.Logf("after %d statements: workspace entries=%d, live rows=%d, zero-row entries=%d",
		stmts, len(txn.writes), liveRows, emptyEntries)
	require.Equal(t, int64(1), liveRows)
	require.Zero(t, emptyEntries)
	require.LessOrEqual(t, len(txn.writes), 4)

	// The reader-visible window — the same ForEachTableWrites scan the
	// workspace read pipeline (filterInMemUnCommittedInserts) consumes —
	// must expose exactly one row carrying the newest version.
	visibleVals := issue25590VisibleValuesLocked(t, txn)
	require.Equal(t, []int64{stmts}, visibleVals,
		"workspace reads must see only the live version of the row")
}

// issue25590VisibleValuesLocked enumerates the reader-visible uncommitted
// INSERT rows for the issue-25590 table through ForEachTableWrites with the
// published snapshot offset, mirroring what the local datasource's in-memory
// uncommitted insert filter drains.
func issue25590VisibleValuesLocked(t *testing.T, txn *Transaction) []int64 {
	return issue25590VisibleValuesForTableLocked(t, txn, issue25590TableID)
}

// issue25590VisibleValuesForTableLocked collects the in-memory INSERT rows a
// workspace reader sees for the given table (file entries excluded, the way
// the reader treats superseded-then-dumped data).
func issue25590VisibleValuesForTableLocked(t *testing.T, txn *Transaction, tableID uint64) []int64 {
	t.Helper()
	var vals []int64
	txn.ForEachTableWrites(issue25590DBID, tableID, txn.GetSnapshotWriteOffset(),
		func(e Entry) {
			if e.bat == nil || e.typ != INSERT || e.bat.RowCount() == 0 || e.fileName != "" {
				return
			}
			vals = append(vals, vector.MustFixedColWithTypeCheck[int64](e.bat.GetVector(1))...)
		})
	return vals
}

// TestIssue25590RollbackAfterRetiredHistory exercises statement rollback once
// retired entries exist: incurs IncrStatementID (which may compact), writes
// a failing statement's entries, rolls the statement back, and requires the
// pre-failure version to be the live one with boundaries still intact.
func TestIssue25590RollbackAfterRetiredHistory(t *testing.T) {
	const prelude = 300
	// prelude closing admission + failing statement admission + follow-up
	// statement admission + follow-up closing admission
	txn, proc := newIssue25590TxnForTest(t, prelude+4)
	defer func() {
		for i := range txn.writes {
			if txn.writes[i].bat != nil {
				txn.writes[i].bat.Clean(proc.Mp())
			}
		}
	}()

	runIssue25590Statements(t, txn, 1, prelude, nil, nil)

	// Failing statement: full production admission first (boundary merged
	// and published), then its writes, then the statement rolls back.
	ctx := context.Background()
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false))
	txn.UpdateSnapshotWriteOffset()
	var liveEntry int
	for i := range txn.writes {
		if e := txn.writes[i]; e.bat != nil && e.bat.RowCount() > 0 {
			liveEntry = i
			break
		}
	}
	liveBat := txn.writes[liveEntry].bat
	liveRowid := vector.MustFixedColWithTypeCheck[types.Rowid](liveBat.GetVector(0))[0]
	deleteBlkId := map[types.Blockid]bool{liveRowid.CloneBlockID(): true}
	rows := map[types.Rowid]uint8{liveRowid: 0}
	sels := vector.GetSels()
	txn.deleteTableWrites(issue25590DBID, issue25590TableID, sels, deleteBlkId,
		liveRowid.GetRowOffset(), liveRowid.GetRowOffset(), rows)
	vector.PutSels(sels)
	bat := issue25590InsertBatch(t, txn, prelude+1)
	txn.appendWorkspaceEntryLocked(Entry{
		typ:          INSERT,
		databaseId:   issue25590DBID,
		tableId:      issue25590TableID,
		databaseName: "db",
		tableName:    "tbl",
		bat:          bat,
	})

	require.NoError(t, txn.RollbackLastStatement(ctx))
	txn.EndStatement()
	txn.UpdateSnapshotWriteOffset()

	// The rolled-back statement's selections must be dropped and the
	// pre-failure version must be the live one.
	require.Empty(t, txn.batchSelectList)
	liveRows, _ := liveIssue25590Rows(t, txn)
	require.Equal(t, int64(1), liveRows,
		"the statement's delete marks must be reverted with the rollback")
	var liveV int64
	for i := range txn.writes {
		if e := txn.writes[i]; e.bat != nil && e.bat.RowCount() > 0 {
			liveV = vector.MustFixedColWithTypeCheck[int64](e.bat.GetVector(1))[0]
		}
	}
	require.Equal(t, int64(prelude), liveV,
		"rollback must restore the pre-statement version value")
	require.LessOrEqual(t, len(txn.writes), 4)

	// A follow-up statement must still work: boundaries survive the
	// rollback and the compaction: the rollback cleared the failed
	// statement's marks and truncated its appended entry, so liveBat is
	// still the live pre-failure version's batch.
	liveBat = runIssue25590Statements(t, txn, prelude+1, 1, nil, liveBat)
	liveRows, emptyEntries := liveIssue25590Rows(t, txn)
	require.Equal(t, int64(1), liveRows)
	require.Zero(t, emptyEntries)
	require.LessOrEqual(t, len(txn.writes), 4)
	var afterV int64
	for i := range txn.writes {
		if e := txn.writes[i]; e.bat != nil && e.bat.RowCount() > 0 {
			afterV = vector.MustFixedColWithTypeCheck[int64](e.bat.GetVector(1))[0]
		}
	}
	require.Equal(t, int64(prelude+1), afterV)
}

func logIssue25590Timing(t *testing.T, label string, durations []time.Duration) {
	t.Helper()
	q := len(durations) / 4
	if q == 0 {
		return
	}
	var first, last time.Duration
	for _, d := range durations[:q] {
		first += d
	}
	for _, d := range durations[len(durations)-q:] {
		last += d
	}
	firstMean := float64(first) / float64(q) / float64(time.Microsecond)
	lastMean := float64(last) / float64(q) / float64(time.Microsecond)
	t.Logf("%s: first-quarter mean %.1fus, last-quarter mean %.1fus, ratio %.2fx",
		label, firstMean, lastMean, lastMean/firstMean)
}

// TestIssue25590ClonedWorkspaceIssuesWorkspaceRowids pins the CloneSnapshotWS
// rowid-generator contract: a cloned workspace (txn retry path, engine test
// harness) must issue TxnWorkspaceSegment rowids like every fresh workspace.
// Without the segment, rows inserted through the clone leave the
// workspace-resident delete path (segment classification in
// deleteTableWrites/deleteBatch), so repeated same-row mutations on a cloned
// workspace cannot be superseded via batchSelectList and the engine-level
// regression test would not exercise the issue-25590 path.
func TestIssue25590ClonedWorkspaceIssuesWorkspaceRowids(t *testing.T) {
	txn, proc := newIssue25590TxnForTest(t, 0)
	defer func() {
		for i := range txn.writes {
			if txn.writes[i].bat != nil {
				txn.writes[i].bat.Clean(proc.Mp())
			}
		}
	}()

	clone := txn.CloneSnapshotWS().(*Transaction)
	defer func() {
		for i := range clone.writes {
			if clone.writes[i].bat != nil {
				clone.writes[i].bat.Clean(proc.Mp())
			}
		}
	}()

	require.True(t, clone.currentRowId.BorrowSegmentID().EQ(&colexec.TxnWorkspaceSegment),
		"a cloned workspace's rowid generator must be workspace-segmented")
}

// newIssue25590InsertBatchWithPKsForTest builds an INSERT batch with fresh
// workspace-segment rowids and explicit int64 PK values, the shape
// txnTable.Write appends for raw CN inserts.
func newIssue25590InsertBatchWithPKsForTest(t *testing.T, txn *Transaction, pks []int64) *batch.Batch {
	t.Helper()
	rowidVec, err := txn.batchAllocNewRowIds(len(pks))
	require.NoError(t, err)
	defer rowidVec.Free(txn.proc.Mp())
	rowids := vector.MustFixedColNoTypeCheck[types.Rowid](rowidVec)

	bat := batch.NewWithSize(2)
	bat.SetAttributes([]string{objectio.PhysicalAddr_Attr, "pk"})
	rowidCol := vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixedList(rowidCol, rowids, nil, txn.proc.Mp()))
	bat.Vecs[0] = rowidCol
	pkCol := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(pkCol, pks, nil, txn.proc.Mp()))
	bat.Vecs[1] = pkCol
	bat.SetRowCount(len(pks))
	return bat
}

// issue25590AdmitBoundary drives the production statement admission: the
// frontend's StartStatement and IncrStatementID, then the boundary
// publication compile performs before statement execution
// (txnOffsetOfCompile).
func issue25590AdmitBoundary(t *testing.T, txn *Transaction, ctx context.Context) {
	t.Helper()
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false))
	txn.UpdateSnapshotWriteOffset()
}

// issue25590AppendInsertEntry appends an INSERT entry for the issue-25590
// fixture table carrying the given rows.
func issue25590AppendInsertEntry(t *testing.T, txn *Transaction, pks ...int64) *batch.Batch {
	t.Helper()
	bat := newIssue25590InsertBatchWithPKsForTest(t, txn, pks)
	txn.appendWorkspaceEntryLocked(Entry{
		typ:          INSERT,
		accountId:    1,
		databaseId:   issue25590DBID,
		tableId:      issue25590TableID,
		databaseName: "db",
		tableName:    "tbl",
		bat:          bat,
	})
	return bat
}

// issue25590MarkRows marks the batch rows at the given positions for
// deletion the way txnTable.Delete does for rows this transaction inserted:
// deleteTableWrites selects by rowid inside the marked rows' block and
// appends the selection to batchSelectList.
func issue25590MarkRows(t *testing.T, txn *Transaction, bat *batch.Batch, idxs ...int) {
	t.Helper()
	all := vector.MustFixedColWithTypeCheck[types.Rowid](bat.GetVector(0))
	deleteBlkId := map[types.Blockid]bool{all[0].CloneBlockID(): true}
	rows := make(map[types.Rowid]uint8, len(idxs))
	min, max := all[idxs[0]].GetRowOffset(), all[idxs[0]].GetRowOffset()
	for _, i := range idxs {
		rows[all[i]] = 0
		if all[i].GetRowOffset() < min {
			min = all[i].GetRowOffset()
		}
		if all[i].GetRowOffset() > max {
			max = all[i].GetRowOffset()
		}
	}
	sels := vector.GetSels()
	txn.deleteTableWrites(issue25590DBID, issue25590TableID, sels, deleteBlkId, min, max, rows)
	vector.PutSels(sels)
}

//   - a batch shrunk only PARTIALLY must never be retired (the value the
//     reviewer scenario guards: retirement keys off RowCount()==0 only);
//   - a mid-statement S3 dump issued from the statement's published boundary
//     (the txnTable.Write path) must only flush this statement's own
//     entries and must not compact prior statements' writes;
//   - a statement rollback after that mid-statement dump must restore the
//     exact pre-statement workspace: retained writes, reader visibility
//     and accounting all intact;
//   - the next boundary retires the now fully-superseded batch and compacts
//     it out while the flushed entry keeps its slot.
func TestIssue25590RetirementKeepsSurvivingEntriesAcrossDumpAndRollback(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	tbl := txn.tableOps.existAndActive(genTableKey(1, "tbl", 7, "db"))
	require.NotNil(t, tbl)
	// the dump path needs the real int64 pk type on the resolved table
	tbl.tableDef.Cols[0].Typ = pbplan.Type{Id: int32(types.T_int64)}
	txn.tnStores = []DNStore{{}}
	txn.tableCache = new(sync.Map)
	txn.batchSelectList = make(map[*batch.Batch][]int64)
	txn.tablesInVain = make(map[uint64]int)
	txn.deletedBlocks = &deletedBlocks{offsets: make(map[types.Blockid][]int64)}
	txn.cnObjsSummary = make(map[types.Objectid]Summary)
	txn.cn_flushed_s3_tombstone_object_stats_list = new(sync.Map)
	txn.currentRowId.SetSegment(colexec.TxnWorkspaceSegment)
	// thresholds far above the working set: admissions never dump on their
	// own; the one flush below is issued explicitly from the statement's
	// published boundary, exactly where txnTable.Write triggers one.
	txn.writeWorkspaceThreshold = 1 << 40
	txn.commitWorkspaceThreshold = 1 << 40

	op, ok := txn.op.(*mock_frontend.MockTxnOperator)
	require.True(t, ok, "fixture must expose the mock operator")
	op.EXPECT().EnterIncrStmt().AnyTimes()
	op.EXPECT().ExitIncrStmt().AnyTimes()
	op.EXPECT().EnterRollbackStmt().AnyTimes()
	op.EXPECT().ExitRollbackStmt().AnyTimes()

	defer func() {
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	}()

	ctx := context.Background()
	admit := func() { issue25590AdmitBoundary(t, txn, ctx) }
	appendInsert := func(pks ...int64) *batch.Batch {
		return issue25590AppendInsertEntry(t, txn, pks...)
	}
	mark := func(bat *batch.Batch, idxs ...int) { issue25590MarkRows(t, txn, bat, idxs...) }

	// Statement 1: insert three rows in one batch (pks 1, 2, 3).
	admit()
	batchA := appendInsert(1, 2, 3)
	txn.EndStatement()

	// Statement 2: partially supersede batchA (two of its three rows) and
	// write batchC, then flush FROM THIS STATEMENT'S PUBLISHED BOUNDARY the
	// way txnTable.Write does (dumpBatch(ctx, GetSnapshotWriteOffset()) at
	// txn_table.go): only this statement's own batchC may leave the
	// workspace memory.
	admit()
	mark(batchA, 0, 1)
	appendInsert(4)
	fs, err := colexec.GetSharedFSFromProc(txn.proc)
	require.NoError(t, err)
	var pkCount int
	txn.Lock()
	err = txn.dumpInsertBatchLocked(ctx, fs, txn.GetSnapshotWriteOffset(), &pkCount)
	txn.Unlock()
	require.NoError(t, err)
	require.Equal(t, 1, pkCount, "the statement's own insert must be flushed")
	require.Len(t, txn.writes, 2, "dump must keep the flushed entry and statement 1's batch")
	require.Same(t, batchA, txn.writes[0].bat)
	txn.EndStatement()

	// Statement 3 admission applies statement 2's marks: batchA shrinks but
	// MUST SURVIVE with its remaining row — partial shrinks are never
	// retired — and no slot compaction may shift the flushed entry out.
	admit()
	require.Equal(t, 1, batchA.RowCount(), "partially superseded batches must survive")
	require.Equal(t, []int64{3}, issue25590VisibleValuesLocked(t, txn),
		"rollback reader window: the surviving row and only it")
	require.Empty(t, txn.batchSelectList)
	require.Len(t, txn.writes, 2)
	require.NoError(t, txn.checkWorkspaceAccountingLocked())

	// Statement 3 now fully supersedes batchA and inserts batchD, then
	// fails: the rollback must drop this statement's batch and marks,
	// restoring the exact post-statement-2 workspace.
	mark(batchA, 0)
	appendInsert(5)
	require.Len(t, txn.writes, 3)
	require.NoError(t, txn.RollbackLastStatement(ctx))
	txn.EndStatement()

	require.Len(t, txn.writes, 2, "rollback must restore the pre-statement writes")
	require.Same(t, batchA, txn.writes[0].bat, "the partially-shrunk batch must be retained")
	require.Equal(t, 1, batchA.RowCount())
	require.Empty(t, txn.batchSelectList, "the failed statement's marks must be discarded")
	require.NotEmpty(t, txn.writes[1].fileName, "the flushed entry must be retained")
	require.Equal(t, 1, txn.writes[1].bat.RowCount())
	require.Equal(t, catalog.BlockMeta_BlockInfo, txn.writes[1].bat.Attrs[0])
	require.Equal(t, []int64{3}, issue25590VisibleValuesLocked(t, txn),
		"reader visibility must be restored to the surviving row")
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
	require.Equal(t, []int{0, 1}, txn.offsets, "recorded boundaries must survive the rollback")

	// A follow-up statement proves the rolled-back workspace still works:
	// its admission retires the now fully-superseded batchA and compacts
	// the retired slot out, while the flushed entry keeps its slot and the
	// live insert survives.
	admit()
	mark(batchA, 0)
	batchD := appendInsert(6)
	txn.EndStatement()
	admit()
	txn.EndStatement()

	require.Len(t, txn.writes, 2, "retirement must compact the fully-shrunk batch out")
	for i := range txn.writes {
		require.NotSame(t, batchA, txn.writes[i].bat,
			"the fully-superseded batch must be retired")
	}
	require.Same(t, batchD, txn.writes[1].bat)
	require.Equal(t, 1, batchD.RowCount())
	require.NotEmpty(t, txn.writes[0].fileName, "the flushed survivor keeps its slot")
	require.Equal(t, []int64{6}, issue25590VisibleValuesLocked(t, txn),
		"the new live row must be the only visible one")
	require.Empty(t, txn.batchSelectList)
	require.Zero(t, batchA.RowCount())
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
}

// TestIssue25590AdmissionFailureKeepsRollbackBoundaryExact pins admission
// bookkeeping to be failure-atomic when the statement-boundary dump fails
// after a merge retired entries. IncrStatementID must publish the new
// boundary and compaction even when its fallible dump returns an error, so
// the RollbackLastStatement the frontend issues on that error
// (pkg/frontend/txn.go's statement-rollback path) can never reuse a
// pre-compaction boundary against the shortened workspace: re-slicing past
// the live length would resurrect compacted-out duplicates (double batches,
// double-Clean).
func TestIssue25590AdmissionFailureKeepsRollbackBoundaryExact(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	tbl := txn.tableOps.existAndActive(genTableKey(1, "tbl", 7, "db"))
	require.NotNil(t, tbl)
	// the dump path needs the real int64 pk type on the resolved table
	tbl.tableDef.Cols[0].Typ = pbplan.Type{Id: int32(types.T_int64)}
	txn.tnStores = []DNStore{{}}
	txn.tableCache = new(sync.Map)
	txn.batchSelectList = make(map[*batch.Batch][]int64)
	txn.tablesInVain = make(map[uint64]int)
	txn.deletedBlocks = &deletedBlocks{offsets: make(map[types.Blockid][]int64)}
	txn.cnObjsSummary = make(map[types.Objectid]Summary)
	txn.cn_flushed_s3_tombstone_object_stats_list = new(sync.Map)
	txn.currentRowId.SetSegment(colexec.TxnWorkspaceSegment)
	// no dumps during the setup statements
	txn.writeWorkspaceThreshold = 1 << 40
	txn.commitWorkspaceThreshold = 1 << 40

	op, ok := txn.op.(*mock_frontend.MockTxnOperator)
	require.True(t, ok, "fixture must expose the mock operator")
	op.EXPECT().EnterIncrStmt().AnyTimes()
	op.EXPECT().ExitIncrStmt().AnyTimes()
	op.EXPECT().EnterRollbackStmt().AnyTimes()
	op.EXPECT().ExitRollbackStmt().AnyTimes()

	defer func() {
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	}()

	ctx := context.Background()
	admit := func() { issue25590AdmitBoundary(t, txn, ctx) }

	// Statement 1: five one-row entries (pks 1..5); statement 1's
	// admission recorded boundary 0.
	admit()
	var seed []*batch.Batch
	for pk := int64(1); pk <= 5; pk++ {
		seed = append(seed, issue25590AppendInsertEntry(t, txn, pk))
	}
	txn.EndStatement()

	// Statement 2: fully supersede the first four entries and append two
	// more (pks 6, 7). Its admission recorded boundary 5.
	admit()
	for i := 0; i < 4; i++ {
		issue25590MarkRows(t, txn, seed[i], 0)
	}
	issue25590AppendInsertEntry(t, txn, 6)
	issue25590AppendInsertEntry(t, txn, 7)
	txn.EndStatement()
	require.Len(t, txn.writes, 7)
	require.Equal(t, []int{0, 5}, txn.offsets)

	// Arm the deterministic dump table-lookup fault and the tiny
	// write threshold so the next admission's dump engages and fails.
	enableReenterSnapshotOffsetFault(t)
	txn.writeWorkspaceThreshold = 0

	// The statement 3 admission fails inside the dump — the exact shape of
	// a dump table lookup error in production. The admission must still
	// publish its bookkeeping: boundary appended and retired slots
	// compacted, so the rollback the frontend issues can never reuse the
	// stale pre-compaction boundary 5.
	txn.StartStatement()
	err := txn.IncrStatementID(ctx, false)
	require.ErrorContains(t, err, "reenter snapshot write offset")
	require.Equal(t, []int{0, 5, 3}, txn.offsets,
		"admission bookkeeping is failure-atomic: the boundary is appended even when the dump fails")
	require.Len(t, txn.writes, 3,
		"retired slots are compacted before the boundary, never after the dump")
	require.Equal(t, []int64{5, 6, 7}, issue25590VisibleValuesLocked(t, txn))
	require.Empty(t, txn.batchSelectList)
	require.NoError(t, txn.checkWorkspaceAccountingLocked())

	// Roll the failed statement back the way the frontend does: the
	// retained neighbors must survive, no compacted-out duplicate may
	// reappear, and statements must keep working afterwards.
	require.NoError(t, txn.RollbackLastStatement(ctx))
	txn.EndStatement()
	require.Len(t, txn.writes, 3,
		"rollback must not resurrect compacted-out entries")
	require.Equal(t, []int64{5, 6, 7}, issue25590VisibleValuesLocked(t, txn),
		"the retained neighboring entries stay visible")
	require.Equal(t, []int{0, 5}, txn.offsets,
		"rollback pops the failed statement's bookkeeping")
	require.Empty(t, txn.batchSelectList)
	require.NoError(t, txn.checkWorkspaceAccountingLocked())

	// Continued execution: the rolled-back workspace takes new statements
	// with exact boundaries.
	txn.writeWorkspaceThreshold = 1 << 40
	admit()
	issue25590AppendInsertEntry(t, txn, 8)
	txn.EndStatement()
	admit()
	txn.EndStatement()

	require.Len(t, txn.writes, 4)
	require.Equal(t, []int64{5, 6, 7, 8}, issue25590VisibleValuesLocked(t, txn))
	require.Empty(t, txn.batchSelectList)
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
}

// TestIssue25590MergeBranchSkipsRetiredNilSlots pins the nil-tolerance of
// the >=30-entries merge branch: shrink retirements and batch combinations
// leave nil slots behind (compaction is deferred until after the admission
// dump, issue #25590), so the merge loops and the DELETE-sort pass must
// skip nil entries instead of dereferencing them. Found live by the
// update_workspace SQL case in an mo-tester comparison run: 16 point
// updates in one txn produce enough DELETE entries for the merge branch,
// the staged merge publication nils the combined sources, and the sort
// pass panicked on their nil batches.
func TestIssue25590MergeBranchSkipsRetiredNilSlots(t *testing.T) {
	txn, proc := newIssue25590TxnForTest(t, 2)
	defer func() {
		for i := range txn.writes {
			if txn.writes[i].bat != nil {
				txn.writes[i].bat.Clean(proc.Mp())
			}
		}
	}()

	ctx := context.Background()
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false))
	txn.UpdateSnapshotWriteOffset()

	// Fifteen one-row INSERT entries, all fully superseded by the pending
	// marks, become nil slots in the admission's shrink pass; thirty-one
	// one-row DELETE entries (the shape writeTnPartition gives point
	// updates of committed rows) push the bitmap count over the merge
	// threshold, so foo() combines them into the first DELETE batch and
	// nils the combined sources.
	ins := make([]*batch.Batch, 0, 15)
	for i := 0; i < 15; i++ {
		b := issue25590AppendInsertEntry(t, txn, int64(i+1))
		ins = append(ins, b)
		issue25590MarkRows(t, txn, b, 0)
	}
	for i := 0; i < 31; i++ {
		bat := batch.NewWithSize(2)
		bat.SetAttributes([]string{objectio.PhysicalAddr_Attr, "pk"})
		rowidCol := vector.NewVec(types.T_Rowid.ToType())
		require.NoError(t, vector.AppendFixedList(rowidCol,
			[]types.Rowid{types.RandomRowid()}, nil, proc.Mp()))
		bat.Vecs[0] = rowidCol
		pkCol := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(pkCol,
			[]int64{int64(1000 + i)}, nil, proc.Mp()))
		bat.Vecs[1] = pkCol
		bat.SetRowCount(1)
		txn.appendWorkspaceEntryLocked(Entry{
			typ:          DELETE,
			accountId:    1,
			databaseId:   issue25590DBID,
			tableId:      issue25590TableID,
			databaseName: "db",
			tableName:    "tbl",
			bat:          bat,
		})
	}
	issue25590AppendInsertEntry(t, txn, 500)
	require.Len(t, txn.writes, 47)

	txn.EndStatement()

	// The admission merge must tolerate the nil slots left by shrink
	// retirements and merge combinations (the DELETE-sort pass), and the
	// post-dump compaction must drop them: the one live INSERT plus the
	// single combined DELETE batch remain.
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false))
	txn.UpdateSnapshotWriteOffset()
	txn.EndStatement()

	require.Len(t, txn.writes, 2,
		"retired and combined nil slots must be compacted out")
	require.Equal(t, []int64{500}, issue25590VisibleValuesLocked(t, txn))
	require.Empty(t, txn.batchSelectList)
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
}

// issue25590ArmDumpFault enables process-level fault injection and installs
// one of the dump-staged publication faults (issue #25590 round-3 review),
// returning a disarm function. freq follows fault.AddFaultPoint's
// "start:end:skip:prob" syntax: ":::" fires on every trigger, "2:2::" on the
// second trigger only.
func issue25590ArmDumpFault(t *testing.T, key, freq string) (disarm func()) {
	t.Helper()
	fault.Enable()
	t.Cleanup(func() {
		fault.Disable()
	})
	t.Cleanup(func() {
		fault.RemoveFaultPoint(context.Background(), key)
	})
	require.NoError(t, fault.AddFaultPoint(
		context.Background(), key, freq, "echo", 0, "", false))
	return func() {
		fault.RemoveFaultPoint(context.Background(), key)
	}
}

// issue25590InitDumpableFixture completes a fixture transaction whose
// statement admissions can run the real insert dump (S3 writer, file entry
// publication) the way TestIssue25590RetirementKeepsSurvivingEntriesAcross-
// DumpAndRollback does: pk column typing, TN stores, accounting maps and
// dump thresholds far above the working set.
func issue25590InitDumpableFixture(t *testing.T, txn *Transaction) {
	t.Helper()
	tbl := txn.tableOps.existAndActive(genTableKey(1, "tbl", 7, "db"))
	require.NotNil(t, tbl)
	// the dump path needs the real int64 pk type on the resolved table
	tbl.tableDef.Cols[0].Typ = pbplan.Type{Id: int32(types.T_int64)}
	txn.tnStores = []DNStore{{}}
	txn.tableCache = new(sync.Map)
	txn.batchSelectList = make(map[*batch.Batch][]int64)
	txn.tablesInVain = make(map[uint64]int)
	txn.deletedBlocks = &deletedBlocks{offsets: make(map[types.Blockid][]int64)}
	txn.cnObjsSummary = make(map[types.Objectid]Summary)
	txn.cn_flushed_s3_tombstone_object_stats_list = new(sync.Map)
	txn.currentRowId.SetSegment(colexec.TxnWorkspaceSegment)
	txn.writeWorkspaceThreshold = 1 << 40
	txn.commitWorkspaceThreshold = 1 << 40

	op, ok := txn.op.(*mock_frontend.MockTxnOperator)
	require.True(t, ok, "fixture must expose the mock operator")
	op.EXPECT().EnterIncrStmt().AnyTimes()
	op.EXPECT().ExitIncrStmt().AnyTimes()
	op.EXPECT().EnterRollbackStmt().AnyTimes()
	op.EXPECT().ExitRollbackStmt().AnyTimes()
}

// TestIssue25590InsertDumpWriteFailureRetainsEveryStagedSource pins the
// failure-atomic staging contract of the round-3 review: while a statement
// admission's insert dump publishes staged batches to storage (a fallible
// Write/Sync/publish), the staged sources must stay live and untouched. A
// storage failure before any publication must leave the workspace exactly
// as the successful statements produced it, so the statement rollback the
// frontend issues on the admission error, continued execution, and the
// eventual commit all keep every earlier successful statement.
func TestIssue25590InsertDumpWriteFailureRetainsEveryStagedSource(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)
	defer func() {
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	}()

	ctx := context.Background()
	admit := func() { issue25590AdmitBoundary(t, txn, ctx) }

	// Statement 1: three rows in one batch.
	admit()
	batchA := issue25590AppendInsertEntry(t, txn, 1, 2, 3)
	txn.EndStatement()

	// Statement 2: two more rows in a second batch.
	admit()
	batchB := issue25590AppendInsertEntry(t, txn, 4, 5)
	txn.EndStatement()
	require.Len(t, txn.writes, 2)

	// Statement 3's admission dump stages the two earlier statements'
	// batches and must fail before publishing anything.
	disarm := issue25590ArmDumpFault(t, objectio.FJ_CNDumpInsertWriteFailed, ":::")
	txn.writeWorkspaceThreshold = 0
	txn.StartStatement()
	err := txn.IncrStatementID(ctx, false)
	require.ErrorContains(t, err, "insert dump write failed")
	require.Equal(t, []int{0, 1, 2}, txn.offsets,
		"admission bookkeeping stays failure-atomic (issue #25590 stage 2)")
	require.Len(t, txn.writes, 2,
		"a dump that published nothing must retain every staged source")
	require.Same(t, batchA, txn.writes[0].bat)
	require.Same(t, batchB, txn.writes[1].bat)
	require.Equal(t, 3, batchA.RowCount())
	require.Equal(t, 2, batchB.RowCount())
	require.Empty(t, txn.writes[0].fileName)
	require.Empty(t, txn.writes[1].fileName)
	// Publish the reader window the way the compile step of an admitted
	// statement does; the failed statement itself never reaches execution.
	txn.UpdateSnapshotWriteOffset()
	require.Equal(t, []int64{1, 2, 3, 4, 5}, issue25590VisibleValuesLocked(t, txn),
		"both successful statements' rows must stay readable in memory")
	require.Equal(t, 5, txn.approximateInMemInsertCnt)
	require.NoError(t, txn.checkWorkspaceAccountingLocked())

	// The frontend rolls the failed statement back; every earlier row stays.
	require.NoError(t, txn.RollbackLastStatement(ctx))
	txn.EndStatement()
	require.Len(t, txn.writes, 2)
	require.Equal(t, []int{0, 1}, txn.offsets)
	require.Equal(t, []int64{1, 2, 3, 4, 5}, issue25590VisibleValuesLocked(t, txn))
	require.NoError(t, txn.checkWorkspaceAccountingLocked())

	// Continued execution: with the fault gone the retried admission
	// publishes both batches; the workspace stays healthy afterwards.
	disarm()
	admit()
	require.Len(t, txn.writes, 1,
		"both staged batches publish into a single file entry")
	require.NotEmpty(t, txn.writes[0].fileName)
	require.Equal(t, issue25590TableID, txn.writes[0].tableId)
	require.Zero(t, txn.approximateInMemInsertCnt)
	require.Empty(t, issue25590VisibleValuesLocked(t, txn))
	require.NoError(t, txn.checkWorkspaceAccountingLocked())

	// A further statement writes a new row; the next admission dumps it
	// the commit-dump way (from the beginning of the workspace), and the
	// published history stays intact.
	issue25590AppendInsertEntry(t, txn, 6)
	txn.EndStatement()
	admit()
	fileEntries := 0
	for i := range txn.writes {
		require.NotEmpty(t, txn.writes[i].fileName,
			"after a successful dump every row lives in a file entry")
		fileEntries++
	}
	require.Equal(t, 2, fileEntries)
	require.Zero(t, txn.approximateInMemInsertCnt)
	require.Empty(t, txn.batchSelectList)
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
	txn.EndStatement()
}

// secondIssue25590TableID is a second committed table of the dump fixture,
// so a dump can stage two distinct tables in one round.
const secondIssue25590TableID = uint64(43)

// TestIssue25590InsertDumpSyncFailurePartialPublication pins the partial
// multi-table publication the round-3 review demands: when an admission
// dump publishes several tables and the storage fails partway, the exactly
// one table published before the failure is retired into its file entry,
// while the failing table's staged sources are retained untouched. The
// frontend's statement rollback on the admission error, continued
// execution, and the eventual full publication must preserve every
// successful statement of BOTH tables.
func TestIssue25590InsertDumpSyncFailurePartialPublication(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)
	// The second table resolves through the same created database the
	// dump's pre-resolution pass consults.
	dbv := txn.databaseOps.existAndActive(genDatabaseKey(1, "db"))
	require.NotNil(t, dbv)
	txn.tableOps.addCreateTable(
		genTableKey(1, "tbl2", 7, "db"),
		0,
		&txnTable{
			accountId: 1,
			tableId:   secondIssue25590TableID,
			tableName: "tbl2",
			db:        dbv,
			tableDef: &pbplan.TableDef{
				Cols: []*pbplan.ColDef{{
					Name: "pk",
					Typ:  pbplan.Type{Id: int32(types.T_int64)},
				}},
				Pkey: &pbplan.PrimaryKeyDef{PkeyColName: "pk"},
			},
		},
	)
	defer func() {
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	}()

	ctx := context.Background()
	admit := func() { issue25590AdmitBoundary(t, txn, ctx) }
	appendInsert2 := func(pks ...int64) *batch.Batch {
		bat := newIssue25590InsertBatchWithPKsForTest(t, txn, pks)
		txn.appendWorkspaceEntryLocked(Entry{
			typ:          INSERT,
			accountId:    1,
			databaseId:   issue25590DBID,
			tableId:      secondIssue25590TableID,
			databaseName: "db",
			tableName:    "tbl2",
			bat:          bat,
		})
		return bat
	}

	// A successful statement whose dump will straddle two tables.
	admit()
	batchA := issue25590AppendInsertEntry(t, txn, 1, 2)
	batchC := appendInsert2(7)
	txn.EndStatement()
	require.Len(t, txn.writes, 2)

	// "2:2::": the fault fires on the second publication attempt only, so
	// the dump publishes exactly one of the two staged tables and then
	// fails. Which table publishes first follows map iteration order, so
	// every assertion below is symmetric in the two tables.
	disarm := issue25590ArmDumpFault(t, objectio.FJ_CNDumpInsertSyncFailed, "2:2::")
	txn.writeWorkspaceThreshold = 0
	txn.StartStatement()
	err := txn.IncrStatementID(ctx, false)
	require.ErrorContains(t, err, "insert dump sync failed")

	// One table's sources retired into a published file entry, the other
	// table's source retained raw — and nothing else in the workspace.
	require.Len(t, txn.writes, 2,
		"exactly the published table's source is retired; the failed table's source stays")
	publishedEntry := -1
	retainedEntry := -1
	for i := range txn.writes {
		if txn.writes[i].fileName == "" {
			retainedEntry = i
		} else {
			publishedEntry = i
		}
	}
	require.NotEqual(t, -1, publishedEntry,
		"the table published before the failure must own a file entry")
	require.NotEqual(t, -1, retainedEntry,
		"the failing table's staged source must be retained raw")
	require.Equal(t, INSERT, txn.writes[publishedEntry].typ)
	require.NotEqual(t, txn.writes[retainedEntry].tableId, txn.writes[publishedEntry].tableId,
		"the file entry belongs to the published table, the raw source to the failed one")
	retainedTable := txn.writes[retainedEntry].tableId
	retainedBat := txn.writes[retainedEntry].bat
	require.True(t, retainedTable == issue25590TableID || retainedTable == secondIssue25590TableID)
	if retainedTable == issue25590TableID {
		require.Same(t, batchA, retainedBat,
			"the retained raw source must be the failing table's untouched batch")
	} else {
		require.Same(t, batchC, retainedBat,
			"the retained raw source must be the failing table's untouched batch")
	}
	require.Empty(t, issue25590VisibleValuesForTableLocked(t, txn, txn.writes[publishedEntry].tableId),
		"the published table's rows left memory")
	// Publish the reader window the way the compile step of an admitted
	// statement does; the failed statement itself never reaches execution.
	txn.UpdateSnapshotWriteOffset()
	want := issue25590VisibleValuesForTableLocked(t, txn, retainedTable)
	if retainedTable == issue25590TableID {
		require.Equal(t, []int64{1, 2}, want,
			"the failed table's rows must stay readable in memory")
	} else {
		require.Equal(t, []int64{7}, want,
			"the failed table's rows must stay readable in memory")
	}
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
	require.Equal(t, []int{0, 2}, txn.offsets,
		"admission bookkeeping published the post-publication boundary")

	// The frontend's statement rollback keeps both halves of the history.
	require.NoError(t, txn.RollbackLastStatement(ctx))
	txn.EndStatement()
	require.Equal(t, []int{0}, txn.offsets)
	require.Len(t, txn.writes, 2,
		"rollback must keep the published file entry and the retained source")
	require.NoError(t, txn.checkWorkspaceAccountingLocked())

	// Continued execution: the retried admission publishes the retained
	// table too; then a further statement adds a row and the next
	// admissions leave the workspace fully published.
	disarm()
	admit()
	require.Len(t, txn.writes, 2, "both tables are file entries now")
	require.NotEmpty(t, txn.writes[0].fileName)
	require.NotEmpty(t, txn.writes[1].fileName)
	require.Zero(t, txn.approximateInMemInsertCnt)
	require.NoError(t, txn.checkWorkspaceAccountingLocked())

	appendInsert2(8)
	// Publish the reader window the way an owning boundary drive does, and
	// prove the new statement's row is visible before its publication.
	txn.UpdateSnapshotWriteOffset()
	require.Equal(t, []int64{8},
		issue25590VisibleValuesForTableLocked(t, txn, secondIssue25590TableID))
	txn.EndStatement()
	admit()

	metaPerTable := make(map[uint64]int)
	for i := range txn.writes {
		require.NotEmpty(t, txn.writes[i].fileName,
			"every row version must be published by the closing admission")
		metaPerTable[txn.writes[i].tableId]++
	}
	require.Equal(t, 1, metaPerTable[issue25590TableID])
	require.Equal(t, 2, metaPerTable[secondIssue25590TableID])
	require.Zero(t, txn.approximateInMemInsertCnt)
	require.Empty(t, txn.batchSelectList)
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
	txn.EndStatement()
}

// TestIssue25590DeleteDumpFailureRetainsEveryStagedSource pins the same
// failure-atomic staging contract on the tombstone dump (the commit-path
// dumpAll variant): a Write failure before any publication must retain the
// staged DELETE sources exactly, and a disarmed retry publishes them.
func TestIssue25590DeleteDumpFailureRetainsEveryStagedSource(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)
	defer func() {
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	}()

	ctx := context.Background()
	fs, err := colexec.GetSharedFSFromProc(txn.proc)
	require.NoError(t, err)

	// One in-memory DELETE entry, the shape writeTnPartition gives a point
	// delete of a committed row.
	delBat := issue25590AppendDeleteEntry(t, txn, 900)
	require.Len(t, txn.writes, 1)
	require.Equal(t, 1, txn.approximateInMemDeleteCnt)

	// The tombstone dump must fail before publishing anything and retain
	// the staged source.
	disarm := issue25590ArmDumpFault(t, objectio.FJ_CNDumpDeleteWriteFailed, ":::")
	txn.Lock()
	err = txn.dumpDeleteBatchLocked(ctx, fs, 0)
	txn.Unlock()
	require.ErrorContains(t, err, "delete dump write failed")
	require.Len(t, txn.writes, 1,
		"a failed delete dump must retain the staged tombstone source")
	require.Same(t, delBat, txn.writes[0].bat)
	require.Equal(t, 1, delBat.RowCount())
	require.Empty(t, txn.writes[0].fileName)
	require.Equal(t, 1, txn.approximateInMemDeleteCnt)
	require.NoError(t, txn.checkWorkspaceAccountingLocked())

	// The disarmed retry publishes; the in-memory tombstone leaves the
	// workspace as a published tombstone file entry.
	disarm()
	txn.Lock()
	err = txn.dumpDeleteBatchLocked(ctx, fs, 0)
	txn.Unlock()
	require.NoError(t, err)
	require.Len(t, txn.writes, 1)
	require.Equal(t, DELETE, txn.writes[0].typ)
	require.NotEmpty(t, txn.writes[0].fileName,
		"the tombstone publication replaces the in-memory source")
	require.Zero(t, txn.approximateInMemDeleteCnt)
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
}

// issue25590AppendDeleteEntry appends an in-memory DELETE entry, the shape
// writeTnPartition gives a point delete of a committed row.
func issue25590AppendDeleteEntry(t *testing.T, txn *Transaction, pk int64) *batch.Batch {
	t.Helper()
	delBat := batch.NewWithSize(2)
	delBat.SetAttributes([]string{objectio.PhysicalAddr_Attr, "pk"})
	rowidCol := vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixed(
		rowidCol, types.RandomRowid(), false, txn.proc.Mp()))
	pkValCol := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(pkValCol, pk, false, txn.proc.Mp()))
	delBat.Vecs[0] = rowidCol
	delBat.Vecs[1] = pkValCol
	delBat.SetRowCount(1)
	txn.appendWorkspaceEntryLocked(Entry{
		typ:          DELETE,
		accountId:    1,
		databaseId:   issue25590DBID,
		tableId:      issue25590TableID,
		databaseName: "db",
		tableName:    "tbl",
		bat:          delBat,
	})
	return delBat
}

// TestIssue25590DeleteDumpCloseFailureStillPublishes pins the post-recording
// edge of the same staging contract: once a tombstone dump has recorded the
// replacement file entry, a writer Close error must retire the sources
// anyway — retaining them would double-apply the tombstones at commit —
// while the dump still reports the error to its caller.
func TestIssue25590DeleteDumpCloseFailureStillPublishes(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)
	defer func() {
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	}()

	ctx := context.Background()
	fs, err := colexec.GetSharedFSFromProc(txn.proc)
	require.NoError(t, err)

	issue25590AppendDeleteEntry(t, txn, 901)

	disarm := issue25590ArmDumpFault(t, objectio.FJ_CNDumpDeleteCloseFailed, ":::")
	defer disarm()
	txn.Lock()
	err = txn.dumpDeleteBatchLocked(ctx, fs, 0)
	txn.Unlock()
	require.ErrorContains(t, err, "delete dump close failed",
		"a Close failure is still reported to the dump caller")
	require.Len(t, txn.writes, 1,
		"the replacement entry was recorded, so the source retires instead of double-applying")
	require.Equal(t, DELETE, txn.writes[0].typ)
	require.NotEmpty(t, txn.writes[0].fileName)
	require.Zero(t, txn.approximateInMemDeleteCnt)
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
}

// TestIssue25590MergeFailureKeepsPriorStatementIntact pins the round-4
// review scenario: an admission whose merge fails inside the fallible
// object-compaction lookup must stay failure-atomic like a dump failure.
// The merge retires the previous statement's fully-superseded raw batches
// first, then resolves the table for the flushed rows' pending block
// deletions; when that lookup fails, the admission still publishes its
// boundary (so the frontend statement rollback cannot truncate the
// previous statement's writes) and the pending block deletions of the
// earlier successful statement must SURVIVE the rollback instead of being
// wiped, so continuation and the eventual commit keep every earlier row
// exact.
func TestIssue25590MergeFailureKeepsPriorStatementIntact(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)
	defer func() {
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	}()

	ctx := context.Background()
	admit := func() { issue25590AdmitBoundary(t, txn, ctx) }

	// Statement 1: rows 1..3 in raw batch A, then A publishes into the
	// file entry P mid-statement (the txnTable.Write dump path); its rows
	// register under the CN uncommitted workspace segment, so a later
	// statement's deletes of them arrive as pending deletedBlocks.
	admit()
	batchA := issue25590AppendInsertEntry(t, txn, 1, 2, 3)
	require.Equal(t, 3, batchA.RowCount())
	fs, err := colexec.GetSharedFSFromProc(txn.proc)
	require.NoError(t, err)
	var pkCount int
	txn.Lock()
	err = txn.dumpInsertBatchLocked(ctx, fs, txn.GetSnapshotWriteOffset(), &pkCount)
	txn.Unlock()
	require.NoError(t, err)
	require.Equal(t, 3, pkCount)
	require.Len(t, txn.writes, 1)
	require.NotEmpty(t, txn.writes[0].fileName)
	require.Equal(t, catalog.BlockMeta_BlockInfo, txn.writes[0].bat.Attrs[0])
	// The published rows carry the object's own rowids (the reader's view),
	// not batchA's workspace rowids; block offsets stay 0..2 in order.
	infoCol, infoArea := vector.MustVarlenaRawData(txn.writes[0].bat.Vecs[0])
	require.Len(t, infoCol, 1)
	pubBlockID := objectio.DecodeBlockInfo(infoCol[0].GetByteSlice(infoArea)).BlockID
	aRowids := make([]types.Rowid, 3)
	for i := range aRowids {
		aRowids[i] = types.NewRowid(&pubBlockID, uint32(i))
	}
	batchB := issue25590AppendInsertEntry(t, txn, 4, 5)
	txn.EndStatement()
	require.Equal(t, []int{0}, txn.offsets)

	// Statement 2 deletes rows 1 and 2 of the flushed P (the
	// deletedBlocks registration path of deleteBatch), fully supersedes
	// raw batch B and writes the replacement batch C.
	admit()
	require.Equal(t, []int{0, 2}, txn.offsets)
	delBat := batch.NewWithSize(2)
	delBat.SetAttributes([]string{objectio.PhysicalAddr_Attr, "pk"})
	rowidVec := vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixedList(
		rowidVec, aRowids[:2], nil, txn.proc.Mp()))
	pkVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(
		pkVec, []int64{1, 2}, nil, txn.proc.Mp()))
	delBat.Vecs[0] = rowidVec
	delBat.Vecs[1] = pkVec
	delBat.SetRowCount(2)
	out := txn.deleteBatch(delBat, issue25590DBID, issue25590TableID)
	t.Cleanup(func() { out.Clean(txn.proc.Mp()) })
	require.Greater(t, txn.deletedBlocks.size(), 0,
		"deletes of flushed rows must register pending block deletions")
	issue25590MarkRows(t, txn, batchB, 0, 1)
	batchC := issue25590AppendInsertEntry(t, txn, 9)
	txn.EndStatement()
	require.Len(t, txn.writes, 3)
	require.Equal(t, []int{0, 2}, txn.offsets)

	// Statement 3's admission applies B's marks (retiring B) and then
	// fails the object-compaction lookup for P's pending deletions.
	disarm := issue25590ArmDumpFault(t, objectio.FJ_CNReenterSnapshotOffsetOnGetTable, ":::")
	defer disarm()
	txn.StartStatement()
	err = txn.IncrStatementID(ctx, false)
	require.ErrorContains(t, err, "reenter snapshot write offset")

	// Failure-atomic publication: B's retired slot is compacted and the
	// boundary appended even though the merge failed, so the frontend
	// rollback ends THIS statement instead of truncating statement 2.
	require.Equal(t, []int{0, 2, 2}, txn.offsets,
		"admission bookkeeping stays failure-atomic across merge errors")
	require.Len(t, txn.writes, 2)
	require.NotEmpty(t, txn.writes[0].fileName, "the flushed entry stays")
	require.Same(t, batchC, txn.writes[1].bat,
		"the successful statement's replacement must be retained")
	require.Empty(t, txn.batchSelectList)
	require.Greater(t, txn.deletedBlocks.size(), 0,
		"the failed lookup must not consume P's pending deletions")
	require.NoError(t, txn.checkWorkspaceAccountingLocked())

	// The frontend statement rollback keeps every earlier statement's
	// data: boundary popped, writes untouched, and — the regression this
	// test pins — the pending block deletions SURVIVE.
	require.NoError(t, txn.RollbackLastStatement(ctx))
	txn.EndStatement()
	require.Equal(t, []int{0, 2}, txn.offsets)
	require.Len(t, txn.writes, 2)
	require.Same(t, batchC, txn.writes[1].bat)
	require.Empty(t, txn.batchSelectList)
	require.Greater(t, txn.deletedBlocks.size(), 0,
		"a rollback after an interrupted admission may not wipe the pending deletions of earlier statements")
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
	// The continuation that reapplies the retained deletions (the object
	// rewrite and the committed result) is covered end to end by the
	// engine-level TestIssue25590MergeFailureKeepsPriorStatements below;
	// the white-box fixture cannot serve the full compaction rewrite.
}

// issue25590PostPersistFailFS is a shared-file-service wrapper that makes
// a storage failure strike exactly the way production storage can: the
// object IS persisted and the write still returns an error — the
// commit-ambiguous failure the sinker must plan for. failWhen is consulted
// after persisting every write; the first true verdict fires once. A
// one-shot failDeleteOnce injects a deletion failure the same way, forcing
// the cleanup-ownership handoff deterministically.
//
// Every mutable field is guarded by mu, because deletes and writes arrive
// from the transaction's goroutine and the engine's asynchronous GC pool
// worker concurrently (round-7 review). deleteGate is the barrier that
// keeps a background worker's deletion parked until the test's
// synchronous assertions are done: DELETE calls beyond the first
// allowDeletesBeforeGate ones block on the gate before they touch any
// visible state, so the parked worker counts neither as attempted nor as
// completed while it waits.
type issue25590PostPersistFailFS struct {
	fileservice.FileService
	mu             sync.Mutex
	failWhen       func() bool
	fired          bool
	persisted      []string
	failedAt       string
	failDeleteOnce bool
	// failDeleteShots arms several consecutive delete failures, forcing a
	// cleanup chain to fail through several owners (issue #25590 round-6
	// review); failDeleteOnce keeps its single-shot meaning.
	failDeleteShots        int
	deleteCalls            int
	deleted                []string
	deleteGate             chan struct{}
	allowDeletesBeforeGate int
	// deleteFailurePath fails every deletion call that contains this one
	// object, persistently and without consuming any shot: the
	// object-specific storage failure of the round-10 review, whose batch
	// must keep exactly its own names.
	deleteFailurePath string
	// deleteBlock parks every deletion call inside a select on the
	// release channel or the CALLER'S CONTEXT — a stalled storage request
	// only the cancellation can abort. blockedDeleteCalls observes the
	// parked deletions without touching the regular accounting the round-7
	// gated counting owns (issue #25590 round-10 review).
	deleteBlock    chan struct{}
	blockedDeletes int
	// deleteIgnoreCtx parks every deletion call on the barrier ALONE: the
	// parked call IGNORES its context the way LocalFS deleteSingle does,
	// so only the explicit release can end the stall (issue #25590
	// round-11 review). ignoredDeletes observes those parked,
	// context-blind deletions.
	deleteIgnoreCtx chan struct{}
	ignoredDeletes  int
}

// armPostPersistFailure installs the post-persist write failure trigger.
func (fs *issue25590PostPersistFailFS) armPostPersistFailure(failWhen func() bool) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.failWhen = failWhen
	fs.fired = false
}

// stopPostPersistFailure disables the write failure permanently: further
// writes must succeed even when they would have matched failWhen.
func (fs *issue25590PostPersistFailFS) stopPostPersistFailure() {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.failWhen = nil
	fs.fired = true
}

// armDeleteFailures arms one-shot (once=true) or multi-shot consecutive
// delete failures.
func (fs *issue25590PostPersistFailFS) armDeleteFailures(once bool, shots int) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.failDeleteOnce = once
	fs.failDeleteShots = shots
}

// gateDeletesAfter arcs the deletion barrier: once allow deletes have
// been serviced, every further DELETE call parks until closeGate runs.
func (fs *issue25590PostPersistFailFS) gateDeletesAfter(allow int) (closeGate func()) {
	fs.mu.Lock()
	gate := make(chan struct{})
	fs.deleteGate = gate
	fs.allowDeletesBeforeGate = allow
	fs.mu.Unlock()
	return func() { close(gate) }
}

func (fs *issue25590PostPersistFailFS) persistedSnapshot() []string {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return slices.Clone(fs.persisted)
}

func (fs *issue25590PostPersistFailFS) failedAtName() string {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.failedAt
}

func (fs *issue25590PostPersistFailFS) deletedSnapshot() []string {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return slices.Clone(fs.deleted)
}

func (fs *issue25590PostPersistFailFS) deleteCallCount() int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.deleteCalls
}

func (fs *issue25590PostPersistFailFS) Write(
	ctx context.Context, vector fileservice.IOVector,
) error {
	if err := fs.FileService.Write(ctx, vector); err != nil {
		return err
	}
	fs.mu.Lock()
	fs.persisted = append(fs.persisted, vector.FilePath)
	failNow := !fs.fired && fs.failWhen != nil && fs.failWhen()
	if failNow {
		fs.fired = true
		fs.failedAt = vector.FilePath
	}
	fs.mu.Unlock()
	if failNow {
		return moerr.NewInternalErrorNoCtx(
			"issue25590 injected post-persist fs write failure")
	}
	return nil
}

func (fs *issue25590PostPersistFailFS) Delete(ctx context.Context, paths ...string) error {
	fs.mu.Lock()
	gate := fs.deleteGate
	park := gate != nil && fs.deleteCalls >= fs.allowDeletesBeforeGate
	block := fs.deleteBlock
	ignore := fs.deleteIgnoreCtx
	failPath := fs.deleteFailurePath
	if block != nil {
		fs.blockedDeletes++
	}
	if ignore != nil {
		fs.ignoredDeletes++
	}
	fs.mu.Unlock()
	if park {
		// Hold this asynchronous deletion until the caller's synchronous
		// assertions finished: the parked call changed no state yet.
		<-gate
	}
	if block != nil {
		// A stalled deletion: only the caller's own context can abort it,
		// exactly the shutdown case the round-10 review demands a bound
		// for. The parked call touches neither state nor accounting.
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if ignore != nil {
		// The context-blind stall of LocalFS deleteSingle: no cancellation
		// can move it — the round-11 shutdown bound must survive exactly
		// this shape — and once released the storage operation runs on
		// its own too, context-blind like deleteSingle.
		<-ignore
		ctx = context.WithoutCancel(ctx)
	}
	fs.mu.Lock()
	fs.deleteCalls++
	fs.deleted = append(fs.deleted, paths...)
	fail := false
	if failPath != "" && slices.Contains(paths, failPath) {
		fail = true
	}
	if fs.failDeleteOnce {
		fs.failDeleteOnce = false
		fail = true
	}
	if fs.failDeleteShots > 0 {
		fs.failDeleteShots--
		fail = true
	}
	fs.mu.Unlock()
	if fail {
		return moerr.NewInternalErrorNoCtx(
			"issue25590 injected fs delete failure")
	}
	return fs.FileService.Delete(ctx, paths...)
}

// armDeleteFailureFor makes every deletion call that contains path fail
// persistently, without consuming any shot: one poisoned object fails
// exactly its own batch per round under the round-10 partitioning.
func (fs *issue25590PostPersistFailFS) armDeleteFailureFor(path string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.deleteFailurePath = path
}

// armDeleteBlock parks every deletion call until the returned release
// closes the block or the call's own context aborts it. Releasing twice is
// safe; blocked calls are observable via blockedDeleteCalls.
func (fs *issue25590PostPersistFailFS) armDeleteBlock() (release func()) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	block := make(chan struct{})
	fs.deleteBlock = block
	var once sync.Once
	return func() { once.Do(func() { close(block) }) }
}

func (fs *issue25590PostPersistFailFS) blockedDeleteCalls() int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.blockedDeletes
}

// armDeleteIgnoreCtxBlock parks every deletion call on a barrier its
// context cannot cross: the parked call ignores cancellation exactly the
// way LocalFS deleteSingle does synchronous filesystem work, so only the
// returned release ends the stall (issue #25590 round-11 review).
// Visible via ignoredDeleteCalls.
func (fs *issue25590PostPersistFailFS) armDeleteIgnoreCtxBlock() (release func()) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	ignore := make(chan struct{})
	fs.deleteIgnoreCtx = ignore
	var once sync.Once
	return func() { once.Do(func() { close(ignore) }) }
}

func (fs *issue25590PostPersistFailFS) ignoredDeleteCalls() int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.ignoredDeletes
}

// issue25590RequireGone / issue25590RequireKept wrap the baby-orphan
// oracle: the object file must be present exactly when someone still owes
// its collection.
func issue25590RequireGone(t *testing.T, fs fileservice.FileService, path, why string) {
	t.Helper()
	_, err := fs.StatFile(context.Background(), path)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound),
		why, " expecting file not found, got %v", err)
}

func issue25590RequireKept(t *testing.T, fs fileservice.FileService, path, why string) {
	t.Helper()
	_, err := fs.StatFile(context.Background(), path)
	require.NoError(t, err, why)
}

// issue25590SwapProcSharedFS replaces the shared service under the
// transaction process's file-service registry with the wrapper, so every
// production consumer — GetSharedFSFromProc inside dumpBatchLocked,
// admission dumps, the rollback purge — flows through the injection point.
func issue25590SwapProcSharedFS(
	t *testing.T, txn *Transaction,
) *issue25590PostPersistFailFS {
	t.Helper()
	shared, err := colexec.GetSharedFSFromProc(txn.proc)
	require.NoError(t, err)
	local, err := fileservice.Get[fileservice.FileService](
		txn.proc.GetFileService(), defines.LocalFileServiceName)
	require.NoError(t, err)
	etl, err := fileservice.Get[fileservice.FileService](
		txn.proc.GetFileService(), defines.ETLFileServiceName)
	require.NoError(t, err)
	failFS := &issue25590PostPersistFailFS{FileService: shared}
	// The SHARED default routes the registry's unprefixed object IO to
	// the wrapped shared service exactly the production registry does -
	// readers built by the ACTUAL NewTableMetaReader constructor resolve
	// fs = proc.GetFileService() and read objects through the registry's
	// default-name routing (an empty default here fails every block read
	// with a missing service mapping).
	registry, err := fileservice.NewFileServices(
		defines.SharedFileServiceName, local, failFS, etl)
	require.NoError(t, err)
	txn.proc.SetFileService(registry)
	return failFS
}

// TestIssue25590InsertDumpPostPersistFailureCleansOrphans pins the round-5
// review for the insert dump: a publication whose object writes succeed
// physically and still fail — the commit-ambiguous case the pre-Sync
// fault keys can never produce — must not leak its persisted objects. The
// failed publication owns them until the file entry is recorded: no entry
// records them ever again, so Close's silent reference drop is not
// cleanup. Exactly the failing table's object is deleted on the spot;
// the earlier table's published object survives untouched; the failing
// table's sources stay raw and retry-safe; the deletion then succeeds so
// the transaction keeps no ownership.
func TestIssue25590InsertDumpPostPersistFailureCleansOrphans(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)
	// The second table resolves through the same created database the
	// dump's pre-resolution pass consults.
	dbv := txn.databaseOps.existAndActive(genDatabaseKey(1, "db"))
	require.NotNil(t, dbv)
	txn.tableOps.addCreateTable(
		genTableKey(1, "tbl2", 7, "db"),
		0,
		&txnTable{
			accountId: 1,
			tableId:   secondIssue25590TableID,
			tableName: "tbl2",
			db:        dbv,
			tableDef: &pbplan.TableDef{
				Cols: []*pbplan.ColDef{{
					Name: "pk",
					Typ:  pbplan.Type{Id: int32(types.T_int64)},
				}},
				Pkey: &pbplan.PrimaryKeyDef{PkeyColName: "pk"},
			},
		},
	)
	defer func() {
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	}()

	ctx := context.Background()
	admit := func() { issue25590AdmitBoundary(t, txn, ctx) }
	appendInsert2 := func(pks ...int64) *batch.Batch {
		bat := newIssue25590InsertBatchWithPKsForTest(t, txn, pks)
		txn.appendWorkspaceEntryLocked(Entry{
			typ:          INSERT,
			accountId:    1,
			databaseId:   issue25590DBID,
			tableId:      secondIssue25590TableID,
			databaseName: "db",
			tableName:    "tbl2",
			bat:          bat,
		})
		return bat
	}
	fileEntriesOf := func() int {
		n := 0
		for i := range txn.writes {
			if txn.writes[i].fileName != "" {
				n++
			}
		}
		return n
	}

	// One statement staged across two tables; its next admission dumps.
	admit()
	batchA := issue25590AppendInsertEntry(t, txn, 1, 2)
	batchC := appendInsert2(7)
	txn.EndStatement()
	require.Len(t, txn.writes, 2)

	// Fail the FIRST write issued after some table completed its
	// publication: whichever table the map order publishes first succeeds,
	// the other one persists an object and then fails — partial
	// multi-table publication, deterministic in the assertions below.
	failFS := issue25590SwapProcSharedFS(t, txn)
	// Round 15 routes the failed writer's names through the engine
	// queue, so the engine needs its own file service and GC pool for the
	// collection legs.
	txn.engine.fs = failFS
	pool, err := ants.NewPool(GCPoolSize)
	require.NoError(t, err)
	t.Cleanup(func() { pool.Release() })
	require.NoError(t, txn.engine.ResetGCWorkerPool(pool))
	failFS.armPostPersistFailure(func() bool { return fileEntriesOf() > 0 })
	txn.writeWorkspaceThreshold = 0
	txn.StartStatement()
	err = txn.IncrStatementID(ctx, false)
	require.ErrorContains(t, err, "post-persist fs write failure")

	// Exactly one table published; the failing table's sources stay raw.
	require.Len(t, txn.writes, 2,
		"the published table's file entry and the failed table's raw source")
	publishedEntry, retainedEntry := -1, -1
	for i := range txn.writes {
		if txn.writes[i].fileName == "" {
			retainedEntry = i
		} else {
			publishedEntry = i
		}
	}
	require.NotEqual(t, -1, publishedEntry)
	require.NotEqual(t, -1, retainedEntry)
	require.Equal(t, INSERT, txn.writes[publishedEntry].typ)
	require.NotEqual(t, txn.writes[retainedEntry].tableId, txn.writes[publishedEntry].tableId)
	retainedBat := txn.writes[retainedEntry].bat
	if txn.writes[retainedEntry].tableId == issue25590TableID {
		require.Same(t, batchA, retainedBat)
	} else {
		require.Same(t, batchC, retainedBat)
	}
	require.Equal(t, []int{0, 2}, txn.offsets,
		"admission bookkeeping published the post-publication boundary")

	// Round 15 (issue #25590): the committed-ambiguous object of the
	// failing table is NOT deleted on the spot — the failed
	// publication's statement path performs no deletion at all — so the
	// writer transfers its persisted names to the asynchronous cleanup
	// owner, which the engine's own drain collects once a boundary hands
	// them to the queue; the FIRST table's published object — recorded
	// by its file entry — must survive every cleanup of the failed
	// publication.
	require.Len(t, failFS.persistedSnapshot(), 2,
		"one write per publication; the second one is the failing table's object")
	persistedNow := failFS.persistedSnapshot()
	failedAtNow := failFS.failedAtName()
	require.Equal(t, persistedNow[1], failedAtNow)
	require.Equal(t, 0, failFS.deleteCallCount(),
		"round-15: the failed publication deleted nothing on the statement path")
	require.Len(t, txn.pendingObjectCleanup, 1,
		"the failed writer transferred its persisted name to the asynchronous cleanup owner")
	require.Equal(t, failedAtNow, txn.pendingObjectCleanup[0].name)
	issue25590RequireKept(t, failFS.FileService, failedAtNow,
		"the orphan of the failed publication until the asynchronous owner collects it")
	issue25590RequireKept(t, failFS.FileService, persistedNow[0],
		"the published table's object")

	// The retained rows stay exactly the rows the statement wrote.
	txn.UpdateSnapshotWriteOffset()
	retainedValues := issue25590VisibleValuesForTableLocked(
		t, txn, txn.writes[retainedEntry].tableId)
	if txn.writes[retainedEntry].tableId == issue25590TableID {
		require.Equal(t, []int64{1, 2}, retainedValues)
	} else {
		require.Equal(t, []int64{7}, retainedValues)
	}
	require.Empty(t, issue25590VisibleValuesForTableLocked(
		t, txn, txn.writes[publishedEntry].tableId),
		"the published table's rows left memory")
	require.NoError(t, txn.checkWorkspaceAccountingLocked())

	// The frontend statement rollback keeps both halves of the history.
	require.NoError(t, txn.RollbackLastStatement(ctx))
	txn.EndStatement()
	require.Equal(t, []int{0}, txn.offsets)
	require.Len(t, txn.writes, 2)

	// Retry with no fault: the retained table publishes and nothing is
	// left behind or lost.
	failFS.stopPostPersistFailure()
	admit()
	require.Len(t, txn.writes, 2, "both tables are file entries now")
	for i := range txn.writes {
		require.NotEmpty(t, txn.writes[i].fileName)
	}
	require.Zero(t, txn.approximateInMemInsertCnt)
	require.Empty(t, txn.pendingObjectCleanup,
		"the retry boundary's pre-gate handed the transferred orphan to the engine queue")
	require.Eventually(t, func() bool {
		return len(issue25590QueuedNames(txn.engine)) == 0
	}, 10*time.Second, 10*time.Millisecond,
		"the engine's drain collected the transferred orphan after the retry boundary handed it over")
	issue25590RequireGone(t, failFS.FileService, failedAtNow,
		"the failed publication's orphan after the engine's drain collected it")
	issue25590RequireKept(t, failFS.FileService, failFS.persistedSnapshot()[0],
		"the first table's published object through the retry")
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
	txn.EndStatement()
}

// TestIssue25590DumpCleanupKeepsOwnershipWhenDeleteFails pins the
// ownership handoff on the tombstone dump path (round-15 shape): a
// publication that persists its tombstone object and still fails
// performs NO deletion on the statement path — the failed writer hands
// the ownership over before closing, the transaction lists own the
// names, and the engine's own drain collects them on the file service
// once a later dump boundary hands them to the queue.
func TestIssue25590DumpCleanupKeepsOwnershipWhenDeleteFails(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)
	defer func() {
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	}()

	ctx := context.Background()
	failFS := issue25590SwapProcSharedFS(t, txn)
	// The round-14 contract collects the transferred orphan through the
	// engine's own drain, so the engine needs the file service the
	// orphan was persisted under and a live GC worker pool.
	txn.engine.fs = failFS
	pool, err := ants.NewPool(GCPoolSize)
	require.NoError(t, err)
	t.Cleanup(func() { pool.Release() })
	require.NoError(t, txn.engine.ResetGCWorkerPool(pool))
	delBat := issue25590AppendDeleteEntry(t, txn, 900)
	require.Len(t, txn.writes, 1)
	require.Equal(t, 1, txn.approximateInMemDeleteCnt)

	// The tombstone object persists physically and the write still fails
	// (round-15): the failed writer attempts no deletion and hands its
	// names over — ownership must not be lost.
	failFS.armPostPersistFailure(func() bool { return true })
	txn.Lock()
	err = txn.dumpDeleteBatchLocked(ctx, failFS, 0)
	txn.Unlock()
	require.ErrorContains(t, err, "post-persist fs write failure")

	require.Len(t, txn.writes, 1,
		"a failed delete dump must retain the staged tombstone source")
	require.Same(t, delBat, txn.writes[0].bat)
	require.Equal(t, 1, delBat.RowCount())
	require.Empty(t, txn.writes[0].fileName)
	require.Equal(t, 1, txn.approximateInMemDeleteCnt)
	require.NoError(t, txn.checkWorkspaceAccountingLocked())

	require.Len(t, failFS.persistedSnapshot(), 1)
	require.Len(t, txn.pendingObjectCleanup, 1,
		"the failed publication hands the ownership to the transaction without deleting anything")
	require.Equal(t, failFS.failedAtName(), txn.pendingObjectCleanup[0].name)
	require.Equal(t, 0, failFS.deleteCallCount(),
		"round-15: the failed publication attempted no deletion on the statement path")
	issue25590RequireKept(t, failFS.FileService, failFS.failedAtName(),
		"the not-yet-collected orphan waits for its owner's transfer")

	// Retry semantics after round 14 (issue #25590): the dump entry
	// REPLACED its synchronous pre-gate deletion with a transfer to the
	// engine's lossless queue — a mutex-only move — so even a dump that
	// skips all publishing owns no orphan, and the engine's own
	// scheduled drain performs the deletion on the healthy file service.
	txn.Lock()
	require.NoError(t, txn.dumpBatchLocked(ctx, 0))
	txn.Unlock()
	require.Empty(t, txn.pendingObjectCleanup,
		"the pre-gate transfer owns nothing: the dump handed the orphan to the engine queue")
	require.Eventually(t, func() bool {
		return failFS.deleteCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond,
		"the engine's own drain - the failed publication itself deleted nothing - performs the collection deletion")
	issue25590RequireGone(t, failFS.FileService, failFS.failedAtName(),
		"the orphan after the engine's drain collected it")
	require.Eventually(t, func() bool {
		return len(issue25590QueuedNames(txn.engine)) == 0
	}, 10*time.Second, 10*time.Millisecond,
		"the retired name left the queue")

	// With the file service healthy the tombstone publication succeeds.
	failFS.stopPostPersistFailure()
	txn.Lock()
	require.NoError(t, txn.dumpDeleteBatchLocked(ctx, failFS, 0))
	txn.Unlock()
	require.Len(t, txn.writes, 1)
	require.Equal(t, DELETE, txn.writes[0].typ)
	require.NotEmpty(t, txn.writes[0].fileName,
		"the tombstone publication replaces the in-memory source")
	require.Zero(t, txn.approximateInMemDeleteCnt)
	require.Empty(t, txn.pendingObjectCleanup)
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
}

// TestIssue25590DumpCleanupOwnershipCollectedAtTxnRollback pins the last
// owner: a transaction that dies while still owning not-yet-deletable
// orphans must hand them over — the workspace GC cannot know files
// recorded by no entry — and the engine queue it fed must collect them,
// with the retained raw source staying exactly the rows the statements
// wrote until the very end (issue #25590 round-12 review: the terminal
// transfer performs no storage I/O, so the drain, not the rollback,
// performs the physical delete).
func TestIssue25590DumpCleanupOwnershipCollectedAtTxnRollback(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)

	ctx := context.Background()
	failFS := issue25590SwapProcSharedFS(t, txn)
	// The terminal transfer routes through the engine's queue and drain,
	// so the engine needs the file service and a live GC worker.
	txn.engine.fs = failFS
	pool, err := ants.NewPool(GCPoolSize)
	require.NoError(t, err)
	t.Cleanup(func() { pool.Release() })
	require.NoError(t, txn.engine.ResetGCWorkerPool(pool))

	batchB := issue25590AppendInsertEntry(t, txn, 1, 2, 3)
	require.Len(t, txn.writes, 1)

	failFS.armPostPersistFailure(func() bool { return true })
	txn.Lock()
	err = txn.dumpInsertBatchLocked(ctx, failFS, 0, new(int))
	txn.Unlock()
	require.ErrorContains(t, err, "post-persist fs write failure")

	// Source-row preservation: the failed publication retained the raw
	// statement rows readable and untouched.
	require.Len(t, txn.writes, 1)
	require.Same(t, batchB, txn.writes[0].bat)
	require.Empty(t, txn.writes[0].fileName)
	require.Equal(t, 3, batchB.RowCount())
	txn.UpdateSnapshotWriteOffset()
	require.Equal(t, []int64{1, 2, 3}, issue25590VisibleValuesLocked(t, txn))
	require.NoError(t, txn.checkWorkspaceAccountingLocked())

	require.Len(t, txn.pendingObjectCleanup, 1)
	orphanName := failFS.failedAtName()
	require.Equal(t, orphanName, txn.pendingObjectCleanup[0].name)
	require.Equal(t, 0, failFS.deleteCallCount(),
		"round-15: the failed publication attempted no deletion on the locked dump path")
	issue25590RequireKept(t, failFS.FileService, orphanName,
		"the orphan before the rollback")

	// The transaction dies: the terminal ownership transfer — no storage
	// I/O on the rollback's own path — leaves nothing in the dying
	// transaction's list (neither collected synchronously nor kept), and
	// nothing of the dead transaction is needed for the collection
	// afterwards. The queue-mid state needs no snapshot race here:
	// dropping the name would leave the file undeletable forever, and the
	// parked-barrier terminal tests pin the handoff shape itself.
	require.NoError(t, txn.Rollback(ctx))
	require.Empty(t, txn.pendingObjectCleanup,
		"the rollback handed every owned orphan over instead of keeping or dropping them")
	require.Eventually(t, func() bool {
		_, statErr := failFS.StatFile(ctx, orphanName)
		return moerr.IsMoErrCode(statErr, moerr.ErrFileNotFound)
	}, 10*time.Second, 10*time.Millisecond,
		"the engine's own drain collects the handed-off orphan once the transaction is gone")
	require.Equal(t, 1, failFS.deleteCallCount(),
		"the drain's single successful delete is the only call - the publication itself performed none")
	require.Equal(t, []string{orphanName}, failFS.deletedSnapshot(),
		"exactly the drain's one call targeted the handed-over orphan")
	require.Eventually(t, func() bool {
		return len(issue25590QueuedNames(txn.engine)) == 0
	}, 5*time.Second, 5*time.Millisecond,
		"a successful drain empties the engine queue")
}

// TestIssue25590MergeRowidAllocFailurePreservesSources pins the round-6
// review for the admission merge: when the >=30-entries merge pass fails
// while regenerating the combined batch's rowids, the original sources
// must survive untouched — the old flow had already merged the sources in
// place by then, leaving a retained batch with rowids from several blocks
// whose one-block-per-batch and zero-based-offset invariants no later
// admission could repair, because the merge had shrunk the entry count
// below its own trigger. After the fix, the failure preserves every
// source exactly and a disarmed retry publishes the merged batch with a
// freshly regenerated, consistent rowid column that workspace reads and
// deleteTableWrites can rely on.
func TestIssue25590MergeRowidAllocFailurePreservesSources(t *testing.T) {
	colexec.NewServer("")
	txn, proc := newIssue25590TxnForTest(t, 4)
	defer func() {
		for i := range txn.writes {
			if txn.writes[i].bat != nil {
				txn.writes[i].bat.Clean(proc.Mp())
			}
		}
	}()
	ctx := context.Background()

	// One statement writes thirty-two separate single-row entries of the
	// same table: the next admission's merge pass sees >= 30 entries and
	// combines them.
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false))
	txn.UpdateSnapshotWriteOffset()
	sources := make([]*batch.Batch, 0, 32)
	for pk := int64(1); pk <= 32; pk++ {
		sources = append(sources, issue25590AppendInsertEntry(t, txn, pk))
	}
	require.Len(t, txn.writes, 32)
	txn.EndStatement()

	// Fail the merge exactly at the combined batch's rowid regeneration.
	disarm := issue25590ArmDumpFault(
		t, objectio.FJ_CNWorkspaceMergeRowidAllocFailed, ":::")

	txn.StartStatement()
	require.ErrorContains(t, txn.IncrStatementID(ctx, false),
		"workspace merge rowid alloc failed")

	// The failure must not have touched any of the thirty-two sources:
	// same pointers, same single rows, all readable, nothing retired and
	// no half-merged combination behind.
	require.Len(t, txn.writes, 32,
		"an allocation failure before publication may not merge or retire anything")
	for i := range sources {
		require.Same(t, sources[i], txn.writes[i].bat,
			"every source batch survives the failed merge untouched")
		require.Equal(t, 1, txn.writes[i].bat.RowCount())
		require.Empty(t, txn.writes[i].fileName)
	}
	txn.UpdateSnapshotWriteOffset()
	require.Equal(t, 32, len(issue25590VisibleValuesLocked(t, txn)),
		"all rows written by the statement stay readable")
	require.Empty(t, txn.batchSelectList)
	require.NoError(t, txn.checkWorkspaceAccountingLocked())

	// The frontend statement rollback ends the failed statement without
	// altering the preserved sources.
	require.NoError(t, txn.RollbackLastStatement(ctx))
	txn.EndStatement()
	require.Len(t, txn.writes, 32)
	for i := range sources {
		require.Same(t, sources[i], txn.writes[i].bat)
	}

	// The disarmed admission really merges now, and the published batch
	// carries the regenerated rowid column both consumers rely on: one
	// block, offsets running consecutively from zero.
	disarm()
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false))
	txn.UpdateSnapshotWriteOffset()
	txn.EndStatement()

	require.Len(t, txn.writes, 1,
		"the retry combines the thirty-two sources into one entry")
	merged := txn.writes[0]
	require.Equal(t, INSERT, merged.typ)
	require.Empty(t, merged.fileName)
	require.Equal(t, 32, merged.bat.RowCount())
	require.Len(t, sources, 32)
	rowIds := vector.MustFixedColNoTypeCheck[types.Rowid](merged.bat.Vecs[0])
	require.Equal(t, 32, len(rowIds))
	blk := rowIds[0].CloneBlockID()
	for i, rowId := range rowIds {
		require.Equal(t, blk, rowId.CloneBlockID(),
			"published merged batches carry one block only")
		require.Equal(t, uint32(i), rowId.GetRowOffset(),
			"published merged batches run offsets consecutively from zero")
	}
	require.Equal(t, 32, len(issue25590VisibleValuesLocked(t, txn)))
	require.Empty(t, txn.batchSelectList)
	require.NoError(t, txn.checkWorkspaceAccountingLocked())

	// Continued mutation through the merged batch works: a marked row is
	// shrunk away and a fresh row flows into the workspace, all reads
	// stay exact, and the merge re-run after a later statement keeps the
	// entry readable end to end.
	txn.StartStatement()
	issue25590MarkRows(t, txn, merged.bat, 0)
	fresh := issue25590AppendInsertEntry(t, txn, 2099)
	txn.UpdateSnapshotWriteOffset()
	require.Equal(t, 33, len(issue25590VisibleValuesLocked(t, txn)))
	txn.EndStatement()

	// The admission consumes the mark (the merged batch shrinks to
	// thirty-one rows) exactly like it would for a raw one-block batch.
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false))
	txn.UpdateSnapshotWriteOffset()
	require.Same(t, merged.bat, txn.writes[0].bat,
		"the published merged batch keeps its slot through the shrink")
	require.Equal(t, 31, merged.bat.RowCount())
	require.Same(t, fresh, txn.writes[1].bat)
	require.Empty(t, txn.batchSelectList,
		"marks on the merged batch consumed exactly like on raw batches")
	require.Equal(t, 32, len(issue25590VisibleValuesLocked(t, txn)),
		"the shrunken merged batch and the fresh row are both readable")
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
	txn.EndStatement()
}

// TestIssue25590RollbackOrphanPurgeSurvivesCanceledContext pins the
// round-6 masked-context hazard on its round-12 carrier: the transaction
// client rolls a failed workspace commit back with the same, usually
// canceled context, LocalFS/S3FS/MemoryFS deletes reject canceled
// contexts outright, and nothing after delTransaction can retry. The
// terminal transfer thus consults neither the caller's cancellation nor
// any storage: a dead context changes nothing on the destruction path,
// and the physical collection rides the engine's own
// cancellation-independent drain, deleting exactly the names the dying
// transaction handed over.
func TestIssue25590RollbackOrphanPurgeSurvivesCanceledContext(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)

	ctx := context.Background()
	failFS := issue25590SwapProcSharedFS(t, txn)
	// The terminal transfer routes through the engine's queue and drain,
	// so the engine needs the wrapped file service and a live GC worker.
	txn.engine.fs = failFS
	pool, err := ants.NewPool(GCPoolSize)
	require.NoError(t, err)
	t.Cleanup(func() { pool.Release() })
	require.NoError(t, txn.engine.ResetGCWorkerPool(pool))

	failFS.armPostPersistFailure(func() bool { return true })

	batchB := issue25590AppendInsertEntry(t, txn, 1, 2, 3)
	require.Len(t, txn.writes, 1)

	txn.Lock()
	require.ErrorContains(t, txn.dumpInsertBatchLocked(ctx, failFS, 0, new(int)),
		"post-persist fs write failure")
	txn.Unlock()
	require.Len(t, txn.pendingObjectCleanup, 1)
	orphanName := failFS.failedAtName()
	require.Equal(t, orphanName, txn.pendingObjectCleanup[0].name)
	require.Equal(t, 3, batchB.RowCount())

	// The caller cancels before the terminal rollback, exactly like a
	// commit that failed on a statement cancellation.
	canceled, cancel := context.WithCancel(ctx)
	cancel()

	require.NoError(t, txn.Rollback(canceled),
		"terminal destruction must not consult the caller's cancellation")
	require.Empty(t, txn.pendingObjectCleanup,
		"the canceled rollback handed every owned orphan over instead of keeping or dropping them")
	require.Eventually(t, func() bool {
		_, statErr := failFS.StatFile(ctx, orphanName)
		return moerr.IsMoErrCode(statErr, moerr.ErrFileNotFound)
	}, 10*time.Second, 10*time.Millisecond,
		"the physical orphan is collected through the canceled rollback's handoff")
	require.Equal(t, 1, failFS.deleteCallCount(),
		"the engine drain's delete is the only call - the failed publication itself performed none")
	require.Equal(t, []string{orphanName}, failFS.deletedSnapshot(),
		"exactly the drain's one call targeted the transaction's orphan")
	require.Eventually(t, func() bool {
		return len(issue25590QueuedNames(txn.engine)) == 0
	}, 5*time.Second, 5*time.Millisecond,
		"a successful drain empties the engine queue")
}

// TestIssue25590RollbackHandsOrphanCleanupToEngineGC pins the second half
// of the terminal-cleanup contract on its round-12 carrier: the dying
// transaction transfers the names BEFORE any storage I/O — no terminal
// purge exists to fail — and the engine's asynchronous GC pool, the
// same pool workspace GC already uses at rollback, completes the
// physical deletion after delTransaction destroyed the transaction's
// state.
func TestIssue25590RollbackHandsOrphanCleanupToEngineGC(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)

	pool, err := ants.NewPool(2)
	require.NoError(t, err)
	t.Cleanup(func() { pool.Release() })

	ctx := context.Background()
	failFS := issue25590SwapProcSharedFS(t, txn)
	// The engine's own GC and the handoff flow through this file service.
	txn.engine.fs = failFS
	require.NoError(t, txn.engine.ResetGCWorkerPool(pool))

	failFS.armPostPersistFailure(func() bool { return true })

	batchB := issue25590AppendInsertEntry(t, txn, 7, 8, 9)
	require.Len(t, txn.writes, 1)

	txn.Lock()
	require.ErrorContains(t, txn.dumpInsertBatchLocked(ctx, failFS, 0, new(int)),
		"post-persist fs write failure")
	txn.Unlock()
	require.Len(t, txn.pendingObjectCleanup, 1)
	require.Equal(t, failFS.failedAtName(), txn.pendingObjectCleanup[0].name)
	require.Equal(t, 3, batchB.RowCount())

	// The run-7 review barrier, armed only now — after the failed
	// publication, whose statement path performs no deletion in round
	// 15, is exactly why the worker's parked call is the FIRST delete
	// call: the asynchronous GC worker's DELETE parks here — before it
	// touches any visible wrapper state — until the synchronous
	// assertions below are done, and only the gated final count is
	// exact.
	closeDeleteGate := failFS.gateDeletesAfter(0)

	canceled, cancel := context.WithCancel(ctx)
	cancel()

	require.NoError(t, txn.Rollback(canceled))
	require.Empty(t, txn.pendingObjectCleanup,
		"the dying transaction handed every name over to the engine queue")
	failedOrphan := failFS.failedAtName()
	require.Len(t, issue25590QueuedNames(txn.engine), 1,
		"the queue owns the handed-off orphan while its worker holds at the barrier")
	require.Equal(t, 0, failFS.deleteCallCount(),
		"round-15: the failed publication attempted no deletion, and the worker at the barrier completed none")

	// Release the barrier only after the synchronous assertions are done.
	closeDeleteGate()

	// The engine pool completes the delete the dying transaction could
	// not wait for; the wait below observes the worker's completion, so
	// the final count is exact and race-free.
	require.Eventually(t, func() bool {
		_, statErr := failFS.StatFile(ctx, failedOrphan)
		return moerr.IsMoErrCode(statErr, moerr.ErrFileNotFound)
	}, 10*time.Second, 10*time.Millisecond,
		"the engine GC pool completes the handed-off collection")
	require.Equal(t, 1, failFS.deleteCallCount(),
		"the pooled worker's single successful delete is the only call")
	require.Equal(t, []string{failedOrphan}, failFS.deletedSnapshot(),
		"the worker's success is the only call that touched the orphan")
	require.Eventually(t, func() bool {
		return len(issue25590QueuedNames(txn.engine)) == 0
	}, 5*time.Second, 5*time.Millisecond,
		"a successful drain empties the engine queue")
}

// issue25590ArmKeyedFault installs a fault point carrying explicit integer
// and string arguments — the keyed injections whose trigger semantics
// parse iarg/sarg (like the small-objects flush policy), for which the
// plain `:::` echo helpers from issue25590ArmDumpFault are not enough.
func issue25590ArmKeyedFault(
	t *testing.T, key string, iarg int64, sarg string,
) (disarm func()) {
	t.Helper()
	fault.Enable()
	t.Cleanup(func() {
		fault.Disable()
	})
	t.Cleanup(func() {
		fault.RemoveFaultPoint(context.Background(), key)
	})
	require.NoError(t, fault.AddFaultPoint(
		context.Background(), key, ":::", "echo", iarg, sarg, false))
	return func() {
		fault.RemoveFaultPoint(context.Background(), key)
	}
}

// issue25590MetadataEntry locates the workspace's first block-meta entry of
// the fixture table and unpacks its object stats column.
func issue25590MetadataEntry(
	t *testing.T, txn *Transaction,
) (idx int, statsCol []types.Varlena, statsArea []byte) {
	t.Helper()
	for i := range txn.writes {
		e := txn.writes[i]
		if e.bat == nil || e.bat.IsEmpty() ||
			e.typ != INSERT || e.bat.Attrs[0] != catalog.BlockMeta_BlockInfo {
			continue
		}
		statsCol, statsArea = vector.MustVarlenaRawData(e.bat.Vecs[1])
		return i, statsCol, statsArea
	}
	require.FailNow(t, "no block-meta entry found in the workspace")
	return
}

// TestIssue25590CompactPublishFailureKeepsPendingDeletes pins the round-7
// P1 review: the object-compaction pass re-publishes the clean objects of
// every metadata batch it touches, and that publication is a chain of
// mpool-fallible steps running AFTER the tables resolved and BEFORE the
// dirty object was rewritten. A failure there must leave the whole
// workspace re-runnable exactly as found — above all the pending
// deletedBlocks of the un-rewritten dirty object, which are the ONLY
// record that those rows are gone (the pre-fix deferred clean() wiped them,
// so the deleted rows resurrected through the rollback and the retried
// admission) — and it must not consume any original metadata batch before
// this statement's publications survive it.
func TestIssue25590CompactPublishFailureKeepsPendingDeletes(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)
	defer func() {
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	}()

	ctx := context.Background()
	admit := func() { issue25590AdmitBoundary(t, txn, ctx) }
	fs, err := colexec.GetSharedFSFromProc(txn.proc)
	require.NoError(t, err)

	// Statement 1: one large raw entry whose forced dump intersects the
	// sinker's 512KB in-memory threshold twice, publishing ONE metadata
	// batch that carries TWO objects — the production shape in which an
	// affected batch always re-publishes its clean objects next to the
	// dirty ones it gets rewritten.
	admit()
	pks := make([]int64, 100000)
	for i := range pks {
		pks[i] = int64(i + 1)
	}
	batchA := newIssue25590InsertBatchWithPKsForTest(t, txn, pks)
	txn.appendWorkspaceEntryLocked(Entry{
		typ:          INSERT,
		accountId:    1,
		databaseId:   issue25590DBID,
		tableId:      issue25590TableID,
		databaseName: "db",
		tableName:    "tbl",
		bat:          batchA,
	})
	smallObjsDisarm := issue25590ArmKeyedFault(t, objectio.FJ_CNFlushSmallObjs, 20, "")
	txn.Lock()
	var pkCount int
	err = txn.dumpInsertBatchLocked(ctx, fs, txn.GetSnapshotWriteOffset(), &pkCount)
	txn.Unlock()
	smallObjsDisarm()
	require.NoError(t, err)
	require.Equal(t, 100000, pkCount)

	metaIdx, statsCol, statsArea := issue25590MetadataEntry(t, txn)
	require.Len(t, statsCol, 2,
		"the dump must publish the mixed metadata batch: one clean and one dirty object")
	obj1Stats := objectio.ObjectStats(statsCol[0].GetByteSlice(statsArea))
	obj2Stats := objectio.ObjectStats(statsCol[1].GetByteSlice(statsArea))
	require.Greater(t, int(obj1Stats.BlkCnt()), 0)
	require.Greater(t, int(obj2Stats.BlkCnt()), 0)
	metaBat := txn.writes[metaIdx].bat
	metaRowCount := metaBat.RowCount()
	infoCol, infoArea := vector.MustVarlenaRawData(metaBat.Vecs[0])
	obj2FirstBlock := objectio.DecodeBlockInfo(
		infoCol[int(obj1Stats.BlkCnt())].GetByteSlice(infoArea)).BlockID
	// obj1 spilled only at full-block boundaries, so its rows are exactly
	// BlkCnt() * BlockMaxRows and the first two rows of obj2 carry the
	// next two pks of the ordered raw batch.
	obj1Rows := int(obj1Stats.BlkCnt()) * objectio.BlockMaxRows
	require.LessOrEqual(t, obj1Rows+2, 100000)
	txn.EndStatement()

	// Statement 2: delete the first two rows of the second object — the
	// rows whose only deletion record is the pending deletedBlocks entry
	// on the dirty object.
	admit()
	delBat := batch.NewWithSize(2)
	delBat.SetAttributes([]string{objectio.PhysicalAddr_Attr, "pk"})
	rowidVec := vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixedList(
		rowidVec, []types.Rowid{
			types.NewRowid(&obj2FirstBlock, 0),
			types.NewRowid(&obj2FirstBlock, 1),
		}, nil, txn.proc.Mp()))
	pkVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(
		pkVec, []int64{pks[obj1Rows], pks[obj1Rows+1]}, nil, txn.proc.Mp()))
	delBat.Vecs[0] = rowidVec
	delBat.Vecs[1] = pkVec
	delBat.SetRowCount(2)
	out := txn.deleteBatch(delBat, issue25590DBID, issue25590TableID)
	t.Cleanup(func() { out.Clean(txn.proc.Mp()) })
	require.Equal(t, 1, txn.deletedBlocks.size(),
		"the two dumped-row deletes register one pending block deletion")
	markedOffsets := len(txn.deletedBlocks.offsets[obj2FirstBlock])
	require.Equal(t, 2, markedOffsets)
	txn.EndStatement()

	// Statement 3's admission fails exactly at the clean object's post-
	// lookup re-publication, with the dirty object's rewrite still
	// pending.
	disarm := issue25590ArmDumpFault(
		t, objectio.FJ_CNWorkspaceCompactPublishFailed, ":::")
	defer disarm()
	txn.StartStatement()
	err = txn.IncrStatementID(ctx, false)
	require.ErrorContains(t, err, "object compaction publish failed")

	// The pending deletions of the un-rewritten object survive the failed
	// compaction — with them, the retried admission reapplies the deletes;
	// without them, the deleted rows resurrect.
	require.Equal(t, 1, txn.deletedBlocks.size(),
		"the failed compaction may not consume the pending deletions")
	require.Equal(t, 2,
		len(txn.deletedBlocks.offsets[obj2FirstBlock]),
		"both marked offsets of the un-rewritten dirty object stay pending")

	// No original metadata batch may be retired before this statement's
	// publications outlive it: the mixed batch itself is byte-identical —
	// same row count, same two objects, same stats order.
	metaAfter := txn.writes[metaIdx].bat
	require.NotNil(t, metaAfter,
		"the mixed metadata batch must stay untouched by the failed pass")
	require.Equal(t, metaRowCount, metaAfter.RowCount())
	statsColAfter, statsAreaAfter := vector.MustVarlenaRawData(metaAfter.Vecs[1])
	require.Len(t, statsColAfter, 2)
	after1 := objectio.ObjectStats(statsColAfter[0].GetByteSlice(statsAreaAfter))
	after2 := objectio.ObjectStats(statsColAfter[1].GetByteSlice(statsAreaAfter))
	require.Equal(t, obj1Stats.ObjectName().String(), after1.ObjectName().String())
	require.Equal(t, obj2Stats.ObjectName().String(), after2.ObjectName().String())
	require.Len(t, txn.writes, 1,
		"the failed pass appended no replacement metadata entries")
	require.NoError(t, txn.checkWorkspaceAccountingLocked())

	// The frontend statement rollback ends the failed statement and must
	// keep every earlier statement plus the pending deletions intact.
	require.NoError(t, txn.RollbackLastStatement(ctx))
	txn.EndStatement()
	require.Equal(t, 1, txn.deletedBlocks.size(),
		"a rollback after the interrupted admission keeps the pending deletions")
	require.Equal(t, 2, len(txn.deletedBlocks.offsets[obj2FirstBlock]))
	require.NotNil(t, txn.writes[metaIdx].bat)
	require.Equal(t, metaRowCount, txn.writes[metaIdx].bat.RowCount())
	require.Len(t, txn.writes, 1)
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
	// The full continuation — retried compaction, rewrite, reads and
	// commit — is served end to end by the engine-level
	// TestIssue25590CompactPublishFailurePreservesDeletesAndCommits; the
	// white-box fixture's engine carries no file service for
	// rewriteObjectByDeletion.
}

// TestIssue25590FinalizeCommitCollectsStillOwnedOrphans pins the round-7
// P2 commit-finalizer review: the terminal orphan cleanup that Rollback
// already runs must also own the proven-unpublished names when a commit
// SUCCEEDS with the file service still failing — the failed writer's and
// the commit dump's deletes keep failing, so after delTransaction nothing
// would remain to retry with unless FinalizeCommit (or its unknown-result
// sibling) hands the names to the engine's GC pool. Recorded, published
// objects — the entries' own files — must be preserved untouched.
func TestIssue25590FinalizeCommitCollectsStillOwnedOrphans(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)

	pool, err := ants.NewPool(2)
	require.NoError(t, err)
	t.Cleanup(func() { pool.Release() })

	ctx := context.Background()
	failFS := issue25590SwapProcSharedFS(t, txn)
	txn.engine.fs = failFS
	require.NoError(t, txn.engine.ResetGCWorkerPool(pool))

	// A first healthy publication: its recorded file entry keeps its own
	// object alive through every cleanup that follows. The physical path
	// is the one the file service recorded (the entry's fileName carries
	// the versioned object location instead).
	issue25590AppendInsertEntry(t, txn, 91, 92)
	txn.Lock()
	var pkCount int
	err = txn.dumpInsertBatchLocked(ctx, failFS, txn.GetSnapshotWriteOffset(), &pkCount)
	txn.Unlock()
	require.NoError(t, err)
	require.Equal(t, 2, pkCount)
	require.Len(t, failFS.persistedSnapshot(), 1)
	publishedFile := failFS.persistedSnapshot()[0]
	require.NotEmpty(t, txn.writes[0].fileName)
	require.Empty(t, txn.pendingObjectCleanup)

	// The second publication persists physically and still fails; the
	// failed writer transfers its names without deleting anything on
	// the statement path (round-15), the engine drain's first two
	// rounds meet the armed delete failures, and only the finalizer's
	// shared terminal transfer can move the residual ownership before
	// delTransaction.
	issue25590AppendInsertEntry(t, txn, 93)
	failFS.armPostPersistFailure(func() bool { return true })
	failFS.armDeleteFailures(false, 2)
	txn.Lock()
	err = txn.dumpInsertBatchLocked(ctx, failFS, txn.GetSnapshotWriteOffset(), &pkCount)
	txn.Unlock()
	require.ErrorContains(t, err, "post-persist fs write failure")
	require.Len(t, txn.pendingObjectCleanup, 1)
	orphanPath := txn.pendingObjectCleanup[0].name
	require.NotEqual(t, publishedFile, orphanPath)

	// Successful commit finalization through the production entry point:
	// the shared terminal leg performs the ownership transfer alone — no
	// storage I/O waits on the commit's final moment — and the engine
	// GC pool's drain completes the delete after delTransaction
	// destroyed the transaction, retrying through its own ladder while
	// the dirty service keeps failing.
	txn.FinalizeCommit(ctx)
	require.Nil(t, txn.pendingObjectCleanup,
		"a successful commit finalization never leaves owned orphans behind")
	require.Nil(t, txn.cnObjsSummary,
		"the finalizer went through delTransaction after the terminal cleanup")
	require.Eventually(t, func() bool {
		_, statErr := failFS.StatFile(ctx, orphanPath)
		return moerr.IsMoErrCode(statErr, moerr.ErrFileNotFound)
	}, 10*time.Second, 10*time.Millisecond,
		"the finalized commit must collect the proven-unpublished orphan")
	issue25590RequireKept(t, failFS.FileService, publishedFile,
		"the published object of the recorded file entry")
}

// TestIssue25590FinalizeCommitWithUnknownResultCollectsOrphans pins the
// unknown-result sibling of the round-7 P2 review: a commit that may have
// reached TN destroys the transaction without ever rolling the workspace
// back, so the same shared terminal cleanup must reach it — on the VERY
// context that just timed out or was canceled, proving the
// cancellation-independent retry — and the proven-unpublished names are
// safe to delete under an unknown result precisely because no commit
// payload can reference them. The recorded, published object survives.
func TestIssue25590FinalizeCommitWithUnknownResultCollectsOrphans(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)

	pool, err := ants.NewPool(2)
	require.NoError(t, err)
	t.Cleanup(func() { pool.Release() })

	ctx := context.Background()
	failFS := issue25590SwapProcSharedFS(t, txn)
	txn.engine.fs = failFS
	require.NoError(t, txn.engine.ResetGCWorkerPool(pool))

	// A healthy recorded publication whose object must survive every
	// cleanup, then a failed one whose orphan the dying transaction still
	// owns: the failed writer deletes nothing on the statement path
	// (round-15) and no terminal attempt follows — the ownership
	// transfer to the engine GC pool is the only owner left, and its
	// drain consumes the two armed shots on the way to its successful
	// retry.
	issue25590AppendInsertEntry(t, txn, 7, 8, 9)
	txn.Lock()
	var pkCount int
	err = txn.dumpInsertBatchLocked(ctx, failFS, txn.GetSnapshotWriteOffset(), &pkCount)
	txn.Unlock()
	require.NoError(t, err)
	require.Equal(t, 3, pkCount)
	require.Len(t, failFS.persistedSnapshot(), 1)
	publishedFile := failFS.persistedSnapshot()[0]
	require.NotEmpty(t, txn.writes[0].fileName)

	issue25590AppendInsertEntry(t, txn, 11)
	failFS.armPostPersistFailure(func() bool { return true })
	failFS.armDeleteFailures(false, 2)
	txn.Lock()
	err = txn.dumpInsertBatchLocked(ctx, failFS, txn.GetSnapshotWriteOffset(), &pkCount)
	txn.Unlock()
	require.ErrorContains(t, err, "post-persist fs write failure")
	require.Len(t, txn.pendingObjectCleanup, 1)
	orphanPath := txn.pendingObjectCleanup[0].name

	// The client finalizes an unknown commit with the very context it just
	// canceled — the rollback-with-unknown flow of pkg/txn/client.
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	txn.FinalizeCommitWithUnknownResult(canceled)
	require.Nil(t, txn.pendingObjectCleanup,
		"the unknown-result finalization transfers residual ownership before transaction destruction")
	require.Nil(t, txn.cnObjsSummary,
		"the finalizer went through delTransaction after the terminal cleanup")
	require.Eventually(t, func() bool {
		_, statErr := failFS.StatFile(ctx, orphanPath)
		return moerr.IsMoErrCode(statErr, moerr.ErrFileNotFound)
	}, 10*time.Second, 10*time.Millisecond,
		"the GC pool must collect the orphan of the finalized unknown-result commit")
	issue25590RequireKept(t, failFS.FileService, publishedFile,
		"the published object of the recorded file entry")
}

// issue25590AllMetadataEntries returns the workspace indices of every
// block-meta (object publication) entry of the fixture table.
func issue25590AllMetadataEntries(t *testing.T, txn *Transaction) []int {
	t.Helper()
	idxes := make([]int, 0, 4)
	for i := range txn.writes {
		e := txn.writes[i]
		if e.bat == nil || e.bat.IsEmpty() ||
			e.typ != INSERT || e.bat.Attrs[0] != catalog.BlockMeta_BlockInfo {
			continue
		}
		idxes = append(idxes, i)
	}
	require.NotEmpty(t, idxes, "no block-meta entry found in the workspace")
	return idxes
}

// issue25590StatsNamesOfMeta returns the object names of a block-meta
// entry in publication order, plus its row count — the byte-level shape
// the round-8 invariants compare across a failed compaction pass.
func issue25590StatsNamesOfMeta(t *testing.T, txn *Transaction, idx int) (names []string) {
	t.Helper()
	bat := txn.writes[idx].bat
	require.NotNil(t, bat)
	statsCol, statsArea := vector.MustVarlenaRawData(bat.Vecs[1])
	for j := range statsCol {
		stats := objectio.ObjectStats(statsCol[j].GetByteSlice(statsArea))
		names = append(names, stats.ObjectName().String())
	}
	return names
}

// issue25590ArmCountedPublishFault arms the compaction re-publication fault
// so it fires exactly at the n-th invocation instead of the first — the
// round-8 second-publication scenario: one affected object's replacement
// is already staged when the next one fails.
func issue25590ArmCountedPublishFault(t *testing.T, nth int64) (disarm func()) {
	t.Helper()
	fault.Enable()
	key := objectio.FJ_CNWorkspaceCompactPublishFailed
	t.Cleanup(func() {
		fault.Disable()
	})
	t.Cleanup(func() {
		fault.RemoveFaultPoint(context.Background(), key)
	})
	freq := strconv.FormatInt(nth, 10) + ":" + strconv.FormatInt(nth, 10) + ":1:"
	require.NoError(t, fault.AddFaultPoint(
		context.Background(), key, freq, "echo", 0, "", false))
	return func() {
		fault.RemoveFaultPoint(context.Background(), key)
	}
}

// TestIssue25590CompactSecondPublishFailureKeepsEveryOriginal pins the
// round-8 P1 review: a metadata batch with a clean and a dirty object is
// re-published per clean object, and the pre-staging pass appended each
// publication as soon as it was built. With MORE THAN ONE affected entry
// (or more than one clean object), a second republication's failure left
// the FIRST entry's replacement appended beside its un-retired original:
// the statement boundary (types.go IncrStatementID) is recorded after the
// merge even on failure, so the interrupted-statement rollback retained
// the stranded duplicate, and the retried admission published the same
// object again while txnTable's collectUnCommittedObjStats deduplicates
// nothing — duplicate reads and commit entries. The staged pass below
// makes every publication leave the workspace only AFTER every fallible
// step of every affected object succeeded, so a second-publication failure
// appends nothing at all and both originals alone remain.
func TestIssue25590CompactSecondPublishFailureKeepsEveryOriginal(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)
	defer func() {
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	}()

	ctx := context.Background()
	admit := func() { issue25590AdmitBoundary(t, txn, ctx) }
	fs, err := colexec.GetSharedFSFromProc(txn.proc)
	require.NoError(t, err)

	// Statement 1 publishes TWO mixed metadata batches — each carrying a
	// clean and a dirty object — then marks two rows of each batch's dirty
	// object, so the next admission's compaction must re-publish BOTH
	// clean objects: the multi-entry, second-publication premise.
	admit()
	dumpOneMixedEntry := func(pkBase int64) (metaIdx int, obj2FirstBlock types.Blockid, obj1Rows int) {
		pks := make([]int64, 100000)
		for i := range pks {
			pks[i] = pkBase + int64(i)
		}
		bat := newIssue25590InsertBatchWithPKsForTest(t, txn, pks)
		txn.appendWorkspaceEntryLocked(Entry{
			typ:          INSERT,
			accountId:    1,
			databaseId:   issue25590DBID,
			tableId:      issue25590TableID,
			databaseName: "db",
			tableName:    "tbl",
			bat:          bat,
		})
		smallObjsDisarm := issue25590ArmKeyedFault(t, objectio.FJ_CNFlushSmallObjs, 20, "")
		txn.Lock()
		var pkCount int
		require.NoError(t, txn.dumpInsertBatchLocked(ctx, fs, txn.GetSnapshotWriteOffset(), &pkCount))
		txn.Unlock()
		smallObjsDisarm()
		require.Equal(t, 100000, pkCount)

		idxes := issue25590AllMetadataEntries(t, txn)
		metaIdx = idxes[len(idxes)-1]
		metaBat := txn.writes[metaIdx].bat
		statsCol, statsArea := vector.MustVarlenaRawData(metaBat.Vecs[1])
		require.Len(t, statsCol, 2,
			"the dump must publish a mixed metadata batch: one clean and one dirty object")
		obj1Stats := objectio.ObjectStats(statsCol[0].GetByteSlice(statsArea))
		obj1Rows = int(obj1Stats.BlkCnt()) * objectio.BlockMaxRows
		require.LessOrEqual(t, obj1Rows+2, 100000)
		infoCol, infoArea := vector.MustVarlenaRawData(metaBat.Vecs[0])
		obj2FirstBlock = objectio.DecodeBlockInfo(
			infoCol[int(obj1Stats.BlkCnt())].GetByteSlice(infoArea)).BlockID
		return metaIdx, obj2FirstBlock, obj1Rows
	}

	metaIdx1, obj2Block1, obj1Rows1 := dumpOneMixedEntry(1)
	metaIdx2, obj2Block2, obj1Rows2 := dumpOneMixedEntry(int64(100000))

	deleteTwoMarkedRows := func(obj2FirstBlock types.Blockid, pkBase int64, obj1Rows int) {
		delBat := batch.NewWithSize(2)
		delBat.SetAttributes([]string{objectio.PhysicalAddr_Attr, "pk"})
		rowidVec := vector.NewVec(types.T_Rowid.ToType())
		require.NoError(t, vector.AppendFixedList(
			rowidVec, []types.Rowid{
				types.NewRowid(&obj2FirstBlock, 0),
				types.NewRowid(&obj2FirstBlock, 1),
			}, nil, txn.proc.Mp()))
		pkVec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(
			pkVec, []int64{pkBase + int64(obj1Rows), pkBase + int64(obj1Rows+1)},
			nil, txn.proc.Mp()))
		delBat.Vecs[0] = rowidVec
		delBat.Vecs[1] = pkVec
		delBat.SetRowCount(2)
		out := txn.deleteBatch(delBat, issue25590DBID, issue25590TableID)
		t.Cleanup(func() { out.Clean(txn.proc.Mp()) })
	}
	deleteTwoMarkedRows(obj2Block1, 1, obj1Rows1)
	deleteTwoMarkedRows(obj2Block2, int64(100000), obj1Rows2)
	require.Equal(t, 2, txn.deletedBlocks.size(),
		"both dirty objects carry their own pending block deletion")
	txn.EndStatement()

	// The workspace holds exactly the two mixed metadata batches before the
	// fault-armed admission, and their byte-level shape is captured for
	// the post-failure comparison.
	require.Len(t, txn.writes, 2)
	shape1 := issue25590StatsNamesOfMeta(t, txn, metaIdx1)
	shape2 := issue25590StatsNamesOfMeta(t, txn, metaIdx2)

	// Statement 2's admission: the first clean object's re-publication
	// succeeds (its staging precedes everything), the SECOND one fails
	// deterministically at the counted fault — the reviewer's partial-
	// publication window.
	disarm := issue25590ArmCountedPublishFault(t, 2)
	defer disarm()
	txn.StartStatement()
	err = txn.IncrStatementID(ctx, false)
	require.ErrorContains(t, err, "object compaction publish failed")

	// The failure appended NOTHING: with all-or-nothing staging, the first
	// entry's staged replacement was never published, so no duplicate
	// object references can be stranded inside the rolled-back statement.
	require.Len(t, txn.writes, 2,
		"a second-publication failure must not append any replacement; the pre-staging pass left the first one behind")
	require.Equal(t, 2, txn.deletedBlocks.size(),
		"the failed compaction keeps every affected object's pending deletions")
	require.Equal(t, 2, len(txn.deletedBlocks.offsets[obj2Block1]))
	require.Equal(t, 2, len(txn.deletedBlocks.offsets[obj2Block2]))

	// Both originals stay byte-identical: same objects, same order.
	require.Equal(t, shape1, issue25590StatsNamesOfMeta(t, txn, metaIdx1),
		"the first affected entry's original stays untouched even though its replacement had staged")
	require.Equal(t, shape2, issue25590StatsNamesOfMeta(t, txn, metaIdx2))
	require.NoError(t, txn.checkWorkspaceAccountingLocked())

	// The frontend statement rollback keeps both originals and the marks.
	require.NoError(t, txn.RollbackLastStatement(ctx))
	txn.EndStatement()
	require.Len(t, txn.writes, 2)
	require.Equal(t, 2, txn.deletedBlocks.size(),
		"the rollback after the interrupted admission keeps both pending deletions")
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
}

// TestIssue25590HandoffRetainsOwnershipUntilStorageRecovers pins the
// round-8 P2 review: the terminal handoff used to submit ONE fire-and-forget
// pool task and clear the transaction's list right after the SUBMISSION —
// a task whose DeleteUnpublishedObjects failed merely logged its captured
// names, and the last owner of the orphan was gone. In this reproduction
// every initial delete fails: the abandoned writer's attempt, the
// finalizer's synchronous retry and the first drain round — so the orphan
// can only disappear when the ownership was retained by an engine-level
// queue whose drain keeps retrying until storage accepts the deletion.
func TestIssue25590HandoffRetainsOwnershipUntilStorageRecovers(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)

	pool, err := ants.NewPool(2)
	require.NoError(t, err)
	t.Cleanup(func() { pool.Release() })

	ctx := context.Background()
	failFS := issue25590SwapProcSharedFS(t, txn)
	txn.engine.fs = failFS
	require.NoError(t, txn.engine.ResetGCWorkerPool(pool))

	// A healthy recorded publication whose object must survive every
	// cleanup.
	issue25590AppendInsertEntry(t, txn, 91, 92)
	txn.Lock()
	var pkCount int
	err = txn.dumpInsertBatchLocked(ctx, failFS, txn.GetSnapshotWriteOffset(), &pkCount)
	txn.Unlock()
	require.NoError(t, err)
	require.Equal(t, 2, pkCount)
	require.Len(t, failFS.persistedSnapshot(), 1)
	publishedFile := failFS.persistedSnapshot()[0]
	require.Empty(t, txn.pendingObjectCleanup)

	// The orphan-producing publication (round-15 shape: the publication
	// itself attempts no deletion): THREE delete failures stretch across
	// the first three asynchronous drain rounds, so the recovery on the
	// fourth call proves the retained engine owner kept retrying — a
	// one-shot task would have dropped the name after its single failed
	// attempt.
	issue25590AppendInsertEntry(t, txn, 93)
	failFS.armPostPersistFailure(func() bool { return true })
	failFS.armDeleteFailures(false, 3)
	txn.Lock()
	err = txn.dumpInsertBatchLocked(ctx, failFS, txn.GetSnapshotWriteOffset(), &pkCount)
	txn.Unlock()
	require.ErrorContains(t, err, "post-persist fs write failure")
	require.Len(t, txn.pendingObjectCleanup, 1)
	orphanPath := txn.pendingObjectCleanup[0].name
	require.NotEqual(t, publishedFile, orphanPath)

	txn.FinalizeCommit(ctx)
	require.Nil(t, txn.pendingObjectCleanup,
		"the finalizer transfers residual ownership before transaction destruction")

	require.Eventually(t, func() bool {
		_, statErr := failFS.StatFile(ctx, orphanPath)
		return moerr.IsMoErrCode(statErr, moerr.ErrFileNotFound)
	}, 15*time.Second, 10*time.Millisecond,
		"the retained-owner drain must collect the orphan once storage recovers")
	require.Equal(t, 4, failFS.deleteCallCount(),
		"the drain's three failed rounds precede the accepted fourth delete (round-15: the publication itself attempts none)")
	// The wrapper records the fourth call before the physical deletion, so
	// the drain's queue bookkeeping lands microseconds later; the empty
	// queue is asserted with the same tolerance.
	require.Eventually(t, func() bool {
		txn.engine.unpublishedPending.mu.Lock()
		defer txn.engine.unpublishedPending.mu.Unlock()
		return len(txn.engine.unpublishedPending.names) == 0
	}, 5*time.Second, 5*time.Millisecond,
		"a successful drain round empties the engine queue")
	issue25590RequireKept(t, failFS.FileService, publishedFile,
		"the published object of the recorded file entry")
}

// issue25590WithPendingGCTicker pins the engine GC scheduler's pending-
// queue retry tick to the test pace, restoring the production default on
// cleanup the same way the logtail tests pin their GC tickers.
func issue25590WithPendingGCTicker(t *testing.T, d time.Duration) {
	t.Helper()
	orig := gcUnpublishedPendingObjectTicker
	gcUnpublishedPendingObjectTicker = d
	t.Cleanup(func() { gcUnpublishedPendingObjectTicker = orig })
}

// issue25590RunEngineGCScheduler starts the production engine GC scheduler
// on a test-owned context and joins it bounded on cleanup, the same way
// engine.Close stops the scheduler before retiring the GC pool.
func issue25590RunEngineGCScheduler(t *testing.T, e *Engine) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, e.StartGCScheduler(ctx))
	t.Cleanup(func() {
		cancel()
		e.gcMu.Lock()
		done := e.gcSchedulerDone
		e.gcMu.Unlock()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Log("engine GC scheduler did not join within the bound")
		}
	})
}

// TestIssue25590FinalizationPromptUnderSaturatedGCPool covers the
// round-9 P2 finding on drain scheduling with the production GC pool
// shape engine.go constructs: a blocking ants.Pool of GCPoolSize workers,
// every one occupied by parked GC work. Transaction finalization must
// return without waiting for that capacity — ants' blocking Submit parks
// its caller in an unbounded worker wait — and ownership of the queued
// orphan must survive the full occupancy until a worker frees up, when
// the queued submission alone runs the cleanup. The test schedules
// nothing itself.
func TestIssue25590FinalizationPromptUnderSaturatedGCPool(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)

	// The production pool configuration (engine.go): a blocking
	// ants.Pool of GCPoolSize workers.
	pool, err := ants.NewPool(GCPoolSize)
	require.NoError(t, err)
	t.Cleanup(func() { pool.Release() })

	ctx := context.Background()
	failFS := issue25590SwapProcSharedFS(t, txn)
	txn.engine.fs = failFS
	require.NoError(t, txn.engine.ResetGCWorkerPool(pool))

	// Every GC worker is occupied and stays occupied until the barrier
	// opens: a pre-fix synchronous schedule inside FinalizeCommit parks
	// the finalization in ants' blocking worker wait.
	gcBarrier := make(chan struct{})
	var barrierOnce sync.Once
	releaseGC := func() { barrierOnce.Do(func() { close(gcBarrier) }) }
	defer releaseGC()
	for i := 0; i < GCPoolSize; i++ {
		require.NoError(t, pool.Submit(func() { <-gcBarrier }))
	}

	// A healthy recorded publication whose object must survive every
	// cleanup.
	issue25590AppendInsertEntry(t, txn, 91, 92)
	txn.Lock()
	var pkCount int
	err = txn.dumpInsertBatchLocked(ctx, failFS, txn.GetSnapshotWriteOffset(), &pkCount)
	txn.Unlock()
	require.NoError(t, err)
	require.Len(t, failFS.persistedSnapshot(), 1)
	publishedFile := failFS.persistedSnapshot()[0]

	// The orphan publication (round-15 shape): the writer attempts no
	// deletion — and the terminal path attempts nothing — so ownership
	// must move to the engine queue while every GC worker stays
	// occupied.
	issue25590AppendInsertEntry(t, txn, 93)
	failFS.armPostPersistFailure(func() bool { return true })
	txn.Lock()
	err = txn.dumpInsertBatchLocked(ctx, failFS, txn.GetSnapshotWriteOffset(), &pkCount)
	txn.Unlock()
	require.ErrorContains(t, err, "post-persist fs write failure")
	require.Len(t, txn.pendingObjectCleanup, 1)
	orphanPath := txn.pendingObjectCleanup[0].name
	require.NotEqual(t, publishedFile, orphanPath)

	finalizeDone := make(chan struct{})
	go func() {
		txn.FinalizeCommit(ctx)
		close(finalizeDone)
	}()
	select {
	case <-finalizeDone:
	case <-time.After(3 * time.Second):
		t.Fatal("FinalizeCommit must return promptly while every GC worker is occupied: the drain schedule never waits for pool capacity")
	}
	require.Nil(t, txn.pendingObjectCleanup,
		"the finalizer transfers residual ownership before transaction destruction")
	txn.engine.unpublishedPending.mu.Lock()
	require.Len(t, txn.engine.unpublishedPending.names, 1,
		"a fully occupied GC pool may not lose the handed-off name")
	require.Equal(t, orphanPath, txn.engine.unpublishedPending.names[0])
	txn.engine.unpublishedPending.mu.Unlock()
	require.Equal(t, 0, failFS.deleteCallCount(),
		"round-15: the publication attempted no deletion; the queued submission waits for a worker")
	_, statErr := failFS.StatFile(ctx, orphanPath)
	require.NoError(t, statErr,
		"nothing could have deleted the orphan while every GC worker was parked")

	// Capacity returns, and only the submission the finalization parked
	// when capacity was unavailable may recover the orphan.
	releaseGC()
	require.Eventually(t, func() bool {
		_, statErr := failFS.StatFile(ctx, orphanPath)
		return moerr.IsMoErrCode(statErr, moerr.ErrFileNotFound)
	}, 10*time.Second, 20*time.Millisecond,
		"the queued orphan drains away on the first freed GC worker")
	require.Equal(t, 1, failFS.deleteCallCount(),
		"the recovered drain's single call performs the collection")
	require.Equal(t, []string{orphanPath}, func() []string {
		deleted := failFS.deletedSnapshot()
		return deleted[len(deleted)-1:]
	}(),
		"the recovered drain deletes exactly the queued name")
	require.Eventually(t, func() bool {
		txn.engine.unpublishedPending.mu.Lock()
		defer txn.engine.unpublishedPending.mu.Unlock()
		return len(txn.engine.unpublishedPending.names) == 0
	}, 5*time.Second, 5*time.Millisecond,
		"the drained name leaves the engine queue")
	issue25590RequireKept(t, failFS.FileService, publishedFile,
		"the published object of the recorded file entry")
}

// TestIssue25590ExhaustedLadderRecoversOnEngineGCTick covers the round-9
// P2 finding on the queue's missing retry owner: once a drain ladder
// exhausts itself on a dirty file service, storage that recovers LATER
// may not wait for the arrival of a new orphan — the engine's own GC
// scheduler tick schedules the retry. A scheduling storm while a ladder
// drains must also contribute nothing: the round-9 coalescing keeps
// exactly one pipeline per engine instead of stacking worker-occupying
// ladders for the same backlog.
func TestIssue25590ExhaustedLadderRecoversOnEngineGCTick(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)

	pool, err := ants.NewPool(GCPoolSize)
	require.NoError(t, err)
	t.Cleanup(func() { pool.Release() })

	ctx := context.Background()
	failFS := issue25590SwapProcSharedFS(t, txn)
	txn.engine.fs = failFS
	require.NoError(t, txn.engine.ResetGCWorkerPool(pool))

	issue25590AppendInsertEntry(t, txn, 91, 92)
	txn.Lock()
	var pkCount int
	err = txn.dumpInsertBatchLocked(ctx, failFS, txn.GetSnapshotWriteOffset(), &pkCount)
	txn.Unlock()
	require.NoError(t, err)
	require.Len(t, failFS.persistedSnapshot(), 1)
	publishedFile := failFS.persistedSnapshot()[0]

	// A long dirt window: the drain's whole ladder exhausts itself on
	// failing deletes.
	issue25590AppendInsertEntry(t, txn, 93)
	failFS.armPostPersistFailure(func() bool { return true })
	failFS.armDeleteFailures(false, 1000)
	txn.Lock()
	err = txn.dumpInsertBatchLocked(ctx, failFS, txn.GetSnapshotWriteOffset(), &pkCount)
	txn.Unlock()
	require.ErrorContains(t, err, "post-persist fs write failure")
	require.Len(t, txn.pendingObjectCleanup, 1)
	orphanPath := txn.pendingObjectCleanup[0].name
	require.NotEqual(t, publishedFile, orphanPath)

	txn.FinalizeCommit(ctx)
	require.Nil(t, txn.pendingObjectCleanup,
		"the finalizer transfers residual ownership before transaction destruction")

	// The exhaustion state: exactly eight failed drain ladder rounds —
	// eight delete calls (round-15: the failed publication itself
	// attempts no deletion), the name still owned by the engine queue.
	require.Eventually(t, func() bool {
		txn.engine.unpublishedPending.mu.Lock()
		queued := len(txn.engine.unpublishedPending.names)
		txn.engine.unpublishedPending.mu.Unlock()
		return queued == 1 && failFS.deleteCallCount() == 8
	}, 20*time.Second, 100*time.Millisecond,
		"the exhausted ladder keeps the name queued after exactly eight failed drain rounds")

	// The scheduling storm may not stack a second pipeline onto the same
	// backlog. It runs on its own goroutine because a pre-fix synchronous
	// schedule parks its caller in ants' blocking worker wait.
	go func() {
		for i := 0; i < 8; i++ {
			txn.engine.scheduleUnpublishedPendingDrain()
		}
	}()
	require.Never(t, func() bool {
		return failFS.deleteCallCount() != 8
	}, 2*time.Second, 100*time.Millisecond,
		"a scheduling storm may not stack a second drain ladder onto the same backlog")
	txn.engine.unpublishedPending.mu.Lock()
	require.Len(t, txn.engine.unpublishedPending.names, 1,
		"the storm leaves the queue state unchanged")
	txn.engine.unpublishedPending.mu.Unlock()

	// Storage recovers, and with it the engine's GC scheduler alone: the
	// tick retries the queue without any new orphan and without the test
	// scheduling anything.
	failFS.armDeleteFailures(false, 0)
	issue25590WithPendingGCTicker(t, 5*time.Millisecond)
	issue25590RunEngineGCScheduler(t, txn.engine)
	require.Eventually(t, func() bool {
		_, statErr := failFS.StatFile(ctx, orphanPath)
		return moerr.IsMoErrCode(statErr, moerr.ErrFileNotFound)
	}, 15*time.Second, 20*time.Millisecond,
		"the engine GC scheduler tick alone deletes the recovered backlog")
	require.Equal(t, 9, failFS.deleteCallCount(),
		"the tick-scheduled drain adds exactly one delete call after the eight-round exhaustion")
	require.Eventually(t, func() bool {
		txn.engine.unpublishedPending.mu.Lock()
		defer txn.engine.unpublishedPending.mu.Unlock()
		return len(txn.engine.unpublishedPending.names) == 0
	}, 5*time.Second, 5*time.Millisecond,
		"the autonomous retry empties the engine queue")
	issue25590RequireKept(t, failFS.FileService, publishedFile,
		"the published object of the recorded file entry")
}

// TestIssue25590RejectedScheduleRecoversOnEngineGCTick covers the
// refused-submission half of the round-9 P2 finding: a pool released
// under a live engine refuses every drain submission outright, exactly
// the refusal a pool shutdown produces in production. The refused
// scheduling may lose no ownership, and the engine's GC scheduler — not
// the arrival of a new orphan and not a manual schedule — must fire the
// recovery once a usable pool is in place again.
func TestIssue25590RejectedScheduleRecoversOnEngineGCTick(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)

	// The refused pool: released before the first scheduling, so every
	// Submit fails deterministically while the engine stays alive. The
	// healthy replacement carries the recovery.
	closedPool, err := ants.NewPool(GCPoolSize)
	require.NoError(t, err)
	closedPool.Release()
	healthyPool, err := ants.NewPool(GCPoolSize)
	require.NoError(t, err)
	t.Cleanup(func() { healthyPool.Release() })

	ctx := context.Background()
	failFS := issue25590SwapProcSharedFS(t, txn)
	txn.engine.fs = failFS
	require.NoError(t, txn.engine.ResetGCWorkerPool(closedPool))

	issue25590AppendInsertEntry(t, txn, 91, 92)
	txn.Lock()
	var pkCount int
	err = txn.dumpInsertBatchLocked(ctx, failFS, txn.GetSnapshotWriteOffset(), &pkCount)
	txn.Unlock()
	require.NoError(t, err)
	require.Len(t, failFS.persistedSnapshot(), 1)
	publishedFile := failFS.persistedSnapshot()[0]

	// Round-15 shape: the failed publication deletes nothing, and the
	// drain submission is refused outright: the engine queue is the
	// only owner left — the refusal keeps the name queued
	// deterministically for the tick to recover.
	issue25590AppendInsertEntry(t, txn, 93)
	failFS.armPostPersistFailure(func() bool { return true })
	txn.Lock()
	err = txn.dumpInsertBatchLocked(ctx, failFS, txn.GetSnapshotWriteOffset(), &pkCount)
	txn.Unlock()
	require.ErrorContains(t, err, "post-persist fs write failure")
	require.Len(t, txn.pendingObjectCleanup, 1)
	orphanPath := txn.pendingObjectCleanup[0].name
	require.NotEqual(t, publishedFile, orphanPath)

	txn.FinalizeCommit(ctx)
	require.Nil(t, txn.pendingObjectCleanup,
		"a refused submission loses no ownership: the queue owns the name before the scheduling attempt")
	require.Eventually(t, func() bool {
		txn.engine.unpublishedPending.mu.Lock()
		defer txn.engine.unpublishedPending.mu.Unlock()
		return len(txn.engine.unpublishedPending.names) == 1
	}, 3*time.Second, 20*time.Millisecond,
		"the refused drain leaves the handed-off name queued")
	txn.engine.unpublishedPending.mu.Lock()
	require.Equal(t, orphanPath, txn.engine.unpublishedPending.names[0])
	txn.engine.unpublishedPending.mu.Unlock()
	require.Equal(t, 0, failFS.deleteCallCount(),
		"round-15: the failed publication attempted no deletion; the refused submission deleted nothing")
	_, statErr := failFS.StatFile(ctx, orphanPath)
	require.NoError(t, statErr,
		"nothing could have deleted the orphan while every submission was refused")

	// A usable pool returns, installed under the engine's GC lock the way
	// ResetGCWorkerPool installs pools (whose release of a closed pool
	// would fail legitimately).
	txn.engine.gcMu.Lock()
	txn.engine.gcPool = healthyPool
	txn.engine.gcMu.Unlock()

	// The engine GC scheduler tick alone runs the recovery — no new
	// orphan, no scheduling from the test.
	issue25590WithPendingGCTicker(t, 5*time.Millisecond)
	issue25590RunEngineGCScheduler(t, txn.engine)
	require.Eventually(t, func() bool {
		_, statErr := failFS.StatFile(ctx, orphanPath)
		return moerr.IsMoErrCode(statErr, moerr.ErrFileNotFound)
	}, 15*time.Second, 20*time.Millisecond,
		"the engine GC scheduler tick alone deletes the backlog a refused submission left queued")
	require.Equal(t, 1, failFS.deleteCallCount(),
		"the tick-scheduled drain's single successful delete is the only call")
	require.Eventually(t, func() bool {
		txn.engine.unpublishedPending.mu.Lock()
		defer txn.engine.unpublishedPending.mu.Unlock()
		return len(txn.engine.unpublishedPending.names) == 0
	}, 5*time.Second, 5*time.Millisecond,
		"the autonomous retry empties the engine queue")
	issue25590RequireKept(t, failFS.FileService, publishedFile,
		"the published object of the recorded file entry")
}

// issue25590WriteRawFile writes one physical object file through the
// wrapped shared service itself — bypassing the wrapper's recording — so
// a test can hold real files the storage keeps or collects.
func issue25590WriteRawFile(t *testing.T, fs fileservice.FileService, name string) {
	t.Helper()
	vec := fileservice.IOVector{
		FilePath: name,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(issue25590RawFileData)),
			Data:   issue25590RawFileData,
		}},
	}
	require.NoError(t, fs.Write(context.Background(), vec))
	_, err := fs.StatFile(context.Background(), name)
	require.NoError(t, err, "the raw object file must exist physically")
}

var issue25590RawFileData = []byte("issue25590 raw object file")

// issue25590InjectOwnedOrphan puts one real orphaned object behind the
// transaction's ownership exactly the way the production
// publication-failure paths do: appended to its retained list AND
// accounted in the engine's live-orphan count, the pair the round-12
// admission bound and the terminal handoff's accounting together rely
// on. Direct list assignments would leave the two voices disagreeing.
func issue25590InjectOwnedOrphan(t *testing.T, txn *Transaction, name string) {
	t.Helper()
	txn.pendingObjectCleanup = append(txn.pendingObjectCleanup, pendingObjectCleanupItem{name: name})
	if txn.engine != nil {
		// Round 18: retained ownership is set-keyed — one ledger member
		// per name — so the injection joins the owned-name set and its
		// counter together (issue #25590 round-18 review).
		txn.engine.retainUnpublishedPendingNames([]string{name})
	}
}

// issue25590WithCleanupKnob pins one package-level duration knob for the
// duration of a test and restores the production default on cleanup; the
// pointer keeps the helper free of any knob-specific symbol.
func issue25590WithCleanupKnob(t *testing.T, knob *time.Duration, v time.Duration) {
	t.Helper()
	orig := *knob
	*knob = v
	t.Cleanup(func() { *knob = orig })
}

// issue25590WithCleanupIntKnob is the integer twin of the duration pinner
// above, for the round-11 admission bound.
func issue25590WithCleanupIntKnob(t *testing.T, knob *int, v int) {
	t.Helper()
	orig := *knob
	*knob = v
	t.Cleanup(func() { *knob = orig })
}

// issue25590QueuedNames snapshots the engine queue's names.
func issue25590QueuedNames(e *Engine) []string {
	e.unpublishedPending.mu.Lock()
	defer e.unpublishedPending.mu.Unlock()
	return slices.Clone(e.unpublishedPending.names)
}

// TestIssue25590FailedBatchRetainsWhileLaterBatchesCollect covers the
// round-10 P2 finding on batch liveness: the storage-side delete stops at
// its first failing 1000-name batch, so with 1001 queued names one
// poisoned object in the first batch kept the 1001st name away from every
// drain round, tick and retry. The round-10 partitioning attempts every
// batch independently: the poisoned batch keeps exactly its own names
// and stays queued, the healthy 1001st name is collected on the FIRST
// attempt, and a healthy orphan arriving later drains behind the poison
// instead of accumulating behind the failing batch.
func TestIssue25590FailedBatchRetainsWhileLaterBatchesCollect(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")

	pool, err := ants.NewPool(GCPoolSize)
	require.NoError(t, err)
	t.Cleanup(func() { pool.Release() })

	ctx := context.Background()
	failFS := issue25590SwapProcSharedFS(t, txn)
	txn.engine.fs = failFS
	require.NoError(t, txn.engine.ResetGCWorkerPool(pool))

	// Physical objects: the published survivor, the poisoned first-batch
	// object and the healthy 1001st object of the review's scenario.
	unique := "issue25590-" + uuid.NewString()[:8]
	names := make([]string, 0, 1001)
	for i := 1; i <= 1001; i++ {
		names = append(names, fmt.Sprintf("%s-%04d", unique, i))
	}
	poisonPath, latePath := names[0], names[1000]
	publishedName := unique + "-published"
	issue25590WriteRawFile(t, failFS.FileService, poisonPath)
	issue25590WriteRawFile(t, failFS.FileService, latePath)
	issue25590WriteRawFile(t, failFS.FileService, publishedName)

	// One poisoned object in the first 1000-name batch: every deletion
	// call that contains it fails, persistently and object-specifically.
	failFS.armDeleteFailureFor(poisonPath)
	txn.engine.enqueueUnpublishedPending(names)

	// The healthy 1001st name is collected by the very first drain round -
	// it never waits for the poisoned batch.
	require.Eventually(t, func() bool {
		_, statErr := failFS.StatFile(ctx, latePath)
		return moerr.IsMoErrCode(statErr, moerr.ErrFileNotFound)
	}, 10*time.Second, 20*time.Millisecond,
		"the healthy 1001st name must be collected while the poisoned first batch keeps failing")
	// The poisoned batch stays owned: exactly the thousand names of the
	// first batch remain queued once the first ladder ran out.
	require.Eventually(t, func() bool {
		return failFS.deleteCallCount() == 9
	}, 15*time.Second, 50*time.Millisecond,
		"the first ladder makes exactly nine storage calls: two batches in its first round, the poisoned one alone in rounds two to eight")
	require.Equal(t, 1000, len(issue25590QueuedNames(txn.engine)),
		"only the poisoned batch's thousand names stay queued")
	require.NotContains(t, issue25590QueuedNames(txn.engine), latePath)
	require.Contains(t, issue25590QueuedNames(txn.engine), poisonPath)
	issue25590RequireKept(t, failFS.FileService, poisonPath,
		"the poisoned first-batch object stays on storage")
	issue25590RequireKept(t, failFS.FileService, publishedName,
		"the published object of the recorded file entry")

	// The accumulation counterexample: a healthy orphan arriving LATER
	// must drain behind the still-failing batch, not queue behind it. The
	// engine's own GC-lifecycle tick - the autonomous retry owner of the
	// r9 review - hands the poisoned-plus-healthy queue to a fresh ladder
	// no matter when the tail of the first ladder released its slot:
	// this phase schedules nothing manually.
	issue25590WithPendingGCTicker(t, 5*time.Millisecond)
	issue25590RunEngineGCScheduler(t, txn.engine)
	laterOrphan := unique + "-2001"
	issue25590WriteRawFile(t, failFS.FileService, laterOrphan)
	txn.engine.enqueueUnpublishedPending([]string{laterOrphan})
	require.Eventually(t, func() bool {
		_, statErr := failFS.StatFile(ctx, laterOrphan)
		return moerr.IsMoErrCode(statErr, moerr.ErrFileNotFound)
	}, 15*time.Second, 20*time.Millisecond,
		"a healthy orphan arriving later drains behind the poisoned batch exactly once")
	// Stop the GC scheduler so the second ladder is the last one: the
	// exact-call oracle below stays deterministic.
	txn.engine.gcMu.Lock()
	schedCancel := txn.engine.gcSchedulerCancel
	txn.engine.gcMu.Unlock()
	if schedCancel != nil {
		schedCancel()
	}
	require.Eventually(t, func() bool {
		return failFS.deleteCallCount() == 18
	}, 15*time.Second, 50*time.Millisecond,
		"the second ladder adds exactly nine more storage calls on the same shape")
	require.Equal(t, 1000, len(issue25590QueuedNames(txn.engine)),
		"only the poisoned batch's thousand names remain queued forever failing")
	issue25590RequireKept(t, failFS.FileService, poisonPath,
		"the poisoned object is still on storage after both ladders")
	issue25590RequireKept(t, failFS.FileService, publishedName,
		"the published object of the recorded file entry")
}

// TestIssue25590CloseCancelsBlockedDrainWithinBudget covers the round-10
// P2 finding on shutdown liveness: a background drain blocked inside a
// deletion used to sit on a plain Background context until the storage
// attempt timer expired, with Engine.Close waiting behind it. Every
// background drain now runs on the engine's own cancellable context;
// Close cancels and joins the pipeline inside the cleanup budget, the
// final drain attempt stays bounded by the same budget, and nothing was
// deleted - the aborted calls leave every name owned by the queue.
func TestIssue25590CloseCancelsBlockedDrainWithinBudget(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")

	// A minimal engine the production Close can run against: the
	// zero-value dependency lifecycles (PushClient.Close and
	// dynamicCtx.Close nil-guard everything) and the production pool
	// shape.
	e := txn.engine
	e.pClient = PushClient{}
	failFS := issue25590SwapProcSharedFS(t, txn)
	e.fs = failFS
	pool, err := ants.NewPool(GCPoolSize)
	require.NoError(t, err)
	issue25590WithCleanupKnob(t, &unpublishedPendingCloseDrainTimeout, 2*time.Second)
	require.NoError(t, e.ResetGCWorkerPool(pool))

	ctx := context.Background()
	unique := "issue25590-" + uuid.NewString()[:8]
	orphanPath := unique + "-orphan"
	publishedName := unique + "-published"
	issue25590WriteRawFile(t, failFS.FileService, orphanPath)
	issue25590WriteRawFile(t, failFS.FileService, publishedName)

	// The deletion parks inside the drain: a stalled storage request only
	// its context can abort.
	release := failFS.armDeleteBlock()
	defer release()
	txn.engine.enqueueUnpublishedPending([]string{orphanPath})
	require.Eventually(t, func() bool {
		return failFS.blockedDeleteCalls() == 1
	}, 10*time.Second, 20*time.Millisecond,
		"the background drain must be blocked inside its deletion before shutdown starts")

	closeDone := make(chan struct{})
	started := time.Now()
	go func() {
		_ = e.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
	case <-time.After(30 * time.Second):
		t.Fatal("Engine.Close must terminate inside its cleanup budget when a drain is already blocked in deletion")
	}
	require.Less(t, int64(time.Since(started)), int64(10*time.Second),
		"Close's total cleanup work stays inside the pinned budget")
	_, statErr := failFS.StatFile(ctx, orphanPath)
	require.NoError(t, statErr,
		"an aborted deletion must not remove the object")
	require.Equal(t, 2, failFS.blockedDeleteCalls(),
		"the background drain and the final shutdown attempt each parked once and aborted on their own contexts")
	require.Equal(t, 0, failFS.deleteCallCount(),
		"no deletion reached the storage")
	require.Equal(t, []string{orphanPath}, issue25590QueuedNames(e),
		"ownership outlives the aborted drains: nothing but a successful delete removes the name")
	issue25590RequireKept(t, failFS.FileService, publishedName,
		"the published object of the recorded file entry")
}

// TestIssue25590DrainBudgetBoundsTheLadder covers the budget half of the
// round-10 P2 finding: a whole drain task - not only one storage attempt -
// must stay bounded, so a stalled storage cannot keep a GC worker for
// ladder after ladder. The budget aborts the drain at its stall; the name
// stays queued, and the next scheduled drain collects everything once
// the stall ends.
func TestIssue25590DrainBudgetBoundsTheLadder(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")

	pool, err := ants.NewPool(GCPoolSize)
	require.NoError(t, err)
	t.Cleanup(func() { pool.Release() })

	ctx := context.Background()
	failFS := issue25590SwapProcSharedFS(t, txn)
	txn.engine.fs = failFS
	require.NoError(t, txn.engine.ResetGCWorkerPool(pool))

	unique := "issue25590-" + uuid.NewString()[:8]
	orphanPath := unique + "-orphan"
	publishedName := unique + "-published"
	issue25590WriteRawFile(t, failFS.FileService, orphanPath)
	issue25590WriteRawFile(t, failFS.FileService, publishedName)

	prodBudget := unpublishedPendingDrainBudget
	issue25590WithCleanupKnob(t, &unpublishedPendingDrainBudget, 100*time.Millisecond)
	// The stall resolves well after the budget already aborts the
	// ladder's first attempt - a context-less drain would stay parked
	// until then and collect the file, the budget owns the return.
	release := failFS.armDeleteBlock()
	stall := time.AfterFunc(4*time.Second, release)
	t.Cleanup(func() { stall.Stop(); release() })
	txn.engine.enqueueUnpublishedPending([]string{orphanPath})

	// The ladder's first deletion parks on the stall, the budget aborts
	// it, and no further round runs: one parked call, the file still on
	// storage, the name still owned by the queue - and that state is
	// stable well past the moment the stall resolves.
	require.Eventually(t, func() bool {
		txn.engine.unpublishedPending.mu.Lock()
		defer txn.engine.unpublishedPending.mu.Unlock()
		return len(txn.engine.unpublishedPending.names) == 1
	}, 3*time.Second, 20*time.Millisecond,
		"the queued name survives the budget-spent ladder")
	require.Eventually(t, func() bool {
		return failFS.blockedDeleteCalls() == 1
	}, 3*time.Second, 10*time.Millisecond,
		"the ladder's first deletion parks inside the stall before the budget aborts it")
	require.Never(t, func() bool {
		_, statErr := failFS.StatFile(ctx, orphanPath)
		return failFS.blockedDeleteCalls() != 1 || moerr.IsMoErrCode(statErr, moerr.ErrFileNotFound)
	}, 6*time.Second, 50*time.Millisecond,
		"the budget-spent ladder must make exactly one parked attempt and never collect the stalled file")

	// The stall ends and the next drain - scheduled through a real
	// enqueue - collects everything through the now-unblocked storage.
	// The pinned budget already bounded the stalled ladder; restore the
	// production budget first so the recovery drain's healthy deletion
	// does not also race the 100-millisecond bound under host load.
	// The knob's cleanup still restores the pre-test production value.
	unpublishedPendingDrainBudget = prodBudget
	release()
	laterOrphan := unique + "-2001"
	issue25590WriteRawFile(t, failFS.FileService, laterOrphan)
	txn.engine.enqueueUnpublishedPending([]string{laterOrphan})
	require.Eventually(t, func() bool {
		_, errA := failFS.StatFile(ctx, orphanPath)
		_, errB := failFS.StatFile(ctx, laterOrphan)
		return moerr.IsMoErrCode(errA, moerr.ErrFileNotFound) &&
			moerr.IsMoErrCode(errB, moerr.ErrFileNotFound)
	}, 10*time.Second, 20*time.Millisecond,
		"the drain after the stall collects both objects through one healthy deletion")
	// The file system can observe both deletions a few microseconds before
	// the helper's retire-filter empties the queue under the mutex: a
	// successful drain must still be ABLE to reach the empty queue, never
	// instantly BE there - so wait for the retirement, not a moment.
	require.Eventually(t, func() bool {
		return len(issue25590QueuedNames(txn.engine)) == 0
	}, 5*time.Second, 5*time.Millisecond,
		"a successful drain empties the queue")
	require.Equal(t, 1, failFS.deleteCallCount(),
		"exactly one deletion reached the storage - the recovered batch")
	require.Equal(t, 2, failFS.blockedDeleteCalls(),
		"the stalled round plus the recovered round parked exactly twice")
	issue25590RequireKept(t, failFS.FileService, publishedName,
		"the published object of the recorded file entry")
}

// issue25590BlockedTerminalCleanupCheck drives one terminal destruction
// path while a CONTEXT-BLIND deletion parks inside the file service — the
// LocalFS deleteSingle shape no deadline or cancellation can move (issue
// #25590 round-12 review): the terminal path must terminate without
// waiting on storage at all — the ownership transfer precedes every
// potentially blocking storage I/O — hand the owned name to the engine
// queue whole and untouched, and the engine's own drain must collect it
// once the barrier releases. Both caller states run: an alive context
// and a context that is ALREADY dead when the terminal path begins,
// proving the terminal round consults no caller fate. While the barrier
// holds, exactly the drain's call is parked and nothing has performed
// its storage I/O, so the transaction lock release behind rollbackOwned
// is never hostage to a stalled deletion again.
func issue25590BlockedTerminalCleanupCheck(
	t *testing.T,
	drive func(txn *Transaction, ctx context.Context) error,
) {
	t.Helper()
	for _, dead := range []bool{false, true} {
		t.Run(fmt.Sprintf("deadCaller=%v", dead), func(t *testing.T) {
			colexec.NewServer("")
			txn := newTransactionWithActivePKTableForTest(t, "pk")

			pool, err := ants.NewPool(GCPoolSize)
			require.NoError(t, err)
			t.Cleanup(func() { pool.Release() })

			failFS := issue25590SwapProcSharedFS(t, txn)
			txn.engine.fs = failFS
			require.NoError(t, txn.engine.ResetGCWorkerPool(pool))

			unique := "issue25590-" + uuid.NewString()[:8]
			orphanPath := unique + "-orphan"
			publishedName := unique + "-published"
			issue25590WriteRawFile(t, failFS.FileService, orphanPath)
			issue25590WriteRawFile(t, failFS.FileService, publishedName)

			// The transaction still owns an orphan whose earlier cleanup
			// failed; the terminal round below transfers that ownership
			// (issue #25590 rounds 7-11 provenance) and must not touch
			// storage doing it (round-12 review). The accounting-aware
			// injection keeps the live count the admission bound
			// consults exactly the ones the production append sites keep.
			issue25590InjectOwnedOrphan(t, txn, orphanPath)
			release := failFS.armDeleteIgnoreCtxBlock()
			stall := time.AfterFunc(15*time.Second, release)
			t.Cleanup(func() { stall.Stop(); release() })

			ctx, cancel := context.WithCancel(context.Background())
			if dead {
				cancel()
			} else {
				t.Cleanup(cancel)
			}

			started := time.Now()
			done := make(chan error, 1)
			go func() { done <- drive(txn, ctx) }()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(8 * time.Second):
				t.Fatal("the terminal path must return without waiting on storage: the ownership transfer precedes every potentially blocking deletion")
			}
			require.Less(t, time.Since(started), 5*time.Second,
				"the terminal path returned inside its oracle, nowhere near the parked deletion")

			// The barrier is STILL CLOSED: the terminal round transferred
			// the whole ownership without performing any storage I/O — no
			// terminal attempt exists to park — and the recorded objects
			// are untouched.
			require.Empty(t, txn.pendingObjectCleanup,
				"the terminal round transferred the whole ownership")
			require.Equal(t, []string{orphanPath}, issue25590QueuedNames(txn.engine),
				"the transfer hands the name - whole - to the engine queue")
			require.Equal(t, 0, failFS.deleteCallCount(),
				"while the barrier holds, no call has completed its storage I/O: the terminal round attempted no deletion")
			issue25590RequireKept(t, failFS.FileService, orphanPath,
				"the stalled file the terminal round never waited on")
			issue25590RequireKept(t, failFS.FileService, publishedName,
				"the published object of the recorded file entry")
			// The engine's own scheduled drain — and nothing of the
			// transaction — is the parked caller.
			require.Eventually(t, func() bool {
				return failFS.ignoredDeleteCalls() == 1
			}, 5*time.Second, 5*time.Millisecond,
				"exactly the engine drain's deletion parks in the context-blind barrier")

			// Recovery: the released drain call performs its
			// context-blind deletion — the underlying storage runs on its
			// own, like deleteSingle — and the queue empties only through
			// the successful delete.
			release()
			require.Eventually(t, func() bool {
				_, statErr := failFS.StatFile(context.Background(), orphanPath)
				return moerr.IsMoErrCode(statErr, moerr.ErrFileNotFound)
			}, 10*time.Second, 10*time.Millisecond,
				"the handed-off orphan is collected once the barrier releases")
			require.Equal(t, 1, failFS.deleteCallCount(),
				"the drain's single call performs the whole collection")
			require.Eventually(t, func() bool {
				return len(issue25590QueuedNames(txn.engine)) == 0
			}, 5*time.Second, 5*time.Millisecond,
				"a successful drain empties the queue")
			issue25590RequireKept(t, failFS.FileService, publishedName,
				"the published object of the recorded file entry, still untouched")
		})
	}
}

// TestIssue25590RollbackReturnsBoundedWhileDeleteParked pins the
// round-12 review's Rollback face: rollbackOwned releases the
// transaction locks only after workspace.Rollback returns, and a
// context-blind deletion — storage fate no deadline can move — must not
// hold the rollback behind it at all: the terminal round performs no
// storage I/O, for an alive caller and for an already-canceled caller
// alike.
func TestIssue25590RollbackReturnsBoundedWhileDeleteParked(t *testing.T) {
	issue25590BlockedTerminalCleanupCheck(t,
		func(txn *Transaction, ctx context.Context) error {
			return txn.Rollback(ctx)
		})
}

// TestIssue25590FinalizeCommitReturnsBoundedWhileDeleteParked is the
// successful-commit face of the same round-12 finding: the commit
// finalizer runs the shared terminal ownership transfer before
// delTransaction, and no storage stall — context-blind included — can
// hold it, because the transfer alone carries the names to the engine.
func TestIssue25590FinalizeCommitReturnsBoundedWhileDeleteParked(t *testing.T) {
	issue25590BlockedTerminalCleanupCheck(t,
		func(txn *Transaction, ctx context.Context) error {
			txn.FinalizeCommit(ctx)
			return nil
		})
}

// TestIssue25590FinalizeCommitWithUnknownResultReturnsBoundedWhileDeleteParked
// is the unknown-result face: that finalizer runs on a context that may
// already be dead — the terminal round consults neither the caller's
// fate nor any storage, so both caller states terminate identically and
// the proven-unsafe-to-keep names still leave with the engine.
func TestIssue25590FinalizeCommitWithUnknownResultReturnsBoundedWhileDeleteParked(t *testing.T) {
	issue25590BlockedTerminalCleanupCheck(t,
		func(txn *Transaction, ctx context.Context) error {
			txn.FinalizeCommitWithUnknownResult(ctx)
			return nil
		})
}

// TestIssue25590CloseDetachesUncancellableCleanupWithinBudget pins the
// second round-11 finding: a deletion that IGNORES its context - the
// LocalFS deleteSingle shape - parks the drain, and neither the round-10
// cancellation nor any deadline can move it. Engine.Close must still
// terminate inside ONE shutdown budget: the final deletion detaches
// behind the shared deadline carrying its own references until it
// finishes, the pool release stops waiting on the same deadline instead
// of blocking on the parked worker through ReleaseContext, and ownership
// stays with the queue until a delete actually succeeds.
func TestIssue25590CloseDetachesUncancellableCleanupWithinBudget(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")

	// A minimal engine the production Close can run against: the
	// zero-value dependency lifecycles and the production pool shape.
	e := txn.engine
	e.pClient = PushClient{}

	pool, err := ants.NewPool(GCPoolSize)
	require.NoError(t, err)
	issue25590WithCleanupKnob(t, &unpublishedPendingCloseDrainTimeout, 2*time.Second)

	failFS := issue25590SwapProcSharedFS(t, txn)
	e.fs = failFS
	require.NoError(t, e.ResetGCWorkerPool(pool))

	unique := "issue25590-" + uuid.NewString()[:8]
	orphanPath := unique + "-orphan"
	publishedName := unique + "-published"
	issue25590WriteRawFile(t, failFS.FileService, orphanPath)
	issue25590WriteRawFile(t, failFS.FileService, publishedName)

	// The deletion parks on a barrier its context cannot cross: only the
	// explicit release below ends the stall.
	release := failFS.armDeleteIgnoreCtxBlock()
	t.Cleanup(release)
	e.enqueueUnpublishedPending([]string{orphanPath})
	require.Eventually(t, func() bool {
		return failFS.ignoredDeleteCalls() == 1
	}, 10*time.Second, 10*time.Millisecond,
		"the background drain must be parked in the context-blind deletion before shutdown starts")

	closeDone := make(chan struct{})
	started := time.Now()
	go func() {
		_ = e.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
	case <-time.After(20 * time.Second):
		t.Fatal("Engine.Close must stay inside its budget while a deletion ignores its context")
	}
	require.Less(t, int64(time.Since(started)), int64(10*time.Second),
		"Close's whole shutdown tail - join, final attempt, pool release - fits the pinned 2s budget plus telemetry")
	require.True(t, pool.IsClosed(),
		"the bounded release closed the pool immediately; only the worker-exit wait is bounded")

	// The barrier is still closed: nothing was deleted, the detached and
	// parked work owns nothing but references, and the queue keeps the
	// name exactly as a crash mid-queue would.
	require.Equal(t, []string{orphanPath}, issue25590QueuedNames(e),
		"ownership outlives the detached, still-running cleanup")
	issue25590RequireKept(t, failFS.FileService, orphanPath,
		"the context-blind deletion touched nothing before its release")
	issue25590RequireKept(t, failFS.FileService, publishedName,
		"the published object of the recorded file entry")
	require.Eventually(t, func() bool {
		return failFS.ignoredDeleteCalls() == 2
	}, 5*time.Second, 5*time.Millisecond,
		"the drain and the detached final attempt are both parked in the context-blind deletion")
	require.Equal(t, 0, failFS.deleteCallCount(),
		"while the barrier holds, neither parked call has performed its storage I/O")

	// Recovery: release the barrier - the parked drain performs its
	// context-blind deletion like LocalFS (the object goes away), the
	// detached final attempt races harmlessly (a second delete of the
	// same file reports not-found and is tolerated), and the queue
	// empties only through the successful delete.
	release()
	require.Eventually(t, func() bool {
		_, statErr := failFS.StatFile(context.Background(), orphanPath)
		return moerr.IsMoErrCode(statErr, moerr.ErrFileNotFound)
	}, 15*time.Second, 10*time.Millisecond,
		"the context-blind park itself still completes the collection after release")
	require.Eventually(t, func() bool {
		return failFS.deleteCallCount() == 2
	}, 5*time.Second, 5*time.Millisecond,
		"both detached attempts finished their storage I/O after the release")
	require.Eventually(t, func() bool {
		return len(issue25590QueuedNames(e)) == 0
	}, 10*time.Second, 5*time.Millisecond,
		"a successful drain empties the queue even though shutdown already returned")
}

// TestIssue25590AdmissionBoundsRetainedOrphanBacklog pins the third
// round-11 finding: retained orphan ownership needs a bound that neither
// drain rounds nor time budgets gave. A real failed publication under a
// delete outage produces the chain the bound exists for; with the engine
// queue retained at the pinned admission bound, the next publication is
// refused BEFORE persisting (bounded retention, no new orphans), the
// refusal keeps every already-owned name (lossless ownership: a terminal
// handoff still transfers), and once the outage ends the engine's own GC
// tick collects the backlog so admission re-opens and the refused
// statement publishes (recovery under repeated publication/delete
// failures).
func TestIssue25590AdmissionBoundsRetainedOrphanBacklog(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)
	defer func() {
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	}()

	failFS := issue25590SwapProcSharedFS(t, txn)
	txn.engine.fs = failFS
	pool, err := ants.NewPool(GCPoolSize)
	require.NoError(t, err)
	t.Cleanup(func() { pool.Release() })
	require.NoError(t, txn.engine.ResetGCWorkerPool(pool))

	issue25590WithCleanupIntKnob(t, &unpublishedPendingAdmissionLimit, 4)

	ctx := context.Background()
	admit := func() { issue25590AdmitBoundary(t, txn, ctx) }
	require.NoError(t, txn.checkWorkspaceAccountingLocked())

	// The sustained outage: deletions keep failing, so every orphan stays
	// retained whoever owns it.
	failFS.armDeleteFailures(false, 1_000_000)

	// A REAL failed publication under that outage: the writer persists
	// physically, the publication fails, and the failed writer hands
	// its names over without deleting anything (round-15) - the
	// publication failure that produces transaction-owned orphans, held
	// by the sustained delete failures wherever they go.
	admit()
	issue25590AppendInsertEntry(t, txn, 11, 12)
	txn.EndStatement()
	persistedBefore := len(failFS.persistedSnapshot())
	failFS.armPostPersistFailure(func() bool { return true })
	txn.writeWorkspaceThreshold = 0
	txn.StartStatement()
	err = txn.IncrStatementID(ctx, false)
	require.ErrorContains(t, err, "post-persist fs write failure")
	txn.UpdateSnapshotWriteOffset()
	require.NoError(t, txn.RollbackLastStatement(ctx))
	txn.EndStatement()
	require.NotEmpty(t, txn.pendingObjectCleanup,
		"the failed publication's objects stay with the transaction, awaiting terminal cleanup")
	pendingName := txn.pendingObjectCleanup[0].name
	require.Greater(t, len(failFS.persistedSnapshot()), persistedBefore,
		"the failed publication persisted physically before failing")

	// Retained backlog to the pinned bound: the same engine queue every
	// terminal handoff of the outage transfers into (the round-8 lossless
	// transfer), each name with its physical object present.
	unique := "issue25590-" + uuid.NewString()[:8]
	retained := make([]string, 0, 4)
	for i := 0; i < 4; i++ {
		name := fmt.Sprintf("%s-retained-%03d", unique, i)
		issue25590WriteRawFile(t, failFS.FileService, name)
		txn.engine.enqueueUnpublishedPending([]string{name})
		retained = append(retained, name)
	}
	require.Len(t, issue25590QueuedNames(txn.engine), 4,
		"the retained orphan backlog reached the pinned admission bound")

	// THE BOUND: the transaction's next real dump refuses to persist more
	// storage objects - before any publication, so no new orphan exists -
	// while everything already queued stays exactly owned. The refused
	// boundary's pre-gate step first transferred the real failed
	// publication's transaction-owned orphan losslessly (issue #25590
	// round-14): the queue carries the whole retention now, the transfer
	// is not a publication, and the refusal adds no name of its own.
	txn.StartStatement()
	err = txn.IncrStatementID(ctx, false)
	require.ErrorContains(t, err, "workspace admission paused",
		"a dump that would persist more storage objects is refused at the admission bound")
	require.Len(t, issue25590QueuedNames(txn.engine), 5,
		"bounded retention: the refusal adds no name of its own beyond the boundary's lossless transfer")
	persistedAtRefusal := len(failFS.persistedSnapshot())
	txn.UpdateSnapshotWriteOffset()
	require.NoError(t, txn.RollbackLastStatement(ctx))
	txn.EndStatement()
	require.Equal(t, persistedAtRefusal, len(failFS.persistedSnapshot()),
		"the refusal happened before any additional persistence")

	// Lossless ownership is never the bound's price: a terminal handoff
	// of an already-orphaned name still transfers at the bound - the
	// bound stops new orphans, not the handoff of existing ones.
	handoffName := unique + "-handoff"
	issue25590WriteRawFile(t, failFS.FileService, handoffName)
	txn.engine.enqueueUnpublishedPending([]string{handoffName})
	require.Len(t, issue25590QueuedNames(txn.engine), 6,
		"lossless ownership: a terminal handoff transfers even at the bound")

	// RECOVERY: the outage ends, and the engine's own GC tick collects
	// the whole retained backlog so admission re-opens.
	failFS.stopPostPersistFailure()
	failFS.armDeleteFailures(false, 0)
	issue25590WithPendingGCTicker(t, 5*time.Millisecond)
	issue25590RunEngineGCScheduler(t, txn.engine)
	require.Eventually(t, func() bool {
		return len(issue25590QueuedNames(txn.engine)) == 0
	}, 30*time.Second, 20*time.Millisecond,
		"recovery: the engine's own GC tick collects the whole retained backlog once the outage ends")
	for _, gone := range append(append(retained, handoffName), pendingName) {
		issue25590RequireGone(t, failFS.FileService, gone,
			"recovered storage collected the retained orphan")
	}

	// Admission re-opens: the SAME statement now publishes its staged
	// rows, and nothing of the failed publication survives
	// transaction-owned: the refused boundary transferred it (issue
	// #25590 round-14) and the engine's healthy tick collected it.
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"admission re-opens once the drain caught up")
	txn.UpdateSnapshotWriteOffset()
	txn.EndStatement()
	require.Empty(t, txn.pendingObjectCleanup,
		"nothing stays transaction-owned: the boundary transferred it and the tick collected it")
	published := 0
	for i := range txn.writes {
		if txn.writes[i].fileName != "" {
			published++
		}
	}
	require.GreaterOrEqual(t, published, 1,
		"the previously refused statement publishes its staged rows")
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
	require.Len(t, issue25590QueuedNames(txn.engine), 0,
		"the queue ends empty")
}

// TestIssue25590AdmissionBindsActiveTxnOrphanGrowth pins the round-12
// review's counterexample: an EXPLICIT transaction under a sustained
// publication/delete outage hands nothing to the engine — every failed
// publication stages its name on the transaction's own list — so a
// bound that counted only the engine queue never tripped while the
// transaction's own backlog grew without limit through the report's
// retry loop (fail, roll the statement back, retry). The bound now
// accounts the transaction-owned orphans too: at the pinned limit the
// next real dump refuses BEFORE persisting — the retention riding the
// union of the transaction's own list and the queue its boundaries
// transfer to (issue #25590 round-14) — and once the outage ends the
// engine's own GC tick collects the backlog and the statement
// publishes: bounded retention, transfer-on-admission, recovery and
// re-opened admission, with no manual queue filling anywhere.
func TestIssue25590AdmissionBindsActiveTxnOrphanGrowth(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)
	defer func() {
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	}()

	failFS := issue25590SwapProcSharedFS(t, txn)
	txn.engine.fs = failFS
	pool, err := ants.NewPool(GCPoolSize)
	require.NoError(t, err)
	t.Cleanup(func() { pool.Release() })
	require.NoError(t, txn.engine.ResetGCWorkerPool(pool))

	issue25590WithCleanupIntKnob(t, &unpublishedPendingAdmissionLimit, 2)

	ctx := context.Background()
	admit := func() { issue25590AdmitBoundary(t, txn, ctx) }
	require.NoError(t, txn.checkWorkspaceAccountingLocked())

	// The sustained outage: publications persist physically and then
	// fail, and every deletion fails — the chain that produces
	// transaction-owned orphans.
	failFS.armPostPersistFailure(func() bool { return true })
	failFS.armDeleteFailures(false, 1_000_000)

	// One well-formed boundary first; with the default threshold and
	// nothing staged it persists nothing.
	admit()
	txn.EndStatement()

	// Two failed publication rounds on ONE still-active transaction:
	// each round stages a fresh statement, its next admission dumps it,
	// the publication persists physically, fails, its emergency delete
	// fails too, and the round's fresh name lands on the transaction's
	// own list — while the pre-gate step transfers the PREVIOUS round's
	// name to the engine queue at every admission boundary (issue
	// #25590 round-14): only the newest failed name stays
	// transaction-owned between failures.
	for round := 0; round < 2; round++ {
		// The fixture's post-persist failure is one shot per arm, like the
		// single failing publication each round models.
		failFS.armPostPersistFailure(func() bool { return true })
		txn.StartStatement()
		issue25590AppendInsertEntry(t, txn, int64(100+2*round), int64(101+2*round))
		txn.EndStatement()
		txn.writeWorkspaceThreshold = 0
		txn.StartStatement()
		err := txn.IncrStatementID(ctx, false)
		require.ErrorContains(t, err, "post-persist fs write failure",
			"round %d's real publication fails under the outage", round)
		txn.UpdateSnapshotWriteOffset()
		require.NoError(t, txn.RollbackLastStatement(ctx))
		txn.EndStatement()
		require.Len(t, txn.pendingObjectCleanup, 1,
			"the failed round's newest name stays owned by the transaction until the next boundary")
		if round == 0 {
			require.Empty(t, issue25590QueuedNames(txn.engine),
				"nothing reaches the engine before the next statement boundary")
		} else {
			require.Len(t, issue25590QueuedNames(txn.engine), 1,
				"the previous round's transferred name rides the engine queue")
		}
	}
	queuedNames := issue25590QueuedNames(txn.engine)
	ownedNames := append(append([]string{}, queuedNames...),
		txn.pendingObjectCleanupNames()...)

	// THE BOUND: the third round's real dump refuses BEFORE persisting —
	// the retained names, wherever they ride, have reached the pinned
	// bound — so no additional object exists.
	persistedAtBound := len(failFS.persistedSnapshot())
	txn.StartStatement()
	issue25590AppendInsertEntry(t, txn, 300)
	txn.EndStatement()
	txn.writeWorkspaceThreshold = 0
	txn.StartStatement()
	err = txn.IncrStatementID(ctx, false)
	require.ErrorContains(t, err, "workspace admission paused",
		"the dump that would persist more storage objects is refused on the transaction's own retained names")
	txn.UpdateSnapshotWriteOffset()
	require.NoError(t, txn.RollbackLastStatement(ctx))
	txn.EndStatement()
	// The refused dump's pre-gate step transferred the last
	// transaction-owned name before the gate refused: the whole pinned
	// bound rides the queue now, nothing lives in two owners, and the
	// refusal itself changed no ownership.
	require.Empty(t, txn.pendingObjectCleanup,
		"the pre-gate step transferred the last transaction-owned name (issue #25590 round-14)")
	require.Equal(t, ownedNames, issue25590QueuedNames(txn.engine),
		"the refusal changes no ownership: every retained name stays queued whole")
	require.Equal(t, persistedAtBound, len(failFS.persistedSnapshot()),
		"the refusal happened before any additional persistence")

	// RECOVERY: the outage ends and the engine's own GC tick — not any
	// statement's synchronous work (issue #25590 round-14 review) —
	// collects the queued backlog, so admission re-opens with the queue
	// empty, the accounting clean, and the publication then goes
	// through.
	failFS.stopPostPersistFailure()
	failFS.armDeleteFailures(false, 0)
	issue25590WithPendingGCTicker(t, 5*time.Millisecond)
	require.Eventually(t, func() bool {
		return len(issue25590QueuedNames(txn.engine)) == 0
	}, 30*time.Second, 20*time.Millisecond,
		"the engine's tick drain retires the queued backlog once the outage ends")
	persistedBeforeRecovery := len(failFS.persistedSnapshot())
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"admission re-opens once the engine's drain released the bound")
	txn.UpdateSnapshotWriteOffset()
	txn.EndStatement()
	require.Empty(t, txn.pendingObjectCleanup,
		"nothing is transaction-owned after the recovery admission")
	require.Greater(t, len(failFS.persistedSnapshot()), persistedBeforeRecovery,
		"the previously refused statement publishes its staged rows")
	require.Empty(t, issue25590QueuedNames(txn.engine),
		"the queue stays empty: the recovery publication added no orphan")
	for _, gone := range ownedNames {
		issue25590RequireGone(t, failFS.FileService, gone,
			"the recovered storage collected the transaction-owned orphan")
	}
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
	require.NoError(t, txn.Rollback(ctx))
}

// TestIssue25590ConcurrentAdmissionsCannotBypassTheBound pins the
// concurrency requirement across the round-12/18 reviews: in-flight
// capacity is reserved in OBJECT units, and every reservation is part of
// the bound each concurrent reservation consults inside the ONE
// queue-mutex section, so reservations racing the last capacity cannot
// slip past it together. The seam is the production per-object gate a
// spilling writer takes before each object persists, at a pinned limit
// with the queue one name under it: while one reservation is in flight,
// every concurrent taker is refused, and the settled unit re-opens the
// gate for exactly the next object.
func TestIssue25590ConcurrentAdmissionsCannotBypassTheBound(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")

	// No GC worker is installed, so no scheduled drain can run and no
	// physical file is needed: the queue keeps its names and the seam is
	// observed exactly.
	e := txn.engine
	issue25590WithCleanupIntKnob(t, &unpublishedPendingAdmissionLimit, 4)

	unique := "issue25590-" + uuid.NewString()[:8]
	for i := 0; i < 3; i++ {
		e.enqueueUnpublishedPending([]string{fmt.Sprintf("%s-seam-%03d", unique, i)})
	}
	require.Len(t, issue25590QueuedNames(e), 3,
		"the retained queue sits one name under the pinned bound")

	// One in-flight object reservation holds the last unit: the object's
	// name simply settles through the engine's abandon seam, because
	// the reservation below never has a publication behind it.
	held := unique + "-held"
	require.NoError(t, e.reserveUnpublishedPendingObject(held),
		"the first object's reservation takes the one remaining capacity")

	// While that object is in flight, racing reservations must all see
	// the held unit: none may take a second one at the bound.
	const racers = 8
	results := make(chan bool, racers)
	for i := 0; i < racers; i++ {
		go func() {
			err := e.reserveUnpublishedPendingObject(
				unique + "-racer")
			if err == nil {
				// Defensive only: at the bound this cannot happen, and
				// the settle keeps the ledger exact if it ever did.
				e.abandonUnpublishedPendingReservations(
					[]string{unique + "-racer"})
			}
			results <- err == nil
		}()
	}
	admitted := 0
	for i := 0; i < racers; i++ {
		if <-results {
			admitted++
		}
	}
	require.Equal(t, 0, admitted,
		"a held in-flight object unit reserves the boundary capacity: no concurrent reservation slips past the bound")
	require.Len(t, issue25590QueuedNames(e), 3,
		"the refused racers changed no ownership either")
	e.unpublishedPending.mu.Lock()
	inFlight := int(e.unpublishedPending.inFlight)
	e.unpublishedPending.mu.Unlock()
	require.Equal(t, 1, inFlight,
		"only the held unit remains in flight after the raced round")

	// The settled unit re-opens the gate for exactly the next object,
	// and holding it again refills the boundary: one more object at the
	// same position is refused.
	e.abandonUnpublishedPendingReservations([]string{held})
	require.NoError(t, e.reserveUnpublishedPendingObject(held),
		"the settled unit re-opens the gate for the next object")
	require.ErrorContains(t, e.reserveUnpublishedPendingObject(
		unique+"-next"), "workspace admission paused",
		"the re-taken unit again reserves the boundary's only capacity")
	e.abandonUnpublishedPendingReservations([]string{held})
	e.unpublishedPending.mu.Lock()
	inFlight = int(e.unpublishedPending.inFlight)
	e.unpublishedPending.mu.Unlock()
	require.Zero(t, inFlight,
		"in-flight accounting alone moved the check: the queue itself stayed exactly as owned")
}

// TestIssue25590DumpReservesCapacityPerPersistedObject pins the round-18
// review finding 1: the round-17 admission reserved ONE unit per DUMP,
// so a spilling writer producing several objects could push the
// retained ledger past the promised bound - with the bound one name
// short, an admitted three-object dump exceeded the cap by two. Round 18
// reserves capacity in OBJECT units: the spilling writers' capacity hook
// takes one in-flight unit per object right before it persists, the
// second object of a boundary dump is refused while the first is still
// in flight, and the failed publication's epilogue converts exactly the
// units of the names it persisted. The boundary below seeds the queue
// one name under the pinned bound and drives a real multi-object dump:
// the first object takes the last unit, the second is refused BEFORE
// anything of it persists, and the settled ledger sits exactly at the
// bound. The concurrent leg seeds an independent engine's queue three
// names under the same pinned bound and races two one-object dumps for
// its single free unit: whichever wins, the other is refused before
// anything of it persists and that ledger never exceeds the bound.
func TestIssue25590DumpReservesCapacityPerPersistedObject(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)
	defer func() {
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	}()
	ctx := context.Background()
	e := txn.engine
	e.pClient = PushClient{}

	failFS := issue25590SwapProcSharedFS(t, txn)
	e.fs = failFS

	issue25590WithCleanupIntKnob(t, &unpublishedPendingAdmissionLimit, 4)
	// One spilled object per dumped entry, deterministically: every
	// write into the dump's writer spills at a threshold of one byte.
	issue25590WithCleanupIntKnob(t, &workspaceDumpWriteS3Threshold, 1)

	unique := "issue25590-" + uuid.NewString()[:8]
	for i := 0; i < 3; i++ {
		e.enqueueUnpublishedPending([]string{fmt.Sprintf("%s-retain-%03d", unique, i)})
	}
	require.Len(t, issue25590QueuedNames(e), 3,
		"the retained queue sits one name under the pinned bound")

	// A real three-entry dump: each entry's write spills an object, and
	// the spilling writer's first object takes the bound's last unit.
	for i := 0; i < 3; i++ {
		issue25590AppendInsertEntry(t, txn, 1, int64(i+1))
	}
	pkCount := new(int)
	var err error
	txn.Lock()
	err = txn.dumpInsertBatchLocked(ctx, failFS, 0, pkCount)
	txn.Unlock()
	require.ErrorContains(t, err, "workspace admission paused",
		"the second object of the boundary dump is refused while the first holds the last unit")
	require.Equal(t, 1, len(failFS.persistedSnapshot()),
		"only the admitted first object persisted: the refused spill persisted nothing")

	ledgerSnapshot := func(eng *Engine) (queued int, live int64, inFlight int64) {
		eng.unpublishedPending.mu.Lock()
		defer eng.unpublishedPending.mu.Unlock()
		return len(eng.unpublishedPending.names), eng.unpublishedPending.live,
			eng.unpublishedPending.inFlight
	}
	queued, live, inFlight := ledgerSnapshot(e)
	require.Zero(t, inFlight,
		"the failed dump settled its reservation exactly: no unit outlives the dump")
	require.Equal(t, int64(1), live,
		"the one persisted object retained as transaction-owned obligation")
	require.Equal(t, 3, queued)
	require.Equal(t, 4, queued+int(live)+int(inFlight),
		"the round-18 review finding 1 bound: the ledger sits EXACTLY at the limit - on the round-17 base this scene exceeded the cap by two after its single unit released")
	require.Len(t, txn.pendingObjectCleanup, 1,
		"only the persisted first object became a cleanup obligation")
	require.Equal(t, failFS.persistedSnapshot()[0], txn.pendingObjectCleanup[0].name,
		"the failed dump's one cleanup obligation is exactly the persisted object's name")

	// The concurrent leg, on an independent engine's bound: three
	// retained names leave exactly one unit of capacity, and two
	// independent one-object dumps race for it. Exactly one dumps and
	// records, the other is refused before anything of it persists;
	// whichever order the mutex serves them in, that engine's ledger
	// cannot exceed the bound.
	txnA := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txnA)
	eA := txnA.engine
	eA.pClient = PushClient{}
	eA.fs = failFS
	txnB := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txnB)
	txnB.engine = eA

	for i := 0; i < 3; i++ {
		eA.enqueueUnpublishedPending([]string{fmt.Sprintf("%s-race-%03d", unique, i)})
	}

	issue25590AppendInsertEntry(t, txnA, 11)
	issue25590AppendInsertEntry(t, txnB, 22)

	dumpDone := make(chan error, 2)
	go func() {
		txnA.Lock()
		dumpDone <- txnA.dumpInsertBatchLocked(ctx, failFS, 0, new(int))
		txnA.Unlock()
	}()
	go func() {
		txnB.Lock()
		dumpDone <- txnB.dumpInsertBatchLocked(ctx, failFS, 0, new(int))
		txnB.Unlock()
	}()
	refused, admitted := 0, 0
	for i := 0; i < 2; i++ {
		if err := <-dumpDone; err != nil {
			require.ErrorContains(t, err, "workspace admission paused",
				"the raced loser's refusal is the admission bound, not a storage failure")
			refused++
		} else {
			admitted++
		}
	}
	require.Equal(t, 1, admitted, "exactly one racing dump takes the single free unit")
	require.Equal(t, 1, refused, "the other racing dump is refused at the bound")

	queued, live, inFlight = ledgerSnapshot(eA)
	require.Equal(t, 4, queued+int(live)+int(inFlight),
		"concurrent writers share the remaining capacity without exceeding the bound")
	require.Zero(t, inFlight,
		"the racing rounds settled their reservations: the loser's refusal booked no unit")
	// Exactly one winner recorded its dumped object; its name sits in
	// the owned ledger (live) alongside the untouched retained queue,
	// and the refused loser recorded nothing.
	recorders := 0
	for _, contestant := range []*Transaction{txnA, txnB} {
		if issue25590RecordedLen(contestant) == 1 {
			recorders++
		}
	}
	require.Equal(t, 1, recorders,
		"exactly one racing dump recorded its admitted object; the refused one recorded nothing")
	require.Equal(t, int64(1), live,
		"the winning dump's converted obligation is the race engine's only owned name")
}

// TestIssue25590RollbackProducersEnterTheCapacityBound pins the round-18
// review finding 2: only the workspace DUMP was admitted, so objects a
// ROLLBACK could hand to the queue - the direct-S3 record branch, the
// successfully dumped objects awaiting the terminal, the compaction
// rewrites - entered the engine behind the bound. Repeated
// record-and-rollback transactions could keep growing the retained
// backlog after the advertised limit was reached. Round 18 admits every
// recorded obligation at CREATION through the single record choke, so
// the loop below is refused at the record the moment the bound is
// full: the recorded-obligation backlog can cross the bound via NO
// terminal path. The round-21 r5 contract adds one refused-output class
// (finding G2): a refused record's reservation-less name is
// fail-open-retained by the recording side so refused output never
// loses its cleanup owner, which books one owned member per refusal -
// the drain's not-found tolerance retires it as soon as deletes can
// run. The round-22 r6 review rejected the r21 sustained phase that
// ASSERTED that per-refusal growth (finding H2): the fresh refusal
// feed closes at its source when every new obligation — remote-relayed
// names included — books through the same per-object admission gate
// the writers use, and the sustained rounds below hold the bounded
// population flat behind a stalled drain instead of counting it up.
func TestIssue25590RollbackProducersEnterTheCapacityBound(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)
	ctx := context.Background()
	e := txn.engine
	e.pClient = PushClient{}

	failFS := issue25590SwapProcSharedFS(t, txn)
	e.fs = failFS
	// A stalled delete keeps every handed name queued: nothing the
	// rollback hands to the queue retires during the scene, so the
	// bound must hold on recorded obligations alone.
	unblockDeletes := failFS.armDeleteBlock()
	t.Cleanup(unblockDeletes)

	issue25590WithCleanupIntKnob(t, &unpublishedPendingAdmissionLimit, 8)

	// The production record path of the direct-S3 branch: a BlockInfo
	// record carrying object stats joins the workspace through the
	// record choke, exactly like txnTable.Write's metadata branch.
	recordObject := func(tx *Transaction) (string, error) {
		rowid := types.RandomRowid()
		oid := rowid.BorrowObjectID()
		stats := objectio.NewObjectStatsWithObjectID(oid, false, false, true)
		// One block per record object, exactly like a real dumped object:
		// the recorded-file selectors key off the BlockInfo column's
		// row count, so a zero-block stats would record a name the
		// rollback selector can never find again.
		require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, 1))
		require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 1))
		bat := colexec.AllocCNS3ResultBat(false)
		require.NoError(t, colexec.ExpandObjectStatsToBatch(
			tx.proc.GetMPool(), false, bat, true, *stats))
		defer bat.Clean(tx.proc.GetMPool())
		name := stats.ObjectName().String()
		tx.Lock()
		err := tx.WriteFileLocked(
			INSERT, 1, 7, 1, "db", "tbl", name, bat, DNStore{})
		tx.Unlock()
		return name, err
	}

	// Records fill the bound one by one; the ninth is refused at
	// creation with the workspace untouched.
	var recorded []string
	for i := 0; i < 8; i++ {
		name, err := recordObject(txn)
		require.NoError(t, err, "record %d admits inside the bound", i)
		recorded = append(recorded, name)
	}
	_, err := recordObject(txn)
	require.ErrorContains(t, err, "workspace admission paused",
		"the ninth record's obligation is refused at creation - on the round-17 base this record succeeded and only the terminal could reveal it")
	require.Len(t, txn.writes, 8,
		"the refused record left the workspace untouched")

	// The ninth's refusal fail-open-retains its reservation-less name
	// (issue #25590 round-21 review, finding G2): a refused record's
	// output must keep a reachable cleanup owner on the recording
	// engine, exactly like a remote fragment's persisted files, so the
	// recording-side retain books the refused name as one owned member
	// behind the already-full bound. The refusal admitted NOTHING (the
	// workspace is untouched above); the retained member is the owner
	// the review demanded for refused output, handed to the queue by the
	// failing statement's own terminal and retired as not-found by the
	// drain when no file ever landed.
	e.unpublishedPending.mu.Lock()
	live, inFlight := e.unpublishedPending.live, e.unpublishedPending.inFlight
	queued := len(e.unpublishedPending.names)
	e.unpublishedPending.mu.Unlock()
	require.Equal(t, 9, queued+int(live)+int(inFlight),
		"eight recorded obligations fill the bound and the ninth's refusal fail-open-retains its name as the ninth owner, before any terminal")
	require.Equal(t, int64(9), live,
		"every recorded name plus the refused record's retained name sit in the owned ledger - the round-18 admission class plus the r21 refused-output owner")

	// The rollback hands exactly them to the queue, losslessly and
	// without admitting anything: eight recorded obligations plus the
	// refused record's retained name.
	require.NoError(t, txn.Rollback(ctx))
	require.Len(t, issue25590QueuedNames(e), 9,
		"the terminal transfer carries every recorded obligation and the refused record's retained name to the queue")
	e.unpublishedPending.mu.Lock()
	live, inFlight = e.unpublishedPending.live, e.unpublishedPending.inFlight
	queued = len(e.unpublishedPending.names)
	e.unpublishedPending.mu.Unlock()
	require.Equal(t, 9, queued+int(live)+int(inFlight),
		"the rollback transfer carried the whole obligation population exactly - no record-admission growth, the one retained refused name included")
	require.Zero(t, live,
		"the queued names left the owned ledger exactly once")

	// Sustained rounds against the saturated, deletion-stalled bound —
	// r6 (issue #25590 round-22 review, finding H2): the r21 phase drove
	// raw fresh records through the choked admission and asserted one more
	// fail-open-retained name per refused record (9 -> 10 -> 11 -> 12
	// behind limit 8): growth-accepting assertions proving continuing
	// growth rather than the bound, exactly the review's rejection. The
	// r6 intake gate closes the feed at its source: every NEW obligation
	// — the remote-relayed names included — books through the SAME
	// per-object admission reservation the writers use, so a saturated
	// engine refuses the intake BEFORE anything books, the refusing
	// round's failed run settles through the ordinary rollback terminal,
	// and the aggregate obligation population stays exactly where the
	// stalled backlog pinned it, no matter how long the loop runs with
	// the deletes stalled.
	for round := 0; round < 3; round++ {
		txnLoop := newTransactionWithActivePKTableForTest(t, "pk")
		issue25590InitDumpableFixture(t, txnLoop)
		txnLoop.engine = e
		txnLoop.engine.fs = failFS
		err := txnLoop.AdoptRemoteTransferredObjectNames([]string{
			"issue25590-r22-relayed-name-" + fmt.Sprint(round)})
		require.ErrorContains(t, err, "workspace admission paused",
			"round %d's relayed intake is refused at the saturated bound", round)
		e.unpublishedPending.mu.Lock()
		live, inFlight = e.unpublishedPending.live, e.unpublishedPending.inFlight
		queued = len(e.unpublishedPending.names)
		e.unpublishedPending.mu.Unlock()
		require.Equal(t, 9, queued+int(live)+int(inFlight),
			"round %d's refused intake booked nothing - the bounded population stays exactly at the stalled backlog", round)
		require.NoError(t, txnLoop.Rollback(ctx),
			"round %d's refused run settles its own workspace through the ordinary rollback terminal", round)
		require.Len(t, issue25590QueuedNames(e), 9,
			"round %d's terminal transfer handed exactly nothing new to the queue - the bound gates sustained work", round)
	}

	// The drain's delete round re-opens the bound: with the stalled
	// deletes released, one queued-deletion round retires every handed
	// name exactly, so the system holds no obligation anymore - the
	// bound gates the work in the system, it is not a one-way ratchet.
	unblockDeletes()
	require.NoError(t, e.deleteQueuedUnpublishedPendingOnce(ctx))
	require.Empty(t, issue25590QueuedNames(e),
		"the released drain round retired the whole stalled backlog")

	// The commit's settle shape (releaseUnpublishedPendingPublished, as
	// invoked at commit success): a fresh recorded obligation leaves the
	// owned ledger exactly when its transaction's names publish, and a
	// repeat release is idempotent.
	txnC := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txnC)
	txnC.engine = e
	nameC, err := recordObject(txnC)
	require.NoError(t, err,
		"the retired backlog re-opened admission for the next transaction")
	txnC.Lock()
	txnC.releaseRecordedUnpublishedObjectsLocked()
	txnC.Unlock()
	e.unpublishedPending.mu.Lock()
	liveAfter := e.unpublishedPending.live
	e.unpublishedPending.mu.Unlock()
	require.Zero(t, liveAfter,
		"the published record's name retired from the owned ledger exactly once: %s", nameC)
	txnC.Lock()
	txnC.releaseRecordedUnpublishedObjectsLocked()
	txnC.Unlock()
	e.unpublishedPending.mu.Lock()
	liveAfter = e.unpublishedPending.live
	e.unpublishedPending.mu.Unlock()
	require.Zero(t, liveAfter,
		"the settle is idempotent for repeated terminal shapes")
	require.NoError(t, txnC.Rollback(ctx))
}

// issue25590RecordedNames mirrors the production recorded-file selector
// both GC front doors use (gcObjsByIdxRange and the round-14 owner
// variant), deriving the saturation regression's expected transfer sets
// the same way on every tree the NC differential swaps in. These scenes
// record INSERT entries only, so the selector's tombstone-list side
// effect never fires here.
func issue25590RecordedNames(
	t *testing.T, txn *Transaction, start, end int,
) []string {
	t.Helper()
	var names []string
	for i := start; i <= end; i++ {
		if txn.writes[i].bat == nil ||
			txn.writes[i].bat.RowCount() == 0 {
			continue
		}
		if txn.writes[i].fileName != "" && txn.writes[i].typ != SOFT_DELETE_OBJECT {
			var vec *vector.Vector
			if txn.writes[i].typ == DELETE {
				vec = txn.writes[i].bat.Vecs[0]
			} else {
				vec = txn.writes[i].bat.Vecs[1]
			}
			for j := range vec.Length() {
				ss := objectio.ObjectStats(vec.GetBytesAt(j))
				names = append(names, ss.ObjectName().String())
			}
		}
	}
	return names
}

// TestIssue25590TerminalAndCloseReturnWhileGCAdmissionStalls pins the
// round-13 review's first finding: with every GC worker stalled in a
// context-blind deletion and a real gcFiles submission parked waiting for
// pool capacity, the admission's long-held gcMu chained the terminal
// ownership transfer and Engine.Close onto the storage stall — each
// parked on gcMu behind the gcFiles' blocked Submit, and rollbackOwned
// kept the transaction locks with it. The admission under gcMu is now
// short, never holding the mutex across the blocking submit: with the
// SAME parked shape, the terminal paths and Close finish their
// deterministic part inside their own oracle — with the barrier STILL
// closed — and the resumed storage completes every collection
// afterwards.
func TestIssue25590TerminalAndCloseReturnWhileGCAdmissionStalls(t *testing.T) {
	drives := []struct {
		name     string
		recorded bool
		drive    func(txn *Transaction, e *Engine, ctx context.Context) error
	}{
		{"Rollback", false, func(txn *Transaction, e *Engine, ctx context.Context) error {
			return txn.Rollback(ctx)
		}},
		{"FinalizeCommit", false, func(txn *Transaction, e *Engine, ctx context.Context) error {
			txn.FinalizeCommit(ctx)
			return nil
		}},
		{"FinalizeCommitWithUnknownResult", false, func(txn *Transaction, e *Engine, ctx context.Context) error {
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			txn.FinalizeCommitWithUnknownResult(canceled)
			return nil
		}},
		{"EngineClose", false, func(txn *Transaction, e *Engine, ctx context.Context) error {
			return e.Close()
		}},
		// The round-14 recorded legs: REAL recorded workspace file
		// entries — written, published and recorded by a healthy dump
		// under the production path — sweep through the rollback paths,
		// which must hand them to the queue without any pool
		// submission while the whole GC pool sits in stalled storage
		// and the sixth complementary submission parks for capacity.
		{"TxnRollbackRecorded", true, func(txn *Transaction, e *Engine, ctx context.Context) error {
			return txn.Rollback(ctx)
		}},
		{"StmtRollbackRecorded", true, func(txn *Transaction, e *Engine, ctx context.Context) error {
			return txn.RollbackLastStatement(ctx)
		}},
	}
	for _, drive := range drives {
		t.Run(drive.name, func(t *testing.T) {
			colexec.NewServer("")
			txn := newTransactionWithActivePKTableForTest(t, "pk")

			pool, err := ants.NewPool(GCPoolSize)
			require.NoError(t, err)
			t.Cleanup(func() { pool.Release() })

			failFS := issue25590SwapProcSharedFS(t, txn)
			e := txn.engine
			e.fs = failFS
			require.NoError(t, e.ResetGCWorkerPool(pool))

			// The recorded legs hold REAL recorded workspace file
			// entries — written, published and recorded under the
			// production dump path while storage is still healthy —
			// so the rollback sweeps collect exactly the production
			// shapes (issue #25590 round-14 review). The transfer
			// proof names come from the production selector the
			// rollback itself uses; the untouched proof is the zero
			// completed-delete-call count below, and the deletion
			// proof is the drain's completed call set.
			var recordedNames []string
			if drive.recorded {
				issue25590InitDumpableFixture(t, txn)
				ctx := context.Background()
				txn.StartStatement()
				issue25590AppendInsertEntry(t, txn, 900, 901)
				txn.EndStatement()
				txn.writeWorkspaceThreshold = 0
				txn.StartStatement()
				require.NoError(t, txn.IncrStatementID(ctx, false),
					"the healthy publication records the statement's dump before the stall")
				txn.UpdateSnapshotWriteOffset()
				if drive.name == "StmtRollbackRecorded" {
					// The rolling-back statement's OWN records: stage in
					// the open statement and record through the same
					// dump entry the admission flush uses, so the
					// statement's own rollback range holds real
					// recorded entries.
					issue25590AppendInsertEntry(t, txn, 902, 903)
					txn.Lock()
					dumpErr := txn.dumpInsertBatchLocked(
						ctx, failFS, len(txn.writes)-1, new(int))
					txn.Unlock()
					require.NoError(t, dumpErr,
						"the statement's own dump recorded its file entries")
					last := len(txn.writes) - 1
					recordedNames = issue25590RecordedNames(t, txn, last, last)
				} else {
					recordedNames = issue25590RecordedNames(
						t, txn, 0, len(txn.writes)-1)
				}
				require.NotEmpty(t, recordedNames,
					"the recording publication produced the recorded names")
				require.Empty(t, issue25590QueuedNames(e),
					"the healthy recorded dump enqueues nothing")
			}

			// One real orphaned object the terminal path owns.
			unique := "issue25590-" + uuid.NewString()[:8]
			orphanPath := unique + "-orphan"
			issue25590WriteRawFile(t, failFS.FileService, orphanPath)
			issue25590InjectOwnedOrphan(t, txn, orphanPath)

			release := failFS.armDeleteIgnoreCtxBlock()
			stall := time.AfterFunc(30*time.Second, release)
			t.Cleanup(func() { stall.Stop(); release() })

			// THE SCENE: a real gcFiles call whose first five batch
			// submissions stall the whole five-worker pool in
			// context-blind deletions; its sixth batch's submission then
			// waits for capacity — the state the round-13 review pins:
			// while that real submission is parked, pre-fix gcMu stayed
			// held; the names need no physical presence because the
			// deletions park at the wrapper before any storage read.
			batchNames := make([]string, 0, 6*GCBatchOfFileCount)
			for i := 0; i < 6*GCBatchOfFileCount; i++ {
				batchNames = append(batchNames,
					fmt.Sprintf("%s-batch-%05d", unique, i))
			}
			gcReturned := make(chan error, 1)
			go func() { gcReturned <- gcFiles(txn, cloneGCTxnRollback, batchNames...) }()
			require.Eventually(t, func() bool {
				return failFS.ignoredDeleteCalls() == 5
			}, 10*time.Second, 5*time.Millisecond,
				"all five GC workers stall inside their batch deletions")
			select {
			case gcErr := <-gcReturned:
				t.Fatalf("the gcFiles submission returned while every worker is still stalled: %v", gcErr)
			default:
			}

			// The parked submission changes nothing for the terminal
			// path's deterministic work: the transfer carries the orphan
			// to the engine queue, and Close fits its own pinned budget -
			// neither touches the barrier still holding the workers. The
			// drive runs off the test goroutine so the bounded-return
			// oracle below reads its result here.
			if drive.name == "EngineClose" {
				e.pClient = PushClient{}
				issue25590WithCleanupKnob(
					t, &unpublishedPendingCloseDrainTimeout, 2*time.Second)
			}
			started := time.Now()
			driveErr := make(chan error, 1)
			go func() { driveErr <- drive.drive(txn, e, context.Background()) }()
			select {
			case err = <-driveErr:
			case <-time.After(20 * time.Second):
				t.Fatal("the terminal path or Close must return behind its own machinery with the admission barrier still closed")
			}
			require.Less(t, time.Since(started), 10*time.Second,
				"the drive returned inside its oracle with the barrier still closed")
			require.Equal(t, 0, failFS.deleteCallCount(),
				"while the barrier holds, no call has completed its storage I/O")
			if drive.name == "EngineClose" {
				require.True(t, err == nil || errors.Is(err, context.DeadlineExceeded),
					"Close's only admissible residue is the bounded worker-exit wait: %v", err)
				require.True(t, pool.IsClosed(),
					"the bounded pool release closed the pool immediately")
				// The release refused the parked sixth batch with the
				// lossless handoff: the queue owns exactly those names
				// with the barrier still closed — the detached shutdown
				// drain sits parked on the same barrier, so nothing can
				// retire them before their collection is actually
				// observable (issue #25590 round-13).
				require.Eventually(t, func() bool {
					return len(issue25590QueuedNames(e)) == GCBatchOfFileCount
				}, 5*time.Second, 5*time.Millisecond,
					"the refused batch's names transferred whole to the engine queue")
			} else {
				require.NoError(t, err,
					"the terminal path completes its deterministic transfer without any pool submission")
				// Expected queue contents: the terminal transfers land
				// the transaction-owned orphan first (finalization
				// precedes the recorded sweep), the recorded legs add
				// the statement's recorded files, and a STATEMENT
				// rollback keeps the transaction-owned orphan on the
				// transaction — only its recorded files ride the queue.
				expectedQueued := []string{orphanPath}
				if drive.recorded {
					expectedQueued = append(expectedQueued, recordedNames...)
				}
				if drive.name == "StmtRollbackRecorded" {
					expectedQueued = append([]string{}, recordedNames...)
				}
				require.Equal(t, expectedQueued, issue25590QueuedNames(e),
					"the nonblocking transfer hands every owed name - whole - to the engine queue")
			}

			// Release the barrier only after every assert: the stalled
			// shape resumes — workers finish their context-blind storage
			// work exactly like LocalFS would on its own time, the sixth
			// submission gets a worker, and the queue's own drain
			// collects the handed-off orphan.
			release()
			if drive.name == "EngineClose" {
				// Close's pool release refused the parked sixth batch
				// instead of hitching Close onto the storage stall — and
				// that refusal was a lossless handoff: gcFiles returned
				// nil, the queue owned the unsent batch whole until the
				// barrier opened (asserted above), and the detached
				// shutdown drain collects it once storage resumes (issue
				// #25590 round-13).
				require.Eventually(t, func() bool {
					select {
					case gcErr := <-gcReturned:
						return gcErr == nil
					default:
						return false
					}
				}, 15*time.Second, 5*time.Millisecond,
					"gcFiles returns nil after handing its refused tail to the queue")
				require.Eventually(t, func() bool {
					return len(issue25590QueuedNames(e)) == 0
				}, 15*time.Second, 5*time.Millisecond,
					"the detached shutdown drain collects the handed-off batch after release")
			} else {
				require.Eventually(t, func() bool {
					select {
					case gcErr := <-gcReturned:
						return gcErr == nil
					default:
						return false
					}
				}, 15*time.Second, 5*time.Millisecond,
					"the real gcFiles submission completes once storage resumes")
				require.Eventually(t, func() bool {
					return len(issue25590QueuedNames(e)) == 0
				}, 15*time.Second, 5*time.Millisecond,
					"the queue's own drain collects the handed-off orphan after release")
				// Eventual deletion proof for the recorded legs: the
				// drain's successful storage calls targeted exactly the
				// production-selected names the rollback queued.
				require.Eventually(t, func() bool {
					deleted := failFS.deletedSnapshot()
					for _, recorded := range recordedNames {
						if !slices.Contains(deleted, recorded) {
							return false
						}
					}
					return true
				}, 15*time.Second, 5*time.Millisecond,
					"the drained collection deleted exactly the recorded names the rollback queued")
				if drive.name == "StmtRollbackRecorded" {
					// A statement rollback never touches the
					// transaction-owned orphan: it stays pending with
					// its file kept until the transaction itself ends.
					require.Len(t, txn.pendingObjectCleanup, 1,
						"the transaction-owned orphan survives the statement rollback")
					issue25590RequireKept(t, failFS.FileService, orphanPath,
						"nothing but the transaction's own end owns the injected orphan")
				} else {
					issue25590RequireGone(t, failFS.FileService, orphanPath,
						"the drained collection deleted the handed-off object")
				}
			}
		})
	}
}

// TestIssue25590AdmissionPreservesTotalAcrossOwnershipTransfers pins the
// round-13/18 requirement: the per-object reservation gate composes its
// counters inside ONE queue-mutex section, so the total moves with the
// obligations and never dips while they ride an ownership transfer - and
// every obligation class the round-18 review named is inside that total:
// queued names, transaction-owned orphans, recorded objects, and the
// per-object reservations of writers in flight. The legs below drive
// the ownership transitions on the production per-object seam -
// transaction-to-queue handoff and the in-flight-to-owned conversion -
// and pin the refusal the gate must compute before, during and after
// each transfer: the refused writer never persists another object.
func TestIssue25590AdmissionPreservesTotalAcrossOwnershipTransfers(t *testing.T) {
	colexec.NewServer("")
	issue25590WithCleanupIntKnob(t, &unpublishedPendingAdmissionLimit, 2)
	unique := "issue25590-" + uuid.NewString()[:8]

	// Obligations owned by the TRANSACTION alone already fill the bound:
	// the gate sees the owned count before any transfer runs.
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	names := []string{unique + "-a", unique + "-b"}
	for _, name := range names {
		issue25590InjectOwnedOrphan(t, txn, name)
	}
	err := txn.engine.reserveUnpublishedPendingObject(unique + "-x")
	require.ErrorContains(t, err, "workspace admission paused",
		"transaction-owned orphans already fill the pinned bound before the handoff")

	// The transaction-to-queue transfer carries the total with it: the
	// same two obligations ride from the owned ledger into the engine
	// queue inside one section, so the gate that refused before the
	// handoff keeps refusing on the transferred names alone — and with
	// the drain frozen, nothing retires mid-snapshot.
	failFS := issue25590SwapProcSharedFS(t, txn)
	txn.engine.fs = failFS
	unblock := failFS.armDeleteBlock()
	t.Cleanup(unblock)
	txn.handoffPendingObjectCleanup()
	require.Len(t, issue25590QueuedNames(txn.engine), 2,
		"the whole ownership moved to the engine queue")
	err = txn.engine.reserveUnpublishedPendingObject(unique + "-x")
	require.ErrorContains(t, err, "workspace admission paused",
		"the transferred names alone still fill the bound right after the handoff")
	unblock()

	// An in-flight object reservation while a transfer lands behind it:
	// the held unit plus the landed names still refuse the next object,
	// and after the holder's unit settles, the landed names alone keep
	// the gate shut.
	txn2 := newTransactionWithActivePKTableForTest(t, "pk")
	failFS2 := issue25590SwapProcSharedFS(t, txn2)
	txn2.engine.fs = failFS2
	unblock2 := failFS2.armDeleteBlock()
	t.Cleanup(unblock2)
	held2 := unique + "-held2"
	require.NoError(t, txn2.engine.reserveUnpublishedPendingObject(held2),
		"an empty workspace admits the first object's reservation")
	issue25590InjectOwnedOrphan(t, txn2, unique+"-c")
	issue25590InjectOwnedOrphan(t, txn2, unique+"-d")
	txn2.handoffPendingObjectCleanup()
	require.Len(t, issue25590QueuedNames(txn2.engine), 2,
		"the holder's round landed its own names on the queue")
	err = txn2.engine.reserveUnpublishedPendingObject(unique + "-y")
	require.ErrorContains(t, err, "workspace admission paused",
		"the held in-flight unit plus the landed names keep the gate shut during the holder's round")
	txn2.engine.abandonUnpublishedPendingReservations([]string{held2})
	err = txn2.engine.reserveUnpublishedPendingObject(unique + "-y")
	require.ErrorContains(t, err, "workspace admission paused",
		"the landed names alone keep the gate shut after the holder's unit settles")
	unblock2()

	// The in-flight-to-owned conversion: names persisted while their dump
	// holds their reservations convert to transaction-owned obligations
	// at the failed publication, and the total refuses the next object
	// both during the holder's round and after its unit settles.
	txn3 := newTransactionWithActivePKTableForTest(t, "pk")
	held3 := unique + "-held3"
	require.NoError(t, txn3.engine.reserveUnpublishedPendingObject(held3),
		"an empty workspace admits the holder's object reservation")
	txn3.noteUnpublishedDumpReservation(held3)
	issue25590InjectOwnedOrphan(t, txn3, unique+"-e")
	issue25590InjectOwnedOrphan(t, txn3, unique+"-f")
	err = txn3.engine.reserveUnpublishedPendingObject(unique + "-z")
	require.ErrorContains(t, err, "workspace admission paused",
		"converted owned obligations plus the held unit overfill the bound during the round")
	txn3.convertDumpedReservationsLocked([]string{held3})
	txn3.engine.unpublishedPending.mu.Lock()
	inFlight3 := txn3.engine.unpublishedPending.inFlight
	live3 := txn3.engine.unpublishedPending.live
	txn3.engine.unpublishedPending.mu.Unlock()
	require.Zero(t, inFlight3,
		"the failed publication's conversion settled its reservation exactly")
	require.Equal(t, int64(3), live3,
		"the converted obligation joined the owned names: two injected plus the converted unit")
	err = txn3.engine.reserveUnpublishedPendingObject(unique + "-z")
	require.ErrorContains(t, err, "workspace admission paused",
		"the converted owned obligations alone fill the bound after the unit settles")
}

// TestIssue25590CloseWaitsForParkedGCProducer pins the round-14 first
// finding: gcFiles released gcMu for its submissions without registering
// the caller as an outstanding producer, so a GC caller admitted before
// Close and descheduled until after Close's pool release handed its
// names to a queue nobody drained — the engine's GC scheduler had
// stopped and the closed-refusing schedule left the names uncollected
// even on healthy storage. Producer counts now admit every GC caller
// through its last name's arrival at an owner, and Close's final drain
// consults them: with a real gcFiles caller parked between its
// admission and its first submission, the drain observes the empty
// queue, must NOT conclude behind it, waits for the handoff, and
// collects the parked caller's name before Close returns.
func TestIssue25590CloseWaitsForParkedGCProducer(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	e := txn.engine
	e.pClient = PushClient{}

	pool, err := ants.NewPool(GCPoolSize)
	require.NoError(t, err)
	t.Cleanup(func() { pool.Release() })

	failFS := issue25590SwapProcSharedFS(t, txn)
	e.fs = failFS
	require.NoError(t, e.ResetGCWorkerPool(pool))
	issue25590WithCleanupKnob(
		t, &unpublishedPendingCloseDrainTimeout, 10*time.Second)

	unique := "issue25590-" + uuid.NewString()[:8]
	producerName := unique + "-producer-orphan"
	issue25590WriteRawFile(t, failFS.FileService, producerName)

	// Park a real gcFiles caller between its admission snapshot and its
	// first pool submission — the descheduled-caller window round 14
	// arms exactly.
	hookHit := make(chan struct{})
	releaseProducer := make(chan struct{})
	gcFilesAfterAdmit = func() {
		close(hookHit)
		<-releaseProducer
	}
	defer func() { gcFilesAfterAdmit = nil }()

	gcReturned := make(chan error, 1)
	go func() {
		gcReturned <- gcFiles(txn, cloneGCTxnRollback, producerName)
	}()
	<-hookHit
	e.gcMu.Lock()
	producers := e.gcProducers
	e.gcMu.Unlock()
	require.Equal(t, int64(1), producers,
		"the parked caller is registered as an outstanding producer")
	require.Empty(t, issue25590QueuedNames(e),
		"the parked producer owns the name: no owner sees it yet")

	closeReturned := make(chan error, 1)
	go func() { closeReturned <- e.Close() }()
	require.Eventually(t, pool.IsClosed, 5*time.Second, 5*time.Millisecond,
		"Close released the idle pool and entered the coordinated final drain")
	// The final drain sees the empty queue — and must not conclude
	// while the producer count is above zero.
	select {
	case closeErr := <-closeReturned:
		t.Fatalf("Close concluded on the empty queue while an admitted GC producer was still parked: %v", closeErr)
	case <-time.After(2 * time.Second):
	}
	require.Empty(t, issue25590QueuedNames(e),
		"the parked producer still owes its handoff: the queue stayed empty")

	// Resume the producer: the pool release already refused its
	// submission — the lossless handoff is its landing, and Close's
	// coordinated drain collects it inside the same shutdown deadline.
	close(releaseProducer)
	require.NoError(t, <-gcReturned,
		"gcFiles returns nil through the lossless handoff")
	require.Eventually(t, func() bool {
		return len(issue25590QueuedNames(e)) == 1
	}, 5*time.Second, 5*time.Millisecond,
		"the resumed producer's handoff reached the queue")

	closeErr := <-closeReturned
	require.NoError(t, closeErr,
		"Close concluded only after the parked producer's handoff was collected")
	require.Empty(t, issue25590QueuedNames(e),
		"queue empty at Close return")
	issue25590RequireGone(t, failFS.FileService, producerName,
		"the coordinated final drain deleted the parked producer's name before Close returned")
}

// TestIssue25590AdmissionBoundedWhileCleanupDeletionStalls pins the
// round-14 second finding: dumpBatchLocked's pre-gate cleanup step ran
// one SYNCHRONOUS deletion round for the transaction's
// failed-publication orphans on every dump attempt — including
// statements whose workspaces would dump nothing — under the
// transaction's own locks inside IncrStatementID, so one stalled
// deletion (LocalFS deleteSingle performs synchronous filesystem work
// that ignores its context) parked the entire statement admission and
// behind it the request's cancellation. The pre-gate step now TRANSFERS
// the names to the engine's lossless queue — a mutex-only move — so a
// no-dump statement admission returns while the stalled deletion stays
// parked, and the queue's own scheduled drain collects after the stall
// lifts.
func TestIssue25590AdmissionBoundedWhileCleanupDeletionStalls(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)
	defer func() {
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	}()
	ctx := context.Background()

	failFS := issue25590SwapProcSharedFS(t, txn)
	e := txn.engine
	e.fs = failFS
	pool, err := ants.NewPool(GCPoolSize)
	require.NoError(t, err)
	t.Cleanup(func() { pool.Release() })
	require.NoError(t, e.ResetGCWorkerPool(pool))

	// A real orphan the transaction owns after a failed publication.
	unique := "issue25590-" + uuid.NewString()[:8]
	orphanPath := unique + "-orphan"
	issue25590WriteRawFile(t, failFS.FileService, orphanPath)
	issue25590InjectOwnedOrphan(t, txn, orphanPath)

	release := failFS.armDeleteIgnoreCtxBlock()
	stall := time.AfterFunc(30*time.Second, release)
	t.Cleanup(func() { stall.Stop(); release() })

	// A statement whose workspace will dump nothing — no staged rows,
	// no threshold pressure — still runs the pre-gate cleanup step, the
	// exact shape the review named: with the stalled deletion parked,
	// admission must return on its own, without deleting anything.
	txn.StartStatement()
	started := time.Now()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"the no-dump admission returns although the stalled deletion is parked")
	require.Less(t, int64(time.Since(started)), int64(10*time.Second),
		"statement admission stays bounded while storage stalls its deletion context-blind")
	txn.UpdateSnapshotWriteOffset()
	txn.EndStatement()

	// The transfer — not a delete — carried the name to the queue: the
	// stalled deletion completed nothing, the file is untouched, and the
	// queue's own scheduled drain sits parked on the same barrier.
	require.Empty(t, txn.pendingObjectCleanup,
		"the pre-gate step transferred the transaction-owned orphan")
	require.Equal(t, []string{orphanPath}, issue25590QueuedNames(e),
		"the queue owns the name the stalled deletion would have retired")
	require.Equal(t, 0, failFS.deleteCallCount(),
		"while the barrier holds, no parked call has completed its storage I/O")
	issue25590RequireKept(t, failFS.FileService, orphanPath,
		"the stalled deletion touched nothing")
	require.Eventually(t, func() bool {
		return failFS.ignoredDeleteCalls() == 1
	}, 10*time.Second, 10*time.Millisecond,
		"the queue's own drain is parked in the stalled deletion")

	// Once the stall lifts, the queue's drain collects — the engine's
	// ladder, not any statement, pays for the deletion.
	release()
	require.Eventually(t, func() bool {
		return len(issue25590QueuedNames(e)) == 0
	}, 15*time.Second, 5*time.Millisecond,
		"the queue's drain collects after the stall lifts")
	issue25590RequireGone(t, failFS.FileService, orphanPath,
		"the collection deleted the orphan")
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
	require.NoError(t, txn.Rollback(ctx))
}

// TestIssue25590FailedPublicationAdmitsWhileDeletionStalls pins the
// round-15 review finding 1 on both dump paths: a publication whose
// objects persist physically and still fails runs its ownership epilogue
// INSIDE the statement admission under the transaction lock, so the
// round-14 DeletePersisted there — a synchronous FileService.Delete that
// LocalFS performs context-blind — parked the failed statement's return,
// the statements behind the lock and rollback with it. After round 15
// the epilogue takes the persisted-name ownership snapshot without
// deleting anything, the ORIGINAL publication error returns while the
// orphan's deletion remains stalled, the names transfer to the
// asynchronous cleanup owner, and the collection happens afterwards.
func TestIssue25590FailedPublicationAdmitsWhileDeletionStalls(t *testing.T) {
	t.Run("Insert", func(t *testing.T) {
		colexec.NewServer("")
		txn := newTransactionWithActivePKTableForTest(t, "pk")
		issue25590InitDumpableFixture(t, txn)
		defer func() {
			for i := range txn.writes {
				txn.releaseWorkspaceEntryBatchLocked(i)
			}
		}()
		ctx := context.Background()
		failFS := issue25590SwapProcSharedFS(t, txn)
		e := txn.engine
		e.fs = failFS
		pool, err := ants.NewPool(GCPoolSize)
		require.NoError(t, err)
		t.Cleanup(func() { pool.Release() })
		require.NoError(t, e.ResetGCWorkerPool(pool))

		// One staged statement whose next admission dumps it.
		issue25590AdmitBoundary(t, txn, ctx)
		batchA := issue25590AppendInsertEntry(t, txn, 1, 2)
		txn.EndStatement()

		// The publication persists its object and still fails, while
		// every deletion is stalled context-blind.
		failFS.armPostPersistFailure(func() bool { return true })
		release := failFS.armDeleteIgnoreCtxBlock()
		stall := time.AfterFunc(30*time.Second, release)
		t.Cleanup(func() { stall.Stop(); release() })

		txn.writeWorkspaceThreshold = 0
		txn.StartStatement()
		started := time.Now()
		require.ErrorContains(t, txn.IncrStatementID(ctx, false),
			"post-persist fs write failure")
		require.Less(t, int64(time.Since(started)), int64(10*time.Second),
			"round-15: the original publication error returns while its orphan's deletion stays stalled - the statement path performs no deletion")
		txn.UpdateSnapshotWriteOffset()

		// Retained raw source, names handed to the asynchronous owner,
		// and not one storage deletion attempted by the statement.
		require.Len(t, txn.writes, 1,
			"the failed publication retained the staged source raw")
		require.Same(t, batchA, txn.writes[0].bat)
		require.Empty(t, txn.writes[0].fileName)
		require.Equal(t, 0, failFS.ignoredDeleteCalls(),
			"no deletion was even attempted inside the failed statement's admission")
		require.Equal(t, 0, failFS.deleteCallCount(),
			"round-15: the failed writer deletes nothing on the spot")
		orphanName := failFS.failedAtName()
		require.Len(t, txn.pendingObjectCleanup, 1,
			"the failed writer transferred its persisted names to the asynchronous cleanup owner")
		require.Equal(t, orphanName, txn.pendingObjectCleanup[0].name)
		require.Equal(t, []string{orphanName}, failFS.persistedSnapshot())
		txn.EndStatement()

		// Eventual collection: the terminal handoff moves the names to
		// the engine queue — rollback still returns behind its own
		// machinery — and the queue's drain pays the deletion once the
		// stall lifts.
		release()
		require.NoError(t, txn.Rollback(ctx),
			"rollback returns boundedly: it hands the names over, storage is not its work")
		require.Empty(t, txn.pendingObjectCleanup,
			"the terminal handoff transferred the orphan")
		require.Eventually(t, func() bool {
			return len(issue25590QueuedNames(e)) == 0
		}, 15*time.Second, 5*time.Millisecond,
			"the queue's drain collected the handed-off orphan after the stall lifted")
		issue25590RequireGone(t, failFS.FileService, orphanName,
			"the collection deleted the failed publication's orphan")
		require.Equal(t, 1, failFS.deleteCallCount(),
			"exactly the drain's one call performed the collection")
		e.unpublishedPending.mu.Lock()
		live := e.unpublishedPending.live
		e.unpublishedPending.mu.Unlock()
		require.Zero(t, live,
			"the handed-off name left the live-orphan accounting")
	})

	t.Run("Delete", func(t *testing.T) {
		colexec.NewServer("")
		txn := newTransactionWithActivePKTableForTest(t, "pk")
		issue25590InitDumpableFixture(t, txn)
		defer func() {
			for i := range txn.writes {
				txn.releaseWorkspaceEntryBatchLocked(i)
			}
		}()
		ctx := context.Background()
		failFS := issue25590SwapProcSharedFS(t, txn)
		e := txn.engine
		e.fs = failFS
		pool, err := ants.NewPool(GCPoolSize)
		require.NoError(t, err)
		t.Cleanup(func() { pool.Release() })
		require.NoError(t, e.ResetGCWorkerPool(pool))

		// A staged tombstone whose dump keeps the admission path's own
		// locked context: dumpDeleteBatchLocked runs under the
		// transaction lock inside IncrStatementID.
		delBat := issue25590AppendDeleteEntry(t, txn, 900)
		require.Equal(t, 1, txn.approximateInMemDeleteCnt)

		failFS.armPostPersistFailure(func() bool { return true })
		release := failFS.armDeleteIgnoreCtxBlock()
		stall := time.AfterFunc(30*time.Second, release)
		t.Cleanup(func() { stall.Stop(); release() })

		txn.Lock()
		started := time.Now()
		require.ErrorContains(t, txn.dumpDeleteBatchLocked(ctx, failFS, 0),
			"post-persist fs write failure")
		require.Less(t, int64(time.Since(started)), int64(10*time.Second),
			"round-15: the delete dump's original error returns while its orphan's deletion stays stalled - the locked path performs no deletion")
		txn.Unlock()

		require.Same(t, delBat, txn.writes[0].bat,
			"the failed tombstone publication retained the staged source")
		require.Empty(t, txn.writes[0].fileName)
		require.Equal(t, 0, failFS.ignoredDeleteCalls(),
			"no deletion was even attempted on the locked dump path")
		orphanName := failFS.failedAtName()
		require.Len(t, txn.pendingObjectCleanup, 1,
			"the failed writer transferred its persisted names to the asynchronous cleanup owner")
		require.Equal(t, orphanName, txn.pendingObjectCleanup[0].name)

		release()
		require.NoError(t, txn.Rollback(ctx),
			"rollback returns boundedly: it hands the names over, storage is not its work")
		require.Empty(t, txn.pendingObjectCleanup,
			"the terminal handoff transferred the orphan")
		require.Eventually(t, func() bool {
			return len(issue25590QueuedNames(e)) == 0
		}, 15*time.Second, 5*time.Millisecond,
			"the queue's drain collected the handed-off orphan after the stall lifted")
		issue25590RequireGone(t, failFS.FileService, orphanName,
			"the collection deleted the failed delete dump's orphan")
		require.Equal(t, 1, failFS.deleteCallCount(),
			"exactly the drain's one call performed the collection")
	})
}

// TestIssue25590CloseWaitsForParkedOrphanHandoff pins the round-15 review
// finding 2: a transaction-owned orphan handoff descheduled mid-window —
// its names already cleared off the transaction's list, not yet
// enqueued on the queue — holds names that NO owner can enumerate while
// the live accounting still counts them. Round 14 left that window
// unregistered, so Engine.Close's coordinated final drain could observe
// producers==0 alongside an empty queue and conclude, stranding the
// resumed transfer's enqueue on a dead engine even with healthy storage
// and no deadline expiry. After round 15 the transfer registers with
// the same producer count the drain consults BEFORE it reads the
// queue, and after round 16 the clear-to-enqueue window itself runs
// WHOLE under the queue mutex - the same mutex Close takes for its
// first queue-side step and the drain takes for its obligation
// snapshot - so a Close started behind a parked transfer can neither
// release the pool nor observe the queue until the window finishes.
// Once the transfer resumes within the one shutdown budget, Close's
// own drain collects and physically deletes the orphan before Close
// returns; the late-registration ordering INSIDE a running drain is
// pinned by TestIssue25590LateOrphanRegistrationCollectedBeforeShutdown.
func TestIssue25590CloseWaitsForParkedOrphanHandoff(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)
	defer func() {
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	}()
	ctx := context.Background()
	e := txn.engine
	e.pClient = PushClient{}

	failFS := issue25590SwapProcSharedFS(t, txn)
	e.fs = failFS
	pool, err := ants.NewPool(GCPoolSize)
	require.NoError(t, err)
	t.Cleanup(func() { pool.Release() })
	require.NoError(t, e.ResetGCWorkerPool(pool))
	issue25590WithCleanupKnob(
		t, &unpublishedPendingCloseDrainTimeout, 10*time.Second)

	// A REAL failed-publication orphan through round-15's own epilogue:
	// the write persists physically, the publication fails, and the
	// writer hands the names over without deleting them on the spot.
	issue25590AppendInsertEntry(t, txn, 1, 2)
	failFS.armPostPersistFailure(func() bool { return true })
	txn.Lock()
	require.ErrorContains(t,
		txn.dumpInsertBatchLocked(ctx, failFS, 0, new(int)),
		"post-persist fs write failure")
	txn.Unlock()
	failFS.stopPostPersistFailure()
	orphanName := failFS.failedAtName()
	require.Len(t, txn.pendingObjectCleanup, 1,
		"the failed publication left its orphan with the transaction, deleted nowhere")
	require.Equal(t, orphanName, txn.pendingObjectCleanup[0].name)
	require.Equal(t, []string{orphanName}, failFS.persistedSnapshot(),
		"the orphan exists physically on storage")

	// Park the terminal handoff exactly inside the round-15 window.
	hookHit := make(chan struct{})
	resumeHandoff := make(chan struct{})
	orphanHandoffAfterClear = func() {
		close(hookHit)
		<-resumeHandoff
	}
	defer func() { orphanHandoffAfterClear = nil }()

	// Before the transfer starts: the queue is empty and the live
	// accounting owes the orphan — round 16 runs the transfer's whole
	// clear-to-enqueue window under the queue mutex, so queue-side reads
	// below happen before the window opens (a parked window HOLDS that
	// mutex; reading through it would block).
	require.Empty(t, issue25590QueuedNames(e),
		"the queue is empty before the transfer's window opens")
	e.unpublishedPending.mu.Lock()
	liveBefore := e.unpublishedPending.live
	e.unpublishedPending.mu.Unlock()
	require.Equal(t, int64(1), liveBefore,
		"the failed publication's orphan is live-accounted before the transfer")

	rollbackDone := make(chan error, 1)
	go func() { rollbackDone <- txn.Rollback(ctx) }()
	<-hookHit

	// Mid-window: the names left the transaction's list and reached no
	// owner. Round 16 makes the parked transfer HOLD the queue mutex
	// across its window — pinned here with a TryLock instead of a queue
	// read that would block behind it — while the producer count (read
	// under gcMu, which the window never holds) registers the transfer
	// for the shutdown protocol.
	e.gcMu.Lock()
	producers := e.gcProducers
	e.gcMu.Unlock()
	require.Equal(t, int64(1), producers,
		"the parked orphan transfer is registered with the producer count the drain consults")
	windowHoldsQueue := e.unpublishedPending.mu.TryLock()
	if windowHoldsQueue {
		e.unpublishedPending.mu.Unlock()
	}
	require.False(t, windowHoldsQueue,
		"the parked transfer's window holds the queue mutex - the drain's obligation snapshots wait behind it")

	// Round 16 also closes the r15 reachability from Close's side: the
	// transfer's clear-to-enqueue section runs WHOLE under the queue
	// mutex, so a Close started behind the parked window cannot take
	// even its first queue-side step - cancelUnpublishedPendingDrains
	// acquires that mutex before ANY pClient, cache, snapshot or pool
	// teardown - let alone run the pre-release drain join or observe an
	// empty queue. The pool therefore stays unreleased, and Close
	// un-returned, for as long as the transfer is parked mid-window.
	closeReturned := make(chan error, 1)
	go func() { closeReturned <- e.Close() }()
	require.Never(t, pool.IsClosed, 2*time.Second, 5*time.Millisecond,
		"Close cannot release its pool behind a parked mid-window transfer: it waits behind the window's queue mutex before its first queue-side step")
	select {
	case closeErr := <-closeReturned:
		t.Fatalf("round-15 finding 2: Close concluded while a transaction-owned orphan transfer was still parked mid-window: %v", closeErr)
	case <-time.After(2 * time.Second):
	}

	// Resume the handoff INSIDE the shutdown budget: the enqueue makes
	// the names visible, the scheduling on the closed engine refuses and
	// leaves them queued, the registration drops, and Close's own final
	// drain collects — the physical deletion happens before Close
	// returns, on storage that was healthy the whole time.
	close(resumeHandoff)
	require.NoError(t, <-rollbackDone,
		"the resumed rollback completed its handoff and returned")
	closeErr := <-closeReturned
	require.NoError(t, closeErr,
		"Close concluded only after the resumed orphan transfer was collected")
	require.Empty(t, issue25590QueuedNames(e),
		"queue empty at Close return")
	issue25590RequireGone(t, failFS.FileService, orphanName,
		"the coordinated final drain physically deleted the handed-off orphan before Close returned")
	e.gcMu.Lock()
	producers = e.gcProducers
	e.gcMu.Unlock()
	require.Zero(t, producers,
		"no producer registration survived the transfer")
	e.unpublishedPending.mu.Lock()
	live := e.unpublishedPending.live
	e.unpublishedPending.mu.Unlock()
	require.Zero(t, live,
		"the handed-off name left the live-orphan accounting")
}

// TestIssue25590LateOrphanRegistrationCollectedBeforeShutdown pins the
// round-16 review finding: Engine.Close's final drain read the producer
// count and the queue separately, so a transaction-owned orphan handoff
// that registered BETWEEN the drain's captured count and its completion
// decision could be concluded over by the stale producers==0 - Close
// finished behind a registered, mid-window transfer and the resumed
// enqueue stranded on the dead queue. Round 16 reads the obligations
// (producers, queue, arrival horizon) as one consistent snapshot under
// the queue mutex — the same mutex the transfer window now holds — so
// every completion decision is atomically up to date. The regression
// parks Close's drain inside its deletion attempt AFTER it observed
// producers==0, starts the orphan handoff mid-drain (the registration
// lands behind the stalled drain's captured count), and proves on the
// fixed tree that the collection of both names completes before
// shutdown does - while the round-15 base concludes behind it.
func TestIssue25590LateOrphanRegistrationCollectedBeforeShutdown(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)
	defer func() {
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	}()
	ctx := context.Background()
	e := txn.engine
	e.pClient = PushClient{}

	failFS := issue25590SwapProcSharedFS(t, txn)
	e.fs = failFS
	issue25590WithCleanupKnob(
		t, &unpublishedPendingCloseDrainTimeout, 10*time.Second)

	// One queued object gives Close's final drain real work FIRST, so
	// its zero-producers observation happens with the drain mid-flight;
	// its deletion parks on the barrier the test controls and fails
	// once the barrier lifts - the parked, failed attempt is the
	// review's "paused after the zero-producer observation". No GC pool
	// is installed and the scheduler tick never starts, so Close's
	// final drain is the ONLY deletion lane and the parked attempt
	// deterministically belongs to it.
	unique := "issue25590-" + uuid.NewString()[:8]
	queuedName := unique + "-queued"
	issue25590WriteRawFile(t, failFS.FileService, queuedName)
	e.enqueueUnpublishedPending([]string{queuedName})
	failFS.armDeleteFailures(false, 1)
	releaseDeletes := failFS.armDeleteIgnoreCtxBlock()
	stall := time.AfterFunc(30*time.Second, releaseDeletes)
	t.Cleanup(func() { stall.Stop(); releaseDeletes() })

	// A real failed-publication orphan the transaction still owns: the
	// write persists physically, the publication fails, and the round-15
	// epilogue hands the names over without deleting anything.
	issue25590AppendInsertEntry(t, txn, 1, 2)
	failFS.armPostPersistFailure(func() bool { return true })
	txn.Lock()
	require.ErrorContains(t,
		txn.dumpInsertBatchLocked(ctx, failFS, 0, new(int)),
		"post-persist fs write failure")
	txn.Unlock()
	failFS.stopPostPersistFailure()
	orphanName := failFS.failedAtName()
	require.Len(t, txn.pendingObjectCleanup, 1,
		"the failed publication's orphan is the transfer the round-16 finding strands")
	require.Equal(t, orphanName, txn.pendingObjectCleanup[0].name)

	// Park the terminal handoff exactly inside its mid-window seam: the
	// registration happens, the names leave the transaction's list, and
	// the enqueue waits for the test.
	hookHit := make(chan struct{})
	resumeHandoff := make(chan struct{})
	orphanHandoffAfterClear = func() {
		close(hookHit)
		<-resumeHandoff
	}
	defer func() { orphanHandoffAfterClear = nil }()

	// Start Close: its final drain observes producers==0 (no handoff has
	// registered yet) and enters its deletion attempt of the queued
	// name - parked on the context-blind barrier.
	closeReturned := make(chan error, 1)
	go func() { closeReturned <- e.Close() }()
	require.Eventually(t, func() bool {
		return failFS.ignoredDeleteCalls() >= 1
	}, 5*time.Second, 5*time.Millisecond,
		"Close's final drain is parked inside its deletion attempt behind its zero-producers observation")

	// THE LATE REGISTRATION: while the drain is parked mid-attempt, the
	// rollback enters its handoff and registers - behind the count the
	// parked drain already captured.
	rollbackDone := make(chan error, 1)
	go func() { rollbackDone <- txn.Rollback(ctx) }()
	<-hookHit
	e.gcMu.Lock()
	producers := e.gcProducers
	e.gcMu.Unlock()
	require.Equal(t, int64(1), producers,
		"the late handoff registered while the parked drain held its already-captured count")

	// The barrier lifts and the queued name's deletion fails once: on
	// the round-15 loop the no-progress rule decided on the STALE
	// captured count and concluded here - stranding the registered
	// handoff's enqueue; the round-16 snapshot decides on a consistent
	// fresh triple and stays open behind the registration and the
	// mid-attempt arrival.
	releaseDeletes()
	select {
	case closeErr := <-closeReturned:
		close(resumeHandoff)
		select {
		case <-rollbackDone:
		case <-time.After(5 * time.Second):
			t.Log("the stranded rollback did not finish before the oracle")
		}
		t.Fatalf("round-16 review finding: Close concluded behind the freshly-registered mid-window handoff over its stale producers==0: %v", closeErr)
	case <-time.After(2 * time.Second):
	}

	// Resume the handoff INSIDE the shutdown budget: the enqueue lands
	// on the queue the still-running drain watches, the registration
	// drops, and the drain's own retried attempt retires both names —
	// the round-15 loop's one delete failure is already spent — before
	// Close returns.
	close(resumeHandoff)
	require.NoError(t, <-rollbackDone,
		"the resumed rollback completed its handoff and returned")
	closeErr := <-closeReturned
	require.NoError(t, closeErr,
		"Close returned only after collecting the late-registered handoff's orphan")
	issue25590RequireGone(t, failFS.FileService, queuedName,
		"the queued name collected by the final drain's retried attempt")
	issue25590RequireGone(t, failFS.FileService, orphanName,
		"the late-registered handoff's orphan physically collected before shutdown completed")
	e.unpublishedPending.mu.Lock()
	queued := len(e.unpublishedPending.names)
	live := e.unpublishedPending.live
	e.unpublishedPending.mu.Unlock()
	require.Zero(t, queued, "queue empty at Close return")
	require.Zero(t, live, "the handed-off name left the live accounting")
	e.gcMu.Lock()
	producers = e.gcProducers
	e.gcMu.Unlock()
	require.Zero(t, producers, "no producer registration survived")
}

// TestIssue25590QueueMembershipDeduplicatesAtScale pins the round-11
// requirement that replaced the linear membership scans: the engine
// queue answers duplicates from the membership index, keeps the index in
// lockstep with retirements, and accepts retired names again as new
// members. The scale makes the semantic difference of the index
// observable while the drain is frozen for exact snapshots.
func TestIssue25590QueueMembershipDeduplicatesAtScale(t *testing.T) {
	const n = 30_000
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	e := txn.engine

	pool, err := ants.NewPool(GCPoolSize)
	require.NoError(t, err)
	t.Cleanup(func() { pool.Release() })

	failFS := issue25590SwapProcSharedFS(t, txn)
	e.fs = failFS
	require.NoError(t, e.ResetGCWorkerPool(pool))

	unique := "issue25590-" + uuid.NewString()[:8]
	names := make([]string, 0, n)
	for i := 0; i < n; i++ {
		names = append(names, fmt.Sprintf("%s-member-%08d", unique, i))
	}

	// Freeze the drain so the exact membership snapshots below observe a
	// stable queue, the same discipline the round-10 storm pins.
	freeze := failFS.armDeleteBlock()
	t.Cleanup(freeze)
	e.enqueueUnpublishedPending(names)
	require.Len(t, issue25590QueuedNames(e), n,
		"the first delivery retains every distinct member")
	require.Eventually(t, func() bool {
		return failFS.blockedDeleteCalls() >= 1
	}, 10*time.Second, 10*time.Millisecond,
		"the drain reached the frozen storage")

	// One full re-delivery of the whole backlog - the duplicate shape a
	// linear rescan would have made quadratic on finalizers.
	started := time.Now()
	e.enqueueUnpublishedPending(names)
	wall := time.Since(started)
	require.Len(t, issue25590QueuedNames(e), n,
		"a duplicate delivery never double-retains")
	require.Less(t, int64(wall), int64(20*time.Second),
		"membership answers from the index, not a retained-backlog rescan")

	// Retire keeps the index in lockstep: release the freeze, a healthy
	// drain retires every name (missing files are tolerated deletions),
	// and the same names are then accepted again as new members.
	freeze()
	require.Eventually(t, func() bool {
		return len(issue25590QueuedNames(e)) == 0
	}, 30*time.Second, 20*time.Millisecond,
		"the healthy drain retires the whole backlog in lockstep with the index")
	freeze2 := failFS.armDeleteBlock()
	t.Cleanup(freeze2)
	e.enqueueUnpublishedPending(names)
	require.Len(t, issue25590QueuedNames(e), n,
		"the index accepts retired names again after lockstep maintenance")
	freeze2()
	require.Eventually(t, func() bool {
		return len(issue25590QueuedNames(e)) == 0
	}, 30*time.Second, 20*time.Millisecond,
		"the reopened drain retires the accepted members")
}

// === round-19 regressions: admission-bound compaction and direct writers ===

// TestIssue25590CompactionRefusalRetainsDirtyObjectSources pins the
// round-19 review finding 1 at the pass level: the dirty object's
// replacement must complete EVERY fallible step — its persistence, its
// record's admission — before any original may retire, and a refusal at
// any of those points must fail the WHOLE admission with the workspace
// exactly as found. The pre-fix compaction panicked a record refusal away
// inside its ants worker (ants recovered it, waiter.Done still ran), the
// swallow-tail then garbage-collected the original objects, wiped the
// pending marks and returned nil — the rejected replacement's surviving
// rows vanished from the workspace and the physically persisted
// replacement leaked with no cleanup owner. Here the pinned bound refuses
// the replacement's spilling object at its hook; the pass propagates the
// admission error, the marks, the original metadata batch and the
// original physical objects survive untouched, and the retried pass under
// the restored bound completes.
func TestIssue25590CompactionRefusalRetainsDirtyObjectSources(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)

	ctx := context.Background()
	e := txn.engine
	e.pClient = PushClient{}
	failFS := issue25590SwapProcSharedFS(t, txn)
	e.fs = failFS

	// A table with REAL data columns for the dirty rewrite: the rewrite's
	// surviving rows stream through tbl.seqnums, so the reduction fixture
	// reserved for the compacted table carries [a(pk), v, rowid] exactly
	// like a physical table's def.
	dbv := txn.databaseOps.existAndActive(genDatabaseKey(1, "db"))
	require.NotNil(t, dbv)
	const ctblID = uint64(44)
	txn.tableOps.addCreateTable(
		genTableKey(1, "ctbl", 7, "db"),
		0,
		&txnTable{
			accountId: 1,
			tableId:   ctblID,
			tableName: "ctbl",
			db:        dbv,
			tableDef: &pbplan.TableDef{
				Cols: []*pbplan.ColDef{
					{Name: "a", Typ: pbplan.Type{Id: int32(types.T_int64)}, Primary: true},
					{Name: "v", Typ: pbplan.Type{Id: int32(types.T_int64)}},
					{Name: catalog.Row_ID,
						Typ: pbplan.Type{Id: int32(types.T_Rowid)}},
				},
				Pkey: &pbplan.PrimaryKeyDef{PkeyColName: "a"},
			},
		},
	)
	defer func() {
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	}()

	admit := func() { issue25590AdmitBoundary(t, txn, ctx) }

	ledger := func() (queued int, live int64, inFlight int64) {
		e.unpublishedPending.mu.Lock()
		defer e.unpublishedPending.mu.Unlock()
		return len(e.unpublishedPending.names), e.unpublishedPending.live,
			e.unpublishedPending.inFlight
	}

	// Statement 1: the production shape whose next admission compacts -
	// one forced dump publishing ONE metadata batch whose objects are
	// all clean except the tail one that gets marked below.
	admit()
	pks := make([]int64, 100000)
	vals := make([]int64, 100000)
	for i := range pks {
		pks[i] = int64(i + 1)
		vals[i] = int64(i + 1)
	}
	rowidVec, err := txn.batchAllocNewRowIds(len(pks))
	require.NoError(t, err)
	defer rowidVec.Free(txn.proc.Mp())
	rowids := vector.MustFixedColNoTypeCheck[types.Rowid](rowidVec)
	batchA := batch.NewWithSize(3)
	batchA.SetAttributes([]string{objectio.PhysicalAddr_Attr, "a", "v"})
	rowidCol := vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixedList(rowidCol, rowids, nil, txn.proc.Mp()))
	aCol := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(aCol, pks, nil, txn.proc.Mp()))
	vCol := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(vCol, vals, nil, txn.proc.Mp()))
	batchA.Vecs[0] = rowidCol
	batchA.Vecs[1] = aCol
	batchA.Vecs[2] = vCol
	batchA.SetRowCount(len(pks))
	txn.appendWorkspaceEntryLocked(Entry{
		typ:          INSERT,
		accountId:    1,
		databaseId:   issue25590DBID,
		tableId:      ctblID,
		databaseName: "db",
		tableName:    "ctbl",
		bat:          batchA,
	})
	smallObjsDisarm := issue25590ArmKeyedFault(t, objectio.FJ_CNFlushSmallObjs, 20, "")
	txn.Lock()
	var pkCount int
	err = txn.dumpInsertBatchLocked(ctx, failFS, txn.GetSnapshotWriteOffset(), &pkCount)
	txn.Unlock()
	smallObjsDisarm()
	require.NoError(t, err)
	require.Equal(t, 100000, pkCount)

	metaIdx, statsCol, statsArea := issue25590MetadataEntry(t, txn)
	objCount := len(statsCol)
	require.GreaterOrEqual(t, objCount, 2,
		"the forced dump published a multi-object metadata batch")
	objStats := make([]objectio.ObjectStats, objCount)
	for i := 0; i < objCount; i++ {
		objStats[i] = objectio.ObjectStats(statsCol[i].GetByteSlice(statsArea))
		require.Greater(t, int(objStats[i].BlkCnt()), 0)
	}
	lastStats := objStats[objCount-1]
	metaBat := txn.writes[metaIdx].bat
	metaRowCount := metaBat.RowCount()
	infoCol, infoArea := vector.MustVarlenaRawData(metaBat.Vecs[0])
	tailBlockOffset := 0
	for i := 0; i < objCount-1; i++ {
		tailBlockOffset += int(objStats[i].BlkCnt())
	}
	tailFirstBlock := objectio.DecodeBlockInfo(
		infoCol[tailBlockOffset].GetByteSlice(infoArea)).BlockID
	tailRows := int(lastStats.BlkCnt()) * objectio.BlockMaxRows
	tailRowStart := 100000 - tailRows
	require.LessOrEqual(t, tailRowStart+2, 100000)
	txn.EndStatement()

	// Statement 2: two pending deletes on the tail object's first block -
	// the only record that those rows are gone.
	admit()
	delBat := batch.NewWithSize(2)
	delBat.SetAttributes([]string{objectio.PhysicalAddr_Attr, "a"})
	rowidVec2 := vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixedList(
		rowidVec2, []types.Rowid{
			types.NewRowid(&tailFirstBlock, 0),
			types.NewRowid(&tailFirstBlock, 1),
		}, nil, txn.proc.Mp()))
	pkVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(
		pkVec, []int64{pks[tailRowStart], pks[tailRowStart+1]}, nil, txn.proc.Mp()))
	delBat.Vecs[0] = rowidVec2
	delBat.Vecs[1] = pkVec
	delBat.SetRowCount(2)
	out := txn.deleteBatch(delBat, issue25590DBID, ctblID)
	t.Cleanup(func() { out.Clean(txn.proc.Mp()) })
	require.Equal(t, 1, txn.deletedBlocks.size())
	txn.EndStatement()

	dumpedNames := failFS.persistedSnapshot()
	require.Len(t, dumpedNames, objCount,
		"the forced dump persisted exactly its batch's objects")

	require.Len(t, txn.writes, 1,
		"the arrange phase holds exactly the dumped metadata entry")
	_, live, inFlight := ledger()
	require.Equal(t, int64(objCount), live,
		"the dump's recorded objects are the only owned obligations")
	require.Zero(t, inFlight)

	// The pinned bound: every per-object reservation a replacement's
	// spilling writer would book is refused before anything persists.
	knobSaved := unpublishedPendingAdmissionLimit
	unpublishedPendingAdmissionLimit = 0
	t.Cleanup(func() { unpublishedPendingAdmissionLimit = knobSaved })

	// The admission's compaction pass with the bound saturated: the
	// dirty object's rewrite is refused BEFORE its replacement persists
	// and the pass fails the admission whole - the pre-fix pass panicked
	// the refusal away inside its worker, the tail GC'd the originals,
	// wiped the marks and returned nil here.
	txn.Lock()
	err = txn.compactDeletionOnObjsLocked(ctx)
	txn.Unlock()
	require.ErrorContains(t, err, "workspace admission paused",
		"a saturated bound refuses the dirty replacement's persistence and the compaction fails the admission whole")

	// The pending deletions stay put - the retried admission reapplies
	// them; wiping them resurrects the deleted rows.
	require.Equal(t, 1, txn.deletedBlocks.size(),
		"the refused compaction keeps the dirty object's pending deletions")
	require.Len(t, txn.deletedBlocks.offsets[tailFirstBlock], 2,
		"both marked offsets survive the refused pass")

	// The original metadata batch is byte-identical - its release is
	// gated behind the replacements' full completion.
	require.NotNil(t, txn.writes[metaIdx].bat,
		"the refused pass may not consume the original metadata batch")
	require.Equal(t, metaRowCount, txn.writes[metaIdx].bat.RowCount())
	statsColAfter, statsAreaAfter := vector.MustVarlenaRawData(txn.writes[metaIdx].bat.Vecs[1])
	require.Len(t, statsColAfter, objCount)
	for i := 0; i < objCount; i++ {
		after := objectio.ObjectStats(statsColAfter[i].GetByteSlice(statsAreaAfter))
		require.Equal(t, objStats[i].ObjectName().String(), after.ObjectName().String(),
			"the refused pass left object %d of the original batch untouched", i)
	}

	// The refused pass appended NOTHING: no staged re-publication, no
	// replacement record, no orphan obligation.
	require.Len(t, txn.writes, 1,
		"the refused pass published and recorded nothing")
	require.Empty(t, txn.pendingObjectCleanup,
		"the refused replacement persisted nothing, so nothing needed an orphan retake")
	require.Len(t, failFS.persistedSnapshot(), objCount,
		"the refused spilling writer persisted no object behind the refusals")

	// The original physical objects survive untouched.
	for i := range objStats {
		issue25590RequireKept(t, failFS.FileService,
			objStats[i].ObjectName().String(), "an original object")
	}

	// Ledger hygiene: no reservation outlives the refused pass.
	_, live, inFlight = ledger()
	require.Zero(t, inFlight,
		"the refused rewrite's hook released its exact in-flight unit")
	require.Equal(t, int64(objCount), live)
	require.NoError(t, txn.checkWorkspaceAccountingLocked())

	// The statement continuation - rollback and the retried admission
	// through the frontend's statement ceremony - is covered by the
	// engine-level TestIssue25590CompactionAdmissionRefusalPreservesRowsAndCommits;
	// this fixture's refusal leg keeps the marks and rings the retried
	// pass directly: under the restored bound the compaction re-runs and
	// completes.
	unpublishedPendingAdmissionLimit = knobSaved

	txn.Lock()
	err = txn.compactDeletionOnObjsLocked(ctx)
	txn.Unlock()
	require.NoError(t, err,
		"the retried pass under the restored bound completes")

	// The marks cleared - the surviving rows are now embodied in the
	// rewritten storage object; the re-publication and the replacement
	// both recorded into the workspace.
	require.Zero(t, txn.deletedBlocks.size(),
		"the completed compaction embodies the deletions and clears their marks")
	require.Nil(t, txn.writes[metaIdx].bat,
		"the ORIGINAL metadata batch retired only after the pass fully completed")
	idxes := issue25590AllMetadataEntries(t, txn)
	require.Len(t, idxes, objCount,
		"the completed pass recorded the re-publications of the clean objects plus the tail's replacement")

	// The pass appends the replacement record BEFORE the re-publications:
	// the first new entry is the dirty object's replacement - one fresh
	// object - and the remaining entries carry the clean objects' own
	// names, one publication per object.
	rewriteNames := issue25590StatsNamesOfMeta(t, txn, idxes[0])
	require.Len(t, rewriteNames, 1,
		"the dirty object's replacement recorded one fresh object")
	require.NotEqual(t, lastStats.ObjectName().String(), rewriteNames[0],
		"the dirty object's replacement is a NEW storage object, not its original")
	repubNames := make([]string, 0, objCount-1)
	for i := 1; i < objCount; i++ {
		repubNames = append(repubNames, issue25590StatsNamesOfMeta(t, txn, idxes[i])...)
	}
	require.Len(t, repubNames, objCount-1)
	require.ElementsMatch(t, repubNames,
		func() []string {
			names := make([]string, 0, objCount-1)
			for i := 0; i < objCount-1; i++ {
				names = append(names, objStats[i].ObjectName().String())
			}
			return names
		}(),
		"the re-publications carry exactly the clean objects' names")

	// The replacement object physically exists and carries real rows:
	// the recorded entry's name names a readable file.
	issue25590RequireKept(t, failFS.FileService, rewriteNames[0],
		"the completed pass's replacement object")

	// Ledger shape at completion: the recorded originals plus the
	// recorded replacement - one owned member per distinct object, and
	// no in-flight unit anywhere.
	_, live, inFlight = ledger()
	require.Equal(t, int64(objCount+1), live,
		"the replacements' reservations converted into exactly one owned member per object")
	require.Zero(t, inFlight)
	require.Contains(t, issue25590RecordedSnapshot(txn), rewriteNames[0],
		"the replacement's record carries its name's obligation")
	require.Empty(t, txn.pendingObjectCleanup)
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
}

// TestIssue25590DirectWritesReserveAndRetainOnUnwind pins the round-19
// review finding 2: a direct-S3 writer bound to its workspace reserves
// one in-flight unit per object BEFORE the object persists, a saturated
// bound refuses the spill with nothing half-persisted behind repeated
// refusals, a recorded object's reservation converts at its record, and
// a writer unwound between persistence and recording hands ONLY its
// unrecorded names to the transaction's asynchronous cleanup ownership -
// recorded names stay with their record's lifecycle - from where the
// engine's lossless queue deletes the physical object while the recorded
// one is kept untouched.
func TestIssue25590DirectWritesReserveAndRetainOnUnwind(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)
	defer func() {
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	}()

	ctx := context.Background()
	e := txn.engine
	e.pClient = PushClient{}
	failFS := issue25590SwapProcSharedFS(t, txn)
	e.fs = failFS
	pool, err := ants.NewPool(2)
	require.NoError(t, err)
	t.Cleanup(func() { pool.Release() })
	require.NoError(t, e.ResetGCWorkerPool(pool))

	rel, err := txn.getTable(ctx, 1, "db", "tbl")
	require.NoError(t, err)
	tbl, ok := rel.(*txnTable)
	require.True(t, ok)

	writeRows := func(pks ...int64) *batch.Batch {
		bat := batch.NewWithSize(1)
		bat.SetAttributes([]string{"pk"})
		pkCol := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(pkCol, pks, nil, txn.proc.Mp()))
		bat.Vecs[0] = pkCol
		bat.SetRowCount(len(pks))
		return bat
	}

	ledger := func() (queued int, live int64, inFlight int64) {
		e.unpublishedPending.mu.Lock()
		defer e.unpublishedPending.mu.Unlock()
		return len(e.unpublishedPending.names), e.unpublishedPending.live,
			e.unpublishedPending.inFlight
	}

	freshWriter := func() *colexec.CNS3Writer {
		return colexec.NewCNS3DataWriterForService(
			txn.proc.GetService(), txn.proc.Mp(), failFS, tbl.tableDef, -1, false,
			txn.SinkerCapacityOption())
	}

	// The binding itself: the workspace's option rides on the writer.
	w0 := freshWriter()
	require.True(t, w0.HasUncommittedCapacityHook(),
		"a writer built on the workspace's option carries the per-object capacity hook")
	w0.Close()

	// Leg A - the saturated bound refuses the writer's spill BEFORE any
	// object persists: repeated refused rounds keep persisting NOTHING.
	knobSaved := unpublishedPendingAdmissionLimit
	unpublishedPendingAdmissionLimit = 0
	t.Cleanup(func() { unpublishedPendingAdmissionLimit = knobSaved })
	for round := 0; round < 3; round++ {
		w := freshWriter()
		rows := writeRows(1, 2, 3)
		require.NoError(t, w.Write(ctx, rows))
		rows.Clean(txn.proc.Mp())
		_, err := w.Sync(ctx)
		require.ErrorContains(t, err, "workspace admission paused",
			"the saturated bound refuses the object's persistence before it goes durable - round %d", round)
		require.Empty(t, w.PersistedNames(),
			"the refused writer holds no persisted or possibly-persisted name - round %d", round)
		require.Empty(t, txn.pendingObjectCleanup,
			"refused rounds leave no orphan behind - round %d", round)
		w.Close()
	}
	_, _, inFlight := ledger()
	require.Zero(t, inFlight,
		"every refused round released its exact in-flight unit")
	require.Len(t, failFS.persistedSnapshot(), 0,
		"the three refused rounds persisted nothing at all")
	unpublishedPendingAdmissionLimit = knobSaved

	// Leg B - the production record shape: the writer persists ONE
	// object (its hook reserves the unit), fillBlockInfoBat assembles the
	// metadata batch, and txnTable.Write's metadata branch - driven here
	// through its WriteFileLocked base - CONVERts that reservation at the
	// record: the object is a recorded obligation, owned by its record's
	// lifecycle, and its physical file stays untouched forever after.
	w1 := freshWriter()
	rows1 := writeRows(11, 12)
	require.NoError(t, w1.Write(ctx, rows1))
	rows1.Clean(txn.proc.Mp())
	_, err = w1.Sync(ctx)
	require.NoError(t, err)
	require.Len(t, w1.PersistedNames(), 1)
	// captured BEFORE the fill call: SyncAndTakeResults moves the stats
	// out of the sinker's result registry, and the writer's ownership
	// snapshot must not depend on a second sync.
	w1Persisted := w1.PersistedNames()
	require.Equal(t, 1, len(failFS.persistedSnapshot()))
	_, _, inFlightA := ledger()
	require.Equal(t, int64(1), inFlightA,
		"the persisted object holds exactly one unreleased reservation")
	meta1, err := w1.SyncAndFillBlockInfoBat(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { meta1.Clean(txn.proc.Mp()) })
	txn.Lock()
	err = txn.WriteFileLocked(INSERT, 1, 7, 42, "db", "tbl",
		"", meta1, DNStore{})
	txn.Unlock()
	require.NoError(t, err,
		"the record of the reserved object converts its own reservation")
	recordedName := w1Persisted[0]
	_, liveB, inFlightB := ledger()
	require.Zero(t, inFlightB,
		"the record retired the reservation exactly")
	require.Equal(t, int64(1), liveB,
		"the recorded object is the ledger's one owned member")
	require.Contains(t, issue25590RecordedSnapshot(txn), recordedName,
		"the record carries its object's obligation")
	w1.Close()

	// Leg C - the unwind shape: the writer persisted its object, the
	// stream died before mergeBlock's record (a pipeline failure or a
	// refused later stage). The teardown's retake hands ONLY the
	// UNRECORDED names to the transaction's asynchronous cleanup
	// ownership - the recorded one above stays with its record - and the
	// next admission boundary's handoff moves them to the engine's
	// lossless queue, whose drain deletes the physical object while the
	// recorded object's file stays untouched.
	w2 := freshWriter()
	rows2 := writeRows(21, 22)
	require.NoError(t, w2.Write(ctx, rows2))
	rows2.Clean(txn.proc.Mp())
	_, err = w2.Sync(ctx)
	require.NoError(t, err)
	require.Len(t, w2.PersistedNames(), 1)
	orphanName := w2.PersistedNames()[0]
	require.NotEqual(t, recordedName, orphanName)
	// The production Free hands the COMPLETE snapshot - the retake filter
	// must drop the recorded member and keep only the unwound one.
	txn.RetainUnrecordedS3Objects(
		[]string{recordedName, orphanName},
		"test unwind: the pipeline died between persistence and mergeBlock")
	w2.Close()
	require.Equal(t, []string{orphanName}, txn.pendingObjectCleanupNames(),
		"exactly the UNRECORDED object joined the cleanup obligation set")
	_, liveC, inFlightC := ledger()
	require.Equal(t, int64(2), liveC,
		"the retake converted the unwound object's reservation into owned membership")
	require.Zero(t, inFlightC)

	// The next admission boundary hands the pending obligation to the
	// engine queue, and one drain round deletes the physical orphan.
	txn.Lock()
	txn.handoffPendingObjectCleanup()
	txn.Unlock()
	require.Equal(t, []string{orphanName}, issue25590QueuedNames(e))
	require.NoError(t, e.deleteQueuedUnpublishedPendingOnce(ctx))
	issue25590RequireGone(t, failFS.FileService, orphanName,
		"the unwound writer's object, deleted by the engine queue once its record never came")
	issue25590RequireKept(t, failFS.FileService, recordedName,
		"the RECORDED object's file - its deletion is owned by its record's lifecycle, never by an unwind")
}

func issue25590AssertLiveMapSize(t *testing.T, txn *Transaction, want int, msgAndArgs ...interface{}) {
	t.Helper()
	require.Len(t, issue25590RecordedSnapshot(txn), want, msgAndArgs...)
}

// issue25590RecordedSnapshot copies the transaction's recorded-name set
// under the recorded-name guard: the round-37 gcFiles settle retires
// settled members from its pool worker without the transaction lock, so
// every assertion on the set reads the same guarded copy the production
// settle path owns (issue #25590 round-37 review).
func issue25590RecordedSnapshot(txn *Transaction) map[string]struct{} {
	txn.recordedUnpublishedObjectNamesMu.Lock()
	defer txn.recordedUnpublishedObjectNamesMu.Unlock()
	if txn.recordedUnpublishedObjectNames == nil {
		return nil
	}
	names := make(map[string]struct{}, len(txn.recordedUnpublishedObjectNames))
	for n := range txn.recordedUnpublishedObjectNames {
		names[n] = struct{}{}
	}
	return names
}

// issue25590RecordedLen is the counting twin of issue25590RecordedSnapshot.
func issue25590RecordedLen(txn *Transaction) int {
	txn.recordedUnpublishedObjectNamesMu.Lock()
	defer txn.recordedUnpublishedObjectNamesMu.Unlock()
	return len(txn.recordedUnpublishedObjectNames)
}

// TestIssue25590TwoDirtyObjectCompactionIsAtomicAcrossReplacements pins
// the round-20 review finding 1: the compaction pass must publish its
// replacement records ATOMICALLY across every replacement of the pass.
// Two dirty objects share the pass and the admission bound allows exactly
// one per-object reservation, so running through the deterministic
// single-slot compaction worker the FIRST dirty object rewrites and
// persists while the SECOND rewrite is refused at its writer's capacity
// hook. The first replacement's record has by then completed EVERY
// fallible step (its batch copy, its choke admission) on the fixed tree -
// none of that may survive the failed pass as a live workspace entry next
// to an un-retired original, because the failed admission's statement
// boundary sits above such an append and the retried compaction would
// commit the surviving rows twice. The failed pass also releases the
// clean objects' staged re-publication batches, whose allocation only
// this pass could have freed (round-20 review, finding 4), and hands the
// persisted-but-unrecorded replacement to the cleanup ownership so the
// queue drains the orphaned file while every original stays kept.
func TestIssue25590TwoDirtyObjectCompactionIsAtomicAcrossReplacements(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)
	defer func() {
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	}()

	ctx := context.Background()
	e := txn.engine
	e.pClient = PushClient{}
	failFS := issue25590SwapProcSharedFS(t, txn)
	e.fs = failFS

	// A sequential single-slot compaction worker makes the pass order
	// deterministic: the first dirty object's rewrite always hooks and
	// persists first, and the second rewrite always faces the pinned
	// bound. The engine GC pool serves the queue drain below.
	compactPool, err := ants.NewPool(1)
	require.NoError(t, err)
	t.Cleanup(func() { compactPool.Release() })
	txn.compactWorker = compactPool
	gcPool, err := ants.NewPool(2)
	require.NoError(t, err)
	t.Cleanup(func() { gcPool.Release() })
	require.NoError(t, e.ResetGCWorkerPool(gcPool))

	// The data-column table whose dirty objects the pass rewrites:
	// [a(pk), v, rowid] exactly like a physical table's def.
	dbv := txn.databaseOps.existAndActive(genDatabaseKey(1, "db"))
	require.NotNil(t, dbv)
	const ctblID = uint64(44)
	txn.tableOps.addCreateTable(
		genTableKey(1, "ctbl", 7, "db"),
		0,
		&txnTable{
			accountId: 1,
			tableId:   ctblID,
			tableName: "ctbl",
			db:        dbv,
			tableDef: &pbplan.TableDef{
				Cols: []*pbplan.ColDef{
					{Name: "a", Typ: pbplan.Type{Id: int32(types.T_int64)}, Primary: true},
					{Name: "v", Typ: pbplan.Type{Id: int32(types.T_int64)}},
					{Name: catalog.Row_ID,
						Typ: pbplan.Type{Id: int32(types.T_Rowid)}},
				},
				Pkey: &pbplan.PrimaryKeyDef{PkeyColName: "a"},
			},
		},
	)

	admit := func() { issue25590AdmitBoundary(t, txn, ctx) }

	ledger := func() (queued int, live int64, inFlight int64) {
		e.unpublishedPending.mu.Lock()
		defer e.unpublishedPending.mu.Unlock()
		return len(e.unpublishedPending.names), e.unpublishedPending.live,
			e.unpublishedPending.inFlight
	}

	// Statement 1: a wide raw entry whose forced dump publishes ONE
	// metadata batch with at least three objects - two to mark dirty
	// below and one clean object whose re-publication the pass stages.
	const totalRows = 300000
	admit()
	pks := make([]int64, totalRows)
	vals := make([]int64, totalRows)
	for i := range pks {
		pks[i] = int64(i + 1)
		vals[i] = int64(i + 1)
	}
	rowidVec, err := txn.batchAllocNewRowIds(totalRows)
	require.NoError(t, err)
	defer rowidVec.Free(txn.proc.Mp())
	rowids := vector.MustFixedColNoTypeCheck[types.Rowid](rowidVec)
	batchA := batch.NewWithSize(3)
	batchA.SetAttributes([]string{objectio.PhysicalAddr_Attr, "a", "v"})
	rowidCol := vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixedList(rowidCol, rowids, nil, txn.proc.Mp()))
	aCol := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(aCol, pks, nil, txn.proc.Mp()))
	vCol := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(vCol, vals, nil, txn.proc.Mp()))
	batchA.Vecs[0] = rowidCol
	batchA.Vecs[1] = aCol
	batchA.Vecs[2] = vCol
	batchA.SetRowCount(totalRows)
	txn.appendWorkspaceEntryLocked(Entry{
		typ:          INSERT,
		accountId:    1,
		databaseId:   issue25590DBID,
		tableId:      ctblID,
		databaseName: "db",
		tableName:    "ctbl",
		bat:          batchA,
	})
	smallObjsDisarm := issue25590ArmKeyedFault(t, objectio.FJ_CNFlushSmallObjs, 20, "")
	txn.Lock()
	var pkCount int
	err = txn.dumpInsertBatchLocked(ctx, failFS, txn.GetSnapshotWriteOffset(), &pkCount)
	txn.Unlock()
	smallObjsDisarm()
	require.NoError(t, err)
	require.Equal(t, totalRows, pkCount)

	metaIdx, statsCol, statsArea := issue25590MetadataEntry(t, txn)
	objCount := len(statsCol)
	require.GreaterOrEqual(t, objCount, 3,
		"the forced dump published a multi-object batch with a clean object to re-publish")
	objStats := make([]objectio.ObjectStats, objCount)
	for i := 0; i < objCount; i++ {
		objStats[i] = objectio.ObjectStats(statsCol[i].GetByteSlice(statsArea))
		require.Greater(t, int(objStats[i].BlkCnt()), 0)
	}
	lastStats := objStats[objCount-1]
	metaBat := txn.writes[metaIdx].bat
	metaRowCount := metaBat.RowCount()
	infoCol, infoArea := vector.MustVarlenaRawData(metaBat.Vecs[0])
	headFirstBlock := objectio.DecodeBlockInfo(
		infoCol[0].GetByteSlice(infoArea)).BlockID
	tailBlockOffset := 0
	for i := 0; i < objCount-1; i++ {
		tailBlockOffset += int(objStats[i].BlkCnt())
	}
	tailFirstBlock := objectio.DecodeBlockInfo(
		infoCol[tailBlockOffset].GetByteSlice(infoArea)).BlockID
	tailRows := int(lastStats.BlkCnt()) * objectio.BlockMaxRows
	tailRowStart := totalRows - tailRows
	require.LessOrEqual(t, tailRowStart+2, totalRows)
	txn.EndStatement()

	// Statement 2: pending deletes on the HEAD object's and the TAIL
	// object's first blocks - TWO dirty objects for the next pass.
	admit()
	delBat := batch.NewWithSize(2)
	delBat.SetAttributes([]string{objectio.PhysicalAddr_Attr, "a"})
	rowidVec2 := vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixedList(
		rowidVec2, []types.Rowid{
			types.NewRowid(&headFirstBlock, 0),
			types.NewRowid(&headFirstBlock, 1),
			types.NewRowid(&tailFirstBlock, 0),
			types.NewRowid(&tailFirstBlock, 1),
		}, nil, txn.proc.Mp()))
	pk2Vec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(
		pk2Vec,
		[]int64{pks[0], pks[1], pks[tailRowStart], pks[tailRowStart+1]},
		nil, txn.proc.Mp()))
	delBat.Vecs[0] = rowidVec2
	delBat.Vecs[1] = pk2Vec
	delBat.SetRowCount(4)
	out := txn.deleteBatch(delBat, issue25590DBID, ctblID)
	t.Cleanup(func() { out.Clean(txn.proc.Mp()) })
	require.Equal(t, 2, txn.deletedBlocks.size(),
		"the four drops mark two blocks on two dirty objects")
	txn.EndStatement()

	dumpedNames := failFS.persistedSnapshot()
	require.Len(t, dumpedNames, objCount,
		"the forced dump persisted exactly its batch's objects")
	require.Len(t, txn.writes, 1,
		"the arrange phase holds exactly the dumped metadata entry")
	_, live, inFlight := ledger()
	require.Equal(t, int64(objCount), live,
		"the dump's recorded objects are the only owned obligations")
	require.Zero(t, inFlight)

	originalNames := make(map[string]bool, objCount)
	for i := range objStats {
		originalNames[objStats[i].ObjectName().String()] = true
	}

	// The pinned bound lets exactly ONE replacement reserve its in-flight
	// unit: the first (head) rewrite persists, the second (tail) rewrite
	// is refused at its writer's hook - the later-rewrite failure with the
	// earlier replacement ready - and the first record's full staged
	// admission retracts with the pass.
	knobSaved := unpublishedPendingAdmissionLimit
	unpublishedPendingAdmissionLimit = objCount + 1
	t.Cleanup(func() { unpublishedPendingAdmissionLimit = knobSaved })

	mpOf := txn.proc.Mp()
	var pendingBefore []string
	for round := 0; round < 3; round++ {
		currNBBefore := mpOf.CurrNB()
		txn.Lock()
		err = txn.compactDeletionOnObjsLocked(ctx)
		txn.Unlock()
		require.ErrorContains(t, err, "workspace admission paused",
			"round %d: the saturated-bound failure fails the whole pass", round)
		require.Len(t, txn.writes, 1,
			"round %d: NO replacement record survived the partially failed pass - the pre-fix loop appended the earlier successful record next to its un-retired original and the rollback could not remove it (issue #25590 round-20 review, finding 1)", round)
		require.NotNil(t, txn.writes[metaIdx].bat,
			"round %d: the refused pass may not consume the original metadata batch", round)
		require.Equal(t, metaRowCount, txn.writes[metaIdx].bat.RowCount(),
			"round %d: the original metadata batch is byte-identical", round)
		require.Equal(t, 2, txn.deletedBlocks.size(),
			"round %d: both dirty objects' pending deletions stay for the retry", round)
		require.Len(t, txn.deletedBlocks.offsets[headFirstBlock], 2,
			"round %d: the head object's marks stay exact", round)
		require.Len(t, txn.deletedBlocks.offsets[tailFirstBlock], 2,
			"round %d: the tail object's marks stay exact", round)
		issue25590AssertLiveMapSize(t, txn, objCount,
			"round %d: the staged record's admission retracted - only the dump's objects stay recorded", round)
		pending := append([]string(nil), txn.pendingObjectCleanupNames()...)
		if round == 0 {
			require.Len(t, pending, 1,
				"round 0: exactly the persisted-but-unrecorded replacement joined the cleanup ownership")
			require.False(t, originalNames[pending[0]],
				"round 0: the taken name is a FRESH replacement object, not an original")
			require.Len(t, failFS.persistedSnapshot(), objCount+1,
				"round 0: exactly one replacement persisted behind the one granted reservation")
			_, live, inFlight = ledger()
			require.Equal(t, int64(objCount+1), live,
				"round 0: the retake kept exactly one owned member for the orphaned replacement")
			require.Zero(t, inFlight)
		} else {
			require.Equal(t, pendingBefore, pending,
				"round %d: repeated refusals accumulate no new obligation", round)
			require.Len(t, failFS.persistedSnapshot(), objCount+1,
				"round %d: repeated refusals persist nothing new", round)
			_, live, inFlight = ledger()
			require.Equal(t, int64(objCount+1), live)
			require.Zero(t, inFlight)
		}
		// The staged re-publication batches and the prepared record copy
		// released with the failed pass: no refused admission may leave
		// allocated batches behind the bound (round-20 review, finding 4).
		require.Equal(t, currNBBefore, mpOf.CurrNB(),
			"round %d: the failed pass left the mpool exactly where it found it", round)
		if round == 0 {
			pendingBefore = pending
		}
	}

	// The cleanup boundary hands the orphaned replacement to the engine
	// queue, whose drain deletes the physical file while every original
	// stays kept. The rewriter's snapshot is possibility-inclusive: a
	// possibly-persisted name whose spill never reached storage is
	// already gone, and the lossless ladder keeps it queued with a
	// not-found answer - the only tolerated drain error; a PHYSICALLY
	// persisted file may never fail the deletion.
	txn.Lock()
	txn.handoffPendingObjectCleanup()
	txn.Unlock()
	require.Equal(t, pendingBefore, issue25590QueuedNames(e))
	drainErr := e.deleteQueuedUnpublishedPendingOnce(ctx)
	if drainErr != nil {
		require.ErrorContains(t, drainErr, "no such file or directory",
			"the only tolerated drain error is an already-absent possibly-persisted name")
		persistedNow := failFS.persistedSnapshot()
		for _, name := range pendingBefore {
			require.NotContains(t, persistedNow, name,
				"a PHYSICALLY persisted replacement may not fail the drain")
		}
	}
	for _, name := range pendingBefore {
		issue25590RequireGone(t, failFS.FileService, name,
			"the refused pass's persisted replacement, deleted once no record owns it")
	}
	for name := range originalNames {
		issue25590RequireKept(t, failFS.FileService, name, "an original object")
	}

	// The retried pass under the restored bound completes ATOMICALLY: both
	// dirty objects re-rewrite, the clean objects re-publish, the
	// originals retire only now - and no duplicated live entry from the
	// half-recorded earlier pass exists anywhere.
	unpublishedPendingAdmissionLimit = knobSaved
	txn.Lock()
	err = txn.compactDeletionOnObjsLocked(ctx)
	txn.Unlock()
	require.NoError(t, err, "the retried pass under the restored bound completes")

	require.Zero(t, txn.deletedBlocks.size(),
		"the completed compaction embodies the deletions and clears their marks")
	require.Nil(t, txn.writes[metaIdx].bat,
		"the original metadata batch retired only after the pass fully completed")
	require.Empty(t, txn.pendingObjectCleanup)

	idxes := issue25590AllMetadataEntries(t, txn)
	require.Len(t, idxes, objCount,
		"two replacement records plus the clean objects' re-publications - one entry per object, no duplicates")
	freshReplacements := make([]string, 0, 2)
	for _, idx := range idxes {
		for _, name := range issue25590StatsNamesOfMeta(t, txn, idx) {
			if !originalNames[name] {
				freshReplacements = append(freshReplacements, name)
			}
		}
	}
	require.Len(t, freshReplacements, 2,
		"exactly TWO fresh replacement names - the retried pass re-rewrote both dirty objects without resurrecting the aborted attempt's name")
	for _, name := range freshReplacements {
		require.Contains(t, issue25590RecordedSnapshot(txn), name,
			"the completed pass's replacement record carries its name's obligation")
		issue25590RequireKept(t, failFS.FileService, name,
			"the completed pass's replacement object")
	}
	_, live, inFlight = ledger()
	require.Equal(t, int64(objCount+2), live,
		"replacements' reservations converted into exactly one owned member per object")
	require.Zero(t, inFlight)
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
}

// TestIssue25590RecordFailureAfterStagedReplacementRetracts is the
// record-level leg of the round-20 finding 1 proof: two dirty objects on
// two tables, whose rewrites BOTH persist under a bound with room for two
// fresh reservations, and the FIRST record fully stages - its choke
// admitted and converted its name - before the SECOND table's record
// fails its own preparation (the keyed FJ_CNWorkspaceRecordStageFailed
// refusal point stands in for any record-level failure an operator cannot
// yield to). The staged admission of the first record must retract with
// the failed pass, nothing may append, both persisted replacements join
// the cleanup ownership, and the retried pass - with the refusal disarmed -
// rewrites both objects without duplicating any surviving row.
func TestIssue25590RecordFailureAfterStagedReplacementRetracts(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)
	defer func() {
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	}()

	ctx := context.Background()
	e := txn.engine
	e.pClient = PushClient{}
	failFS := issue25590SwapProcSharedFS(t, txn)
	e.fs = failFS

	compactPool, err := ants.NewPool(1)
	require.NoError(t, err)
	t.Cleanup(func() { compactPool.Release() })
	txn.compactWorker = compactPool
	gcPool, err := ants.NewPool(2)
	require.NoError(t, err)
	t.Cleanup(func() { gcPool.Release() })
	require.NoError(t, e.ResetGCWorkerPool(gcPool))

	dbv := txn.databaseOps.existAndActive(genDatabaseKey(1, "db"))
	require.NotNil(t, dbv)
	const (
		ctaID = uint64(44)
		ctbID = uint64(45)
	)
	newCtbl := func(id uint64, name string) *txnTable {
		return &txnTable{
			accountId: 1,
			tableId:   id,
			tableName: name,
			db:        dbv,
			tableDef: &pbplan.TableDef{
				Cols: []*pbplan.ColDef{
					{Name: "a", Typ: pbplan.Type{Id: int32(types.T_int64)}, Primary: true},
					{Name: "v", Typ: pbplan.Type{Id: int32(types.T_int64)}},
					{Name: catalog.Row_ID,
						Typ: pbplan.Type{Id: int32(types.T_Rowid)}},
				},
				Pkey: &pbplan.PrimaryKeyDef{PkeyColName: "a"},
			},
		}
	}
	txn.tableOps.addCreateTable(genTableKey(1, "cta", 7, "db"), 0, newCtbl(ctaID, "cta"))
	txn.tableOps.addCreateTable(genTableKey(1, "ctb", 7, "db"), 0, newCtbl(ctbID, "ctb"))

	admit := func() { issue25590AdmitBoundary(t, txn, ctx) }
	ledger := func() (queued int, live int64, inFlight int64) {
		e.unpublishedPending.mu.Lock()
		defer e.unpublishedPending.mu.Unlock()
		return len(e.unpublishedPending.names), e.unpublishedPending.live,
			e.unpublishedPending.inFlight
	}

	// Statement 1: two raw entries, one per table; ONE forced dump
	// publishes both tables' metadata batches in workspace order - cta's
	// entries always come before ctb's, so cta's rewrite and record run
	// FIRST deterministically under the single-slot compaction worker.
	const rowsPerTable = 100000
	admit()
	for _, tt := range []struct {
		id   uint64
		name string
	}{{ctaID, "cta"}, {ctbID, "ctb"}} {
		pks := make([]int64, rowsPerTable)
		vals := make([]int64, rowsPerTable)
		for i := range pks {
			pks[i] = int64(i + 1)
			vals[i] = int64(i + 1)
		}
		rowidVec, err := txn.batchAllocNewRowIds(rowsPerTable)
		require.NoError(t, err)
		defer rowidVec.Free(txn.proc.Mp())
		rowids := vector.MustFixedColNoTypeCheck[types.Rowid](rowidVec)
		bat := batch.NewWithSize(3)
		bat.SetAttributes([]string{objectio.PhysicalAddr_Attr, "a", "v"})
		rowidCol := vector.NewVec(types.T_Rowid.ToType())
		require.NoError(t, vector.AppendFixedList(rowidCol, rowids, nil, txn.proc.Mp()))
		aCol := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(aCol, pks, nil, txn.proc.Mp()))
		vCol := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(vCol, vals, nil, txn.proc.Mp()))
		bat.Vecs[0] = rowidCol
		bat.Vecs[1] = aCol
		bat.Vecs[2] = vCol
		bat.SetRowCount(rowsPerTable)
		txn.appendWorkspaceEntryLocked(Entry{
			typ:          INSERT,
			accountId:    1,
			databaseId:   issue25590DBID,
			tableId:      tt.id,
			databaseName: "db",
			tableName:    tt.name,
			bat:          bat,
		})
	}
	smallObjsDisarm := issue25590ArmKeyedFault(t, objectio.FJ_CNFlushSmallObjs, 20, "")
	txn.Lock()
	var pkCount int
	err = txn.dumpInsertBatchLocked(ctx, failFS, txn.GetSnapshotWriteOffset(), &pkCount)
	txn.Unlock()
	smallObjsDisarm()
	require.NoError(t, err)
	require.Equal(t, 2*rowsPerTable, pkCount)
	txn.EndStatement()

	type metaBatInfo struct {
		idx  int
		id   uint64
		name string
	}
	metas := []metaBatInfo{}
	for i := range txn.writes {
		entry := txn.writes[i]
		if entry.bat == nil || entry.bat.IsEmpty() ||
			entry.typ != INSERT || entry.bat.Attrs[0] != catalog.BlockMeta_BlockInfo {
			continue
		}
		metas = append(metas, metaBatInfo{idx: i, id: entry.tableId, name: entry.tableName})
	}
	require.Len(t, metas, 2,
		"the forced dump published exactly two metadata batches")
	require.NotEqual(t, metas[0].name, metas[1].name,
		"the fixture's two tables published distinct metadata batches")

	objCountOf := func(i int) (objectio.ObjectStats, objectio.ObjectStats, int, types.Blockid) {
		entry := txn.writes[metas[i].idx]
		statsCol, statsArea := vector.MustVarlenaRawData(entry.bat.Vecs[1])
		infoCol, infoArea := vector.MustVarlenaRawData(entry.bat.Vecs[0])
		n := len(statsCol)
		require.GreaterOrEqual(t, n, 2,
			"table %s's dump batch holds a clean object to re-publish", metas[i].name)
		objs := make([]objectio.ObjectStats, n)
		for j := 0; j < n; j++ {
			objs[j] = objectio.ObjectStats(statsCol[j].GetByteSlice(statsArea))
		}
		tailOffset := 0
		for j := 0; j < n-1; j++ {
			tailOffset += int(objs[j].BlkCnt())
		}
		blk := objectio.DecodeBlockInfo(
			infoCol[tailOffset].GetByteSlice(infoArea)).BlockID
		return objs[0], objs[n-1], n, blk
	}
	_, _, ctaCount, firstTailBlock := objCountOf(0)
	_, _, ctbCount, secondTailBlock := objCountOf(1)
	totalObjs := ctaCount + ctbCount

	// Statement 2: mark each table's TAIL object dirty through its final
	// block's first row - position-bound, because the dump publishes the
	// two batches in map-iteration order.
	admit()
	for _, mt := range []struct {
		id  uint64
		blk types.Blockid
	}{{metas[0].id, firstTailBlock}, {metas[1].id, secondTailBlock}} {
		delBat := batch.NewWithSize(2)
		delBat.SetAttributes([]string{objectio.PhysicalAddr_Attr, "a"})
		rowid := vector.NewVec(types.T_Rowid.ToType())
		require.NoError(t, vector.AppendFixedList(
			rowid, []types.Rowid{types.NewRowid(&mt.blk, 0)}, nil, txn.proc.Mp()))
		pkOne := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(pkOne, []int64{1}, nil, txn.proc.Mp()))
		delBat.Vecs[0] = rowid
		delBat.Vecs[1] = pkOne
		delBat.SetRowCount(1)
		out := txn.deleteBatch(delBat, issue25590DBID, mt.id)
		t.Cleanup(func() { out.Clean(txn.proc.Mp()) })
	}
	require.Equal(t, 2, txn.deletedBlocks.size(),
		"each table's tail object carries one pending deletion")
	txn.EndStatement()

	dumpedNames := failFS.persistedSnapshot()
	require.Len(t, dumpedNames, totalObjs)
	require.Len(t, txn.writes, 2,
		"the arrange phase holds exactly the two dumped metadata entries")
	_, live, inFlight := ledger()
	require.Equal(t, int64(totalObjs), live)
	require.Zero(t, inFlight)

	allOriginalNames := make(map[string]bool, totalObjs)
	for _, i := range []int{metas[0].idx, metas[1].idx} {
		statsCol, statsArea := vector.MustVarlenaRawData(txn.writes[i].bat.Vecs[1])
		for j := range statsCol {
			st := objectio.ObjectStats(statsCol[j].GetByteSlice(statsArea))
			allOriginalNames[st.ObjectName().String()] = true
		}
	}

	// The bound has room for BOTH replacements' fresh reservations, so
	// both rewrites persist under the deterministic single-slot
	// compaction worker and Phase B stages the FIRST-ordered batch's
	// record FULLY (copy + choke admission) before the SECOND-ordered
	// batch's record fails its own preparation. The dump publishes the
	// two tables' batches in map-iteration order, so the fault arms for
	// whichever table landed SECOND - the pass walks the dirty objects in
	// workspace order either way, keeping the staged-then-refused shape
	// deterministic.
	secondTable := metas[1].name
	stageDisarm := issue25590ArmKeyedFault(
		t, objectio.FJ_CNWorkspaceRecordStageFailed, 0, secondTable)
	unpublishedPendingAdmissionLimit = totalObjs + 2
	t.Cleanup(func() { unpublishedPendingAdmissionLimit = 500000 })

	mpOf := txn.proc.Mp()
	currNBBefore := mpOf.CurrNB()
	txn.Lock()
	err = txn.compactDeletionOnObjsLocked(ctx)
	txn.Unlock()
	require.ErrorContains(t, err, "fault injection: workspace record staging failed",
		"the second table's record fails its preparation after the first record staged")
	// Nothing survived: no append, no staged admission, no marks cleared,
	// no original retired.
	require.Len(t, txn.writes, 2,
		"the failed record left NO live replacement entry - the pre-fix pass kept the first table's fully recorded replacement, which the rollback could not remove and the retry then duplicated (issue #25590 round-20 review, finding 1)")
	require.Equal(t, 2, txn.deletedBlocks.size())
	for i := range metas {
		require.NotNil(t, txn.writes[metas[i].idx].bat,
			"table %s's original metadata batch stays intact", metas[i].name)
	}
	issue25590AssertLiveMapSize(t, txn, totalObjs,
		"the staged record's admission retracted - only the dump's objects stay recorded")
	pending := append([]string(nil), txn.pendingObjectCleanupNames()...)
	require.Len(t, pending, 2,
		"both persisted-but-unrecorded replacements joined the cleanup ownership")
	for _, name := range pending {
		require.False(t, allOriginalNames[name],
			"the taken names are FRESH replacement objects, not originals")
	}
	_, live, inFlight = ledger()
	require.Equal(t, int64(totalObjs+2), live,
		"each orphaned replacement kept exactly one owned member")
	require.Equal(t, int64(0), inFlight,
		"both reservations converted at the retraction retake")
	require.Equal(t, currNBBefore, mpOf.CurrNB(),
		"the failed pass released every staged re-publication batch and prepared record copy")

	// The queue drains both orphaned replacement files physically. The
	// rewriter's persisted-name snapshot is deliberately
	// possibility-inclusive (the writer reports objects whose spill may
	// not have reached the filesystem): a name that never landed is
	// already gone, and the LOSSLESS ladder answers the not-found by
	// keepING the name queued - the only tolerated drain error. A file
	// that DID physically persist may never fail the deletion.
	txn.Lock()
	txn.handoffPendingObjectCleanup()
	txn.Unlock()
	require.ElementsMatch(t, pending, issue25590QueuedNames(e))
	drainErr := e.deleteQueuedUnpublishedPendingOnce(ctx)
	if drainErr != nil {
		require.ErrorContains(t, drainErr, "no such file or directory",
			"the only tolerated drain error is an already-absent possibly-persisted name")
		persisted := failFS.persistedSnapshot()
		for _, name := range pending {
			require.NotContains(t, persisted, name,
				"a PHYSICALLY persisted replacement may not fail the drain")
		}
	}
	for _, name := range pending {
		issue25590RequireGone(t, failFS.FileService, name,
			"an orphaned replacement the failed record left ownerless")
	}
	for name := range allOriginalNames {
		issue25590RequireKept(t, failFS.FileService, name, "an original object")
	}

	// The retried pass - refusal disarmed, bound restored - completes and
	// rewrites BOTH dirty objects exactly once.
	stageDisarm()
	unpublishedPendingAdmissionLimit = 500000
	txn.Lock()
	err = txn.compactDeletionOnObjsLocked(ctx)
	txn.Unlock()
	require.NoError(t, err, "the retried pass completes with the refusal disarmed")

	require.Zero(t, txn.deletedBlocks.size())
	for i := range metas {
		require.Nil(t, txn.writes[metas[i].idx].bat,
			"table %s's original metadata batch retired only after the pass completed", metas[i].name)
	}
	idxes := issue25590AllMetadataEntries(t, txn)
	require.Len(t, idxes, totalObjs,
		"one record per object - replacements and re-publications, no duplicate from the aborted attempt")
	freshReplacements := 0
	for _, idx := range idxes {
		for _, name := range issue25590StatsNamesOfMeta(t, txn, idx) {
			if !allOriginalNames[name] {
				freshReplacements++
			}
		}
	}
	require.Equal(t, 2, freshReplacements,
		"exactly two fresh replacement names - one re-rewrite per dirty object")
	_, live, inFlight = ledger()
	require.Equal(t, int64(totalObjs+2), live)
	require.Zero(t, inFlight)
	require.Empty(t, txn.pendingObjectCleanup)
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
}

// TestIssue25590FailedRecordCopyKeepsObjectRetrievable pins the
// round-20 review finding 3: the workspace's own batch copy of a record
// runs BEFORE the record commits its name membership and converts its
// reservations, so a copy failure leaves the persisted object reachable
// for the unwinding teardown, which hands it to the cleanup ownership and
// the queue deletes the file - balanced against the ledger. The choke
// refusal AFTER a successful copy must release the prepared copy bit by
// bit. Before this order, a failed copy had already merged the name as
// recorded and consumed the reservation, so every left-over-object
// enumeration - the retainer's filter, rollback's recorded entries, the
// terminal's release - skipped the object as somebody else's obligation
// while no workspace entry ever existed for it: its file survived on
// storage with no owner.
func TestIssue25590FailedRecordCopyKeepsObjectRetrievable(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)
	defer func() {
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	}()

	ctx := context.Background()
	e := txn.engine
	e.pClient = PushClient{}
	failFS := issue25590SwapProcSharedFS(t, txn)
	e.fs = failFS
	gcPool, err := ants.NewPool(2)
	require.NoError(t, err)
	t.Cleanup(func() { gcPool.Release() })
	require.NoError(t, e.ResetGCWorkerPool(gcPool))

	rel, err := txn.getTable(ctx, 1, "db", "tbl")
	require.NoError(t, err)
	tbl, ok := rel.(*txnTable)
	require.True(t, ok)

	writeRows := func(pks ...int64) *batch.Batch {
		bat := batch.NewWithSize(1)
		bat.SetAttributes([]string{"pk"})
		pkCol := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(pkCol, pks, nil, txn.proc.Mp()))
		bat.Vecs[0] = pkCol
		bat.SetRowCount(len(pks))
		return bat
	}
	ledger := func() (queued int, live int64, inFlight int64) {
		e.unpublishedPending.mu.Lock()
		defer e.unpublishedPending.mu.Unlock()
		return len(e.unpublishedPending.names), e.unpublishedPending.live,
			e.unpublishedPending.inFlight
	}

	// Leg A: the record's own copy fails at the mpool allocation cap.
	// The writer persisted one object and holds one in-flight reservation
	// - exactly the production shape of a record whose allocation fails
	// between persistence and recording.
	w1 := colexec.NewCNS3DataWriterForService(
		txn.proc.GetService(), txn.proc.Mp(), failFS, tbl.tableDef, -1, false,
		txn.SinkerCapacityOption())
	require.True(t, w1.HasUncommittedCapacityHook())
	rows1 := writeRows(11, 12)
	require.NoError(t, w1.Write(ctx, rows1))
	rows1.Clean(txn.proc.Mp())
	_, err = w1.Sync(ctx)
	require.NoError(t, err)
	require.Len(t, w1.PersistedNames(), 1)
	nameA := w1.PersistedNames()[0]
	metaA, err := w1.SyncAndFillBlockInfoBat(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { metaA.Clean(txn.proc.Mp()) })

	capSaved := mpool.CapLimit
	mpool.CapLimit = 1
	txn.Lock()
	err = txn.WriteFileLocked(INSERT, 1, 7, 42, "db", "tbl", "", metaA, DNStore{})
	txn.Unlock()
	require.Error(t, err, "the record's own batch copy fails at the allocation cap")
	mpool.CapLimit = capSaved

	// The copy failure consumed NOTHING: the name never registered and
	// the reservation stays outstanding until an owner settles it.
	require.NotContains(t, issue25590RecordedSnapshot(txn), nameA,
		"a record whose copy failed registered no name membership (issue #25590 round-20 review, finding 3)")
	require.Empty(t, txn.pendingObjectCleanup)
	_, liveA, inFlightA := ledger()
	require.Zero(t, liveA)
	require.Equal(t, int64(1), inFlightA,
		"the failed copy converted no reservation")

	// The unwinding teardown hands the persisted name to the cleanup
	// ownership - the retainer can now see it - and the queue deletes
	// the file with the ledger balanced.
	txn.RetainUnrecordedS3Objects([]string{nameA},
		"test: the record died at its own batch copy")
	require.Equal(t, []string{nameA}, txn.pendingObjectCleanupNames())
	_, liveA, inFlightA = ledger()
	require.Equal(t, int64(1), liveA, "the retake converted the reservation into owned membership")
	require.Zero(t, inFlightA)
	txn.Lock()
	txn.handoffPendingObjectCleanup()
	txn.Unlock()
	require.Equal(t, []string{nameA}, issue25590QueuedNames(e))
	require.NoError(t, e.deleteQueuedUnpublishedPendingOnce(ctx))
	issue25590RequireGone(t, failFS.FileService, nameA,
		"the copy-failed record's object deleted by the queue once no record owns it")
	w1.Close()

	// Leg B: the choke refuses AFTER the copy prepared. The unhooked
	// writer (a producer built without the workspace owner surface, as
	// pre-fix operators constructed theirs) persists without a
	// reservation, so its record's name is FRESH at the choke - the
	// pinned bound refuses it. The prepared copy must release exactly.
	unhooked := colexec.NewCNS3DataWriterForService(
		txn.proc.GetService(), txn.proc.Mp(), failFS, tbl.tableDef, -1, false)
	require.False(t, unhooked.HasUncommittedCapacityHook())
	rows2 := writeRows(21, 22)
	require.NoError(t, unhooked.Write(ctx, rows2))
	rows2.Clean(txn.proc.Mp())
	_, err = unhooked.Sync(ctx)
	require.NoError(t, err)
	require.Len(t, unhooked.PersistedNames(), 1)
	nameB := unhooked.PersistedNames()[0]
	metaB, err := unhooked.SyncAndFillBlockInfoBat(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { metaB.Clean(txn.proc.Mp()) })

	knobSaved := unpublishedPendingAdmissionLimit
	unpublishedPendingAdmissionLimit = 0
	t.Cleanup(func() { unpublishedPendingAdmissionLimit = knobSaved })
	currNB := txn.proc.Mp().CurrNB()
	txn.Lock()
	err = txn.WriteFileLocked(INSERT, 1, 7, 42, "db", "tbl", "", metaB, DNStore{})
	txn.Unlock()
	require.ErrorContains(t, err, "workspace admission paused",
		"the record's choke refuses the fresh name behind the pinned bound")
	require.Equal(t, currNB, txn.proc.Mp().CurrNB(),
		"the refused record released its prepared copy exactly")
	unpublishedPendingAdmissionLimit = knobSaved
	require.NotContains(t, issue25590RecordedSnapshot(txn), nameB)
	// The r5 refusal itself owns the refused fresh name now (issue #25590
	// round-21 review, finding G2): the record choke fail-open-retains the
	// reservation-less name so the refused record's output keeps a
	// reachable cleanup owner exactly like a remote fragment's persisted
	// files.
	require.Equal(t, []string{nameB}, txn.pendingObjectCleanupNames(),
		"the choke's refusal fail-open-retained the fresh name - the round-18 empty-list shape predates the r5 refused-output owner")

	// The producer's own teardown retain stays the idempotent second
	// surface: it appends the same name again, the handoff dedupes at the
	// queue's membership index, and the drain deletes the file exactly once.
	txn.RetainUnrecordedS3Objects([]string{nameB},
		"test: the record was refused at its own choke")
	require.Equal(t, []string{nameB, nameB}, txn.pendingObjectCleanupNames(),
		"the second retain surface appends idempotent ownership records for the same refused name")
	txn.Lock()
	txn.handoffPendingObjectCleanup()
	txn.Unlock()
	require.Equal(t, []string{nameB}, issue25590QueuedNames(e),
		"the handoff deduplicated the twice-retained name to one queue obligation")
	require.NoError(t, e.deleteQueuedUnpublishedPendingOnce(ctx))
	issue25590RequireGone(t, failFS.FileService, nameB,
		"the choke-refused record's unhooked-writer object deleted through the retainer's fail-open membership")
	unhooked.Close()
}

// TestIssue25590TombstoneWriterReserveAndRetainOnUnwind extends the
// direct-write producer coverage (round-20 review, finding 2) to the
// TOMBSTONE writer shape multi_update builds: the capacity hook refuses
// the spill before anything persists behind a saturated bound, and a
// writer unwound between persistence and its record joins the cleanup
// ownership, from where the queue drains the physical tombstone object.
func TestIssue25590TombstoneWriterReserveAndRetainOnUnwind(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)
	defer func() {
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	}()

	ctx := context.Background()
	e := txn.engine
	e.pClient = PushClient{}
	failFS := issue25590SwapProcSharedFS(t, txn)
	e.fs = failFS
	gcPool, err := ants.NewPool(2)
	require.NoError(t, err)
	t.Cleanup(func() { gcPool.Release() })
	require.NoError(t, e.ResetGCWorkerPool(gcPool))

	freshTombstone := func() *colexec.CNS3Writer {
		return colexec.NewCNS3TombstoneWriterForService(
			txn.proc.GetService(), txn.proc.Mp(), failFS,
			types.T_int64.ToType(), -1, txn.SinkerCapacityOption())
	}

	// The binding itself: the option's hook rides on the tombstone writer.
	bound := freshTombstone()
	require.True(t, bound.HasUncommittedCapacityHook(),
		"a tombstone writer built on the workspace's option carries the per-object capacity hook")
	bound.Close()

	writeDeletes := func(pks ...int64) *batch.Batch {
		rowidVec, err := txn.batchAllocNewRowIds(len(pks))
		require.NoError(t, err)
		defer rowidVec.Free(txn.proc.Mp())
		rowids := vector.MustFixedColNoTypeCheck[types.Rowid](rowidVec)
		bat := batch.NewWithSize(2)
		bat.SetAttributes([]string{objectio.PhysicalAddr_Attr, "pk"})
		rowidCol := vector.NewVec(types.T_Rowid.ToType())
		require.NoError(t, vector.AppendFixedList(rowidCol, rowids, nil, txn.proc.Mp()))
		pkCol := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(pkCol, pks, nil, txn.proc.Mp()))
		bat.Vecs[0] = rowidCol
		bat.Vecs[1] = pkCol
		bat.SetRowCount(len(pks))
		return bat
	}

	ledger := func() (queued int, live int64, inFlight int64) {
		e.unpublishedPending.mu.Lock()
		defer e.unpublishedPending.mu.Unlock()
		return len(e.unpublishedPending.names), e.unpublishedPending.live,
			e.unpublishedPending.inFlight
	}

	// Saturation: repeated refused rounds persist nothing and settle their
	// in-flight units exactly.
	knobSaved := unpublishedPendingAdmissionLimit
	unpublishedPendingAdmissionLimit = 0
	t.Cleanup(func() { unpublishedPendingAdmissionLimit = knobSaved })
	for round := 0; round < 3; round++ {
		w := freshTombstone()
		rows := writeDeletes(1, 2, 3)
		require.NoError(t, w.Write(ctx, rows))
		rows.Clean(txn.proc.Mp())
		_, err := w.Sync(ctx)
		require.ErrorContains(t, err, "workspace admission paused",
			"the saturated bound refuses the tombstone object's persistence - round %d", round)
		require.Empty(t, w.PersistedNames(),
			"the refused tombstone writer holds no persisted name - round %d", round)
		require.Empty(t, txn.pendingObjectCleanup,
			"refused rounds leave no orphan behind - round %d", round)
		w.Close()
	}
	_, _, inFlight := ledger()
	require.Zero(t, inFlight,
		"every refused tombstone round released its exact in-flight unit")
	require.Len(t, failFS.persistedSnapshot(), 0)
	unpublishedPendingAdmissionLimit = knobSaved

	// Unwind: the tombstone object persists and its record never lands;
	// the retainer's handoff moves it into the queue, which deletes the
	// physical tombstone file.
	w := freshTombstone()
	rows := writeDeletes(7, 8)
	require.NoError(t, w.Write(ctx, rows))
	rows.Clean(txn.proc.Mp())
	_, err = w.Sync(ctx)
	require.NoError(t, err)
	require.Len(t, w.PersistedNames(), 1)
	orphan := w.PersistedNames()[0]
	txn.RetainUnrecordedS3Objects([]string{orphan},
		"test: the tombstone writer unwound before its metadata record")
	require.Equal(t, []string{orphan}, txn.pendingObjectCleanupNames())
	txn.Lock()
	txn.handoffPendingObjectCleanup()
	txn.Unlock()
	require.Equal(t, []string{orphan}, issue25590QueuedNames(e))
	require.NoError(t, e.deleteQueuedUnpublishedPendingOnce(ctx))
	issue25590RequireGone(t, failFS.FileService, orphan,
		"the unwound tombstone writer's object, deleted by the drain once no record owns it")
	w.Close()
}

// ============================================================================
// Round-21 regressions (pre-PR review findings G1–G4; design r5 §5.3). They
// pin the boundary classification of publication-in-flight retentions (G1),
// the commit-terminal capacity model (G3), the standalone DELETE tombstone
// producer's production binding and teardown (G4), and the remote fragment
// settlement (G2).
// ============================================================================

// issue25590LedgerNow snapshots the engine queue's (queued, live, inFlight)
// triple.
func issue25590LedgerNow(e *Engine) (queued int, live int64, inFlight int64) {
	e.unpublishedPending.mu.Lock()
	defer e.unpublishedPending.mu.Unlock()
	return len(e.unpublishedPending.names), e.unpublishedPending.live,
		e.unpublishedPending.inFlight
}

// issue25590NewRound21TxnForTest is the round-21 fixture: a REAL workspace
// over a REAL queue-enabled engine (mirror of the r20 tombstone test's
// setup), with the lite mock operator widened for the statement boundary
// ceremony IncrStatementID drives.
func issue25590NewRound21TxnForTest(t *testing.T) (*Transaction, *issue25590PostPersistFailFS) {
	t.Helper()
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590BindRound21Txn(t, txn)
	e := txn.engine
	e.pClient = PushClient{}
	failFS := issue25590SwapProcSharedFS(t, txn)
	e.fs = failFS
	gcPool, err := ants.NewPool(2)
	require.NoError(t, err)
	t.Cleanup(func() { gcPool.Release() })
	require.NoError(t, e.ResetGCWorkerPool(gcPool))
	return txn, failFS
}

// issue25590BindRound21Txn completes the lite fixture the way production
// binds a session's process: the process carries the transaction operator,
// so the structural workspace resolution (WorkspaceSinkerCapacityOption)
// the operators perform finds this workspace's hook, and the statement-cer-
// emony callbacks IncrStatementID drives are mocked.
func issue25590BindRound21Txn(t *testing.T, txn *Transaction) {
	t.Helper()
	txn.proc.Base.TxnOperator = txn.op
	if txn.cn_flushed_s3_tombstone_object_stats_list == nil {
		txn.cn_flushed_s3_tombstone_object_stats_list = new(sync.Map)
	}
	if mockOp, ok := txn.op.(*mock_frontend.MockTxnOperator); ok {
		mockOp.EXPECT().EnterIncrStmt().AnyTimes()
		mockOp.EXPECT().ExitIncrStmt().AnyTimes()
		mockOp.EXPECT().EnterRollbackStmt().AnyTimes()
		mockOp.EXPECT().ExitRollbackStmt().AnyTimes()
	}
}

// issue25590Round21TombstoneStatsBatch builds the merge-delete consumer's
// record batch for one object: [object_stats, pk] — exactly the layout the
// txnTable.Delete ObjectMeta_ObjectStats record path takes.
func issue25590Round21TombstoneStatsBatch(
	t *testing.T, mp *mpool.MPool, stats objectio.ObjectStats,
) *batch.Batch {
	t.Helper()
	bat := batch.NewWithSize(2)
	bat.SetAttributes([]string{catalog.ObjectMeta_ObjectStats, "pk"})
	statsVec := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(statsVec, stats.Marshal(), false, mp))
	pkVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(pkVec, int64(1), false, mp))
	bat.Vecs[0] = statsVec
	bat.Vecs[1] = pkVec
	bat.SetRowCount(1)
	return bat
}

// issue25590Round21PushTombstone persists one REAL tombstone object through
// the production writer class and binding (WorkspaceSinkerCapacityOption is
// the exact resolution insert/multi_update/deletion operators perform) and
// returns the persisted name plus the writer's REAL stats.
func issue25590Round21PushTombstone(
	t *testing.T,
	txn *Transaction,
	failFS *issue25590PostPersistFailFS,
) (name string, stats objectio.ObjectStats, w *colexec.CNS3Writer) {
	t.Helper()
	name, stats, w, err := issue25590Round22PushTombstoneResult(t, txn, failFS)
	require.NoError(t, err)
	return name, stats, w
}

// issue25590Round22PushTombstoneResult is the error-carrying core of the
// push helper (issue #25590 round-22 review): a fragment saturated at its
// OWN admission bound refuses the writer's spill through the same
// reservation surface, and the sustained two-engine rounds must observe
// that refusal instead of treating it as a fixture failure.
func issue25590Round22PushTombstoneResult(
	t *testing.T,
	txn *Transaction,
	failFS *issue25590PostPersistFailFS,
) (name string, stats objectio.ObjectStats, w *colexec.CNS3Writer, err error) {
	t.Helper()
	ctx := context.Background()
	wOpts := make([]ioutil.SinkerOption, 0, 1)
	if opt := colexec.WorkspaceSinkerCapacityOption(txn.proc); opt != nil {
		wOpts = append(wOpts, opt)
	}
	w = colexec.NewCNS3TombstoneWriterForService(
		txn.proc.GetService(), txn.proc.Mp(), failFS,
		types.T_int64.ToType(), -1, wOpts...)
	rowidVec, err := txn.batchAllocNewRowIds(2)
	if err != nil {
		return "", stats, w, err
	}
	defer rowidVec.Free(txn.proc.Mp())
	rowids := vector.MustFixedColNoTypeCheck[types.Rowid](rowidVec)
	bat := batch.NewWithSize(2)
	bat.SetAttributes([]string{objectio.PhysicalAddr_Attr, "pk"})
	rowidCol := vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixedList(rowidCol, rowids, nil, txn.proc.Mp()))
	pkCol := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(pkCol, []int64{1, 2}, nil, txn.proc.Mp()))
	bat.Vecs[0] = rowidCol
	bat.Vecs[1] = pkCol
	bat.SetRowCount(2)
	if err = w.Write(ctx, bat); err != nil {
		bat.Clean(txn.proc.Mp())
		return "", stats, w, err
	}
	bat.Clean(txn.proc.Mp())
	statsList, err := w.Sync(ctx)
	if err != nil {
		return "", stats, w, err
	}
	require.Len(t, w.PersistedNames(), 1)
	require.Len(t, statsList, 1)
	return w.PersistedNames()[0], statsList[0], w, nil
}

// TestIssue25590InflightPublicationSurvivesDrain pins the round-21 review's
// P1 finding (G1): a producer teardown that completes BEFORE its downstream
// recording consumer — the reachable interleaving pipeline/types.go
// cleanupSenderReceiverPipeline produces — must not hand its in-flight
// publication to the engine's deletion queue inside the recording statement,
// and once the record lands the name belongs to the record's lifecycle: the
// next boundary's recorded-membership recheck keeps it OUT of the queue,
// the commit publishes it, nothing deletes the physical object, and the
// ledger retires it exactly once.
func TestIssue25590InflightPublicationSurvivesDrain(t *testing.T) {
	txn, failFS := issue25590NewRound21TxnForTest(t)
	ctx := context.Background()
	e := txn.engine

	// The recording statement begins, exactly like a user statement.
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false))
	txn.UpdateSnapshotWriteOffset()

	// PAUSED METADATA RECORDING: the producer persisted its tombstone
	// object; its downstream consumer has NOT recorded yet.
	name, stats, w := issue25590Round21PushTombstone(t, txn, failFS)

	// COMPLETE PRODUCER RESET — the production teardown contract (insert's
	// Reset/Free and the multi_update delegate's free): hand the writer's
	// unrecorded names to the workspace retainer, then close.
	txn.RetainUnrecordedS3Objects([]string{name},
		"producer teardown raced ahead of its recording consumer (issue #25590 round-21 review)")
	require.Equal(t, []string{name}, txn.pendingObjectCleanupNames())
	w.Close()

	// A MID-STATEMENT dump boundary (the offset>=0 premise of the round-14
	// pre-gate transfer) runs inside the SAME statement epoch and must not
	// move the in-flight publication to the queue.
	txn.Lock()
	require.NoError(t, txn.dumpBatchLocked(ctx, 0))
	txn.Unlock()
	require.Empty(t, issue25590QueuedNames(e),
		"r5/G1: the in-flight publication stays transaction-owned while its recording consumer can still run")
	require.Equal(t, []string{name}, txn.pendingObjectCleanupNames())

	// The mid-statement handoff must also hand over OLDER-epoch work, so a
	// deferred name cannot strand forever: a definite-orphan entry stamped
	// with an older epoch moves at the same boundary.
	issue25590WriteRawFile(t, failFS.FileService, "issue25590-inflight-older.local")
	txn.Lock()
	txn.pendingObjectCleanup = append(txn.pendingObjectCleanup,
		pendingObjectCleanupItem{name: "issue25590-inflight-older.local"})
	txn.dumpBatchLocked(ctx, 0)
	txn.Unlock()
	require.Equal(t, []string{"issue25590-inflight-older.local"},
		issue25590QueuedNames(e),
		"r5/G1: definite orphans and older-epoch entries still transfer at the mid-statement dump")
	require.NoError(t, e.deleteQueuedUnpublishedPendingOnce(ctx))
	require.Empty(t, issue25590QueuedNames(e),
		"the older-epoch entry drained at the mid-statement boundary")
	require.Equal(t, []string{name}, txn.pendingObjectCleanupNames())

	// RESUME RECORDING: the consumer's record lands inside the same
	// statement — the real record body admits the object and records the
	// metadata entry.
	bat := issue25590Round21TombstoneStatsBatch(t, txn.proc.Mp(), stats)
	require.NoError(t, txn.writeFileWithAutoIncrEpoch(DELETE, 1, issue25590DBID,
		issue25590TableID, "db", "tbl", name, bat,
		metadata.TNService{ServiceID: "issue25590-dn"}, 0))
	require.Contains(t, failFS.persistedSnapshot(), name)

	// The statement ends. The NEXT boundary's handoff rechecks recorded
	// membership: a name whose record landed stays with its record's
	// lifecycle and never enters the deletion queue.
	txn.EndStatement()
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false))
	txn.UpdateSnapshotWriteOffset()
	require.Empty(t, issue25590QueuedNames(e),
		"r5/G1: the recorded name never re-enters the deletion queue through the pending handoff")
	require.Empty(t, txn.pendingObjectCleanupNames(),
		"r5/G1: the boundary classified the recorded name — nothing of it stays transaction-owned")
	queued, live, inFlight := issue25590LedgerNow(e)
	require.Equal(t, int64(1), live,
		"the recorded object holds exactly one owned ledger member")
	require.Zero(t, queued)
	require.Zero(t, inFlight)

	// The drain runs across everything: nothing is queued, the object is
	// safe.
	require.NoError(t, e.deleteQueuedUnpublishedPendingOnce(ctx))
	require.Len(t, failFS.persistedSnapshot(), 1)
	require.Equal(t, name, failFS.persistedSnapshot()[0],
		"the in-flight publication's physical object survives the drain after its record landed")

	// The commit classifies SUCCESS: the recorded name releases as
	// published, the physical object stays, and the drain has nothing.
	txn.EndStatement()
	txn.FinalizeCommit(ctx)
	queued, live, inFlight = issue25590LedgerNow(e)
	require.Zero(t, queued)
	require.Zero(t, live)
	require.Zero(t, inFlight)
	require.Len(t, failFS.persistedSnapshot(), 1,
		"the published object stays on storage after the successful commit")
	require.NoError(t, e.deleteQueuedUnpublishedPendingOnce(ctx))
	require.Len(t, failFS.persistedSnapshot(), 1,
		"the published object is not a deletion obligation at any drain")
}

// TestIssue25590RecordedBeforeBoundaryKeepsObjectOutofQueue drives the G1
// recorded-recheck at the COMMIT-PREPARE boundary: the handoff runs under
// the commit dump (quiesced) AFTER a record landed, and the queue never
// receives the recorded object; the commit draft itself keeps the capacity
// reserved (G3) and the success classification retires it exactly.
func TestIssue25590RecordedBeforeBoundaryKeepsObjectOutofQueue(t *testing.T) {
	txn, failFS := issue25590NewRound21TxnForTest(t)
	ctx := context.Background()
	e := txn.engine

	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false))
	txn.UpdateSnapshotWriteOffset()

	name, stats, w := issue25590Round21PushTombstone(t, txn, failFS)
	txn.RetainUnrecordedS3Objects([]string{name},
		"test: producer teardown before the same statement's record")
	w.Close()

	// The consumer records INSIDE the statement.
	bat := issue25590Round21TombstoneStatsBatch(t, txn.proc.Mp(), stats)
	require.NoError(t, txn.writeFileWithAutoIncrEpoch(DELETE, 1, issue25590DBID,
		issue25590TableID, "db", "tbl", name, bat,
		metadata.TNService{ServiceID: "issue25590-dn"}, 0))

	// Commit preparation drafts the payload: the quiesced handoff runs
	// first (recorded-recheck), nothing moves to the queue, and — G3 — the
	// recorded names STAY counted behind the bound until the caller
	// classifies the outcome.
	_, err := txn.Commit(ctx)
	require.NoError(t, err)
	require.Empty(t, issue25590QueuedNames(e),
		"r5/G1: the commit-prepare handoff rechecks recorded membership and queues nothing")
	require.Empty(t, txn.pendingObjectCleanupNames())
	_, live, _ := issue25590LedgerNow(e)
	require.Equal(t, int64(1), live,
		"r5/G3: commit preparation parked, the recorded object's capacity stays counted")

	txn.EndStatement()
	txn.FinalizeCommit(ctx)
	_, live, inFlight := issue25590LedgerNow(e)
	require.Zero(t, live)
	require.Zero(t, inFlight)
	require.Equal(t, name, failFS.persistedSnapshot()[0],
		"the published object stays on storage through the successful commit")
}

// TestIssue25590UnrecordedRetainedNameDrainsAtBoundary is the G1
// counterexample guard: a retained name that stays UNRECORDED past its
// recording statement IS a definite orphan at the next boundary — the
// in-flight guard may not become a leak (the queue receives it and the
// drain deletes the physical file, the ledger balanced).
func TestIssue25590UnrecordedRetainedNameDrainsAtBoundary(t *testing.T) {
	txn, failFS := issue25590NewRound21TxnForTest(t)
	ctx := context.Background()
	e := txn.engine

	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false))
	txn.UpdateSnapshotWriteOffset()

	name, _, w := issue25590Round21PushTombstone(t, txn, failFS)
	txn.RetainUnrecordedS3Objects([]string{name},
		"test: producer teardown, no record ever lands")
	w.Close()
	txn.EndStatement()

	// The recording statement is quiesced: the next boundary moves the name
	// to the engine's queue.
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false))
	txn.UpdateSnapshotWriteOffset()
	require.Equal(t, []string{name}, issue25590QueuedNames(e),
		"a definite orphan of a quiesced statement transfers at the boundary")
	require.Empty(t, txn.pendingObjectCleanupNames())

	// The drain deletes the physical file and the ledger empties exactly.
	require.NoError(t, e.deleteQueuedUnpublishedPendingOnce(ctx))
	issue25590RequireGone(t, failFS.FileService, name,
		"the definite orphan's file, deleted by the drain after the boundary classified it")
	queued, live, inFlight := issue25590LedgerNow(e)
	require.Zero(t, queued)
	require.Zero(t, live)
	require.Zero(t, inFlight)
}

// TestIssue25590ParkedCommitKeepsUnitAndFailureTransfersOnce pins the
// round-21 review's G3 finding at the client-visible seam the reviewer
// cited: a prepared commit parks before its outcome is classified — and
// the parked unit STAYS counted (a competing transaction on the same engine
// is refused through the whole prepare/RPC window), the definite failure
// transfers owned→queued without increasing the total, the success
// classification retires the recorded names exactly, and the unknown
// classification retires WITHOUT deleting.
func TestIssue25590ParkedCommitKeepsUnitAndFailureTransfersOnce(t *testing.T) {
	txnA, failFS := issue25590NewRound21TxnForTest(t)
	ctx := context.Background()
	e := txnA.engine
	savedLimit := unpublishedPendingAdmissionLimit
	unpublishedPendingAdmissionLimit = 1
	t.Cleanup(func() { unpublishedPendingAdmissionLimit = savedLimit })

	// Transaction A records one object behind the pinned one-unit bound.
	txnA.StartStatement()
	require.NoError(t, txnA.IncrStatementID(ctx, false))
	txnA.UpdateSnapshotWriteOffset()
	nameA, statsA, wA := issue25590Round21PushTombstone(t, txnA, failFS)
	batA := issue25590Round21TombstoneStatsBatch(t, txnA.proc.Mp(), statsA)
	require.NoError(t, txnA.writeFileWithAutoIncrEpoch(DELETE, 1, issue25590DBID,
		issue25590TableID, "db", "tbl", nameA, batA,
		metadata.TNService{ServiceID: "issue25590-dn"}, 0))
	wA.Close()
	_, live, _ := issue25590LedgerNow(e)
	require.Equal(t, int64(1), live, "the recorded object holds the bound's one unit")

	// COMMIT PARKED: the payload is drafted (workspace.Commit returns the
	// reqs the client still has to drive), the outcome is NOT classified.
	_, err := txnA.Commit(ctx)
	require.NoError(t, err)
	queued, live, inFlight := issue25590LedgerNow(e)
	require.Empty(t, queued)
	require.Equal(t, int64(1), live,
		"r5/G3: the parked commit keeps its recorded name counted through prepare (issue #25590 round-21 review)")
	require.Zero(t, inFlight)

	// A competing transaction on the SAME engine cannot consume the parked
	// unit: its producer's per-object reservation is refused behind the
	// bound for the whole prepare window.
	require.ErrorContains(t, e.reserveUnpublishedPendingObject("issue25590-competing.dump"),
		"workspace admission paused",
		"r5/G3: the competitor is refused while the parked commit holds its unit")
	queued, live, inFlight = issue25590LedgerNow(e)
	require.Zero(t, queued)
	require.Equal(t, int64(1), live)
	require.Zero(t, inFlight, "the refused reservation consumed nothing")

	// The parked commit's DEFINITE FAILURE: the rollback terminal transfers
	// the recorded name owned→queued exactly — the retained population
	// never exceeds the bound.
	txnA.EndStatement()
	require.NoError(t, txnA.Rollback(ctx))
	queued, live, inFlight = issue25590LedgerNow(e)
	require.Equal(t, 1, queued, "the definite failure transferred its obligation to the queue")
	require.Zero(t, live)
	require.Zero(t, inFlight)
	require.NoError(t, e.deleteQueuedUnpublishedPendingOnce(ctx))
	issue25590RequireGone(t, failFS.FileService, nameA,
		"the rolled-back commit's recorded object, deleted by the drain after the transfer")
	queued, live, inFlight = issue25590LedgerNow(e)
	require.Zero(t, queued)
	require.Zero(t, live)
	require.Zero(t, inFlight)
	// Post-failure the competitor admits freely: the total the bound counts
	// returned below the limit exactly once.
	require.NoError(t, e.reserveUnpublishedPendingObject("issue25590-competing.dump"))
	e.abandonUnpublishedPendingReservations([]string{"issue25590-competing.dump"})

	// The SUCCESS classification leg: park, then FinalizeCommit retires the
	// unit as published with no deletion anywhere.
	txnC := newTransactionWithActivePKTableForTest(t, "pk")
	txnC.engine = e
	issue25590BindRound21Txn(t, txnC)
	nameC, statsC, wC := issue25590Round21PushTombstone(t, txnC, failFS)
	batC := issue25590Round21TombstoneStatsBatch(t, txnC.proc.Mp(), statsC)
	require.NoError(t, txnC.writeFileWithAutoIncrEpoch(DELETE, 1, issue25590DBID,
		issue25590TableID, "db", "tbl", nameC, batC,
		metadata.TNService{ServiceID: "issue25590-dn"}, 0))
	wC.Close()
	_, err = txnC.Commit(ctx)
	require.NoError(t, err)
	_, live, _ = issue25590LedgerNow(e)
	require.Equal(t, int64(1), live,
		"r5/G3: the second park also keeps its unit counted (only one unit is live: the earlier leg retired)")
	txnC.FinalizeCommit(ctx)
	queued, live, inFlight = issue25590LedgerNow(e)
	require.Zero(t, queued)
	require.Zero(t, live)
	require.Zero(t, inFlight)
	require.Contains(t, failFS.persistedSnapshot(), nameC,
		"the successful commit publishes its object — no deletion obligation anywhere")
	require.NoError(t, e.deleteQueuedUnpublishedPendingOnce(ctx))
	require.Contains(t, failFS.persistedSnapshot(), nameC)

	// The UNKNOWN classification leg: park, then the unknown-result
	// terminal retires the unit WITHOUT deleting (the commit may have
	// reached TN).
	txnD := newTransactionWithActivePKTableForTest(t, "pk")
	txnD.engine = e
	issue25590BindRound21Txn(t, txnD)
	nameD, statsD, wD := issue25590Round21PushTombstone(t, txnD, failFS)
	batD := issue25590Round21TombstoneStatsBatch(t, txnD.proc.Mp(), statsD)
	require.NoError(t, txnD.writeFileWithAutoIncrEpoch(DELETE, 1, issue25590DBID,
		issue25590TableID, "db", "tbl", nameD, batD,
		metadata.TNService{ServiceID: "issue25590-dn"}, 0))
	wD.Close()
	_, err = txnD.Commit(ctx)
	require.NoError(t, err)
	txnD.FinalizeCommitWithUnknownResult(ctx)
	queued, live, inFlight = issue25590LedgerNow(e)
	require.Zero(t, queued)
	require.Zero(t, live)
	require.Zero(t, inFlight)
	require.Contains(t, failFS.persistedSnapshot(), nameD,
		"the unknown-result retire deleted nothing — the ambiguity doctrine")
}

// TestIssue25590StandaloneDeleteFlushProducerBindsReservesAndHandsOver
// pins the round-21 review's G4 finding through the REAL deletion operator:
// the production flush path (compileDelete's tombstone writer behind the
// pinned threshold) persists BEHIND its own per-object reservation, the
// operator's teardown hands the unrecorded names to the workspace retainer
// (the boundary moves them and the drain deletes physically), and a
// saturated bound refuses the flush's own spill through the production
// binding — the exact residues the manually-optioned r20 tombstone test
// could not see.
func TestIssue25590StandaloneDeleteFlushProducerBindsReservesAndHandsOver(t *testing.T) {
	txn, failFS := issue25590NewRound21TxnForTest(t)
	ctx := context.Background()
	e := txn.engine
	savedLimit := unpublishedPendingAdmissionLimit
	unpublishedPendingAdmissionLimit = 2
	t.Cleanup(func() { unpublishedPendingAdmissionLimit = savedLimit })
	colexecdeletion.SetCNFlushDeletesThreshold(1)
	t.Cleanup(func() { colexecdeletion.SetCNFlushDeletesThreshold(5) })

	newDeletionArg := func() *colexecdeletion.Deletion {
		arg := colexecdeletion.NewArgument()
		arg.RemoteDelete = true
		arg.IBucket = 0
		arg.Nbucket = 1
		arg.DeleteCtx = &colexecdeletion.DeleteCtx{
			CanTruncate:     false,
			RowIdIdx:        0,
			Ref:             &pbplan.ObjectRef{},
			AddAffectedRows: true,
			PrimaryKeyIdx:   1,
		}
		require.NoError(t, arg.Prepare(txn.proc))
		return arg
	}

	segmentID := objectio.NewSegmentid()
	blkID0 := objectio.NewBlockid(segmentID, 0, 0)
	bigDeleteBatch := func(n int) *batch.Batch {
		rowidCol := vector.NewVec(types.T_Rowid.ToType())
		pkCol := vector.NewVec(types.T_int64.ToType())
		rowids := make([]types.Rowid, 0, n)
		pks := make([]int64, 0, n)
		for i := 0; i < n; i++ {
			rowids = append(rowids, types.NewRowid(blkID0, uint32(i)))
			pks = append(pks, int64(i))
		}
		require.NoError(t, vector.AppendFixedList(rowidCol, rowids, nil, txn.proc.Mp()))
		require.NoError(t, vector.AppendFixedList(pkCol, pks, nil, txn.proc.Mp()))
		bat := batch.NewWithSize(2)
		bat.SetAttributes([]string{catalog.Row_ID, "pk"})
		bat.Vecs[0] = rowidCol
		bat.Vecs[1] = pkCol
		bat.SetRowCount(n)
		return bat
	}

	// LEG A — the production flush persists behind its own reservation:
	// the production constructor's writer carries the workspace's capacity
	// option, so the spilled tombstone object holds one in-flight unit on
	// the engine ledger before its record could ever land.
	argA := newDeletionArg()
	require.NoError(t, argA.SplitBatch(txn.proc, bigDeleteBatch(70000), argA.OpAnalyzer))
	require.Len(t, failFS.persistedSnapshot(), 1,
		"the pinned threshold made the real flush persist its tombstone object")
	tombstoneA := failFS.persistedSnapshot()[0]
	_, live, inFlight := issue25590LedgerNow(e)
	require.Equal(t, int64(1), inFlight,
		"r5/G4 (RED on r20): the production deletion writer reserved its object's in-flight unit before the persist")
	require.Zero(t, live, "the flush's consumer has not recorded — no owned member yet")

	// LEG B — the persist-then-no-record path: the real operator teardown
	// (Free) hands the collected names to the workspace retainer, the next
	// boundary classes them definite (the recording statement quiesced with
	// no record), and the drain deletes the physical tombstone file.
	argA.Free(txn.proc, true,
		moerr.NewInternalErrorNoCtx("statement cancelled"))
	require.Len(t, txn.pendingObjectCleanupNames(), 1,
		"r5/G4 (RED on r20): the real teardown handed the flush's unrecorded names to the retainer")
	require.Equal(t, tombstoneA, txn.pendingObjectCleanupNames()[0])
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false))
	txn.UpdateSnapshotWriteOffset()
	require.Equal(t, []string{tombstoneA}, issue25590QueuedNames(e),
		"the boundary moved the unrecorded flush name to the engine queue")
	require.NoError(t, e.deleteQueuedUnpublishedPendingOnce(ctx))
	issue25590RequireGone(t, failFS.FileService, tombstoneA,
		"the unrecorded tombstone object, deleted physically by the drain")
	queued, live, inFlight := issue25590LedgerNow(e)
	require.Zero(t, queued)
	require.Zero(t, live)
	require.Zero(t, inFlight)

	// LEG C — saturation refuses the flush's own spill THROUGH the
	// production binding: with the bound at one unit and the first flush's
	// reservation outstanding, the second spill's reserve is refused before
	// the object goes durable and the statement fails with nothing
	// half-persisted.
	unpublishedPendingAdmissionLimit = 1
	argC1 := newDeletionArg()
	require.NoError(t, argC1.SplitBatch(txn.proc, bigDeleteBatch(70000), argC1.OpAnalyzer))
	argC1.Free(txn.proc, false, nil)
	require.Len(t, txn.pendingObjectCleanupNames(), 1,
		"the first saturated-leg flush collected its persisted name for the teardown")
	require.Equal(t, failFS.persistedSnapshot()[len(failFS.persistedSnapshot())-1],
		txn.pendingObjectCleanupNames()[0],
		"the collected name is exactly the saturated flush's newest persisted object")
	argC2 := newDeletionArg()
	err := argC2.SplitBatch(txn.proc, bigDeleteBatch(70000), argC2.OpAnalyzer)
	require.ErrorContains(t, err, "workspace admission paused",
		"r5/G4 (RED on r20): the production deletion writer's spill is refused at a saturated bound — the missing binding was exactly this leg's hole")
	persistedAtRefusal := len(failFS.persistedSnapshot())
	require.Len(t, failFS.persistedSnapshot(), persistedAtRefusal,
		"the refused spill persisted nothing new")
	argC2.Free(txn.proc, true, err)
	// The teardown hands over the first (never-recorded) object; the
	// refused one contributes nothing; the boundary + drain collect the
	// physical file and the ledger empties exactly.
	names := append([]string(nil), txn.pendingObjectCleanupNames()...)
	txn.Lock()
	txn.handoffPendingObjectCleanupLocked(true)
	txn.Unlock()
	require.ElementsMatch(t, names, issue25590QueuedNames(e),
		"the teardown's handoff moved exactly the collected names")
	require.NoError(t, e.deleteQueuedUnpublishedPendingOnce(ctx))
	queued, live, inFlight = issue25590LedgerNow(e)
	require.Zero(t, queued)
	require.Zero(t, live)
	require.Zero(t, inFlight)
}

// TestIssue25590RemoteFragmentSettlement pins the round-21 review's G2
// finding across TWO REAL engines sharing one file backend — the fragment
// engine and the coordinator engine: a clean fragment settles TRANSFERRED
// (its reservations and retained names release with NOTHING deleted, and
// the coordinator's record is the acknowledgment), a failed or cancelled
// fragment settles through the ordinary rollback terminal which keeps a
// reachable local cleanup owner (the fragment engine's own queue), and a
// coordinator-side refusal of a clean fragment's names fail-open-retains
// them on the recording engine so the files never lose their owner. The
// pre-r5 tree never settles the mirror workspace at all: its reservations
// and owned names stay on the fragment engine's ledger forever.
func TestIssue25590RemoteFragmentSettlement(t *testing.T) {
	ctx := context.Background()
	fragmentTxn, failFS := issue25590NewRound21TxnForTest(t)
	engineR := fragmentTxn.engine

	// The fragment's writer persists behind the fragment engine's own
	// reservation surface (the production binding its operators perform),
	// then the operator teardown retains — exactly what Insert.Reset /
	// the delegate's free leave behind on a remote run.
	nameR, statsR, wR := issue25590Round21PushTombstone(t, fragmentTxn, failFS)
	fragmentTxn.RetainUnrecordedS3Objects([]string{nameR},
		"remote fragment teardown before the coordinator's record (issue #25590 round-21 review)")
	wR.Close()
	_, liveR, inFlightR := issue25590LedgerNow(engineR)
	require.Equal(t, int64(1), liveR,
		"the fragment's teardown retained its persisted name as owned on the fragment engine")
	require.Zero(t, inFlightR, "the teardown converted the reservation into ownership")

	// The clean run settles TRANSFERRED under the r5 protocol.
	require.NoError(t, fragmentTxn.FinalizeRemoteFragmentWorkspace(ctx, true))
	queuedR, liveR, inFlightR := issue25590LedgerNow(engineR)
	require.Zero(t, queuedR, "the transferred settlement enqueues nothing on the fragment engine")
	require.Zero(t, liveR,
		"r5/G2 (RED on r20): the transferred settlement retires the fragment's owned membership")
	require.Zero(t, inFlightR,
		"r5/G2 (RED on r20): the transferred settlement retires the fragment's reservation")
	require.Contains(t, failFS.persistedSnapshot(), nameR,
		"the transferred settlement deleted nothing — the coordinator owns the fate")

	// The coordinator acknowledges by recording: its own engine's ledger
	// carries the object, its success classification retires it.
	coordinatorTxn, _ := issue25590NewRound21TxnForTest(t)
	engineC := coordinatorTxn.engine
	engineC.fs = failFS
	coordinatorTxn.StartStatement()
	require.NoError(t, coordinatorTxn.IncrStatementID(ctx, false))
	coordinatorTxn.UpdateSnapshotWriteOffset()
	batR := issue25590Round21TombstoneStatsBatch(t, coordinatorTxn.proc.Mp(), statsR)
	require.NoError(t, coordinatorTxn.writeFileWithAutoIncrEpoch(DELETE, 1, issue25590DBID,
		issue25590TableID, "db", "tbl", nameR, batR,
		metadata.TNService{ServiceID: "issue25590-dn"}, 0))
	_, liveC, _ := issue25590LedgerNow(engineC)
	require.Equal(t, int64(1), liveC,
		"the recording side's acknowledgment admissions carry the transferred object")
	coordinatorTxn.EndStatement()
	coordinatorTxn.FinalizeCommit(ctx)
	_, liveC, inFlightC := issue25590LedgerNow(engineC)
	require.Zero(t, liveC)
	require.Zero(t, inFlightC)
	require.Contains(t, failFS.persistedSnapshot(), nameR,
		"the published remote-produced object survives the coordinator's commit")

	// THE REFUSED ACKNOWLEDGMENT: a second fragment run releases (settles
	// transferred) while the recording side refuses the record at a
	// saturated bound — the r5 refusal fail-open-retain keeps the refused
	// reservation-less names reachable on the RECORDING engine, and the
	// failed statement drains them physically. r6 note (round-22 review):
	// on the r6 tree production refusals move to the transport client's
	// INTAKE — before the fragment can release — pinned by the intake test
	// below; this leg keeps driving the reservation-less class straight at
	// the choke because that class survives as the legacy/unhooked-writer
	// compat net, and its last-owner retain stays exactly this doctrine.
	fragmentTxn2 := newTransactionWithActivePKTableForTest(t, "pk")
	fragmentTxn2.engine = engineR
	issue25590BindRound21Txn(t, fragmentTxn2)
	nameR2, statsR2, wR2 := issue25590Round21PushTombstone(t, fragmentTxn2, failFS)
	fragmentTxn2.RetainUnrecordedS3Objects([]string{nameR2},
		"remote fragment teardown before the refused record (issue #25590 round-21 review)")
	wR2.Close()
	require.NoError(t, fragmentTxn2.FinalizeRemoteFragmentWorkspace(ctx, true))
	_, liveR2, inFlightR2 := issue25590LedgerNow(engineR)
	require.Zero(t, liveR2)
	require.Zero(t, inFlightR2)

	coordinatorTxn2 := newTransactionWithActivePKTableForTest(t, "pk")
	coordinatorTxn2.engine = engineC
	issue25590BindRound21Txn(t, coordinatorTxn2)
	savedLimit := unpublishedPendingAdmissionLimit
	unpublishedPendingAdmissionLimit = 1
	t.Cleanup(func() { unpublishedPendingAdmissionLimit = savedLimit })
	// One already-reserved in-flight unit parks the coordinator's bound at
	// one name, so the refused class is reachable deterministically: the
	// fragment's names carry NO local reservation (their reservation sat
	// on the fragment engine), so the record's admission sees them fresh.
	parkedName := "issue25590-refusal-parked.dump"
	issue25590WriteRawFile(t, failFS.FileService, parkedName)
	require.NoError(t, engineC.reserveUnpublishedPendingObject(parkedName))
	batR2 := issue25590Round21TombstoneStatsBatch(t, coordinatorTxn2.proc.Mp(), statsR2)
	err := coordinatorTxn2.writeFileWithAutoIncrEpoch(DELETE, 1, issue25590DBID,
		issue25590TableID, "db", "tbl", nameR2, batR2,
		metadata.TNService{ServiceID: "issue25590-dn"}, 0)
	require.ErrorContains(t, err, "workspace admission paused",
		"the saturated recording side refuses the fragment's record")
	require.Equal(t, []string{nameR2}, coordinatorTxn2.pendingObjectCleanupNames(),
		"r5/G2 (RED on r20): the refused record's reservation-less names stay reachable through the recording side's fail-open retain")
	// The failed statement's classification moves them and the drain deletes
	// the physical file — nothing leaks ownerless anywhere: the fragment
	// already released, and the recording engine owns the deletes.
	coordinatorTxn2.Lock()
	coordinatorTxn2.handoffPendingObjectCleanupLocked(true)
	coordinatorTxn2.Unlock()
	require.NoError(t, coordinatorTxn2.Rollback(ctx))
	require.NoError(t, engineC.deleteQueuedUnpublishedPendingOnce(ctx))
	issue25590RequireGone(t, failFS.FileService, nameR2,
		"the refused record's files, deleted by the recording engine's drain — the fragment already released its reservation")
	// The parked in-flight unit was never a queue obligation: its
	// reservation retires with the abandoned unit, its raw file is
	// fixture-only scaffolding (the reservation never recorded a producer's
	// object), and the recording engine's ledger empties exactly.
	engineC.abandonUnpublishedPendingReservations([]string{parkedName})
	queuedC, liveC, inFlightC := issue25590LedgerNow(engineC)
	require.Zero(t, queuedC)
	require.Zero(t, liveC)
	require.Zero(t, inFlightC,
		"the refused-acknowledgment leg leaves the recording engine exactly empty")

	// THE FAILED-RUN settlement: fragment 3 persists + retains; the run
	// FAILED (or was cancelled) — the settlement keeps a reachable cleanup
	// owner on the fragment engine.
	fragmentTxn3 := newTransactionWithActivePKTableForTest(t, "pk")
	fragmentTxn3.engine = engineR
	issue25590BindRound21Txn(t, fragmentTxn3)
	nameR3, _, wR3 := issue25590Round21PushTombstone(t, fragmentTxn3, failFS)
	fragmentTxn3.RetainUnrecordedS3Objects([]string{nameR3}, "remote fragment teardown on a failed run")
	wR3.Close()
	require.NoError(t, fragmentTxn3.FinalizeRemoteFragmentWorkspace(ctx, false))
	require.Equal(t, []string{nameR3}, issue25590QueuedNames(engineR),
		"the failed run's settlement retained the names on the fragment engine's own queue")
	require.NoError(t, engineR.deleteQueuedUnpublishedPendingOnce(ctx))
	issue25590RequireGone(t, failFS.FileService, nameR3,
		"the failed fragment's object, deleted by the fragment engine's drain — a reachable owner everywhere")
	_, liveR3, inFlightR3 := issue25590LedgerNow(engineR)
	require.Zero(t, liveR3)
	require.Zero(t, inFlightR3)
}

// TestIssue25590RemoteIntakeAdoptsBeforeRelease pins the round-22 review's
// finding H1 across two REAL engines sharing one file backend: the
// coordinator's transport client ADOPTS every relayed S3 object name at its
// intake — before forwarding, before the ack — through the SAME per-object
// admission reservation the writers use, so an enumerable cleanup owner
// exists on the recording side strictly before the fragment's own terminal
// settlement can release anything. The legs cover the review's exact end
// classes: the adopted object's survival through the recording
// transaction's commit (physical survival plus balanced accounting),
// cancellation between adoption and any record (the recording side's
// terminal drain-adopt keeps the released file enumerated and retires it
// physically, with no ownerless window), a record whose own PREPARATION
// fails after adoption (the reservation is the owner; the terminal deletes
// the file physically), and the saturated recording side refusing the
// intake itself so nothing books and the fragment settles through the
// failure terminal (finding H2's intake gate).
func TestIssue25590RemoteIntakeAdoptsBeforeRelease(t *testing.T) {
	ctx := context.Background()
	colexec.NewServer("")
	fragmentTxn, failFS := issue25590NewRound21TxnForTest(t)
	engineR := fragmentTxn.engine
	coordinatorTxn, _ := issue25590NewRound21TxnForTest(t)
	engineC := coordinatorTxn.engine
	engineC.fs = failFS

	// LEG A — ADOPTION PRECEDES THE RELEASE, AND THE ADOPTED OBJECT
	// SURVIVES THE RECORDING TRANSACTION'S COMMIT. The fragment's writer
	// persists behind the fragment engine's reservation (the production
	// binding), the transport client adopts the relayed name at its
	// intake, and only then does the fragment's run transfer - so when the
	// coordinator records the name and commits, the published object
	// survives with every ledger balanced.
	nameA, statsA, wA := issue25590Round21PushTombstone(t, fragmentTxn, failFS)
	require.NoError(t, coordinatorTxn.AdoptRemoteTransferredObjectNames([]string{nameA}),
		"the coordinator's intake adoption books the relayed name")
	_, liveC, inFlightC := issue25590LedgerNow(engineC)
	require.Equal(t, int64(1), inFlightC,
		"r6/H1: the recording side holds the adopted name's in-flight unit BEFORE the fragment can release")
	require.Zero(t, liveC)
	fragmentTxn.RetainUnrecordedS3Objects([]string{nameA},
		"remote fragment teardown after the coordinator's intake adoption (issue #25590 round-22 review)")
	wA.Close()
	require.NoError(t, fragmentTxn.FinalizeRemoteFragmentWorkspace(ctx, true))
	_, liveR, inFlightR := issue25590LedgerNow(engineR)
	require.Zero(t, liveR, "the transferred settlement retired the fragment's ownership")
	require.Zero(t, inFlightR)
	require.Contains(t, failFS.persistedSnapshot(), nameA,
		"nothing deleted on transfer - the recording side owns the fate now")

	// The coordinator's record converts the adopted reservation and its
	// commit publishes the object: the ADOPTED OBJECT SURVIVES.
	coordinatorTxn.StartStatement()
	require.NoError(t, coordinatorTxn.IncrStatementID(ctx, false))
	coordinatorTxn.UpdateSnapshotWriteOffset()
	batA := issue25590Round21TombstoneStatsBatch(t, coordinatorTxn.proc.Mp(), statsA)
	require.NoError(t, coordinatorTxn.writeFileWithAutoIncrEpoch(DELETE, 1, issue25590DBID,
		issue25590TableID, "db", "tbl", nameA, batA,
		metadata.TNService{ServiceID: "issue25590-dn"}, 0))
	_, liveC2, inFlightC2 := issue25590LedgerNow(engineC)
	require.Equal(t, int64(1), liveC2,
		"the record converted the adopted reservation into recorded ownership")
	require.Zero(t, inFlightC2)
	coordinatorTxn.EndStatement()
	coordinatorTxn.FinalizeCommit(ctx)
	_, liveC3, inFlightC3 := issue25590LedgerNow(engineC)
	require.Zero(t, liveC3)
	require.Zero(t, inFlightC3,
		"balanced accounting: the recording ledger retires the published adoption exactly")
	require.Contains(t, failFS.persistedSnapshot(), nameA,
		"r6/H1: the adopted object SURVIVES the recording transaction's commit")

	// LEG B — CANCELLATION BETWEEN ADOPTION AND ANY RECORD. The fragment
	// already released (the r5 protocol's exact residual window), the
	// coordinator's statement is cancelled before its record runs, and
	// the recording side's terminal must still own and retire the file:
	// the round-22 reservation-drain adoption keeps the adopted name
	// enumerated through the rollback, the sink deletes it physically,
	// and no ownerless file survives anywhere.
	fragmentTxn2 := newTransactionWithActivePKTableForTest(t, "pk")
	fragmentTxn2.engine = engineR
	issue25590BindRound21Txn(t, fragmentTxn2)
	nameB, _, wB := issue25590Round21PushTombstone(t, fragmentTxn2, failFS)
	coordinatorTxn2 := newTransactionWithActivePKTableForTest(t, "pk")
	coordinatorTxn2.engine = engineC
	issue25590BindRound21Txn(t, coordinatorTxn2)
	require.NoError(t, coordinatorTxn2.AdoptRemoteTransferredObjectNames([]string{nameB}))
	_, _, inFlightB := issue25590LedgerNow(engineC)
	require.Equal(t, int64(1), inFlightB, "the cancelled leg's intake booked its adoption")
	fragmentTxn2.RetainUnrecordedS3Objects([]string{nameB},
		"remote fragment teardown after adoption, before the coordinator's cancellation")
	wB.Close()
	require.NoError(t, fragmentTxn2.FinalizeRemoteFragmentWorkspace(ctx, true))
	_, liveR2, inFlightR2 := issue25590LedgerNow(engineR)
	require.Zero(t, liveR2)
	require.Zero(t, inFlightR2)
	require.Contains(t, failFS.persistedSnapshot(), nameB,
		"the fragment released without deleting - the adoption owns the file")
	// The statement cancels before any record: the rollback terminal's
	// reservation drain ADOPTS the leftover reservation, the handoff
	// carries it to the recording engine's queue, and the drain retires
	// it physically.
	require.NoError(t, coordinatorTxn2.Rollback(ctx))
	require.Equal(t, []string{nameB}, issue25590QueuedNames(engineC),
		"r6/H1: the cancelled statement's terminal keeps the adopted-but-never-recorded name enumerated on the recording side")
	require.NoError(t, engineC.deleteQueuedUnpublishedPendingOnce(ctx))
	issue25590RequireGone(t, failFS.FileService, nameB,
		"r6/H1: a released fragment plus a cancelled record deletes the physical file through the recording side's own owner - no ownerless object remains")
	queuedC, liveC4, inFlightC4 := issue25590LedgerNow(engineC)
	require.Zero(t, queuedC)
	require.Zero(t, liveC4)
	require.Zero(t, inFlightC4)

	// LEG C — THE RECORD'S OWN PREPARATION FAILS AFTER ADOPTION. The keyed
	// fault refuses the record's staging before its admission can convert
	// the adopted reservation, exactly the round-22 H1a reachability class:
	// the reservation is the owner, the refusing statement fails, and its
	// terminal drains and deletes the file physically.
	fragmentTxn3 := newTransactionWithActivePKTableForTest(t, "pk")
	fragmentTxn3.engine = engineR
	issue25590BindRound21Txn(t, fragmentTxn3)
	nameC, statsC, wC := issue25590Round21PushTombstone(t, fragmentTxn3, failFS)
	coordinatorTxn3 := newTransactionWithActivePKTableForTest(t, "pk")
	coordinatorTxn3.engine = engineC
	issue25590BindRound21Txn(t, coordinatorTxn3)
	require.NoError(t, coordinatorTxn3.AdoptRemoteTransferredObjectNames([]string{nameC}))
	fragmentTxn3.RetainUnrecordedS3Objects([]string{nameC},
		"remote fragment teardown after adoption, before the preparation failure")
	wC.Close()
	require.NoError(t, fragmentTxn3.FinalizeRemoteFragmentWorkspace(ctx, true))
	coordinatorTxn3.StartStatement()
	require.NoError(t, coordinatorTxn3.IncrStatementID(ctx, false))
	coordinatorTxn3.UpdateSnapshotWriteOffset()
	stageDisarm := issue25590ArmKeyedFault(
		t, objectio.FJ_CNWorkspaceRecordStageFailed, 0, "tbl")
	batC := issue25590Round21TombstoneStatsBatch(t, coordinatorTxn3.proc.Mp(), statsC)
	err := coordinatorTxn3.writeFileWithAutoIncrEpoch(DELETE, 1, issue25590DBID,
		issue25590TableID, "db", "tbl", nameC, batC,
		metadata.TNService{ServiceID: "issue25590-dn"}, 0)
	require.ErrorContains(t, err, "fault injection: workspace record staging failed",
		"the record's preparation failed after the intake adoption - before its admission")
	require.Empty(t, coordinatorTxn3.pendingObjectCleanupNames(),
		"the preparation-failure retain skips the reserved name: the adopted reservation is the owner")
	stageDisarm()
	// The failing statement's terminal drains the leftover reservation,
	// hands it to the queue, and the physical file retires through the
	// recording engine's drain.
	require.NoError(t, coordinatorTxn3.Rollback(ctx))
	require.Equal(t, []string{nameC}, issue25590QueuedNames(engineC),
		"r6/H1: the preparation failure's terminal keeps the adopted name enumerated")
	require.NoError(t, engineC.deleteQueuedUnpublishedPendingOnce(ctx))
	issue25590RequireGone(t, failFS.FileService, nameC,
		"r6/H1: the preparation failure deletes the physical file through the recording side's terminal owner")
	queuedC, liveC5, inFlightC5 := issue25590LedgerNow(engineC)
	require.Zero(t, queuedC)
	require.Zero(t, liveC5)
	require.Zero(t, inFlightC5)

	// LEG D — THE SATURATED RECORDING SIDE REFUSES THE INTAKE ITSELF
	// (finding H2's gate): with the recording engine's bound parked at
	// one unit, the transport client's adoption is refused BEFORE anything
	// books, the refusing transport fails the run, and the fragment
	// settles through the FAILURE terminal - its local owner keeps the
	// file reachable and retires it - while the recording engine's ledger
	// never sees the refused name.
	savedLimit := unpublishedPendingAdmissionLimit
	unpublishedPendingAdmissionLimit = 1
	t.Cleanup(func() { unpublishedPendingAdmissionLimit = savedLimit })
	parkedName := "issue25590-r22-intake-parked.dump"
	issue25590WriteRawFile(t, failFS.FileService, parkedName)
	require.NoError(t, engineC.reserveUnpublishedPendingObject(parkedName))
	fragmentTxn4 := newTransactionWithActivePKTableForTest(t, "pk")
	fragmentTxn4.engine = engineR
	issue25590BindRound21Txn(t, fragmentTxn4)
	nameD, _, wD := issue25590Round21PushTombstone(t, fragmentTxn4, failFS)
	coordinatorTxn4 := newTransactionWithActivePKTableForTest(t, "pk")
	coordinatorTxn4.engine = engineC
	issue25590BindRound21Txn(t, coordinatorTxn4)
	require.ErrorContains(t, coordinatorTxn4.AdoptRemoteTransferredObjectNames([]string{nameD}),
		"workspace admission paused",
		"r6/H2: the saturated recording engine refuses the intake before anything books")
	_, liveC6, inFlightC6 := issue25590LedgerNow(engineC)
	require.Zero(t, liveC6, "the refused intake booked no owned member")
	require.Equal(t, int64(1), inFlightC6,
		"the refused intake booked nothing - the parked unit is the recording ledger's only member")
	// The refusing transport failed the run: the fragment keeps its local
	// owner and the fragment engine's own drain retires the file.
	fragmentTxn4.RetainUnrecordedS3Objects([]string{nameD},
		"the refused-intake run's teardown keeps the fragment-local owner")
	wD.Close()
	require.NoError(t, fragmentTxn4.FinalizeRemoteFragmentWorkspace(ctx, false))
	require.Equal(t, []string{nameD}, issue25590QueuedNames(engineR),
		"r6/H1: the fragment settles through the failure terminal and stays the reachable owner")
	require.NoError(t, engineR.deleteQueuedUnpublishedPendingOnce(ctx))
	issue25590RequireGone(t, failFS.FileService, nameD,
		"the refused-intake fragment file, retired by the fragment engine's own drain")
	engineC.abandonUnpublishedPendingReservations([]string{parkedName})
	queuedC, liveC7, inFlightC7 := issue25590LedgerNow(engineC)
	require.Zero(t, queuedC)
	require.Zero(t, liveC7)
	require.Zero(t, inFlightC7,
		"balanced accounting: the refused-intake leg leaves the recording engine exactly empty")
}

// TestIssue25590SustainedTwoEngineRefusalBounded is the round-22 review's
// finding H2 demanded shape: sustained remote rounds against TWO engines
// sharing one stalled file backend (deletion blocked everywhere), each
// round a fragment persisting through its own engine's reservation surface
// and relaying to a recording coordinator whose intake adopts through the
// SAME admission gate. The pre-r6 protocol leaked exactly here: a saturated
// coordinator fail-open-retained each refused relayed name (+1 per round
// behind a bound nobody releases) while the fragment recycled its capacity
// on every clean completion, so repeated rounds grew the aggregate without
// limit (the r21 test asserted the growth). The r6 gate turns every
// refusal into a FAILED run: the fragment then keeps its local obligations,
// so the fragment engine's own bound saturates too and its writers refuse
// further spills - the aggregate obligation population stabilizes at the
// sum of the per-engine bounds and stays there, no matter how many rounds
// run with deletion stalled.
func TestIssue25590SustainedTwoEngineRefusalBounded(t *testing.T) {
	ctx := context.Background()
	colexec.NewServer("")
	fragmentTxn, failFS := issue25590NewRound21TxnForTest(t)
	engineR := fragmentTxn.engine
	coordinatorTxn, _ := issue25590NewRound21TxnForTest(t)
	engineC := coordinatorTxn.engine
	engineC.fs = failFS

	// Deletion is blocked on the shared backend for the whole scene: the
	// drained names cannot retire, so the bound must hold on the ledgers.
	unblockDeletes := failFS.armDeleteBlock()
	t.Cleanup(unblockDeletes)

	const bound = 2
	savedLimit := unpublishedPendingAdmissionLimit
	unpublishedPendingAdmissionLimit = bound
	t.Cleanup(func() { unpublishedPendingAdmissionLimit = savedLimit })

	aggregate := func() int {
		queuedR, liveR, inFlightR := issue25590LedgerNow(engineR)
		queuedC, liveC, inFlightC := issue25590LedgerNow(engineC)
		return queuedR + int(liveR) + int(inFlightR) + queuedC + int(liveC) + int(inFlightC)
	}

	persistedWitness := []string{}
	persistedCount := func() int { return len(failFS.persistedSnapshot()) }

	// Rounds 1..2 fill the coordinator's queue through clean transfers
	// (adopt -> record -> rollback hands the recorded name to the
	// recording queue); round 3's intake is refused and fails its run,
	// so the fragment keeps its obligation; round 4's refused intake
	// saturates the fragment engine too; from round 5 on the fragment
	// writers' own reservation refuses the spill with NOTHING persisted,
	// and the aggregate pins at the sum of the two bounds. The rounds
	// continue well past saturation: the bounded population must hold,
	// further rounds change nothing.
	baselinePersisted := 0
	for round := 0; round < 8; round++ {
		fragTxn := newTransactionWithActivePKTableForTest(t, "pk")
		fragTxn.engine = engineR
		issue25590BindRound21Txn(t, fragTxn)
		name, stats, w, pushErr := issue25590Round22PushTombstoneResult(t, fragTxn, failFS)
		w.Close()
		if pushErr != nil {
			// The fragment engine's OWN bound refused the writer's spill
			// before anything persisted: a failed round changes nothing
			// but the file witness count.
			require.ErrorContains(t, pushErr, "workspace admission paused",
				"round %d: the fragment writers' own reservation refuses further persistence", round)
			require.NoError(t, fragTxn.FinalizeRemoteFragmentWorkspace(ctx, false))
			if round <= 3 {
				require.FailNowf(t, "unexpected early writer refusal",
					"round %d: rounds before saturation must persist - only the saturated fragment can refuse", round)
			}
			if baselinePersisted == 0 {
				baselinePersisted = persistedCount()
			} else {
				require.Equal(t, baselinePersisted, persistedCount(),
					"round %d: a writer-refused round persisted nothing new", round)
			}
			require.Equal(t, 2*bound, aggregate(),
				"round %d: the aggregate obligation population is exactly pinned at the sum of the per-engine bounds", round)
			continue
		}
		persistedWitness = append(persistedWitness, name)
		recTxn := newTransactionWithActivePKTableForTest(t, "pk")
		recTxn.engine = engineC
		issue25590BindRound21Txn(t, recTxn)
		adoptErr := recTxn.AdoptRemoteTransferredObjectNames([]string{name})
		fragTxn.RetainUnrecordedS3Objects([]string{name},
			"sustained fragment teardown (issue #25590 round-22 review)")
		if adoptErr == nil {
			// Clean round: the adoption precedes the release, the run
			// transfers, and the coordinator's own record lands — its
			// rollback then hands the recorded obligation to the
			// recording side's queue.
			require.NoError(t, fragTxn.FinalizeRemoteFragmentWorkspace(ctx, true))
			recTxn.StartStatement()
			require.NoError(t, recTxn.IncrStatementID(ctx, false))
			recTxn.UpdateSnapshotWriteOffset()
			batRec := issue25590Round21TombstoneStatsBatch(t, recTxn.proc.Mp(), stats)
			require.NoError(t, recTxn.writeFileWithAutoIncrEpoch(DELETE, 1, issue25590DBID,
				issue25590TableID, "db", "tbl", name, batRec,
				metadata.TNService{ServiceID: "issue25590-dn"}, 0))
			recTxn.EndStatement()
		} else {
			// The saturated recording engine refused the INTAKE: the
			// transport fails the run, the fragment settles through
			// the failure terminal and keeps its own reachable owner.
			require.ErrorContains(t, adoptErr, "workspace admission paused",
				"round %d: only the recording engine's admission bound refuses the intake", round)
			require.NoError(t, fragTxn.FinalizeRemoteFragmentWorkspace(ctx, false))
		}
		require.NoError(t, recTxn.Rollback(ctx))
		require.LessOrEqual(t, aggregate(), 2*bound,
			"round %d: the two-engine aggregate obligation population stays bounded by the sum of the per-engine bounds", round)
	}

	// The sustained scene proves NOTHING grew without bound: the pinned
	// file count and the bounded aggregate stay stable across the
	// writer-refused rounds, whatever ran before them.
	require.Equal(t, baselinePersisted, persistedCount(),
		"the pinned file witness count is stable across further stalled rounds")
	require.Equal(t, 2*bound, aggregate(),
		"the aggregate obligation population is bounded — pinned exactly at the sum of the per-engine bounds — under sustained refusal with deletion blocked")

	// Releasing the stalled backend retires the whole bounded backlog
	// exactly: both queues drain, every witness file is gone, and both
	// ledgers end exactly empty - balanced accounting through the drain.
	unblockDeletes()
	require.NoError(t, engineR.deleteQueuedUnpublishedPendingOnce(ctx))
	require.NoError(t, engineC.deleteQueuedUnpublishedPendingOnce(ctx))
	for _, name := range persistedWitness {
		issue25590RequireGone(t, failFS.FileService, name,
			"the sustained rounds' witness file, retired by the released drain")
	}
	queuedR, liveR, inFlightR := issue25590LedgerNow(engineR)
	queuedC, liveC, inFlightC := issue25590LedgerNow(engineC)
	require.Zero(t, queuedR)
	require.Zero(t, liveR)
	require.Zero(t, inFlightR)
	require.Zero(t, queuedC)
	require.Zero(t, liveC)
	require.Zero(t, inFlightC,
		"the released drain retires the whole bounded backlog on both engines")
}

// TestIssue25590OrdinaryQueryRowsCannotBecomeCleanupObligations is the
// round-23 transport regression for the P1 finding: ordinary
// five-column query rows whose 4th column names an existing committed
// object never become cleanup obligations. The r22 intake recognized
// exactly this row shape (the "r23/F1 (RED on r22)" probe adopts it
// outright on the r22 base), booked the named file, and the
// transaction's terminals deleted it. The r23 protocol splits the
// authority across layers: the compile-side producer-provenance gate
// (its own registry test) never routes an ordinary query stream to a
// recognizer, every per-format classifier rejects the ordinary row
// shape, and a row claiming a transport class without a provable tail
// rejects fail-closed (the multi_update recognizer's own test). This
// leg proves the recording-side outcome those layers protect: through
// every reachable intake surface the ordinary rows book nothing, and
// a file named by an ordinary row stays untouched through both COMMIT
// and ROLLBACK.
func TestIssue25590OrdinaryQueryRowsCannotBecomeCleanupObligations(t *testing.T) {
	ctx := context.Background()
	colexec.NewServer("")
	// The fragment side pushes the witness files; the recording side is
	// a separate engine sharing the same filesystem, exactly like the
	// r22 intake fixture.
	fragmentTxn, failFS := issue25590NewRound21TxnForTest(t)
	engineR := fragmentTxn.engine
	recordingFix, _ := issue25590NewRound21TxnForTest(t)
	engineC := recordingFix.engine
	engineC.fs = failFS

	// The danger control first: the recording machinery WOULD book and
	// physically delete a real adoption of a relayed name - the exact
	// route the r22 shape-recognition opened to ordinary rows. This
	// proves the terminals are live, so the untouched legs below prove
	// absence of booking, not absence of machinery.
	dangerName, _, wDanger := issue25590Round21PushTombstone(t, fragmentTxn, failFS)
	wDanger.Close()
	fragmentTxn.RetainUnrecordedS3Objects([]string{dangerName},
		"witness for the danger control (issue #25590 round-23 review)")
	require.NoError(t, fragmentTxn.FinalizeRemoteFragmentWorkspace(ctx, true))
	require.Contains(t, failFS.persistedSnapshot(), dangerName,
		"the released fragment leaves the witness file persisted")

	dangerTxn := newTransactionWithActivePKTableForTest(t, "pk")
	dangerTxn.engine = engineC
	issue25590BindRound21Txn(t, dangerTxn)
	require.NoError(t, dangerTxn.AdoptRemoteTransferredObjectNames([]string{dangerName}))
	_, _, inFlightDanger := issue25590LedgerNow(engineC)
	require.Equal(t, int64(1), inFlightDanger, "a real adoption books the file - the machinery is live")
	require.NoError(t, dangerTxn.Rollback(ctx))
	require.Equal(t, []string{dangerName}, issue25590QueuedNames(engineC),
		"the rolled-back adopted name reaches the cleanup queue")
	require.NoError(t, engineC.deleteQueuedUnpublishedPendingOnce(ctx))
	issue25590RequireGone(t, failFS.FileService, dangerName,
		"an adopted name deletes physically at the terminal - the ordinary legs need a fresh witness")

	// A fresh witness file for the untouched legs, again released from
	// the fragment side so the recording engine starts empty.
	fragmentTxn2 := newTransactionWithActivePKTableForTest(t, "pk")
	fragmentTxn2.engine = engineR
	issue25590BindRound21Txn(t, fragmentTxn2)
	witnessName, _, wWitness := issue25590Round21PushTombstone(t, fragmentTxn2, failFS)
	wWitness.Close()
	fragmentTxn2.RetainUnrecordedS3Objects([]string{witnessName},
		"witness for the ordinary-row regression (issue #25590 round-23)")
	require.NoError(t, fragmentTxn2.FinalizeRemoteFragmentWorkspace(ctx, true))
	require.Contains(t, failFS.persistedSnapshot(), witnessName, "the witness survives the fragment's release")
	_, liveR, inFlightR := issue25590LedgerNow(engineC)
	require.Zero(t, liveR)
	require.Zero(t, inFlightR, "the recording engine starts empty")

	// The ordinary five-column result rows naming the committed file:
	// no reachable intake surface claims them as ownership metadata.
	ordinary := func(txn *Transaction) *batch.Batch {
		bat := batch.NewWithSize(5)
		bat.Vecs[0] = vector.NewVec(types.T_uint8.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_uint64.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_uint64.ToType())
		bat.Vecs[3] = vector.NewVec(types.T_varchar.ToType())
		bat.Vecs[4] = vector.NewVec(types.T_text.ToType())
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], uint8(1), false, txn.proc.Mp()))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], uint64(2), false, txn.proc.Mp()))
		require.NoError(t, vector.AppendFixed(bat.Vecs[2], uint64(3), false, txn.proc.Mp()))
		require.NoError(t, vector.AppendBytes(bat.Vecs[3], []byte(witnessName), false, txn.proc.Mp()))
		require.NoError(t, vector.AppendBytes(bat.Vecs[4], []byte(witnessName), false, txn.proc.Mp()))
		bat.SetRowCount(1)
		return bat
	}
	requireOrdinaryBooksNothing := func(txn *Transaction) {
		bat := ordinary(txn)
		defer bat.Clean(txn.proc.Mp())
		_, ok, err := colexecinsert.TryExtractS3InsertObjectNames(bat)
		require.NoError(t, err)
		require.False(t, ok,
			"the standalone insert classifier never mistakes ordinary five-column rows for ownership metadata")
		_, ok, err = colexecdeletion.TryExtractRemoteDeleteObjectNames(bat, txn.proc.Mp())
		require.NoError(t, err)
		require.False(t, ok,
			"the standalone delete classifier never mistakes ordinary five-column rows for ownership metadata")
		_, live, inFlight := issue25590LedgerNow(engineC)
		require.Zero(t, live)
		require.Zero(t, inFlight,
			"ordinary query rows naming a committed file book no obligation anywhere")
	}

	// COMMIT leg.
	commitTxn := newTransactionWithActivePKTableForTest(t, "pk")
	commitTxn.engine = engineC
	issue25590BindRound21Txn(t, commitTxn)
	requireOrdinaryBooksNothing(commitTxn)
	commitTxn.StartStatement()
	require.NoError(t, commitTxn.IncrStatementID(ctx, false))
	commitTxn.EndStatement()
	commitTxn.FinalizeCommit(ctx)
	issue25590RequireKept(t, failFS.FileService, witnessName,
		"r23/F1: an ordinary query row naming a committed file leaves it untouched through COMMIT")

	// ROLLBACK leg.
	rollbackTxn := newTransactionWithActivePKTableForTest(t, "pk")
	rollbackTxn.engine = engineC
	issue25590BindRound21Txn(t, rollbackTxn)
	requireOrdinaryBooksNothing(rollbackTxn)
	require.NoError(t, rollbackTxn.Rollback(ctx))
	issue25590RequireKept(t, failFS.FileService, witnessName,
		"r23/F1: an ordinary query row naming a committed file leaves it untouched through ROLLBACK")
	_, liveC, inFlightC := issue25590LedgerNow(engineC)
	require.Zero(t, liveC)
	require.Zero(t, inFlightC)
}

// TestIssue25590StandaloneProducerFormatsAdoptBeforeRelease is the
// round-23 receiver-side proof for finding 3: the standalone INSERT
// and DELETE transport formats adopt through the real intake
// classifiers before the fragment can release - so a cancellation
// between the intake adoption and any record keeps the relayed files
// enumerated and physically retired at the recording terminal - and
// a saturated recording engine refuses a further standalone intake
// fail-closed with nothing booked while deletion stays stalled, the
// backlog pinning exactly at the admission bound until the physical
// retirement can run.
func TestIssue25590StandaloneProducerFormatsAdoptBeforeRelease(t *testing.T) {
	ctx := context.Background()
	colexec.NewServer("")
	fragmentTxn, failFS := issue25590NewRound21TxnForTest(t)
	engineR := fragmentTxn.engine
	recordingFix, _ := issue25590NewRound21TxnForTest(t)
	engineC := recordingFix.engine
	engineC.fs = failFS

	// RECEIVER - the standalone INSERT result stream.
	nameA, statsA, wA := issue25590Round21PushTombstone(t, fragmentTxn, failFS)
	insTxn := newTransactionWithActivePKTableForTest(t, "pk")
	insTxn.engine = engineC
	issue25590BindRound21Txn(t, insTxn)
	insBatch := colexec.AllocCNS3ResultBat(false)
	require.NoError(t, colexec.ExpandObjectStatsToBatch(
		insTxn.proc.Mp(), false, insBatch, true, statsA))
	insBatch.SetRowCount(insBatch.Vecs[1].Length())
	namesA, ok, err := colexecinsert.TryExtractS3InsertObjectNames(insBatch)
	require.NoError(t, err)
	require.True(t, ok, "the standalone insert result layout adopts")
	require.Equal(t, []string{nameA}, namesA,
		"the insert classifier derives exactly the persisted object's canonical name")
	require.NoError(t, insTxn.AdoptRemoteTransferredObjectNames(namesA))
	_, liveI, inFlightI := issue25590LedgerNow(engineC)
	require.Equal(t, int64(1), inFlightI,
		"r23/F3: the standalone insert intake books its relayed object before the fragment can release")
	require.Zero(t, liveI)
	fragmentTxn.RetainUnrecordedS3Objects([]string{nameA},
		"remote fragment teardown after the standalone-insert adoption (issue #25590 round-23 review)")
	wA.Close()
	require.NoError(t, fragmentTxn.FinalizeRemoteFragmentWorkspace(ctx, true))
	require.Contains(t, failFS.persistedSnapshot(), nameA,
		"the fragment released without deleting - the adopted reservation owns the file")
	// Cancellation before any record: the recording terminal keeps the
	// adopted name enumerated and the queue deletes it physically.
	require.NoError(t, insTxn.Rollback(ctx))
	require.Equal(t, []string{nameA}, issue25590QueuedNames(engineC),
		"r23/F3: the cancelled standalone-insert terminal keeps the adopted name enumerated")
	require.NoError(t, engineC.deleteQueuedUnpublishedPendingOnce(ctx))
	issue25590RequireGone(t, failFS.FileService, nameA,
		"the cancelled standalone-insert record's file retires physically")

	// RECEIVER - the standalone DELETE remote stream, with a retained
	// block class row beside the flushed tombstone row: only the
	// tombstone adopts.
	fragmentTxn2 := newTransactionWithActivePKTableForTest(t, "pk")
	fragmentTxn2.engine = engineR
	issue25590BindRound21Txn(t, fragmentTxn2)
	nameB, statsB, wB := issue25590Round21PushTombstone(t, fragmentTxn2, failFS)

	delTxn := newTransactionWithActivePKTableForTest(t, "pk")
	delTxn.engine = engineC
	issue25590BindRound21Txn(t, delTxn)
	tail := issue25590Round21TombstoneStatsBatch(t, delTxn.proc.Mp(), statsB)
	defer tail.Clean(delTxn.proc.Mp())
	tailBytes, err := tail.MarshalBinary()
	require.NoError(t, err)
	rowBat := batch.NewWithSize(2)
	rowBat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	rowBat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(rowBat.Vecs[0], types.RandomRowid(), false, delTxn.proc.Mp()))
	require.NoError(t, vector.AppendFixed(rowBat.Vecs[1], int64(7), false, delTxn.proc.Mp()))
	rowBat.SetRowCount(1)
	defer rowBat.Clean(delTxn.proc.Mp())
	rowBytes, err := rowBat.MarshalBinary()
	require.NoError(t, err)
	delBatch := batch.NewWithSize(4)
	delBatch.SetAttributes([]string{
		catalog.BlockMeta_Delete_ID, catalog.BlockMeta_DeltaLoc,
		catalog.BlockMeta_Type, catalog.BlockMeta_Partition,
	})
	delBatch.Vecs[0] = vector.NewVec(types.T_text.ToType())
	delBatch.Vecs[1] = vector.NewVec(types.T_text.ToType())
	delBatch.Vecs[2] = vector.NewVec(types.T_int8.ToType())
	delBatch.Vecs[3] = vector.NewVec(types.T_int32.ToType())
	require.NoError(t, vector.AppendBytes(delBatch.Vecs[0], []byte("blk-a"), false, delTxn.proc.Mp()))
	require.NoError(t, vector.AppendBytes(delBatch.Vecs[1], rowBytes, false, delTxn.proc.Mp()))
	require.NoError(t, vector.AppendFixed(delBatch.Vecs[2], int8(1), false, delTxn.proc.Mp()))
	require.NoError(t, vector.AppendFixed(delBatch.Vecs[3], int32(0), false, delTxn.proc.Mp()))
	require.NoError(t, vector.AppendBytes(delBatch.Vecs[0], []byte("obj-id"), false, delTxn.proc.Mp()))
	require.NoError(t, vector.AppendBytes(delBatch.Vecs[1], tailBytes, false, delTxn.proc.Mp()))
	require.NoError(t, vector.AppendFixed(delBatch.Vecs[2], int8(colexecdeletion.FlushDeltaLoc), false, delTxn.proc.Mp()))
	require.NoError(t, vector.AppendFixed(delBatch.Vecs[3], int32(0), false, delTxn.proc.Mp()))
	delBatch.SetRowCount(2)
	defer delBatch.Clean(delTxn.proc.Mp())
	namesB, ok, err := colexecdeletion.TryExtractRemoteDeleteObjectNames(delBatch, delTxn.proc.Mp())
	require.NoError(t, err)
	require.True(t, ok, "the standalone deletion remote layout adopts")
	require.Equal(t, []string{nameB}, namesB,
		"the delete classifier adopts exactly the flushed tombstone, never the retained block")
	require.NoError(t, delTxn.AdoptRemoteTransferredObjectNames(namesB))
	_, liveD, inFlightD := issue25590LedgerNow(engineC)
	require.Equal(t, int64(1), inFlightD, "r23/F3: the standalone delete intake books the tombstone before release")
	require.Zero(t, liveD)
	fragmentTxn2.RetainUnrecordedS3Objects([]string{nameB},
		"remote fragment teardown after the standalone-delete adoption (issue #25590 round-23 review)")
	wB.Close()
	require.NoError(t, fragmentTxn2.FinalizeRemoteFragmentWorkspace(ctx, true))
	require.NoError(t, delTxn.Rollback(ctx))
	require.Equal(t, []string{nameB}, issue25590QueuedNames(engineC),
		"r23/F3: the cancelled standalone-delete terminal owns the tombstone now")
	require.NoError(t, engineC.deleteQueuedUnpublishedPendingOnce(ctx))
	issue25590RequireGone(t, failFS.FileService, nameB,
		"the cancelled standalone-delete tombstone retires physically")

	// SUSTAINED REFUSAL - deletion stalled at the recording queue and the
	// third standalone intake refused fail-closed with nothing booked.
	const bound = 2

	// Three real witnesses pushed and released from the fragment side
	// while the default bound still admits the fragment's own spills.
	fragmentTxn3 := newTransactionWithActivePKTableForTest(t, "pk")
	fragmentTxn3.engine = engineR
	issue25590BindRound21Txn(t, fragmentTxn3)
	var satNames []string
	var satBatches []*batch.Batch
	for i := 0; i < bound+1; i++ {
		name, stats, w := issue25590Round21PushTombstone(t, fragmentTxn3, failFS)
		satNames = append(satNames, name)
		bat := colexec.AllocCNS3ResultBat(false)
		require.NoError(t, colexec.ExpandObjectStatsToBatch(
			fragmentTxn3.proc.Mp(), false, bat, true, stats))
		bat.SetRowCount(bat.Vecs[1].Length())
		satBatches = append(satBatches, bat)
		w.Close()
	}
	fragmentTxn3.RetainUnrecordedS3Objects(satNames,
		"remote fragment teardown while the recording engine stands saturated (issue #25590 round-23 review)")
	require.NoError(t, fragmentTxn3.FinalizeRemoteFragmentWorkspace(ctx, true))

	// Only now the recording side arms its own bound and the shared
	// backend stalls deletion.
	savedLimit := unpublishedPendingAdmissionLimit
	unpublishedPendingAdmissionLimit = bound
	t.Cleanup(func() { unpublishedPendingAdmissionLimit = savedLimit })
	unblockDeletes := failFS.armDeleteBlock()
	t.Cleanup(unblockDeletes)

	satTxn := newTransactionWithActivePKTableForTest(t, "pk")
	satTxn.engine = engineC
	issue25590BindRound21Txn(t, satTxn)
	intakeInsert := func(bat *batch.Batch) error {
		names, ok, err := colexecinsert.TryExtractS3InsertObjectNames(bat)
		if err != nil || !ok {
			return err
		}
		return satTxn.AdoptRemoteTransferredObjectNames(names)
	}
	for i := 0; i < bound; i++ {
		require.NoError(t, intakeInsert(satBatches[i]),
			"a clean recording engine admits standalone intakes up to its bound")
	}
	_, liveS, inFlightS := issue25590LedgerNow(engineC)
	require.Equal(t, int64(bound), inFlightS, "the recording side holds exactly its bound")
	require.Zero(t, liveS)
	require.Error(t, intakeInsert(satBatches[bound]),
		"a saturated recording engine refuses a further standalone intake fail-closed")
	_, liveS2, inFlightS2 := issue25590LedgerNow(engineC)
	require.Equal(t, int64(bound), inFlightS2,
		"the refused intake booked nothing beyond the bound - the aggregate population pins")
	require.Zero(t, liveS2)
	// The stalled drain: the terminal rolls the bounded backlog into the
	// queue, deletion stays blocked, and every name stays enumerable.
	require.NoError(t, satTxn.Rollback(ctx))
	require.ElementsMatch(t, satNames[:bound], issue25590QueuedNames(engineC),
		"the stalled drain keeps the whole bounded backlog enumerable on the recording side")
	require.Equal(t, bound, len(issue25590QueuedNames(engineC)),
		"the bounded accounting holds through the stalled drain - deletion is blocked, so the backlog cannot retire yet")
	// The blocked backend stalls the physical retirement; once the
	// drain can delete again the whole bounded backlog retires.
	unblockDeletes()
	require.NoError(t, engineC.deleteQueuedUnpublishedPendingOnce(ctx))
	for _, name := range satNames[:bound] {
		issue25590RequireGone(t, failFS.FileService, name,
			"the bounded backlog retires physically once the drain can delete")
	}
}

// TestIssue25590FullyDeletedObjectLeavesNoEmptyReplacement pins the
// round-32 pre-PR review's finding 1 against design r11's I1: an
// object-compaction rewrite whose pending deletions remove EVERY row of
// its object persists no replacement object, so the pass represents it as
// NO replacement record - the original entry retires into nothing and no
// EMPTY workspace entry is ever appended. The pre-fix pass staged and
// published the rewrite's empty metadata batch (rewriteObjectByDeletion
// returned the empty info batch, the stage loop staged it
// unconditionally and compactNilWorkspaceSlots removes only nil
// batches): merges skip empty batches, so every fully superseded spilled
// object left one inert entry behind and repeated same-PK mutations that
// spill between statements rebuilt the history-bound rescans over a
// constant live working set. This regression drives exactly that loop -
// one spilled single-object statement per round, then a full deletion of
// the object's every row, then the pass - and asserts at every boundary
// that the writes slice stays proportional to the live working set,
// with the pass's atomic retirement and cleanup rules intact.
func TestIssue25590FullyDeletedObjectLeavesNoEmptyReplacement(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)
	defer func() {
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	}()

	ctx := context.Background()
	e := txn.engine
	e.pClient = PushClient{}
	failFS := issue25590SwapProcSharedFS(t, txn)
	e.fs = failFS

	// A sequential one-slot compaction worker keeps each round's rewrite
	// deterministic, and the engine GC pool serves the original's GC.
	compactPool, err := ants.NewPool(1)
	require.NoError(t, err)
	t.Cleanup(func() { compactPool.Release() })
	txn.compactWorker = compactPool
	gcPool, gcErr := ants.NewPool(2)
	require.NoError(t, gcErr)
	t.Cleanup(func() { gcPool.Release() })
	require.NoError(t, e.ResetGCWorkerPool(gcPool))

	// The data-column table whose spilled objects the pass rewrites:
	// [a(pk), v, rowid] exactly like a physical table's def.
	dbv := txn.databaseOps.existAndActive(genDatabaseKey(1, "db"))
	require.NotNil(t, dbv)
	const ctblID = uint64(44)
	txn.tableOps.addCreateTable(
		genTableKey(1, "ctbl", 7, "db"),
		0,
		&txnTable{
			accountId: 1,
			tableId:   ctblID,
			tableName: "ctbl",
			db:        dbv,
			tableDef: &pbplan.TableDef{
				Cols: []*pbplan.ColDef{
					{Name: "a", Typ: pbplan.Type{Id: int32(types.T_int64)}, Primary: true},
					{Name: "v", Typ: pbplan.Type{Id: int32(types.T_int64)}},
					{Name: catalog.Row_ID,
						Typ: pbplan.Type{Id: int32(types.T_Rowid)}},
				},
				Pkey: &pbplan.PrimaryKeyDef{PkeyColName: "a"},
			},
		},
	)

	admit := func() { issue25590AdmitBoundary(t, txn, ctx) }
	noEmptyEntries := func(where string) {
		for i := range txn.writes {
			entry := txn.writes[i]
			require.False(t, entry.bat != nil && entry.bat.RowCount() == 0,
				"%s: writes[%d] is an EMPTY non-nil entry - the inert record a fully deleted object leaves behind (issue #25590 I1)",
				where, i)
		}
	}

	// One statement's rows per object: a single block forces exactly ONE
	// small object per dump, the unit the round fully supersedes.
	const rowsPerObject = objectio.BlockMaxRows
	var nextPK int64

	// dumpObject publishes one fresh single-object metadata entry and
	// returns its stats and first block.
	dumpObject := func() (objectio.ObjectStats, types.Blockid) {
		t.Helper()
		admit()
		pks := make([]int64, rowsPerObject)
		for i := range pks {
			nextPK++
			pks[i] = nextPK
		}
		rowidVec, err := txn.batchAllocNewRowIds(rowsPerObject)
		require.NoError(t, err)
		t.Cleanup(func() { rowidVec.Free(txn.proc.Mp()) })
		rowids := vector.MustFixedColNoTypeCheck[types.Rowid](rowidVec)
		bat := batch.NewWithSize(3)
		bat.SetAttributes([]string{objectio.PhysicalAddr_Attr, "a", "v"})
		rowidCol := vector.NewVec(types.T_Rowid.ToType())
		require.NoError(t, vector.AppendFixedList(rowidCol, rowids, nil, txn.proc.Mp()))
		aCol := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(aCol, pks, nil, txn.proc.Mp()))
		vCol := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(vCol, pks, nil, txn.proc.Mp()))
		bat.Vecs[0] = rowidCol
		bat.Vecs[1] = aCol
		bat.Vecs[2] = vCol
		bat.SetRowCount(rowsPerObject)
		txn.appendWorkspaceEntryLocked(Entry{
			typ:          INSERT,
			accountId:    1,
			databaseId:   issue25590DBID,
			tableId:      ctblID,
			databaseName: "db",
			tableName:    "ctbl",
			bat:          bat,
		})
		smallObjsDisarm := issue25590ArmKeyedFault(t, objectio.FJ_CNFlushSmallObjs, 20, "")
		txn.Lock()
		var pkCount int
		err = txn.dumpInsertBatchLocked(ctx, failFS, txn.GetSnapshotWriteOffset(), &pkCount)
		txn.Unlock()
		smallObjsDisarm()
		require.NoError(t, err)
		require.Equal(t, int(rowsPerObject), pkCount)
		metaIdx, statsCol, statsArea := issue25590MetadataEntry(t, txn)
		require.Equal(t, 1, len(statsCol),
			"each statement round dumps exactly ONE small object")
		st := objectio.ObjectStats(statsCol[0].GetByteSlice(statsArea))
		require.Greater(t, int(st.Rows()), 0)
		infoCol, infoArea := vector.MustVarlenaRawData(txn.writes[metaIdx].bat.Vecs[0])
		firstBlock := objectio.DecodeBlockInfo(
			infoCol[0].GetByteSlice(infoArea)).BlockID
		txn.EndStatement()
		return st, firstBlock
	}

	// fullyDelete marks EVERY row of the round's object deleted.
	fullyDelete := func(st objectio.ObjectStats, block types.Blockid) {
		t.Helper()
		rows := int(st.Rows())
		rowids := make([]types.Rowid, rows)
		pks := make([]int64, rows)
		for r := range rowids {
			rowids[r] = types.NewRowid(&block, uint32(r))
			pks[r] = nextPK - int64(rows) + int64(r)
		}
		delBat := batch.NewWithSize(2)
		delBat.SetAttributes([]string{objectio.PhysicalAddr_Attr, "a"})
		rowidVec2 := vector.NewVec(types.T_Rowid.ToType())
		require.NoError(t, vector.AppendFixedList(rowidVec2, rowids, nil, txn.proc.Mp()))
		pk2Vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(pk2Vec, pks, nil, txn.proc.Mp()))
		delBat.Vecs[0] = rowidVec2
		delBat.Vecs[1] = pk2Vec
		delBat.SetRowCount(rows)
		admit()
		out := txn.deleteBatch(delBat, issue25590DBID, ctblID)
		t.Cleanup(func() { out.Clean(txn.proc.Mp()) })
		require.Equal(t, 1, txn.deletedBlocks.size(),
			"the full delete marks the object's single block")
		require.Len(t, txn.deletedBlocks.offsets[block], rows,
			"the object's every row is a pending deletion")
		txn.EndStatement()
	}

	persistedBefore := len(failFS.persistedSnapshot())
	// round-37 (issue #25590 review of 2026-10-24, finding 1): the loop
	// also pins the boundedness of the transaction's recorded-name copy.
	// The admission bound is pinned to the review's smallest working
	// value: every round's settle closes the ledger, and the recorded set
	// must keep exactly the LIVE obligations rather than carrying every
	// historical name until transaction termination — memory no admission
	// bound ever counted, and a terminal release that copied and scanned
	// the whole spill history. The mixed round below restores the
	// production bound first because it legitimately exceeds two.
	restoreAdmissionLimit := PinUnpublishedPendingAdmissionLimitForTest(2)
	// recordedNames reads the transaction's recorded-name copy - the
	// round-37 settle path may clear it from a GC worker without the
	// transaction lock, so the green tree reads it under the recorded-name
	// guard exactly like the production settle does.
	recordedNames := func() int {
		return issue25590RecordedLen(txn)
	}
	for round := 0; round < 4; round++ {
		st, block := dumpObject()
		require.Len(t, failFS.persistedSnapshot(), persistedBefore+round+1,
			"round %d: the statement's dump persisted exactly its one small object", round)
		// While the round's obligation is still live, the recorded-name
		// copy keeps protecting it: exactly the one recorded object of
		// this round — never the retired history.
		require.Equal(t, 1, recordedNames(),
			"round %d: a live recorded obligation keeps its recorded-name copy", round)
		fullyDelete(st, block)

		txn.Lock()
		err = txn.compactDeletionOnObjsLocked(ctx)
		txn.Unlock()
		require.NoError(t, err,
			"round %d: the fully deleted rewrite completes as a clean NO-RECORD pass", round)

		// The leak's counterexample: NO write entry is an empty non-nil
		// batch, at this boundary or any earlier one.
		noEmptyEntries(fmt.Sprintf("round %d after the pass", round))
		// A fully deleted rewrite has no replacement: nothing persisted.
		require.Len(t, failFS.persistedSnapshot(), persistedBefore+round+1,
			"round %d: no replacement object persisted for a fully deleted rewrite", round)
		// The pass consumed the marks and retired the original entry.
		require.Zero(t, txn.deletedBlocks.size(),
			"round %d: the pass embodies the deletions and clears their marks", round)
		// The pass's cleanup rules hold: the transaction owns nothing
		// unsettled and the engine queue holds nothing.
		pending, reserved := txn.UnsettledCleanupOwnershipForTest()
		require.Empty(t, pending, "round %d: nothing joined pending cleanup", round)
		require.Empty(t, reserved, "round %d: no reservation stayed outstanding", round)
		queued, _, inFlight := e.InspectUnpublishedPendingForTest()
		require.Zero(t, queued, "round %d: the engine queue holds nothing", round)
		require.Zero(t, inFlight, "round %d: the pass retired every in-flight unit", round)
		// round-37: the GC settle that retires the deleted original's
		// ledger membership retires the recorded-name copy at the same
		// boundary, so the copy tracks the LIVE working set — repeated
		// spill/fully-delete rounds leave nothing recorded behind, and
		// every later boundary or terminal classification walks a set
		// bounded by what the admission bound ever counted. The live
		// membership retires in the same GC worker right before the copy.
		require.Eventually(t, func() bool {
			_, live, _ := e.InspectUnpublishedPendingForTest()
			return live == 0 && recordedNames() == 0
		}, 10*time.Second, 20*time.Millisecond,
			"round %d: the settle that ends the file's ownership must retire BOTH the deleted original's ledger membership and the transaction's recorded-name copy (round entered with %d recorded)", round, recordedNames())
	}
	restoreAdmissionLimit()

	// The boundary proves the live-set bound: after the last admission the
	// nil slots the passes left behind compact away entirely - the writes
	// slice is exactly the live working set, and FOUR fully superseded
	// spilled objects leave NOTHING behind.
	admit()
	require.Equal(t, 0, liveEntryCount(txn),
		"after the final boundary the workspace holds only live entries")
	require.Len(t, txn.writes, 0,
		"the nil slots of four fully superseded objects compacted away at the boundary: txn.writes stays proportional to the live working set (issue #25590 I1)")
	// Close the boundary probe's statement before the mixed round's own
	// statements begin.
	txn.EndStatement()

	// The mixed round: one dumped ENTRY with SEVERAL objects, then a full
	// deletion of the FIRST object only. The surviving objects must stay
	// visible - re-published or rewritten with their rows intact - while
	// the fully deleted object again leaves no empty record: the pass's
	// retirement publishes exactly the survivors.
	admit()
	const mixedRows = 300000
	mixedPKs := make([]int64, mixedRows)
	for i := range mixedPKs {
		nextPK++
		mixedPKs[i] = nextPK
	}
	mixedRowidVec, err := txn.batchAllocNewRowIds(mixedRows)
	require.NoError(t, err)
	t.Cleanup(func() { mixedRowidVec.Free(txn.proc.Mp()) })
	mixedRowids := vector.MustFixedColNoTypeCheck[types.Rowid](mixedRowidVec)
	mixedBat := batch.NewWithSize(3)
	mixedBat.SetAttributes([]string{objectio.PhysicalAddr_Attr, "a", "v"})
	mRidCol := vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixedList(mRidCol, mixedRowids, nil, txn.proc.Mp()))
	mACol := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(mACol, mixedPKs, nil, txn.proc.Mp()))
	mVCol := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(mVCol, mixedPKs, nil, txn.proc.Mp()))
	mixedBat.Vecs[0] = mRidCol
	mixedBat.Vecs[1] = mACol
	mixedBat.Vecs[2] = mVCol
	mixedBat.SetRowCount(mixedRows)
	txn.appendWorkspaceEntryLocked(Entry{
		typ:          INSERT,
		accountId:    1,
		databaseId:   issue25590DBID,
		tableId:      ctblID,
		databaseName: "db",
		tableName:    "ctbl",
		bat:          mixedBat,
	})
	mixedDisarm := issue25590ArmKeyedFault(t, objectio.FJ_CNFlushSmallObjs, 20, "")
	txn.Lock()
	var mixedStorePKs int
	err = txn.dumpInsertBatchLocked(ctx, failFS, txn.GetSnapshotWriteOffset(), &mixedStorePKs)
	txn.Unlock()
	mixedDisarm()
	require.NoError(t, err)
	require.Equal(t, mixedRows, mixedStorePKs)

	// The mixed entry's objects and the first object's full row span.
	metaEntries := issue25590AllMetadataEntries(t, txn)
	require.GreaterOrEqual(t, len(metaEntries), 1,
		"the mixed dump published its metadata entry")
	mixedMeta := metaEntries[len(metaEntries)-1]
	mixedStatsCol, mixedStatsArea := vector.MustVarlenaRawData(txn.writes[mixedMeta].bat.Vecs[1])
	mixedObjNames := make([]string, 0, len(mixedStatsCol))
	var deletedObjName string
	var deletedRows int
	for i := range mixedStatsCol {
		st := objectio.ObjectStats(mixedStatsCol[i].GetByteSlice(mixedStatsArea))
		mixedObjNames = append(mixedObjNames, st.ObjectName().String())
		if i == 0 {
			deletedObjName = st.ObjectName().String()
			deletedRows = int(st.Rows())
		}
	}
	mixedInfoCol, mixedInfoArea := vector.MustVarlenaRawData(txn.writes[mixedMeta].bat.Vecs[0])
	firstMixedBlock := objectio.DecodeBlockInfo(
		mixedInfoCol[0].GetByteSlice(mixedInfoArea)).BlockID
	txn.EndStatement()

	// A full deletion of the FIRST object's every row.
	delRowids := make([]types.Rowid, deletedRows)
	delPKs := make([]int64, deletedRows)
	for r := range delRowids {
		delRowids[r] = types.NewRowid(&firstMixedBlock, uint32(r))
		delPKs[r] = nextPK - int64(mixedRows) + int64(r)
	}
	requiring := batch.NewWithSize(2)
	requiring.SetAttributes([]string{objectio.PhysicalAddr_Attr, "a"})
	delRidCol := vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixedList(delRidCol, delRowids, nil, txn.proc.Mp()))
	delPKCol := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(delPKCol, delPKs, nil, txn.proc.Mp()))
	requiring.Vecs[0] = delRidCol
	requiring.Vecs[1] = delPKCol
	requiring.SetRowCount(deletedRows)
	admit()
	outMixed := txn.deleteBatch(requiring, issue25590DBID, ctblID)
	t.Cleanup(func() { outMixed.Clean(txn.proc.Mp()) })
	require.GreaterOrEqual(t, txn.deletedBlocks.size(), 1,
		"the full deletes mark the first object's blocks")
	txn.EndStatement()

	txn.Lock()
	err = txn.compactDeletionOnObjsLocked(ctx)
	txn.Unlock()
	require.NoError(t, err, "the mixed round's pass completes")

	noEmptyEntries("the mixed round after the pass")
	// The survivors: every surviving object keeps exactly one metadata
	// record - its replacement for a dirty survivor, its re-publication
	// for a clean one - and the fully deleted first object is GONE from
	// every record.
	metaIdxes := issue25590AllMetadataEntries(t, txn)
	visibleStatsNames := make([]string, 0, len(metaIdxes))
	for _, metaIdx := range metaIdxes {
		visibleStatsNames = append(visibleStatsNames,
			issue25590StatsNamesOfMeta(t, txn, metaIdx)...)
	}
	for _, name := range mixedObjNames[1:] {
		require.Contains(t, visibleStatsNames, name,
			"surviving object %s stays visible in the pass's records", name)
	}
	require.NotContains(t, visibleStatsNames, deletedObjName,
		"the fully deleted object leaves every record: its deletion is embodied by pure absence")
}

// liveEntryCount counts the workspace's non-nil entries - the entries
// every workspace consumer rescans per statement.
func liveEntryCount(txn *Transaction) int {
	live := 0
	for i := range txn.writes {
		if txn.writes[i].bat != nil {
			live++
		}
	}
	return live
}
