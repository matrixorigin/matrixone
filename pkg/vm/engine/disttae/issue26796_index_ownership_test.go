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

// Issue 26796 review coverage for key-index ownership: this file pins the
// lifecycle contracts that the per-batch exact-key index must hold across
// the workspace's terminal paths:
//
//   - transaction teardown (commit finalization and rollback) drops every
//     hydrated index together with its charge, proven with debug accounting
//     enabled so the enforcement point (assertWorkspaceAccountingLocked)
//     actually runs — the pre-fix teardown refunded the charge without
//     dropping the map and panicked there on every hydrated txn;
//   - an in-transaction ALTER that duplicates a live entry never copies the
//     original's index pointer or its charged bytes: two refund owners for
//     one charge refund the same bytes twice and underflow the uint64
//     budget totals;
//   - forward workspace compactions (statement-boundary merges, spill
//     truncation) clear the vacated backing-array slots so retired batches
//     and refunded maps are not left reachable;
//   - spill sizing, quota calculation, and per-table spill selection see
//     the hydrated index bytes through the same in-memory entry size the
//     workspace budget charges, so index retention can push a workspace
//     over a spill threshold instead of hiding below it in every sizing
//     pass.

import (
	"context"
	"sync"
	"testing"

	commonruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

// issue26796TableSpec names one fixture table registered as created in the
// transaction.
type issue26796TableSpec struct {
	id   uint64
	name string
}

// newIssue26796TxnWithTables is the single-table fixture extended to
// registered tables: write-time primary-key resolution needs each table
// registered in tableOps the way the fixture registers its one table.
func newIssue26796TxnWithTables(t testing.TB, specs ...issue26796TableSpec) (*Transaction, *fileservice.Pool[*types.Packer]) {
	t.Helper()
	packerPool := fileservice.NewPool(
		4,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(packer *types.Packer) { packer.Close() },
	)
	proc := testutil.NewProc(t)
	txn := &Transaction{
		proc:            proc,
		engine:          &Engine{packerPool: packerPool},
		tableOps:        newTableOps(),
		databaseOps:     newDbOps(),
		deletedBlocks:   &deletedBlocks{},
		batchSelectList: make(map[*batch.Batch][]int64),
		cn_flushed_s3_tombstone_object_stats_list: new(sync.Map),
	}
	op := newTxnOperatorForTestWithWorkspace(t, txn)
	txn.op = op

	db := &txnDatabase{
		op:           op,
		databaseId:   issue26796DBID,
		databaseName: issue26796DB,
	}
	txn.databaseOps.addCreateDatabase(genDatabaseKey(1, issue26796DB), 0, db)
	for _, spec := range specs {
		txn.tableOps.addCreateTable(
			genTableKey(1, spec.name, issue26796DBID, issue26796DB),
			0,
			&txnTable{
				accountId: 1,
				tableId:   spec.id,
				tableName: spec.name,
				db:        db,
				tableDef:  issue26796TableDef(),
			},
		)
	}
	return txn, packerPool
}

// issue26796InsertBatchInto appends one multi-row insert entry for a named
// fixture table; values mirror the primary keys.
func issue26796InsertBatchInto(
	t *testing.T,
	txn *Transaction,
	tableID uint64,
	tableName string,
	pks ...int64,
) {
	t.Helper()
	bat := batch.NewWithSize(2)
	bat.Attrs = []string{issue26796PKName, "value"}
	bat.SetVector(0, vector.NewVec(types.T_int64.ToType()))
	bat.SetVector(1, vector.NewVec(types.T_int64.ToType()))
	for _, pk := range pks {
		require.NoError(t, vector.AppendFixed(bat.GetVector(0), pk, false, txn.proc.Mp()))
		require.NoError(t, vector.AppendFixed(bat.GetVector(1), pk, false, txn.proc.Mp()))
	}
	bat.SetRowCount(len(pks))
	_, err := txn.WriteBatch(
		INSERT, "", 1, issue26796DBID, tableID,
		issue26796DB, tableName, bat, DNStore{},
	)
	require.NoError(t, err)
}

// TestIssue26796TeardownClearsKeyIndexesWithDebugAccounting proves that
// transaction teardown — commit finalization and rollback — invalidates each
// hydrated exact-key index together with its budget charge. With the debug
// log level enabled, DoIfDebugEnabled gates assertWorkspaceAccountingLocked
// on, so the hydrated-index-to-charged-bytes invariant is enforced exactly
// as on a debug-enabled CN: the pre-fix teardown refunded the bytes without
// dropping the map, and the assertion in delTransaction reached the
// hydrated-index/zero-charge rejection and panicked on the first hydrated
// transaction that committed or rolled back.
func TestIssue26796TeardownClearsKeyIndexesWithDebugAccounting(t *testing.T) {
	// Enable debug for the whole test; restore the default configuration so
	// later package tests run as before.
	logutil.SetupMOLogger(&logutil.LogConfig{Level: "debug"})
	require.True(t, logutil.GetSkip1Logger().Core().Enabled(zapcore.DebugLevel),
		"the test must run with the accounting assertion enforced")
	t.Cleanup(func() {
		cfg := logutil.GetDefaultConfig()
		logutil.SetupMOLogger(&cfg)
	})

	// delTransaction notifies the colexec server owned by the engine's
	// service name; the fixture engine uses the empty service, so register a
	// real server for it (production registers one at CN startup) and remove
	// the registration so no other test observes it.
	rt := commonruntime.ServiceRuntime("")
	colexec.NewServer("")
	t.Cleanup(func() { commonruntime.CompareAndDeleteServiceBasedRuntime("", rt) })

	setupHydratedTxn := func(t *testing.T) (*Transaction, *fileservice.Pool[*types.Packer]) {
		txn, pool := newIssue26796Txn(t)
		issue26796InsertBatch(t, txn, 7, 3, 9, 12)
		values, d := issue26796PointRead(t, txn, issue26796Filter(t, pool, readutil.BasePKFilter{
			Valid: true, Op: function.EQUAL, Oid: types.T_int64,
			LB: types.EncodeFixed(int64(9)),
		}))
		require.Equal(t, []int64{9}, values)
		require.Equal(t, uint64(1), d.IndexBuilds, "the probe must hydrate exactly one index")

		txn.Lock()
		require.NotNil(t, txn.writes[0].keyOffsets)
		require.Positive(t, txn.writes[0].keyIndexAccountedSize)
		require.NoError(t, txn.checkWorkspaceAccountingLocked())
		txn.Unlock()
		return txn, pool
	}

	assertTornDown := func(t *testing.T, txn *Transaction) {
		t.Helper()
		txn.Lock()
		defer txn.Unlock()
		require.True(t, txn.removed)
		require.Zero(t, txn.workspaceSize, "teardown must refund every charged byte")
		require.Zero(t, txn.approximateInMemInsertSize)
		for i := range txn.writes {
			require.Nil(t, txn.writes[i].keyOffsets,
				"teardown must drop each hydrated key index at entry %d, not only its charge", i)
			require.Zero(t, txn.writes[i].keyIndexAccountedSize)
			require.Zero(t, txn.writes[i].accountedSize)
		}
		require.NoError(t, txn.checkWorkspaceAccountingLocked(),
			"the accounting invariant must hold on the torn-down workspace")
	}

	t.Run("commit_finalization", func(t *testing.T) {
		txn, _ := setupHydratedTxn(t)
		require.NotPanics(t, func() {
			txn.FinalizeCommit(context.Background())
		}, "commit finalization must tear down a hydrated workspace without tripping the debug accounting assertion")
		assertTornDown(t, txn)
	})

	t.Run("rollback", func(t *testing.T) {
		txn, _ := setupHydratedTxn(t)
		require.NotPanics(t, func() {
			require.NoError(t, txn.Rollback(context.Background()))
		}, "rollback must tear down a hydrated workspace without tripping the debug accounting assertion")
		assertTornDown(t, txn)
	})
}

// TestIssue26796AlterTransferResetsKeyIndexOwnership proves that the
// transactional ALTER write transfer duplicates a live in-memory entry
// without copying its hydrated key index or the charged index bytes: the
// duplicate hydrates its own index under its own ownership, and the
// archived original refunds exactly once at the statement-boundary shrink.
// Copying either the pointer or the charge (the pre-fix behavior) leaves two
// refund owners for one charge and underflows the uint64 budget totals.
func TestIssue26796AlterTransferResetsKeyIndexOwnership(t *testing.T) {
	keysOf := func(t *testing.T, txn *Transaction, pool *fileservice.Pool[*types.Packer], idx int) [][]byte {
		t.Helper()
		entry := &txn.writes[idx]
		var packer *types.Packer
		put := pool.Get(&packer)
		defer put.Put()
		return readutil.EncodePrimaryKeyVector(entry.bat.Vecs[entry.pkCheckPos], packer)
	}
	probe := func(t *testing.T, txn *Transaction, pool *fileservice.Pool[*types.Packer], key int64) ([]int64, issue26796Counters) {
		return issue26796PointRead(t, txn, issue26796Filter(t, pool, readutil.BasePKFilter{
			Valid: true, Op: function.EQUAL, Oid: types.T_int64,
			LB: types.EncodeFixed(key),
		}))
	}

	txn, pool := newIssue26796Txn(t)
	issue26796InsertBatch(t, txn, 7, 3, 9, 12)

	txn.Lock()
	origBatBytes := uint64(txn.writes[0].bat.Size())
	require.Equal(t, origBatBytes, txn.workspaceSize, "only the batch is charged before hydration")
	txn.Unlock()

	// hydrate the original's index
	values, d := probe(t, txn, pool, 9)
	require.Equal(t, []int64{9}, values)
	require.Equal(t, uint64(1), d.IndexBuilds)
	txn.Lock()
	charge := txn.writes[0].keyIndexAccountedSize
	require.Positive(t, charge)
	require.Equal(t, origBatBytes+charge, txn.workspaceSize)
	txn.Unlock()

	// in-transaction ALTER of a table created in this txn: every live entry
	// is duplicated under the new identity, originals fully selected
	require.NoError(t, txn.transferWorkspaceWritesForAlterLocked(issue26796TblID, issue26796Table, nil))

	txn.Lock()
	require.NoError(t, txn.checkWorkspaceAccountingLocked(),
		"the transfer must charge the duplicate's batch bytes once, without a second charge owner of the index")
	require.Len(t, txn.writes, 2)
	require.Equal(t, origBatBytes+charge+origBatBytes, txn.workspaceSize,
		"the duplicate contributes its batch bytes; its absent index contributes nothing")
	dup := txn.writes[1]
	require.Nil(t, dup.keyOffsets, "the ALTER duplicate must not share the original's hydrated map")
	require.Zero(t, dup.keyIndexAccountedSize, "the ALTER duplicate must not inherit charged index bytes")
	require.Equal(t, origBatBytes, dup.accountedSize)
	require.Equal(t, origBatBytes, uint64(dup.bat.Size()))
	require.NotNil(t, txn.writes[0].keyOffsets)
	require.Equal(t, charge, txn.writes[0].keyIndexAccountedSize,
		"ownership of the original's index and charge stays with the original")
	sels, selected := txn.batchSelectList[txn.writes[0].bat]
	require.True(t, selected)
	require.Len(t, sels, 4, "the original's rows are all marked deleted for the boundary shrink")
	txn.Unlock()

	// statement boundary: the fully-selected original shrinks away and
	// refunds its index exactly once; only the duplicate's data remains
	require.NoError(t, txn.mergeTxnWorkspaceLocked(context.Background()))
	txn.Lock()
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
	require.Nil(t, txn.writes[0].keyOffsets, "the shrunk original must have dropped its index")
	require.Zero(t, txn.writes[0].keyIndexAccountedSize)
	require.Zero(t, txn.writes[0].bat.RowCount())
	// the zero-row entry must fail closed on hydration: its index is
	// provably empty, so a charge here would book bytes no probe can ever
	// answer from
	index, built := txn.ensureInsertKeyOffsetsLocked(&txn.writes[0])
	require.False(t, built, "a zero-row entry must never hydrate an index")
	require.Nil(t, index)
	require.Zero(t, txn.writes[0].keyIndexAccountedSize)
	require.Equal(t, origBatBytes, txn.workspaceSize,
		"after the single refund the budget carries only the duplicate's batch; duplicated ownership would show as doubled bytes or an underflow")
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
	txn.Unlock()

	// the duplicate hydrates its own index under its own ownership
	values, d = probe(t, txn, pool, 9)
	require.Equal(t, []int64{9}, values)
	require.Equal(t, uint64(1), d.IndexBuilds,
		"only the duplicate's index is built: the zero-row original must fail closed, not build an empty charged index")
	txn.Lock()
	dupCharge := insertKeyOffsetsEstimateSize(keysOf(t, txn, pool, 1))
	require.Equal(t, dupCharge, txn.writes[1].keyIndexAccountedSize,
		"the duplicate's fresh charge comes from its own keys")
	require.Equal(t, origBatBytes+dupCharge, txn.workspaceSize)
	require.Nil(t, txn.writes[0].keyOffsets)
	require.Zero(t, txn.writes[0].keyIndexAccountedSize)
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
	txn.Unlock()
}

// TestIssue26796WorkspaceCompactionClearsRetiredIndexSlots proves that the
// statement-boundary compaction of txn.writes keeps retired entries
// unreachable after the slice truncates: forward compaction moves live
// entries toward the front and clears the vacated backing-array slots, so a
// later invalidation of a live entry (merge or shrink) cannot leave a stale
// Entry copy — holding a refunded key-index map or a cleaned batch — in the
// array the workspace still owns.
func TestIssue26796WorkspaceCompactionClearsRetiredIndexSlots(t *testing.T) {
	vacatedAreClear := func(t *testing.T, txn *Transaction, oldLen int, savedIndex *insertKeyOffsets) {
		t.Helper()
		slots := txn.writes[:cap(txn.writes)]
		for j := len(txn.writes); j < oldLen; j++ {
			require.Nil(t, slots[j].bat,
				"vacated slot %d must not reference a retired batch", j)
			require.Nil(t, slots[j].keyOffsets,
				"vacated slot %d must not reference a refunded key index", j)
			require.Zero(t, slots[j].keyIndexAccountedSize)
			require.NotEqual(t, savedIndex, slots[j].keyOffsets,
				"the invalidation of a moved entry must not be undone by a stale copy in slot %d", j)
		}
	}

	// 29 single-row filler entries of a second table cross the 30-entry merge
	// gate, so the statement boundary runs batch merging plus the forward
	// compaction; the last entry — hydrated and partnerless in its table —
	// moves to the front across the merged-away holes.
	txn, pool := newIssue26796TxnWithTables(t,
		issue26796TableSpec{id: 777, name: "filltbl"},
		issue26796TableSpec{id: issue26796TblID, name: issue26796Table},
	)
	for pk := int64(1); pk <= 29; pk++ {
		issue26796InsertBatchInto(t, txn, 777, "filltbl", pk)
	}
	issue26796InsertBatchInto(t, txn, issue26796TblID, issue26796Table, 1000001)
	require.Len(t, txn.writes, 30)

	// hydrate the last entry's index
	values, d := issue26796PointRead(t, txn, issue26796Filter(t, pool, readutil.BasePKFilter{
		Valid: true, Op: function.EQUAL, Oid: types.T_int64,
		LB: types.EncodeFixed(int64(1000001)),
	}))
	require.Equal(t, []int64{1000001}, values)
	require.Equal(t, uint64(1), d.IndexBuilds)

	txn.Lock()
	require.NotNil(t, txn.writes[29].keyOffsets)
	savedIndex := txn.writes[29].keyOffsets
	savedCharge := txn.writes[29].keyIndexAccountedSize
	require.Positive(t, savedCharge)
	txn.Unlock()

	// statement boundary: fillers merge into one fat batch, the hydrated
	// entry moves forward over the holes, the tail truncates
	require.NoError(t, txn.mergeTxnWorkspaceLocked(context.Background()))

	txn.Lock()
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
	require.Len(t, txn.writes, 2, "29 fillers merge into one fat batch beside the moved hydrated entry")
	moved := txn.writes[1]
	require.Same(t, savedIndex, moved.keyOffsets,
		"the moved entry keeps its hydrated index")
	require.Equal(t, savedCharge, moved.keyIndexAccountedSize)
	require.Equal(t, int64(1000001), vector.MustFixedColNoTypeCheck[int64](moved.bat.Vecs[moved.pkCheckPos])[0])
	vacatedAreClear(t, txn, 30, savedIndex)
	txn.Unlock()

	// subsequent invalidation of the moved entry: its rows are all selected
	// at the next boundary, so the index refunds and drops on its live slot
	// — vacated slots must not resurrect the map through a stale copy
	txn.Lock()
	txn.addBatchSelectionsLocked(txn.writes[1].bat, []int64{0})
	txn.Unlock()
	require.NoError(t, txn.mergeTxnWorkspaceLocked(context.Background()))

	txn.Lock()
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
	require.Nil(t, txn.writes[1].keyOffsets, "the shrunk entry's index is gone")
	require.Zero(t, txn.writes[1].keyIndexAccountedSize)
	require.Zero(t, txn.writes[1].bat.RowCount())
	vacatedAreClear(t, txn, 30, savedIndex)
	txn.Unlock()
}

// TestIssue26796DumpAllClearsVacatedSlotsThroughFullSlice proves the commit
// -time dumpBatchLocked(ctx, -1) path: the dumpAll compaction dropped entries
// into a shortened append-slice and the clearing helper indexed past that
// slice's length, so any retained hole (a DROP/TRUNCATE-released entry that
// the fewer-than-30 merge gate never compacts away) turned a commit dump
// into an out-of-range panic. The fix clears through the original
// full-length slice.
func TestIssue26796DumpAllClearsVacatedSlotsThroughFullSlice(t *testing.T) {
	// writeFileLockedWithAutoIncrEpochKnown notifies the colexec server owned
	// by the engine's service name; register one for the empty service and
	// remove it in cleanup (same contract as the teardown test).
	rt := commonruntime.ServiceRuntime("")
	colexec.NewServer("")
	t.Cleanup(func() { commonruntime.CompareAndDeleteServiceBasedRuntime("", rt) })

	txn, _ := newIssue26796Txn(t)
	issue26796InsertBatch(t, txn, 10)
	issue26796InsertBatch(t, txn, 11)
	issue26796InsertBatch(t, txn, 12)

	txn.Lock()
	// retire the third entry DROP/TRUNCATE-style: a nil hole the fewer-than-
	// 30 merge gate never compacts away, preserved by the insert dump
	txn.releaseWorkspaceEntryBatchLocked(2)
	require.Nil(t, txn.writes[2].bat)
	// any positive retained bytes cross the commit threshold so a commit
	// dump runs; the entry-count stays below the max-count gates
	txn.commitWorkspaceThreshold = 1
	txn.engine.config.insertEntryMaxCount = 1 << 30
	// the dump annotates file entries with a TN store and registers CN
	// objects under the transaction's summary map
	txn.tnStores = []metadata.TNService{{}}
	txn.cnObjsSummary = map[types.Objectid]Summary{}
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
	txn.Unlock()

	// the real commit-time dump: the two live entries are spilled to the
	// local shared file service and removed from the workspace, the hole
	// survives the insert dump truncation, and the dumpAll compaction drops
	// it — with zero kept entries before the file entry this must not index
	// the shortened append-slice out of range; the Locked contract is met
	// by holding the lock the way dumpBatch does in production
	require.NotPanics(t, func() {
		txn.Lock()
		defer txn.Unlock()
		require.NoError(t, txn.dumpBatchLocked(context.Background(), -1))
	}, "commit-time dump with a retired hole must not panic while clearing vacated slots")

	txn.Lock()
	defer txn.Unlock()
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
	require.Len(t, txn.writes, 1, "only the freshly written file entry survives")

	// preserved data: the spilled rows live in the dumped object file entry
	kept := txn.writes[0]
	require.Equal(t, INSERT, kept.typ)
	require.NotEmpty(t, kept.fileName, "the surviving table must be preserved as a spilled file entry")
	require.NotNil(t, kept.bat)
	require.Positive(t, kept.bat.RowCount())

	// cleared tail slots: nothing retired is left referenced beyond the
	// workspace length, in the region moved entries vacated
	slots := txn.writes[:cap(txn.writes)]
	for j := len(txn.writes); j < cap(txn.writes); j++ {
		require.Nil(t, slots[j].bat, "slot %d must not reference a retired batch", j)
		require.Nil(t, slots[j].keyOffsets, "slot %d must not reference a refunded key index", j)
		require.Zero(t, slots[j].keyIndexAccountedSize)
		require.Zero(t, slots[j].accountedSize)
	}
}

// TestIssue26796SpillSizingSeesHydratedKeyIndexBytes proves that every spill
// consumer sees the hydrated key-index bytes with the same in-memory entry
// size the workspace budget charges: the partial-dump scan (which gates
// spilling and sizes the quota request) and per-table spill selection. A
// retained index charge must therefore be able to push a workspace over a
// spill threshold on its own — before the fix, hydration moved
// approximateInMemInsertSize past the threshold while every sizing pass below
// still counted batch bytes only, so dumpBatchLocked kept returning early
// and the workspace never spilled or acquired quota.
func TestIssue26796SpillSizingSeesHydratedKeyIndexBytes(t *testing.T) {
	probe := func(t *testing.T, txn *Transaction, pool *fileservice.Pool[*types.Packer], key int64) ([]int64, issue26796Counters) {
		return issue26796PointRead(t, txn, issue26796Filter(t, pool, readutil.BasePKFilter{
			Valid: true, Op: function.EQUAL, Oid: types.T_int64,
			LB: types.EncodeFixed(key),
		}))
	}

	t.Run("scan_counts_index_bytes", func(t *testing.T) {
		txn, pool := newIssue26796Txn(t)
		issue26796InsertBatch(t, txn, 7, 3)  // entry 0
		issue26796InsertBatch(t, txn, 9, 12) // entry 1

		txn.Lock()
		base := uint64(txn.writes[0].bat.Size()) + uint64(txn.writes[1].bat.Size())
		require.Equal(t, base, txn.scanInMemInsertSize(0))
		txn.Unlock()

		// probe 9 only lives in entry 1's envelope, so only it hydrates
		values, d := probe(t, txn, pool, 9)
		require.Equal(t, []int64{9}, values)
		require.Equal(t, uint64(1), d.IndexBuilds)

		txn.Lock()
		charge := txn.writes[1].keyIndexAccountedSize
		require.Positive(t, charge)
		require.Equal(t, base+charge, txn.scanInMemInsertSize(0),
			"the spill scan must count the hydrated index bytes of the entries it sizes")
		txn.Unlock()
	})

	t.Run("write_threshold_crossed_by_hydration", func(t *testing.T) {
		txn, pool := newIssue26796Txn(t)
		issue26796InsertBatch(t, txn, 7, 3)
		issue26796InsertBatch(t, txn, 9, 12)

		txn.Lock()
		base := txn.scanInMemInsertSize(0)
		// Position the write threshold one byte above the batch-only workspace:
		// dumpBatchLocked's partial-dump gate must hold the workspace in
		// memory before hydration.
		txn.writeWorkspaceThreshold = base + 1
		require.Less(t, txn.scanInMemInsertSize(0), txn.writeWorkspaceThreshold,
			"pre-hydration the batch-only workspace sits below the write threshold")
		txn.Unlock()

		values, d := probe(t, txn, pool, 9)
		require.Equal(t, []int64{9}, values)
		require.Equal(t, uint64(1), d.IndexBuilds)

		txn.Lock()
		require.GreaterOrEqual(t, txn.approximateInMemInsertSize, txn.writeWorkspaceThreshold,
			"hydration must push the insert budget over the write threshold")
		require.False(t, txn.scanInMemInsertSize(0) < txn.writeWorkspaceThreshold,
			"the spill scan must also cross the threshold, or dumpBatchLocked returns early and the over-threshold workspace never spills")
		require.NoError(t, txn.checkWorkspaceAccountingLocked())
		txn.Unlock()
	})

	t.Run("table_selection_counts_index_bytes", func(t *testing.T) {
		txn, pool := newIssue26796TxnWithTables(t,
			issue26796TableSpec{id: issue26796TblID, name: issue26796Table},
			issue26796TableSpec{id: 43, name: "tblb"},
		)
		var pksA []int64
		for pk := int64(1); pk <= 50; pk++ {
			pksA = append(pksA, pk)
		}
		issue26796InsertBatchInto(t, txn, issue26796TblID, issue26796Table, pksA...)
		issue26796InsertBatchInto(t, txn, 43, "tblb", 1)

		txn.Lock()
		txn.engine.config.insertEntryMaxCount = 1 << 30
		txn.Unlock()

		// hydrate the big table's index: probe a key from its batch
		values, d := probe(t, txn, pool, 25)
		require.Equal(t, []int64{25}, values)
		require.Equal(t, uint64(1), d.IndexBuilds)

		txn.Lock()
		sizeA := uint64(txn.writes[0].bat.Size())
		sizeB := uint64(txn.writes[1].bat.Size())
		chargeA := txn.writes[0].keyIndexAccountedSize
		require.Positive(t, chargeA)
		// Between the batch-only total and the charged total, so the skipTable
		// decision flips only if the per-table size includes the charge.
		txn.commitWorkspaceThreshold = sizeA + sizeB + chargeA/2

		tbSize, tbCount, skipTable := txn.dumpInsertTableSelectionLocked(0, false)
		require.Equal(t, int(sizeA+chargeA), tbSize[issue26796TblID],
			"per-table spill selection must size the hydrated index bytes with the batch")
		require.Equal(t, int(sizeB), tbSize[43])
		require.Equal(t, 50, tbCount[issue26796TblID])
		require.NotContains(t, skipTable, uint64(issue26796TblID),
			"the index charge must push its table over the commit threshold: it must be spilled, not kept in memory")
		require.Contains(t, skipTable, uint64(43),
			"the small unhydrated table stays below the commit threshold and is skipped")
		require.NoError(t, txn.checkWorkspaceAccountingLocked())
		txn.Unlock()
	})
}
