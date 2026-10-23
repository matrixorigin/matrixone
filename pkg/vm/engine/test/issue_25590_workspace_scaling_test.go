// Copyright 2021 - 2024 MatrixOne
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

package test

import (
	"context"
	"os"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/stretchr/testify/require"
)

// Issue 25590: long transactions whose statements repeatedly supersede the
// same row (UPDATE / REPLACE / DELETE+INSERT pairs all delete the previous
// version by rowid and insert the next one) must keep per-statement cost and
// workspace size proportional to the live working set. Superseded zero-row
// workspace entries are retired and compacted at statement boundaries;
// without that, every workspace consumer (statement merging, workspace
// delete resolution, reader filters, tombstone collection) rescans one
// inert entry per mutation and per-statement cost grows with the whole
// superseded history.
//
// Each statement below drives the production statement lifecycle through the
// real disttae+TAE engine exactly as the frontend/compile drive a user
// statement (frontend executeStmtWithWorkspace/stmtIDIncrease and compile
// txnOffsetOfCompile):
//
//	StartStatement -> IncrStatementID (admission: merges the previous
//	statement's delete markings and records the boundary) ->
//	UpdateSnapshotWriteOffset (the boundary is published BEFORE statement
//	execution; txnTable.Write dumps from that offset) -> execution ops
//	(delete of the previous version by rowid, insert of the next version)
//	-> EndStatement.
//
// The workloads start from a COMMITTED seed row (insert into point_t
// values(1,0) committed by an earlier transaction), so the first mutation
// deletes a committed row — a real DELETE/tombstone entry — and every later
// mutation supersedes a row this transaction inserted — the workspace-resident
// delete-marking path the fix retires at statement boundaries. Reader
// visibility is asserted by draining the reader to end-of-stream: a drained
// read is the only oracle strong enough to catch an extra persisted version
// behind a correct first in-memory batch.

const (
	issue25590DB       = "db_issue_25590"
	issue25590Table    = "point_t"
	issue25590Stmts    = 2000
	issue25590Pairs    = 1000
	issue25590WriteCap = 8 // entries the 1-row live working set may occupy
)

func issue25590InsertRow(t *testing.T, mp *mpool.MPool, pk, v int32) *batch.Batch {
	t.Helper()
	bat := batch.NewWithSize(2)
	bat.Attrs = []string{"mock_0", "mock_1"}
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], pk, false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[1], v, false, mp))
	bat.SetRowCount(1)
	return bat
}

// issue25590DeleteRow builds the production delete-by-rowid batch: rowid
// plus the primary-key value column, as the lock operator emits for UPDATE,
// REPLACE and DELETE statements (see disttae delete batches: Row_ID +
// TableTailAttrPKVal).
func issue25590DeleteRow(t *testing.T, mp *mpool.MPool, rowid types.Rowid, pk int32) *batch.Batch {
	t.Helper()
	bat := batch.NewWithSize(2)
	bat.Attrs = []string{catalog.Row_ID, catalog.TableTailAttrPKVal}
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], rowid, false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[1], pk, false, mp))
	bat.SetRowCount(1)
	return bat
}

// engine-level statement admission and teardown: the production
// frontend/compile sequence around statement execution.
func issue25590AdmitStatement(t *testing.T, ctx context.Context, txn client.TxnOperator) {
	t.Helper()
	ws := txn.GetWorkspace()
	ws.StartStatement()
	require.NoError(t, ws.IncrStatementID(ctx, false))
	// compile.go txnOffsetOfCompile publishes the boundary before
	// execution; txnTable.Write dumps from it.
	ws.UpdateSnapshotWriteOffset()
}

func issue25590EndStatement(t *testing.T, txn client.TxnOperator) {
	t.Helper()
	txn.GetWorkspace().EndStatement()
}

// issue25590DeleteRowByRowid deletes a row version by rowid through the
// production relation API. The CALLER owns the input batch, exactly like
// the executor does: relation.Delete shrinks/transfers the input without
// cleaning it, so the helper scopes cleanup lexically around the call.
func issue25590DeleteRowByRowid(
	t *testing.T,
	ctx context.Context,
	mp *mpool.MPool,
	relation engine.Relation,
	rowid types.Rowid,
	pk int32,
) {
	t.Helper()
	del := issue25590DeleteRow(t, mp, rowid, pk)
	defer del.Clean(mp)
	require.NoError(t, relation.Delete(ctx, del, catalog.Row_ID))
}

// issue25590InsertRowViaRelation inserts the next row version through the
// production relation API. txnTable.Write copies its input, so the caller
// owns the input batch and cleans it lexically.
func issue25590InsertRowViaRelation(
	t *testing.T,
	ctx context.Context,
	mp *mpool.MPool,
	relation engine.Relation,
	pk, v int32,
) {
	t.Helper()
	ins := issue25590InsertRow(t, mp, pk, v)
	defer ins.Clean(mp)
	require.NoError(t, relation.Write(ctx, ins))
}

// issue25590NextStatementBoundary closes the workspace the way production
// does after the last statement: Commit calls IncrStatementID(ctx, true)
// first, and any subsequent statement admits the same way, which applies the
// final statement's pending delete markings. The pre-commit asserted state
// is therefore the state every reader and the commit path itself observe.
func issue25590NextStatementBoundary(t *testing.T, ctx context.Context, txn client.TxnOperator) {
	t.Helper()
	ws := txn.GetWorkspace()
	ws.StartStatement()
	require.NoError(t, ws.IncrStatementID(ctx, false))
	ws.UpdateSnapshotWriteOffset()
	ws.EndStatement()
}

// issue25590WorkspaceStats reports the total number of workspace entries
// and the number of zero-row (superseded but unretired) entries for the
// given user table.
func issue25590WorkspaceStats(t *testing.T, txn client.TxnOperator, dbID, tableID uint64) (entries, zeroRows int) {
	t.Helper()
	dtxn, ok := txn.GetWorkspace().(*disttae.Transaction)
	require.True(t, ok, "workspace must be *disttae.Transaction")
	txnValue := reflect.ValueOf(dtxn).Elem()
	writesField := txnValue.FieldByName("writes")
	require.True(t, writesField.IsValid(), "writes field must exist")
	entries = writesField.Len()
	dtxn.ForEachTableWrites(dbID, tableID, entries, func(entry disttae.Entry) {
		if entry.Bat() == nil {
			return
		}
		if entry.Bat().RowCount() == 0 {
			zeroRows++
		}
	})
	return entries, zeroRows
}

func issue25590LiveRows(t *testing.T, txn client.TxnOperator, dbID, tableID uint64) int {
	t.Helper()
	dtxn := txn.GetWorkspace().(*disttae.Transaction)
	rows := 0
	dtxn.ForEachTableWrites(dbID, tableID, int(dtxn.WriteOffset()), func(entry disttae.Entry) {
		if entry.Type() != disttae.INSERT || entry.Bat() == nil || entry.FileName() != "" {
			return
		}
		rows += entry.Bat().RowCount()
	})
	return rows
}

// issue25590UpdateStatement runs one UPDATE-shaped statement (delete the
// previous version by rowid, insert the next version) under the production
// statement admission order, and returns the rowid of the inserted row.
// prevIsCommitted distinguishes the first mutation of the committed seed row
// (a real tombstone entry) from the in-transaction supersede path (workspace
// delete markings, the issue-25590 mechanism).
func issue25590UpdateStatement(
	t *testing.T,
	ctx context.Context,
	mp *mpool.MPool,
	txn client.TxnOperator,
	relation engine.Relation,
	prevRowid types.Rowid,
	v int32,
) types.Rowid {
	t.Helper()
	issue25590AdmitStatement(t, ctx, txn)
	defer issue25590EndStatement(t, txn)

	issue25590DeleteRowByRowid(t, ctx, mp, relation, prevRowid, 1)
	issue25590InsertRowViaRelation(t, ctx, mp, relation, 1, v)
	return issue25590LastInsertRowid(t, ctx, txn, relation)
}

func issue25590LastInsertRowid(t *testing.T, ctx context.Context, txn client.TxnOperator, relation engine.Relation) types.Rowid {
	t.Helper()
	dtxn := txn.GetWorkspace().(*disttae.Transaction)
	var inserted types.Rowid
	found := false
	dtxn.ForEachTableWrites(
		relation.GetDBID(ctx), relation.GetTableID(ctx), int(dtxn.WriteOffset()),
		func(entry disttae.Entry) {
			if entry.Type() != disttae.INSERT || entry.Bat() == nil ||
				entry.FileName() != "" || entry.Bat().RowCount() == 0 {
				return
			}
			rids := vector.MustFixedColWithTypeCheck[types.Rowid](entry.Bat().GetVector(0))
			// entries iterate in index order; the last live INSERT entry
			// belongs to this statement
			inserted = rids[len(rids)-1]
			found = true
		})
	require.True(t, found, "statement must insert exactly one workspace row")
	return inserted
}

// issue25590SeedCommittedRow seeds the row (id=1, v=0) through the engine in
// its own committed transaction — the "insert into point_t values(1,0)" +
// commit that precedes the issue's update transaction — and returns the
// committed row's rowid, read back through the TAE storage exactly the way
// the lock/scan operators see committed data.
func issue25590SeedCommittedRow(
	t *testing.T,
	ctx context.Context,
	de *testutil.TestDisttaeEngine,
	tae *testutil.TestTxnStorage,
	mp *mpool.MPool,
) (dbID, tableID uint64, seedRowid types.Rowid) {
	t.Helper()
	_, rel, seedTxn, err := de.GetTable(ctx, issue25590DB, issue25590Table)
	require.NoError(t, err)
	issue25590InsertRowViaRelation(t, ctx, mp, rel, 1, 0)
	require.NoError(t, seedTxn.Commit(ctx))

	dbID, tableID = rel.GetDBID(ctx), rel.GetTableID(ctx)

	tnTxn, err := tae.StartTxn()
	require.NoError(t, err)
	userDB, err := tnTxn.GetDatabaseByID(dbID)
	require.NoError(t, err)
	userTbl, err := userDB.GetRelationByID(tableID)
	require.NoError(t, err)
	id, row, err := userTbl.GetByFilter(ctx, handle.NewEQFilter(int32(1)))
	require.NoError(t, err)
	return dbID, tableID, objectio.NewRowid(&id.BlockID, row)
}

func issue25590StmtCount(t *testing.T, dflt int) int {
	t.Helper()
	if v := os.Getenv("ISSUE25590_ENGINE_STMTS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return dflt
}

// issue25590ReadTableRows drains the production reader for the table to
// end-of-stream, returning every visible row's (pk, v). A single Read call
// is not a sufficient oracle: LocalDisttaeDataSource serves in-memory
// uncommitted rows before persisted blocks, so a stale persisted version
// would only surface in a later batch.
func issue25590ReadTableRows(
	t *testing.T,
	ctx context.Context,
	de *testutil.TestDisttaeEngine,
	txn client.TxnOperator,
	relation engine.Relation,
	mp *mpool.MPool,
	schema *catalog2.Schema,
) (pks, vs []int32) {
	t.Helper()
	reader, err := testutil.GetRelationReader(ctx, de, txn, relation, nil, mp, t)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, reader.Close())
	}()
	for {
		ret := testutil.EmptyBatchFromSchema(schema, 0, 1)
		isEnd, err := reader.Read(ctx, ret.Attrs, nil, mp, ret)
		if err != nil {
			ret.Clean(mp)
			require.NoError(t, err)
		}
		if rc := ret.RowCount(); rc > 0 {
			pkCol := vector.MustFixedColWithTypeCheck[int32](ret.Vecs[0])
			vCol := vector.MustFixedColWithTypeCheck[int32](ret.Vecs[1])
			for i := 0; i < rc; i++ {
				pks = append(pks, pkCol[i])
				vs = append(vs, vCol[i])
			}
		}
		ret.Clean(mp)
		if isEnd {
			return pks, vs
		}
	}
}

// issue25590AssertSoleVisibleRow asserts the drained reader saw exactly one
// row, carrying pk id=1 and the expected value.
func issue25590AssertSoleVisibleRow(t *testing.T, pks, vs []int32, wantV int32, where string) {
	t.Helper()
	require.Len(t, pks, 1,
		"%s: exactly one row with id=1 must be visible, got %v (v=%v)", where, pks, vs)
	require.Equal(t, int32(1), pks[0], "%s: the sole visible row must carry pk id=1", where)
	require.Equal(t, wantV, vs[0], "%s: the sole visible row must hold v=%d", where, wantV)
}

// issue25590AssertWorkspaceBounded checks the structural invariant of the
// fix: the workspace holds no zero-row entries and stays bounded by the live
// working set after any number of same-row superseding mutations.
func issue25590AssertWorkspaceBounded(
	t *testing.T,
	txn client.TxnOperator,
	dbID, tableID uint64,
	liveRows int,
) {
	t.Helper()
	entries, zeroRows := issue25590WorkspaceStats(t, txn, dbID, tableID)
	t.Logf("workspace after workload: entries=%d, live rows=%d, zero-row entries=%d",
		entries, issue25590LiveRows(t, txn, dbID, tableID), zeroRows)
	require.Equal(t, liveRows, issue25590LiveRows(t, txn, dbID, tableID),
		"the live working set must be the only thing kept in memory")
	require.Zero(t, zeroRows, "superseded versions must not leave zero-row entries behind")
	require.LessOrEqual(t, entries, issue25590WriteCap,
		"workspace must stay bounded by the live working set, got %d entries",
		entries)
}

// TestIssue25590RepeatedSameRowMutationsBounded drives the issue's reported
// workload against committed data: point updates of the same primary-key row
// as separate statements inside one long explicit transaction, where the
// first update replaces a row committed by an earlier transaction. It is the
// engine-level reproduction of the reported 5k/10k/20k/30k UPDATE loop.
func TestIssue25590RepeatedSameRowMutationsBounded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)

	schema := catalog2.MockSchema(2, 0)
	schema.Name = issue25590Table
	disttaeEngine, taeEngine, rpcAgent, mp := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	// Engine creation needs a context without a deadline; every subsequent
	// txn operation on the mock TAE requires one.
	ctx, cancel = context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, issue25590DB, issue25590Table, schema)
	require.NoError(t, err)

	dbID, tableID, seedRowid := issue25590SeedCommittedRow(t, ctx, disttaeEngine, taeEngine, mp)

	// The long update transaction: the reported workload, one statement
	// per supersede of the row.
	_, rel, txn, err := disttaeEngine.GetTable(ctx, issue25590DB, issue25590Table)
	require.NoError(t, err)

	stmtCount := issue25590StmtCount(t, issue25590Stmts)
	durations := make([]time.Duration, stmtCount)
	prev := seedRowid
	require.False(t, prev.BorrowSegmentID().EQ(&colexec.TxnWorkspaceSegment),
		"the committed seed row must live in persisted storage")
	for i := 1; i <= stmtCount; i++ {
		begin := time.Now()
		prev = issue25590UpdateStatement(t, ctx, mp, txn, rel, prev, int32(i))
		require.True(t, prev.BorrowSegmentID().EQ(&colexec.TxnWorkspaceSegment),
			"rows inserted by statements of this transaction must use workspace rowids")
		durations[i-1] = time.Since(begin)
	}

	logIssue25590TimingEng(t, "engine-level update loop (committed seed)", durations)
	issue25590NextStatementBoundary(t, ctx, txn)
	issue25590AssertWorkspaceBounded(t, txn, dbID, tableID, 1)

	// In-transaction visibility of the whole drained read: exactly one row,
	// id=1, v=N — the committed seed version must be hidden by the
	// transaction's tombstone for it.
	pks, vs := issue25590ReadTableRows(t, ctx, disttaeEngine, txn, rel, mp, schema)
	issue25590AssertSoleVisibleRow(t, pks, vs, int32(stmtCount), "in-txn read")

	require.NoError(t, txn.Commit(ctx))

	// Fresh-transaction read of the committed state: still exactly one row
	// with the final value.
	_, rel2, txn2, err := disttaeEngine.GetTable(ctx, issue25590DB, issue25590Table)
	require.NoError(t, err)
	pks, vs = issue25590ReadTableRows(t, ctx, disttaeEngine, txn2, rel2, mp, schema)
	issue25590AssertSoleVisibleRow(t, pks, vs, int32(stmtCount), "post-commit read")
	require.NoError(t, txn2.Commit(ctx))
}

// TestIssue25590DeleteInsertPairsBounded drives the reported DELETE+INSERT
// pairs variant: each pair is two separate statements — one deleting the
// current version of the row, the next inserting the replacement — inside
// one long transaction. The final quarter of the loop must not cost more
// than the first, and the workspace must stay bounded.
func TestIssue25590DeleteInsertPairsBounded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)

	schema := catalog2.MockSchema(2, 0)
	schema.Name = issue25590Table
	disttaeEngine, taeEngine, rpcAgent, mp := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, issue25590DB, issue25590Table, schema)
	require.NoError(t, err)

	dbID, tableID, seedRowid := issue25590SeedCommittedRow(t, ctx, disttaeEngine, taeEngine, mp)

	_, rel, txn, err := disttaeEngine.GetTable(ctx, issue25590DB, issue25590Table)
	require.NoError(t, err)

	pairs := issue25590Pairs
	if stmtCount := issue25590StmtCount(t, 0); stmtCount > 0 {
		pairs = stmtCount / 2
	}
	durations := make([]time.Duration, 2*pairs)
	prev := seedRowid
	for i := 1; i <= pairs; i++ {
		// DELETE statement: supersede the current version only.
		begin := time.Now()
		issue25590AdmitStatement(t, ctx, txn)
		issue25590DeleteRowByRowid(t, ctx, mp, rel, prev, 1)
		issue25590EndStatement(t, txn)
		durations[2*(i-1)] = time.Since(begin)

		// INSERT statement: the replacement version.
		begin = time.Now()
		issue25590AdmitStatement(t, ctx, txn)
		issue25590InsertRowViaRelation(t, ctx, mp, rel, 1, int32(i))
		issue25590EndStatement(t, txn)
		prev = issue25590LastInsertRowid(t, ctx, txn, rel)
		require.True(t, prev.BorrowSegmentID().EQ(&colexec.TxnWorkspaceSegment),
			"rows inserted by statements of this transaction must use workspace rowids")
		durations[2*i-1] = time.Since(begin)
	}

	logIssue25590TimingEng(t, "engine-level DELETE+INSERT pairs (committed seed)", durations)
	issue25590NextStatementBoundary(t, ctx, txn)
	issue25590AssertWorkspaceBounded(t, txn, dbID, tableID, 1)

	pks, vs := issue25590ReadTableRows(t, ctx, disttaeEngine, txn, rel, mp, schema)
	issue25590AssertSoleVisibleRow(t, pks, vs, int32(pairs), "in-txn read after pairs")

	require.NoError(t, txn.Commit(ctx))

	_, rel2, txn2, err := disttaeEngine.GetTable(ctx, issue25590DB, issue25590Table)
	require.NoError(t, err)
	pks, vs = issue25590ReadTableRows(t, ctx, disttaeEngine, txn2, rel2, mp, schema)
	issue25590AssertSoleVisibleRow(t, pks, vs, int32(pairs), "post-commit read after pairs")
	require.NoError(t, txn2.Commit(ctx))
}

// TestIssue25590RollbackAfterLongHistory fails one statement after a long
// same-row update history over a committed seed row and verifies statement
// rollback still restores the exact pre-statement version: retired-slot
// compaction and the mid-workload tombstone must never corrupt the recorded
// statement boundaries that rollback truncation consumes.
func TestIssue25590RollbackAfterLongHistory(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)

	schema := catalog2.MockSchema(2, 0)
	schema.Name = issue25590Table
	disttaeEngine, taeEngine, rpcAgent, mp := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, issue25590DB, issue25590Table, schema)
	require.NoError(t, err)

	dbID, tableID, seedRowid := issue25590SeedCommittedRow(t, ctx, disttaeEngine, taeEngine, mp)

	_, rel, txn, err := disttaeEngine.GetTable(ctx, issue25590DB, issue25590Table)
	require.NoError(t, err)

	const prelude = 150
	prev := seedRowid
	for i := 1; i <= prelude; i++ {
		prev = issue25590UpdateStatement(t, ctx, mp, txn, rel, prev, int32(i))
	}

	// Failing statement: full production admission, then a full mutation
	// that would raise v to prelude+1, then the statement rolls back.
	issue25590AdmitStatement(t, ctx, txn)
	issue25590DeleteRowByRowid(t, ctx, mp, rel, prev, 1)
	issue25590InsertRowViaRelation(t, ctx, mp, rel, 1, prelude+1)
	require.NoError(t, txn.GetWorkspace().RollbackLastStatement(ctx))
	issue25590EndStatement(t, txn)

	// Drained visibility: exactly the pre-statement version survived.
	pks, vs := issue25590ReadTableRows(t, ctx, disttaeEngine, txn, rel, mp, schema)
	issue25590AssertSoleVisibleRow(t, pks, vs, int32(prelude), "read after statement rollback")
	issue25590AssertWorkspaceBounded(t, txn, dbID, tableID, 1)

	// The transaction continues normally afterwards and commits.
	issue25590UpdateStatement(t, ctx, mp, txn, rel, prev, prelude+1)
	issue25590NextStatementBoundary(t, ctx, txn)
	pks, vs = issue25590ReadTableRows(t, ctx, disttaeEngine, txn, rel, mp, schema)
	issue25590AssertSoleVisibleRow(t, pks, vs, int32(prelude+1), "read after continued txn")
	require.NoError(t, txn.Commit(ctx))

	_, rel2, txn2, err := disttaeEngine.GetTable(ctx, issue25590DB, issue25590Table)
	require.NoError(t, err)
	pks, vs = issue25590ReadTableRows(t, ctx, disttaeEngine, txn2, rel2, mp, schema)
	issue25590AssertSoleVisibleRow(t, pks, vs, int32(prelude+1), "post-commit read after rollback")
	require.NoError(t, txn2.Commit(ctx))
}

// TestIssue25590DumpFailureCommitPreservesEveryStatement drives the round-3
// review scenario end to end against committed data: after one successful
// replacing statement, the NEXT admission's workspace dump fails inside its
// fallible storage publication while the earlier statement's rows are
// staged. The failed publication must retain them, the frontend's statement
// rollback on the admission error must leave the exact version readable, and
// continued execution plus COMMIT must preserve the whole mutation chain.
func TestIssue25590DumpFailureCommitPreservesEveryStatement(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)

	schema := catalog2.MockSchema(2, 0)
	schema.Name = issue25590Table
	disttaeEngine, taeEngine, rpcAgent, mp := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, issue25590DB, issue25590Table, schema)
	require.NoError(t, err)

	dbID, tableID, seedRowid := issue25590SeedCommittedRow(t, ctx, disttaeEngine, taeEngine, mp)

	_, rel, txn, err := disttaeEngine.GetTable(ctx, issue25590DB, issue25590Table)
	require.NoError(t, err)

	// Statement 1 replaces the committed seed row (v=301) under default
	// thresholds: no dump runs, so its insert row is staged in memory.
	prev := issue25590UpdateStatement(t, ctx, mp, txn, rel, seedRowid, 301)

	// Arm a deterministic Sync failure inside the next admission's insert
	// dump publication, and force the dump to engage while that staged
	// row is still in memory.
	fault.Enable()
	t.Cleanup(func() { fault.Disable() })
	t.Cleanup(func() {
		fault.RemoveFaultPoint(context.Background(), objectio.FJ_CNDumpInsertSyncFailed)
		fault.RemoveFaultPoint(context.Background(), objectio.FJ_CNWorkspaceForceFlush)
	})
	require.NoError(t, fault.AddFaultPoint(
		context.Background(), objectio.FJ_CNDumpInsertSyncFailed, "1:1::", "echo", 0, "", false))
	require.NoError(t, fault.AddFaultPoint(
		context.Background(), objectio.FJ_CNWorkspaceForceFlush, ":::", "return", 0, "", false))

	// Statement 2's admission dumps statement 1's staged rows and fails
	// inside the storage publication. Exactly the way the frontend handles
	// an admission error, the statement rolls back without touching the
	// earlier statements' history.
	ws := txn.GetWorkspace()
	ws.StartStatement()
	err = ws.IncrStatementID(ctx, false)
	require.ErrorContains(t, err, "insert dump sync failed")
	require.NoError(t, ws.RollbackLastStatement(ctx))
	ws.EndStatement()

	// The failed publication must have retained every staged source. The
	// probe publishes the reader window the way the next statement's
	// compile does, then drains the reader: had the staged insert been
	// discarded without its replacement, this read would show the
	// committed seed value or nothing at all.
	ws.UpdateSnapshotWriteOffset()
	pks, vs := issue25590ReadTableRows(t, ctx, disttaeEngine, txn, rel, mp, schema)
	issue25590AssertSoleVisibleRow(t, pks, vs, 301, "read after failed dump admission")
	issue25590AssertWorkspaceBounded(t, txn, dbID, tableID, 1)

	// Disarm the dump drivers: from here the transaction continues the way
	// a default-configured frontend flow would, with the retained rows
	// publishing at commit.
	fault.RemoveFaultPoint(context.Background(), objectio.FJ_CNWorkspaceForceFlush)
	fault.RemoveFaultPoint(context.Background(), objectio.FJ_CNDumpInsertSyncFailed)

	// Continued execution: two more statements extend the chain over the
	// statement the failed dump admission kept.
	prev = issue25590UpdateStatement(t, ctx, mp, txn, rel, prev, 302)
	ws.UpdateSnapshotWriteOffset()
	pks, vs = issue25590ReadTableRows(t, ctx, disttaeEngine, txn, rel, mp, schema)
	issue25590AssertSoleVisibleRow(t, pks, vs, 302, "read after failed dump recovery")

	issue25590UpdateStatement(t, ctx, mp, txn, rel, prev, 303)
	issue25590NextStatementBoundary(t, ctx, txn)
	issue25590AssertWorkspaceBounded(t, txn, dbID, tableID, 1)
	pks, vs = issue25590ReadTableRows(t, ctx, disttaeEngine, txn, rel, mp, schema)
	issue25590AssertSoleVisibleRow(t, pks, vs, 303, "read before commit")

	require.NoError(t, txn.Commit(ctx))

	// Fresh-transaction read of the committed state: the whole chain made
	// it through the failed admission, the rollback, and the commit.
	_, rel2, txn2, err := disttaeEngine.GetTable(ctx, issue25590DB, issue25590Table)
	require.NoError(t, err)
	pks, vs = issue25590ReadTableRows(t, ctx, disttaeEngine, txn2, rel2, mp, schema)
	issue25590AssertSoleVisibleRow(t, pks, vs, 303, "post-commit read after dump failure")
	require.NoError(t, txn2.Commit(ctx))
}

// issue25590DeletedBlocksEntries reports how many blocks hold pending
// in-transaction deletions (the object-compaction input of the merge).
func issue25590DeletedBlocksEntries(dtxn *disttae.Transaction) int {
	reflectDB := reflect.ValueOf(dtxn).
		Elem().FieldByName("deletedBlocks").Elem().FieldByName("offsets")
	return reflectDB.Len()
}

// issue25590ArmEngineFault armors a process-global fault point for the
// round-4 failure-atomic admission scenarios, mirroring the white-box
// helper; the returned disarm removes just the point, cleanup disables
// injection.
func issue25590ArmEngineFault(t *testing.T, key string) (disarm func()) {
	t.Helper()
	fault.Enable()
	t.Cleanup(func() {
		fault.Disable()
	})
	t.Cleanup(func() {
		fault.RemoveFaultPoint(context.Background(), key)
	})
	require.NoError(t, fault.AddFaultPoint(
		context.Background(), key, ":::", "echo", 0, "", false))
	return func() {
		fault.RemoveFaultPoint(context.Background(), key)
	}
}

// TestIssue25590MergeFailureKeepsPriorStatements drives the round-4 review
// scenario end to end: an in-transaction insert publishes to a CN object,
// the next successful statement deletes the flushed rows (registering
// pending deletedBlocks) and writes replacements, and the FOLLOWING
// admission fails inside the merge's fallible object-compaction lookup.
// Failure-atomic admission bookkeeping must let the frontend statement
// rollback keep every earlier successful statement exact: the drained read
// still shows their replacements (the pending deletions survived the
// rollback), continuation statements work, and the committed transaction
// retains every row.
func TestIssue25590MergeFailureKeepsPriorStatements(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)

	schema := catalog2.MockSchema(2, 0)
	schema.Name = issue25590Table
	disttaeEngine, taeEngine, rpcAgent, mp := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, issue25590DB, issue25590Table, schema)
	require.NoError(t, err)

	_, _, seedRowid := issue25590SeedCommittedRow(t, ctx, disttaeEngine, taeEngine, mp)

	_, rel, txn, err := disttaeEngine.GetTable(ctx, issue25590DB, issue25590Table)
	require.NoError(t, err)

	// Statement 1 writes pk=2 as a raw workspace row.
	issue25590AdmitStatement(t, ctx, txn)
	issue25590InsertRowViaRelation(t, ctx, mp, rel, 2, 21)
	r2 := issue25590LastInsertRowid(t, ctx, txn, rel)
	require.True(t, r2.BorrowSegmentID().EQ(&colexec.TxnWorkspaceSegment),
		"the raw insert must use a workspace rowid")
	issue25590EndStatement(t, txn)

	// Statement 2's admission publishes pk=2's row into a CN object (the
	// forced workspace flush), so the table carries mixed raw and flushed
	// in-txn rows. The single-row object puts the row at block offset 0.
	flushDisarm := issue25590ArmEngineFault(t, objectio.FJ_CNWorkspaceForceFlush)
	issue25590AdmitStatement(t, ctx, txn)
	flushDisarm()
	blocks := issue25590FlushedBlocks(t, ctx, txn, rel)
	require.NotEmpty(t, blocks,
		"statement 2's admission must publish the raw insert to a CN object")
	flushedRowid := types.NewRowid(&blocks[0], 0)
	require.False(t, flushedRowid.BorrowSegmentID().EQ(&colexec.TxnWorkspaceSegment),
		"the published row carries the object's own rowid")

	// Statement 2 (the earlier successful statement of the review
	// scenario): delete the flushed row — registering pending deletedBlocks
	// — delete the committed seed row, and insert replacements.
	issue25590DeleteRowByRowid(t, ctx, mp, rel, flushedRowid, 2)
	issue25590DeleteRowByRowid(t, ctx, mp, rel, seedRowid, 1)
	issue25590InsertRowViaRelation(t, ctx, mp, rel, 1, 101)
	issue25590InsertRowViaRelation(t, ctx, mp, rel, 2, 202)
	issue25590EndStatement(t, txn)
	dtxn := txn.GetWorkspace().(*disttae.Transaction)
	require.Equal(t, 1, issue25590DeletedBlocksEntries(dtxn),
		"the flushed-row delete must register a pending block deletion")

	// Statement 3's admission fails inside the merge's fallible
	// object-compaction table lookup; the frontend rolls the failed
	// statement back exactly the way pkg/frontend/txn.go does on an
	// admission error.
	ws := txn.GetWorkspace()
	compactDisarm := issue25590ArmEngineFault(t, objectio.FJ_CNReenterSnapshotOffsetOnGetTable)
	ws.StartStatement()
	err = ws.IncrStatementID(ctx, false)
	require.ErrorContains(t, err, "reenter snapshot write offset")
	require.Equal(t, 1, issue25590DeletedBlocksEntries(dtxn),
		"the failed admission must leave the pending block deletions in place")
	require.NoError(t, ws.RollbackLastStatement(ctx))
	ws.EndStatement()
	require.Equal(t, 1, issue25590DeletedBlocksEntries(dtxn),
		"the rollback after an interrupted admission keeps the pending block deletions")

	// Exact visibility after the rollback: the replacements of the two
	// successful statements' rows are the only visible data. A merge
	// failure that suppressed the boundary or let the rollback wipe the
	// pending deletedBlocks would resurrect the flushed (2,21) row or the
	// committed (1,0) seed here.
	ws.UpdateSnapshotWriteOffset()
	pks, vs := issue25590ReadTableRows(t, ctx, disttaeEngine, txn, rel, mp, schema)
	require.ElementsMatch(t, []int32{1, 2}, pks, "read after merge-failure rollback: pks")
	require.ElementsMatch(t, []int32{101, 202}, vs, "read after merge-failure rollback: vs")

	// Continued execution: the retried admission completes the interrupted
	// object compaction and new statements extend the transaction.
	compactDisarm()
	issue25590AdmitStatement(t, ctx, txn)
	issue25590InsertRowViaRelation(t, ctx, mp, rel, 3, 303)
	issue25590EndStatement(t, txn)
	ws.UpdateSnapshotWriteOffset()
	pks, vs = issue25590ReadTableRows(t, ctx, disttaeEngine, txn, rel, mp, schema)
	require.ElementsMatch(t, []int32{1, 2, 3}, pks, "read after continuation: pks")
	require.ElementsMatch(t, []int32{101, 202, 303}, vs, "read after continuation: vs")

	// COMMIT preserves every earlier successful statement through the
	// failed admission and the rollback.
	require.NoError(t, txn.Commit(ctx))

	_, rel2, txn2, err := disttaeEngine.GetTable(ctx, issue25590DB, issue25590Table)
	require.NoError(t, err)
	pks, vs = issue25590ReadTableRows(t, ctx, disttaeEngine, txn2, rel2, mp, schema)
	require.ElementsMatch(t, []int32{1, 2, 3}, pks, "post-commit read: pks")
	require.ElementsMatch(t, []int32{101, 202, 303}, vs, "post-commit read: vs")
	require.NoError(t, txn2.Commit(ctx))
}

// issue25590FlushedBlocks extracts the block ids of the in-transaction rows
// published to CN objects, from the workspace's block-info meta entries —
// the same blocks the scan pipeline derives reader-visible rowids for.
func issue25590FlushedBlocks(
	t *testing.T,
	ctx context.Context,
	txn client.TxnOperator,
	relation engine.Relation,
) []types.Blockid {
	t.Helper()
	dtxn := txn.GetWorkspace().(*disttae.Transaction)
	var out []types.Blockid
	dtxn.ForEachTableWrites(
		relation.GetDBID(ctx), relation.GetTableID(ctx),
		int(dtxn.WriteOffset()), func(entry disttae.Entry) {
			if entry.Type() != disttae.INSERT || entry.Bat() == nil ||
				entry.Bat().Attrs[0] != catalog.BlockMeta_BlockInfo ||
				entry.Bat().RowCount() == 0 {
				return
			}
			infoCol, infoArea := vector.MustVarlenaRawData(entry.Bat().Vecs[0])
			for _, info := range infoCol {
				out = append(out,
					objectio.DecodeBlockInfo(info.GetByteSlice(infoArea)).BlockID)
			}
		})
	return out
}

func logIssue25590TimingEng(t *testing.T, label string, durations []time.Duration) {
	t.Helper()
	q := len(durations) / 4
	if q == 0 {
		return
	}
	var first, last, total time.Duration
	for _, d := range durations {
		total += d
	}
	for _, d := range durations[:q] {
		first += d
	}
	for _, d := range durations[len(durations)-q:] {
		last += d
	}
	us := func(d time.Duration, n int) float64 {
		return float64(d) / float64(n) / float64(time.Microsecond)
	}
	t.Logf("%s: statements=%d mean=%.1fus first-quarter=%.1fus last-quarter=%.1fus ratio=%.2fx",
		label, len(durations), us(total, len(durations)), us(first, q), us(last, q),
		us(last, q)/us(first, q))
}

// TestIssue25590MergeRowidAllocFailureRecoversAndCommits is the engine-level
// leg of the round-6 review: thirty-one one-row inserts of one table make the
// next admission's >=30-entries merge pass stage their combination, and a
// failure at the combined batch's rowid regeneration must preserve every
// source exactly — the old in-place flow had already merged the sources by
// that point, destroying the one-block-per-batch and zero-based-offset
// invariants that workspace reads and the workspace delete scans rely on.
// After the frontend-style statement rollback the rows stay readable, the
// disarmed retry publishes one merged batch with a freshly regenerated
// consistent rowid column, a production delete through that batch works, and
// the commit plus a fresh-transaction read keep every surviving row.
func TestIssue25590MergeRowidAllocFailureRecoversAndCommits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)

	schema := catalog2.MockSchema(2, 0)
	schema.Name = issue25590Table
	disttaeEngine, taeEngine, rpcAgent, mp := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, issue25590DB, issue25590Table, schema)
	require.NoError(t, err)

	_, rel, txn, err := disttaeEngine.GetTable(ctx, issue25590DB, issue25590Table)
	require.NoError(t, err)
	dbID, tableID := rel.GetDBID(ctx), rel.GetTableID(ctx)
	require.NoError(t, txn.Commit(ctx))

	_, rel, txn, err = disttaeEngine.GetTable(ctx, issue25590DB, issue25590Table)
	require.NoError(t, err)

	// One statement writing thirty-one separate single-row entries of the
	// same table: the next admission's merge pass must combine them.
	issue25590AdmitStatement(t, ctx, txn)
	for pk := int32(1); pk <= 31; pk++ {
		issue25590InsertRowViaRelation(t, ctx, mp, rel, pk, pk)
	}
	ws := txn.GetWorkspace()
	entries, _ := issue25590WorkspaceStats(t, txn, dbID, tableID)
	require.GreaterOrEqual(t, entries-1, 30,
		"the merged-entries trigger must be reached: %d entries", entries)
	issue25590EndStatement(t, txn)

	// Fail the merge exactly at the combined batch's rowid regeneration.
	disarm := issue25590ArmEngineFault(t, objectio.FJ_CNWorkspaceMergeRowidAllocFailed)
	ws.StartStatement()
	err = ws.IncrStatementID(ctx, false)
	require.ErrorContains(t, err, "workspace merge rowid alloc failed")
	require.NoError(t, ws.RollbackLastStatement(ctx))
	ws.EndStatement()

	// Every source survived the failed admission untouched: all rows stay
	// readable through the production reader path.
	ws.UpdateSnapshotWriteOffset()
	pks, vs := issue25590ReadTableRows(t, ctx, disttaeEngine, txn, rel, mp, schema)
	require.Len(t, pks, 31,
		"an allocation failure before publication may not lose or merge any row")
	for i := int32(1); i <= 31; i++ {
		require.Contains(t, pks, i, "row %d stays visible after the failed merge", i)
	}
	for i := range pks {
		require.Equal(t, pks[i], vs[i], "pk and v columns stay aligned")
	}
	require.Equal(t, 31, issue25590LiveRows(t, txn, dbID, tableID),
		"every source row stays live after the failed merge preserved its entry")
	_, zeroRows := issue25590WorkspaceStats(t, txn, dbID, tableID)
	require.Zero(t, zeroRows,
		"the preserved entries are the statement's original rows, no inert shells")

	// The disarmed retry merges: one published batch carries all rows with
	// a freshly regenerated rowid column — one block, offsets from zero.
	disarm()
	issue25590AdmitStatement(t, ctx, txn)
	issue25590InsertRowViaRelation(t, ctx, mp, rel, 101, 101)
	issue25590EndStatement(t, txn)
	issue25590NextStatementBoundary(t, ctx, txn)

	dtxn := txn.GetWorkspace().(*disttae.Transaction)
	var mergedBat *batch.Batch
	mergedCount := 0
	writeEntries, _ := issue25590WorkspaceStats(t, txn, dbID, tableID)
	dtxn.ForEachTableWrites(dbID, tableID, writeEntries, func(entry disttae.Entry) {
		if entry.Type() != disttae.INSERT || entry.Bat() == nil || entry.FileName() != "" {
			return
		}
		if entry.Bat().RowCount() != 31 {
			return
		}
		mergedBat = entry.Bat()
		mergedCount++
	})
	require.Equal(t, 1, mergedCount,
		"the disarmed retry must publish exactly one merged thirty-one-row batch")
	rowIds := vector.MustFixedColWithTypeCheck[types.Rowid](mergedBat.GetVector(0))
	require.Equal(t, 31, len(rowIds))
	blk := rowIds[0].CloneBlockID()
	for i, rowId := range rowIds {
		require.Equal(t, blk, rowId.CloneBlockID(),
			"the published merged batch carries one block only")
		require.Equal(t, uint32(i), rowId.GetRowOffset(),
			"the published merged batch runs offsets consecutively from zero")
	}

	// A production delete through the merged batch's regenerated rowid
	// selects exactly its own row (the workspace delete scans rely on the
	// one-block-per-batch invariant this round pins), and the resulting
	// marks apply on the closing admission of the statement.
	issue25590AdmitStatement(t, ctx, txn)
	issue25590DeleteRowByRowid(t, ctx, mp, rel, rowIds[0], 1)
	issue25590EndStatement(t, txn)
	issue25590NextStatementBoundary(t, ctx, txn)

	pks, _ = issue25590ReadTableRows(t, ctx, disttaeEngine, txn, rel, mp, schema)
	require.Len(t, pks, 31, "the deleted row vanished, every other row survived")
	require.NotContains(t, pks, int32(1))
	for _, want := range []int32{2, 3, 31, 101} {
		require.Contains(t, pks, want)
	}
	issue25590AssertWorkspaceBounded(t, txn, dbID, tableID, 31)

	// Commit and a fresh transaction read keep every surviving row.
	require.NoError(t, txn.Commit(ctx))
	_, rel2, txn2, err := disttaeEngine.GetTable(ctx, issue25590DB, issue25590Table)
	require.NoError(t, err)
	pks, _ = issue25590ReadTableRows(t, ctx, disttaeEngine, txn2, rel2, mp, schema)
	require.Len(t, pks, 31, "the committed transaction keeps every surviving row")
	require.NotContains(t, pks, int32(1))
	require.NoError(t, txn2.Commit(ctx))
}

// issue25590ArmEngineKeyedFault installs a keyed fault point with explicit
// iarg/sarg — the injections whose trigger parses them (for example the
// small-object flush policy, matched against the flushed table's name:
// iarg 20 selects the equal-table-name matcher).
func issue25590ArmEngineKeyedFault(
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

// issue25590FlushedObjects collects the flushed workspace objects of the
// relation from its block-meta entries: the blocks in Vecs[0] order and
// the per-object stats in Vecs[1] order — the same layout the object
// compaction consumes.
func issue25590FlushedObjects(
	t *testing.T,
	ctx context.Context,
	txn client.TxnOperator,
	relation engine.Relation,
) (blocks []types.Blockid, statsList []objectio.ObjectStats) {
	t.Helper()
	dtxn := txn.GetWorkspace().(*disttae.Transaction)
	dtxn.ForEachTableWrites(
		relation.GetDBID(ctx), relation.GetTableID(ctx),
		int(dtxn.WriteOffset()), func(entry disttae.Entry) {
			if entry.Type() != disttae.INSERT || entry.Bat() == nil ||
				entry.Bat().Attrs[0] != catalog.BlockMeta_BlockInfo ||
				entry.Bat().RowCount() == 0 {
				return
			}
			infoCol, infoArea := vector.MustVarlenaRawData(entry.Bat().Vecs[0])
			for _, info := range infoCol {
				blocks = append(blocks,
					objectio.DecodeBlockInfo(info.GetByteSlice(infoArea)).BlockID)
			}
			statsCol, statsArea := vector.MustVarlenaRawData(entry.Bat().Vecs[1])
			for _, st := range statsCol {
				statsList = append(statsList, objectio.ObjectStats(st.GetByteSlice(statsArea)))
			}
		})
	return blocks, statsList
}

// issue25590WantSurvivingPks computes the ordered full pk range minus the
// deleted pks — the exact visibility oracle for drained reads.
func issue25590WantSurvivingPks(totalRows int, deleted ...int32) []int32 {
	missing := make(map[int32]bool, len(deleted))
	for _, d := range deleted {
		missing[d] = true
	}
	want := make([]int32, 0, totalRows)
	for i := int32(1); i <= int32(totalRows); i++ {
		if !missing[i] {
			want = append(want, i)
		}
	}
	return want
}

// TestIssue25590CompactPublishFailurePreservesDeletesAndCommits is the
// engine-level leg of the round-7 P1 review: a metadata batch publishes
// TWO objects in one flush, the next statement deletes rows of the SECOND
// (dirty) object — whose only deletion record is the pending deletedBlocks —
// and the following admission fails exactly at the post-lookup
// re-publication of the FIRST (clean) object. The failure must not consume
// the pending deletions (the pre-fix pass wiped them on its own error
// path, resurrecting the deleted rows), the rollback keeps them, the
// disarmed retry completes the compaction physically, and the final commit
// plus a fresh read retain exactly the surviving rows.
func TestIssue25590CompactPublishFailurePreservesDeletesAndCommits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)

	schema := catalog2.MockSchema(2, 0)
	schema.Name = issue25590Table
	disttaeEngine, taeEngine, rpcAgent, mp := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, issue25590DB, issue25590Table, schema)
	require.NoError(t, err)

	_, rel, txn, err := disttaeEngine.GetTable(ctx, issue25590DB, issue25590Table)
	require.NoError(t, err)

	// Statement 1: one wide raw entry — 100,000 rows — so the forced flush
	// of the next admission intersects the (injected small) in-memory
	// spill threshold once and spills the tail at Sync: ONE metadata batch
	// carrying TWO objects of the same table.
	const totalRows = 100000
	issue25590AdmitStatement(t, ctx, txn)
	bulk := batch.NewWithSize(2)
	bulk.Attrs = []string{"mock_0", "mock_1"}
	pkCol := vector.NewVec(types.T_int32.ToType())
	vCol := vector.NewVec(types.T_int32.ToType())
	bulkPks := make([]int32, totalRows)
	bulkVs := make([]int32, totalRows)
	for i := range bulkPks {
		bulkPks[i] = int32(i + 1)
		bulkVs[i] = int32(i + 1)
	}
	require.NoError(t, vector.AppendFixedList(pkCol, bulkPks, nil, mp))
	require.NoError(t, vector.AppendFixedList(vCol, bulkVs, nil, mp))
	bulk.Vecs[0] = pkCol
	bulk.Vecs[1] = vCol
	bulk.SetRowCount(totalRows)
	require.NoError(t, rel.Write(ctx, bulk))
	bulk.Clean(mp)
	issue25590EndStatement(t, txn)

	// Statement 2's admission force-flushes the staged entry under the
	// injected 512KB sinker threshold.
	flushDisarm := issue25590ArmEngineFault(t, objectio.FJ_CNWorkspaceForceFlush)
	smallObjsDisarm := issue25590ArmEngineKeyedFault(
		t, objectio.FJ_CNFlushSmallObjs, 20, issue25590Table)
	issue25590AdmitStatement(t, ctx, txn)
	flushDisarm()
	smallObjsDisarm()

	blocks, statsList := issue25590FlushedObjects(t, ctx, txn, rel)
	require.Len(t, statsList, 2,
		"the flush must publish one mixed metadata batch: a clean and a dirty object")
	obj1BlkCnt := int(statsList[0].BlkCnt())
	require.Greater(t, obj1BlkCnt, 0)
	require.Equal(t, obj1BlkCnt+int(statsList[1].BlkCnt()), len(blocks),
		"the block list covers both objects in publication order")
	// obj1 spilled only at full-block boundaries, so its rows are exactly
	// BlkCnt()*BlockMaxRows: the first two rows of obj2 carry the next two
	// pks of the ordered bulk insert.
	obj1Rows := obj1BlkCnt * objectio.BlockMaxRows
	require.LessOrEqual(t, obj1Rows+2, totalRows)
	obj2FirstBlock := blocks[obj1BlkCnt]
	deletedPk1, deletedPk2 := int32(obj1Rows+1), int32(obj1Rows+2)
	issue25590EndStatement(t, txn)

	// Statement 3 deletes the first two rows published into the second
	// object: dumped workspace rows whose only deletion record is the
	// pending deletedBlocks the object compaction consumes.
	issue25590AdmitStatement(t, ctx, txn)
	issue25590DeleteRowByRowid(t, ctx, mp, rel, types.NewRowid(&obj2FirstBlock, 0), deletedPk1)
	issue25590DeleteRowByRowid(t, ctx, mp, rel, types.NewRowid(&obj2FirstBlock, 1), deletedPk2)
	issue25590EndStatement(t, txn)
	dtxn := txn.GetWorkspace().(*disttae.Transaction)
	require.Equal(t, 1, issue25590DeletedBlocksEntries(dtxn),
		"the two dumped-row deletes register the pending compaction input")

	// Statement 4's admission fails at the clean object's post-lookup
	// re-publication, before the dirty object was rewritten.
	compactDisarm := issue25590ArmEngineFault(t, objectio.FJ_CNWorkspaceCompactPublishFailed)
	ws := txn.GetWorkspace()
	ws.StartStatement()
	err = ws.IncrStatementID(ctx, false)
	require.ErrorContains(t, err, "object compaction publish failed")
	require.Equal(t, 1, issue25590DeletedBlocksEntries(dtxn),
		"a compaction failure after the lookup may not consume pending deletions")

	// The frontend statement rollback ends the failed statement and both
	// marked rows stay gone.
	require.NoError(t, ws.RollbackLastStatement(ctx))
	ws.EndStatement()
	require.Equal(t, 1, issue25590DeletedBlocksEntries(dtxn),
		"the rollback after the interrupted admission keeps the pending deletions")

	// Exact read through the rollback: 99,998 rows, exactly the first two
	// rows of the second object missing. A pass that wiped the marks on
	// its own failure path resurrects them here.
	ws.UpdateSnapshotWriteOffset()
	pks, _ := issue25590ReadTableRows(t, ctx, disttaeEngine, txn, rel, mp, schema)
	require.Len(t, pks, totalRows-2,
		"exactly the two deleted rows are gone after the failed admission's rollback")
	slices.Sort(pks)
	require.Equal(t, issue25590WantSurvivingPks(totalRows, deletedPk1, deletedPk2), pks,
		"the read after the failed-admission rollback keeps every earlier statement exact")

	// Disarmed retry admission: the compaction completes — the clean object
	// is re-published, the dirty object is physically rewritten without
	// its marked rows — and the pending marks clear because the deletions
	// are now embodied in the rewritten storage object.
	compactDisarm()
	issue25590AdmitStatement(t, ctx, txn)
	require.Zero(t, issue25590DeletedBlocksEntries(dtxn),
		"the completed compaction embodies the deletions and clears their marks")
	issue25590EndStatement(t, txn)

	// The rewrite resurrected nothing: the drained read stays exact.
	ws.UpdateSnapshotWriteOffset()
	pks, _ = issue25590ReadTableRows(t, ctx, disttaeEngine, txn, rel, mp, schema)
	require.Len(t, pks, totalRows-2,
		"the physically rewritten object keeps the deleted rows deleted")
	slices.Sort(pks)
	require.Equal(t, issue25590WantSurvivingPks(totalRows, deletedPk1, deletedPk2), pks,
		"the read after the retried compaction is exact")

	// COMMIT publishes the rewritten objects, and a fresh transaction
	// reads back exactly the surviving rows.
	require.NoError(t, txn.Commit(ctx))

	_, rel2, txn2, err := disttaeEngine.GetTable(ctx, issue25590DB, issue25590Table)
	require.NoError(t, err)
	pks, _ = issue25590ReadTableRows(t, ctx, disttaeEngine, txn2, rel2, mp, schema)
	require.Len(t, pks, totalRows-2,
		"the committed data excludes exactly the two deleted rows")
	slices.Sort(pks)
	require.Equal(t, issue25590WantSurvivingPks(totalRows, deletedPk1, deletedPk2), pks,
		"the post-commit read is exact")
	require.NoError(t, txn2.Commit(ctx))
}

// issue25590ArmEngineCountedPublishFault arms the workspace compaction
// re-publication fault to fire exactly at the n-th invocation instead of
// the first — the round-8 second-publication scenario: one affected
// object's republication succeeds before the next one fails.
func issue25590ArmEngineCountedPublishFault(t *testing.T, nth int64) (disarm func()) {
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

// TestIssue25590CompactSecondPublishFailureRetriesAndCommits is the
// engine-level leg of the round-8 P1 review: TWO mixed metadata batches
// (each a clean and a dirty object) are affected by the next admission's
// object compaction, and the SECOND republication fails while the first
// had already been published by the pre-staging pass. The duplicate
// object reference that pass stranded inside the rolled-back statement
// made the drained read see the same object twice; the staged pass below
// must keep the failure all-or-nothing: nothing appended, both originals
// and both pending-deletion marks intact, exact reads after the rollback,
// the retried admission and the commit.
func TestIssue25590CompactSecondPublishFailureRetriesAndCommits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)

	schema := catalog2.MockSchema(2, 0)
	schema.Name = issue25590Table
	disttaeEngine, taeEngine, rpcAgent, mp := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, issue25590DB, issue25590Table, schema)
	require.NoError(t, err)

	_, rel, txn, err := disttaeEngine.GetTable(ctx, issue25590DB, issue25590Table)
	require.NoError(t, err)

	const rowsPerEntry = 100000
	const totalRows = 2 * rowsPerEntry
	deletedOfEntry := func(entry int32, obj1Rows int) (int32, int32) {
		return entry*rowsPerEntry + int32(obj1Rows) + 1,
			entry*rowsPerEntry + int32(obj1Rows) + 2
	}

	// One wide raw entry per staged statement: 100,000 rows whose forced
	// flush intersects the injected small-objects threshold once and
	// spills the tail at Sync, publishing ONE mixed metadata batch.
	insertBulkEntry := func(entry int32) {
		bulk := batch.NewWithSize(2)
		bulk.Attrs = []string{"mock_0", "mock_1"}
		pkCol := vector.NewVec(types.T_int32.ToType())
		vCol := vector.NewVec(types.T_int32.ToType())
		bulkPks := make([]int32, rowsPerEntry)
		bulkVs := make([]int32, rowsPerEntry)
		for i := range bulkPks {
			bulkPks[i] = entry*rowsPerEntry + int32(i) + 1
			bulkVs[i] = entry*rowsPerEntry + int32(i) + 1
		}
		require.NoError(t, vector.AppendFixedList(pkCol, bulkPks, nil, mp))
		require.NoError(t, vector.AppendFixedList(vCol, bulkVs, nil, mp))
		bulk.Vecs[0] = pkCol
		bulk.Vecs[1] = vCol
		bulk.SetRowCount(rowsPerEntry)
		require.NoError(t, rel.Write(ctx, bulk))
		bulk.Clean(mp)
	}

	// The forced-flush arming pair publishes the just-written raw entry on
	// the NEXT admission; the shapes (first block of the dirty second
	// object) are collected per metadata batch, in publication order.
	armFlush := func() (disarm func()) {
		flushDisarm := issue25590ArmEngineFault(t, objectio.FJ_CNWorkspaceForceFlush)
		smallObjsDisarm := issue25590ArmEngineKeyedFault(
			t, objectio.FJ_CNFlushSmallObjs, 20, issue25590Table)
		return func() {
			flushDisarm()
			smallObjsDisarm()
		}
	}
	// issue25590MixedBatches groups the relation's block-meta batches: one
	// stats slice and one block-id slice per published metadata batch, in
	// publication order. ForEachTableWrites visits the FIRST offset
	// entries, so a sentinel above any live length walks the whole
	// workspace; this test's table publishes exactly its own batches.
	mixedBatches := func() (batches [][]objectio.ObjectStats, blockIds [][]types.Blockid) {
		dtxn := txn.GetWorkspace().(*disttae.Transaction)
		dtxn.ForEachTableWrites(
			rel.GetDBID(ctx), rel.GetTableID(ctx), 1<<30, func(entry disttae.Entry) {
				if entry.Type() != disttae.INSERT || entry.Bat() == nil ||
					entry.Bat().Attrs[0] != catalog.BlockMeta_BlockInfo ||
					entry.Bat().RowCount() == 0 {
					return
				}
				ids := make([]types.Blockid, 0, 16)
				infoCol, infoArea := vector.MustVarlenaRawData(entry.Bat().Vecs[0])
				for _, info := range infoCol {
					ids = append(ids,
						objectio.DecodeBlockInfo(info.GetByteSlice(infoArea)).BlockID)
				}
				statsCol, statsArea := vector.MustVarlenaRawData(entry.Bat().Vecs[1])
				sts := make([]objectio.ObjectStats, 0, 8)
				for _, st := range statsCol {
					sts = append(sts, objectio.ObjectStats(st.GetByteSlice(statsArea)))
				}
				batches = append(batches, sts)
				blockIds = append(blockIds, ids)
			})
		require.NotEmpty(t, batches, "no block-meta batch found")
		return batches, blockIds
	}
	requireMixedBatch := func(entryNum int) (obj2FirstBlock types.Blockid, obj1Rows int) {
		batches, blockIds := mixedBatches()
		require.Len(t, batches, entryNum,
			"exactly one new mixed metadata batch per flushed entry")
		stats := batches[entryNum-1]
		ids := blockIds[entryNum-1]
		require.Len(t, stats, 2,
			"the flush must publish one mixed metadata batch: a clean and a dirty object")
		obj1BlkCnt := int(stats[0].BlkCnt())
		require.Greater(t, obj1BlkCnt, 0)
		require.Equal(t, obj1BlkCnt+int(stats[1].BlkCnt()), len(ids),
			"the block list covers both objects in publication order")
		obj1Rows = obj1BlkCnt * objectio.BlockMaxRows
		require.LessOrEqual(t, obj1Rows+2, rowsPerEntry)
		return ids[obj1BlkCnt], obj1Rows
	}

	// Statement 1: raw entry 1 (pks 1..100000).
	issue25590AdmitStatement(t, ctx, txn)
	insertBulkEntry(0)
	issue25590EndStatement(t, txn)

	// Statement 2: flush entry 1 into mixed metadata batch 1.
	disarmFlush := armFlush()
	issue25590AdmitStatement(t, ctx, txn)
	disarmFlush()
	obj2Block1, obj1Rows1 := requireMixedBatch(1)
	delA1, delA2 := deletedOfEntry(0, obj1Rows1)
	issue25590EndStatement(t, txn)

	// Statement 3: raw entry 2 (pks 100001..200000).
	issue25590AdmitStatement(t, ctx, txn)
	insertBulkEntry(1)
	issue25590EndStatement(t, txn)

	// Statement 4: flush entry 2 into mixed metadata batch 2.
	disarmFlush = armFlush()
	issue25590AdmitStatement(t, ctx, txn)
	disarmFlush()
	obj2Block2, obj1Rows2 := requireMixedBatch(2)
	delB1, delB2 := deletedOfEntry(1, obj1Rows2)
	issue25590EndStatement(t, txn)

	// Statement 5: delete the first two rows of EACH batch's dirty second
	// object — four dumped rows whose only deletion records are the two
	// pending deletedBlocks entries the object compaction consumes.
	issue25590AdmitStatement(t, ctx, txn)
	issue25590DeleteRowByRowid(t, ctx, mp, rel, types.NewRowid(&obj2Block1, 0), delA1)
	issue25590DeleteRowByRowid(t, ctx, mp, rel, types.NewRowid(&obj2Block1, 1), delA2)
	issue25590DeleteRowByRowid(t, ctx, mp, rel, types.NewRowid(&obj2Block2, 0), delB1)
	issue25590DeleteRowByRowid(t, ctx, mp, rel, types.NewRowid(&obj2Block2, 1), delB2)
	issue25590EndStatement(t, txn)
	dtxn := txn.GetWorkspace().(*disttae.Transaction)
	require.Equal(t, 2, issue25590DeletedBlocksEntries(dtxn),
		"the four dumped-row deletes register two pending block deletions, one per dirty object")

	// Statement 6's admission fails at the SECOND re-publication — the
	// first clean object's replacement was published by the pre-staging
	// pass before it, exactly the partial-publication window the round-8
	// review reported.
	compactDisarm := issue25590ArmEngineCountedPublishFault(t, 2)
	ws := txn.GetWorkspace()
	ws.StartStatement()
	err = ws.IncrStatementID(ctx, false)
	require.ErrorContains(t, err, "object compaction publish failed",
		"the counted fault must fail exactly the second re-publication")
	require.Equal(t, 2, issue25590DeletedBlocksEntries(dtxn),
		"the second-publication failure may not consume any pending deletions")

	require.NoError(t, ws.RollbackLastStatement(ctx))
	ws.EndStatement()
	require.Equal(t, 2, issue25590DeletedBlocksEntries(dtxn),
		"the rollback after the interrupted admission keeps both pending deletions")

	// Exact read through the rollback: 199,998 rows, exactly the four
	// marked rows missing. The stranded duplicate of the pre-staging pass
	// doubles the first clean object here instead.
	ws.UpdateSnapshotWriteOffset()
	pks, _ := issue25590ReadTableRows(t, ctx, disttaeEngine, txn, rel, mp, schema)
	require.Len(t, pks, totalRows-4,
		"the read after the failed-admission rollback sees each row exactly once")
	slices.Sort(pks)
	require.Equal(t,
		issue25590WantSurvivingPks(totalRows, delA1, delA2, delB1, delB2), pks,
		"the read after the second-publication failure and rollback is exact")

	// Disarmed retry admission: the compaction completes — both clean
	// objects re-published once each, both dirty objects physically
	// rewritten without their marked rows — and the pending marks clear.
	compactDisarm()
	issue25590AdmitStatement(t, ctx, txn)
	require.Zero(t, issue25590DeletedBlocksEntries(dtxn),
		"the completed compaction embodies the deletions and clears their marks")
	issue25590EndStatement(t, txn)

	ws.UpdateSnapshotWriteOffset()
	pks, _ = issue25590ReadTableRows(t, ctx, disttaeEngine, txn, rel, mp, schema)
	require.Len(t, pks, totalRows-4,
		"the physically rewritten objects keep the deleted rows deleted")
	slices.Sort(pks)
	require.Equal(t,
		issue25590WantSurvivingPks(totalRows, delA1, delA2, delB1, delB2), pks,
		"the read after the retried compaction is exact")

	// COMMIT publishes the rewritten objects; a fresh transaction reads
	// back exactly the surviving rows.
	require.NoError(t, txn.Commit(ctx))

	_, rel2, txn2, err := disttaeEngine.GetTable(ctx, issue25590DB, issue25590Table)
	require.NoError(t, err)
	pks, _ = issue25590ReadTableRows(t, ctx, disttaeEngine, txn2, rel2, mp, schema)
	require.Len(t, pks, totalRows-4,
		"the committed data excludes exactly the four deleted rows")
	slices.Sort(pks)
	require.Equal(t,
		issue25590WantSurvivingPks(totalRows, delA1, delA2, delB1, delB2), pks,
		"the post-commit read is exact")
	require.NoError(t, txn2.Commit(ctx))
}

// === round-19 regression: compaction admission refusal, end to end ===

// TestIssue25590CompactionAdmissionRefusalPreservesRowsAndCommits drives
// the round-19 review finding 1 through the real engine: at a saturated
// admission bound, the object-compaction pass of a statement admission is
// refused at its dirty replacement's persistence (the writer's per-object
// capacity hook) BEFORE any original retires, and the FAILED ADMISSION
// reaches the frontend as an error - the pre-fix pass swallowed the
// refusal inside its ants worker (ants recovered the panic), GC'd the
// dirty object's original, wiped the pending marks and returned nil, so
// the object's surviving rows vanished from the workspace while the
// statement believed it had succeeded. The failed statement rolls back
// with every source and mark intact, the exact surviving rows read back
// before and after the rollback, and the retried admission and COMMIT
// publish exactly them.
func TestIssue25590CompactionAdmissionRefusalPreservesRowsAndCommits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)

	schema := catalog2.MockSchema(2, 0)
	schema.Name = issue25590Table
	disttaeEngine, taeEngine, rpcAgent, mp := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, issue25590DB, issue25590Table, schema)
	require.NoError(t, err)

	_, rel, txn, err := disttaeEngine.GetTable(ctx, issue25590DB, issue25590Table)
	require.NoError(t, err)

	// Statement 1: the wide raw entry whose forced flush spills ONE
	// metadata batch with two objects.
	const totalRows = 100000
	issue25590AdmitStatement(t, ctx, txn)
	bulk := batch.NewWithSize(2)
	bulk.Attrs = []string{"mock_0", "mock_1"}
	pkCol := vector.NewVec(types.T_int32.ToType())
	vCol := vector.NewVec(types.T_int32.ToType())
	bulkPks := make([]int32, totalRows)
	bulkVs := make([]int32, totalRows)
	for i := range bulkPks {
		bulkPks[i] = int32(i + 1)
		bulkVs[i] = int32(i + 1)
	}
	require.NoError(t, vector.AppendFixedList(pkCol, bulkPks, nil, mp))
	require.NoError(t, vector.AppendFixedList(vCol, bulkVs, nil, mp))
	bulk.Vecs[0] = pkCol
	bulk.Vecs[1] = vCol
	bulk.SetRowCount(totalRows)
	require.NoError(t, rel.Write(ctx, bulk))
	bulk.Clean(mp)
	issue25590EndStatement(t, txn)

	// Statement 2: the force-flush under the injected small-object knob
	// publishes the mixed metadata batch.
	flushDisarm := issue25590ArmEngineFault(t, objectio.FJ_CNWorkspaceForceFlush)
	smallObjsDisarm := issue25590ArmEngineKeyedFault(
		t, objectio.FJ_CNFlushSmallObjs, 20, issue25590Table)
	issue25590AdmitStatement(t, ctx, txn)
	flushDisarm()
	smallObjsDisarm()

	blocks, statsList := issue25590FlushedObjects(t, ctx, txn, rel)
	require.Len(t, statsList, 2,
		"the flush must publish one mixed metadata batch: a clean and a dirty object")
	obj1BlkCnt := int(statsList[0].BlkCnt())
	require.Greater(t, obj1BlkCnt, 0)
	obj1Rows := obj1BlkCnt * objectio.BlockMaxRows
	require.LessOrEqual(t, obj1Rows+2, totalRows)
	obj2FirstBlock := blocks[obj1BlkCnt]
	deletedPk1, deletedPk2 := int32(obj1Rows+1), int32(obj1Rows+2)
	issue25590EndStatement(t, txn)

	// Statement 3: two pending deletes on the dirty object's rows - the
	// marks the next admission's compaction embodies.
	issue25590AdmitStatement(t, ctx, txn)
	issue25590DeleteRowByRowid(t, ctx, mp, rel, types.NewRowid(&obj2FirstBlock, 0), deletedPk1)
	issue25590DeleteRowByRowid(t, ctx, mp, rel, types.NewRowid(&obj2FirstBlock, 1), deletedPk2)
	issue25590EndStatement(t, txn)
	ws := txn.GetWorkspace()
	require.Equal(t, 1, issue25590DeletedBlocksEntries(ws.(*disttae.Transaction)),
		"the two deleted rows registered the pending compaction input")

	// Statement 4's admission runs with the admission bound PINNED to
	// zero: the dirty object's replacement cannot reserve its in-flight
	// unit, so the compaction pass fails instead of panicking the
	// refusal away inside its worker and returning a nil error with the
	// original GC'd behind it.
	restoreLimit := disttae.PinUnpublishedPendingAdmissionLimitForTest(0)
	ws.StartStatement()
	err = ws.IncrStatementID(ctx, false)
	require.ErrorContains(t, err, "workspace admission paused",
		"a saturated bound refuses the dirty replacement's persistence and the admission fails whole")
	require.Equal(t, 1, issue25590DeletedBlocksEntries(ws.(*disttae.Transaction)),
		"the refused compaction leaves the pending deletions in place")

	// The frontend rollback of the failed statement keeps every source
	// and mark; the exact surviving rows read back afterwards.
	require.NoError(t, ws.RollbackLastStatement(ctx))
	ws.EndStatement()
	require.Equal(t, 1, issue25590DeletedBlocksEntries(ws.(*disttae.Transaction)),
		"the statement rollback keeps the pending deletions for the retry")
	ws.UpdateSnapshotWriteOffset()
	pks, _ := issue25590ReadTableRows(t, ctx, disttaeEngine, txn, rel, mp, schema)
	require.Len(t, pks, totalRows-2,
		"after the refused admission's rollback, exactly the two deleted rows are gone")
	slices.Sort(pks)
	require.Equal(t, issue25590WantSurvivingPks(totalRows, deletedPk1, deletedPk2), pks,
		"the surviving-row contents after the refused admission are exact")

	// The retried admission under the restored bound completes the
	// compaction - the replacement records and the originals retire -
	// and the drained read stays exact.
	restoreLimit()
	issue25590AdmitStatement(t, ctx, txn)
	require.Zero(t, issue25590DeletedBlocksEntries(ws.(*disttae.Transaction)),
		"the retried admission's compaction embodies the deletions and clears their marks")
	issue25590EndStatement(t, txn)
	ws.UpdateSnapshotWriteOffset()
	pks, _ = issue25590ReadTableRows(t, ctx, disttaeEngine, txn, rel, mp, schema)
	require.Len(t, pks, totalRows-2,
		"the physically rewritten object keeps the deleted rows deleted")
	slices.Sort(pks)
	require.Equal(t, issue25590WantSurvivingPks(totalRows, deletedPk1, deletedPk2), pks,
		"the surviving-row contents after the retried compaction are exact")

	// COMMIT publishes the rewritten objects and a fresh transaction
	// reads back exactly the surviving rows.
	require.NoError(t, txn.Commit(ctx))

	_, rel2, txn2, err := disttaeEngine.GetTable(ctx, issue25590DB, issue25590Table)
	require.NoError(t, err)
	pks, _ = issue25590ReadTableRows(t, ctx, disttaeEngine, txn2, rel2, mp, schema)
	require.Len(t, pks, totalRows-2,
		"the committed data excludes exactly the two deleted rows")
	slices.Sort(pks)
	require.Equal(t, issue25590WantSurvivingPks(totalRows, deletedPk1, deletedPk2), pks,
		"the committed surviving-row contents are exact")
	require.NoError(t, txn2.Commit(ctx))
}

// TestIssue25590TwoDirtyObjectRefusalRollsBackRetriesAndCommits is the
// round-20 engine-level atomicity proof across ALL replacements of one
// compaction pass (round-20 review, finding 1): the flush publishes a
// mixed metadata batch whose TWO objects both carry pending deletes, and
// the admission bound allows exactly one fresh per-object reservation.
// The pass therefore has one replacement rewritten and ready when the
// other is refused - and no live replacement record may survive that
// partially failed pass, or the statement rollback (whose boundary sits
// above any such append) leaves it in the workspace and the retried
// compaction commits its surviving rows a second time. Exact surviving-row
// reads after the rollback, after the retried admission and after the
// commit catch any such duplication.
func TestIssue25590TwoDirtyObjectRefusalRollsBackRetriesAndCommits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)

	schema := catalog2.MockSchema(2, 0)
	schema.Name = issue25590Table
	disttaeEngine, taeEngine, rpcAgent, mp := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, issue25590DB, issue25590Table, schema)
	require.NoError(t, err)

	_, rel, txn, err := disttaeEngine.GetTable(ctx, issue25590DB, issue25590Table)
	require.NoError(t, err)

	// Statement 1: the wide raw entry whose forced flush spills ONE
	// metadata batch with two objects - both to be marked dirty.
	const totalRows = 100000
	issue25590AdmitStatement(t, ctx, txn)
	bulk := batch.NewWithSize(2)
	bulk.Attrs = []string{"mock_0", "mock_1"}
	pkCol := vector.NewVec(types.T_int32.ToType())
	vCol := vector.NewVec(types.T_int32.ToType())
	bulkPks := make([]int32, totalRows)
	bulkVs := make([]int32, totalRows)
	for i := range bulkPks {
		bulkPks[i] = int32(i + 1)
		bulkVs[i] = int32(i + 1)
	}
	require.NoError(t, vector.AppendFixedList(pkCol, bulkPks, nil, mp))
	require.NoError(t, vector.AppendFixedList(vCol, bulkVs, nil, mp))
	bulk.Vecs[0] = pkCol
	bulk.Vecs[1] = vCol
	bulk.SetRowCount(totalRows)
	require.NoError(t, rel.Write(ctx, bulk))
	bulk.Clean(mp)
	issue25590EndStatement(t, txn)

	// Statement 2: the force-flush under the injected small-object knob
	// publishes the mixed metadata batch.
	flushDisarm := issue25590ArmEngineFault(t, objectio.FJ_CNWorkspaceForceFlush)
	smallObjsDisarm := issue25590ArmEngineKeyedFault(
		t, objectio.FJ_CNFlushSmallObjs, 20, issue25590Table)
	issue25590AdmitStatement(t, ctx, txn)
	flushDisarm()
	smallObjsDisarm()

	blocks, statsList := issue25590FlushedObjects(t, ctx, txn, rel)
	require.Len(t, statsList, 2,
		"the flush must publish one mixed metadata batch over two objects")
	obj1BlkCnt := int(statsList[0].BlkCnt())
	require.Greater(t, obj1BlkCnt, 0)
	obj1Rows := obj1BlkCnt * objectio.BlockMaxRows
	require.LessOrEqual(t, obj1Rows+2, totalRows)
	obj1FirstBlock := blocks[0]
	obj2FirstBlock := blocks[obj1BlkCnt]
	// The four deleted pks: the first two rows of each object's region.
	headPk1, headPk2 := int32(1), int32(2)
	tailPk1, tailPk2 := int32(obj1Rows+1), int32(obj1Rows+2)
	issue25590EndStatement(t, txn)

	// Statement 3: two pending deletes on EACH object - two dirty
	// objects for the next admission's compaction pass.
	issue25590AdmitStatement(t, ctx, txn)
	issue25590DeleteRowByRowid(t, ctx, mp, rel, types.NewRowid(&obj1FirstBlock, 0), headPk1)
	issue25590DeleteRowByRowid(t, ctx, mp, rel, types.NewRowid(&obj1FirstBlock, 1), headPk2)
	issue25590DeleteRowByRowid(t, ctx, mp, rel, types.NewRowid(&obj2FirstBlock, 0), tailPk1)
	issue25590DeleteRowByRowid(t, ctx, mp, rel, types.NewRowid(&obj2FirstBlock, 1), tailPk2)
	issue25590EndStatement(t, txn)
	ws := txn.GetWorkspace()
	require.Equal(t, 2, issue25590DeletedBlocksEntries(ws.(*disttae.Transaction)),
		"the four deleted rows marked two blocks on the two dirtied objects")

	// Statement 4's admission runs with the bound allowing exactly ONE
	// fresh per-object reservation over the flush's two owned objects:
	// one replacement rewrites and persists, the other is refused at its
	// writer's hook, and the whole pass fails - atomically across both
	// replacements.
	restoreLimit := disttae.PinUnpublishedPendingAdmissionLimitForTest(3)
	ws.StartStatement()
	err = ws.IncrStatementID(ctx, false)
	require.ErrorContains(t, err, "workspace admission paused",
		"a one-reservation bound fails the whole two-replacement pass")
	require.Equal(t, 2, issue25590DeletedBlocksEntries(ws.(*disttae.Transaction)),
		"the refused compaction leaves both dirty objects' pending deletions in place")

	// The frontend rollback of the failed statement keeps every source
	// and mark; the exact surviving rows read back afterwards - any live
	// replacement record that survived the failed pass past this
	// boundary would surface here (and again after the retry) as
	// duplicated rows.
	require.NoError(t, ws.RollbackLastStatement(ctx))
	ws.EndStatement()
	require.Equal(t, 2, issue25590DeletedBlocksEntries(ws.(*disttae.Transaction)),
		"the statement rollback keeps both objects' pending deletions for the retry")
	ws.UpdateSnapshotWriteOffset()
	pks, _ := issue25590ReadTableRows(t, ctx, disttaeEngine, txn, rel, mp, schema)
	require.Len(t, pks, totalRows-4,
		"after the refused admission's rollback, exactly the four deleted rows are gone")
	slices.Sort(pks)
	require.Equal(t,
		issue25590WantSurvivingPks(totalRows, headPk1, headPk2, tailPk1, tailPk2), pks,
		"the surviving-row contents after the refused admission are exact - no duplicated object")

	// The retried admission under the restored bound completes the
	// compaction - both replacements record, the originals retire - and
	// the drained read stays exact in CONTENT, not only in count.
	restoreLimit()
	issue25590AdmitStatement(t, ctx, txn)
	require.Zero(t, issue25590DeletedBlocksEntries(ws.(*disttae.Transaction)),
		"the retried admission's compaction embodies the deletions and clears their marks")
	issue25590EndStatement(t, txn)
	ws.UpdateSnapshotWriteOffset()
	pks, _ = issue25590ReadTableRows(t, ctx, disttaeEngine, txn, rel, mp, schema)
	require.Len(t, pks, totalRows-4,
		"the physically rewritten objects keep the deleted rows deleted - and no half-recorded replacement doubles any survivor")
	slices.Sort(pks)
	require.Equal(t,
		issue25590WantSurvivingPks(totalRows, headPk1, headPk2, tailPk1, tailPk2), pks,
		"the surviving-row contents after the retried compaction are exact")

	// COMMIT publishes the rewritten objects and a fresh transaction
	// reads back exactly the surviving rows.
	require.NoError(t, txn.Commit(ctx))

	_, rel2, txn2, err := disttaeEngine.GetTable(ctx, issue25590DB, issue25590Table)
	require.NoError(t, err)
	pks, _ = issue25590ReadTableRows(t, ctx, disttaeEngine, txn2, rel2, mp, schema)
	require.Len(t, pks, totalRows-4,
		"the committed data excludes exactly the four deleted rows")
	slices.Sort(pks)
	require.Equal(t,
		issue25590WantSurvivingPks(totalRows, headPk1, headPk2, tailPk1, tailPk2), pks,
		"the committed surviving-row contents are exact")
	require.NoError(t, txn2.Commit(ctx))
}
