// Copyright 2022 Matrix Origin
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
	"fmt"
	"testing"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	testutil2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// #region basic test

// insert 10 rows and delete 5 rows
func Test_BasicInsertDelete(t *testing.T) {
	var (
		err          error
		mp           *mpool.MPool
		txn          client.TxnOperator
		accountId    = catalog.System_Account
		tableName    = "test_table"
		databaseName = "test_database"

		primaryKeyIdx = 3

		relation engine.Relation
		_        engine.Database

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	schema := catalog2.MockSchemaAll(4, primaryKeyIdx)
	schema.Name = tableName

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(
		ctx,
		testutil.TestOptions{},
		t,
	)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	rowsCount := 10
	bat := catalog2.MockBatch(schema, rowsCount)
	bat1 := containers.ToCNBatch(bat)

	// write table
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat1, false, true))

		var bat2 *batch.Batch
		txn.GetWorkspace().(*disttae.Transaction).ForEachTableWrites(
			relation.GetDBID(ctx), relation.GetTableID(ctx), 1, func(entry disttae.Entry) {
				waitedDeletes := vector.MustFixedColWithTypeCheck[types.Rowid](entry.Bat().GetVector(0))
				waitedDeletes = waitedDeletes[:rowsCount/2]
				bat2 = batch.NewWithSize(1)
				bat2.Attrs = append(bat2.Attrs, catalog.Row_ID)
				bat2.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
				require.NoError(t, vector.AppendFixedList[types.Rowid](bat2.Vecs[0], waitedDeletes, nil, mp))
				bat2.SetRowCount(len(waitedDeletes))
			})
		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat2, true, true))
		require.NoError(t, txn.Commit(ctx))
	}

	_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
	require.NoError(t, err)
	reader, err := testutil.GetRelationReader(
		ctx,
		disttaeEngine,
		txn,
		relation,
		nil,
		mp,
		t,
	)
	require.NoError(t, err)

	ret := testutil.EmptyBatchFromSchema(schema, primaryKeyIdx)
	_, err = reader.Read(ctx, ret.Attrs, nil, mp, ret)
	require.NoError(t, err)

	require.Equal(t, 5, ret.RowCount())
	require.NoError(t, txn.Commit(ctx))
}

func Test_BasicS3InsertDelete(t *testing.T) {
	var (
		err          error
		mp           *mpool.MPool
		txn          client.TxnOperator
		accountId    = catalog.System_Account
		tableName    = "test_table"
		databaseName = "test_database"

		primaryKeyIdx = 3

		relation engine.Relation
		_        engine.Database

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	schema := catalog2.MockSchemaAll(4, primaryKeyIdx)
	schema.Name = tableName

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(
		ctx,
		testutil.TestOptions{},
		t,
		testutil.WithDisttaeEngineInsertEntryMaxCount(1),
		testutil.WithDisttaeEngineCommitWorkspaceThreshold(1),
	)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	rowsCount := 10
	bat := catalog2.MockBatch(schema, rowsCount)
	bat1 := containers.ToCNBatch(bat)

	// write table
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat1, false, true))
	}

	// read row id
	tombstoneBat := batch.NewWithSize(1)
	tombstoneBat.Attrs = append(tombstoneBat.Attrs, catalog.Row_ID)
	tombstoneBat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	{
		reader, err := testutil.GetRelationReader(
			ctx,
			disttaeEngine,
			txn,
			relation,
			nil,
			mp,
			t,
		)
		require.NoError(t, err)

		ret := batch.NewWithSize(1)
		ret.Attrs = []string{catalog.Row_ID}
		ret.Vecs = []*vector.Vector{vector.NewVec(types.T_Rowid.ToType())}

		for {
			done, err := reader.Read(ctx, ret.Attrs, nil, mp, ret)

			if done {
				break
			}

			require.NoError(t, err)
			for i := range ret.RowCount() {
				err = vector.AppendFixed[types.Rowid](
					tombstoneBat.Vecs[0],
					vector.GetFixedAtWithTypeCheck[types.Rowid](ret.Vecs[0], i),
					false, mp)
				require.NoError(t, err)
			}
		}

		tombstoneBat.SetRowCount(tombstoneBat.Vecs[0].Length())
		require.Equal(t, bat.Length(), tombstoneBat.Vecs[0].Length())
	}

	// delete batch
	{
		require.NoError(t, err)
		bat2, err := tombstoneBat.Window(0, 5)
		bat2.Attrs = tombstoneBat.Attrs
		require.NoError(t, err)
		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat2, true, true))
		require.NoError(t, txn.Commit(ctx))
	}

	_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)

	require.NoError(t, err)
	reader, err := testutil.GetRelationReader(
		ctx,
		disttaeEngine,
		txn,
		relation,
		nil,
		mp,
		t,
	)
	require.NoError(t, err)

	ret := testutil.EmptyBatchFromSchema(schema, primaryKeyIdx)
	_, err = reader.Read(ctx, ret.Attrs, nil, mp, ret)
	require.NoError(t, err)

	require.Equal(t, 5, ret.RowCount())
	require.NoError(t, txn.Commit(ctx))
}

// Test_Issue25557MidTxnDumpKeepsWorkspaceVisibility drives the full
// issue #25557 chain against a real engine:
//
//	relation.Write -> dumpBatch [txn.Lock()]
//	  -> dumpInsertBatchLocked -> getTable
//	    -> (fault-injected) internal SQL [locks the workspace]
//
// A tiny write-workspace threshold and an exhausted quota pool force the
// dump in the middle of the statement, and the fault point makes getTable
// run the internal-SQL leg exactly like a compile hitting the dump window.
// The internal SQL must not advance the statement boundary, so the
// statement keeps not seeing its own writes, and after the statement ends
// the flushed rows are fully visible.
func Test_Issue25557MidTxnDumpKeepsWorkspaceVisibility(t *testing.T) {
	var (
		err          error
		mp           *mpool.MPool
		txn          client.TxnOperator
		accountId    = catalog.System_Account
		tableName    = "test_table"
		databaseName = "test_database"

		primaryKeyIdx = 3

		relation engine.Relation

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	schema := catalog2.MockSchemaAll(4, primaryKeyIdx)
	schema.Name = tableName

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(
		ctx,
		testutil.TestOptions{},
		t,
		// force the dump in the middle of the write statement
		testutil.WithDisttaeEngineWriteWorkspaceThreshold(1),
		testutil.WithDisttaeEngineCommitWorkspaceThreshold(1),
		// an exhausted quota pool so the dump cannot be postponed
		testutil.WithDisttaeEngineQuota(1),
	)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	// Make getTable run the internal-SQL leg (iarg 1: keep the normal lookup
	// working afterwards).
	fault.Enable()
	defer fault.Disable()
	require.NoError(t, fault.AddFaultPoint(
		ctx, objectio.FJ_CNReenterSnapshotOffsetOnGetTable, ":::", "echo", 1, "", false))
	defer func() {
		_, err := fault.RemoveFaultPoint(
			ctx, objectio.FJ_CNReenterSnapshotOffsetOnGetTable)
		require.NoError(t, err)
	}()

	rowsCount := 10
	bat := catalog2.MockBatch(schema, rowsCount)
	bat1 := containers.ToCNBatch(bat)

	_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
	require.NoError(t, err)

	// Do NOT end the statement yet: the boundary must stay at the statement
	// start even though the dump ran an internal SQL in between.
	require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat1, false, false))

	// the internal SQL must not advance the statement boundary, so the
	// statement keeps not seeing its own writes
	require.Equal(t, 0, txn.GetWorkspace().GetSnapshotWriteOffset(),
		"internal SQL must not advance snapshotWriteOffset")
	require.Equal(t, 0, issue25557ReadRowCount(
		t, ctx, disttaeEngine, txn, relation, schema, primaryKeyIdx, mp),
		"a statement must not see its own writes")

	// the write workspace threshold guarantees the insert was flushed to S3
	// during the statement; once the statement ends, the flushed rows must
	// be visible to this transaction
	require.NoError(t, testutil.EndThisStatement(ctx, txn))
	require.Equal(t, rowsCount, issue25557ReadRowCount(
		t, ctx, disttaeEngine, txn, relation, schema, primaryKeyIdx, mp),
		"rows flushed by the mid-statement dump must stay visible "+
			"to reads of the same transaction")
	require.NoError(t, txn.Commit(ctx))

	// the committed data must be complete
	_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
	require.NoError(t, err)
	require.Equal(t, rowsCount, issue25557ReadRowCount(
		t, ctx, disttaeEngine, txn, relation, schema, primaryKeyIdx, mp))
	require.NoError(t, txn.Commit(ctx))
}

// issue25557ReadRowCount reads every row of the relation visible to txn and
// returns the total row count.
func issue25557ReadRowCount(
	t *testing.T,
	ctx context.Context,
	e *testutil.TestDisttaeEngine,
	txn client.TxnOperator,
	relation engine.Relation,
	schema *catalog2.Schema,
	primaryKeyIdx int,
	mp *mpool.MPool,
) int {
	reader, err := testutil.GetRelationReader(ctx, e, txn, relation, nil, mp, t)
	require.NoError(t, err)
	total := 0
	ret := testutil.EmptyBatchFromSchema(schema, primaryKeyIdx)
	for {
		done, err := reader.Read(ctx, ret.Attrs, nil, mp, ret)
		require.NoError(t, err)
		total += ret.RowCount()
		if done {
			break
		}
	}
	reader.Close()
	return total
}

// Test_Issue25557MultiEntryDumpCompaction covers the many-to-one workspace
// compaction case: two raw INSERT entries of one table, written in the same
// statement, are merged into a single S3 entry by a mid-statement dump. A
// snapshot offset captured by the (fault-injected) internal SQL inside the
// dump-resolution window must not exceed the compacted workspace: on the
// broken code the captured offset is 2 while one entry remains, and the
// same-transaction read panics with index out of range in
// ForEachTableWrites.
func Test_Issue25557MultiEntryDumpCompaction(t *testing.T) {
	var (
		err          error
		mp           *mpool.MPool
		txn          client.TxnOperator
		accountId    = catalog.System_Account
		tableName    = "test_table"
		databaseName = "test_database"

		primaryKeyIdx = 3

		relation engine.Relation

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	schema := catalog2.MockSchemaAll(4, primaryKeyIdx)
	schema.Name = tableName

	totalRows := 210
	bat := containers.ToCNBatch(catalog2.MockBatch(schema, totalRows))
	bat1, err := bat.Window(0, 10)
	require.NoError(t, err)
	bat2, err := bat.Window(10, totalRows)
	require.NoError(t, err)

	// The first write must stay below the dump threshold and the second one
	// must cross it, so that the dump sees two raw entries of one table. The
	// in-memory entry of a write is its batch plus a generated rowid vector,
	// so bat1's entry stays below bat1.Size()+bat2.Size() while both entries
	// together always cross it.
	dumpThreshold := uint64(bat1.Size() + bat2.Size())

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(
		ctx,
		testutil.TestOptions{},
		t,
		testutil.WithDisttaeEngineWriteWorkspaceThreshold(dumpThreshold),
		testutil.WithDisttaeEngineCommitWorkspaceThreshold(1),
		// an exhausted quota pool so the dump cannot be postponed
		testutil.WithDisttaeEngineQuota(1),
	)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	// getTable runs three times: the write-time PK resolution of each write,
	// then the dump-side table resolution. Only the last one must simulate
	// the internal-SQL leg, exactly like a compile hitting the dump window.
	fault.Enable()
	defer fault.Disable()
	require.NoError(t, fault.AddFaultPoint(
		ctx, objectio.FJ_CNReenterSnapshotOffsetOnGetTable, "3:3::", "echo", 1, "", false))
	defer func() {
		_, err := fault.RemoveFaultPoint(
			ctx, objectio.FJ_CNReenterSnapshotOffsetOnGetTable)
		require.NoError(t, err)
	}()

	_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
	require.NoError(t, err)

	txn.GetWorkspace().StartStatement()
	require.NoError(t, relation.Write(ctx, bat1))
	// crosses the threshold: the dump compacts both raw entries into one S3
	// entry while the fault-injected internal SQL runs in the resolution
	// window
	require.NoError(t, relation.Write(ctx, bat2))

	// Reads inside the writing statement must not observe the statement's own
	// writes; on the broken code this read panics with index out of range
	// because the captured offset (2) exceeds the compacted workspace (1).
	require.Equal(t, 0, issue25557ReadRowCount(
		t, ctx, disttaeEngine, txn, relation, schema, primaryKeyIdx, mp),
		"a statement must not see its own writes")

	// the internal SQL must not advance the statement boundary
	require.Equal(t, 0, txn.GetWorkspace().GetSnapshotWriteOffset(),
		"internal SQL must not advance snapshotWriteOffset")

	require.NoError(t, testutil.EndThisStatement(ctx, txn))
	require.Equal(t, totalRows, issue25557ReadRowCount(
		t, ctx, disttaeEngine, txn, relation, schema, primaryKeyIdx, mp),
		"rows flushed by the mid-statement dump must be visible after the statement ends")
	require.NoError(t, txn.Commit(ctx))

	// the committed data must be complete
	_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
	require.NoError(t, err)
	require.Equal(t, totalRows, issue25557ReadRowCount(
		t, ctx, disttaeEngine, txn, relation, schema, primaryKeyIdx, mp))
	require.NoError(t, txn.Commit(ctx))
}

// Test_Issue25557DumpWindowAppendKeepsPrefix orchestrates a workspace append
// while the dump-resolution window is open: table A's dump parks inside the
// window (WAIT fault), table B is written meanwhile, and the dump then
// compacts A only. Any statement boundary captured around the window must
// keep excluding both in-flight writes: on the broken code the reentrant
// update inside the window sets the boundary to 1, and after compaction the
// prefix [0, 1) is the raw entry of B instead of A — B becomes visible to
// its own statement and A's rows disappear.
func Test_Issue25557DumpWindowAppendKeepsPrefix(t *testing.T) {
	var (
		err          error
		mp           *mpool.MPool
		txn          client.TxnOperator
		accountId    = catalog.System_Account
		tableAName   = "test_table_a"
		tableBName   = "test_table_b"
		databaseName = "test_database"

		primaryKeyIdx = 3

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	schemaA := catalog2.MockSchemaAll(4, primaryKeyIdx)
	schemaA.Name = tableAName
	schemaB := catalog2.MockSchemaAll(4, primaryKeyIdx)
	schemaB.Name = tableBName

	rowsA := 200
	rowsB := 5
	batA := containers.ToCNBatch(catalog2.MockBatch(schemaA, rowsA))
	batB := containers.ToCNBatch(catalog2.MockBatch(schemaB, rowsB))

	// table A's write crosses the threshold on its own (its entry includes
	// the generated rowid vector on top of batA), while table B's small
	// write alone stays below it
	dumpThreshold := uint64(batA.Size())

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(
		ctx,
		testutil.TestOptions{},
		t,
		testutil.WithDisttaeEngineWriteWorkspaceThreshold(dumpThreshold),
		testutil.WithDisttaeEngineCommitWorkspaceThreshold(1),
		testutil.WithDisttaeEngineQuota(1),
	)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableAName, schemaA)
	require.NoError(t, err)
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName+"_b", tableBName, schemaB)
	require.NoError(t, err)

	fault.Enable()
	defer fault.Disable()
	// every getTable simulates the internal-SQL leg
	require.NoError(t, fault.AddFaultPoint(
		ctx, objectio.FJ_CNReenterSnapshotOffsetOnGetTable, ":::", "echo", 1, "", false))
	defer func() {
		_, err := fault.RemoveFaultPoint(
			ctx, objectio.FJ_CNReenterSnapshotOffsetOnGetTable)
		require.NoError(t, err)
	}()
	// only the first dump (table A's) parks inside its resolution window
	require.NoError(t, fault.AddFaultPoint(
		ctx, objectio.FJ_CNDumpResolveWindowWait, "1:1::", "WAIT", 0, "", false))
	defer func() {
		_, _ = fault.RemoveFaultPoint(ctx, objectio.FJ_CNDumpResolveWindowWait)
	}()
	const waitersProbe = "issue25557_window_waiters"
	const windowNotify = "issue25557_window_notify"
	require.NoError(t, fault.AddFaultPoint(
		ctx, waitersProbe, ":::", "GETWAITERS", 0, objectio.FJ_CNDumpResolveWindowWait, false))
	defer func() {
		_, _ = fault.RemoveFaultPoint(ctx, waitersProbe)
	}()
	require.NoError(t, fault.AddFaultPoint(
		ctx, windowNotify, ":::", "NOTIFYALL", 0, objectio.FJ_CNDumpResolveWindowWait, false))
	defer func() {
		_, _ = fault.RemoveFaultPoint(ctx, windowNotify)
	}()

	_, relationA, txn, err := disttaeEngine.GetTable(ctx, databaseName, tableAName)
	require.NoError(t, err)
	dbB, err := disttaeEngine.Engine.Database(ctx, databaseName+"_b", txn)
	require.NoError(t, err)
	relationB, err := dbB.Relation(ctx, tableBName, nil)
	require.NoError(t, err)

	txn.GetWorkspace().StartStatement()

	// table A's write triggers the dump, which parks inside the resolution
	// window with the transaction lock released
	writeADone := make(chan error, 1)
	go func() {
		writeADone <- relationA.Write(ctx, batA)
	}()

	waitDeadline := time.Now().Add(10 * time.Second)
	for {
		n, _, ok := fault.TriggerFault(waitersProbe)
		require.True(t, ok)
		if n >= 1 {
			break
		}
		require.True(t, time.Now().Before(waitDeadline),
			"table A's dump never reached the resolution window")
		time.Sleep(10 * time.Millisecond)
	}

	// append table B's write while the window is open
	require.NoError(t, relationB.Write(ctx, batB))

	// resume table A's dump
	_, _, ok := fault.TriggerFault(windowNotify)
	require.True(t, ok)

	select {
	case err = <-writeADone:
		require.NoError(t, err)
	case <-time.After(20 * time.Second):
		t.Fatal("table A's write did not finish: dump deadlocked")
	}

	// the statement boundary must still be at the statement start: table B's
	// raw entry must not have slipped into the visible prefix
	require.Equal(t, 0, txn.GetWorkspace().GetSnapshotWriteOffset(),
		"internal SQL must not advance snapshotWriteOffset")
	require.Equal(t, 0, issue25557ReadRowCount(
		t, ctx, disttaeEngine, txn, relationB, schemaB, primaryKeyIdx, mp),
		"a statement must not see its own writes (table B)")
	require.Equal(t, 0, issue25557ReadRowCount(
		t, ctx, disttaeEngine, txn, relationA, schemaA, primaryKeyIdx, mp),
		"a statement must not see its own writes (table A)")

	require.NoError(t, testutil.EndThisStatement(ctx, txn))
	require.Equal(t, rowsA, issue25557ReadRowCount(
		t, ctx, disttaeEngine, txn, relationA, schemaA, primaryKeyIdx, mp))
	require.Equal(t, rowsB, issue25557ReadRowCount(
		t, ctx, disttaeEngine, txn, relationB, schemaB, primaryKeyIdx, mp))
	require.NoError(t, txn.Commit(ctx))

	// the committed data must be complete for both tables
	_, relationA, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableAName)
	require.NoError(t, err)
	require.Equal(t, rowsA, issue25557ReadRowCount(
		t, ctx, disttaeEngine, txn, relationA, schemaA, primaryKeyIdx, mp))
	require.NoError(t, txn.Commit(ctx))

	_, relationB, txn, err = disttaeEngine.GetTable(ctx, databaseName+"_b", tableBName)
	require.NoError(t, err)
	require.Equal(t, rowsB, issue25557ReadRowCount(
		t, ctx, disttaeEngine, txn, relationB, schemaB, primaryKeyIdx, mp))
	require.NoError(t, txn.Commit(ctx))
}

// Test_Issue25557ObjectCompactionNoDeadlock covers the object-deletion
// compaction leg of issue #25557: a transaction dumps an insert to S3,
// deletes a row of the uncommitted block, and commits. The commit runs
// IncrStatementID -> mergeTxnWorkspaceLocked -> compactDeletionOnObjsLocked
// while holding the transaction lock; on the broken code the compaction
// workers call getTable, whose (fault-injected) internal SQL waits for the
// same lock while the lock owner waits for the workers — a cross-goroutine
// deadlock.
func Test_Issue25557ObjectCompactionNoDeadlock(t *testing.T) {
	var (
		err          error
		mp           *mpool.MPool
		accountId    = catalog.System_Account
		tableName    = "test_table"
		databaseName = "test_database"

		primaryKeyIdx = 3

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	_ = colexec.NewServer(nil)

	schema := catalog2.MockSchemaEnhanced(4, primaryKeyIdx, 9)
	schema.Name = tableName

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(
		ctx,
		testutil.TestOptions{},
		t,
		testutil.WithDisttaeEngineInsertEntryMaxCount(1),
		testutil.WithDisttaeEngineCommitWorkspaceThreshold(1),
		testutil.WithDisttaeEngineWriteWorkspaceThreshold(1),
		testutil.WithDisttaeEngineQuota(1),
	)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	fault.Enable()
	defer fault.Disable()
	require.NoError(t, fault.AddFaultPoint(
		ctx, objectio.FJ_CNReenterSnapshotOffsetOnGetTable, ":::", "echo", 1, "", false))
	defer func() {
		_, err := fault.RemoveFaultPoint(
			ctx, objectio.FJ_CNReenterSnapshotOffsetOnGetTable)
		require.NoError(t, err)
	}()

	insertCnt := 150
	deleteCnt := 1
	var tombstoneBat *batch.Batch

	_, table, txn, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
	require.NoError(t, err)

	// the tiny write threshold flushes the insert to S3 during the write
	bat := catalog2.MockBatch(schema, insertCnt)
	require.NoError(t, table.Write(ctx, containers.ToCNBatch(bat)))

	entryCnt := 0
	txn.GetWorkspace().(*disttae.Transaction).ForEachTableWrites(
		table.GetDBID(ctx), table.GetTableID(ctx), 1, func(entry disttae.Entry) {
			if entry.Bat() == nil ||
				entry.Bat().RowCount() == 0 ||
				entry.FileName() == "" {
				return
			}
			entryCnt++

			tombstoneBat = batch.NewWithSize(1)
			tombstoneBat.Attrs = append(tombstoneBat.Attrs, catalog.Row_ID)
			tombstoneBat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())

			blk := objectio.DecodeBlockInfo(entry.Bat().GetVector(0).GetBytesAt(0))
			for i := 0; i < deleteCnt; i++ {
				rid := types.NewRowid(&blk.BlockID, uint32(i))
				require.NoError(t, vector.AppendFixed[types.Rowid](
					tombstoneBat.Vecs[0], rid, false, mp))
			}
			tombstoneBat.SetRowCount(deleteCnt)
		})
	require.Equal(t, 1, entryCnt)

	// deleting rows of the uncommitted block feeds txn.deletedBlocks, so the
	// commit must compact the dumped object
	require.NoError(t, table.Delete(ctx, tombstoneBat, catalog.Row_ID))

	commitDone := make(chan error, 1)
	go func() {
		commitDone <- txn.Commit(ctx)
	}()
	select {
	case err = <-commitDone:
		require.NoError(t, err)
	case <-time.After(20 * time.Second):
		t.Fatal("commit did not finish: object-deletion compaction deadlocked")
	}

	// the committed data must be complete
	{
		_, _, reader, err := testutil.GetTableTxnReader(
			ctx,
			disttaeEngine,
			databaseName,
			tableName,
			nil,
			mp,
			t,
		)
		require.NoError(t, err)

		ret := testutil.EmptyBatchFromSchema(schema, primaryKeyIdx)
		_, err = reader.Read(ctx, ret.Attrs, nil, mp, ret)
		require.NoError(t, err)
		require.Equal(t, insertCnt-deleteCnt, ret.RowCount())
	}
}

// Test_Issue25557MultiEntryDeleteDumpAtCommit covers the DELETE twin of the
// many-to-one compaction case: two raw DELETE entries of one table are
// merged into a single tombstone object by the commit-time dump, with the
// fault-injected internal SQL running in the dump-resolution window.
func Test_Issue25557MultiEntryDeleteDumpAtCommit(t *testing.T) {
	var (
		err          error
		mp           *mpool.MPool
		txn          client.TxnOperator
		accountId    = catalog.System_Account
		tableName    = "test_table"
		databaseName = "test_database"

		primaryKeyIdx = 3

		relation engine.Relation

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	schema := catalog2.MockSchemaAll(4, primaryKeyIdx)
	schema.Name = tableName

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(
		ctx,
		testutil.TestOptions{},
		t,
		// force the commit-time dump of the delete entries
		testutil.WithDisttaeEngineInsertEntryMaxCount(5),
	)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	rowsCount := 20
	bat := containers.ToCNBatch(catalog2.MockBatch(schema, rowsCount))

	// insert and commit the rows the deletes will target
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)
		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat, false, true))
		require.NoError(t, txn.Commit(ctx))
	}

	fault.Enable()
	defer fault.Disable()
	require.NoError(t, fault.AddFaultPoint(
		ctx, objectio.FJ_CNReenterSnapshotOffsetOnGetTable, ":::", "echo", 1, "", false))
	defer func() {
		_, err := fault.RemoveFaultPoint(
			ctx, objectio.FJ_CNReenterSnapshotOffsetOnGetTable)
		require.NoError(t, err)
	}()

	// collect the rowids and pks of the first 10 rows
	deleteRows := 10
	tombstoneBat := batch.NewWithSize(2)
	tombstoneBat.Attrs = []string{catalog.Row_ID, schema.GetPrimaryKey().Name}
	tombstoneBat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	tombstoneBat.Vecs[1] = vector.NewVec(schema.GetPrimaryKey().Type)

	_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
	require.NoError(t, err)
	{
		reader, err := testutil.GetRelationReader(
			ctx, disttaeEngine, txn, relation, nil, mp, t)
		require.NoError(t, err)

		ret := batch.NewWithSize(2)
		ret.Attrs = []string{schema.GetPrimaryKey().Name, catalog.Row_ID}
		ret.Vecs = []*vector.Vector{
			vector.NewVec(schema.GetPrimaryKey().Type),
			vector.NewVec(types.T_Rowid.ToType()),
		}
		for {
			done, err := reader.Read(ctx, ret.Attrs, nil, mp, ret)
			require.NoError(t, err)
			if done {
				break
			}
			for i := 0; i < ret.RowCount() && tombstoneBat.Vecs[0].Length() < deleteRows; i++ {
				require.NoError(t, vector.AppendFixed[types.Rowid](
					tombstoneBat.Vecs[0],
					vector.GetFixedAtWithTypeCheck[types.Rowid](ret.Vecs[1], i),
					false, mp))
				require.NoError(t, vector.AppendFixed[int64](
					tombstoneBat.Vecs[1],
					vector.GetFixedAtWithTypeCheck[int64](ret.Vecs[0], i),
					false, mp))
			}
		}
		tombstoneBat.SetRowCount(tombstoneBat.Vecs[0].Length())
		require.Equal(t, deleteRows, tombstoneBat.RowCount())
	}

	// two DELETE entries of one table in the same transaction
	tb1, err := tombstoneBat.Window(0, deleteRows/2)
	require.NoError(t, err)
	tb2, err := tombstoneBat.Window(deleteRows/2, deleteRows)
	require.NoError(t, err)
	require.NoError(t, relation.Delete(ctx, tb1, catalog.Row_ID))
	require.NoError(t, relation.Delete(ctx, tb2, catalog.Row_ID))

	// the commit-time dump merges both entries into one tombstone object
	commitDone := make(chan error, 1)
	go func() {
		commitDone <- txn.Commit(ctx)
	}()
	select {
	case err = <-commitDone:
		require.NoError(t, err)
	case <-time.After(20 * time.Second):
		t.Fatal("commit did not finish: delete dump deadlocked")
	}

	// the committed data must be complete
	_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
	require.NoError(t, err)
	require.Equal(t, rowsCount-deleteRows, issue25557ReadRowCount(
		t, ctx, disttaeEngine, txn, relation, schema, primaryKeyIdx, mp))
	require.NoError(t, txn.Commit(ctx))
}

// #endregion
// #region multi-txn test

// txn1 insert 0-10
// txn1 commit
// txn2 insert 10-20
// txn2 delete 5-15
// txn2 commit
// read -> 10 rows
func Test_MultiTxnInsertDelete(t *testing.T) {
	var (
		err          error
		mp           *mpool.MPool
		txn          client.TxnOperator
		accountId    = catalog.System_Account
		tableName    = "test_table"
		databaseName = "test_database"

		primaryKeyIdx = 3

		relation engine.Relation
		_        engine.Database

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	schema := catalog2.MockSchemaAll(4, primaryKeyIdx)
	schema.Name = tableName

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(
		ctx,
		testutil.TestOptions{},
		t,
	)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	rowsCount := 20
	bat := containers.ToCNBatch(catalog2.MockBatch(schema, rowsCount))
	bat1, err := bat.Window(0, 10)
	require.NoError(t, err)
	bat2, err := bat.Window(10, 20)
	require.NoError(t, err)

	// txn1 insert 0-10
	// txn1 commit
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat1, false, true))
		require.NoError(t, txn.Commit(ctx))
	}

	// read row id and pk data
	tombstoneBat := batch.NewWithSize(2)
	tombstoneBat.Attrs = []string{catalog.Row_ID, schema.GetPrimaryKey().Name}
	tombstoneBat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	tombstoneBat.Vecs[1] = vector.NewVec(types.T_int64.ToType())

	_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
	require.NoError(t, err)
	{
		reader, err := testutil.GetRelationReader(
			ctx,
			disttaeEngine,
			txn,
			relation,
			nil,
			mp,
			t,
		)
		require.NoError(t, err)

		ret := batch.NewWithSize(2)
		ret.Attrs = []string{schema.GetPrimaryKey().Name, catalog.Row_ID}
		ret.Vecs = []*vector.Vector{vector.NewVec(schema.GetPrimaryKey().Type), vector.NewVec(types.T_Rowid.ToType())}

		for {
			done, err := reader.Read(ctx, ret.Attrs, nil, mp, ret)

			if done {
				break
			}

			require.NoError(t, err)
			for i := range ret.RowCount() {
				err = vector.AppendFixed[types.Rowid](
					tombstoneBat.Vecs[0],
					vector.GetFixedAtWithTypeCheck[types.Rowid](ret.Vecs[1], i),
					false, mp)
				require.NoError(t, err)

				err = vector.AppendFixed[int64](
					tombstoneBat.Vecs[1],
					vector.GetFixedAtWithTypeCheck[int64](ret.Vecs[0], i),
					false, mp)
				require.NoError(t, err)
			}
		}

		tombstoneBat.SetRowCount(tombstoneBat.Vecs[0].Length())
		require.Equal(t, bat1.RowCount(), tombstoneBat.Vecs[0].Length())
	}

	// txn2 insert 10-20
	// txn2 delete 5-15
	// txn2 commit
	{
		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat2, false, true))

		txn.GetWorkspace().(*disttae.Transaction).ForEachTableWrites(
			relation.GetDBID(ctx), relation.GetTableID(ctx), 1, func(entry disttae.Entry) {
				waitedDeletes := vector.MustFixedColWithTypeCheck[types.Rowid](entry.Bat().GetVector(0))
				require.NoError(t, vector.AppendFixedList[types.Rowid](tombstoneBat.Vecs[0], waitedDeletes, nil, mp))
				tombstoneBat.SetRowCount(tombstoneBat.RowCount() + len(waitedDeletes))
			})

		bat, err := tombstoneBat.Window(5, 15)
		require.NoError(t, err)
		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat, true, true))
		require.NoError(t, txn.Commit(ctx))
	}

	_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
	require.NoError(t, err)
	reader, err := testutil.GetRelationReader(
		ctx,
		disttaeEngine,
		txn,
		relation,
		nil,
		mp,
		t,
	)
	require.NoError(t, err)

	ret := testutil.EmptyBatchFromSchema(schema, primaryKeyIdx)
	_, err = reader.Read(ctx, ret.Attrs, nil, mp, ret)
	require.NoError(t, err)

	require.Equal(t, 10, ret.RowCount())
	require.NoError(t, txn.Commit(ctx))
}

// txn1 insert 0-10
// txn1 commit
// txn2 insert 10-20
// txn2 delete 5-15
// txn2 commit
// read -> 10 rows
func Test_MultiTxnS3InsertDelete(t *testing.T) {
	var (
		err          error
		mp           *mpool.MPool
		txn          client.TxnOperator
		accountId    = catalog.System_Account
		tableName    = "test_table"
		databaseName = "test_database"

		primaryKeyIdx = 3

		relation engine.Relation
		_        engine.Database

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	schema := catalog2.MockSchemaAll(4, primaryKeyIdx)
	schema.Name = tableName

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(
		ctx,
		testutil.TestOptions{},
		t,
		testutil.WithDisttaeEngineInsertEntryMaxCount(1),
		testutil.WithDisttaeEngineCommitWorkspaceThreshold(1),
	)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	rowsCount := 20
	bat := containers.ToCNBatch(catalog2.MockBatch(schema, rowsCount))
	bat1, err := bat.Window(0, 10)
	require.NoError(t, err)
	bat2, err := bat.Window(10, 20)
	require.NoError(t, err)

	// txn1 insert 0-10
	// txn1 commit
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat1, false, true))
		require.NoError(t, txn.Commit(ctx))
	}

	// txn2 insert 10-20
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)
		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat2, false, true))
	}

	// read row id and pk data
	tombstoneBat := batch.NewWithSize(2)
	tombstoneBat.Attrs = []string{catalog.Row_ID, schema.GetPrimaryKey().Name}
	tombstoneBat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	tombstoneBat.Vecs[1] = vector.NewVec(types.T_int64.ToType())

	{
		reader, err := testutil.GetRelationReader(
			ctx,
			disttaeEngine,
			txn,
			relation,
			nil,
			mp,
			t,
		)
		require.NoError(t, err)

		ret := batch.NewWithSize(2)
		ret.Attrs = []string{schema.GetPrimaryKey().Name, catalog.Row_ID}
		ret.Vecs = []*vector.Vector{vector.NewVec(schema.GetPrimaryKey().Type), vector.NewVec(types.T_Rowid.ToType())}

		for {
			done, err := reader.Read(ctx, ret.Attrs, nil, mp, ret)

			if done {
				break
			}

			require.NoError(t, err)
			for i := range ret.RowCount() {
				err = vector.AppendFixed[types.Rowid](
					tombstoneBat.Vecs[0],
					vector.GetFixedAtWithTypeCheck[types.Rowid](ret.Vecs[1], i),
					false, mp)
				require.NoError(t, err)

				err = vector.AppendFixed[int64](
					tombstoneBat.Vecs[1],
					vector.GetFixedAtWithTypeCheck[int64](ret.Vecs[0], i),
					false, mp)
				require.NoError(t, err)
			}
		}

		tombstoneBat.SetRowCount(tombstoneBat.Vecs[0].Length())
		require.Equal(t, bat.RowCount(), tombstoneBat.Vecs[0].Length())
	}

	// txn2 insert 10-20
	{
		bat, err := tombstoneBat.Window(5, 15)
		require.NoError(t, err)
		require.Equal(t, 10, bat.RowCount())
		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat, true, true))
		require.NoError(t, txn.Commit(ctx))
	}
	_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
	require.NoError(t, err)

	reader, err := testutil.GetRelationReader(
		ctx,
		disttaeEngine,
		txn,
		relation,
		nil,
		mp,
		t,
	)
	require.NoError(t, err)

	ret := testutil.EmptyBatchFromSchema(schema, primaryKeyIdx)
	cnt := 0
	for {
		done, err := reader.Read(ctx, ret.Attrs, nil, mp, ret)
		require.NoError(t, err)
		cnt += ret.RowCount()

		if done {
			break
		}
	}

	require.Equal(t, 10, cnt)
	require.NoError(t, txn.Commit(ctx))
}

func Test_MultiTxnS3Tombstones(t *testing.T) {
	var (
		err          error
		mp           *mpool.MPool
		txn          client.TxnOperator
		accountId    = catalog.System_Account
		tableName    = "test_table"
		databaseName = "test_database"

		relation engine.Relation
		_        engine.Database

		primaryKeyIdx = 1

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	schema := catalog2.MockSchemaEnhanced(2, primaryKeyIdx, 2)
	schema.Name = tableName

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(
		ctx,
		testutil.TestOptions{},
		t,
		testutil.WithDisttaeEngineInsertEntryMaxCount(1),
		testutil.WithDisttaeEngineCommitWorkspaceThreshold(1),
	)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	rowsCount := 30
	bat := catalog2.MockBatch(schema, 0)
	for i := range rowsCount {
		bat.Vecs[0].Append(int32(i/10), false)
		bat.Vecs[1].Append(int64(i), false)
	}

	// txn1 insert 0-30
	// txn1 commit
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, containers.ToCNBatch(bat), false, true))
		require.NoError(t, txn.Commit(ctx))
	}

	// read row id and pk data
	tombstoneBat := batch.NewWithSize(2)
	tombstoneBat.Attrs = []string{catalog.Row_ID, schema.GetPrimaryKey().Name}
	tombstoneBat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	tombstoneBat.Vecs[1] = vector.NewVec(types.T_int64.ToType())

	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)
		reader, err := testutil.GetRelationReader(
			ctx,
			disttaeEngine,
			txn,
			relation,
			nil,
			mp,
			t,
		)
		require.NoError(t, err)

		ret := batch.NewWithSize(2)
		ret.Attrs = []string{schema.GetPrimaryKey().Name, catalog.Row_ID}
		ret.Vecs = []*vector.Vector{vector.NewVec(schema.GetPrimaryKey().Type), vector.NewVec(types.T_Rowid.ToType())}

		for {
			done, err := reader.Read(ctx, ret.Attrs, nil, mp, ret)

			if done {
				break
			}

			require.NoError(t, err)
			for i := range ret.RowCount() {
				err = vector.AppendFixed[types.Rowid](
					tombstoneBat.Vecs[0],
					vector.GetFixedAtWithTypeCheck[types.Rowid](ret.Vecs[1], i),
					false, mp)
				require.NoError(t, err)

				err = vector.AppendFixed[int64](
					tombstoneBat.Vecs[1],
					vector.GetFixedAtWithTypeCheck[int64](ret.Vecs[0], i),
					false, mp)
				require.NoError(t, err)
			}
		}

		tombstoneBat.SetRowCount(tombstoneBat.Vecs[0].Length())
		require.Equal(t, bat.Length(), tombstoneBat.Vecs[0].Length())
		require.NoError(t, txn.Commit(ctx))
	}

	bat1, _ := tombstoneBat.Window(0, 10)
	bat2, _ := tombstoneBat.Window(10, 20)

	// txn2 delete 0-10
	// txn2 commit
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat1, true, true))
		require.NoError(t, txn.Commit(ctx))
	}

	// txn3 delete 10-20
	// txn3 commit
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat2, true, true))
		require.NoError(t, txn.Commit(ctx))
	}

	_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
	require.NoError(t, err)

	reader, err := testutil.GetRelationReader(
		ctx,
		disttaeEngine,
		txn,
		relation,
		nil,
		mp,
		t,
	)
	require.NoError(t, err)

	ret := testutil.EmptyBatchFromSchema(schema)
	cnt := 0
	for {
		done, err := reader.Read(ctx, ret.Attrs, nil, mp, ret)
		require.NoError(t, err)
		cnt += ret.RowCount()

		if done {
			break
		}
	}

	require.Equal(t, 10, cnt)
	require.NoError(t, txn.Commit(ctx))
}

// #endregion
// #region rollback test

// insert 0-10
// insert 10-20 and rollback
// delete 0-5 and rollback
// delete 0-5 and commit
// read -> 5 rows
func Test_BasicRollbackStatement(t *testing.T) {
	var (
		err          error
		mp           *mpool.MPool
		txn          client.TxnOperator
		accountId    = catalog.System_Account
		tableName    = "test_table"
		databaseName = "test_database"

		primaryKeyIdx = 3

		relation engine.Relation
		_        engine.Database

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	schema := catalog2.MockSchemaAll(4, primaryKeyIdx)
	schema.Name = tableName

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(
		ctx,
		testutil.TestOptions{},
		t,
	)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	rowsCount := 20
	bat := catalog2.MockBatch(schema, rowsCount)
	bat1 := containers.ToCNBatch(bat.Window(0, 10))
	bat2 := containers.ToCNBatch(bat.Window(10, 10))

	// write table
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat1, false, true))

		txn.GetWorkspace().StartStatement()
		require.NoError(t, relation.Write(ctx, bat2))
		require.NoError(t, txn.GetWorkspace().RollbackLastStatement(ctx))
		require.NoError(t, txn.GetWorkspace().IncrStatementID(ctx, false))
	}

	{
		var tombstoneBat *batch.Batch
		txn.GetWorkspace().(*disttae.Transaction).ForEachTableWrites(
			relation.GetDBID(ctx), relation.GetTableID(ctx), 1, func(entry disttae.Entry) {
				waitedDeletes := vector.MustFixedColWithTypeCheck[types.Rowid](entry.Bat().GetVector(0))
				waitedDeletes = waitedDeletes[:rowsCount/2]
				tombstoneBat = batch.NewWithSize(1)
				tombstoneBat.Attrs = append(tombstoneBat.Attrs, catalog.Row_ID)
				tombstoneBat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
				require.NoError(t, vector.AppendFixedList[types.Rowid](tombstoneBat.Vecs[0], waitedDeletes, nil, mp))
				tombstoneBat.SetRowCount(len(waitedDeletes))
			})

		tombstoneBat2, err := tombstoneBat.Window(0, 5)
		require.NoError(t, err)

		require.NoError(t, relation.Delete(ctx, tombstoneBat, catalog.Row_ID))
		require.NoError(t, txn.GetWorkspace().RollbackLastStatement(ctx))
		require.NoError(t, txn.GetWorkspace().IncrStatementID(ctx, false))

		require.NoError(t, relation.Delete(ctx, tombstoneBat2, catalog.Row_ID))
		require.NoError(t, txn.Commit(ctx))
	}

	_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
	require.NoError(t, err)

	reader, err := testutil.GetRelationReader(
		ctx,
		disttaeEngine,
		txn,
		relation,
		nil,
		mp,
		t,
	)
	require.NoError(t, err)

	ret := testutil.EmptyBatchFromSchema(schema, primaryKeyIdx)
	_, err = reader.Read(ctx, ret.Attrs, nil, mp, ret)
	require.NoError(t, err)

	require.Equal(t, 5, ret.RowCount())
	require.NoError(t, txn.Commit(ctx))
}

// insert 0-10
// insert 10-20 and rollback
// delete 0-5 and rollback
// delete 0-5 and commit
// read -> 5 rows
func Test_BasicRollbackStatementS3(t *testing.T) {
	var (
		err          error
		mp           *mpool.MPool
		txn          client.TxnOperator
		accountId    = catalog.System_Account
		tableName    = "test_table"
		databaseName = "test_database"

		primaryKeyIdx = 3

		relation engine.Relation
		_        engine.Database

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	schema := catalog2.MockSchemaAll(4, primaryKeyIdx)
	schema.Name = tableName

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(
		ctx,
		testutil.TestOptions{},
		t,
		testutil.WithDisttaeEngineInsertEntryMaxCount(1),
		testutil.WithDisttaeEngineCommitWorkspaceThreshold(1),
	)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	rowsCount := 20
	bat := catalog2.MockBatch(schema, rowsCount)
	bat1 := containers.ToCNBatch(bat.Window(0, 10))
	bat2 := containers.ToCNBatch(bat.Window(10, 10))

	// write table
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat1, false, true))

		txn.GetWorkspace().StartStatement()
		require.NoError(t, relation.Write(ctx, bat2))
		require.NoError(t, txn.GetWorkspace().RollbackLastStatement(ctx))
		require.NoError(t, txn.GetWorkspace().IncrStatementID(ctx, false))
	}

	// read row id
	tombstoneBat := batch.NewWithSize(1)
	tombstoneBat.Attrs = append(tombstoneBat.Attrs, catalog.Row_ID)
	tombstoneBat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	{
		reader, err := testutil.GetRelationReader(
			ctx,
			disttaeEngine,
			txn,
			relation,
			nil,
			mp,
			t,
		)
		require.NoError(t, err)

		ret := batch.NewWithSize(1)
		ret.Attrs = []string{catalog.Row_ID}
		ret.Vecs = []*vector.Vector{vector.NewVec(types.T_Rowid.ToType())}

		for {
			done, err := reader.Read(ctx, ret.Attrs, nil, mp, ret)

			if done {
				break
			}

			require.NoError(t, err)
			for i := range ret.RowCount() {
				err = vector.AppendFixed[types.Rowid](
					tombstoneBat.Vecs[0],
					vector.GetFixedAtWithTypeCheck[types.Rowid](ret.Vecs[0], i),
					false, mp)
				require.NoError(t, err)
			}
		}

		tombstoneBat.SetRowCount(tombstoneBat.Vecs[0].Length())
		require.Equal(t, bat1.RowCount(), tombstoneBat.Vecs[0].Length())
	}

	// delete batch
	{
		tb1, err := tombstoneBat.Window(0, 5)
		require.NoError(t, err)
		tb2, err := tombstoneBat.Window(5, 10)
		require.NoError(t, err)

		require.NoError(t, relation.Delete(ctx, tb1, catalog.Row_ID))
		require.NoError(t, txn.GetWorkspace().RollbackLastStatement(ctx))
		require.NoError(t, txn.GetWorkspace().IncrStatementID(ctx, false))

		require.NoError(t, relation.Delete(ctx, tb2, catalog.Row_ID))
		require.NoError(t, txn.Commit(ctx))
	}

	_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)

	require.NoError(t, err)
	reader, err := testutil.GetRelationReader(
		ctx,
		disttaeEngine,
		txn,
		relation,
		nil,
		mp,
		t,
	)
	require.NoError(t, err)

	ret := testutil.EmptyBatchFromSchema(schema, primaryKeyIdx)
	_, err = reader.Read(ctx, ret.Attrs, nil, mp, ret)
	require.NoError(t, err)

	require.Equal(t, 5, ret.RowCount())
	require.NoError(t, txn.Commit(ctx))
}

// https://github.com/matrixorigin/MO-Cloud/issues/4602
func Test_RollbackDeleteAndDrop(t *testing.T) {
	opts := config.WithLongScanAndCKPOpts(nil)
	p := testutil.InitEnginePack(testutil.TestOptions{
		TaeEngineOptions: opts,
		DisttaeOptions:   []testutil.TestDisttaeEngineOptions{testutil.WithDisttaeEngineInsertEntryMaxCount(5)}},
		t)
	defer p.Close()

	schema := catalog2.MockSchemaAll(10, 1)
	schema.Name = "test"
	schema2 := catalog2.MockSchemaAll(10, 1)
	schema2.Name = "test2"
	schema3 := catalog2.MockSchemaAll(10, 1)
	schema3.Name = "test3"
	txnop := p.StartCNTxn()

	bat := catalog2.MockBatch(schema, 10)
	_, rels := p.CreateDBAndTables(txnop, "db", schema, schema2, schema3)
	require.NoError(t, rels[2].Write(p.Ctx, containers.ToCNBatch(bat)))
	require.NoError(t, txnop.Commit(p.Ctx))

	v, ok := runtime.ServiceRuntime("").GetGlobalVariables(runtime.InternalSQLExecutor)
	if !ok {
		panic(fmt.Sprintf("missing sql executor in service %q", ""))
	}
	txnop = p.StartCNTxn()
	exec := v.(executor.SQLExecutor)
	execopts := executor.Options{}.WithTxn(txnop).WithDisableIncrStatement()
	txnop.GetWorkspace().StartStatement()
	txnop.GetWorkspace().IncrStatementID(p.Ctx, false)
	dropTable := func() {
		_, err := exec.Exec(p.Ctx, "delete from db.test3 where mock_1 = 0", execopts)
		require.NoError(t, err)
		p.DeleteTableInDB(txnop, "db", "test")
		p.DeleteTableInDB(txnop, "db", "test2")
		_, err = exec.Exec(p.Ctx, "delete from db.test3 where mock_1 = 2", execopts)
		require.NoError(t, err)
	}
	dropTable() // approximateInMemDeleteCnt = 2
	txnop.GetWorkspace().RollbackLastStatement(p.Ctx)
	txnop.GetWorkspace().IncrStatementID(p.Ctx, false)
	dropTable() // approximateInMemDeleteCnt = 4
	txnop.GetWorkspace().RollbackLastStatement(p.Ctx)
	txnop.GetWorkspace().IncrStatementID(p.Ctx, false)
	dropTable() // approximateInMemDeleteCnt = 6
	t.Log(txnop.GetWorkspace().PPString())
	err := txnop.Commit(p.Ctx) // dumpDeleteBatchLocked messes up the writes list and get bad write format error
	require.NoError(t, err)
}

// #endregion
// #region multi-txn rollback test

// txn1 insert 0-10
// txn1 commit
// txn2 insert 10-20
// txn2 insert 20-30 and rollback
// txn2 delete 5-15 and rollback
// txn2 delete 5-15
// txn2 commit
// read -> 10 rows
func Test_MultiTxnRollbackStatement(t *testing.T) {
	var (
		err          error
		mp           *mpool.MPool
		txn          client.TxnOperator
		accountId    = catalog.System_Account
		tableName    = "test_table"
		databaseName = "test_database"

		primaryKeyIdx = 3

		relation engine.Relation
		_        engine.Database

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	schema := catalog2.MockSchemaAll(4, primaryKeyIdx)
	schema.Name = tableName

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(
		ctx,
		testutil.TestOptions{},
		t,
	)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	rowsCount := 20
	bat := containers.ToCNBatch(catalog2.MockBatch(schema, rowsCount))
	bat1, err := bat.Window(0, 10)
	require.NoError(t, err)
	bat2, err := bat.Window(10, 20)
	require.NoError(t, err)

	// txn1 insert 0-10
	// txn1 commit
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat1, false, true))
		require.NoError(t, txn.Commit(ctx))
	}

	// read row id and pk data
	tombstoneBat := batch.NewWithSize(2)
	tombstoneBat.Attrs = []string{catalog.Row_ID, schema.GetPrimaryKey().Name}
	tombstoneBat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	tombstoneBat.Vecs[1] = vector.NewVec(types.T_int64.ToType())

	_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
	require.NoError(t, err)
	{
		reader, err := testutil.GetRelationReader(
			ctx,
			disttaeEngine,
			txn,
			relation,
			nil,
			mp,
			t,
		)
		require.NoError(t, err)

		ret := batch.NewWithSize(2)
		ret.Attrs = []string{schema.GetPrimaryKey().Name, catalog.Row_ID}
		ret.Vecs = []*vector.Vector{vector.NewVec(schema.GetPrimaryKey().Type), vector.NewVec(types.T_Rowid.ToType())}

		for {
			done, err := reader.Read(ctx, ret.Attrs, nil, mp, ret)

			if done {
				break
			}

			require.NoError(t, err)
			for i := range ret.RowCount() {
				err = vector.AppendFixed[types.Rowid](
					tombstoneBat.Vecs[0],
					vector.GetFixedAtWithTypeCheck[types.Rowid](ret.Vecs[1], i),
					false, mp)
				require.NoError(t, err)

				err = vector.AppendFixed[int64](
					tombstoneBat.Vecs[1],
					vector.GetFixedAtWithTypeCheck[int64](ret.Vecs[0], i),
					false, mp)
				require.NoError(t, err)
			}
		}

		tombstoneBat.SetRowCount(tombstoneBat.Vecs[0].Length())
		require.Equal(t, bat1.RowCount(), tombstoneBat.Vecs[0].Length())
	}

	// txn2 insert 10-20
	// txn2 delete 5-15
	// txn2 commit
	{
		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat2, false, true))

		txn.GetWorkspace().StartStatement()
		require.NoError(t, relation.Write(ctx, bat2))
		require.NoError(t, txn.GetWorkspace().RollbackLastStatement(ctx))
		require.NoError(t, txn.GetWorkspace().IncrStatementID(ctx, false))

		txn.GetWorkspace().(*disttae.Transaction).ForEachTableWrites(
			relation.GetDBID(ctx), relation.GetTableID(ctx), 1, func(entry disttae.Entry) {
				waitedDeletes := vector.MustFixedColWithTypeCheck[types.Rowid](entry.Bat().GetVector(0))
				require.NoError(t, vector.AppendFixedList[types.Rowid](tombstoneBat.Vecs[0], waitedDeletes, nil, mp))
				tombstoneBat.SetRowCount(tombstoneBat.RowCount() + len(waitedDeletes))
			})

		tb1, err := tombstoneBat.Window(5, 15)
		require.NoError(t, err)
		tb2, err := tombstoneBat.Window(5, 15)
		require.NoError(t, err)
		require.NoError(t, relation.Delete(ctx, tb1, catalog.Row_ID))
		require.NoError(t, txn.GetWorkspace().RollbackLastStatement(ctx))
		require.NoError(t, txn.GetWorkspace().IncrStatementID(ctx, false))

		require.NoError(t, relation.Delete(ctx, tb2, catalog.Row_ID))
		require.NoError(t, txn.Commit(ctx))
	}

	_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
	require.NoError(t, err)
	reader, err := testutil.GetRelationReader(
		ctx,
		disttaeEngine,
		txn,
		relation,
		nil,
		mp,
		t,
	)
	require.NoError(t, err)

	ret := testutil.EmptyBatchFromSchema(schema, primaryKeyIdx)
	_, err = reader.Read(ctx, ret.Attrs, nil, mp, ret)
	require.NoError(t, err)

	require.Equal(t, 10, ret.RowCount())
	require.NoError(t, txn.Commit(ctx))
}

// txn1 insert 0-10
// txn1 commit
// txn2 insert 10-20
// txn2 insert 20-30 and rollback
// txn2 delete 5-15 and rollback
// txn2 delete 5-15
// txn2 commit
// read -> 10 rows
// TODO: fix me
func Test_MultiTxnRollbackStatementS3(t *testing.T) {
	t.Skip("skip")
	var (
		err          error
		mp           *mpool.MPool
		txn          client.TxnOperator
		accountId    = catalog.System_Account
		tableName    = "test_table"
		databaseName = "test_database"

		primaryKeyIdx = 3

		relation engine.Relation
		_        engine.Database

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	schema := catalog2.MockSchemaAll(4, primaryKeyIdx)
	schema.Name = tableName

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(
		ctx,
		testutil.TestOptions{},
		t,
		testutil.WithDisttaeEngineInsertEntryMaxCount(1),
		testutil.WithDisttaeEngineCommitWorkspaceThreshold(1),
	)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	rowsCount := 20
	bat := containers.ToCNBatch(catalog2.MockBatch(schema, rowsCount))
	bat1, err := bat.Window(0, 10)
	require.NoError(t, err)
	bat2, err := bat.Window(10, 20)
	require.NoError(t, err)

	// txn1 insert 0-10
	// txn1 commit
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat1, false, true))
		require.NoError(t, txn.Commit(ctx))
	}
	ws := txn.GetWorkspace().(*disttae.Transaction)
	_ = ws
	// txn2 insert 10-20
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)
		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, bat2, false, true))
	}

	// read row id and pk data
	tombstoneBat := batch.NewWithSize(2)
	tombstoneBat.Attrs = []string{catalog.Row_ID, schema.GetPrimaryKey().Name}
	tombstoneBat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	tombstoneBat.Vecs[1] = vector.NewVec(types.T_int64.ToType())

	{
		reader, err := testutil.GetRelationReader(
			ctx,
			disttaeEngine,
			txn,
			relation,
			nil,
			mp,
			t,
		)
		require.NoError(t, err)

		ret := batch.NewWithSize(2)
		ret.Attrs = []string{schema.GetPrimaryKey().Name, catalog.Row_ID}
		ret.Vecs = []*vector.Vector{vector.NewVec(schema.GetPrimaryKey().Type), vector.NewVec(types.T_Rowid.ToType())}

		for {
			done, err := reader.Read(ctx, ret.Attrs, nil, mp, ret)

			if done {
				break
			}

			require.NoError(t, err)
			for i := range ret.RowCount() {
				err = vector.AppendFixed[types.Rowid](
					tombstoneBat.Vecs[0],
					vector.GetFixedAtWithTypeCheck[types.Rowid](ret.Vecs[1], i),
					false, mp)
				require.NoError(t, err)

				err = vector.AppendFixed[int64](
					tombstoneBat.Vecs[1],
					vector.GetFixedAtWithTypeCheck[int64](ret.Vecs[0], i),
					false, mp)
				require.NoError(t, err)
			}
		}

		tombstoneBat.SetRowCount(tombstoneBat.Vecs[0].Length())
		require.Equal(t, bat.RowCount(), tombstoneBat.Vecs[0].Length())
	}

	// txn2 delete 5-15
	{
		txn.GetWorkspace().StartStatement()
		require.NoError(t, relation.Write(ctx, bat2))
		require.NoError(t, txn.GetWorkspace().RollbackLastStatement(ctx))
		require.NoError(t, txn.GetWorkspace().IncrStatementID(ctx, false))

		tb1, err := tombstoneBat.Window(5, 15)
		require.NoError(t, err)
		tb2, err := tombstoneBat.Window(5, 15)

		require.NoError(t, err)
		require.NoError(t, relation.Delete(ctx, tb1, catalog.Row_ID))
		require.NoError(t, txn.GetWorkspace().RollbackLastStatement(ctx))
		require.NoError(t, txn.GetWorkspace().IncrStatementID(ctx, false))

		require.NoError(t, relation.Delete(ctx, tb2, catalog.Row_ID))
		require.NoError(t, txn.Commit(ctx))
	}
	_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
	require.NoError(t, err)

	reader, err := testutil.GetRelationReader(
		ctx,
		disttaeEngine,
		txn,
		relation,
		nil,
		mp,
		t,
	)
	require.NoError(t, err)

	ret := testutil.EmptyBatchFromSchema(schema, primaryKeyIdx)
	cnt := 0
	for {
		done, err := reader.Read(ctx, ret.Attrs, nil, mp, ret)
		require.NoError(t, err)
		cnt += ret.RowCount()

		if done {
			break
		}
	}

	require.Equal(t, 10, cnt)
	require.NoError(t, txn.Commit(ctx))
}

func Test_DeleteUncommittedBlock(t *testing.T) {
	var (
		err          error
		mp           *mpool.MPool
		accountId    = catalog.System_Account
		tableName    = "test_reader_table"
		databaseName = "test_reader_database"

		primaryKeyIdx = 3

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	_ = colexec.NewServer(nil)

	// mock a schema with 4 columns and the 4th column as primary key
	// the first column is the 9th column in the predefined columns in
	// the mock function. Here we exepct the type of the primary key
	// is types.T_char or types.T_varchar
	schema := catalog2.MockSchemaEnhanced(4, primaryKeyIdx, 9)
	schema.Name = tableName

	{

		disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(
			ctx,
			testutil.TestOptions{},
			t,
			testutil.WithDisttaeEngineInsertEntryMaxCount(1),
			testutil.WithDisttaeEngineCommitWorkspaceThreshold(1),
			testutil.WithDisttaeEngineWriteWorkspaceThreshold(1),
			testutil.WithDisttaeEngineQuota(1),
		)
		defer func() {
			disttaeEngine.Close(ctx)
			taeEngine.Close(true)
			rpcAgent.Close()
		}()

		ctx, cancel = context.WithTimeout(ctx, time.Minute)
		defer cancel()
		_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
		require.NoError(t, err)
	}

	insertCnt := 150
	deleteCnt := 1
	var bat2 *batch.Batch

	{
		// insert 150 rows
		_, table, txn, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		bat := catalog2.MockBatch(schema, insertCnt)
		err = table.Write(ctx, containers.ToCNBatch(bat))
		require.NoError(t, err)

		entryCnt := 0
		txn.GetWorkspace().(*disttae.Transaction).ForEachTableWrites(
			table.GetDBID(ctx), table.GetTableID(ctx), 1, func(entry disttae.Entry) {
				if entry.Bat() == nil ||
					entry.Bat().RowCount() == 0 ||
					entry.FileName() == "" {
					return
				}
				entryCnt++

				bat2 = batch.NewWithSize(1)
				bat2.Attrs = append(bat2.Attrs, catalog.Row_ID)
				bat2.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())

				blk := objectio.DecodeBlockInfo(entry.Bat().GetVector(0).GetBytesAt(0))
				for i := 0; i < deleteCnt; i++ {
					rid := types.NewRowid(&blk.BlockID, uint32(i))
					require.NoError(t, vector.AppendFixed[types.Rowid](bat2.Vecs[0], rid, false, mp))
				}
				bat2.SetRowCount(deleteCnt)

			})
		require.Equal(t, 1, entryCnt)
		//delete 100 rows
		require.NoError(t, table.Delete(ctx, bat2, catalog.Row_ID))
		require.NoError(t, txn.Commit(ctx))
	}

	{
		_, _, reader, err := testutil.GetTableTxnReader(
			ctx,
			disttaeEngine,
			databaseName,
			tableName,
			nil,
			mp,
			t,
		)
		require.NoError(t, err)

		ret := testutil.EmptyBatchFromSchema(schema, primaryKeyIdx)
		_, err = reader.Read(ctx, ret.Attrs, nil, mp, ret)
		require.NoError(t, err)
		require.Equal(t, insertCnt-deleteCnt, ret.RowCount())
	}
}

func Test_BigDeleteWriteS3(t *testing.T) {
	var (
		err          error
		mp           *mpool.MPool
		accountId    = catalog.System_Account
		tableName    = "test_reader_table"
		databaseName = "test_reader_database"

		primaryKeyIdx = 3

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	// mock a schema with 4 columns and the 4th column as primary key
	// the first column is the 9th column in the predefined columns in
	// the mock function. Here we exepct the type of the primary key
	// is types.T_char or types.T_varchar
	schema := catalog2.MockSchemaEnhanced(4, primaryKeyIdx, 9)
	schema.Name = tableName

	{
		disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(
			ctx,
			testutil.TestOptions{},
			t,
			testutil.WithDisttaeEngineInsertEntryMaxCount(1),
			testutil.WithDisttaeEngineCommitWorkspaceThreshold(1),
		)
		defer func() {
			disttaeEngine.Close(ctx)
			taeEngine.Close(true)
			rpcAgent.Close()
		}()

		ctx, cancel = context.WithTimeout(ctx, time.Minute)
		defer cancel()
		_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
		require.NoError(t, err)
	}

	insertCnt := 150
	//deleteCnt := 100
	//var bat2 *batch.Batch

	{
		// insert 150 rows
		_, table, txn, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		bat := catalog2.MockBatch(schema, insertCnt)
		err = table.Write(ctx, containers.ToCNBatch(bat))
		require.NoError(t, err)

		//delete 100 rows
		//require.NoError(t, table.Delete(ctx, bat2, catalog.Row_ID))
		require.NoError(t, txn.Commit(ctx))
	}

	{
		_, _, reader, err := testutil.GetTableTxnReader(
			ctx,
			disttaeEngine,
			databaseName,
			tableName,
			nil,
			mp,
			t,
		)
		require.NoError(t, err)

		ret := testutil.EmptyBatchFromSchema(schema, primaryKeyIdx)
		_, err = reader.Read(ctx, ret.Attrs, nil, mp, ret)
		require.NoError(t, err)
		//require.Equal(t, insertCnt-deleteCnt, ret.RowCount())
	}
}

func Test_CNTransferTombstoneObjects(t *testing.T) {
	var (
		err          error
		opts         testutil.TestOptions
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(0))

	opts.TaeEngineOptions = config.WithLongScanAndCKPOpts(nil)

	p := testutil.InitEnginePack(opts, t)
	defer p.Close()

	schema := catalog2.MockSchemaEnhanced(1, 0, 2)
	schema.Name = tableName

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	cnTxnOp := p.StartCNTxn()
	_, rel := p.CreateDBAndTable(cnTxnOp, databaseName, schema)
	require.NotNil(t, rel)
	require.NoError(t, cnTxnOp.Commit(ctx))

	bat := catalog2.MockBatch(schema, 20)
	bats := bat.Split(2)

	require.Equal(t, 2, len(bats))
	require.Equal(t, 10, bats[0].Length())

	// append data
	{
		for i := 0; i < len(bats); i++ {
			tnTxnOp, err := p.T.GetDB().StartTxn(nil)
			require.NoError(t, err)

			tnDB, err := tnTxnOp.GetDatabase(databaseName)
			require.NoError(t, err)

			tnRel, err := tnDB.GetRelationByName(tableName)
			require.NoError(t, err)

			err = tnRel.Append(ctx, bats[i])
			require.NoError(t, err)

			require.NoError(t, tnTxnOp.Commit(ctx))

			testutil2.CompactBlocks(t, 0, p.T.GetDB(), databaseName, schema, true)
		}
	}

	{
		_, _, cnTxnOp, err = p.D.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		cnTxnOp.GetWorkspace().StartStatement()
		err = cnTxnOp.GetWorkspace().IncrStatementID(ctx, false)
		require.NoError(t, err)
	}

	// read row id and pk data
	tombstoneBat := batch.NewWithSize(2)
	tombstoneBat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	tombstoneBat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	{
		txn, _, reader, err := testutil.GetTableTxnReader(
			ctx, p.D, databaseName, tableName, nil, p.Mp, t,
		)
		require.NoError(t, err)

		ret := testutil.EmptyBatchFromSchema(schema)

		for {
			done, err := reader.Read(ctx,
				[]string{schema.GetPrimaryKey().Name, catalog.Row_ID}, nil, p.Mp, ret)

			if done {
				break
			}

			require.NoError(t, err)
			for i := range ret.RowCount() {
				err = vector.AppendFixed[types.Rowid](
					tombstoneBat.Vecs[0],
					vector.GetFixedAtNoTypeCheck[types.Rowid](ret.Vecs[1], i),
					false, p.Mp)
				require.NoError(t, err)

				err = vector.AppendFixed[int32](
					tombstoneBat.Vecs[1],
					vector.GetFixedAtNoTypeCheck[int32](ret.Vecs[0], i),
					false, p.Mp)
				require.NoError(t, err)
			}
		}

		require.NoError(t, txn.Commit(ctx))

		tombstoneBat.SetRowCount(tombstoneBat.Vecs[0].Length())
		require.Equal(t, bat.Length(), tombstoneBat.Vecs[0].Length())
	}

	// merge data objects
	{
		testutil2.MergeBlocks(t, 0, p.T.GetDB(), databaseName, schema, true)

		ss, err := p.D.GetPartitionStateStats(ctx, rel.GetDBID(ctx), rel.GetTableID(ctx))
		require.NoError(t, err)

		fmt.Println(ss.String())
		require.Equal(t, 1, ss.DataObjectsVisible.ObjCnt)
	}

	// mock tombstone object and put it into workspace
	{
		proc := rel.GetProcess().(*process.Process)

		w := colexec.NewCNS3TombstoneWriter(
			proc.Mp(), proc.GetFileService(), types.T_int32.ToType(), -1,
		)
		require.NoError(t, err)

		err = w.Write(proc.Ctx, tombstoneBat)
		require.NoError(t, err)

		ss, err := w.Sync(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, len(ss))

		require.Equal(t, bat.Length(), int(ss[0].Rows()))

		tbat := batch.NewWithSize(1)
		tbat.Attrs = []string{catalog.ObjectMeta_ObjectStats}
		tbat.Vecs[0] = vector.NewVec(types.T_text.ToType())
		err = vector.AppendBytes(tbat.Vecs[0], ss[0].Marshal(), false, p.Mp)
		require.NoError(t, err)

		tbat.SetRowCount(tbat.Vecs[0].Length())

		transaction := cnTxnOp.GetWorkspace().(*disttae.Transaction)
		err = transaction.WriteFile(
			disttae.DELETE,
			0, rel.GetDBID(ctx), rel.GetTableID(ctx), databaseName, tableName,
			ss[0].ObjectLocation().String(),
			tbat,
			p.D.Engine.GetTNServices()[0],
		)
		require.NoError(t, err)

		err = cnTxnOp.UpdateSnapshot(ctx, p.D.Now())
		require.NoError(t, err)
	}

	// check result
	{
		expected := 20
		for i := 0; i < 2; i++ {
			if i == 1 {
				expected = 0
				ctx = context.WithValue(ctx, disttae.UT_ForceTransCheck{}, "yes")
				require.NoError(t, cnTxnOp.Commit(ctx))
			}

			txnop := p.StartCNTxn()
			v, ok := runtime.ServiceRuntime("").GetGlobalVariables(runtime.InternalSQLExecutor)
			require.True(t, ok)

			exec := v.(executor.SQLExecutor)

			res, err := exec.Exec(p.Ctx,
				fmt.Sprintf("select count(*) from `%s`.`%s`;",
					databaseName, tableName), executor.Options{}.WithTxn(txnop))
			require.NoError(t, err)
			require.NoError(t, txnop.Commit(ctx))

			res.ReadRows(func(rows int, cols []*vector.Vector) bool {
				require.Equal(t, 1, rows)
				require.Equal(t, 1, len(cols))

				result := vector.MustFixedColNoTypeCheck[int64](cols[0])
				require.Equal(t, expected, int(result[0]))
				return true
			})

			res.Close()
		}
	}
}

func TestGCFiles(t *testing.T) {
	var (
		err error
	)

	p := testutil.InitEnginePack(testutil.TestOptions{}, t)
	defer p.Close()

	var (
		pool  *ants.Pool
		files = make([]objectio.ObjectStats, 3)
	)

	objectio.SetObjectStatsObjectName(&files[0], objectio.ObjectName{0x1})
	objectio.SetObjectStatsObjectName(&files[0], objectio.ObjectName{0x2})
	objectio.SetObjectStatsObjectName(&files[0], objectio.ObjectName{0x3})

	op := p.StartCNTxn()
	txn := op.GetWorkspace().(*disttae.Transaction)

	txn.GetProc()

	for j := 0; j < 2; j++ {
		if j == 1 {
			pool, err = ants.NewPool(0, ants.WithNonblocking(true))
			require.NoError(t, err)
			p.D.Engine.ResetGCWorkerPool(pool)
			pool.Release()
		}

		err = txn.GCObjsByStats(files...)
		if j == 1 {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}

		for i := range files {
			objectio.SetObjectStatsBlkCnt(&files[i], 1)
			objectio.SetObjectStatsRowCnt(&files[i], 1)
			bat := colexec.AllocCNS3ResultBat(false, false)
			colexec.ExpandObjectStatsToBatch(p.Mp, false, bat, true, files[i])
			err = txn.WriteFile(
				disttae.INSERT,
				0, 0, 0,
				"", "", "0x1",
				bat, disttae.DNStore{},
			)

			require.NoError(t, err)
			bat.Clean(p.Mp)
		}

		err = txn.GCObjsByIdxRange(0, len(files)-1)
		if j == 1 {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
	}
}

// Test_WorkspaceForceDumpOnGlobalAccumulation verifies that the workspace
// safety valve forces a dump when global in-memory insert size accumulates
// beyond extraWorkspaceThreshold, even when IncrStatementID is disabled
// (simulating HNSW index creation via RunSql).
//
// Without the fix, writes keep accumulating because:
// 1. IncrStatementID is disabled (no cumulative dump from offset 0)
// 2. Each write's offset-scanned size < raised writeWorkspaceThreshold
// 3. Only quota check on first write, threshold raised permanently
//
// With the fix, a global safety valve triggers dump when
// approximateInMemInsertSize >= extraWorkspaceThreshold.
func Test_WorkspaceForceDumpOnGlobalAccumulation(t *testing.T) {
	var (
		err          error
		mp           *mpool.MPool
		txn          client.TxnOperator
		accountId    = catalog.System_Account
		tableName    = "test_table"
		databaseName = "test_database"

		primaryKeyIdx = 3

		relation engine.Relation
		_        engine.Database

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	schema := catalog2.MockSchemaAll(4, primaryKeyIdx)
	schema.Name = tableName

	// Use small thresholds to trigger the safety valve with small batches:
	// - writeWorkspaceThreshold = 100 bytes: writes > 100 bytes trigger quota check
	// - extraWorkspaceThreshold = 3000 bytes: global accumulation > 3000 bytes triggers force dump
	// - quota = 100MB: enough for quota acquisition
	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(
		ctx,
		testutil.TestOptions{},
		t,
		testutil.WithDisttaeEngineWriteWorkspaceThreshold(100),
		testutil.WithDisttaeEngineExtraWorkspaceThreshold(3000),
		testutil.WithDisttaeEngineCommitWorkspaceThreshold(1),
		testutil.WithDisttaeEngineQuota(100*1024*1024),
	)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	rowsCount := 10
	totalWrites := 10

	_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
	require.NoError(t, err)

	ws := txn.GetWorkspace().(*disttae.Transaction)

	// Start the outer statement boundary for the SQL that triggers HNSW creation.
	// The loop below simulates DisableIncrStatement behavior for sub-writes by
	// reusing this statement context instead of incrementing the statement ID again.
	ws.StartStatement()
	require.NoError(t, ws.IncrStatementID(ctx, false))

	// Simulate DisableIncrStatement for the repeated writes: write multiple batches
	// without any additional IncrStatementID or StartStatement/EndStatement calls,
	// but still call UpdateSnapshotWriteOffset between writes (as NewCompile does).
	var maxInMemSize uint64
	forceDumpTriggered := false

	for i := 0; i < totalWrites; i++ {
		func() {
			bat := catalog2.MockBatch(schema, rowsCount)
			defer bat.Close()
			cnBat := containers.ToCNBatch(bat)

			// Simulate NewCompile → UpdateSnapshotWriteOffset
			ws.UpdateSnapshotWriteOffset()

			err = relation.Write(ctx, cnBat)
			require.NoError(t, err)

			inMemSize := ws.ApproximateInMemInsertSize()
			if inMemSize > maxInMemSize {
				maxInMemSize = inMemSize
			}

			// If inMemSize dropped, a dump was triggered
			if i > 0 && inMemSize < maxInMemSize {
				forceDumpTriggered = true
			}
		}()
	}

	// Verify that the safety valve triggered at least once
	require.True(t, forceDumpTriggered,
		"expected force dump to trigger when global accumulation exceeds extraWorkspaceThreshold, "+
			"but approximateInMemInsertSize only grew (max=%d)", maxInMemSize)

	// Verify that the final in-memory size is bounded
	finalInMemSize := ws.ApproximateInMemInsertSize()
	require.Less(t, finalInMemSize, uint64(3000)*2,
		"expected final in-memory size to be bounded by ~2x extraWorkspaceThreshold, got %d", finalInMemSize)

	// End the statement and commit
	ws.EndStatement()

	// Commit and verify data integrity
	require.NoError(t, txn.Commit(ctx))

	// Read back all rows to verify data wasn't lost during force dump
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		reader, err := testutil.GetRelationReader(
			ctx,
			disttaeEngine,
			txn,
			relation,
			nil,
			mp,
			t,
		)
		require.NoError(t, err)

		ret := testutil.EmptyBatchFromSchema(schema, primaryKeyIdx)
		totalRows := 0
		for {
			done, err := reader.Read(ctx, ret.Attrs, nil, mp, ret)
			if done {
				break
			}
			require.NoError(t, err)
			totalRows += ret.RowCount()
		}
		reader.Close()

		// We wrote totalWrites * rowsCount rows total
		require.Equal(t, totalWrites*rowsCount, totalRows,
			"expected %d rows but got %d after force dump + commit", totalWrites*rowsCount, totalRows)

		require.NoError(t, txn.Commit(ctx))
	}
}

// Test_WorkspaceForceDumpNoIncrStatement simulates the real HNSW scenario where
// IncrStatementID is completely disabled (statementID==0). This verifies the safety
// valve works when stmtStart falls back to 0, which is the actual code path hit
// during HNSW index creation via sqlexec.RunSql with WithDisableIncrStatement.
func Test_WorkspaceForceDumpNoIncrStatement(t *testing.T) {
	var (
		err          error
		mp           *mpool.MPool
		txn          client.TxnOperator
		accountId    = catalog.System_Account
		tableName    = "test_table"
		databaseName = "test_database"

		primaryKeyIdx = 3

		relation engine.Relation
		_        engine.Database

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	schema := catalog2.MockSchemaAll(4, primaryKeyIdx)
	schema.Name = tableName

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(
		ctx,
		testutil.TestOptions{},
		t,
		testutil.WithDisttaeEngineWriteWorkspaceThreshold(100),
		testutil.WithDisttaeEngineExtraWorkspaceThreshold(3000),
		testutil.WithDisttaeEngineCommitWorkspaceThreshold(1),
		testutil.WithDisttaeEngineQuota(100*1024*1024),
	)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	rowsCount := 10
	totalWrites := 10

	_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
	require.NoError(t, err)

	ws := txn.GetWorkspace().(*disttae.Transaction)

	// Only call StartStatement — NO IncrStatementID. This is the real HNSW path:
	// sqlexec.RunSql uses WithDisableIncrStatement, so IncrStatementID is never called,
	// leaving statementID==0 and offsets[] empty.
	ws.StartStatement()

	var maxInMemSize uint64
	forceDumpTriggered := false

	for i := 0; i < totalWrites; i++ {
		func() {
			bat := catalog2.MockBatch(schema, rowsCount)
			defer bat.Close()
			cnBat := containers.ToCNBatch(bat)

			ws.UpdateSnapshotWriteOffset()

			err = relation.Write(ctx, cnBat)
			require.NoError(t, err)

			inMemSize := ws.ApproximateInMemInsertSize()
			if inMemSize > maxInMemSize {
				maxInMemSize = inMemSize
			}

			if i > 0 && inMemSize < maxInMemSize {
				forceDumpTriggered = true
			}
		}()
	}

	require.True(t, forceDumpTriggered,
		"expected force dump to trigger with statementID==0, "+
			"but approximateInMemInsertSize only grew (max=%d)", maxInMemSize)

	finalInMemSize := ws.ApproximateInMemInsertSize()
	require.Less(t, finalInMemSize, uint64(3000)*2,
		"expected final in-memory size to be bounded, got %d", finalInMemSize)

	ws.EndStatement()

	require.NoError(t, txn.Commit(ctx))

	// Read back and verify data integrity
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		reader, err := testutil.GetRelationReader(
			ctx,
			disttaeEngine,
			txn,
			relation,
			nil,
			mp,
			t,
		)
		require.NoError(t, err)

		ret := testutil.EmptyBatchFromSchema(schema, primaryKeyIdx)
		totalRows := 0
		for {
			done, err := reader.Read(ctx, ret.Attrs, nil, mp, ret)
			if done {
				break
			}
			require.NoError(t, err)
			totalRows += ret.RowCount()
		}
		reader.Close()

		require.Equal(t, totalWrites*rowsCount, totalRows,
			"expected %d rows but got %d", totalWrites*rowsCount, totalRows)

		require.NoError(t, txn.Commit(ctx))
	}
}
