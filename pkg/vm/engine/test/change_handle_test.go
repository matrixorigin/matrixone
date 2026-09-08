// Copyright 2024 Matrix Origin
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
	"encoding/base64"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	cmd_util "github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	testutil2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
)

func TestGetErrorMsg(t *testing.T) {
	catalog.SetupDefines("")

	// idAllocator := common.NewIdAllocator(1000)

	var (
		accountId = catalog.System_Account
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	err := mock_mo_indexes(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = exec_sql(disttaeEngine, ctxWithTimeout, frontend.MoCatalogMoCdcWatermarkDDL)
	require.NoError(t, err)
	taskID := uuid.New().String()

	ie := &mockCDCIE{de: disttaeEngine}
	hasError, err := frontend.GetTableErrMsg(ctxWithTimeout, accountId, ie, taskID, &cdc.DbTableInfo{
		SourceDbName:  "test_db",
		SourceTblName: "test_table",
	})
	require.False(t, hasError)
	require.NoError(t, err)
	insert_sql := cdc.CDCSQLBuilder.InsertWatermarkSQL(uint64(accountId), taskID, "test_db", "test_table", "1000")
	err = exec_sql(disttaeEngine, ctxWithTimeout, insert_sql)
	require.NoError(t, err)

	ie = &mockCDCIE{de: disttaeEngine}
	hasError, err = frontend.GetTableErrMsg(ctxWithTimeout, accountId, ie, taskID, &cdc.DbTableInfo{
		SourceDbName:  "test_db",
		SourceTblName: "test_table",
	})
	require.False(t, hasError)
	require.NoError(t, err)

	values := fmt.Sprintf("(%d, '%s', '%s', '%s', '%s')", accountId, taskID, "test_db", "test_table", "error_msg")
	err = exec_sql(disttaeEngine, ctxWithTimeout, cdc.CDCSQLBuilder.OnDuplicateUpdateWatermarkErrMsgSQL(values))
	require.NoError(t, err)

	hasError, err = frontend.GetTableErrMsg(ctxWithTimeout, accountId, ie, taskID, &cdc.DbTableInfo{
		SourceDbName:  "test_db",
		SourceTblName: "test_table",
	})
	require.True(t, hasError)
	require.NoError(t, err)

	ie.setError(moerr.NewInternalErrorNoCtx("debug"))

	hasError, err = frontend.GetTableErrMsg(ctxWithTimeout, accountId, ie, taskID, &cdc.DbTableInfo{
		SourceDbName:  "test_db",
		SourceTblName: "test_table",
	})
	require.False(t, hasError)
	require.Error(t, err)

	ie.setError(nil)
	ie.setStringError(moerr.NewInternalErrorNoCtx("debug"))

	hasError, err = frontend.GetTableErrMsg(ctxWithTimeout, accountId, ie, taskID, &cdc.DbTableInfo{
		SourceDbName:  "test_db",
		SourceTblName: "test_table",
	})
	require.False(t, hasError)
	require.Error(t, err)

	ie.setStringError(nil)

	values = fmt.Sprintf("(%d, '%s', '%s', '%s', '%s')", accountId, taskID, "test_db", "test_table", cdc.RetryableErrorPrefix+"debug")
	err = exec_sql(disttaeEngine, ctxWithTimeout, cdc.CDCSQLBuilder.OnDuplicateUpdateWatermarkErrMsgSQL(values))
	require.NoError(t, err)

	hasError, err = frontend.GetTableErrMsg(ctxWithTimeout, accountId, ie, taskID, &cdc.DbTableInfo{
		SourceDbName:  "test_db",
		SourceTblName: "test_table",
	})
	require.False(t, hasError)
	require.NoError(t, err)
}

func TestFlushErrorMsg(t *testing.T) {
	catalog.SetupDefines("")

	// idAllocator := common.NewIdAllocator(1000)

	var (
		accountId = catalog.System_Account
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	err := mock_mo_indexes(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_foreign_keys(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_intra_system_change_propagation_log(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)

	spec := iscp.JobSpec{}
	specStr, err := iscp.MarshalJobSpec(&spec)
	require.NoError(t, err)
	status := iscp.JobStatus{}
	statusStr, err := iscp.MarshalJobStatus(&status)
	require.NoError(t, err)
	sql := cdc.CDCSQLBuilder.ISCPLogInsertSQL(uint32(accountId), 1, "test", 1, specStr, 2, types.TS{}, statusStr)
	err = exec_sql(disttaeEngine, ctxWithTimeout, sql)
	require.NoError(t, err)
	err = iscp.FlushPermanentErrorMessage(
		ctx,
		"",
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		uint32(accountId),
		1,
		[]string{"test"},
		[]uint64{1},
		[]uint64{1},
		[]*iscp.JobStatus{{}},
		types.MaxTs(),
		"test",
		[]uint64{0},
	)
	require.NoError(t, err)
}

func TestChangesHandle1(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()
	startTS := taeHandler.GetDB().TxnMgr.Now()
	schema := catalog2.MockSchemaAll(20, 0)
	schema.Name = tableName
	bat := catalog2.MockBatch(schema, 10)

	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)
	txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	require.Nil(t, rel.Append(ctx, bat))
	require.Nil(t, txn.Commit(ctx))

	txn, rel = testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	id := rel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	obj := testutil2.GetOneBlockMeta(rel)
	err = rel.RangeDelete(obj.AsCommonID(), 0, 0, handle.DT_Normal)
	require.Nil(t, err)
	require.Nil(t, txn.Commit(ctx))

	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, databaseName, tableName, false)
	require.Nil(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	mp := common.DebugAllocator

	// check partition state, before flush
	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		handle, err := rel.CollectChanges(ctx, types.TS{}, taeHandler.GetDB().TxnMgr.Now(), true, mp)
		assert.NoError(t, err)
		totalRows := 0
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			assert.Equal(t, hint, engine.ChangesHandle_Snapshot)
			assert.Nil(t, tombstone)
			t.Log(data.Attrs)
			checkInsertBatch(bat, data, t)
			assert.NotEqual(t, data.Vecs[0].Length(), 0)
			totalRows += data.Vecs[0].Length()
		}
		assert.Equal(t, totalRows, 9)
		assert.NoError(t, handle.Close())

		handle, err = rel.CollectChanges(ctx, startTS, taeHandler.GetDB().TxnMgr.Now(), true, mp)
		assert.NoError(t, err)
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			assert.Equal(t, hint, engine.ChangesHandle_Tail_done)
			assert.Nil(t, tombstone)
			t.Log(data.Attrs)
			checkInsertBatch(bat, data, t)
			assert.Equal(t, data.Vecs[0].Length(), 9)
			data.Clean(mp)
		}
		assert.NoError(t, handle.Close())
	}
}

func TestChangesHandle2(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()
	startTS := taeHandler.GetDB().TxnMgr.Now()
	schema := catalog2.MockSchemaAll(10, 0)
	schema.Name = tableName
	bat := catalog2.MockBatch(schema, 10)
	mp := common.DebugAllocator

	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)
	txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	require.Nil(t, rel.Append(ctx, bat))
	require.Nil(t, txn.Commit(ctx))

	txn, rel = testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	id := rel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	obj := testutil2.GetOneBlockMeta(rel)
	err = rel.RangeDelete(obj.AsCommonID(), 0, 0, handle.DT_Normal)
	require.Nil(t, err)
	require.Nil(t, txn.Commit(ctx))

	testutil2.CompactBlocks(t, accountId, taeHandler.GetDB(), databaseName, schema, true)

	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, databaseName, tableName, false)
	require.Nil(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))

	// check partition state, before flush
	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		handle, err := rel.CollectChanges(ctx, types.TS{}, taeHandler.GetDB().TxnMgr.Now(), true, mp)
		assert.NoError(t, err)
		totalRows := 0
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			assert.Equal(t, hint, engine.ChangesHandle_Snapshot)
			assert.Nil(t, tombstone)
			t.Log(data.Attrs)
			checkInsertBatch(bat, data, t)
			totalRows += data.Vecs[0].Length()
			assert.NotEqual(t, data.Vecs[0].Length(), 0)
			data.Clean(mp)
		}
		assert.Equal(t, totalRows, 9)
		assert.NoError(t, handle.Close())

		handle, err = rel.CollectChanges(ctx, startTS, taeHandler.GetDB().TxnMgr.Now(), true, mp)
		assert.NoError(t, err)
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			assert.Equal(t, hint, engine.ChangesHandle_Tail_done)
			assert.Nil(t, tombstone)
			checkInsertBatch(bat, data, t)
			assert.Equal(t, data.Vecs[0].Length(), 9)
			data.Clean(mp)
		}
		assert.NoError(t, handle.Close())
	}
}

func checkTombstoneBatch(bat *batch.Batch, pkType types.Type, t *testing.T) {
	if bat == nil {
		return
	}
	assert.Equal(t, len(bat.Vecs), 2)
	assert.Equal(t, bat.Vecs[0].GetType().Oid, pkType.Oid)
	assert.Equal(t, bat.Vecs[1].GetType().Oid, types.T_TS)
	assert.Equal(t, bat.Vecs[0].Length(), bat.Vecs[1].Length())
}

func checkInsertBatch(userBatch *containers.Batch, bat *batch.Batch, t *testing.T) {
	if bat == nil {
		return
	}
	length := bat.RowCount()
	require.GreaterOrEqual(t, len(bat.Vecs), len(userBatch.Vecs)+1)
	for i, vec := range userBatch.Vecs {
		assert.Equal(t, bat.Vecs[i].GetType().Oid, vec.GetType().Oid)
		assert.Equal(t, bat.Vecs[i].Length(), length)
	}
	commitPos := -1
	for pos, attr := range bat.Attrs {
		if attr == objectio.DefaultCommitTS_Attr {
			commitPos = pos
			break
		}
	}
	if commitPos == -1 {
		for pos := len(bat.Vecs) - 1; pos >= len(userBatch.Vecs); pos-- {
			if bat.Vecs[pos].GetType().Oid == types.T_TS {
				commitPos = pos
				break
			}
		}
	}
	require.NotEqual(t, -1, commitPos)
	require.Less(t, commitPos, len(bat.Vecs))
	assert.Equal(t, types.T_TS, bat.Vecs[commitPos].GetType().Oid)
	assert.Equal(t, length, bat.Vecs[commitPos].Length())
}

func changesHandleTestRowCount() int {
	if testing.Short() {
		// Four blocks still cover compaction plus snapshot/tail iteration while
		// keeping the race-enabled PR test bounded.
		return objectio.BlockMaxRows * 4
	}
	return objectio.BlockMaxRows * 20
}

func TestChangesHandle3(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	opts := config.WithLongScanAndCKPOpts(nil)
	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{TaeEngineOptions: opts}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()
	startTS := taeHandler.GetDB().TxnMgr.Now()
	schema := catalog2.MockSchemaAll(23, 9)
	schema.Name = tableName
	rowCount := changesHandleTestRowCount()
	bat := catalog2.MockBatch(schema, rowCount)
	mp := common.DebugAllocator

	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)
	txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	require.Nil(t, rel.Append(ctx, bat))
	require.Nil(t, txn.Commit(ctx))

	txn, rel = testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	id := rel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	iter := rel.MakeObjectIt(false)
	deletedRows := 0
	for iter.Next() {
		obj := iter.GetObject()
		err = rel.RangeDelete(obj.Fingerprint(), 0, 0, handle.DT_Normal)
		deletedRows++
	}
	require.Nil(t, err)
	require.Nil(t, txn.Commit(ctx))

	testutil2.CompactBlocks(t, accountId, taeHandler.GetDB(), databaseName, schema, true)

	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, databaseName, tableName, false)
	require.Nil(t, err)

	// check partition state, before flush
	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		handle, err := rel.CollectChanges(ctx, types.TS{}, taeHandler.GetDB().TxnMgr.Now(), true, mp)
		assert.NoError(t, err)
		totalRows := 0
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			assert.Equal(t, hint, engine.ChangesHandle_Snapshot)
			assert.Nil(t, tombstone)
			t.Log(data.Attrs)
			checkInsertBatch(bat, data, t)
			totalRows += data.Vecs[0].Length()
			data.Clean(mp)
		}
		assert.Equal(t, rowCount-deletedRows, totalRows)
		assert.NoError(t, handle.Close())

		handle, err = rel.CollectChanges(ctx, startTS, taeHandler.GetDB().TxnMgr.Now(), true, mp)
		assert.NoError(t, err)
		totalRows = 0
		totalTombstones := 0
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			if tombstone != nil {
				assert.Equal(t, hint, engine.ChangesHandle_Tail_done)
				checkTombstoneBatch(tombstone, schema.GetPrimaryKey().Type, t)
				totalTombstones += tombstone.Vecs[0].Length()
				tombstone.Clean(mp)
			}
			if data != nil {
				checkInsertBatch(bat, data, t)
				totalRows += data.Vecs[0].Length()
				data.Clean(mp)
			}
		}
		assert.Equal(t, deletedRows, totalTombstones)
		assert.Equal(t, rowCount, totalRows)
		assert.NoError(t, handle.Close())
	}
}
func TestChangesHandleForCNWrite(t *testing.T) {
	var (
		err          error
		txn          client.TxnOperator
		mp           *mpool.MPool
		accountId    = catalog.System_Account
		tableName    = "test_reader_table"
		databaseName = "test_reader_database"

		primaryKeyIdx int = 3

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

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()
	startTS := taeEngine.GetDB().TxnMgr.Now()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	blockCnt := 10
	rowsCount := objectio.BlockMaxRows * blockCnt
	bat := catalog2.MockBatch(schema, rowsCount)
	bats := bat.Split(blockCnt)

	// write table
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		for idx := 0; idx < blockCnt; idx++ {
			require.NoError(t, relation.Write(ctx, containers.ToCNBatch(bats[idx])))
		}

		require.NoError(t, txn.Commit(ctx))
	}
	dnTxn, dnRel := testutil2.GetRelation(t, accountId, taeEngine.GetDB(), databaseName, tableName)
	id := dnRel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	t.Log(taeEngine.GetDB().Catalog.SimplePPString(3))
	assert.NoError(t, dnTxn.Commit(ctx))
	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, databaseName, tableName, false)
	require.Nil(t, err)

	// check partition state, before flush
	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		handle, err := rel.CollectChanges(ctx, types.TS{}, taeEngine.GetDB().TxnMgr.Now(), true, mp)
		assert.NoError(t, err)
		totalRows := 0
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			assert.Equal(t, hint, engine.ChangesHandle_Snapshot)
			assert.Nil(t, tombstone)
			t.Log(data.Attrs)
			checkInsertBatch(bat, data, t)
			totalRows += data.Vecs[0].Length()
			data.Clean(mp)
		}
		assert.Equal(t, totalRows, bat.Length())
		assert.NoError(t, handle.Close())

		handle, err = rel.CollectChanges(ctx, startTS, taeEngine.GetDB().TxnMgr.Now(), true, mp)
		assert.NoError(t, err)
		batchCount := 0
		for {
			data, tombstone, _, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			batchCount++
			assert.NoError(t, err)
			assert.Nil(t, tombstone)
			checkInsertBatch(bat, data, t)
			assert.Equal(t, data.Vecs[0].Length(), 16384)
			data.Clean(mp)
		}
		assert.Equal(t, batchCount, 5)
		assert.NoError(t, handle.Close())
	}
}

func TestChangesHandle4(t *testing.T) {
	var (
		err          error
		txn          client.TxnOperator
		mp           *mpool.MPool
		accountId    = catalog.System_Account
		tableName    = "test_reader_table"
		databaseName = "test_reader_database"

		primaryKeyIdx int = 3

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

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()
	startTS := taeEngine.GetDB().TxnMgr.Now()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	blockCnt := 10
	batchCount := blockCnt * 2
	rowsCount := objectio.BlockMaxRows * blockCnt
	bat := catalog2.MockBatch(schema, rowsCount)
	bats := bat.Split(batchCount)

	dntxn, dnrel := testutil2.GetRelation(t, accountId, taeEngine.GetDB(), databaseName, tableName)
	assert.NoError(t, dnrel.Append(ctx, bats[0]))
	assert.NoError(t, dntxn.Commit(ctx))

	testutil2.CompactBlocks(t, accountId, taeEngine.GetDB(), databaseName, schema, true)

	dntxn, dnrel = testutil2.GetRelation(t, accountId, taeEngine.GetDB(), databaseName, tableName)
	assert.NoError(t, dnrel.Append(ctx, bats[1]))
	assert.NoError(t, dntxn.Commit(ctx))
	// write table
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		for idx := 3; idx < batchCount; idx++ {
			require.NoError(t, relation.Write(ctx, containers.ToCNBatch(bats[idx])))
		}

		require.NoError(t, txn.Commit(ctx))
	}
	dnTxn, dnRel := testutil2.GetRelation(t, accountId, taeEngine.GetDB(), databaseName, tableName)
	id := dnRel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	t.Log(taeEngine.GetDB().Catalog.SimplePPString(3))
	assert.NoError(t, dnTxn.Commit(ctx))
	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, databaseName, tableName, false)
	require.Nil(t, err)

	dntxn, dnrel = testutil2.GetRelation(t, accountId, taeEngine.GetDB(), databaseName, tableName)
	assert.NoError(t, dnrel.Append(ctx, bats[2]))
	assert.NoError(t, dntxn.Commit(ctx))

	// check partition state, before flush
	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		c := &checkHelper{}
		handle, err := rel.CollectChanges(ctx, startTS, taeEngine.GetDB().TxnMgr.Now(), true, mp)
		assert.NoError(t, err)
		for {
			data, tombstone, _, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			c.check(data, tombstone, t)
			if tombstone != nil {
				tombstone.Clean(mp)
			}
			if data != nil {
				data.Clean(mp)
			}
		}
		c.checkRows(rowsCount, 0, t)
		assert.NoError(t, handle.Close())
	}

}

type checkHelper struct {
	prevDataTS, prevTombstoneTS       types.TS
	totalDataRows, totalTombstoneRows int
}

func (c *checkHelper) check(data, tombstone *batch.Batch, t *testing.T) {
	if data != nil {
		maxTS := types.TS{}
		commitTSVec := data.Vecs[len(data.Vecs)-1]
		commitTSs := vector.MustFixedColNoTypeCheck[types.TS](commitTSVec)
		for _, ts := range commitTSs {
			assert.True(t, ts.GE(&c.prevTombstoneTS))
			if ts.GT(&maxTS) {
				maxTS = ts
			}
		}
		c.prevDataTS = maxTS
		c.totalDataRows += commitTSVec.Length()
	}
	if tombstone != nil {
		maxTS := types.TS{}
		commitTSVec := data.Vecs[len(tombstone.Vecs)-1]
		commitTSs := vector.MustFixedColNoTypeCheck[types.TS](commitTSVec)
		for _, ts := range commitTSs {
			assert.True(t, ts.GT(&c.prevDataTS))
			if ts.GT(&maxTS) {
				maxTS = ts
			}
		}
		c.prevTombstoneTS = maxTS
		c.totalTombstoneRows += commitTSVec.Length()
	}
}

func (c *checkHelper) checkRows(data, tombstone int, t *testing.T) {
	assert.Equal(t, data, c.totalDataRows)
	assert.Equal(t, tombstone, c.totalTombstoneRows)
}

func TestChangesHandle5(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	opts := config.WithLongScanAndCKPOpts(nil)
	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{TaeEngineOptions: opts}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()
	startTS := taeHandler.GetDB().TxnMgr.Now()
	schema := catalog2.MockSchemaAll(20, 0)
	schema.Name = tableName
	bat := catalog2.MockBatch(schema, 10)

	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)
	txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	id := rel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	require.Nil(t, rel.Append(ctx, bat))
	require.Nil(t, txn.Commit(ctx))

	flushTxn, flushRel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	obj := testutil2.GetOneBlockMeta(flushRel)
	{
		txn, rel = testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
		obj := testutil2.GetOneBlockMeta(rel)
		err = rel.RangeDelete(obj.AsCommonID(), 0, 0, handle.DT_Normal)
		require.Nil(t, err)
		require.Nil(t, txn.Commit(ctx))
	}
	task, err := jobs.NewFlushTableTailTask(nil, flushTxn, []*catalog2.ObjectEntry{obj}, nil, taeHandler.GetDB().Runtime)
	assert.NoError(t, err)
	err = task.OnExec(context.Background())
	assert.NoError(t, err)
	assert.NoError(t, flushTxn.Commit(context.Background()))

	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, databaseName, tableName, false)
	require.Nil(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	mp := common.DebugAllocator

	// check partition state, before flush
	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		handle, err := rel.CollectChanges(ctx, types.TS{}, taeHandler.GetDB().TxnMgr.Now(), true, mp)
		assert.NoError(t, err)
		totalRows := 0
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			assert.Equal(t, hint, engine.ChangesHandle_Snapshot)
			assert.Nil(t, tombstone)
			t.Log(data.Attrs)
			checkInsertBatch(bat, data, t)
			assert.NotEqual(t, data.Vecs[0].Length(), 0)
			totalRows += data.Vecs[0].Length()
		}
		assert.Equal(t, totalRows, 9)
		assert.NoError(t, handle.Close())

		handle, err = rel.CollectChanges(ctx, startTS, taeHandler.GetDB().TxnMgr.Now(), true, mp)
		assert.NoError(t, err)
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			assert.Equal(t, hint, engine.ChangesHandle_Tail_done)
			assert.Nil(t, tombstone)
			t.Log(data.Attrs)
			checkInsertBatch(bat, data, t)
			assert.Equal(t, data.Vecs[0].Length(), 9)
			data.Clean(mp)
		}
		assert.NoError(t, handle.Close())
	}
}

func TestChangesHandle6(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	opts := config.WithLongScanAndCKPOpts(nil)
	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{TaeEngineOptions: opts}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()
	startTS := taeHandler.GetDB().TxnMgr.Now()
	schema := catalog2.MockSchemaAll(20, 0)
	schema.Name = tableName
	bat := catalog2.MockBatch(schema, 10)

	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)
	txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	id := rel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	require.Nil(t, rel.Append(ctx, bat))
	require.Nil(t, txn.Commit(ctx))

	flushTxn, flushRel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	obj := testutil2.GetOneBlockMeta(flushRel)
	{
		txn, rel = testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
		obj := testutil2.GetOneBlockMeta(rel)
		err = rel.RangeDelete(obj.AsCommonID(), 0, 0, handle.DT_Normal)
		require.Nil(t, err)
		require.Nil(t, txn.Commit(ctx))
	}
	task, err := jobs.NewFlushTableTailTask(nil, flushTxn, []*catalog2.ObjectEntry{obj}, nil, taeHandler.GetDB().Runtime)
	assert.NoError(t, err)
	err = task.OnExec(context.Background())
	assert.NoError(t, err)
	assert.NoError(t, flushTxn.Commit(context.Background()))

	testutil2.CompactBlocks(t, accountId, taeHandler.GetDB(), databaseName, schema, true)

	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, databaseName, tableName, false)
	require.Nil(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	mp := common.DebugAllocator

	// check partition state, before flush
	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		handle, err := rel.CollectChanges(ctx, types.TS{}, taeHandler.GetDB().TxnMgr.Now(), true, mp)
		assert.NoError(t, err)
		totalRows := 0
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			assert.Equal(t, hint, engine.ChangesHandle_Snapshot)
			assert.Nil(t, tombstone)
			t.Log(data.Attrs)
			checkInsertBatch(bat, data, t)
			assert.NotEqual(t, data.Vecs[0].Length(), 0)
			totalRows += data.Vecs[0].Length()
		}
		assert.Equal(t, totalRows, 9)
		assert.NoError(t, handle.Close())

		handle, err = rel.CollectChanges(ctx, startTS, taeHandler.GetDB().TxnMgr.Now(), true, mp)
		assert.NoError(t, err)
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			assert.Equal(t, hint, engine.ChangesHandle_Tail_done)
			assert.Nil(t, tombstone)
			t.Log(data.Attrs)
			checkInsertBatch(bat, data, t)
			assert.Equal(t, data.Vecs[0].Length(), 9)
			data.Clean(mp)
		}
		assert.NoError(t, handle.Close())
	}
}

func TestChangesHandleStaleFiles1(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	opts := config.WithLongScanAndCKPOpts(nil)
	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{TaeEngineOptions: opts}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()
	startTS := taeHandler.GetDB().TxnMgr.Now()
	schema := catalog2.MockSchemaAll(20, 0)
	schema.Name = tableName
	bat := catalog2.MockBatch(schema, 10)

	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)
	txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	id := rel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	require.Nil(t, rel.Append(ctx, bat))
	require.Nil(t, txn.Commit(ctx))

	txn, rel = testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, schema.Name)
	obj := testutil2.GetOneBlockMeta(rel)
	txn.Commit(ctx)
	assert.NoError(t, err)

	testutil2.CompactBlocks(t, accountId, taeHandler.GetDB(), databaseName, schema, true)

	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, databaseName, tableName, false)
	require.Nil(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	mp := common.DebugAllocator

	fs := taeHandler.GetDB().Runtime.Fs
	deleteFileName := obj.ObjectStats.ObjectName().String()
	err = fs.Delete(ctx, []string{string(deleteFileName)}...)
	assert.NoError(t, err)
	gcTS := taeHandler.GetDB().TxnMgr.Now()
	gcTSFileName := ioutil.EncodeCompactCKPMetadataFullName(
		types.TS{}, gcTS,
	)
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterCheckpoint, gcTSFileName, fs)
	assert.NoError(t, err)
	_, err = writer.Write(containers.ToCNBatch(bat))
	assert.NoError(t, err)
	_, err = writer.WriteEnd(ctx)
	assert.NoError(t, err)
	// This test targets stale checkpoint coverage after the partition file is
	// gone, not temporary TN checkpoint lag. A successful empty response keeps
	// that contract isolated and avoids entering the shared bounded retry path.
	ssStub := gostub.Stub(
		&disttae.RequestSnapshotRead,
		disttae.GetSnapshotReadFnWithHandler(
			func(
				_ context.Context,
				_ pbtxn.TxnMeta,
				_ *cmd_util.SnapshotReadReq,
				resp *cmd_util.SnapshotReadResp,
			) (func(), error) {
				resp.Succeed = true
				return nil, nil
			},
		),
	)
	defer ssStub.Reset()

	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		_, err = rel.CollectChanges(ctx, startTS, taeHandler.GetDB().TxnMgr.Now(), true, mp)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrStaleRead))

	}
}
func TestChangesHandleStaleFiles2(t *testing.T) {
	var (
		err          error
		txn          client.TxnOperator
		mp           *mpool.MPool
		accountId    = catalog.System_Account
		tableName    = "test_reader_table"
		databaseName = "test_reader_database"

		primaryKeyIdx int = 3

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

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()
	startTS := taeEngine.GetDB().TxnMgr.Now()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	blockCnt := 10
	rowsCount := objectio.BlockMaxRows * blockCnt
	bat := catalog2.MockBatch(schema, rowsCount)
	bats := bat.Split(blockCnt)

	// write table
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		for idx := 0; idx < blockCnt; idx++ {
			require.NoError(t, relation.Write(ctx, containers.ToCNBatch(bats[idx])))
		}

		require.NoError(t, txn.Commit(ctx))
	}
	dnTxn, dnRel := testutil2.GetRelation(t, accountId, taeEngine.GetDB(), databaseName, tableName)
	id := dnRel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	t.Log(taeEngine.GetDB().Catalog.SimplePPString(3))
	assert.NoError(t, dnTxn.Commit(ctx))
	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, databaseName, tableName, false)
	require.Nil(t, err)

	// check partition state, before flush
	{

		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)
		handle, err := rel.CollectChanges(ctx, startTS, taeEngine.GetDB().TxnMgr.Now(), true, mp)
		assert.NoError(t, err)
		{
			txn, dnRel := testutil2.GetRelation(t, accountId, taeEngine.GetDB(), databaseName, tableName)
			iter := dnRel.MakeObjectItOnSnap(false)
			objs := make([]*catalog2.ObjectEntry, 0)
			for iter.Next() {
				obj := iter.GetObject().GetMeta().(*catalog2.ObjectEntry)
				if obj.ObjectStats.GetCNCreated() {
					objs = append(objs, obj)
				}
			}
			assert.NoError(t, txn.Commit(ctx))
			fs := taeEngine.GetDB().Runtime.Fs
			for _, obj := range objs {
				deleteFileName := obj.ObjectStats.ObjectName().String()
				err = fs.Delete(ctx, deleteFileName)
				assert.NoError(t, err)
			}
			gcTS := taeEngine.GetDB().TxnMgr.Now()
			gcTSFileName := ioutil.EncodeCompactCKPMetadataFullName(
				types.TS{}, gcTS,
			)
			writer, err := objectio.NewObjectWriterSpecial(objectio.WriterCheckpoint, gcTSFileName, fs)
			assert.NoError(t, err)
			_, err = writer.Write(containers.ToCNBatch(bat))
			assert.NoError(t, err)
			_, err = writer.WriteEnd(ctx)
			assert.NoError(t, err)
		}
		data, tombstone, _, err := handle.Next(ctx, mp)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
		assert.Nil(t, tombstone)
		assert.Nil(t, data)
	}
}

func TestChangesHandleStaleFiles5(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()
	startTS := taeHandler.GetDB().TxnMgr.Now()
	schema := catalog2.MockSchemaAll(23, 9)
	schema.Name = tableName
	rowCount := changesHandleTestRowCount()
	bat := catalog2.MockBatch(schema, rowCount)
	mp := common.DebugAllocator

	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)
	txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	require.Nil(t, rel.Append(ctx, bat))
	require.Nil(t, txn.Commit(ctx))

	txn, rel = testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	id := rel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	iter := rel.MakeObjectIt(false)
	deletedRows := 0
	for iter.Next() {
		obj := iter.GetObject()
		err = rel.RangeDelete(obj.Fingerprint(), 0, 0, handle.DT_Normal)
		deletedRows++
	}
	require.Nil(t, err)
	require.Nil(t, txn.Commit(ctx))

	testutil2.CompactBlocks(t, accountId, taeHandler.GetDB(), databaseName, schema, true)

	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, databaseName, tableName, false)
	require.Nil(t, err)

	// check partition state, before flush
	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		handle, err := rel.CollectChanges(ctx, startTS, taeHandler.GetDB().TxnMgr.Now(), true, mp)
		assert.NoError(t, err)
		totalRows := 0
		totalTombstones := 0
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			if tombstone != nil {
				assert.Equal(t, hint, engine.ChangesHandle_Tail_done)
				checkTombstoneBatch(tombstone, schema.GetPrimaryKey().Type, t)
				totalTombstones += tombstone.Vecs[0].Length()
				tombstone.Clean(mp)
			}
			if data != nil {
				checkInsertBatch(bat, data, t)
				totalRows += data.Vecs[0].Length()
				data.Clean(mp)
			}
		}
		assert.Equal(t, deletedRows, totalTombstones)
		assert.Equal(t, rowCount, totalRows)
		assert.NoError(t, handle.Close())
	}
}

func TestChangeHandleFilterBatch1(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()
	startTS := taeHandler.GetDB().TxnMgr.Now()
	schema := catalog2.MockSchemaAll(20, 0)
	schema.Name = tableName
	bat := catalog2.MockBatch(schema, 1)

	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	id := rel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	assert.NoError(t, txn.Commit(ctx))

	appendFn := func() {
		txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
		require.Nil(t, rel.Append(ctx, bat))
		require.Nil(t, txn.Commit(ctx))
	}

	deleteFn := func() {
		txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
		pkVal := bat.Vecs[0].Get(0)
		filter := handle.NewEQFilter(pkVal)
		err = rel.DeleteByFilter(ctx, filter)
		require.Nil(t, err)
		require.Nil(t, txn.Commit(ctx))
	}

	appendFn()
	deleteFn()
	ts1 := taeHandler.GetDB().TxnMgr.Now()

	appendFn()
	ts2 := taeHandler.GetDB().TxnMgr.Now()
	deleteFn()
	appendFn()
	ts3 := taeHandler.GetDB().TxnMgr.Now()
	deleteFn()
	ts4 := taeHandler.GetDB().TxnMgr.Now()

	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, databaseName, tableName, false)
	require.Nil(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	mp := common.DebugAllocator

	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		handle, err := rel.CollectChanges(ctx, startTS, ts1, true, mp)
		assert.NoError(t, err)
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			assert.NoError(t, err)
			assert.Nil(t, data)
			assert.Nil(t, tombstone)
			assert.Equal(t, hint, engine.ChangesHandle_Tail_done)
			if data == nil && tombstone == nil {
				break
			}
		}
		assert.NoError(t, handle.Close())

		handle, err = rel.CollectChanges(ctx, startTS, ts3, true, mp)
		assert.NoError(t, err)
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			assert.NoError(t, err)
			assert.Equal(t, hint, engine.ChangesHandle_Tail_done)
			if data == nil && tombstone == nil {
				break
			}
			assert.NotNil(t, data)
			assert.Equal(t, data.Vecs[0].Length(), 1)
			assert.Nil(t, tombstone)
		}
		assert.NoError(t, handle.Close())

		handle, err = rel.CollectChanges(ctx, ts2, ts3, true, mp)
		assert.NoError(t, err)
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			assert.NotNil(t, data)
			assert.Nil(t, tombstone)
			assert.Equal(t, data.Vecs[0].Length(), 1)
			assert.Equal(t, hint, engine.ChangesHandle_Tail_done)
		}
		assert.NoError(t, handle.Close())

		handle, err = rel.CollectChanges(ctx, ts2, ts4, true, mp)
		assert.NoError(t, err)
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			assert.NotNil(t, tombstone)
			assert.Nil(t, data)
			assert.Equal(t, tombstone.Vecs[0].Length(), 1)
			assert.Equal(t, hint, engine.ChangesHandle_Tail_done)
		}
		assert.NoError(t, handle.Close())
	}
}

func TestChangeHandleFilterBatch2(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()
	startTS := taeHandler.GetDB().TxnMgr.Now()
	schema := catalog2.MockSchemaAll(20, -1)
	schema.Name = tableName
	bat := catalog2.MockBatch(schema, 1)

	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	id := rel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	assert.NoError(t, txn.Commit(ctx))

	appendFn := func() {
		txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
		require.Nil(t, rel.Append(ctx, bat))
		require.Nil(t, txn.Commit(ctx))
	}

	deleteFn := func() {
		txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
		schema := rel.GetMeta().(*catalog2.TableEntry).GetLastestSchemaLocked(false)
		pkIdx := schema.GetPrimaryKey().Idx
		rowIDIdx := schema.GetColIdx(catalog2.PhyAddrColumnName)
		it := rel.MakeObjectIt(false)
		for it.Next() {
			blk := it.GetObject()
			defer blk.Close()
			blkCnt := uint16(blk.BlkCnt())
			for i := uint16(0); i < blkCnt; i++ {
				var view *containers.Batch
				err := blk.HybridScan(context.Background(), &view, i, []int{rowIDIdx, pkIdx}, common.DefaultAllocator)
				assert.NoError(t, err)
				defer view.Close()
				view.Compact()
				err = rel.DeleteByPhyAddrKeys(view.Vecs[0], view.Vecs[1], handle.DT_Normal)
				assert.NoError(t, err)
			}
		}
		err := txn.Commit(context.Background())
		assert.NoError(t, err)
	}

	appendFn()
	deleteFn()
	appendFn()
	deleteFn()
	appendFn()
	deleteFn()
	end := taeHandler.GetDB().TxnMgr.Now()

	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, databaseName, tableName, false)
	require.Nil(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	mp := common.DebugAllocator

	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		handle, err := rel.CollectChanges(ctx, startTS, end, true, mp)
		assert.NoError(t, err)
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			assert.NoError(t, err)
			assert.Equal(t, hint, engine.ChangesHandle_Tail_done)
			if data == nil && tombstone == nil {
				break
			}
			assert.NotNil(t, tombstone)
			assert.Equal(t, tombstone.Vecs[0].Length(), 3)
			assert.NotNil(t, data)
			assert.Equal(t, data.Vecs[0].Length(), 3)
		}
		assert.NoError(t, handle.Close())
	}
}

func TestChangesHandle7(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()
	startTS := taeHandler.GetDB().TxnMgr.Now()
	schema := catalog2.MockSchemaAll(20, -1)
	schema.Name = tableName
	bat := catalog2.MockBatch(schema, 8192)

	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)
	txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	require.Nil(t, rel.Append(ctx, bat))
	require.Nil(t, rel.Append(ctx, bat))
	require.Nil(t, txn.Commit(ctx))

	testutil2.CompactBlocks(t, accountId, taeHandler.GetDB(), databaseName, schema, true)

	txn, rel = testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	id := rel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	require.Nil(t, txn.Commit(ctx))

	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, databaseName, tableName, false)
	require.Nil(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	mp := common.DebugAllocator

	// check partition state, before flush
	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		handle, err := rel.CollectChanges(ctx, startTS, taeHandler.GetDB().TxnMgr.Now(), true, mp)
		assert.NoError(t, err)
		totalRowCount := 0
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			assert.Equal(t, hint, engine.ChangesHandle_Tail_done)
			assert.Nil(t, tombstone)
			checkInsertBatch(bat, data, t)
			totalRowCount += data.Vecs[0].Length()
			data.Clean(mp)
		}
		assert.Equal(t, totalRowCount, 8192*2)
		assert.NoError(t, handle.Close())
	}
}

func TestPartitionChangesHandle(t *testing.T) {
	/*
		t1 insert 1 row
		force ckp
		force ckp
		t2 insert 1 row
		ps.gc t1.next
		collect[t1, now]
	*/

	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()
	schema := catalog2.MockSchemaAll(20, 0)
	schema.Name = tableName
	bat := catalog2.MockBatch(schema, 2)
	defer bat.Close()
	bats := bat.Split(2)

	ssStub := gostub.Stub(
		&disttae.RequestSnapshotRead,
		disttae.GetSnapshotReadFnWithHandler(
			taeHandler.GetRPCHandle().HandleSnapshotRead,
		),
	)
	defer ssStub.Reset()
	// insert 1 row
	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)
	txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	require.Nil(t, rel.Append(ctx, bats[0]))
	require.Nil(t, txn.Commit(ctx))
	t1 := txn.GetCommitTS()

	txn, rel = testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	id := rel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	require.Nil(t, txn.Commit(ctx))

	// force ckp
	now := taeHandler.GetDB().TxnMgr.Now()
	taeHandler.GetDB().ForceCheckpoint(ctx, now)
	now = taeHandler.GetDB().TxnMgr.Now()
	taeHandler.GetDB().ForceCheckpoint(ctx, now)

	// insert 1 row

	txn, rel = testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	require.Nil(t, rel.Append(ctx, bats[1]))
	require.Nil(t, txn.Commit(ctx))
	t2 := txn.GetCommitTS()

	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, databaseName, tableName, false)
	require.Nil(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	mp := common.DebugAllocator

	disttaeEngine.Engine.ForceGC(ctx, t1.Next())
	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		handle, err := rel.CollectChanges(ctx, t1.Prev(), t2.Next(), true, mp)
		assert.NoError(t, err)
		var totalRows int
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			assert.Equal(t, hint, engine.ChangesHandle_Tail_done)
			assert.Nil(t, tombstone)
			t.Log(data.Attrs)
			totalRows += data.Vecs[0].Length()
			data.Clean(mp)
		}
		assert.Equal(t, totalRows, 2)
		assert.NoError(t, handle.Close())
	}
}

func TestGetObjectsFromCheckpointEntriesDedup(t *testing.T) {
	ioutil.RunPipelineTest(
		func() {
			catalog.SetupDefines("")

			ctx := context.Background()
			start := types.BuildTS(1, 0)
			end := types.BuildTS(2, 0)

			dataAppendable := newObjectEntryForCheckpointTest(t, 1, true, false, types.BuildTS(3, 0), types.TS{})
			dataCN := newObjectEntryForCheckpointTest(t, 2, false, true, types.BuildTS(4, 0), types.TS{})
			tombstoneAppendable := newObjectEntryForCheckpointTest(t, 3, true, false, types.BuildTS(5, 0), types.BuildTS(6, 0))
			tombstoneCN := newObjectEntryForCheckpointTest(t, 4, false, true, types.BuildTS(7, 0), types.BuildTS(8, 0))

			fakeReaders := []*checkpointReaderStub{
				{
					objects: []checkpointObject{
						{entry: dataAppendable, isTombstone: false},
						{entry: dataCN, isTombstone: false},
						{entry: tombstoneAppendable, isTombstone: true},
						{entry: tombstoneCN, isTombstone: true},
					},
				},
				{
					objects: []checkpointObject{
						{entry: dataAppendable, isTombstone: false},
						{entry: tombstoneAppendable, isTombstone: true},
					},
				},
			}

			readerIdx := 0
			restore := logtailreplay.SetCheckpointReaderFactoryForTest(func(uint32, objectio.Location, uint64, *mpool.MPool, fileservice.FileService) logtailreplay.CheckpointEntryReader {
				r := fakeReaders[readerIdx]
				readerIdx++
				return r
			})
			defer restore()

			entry1 := checkpoint.NewCheckpointEntry("", start, end, checkpoint.ET_Global)
			entry2 := checkpoint.NewCheckpointEntry("", start, end, checkpoint.ET_Global)

			dataAobjs, dataCNObjs, tombstoneAobjs, tombstoneCNObjs, err := logtailreplay.TestGetObjectsFromCheckpointEntries(ctx, 1, "", start, end, []*checkpoint.CheckpointEntry{entry1, entry2}, nil, nil)
			require.NoError(t, err)

			require.Len(t, dataAobjs, 1)
			require.Equal(t, dataAppendable.ObjectShortName().ShortString(), dataAobjs[0].ObjectShortName().ShortString())

			require.Len(t, dataCNObjs, 1)
			require.Equal(t, dataCN.ObjectShortName().ShortString(), dataCNObjs[0].ObjectShortName().ShortString())

			require.Len(t, tombstoneAobjs, 1)
			require.Equal(t, tombstoneAppendable.ObjectShortName().ShortString(), tombstoneAobjs[0].ObjectShortName().ShortString())

			require.Len(t, tombstoneCNObjs, 1)
			require.Equal(t, tombstoneCN.ObjectShortName().ShortString(), tombstoneCNObjs[0].ObjectShortName().ShortString())
		},
	)
}

func TestGetObjectsFromCheckpointRange(t *testing.T) {
	ioutil.RunPipelineTest(
		func() {
			catalog.SetupDefines("")

			ctx := context.Background()
			start := types.BuildTS(10, 0)
			end := types.BuildTS(20, 0)

			appendableOverlap := newObjectEntryForCheckpointTest(t, 1, true, false, types.BuildTS(5, 0), types.BuildTS(25, 0))
			appendableStillVisible := newObjectEntryForCheckpointTest(t, 2, true, false, types.BuildTS(6, 0), types.TS{})
			appendableDeletedBeforeRange := newObjectEntryForCheckpointTest(t, 3, true, false, types.BuildTS(4, 0), types.BuildTS(9, 0))
			appendableCreatedAfterRange := newObjectEntryForCheckpointTest(t, 4, true, false, types.BuildTS(21, 0), types.TS{})
			cnInRange := newObjectEntryForCheckpointTest(t, 5, false, true, types.BuildTS(12, 0), types.TS{})
			cnBeforeRange := newObjectEntryForCheckpointTest(t, 6, false, true, types.BuildTS(8, 0), types.TS{})
			mergedTNObject := newObjectEntryForCheckpointTest(t, 7, false, false, types.BuildTS(13, 0), types.TS{})
			mergedTNTombstone := newObjectEntryForCheckpointTest(t, 8, false, false, types.BuildTS(14, 0), types.TS{})

			fakeReaders := []*checkpointReaderStub{
				{
					objects: []checkpointObject{
						{entry: appendableOverlap, isTombstone: false},
						{entry: appendableStillVisible, isTombstone: false},
						{entry: appendableDeletedBeforeRange, isTombstone: false},
						{entry: appendableCreatedAfterRange, isTombstone: false},
						{entry: cnInRange, isTombstone: false},
						{entry: cnBeforeRange, isTombstone: false},
						{entry: mergedTNObject, isTombstone: false},
						{entry: appendableOverlap, isTombstone: true},
						{entry: cnInRange, isTombstone: true},
						{entry: mergedTNTombstone, isTombstone: true},
					},
				},
			}

			readerIdx := 0
			restore := logtailreplay.SetCheckpointReaderFactoryForTest(func(uint32, objectio.Location, uint64, *mpool.MPool, fileservice.FileService) logtailreplay.CheckpointEntryReader {
				r := fakeReaders[readerIdx]
				readerIdx++
				return r
			})
			defer restore()

			entry := checkpoint.NewCheckpointEntry("", start, end, checkpoint.ET_Global)

			dataAobjs, dataCNObjs, tombstoneAobjs, tombstoneCNObjs, err := logtailreplay.TestGetObjectsFromCheckpointRange(ctx, 1, "", start, end, []*checkpoint.CheckpointEntry{entry}, nil, nil)
			require.NoError(t, err)

			require.Len(t, dataCNObjs, 1)
			require.Equal(t, cnInRange.ObjectShortName().ShortString(), dataCNObjs[0].ObjectShortName().ShortString())

			require.Len(t, dataAobjs, 3)
			require.Equal(t, appendableOverlap.ObjectShortName().ShortString(), dataAobjs[0].ObjectShortName().ShortString())
			require.Equal(t, appendableStillVisible.ObjectShortName().ShortString(), dataAobjs[1].ObjectShortName().ShortString())
			require.Equal(t, mergedTNObject.ObjectShortName().ShortString(), dataAobjs[2].ObjectShortName().ShortString())

			require.Len(t, tombstoneAobjs, 2)
			require.Equal(t, appendableOverlap.ObjectShortName().ShortString(), tombstoneAobjs[0].ObjectShortName().ShortString())
			require.Equal(t, mergedTNTombstone.ObjectShortName().ShortString(), tombstoneAobjs[1].ObjectShortName().ShortString())

			require.Len(t, tombstoneCNObjs, 1)
			require.Equal(t, cnInRange.ObjectShortName().ShortString(), tombstoneCNObjs[0].ObjectShortName().ShortString())
		},
	)
}

type checkpointObject struct {
	entry       objectio.ObjectEntry
	isTombstone bool
}

type checkpointReaderStub struct {
	objects []checkpointObject
}

func (f *checkpointReaderStub) ReadMeta(context.Context) error {
	return nil
}

func (f *checkpointReaderStub) PrefetchData(string) {}

func (f *checkpointReaderStub) ConsumeCheckpointWithTableID(ctx context.Context, fn func(context.Context, fileservice.FileService, objectio.ObjectEntry, bool) error) error {
	for _, obj := range f.objects {
		if err := fn(ctx, nil, obj.entry, obj.isTombstone); err != nil {
			return err
		}
	}
	return nil
}

func newObjectEntryForCheckpointTest(t *testing.T, id byte, appendable bool, cnCreated bool, create types.TS, delete types.TS) objectio.ObjectEntry {
	t.Helper()

	var uuid types.Uuid
	uuid[15] = id
	seg := objectio.Segmentid(uuid)
	name := objectio.BuildObjectName(&seg, uint16(id))

	stats := objectio.NewObjectStats()
	require.NoError(t, objectio.SetObjectStatsObjectName(stats, name))
	if appendable {
		objectio.WithAppendable()(stats)
	}
	if cnCreated {
		objectio.WithCNCreated()(stats)
	}

	return objectio.ObjectEntry{
		ObjectStats: *stats,
		CreateTime:  create,
		DeleteTime:  delete,
	}
}

func TestISCPExecutor1(t *testing.T) {
	catalog.SetupDefines("")

	// idAllocator := common.NewIdAllocator(1000)

	var (
		accountId = catalog.System_Account
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	err := mock_mo_indexes(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_foreign_keys(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	result, err := execSql(disttaeEngine, ctxWithTimeout,
		"SELECT referenced_index_name, on_delete_origin, on_update_origin FROM mo_catalog.mo_foreign_keys")
	require.NoError(t, err)
	result.Close()
	err = mock_mo_intra_system_change_propagation_log(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))

	// create database and table

	bat := CreateDBAndTableForCNConsumerAndGetAppendData(t, disttaeEngine, ctxWithTimeout, "srcdb", "src_table", 10)
	bats := bat.Split(10)
	defer bat.Close()

	// append 1 row
	_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table")
	require.Nil(t, err)

	tableID := rel.GetTableID(ctxWithTimeout)

	err = rel.Write(ctxWithTimeout, containers.ToCNBatch(bats[0]))
	require.NoError(t, err)

	require.NoError(t, txn.Commit(ctxWithTimeout))

	// init cdc executor
	checkLeaseStub := gostub.Stub(
		&iscp.CheckLeaseWithRetry,
		func(
			context.Context,
			string,
			engine.Engine,
			client.TxnClient,
		) (bool, error) {
			return true, nil
		},
	)
	defer checkLeaseStub.Reset()
	cdcExecutor, err := iscp.NewISCPTaskExecutor(
		ctxWithTimeout,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		"",
		&iscp.ISCPExecutorOption{
			GCInterval:             time.Hour,
			GCTTL:                  time.Hour,
			SyncTaskInterval:       time.Millisecond * 100,
			FlushWatermarkInterval: time.Millisecond * 100,
			RetryTimes:             1,
		},
		common.DebugAllocator,
	)
	require.NoError(t, err)
	cdcExecutor.SetRpcHandleFn(taeHandler.GetRPCHandle().HandleGetChangedTableList)

	require.NoError(t, cdcExecutor.Start())
	defer cdcExecutor.Stop()

	// register cdc job
	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
	require.NoError(t, err)
	ok, err := iscp.RegisterJob(
		ctx, "", txn,
		&iscp.JobSpec{
			ConsumerInfo: iscp.ConsumerInfo{
				ConsumerType: int8(iscp.ConsumerType_CNConsumer),
			},
		},
		&iscp.JobID{
			JobName:   "hnsw_idx",
			DBName:    "srcdb",
			TableName: "src_table",
		},
		false,
	)
	require.True(t, ok)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctxWithTimeout))

	const jobName = "hnsw_idx"
	waitForISCPWatermark(
		t,
		func() (types.TS, bool) {
			return cdcExecutor.GetWatermark(accountId, tableID, jobName)
		},
		types.TimestampToTS(txn.Txn().CommitTS),
		10*time.Second,
		10*time.Millisecond,
		accountId,
		tableID,
		jobName,
	)

	// append 1 row
	_, rel, txn, err = disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table")
	require.Nil(t, err)

	err = rel.Write(ctxWithTimeout, containers.ToCNBatch(bats[1]))
	require.NoError(t, err)

	require.NoError(t, txn.Commit(ctxWithTimeout))

	target := types.TimestampToTS(txn.Txn().CommitTS)
	waitForISCPWatermark(
		t,
		func() (types.TS, bool) {
			return cdcExecutor.GetWatermark(accountId, tableID, jobName)
		},
		target,
		10*time.Second,
		10*time.Millisecond,
		accountId,
		tableID,
		jobName,
	)
	t.Logf("watermark reached %v", target.ToString())

	cdcExecutor.Stop()
	require.NoError(t, cdcExecutor.Start())

	// unregister cdc job
	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
	require.NoError(t, err)
	ok, err = iscp.UnregisterJob(
		ctx,
		"",
		txn,
		&iscp.JobID{
			JobName:   "hnsw_idx",
			DBName:    "srcdb",
			TableName: "src_table",
		},
	)
	require.True(t, ok)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctxWithTimeout))

	waitForISCPWatermarkAbsent(
		t,
		func() (types.TS, bool) {
			return cdcExecutor.GetWatermark(accountId, tableID, jobName)
		},
		10*time.Second,
		10*time.Millisecond,
		accountId,
		tableID,
		jobName,
	)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))

	CheckTableData(t, disttaeEngine, ctxWithTimeout, "srcdb", "src_table", tableID, "hnsw_idx")

}

func TestISCPRegisterUnregisterIdempotence(t *testing.T) {
	catalog.SetupDefines("")

	// idAllocator := common.NewIdAllocator(1000)

	var (
		accountId = catalog.System_Account
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	err := mock_mo_indexes(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_foreign_keys(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_intra_system_change_propagation_log(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))

	// create database and table

	bats := CreateDBAndTableForCNConsumerAndGetAppendData(t, disttaeEngine, ctxWithTimeout, "srcdb", "src_table", 10)
	defer bats.Close()

	_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table")
	require.Nil(t, err)

	tableID := rel.GetTableID(ctxWithTimeout)

	require.NoError(t, txn.Commit(ctxWithTimeout))

	// init cdc executor
	opts := GetTestISCPExecutorOption()
	opts.GCTTL = time.Hour
	checkLeaseStub := gostub.Stub(
		&iscp.CheckLeaseWithRetry,
		func(
			context.Context,
			string,
			engine.Engine,
			client.TxnClient,
		) (bool, error) {
			return true, nil
		},
	)
	defer checkLeaseStub.Reset()
	cdcExecutor, err := iscp.NewISCPTaskExecutor(
		ctxWithTimeout,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		"",
		opts,
		common.DebugAllocator,
	)
	require.NoError(t, err)
	cdcExecutor.SetRpcHandleFn(taeHandler.GetRPCHandle().HandleGetChangedTableList)

	require.NoError(t, cdcExecutor.Start())
	defer cdcExecutor.Stop()

	// unregister a job that not exist
	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
	require.NoError(t, err)
	ok, err := iscp.UnregisterJob(ctx, "", txn,
		&iscp.JobID{
			JobName:   "hnsw_idx",
			DBName:    "srcdb",
			TableName: "src_table",
		},
	)
	require.False(t, ok)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctxWithTimeout))

	// register cdc job
	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
	require.NoError(t, err)
	ok, err = iscp.RegisterJob(
		ctx, "", txn,
		&iscp.JobSpec{
			ConsumerInfo: iscp.ConsumerInfo{
				ConsumerType: int8(iscp.ConsumerType_CNConsumer),
			},
		},
		&iscp.JobID{
			JobName:   "hnsw_idx",
			DBName:    "srcdb",
			TableName: "src_table",
		},
		false,
	)
	require.True(t, ok)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctxWithTimeout))
	firstRegisterTarget := types.TimestampToTS(txn.Txn().CommitTS)
	waitForISCPWatermark(
		t,
		func() (types.TS, bool) {
			return cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
		},
		firstRegisterTarget,
		30*time.Second,
		10*time.Millisecond,
		accountId,
		tableID,
		"hnsw_idx",
	)

	// register duplicate job
	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
	require.NoError(t, err)
	ok, err = iscp.RegisterJob(ctx, "", txn,
		&iscp.JobSpec{
			ConsumerInfo: iscp.ConsumerInfo{
				ConsumerType: int8(iscp.ConsumerType_CNConsumer),
			},
		},
		&iscp.JobID{
			JobName:   "hnsw_idx",
			DBName:    "srcdb",
			TableName: "src_table",
		},
		false,
	)
	require.False(t, ok)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctxWithTimeout))

	// unregister cdc job
	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
	require.NoError(t, err)
	ok, err = iscp.UnregisterJob(ctx, "", txn,
		&iscp.JobID{
			JobName:   "hnsw_idx",
			DBName:    "srcdb",
			TableName: "src_table",
		},
	)
	require.True(t, ok)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctxWithTimeout))

	// unregister droppend job
	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
	require.NoError(t, err)
	ok, err = iscp.UnregisterJob(ctx, "", txn,
		&iscp.JobID{
			JobName:   "hnsw_idx",
			DBName:    "srcdb",
			TableName: "src_table",
		},
	)
	require.False(t, ok)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctxWithTimeout))

	waitForISCPWatermarkAbsent(
		t,
		func() (types.TS, bool) {
			return cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
		},
		30*time.Second,
		10*time.Millisecond,
		accountId,
		tableID,
		"hnsw_idx",
	)

	// register job again
	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
	require.NoError(t, err)
	ok, err = iscp.RegisterJob(ctx, "", txn,
		&iscp.JobSpec{
			ConsumerInfo: iscp.ConsumerInfo{
				ConsumerType: int8(iscp.ConsumerType_CNConsumer),
			},
		},
		&iscp.JobID{
			JobName:   "hnsw_idx",
			DBName:    "srcdb",
			TableName: "src_table",
		},
		false,
	)
	require.True(t, ok)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctxWithTimeout))
	registerTarget := types.TimestampToTS(txn.Txn().CommitTS)

	waitForISCPWatermark(
		t,
		func() (types.TS, bool) {
			return cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
		},
		registerTarget,
		30*time.Second,
		10*time.Millisecond,
		accountId,
		tableID,
		"hnsw_idx",
	)
}

// test error handle
func TestISCPExecutor3(t *testing.T) {
	catalog.SetupDefines("")

	// idAllocator := common.NewIdAllocator(1000)

	var (
		accountId = catalog.System_Account
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	err := mock_mo_indexes(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_foreign_keys(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_intra_system_change_propagation_log(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))

	bat := CreateDBAndTableForCNConsumerAndGetAppendData(t, disttaeEngine, ctxWithTimeout, "srcdb", "src_table", 10)
	bats := bat.Split(10)
	defer bat.Close()

	_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table")
	require.Nil(t, err)

	tableID := rel.GetTableID(ctxWithTimeout)

	require.NoError(t, txn.Commit(ctxWithTimeout))

	// init cdc executor
	checkLeaseStub := gostub.Stub(
		&iscp.CheckLeaseWithRetry,
		func(
			context.Context,
			string,
			engine.Engine,
			client.TxnClient,
		) (bool, error) {
			return true, nil
		},
	)
	defer checkLeaseStub.Reset()
	cdcExecutor, err := iscp.NewISCPTaskExecutor(
		ctxWithTimeout,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		"",
		&iscp.ISCPExecutorOption{
			SyncTaskInterval:       time.Millisecond * 10,
			FlushWatermarkInterval: time.Hour,
			GCTTL:                  time.Hour,
			GCInterval:             time.Hour,
			RetryTimes:             1,
		},
		common.DebugAllocator,
	)
	require.NoError(t, err)
	cdcExecutor.SetRpcHandleFn(taeHandler.GetRPCHandle().HandleGetChangedTableList)

	require.NoError(t, cdcExecutor.Start())
	defer cdcExecutor.Stop()

	require.True(t, fault.Enable(), "fault injection was already enabled before TestISCPExecutor3")
	t.Cleanup(func() {
		fault.Disable()
	})

	registerFn := func(indexName string) types.TS {
		txn, err := disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
		require.NoError(t, err)
		ok, err := iscp.RegisterJob(
			ctx, "", txn,
			&iscp.JobSpec{
				ConsumerInfo: iscp.ConsumerInfo{
					ConsumerType: int8(iscp.ConsumerType_CNConsumer),
				},
			},
			&iscp.JobID{
				JobName:   indexName,
				DBName:    "srcdb",
				TableName: "src_table",
			},
			false,
		)
		require.True(t, ok)
		require.NoError(t, err)
		require.NoError(t, txn.Commit(ctxWithTimeout))
		return types.TimestampToTS(txn.Txn().CommitTS)
	}

	unregisterFn := func(indexName string) types.TS {
		txn, err := disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
		require.NoError(t, err)
		ok, err := iscp.UnregisterJob(ctx, "", txn, &iscp.JobID{
			JobName:   indexName,
			DBName:    "srcdb",
			TableName: "src_table",
		})
		require.True(t, ok)
		require.NoError(t, err)
		require.NoError(t, txn.Commit(ctxWithTimeout))
		return types.TimestampToTS(txn.Txn().CommitTS)
	}

	appendFn := func(idx int) types.TS {
		_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table")
		require.Nil(t, err)

		err = rel.Write(ctxWithTimeout, containers.ToCNBatch(bats[idx]))
		require.NoError(t, err)

		require.NoError(t, txn.Commit(ctxWithTimeout))
		return types.TimestampToTS(txn.Txn().CommitTS)
	}

	waitForWatermark := func(indexName string, target types.TS) {
		waitForISCPWatermark(
			t,
			func() (types.TS, bool) {
				return cdcExecutor.GetWatermark(accountId, tableID, indexName)
			},
			target,
			30*time.Second,
			10*time.Millisecond,
			accountId,
			tableID,
			indexName,
		)
	}
	// The executor must recover after applying the job-log entry fails. The
	// phase barrier proves that the worker reached the injected branch before
	// the test inspects the watermark.
	applyLogFault := newISCPFaultBarrier(t, ctx, "applyISCPLog")
	registerTarget := registerFn("hnsw_idx_0")
	applyLogFault.WaitUntilHit(time.Now().Add(30 * time.Second))
	_, found := cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx_0")
	require.False(t, found)
	applyLogFault.Remove()
	waitForWatermark("hnsw_idx_0", registerTarget)

	// Removing the job must remove both its durable watermark and runtime
	// fence state. Re-register it after publishing data so the initial replay
	// path remains covered as a distinct contract from incremental replay.
	unregisterFn("hnsw_idx_0")
	waitForISCPWatermarkAbsent(
		t,
		func() (types.TS, bool) {
			return cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx_0")
		},
		30*time.Second,
		10*time.Millisecond,
		accountId,
		tableID,
		"hnsw_idx_0",
	)
	appendFn(0)
	waitForWatermark("hnsw_idx_0", registerFn("hnsw_idx_0"))

	exerciseInitialReplayFault := func(faultName, jobName string) {
		deadline := time.Now().Add(30 * time.Second)
		faultBarrier := newISCPFaultBarrier(t, ctx, faultName)
		target := registerFn(jobName)
		faultBarrier.WaitUntilHit(deadline)
		current, found := cdcExecutor.GetWatermark(accountId, tableID, jobName)
		require.Truef(
			t,
			!found || current.LT(&target),
			"fault %s did not stop %s before registration target: found=%t current=%s target=%s",
			faultName,
			jobName,
			found,
			current.ToString(),
			target.ToString(),
		)
		faultBarrier.Remove()
		waitForWatermark(jobName, target)
	}

	// Preserve initial replay coverage for each recoverable iteration phase;
	// TestISCPExecutor4 separately covers the same faults during incremental
	// replay of already-running jobs.
	exerciseInitialReplayFault("changesNext", "hnsw_idx_1")
	exerciseInitialReplayFault("collectChanges", "hnsw_idx_2")
	exerciseInitialReplayFault("consume", "hnsw_idx_3")

	// A dirty-table discovery failure is recoverable: the executor falls back
	// to running the candidate iteration. Block the exact branch, publish a
	// committed data target, then release it and require progress to that TS.
	dirtyTablesFault := newISCPFaultBarrier(t, ctx, "getDirtyTables")
	appendTarget := appendFn(1)
	dirtyTablesFault.WaitUntilHit(time.Now().Add(30 * time.Second))
	current, found := cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx_0")
	require.True(t, found)
	require.Truef(
		t,
		current.LT(&appendTarget),
		"getDirtyTables fault did not stop watermark before target: current=%s target=%s",
		current.ToString(),
		appendTarget.ToString(),
	)
	dirtyTablesFault.Remove()
	waitForWatermark("hnsw_idx_0", appendTarget)
	for i := 0; i < 4; i++ {
		CheckTableData(
			t,
			disttaeEngine,
			ctxWithTimeout,
			"srcdb",
			"src_table",
			tableID,
			fmt.Sprintf("hnsw_idx_%d", i),
		)
	}
}

// test error handle
func TestISCPExecutor4(t *testing.T) {
	catalog.SetupDefines("")

	// idAllocator := common.NewIdAllocator(1000)

	var (
		accountId = catalog.System_Account
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	err := mock_mo_indexes(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_foreign_keys(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_intra_system_change_propagation_log(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))

	bat := CreateDBAndTableForCNConsumerAndGetAppendData(t, disttaeEngine, ctxWithTimeout, "srcdb", "src_table", 10)
	bats := bat.Split(10)
	defer bat.Close()

	_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table")
	require.Nil(t, err)

	tableID := rel.GetTableID(ctxWithTimeout)

	require.NoError(t, txn.Commit(ctxWithTimeout))

	// init cdc executor
	checkLeaseStub := gostub.Stub(
		&iscp.CheckLeaseWithRetry,
		func(
			context.Context,
			string,
			engine.Engine,
			client.TxnClient,
		) (bool, error) {
			return true, nil
		},
	)
	defer checkLeaseStub.Reset()
	cdcExecutor, err := iscp.NewISCPTaskExecutor(
		ctxWithTimeout,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		"",
		&iscp.ISCPExecutorOption{
			SyncTaskInterval:       time.Millisecond * 10,
			FlushWatermarkInterval: time.Hour,
			GCTTL:                  time.Hour,
			GCInterval:             time.Hour,
			RetryTimes:             1,
		},
		common.DebugAllocator,
	)
	require.NoError(t, err)
	cdcExecutor.SetRpcHandleFn(taeHandler.GetRPCHandle().HandleGetChangedTableList)

	require.NoError(t, cdcExecutor.Start())
	defer cdcExecutor.Stop()

	require.True(t, fault.Enable(), "fault injection was already enabled before TestISCPExecutor4")
	t.Cleanup(func() {
		fault.Disable()
	})

	registerFn := func(jobName string) {
		txn, err := disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
		require.NoError(t, err)
		ok, err := iscp.RegisterJob(
			ctx, "", txn,
			&iscp.JobSpec{
				ConsumerInfo: iscp.ConsumerInfo{
					ConsumerType: int8(iscp.ConsumerType_CNConsumer),
				},
			},
			&iscp.JobID{
				JobName:   jobName,
				DBName:    "srcdb",
				TableName: "src_table",
			},
			false,
		)
		require.True(t, ok)
		require.NoError(t, err)
		require.NoError(t, txn.Commit(ctxWithTimeout))
	}

	appendFn := func(idx int) types.TS {
		_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table")
		require.NoError(t, err)

		err = rel.Write(ctxWithTimeout, containers.ToCNBatch(bats[idx]))
		require.NoError(t, err)

		require.NoError(t, txn.Commit(ctxWithTimeout))
		return types.TimestampToTS(txn.Txn().CommitTS)
	}

	const (
		indexCount = 3
		// Observable state transitions, rather than this duration, are the
		// correctness oracle. This only bounds a broken asynchronous phase;
		// successful phases return as soon as every watermark is durable.
		iscpPhaseHangGuard = 30 * time.Second
	)
	waitForAllWatermarks := func(target types.TS, deadline time.Time) {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			require.FailNowf(
				t,
				"ISCP phase budget exhausted before watermark wait",
				"account=%d table=%d target=%s",
				accountId,
				tableID,
				target.ToString(),
			)
		}

		reached := assert.Eventually(t, func() bool {
			for i := 0; i < indexCount; i++ {
				current, found := cdcExecutor.GetWatermark(
					accountId,
					tableID,
					fmt.Sprintf("hnsw_idx_%d", i),
				)
				if !found || current.LT(&target) {
					return false
				}
			}
			return true
		}, remaining, 10*time.Millisecond)
		if reached {
			return
		}

		for i := 0; i < indexCount; i++ {
			indexName := fmt.Sprintf("hnsw_idx_%d", i)
			current, found := cdcExecutor.GetWatermark(accountId, tableID, indexName)
			t.Logf(
				"ISCP watermark timeout: account=%d table=%d job=%s found=%t current=%s target=%s",
				accountId,
				tableID,
				indexName,
				found,
				current.ToString(),
				target.ToString(),
			)
		}
		require.FailNow(t, "not all ISCP watermarks reached the target")
	}

	exerciseFaultRecovery := func(name string, batchIndex int) {
		deadline := time.Now().Add(iscpPhaseHangGuard)
		faultBarrier := newISCPFaultBarrier(t, ctx, name)
		target := appendFn(batchIndex)
		faultBarrier.WaitUntilHit(deadline)

		current, found := cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx_0")
		require.True(t, found)
		require.Truef(
			t,
			current.LT(&target),
			"fault %s did not stop hnsw_idx_0 before target: current=%s target=%s",
			name,
			current.ToString(),
			target.ToString(),
		)

		faultBarrier.Remove()
		waitForAllWatermarks(target, deadline)
	}

	for i := 0; i < indexCount; i++ {
		registerFn(fmt.Sprintf("hnsw_idx_%d", i))
	}

	appendFn(0)
	appendFn(1)

	// insertAsyncIndexIterations failed
	deadline := time.Now().Add(iscpPhaseHangGuard)
	target := appendFn(2)
	waitForAllWatermarks(target, deadline)

	// collectChanges failed
	exerciseFaultRecovery("collectChanges", 3)

	// changesNext failed
	exerciseFaultRecovery("changesNext", 6)

	// consume failed
	exerciseFaultRecovery("consume", 7)

	// consume, firstTxn failed
	exerciseFaultRecovery("consumeWithJobName:hnsw_idx_0", 8)

	for i := 0; i < indexCount; i++ {
		CheckTableData(t, disttaeEngine, ctxWithTimeout, "srcdb", "src_table", tableID, fmt.Sprintf("hnsw_idx_%d", i))
	}
}

// test multiple indexes with same table
func TestISCPExecutor5(t *testing.T) {
	catalog.SetupDefines("")

	// idAllocator := common.NewIdAllocator(1000)

	var (
		accountId = catalog.System_Account
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(
		ctx,
		testutil.TestOptions{TaeEngineOptions: config.WithLongScanAndCKPOpts(nil)},
		t,
	)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	err := mock_mo_indexes(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_foreign_keys(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_intra_system_change_propagation_log(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))

	dbName := "db"
	rows := 3
	tableCount := 2
	tableIDs := make([]uint64, tableCount)
	bats := make([]*containers.Batch, tableCount)
	for i := 0; i < tableCount; i++ {

		tableName := fmt.Sprintf("src_table_%d", i)
		bat := CreateDBAndTableForCNConsumerAndGetAppendData(t, disttaeEngine, ctxWithTimeout, dbName, tableName, rows)
		bats[i] = bat
		defer bat.Close()
	}

	for i := 0; i < tableCount; i++ {
		tableName := fmt.Sprintf("src_table_%d", i)
		_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, dbName, tableName)
		require.Nil(t, err)

		tableIDs[i] = rel.GetTableID(ctxWithTimeout)

		require.NoError(t, txn.Commit(ctxWithTimeout))
	}

	// init cdc executor
	checkLeaseStub := gostub.Stub(
		&iscp.CheckLeaseWithRetry,
		func(
			context.Context,
			string,
			engine.Engine,
			client.TxnClient,
		) (bool, error) {
			return true, nil
		},
	)
	defer checkLeaseStub.Reset()
	cdcExecutor, err := iscp.NewISCPTaskExecutor(
		ctxWithTimeout,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		"",
		&iscp.ISCPExecutorOption{
			SyncTaskInterval:       time.Millisecond * 10,
			FlushWatermarkInterval: time.Hour,
			GCTTL:                  time.Hour,
			GCInterval:             time.Hour,
			RetryTimes:             1,
		},
		common.DebugAllocator,
	)
	require.NoError(t, err)
	cdcExecutor.SetRpcHandleFn(taeHandler.GetRPCHandle().HandleGetChangedTableList)

	require.NoError(t, cdcExecutor.Start())
	defer cdcExecutor.Stop()

	registerFn := func(indexName string, tableName string) {
		txn, err := disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
		require.NoError(t, err)
		ok, err := iscp.RegisterJob(
			ctx, "", txn,
			&iscp.JobSpec{
				ConsumerInfo: iscp.ConsumerInfo{
					ConsumerType: int8(iscp.ConsumerType_CNConsumer),
				},
			},
			&iscp.JobID{
				JobName:   indexName,
				DBName:    dbName,
				TableName: tableName,
			},
			false,
		)
		require.True(t, ok)
		require.NoError(t, err)
		require.NoError(t, txn.Commit(ctxWithTimeout))
	}

	waitForWatermark := func(indexName string, target types.TS, tableIdx int) {
		waitForISCPWatermark(
			t,
			func() (types.TS, bool) {
				return cdcExecutor.GetWatermark(accountId, tableIDs[tableIdx], indexName)
			},
			target,
			30*time.Second,
			10*time.Millisecond,
			accountId,
			tableIDs[tableIdx],
			indexName,
		)
	}

	indexCount := 3
	updateTimes := 10
	if testing.Short() {
		// Exercise one complete delete/append cycle and finish with populated
		// source and index tables, so CheckTableData still compares real rows.
		updateTimes = 2
	}

	for j := 0; j < tableCount; j++ {
		for i := 0; i < indexCount; i++ {
			registerFn(fmt.Sprintf("hnsw_idx_%d", i), fmt.Sprintf("src_table_%d", j))
		}
	}

	deleted := make([]bool, tableCount)
	targets := make([]types.TS, tableCount)
	for i := 0; i < updateTimes; i++ {
		for j := 0; j < tableCount; j++ {
			if deleted[j] {
				targets[j] = testutil2.AppendWithCommitTS(t, accountId, taeHandler.GetDB(), dbName, fmt.Sprintf("src_table_%d", j), bats[j])
				deleted[j] = false
			} else {
				targets[j] = testutil2.DeleteAllWithCommitTS(t, accountId, taeHandler.GetDB(), dbName, fmt.Sprintf("src_table_%d", j))
				deleted[j] = true
			}
		}
	}

	for j := 0; j < tableCount; j++ {
		for i := 0; i < indexCount; i++ {
			waitForWatermark(fmt.Sprintf("hnsw_idx_%d", i), targets[j], j)
			CheckTableData(t, disttaeEngine, ctxWithTimeout, dbName, fmt.Sprintf("src_table_%d", j), tableIDs[j], fmt.Sprintf("hnsw_idx_%d", i))
		}
	}
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))

}

func TestISCPExecutor6(t *testing.T) {
	catalog.SetupDefines("")

	// idAllocator := common.NewIdAllocator(1000)

	var (
		accountId = catalog.System_Account
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	err := mock_mo_indexes(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_foreign_keys(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_intra_system_change_propagation_log(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))

	account2 := uint32(2)
	ctxAccountID2 := context.WithValue(ctx, defines.TenantIDKey{}, account2)
	ctxAccountID2, cancel2 := context.WithTimeout(ctxAccountID2, time.Minute*5)
	err = mock_mo_indexes(disttaeEngine, ctxAccountID2)
	require.NoError(t, err)
	err = mock_mo_foreign_keys(disttaeEngine, ctxAccountID2)
	require.NoError(t, err)
	defer cancel2()
	bat := CreateDBAndTableForCNConsumerAndGetAppendData(t, disttaeEngine, ctxAccountID2, "srcdb", "src_table", 10)
	defer bat.Close()

	_, rel, txn, err := disttaeEngine.GetTable(ctxAccountID2, "srcdb", "src_table")
	require.Nil(t, err)

	tableID := rel.GetTableID(ctxAccountID2)

	require.NoError(t, txn.Commit(ctxAccountID2))

	// init cdc executor
	checkLeaseStub := gostub.Stub(
		&iscp.CheckLeaseWithRetry,
		func(
			context.Context,
			string,
			engine.Engine,
			client.TxnClient,
		) (bool, error) {
			return true, nil
		},
	)
	defer checkLeaseStub.Reset()
	cdcExecutor, err := iscp.NewISCPTaskExecutor(
		ctxWithTimeout,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		"",
		&iscp.ISCPExecutorOption{
			SyncTaskInterval:       time.Millisecond * 10,
			FlushWatermarkInterval: time.Hour,
			GCTTL:                  time.Hour,
			GCInterval:             time.Hour,
			RetryTimes:             1,
		},
		common.DebugAllocator,
	)
	require.NoError(t, err)
	cdcExecutor.SetRpcHandleFn(taeHandler.GetRPCHandle().HandleGetChangedTableList)

	require.NoError(t, cdcExecutor.Start())
	defer cdcExecutor.Stop()

	txn, err = disttaeEngine.NewTxnOperator(ctxAccountID2, disttaeEngine.Engine.LatestLogtailAppliedTime())
	require.NoError(t, err)
	ok, err := iscp.RegisterJob(
		ctxAccountID2, "", txn,
		&iscp.JobSpec{
			ConsumerInfo: iscp.ConsumerInfo{
				ConsumerType: int8(iscp.ConsumerType_CNConsumer),
			},
		},
		&iscp.JobID{
			JobName:   "idx",
			DBName:    "srcdb",
			TableName: "src_table",
		},
		false,
	)
	require.True(t, ok)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctxAccountID2))
	waitForISCPWatermark(
		t,
		func() (types.TS, bool) {
			return cdcExecutor.GetWatermark(account2, tableID, "idx")
		},
		types.TimestampToTS(txn.Txn().CommitTS),
		10*time.Second,
		10*time.Millisecond,
		account2,
		tableID,
		"idx",
	)
	t.Log(cdcExecutor.String())
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
}

// test flush watermark for all tables
func TestISCPExecutor7(t *testing.T) {
	catalog.SetupDefines("")

	// idAllocator := common.NewIdAllocator(1000)

	var (
		accountId = catalog.System_Account
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	err := mock_mo_indexes(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_foreign_keys(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_intra_system_change_propagation_log(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))

	// create database and table

	bat := CreateDBAndTableForCNConsumerAndGetAppendData(t, disttaeEngine, ctxWithTimeout, "srcdb", "src_table", 10)
	bats := bat.Split(10)
	defer bat.Close()

	// append 1 row
	_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table")
	require.Nil(t, err)

	tableID := rel.GetTableID(ctxWithTimeout)

	err = rel.Write(ctxWithTimeout, containers.ToCNBatch(bats[0]))
	require.NoError(t, err)

	require.NoError(t, txn.Commit(ctxWithTimeout))

	// init cdc executor
	checkLeaseStub := gostub.Stub(
		&iscp.CheckLeaseWithRetry,
		func(
			context.Context,
			string,
			engine.Engine,
			client.TxnClient,
		) (bool, error) {
			return true, nil
		},
	)
	defer checkLeaseStub.Reset()
	cdcExecutor, err := iscp.NewISCPTaskExecutor(
		ctxWithTimeout,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		"",
		&iscp.ISCPExecutorOption{
			GCInterval:             time.Hour,
			GCTTL:                  time.Hour,
			SyncTaskInterval:       time.Millisecond * 100,
			FlushWatermarkInterval: time.Millisecond * 100,
			RetryTimes:             1,
		},
		common.DebugAllocator,
	)
	require.NoError(t, err)
	cdcExecutor.SetRpcHandleFn(taeHandler.GetRPCHandle().HandleGetChangedTableList)

	require.NoError(t, cdcExecutor.Start())
	defer cdcExecutor.Stop()

	// register cdc job
	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
	require.NoError(t, err)
	ok, err := iscp.RegisterJob(
		ctx, "", txn,
		&iscp.JobSpec{
			ConsumerInfo: iscp.ConsumerInfo{
				ConsumerType: int8(iscp.ConsumerType_CNConsumer),
			},
		},
		&iscp.JobID{
			JobName:   "hnsw_idx",
			DBName:    "srcdb",
			TableName: "src_table",
		},
		false,
	)
	require.True(t, ok)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctxWithTimeout))

	const jobName = "hnsw_idx"
	waitForISCPWatermark(
		t,
		func() (types.TS, bool) {
			return cdcExecutor.GetWatermark(accountId, tableID, jobName)
		},
		types.TimestampToTS(txn.Txn().CommitTS),
		10*time.Second,
		10*time.Millisecond,
		accountId,
		tableID,
		jobName,
	)

}

type iscpFaultBarrier struct {
	t          *testing.T
	name       string
	waitersKey string
	removeOnce sync.Once
	removers   []func() (bool, error)
}

func newISCPFaultBarrier(t *testing.T, ctx context.Context, name string) *iscpFaultBarrier {
	t.Helper()
	b := &iscpFaultBarrier{
		t:          t,
		name:       name,
		waitersKey: objectio.ISCPExecutorFaultWaitKey(name) + ":waiters",
	}
	t.Cleanup(b.Remove)

	addFault := func(faultName, action, arg string) {
		require.NoError(t, fault.AddFaultPoint(
			ctx,
			faultName,
			":::",
			action,
			0,
			arg,
			false,
		))
		b.removers = append(b.removers, func() (bool, error) {
			return fault.RemoveFaultPoint(context.Background(), faultName)
		})
	}

	waitKey := objectio.ISCPExecutorFaultWaitKey(name)
	addFault(waitKey, "wait", "")
	addFault(b.waitersKey, "getwaiters", waitKey)
	removeInjection, err := objectio.InjectCDCExecutor(name)
	require.NoError(t, err)
	b.removers = append(b.removers, removeInjection)
	return b
}

func (b *iscpFaultBarrier) WaitUntilHit(deadline time.Time) {
	b.t.Helper()
	remaining := time.Until(deadline)
	if remaining <= 0 {
		require.FailNowf(b.t, "ISCP fault phase budget exhausted", "fault=%s", b.name)
	}
	require.Eventuallyf(
		b.t,
		func() bool {
			waiters, _, ok := fault.TriggerFault(b.waitersKey)
			return ok && waiters > 0
		},
		remaining,
		10*time.Millisecond,
		"ISCP worker did not reach injected fault %s",
		b.name,
	)
}

func (b *iscpFaultBarrier) Remove() {
	b.t.Helper()
	b.removeOnce.Do(func() {
		// Remove the injected failure first, then release the worker parked at
		// its exact phase barrier. Remove in reverse installation order so no
		// new worker can enter while the existing waiter is being released.
		for i := len(b.removers) - 1; i >= 0; i-- {
			removed, err := b.removers[i]()
			require.NoError(b.t, err)
			require.True(b.t, removed)
		}
	})
}

type iscpWatermarkSnapshot struct {
	current types.TS
	found   bool
}

func waitForISCPWatermark(
	t require.TestingT,
	getWatermark func() (types.TS, bool),
	target types.TS,
	waitFor time.Duration,
	tick time.Duration,
	accountID uint32,
	tableID uint64,
	jobName string,
) {
	waitForISCPWatermarkState(
		t,
		getWatermark,
		func(current types.TS, found bool) bool {
			return found && current.GE(&target)
		},
		fmt.Sprintf("reach %s", target.ToString()),
		waitFor,
		tick,
		accountID,
		tableID,
		jobName,
	)
}

func waitForISCPWatermarkAbsent(
	t require.TestingT,
	getWatermark func() (types.TS, bool),
	waitFor time.Duration,
	tick time.Duration,
	accountID uint32,
	tableID uint64,
	jobName string,
) {
	waitForISCPWatermarkState(
		t,
		getWatermark,
		func(_ types.TS, found bool) bool {
			return !found
		},
		"be removed",
		waitFor,
		tick,
		accountID,
		tableID,
		jobName,
	)
}

func waitForISCPWatermarkState(
	t require.TestingT,
	getWatermark func() (types.TS, bool),
	reachedState func(types.TS, bool) bool,
	expectation string,
	waitFor time.Duration,
	tick time.Duration,
	accountID uint32,
	tableID uint64,
	jobName string,
) {
	var lastCompleted atomic.Pointer[iscpWatermarkSnapshot]
	reached := assert.Eventually(t, func() bool {
		current, found := getWatermark()
		lastCompleted.Store(&iscpWatermarkSnapshot{
			current: current,
			found:   found,
		})
		return reachedState(current, found)
	}, waitFor, tick)
	if reached {
		return
	}

	var (
		current  types.TS
		found    bool
		observed bool
	)
	if snapshot := lastCompleted.Load(); snapshot != nil {
		current = snapshot.current
		found = snapshot.found
		observed = true
	}
	require.FailNowf(
		t,
		"ISCP watermark did not reach expected state",
		"account=%d table=%d job=%s expectation=%s observed=%t found=%t current=%s",
		accountID,
		tableID,
		jobName,
		expectation,
		observed,
		found,
		current.ToString(),
	)
}

type failNowPanicTestingT struct{}

func (failNowPanicTestingT) Errorf(string, ...any) {}

func (failNowPanicTestingT) FailNow() {
	panic("fail now")
}

func TestWaitForISCPWatermarkTimeoutDoesNotWaitForBlockedGetter(t *testing.T) {
	var tableMu sync.RWMutex
	tableMu.Lock()
	var unlockOnce sync.Once
	unlockTable := func() {
		unlockOnce.Do(tableMu.Unlock)
	}
	defer unlockTable()

	conditionEntered := make(chan struct{})
	conditionExited := make(chan struct{})
	var calls atomic.Int32

	getWatermark := func() (types.TS, bool) {
		calls.Add(1)
		close(conditionEntered)
		tableMu.RLock()
		defer tableMu.RUnlock()
		close(conditionExited)
		return types.BuildTS(1, 0), true
	}

	helperResult := make(chan any, 1)
	go func() {
		defer func() {
			helperResult <- recover()
		}()
		waitForISCPWatermark(
			failNowPanicTestingT{},
			getWatermark,
			types.BuildTS(2, 0),
			100*time.Millisecond,
			time.Millisecond,
			1,
			2,
			"blocked-job",
		)
	}()

	select {
	case <-conditionEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("watermark condition did not enter the blocked getter")
	}
	select {
	case recovered := <-helperResult:
		require.Equal(t, "fail now", recovered)
		require.Equal(t, int32(1), calls.Load())
	case <-time.After(5 * time.Second):
		t.Fatal("watermark timeout waited for the blocked getter")
	}

	unlockTable()
	select {
	case <-conditionExited:
	case <-time.After(5 * time.Second):
		t.Fatal("blocked watermark condition did not exit after lock release")
	}
}

// test delete
func TestISCPExecutor8(t *testing.T) {
	catalog.SetupDefines("")

	// idAllocator := common.NewIdAllocator(1000)

	var (
		accountId = catalog.System_Account
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	err := mock_mo_indexes(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_foreign_keys(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_intra_system_change_propagation_log(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))

	// create database and table

	bat := CreateDBAndTableForCNConsumerAndGetAppendData(t, disttaeEngine, ctxWithTimeout, "srcdb", "src_table", 10)
	bats := bat.Split(10)
	defer bat.Close()

	// append 1 row
	_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table")
	require.Nil(t, err)

	tableID := rel.GetTableID(ctxWithTimeout)

	err = rel.Write(ctxWithTimeout, containers.ToCNBatch(bats[0]))
	require.NoError(t, err)

	require.NoError(t, txn.Commit(ctxWithTimeout))

	// init cdc executor
	checkLeaseStub := gostub.Stub(
		&iscp.CheckLeaseWithRetry,
		func(
			context.Context,
			string,
			engine.Engine,
			client.TxnClient,
		) (bool, error) {
			return true, nil
		},
	)
	defer checkLeaseStub.Reset()
	cdcExecutor, err := iscp.NewISCPTaskExecutor(
		ctxWithTimeout,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		"",
		&iscp.ISCPExecutorOption{
			GCInterval:             time.Hour,
			GCTTL:                  time.Hour,
			SyncTaskInterval:       time.Millisecond * 100,
			FlushWatermarkInterval: time.Millisecond * 100,
			RetryTimes:             1,
		},
		common.DebugAllocator,
	)
	require.NoError(t, err)
	cdcExecutor.SetRpcHandleFn(taeHandler.GetRPCHandle().HandleGetChangedTableList)

	require.NoError(t, cdcExecutor.Start())
	defer cdcExecutor.Stop()

	// register cdc job
	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
	require.NoError(t, err)
	ok, err := iscp.RegisterJob(
		ctx, "", txn,
		&iscp.JobSpec{
			ConsumerInfo: iscp.ConsumerInfo{
				ConsumerType: int8(iscp.ConsumerType_CNConsumer),
			},
		},
		&iscp.JobID{
			JobName:   "hnsw_idx",
			DBName:    "srcdb",
			TableName: "src_table",
		},
		false,
	)
	require.True(t, ok)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctxWithTimeout))

	const jobName = "hnsw_idx"
	waitForWatermark := func(target types.TS) {
		waitForISCPWatermark(
			t,
			func() (types.TS, bool) {
				return cdcExecutor.GetWatermark(accountId, tableID, jobName)
			},
			target,
			10*time.Second,
			10*time.Millisecond,
			accountId,
			tableID,
			jobName,
		)
	}

	waitForWatermark(types.TimestampToTS(txn.Txn().CommitTS))

	deleteCommitTS := testutil2.DeleteAllWithCommitTS(t, accountId, taeHandler.GetDB(), "srcdb", "src_table")
	waitForWatermark(deleteCommitTS)

}

func TestUpdateJobSpec(t *testing.T) {
	catalog.SetupDefines("")

	// idAllocator := common.NewIdAllocator(1000)

	var (
		accountId = catalog.System_Account
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	err := mock_mo_indexes(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_foreign_keys(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_intra_system_change_propagation_log(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	bat := CreateDBAndTableForCNConsumerAndGetAppendData(t, disttaeEngine, ctxWithTimeout, "srcdb", "src_table", 10)
	bats := bat.Split(10)
	defer bat.Close()

	_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table")
	require.Nil(t, err)

	tableID := rel.GetTableID(ctxWithTimeout)

	require.NoError(t, txn.Commit(ctxWithTimeout))

	// init cdc executor
	checkLeaseStub := gostub.Stub(
		&iscp.CheckLeaseWithRetry,
		func(
			context.Context,
			string,
			engine.Engine,
			client.TxnClient,
		) (bool, error) {
			return true, nil
		},
	)
	defer checkLeaseStub.Reset()
	cdcExecutor, err := iscp.NewISCPTaskExecutor(
		ctxWithTimeout,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		"",
		&iscp.ISCPExecutorOption{
			SyncTaskInterval:       time.Millisecond * 10,
			FlushWatermarkInterval: time.Millisecond * 10,
			FlushWatermarkTTL:      time.Hour,
			GCTTL:                  time.Hour,
			GCInterval:             time.Hour,
			RetryTimes:             1,
		},
		common.DebugAllocator,
	)
	require.NoError(t, err)
	cdcExecutor.SetRpcHandleFn(taeHandler.GetRPCHandle().HandleGetChangedTableList)

	require.NoError(t, cdcExecutor.Start())
	defer cdcExecutor.Stop()
	jobName := "job1"
	dbName := "srcdb"
	tableName := "src_table"

	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
	require.NoError(t, err)
	ok, err := iscp.RegisterJob(
		ctx, "", txn,
		&iscp.JobSpec{
			ConsumerInfo: iscp.ConsumerInfo{
				ConsumerType: int8(iscp.ConsumerType_CNConsumer),
			},
		},
		&iscp.JobID{
			JobName:   jobName,
			DBName:    dbName,
			TableName: tableName,
		},
		false,
	)
	require.True(t, ok)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctxWithTimeout))

	appendFn := func(idx int) types.TS {
		_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, dbName, tableName)
		require.NoError(t, err)

		err = rel.Write(ctxWithTimeout, containers.ToCNBatch(bats[idx]))
		require.NoError(t, err)

		require.NoError(t, txn.Commit(ctxWithTimeout))
		return types.TimestampToTS(txn.Txn().CommitTS)
	}

	waitForWatermark := func(target types.TS) {
		waitForISCPWatermark(
			t,
			func() (types.TS, bool) {
				return cdcExecutor.GetWatermark(accountId, tableID, jobName)
			},
			target,
			10*time.Second,
			10*time.Millisecond,
			accountId,
			tableID,
			jobName,
		)
	}

	waitForJobType := func(expected uint16) {
		reached := assert.Eventually(t, func() bool {
			jobType, found := cdcExecutor.GetJobType(accountId, tableID, jobName)
			return found && jobType == expected
		}, 10*time.Second, 10*time.Millisecond)
		if reached {
			return
		}
		jobType, found := cdcExecutor.GetJobType(accountId, tableID, jobName)
		require.FailNowf(
			t,
			"ISCP job type did not reach target",
			"account=%d table=%d job=%s found=%t current=%d target=%d",
			accountId,
			tableID,
			jobName,
			found,
			jobType,
			expected,
		)
	}
	waitForWatermark(appendFn(0))

	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
	require.NoError(t, err)
	err = iscp.UpdateJobSpec(
		ctx, "", txn,
		&iscp.JobID{
			JobName:   jobName,
			DBName:    dbName,
			TableName: tableName,
		},
		&iscp.JobSpec{
			ConsumerInfo: iscp.ConsumerInfo{
				ConsumerType: int8(iscp.ConsumerType_CNConsumer),
			},
			TriggerSpec: iscp.TriggerSpec{
				JobType: iscp.TriggerType_AlwaysUpdate,
			},
		},
	)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctxWithTimeout))

	waitForJobType(iscp.TriggerType_AlwaysUpdate)
	waitForWatermark(appendFn(1))
	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
	require.NoError(t, err)
	err = iscp.UpdateJobSpec(
		ctx, "", txn,
		&iscp.JobID{
			JobName:   jobName,
			DBName:    dbName,
			TableName: tableName,
		},
		&iscp.JobSpec{
			ConsumerInfo: iscp.ConsumerInfo{
				ConsumerType: int8(iscp.ConsumerType_CNConsumer),
			},
			TriggerSpec: iscp.TriggerSpec{
				JobType: iscp.TriggerType_Timed,
				Schedule: iscp.Schedule{
					Interval: time.Millisecond * 10,
				},
			},
		},
	)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctxWithTimeout))

	waitForJobType(iscp.TriggerType_Timed)
	waitForWatermark(appendFn(2))

	CheckTableData(t, disttaeEngine, ctxWithTimeout, dbName, tableName, tableID, jobName)
}

func TestFlushWatermark(t *testing.T) {
	catalog.SetupDefines("")

	// idAllocator := common.NewIdAllocator(1000)

	var (
		accountId = catalog.System_Account
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	err := mock_mo_indexes(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_foreign_keys(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_intra_system_change_propagation_log(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	bat := CreateDBAndTableForCNConsumerAndGetAppendData(t, disttaeEngine, ctxWithTimeout, "srcdb", "src_table", 10)
	defer bat.Close()

	_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table")
	require.Nil(t, err)

	tableID := rel.GetTableID(ctxWithTimeout)

	require.NoError(t, txn.Commit(ctxWithTimeout))

	// init cdc executor
	checkLeaseStub := gostub.Stub(
		&iscp.CheckLeaseWithRetry,
		func(
			context.Context,
			string,
			engine.Engine,
			client.TxnClient,
		) (bool, error) {
			return true, nil
		},
	)
	defer checkLeaseStub.Reset()
	cdcExecutor, err := iscp.NewISCPTaskExecutor(
		ctxWithTimeout,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		"",
		&iscp.ISCPExecutorOption{
			SyncTaskInterval:       time.Millisecond * 10,
			FlushWatermarkInterval: time.Hour,
			FlushWatermarkTTL:      time.Hour,
			GCTTL:                  time.Hour,
			GCInterval:             time.Hour,
			RetryTimes:             1,
		},
		common.DebugAllocator,
	)
	require.NoError(t, err)
	cdcExecutor.SetRpcHandleFn(taeHandler.GetRPCHandle().HandleGetChangedTableList)

	require.NoError(t, cdcExecutor.Start())
	defer cdcExecutor.Stop()
	jobName := "job1"
	dbName := "srcdb"
	tableName := "src_table"

	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
	require.NoError(t, err)
	ok, err := iscp.RegisterJob(
		ctx, "", txn,
		&iscp.JobSpec{
			ConsumerInfo: iscp.ConsumerInfo{
				ConsumerType: int8(iscp.ConsumerType_CNConsumer),
			},
		},
		&iscp.JobID{
			JobName:   jobName,
			DBName:    dbName,
			TableName: tableName,
		},
		false,
	)
	require.True(t, ok)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctxWithTimeout))
	waitForISCPWatermark(
		t,
		func() (types.TS, bool) {
			return cdcExecutor.GetWatermark(accountId, tableID, jobName)
		},
		types.TimestampToTS(txn.Txn().CommitTS),
		10*time.Second,
		10*time.Millisecond,
		accountId,
		tableID,
		jobName,
	)

	require.NoError(t, cdcExecutor.FlushWatermarkForAllTables(0))
}

func TestGCInMemoryJob(t *testing.T) {
	catalog.SetupDefines("")

	// idAllocator := common.NewIdAllocator(1000)

	var (
		accountId = catalog.System_Account
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	err := mock_mo_indexes(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_foreign_keys(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_intra_system_change_propagation_log(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))

	bat := CreateDBAndTableForCNConsumerAndGetAppendData(t, disttaeEngine, ctxWithTimeout, "srcdb", "src_table", 10)
	bats := bat.Split(10)
	defer bat.Close()

	// append 1 row
	_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table")
	require.Nil(t, err)

	tableID := rel.GetTableID(ctxWithTimeout)

	err = rel.Write(ctxWithTimeout, containers.ToCNBatch(bats[0]))
	require.NoError(t, err)

	require.NoError(t, txn.Commit(ctxWithTimeout))

	// init cdc executor
	checkLeaseStub := gostub.Stub(
		&iscp.CheckLeaseWithRetry,
		func(
			context.Context,
			string,
			engine.Engine,
			client.TxnClient,
		) (bool, error) {
			return true, nil
		},
	)
	defer checkLeaseStub.Reset()
	cdcExecutor, err := iscp.NewISCPTaskExecutor(
		ctxWithTimeout,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		"",
		&iscp.ISCPExecutorOption{
			GCInterval:             time.Hour,
			GCTTL:                  time.Hour,
			SyncTaskInterval:       time.Millisecond * 100,
			FlushWatermarkInterval: time.Millisecond * 100,
			RetryTimes:             1,
		},
		common.DebugAllocator,
	)
	require.NoError(t, err)
	cdcExecutor.SetRpcHandleFn(taeHandler.GetRPCHandle().HandleGetChangedTableList)

	require.NoError(t, cdcExecutor.Start())
	defer cdcExecutor.Stop()

	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
	require.NoError(t, err)
	ok, err := iscp.RegisterJob(
		ctx, "", txn,
		&iscp.JobSpec{
			ConsumerInfo: iscp.ConsumerInfo{
				ConsumerType: int8(iscp.ConsumerType_CNConsumer),
			},
		},
		&iscp.JobID{
			JobName:   "hnsw_idx",
			DBName:    "srcdb",
			TableName: "src_table",
		},
		false,
	)
	require.True(t, ok)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctxWithTimeout))

	const jobName = "hnsw_idx"
	target := types.TimestampToTS(txn.Txn().CommitTS)
	waitForISCPWatermark(
		t,
		func() (types.TS, bool) {
			return cdcExecutor.GetWatermark(accountId, tableID, jobName)
		},
		target,
		10*time.Second,
		10*time.Millisecond,
		accountId,
		tableID,
		jobName,
	)
	t.Logf("watermark reached %v", target.ToString())

	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
	require.NoError(t, err)
	ok, err = iscp.UnregisterJob(
		ctx,
		"",
		txn,
		&iscp.JobID{
			JobName:   "hnsw_idx",
			DBName:    "srcdb",
			TableName: "src_table",
		},
	)
	require.True(t, ok)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctxWithTimeout))

	waitForISCPWatermarkAbsent(
		t,
		func() (types.TS, bool) {
			return cdcExecutor.GetWatermark(accountId, tableID, jobName)
		},
		10*time.Second,
		10*time.Millisecond,
		accountId,
		tableID,
		jobName,
	)
	cdcExecutor.GCInMemoryJob(0)
}

func TestIteration(t *testing.T) {
	catalog.SetupDefines("")

	// idAllocator := common.NewIdAllocator(1000)

	var (
		accountId = catalog.System_Account
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	err := mock_mo_indexes(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_foreign_keys(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_intra_system_change_propagation_log(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))

	checkLeaseStub := gostub.Stub(
		&iscp.CheckLeaseWithRetry,
		func(
			context.Context,
			string,
			engine.Engine,
			client.TxnClient,
		) (bool, error) {
			return true, nil
		},
	)
	defer checkLeaseStub.Reset()
	cdcExecutor, err := iscp.NewISCPTaskExecutor(
		ctxWithTimeout,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		"",
		&iscp.ISCPExecutorOption{
			SyncTaskInterval:       time.Millisecond * 10,
			FlushWatermarkInterval: time.Hour,
			GCTTL:                  time.Hour,
			GCInterval:             time.Hour,
			RetryTimes:             1,
		},
		common.DebugAllocator,
	)
	require.NoError(t, err)
	cdcExecutor.SetRpcHandleFn(taeHandler.GetRPCHandle().HandleGetChangedTableList)

	require.NoError(t, cdcExecutor.Start())
	defer cdcExecutor.Stop()

	fault.Enable()
	defer fault.Disable()
	// create database and table

	bat1 := CreateDBAndTableForCNConsumerAndGetAppendData(t, disttaeEngine, ctxWithTimeout, "srcdb", "src_table1", 10)
	defer bat1.Close()

	bat2 := CreateDBAndTableForCNConsumerAndGetAppendData(t, disttaeEngine, ctxWithTimeout, "srcdb", "src_table2", 10)
	defer bat2.Close()

	rmFn, err := objectio.InjectCDCExecutor("iteration:src_table1")
	require.NoError(t, err)
	defer rmFn()

	registerFn := func(tableName string) {
		txn, err := disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
		require.NoError(t, err)
		ok, err := iscp.RegisterJob(
			ctx, "", txn,
			&iscp.JobSpec{
				ConsumerInfo: iscp.ConsumerInfo{
					ConsumerType: int8(iscp.ConsumerType_CNConsumer),
				},
			},
			&iscp.JobID{
				JobName:   "job1",
				DBName:    "srcdb",
				TableName: tableName,
			},
			false,
		)
		require.True(t, ok)
		require.NoError(t, err)
		require.NoError(t, txn.Commit(ctxWithTimeout))
	}

	registerFn("src_table1")
	registerFn("src_table2")

	_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table1")
	require.Nil(t, err)

	err = rel.Write(ctxWithTimeout, containers.ToCNBatch(bat1))
	require.NoError(t, err)

	require.NoError(t, txn.Commit(ctxWithTimeout))

	_, rel, txn, err = disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table2")
	require.Nil(t, err)

	err = rel.Write(ctxWithTimeout, containers.ToCNBatch(bat2))
	require.NoError(t, err)

	tableID2 := rel.GetTableID(ctxWithTimeout)

	require.NoError(t, txn.Commit(ctxWithTimeout))

	const jobName = "job1"
	waitForISCPWatermark(
		t,
		func() (types.TS, bool) {
			return cdcExecutor.GetWatermark(accountId, tableID2, jobName)
		},
		types.TimestampToTS(txn.Txn().CommitTS),
		10*time.Second,
		10*time.Millisecond,
		accountId,
		tableID2,
		jobName,
	)
}

func TestDropJobsByDBName(t *testing.T) {
	catalog.SetupDefines("")

	// idAllocator := common.NewIdAllocator(1000)

	var (
		accountId = catalog.System_Account
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	err := mock_mo_indexes(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_foreign_keys(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_intra_system_change_propagation_log(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))

	bat := CreateDBAndTableForCNConsumerAndGetAppendData(t, disttaeEngine, ctxWithTimeout, "srcdb", "src_table", 10)
	defer bat.Close()

	bat2 := CreateDBAndTableForCNConsumerAndGetAppendData(t, disttaeEngine, ctxWithTimeout, "srcdb", "src_table2", 10)
	defer bat2.Close()

	_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table")
	require.Nil(t, err)

	tableID := rel.GetTableID(ctxWithTimeout)

	require.NoError(t, txn.Commit(ctxWithTimeout))

	_, rel, txn, err = disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table2")
	require.Nil(t, err)

	tableID2 := rel.GetTableID(ctxWithTimeout)

	require.NoError(t, txn.Commit(ctxWithTimeout))

	checkLeaseStub := gostub.Stub(
		&iscp.CheckLeaseWithRetry,
		func(
			context.Context,
			string,
			engine.Engine,
			client.TxnClient,
		) (bool, error) {
			return true, nil
		},
	)
	defer checkLeaseStub.Reset()
	cdcExecutor, err := iscp.NewISCPTaskExecutor(
		ctxWithTimeout,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		"",
		&iscp.ISCPExecutorOption{
			GCInterval:             time.Hour,
			GCTTL:                  time.Hour,
			SyncTaskInterval:       time.Millisecond * 100,
			FlushWatermarkInterval: time.Hour,
			RetryTimes:             1,
		},
		common.DebugAllocator,
	)
	require.NoError(t, err)
	cdcExecutor.SetRpcHandleFn(taeHandler.GetRPCHandle().HandleGetChangedTableList)

	require.NoError(t, cdcExecutor.Start())
	defer cdcExecutor.Stop()

	registerFn := func(tableName string, jobName string) types.TS {
		txn, err := disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
		require.NoError(t, err)
		ok, err := iscp.RegisterJob(
			ctx, "", txn,
			&iscp.JobSpec{
				ConsumerInfo: iscp.ConsumerInfo{
					ConsumerType: int8(iscp.ConsumerType_CNConsumer),
				},
			},
			&iscp.JobID{
				JobName:   jobName,
				DBName:    "srcdb",
				TableName: tableName,
			},
			false,
		)
		require.True(t, ok)
		require.NoError(t, err)
		require.NoError(t, txn.Commit(ctxWithTimeout))
		return types.TimestampToTS(txn.Txn().CommitTS)
	}

	target1 := registerFn("src_table", "job1")
	target2 := registerFn("src_table2", "job2")
	// Shared CI runners can pause the embedded TN/logtail path for longer than
	// the normal ten-second propagation window (a recent run spent about ten
	// seconds in one commit). Keep the assertion bounded, but leave enough
	// budget for the watermark to catch up under scheduler pressure.
	const watermarkWait = 30 * time.Second
	waitForISCPWatermark(
		t,
		func() (types.TS, bool) {
			return cdcExecutor.GetWatermark(accountId, tableID, "job1")
		},
		target1,
		watermarkWait,
		10*time.Millisecond,
		accountId,
		tableID,
		"job1",
	)
	waitForISCPWatermark(
		t,
		func() (types.TS, bool) {
			return cdcExecutor.GetWatermark(accountId, tableID2, "job2")
		},
		target2,
		watermarkWait,
		10*time.Millisecond,
		accountId,
		tableID2,
		"job2",
	)

	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
	require.NoError(t, err)
	err = iscp.UnregisterJobsByDBName(
		ctx, "", txn, "srcdb",
	)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctxWithTimeout))
	waitForISCPWatermarkAbsent(
		t,
		func() (types.TS, bool) {
			return cdcExecutor.GetWatermark(accountId, tableID, "job1")
		},
		watermarkWait,
		10*time.Millisecond,
		accountId,
		tableID,
		"job1",
	)
	waitForISCPWatermarkAbsent(
		t,
		func() (types.TS, bool) {
			return cdcExecutor.GetWatermark(accountId, tableID2, "job2")
		},
		watermarkWait,
		10*time.Millisecond,
		accountId,
		tableID2,
		"job2",
	)
}

func TestInvalidTimestamp(t *testing.T) {
	catalog.SetupDefines("")

	// idAllocator := common.NewIdAllocator(1000)

	var (
		accountId = catalog.System_Account
	)
	const jobName = "hnsw_idx"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	err := mock_mo_indexes(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_foreign_keys(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_intra_system_change_propagation_log(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	// init cdc executor
	checkLeaseStub := gostub.Stub(
		&iscp.CheckLeaseWithRetry,
		func(
			context.Context,
			string,
			engine.Engine,
			client.TxnClient,
		) (bool, error) {
			return true, nil
		},
	)
	defer checkLeaseStub.Reset()
	cdcExecutor, err := iscp.NewISCPTaskExecutor(
		ctxWithTimeout,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		"",
		&iscp.ISCPExecutorOption{
			GCInterval:             time.Hour,
			GCTTL:                  time.Hour,
			SyncTaskInterval:       time.Millisecond * 100,
			FlushWatermarkInterval: time.Millisecond * 100,
			RetryTimes:             1,
		},
		common.DebugAllocator,
	)
	require.NoError(t, err)
	cdcExecutor.SetRpcHandleFn(taeHandler.GetRPCHandle().HandleGetChangedTableList)

	err = cdcExecutor.Start()
	require.NoError(t, err)
	defer cdcExecutor.Stop()

	// create database and table

	bat := CreateDBAndTableForCNConsumerAndGetAppendData(t, disttaeEngine, ctxWithTimeout, "srcdb", "src_table", 10)
	bats := bat.Split(10)
	defer bat.Close()

	// append 1 row
	_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table")
	require.Nil(t, err)

	tableID := rel.GetTableID(ctxWithTimeout)

	err = rel.Write(ctxWithTimeout, containers.ToCNBatch(bats[0]))
	require.Nil(t, err)

	require.NoError(t, txn.Commit(ctxWithTimeout))

	// The recovery budget must not include cold consumer DDL. Prepare only
	// its empty destination; the executor still has to discover the job, copy
	// the source row and advance the watermark after the fault is removed.
	v, ok := moruntime.ServiceRuntime("").GetGlobalVariables(moruntime.InternalSQLExecutor)
	require.True(t, ok)
	sqlExecutor := v.(executor.SQLExecutor)
	for _, sql := range []string{
		fmt.Sprintf("create database if not exists %s", iscp.TargetDbName),
		fmt.Sprintf("create table %s.test_table_%d_%s like srcdb.src_table", iscp.TargetDbName, tableID, jobName),
	} {
		// No caller-owned transaction: Exec owns commit/rollback, including
		// SQL errors. Release any result before a fatal assertion exits.
		result, err := sqlExecutor.Exec(ctxWithTimeout, sql, executor.Options{})
		result.Close()
		require.NoError(t, err)
	}

	require.True(t, fault.Enable(), "fault injection was already enabled before TestInvalidTimestamp")
	t.Cleanup(func() {
		fault.Disable()
	})
	invalidTimestampFault := newISCPFaultBarrier(t, ctx, "invalid timestamp")
	defer invalidTimestampFault.Remove()

	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
	require.NoError(t, err)
	ok, err = iscp.RegisterJob(
		ctx, "", txn,
		&iscp.JobSpec{
			ConsumerInfo: iscp.ConsumerInfo{
				ConsumerType: int8(iscp.ConsumerType_CNConsumer),
			},
		},
		&iscp.JobID{
			JobName:   jobName,
			DBName:    "srcdb",
			TableName: "src_table",
		},
		false,
	)
	require.True(t, ok)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctxWithTimeout))
	target := types.TimestampToTS(txn.Txn().CommitTS)

	invalidTimestampFault.WaitUntilHit(time.Now().Add(30 * time.Second))
	_, found := cdcExecutor.GetWatermark(accountId, tableID, jobName)
	require.False(t, found)
	invalidTimestampFault.Remove()
	waitForISCPWatermark(
		t,
		func() (types.TS, bool) {
			return cdcExecutor.GetWatermark(accountId, tableID, jobName)
		},
		target,
		10*time.Second,
		10*time.Millisecond,
		accountId,
		tableID,
		jobName,
	)
	// Compare both directions: watermark progress must represent copied data,
	// not just job discovery. Let the SQL executor own these transactions too.
	destination := fmt.Sprintf("%s.test_table_%d_%s", iscp.TargetDbName, tableID, jobName)
	for _, sql := range []string{
		fmt.Sprintf("select * from srcdb.src_table except select * from %s", destination),
		fmt.Sprintf("select * from %s except select * from srcdb.src_table", destination),
	} {
		result, err := sqlExecutor.Exec(ctxWithTimeout, sql, executor.Options{})
		rows := 0
		if err == nil {
			result.ReadRows(func(n int, _ []*vector.Vector) bool {
				rows += n
				return true
			})
		}
		result.Close()
		require.NoError(t, err)
		require.Zero(t, rows, sql)
	}
}

func TestCancelIteration1(t *testing.T) {
	catalog.SetupDefines("")

	// idAllocator := common.NewIdAllocator(1000)

	var (
		accountId = catalog.System_Account
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	cancelCh := make(chan struct{})

	stub := gostub.Stub(
		&iscp.GetJobSpecs,
		func(
			context.Context,
			string,
			client.TxnClient,
			engine.Engine,
			client.TxnOperator,
			uint32,
			uint64,
			[]string,
			[]uint64,
			types.TS,
			[]*iscp.JobStatus,
			[]uint64,
		) (jobSpec []*iscp.JobSpec, prevStatus []*iscp.JobStatus, err error) {
			cancelCh <- struct{}{}
			<-cancelCh
			return []*iscp.JobSpec{
					{
						ConsumerInfo: iscp.ConsumerInfo{
							ConsumerType: int8(iscp.ConsumerType_CNConsumer),
							SrcTable: iscp.TableInfo{
								DBName:    "srcdb",
								TableName: "src_table",
							},
						},
					},
				}, []*iscp.JobStatus{
					{
						Stage: iscp.JobStage_Running,
					},
				}, nil
		},
	)
	defer stub.Reset()

	err := mock_mo_indexes(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_foreign_keys(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_intra_system_change_propagation_log(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	bat := CreateDBAndTableForCNConsumerAndGetAppendData(t, disttaeEngine, ctxWithTimeout, "srcdb", "src_table", 10)
	defer bat.Close()

	_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table")
	require.Nil(t, err)

	tableID := rel.GetTableID(ctxWithTimeout)

	txn.Commit(ctxWithTimeout)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = iscp.ExecuteIteration(
			ctxWithTimeout,
			"",
			disttaeEngine.Engine,
			disttaeEngine.GetTxnClient(),
			iscp.NewIterationContext(accountId, tableID, []string{"job1"}, []uint64{1}, []uint64{1}, types.TS{}, types.TS{}),
			common.DebugAllocator,
		)
		assert.Error(t, err)
	}()
	<-cancelCh
	cancel()
	close(cancelCh)
	wg.Wait()
}

func TestCancelIteration2(t *testing.T) {
	catalog.SetupDefines("")

	// idAllocator := common.NewIdAllocator(1000)

	var (
		accountId = catalog.System_Account
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	cancelCh := make(chan struct{})

	checkLeaseStub := gostub.Stub(
		&iscp.CheckLeaseWithRetry,
		func(
			context.Context,
			string,
			engine.Engine,
			client.TxnClient,
		) (bool, error) {
			return true, nil
		},
	)
	defer checkLeaseStub.Reset()
	stub := gostub.Stub(
		&iscp.GetJobSpecs,
		func(
			context.Context,
			string,
			client.TxnClient,
			engine.Engine,
			client.TxnOperator,
			uint32,
			uint64,
			[]string,
			[]uint64,
			types.TS,
			[]*iscp.JobStatus,
			[]uint64,
		) (jobSpec []*iscp.JobSpec, prevStatus []*iscp.JobStatus, err error) {
			return []*iscp.JobSpec{
					{
						ConsumerInfo: iscp.ConsumerInfo{
							ConsumerType: int8(iscp.ConsumerType_CNConsumer),
							SrcTable: iscp.TableInfo{
								DBName:    "srcdb",
								TableName: "src_table",
							},
						},
					},
				}, []*iscp.JobStatus{
					{
						Stage: iscp.JobStage_Running,
					},
				}, nil
		},
	)
	defer stub.Reset()

	var flushCount int
	stub2 := gostub.Stub(
		&iscp.FlushJobStatusOnIterationState,
		func(
			context.Context,
			string,
			engine.Engine,
			client.TxnClient,
			uint32,
			uint64,
			[]string,
			[]uint64,
			[]uint64,
			[]*iscp.JobStatus,
			types.TS,
			int8,
			[]uint64,
		) error {
			flushCount++
			if flushCount == 1 {
				cancelCh <- struct{}{}
				<-cancelCh
				return nil
			}
			return nil
		},
	)
	defer stub2.Reset()

	err := mock_mo_indexes(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_foreign_keys(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_intra_system_change_propagation_log(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	bat := CreateDBAndTableForCNConsumerAndGetAppendData(t, disttaeEngine, ctxWithTimeout, "srcdb", "src_table", 10)
	defer bat.Close()

	_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table")
	require.Nil(t, err)

	tableID := rel.GetTableID(ctxWithTimeout)

	txn.Commit(ctxWithTimeout)

	iterationErr := make(chan error, 1)
	go func() {
		iterationErr <- iscp.ExecuteIteration(
			ctxWithTimeout,
			"",
			disttaeEngine.Engine,
			disttaeEngine.GetTxnClient(),
			iscp.NewIterationContext(accountId, tableID, []string{"job1"}, []uint64{1}, []uint64{1}, types.TS{}, types.TS{}),
			common.DebugAllocator,
		)
	}()
	<-cancelCh
	cancel()
	close(cancelCh)
	require.ErrorIs(t, <-iterationErr, context.Canceled)

}

func TestStartFromNow(t *testing.T) {
	catalog.SetupDefines("")

	// idAllocator := common.NewIdAllocator(1000)

	var (
		accountId = catalog.System_Account
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	err := mock_mo_indexes(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_foreign_keys(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_intra_system_change_propagation_log(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))

	// create database and table

	bat := CreateDBAndTableForCNConsumerAndGetAppendData(t, disttaeEngine, ctxWithTimeout, "srcdb", "src_table", 10)
	bats := bat.Split(10)
	defer bat.Close()

	// append 1 row
	_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table")
	require.Nil(t, err)

	tableID := rel.GetTableID(ctxWithTimeout)

	err = rel.Write(ctxWithTimeout, containers.ToCNBatch(bats[0]))
	require.Nil(t, err)

	require.NoError(t, txn.Commit(ctxWithTimeout))

	// init cdc executor
	checkLeaseStub := gostub.Stub(
		&iscp.CheckLeaseWithRetry,
		func(
			context.Context,
			string,
			engine.Engine,
			client.TxnClient,
		) (bool, error) {
			return true, nil
		},
	)
	defer checkLeaseStub.Reset()
	cdcExecutor, err := iscp.NewISCPTaskExecutor(
		ctxWithTimeout,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		"",
		&iscp.ISCPExecutorOption{
			GCInterval:             time.Hour,
			GCTTL:                  time.Hour,
			SyncTaskInterval:       time.Millisecond * 100,
			FlushWatermarkInterval: time.Millisecond * 100,
			RetryTimes:             1,
		},
		common.DebugAllocator,
	)
	require.NoError(t, err)
	cdcExecutor.SetRpcHandleFn(taeHandler.GetRPCHandle().HandleGetChangedTableList)

	require.NoError(t, cdcExecutor.Start())
	defer cdcExecutor.Stop()

	// register cdc job
	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
	require.NoError(t, err)
	ok, err := iscp.RegisterJob(
		ctx, "", txn,
		&iscp.JobSpec{
			ConsumerInfo: iscp.ConsumerInfo{
				ConsumerType: int8(iscp.ConsumerType_CNConsumer),
			},
		},
		&iscp.JobID{
			JobName:   "hnsw_idx",
			DBName:    "srcdb",
			TableName: "src_table",
		},
		true,
	)
	require.True(t, ok)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctxWithTimeout))
	waitForISCPWatermark(
		t,
		func() (types.TS, bool) {
			return cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
		},
		types.TimestampToTS(txn.Txn().CommitTS),
		10*time.Second,
		10*time.Millisecond,
		accountId,
		tableID,
		"hnsw_idx",
	)
}

func TestApplyISCPLog(t *testing.T) {
	catalog.SetupDefines("")

	// idAllocator := common.NewIdAllocator(1000)

	var (
		accountId = catalog.System_Account
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	err := mock_mo_indexes(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_foreign_keys(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_intra_system_change_propagation_log(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	// create database and table

	bat := CreateDBAndTableForCNConsumerAndGetAppendData(t, disttaeEngine, ctxWithTimeout, "srcdb", "src_table", 10)
	bats := bat.Split(10)
	defer bat.Close()

	// append 1 row
	_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table")
	require.Nil(t, err)

	tableID := rel.GetTableID(ctxWithTimeout)

	err = rel.Write(ctxWithTimeout, containers.ToCNBatch(bats[0]))
	require.Nil(t, err)

	require.NoError(t, txn.Commit(ctxWithTimeout))

	// init cdc executor
	checkLeaseStub := gostub.Stub(
		&iscp.CheckLeaseWithRetry,
		func(
			context.Context,
			string,
			engine.Engine,
			client.TxnClient,
		) (bool, error) {
			return true, nil
		},
	)
	defer checkLeaseStub.Reset()
	cdcExecutor, err := iscp.NewISCPTaskExecutor(
		ctxWithTimeout,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		"",
		&iscp.ISCPExecutorOption{
			GCInterval:             time.Hour,
			GCTTL:                  time.Hour,
			SyncTaskInterval:       time.Millisecond * 100,
			FlushWatermarkInterval: time.Hour,
			RetryTimes:             1,
		},
		common.DebugAllocator,
	)
	require.NoError(t, err)
	cdcExecutor.SetRpcHandleFn(taeHandler.GetRPCHandle().HandleGetChangedTableList)

	require.NoError(t, cdcExecutor.Start())
	defer cdcExecutor.Stop()

	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
	require.NoError(t, err)
	ok, err := iscp.RegisterJob(
		ctx, "", txn,
		&iscp.JobSpec{
			ConsumerInfo: iscp.ConsumerInfo{
				ConsumerType: int8(iscp.ConsumerType_CNConsumer),
			},
		},
		&iscp.JobID{
			JobName:   "hnsw_idx",
			DBName:    "srcdb",
			TableName: "src_table",
		},
		true,
	)
	require.True(t, ok)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctxWithTimeout))
	waitForISCPWatermark(
		t,
		func() (types.TS, bool) {
			return cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
		},
		types.TimestampToTS(txn.Txn().CommitTS),
		10*time.Second,
		10*time.Millisecond,
		accountId,
		tableID,
		"hnsw_idx",
	)

	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
	require.NoError(t, err)
	ok, err = iscp.UnregisterJob(ctx, "", txn,
		&iscp.JobID{
			JobName:   "hnsw_idx",
			DBName:    "srcdb",
			TableName: "src_table",
		},
	)
	require.True(t, ok)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctxWithTimeout))
	waitForISCPWatermarkAbsent(
		t,
		func() (types.TS, bool) {
			return cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
		},
		10*time.Second,
		10*time.Millisecond,
		accountId,
		tableID,
		"hnsw_idx",
	)
}

func TestISCPResumeRecoversAcceptedIteration(t *testing.T) {
	catalog.SetupDefines("")
	accountID := catalog.System_Account

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountID)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	err := mock_mo_indexes(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_foreign_keys(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_intra_system_change_propagation_log(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)

	bat := CreateDBAndTableForCNConsumerAndGetAppendData(t, disttaeEngine, ctxWithTimeout, "srcdb", "src_table", 10)
	bats := bat.Split(10)
	defer bat.Close()

	_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table")
	require.NoError(t, err)
	tableID := rel.GetTableID(ctxWithTimeout)
	require.NoError(t, rel.Write(ctxWithTimeout, containers.ToCNBatch(bats[0])))
	require.NoError(t, txn.Commit(ctxWithTimeout))
	minimumRecoveredWatermark := taeHandler.GetDB().TxnMgr.Now()

	// Fail after admission but before FlushJobStatusOnIterationState persists
	// Running. This creates the original divergence deterministically: memory is
	// Pending at LSN 1 while storage is still Completed at LSN 0.
	fault.Enable()
	defer fault.Disable()
	rmFault, err := objectio.InjectCDCExecutor("iteration:src_table")
	require.NoError(t, err)
	faultRemoved := false
	defer func() {
		if !faultRemoved {
			_, _ = rmFault()
		}
	}()

	checkLeaseStub := gostub.Stub(
		&iscp.CheckLeaseWithRetry,
		func(
			context.Context,
			string,
			engine.Engine,
			client.TxnClient,
		) (bool, error) {
			return true, nil
		},
	)
	defer checkLeaseStub.Reset()
	cdcExecutor, err := iscp.NewISCPTaskExecutor(
		ctxWithTimeout,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		"",
		&iscp.ISCPExecutorOption{
			GCInterval:             time.Hour,
			GCTTL:                  time.Hour,
			SyncTaskInterval:       time.Millisecond * 100,
			FlushWatermarkInterval: time.Hour,
			RetryTimes:             1,
		},
		common.DebugAllocator,
	)
	require.NoError(t, err)
	cdcExecutor.SetRpcHandleFn(taeHandler.GetRPCHandle().HandleGetChangedTableList)

	require.NoError(t, cdcExecutor.Start())
	defer cdcExecutor.Stop()

	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
	require.NoError(t, err)
	ok, err := iscp.RegisterJob(
		ctx,
		"",
		txn,
		&iscp.JobSpec{ConsumerInfo: iscp.ConsumerInfo{ConsumerType: int8(iscp.ConsumerType_CNConsumer)}},
		&iscp.JobID{JobName: "replay_job", DBName: "srcdb", TableName: "src_table"},
		false,
	)
	require.True(t, ok)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctxWithTimeout))

	require.Eventually(t, func() bool {
		lsn, state, found := cdcExecutor.GetJobState(accountID, tableID, "replay_job")
		return found && lsn == 1 && state == iscp.ISCPJobState_Pending
	}, 4*time.Second, 10*time.Millisecond)

	readPersistedState := func() (int8, uint64) {
		readTxn, readErr := disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
		require.NoError(t, readErr)
		result, readErr := iscp.ExecWithResult(
			ctxWithTimeout,
			fmt.Sprintf(
				"SELECT job_state, job_status FROM `mo_catalog`.`mo_iscp_log` WHERE account_id = %d AND table_id = %d AND job_name = 'replay_job'",
				accountID,
				tableID,
			),
			"",
			readTxn,
		)
		require.NoError(t, readErr)
		defer result.Close()

		var state int8
		var lsn uint64
		result.ReadRows(func(rows int, cols []*vector.Vector) bool {
			require.Equal(t, 1, rows)
			state = vector.MustFixedColWithTypeCheck[int8](cols[0])[0]
			status, statusErr := iscp.UnmarshalJobStatus(cols[1].GetBytesAt(0))
			require.NoError(t, statusErr)
			lsn = status.LSN
			return true
		})
		require.NoError(t, readTxn.Commit(ctxWithTimeout))
		return state, lsn
	}

	persistedState, persistedLSN := readPersistedState()
	require.Equal(t, iscp.ISCPJobState_Completed, persistedState)
	require.Zero(t, persistedLSN)

	cdcExecutor.Stop()
	_, err = rmFault()
	require.NoError(t, err)
	faultRemoved = true
	require.NoError(t, cdcExecutor.Resume())

	require.Eventually(t, func() bool {
		lsn, state, found := cdcExecutor.GetJobState(accountID, tableID, "replay_job")
		watermark, watermarkFound := cdcExecutor.GetWatermark(accountID, tableID, "replay_job")
		return found && watermarkFound &&
			lsn == 1 && state == iscp.ISCPJobState_Completed &&
			watermark.GE(&minimumRecoveredWatermark)
	}, 10*time.Second, 10*time.Millisecond)

	persistedState, persistedLSN = readPersistedState()
	require.Equal(t, iscp.ISCPJobState_Completed, persistedState)
	require.Equal(t, uint64(1), persistedLSN)
}

func TestRenameSrcTable(t *testing.T) {
	catalog.SetupDefines("")

	// idAllocator := common.NewIdAllocator(1000)

	var (
		accountId = catalog.System_Account
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	err := mock_mo_indexes(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_foreign_keys(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_intra_system_change_propagation_log(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))

	// create database and table

	bats := CreateDBAndTableForCNConsumerAndGetAppendData(t, disttaeEngine, ctxWithTimeout, "srcdb", "src_table", 10)
	defer bats.Close()

	_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table")
	require.Nil(t, err)

	tableID := rel.GetTableID(ctxWithTimeout)

	require.NoError(t, txn.Commit(ctxWithTimeout))

	// init cdc executor
	checkLeaseStub := gostub.Stub(
		&iscp.CheckLeaseWithRetry,
		func(
			context.Context,
			string,
			engine.Engine,
			client.TxnClient,
		) (bool, error) {
			return true, nil
		},
	)
	defer checkLeaseStub.Reset()
	opts := &iscp.ISCPExecutorOption{
		GCInterval:             time.Hour,
		GCTTL:                  time.Hour,
		SyncTaskInterval:       time.Millisecond * 100,
		FlushWatermarkInterval: time.Hour,
		RetryTimes:             1,
	}
	opts.GCTTL = time.Hour
	cdcExecutor, err := iscp.NewISCPTaskExecutor(
		ctxWithTimeout,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		"",
		opts,
		common.DebugAllocator,
	)
	require.NoError(t, err)
	cdcExecutor.SetRpcHandleFn(taeHandler.GetRPCHandle().HandleGetChangedTableList)

	require.NoError(t, cdcExecutor.Start())
	defer cdcExecutor.Stop()
	registerFn := func(indexName string) types.TS {
		txn, err := disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
		require.NoError(t, err)
		ok, err := iscp.RegisterJob(
			ctx, "", txn,
			&iscp.JobSpec{
				ConsumerInfo: iscp.ConsumerInfo{
					ConsumerType: int8(iscp.ConsumerType_CNConsumer),
				},
			},
			&iscp.JobID{
				JobName:   indexName,
				DBName:    "srcdb",
				TableName: "src_table",
			},
			false,
		)
		require.True(t, ok)
		require.NoError(t, err)
		require.NoError(t, txn.Commit(ctxWithTimeout))
		return types.TimestampToTS(txn.Txn().CommitTS)
	}
	var target types.TS
	for i := 0; i < 10; i++ {
		target = registerFn(fmt.Sprintf("hnsw_idx_%d", i))
	}

	require.Eventually(
		t,
		func() bool {
			for i := 0; i < 10; i++ {
				ts, ok := cdcExecutor.GetWatermark(accountId, tableID, fmt.Sprintf("hnsw_idx_%d", i))
				if !ok || !ts.GE(&target) {
					return false
				}
			}
			return true
		},
		30*time.Second,
		10*time.Millisecond,
	)
	for i := 0; i < 10; i++ {
		ts, ok := cdcExecutor.GetWatermark(accountId, tableID, fmt.Sprintf("hnsw_idx_%d", i))
		require.True(t, ok)
		require.True(t, ts.GE(&target))
	}
	cdcExecutor.Stop()

	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
	require.NoError(t, err)
	iscp.RenameSrcTable(
		ctxWithTimeout,
		"",
		txn,
		rel.GetDBID(ctxWithTimeout),
		tableID,
		"src_table",
		"src_table_new",
	)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctxWithTimeout))
	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
	require.NoError(t, err)
	selectJobSql := fmt.Sprintf("SELECT job_spec FROM `mo_catalog`.`mo_iscp_log` WHERE account_id = %d AND table_id = %d", accountId, tableID)
	execResult, err := iscp.ExecWithResult(ctxWithTimeout, selectJobSql, "", txn)
	require.NoError(t, err)
	defer execResult.Close()
	execResult.ReadRows(func(rows int, cols []*vector.Vector) bool {
		require.Equal(t, 10, rows)
		for i := 0; i < rows; i++ {
			jobSpec, err := iscp.UnmarshalJobSpec(cols[0].GetBytesAt(i))
			require.NoError(t, err)
			require.Equal(t, "src_table_new", jobSpec.SrcTable.TableName)
			require.Equal(t, "src_table_new", jobSpec.TableName)
		}
		return true
	})
	require.NoError(t, txn.Commit(ctxWithTimeout))
}

func TestISCPExecutorStartError(t *testing.T) {

	catalog.SetupDefines("")

	// idAllocator := common.NewIdAllocator(1000)

	var (
		accountId = catalog.System_Account
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	err := mock_mo_indexes(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_foreign_keys(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_intra_system_change_propagation_log(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))

	// init cdc executor
	checkLeaseStub := gostub.Stub(
		&iscp.CheckLeaseWithRetry,
		func(
			context.Context,
			string,
			engine.Engine,
			client.TxnClient,
		) (bool, error) {
			return true, nil
		},
	)
	defer checkLeaseStub.Reset()
	cdcExecutor, err := iscp.NewISCPTaskExecutor(
		ctxWithTimeout,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		"",
		&iscp.ISCPExecutorOption{
			GCInterval:             time.Hour,
			GCTTL:                  time.Hour,
			SyncTaskInterval:       time.Millisecond * 100,
			FlushWatermarkInterval: time.Millisecond * 100,
			RetryTimes:             1,
		},
		common.DebugAllocator,
	)
	require.NoError(t, err)
	cdcExecutor.SetRpcHandleFn(taeHandler.GetRPCHandle().HandleGetChangedTableList)

	err = cdcExecutor.Start()
	require.NoError(t, err)
	defer cdcExecutor.Stop()

	err = cdcExecutor.Restart()
	require.NoError(t, err)

	cdcExecutor.Stop()

	fault.Enable()
	defer fault.Disable()
	rmFn, err := objectio.InjectCDCExecutor("replay")
	assert.NoError(t, err)
	err = cdcExecutor.Resume()
	require.Error(t, err)
	require.False(t, cdcExecutor.IsRunning())
	rmFn()
	err = cdcExecutor.Resume()
	require.NoError(t, err)
	require.True(t, cdcExecutor.IsRunning())
}

func TestStaleRead(t *testing.T) {

	catalog.SetupDefines("")

	// idAllocator := common.NewIdAllocator(1000)

	var (
		accountId = catalog.System_Account
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	err := mock_mo_indexes(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_foreign_keys(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_intra_system_change_propagation_log(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	// init cdc executor
	checkLeaseStub := gostub.Stub(
		&iscp.CheckLeaseWithRetry,
		func(
			context.Context,
			string,
			engine.Engine,
			client.TxnClient,
		) (bool, error) {
			return true, nil
		},
	)
	defer checkLeaseStub.Reset()
	cdcExecutor, err := iscp.NewISCPTaskExecutor(
		ctxWithTimeout,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		"",
		&iscp.ISCPExecutorOption{
			GCInterval:             time.Hour,
			GCTTL:                  time.Hour,
			SyncTaskInterval:       time.Millisecond * 100,
			FlushWatermarkInterval: time.Millisecond * 100,
			RetryTimes:             1,
		},
		common.DebugAllocator,
	)
	require.NoError(t, err)
	cdcExecutor.SetRpcHandleFn(taeHandler.GetRPCHandle().HandleGetChangedTableList)

	err = cdcExecutor.Start()
	require.NoError(t, err)
	defer cdcExecutor.Stop()

	// create database and table

	bat := CreateDBAndTableForCNConsumerAndGetAppendData(t, disttaeEngine, ctxWithTimeout, "srcdb", "src_table", 10)
	bats := bat.Split(10)
	defer bat.Close()

	// append 1 row
	_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table")
	require.Nil(t, err)

	tableID := rel.GetTableID(ctxWithTimeout)

	err = rel.Write(ctxWithTimeout, containers.ToCNBatch(bats[0]))
	require.NoError(t, err)

	require.NoError(t, txn.Commit(ctxWithTimeout))

	fault.Enable()
	defer fault.Disable()
	rmFn, err := objectio.InjectCDCExecutor("stale read")
	require.NoError(t, err)
	defer rmFn()

	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
	require.NoError(t, err)
	ok, err := iscp.RegisterJob(
		ctx, "", txn,
		&iscp.JobSpec{
			ConsumerInfo: iscp.ConsumerInfo{
				ConsumerType: int8(iscp.ConsumerType_CNConsumer),
			},
		},
		&iscp.JobID{
			JobName:   "hnsw_idx",
			DBName:    "srcdb",
			TableName: "src_table",
		},
		false,
	)
	require.True(t, ok)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctxWithTimeout))

	const jobName = "hnsw_idx"
	waitForISCPWatermark(
		t,
		func() (types.TS, bool) {
			return cdcExecutor.GetWatermark(accountId, tableID, jobName)
		},
		types.TimestampToTS(txn.Txn().CommitTS),
		10*time.Second,
		10*time.Millisecond,
		accountId,
		tableID,
		jobName,
	)
}

func TestInitSql(t *testing.T) {
	catalog.SetupDefines("")

	// idAllocator := common.NewIdAllocator(1000)

	var (
		accountId      = catalog.System_Account
		tableAccountID = uint32(1)
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	err := mock_mo_indexes(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_foreign_keys(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_intra_system_change_propagation_log(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	ctxWithAccount := context.WithValue(ctxWithTimeout, defines.TenantIDKey{}, tableAccountID)
	err = mock_mo_indexes(disttaeEngine, ctxWithAccount)
	require.NoError(t, err)
	err = mock_mo_foreign_keys(disttaeEngine, ctxWithAccount)
	require.NoError(t, err)
	bats := CreateDBAndTableForCNConsumerAndGetAppendData(t, disttaeEngine, ctxWithAccount, "srcdb", "src_table", 10)
	defer bats.Close()

	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	_, rel, txn, err := disttaeEngine.GetTable(ctxWithAccount, "srcdb", "src_table")
	require.Nil(t, err)

	tableID := rel.GetTableID(ctxWithTimeout)

	require.NoError(t, txn.Commit(ctxWithAccount))

	// init cdc executor
	opts := &iscp.ISCPExecutorOption{
		GCInterval:             time.Hour,
		GCTTL:                  time.Hour,
		SyncTaskInterval:       time.Millisecond * 100,
		FlushWatermarkInterval: time.Hour,
		RetryTimes:             1,
	}
	opts.GCTTL = time.Hour
	checkLeaseStub := gostub.Stub(
		&iscp.CheckLeaseWithRetry,
		func(
			context.Context,
			string,
			engine.Engine,
			client.TxnClient,
		) (bool, error) {
			return true, nil
		},
	)
	defer checkLeaseStub.Reset()
	cdcExecutor, err := iscp.NewISCPTaskExecutor(
		ctxWithTimeout,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		"",
		opts,
		common.DebugAllocator,
	)
	require.NoError(t, err)
	cdcExecutor.SetRpcHandleFn(taeHandler.GetRPCHandle().HandleGetChangedTableList)

	require.NoError(t, cdcExecutor.Start())
	defer cdcExecutor.Stop()

	const jobName = "idx"
	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
	require.NoError(t, err)
	ok, err := iscp.RegisterJob(
		ctxWithAccount, "", txn,
		&iscp.JobSpec{
			ConsumerInfo: iscp.ConsumerInfo{
				ConsumerType: int8(iscp.ConsumerType_CNConsumer),
				InitSQL:      "create database t;",
			},
		},
		&iscp.JobID{
			JobName:   jobName,
			DBName:    "srcdb",
			TableName: "src_table",
		},
		false,
	)
	require.True(t, ok)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctxWithTimeout))

	target := types.TimestampToTS(txn.Txn().CommitTS)
	waitForISCPWatermark(
		t,
		func() (types.TS, bool) {
			return cdcExecutor.GetWatermark(tableAccountID, tableID, jobName)
		},
		target,
		10*time.Second,
		10*time.Millisecond,
		tableAccountID,
		tableID,
		jobName,
	)

	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Now())
	require.NoError(t, err)

	_, err = disttaeEngine.Engine.Database(ctxWithAccount, "t", txn)
	require.NoError(t, err)
	err = txn.Commit(ctxWithTimeout)
	require.NoError(t, err)

	txn2, rel2 := testutil2.GetRelation(t, tableAccountID, taeHandler.GetDB(), "srcdb", "src_table")
	require.NoError(t, rel2.Append(ctx, bats))
	require.NoError(t, txn2.Commit(ctx))

	target = txn2.GetCommitTS()
	waitForISCPWatermark(
		t,
		func() (types.TS, bool) {
			return cdcExecutor.GetWatermark(tableAccountID, tableID, jobName)
		},
		target,
		10*time.Second,
		10*time.Millisecond,
		tableAccountID,
		tableID,
		jobName,
	)

	CheckTableData(t, disttaeEngine, ctxWithAccount, "srcdb", "src_table", tableID, jobName)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
}

func TestCheckLeaseFailed(t *testing.T) {

	catalog.SetupDefines("")

	// idAllocator := common.NewIdAllocator(1000)

	var (
		accountId = catalog.System_Account
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	err := mock_mo_indexes(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_foreign_keys(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_intra_system_change_propagation_log(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	// init cdc executor

	leaseRejected := make(chan struct{}, 1)
	checkLeaseStub := gostub.Stub(
		&iscp.CheckLeaseWithRetry,
		func(
			context.Context,
			string,
			engine.Engine,
			client.TxnClient,
		) (bool, error) {
			if msg, injected := objectio.ISCPExecutorInjected(); injected && msg == "check lease" {
				select {
				case leaseRejected <- struct{}{}:
				default:
				}
				return false, nil
			}
			return true, nil
		},
	)
	defer checkLeaseStub.Reset()
	cdcExecutor, err := iscp.NewISCPTaskExecutor(
		ctxWithTimeout,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		"",
		&iscp.ISCPExecutorOption{
			GCInterval:             time.Hour,
			GCTTL:                  time.Hour,
			SyncTaskInterval:       time.Millisecond * 100,
			FlushWatermarkInterval: time.Millisecond * 100,
			RetryTimes:             1,
		},
		common.DebugAllocator,
	)
	require.NoError(t, err)
	cdcExecutor.SetRpcHandleFn(taeHandler.GetRPCHandle().HandleGetChangedTableList)

	err = cdcExecutor.Start()
	require.NoError(t, err)
	t.Cleanup(cdcExecutor.Stop)

	bat := CreateDBAndTableForCNConsumerAndGetAppendData(t, disttaeEngine, ctxWithTimeout, "srcdb", "src_table", 10)
	bats := bat.Split(10)
	defer bat.Close()

	// append 1 row
	_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table")
	require.Nil(t, err)

	tableID := rel.GetTableID(ctxWithTimeout)

	err = rel.Write(ctxWithTimeout, containers.ToCNBatch(bats[0]))
	require.Nil(t, err)

	require.NoError(t, txn.Commit(ctxWithTimeout))

	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
	require.NoError(t, err)
	ok, err := iscp.RegisterJob(
		ctx, "", txn,
		&iscp.JobSpec{
			ConsumerInfo: iscp.ConsumerInfo{
				ConsumerType: int8(iscp.ConsumerType_CNConsumer),
			},
		},
		&iscp.JobID{
			JobName:   "hnsw_idx",
			DBName:    "srcdb",
			TableName: "src_table",
		},
		false,
	)
	require.True(t, ok)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctxWithTimeout))
	waitForISCPWatermark(
		t,
		func() (types.TS, bool) {
			return cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
		},
		types.TimestampToTS(txn.Txn().CommitTS),
		10*time.Second,
		10*time.Millisecond,
		accountId,
		tableID,
		"hnsw_idx",
	)

	require.True(t, fault.Enable(), "fault injection was already enabled before TestCheckLeaseFailed")
	t.Cleanup(func() {
		fault.Disable()
	})
	rmFn, err := objectio.InjectCDCExecutor("check lease")
	require.NoError(t, err)
	t.Cleanup(func() {
		removed, removeErr := rmFn()
		require.NoError(t, removeErr)
		require.True(t, removed)
	})

	_, rel, txn, err = disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table")
	require.Nil(t, err)

	err = rel.Write(ctxWithTimeout, containers.ToCNBatch(bats[1]))
	require.Nil(t, err)

	require.NoError(t, txn.Commit(ctxWithTimeout))

	select {
	case <-leaseRejected:
	case <-time.After(10 * time.Second):
		t.Fatal("ISCP executor did not run the rejected lease check")
	}
	require.Eventually(
		t,
		func() bool { return !cdcExecutor.IsRunning() },
		10*time.Second,
		10*time.Millisecond,
		"ISCP executor did not stop after lease rejection",
	)
}

func TestPartitionChangesHandleStaleRead(t *testing.T) {
	/*
		This test verifies that when nextFrom is not in the checkpoint entry range (minTS, maxTS),
		it returns a stale read error. This tests the logic in change_handle.go:223-226:
		if nextFrom.LT(&minTS) || nextFrom.GT(&maxTS) {
			logutil.Infof("ChangesHandle-Split nextFrom is not in the checkpoint entry range: %s-%s", minTS.ToString(), maxTS.ToString())
			return false, moerr.NewErrStaleReadNoCtx(minTS.ToString(), nextFrom.ToString())
		}
	*/

	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		tableName    = "test_stale_read"
		databaseName = "db_stale_read"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	schema := catalog2.MockSchemaAll(20, 0)
	schema.Name = tableName
	bat := catalog2.MockBatch(schema, 3)
	defer bat.Close()
	bats := bat.Split(3)

	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	// Create database and table
	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	// Insert first batch and commit
	txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	require.Nil(t, rel.Append(ctx, bats[0]))
	require.Nil(t, txn.Commit(ctx))
	t1 := txn.GetCommitTS()

	txn, rel = testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	id := rel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	require.Nil(t, txn.Commit(ctx))

	// Force checkpoint to create a checkpoint entry
	now := taeHandler.GetDB().TxnMgr.Now()
	taeHandler.GetDB().ForceCheckpoint(ctx, now)
	now = taeHandler.GetDB().TxnMgr.Now()
	taeHandler.GetDB().ForceCheckpoint(ctx, now)

	// Insert second batch
	txn, rel = testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	require.Nil(t, rel.Append(ctx, bats[1]))
	require.Nil(t, txn.Commit(ctx))
	t2 := txn.GetCommitTS()

	// Force another checkpoint
	now = taeHandler.GetDB().TxnMgr.Now()
	taeHandler.GetDB().ForceCheckpoint(ctx, now)

	// Insert third batch
	txn, rel = testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	require.Nil(t, rel.Append(ctx, bats[2]))
	require.Nil(t, txn.Commit(ctx))
	t3 := txn.GetCommitTS()

	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, databaseName, tableName, false)
	require.Nil(t, err)

	mp := common.DebugAllocator

	// Force GC to clean up old partition state, making t1 fall outside the current partition state range
	// This will force the code to use RequestSnapshotRead
	disttaeEngine.Engine.ForceGC(ctx, t2.Next())

	// Setup stub for RequestSnapshotRead to return a checkpoint entry with a range that doesn't include t1
	// The key is to return checkpoint entries where minTS > t1, which will trigger the stale read error
	ssStub := gostub.Stub(
		&disttae.RequestSnapshotRead,
		disttae.GetSnapshotReadFnWithHandler(
			func(ctx context.Context, meta pbtxn.TxnMeta, req *cmd_util.SnapshotReadReq, resp *cmd_util.SnapshotReadResp) (func(), error) {
				// Create a fake checkpoint entry with time range [t2, t3]
				// When we try to read from t1 (which is < t2), it will be less than minTS
				// This will trigger the stale read error at line 223-226
				t2Timestamp := t2.ToTimestamp()
				t3Timestamp := t3.ToTimestamp()

				resp.Succeed = true
				resp.Entries = []*cmd_util.CheckpointEntryResp{
					{
						Start:     &t2Timestamp,
						End:       &t3Timestamp,
						Location1: []byte("fake_location1"),
						Location2: []byte("fake_location2"),
						EntryType: int32(checkpoint.ET_Incremental),
						Version:   1,
					},
				}
				return func() {}, nil
			},
		),
	)
	defer ssStub.Reset()

	// Try to collect changes from t1 (which is now before the available checkpoint range)
	// This should trigger the stale read error at line 223-226
	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		// Try to collect changes starting from a time that's been GC'd
		// Since t1.Prev() < t2 (minTS of checkpoint entry), this should trigger the stale read error
		handle, err := rel.CollectChanges(ctx, t1.Prev(), t3.Next(), true, mp)

		// We expect either:
		// 1. An error during CollectChanges (stale read error)
		// 2. Or an error when iterating through Next()
		if err != nil {
			// Check if it's a stale read error
			t.Logf("Got expected error during CollectChanges: %v", err)
			assert.True(t, moerr.IsMoErrCode(err, moerr.ErrStaleRead), "Expected stale read error, got: %v", err)
		} else {
			// Try to get data, should fail with stale read error
			gotError := false
			for {
				data, tombstone, _, err := handle.Next(ctx, mp)
				if err != nil {
					t.Logf("Got expected error during Next: %v", err)
					assert.True(t, moerr.IsMoErrCode(err, moerr.ErrStaleRead), "Expected stale read error, got: %v", err)
					gotError = true
					break
				}
				if data != nil {
					data.Clean(mp)
				}
				if tombstone != nil {
					tombstone.Clean(mp)
				}
				if data == nil && tombstone == nil {
					break
				}
			}
			// We expect to get the stale read error
			assert.True(t, gotError, "Expected to encounter stale read error")
			if handle != nil {
				handle.Close()
			}
		}
	}
}

func TestPartitionChangesHandleGCKPBoundaryStaleRead(t *testing.T) {
	/*
		This test reproduces a bug in FilterSortedMetaFilesByTimestamp (snapshot.go:51).
		Because FilterSortedMetaFilesByTimestamp uses LE (<=) instead of LT (<), when the request
		timestamp equals the end of a newer GCKP, it incorrectly returns the previous GCKP segment
		instead of the correct one.

		Test scenario:
		1. Insert data and create first global checkpoint (GCKP1) with range [0, gckp1End]
		2. Insert more data and create second global checkpoint (GCKP2) with range [0, gckp2End]
		3. Force GC to clean up old partition state, forcing RequestSnapshotRead to be used
		4. Try to read from a timestamp equal to gckp2End
		5. Due to the LE bug in FilterSortedMetaFilesByTimestamp, it will return GCKP1 instead of GCKP2
		6. This causes getNextChangeHandle to receive the wrong checkpoint entry, leading to stale read error
	*/

	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		tableName    = "test_gckp_boundary"
		databaseName = "db_gckp_boundary"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	schema := catalog2.MockSchemaAll(20, 0)
	schema.Name = tableName
	bat := catalog2.MockBatch(schema, 3)
	defer bat.Close()
	bats := bat.Split(3)

	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	// Create database and table
	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	// Insert first batch
	txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	require.Nil(t, rel.Append(ctx, bats[0]))
	require.Nil(t, txn.Commit(ctx))
	t1 := txn.GetCommitTS()

	txn, rel = testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	id := rel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	require.Nil(t, txn.Commit(ctx))

	// Force first global checkpoint (GCKP1)
	gckp1TS := taeHandler.GetDB().TxnMgr.Now()
	err = taeHandler.GetDB().ForceGlobalCheckpoint(ctx, gckp1TS, 0)
	require.NoError(t, err)
	gckp1 := taeHandler.GetDB().BGCheckpointRunner.MaxGlobalCheckpoint()
	require.NotNil(t, gckp1)
	gckp1End := gckp1.GetEnd()
	t.Logf("GCKP1: %s", gckp1.String())

	// Insert second batch
	txn, rel = testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	require.Nil(t, rel.Append(ctx, bats[1]))
	require.Nil(t, txn.Commit(ctx))

	// Force second global checkpoint (GCKP2)
	gckp2TS := taeHandler.GetDB().TxnMgr.Now()
	err = taeHandler.GetDB().ForceGlobalCheckpoint(ctx, gckp2TS, 0)
	require.NoError(t, err)
	gckp2 := taeHandler.GetDB().BGCheckpointRunner.MaxGlobalCheckpoint()
	require.NotNil(t, gckp2)
	gckp2End := gckp2.GetEnd()
	t.Logf("GCKP2: %s", gckp2.String())

	// Verify we have two different global checkpoints
	require.True(t, gckp2End.GT(&gckp1End), "GCKP2 end should be greater than GCKP1 end")

	// Insert third batch
	txn, rel = testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	require.Nil(t, rel.Append(ctx, bats[2]))
	require.Nil(t, txn.Commit(ctx))

	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, databaseName, tableName, false)
	require.Nil(t, err)

	mp := common.DebugAllocator

	// Force GC to clean up old partition state
	// This forces the code path to use RequestSnapshotRead in getNextChangeHandle
	disttaeEngine.Engine.ForceGC(ctx, t1.Next())

	// Setup stub to use real HandleSnapshotRead from taeHandler
	// This will return real checkpoint entries from the TAE engine
	ssStub := gostub.Stub(
		&disttae.RequestSnapshotRead,
		disttae.GetSnapshotReadFnWithHandler(
			taeHandler.GetRPCHandle().HandleSnapshotRead,
		),
	)
	defer ssStub.Reset()

	// Try to collect changes from a timestamp that will trigger the bug
	// Due to the bug in FilterSortedMetaFilesByTimestamp using LE,
	// when nextFrom == gckp2End, it will return GCKP1 instead of GCKP2
	// This causes getNextChangeHandle to get the wrong checkpoint entry range
	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		// Start reading from just before t1, which will cause nextFrom to be around gckp2End
		// The bug occurs when the code tries to find the right checkpoint for a timestamp
		// that equals the end of the second GCKP
		readFromTS := t1.Prev()
		readToTS := taeHandler.GetDB().TxnMgr.Now()

		handle, err := rel.CollectChanges(ctx, readFromTS, readToTS, false, mp)

		assert.NoError(t, err)
		assert.NotNil(t, handle)

		for {
			data, tombstone, _, err := handle.Next(ctx, mp)
			assert.NoError(t, err)
			if data != nil {
				data.Clean(mp)
			}
			if tombstone != nil {
				tombstone.Clean(mp)
			}
			if data == nil && tombstone == nil {
				break
			}
		}
	}
}

func TestISCPTableIDChange(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId = catalog.System_Account
	)
	const jobName = "test_idx"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	err := mock_mo_indexes(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_foreign_keys(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_intra_system_change_propagation_log(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)

	bat := CreateDBAndTableForCNConsumerAndGetAppendData(t, disttaeEngine, ctxWithTimeout, "srcdb", "src_table", 10)
	bats := bat.Split(10)
	defer bat.Close()

	// append 1 row
	_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table")
	require.NoError(t, err)

	tableID := rel.GetTableID(ctxWithTimeout)

	err = rel.Write(ctxWithTimeout, containers.ToCNBatch(bats[0]))
	require.NoError(t, err)

	require.NoError(t, txn.Commit(ctxWithTimeout))
	initialTarget := types.TimestampToTS(txn.Txn().CommitTS)

	// init cdc executor
	checkLeaseStub := gostub.Stub(
		&iscp.CheckLeaseWithRetry,
		func(
			context.Context,
			string,
			engine.Engine,
			client.TxnClient,
		) (bool, error) {
			return true, nil
		},
	)
	defer checkLeaseStub.Reset()
	cdcExecutor, err := iscp.NewISCPTaskExecutor(
		ctxWithTimeout,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		"",
		&iscp.ISCPExecutorOption{
			GCInterval:             time.Hour,
			GCTTL:                  time.Hour,
			SyncTaskInterval:       time.Millisecond * 100,
			FlushWatermarkInterval: time.Millisecond * 100,
			RetryTimes:             1,
		},
		common.DebugAllocator,
	)
	require.NoError(t, err)
	cdcExecutor.SetRpcHandleFn(taeHandler.GetRPCHandle().HandleGetChangedTableList)

	err = cdcExecutor.Start()
	require.NoError(t, err)
	defer cdcExecutor.Stop()

	// register index job
	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
	require.NoError(t, err)
	ok, err := iscp.RegisterJob(
		ctx, "", txn,
		&iscp.JobSpec{
			ConsumerInfo: iscp.ConsumerInfo{
				ConsumerType: int8(iscp.ConsumerType_CNConsumer),
			},
		},
		&iscp.JobID{
			JobName:   jobName,
			DBName:    "srcdb",
			TableName: "src_table",
		},
		false,
	)
	require.True(t, ok)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctxWithTimeout))

	// Establish the initial replay and snapshot before simulating a table ID
	// change. Otherwise the fault can race the state it is meant to invalidate.
	waitForISCPWatermark(
		t,
		func() (types.TS, bool) {
			return cdcExecutor.GetWatermark(accountId, tableID, jobName)
		},
		initialTarget,
		10*time.Second,
		10*time.Millisecond,
		accountId,
		tableID,
		jobName,
	)

	// enable injection to trigger table id change check
	fault.Enable()
	defer fault.Disable()
	rmFn, err := objectio.InjectCDCExecutor("tableIDChange")
	require.NoError(t, err)
	defer rmFn()

	// append more data to trigger synchronization
	_, rel, txn, err = disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table")
	require.NoError(t, err)

	err = rel.Write(ctxWithTimeout, containers.ToCNBatch(bats[1]))
	require.NoError(t, err)

	require.NoError(t, txn.Commit(ctxWithTimeout))
	target := types.TimestampToTS(txn.Txn().CommitTS)
	waitForISCPWatermark(
		t,
		func() (types.TS, bool) {
			return cdcExecutor.GetWatermark(accountId, tableID, jobName)
		},
		target,
		10*time.Second,
		10*time.Millisecond,
		accountId,
		tableID,
		jobName,
	)
	CheckTableData(t, disttaeEngine, ctxWithTimeout, "srcdb", "src_table", tableID, jobName)
}

func TestIterationError(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId = catalog.System_Account
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	err := mock_mo_indexes(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_foreign_keys(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_intra_system_change_propagation_log(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)

	fault.Enable()
	defer fault.Disable()
	rmFn, err := objectio.InjectCDCExecutor("processInitSQLNewTxn")
	assert.NoError(t, err)
	defer rmFn()
	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	encoded := base64.StdEncoding.EncodeToString([]byte("invalid sql"))
	err = iscp.ProcessInitSQL(ctx, "", disttaeEngine.Engine, disttaeEngine.GetTxnClient(), encoded, "", "", "")
	require.Error(t, err)
}

func TestFileNotFoundFallbackToSnapshotRead(t *testing.T) {
	// When PartitionState references GC-ed object files, NewChangesHandler
	// returns ErrFileNotFound. getNextChangeHandle should fall back to the
	// snapshot read path and succeed.

	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		tableName    = "test_fnf_fallback"
		databaseName = "db_fnf_fallback"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	schema := catalog2.MockSchemaAll(20, 0)
	schema.Name = tableName
	bat := catalog2.MockBatch(schema, 3)
	defer bat.Close()
	bats := bat.Split(3)

	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	require.Nil(t, rel.Append(ctx, bats[0]))
	require.Nil(t, txn.Commit(ctx))
	t1 := txn.GetCommitTS()

	txn, rel = testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	id := rel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	require.Nil(t, txn.Commit(ctx))

	now := taeHandler.GetDB().TxnMgr.Now()
	taeHandler.GetDB().ForceCheckpoint(ctx, now)
	now = taeHandler.GetDB().TxnMgr.Now()
	taeHandler.GetDB().ForceCheckpoint(ctx, now)

	txn, rel = testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	require.Nil(t, rel.Append(ctx, bats[1]))
	require.Nil(t, txn.Commit(ctx))

	now = taeHandler.GetDB().TxnMgr.Now()
	taeHandler.GetDB().ForceCheckpoint(ctx, now)

	txn, rel = testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	require.Nil(t, rel.Append(ctx, bats[2]))
	require.Nil(t, txn.Commit(ctx))

	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, databaseName, tableName, false)
	require.Nil(t, err)

	mp := common.DebugAllocator

	// Inject FileNotFound to simulate GC-ed object files while PartitionState
	// still references them. The partition state path will fail, and the code
	// should fall back to snapshot read.
	chStub := gostub.Stub(
		&disttae.NewPartitionStateChangesHandler,
		func(
			ctx context.Context,
			state *logtailreplay.PartitionState,
			start, end types.TS,
			skipDeletes bool,
			maxRow uint32,
			primarySeqnum int,
			mp *mpool.MPool,
			fs fileservice.FileService,
		) (*logtailreplay.ChangeHandler, error) {
			return nil, moerr.NewFileNotFoundNoCtx("simulated-gc-deleted-object")
		},
	)
	defer chStub.Reset()

	ssStub := gostub.Stub(
		&disttae.RequestSnapshotRead,
		disttae.GetSnapshotReadFnWithHandler(
			taeHandler.GetRPCHandle().HandleSnapshotRead,
		),
	)
	defer ssStub.Reset()

	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		readToTS := taeHandler.GetDB().TxnMgr.Now()
		handle, err := rel.CollectChanges(ctx, t1.Prev(), readToTS, false, mp)
		// Should succeed via snapshot read fallback, not return ErrFileNotFound
		assert.NoError(t, err, "expected fallback to snapshot read to succeed")
		if handle != nil {
			handle.Close()
		}
	}
}

func TestRealStaleReadStillReturnsError(t *testing.T) {
	// A real ErrStaleRead (state.start > start, logical range not covered)
	// must NOT be swallowed — it should propagate to the caller as error 22101.

	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		tableName    = "test_real_stale"
		databaseName = "db_real_stale"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	schema := catalog2.MockSchemaAll(20, 0)
	schema.Name = tableName
	bat := catalog2.MockBatch(schema, 3)
	defer bat.Close()
	bats := bat.Split(3)

	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	require.Nil(t, rel.Append(ctx, bats[0]))
	require.Nil(t, txn.Commit(ctx))
	t1 := txn.GetCommitTS()

	txn, rel = testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	id := rel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	require.Nil(t, txn.Commit(ctx))

	now := taeHandler.GetDB().TxnMgr.Now()
	taeHandler.GetDB().ForceCheckpoint(ctx, now)
	now = taeHandler.GetDB().TxnMgr.Now()
	taeHandler.GetDB().ForceCheckpoint(ctx, now)

	txn, rel = testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	require.Nil(t, rel.Append(ctx, bats[1]))
	require.Nil(t, txn.Commit(ctx))
	t2 := txn.GetCommitTS()

	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, databaseName, tableName, false)
	require.Nil(t, err)

	mp := common.DebugAllocator

	// Force GC so that state.start > t1, making the partition state unable
	// to serve the requested range. This is a real stale read.
	disttaeEngine.Engine.ForceGC(ctx, t2.Next())

	// Stub snapshot read to also fail (return entries that don't cover t1)
	ssStub := gostub.Stub(
		&disttae.RequestSnapshotRead,
		disttae.GetSnapshotReadFnWithHandler(
			func(ctx context.Context, meta pbtxn.TxnMeta, req *cmd_util.SnapshotReadReq, resp *cmd_util.SnapshotReadResp) (func(), error) {
				t2ts := t2.ToTimestamp()
				t2NextTs := t2.Next().ToTimestamp()
				resp.Succeed = true
				resp.Entries = []*cmd_util.CheckpointEntryResp{
					{
						Start:     &t2ts,
						End:       &t2NextTs,
						Location1: []byte("fake"),
						Location2: []byte("fake"),
						EntryType: int32(checkpoint.ET_Incremental),
						Version:   1,
					},
				}
				return func() {}, nil
			},
		),
	)
	defer ssStub.Reset()

	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		readToTS := taeHandler.GetDB().TxnMgr.Now()
		handle, err := rel.CollectChanges(ctx, t1.Prev(), readToTS, false, mp)
		// Real stale read: must return ErrStaleRead, not silently succeed
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrStaleRead),
			"expected ErrStaleRead (22101), got: %v", err)
		if handle != nil {
			handle.Close()
		}
	}
}
