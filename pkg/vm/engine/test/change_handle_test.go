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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
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
		[]uint64{1},
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
	assert.Equal(t, len(bat.Vecs), len(userBatch.Vecs)+1) // user rows + committs
	for i, vec := range userBatch.Vecs {
		assert.Equal(t, bat.Vecs[i].GetType().Oid, vec.GetType().Oid)
		assert.Equal(t, bat.Vecs[i].Length(), length)
	}
	assert.Equal(t, bat.Vecs[len(userBatch.Vecs)].GetType().Oid, types.T_TS)
	assert.Equal(t, bat.Vecs[len(userBatch.Vecs)].Length(), length)
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
	bat := catalog2.MockBatch(schema, 163840)
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
	for iter.Next() {
		obj := iter.GetObject()
		err = rel.RangeDelete(obj.Fingerprint(), 0, 0, handle.DT_Normal)
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
		assert.Equal(t, totalRows, 163820)
		assert.NoError(t, handle.Close())

		handle, err = rel.CollectChanges(ctx, startTS, taeHandler.GetDB().TxnMgr.Now(), true, mp)
		assert.NoError(t, err)
		totalRows = 0
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			if tombstone != nil {
				assert.Equal(t, hint, engine.ChangesHandle_Tail_done)
				checkTombstoneBatch(tombstone, schema.GetPrimaryKey().Type, t)
				assert.Equal(t, tombstone.Vecs[0].Length(), 20)
				tombstone.Clean(mp)
			}
			if data != nil {
				checkInsertBatch(bat, data, t)
				totalRows += data.Vecs[0].Length()
				data.Clean(mp)
			}
		}
		assert.Equal(t, totalRows, 163840)
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
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrStaleRead))
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
	bat := catalog2.MockBatch(schema, 163840)
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
	for iter.Next() {
		obj := iter.GetObject()
		err = rel.RangeDelete(obj.Fingerprint(), 0, 0, handle.DT_Normal)
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
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			if tombstone != nil {
				assert.Equal(t, hint, engine.ChangesHandle_Tail_done)
				checkTombstoneBatch(tombstone, schema.GetPrimaryKey().Type, t)
				assert.Equal(t, tombstone.Vecs[0].Length(), 20)
				tombstone.Clean(mp)
			}
			if data != nil {
				checkInsertBatch(bat, data, t)
				totalRows += data.Vecs[0].Length()
				data.Clean(mp)
			}
		}
		assert.Equal(t, totalRows, 163840)
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

	txn.Commit(ctxWithTimeout)

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

	cdcExecutor.Start()
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
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(ctxWithTimeout))

	testutils.WaitExpect(
		4000,
		func() bool {
			_, ok := cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
			return ok
		},
	)
	_, ok = cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
	assert.True(t, ok)

	// append 1 row
	_, rel, txn, err = disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table")
	require.Nil(t, err)

	err = rel.Write(ctxWithTimeout, containers.ToCNBatch(bats[1]))
	require.Nil(t, err)

	txn.Commit(ctxWithTimeout)

	now := taeHandler.GetDB().TxnMgr.Now()
	testutils.WaitExpect(
		4000,
		func() bool {
			ts, _ := cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
			return ts.GE(&now)
		},
	)
	ts, _ := cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
	assert.True(t, ts.GE(&now))
	t.Logf("watermark greater than %v", now.ToString())

	cdcExecutor.Stop()
	cdcExecutor.Start()

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
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(ctxWithTimeout))

	testutils.WaitExpect(
		4000,
		func() bool {
			_, ok := cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
			return !ok
		},
	)
	_, ok = cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
	assert.False(t, ok)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))

	CheckTableData(t, disttaeEngine, ctxWithTimeout, "srcdb", "src_table", tableID, "hnsw_idx")

}

// test register and unregister job
func TestISCPExecutor2(t *testing.T) {
	t.Skip("todo")
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

	txn.Commit(ctxWithTimeout)

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

	cdcExecutor.Start()
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
	assert.False(t, ok)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(ctxWithTimeout))

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
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(ctxWithTimeout))

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
	assert.False(t, ok)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(ctxWithTimeout))

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
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(ctxWithTimeout))

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
	assert.False(t, ok)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(ctxWithTimeout))

	testutils.WaitExpect(
		4000,
		func() bool {
			_, ok := cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
			return !ok
		},
	)
	_, ok = cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
	assert.False(t, ok)
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
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(ctxWithTimeout))

	testutils.WaitExpect(
		1000,
		func() bool {
			_, ok := cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
			return ok
		},
	)
	_, ok = cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
	assert.True(t, ok)
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

	txn.Commit(ctxWithTimeout)

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

	cdcExecutor.Start()
	defer cdcExecutor.Stop()

	fault.Enable()
	defer fault.Disable()

	registerFn := func(indexName string) {
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
		assert.True(t, ok)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctxWithTimeout))
	}
	unregisterFn := func(indexName string) {
		txn, err := disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
		require.NoError(t, err)
		ok, err := iscp.UnregisterJob(ctx, "", txn,
			&iscp.JobID{
				JobName:   indexName,
				DBName:    "srcdb",
				TableName: "src_table",
			},
		)
		assert.True(t, ok)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctxWithTimeout))
	}

	appendFn := func(idx int) {
		_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table")
		require.Nil(t, err)

		err = rel.Write(ctxWithTimeout, containers.ToCNBatch(bats[idx]))
		require.Nil(t, err)

		txn.Commit(ctxWithTimeout)
	}

	checkWaterMarkFn := func(indexName string, waitTime int, expectResult bool) {
		now := taeHandler.GetDB().TxnMgr.Now()
		testutils.WaitExpect(
			waitTime,
			func() bool {
				ts, _ := cdcExecutor.GetWatermark(accountId, tableID, indexName)
				return ts.GE(&now)
			},
		)
		ts, _ := cdcExecutor.GetWatermark(accountId, tableID, indexName)
		if expectResult {
			assert.True(t, ts.GE(&now), indexName)
		} else {
			assert.False(t, ts.GE(&now), indexName)
		}
	}
	// add index
	rmFn, err := objectio.InjectCDCExecutor("applyISCPLog")
	assert.NoError(t, err)

	registerFn("hnsw_idx_0")
	testutils.WaitExpect(
		100,
		func() bool {
			_, ok := cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx_0")
			return ok
		},
	)
	_, ok := cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx_0")
	assert.False(t, ok)

	rmFn()

	unregisterFn("hnsw_idx_0")
	// first iteration
	appendFn(0)

	// insert AsyncIndexIterations failed

	registerFn("hnsw_idx_0")
	checkWaterMarkFn("hnsw_idx_0", 4000, true)

	// changesNext failed
	rmFn, err = objectio.InjectCDCExecutor("changesNext")
	assert.NoError(t, err)

	registerFn("hnsw_idx_1")

	checkWaterMarkFn("hnsw_idx_1", 100, false)
	rmFn()

	checkWaterMarkFn("hnsw_idx_1", 4000, true)

	// collect Changes failed
	rmFn, err = objectio.InjectCDCExecutor("collectChanges")
	assert.NoError(t, err)

	registerFn("hnsw_idx_2")

	checkWaterMarkFn("hnsw_idx_2", 100, false)
	rmFn()

	checkWaterMarkFn("hnsw_idx_2", 4000, true)

	// consume failed
	rmFn, err = objectio.InjectCDCExecutor("consume")
	assert.NoError(t, err)

	registerFn("hnsw_idx_3")

	checkWaterMarkFn("hnsw_idx_3", 100, false)
	rmFn()

	checkWaterMarkFn("hnsw_idx_3", 4000, true)

	// getDirtyTables
	rmFn, err = objectio.InjectCDCExecutor("getDirtyTables")
	assert.NoError(t, err)

	for i := 0; i < 4; i++ {
		checkWaterMarkFn(fmt.Sprintf("hnsw_idx_%d", i), 10000, true)
	}
	rmFn()

	// drop index
	rmFn, err = objectio.InjectCDCExecutor("deleteIndex")
	assert.NoError(t, err)
	testutils.WaitExpect(
		4000,
		func() bool {
			_, ok := cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx_0")
			return !ok
		},
	)
	_, ok = cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx_0")
	assert.True(t, ok)
	rmFn()
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

	txn.Commit(ctxWithTimeout)

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

	cdcExecutor.Start()
	defer cdcExecutor.Stop()

	fault.Enable()
	defer fault.Disable()

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
		assert.True(t, ok)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctxWithTimeout))
	}

	appendFn := func(idx int) {
		_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table")
		require.Nil(t, err)

		err = rel.Write(ctxWithTimeout, containers.ToCNBatch(bats[idx]))
		require.Nil(t, err)

		txn.Commit(ctxWithTimeout)
	}

	checkWaterMarkFn := func(indexName string, waitTime int, expectResult bool) {
		now := taeHandler.GetDB().TxnMgr.Now()
		testutils.WaitExpect(
			waitTime,
			func() bool {
				ts, _ := cdcExecutor.GetWatermark(accountId, tableID, indexName)
				return ts.GE(&now)
			},
		)
		ts, _ := cdcExecutor.GetWatermark(accountId, tableID, indexName)
		if expectResult {
			assert.True(t, ts.GE(&now), indexName)
		} else {
			assert.False(t, ts.GE(&now), indexName)
		}
	}

	indexCount := 3
	for i := 0; i < indexCount; i++ {
		registerFn(fmt.Sprintf("hnsw_idx_%d", i))
	}

	appendFn(0)
	appendFn(1)

	// insertAsyncIndexIterations failed
	appendFn(2)
	for i := 0; i < indexCount; i++ {
		checkWaterMarkFn(fmt.Sprintf("hnsw_idx_%d", i), 4000, true)
	}

	// collectChanges failed
	rmFn, err := objectio.InjectCDCExecutor("collectChanges")
	assert.NoError(t, err)
	appendFn(3)
	checkWaterMarkFn("hnsw_idx_0", 100, false)
	rmFn()
	for i := 0; i < indexCount; i++ {
		checkWaterMarkFn(fmt.Sprintf("hnsw_idx_%d", i), 4000, true)
	}

	// changesNext failed
	rmFn, err = objectio.InjectCDCExecutor("changesNext")
	assert.NoError(t, err)
	appendFn(6)
	checkWaterMarkFn("hnsw_idx_0", 100, false)
	rmFn()
	for i := 0; i < indexCount; i++ {
		checkWaterMarkFn(fmt.Sprintf("hnsw_idx_%d", i), 1000, true)
	}

	// consume failed
	rmFn, err = objectio.InjectCDCExecutor("consume")
	assert.NoError(t, err)
	appendFn(7)
	checkWaterMarkFn("hnsw_idx_0", 100, false)
	rmFn()
	for i := 0; i < indexCount; i++ {
		checkWaterMarkFn(fmt.Sprintf("hnsw_idx_%d", i), 1000, true)
	}
	// consume, firstTxn failed
	rmFn, err = objectio.InjectCDCExecutor("consumeWithJobName:hnsw_idx_0")
	assert.NoError(t, err)
	appendFn(8)
	checkWaterMarkFn("hnsw_idx_0", 100, false)
	// for i := 1; i < indexCount; i++ {
	// 	CheckTableData(t, disttaeEngine, ctxWithTimeout, "srcdb", "src_table", tableID, fmt.Sprintf("hnsw_idx_%d", i))
	// }
	rmFn()
	for i := 1; i < indexCount; i++ {
		checkWaterMarkFn(fmt.Sprintf("hnsw_idx_%d", i), 1000, true)
	}

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

		txn.Commit(ctxWithTimeout)
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

	cdcExecutor.Start()
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
		assert.True(t, ok)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctxWithTimeout))
	}

	checkWaterMarkFn := func(indexName string, waitTime int, tableIdx int) {
		now := taeHandler.GetDB().TxnMgr.Now()
		testutils.WaitExpect(
			waitTime,
			func() bool {
				ts, _ := cdcExecutor.GetWatermark(accountId, tableIDs[tableIdx], indexName)
				return ts.GE(&now)
			},
		)
		ts, _ := cdcExecutor.GetWatermark(accountId, tableIDs[tableIdx], indexName)
		assert.True(t, ts.GE(&now), indexName)
	}

	indexCount := 3
	updateTimes := 10

	for j := 0; j < tableCount; j++ {
		for i := 0; i < indexCount; i++ {
			registerFn(fmt.Sprintf("hnsw_idx_%d", i), fmt.Sprintf("src_table_%d", j))
		}
	}

	deleted := make([]bool, tableCount)
	for i := 0; i < updateTimes; i++ {
		for j := 0; j < tableCount; j++ {
			if deleted[j] {
				testutil2.Append(t, accountId, taeHandler.GetDB(), dbName, fmt.Sprintf("src_table_%d", j), bats[j])
				deleted[j] = false
			} else {
				testutil2.DeleteAll(t, accountId, taeHandler.GetDB(), dbName, fmt.Sprintf("src_table_%d", j))
				deleted[j] = true
			}
		}
	}

	for j := 0; j < tableCount; j++ {
		for i := 0; i < indexCount; i++ {
			checkWaterMarkFn(fmt.Sprintf("hnsw_idx_%d", i), 4000, j)
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

	txn.Commit(ctxAccountID2)

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

	cdcExecutor.Start()
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
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(ctxAccountID2))

	testutils.WaitExpect(
		1000,
		func() bool {
			_, ok := cdcExecutor.GetWatermark(account2, tableID, "idx")
			return ok
		},
	)
	_, ok = cdcExecutor.GetWatermark(account2, tableID, "idx")
	assert.True(t, ok)
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
	require.Nil(t, err)

	txn.Commit(ctxWithTimeout)

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

	cdcExecutor.Start()
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
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(ctxWithTimeout))

	now := taeHandler.GetDB().TxnMgr.Now()
	testutils.WaitExpect(
		4000,
		func() bool {
			ts, ok := cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
			return ok && ts.GE(&now)
		},
	)
	ts, ok := cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
	assert.True(t, ok)
	assert.True(t, ts.GE(&now))

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
	require.Nil(t, err)

	txn.Commit(ctxWithTimeout)

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

	cdcExecutor.Start()
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
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(ctxWithTimeout))

	now := taeHandler.GetDB().TxnMgr.Now()
	testutils.WaitExpect(
		4000,
		func() bool {
			ts, ok := cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
			return ok && ts.GE(&now)
		},
	)
	ts, ok := cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
	assert.True(t, ok)
	assert.True(t, ts.GE(&now))

	testutil2.DeleteAll(t, accountId, taeHandler.GetDB(), "srcdb", "src_table")

	now = taeHandler.GetDB().TxnMgr.Now()
	testutils.WaitExpect(
		4000,
		func() bool {
			ts, ok := cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
			return ok && ts.GE(&now)
		},
	)
	ts, ok = cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
	assert.True(t, ok)
	assert.True(t, ts.GE(&now))

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

	txn.Commit(ctxWithTimeout)

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

	cdcExecutor.Start()
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
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(ctxWithTimeout))

	appendFn := func(idx int) {
		_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, dbName, tableName)
		require.Nil(t, err)

		err = rel.Write(ctxWithTimeout, containers.ToCNBatch(bats[idx]))
		require.Nil(t, err)

		txn.Commit(ctxWithTimeout)
	}

	checkWaterMarkFn := func(waitTime int) {
		now := taeHandler.GetDB().TxnMgr.Now()
		testutils.WaitExpect(
			waitTime,
			func() bool {
				ts, _ := cdcExecutor.GetWatermark(accountId, tableID, jobName)
				return ts.GE(&now)
			},
		)
		ts, _ := cdcExecutor.GetWatermark(accountId, tableID, jobName)
		assert.True(t, ts.GE(&now), jobName)
	}

	checkJobTypeFn := func(waitTime int, expectJobType uint16) {
		testutils.WaitExpect(
			waitTime,
			func() bool {
				jobType, _ := cdcExecutor.GetJobType(accountId, tableID, jobName)
				return jobType == expectJobType
			},
		)
		jobType, _ := cdcExecutor.GetJobType(accountId, tableID, jobName)
		assert.True(t, jobType == expectJobType, jobName)
	}
	appendFn(0)
	checkWaterMarkFn(4000)

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
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(ctxWithTimeout))

	checkJobTypeFn(4000, iscp.TriggerType_AlwaysUpdate)
	appendFn(1)
	checkWaterMarkFn(4000)
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
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(ctxWithTimeout))

	checkJobTypeFn(4000, iscp.TriggerType_Timed)
	appendFn(2)
	checkWaterMarkFn(4000)

	CheckTableData(t, disttaeEngine, ctxWithTimeout, "srcdb", "src_table", tableID, "job1")
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

	txn.Commit(ctxWithTimeout)

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

	cdcExecutor.Start()
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
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(ctxWithTimeout))
	checkWaterMarkFn := func(waitTime int) {
		now := taeHandler.GetDB().TxnMgr.Now()
		testutils.WaitExpect(
			waitTime,
			func() bool {
				ts, _ := cdcExecutor.GetWatermark(accountId, tableID, jobName)
				return ts.GE(&now)
			},
		)
		ts, _ := cdcExecutor.GetWatermark(accountId, tableID, jobName)
		assert.True(t, ts.GE(&now), jobName)
	}
	checkWaterMarkFn(4000)

	cdcExecutor.FlushWatermarkForAllTables(0)
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
	require.Nil(t, err)

	txn.Commit(ctxWithTimeout)

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

	cdcExecutor.Start()
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
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(ctxWithTimeout))

	now := taeHandler.GetDB().TxnMgr.Now()
	testutils.WaitExpect(
		4000,
		func() bool {
			ts, _ := cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
			return ts.GE(&now)
		},
	)
	ts, _ := cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
	assert.True(t, ts.GE(&now))
	t.Logf("watermark greater than %v", now.ToString())

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
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(ctxWithTimeout))

	now = taeHandler.GetDB().TxnMgr.Now()
	testutils.WaitExpect(
		4000,
		func() bool {
			_, ok := cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
			return !ok
		},
	)
	_, ok = cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
	assert.False(t, ok)
	t.Logf("watermark greater than %v", now.ToString())
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

	cdcExecutor.Start()
	defer cdcExecutor.Stop()

	fault.Enable()
	defer fault.Disable()
	// create database and table

	bat1 := CreateDBAndTableForCNConsumerAndGetAppendData(t, disttaeEngine, ctxWithTimeout, "srcdb", "src_table1", 10)
	defer bat1.Close()

	bat2 := CreateDBAndTableForCNConsumerAndGetAppendData(t, disttaeEngine, ctxWithTimeout, "srcdb", "src_table2", 10)
	defer bat2.Close()

	rmFn, err := objectio.InjectCDCExecutor("iteration:src_table1")
	assert.NoError(t, err)
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
		assert.True(t, ok)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctxWithTimeout))
	}

	registerFn("src_table1")
	registerFn("src_table2")

	_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table1")
	require.Nil(t, err)

	err = rel.Write(ctxWithTimeout, containers.ToCNBatch(bat1))
	require.Nil(t, err)

	txn.Commit(ctxWithTimeout)

	_, rel, txn, err = disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table2")
	require.Nil(t, err)

	err = rel.Write(ctxWithTimeout, containers.ToCNBatch(bat2))
	require.Nil(t, err)

	tableID2 := rel.GetTableID(ctxWithTimeout)

	txn.Commit(ctxWithTimeout)

	now := taeHandler.GetDB().TxnMgr.Now()
	testutils.WaitExpect(
		4000,
		func() bool {
			ts, _ := cdcExecutor.GetWatermark(accountId, tableID2, "job1")
			return ts.GE(&now)
		},
	)
	ts, _ := cdcExecutor.GetWatermark(accountId, tableID2, "job1")
	assert.True(t, ts.GE(&now))
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

	txn.Commit(ctxWithTimeout)

	_, rel, txn, err = disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table2")
	require.Nil(t, err)

	tableID2 := rel.GetTableID(ctxWithTimeout)

	txn.Commit(ctxWithTimeout)

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

	cdcExecutor.Start()
	defer cdcExecutor.Stop()

	registerFn := func(tableName string, jobName string) {
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
		assert.True(t, ok)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctxWithTimeout))
	}

	registerFn("src_table", "job1")
	registerFn("src_table2", "job2")

	now := taeHandler.GetDB().TxnMgr.Now()
	testutils.WaitExpect(
		4000,
		func() bool {
			ts, ok := cdcExecutor.GetWatermark(accountId, tableID, "job1")
			return ok && ts.GE(&now)
		},
	)
	ts, ok := cdcExecutor.GetWatermark(accountId, tableID, "job1")
	assert.True(t, ok)
	assert.True(t, ts.GE(&now))

	testutils.WaitExpect(
		4000,
		func() bool {
			ts, ok := cdcExecutor.GetWatermark(accountId, tableID2, "job2")
			return ok && ts.GE(&now)
		},
	)
	ts, ok = cdcExecutor.GetWatermark(accountId, tableID2, "job2")
	assert.True(t, ok)
	assert.True(t, ts.GE(&now))

	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
	require.NoError(t, err)
	err = iscp.UnregisterJobsByDBName(
		ctx, "", txn, "srcdb",
	)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(ctxWithTimeout))

	testutils.WaitExpect(
		4000,
		func() bool {
			_, ok := cdcExecutor.GetWatermark(accountId, tableID, "job1")
			return !ok
		},
	)
	_, ok = cdcExecutor.GetWatermark(accountId, tableID, "job1")
	assert.False(t, ok)

	testutils.WaitExpect(
		4000,
		func() bool {
			_, ok := cdcExecutor.GetWatermark(accountId, tableID2, "job2")
			return !ok
		},
	)
	_, ok = cdcExecutor.GetWatermark(accountId, tableID2, "job2")
	assert.False(t, ok)
}

func TestInvalidTimestamp(t *testing.T) {
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
	require.Nil(t, err)

	txn.Commit(ctxWithTimeout)

	fault.Enable()
	defer fault.Disable()

	rmFn, err := objectio.InjectCDCExecutor("invalid timestamp")
	assert.NoError(t, err)

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
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(ctxWithTimeout))

	now := taeHandler.GetDB().TxnMgr.Now()
	testutils.WaitExpect(
		4000,
		func() bool {
			ts, ok := cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
			return ok && ts.GE(&now)
		},
	)
	_, ok = cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
	assert.False(t, ok)

	rmFn()

	now = taeHandler.GetDB().TxnMgr.Now()
	testutils.WaitExpect(
		4000,
		func() bool {
			ts, ok := cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
			return ok && ts.GE(&now)
		},
	)
	ts, ok := cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
	assert.True(t, ok)
	assert.True(t, ts.GE(&now))
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
			if flushCount == 0 {
				cancelCh <- struct{}{}
				<-cancelCh
				return nil
			}
			flushCount++
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
		assert.NoError(t, err)
	}()
	<-cancelCh
	cancel()
	close(cancelCh)
	wg.Wait()

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

	txn.Commit(ctxWithTimeout)

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

	cdcExecutor.Start()
	defer cdcExecutor.Stop()

	fault.Enable()
	defer fault.Disable()

	rmFn, err := objectio.InjectCDCExecutor("changesNext")
	require.NoError(t, err)
	defer rmFn()

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
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(ctxWithTimeout))

	now := taeHandler.GetDB().TxnMgr.Now()
	testutils.WaitExpect(
		4000,
		func() bool {
			ts, ok := cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
			return ok && ts.GE(&now)
		},
	)
	ts, ok := cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
	assert.True(t, ok)
	assert.True(t, ts.GE(&now))
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

	txn.Commit(ctxWithTimeout)

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

	cdcExecutor.Start()
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
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(ctxWithTimeout))

	now := taeHandler.GetDB().TxnMgr.Now()
	testutils.WaitExpect(
		4000,
		func() bool {
			ts, ok := cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
			return ok && ts.GE(&now)
		},
	)
	ts, ok := cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
	assert.True(t, ok)
	assert.True(t, ts.GE(&now))

	// update in memory wm
	now = taeHandler.GetDB().TxnMgr.Now()
	testutils.WaitExpect(
		4000,
		func() bool {
			ts, ok := cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
			return ok && ts.GE(&now)
		},
	)
	ts, ok = cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
	assert.True(t, ok)
	assert.True(t, ts.GE(&now))

	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Engine.LatestLogtailAppliedTime())
	require.NoError(t, err)
	ok, err = iscp.UnregisterJob(ctx, "", txn,
		&iscp.JobID{
			JobName:   "hnsw_idx",
			DBName:    "srcdb",
			TableName: "src_table",
		},
	)
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(ctxWithTimeout))

	testutils.WaitExpect(
		4000,
		func() bool {
			_, ok := cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
			return !ok
		},
	)
	_, ok = cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
	assert.False(t, ok)
}

func TestISCPReplay(t *testing.T) {

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

	// tableID := rel.GetTableID(ctxWithTimeout)

	err = rel.Write(ctxWithTimeout, containers.ToCNBatch(bats[0]))
	require.Nil(t, err)

	txn.Commit(ctxWithTimeout)

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

	cdcExecutor.Start()
	defer cdcExecutor.Stop()

	registerFn := func(indexName string) {
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
			true,
		)
		assert.True(t, ok)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctxWithTimeout))
	}

	for i := 0; i < 10; i++ {
		registerFn(fmt.Sprintf("hnsw_idx_%d", i))
	}
	cdcExecutor.Stop()
	cdcExecutor.Start()
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

	txn.Commit(ctxWithTimeout)

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

	cdcExecutor.Start()
	defer cdcExecutor.Stop()
	registerFn := func(indexName string) {
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
		assert.True(t, ok)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctxWithTimeout))
	}
	for i := 0; i < 10; i++ {
		registerFn(fmt.Sprintf("hnsw_idx_%d", i))
	}

	now := taeHandler.GetDB().TxnMgr.Now()
	testutils.WaitExpect(
		4000,
		func() bool {
			for i := 0; i < 10; i++ {
				ts, ok := cdcExecutor.GetWatermark(accountId, tableID, fmt.Sprintf("hnsw_idx_%d", i))
				if !ok || !ts.GE(&now) {
					return false
				}
			}
			return true
		},
	)
	for i := 0; i < 10; i++ {
		ts, ok := cdcExecutor.GetWatermark(accountId, tableID, fmt.Sprintf("hnsw_idx_%d", i))
		assert.True(t, ok)
		assert.True(t, ts.GE(&now))
	}

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
	rmFn()
	err = cdcExecutor.Resume()
	require.NoError(t, err)
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
	require.Nil(t, err)

	txn.Commit(ctxWithTimeout)

	fault.Enable()
	defer fault.Disable()
	rmFn, err := objectio.InjectCDCExecutor("stale read")
	assert.NoError(t, err)
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
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(ctxWithTimeout))

	now := taeHandler.GetDB().TxnMgr.Now()
	testutils.WaitExpect(
		4000,
		func() bool {
			ts, ok := cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
			return ok && ts.GE(&now)
		},
	)
	ts, ok := cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
	assert.True(t, ok)
	assert.True(t, ts.GE(&now))
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

	txn.Commit(ctxWithAccount)

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

	cdcExecutor.Start()
	defer cdcExecutor.Stop()

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
			JobName:   "idx",
			DBName:    "srcdb",
			TableName: "src_table",
		},
		false,
	)
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(ctxWithTimeout))

	now := taeHandler.GetDB().TxnMgr.Now()
	testutils.WaitExpect(
		4000,
		func() bool {
			ts, ok := cdcExecutor.GetWatermark(tableAccountID, tableID, "idx")
			if !ok || !ts.GE(&now) {
				return false
			}
			return true
		},
	)
	ts, ok := cdcExecutor.GetWatermark(tableAccountID, tableID, "idx")
	assert.True(t, ok)
	assert.True(t, ts.GE(&now))

	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Now())
	require.NoError(t, err)

	_, err = disttaeEngine.Engine.Database(ctxWithAccount, "t", txn)
	require.NoError(t, err)
	err = txn.Commit(ctxWithTimeout)
	require.NoError(t, err)

	txn2, rel2 := testutil2.GetRelation(t, tableAccountID, taeHandler.GetDB(), "srcdb", "src_table")
	require.Nil(t, rel2.Append(ctx, bats))
	require.Nil(t, txn2.Commit(ctx))

	now = taeHandler.GetDB().TxnMgr.Now()
	testutils.WaitExpect(
		4000,
		func() bool {
			ts, ok := cdcExecutor.GetWatermark(tableAccountID, tableID, "idx")
			if !ok || !ts.GE(&now) {
				return false
			}
			return true
		},
	)
	ts, ok = cdcExecutor.GetWatermark(tableAccountID, tableID, "idx")
	assert.True(t, ok)
	assert.True(t, ts.GE(&now))

	CheckTableData(t, disttaeEngine, ctxWithAccount, "srcdb", "src_table", tableID, "idx")
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

	checkLeaseStub := gostub.Stub(
		&iscp.CheckLeaseWithRetry,
		func(
			context.Context,
			string,
			engine.Engine,
			client.TxnClient,
		) (bool, error) {
			if msg, injected := objectio.ISCPExecutorInjected(); injected && msg == "check lease" {
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

	bat := CreateDBAndTableForCNConsumerAndGetAppendData(t, disttaeEngine, ctxWithTimeout, "srcdb", "src_table", 10)
	bats := bat.Split(10)
	defer bat.Close()

	// append 1 row
	_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table")
	require.Nil(t, err)

	tableID := rel.GetTableID(ctxWithTimeout)

	err = rel.Write(ctxWithTimeout, containers.ToCNBatch(bats[0]))
	require.Nil(t, err)

	txn.Commit(ctxWithTimeout)

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
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(ctxWithTimeout))

	now := taeHandler.GetDB().TxnMgr.Now()
	testutils.WaitExpect(
		4000,
		func() bool {
			ts, ok := cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
			return ok && ts.GE(&now)
		},
	)
	ts, ok := cdcExecutor.GetWatermark(accountId, tableID, "hnsw_idx")
	assert.True(t, ok)
	assert.True(t, ts.GE(&now))

	fault.Enable()
	defer fault.Disable()
	rmFn, err := objectio.InjectCDCExecutor("check lease")
	defer rmFn()
	assert.NoError(t, err)

	_, rel, txn, err = disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table")
	require.Nil(t, err)

	err = rel.Write(ctxWithTimeout, containers.ToCNBatch(bats[1]))
	require.Nil(t, err)

	txn.Commit(ctxWithTimeout)

	testutils.WaitExpect(
		4000,
		func() bool {
			return !cdcExecutor.IsRunning()
		},
	)
	assert.False(t, cdcExecutor.IsRunning())
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
						EntryType: 0,
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
	require.Nil(t, err)

	tableID := rel.GetTableID(ctxWithTimeout)

	err = rel.Write(ctxWithTimeout, containers.ToCNBatch(bats[0]))
	require.Nil(t, err)

	txn.Commit(ctxWithTimeout)

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
			JobName:   "test_idx",
			DBName:    "srcdb",
			TableName: "src_table",
		},
		false,
	)
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(ctxWithTimeout))

	// wait for synchronization to initialize prevISCPTableID

	// enable injection to trigger table id change check
	fault.Enable()
	defer fault.Disable()
	rmFn, err := objectio.InjectCDCExecutor("tableIDChange")
	assert.NoError(t, err)
	defer rmFn()

	// append more data to trigger synchronization
	_, rel, txn, err = disttaeEngine.GetTable(ctxWithTimeout, "srcdb", "src_table")
	require.Nil(t, err)

	err = rel.Write(ctxWithTimeout, containers.ToCNBatch(bats[1]))
	require.Nil(t, err)

	txn.Commit(ctxWithTimeout)

	now := taeHandler.GetDB().TxnMgr.Now()
	testutils.WaitExpect(
		4000,
		func() bool {
			ts, ok := cdcExecutor.GetWatermark(accountId, tableID, "test_idx")
			return ok && ts.GE(&now)
		},
	)
	ts, ok := cdcExecutor.GetWatermark(accountId, tableID, "test_idx")
	assert.True(t, ok)
	assert.True(t, ts.GE(&now))
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
	err = iscp.ProcessInitSQL(ctx, "", disttaeEngine.Engine, disttaeEngine.GetTxnClient(), encoded)
	require.Error(t, err)
}
