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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	testutil2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChangesHandle1(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, accountId)
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

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

	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, false)
	require.Nil(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	mp := common.DebugAllocator

	// check partition state, before flush
	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		handle, err := rel.CollectChanges(ctx, types.TS{}, taeHandler.GetDB().TxnMgr.Now(), mp)
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

		handle, err = rel.CollectChanges(ctx, startTS, taeHandler.GetDB().TxnMgr.Now(), mp)
		assert.NoError(t, err)
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			assert.Equal(t, hint, engine.ChangesHandle_Tail_done)
			t.Log(tombstone.Attrs)
			checkTombstoneBatch(tombstone, schema.GetPrimaryKey().Type, t)
			assert.Equal(t, tombstone.Vecs[0].Length(), 1)
			tombstone.Clean(mp)
			t.Log(data.Attrs)
			checkInsertBatch(bat, data, t)
			assert.Equal(t, data.Vecs[0].Length(), 10)
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

	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, accountId)
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

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

	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, false)
	require.Nil(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))

	// check partition state, before flush
	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		handle, err := rel.CollectChanges(ctx, types.TS{}, taeHandler.GetDB().TxnMgr.Now(), mp)
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

		handle, err = rel.CollectChanges(ctx, startTS, taeHandler.GetDB().TxnMgr.Now(), mp)
		assert.NoError(t, err)
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			assert.Equal(t, hint, engine.ChangesHandle_Tail_done)
			checkTombstoneBatch(tombstone, schema.GetPrimaryKey().Type, t)
			assert.Equal(t, tombstone.Vecs[0].Length(), 1)
			tombstone.Clean(mp)
			checkInsertBatch(bat, data, t)
			assert.Equal(t, data.Vecs[0].Length(), 10)
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
	length := bat.Vecs[0].Length()
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

	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, accountId)
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

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
	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, false)
	require.Nil(t, err)

	// check partition state, before flush
	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		handle, err := rel.CollectChanges(ctx, types.TS{}, taeHandler.GetDB().TxnMgr.Now(), mp)
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

		handle, err = rel.CollectChanges(ctx, startTS, taeHandler.GetDB().TxnMgr.Now(), mp)
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
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
	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, false)
	require.Nil(t, err)

	// check partition state, before flush
	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		handle, err := rel.CollectChanges(ctx, types.TS{}, taeEngine.GetDB().TxnMgr.Now(), mp)
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

		handle, err = rel.CollectChanges(ctx, startTS, taeEngine.GetDB().TxnMgr.Now(), mp)
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
			assert.Equal(t, data.Vecs[0].Length(), 8192)
			data.Clean(mp)
		}
		assert.Equal(t, batchCount, 10)
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
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
	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, false)
	require.Nil(t, err)

	dntxn, dnrel = testutil2.GetRelation(t, accountId, taeEngine.GetDB(), databaseName, tableName)
	assert.NoError(t, dnrel.Append(ctx, bats[2]))
	assert.NoError(t, dntxn.Commit(ctx))

	// check partition state, before flush
	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		c := &checkHelper{}
		handle, err := rel.CollectChanges(ctx, startTS, taeEngine.GetDB().TxnMgr.Now(), mp)
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

	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, accountId)
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

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

	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, false)
	require.Nil(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	mp := common.DebugAllocator

	// check partition state, before flush
	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		handle, err := rel.CollectChanges(ctx, types.TS{}, taeHandler.GetDB().TxnMgr.Now(), mp)
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

		handle, err = rel.CollectChanges(ctx, startTS, taeHandler.GetDB().TxnMgr.Now(), mp)
		assert.NoError(t, err)
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			assert.Equal(t, hint, engine.ChangesHandle_Tail_done)
			t.Log(tombstone.Attrs)
			checkTombstoneBatch(tombstone, schema.GetPrimaryKey().Type, t)
			assert.Equal(t, tombstone.Vecs[0].Length(), 1)
			tombstone.Clean(mp)
			t.Log(data.Attrs)
			checkInsertBatch(bat, data, t)
			assert.Equal(t, data.Vecs[0].Length(), 10)
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

	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, accountId)
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

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

	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, false)
	require.Nil(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	mp := common.DebugAllocator

	// check partition state, before flush
	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		handle, err := rel.CollectChanges(ctx, types.TS{}, taeHandler.GetDB().TxnMgr.Now(), mp)
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

		handle, err = rel.CollectChanges(ctx, startTS, taeHandler.GetDB().TxnMgr.Now(), mp)
		assert.NoError(t, err)
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			assert.NoError(t, err)
			assert.Equal(t, hint, engine.ChangesHandle_Tail_done)
			t.Log(tombstone.Attrs)
			checkTombstoneBatch(tombstone, schema.GetPrimaryKey().Type, t)
			assert.Equal(t, tombstone.Vecs[0].Length(), 1)
			tombstone.Clean(mp)
			t.Log(data.Attrs)
			checkInsertBatch(bat, data, t)
			assert.Equal(t, data.Vecs[0].Length(), 10)
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

	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, accountId)
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

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

	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, false)
	require.Nil(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	mp := common.DebugAllocator

	fs := taeHandler.GetDB().Runtime.Fs
	deleteFileName := obj.ObjectStats.ObjectName().String()
	err = fs.DelFiles(ctx, []string{string(deleteFileName)})
	assert.NoError(t, err)
	gcTS := taeHandler.GetDB().TxnMgr.Now()
	gcTSFileName := blockio.EncodeCompactedMetadataFileName(checkpoint.CheckpointDir, checkpoint.PrefixMetadata, types.TS{}, gcTS)
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterCheckpoint, gcTSFileName, fs.Service)
	assert.NoError(t, err)
	_, err = writer.Write(containers.ToCNBatch(bat))
	assert.NoError(t, err)
	_, err = writer.WriteEnd(ctx)
	assert.NoError(t, err)

	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		_, err = rel.CollectChanges(ctx, startTS, taeHandler.GetDB().TxnMgr.Now(), mp)
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
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
	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, false)
	require.Nil(t, err)

	// check partition state, before flush
	{

		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)
		handle, err := rel.CollectChanges(ctx, startTS, taeEngine.GetDB().TxnMgr.Now(), mp)
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
				err = fs.DelFiles(ctx, []string{string(deleteFileName)})
				assert.NoError(t, err)
			}
			gcTS := taeEngine.GetDB().TxnMgr.Now()
			gcTSFileName := blockio.EncodeCompactedMetadataFileName(checkpoint.CheckpointDir, checkpoint.PrefixMetadata, types.TS{}, gcTS)
			writer, err := objectio.NewObjectWriterSpecial(objectio.WriterCheckpoint, gcTSFileName, fs.Service)
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

	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, accountId)
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

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
	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, false)
	require.Nil(t, err)

	// check partition state, before flush
	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		handle, err := rel.CollectChanges(ctx, startTS, taeHandler.GetDB().TxnMgr.Now(), mp)
		assert.NoError(t, err)
		totalRows := 0
		handle.(*logtailreplay.ChangeHandler).LogThreshold = time.Microsecond
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
