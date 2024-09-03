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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	testutil2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
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
			assert.Equal(t, hint, engine.ChangesHandle_Tail_wip)
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
			assert.Equal(t, hint, engine.ChangesHandle_Tail_wip)
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
		batchCount := 0
		for {
			data, tombstone, hint, err := handle.Next(ctx, mp)
			if data == nil && tombstone == nil {
				break
			}
			batchCount++
			assert.NoError(t, err)
			if batchCount > 1 {
				assert.Nil(t, tombstone)
			} else {
				assert.Equal(t, hint, engine.ChangesHandle_Tail_wip)
				checkTombstoneBatch(tombstone, schema.GetPrimaryKey().Type, t)
				assert.Equal(t, tombstone.Vecs[0].Length(), 20)
				tombstone.Clean(mp)
			}
			checkInsertBatch(bat, data, t)
			assert.Equal(t, data.Vecs[0].Length(), 8192)
			data.Clean(mp)
		}
		assert.Equal(t, batchCount, 20)
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

	opt, err := testutil.GetS3SharedFileServiceOption(ctx, testutil.GetDefaultTestPath("test", t))
	require.NoError(t, err)

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(ctx, testutil.TestOptions{TaeEngineOptions: opt}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()
	startTS := taeEngine.GetDB().TxnMgr.Now()

	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	blockCnt := 10
	rowsCount := int(options.DefaultBlockMaxRows) * blockCnt
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
