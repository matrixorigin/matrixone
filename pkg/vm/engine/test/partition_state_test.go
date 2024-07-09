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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	testutil "github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/stretchr/testify/require"
)

func Test_CreateDataBase(t *testing.T) {
	var (
		accountId    = catalog.System_Account
		databaseName = "db1"
	)

	ctx := context.Background()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(ctx)
		rpcAgent.Close()
	}()

	testutil.CreateDatabaseOnly(ctx, databaseName, disttaeEngine, taeHandler, rpcAgent, t)

}

func Test_InsertRows(t *testing.T) {
	var (
		accountId  = catalog.System_Account
		tableId    uint64
		databaseId uint64

		tableName    = "test1"
		databaseName = "db1"
	)

	ctx := context.Background()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	disttaeEngine, taeHandler, rpcAgent, mp := testutil.CreateEngines(ctx, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(ctx)
		rpcAgent.Close()
	}()

	dbHandler, dbEntry, txnOp := testutil.CreateDatabaseOnly(ctx, databaseName, disttaeEngine, taeHandler, rpcAgent, t)
	databaseId = dbEntry.GetID()

	schema := catalog2.MockSchemaAll(10, 0)
	bat := catalog2.MockBatch(schema, 10)
	schema.Name = tableName

	var err error

	tblHandler, _ := testutil.CreateTableOnly(ctx, schema, dbHandler, dbEntry, rpcAgent, disttaeEngine.Now(), t)
	tableId = tblHandler.GetTableID(ctx)

	err = disttaeEngine.Engine.PushClient().TryToSubscribeTable(ctx, databaseId, tableId)
	require.Nil(t, err)

	err = txnOp.UpdateSnapshot(ctx, disttaeEngine.Now())
	require.Nil(t, err)

	rpcAgent.InsertRows(ctx, accountId, tblHandler, databaseName, bat, mp, txnOp.SnapshotTS())

	stats, err := disttaeEngine.GetPartitionStateStats(ctx, uint64(databaseId), uint64(tableId))
	require.Nil(t, err)
	require.Equal(t, stats.TotalVisibleRows, uint32(10))
}

func Test_InsertDataObjects(t *testing.T) {
	var (
		accountId    = catalog.System_Account
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx := context.Background()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	disttaeEngine, taeHandler, rpcAgent, mp := testutil.CreateEngines(ctx, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(ctx)
		rpcAgent.Close()
	}()

	schema := catalog2.MockSchemaAll(10, 0)
	schema.Name = tableName

	var (
		err        error
		tblHandler engine.Relation
	)

	_, _, tblHandler, _, _ = testutil.CreateDatabaseAndTable(
		ctx, schema, databaseName, disttaeEngine, taeHandler, rpcAgent, t)

	bat := testutil.MockObjectStatsBatBySchema(tblHandler.GetTableDef(ctx), schema, 8192, 100, mp, t)

	resp := rpcAgent.InsertDataObject(ctx, tblHandler, bat, disttaeEngine.Now())
	require.Nil(t, resp.TxnError)

	err = disttaeEngine.Engine.PushClient().TryToSubscribeTable(ctx, tblHandler.GetDBID(ctx), tblHandler.GetTableID(ctx))
	require.Nil(t, err)

	stats, err := disttaeEngine.GetPartitionStateStats(ctx, tblHandler.GetDBID(ctx), tblHandler.GetTableID(ctx))
	require.Nil(t, err)

	require.Equal(t, stats.TotalVisibleRows, uint32(8192*100))

	require.Equal(t, stats.Blocks.Visible, uint32(100))
	require.Equal(t, stats.Blocks.Invisible, uint32(100))

	require.Equal(t, stats.DataObjets.Visible, 1)
	require.Equal(t, stats.DataObjets.Invisible, 1)
}

func Test_FlushCleanRows(t *testing.T) {
	var (
		accountId    = catalog.System_Account
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx := context.Background()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	disttaeEngine, taeHandler, rpcAgent, mp := testutil.CreateEngines(ctx, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(ctx)
		rpcAgent.Close()
	}()

	schema := catalog2.MockSchemaAll(10, 0)
	schema.Name = tableName
	bat := catalog2.MockBatch(schema, 10)

	var (
		err        error
		tblHandler engine.Relation
		txnOp      client.TxnOperator
	)

	_, _, tblHandler, _, txnOp = testutil.CreateDatabaseAndTable(ctx, schema, databaseName, disttaeEngine, taeHandler, rpcAgent, t)

	err = txnOp.UpdateSnapshot(ctx, disttaeEngine.Now())
	require.Nil(t, err)

	rpcAgent.InsertRows(ctx, accountId, tblHandler, databaseName, bat, mp, txnOp.SnapshotTS())

	err = disttaeEngine.Engine.PushClient().TryToSubscribeTable(ctx, tblHandler.GetDBID(ctx), tblHandler.GetTableID(ctx))
	require.Nil(t, err)

	// before flush
	{
		stats, err := disttaeEngine.GetPartitionStateStats(ctx, tblHandler.GetDBID(ctx), tblHandler.GetTableID(ctx))
		require.Nil(t, err)

		require.Equal(t, stats.TotalVisibleRows, uint32(10))

		require.Equal(t, stats.DataObjets.Visible, 0)
		require.Equal(t, stats.DataObjets.Invisible, 0)
	}

	err = taeHandler.GetDB().FlushTable(ctx, accountId, tblHandler.GetDBID(ctx),
		tblHandler.GetTableID(ctx), types.TimestampToTS(disttaeEngine.Now()))
	require.Nil(t, err)

	// after flush
	{
		stats, err := disttaeEngine.GetPartitionStateStats(ctx, tblHandler.GetDBID(ctx), tblHandler.GetTableID(ctx))
		require.Nil(t, err)

		require.Equal(t, stats.TotalVisibleRows, uint32(10))

		require.Equal(t, stats.DataObjets.Visible, 1)
		require.Equal(t, stats.DataObjets.Invisible, 1)
	}
}

func Test_ConsumeCheckpointLazy(t *testing.T) {
	var (
		accountId    = catalog.System_Account
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx := context.Background()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	disttaeEngine, taeHandler, rpcAgent, mp := testutil.CreateEngines(ctx, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(ctx)
		rpcAgent.Close()
	}()

	var (
		err        error
		txnOp      client.TxnOperator
		tblHandler engine.Relation
	)

	schema := catalog2.MockSchemaAll(10, 0)
	schema.Name = tableName

	_, _, tblHandler, _, txnOp = testutil.CreateDatabaseAndTable(
		ctx, schema, databaseName, disttaeEngine, taeHandler, rpcAgent, t)

	err = txnOp.UpdateSnapshot(ctx, disttaeEngine.Now())
	require.Nil(t, err)

	bat := catalog2.MockBatch(schema, 10)
	resp := rpcAgent.InsertRows(ctx, accountId, tblHandler, databaseName, bat, mp, txnOp.SnapshotTS())
	require.Nil(t, resp.TxnError)

	// flush first to make sure the later checkpoint can contain all inserted data.
	// otherwise, the later checkpoint could be empty
	err = taeHandler.GetDB().FlushTable(ctx, accountId, tblHandler.GetDBID(ctx),
		tblHandler.GetTableID(ctx), types.TimestampToTS(disttaeEngine.Now()))
	require.Nil(t, err)

	err = taeHandler.GetDB().ForceCheckpoint(ctx, types.TimestampToTS(disttaeEngine.Now()), time.Second*5)
	require.Nil(t, err)

	// before subscribe
	{
		stats, err := disttaeEngine.GetPartitionStateStats(ctx, tblHandler.GetDBID(ctx), tblHandler.GetTableID(ctx))
		require.Nil(t, err)

		require.Equal(t, stats.TotalVisibleRows, 0)

		require.Equal(t, stats.DataObjets.Visible, 0)
		require.Equal(t, stats.DataObjets.Invisible, 0)
	}

	err = disttaeEngine.Engine.PushClient().TryToSubscribeTable(ctx, tblHandler.GetDBID(ctx), tblHandler.GetTableID(ctx))
	require.Nil(t, err)

	// after subscribe
	{
		stats, err := disttaeEngine.GetPartitionStateStats(ctx, tblHandler.GetDBID(ctx), tblHandler.GetTableID(ctx))
		require.Nil(t, err)

		require.Equal(t, stats.TotalVisibleRows, 0)

		require.Equal(t, stats.DataObjets.Visible, 0)
		require.Equal(t, stats.DataObjets.Invisible, 0)

		require.Equal(t, stats.CheckpointCnt, 1)
	}

	// lazy load checkpoint
	{
		_, err = disttaeEngine.Engine.LazyLoadLatestCkp(ctx, tblHandler)
		require.Nil(t, err)

		stats, err := disttaeEngine.GetPartitionStateStats(ctx, tblHandler.GetDBID(ctx), tblHandler.GetTableID(ctx))
		require.Nil(t, err)

		require.Equal(t, stats.InmemRows.Invisible+stats.InmemRows.Visible, 0)
		require.Equal(t, stats.TotalVisibleRows, 10)

		require.Equal(t, stats.DataObjets.Visible, 1)
		require.Equal(t, stats.DataObjets.Invisible, 1)

		require.Equal(t, stats.CheckpointCnt, 0)
	}
}
