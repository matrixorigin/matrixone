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
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/defines"
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

	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.Nil(t, err)

	rpcAgent := testutil.NewMockLogtailAgent()
	defer rpcAgent.Close()

	taeHandler, err := testutil.NewTestTAEEngine(ctx, "partition_state", t, rpcAgent, nil)
	require.Nil(t, err)
	defer taeHandler.Close(ctx)

	disttaeEngine, err := testutil.NewTestDisttaeEngine(ctx, mp, taeHandler.GetDB().Runtime.Fs.Service, rpcAgent)
	require.Nil(t, err)
	defer disttaeEngine.Close(ctx)

	txnOp, err := disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Now())
	require.Nil(t, err)

	resp, databaseId := rpcAgent.CreateDatabase(ctx, databaseName, disttaeEngine.Engine, txnOp)
	require.Nil(t, resp.TxnError)

	db, err := disttaeEngine.Engine.Database(ctx, databaseName, txnOp)
	require.Nil(t, err)
	require.NotNil(t, db)

	db.GetDatabaseId(ctx)
	require.Equal(t, strconv.Itoa(int(databaseId)), db.GetDatabaseId(ctx))

	dbEntry, err := taeHandler.GetDB().Catalog.GetDatabaseByID(databaseId)
	require.Nil(t, err)
	require.Equal(t, dbEntry.ID, databaseId)

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

	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.Nil(t, err)

	rpcAgent := testutil.NewMockLogtailAgent()
	defer rpcAgent.Close()

	taeHandler, err := testutil.NewTestTAEEngine(ctx, "partition_state", t, rpcAgent, nil)
	require.Nil(t, err)
	defer taeHandler.Close(ctx)

	disttaeEngine, err := testutil.NewTestDisttaeEngine(ctx, mp, taeHandler.GetDB().Runtime.Fs.Service, rpcAgent)
	require.Nil(t, err)
	defer disttaeEngine.Close(ctx)

	txnOp, err := disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Now())
	require.Nil(t, err)

	resp, databaseId := rpcAgent.CreateDatabase(ctx, databaseName, disttaeEngine.Engine, txnOp)
	require.Nil(t, resp.TxnError)

	db, err := disttaeEngine.Engine.Database(ctx, databaseName, txnOp)
	require.Nil(t, err)
	require.NotNil(t, db)

	require.Equal(t, strconv.Itoa(int(databaseId)), db.GetDatabaseId(ctx))

	dbEntry, err := taeHandler.GetDB().Catalog.GetDatabaseByID(databaseId)
	require.Nil(t, err)
	require.Equal(t, dbEntry.ID, databaseId)

	schema := catalog2.MockSchemaAll(10, 0)
	bat := catalog2.MockBatch(schema, 10)
	schema.Name = tableName

	err = txnOp.UpdateSnapshot(ctx, disttaeEngine.Now())
	require.Nil(t, err)

	resp, tableId = rpcAgent.CreateTable(ctx, db, schema, txnOp.SnapshotTS())
	require.Nil(t, resp.TxnError)

	err = disttaeEngine.Engine.PushClient().TryToSubscribeTable(ctx, databaseId, tableId)
	require.Nil(t, err)

	entry, err := taeHandler.GetDB().Catalog.GetDatabaseByID(databaseId)
	require.Nil(t, err)
	require.Equal(t, entry.ID, databaseId)

	tt, err := entry.GetTableEntryByID(tableId)
	require.Nil(t, err)
	require.Equal(t, tt.ID, tableId)

	err = txnOp.UpdateSnapshot(ctx, disttaeEngine.Now())
	require.Nil(t, err)

	dbName, tblName, rel, err := disttaeEngine.Engine.GetRelationById(ctx, txnOp, uint64(tableId))
	require.Nil(t, err)
	require.Equal(t, dbName, databaseName)
	require.Equal(t, tblName, tableName)

	rpcAgent.Insert(ctx, accountId, rel, databaseName, bat, mp, txnOp.SnapshotTS())
	require.Nil(t, resp.TxnError)

	rows, err := disttaeEngine.CountStar(ctx, uint64(databaseId), uint64(tableId))
	require.Nil(t, err)
	require.Equal(t, rows, uint32(10))

}
