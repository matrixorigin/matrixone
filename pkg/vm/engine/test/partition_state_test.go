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

	"github.com/matrixorigin/matrixone/pkg/catalog"
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

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(ctx)
		rpcAgent.Close()
	}()

	testutil.CreateDatabase(ctx, databaseName, disttaeEngine, taeHandler, rpcAgent, t)

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

	dbHandler, dbEntry, txnOp := testutil.CreateDatabase(ctx, databaseName, disttaeEngine, taeHandler, rpcAgent, t)
	databaseId = dbEntry.GetID()

	schema := catalog2.MockSchemaAll(10, 0)
	bat := catalog2.MockBatch(schema, 10)
	schema.Name = tableName

	var err error

	tblHandler, _ := testutil.CreateTable(ctx, schema, dbHandler, dbEntry, rpcAgent, disttaeEngine.Now(), t)
	tableId = tblHandler.GetTableID(ctx)

	err = disttaeEngine.Engine.PushClient().TryToSubscribeTable(ctx, databaseId, tableId)
	require.Nil(t, err)

	err = txnOp.UpdateSnapshot(ctx, disttaeEngine.Now())
	require.Nil(t, err)

	rpcAgent.Insert(ctx, accountId, tblHandler, databaseName, bat, mp, txnOp.SnapshotTS())

	rows, err := disttaeEngine.CountStar(ctx, uint64(databaseId), uint64(tableId))
	require.Nil(t, err)
	require.Equal(t, rows, uint32(10))
}
