// Copyright 2025 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/stretchr/testify/require"
)

func TestGetFlushTS(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountID = catalog.System_Account
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountID)
	ctxWithTimeout, cancelTimeout := context.WithTimeout(ctx, time.Minute*5)
	defer cancelTimeout()

	// Start cluster
	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	// Register mock auto increment service
	mockIncrService := NewMockAutoIncrementService("")
	incrservice.SetAutoIncrementServiceByID("", mockIncrService)
	defer mockIncrService.Close()

	// Step 1: Create database and table
	dbName := "test_db"
	tableName := "test_table"
	schema := catalog2.MockSchemaAll(4, 3)
	schema.Name = tableName

	// Create database and table
	txn, err := disttaeEngine.NewTxnOperator(ctxWithTimeout, disttaeEngine.Now())
	require.NoError(t, err)

	err = disttaeEngine.Engine.Create(ctxWithTimeout, dbName, txn)
	require.NoError(t, err)

	db, err := disttaeEngine.Engine.Database(ctxWithTimeout, dbName, txn)
	require.NoError(t, err)

	defs, err := testutil.EngineTableDefBySchema(schema)
	require.NoError(t, err)

	err = db.Create(ctxWithTimeout, tableName, defs)
	require.NoError(t, err)

	rel, err := db.Relation(ctxWithTimeout, tableName, nil)
	require.NoError(t, err)

	// Insert data into table
	bat := catalog2.MockBatch(schema, 10)
	defer bat.Close()
	bats := bat.Split(10)
	err = rel.Write(ctxWithTimeout, containers.ToCNBatch(bats[0]))
	require.NoError(t, err)

	err = txn.Commit(ctxWithTimeout)
	require.NoError(t, err)
	insertTS1 := types.TimestampToTS(txn.Txn().CommitTS)

	txn2, err := disttaeEngine.NewTxnOperator(ctxWithTimeout, disttaeEngine.Now())
	require.NoError(t, err)

	db2, err := disttaeEngine.Engine.Database(ctxWithTimeout, dbName, txn2)
	require.NoError(t, err)

	rel2, err := db2.Relation(ctxWithTimeout, tableName, nil)
	require.NoError(t, err)

	flushTS, err := rel2.GetFlushTS(ctxWithTimeout)
	require.NoError(t, err)

	expectedFlushTS := insertTS1.Prev()
	require.True(t, flushTS.EQ(&expectedFlushTS), "expect %v, get %v", expectedFlushTS.ToString(), flushTS.ToString())

	err = txn2.Commit(ctxWithTimeout)
	require.NoError(t, err)

	// Step 2: Force checkpoint to flush data
	nowTS := types.TimestampToTS(disttaeEngine.Now())
	err = taeHandler.GetDB().ForceCheckpoint(ctxWithTimeout, nowTS)
	require.NoError(t, err)

	// Step 3: Get flush TS from relation
	// Need to get relation again after checkpoint
	txn3, err := disttaeEngine.NewTxnOperator(ctxWithTimeout, disttaeEngine.Now())
	require.NoError(t, err)

	db3, err := disttaeEngine.Engine.Database(ctxWithTimeout, dbName, txn3)
	require.NoError(t, err)

	rel3, err := db3.Relation(ctxWithTimeout, tableName, nil)
	require.NoError(t, err)

	flushTS, err = rel3.GetFlushTS(ctxWithTimeout)
	require.NoError(t, err)

	maxTS := types.MaxTs()
	require.True(t, flushTS.GE(&maxTS), "flush TS should be greater than or equal to checkpoint TS")

	err = txn3.Commit(ctxWithTimeout)
	require.NoError(t, err)

	// Get relation from CN, insert some data, then get flush TS
	txn4, err := disttaeEngine.NewTxnOperator(ctxWithTimeout, disttaeEngine.Now())
	require.NoError(t, err)

	db4, err := disttaeEngine.Engine.Database(ctxWithTimeout, dbName, txn4)
	require.NoError(t, err)

	rel4, err := db4.Relation(ctxWithTimeout, tableName, nil)
	require.NoError(t, err)

	// Insert some data into table
	err = rel4.Write(ctxWithTimeout, containers.ToCNBatch(bats[1]))
	require.NoError(t, err)

	err = txn4.Commit(ctxWithTimeout)
	require.NoError(t, err)

	insertTS2 := types.TimestampToTS(txn4.Txn().CommitTS)

	// Get flush TS after inserting data
	txn5, err := disttaeEngine.NewTxnOperator(ctxWithTimeout, disttaeEngine.Now())
	require.NoError(t, err)

	db5, err := disttaeEngine.Engine.Database(ctxWithTimeout, dbName, txn5)
	require.NoError(t, err)

	rel5, err := db5.Relation(ctxWithTimeout, tableName, nil)
	require.NoError(t, err)

	cnFlushTS, err := rel5.GetFlushTS(ctxWithTimeout)
	require.NoError(t, err)

	expectedFlushTS = insertTS2.Prev()
	// Verify flush TS is equal to expected data flush TS
	require.True(t, cnFlushTS.EQ(&expectedFlushTS), "expect %v, get %v", expectedFlushTS.ToString(), cnFlushTS.ToString())

	err = txn5.Commit(ctxWithTimeout)
	require.NoError(t, err)
}
