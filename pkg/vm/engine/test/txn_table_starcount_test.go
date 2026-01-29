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
	"reflect"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/stretchr/testify/require"
)

func TestStarCountBasic(t *testing.T) {
	catalog.SetupDefines("")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()

	// Create database
	txn, err := disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Now())
	require.NoError(t, err)

	err = disttaeEngine.Engine.Create(ctx, "testdb", txn)
	require.NoError(t, err)

	db, err := disttaeEngine.Engine.Database(ctx, "testdb", txn)
	require.NoError(t, err)

	// Create table
	schema := catalog2.MockSchemaAll(3, 0)
	schema.Name = "test_table"
	defs, err := testutil.EngineTableDefBySchema(schema)
	require.NoError(t, err)

	err = db.Create(ctx, "test_table", defs)
	require.NoError(t, err)

	err = txn.Commit(ctx)
	require.NoError(t, err)

	// Test 1: Empty table
	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Now())
	require.NoError(t, err)

	db, err = disttaeEngine.Engine.Database(ctx, "testdb", txn)
	require.NoError(t, err)

	rel, err := db.Relation(ctx, "test_table", nil)
	require.NoError(t, err)

	count, err := rel.StarCount(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(0), count, "Empty table should have 0 rows")

	err = txn.Commit(ctx)
	require.NoError(t, err)

	// Test 2: Insert and count
	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Now())
	require.NoError(t, err)

	db, err = disttaeEngine.Engine.Database(ctx, "testdb", txn)
	require.NoError(t, err)

	rel, err = db.Relation(ctx, "test_table", nil)
	require.NoError(t, err)

	// Insert 100 rows
	bat := catalog2.MockBatch(schema, 100)
	err = rel.Write(ctx, containers.ToCNBatch(bat))
	require.NoError(t, err)

	err = txn.Commit(ctx)
	require.NoError(t, err)

	// Test 2: Count after commit
	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Now())
	require.NoError(t, err)

	db, err = disttaeEngine.Engine.Database(ctx, "testdb", txn)
	require.NoError(t, err)

	rel, err = db.Relation(ctx, "test_table", nil)
	require.NoError(t, err)

	count, err = rel.StarCount(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(100), count, "Should count committed rows")

	err = txn.Commit(ctx)
	require.NoError(t, err)
}

func TestStarCountWithUncommittedInserts(t *testing.T) {
	catalog.SetupDefines("")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()

	// Setup: Create database and table with 100 rows
	txn, err := disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Now())
	require.NoError(t, err)

	err = disttaeEngine.Engine.Create(ctx, "testdb", txn)
	require.NoError(t, err)

	db, err := disttaeEngine.Engine.Database(ctx, "testdb", txn)
	require.NoError(t, err)

	schema := catalog2.MockSchemaAll(3, -1) // -1 means no primary key
	schema.Name = "test_table"
	defs, err := testutil.EngineTableDefBySchema(schema)
	require.NoError(t, err)

	err = db.Create(ctx, "test_table", defs)
	require.NoError(t, err)

	rel, err := db.Relation(ctx, "test_table", nil)
	require.NoError(t, err)

	bat := catalog2.MockBatch(schema, 100)
	err = rel.Write(ctx, containers.ToCNBatch(bat))
	require.NoError(t, err)

	err = txn.Commit(ctx)
	require.NoError(t, err)

	// Test: Add uncommitted inserts
	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Now())
	require.NoError(t, err)

	db, err = disttaeEngine.Engine.Database(ctx, "testdb", txn)
	require.NoError(t, err)

	rel, err = db.Relation(ctx, "test_table", nil)
	require.NoError(t, err)

	// Insert 50 more rows (uncommitted) - no PK so no conflict
	bat = catalog2.MockBatch(schema, 50)
	err = rel.Write(ctx, containers.ToCNBatch(bat))
	require.NoError(t, err)

	// Count should include uncommitted inserts
	count, err := rel.StarCount(ctx)
	require.NoError(t, err)
	t.Logf("StarCount returned: %d, expected: 150 (100 committed + 50 uncommitted)", count)
	require.Equal(t, uint64(150), count, "Should count 100 committed + 50 uncommitted")

	err = txn.Commit(ctx)
	require.NoError(t, err)
}

func TestStarCountReadonly(t *testing.T) {
	catalog.SetupDefines("")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()

	// Setup: Create database and table with 100 rows
	txn, err := disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Now())
	require.NoError(t, err)

	err = disttaeEngine.Engine.Create(ctx, "testdb", txn)
	require.NoError(t, err)

	db, err := disttaeEngine.Engine.Database(ctx, "testdb", txn)
	require.NoError(t, err)

	schema := catalog2.MockSchemaAll(3, 0)
	schema.Name = "test_table"
	defs, err := testutil.EngineTableDefBySchema(schema)
	require.NoError(t, err)

	err = db.Create(ctx, "test_table", defs)
	require.NoError(t, err)

	rel, err := db.Relation(ctx, "test_table", nil)
	require.NoError(t, err)

	bat := catalog2.MockBatch(schema, 100)
	err = rel.Write(ctx, containers.ToCNBatch(bat))
	require.NoError(t, err)

	err = txn.Commit(ctx)
	require.NoError(t, err)

	// Test: Readonly transaction (use snapshot)
	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Now())
	require.NoError(t, err)

	db, err = disttaeEngine.Engine.Database(ctx, "testdb", txn)
	require.NoError(t, err)

	rel, err = db.Relation(ctx, "test_table", nil)
	require.NoError(t, err)

	// Readonly transaction should only see committed rows
	count, err := rel.StarCount(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(100), count, "Readonly should only count committed rows")

	err = txn.Commit(ctx)
	require.NoError(t, err)
}

// TestStarCountWithPersistedInserts tests StarCount with persisted uncommitted inserts
func TestStarCountWithPersistedInserts(t *testing.T) {
	catalog.SetupDefines("")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)

	// Create engine with very small workspace threshold to trigger persist
	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(
		ctx,
		testutil.TestOptions{
			DisttaeOptions: []testutil.TestDisttaeEngineOptions{
				testutil.WithDisttaeEngineWriteWorkspaceThreshold(1024), // 1KB threshold
			},
		},
		t,
	)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()

	// Setup: Create database and table with 100 rows
	txn, err := disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Now())
	require.NoError(t, err)

	err = disttaeEngine.Engine.Create(ctx, "testdb", txn)
	require.NoError(t, err)

	db, err := disttaeEngine.Engine.Database(ctx, "testdb", txn)
	require.NoError(t, err)

	schema := catalog2.MockSchemaAll(3, -1) // No PK
	schema.Name = "test_table"
	defs, err := testutil.EngineTableDefBySchema(schema)
	require.NoError(t, err)

	err = db.Create(ctx, "test_table", defs)
	require.NoError(t, err)

	rel, err := db.Relation(ctx, "test_table", nil)
	require.NoError(t, err)

	bat := catalog2.MockBatch(schema, 100)
	err = rel.Write(ctx, containers.ToCNBatch(bat))
	require.NoError(t, err)

	err = txn.Commit(ctx)
	require.NoError(t, err)

	// Test: Add uncommitted inserts that will be persisted
	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Now())
	require.NoError(t, err)

	db, err = disttaeEngine.Engine.Database(ctx, "testdb", txn)
	require.NoError(t, err)

	rel, err = db.Relation(ctx, "test_table", nil)
	require.NoError(t, err)

	// Insert enough data to trigger persist (> 1KB)
	// Each row is ~100 bytes, so 20 rows should be enough
	for i := 0; i < 5; i++ {
		bat = catalog2.MockBatch(schema, 20)
		err = rel.Write(ctx, containers.ToCNBatch(bat))
		require.NoError(t, err)
	}

	// Count should include both committed and persisted uncommitted inserts
	count, err := rel.StarCount(ctx)
	require.NoError(t, err)
	t.Logf("StarCount returned: %d, expected: 200 (100 committed + 100 persisted uncommitted)", count)
	require.Equal(t, uint64(200), count, "Should count 100 committed + 100 persisted uncommitted")

	err = txn.Commit(ctx)
	require.NoError(t, err)
}

// TestStarCountReadonlyLarge tests readonly transaction with large dataset
func TestStarCountReadonlyLarge(t *testing.T) {
	catalog.SetupDefines("")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute*2)
	defer cancel()

	// Create database and table
	txn, err := disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Now())
	require.NoError(t, err)

	err = disttaeEngine.Engine.Create(ctx, "testdb", txn)
	require.NoError(t, err)

	db, err := disttaeEngine.Engine.Database(ctx, "testdb", txn)
	require.NoError(t, err)

	schema := catalog2.MockSchemaAll(3, -1)
	schema.Name = "test_table"
	defs, err := testutil.EngineTableDefBySchema(schema)
	require.NoError(t, err)

	err = db.Create(ctx, "test_table", defs)
	require.NoError(t, err)

	rel, err := db.Relation(ctx, "test_table", nil)
	require.NoError(t, err)

	// Insert 10k rows (reduced from 1M to keep test fast)
	totalRows := 10000
	batchSize := 1000
	for i := 0; i < totalRows/batchSize; i++ {
		bat := catalog2.MockBatch(schema, batchSize)
		err = rel.Write(ctx, containers.ToCNBatch(bat))
		require.NoError(t, err)
	}

	err = txn.Commit(ctx)
	require.NoError(t, err)

	// Readonly transaction
	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Now())
	require.NoError(t, err)

	db, err = disttaeEngine.Engine.Database(ctx, "testdb", txn)
	require.NoError(t, err)

	rel, err = db.Relation(ctx, "test_table", nil)
	require.NoError(t, err)

	count, err := rel.StarCount(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(totalRows), count)

	err = txn.Commit(ctx)
	require.NoError(t, err)
}

// TestStarCountInMemoryEmpty tests in-memory inserts on empty table
func TestStarCountInMemoryEmpty(t *testing.T) {
	catalog.SetupDefines("")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()

	// Create empty table
	txn, err := disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Now())
	require.NoError(t, err)

	err = disttaeEngine.Engine.Create(ctx, "testdb", txn)
	require.NoError(t, err)

	db, err := disttaeEngine.Engine.Database(ctx, "testdb", txn)
	require.NoError(t, err)

	schema := catalog2.MockSchemaAll(3, -1)
	schema.Name = "test_table"
	defs, err := testutil.EngineTableDefBySchema(schema)
	require.NoError(t, err)

	err = db.Create(ctx, "test_table", defs)
	require.NoError(t, err)

	err = txn.Commit(ctx)
	require.NoError(t, err)

	// Insert 50 rows on empty table
	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Now())
	require.NoError(t, err)

	db, err = disttaeEngine.Engine.Database(ctx, "testdb", txn)
	require.NoError(t, err)

	rel, err := db.Relation(ctx, "test_table", nil)
	require.NoError(t, err)

	bat := catalog2.MockBatch(schema, 50)
	err = rel.Write(ctx, containers.ToCNBatch(bat))
	require.NoError(t, err)

	count, err := rel.StarCount(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(50), count, "Empty table + 50 uncommitted = 50")

	err = txn.Commit(ctx)
	require.NoError(t, err)
}

// TestStarCountInMemoryLarge tests in-memory inserts with large dataset
func TestStarCountInMemoryLarge(t *testing.T) {
	catalog.SetupDefines("")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute*2)
	defer cancel()

	// Create table with 10k rows
	txn, err := disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Now())
	require.NoError(t, err)

	err = disttaeEngine.Engine.Create(ctx, "testdb", txn)
	require.NoError(t, err)

	db, err := disttaeEngine.Engine.Database(ctx, "testdb", txn)
	require.NoError(t, err)

	schema := catalog2.MockSchemaAll(3, -1)
	schema.Name = "test_table"
	defs, err := testutil.EngineTableDefBySchema(schema)
	require.NoError(t, err)

	err = db.Create(ctx, "test_table", defs)
	require.NoError(t, err)

	rel, err := db.Relation(ctx, "test_table", nil)
	require.NoError(t, err)

	// Insert 10k rows
	totalRows := 10000
	batchSize := 1000
	for i := 0; i < totalRows/batchSize; i++ {
		bat := catalog2.MockBatch(schema, batchSize)
		err = rel.Write(ctx, containers.ToCNBatch(bat))
		require.NoError(t, err)
	}

	err = txn.Commit(ctx)
	require.NoError(t, err)

	// Add 1k uncommitted rows
	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Now())
	require.NoError(t, err)

	db, err = disttaeEngine.Engine.Database(ctx, "testdb", txn)
	require.NoError(t, err)

	rel, err = db.Relation(ctx, "test_table", nil)
	require.NoError(t, err)

	uncommittedRows := 1000
	bat := catalog2.MockBatch(schema, uncommittedRows)
	err = rel.Write(ctx, containers.ToCNBatch(bat))
	require.NoError(t, err)

	count, err := rel.StarCount(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(totalRows+uncommittedRows), count, "10k committed + 1k uncommitted = 11k")

	err = txn.Commit(ctx)
	require.NoError(t, err)
}

// TestStarCountMixedInMemoryAndPersisted tests mixed in-memory and persisted inserts
func TestStarCountMixedInMemoryAndPersisted(t *testing.T) {
	catalog.SetupDefines("")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()

	// Create table with 100 rows
	txn, err := disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Now())
	require.NoError(t, err)

	err = disttaeEngine.Engine.Create(ctx, "testdb", txn)
	require.NoError(t, err)

	db, err := disttaeEngine.Engine.Database(ctx, "testdb", txn)
	require.NoError(t, err)

	schema := catalog2.MockSchemaAll(3, -1)
	schema.Name = "test_table"
	defs, err := testutil.EngineTableDefBySchema(schema)
	require.NoError(t, err)

	err = db.Create(ctx, "test_table", defs)
	require.NoError(t, err)

	rel, err := db.Relation(ctx, "test_table", nil)
	require.NoError(t, err)

	bat := catalog2.MockBatch(schema, 100)
	err = rel.Write(ctx, containers.ToCNBatch(bat))
	require.NoError(t, err)

	err = txn.Commit(ctx)
	require.NoError(t, err)

	// Start new transaction with fault injection enabled
	txn, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Now())
	require.NoError(t, err)

	// Enable fault injection to force workspace flush
	fault.Enable()
	err = fault.AddFaultPoint(ctx, objectio.FJ_CNWorkspaceForceFlush, ":::", "return", 0, "", false)
	require.NoError(t, err)

	db, err = disttaeEngine.Engine.Database(ctx, "testdb", txn)
	require.NoError(t, err)

	rel, err = db.Relation(ctx, "test_table", nil)
	require.NoError(t, err)

	// Write batches with fault injection enabled - these will be persisted
	for i := 0; i < 3; i++ {
		bat = catalog2.MockBatch(schema, 20)
		err = rel.Write(ctx, containers.ToCNBatch(bat))
		require.NoError(t, err)
	}

	// Disable fault injection
	fault.RemoveFaultPoint(ctx, objectio.FJ_CNWorkspaceForceFlush)
	fault.Disable()

	// Write more batches without fault injection - these will stay in-memory
	for i := 0; i < 2; i++ {
		bat = catalog2.MockBatch(schema, 25)
		err = rel.Write(ctx, containers.ToCNBatch(bat))
		require.NoError(t, err)
	}

	// Total: 100 committed + 60 persisted + 50 in-memory = 210
	count, err := rel.StarCount(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(210), count, "Should count all rows including mixed in-memory and persisted")

	// Verify workspace state: should have both in-memory and persisted writes
	inMemory, persisted := countWorkspaceWrites(txn)
	t.Logf("Workspace state: in-memory writes=%d, persisted writes=%d", inMemory, persisted)
	require.Greater(t, inMemory, 0, "Should have in-memory writes")
	require.Greater(t, persisted, 0, "Should have persisted writes")

	err = txn.Commit(ctx)
	require.NoError(t, err)
}

// countWorkspaceWrites counts in-memory and persisted writes in the transaction workspace
func countWorkspaceWrites(txn client.TxnOperator) (inMemory, persisted int) {
	// Access the internal transaction workspace
	workspace := txn.GetWorkspace()
	dtxn, ok := workspace.(*disttae.Transaction)
	if !ok {
		return 0, 0
	}

	// Use reflection to access the writes slice since there's no public API
	// to iterate all writes across all tables
	txnValue := reflect.ValueOf(dtxn).Elem()
	writesField := txnValue.FieldByName("writes")
	if !writesField.IsValid() {
		return 0, 0
	}

	// Count all INSERT writes
	for i := 0; i < writesField.Len(); i++ {
		entry := writesField.Index(i)

		// Get typ field
		typField := entry.FieldByName("typ")
		if !typField.IsValid() || typField.Int() != int64(disttae.INSERT) {
			continue
		}

		// Get fileName field
		fileNameField := entry.FieldByName("fileName")
		if !fileNameField.IsValid() {
			continue
		}

		// Get bat field
		batField := entry.FieldByName("bat")
		if !batField.IsValid() {
			continue
		}

		if fileNameField.String() != "" {
			persisted++
		} else if !batField.IsNil() {
			inMemory++
		}
	}

	return inMemory, persisted
}
