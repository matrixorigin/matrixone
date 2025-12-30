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
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/publication"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	testutil2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/stretchr/testify/require"
)

// checkpointUTHelper implements publication.UTHelper for unit testing
// It performs force checkpoint when snapshot is created
type checkpointUTHelper struct {
	taeHandler    *testutil.TestTxnStorage
	disttaeEngine *testutil.TestDisttaeEngine
	checkpointC   chan struct{}
}

func (h *checkpointUTHelper) OnSnapshotCreated(ctx context.Context, snapshotName string, snapshotTS types.TS) error {
	// Perform force checkpoint
	err := h.taeHandler.GetDB().ForceCheckpoint(ctx, types.TimestampToTS(h.disttaeEngine.Now()))
	if err != nil {
		return err
	}
	// Start a goroutine to checkpoint every 100ms until ExecuteIteration completes
	if h.checkpointC != nil {
		go func() {
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-h.checkpointC:
					// ExecuteIteration completed, stop checkpointing
					return
				case <-ticker.C:
					// Execute checkpoint every 100ms
					_ = h.taeHandler.GetDB().ForceCheckpoint(ctx, types.TimestampToTS(h.disttaeEngine.Now()))
				}
			}
		}()
	}
	return nil
}

func TestCheckIterationStatus(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId = catalog.System_Account
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	// Start cluster only once
	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	// Register mock auto increment service
	mockIncrService := NewMockAutoIncrementService("mock-cn-uuid")
	incrservice.SetAutoIncrementServiceByID("", mockIncrService)
	defer mockIncrService.Close()

	// Create mo_indexes table using DDL from frontend/predefine (only once)
	err := exec_sql(disttaeEngine, ctxWithTimeout, frontend.MoCatalogMoIndexesDDL)
	require.NoError(t, err)

	// Create mo_ccpr_log table using system account context (only once)
	// mo_ccpr_log is a system table, so we must use system account
	systemCtx := context.WithValue(ctxWithTimeout, defines.TenantIDKey{}, catalog.System_Account)
	err = exec_sql(disttaeEngine, systemCtx, frontend.MoCatalogMoCcprLogDDL)
	require.NoError(t, err)

	// Create InternalSQLExecutor (only once)
	// Pass nil for txnClient - transactions will be managed externally via ExecTxn
	executor, err := publication.NewInternalSQLExecutor("", nil, nil, accountId)
	require.NoError(t, err)
	defer executor.Close()

	// Define test cases
	testCases := []struct {
		name           string
		taskID         uint64
		cnUUID         string
		expectedCNUUID string
		iterationLSN   uint64
		expectedLSN    uint64
		iterationState int8
		shouldInsert   bool
		expectError    bool
		errorContains  string
	}{
		{
			name:           "Success",
			taskID:         1,
			cnUUID:         "test-cn-uuid-123",
			expectedCNUUID: "test-cn-uuid-123",
			iterationLSN:   1000,
			expectedLSN:    1000,
			iterationState: publication.IterationStateCompleted,
			shouldInsert:   true,
			expectError:    false,
		},
		{
			name:           "WrongCNUUID",
			taskID:         2,
			cnUUID:         "test-cn-uuid-123",
			expectedCNUUID: "wrong-cn-uuid",
			iterationLSN:   1000,
			expectedLSN:    1000,
			iterationState: publication.IterationStateCompleted,
			shouldInsert:   true,
			expectError:    true,
			errorContains:  "cn_uuid mismatch",
		},
		{
			name:           "WrongIterationLSN",
			taskID:         3,
			cnUUID:         "test-cn-uuid-123",
			expectedCNUUID: "test-cn-uuid-123",
			iterationLSN:   1000,
			expectedLSN:    2000,
			iterationState: publication.IterationStateCompleted,
			shouldInsert:   true,
			expectError:    true,
			errorContains:  "iteration_lsn mismatch",
		},
		{
			name:           "NotCompleted",
			taskID:         4,
			cnUUID:         "test-cn-uuid-123",
			expectedCNUUID: "test-cn-uuid-123",
			iterationLSN:   1000,
			expectedLSN:    1000,
			iterationState: publication.IterationStateRunning,
			shouldInsert:   true,
			expectError:    true,
			errorContains:  "iteration_state is not completed",
		},
		{
			name:           "NoRows",
			taskID:         999,
			cnUUID:         "",
			expectedCNUUID: "test-cn-uuid-123",
			iterationLSN:   0,
			expectedLSN:    1000,
			iterationState: 0,
			shouldInsert:   false,
			expectError:    true,
			errorContains:  "no rows returned",
		},
	}

	// Run test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Insert test data if needed
			if tc.shouldInsert {
				insertSQL := fmt.Sprintf(
					`INSERT INTO mo_catalog.mo_ccpr_log (
					task_id, 
					subscription_name, 
					sync_level, 
					account_id,
					db_name, 
					table_name, 
					upstream_conn, 
					sync_config, 
					state, 
					iteration_state, 
					iteration_lsn, 
					cn_uuid
				) VALUES (
					%d, 
					'test_subscription', 
					'full', 
					%d,
					'test_db', 
					'test_table', 
					'test_conn', 
					'{}', 
					0, 
					%d, 
					%d, 
					'%s'
				)`,
					tc.taskID,
					catalog.System_Account,
					tc.iterationState,
					tc.iterationLSN,
					tc.cnUUID,
				)

				// mo_ccpr_log is a system table, so we must use system account
				systemCtx := context.WithValue(ctxWithTimeout, defines.TenantIDKey{}, catalog.System_Account)
				err := exec_sql(disttaeEngine, systemCtx, insertSQL)
				require.NoError(t, err)
			}

			// Test CheckIterationStatus
			err := publication.CheckIterationStatus(
				ctxWithTimeout,
				executor,
				tc.taskID,
				tc.expectedCNUUID,
				tc.expectedLSN,
			)

			if tc.expectError {
				require.Error(t, err)
				if tc.errorContains != "" {
					require.Contains(t, err.Error(), tc.errorContains)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestExecuteIteration1(t *testing.T) {
	catalog.SetupDefines("")

	var (
		srcAccountID  = catalog.System_Account
		destAccountID = uint32(2)
		cnUUID        = ""
	)

	// Setup source account context
	srcCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srcCtx = context.WithValue(srcCtx, defines.TenantIDKey{}, srcAccountID)
	srcCtxWithTimeout, cancelSrc := context.WithTimeout(srcCtx, time.Minute*5)
	defer cancelSrc()

	// Setup destination account context
	destCtx, cancelDest := context.WithCancel(context.Background())
	defer cancelDest()
	destCtx = context.WithValue(destCtx, defines.TenantIDKey{}, destAccountID)
	destCtxWithTimeout, cancelDestTimeout := context.WithTimeout(destCtx, time.Minute*5)
	defer cancelDestTimeout()

	// Create engines with source account context
	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(srcCtx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(srcCtx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	// Register mock auto increment service
	mockIncrService := NewMockAutoIncrementService(cnUUID)
	incrservice.SetAutoIncrementServiceByID("", mockIncrService)
	defer mockIncrService.Close()

	// Create mo_indexes table for source account
	err := exec_sql(disttaeEngine, srcCtxWithTimeout, frontend.MoCatalogMoIndexesDDL)
	require.NoError(t, err)

	// Create mo_ccpr_log table using system account context
	systemCtx := context.WithValue(srcCtxWithTimeout, defines.TenantIDKey{}, catalog.System_Account)
	err = exec_sql(disttaeEngine, systemCtx, frontend.MoCatalogMoCcprLogDDL)
	require.NoError(t, err)

	// Create mo_snapshots table for source account
	moSnapshotsDDL := frontend.MoCatalogMoSnapshotsDDL
	err = exec_sql(disttaeEngine, srcCtxWithTimeout, moSnapshotsDDL)
	require.NoError(t, err)

	// Create system tables for destination account
	// These tables are needed when creating tables in the destination account
	err = exec_sql(disttaeEngine, destCtxWithTimeout, frontend.MoCatalogMoIndexesDDL)
	require.NoError(t, err)

	err = exec_sql(disttaeEngine, destCtxWithTimeout, frontend.MoCatalogMoTablePartitionsDDL)
	require.NoError(t, err)

	err = exec_sql(disttaeEngine, destCtxWithTimeout, frontend.MoCatalogMoAutoIncrTableDDL)
	require.NoError(t, err)

	err = exec_sql(disttaeEngine, destCtxWithTimeout, frontend.MoCatalogMoForeignKeysDDL)
	require.NoError(t, err)

	err = exec_sql(disttaeEngine, destCtxWithTimeout, frontend.MoCatalogMoSnapshotsDDL)
	require.NoError(t, err)

	// Step 1: Create source database and table in source account
	srcDBName := "src_db"
	srcTableName := "src_table"
	schema := catalog2.MockSchemaAll(4, 3)
	schema.Name = srcTableName

	// Create database and table in source account
	txn, err := disttaeEngine.NewTxnOperator(srcCtxWithTimeout, disttaeEngine.Now())
	require.NoError(t, err)

	err = disttaeEngine.Engine.Create(srcCtxWithTimeout, srcDBName, txn)
	require.NoError(t, err)

	db, err := disttaeEngine.Engine.Database(srcCtxWithTimeout, srcDBName, txn)
	require.NoError(t, err)

	defs, err := testutil.EngineTableDefBySchema(schema)
	require.NoError(t, err)

	err = db.Create(srcCtxWithTimeout, srcTableName, defs)
	require.NoError(t, err)

	rel, err := db.Relation(srcCtxWithTimeout, srcTableName, nil)
	require.NoError(t, err)

	// Insert data into source table
	// Create a batch with 10 rows
	bat := catalog2.MockBatch(schema, 10)
	defer bat.Close()
	err = rel.Write(srcCtxWithTimeout, containers.ToCNBatch(bat))
	require.NoError(t, err)

	err = txn.Commit(srcCtxWithTimeout)
	require.NoError(t, err)

	// Note: We do NOT force checkpoint here - that will be done in parallel

	// Step 2: Write mo_ccpr_log table in destination account context
	taskID := uint64(1)
	iterationLSN := uint64(1)
	subscriptionName := "test_subscription"
	insertSQL := fmt.Sprintf(
		`INSERT INTO mo_catalog.mo_ccpr_log (
			task_id, 
			subscription_name, 
			sync_level, 
			account_id,
			db_name, 
			table_name, 
			upstream_conn, 
			sync_config, 
			iteration_state, 
			iteration_lsn, 
			cn_uuid
		) VALUES (
			%d, 
			'%s', 
			'table', 
			%d,
			'%s', 
			'%s', 
			'%s', 
			'{}', 
			%d, 
			%d, 
			'%s'
		)`,
		taskID,
		subscriptionName,
		destAccountID,
		srcDBName,
		srcTableName,
		fmt.Sprintf("%s:%d", publication.InternalSQLExecutorType, srcAccountID),
		publication.IterationStatePending,
		iterationLSN,
		cnUUID,
	)

	// Write mo_ccpr_log using system account context
	err = exec_sql(disttaeEngine, systemCtx, insertSQL)
	require.NoError(t, err)

	// Step 3: Create upstream SQL helper factory
	upstreamSQLHelperFactory := func(
		txnOp client.TxnOperator,
		engine engine.Engine,
		accountID uint32,
		exec executor.SQLExecutor,
		txnClient client.TxnClient,
	) publication.UpstreamSQLHelper {
		return NewUpstreamSQLHelper(txnOp, engine, accountID, exec, txnClient)
	}

	// Create mpool for ExecuteIteration
	mp, err := mpool.NewMPool("test_execute_iteration2", 0, mpool.NoFixed)
	require.NoError(t, err)

	// Step 4: Create UTHelper for checkpointing
	checkpointDone := make(chan struct{}, 1)
	utHelper := &checkpointUTHelper{
		taeHandler:    taeHandler,
		disttaeEngine: disttaeEngine,
		checkpointC:   checkpointDone,
	}

	// Execute ExecuteIteration with UTHelper
	err = publication.ExecuteIteration(
		srcCtxWithTimeout,
		cnUUID,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		taskID,
		iterationLSN,
		upstreamSQLHelperFactory,
		mp,
		utHelper,
		100*time.Millisecond, // snapshotFlushInterval for test
	)

	// Signal checkpoint goroutine to stop
	close(checkpointDone)

	// Check errors
	require.NoError(t, err, "ExecuteIteration should complete successfully")

	// Step 5: Verify that the iteration state was updated
	// Query mo_ccpr_log to check iteration_state using system account
	querySQL := fmt.Sprintf(
		`SELECT iteration_state, iteration_lsn FROM mo_catalog.mo_ccpr_log WHERE task_id = %d`,
		taskID,
	)

	v, ok := runtime.ServiceRuntime("").GetGlobalVariables(runtime.InternalSQLExecutor)
	require.True(t, ok)
	exec := v.(executor.SQLExecutor)

	querySystemCtx := context.WithValue(destCtxWithTimeout, defines.TenantIDKey{}, catalog.System_Account)
	txn, err = disttaeEngine.NewTxnOperator(querySystemCtx, disttaeEngine.Now())
	require.NoError(t, err)

	res, err := exec.Exec(querySystemCtx, querySQL, executor.Options{}.WithTxn(txn))
	require.NoError(t, err)
	defer res.Close()

	// Check that iteration_state is completed
	var found bool
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		require.Equal(t, 1, rows)
		require.Equal(t, 2, len(cols))

		state := vector.GetFixedAtWithTypeCheck[int8](cols[0], 0)
		lsn := vector.GetFixedAtWithTypeCheck[int64](cols[1], 0)

		require.Equal(t, publication.IterationStateCompleted, state)
		require.Equal(t, int64(iterationLSN), lsn)
		found = true
		return true
	})
	require.True(t, found, "should find the updated iteration record")

	err = txn.Commit(querySystemCtx)
	require.NoError(t, err)

	// Step 6: Check destination table row count
	// The destination table should have 10 rows (same as source table)
	checkRowCountSQL := fmt.Sprintf(`SELECT COUNT(*) FROM %s.%s`, srcDBName, srcTableName)
	queryDestCtx := context.WithValue(destCtxWithTimeout, defines.TenantIDKey{}, destAccountID)
	txn, err = disttaeEngine.NewTxnOperator(queryDestCtx, disttaeEngine.Now())
	require.NoError(t, err)

	rowCountRes, err := exec.Exec(queryDestCtx, checkRowCountSQL, executor.Options{}.WithTxn(txn))
	require.NoError(t, err)
	defer rowCountRes.Close()

	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))

	var rowCount int64
	rowCountRes.ReadRows(func(rows int, cols []*vector.Vector) bool {
		require.Equal(t, 1, rows)
		require.Equal(t, 1, len(cols))
		rowCount = vector.GetFixedAtWithTypeCheck[int64](cols[0], 0)
		return true
	})
	require.Equal(t, int64(10), rowCount, "destination table should have 10 rows after iteration")

	err = txn.Commit(queryDestCtx)
	require.NoError(t, err)

	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))

	// Step 7: Execute second iteration without inserting new data
	iterationLSN2 := uint64(2)
	updateSQL := fmt.Sprintf(
		`UPDATE mo_catalog.mo_ccpr_log 
		SET iteration_state = %d, iteration_lsn = %d 
		WHERE task_id = %d`,
		publication.IterationStatePending,
		iterationLSN2,
		taskID,
	)

	// Update mo_ccpr_log using system account context
	err = exec_sql(disttaeEngine, systemCtx, updateSQL)
	require.NoError(t, err)

	// Create new checkpoint channel for second iteration
	checkpointDone2 := make(chan struct{}, 1)
	utHelper2 := &checkpointUTHelper{
		taeHandler:    taeHandler,
		disttaeEngine: disttaeEngine,
		checkpointC:   checkpointDone2,
	}

	// Execute second ExecuteIteration
	err = publication.ExecuteIteration(
		srcCtxWithTimeout,
		cnUUID,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		taskID,
		iterationLSN2,
		upstreamSQLHelperFactory,
		mp,
		utHelper2,
		100*time.Millisecond, // snapshotFlushInterval for test
	)

	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	// Signal checkpoint goroutine to stop
	close(checkpointDone2)

	// Check errors
	require.NoError(t, err, "Second ExecuteIteration should complete successfully")

	// Step 8: Verify that the second iteration state was updated
	querySQL2 := fmt.Sprintf(
		`SELECT iteration_state, iteration_lsn FROM mo_catalog.mo_ccpr_log WHERE task_id = %d`,
		taskID,
	)

	querySystemCtx2 := context.WithValue(destCtxWithTimeout, defines.TenantIDKey{}, catalog.System_Account)
	txn2, err := disttaeEngine.NewTxnOperator(querySystemCtx2, disttaeEngine.Now())
	require.NoError(t, err)

	res2, err := exec.Exec(querySystemCtx2, querySQL2, executor.Options{}.WithTxn(txn2))
	require.NoError(t, err)
	defer res2.Close()

	// Check that iteration_state is completed for second iteration
	var found2 bool
	res2.ReadRows(func(rows int, cols []*vector.Vector) bool {
		require.Equal(t, 1, rows)
		require.Equal(t, 2, len(cols))

		state := vector.GetFixedAtWithTypeCheck[int8](cols[0], 0)
		lsn := vector.GetFixedAtWithTypeCheck[int64](cols[1], 0)

		require.Equal(t, publication.IterationStateCompleted, state)
		require.Equal(t, int64(iterationLSN2), lsn)
		found2 = true
		return true
	})
	require.True(t, found2, "should find the updated iteration record for second iteration")

	err = txn2.Commit(querySystemCtx2)
	require.NoError(t, err)

	// Step 9: Check destination table row count after second iteration
	// The destination table should still have 10 rows (no new data inserted)
	checkRowCountSQL2 := fmt.Sprintf(`SELECT COUNT(*) FROM %s.%s`, srcDBName, srcTableName)
	queryDestCtx2 := context.WithValue(destCtxWithTimeout, defines.TenantIDKey{}, destAccountID)
	txn3, err := disttaeEngine.NewTxnOperator(queryDestCtx2, disttaeEngine.Now())
	require.NoError(t, err)

	rowCountRes2, err := exec.Exec(queryDestCtx2, checkRowCountSQL2, executor.Options{}.WithTxn(txn3))
	require.NoError(t, err)
	defer rowCountRes2.Close()

	var rowCount2 int64
	rowCountRes2.ReadRows(func(rows int, cols []*vector.Vector) bool {
		require.Equal(t, 1, rows)
		require.Equal(t, 1, len(cols))
		rowCount2 = vector.GetFixedAtWithTypeCheck[int64](cols[0], 0)
		return true
	})
	require.Equal(t, int64(10), rowCount2, "destination table should still have 10 rows after second iteration")

	err = txn3.Commit(queryDestCtx2)
	require.NoError(t, err)

	// Step 10: Execute third iteration with delete operations
	// Delete some rows from source table
	txn4, taeRel := testutil2.GetRelation(t, srcAccountID, taeHandler.GetDB(), srcDBName, srcTableName)
	// Get primary key column index from schema
	pkIdx := schema.GetPrimaryKey().Idx
	// Delete first 3 rows using primary key filter
	// Get the primary key value from the first row of the batch
	pkVal := bat.Vecs[pkIdx].Get(0)
	filter := handle.NewEQFilter(pkVal)
	err = taeRel.DeleteByFilter(srcCtxWithTimeout, filter)
	require.NoError(t, err)

	// Delete second row
	pkVal2 := bat.Vecs[pkIdx].Get(1)
	filter2 := handle.NewEQFilter(pkVal2)
	err = taeRel.DeleteByFilter(srcCtxWithTimeout, filter2)
	require.NoError(t, err)

	// Delete third row
	pkVal3 := bat.Vecs[pkIdx].Get(2)
	filter3 := handle.NewEQFilter(pkVal3)
	err = taeRel.DeleteByFilter(srcCtxWithTimeout, filter3)
	require.NoError(t, err)

	err = txn4.Commit(srcCtxWithTimeout)
	require.NoError(t, err)

	// Update mo_ccpr_log for third iteration
	iterationLSN3 := uint64(3)
	updateSQL3 := fmt.Sprintf(
		`UPDATE mo_catalog.mo_ccpr_log 
		SET iteration_state = %d, iteration_lsn = %d 
		WHERE task_id = %d`,
		publication.IterationStatePending,
		iterationLSN3,
		taskID,
	)

	// Update mo_ccpr_log using system account context
	err = exec_sql(disttaeEngine, systemCtx, updateSQL3)
	require.NoError(t, err)

	// Create new checkpoint channel for third iteration
	checkpointDone3 := make(chan struct{}, 1)
	utHelper3 := &checkpointUTHelper{
		taeHandler:    taeHandler,
		disttaeEngine: disttaeEngine,
		checkpointC:   checkpointDone3,
	}

	// Execute third ExecuteIteration
	err = publication.ExecuteIteration(
		srcCtxWithTimeout,
		cnUUID,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		taskID,
		iterationLSN3,
		upstreamSQLHelperFactory,
		mp,
		utHelper3,
		100*time.Millisecond, // snapshotFlushInterval for test
	)

	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	// Signal checkpoint goroutine to stop
	close(checkpointDone3)

	// Check errors
	require.NoError(t, err, "Third ExecuteIteration should complete successfully")

	// Step 11: Verify that the third iteration state was updated
	querySQL3 := fmt.Sprintf(
		`SELECT iteration_state, iteration_lsn FROM mo_catalog.mo_ccpr_log WHERE task_id = %d`,
		taskID,
	)

	querySystemCtx3 := context.WithValue(destCtxWithTimeout, defines.TenantIDKey{}, catalog.System_Account)
	txn5, err := disttaeEngine.NewTxnOperator(querySystemCtx3, disttaeEngine.Now())
	require.NoError(t, err)

	res3, err := exec.Exec(querySystemCtx3, querySQL3, executor.Options{}.WithTxn(txn5))
	require.NoError(t, err)
	defer res3.Close()

	// Check that iteration_state is completed for third iteration
	var found3 bool
	res3.ReadRows(func(rows int, cols []*vector.Vector) bool {
		require.Equal(t, 1, rows)
		require.Equal(t, 2, len(cols))

		state := vector.GetFixedAtWithTypeCheck[int8](cols[0], 0)
		lsn := vector.GetFixedAtWithTypeCheck[int64](cols[1], 0)

		require.Equal(t, publication.IterationStateCompleted, state)
		require.Equal(t, int64(iterationLSN3), lsn)
		found3 = true
		return true
	})
	require.True(t, found3, "should find the updated iteration record for third iteration")

	err = txn5.Commit(querySystemCtx3)
	require.NoError(t, err)

	// Step 12: Check destination table row count after third iteration
	// The destination table should have 7 rows (10 - 3 deleted)
	checkRowCountSQL3 := fmt.Sprintf(`SELECT COUNT(*) FROM %s.%s`, srcDBName, srcTableName)
	queryDestCtx3 := context.WithValue(destCtxWithTimeout, defines.TenantIDKey{}, destAccountID)
	txn6, err := disttaeEngine.NewTxnOperator(queryDestCtx3, disttaeEngine.Now())
	require.NoError(t, err)

	rowCountRes3, err := exec.Exec(queryDestCtx3, checkRowCountSQL3, executor.Options{}.WithTxn(txn6))
	require.NoError(t, err)
	defer rowCountRes3.Close()

	var rowCount3 int64
	rowCountRes3.ReadRows(func(rows int, cols []*vector.Vector) bool {
		require.Equal(t, 1, rows)
		require.Equal(t, 1, len(cols))
		rowCount3 = vector.GetFixedAtWithTypeCheck[int64](cols[0], 0)
		return true
	})
	require.Equal(t, int64(7), rowCount3, "destination table should have 7 rows after third iteration (10 - 3 deleted)")

	err = txn6.Commit(queryDestCtx3)
	require.NoError(t, err)
}

func TestExecuteIterationDatabaseLevel(t *testing.T) {
	catalog.SetupDefines("")

	var (
		srcAccountID  = catalog.System_Account
		destAccountID = uint32(2)
		cnUUID        = ""
	)

	// Setup source account context
	srcCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srcCtx = context.WithValue(srcCtx, defines.TenantIDKey{}, srcAccountID)
	srcCtxWithTimeout, cancelSrc := context.WithTimeout(srcCtx, time.Minute*5)
	defer cancelSrc()

	// Setup destination account context
	destCtx, cancelDest := context.WithCancel(context.Background())
	defer cancelDest()
	destCtx = context.WithValue(destCtx, defines.TenantIDKey{}, destAccountID)
	destCtxWithTimeout, cancelDestTimeout := context.WithTimeout(destCtx, time.Minute*5)
	defer cancelDestTimeout()

	// Create engines with source account context
	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(srcCtx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(srcCtx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	// Register mock auto increment service
	mockIncrService := NewMockAutoIncrementService(cnUUID)
	incrservice.SetAutoIncrementServiceByID("", mockIncrService)
	defer mockIncrService.Close()

	// Create mo_indexes table for source account
	err := exec_sql(disttaeEngine, srcCtxWithTimeout, frontend.MoCatalogMoIndexesDDL)
	require.NoError(t, err)

	// Create mo_ccpr_log table using system account context
	systemCtx := context.WithValue(srcCtxWithTimeout, defines.TenantIDKey{}, catalog.System_Account)
	err = exec_sql(disttaeEngine, systemCtx, frontend.MoCatalogMoCcprLogDDL)
	require.NoError(t, err)

	// Create mo_snapshots table for source account
	moSnapshotsDDL := frontend.MoCatalogMoSnapshotsDDL
	err = exec_sql(disttaeEngine, srcCtxWithTimeout, moSnapshotsDDL)
	require.NoError(t, err)

	// Create system tables for destination account
	// These tables are needed when creating tables in the destination account
	err = exec_sql(disttaeEngine, destCtxWithTimeout, frontend.MoCatalogMoIndexesDDL)
	require.NoError(t, err)

	err = exec_sql(disttaeEngine, destCtxWithTimeout, frontend.MoCatalogMoTablePartitionsDDL)
	require.NoError(t, err)

	err = exec_sql(disttaeEngine, destCtxWithTimeout, frontend.MoCatalogMoAutoIncrTableDDL)
	require.NoError(t, err)

	err = exec_sql(disttaeEngine, destCtxWithTimeout, frontend.MoCatalogMoForeignKeysDDL)
	require.NoError(t, err)

	err = exec_sql(disttaeEngine, destCtxWithTimeout, frontend.MoCatalogMoSnapshotsDDL)
	require.NoError(t, err)

	// Step 1: Create source database and multiple tables in source account
	srcDBName := "src_db"
	srcTable1Name := "src_table1"
	srcTable2Name := "src_table2"
	schema1 := catalog2.MockSchemaAll(4, 3)
	schema1.Name = srcTable1Name
	schema2 := catalog2.MockSchemaAll(5, 4)
	schema2.Name = srcTable2Name

	// Create database and tables in source account
	txn, err := disttaeEngine.NewTxnOperator(srcCtxWithTimeout, disttaeEngine.Now())
	require.NoError(t, err)

	err = disttaeEngine.Engine.Create(srcCtxWithTimeout, srcDBName, txn)
	require.NoError(t, err)

	db, err := disttaeEngine.Engine.Database(srcCtxWithTimeout, srcDBName, txn)
	require.NoError(t, err)

	// Create first table
	defs1, err := testutil.EngineTableDefBySchema(schema1)
	require.NoError(t, err)

	err = db.Create(srcCtxWithTimeout, srcTable1Name, defs1)
	require.NoError(t, err)

	rel1, err := db.Relation(srcCtxWithTimeout, srcTable1Name, nil)
	require.NoError(t, err)

	// Insert data into first source table
	// Create a batch with 10 rows
	bat1 := catalog2.MockBatch(schema1, 10)
	defer bat1.Close()
	err = rel1.Write(srcCtxWithTimeout, containers.ToCNBatch(bat1))
	require.NoError(t, err)

	// Create second table
	defs2, err := testutil.EngineTableDefBySchema(schema2)
	require.NoError(t, err)

	err = db.Create(srcCtxWithTimeout, srcTable2Name, defs2)
	require.NoError(t, err)

	rel2, err := db.Relation(srcCtxWithTimeout, srcTable2Name, nil)
	require.NoError(t, err)

	// Insert data into second source table
	// Create a batch with 15 rows
	bat2 := catalog2.MockBatch(schema2, 15)
	defer bat2.Close()
	err = rel2.Write(srcCtxWithTimeout, containers.ToCNBatch(bat2))
	require.NoError(t, err)

	err = txn.Commit(srcCtxWithTimeout)
	require.NoError(t, err)

	// Note: We do NOT force checkpoint here - that will be done in parallel

	// Step 2: Write mo_ccpr_log table with database level sync
	taskID := uint64(1)
	iterationLSN := uint64(1)
	subscriptionName := "test_subscription_db"
	insertSQL := fmt.Sprintf(
		`INSERT INTO mo_catalog.mo_ccpr_log (
			task_id, 
			subscription_name, 
			sync_level, 
			account_id,
			db_name, 
			table_name, 
			upstream_conn, 
			sync_config, 
			iteration_state, 
			iteration_lsn, 
			cn_uuid
		) VALUES (
			%d, 
			'%s', 
			'database', 
			%d,
			'%s', 
			'', 
			'%s', 
			'{}', 
			%d, 
			%d, 
			'%s'
		)`,
		taskID,
		subscriptionName,
		destAccountID,
		srcDBName,
		fmt.Sprintf("%s:%d", publication.InternalSQLExecutorType, srcAccountID),
		publication.IterationStatePending,
		iterationLSN,
		cnUUID,
	)

	// Write mo_ccpr_log using system account context
	err = exec_sql(disttaeEngine, systemCtx, insertSQL)
	require.NoError(t, err)

	// Step 3: Create upstream SQL helper factory
	upstreamSQLHelperFactory := func(
		txnOp client.TxnOperator,
		engine engine.Engine,
		accountID uint32,
		exec executor.SQLExecutor,
		txnClient client.TxnClient,
	) publication.UpstreamSQLHelper {
		return NewUpstreamSQLHelper(txnOp, engine, accountID, exec, txnClient)
	}

	// Create mpool for ExecuteIteration
	mp, err := mpool.NewMPool("test_execute_iteration_database", 0, mpool.NoFixed)
	require.NoError(t, err)

	// Step 4: Create UTHelper for checkpointing
	checkpointDone := make(chan struct{}, 1)
	utHelper := &checkpointUTHelper{
		taeHandler:    taeHandler,
		disttaeEngine: disttaeEngine,
		checkpointC:   checkpointDone,
	}

	// Execute ExecuteIteration with UTHelper
	err = publication.ExecuteIteration(
		srcCtxWithTimeout,
		cnUUID,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		taskID,
		iterationLSN,
		upstreamSQLHelperFactory,
		mp,
		utHelper,
		100*time.Millisecond, // snapshotFlushInterval for test
	)

	// Signal checkpoint goroutine to stop
	close(checkpointDone)

	// Check errors
	require.NoError(t, err, "ExecuteIteration should complete successfully")

	// Step 5: Verify that the iteration state was updated
	// Query mo_ccpr_log to check iteration_state using system account
	querySQL := fmt.Sprintf(
		`SELECT iteration_state, iteration_lsn FROM mo_catalog.mo_ccpr_log WHERE task_id = %d`,
		taskID,
	)

	v, ok := runtime.ServiceRuntime("").GetGlobalVariables(runtime.InternalSQLExecutor)
	require.True(t, ok)
	exec := v.(executor.SQLExecutor)

	querySystemCtx := context.WithValue(destCtxWithTimeout, defines.TenantIDKey{}, catalog.System_Account)
	txn, err = disttaeEngine.NewTxnOperator(querySystemCtx, disttaeEngine.Now())
	require.NoError(t, err)

	res, err := exec.Exec(querySystemCtx, querySQL, executor.Options{}.WithTxn(txn))
	require.NoError(t, err)
	defer res.Close()

	// Check that iteration_state is completed
	var found bool
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		require.Equal(t, 1, rows)
		require.Equal(t, 2, len(cols))

		state := vector.GetFixedAtWithTypeCheck[int8](cols[0], 0)
		lsn := vector.GetFixedAtWithTypeCheck[int64](cols[1], 0)

		require.Equal(t, publication.IterationStateCompleted, state)
		require.Equal(t, int64(iterationLSN), lsn)
		found = true
		return true
	})
	require.True(t, found, "should find the updated iteration record")

	err = txn.Commit(querySystemCtx)
	require.NoError(t, err)

	// Step 6: Check destination tables row counts
	// For database level iteration, all tables in the database should be synced
	queryDestCtx := context.WithValue(destCtxWithTimeout, defines.TenantIDKey{}, destAccountID)
	txn, err = disttaeEngine.NewTxnOperator(queryDestCtx, disttaeEngine.Now())
	require.NoError(t, err)

	// Check first table row count
	checkRowCountSQL1 := fmt.Sprintf(`SELECT COUNT(*) FROM %s.%s`, srcDBName, srcTable1Name)
	rowCountRes1, err := exec.Exec(queryDestCtx, checkRowCountSQL1, executor.Options{}.WithTxn(txn))
	require.NoError(t, err)
	defer rowCountRes1.Close()

	var rowCount1 int64
	rowCountRes1.ReadRows(func(rows int, cols []*vector.Vector) bool {
		require.Equal(t, 1, rows)
		require.Equal(t, 1, len(cols))
		rowCount1 = vector.GetFixedAtWithTypeCheck[int64](cols[0], 0)
		return true
	})
	require.Equal(t, int64(10), rowCount1, "destination table1 should have 10 rows after iteration")

	// Check second table row count
	checkRowCountSQL2 := fmt.Sprintf(`SELECT COUNT(*) FROM %s.%s`, srcDBName, srcTable2Name)
	rowCountRes2, err := exec.Exec(queryDestCtx, checkRowCountSQL2, executor.Options{}.WithTxn(txn))
	require.NoError(t, err)
	defer rowCountRes2.Close()

	var rowCount2 int64
	rowCountRes2.ReadRows(func(rows int, cols []*vector.Vector) bool {
		require.Equal(t, 1, rows)
		require.Equal(t, 1, len(cols))
		rowCount2 = vector.GetFixedAtWithTypeCheck[int64](cols[0], 0)
		return true
	})
	require.Equal(t, int64(15), rowCount2, "destination table2 should have 15 rows after iteration")

	err = txn.Commit(queryDestCtx)
	require.NoError(t, err)

	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
}

func TestExecuteIterationWithIndex(t *testing.T) {
	catalog.SetupDefines("")

	var (
		srcAccountID  = uint32(1)
		destAccountID = uint32(2)
		cnUUID        = ""
	)

	// Setup source account context
	srcCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srcCtx = context.WithValue(srcCtx, defines.TenantIDKey{}, srcAccountID)
	srcCtxWithTimeout, cancelSrc := context.WithTimeout(srcCtx, time.Minute*5)
	defer cancelSrc()

	// Setup destination account context
	destCtx, cancelDest := context.WithCancel(context.Background())
	defer cancelDest()
	destCtx = context.WithValue(destCtx, defines.TenantIDKey{}, destAccountID)
	destCtxWithTimeout, cancelDestTimeout := context.WithTimeout(destCtx, time.Minute*5)
	defer cancelDestTimeout()

	// Setup system account context
	systemCtxBase, cancelSystem := context.WithCancel(context.Background())
	defer cancelSystem()
	systemCtxBase = context.WithValue(systemCtxBase, defines.TenantIDKey{}, catalog.System_Account)
	systemCtxWithTimeout, cancelSystemTimeout := context.WithTimeout(systemCtxBase, time.Minute*5)
	defer cancelSystemTimeout()

	// Create engines with source account context
	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(srcCtx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(srcCtx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	// Register mock auto increment service
	mockIncrService := NewMockAutoIncrementService(cnUUID)
	incrservice.SetAutoIncrementServiceByID("", mockIncrService)
	defer mockIncrService.Close()

	// Create system tables for source account
	// These tables are needed when creating tables in the source account
	err := exec_sql(disttaeEngine, srcCtxWithTimeout, frontend.MoCatalogMoIndexesDDL)
	require.NoError(t, err)

	err = exec_sql(disttaeEngine, srcCtxWithTimeout, frontend.MoCatalogMoTablePartitionsDDL)
	require.NoError(t, err)

	err = exec_sql(disttaeEngine, srcCtxWithTimeout, frontend.MoCatalogMoAutoIncrTableDDL)
	require.NoError(t, err)

	err = exec_sql(disttaeEngine, srcCtxWithTimeout, frontend.MoCatalogMoForeignKeysDDL)
	require.NoError(t, err)

	// Create mo_snapshots table for source account
	moSnapshotsDDL := frontend.MoCatalogMoSnapshotsDDL
	err = exec_sql(disttaeEngine, srcCtxWithTimeout, moSnapshotsDDL)
	require.NoError(t, err)

	// Create system tables for system account
	// These tables are needed for system account operations
	err = exec_sql(disttaeEngine, systemCtxWithTimeout, frontend.MoCatalogMoIndexesDDL)
	require.NoError(t, err)

	err = exec_sql(disttaeEngine, systemCtxWithTimeout, frontend.MoCatalogMoTablePartitionsDDL)
	require.NoError(t, err)

	err = exec_sql(disttaeEngine, systemCtxWithTimeout, frontend.MoCatalogMoAutoIncrTableDDL)
	require.NoError(t, err)

	err = exec_sql(disttaeEngine, systemCtxWithTimeout, frontend.MoCatalogMoForeignKeysDDL)
	require.NoError(t, err)

	err = exec_sql(disttaeEngine, systemCtxWithTimeout, frontend.MoCatalogMoCcprLogDDL)
	require.NoError(t, err)

	moSnapshotsDDLSystem := frontend.MoCatalogMoSnapshotsDDL
	err = exec_sql(disttaeEngine, systemCtxWithTimeout, moSnapshotsDDLSystem)
	require.NoError(t, err)

	// Create system tables for destination account
	// These tables are needed when creating tables in the destination account
	err = exec_sql(disttaeEngine, destCtxWithTimeout, frontend.MoCatalogMoIndexesDDL)
	require.NoError(t, err)

	err = exec_sql(disttaeEngine, destCtxWithTimeout, frontend.MoCatalogMoTablePartitionsDDL)
	require.NoError(t, err)

	err = exec_sql(disttaeEngine, destCtxWithTimeout, frontend.MoCatalogMoAutoIncrTableDDL)
	require.NoError(t, err)

	err = exec_sql(disttaeEngine, destCtxWithTimeout, frontend.MoCatalogMoForeignKeysDDL)
	require.NoError(t, err)

	err = exec_sql(disttaeEngine, destCtxWithTimeout, frontend.MoCatalogMoSnapshotsDDL)
	require.NoError(t, err)

	// Step 1: Create source database and table with index in source account using SQL
	srcDBName := "src_db"
	srcTableName := "src_table_with_index"

	// Create database using SQL
	createDBSQL := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", srcDBName)
	err = exec_sql(disttaeEngine, srcCtxWithTimeout, createDBSQL)
	require.NoError(t, err)

	// Create table with regular column and index using SQL
	// Using regular index instead of HNSW to avoid experimental feature check
	createTableSQL := fmt.Sprintf(
		`CREATE TABLE %s.%s (
			id BIGINT PRIMARY KEY,
			value_col INT,
			name VARCHAR(100),
			KEY idx_value (value_col)
		)`,
		srcDBName, srcTableName,
	)
	err = exec_sql(disttaeEngine, srcCtxWithTimeout, createTableSQL)
	require.NoError(t, err)

	// Insert data into source table using SQL
	// Insert 10 rows with consistent column definitions
	for i := 0; i < 10; i++ {
		insertSQL := fmt.Sprintf(
			`INSERT INTO %s.%s (id, value_col, name) VALUES (%d, %d, 'name_%d')`,
			srcDBName, srcTableName, i, i*10, i,
		)
		err = exec_sql(disttaeEngine, srcCtxWithTimeout, insertSQL)
		require.NoError(t, err)
	}

	// Step 2: Write mo_ccpr_log table in destination account context
	taskID := uint64(1)
	iterationLSN := uint64(1)
	subscriptionName := "test_subscription_with_index"
	insertSQL := fmt.Sprintf(
		`INSERT INTO mo_catalog.mo_ccpr_log (
			task_id, 
			subscription_name, 
			sync_level, 
			account_id,
			db_name, 
			table_name, 
			upstream_conn, 
			sync_config, 
			iteration_state, 
			iteration_lsn, 
			cn_uuid
		) VALUES (
			%d, 
			'%s', 
			'table', 
			%d,
			'%s', 
			'%s', 
			'%s', 
			'{}', 
			%d, 
			%d, 
			'%s'
		)`,
		taskID,
		subscriptionName,
		destAccountID,
		srcDBName,
		srcTableName,
		fmt.Sprintf("%s:%d", publication.InternalSQLExecutorType, srcAccountID),
		publication.IterationStatePending,
		iterationLSN,
		cnUUID,
	)

	// Write mo_ccpr_log using system account context
	err = exec_sql(disttaeEngine, systemCtxWithTimeout, insertSQL)
	require.NoError(t, err)

	// Step 3: Create upstream SQL helper factory
	upstreamSQLHelperFactory := func(
		txnOp client.TxnOperator,
		engine engine.Engine,
		accountID uint32,
		exec executor.SQLExecutor,
		txnClient client.TxnClient,
	) publication.UpstreamSQLHelper {
		return NewUpstreamSQLHelper(txnOp, engine, accountID, exec, txnClient)
	}

	// Create mpool for ExecuteIteration
	mp, err := mpool.NewMPool("test_execute_iteration_with_index", 0, mpool.NoFixed)
	require.NoError(t, err)

	// Step 4: Create UTHelper for checkpointing
	checkpointDone := make(chan struct{}, 1)
	utHelper := &checkpointUTHelper{
		taeHandler:    taeHandler,
		disttaeEngine: disttaeEngine,
		checkpointC:   checkpointDone,
	}

	// Execute ExecuteIteration with UTHelper
	err = publication.ExecuteIteration(
		systemCtxWithTimeout,
		cnUUID,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		taskID,
		iterationLSN,
		upstreamSQLHelperFactory,
		mp,
		utHelper,
		100*time.Millisecond, // snapshotFlushInterval for test
	)

	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))

	// Signal checkpoint goroutine to stop
	close(checkpointDone)

	// Check errors
	require.NoError(t, err, "ExecuteIteration should complete successfully")

	// Step 5: Verify that the iteration state was updated
	// Query mo_ccpr_log to check iteration_state using system account
	querySQL := fmt.Sprintf(
		`SELECT iteration_state, iteration_lsn FROM mo_catalog.mo_ccpr_log WHERE task_id = %d`,
		taskID,
	)

	v, ok := runtime.ServiceRuntime("").GetGlobalVariables(runtime.InternalSQLExecutor)
	require.True(t, ok)
	exec := v.(executor.SQLExecutor)

	querySystemCtx := context.WithValue(destCtxWithTimeout, defines.TenantIDKey{}, catalog.System_Account)
	txn, err := disttaeEngine.NewTxnOperator(querySystemCtx, disttaeEngine.Now())
	require.NoError(t, err)

	res, err := exec.Exec(querySystemCtx, querySQL, executor.Options{}.WithTxn(txn))
	require.NoError(t, err)
	defer res.Close()

	// Check that iteration_state is completed
	var found bool
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		require.Equal(t, 1, rows)
		require.Equal(t, 2, len(cols))

		state := vector.GetFixedAtWithTypeCheck[int8](cols[0], 0)
		lsn := vector.GetFixedAtWithTypeCheck[int64](cols[1], 0)

		require.Equal(t, publication.IterationStateCompleted, state)
		require.Equal(t, int64(iterationLSN), lsn)
		found = true
		return true
	})
	require.True(t, found, "should find the updated iteration record")

	err = txn.Commit(querySystemCtx)
	require.NoError(t, err)

	// Step 6: Check destination table row count
	// The destination table should have 10 rows (same as source table)
	checkRowCountSQL := fmt.Sprintf(`SELECT COUNT(*) FROM %s.%s`, srcDBName, srcTableName)
	queryDestCtx := context.WithValue(destCtxWithTimeout, defines.TenantIDKey{}, destAccountID)
	txn2, err := disttaeEngine.NewTxnOperator(queryDestCtx, disttaeEngine.Now())
	require.NoError(t, err)

	rowCountRes, err := exec.Exec(queryDestCtx, checkRowCountSQL, executor.Options{}.WithTxn(txn2))
	require.NoError(t, err)
	defer rowCountRes.Close()

	var rowCount int64
	rowCountRes.ReadRows(func(rows int, cols []*vector.Vector) bool {
		require.Equal(t, 1, rows)
		require.Equal(t, 1, len(cols))
		rowCount = vector.GetFixedAtWithTypeCheck[int64](cols[0], 0)
		return true
	})
	require.Equal(t, int64(10), rowCount, "destination table should have 10 rows after iteration")

	err = txn2.Commit(queryDestCtx)
	require.NoError(t, err)

	// Step 7: Verify that the index was created in the destination table
	// Query mo_indexes to check if the index exists in the destination account
	queryIndexSQL := fmt.Sprintf(
		`SELECT COUNT(*) FROM mo_catalog.mo_indexes 
		WHERE database_id IN (
			SELECT dat_id FROM mo_catalog.mo_database WHERE datname = '%s'
		) 
		AND table_id IN (
			SELECT rel_id FROM mo_catalog.mo_tables WHERE relname = '%s' AND reldatabase = '%s'
		)`,
		srcDBName, srcTableName, srcDBName,
	)

	queryIndexCtx := context.WithValue(destCtxWithTimeout, defines.TenantIDKey{}, destAccountID)
	txn3, err := disttaeEngine.NewTxnOperator(queryIndexCtx, disttaeEngine.Now())
	require.NoError(t, err)

	indexRes, err := exec.Exec(queryIndexCtx, queryIndexSQL, executor.Options{}.WithTxn(txn3))
	require.NoError(t, err)
	defer indexRes.Close()

	var indexCount int64
	indexRes.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows > 0 && len(cols) > 0 {
			indexCount = vector.GetFixedAtWithTypeCheck[int64](cols[0], 0)
		}
		return true
	})
	// The index should exist in the destination table
	// Note: The exact count depends on how many index entries are created (metadata + storage tables)
	require.Greater(t, indexCount, int64(0), "destination table should have at least one index entry")

	err = txn3.Commit(queryIndexCtx)
	require.NoError(t, err)

	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
}

func TestGetObjectChunk(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId = catalog.System_Account
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	// Start cluster
	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	// Get fileservice from engine
	de := disttaeEngine.Engine
	fs := de.FS()
	if fs == nil {
		t.Fatal("fileservice is not available")
	}

	// Test case 1: Small file (<= 100MB) - should have 1 chunk
	testObjectName1 := "test_object_small"
	testContent1 := []byte("This is a small test file content")
	err := fs.Write(ctxWithTimeout, fileservice.IOVector{
		FilePath: testObjectName1,
		Entries: []fileservice.IOEntry{
			{
				Offset: 0,
				Size:   int64(len(testContent1)),
				Data:   testContent1,
			},
		},
	})
	require.NoError(t, err)

	// Test ReadObjectFromEngine with chunk 0 (should return full content)
	content, err := frontend.ReadObjectFromEngine(ctxWithTimeout, de, testObjectName1, 0, -1)
	require.NoError(t, err)
	require.Equal(t, testContent1, content)

	// Test case 2: Large file (> 100MB) - should have multiple chunks
	// Create a file larger than 100MB (e.g., 150MB)
	const chunkSize = 100 * 1024 * 1024       // 100MB
	largeFileSize := int64(150 * 1024 * 1024) // 150MB
	testObjectName2 := "test_object_large"
	largeContent := make([]byte, largeFileSize)
	for i := range largeContent {
		largeContent[i] = byte(i % 256)
	}

	// Write in chunks to avoid memory issues
	const writeChunkSize = 10 * 1024 * 1024 // 10MB per write
	writeVector := fileservice.IOVector{
		FilePath: testObjectName2,
	}
	offset := int64(0)
	for offset < largeFileSize {
		chunkEnd := offset + writeChunkSize
		if chunkEnd > largeFileSize {
			chunkEnd = largeFileSize
		}
		writeVector.Entries = append(writeVector.Entries, fileservice.IOEntry{
			Offset: offset,
			Size:   chunkEnd - offset,
			Data:   largeContent[offset:chunkEnd],
		})
		offset = chunkEnd
	}
	err = fs.Write(ctxWithTimeout, writeVector)
	require.NoError(t, err)

	// Verify file size
	dirEntry, err := fs.StatFile(ctxWithTimeout, testObjectName2)
	require.NoError(t, err)
	require.Equal(t, largeFileSize, dirEntry.Size)

	// Test ReadObjectFromEngine with different offsets and sizes
	// Read first chunk (100MB)
	chunk0, err := frontend.ReadObjectFromEngine(ctxWithTimeout, de, testObjectName2, 0, chunkSize)
	require.NoError(t, err)
	require.Equal(t, chunkSize, int64(len(chunk0)))
	require.Equal(t, largeContent[0:chunkSize], chunk0)

	// Read second chunk (50MB)
	remainingSize := largeFileSize - chunkSize
	chunk1, err := frontend.ReadObjectFromEngine(ctxWithTimeout, de, testObjectName2, chunkSize, remainingSize)
	require.NoError(t, err)
	require.Equal(t, remainingSize, int64(len(chunk1)))
	require.Equal(t, largeContent[chunkSize:], chunk1)

	// Test ReadObjectFromEngine with partial chunk
	partialSize := int64(1024) // 1KB
	partialChunk, err := frontend.ReadObjectFromEngine(ctxWithTimeout, de, testObjectName2, 0, partialSize)
	require.NoError(t, err)
	require.Equal(t, partialSize, int64(len(partialChunk)))
	require.Equal(t, largeContent[0:partialSize], partialChunk)
}
