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
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/publication"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	testutil2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/stretchr/testify/assert"
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
	executor, err := publication.NewInternalSQLExecutor("", nil, nil, accountId, &publication.SQLExecutorRetryOption{
		MaxRetries:    0,
		RetryInterval: time.Second,
		Classifier:    publication.NewDownstreamCommitClassifier(),
	}, false)
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
			iterationState: publication.IterationStateRunning,
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
			iterationState: publication.IterationStateRunning,
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
			iterationState: publication.IterationStateRunning,
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
			iterationState: publication.IterationStatePending,
			shouldInsert:   true,
			expectError:    true,
			errorContains:  "iteration_state is not running",
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
					%d, 
					%d, 
					%d, 
					'%s'
				)`,
					tc.taskID,
					catalog.System_Account,
					publication.SubscriptionStateRunning,
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
			state, 
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
			%d, 
			'%s'
		)`,
		taskID,
		subscriptionName,
		destAccountID,
		srcDBName,
		srcTableName,
		fmt.Sprintf("%s:%d", publication.InternalSQLExecutorType, srcAccountID),
		publication.SubscriptionStateRunning,
		publication.IterationStateRunning,
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
		context.Background(),
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
		require.Equal(t, int64(iterationLSN+1), lsn)
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
		publication.IterationStateRunning,
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
		context.Background(),
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
		require.Equal(t, int64(iterationLSN2+1), lsn)
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
		publication.IterationStateRunning,
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
		context.Background(),
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
		require.Equal(t, int64(iterationLSN3+1), lsn)
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
			state, 
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
			%d, 
			'%s'
		)`,
		taskID,
		subscriptionName,
		destAccountID,
		srcDBName,
		fmt.Sprintf("%s:%d", publication.InternalSQLExecutorType, srcAccountID),
		publication.SubscriptionStateRunning,
		publication.IterationStateRunning,
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
		context.Background(),
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
		require.Equal(t, int64(iterationLSN+1), lsn)
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
			state, 
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
			%d, 
			'%s'
		)`,
		taskID,
		subscriptionName,
		destAccountID,
		srcDBName,
		fmt.Sprintf("%s:%d", publication.InternalSQLExecutorType, srcAccountID),
		publication.SubscriptionStateRunning,
		publication.IterationStateRunning,
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
		context.Background(),
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
		require.Equal(t, int64(iterationLSN+1), lsn)
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

	// Step 8: Delete the upstream table
	txnDelete, err := disttaeEngine.NewTxnOperator(srcCtxWithTimeout, disttaeEngine.Now())
	require.NoError(t, err)

	db, err := disttaeEngine.Engine.Database(srcCtxWithTimeout, srcDBName, txnDelete)
	require.NoError(t, err)

	err = db.Delete(srcCtxWithTimeout, srcTableName)
	require.NoError(t, err)

	err = txnDelete.Commit(srcCtxWithTimeout)
	require.NoError(t, err)

	// Step 9: Update mo_ccpr_log for second iteration
	iterationLSN2 := uint64(2)
	updateSQL := fmt.Sprintf(
		`UPDATE mo_catalog.mo_ccpr_log 
		SET iteration_state = %d, iteration_lsn = %d 
		WHERE task_id = %d`,
		publication.IterationStateRunning,
		iterationLSN2,
		taskID,
	)

	// Update mo_ccpr_log using system account context
	err = exec_sql(disttaeEngine, systemCtxWithTimeout, updateSQL)
	require.NoError(t, err)

	// Step 10: Create new checkpoint channel for second iteration
	checkpointDone2 := make(chan struct{}, 1)
	utHelper2 := &checkpointUTHelper{
		taeHandler:    taeHandler,
		disttaeEngine: disttaeEngine,
		checkpointC:   checkpointDone2,
	}

	// Step 11: Execute second iteration (iteration2)
	err = publication.ExecuteIteration(
		context.Background(),
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

	// Signal checkpoint goroutine to stop
	close(checkpointDone2)

	// Check errors
	require.NoError(t, err, "Second ExecuteIteration should complete successfully")

	// Step 12: Verify that the second iteration state was updated
	querySQL2 := fmt.Sprintf(
		`SELECT iteration_state, iteration_lsn FROM mo_catalog.mo_ccpr_log WHERE task_id = %d`,
		taskID,
	)

	querySystemCtx2 := context.WithValue(destCtxWithTimeout, defines.TenantIDKey{}, catalog.System_Account)
	txn4, err := disttaeEngine.NewTxnOperator(querySystemCtx2, disttaeEngine.Now())
	require.NoError(t, err)

	res2, err := exec.Exec(querySystemCtx2, querySQL2, executor.Options{}.WithTxn(txn4))
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
		require.Equal(t, int64(iterationLSN2+1), lsn)
		found2 = true
		return true
	})
	require.True(t, found2, "should find the updated iteration record for second iteration")

	err = txn4.Commit(querySystemCtx2)
	require.NoError(t, err)

	// Step 13: Check that downstream table does NOT exist after iteration2
	checkTableExistsSQL := fmt.Sprintf(
		`SELECT COUNT(*) FROM mo_catalog.mo_tables 
		WHERE reldatabase = '%s' AND relname = '%s'`,
		srcDBName, srcTableName,
	)
	queryDestCtx2 := context.WithValue(destCtxWithTimeout, defines.TenantIDKey{}, destAccountID)
	txn5, err := disttaeEngine.NewTxnOperator(queryDestCtx2, disttaeEngine.Now())
	require.NoError(t, err)

	tableExistsRes, err := exec.Exec(queryDestCtx2, checkTableExistsSQL, executor.Options{}.WithTxn(txn5))
	require.NoError(t, err)
	defer tableExistsRes.Close()

	var tableCount int64
	tableExistsRes.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows > 0 && len(cols) > 0 {
			tableCount = vector.GetFixedAtWithTypeCheck[int64](cols[0], 0)
		}
		return true
	})
	require.Equal(t, int64(0), tableCount, "destination table should not exist after iteration2 (table was deleted)")

	err = txn5.Commit(queryDestCtx2)
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
	require.Equal(t, int64(chunkSize), int64(len(chunk0)))
	require.Equal(t, largeContent[0:chunkSize], chunk0)

	// Read second chunk (50MB)
	remainingSize := largeFileSize - chunkSize
	chunk1, err := frontend.ReadObjectFromEngine(ctxWithTimeout, de, testObjectName2, chunkSize, remainingSize)
	require.NoError(t, err)
	require.Equal(t, int64(remainingSize), int64(len(chunk1)))
	require.Equal(t, largeContent[chunkSize:], chunk1)

	// Test ReadObjectFromEngine with partial chunk
	partialSize := int64(1024) // 1KB
	partialChunk, err := frontend.ReadObjectFromEngine(ctxWithTimeout, de, testObjectName2, 0, partialSize)
	require.NoError(t, err)
	require.Equal(t, int64(partialSize), int64(len(partialChunk)))
	require.Equal(t, largeContent[0:partialSize], partialChunk)
}

func TestExecuteIterationWithSnapshotFinishedInjection(t *testing.T) {
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

	// Create system tables for source account
	moSnapshotsDDL := frontend.MoCatalogMoSnapshotsDDL
	err = exec_sql(disttaeEngine, srcCtxWithTimeout, moSnapshotsDDL)
	require.NoError(t, err)
	err = exec_sql(disttaeEngine, srcCtxWithTimeout, frontend.MoCatalogMoTablePartitionsDDL)
	require.NoError(t, err)
	err = exec_sql(disttaeEngine, srcCtxWithTimeout, frontend.MoCatalogMoAutoIncrTableDDL)
	require.NoError(t, err)
	err = exec_sql(disttaeEngine, srcCtxWithTimeout, frontend.MoCatalogMoForeignKeysDDL)
	require.NoError(t, err)

	// Create system tables for destination account
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
	bat := catalog2.MockBatch(schema, 10)
	defer bat.Close()
	err = rel.Write(srcCtxWithTimeout, containers.ToCNBatch(bat))
	require.NoError(t, err)

	err = txn.Commit(srcCtxWithTimeout)
	require.NoError(t, err)

	// Step 2: Write mo_ccpr_log table in destination account context
	taskID := uint64(1)
	iterationLSN := uint64(1)
	subscriptionName := "test_subscription_injection"
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
			'%s', 
			'table', 
			%d,
			'%s', 
			'%s', 
			'%s', 
			'{}', 
			%d, 
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
		publication.SubscriptionStateRunning,
		publication.IterationStateRunning,
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
	mp, err := mpool.NewMPool("test_execute_iteration_injection", 0, mpool.NoFixed)
	require.NoError(t, err)

	// Step 4: Create UTHelper for checkpointing
	checkpointDone := make(chan struct{}, 1)
	utHelper := &checkpointUTHelper{
		taeHandler:    taeHandler,
		disttaeEngine: disttaeEngine,
		checkpointC:   checkpointDone,
	}

	// Enable fault injection
	fault.Enable()
	defer fault.Disable()

	// First iteration: Inject error to trigger failure
	rmFn, err := objectio.InjectPublicationSnapshotFinished("ut injection: publicationSnapshotFinished")
	require.NoError(t, err)
	defer rmFn()

	// Execute first ExecuteIteration - should fail due to injection
	err = publication.ExecuteIteration(
		context.Background(),
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

	// error is flushed
	require.Error(t, err)

	// Remove the injection for second iteration
	rmFn()

	// Step 5: Update iteration state for second iteration
	updateSQL := fmt.Sprintf(
		`UPDATE mo_catalog.mo_ccpr_log 
		SET iteration_state = %d, iteration_lsn = %d 
		WHERE task_id = %d`,
		publication.IterationStateRunning,
		iterationLSN,
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

	// Second iteration: No injection, should succeed
	err = publication.ExecuteIteration(
		context.Background(),
		cnUUID,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		taskID,
		iterationLSN,
		upstreamSQLHelperFactory,
		mp,
		utHelper2,
		100*time.Millisecond, // snapshotFlushInterval for test
	)

	// Signal checkpoint goroutine to stop
	close(checkpointDone2)

	// Second iteration should succeed
	require.NoError(t, err, "Second ExecuteIteration should complete successfully")

	// Step 6: Verify that the iteration state was updated
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
		require.Equal(t, int64(iterationLSN+1), lsn)
		found = true
		return true
	})
	require.True(t, found, "should find the updated iteration record")

	err = txn.Commit(querySystemCtx)
	require.NoError(t, err)

	// Step 7: Check destination table row count - verify second iteration succeeded
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
	require.Equal(t, int64(10), rowCount, "destination table should have 10 rows after second iteration")
	err = txn.Commit(queryDestCtx)
	require.NoError(t, err)
}

func TestExecuteIterationWithCommitFailedInjection(t *testing.T) {
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
	bat := catalog2.MockBatch(schema, 10)
	defer bat.Close()
	err = rel.Write(srcCtxWithTimeout, containers.ToCNBatch(bat))
	require.NoError(t, err)

	err = txn.Commit(srcCtxWithTimeout)
	require.NoError(t, err)

	// Step 2: Write mo_ccpr_log table in destination account context
	taskID := uint64(1)
	iterationLSN := uint64(1)
	subscriptionName := "test_subscription_commit_failed"
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
			'%s', 
			'table', 
			%d,
			'%s', 
			'%s', 
			'%s', 
			'{}', 
			%d, 
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
		publication.SubscriptionStateRunning,
		publication.IterationStateRunning,
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
	mp, err := mpool.NewMPool("test_execute_iteration_commit_failed", 0, mpool.NoFixed)
	require.NoError(t, err)

	// Step 4: Create UTHelper for checkpointing
	checkpointDone := make(chan struct{}, 1)
	utHelper := &checkpointUTHelper{
		taeHandler:    taeHandler,
		disttaeEngine: disttaeEngine,
		checkpointC:   checkpointDone,
	}

	// Enable fault injection
	fault.Enable()
	defer fault.Disable()

	// Inject commit failed error
	rmFn, err := objectio.InjectPublicationSnapshotFinished("ut injection: commit failed")
	require.NoError(t, err)
	defer rmFn()

	// Execute ExecuteIteration - should fail due to commit injection
	err = publication.ExecuteIteration(
		context.Background(),
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
	assert.NoError(t, err)

	// Signal checkpoint goroutine to stop
	close(checkpointDone)

	// Step 5: Query mo_ccpr_log to check error_message using system account
	querySQL := fmt.Sprintf(
		`SELECT error_message FROM mo_catalog.mo_ccpr_log WHERE task_id = %d`,
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

	// Check that error_message has a value
	var found bool
	var errorMessage string
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		require.Equal(t, 1, rows)
		require.Equal(t, 1, len(cols))

		errorMessage = cols[0].GetStringAt(0)
		require.NotEmpty(t, errorMessage, "error_message should have a value after commit failed injection")
		found = true
		return true
	})
	require.True(t, found, "should find the iteration record with error_message")
	require.NotEmpty(t, errorMessage, "error_message should not be empty after commit failed injection")
	require.Contains(t, errorMessage, "commit failed", "error_message should contain 'commit failed'")

	err = txn.Commit(querySystemCtx)
	require.NoError(t, err)
}

func TestCCPRGC(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountID = catalog.System_Account
		cnUUID    = ""
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
	mockIncrService := NewMockAutoIncrementService(cnUUID)
	incrservice.SetAutoIncrementServiceByID("", mockIncrService)
	defer mockIncrService.Close()

	// Create mo_indexes table
	err := exec_sql(disttaeEngine, ctxWithTimeout, frontend.MoCatalogMoIndexesDDL)
	require.NoError(t, err)
	err = exec_sql(disttaeEngine, ctxWithTimeout, frontend.MoCatalogMoForeignKeysDDL)
	require.NoError(t, err)

	// Create mo_ccpr_log table using system account context
	systemCtx := context.WithValue(ctxWithTimeout, defines.TenantIDKey{}, catalog.System_Account)
	err = exec_sql(disttaeEngine, systemCtx, frontend.MoCatalogMoCcprLogDDL)
	require.NoError(t, err)

	// Create mo_snapshots table
	err = exec_sql(disttaeEngine, ctxWithTimeout, frontend.MoCatalogMoSnapshotsDDL)
	require.NoError(t, err)

	// Create upstream SQL helper factory
	upstreamSQLHelperFactory := func(
		txnOp client.TxnOperator,
		engine engine.Engine,
		accountID uint32,
		exec executor.SQLExecutor,
		txnClient client.TxnClient,
	) publication.UpstreamSQLHelper {
		return NewUpstreamSQLHelper(txnOp, engine, accountID, exec, txnClient)
	}

	// Define test cases
	testCases := []struct {
		name                       string
		iterationLSN               uint64
		dropAtHoursAgo             float64 // Hours ago for drop_at (negative means future)
		state                      int8    // subscription state
		snapshotLSNs               []uint64
		snapshotHoursAgo           float64 // Hours ago for snapshot timestamp (default: 0 = current time)
		gcThresholdHours           float64
		expectedSnapshotCountAfter int64
		expectedRecordExists       bool // Whether mo_ccpr_log record should exist after GC
	}{
		{
			name:                       "DroppedTaskOlderThanThreshold",
			iterationLSN:               10,
			dropAtHoursAgo:             48, // 2 days ago
			state:                      publication.SubscriptionStateDropped,
			snapshotLSNs:               []uint64{5, 8, 9},
			snapshotHoursAgo:           0,  // Current time
			gcThresholdHours:           24, // 1 day threshold
			expectedSnapshotCountAfter: 0,  // All snapshots should be deleted
			expectedRecordExists:       false,
		},
		// Running task cases
		{
			name:                       "RunningTask_SnapshotEqualToCurrentLSN",
			iterationLSN:               10,
			dropAtHoursAgo:             -1, // No drop_at
			state:                      publication.SubscriptionStateRunning,
			snapshotLSNs:               []uint64{10}, // Equal to current LSN
			snapshotHoursAgo:           0,
			gcThresholdHours:           24,
			expectedSnapshotCountAfter: 1, // Should keep (lsn >= current_lsn - 1)
			expectedRecordExists:       true,
		},
		{
			name:                       "RunningTask_SnapshotLessThanCurrentLSN",
			iterationLSN:               10,
			dropAtHoursAgo:             -1, // No drop_at
			state:                      publication.SubscriptionStateRunning,
			snapshotLSNs:               []uint64{8, 9}, // Less than current LSN (10)
			snapshotHoursAgo:           0,
			gcThresholdHours:           24,
			expectedSnapshotCountAfter: 1, // Should delete (lsn < current_lsn - 1, i.e., lsn < 9)
			expectedRecordExists:       true,
		},
		// Error/Pause task cases
		{
			name:                       "ErrorTask_SnapshotEqualToCurrentLSN",
			iterationLSN:               10,
			dropAtHoursAgo:             -1, // No drop_at
			state:                      publication.SubscriptionStateError,
			snapshotLSNs:               []uint64{10}, // Equal to current LSN
			snapshotHoursAgo:           0,
			gcThresholdHours:           24,
			expectedSnapshotCountAfter: 1, // Should keep (lsn >= current_lsn - 1)
			expectedRecordExists:       true,
		},
		{
			name:                       "ErrorTask_SnapshotLessThanCurrentLSN",
			iterationLSN:               10,
			dropAtHoursAgo:             -1, // No drop_at
			state:                      publication.SubscriptionStateError,
			snapshotLSNs:               []uint64{8, 9}, // Less than current LSN (10)
			snapshotHoursAgo:           0,
			gcThresholdHours:           24,
			expectedSnapshotCountAfter: 1, // Should delete (lsn < current_lsn - 1)
			expectedRecordExists:       true,
		},
		{
			name:                       "ErrorTask_OldSnapshotEqualToCurrentLSN",
			iterationLSN:               10,
			dropAtHoursAgo:             -1, // No drop_at
			state:                      publication.SubscriptionStateError,
			snapshotLSNs:               []uint64{10}, // Equal to current LSN
			snapshotHoursAgo:           48,           // 2 days ago (older than snapshot_threshold = 1 day)
			gcThresholdHours:           24,
			expectedSnapshotCountAfter: 0, // Should delete (old snapshot, even if lsn equals current)
			expectedRecordExists:       true,
		},
		{
			name:                       "PauseTask_SnapshotEqualToCurrentLSN",
			iterationLSN:               10,
			dropAtHoursAgo:             -1, // No drop_at
			state:                      publication.SubscriptionStatePause,
			snapshotLSNs:               []uint64{10}, // Equal to current LSN
			snapshotHoursAgo:           0,
			gcThresholdHours:           24,
			expectedSnapshotCountAfter: 1, // Should keep (lsn >= current_lsn - 1)
			expectedRecordExists:       true,
		},
		{
			name:                       "PauseTask_SnapshotLessThanCurrentLSN",
			iterationLSN:               10,
			dropAtHoursAgo:             -1, // No drop_at
			state:                      publication.SubscriptionStatePause,
			snapshotLSNs:               []uint64{8, 9}, // Less than current LSN (10)
			snapshotHoursAgo:           0,
			gcThresholdHours:           24,
			expectedSnapshotCountAfter: 1, // Should delete (lsn < current_lsn - 1)
			expectedRecordExists:       true,
		},
		// Dropped task cases
		{
			name:                       "DroppedTask_SnapshotEqualToCurrentLSN",
			iterationLSN:               10,
			dropAtHoursAgo:             48, // 2 days ago
			state:                      publication.SubscriptionStateDropped,
			snapshotLSNs:               []uint64{10}, // Equal to current LSN
			snapshotHoursAgo:           0,
			gcThresholdHours:           24,
			expectedSnapshotCountAfter: 0, // Should delete (dropped deletes all)
			expectedRecordExists:       false,
		},
		{
			name:                       "DroppedTask_SnapshotLessThanCurrentLSN",
			iterationLSN:               10,
			dropAtHoursAgo:             48, // 2 days ago
			state:                      publication.SubscriptionStateDropped,
			snapshotLSNs:               []uint64{8, 9}, // Less than current LSN (10)
			snapshotHoursAgo:           0,
			gcThresholdHours:           24,
			expectedSnapshotCountAfter: 0, // Should delete (dropped deletes all)
			expectedRecordExists:       false,
		},
	}

	// Run test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clean up mo_ccpr_log before each test case
			cleanupSQL := `DELETE FROM mo_catalog.mo_ccpr_log`
			err := exec_sql(disttaeEngine, systemCtx, cleanupSQL)
			require.NoError(t, err)

			// Clean up mo_snapshots before each test case
			cleanupSnapshotSQL := `DELETE FROM mo_catalog.mo_snapshots WHERE sname LIKE 'ccpr_%'`
			err = exec_sql(disttaeEngine, ctxWithTimeout, cleanupSnapshotSQL)
			require.NoError(t, err)

			subscriptionName := "test_subscription_gc"

			// Calculate drop_at timestamp
			var dropAtStr string
			if tc.dropAtHoursAgo >= 0 {
				dropAtTime := time.Now().Add(-time.Duration(tc.dropAtHoursAgo) * time.Hour)
				dropAtStr = dropAtTime.Format("2006-01-02 15:04:05")
			} else {
				// For non-dropped tasks, drop_at should be NULL
				dropAtStr = "NULL"
			}

			// Insert test data into mo_ccpr_log
			var insertCcprLogSQL string
			if tc.dropAtHoursAgo >= 0 {
				insertCcprLogSQL = fmt.Sprintf(
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
						cn_uuid,
						drop_at
					) VALUES (
						0, 
						'%s', 
						'table', 
						%d,
						'test_db', 
						'test_table', 
						'%s:%d', 
						'{}', 
						%d, 
						%d, 
						%d, 
						'%s',
						'%s'
					)`,
					subscriptionName,
					accountID,
					publication.InternalSQLExecutorType,
					accountID,
					tc.state,
					publication.IterationStateCompleted,
					tc.iterationLSN,
					cnUUID,
					dropAtStr,
				)
			} else {
				insertCcprLogSQL = fmt.Sprintf(
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
						0, 
						'%s', 
						'table', 
						%d,
						'test_db', 
						'test_table', 
						'%s:%d', 
						'{}', 
						%d, 
						%d, 
						%d, 
						'%s'
					)`,
					subscriptionName,
					accountID,
					publication.InternalSQLExecutorType,
					accountID,
					tc.state,
					publication.IterationStateCompleted,
					tc.iterationLSN,
					cnUUID,
				)
			}

			err = exec_sql(disttaeEngine, systemCtx, insertCcprLogSQL)
			require.NoError(t, err)

			// Read task_id from mo_ccpr_log after insertion
			// Since we just inserted one record, query it to get the task_id
			v, ok := runtime.ServiceRuntime("").GetGlobalVariables(runtime.InternalSQLExecutor)
			require.True(t, ok)
			exec := v.(executor.SQLExecutor)

			queryTaskIDSQL := `SELECT task_id FROM mo_catalog.mo_ccpr_log WHERE subscription_name = 'test_subscription_gc'`
			txn, err := disttaeEngine.NewTxnOperator(ctxWithTimeout, disttaeEngine.Now())
			require.NoError(t, err)

			res, err := exec.Exec(ctxWithTimeout, queryTaskIDSQL, executor.Options{}.WithTxn(txn))
			require.NoError(t, err)

			var taskID uint64
			res.ReadRows(func(rows int, cols []*vector.Vector) bool {
				require.Equal(t, 1, rows, "should have exactly one record after insertion")
				taskID = uint64(vector.GetFixedAtWithTypeCheck[uint32](cols[0], 0))
				return true
			})
			res.Close()

			err = txn.Commit(ctxWithTimeout)
			require.NoError(t, err)

			// Insert test snapshots into mo_snapshots
			// Calculate snapshot timestamp based on snapshotHoursAgo
			snapshotTime := time.Now().Add(-time.Duration(tc.snapshotHoursAgo) * time.Hour)
			snapshotTS := snapshotTime.UnixNano()
			for _, lsn := range tc.snapshotLSNs {
				snapshotName := fmt.Sprintf("ccpr_%d_%d", taskID, lsn)
				snapshotID, err := uuid.NewV7()
				require.NoError(t, err)

				insertSnapshotSQL := fmt.Sprintf(
					`INSERT INTO mo_catalog.mo_snapshots(
						snapshot_id,
						sname,
						ts,
						level,
						account_name,
						database_name,
						table_name,
						obj_id
					) VALUES ('%s', '%s', %d, '%s', '%s', '%s', '%s', %d)`,
					snapshotID.String(),
					snapshotName,
					snapshotTS,
					"table",
					"",
					"test_db",
					"test_table",
					0,
				)

				err = exec_sql(disttaeEngine, ctxWithTimeout, insertSnapshotSQL)
				require.NoError(t, err)
			}

			// Verify snapshots exist before GC
			checkSnapshotSQL := fmt.Sprintf(
				`SELECT COUNT(*) FROM mo_catalog.mo_snapshots WHERE sname LIKE 'ccpr_%d_%%'`,
				taskID,
			)

			txn, err = disttaeEngine.NewTxnOperator(ctxWithTimeout, disttaeEngine.Now())
			require.NoError(t, err)

			res, err = exec.Exec(ctxWithTimeout, checkSnapshotSQL, executor.Options{}.WithTxn(txn))
			require.NoError(t, err)
			defer res.Close()

			var snapshotCountBefore int64
			res.ReadRows(func(rows int, cols []*vector.Vector) bool {
				require.Equal(t, 1, rows)
				snapshotCountBefore = vector.GetFixedAtWithTypeCheck[int64](cols[0], 0)
				return true
			})
			require.Equal(t, int64(len(tc.snapshotLSNs)), snapshotCountBefore, "should have correct number of snapshots before GC")

			err = txn.Commit(ctxWithTimeout)
			require.NoError(t, err)

			// Call GC
			gcThreshold := time.Duration(tc.gcThresholdHours) * time.Hour
			err = publication.GC(
				ctxWithTimeout,
				disttaeEngine.Engine,
				disttaeEngine.GetTxnClient(),
				cnUUID,
				upstreamSQLHelperFactory,
				gcThreshold,
			)
			require.NoError(t, err)

			// Verify snapshots after GC
			txn, err = disttaeEngine.NewTxnOperator(ctxWithTimeout, disttaeEngine.Now())
			require.NoError(t, err)

			res, err = exec.Exec(ctxWithTimeout, checkSnapshotSQL, executor.Options{}.WithTxn(txn))
			require.NoError(t, err)
			defer res.Close()

			var snapshotCountAfter int64
			res.ReadRows(func(rows int, cols []*vector.Vector) bool {
				require.Equal(t, 1, rows)
				snapshotCountAfter = vector.GetFixedAtWithTypeCheck[int64](cols[0], 0)
				return true
			})
			require.Equal(t, tc.expectedSnapshotCountAfter, snapshotCountAfter,
				"snapshot count after GC should match expected value")

			err = txn.Commit(ctxWithTimeout)
			require.NoError(t, err)

			// Verify mo_ccpr_log record exists or not after GC
			checkRecordSQL := fmt.Sprintf(
				`SELECT COUNT(*) FROM mo_catalog.mo_ccpr_log WHERE task_id = %d`,
				taskID,
			)

			txn, err = disttaeEngine.NewTxnOperator(ctxWithTimeout, disttaeEngine.Now())
			require.NoError(t, err)

			res, err = exec.Exec(ctxWithTimeout, checkRecordSQL, executor.Options{}.WithTxn(txn))
			require.NoError(t, err)
			defer res.Close()

			var recordCount int64
			res.ReadRows(func(rows int, cols []*vector.Vector) bool {
				require.Equal(t, 1, rows)
				recordCount = vector.GetFixedAtWithTypeCheck[int64](cols[0], 0)
				return true
			})

			if tc.expectedRecordExists {
				require.Equal(t, int64(1), recordCount,
					"mo_ccpr_log record should exist after GC")
			} else {
				require.Equal(t, int64(0), recordCount,
					"mo_ccpr_log record should be deleted after GC")
			}

			err = txn.Commit(ctxWithTimeout)
			require.NoError(t, err)
		})
	}
}

func TestCCPRCreateDelete(t *testing.T) {
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

	// Step 2: Write mo_ccpr_log table with database level sync
	taskID := uint64(1)
	iterationLSN1 := uint64(1)
	subscriptionName := "test_subscription_create_delete"
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
			'%s', 
			'database', 
			%d,
			'%s', 
			'', 
			'%s', 
			'{}', 
			%d, 
			%d, 
			%d, 
			'%s'
		)`,
		taskID,
		subscriptionName,
		destAccountID,
		srcDBName,
		fmt.Sprintf("%s:%d", publication.InternalSQLExecutorType, srcAccountID),
		publication.SubscriptionStateRunning,
		publication.IterationStateRunning,
		iterationLSN1,
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
	mp, err := mpool.NewMPool("test_ccpr_create_delete", 0, mpool.NoFixed)
	require.NoError(t, err)

	// Step 4: Create UTHelper for checkpointing
	checkpointDone1 := make(chan struct{}, 1)
	utHelper1 := &checkpointUTHelper{
		taeHandler:    taeHandler,
		disttaeEngine: disttaeEngine,
		checkpointC:   checkpointDone1,
	}

	// Step 5: Execute first iteration (iteration1)
	err = publication.ExecuteIteration(
		context.Background(),
		cnUUID,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		taskID,
		iterationLSN1,
		upstreamSQLHelperFactory,
		mp,
		utHelper1,
		100*time.Millisecond, // snapshotFlushInterval for test
	)

	// Signal checkpoint goroutine to stop
	close(checkpointDone1)

	// Check errors
	require.NoError(t, err, "First ExecuteIteration should complete successfully")

	// Step 6: Verify that the first iteration state was updated
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
		require.Equal(t, int64(iterationLSN1+1), lsn)
		found = true
		return true
	})
	require.True(t, found, "should find the updated iteration record")

	err = txn.Commit(querySystemCtx)
	require.NoError(t, err)

	// Step 7: Check that downstream table exists after iteration1
	// The destination table should have 10 rows (same as source table)
	checkRowCountSQL := fmt.Sprintf(`SELECT COUNT(*) FROM %s.%s`, srcDBName, srcTableName)
	queryDestCtx := context.WithValue(destCtxWithTimeout, defines.TenantIDKey{}, destAccountID)
	txn, err = disttaeEngine.NewTxnOperator(queryDestCtx, disttaeEngine.Now())
	require.NoError(t, err)

	rowCountRes, err := exec.Exec(queryDestCtx, checkRowCountSQL, executor.Options{}.WithTxn(txn))
	require.NoError(t, err)
	defer rowCountRes.Close()

	var rowCount int64
	rowCountRes.ReadRows(func(rows int, cols []*vector.Vector) bool {
		require.Equal(t, 1, rows)
		require.Equal(t, 1, len(cols))
		rowCount = vector.GetFixedAtWithTypeCheck[int64](cols[0], 0)
		return true
	})
	require.Equal(t, int64(10), rowCount, "destination table should have 10 rows after iteration1")

	err = txn.Commit(queryDestCtx)
	require.NoError(t, err)

	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))

	// Step 8: Delete the source table
	txnDelete, err := disttaeEngine.NewTxnOperator(srcCtxWithTimeout, disttaeEngine.Now())
	require.NoError(t, err)

	db, err = disttaeEngine.Engine.Database(srcCtxWithTimeout, srcDBName, txnDelete)
	require.NoError(t, err)

	err = db.Delete(srcCtxWithTimeout, srcTableName)
	require.NoError(t, err)

	err = txnDelete.Commit(srcCtxWithTimeout)
	require.NoError(t, err)

	// Step 9: Update mo_ccpr_log for second iteration
	iterationLSN2 := uint64(2)
	updateSQL := fmt.Sprintf(
		`UPDATE mo_catalog.mo_ccpr_log 
		SET iteration_state = %d, iteration_lsn = %d 
		WHERE task_id = %d`,
		publication.IterationStateRunning,
		iterationLSN2,
		taskID,
	)

	// Update mo_ccpr_log using system account context
	err = exec_sql(disttaeEngine, systemCtx, updateSQL)
	require.NoError(t, err)

	// Step 10: Create new checkpoint channel for second iteration
	checkpointDone2 := make(chan struct{}, 1)
	utHelper2 := &checkpointUTHelper{
		taeHandler:    taeHandler,
		disttaeEngine: disttaeEngine,
		checkpointC:   checkpointDone2,
	}

	// Step 11: Execute second iteration (iteration2)
	err = publication.ExecuteIteration(
		context.Background(),
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

	// Signal checkpoint goroutine to stop
	close(checkpointDone2)

	// Check errors
	require.NoError(t, err, "Second ExecuteIteration should complete successfully")

	// Step 12: Verify that the second iteration state was updated
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
		require.Equal(t, int64(iterationLSN2+1), lsn)
		found2 = true
		return true
	})
	require.True(t, found2, "should find the updated iteration record for second iteration")

	err = txn2.Commit(querySystemCtx2)
	require.NoError(t, err)

	// Step 13: Check that downstream table does NOT exist after iteration2
	// Try to query the table - it should fail or return 0 rows
	checkTableExistsSQL := fmt.Sprintf(
		`SELECT COUNT(*) FROM mo_catalog.mo_tables 
		WHERE reldatabase = '%s' AND relname = '%s'`,
		srcDBName, srcTableName,
	)
	queryDestCtx2 := context.WithValue(destCtxWithTimeout, defines.TenantIDKey{}, destAccountID)
	txn3, err := disttaeEngine.NewTxnOperator(queryDestCtx2, disttaeEngine.Now())
	require.NoError(t, err)

	tableExistsRes, err := exec.Exec(queryDestCtx2, checkTableExistsSQL, executor.Options{}.WithTxn(txn3))
	require.NoError(t, err)
	defer tableExistsRes.Close()

	var tableCount int64
	tableExistsRes.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows > 0 && len(cols) > 0 {
			tableCount = vector.GetFixedAtWithTypeCheck[int64](cols[0], 0)
		}
		return true
	})
	require.Equal(t, int64(0), tableCount, "destination table should not exist after iteration2 (table was deleted)")

	err = txn3.Commit(queryDestCtx2)
	require.NoError(t, err)

	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
}

func TestCCPRAlterTable(t *testing.T) {
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

	// Step 2: Write mo_ccpr_log table with database level sync
	taskID := uint64(1)
	iterationLSN1 := uint64(1)
	subscriptionName := "test_subscription_alter_table"
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
			'%s', 
			'table', 
			%d,
			'%s', 
			'%s', 
			'%s', 
			'{}', 
			%d, 
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
		publication.SubscriptionStateRunning,
		publication.IterationStateRunning,
		iterationLSN1,
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
	mp, err := mpool.NewMPool("test_ccpr_alter_table", 0, mpool.NoFixed)
	require.NoError(t, err)

	// Step 4: Create UTHelper for checkpointing
	checkpointDone1 := make(chan struct{}, 1)
	utHelper1 := &checkpointUTHelper{
		taeHandler:    taeHandler,
		disttaeEngine: disttaeEngine,
		checkpointC:   checkpointDone1,
	}

	// Step 5: Execute first iteration (iteration1)
	err = publication.ExecuteIteration(
		context.Background(),
		cnUUID,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		taskID,
		iterationLSN1,
		upstreamSQLHelperFactory,
		mp,
		utHelper1,
		100*time.Millisecond, // snapshotFlushInterval for test
	)

	// Signal checkpoint goroutine to stop
	close(checkpointDone1)

	// Check errors
	require.NoError(t, err, "First ExecuteIteration should complete successfully")

	// Step 6: Verify that the first iteration state was updated
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
		require.Equal(t, int64(iterationLSN1+1), lsn)
		found = true
		return true
	})
	require.True(t, found, "should find the updated iteration record")

	err = txn.Commit(querySystemCtx)
	require.NoError(t, err)

	// Step 7: Check that downstream table exists after iteration1
	// The destination table should have 10 rows (same as source table)
	checkRowCountSQL := fmt.Sprintf(`SELECT COUNT(*) FROM %s.%s`, srcDBName, srcTableName)
	queryDestCtx := context.WithValue(destCtxWithTimeout, defines.TenantIDKey{}, destAccountID)
	txn, err = disttaeEngine.NewTxnOperator(queryDestCtx, disttaeEngine.Now())
	require.NoError(t, err)

	rowCountRes, err := exec.Exec(queryDestCtx, checkRowCountSQL, executor.Options{}.WithTxn(txn))
	require.NoError(t, err)
	defer rowCountRes.Close()

	var rowCount int64
	rowCountRes.ReadRows(func(rows int, cols []*vector.Vector) bool {
		require.Equal(t, 1, rows)
		require.Equal(t, 1, len(cols))
		rowCount = vector.GetFixedAtWithTypeCheck[int64](cols[0], 0)
		return true
	})
	require.Equal(t, int64(10), rowCount, "destination table should have 10 rows after iteration1")

	err = txn.Commit(queryDestCtx)
	require.NoError(t, err)

	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))

	// Step 8: Delete the source table
	txnDelete, err := disttaeEngine.NewTxnOperator(srcCtxWithTimeout, disttaeEngine.Now())
	require.NoError(t, err)

	db, err = disttaeEngine.Engine.Database(srcCtxWithTimeout, srcDBName, txnDelete)
	require.NoError(t, err)

	err = db.Delete(srcCtxWithTimeout, srcTableName)
	require.NoError(t, err)

	err = txnDelete.Commit(srcCtxWithTimeout)
	require.NoError(t, err)

	// Step 9: Recreate the source table with new data
	txnRecreate, err := disttaeEngine.NewTxnOperator(srcCtxWithTimeout, disttaeEngine.Now())
	require.NoError(t, err)

	db, err = disttaeEngine.Engine.Database(srcCtxWithTimeout, srcDBName, txnRecreate)
	require.NoError(t, err)

	// Create table again with same schema
	defs, err = testutil.EngineTableDefBySchema(schema)
	require.NoError(t, err)

	err = db.Create(srcCtxWithTimeout, srcTableName, defs)
	require.NoError(t, err)

	rel, err = db.Relation(srcCtxWithTimeout, srcTableName, nil)
	require.NoError(t, err)

	// Insert new data into recreated source table
	// Create a batch with 15 rows (different from first iteration)
	bat2 := catalog2.MockBatch(schema, 15)
	defer bat2.Close()
	err = rel.Write(srcCtxWithTimeout, containers.ToCNBatch(bat2))
	require.NoError(t, err)

	err = txnRecreate.Commit(srcCtxWithTimeout)
	require.NoError(t, err)

	// Step 10: Update mo_ccpr_log for second iteration
	iterationLSN2 := uint64(2)
	updateSQL2 := fmt.Sprintf(
		`UPDATE mo_catalog.mo_ccpr_log 
		SET iteration_state = %d, iteration_lsn = %d 
		WHERE task_id = %d`,
		publication.IterationStateRunning,
		iterationLSN2,
		taskID,
	)

	// Update mo_ccpr_log using system account context
	err = exec_sql(disttaeEngine, systemCtx, updateSQL2)
	require.NoError(t, err)

	// Step 11: Create new checkpoint channel for second iteration
	checkpointDone2 := make(chan struct{}, 1)
	utHelper2 := &checkpointUTHelper{
		taeHandler:    taeHandler,
		disttaeEngine: disttaeEngine,
		checkpointC:   checkpointDone2,
	}

	// Step 12: Execute second iteration (iteration2) - should recreate downstream table
	err = publication.ExecuteIteration(
		context.Background(),
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

	// Step 13: Verify that the second iteration state was updated
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
		require.Equal(t, int64(iterationLSN2+1), lsn)
		found2 = true
		return true
	})
	require.True(t, found2, "should find the updated iteration record for second iteration")

	err = txn2.Commit(querySystemCtx2)
	require.NoError(t, err)

	// Step 14: Verify that downstream table exists after iteration2
	checkTableExistsSQL2 := fmt.Sprintf(
		`SELECT COUNT(*) FROM mo_catalog.mo_tables 
		WHERE reldatabase = '%s' AND relname = '%s'`,
		srcDBName, srcTableName,
	)
	queryDestCtx3 := context.WithValue(destCtxWithTimeout, defines.TenantIDKey{}, destAccountID)
	txn5, err := disttaeEngine.NewTxnOperator(queryDestCtx3, disttaeEngine.Now())
	require.NoError(t, err)

	tableExistsRes2, err := exec.Exec(queryDestCtx3, checkTableExistsSQL2, executor.Options{}.WithTxn(txn5))
	require.NoError(t, err)
	defer tableExistsRes2.Close()

	var tableCount2 int64
	tableExistsRes2.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows > 0 && len(cols) > 0 {
			tableCount2 = vector.GetFixedAtWithTypeCheck[int64](cols[0], 0)
		}
		return true
	})
	require.Equal(t, int64(1), tableCount2, "destination table should exist after iteration2 (table was recreated)")

	err = txn5.Commit(queryDestCtx3)
	require.NoError(t, err)

	// Step 15: Check that downstream table has correct row count after iteration2
	checkRowCountSQL2 := fmt.Sprintf(`SELECT COUNT(*) FROM %s.%s`, srcDBName, srcTableName)
	queryDestCtx4 := context.WithValue(destCtxWithTimeout, defines.TenantIDKey{}, destAccountID)
	txn6, err := disttaeEngine.NewTxnOperator(queryDestCtx4, disttaeEngine.Now())
	require.NoError(t, err)

	rowCountRes2, err := exec.Exec(queryDestCtx4, checkRowCountSQL2, executor.Options{}.WithTxn(txn6))
	require.NoError(t, err)
	defer rowCountRes2.Close()

	var rowCount2 int64
	rowCountRes2.ReadRows(func(rows int, cols []*vector.Vector) bool {
		require.Equal(t, 1, rows)
		require.Equal(t, 1, len(cols))
		rowCount2 = vector.GetFixedAtWithTypeCheck[int64](cols[0], 0)
		return true
	})
	require.Equal(t, int64(15), rowCount2, "destination table should have 15 rows after iteration2 (recreated table)")

	err = txn6.Commit(queryDestCtx4)
	require.NoError(t, err)
}

func TestCCPRErrorHandling1(t *testing.T) {
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
	bat := catalog2.MockBatch(schema, 10)
	defer bat.Close()
	err = rel.Write(srcCtxWithTimeout, containers.ToCNBatch(bat))
	require.NoError(t, err)

	err = txn.Commit(srcCtxWithTimeout)
	require.NoError(t, err)

	// Step 2: Write mo_ccpr_log table
	taskID := uint64(1)
	iterationLSN1 := uint64(0)
	subscriptionName := "test_subscription_retryable_error"

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
			'%s', 
			'table', 
			%d,
			'%s', 
			'%s', 
			'%s', 
			'{}', 
			%d, 
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
		publication.SubscriptionStateRunning,
		publication.IterationStateRunning,
		iterationLSN1,
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
	mp, err := mpool.NewMPool("test_execute_iteration_retryable_error", 0, mpool.NoFixed)
	require.NoError(t, err)

	// Step 4: Create UTHelper for checkpointing
	checkpointDone1 := make(chan struct{}, 1)
	utHelper1 := &checkpointUTHelper{
		taeHandler:    taeHandler,
		disttaeEngine: disttaeEngine,
		checkpointC:   checkpointDone1,
	}

	// Enable fault injection
	fault.Enable()
	defer fault.Disable()

	// Inject retryable error - this will trigger in iterationctx commit before
	rmFn, err := objectio.InjectPublicationSnapshotFinished("ut injection: commit failed retryable")
	require.NoError(t, err)

	// Execute first ExecuteIteration - should fail due to injection, but error is retryable
	err = publication.ExecuteIteration(
		context.Background(),
		cnUUID,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		taskID,
		iterationLSN1,
		upstreamSQLHelperFactory,
		mp,
		utHelper1,
		100*time.Millisecond, // snapshotFlushInterval for test
	)

	// Signal checkpoint goroutine to stop
	close(checkpointDone1)

	// First iteration should fail but error is retryable
	require.NoError(t, err, "First ExecuteIteration should fail due to injection")

	// Step 5: Check mo_ccpr_log after first iteration
	// Verify lsn, state, iteration_state, and error_message
	querySQL1 := fmt.Sprintf(
		`SELECT iteration_lsn, state, iteration_state, error_message 
		FROM mo_catalog.mo_ccpr_log WHERE task_id = %d`,
		taskID,
	)

	v, ok := runtime.ServiceRuntime("").GetGlobalVariables(runtime.InternalSQLExecutor)
	require.True(t, ok)
	exec := v.(executor.SQLExecutor)

	querySystemCtx := context.WithValue(destCtxWithTimeout, defines.TenantIDKey{}, catalog.System_Account)
	txn, err = disttaeEngine.NewTxnOperator(querySystemCtx, disttaeEngine.Now())
	require.NoError(t, err)

	res1, err := exec.Exec(querySystemCtx, querySQL1, executor.Options{}.WithTxn(txn))
	require.NoError(t, err)
	defer res1.Close()

	// Check that all fields are as expected
	var found1 bool
	var iterationLSNFromDB uint64
	var stateFromDB int8
	var iterationStateFromDB int8
	var errorMessageFromDB string
	res1.ReadRows(func(rows int, cols []*vector.Vector) bool {
		require.Equal(t, 1, rows)
		require.Equal(t, 4, len(cols))

		iterationLSNFromDB = uint64(vector.GetFixedAtWithTypeCheck[int64](cols[0], 0))
		stateFromDB = vector.GetFixedAtWithTypeCheck[int8](cols[1], 0)
		iterationStateFromDB = vector.GetFixedAtWithTypeCheck[int8](cols[2], 0)
		errorMessageFromDB = cols[3].GetStringAt(0)

		found1 = true
		return true
	})
	require.True(t, found1, "should find the iteration record after first iteration")

	// Verify lsn should still be iterationLSN1 (no increment on retryable error)
	require.Equal(t, iterationLSN1, iterationLSNFromDB, "iteration_lsn should remain the same after retryable error")

	// Verify state should still be running (retryable error doesn't change subscription state)
	require.Equal(t, publication.SubscriptionStateRunning, stateFromDB, "state should be running after retryable error")

	// Verify iteration_state should be completed (retryable error sets state to completed)
	require.Equal(t, publication.IterationStateCompleted, iterationStateFromDB, "iteration_state should be completed after retryable error")

	// Verify error_message should contain retryable error format
	require.NotEmpty(t, errorMessageFromDB, "error_message should not be empty after retryable error")
	require.Contains(t, errorMessageFromDB, "ut injection: commit failed retryable", "error_message should contain injection message")

	err = txn.Commit(querySystemCtx)
	require.NoError(t, err)

	rmFn()
	// Step 6: Update mo_ccpr_log for second iteration (retryable error)
	iterationLSN2 := uint64(1)
	updateSQL2 := fmt.Sprintf(
		`UPDATE mo_catalog.mo_ccpr_log 
		SET iteration_state = %d, iteration_lsn = %d, error_message = ''
		WHERE task_id = %d`,
		publication.IterationStateRunning,
		iterationLSN2,
		taskID,
	)

	// Update mo_ccpr_log using system account context
	err = exec_sql(disttaeEngine, systemCtx, updateSQL2)
	require.NoError(t, err)

	// Inject retryable error for second iteration
	rmFn2, err := objectio.InjectPublicationSnapshotFinished("ut injection: commit failed retryable")
	require.NoError(t, err)

	// Execute second ExecuteIteration - should fail due to injection, but error is retryable
	checkpointDone2 := make(chan struct{}, 1)
	utHelper2 := &checkpointUTHelper{
		taeHandler:    taeHandler,
		disttaeEngine: disttaeEngine,
		checkpointC:   checkpointDone2,
	}

	err = publication.ExecuteIteration(
		context.Background(),
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

	// Signal checkpoint goroutine to stop
	close(checkpointDone2)

	// Second iteration should fail but error is retryable
	require.NoError(t, err, "Second ExecuteIteration should fail due to injection")

	// Verify second iteration error message
	querySQL2 := fmt.Sprintf(
		`SELECT iteration_lsn, state, iteration_state, error_message 
		FROM mo_catalog.mo_ccpr_log WHERE task_id = %d`,
		taskID,
	)

	txn2, err := disttaeEngine.NewTxnOperator(querySystemCtx, disttaeEngine.Now())
	require.NoError(t, err)

	res2, err := exec.Exec(querySystemCtx, querySQL2, executor.Options{}.WithTxn(txn2))
	require.NoError(t, err)
	defer res2.Close()

	var found2 bool
	var iterationLSNFromDB2 uint64
	var stateFromDB2 int8
	var iterationStateFromDB2 int8
	var errorMessageFromDB2 string
	res2.ReadRows(func(rows int, cols []*vector.Vector) bool {
		require.Equal(t, 1, rows)
		require.Equal(t, 4, len(cols))

		iterationLSNFromDB2 = uint64(vector.GetFixedAtWithTypeCheck[int64](cols[0], 0))
		stateFromDB2 = vector.GetFixedAtWithTypeCheck[int8](cols[1], 0)
		iterationStateFromDB2 = vector.GetFixedAtWithTypeCheck[int8](cols[2], 0)
		errorMessageFromDB2 = cols[3].GetStringAt(0)

		found2 = true
		return true
	})
	require.True(t, found2, "should find the iteration record after second iteration")

	// Verify lsn should still be iterationLSN2 (no increment on retryable error)
	require.Equal(t, iterationLSN2, iterationLSNFromDB2, "iteration_lsn should remain the same after retryable error")

	// Verify state should still be running (retryable error doesn't change subscription state)
	require.Equal(t, publication.SubscriptionStateRunning, stateFromDB2, "state should be running after retryable error")

	// Verify iteration_state should be completed (retryable error sets state to completed)
	require.Equal(t, publication.IterationStateCompleted, iterationStateFromDB2, "iteration_state should be completed after retryable error")

	// Verify error_message should contain retryable error format
	require.NotEmpty(t, errorMessageFromDB2, "error_message should not be empty after retryable error")
	require.Contains(t, errorMessageFromDB2, "ut injection: commit failed retryable", "error_message should contain injection message")

	err = txn2.Commit(querySystemCtx)
	require.NoError(t, err)

	rmFn2()
	// Step 7: Update mo_ccpr_log for third iteration (non-retryable error)
	iterationLSN3 := uint64(2)
	updateSQL3 := fmt.Sprintf(
		`UPDATE mo_catalog.mo_ccpr_log 
		SET iteration_state = %d, iteration_lsn = %d, error_message = ''
		WHERE task_id = %d`,
		publication.IterationStateRunning,
		iterationLSN3,
		taskID,
	)

	// Update mo_ccpr_log using system account context
	err = exec_sql(disttaeEngine, systemCtx, updateSQL3)
	require.NoError(t, err)

	// Inject non-retryable error for third iteration (message without "retryable" keyword)
	rmFn3, err := objectio.InjectPublicationSnapshotFinished("ut injection: commit failed")
	require.NoError(t, err)

	// Execute third ExecuteIteration - should fail with non-retryable error
	checkpointDone3 := make(chan struct{}, 1)
	utHelper3 := &checkpointUTHelper{
		taeHandler:    taeHandler,
		disttaeEngine: disttaeEngine,
		checkpointC:   checkpointDone3,
	}

	err = publication.ExecuteIteration(
		context.Background(),
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

	// Signal checkpoint goroutine to stop
	close(checkpointDone3)

	// Third iteration should fail with non-retryable error
	require.NoError(t, err, "Third ExecuteIteration should fail due to injection")

	// Verify third iteration error message and state
	querySQL3 := fmt.Sprintf(
		`SELECT iteration_lsn, state, iteration_state, error_message 
		FROM mo_catalog.mo_ccpr_log WHERE task_id = %d`,
		taskID,
	)

	txn3, err := disttaeEngine.NewTxnOperator(querySystemCtx, disttaeEngine.Now())
	require.NoError(t, err)

	res3, err := exec.Exec(querySystemCtx, querySQL3, executor.Options{}.WithTxn(txn3))
	require.NoError(t, err)
	defer res3.Close()

	var found3 bool
	var iterationLSNFromDB3 uint64
	var stateFromDB3 int8
	var iterationStateFromDB3 int8
	var errorMessageFromDB3 string
	res3.ReadRows(func(rows int, cols []*vector.Vector) bool {
		require.Equal(t, 1, rows)
		require.Equal(t, 4, len(cols))

		iterationLSNFromDB3 = uint64(vector.GetFixedAtWithTypeCheck[int64](cols[0], 0))
		stateFromDB3 = vector.GetFixedAtWithTypeCheck[int8](cols[1], 0)
		iterationStateFromDB3 = vector.GetFixedAtWithTypeCheck[int8](cols[2], 0)
		errorMessageFromDB3 = cols[3].GetStringAt(0)

		found3 = true
		return true
	})
	require.True(t, found3, "should find the iteration record after third iteration")

	// Verify lsn should still be iterationLSN3 (no increment on non-retryable error)
	require.Equal(t, iterationLSN3, iterationLSNFromDB3, "iteration_lsn should remain the same after non-retryable error")

	// Verify state should be error (non-retryable error changes subscription state to error)
	require.Equal(t, publication.SubscriptionStateError, stateFromDB3, "state should be error after non-retryable error")

	// Verify iteration_state should be error (non-retryable error sets state to error)
	require.Equal(t, publication.IterationStateError, iterationStateFromDB3, "iteration_state should be error after non-retryable error")

	// Verify error_message should contain non-retryable error format (starts with "N:")
	require.NotEmpty(t, errorMessageFromDB3, "error_message should not be empty after non-retryable error")
	require.Contains(t, errorMessageFromDB3, "ut injection: commit failed", "error_message should contain injection message")
	require.True(t, strings.HasPrefix(errorMessageFromDB3, "N:"), "error_message should start with 'N:' for non-retryable error")

	err = txn3.Commit(querySystemCtx)
	require.NoError(t, err)

	rmFn3()
	// Step 8: Reset mo_ccpr_log table and manually write a retryable error, then execute normal iteration
	// Reset the table
	resetSQL := fmt.Sprintf(
		`UPDATE mo_catalog.mo_ccpr_log 
		SET state = %d, iteration_state = %d, iteration_lsn = %d, error_message = 'R:1:%d:%d:ut injection: commit failed retryable'
		WHERE task_id = %d`,
		publication.SubscriptionStateRunning,
		publication.IterationStateRunning,
		iterationLSN3,
		time.Now().Unix(),
		time.Now().Unix(),
		taskID,
	)

	err = exec_sql(disttaeEngine, systemCtx, resetSQL)
	require.NoError(t, err)

	// Execute fourth ExecuteIteration - should succeed normally (no injection)
	iterationLSN4 := iterationLSN3
	checkpointDone4 := make(chan struct{}, 1)
	utHelper4 := &checkpointUTHelper{
		taeHandler:    taeHandler,
		disttaeEngine: disttaeEngine,
		checkpointC:   checkpointDone4,
	}

	err = publication.ExecuteIteration(
		context.Background(),
		cnUUID,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		taskID,
		iterationLSN4,
		upstreamSQLHelperFactory,
		mp,
		utHelper4,
		100*time.Millisecond, // snapshotFlushInterval for test
	)

	// Signal checkpoint goroutine to stop
	close(checkpointDone4)

	// Fourth iteration should succeed
	require.NoError(t, err, "Fourth ExecuteIteration should complete successfully")

	// Verify fourth iteration state
	querySQL4 := fmt.Sprintf(
		`SELECT iteration_state, iteration_lsn FROM mo_catalog.mo_ccpr_log WHERE task_id = %d`,
		taskID,
	)

	txn4, err := disttaeEngine.NewTxnOperator(querySystemCtx, disttaeEngine.Now())
	require.NoError(t, err)

	res4, err := exec.Exec(querySystemCtx, querySQL4, executor.Options{}.WithTxn(txn4))
	require.NoError(t, err)
	defer res4.Close()

	var found4 bool
	res4.ReadRows(func(rows int, cols []*vector.Vector) bool {
		require.Equal(t, 1, rows)
		require.Equal(t, 2, len(cols))

		state := vector.GetFixedAtWithTypeCheck[int8](cols[0], 0)
		lsn := vector.GetFixedAtWithTypeCheck[int64](cols[1], 0)

		require.Equal(t, publication.IterationStateCompleted, state)
		require.Equal(t, int64(iterationLSN4+1), lsn)
		found4 = true
		return true
	})
	require.True(t, found4, "should find the updated iteration record")

	err = txn4.Commit(querySystemCtx)
	require.NoError(t, err)

	// Step 9: Inject error in sql executor for fifth iteration
	iterationLSN5 := iterationLSN4 + 1
	updateSQL5 := fmt.Sprintf(
		`UPDATE mo_catalog.mo_ccpr_log 
		SET iteration_state = %d, iteration_lsn = %d, error_message = ''
		WHERE task_id = %d`,
		publication.IterationStateRunning,
		iterationLSN5,
		taskID,
	)

	err = exec_sql(disttaeEngine, systemCtx, updateSQL5)
	require.NoError(t, err)

	// Inject error in sql executor
	rmFn5, err := objectio.InjectPublicationSnapshotFinished("ut injection: sql fail")
	require.NoError(t, err)

	// Execute fifth ExecuteIteration - should fail due to sql executor injection
	checkpointDone5 := make(chan struct{}, 1)
	utHelper5 := &checkpointUTHelper{
		taeHandler:    taeHandler,
		disttaeEngine: disttaeEngine,
		checkpointC:   checkpointDone5,
	}

	err = publication.ExecuteIteration(
		context.Background(),
		cnUUID,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		taskID,
		iterationLSN5,
		upstreamSQLHelperFactory,
		mp,
		utHelper5,
		100*time.Millisecond, // snapshotFlushInterval for test
		&publication.SQLExecutorRetryOption{
			MaxRetries:    2,
			RetryInterval: time.Second,
			Classifier:    nil,
		},
	)

	// Signal checkpoint goroutine to stop
	close(checkpointDone5)

	// Fifth iteration should fail due to sql executor injection
	require.Error(t, err, "Fifth ExecuteIteration should fail due to sql executor injection")
	rmFn5()
}

func TestCCPRDDLAccountLevel(t *testing.T) {
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
	systemCtx := context.WithValue(srcCtxWithTimeout, defines.TenantIDKey{}, catalog.System_Account)
	err := exec_sql(disttaeEngine, systemCtx, frontend.MoCatalogMoIndexesDDL)
	require.NoError(t, err)

	// Create mo_ccpr_log table using system account context
	err = exec_sql(disttaeEngine, systemCtx, frontend.MoCatalogMoCcprLogDDL)
	require.NoError(t, err)

	err = exec_sql(disttaeEngine, systemCtx, frontend.MoCatalogMoAccountDDL)
	require.NoError(t, err)

	err = exec_sql(disttaeEngine, srcCtxWithTimeout, frontend.MoCatalogMoIndexesDDL)
	require.NoError(t, err)

	err = exec_sql(disttaeEngine, srcCtxWithTimeout, frontend.MoCatalogMoAccountDDL)
	require.NoError(t, err)
	// Create mo_snapshots table for source account
	moSnapshotsDDL := frontend.MoCatalogMoSnapshotsDDL
	err = exec_sql(disttaeEngine, srcCtxWithTimeout, moSnapshotsDDL)
	require.NoError(t, err)

	// Create system tables for destination account
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

	// Step 1: Create source database in source account
	srcDBName := "test_account_db"
	txn, err := disttaeEngine.NewTxnOperator(srcCtxWithTimeout, disttaeEngine.Now())
	require.NoError(t, err)

	err = disttaeEngine.Engine.Create(srcCtxWithTimeout, srcDBName, txn)
	require.NoError(t, err)

	err = txn.Commit(srcCtxWithTimeout)
	require.NoError(t, err)

	// Step 2: Write mo_ccpr_log table with account level sync
	taskID := uint64(1)
	iterationLSN1 := uint64(1)
	subscriptionName := "test_subscription_account"
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
			'%s', 
			'account', 
			%d,
			'', 
			'', 
			'%s', 
			'{}', 
			%d, 
			%d, 
			%d, 
			'%s'
		)`,
		taskID,
		subscriptionName,
		destAccountID,
		fmt.Sprintf("%s:%d", publication.InternalSQLExecutorType, srcAccountID),
		publication.SubscriptionStateRunning,
		publication.IterationStateRunning,
		iterationLSN1,
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
	mp, err := mpool.NewMPool("test_account_db_create_delete", 0, mpool.NoFixed)
	require.NoError(t, err)

	// Step 4: Create UTHelper for checkpointing
	checkpointDone1 := make(chan struct{}, 1)
	utHelper1 := &checkpointUTHelper{
		taeHandler:    taeHandler,
		disttaeEngine: disttaeEngine,
		checkpointC:   checkpointDone1,
	}

	// Step 5: Execute first iteration
	err = publication.ExecuteIteration(
		context.Background(),
		cnUUID,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		taskID,
		iterationLSN1,
		upstreamSQLHelperFactory,
		mp,
		utHelper1,
		100*time.Millisecond, // snapshotFlushInterval for test
	)

	// Signal checkpoint goroutine to stop
	close(checkpointDone1)

	// Check errors
	require.NoError(t, err, "First ExecuteIteration should complete successfully")

	// Step 6: Verify that the iteration state was updated
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
		require.Equal(t, int64(iterationLSN1+1), lsn)
		found = true
		return true
	})
	require.True(t, found, "should find the updated iteration record")

	err = txn.Commit(querySystemCtx)
	require.NoError(t, err)

	// Step 7: Check that downstream database exists after iteration
	queryDestCtx := context.WithValue(destCtxWithTimeout, defines.TenantIDKey{}, destAccountID)
	txn, err = disttaeEngine.NewTxnOperator(queryDestCtx, disttaeEngine.Now())
	require.NoError(t, err)

	db, err := disttaeEngine.Engine.Database(queryDestCtx, srcDBName, txn)
	require.NoError(t, err, "downstream database should exist after iteration")
	require.NotNil(t, db, "downstream database should not be nil")

	err = txn.Commit(queryDestCtx)
	require.NoError(t, err)

	// Step 8: Delete the source database
	txnDelete, err := disttaeEngine.NewTxnOperator(srcCtxWithTimeout, disttaeEngine.Now())
	require.NoError(t, err)

	err = disttaeEngine.Engine.Delete(srcCtxWithTimeout, srcDBName, txnDelete)
	require.NoError(t, err)

	err = txnDelete.Commit(srcCtxWithTimeout)
	require.NoError(t, err)

	// Step 9: Update mo_ccpr_log for second iteration
	iterationLSN2 := uint64(2)
	updateSQL := fmt.Sprintf(
		`UPDATE mo_catalog.mo_ccpr_log 
		SET iteration_state = %d, iteration_lsn = %d 
		WHERE task_id = %d`,
		publication.IterationStateRunning,
		iterationLSN2,
		taskID,
	)

	// Update mo_ccpr_log using system account context
	err = exec_sql(disttaeEngine, systemCtx, updateSQL)
	require.NoError(t, err)

	// Step 10: Create new checkpoint channel for second iteration
	checkpointDone2 := make(chan struct{}, 1)
	utHelper2 := &checkpointUTHelper{
		taeHandler:    taeHandler,
		disttaeEngine: disttaeEngine,
		checkpointC:   checkpointDone2,
	}

	// Step 11: Execute second iteration
	err = publication.ExecuteIteration(
		context.Background(),
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

	// Signal checkpoint goroutine to stop
	close(checkpointDone2)

	// Check errors
	require.NoError(t, err, "Second ExecuteIteration should complete successfully")

	// Step 12: Verify that the second iteration state was updated
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
		require.Equal(t, int64(iterationLSN2+1), lsn)
		found2 = true
		return true
	})
	require.True(t, found2, "should find the updated iteration record for second iteration")

	err = txn2.Commit(querySystemCtx2)
	require.NoError(t, err)

	// Step 13: Check that downstream database does NOT exist after iteration
	queryDestCtx2 := context.WithValue(destCtxWithTimeout, defines.TenantIDKey{}, destAccountID)
	txn3, err := disttaeEngine.NewTxnOperator(queryDestCtx2, disttaeEngine.Now())
	require.NoError(t, err)

	_, err = disttaeEngine.Engine.Database(queryDestCtx2, srcDBName, txn3)
	require.Error(t, err, "downstream database should not exist after iteration (database was deleted)")
	// Check that the error is the expected "does not exist" error
	require.Contains(t, err.Error(), "ExpectedEOB", "error should indicate database does not exist")

	err = txn3.Commit(queryDestCtx2)
	require.NoError(t, err)

	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
}

func TestCCPRExecutorWithGC(t *testing.T) {
	catalog.SetupDefines("")

	var (
		srcAccountID  = catalog.System_Account
		destAccountID = uint32(2)
		cnUUID        = ""
	)

	// Generate a UUID for cnUUID
	testUUID, err := uuid.NewV7()
	require.NoError(t, err)
	cnUUID = testUUID.String()

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

	// Get or create runtime for the new cnUUID
	existingRuntime := runtime.ServiceRuntime("")
	runtime.SetupServiceBasedRuntime(cnUUID, existingRuntime)

	// Create mo_ccpr_log table using system account context
	systemCtx := context.WithValue(srcCtxWithTimeout, defines.TenantIDKey{}, catalog.System_Account)

	// Create mo_task database
	createMoTaskDBSQL := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", catalog.MOTaskDB)
	err = exec_sql(disttaeEngine, systemCtx, createMoTaskDBSQL)
	require.NoError(t, err)

	// Create mo_task.sys_daemon_task table
	err = exec_sql(disttaeEngine, systemCtx, frontend.MoTaskSysDaemonTaskDDL)
	require.NoError(t, err)

	// Insert a record into mo_task.sys_daemon_task for lease check
	insertDaemonTaskSQL := fmt.Sprintf(
		`INSERT INTO mo_task.sys_daemon_task (
			task_id,
			task_metadata_id,
			task_metadata_executor,
			task_metadata_context,
			task_metadata_option,
			account_id,
			account,
			task_type,
			task_runner,
			task_status,
			last_heartbeat,
			create_at,
			update_at
		) VALUES (
		    1,
			'%s',
			0,
			NULL,
			NULL,
			%d,
			'sys',
			'Publication',
			'%s',
			0,
			utc_timestamp(),
			utc_timestamp(),
			utc_timestamp()
		)`,
		cnUUID,
		catalog.System_Account,
		cnUUID,
	)
	err = exec_sql(disttaeEngine, systemCtx, insertDaemonTaskSQL)
	require.NoError(t, err)

	// Create mo_indexes table for source account
	err = exec_sql(disttaeEngine, srcCtxWithTimeout, frontend.MoCatalogMoIndexesDDL)
	require.NoError(t, err)

	// Create mo_ccpr_log table using system account context
	err = exec_sql(disttaeEngine, systemCtx, frontend.MoCatalogMoCcprLogDDL)
	require.NoError(t, err)
	// Create mo_foreign_keys table using system account context
	err = exec_sql(disttaeEngine, systemCtx, frontend.MoCatalogMoForeignKeysDDL)
	require.NoError(t, err)

	// Create mo_snapshots table for source account
	moSnapshotsDDL := frontend.MoCatalogMoSnapshotsDDL
	err = exec_sql(disttaeEngine, srcCtxWithTimeout, moSnapshotsDDL)
	require.NoError(t, err)

	// Create system tables for destination account
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
	bat := catalog2.MockBatch(schema, 10)
	defer bat.Close()
	err = rel.Write(srcCtxWithTimeout, containers.ToCNBatch(bat))
	require.NoError(t, err)

	err = txn.Commit(srcCtxWithTimeout)
	require.NoError(t, err)

	// Force checkpoint after inserting data
	err = taeHandler.GetDB().ForceCheckpoint(srcCtxWithTimeout, types.TimestampToTS(disttaeEngine.Now()))
	require.NoError(t, err)

	// Step 2: Create upstream SQL helper factory
	upstreamSQLHelperFactory := func(
		txnOp client.TxnOperator,
		engine engine.Engine,
		accountID uint32,
		exec executor.SQLExecutor,
		txnClient client.TxnClient,
	) publication.UpstreamSQLHelper {
		return NewUpstreamSQLHelper(txnOp, engine, accountID, exec, txnClient)
	}

	// Create mpool for executor
	mp, err := mpool.NewMPool("test_ccpr_executor_gc", 0, mpool.NoFixed)
	require.NoError(t, err)

	// Step 3: Create and start publication executor
	executorCtx, executorCancel := context.WithCancel(context.Background())
	defer executorCancel()

	executorOption := &publication.PublicationExecutorOption{
		GCInterval:       time.Hour * 24,
		GCTTL:            time.Hour * 24,
		SyncTaskInterval: 100 * time.Millisecond, // Short interval for testing
	}

	exec, err := publication.NewPublicationTaskExecutor(
		executorCtx,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		cnUUID,
		executorOption,
		mp,
		upstreamSQLHelperFactory,
	)
	require.NoError(t, err)

	// Start the executor
	err = exec.Start()
	require.NoError(t, err)
	defer exec.Stop()

	// Step 4: Insert mo_ccpr_log record
	taskID := uint64(1)
	iterationLSN := uint64(1)
	subscriptionName := "test_subscription_executor_gc"
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
			'%s',
			'table',
			%d,
			'%s',
			'%s',
			'%s',
			'{}',
			%d,
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
		publication.SubscriptionStateRunning,
		publication.IterationStateCompleted,
		iterationLSN,
		cnUUID,
	)

	// Write mo_ccpr_log using system account context
	err = exec_sql(disttaeEngine, systemCtx, insertSQL)
	require.NoError(t, err)

	// Step 5: Wait for executor to pick up and execute the task
	// Poll mo_ccpr_log to check if iteration is completed
	v, ok := runtime.ServiceRuntime("").GetGlobalVariables(runtime.InternalSQLExecutor)
	require.True(t, ok)
	sqlExec := v.(executor.SQLExecutor)

	maxWaitTime := 30 * time.Second
	checkInterval := 100 * time.Millisecond
	startTime := time.Now()
	var completed bool

	for time.Since(startTime) < maxWaitTime {
		querySQL := fmt.Sprintf(
			`SELECT iteration_state, iteration_lsn FROM mo_catalog.mo_ccpr_log WHERE task_id = %d`,
			taskID,
		)

		querySystemCtx := context.WithValue(destCtxWithTimeout, defines.TenantIDKey{}, catalog.System_Account)
		txn, err := disttaeEngine.NewTxnOperator(querySystemCtx, disttaeEngine.Now())
		require.NoError(t, err)

		res, err := sqlExec.Exec(querySystemCtx, querySQL, executor.Options{}.WithTxn(txn))
		require.NoError(t, err)

		res.ReadRows(func(rows int, cols []*vector.Vector) bool {
			if rows > 0 {
				state := vector.GetFixedAtWithTypeCheck[int8](cols[0], 0)
				lsn := vector.GetFixedAtWithTypeCheck[int64](cols[1], 0)

				if state == publication.IterationStateCompleted && lsn == int64(iterationLSN+1) {
					completed = true
				}
			}
			return true
		})
		res.Close()

		err = txn.Commit(querySystemCtx)
		require.NoError(t, err)

		if completed {
			break
		}

		time.Sleep(checkInterval)
	}

	require.True(t, completed, "iteration should complete within max wait time")

	// Step 6: Update drop_at for the record
	dropAtTime := time.Now().Add(-2 * time.Hour) // Set drop_at to 2 hours ago
	dropAtStr := dropAtTime.Format("2006-01-02 15:04:05")
	updateSQL := fmt.Sprintf(
		`UPDATE mo_catalog.mo_ccpr_log
		SET drop_at = '%s'
		WHERE task_id = %d`,
		dropAtStr,
		taskID,
	)

	// w-w occurs
	for i := 0; i < 10; i++ {
		err = exec_sql(disttaeEngine, systemCtx, updateSQL)
		if err == nil {
			break
		}
	}
	require.NoError(t, err)

	// Step 7: Wait for executor to sync and update task entry in memory
	// Poll executor's task entry to check if drop_at was updated
	maxWaitTime2 := 10 * time.Second
	checkInterval2 := 100 * time.Millisecond
	startTime2 := time.Now()
	var dropAtUpdatedInMemory bool

	for time.Since(startTime2) < maxWaitTime2 {
		taskEntry, ok := exec.GetTask(taskID)
		if ok && taskEntry != nil && taskEntry.DropAt != nil {
			// Verify drop_at is approximately 2 hours ago (allow some tolerance)
			expectedTime := dropAtTime
			actualTime := *taskEntry.DropAt
			diff := actualTime.Sub(expectedTime)
			if diff < time.Minute && diff > -time.Minute {
				dropAtUpdatedInMemory = true
				break
			}
		}
		time.Sleep(checkInterval2)
	}

	require.True(t, dropAtUpdatedInMemory, "drop_at should be updated in executor's task entry")

	// Step 8: Manually trigger GC
	exec.GCInMemoryTask(0)
	_, ok = exec.GetTask(taskID)
	require.False(t, ok, "task should be deleted by GC")

	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
}
