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
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/publication"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/stretchr/testify/require"
)

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

func TestExecuteIteration(t *testing.T) {
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

	// Create mo_indexes table
	err := exec_sql(disttaeEngine, srcCtxWithTimeout, frontend.MoCatalogMoIndexesDDL)
	require.NoError(t, err)

	// Create mo_ccpr_log table using system account context
	systemCtx := context.WithValue(srcCtxWithTimeout, defines.TenantIDKey{}, catalog.System_Account)
	err = exec_sql(disttaeEngine, systemCtx, frontend.MoCatalogMoCcprLogDDL)
	require.NoError(t, err)

	// Create mo_snapshots table (if needed)
	moSnapshotsDDL := `CREATE TABLE IF NOT EXISTS mo_catalog.mo_snapshots (
		sname VARCHAR(5000) NOT NULL PRIMARY KEY,
		ts BIGINT NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`
	err = exec_sql(disttaeEngine, srcCtxWithTimeout, moSnapshotsDDL)
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
	err = rel.Write(srcCtxWithTimeout, containers.ToCNBatch(bat))
	require.NoError(t, err)

	err = txn.Commit(srcCtxWithTimeout)
	require.NoError(t, err)

	// Force flush the table
	dbID := rel.GetDBID(srcCtxWithTimeout)
	tableID := rel.GetTableID(srcCtxWithTimeout)
	err = taeHandler.GetDB().FlushTable(srcCtxWithTimeout, srcAccountID, dbID, tableID, types.TimestampToTS(disttaeEngine.Now()))
	require.NoError(t, err)

	// Step 2: Write mo_ccpr_log table in destination account context
	// Note: We need to use destination account context to write mo_ccpr_log
	// but the upstream_conn should point to InternalSQLExecutorType
	taskID := uint64(1)
	iterationLSN := uint64(1)
	iterationState := publication.IterationStatePending
	subscriptionName := "test_subscription"
	insertSQL := fmt.Sprintf(
		`INSERT INTO mo_catalog.mo_ccpr_log (
			task_id, 
			subscription_name, 
			sync_level, 
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
			'%s', 
			'%s', 
			'%s', 
			'{}', 
			0, 
			%d, 
			%d, 
			'%s'
		)`,
		taskID,
		subscriptionName,
		srcDBName,
		srcTableName,
		fmt.Sprintf("%s:%d", publication.InternalSQLExecutorType, srcAccountID),
		publication.IterationStatePending,
		iterationLSN,
		cnUUID,
	)

	// Write mo_ccpr_log using system account context
	// mo_ccpr_log is a system table, so we must use system account
	err = exec_sql(disttaeEngine, systemCtx, insertSQL)
	require.NoError(t, err)

	// Step 3: Call ExecuteIteration
	// Use destination account context for local operations
	// The upstream account ID is now read from upstream_conn in mo_ccpr_log table

	err = publication.ExecuteIteration(
		srcCtxWithTimeout,
		cnUUID,
		disttaeEngine.Engine,
		disttaeEngine.GetTxnClient(),
		taskID,
		iterationLSN,
		iterationState,
	)

	// The iteration should complete successfully
	require.NoError(t, err)

	// Verify that the iteration state was updated
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

	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
}
