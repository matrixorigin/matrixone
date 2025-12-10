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
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/publication"
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

	// Create mo_ccpr_log table using DDL from frontend/predefine (only once)
	err = exec_sql(disttaeEngine, ctxWithTimeout, frontend.MoCatalogMoCcprLogDDL)
	require.NoError(t, err)

	// Create InternalSQLExecutor (only once)
	executor, err := publication.NewInternalSQLExecutor("")
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

				err := exec_sql(disttaeEngine, ctxWithTimeout, insertSQL)
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
