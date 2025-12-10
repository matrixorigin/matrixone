// Copyright 2021 Matrix Origin
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

package publication

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// IterationState represents the state of an iteration
const (
	IterationStatePending   int8 = 0 // 'pending'
	IterationStateRunning   int8 = 1 // 'running'
	IterationStateCompleted int8 = 2 // 'complete'
	IterationStateError     int8 = 3 // 'error'
	IterationStateCanceled  int8 = 4 // 'cancel'
)

// SQLExecutor is an interface for executing SQL queries
// This is a temporary interface, implementation will be added later
type SQLExecutor interface {
	// Exec executes a SQL query and returns the result
	// The result should contain columns: cn_uuid, iteration_state, iteration_lsn
	Exec(ctx context.Context, sql string) (cnUUID string, iterationState int8, iterationLSN uint64, err error)
}

// CheckIterationStatus checks the iteration status in mo_ccpr_log table
// It verifies that cn_uuid, iteration_lsn match the expected values,
// and that iteration_state is completed
func checkIterationStatus(
	ctx context.Context,
	executor SQLExecutor,
	taskID uint64,
	expectedCNUUID string,
	expectedIterationLSN uint64,
) error {
	// Build SQL query using sql_builder
	sql := PublicationSQLBuilder.QueryMoCcprLogSQL(taskID)

	// Execute SQL query
	cnUUID, iterationState, iterationLSN, err := executor.Exec(ctx, sql)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to execute query: %v", err)
	}

	// Check if cn_uuid matches
	if cnUUID != expectedCNUUID {
		return moerr.NewInternalErrorf(ctx, "cn_uuid mismatch: expected %s, got %s", expectedCNUUID, cnUUID)
	}

	// Check if iteration_lsn matches
	if iterationLSN != expectedIterationLSN {
		return moerr.NewInternalErrorf(ctx, "iteration_lsn mismatch: expected %d, got %d", expectedIterationLSN, iterationLSN)
	}

	// Check if iteration_state is completed
	if iterationState != IterationStateCompleted {
		return moerr.NewInternalErrorf(ctx, "iteration_state is not completed: expected %d (completed), got %d", IterationStateCompleted, iterationState)
	}

	return nil
}
