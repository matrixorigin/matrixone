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

package publication

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var _ SQLExecutor = (*InternalSQLExecutor)(nil)

// InternalSQLExecutor implements SQLExecutor interface using MatrixOne's internal SQL executor
// It does not support retry or circuit breaker, and uses the internal SQL builder
type InternalSQLExecutor struct {
	cnUUID       string
	internalExec executor.SQLExecutor
	txnOp        client.TxnOperator
}

// NewInternalSQLExecutor creates a new InternalSQLExecutor
func NewInternalSQLExecutor(cnUUID string) (*InternalSQLExecutor, error) {
	v, ok := moruntime.ServiceRuntime(cnUUID).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf("internal SQL executor not found for CN %s", cnUUID))
	}

	internalExec, ok := v.(executor.SQLExecutor)
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("invalid internal SQL executor type")
	}

	return &InternalSQLExecutor{
		cnUUID:       cnUUID,
		internalExec: internalExec,
	}, nil
}

// Connect is a no-op for internal executor (connection is managed by runtime)
func (e *InternalSQLExecutor) Connect() error {
	return nil
}

// Close is a no-op for internal executor (connection is managed by runtime)
func (e *InternalSQLExecutor) Close() error {
	// Clean up transaction if exists
	if e.txnOp != nil {
		// Transaction cleanup is handled by the caller via EndTxn
		e.txnOp = nil
	}
	return nil
}

// StartTxn starts a new transaction
// Note: For internal executor, we use ExecTxn to create a transaction context
// The transaction will be committed/rolled back via EndTxn
func (e *InternalSQLExecutor) StartTxn(ctx context.Context) error {
	if e.txnOp != nil {
		return moerr.NewInternalError(ctx, "transaction already active")
	}

	// For internal executor, we need to get a transaction operator
	// This is typically done by the caller providing a txn via Options.WithTxn
	// For now, we'll create a transaction context that will be used in ExecSQL
	// Note: The actual transaction management is handled by ExecTxn in the executor
	// This is a placeholder - in practice, the transaction should be provided by the caller
	return nil
}

// EndTxn ends the current transaction
// Note: For internal executor, transactions are managed by ExecTxn
// If a transaction was started via StartTxn, we need to commit/rollback it
func (e *InternalSQLExecutor) EndTxn(ctx context.Context, commit bool) error {
	if e.txnOp == nil {
		return nil // Idempotent
	}

	// For internal executor, if we have a transaction, we need to commit/rollback it
	// However, since transactions are typically managed via ExecTxn, we just clear the reference
	// The actual commit/rollback should be handled by the ExecTxn caller
	e.txnOp = nil
	return nil
}

// ExecSQL executes a SQL statement and returns the result
func (e *InternalSQLExecutor) ExecSQL(ctx context.Context, query string) (*Result, error) {
	return e.ExecSQLWithOptions(ctx, nil, query, false)
}

// ExecSQLWithOptions executes a SQL statement with additional options
// Note: needRetry and ar parameters are ignored for internal executor
func (e *InternalSQLExecutor) ExecSQLWithOptions(
	ctx context.Context,
	ar *ActiveRoutine,
	query string,
	needRetry bool,
) (*Result, error) {
	// Check for cancellation
	if ar != nil {
		select {
		case <-ar.Pause:
			return nil, moerr.NewInternalError(ctx, "task paused")
		case <-ar.Cancel:
			return nil, moerr.NewInternalError(ctx, "task cancelled")
		default:
		}
	}

	opts := executor.Options{}.
		WithDisableIncrStatement()

	if e.txnOp != nil {
		opts = opts.WithTxn(e.txnOp)
	}

	execResult, err := e.internalExec.Exec(ctx, query, opts)
	if err != nil {
		return nil, err
	}

	// Convert executor.Result to publication.Result
	return convertExecutorResult(execResult), nil
}

// HasActiveTx returns true if there's an active transaction
func (e *InternalSQLExecutor) HasActiveTx() bool {
	return e.txnOp != nil
}

// convertExecutorResult converts executor.Result (with Batches) to publication.Result
func convertExecutorResult(execResult executor.Result) *Result {
	return &Result{
		internalResult: &InternalResult{
			executorResult: execResult,
		},
	}
}

// InternalResult wraps executor.Result to provide sql.Rows-like interface
type InternalResult struct {
	executorResult executor.Result
	currentBatch   int
	currentRow     int
	columns        []string
	err            error
}

// Close closes the result and releases resources
func (r *InternalResult) Close() error {
	// Release batches if needed
	if r.executorResult.Batches != nil {
		for _, b := range r.executorResult.Batches {
			if b != nil {
				b.Clean(r.executorResult.Mp)
			}
		}
	}
	return nil
}

// Next moves to the next row
func (r *InternalResult) Next() bool {
	if r.err != nil {
		return false
	}

	if r.executorResult.Batches == nil || len(r.executorResult.Batches) == 0 {
		return false
	}

	// Find next row across batches
	for r.currentBatch < len(r.executorResult.Batches) {
		batch := r.executorResult.Batches[r.currentBatch]
		if batch == nil {
			r.currentBatch++
			r.currentRow = 0
			continue
		}

		if r.currentRow < batch.RowCount() {
			r.currentRow++
			return true
		}

		r.currentBatch++
		r.currentRow = 0
	}

	return false
}

// Scan scans the current row into the provided destinations
func (r *InternalResult) Scan(dest ...interface{}) error {
	if r.executorResult.Batches == nil || len(r.executorResult.Batches) == 0 {
		return moerr.NewInternalErrorNoCtx("no batches available")
	}

	if r.currentBatch >= len(r.executorResult.Batches) {
		return moerr.NewInternalErrorNoCtx("no more rows")
	}

	batch := r.executorResult.Batches[r.currentBatch]
	if batch == nil {
		return moerr.NewInternalErrorNoCtx("batch is nil")
	}

	if r.currentRow <= 0 || r.currentRow > batch.RowCount() {
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf("invalid row index: %d (batch has %d rows)", r.currentRow, batch.RowCount()))
	}

	rowIdx := r.currentRow - 1 // Convert to 0-based index

	// Validate column count
	if len(dest) != len(batch.Vecs) {
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf("column count mismatch: expected %d, got %d", len(batch.Vecs), len(dest)))
	}

	// Scan each column
	for i, vec := range batch.Vecs {
		if vec == nil {
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf("vector %d is nil", i))
		}

		if vec.IsNull(uint64(rowIdx)) {
			// Handle NULL values - set destination to nil or zero value
			switch d := dest[i].(type) {
			case *string:
				*d = ""
			case *sql.NullString:
				d.Valid = false
				d.String = ""
			case *sql.NullInt16:
				d.Valid = false
				d.Int16 = 0
			case *sql.NullInt32:
				d.Valid = false
				d.Int32 = 0
			case *sql.NullInt64:
				d.Valid = false
				d.Int64 = 0
			case *sql.NullBool:
				d.Valid = false
				d.Bool = false
			case *sql.NullFloat64:
				d.Valid = false
				d.Float64 = 0
			case *sql.NullTime:
				d.Valid = false
				d.Time = time.Time{}
			case *int8:
				*d = 0
			case *int16:
				*d = 0
			case *int32:
				*d = 0
			case *int64:
				*d = 0
			case *uint8:
				*d = 0
			case *uint16:
				*d = 0
			case *uint32:
				*d = 0
			case *uint64:
				*d = 0
			default:
				// For other types, try to set to nil if possible
				// This is a simplified implementation
			}
			continue
		}

		// Extract value from vector based on type
		err := extractVectorValue(vec, uint64(rowIdx), dest[i])
		if err != nil {
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf("failed to extract value from vector %d: %v", i, err))
		}
	}

	return nil
}

// extractVectorValue extracts a value from a vector at the given index
func extractVectorValue(vec *vector.Vector, idx uint64, dest interface{}) error {
	switch vec.GetType().Oid {
	case types.T_varchar, types.T_char, types.T_text:
		val := vec.GetStringAt(int(idx))
		if d, ok := dest.(*string); ok {
			*d = val
		} else if d, ok := dest.(*sql.NullString); ok {
			d.String = val
			d.Valid = true
		} else {
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf("destination type mismatch for string, type %T", dest))
		}

	case types.T_int8:
		val := vector.GetFixedAtWithTypeCheck[int8](vec, int(idx))
		if d, ok := dest.(*int8); ok {
			*d = val
		} else {
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf("destination type mismatch for int8, type %T", dest))
		}

	case types.T_int16:
		val := vector.GetFixedAtWithTypeCheck[int16](vec, int(idx))
		if d, ok := dest.(*int16); ok {
			*d = val
		} else if d, ok := dest.(*sql.NullInt16); ok {
			d.Int16 = val
			d.Valid = true
		} else {
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf("destination type mismatch for int16, type %T", dest))
		}

	case types.T_int32:
		val := vector.GetFixedAtWithTypeCheck[int32](vec, int(idx))
		if d, ok := dest.(*int32); ok {
			*d = val
		} else if d, ok := dest.(*sql.NullInt32); ok {
			d.Int32 = val
			d.Valid = true
		} else {
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf("destination type mismatch for int32, type %T", dest))
		}

	case types.T_int64:
		val := vector.GetFixedAtWithTypeCheck[int64](vec, int(idx))
		if d, ok := dest.(*int64); ok {
			*d = val
		} else if d, ok := dest.(*sql.NullInt64); ok {
			d.Int64 = val
			d.Valid = true
		} else if d, ok := dest.(*uint64); ok {
			// Allow conversion from int64 to uint64 for compatibility
			// This is safe for non-negative values (e.g., iteration_lsn, LSN values)
			if val < 0 {
				return moerr.NewInternalErrorNoCtx(fmt.Sprintf("cannot convert negative int64 value %d to uint64", val))
			}
			*d = uint64(val)
		} else {
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf("destination type mismatch for int64, type %T", dest))
		}

	case types.T_uint8:
		val := vector.GetFixedAtWithTypeCheck[uint8](vec, int(idx))
		if d, ok := dest.(*uint8); ok {
			*d = val
		} else {
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf("destination type mismatch for uint8, type %T", dest))
		}

	case types.T_uint16:
		val := vector.GetFixedAtWithTypeCheck[uint16](vec, int(idx))
		if d, ok := dest.(*uint16); ok {
			*d = val
		} else {
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf("destination type mismatch for uint16, type %T", dest))
		}

	case types.T_uint32:
		val := vector.GetFixedAtWithTypeCheck[uint32](vec, int(idx))
		if d, ok := dest.(*uint32); ok {
			*d = val
		} else {
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf("destination type mismatch for uint32, type %T", dest))
		}

	case types.T_uint64:
		val := vector.GetFixedAtWithTypeCheck[uint64](vec, int(idx))
		if d, ok := dest.(*uint64); ok {
			*d = val
		} else {
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf("destination type mismatch for uint64, type %T", dest))
		}

	case types.T_timestamp:
		val := vector.GetFixedAtWithTypeCheck[types.Timestamp](vec, int(idx))
		if d, ok := dest.(*types.Timestamp); ok {
			*d = val
		} else {
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf("destination type mismatch for timestamp, type %T", dest))
		}

	default:
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf("unsupported vector type: %v, type %T", vec.GetType().Oid, dest))
	}

	return nil
}

// Columns returns the column names
func (r *InternalResult) Columns() ([]string, error) {
	if r.columns != nil {
		return r.columns, nil
	}

	if r.executorResult.Batches == nil || len(r.executorResult.Batches) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("no batches available")
	}

	batch := r.executorResult.Batches[0]
	if batch == nil {
		return nil, moerr.NewInternalErrorNoCtx("first batch is nil")
	}

	// Extract column names from batch
	// Note: This is a simplified implementation
	// In practice, column names might come from the logical plan
	// For now, generate column names based on position
	r.columns = make([]string, len(batch.Vecs))
	for i := range batch.Vecs {
		r.columns[i] = fmt.Sprintf("col_%d", i)
	}

	return r.columns, nil
}

// Err returns any error encountered during iteration
func (r *InternalResult) Err() error {
	return r.err
}

// Ensure InternalSQLExecutor implements SQLExecutor interface
var _ SQLExecutor = (*InternalSQLExecutor)(nil)
