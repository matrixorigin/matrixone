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
	"math"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"go.uber.org/zap"
)

var _ SQLExecutor = (*InternalSQLExecutor)(nil)

// UpstreamSQLHelper interface for handling special SQL statements
// (CREATE/DROP SNAPSHOT, OBJECTLIST, GET OBJECT) without requiring Session
type UpstreamSQLHelper interface {
	// HandleSpecialSQL checks if the SQL is a special statement and handles it directly
	// Returns (handled, result, error) where handled indicates if the statement was handled
	HandleSpecialSQL(ctx context.Context, query string) (bool, *Result, error)
}

// UpstreamSQLHelperFactory is a function type that creates an UpstreamSQLHelper
// Parameters: txnOp, engine, accountID, executor, txnClient (optional)
type UpstreamSQLHelperFactory func(
	txnOp client.TxnOperator,
	engine engine.Engine,
	accountID uint32,
	executor executor.SQLExecutor,
	txnClient client.TxnClient, // Optional: if nil, helper will try to get from engine
) UpstreamSQLHelper

// InternalSQLExecutor implements SQLExecutor interface using MatrixOne's internal SQL executor
// It supports retry for RC mode transaction errors (txn need retry errors)
// If upstreamSQLHelper is provided, special statements (CREATE/DROP SNAPSHOT, OBJECTLIST, GET OBJECT)
// will be routed through the helper
type InternalSQLExecutor struct {
	cnUUID            string
	internalExec      executor.SQLExecutor
	txnOp             client.TxnOperator
	txnClient         client.TxnClient
	engine            engine.Engine
	accountID         uint32            // Account ID for tenant context
	upstreamSQLHelper UpstreamSQLHelper // Optional helper for special SQL statements
	maxRetries        int               // Maximum number of retries for retryable errors
	retryInterval     time.Duration     // Interval between retries
	classifier        ErrorClassifier   // Error classifier for retry logic
}

// SetUpstreamSQLHelper sets the upstream SQL helper
func (e *InternalSQLExecutor) SetUpstreamSQLHelper(helper UpstreamSQLHelper) {
	e.upstreamSQLHelper = helper
}

// GetInternalExec returns the internal executor (for creating helper)
func (e *InternalSQLExecutor) GetInternalExec() executor.SQLExecutor {
	return e.internalExec
}

// GetTxnClient returns the txnClient (for creating helper)
func (e *InternalSQLExecutor) GetTxnClient() client.TxnClient {
	return e.txnClient
}

// NewInternalSQLExecutor creates a new InternalSQLExecutor
// txnClient is optional - if provided, transactions can be created via SetTxn
// engine is required for registering transactions with the engine
// accountID is the tenant account ID to use when executing SQL
// classifier is the error classifier to use for retry logic
// upstreamSQLHelper is optional - if provided, special SQL statements will be handled by it
func NewInternalSQLExecutor(
	cnUUID string,
	txnClient client.TxnClient,
	engine engine.Engine,
	accountID uint32,
	classifier ErrorClassifier,
) (*InternalSQLExecutor, error) {
	v, ok := moruntime.ServiceRuntime(cnUUID).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf("internal SQL executor not found for CN %s", cnUUID))
	}

	internalExec, ok := v.(executor.SQLExecutor)
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("invalid internal SQL executor type")
	}

	return &InternalSQLExecutor{
		cnUUID:        cnUUID,
		internalExec:  internalExec,
		txnClient:     txnClient,
		engine:        engine,
		accountID:     accountID,
		maxRetries:    5,                     // Default max retries
		retryInterval: 10 * time.Millisecond, // Default retry interval
		classifier:    classifier,
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

// SetTxn sets an external transaction operator for the executor
// This allows the executor to use a transaction created outside of it
func (e *InternalSQLExecutor) SetTxn(txnOp client.TxnOperator) {
	e.txnOp = txnOp
}

// EndTxn ends the current transaction
// It commits or rolls back the transaction set via SetTxn
func (e *InternalSQLExecutor) EndTxn(ctx context.Context, commit bool) error {
	if e.txnOp == nil {
		return nil // Idempotent
	}

	ctx, cancel := context.WithTimeoutCause(ctx, time.Hour, moerr.NewInternalErrorNoCtx("internal sql timeout"))
	defer cancel()

	var err error
	if commit {
		err = e.txnOp.Commit(ctx)
	} else {
		err = e.txnOp.Rollback(ctx)
	}
	e.txnOp = nil // Always clear, even on error
	return err
}

// ExecSQL executes a SQL statement with options
// useTxn: if true, execute within a transaction (requires active transaction, will error if txnOp is nil)
// if false, execute as autocommit (will create and commit transaction automatically)
// It supports automatic retry for RC mode transaction errors (txn need retry errors)
// If session is set and the statement is CREATE/DROP SNAPSHOT, OBJECTLIST, or GET OBJECT,
// it will be routed through frontend layer
func (e *InternalSQLExecutor) ExecSQL(
	ctx context.Context,
	ar *ActiveRoutine,
	query string,
	useTxn bool,
	needRetry bool,
) (*Result, error) {
	// If useTxn is true, check if transaction is available
	if useTxn && e.txnOp == nil {
		return nil, moerr.NewInternalError(ctx, "transaction required but no active transaction found. Call SetTxn() first")
	}
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

	// Create context with account ID if specified
	execCtx := ctx
	if e.accountID > 0 {
		execCtx = context.WithValue(ctx, defines.TenantIDKey{}, e.accountID)
	}

	// Handle snapshot clause in query before processing
	// Extract snapshot, update transaction, and remove snapshot clause from query
	query, txnOp, hasSnapshot, err := e.handleSnapshotInQuery(execCtx, query)
	if err != nil {
		return nil, err
	}
	if hasSnapshot && useTxn {
		return nil, moerr.NewInternalError(ctx, "snapshot clause is not supported in transaction")
	}

	// Check if upstreamSQLHelper can handle this SQL
	if e.upstreamSQLHelper != nil {
		handled, result, err := e.upstreamSQLHelper.HandleSpecialSQL(execCtx, query)
		if err != nil {
			return nil, err
		}
		if handled {
			return result, nil
		}
	}
	// For other statements, use internal executor with retry logic
	opts := executor.Options{}.
		WithDisableIncrStatement()

	if hasSnapshot {
		opts = opts.WithTxn(txnOp)
		defer txnOp.Commit(ctx)
	}
	// Only use transaction if useTxn is true and txnOp is available
	if useTxn && e.txnOp != nil {
		opts = opts.WithTxn(e.txnOp)
	}

	// Retry loop for retryable errors
	var execResult executor.Result
	var lastErr error
	retryCount := 0

	for retryCount <= e.maxRetries {
		execResult, err = e.internalExec.Exec(execCtx, query, opts)
		if err == nil {
			// Success, return result
			if retryCount > 0 {
				logutil.Info("internal sql executor retry succeeded",
					zap.String("query", truncateSQL(query)),
					zap.Int("retryCount", retryCount),
				)
			}
			return convertExecutorResult(execResult), nil
		}

		// Check if error is retryable using the provided classifier
		if e.classifier == nil || !e.classifier.IsRetryable(err) {
			// Not retryable, return error immediately
			return nil, err
		}

		lastErr = err
		retryCount++

		// Log retry attempt
		defChanged := moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged)
		logutil.Warn("internal sql executor retry attempt",
			zap.String("query", truncateSQL(query)),
			zap.Int("retryCount", retryCount),
			zap.Int("maxRetries", e.maxRetries),
			zap.Bool("defChanged", defChanged),
			zap.Error(err),
		)

		// If defChanged, wait a bit longer for DDL to complete
		if defChanged && retryCount == 1 {
			// First retry after defChanged, wait a bit longer
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(e.retryInterval * 2):
			}
		} else {
			// Regular retry interval
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(e.retryInterval):
			}
		}

		// Check for cancellation before retry
		if ar != nil {
			select {
			case <-ar.Pause:
				return nil, moerr.NewInternalError(ctx, "task paused")
			case <-ar.Cancel:
				return nil, moerr.NewInternalError(ctx, "task cancelled")
			default:
			}
		}
	}

	// Max retries exceeded
	logutil.Error("internal sql executor max retries exceeded",
		zap.String("query", truncateSQL(query)),
		zap.Int("retryCount", retryCount),
		zap.Error(lastErr),
	)
	return nil, lastErr
}

// truncateSQL truncates SQL string for logging
func truncateSQL(sql string) string {
	const maxLen = 200
	if len(sql) <= maxLen {
		return sql
	}
	return sql[:maxLen] + "..."
}

// convertExecutorResult converts executor.Result (with Batches) to publication.Result
func convertExecutorResult(execResult executor.Result) *Result {
	return &Result{
		internalResult: &InternalResult{
			executorResult: execResult,
		},
	}
}

// NewResultFromExecutorResult creates a new Result from executor.Result
// This is exported so that external packages (like test helpers) can create Result objects
func NewResultFromExecutorResult(execResult executor.Result) *Result {
	return convertExecutorResult(execResult)
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

	if len(r.executorResult.Batches) == 0 {
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
	if len(r.executorResult.Batches) == 0 {
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
			case *[]byte:
				*d = nil
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
			case *bool:
				*d = false
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
			case *types.TS:
				*d = types.TS{}
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
	case types.T_varchar, types.T_char, types.T_text, types.T_blob, types.T_binary, types.T_varbinary, types.T_datalink:
		if d, ok := dest.(*[]byte); ok {
			// For byte slice, get bytes directly and make a copy
			bytesVal := vec.GetBytesAt(int(idx))
			*d = make([]byte, len(bytesVal))
			copy(*d, bytesVal)
		} else if d, ok := dest.(*string); ok {
			val := vec.GetStringAt(int(idx))
			*d = val
		} else if d, ok := dest.(*sql.NullString); ok {
			val := vec.GetStringAt(int(idx))
			d.String = val
			d.Valid = true
		} else {
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf("destination type mismatch for string, type %T", dest))
		}

	case types.T_bool:
		val := vector.GetFixedAtWithTypeCheck[bool](vec, int(idx))
		if d, ok := dest.(*bool); ok {
			*d = val
		} else if d, ok := dest.(*sql.NullBool); ok {
			d.Bool = val
			d.Valid = true
		} else {
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf("destination type mismatch for bool, type %T", dest))
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
		} else if d, ok := dest.(*sql.NullInt64); ok {
			// Support sql.NullInt64 for uint32 values (e.g., account_id)
			// This is safe as uint32 max (4294967295) is well within int64 range
			d.Int64 = int64(val)
			d.Valid = true
		} else {
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf("destination type mismatch for uint32, type %T", dest))
		}

	case types.T_uint64:
		val := vector.GetFixedAtWithTypeCheck[uint64](vec, int(idx))
		if d, ok := dest.(*uint64); ok {
			*d = val
		} else if d, ok := dest.(*sql.NullInt64); ok {
			// Support sql.NullInt64 for uint64 values (e.g., dat_id)
			// Note: This may overflow if uint64 value exceeds int64 max, but it's acceptable for most use cases
			// where database IDs and account IDs are typically within int64 range
			if val <= uint64(math.MaxInt64) {
				d.Int64 = int64(val)
				d.Valid = true
			} else {
				// If value exceeds int64 max, we can't represent it in NullInt64
				// This should be rare in practice for database/account IDs
				return moerr.NewInternalErrorNoCtx(fmt.Sprintf("uint64 value %d exceeds int64 max, cannot convert to sql.NullInt64", val))
			}
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

	case types.T_TS:
		val := vector.GetFixedAtWithTypeCheck[types.TS](vec, int(idx))
		if d, ok := dest.(*types.TS); ok {
			*d = val
		} else {
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf("destination type mismatch for TS, type %T", dest))
		}

	case types.T_json:
		bytesVal := vec.GetBytesAt(int(idx))
		byteJson := types.DecodeJson(bytesVal)
		if d, ok := dest.(*string); ok {
			*d = byteJson.String()
		} else if d, ok := dest.(*sql.NullString); ok {
			d.String = byteJson.String()
			d.Valid = true
		} else if d, ok := dest.(*[]byte); ok {
			// For byte slice, get the JSON bytes and make a copy
			*d = make([]byte, len(bytesVal))
			copy(*d, bytesVal)
		} else {
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf("destination type mismatch for JSON, type %T", dest))
		}

	default:
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf("unsupported vector type: %v, type %T", vec.GetType().Oid, dest))
	}

	return nil
}

// Err returns any error encountered during iteration
func (r *InternalResult) Err() error {
	return r.err
}

// handleSnapshotInQuery handles snapshot clause in SQL query
// If the query contains {SNAPSHOT = 'snapshot_name'}, it extracts the snapshot name,
// queries the snapshot timestamp, updates the transaction snapshot, and removes the snapshot clause from query
// Returns the modified query (with snapshot clause removed), the transaction operator, whether snapshot was found, and error
func (e *InternalSQLExecutor) handleSnapshotInQuery(ctx context.Context, query string) (string, client.TxnOperator, bool, error) {
	// Check if query contains snapshot clause
	snapshotName, found := extractSnapshotFromSQL(query)
	if !found || snapshotName == "" {
		return query, e.txnOp, false, nil // No snapshot clause, return original query and current txn
	}

	// Create a separate transaction for querying snapshot if no transaction exists
	var txnOp client.TxnOperator
	var createdTxn bool
	if e.txnOp == nil {
		// Need to create a new transaction
		if e.engine == nil {
			return "", nil, true, moerr.NewInternalError(ctx, "engine is required to create transaction for snapshot query")
		}
		if e.txnClient == nil {
			return "", nil, true, moerr.NewInternalError(ctx, "txnClient is required to create transaction for snapshot query")
		}

		// Get latest logtail applied time as snapshot timestamp
		snapshotTS := e.engine.LatestLogtailAppliedTime()

		// Create new txn operator
		var err error
		txnOp, err = e.txnClient.New(ctx, snapshotTS)
		if err != nil {
			return "", nil, true, moerr.NewInternalErrorf(ctx, "failed to create transaction for snapshot query: %v", err)
		}

		// Initialize engine with the new txn
		if err := e.engine.New(ctx, txnOp); err != nil {
			txnOp.Rollback(ctx)
			return "", nil, true, moerr.NewInternalErrorf(ctx, "failed to initialize engine with transaction: %v", err)
		}

		createdTxn = true
	} else {
		txnOp = e.txnOp
	}

	// Query snapshot timestamp using internal executor
	querySnapshotTsSQL := fmt.Sprintf(`SELECT ts FROM mo_catalog.mo_snapshots WHERE sname = '%s' ORDER BY snapshot_id LIMIT 1`, escapeSQLStringForSnapshot(snapshotName))
	opts := executor.Options{}.WithDisableIncrStatement().WithTxn(txnOp)
	result, err := e.internalExec.Exec(ctx, querySnapshotTsSQL, opts)
	if err != nil {
		if createdTxn {
			txnOp.Rollback(ctx)
		}
		return "", nil, true, moerr.NewInternalErrorf(ctx, "failed to query snapshot %s timestamp: %v", snapshotName, err)
	}
	defer result.Close()

	var snapshotTS int64
	var foundTS bool
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows > 0 && len(cols) > 0 {
			snapshotTS = vector.GetFixedAtWithTypeCheck[int64](cols[0], 0)
			foundTS = true
		}
		return true
	})

	if !foundTS {
		if createdTxn {
			txnOp.Rollback(ctx)
		}
		return "", nil, true, moerr.NewInternalErrorf(ctx, "snapshot %s not found", snapshotName)
	}

	// Update transaction snapshot
	ts := types.BuildTS(snapshotTS, 0)
	err = txnOp.UpdateSnapshot(ctx, ts.ToTimestamp())
	if err != nil {
		if createdTxn {
			txnOp.Rollback(ctx)
		}
		return "", nil, true, moerr.NewInternalErrorf(ctx, "failed to update transaction snapshot: %v", err)
	}

	logutil.Info("InternalSQLExecutor: updated transaction snapshot from query",
		zap.String("snapshot_name", snapshotName),
		zap.Int64("snapshot_ts", snapshotTS),
		zap.Bool("created_new_txn", createdTxn),
	)

	// Remove snapshot clause from query
	modifiedQuery := removeSnapshotFromSQL(query)
	return modifiedQuery, txnOp, true, nil
}

// extractSnapshotFromSQL extracts snapshot name from SQL query
// Looks for pattern: {SNAPSHOT = 'snapshot_name'} or {SNAPSHOT = "snapshot_name"}
// Returns (snapshotName, found)
func extractSnapshotFromSQL(query string) (string, bool) {
	// Convert to uppercase for case-insensitive matching
	upperQuery := strings.ToUpper(query)

	// Look for {SNAPSHOT = pattern
	startIdx := strings.Index(upperQuery, "{SNAPSHOT =")
	if startIdx == -1 {
		return "", false
	}

	// Find the opening brace (should be at startIdx)
	braceStart := startIdx
	if query[braceStart] != '{' {
		// Try to find the actual brace
		braceStart = strings.LastIndex(query[:startIdx+1], "{")
		if braceStart == -1 {
			return "", false
		}
	}

	// Find the closing brace
	braceEnd := strings.Index(query[braceStart:], "}")
	if braceEnd == -1 {
		return "", false
	}

	// Extract the snapshot clause
	clause := query[braceStart : braceStart+braceEnd+1]

	// Extract snapshot name
	// Pattern: {SNAPSHOT = 'name'} or {SNAPSHOT = "name"}
	equalIdx := strings.Index(clause, "=")
	if equalIdx == -1 {
		return "", false
	}

	// Get the value part after =
	valuePart := strings.TrimSpace(clause[equalIdx+1:])
	valuePart = strings.TrimSuffix(valuePart, "}")
	valuePart = strings.TrimSpace(valuePart)

	// Remove quotes (single or double)
	if len(valuePart) >= 2 {
		if (valuePart[0] == '\'' && valuePart[len(valuePart)-1] == '\'') ||
			(valuePart[0] == '"' && valuePart[len(valuePart)-1] == '"') {
			valuePart = valuePart[1 : len(valuePart)-1]
		}
	}

	if valuePart == "" {
		return "", false
	}

	return valuePart, true
}

// removeSnapshotFromSQL removes snapshot clause from SQL query
// Removes pattern: {SNAPSHOT = 'snapshot_name'} or {SNAPSHOT = "snapshot_name"}
func removeSnapshotFromSQL(query string) string {
	// Convert to uppercase for case-insensitive matching
	upperQuery := strings.ToUpper(query)

	// Look for {SNAPSHOT = pattern
	startIdx := strings.Index(upperQuery, "{SNAPSHOT =")
	if startIdx == -1 {
		return query // No snapshot clause found
	}

	// Find the opening brace
	braceStart := startIdx
	if query[braceStart] != '{' {
		braceStart = strings.LastIndex(query[:startIdx+1], "{")
		if braceStart == -1 {
			return query
		}
	}

	// Find the closing brace
	braceEnd := strings.Index(query[braceStart:], "}")
	if braceEnd == -1 {
		return query // Malformed, return original
	}

	// Remove the snapshot clause
	before := query[:braceStart]
	after := query[braceStart+braceEnd+1:]

	// Trim trailing spaces from before and leading spaces from after
	beforeTrimmed := strings.TrimRight(before, " \t\n\r")
	afterTrimmed := strings.TrimLeft(after, " \t\n\r")

	// Check if we need to add a space between before and after
	// If both before and after end/start with non-whitespace characters, add a space
	if len(beforeTrimmed) > 0 && len(afterTrimmed) > 0 {
		lastChar := beforeTrimmed[len(beforeTrimmed)-1]
		firstChar := afterTrimmed[0]
		// If both are non-whitespace characters, we need a space
		if lastChar != ' ' && firstChar != ' ' {
			return beforeTrimmed + " " + afterTrimmed
		}
	}

	return beforeTrimmed + afterTrimmed
}

// escapeSQLStringForSnapshot escapes SQL string for use in snapshot name queries
func escapeSQLStringForSnapshot(s string) string {
	// Replace backslash first (before replacing quotes) to avoid double-escaping
	s = strings.ReplaceAll(s, `\`, `\\`)
	// Replace single quotes with double single quotes (SQL standard escaping)
	s = strings.ReplaceAll(s, "'", "''")
	return s
}

// Ensure InternalSQLExecutor implements SQLExecutor interface
var _ SQLExecutor = (*InternalSQLExecutor)(nil)
