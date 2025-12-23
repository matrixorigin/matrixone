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
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"go.uber.org/zap"
)

// IterationState represents the state of an iteration
const (
	IterationStatePending   int8 = 0 // 'pending'
	IterationStateRunning   int8 = 1 // 'running'
	IterationStateCompleted int8 = 2 // 'complete'
	IterationStateError     int8 = 3 // 'error'
	IterationStateCanceled  int8 = 4 // 'cancel'
)

const (
	InternalSQLExecutorType = "internal_sql_executor"
)

// ObjectWithTableInfo contains ObjectStats with its table and database information
type ObjectWithTableInfo struct {
	Stats       objectio.ObjectStats
	DBName      string
	TableName   string
	IsTombstone bool
	Delete      bool
}

// AObjMappingJSON represents the serializable part of AObjMapping
type AObjMappingJSON struct {
	Current  string `json:"current"`  // ObjectStats as base64-encoded string
	Previous string `json:"previous"` // ObjectStats as base64-encoded string
}

// IterationContextJSON represents the serializable part of IterationContext
// This structure is used to store iteration context in mo_ccpr_log.context field
type IterationContextJSON struct {
	// Task identification
	TaskID           uint64  `json:"task_id"`
	SubscriptionName string  `json:"subscription_name"`
	SrcInfo          SrcInfo `json:"src_info"`

	// Context information
	PrevSnapshotName    string                     `json:"prev_snapshot_name"`
	PrevSnapshotTS      int64                      `json:"prev_snapshot_ts"` // types.TS as int64
	CurrentSnapshotName string                     `json:"current_snapshot_name"`
	CurrentSnapshotTS   int64                      `json:"current_snapshot_ts"` // types.TS as int64
	ActiveAObj          map[string]AObjMappingJSON `json:"active_aobj"`         // ActiveAObj as serializable map (key is ObjectId as string)
	TableIDs            map[string]uint64          `json:"table_ids"`
}

// InitializeIterationContext initializes IterationContext from mo_ccpr_log table
// It reads subscription_name, srcinfo, upstream_conn, and context from the table,
// creates local executor and upstream executor, creates a local transaction,
// and sets up the local executor to use that transaction.
// iterationLSN is passed in as a parameter.
// ActiveAObj and TableIDs are read from the context JSON field.
func InitializeIterationContext(
	ctx context.Context,
	cnUUID string,
	cnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	taskID uint64,
	iterationLSN uint64,
	upstreamSQLHelperFactory UpstreamSQLHelperFactory,
) (*IterationContext, error) {
	if cnTxnClient == nil {
		return nil, moerr.NewInternalError(ctx, "txn client is nil")
	}

	// Create local executor first (without transaction) to query mo_ccpr_log
	// mo_ccpr_log is a system table, so we must use system account
	// Local executor doesn't need upstream SQL helper (no special SQL statements)
	localExecutorInternal, err := NewInternalSQLExecutor(cnUUID, nil, nil, catalog.System_Account)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to create local executor: %v", err)
	}
	var localExecutor SQLExecutor = localExecutorInternal

	// Query mo_ccpr_log table to get subscription_name, sync_level, db_name, table_name, upstream_conn, context
	// mo_ccpr_log is a system table, so we must use system account context
	systemCtx := context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	querySQL := PublicationSQLBuilder.QueryMoCcprLogFullSQL(taskID)
	result, err := localExecutor.ExecSQL(systemCtx, querySQL)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to query mo_ccpr_log: %v", err)
	}
	defer result.Close()

	// Scan the result
	var subscriptionName sql.NullString
	var syncLevel sql.NullString
	var accountID sql.NullInt64
	var dbName sql.NullString
	var tableName sql.NullString
	var upstreamConn sql.NullString
	var contextJSON sql.NullString

	if !result.Next() {
		if err := result.Err(); err != nil {
			return nil, moerr.NewInternalErrorf(ctx, "failed to read query result: %v", err)
		}
		return nil, moerr.NewInternalErrorf(ctx, "no rows returned for task_id %d", taskID)
	}

	if err := result.Scan(&subscriptionName, &syncLevel, &accountID, &dbName, &tableName, &upstreamConn, &contextJSON); err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to scan query result: %v", err)
	}

	// Validate required fields
	if !subscriptionName.Valid {
		return nil, moerr.NewInternalErrorf(ctx, "subscription_name is null for task_id %d", taskID)
	}
	if !syncLevel.Valid {
		return nil, moerr.NewInternalErrorf(ctx, "sync_level is null for task_id %d", taskID)
	}
	if !accountID.Valid {
		return nil, moerr.NewInternalErrorf(ctx, "account_id is null for task_id %d", taskID)
	}

	// Build SrcInfo from sync_level, account_id, db_name, table_name
	srcInfo := SrcInfo{
		SyncLevel: syncLevel.String,
		AccountID: uint32(accountID.Int64),
	}
	if dbName.Valid {
		srcInfo.DBName = dbName.String
	}
	if tableName.Valid {
		srcInfo.TableName = tableName.String
	}

	// Create upstream executor from upstream_conn
	var upstreamExecutor SQLExecutor
	var upstreamAccountID uint32
	if !upstreamConn.Valid || upstreamConn.String == "" {
		return nil, moerr.NewInternalErrorf(ctx, "upstream_conn is null or empty for task_id %d", taskID)
	}

	// Parse upstream_conn to check if it's internal_sql_executor with account ID
	// Format: internal_sql_executor or internal_sql_executor:<account_id>
	if strings.HasPrefix(upstreamConn.String, InternalSQLExecutorType) {
		// Check if account ID is specified after colon
		parts := strings.Split(upstreamConn.String, ":")
		if len(parts) == 2 {
			// Parse account ID from upstream_conn
			var accountID uint32
			_, err := fmt.Sscanf(parts[1], "%d", &accountID)
			if err != nil {
				return nil, moerr.NewInternalErrorf(ctx, "failed to parse account ID from upstream_conn %s: %v", upstreamConn.String, err)
			}
			upstreamAccountID = accountID
		} else if len(parts) == 1 {
			// No account ID specified, use account ID from context as fallback
			if v := ctx.Value(defines.TenantIDKey{}); v != nil {
				if accountID, ok := v.(uint32); ok {
					upstreamAccountID = accountID
				}
			}
		} else {
			return nil, moerr.NewInternalErrorf(ctx, "invalid upstream_conn format: %s, expected internal_sql_executor or internal_sql_executor:<account_id>", upstreamConn.String)
		}

		// Create upstream executor with account ID
		upstreamExecutorInternal, err := NewInternalSQLExecutor(cnUUID, cnTxnClient, cnEngine, upstreamAccountID)
		if err != nil {
			return nil, moerr.NewInternalErrorf(ctx, "failed to create upstream executor: %v", err)
		}
		upstreamExecutor = upstreamExecutorInternal
		// Create upstream SQL helper if factory is provided and upstream executor is InternalSQLExecutor
		if upstreamSQLHelperFactory != nil {
			if upstreamExecutorInternal, ok := upstreamExecutor.(*InternalSQLExecutor); ok {
				// Create helper with nil txnOp - it will be updated when StartTxn is called
				helper := upstreamSQLHelperFactory(
					nil, // txnOp will be set when StartTxn is called
					cnEngine,
					upstreamAccountID,
					upstreamExecutorInternal.GetInternalExec(),
				)
				upstreamExecutorInternal.SetUpstreamSQLHelper(helper)
			}
		}
		// Helper will be created after local transaction is created (helper needs txnOp)
	} else {
		connConfig, err := ParseUpstreamConn(upstreamConn.String)
		if err != nil {
			return nil, moerr.NewInternalErrorf(ctx, "failed to parse upstream connection string: %v", err)
		}
		upstreamExecutor, err = NewUpstreamExecutor(
			connConfig.User,
			connConfig.Password,
			connConfig.Host,
			connConfig.Port,
			-1, // retryTimes: -1 for infinite retry
			0,  // retryDuration: 0 for no limit
			connConfig.Timeout,
		)
		if err != nil {
			return nil, moerr.NewInternalErrorf(ctx, "failed to create upstream executor: %v", err)
		}

	}
	err = upstreamExecutor.StartTxn(ctx)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to start upstream transaction: %v", err)
	}

	// Create local transaction
	nowTs := cnEngine.LatestLogtailAppliedTime()
	createByOpt := client.WithTxnCreateBy(
		0,
		"",
		"publication iteration",
		0)
	localTxn, err := cnTxnClient.New(ctx, nowTs, createByOpt)
	if err != nil {
		upstreamExecutor.Close()
		return nil, moerr.NewInternalErrorf(ctx, "failed to create local transaction: %v", err)
	}

	// Register the transaction with the engine
	err = cnEngine.New(ctx, localTxn)
	if err != nil {
		upstreamExecutor.Close()
		return nil, moerr.NewInternalErrorf(ctx, "failed to register transaction with engine: %v", err)
	}

	// Set the transaction in local executor
	localExecutorInternal.SetTxn(localTxn)

	// Initialize IterationContext
	iterationCtx := &IterationContext{
		TaskID:           taskID,
		SubscriptionName: subscriptionName.String,
		SrcInfo:          srcInfo,
		LocalTxn:         localTxn,
		LocalExecutor:    localExecutor,
		UpstreamExecutor: upstreamExecutor,
		IterationLSN:     iterationLSN,
		ActiveAObj:       make(map[objectio.ObjectId]AObjMapping),
		TableIDs:         make(map[TableKey]uint64),
	}

	// Parse context JSON if available
	if contextJSON.Valid && contextJSON.String != "" && contextJSON.String != "null" {
		var ctxJSON IterationContextJSON
		if err := json.Unmarshal([]byte(contextJSON.String), &ctxJSON); err != nil {
			return nil, moerr.NewInternalErrorf(ctx, "failed to unmarshal context JSON: %v", err)
		}

		// Restore ActiveAObj from JSON
		if ctxJSON.ActiveAObj != nil {
			for uuidStr, mappingJSON := range ctxJSON.ActiveAObj {
				// Parse ObjectId from string
				uuid, err := parseObjectIdFromString(uuidStr)
				if err != nil {
					return nil, moerr.NewInternalErrorf(ctx, "failed to parse object id from string %s: %v", uuidStr, err)
				}
				mapping := AObjMapping{}
				// Deserialize Current ObjectStats from base64
				if mappingJSON.Current != "" {
					currentBytes, err := base64.StdEncoding.DecodeString(mappingJSON.Current)
					if err != nil {
						return nil, moerr.NewInternalErrorf(ctx, "failed to decode current object stats for uuid %s: %v", uuidStr, err)
					}
					if len(currentBytes) == objectio.ObjectStatsLen {
						mapping.Current.UnMarshal(currentBytes)
					}
				}
				// Deserialize Previous ObjectStats from base64
				if mappingJSON.Previous != "" {
					previousBytes, err := base64.StdEncoding.DecodeString(mappingJSON.Previous)
					if err != nil {
						return nil, moerr.NewInternalErrorf(ctx, "failed to decode previous object stats for uuid %s: %v", uuidStr, err)
					}
					if len(previousBytes) == objectio.ObjectStatsLen {
						mapping.Previous.UnMarshal(previousBytes)
					}
				}
				// Delete flag is not persisted, it's only used in current iteration
				iterationCtx.ActiveAObj[uuid] = mapping
			}
		}

		// Restore TableIDs from JSON
		if ctxJSON.TableIDs != nil {
			iterationCtx.TableIDs = make(map[TableKey]uint64)
			for keyStr, id := range ctxJSON.TableIDs {
				// Parse key string format: "dbname.tablename"
				key := parseTableKeyFromString(keyStr)
				// Only store valid table keys (skip empty keys from legacy database format)
				if key.DBName != "" && key.TableName != "" {
					iterationCtx.TableIDs[key] = id
				}
			}
		}

		// Restore snapshot information if available
		if ctxJSON.PrevSnapshotName != "" {
			iterationCtx.PrevSnapshotName = ctxJSON.PrevSnapshotName
		}
		if ctxJSON.PrevSnapshotTS > 0 {
			iterationCtx.PrevSnapshotTS = types.BuildTS(ctxJSON.PrevSnapshotTS, 0)
		}
		if ctxJSON.CurrentSnapshotName != "" {
			iterationCtx.CurrentSnapshotName = ctxJSON.CurrentSnapshotName
		}
		if ctxJSON.CurrentSnapshotTS > 0 {
			iterationCtx.CurrentSnapshotTS = types.BuildTS(ctxJSON.CurrentSnapshotTS, 0)
		}
	}

	return iterationCtx, nil
}

func (iterCtx *IterationContext) Close(commit bool) error {
	// Create context with PkCheckByTN set to SkipAllDedup for publication iteration
	// This ensures that all deduplication checks are skipped when committing the transaction
	ctx := context.Background()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, iterCtx.SrcInfo.AccountID)
	ctx = context.WithValue(ctx, defines.PkCheckByTN{}, int8(cmd_util.SkipAllDedup))

	if iterCtx.LocalExecutor != nil {
		iterCtx.LocalExecutor.EndTxn(ctx, commit)
		iterCtx.LocalExecutor.Close()
	}
	if iterCtx.UpstreamExecutor != nil {
		iterCtx.UpstreamExecutor.EndTxn(ctx, commit)
		iterCtx.UpstreamExecutor.Close()
	}
	return nil
}

// UpdateIterationState updates iteration state, iteration LSN, iteration context, and error message in mo_ccpr_log table
// It serializes the relevant parts of IterationContext to JSON and updates the corresponding fields
func UpdateIterationState(
	ctx context.Context,
	executor SQLExecutor,
	taskID uint64,
	iterationState int8,
	iterationLSN uint64,
	iterationCtx *IterationContext,
	errorMessage string,
) error {
	if executor == nil {
		return moerr.NewInternalError(ctx, "executor is nil")
	}

	// Serialize IterationContext to JSON
	var contextJSON string
	if iterationCtx != nil {
		// Convert ActiveAObj to serializable format
		activeAObjJSON := make(map[string]AObjMappingJSON)
		if iterationCtx.ActiveAObj != nil {
			for uuid, mapping := range iterationCtx.ActiveAObj {
				mappingJSON := AObjMappingJSON{}
				// Serialize Current ObjectStats to base64
				if !mapping.Current.IsZero() {
					currentBytes := mapping.Current.Marshal()
					mappingJSON.Current = base64.StdEncoding.EncodeToString(currentBytes)
				}
				// Serialize Previous ObjectStats to base64
				if !mapping.Previous.IsZero() {
					previousBytes := mapping.Previous.Marshal()
					mappingJSON.Previous = base64.StdEncoding.EncodeToString(previousBytes)
				}
				// Delete flag is not persisted, it's only used in current iteration
				// Convert ObjectId to string for JSON serialization
				activeAObjJSON[uuid.String()] = mappingJSON
			}
		}

		// Convert TableIDs to string map for JSON serialization
		tableIDsJSON := make(map[string]uint64)
		for key, id := range iterationCtx.TableIDs {
			keyStr := tableKeyToString(key)
			tableIDsJSON[keyStr] = id
		}

		// Create a serializable context structure
		ctxJSON := IterationContextJSON{
			TaskID:              iterationCtx.TaskID,
			SubscriptionName:    iterationCtx.SubscriptionName,
			SrcInfo:             iterationCtx.SrcInfo,
			PrevSnapshotName:    iterationCtx.PrevSnapshotName,
			PrevSnapshotTS:      iterationCtx.PrevSnapshotTS.Physical(),
			CurrentSnapshotName: iterationCtx.CurrentSnapshotName,
			CurrentSnapshotTS:   iterationCtx.CurrentSnapshotTS.Physical(),
			ActiveAObj:          activeAObjJSON,
			TableIDs:            tableIDsJSON,
		}

		contextBytes, err := json.Marshal(ctxJSON)
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to marshal iteration context: %v", err)
		}
		contextJSON = string(contextBytes)
	} else {
		contextJSON = "null"
	}

	// Build update SQL
	updateSQL := PublicationSQLBuilder.UpdateMoCcprLogSQL(
		taskID,
		iterationState,
		iterationLSN,
		contextJSON,
		errorMessage,
	)

	// Execute update SQL using system account context
	// mo_ccpr_log is a system table, so we must use system account
	systemCtx := context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	result, err := executor.ExecSQL(systemCtx, updateSQL)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to execute update SQL: %v", err)
	}
	defer result.Close()

	return nil
}

// CheckIterationStatus checks the iteration status in mo_ccpr_log table
// It verifies that cn_uuid, iteration_lsn match the expected values,
// and that iteration_state is completed
func CheckIterationStatus(
	ctx context.Context,
	executor SQLExecutor,
	taskID uint64,
	expectedCNUUID string,
	expectedIterationLSN uint64,
) error {
	// Build SQL query using sql_builder
	querySQL := PublicationSQLBuilder.QueryMoCcprLogSQL(taskID)

	// Execute SQL query using system account context
	// mo_ccpr_log is a system table, so we must use system account
	systemCtx := context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	result, err := executor.ExecSQL(systemCtx, querySQL)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to execute query: %v", err)
	}
	defer result.Close()

	// Scan the result - expecting columns: cn_uuid, iteration_state, iteration_lsn
	var cnUUID sql.NullString
	var iterationState int8
	var iterationLSN uint64

	if !result.Next() {
		if err := result.Err(); err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to read query result: %v", err)
		}
		return moerr.NewInternalErrorf(ctx, "no rows returned for task_id %d", taskID)
	}

	if err := result.Scan(&cnUUID, &iterationState, &iterationLSN); err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to scan query result: %v", err)
	}

	// Check if there are more rows (should not happen for a single task_id)
	if result.Next() {
		return moerr.NewInternalErrorf(ctx, "multiple rows returned for task_id %d", taskID)
	}

	// Check if cn_uuid matches
	if !cnUUID.Valid {
		return moerr.NewInternalErrorf(ctx, "cn_uuid is null for task_id %d", taskID)
	}
	if cnUUID.String != expectedCNUUID {
		return moerr.NewInternalErrorf(ctx, "cn_uuid mismatch: expected %s, got %s", expectedCNUUID, cnUUID.String)
	}

	// Check if iteration_lsn matches
	if iterationLSN != expectedIterationLSN {
		return moerr.NewInternalErrorf(ctx, "iteration_lsn mismatch: expected %d, got %d", expectedIterationLSN, iterationLSN)
	}

	// Check if iteration_state is completed
	if iterationState != IterationStatePending {
		return moerr.NewInternalErrorf(ctx, "iteration_state is not pending: expected %d (pending), got %d", IterationStatePending, iterationState)
	}

	return nil
}

// GenerateSnapshotName generates a snapshot name using a rule-based encoding
// Format: ccpr_<taskID>_<iterationLSN>
func GenerateSnapshotName(taskID uint64, iterationLSN uint64) string {
	return fmt.Sprintf("ccpr_%d_%d", taskID, iterationLSN)
}

// RequestUpstreamSnapshot requests a snapshot from upstream cluster
// It creates a snapshot based on the srcinfo in IterationContext and stores the snapshot name in the context
func RequestUpstreamSnapshot(
	ctx context.Context,
	iterationCtx *IterationContext,
) error {
	if iterationCtx == nil {
		return moerr.NewInternalError(ctx, "iteration context is nil")
	}

	if iterationCtx.UpstreamExecutor == nil {
		return moerr.NewInternalError(ctx, "upstream executor is nil")
	}

	// Generate snapshot name using rule-based encoding
	snapshotName := GenerateSnapshotName(iterationCtx.TaskID, iterationCtx.IterationLSN)

	// Build SQL based on srcinfo
	var createSnapshotSQL string
	switch iterationCtx.SrcInfo.SyncLevel {
	case SyncLevelTable:
		if iterationCtx.SrcInfo.DBName == "" || iterationCtx.SrcInfo.TableName == "" {
			return moerr.NewInternalError(ctx, "db_name and table_name are required for table level snapshot")
		}
		createSnapshotSQL = PublicationSQLBuilder.CreateSnapshotForTableSQL(
			snapshotName,
			iterationCtx.SrcInfo.DBName,
			iterationCtx.SrcInfo.TableName,
		)
	case SyncLevelDatabase:
		if iterationCtx.SrcInfo.DBName == "" {
			return moerr.NewInternalError(ctx, "db_name is required for database level snapshot")
		}
		createSnapshotSQL = PublicationSQLBuilder.CreateSnapshotForDatabaseSQL(
			snapshotName,
			iterationCtx.SrcInfo.DBName,
		)
	case SyncLevelAccount:
		// Note: CreateSnapshotForAccountSQL currently uses account name, not account ID
		// This is for upstream snapshot creation, which may require account name
		// For now, we use empty string to create snapshot for current account
		createSnapshotSQL = PublicationSQLBuilder.CreateSnapshotForAccountSQL(
			snapshotName,
			"", // Empty account name means current account
		)
	default:
		return moerr.NewInternalErrorf(ctx, "unsupported sync_level: %s", iterationCtx.SrcInfo.SyncLevel)
	}

	// Execute SQL through upstream executor (account ID is handled internally)
	result, err := iterationCtx.UpstreamExecutor.ExecSQL(ctx, createSnapshotSQL)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to create snapshot: %v", err)
	}
	defer result.Close()

	// Before setting new current snapshot, save old current snapshot as prev snapshot
	if iterationCtx.CurrentSnapshotName != "" {
		iterationCtx.PrevSnapshotName = iterationCtx.CurrentSnapshotName
		iterationCtx.PrevSnapshotTS = iterationCtx.CurrentSnapshotTS
	}

	// Store snapshot name in iteration context
	iterationCtx.CurrentSnapshotName = snapshotName

	// Query snapshot TS from mo_snapshots table
	querySnapshotTsSQL := PublicationSQLBuilder.QuerySnapshotTsSQL(snapshotName)
	tsResult, err := iterationCtx.UpstreamExecutor.ExecSQL(ctx, querySnapshotTsSQL)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to query snapshot TS: %v", err)
	}
	defer tsResult.Close()

	// Scan the TS result
	var tsValue sql.NullInt64
	if !tsResult.Next() {
		if err := tsResult.Err(); err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to read snapshot TS result: %v", err)
		}
		return moerr.NewInternalErrorf(ctx, "no rows returned for snapshot %s", snapshotName)
	}

	if err := tsResult.Scan(&tsValue); err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to scan snapshot TS result: %v", err)
	}

	if !tsValue.Valid {
		return moerr.NewInternalErrorf(ctx, "snapshot TS is null for snapshot %s", snapshotName)
	}

	// Convert bigint TS to types.TS (logical time is set to 0)
	snapshotTS := types.BuildTS(tsValue.Int64, 0)
	iterationCtx.CurrentSnapshotTS = snapshotTS

	return nil
}

// WaitForSnapshotFlushed waits for the snapshot to be flushed with exponential backoff
// It checks if the snapshot is flushed, and if not, waits and retries
// firstInterval: initial wait time (default: 1s)
// totalTimeout: total time to wait before giving up (default: 30min)
func WaitForSnapshotFlushed(
	ctx context.Context,
	iterationCtx *IterationContext,
	firstInterval time.Duration,
	totalTimeout time.Duration,
) error {
	if iterationCtx == nil {
		return moerr.NewInternalError(ctx, "iteration context is nil")
	}

	if iterationCtx.UpstreamExecutor == nil {
		return moerr.NewInternalError(ctx, "upstream executor is nil")
	}

	if iterationCtx.CurrentSnapshotName == "" {
		return moerr.NewInternalError(ctx, "current snapshot name is empty")
	}

	// Set default values if not provided
	if firstInterval <= 0 {
		firstInterval = 1 * time.Second
	}
	if totalTimeout <= 0 {
		totalTimeout = 30 * time.Minute
	}

	snapshotName := iterationCtx.CurrentSnapshotName
	checkSQL := PublicationSQLBuilder.CheckSnapshotFlushedSQL(snapshotName)

	startTime := time.Now()
	currentInterval := firstInterval
	attempt := 0

	for {
		// Check if we've exceeded total timeout
		if time.Since(startTime) > totalTimeout {
			return moerr.NewInternalErrorf(
				ctx,
				"timeout waiting for snapshot %s to be flushed after %v",
				snapshotName,
				totalTimeout,
			)
		}

		// Check if context is cancelled
		select {
		case <-ctx.Done():
			return moerr.NewInternalErrorf(ctx, "context cancelled while waiting for snapshot %s to be flushed", snapshotName)
		default:
		}

		// Execute check snapshot flushed SQL
		result, err := iterationCtx.UpstreamExecutor.ExecSQL(ctx, checkSQL)
		if err != nil {
			logutil.Warn("ccpr-iteration check snapshot flushed failed",
				zap.String("snapshot_name", snapshotName),
				zap.Int("attempt", attempt),
				zap.Error(err),
			)
			// Continue to retry on error
		} else {
			// Read the result
			var flushed bool
			var found bool
			if result.Next() {
				var resultValue sql.NullBool
				if err := result.Scan(&resultValue); err == nil && resultValue.Valid {
					flushed = resultValue.Bool
					found = true
				}
			}
			result.Close()

			if found && flushed {
				logutil.Info("ccpr-iteration snapshot flushed",
					zap.String("snapshot_name", snapshotName),
					zap.Int("attempt", attempt),
					zap.Duration("elapsed", time.Since(startTime)),
				)
				return nil
			}
		}

		// Log retry attempt
		attempt++
		logutil.Info("ccpr-iteration waiting for snapshot to be flushed",
			zap.String("snapshot_name", snapshotName),
			zap.Int("attempt", attempt),
			zap.Duration("next_interval", currentInterval),
			zap.Duration("elapsed", time.Since(startTime)),
			zap.Duration("remaining", totalTimeout-time.Since(startTime)),
		)

		// Wait before next retry
		select {
		case <-ctx.Done():
			return moerr.NewInternalErrorf(ctx, "context cancelled while waiting for snapshot %s to be flushed", snapshotName)
		case <-time.After(currentInterval):
			// Double the interval for next retry (exponential backoff)
			currentInterval *= 2
		}
	}
}

// DropPreviousUpstreamSnapshot drops the previous snapshot from upstream cluster
// It deletes the snapshot specified by PrevSnapshotName in IterationContext
func DropPreviousUpstreamSnapshot(
	ctx context.Context,
	iterationCtx *IterationContext,
) error {
	if iterationCtx == nil {
		return moerr.NewInternalError(ctx, "iteration context is nil")
	}

	if iterationCtx.UpstreamExecutor == nil {
		return moerr.NewInternalError(ctx, "upstream executor is nil")
	}

	// Check if there's a previous snapshot to drop
	if iterationCtx.PrevSnapshotName == "" {
		// No previous snapshot to drop, silently return
		return nil
	}

	// Build drop snapshot SQL using IF EXISTS to avoid errors if snapshot already deleted
	dropSnapshotSQL := PublicationSQLBuilder.DropSnapshotIfExistsSQL(iterationCtx.PrevSnapshotName)

	// Execute SQL through upstream executor
	result, err := iterationCtx.UpstreamExecutor.ExecSQL(ctx, dropSnapshotSQL)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to drop previous snapshot %s: %v", iterationCtx.PrevSnapshotName, err)
	}
	defer result.Close()

	return nil
}

// GetObjectListFromSnapshotDiff calculates snapshot diff and gets object list from upstream
// It executes: OBJECTLIST DATABASE db1 TABLE t1 SNAPSHOT sp2 AGAINST SNAPSHOT sp1
// Returns: query result containing db name, table name, object list (stats, create_at, delete_at, is_tombstone)
// The caller is responsible for closing the result
func GetObjectListFromSnapshotDiff(
	ctx context.Context,
	iterationCtx *IterationContext,
) (*Result, error) {
	if iterationCtx == nil {
		return nil, moerr.NewInternalError(ctx, "iteration context is nil")
	}

	if iterationCtx.UpstreamExecutor == nil {
		return nil, moerr.NewInternalError(ctx, "upstream executor is nil")
	}

	// Check if we have current snapshot
	if iterationCtx.CurrentSnapshotName == "" {
		return nil, moerr.NewInternalError(ctx, "current snapshot name is empty")
	}

	// Determine database and table names based on sync level
	var dbName, tableName string
	if iterationCtx.SrcInfo.SyncLevel == SyncLevelDatabase || iterationCtx.SrcInfo.SyncLevel == SyncLevelTable {
		dbName = iterationCtx.SrcInfo.DBName
	}
	if iterationCtx.SrcInfo.SyncLevel == SyncLevelTable {
		tableName = iterationCtx.SrcInfo.TableName
	}

	// Determine against snapshot name
	var againstSnapshotName string
	if iterationCtx.PrevSnapshotName != "" {
		// Not first sync: get diff between current and previous snapshots
		againstSnapshotName = iterationCtx.PrevSnapshotName
	}
	// For first sync, againstSnapshotName is empty, which means get all objects from current snapshot

	// Build OBJECTLIST SQL
	objectListSQL := PublicationSQLBuilder.ObjectListSQL(
		dbName,
		tableName,
		iterationCtx.CurrentSnapshotName,
		againstSnapshotName,
	)

	// Execute SQL through upstream executor and return result directly
	result, err := iterationCtx.UpstreamExecutor.ExecSQL(ctx, objectListSQL)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to execute object list query: %v", err)
	}

	return result, nil
}

// submitObjectsAsInsert submits objects as INSERT operation
func submitObjectsAsInsert(ctx context.Context, iterationCtx *IterationContext, cnEngine engine.Engine, tombstoneInsertStats []ObjectWithTableInfo, dataInsertStats []ObjectWithTableInfo, mp *mpool.MPool) error {
	if len(tombstoneInsertStats) == 0 && len(dataInsertStats) == 0 {
		return nil
	}

	if iterationCtx == nil {
		return moerr.NewInternalError(ctx, "iteration context is nil")
	}
	if cnEngine == nil {
		return moerr.NewInternalError(ctx, "engine is nil")
	}

	// Group objects by (dbName, tableName)
	type tableKey struct {
		dbName    string
		tableName string
	}
	tombstoneByTable := make(map[tableKey][]objectio.ObjectStats)
	dataByTable := make(map[tableKey][]objectio.ObjectStats)

	for _, obj := range tombstoneInsertStats {
		key := tableKey{dbName: obj.DBName, tableName: obj.TableName}
		tombstoneByTable[key] = append(tombstoneByTable[key], obj.Stats)
	}
	for _, obj := range dataInsertStats {
		key := tableKey{dbName: obj.DBName, tableName: obj.TableName}
		dataByTable[key] = append(dataByTable[key], obj.Stats)
	}

	// Process each table separately
	for key, tombstoneStats := range tombstoneByTable {
		if len(tombstoneStats) == 0 {
			continue
		}

		// Get database using transaction from iteration context
		db, err := cnEngine.Database(ctx, key.dbName, iterationCtx.LocalTxn)
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to get database %s: %v", key.dbName, err)
		}

		// Get relation using transaction from iteration context
		rel, err := db.Relation(ctx, key.tableName, nil)
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to get relation %s.%s: %v", key.dbName, key.tableName, err)
		}

		// Log tombstone insert objects
		for _, stats := range tombstoneStats {
			logutil.Info("ccpr-iteration submitting object",
				zap.Uint64("task_id", iterationCtx.TaskID),
				zap.Uint64("lsn", iterationCtx.IterationLSN),
				zap.String("db_name", key.dbName),
				zap.String("table_name", key.tableName),
				zap.String("object_name", stats.ObjectName().String()),
				zap.String("object_info", stats.String()),
				zap.String("object_type", "tombstone"),
				zap.String("operation", "insert"),
			)
		}

		// Create batch with ObjectStats for deletion
		deleteBat := batch.NewWithSize(1)
		deleteBat.SetAttributes([]string{catalog.ObjectMeta_ObjectStats})

		// ObjectStats column (T_binary)
		statsVec := vector.NewVec(types.T_binary.ToType())
		deleteBat.Vecs[0] = statsVec

		// Append ObjectStats to the batch using Marshal()
		for _, stats := range tombstoneStats {
			statsBytes := stats.Marshal()
			if err := vector.AppendBytes(statsVec, statsBytes, false, mp); err != nil {
				deleteBat.Clean(mp)
				return moerr.NewInternalErrorf(ctx, "failed to append tombstone object stats: %v", err)
			}
		}

		deleteBat.SetRowCount(len(tombstoneStats))

		// Delete through relation
		if err := rel.Delete(ctx, deleteBat, ""); err != nil {
			deleteBat.Clean(mp)
			return moerr.NewInternalErrorf(ctx, "failed to delete tombstone objects: %v", err)
		}
		deleteBat.Clean(mp)
	}

	// Handle regular data objects: use the original Write logic
	for key, dataStats := range dataByTable {
		if len(dataStats) == 0 {
			continue
		}

		// Get database using transaction from iteration context
		db, err := cnEngine.Database(ctx, key.dbName, iterationCtx.LocalTxn)
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to get database %s: %v", key.dbName, err)
		}

		// Get relation using transaction from iteration context
		rel, err := db.Relation(ctx, key.tableName, nil)
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to get relation %s.%s: %v", key.dbName, key.tableName, err)
		}

		// Log data insert objects
		for _, stats := range dataStats {
			logutil.Info("ccpr-iteration submitting object",
				zap.Uint64("task_id", iterationCtx.TaskID),
				zap.Uint64("lsn", iterationCtx.IterationLSN),
				zap.String("db_name", key.dbName),
				zap.String("table_name", key.tableName),
				zap.String("object_name", stats.ObjectName().String()),
				zap.String("object_info", stats.String()),
				zap.String("object_type", "data"),
				zap.String("operation", "insert"),
			)
		}

		// Create batch with ObjectStats using the same structure as s3util
		bat := batch.NewWithSize(2)
		bat.SetAttributes([]string{catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats})

		// First column: BlockInfo (T_text)
		blockInfoVec := vector.NewVec(types.T_text.ToType())
		bat.Vecs[0] = blockInfoVec

		// Second column: ObjectStats (T_binary)
		statsVec := vector.NewVec(types.T_binary.ToType())
		bat.Vecs[1] = statsVec

		// Use ExpandObjectStatsToBatch to properly expand ObjectStats to batch
		// This handles the correct mapping between blocks and their parent objects
		if err := colexec.ExpandObjectStatsToBatch(
			mp,
			false, // isTombstone = false for INSERT
			bat,
			true, // isCNCreated = true
			dataStats...,
		); err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to expand object stats to batch: %v", err)
		}

		// Write through relation
		if err := rel.Write(ctx, bat); err != nil {
			bat.Clean(mp)
			return moerr.NewInternalErrorf(ctx, "failed to write objects: %v", err)
		}
		bat.Clean(mp)
	}

	return nil
}

// submitObjectsAsDelete submits objects as DELETE operation
// It uses SoftDeleteObject to soft delete objects by setting their deleteat timestamp
func submitObjectsAsDelete(ctx context.Context, iterationCtx *IterationContext, cnEngine engine.Engine, statsList []ObjectWithTableInfo, mp *mpool.MPool) error {
	if len(statsList) == 0 {
		return nil
	}

	if iterationCtx == nil {
		return moerr.NewInternalError(ctx, "iteration context is nil")
	}
	if cnEngine == nil {
		return moerr.NewInternalError(ctx, "engine is nil")
	}

	// Group objects by (dbName, tableName)
	type tableKey struct {
		dbName    string
		tableName string
	}
	statsByTable := make(map[tableKey][]ObjectWithTableInfo)

	for _, obj := range statsList {
		key := tableKey{dbName: obj.DBName, tableName: obj.TableName}
		statsByTable[key] = append(statsByTable[key], obj)
	}

	// Process each table separately
	for key, tableStats := range statsByTable {
		if len(tableStats) == 0 {
			continue
		}

		// Get database using transaction from iteration context
		db, err := cnEngine.Database(ctx, key.dbName, iterationCtx.LocalTxn)
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to get database %s: %v", key.dbName, err)
		}

		// Get relation using transaction from iteration context
		rel, err := db.Relation(ctx, key.tableName, nil)
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to get relation %s.%s: %v", key.dbName, key.tableName, err)
		}

		// Try to use SoftDeleteObject if available (for disttae txnTable or txnTableDelegate)
		// Otherwise fall back to the old Delete method
		// Check if it's a txnTableDelegate first
		if delegate, ok := rel.(interface {
			SoftDeleteObject(ctx context.Context, objID *objectio.ObjectId, isTombstone bool) error
		}); ok {
			// Use SoftDeleteObject for each object
			// The deleteat will be set to the transaction's commit timestamp
			for _, obj := range tableStats {
				objID := obj.Stats.ObjectName().ObjectId()
				// Check if it's a tombstone object - use IsTombstone field from ObjectWithTableInfo
				isTombstone := obj.IsTombstone
				objName := obj.Stats.ObjectName().String()

				// Log object deletion
				objectType := "data"
				if isTombstone {
					objectType = "tombstone"
				}
				logutil.Info("ccpr-iteration submitting object",
					zap.Uint64("task_id", iterationCtx.TaskID),
					zap.Uint64("lsn", iterationCtx.IterationLSN),
					zap.String("db_name", key.dbName),
					zap.String("table_name", key.tableName),
					zap.String("object_name", objName),
					zap.String("object_info", obj.Stats.String()),
					zap.String("object_type", objectType),
					zap.String("operation", "delete"),
				)

				// objID is already *objectio.ObjectId, so we pass it directly
				if err := delegate.SoftDeleteObject(ctx, objID, isTombstone); err != nil {
					return moerr.NewInternalErrorf(ctx, "failed to soft delete object %s: %v", objID.ShortStringEx(), err)
				}
			}
			continue
		}

		// Fallback to old Delete method for other relation types
		// Log objects before deletion
		for _, obj := range tableStats {
			objName := obj.Stats.ObjectName().String()
			// Check if it's a tombstone object - use IsTombstone field from ObjectWithTableInfo
			isTombstone := obj.IsTombstone
			objectType := "data"
			if isTombstone {
				objectType = "tombstone"
			}
			logutil.Info("ccpr-iteration submitting object",
				zap.Uint64("task_id", iterationCtx.TaskID),
				zap.Uint64("lsn", iterationCtx.IterationLSN),
				zap.String("db_name", key.dbName),
				zap.String("table_name", key.tableName),
				zap.String("object_name", objName),
				zap.String("object_info", obj.Stats.String()),
				zap.String("object_type", objectType),
				zap.String("operation", "delete"),
			)
		}

		// Create batch with ObjectStats for deletion
		bat := batch.NewWithSize(1)
		bat.SetAttributes([]string{catalog.ObjectMeta_ObjectStats})

		// ObjectStats column (T_binary)
		statsVec := vector.NewVec(types.T_binary.ToType())
		bat.Vecs[0] = statsVec

		// Append ObjectStats to the batch using Marshal()
		for _, obj := range tableStats {
			statsBytes := obj.Stats.Marshal()
			if err := vector.AppendBytes(statsVec, statsBytes, false, mp); err != nil {
				bat.Clean(mp)
				return moerr.NewInternalErrorf(ctx, "failed to append object stats: %v", err)
			}
		}

		bat.SetRowCount(len(tableStats))

		// Delete through relation
		if err := rel.Delete(ctx, bat, ""); err != nil {
			bat.Clean(mp)
			return moerr.NewInternalErrorf(ctx, "failed to delete objects: %v", err)
		}
		bat.Clean(mp)
	}

	return nil
}

// ExecuteIteration executes a complete iteration according to the design document
// It follows the sequence: initialization -> DDL -> snapshot diff -> object processing -> cleanup -> update system table
func ExecuteIteration(
	ctx context.Context,
	cnUUID string,
	cnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	taskID uint64,
	iterationLSN uint64,
	iterationState int8,
	upstreamSQLHelperFactory UpstreamSQLHelperFactory,
	mp *mpool.MPool,
) (err error) {
	var objectListResult *Result
	var iterationCtx *IterationContext

	// Check if account ID exists in context and is not 0
	if v := ctx.Value(defines.TenantIDKey{}); v != nil {
		if accountID, ok := v.(uint32); ok && accountID != 0 {
			return moerr.NewInternalErrorf(ctx, "account ID must be 0 or not set in context, got %d", accountID)
		}
	}

	iterationCtx, err = InitializeIterationContext(ctx, cnUUID, cnEngine, cnTxnClient, taskID, iterationLSN, upstreamSQLHelperFactory)
	if err != nil {
		return
	}

	// Log iteration start with task id, lsn, and src info
	logutil.Info("ccpr-iteration iteration start",
		zap.Uint64("task_id", iterationCtx.TaskID),
		zap.Uint64("lsn", iterationCtx.IterationLSN),
		zap.String("src_info", fmt.Sprintf("sync_level=%s, account_id=%d, db_name=%s, table_name=%s",
			iterationCtx.SrcInfo.SyncLevel,
			iterationCtx.SrcInfo.AccountID,
			iterationCtx.SrcInfo.DBName,
			iterationCtx.SrcInfo.TableName)),
	)

	defer func() {
		commitErr := iterationCtx.Close(err == nil)
		if commitErr != nil {
			if err != nil {
				err = moerr.NewInternalErrorf(ctx, "failed to close iteration context: %v; previous error: %v", commitErr, err)
			} else {
				err = moerr.NewInternalErrorf(ctx, "failed to close iteration context: %v", commitErr)
			}
		}
	}()
	// Step 0: 初始化阶段
	// 0.1 检查iteration状态
	if err = CheckIterationStatus(ctx, iterationCtx.LocalExecutor, taskID, cnUUID, iterationLSN); err != nil {
		return
	}

	// Update iteration state in defer to ensure it's always called
	defer func() {
		var errorMsg string
		if err != nil {
			errorMsg = err.Error()
		}
		var finalState int8
		if err == nil {
			finalState = IterationStateCompleted
		} else {
			finalState = IterationStateError
		}
		if err = UpdateIterationState(ctx, iterationCtx.LocalExecutor, taskID, finalState, iterationLSN, iterationCtx, errorMsg); err != nil {
			// Log error but don't override the original error
			err = moerr.NewInternalErrorf(ctx, "failed to update iteration state: %v", err)
		}
	}()

	// 1.1 请求上游snapshot (includes 1.1.2 请求上游的snapshot ts)
	if err = RequestUpstreamSnapshot(ctx, iterationCtx); err != nil {
		err = moerr.NewInternalErrorf(ctx, "failed to request upstream snapshot: %v", err)
		return
	}

	// 1.2 等待上游snapshot刷盘
	if err = WaitForSnapshotFlushed(ctx, iterationCtx, 1*time.Second, 30*time.Minute); err != nil {
		err = moerr.NewInternalErrorf(ctx, "failed to wait for snapshot to be flushed: %v", err)
		return
	}

	// Log snapshot information
	logutil.Info("ccpr-iteration snapshot info",
		zap.Uint64("task_id", iterationCtx.TaskID),
		zap.Uint64("lsn", iterationCtx.IterationLSN),
		zap.String("current_snapshot_name", iterationCtx.CurrentSnapshotName),
		zap.Int64("current_snapshot_ts", iterationCtx.CurrentSnapshotTS.Physical()),
		zap.String("prev_snapshot_name", iterationCtx.PrevSnapshotName),
		zap.Int64("prev_snapshot_ts", iterationCtx.PrevSnapshotTS.Physical()),
	)

	// TODO: 找到做snapshot的table，获取objectlist snapshot, 需要当前snapshot
	if err = ProcessDDLChanges(ctx, cnEngine, iterationCtx); err != nil {
		err = moerr.NewInternalErrorf(ctx, "failed to process DDL changes: %v", err)
		return
	}
	// Step 2: 计算snapshot diff获取object list
	objectListResult, err = GetObjectListFromSnapshotDiff(ctx, iterationCtx)
	if err != nil {
		err = moerr.NewInternalErrorf(ctx, "failed to get object list from snapshot diff: %v", err)
		return
	}
	defer func() {
		if objectListResult != nil {
			objectListResult.Close()
		}
	}()

	// Step 3: 获取object数据
	// 遍历object list中的每个object，调用FilterObject接口处理
	// 同时收集对象数据用于 Step 5 提交到 TN
	var collectedTombstoneDeleteStats []ObjectWithTableInfo
	var collectedTombstoneInsertStats []ObjectWithTableInfo
	var collectedDataDeleteStats []ObjectWithTableInfo
	var collectedDataInsertStats []ObjectWithTableInfo

	fs := cnEngine.(*disttae.Engine).FS()

	// Map to deduplicate objects by ObjectId
	// Key: ObjectId, Value: object info
	objectMap := make(map[objectio.ObjectId]ObjectWithTableInfo)

	if objectListResult != nil {
		// Check for errors during iteration
		if err = objectListResult.Err(); err != nil {
			err = moerr.NewInternalErrorf(ctx, "error reading object list result: %v", err)
			return
		}

		// Get snapshot TS from iteration context
		snapshotTS := iterationCtx.CurrentSnapshotTS

		// Iterate through object list
		for objectListResult.Next() {
			// Read columns: db name, table name, object stats, create at, delete at, is tombstone
			var dbName, tableName string
			var statsBytes []byte
			var createAt, deleteAt types.TS
			var isTombstone bool

			if err = objectListResult.Scan(&dbName, &tableName, &statsBytes, &createAt, &deleteAt, &isTombstone); err != nil {
				err = moerr.NewInternalErrorf(ctx, "failed to scan object list result: %v", err)
				return
			}

			// Parse ObjectStats from bytes
			var stats objectio.ObjectStats
			stats.UnMarshal(statsBytes)

			// Get ObjectId from stats
			objID := *stats.ObjectName().ObjectId()
			delete := !deleteAt.IsEmpty()

			// Check if this object already exists in map
			if existing, exists := objectMap[objID]; exists {
				// If there are two records, one without delete and one with delete, use delete to override
				if delete {
					// New record is delete, override existing record
					objectMap[objID] = ObjectWithTableInfo{
						Stats:       stats,
						IsTombstone: isTombstone,
						Delete:      true,
						DBName:      dbName,
						TableName:   tableName,
					}
				} else if existing.Delete {
					// Existing record is delete, keep delete (don't override)
					// Keep existing record
				} else {
					// Both are non-delete, update with new record
					objectMap[objID] = ObjectWithTableInfo{
						Stats:       stats,
						IsTombstone: isTombstone,
						Delete:      false,
						DBName:      dbName,
						TableName:   tableName,
					}
				}
			} else {
				// New object, add to map
				objectMap[objID] = ObjectWithTableInfo{
					Stats:       stats,
					IsTombstone: isTombstone,
					Delete:      delete,
					DBName:      dbName,
					TableName:   tableName,
				}
			}

		}

		// Extract objects from map to collected stats lists
		for _, info := range objectMap {
			statsBytes := info.Stats.Marshal()
			delete := info.Delete

			if info.Delete {
				// Object to delete
				if info.IsTombstone {
					collectedTombstoneDeleteStats = append(collectedTombstoneDeleteStats, info)
				} else {
					collectedDataDeleteStats = append(collectedDataDeleteStats, info)
				}
			} else {
				// Call FilterObject to handle the object (for both insert and delete)
				// FilterObject will:
				// - For aobj: get object from upstream, convert to batch, filter by snapshot TS, create new object
				//   For delete: mark object for deletion in ActiveAObj
				// - For nobj: get object from upstream and write directly to fileservice
				if err = FilterObject(ctx, statsBytes, snapshotTS, iterationCtx, fs, mp, delete); err != nil {
					err = moerr.NewInternalErrorf(ctx, "failed to filter object: %v", err)
					return
				}
				// Object to insert
				if info.IsTombstone {
					collectedTombstoneInsertStats = append(collectedTombstoneInsertStats, info)
				} else {
					collectedDataInsertStats = append(collectedDataInsertStats, info)
				}
			}
		}
	}

	// Process ActiveAObj and merge into collected stats
	// All objects (aobj and nobj) will be submitted together after processing
	if iterationCtx.ActiveAObj != nil {
		var objectsToDelete []objectio.ObjectId // Track UUIDs to delete from map

		for upstreamUUID, mapping := range iterationCtx.ActiveAObj {
			upstreamInfo, ok := objectMap[upstreamUUID]
			if !ok {
				continue
			}
			isTombstone := upstreamInfo.IsTombstone
			dbName := upstreamInfo.DBName
			tableName := upstreamInfo.TableName
			// If delete is true, delete the object and remove from map
			if mapping.Delete {
				// Delete previous object if it exists (previous object was created in earlier iteration)
				if !mapping.Previous.IsZero() {
					// Delete the previous object (assume data object, not tombstone)
					// Use srcInfo for dbName and tableName since ActiveAObj doesn't have table info
					if isTombstone {
						collectedTombstoneDeleteStats = append(collectedTombstoneDeleteStats, ObjectWithTableInfo{
							Stats:       mapping.Previous,
							DBName:      dbName,
							TableName:   tableName,
							IsTombstone: isTombstone,
							Delete:      true,
						})

					} else {
						collectedDataDeleteStats = append(collectedDataDeleteStats, ObjectWithTableInfo{
							Stats:       mapping.Previous,
							DBName:      dbName,
							TableName:   tableName,
							IsTombstone: isTombstone,
							Delete:      true,
						})
					}
				}
				// Mark for removal from map (no need to record in table)
				objectsToDelete = append(objectsToDelete, upstreamUUID)
				continue
			}

			// Check if current stats is valid (not zero value)
			if !mapping.Current.IsZero() {
				// New object to insert (not tombstone by default for ActiveAObj)
				// Use srcInfo for dbName and tableName since ActiveAObj doesn't have table info
				if isTombstone {
					collectedTombstoneInsertStats = append(collectedTombstoneInsertStats, ObjectWithTableInfo{
						Stats:       mapping.Current,
						DBName:      dbName,
						TableName:   tableName,
						IsTombstone: isTombstone,
						Delete:      false,
					})
				} else {
					collectedDataInsertStats = append(collectedDataInsertStats, ObjectWithTableInfo{
						Stats:       mapping.Current,
						DBName:      dbName,
						TableName:   tableName,
						IsTombstone: isTombstone,
						Delete:      false,
					})
				}
			}

			// Check if previous stats is valid (not zero value)
			if !mapping.Previous.IsZero() {
				// Previous object to delete (assume data object, not tombstone)
				// Use srcInfo for dbName and tableName since ActiveAObj doesn't have table info
				if isTombstone {
					collectedTombstoneDeleteStats = append(collectedTombstoneDeleteStats, ObjectWithTableInfo{
						Stats:       mapping.Previous,
						DBName:      dbName,
						TableName:   tableName,
						IsTombstone: isTombstone,
						Delete:      true,
					})
				} else {
					collectedDataDeleteStats = append(collectedDataDeleteStats, ObjectWithTableInfo{
						Stats:       mapping.Previous,
						DBName:      dbName,
						TableName:   tableName,
						IsTombstone: isTombstone,
						Delete:      true,
					})
				}
			}
		}

		// Remove objects marked for deletion from map
		for _, uuid := range objectsToDelete {
			delete(iterationCtx.ActiveAObj, uuid)
		}
	}

	// Submit all collected objects to TN in order: tombstone delete -> tombstone insert -> data delete -> data insert
	// Use downstream account ID from iterationCtx.SrcInfo
	// Set PkCheckByTN to SkipAllDedup to completely skip all deduplication checks in TN
	downstreamCtx := context.WithValue(ctx, defines.TenantIDKey{}, iterationCtx.SrcInfo.AccountID)
	downstreamCtx = context.WithValue(downstreamCtx, defines.PkCheckByTN{}, int8(cmd_util.SkipAllDedup))

	// 1. Submit tombstone delete objects (soft delete)
	if len(collectedTombstoneDeleteStats) > 0 {
		if err = submitObjectsAsDelete(downstreamCtx, iterationCtx, cnEngine, collectedTombstoneDeleteStats, mp); err != nil {
			err = moerr.NewInternalErrorf(ctx, "failed to submit tombstone delete objects: %v", err)
			return
		}
	}

	// 2. Submit tombstone insert objects
	if len(collectedTombstoneInsertStats) > 0 {
		if err = submitObjectsAsInsert(downstreamCtx, iterationCtx, cnEngine, collectedTombstoneInsertStats, nil, mp); err != nil {
			err = moerr.NewInternalErrorf(ctx, "failed to submit tombstone insert objects: %v", err)
			return
		}
	}

	// 3. Submit data delete objects (soft delete)
	if len(collectedDataDeleteStats) > 0 {
		if err = submitObjectsAsDelete(downstreamCtx, iterationCtx, cnEngine, collectedDataDeleteStats, mp); err != nil {
			err = moerr.NewInternalErrorf(ctx, "failed to submit data delete objects: %v", err)
			return
		}
	}

	// 4. Submit data insert objects
	if len(collectedDataInsertStats) > 0 {
		if err = submitObjectsAsInsert(downstreamCtx, iterationCtx, cnEngine, nil, collectedDataInsertStats, mp); err != nil {
			err = moerr.NewInternalErrorf(ctx, "failed to submit data insert objects: %v", err)
			return
		}
	}

	return
}

// tableKeyToString converts TableKey to string format for JSON serialization
// Format: "dbname.tablename"
func tableKeyToString(key TableKey) string {
	return fmt.Sprintf("%s.%s", key.DBName, key.TableName)
}

// parseTableKeyFromString parses string format to TableKey
// Format: "dbname.tablename" for table keys
// Legacy format "db_dbname" is ignored (database IDs are no longer stored)
func parseTableKeyFromString(keyStr string) TableKey {
	// Ignore legacy database key format "db_dbname"
	if strings.HasPrefix(keyStr, "db_") {
		// Return empty key for legacy database keys (they are no longer used)
		return TableKey{}
	}
	// Table key format: "dbname.tablename"
	parts := strings.SplitN(keyStr, ".", 2)
	if len(parts) == 2 {
		return TableKey{DBName: parts[0], TableName: parts[1]}
	}
	// Invalid format, return empty key
	return TableKey{}
}

// parseObjectIdFromString parses ObjectId from string format
// Format: "{segment}_{offset}" where segment is UUID string and offset is uint16
func parseObjectIdFromString(str string) (objectio.ObjectId, error) {
	parts := strings.SplitN(str, "_", 2)
	if len(parts) != 2 {
		return objectio.ObjectId{}, moerr.NewInternalErrorNoCtx(fmt.Sprintf("invalid object id format: %s, expected format: {uuid}_{offset}", str))
	}

	// Parse segment (UUID)
	segment, err := types.ParseUuid(parts[0])
	if err != nil {
		return objectio.ObjectId{}, moerr.NewInternalErrorNoCtx(fmt.Sprintf("failed to parse segment UUID %s: %v", parts[0], err))
	}

	// Parse offset (uint16)
	offsetUint, err := strconv.ParseUint(parts[1], 10, 16)
	if err != nil {
		return objectio.ObjectId{}, moerr.NewInternalErrorNoCtx(fmt.Sprintf("failed to parse offset %s: %v", parts[1], err))
	}
	offset := uint16(offsetUint)

	// Build ObjectId
	var objID objectio.ObjectId
	copy(objID[:types.UuidSize], segment[:])
	copy(objID[types.UuidSize:types.UuidSize+2], types.EncodeUint16(&offset))

	return objID, nil
}
