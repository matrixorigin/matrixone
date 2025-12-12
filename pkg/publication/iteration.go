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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
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
	ActiveAObj          map[string]AObjMappingJSON `json:"active_aobj"`         // ActiveAObj as serializable map
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

	if err := result.Scan(&subscriptionName, &syncLevel, &dbName, &tableName, &upstreamConn, &contextJSON); err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to scan query result: %v", err)
	}

	// Validate required fields
	if !subscriptionName.Valid {
		return nil, moerr.NewInternalErrorf(ctx, "subscription_name is null for task_id %d", taskID)
	}
	if !syncLevel.Valid {
		return nil, moerr.NewInternalErrorf(ctx, "sync_level is null for task_id %d", taskID)
	}

	// Build SrcInfo from sync_level, db_name, table_name
	srcInfo := SrcInfo{
		SyncLevel: syncLevel.String,
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
		ActiveAObj:       make(map[string]AObjMapping),
		TableIDs:         make(map[string]uint64),
	}

	// Parse context JSON if available
	if contextJSON.Valid && contextJSON.String != "" && contextJSON.String != "null" {
		var ctxJSON IterationContextJSON
		if err := json.Unmarshal([]byte(contextJSON.String), &ctxJSON); err != nil {
			return nil, moerr.NewInternalErrorf(ctx, "failed to unmarshal context JSON: %v", err)
		}

		// Restore ActiveAObj from JSON
		if ctxJSON.ActiveAObj != nil {
			for uuid, mappingJSON := range ctxJSON.ActiveAObj {
				mapping := AObjMapping{}
				// Deserialize Current ObjectStats from base64
				if mappingJSON.Current != "" {
					currentBytes, err := base64.StdEncoding.DecodeString(mappingJSON.Current)
					if err != nil {
						return nil, moerr.NewInternalErrorf(ctx, "failed to decode current object stats for uuid %s: %v", uuid, err)
					}
					if len(currentBytes) == objectio.ObjectStatsLen {
						mapping.Current.UnMarshal(currentBytes)
					}
				}
				// Deserialize Previous ObjectStats from base64
				if mappingJSON.Previous != "" {
					previousBytes, err := base64.StdEncoding.DecodeString(mappingJSON.Previous)
					if err != nil {
						return nil, moerr.NewInternalErrorf(ctx, "failed to decode previous object stats for uuid %s: %v", uuid, err)
					}
					if len(previousBytes) == objectio.ObjectStatsLen {
						mapping.Previous.UnMarshal(previousBytes)
					}
				}
				iterationCtx.ActiveAObj[uuid] = mapping
			}
		}

		// Restore TableIDs from JSON
		if ctxJSON.TableIDs != nil {
			iterationCtx.TableIDs = ctxJSON.TableIDs
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

func (iterCtx *IterationContext) Close() error {
	ctx := context.Background()
	if iterCtx.LocalExecutor != nil {
		iterCtx.LocalExecutor.EndTxn(ctx, false)
		iterCtx.LocalExecutor.Close()
	}
	if iterCtx.UpstreamExecutor != nil {
		iterCtx.UpstreamExecutor.EndTxn(ctx, false)
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
				activeAObjJSON[uuid] = mappingJSON
			}
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
			TableIDs:            iterationCtx.TableIDs,
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
		createSnapshotSQL = PublicationSQLBuilder.CreateSnapshotForAccountSQL(
			snapshotName,
			iterationCtx.SrcInfo.AccountName,
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

// SubmitObjectsToTN submits objects to TN node using the transaction from iteration context
// It processes:
// 1. Objects from objectlist with DeleteAt == 0 (submit as INSERT)
// 2. New objects from ActiveAObj (submit as INSERT)
// 3. Objects from objectlist with DeleteAt != 0 (submit as DELETE)
// 4. Previous objects from ActiveAObj (submit as DELETE)
func SubmitObjectsToTN(
	ctx context.Context,
	iterationCtx *IterationContext,
	cnEngine engine.Engine,
	objectListResult *Result,
) error {
	if iterationCtx == nil {
		return moerr.NewInternalError(ctx, "iteration context is nil")
	}
	if cnEngine == nil {
		return moerr.NewInternalError(ctx, "engine is nil")
	}

	// Validate sync level - currently only support table level
	if iterationCtx.SrcInfo.SyncLevel != SyncLevelTable {
		return moerr.NewInternalErrorf(ctx, "table level sync required for submitting objects, got %s",
			iterationCtx.SrcInfo.SyncLevel)
	}

	// Validate table ID exists
	tableKey := fmt.Sprintf("%s.%s", iterationCtx.SrcInfo.DBName, iterationCtx.SrcInfo.TableName)
	if _, ok := iterationCtx.TableIDs[tableKey]; !ok {
		return moerr.NewInternalErrorf(ctx, "table ID not found for %s.%s",
			iterationCtx.SrcInfo.DBName, iterationCtx.SrcInfo.TableName)
	}

	// Collect objects to insert and delete
	var insertStats []objectio.ObjectStats
	var deleteStats []objectio.ObjectStats

	// Process objectlist result
	if objectListResult != nil {
		// Scan objectlist result
		// Assuming the result has columns: stats, create_at, delete_at, is_tombstone
		for objectListResult.Next() {
			var statsBytes []byte
			var createAt, deleteAt types.TS
			var isTombstone bool

			if err := objectListResult.Scan(&statsBytes, &createAt, &deleteAt, &isTombstone); err != nil {
				return moerr.NewInternalErrorf(ctx, "failed to scan object list result: %v", err)
			}

			if len(statsBytes) != objectio.ObjectStatsLen {
				return moerr.NewInternalErrorf(ctx, "invalid object stats length: expected %d, got %d",
					objectio.ObjectStatsLen, len(statsBytes))
			}

			var stats objectio.ObjectStats
			stats.UnMarshal(statsBytes)

			if deleteAt.IsEmpty() {
				// Object to insert: DeleteAt is empty
				if !isTombstone {
					insertStats = append(insertStats, stats)
				} else {
					// Tombstone object to insert
					insertStats = append(insertStats, stats)
				}
			} else {
				// Object to delete: DeleteAt is not empty
				deleteStats = append(deleteStats, stats)
			}
		}

		if err := objectListResult.Err(); err != nil {
			return moerr.NewInternalErrorf(ctx, "error reading object list result: %v", err)
		}
	}

	// Process ActiveAObj
	if iterationCtx.ActiveAObj != nil {
		for upstreamUUID, mapping := range iterationCtx.ActiveAObj {
			// Check if current stats is valid (not zero value)
			var zeroStats objectio.ObjectStats
			if mapping.Current != zeroStats {
				// New object to insert
				insertStats = append(insertStats, mapping.Current)
			}

			// Check if previous stats is valid (not zero value)
			if mapping.Previous != zeroStats {
				// Previous object to delete
				deleteStats = append(deleteStats, mapping.Previous)
			}

			// Update ActiveAObj: move current to previous for next iteration
			// Note: This should be done after processing, but we do it here for clarity
			_ = upstreamUUID // avoid unused variable warning
		}
	}

	// Submit insert objects
	if len(insertStats) > 0 {
		if err := submitObjectsAsInsert(ctx, iterationCtx, cnEngine, iterationCtx.SrcInfo.DBName, iterationCtx.SrcInfo.TableName, insertStats); err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to submit insert objects: %v", err)
		}
	}

	// Submit delete objects
	if len(deleteStats) > 0 {
		if err := submitObjectsAsDelete(ctx, iterationCtx, cnEngine, iterationCtx.SrcInfo.DBName, iterationCtx.SrcInfo.TableName, deleteStats); err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to submit delete objects: %v", err)
		}
	}

	return nil
}

// submitObjectsAsInsert submits objects as INSERT operation
func submitObjectsAsInsert(ctx context.Context, iterationCtx *IterationContext, cnEngine engine.Engine, dbName, tableName string, statsList []objectio.ObjectStats) error {
	if len(statsList) == 0 {
		return nil
	}

	if iterationCtx == nil {
		return moerr.NewInternalError(ctx, "iteration context is nil")
	}
	if cnEngine == nil {
		return moerr.NewInternalError(ctx, "engine is nil")
	}
	if dbName == "" || tableName == "" {
		return moerr.NewInternalError(ctx, "db name or table name is empty")
	}

	// Get database using transaction from iteration context
	db, err := cnEngine.Database(ctx, dbName, iterationCtx.LocalTxn)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to get database %s: %v", dbName, err)
	}

	// Get relation using transaction from iteration context
	rel, err := db.Relation(ctx, tableName, nil)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to get relation %s.%s: %v", dbName, tableName, err)
	}

	// Create batch with ObjectStats
	bat := batch.NewWithSize(2)
	bat.SetAttributes([]string{catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats})
	bat.SetRowCount(len(statsList))

	// First column: BlockInfo (can be empty for object stats only)
	blockInfoVec := vector.NewVec(types.T_varchar.ToType())
	bat.Vecs[0] = blockInfoVec

	// Second column: ObjectStats
	statsVec := vector.NewVec(types.T_varchar.ToType())
	bat.Vecs[1] = statsVec

	// Append ObjectStats to the batch
	for _, stats := range statsList {
		statsBytes := stats[:]
		if err := vector.AppendBytes(statsVec, statsBytes, false, nil); err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to append object stats: %v", err)
		}
		// Append empty block info (or you can construct proper block info if needed)
		if err := vector.AppendBytes(blockInfoVec, nil, false, nil); err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to append block info: %v", err)
		}
	}

	// Write through relation
	if err := rel.Write(ctx, bat); err != nil {
		bat.Clean(nil)
		return moerr.NewInternalErrorf(ctx, "failed to write objects: %v", err)
	}

	return nil
}

// submitObjectsAsDelete submits objects as DELETE operation
func submitObjectsAsDelete(ctx context.Context, iterationCtx *IterationContext, cnEngine engine.Engine, dbName, tableName string, statsList []objectio.ObjectStats) error {
	if len(statsList) == 0 {
		return nil
	}

	if iterationCtx == nil {
		return moerr.NewInternalError(ctx, "iteration context is nil")
	}
	if cnEngine == nil {
		return moerr.NewInternalError(ctx, "engine is nil")
	}
	if dbName == "" || tableName == "" {
		return moerr.NewInternalError(ctx, "db name or table name is empty")
	}

	// Get database using transaction from iteration context
	db, err := cnEngine.Database(ctx, dbName, iterationCtx.LocalTxn)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to get database %s: %v", dbName, err)
	}

	// Get relation using transaction from iteration context
	rel, err := db.Relation(ctx, tableName, nil)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to get relation %s.%s: %v", dbName, tableName, err)
	}

	// Create batch with ObjectStats for deletion
	bat := batch.NewWithSize(1)
	bat.SetAttributes([]string{catalog.ObjectMeta_ObjectStats})
	bat.SetRowCount(len(statsList))

	// ObjectStats column
	statsVec := vector.NewVec(types.T_varchar.ToType())
	bat.Vecs[0] = statsVec

	// Append ObjectStats to the batch
	for _, stats := range statsList {
		statsBytes := stats[:]
		if err := vector.AppendBytes(statsVec, statsBytes, false, nil); err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to append object stats: %v", err)
		}
	}

	// Delete through relation
	if err := rel.Delete(ctx, bat, ""); err != nil {
		bat.Clean(nil)
		return moerr.NewInternalErrorf(ctx, "failed to delete objects: %v", err)
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
) (err error) {
	var objectListResult *Result
	var ddlStatements []string
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

	// Step 0: 初始化阶段
	// 0.1 检查iteration状态
	if err = CheckIterationStatus(ctx, iterationCtx.LocalExecutor, taskID, cnUUID, iterationLSN); err != nil {
		return
	}

	defer func() {
		iterationCtx.Close()
	}()

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
		if err := UpdateIterationState(ctx, iterationCtx.LocalExecutor, taskID, finalState, iterationLSN, iterationCtx, errorMsg); err != nil {
			// Log error but don't override the original error
			err = moerr.NewInternalErrorf(ctx, "failed to update iteration state: %v", err)
		}
	}()

	// 1.2 查询上游三表获取DDL
	ddlStatements, err = QueryUpstreamDDL(ctx, iterationCtx, iterationCtx.LocalExecutor)
	if err != nil {
		err = moerr.NewInternalErrorf(ctx, "failed to query upstream DDL: %v", err)
		return
	}

	// Execute DDL statements locally
	for _, ddl := range ddlStatements {
		if ddl != "" {
			// TODO: Execute DDL using local executor or transaction
			// This might require executing through the local transaction
			// For now, we'll execute through localExecutor
			var result *Result
			result, err = iterationCtx.LocalExecutor.ExecSQL(ctx, ddl)
			if err != nil {
				err = moerr.NewInternalErrorf(ctx, "failed to execute DDL: %v", err)
				return
			}
			if result != nil {
				result.Close()
			}
		}
	}

	// 1.1 请求上游snapshot (includes 1.1.2 请求上游的snapshot ts)
	if err = RequestUpstreamSnapshot(ctx, iterationCtx); err != nil {
		err = moerr.NewInternalErrorf(ctx, "failed to request upstream snapshot: %v", err)
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
	var collectedInsertStats []objectio.ObjectStats
	var collectedDeleteStats []objectio.ObjectStats

	fs := cnEngine.(*disttae.Engine).FS()

	if objectListResult != nil {
		// Check for errors during iteration
		if err = objectListResult.Err(); err != nil {
			err = moerr.NewInternalErrorf(ctx, "error reading object list result: %v", err)
			return
		}
		// Create a temporary mpool for FilterObject
		var mp *mpool.MPool
		mp, err = mpool.NewMPool("iteration_filter", 0, mpool.NoFixed)
		if err != nil {
			err = moerr.NewInternalErrorf(ctx, "failed to create mpool: %v", err)
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

			// Collect object stats for Step 5 (SubmitObjectsToTN)
			if len(statsBytes) == objectio.ObjectStatsLen {
				var stats objectio.ObjectStats
				stats.UnMarshal(statsBytes)
				if deleteAt.IsEmpty() {
					// Object to insert: DeleteAt is empty
					collectedInsertStats = append(collectedInsertStats, stats)
				} else {
					// Object to delete: DeleteAt is not empty
					collectedDeleteStats = append(collectedDeleteStats, stats)
				}
			}

			// Call FilterObject to handle the object
			// FilterObject will:
			// - For aobj: get object from upstream, convert to batch, filter by snapshot TS, create new object
			// - For nobj: get object from upstream and write directly to fileservice
			if err = FilterObject(ctx, statsBytes, snapshotTS, iterationCtx, fs, mp); err != nil {
				err = moerr.NewInternalErrorf(ctx, "failed to filter object: %v", err)
				return
			}
		}
	}

	// Submit collected objects to TN
	// Process collected insert stats
	if len(collectedInsertStats) > 0 {
		if err = submitObjectsAsInsert(ctx, iterationCtx, cnEngine, iterationCtx.SrcInfo.DBName, iterationCtx.SrcInfo.TableName, collectedInsertStats); err != nil {
			err = moerr.NewInternalErrorf(ctx, "failed to submit insert objects: %v", err)
			return
		}
	}

	// Process collected delete stats
	if len(collectedDeleteStats) > 0 {
		if err = submitObjectsAsDelete(ctx, iterationCtx, cnEngine, iterationCtx.SrcInfo.DBName, iterationCtx.SrcInfo.TableName, collectedDeleteStats); err != nil {
			err = moerr.NewInternalErrorf(ctx, "failed to submit delete objects: %v", err)
			return
		}
	}

	// Process ActiveAObj (same as SubmitObjectsToTN does)
	if iterationCtx.ActiveAObj != nil {
		var activeInsertStats []objectio.ObjectStats
		var activeDeleteStats []objectio.ObjectStats

		for upstreamUUID, mapping := range iterationCtx.ActiveAObj {
			// Check if current stats is valid (not zero value)
			var zeroStats objectio.ObjectStats
			if mapping.Current != zeroStats {
				// New object to insert
				activeInsertStats = append(activeInsertStats, mapping.Current)
			}

			// Check if previous stats is valid (not zero value)
			if mapping.Previous != zeroStats {
				// Previous object to delete
				activeDeleteStats = append(activeDeleteStats, mapping.Previous)
			}

			_ = upstreamUUID // avoid unused variable warning
		}

		// Submit active insert objects
		if len(activeInsertStats) > 0 {
			if err = submitObjectsAsInsert(ctx, iterationCtx, cnEngine, iterationCtx.SrcInfo.DBName, iterationCtx.SrcInfo.TableName, activeInsertStats); err != nil {
				err = moerr.NewInternalErrorf(ctx, "failed to submit active insert objects: %v", err)
				return
			}
		}

		// Submit active delete objects
		if len(activeDeleteStats) > 0 {
			if err = submitObjectsAsDelete(ctx, iterationCtx, cnEngine, iterationCtx.SrcInfo.DBName, iterationCtx.SrcInfo.TableName, activeDeleteStats); err != nil {
				err = moerr.NewInternalErrorf(ctx, "failed to submit active delete objects: %v", err)
				return
			}
		}
	}

	return
}
