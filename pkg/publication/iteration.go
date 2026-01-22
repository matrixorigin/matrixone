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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
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

// UTHelper is an interface for unit test helpers
// It provides hooks for testing purposes
type UTHelper interface {
	// OnSnapshotCreated is called after a snapshot is created in the upstream
	OnSnapshotCreated(ctx context.Context, snapshotName string, snapshotTS types.TS) error
}

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
	ActiveAObj         map[string]AObjMappingJSON `json:"active_aobj"` // ActiveAObj as serializable map (key is ObjectId as string)
	TableIDs           map[string]uint64          `json:"table_ids"`
	IndexTableMappings map[string]string          `json:"index_table_mappings"` // IndexTableMappings as serializable map (key is upstream_index_table_name, value is downstream_index_table_name)
}

func (iterCtx *IterationContext) String() string {
	return fmt.Sprintf("%d-%d", iterCtx.TaskID, iterCtx.IterationLSN)
}

// InitializeIterationContext initializes IterationContext from mo_ccpr_log table
// It reads subscription_name, srcinfo, upstream_conn, and context from the table,
// creates local executor and upstream executor, creates a local transaction,
// and sets up the local executor to use that transaction.
// iterationLSN is passed in as a parameter.
// ActiveAObj and TableIDs are read from the context JSON field.
// sqlExecutorRetryOpt: retry options for SQL executor operations (nil to use default)
func InitializeIterationContext(
	ctx context.Context,
	cnUUID string,
	cnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	taskID uint64,
	iterationLSN uint64,
	upstreamSQLHelperFactory UpstreamSQLHelperFactory,
	sqlExecutorRetryOpt *SQLExecutorRetryOption,
) (*IterationContext, error) {
	if cnTxnClient == nil {
		return nil, moerr.NewInternalError(ctx, "txn client is nil")
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
		return nil, moerr.NewInternalErrorf(ctx, "failed to create local transaction: %v", err)
	}

	// Register the transaction with the engine
	err = cnEngine.New(ctx, localTxn)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to register transaction with engine: %v", err)
	}

	// Create local executor first (without transaction) to query mo_ccpr_log
	// mo_ccpr_log is a system table, so we must use system account
	// Local executor doesn't need upstream SQL helper (no special SQL statements)
	localRetryOpt := sqlExecutorRetryOpt
	if localRetryOpt == nil {
		localRetryOpt = DefaultSQLExecutorRetryOption()
	}
	// Override classifier for local executor
	localRetryOpt = &SQLExecutorRetryOption{
		MaxRetries:    localRetryOpt.MaxRetries,
		RetryInterval: localRetryOpt.RetryInterval,
	}
	localRetryOpt.Classifier = NewDownstreamConnectionClassifier()
	localExecutorInternal, err := NewInternalSQLExecutor(cnUUID, nil, nil, catalog.System_Account, localRetryOpt, false)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to create local executor: %v", err)
	}
	// Set the transaction in local executor
	localExecutorInternal.SetTxn(localTxn)
	var localExecutor SQLExecutor = localExecutorInternal

	// Query mo_ccpr_log table to get subscription_name, sync_level, db_name, table_name, upstream_conn, context
	// mo_ccpr_log is a system table, so we must use system account context
	systemCtx := context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	querySQL := PublicationSQLBuilder.QueryMoCcprLogFullSQL(taskID)
	result, err := localExecutor.ExecSQL(systemCtx, nil, querySQL, true, false)
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
	var errorMessage sql.NullString
	var subscriptionState int8

	if !result.Next() {
		if err := result.Err(); err != nil {
			return nil, moerr.NewInternalErrorf(ctx, "failed to read query result: %v", err)
		}
		return nil, moerr.NewInternalErrorf(ctx, "no rows returned for task_id %d", taskID)
	}

	if err := result.Scan(&subscriptionName, &syncLevel, &accountID, &dbName, &tableName, &upstreamConn, &contextJSON, &errorMessage, &subscriptionState); err != nil {
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
		upstreamRetryOpt := sqlExecutorRetryOpt
		if upstreamRetryOpt == nil {
			upstreamRetryOpt = DefaultSQLExecutorRetryOption()
		}
		// Override classifier for upstream executor
		upstreamRetryOpt = &SQLExecutorRetryOption{
			MaxRetries:    upstreamRetryOpt.MaxRetries,
			RetryInterval: upstreamRetryOpt.RetryInterval,
		}
		upstreamRetryOpt.Classifier = NewUpstreamConnectionClassifier()
		upstreamExecutorInternal, err := NewInternalSQLExecutor(cnUUID, cnTxnClient, cnEngine, upstreamAccountID, upstreamRetryOpt, true)
		if err != nil {
			return nil, moerr.NewInternalErrorf(ctx, "failed to create upstream executor: %v", err)
		}
		upstreamExecutor = upstreamExecutorInternal
		// Create upstream SQL helper if factory is provided and upstream executor is InternalSQLExecutor
		if upstreamSQLHelperFactory != nil {
			if upstreamExecutorInternal, ok := upstreamExecutor.(*InternalSQLExecutor); ok {
				// Create helper with nil txnOp - it will be updated when SetTxn is called
				// Pass txnClient from InternalSQLExecutor so helper can create transactions when needed
				helper := upstreamSQLHelperFactory(
					nil, // txnOp will be set when SetTxn is called
					cnEngine,
					upstreamAccountID,
					upstreamExecutorInternal.GetInternalExec(),
					upstreamExecutorInternal.GetTxnClient(), // Pass txnClient so helper can create txn if needed
				)
				upstreamExecutorInternal.SetUpstreamSQLHelper(helper)
			}
		}
		// Helper will be created after local transaction is created (helper needs txnOp)
	} else {
		// Parse upstream connection string with optional password decryption
		// KeyEncryptionKey will be read using cnUUID (similar to CDC's getGlobalPuWrapper)
		connConfig, err := ParseUpstreamConnWithDecrypt(ctx, upstreamConn.String, localExecutor, cnUUID)
		if err != nil {
			return nil, moerr.NewInternalErrorf(ctx, "failed to parse upstream connection string: %v", err)
		}
		upstreamExecutor, err = NewUpstreamExecutor(
			connConfig.Account,
			connConfig.User,
			connConfig.Password,
			connConfig.Host,
			connConfig.Port,
			-1, // retryTimes: -1 for infinite retry
			0,  // retryDuration: 0 for no limit
			connConfig.Timeout,
			NewUpstreamConnectionClassifier(),
		)
		if err != nil {
			return nil, moerr.NewInternalErrorf(ctx, "failed to create upstream executor: %v", err)
		}

	}
	// Parse error message if available
	var errorMetadata *ErrorMetadata
	if errorMessage.Valid && errorMessage.String != "" {
		errorMetadata = Parse(errorMessage.String)
	}

	// Initialize IterationContext
	iterationCtx := &IterationContext{
		TaskID:             taskID,
		SubscriptionName:   subscriptionName.String,
		SrcInfo:            srcInfo,
		LocalTxn:           localTxn,
		LocalExecutor:      localExecutor,
		UpstreamExecutor:   upstreamExecutor,
		IterationLSN:       iterationLSN,
		SubscriptionState:  subscriptionState,
		ActiveAObj:         make(map[objectio.ObjectId]AObjMapping),
		TableIDs:           make(map[TableKey]uint64),
		IndexTableMappings: make(map[string]string),
		ErrorMetadata:      errorMetadata,
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

		// Restore IndexTableMappings from JSON
		if ctxJSON.IndexTableMappings != nil {
			iterationCtx.IndexTableMappings = make(map[string]string)
			for upstreamName, downstreamName := range ctxJSON.IndexTableMappings {
				// Only store valid mappings
				if upstreamName != "" && downstreamName != "" {
					iterationCtx.IndexTableMappings[upstreamName] = downstreamName
				}
			}
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

	// Check subscription state: if not running, rollback transaction
	if iterCtx.SubscriptionState != SubscriptionStateRunning {
		commit = false
	}

	var err error
	if iterCtx.LocalExecutor != nil {
		tmpErr := iterCtx.LocalExecutor.EndTxn(ctx, commit)
		if tmpErr != nil {
			logutil.Infof("ccpr-iteration local executor end txn error: %v", tmpErr)
			err = tmpErr
		}
		tmpErr = iterCtx.LocalExecutor.Close()
		if tmpErr != nil {
			logutil.Infof("ccpr-iteration local executor close error: %v", tmpErr)
			err = tmpErr
		}
	}
	if iterCtx.UpstreamExecutor != nil {
		tmpErr := iterCtx.UpstreamExecutor.Close()
		if tmpErr != nil {
			logutil.Infof("ccpr-iteration upstream executor close error: %v", tmpErr)
			err = tmpErr
		}
	}
	return err
}

// UpdateIterationState updates iteration state, iteration LSN, iteration context, error message, and subscription state in mo_ccpr_log table
// It serializes the relevant parts of IterationContext to JSON and updates the corresponding fields
func UpdateIterationState(
	ctx context.Context,
	executor SQLExecutor,
	taskID uint64,
	iterationState int8,
	iterationLSN uint64,
	iterationCtx *IterationContext,
	errorMessage string,
	useTxn bool,
	subscriptionState int8,
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

		// Convert IndexTableMappings to string map for JSON serialization
		indexTableMappingsJSON := make(map[string]string)
		if iterationCtx.IndexTableMappings != nil {
			for upstreamName, downstreamName := range iterationCtx.IndexTableMappings {
				indexTableMappingsJSON[upstreamName] = downstreamName
			}
		}

		// Create a serializable context structure
		ctxJSON := IterationContextJSON{
			TaskID:             iterationCtx.TaskID,
			SubscriptionName:   iterationCtx.SubscriptionName,
			SrcInfo:            iterationCtx.SrcInfo,
			ActiveAObj:         activeAObjJSON,
			TableIDs:           tableIDsJSON,
			IndexTableMappings: indexTableMappingsJSON,
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
		subscriptionState,
	)

	// Execute update SQL using system account context
	// mo_ccpr_log is a system table, so we must use system account
	systemCtx := context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	ctxWithTimeout, cancel := context.WithTimeout(systemCtx, time.Minute)
	defer cancel()
	result, err := executor.ExecSQL(ctxWithTimeout, nil, updateSQL, useTxn, false)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to execute update SQL: %v", err)
	}
	defer result.Close()

	return nil
}

// UpdateIterationStateNoSubscriptionState updates iteration state, iteration LSN, iteration context, and error message in mo_ccpr_log table
// It does NOT update the subscription state field - used for successful iterations
func UpdateIterationStateNoSubscriptionState(
	ctx context.Context,
	executor SQLExecutor,
	taskID uint64,
	iterationState int8,
	iterationLSN uint64,
	iterationCtx *IterationContext,
	useTxn bool,
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

		// Convert IndexTableMappings to string map for JSON serialization
		indexTableMappingsJSON := make(map[string]string)
		if iterationCtx.IndexTableMappings != nil {
			for upstreamName, downstreamName := range iterationCtx.IndexTableMappings {
				indexTableMappingsJSON[upstreamName] = downstreamName
			}
		}

		// Create a serializable context structure
		ctxJSON := IterationContextJSON{
			TaskID:             iterationCtx.TaskID,
			SubscriptionName:   iterationCtx.SubscriptionName,
			SrcInfo:            iterationCtx.SrcInfo,
			ActiveAObj:         activeAObjJSON,
			TableIDs:           tableIDsJSON,
			IndexTableMappings: indexTableMappingsJSON,
		}

		contextBytes, err := json.Marshal(ctxJSON)
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to marshal iteration context: %v", err)
		}
		contextJSON = string(contextBytes)
	} else {
		contextJSON = "null"
	}

	// Build update SQL without state field
	updateSQL := PublicationSQLBuilder.UpdateMoCcprLogNoStateSQL(
		taskID,
		iterationState,
		iterationLSN,
		contextJSON,
		errorMessage,
	)

	// Execute update SQL using system account context
	// mo_ccpr_log is a system table, so we must use system account
	systemCtx := context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	result, err := executor.ExecSQL(systemCtx, nil, updateSQL, useTxn, false)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to execute update SQL: %v", err)
	}
	defer result.Close()

	return nil
}

// CheckStateBeforeUpdate checks the state, iteration_state, and iteration_lsn in mo_ccpr_log table before update
// It verifies that state is running, iteration_state is running, and iteration_lsn matches the expected value
// This check uses a separate executor (new transaction) to ensure isolation
func CheckStateBeforeUpdate(
	ctx context.Context,
	executor SQLExecutor,
	taskID uint64,
	expectedIterationLSN uint64,
) error {
	// Build SQL query using sql_builder
	querySQL := PublicationSQLBuilder.QueryMoCcprLogStateBeforeUpdateSQL(taskID)

	// Execute SQL query using system account context
	// mo_ccpr_log is a system table, so we must use system account
	systemCtx := context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	ctxWithTimeout, cancel := context.WithTimeout(systemCtx, time.Minute)
	defer cancel()
	result, err := executor.ExecSQL(ctxWithTimeout, nil, querySQL, false, false)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to execute state check query: %v", err)
	}
	defer result.Close()

	// Scan the result - expecting columns: state, iteration_state, iteration_lsn
	var subscriptionState int8
	var iterationState int8
	var iterationLSN uint64

	if !result.Next() {
		if err := result.Err(); err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to read state check query result: %v", err)
		}
		return moerr.NewInternalErrorf(ctx, "no rows returned for task_id %d in state check", taskID)
	}

	if err := result.Scan(&subscriptionState, &iterationState, &iterationLSN); err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to scan state check query result: %v", err)
	}

	// Check if state is running
	if subscriptionState != SubscriptionStateRunning {
		return moerr.NewInternalErrorf(ctx, "subscription state is not running: expected %d (running), got %d", SubscriptionStateRunning, subscriptionState)
	}

	// Check if iteration_state is running
	if iterationState != IterationStateRunning {
		return moerr.NewInternalErrorf(ctx, "iteration_state is not running: expected %d (running), got %d", IterationStateRunning, iterationState)
	}

	// Check if iteration_lsn matches
	if iterationLSN != expectedIterationLSN {
		return moerr.NewInternalErrorf(ctx, "iteration_lsn mismatch: expected %d, got %d", expectedIterationLSN, iterationLSN)
	}

	return nil
}

// CheckIterationStatus checks the iteration status in mo_ccpr_log table
// It verifies that cn_uuid, iteration_lsn match the expected values,
// and that iteration_state is pending
// If all checks pass, it updates iteration_state to running using the existing executor
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
	ctxWithTimeout, cancel := context.WithTimeout(systemCtx, time.Minute)
	defer cancel()
	result, err := executor.ExecSQL(ctxWithTimeout, nil, querySQL, false, false)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to execute query: %v", err)
	}
	defer result.Close()

	// Scan the result - expecting columns: cn_uuid, iteration_state, iteration_lsn
	var cnUUIDFromDB sql.NullString
	var iterationState int8
	var iterationLSN uint64

	if !result.Next() {
		if err := result.Err(); err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to read query result: %v", err)
		}
		return moerr.NewInternalErrorf(ctx, "no rows returned for task_id %d", taskID)
	}

	if err := result.Scan(&cnUUIDFromDB, &iterationState, &iterationLSN); err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to scan query result: %v", err)
	}

	// Check if there are more rows (should not happen for a single task_id)
	if result.Next() {
		return moerr.NewInternalErrorf(ctx, "multiple rows returned for task_id %d", taskID)
	}

	// Check if cn_uuid matches
	if !cnUUIDFromDB.Valid {
		return moerr.NewInternalErrorf(ctx, "cn_uuid is null for task_id %d", taskID)
	}
	if cnUUIDFromDB.String != expectedCNUUID {
		return moerr.NewInternalErrorf(ctx, "cn_uuid mismatch: expected %s, got %s", expectedCNUUID, cnUUIDFromDB.String)
	}

	// Check if iteration_lsn matches
	if iterationLSN != expectedIterationLSN {
		return moerr.NewInternalErrorf(ctx, "iteration_lsn mismatch: expected %d, got %d", expectedIterationLSN, iterationLSN)
	}

	// Check if iteration_state is pending
	if iterationState != IterationStateRunning {
		return moerr.NewInternalErrorf(ctx, "iteration_state is not running: expected %d (running), got %d", IterationStateRunning, iterationState)
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
			true, // Use IF NOT EXISTS to handle existing snapshots gracefully
		)
	case SyncLevelDatabase:
		if iterationCtx.SrcInfo.DBName == "" {
			return moerr.NewInternalError(ctx, "db_name is required for database level snapshot")
		}
		createSnapshotSQL = PublicationSQLBuilder.CreateSnapshotForDatabaseSQL(
			snapshotName,
			iterationCtx.SrcInfo.DBName,
			true, // Use IF NOT EXISTS to handle existing snapshots gracefully
		)
	case SyncLevelAccount:
		// Note: CreateSnapshotForAccountSQL currently uses account name, not account ID
		// This is for upstream snapshot creation, which may require account name
		// For now, we use empty string to create snapshot for current account
		createSnapshotSQL = PublicationSQLBuilder.CreateSnapshotForAccountSQL(
			snapshotName,
			"",   // Empty account name means current account
			true, // Use IF NOT EXISTS to handle existing snapshots gracefully
		)
	default:
		return moerr.NewInternalErrorf(ctx, "unsupported sync_level: %s", iterationCtx.SrcInfo.SyncLevel)
	}

	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	// Execute SQL through upstream executor (account ID is handled internally)
	result, err := iterationCtx.UpstreamExecutor.ExecSQL(ctxWithTimeout, nil, createSnapshotSQL, false, true)
	if err != nil {
		// Check if error is due to snapshot already existing
		errMsg := err.Error()
		if strings.Contains(errMsg, "already exists") && strings.Contains(errMsg, "snapshot") {
			// Snapshot already exists, this is acceptable, continue execution
			logutil.Info("ccpr-iteration snapshot already exists, continuing",
				zap.String("snapshot_name", snapshotName),
				zap.Error(err),
			)
			if result != nil {
				result.Close()
			}
		} else {
			// Other errors, return as before
			return moerr.NewInternalErrorf(ctx, "failed to create snapshot: %v", err)
		}
	} else {
		result.Close()
	}
	// Store snapshot name in iteration context
	iterationCtx.CurrentSnapshotName = snapshotName
	ctxWithTimeout2, cancel2 := context.WithTimeout(ctx, time.Minute)
	defer cancel2()
	iterationCtx.CurrentSnapshotTS, err = querySnapshotTS(ctxWithTimeout2, iterationCtx.UpstreamExecutor, snapshotName)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to query current snapshot TS: %v", err)
	}
	if iterationCtx.IterationLSN > 1 {
		prevSnapshotName := GenerateSnapshotName(iterationCtx.TaskID, iterationCtx.IterationLSN-1)
		iterationCtx.PrevSnapshotName = prevSnapshotName
		ctxWithTimeout3, cancel3 := context.WithTimeout(ctx, time.Minute)
		defer cancel3()
		iterationCtx.PrevSnapshotTS, err = querySnapshotTS(ctxWithTimeout3, iterationCtx.UpstreamExecutor, prevSnapshotName)
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to query previous snapshot TS: %v", err)
		}
	}
	return nil
}

func querySnapshotTS(ctx context.Context, upstreamExecutor SQLExecutor, snapshotName string) (types.TS, error) {
	querySnapshotTsSQL := PublicationSQLBuilder.QuerySnapshotTsSQL(snapshotName)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	tsResult, err := upstreamExecutor.ExecSQL(ctxWithTimeout, nil, querySnapshotTsSQL, false, true)
	if err != nil {
		return types.TS{}, moerr.NewInternalErrorf(ctx, "failed to query snapshot TS: %v", err)
	}
	defer tsResult.Close()

	// Scan the TS result
	var tsValue sql.NullInt64
	if !tsResult.Next() {
		if err := tsResult.Err(); err != nil {
			return types.TS{}, moerr.NewInternalErrorf(ctx, "failed to read snapshot TS result: %v", err)
		}
		return types.TS{}, moerr.NewInternalErrorf(ctx, "no rows returned for snapshot %s", snapshotName)
	}

	if err := tsResult.Scan(&tsValue); err != nil {
		return types.TS{}, moerr.NewInternalErrorf(ctx, "failed to scan snapshot TS result: %v", err)
	}

	if !tsValue.Valid {
		return types.TS{}, moerr.NewInternalErrorf(ctx, "snapshot TS is null for snapshot %s", snapshotName)
	}

	// Convert bigint TS to types.TS (logical time is set to 0)
	snapshotTS := types.BuildTS(tsValue.Int64, 0)

	return snapshotTS, nil

}

// WaitForSnapshotFlushed waits for the snapshot to be flushed with fixed interval
// It checks if the snapshot is flushed, and if not, waits and retries
// interval: fixed wait time between retries (default: 1min)
// totalTimeout: total time to wait before giving up (default: 30min)
func WaitForSnapshotFlushed(
	ctx context.Context,
	iterationCtx *IterationContext,
	interval time.Duration,
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
	ctx, cancel := context.WithTimeout(ctx, totalTimeout)
	defer cancel()

	// Set default values if not provided
	if interval <= 0 {
		interval = 1 * time.Minute
	}
	if totalTimeout <= 0 {
		totalTimeout = 30 * time.Minute
	}

	snapshotName := iterationCtx.CurrentSnapshotName
	checkSQL := PublicationSQLBuilder.CheckSnapshotFlushedSQL(snapshotName)

	startTime := time.Now()
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
		ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()
		result, err := iterationCtx.UpstreamExecutor.ExecSQL(ctxWithTimeout, nil, checkSQL, false, true)
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
			zap.Duration("next_interval", interval),
			zap.Duration("elapsed", time.Since(startTime)),
			zap.Duration("remaining", totalTimeout-time.Since(startTime)),
		)

		// Wait before next retry with fixed interval
		select {
		case <-ctx.Done():
			return moerr.NewInternalErrorf(ctx, "context cancelled while waiting for snapshot %s to be flushed", snapshotName)
		case <-time.After(interval):
			// Use fixed interval for all retries
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
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	result, err := iterationCtx.UpstreamExecutor.ExecSQL(ctxWithTimeout, nil, dropSnapshotSQL, false, true)
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

	// Log before getting object list
	logutil.Info("ccpr-iteration getting object list",
		zap.Uint64("task_id", iterationCtx.TaskID),
		zap.Uint64("lsn", iterationCtx.IterationLSN),
		zap.String("db_name", dbName),
		zap.String("table_name", tableName),
		zap.String("current_snapshot", iterationCtx.CurrentSnapshotName),
		zap.String("against_snapshot", againstSnapshotName),
	)

	// Execute SQL through upstream executor and return result directly
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	result, err := iterationCtx.UpstreamExecutor.ExecSQL(ctxWithTimeout, nil, objectListSQL, false, true)
	if err != nil {
		logutil.Error("ccpr-iteration failed to get object list",
			zap.Uint64("task_id", iterationCtx.TaskID),
			zap.Uint64("lsn", iterationCtx.IterationLSN),
			zap.String("db_name", dbName),
			zap.String("table_name", tableName),
			zap.Error(err),
		)
		return nil, moerr.NewInternalErrorf(ctx, "failed to execute object list query: %v", err)
	}

	logutil.Info("ccpr-iteration got object list result",
		zap.Uint64("task_id", iterationCtx.TaskID),
		zap.Uint64("lsn", iterationCtx.IterationLSN),
		zap.String("db_name", dbName),
		zap.String("table_name", tableName),
	)

	return result, nil
}

// updateObjectStatsFlags updates ObjectStats flags according to the requirements:
// - appendable: always false
// - sorted: true if tombstone, or if no fake pk; false if has fake pk
// - cnCreated: always true
func updateObjectStatsFlags(stats *objectio.ObjectStats, isTombstone bool, hasFakePK bool) {
	// Get current level to preserve it
	level := stats.GetLevel()

	// Clear all flags (b0~b4) but preserve level bits (b5~b7)
	statsBytes := stats.Marshal()
	reservedByte := statsBytes[objectio.ObjectStatsLen-1]
	reservedByte = reservedByte & 0xE0 // Keep only level bits (b5~b7), clear flags (b0~b4)

	// Set sorted flag: true for tombstone, or if no fake pk; false if has fake pk
	sorted := isTombstone || !hasFakePK
	if sorted {
		reservedByte |= objectio.ObjectFlag_Sorted
	}

	// Set cnCreated flag: always true
	reservedByte |= objectio.ObjectFlag_CNCreated

	// appendable is false (not set, cleared above)

	// Update the reserved byte
	statsBytes[objectio.ObjectStatsLen-1] = reservedByte
	stats.UnMarshal(statsBytes)

	// Restore level (in case it was affected)
	stats.SetLevel(level)
}

// ExecuteIteration executes a complete iteration according to the design document
// It follows the sequence: initialization -> DDL -> snapshot diff -> object processing -> cleanup -> update system table
// snapshotFlushInterval: interval between retries when waiting for snapshot to be flushed (default: 1min if 0)
// executorRetryOpt: retry options for executor operations (nil to use default)
// sqlExecutorRetryOpt: retry options for SQL executor operations (nil to use default)
func ExecuteIteration(
	ctx context.Context,
	cnUUID string,
	cnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	taskID uint64,
	iterationLSN uint64,
	upstreamSQLHelperFactory UpstreamSQLHelperFactory,
	mp *mpool.MPool,
	utHelper UTHelper,
	snapshotFlushInterval time.Duration,
	sqlExecutorRetryOpts ...*SQLExecutorRetryOption,
) (err error) {
	var iterationCtx *IterationContext
	var sqlExecutorRetryOpt *SQLExecutorRetryOption
	if len(sqlExecutorRetryOpts) > 0 {
		sqlExecutorRetryOpt = sqlExecutorRetryOpts[0]
	}

	// Check if account ID exists in context and is not 0
	if v := ctx.Value(defines.TenantIDKey{}); v != nil {
		if accountID, ok := v.(uint32); ok && accountID != 0 {
			return moerr.NewInternalErrorf(ctx, "account ID must be 0 or not set in context, got %d", accountID)
		}
	}

	if _, ok := ctx.Deadline(); ok {
		return moerr.NewInternalErrorf(ctx, "context deadline must be nil")
	}

	iterationCtx, err = InitializeIterationContext(ctx, cnUUID, cnEngine, cnTxnClient, taskID, iterationLSN, upstreamSQLHelperFactory, sqlExecutorRetryOpt)
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

	needFlushCCPRLog := true
	defer func() {
		injectCommitFailed := false
		var injectMessage string
		if msg, injected := objectio.PublicationSnapshotFinishedInjected(); injected && msg == "ut injection: commit failed" {
			injectCommitFailed = true
			injectMessage = msg
		}
		if msg, injected := objectio.PublicationSnapshotFinishedInjected(); injected && msg == "ut injection: commit failed retryable" {
			injectCommitFailed = true
			injectMessage = msg
		}
		var commitErr error
		if injectCommitFailed {
			iterationCtx.Close(false)
		} else {
			commitErr = iterationCtx.Close(err == nil)
		}
		if injectCommitFailed {
			commitErr = moerr.NewInternalErrorNoCtx(injectMessage)
		}
		if commitErr != nil {
			logutil.Error("ccpr-iteration commit error",
				zap.Error(commitErr),
				zap.String("task_id", iterationCtx.String()),
			)
			if err != nil {
				err = moerr.NewInternalErrorf(ctx, "failed to close iteration context: %v; previous error: %v", commitErr, err)
			} else {
				err = moerr.NewInternalErrorf(ctx, "failed to close iteration context: %v", commitErr)
			}
			if needFlushCCPRLog {
				classifier := NewDownstreamCommitClassifier()
				errorMetadata, retryable := BuildErrorMetadata(iterationCtx.ErrorMetadata, err, classifier)
				finalState := IterationStateError
				subscriptionState := SubscriptionStateRunning
				if retryable {
					finalState = IterationStateCompleted
				} else {
					subscriptionState = SubscriptionStateError
				}
				errorMsg := errorMetadata.Format()
				if err = UpdateIterationState(ctx, iterationCtx.LocalExecutor, taskID, finalState, iterationLSN, iterationCtx, errorMsg, false, subscriptionState); err != nil {
					// Log error but don't override the original error
					err = moerr.NewInternalErrorf(ctx, "failed to update iteration state: %v", err)
					logutil.Error(
						"ccpr-iteration failed to update iteration state",
						zap.Error(err),
						zap.String("task", iterationCtx.String()),
					)
				}
			}
		}
		err = nil
	}()
	// Step 0: Initialization phase
	// 0.1 Check iteration status
	if err = CheckIterationStatus(ctx, iterationCtx.LocalExecutor, taskID, cnUUID, iterationLSN); err != nil {
		return
	}

	// Update iteration state in defer to ensure it's always called
	defer func() {
		// Create a new executor without transaction for checking state before update
		checkRetryOpt := sqlExecutorRetryOpt
		if checkRetryOpt == nil {
			checkRetryOpt = DefaultSQLExecutorRetryOption()
		}
		// Override classifier for check executor
		checkRetryOpt = &SQLExecutorRetryOption{
			MaxRetries:    checkRetryOpt.MaxRetries,
			RetryInterval: checkRetryOpt.RetryInterval,
			Classifier:    NewDownstreamConnectionClassifier(),
		}
		checkExecutor, checkErr := NewInternalSQLExecutor(cnUUID, nil, nil, catalog.System_Account, checkRetryOpt, true)
		if checkErr != nil {
			logutil.Error("ccpr-iteration failed to create check executor",
				zap.Error(checkErr),
				zap.Uint64("task_id", taskID),
			)
			err = moerr.NewInternalErrorf(ctx, "failed to create check executor: %v", checkErr)
			return
		}

		// Check state before update: expecting state=running, iteration_state=running, iteration_lsn=iterationCtx.IterationLSN
		if checkErr = CheckStateBeforeUpdate(ctx, checkExecutor, taskID, iterationCtx.IterationLSN); checkErr != nil {
			logutil.Error("ccpr-iteration state check before update failed",
				zap.Error(checkErr),
				zap.Uint64("task_id", taskID),
				zap.Uint64("iteration_lsn", iterationCtx.IterationLSN),
			)
			err = moerr.NewInternalErrorf(ctx, "state check before update failed: %v", checkErr)
			// Task failure is usually caused by CN UUID or LSN validation errors.
			// The state will be reset by another CN node.
			needFlushCCPRLog = false
		}

		if needFlushCCPRLog {
			var errorMsg string
			finalState := IterationStateCompleted
			nextLSN := iterationLSN + 1
			var updateErr error

			if err != nil {
				// Error case: check if retryable
				classifier := NewDownstreamCommitClassifier()
				errorMetadata, retryable := BuildErrorMetadata(iterationCtx.ErrorMetadata, err, classifier)
				errorMsg = errorMetadata.Format()
				nextLSN = iterationLSN
				if !retryable {
					// Non-retryable error: set state to error
					finalState = IterationStateError
					updateErr = UpdateIterationState(ctx, iterationCtx.LocalExecutor, taskID, finalState, nextLSN, iterationCtx, errorMsg, true, SubscriptionStateError)
				} else {
					// Retryable error: don't change subscription state
					updateErr = UpdateIterationStateNoSubscriptionState(ctx, iterationCtx.LocalExecutor, taskID, finalState, nextLSN, iterationCtx, true, errorMsg)
				}
			} else {
				// Success case: don't set subscription state
				updateErr = UpdateIterationStateNoSubscriptionState(ctx, iterationCtx.LocalExecutor, taskID, finalState, nextLSN, iterationCtx, true, errorMsg)
			}

			if updateErr != nil {
				// Log error but don't override the original error
				err = moerr.NewInternalErrorf(ctx, "failed to update iteration state: %v", updateErr)
			}
		}
	}()

	// 1.1 Request upstream snapshot (includes 1.1.2 request upstream snapshot TS)
	if err = RequestUpstreamSnapshot(ctx, iterationCtx); err != nil {
		err = moerr.NewInternalErrorf(ctx, "failed to request upstream snapshot: %v", err)
		return
	}

	// Injection point: on snapshot finished
	if msg, injected := objectio.PublicationSnapshotFinishedInjected(); injected && msg == "ut injection: publicationSnapshotFinished" {
		err = moerr.NewInternalErrorNoCtx(msg)
		return
	}

	// Defer to drop snapshot if error occurs
	defer func() {
		if err != nil && iterationCtx.CurrentSnapshotName != "" {
			// Drop the snapshot that was created if there's an error
			dropSnapshotSQL := PublicationSQLBuilder.DropSnapshotIfExistsSQL(iterationCtx.CurrentSnapshotName)
			ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute)
			defer cancel()
			if dropResult, dropErr := iterationCtx.UpstreamExecutor.ExecSQL(ctxWithTimeout, nil, dropSnapshotSQL, false, true); dropErr != nil {
				logutil.Warn("ccpr-iteration failed to drop snapshot on error",
					zap.String("snapshot_name", iterationCtx.CurrentSnapshotName),
					zap.Error(dropErr),
				)
			} else {
				dropResult.Close()
			}
		}
	}()

	// Call OnSnapshotCreated callback if utHelper is provided
	if utHelper != nil {
		if err = utHelper.OnSnapshotCreated(ctx, iterationCtx.CurrentSnapshotName, iterationCtx.CurrentSnapshotTS); err != nil {
			err = moerr.NewInternalErrorf(ctx, "failed to call OnSnapshotCreated: %v", err)
			return
		}
	}

	// 1.2 Wait for upstream snapshot to be flushed
	// Use provided interval, or default to 1 minute if not specified
	flushInterval := snapshotFlushInterval
	if flushInterval <= 0 {
		flushInterval = 1 * time.Minute
	}
	if err = WaitForSnapshotFlushed(ctx, iterationCtx, flushInterval, 30*time.Minute); err != nil {
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

	// TODO: Find the table that created the snapshot, get objectlist snapshot, need current snapshot
	if err = ProcessDDLChanges(ctx, cnEngine, iterationCtx); err != nil {
		err = moerr.NewInternalErrorf(ctx, "failed to process DDL changes: %v", err)
		return
	}
	// Step 2: Calculate snapshot diff to get object list

	// Step 3: Get object data
	// Iterate through each object in the object list, call FilterObject interface to process
	// Collect object data for Step 5 submission to TN

	objectMap, err := GetObjectListMap(ctx, iterationCtx, cnEngine)
	if err != nil {
		err = moerr.NewInternalErrorf(ctx, "failed to get object list map: %v", err)
		return
	}
	err = ApplyObjects(
		ctx,
		iterationCtx.String(),
		iterationCtx.SrcInfo.AccountID,
		iterationCtx.IndexTableMappings,
		iterationCtx.ActiveAObj,
		objectMap,
		iterationCtx.UpstreamExecutor,
		iterationCtx.CurrentSnapshotTS,
		iterationCtx.LocalTxn,
		cnEngine,
		mp,
		cnEngine.(*disttae.Engine).FS(),
	)
	if err != nil {
		err = moerr.NewInternalErrorf(ctx, "failed to apply object list: %v", err)
		return
	}
	// Log completion of all object submissions
	logutil.Info("ccpr-iteration finished submitting all objects",
		zap.Uint64("task_id", iterationCtx.TaskID),
		zap.Uint64("lsn", iterationCtx.IterationLSN),
	)

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
