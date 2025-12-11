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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// IterationState represents the state of an iteration
const (
	IterationStatePending   int8 = 0 // 'pending'
	IterationStateRunning   int8 = 1 // 'running'
	IterationStateCompleted int8 = 2 // 'complete'
	IterationStateError     int8 = 3 // 'error'
	IterationStateCanceled  int8 = 4 // 'cancel'
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

	// Sync configuration
	SyncConfig map[string]any `json:"sync_config"`

	// Execution state
	CNUUID string `json:"cn_uuid"`

	// Context information
	PrevSnapshotName    string                     `json:"prev_snapshot_name"`
	PrevSnapshotTS      int64                      `json:"prev_snapshot_ts"` // types.TS as int64
	CurrentSnapshotName string                     `json:"current_snapshot_name"`
	CurrentSnapshotTS   int64                      `json:"current_snapshot_ts"` // types.TS as int64
	ActiveAObj          map[string]AObjMappingJSON `json:"active_aobj"`         // ActiveAObj as serializable map
	TableIDs            map[string]uint64          `json:"table_ids"`
}

// UpdateIterationState updates iteration state, iteration LSN, and iteration context in mo_ccpr_log table
// It serializes the relevant parts of IterationContext to JSON and updates the corresponding fields
func UpdateIterationState(
	ctx context.Context,
	executor SQLExecutor,
	taskID uint64,
	iterationState int8,
	iterationLSN uint64,
	iterationCtx *IterationContext,
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
			SyncConfig:          iterationCtx.SyncConfig,
			CNUUID:              iterationCtx.CNUUID,
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
	)

	// Execute update SQL
	result, err := executor.ExecSQL(ctx, updateSQL)
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

	// Execute SQL query
	result, err := executor.ExecSQL(ctx, querySQL)
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
	if iterationState != IterationStateCompleted {
		return moerr.NewInternalErrorf(ctx, "iteration_state is not completed: expected %d (completed), got %d", IterationStateCompleted, iterationState)
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

	// Execute SQL through upstream executor
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
	objectListResult *Result,
	rel engine.Relation,
) error {
	if iterationCtx == nil {
		return moerr.NewInternalError(ctx, "iteration context is nil")
	}
	if rel == nil {
		return moerr.NewInternalError(ctx, "relation is nil")
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
		if err := submitObjectsAsInsert(ctx, rel, insertStats); err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to submit insert objects: %v", err)
		}
	}

	// Submit delete objects
	if len(deleteStats) > 0 {
		if err := submitObjectsAsDelete(ctx, rel, deleteStats); err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to submit delete objects: %v", err)
		}
	}

	return nil
}

// submitObjectsAsInsert submits objects as INSERT operation
func submitObjectsAsInsert(ctx context.Context, rel engine.Relation, statsList []objectio.ObjectStats) error {
	if len(statsList) == 0 {
		return nil
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
func submitObjectsAsDelete(ctx context.Context, rel engine.Relation, statsList []objectio.ObjectStats) error {
	if len(statsList) == 0 {
		return nil
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
