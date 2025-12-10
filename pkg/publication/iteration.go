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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// IterationState represents the state of an iteration
const (
	IterationStatePending   int8 = 0 // 'pending'
	IterationStateRunning   int8 = 1 // 'running'
	IterationStateCompleted int8 = 2 // 'complete'
	IterationStateError     int8 = 3 // 'error'
	IterationStateCanceled  int8 = 4 // 'cancel'
)

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

// DatabaseMetadata represents metadata from mo_databases table
type DatabaseMetadata struct {
	DatID        uint64
	DatName      string
	DatCreateSQL sql.NullString
	AccountID    uint32
}

// TableMetadata represents metadata from mo_tables table
type TableMetadata struct {
	RelID         uint64
	RelName       string
	RelDatabaseID uint64
	RelDatabase   string
	RelCreateSQL  sql.NullString
	AccountID     uint32
}

// IsFirstSync checks if this is the first sync based on whether there's a previous snapshot
func IsFirstSync(iterationCtx *IterationContext) bool {
	return iterationCtx.PrevSnapshotName == ""
}

// QueryUpstreamDDL queries upstream three tables (mo_databases, mo_tables, mo_columns) to get DDL statements
// It compares with local metadata and generates DDL statements if there are changes
// Input: internal_sql_executor (for local queries), upstream executor (for upstream queries), iteration context
// Returns: list of DDL statements to execute, updated table IDs and database IDs
func QueryUpstreamDDL(
	ctx context.Context,
	iterationCtx *IterationContext,
	localExecutor SQLExecutor,
) ([]string, error) {
	if iterationCtx == nil {
		return nil, moerr.NewInternalError(ctx, "iteration context is nil")
	}

	if iterationCtx.UpstreamExecutor == nil {
		return nil, moerr.NewInternalError(ctx, "upstream executor is nil")
	}

	if localExecutor == nil {
		return nil, moerr.NewInternalError(ctx, "local executor is nil")
	}

	isFirstSync := IsFirstSync(iterationCtx)
	var ddlStatements []string

	// Initialize TableIDs map if nil
	if iterationCtx.TableIDs == nil {
		iterationCtx.TableIDs = make(map[string]uint64)
	}

	// Query upstream databases
	upstreamDBs, err := queryUpstreamDatabases(ctx, iterationCtx)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to query upstream databases: %v", err)
	}

	// Query upstream tables
	upstreamTables, err := queryUpstreamTables(ctx, iterationCtx)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to query upstream tables: %v", err)
	}

	if isFirstSync {
		// First sync: generate DDL from upstream metadata and record IDs
		for _, db := range upstreamDBs {
			if db.DatCreateSQL.Valid && db.DatCreateSQL.String != "" {
				ddlStatements = append(ddlStatements, db.DatCreateSQL.String)
				// Record database ID
				iterationCtx.TableIDs[fmt.Sprintf("db_%s", db.DatName)] = db.DatID
			}
		}

		for _, tbl := range upstreamTables {
			if tbl.RelCreateSQL.Valid && tbl.RelCreateSQL.String != "" {
				ddlStatements = append(ddlStatements, tbl.RelCreateSQL.String)
				// Record table ID and database ID
				key := fmt.Sprintf("%s.%s", tbl.RelDatabase, tbl.RelName)
				iterationCtx.TableIDs[key] = tbl.RelID
				iterationCtx.TableIDs[fmt.Sprintf("db_%s", tbl.RelDatabase)] = tbl.RelDatabaseID
			}
		}
	} else {
		// Not first sync: compare with local metadata and generate DDL if changed
		localDBs, err := queryLocalDatabases(ctx, localExecutor, iterationCtx)
		if err != nil {
			return nil, moerr.NewInternalErrorf(ctx, "failed to query local databases: %v", err)
		}

		localTables, err := queryLocalTables(ctx, localExecutor, iterationCtx)
		if err != nil {
			return nil, moerr.NewInternalErrorf(ctx, "failed to query local tables: %v", err)
		}

		// Compare databases
		for _, upstreamDB := range upstreamDBs {
			var localDB *DatabaseMetadata
			for _, db := range localDBs {
				if db.DatName == upstreamDB.DatName {
					localDB = &db
					break
				}
			}

			if localDB == nil {
				// Database doesn't exist locally, create it
				if upstreamDB.DatCreateSQL.Valid && upstreamDB.DatCreateSQL.String != "" {
					ddlStatements = append(ddlStatements, upstreamDB.DatCreateSQL.String)
					iterationCtx.TableIDs[fmt.Sprintf("db_%s", upstreamDB.DatName)] = upstreamDB.DatID
				}
			} else {
				// Database exists, check if DDL changed
				if upstreamDB.DatCreateSQL.Valid && localDB.DatCreateSQL.Valid {
					if upstreamDB.DatCreateSQL.String != localDB.DatCreateSQL.String {
						// DDL changed, regenerate
						if upstreamDB.DatCreateSQL.String != "" {
							ddlStatements = append(ddlStatements, upstreamDB.DatCreateSQL.String)
						}
					}
				} else if upstreamDB.DatCreateSQL.Valid && !localDB.DatCreateSQL.Valid {
					// Upstream has DDL but local doesn't
					if upstreamDB.DatCreateSQL.String != "" {
						ddlStatements = append(ddlStatements, upstreamDB.DatCreateSQL.String)
					}
				}

				// Update database ID
				iterationCtx.TableIDs[fmt.Sprintf("db_%s", upstreamDB.DatName)] = upstreamDB.DatID
			}
		}

		// Compare tables
		for _, upstreamTbl := range upstreamTables {
			var localTbl *TableMetadata
			for _, tbl := range localTables {
				if tbl.RelDatabase == upstreamTbl.RelDatabase && tbl.RelName == upstreamTbl.RelName {
					localTbl = &tbl
					break
				}
			}

			if localTbl == nil {
				// Table doesn't exist locally, create it
				if upstreamTbl.RelCreateSQL.Valid && upstreamTbl.RelCreateSQL.String != "" {
					ddlStatements = append(ddlStatements, upstreamTbl.RelCreateSQL.String)
					key := fmt.Sprintf("%s.%s", upstreamTbl.RelDatabase, upstreamTbl.RelName)
					iterationCtx.TableIDs[key] = upstreamTbl.RelID
					iterationCtx.TableIDs[fmt.Sprintf("db_%s", upstreamTbl.RelDatabase)] = upstreamTbl.RelDatabaseID
				}
			} else {
				// Table exists, check if DDL changed or ID mismatch
				if upstreamTbl.RelID != localTbl.RelID {
					return nil, moerr.NewInternalErrorf(ctx, "table ID mismatch for %s.%s: upstream %d, local %d",
						upstreamTbl.RelDatabase, upstreamTbl.RelName, upstreamTbl.RelID, localTbl.RelID)
				}

				if upstreamTbl.RelCreateSQL.Valid && localTbl.RelCreateSQL.Valid {
					if upstreamTbl.RelCreateSQL.String != localTbl.RelCreateSQL.String {
						// DDL changed, regenerate
						if upstreamTbl.RelCreateSQL.String != "" {
							ddlStatements = append(ddlStatements, upstreamTbl.RelCreateSQL.String)
						}
					}
				} else if upstreamTbl.RelCreateSQL.Valid && !localTbl.RelCreateSQL.Valid {
					// Upstream has DDL but local doesn't
					if upstreamTbl.RelCreateSQL.String != "" {
						ddlStatements = append(ddlStatements, upstreamTbl.RelCreateSQL.String)
					}
				}

				// Update table ID
				key := fmt.Sprintf("%s.%s", upstreamTbl.RelDatabase, upstreamTbl.RelName)
				iterationCtx.TableIDs[key] = upstreamTbl.RelID
				iterationCtx.TableIDs[fmt.Sprintf("db_%s", upstreamTbl.RelDatabase)] = upstreamTbl.RelDatabaseID
			}
		}
	}

	return ddlStatements, nil
}

// queryUpstreamDatabases queries mo_databases from upstream
func queryUpstreamDatabases(ctx context.Context, iterationCtx *IterationContext) ([]DatabaseMetadata, error) {
	var accountID uint32
	// Extract account_id from context if available, otherwise use 0
	// TODO: Get account_id from iteration context or connection

	var dbName string
	if iterationCtx.SrcInfo.SyncLevel == SyncLevelDatabase || iterationCtx.SrcInfo.SyncLevel == SyncLevelTable {
		dbName = iterationCtx.SrcInfo.DBName
	}

	querySQL := PublicationSQLBuilder.QueryMoDatabasesSQL(accountID, dbName)
	result, err := iterationCtx.UpstreamExecutor.ExecSQL(ctx, querySQL)
	if err != nil {
		return nil, err
	}
	defer result.Close()

	var databases []DatabaseMetadata
	for result.Next() {
		var db DatabaseMetadata
		var datCreateSQL sql.NullString
		if err := result.Scan(&db.DatID, &db.DatName, &datCreateSQL, &db.AccountID); err != nil {
			return nil, moerr.NewInternalErrorf(ctx, "failed to scan database result: %v", err)
		}
		db.DatCreateSQL = datCreateSQL
		databases = append(databases, db)
	}

	if err := result.Err(); err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "error reading database results: %v", err)
	}

	return databases, nil
}

// queryUpstreamTables queries mo_tables from upstream
func queryUpstreamTables(ctx context.Context, iterationCtx *IterationContext) ([]TableMetadata, error) {
	var accountID uint32
	// Extract account_id from context if available, otherwise use 0
	// TODO: Get account_id from iteration context or connection

	var dbName, tableName string
	if iterationCtx.SrcInfo.SyncLevel == SyncLevelDatabase || iterationCtx.SrcInfo.SyncLevel == SyncLevelTable {
		dbName = iterationCtx.SrcInfo.DBName
	}
	if iterationCtx.SrcInfo.SyncLevel == SyncLevelTable {
		tableName = iterationCtx.SrcInfo.TableName
	}

	querySQL := PublicationSQLBuilder.QueryMoTablesSQL(accountID, dbName, tableName)
	result, err := iterationCtx.UpstreamExecutor.ExecSQL(ctx, querySQL)
	if err != nil {
		return nil, err
	}
	defer result.Close()

	var tables []TableMetadata
	for result.Next() {
		var tbl TableMetadata
		var relCreateSQL sql.NullString
		if err := result.Scan(&tbl.RelID, &tbl.RelName, &tbl.RelDatabaseID, &tbl.RelDatabase, &relCreateSQL, &tbl.AccountID); err != nil {
			return nil, moerr.NewInternalErrorf(ctx, "failed to scan table result: %v", err)
		}
		tbl.RelCreateSQL = relCreateSQL
		tables = append(tables, tbl)
	}

	if err := result.Err(); err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "error reading table results: %v", err)
	}

	return tables, nil
}

// queryLocalDatabases queries mo_databases from local
func queryLocalDatabases(ctx context.Context, executor SQLExecutor, iterationCtx *IterationContext) ([]DatabaseMetadata, error) {
	var accountID uint32
	// Extract account_id from context if available, otherwise use 0
	// TODO: Get account_id from iteration context or connection

	var dbName string
	if iterationCtx.SrcInfo.SyncLevel == SyncLevelDatabase || iterationCtx.SrcInfo.SyncLevel == SyncLevelTable {
		dbName = iterationCtx.SrcInfo.DBName
	}

	querySQL := PublicationSQLBuilder.QueryMoDatabasesSQL(accountID, dbName)
	result, err := executor.ExecSQL(ctx, querySQL)
	if err != nil {
		return nil, err
	}
	defer result.Close()

	var databases []DatabaseMetadata
	for result.Next() {
		var db DatabaseMetadata
		var datCreateSQL sql.NullString
		if err := result.Scan(&db.DatID, &db.DatName, &datCreateSQL, &db.AccountID); err != nil {
			return nil, moerr.NewInternalErrorf(ctx, "failed to scan local database result: %v", err)
		}
		db.DatCreateSQL = datCreateSQL
		databases = append(databases, db)
	}

	if err := result.Err(); err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "error reading local database results: %v", err)
	}

	return databases, nil
}

// queryLocalTables queries mo_tables from local
func queryLocalTables(ctx context.Context, executor SQLExecutor, iterationCtx *IterationContext) ([]TableMetadata, error) {
	var accountID uint32
	// Extract account_id from context if available, otherwise use 0
	// TODO: Get account_id from iteration context or connection

	var dbName, tableName string
	if iterationCtx.SrcInfo.SyncLevel == SyncLevelDatabase || iterationCtx.SrcInfo.SyncLevel == SyncLevelTable {
		dbName = iterationCtx.SrcInfo.DBName
	}
	if iterationCtx.SrcInfo.SyncLevel == SyncLevelTable {
		tableName = iterationCtx.SrcInfo.TableName
	}

	querySQL := PublicationSQLBuilder.QueryMoTablesSQL(accountID, dbName, tableName)
	result, err := executor.ExecSQL(ctx, querySQL)
	if err != nil {
		return nil, err
	}
	defer result.Close()

	var tables []TableMetadata
	for result.Next() {
		var tbl TableMetadata
		var relCreateSQL sql.NullString
		if err := result.Scan(&tbl.RelID, &tbl.RelName, &tbl.RelDatabaseID, &tbl.RelDatabase, &relCreateSQL, &tbl.AccountID); err != nil {
			return nil, moerr.NewInternalErrorf(ctx, "failed to scan local table result: %v", err)
		}
		tbl.RelCreateSQL = relCreateSQL
		tables = append(tables, tbl)
	}

	if err := result.Err(); err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "error reading local table results: %v", err)
	}

	return tables, nil
}
