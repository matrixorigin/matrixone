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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

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

// TableDDLInfo contains table DDL information
type TableDDLInfo struct {
	TableID        uint64
	TableCreateSQL string
	Operation      string // DDLOperationCreate, DDLOperationAlter, or DDLOperationDrop
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
		iterationCtx.TableIDs = make(map[TableKey]uint64)
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
			}
		}

		for _, tbl := range upstreamTables {
			if tbl.RelCreateSQL.Valid && tbl.RelCreateSQL.String != "" {
				ddlStatements = append(ddlStatements, tbl.RelCreateSQL.String)
				// Record table ID
				iterationCtx.TableIDs[TableKey{DBName: tbl.RelDatabase, TableName: tbl.RelName}] = tbl.RelID
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
					iterationCtx.TableIDs[TableKey{DBName: upstreamTbl.RelDatabase, TableName: upstreamTbl.RelName}] = upstreamTbl.RelID
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
				iterationCtx.TableIDs[TableKey{DBName: upstreamTbl.RelDatabase, TableName: upstreamTbl.RelName}] = upstreamTbl.RelID
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

// GetUpstreamDDLUsingGetDdl queries upstream DDL using GETDDL statement
// It uses the format: GETDDL [DATABASE dbname] [TABLE tablename] SNAPSHOT <current snapshot>
// Returns a map: map[dbname][table name] TableDDLInfo{TableID, TableCreateSQL}
func GetUpstreamDDLUsingGetDdl(
	ctx context.Context,
	iterationCtx *IterationContext,
) (map[string]map[string]TableDDLInfo, error) {
	if iterationCtx == nil {
		return nil, moerr.NewInternalError(ctx, "iteration context is nil")
	}

	if iterationCtx.UpstreamExecutor == nil {
		return nil, moerr.NewInternalError(ctx, "upstream executor is nil")
	}

	// Get database name and table name from sync level
	var dbName, tableName string
	if iterationCtx.SrcInfo.SyncLevel == SyncLevelDatabase || iterationCtx.SrcInfo.SyncLevel == SyncLevelTable {
		dbName = iterationCtx.SrcInfo.DBName
	}
	if iterationCtx.SrcInfo.SyncLevel == SyncLevelTable {
		tableName = iterationCtx.SrcInfo.TableName
	}

	// Get snapshot name from iteration context
	snapshotName := iterationCtx.CurrentSnapshotName
	if snapshotName == "" {
		return nil, moerr.NewInternalError(ctx, "current snapshot name is required for GETDDL")
	}

	// Initialize TableIDs map if nil
	if iterationCtx.TableIDs == nil {
		iterationCtx.TableIDs = make(map[TableKey]uint64)
	}

	// Build GETDDL SQL
	querySQL := PublicationSQLBuilder.GetDdlSQL(dbName, tableName, snapshotName)

	// Execute GETDDL SQL
	result, err := iterationCtx.UpstreamExecutor.ExecSQL(ctx, querySQL)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to execute GETDDL: %v", err)
	}
	defer result.Close()

	// Parse results into map: map[dbname][table name] TableDDLInfo
	// GETDDL returns: dbname, tablename, tableid, tablesql
	ddlMap := make(map[string]map[string]TableDDLInfo)
	for result.Next() {
		var dbNameResult, tableNameResult, tableSQL sql.NullString
		var tableID sql.NullInt64

		if err := result.Scan(&dbNameResult, &tableNameResult, &tableID, &tableSQL); err != nil {
			return nil, moerr.NewInternalErrorf(ctx, "failed to scan GETDDL result: %v", err)
		}

		// Only process valid results
		if !dbNameResult.Valid || !tableNameResult.Valid {
			continue
		}

		dbNameStr := dbNameResult.String
		tableNameStr := tableNameResult.String

		// Initialize inner map if needed
		if ddlMap[dbNameStr] == nil {
			ddlMap[dbNameStr] = make(map[string]TableDDLInfo)
		}

		// Create TableDDLInfo
		ddlInfo := TableDDLInfo{
			TableID:        0,
			TableCreateSQL: "",
		}

		if tableID.Valid {
			ddlInfo.TableID = uint64(tableID.Int64)
			// Record table ID in iteration context
			iterationCtx.TableIDs[TableKey{DBName: dbNameStr, TableName: tableNameStr}] = ddlInfo.TableID
		}

		if tableSQL.Valid {
			ddlInfo.TableCreateSQL = tableSQL.String
		}

		// Store in map
		ddlMap[dbNameStr][tableNameStr] = ddlInfo
	}

	if err := result.Err(); err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "error reading GETDDL results: %v", err)
	}

	return ddlMap, nil
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

// FillDDLOperation fills DDL operation type for each table in the ddlMap
// It determines the operation type (DDLOperationCreate, DDLOperationAlter, DDLOperationDrop) based on table existence and ID comparison:
// - If table doesn't exist and table ID doesn't exist in TableIDs: DDLOperationCreate
// - If table doesn't exist but table ID exists in TableIDs: DDLOperationAlter
// - If table exists but table ID changed: DDLOperationDrop (then will be followed by create)
// - If table exists and ID matches: empty string (no operation needed)
// The ddlMap is modified in-place with operation types filled
func FillDDLOperation(
	ctx context.Context,
	cnEngine engine.Engine,
	ddlMap map[string]map[string]TableDDLInfo,
	tableIDs map[TableKey]uint64,
	txn client.TxnOperator,
) error {
	if cnEngine == nil {
		return moerr.NewInternalError(ctx, "engine is nil")
	}
	if ddlMap == nil {
		return moerr.NewInternalError(ctx, "ddlMap is nil")
	}

	// Process each database and table
	for dbName, tables := range ddlMap {
		for tableName, ddlInfo := range tables {
			// Get database
			db, err := cnEngine.Database(ctx, dbName, txn)
			if err != nil {
				ddlInfo.Operation = DDLOperationCreate
				continue
			}

			// Try to get table
			rel, err := db.Relation(ctx, tableName, nil)
			if err != nil {
				ddlInfo.Operation = DDLOperationCreate
				continue
			}

			key := TableKey{DBName: dbName, TableName: tableName}

			// Table exists, check table ID
			tableID := rel.GetTableID(ctx)
			expectedTableID, idExists := tableIDs[key]
			if !idExists {
				return moerr.NewInternalErrorf(ctx, "table %s.%s id not exists", dbName, tableName)
			}

			// Check if table ID changed
			if tableID != expectedTableID {
				// Table ID changed, need to drop and recreate
				ddlInfo.Operation = DDLOperationAlter
				// Note: After drop, a create operation will be needed separately
			}

			// Update the ddlMap with the modified ddlInfo
			tables[tableName] = ddlInfo
		}
	}

	return nil
}

// createTable creates a table using the provided CREATE TABLE SQL statement
func createTable(
	ctx context.Context,
	executor SQLExecutor,
	dbName string,
	tableName string,
	createSQL string,
) error {
	if createSQL == "" {
		return moerr.NewInternalError(ctx, "create SQL is empty")
	}
	result, err := executor.ExecSQL(ctx, createSQL)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to create table %s.%s: %v", dbName, tableName, err)
	}
	if result != nil {
		result.Close()
	}
	return nil
}

// dropTable drops a table if it exists
func dropTable(
	ctx context.Context,
	executor SQLExecutor,
	dbName string,
	tableName string,
) error {
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`",
		escapeSQLIdentifierForDDL(dbName),
		escapeSQLIdentifierForDDL(tableName))
	result, err := executor.ExecSQL(ctx, dropSQL)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to drop table %s.%s: %v", dbName, tableName, err)
	}
	if result != nil {
		result.Close()
	}
	return nil
}

// escapeSQLIdentifierForDDL escapes SQL identifier for use in DDL statements
func escapeSQLIdentifierForDDL(s string) string {
	// Replace backticks with double backticks
	s = strings.ReplaceAll(s, "`", "``")
	return s
}

// IndexInfo represents minimal index information from mo_indexes table
type IndexInfo struct {
	TableID        uint64
	Name           string
	AlgoTableType  sql.NullString
	IndexTableName sql.NullString
}

// QueryIndexTableMapping queries index table mapping between downstream and upstream
// It queries local mo_indexes to get index information, then queries upstream mo_indexes
// to get corresponding upstream index table names, and stores the mapping in iterationCtx
func QueryIndexTableMapping(
	ctx context.Context,
	upstreamTableID uint64,
	downstreamTableID uint64,
	iterationCtx *IterationContext,
) error {
	if iterationCtx == nil {
		return moerr.NewInternalError(ctx, "iteration context is nil")
	}

	if iterationCtx.LocalExecutor == nil {
		return moerr.NewInternalError(ctx, "local executor is nil")
	}

	if iterationCtx.UpstreamExecutor == nil {
		return moerr.NewInternalError(ctx, "upstream executor is nil")
	}

	// Initialize IndexTableMappings map if nil
	if iterationCtx.IndexTableMappings == nil {
		iterationCtx.IndexTableMappings = make(map[IndexKey]IndexTableMapping)
	}

	// Step 1: Query local (downstream) mo_indexes to get index information
	var accountID uint32 // TODO: Get account_id from iteration context or connection
	querySQL := PublicationSQLBuilder.QueryMoIndexesSQL(accountID, downstreamTableID, "", "")
	result, err := iterationCtx.LocalExecutor.ExecSQL(ctx, querySQL)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to query local mo_indexes: %v", err)
	}
	defer result.Close()

	var localIndexes []IndexInfo
	for result.Next() {
		var idx IndexInfo
		var algoTableType, indexTableName sql.NullString

		if err := result.Scan(
			&idx.TableID,
			&idx.Name,
			&algoTableType,
			&indexTableName,
		); err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to scan local index result: %v", err)
		}

		idx.AlgoTableType = algoTableType
		idx.IndexTableName = indexTableName

		// Only process indexes that have index_table_name (skip NULL)
		if indexTableName.Valid && indexTableName.String != "" {
			localIndexes = append(localIndexes, idx)
		}
	}

	if err := result.Err(); err != nil {
		return moerr.NewInternalErrorf(ctx, "error reading local index results: %v", err)
	}

	// Step 2: Deduplicate local indexes by table_id + index_name + algo_table_type
	// Since the same index may have multiple rows (one per column), we only need to process each unique index once
	indexMap := make(map[string]IndexInfo) // key: table_id:index_name:algo_table_type
	for _, localIdx := range localIndexes {
		algoTableTypeStr := ""
		if localIdx.AlgoTableType.Valid {
			algoTableTypeStr = localIdx.AlgoTableType.String
		}
		key := fmt.Sprintf("%s:%s", localIdx.Name, algoTableTypeStr)
		if _, exists := indexMap[key]; !exists {
			indexMap[key] = localIdx
		}
	}

	// Step 3: For each unique local index, query upstream to get corresponding upstream index table name
	for _, localIdx := range indexMap {
		// Get algo_table_type, default to empty string if NULL
		algoTableTypeStr := ""
		if localIdx.AlgoTableType.Valid {
			algoTableTypeStr = localIdx.AlgoTableType.String
		}

		// Query upstream mo_indexes using index name, algo_table_type, and upstream table_id
		upstreamQuerySQL := PublicationSQLBuilder.QueryMoIndexesSQL(
			accountID,
			upstreamTableID,
			localIdx.Name,
			algoTableTypeStr,
		)
		upstreamResult, err := iterationCtx.UpstreamExecutor.ExecSQL(ctx, upstreamQuerySQL)
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to query upstream mo_indexes for index %s: %v", localIdx.Name, err)
		}

		// Find matching upstream index
		var upstreamIndexTableName string
		found := false
		for upstreamResult.Next() {
			var upstreamIdx IndexInfo
			var upstreamAlgoTableType, upstreamIndexTableNameNull sql.NullString

			if err := upstreamResult.Scan(
				&upstreamIdx.TableID,
				&upstreamIdx.Name,
				&upstreamAlgoTableType,
				&upstreamIndexTableNameNull,
			); err != nil {
				upstreamResult.Close()
				return moerr.NewInternalErrorf(ctx, "failed to scan upstream index result: %v", err)
			}

			// Check if algo_table_type matches
			upstreamAlgoTableTypeStr := ""
			if upstreamAlgoTableType.Valid {
				upstreamAlgoTableTypeStr = upstreamAlgoTableType.String
			}

			if upstreamAlgoTableTypeStr == algoTableTypeStr {
				// Found matching upstream index
				if upstreamIndexTableNameNull.Valid && upstreamIndexTableNameNull.String != "" {
					upstreamIndexTableName = upstreamIndexTableNameNull.String
					found = true
					break
				}
			}
		}

		upstreamResult.Close()

		if err := upstreamResult.Err(); err != nil {
			return moerr.NewInternalErrorf(ctx, "error reading upstream index results: %v", err)
		}

		// Step 4: Store the mapping in iterationCtx
		if found {
			downstreamIndexTableName := ""
			if localIdx.IndexTableName.Valid {
				downstreamIndexTableName = localIdx.IndexTableName.String
			}

			key := IndexKey{
				TableID:       localIdx.TableID,
				IndexName:     localIdx.Name,
				AlgoTableType: algoTableTypeStr,
			}

			mapping := IndexTableMapping{
				IndexName:                localIdx.Name,
				AlgoTableType:            algoTableTypeStr,
				DownstreamIndexTableName: downstreamIndexTableName,
				UpstreamIndexTableName:   upstreamIndexTableName,
			}

			iterationCtx.IndexTableMappings[key] = mapping
		}
	}

	return nil
}
