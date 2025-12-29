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
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"go.uber.org/zap"
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
	Operation      int8 // DDLOperationCreate (1), DDLOperationAlter (2), or DDLOperationDrop (3), 0 means no operation
}

// GetUpstreamDDLUsingGetDdl queries upstream DDL using GETDDL statement
// It uses the format: GETDDL [DATABASE dbname] [TABLE tablename] SNAPSHOT <current snapshot>
// Returns a map: map[dbname][table name] *TableDDLInfo{TableID, TableCreateSQL}
func GetUpstreamDDLUsingGetDdl(
	ctx context.Context,
	iterationCtx *IterationContext,
) (map[string]map[string]*TableDDLInfo, error) {
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

	// Parse results into map: map[dbname][table name] *TableDDLInfo
	// GETDDL returns: dbname, tablename, tableid, tablesql
	ddlMap := make(map[string]map[string]*TableDDLInfo)
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

		if isIndexTable(tableNameStr) {
			continue
		}

		// Initialize inner map if needed
		if ddlMap[dbNameStr] == nil {
			ddlMap[dbNameStr] = make(map[string]*TableDDLInfo)
		}

		// Create TableDDLInfo
		ddlInfo := &TableDDLInfo{
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

// ProcessDDLChanges processes DDL changes by:
// 1. Getting upstream DDL map using GetUpstreamDDLUsingGetDdl
// 2. Filling DDL operations using FillDDLOperation
// 3. Executing DDL operations for tables with non-empty operations:
//   - create/drop: execute directly
//   - alter: drop first, then create
//
// 4. Dropping databases returned by FillDDLOperation
func ProcessDDLChanges(
	ctx context.Context,
	cnEngine engine.Engine,
	iterationCtx *IterationContext,
) error {
	if iterationCtx == nil {
		return moerr.NewInternalError(ctx, "iteration context is nil")
	}
	if iterationCtx.LocalExecutor == nil {
		return moerr.NewInternalError(ctx, "local executor is nil")
	}
	if iterationCtx.LocalTxn == nil {
		return moerr.NewInternalError(ctx, "local transaction is nil")
	}

	// Step 1: Get upstream DDL map
	ddlMap, err := GetUpstreamDDLUsingGetDdl(ctx, iterationCtx)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to get upstream DDL: %v", err)
	}

	// Step 2: Fill DDL operations
	// Use downstream account ID from iterationCtx.SrcInfo
	downstreamCtx := context.WithValue(ctx, defines.TenantIDKey{}, iterationCtx.SrcInfo.AccountID)
	dbToDrop, err := FillDDLOperation(
		downstreamCtx,
		cnEngine,
		ddlMap,
		iterationCtx.TableIDs,
		iterationCtx.LocalTxn,
		iterationCtx,
	)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to fill DDL operations: %v", err)
	}

	// Step 3: Execute DDL operations for tables with non-empty operations
	// Use downstream account ID from iterationCtx.SrcInfo
	// Log DDL operations to be executed
	var ddlOperations []string
	for dbName, tables := range ddlMap {
		for tableName, ddlInfo := range tables {
			if ddlInfo.Operation == 0 {
				// Skip tables with no operation needed
				continue
			}

			var operationStr string
			switch ddlInfo.Operation {
			case DDLOperationCreate:
				operationStr = "CREATE"
			case DDLOperationAlter:
				operationStr = "ALTER"
			case DDLOperationDrop:
				operationStr = "DROP"
			default:
				operationStr = fmt.Sprintf("UNKNOWN(%d)", ddlInfo.Operation)
			}
			ddlOperations = append(ddlOperations, fmt.Sprintf("%s TABLE %s.%s", operationStr, dbName, tableName))
		}
	}

	// Log DDL operations with task id and lsn
	if len(ddlOperations) > 0 {
		logutil.Info("ccpr-iteration DDL operations to execute",
			zap.Uint64("task_id", iterationCtx.TaskID),
			zap.Uint64("lsn", iterationCtx.IterationLSN),
			zap.Strings("ddl_operations", ddlOperations),
		)
	}

	for dbName, tables := range ddlMap {
		for tableName, ddlInfo := range tables {
			if ddlInfo.Operation == 0 {
				// Skip tables with no operation needed
				continue
			}

			switch ddlInfo.Operation {
			case DDLOperationCreate:
				// Execute CREATE TABLE
				if ddlInfo.TableCreateSQL != "" {
					if err := createTable(downstreamCtx, iterationCtx.LocalExecutor, dbName, tableName, ddlInfo.TableCreateSQL, iterationCtx, cnEngine); err != nil {
						return moerr.NewInternalErrorf(ctx, "failed to create table %s.%s: %v", dbName, tableName, err)
					}
					// Update TableIDs after successful table creation
					if ddlInfo.TableID > 0 {
						key := TableKey{DBName: dbName, TableName: tableName}
						if iterationCtx.TableIDs == nil {
							iterationCtx.TableIDs = make(map[TableKey]uint64)
						}
						iterationCtx.TableIDs[key] = ddlInfo.TableID
					}
				}
			case DDLOperationDrop:
				// Execute DROP TABLE
				if err := dropTable(downstreamCtx, iterationCtx.LocalExecutor, dbName, tableName, iterationCtx, ddlInfo.TableID); err != nil {
					return moerr.NewInternalErrorf(ctx, "failed to drop table %s.%s: %v", dbName, tableName, err)
				}
			case DDLOperationAlter:
				// For alter, drop first then create
				if err := dropTable(downstreamCtx, iterationCtx.LocalExecutor, dbName, tableName, iterationCtx, ddlInfo.TableID); err != nil {
					return moerr.NewInternalErrorf(ctx, "failed to drop table %s.%s for alter: %v", dbName, tableName, err)
				}
				if ddlInfo.TableCreateSQL != "" {
					if err := createTable(downstreamCtx, iterationCtx.LocalExecutor, dbName, tableName, ddlInfo.TableCreateSQL, iterationCtx, cnEngine); err != nil {
						return moerr.NewInternalErrorf(ctx, "failed to create table %s.%s after alter: %v", dbName, tableName, err)
					}
					// Update TableIDs after successful table creation
					if ddlInfo.TableID > 0 {
						key := TableKey{DBName: dbName, TableName: tableName}
						if iterationCtx.TableIDs == nil {
							iterationCtx.TableIDs = make(map[TableKey]uint64)
						}
						iterationCtx.TableIDs[key] = ddlInfo.TableID
					}
				}
			}
		}
	}

	// Step 4: Drop databases
	for _, dbName := range dbToDrop {
		dropDBSQL := fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", escapeSQLIdentifierForDDL(dbName))
		result, err := iterationCtx.LocalExecutor.ExecSQL(downstreamCtx, dropDBSQL)
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to drop database %s: %v", dbName, err)
		}
		if result != nil {
			result.Close()
		}
	}

	return nil
}

// isIndexTable checks if a table name is an index table
func isIndexTable(name string) bool {
	return strings.HasPrefix(name, catalog.IndexTableNamePrefix)
}

// getCurrentTableCreateSQL gets the current table's create SQL using the same method as CDC
func getCurrentTableCreateSQL(
	ctx context.Context,
	rel engine.Relation,
	dbName string,
	tableName string,
) (string, error) {
	// Get tableDef from relation
	tableDef := rel.CopyTableDef(ctx)
	if tableDef == nil {
		return "", moerr.NewInternalError(ctx, "failed to get table definition")
	}

	// Generate create SQL using the same method as CDC
	newTableDef := *tableDef
	newTableDef.DbName = dbName
	newTableDef.Name = tableName
	newTableDef.Fkeys = nil
	newTableDef.Partition = nil

	if newTableDef.TableType == catalog.SystemClusterRel {
		return "", moerr.NewInternalError(ctx, "cluster table is not supported")
	}
	if newTableDef.TableType == catalog.SystemExternalRel {
		return "", moerr.NewInternalError(ctx, "external table is not supported")
	}

	createSQL, _, err := plan2.ConstructCreateTableSQL(nil, &newTableDef, nil, true, nil)
	if err != nil {
		return "", moerr.NewInternalErrorf(ctx, "failed to construct create table SQL: %v", err)
	}

	return createSQL, nil
}

// FillDDLOperation fills DDL operation type for each table in the ddlMap
// It determines the operation type (DDLOperationCreate, DDLOperationAlter, DDLOperationDrop) based on table existence and ID comparison:
// - If table doesn't exist and table ID doesn't exist in TableIDs: DDLOperationCreate
// - If table doesn't exist but table ID exists in TableIDs: DDLOperationAlter
// - If table exists but table ID changed: DDLOperationDrop (then will be followed by create)
// - If table exists and ID matches: empty string (no operation needed)
// Additionally, it traverses local tables based on iterationCtx.SrcInfo and marks tables that exist locally
// but not in ddlMap as DDLOperationDrop
// The ddlMap is modified in-place with operation types filled
func FillDDLOperation(
	ctx context.Context,
	cnEngine engine.Engine,
	ddlMap map[string]map[string]*TableDDLInfo,
	tableIDs map[TableKey]uint64,
	txn client.TxnOperator,
	iterationCtx *IterationContext,
) (dbToDrop []string, err error) {
	if cnEngine == nil {
		return nil, moerr.NewInternalError(ctx, "engine is nil")
	}
	if ddlMap == nil {
		return nil, moerr.NewInternalError(ctx, "ddlMap is nil")
	}
	dbToDrop = make([]string, 0)

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
			expectedTableID, idExists := tableIDs[key]
			if !idExists {
				return nil, moerr.NewInternalErrorf(ctx, "table %s.%s id not exists", dbName, tableName)
			}

			// Check if table ID changed
			if ddlInfo.TableID != expectedTableID {
				// Table ID changed, need to drop and recreate
				ddlInfo.Operation = DDLOperationAlter
				// Note: After drop, a create operation will be needed separately
			} else if ddlInfo.TableCreateSQL != "" {
				// Table ID matches, check if create SQL changed
				currentCreateSQL, err := getCurrentTableCreateSQL(ctx, rel, dbName, tableName)
				if err != nil {
					return nil, moerr.NewInternalErrorf(ctx, "failed to get current table create SQL for %s.%s: %v", dbName, tableName, err)
				}
				if currentCreateSQL != ddlInfo.TableCreateSQL {
					// Create SQL changed, need to alter
					ddlInfo.Operation = DDLOperationAlter
				}
			}
		}
	}

	// Traverse local tables based on iterationCtx.SrcInfo
	// Find tables that exist locally but not in ddlMap, and mark them as drop
	if iterationCtx != nil {
		dbToDropLocal, err := findMissingTablesInDdlMap(ctx, cnEngine, ddlMap, tableIDs, txn, iterationCtx)
		if err != nil {
			return nil, err
		}
		dbToDrop = append(dbToDrop, dbToDropLocal...)
	}

	return dbToDrop, nil
}

// findMissingTablesInDdlMap traverses local tables based on SrcInfo and marks tables
// that exist locally but not in ddlMap as DDLOperationDrop
func findMissingTablesInDdlMap(
	ctx context.Context,
	cnEngine engine.Engine,
	ddlMap map[string]map[string]*TableDDLInfo,
	tableIDs map[TableKey]uint64,
	txn client.TxnOperator,
	iterationCtx *IterationContext,
) (dbToDrop []string, err error) {
	var dbNames []string

	// Determine which databases to traverse based on SrcInfo
	switch iterationCtx.SrcInfo.SyncLevel {
	case SyncLevelAccount:
		// Traverse all databases
		var err error
		dbNames, err = cnEngine.Databases(ctx, txn)
		if err != nil {
			return nil, moerr.NewInternalErrorf(ctx, "failed to get database names: %v", err)
		}
	case SyncLevelDatabase, SyncLevelTable:
		// Traverse only the specified database
		if iterationCtx.SrcInfo.DBName == "" {
			return nil, moerr.NewInternalError(ctx, "database name is empty")
		}
		dbNames = []string{iterationCtx.SrcInfo.DBName}
	default:
		return nil, moerr.NewInternalError(ctx, "invalid sync level")
	}

	// Traverse each database
	for _, dbName := range dbNames {
		db, err := cnEngine.Database(ctx, dbName, txn)
		if err != nil {
			// Skip databases that don't exist
			continue
		}

		// Get all table names in the database
		tableNames, err := db.Relations(ctx)
		if err != nil {
			return nil, moerr.NewInternalErrorf(ctx, "failed to get table names from database %s: %v", dbName, err)
		}

		// Process each table
		for _, tableName := range tableNames {
			// Filter out index tables
			if isIndexTable(tableName) {
				continue
			}

			// If SyncLevelTable, only process the specified table
			if iterationCtx.SrcInfo.SyncLevel == SyncLevelTable {
				if tableName != iterationCtx.SrcInfo.TableName {
					continue
				}
			}

			// Check if table exists in ddlMap
			if tables, exists := ddlMap[dbName]; exists {
				if _, existsInMap := tables[tableName]; existsInMap {
					// Table exists in ddlMap, skip
					continue
				} else {
					// Initialize inner map if needed
					if ddlMap[dbName] == nil {
						ddlMap[dbName] = make(map[string]*TableDDLInfo)
					}

					// Add table to ddlMap with drop operation
					// for tables to drop, table id may be 0
					ddlMap[dbName][tableName] = &TableDDLInfo{
						Operation: DDLOperationDrop,
					}

				}
			} else {
				dbToDrop = append(dbToDrop, dbName)
			}

		}
	}

	return dbToDrop, nil
}

// createTable creates a table using the provided CREATE TABLE SQL statement
// It also creates the database if it doesn't exist
// For tables created by publication, it adds the "from_publication" property to mark them
// After creating the table, it processes index table mappings
func createTable(
	ctx context.Context,
	executor SQLExecutor,
	dbName string,
	tableName string,
	createSQL string,
	iterationCtx *IterationContext,
	cnEngine engine.Engine,
) error {
	if createSQL == "" {
		return moerr.NewInternalError(ctx, "create SQL is empty")
	}

	// Create database if not exists
	createDBSQL := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", escapeSQLIdentifierForDDL(dbName))
	result, err := executor.ExecSQL(ctx, createDBSQL)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to create database %s: %v", dbName, err)
	}
	if result != nil {
		result.Close()
	}

	// Create table
	// Note: The "from_publication" property is already added in GetUpstreamDDLUsingGetDdl
	// when processing the CREATE SQL from upstream
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	result, err = executor.ExecSQL(ctxWithTimeout, createSQL)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to create table %s.%s: %v", dbName, tableName, err)
	}
	if result != nil {
		result.Close()
	}

	// Process index table mappings after table creation
	if iterationCtx != nil && cnEngine != nil {
		if err := processIndexTableMappings(ctx, iterationCtx, cnEngine, dbName, tableName); err != nil {
			logutil.Warn("ccpr-iteration failed to process index table mappings",
				zap.Uint64("task_id", iterationCtx.TaskID),
				zap.Uint64("lsn", iterationCtx.IterationLSN),
				zap.String("db_name", dbName),
				zap.String("table_name", tableName),
				zap.Error(err),
			)
			// Don't fail the table creation if index mapping fails
		}
	}

	return nil
}

// dropTable drops a table if it exists
// It also removes index table mappings from iterationCtx.IndexTableMappings
func dropTable(
	ctx context.Context,
	executor SQLExecutor,
	dbName string,
	tableName string,
	iterationCtx *IterationContext,
	tableID uint64,
) error {
	// Remove index table mappings before dropping the table
	if iterationCtx != nil && iterationCtx.IndexTableMappings != nil && tableID > 0 {
		if err := removeIndexTableMappings(ctx, iterationCtx, tableID, dbName, tableName); err != nil {
			logutil.Warn("ccpr-iteration failed to remove index table mappings",
				zap.Uint64("task_id", iterationCtx.TaskID),
				zap.Uint64("lsn", iterationCtx.IterationLSN),
				zap.String("db_name", dbName),
				zap.String("table_name", tableName),
				zap.Uint64("table_id", tableID),
				zap.Error(err),
			)
			// Don't fail the table drop if index mapping removal fails
		}
	}

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

// removeIndexTableMappings removes index table mappings from iterationCtx.IndexTableMappings
// It queries upstream index table names and removes them from the map
func removeIndexTableMappings(
	ctx context.Context,
	iterationCtx *IterationContext,
	tableID uint64,
	dbName string,
	tableName string,
) error {
	if iterationCtx == nil || iterationCtx.UpstreamExecutor == nil {
		return nil
	}

	// Query upstream index information
	upstreamIndexMap, err := queryUpstreamIndexInfo(ctx, iterationCtx, tableID)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to query upstream index info: %v", err)
	}

	// Remove mappings for each upstream index table name
	if upstreamIndexMap != nil && iterationCtx.IndexTableMappings != nil {
		for _, upstreamIndexTableName := range upstreamIndexMap {
			if upstreamIndexTableName != "" {
				if downstreamName, exists := iterationCtx.IndexTableMappings[upstreamIndexTableName]; exists {
					// Remove the mapping
					delete(iterationCtx.IndexTableMappings, upstreamIndexTableName)
					// Log the removal
					logutil.Info("ccpr-iteration removed index table mapping",
						zap.Uint64("task_id", iterationCtx.TaskID),
						zap.Uint64("lsn", iterationCtx.IterationLSN),
						zap.String("db_name", dbName),
						zap.String("table_name", tableName),
						zap.Uint64("table_id", tableID),
						zap.String("upstream_index_table_name", upstreamIndexTableName),
						zap.String("downstream_index_table_name", downstreamName),
					)
				}
			}
		}
	}

	return nil
}

// processIndexTableMappings processes index table mappings for a newly created table
// It reads the table definition to find index tables, queries upstream index information,
// and updates IndexTableMappings in iterationCtx
func processIndexTableMappings(
	ctx context.Context,
	iterationCtx *IterationContext,
	cnEngine engine.Engine,
	dbName string,
	tableName string,
) error {
	if iterationCtx == nil || cnEngine == nil {
		return nil
	}

	// Get database and relation to read table definition
	db, err := cnEngine.Database(ctx, dbName, iterationCtx.LocalTxn)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to get database %s: %v", dbName, err)
	}

	rel, err := db.Relation(ctx, tableName, nil)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to get relation %s.%s: %v", dbName, tableName, err)
	}

	// Get table definition
	tableDef := rel.GetTableDef(ctx)
	if tableDef == nil {
		return moerr.NewInternalErrorf(ctx, "failed to get table definition for %s.%s", dbName, tableName)
	}

	// Get table ID from TableIDs map
	tableKey := TableKey{DBName: dbName, TableName: tableName}
	tableID, exists := iterationCtx.TableIDs[tableKey]
	if !exists {
		// Table ID not found, skip index processing
		return nil
	}

	// Initialize IndexTableMappings if nil
	if iterationCtx.IndexTableMappings == nil {
		iterationCtx.IndexTableMappings = make(map[string]string)
	}

	// Process indexes from table definition
	if tableDef.Indexes != nil && len(tableDef.Indexes) > 0 {
		// Query upstream index information
		upstreamIndexMap, err := queryUpstreamIndexInfo(ctx, iterationCtx, tableID)
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to query upstream index info: %v", err)
		}

		// Process each index
		for _, indexDef := range tableDef.Indexes {
			indexName := indexDef.GetIndexName()
			algo := indexDef.GetIndexAlgo()
			algoTableType := indexDef.GetIndexAlgoTableType()
			downstreamIndexTableName := indexDef.GetIndexTableName()

			// Skip if no index table name (regular index without separate table)
			if downstreamIndexTableName == "" {
				continue
			}

			// Find upstream index table name
			upstreamIndexTableName := ""
			if upstreamIndexMap != nil {
				// Key format: indexName + ":" + algoTableType
				key := indexName + ":" + algoTableType
				if upstreamName, ok := upstreamIndexMap[key]; ok {
					upstreamIndexTableName = upstreamName
				}
			}

			// Skip if no upstream index table name found
			if upstreamIndexTableName == "" {
				logutil.Warn("ccpr-iteration upstream index table name not found",
					zap.Uint64("task_id", iterationCtx.TaskID),
					zap.Uint64("lsn", iterationCtx.IterationLSN),
					zap.String("db_name", dbName),
					zap.String("table_name", tableName),
					zap.String("index_name", indexName),
					zap.String("algo", algo),
					zap.String("algo_table_type", algoTableType),
					zap.String("downstream_index_table_name", downstreamIndexTableName),
				)
				continue
			}

			// Store mapping: key is upstream_index_table_name, value is downstream_index_table_name
			iterationCtx.IndexTableMappings[upstreamIndexTableName] = downstreamIndexTableName

			// Log index table mapping update
			logutil.Info("ccpr-iteration updated index table mapping",
				zap.Uint64("task_id", iterationCtx.TaskID),
				zap.Uint64("lsn", iterationCtx.IterationLSN),
				zap.String("db_name", dbName),
				zap.String("table_name", tableName),
				zap.String("index_name", indexName),
				zap.String("algo", algo),
				zap.String("algo_table_type", algoTableType),
				zap.String("upstream_index_table_name", upstreamIndexTableName),
				zap.String("downstream_index_table_name", downstreamIndexTableName),
			)
		}
	}

	return nil
}

// queryUpstreamIndexInfo queries upstream index information from mo_indexes table
// Returns a map: key is "indexName:algoTableType", value is index_table_name
func queryUpstreamIndexInfo(
	ctx context.Context,
	iterationCtx *IterationContext,
	tableID uint64,
) (map[string]string, error) {
	if iterationCtx == nil || iterationCtx.UpstreamExecutor == nil {
		return nil, nil
	}

	// Build SQL to query upstream mo_indexes
	// Format: SELECT name, algo_table_type, index_table_name FROM mo_catalog.mo_indexes WHERE table_id = ?
	querySQL := PublicationSQLBuilder.QueryMoIndexesSQL(0, tableID, "", "")

	// Execute query
	result, err := iterationCtx.UpstreamExecutor.ExecSQL(ctx, querySQL)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to execute query upstream index info: %v", err)
	}
	defer result.Close()

	// Parse results
	// QueryMoIndexesSQL returns: table_id, name, algo_table_type, index_table_name
	indexMap := make(map[string]string)
	for result.Next() {
		var tableID sql.NullInt64
		var indexName, algoTableType, indexTableName sql.NullString

		if err := result.Scan(&tableID, &indexName, &algoTableType, &indexTableName); err != nil {
			return nil, moerr.NewInternalErrorf(ctx, "failed to scan upstream index info: %v", err)
		}

		// Only process valid results
		if !indexName.Valid {
			continue
		}

		// Key format: indexName + ":" + algoTableType
		algoType := ""
		if algoTableType.Valid {
			algoType = algoTableType.String
		}
		key := indexName.String + ":" + algoType

		// Value is index_table_name
		value := ""
		if indexTableName.Valid {
			value = indexTableName.String
		}
		indexMap[key] = value
	}

	if err := result.Err(); err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "error reading upstream index info results: %v", err)
	}

	return indexMap, nil
}

// escapeSQLIdentifierForDDL escapes SQL identifier for use in DDL statements
func escapeSQLIdentifierForDDL(s string) string {
	// Replace backticks with double backticks
	s = strings.ReplaceAll(s, "`", "``")
	return s
}
