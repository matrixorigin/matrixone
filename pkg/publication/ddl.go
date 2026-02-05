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
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
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
	TableID         uint64
	TableCreateSQL  string
	Operation       int8     // DDLOperationCreate (1), DDLOperationAlter (2), DDLOperationDrop (3), DDLOperationAlterInplace (4), 0 means no operation
	AlterStatements []string // ALTER TABLE statements for inplace alter operations
}

// GetUpstreamDDLUsingGetDdl queries upstream DDL using internal GETDDL command
// Uses internal command: __++__internal_get_ddl <snapshotName> <subscriptionAccountName> <publicationName>
// The internal command checks publication permission and uses the snapshot's level to determine dbName and tableName scope
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

	// Get snapshot name from iteration context
	snapshotName := iterationCtx.CurrentSnapshotName
	if snapshotName == "" {
		return nil, moerr.NewInternalError(ctx, "current snapshot name is required for GETDDL")
	}

	// Build GETDDL SQL using internal command
	// The internal command uses the provided level, dbName and tableName to determine scope
	querySQL := PublicationSQLBuilder.GetDdlSQL(
		snapshotName,
		iterationCtx.SubscriptionAccountName,
		iterationCtx.SubscriptionName,
		iterationCtx.SrcInfo.SyncLevel,
		iterationCtx.SrcInfo.DBName,
		iterationCtx.SrcInfo.TableName,
	)

	// Execute GETDDL SQL
	result, cancel, err := iterationCtx.UpstreamExecutor.ExecSQL(ctx, nil, querySQL, false, true, time.Minute)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to execute GETDDL: %v", err)
	}
	defer cancel()
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

// getDatabaseDiff returns lists of databases that need to be created and dropped
// It queries upstream databases using iterationCtx.UpstreamExecutor and compares with local databases
// Logic:
//   - If table level: returns empty lists
//   - If db level: checks if the database exists upstream and locally, returns appropriate lists
//   - If account level: queries all databases for the account upstream, compares with local databases,
//     and returns databases that exist upstream but not locally (to create) and databases that exist locally but not upstream (to drop)
//
// Uses GETDATABASES internal command which checks publication permission and uses the authorized account
// to query databases covered by the snapshot
func getDatabaseDiff(
	ctx context.Context,
	iterationCtx *IterationContext,
	cnEngine engine.Engine,
) (dbToCreate []string, dbToDrop []string, err error) {
	if iterationCtx == nil {
		return nil, nil, moerr.NewInternalError(ctx, "iteration context is nil")
	}
	if iterationCtx.UpstreamExecutor == nil {
		return nil, nil, moerr.NewInternalError(ctx, "upstream executor is nil")
	}
	if cnEngine == nil {
		return nil, nil, moerr.NewInternalError(ctx, "engine is nil")
	}
	if iterationCtx.LocalTxn == nil {
		return nil, nil, moerr.NewInternalError(ctx, "local transaction is nil")
	}

	// If table level, return empty lists
	if iterationCtx.SrcInfo.SyncLevel == SyncLevelTable {
		return dbToCreate, dbToDrop, nil
	}

	// Use downstream account ID from iterationCtx.SrcInfo
	downstreamCtx := context.WithValue(ctx, defines.TenantIDKey{}, iterationCtx.SrcInfo.AccountID)

	// Query upstream databases using GETDATABASES command
	// This command checks publication permission and uses the authorized account
	// to query databases covered by the snapshot
	snapshotName := iterationCtx.CurrentSnapshotName
	querySQL := PublicationSQLBuilder.GetDatabasesSQL(
		snapshotName,
		iterationCtx.SubscriptionAccountName,
		iterationCtx.SubscriptionName,
		iterationCtx.SrcInfo.SyncLevel,
		iterationCtx.SrcInfo.DBName,
		iterationCtx.SrcInfo.TableName,
	)
	result, cancel, err := iterationCtx.UpstreamExecutor.ExecSQL(ctx, nil, querySQL, false, true, time.Minute)
	if err != nil {
		return nil, nil, moerr.NewInternalErrorf(ctx, "failed to query upstream databases using GETDATABASES: %v", err)
	}
	defer cancel()
	defer result.Close()

	// Collect upstream database names
	upstreamDBs := make(map[string]bool)
	for result.Next() {
		var datName sql.NullString

		if err := result.Scan(&datName); err != nil {
			return nil, nil, moerr.NewInternalErrorf(ctx, "failed to scan upstream database result: %v", err)
		}

		if datName.Valid {
			upstreamDBs[datName.String] = true
		}
	}
	if err := result.Err(); err != nil {
		return nil, nil, moerr.NewInternalErrorf(ctx, "error reading upstream database query results: %v", err)
	}

	// If db level, check if the database exists upstream and locally
	if iterationCtx.SrcInfo.SyncLevel == SyncLevelDatabase {
		if iterationCtx.SrcInfo.DBName == "" {
			return nil, nil, moerr.NewInternalError(ctx, "database name is empty for database level sync")
		}

		// Check if database exists upstream (in the GETDATABASES result)
		existsUpstream := upstreamDBs[iterationCtx.SrcInfo.DBName]

		// Check if database exists locally
		_, err = cnEngine.Database(downstreamCtx, iterationCtx.SrcInfo.DBName, iterationCtx.LocalTxn)
		existsLocal := err == nil

		// If database exists upstream but not locally, add to create list
		if existsUpstream && !existsLocal {
			dbToCreate = append(dbToCreate, iterationCtx.SrcInfo.DBName)
		}
		// If database exists locally but not upstream, add to drop list
		if !existsUpstream && existsLocal {
			dbToDrop = append(dbToDrop, iterationCtx.SrcInfo.DBName)
		}

		return dbToCreate, dbToDrop, nil
	}

	// If account level, compare upstream databases with local databases
	if iterationCtx.SrcInfo.SyncLevel == SyncLevelAccount {
		// Get local databases
		localDBs, err := cnEngine.Databases(downstreamCtx, iterationCtx.LocalTxn)
		if err != nil {
			return nil, nil, moerr.NewInternalErrorf(ctx, "failed to get local databases: %v", err)
		}

		// Create a map of local databases for efficient lookup
		localDBsMap := make(map[string]bool)
		for _, localDB := range localDBs {
			localDBsMap[localDB] = true
		}

		// Find databases that exist upstream but not locally (to create)
		for upstreamDB := range upstreamDBs {
			if !localDBsMap[upstreamDB] {
				dbToCreate = append(dbToCreate, upstreamDB)
			}
		}

		// Find databases that exist locally but not upstream (to drop)
		for _, localDB := range localDBs {
			if !upstreamDBs[localDB] {
				dbToDrop = append(dbToDrop, localDB)
			}
		}

		return dbToCreate, dbToDrop, nil
	}

	return nil, nil, moerr.NewInternalErrorf(ctx, "unsupported sync level: %s", iterationCtx.SrcInfo.SyncLevel)
}

// ProcessDDLChanges processes DDL changes by:
// 1. Getting database differences (databases to create and drop)
// 2. Creating databases that exist upstream but not locally
// 3. Getting upstream DDL map using GetUpstreamDDLUsingGetDdl
// 4. Filling DDL operations using FillDDLOperation
// 5. Executing DDL operations for tables with non-empty operations:
//   - create/drop: execute directly
//   - alter: drop first, then create
//
// 6. Dropping databases that exist locally but not upstream
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

	// Use downstream account ID from iterationCtx.SrcInfo
	downstreamCtx := context.WithValue(ctx, defines.TenantIDKey{}, iterationCtx.SrcInfo.AccountID)

	// Step 1: Get database differences (databases to create and drop)
	dbToCreate, dbToDrop, err := getDatabaseDiff(ctx, iterationCtx, cnEngine)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to get database differences: %v", err)
	}

	logutil.Info("ccpr-iteration DDL",
		zap.String("task_id", iterationCtx.String()),
		zap.String("operation", "create"),
		zap.String("type", "database"),
		zap.Any("database", dbToCreate),
	)
	// Step 2: Create databases that exist upstream but not locally
	for _, dbName := range dbToCreate {
		// Check if database already exists (for robustness in case of concurrent operations)
		_, err := cnEngine.Database(downstreamCtx, dbName, iterationCtx.LocalTxn)
		if err == nil {
			// Database already exists, skip creation
			logutil.Info("ccpr-iteration database already exists, skipping",
				zap.String("task_id", iterationCtx.TaskID),
				zap.Uint64("lsn", iterationCtx.IterationLSN),
				zap.String("database", dbName),
			)
			continue
		}

		err = cnEngine.Create(downstreamCtx, dbName, iterationCtx.LocalTxn)
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to create database %s: %v", dbName, err)
		}

		// Write database ID and task ID to mo_ccpr_dbs
		db, err := cnEngine.Database(downstreamCtx, dbName, iterationCtx.LocalTxn)
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to get database %s after creation: %v", dbName, err)
		}
		if err := insertCCPRDb(ctx, iterationCtx.LocalExecutor, db.GetDatabaseId(downstreamCtx), iterationCtx.TaskID, dbName); err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to insert ccpr db record for %s: %v", dbName, err)
		}
	}

	// Step 3: Get upstream DDL map
	ddlMap, err := GetUpstreamDDLUsingGetDdl(ctx, iterationCtx)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to get upstream DDL: %v", err)
	}

	// Step 4: Fill DDL operations
	err = FillDDLOperation(
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

	// Step 5: Execute DDL operations for tables with non-empty operations
	// Use downstream account ID from iterationCtx.SrcInfo
	// Collect DDL operation results for logging
	var createOps []string
	var dropOps []string
	var alterOps []string

	// drop/create tables
	// update index table mappings
	// update table ids
	for dbName, tables := range ddlMap {
		for tableName, ddlInfo := range tables {
			if ddlInfo.Operation == 0 {
				// Skip tables with no operation needed
				continue
			}

			switch ddlInfo.Operation {
			case DDLOperationCreate:
				// Execute CREATE TABLE
				if err := createTable(downstreamCtx, iterationCtx.LocalExecutor, dbName, tableName, ddlInfo.TableCreateSQL, iterationCtx, ddlInfo, cnEngine); err != nil {
					return moerr.NewInternalErrorf(ctx, "failed to create table %s.%s: %v", dbName, tableName, err)
				}
				// Get new table ID for logging
				if db, err := cnEngine.Database(downstreamCtx, dbName, iterationCtx.LocalTxn); err == nil {
					if rel, err := db.Relation(downstreamCtx, tableName, nil); err == nil {
						newTableID := rel.GetTableID(downstreamCtx)
						createOps = append(createOps, fmt.Sprintf("%s.%s-%d", dbName, tableName, newTableID))
					}
				}
			case DDLOperationDrop:
				// Get old table ID before drop for logging
				var oldTableID uint64
				if db, err := cnEngine.Database(downstreamCtx, dbName, iterationCtx.LocalTxn); err == nil {
					if rel, err := db.Relation(downstreamCtx, tableName, nil); err == nil {
						oldTableID = rel.GetTableID(downstreamCtx)
					}
				}
				// Execute DROP TABLE
				if err := dropTable(downstreamCtx, iterationCtx.LocalExecutor, dbName, tableName, iterationCtx, ddlInfo.TableID, cnEngine); err != nil {
					return moerr.NewInternalErrorf(ctx, "failed to drop table %s.%s: %v", dbName, tableName, err)
				}
				dropOps = append(dropOps, fmt.Sprintf("%s.%s-%d", dbName, tableName, oldTableID))
			case DDLOperationAlter:
				// Get old table ID before drop for logging
				var oldTableID uint64
				if db, err := cnEngine.Database(downstreamCtx, dbName, iterationCtx.LocalTxn); err == nil {
					if rel, err := db.Relation(downstreamCtx, tableName, nil); err == nil {
						oldTableID = rel.GetTableID(downstreamCtx)
					}
				}
				// For alter, drop first then create
				if err := dropTable(downstreamCtx, iterationCtx.LocalExecutor, dbName, tableName, iterationCtx, ddlInfo.TableID, cnEngine); err != nil {
					return moerr.NewInternalErrorf(ctx, "failed to drop table %s.%s for alter: %v", dbName, tableName, err)
				}
				if err := createTable(downstreamCtx, iterationCtx.LocalExecutor, dbName, tableName, ddlInfo.TableCreateSQL, iterationCtx, ddlInfo, cnEngine); err != nil {
					return moerr.NewInternalErrorf(ctx, "failed to create table %s.%s after alter: %v", dbName, tableName, err)
				}
				// Get new table ID after create for logging
				var newTableID uint64
				if db, err := cnEngine.Database(downstreamCtx, dbName, iterationCtx.LocalTxn); err == nil {
					if rel, err := db.Relation(downstreamCtx, tableName, nil); err == nil {
						newTableID = rel.GetTableID(downstreamCtx)
					}
				}
				alterOps = append(alterOps, fmt.Sprintf("%s.%s-%d->%d", dbName, tableName, oldTableID, newTableID))
			case DDLOperationAlterInplace:
				// Execute ALTER TABLE statements inplace without drop+create
				var tableID uint64
				db, err := cnEngine.Database(downstreamCtx, dbName, iterationCtx.LocalTxn)
				if err != nil {
					return moerr.NewInternalErrorf(ctx, "failed to get database %s.%s: %v", dbName, tableName, err)
				}
				rel, err := db.Relation(downstreamCtx, tableName, nil)
				if err != nil {
					return moerr.NewInternalErrorf(ctx, "failed to get relation %s.%s: %v", dbName, tableName, err)
				}
				tableID = rel.GetTableID(downstreamCtx)
				if err := removeIndexTableMappings(ctx, iterationCtx, tableID, rel); err != nil {
					return moerr.NewInternalErrorf(ctx, "failed to remove index table mappings: %v", err)
				}
				// Execute each ALTER statement
				for _, alterSQL := range ddlInfo.AlterStatements {
					result, cancel, err := iterationCtx.LocalExecutor.ExecSQL(downstreamCtx, nil, alterSQL, true, true, time.Minute)
					if err != nil {
						return moerr.NewInternalErrorf(ctx, "failed to execute alter inplace for %s.%s: %v, SQL: %s", dbName, tableName, err, alterSQL)
					}
					if result != nil {
						result.Close()
					}
					cancel()
				}
				// Process index table mappings after table creation
				if err := processIndexTableMappings(downstreamCtx, iterationCtx, cnEngine, dbName, tableName, ddlInfo.TableID); err != nil {
					return moerr.NewInternalErrorf(ctx, "failed to process index table mappings: %v", err)
				}
				alterOps = append(alterOps, fmt.Sprintf("%s.%s-%d(inplace:%d)", dbName, tableName, tableID, len(ddlInfo.AlterStatements)))
			}
		}
	}

	// Log DDL operations after execution with downstream table IDs
	if len(createOps) > 0 || len(dropOps) > 0 || len(alterOps) > 0 {
		var ddlSummary string
		if len(createOps) > 0 {
			ddlSummary += "create " + strings.Join(createOps, ", ")
		}
		if len(dropOps) > 0 {
			if ddlSummary != "" {
				ddlSummary += "; "
			}
			ddlSummary += "drop " + strings.Join(dropOps, ", ")
		}
		if len(alterOps) > 0 {
			if ddlSummary != "" {
				ddlSummary += "; "
			}
			ddlSummary += "alter " + strings.Join(alterOps, ", ")
		}
		logutil.Info("ccpr-iteration DDL executed",
			zap.String("task_id", iterationCtx.TaskID),
			zap.Uint64("lsn", iterationCtx.IterationLSN),
			zap.String("ddl_summary", ddlSummary),
		)
	}

	logutil.Info("ccpr-iteration DDL",
		zap.String("task_id", iterationCtx.String()),
		zap.String("operation", "drop"),
		zap.String("type", "database"),
		zap.Any("database", dbToDrop),
	)
	// Step 6: Drop databases that exist locally but not upstream
	for _, dbName := range dbToDrop {
		// Get database ID before deletion
		var dbID uint64
		db, dbErr := cnEngine.Database(downstreamCtx, dbName, iterationCtx.LocalTxn)
		if dbErr == nil {
			dbIDStr := db.GetDatabaseId(downstreamCtx)
			dbID, _ = strconv.ParseUint(dbIDStr, 10, 64)
		}

		err := cnEngine.Delete(downstreamCtx, dbName, iterationCtx.LocalTxn)
		if err != nil {
			// Check if error is due to database not existing (similar to IF EXISTS behavior)
			if moerr.IsMoErrCode(err, moerr.ErrBadDB) || moerr.IsMoErrCode(err, moerr.ErrNoDB) {
				logutil.Info("ccpr-iteration database does not exist, skipping",
					zap.String("task_id", iterationCtx.TaskID),
					zap.Uint64("lsn", iterationCtx.IterationLSN),
					zap.String("database", dbName),
				)
				continue
			}
			return moerr.NewInternalErrorf(ctx, "failed to drop database %s: %v", dbName, err)
		}

		// Delete the record from mo_ccpr_dbs if we got the database ID
		if dbID > 0 {
			deleteCcprSql := fmt.Sprintf(
				"DELETE FROM `%s`.`%s` WHERE dbid = %d",
				catalog.MO_CATALOG,
				catalog.MO_CCPR_DBS,
				dbID,
			)
			if _, _, err := iterationCtx.LocalExecutor.ExecSQL(ctx, nil, deleteCcprSql, true, true, time.Minute); err != nil {
				logutil.Warn("CCPR: failed to delete record from mo_ccpr_dbs",
					zap.Uint64("dbID", dbID),
					zap.Error(err))
			}
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
) (err error) {
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
			expectedTableID, idExists := tableIDs[key]
			if !idExists {
				return moerr.NewInternalErrorf(ctx, "table %s.%s id not exists", dbName, tableName)
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
					return moerr.NewInternalErrorf(ctx, "failed to get current table create SQL for %s.%s: %v", dbName, tableName, err)
				}
				if currentCreateSQL != ddlInfo.TableCreateSQL {
					// Create SQL changed, try to use inplace alter if possible
					alterStmts, _, err := compareTableDefsAndGenerateAlterStatements(
						ctx, dbName, tableName, currentCreateSQL, ddlInfo.TableCreateSQL)
					if err != nil {
						// Log the error but fall back to drop+create
						logutil.Warn("ccpr-iteration failed to compare table defs, falling back to drop+create",
							zap.String("db", dbName),
							zap.String("table", tableName),
							zap.Error(err),
						)
						ddlInfo.Operation = DDLOperationAlter
					} else if len(alterStmts) > 0 {
						// Can do inplace alter
						ddlInfo.Operation = DDLOperationAlterInplace
						ddlInfo.AlterStatements = alterStmts
						logutil.Info("ccpr-iteration using inplace alter",
							zap.String("db", dbName),
							zap.String("table", tableName),
							zap.Int("alter_count", len(alterStmts)),
							zap.Strings("statements", alterStmts),
						)
					}
					// If canInplace && len(alterStmts) == 0, no operation needed (DDL normalized to same result)
				}
			}
		}
	}

	// Traverse local tables based on iterationCtx.SrcInfo
	// Find tables that exist locally but not in ddlMap, and mark them as drop
	if iterationCtx != nil {
		err := findMissingTablesInDdlMap(ctx, cnEngine, ddlMap, tableIDs, txn, iterationCtx)
		if err != nil {
			return err
		}
	}

	return nil
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
) (err error) {
	var dbNames []string

	// Determine which databases to traverse based on SrcInfo
	switch iterationCtx.SrcInfo.SyncLevel {
	case SyncLevelAccount:
		// Traverse all databases
		var err error
		dbNames, err = cnEngine.Databases(ctx, txn)
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to get database names: %v", err)
		}
	case SyncLevelDatabase, SyncLevelTable:
		// Traverse only the specified database
		if iterationCtx.SrcInfo.DBName == "" {
			return moerr.NewInternalError(ctx, "database name is empty")
		}
		dbNames = []string{iterationCtx.SrcInfo.DBName}
	default:
		return moerr.NewInternalError(ctx, "invalid sync level")
	}

	// Traverse each database
	for _, dbName := range dbNames {
		db, err := cnEngine.Database(ctx, dbName, txn)
		if err != nil {
			// Skip databases that don't exist
			continue
		}

		var tableNames []string
		if iterationCtx.SrcInfo.SyncLevel == SyncLevelTable {
			tableNames = []string{iterationCtx.SrcInfo.TableName}
		} else {
			tableNames, err = db.Relations(ctx)
			if err != nil {
				return moerr.NewInternalErrorf(ctx, "failed to get table names from database %s: %v", dbName, err)
			}
		}

		// Process each table
		for _, tableName := range tableNames {
			// Filter out index tables
			if isIndexTable(tableName) {
				continue
			}

			// Check if table exists in ddlMap
			if tables, exists := ddlMap[dbName]; exists {
				if _, existsInMap := tables[tableName]; existsInMap {
					// Table exists in ddlMap, skip
					continue
				} else {
					// Add table to ddlMap with drop operation
					// for tables to drop, table id may be 0
					ddlMap[dbName][tableName] = &TableDDLInfo{
						Operation: DDLOperationDrop,
					}

				}
			} else {
				ddlMap[dbName] = make(map[string]*TableDDLInfo)

				// Add table to ddlMap with drop operation
				// for tables to drop, table id may be 0
				ddlMap[dbName][tableName] = &TableDDLInfo{
					Operation: DDLOperationDrop,
				}
			}

		}
	}

	return nil
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
	ddlInfo *TableDDLInfo,
	cnEngine engine.Engine,
) error {
	if createSQL == "" {
		return moerr.NewInternalError(ctx, "create SQL is empty")
	}

	// Create database if not exists
	createDBSQL := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", escapeSQLIdentifierForDDL(dbName))
	result, cancel, err := executor.ExecSQL(ctx, nil, createDBSQL, true, false, time.Minute)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to create database %s: %v", dbName, err)
	}
	if result != nil {
		result.Close()
	}
	cancel()

	// Create table
	// Note: The "from_publication" property is already added in GetUpstreamDDLUsingGetDdl
	// when processing the CREATE SQL from upstream
	result, cancel, err = executor.ExecSQL(ctx, nil, createSQL, true, false, time.Minute)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to create table %s.%s: %v", dbName, tableName, err)
	}
	if result != nil {
		result.Close()
	}
	cancel()
	// Process index table mappings after table creation
	if err := processIndexTableMappings(ctx, iterationCtx, cnEngine, dbName, tableName, ddlInfo.TableID); err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to process index table mappings: %v", err)
	}

	// Get the actual table ID from the created table and write to mo_ccpr_tables
	db, err := cnEngine.Database(ctx, dbName, iterationCtx.LocalTxn)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to get database %s: %v", dbName, err)
	}
	rel, err := db.Relation(ctx, tableName, nil)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to get relation %s.%s: %v", dbName, tableName, err)
	}
	tableID := rel.GetTableID(ctx)
	if err := insertCCPRTable(ctx, executor, tableID, iterationCtx.TaskID, dbName, tableName); err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to insert ccpr table record for %s.%s: %v", dbName, tableName, err)
	}

	// Update TableIDs after successful table creation
	key := TableKey{DBName: dbName, TableName: tableName}
	if iterationCtx.TableIDs == nil {
		iterationCtx.TableIDs = make(map[TableKey]uint64)
	}
	iterationCtx.TableIDs[key] = ddlInfo.TableID

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
	cnEngine engine.Engine,
) error {

	if cnEngine == nil {
		return moerr.NewInternalError(ctx, "engine is nil")
	}
	if iterationCtx == nil || iterationCtx.LocalTxn == nil {
		return moerr.NewInternalError(ctx, "iteration context or transaction is nil")
	}

	// Get database using engine
	db, err := cnEngine.Database(ctx, dbName, iterationCtx.LocalTxn)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to get database %s: %v", dbName, err)
	}

	rel, err := db.Relation(ctx, tableName, nil)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to get relation %s.%s: %v", dbName, tableName, err)
	}

	def := rel.GetTableDef(ctx)

	for _, index := range def.Indexes {
		err = db.Delete(ctx, index.IndexTableName)
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to drop table %s.%s: %v", dbName, index.IndexTableName, err)
		}
	}

	if err := removeIndexTableMappings(ctx, iterationCtx, tableID, rel); err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to remove index table mappings: %v", err)
	}

	// Get the actual table ID before deletion
	actualTableID := rel.GetTableID(ctx)

	// Delete table using database API
	err = db.Delete(ctx, tableName)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to drop table %s.%s: %v", dbName, tableName, err)
	}

	// Delete the record from mo_ccpr_tables
	deleteCcprSql := fmt.Sprintf(
		"DELETE FROM `%s`.`%s` WHERE tableid = %d",
		catalog.MO_CATALOG,
		catalog.MO_CCPR_TABLES,
		actualTableID,
	)
	if _, _, err := executor.ExecSQL(ctx, nil, deleteCcprSql, true, true, time.Minute); err != nil {
		logutil.Warn("CCPR: failed to delete record from mo_ccpr_tables",
			zap.Uint64("tableID", actualTableID),
			zap.Error(err))
	}

	delete(iterationCtx.TableIDs, TableKey{DBName: dbName, TableName: tableName})
	return nil
}

// removeIndexTableMappings removes index table mappings from iterationCtx.IndexTableMappings
// It queries upstream index table names and removes them from the map
func removeIndexTableMappings(
	ctx context.Context,
	iterationCtx *IterationContext,
	tableID uint64,
	rel engine.Relation,
) error {
	def := rel.GetTableDef(ctx)
	indexTableNames := make(map[string]struct{})
	for _, index := range def.Indexes {
		indexTableNames[index.IndexTableName] = struct{}{}
	}
	for upstreamTableName := range iterationCtx.IndexTableMappings {
		if _, exists := indexTableNames[upstreamTableName]; exists {
			// Remove the mapping
			delete(iterationCtx.IndexTableMappings, upstreamTableName)
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
	upstreamTableID uint64,
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

	// Initialize IndexTableMappings if nil
	if iterationCtx.IndexTableMappings == nil {
		iterationCtx.IndexTableMappings = make(map[string]string)
	}

	// Process indexes from table definition
	if len(tableDef.Indexes) > 0 {
		// Query upstream index information
		upstreamIndexMap, err := queryUpstreamIndexInfo(ctx, iterationCtx, upstreamTableID)
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
					zap.String("task_id", iterationCtx.TaskID),
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
			logutil.Info("ccpr-iteration-ddl updated index table mapping",
				zap.String("task_id", iterationCtx.TaskID),
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
// Uses internal command with publication permission check and snapshot support
// Returns a map: key is "indexName:algoTableType", value is index_table_name
func queryUpstreamIndexInfo(
	ctx context.Context,
	iterationCtx *IterationContext,
	tableID uint64,
) (map[string]string, error) {
	if iterationCtx == nil || iterationCtx.UpstreamExecutor == nil {
		return nil, nil
	}

	// Build SQL to query upstream mo_indexes using internal command
	// Format: __++__internal_get_mo_indexes <tableId> <subscriptionAccountName> <publicationName> <snapshotName>
	querySQL := PublicationSQLBuilder.QueryMoIndexesSQL(
		tableID,
		iterationCtx.SubscriptionAccountName,
		iterationCtx.SubscriptionName,
		iterationCtx.CurrentSnapshotName,
	)

	// Execute query
	result, cancel, err := iterationCtx.UpstreamExecutor.ExecSQL(ctx, nil, querySQL, false, true, time.Minute)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to execute query upstream index info: %v", err)
	}
	defer cancel()
	defer result.Close()

	// Parse results
	// QueryMoIndexesSQL returns: table_id, name, algo_table_type, index_table_name
	indexMap := make(map[string]string)
	for result.Next() {
		var tableID uint64
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

// insertCCPRTable inserts a record into mo_ccpr_tables for CCPR transactions
func insertCCPRTable(ctx context.Context, executor SQLExecutor, tableID uint64, taskID string, dbName string, tableName string) error {
	sql := fmt.Sprintf(
		"INSERT INTO `%s`.`%s` (tableid, taskid, dbname, tablename) VALUES (%d, '%s', '%s', '%s')",
		catalog.MO_CATALOG,
		catalog.MO_CCPR_TABLES,
		tableID,
		taskID,
		dbName,
		tableName,
	)
	result, cancel, err := executor.ExecSQL(ctx, nil, sql, true, true, time.Minute)
	if err != nil {
		return err
	}
	if result != nil {
		result.Close()
	}
	cancel()
	return nil
}

// insertCCPRDb inserts a record into mo_ccpr_dbs for CCPR transactions
func insertCCPRDb(ctx context.Context, executor SQLExecutor, dbIDStr string, taskID string, dbName string) error {
	dbID, err := strconv.ParseUint(dbIDStr, 10, 64)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to parse database ID: %v", err)
	}
	sql := fmt.Sprintf(
		"INSERT INTO `%s`.`%s` (dbid, taskid, dbname) VALUES (%d, '%s', '%s')",
		catalog.MO_CATALOG,
		catalog.MO_CCPR_DBS,
		dbID,
		taskID,
		dbName,
	)
	result, cancel, err := executor.ExecSQL(ctx, nil, sql, true, true, time.Minute)
	if err != nil {
		return err
	}
	if result != nil {
		result.Close()
	}
	cancel()
	return nil
}

// parseCreateTableSQL parses CREATE TABLE SQL and returns the AST
func parseCreateTableSQL(ctx context.Context, createSQL string) (*tree.CreateTable, error) {
	stmts, err := parsers.Parse(ctx, dialect.MYSQL, createSQL, 1)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to parse CREATE TABLE SQL: %v", err)
	}
	if len(stmts) == 0 {
		return nil, moerr.NewInternalError(ctx, "no statement parsed from CREATE TABLE SQL")
	}
	createTableStmt, ok := stmts[0].(*tree.CreateTable)
	if !ok {
		return nil, moerr.NewInternalError(ctx, "parsed statement is not CREATE TABLE")
	}
	return createTableStmt, nil
}

// AlterInplaceType represents the type of inplace alter operation
type AlterInplaceType int

const (
	AlterInplaceDropForeignKey AlterInplaceType = iota
	AlterInplaceAddForeignKey
	AlterInplaceDropIndex
	AlterInplaceAddIndex
	AlterInplaceAlterIndexVisibility
	AlterInplaceAlterComment
	AlterInplaceRenameColumn
)

// compareTableDefsAndGenerateAlterStatements compares old and new TableDef and generates ALTER TABLE statements
// for supported inplace operations. Returns the ALTER statements and whether all changes can be done inplace.
// Supported inplace operations:
// - Drop/Add Foreign Key
// - Drop/Add Index (including unique, fulltext, etc.)
// - Alter Index Visibility
// - Alter Table Comment
// - Rename Column
func compareTableDefsAndGenerateAlterStatements(
	ctx context.Context,
	dbName string,
	tableName string,
	oldCreateSQL string,
	newCreateSQL string,
) ([]string, bool, error) {
	// Parse both CREATE TABLE statements
	oldStmt, err := parseCreateTableSQL(ctx, oldCreateSQL)
	if err != nil {
		return nil, false, moerr.NewInternalErrorf(ctx, "failed to parse old CREATE TABLE: %v", err)
	}
	newStmt, err := parseCreateTableSQL(ctx, newCreateSQL)
	if err != nil {
		return nil, false, moerr.NewInternalErrorf(ctx, "failed to parse new CREATE TABLE: %v", err)
	}

	var alterStatements []string
	fullTableName := fmt.Sprintf("`%s`.`%s`", escapeSQLIdentifierForDDL(dbName), escapeSQLIdentifierForDDL(tableName))

	// Build column maps for comparison
	oldCols := buildColumnMap(oldStmt)
	newCols := buildColumnMap(newStmt)

	// Build index maps for comparison
	oldIndexes := buildIndexMap(oldStmt)
	newIndexes := buildIndexMap(newStmt)

	// Build foreign key maps for comparison
	oldFKs := buildForeignKeyMap(oldStmt)
	newFKs := buildForeignKeyMap(newStmt)

	// Check for column changes that cannot be done inplace
	// Only rename is supported inplace
	if !canDoColumnChangesInplace(oldCols, newCols) {
		return nil, false, moerr.NewInternalErrorf(ctx, "column changes cannot be done inplace")
	}

	// Generate column rename statements
	renameStmts := generateColumnRenameStatements(ctx, fullTableName, oldCols, newCols)
	alterStatements = append(alterStatements, renameStmts...)

	// Generate foreign key drop statements
	for fkName := range oldFKs {
		if _, exists := newFKs[fkName]; !exists {
			stmt := fmt.Sprintf("ALTER TABLE %s DROP FOREIGN KEY `%s`", fullTableName, escapeSQLIdentifierForDDL(fkName))
			alterStatements = append(alterStatements, stmt)
		}
	}

	// Generate foreign key add statements
	for fkName, fkDef := range newFKs {
		if _, exists := oldFKs[fkName]; !exists {
			stmt := generateAddForeignKeyStatement(fullTableName, fkName, fkDef)
			alterStatements = append(alterStatements, stmt)
		}
	}

	// Generate index drop statements
	for idxName := range oldIndexes {
		if _, exists := newIndexes[idxName]; !exists {
			stmt := fmt.Sprintf("ALTER TABLE %s DROP INDEX `%s`", fullTableName, escapeSQLIdentifierForDDL(idxName))
			alterStatements = append(alterStatements, stmt)
		}
	}

	// Generate index add statements
	for idxName, idxDef := range newIndexes {
		if _, exists := oldIndexes[idxName]; !exists {
			stmt := generateAddIndexStatement(fullTableName, idxName, idxDef)
			alterStatements = append(alterStatements, stmt)
		}
	}

	// Check for index visibility changes
	for idxName, newIdx := range newIndexes {
		if oldIdx, exists := oldIndexes[idxName]; exists {
			if oldIdx.visible != newIdx.visible {
				var visStr string
				if newIdx.visible {
					visStr = "VISIBLE"
				} else {
					visStr = "INVISIBLE"
				}
				stmt := fmt.Sprintf("ALTER TABLE %s ALTER INDEX `%s` %s", fullTableName, escapeSQLIdentifierForDDL(idxName), visStr)
				alterStatements = append(alterStatements, stmt)
			}
		}
	}

	// Check for table comment change
	oldComment := getTableComment(oldStmt)
	newComment := getTableComment(newStmt)
	if oldComment != newComment {
		stmt := fmt.Sprintf("ALTER TABLE %s COMMENT '%s'", fullTableName, strings.ReplaceAll(newComment, "'", "''"))
		alterStatements = append(alterStatements, stmt)
	}

	return alterStatements, true, nil
}

// columnInfo stores column information for comparison
type columnInfo struct {
	name       string
	typ        string
	defaultVal string
	nullable   bool
	comment    string
	position   int
}

// indexInfo stores index information for comparison
type indexInfo struct {
	name      string
	unique    bool
	columns   []string
	indexType string // BTREE, FULLTEXT, etc.
	visible   bool
}

// foreignKeyInfo stores foreign key information for comparison
type foreignKeyInfo struct {
	name       string
	columns    []string
	refTable   string
	refColumns []string
	onDelete   string
	onUpdate   string
}

// buildColumnMap builds a map of column name to column info from CREATE TABLE statement
func buildColumnMap(stmt *tree.CreateTable) map[string]*columnInfo {
	result := make(map[string]*columnInfo)
	position := 0
	for _, def := range stmt.Defs {
		if colDef, ok := def.(*tree.ColumnTableDef); ok {
			info := &columnInfo{
				name:     string(colDef.Name.ColName()),
				position: position,
				nullable: true,
			}
			if colDef.Type != nil {
				info.typ = formatTypeReference(colDef.Type)
			}
			for _, attr := range colDef.Attributes {
				switch a := attr.(type) {
				case *tree.AttributeNull:
					info.nullable = !a.Is
				case *tree.AttributeDefault:
					if a.Expr != nil {
						info.defaultVal = tree.String(a.Expr, dialect.MYSQL)
					}
				case *tree.AttributeComment:
					info.comment = tree.String(a.CMT, dialect.MYSQL)
				}
			}
			result[strings.ToLower(info.name)] = info
			position++
		}
	}
	return result
}

// formatTypeReference formats a ResolvableTypeReference to string
func formatTypeReference(t tree.ResolvableTypeReference) string {
	if t == nil {
		return ""
	}
	switch nf := t.(type) {
	case tree.NodeFormatter:
		return tree.String(nf, dialect.MYSQL)
	case *tree.T:
		return nf.InternalType.FamilyString
	default:
		return fmt.Sprintf("%T %v", t, t)
	}
}

// buildIndexMap builds a map of index name to index info from CREATE TABLE statement
func buildIndexMap(stmt *tree.CreateTable) map[string]*indexInfo {
	result := make(map[string]*indexInfo)
	for _, def := range stmt.Defs {
		var info *indexInfo
		switch idx := def.(type) {
		case *tree.Index:
			info = &indexInfo{
				name:    string(idx.Name),
				unique:  false,
				visible: true,
			}
			for _, col := range idx.KeyParts {
				if col.ColName != nil {
					info.columns = append(info.columns, string(col.ColName.ColName()))
				}
			}
			if idx.IndexOption != nil {
				info.visible = idx.IndexOption.Visible == tree.VISIBLE_TYPE_VISIBLE
			}
		case *tree.UniqueIndex:
			info = &indexInfo{
				name:    string(idx.Name),
				unique:  true,
				visible: true,
			}
			for _, col := range idx.KeyParts {
				if col.ColName != nil {
					info.columns = append(info.columns, string(col.ColName.ColName()))
				}
			}
			if idx.IndexOption != nil {
				info.visible = idx.IndexOption.Visible == tree.VISIBLE_TYPE_VISIBLE
			}
		case *tree.FullTextIndex:
			info = &indexInfo{
				name:      string(idx.Name),
				unique:    false,
				indexType: "FULLTEXT",
				visible:   true,
			}
			for _, col := range idx.KeyParts {
				if col.ColName != nil {
					info.columns = append(info.columns, string(col.ColName.ColName()))
				}
			}
		}
		if info != nil && info.name != "" {
			result[strings.ToLower(info.name)] = info
		}
	}
	return result
}

// buildForeignKeyMap builds a map of foreign key name to foreign key info from CREATE TABLE statement
func buildForeignKeyMap(stmt *tree.CreateTable) map[string]*foreignKeyInfo {
	result := make(map[string]*foreignKeyInfo)
	for _, def := range stmt.Defs {
		if fk, ok := def.(*tree.ForeignKey); ok {
			info := &foreignKeyInfo{
				name: string(fk.Name),
			}
			for _, col := range fk.KeyParts {
				if col.ColName != nil {
					info.columns = append(info.columns, string(col.ColName.ColName()))
				}
			}
			if fk.Refer != nil {
				info.refTable = formatTableName(fk.Refer.TableName)
				for _, col := range fk.Refer.KeyParts {
					if col.ColName != nil {
						info.refColumns = append(info.refColumns, string(col.ColName.ColName()))
					}
				}
				info.onDelete = fk.Refer.OnDelete.ToString()
				info.onUpdate = fk.Refer.OnUpdate.ToString()
			}
			if info.name != "" {
				result[strings.ToLower(info.name)] = info
			}
		}
	}
	return result
}

// formatTableName formats a TableName to string
func formatTableName(tn *tree.TableName) string {
	if tn == nil {
		return ""
	}
	return tree.String(tn, dialect.MYSQL)
}

// canDoColumnChangesInplace checks if column changes can be done inplace
// Only column rename is supported; adding/dropping/modifying columns requires drop+create
func canDoColumnChangesInplace(oldCols, newCols map[string]*columnInfo) bool {
	// Check if column count is the same
	if len(oldCols) != len(newCols) {
		return false
	}

	// Check if all columns in old table exist in new table (possibly renamed)
	// and their types/defaults/nullable are the same
	for oldName, oldCol := range oldCols {
		// Try to find matching column by position or by name
		found := false
		for newName, newCol := range newCols {
			// Skip if already matched
			if oldCol.position == newCol.position {
				// Same position, check if types match
				if oldCol.typ != newCol.typ ||
					oldCol.defaultVal != newCol.defaultVal ||
					oldCol.nullable != newCol.nullable {
					return false
				}
				found = true
				break
			} else if oldName == newName {
				// Same name but different position - not inplace
				return false
			}
		}
		if !found {
			// Check if column was renamed (same position, different name, same type)
			for _, newCol := range newCols {
				if oldCol.position == newCol.position {
					if oldCol.typ == newCol.typ &&
						oldCol.defaultVal == newCol.defaultVal &&
						oldCol.nullable == newCol.nullable {
						found = true
						break
					}
				}
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// generateColumnRenameStatements generates ALTER TABLE RENAME COLUMN statements
func generateColumnRenameStatements(ctx context.Context, fullTableName string, oldCols, newCols map[string]*columnInfo) []string {
	var stmts []string
	for oldName, oldCol := range oldCols {
		for newName, newCol := range newCols {
			if oldCol.position == newCol.position && oldName != newName {
				// Column was renamed
				stmt := fmt.Sprintf("ALTER TABLE %s RENAME COLUMN `%s` TO `%s`",
					fullTableName,
					escapeSQLIdentifierForDDL(oldCol.name),
					escapeSQLIdentifierForDDL(newCol.name))
				stmts = append(stmts, stmt)
			}
		}
	}
	return stmts
}

// generateAddForeignKeyStatement generates ADD FOREIGN KEY statement
func generateAddForeignKeyStatement(fullTableName string, fkName string, fk *foreignKeyInfo) string {
	cols := make([]string, len(fk.columns))
	for i, col := range fk.columns {
		cols[i] = fmt.Sprintf("`%s`", escapeSQLIdentifierForDDL(col))
	}
	refCols := make([]string, len(fk.refColumns))
	for i, col := range fk.refColumns {
		refCols[i] = fmt.Sprintf("`%s`", escapeSQLIdentifierForDDL(col))
	}

	stmt := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT `%s` FOREIGN KEY (%s) REFERENCES %s (%s)",
		fullTableName,
		escapeSQLIdentifierForDDL(fkName),
		strings.Join(cols, ", "),
		fk.refTable,
		strings.Join(refCols, ", "))

	if fk.onDelete != "" && fk.onDelete != "RESTRICT" {
		stmt += " ON DELETE " + fk.onDelete
	}
	if fk.onUpdate != "" && fk.onUpdate != "RESTRICT" {
		stmt += " ON UPDATE " + fk.onUpdate
	}
	return stmt
}

// generateAddIndexStatement generates ADD INDEX statement
func generateAddIndexStatement(fullTableName string, idxName string, idx *indexInfo) string {
	cols := make([]string, len(idx.columns))
	for i, col := range idx.columns {
		cols[i] = fmt.Sprintf("`%s`", escapeSQLIdentifierForDDL(col))
	}

	var stmt string
	if idx.indexType == "FULLTEXT" {
		stmt = fmt.Sprintf("ALTER TABLE %s ADD FULLTEXT INDEX `%s` (%s)",
			fullTableName,
			escapeSQLIdentifierForDDL(idxName),
			strings.Join(cols, ", "))
	} else if idx.unique {
		stmt = fmt.Sprintf("ALTER TABLE %s ADD UNIQUE INDEX `%s` (%s)",
			fullTableName,
			escapeSQLIdentifierForDDL(idxName),
			strings.Join(cols, ", "))
	} else {
		stmt = fmt.Sprintf("ALTER TABLE %s ADD INDEX `%s` (%s)",
			fullTableName,
			escapeSQLIdentifierForDDL(idxName),
			strings.Join(cols, ", "))
	}
	return stmt
}

// getTableComment extracts table comment from CREATE TABLE statement
func getTableComment(stmt *tree.CreateTable) string {
	for _, opt := range stmt.Options {
		if comment, ok := opt.(*tree.TableOptionComment); ok {
			return comment.Comment
		}
	}
	return ""
}
