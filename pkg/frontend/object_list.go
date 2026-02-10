// Copyright 2025 Matrix Origin
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

package frontend

import (
	"context"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

// ObjectListPermissionChecker is the function to check publication permission for ObjectList
// This is exported as a variable to allow stubbing in tests
// Returns the authorized account ID for execution
var ObjectListPermissionChecker = func(ctx context.Context, ses *Session, pubAccountName, pubName string) (uint64, error) {
	if len(pubAccountName) == 0 || len(pubName) == 0 {
		return 0, moerr.NewInternalError(ctx, "publication account name and publication name are required for OBJECT LIST")
	}
	bh := ses.GetShareTxnBackgroundExec(ctx, false)
	defer bh.Close()
	currentAccount := ses.GetTenantInfo().GetTenant()
	accountID, _, err := getAccountFromPublication(ctx, bh, pubAccountName, pubName, currentAccount)
	return accountID, err
}

// ProcessObjectListFunc is a function variable for ProcessObjectList to allow stubbing in tests
var ProcessObjectListFunc = processObjectListImpl

// ProcessObjectList is the core function that processes OBJECTLIST statement
// It returns a batch with "table name", "db name" columns plus object list columns
// This function can be used by both handleObjectList and test utilities
// If dbname is empty, it will iterate over all databases
// If tablename is empty (but dbname is not), it will iterate over all tables in the database
func ProcessObjectList(
	ctx context.Context,
	stmt *tree.ObjectList,
	eng engine.Engine,
	txnOp client.TxnOperator,
	mp *mpool.MPool,
	resolveSnapshot func(ctx context.Context, snapshotName string) (*timestamp.Timestamp, error),
	getCurrentTS func() types.TS,
	dbname string,
	tablename string,
) (*batch.Batch, error) {
	return ProcessObjectListFunc(ctx, stmt, eng, txnOp, mp, resolveSnapshot, getCurrentTS, dbname, tablename)
}

// processObjectListImpl is the actual implementation of ProcessObjectList
func processObjectListImpl(
	ctx context.Context,
	stmt *tree.ObjectList,
	eng engine.Engine,
	txnOp client.TxnOperator,
	mp *mpool.MPool,
	resolveSnapshot func(ctx context.Context, snapshotName string) (*timestamp.Timestamp, error),
	getCurrentTS func() types.TS,
	dbname string,
	tablename string,
) (*batch.Batch, error) {

	// Parse snapshot timestamps if specified
	var from, to types.TS

	// Parse against snapshot (from timestamp)
	if stmt.AgainstSnapshot != nil && len(string(*stmt.AgainstSnapshot)) > 0 {
		snapshotTS, err := resolveSnapshot(ctx, string(*stmt.AgainstSnapshot))
		if err == nil && snapshotTS != nil {
			from = types.TimestampToTS(*snapshotTS)
		} else {
			// If parsing fails, use empty TS
			from = types.MinTs()
		}
	} else {
		// If against snapshot not specified, use empty TS
		from = types.MinTs()
	}

	// Parse snapshot (to timestamp)
	if len(string(stmt.Snapshot)) > 0 {
		snapshotTS, err := resolveSnapshot(ctx, string(stmt.Snapshot))
		if err == nil && snapshotTS != nil {
			to = types.TimestampToTS(*snapshotTS)
		} else {
			// If parsing fails, use current timestamp
			to = getCurrentTS()
		}
	} else {
		// If snapshot not specified, use current timestamp
		to = getCurrentTS()
	}

	// Get object list batch (batch is created inside GetObjectListWithoutSession)
	objBatch, err := GetObjectListWithoutSession(ctx, from, to, dbname, tablename, eng, txnOp, mp)
	if err != nil {
		return nil, err
	}

	// objBatch already contains dbname and tablename columns, so we can return it directly
	return objBatch, nil
}

func handleObjectList(
	ctx context.Context,
	ses *Session,
	stmt *tree.ObjectList,
) error {
	var (
		mrs      = ses.GetMysqlResultSet()
		showCols []*MysqlColumn
	)

	ses.ClearAllMysqlResultSet()
	ses.ClearResultBatches()

	// Get database name and table name from stmt or session
	dbname := string(stmt.Database)
	if len(dbname) == 0 {
		dbname = ses.GetDatabaseName()
	}
	tablename := string(stmt.Table)

	// Check publication permission using getAccountFromPublication and get account ID
	pubAccountName := stmt.SubscriptionAccountName
	pubName := string(stmt.PubName)
	accountID, err := ObjectListPermissionChecker(ctx, ses, pubAccountName, pubName)
	if err != nil {
		return err
	}

	// Use the authorized account context for execution
	ctx = defines.AttachAccountId(ctx, uint32(accountID))

	// Resolve snapshot using session
	resolveSnapshot := func(ctx context.Context, snapshotName string) (*timestamp.Timestamp, error) {
		snapshot, err := ses.GetTxnCompileCtx().ResolveSnapshotWithSnapshotName(snapshotName)
		if err != nil || snapshot == nil || snapshot.TS == nil {
			return nil, err
		}
		return snapshot.TS, nil
	}

	// Get current timestamp from session
	getCurrentTS := func() types.TS {
		if ses.GetProc() != nil && ses.GetProc().GetTxnOperator() != nil {
			return types.TimestampToTS(ses.GetProc().GetTxnOperator().SnapshotTS())
		}
		return types.MaxTs()
	}

	// Get engine and txn from session
	eng := ses.GetTxnHandler().GetStorage()
	txn := ses.GetTxnHandler().GetTxn()
	mp := ses.GetMemPool()

	// Process object list using core function
	resultBatch, err := ProcessObjectList(ctx, stmt, eng, txn, mp, resolveSnapshot, getCurrentTS, dbname, tablename)
	if err != nil {
		return err
	}
	defer resultBatch.Clean(mp)

	// Build columns from result batch (which already includes dbname and tablename)
	if resultBatch != nil && resultBatch.Attrs != nil {
		for i := 0; i < len(resultBatch.Attrs); i++ {
			attr := resultBatch.Attrs[i]
			col := new(MysqlColumn)
			// Map dbname/tablename to "db name"/"table name" for display
			if attr == "dbname" {
				col.SetName("db name")
			} else if attr == "tablename" {
				col.SetName("table name")
			} else {
				col.SetName(attr)
			}
			// Convert batch column type to MySQL type
			if i < len(resultBatch.Vecs) {
				typ := resultBatch.Vecs[i].GetType()
				err := convertEngineTypeToMysqlType(ctx, typ.Oid, col)
				if err != nil {
					return err
				}
			}
			showCols = append(showCols, col)
		}
	}

	for _, col := range showCols {
		mrs.AddColumn(col)
	}

	// Extract rows from batch and add to MySQLResultSet
	if resultBatch != nil {
		n := resultBatch.RowCount()
		for j := 0; j < n; j++ {
			row := make([]any, len(showCols))
			// Extract all columns from batch
			if err := extractRowFromEveryVector(ctx, ses, resultBatch, j, row, false); err != nil {
				return err
			}
			mrs.AddRow(row)
		}
	}

	// Save query result if needed
	return trySaveQueryResult(ctx, ses, mrs)
}

// getIndexTableNamesFromTableDef gets index table names from table's tableDef
func getIndexTableNamesFromTableDef(
	ctx context.Context,
	table engine.Relation,
) []string {
	if table == nil {
		return nil
	}

	// Get tableDef from table
	tableDef := table.GetTableDef(ctx)
	if tableDef == nil {
		return nil
	}

	// Get index table names from Indexes
	var indexTableNames []string
	seen := make(map[string]bool)
	if tableDef.Indexes != nil {
		for _, indexDef := range tableDef.Indexes {
			if indexDef != nil && len(indexDef.IndexTableName) > 0 {
				indexTableName := indexDef.IndexTableName
				if !seen[indexTableName] {
					indexTableNames = append(indexTableNames, indexTableName)
					seen[indexTableName] = true
				}
			}
		}
	}

	return indexTableNames
}

// collectObjectListForTable collects object list for a specific table in a specific database
func collectObjectListForTable(
	ctx context.Context,
	from, to types.TS,
	dbname, tablename string,
	eng engine.Engine,
	txn client.TxnOperator,
	bat *batch.Batch,
	mp *mpool.MPool,
) error {
	if eng == nil {
		return moerr.NewInternalError(ctx, "engine is nil")
	}
	if txn == nil {
		return moerr.NewInternalError(ctx, "txn is nil")
	}
	if mp == nil {
		return moerr.NewInternalError(ctx, "mpool is nil")
	}
	if bat == nil {
		return moerr.NewInternalError(ctx, "batch is nil")
	}
	if len(dbname) == 0 {
		return moerr.NewInternalError(ctx, "dbname is required")
	}
	if len(tablename) == 0 {
		return moerr.NewInternalError(ctx, "tablename is required")
	}

	// Get database from engine using txn
	db, err := eng.Database(ctx, dbname, txn)
	if err != nil {
		return moerr.NewInternalError(ctx, fmt.Sprintf("failed to get database: %v", err))
	}

	// Get table from database
	table, err := db.Relation(ctx, tablename, nil)
	if err != nil {
		return moerr.NewInternalError(ctx, fmt.Sprintf("failed to get table: %v", err))
	}

	if strings.ToUpper(table.GetTableDef(ctx).TableType) == "V" {
		return nil
	}

	// Call CollectObjectList on table
	err = table.CollectObjectList(ctx, from, to, bat, mp)
	if err != nil {
		return err
	}

	// Get index table names from tableDef
	indexTableNames := getIndexTableNamesFromTableDef(ctx, table)

	// Collect object list for each index table
	for _, indexTableName := range indexTableNames {
		if len(indexTableName) > 0 {
			indexTable, err := db.Relation(ctx, indexTableName, nil)
			if err != nil {
				// Log error but continue with other index tables
				continue
			}
			err = indexTable.CollectObjectList(ctx, from, to, bat, mp)
			if err != nil {
				// Log error but continue with other index tables
				continue
			}
		}
	}

	return nil
}

// collectObjectListForDatabase collects object list for a specific database
// If tablename is empty, it will iterate over all tables in the database
func collectObjectListForDatabase(
	ctx context.Context,
	from, to types.TS,
	dbname, tablename string,
	eng engine.Engine,
	txn client.TxnOperator,
	bat *batch.Batch,
	mp *mpool.MPool,
) error {
	if eng == nil {
		return moerr.NewInternalError(ctx, "engine is nil")
	}
	if txn == nil {
		return moerr.NewInternalError(ctx, "txn is nil")
	}
	if mp == nil {
		return moerr.NewInternalError(ctx, "mpool is nil")
	}
	if bat == nil {
		return moerr.NewInternalError(ctx, "batch is nil")
	}
	if len(dbname) == 0 {
		return moerr.NewInternalError(ctx, "dbname is required")
	}

	// Get database from engine using txn
	db, err := eng.Database(ctx, dbname, txn)
	if err != nil {
		return moerr.NewInternalError(ctx, fmt.Sprintf("failed to get database: %v", err))
	}

	// If tablename is specified, collect for that specific table
	if len(tablename) > 0 {
		return collectObjectListForTable(ctx, from, to, dbname, tablename, eng, txn, bat, mp)
	}

	// Otherwise, iterate over all tables in the database
	tableNames, err := db.Relations(ctx)
	if err != nil {
		return moerr.NewInternalError(ctx, fmt.Sprintf("failed to get relations: %v", err))
	}

	for _, tableName := range tableNames {
		// Filter out index tables
		if strings.HasPrefix(tableName, catalog.IndexTableNamePrefix) {
			continue
		}
		err = collectObjectListForTable(ctx, from, to, dbname, tableName, eng, txn, bat, mp)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetObjectListWithoutSession gets object list from the specified table/database without requiring Session
// This is a version that can be used by test utilities
// If dbname is empty, it will iterate over all databases
// If tablename is empty (but dbname is not), it will iterate over all tables in the database
func GetObjectListWithoutSession(
	ctx context.Context,
	from, to types.TS,
	dbname, tablename string,
	eng engine.Engine,
	txn client.TxnOperator,
	mp *mpool.MPool,
) (*batch.Batch, error) {
	if eng == nil {
		return nil, moerr.NewInternalError(ctx, "engine is nil")
	}
	if txn == nil {
		return nil, moerr.NewInternalError(ctx, "txn is nil")
	}
	if mp == nil {
		return nil, moerr.NewInternalError(ctx, "mpool is nil")
	}

	// Create object list batch
	bat := logtailreplay.CreateObjectListBatch()

	// If dbname is specified, collect for that specific database
	if len(dbname) > 0 {
		err := collectObjectListForDatabase(ctx, from, to, dbname, tablename, eng, txn, bat, mp)
		if err != nil {
			bat.Clean(mp)
			return nil, err
		}
		return bat, nil
	}

	// Otherwise, iterate over all databases
	dbNames, err := eng.Databases(ctx, txn)
	if err != nil {
		bat.Clean(mp)
		return nil, moerr.NewInternalError(ctx, fmt.Sprintf("failed to get databases: %v", err))
	}

	for _, dbName := range dbNames {
		err = collectObjectListForDatabase(ctx, from, to, dbName, tablename, eng, txn, bat, mp)
		if err != nil {
			bat.Clean(mp)
			return nil, err
		}
	}

	return bat, nil
}

// ResolveSnapshotWithSnapshotNameWithoutSession resolves snapshot name to timestamp without requiring Session
// This is a version that can be used by test utilities
func ResolveSnapshotWithSnapshotNameWithoutSession(
	ctx context.Context,
	snapshotName string,
	sqlExecutor executor.SQLExecutor,
	txnOp client.TxnOperator,
) (*timestamp.Timestamp, error) {
	if sqlExecutor == nil {
		return nil, moerr.NewInternalError(ctx, "executor is required for resolving snapshot")
	}

	// Query mo_snapshots table to get snapshot timestamp
	sql := fmt.Sprintf(`select ts from mo_catalog.mo_snapshots where sname = '%s' order by snapshot_id limit 1;`, snapshotName)
	opts := executor.Options{}.WithDisableIncrStatement().WithTxn(txnOp)
	result, err := sqlExecutor.Exec(ctx, sql, opts)
	if err != nil {
		return nil, err
	}
	defer result.Close()

	var snapshotTS int64
	var found bool
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows > 0 && len(cols) > 0 {
			snapshotTS = vector.GetFixedAtWithTypeCheck[int64](cols[0], 0)
			found = true
		}
		return true
	})

	if !found {
		return nil, moerr.NewInternalErrorf(ctx, "snapshot %s does not exist", snapshotName)
	}

	return &timestamp.Timestamp{PhysicalTime: snapshotTS}, nil
}

// handleInternalObjectList handles the internal command objectlist
// It checks permission via publication and returns object list using the snapshot's level to determine scope
func handleInternalObjectList(ses FeSession, execCtx *ExecCtx, ic *InternalCmdObjectList) error {
	ctx := execCtx.reqCtx
	session := ses.(*Session)

	var (
		mrs      = ses.GetMysqlResultSet()
		showCols []*MysqlColumn
	)

	session.ClearAllMysqlResultSet()
	session.ClearResultBatches()

	bh := session.GetShareTxnBackgroundExec(ctx, false)
	defer bh.Close()

	// Get current account name
	currentAccount := ses.GetTenantInfo().GetTenant()

	// Step 1: Check permission via publication and get authorized account
	accountID, _, err := GetAccountIDFromPublication(ctx, bh, ic.subscriptionAccountName, ic.publicationName, currentAccount)
	if err != nil {
		return err
	}

	// Use the authorized account context for execution
	ctx = defines.AttachAccountId(ctx, uint32(accountID))

	// Step 2: Get snapshot covered scope (database name, table name, level)
	scope, _, err := GetSnapshotCoveredScope(ctx, bh, ic.snapshotName)
	if err != nil {
		return err
	}

	// Build tree.ObjectList statement for ProcessObjectList
	stmt := &tree.ObjectList{
		Database: tree.Identifier(scope.DatabaseName),
		Table:    tree.Identifier(scope.TableName),
		Snapshot: tree.Identifier(ic.snapshotName),
	}
	if ic.againstSnapshotName != "" {
		againstName := tree.Identifier(ic.againstSnapshotName)
		stmt.AgainstSnapshot = &againstName
	}

	// Resolve snapshot using session
	resolveSnapshot := func(ctx context.Context, snapshotName string) (*timestamp.Timestamp, error) {
		snapshot, err := session.GetTxnCompileCtx().ResolveSnapshotWithSnapshotName(snapshotName)
		if err != nil || snapshot == nil || snapshot.TS == nil {
			return nil, err
		}
		return snapshot.TS, nil
	}

	// Get current timestamp from session
	getCurrentTS := func() types.TS {
		if session.GetProc() != nil && session.GetProc().GetTxnOperator() != nil {
			return types.TimestampToTS(session.GetProc().GetTxnOperator().SnapshotTS())
		}
		return types.MaxTs()
	}

	// Get engine and txn from session
	eng := session.GetTxnHandler().GetStorage()
	txn := session.GetTxnHandler().GetTxn()
	mp := session.GetMemPool()

	// Step 3: Process object list using core function
	resultBatch, err := ProcessObjectList(ctx, stmt, eng, txn, mp, resolveSnapshot, getCurrentTS, scope.DatabaseName, scope.TableName)
	if err != nil {
		return err
	}
	defer resultBatch.Clean(mp)

	// Step 4: Build columns from result batch
	if resultBatch != nil && resultBatch.Attrs != nil {
		for i := 0; i < len(resultBatch.Attrs); i++ {
			attr := resultBatch.Attrs[i]
			col := new(MysqlColumn)
			if attr == "dbname" {
				col.SetName("db name")
			} else if attr == "tablename" {
				col.SetName("table name")
			} else {
				col.SetName(attr)
			}
			if i < len(resultBatch.Vecs) {
				typ := resultBatch.Vecs[i].GetType()
				err := convertEngineTypeToMysqlType(ctx, typ.Oid, col)
				if err != nil {
					return err
				}
			}
			showCols = append(showCols, col)
		}
	}

	for _, col := range showCols {
		mrs.AddColumn(col)
	}

	// Fill result set from batch
	if resultBatch != nil && resultBatch.RowCount() > 0 {
		n := resultBatch.RowCount()
		for j := 0; j < n; j++ {
			row := make([]any, len(showCols))
			if err := extractRowFromEveryVector(ctx, session, resultBatch, j, row, false); err != nil {
				return err
			}
			mrs.AddRow(row)
		}
	}

	return trySaveQueryResult(ctx, session, mrs)
}
