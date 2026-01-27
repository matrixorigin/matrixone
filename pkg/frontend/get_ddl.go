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
	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// PublicationPermissionChecker is a function type for checking publication permission
// This type allows dependency injection for testing
type PublicationPermissionChecker func(ctx context.Context, ses *Session, databaseName, tableName string) error

// SnapshotResolver is a function type for resolving snapshot by name
// This type allows dependency injection for testing
type SnapshotResolver func(ses *Session, snapshotName string) (*plan2.Snapshot, error)

// defaultSnapshotResolver is the default implementation of SnapshotResolver
func defaultSnapshotResolver(ses *Session, snapshotName string) (*plan2.Snapshot, error) {
	return ses.GetTxnCompileCtx().ResolveSnapshotWithSnapshotName(snapshotName)
}

func handleGetDdl(
	ctx context.Context,
	ses *Session,
	stmt *tree.GetDdl,
) error {
	return handleGetDdlWithChecker(ctx, ses, stmt, checkPublicationPermission, defaultSnapshotResolver)
}

// handleGetDdlWithChecker is the internal implementation that accepts a permission checker
// This allows dependency injection for testing
func handleGetDdlWithChecker(
	ctx context.Context,
	ses *Session,
	stmt *tree.GetDdl,
	permChecker PublicationPermissionChecker,
	snapshotResolver SnapshotResolver,
) error {
	var (
		mrs      = ses.GetMysqlResultSet()
		showCols []*MysqlColumn
	)

	ses.ClearAllMysqlResultSet()
	ses.ClearResultBatches()

	// Create columns: dbname, tablename, tableid, tablesql
	col1 := new(MysqlColumn)
	col1.SetName("dbname")
	col1.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col1.SetCharset(charsetVarchar)
	col1.SetLength(255) // reasonable default length
	showCols = append(showCols, col1)

	col2 := new(MysqlColumn)
	col2.SetName("tablename")
	col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col2.SetCharset(charsetVarchar)
	col2.SetLength(255) // reasonable default length
	showCols = append(showCols, col2)

	col3 := new(MysqlColumn)
	col3.SetName("tableid")
	col3.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	col3.SetSigned(true)           // tableid is signed integer
	col3.SetCharset(charsetBinary) // integer types use binary charset
	showCols = append(showCols, col3)

	col4 := new(MysqlColumn)
	col4.SetName("tablesql")
	col4.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col4.SetCharset(charsetVarchar)
	col4.SetLength(5000) // SQL can be long
	showCols = append(showCols, col4)

	for _, col := range showCols {
		mrs.AddColumn(col)
	}

	// Get database name and table name from statement
	var databaseName string
	var tableName string
	if stmt.Database != nil {
		databaseName = string(*stmt.Database)
	}
	if stmt.Table != nil {
		tableName = string(*stmt.Table)
	}

	// Check publication permission
	if err := permChecker(ctx, ses, databaseName, tableName); err != nil {
		return err
	}

	// Get engine, mpool, and txn from session
	eng := ses.GetTxnHandler().GetStorage()
	if eng == nil {
		return moerr.NewInternalError(ctx, "engine is nil")
	}
	mp := ses.GetMemPool()
	if mp == nil {
		return moerr.NewInternalError(ctx, "mpool is nil")
	}

	// Get or create txn
	// Try to get txn from TxnHandler first
	txn := ses.GetTxnHandler().GetTxn()
	// If no txn from TxnHandler, try to get from proc
	if txn == nil && ses.GetProc() != nil {
		txn = ses.GetProc().GetTxnOperator()
	}
	if txn == nil {
		// If no txn exists, we need to create one
		// For read-only operations, we can use a snapshot read txn
		// But creating a new txn requires ExecCtx which we don't have here
		// So we return an error for now
		return moerr.NewInternalError(ctx, "transaction is required for GETDDL")
	}

	// Resolve snapshot if provided
	var snapshot *plan2.Snapshot
	if stmt.Snapshot != nil {
		snapshotName := string(*stmt.Snapshot)
		var err error
		snapshot, err = snapshotResolver(ses, snapshotName)
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to resolve snapshot %s: %v", snapshotName, err)
		}
		if snapshot != nil && snapshot.TS != nil {
			// Clone txn with snapshot timestamp
			txn = txn.CloneSnapshotOp(*snapshot.TS)
		}
	}

	// Call getddlbatch to get the batch with DDL information
	resultBatch, err := getddlbatch(ctx, databaseName, tableName, eng, mp, txn)
	if err != nil {
		return err
	}
	defer resultBatch.Clean(mp)

	// Fill MySQL result set from batch
	err = fillResultSet(ctx, resultBatch, ses, mrs)
	if err != nil {
		return err
	}

	// Save query result if needed
	return trySaveQueryResult(ctx, ses, mrs)
}

// visitTableDdl fills the batch with table DDL information
// The batch should have 4 columns: dbname, tablename, tableid, tablesql
// Only one row will be filled
func visitTableDdl(
	ctx context.Context,
	databaseName string,
	tableName string,
	batch *batch.Batch,
	txn TxnOperator,
	eng engine.Engine,
	mp *mpool.MPool,
) error {
	if batch == nil {
		return moerr.NewInternalError(ctx, "batch is nil")
	}
	if len(batch.Vecs) < 4 {
		return moerr.NewInternalError(ctx, "batch should have at least 4 columns")
	}
	if mp == nil {
		return moerr.NewInternalError(ctx, "mpool is nil")
	}
	if eng == nil {
		return moerr.NewInternalError(ctx, "engine is nil")
	}
	if txn == nil {
		return moerr.NewInternalError(ctx, "txn is nil")
	}

	// Get database from engine using txn
	db, err := eng.Database(ctx, databaseName, txn)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to get database: %v", err)
	}

	// Get table from database
	table, err := db.Relation(ctx, tableName, nil)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to get table: %v", err)
	}

	if strings.ToUpper(table.GetTableDef(ctx).TableType) == "V" {
		return nil
	}

	// Get tableDef from relation
	tableDef := table.CopyTableDef(ctx)
	if tableDef == nil {
		return moerr.NewInternalError(ctx, "failed to get table definition")
	}

	// Get table ID
	tableID := table.GetTableID(ctx)

	// Generate create SQL using the same method as CDC
	newTableDef := *tableDef
	newTableDef.DbName = databaseName
	newTableDef.Name = tableName
	newTableDef.Fkeys = nil
	newTableDef.Partition = nil

	// Check if newTableDef already has this property
	propertyExists := false
	var propertiesDef *plan2.TableDef_DefType_Properties
	for _, def := range newTableDef.Defs {
		if proDef, ok := def.Def.(*plan2.TableDef_DefType_Properties); ok {
			propertiesDef = proDef
			for _, kv := range proDef.Properties.Properties {
				if kv.Key == catalog.PropFromPublication {
					propertyExists = true
					break
				}
			}
			if propertyExists {
				break
			}
		}
	}

	// Add property if it doesn't exist in newTableDef
	if !propertyExists {
		if propertiesDef == nil {
			// Create new PropertiesDef
			propertiesDef = &plan2.TableDef_DefType_Properties{
				Properties: &plan2.PropertiesDef{
					Properties: []*plan2.Property{},
				},
			}
			newTableDef.Defs = append(newTableDef.Defs, &plan2.TableDefType{
				Def: propertiesDef,
			})
		}
		propertiesDef.Properties.Properties = append(propertiesDef.Properties.Properties, &plan2.Property{
			Key:   catalog.PropFromPublication,
			Value: "true",
		})
	}

	if newTableDef.TableType == catalog.SystemClusterRel {
		return moerr.NewInternalError(ctx, "cluster table is not supported")
	}
	if newTableDef.TableType == catalog.SystemExternalRel {
		return moerr.NewInternalError(ctx, "external table is not supported")
	}

	createSql, _, err := plan2.ConstructCreateTableSQL(nil, &newTableDef, nil, true, nil)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to construct create table SQL: %v", err)
	}

	// Fill batch with data
	// Column 0: dbname (varchar)
	err = vector.AppendBytes(batch.Vecs[0], []byte(databaseName), false, mp)
	if err != nil {
		return err
	}

	// Column 1: tablename (varchar)
	err = vector.AppendBytes(batch.Vecs[1], []byte(tableName), false, mp)
	if err != nil {
		return err
	}

	// Column 2: tableid (int64)
	err = vector.AppendFixed[int64](batch.Vecs[2], int64(tableID), false, mp)
	if err != nil {
		return err
	}

	// Column 3: tablesql (varchar)
	err = vector.AppendBytes(batch.Vecs[3], []byte(createSql), false, mp)
	if err != nil {
		return err
	}

	// Set row count
	batch.SetRowCount(batch.Vecs[0].Length())

	return nil
}

// visitDatabaseDdl fills the batch with table DDL information for tables in the database
// If tableName is empty, it will iterate through all tables in the database
// If tableName is provided, it will only process that specific table
// The batch should have 4 columns: dbname, tablename, tableid, tablesql
func visitDatabaseDdl(
	ctx context.Context,
	databaseName string,
	tableName string,
	batch *batch.Batch,
	txn TxnOperator,
	eng engine.Engine,
	mp *mpool.MPool,
) error {
	if batch == nil {
		return moerr.NewInternalError(ctx, "batch is nil")
	}
	if len(batch.Vecs) < 4 {
		return moerr.NewInternalError(ctx, "batch should have at least 4 columns")
	}
	if mp == nil {
		return moerr.NewInternalError(ctx, "mpool is nil")
	}
	if eng == nil {
		return moerr.NewInternalError(ctx, "engine is nil")
	}
	if txn == nil {
		return moerr.NewInternalError(ctx, "txn is nil")
	}

	// Get database from engine using txn
	db, err := eng.Database(ctx, databaseName, txn)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to get database: %v", err)
	}

	// If tableName is provided, call visitTableDdl directly
	if len(tableName) > 0 {
		return visitTableDdl(ctx, databaseName, tableName, batch, txn, eng, mp)
	}

	// If tableName is empty, get all table names and process each one
	tableNames, err := db.Relations(ctx)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to get table names: %v", err)
	}

	// Process each table
	for _, tblName := range tableNames {
		err = visitTableDdl(ctx, databaseName, tblName, batch, txn, eng, mp)
		if err != nil {
			return err
		}
	}

	return nil
}

// getddlbatch creates a new batch with 4 columns (database name, table name, table id, table create sql)
// and fills it with DDL information
// If databaseName is provided, it calls visitDatabaseDdl with that database
// If databaseName is empty, it iterates through all databases
func getddlbatch(
	ctx context.Context,
	databaseName string,
	tableName string,
	eng engine.Engine,
	mp *mpool.MPool,
	txn TxnOperator,
) (*batch.Batch, error) {
	if eng == nil {
		return nil, moerr.NewInternalError(ctx, "engine is nil")
	}
	if mp == nil {
		return nil, moerr.NewInternalError(ctx, "mpool is nil")
	}
	if txn == nil {
		return nil, moerr.NewInternalError(ctx, "txn is nil")
	}

	// Create a new batch with 4 columns: database name, table name, table id, table create sql
	colNames := []string{"database name", "table name", "table id", "table create sql"}
	resultBatch := batch.New(colNames)

	// Initialize vectors for each column
	// Column 0: database name (varchar)
	resultBatch.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	// Column 1: table name (varchar)
	resultBatch.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	// Column 2: table id (int64)
	resultBatch.Vecs[2] = vector.NewVec(types.T_int64.ToType())
	// Column 3: table create sql (varchar)
	resultBatch.Vecs[3] = vector.NewVec(types.T_varchar.ToType())

	// If databaseName is provided, call visitDatabaseDdl with that database
	if len(databaseName) > 0 {
		err := visitDatabaseDdl(ctx, databaseName, tableName, resultBatch, txn, eng, mp)
		if err != nil {
			resultBatch.Clean(mp)
			return nil, err
		}
		return resultBatch, nil
	}

	// If databaseName is empty, get all database names and process each one
	dbNames, err := eng.Databases(ctx, txn)
	if err != nil {
		resultBatch.Clean(mp)
		return nil, moerr.NewInternalErrorf(ctx, "failed to get database names: %v", err)
	}

	// Process each database
	for _, dbName := range dbNames {
		err = visitDatabaseDdl(ctx, dbName, tableName, resultBatch, txn, eng, mp)
		if err != nil {
			resultBatch.Clean(mp)
			return nil, err
		}
	}

	return resultBatch, nil
}

// GetDdlBatchWithoutSession gets DDL batch without requiring Session
// This is a version that can be used by test utilities or other components
// If snapshot is provided, it will be applied to the transaction
func GetDdlBatchWithoutSession(
	ctx context.Context,
	databaseName string,
	tableName string,
	eng engine.Engine,
	txn client.TxnOperator,
	mp *mpool.MPool,
	snapshot *plan2.Snapshot,
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

	// Apply snapshot to txn if provided
	if snapshot != nil && snapshot.TS != nil {
		txn = txn.CloneSnapshotOp(*snapshot.TS)
	}

	// Call getddlbatch with the txn (which may have been cloned with snapshot)
	return getddlbatch(ctx, databaseName, tableName, eng, mp, txn)
}

// checkPublicationPermission checks if the current account has permission to access the specified database/table
// based on publication table (mo_pubs) configuration
func checkPublicationPermission(
	ctx context.Context,
	ses *Session,
	databaseName string,
	tableName string,
) error {
	bh := ses.GetShareTxnBackgroundExec(ctx, false)
	defer bh.Close()
	return checkPublicationPermissionWithBh(ctx, bh, databaseName, tableName)
}

// checkPublicationPermissionWithBh is the internal implementation that accepts a BackgroundExec
// This is useful for testing
func checkPublicationPermissionWithBh(
	ctx context.Context,
	bh BackgroundExec,
	databaseName string,
	tableName string,
) error {
	// Get current account ID and name
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}

	systemCtx := defines.AttachAccountId(ctx, catalog.System_Account)
	getAccountNameSQL := fmt.Sprintf(`select account_name from mo_catalog.mo_account where account_id = %d;`, accountID)
	bh.ClearExecResultSet()
	if err = bh.Exec(systemCtx, getAccountNameSQL); err != nil {
		return err
	}

	erArray, err := getResultSet(systemCtx, bh)
	if err != nil {
		return err
	}

	var accountName string
	if execResultArrayHasData(erArray) {
		if accountName, err = erArray[0].GetString(systemCtx, 0, 0); err != nil {
			return err
		}
	}

	if accountName == "" {
		return moerr.NewInternalError(ctx, "failed to get account name")
	}

	// Query mo_pubs table to find matching publications
	var querySQL string
	if databaseName != "" && tableName != "" {
		// Table level: check database_name and table_list
		querySQL = fmt.Sprintf(`select account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			from mo_catalog.mo_pubs 
			where database_name = "%s" 
			order by account_id, pub_name;`, databaseName)
	} else if databaseName != "" {
		// Database level: check database_name
		querySQL = fmt.Sprintf(`select account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			from mo_catalog.mo_pubs 
			where database_name = "%s" 
			order by account_id, pub_name;`, databaseName)
	} else {
		// Account level: check all publications
		querySQL = `select account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			from mo_catalog.mo_pubs 
			order by account_id, pub_name;`
	}

	bh.ClearExecResultSet()
	if err = bh.Exec(systemCtx, querySQL); err != nil {
		return err
	}

	erArray, err = getResultSet(systemCtx, bh)
	if err != nil {
		return err
	}

	var hasPermission bool
	if execResultArrayHasData(erArray) {
		for _, er := range erArray {
			for row := uint64(0); row < er.GetRowCount(); row++ {
				// Parse publication info
				var pubDbName, tableList, accountList string
				if pubDbName, err = er.GetString(systemCtx, row, 3); err != nil {
					continue
				}
				if tableList, err = er.GetString(systemCtx, row, 5); err != nil {
					continue
				}
				if accountList, err = er.GetString(systemCtx, row, 6); err != nil {
					continue
				}

				// Check if account is allowed
				pubInfo := &pubsub.PubInfo{
					SubAccountsStr: accountList,
				}
				if !pubInfo.InSubAccounts(accountName) {
					continue
				}

				// Check database match
				if databaseName != "" && pubDbName != databaseName {
					continue
				}

				// Check table match
				if tableName != "" {
					if strings.ToLower(tableList) == pubsub.TableAll {
						hasPermission = true
						break
					}
					// Check if table is in the list
					tableFound := false
					for _, tbl := range strings.Split(tableList, pubsub.Sep) {
						if strings.TrimSpace(tbl) == tableName {
							tableFound = true
							break
						}
					}
					if !tableFound {
						continue
					}
				}

				hasPermission = true
				break
			}
			if hasPermission {
				break
			}
		}
	}

	if !hasPermission {
		if tableName != "" {
			return moerr.NewInternalErrorf(ctx, "account %s does not have permission to access table %s.%s", accountName, databaseName, tableName)
		} else if databaseName != "" {
			return moerr.NewInternalErrorf(ctx, "account %s does not have permission to access database %s", accountName, databaseName)
		} else {
			return moerr.NewInternalErrorf(ctx, "account %s does not have permission to access account level resources", accountName)
		}
	}

	return nil
}

// checkPublicationPermissionForGetObject checks if the current account has permission to access any publication
// This is used for GET OBJECT where we don't have database/table information from objectName
func checkPublicationPermissionForGetObject(
	ctx context.Context,
	ses *Session,
) error {
	// Get current account ID and name
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}

	// Get account name
	bh := ses.GetShareTxnBackgroundExec(ctx, false)
	defer bh.Close()

	systemCtx := defines.AttachAccountId(ctx, catalog.System_Account)
	getAccountNameSQL := fmt.Sprintf(`select account_name from mo_catalog.mo_account where account_id = %d;`, accountID)
	bh.ClearExecResultSet()
	if err = bh.Exec(systemCtx, getAccountNameSQL); err != nil {
		return err
	}

	erArray, err := getResultSet(systemCtx, bh)
	if err != nil {
		return err
	}

	var accountName string
	if execResultArrayHasData(erArray) {
		if accountName, err = erArray[0].GetString(systemCtx, 0, 0); err != nil {
			return err
		}
	}

	if accountName == "" {
		return moerr.NewInternalError(ctx, "failed to get account name")
	}

	// Query all publications to check if account has access to any
	querySQL := `select account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
		from mo_catalog.mo_pubs 
		order by account_id, pub_name;`

	bh.ClearExecResultSet()
	if err = bh.Exec(systemCtx, querySQL); err != nil {
		return err
	}

	erArray, err = getResultSet(systemCtx, bh)
	if err != nil {
		return err
	}

	var hasPermission bool
	if execResultArrayHasData(erArray) {
		for _, er := range erArray {
			for row := uint64(0); row < er.GetRowCount(); row++ {
				// Parse publication info
				var accountList string
				if accountList, err = er.GetString(systemCtx, row, 6); err != nil {
					continue
				}

				// Check if account is allowed
				pubInfo := &pubsub.PubInfo{
					SubAccountsStr: accountList,
				}
				if pubInfo.InSubAccounts(accountName) {
					hasPermission = true
					break
				}
			}
			if hasPermission {
				break
			}
		}
	}

	if !hasPermission {
		return moerr.NewInternalErrorf(ctx, "account %s does not have permission to access any publication", accountName)
	}

	return nil
}
