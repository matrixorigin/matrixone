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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// SnapshotCoveredScope represents the scope of databases/tables covered by a snapshot
type SnapshotCoveredScope struct {
	DatabaseName string
	TableName    string
	Level        string
}

// GetAccountIDFromPublication verifies publication permission and returns the upstream account ID
// Parameters:
//   - ctx: context
//   - bh: background executor
//   - pubAccountName: publication account name
//   - pubName: publication name
//   - currentAccount: current tenant account name
//
// Returns:
//   - accountID: the upstream account ID
//   - accountName: the upstream account name
//   - err: error if permission denied or publication not found
func GetAccountIDFromPublication(ctx context.Context, bh BackgroundExec, pubAccountName string, pubName string, currentAccount string) (uint64, string, error) {
	return getAccountFromPublication(ctx, bh, pubAccountName, pubName, currentAccount)
}

// GetSnapshotTsByName gets the snapshot timestamp by snapshot name
// Parameters:
//   - ctx: context with account ID attached
//   - bh: background executor
//   - snapshotName: the snapshot name
//
// Returns:
//   - ts: snapshot timestamp (physical time in nanoseconds)
//   - err: error if snapshot not found or query failed
func GetSnapshotTsByName(ctx context.Context, bh BackgroundExec, snapshotName string) (int64, error) {
	record, err := getSnapshotByName(ctx, bh, snapshotName)
	if err != nil {
		return 0, moerr.NewInternalErrorf(ctx, "failed to query snapshot: %v", err)
	}
	if record == nil {
		return 0, moerr.NewInternalErrorf(ctx, "snapshot %s does not exist", snapshotName)
	}
	return record.ts, nil
}

// GetSnapshotCoveredScope gets the database/table scope covered by a snapshot
// Parameters:
//   - ctx: context with account ID attached
//   - bh: background executor
//   - snapshotName: the snapshot name
//
// Returns:
//   - scope: the covered scope containing database name, table name and level
//   - snapshotTs: snapshot timestamp
//   - err: error if snapshot not found or query failed
func GetSnapshotCoveredScope(ctx context.Context, bh BackgroundExec, snapshotName string) (*SnapshotCoveredScope, int64, error) {
	record, err := getSnapshotByName(ctx, bh, snapshotName)
	if err != nil {
		return nil, 0, moerr.NewInternalErrorf(ctx, "failed to query snapshot: %v", err)
	}
	if record == nil {
		return nil, 0, moerr.NewInternalErrorf(ctx, "snapshot %s does not exist", snapshotName)
	}

	scope := &SnapshotCoveredScope{
		Level: record.level,
	}

	switch record.level {
	case "table":
		scope.DatabaseName = record.databaseName
		scope.TableName = record.tableName
	case "database":
		scope.DatabaseName = record.databaseName
		scope.TableName = ""
	case "account", "cluster":
		scope.DatabaseName = ""
		scope.TableName = ""
	default:
		return nil, 0, moerr.NewInternalErrorf(ctx, "unsupported snapshot level: %s", record.level)
	}

	return scope, record.ts, nil
}

// ComputeDdlBatch computes the DDL batch for given scope
// This is the core function that can be used by both frontend handlers and upstream sql helper
// Parameters:
//   - ctx: context with account ID attached
//   - databaseName: database name (empty to iterate all databases)
//   - tableName: table name (empty to iterate all tables in database)
//   - eng: storage engine
//   - mp: memory pool
//   - txn: transaction operator (should already have snapshot timestamp applied if needed)
//
// Returns:
//   - batch: the result batch with columns (dbname, tablename, tableid, tablesql)
//   - err: error if failed
func ComputeDdlBatch(
	ctx context.Context,
	databaseName string,
	tableName string,
	eng engine.Engine,
	mp *mpool.MPool,
	txn TxnOperator,
) (*batch.Batch, error) {
	return getddlbatch(ctx, databaseName, tableName, eng, mp, txn)
}

// ComputeDdlBatchWithSnapshot computes the DDL batch with snapshot applied
// Parameters:
//   - ctx: context with account ID attached
//   - databaseName: database name (empty to iterate all databases)
//   - tableName: table name (empty to iterate all tables in database)
//   - eng: storage engine
//   - mp: memory pool
//   - txn: transaction operator
//   - snapshotTs: snapshot timestamp (0 means no snapshot)
//
// Returns:
//   - batch: the result batch with columns (dbname, tablename, tableid, tablesql)
//   - err: error if failed
func ComputeDdlBatchWithSnapshot(
	ctx context.Context,
	databaseName string,
	tableName string,
	eng engine.Engine,
	mp *mpool.MPool,
	txn TxnOperator,
	snapshotTs int64,
) (*batch.Batch, error) {
	// Apply snapshot timestamp if provided
	if snapshotTs != 0 {
		txn = txn.CloneSnapshotOp(timestamp.Timestamp{PhysicalTime: snapshotTs})
	}
	return getddlbatch(ctx, databaseName, tableName, eng, mp, txn)
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

	if strings.HasPrefix(tableName, catalog.IndexTableNamePrefix) {
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

// BuildDdlMysqlResultSet builds the MySQL result set columns for DDL query
func BuildDdlMysqlResultSet(mrs *MysqlResultSet) {
	colDbName := new(MysqlColumn)
	colDbName.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	colDbName.SetName("dbname")

	colTableName := new(MysqlColumn)
	colTableName.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	colTableName.SetName("tablename")

	colTableId := new(MysqlColumn)
	colTableId.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	colTableId.SetName("tableid")

	colTableSql := new(MysqlColumn)
	colTableSql.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	colTableSql.SetName("tablesql")

	mrs.AddColumn(colDbName)
	mrs.AddColumn(colTableName)
	mrs.AddColumn(colTableId)
	mrs.AddColumn(colTableSql)
}

// FillDdlMysqlResultSet fills the MySQL result set from the DDL batch
func FillDdlMysqlResultSet(resultBatch *batch.Batch, mrs *MysqlResultSet) {
	if resultBatch != nil && resultBatch.RowCount() > 0 {
		for i := 0; i < resultBatch.RowCount(); i++ {
			row := make([]interface{}, 4)
			// dbname
			row[0] = resultBatch.Vecs[0].GetBytesAt(i)
			// tablename
			row[1] = resultBatch.Vecs[1].GetBytesAt(i)
			// tableid
			row[2] = vector.GetFixedAtNoTypeCheck[int64](resultBatch.Vecs[2], i)
			// tablesql
			row[3] = resultBatch.Vecs[3].GetBytesAt(i)
			mrs.AddRow(row)
		}
	}
}
