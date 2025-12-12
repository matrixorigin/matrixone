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
)

// ProcessObjectList is the core function that processes OBJECTLIST statement
// It returns a batch with "table name", "db name" columns plus object list columns
// This function can be used by both handleObjectList and test utilities
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
	if len(tablename) == 0 {
		return nil, moerr.NewInternalError(ctx, "table name is required")
	}

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

	// Get object list batch
	objBatch, err := GetObjectListWithoutSession(ctx, from, to, dbname, tablename, eng, txnOp, mp)
	if err != nil {
		return nil, err
	}

	// Build result batch with table name, db name + object list batch columns
	resultBatch, err := BuildObjectListResultBatch(objBatch, dbname, tablename, mp)
	if err != nil {
		return nil, err
	}

	return resultBatch, nil
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

	// Build columns: table name, db name + object list batch columns
	col1 := new(MysqlColumn)
	col1.SetName("table name")
	col1.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	showCols = append(showCols, col1)

	col2 := new(MysqlColumn)
	col2.SetName("db name")
	col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	showCols = append(showCols, col2)

	// Add object list batch columns
	if resultBatch != nil && resultBatch.Attrs != nil && len(resultBatch.Attrs) > 2 {
		for i := 2; i < len(resultBatch.Attrs); i++ {
			attr := resultBatch.Attrs[i]
			col := new(MysqlColumn)
			col.SetName(attr)
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

// GetObjectList gets object list from the specified table within the given time range
func GetObjectList(
	ctx context.Context,
	from, to types.TS,
	dbname, tablename string,
	ses *Session,
) (*batch.Batch, error) {
	// Get txnHandler from session
	txnHandler := ses.GetTxnHandler()
	if txnHandler == nil {
		return nil, moerr.NewInternalError(ctx, "txn handler is nil")
	}

	// Get txn from txnHandler
	txn := txnHandler.GetTxn()
	if txn == nil {
		return nil, moerr.NewInternalError(ctx, "txn is nil")
	}

	// Get engine from txnHandler
	eng := txnHandler.GetStorage()
	if eng == nil {
		return nil, moerr.NewInternalError(ctx, "engine is nil")
	}

	// Get database from engine using txn
	var db engine.Database
	var err error
	db, err = eng.Database(ctx, dbname, txn)
	if err != nil {
		return nil, moerr.NewInternalError(ctx, fmt.Sprintf("failed to get database: %v", err))
	}

	// Get table from database
	var table engine.Relation
	table, err = db.Relation(ctx, tablename, nil)
	if err != nil {
		return nil, moerr.NewInternalError(ctx, fmt.Sprintf("failed to get table: %v", err))
	}

	// Call CollectObjectList on table
	mp := ses.GetMemPool()
	if mp == nil {
		return nil, moerr.NewInternalError(ctx, "mpool is nil")
	}

	bat, err := table.CollectObjectList(ctx, from, to, mp)
	if err != nil {
		return nil, err
	}

	return bat, nil
}

// GetObjectListWithoutSession gets object list from the specified table without requiring Session
// This is a version that can be used by test utilities
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

	// Get database from engine using txn
	db, err := eng.Database(ctx, dbname, txn)
	if err != nil {
		return nil, moerr.NewInternalError(ctx, fmt.Sprintf("failed to get database: %v", err))
	}

	// Get table from database
	table, err := db.Relation(ctx, tablename, nil)
	if err != nil {
		return nil, moerr.NewInternalError(ctx, fmt.Sprintf("failed to get table: %v", err))
	}

	// Call CollectObjectList on table
	bat, err := table.CollectObjectList(ctx, from, to, mp)
	if err != nil {
		return nil, err
	}

	return bat, nil
}

// BuildObjectListResultBatch builds a result batch with table name, db name + object list batch columns
// This function adds "table name" and "db name" columns to the object list batch
func BuildObjectListResultBatch(
	objBatch *batch.Batch,
	dbname, tablename string,
	mp *mpool.MPool,
) (*batch.Batch, error) {
	// Column names: "table name", "db name" + object list batch columns
	colNames := []string{"table name", "db name"}
	if objBatch != nil && objBatch.Attrs != nil {
		colNames = append(colNames, objBatch.Attrs...)
	}

	resultBatch := batch.New(colNames)

	// Create "table name" column (varchar)
	resultBatch.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	// Create "db name" column (varchar)
	resultBatch.Vecs[1] = vector.NewVec(types.T_varchar.ToType())

	// Copy object list batch columns to result batch
	// We directly use objBatch vectors and will not clean objBatch
	// to avoid double cleanup
	if objBatch != nil && objBatch.Vecs != nil {
		for i := 0; i < len(objBatch.Vecs); i++ {
			resultBatch.Vecs[i+2] = objBatch.Vecs[i]
		}
	}

	// Extract rows from batch and add table name and db name
	if objBatch != nil {
		n := objBatch.RowCount()
		for j := 0; j < n; j++ {
			// Append table name
			err := vector.AppendBytes(resultBatch.Vecs[0], []byte(tablename), false, mp)
			if err != nil {
				resultBatch.Clean(mp)
				return nil, err
			}

			// Append db name
			err = vector.AppendBytes(resultBatch.Vecs[1], []byte(dbname), false, mp)
			if err != nil {
				resultBatch.Clean(mp)
				return nil, err
			}
		}
		// Don't clean objBatch here as its vectors are now used by resultBatch
		// resultBatch.Clean will clean all vectors including the ones from objBatch
	}

	return resultBatch, nil
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
