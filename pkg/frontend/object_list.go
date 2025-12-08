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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

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

	if len(tablename) == 0 {
		return moerr.NewInternalError(ctx, "table name is required")
	}

	// Parse snapshot timestamps if specified
	var from, to types.TS

	// Parse against snapshot (from timestamp)
	if stmt.AgainstSnapshot != nil && len(string(*stmt.AgainstSnapshot)) > 0 {
		snapshot, err := ses.GetTxnCompileCtx().ResolveSnapshotWithSnapshotName(string(*stmt.AgainstSnapshot))
		if err == nil && snapshot != nil && snapshot.TS != nil {
			from = types.TimestampToTS(*snapshot.TS)
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
		snapshot, err := ses.GetTxnCompileCtx().ResolveSnapshotWithSnapshotName(string(stmt.Snapshot))
		if err == nil && snapshot != nil && snapshot.TS != nil {
			to = types.TimestampToTS(*snapshot.TS)
		} else {
			// If parsing fails, use current timestamp
			if ses.GetProc() != nil && ses.GetProc().GetTxnOperator() != nil {
				to = types.TimestampToTS(ses.GetProc().GetTxnOperator().SnapshotTS())
			} else {
				to = types.MaxTs()
			}
		}
	} else {
		// If snapshot not specified, use current timestamp
		if ses.GetProc() != nil && ses.GetProc().GetTxnOperator() != nil {
			to = types.TimestampToTS(ses.GetProc().GetTxnOperator().SnapshotTS())
		} else {
			to = types.MaxTs()
		}
	}

	// Get object list batch
	objBatch, err := GetObjectList(ctx, from, to, dbname, tablename, ses)
	if err != nil {
		return err
	}

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
	if objBatch != nil && objBatch.Attrs != nil {
		for _, attr := range objBatch.Attrs {
			col := new(MysqlColumn)
			col.SetName(attr)
			// Convert batch column type to MySQL type
			for i, batchAttr := range objBatch.Attrs {
				if batchAttr == attr && i < len(objBatch.Vecs) {
					typ := objBatch.Vecs[i].GetType()
					err := convertEngineTypeToMysqlType(ctx, typ.Oid, col)
					if err != nil {
						return err
					}
					break
				}
			}
			showCols = append(showCols, col)
		}
	}

	for _, col := range showCols {
		mrs.AddColumn(col)
	}

	// Extract rows from batch and add table name and db name
	if objBatch != nil {
		n := objBatch.RowCount()
		for j := 0; j < n; j++ {
			row := make([]any, len(showCols))
			// First column: table name
			row[0] = tablename
			// Second column: db name
			row[1] = dbname
			// Extract object list batch columns
			if err := extractRowFromEveryVector(ctx, ses, objBatch, j, row[2:], false); err != nil {
				objBatch.Clean(ses.GetMemPool())
				return err
			}
			mrs.AddRow(row)
		}
		objBatch.Clean(ses.GetMemPool())
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
