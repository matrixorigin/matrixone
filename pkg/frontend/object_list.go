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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
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

	// Build three columns: db name, table name, object list
	col1 := new(MysqlColumn)
	col1.SetName("db name")
	col1.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

	col2 := new(MysqlColumn)
	col2.SetName("table name")
	col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

	col3 := new(MysqlColumn)
	col3.SetName("object list")
	col3.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

	showCols = append(showCols, col1, col2, col3)

	for _, col := range showCols {
		mrs.AddColumn(col)
	}

	// Parse snapshot timestamps if specified
	var dbNameTS, tableNameTS types.TS

	// Parse against snapshot (db name column)
	if stmt.AgainstSnapshot != nil && len(string(*stmt.AgainstSnapshot)) > 0 {
		snapshot, err := ses.GetTxnCompileCtx().ResolveSnapshotWithSnapshotName(string(*stmt.AgainstSnapshot))
		if err == nil && snapshot != nil && snapshot.TS != nil {
			dbNameTS = types.TimestampToTS(*snapshot.TS)
		} else {
			// If parsing fails, use empty TS
			dbNameTS = types.MinTs()
		}
	} else {
		// If against snapshot not specified, use empty TS
		dbNameTS = types.MinTs()
	}

	// Parse snapshot (table name column)
	if len(string(stmt.Snapshot)) > 0 {
		snapshot, err := ses.GetTxnCompileCtx().ResolveSnapshotWithSnapshotName(string(stmt.Snapshot))
		if err == nil && snapshot != nil && snapshot.TS != nil {
			tableNameTS = types.TimestampToTS(*snapshot.TS)
		} else {
			// If parsing fails, use empty TS
			tableNameTS = types.MinTs()
		}
	} else {
		// If snapshot not specified, use empty TS
		tableNameTS = types.MinTs()
	}

	// Add row with snapshot timestamps
	row := []any{dbNameTS.ToString(), tableNameTS.ToString(), "1"}
	mrs.AddRow(row)

	// Save query result if needed
	return trySaveQueryResult(ctx, ses, mrs)
}
