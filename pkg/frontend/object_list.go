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

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// handleObjectList handles objectlist command
// Returns mock data with three columns: db name, table name, object list
// Each column is filled with "1" as requested
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

	// Add one row with all columns filled with "1"
	row := []any{"1", "1", "1"}
	mrs.AddRow(row)

	// Save query result if needed
	return trySaveQueryResult(ctx, ses, mrs)
}
