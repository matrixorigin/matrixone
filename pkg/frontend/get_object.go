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

func handleGetObject(
	ctx context.Context,
	ses *Session,
	stmt *tree.GetObject,
) error {
	var (
		mrs      = ses.GetMysqlResultSet()
		showCols []*MysqlColumn
	)

	ses.ClearAllMysqlResultSet()
	ses.ClearResultBatches()

	// Create column: data
	col := new(MysqlColumn)
	col.SetName("data")
	col.SetColumnType(defines.MYSQL_TYPE_LONG)
	showCols = append(showCols, col)

	for _, col := range showCols {
		mrs.AddColumn(col)
	}

	// Add one row with value 1
	row := make([]any, 1)
	row[0] = int32(1)
	mrs.AddRow(row)

	// Save query result if needed
	return trySaveQueryResult(ctx, ses, mrs)
}

