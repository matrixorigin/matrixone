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

package taskservice

import (
	"context"
	"database/sql"
)

// ForeachQueriedRow visits query results and counts rows accepted by the callback.
func ForeachQueriedRow(
	ctx context.Context,
	tx SqlExecutor,
	query string,
	onEachRow func(context.Context, *sql.Rows) (bool, error),
) (cnt int64, err error) {
	var (
		ok   bool
		rows *sql.Rows
	)
	if rows, err = tx.QueryContext(ctx, query); err != nil {
		return
	}
	if rows.Err() != nil {
		err = rows.Err()
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		if ok, err = onEachRow(ctx, rows); err != nil {
			return
		}
		if ok {
			cnt++
		}
	}
	err = rows.Err()
	return
}
