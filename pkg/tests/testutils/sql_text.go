// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package testutils

import (
	"context"
	"database/sql"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// SQLTextQueryer is the subset of database/sql used by text-returning SQL
// assertions. Both *sql.DB and *sql.Conn implement it.
type SQLTextQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// TextQueryResult preserves both parts of a one-text-column SQL result: its
// protocol column name and its text rows. EXPLAIN uses the column name to carry
// the plan class, so both are part of that public SQL contract.
type TextQueryResult struct {
	ColumnName string
	Text       string
}

// QueryTextResult executes a one-text-column query without discarding its SQL
// protocol metadata.
func QueryTextResult(
	ctx context.Context,
	queryer SQLTextQueryer,
	statement string,
) (TextQueryResult, error) {
	rows, err := queryer.QueryContext(ctx, statement)
	if err != nil {
		return TextQueryResult{}, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return TextQueryResult{}, err
	}
	if len(columns) != 1 {
		return TextQueryResult{}, moerr.NewInternalErrorf(ctx,
			"expected one text column, got %d columns", len(columns))
	}

	var lines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return TextQueryResult{}, err
		}
		lines = append(lines, line)
	}
	if err := rows.Err(); err != nil {
		return TextQueryResult{}, err
	}
	return TextQueryResult{
		ColumnName: columns[0],
		Text:       strings.Join(lines, "\n"),
	}, nil
}

// QueryText executes a one-text-column query and joins its rows with newlines.
// It is suitable for EXPLAIN and other SQL-protocol assertions whose result is
// deliberately textual rather than a typed data set. Use QueryTextResult when
// the protocol column name is also part of the assertion.
func QueryText(ctx context.Context, queryer SQLTextQueryer, statement string) (string, error) {
	result, err := QueryTextResult(ctx, queryer, statement)
	if err != nil {
		return "", err
	}
	return result.Text, nil
}
