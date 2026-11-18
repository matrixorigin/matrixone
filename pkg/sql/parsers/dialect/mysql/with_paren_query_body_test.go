// Copyright 2026 Matrix Origin
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

package mysql

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// The parenthesized query-expression body after a WITH clause is standard and
// MySQL-accepted, so the outer With must be preserved on the wrapped
// ParenSelect exactly as for the unparenthesized twin (#32606).
func TestWithClauseParenthesizedQueryBodyParses(t *testing.T) {
	for _, sql := range []string{
		"WITH cte AS (SELECT 1 AS a) (SELECT * FROM cte)",
		"WITH cte AS (SELECT 1 AS a) ((SELECT * FROM cte))",
		"WITH cte AS (SELECT 1 AS a) (SELECT * FROM cte UNION ALL SELECT 2 AS a)",
		"WITH cte AS (SELECT 1 AS a) (SELECT a INTO @x FROM cte)",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), sql, 1)
			require.NoError(t, err, sql)
			sel, ok := stmt.(*tree.Select)
			require.True(t, ok, sql)
			require.NotNil(t, sel.With, "outer WITH must be preserved: %s", sql)
			require.Len(t, sel.With.CTEs, 1, sql)
			_, ok = sel.Select.(*tree.ParenSelect)
			require.True(t, ok, sql)
			formatted := tree.String(stmt, dialect.MYSQL)
			reStmt, err := ParseOne(context.Background(), formatted, 1)
			require.NoError(t, err, formatted)
			reSel, ok := reStmt.(*tree.Select)
			require.True(t, ok, formatted)
			require.NotNil(t, reSel.With, "outer WITH must survive the formatted replay: %s", formatted)
			require.Len(t, reSel.With.CTEs, 1, formatted)
			_, ok = reSel.Select.(*tree.ParenSelect)
			require.True(t, ok, formatted)
		})
	}
}

// Existing boundary behavior of the WITH + parenthesized forms: plain and
// tailed queries keep parsing, empty bodies stay rejected, INTO dangling after
// the body stays rejected, and a failed parse does not poison the next call.
func TestWithClauseParenQueryBodyPreExistingBoundaries(t *testing.T) {
	for _, sql := range []string{
		"WITH cte AS (SELECT 1 AS a) SELECT * FROM cte",
		"WITH cte AS (SELECT 1 AS a) (SELECT * FROM cte) ORDER BY a",
		"WITH cte AS (SELECT 1 AS a) (SELECT * FROM cte) ORDER BY a LIMIT 1",
		"WITH cte AS (SELECT 1 AS a) (SELECT * FROM cte) LIMIT 1",
		"WITH cte AS (SELECT 1 AS a) (SELECT * FROM cte) LIMIT 1 OFFSET 0",
		"SELECT 1 INTO @x",
		"(SELECT 1 INTO @x)",
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := ParseOne(context.Background(), sql, 1)
			require.NoError(t, err, sql)
		})
	}
}

func TestWithClauseParenQueryBodyNegations(t *testing.T) {
	for _, sql := range []string{
		"WITH cte AS (SELECT 1 AS a) ()",
		"WITH cte AS (SELECT 1 AS a) (SELECT * FROM cte) INTO @x",
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := ParseOne(context.Background(), sql, 1)
			require.Error(t, err, sql)
		})
	}
	// A rejected statement must not poison the parser for the next call.
	stmt, err := ParseOne(context.Background(), "select 42", 1)
	require.NoError(t, err)
	require.NotNil(t, stmt)
}
