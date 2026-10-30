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

package mysql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQueryLockAndIntoContext(t *testing.T) {
	for _, tc := range []struct {
		sql   string
		valid bool
	}{
		{"(SELECT n_nationkey FROM nation FOR SHARE) FOR UPDATE", false},
		{"((SELECT n_nationkey FROM nation FOR UPDATE NOWAIT)) FOR SHARE", false},
		{"((SELECT n_nationkey FROM nation)) FOR UPDATE NOWAIT", true},
		{"(SELECT 1 FOR SHARE) FOR UPDATE", true},
		{"(SELECT 1 FROM DUAL FOR SHARE) FOR UPDATE", true},
		{"(SELECT * FROM (SELECT 1) d FOR SHARE) FOR UPDATE", true},
		{"WITH c AS (SELECT 1) ((SELECT * FROM c FOR SHARE) FOR UPDATE)", true},
		{"SELECT * FROM (SELECT n_nationkey FROM nation FOR SHARE) d FOR UPDATE", true},
		{"WITH c AS (SELECT 1) (SELECT 1 FROM c INTO @x)", false},
		{"SELECT (WITH nation AS (SELECT 1) SELECT 1), (((SELECT n_nationkey FROM nation FOR SHARE) FOR UPDATE))", false},
		{"WITH c AS (SELECT 1) (SELECT 1 INTO @x FROM c)", true},
		{"(SELECT * FROM nation INTO @x)", true},
		{"EXPLAIN (SELECT n_nationkey FROM nation FOR SHARE) FOR UPDATE", true},
		{"EXPLAIN ANALYZE (SELECT n_nationkey FROM nation FOR SHARE) FOR UPDATE", true},
		{"(SELECT * FROM nation UNION (SELECT * FROM region FOR SHARE)) FOR UPDATE", false},
		{"((SELECT * FROM nation FOR SHARE) UNION (SELECT * FROM region)) FOR UPDATE", true},
		{"(SELECT * FROM nation UNION (SELECT * FROM (SELECT * FROM region FOR SHARE) d)) FOR UPDATE", true},
		{"(SELECT 1 FROM `dual` FOR SHARE) FOR UPDATE", false},
		{"WITH c AS (SELECT 1) (WITH d AS (SELECT 2) ((SELECT * FROM c FOR SHARE) FOR UPDATE))", false},
		{"WITH c AS (SELECT 1) (WITH d AS (SELECT 2) ((SELECT * FROM d FOR SHARE) FOR UPDATE))", true},
		{"WITH c AS (SELECT 1) SELECT * FROM (WITH d AS (SELECT 2) ((SELECT * FROM c FOR SHARE) FOR UPDATE)) q", true},
		{"SELECT (WITH c AS (SELECT 1) (WITH d AS (SELECT 2) SELECT * FROM d)), (((SELECT * FROM c FOR SHARE) FOR UPDATE))", false},
		{"WITH c AS (SELECT 1) (SELECT 1 FROM nation INTO OUTFILE '/tmp/claude_no_execute')", false},
		{"WITH c AS (SELECT 1) (SELECT 1 INTO OUTFILE '/tmp/claude_no_execute' FROM nation)", true},
		{"(SELECT 1 FROM nation INTO OUTFILE '/tmp/claude_no_execute')", true},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			// Reuse a real parser so lock admission state cannot leak across calls.
			parser := &MySQLParser{}
			for _, sql := range []string{tc.sql, "SELECT 1"} {
				func() {
					statements, err := parser.ParseWithSQLMode(t.Context(), sql, 0, "")
					if sql == tc.sql && !tc.valid {
						require.Error(t, err)
						return
					}
					require.NoError(t, err)
					defer func() {
						for _, statement := range statements {
							statement.Free()
						}
					}()
					require.Len(t, statements, 1)
				}()
			}
		})
	}
}

func TestNestedSetIntoPlacement(t *testing.T) {
	// A small deep right spine catches collector/validator mutual recursion
	// without starting a service, allocating rows, or asserting wall time.
	const depth = 48
	for _, tc := range []struct {
		body  string
		valid bool
	}{
		{"SELECT 1", true},
		{"SELECT 1 INTO @x", true},
		{"SELECT 1 FROM nation INTO @x", false},
		{"SELECT 1 FROM nation INTO OUTFILE '/tmp/claude_no_execute'", false},
	} {
		sql := strings.Repeat("SELECT 1 UNION (", depth) + tc.body + strings.Repeat(")", depth)
		t.Run(tc.body, func(t *testing.T) {
			statements, err := (&MySQLParser{}).ParseWithSQLMode(t.Context(), sql, 0, "")
			if !tc.valid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			defer func() {
				for _, statement := range statements {
					statement.Free()
				}
			}()
			require.Len(t, statements, 1)
		})
	}
}

func TestDigestSubqueryExportContext(t *testing.T) {
	for _, sql := range []string{
		"ALTER TABLE t ALTER COLUMN a SET DEFAULT ((SELECT 1 INTO OUTFILE '/tmp/claude_no_execute'))",
		"CREATE TABLE t(a INT AS (SUM(1) OVER(ORDER BY (SELECT 1 INTO OUTFILE '/tmp/claude_no_execute'))))",
		"CREATE TABLE t(a INT DEFAULT (CASE WHEN 0 THEN (SELECT 1 INTO OUTFILE '/tmp/claude_no_execute') ELSE 1 END))",
	} {
		_, _, err := StatementDigest(t.Context(), sql, "")
		require.Error(t, err)
	}
	for _, sql := range []string{
		"SELECT 1 INTO OUTFILE '/tmp/claude_no_execute'",
		"ALTER TABLE t ALTER COLUMN a SET DEFAULT ((SELECT 1))",
		"CREATE TABLE t(a INT AS (SUM(1) OVER(PARTITION BY @x)))",
	} {
		_, _, err := StatementDigest(t.Context(), sql, "")
		require.NoError(t, err)
	}
}
