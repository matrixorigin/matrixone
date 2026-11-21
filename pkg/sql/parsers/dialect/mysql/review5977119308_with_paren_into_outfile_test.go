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

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// The frontend initializes a SELECT INTO OUTFILE export only from the root
// Select.Ep (pkg/frontend/mysql_cmd_executor.go), so every WITH form whose
// parenthesized body owns a nested INTO OUTFILE must surface that export on
// the root Ep. Review 5977119308 on PR 32906: the bare parenthesized body was
// the only branch that left the root Ep nil, so the file was never created and
// the rows silently went back to the client. The grammar-side owner fix routes
// the bare branch through the same tree.SelectIntoExportOr helper the outer
// LIMIT / ORDER BY branches already use.
func TestWithParenBareBodyCarriesRootIntoOutfileExport(t *testing.T) {
	sql := "WITH cte AS (SELECT 1 AS a) (SELECT a FROM cte INTO OUTFILE '/tmp/outfile-actual-result.csv')"
	stmt, err := ParseOne(context.Background(), sql, 1)
	require.NoError(t, err, sql)
	sel, ok := stmt.(*tree.Select)
	require.True(t, ok, sql)
	require.NotNil(t, sel.Ep, "bare parenthesized WITH body must surface the nested export on the root select: %s", sql)
	require.True(t, sel.Ep.Outfile, sql)
	require.Equal(t, "/tmp/outfile-actual-result.csv", sel.Ep.FilePath, sql)
	require.NotNil(t, sel.With, "outer WITH must be preserved: %s", sql)
	require.Len(t, sel.With.CTEs, 1, sql)
	_, isParen := sel.Select.(*tree.ParenSelect)
	require.True(t, isParen, "the body must stay the wrapped paren select: %s", sql)
}

// The outer suffix forms already route through tree.SelectIntoExportOr; the
// fix must not regress them, and the nested export must survive the extra
// LIMIT / ORDER BY nodes.
func TestWithParenSuffixFormsKeepRootIntoOutfileExport(t *testing.T) {
	for _, sql := range []string{
		"WITH cte AS (SELECT 1 AS a) (SELECT a FROM cte INTO OUTFILE '/tmp/outfile-actual-result.csv') ORDER BY a DESC",
		"WITH cte AS (SELECT 1 AS a) (SELECT a FROM cte INTO OUTFILE '/tmp/outfile-actual-result.csv') LIMIT 1",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), sql, 1)
			require.NoError(t, err, sql)
			sel, ok := stmt.(*tree.Select)
			require.True(t, ok, sql)
			require.NotNil(t, sel.Ep, "outer suffix forms keep the export on the root select: %s", sql)
			require.True(t, sel.Ep.Outfile, sql)
			require.NotNil(t, sel.With, sql)
			require.Len(t, sel.With.CTEs, 1, sql)
			_, isParen := sel.Select.(*tree.ParenSelect)
			require.True(t, isParen, sql)
		})
	}
}

// Negative controls: without any INTO OUTFILE the root Ep stays nil in the
// bare form, with and without the outer suffixes, and the WITH body still
// round-trips through the canonical formatter.
func TestWithParenWithoutOutfileKeepsRootEpNil(t *testing.T) {
	for _, sql := range []string{
		"WITH cte AS (SELECT 1 AS a) (SELECT a FROM cte)",
		"WITH cte AS (SELECT 1 AS a) (SELECT a FROM cte) LIMIT 1",
		"WITH cte AS (SELECT 1 AS a) (SELECT a FROM cte) ORDER BY a",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), sql, 1)
			require.NoError(t, err, sql)
			sel, ok := stmt.(*tree.Select)
			require.True(t, ok, sql)
			require.Nil(t, sel.Ep, "no OUTFILE in the tree means no root export: %s", sql)
			require.NotNil(t, sel.With, sql)
			_, isParen := sel.Select.(*tree.ParenSelect)
			require.True(t, isParen, sql)
		})
	}
}
