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
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestExplainFormatJSONGrammarAndRoundTrip(t *testing.T) {
	for _, sql := range []string{
		"EXPLAIN FORMAT=JSON SELECT 1",
		"explain format = 'json' select 1",
		"EXPLAIN (FORMAT JSON) SELECT 1",
	} {
		stmt, err := ParseOne(context.Background(), sql, 1)
		require.NoError(t, err, sql)

		explainStmt, ok := stmt.(*tree.ExplainStmt)
		require.True(t, ok, sql)
		require.Len(t, explainStmt.Options, 1)
		require.Equal(t, tree.FormatOption, strings.ToLower(explainStmt.Options[0].Name))
		require.Equal(t, "json", strings.ToLower(strings.Trim(explainStmt.Options[0].Value, "'")))

		formatted := tree.String(stmt, dialect.MYSQL)
		roundTrip, err := ParseOne(context.Background(), formatted, 1)
		require.NoError(t, err, formatted)
		roundTripExplain, ok := roundTrip.(*tree.ExplainStmt)
		require.True(t, ok, formatted)
		require.Equal(t, strings.ToLower(explainStmt.Options[0].Value),
			strings.ToLower(roundTripExplain.Options[0].Value))
	}
}

func TestExplainAnalyzeJSONAndAnalyzeFalseAST(t *testing.T) {
	stmt, err := ParseOne(context.Background(),
		"EXPLAIN ANALYZE FORMAT=JSON SELECT 1", 1)
	require.NoError(t, err)
	analyze, ok := stmt.(*tree.ExplainAnalyze)
	require.True(t, ok)
	require.Len(t, analyze.Options, 2)

	stmt, err = ParseOne(context.Background(),
		"EXPLAIN (ANALYZE FALSE, FORMAT JSON) SELECT 1", 1)
	require.NoError(t, err)
	regular, ok := stmt.(*tree.ExplainStmt)
	require.True(t, ok)
	require.Len(t, regular.Options, 2)
}

func TestExplainFormatJSONPreservesSelectIntoValidation(t *testing.T) {
	_, err := ParseOne(context.Background(),
		"EXPLAIN FORMAT=JSON SELECT 1 INTO @explain_json_source", 1)
	require.NoError(t, err)

	_, err = ParseOne(context.Background(),
		"EXPLAIN FORMAT=JSON SELECT 1 INTO @explain_json_source UNION SELECT 1", 1)
	require.Error(t, err)
}

func TestExplainFormatJSONRejectsOutOfScopeFormat(t *testing.T) {
	_, err := ParseOne(context.Background(),
		"EXPLAIN FORMAT=TRADITIONAL SELECT 1", 1)
	require.Error(t, err)
}
