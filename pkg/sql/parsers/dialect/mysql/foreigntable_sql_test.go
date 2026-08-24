// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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

func TestForeignCreateExternalTable(t *testing.T) {
	ctx := context.Background()

	sql := `create external table t (id bigint, name varchar(64)) engine = sql with ('config' = '{"driver":"mysql","dsn":"u:p@tcp(h)/db"}', 'query' = 'select 1')`
	stmt, err := ParseOne(ctx, sql, 1)
	require.NoError(t, err)
	ct, ok := stmt.(*tree.CreateTable)
	require.True(t, ok)
	require.NotNil(t, ct.ForeignParam)
	require.Equal(t, "sql", ct.ForeignParam.Kind)
	require.Len(t, ct.ForeignParam.Options, 2)
	require.Equal(t, "config", string(ct.ForeignParam.Options[0].Key))
	require.Equal(t, `{"driver":"mysql","dsn":"u:p@tcp(h)/db"}`, ct.ForeignParam.Options[0].Val)
	require.Equal(t, "query", string(ct.ForeignParam.Options[1].Key))

	// Format redacts an inline config (it carries credentials) but keeps the
	// query, and the redacted form must re-parse.
	formatted := tree.String(stmt, dialect.MYSQL)
	require.Contains(t, formatted, "engine = sql with (")
	require.Contains(t, formatted, `"config" = '<redacted>'`)
	require.NotContains(t, formatted, "u:p@tcp")
	require.Contains(t, formatted, `"query" = 'select 1'`)
	_, err = ParseOne(ctx, formatted, 1)
	require.NoError(t, err, formatted)

	// every config value is redacted on re-render, whatever its shape
	stmtEsql, err := ParseOne(ctx, "create external table t (a int) engine = ESQL with ('config' = '{\"addresses\":[\"http://h\"]}')", 1)
	require.NoError(t, err)
	require.Equal(t, "esql", stmtEsql.(*tree.CreateTable).ForeignParam.Kind)
	formattedEsql := tree.String(stmtEsql, dialect.MYSQL)
	require.Contains(t, formattedEsql, "engine = esql with (")
	require.Contains(t, formattedEsql, `"config" = '<redacted>'`)

	// optionless forms, with and without '='.
	for _, s := range []string{
		"create external table t (a int) engine = esql",
		"create external table t (a int) engine sql",
	} {
		st, err := ParseOne(ctx, s, 1)
		require.NoError(t, err, s)
		require.NotNil(t, st.(*tree.CreateTable).ForeignParam)
		require.Nil(t, st.(*tree.CreateTable).ForeignParam.Options)
	}

	// ordinary tables keep ENGINE = SQL as a plain table option, and esql
	// stays usable as an identifier.
	stmtPlain, err := ParseOne(ctx, "create table t (a int) engine = sql", 1)
	require.NoError(t, err)
	require.Nil(t, stmtPlain.(*tree.CreateTable).ForeignParam)
	_, err = ParseOne(ctx, "create table esql (esql int)", 1)
	require.NoError(t, err)
	_, err = ParseOne(ctx, "select esql from t", 1)
	require.NoError(t, err)
}
