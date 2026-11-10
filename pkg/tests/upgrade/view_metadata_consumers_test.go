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

package upgrade

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Reuse the public View suite's cluster. Keep DDL on a different connection so
// the reader's session-local DDL counter cannot hide missing cache dependencies.
func testViewMetadataConsumers(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	writer, err := db.Conn(ctx)
	require.NoError(t, err)
	defer writer.Close()
	reader, err := db.Conn(ctx)
	require.NoError(t, err)
	defer reader.Close()
	exec := func(conn *sql.Conn, query string) {
		t.Helper()
		_, err := conn.ExecContext(ctx, query)
		require.NoError(t, err, query)
	}
	const database = "view_metadata_consumers"
	exec(writer, "create database "+database)
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := writer.ExecContext(cleanupCtx, "drop database "+database)
		require.NoError(t, err)
	}()
	exec(writer, "use "+database)
	exec(writer, "create table src (n int, code varchar(5))")
	exec(writer, "create view inner_v as select n as value, code from src")
	exec(writer, "create view outer_v as select value, code from inner_v")
	exec(reader, "use "+database)

	type preparedRead struct {
		sql  string
		stmt *sql.Stmt
	}
	queries := []string{
		"select * from outer_v limit 0",
		"select value, code from outer_v limit 0",
		"select value, code from outer_v where false",
	}
	prepared := make([]preparedRead, 0, len(queries))
	for _, query := range queries {
		stmt, err := reader.PrepareContext(ctx, query)
		require.NoError(t, err)
		defer stmt.Close()
		prepared = append(prepared, preparedRead{sql: query, stmt: stmt})
	}
	exec(reader, "prepare view_empty from 'select * from outer_v limit 0'")
	defer reader.ExecContext(ctx, "deallocate prepare view_empty")
	check := func(want string) {
		t.Helper()
		checkRows := func(query func() (*sql.Rows, error)) {
			t.Helper()
			rows, err := query()
			require.NoError(t, err)
			defer rows.Close()
			columns, err := rows.ColumnTypes()
			require.NoError(t, err)
			require.Len(t, columns, 2)
			require.Equal(t, "value", columns[0].Name())
			require.Equal(t, want, columns[0].DatabaseTypeName(), "empty results still carry a current schema")
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
		}
		for _, item := range prepared {
			checkRows(func() (*sql.Rows, error) { return reader.QueryContext(ctx, item.sql) })
			checkRows(func() (*sql.Rows, error) { return item.stmt.QueryContext(ctx) })
		}
		checkRows(func() (*sql.Rows, error) { return reader.QueryContext(ctx, "execute view_empty") })
	}
	check("INT")
	check("INT") // Warm the ordinary COM_QUERY cache independently of PREPARE.
	exec(writer, "alter table src modify column n bigint")
	check("BIGINT")
	exec(writer, "alter view inner_v as select cast(n as smallint) as value, code from src")
	check("SMALLINT")
	exec(writer, "alter view inner_v as select n as value, code from src")
	exec(writer, "drop table src")
	_, err = reader.ExecContext(ctx, "show column_number from outer_v")
	require.Error(t, err, "stored column counts are not a description of an invalid View")
	exec(writer, "create table src (n int, code varchar(90))")
	check("INT")
	var count int
	require.NoError(t, reader.QueryRowContext(ctx, "show column_number from outer_v").Scan(&count))
	require.Equal(t, 2, count)

	// Exercise both sides of the catalog-only shortcut. A cached empty table
	// result must not hide a later same-named View (and vice versa).
	exec(writer, "drop view outer_v")
	exec(writer, "create table outer_v (value int, code varchar(90))")
	check("INT")
	check("INT")
	exec(writer, "drop table outer_v")
	exec(writer, "create view outer_v as select cast(value as bigint) as value, code from inner_v")
	check("BIGINT")

	// A current VARCHAR must use MIN/MAX even when the persisted View column
	// was JSON. Explicit qualification must not select the reader's current DB.
	exec(writer, "create table json_src (j json)")
	exec(writer, "create view values_v as select j from json_src")
	exec(writer, "alter table json_src modify column j varchar(60)")
	exec(writer, "insert into json_src values ('a'), ('z')")
	exec(reader, "use mo_catalog")
	var maximum, minimum sql.NullString
	require.NoError(t, reader.QueryRowContext(ctx,
		"show table_values from "+database+".values_v").Scan(&maximum, &minimum))
	require.Equal(t, sql.NullString{String: "z", Valid: true}, maximum)
	require.Equal(t, sql.NullString{String: "a", Valid: true}, minimum)
}
