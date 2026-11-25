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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

// TestConditionalStringDomain checks text/binary string semantics of
// conditional expressions against results recorded on MySQL 8.4:
//   - a binary literal branch is COERCIBLE and loses to a text column;
//   - COALESCE(txt, bin) is binary-typed, but character-aware functions see
//     the text value of rows taken from txt, also through a merged derived
//     table or scalar subquery, and not through a materialized one.
func TestConditionalStringDomain(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		dbName := testutils.GetDatabaseName(t)
		_, err = conn.ExecContext(ctx, fmt.Sprintf("create database `%s`", dbName))
		require.NoError(t, err)
		defer func() {
			_, cleanupErr := conn.ExecContext(ctx, fmt.Sprintf("drop database if exists `%s`", dbName))
			require.NoError(t, cleanupErr)
		}()
		for _, stmt := range []string{
			fmt.Sprintf("use `%s`", dbName),
			"create table sd(id int, txt varchar(16), bin varbinary(16))",
			"insert into sd values (1, '你a', null), (2, null, X'e4bda061'), (3, '你a', X'e4bda061')",
		} {
			_, err = conn.ExecContext(ctx, stmt)
			require.NoError(t, err, stmt)
		}

		for _, tc := range []struct{ sql, want string }{
			{"select id, char_length(if(txt is not null, txt, _binary '')), charset(if(txt is not null, txt, _binary '')) from sd order by id", "1,2,utf8mb4;2,0,utf8mb4;3,2,utf8mb4"},
			{"select id, hex(reverse(coalesce(txt, _binary ''))), charset(coalesce(txt, X'ff')) from sd order by id", "1,61E4BDA0,utf8mb4;2,,utf8mb4;3,61E4BDA0,utf8mb4"},
			{"select id, char_length(if(txt is not null, txt, bin)), charset(if(id > 0, bin, 'abc')) from sd order by id", "1,4,binary;2,4,binary;3,4,binary"},
			{"select id, char_length(coalesce(txt, bin)), charset(coalesce(txt, bin)), hex(left(coalesce(txt, bin), 1)) from sd order by id", "1,2,binary,E4BDA0;2,4,binary,E4;3,2,binary,E4BDA0"},
			{"select id, hex(reverse(coalesce(txt, bin))), char_length(upper(coalesce(txt, bin))), char_length(lpad(coalesce(txt, bin), 3, 'z')) from sd order by id", "1,61E4BDA0,2,5;2,61A0BDE4,4,3;3,61E4BDA0,2,5"},
			{"select id, char_length(nullif(coalesce(txt, bin), '')), char_length(coalesce(reverse(coalesce(txt, bin)), '')) from sd order by id", "1,2,2;2,4,4;3,2,2"},
			{"select id, locate('a', coalesce(txt, bin)), coalesce(txt, bin) like '__', hex(coalesce(txt, bin)) from sd order by id", "1,4,0,E4BDA061;2,4,0,E4BDA061;3,4,0,E4BDA061"},
			{"select id, char_length(x), hex(left(x, 1)) from (select id, coalesce(txt, bin) x from sd) d order by id", "1,2,E4BDA0;2,4,E4;3,2,E4BDA0"},
			{"select id, char_length(x) from (select id, coalesce(txt, bin) x from sd limit 3) d order by id", "1,4;2,4;3,4"},
			{"select char_length((select coalesce(txt, bin) from sd where id = 1 limit 1))", "2"},
		} {
			require.Equal(t, tc.want, queryRowsAsText(t, ctx, conn, tc.sql), tc.sql)
		}
	})
}

// queryRowsAsText renders a result as comma-separated fields and
// semicolon-separated rows; NULL renders as an empty field.
func queryRowsAsText(t *testing.T, ctx context.Context, conn *sql.Conn, query string) string {
	rows, err := conn.QueryContext(ctx, query)
	require.NoError(t, err, query)
	defer rows.Close()
	columns, err := rows.Columns()
	require.NoError(t, err)
	values := make([]sql.RawBytes, len(columns))
	targets := make([]any, len(values))
	for i := range values {
		targets[i] = &values[i]
	}
	var got []string
	for rows.Next() {
		require.NoError(t, rows.Scan(targets...))
		fields := make([]string, len(values))
		for i, value := range values {
			fields[i] = string(value)
		}
		got = append(got, strings.Join(fields, ","))
	}
	require.NoError(t, rows.Err())
	return strings.Join(got, ";")
}
