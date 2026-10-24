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

package issues

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestIssue30545ShowMetadataQuotedAndTemporaryTables(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", issue27487DSN(cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()

		const database = "issue_30545_show"
		execSQLRequire(t, ctx, db, "create database "+database)
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
			defer stop()
			execSQLRequire(t, cleanup, db, "drop database if exists "+database)
		}()

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(statement string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		queryRowCount := func(statement string) int {
			t.Helper()
			rows, err := conn.QueryContext(ctx, statement)
			require.NoError(t, err, statement)
			defer rows.Close()
			columns, err := rows.Columns()
			require.NoError(t, err)
			count := 0
			for rows.Next() {
				values := make([]any, len(columns))
				for i := range values {
					values[i] = new(sql.RawBytes)
				}
				require.NoError(t, rows.Scan(values...))
				count++
			}
			require.NoError(t, rows.Err())
			return count
		}
		queryFields := func(statement string) []string {
			t.Helper()
			rows, err := conn.QueryContext(ctx, statement)
			require.NoError(t, err, statement)
			defer rows.Close()
			columns, err := rows.Columns()
			require.NoError(t, err)
			field := -1
			for i, column := range columns {
				if column == "Field" {
					field = i
				}
			}
			require.GreaterOrEqual(t, field, 0, statement)
			var fields []string
			for rows.Next() {
				values := make([]any, len(columns))
				for i := range values {
					values[i] = new(sql.RawBytes)
				}
				require.NoError(t, rows.Scan(values...))
				fields = append(fields, string(*values[field].(*sql.RawBytes)))
			}
			require.NoError(t, rows.Err())
			return fields
		}

		exec("use " + database)
		exec("create table plain (id int primary key, v int)")
		exec("create table `a'b` (id int primary key, `x'y` int, key ix_x (`x'y`))")
		exec("insert into `a'b` values (1, 11)")
		require.GreaterOrEqual(t, queryRowCount("show table status from "+database+" like 'a%'"), 1)
		require.GreaterOrEqual(t, queryRowCount("show table status from "+database), 2)
		require.GreaterOrEqual(t, queryRowCount("show index from `a'b`"), 2)
		require.Contains(t, queryFields("show columns from `a'b`"), "x'y")

		exec("create temporary table temp_show (id int primary key, nm varchar(20), key k_nm (nm))")
		exec("insert into temp_show values (1, 'a')")
		for _, statement := range []string{
			"desc temp_show",
			"describe temp_show",
			"show columns from temp_show",
			"show fields from temp_show",
			"show full columns from temp_show",
		} {
			require.GreaterOrEqual(t, queryRowCount(statement), 2, statement)
		}
		require.GreaterOrEqual(t, queryRowCount("show index from temp_show"), 2)
		require.Equal(t, 1, queryRowCount("show create table temp_show"))
		exec("drop temporary table temp_show")
	})
}
