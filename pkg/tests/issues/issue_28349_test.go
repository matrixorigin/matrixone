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
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

// TestIssue28349AutoIncrementPublicPaths exercises the three public paths added
// by the auto-increment session/provenance work: one multi-statement COM_QUERY,
// ordered INSERT IGNORE candidate reuse, and REPLACE LAST_INSERT_ID reporting.
func TestIssue28349AutoIncrementPublicPaths(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		dbName := testutils.GetDatabaseName(t)
		dsn := fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/?multiStatements=true",
			cn.GetServiceConfig().CN.Frontend.Port,
		)
		db, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(statement string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement)
			require.NoErrorf(t, err, "exec failed: %s", statement)
		}
		queryInt64Rows := func(statement string, columns int) [][]int64 {
			t.Helper()
			rows, err := conn.QueryContext(ctx, statement)
			require.NoErrorf(t, err, "query failed: %s", statement)
			defer rows.Close()
			var result [][]int64
			for rows.Next() {
				values := make([]int64, columns)
				dest := make([]any, columns)
				for i := range values {
					dest[i] = &values[i]
				}
				require.NoError(t, rows.Scan(dest...))
				result = append(result, values)
			}
			require.NoError(t, rows.Err())
			return result
		}
		queryInt64 := func(statement string) int64 {
			t.Helper()
			var value int64
			require.NoError(t, conn.QueryRowContext(ctx, statement).Scan(&value))
			return value
		}

		exec(fmt.Sprintf("create database `%s`", dbName))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, _ = conn.ExecContext(cleanupCtx, fmt.Sprintf("drop database if exists `%s`", dbName))
		}()
		exec(fmt.Sprintf("use `%s`", dbName))
		exec("set auto_increment_increment = 1")
		exec("set auto_increment_offset = 1")

		exec("create table ai_multi(id bigint auto_increment primary key, v int)")
		// The SET statements and INSERT must share one COM_QUERY. Separate
		// Exec calls would not exercise frontend statement snapshot refresh.
		exec("set auto_increment_increment = 3; set auto_increment_offset = 2; insert into ai_multi(v) values (1), (2), (3)")
		require.Equal(t, [][]int64{{2, 1}, {5, 2}, {8, 3}},
			queryInt64Rows("select id, v from ai_multi order by id", 2))
		exec("drop table ai_multi")

		exec("set auto_increment_increment = 1")
		exec("set auto_increment_offset = 1")
		exec("create table ai_ignore(id bigint auto_increment primary key, uk int unique, v int)")
		exec("insert into ai_ignore(uk, v) values (10, 0)")
		exec("insert ignore into ai_ignore(uk, v) values (20, 1), (10, 2), (30, 3)")
		require.Equal(t, [][]int64{{1, 10}, {2, 20}, {3, 30}},
			queryInt64Rows("select id, uk from ai_ignore order by id", 2))
		require.Equal(t, int64(2), queryInt64("select last_insert_id()"),
			"INSERT IGNORE must publish the first accepted generated candidate")
		exec("drop table ai_ignore")

		exec("create table ai_replace(id bigint auto_increment primary key, uk int unique, v int)")
		exec("insert into ai_replace(uk, v) values (10, 1)")
		exec("replace into ai_replace(uk, v) values (10, 2)")
		require.Equal(t, [][]int64{{2, 10, 2}},
			queryInt64Rows("select id, uk, v from ai_replace order by id", 3))
		require.Equal(t, int64(2), queryInt64("select last_insert_id()"),
			"REPLACE must publish the generated replacement key")
		exec("drop table ai_replace")
	})
}
