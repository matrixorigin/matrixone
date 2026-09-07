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
		exec := func(t *testing.T, statement string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement)
			require.NoErrorf(t, err, "exec failed: %s", statement)
		}
		queryInt64Rows := func(t *testing.T, statement string, columns int) [][]int64 {
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
		queryInt64 := func(t *testing.T, statement string) int64 {
			t.Helper()
			var value int64
			require.NoError(t, conn.QueryRowContext(ctx, statement).Scan(&value))
			return value
		}

		exec(t, fmt.Sprintf("create database `%s`", dbName))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, _ = conn.ExecContext(cleanupCtx, fmt.Sprintf("drop database if exists `%s`", dbName))
		}()
		exec(t, fmt.Sprintf("use `%s`", dbName))
		exec(t, "set auto_increment_increment = 1")
		exec(t, "set auto_increment_offset = 1")

		exec(t, "create table ai_multi(id bigint auto_increment primary key, v int)")
		// The SET statements and INSERT must share one COM_QUERY. Separate
		// Exec calls would not exercise frontend statement snapshot refresh.
		exec(t, "set auto_increment_increment = 3; set auto_increment_offset = 2; insert into ai_multi(v) values (1), (2), (3)")
		require.Equal(t, [][]int64{{2, 1}, {5, 2}, {8, 3}},
			queryInt64Rows(t, "select id, v from ai_multi order by id", 2))
		exec(t, "drop table ai_multi")

		exec(t, "set auto_increment_increment = 1")
		exec(t, "set auto_increment_offset = 1")
		exec(t, "create table ai_ignore(id bigint auto_increment primary key, uk int unique, v int)")
		exec(t, "insert into ai_ignore(uk, v) values (10, 0)")
		exec(t, "insert ignore into ai_ignore(uk, v) values (20, 1), (10, 2), (30, 3)")
		require.Equal(t, [][]int64{{1, 10}, {2, 20}, {3, 30}},
			queryInt64Rows(t, "select id, uk from ai_ignore order by id", 2))
		require.Equal(t, int64(2), queryInt64(t, "select last_insert_id()"),
			"INSERT IGNORE must publish the first accepted generated candidate")
		exec(t, "drop table ai_ignore")

		for _, tc := range []struct {
			name, definition, rows string
		}{
			{"composite", "a varchar(10), b int, unique key uk(a,b)", "('ab1',1),('ab1',1),('ab2',2)"},
			{"prefix", "a varchar(10), b int, unique key uk(a(2))", "('ab1',1),('ab2',1),('cd1',2)"},
			{"check", "a varchar(10), b int check(b > 0), unique key uk(a,b)", "('ab1',1),('ab1',1),('ab2',2)"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				exec(t, "create table ai_aux(id bigint auto_increment primary key, "+tc.definition+")")
				defer exec(t, "drop table ai_aux")
				exec(t, "insert ignore into ai_aux(a,b) values "+tc.rows)
				require.Equal(t, [][]int64{{1, 1}, {2, 2}}, queryInt64Rows(t, "select id,b from ai_aux order by id", 2))
				require.Equal(t, int64(1), queryInt64(t, "select last_insert_id()"))
				// Lookup by the UK, not only a base-table scan, must see final PKs.
				require.Equal(t, [][]int64{{1, 1}}, queryInt64Rows(t, "select id,b from ai_aux where a='ab1' and b=1", 2))
				exec(t, "update ai_aux set b=3 where id=2")
				require.Equal(t, [][]int64{{1, 1}, {2, 3}}, queryInt64Rows(t, "select id,b from ai_aux order by id", 2))
				stmt, err := conn.PrepareContext(ctx, "insert ignore into ai_aux(a,b) values (?,?)")
				require.NoError(t, err)
				defer stmt.Close()
				_, err = stmt.ExecContext(ctx, "xy1", 4)
				require.NoError(t, err)
				_, err = stmt.ExecContext(ctx, "xy1", 4)
				require.NoError(t, err)
				require.Equal(t, int64(3), queryInt64(t, "select count(*) from ai_aux"))
			})
		}

		exec(t, "create table ai_replace(id bigint auto_increment primary key, uk int unique, v int)")
		exec(t, "insert into ai_replace(uk, v) values (10, 1)")
		exec(t, "replace into ai_replace(uk, v) values (10, 2)")
		require.Equal(t, [][]int64{{2, 10, 2}},
			queryInt64Rows(t, "select id, uk, v from ai_replace order by id", 3))
		require.Equal(t, int64(2), queryInt64(t, "select last_insert_id()"),
			"REPLACE must publish the generated replacement key")
		exec(t, "drop table ai_replace")

		// Two explicit CN endpoints: another writer owns a different range but
		// must honor its own series, independent of the creating session.
		exec(t, "set auto_increment_increment=3; set auto_increment_offset=2")
		exec(t, "create table ai_remote(id bigint auto_increment primary key, v int)")
		defer exec(t, "drop table ai_remote")
		exec(t, "set auto_increment_increment=1; set auto_increment_offset=1")
		exec(t, "insert into ai_remote(v) values (0)")
		require.Equal(t, int64(1), queryInt64(t, "select id from ai_remote"))
		cn1, err := c.GetCNService(1)
		require.NoError(t, err)
		other, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?multiStatements=true",
			cn1.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer other.Close()
		other.SetMaxOpenConns(1)
		_, err = other.ExecContext(ctx, fmt.Sprintf("use `%s`", dbName))
		require.NoError(t, err)
		_, err = other.ExecContext(ctx,
			"set auto_increment_increment=4; set auto_increment_offset=3; insert into ai_remote(v) values (1),(2),(3)")
		require.NoError(t, err)
		require.Equal(t, [][]int64{{3, 3, 8}}, queryInt64Rows(t,
			"select count(*), min(id)%4, max(id)-min(id) from ai_remote where v>0", 3))
		require.Equal(t, int64(4), queryInt64(t, "select count(distinct id) from ai_remote"))
	})
}
