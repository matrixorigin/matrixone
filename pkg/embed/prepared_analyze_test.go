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

package embed

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPreparedAnalyzeOverMySQLProtocol(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port)
		db, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer db.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		exec := func(statement string) {
			_, execErr := conn.ExecContext(ctx, statement)
			require.NoError(t, execErr, statement)
		}
		prepare := func(statement string) *sql.Stmt {
			stmt, prepareErr := conn.PrepareContext(ctx, statement)
			require.NoError(t, prepareErr, statement)
			return stmt
		}
		assertResultSet := func(rows *sql.Rows, expectedColumns []string, expectedValues []int64) {
			columns, columnsErr := rows.Columns()
			require.NoError(t, columnsErr)
			require.Equal(t, expectedColumns, columns)
			require.Len(t, expectedValues, len(columns))
			dest := make([]any, len(expectedValues))
			actual := make([]int64, len(expectedValues))
			for i := range actual {
				dest[i] = &actual[i]
			}
			require.True(t, rows.Next())
			require.NoError(t, rows.Scan(dest...))
			require.Equal(t, expectedValues, actual)
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
		}
		querySingle := func(stmt *sql.Stmt, expectedColumns []string, expectedValues []int64) {
			rows, queryErr := stmt.QueryContext(ctx)
			require.NoError(t, queryErr)
			defer rows.Close()
			assertResultSet(rows, expectedColumns, expectedValues)
			require.False(t, rows.NextResultSet())
			require.NoError(t, rows.Err())
		}

		exec("drop database if exists prepared_analyze_test")
		exec("drop database if exists prepared_analyze_other")
		exec("create database prepared_analyze_test")
		exec("create database prepared_analyze_other")
		defer conn.ExecContext(context.Background(), "drop database if exists prepared_analyze_test")
		defer conn.ExecContext(context.Background(), "drop database if exists prepared_analyze_other")
		exec("use prepared_analyze_test")
		exec("create table t (a int, b varchar(20))")
		exec("insert into t values (1, 'x'), (1, 'y'), (2, 'y')")

		explicit := prepare("analyze table t(a, b)")
		defer explicit.Close()
		for range 2 {
			querySingle(explicit,
				[]string{"approx_count_distinct(a)", "approx_count_distinct(b)"},
				[]int64{2, 2})
		}

		implicit := prepare("analyze table t")
		defer implicit.Close()
		querySingle(implicit,
			[]string{"approx_count_distinct(a)", "approx_count_distinct(b)"},
			[]int64{2, 2})

		multi := prepare("analyze table t(a), t(b, a)")
		defer multi.Close()
		rows, err := multi.QueryContext(ctx)
		require.NoError(t, err)
		assertResultSet(rows, []string{"approx_count_distinct(a)"}, []int64{2})
		require.True(t, rows.NextResultSet())
		assertResultSet(rows,
			[]string{"approx_count_distinct(b)", "approx_count_distinct(a)"},
			[]int64{2, 2})
		require.False(t, rows.NextResultSet())
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())

		exec("use prepared_analyze_other")
		exec("create table t (a int, b varchar(20))")
		exec("insert into t values (9, 'other'), (9, 'other')")
		querySingle(explicit,
			[]string{"approx_count_distinct(a)", "approx_count_distinct(b)"},
			[]int64{2, 2})
		var currentDatabase string
		require.NoError(t, conn.QueryRowContext(ctx, "select database()").Scan(&currentDatabase))
		require.Equal(t, "prepared_analyze_other", currentDatabase)
		exec("use prepared_analyze_test")

		exec("create table drift (x int)")
		exec("insert into drift values (1), (2)")
		drift := prepare("analyze table drift")
		defer drift.Close()
		exec("alter table drift add column y int")
		exec("update drift set y = x")
		querySingle(drift,
			[]string{"approx_count_distinct(x)", "approx_count_distinct(y)"},
			[]int64{2, 2})
		exec("drop table drift")
		exec("create table drift (x int)")
		exec("insert into drift values (7), (7)")
		querySingle(drift, []string{"approx_count_distinct(x)"}, []int64{1})

		exec("begin")
		exec("insert into t values (3, 'z')")
		querySingle(explicit,
			[]string{"approx_count_distinct(a)", "approx_count_distinct(b)"},
			[]int64{3, 3})
		exec("rollback")

		exec("create table later_missing (v int)")
		exec("insert into later_missing values (1)")
		failsAfterPrepare := prepare("analyze table t(a), later_missing(v)")
		defer failsAfterPrepare.Close()
		exec("drop table later_missing")
		failedRows, queryErr := failsAfterPrepare.QueryContext(ctx)
		if failedRows != nil {
			failedRows.Close()
		}
		require.ErrorContains(t, queryErr, "no such table prepared_analyze_test.later_missing")
		var stillUsable int
		require.NoError(t, conn.QueryRowContext(ctx, "select 7").Scan(&stillUsable))
		require.Equal(t, 7, stillUsable)

		exec("drop user if exists prepared_analyze_user")
		exec("drop role if exists prepared_analyze_role")
		exec("create role prepared_analyze_role")
		exec("grant connect on account * to prepared_analyze_role")
		exec("grant select on table prepared_analyze_test.t to prepared_analyze_role")
		exec("create user prepared_analyze_user identified by '111' default role prepared_analyze_role")
		defer conn.ExecContext(context.Background(), "drop user if exists prepared_analyze_user")
		defer conn.ExecContext(context.Background(), "drop role if exists prepared_analyze_role")

		userDSN := fmt.Sprintf("prepared_analyze_user:111@tcp(127.0.0.1:%d)/prepared_analyze_test",
			cn.GetServiceConfig().CN.Frontend.Port)
		userDB, err := sql.Open("mysql", userDSN)
		require.NoError(t, err)
		defer userDB.Close()
		userConn, err := userDB.Conn(ctx)
		require.NoError(t, err)
		defer userConn.Close()
		userStmt, err := userConn.PrepareContext(ctx, "analyze table t(a)")
		require.NoError(t, err)
		defer userStmt.Close()
		exec("revoke select on table prepared_analyze_test.t from prepared_analyze_role")
		unauthorizedRows, queryErr := userStmt.QueryContext(ctx)
		if unauthorizedRows != nil {
			unauthorizedRows.Close()
		}
		require.Error(t, queryErr)
	})
}
