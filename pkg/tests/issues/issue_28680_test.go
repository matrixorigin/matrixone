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
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

// TestIssue28680IgnoreConversionExecutionBoundaries exercises the execution
// boundaries that cannot be covered by planner-only tests:
//
//   - database/sql with interpolateParams=false uses COM_STMT_PREPARE and
//     COM_STMT_EXECUTE, and the same prepared statement is reused;
//   - INSERT ... SELECT converts real string columns at execution time;
//   - UPDATE IGNORE reports one warning per converted value, but does not
//     convert values when no target row matches;
//   - rollback and a non-IGNORE conversion error do not publish partial rows.
func TestIssue28680IgnoreConversionExecutionBoundaries(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false",
			cn.GetServiceConfig().CN.Frontend.Port,
		))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		dbName := testutils.GetDatabaseName(t)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, _ = conn.ExecContext(cleanupCtx, "rollback")
			_, _ = conn.ExecContext(cleanupCtx, "drop database if exists "+dbName)
		}()

		exec := func(statement string, args ...any) sql.Result {
			t.Helper()
			result, execErr := conn.ExecContext(ctx, statement, args...)
			require.NoError(t, execErr, statement)
			return result
		}
		queryRow := func(statement string, args ...any) *sql.Row {
			t.Helper()
			return conn.QueryRowContext(ctx, statement, args...)
		}

		exec("drop database if exists " + dbName)
		exec("create database " + dbName)
		exec("set sql_mode='STRICT_TRANS_TABLES'")
		exec("create table " + dbName + `.target (
			id int primary key,
			i tinyint,
			d decimal(10,2),
			dt date
		)`)

		// The same server-side prepared statement must apply IGNORE conversion
		// for each execution. The valid middle execution also catches stale
		// warning/parameter state from the first execution.
		insertStmt, err := conn.PrepareContext(ctx,
			"insert ignore into "+dbName+".target(id, i, d, dt) values (?, ?, ?, ?)")
		require.NoError(t, err)
		defer insertStmt.Close()

		_, err = insertStmt.ExecContext(ctx, 1, "abc", "abc", "2024-02-30")
		require.NoError(t, err)
		requireWarningCodes(t, ctx, conn, map[uint16]int{1264: 1, 1366: 2})

		_, err = insertStmt.ExecContext(ctx, 2, "12", "1.25", "2024-01-02")
		require.NoError(t, err)
		requireNoWarnings(t, ctx, conn)

		_, err = insertStmt.ExecContext(ctx, 3, "abc", "abc", "2024-02-30")
		require.NoError(t, err)
		requireWarningCodes(t, ctx, conn, map[uint16]int{1264: 1, 1366: 2})

		// Assignment rounding must use the complete decimal prefix, including
		// fractional and exponent components. Complete values must not inherit
		// truncation warnings from a neighboring prepared execution.
		_, err = insertStmt.ExecContext(ctx, 4, "12.9tail", "1.25", "2024-01-03")
		require.NoError(t, err)
		requireWarningCodes(t, ctx, conn, map[uint16]int{1265: 1})
		_, err = insertStmt.ExecContext(ctx, 5, "12.9", "1.25", "2024-01-04")
		require.NoError(t, err)
		requireNoWarnings(t, ctx, conn)
		_, err = insertStmt.ExecContext(ctx, 6, "12.9tail", "1.25", "2024-01-05")
		require.NoError(t, err)
		requireWarningCodes(t, ctx, conn, map[uint16]int{1265: 1})
		_, err = insertStmt.ExecContext(ctx, 7, "1e2", "1.25", "2024-01-06")
		require.NoError(t, err)
		requireNoWarnings(t, ctx, conn)
		_, err = insertStmt.ExecContext(ctx, 8, "1e2tail", "1.25", "2024-01-07")
		require.NoError(t, err)
		requireWarningCodes(t, ctx, conn, map[uint16]int{1265: 1})

		rows, err := conn.QueryContext(ctx,
			"select id, i, cast(d as char), cast(dt as char) from "+dbName+".target order by id")
		require.NoError(t, err)
		defer rows.Close()
		var got []string
		for rows.Next() {
			var id, i int
			var d, dt string
			require.NoError(t, rows.Scan(&id, &i, &d, &dt))
			got = append(got, fmt.Sprintf("%d/%d/%s/%s", id, i, d, dt))
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		require.Equal(t, []string{
			"1/0/0.00/0000-00-00",
			"2/12/1.25/2024-01-02",
			"3/0/0.00/0000-00-00",
			"4/13/1.25/2024-01-03",
			"5/13/1.25/2024-01-04",
			"6/13/1.25/2024-01-05",
			"7/100/1.25/2024-01-06",
			"8/100/1.25/2024-01-07",
		}, got)

		// UPDATE must convert once for every matching row. Reusing this prepared
		// statement with a zero-match predicate must not manufacture warnings.
		exec("update " + dbName + ".target set i=5, d=5.00, dt='2024-01-01' where id=1")
		exec("update " + dbName + ".target set i=6, d=6.00, dt='2024-01-02' where id=2")
		updateStmt, err := conn.PrepareContext(ctx,
			"update ignore "+dbName+".target set i=?, d=?, dt=? where id = ? or id = ?")
		require.NoError(t, err)
		defer updateStmt.Close()
		result, err := updateStmt.ExecContext(ctx, "abc", "abc", "2024-02-30", 1, 2)
		require.NoError(t, err)
		affected, err := result.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(2), affected)
		requireWarningCodes(t, ctx, conn, map[uint16]int{1264: 2, 1366: 4})

		result, err = updateStmt.ExecContext(ctx, "abc", "abc", "2024-02-30", 999, 1000)
		require.NoError(t, err)
		affected, err = result.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(0), affected)
		requireNoWarnings(t, ctx, conn)

		result, err = updateStmt.ExecContext(ctx, "7", "2.50", "2024-03-04", 1, 1)
		require.NoError(t, err)
		affected, err = result.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), affected)
		requireNoWarnings(t, ctx, conn)
		var i int
		var d, dt string
		require.NoError(t, queryRow(
			"select i, cast(d as char), cast(dt as char) from "+dbName+".target where id=1",
		).Scan(&i, &d, &dt))
		require.Equal(t, 7, i)
		require.Equal(t, "2.50", d)
		require.Equal(t, "2024-03-04", dt)

		result, err = updateStmt.ExecContext(ctx, "12.9tail", "3.25", "2024-03-05", 1, 1)
		require.NoError(t, err)
		affected, err = result.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), affected)
		requireWarningCodes(t, ctx, conn, map[uint16]int{1265: 1})
		require.NoError(t, queryRow(
			"select i, cast(d as char), cast(dt as char) from "+dbName+".target where id=1",
		).Scan(&i, &d, &dt))
		require.Equal(t, 13, i)
		require.Equal(t, "3.25", d)
		require.Equal(t, "2024-03-05", dt)

		// INSERT ... SELECT must reach the same runtime assignment boundary for
		// values coming from actual source columns, not only prepared markers.
		exec("create table " + dbName + `.source (
			id int primary key,
			i varchar(20),
			d varchar(20),
			dt varchar(20)
		)`)
		exec("insert into " + dbName + `.source values
			(10, 'abc', 'abc', '2024-02-30'),
			(11, '9', '3.25', '2024-04-05'),
			(12, '12.9tail', '4.50', '2024-04-06'),
			(13, '1e2tail', '5.50', '2024-04-07')`)
		exec("insert ignore into " + dbName + ".target(id, i, d, dt) select id, i, d, dt from " + dbName + ".source")
		requireWarningCodes(t, ctx, conn, map[uint16]int{1264: 1, 1265: 2, 1366: 2})
		require.NoError(t, queryRow(
			"select i, cast(d as char), cast(dt as char) from "+dbName+".target where id=10",
		).Scan(&i, &d, &dt))
		require.Equal(t, 0, i)
		require.Equal(t, "0.00", d)
		require.Equal(t, "0000-00-00", dt)
		require.NoError(t, queryRow(
			"select i, cast(d as char), cast(dt as char) from "+dbName+".target where id=11",
		).Scan(&i, &d, &dt))
		require.Equal(t, 9, i)
		require.Equal(t, "3.25", d)
		require.Equal(t, "2024-04-05", dt)
		require.NoError(t, queryRow(
			"select i, cast(d as char), cast(dt as char) from "+dbName+".target where id=12",
		).Scan(&i, &d, &dt))
		require.Equal(t, 13, i)
		require.Equal(t, "4.50", d)
		require.Equal(t, "2024-04-06", dt)
		require.NoError(t, queryRow(
			"select i, cast(d as char), cast(dt as char) from "+dbName+".target where id=13",
		).Scan(&i, &d, &dt))
		require.Equal(t, 100, i)
		require.Equal(t, "5.50", d)
		require.Equal(t, "2024-04-07", dt)

		// A successful IGNORE write followed by rollback must not become visible.
		tx, err := conn.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		_, err = tx.ExecContext(ctx,
			"insert ignore into "+dbName+".target values (50, 'abc', 'abc', '2024-02-30')")
		require.NoError(t, err)
		require.NoError(t, tx.Rollback())
		var count int
		require.NoError(t, queryRow(
			"select count(*) from "+dbName+".target where id=50",
		).Scan(&count))
		require.Equal(t, 0, count)

		// Non-IGNORE conversion errors must not publish the valid prefix of a
		// multi-row autocommit statement. Checking immediately after the error
		// keeps this an atomicity assertion instead of letting an explicit
		// rollback mask a partial write.
		_, err = conn.ExecContext(ctx, "insert into "+dbName+`.target values
			(60, 8, 4.00, '2024-05-06'),
			(61, 'abc', 'abc', '2024-02-30')`)
		require.Error(t, err)
		require.NoError(t, queryRow(
			"select count(*) from "+dbName+".target where id in (60,61)",
		).Scan(&count))
		require.Equal(t, 0, count)
	})
}

func requireNoWarnings(t *testing.T, ctx context.Context, conn *sql.Conn) {
	t.Helper()
	rows, err := conn.QueryContext(ctx, "show warnings")
	require.NoError(t, err)
	defer rows.Close()
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
}

func requireWarningCodes(t *testing.T, ctx context.Context, conn *sql.Conn, want map[uint16]int) {
	t.Helper()
	rows, err := conn.QueryContext(ctx, "show warnings")
	require.NoError(t, err)
	defer rows.Close()

	got := make(map[uint16]int)
	for rows.Next() {
		var level, message string
		var code uint16
		require.NoError(t, rows.Scan(&level, &code, &message))
		require.Equal(t, "Warning", level)
		got[code]++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, want, got)
}
