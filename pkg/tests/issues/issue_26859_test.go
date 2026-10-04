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
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

func TestIssue26859BinaryPreparedExplain(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port)
		db, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer db.Close()

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		dbName := testutils.GetDatabaseName(t)
		mustExec(t, ctx, conn, fmt.Sprintf("create database `%s`", dbName))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, _ = db.ExecContext(cleanupCtx, fmt.Sprintf("drop database if exists `%s`", dbName))
		}()
		mustExec(t, ctx, conn, fmt.Sprintf("use `%s`", dbName))
		mustExec(t, ctx, conn, "create table t (id bigint primary key, v bigint, key idx_v (v, id))")
		mustExec(t, ctx, conn, "insert into t values (1, 7), (2, 8), (3, 7)")

		selectStmt, err := conn.PrepareContext(ctx, "select id from t where v = ? order by id")
		require.NoError(t, err)
		defer selectStmt.Close()
		require.Equal(t, []int64{1, 3}, queryIssue26859IDs(t, ctx, selectStmt, 7))
		require.Equal(t, []int64{2}, queryIssue26859IDs(t, ctx, selectStmt, 8))

		explainStmt, err := conn.PrepareContext(ctx, "explain select id from t where v = ? order by id")
		require.NoError(t, err)
		defer explainStmt.Close()

		for _, value := range []int64{7, 8, 7} {
			planText := queryIssue26859Explain(t, ctx, explainStmt, value)
			require.Contains(t, planText, "Table Scan")
		}

		var one int
		require.NoError(t, conn.QueryRowContext(ctx, "select 1").Scan(&one))
		require.Equal(t, 1, one)
	})
}

func queryIssue26859Explain(t *testing.T, ctx context.Context, stmt *sql.Stmt, value int64) string {
	t.Helper()
	rows, err := stmt.QueryContext(ctx, value)
	require.NoError(t, err)
	defer rows.Close()

	columns, err := rows.Columns()
	require.NoError(t, err)
	require.Len(t, columns, 1)
	require.NotEmpty(t, columns[0])
	columnTypes, err := rows.ColumnTypes()
	require.NoError(t, err)
	require.Len(t, columnTypes, 1)
	require.Equal(t, "VARCHAR", columnTypes[0].DatabaseTypeName())

	var lines []string
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		lines = append(lines, line)
	}
	require.NoError(t, rows.Err())
	require.NotEmpty(t, lines)
	return strings.Join(lines, "\n")
}

func queryIssue26859IDs(t *testing.T, ctx context.Context, stmt *sql.Stmt, value int64) []int64 {
	t.Helper()
	rows, err := stmt.QueryContext(ctx, value)
	require.NoError(t, err)
	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var id int64
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())
	return ids
}
