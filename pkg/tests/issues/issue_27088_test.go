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
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue27088BinaryPreparedINPreservesWireDomains(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port))
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
		mustExec(t, ctx, conn, "create table t (id int primary key, d decimal(38,10))")
		mustExec(t, ctx, conn, `insert into t values
			(1,9007199254740992.0000000001),(2,9007199254740992.0000000002),
			(3,9007199254740992.0000000003),(4,9007199254740994.0000000001),
			(5,9007199254740995.0000000001),(6,9007199254740996.0000000001)`)
		for _, query := range []string{
			"select nth_value(id, ?) over (order by id) from t",
			"explain select nth_value(id, ?) over (order by id) from t",
		} {
			stmt, err := conn.PrepareContext(ctx, query)
			require.NoError(t, err)
			rows, err := stmt.QueryContext(ctx, int64(1))
			require.NoError(t, err, query)
			for rows.Next() {
			}
			require.NoError(t, rows.Err())
			require.NoError(t, rows.Close())
			require.NoError(t, stmt.Close())
		}
		metadataStmt, err := conn.PrepareContext(ctx,
			"select coalesce(?, cast(2 as decimal(10,2)))")
		require.NoError(t, err)
		defer metadataStmt.Close()
		for _, arg := range []any{nil, "1.25"} {
			rows, err := metadataStmt.QueryContext(ctx, arg)
			require.NoError(t, err)
			require.NoError(t, rows.Close())
		}
		floatRows, err := metadataStmt.QueryContext(ctx, float64(1.5))
		require.NoError(t, err)
		floatColumns, err := floatRows.ColumnTypes()
		require.NoError(t, err)
		require.Len(t, floatColumns, 1)
		widenedDatabaseType := floatColumns[0].DatabaseTypeName()
		widenedScanType := floatColumns[0].ScanType()
		require.NoError(t, floatRows.Close())
		for _, arg := range []any{int64(1), nil} {
			rows, err := metadataStmt.QueryContext(ctx, arg)
			require.NoError(t, err)
			columns, err := rows.ColumnTypes()
			require.NoError(t, err)
			require.Len(t, columns, 1)
			require.Equal(t, widenedDatabaseType, columns[0].DatabaseTypeName())
			require.Equal(t, widenedScanType, columns[0].ScanType())
			require.NoError(t, rows.Close())
		}

		inStmt, err := conn.PrepareContext(ctx, "select id from t where d in (?,?) order by id")
		require.NoError(t, err)
		defer inStmt.Close()
		notInStmt, err := conn.PrepareContext(ctx, "select id from t where d not in (?,?) order by id")
		require.NoError(t, err)
		defer notInStmt.Close()

		exact := "9007199254740992.0000000002"
		require.Equal(t, []int{2}, queryIssue27088IDs(t, ctx, inStmt, exact, float64(0)))
		require.Equal(t, []int{1, 3, 4, 5, 6},
			queryIssue27088IDs(t, ctx, notInStmt, exact, float64(0)))
		require.Equal(t, []int{1, 2, 3},
			queryIssue27088IDs(t, ctx, inStmt, float64(9007199254740992), "0"))

		for _, query := range []string{
			"select id from t where d = ?+0 order by id",
			"select id from t where d = ?-0 order by id",
			"select id from t where d = ?*1 order by id",
			"select id from t where d = ?/1 order by id",
			"select id from t where d in (?+0) order by id",
		} {
			stmt, err := conn.PrepareContext(ctx, query)
			require.NoError(t, err)
			require.Equal(t, []int{1, 2, 3},
				queryIssue27088IDs(t, ctx, stmt, float64(9007199254740992)), query)
			require.NoError(t, stmt.Close())
		}
		mustExec(t, ctx, conn, "create table arithmetic_update as select * from t")
		arithmeticUpdateStmt, err := conn.PrepareContext(ctx,
			"update arithmetic_update set id=id+10 where d in (?+0)")
		require.NoError(t, err)
		arithmeticResult, err := arithmeticUpdateStmt.ExecContext(ctx, float64(9007199254740992))
		require.NoError(t, err)
		arithmeticAffected, err := arithmeticResult.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(3), arithmeticAffected)
		require.NoError(t, arithmeticUpdateStmt.Close())

		singleInStmt, err := conn.PrepareContext(ctx, "select id from t where d in (?) order by id")
		require.NoError(t, err)
		require.Empty(t, queryIssue27088IDs(t, ctx, singleInStmt, float64(9007199254740992)))
		require.NoError(t, singleInStmt.Close())
		singleUpdateStmt, err := conn.PrepareContext(ctx, "update t set id=id+100 where d in (?)")
		require.NoError(t, err)
		singleUpdateResult, err := singleUpdateStmt.ExecContext(ctx, float64(9007199254740992))
		require.NoError(t, err)
		singleAffected, err := singleUpdateResult.RowsAffected()
		require.NoError(t, err)
		require.Zero(t, singleAffected)
		require.NoError(t, singleUpdateStmt.Close())
		singleCTASStmt, err := conn.PrepareContext(ctx,
			"create table single_selected as select id from t where d in (?)")
		require.NoError(t, err)
		_, err = singleCTASStmt.ExecContext(ctx, float64(9007199254740992))
		require.NoError(t, err)
		require.NoError(t, singleCTASStmt.Close())
		require.Empty(t, queryIssue27088QueryIDs(t, ctx, conn,
			"select id from single_selected order by id"))

		ctasStmt, err := conn.PrepareContext(ctx,
			"create table selected as select id from t where d in (?,?)")
		require.NoError(t, err)
		_, err = ctasStmt.ExecContext(ctx, exact, float64(0))
		require.NoError(t, err)
		require.NoError(t, ctasStmt.Close())
		require.Equal(t, []int{2}, queryIssue27088QueryIDs(t, ctx, conn,
			"select id from selected order by id"))

		updateStmt, err := conn.PrepareContext(ctx, "update t set id=id+10 where d in (?,?)")
		require.NoError(t, err)
		result, err := updateStmt.ExecContext(ctx, exact, float64(0))
		require.NoError(t, err)
		affected, err := result.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), affected)
		require.NoError(t, updateStmt.Close())
		require.Equal(t, []int{1, 3, 4, 5, 6, 12}, queryIssue27088QueryIDs(t, ctx, conn,
			"select id from t order by id"))
	})
}

func queryIssue27088IDs(t *testing.T, ctx context.Context, stmt *sql.Stmt, args ...any) []int {
	t.Helper()
	rows, err := stmt.QueryContext(ctx, args...)
	require.NoError(t, err)
	defer rows.Close()
	return scanIssue27088IDs(t, rows)
}

func queryIssue27088QueryIDs(t *testing.T, ctx context.Context, conn *sql.Conn, query string) []int {
	t.Helper()
	rows, err := conn.QueryContext(ctx, query)
	require.NoError(t, err)
	defer rows.Close()
	return scanIssue27088IDs(t, rows)
}

func scanIssue27088IDs(t *testing.T, rows *sql.Rows) []int {
	t.Helper()
	var ids []int
	for rows.Next() {
		var id int
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())
	return ids
}
