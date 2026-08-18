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
			func() {
				stmt, err := conn.PrepareContext(ctx, query)
				require.NoError(t, err)
				defer stmt.Close()
				rows, err := stmt.QueryContext(ctx, int64(1))
				require.NoError(t, err, query)
				defer rows.Close()
				for rows.Next() {
				}
				require.NoError(t, rows.Err())
			}()
		}
		metadataStmt, err := conn.PrepareContext(ctx,
			"select coalesce(?, cast(2 as decimal(10,2)))")
		require.NoError(t, err)
		defer metadataStmt.Close()
		for _, arg := range []any{nil, "1.25"} {
			func() {
				rows, err := metadataStmt.QueryContext(ctx, arg)
				require.NoError(t, err)
				defer rows.Close()
				require.NoError(t, rows.Err())
			}()
		}
		var widenedDatabaseType string
		var widenedScanType any
		func() {
			floatRows, err := metadataStmt.QueryContext(ctx, float64(1.5))
			require.NoError(t, err)
			defer floatRows.Close()
			floatColumns, err := floatRows.ColumnTypes()
			require.NoError(t, err)
			require.Len(t, floatColumns, 1)
			widenedDatabaseType = floatColumns[0].DatabaseTypeName()
			widenedScanType = floatColumns[0].ScanType()
			require.NoError(t, floatRows.Err())
		}()
		for _, arg := range []any{int64(1), nil} {
			func() {
				rows, err := metadataStmt.QueryContext(ctx, arg)
				require.NoError(t, err)
				defer rows.Close()
				columns, err := rows.ColumnTypes()
				require.NoError(t, err)
				require.Len(t, columns, 1)
				require.Equal(t, widenedDatabaseType, columns[0].DatabaseTypeName())
				require.Equal(t, widenedScanType, columns[0].ScanType())
				require.NoError(t, rows.Err())
			}()
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
			func() {
				stmt, err := conn.PrepareContext(ctx, query)
				require.NoError(t, err)
				defer stmt.Close()
				require.Equal(t, []int{1, 2, 3},
					queryIssue27088IDs(t, ctx, stmt, float64(9007199254740992)), query)
			}()
		}
		mustExec(t, ctx, conn, "create table arithmetic_update as select * from t")
		arithmeticUpdateStmt, err := conn.PrepareContext(ctx,
			"update arithmetic_update set id=id+10 where d in (?+0)")
		require.NoError(t, err)
		defer arithmeticUpdateStmt.Close()
		arithmeticResult, err := arithmeticUpdateStmt.ExecContext(ctx, float64(9007199254740992))
		require.NoError(t, err)
		arithmeticAffected, err := arithmeticResult.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(3), arithmeticAffected)

		singleInStmt, err := conn.PrepareContext(ctx, "select id from t where d in (?) order by id")
		require.NoError(t, err)
		defer singleInStmt.Close()
		require.Empty(t, queryIssue27088IDs(t, ctx, singleInStmt, float64(9007199254740992)))
		singleUpdateStmt, err := conn.PrepareContext(ctx, "update t set id=id+100 where d in (?)")
		require.NoError(t, err)
		defer singleUpdateStmt.Close()
		singleUpdateResult, err := singleUpdateStmt.ExecContext(ctx, float64(9007199254740992))
		require.NoError(t, err)
		singleAffected, err := singleUpdateResult.RowsAffected()
		require.NoError(t, err)
		require.Zero(t, singleAffected)
		singleCTASStmt, err := conn.PrepareContext(ctx,
			"create table single_selected as select id from t where d in (?)")
		require.NoError(t, err)
		defer singleCTASStmt.Close()
		_, err = singleCTASStmt.ExecContext(ctx, float64(9007199254740992))
		require.NoError(t, err)
		require.Empty(t, queryIssue27088QueryIDs(t, ctx, conn,
			"select id from single_selected order by id"))

		ctasStmt, err := conn.PrepareContext(ctx,
			"create table selected as select id from t where d in (?,?)")
		require.NoError(t, err)
		defer ctasStmt.Close()
		_, err = ctasStmt.ExecContext(ctx, exact, float64(0))
		require.NoError(t, err)
		require.Equal(t, []int{2}, queryIssue27088QueryIDs(t, ctx, conn,
			"select id from selected order by id"))

		updateStmt, err := conn.PrepareContext(ctx, "update t set id=id+10 where d in (?,?)")
		require.NoError(t, err)
		defer updateStmt.Close()
		result, err := updateStmt.ExecContext(ctx, exact, float64(0))
		require.NoError(t, err)
		affected, err := result.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), affected)
		require.Equal(t, []int{1, 3, 4, 5, 6, 12}, queryIssue27088QueryIDs(t, ctx, conn,
			"select id from t order by id"))

		mustExec(t, ctx, conn, "create table fractional (id int primary key, d decimal(20,4))")
		mustExec(t, ctx, conn, "insert into fractional values (1,1.2500),(2,-1.2500),(3,1.0000)")
		for _, query := range []string{
			"select id from fractional where d=?+0",
			"select id from fractional where d=?-0",
			"select id from fractional where d=?*1",
			"select id from fractional where d=?/1",
		} {
			func() {
				stmt, err := conn.PrepareContext(ctx, query)
				require.NoError(t, err)
				defer stmt.Close()
				require.Equal(t, []int{1}, queryIssue27088IDs(t, ctx, stmt, float64(1.25)), query)
			}()
		}
		mustExec(t, ctx, conn, "create table fractional_update as select * from fractional")
		fractionalUpdate, err := conn.PrepareContext(ctx,
			"update fractional_update set id=id+10 where d=?+0")
		require.NoError(t, err)
		defer fractionalUpdate.Close()
		result, err = fractionalUpdate.ExecContext(ctx, float64(1.25))
		require.NoError(t, err)
		affected, err = result.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), affected)
		require.Equal(t, []int{2, 3, 11}, queryIssue27088QueryIDs(t, ctx, conn,
			"select id from fractional_update order by id"))
		mustExec(t, ctx, conn, "create table fractional_delete as select * from fractional")
		fractionalDelete, err := conn.PrepareContext(ctx, "delete from fractional_delete where d=?+0")
		require.NoError(t, err)
		defer fractionalDelete.Close()
		result, err = fractionalDelete.ExecContext(ctx, float64(1.25))
		require.NoError(t, err)
		affected, err = result.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), affected)
		require.Equal(t, []int{2, 3}, queryIssue27088QueryIDs(t, ctx, conn,
			"select id from fractional_delete order by id"))

		mustExec(t, ctx, conn, "create table exact_compare (id int primary key, d decimal(65,30))")
		mustExec(t, ctx, conn, "insert into exact_compare values (1,0),"+
			"(2,12345678901234567890123456789012345.123456789012345678901234567890)")
		exactCompare, err := conn.PrepareContext(ctx, "select id from exact_compare where d=?")
		require.NoError(t, err)
		defer exactCompare.Close()
		require.Empty(t, queryIssue27088IDs(t, ctx, exactCompare, "1e-77"))
		require.Empty(t, queryIssue27088IDs(t, ctx, exactCompare,
			"12345678901234567890123456789012345.123456789012345678901234567890123456789012"))
		for _, value := range []string{"1e-38", "1e-43"} {
			for name, query := range map[string]string{
				"column_less":   "select id from exact_compare where d < ? order by id",
				"param_greater": "select id from exact_compare where ? > d order by id",
				"between":       "select id from exact_compare where ? between d and cast(1 as decimal(65,30)) order by id",
			} {
				t.Run(value+"/"+name, func(t *testing.T) {
					stmt, err := conn.PrepareContext(ctx, query)
					require.NoError(t, err)
					defer stmt.Close()
					require.Equal(t, []int{1}, queryIssue27088IDs(t, ctx, stmt, value), query)
				})
			}
		}
		wideCommon, err := conn.PrepareContext(ctx, "select coalesce(?, cast(2 as decimal(1,0)))")
		require.NoError(t, err)
		defer wideCommon.Close()
		row := wideCommon.QueryRowContext(ctx,
			"123456789012345678901234567890123456.12345678901234567890123456789012345678901")
		var wideValue string
		require.NoError(t, row.Scan(&wideValue))
		require.Equal(t,
			"123456789012345678901234567890123456.123456789012345678901234567890", wideValue)

		mustExec(t, ctx, conn, "create table volatile_bounds(lo decimal(20,0), hi decimal(20,0))")
		mustExec(t, ctx, conn, "insert into volatile_bounds values (1,1)")
		for _, test := range []struct {
			query string
			args  []any
		}{
			{"select (? + cast(nextval('volatile_seq') as decimal(20,0))) between lo and hi from volatile_bounds", []any{"0"}},
			{"select (? + cast(nextval('volatile_seq') as decimal(20,0))) not between hi + 1 and hi + 1 from volatile_bounds", []any{"0"}},
			{"select (? + cast(nextval('volatile_seq') as decimal(20,0))) in (?, ?) from volatile_bounds", []any{"0", "1", "2"}},
			{"select (? + cast(nextval('volatile_seq') as decimal(20,0))) not in (?, ?) from volatile_bounds", []any{"0", "2", "3"}},
		} {
			mustExec(t, ctx, conn, "drop sequence if exists volatile_seq")
			mustExec(t, ctx, conn, "create sequence volatile_seq increment 1 start with 1 no cycle")
			stmt, err := conn.PrepareContext(ctx, test.query)
			require.NoError(t, err)
			var matched bool
			require.NoError(t, stmt.QueryRowContext(ctx, test.args...).Scan(&matched), test.query)
			require.NoError(t, stmt.Close())
			require.True(t, matched, test.query)
			var current int64
			require.NoError(t, conn.QueryRowContext(ctx, "select currval('volatile_seq')").Scan(&current))
			require.Equal(t, int64(1), current, test.query)
		}
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
