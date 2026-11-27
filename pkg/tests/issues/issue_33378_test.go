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

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestIssue33378MaxExecutionTime(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port)
		db, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		var connID uint32
		require.NoError(t, conn.QueryRowContext(ctx, "select connection_id()").Scan(&connID))
		requireSameConnection := func(t *testing.T) {
			t.Helper()
			var current uint32
			require.NoError(t, conn.QueryRowContext(ctx, "select connection_id()").Scan(&current))
			require.Equal(t, connID, current)
		}
		requireTimeout := func(t *testing.T, query *sql.Row) {
			t.Helper()
			var result int
			var mysqlErr *mysql.MySQLError
			require.ErrorAs(t, query.Scan(&result), &mysqlErr)
			require.Equal(t, uint16(moerr.ER_QUERY_TIMEOUT), mysqlErr.Number)
			require.Equal(t, [5]byte{'H', 'Y', '0', '0', '0'}, mysqlErr.SQLState)
			requireSameConnection(t)
		}

		_, err = conn.ExecContext(ctx, "set session max_execution_time = 250")
		require.NoError(t, err)
		t.Run("ordinary SELECT", func(t *testing.T) {
			start := time.Now()
			requireTimeout(t, conn.QueryRowContext(ctx, "select sleep(5)"))
			require.Less(t, time.Since(start), 3*time.Second)
		})
		t.Run("binary prepared SELECT", func(t *testing.T) {
			stmt, err := conn.PrepareContext(ctx, "select sleep(?)")
			require.NoError(t, err)
			defer stmt.Close()

			requireTimeout(t, stmt.QueryRowContext(ctx, 5))
			var result int
			require.NoError(t, stmt.QueryRowContext(ctx, 0).Scan(&result))
			require.Zero(t, result)
		})
		t.Run("SQL prepared SELECT in a transaction", func(t *testing.T) {
			_, err := conn.ExecContext(ctx, "prepare timed_select from 'select sleep(5)'")
			require.NoError(t, err)
			tx, err := conn.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer tx.Rollback()

			var result int
			var mysqlErr *mysql.MySQLError
			require.ErrorAs(t, tx.QueryRowContext(ctx, "execute timed_select").Scan(&result), &mysqlErr)
			require.Equal(t, uint16(moerr.ER_QUERY_TIMEOUT), mysqlErr.Number)
			require.NoError(t, tx.QueryRowContext(ctx, "select 1").Scan(&result))
			require.Equal(t, 1, result)
			require.NoError(t, tx.Rollback())
			requireSameConnection(t)
		})
		t.Run("zero disables timeout and setting is session scoped", func(t *testing.T) {
			var limit int
			require.NoError(t, db.QueryRowContext(ctx, "select @@session.max_execution_time").Scan(&limit))
			require.Zero(t, limit)
			_, err := conn.ExecContext(ctx, "set session max_execution_time = 0")
			require.NoError(t, err)
			var result int
			require.NoError(t, conn.QueryRowContext(ctx, "select sleep(0.5)").Scan(&result))
			require.Zero(t, result)
		})
		t.Run("client KILL QUERY", func(t *testing.T) {
			_, err := conn.ExecContext(ctx, "set session max_execution_time = 15000")
			require.NoError(t, err)
			const marker = "33378001"
			done := make(chan error, 1)
			go func() {
				var result, markerValue int
				done <- conn.QueryRowContext(ctx, "select sleep(30), "+marker).Scan(&result, &markerValue)
			}()
			require.Eventually(t, func() bool {
				visible, err := issue32362QueryVisible(ctx, db, marker)
				return err == nil && visible
			}, 5*time.Second, 20*time.Millisecond)
			_, err = db.ExecContext(ctx, fmt.Sprintf("kill query %d", connID))
			require.NoError(t, err)
			select {
			case err := <-done:
				var mysqlErr *mysql.MySQLError
				require.ErrorAs(t, err, &mysqlErr)
				require.NotEqual(t, uint16(moerr.ER_QUERY_TIMEOUT), mysqlErr.Number)
			case <-time.After(5 * time.Second):
				t.Fatal("KILL QUERY did not interrupt the SELECT")
			}
			requireSameConnection(t)
		})
	})
}
