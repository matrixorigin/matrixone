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
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
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
		t.Run("nested locking SELECTs can wait past the limit", func(t *testing.T) {
			testIssue33378LockingReads(t, ctx, db, conn)
		})
		t.Run("zero disables timeout and setting is session scoped", func(t *testing.T) {
			var limit int
			require.NoError(t, db.QueryRowContext(ctx, "select @@session.max_execution_time").Scan(&limit))
			require.Zero(t, limit)
			_, err := conn.ExecContext(ctx, "set session max_execution_time = 0")
			require.NoError(t, err)
			var result int
			require.NoError(t, conn.QueryRowContext(ctx, "select sleep(0.75)").Scan(&result))
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
				visible, err := queryVisibleInProcesslist(ctx, db, marker)
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

func testIssue33378LockingReads(t *testing.T, ctx context.Context, db *sql.DB, conn *sql.Conn) {
	t.Helper()
	_, err := db.ExecContext(ctx, "create database issue33378_locking")
	require.NoError(t, err)
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := db.ExecContext(cleanup, "drop database issue33378_locking")
		require.NoError(t, err)
	}()
	_, err = db.ExecContext(ctx, "create table issue33378_locking.t (id int primary key)")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "insert into issue33378_locking.t values (1)")
	require.NoError(t, err)

	var tableID string
	require.NoError(t, db.QueryRowContext(ctx,
		"select rel_id from mo_catalog.mo_tables where reldatabase='issue33378_locking' and relname='t'").Scan(&tableID))
	_, err = conn.ExecContext(ctx, "set session max_execution_time = 500")
	require.NoError(t, err)

	const lockingSelect = "select id from issue33378_locking.t where id=1 for update"
	for _, tc := range []struct{ name, query string }{
		{"derived table", "select id from (" + lockingSelect + ") as locked"},
		{"CTE", "with locked as (" + lockingSelect + ") select id from locked"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			holder, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer holder.Rollback()
			var value int
			require.NoError(t, holder.QueryRowContext(ctx, lockingSelect).Scan(&value))
			require.Equal(t, 1, value)

			queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				var id int
				err := conn.QueryRowContext(queryCtx, tc.query).Scan(&id)
				if err == nil && id != 1 {
					err = fmt.Errorf("locking read returned ID %d, want 1", id)
				}
				done <- err
			}()
			require.Eventually(t, func() bool {
				var waiting int
				err := db.QueryRowContext(queryCtx,
					"select count(*) from mo_catalog.mo_locks where table_id=? and lock_wait<>''",
					tableID).Scan(&waiting)
				return err == nil && waiting > 0
			}, 4*time.Second, 20*time.Millisecond, "locking SELECT must enter the row lock wait queue")
			// Hold the observed lock wait past the configured statement deadline.
			probe := time.NewTimer(750 * time.Millisecond)
			defer probe.Stop()
			select {
			case err := <-done:
				t.Fatalf("locking SELECT returned while the row lock was still held: %v", err)
			case <-probe.C:
			}
			require.NoError(t, holder.Rollback())
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-queryCtx.Done():
				t.Fatal("locking SELECT did not finish after releasing the row lock")
			}
		})
	}
}

func TestIssue33378MaxExecutionTimeWhileWaitingForTxnAdmission(t *testing.T) {
	cluster, err := embed.StartTestCluster(
		embed.WithCNCount(1),
		embed.WithPreStart(func(service embed.ServiceOperator) {
			if service.ServiceType() == metadata.ServiceType_CN {
				service.Adjust(func(config *embed.ServiceConfig) {
					config.CN.Txn.MaxActive = 1
					config.CN.Frontend.SkipCheckUser = false
				})
			}
		}),
	)
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	txnClient := cn.RawService().(cnservice.Service).GetTxnClient()
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	defer db.Close()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	var connectionID uint32
	require.NoError(t, conn.QueryRowContext(ctx, "select connection_id()").Scan(&connectionID))
	_, err = conn.ExecContext(ctx, "set session max_execution_time = 1200")
	require.NoError(t, err)

	holder, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer holder.Rollback()
	require.Equal(t, 1, txnClient.GetState().Users)

	queryCtx, cancelQuery := context.WithCancel(ctx)
	defer cancelQuery()
	done := make(chan error, 1)
	go func() {
		var result int
		done <- conn.QueryRowContext(queryCtx, "select 1").Scan(&result)
	}()
	require.Eventually(t, func() bool {
		return len(txnClient.GetState().WaitActiveTxns) > 0
	}, time.Second, 10*time.Millisecond, "SELECT must enter transaction admission")
	select {
	case err := <-done:
		var mysqlErr *mysql.MySQLError
		require.ErrorAs(t, err, &mysqlErr)
		require.Equal(t, uint16(moerr.ER_QUERY_TIMEOUT), mysqlErr.Number)
		require.Equal(t, [5]byte{'H', 'Y', '0', '0', '0'}, mysqlErr.SQLState)
	case <-time.After(4 * time.Second):
		t.Fatal("statement deadline did not interrupt admission with the slot still occupied")
	}
	require.Equal(t, 1, txnClient.GetState().Users)
	require.NoError(t, holder.Rollback())
	// The task service may also queue work on this CN. Wait for the holders
	// and any such work to drain before inspecting the recovered transaction.
	require.Eventually(t, func() bool {
		state := txnClient.GetState()
		return state.Users == 0 && len(state.WaitActiveTxns) == 0
	}, 5*time.Second, 20*time.Millisecond)

	// The recovered connection can create a long-lived transaction from a SELECT
	// with a short timer, then continue using that same transaction.
	_, err = conn.ExecContext(ctx, "set autocommit = 0")
	require.NoError(t, err)
	var reusedID uint32
	require.NoError(t, conn.QueryRowContext(ctx, "select connection_id()").Scan(&reusedID))
	require.Equal(t, connectionID, reusedID)
	userTxnID := func() string {
		var id string
		txnClient.IterTxns(func(txn client.TxnOverview) bool {
			if txn.UserTxn {
				id = string(txn.Meta.ID)
				return false
			}
			return true
		})
		return id
	}
	txnID := userTxnID()
	require.NotEmpty(t, txnID)
	var result int
	require.NoError(t, conn.QueryRowContext(ctx,
		"select count(*) from mo_catalog.mo_database where datname='mo_catalog'").Scan(&result))
	require.Equal(t, 1, result)
	require.Equal(t, txnID, userTxnID())
	_, err = conn.ExecContext(ctx, "commit")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		present := false
		txnClient.IterTxnIDs(func(id []byte) bool {
			if string(id) == txnID {
				present = true
				return false
			}
			return true
		})
		return !present
	}, time.Second, 10*time.Millisecond, "commit must release the recovered transaction")
}
