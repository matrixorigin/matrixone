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
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	lockpb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// #29903: in one COM_QUERY, statement 1 generates an id and statement 2 inserts
// an explicit id, waits on a row lock and is retried after the holder commits.
// LAST_INSERT_ID keeps statement 1's id and statement 2's OK packet reports 0.
func TestIssue29903RetryKeepsEarlierStatementLastInsertID(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?multiStatements=true", cn.GetServiceConfig().CN.Frontend.Port)
		sqlDB, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer sqlDB.Close()
		writer, err := sqlDB.Conn(ctx)
		require.NoError(t, err)
		defer writer.Close()
		holder, err := sqlDB.Conn(ctx)
		require.NoError(t, err)
		defer holder.Close()

		dbName := strings.ToLower(testutils.GetDatabaseName(t))
		exec := func(conn *sql.Conn, statement string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement)
			require.NoErrorf(t, err, "SQL: %s", statement)
		}
		exec(writer, "create database "+dbName)
		defer func() {
			dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer dropCancel()
			_, _ = sqlDB.ExecContext(dropCtx, "drop database if exists "+dbName)
		}()
		exec(writer, "use "+dbName)
		exec(holder, "use "+dbName)
		exec(writer, "create table t(id bigint auto_increment primary key, v int)")
		exec(writer, "create table t2(id bigint auto_increment primary key, v int)")
		exec(writer, "insert into t2 values (100, 1)")

		var t2ID uint64
		require.NoError(t, writer.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase = ? and relname = 't2'",
			dbName).Scan(&t2ID))

		// conflict runs fn on writer while holder's uncommitted delete of t2 row
		// 100 blocks statement 2; the commit makes statement 2 retry.
		// On failure the holder rolls back and the writer is cancelled and drained.
		conflict := func(fn func(context.Context) error) {
			t.Helper()
			exec(holder, "begin")
			committed := false
			defer func() {
				if !committed {
					rollbackCtx, rollbackCancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer rollbackCancel()
					_, _ = holder.ExecContext(rollbackCtx, "rollback")
				}
			}()
			exec(holder, "delete from t2 where id = 100")
			retriesBefore := testutil.ToFloat64(v2.TxnStatementRetryCounter)
			writerCtx, writerCancel := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() { done <- fn(writerCtx) }()
			drained := false
			defer func() {
				writerCancel()
				if !drained {
					select {
					case <-done:
					case <-time.After(30 * time.Second):
					}
				}
			}()
			require.Eventually(t, func() bool {
				return issue29903LockWaiters(c, t2ID) > 0
			}, 30*time.Second, 10*time.Millisecond, "statement 2 never waited on the t2 row lock")
			exec(holder, "commit")
			committed = true
			select {
			case err := <-done:
				drained = true
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal("multi-statement COM_QUERY did not finish")
			}
			require.GreaterOrEqual(t, testutil.ToFloat64(v2.TxnStatementRetryCounter), retriesBefore+1)
		}

		// OK packets: statement 1 reports its id, the retried statement 2 reports 0.
		var ids []int64
		conflict(func(ctx context.Context) error {
			return writer.Raw(func(dc any) error {
				res, err := dc.(driver.ExecerContext).ExecContext(ctx,
					"insert into t(v) values (1); insert into t2 values (100, 2)", nil)
				if err != nil {
					return err
				}
				ids = res.(mysqlDriver.Result).AllLastInsertIds()
				return nil
			})
		})
		require.Equal(t, []int64{1, 0}, ids)

		// LAST_INSERT_ID() in the same COM_QUERY, after the retried statement 2.
		var lastInsertID int64
		conflict(func(ctx context.Context) error {
			rows, err := writer.QueryContext(ctx,
				"insert into t(v) values (2); insert into t2 values (100, 3); select last_insert_id()")
			if err != nil {
				return err
			}
			defer rows.Close()
			for {
				for rows.Next() {
					if err := rows.Scan(&lastInsertID); err != nil {
						return err
					}
				}
				if !rows.NextResultSet() {
					break
				}
			}
			return rows.Err()
		})
		require.Equal(t, int64(2), lastInsertID)

		var v int
		require.NoError(t, writer.QueryRowContext(ctx, "select v from t2 where id = 100").Scan(&v))
		require.Equal(t, 3, v)
	})
}

func issue29903LockWaiters(c embed.Cluster, tableID uint64) int {
	waiters := 0
	c.ForeachServices(func(svc embed.ServiceOperator) bool {
		if svc.ServiceType() != metadata.ServiceType_CN {
			return true
		}
		lockservice.GetLockServiceByServiceID(svc.ServiceID()).IterLocks(
			func(lockedTableID uint64, _ [][]byte, lock lockservice.Lock) bool {
				if lockedTableID == tableID {
					lock.IterWaiters(func(lockpb.WaitTxn) bool {
						waiters++
						return true
					})
				}
				return true
			})
		return true
	})
	return waiters
}
