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
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
)

// TestIssue29395DropDatabaseStatementRollback reaches the public SQL path after
// DROP DATABASE has dropped its only table, then cancels that statement. The
// surrounding transaction must remain usable without losing the table's
// AUTO_INCREMENT allocator on a later COMMIT.
func TestIssue29395DropDatabaseStatementRollback(t *testing.T) {
	faultEnabledHere := fault.Enable()
	if faultEnabledHere {
		defer fault.Disable()
	}

	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(4)

		guardDB := fmt.Sprintf("issue29395_guard_%d", time.Now().UnixNano())
		execSQLRequire(t, ctx, db, "create database "+guardDB)
		defer func() {
			cleanupCtx, stop := context.WithTimeout(context.Background(), 15*time.Second)
			defer stop()
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists "+guardDB)
		}()
		execSQLRequire(t, ctx, db, "create table "+guardDB+".guard (id int primary key)")

		for _, tc := range []struct {
			name      string
			terminal  string
			guardID   int
			guardRows int
		}{
			{name: "commit", terminal: "commit", guardID: 1, guardRows: 1},
			{name: "rollback", terminal: "rollback", guardID: 2, guardRows: 0},
		} {
			t.Run(tc.name, func(t *testing.T) {
				targetDB := fmt.Sprintf("issue29395_%s_%d", tc.name, time.Now().UnixNano())
				execSQLRequire(t, ctx, db, "create database "+targetDB)
				defer func() {
					cleanupCtx, stop := context.WithTimeout(context.Background(), 15*time.Second)
					defer stop()
					execSQLMaybe(t, cleanupCtx, db, "drop database if exists "+targetDB)
				}()
				table := targetDB + ".auto_table"
				execSQLRequire(t, ctx, db, "create table "+table+" (id bigint primary key auto_increment, v int)")
				execSQLRequire(t, ctx, db, "insert into "+table+" (v) values (1)")

				dropConn, err := db.Conn(ctx)
				require.NoError(t, err)
				defer dropConn.Close()
				observer, err := db.Conn(ctx)
				require.NoError(t, err)
				defer observer.Close()
				_, err = dropConn.ExecContext(ctx, "set mo_rollback_txn_on_error=0")
				require.NoError(t, err)
				_, err = dropConn.ExecContext(ctx, "begin")
				require.NoError(t, err)
				defer func() {
					cleanupCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
					defer stop()
					_, _ = dropConn.ExecContext(cleanupCtx, "rollback")
				}()
				_, err = dropConn.ExecContext(ctx, fmt.Sprintf("insert into %s.guard values (%d)", guardDB, tc.guardID))
				require.NoError(t, err)

				var connectionID int
				require.NoError(t, dropConn.QueryRowContext(ctx, "select connection_id()").Scan(&connectionID))
				const barrier = "drop_database_after_table"
				const waiters = "issue29395_drop_database_waiters"
				require.NoError(t, fault.AddFaultPoint(ctx, barrier, "1:1::", "wait", 0, "", false))
				defer func() { _, _ = fault.RemoveFaultPoint(context.Background(), barrier) }()
				require.NoError(t, fault.AddFaultPoint(ctx, waiters, ":::", "getwaiters", 0, barrier, false))
				defer func() { _, _ = fault.RemoveFaultPoint(context.Background(), waiters) }()

				dropDone := make(chan error, 1)
				go func() {
					_, err := dropConn.ExecContext(ctx, "drop database "+targetDB)
					dropDone <- err
				}()
				joined := false
				defer func() {
					if joined {
						return
					}
					cleanupCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
					defer stop()
					_, _ = observer.ExecContext(cleanupCtx, fmt.Sprintf("kill query %d", connectionID))
					_, _ = fault.RemoveFaultPoint(cleanupCtx, barrier)
					select {
					case <-dropDone:
					case <-cleanupCtx.Done():
						t.Error("canceled DROP DATABASE did not finish during cleanup")
					}
				}()

				require.Eventually(t, func() bool {
					count, _, ok := fault.TriggerFault(waiters)
					return ok && count == 1
				}, 20*time.Second, 10*time.Millisecond, "DROP DATABASE must reach the post-table barrier")
				_, err = observer.ExecContext(ctx, fmt.Sprintf("kill query %d", connectionID))
				require.NoError(t, err)
				select {
				case err = <-dropDone:
					joined = true
				case <-time.After(10 * time.Second):
					t.Fatal("canceled DROP DATABASE did not finish")
				}
				require.Error(t, err)
				_, err = dropConn.ExecContext(ctx, tc.terminal)
				require.NoError(t, err)

				var guardRows int
				require.NoError(t, db.QueryRowContext(ctx,
					fmt.Sprintf("select count(*) from %s.guard where id=%d", guardDB, tc.guardID)).Scan(&guardRows))
				require.Equal(t, tc.guardRows, guardRows)
				var firstID int64
				require.NoError(t, db.QueryRowContext(ctx, "select id from "+table+" where v=1").Scan(&firstID))
				_, err = db.ExecContext(ctx, "insert into "+table+" (v) values (2)")
				require.NoError(t, err)
				var secondID int64
				require.NoError(t, db.QueryRowContext(ctx, "select id from "+table+" where v=2").Scan(&secondID))
				require.Greater(t, secondID, firstID)

				// With the fault removed, the ordinary successful DROP still works.
				_, err = fault.RemoveFaultPoint(ctx, barrier)
				require.NoError(t, err)
				_, err = db.ExecContext(ctx, "drop database "+targetDB)
				require.NoError(t, err)
			})
		}
	})
}
