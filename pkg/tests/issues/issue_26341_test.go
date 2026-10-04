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
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	pblock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/stretchr/testify/require"
)

func TestIssue26341CancelAndRetryFulltextUpdate(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(4)

		const database = "issue_26341_cancel_retry"
		execSQLMaybe(t, ctx, db, "drop database if exists `"+database+"`")
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists `"+database+"`")
		}()
		execSQLRequire(t, ctx, db, "set experimental_fulltext_index = 1")
		execSQLRequire(t, ctx, db, "create database `"+database+"`")
		execSQLRequire(t, ctx, db,
			"create table `"+database+"`.`docs` (id bigint primary key, body text, fulltext ft_body(body))")
		execSQLRequire(t, ctx, db,
			"insert into `"+database+"`.`docs` values (1, 'canceloldtoken')")

		var tableID uint64
		require.NoError(t, db.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase = ? and relname = 'docs'", database).
			Scan(&tableID))
		var indexTable string
		require.NoError(t, db.QueryRowContext(ctx,
			"select index_table_name from mo_catalog.mo_indexes where table_id = ? and name = 'ft_body' limit 1", tableID).
			Scan(&indexTable))

		blocker, err := db.Conn(ctx)
		require.NoError(t, err)
		defer blocker.Close()
		contender, err := db.Conn(ctx)
		require.NoError(t, err)
		defer contender.Close()
		var contenderID uint64
		require.NoError(t, contender.QueryRowContext(ctx, "select connection_id()").Scan(&contenderID))

		execConn := func(conn *sql.Conn, statement string) error {
			_, execErr := conn.ExecContext(ctx, statement)
			return execErr
		}
		require.NoError(t, execConn(blocker, "begin"))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, _ = blocker.ExecContext(cleanupCtx, "rollback")
		}()
		require.NoError(t, execConn(blocker,
			"update `"+database+"`.`docs` set body = 'blockertoken' where id = 1"))

		updateCtx, cancelUpdate := context.WithCancel(ctx)
		defer cancelUpdate()
		updateDone := make(chan error, 1)
		go func() {
			_, updateErr := contender.ExecContext(updateCtx,
				"update `"+database+"`.`docs` set body = 'retrytoken' where id = 1")
			updateDone <- updateErr
		}()

		ls := lockservice.GetLockServiceByServiceID(cn.ServiceID())
		require.Eventually(t, func() bool {
			found := false
			ls.IterLocks(func(lockedTableID uint64, _ [][]byte, lock lockservice.Lock) bool {
				if lockedTableID != tableID {
					return true
				}
				lock.IterWaiters(func(_ pblock.WaitTxn) bool {
					found = true
					return false
				})
				return !found
			})
			return found
		}, 30*time.Second, 10*time.Millisecond, "UPDATE did not enter the row-lock wait queue")

		_, err = db.ExecContext(ctx, fmt.Sprintf("kill query %d", contenderID))
		require.NoError(t, err)
		select {
		case updateErr := <-updateDone:
			require.Error(t, updateErr)
		case <-time.After(30 * time.Second):
			t.Fatal("canceled UPDATE did not return")
		}
		cancelUpdate()
		require.NoError(t, execConn(blocker, "rollback"))

		assertTokenCount := func(token string, want int) {
			t.Helper()
			var count int
			require.NoError(t, db.QueryRowContext(ctx,
				fmt.Sprintf("select count(*) from `%s`.`%s` where word = ?", database, indexTable), token).
				Scan(&count))
			require.Equal(t, want, count, "unexpected hidden-index count for %s", token)
		}
		var body string
		require.NoError(t, db.QueryRowContext(ctx,
			"select body from `"+database+"`.`docs` where id = 1").Scan(&body))
		require.Equal(t, "canceloldtoken", body)
		assertTokenCount("canceloldtoken", 1)
		assertTokenCount("retrytoken", 0)

		// Retry the canceled statement with a fresh context. The first attempt must
		// have left no hidden rows, and the successful retry must maintain one copy.
		retryCtx, cancelRetry := context.WithTimeout(ctx, 30*time.Second)
		defer cancelRetry()
		_, err = db.ExecContext(retryCtx,
			"update `"+database+"`.`docs` set body = 'retrytoken' where id = 1")
		require.NoError(t, err)
		require.NoError(t, db.QueryRowContext(ctx,
			"select body from `"+database+"`.`docs` where id = 1").Scan(&body))
		require.Equal(t, "retrytoken", body)
		assertTokenCount("canceloldtoken", 0)
		assertTokenCount("retrytoken", 1)

		var matches int
		require.NoError(t, db.QueryRowContext(ctx,
			"select count(*) from `"+database+"`.`docs` where match(body) against('retrytoken')").Scan(&matches))
		require.Equal(t, 1, matches)
	})
}
