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
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	pblock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/stretchr/testify/require"
)

func TestIssue27040ConcurrentIfNotExistsDatabaseClone(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		// Keep one connection free for catalog assertions and cleanup while the
		// two clone sessions remain pinned to their transactions.
		db.SetMaxOpenConns(3)

		const (
			sourceDatabase = "issue_27040_clone_source"
			targetDatabase = "issue_27040_clone_target"
		)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists `"+targetDatabase+"`")
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists `"+sourceDatabase+"`")
		}()

		execSQLRequire(t, ctx, db, "set role moadmin")
		execSQLRequire(t, ctx, db, "create database `"+sourceDatabase+"`")
		execSQLRequire(t, ctx, db, "create table `"+sourceDatabase+"`.payload (id int primary key)")
		execSQLRequire(t, ctx, db, "insert into `"+sourceDatabase+"`.payload values (1)")

		var moDatabaseTableID uint64
		require.NoError(t, db.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where account_id = 0 and reldatabase = 'mo_catalog' and relname = 'mo_database'",
		).Scan(&moDatabaseTableID))

		first, err := db.Conn(ctx)
		require.NoError(t, err)
		defer first.Close()
		second, err := db.Conn(ctx)
		require.NoError(t, err)
		defer second.Close()
		execConn := func(conn *sql.Conn, statement string) error {
			_, execErr := conn.ExecContext(ctx, statement)
			return execErr
		}
		require.NoError(t, execConn(first, "set role moadmin"))
		require.NoError(t, execConn(second, "set role moadmin"))

		cloneSQL := "create database if not exists `" + targetDatabase + "` clone `" + sourceDatabase + "`"
		require.NoError(t, execConn(first, "begin"))
		firstOpen := true
		secondDone := make(chan error, 1)
		secondStarted := false
		secondPending := false
		defer func() {
			if firstOpen {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cleanupCancel()
				_, _ = first.ExecContext(cleanupCtx, "rollback")
			}
			if !secondStarted || !secondPending {
				return
			}
			select {
			case <-secondDone:
			case <-time.After(10 * time.Second):
			}
		}()
		require.NoError(t, execConn(first, cloneSQL))

		go func() {
			secondDone <- execConn(second, cloneSQL)
		}()
		secondStarted = true
		secondPending = true

		lockService := lockservice.GetLockServiceByServiceID(cn.ServiceID())
		require.Eventually(t, func() bool {
			waiting := false
			lockService.IterLocks(func(tableID uint64, _ [][]byte, lock lockservice.Lock) bool {
				if tableID != moDatabaseTableID {
					return true
				}
				lock.IterWaiters(func(_ pblock.WaitTxn) bool {
					waiting = true
					return false
				})
				return !waiting
			})
			return waiting
		}, 30*time.Second, 10*time.Millisecond,
			"second clone did not wait for the first clone's mo_database target lock")

		select {
		case cloneErr := <-secondDone:
			secondPending = false
			t.Fatalf("second clone returned before the first transaction committed: %v", cloneErr)
		default:
		}

		require.NoError(t, execConn(first, "commit"))
		firstOpen = false
		select {
		case cloneErr := <-secondDone:
			secondPending = false
			require.NoError(t, cloneErr)
		case <-time.After(30 * time.Second):
			t.Fatal("second clone did not return after the first transaction committed")
		}

		var targetCount int
		require.NoError(t, db.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_database where account_id = 0 and datname = ?", targetDatabase,
		).Scan(&targetCount))
		require.Equal(t, 1, targetCount)
		var payloadCount int
		require.NoError(t, db.QueryRowContext(ctx,
			"select count(*) from `"+targetDatabase+"`.payload",
		).Scan(&payloadCount))
		require.Equal(t, 1, payloadCount)
	})
}
