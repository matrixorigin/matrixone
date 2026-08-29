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
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/stretchr/testify/require"
)

func TestIssue27723GrantDropLifecycleLockProtocol(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?multiStatements=true", cn.GetServiceConfig().CN.Frontend.Port)
		db, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(6)

		const (
			database = "issue_27723_grant_drop"
			role     = "issue_27723_role"
		)
		execSQLMaybe(t, ctx, db, "drop database if exists `"+database+"`")
		execSQLMaybe(t, ctx, db, "drop role if exists `"+role+"`")
		defer execSQLMaybe(t, context.Background(), db, "drop role if exists `"+role+"`")
		defer execSQLMaybe(t, context.Background(), db, "drop database if exists `"+database+"`")
		execSQLRequire(t, ctx, db, "create role `"+role+"`")
		execSQLRequire(t, ctx, db, "create database `"+database+"`")

		lifecycleCatalogTables := make(map[uint64]struct{}, 2)
		rows, err := db.QueryContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase = 'mo_catalog' and relname in ('mo_database', 'mo_tables')")
		require.NoError(t, err)
		defer rows.Close()
		for rows.Next() {
			var tableID uint64
			require.NoError(t, rows.Scan(&tableID))
			lifecycleCatalogTables[tableID] = struct{}{}
		}
		require.NoError(t, rows.Err())
		require.Len(t, lifecycleCatalogTables, 2)

		execConn := func(conn *sql.Conn, statement string) {
			t.Helper()
			_, execErr := conn.ExecContext(ctx, statement)
			require.NoErrorf(t, execErr, "exec failed: %s", statement)
		}
		rollbackConn := func(conn *sql.Conn) {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, _ = conn.ExecContext(cleanupCtx, "rollback")
		}
		installWaiterBarrier := func(t *testing.T) <-chan struct{} {
			t.Helper()
			waiterQueued := make(chan struct{})
			var once sync.Once
			restore := lockservice.SetWaiterEnqueuedHookForTest(func(
				tableID uint64, waiterTxnID []byte, holderTxnIDs [][]byte,
			) {
				if _, ok := lifecycleCatalogTables[tableID]; ok &&
					len(waiterTxnID) > 0 && len(holderTxnIDs) > 0 {
					once.Do(func() { close(waiterQueued) })
				}
			})
			t.Cleanup(restore)
			return waiterQueued
		}
		waitForWaiter := func(t *testing.T, waiterQueued <-chan struct{}, message string) {
			t.Helper()
			select {
			case <-waiterQueued:
			case <-time.After(30 * time.Second):
				t.Fatal(message)
			}
		}

		t.Run("grant holder commits before waiting drop", func(t *testing.T) {
			execSQLRequire(t, ctx, db, "create table `"+database+"`.`t_grant_first`(id int)")

			grantConn, err := db.Conn(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, grantConn.Close()) })
			dropConn, err := db.Conn(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, dropConn.Close()) })

			grantLocked := make(chan struct{})
			releaseGrant := make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releaseGrant) }) }
			t.Cleanup(release)
			var grantLockedOnce sync.Once
			restoreGrantHook := frontend.SetGrantPrivilegeObjectLockedHookForTest(func() {
				grantLockedOnce.Do(func() { close(grantLocked) })
				<-releaseGrant
			})
			t.Cleanup(restoreGrantHook)
			waiterQueued := installWaiterBarrier(t)

			grantDone := make(chan error, 1)
			go func() {
				_, grantErr := grantConn.ExecContext(ctx,
					"grant select on table `"+database+"`.`t_grant_first` to `"+role+"` with grant option")
				grantDone <- grantErr
			}()
			select {
			case <-grantLocked:
			case <-time.After(30 * time.Second):
				t.Fatal("GRANT did not acquire its object lifecycle lock")
			}

			dropDone := make(chan error, 1)
			go func() {
				_, dropErr := dropConn.ExecContext(ctx, "drop table `"+database+"`.`t_grant_first`")
				dropDone <- dropErr
			}()
			waitForWaiter(t, waiterQueued,
				"DROP was not enqueued behind GRANT's object lifecycle lock")

			release()
			require.NoError(t, <-grantDone)
			require.NoError(t, <-dropDone)

			var grants int
			require.NoError(t, db.QueryRowContext(ctx,
				"select count(*) from mo_catalog.mo_role_privs where role_name = ? and privilege_name = 'select'", role).Scan(&grants))
			require.Zero(t, grants)
		})

		t.Run("drop holder commits before waiting grant refresh", func(t *testing.T) {
			execSQLRequire(t, ctx, db, "create table `"+database+"`.`t_drop_first`(id int)")
			dropConn, err := db.Conn(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, dropConn.Close()) })
			grantConn, err := db.Conn(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, grantConn.Close()) })

			execConn(dropConn, "begin")
			defer rollbackConn(dropConn)
			execConn(dropConn, "drop table `"+database+"`.`t_drop_first`")
			waiterQueued := installWaiterBarrier(t)

			grantDone := make(chan error, 1)
			go func() {
				_, grantErr := grantConn.ExecContext(ctx,
					"grant select on table `"+database+"`.`t_drop_first` to `"+role+"`")
				grantDone <- grantErr
			}()
			waitForWaiter(t, waiterQueued,
				"GRANT was not enqueued behind DROP's object lifecycle lock")
			execConn(dropConn, "commit")
			require.Error(t, <-grantDone)

			var objects, grants int
			require.NoError(t, db.QueryRowContext(ctx,
				"select count(*) from mo_catalog.mo_tables where reldatabase = ? and relname = 't_drop_first'", database).Scan(&objects))
			require.NoError(t, db.QueryRowContext(ctx,
				"select count(*) from mo_catalog.mo_role_privs where role_name = ? and privilege_name = 'select'", role).Scan(&grants))
			require.Zero(t, objects)
			require.Zero(t, grants)
		})
	})
}
