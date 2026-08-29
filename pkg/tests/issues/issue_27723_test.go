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
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/frontend"
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
		waitForStatement := func(fragment, message string) {
			t.Helper()
			require.Eventually(t, func() bool {
				rows, queryErr := db.QueryContext(ctx, "show processlist")
				if queryErr != nil {
					return false
				}
				defer rows.Close()
				columns, columnsErr := rows.Columns()
				if columnsErr != nil {
					return false
				}
				for rows.Next() {
					values := make([]sql.RawBytes, len(columns))
					dest := make([]any, len(columns))
					for i := range values {
						dest[i] = &values[i]
					}
					if rows.Scan(dest...) != nil {
						return false
					}
					for _, value := range values {
						if strings.Contains(string(value), fragment) {
							return true
						}
					}
				}
				if rows.Err() != nil {
					return false
				}
				return false
			}, 30*time.Second, 10*time.Millisecond, message)
		}

		t.Run("grant holder commits before waiting drop", func(t *testing.T) {
			execSQLRequire(t, ctx, db, "create table `"+database+"`.`t_grant_first`(id int)")

			grantConn, err := db.Conn(ctx)
			require.NoError(t, err)
			defer grantConn.Close()
			dropConn, err := db.Conn(ctx)
			require.NoError(t, err)
			defer dropConn.Close()

			grantLocked := make(chan struct{})
			releaseGrant := make(chan struct{})
			restoreHook := frontend.SetGrantPrivilegeObjectLockedHookForTest(func() {
				close(grantLocked)
				<-releaseGrant
			})
			defer restoreHook()

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
			waitForStatement("drop table `"+database+"`.`t_grant_first`",
				"DROP did not reach GRANT's object lifecycle lock")

			close(releaseGrant)
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
			defer dropConn.Close()
			grantConn, err := db.Conn(ctx)
			require.NoError(t, err)
			defer grantConn.Close()

			execConn(dropConn, "begin")
			defer rollbackConn(dropConn)
			execConn(dropConn, "drop table `"+database+"`.`t_drop_first`")

			grantDone := make(chan error, 1)
			go func() {
				_, grantErr := grantConn.ExecContext(ctx,
					"grant select on table `"+database+"`.`t_drop_first` to `"+role+"`")
				grantDone <- grantErr
			}()
			waitForStatement("grant select on table `"+database+"`.`t_drop_first`",
				"GRANT did not reach DROP's object lifecycle lock")
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
