// Copyright 2021 - 2026 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	pblock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue26087ConcurrentDataBranchQuota(t *testing.T) {
	embed.RunBaseClusterTests(
		func(c embed.Cluster) {
			ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
			defer cancel()

			cn, err := c.GetCNService(0)
			require.NoError(t, err)
			port := cn.GetServiceConfig().CN.Frontend.Port

			sysDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
			require.NoError(t, err)
			defer sysDB.Close()
			execSQLRequire(t, ctx, sysDB, "set role moadmin")

			accountName := "issue_26087"
			defer execSQLMaybe(t, ctx, sysDB, "drop account if exists "+accountName)
			execSQLRequire(t, ctx, sysDB, "drop account if exists "+accountName)
			accountID := testutils.CreateAccount(t, c, accountName, "111")
			execSQLRequire(t, ctx, sysDB, "select mo_feature_registry_upsert('branch', 'Branch feature', '{\"allowed_scope\":[]}', true)")
			execSQLRequire(t, ctx, sysDB, "select mo_feature_registry_upsert('snapshot', 'Snapshot feature', '{\"allowed_scope\":[\"account\",\"database\",\"table\"]}', true)")
			execSQLRequire(t, ctx, sysDB, fmt.Sprintf("select mo_feature_limit_upsert(%d, 'branch', '', 1)", accountID))
			execSQLRequire(t, ctx, sysDB, fmt.Sprintf("select mo_feature_limit_upsert(%d, 'snapshot', 'table', -1)", accountID))

			var featureLimitTableID uint64
			require.NoError(t, sysDB.QueryRowContext(ctx,
				"select rel_id from mo_catalog.mo_tables where reldatabase = 'mo_catalog' and relname = 'mo_feature_limit'",
			).Scan(&featureLimitTableID))

			tenantDB, err := sql.Open("mysql", fmt.Sprintf("%s#root#moadmin:111@tcp(127.0.0.1:%d)/", accountName, port))
			require.NoError(t, err)
			defer tenantDB.Close()
			tenantDB.SetMaxOpenConns(2)
			execSQLRequire(t, ctx, tenantDB, "create database branch_quota_race")
			execSQLRequire(t, ctx, tenantDB, "create table branch_quota_race.src (a int primary key)")
			execSQLRequire(t, ctx, tenantDB, "insert into branch_quota_race.src values (1)")
			execSQLRequire(t, ctx, tenantDB, "create snapshot issue_26087_sp for table branch_quota_race src")

			conn1, err := tenantDB.Conn(ctx)
			require.NoError(t, err)
			defer conn1.Close()
			conn2, err := tenantDB.Conn(ctx)
			require.NoError(t, err)
			defer conn2.Close()

			execConn := func(conn *sql.Conn, statement string) error {
				_, execErr := conn.ExecContext(ctx, statement)
				return execErr
			}
			require.NoError(t, execConn(conn1, "begin"))
			require.NoError(t, execConn(conn1, "data branch create table branch_quota_race.b1 from branch_quota_race.src{snapshot='issue_26087_sp'}"))

			createDone := make(chan error, 1)
			go func() {
				createDone <- execConn(conn2, "data branch create table branch_quota_race.b2 from branch_quota_race.src{snapshot='issue_26087_sp'}")
			}()

			ls := lockservice.GetLockServiceByServiceID(cn.ServiceID())
			require.Eventually(t, func() bool {
				found := false
				ls.IterLocks(func(tableID uint64, _ [][]byte, lock lockservice.Lock) bool {
					if tableID != featureLimitTableID {
						return true
					}
					lock.IterWaiters(func(_ pblock.WaitTxn) bool {
						found = true
						return false
					})
					return !found
				})
				return found
			}, 30*time.Second, 10*time.Millisecond, "second branch creator did not wait for the quota-row lock")

			require.NoError(t, execConn(conn1, "commit"))
			select {
			case createErr := <-createDone:
				require.Error(t, createErr)
				require.Contains(t, createErr.Error(), "feature BRANCH with scope  has reached the limit of 1")
				require.NotContains(t, strings.ToLower(createErr.Error()), "txn need retry")
			case <-time.After(30 * time.Second):
				t.Fatal("second branch creator did not return after the quota-row lock was released")
			}

			var branchCount int
			require.NoError(t, tenantDB.QueryRowContext(ctx,
				"select count(*) from mo_catalog.mo_tables where reldatabase = 'branch_quota_race' and relname in ('b1', 'b2')",
			).Scan(&branchCount))
			require.Equal(t, 1, branchCount)
		},
	)
}
