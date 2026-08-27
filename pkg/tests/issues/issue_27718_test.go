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
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue27718ConcurrentSnapshotQuota(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port

		sysDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer sysDB.Close()
		execSQLRequire(t, ctx, sysDB, "set role moadmin")

		const accountName = "issue_27718"
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, sysDB, "drop account if exists "+accountName)
		}()
		execSQLRequire(t, ctx, sysDB, "drop account if exists "+accountName)
		accountID := testutils.CreateAccount(t, c, accountName, "111")
		execSQLRequire(t, ctx, sysDB,
			"select mo_feature_registry_upsert('snapshot', 'Snapshot feature', '{\"allowed_scope\":[\"account\",\"database\",\"table\"]}', true)")

		tenantDB, err := sql.Open("mysql", fmt.Sprintf("%s#root#accountadmin:111@tcp(127.0.0.1:%d)/", accountName, port))
		require.NoError(t, err)
		defer tenantDB.Close()
		const creators = 4
		tenantDB.SetMaxOpenConns(creators + 1)
		execSQLRequire(t, ctx, tenantDB, "create database issue_27718_db")
		execSQLRequire(t, ctx, tenantDB, "create table issue_27718_db.t (id int primary key)")
		execSQLRequire(t, ctx, tenantDB, "insert into issue_27718_db.t values (1)")

		connections := make([]*sql.Conn, creators)
		for i := range connections {
			connections[i], err = tenantDB.Conn(ctx)
			require.NoError(t, err)
			defer connections[i].Close()
			var probe int
			require.NoError(t, connections[i].QueryRowContext(ctx, "select 1").Scan(&probe))
			require.Equal(t, 1, probe)
		}
		runConcurrentCreates := func(prefix string, quota, expectedSuccesses int) []string {
			t.Helper()
			execSQLRequire(t, ctx, sysDB, fmt.Sprintf(
				"select mo_feature_limit_upsert(%d, 'snapshot', 'table', %d)", accountID, quota))

			type createResult struct {
				name string
				err  error
			}
			start := make(chan struct{})
			results := make(chan createResult, creators)
			for i := 1; i <= creators; i++ {
				name := fmt.Sprintf("%s_%d", prefix, i)
				conn := connections[i-1]
				go func(conn *sql.Conn, name string) {
					<-start
					_, createErr := conn.ExecContext(ctx, fmt.Sprintf(
						"create snapshot %s for table issue_27718_db t", name))
					results <- createResult{name: name, err: createErr}
				}(conn, name)
			}
			close(start)

			successes := make([]string, 0, creators)
			for i := 0; i < creators; i++ {
				select {
				case result := <-results:
					if result.err == nil {
						successes = append(successes, result.name)
						continue
					}
					if quota == 0 {
						require.Contains(t, result.err.Error(),
							"feature SNAPSHOT with scope table has disabled for account "+accountName)
					} else {
						require.Contains(t, result.err.Error(), fmt.Sprintf(
							"feature SNAPSHOT with scope table has reached the limit of %d", quota))
					}
					require.NotContains(t, strings.ToLower(result.err.Error()), "txn need retry")
				case <-time.After(30 * time.Second):
					t.Fatal("concurrent snapshot creator did not finish")
				}
			}
			require.Len(t, successes, expectedSuccesses)

			var persisted int
			require.NoError(t, tenantDB.QueryRowContext(ctx,
				"select count(*) from mo_catalog.mo_snapshots where account_name = ? and sname in (?, ?, ?, ?)",
				accountName, prefix+"_1", prefix+"_2", prefix+"_3", prefix+"_4").Scan(&persisted))
			require.Equal(t, expectedSuccesses, persisted)
			return successes
		}

		quotaOneSnapshots := runConcurrentCreates("issue27718_q1", 1, 1)
		execSQLRequire(t, ctx, tenantDB, "drop snapshot "+quotaOneSnapshots[0])
		execSQLRequire(t, ctx, tenantDB, "create snapshot issue27718_replacement for table issue_27718_db t")
		_, err = tenantDB.ExecContext(ctx, "create snapshot issue27718_over_limit for table issue_27718_db t")
		require.Error(t, err)
		require.Contains(t, err.Error(), "feature SNAPSHOT with scope table has reached the limit of 1")
		var overLimitRows int
		require.NoError(t, tenantDB.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_snapshots where account_name = ? and sname = 'issue27718_over_limit'",
			accountName).Scan(&overLimitRows))
		require.Zero(t, overLimitRows)
		execSQLRequire(t, ctx, tenantDB, "drop snapshot issue27718_replacement")

		quotaTwoSnapshots := runConcurrentCreates("issue27718_q2", 2, 2)
		for _, name := range quotaTwoSnapshots {
			execSQLRequire(t, ctx, tenantDB, "drop snapshot "+name)
		}

		runConcurrentCreates("issue27718_disabled", 0, 0)

		unlimitedSnapshots := runConcurrentCreates("issue27718_unlimited", -1, creators)
		for _, name := range unlimitedSnapshots {
			execSQLRequire(t, ctx, tenantDB, "drop snapshot "+name)
		}
	})
}
