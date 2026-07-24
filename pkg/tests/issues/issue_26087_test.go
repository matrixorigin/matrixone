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
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	pblock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue26087ConcurrentDataBranchQuota(t *testing.T) {
	require.NoError(t, embed.RunBaseClusterTests(
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
			defer func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cleanupCancel()
				execSQLMaybe(t, cleanupCtx, sysDB, "drop account if exists "+accountName)
			}()
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

			tenantDB, err := sql.Open("mysql", fmt.Sprintf("%s#root#accountadmin:111@tcp(127.0.0.1:%d)/", accountName, port))
			require.NoError(t, err)
			defer tenantDB.Close()
			tenantDB.SetMaxOpenConns(2)
			execSQLRequire(t, ctx, tenantDB, "create database branch_quota_race")
			execSQLRequire(t, ctx, tenantDB, "create table branch_quota_race.src (a int primary key)")
			execSQLRequire(t, ctx, tenantDB, "create table branch_quota_race.mode_probe (a int primary key)")
			execSQLRequire(t, ctx, tenantDB, "insert into branch_quota_race.src values (1)")
			execSQLRequire(t, ctx, tenantDB, "create snapshot issue_26087_sp for table branch_quota_race src")

			conn1, err := tenantDB.Conn(ctx)
			require.NoError(t, err)
			conn2, err := tenantDB.Conn(ctx)
			require.NoError(t, err)

			execCtx, cancelExec := context.WithCancel(ctx)
			var pendingCreate chan error
			defer func() {
				cancelExec()
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cleanupCancel()
				_, _ = conn1.ExecContext(cleanupCtx, "rollback")
				if pendingCreate != nil {
					select {
					case <-pendingCreate:
					case <-cleanupCtx.Done():
					}
				}
				_, _ = conn2.ExecContext(cleanupCtx, "rollback")
				_ = conn1.Close()
				_ = conn2.Close()
			}()

			execConn := func(conn *sql.Conn, statement string) error {
				_, execErr := conn.ExecContext(execCtx, statement)
				return execErr
			}
			ls := lockservice.GetLockServiceByServiceID(cn.ServiceID())
			waitForQuotaWaiter := func() {
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
			}

			require.NoError(t, execConn(conn1, "begin"))
			require.NoError(t, execConn(conn1, "data branch create table branch_quota_race.b1 from branch_quota_race.src{snapshot='issue_26087_sp'}"))

			createDone := make(chan error, 1)
			pendingCreate = createDone
			go func(done chan<- error) {
				done <- execConn(conn2, "data branch create table branch_quota_race.b2 from branch_quota_race.src{snapshot='issue_26087_sp'}")
			}(createDone)

			waitForQuotaWaiter()

			require.NoError(t, execConn(conn1, "commit"))
			select {
			case createErr := <-createDone:
				pendingCreate = nil
				require.Error(t, createErr)
				require.Contains(t, createErr.Error(), "feature BRANCH with scope  has reached the limit of 1")
				require.NotContains(t, strings.ToLower(createErr.Error()), "txn need retry")
			case <-time.After(30 * time.Second):
				t.Fatal("second branch creator did not return after the quota-row lock was released")
			}

			var branchCount int
			require.NoError(t, conn1.QueryRowContext(execCtx,
				"select count(*) from mo_catalog.mo_tables where reldatabase = 'branch_quota_race' and relname in ('b1', 'b2')",
			).Scan(&branchCount))
			require.Equal(t, 1, branchCount)

			require.NoError(t, execConn(conn1, "data branch delete table branch_quota_race.b1"))
			require.NoError(t, execConn(conn1, "create database branch_quota_source"))
			require.NoError(t, execConn(conn1, "create table branch_quota_source.t1 (a int primary key)"))
			require.NoError(t, execConn(conn1, "create table branch_quota_source.t2 (a int primary key)"))
			execSQLRequire(t, ctx, sysDB, fmt.Sprintf("select mo_feature_limit_upsert(%d, 'branch', '', 3)", accountID))

			require.NoError(t, execConn(conn1, "begin"))
			require.NoError(t, execConn(conn1, "data branch create table branch_quota_race.b1 from branch_quota_race.src{snapshot='issue_26087_sp'}"))
			require.NoError(t, execConn(conn2, "begin"))
			createDone = make(chan error, 1)
			pendingCreate = createDone
			go func(done chan<- error) {
				done <- execConn(conn2, "data branch create database branch_quota_destination from branch_quota_source")
			}(createDone)

			waitForQuotaWaiter()
			require.NoError(t, execConn(conn1, "commit"))
			select {
			case createErr := <-createDone:
				pendingCreate = nil
				require.NoError(t, createErr)
			case <-time.After(30 * time.Second):
				t.Fatal("database branch creator did not return after the quota-row lock was released")
			}
			require.NoError(t, execConn(conn2, "commit"))

			var databaseBranchCount int
			require.NoError(t, conn1.QueryRowContext(execCtx,
				"select count(*) from mo_catalog.mo_tables where reldatabase = 'branch_quota_destination' and relname in ('t1', 't2')",
			).Scan(&databaseBranchCount))
			require.Equal(t, 2, databaseBranchCount)

			require.NoError(t, execConn(conn1, "data branch delete table branch_quota_race.b1"))
			require.NoError(t, execConn(conn1, "data branch delete database branch_quota_destination"))
			execSQLRequire(t, ctx, sysDB, fmt.Sprintf("select mo_feature_limit_upsert(%d, 'branch', '', 1)", accountID))

			rt := moruntime.ServiceRuntime(cn.ServiceID())
			oldMode, hadMode := rt.GetGlobalVariables(moruntime.TxnMode)
			oldIsolation, hadIsolation := rt.GetGlobalVariables(moruntime.TxnIsolation)
			rt.SetGlobalVariables(moruntime.TxnMode, pbtxn.TxnMode_Optimistic)
			rt.SetGlobalVariables(moruntime.TxnIsolation, pbtxn.TxnIsolation_SI)
			defer func() {
				if hadMode {
					rt.SetGlobalVariables(moruntime.TxnMode, oldMode)
				} else {
					rt.SetGlobalVariables(moruntime.TxnMode, pbtxn.TxnMode_Pessimistic)
				}
				if hadIsolation {
					rt.SetGlobalVariables(moruntime.TxnIsolation, oldIsolation)
				} else {
					rt.SetGlobalVariables(moruntime.TxnIsolation, pbtxn.TxnIsolation_RC)
				}
			}()

			require.NoError(t, execConn(conn1, "begin"))
			explicitErr := execConn(conn1,
				"data branch create table branch_quota_race.explicit_branch from branch_quota_race.src{snapshot='issue_26087_sp'}")
			require.Error(t, explicitErr)
			require.Contains(t, explicitErr.Error(),
				"finite branch quota requires a pessimistic read committed transaction; retry outside the active transaction")
			require.NoError(t, execConn(conn1, "rollback"))

			require.NoError(t, execConn(conn1, "set autocommit = 0"))
			var modeProbeCount int
			require.NoError(t, conn1.QueryRowContext(execCtx,
				"select count(*) from branch_quota_race.mode_probe",
			).Scan(&modeProbeCount))
			require.Zero(t, modeProbeCount)
			require.NoError(t, execConn(conn2, "insert into branch_quota_race.mode_probe values (1)"))
			require.NoError(t, execConn(conn1,
				"data branch create table branch_quota_race.implicit_branch from branch_quota_race.src"))
			require.NoError(t, conn1.QueryRowContext(execCtx,
				"select count(*) from branch_quota_race.mode_probe",
			).Scan(&modeProbeCount))
			require.Zero(t, modeProbeCount, "DATA BRANCH must not downgrade the outer SI transaction to RC")
			require.NoError(t, execConn(conn1, "rollback"))
			require.NoError(t, execConn(conn1, "set autocommit = 1"))

			var explicitBranchCount int
			require.NoError(t, conn1.QueryRowContext(execCtx,
				"select count(*) from mo_catalog.mo_tables where reldatabase = 'branch_quota_race' and relname = 'explicit_branch'",
			).Scan(&explicitBranchCount))
			require.Zero(t, explicitBranchCount)
			require.NoError(t, execConn(conn1, "data branch delete table branch_quota_race.implicit_branch"))

			optimisticDB, err := sql.Open("mysql", fmt.Sprintf("%s#root#accountadmin:111@tcp(127.0.0.1:%d)/", accountName, port))
			require.NoError(t, err)
			defer optimisticDB.Close()
			const creators = 8
			optimisticDB.SetMaxOpenConns(creators)
			type createResult struct {
				name string
				err  error
			}
			optimisticCtx, cancelOptimistic := context.WithCancel(ctx)
			start := make(chan struct{})
			results := make(chan createResult, creators)
			received := 0
			defer func() {
				cancelOptimistic()
				drainCtx, drainCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer drainCancel()
				for received < creators {
					select {
					case <-results:
						received++
					case <-drainCtx.Done():
						return
					}
				}
			}()
			for i := 0; i < creators; i++ {
				name := fmt.Sprintf("optimistic_branch_%d", i)
				go func() {
					<-start
					_, createErr := optimisticDB.ExecContext(optimisticCtx, fmt.Sprintf(
						"data branch create table branch_quota_race.%s from branch_quota_race.src{snapshot='issue_26087_sp'}", name))
					results <- createResult{name: name, err: createErr}
				}()
			}
			close(start)

			successes := 0
			for i := 0; i < creators; i++ {
				select {
				case result := <-results:
					received++
					if result.err == nil {
						successes++
						continue
					}
					require.Containsf(t, result.err.Error(),
						"feature BRANCH with scope  has reached the limit of 1", "creator %s", result.name)
					require.NotContains(t, strings.ToLower(result.err.Error()), "txn need retry")
				case <-time.After(30 * time.Second):
					t.Fatal("optimistic branch creators did not finish")
				}
			}
			require.Equal(t, 1, successes)

			var optimisticBranchCount int
			require.NoError(t, conn1.QueryRowContext(execCtx,
				"select count(*) from mo_catalog.mo_tables where reldatabase = 'branch_quota_race' and relname like 'optimistic\\_branch\\_%'",
			).Scan(&optimisticBranchCount))
			require.Equal(t, 1, optimisticBranchCount)
		},
	))
}
