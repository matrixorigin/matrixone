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
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue26087ConcurrentDataBranchQuota(t *testing.T) {
	runAuthenticatedClusterTest(t,
		func(c embed.Cluster) {
			ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
			defer cancel()

			cn, err := c.GetCNService(0)
			require.NoError(t, err)
			admissionCN, err := c.GetCNService(1)
			require.NoError(t, err)
			port := cn.GetServiceConfig().CN.Frontend.Port
			admissionPort := admissionCN.GetServiceConfig().CN.Frontend.Port
			require.NotEqual(t, port, admissionPort,
				"the quota mutation and admission check must use distinct CNs")

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

			tenantDB, err := sql.Open("mysql", fmt.Sprintf("%s#root#accountadmin:111@tcp(127.0.0.1:%d)/", accountName, port))
			require.NoError(t, err)
			defer tenantDB.Close()
			tenantDB.SetMaxOpenConns(2)
			var tenantAccountID uint32
			require.NoError(t, tenantDB.QueryRowContext(ctx, "select current_account_id()").Scan(&tenantAccountID))
			require.Equal(t, uint32(accountID), tenantAccountID)
			execSQLRequire(t, ctx, tenantDB, "create database branch_quota_race")
			execSQLRequire(t, ctx, tenantDB, "create table branch_quota_race.src (a int primary key)")
			execSQLRequire(t, ctx, tenantDB, "create table branch_quota_race.mode_probe (a int primary key)")
			execSQLRequire(t, ctx, tenantDB, "create table branch_quota_race.logtail_marker (a int primary key)")
			execSQLRequire(t, ctx, tenantDB, "insert into branch_quota_race.src values (1)")
			execSQLRequire(t, ctx, tenantDB, "create snapshot issue_26087_sp for table branch_quota_race src")
			waitForCatalog := func(
				description string,
				probe func(context.Context) (bool, error),
			) {
				t.Helper()
				waitCtx, waitCancel := context.WithTimeout(ctx, 30*time.Second)
				defer waitCancel()
				require.NoError(t, waitForCatalogVisibility(waitCtx, probe), description)
			}
			waitForCatalog("source database did not become visible on the admission CN",
				func(probeCtx context.Context) (bool, error) {
					return testutils.DBExistsWithAccountE(
						probeCtx, accountID, "branch_quota_race", admissionCN)
				})
			waitForCatalog("source table did not become visible on the admission CN",
				func(probeCtx context.Context) (bool, error) {
					return testutils.TableExistsWithAccountE(
						probeCtx, accountID, "branch_quota_race", "src", admissionCN)
				})

			// Keep one connection on CN-B alive across quota mutations committed on
			// CN-A. This excludes login/setup convergence from the assertion: each
			// DATA BRANCH statement must install its own TN-ordered catalog frontier.
			admissionDB, err := sql.Open("mysql", fmt.Sprintf(
				"%s#root#accountadmin:111@tcp(127.0.0.1:%d)/", accountName, admissionPort))
			require.NoError(t, err)
			defer admissionDB.Close()
			admissionDB.SetMaxOpenConns(1)
			admissionConn, err := admissionDB.Conn(ctx)
			require.NoError(t, err)
			defer admissionConn.Close()
			var quotaProbe int
			require.NoError(t, admissionConn.QueryRowContext(ctx, "select 1").Scan(&quotaProbe))
			require.Equal(t, 1, quotaProbe)

			setBranchQuota := func(quota int) {
				t.Helper()
				execSQLRequire(t, ctx, sysDB, fmt.Sprintf(
					"select mo_feature_limit_upsert(%d, 'branch', '', %d)", accountID, quota))
			}
			createCrossCNBranch := func(name string) error {
				t.Helper()
				_, createErr := admissionConn.ExecContext(ctx, fmt.Sprintf(
					"data branch create table branch_quota_race.%s from branch_quota_race.src", name))
				return createErr
			}
			deleteCrossCNBranch := func(name string) error {
				t.Helper()
				_, deleteErr := admissionConn.ExecContext(ctx, fmt.Sprintf(
					"data branch delete table branch_quota_race.%s", name))
				return deleteErr
			}
			assertCrossCNDenied := func(name string) {
				t.Helper()
				createErr := createCrossCNBranch(name)
				require.Error(t, createErr)
				require.Contains(t, createErr.Error(),
					"feature BRANCH with scope  has disabled for account "+accountName)
			}

			setBranchQuota(0)
			assertCrossCNDenied("cross_cn_disabled_from_finite")
			setBranchQuota(-1)
			require.NoError(t, createCrossCNBranch("cross_cn_unlimited_from_disabled"))
			require.NoError(t, deleteCrossCNBranch("cross_cn_unlimited_from_disabled"))
			setBranchQuota(0)
			assertCrossCNDenied("cross_cn_disabled_from_unlimited")
			setBranchQuota(1)
			require.NoError(t, createCrossCNBranch("cross_cn_finite_from_disabled"))
			require.NoError(t, deleteCrossCNBranch("cross_cn_finite_from_disabled"))

			conn1, err := tenantDB.Conn(ctx)
			require.NoError(t, err)
			conn2, err := tenantDB.Conn(ctx)
			require.NoError(t, err)

			execCtx, cancelExec := context.WithCancel(ctx)
			defer func() {
				cancelExec()
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cleanupCancel()
				_, _ = conn1.ExecContext(cleanupCtx, "rollback")
				_, _ = conn2.ExecContext(cleanupCtx, "rollback")
				_ = conn1.Close()
				_ = conn2.Close()
			}()

			execConn := func(conn *sql.Conn, statement string) error {
				_, execErr := conn.ExecContext(execCtx, statement)
				return execErr
			}
			runConcurrent := func(statements ...struct {
				conn *sql.Conn
				sql  string
			}) []error {
				start := make(chan struct{})
				done := make(chan error, len(statements))
				for _, statement := range statements {
					go func(statement struct {
						conn *sql.Conn
						sql  string
					}) {
						<-start
						done <- execConn(statement.conn, statement.sql)
					}(statement)
				}
				close(start)
				errs := make([]error, 0, len(statements))
				for range statements {
					select {
					case err := <-done:
						errs = append(errs, err)
					case <-time.After(30 * time.Second):
						t.Fatal("concurrent data-branch mutation did not return")
					}
				}
				return errs
			}

			require.NoError(t, execConn(conn1, "begin"))
			require.NoError(t, execConn(conn1,
				"data branch create table branch_quota_race.explicit_holder from branch_quota_race.src"))
			// The explicit tenant transaction has already mutated owner catalogs but
			// must not own the global lifecycle row between statements. A system
			// owner writer therefore completes before the tenant chooses COMMIT.
			snapshotDone := make(chan error, 1)
			go func() {
				_, snapshotErr := sysDB.ExecContext(execCtx,
					"create snapshot issue_26087_holder_probe for account "+accountName)
				snapshotDone <- snapshotErr
			}()
			select {
			case snapshotErr := <-snapshotDone:
				require.NoError(t, snapshotErr)
			case <-time.After(5 * time.Second):
				t.Fatal("system snapshot waited for the open tenant transaction")
			}
			commitErr := execConn(conn1, "commit")
			require.Error(t, commitErr,
				"commit must validate and lose to the completed lifecycle writer")
			require.NotContains(t, strings.ToLower(commitErr.Error()), "deadlock")
			require.NotContains(t, strings.ToLower(commitErr.Error()), "lock wait timeout")
			var explicitHolderCount int
			require.NoError(t, conn1.QueryRowContext(execCtx,
				"select count(*) from mo_catalog.mo_tables where reldatabase = 'branch_quota_race' and relname = 'explicit_holder'",
			).Scan(&explicitHolderCount))
			require.Zero(t, explicitHolderCount)
			execSQLRequire(t, ctx, sysDB, "drop snapshot issue_26087_holder_probe")

			createErrs := runConcurrent(
				struct {
					conn *sql.Conn
					sql  string
				}{conn1, "data branch create table branch_quota_race.b1 from branch_quota_race.src{snapshot='issue_26087_sp'}"},
				struct {
					conn *sql.Conn
					sql  string
				}{conn2, "data branch create table branch_quota_race.b2 from branch_quota_race.src{snapshot='issue_26087_sp'}"},
			)
			var createSuccesses, quotaFailures int
			for _, createErr := range createErrs {
				if createErr == nil {
					createSuccesses++
					continue
				}
				quotaFailures++
				require.Contains(t, createErr.Error(), "feature BRANCH with scope  has reached the limit of 1")
				require.NotContains(t, strings.ToLower(createErr.Error()), "txn need retry")
			}
			require.Equal(t, 1, createSuccesses)
			require.Equal(t, 1, quotaFailures)

			var branchCount int
			require.NoError(t, conn1.QueryRowContext(execCtx,
				"select count(*) from mo_catalog.mo_tables where reldatabase = 'branch_quota_race' and relname in ('b1', 'b2')",
			).Scan(&branchCount))
			require.Equal(t, 1, branchCount)

			var createdBranchName string
			require.NoError(t, conn1.QueryRowContext(execCtx,
				"select relname from mo_catalog.mo_tables where reldatabase = 'branch_quota_race' and relname in ('b1', 'b2')",
			).Scan(&createdBranchName))
			require.NoError(t, execConn(conn1,
				"data branch delete table branch_quota_race."+createdBranchName))
			require.NoError(t, execConn(conn1, "create database branch_quota_source"))
			require.NoError(t, execConn(conn1, "create table branch_quota_source.t1 (a int primary key)"))
			require.NoError(t, execConn(conn1, "create table branch_quota_source.t2 (a int primary key)"))
			execSQLRequire(t, ctx, sysDB, fmt.Sprintf("select mo_feature_limit_upsert(%d, 'branch', '', 3)", accountID))

			createErrs = runConcurrent(
				struct {
					conn *sql.Conn
					sql  string
				}{conn1, "data branch create table branch_quota_race.b1 from branch_quota_race.src{snapshot='issue_26087_sp'}"},
				struct {
					conn *sql.Conn
					sql  string
				}{conn2, "data branch create database branch_quota_destination from branch_quota_source"},
			)
			for _, createErr := range createErrs {
				require.NoError(t, createErr)
			}

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
			// conn1 and conn2 were initialized before the service default changed.
			// Set their session defaults explicitly so the transactions below test
			// the intended SI behavior rather than their original RC defaults.
			require.NoError(t, execConn(conn1, "set session transaction isolation level repeatable read"))
			require.NoError(t, execConn(conn2, "set session transaction isolation level repeatable read"))

			require.NoError(t, execConn(conn1, "begin"))
			var fixedSnapshotProbe int
			require.NoError(t, conn1.QueryRowContext(execCtx,
				"select count(*) from branch_quota_race.mode_probe where a = 2",
			).Scan(&fixedSnapshotProbe))
			require.Zero(t, fixedSnapshotProbe)
			require.NoError(t, execConn(conn2, "insert into branch_quota_race.mode_probe values (2)"))
			explicitErr := execConn(conn1,
				"data branch create table branch_quota_race.explicit_branch from branch_quota_race.src{snapshot='issue_26087_sp'}")
			require.Error(t, explicitErr)
			require.Contains(t, explicitErr.Error(),
				"finite branch quota requires a pessimistic read committed transaction")
			require.NoError(t, conn1.QueryRowContext(execCtx,
				"select count(*) from branch_quota_race.mode_probe where a = 2",
			).Scan(&fixedSnapshotProbe))
			require.Zero(t, fixedSnapshotProbe,
				"feature-limit freshness must not advance the outer SI transaction")
			require.NoError(t, execConn(conn1, "rollback"))
			cleanupResult, err := conn2.ExecContext(execCtx,
				"delete from branch_quota_race.mode_probe where a = 2")
			require.NoError(t, err)
			cleanupRows, err := cleanupResult.RowsAffected()
			require.NoError(t, err)
			require.EqualValues(t, 1, cleanupRows)
			// The driver acknowledges conn2's commit before CN0 necessarily applies
			// its logtail. Commit a later write marker and wait for its frontier before
			// conn1 opens the next SI snapshot.
			markerCommitTS := testutils.ExecSQLWithReadResultAndAccount(
				t,
				accountID,
				"branch_quota_race",
				cn,
				nil,
				"insert into branch_quota_race.logtail_marker values (1)",
			)
			require.False(t, markerCommitTS.IsEmpty())

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
		})
}

func waitForCatalogVisibility(
	ctx context.Context,
	probe func(context.Context) (bool, error),
) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var lastErr error
	for {
		visible, err := probe(ctx)
		if err == nil && visible {
			return nil
		}
		if err != nil {
			lastErr = err
		}

		select {
		case <-ctx.Done():
			cause := context.Cause(ctx)
			if lastErr != nil {
				return fmt.Errorf("%w; last catalog probe failed: %w", cause, lastErr)
			}
			return cause
		case <-ticker.C:
		}
	}
}

func TestWaitForCatalogVisibilityPreservesProbeError(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	probeErr := errors.New("catalog query failed")

	err := waitForCatalogVisibility(ctx, func(context.Context) (bool, error) {
		return false, probeErr
	})
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, probeErr)
}
