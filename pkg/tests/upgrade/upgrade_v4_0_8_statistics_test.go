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

package upgrade

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_8"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_9"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

func TestV408UpgradeRefreshesStatistics(t *testing.T) {
	embed.RunSingleCNBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		sysDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer sysDB.Close()

		const accountName = "statistics_upgrade_28999"
		_, err = sysDB.ExecContext(ctx, "create account "+accountName+" ADMIN_NAME 'root' IDENTIFIED BY '111'")
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, err := sysDB.ExecContext(cleanupCtx, "drop account "+accountName)
			require.NoError(t, err)
		}()
		var tenantID uint32
		require.NoError(t, sysDB.QueryRowContext(ctx,
			"select account_id from mo_catalog.mo_account where account_name = '"+accountName+"'").Scan(&tenantID))

		sqlExecutor := testutils.GetSQLExecutor(cn)
		legacy := strings.Replace(sysview.InformationSchemaStatisticsDDL,
			"coalesce(nullif(`idx`.`algo`, ''), 'BTREE')", "`idx`.`algo`", 1)
		require.NotEqual(t, sysview.InformationSchemaStatisticsDDL, legacy)
		replaceView := func(ctx context.Context, accountID uint32, ddl string) error {
			return sqlExecutor.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
				for _, statement := range []string{"drop view if exists information_schema.STATISTICS", ddl} {
					res, err := txn.Exec(statement, versions.UpgradeStatementOption(accountID))
					if err != nil {
						return err
					}
					res.Close()
				}
				return nil
			}, executor.Options{}.WithDatabase(catalog.MO_CATALOG).
				WithAccountID(accountID).WithWaitCommittedLogApplied())
		}

		for _, test := range []struct {
			name      string
			accountID uint32
			user      string
		}{
			{name: "system", accountID: catalog.System_Account, user: "dump"},
			{name: "tenant", accountID: tenantID, user: accountName + "#root#accountadmin"},
		} {
			t.Run(test.name, func(t *testing.T) {
				db, err := sql.Open("mysql", fmt.Sprintf("%s:111@tcp(127.0.0.1:%d)/", test.user, port))
				require.NoError(t, err)
				defer db.Close()
				conn, err := db.Conn(ctx)
				require.NoError(t, err)
				defer conn.Close()
				exec := func(statement string) {
					t.Helper()
					_, err := conn.ExecContext(ctx, statement)
					require.NoError(t, err, statement)
				}
				const dbName = "statistics_upgrade_28999"
				exec("create database " + dbName)
				defer func() {
					cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cleanupCancel()
					// Restore the shared cluster's view even if an assertion fails mid-upgrade.
					require.NoError(t, replaceView(cleanupCtx, test.accountID, sysview.InformationSchemaStatisticsDDL))
					_, err := conn.ExecContext(cleanupCtx, "drop database "+dbName)
					require.NoError(t, err)
				}()
				exec("use " + dbName)
				exec("set experimental_ivf_index = 1")
				exec("create table t(id int primary key, code varchar(20), embedding vecf32(3), " +
					"unique key uq_code(code), key idx_code(code))")
				exec("create index vidx using ivfflat on t(embedding) lists = 2 op_type 'vector_l2_ops'")

				// Reproduce the persisted view on an existing 4.0.6 / old-offset 4.0.7 tenant,
				// rather than testing the fresh-bootstrap definition.
				require.NoError(t, replaceView(ctx, test.accountID, legacy))
				btreeQuery := "select index_name from information_schema.statistics where table_schema = '" +
					dbName + "' and table_name = 't' and index_type = 'BTREE' order by index_name"
				require.Empty(t, statisticsIndexNames(t, ctx, conn, btreeQuery))
				showRows, err := conn.QueryContext(ctx, "show index from t where Index_type = 'BTREE'")
				require.NoError(t, err)
				defer showRows.Close()
				var showCount int
				for showRows.Next() {
					showCount++
				}
				require.NoError(t, showRows.Err())
				require.Equal(t, 3, showCount, "SHOW INDEX already uses the current runtime definition")
				specializedQuery := "select index_type from information_schema.statistics where table_schema = '" +
					dbName + "' and table_name = 't' and index_name = 'vidx'"
				var originalAlgorithm string
				require.NoError(t, conn.QueryRowContext(ctx, specializedQuery).Scan(&originalAlgorithm))
				require.Equal(t, "ivfflat", originalAlgorithm)

				for run := 0; run < 2; run++ {
					var creates int
					require.NoError(t, sqlExecutor.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
						return v4_0_8.Handler.HandleTenantUpgrade(ctx, int32(test.accountID),
							&statisticsUpgradeTxn{TxnExecutor: txn, creates: &creates})
					}, executor.Options{}.WithDatabase(catalog.MO_CATALOG).
						WithAccountID(test.accountID).WithWaitCommittedLogApplied()))
					if run == 0 {
						require.Equal(t, 1, creates, "the old view must be recreated")
					} else {
						require.Zero(t, creates, "the canonical view must not be recreated again")
					}
					require.ElementsMatch(t, []string{"PRIMARY", "idx_code", "uq_code"},
						statisticsIndexNames(t, ctx, conn, btreeQuery))
					var specialized string
					require.NoError(t, conn.QueryRowContext(ctx, specializedQuery).Scan(&specialized))
					require.Equal(t, originalAlgorithm, specialized, "the specialized algorithm must not become BTREE")
					var exists bool
					var definition string
					require.NoError(t, sqlExecutor.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
						var err error
						exists, definition, err = versions.CheckViewDefinition(
							txn, test.accountID, sysview.InformationDBConst, "statistics")
						return err
					}, executor.Options{}.WithDatabase(catalog.MO_CATALOG).WithAccountID(test.accountID)))
					require.True(t, exists)
					require.Equal(t, sysview.InformationSchemaStatisticsDDL, definition)
				}
			})
		}
	})
}

func TestV408LoginRepairsTenantCreatedAfterUpgradeSnapshot(t *testing.T) {
	embed.RunSingleCNBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		sysDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer sysDB.Close()
		sqlExecutor := testutils.GetSQLExecutor(cn)
		catalogExec := func(ctx context.Context, statement string) error {
			res, err := sqlExecutor.Exec(ctx, statement, executor.Options{}.
				WithDatabase(catalog.MO_CATALOG).WithWaitCommittedLogApplied())
			if err == nil {
				res.Close()
			}
			return err
		}
		const (
			accountName = "statistics_late_upgrade_28999"
			upgradeID   = uint64(290460001)
			dbName      = "statistics_late_upgrade_28999"
		)
		var lastSnapshotID int32
		require.NoError(t, sysDB.QueryRowContext(ctx,
			"select max(account_id) from mo_catalog.mo_account").Scan(&lastSnapshotID))
		// Persist the bounded task before account creation. The fixture models
		// an old writer's catalog, not a fresh account with the new view DDL.
		require.NoError(t, sqlExecutor.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
			return versions.AddUpgradeTenantTask(upgradeID, "4.0.8", 0, lastSnapshotID, txn)
		}, executor.Options{}.WithDatabase(catalog.MO_CATALOG).WithWaitCommittedLogApplied()))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			require.NoError(t, catalogExec(cleanupCtx,
				fmt.Sprintf("delete from mo_catalog.mo_upgrade_tenant where upgrade_id = %d", upgradeID)))
		}()
		_, err = sysDB.ExecContext(ctx, "create account "+accountName+" ADMIN_NAME 'root' IDENTIFIED BY '111'")
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, err := sysDB.ExecContext(cleanupCtx, "drop account "+accountName)
			require.NoError(t, err)
		}()
		var tenantID uint32
		require.NoError(t, sysDB.QueryRowContext(ctx,
			"select account_id from mo_catalog.mo_account where account_name = '"+accountName+"'").Scan(&tenantID))
		require.Greater(t, int32(tenantID), lastSnapshotID)
		legacy := strings.Replace(sysview.InformationSchemaStatisticsDDL,
			"coalesce(nullif(`idx`.`algo`, ''), 'BTREE')", "`idx`.`algo`", 1)
		require.NotEqual(t, sysview.InformationSchemaStatisticsDDL, legacy)
		// Do not log into the tenant before establishing the old-writer state:
		// the first real login must be the operation that repairs it.
		require.NoError(t, sqlExecutor.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
			for _, statement := range []string{
				"create database " + dbName,
				"create table " + dbName + ".t(id int primary key, code varchar(20), embedding vecf32(3), " +
					"unique key uq_code(code), key idx_code(code))",
				"create index vidx using ivfflat on " + dbName + ".t(embedding) lists = 2 op_type 'vector_l2_ops'",
				"drop view if exists information_schema.STATISTICS",
				legacy,
			} {
				res, err := txn.Exec(statement, versions.UpgradeStatementOption(tenantID))
				if err != nil {
					return err
				}
				res.Close()
			}
			return versions.UpgradeTenantVersion(int32(tenantID), "4.0.7", txn)
		}, executor.Options{}.WithDatabase(catalog.MO_CATALOG).WithWaitCommittedLogApplied()))
		require.NoError(t, catalogExec(ctx,
			fmt.Sprintf("update mo_catalog.mo_upgrade_tenant set ready = 1 where upgrade_id = %d", upgradeID)))
		final := v4_0_9.Handler.Metadata()
		require.NoError(t, sqlExecutor.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
			return versions.UpdateVersionState(final.Version, final.VersionOffset, versions.StateReady, txn)
		}, executor.Options{}.WithDatabase(catalog.MO_CATALOG).WithWaitCommittedLogApplied()))
		btreeQuery := "select index_name from information_schema.statistics where table_schema = '" +
			dbName + "' and table_name = 't' and index_type = 'BTREE' order by index_name"
		res, err := sqlExecutor.Exec(ctx, btreeQuery, executor.Options{}.WithAccountID(tenantID))
		require.NoError(t, err)
		defer res.Close()
		require.Empty(t, res.Batches, "the late account still has the old persisted view")

		// Invalid credentials must not trigger catalog changes.
		badDB, err := sql.Open("mysql", fmt.Sprintf("%s#root#accountadmin:wrong@tcp(127.0.0.1:%d)/", accountName, port))
		require.NoError(t, err)
		defer badDB.Close()
		require.Error(t, badDB.PingContext(ctx))
		var createVersion string
		require.NoError(t, sysDB.QueryRowContext(ctx,
			fmt.Sprintf("select create_version from mo_catalog.mo_account where account_id = %d", tenantID)).Scan(&createVersion))
		require.Equal(t, "4.0.7", createVersion)

		db, err := sql.Open("mysql", fmt.Sprintf("%s#root#accountadmin:111@tcp(127.0.0.1:%d)/", accountName, port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx) // real MySQL authentication performs compensation
		require.NoError(t, err)
		defer conn.Close()
		require.ElementsMatch(t, []string{"PRIMARY", "idx_code", "uq_code"},
			statisticsIndexNames(t, ctx, conn, btreeQuery))
		require.NoError(t, sysDB.QueryRowContext(ctx,
			fmt.Sprintf("select create_version from mo_catalog.mo_account where account_id = %d", tenantID)).Scan(&createVersion))
		require.Equal(t, final.Version, createVersion)
		var specialized string
		require.NoError(t, conn.QueryRowContext(ctx,
			"select index_type from information_schema.statistics where table_schema = '"+dbName+
				"' and table_name = 't' and index_name = 'vidx'").Scan(&specialized))
		require.Equal(t, "ivfflat", specialized)
	})
}

func TestV408LoginRejectsAccountDroppedAfterAuthentication(t *testing.T) {
	embed.RunSingleCNBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		admin, err := db.Conn(ctx)
		require.NoError(t, err)
		defer admin.Close()
		const accountName = "statistics_deleted_upgrade_28999"
		_, err = admin.ExecContext(ctx, "create account "+accountName+" ADMIN_NAME 'root' IDENTIFIED BY '111'")
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, err := admin.ExecContext(cleanupCtx, "drop account if exists "+accountName)
			require.NoError(t, err)
		}()
		var tenantID int32
		require.NoError(t, admin.QueryRowContext(ctx,
			"select account_id from mo_catalog.mo_account where account_name = '"+accountName+"'").Scan(&tenantID))
		require.NoError(t, testutils.GetSQLExecutor(cn).ExecTxn(ctx, func(txn executor.TxnExecutor) error {
			return versions.UpgradeTenantVersion(tenantID, "4.0.7", txn)
		}, executor.Options{}.WithDatabase(catalog.MO_CATALOG).WithWaitCommittedLogApplied()))

		// Obtain a separate real frontend session without first logging into the
		// tenant, which would already run compensation and populate its cache.
		authConn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer authConn.Close()
		var connID uint32
		require.NoError(t, authConn.QueryRowContext(ctx, "select connection_id()").Scan(&connID))
		sessionManager := cn.RawService().(frontend.BaseService).SessionMgr()
		var ses *frontend.Session
		for _, candidate := range sessionManager.GetAllSessions() {
			candidate := candidate.(*frontend.Session)
			if candidate.GetResponser().GetU32(frontend.CONNID) == connID {
				ses = candidate
				break
			}
		}
		require.NotNil(t, ses)
		// Normal handshaking registers a session only after compensation. Remove
		// the borrowed sys session before changing its authenticated tenant.
		sessionManager.RemoveSession(ses)
		// Exercise the actual AuthenticateUser catalog transaction, splitting the
		// same two steps as the MySQL wrapper at their deterministic race window.
		_, err = ses.AuthenticateUser(ctx, accountName+"#root#accountadmin", "", nil, nil,
			func([]byte, []byte, []byte) bool { return true })
		require.NoError(t, err)
		require.Equal(t, "4.0.7", ses.GetCreateVersion())
		require.Equal(t, uint32(tenantID), ses.GetTenantInfo().GetTenantID())
		_, err = admin.ExecContext(ctx, "drop account "+accountName)
		require.NoError(t, err)

		for range 2 {
			// Retry also proves that no successful checked-tenant cache entry was
			// published for the failed post-authentication compensation.
			err = ses.MaybeUpgradeTenant(ctx, ses.GetCreateVersion(), int64(tenantID))
			var notFound *moerr.Error
			require.ErrorAs(t, err, &notFound)
			require.True(t, moerr.IsMoErrCode(notFound, moerr.ErrNotFound), "unexpected error: %v", err)
		}
		var alive int
		require.NoError(t, admin.QueryRowContext(ctx, "select 1").Scan(&alive))
		require.Equal(t, 1, alive, "account deletion must not terminate the CN")
	})
}

type statisticsUpgradeTxn struct {
	executor.TxnExecutor
	creates *int
}

func (txn *statisticsUpgradeTxn) Exec(statement string, opts executor.StatementOption) (executor.Result, error) {
	res, err := txn.TxnExecutor.Exec(statement, opts)
	if err == nil && statement == sysview.InformationSchemaStatisticsDDL {
		(*txn.creates)++
	}
	return res, err
}

func statisticsIndexNames(t *testing.T, ctx context.Context, conn *sql.Conn, query string) []string {
	t.Helper()
	rows, err := conn.QueryContext(ctx, query)
	require.NoError(t, err)
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())
	return names
}
