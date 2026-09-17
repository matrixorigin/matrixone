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
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_7"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

func TestV407UpgradeRefreshesStatistics(t *testing.T) {
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
				var showCount int
				for showRows.Next() {
					showCount++
				}
				require.NoError(t, showRows.Err())
				require.NoError(t, showRows.Close())
				require.Equal(t, 3, showCount, "SHOW INDEX already uses the current runtime definition")
				specializedQuery := "select index_type from information_schema.statistics where table_schema = '" +
					dbName + "' and table_name = 't' and index_name = 'vidx'"
				var originalAlgorithm string
				require.NoError(t, conn.QueryRowContext(ctx, specializedQuery).Scan(&originalAlgorithm))
				require.Equal(t, "ivfflat", originalAlgorithm)

				for run := 0; run < 2; run++ {
					var creates int
					require.NoError(t, sqlExecutor.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
						return v4_0_7.Handler.HandleTenantUpgrade(ctx, int32(test.accountID),
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
