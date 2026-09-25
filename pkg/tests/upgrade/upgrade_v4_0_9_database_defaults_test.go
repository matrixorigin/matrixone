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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_9"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

func TestV409UpgradeDatabaseDefaults(t *testing.T) {
	embed.RunSingleCNBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		sysDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer sysDB.Close()
		const account = "defaults_upgrade_28998"
		_, err = sysDB.ExecContext(ctx, "create account "+account+" admin_name 'root' identified by '111'")
		require.NoError(t, err)
		defer func() {
			cleanup, done := context.WithTimeout(context.Background(), 30*time.Second)
			defer done()
			_, err := sysDB.ExecContext(cleanup, "drop account "+account)
			require.NoError(t, err)
		}()
		var tenant uint32
		require.NoError(t, sysDB.QueryRowContext(ctx, "select account_id from mo_catalog.mo_account where account_name='"+account+"'").Scan(&tenant))
		internal := testutils.GetSQLExecutor(cn)
		upgrade := func(ctx context.Context, id uint32) error {
			return internal.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
				return v4_0_9.Handler.HandleTenantUpgrade(ctx, int32(id), tx)
			}, executor.Options{}.WithAccountID(id).WithDatabase(catalog.MO_CATALOG).WithWaitCommittedLogApplied())
		}
		for _, tc := range []struct {
			name, user string
			id         uint32
		}{
			{"system", "dump", 0}, {"tenant", account + "#root#accountadmin", tenant},
		} {
			t.Run(tc.name, func(t *testing.T) {
				db, err := sql.Open("mysql", fmt.Sprintf("%s:111@tcp(127.0.0.1:%d)/", tc.user, port))
				require.NoError(t, err)
				defer db.Close()
				conn, err := db.Conn(ctx)
				require.NoError(t, err)
				defer conn.Close()
				exec := func(s string) { t.Helper(); _, err := conn.ExecContext(ctx, s); require.NoError(t, err, s) }
				exec("create database defaults_upgrade_db")
				defer func() {
					cleanup, done := context.WithTimeout(context.Background(), 30*time.Second)
					defer done()
					require.NoError(t, upgrade(cleanup, tc.id))
					_, err := conn.ExecContext(cleanup, "drop database defaults_upgrade_db")
					require.NoError(t, err)
				}()
				// Restore the persisted 4.0.8 schema, then exercise the actual handler.
				require.NoError(t, internal.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
					for _, s := range []string{"drop view information_schema.schemata", sysview.InformationSchemaSchemataLegacyDDL, "drop table mo_catalog.mo_database_defaults"} {
						r, err := tx.Exec(s, versions.UpgradeStatementOption(tc.id))
						if err != nil {
							return err
						}
						r.Close()
					}
					return nil
				}, executor.Options{}.WithAccountID(tc.id).WithDatabase(catalog.MO_CATALOG).WithWaitCommittedLogApplied()))
				for i := 0; i < 2; i++ {
					require.NoError(t, upgrade(ctx, tc.id))
					var count int
					require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_database_defaults").Scan(&count))
					require.Zero(t, count, "upgrade must not invent historical defaults")
					var collation string
					require.NoError(t, conn.QueryRowContext(ctx, "select default_collation_name from information_schema.schemata where schema_name='defaults_upgrade_db'").Scan(&collation))
					require.Equal(t, "utf8mb4_general_ci", collation)
				}
				exec("alter database defaults_upgrade_db collate utf8mb4_bin")
				exec("create table defaults_upgrade_db.t(v varchar(8))")
				exec("insert into defaults_upgrade_db.t values ('a'),('B')")
				var min, max string
				require.NoError(t, conn.QueryRowContext(ctx, "select min(v),max(v) from defaults_upgrade_db.t").Scan(&min, &max))
				require.Equal(t, "B", min)
				require.Equal(t, "a", max)
			})
		}
	})
}
