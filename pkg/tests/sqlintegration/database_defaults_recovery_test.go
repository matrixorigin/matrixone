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

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
)

func TestDatabaseDefaultsFailureAndRecovery(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 180*time.Second)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		const schema = "c03_recovery"
		defer cleanupSQLIntegration(t, cn, "drop snapshot if exists c03_recovery_snapshot", "drop pitr if exists c03_recovery_pitr", "drop database if exists "+schema,
			"drop account if exists c03_restore_source", "drop account if exists c03_restore_target")
		exec := func(t *testing.T, connection *sql.DB, statement string) {
			t.Helper()
			_, err := connection.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		column := func(t *testing.T, connection *sql.DB, table, want string) {
			t.Helper()
			var actual string
			require.NoError(t, connection.QueryRowContext(ctx,
				"select collation_name from information_schema.columns where table_schema='c03_recovery' and table_name=? and column_name='v'", table).Scan(&actual))
			require.Equal(t, want, actual)
		}
		exec(t, db, "create database "+schema+" collate utf8mb4_bin")
		exec(t, db, "create table "+schema+".t(id int primary key,v varchar(8),index iv(v))")
		exec(t, db, "insert into "+schema+".t values (1,'ß'),(2,'ss')")

		t.Run("commit failure preserves every owner", func(t *testing.T) {
			wasEnabled := fault.Status()
			fault.Enable()
			defer func() {
				_, err := fault.RemoveFaultPoint(context.Background(), objectio.FJ_CNCommitAfterWorkspaceDumpFailed)
				require.NoError(t, err)
				if !wasEnabled {
					fault.Disable()
				}
			}()
			internal := testutils.GetSQLExecutor(cn)
			for _, statement := range []string{
				"alter database " + schema + " collate utf8mb4_unicode_ci",
				"alter table " + schema + ".t default collate=utf8mb4_unicode_ci",
				"alter table " + schema + ".t convert to character set utf8mb4 collate utf8mb4_unicode_ci",
			} {
				func() {
					defer func() {
						_, err := fault.RemoveFaultPoint(context.Background(), objectio.FJ_CNCommitAfterWorkspaceDumpFailed)
						require.NoError(t, err)
					}()
					err := internal.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
						result, err := tx.Exec(statement, executor.StatementOption{})
						if err != nil {
							return err
						}
						result.Close()
						// 使用现有 commit 边界；只在本集群完成 DDL 后武装，
						// 不添加生产 hook，也不依赖后台事务是否先到达。
						return fault.AddFaultPoint(ctx, objectio.FJ_CNCommitAfterWorkspaceDumpFailed, ":::", "echo", 0, "c03 injected commit failure", false)
					}, executor.Options{}.WithAccountID(catalog.System_Account).WithDatabase(schema).WithWaitCommittedLogApplied())
					require.ErrorContains(t, err, "c03 injected commit failure")
				}()
				var defaults, tableDefault string
				require.NoError(t, db.QueryRowContext(ctx, "select default_collation_name from information_schema.schemata where schema_name=?", schema).Scan(&defaults))
				require.Equal(t, "utf8mb4_bin", defaults)
				require.NoError(t, db.QueryRowContext(ctx, "select table_collation from information_schema.tables where table_schema=? and table_name='t'", schema).Scan(&tableDefault))
				require.Equal(t, "utf8mb4_bin", tableDefault)
				column(t, db, "t", "utf8mb4_bin")
				var matched, leftovers int
				require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+schema+".t force index(iv) where v='ss'").Scan(&matched))
				require.Equal(t, 1, matched)
				require.NoError(t, db.QueryRowContext(ctx, "select count(*) from information_schema.tables where table_schema=? and table_name<>'t'", schema).Scan(&leftovers))
				require.Zero(t, leftovers, "failed COPY must not retain a replacement table")
			}
		})

		t.Run("corrupt generation fails closed in metadata and planning", func(t *testing.T) {
			internal := testutils.GetSQLExecutor(cn)
			setVersion := func(version uint64) {
				t.Helper()
				result, err := internal.Exec(ctx, fmt.Sprintf("update mo_catalog.mo_database_defaults set version=%d where account_id=0 and database_id=(select dat_id from mo_catalog.mo_database where datname='%s' and account_id=0)", version, schema), executor.Options{}.WithAccountID(catalog.System_Account).WithWaitCommittedLogApplied())
				require.NoError(t, err)
				result.Close()
			}
			setVersion(0)
			defer setVersion(1)
			var actual sql.NullString
			require.NoError(t, db.QueryRowContext(ctx, "select default_collation_name from information_schema.schemata where schema_name=?", schema).Scan(&actual))
			require.False(t, actual.Valid, "a corrupt persisted row must not look like an admitted default")
			_, err := db.ExecContext(ctx, "create table "+schema+".rejected(v varchar(8))")
			require.ErrorContains(t, err, "invalid database default metadata")
		})

		t.Run("snapshot and PITR honor restoration granularity", func(t *testing.T) {
			exec(t, db, "alter database "+schema+" collate utf8mb4_unicode_ci")
			exec(t, db, "alter table "+schema+".t convert to character set utf8mb4 collate utf8mb4_unicode_ci")
			exec(t, db, "create pitr c03_recovery_pitr for database "+schema+" range 1 'h'")
			exec(t, db, "create snapshot c03_recovery_snapshot for database "+schema)
			// PITR 的公开语法以秒为精度；观察真实时钟跨过前述已提交
			// 数据所在秒，不用 sleep 作为并发屏障或猜测提交发生时间。
			var previous, at string
			clock := "select date_format(current_timestamp(6), '%Y-%m-%d %H:%i:%s')"
			require.NoError(t, db.QueryRowContext(ctx, clock).Scan(&previous))
			require.Eventually(t, func() bool {
				err := db.QueryRowContext(ctx, clock).Scan(&at)
				return err == nil && at != previous
			}, 5*time.Second, 20*time.Millisecond)
			exec(t, db, "alter database "+schema+" collate utf8mb4_bin")
			exec(t, db, "alter table "+schema+".t convert to character set utf8mb4 collate utf8mb4_bin")
			for _, restore := range []string{
				"restore table " + schema + ".t {snapshot='c03_recovery_snapshot'}",
				"restore database " + schema + " table t from pitr c03_recovery_pitr '" + at + "'",
			} {
				exec(t, db, restore)
				column(t, db, "t", "utf8mb4_unicode_ci")
				var current string
				require.NoError(t, db.QueryRowContext(ctx, "select default_collation_name from information_schema.schemata where schema_name=?", schema).Scan(&current))
				require.Equal(t, "utf8mb4_bin", current, "table-only restore must not overwrite the database")
			}
			exec(t, db, "restore database "+schema+" from pitr c03_recovery_pitr '"+at+"'")
			exec(t, db, "create table "+schema+".inherited(v varchar(8))")
			column(t, db, "inherited", "utf8mb4_unicode_ci")
			var matched int
			require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+schema+".t force index(iv) where v='ss'").Scan(&matched))
			require.Equal(t, 2, matched)
		})

		t.Run("cross account identity and revision remapping", func(t *testing.T) {
			exec(t, db, "create account c03_restore_source admin_name 'root' identified by '111'")
			exec(t, db, "create account c03_restore_target admin_name 'root' identified by '111'")
			open := func(name string) *sql.DB {
				t.Helper()
				connection, err := sql.Open("mysql", fmt.Sprintf("%s#root#accountadmin:111@tcp(127.0.0.1:%d)/", name, cn.GetServiceConfig().CN.Frontend.Port))
				require.NoError(t, err)
				return connection
			}
			source := open("c03_restore_source")
			defer source.Close()
			target := open("c03_restore_target")
			defer target.Close()
			exec(t, source, "create database "+schema+" collate utf8_unicode_ci")
			exec(t, source, "create table "+schema+".t(v varchar(8),index iv(v))")
			exec(t, source, "insert into "+schema+".t values ('ß'),('ss')")
			exec(t, db, "create snapshot c03_cross_account for account c03_restore_source")
			defer cleanupSQLIntegration(t, cn, "drop snapshot if exists c03_cross_account")
			exec(t, source, "alter database "+schema+" collate utf8mb4_bin")
			exec(t, db, "restore account c03_restore_source {snapshot='c03_cross_account'} to account c03_restore_target")
			exec(t, target, "create table "+schema+".inherited(v varchar(8))")
			column(t, target, "inherited", "utf8_unicode_ci")
			var sourceRule string
			require.NoError(t, source.QueryRowContext(ctx, "select default_collation_name from information_schema.schemata where schema_name=?", schema).Scan(&sourceRule))
			require.Equal(t, "utf8mb4_bin", sourceRule)
			var matched int
			require.NoError(t, target.QueryRowContext(ctx, "select count(*) from "+schema+".t force index(iv) where v='ss'").Scan(&matched))
			require.Equal(t, 2, matched)
		})
	})
}
