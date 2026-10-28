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

package embed

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_7"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestCatalogUpgradeOrphanTaskPagesOnRealEngine(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		sql := cn.RawService().(cnservice.Service).GetSQLExecutor()
		opts := executor.Options{}.WithAccountID(0).WithWaitCommittedLogApplied()
		const prefix = "catalog-page-test-"
		values := make([]string, 0, 130)
		for i := 0; i < 129; i++ {
			values = append(values, fmt.Sprintf("('%s%03d','sql-task:%d',99)", prefix, i, 8000000+i))
		}
		values = append(values, "('catalog-page-test-retain','other-task',99)")
		result, err := sql.Exec(ctx, "insert into mo_task.sys_async_task(task_metadata_id,task_parent_id,task_status) values "+strings.Join(values, ","), opts)
		if err == nil {
			result.Close()
		}
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cleanupCancel()
			result, err := sql.Exec(cleanupCtx, "delete from mo_task.sys_async_task where task_metadata_id like 'catalog-page-test-%'", opts)
			if err == nil {
				result.Close()
			}
			require.NoError(t, err)
		}()
		count := func(filter string) int64 {
			t.Helper()
			result, err := sql.Exec(ctx, "select count(*) from mo_task.sys_async_task where "+filter, opts)
			require.NoError(t, err)
			defer result.Close()
			var value int64
			result.ReadRows(func(_ int, cols []*vector.Vector) bool {
				value = vector.GetFixedAtWithTypeCheck[int64](cols[0], 0)
				return false
			})
			return value
		}
		orphans := "task_metadata_id like 'catalog-page-test-%' and task_parent_id like 'sql-task:%'"
		require.Equal(t, int64(129), count(orphans))
		// The sixth consolidated cluster entry owns orphan-child cleanup.
		const cleanupEntry = 5
		runPage := func(rollback bool) (bool, error) {
			var more bool
			rollbackErr := errors.New("force rollback after cleanup")
			err := sql.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
				var err error
				more, err = v4_0_7.Handler.UpgradeClusterEntryPage(ctx, cleanupEntry, tx, 64<<20)
				if err != nil {
					return err
				}
				if rollback {
					return rollbackErr
				}
				return nil
			}, opts)
			if rollback {
				require.ErrorIs(t, err, rollbackErr)
			}
			return more, err
		}
		more, _ := runPage(true)
		require.True(t, more)
		require.Equal(t, int64(129), count(orphans), "failed page must roll back")
		more, err = runPage(false)
		require.NoError(t, err)
		require.True(t, more)
		require.Equal(t, int64(1), count(orphans))
		more, err = runPage(false)
		require.NoError(t, err)
		require.True(t, more)
		more, err = runPage(false)
		require.NoError(t, err)
		require.False(t, more)
		require.Zero(t, count(orphans))
		require.Equal(t, int64(1), count("task_metadata_id = 'catalog-page-test-retain'"))
	})
}

// An index metadata table with the same database/name in another tenant must
// not satisfy the current tenant's column check.
func TestCatalogIndexMetadataUpgradeUsesTargetAccount(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		sys := recoverySQLClient(t, cn)
		t.Cleanup(func() {
			_, _ = sys.ExecContext(context.Background(), "drop account if exists meta_scope_tenant")
			_, _ = sys.ExecContext(context.Background(), "drop database if exists meta_scope_db")
			_ = sys.Close()
		})
		for _, statement := range []string{
			"create account meta_scope_tenant admin_name 'root' identified by '111'",
			"create database meta_scope_db",
			"create table meta_scope_db.meta(id int primary key)",
		} {
			_, err := sys.ExecContext(ctx, statement)
			require.NoError(t, err)
		}
		cfg := mysql.NewConfig()
		cfg.User, cfg.Passwd = "meta_scope_tenant:root", "111"
		cfg.Net, cfg.Addr = "tcp", fmt.Sprintf("127.0.0.1:%d", cn.GetServiceConfig().CN.Frontend.Port)
		connector, err := mysql.NewConnector(cfg)
		require.NoError(t, err)
		tenant := sql.OpenDB(connector)
		defer tenant.Close()
		for _, statement := range []string{
			"create database meta_scope_db",
			"create table meta_scope_db.meta(id int primary key, nrow bigint not null default 0, build_ts bigint not null default 0)",
		} {
			_, err := tenant.ExecContext(ctx, statement)
			require.NoError(t, err)
		}
		exec := cn.RawService().(cnservice.Service).GetSQLExecutor()
		err = exec.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
			return v4_0_7.Handler.UpgradeIndexMetadataTable(ctx, 0, tx, v4_0_7.IndexMetadataKey{"meta_scope_db", "meta", catalog.Hnsw_TblType_Metadata})
		}, executor.Options{}.WithDatabase(catalog.MO_CATALOG).WithAccountID(0).WithWaitCommittedLogApplied())
		require.NoError(t, err)
		var count int
		require.NoError(t, sys.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_columns where account_id=0 and att_database='meta_scope_db' and att_relname='meta' and attname in ('nrow','build_ts')").Scan(&count))
		require.Equal(t, 2, count)
	})
}

func TestRestoreHistoricalIcebergCatalogPreservesAllocator(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db := recoverySQLClient(t, cn)
		defer db.Close()
		exec := cn.RawService().(cnservice.Service).GetSQLExecutor()
		internalCtx := context.WithValue(ctx, defines.InternalExecutorKey{}, true)
		internalExec := func(statement string) {
			t.Helper()
			result, err := exec.Exec(internalCtx, statement, executor.Options{}.WithAccountID(0).WithWaitCommittedLogApplied())
			if err == nil {
				result.Close()
			}
			require.NoError(t, err)
		}
		_, err = db.ExecContext(ctx, "create iceberg catalog restore_iceberg_old with ('uri'='https://catalog.example.com/rest','type'='rest','warehouse'='s3://warehouse','auth_mode'='none')")
		require.NoError(t, err)
		var oldID uint64
		require.NoError(t, db.QueryRowContext(ctx, "select catalog_id from mo_catalog.mo_iceberg_catalogs where name='restore_iceberg_old'").Scan(&oldID))
		internalExec("drop index catalog_id_allocator on mo_catalog.mo_iceberg_catalogs")
		_, err = db.ExecContext(ctx, "create snapshot restore_iceberg_legacy for account sys")
		require.NoError(t, err)
		internalExec("create index catalog_id_allocator on mo_catalog.mo_iceberg_catalogs(catalog_id)")
		_, err = db.ExecContext(ctx, "restore account sys {snapshot='restore_iceberg_legacy'}")
		require.NoError(t, err)
		var restoredID uint64
		require.NoError(t, db.QueryRowContext(ctx, "select catalog_id from mo_catalog.mo_iceberg_catalogs where name='restore_iceberg_old'").Scan(&restoredID))
		require.Equal(t, oldID, restoredID)
		var indexCount int
		require.NoError(t, db.QueryRowContext(ctx, "select count(distinct i.id) from mo_catalog.mo_indexes i join mo_catalog.mo_tables t on i.table_id=t.rel_id where t.account_id=0 and t.reldatabase='mo_catalog' and t.relname='mo_iceberg_catalogs' and i.name='catalog_id_allocator'").Scan(&indexCount))
		require.Equal(t, 1, indexCount)
		_, err = db.ExecContext(ctx, "create iceberg catalog restore_iceberg_new with ('uri'='https://catalog.example.com/rest','type'='rest','warehouse'='s3://warehouse','auth_mode'='none')")
		require.NoError(t, err)
		var newID uint64
		require.NoError(t, db.QueryRowContext(ctx, "select catalog_id from mo_catalog.mo_iceberg_catalogs where name='restore_iceberg_new'").Scan(&newID))
		require.Greater(t, newID, oldID)

		// A conflicting historical definition must fail the whole restore, leaving
		// both current rows and the current allocator intact.
		internalExec("drop index catalog_id_allocator on mo_catalog.mo_iceberg_catalogs")
		internalExec("create index catalog_id_allocator on mo_catalog.mo_iceberg_catalogs(name)")
		_, err = db.ExecContext(ctx, "create snapshot restore_iceberg_conflict for account sys")
		require.NoError(t, err)
		internalExec("drop index catalog_id_allocator on mo_catalog.mo_iceberg_catalogs")
		internalExec("create index catalog_id_allocator on mo_catalog.mo_iceberg_catalogs(catalog_id)")
		_, err = db.ExecContext(ctx, "create iceberg catalog restore_iceberg_survivor with ('uri'='https://catalog.example.com/rest','type'='rest','warehouse'='s3://warehouse','auth_mode'='none')")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "restore account sys {snapshot='restore_iceberg_conflict'}")
		require.ErrorContains(t, err, "unexpected Iceberg catalog_id_allocator definition")
		var survivorCount int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_iceberg_catalogs where name='restore_iceberg_survivor'").Scan(&survivorCount))
		require.Equal(t, 1, survivorCount)
		require.NoError(t, db.QueryRowContext(ctx, "select count(distinct i.id) from mo_catalog.mo_indexes i join mo_catalog.mo_tables t on i.table_id=t.rel_id where t.account_id=0 and t.reldatabase='mo_catalog' and t.relname='mo_iceberg_catalogs' and i.name='catalog_id_allocator' and i.column_name='catalog_id'").Scan(&indexCount))
		require.Equal(t, 1, indexCount)
	})
}

func TestCatalogVersionOrderingUsesSemanticVersionAndOffset(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		exec := cn.RawService().(cnservice.Service).GetSQLExecutor()
		ctx := context.WithValue(t.Context(), defines.InternalExecutorKey{}, true)
		rollback := errors.New("rollback test catalog versions")
		err = exec.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
			for _, statement := range []string{
				"insert into mo_catalog.mo_version values ('4.0.9', 1, 2, '2099-01-01 00:00:00', '2099-01-01 00:00:00')",
				"insert into mo_catalog.mo_version values ('4.0.10', 1, 2, '2099-01-01 00:00:00', '2099-01-01 00:00:00')",
				"insert into mo_catalog.mo_version values ('4.0.10', 2, 2, '2098-01-01 00:00:00', '2098-01-01 00:00:00')",
			} {
				result, err := tx.Exec(statement, executor.StatementOption{})
				if err != nil {
					return err
				}
				result.Close()
			}
			latest, err := versions.GetLatestVersion(tx)
			if err != nil {
				return err
			}
			require.Equal(t, "4.0.10", latest.Version)
			require.Equal(t, uint32(2), latest.VersionOffset)
			result, err := tx.Exec("insert into mo_catalog.mo_version values ('4.0.11', 1, 0, '2097-01-01 00:00:00', '2097-01-01 00:00:00')", executor.StatementOption{})
			if err != nil {
				return err
			}
			result.Close()
			latest, err = versions.GetLatestVersion(tx)
			if err != nil {
				return err
			}
			require.Equal(t, "4.0.11", latest.Version)
			ready, err := versions.MustGetLatestReadyVersion(tx)
			if err != nil {
				return err
			}
			require.Equal(t, "4.0.10", ready)
			return rollback
		}, executor.Options{}.WithDatabase(catalog.MO_CATALOG).WithAccountID(0).WithWaitCommittedLogApplied())
		require.ErrorIs(t, err, rollback)
	})
}
