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
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

var errInjectedViewMetadataUpgrade = errors.New("injected View metadata catalog upgrade failure")

type failViewMetadataRefreshCreateTxn struct {
	executor.TxnExecutor
	failed bool
}

func (txn *failViewMetadataRefreshCreateTxn) Exec(
	sql string,
	opts executor.StatementOption,
) (executor.Result, error) {
	if !txn.failed && sql == catalog.MoViewRefreshDDL {
		txn.failed = true
		return executor.Result{}, errInjectedViewMetadataUpgrade
	}
	return txn.TxnExecutor.Exec(sql, opts)
}

type blockViewMetadataRefreshCreateTxn struct {
	executor.TxnExecutor
	executed chan struct{}
	release  chan struct{}
	once     sync.Once
}

func (txn *blockViewMetadataRefreshCreateTxn) Exec(
	sql string,
	opts executor.StatementOption,
) (executor.Result, error) {
	result, err := txn.TxnExecutor.Exec(sql, opts)
	if err == nil && sql == catalog.MoViewRefreshDDL {
		txn.once.Do(func() { close(txn.executed) })
		<-txn.release
	}
	return result, err
}

func isExpectedViewMetadataFenceOverlapError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var moErr *moerr.Error
	if !errors.As(err, &moErr) {
		return false
	}
	switch moErr.ErrorCode() {
	case moerr.ErrNoSuchTable, moerr.ErrBadDB,
		moerr.ErrTxnNeedRetry, moerr.ErrTxnNeedRetryWithDefChanged:
		return true
	default:
		return false
	}
}

func TestV406UpgradeCreatesViewMetadataCatalogTables(t *testing.T) {
	embed.RunSingleCNBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		service := cn.RawService().(cnservice.Service)
		sqlExecutor := testutils.GetSQLExecutor(cn)
		require.NotNil(t, sqlExecutor)

		t.Run("scoped restore reconciliation is transactional", func(t *testing.T) {
			db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			testScopedViewMetadataReconciliation(t, ctx, sqlExecutor, db)
		})

		states := []struct {
			name string
			drop []string
			want map[string]bool
		}{
			{
				name: "neither table exists",
				drop: []string{catalog.MO_VIEW_DEPENDENCIES, catalog.MO_VIEW_REFRESH},
				want: map[string]bool{
					catalog.MO_VIEW_DEPENDENCIES: false,
					catalog.MO_VIEW_REFRESH:      false,
				},
			},
			{
				name: "only dependencies exists",
				drop: []string{catalog.MO_VIEW_REFRESH},
				want: map[string]bool{
					catalog.MO_VIEW_DEPENDENCIES: true,
					catalog.MO_VIEW_REFRESH:      false,
				},
			},
			{
				name: "only refresh exists",
				drop: []string{catalog.MO_VIEW_DEPENDENCIES},
				want: map[string]bool{
					catalog.MO_VIEW_DEPENDENCIES: false,
					catalog.MO_VIEW_REFRESH:      true,
				},
			},
			{
				name: "both tables exist",
				want: map[string]bool{
					catalog.MO_VIEW_DEPENDENCIES: true,
					catalog.MO_VIEW_REFRESH:      true,
				},
			},
		}

		for _, state := range states {
			t.Run(state.name, func(t *testing.T) {
				deleteViewMetadataCatalogTables(t, ctx, service, state.drop...)
				requireViewMetadataCatalogState(t, ctx, sqlExecutor, state.want)
				runV406ClusterUpgrade(t, ctx, sqlExecutor, nil)
				requireViewMetadataCatalogState(t, ctx, sqlExecutor, map[string]bool{
					catalog.MO_VIEW_DEPENDENCIES: true,
					catalog.MO_VIEW_REFRESH:      true,
				})
			})
		}

		t.Run("catalog fence overlaps uncommitted upgrade", func(t *testing.T) {
			deleteViewMetadataCatalogTables(
				t, ctx, service, catalog.MO_VIEW_DEPENDENCIES, catalog.MO_VIEW_REFRESH)

			executed := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			releaseUpgrade := func() { releaseOnce.Do(func() { close(release) }) }
			blocked := &blockViewMetadataRefreshCreateTxn{executed: executed, release: release}
			upgradeDone := make(chan error, 1)
			upgradeFinished := false
			go func() {
				upgradeDone <- runV406ClusterUpgradeWithTxn(
					ctx, sqlExecutor, func(txn executor.TxnExecutor) executor.TxnExecutor {
						blocked.TxnExecutor = txn
						return blocked
					})
			}()
			t.Cleanup(func() {
				releaseUpgrade()
				if !upgradeFinished {
					<-upgradeDone
				}
			})

			select {
			case <-executed:
			case upgradeErr := <-upgradeDone:
				upgradeFinished = true
				t.Fatalf("catalog upgrade ended before the overlap fence: %v", upgradeErr)
			case <-time.After(time.Second):
				t.Fatal("catalog upgrade did not reach the uncommitted refresh-table barrier")
			}

			fenceCtx, cancelFence := context.WithTimeout(ctx, 200*time.Millisecond)
			fenceErr := compile.RequireViewMetadataRevalidation(fenceCtx, sqlExecutor)
			cancelFence()
			require.Error(t, fenceErr)
			require.True(t, isExpectedViewMetadataFenceOverlapError(fenceErr), fenceErr)

			releaseUpgrade()
			select {
			case upgradeErr := <-upgradeDone:
				upgradeFinished = true
				require.NoError(t, upgradeErr)
			case <-time.After(time.Second):
				t.Fatal("catalog upgrade did not commit after releasing the overlap barrier")
			}
			require.NoError(t, compile.RequireViewMetadataRevalidation(ctx, sqlExecutor))
			requireViewMetadataCatalogState(t, ctx, sqlExecutor, map[string]bool{
				catalog.MO_VIEW_DEPENDENCIES: true,
				catalog.MO_VIEW_REFRESH:      true,
			})
		})

		t.Run("failed attempt rolls back and retry converges", func(t *testing.T) {
			deleteViewMetadataCatalogTables(
				t, ctx, service, catalog.MO_VIEW_DEPENDENCIES, catalog.MO_VIEW_REFRESH)

			injected := &failViewMetadataRefreshCreateTxn{}
			upgradeErr := runV406ClusterUpgradeWithTxn(
				ctx, sqlExecutor, func(txn executor.TxnExecutor) executor.TxnExecutor {
					injected.TxnExecutor = txn
					return injected
				})
			require.ErrorIs(t, upgradeErr, errInjectedViewMetadataUpgrade)
			require.True(t, injected.failed)
			requireViewMetadataCatalogState(t, ctx, sqlExecutor, map[string]bool{
				catalog.MO_VIEW_DEPENDENCIES: false,
				catalog.MO_VIEW_REFRESH:      false,
			})

			runV406ClusterUpgrade(t, ctx, sqlExecutor, nil)
			requireViewMetadataCatalogState(t, ctx, sqlExecutor, map[string]bool{
				catalog.MO_VIEW_DEPENDENCIES: true,
				catalog.MO_VIEW_REFRESH:      true,
			})
		})
	})
}

func deleteViewMetadataCatalogTables(
	t *testing.T,
	ctx context.Context,
	service cnservice.Service,
	tableNames ...string,
) {
	t.Helper()
	if len(tableNames) == 0 {
		return
	}

	ctx = defines.AttachAccount(ctx, catalog.System_Account, catalog.System_User, catalog.System_Role)
	txn, err := service.GetTxnClient().New(ctx, timestamp.Timestamp{})
	require.NoError(t, err)
	committed := false
	defer func() {
		if !committed {
			_ = txn.Rollback(ctx)
		}
	}()

	engine := service.GetEngine()
	require.NoError(t, engine.New(ctx, txn))
	database, err := engine.Database(ctx, catalog.MO_CATALOG, txn)
	require.NoError(t, err)
	for _, tableName := range tableNames {
		relation, err := database.Relation(ctx, tableName, nil)
		require.NoError(t, err)
		for _, index := range relation.GetTableDef(ctx).GetIndexes() {
			if index.IndexTableName != "" {
				require.NoError(t, database.Delete(ctx, index.IndexTableName))
			}
		}
		require.NoError(t, database.Delete(ctx, tableName))
	}
	require.NoError(t, txn.Commit(ctx))
	committed = true
}

func requireViewMetadataCatalogState(
	t *testing.T,
	ctx context.Context,
	sqlExecutor executor.SQLExecutor,
	want map[string]bool,
) {
	t.Helper()
	got := make(map[string]bool, len(want))
	err := sqlExecutor.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		for _, tableName := range []string{catalog.MO_VIEW_DEPENDENCIES, catalog.MO_VIEW_REFRESH} {
			exists, err := versions.CheckTableDefinition(
				txn, catalog.System_Account, catalog.MO_CATALOG, tableName)
			if err != nil {
				return err
			}
			got[tableName] = exists
		}
		return nil
	}, viewMetadataUpgradeExecutorOptions())
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func runV406ClusterUpgrade(
	t *testing.T,
	ctx context.Context,
	sqlExecutor executor.SQLExecutor,
	wrap func(executor.TxnExecutor) executor.TxnExecutor,
) {
	t.Helper()
	require.NoError(t, runV406ClusterUpgradeWithTxn(ctx, sqlExecutor, wrap))
}

func runV406ClusterUpgradeWithTxn(
	ctx context.Context,
	sqlExecutor executor.SQLExecutor,
	wrap func(executor.TxnExecutor) executor.TxnExecutor,
) error {
	return sqlExecutor.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		if wrap != nil {
			txn = wrap(txn)
		}
		for _, entry := range viewMetadataCatalogUpgradeEntries() {
			if err := entry.Upgrade(txn, catalog.System_Account); err != nil {
				return err
			}
		}
		return nil
	}, viewMetadataUpgradeExecutorOptions())
}

func viewMetadataCatalogUpgradeEntries() []versions.UpgradeEntry {
	// TestUpgradeEntries in v4_0_6 pins that these constants are registered in
	// this order. Keep the real-engine fixture scoped to this catalog closure so
	// unrelated offset entries do not rerun against an already-current catalog.
	entries := make([]versions.UpgradeEntry, 0, 2)
	for _, table := range []struct {
		name string
		ddl  string
	}{
		{name: catalog.MO_VIEW_DEPENDENCIES, ddl: catalog.MoViewDependenciesDDL},
		{name: catalog.MO_VIEW_REFRESH, ddl: catalog.MoViewRefreshDDL},
	} {
		entries = append(entries, versions.UpgradeEntry{
			Schema:    catalog.MO_CATALOG,
			TableName: table.name,
			UpgType:   versions.CREATE_NEW_TABLE,
			UpgSql:    table.ddl,
			CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
				return versions.CheckTableDefinition(
					txn, accountID, catalog.MO_CATALOG, table.name)
			},
		})
	}
	return entries
}

// Reuse the catalog-upgrade fixture: three constant Views distinguish target,
// same-database sibling and other-database controls without creating a cluster.
func testScopedViewMetadataReconciliation(t *testing.T, ctx context.Context, sqlExecutor executor.SQLExecutor, db *sql.DB) {
	opts := viewMetadataUpgradeExecutorOptions()
	execDDL := func(statement string) error {
		_, err := db.ExecContext(ctx, statement)
		return err
	}
	exec := func(sql string) error {
		result, err := sqlExecutor.Exec(ctx, sql, opts)
		if err == nil {
			result.Close()
		}
		return err
	}
	for _, db := range []string{"view_reconcile_a", "view_reconcile_b"} {
		require.NoError(t, execDDL("create database "+db))
		t.Cleanup(func() {
			require.NoError(t, execDDL("drop database "+db))
			for _, table := range []string{catalog.MO_VIEW_REFRESH, catalog.MO_VIEW_DEPENDENCIES} {
				require.NoError(t, exec("delete from mo_catalog."+table+" where account_id=0 and target_database_name='"+db+"'"))
			}
		})
	}
	for _, name := range []string{"view_reconcile_a.v", "view_reconcile_a.sibling", "view_reconcile_b.v"} {
		require.NoError(t, execDDL("create view "+name+" as select 1 as x"))
	}
	// Exercise both public restore entry points while admission stays disabled.
	// A single row distinguishes the restored definition/data from the live one.
	require.NoError(t, execDDL("create table view_reconcile_a.source(x int)"))
	require.NoError(t, execDDL("insert into view_reconcile_a.source values(1)"))
	require.NoError(t, execDDL("create snapshot view_reconcile_snapshot for database view_reconcile_a"))
	t.Cleanup(func() { require.NoError(t, execDDL("drop snapshot view_reconcile_snapshot")) })
	for _, scope := range []string{"table view_reconcile_a.source", "database view_reconcile_a"} {
		require.NoError(t, execDDL("update view_reconcile_a.source set x=2"))
		require.NoError(t, execDDL("restore "+scope+"{snapshot='view_reconcile_snapshot'}"))
		var value int
		require.NoError(t, db.QueryRowContext(ctx, "select x from view_reconcile_a.source").Scan(&value))
		require.Equal(t, 1, value)
	}
	require.False(t, compile.ViewMetadataRefreshEnabled(""))
	readState := func() []string {
		rows, err := db.QueryContext(ctx, "select concat(target_database_name,'.',target_relation_name,':',status) "+
			"from mo_catalog.mo_view_refresh where account_id=0 and target_database_name in "+
			"('view_reconcile_a','view_reconcile_b') order by target_database_name,target_relation_name")
		require.NoError(t, err)
		defer rows.Close()
		var state []string
		for rows.Next() {
			var value string
			require.NoError(t, rows.Scan(&value))
			state = append(state, value)
		}
		require.NoError(t, rows.Err())
		return state
	}
	reconcile := func(database, relation string, abort bool) error {
		statements, err := compile.ReconcileScopedViewMetadataSQL(0, database, relation, 77)
		if err != nil {
			return err
		}
		return sqlExecutor.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
			run := func(sql string) error {
				result, err := txn.Exec(sql, executor.StatementOption{})
				if err == nil {
					result.Close()
				}
				return err
			}
			if err := catalog.LockViewMetadataLifecycle(run); err != nil {
				return err
			}
			for _, sql := range statements {
				if err := run(sql); err != nil {
					return err
				}
			}
			if abort {
				return errInjectedViewMetadataUpgrade
			}
			return nil
		}, opts)
	}
	require.Empty(t, readState())
	require.ErrorIs(t, reconcile("view_reconcile_a", "v", true), errInjectedViewMetadataUpgrade)
	require.Empty(t, readState(), "rolled-back seeding must not publish recovery progress")
	require.NoError(t, reconcile("view_reconcile_a", "v", false))
	require.Equal(t, []string{"view_reconcile_a.v:DISCOVERING"}, readState())
	require.NoError(t, reconcile("view_reconcile_a", "v", false))
	require.Equal(t, []string{"view_reconcile_a.v:DISCOVERING"}, readState(), "repeated reconciliation is idempotent")
	require.NoError(t, reconcile("view_reconcile_a", "", false))
	require.Equal(t, []string{"view_reconcile_a.sibling:DISCOVERING", "view_reconcile_a.v:DISCOVERING"}, readState())
	require.NoError(t, execDDL("drop view view_reconcile_a.v"))
	require.NoError(t, reconcile("view_reconcile_a", "v", false))
	require.Equal(t, []string{"view_reconcile_a.sibling:DISCOVERING"}, readState(), "scoped orphan cleanup preserves its sibling")
}

func viewMetadataUpgradeExecutorOptions() executor.Options {
	return executor.Options{}.
		WithDatabase(catalog.MO_CATALOG).
		WithAccountID(catalog.System_Account).
		WithWaitCommittedLogApplied()
}
