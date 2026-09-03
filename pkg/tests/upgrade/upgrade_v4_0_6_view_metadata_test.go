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
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
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

func TestV406UpgradeCreatesViewMetadataCatalogTables(t *testing.T) {
	embed.RunSingleCNBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		service := cn.RawService().(cnservice.Service)
		sqlExecutor := testutils.GetSQLExecutor(cn)
		require.NotNil(t, sqlExecutor)

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

func viewMetadataUpgradeExecutorOptions() executor.Options {
	return executor.Options{}.
		WithDatabase(catalog.MO_CATALOG).
		WithAccountID(catalog.System_Account).
		WithWaitCommittedLogApplied()
}
