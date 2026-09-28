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

package v4_0_8

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

func TestStatisticsUpgradeRegistration(t *testing.T) {
	metadata := Handler.Metadata()
	require.Equal(t, "4.0.8", metadata.Version)
	require.Equal(t, "4.0.7", metadata.MinUpgradeVersion)
	require.Greater(t, versions.Compare(metadata.Version, "4.0.7"), 0, "old workers must reject the target version")
	require.False(t, metadata.CanDirectUpgrade("4.0.6"), "do not bypass the 4.0.7 provenance migration")
	require.Equal(t, versions.Yes, metadata.UpgradeTenant)
	require.Equal(t, versions.Yes, metadata.UpgradeCluster)
	require.Equal(t, uint32(len(tenantUpgEntries)+len(clusterUpgEntries)), metadata.VersionOffset)

	var found bool
	for _, entry := range tenantUpgEntries {
		if entry.Schema == sysview.InformationDBConst && entry.TableName == "STATISTICS" {
			found = true
			require.Equal(t, versions.MODIFY_VIEW, entry.UpgType)
			require.Equal(t, sysview.InformationSchemaStatisticsDDL, entry.UpgSql)
		}
	}
	require.True(t, found)
}

func TestStatisticsUpgradeHandlerLifecycle(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		ctx := context.Background()
		txn := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
			if strings.HasPrefix(sql, "SELECT reldatabase, relname, account_id") {
				return statisticsStringResult(t, catalog.MO_CATALOG), nil
			}
			require.True(t, strings.HasPrefix(sql, "SELECT tbl.rel_createsql"), "unexpected SQL: %s", sql)
			return statisticsStringResult(t, sysview.InformationSchemaStatisticsDDL), nil
		})
		require.NoError(t, Handler.Prepare(ctx, txn, true))
		require.NoError(t, Handler.HandleClusterUpgrade(ctx, txn))
		require.ErrorContains(t, Handler.HandleCreateFrameworkDeps(txn), "Only v1.2.0 can initialize upgrade framework")
		require.NoError(t, Handler.HandleTenantUpgrade(ctx, int32(catalog.System_Account), txn))

		injected := errors.New("injected STATISTICS definition query failure")
		failedTxn := newVersionTxnExecutor(t, func(string) (executor.Result, error) {
			return executor.Result{}, injected
		})
		require.ErrorIs(t, Handler.HandleTenantUpgrade(ctx, 7, failedTxn), injected)
	})
}

func TestStatisticsUpgradeRefreshesOnlyStaleDefinitions(t *testing.T) {
	legacy := strings.Replace(sysview.InformationSchemaStatisticsDDL,
		"coalesce(nullif(`idx`.`algo`, ''), 'BTREE')", "`idx`.`algo`", 1)
	require.NotEqual(t, sysview.InformationSchemaStatisticsDDL, legacy)
	for _, test := range []struct {
		name       string
		definition string
		refresh    bool
		lowercase  bool
	}{
		{name: "legacy", definition: legacy, refresh: true},
		{name: "missing", refresh: true},
		{name: "current", definition: sysview.InformationSchemaStatisticsDDL},
		{name: "lowercase_legacy", definition: legacy, refresh: true, lowercase: true},
		{name: "lowercase_current", definition: sysview.InformationSchemaStatisticsDDL, lowercase: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				entry := upgradeInformationSchemaStatistics()
				definition := test.definition
				var statements []string
				txn := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
					switch {
					case strings.HasPrefix(sql, "SELECT tbl.rel_createsql"):
						if definition == "" || (test.lowercase && strings.Contains(sql, "tbl.relname = 'STATISTICS'")) {
							return executor.Result{}, nil
						}
						return statisticsStringResult(t, definition), nil
					case sql == "SELECT mo_ctl('cn', 'GetProtocolVersion', '')":
						return statisticsStringResult(t, `{"method":"GETPROTOCOLVERSION","result":"cn-a:61"}`), nil
					case sql == entry.PreSql:
						statements = append(statements, sql)
						definition = ""
					case sql == entry.UpgSql:
						statements = append(statements, sql)
						definition = sql
					default:
						t.Fatalf("unexpected SQL: %s", sql)
					}
					return executor.Result{}, nil
				})
				require.NoError(t, entry.Upgrade(txn, 7))
				if test.refresh {
					require.Equal(t, []string{entry.PreSql, entry.UpgSql}, statements)
				} else {
					require.Empty(t, statements)
				}
				require.Equal(t, sysview.InformationSchemaStatisticsDDL, definition)
				statements = nil
				require.NoError(t, entry.Upgrade(txn, 7))
				require.Empty(t, statements, "a second upgrade must not recreate the view")
			})
		})
	}
}

func statisticsStringResult(t *testing.T, values ...string) executor.Result {
	t.Helper()
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	res := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, mp)
	res.NewBatchWithRowCount(len(values))
	require.NoError(t, executor.AppendStringRows(res, 0, values))
	return res.GetResult()
}

func newVersionTxnExecutor(t *testing.T, mocker func(string) (executor.Result, error)) executor.TxnExecutor {
	t.Helper()
	txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
	return executor.NewMemTxnExecutor(mocker, txnOperator)
}
