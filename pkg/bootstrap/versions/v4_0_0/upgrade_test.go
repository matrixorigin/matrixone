// Copyright 2025 Matrix Origin
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

package v4_0_0

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

func findUpgradeEntryIndex(entries []versions.UpgradeEntry, target versions.UpgradeEntry) int {
	for i, entry := range entries {
		if entry.Schema == target.Schema &&
			entry.TableName == target.TableName &&
			entry.UpgType == target.UpgType &&
			entry.UpgSql == target.UpgSql {
			return i
		}
	}
	return -1
}

func mockTenantUpgrade(t *testing.T) {
	drop_mo_retention := versions.UpgradeEntry{
		Schema:    catalog.MO_CATALOG,
		TableName: "mo_retention",
		UpgType:   versions.DROP_TABLE,
		UpgSql:    fmt.Sprintf("drop table if exists %s.%s", catalog.MO_CATALOG, "mo_retention"),
		CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
			exist, err := versions.CheckTableDefinition(txn, accountId, catalog.MO_CATALOG, "mo_retention")
			return !exist, err
		},
	}
	tenantUpgEntries = append(tenantUpgEntries, drop_mo_retention)
}

func Test_Upgrade(t *testing.T) {
	originalTenantUpgEntries := append([]versions.UpgradeEntry(nil), tenantUpgEntries...)
	defer func() {
		tenantUpgEntries = originalTenantUpgEntries
	}()
	mockTenantUpgrade(t)

	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()

			executor := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
				return executor.Result{}, nil
			}, txnOperator)

			metadata := Handler.Metadata()
			t.Logf("version metadata:%v", metadata)

			if err := Handler.Prepare(context.Background(), executor, true); err != nil {
				t.Errorf("Prepare() error = %v", err)
			}

			if err := Handler.HandleCreateFrameworkDeps(executor); err == nil {
				t.Errorf("HandleCreateFrameworkDeps() should report err")
			}

			if err := Handler.HandleClusterUpgrade(context.Background(), executor); err != nil {
				t.Errorf("HandleClusterUpgrade() error = %v", err)
			}

			if err := Handler.HandleTenantUpgrade(context.Background(), 0, executor); err != nil {
				t.Errorf("HandleTenantUpgrade() error = %v", err)
			}
		},
	)
}

func Test_versionHandle_HandleClusterUpgrade(t *testing.T) {
	originalClusterUpgEntries := append([]versions.UpgradeEntry(nil), clusterUpgEntries...)
	defer func() {
		clusterUpgEntries = originalClusterUpgEntries
	}()
	clusterUpgEntries = []versions.UpgradeEntry{}

	v := &versionHandle{
		metadata: versions.Version{
			Version: "v4.0.0",
		},
	}
	sid := ""
	txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()
	executor2 := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		return executor.Result{}, moerr.NewInvalidInputNoCtx("return error")
	}, txnOperator)

	err := v.HandleClusterUpgrade(context.Background(),
		executor2,
	)
	assert.Nil(t, err)
}

func Test_upg_statistics_view_check_error(t *testing.T) {
	stubs := gostub.Stub(&versions.CheckViewDefinition, func(txn executor.TxnExecutor, accountId uint32, schema string, viewName string) (bool, string, error) {
		return true, "", moerr.NewInternalErrorNoCtx("return error")
	})
	defer stubs.Reset()
	_, err := upg_information_schema_statistics.CheckFunc(nil, 0)
	assert.Error(t, err)
}

func Test_upg_columns_view_check_error(t *testing.T) {
	stubs := gostub.Stub(&versions.CheckViewDefinition, func(txn executor.TxnExecutor, accountId uint32, schema string, viewName string) (bool, string, error) {
		return true, "", moerr.NewInternalErrorNoCtx("return error")
	})
	defer stubs.Reset()
	_, err := upg_information_schema_columns.CheckFunc(nil, 0)
	assert.Error(t, err)
	_, err = upg_information_schema_columns_geometry_srid.CheckFunc(nil, 0)
	assert.Error(t, err)
}

func Test_upg_columns_view_check_match(t *testing.T) {
	stubs := gostub.Stub(&versions.CheckViewDefinition, func(txn executor.TxnExecutor, accountId uint32, schema string, viewName string) (bool, string, error) {
		return true, upg_information_schema_columns.UpgSql, nil
	})
	defer stubs.Reset()
	ok, err := upg_information_schema_columns.CheckFunc(nil, 0)
	assert.NoError(t, err)
	assert.True(t, ok)
	ok, err = upg_information_schema_columns_geometry_srid.CheckFunc(nil, 0)
	assert.NoError(t, err)
	assert.True(t, ok)
}

func Test_upg_statistics_view_check_match(t *testing.T) {
	stubs := gostub.Stub(&versions.CheckViewDefinition, func(txn executor.TxnExecutor, accountId uint32, schema string, viewName string) (bool, string, error) {
		return true, sysview.InformationSchemaStatisticsDDL, nil
	})
	defer stubs.Reset()
	ok, err := upg_information_schema_statistics.CheckFunc(nil, 0)
	assert.NoError(t, err)
	assert.True(t, ok)
}

func Test_upg_columns_view_check_mismatch(t *testing.T) {
	stubs := gostub.Stub(&versions.CheckViewDefinition, func(txn executor.TxnExecutor, accountId uint32, schema string, viewName string) (bool, string, error) {
		return true, "old_view_def", nil
	})
	defer stubs.Reset()
	ok, err := upg_information_schema_columns.CheckFunc(nil, 0)
	assert.NoError(t, err)
	assert.False(t, ok)
	ok, err = upg_information_schema_columns_geometry_srid.CheckFunc(nil, 0)
	assert.NoError(t, err)
	assert.False(t, ok)
}

func Test_upg_statistics_view_check_mismatch(t *testing.T) {
	stubs := gostub.Stub(&versions.CheckViewDefinition, func(txn executor.TxnExecutor, accountId uint32, schema string, viewName string) (bool, string, error) {
		return true, "old_view_def", nil
	})
	defer stubs.Reset()
	ok, err := upg_information_schema_statistics.CheckFunc(nil, 0)
	assert.NoError(t, err)
	assert.False(t, ok)
}

func Test_upg_mo_indexes_add_included_columns_check(t *testing.T) {
	stubs := gostub.Stub(&checkMoIndexesIncludedColumns, func(txn executor.TxnExecutor, accountId uint32) (versions.ColumnInfo, error) {
		return versions.ColumnInfo{IsExits: true}, nil
	})
	defer stubs.Reset()

	ok, err := upg_mo_indexes_add_included_columns_for_cluster.CheckFunc(nil, catalog.System_Account)
	assert.NoError(t, err)
	assert.True(t, ok)

	ok, err = upg_mo_indexes_add_included_columns_for_tenant.CheckFunc(nil, 100)
	assert.NoError(t, err)
	assert.True(t, ok)
}

func Test_upg_mo_indexes_add_included_columns_check_error(t *testing.T) {
	stubs := gostub.Stub(&checkMoIndexesIncludedColumns, func(txn executor.TxnExecutor, accountId uint32) (versions.ColumnInfo, error) {
		return versions.ColumnInfo{}, moerr.NewInternalErrorNoCtx("return error")
	})
	defer stubs.Reset()

	ok, err := upg_mo_indexes_add_included_columns_for_cluster.CheckFunc(nil, catalog.System_Account)
	assert.Error(t, err)
	assert.False(t, ok)
}

func Test_upg_mo_indexes_add_included_columns_ordering(t *testing.T) {
	clusterEntryIdx := findUpgradeEntryIndex(clusterUpgEntries, upg_mo_indexes_add_included_columns_for_cluster)
	clusterCreateIdx := findUpgradeEntryIndex(clusterUpgEntries, upg_mo_iscp_log_new)
	if assert.NotEqual(t, -1, clusterEntryIdx) && assert.NotEqual(t, -1, clusterCreateIdx) {
		assert.Less(t, clusterEntryIdx, clusterCreateIdx)
	}

	tenantEntryIdx := findUpgradeEntryIndex(tenantUpgEntries, upg_mo_indexes_add_included_columns_for_tenant)
	tenantCreateIdx := findUpgradeEntryIndex(tenantUpgEntries, enablePartitionMetadata)
	if assert.NotEqual(t, -1, tenantEntryIdx) && assert.NotEqual(t, -1, tenantCreateIdx) {
		assert.Less(t, tenantEntryIdx, tenantCreateIdx)
	}

	assert.True(t, strings.Contains(upg_mo_indexes_add_included_columns_for_cluster.UpgSql, catalog.IndexIncludedColumns))
	assert.True(t, strings.Contains(upg_mo_indexes_add_included_columns_for_cluster.UpgSql, "index_table_name"))
}
