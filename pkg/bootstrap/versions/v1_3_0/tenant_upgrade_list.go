// Copyright 2024 Matrix Origin
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

package v1_3_0

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var tenantUpgEntries = []versions.UpgradeEntry{
	upg_systemMetrics_server_snapshot_usage,
	upg_mo_snapshots,
	upg_mo_retention,
}

const viewServerSnapshotUsage = "server_snapshot_usage"
const viewServerSnapshotUsageDDL = "CREATE VIEW IF NOT EXISTS `system_metrics`.`server_snapshot_usage` as select `collecttime`, `value`, `node`, `role`, `account` from `system_metrics`.`metric` where `metric_name` = \"server_snapshot_usage\""

var upg_systemMetrics_server_snapshot_usage = versions.UpgradeEntry{
	Schema:    catalog.MO_SYSTEM_METRICS,
	TableName: viewServerSnapshotUsage,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    viewServerSnapshotUsageDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, catalog.MO_SYSTEM_METRICS, viewServerSnapshotUsage)
	},
}

var upg_mo_snapshots = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_SNAPSHOTS,
	UpgType:   versions.MODIFY_COLUMN,
	PreSql:    fmt.Sprintf("alter table %s.%s drop column ts;", catalog.MO_CATALOG, catalog.MO_SNAPSHOTS),
	UpgSql:    fmt.Sprintf("alter table %s.%s add column ts bigint after sname", catalog.MO_CATALOG, catalog.MO_SNAPSHOTS),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		colInfo, err := versions.CheckTableColumn(txn, accountId, catalog.MO_CATALOG, catalog.MO_SNAPSHOTS, "ts")
		if err != nil {
			return false, err
		}

		if colInfo.ColType != "TIMESTAMP" {
			return true, nil
		}
		return false, nil
	},
}

var upg_mo_retention = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_RETENTION,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    frontend.MoCatalogMoRetentionDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		isExist, err := versions.CheckTableDefinition(txn, accountId, catalog.MO_CATALOG, catalog.MO_RETENTION)
		if err != nil {
			return false, err
		}

		if isExist {
			return true, nil
		}
		needUpgradePubSub = true
		return false, nil
	},
}
