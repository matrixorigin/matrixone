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

package v2_0_0

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

var tenantUpgEntries = []versions.UpgradeEntry{
	upg_systemMetrics_server_snapshot_usage,
	upg_mo_snapshots,
	upg_mo_retention,
	upg_information_schema_columns,
	upg_information_schema_schemata,
	upg_system_stmt_info_add_column_conn_id,
	upg_system_stmt_info_add_column_cu,
	upg_mo_user_add_password_last_changed,
	upg_mo_user_add_password_history,
	upg_mo_user_add_login_attempts,
	upg_mo_user_add_lock_time,
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
		return versions.CheckTableDefinition(txn, accountId, catalog.MO_CATALOG, catalog.MO_RETENTION)
	},
}

var upg_information_schema_columns = versions.UpgradeEntry{
	Schema:    sysview.InformationDBConst,
	TableName: "COLUMNS",
	UpgType:   versions.MODIFY_VIEW,
	UpgSql:    sysview.InformationSchemaColumnsDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, viewDef, err := versions.CheckViewDefinition(txn, accountId, sysview.InformationDBConst, "COLUMNS")
		if err != nil {
			return false, err
		}

		if exists && viewDef == sysview.InformationSchemaColumnsDDL {
			return true, nil
		}
		return false, nil
	},
	PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;", sysview.InformationDBConst, "COLUMNS"),
}

var upg_information_schema_schemata = versions.UpgradeEntry{
	Schema:    sysview.InformationDBConst,
	TableName: "SCHEMATA",
	UpgType:   versions.MODIFY_VIEW,
	UpgSql:    sysview.InformationSchemaSchemataDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, viewDef, err := versions.CheckViewDefinition(txn, accountId, sysview.InformationDBConst, "SCHEMATA")
		if err != nil {
			return false, err
		}

		if exists && viewDef == sysview.InformationSchemaSchemataDDL {
			return true, nil
		}
		return false, nil
	},
	PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;", sysview.InformationDBConst, "SCHEMATA"),
}

var upg_mo_user_add_password_last_changed = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_USER,
	UpgType:   versions.MODIFY_COLUMN,
	UpgSql:    "alter table mo_catalog.mo_user add column password_last_changed timestamp default utc_timestamp",
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		colInfo, err := versions.CheckTableColumn(txn, accountId, catalog.MO_CATALOG, catalog.MO_USER, "password_last_changed")
		if err != nil {
			return false, err
		}

		if colInfo.ColType == "TIMESTAMP" {
			return true, nil
		}
		return false, nil
	},
}

var upg_mo_user_add_password_history = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_USER,
	UpgType:   versions.MODIFY_COLUMN,
	UpgSql:    "alter table mo_catalog.mo_user add column password_history text default '[]'",
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		colInfo, err := versions.CheckTableColumn(txn, accountId, catalog.MO_CATALOG, catalog.MO_USER, "password_history")
		if err != nil {
			return false, err
		}

		if colInfo.ColType == "TEXT" {
			return true, nil
		}
		return false, nil
	},
}

var upg_mo_user_add_login_attempts = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_USER,
	UpgType:   versions.MODIFY_COLUMN,
	UpgSql:    "alter table mo_catalog.mo_user add column login_attempts int unsigned default 0",
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		colInfo, err := versions.CheckTableColumn(txn, accountId, catalog.MO_CATALOG, catalog.MO_USER, "login_attempts")
		if err != nil {
			return false, err
		}

		if colInfo.ColType == "INT UNSIGNED" {
			return true, nil
		}
		return false, nil
	},
}

var upg_mo_user_add_lock_time = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_USER,
	UpgType:   versions.MODIFY_COLUMN,
	UpgSql:    "alter table mo_catalog.mo_user add column lock_time timestamp default utc_timestamp",
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		colInfo, err := versions.CheckTableColumn(txn, accountId, catalog.MO_CATALOG, catalog.MO_USER, "lock_time")
		if err != nil {
			return false, err
		}
		if colInfo.ColType == "TIMESTAMP" {
			return true, nil
		}
		return false, nil
	},
}
