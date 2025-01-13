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

package v2_1_0

import (
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var clusterUpgEntries = []versions.UpgradeEntry{
	upg_mo_pitr_add_status,
	upg_mo_pitr_add_status_changed_time,
}

var upg_mo_pitr_add_status = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_PITR,
	UpgType:   versions.ADD_COLUMN,
	UpgSql:    "alter table mo_catalog.mo_pitr add column pitr_status TINYINT UNSIGNED DEFAULT 1 after pitr_unit",
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		colInfo, err := versions.CheckTableColumn(txn, accountId, catalog.MO_CATALOG, catalog.MO_PITR, "pitr_status")
		if err != nil {
			return false, err
		}
		return colInfo.IsExits, nil
	},
}

var upg_mo_pitr_add_status_changed_time = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_PITR,
	UpgType:   versions.ADD_COLUMN,
	UpgSql:    "alter table mo_catalog.mo_pitr add column pitr_status_changed_time TIMESTAMP DEFAULT UTC_TIMESTAMP after pitr_status",
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		colInfo, err := versions.CheckTableColumn(txn, accountId, catalog.MO_CATALOG, catalog.MO_PITR, "pitr_status_changed_time")
		if err != nil {
			return false, err
		}
		return colInfo.IsExits, nil
	},
}
