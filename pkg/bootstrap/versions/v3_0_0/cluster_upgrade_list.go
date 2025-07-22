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

package v3_0_0

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var clusterUpgEntries = []versions.UpgradeEntry{
	upg_mo_pitr,
	upg_mo_async_index_log_new,
}

var upg_mo_pitr = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_PITR,
	UpgType:   versions.MODIFY_COLUMN,
	UpgSql: fmt.Sprintf(
		"ALTER TABLE "+
			"		%s.%s "+
			"	MODIFY COLUMN create_time bigint not null,"+
			"	MODIFY COLUMN modified_time bigint not null,"+
			"	MODIFY COLUMN pitr_status_changed_time bigint not null",
		catalog.MO_CATALOG, catalog.MO_PITR,
	),

	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		colInfo, err := versions.CheckTableColumn(txn, accountId, catalog.MO_CATALOG, catalog.MO_PITR, "create_time")
		if err != nil {
			return false, err
		}

		if colInfo.ColType != "TIMESTAMP" {
			return true, nil
		}
		return false, nil
	},
}

var upg_mo_async_index_log_new = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_ASYNC_INDEX_LOG,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    frontend.MoCatalogMoCdcAsyncIndexLogDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, catalog.MO_CATALOG, catalog.MO_ASYNC_INDEX_LOG)
	},
}

