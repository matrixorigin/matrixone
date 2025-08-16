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
	upg_mo_iscp_log_new,
	upg_drop_mo_stored_procedure,
	upg_create_mo_stored_procedure,
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

var upg_mo_iscp_log_new = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_ISCP_LOG,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    frontend.MoCatalogMoISCPLogDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, catalog.MO_CATALOG, catalog.MO_ISCP_LOG)
	},
}
var upg_drop_mo_stored_procedure = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_STORED_PROCEDURE,
	UpgType:   versions.ADD_COLUMN,
	UpgSql:    "drop table mo_catalog.mo_stored_procedure",
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exist, err := versions.CheckTableDefinition(txn, accountId, catalog.MO_CATALOG, catalog.MO_STORED_PROCEDURE)
		return !exist, err
	},
}

var upg_create_mo_stored_procedure = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_STORED_PROCEDURE,
	UpgType:   versions.ADD_COLUMN,
	UpgSql:    frontend.MoCatalogMoStoredProcedureDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exist, err := versions.CheckTableDefinition(txn, accountId, catalog.MO_CATALOG, catalog.MO_STORED_PROCEDURE)
		return exist, err
	},
}
