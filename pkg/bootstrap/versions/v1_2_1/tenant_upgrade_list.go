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

package v1_2_1

import (
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

var tenantUpgEntries = []versions.UpgradeEntry{
	upg_mo_mysql_compatibility_mode1,
	upg_information_schema_files,
}

var upg_mo_mysql_compatibility_mode1 = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: "mo_mysql_compatibility_mode",
	UpgType:   versions.MODIFY_METADATA,
	UpgSql:    "insert into mo_catalog.mo_mysql_compatibility_mode(account_id, account_name, variable_name, variable_value, system_variables) values (current_account_id(), current_account_name(), 'keep_user_target_list_in_result', '1',  true)",
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		sql := "select * from mo_catalog.mo_mysql_compatibility_mode where variable_name = 'keep_user_target_list_in_result'"
		return versions.CheckTableDataExist(txn, accountId, sql)
	},
}

var upg_information_schema_files = versions.UpgradeEntry{
	Schema:    sysview.InformationDBConst,
	TableName: "files",
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    sysview.InformationSchemaFilesDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, sysview.InformationDBConst, "files")
	},
}
