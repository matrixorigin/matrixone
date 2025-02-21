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

package v2_0_2

import (
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

var clusterUpgEntries = []versions.UpgradeEntry{
	upg_mo_cdc_watermark,
	upg_mo_pubs_add_account_name,
	upg_mo_subs_add_sub_account_name,
	upg_mo_subs_add_pub_account_id,
	upg_mo_account_lock,
	upg_drop_information_schema_table_constraints,
	upg_create_information_schema_table_constraints,
}

var upg_mo_cdc_watermark = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_CDC_WATERMARK,
	UpgType:   versions.DROP_COLUMN,
	UpgSql:    "alter table mo_catalog.mo_cdc_watermark drop column table_id, drop primary key, add primary key(account_id,task_id,db_name,table_name)",
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		colInfo, err := versions.CheckTableColumn(txn, accountId, catalog.MO_CATALOG, catalog.MO_CDC_WATERMARK, "table_id")
		if err != nil {
			return false, err
		}
		return !colInfo.IsExits, nil
	},
}

var upg_mo_pubs_add_account_name = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_PUBS,
	UpgType:   versions.ADD_COLUMN,
	UpgSql:    "alter table mo_catalog.mo_pubs add column account_name varchar(300) after account_id",
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		colInfo, err := versions.CheckTableColumn(txn, accountId, catalog.MO_CATALOG, catalog.MO_PUBS, "account_name")
		if err != nil {
			return false, err
		}
		return colInfo.IsExits, nil
	},
	PostSql: "UPDATE mo_catalog.mo_pubs t1 INNER JOIN mo_catalog.mo_account t2 ON t1.account_id = t2.account_id SET t1.account_name = t2.account_name",
}

var upg_mo_subs_add_sub_account_name = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_SUBS,
	UpgType:   versions.ADD_COLUMN,
	UpgSql:    "alter table mo_catalog.mo_subs add column sub_account_name VARCHAR(300) NOT NULL after sub_account_id",
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		colInfo, err := versions.CheckTableColumn(txn, accountId, catalog.MO_CATALOG, catalog.MO_SUBS, "sub_account_name")
		if err != nil {
			return false, err
		}
		return colInfo.IsExits, nil
	},
	PostSql: "UPDATE mo_catalog.mo_subs t1 INNER JOIN mo_catalog.mo_account t2 ON t1.sub_account_id = t2.account_id SET t1.sub_account_name = t2.account_name",
}

var upg_mo_subs_add_pub_account_id = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_SUBS,
	UpgType:   versions.ADD_COLUMN,
	UpgSql:    "alter table mo_catalog.mo_subs add column pub_account_id INT NOT NULL after sub_time",
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		colInfo, err := versions.CheckTableColumn(txn, accountId, catalog.MO_CATALOG, catalog.MO_SUBS, "pub_account_id")
		if err != nil {
			return false, err
		}
		return colInfo.IsExits, nil
	},
	PostSql: "UPDATE mo_catalog.mo_subs t1 INNER JOIN mo_catalog.mo_account t2 ON t1.pub_account_name = t2.account_name SET t1.pub_account_id = t2.account_id",
}

var upg_mo_account_lock = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_ACCOUNT_LOCK,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    frontend.MoCatalogMoAccountLockDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, catalog.MO_CATALOG, catalog.MO_ACCOUNT_LOCK)
	},
}

var upg_drop_information_schema_table_constraints = versions.UpgradeEntry{
	Schema:    sysview.InformationDBConst,
	TableName: "table_constraints",
	UpgType:   versions.DROP_TABLE,
	UpgSql:    "drop table information_schema.table_constraints",
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exist, err := versions.CheckTableDefinition(txn, accountId, sysview.InformationDBConst, "table_constraints")
		return !exist, err
	},
}

var upg_create_information_schema_table_constraints = versions.UpgradeEntry{
	Schema:    sysview.InformationDBConst,
	TableName: "table_constraints",
	UpgType:   versions.CREATE_VIEW,
	UpgSql:    sysview.InformationSchemaTableConstraintsDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, sysview.InformationDBConst, "table_constraints")
	},
}
