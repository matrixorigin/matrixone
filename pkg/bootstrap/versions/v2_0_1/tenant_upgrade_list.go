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

package v2_0_1

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var tenantUpgEntries = []versions.UpgradeEntry{
	upg_mo_user_add_password_last_changed,
	upg_mo_user_add_password_history,
	upg_mo_user_add_login_attempts,
	upg_mo_user_add_lock_time,
	drop_mo_pubs,
}

var upg_mo_user_add_password_last_changed = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_USER,
	UpgType:   versions.ADD_COLUMN,
	UpgSql:    "alter table mo_catalog.mo_user add column password_last_changed timestamp default utc_timestamp",
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		colInfo, err := versions.CheckTableColumn(txn, accountId, catalog.MO_CATALOG, catalog.MO_USER, "password_last_changed")
		if err != nil {
			return false, err
		}

		if colInfo.IsExits {
			return true, nil
		}
		return false, nil
	},
}

var upg_mo_user_add_password_history = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_USER,
	UpgType:   versions.ADD_COLUMN,
	UpgSql:    "alter table mo_catalog.mo_user add column password_history text default '[]'",
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		colInfo, err := versions.CheckTableColumn(txn, accountId, catalog.MO_CATALOG, catalog.MO_USER, "password_history")
		if err != nil {
			return false, err
		}

		if colInfo.IsExits {
			return true, nil
		}
		return false, nil
	},
}

var upg_mo_user_add_login_attempts = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_USER,
	UpgType:   versions.ADD_COLUMN,
	UpgSql:    "alter table mo_catalog.mo_user add column login_attempts int unsigned default 0",
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		colInfo, err := versions.CheckTableColumn(txn, accountId, catalog.MO_CATALOG, catalog.MO_USER, "login_attempts")
		if err != nil {
			return false, err
		}

		if colInfo.IsExits {
			return true, nil
		}
		return false, nil
	},
}

var upg_mo_user_add_lock_time = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_USER,
	UpgType:   versions.ADD_COLUMN,
	UpgSql:    "alter table mo_catalog.mo_user add column lock_time timestamp default utc_timestamp",
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		colInfo, err := versions.CheckTableColumn(txn, accountId, catalog.MO_CATALOG, catalog.MO_USER, "lock_time")
		if err != nil {
			return false, err
		}
		if colInfo.IsExits {
			return true, nil
		}
		return false, nil
	},
}

var drop_mo_pubs = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_PUBS,
	UpgType:   versions.DROP_TABLE,
	UpgSql:    fmt.Sprintf("drop table %s.%s", catalog.MO_CATALOG, catalog.MO_PUBS),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		if accountId == catalog.System_Account {
			return true, nil
		}

		exist, err := versions.CheckTableDefinition(txn, accountId, catalog.MO_CATALOG, catalog.MO_PUBS)
		if err != nil {
			return false, err
		}
		return !exist, nil
	},
}
