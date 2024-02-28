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

package v1_2_0

import (
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"strings"
)

var upgradeEntries = []versions.UpgradeEntry{upg_mo_account, upg_mo_pub, upg_sys_async_task}

var upg_mo_account = versions.UpgradeEntry{
	Schema:    "mo_catalog",
	TableName: "mo_account",
	UpgType:   versions.ADD_COLUMN,
	TableType: versions.BASE_TABLE,
	UpgSql:    "alter table `mo_account` add column `create_version` varchar(50) default '1.2.0' after suspended_time",
	CheckFunc: func(txn executor.TxnExecutor) (bool, error) {
		colInfo, err := versions.CheckTableColumn(txn, "mo_catalog", "mo_account", "create_version")
		if err != nil {
			return false, err
		}

		if colInfo.IsExits {
			return true, nil
		}
		return false, nil
	},
}

var upg_mo_pub = versions.UpgradeEntry{
	Schema:    "mo_catalog",
	TableName: "mo_pubs",
	UpgType:   versions.ADD_COLUMN,
	TableType: versions.BASE_TABLE,
	UpgSql:    "alter table `mo_catalog`.`mo_pubs` add column `update_time` timestamp",
	CheckFunc: func(txn executor.TxnExecutor) (bool, error) {
		colInfo, err := versions.CheckTableColumn(txn, "mo_catalog", "mo_pubs", "update_time")
		if err != nil {
			return false, err
		}

		if colInfo.IsExits {
			return true, nil
		}
		return false, nil
	},
}

var upg_sys_async_task = versions.UpgradeEntry{
	Schema:    "mo_task",
	TableName: "sys_async_task",
	UpgType:   versions.MODIFY_COLUMN,
	TableType: versions.BASE_TABLE,
	UpgSql:    "alter table `mo_task`.`sys_async_task` modify task_id bigint auto_increment",
	CheckFunc: func(txn executor.TxnExecutor) (bool, error) {
		colInfo, err := versions.CheckTableColumn(txn, "mo_task", "sys_async_task", "task_id")
		if err != nil {
			return false, err
		}

		if colInfo.IsExits {
			if strings.EqualFold(colInfo.ColType, versions.T_int64) {
				return true, nil
			}
		}
		return false, nil
	},
}
