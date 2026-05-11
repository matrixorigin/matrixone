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

package v3_0_2

import (
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var clusterUpgEntries = []versions.UpgradeEntry{
	upg_create_mo_task_sql_task,
	upg_create_mo_task_sql_task_run,
}

var upg_create_mo_task_sql_task = versions.UpgradeEntry{
	Schema:    catalog.MOTaskDB,
	TableName: catalog.MOSQLTask,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    frontend.MoTaskSQLTaskDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, catalog.MOTaskDB, catalog.MOSQLTask)
	},
}

var upg_create_mo_task_sql_task_run = versions.UpgradeEntry{
	Schema:    catalog.MOTaskDB,
	TableName: catalog.MOSQLTaskRun,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    frontend.MoTaskSQLTaskRunDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, catalog.MOTaskDB, catalog.MOSQLTaskRun)
	},
}
