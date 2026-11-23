// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v4_0_7

import (
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var (
	addSQLTaskRunClaim = sqlTaskOwnershipColumn(
		catalog.MOSysAsyncTask, "sql_task_run_id", "bigint unsigned", "0")
	addSQLTaskScheduleGeneration = sqlTaskOwnershipColumn(
		catalog.MOSQLTask, "schedule_generation", "bigint", "1")
	addSQLTaskRunningIndex = versions.UpgradeEntry{
		Schema:    catalog.MOTaskDB,
		TableName: catalog.MOSQLTaskRun,
		UpgType:   versions.ADD_INDEX,
		UpgSql:    "create index idx_task_running on mo_task.sql_task_run(account_id, status, task_name)",
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			return checkCatalogIndex(txn, accountID, catalog.MOTaskDB, catalog.MOSQLTaskRun,
				"idx_task_running", false, "account_id", "status", "task_name")
		},
	}
)

func sqlTaskOwnershipColumn(table, name, columnType, defaultValue string) versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    catalog.MOTaskDB,
		TableName: table,
		UpgType:   versions.ADD_COLUMN,
		UpgSql:    fmt.Sprintf("alter table mo_task.%s add column %s %s not null default %s", table, name, columnType, defaultValue),
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			column, err := versions.CheckTableColumn(txn, accountID, catalog.MOTaskDB, table, name)
			if err != nil || !column.IsExits {
				return false, err
			}
			if column.Nullable || !strings.EqualFold(column.ColType, columnType) || column.Default != defaultValue {
				return false, moerr.NewInvalidStateNoCtxf("mo_task.%s.%s has an incompatible definition", table, name)
			}
			return true, nil
		},
	}
}
