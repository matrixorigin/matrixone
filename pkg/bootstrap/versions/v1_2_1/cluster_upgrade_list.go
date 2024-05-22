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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var clusterUpgEntries = []versions.UpgradeEntry{
	upg_system_logInfo,
}

// viewSystemLogInfoDDL113 = "CREATE VIEW IF NOT EXISTS `system`.`log_info` as select `trace_id`, `span_id`, `span_kind`, `node_uuid`, `node_type`, `timestamp`, `logger_name`, `level`, `caller`, `message`, `extra`, `stack` from `system`.`rawlog` where `raw_item` = \"log_info\""
const viewSystemLogInfoDDL120 = "CREATE VIEW IF NOT EXISTS `system`.`log_info` as select `trace_id`, `span_id`, `span_kind`, `node_uuid`, `node_type`, `timestamp`, `logger_name`, `level`, `caller`, `message`, `extra`, `stack`, `session_id`, `statement_id` from `system`.`rawlog` where `raw_item` = \"log_info\""

var upg_system_logInfo = versions.UpgradeEntry{
	Schema:    catalog.MO_SYSTEM,
	TableName: "log_info",
	UpgType:   versions.MODIFY_VIEW,
	UpgSql:    viewSystemLogInfoDDL120,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, viewDef, err := versions.CheckViewDefinition(txn, accountId, catalog.MO_SYSTEM, "log_info")
		if err != nil {
			return false, err
		}

		if exists && viewDef == viewSystemLogInfoDDL120 {
			return true, nil
		}
		return false, nil
	},
	PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;", catalog.MO_SYSTEM, "log_info"),
}
