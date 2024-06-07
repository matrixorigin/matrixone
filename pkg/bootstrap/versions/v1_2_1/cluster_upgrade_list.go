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
	upg_system_rawlog_comment,
	upg_system_statementInto_comment,
	upg_systemMetric_metric_comment,
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

const (
	systemRawlogComment121        = `read merge data from log, error, span[mo_no_del_hint]`
	systemStatementInfoComment121 = `record each statement and stats info[mo_no_del_hint]`
	systemMetricComment121        = `metric data[mo_no_del_hint]`
)

func getDDLAlterComment(db, tbl, comment string) string {
	return fmt.Sprintf("ALTER TABLE %s.%s COMMENT %q", db, tbl, comment)
}

var upg_system_rawlog_comment = versions.UpgradeEntry{
	Schema:    catalog.MO_SYSTEM,
	TableName: catalog.MO_RAWLOG,
	UpgType:   versions.MODIFY_TABLE_COMMENT,
	UpgSql:    getDDLAlterComment(catalog.MO_SYSTEM, catalog.MO_RAWLOG, systemRawlogComment121),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, comment, err := versions.CheckTableComment(txn, accountId, catalog.MO_SYSTEM, catalog.MO_RAWLOG)
		if err != nil {
			return false, err
		}
		if exists && comment == systemRawlogComment121 {
			return true, nil
		}
		return false, nil
	},
}

var upg_system_statementInto_comment = versions.UpgradeEntry{
	Schema:    catalog.MO_SYSTEM,
	TableName: catalog.MO_STATEMENT,
	UpgType:   versions.MODIFY_TABLE_COMMENT,
	UpgSql:    getDDLAlterComment(catalog.MO_SYSTEM, catalog.MO_STATEMENT, systemStatementInfoComment121),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, comment, err := versions.CheckTableComment(txn, accountId, catalog.MO_SYSTEM, catalog.MO_STATEMENT)
		if err != nil {
			return false, err
		}
		if exists && comment == systemStatementInfoComment121 {
			return true, nil
		}
		return false, nil
	},
}

var upg_systemMetric_metric_comment = versions.UpgradeEntry{
	Schema:    catalog.MO_SYSTEM_METRICS,
	TableName: catalog.MO_METRIC,
	UpgType:   versions.MODIFY_TABLE_COMMENT,
	UpgSql:    getDDLAlterComment(catalog.MO_SYSTEM_METRICS, catalog.MO_METRIC, systemMetricComment121),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, comment, err := versions.CheckTableComment(txn, accountId, catalog.MO_SYSTEM_METRICS, catalog.MO_METRIC)
		if err != nil {
			return false, err
		}
		if exists && comment == systemMetricComment121 {
			return true, nil
		}
		return false, nil
	},
}
