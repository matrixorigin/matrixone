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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var tenantUpgEntries = []versions.UpgradeEntry{upg_sql_dut_total}

// CREATE VIEW IF NOT EXISTS `system_metrics`.`sql_statement_duration_total` as select `collecttime`, `value`, `node`, `role`, `account`, `type` from `system_metrics`.`metric` where `metric_name` = "sql_statement_duration_total"
var upg_sql_dut_total = versions.UpgradeEntry{
	Schema:    `system_metrics`,
	TableName: `sql_statement_duration_total`,
	UpgType:   versions.CREATE_VIEW,
	TableType: versions.SYSTEM_VIEW,
	UpgSql: fmt.Sprintf("CREATE VIEW IF NOT EXISTS `%s`.`%s` as "+
		"select `collecttime`, `value`, `node`, `role`, `account`, `type` "+
		"from `system_metrics`.`metric` "+
		"where `metric_name` = 'sql_statement_duration_total'",
		catalog.MO_SYSTEM_METRICS, "sql_statement_duration_total"),
	CheckFunc: func(txn executor.TxnExecutor) (bool, error) {
		return false, nil
	},
}
