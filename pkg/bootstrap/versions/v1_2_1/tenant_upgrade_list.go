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
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var tenantUpgEntries = []versions.UpgradeEntry{upg_wuxiliang_test}

const INFORMATION_SCHEMA = "information_schema"

var upg_wuxiliang_test = versions.UpgradeEntry{
	Schema:    INFORMATION_SCHEMA,
	TableName: `wuxiliang_tenant_view_v121`,
	UpgType:   versions.CREATE_VIEW,
	TableType: versions.SYSTEM_VIEW,
	UpgSql: fmt.Sprintf("CREATE VIEW IF NOT EXISTS `%s`.`%s` as "+
		"select rel_id as view_id, "+
		"relname as view_name, "+
		"reldatabase as schema_name, "+
		"rel_comment as comment, "+
		"rel_createsql as create_sql "+
		"from mo_catalog.mo_tables "+
		"where relkind = 'v';",
		INFORMATION_SCHEMA, "wuxiliang_tenant_view_v121"),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		isExisted, _, err := versions.CheckViewDefinition(txn, accountId, INFORMATION_SCHEMA, "wuxiliang_tenant_view_v121")
		if err != nil {
			return false, err
		}

		if isExisted {
			return true, nil
		}
		return false, nil
	},
}
