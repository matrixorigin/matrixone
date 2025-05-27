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

package v2_2_0

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var clusterUpgEntries = []versions.UpgradeEntry{
	upg_mo_merge_settings_new,
	upg_mo_merge_settings_init_data,
}

var upg_mo_merge_settings_new = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_MERGE_SETTINGS,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    frontend.MoCatalogMergeSettingsDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, catalog.MO_CATALOG, catalog.MO_MERGE_SETTINGS)
	},
}

var upg_mo_merge_settings_init_data = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_MERGE_SETTINGS,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    frontend.MoCatalogMergeSettingsInitData,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		sql := fmt.Sprintf("SELECT tid FROM %s.%s limit 1", catalog.MO_CATALOG, catalog.MO_MERGE_SETTINGS)
		res, err := txn.Exec(sql, executor.StatementOption{}.WithAccountID(accountId))
		if err != nil {
			return false, err
		}
		defer res.Close()
		total := 0
		res.ReadRows(func(rows int, _ []*vector.Vector) bool {
			total += rows
			return true
		})
		return total > 0, nil
	},
}
