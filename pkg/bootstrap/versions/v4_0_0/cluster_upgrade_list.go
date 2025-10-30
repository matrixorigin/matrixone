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

package v4_0_0

import (
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/predefine"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var clusterUpgEntries = []versions.UpgradeEntry{
	upg_mo_iscp_task,
	upg_mo_index_update_new,
}

var upg_mo_iscp_task = versions.UpgradeEntry{
	Schema:    catalog.MOTaskDB,
	TableName: catalog.MOSysDaemonTask,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    predefine.GenInitISCPTaskSQL(),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		ok, err := versions.CheckTableDataExist(txn, accountId, predefine.GenISCPTaskCheckSQL())
		return ok, err
	},
}

var upg_mo_index_update_new = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_INDEX_UPDATE,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    frontend.MoCatalogMoIndexUpdateDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, catalog.MO_CATALOG, catalog.MO_INDEX_UPDATE)
	},
}
