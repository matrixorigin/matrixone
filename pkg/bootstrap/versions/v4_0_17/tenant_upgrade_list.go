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

package v4_0_17

import (
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var tenantUpgEntries = []versions.UpgradeEntry{
	versions.InformationSchemaColumnsUpgradeEntry(),
	upg_mo_pwd_ts_convention_table,
	upg_mo_pwd_ts_convention_seed,
}

// upg_mo_pwd_ts_convention_table installs the per-tenant convention
// discriminator used by the default_password_lifetime login check: rows
// written before the anchor came from the legacy UTC_TIMESTAMP column
// default and must keep the legacy fixed reading.
var upg_mo_pwd_ts_convention_table = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: "mo_pwd_ts_convention",
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    frontend.MoCatalogMoPwdTsConventionDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, catalog.MO_CATALOG, "mo_pwd_ts_convention")
	},
}

// upg_mo_pwd_ts_convention_seed stamps the tenant anchor. For every
// existing tenant the anchor is the upgrade instant itself, so all rows
// that predate this upgrade read through the legacy branch.
var upg_mo_pwd_ts_convention_seed = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: "mo_pwd_ts_convention",
	UpgType:   versions.MODIFY_METADATA,
	UpgSql:    frontend.InsertMoPwdTsConventionSql,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		sql := `select convention, anchor from mo_catalog.mo_pwd_ts_convention where convention = 'session_local'`
		return versions.CheckTableDataExist(txn, accountId, sql)
	},
}
