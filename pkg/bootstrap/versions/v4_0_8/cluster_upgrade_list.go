// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v4_0_8

import (
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// The 4.2 release already records catalog version 4.0.6 but does not contain
// these tables. Entries added later to that handler are skipped on upgrade.
// Recheck them in the current unreleased handler before admission can require
// catalog revalidation. Existing tables are retained without DDL or data loss.
var clusterUpgEntries = []versions.UpgradeEntry{
	viewMetadataTable(catalog.MO_VIEW_DEPENDENCIES, catalog.MoViewDependenciesDDL),
	viewMetadataTable(catalog.MO_VIEW_REFRESH, catalog.MoViewRefreshDDL),
	{
		Schema:    catalog.MO_CATALOG,
		TableName: catalog.MO_PUBS,
		UpgType:   versions.ADD_INDEX,
		UpgSql:    "create index idx_mo_pubs_database_id on mo_catalog.mo_pubs(database_id)",
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			// A named publication with a missing or stale physical ID cannot be
			// protected by the indexed DROP guard. Stop the upgrade for explicit
			// catalog repair rather than silently carrying an orphan forward.
			invalid, err := versions.CheckTableDataExist(txn, accountID,
				"select 1 from mo_catalog.mo_pubs p where p.database_name <> '*' and "+
					"(p.database_id is null or p.database_id = 0 or not exists "+
					"(select 1 from mo_catalog.mo_database d where d.dat_id = p.database_id "+
					"and d.datname = p.database_name)) limit 1")
			if err != nil || invalid {
				if err != nil {
					return false, err
				}
				return false, moerr.NewInternalErrorNoCtx("named mo_pubs row has a missing or stale database_id; repair publication catalog before upgrade")
			}
			return versions.CheckIndexDefinition(txn, accountID, catalog.MO_CATALOG, catalog.MO_PUBS, "idx_mo_pubs_database_id")
		},
	},
}

func viewMetadataTable(name, ddl string) versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    catalog.MO_CATALOG,
		TableName: name,
		UpgType:   versions.CREATE_NEW_TABLE,
		UpgSql:    ddl,
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			return versions.CheckTableDefinition(txn, accountID, catalog.MO_CATALOG, name)
		},
	}
}
