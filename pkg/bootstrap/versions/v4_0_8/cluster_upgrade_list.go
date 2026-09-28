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
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// The 4.2 release already records catalog version 4.0.6 but does not contain
// these tables. Entries added later to that handler are skipped on upgrade.
// Recheck them in the current unreleased handler before admission can require
// catalog revalidation. Existing tables are retained without DDL or data loss.
var clusterUpgEntries = []versions.UpgradeEntry{
	viewMetadataTable(catalog.MO_VIEW_DEPENDENCIES, catalog.MoViewDependenciesDDL),
	viewMetadataTable(catalog.MO_VIEW_REFRESH, catalog.MoViewRefreshDDL),
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
