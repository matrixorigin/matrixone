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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/partitionservice"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var tenantUpgEntries = []versions.UpgradeEntry{
	enablePartitionMetadata,
	enablePartitionTables,
	upg_alter_mo_snapshots,
}

var enablePartitionMetadata = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MOPartitionMetadata,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    partitionservice.PartitionTableMetadataSQL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exist, err := versions.CheckTableDefinition(txn, accountId, catalog.MO_CATALOG, catalog.MOPartitionMetadata)
		return exist, err
	},
}

var enablePartitionTables = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MOPartitionTables,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    partitionservice.PartitionTablesSQL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exist, err := versions.CheckTableDefinition(txn, accountId, catalog.MO_CATALOG, catalog.MOPartitionTables)
		return exist, err
	},
}

const kind = "kind"

var upg_alter_mo_snapshots = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_SNAPSHOTS,
	UpgType:   versions.ADD_COLUMN,
	UpgSql: fmt.Sprintf(
		"alter table %s.%s add column %s varchar(32) not null default 'user'",
		catalog.MO_CATALOG, catalog.MO_SNAPSHOTS, kind,
	),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		info, err := versions.CheckTableColumn(txn, accountId, catalog.MO_CATALOG, catalog.MO_SNAPSHOTS, "kind")
		if err != nil {
			return false, err
		}

		return info.IsExits, nil
	},
}
