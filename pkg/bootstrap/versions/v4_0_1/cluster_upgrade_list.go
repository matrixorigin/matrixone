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

package v4_0_1

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var clusterUpgEntries = []versions.UpgradeEntry{
	upg_mo_indexes_add_included_columns_for_cluster,
}

var checkMoIndexesIncludedColumns = func(txn executor.TxnExecutor, accountId uint32) (versions.ColumnInfo, error) {
	return versions.CheckTableColumn(txn, accountId, catalog.MO_CATALOG, catalog.MO_INDEXES, catalog.IndexIncludedColumns)
}

func newMoIndexesAddIncludedColumnsEntry() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    catalog.MO_CATALOG,
		TableName: catalog.MO_INDEXES,
		UpgType:   versions.ADD_COLUMN,
		UpgSql: fmt.Sprintf(
			"alter table %s.%s add column %s text after %s",
			catalog.MO_CATALOG,
			catalog.MO_INDEXES,
			catalog.IndexIncludedColumns,
			"index_table_name",
		),
		CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
			info, err := checkMoIndexesIncludedColumns(txn, accountId)
			if err != nil {
				return false, err
			}
			return info.IsExits, nil
		},
	}
}

var upg_mo_indexes_add_included_columns_for_cluster = newMoIndexesAddIncludedColumnsEntry()
