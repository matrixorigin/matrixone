// Copyright 2026 Matrix Origin
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

package v4_0_5

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	icebergsql "github.com/matrixorigin/matrixone/pkg/sql/iceberg"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var tenantUpgEntries = []versions.UpgradeEntry{
	addOrphanFileColumn("namespace", "varchar(2048) not null default ''", "catalog_id"),
	addOrphanFileColumn("table_name", "varchar(1024) not null default ''", "namespace"),
	addOrphanFileColumn("file_path", "varchar(4096) not null default ''", "table_location_hash"),
}

func addOrphanFileColumn(column, definition, after string) versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    catalog.MO_CATALOG,
		TableName: icebergsql.TableOrphanFiles,
		UpgType:   versions.ADD_COLUMN,
		UpgSql:    fmt.Sprintf("alter table %s.%s add column %s %s after %s", catalog.MO_CATALOG, icebergsql.TableOrphanFiles, column, definition, after),
		CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
			info, err := versions.CheckTableColumn(txn, accountId, catalog.MO_CATALOG, icebergsql.TableOrphanFiles, column)
			if err != nil {
				return false, err
			}
			return info.IsExits, nil
		},
	}
}
