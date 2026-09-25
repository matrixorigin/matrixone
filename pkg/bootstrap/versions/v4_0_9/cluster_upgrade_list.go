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

package v4_0_9

import (
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var clusterUpgEntries = []versions.UpgradeEntry{
	cdcWatermarkColumn("pending_source_table_id", "bigint unsigned null after owner_generation"),
	cdcWatermarkColumn("target_identity", "varchar(256) null after pending_source_table_id"),
}

func cdcWatermarkColumn(name, definition string) versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:                  catalog.MO_CATALOG,
		TableName:               catalog.MO_CDC_WATERMARK,
		UpgType:                 versions.ADD_COLUMN,
		UpgSql:                  "alter table mo_catalog.mo_cdc_watermark add column " + name + " " + definition,
		RequiredProtocolVersion: defines.MORPCVersion97,
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			column, err := versions.CheckTableColumn(txn, accountID, catalog.MO_CATALOG, catalog.MO_CDC_WATERMARK, name)
			return column.IsExits, err
		},
	}
}
