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

package v4_0_11

import (
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// These columns are deliberately introduced after 4.0.10.  That version is
// already published as the final upgrade by older clusters, so changing its
// handler would leave those clusters permanently without the CDC target
// identity catalog state.
//
// Clusters first deployed at catalog 4.0.6 (shipped as v4.2.5) never execute
// the v4_0_6 handler: their upgrade chain starts at 4.0.6 -> 4.0.7 and runs
// the v4_0_7 list. The entries that add source_table_id and owner_generation
// were merged into the already-consumed v4_0_6 handler after the v4.2.5 cut,
// so those clusters reach this list without the AFTER anchors and the
// pending_source_table_id ALTER fails with "Unknown column
// 'owner_generation'". Recheck both prerequisites here; the per-entry
// CheckFunc makes each addition a no-op wherever the v4_0_6 handler did run.
var clusterUpgEntries = []versions.UpgradeEntry{
	cdcWatermarkColumn("source_table_id", "bigint unsigned not null default 0 after watermark"),
	cdcWatermarkColumn("owner_generation", "bigint unsigned not null default 0 after source_table_id"),
	cdcWatermarkColumn("pending_source_table_id", "bigint unsigned null after owner_generation"),
	cdcWatermarkColumn("target_identity", "varchar(256) null after pending_source_table_id"),
}

func cdcWatermarkColumn(name, definition string) versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:                  catalog.MO_CATALOG,
		TableName:               catalog.MO_CDC_WATERMARK,
		UpgType:                 versions.ADD_COLUMN,
		UpgSql:                  "alter table mo_catalog.mo_cdc_watermark add column " + name + " " + definition,
		RequiredProtocolVersion: defines.MORPCVersion106,
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			column, err := versions.CheckTableColumn(txn, accountID, catalog.MO_CATALOG, catalog.MO_CDC_WATERMARK, name)
			return column.IsExits, err
		},
	}
}
