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

package v4_0_7

import (
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var clusterUpgEntries = []versions.UpgradeEntry{
	createMoViewDependencies,
	createMoViewRefresh,
	addSQLTaskAccountIndex,
	addSQLTaskRunAccountIndex,
	addAsyncTaskParentIndex,
	cleanupLegacyOrphanSQLTaskChildren,
	createMoCdcSnapshot,
	cdcWatermarkColumn("source_table_id", "bigint unsigned not null default 0 after watermark"),
	cdcWatermarkColumn("owner_generation", "bigint unsigned not null default 0 after source_table_id"),
	cdcWatermarkColumn("pending_source_table_id", "bigint unsigned null after owner_generation"),
	cdcWatermarkColumn("target_identity", "varchar(256) null after pending_source_table_id"),
	upgradeDaemonClaimPrecision,
	publicationDatabaseIndex(),
	snapshotQuotaIndex(),
}

// EnsureViewMetadataCatalogs prepares the metadata required by CN admission.
// The caller owns the version-table lock and transaction; its commit must
// precede admission and the normal upgrade route.
func EnsureViewMetadataCatalogs(txn executor.TxnExecutor) error {
	for _, entry := range []versions.UpgradeEntry{createMoViewDependencies, createMoViewRefresh} {
		if err := entry.Upgrade(txn, catalog.System_Account); err != nil {
			return err
		}
	}
	return nil
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

func publicationDatabaseIndex() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    catalog.MO_CATALOG,
		TableName: catalog.MO_PUBS,
		UpgType:   versions.ADD_INDEX,
		UpgSql:    "create index idx_mo_pubs_database_id on mo_catalog.mo_pubs(database_id)",
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			invalid, err := versions.CheckTableDataExist(txn, accountID,
				"select 1 from mo_catalog.mo_pubs p where p.database_name <> '*' and "+
					"(p.database_id is null or p.database_id = 0 or not exists "+
					"(select 1 from mo_catalog.mo_database d where d.dat_id = p.database_id "+
					"and d.datname = p.database_name)) limit 1")
			if err != nil {
				return false, err
			}
			if invalid {
				return false, &IndexConflict{"named mo_pubs row has a missing or stale database_id; repair publication catalog before upgrade"}
			}
			return checkCatalogIndex(txn, accountID, catalog.MO_CATALOG, catalog.MO_PUBS, "idx_mo_pubs_database_id", false, "database_id")
		},
	}
}

func snapshotQuotaIndex() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    catalog.MO_CATALOG,
		TableName: catalog.MO_SNAPSHOTS,
		UpgType:   versions.ADD_INDEX,
		UpgSql:    frontend.MoCatalogSysSnapshotQuotaIndexDDL,
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			return checkCatalogIndex(txn, accountID, catalog.MO_CATALOG, catalog.MO_SNAPSHOTS, "idx_snapshot_quota_target", false, "account_name", "level", "obj_id", "kind")
		},
	}
}
