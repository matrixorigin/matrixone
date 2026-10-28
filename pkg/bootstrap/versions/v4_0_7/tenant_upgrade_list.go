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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

var tenantUpgEntries = []versions.UpgradeEntry{
	routineSQLModeColumn("mo_user_defined_function", "database_collation"),
	routineSQLModeColumn(catalog.MO_STORED_PROCEDURE, "body"),
	addForeignKeyMetadataColumn("referenced_index_name", "varchar(5000) not null default ''", "on_update"),
	addForeignKeyMetadataColumn("on_delete_origin", "varchar(64) not null default 'ACTION_ORIGIN_LEGACY_AMBIGUOUS'", "referenced_index_name"),
	addForeignKeyMetadataColumn("on_update_origin", "varchar(64) not null default 'ACTION_ORIGIN_LEGACY_AMBIGUOUS'", "on_delete_origin"),
	upgradeInformationSchemaKeyColumnUsage(),
	upgradeInformationSchemaReferentialConstraints(),
	populateInformationSchemaCollations(),
	refreshInformationSchemaCharacterSets(),
	upgradeInformationSchemaCheckConstraints(),
	upgradeInformationSchemaTableConstraints(),
	dropUserDefinedFunctionNameIndex(),
	addUserDefinedFunctionArgumentTypesColumn(),
	backfillUserDefinedFunctionArgumentTypes(),
	addUserDefinedFunctionSignatureIndex(),
	upgradeInformationSchemaCollationCharacterSetApplicability(),
	backfillMoColumnsAttIsUnsigned(),
	upgradeInformationSchemaStatistics(),
	addMoRoleGrantGranteeIndex(),
	upgradeInformationSchemaViews(),
	upgradeInformationSchemaMetadataVisibilityView("PARTITIONS", sysview.InformationSchemaPartitionsDDL),
	upgradeInformationSchemaTablePrivileges(),
	addIcebergCatalogIDAllocatorIndex(),
	createDatabaseDefaults(),
	upgradeInformationSchemaMetadataVisibilityView("SCHEMATA", sysview.InformationSchemaSchemataDDL),
	upgradeDatabaseDefaultsColumns(),
	versions.InformationSchemaTablesUpgradeEntry(),
	// Tenants that predate the data-branch provenance work lack
	// mo_catalog.mo_data_branch_update_link, so data-branch diff reading
	// cannot recover update provenance for primary-key moves and must
	// keep failing closed (delete+insert replay). Creating the table for
	// existing tenants lets the pk-move recorder persist update
	// provenance for statements executed after the upgrade. The sibling
	// retention ledger mo_catalog.mo_data_branch_link_retention records
	// the identity of every retained dead table whose links a funnel
	// verdict kept alive; tenants predating the ledger simply keep no
	// ledger rows, which reaps treat as the empty fast path. The catalog
	// creations carry no protocol gate: the repository supports shutdown
	// upgrade only, so every service runs the binary that maintains
	// these tables and the CheckFunc-gated idempotent creation needs no
	// cluster-wide protocol check. Unpublished 4.0.8..4.0.19 identities
	// may not be registered as upgrade steps
	// (docs/design/20261028-catalog-upgrade-407.md), so these creations
	// append to the unpublished 4.0.7 handler and raise its offset.
	{
		Schema:    catalog.MO_CATALOG,
		TableName: catalog.MO_DATA_BRANCH_UPDATE_LINK,
		UpgType:   versions.CREATE_NEW_TABLE,
		UpgSql:    frontend.MoCatalogDataBranchUpdateLinkDDL,
		CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
			return versions.CheckTableDefinition(txn, accountId, catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_UPDATE_LINK)
		},
	},
	{
		Schema:    catalog.MO_CATALOG,
		TableName: catalog.MO_DATA_BRANCH_LINK_RETENTION,
		UpgType:   versions.CREATE_NEW_TABLE,
		UpgSql:    frontend.MoCatalogDataBranchLinkRetentionDDL,
		CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
			return versions.CheckTableDefinition(txn, accountId, catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_LINK_RETENTION)
		},
	},
}

func upgradeDatabaseDefaultsColumns() versions.UpgradeEntry {
	entry := versions.InformationSchemaColumnsUpgradeEntry()
	// The final definition depends on database defaults introduced at v118.
	entry.RequiredProtocolVersion = defines.MORPCVersion118
	return entry
}

func refreshInformationSchemaCharacterSets() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:                  sysview.InformationDBConst,
		TableName:               "CHARACTER_SETS",
		UpgType:                 versions.MODIFY_METADATA,
		PreSql:                  "DELETE FROM information_schema.CHARACTER_SETS",
		UpgSql:                  sysview.InformationSchemaCharacterSetsData,
		RequiredProtocolVersion: defines.MORPCVersion106,
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			return versions.CheckTableDataExist(txn, accountID, sysview.InformationSchemaCharacterSetsCheckSQL())
		},
	}
}

func createDatabaseDefaults() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:                  catalog.MO_CATALOG,
		TableName:               catalog.MODatabaseDefaults,
		UpgType:                 versions.CREATE_NEW_TABLE,
		UpgSql:                  catalog.MoDatabaseDefaultsDDL,
		RequiredProtocolVersion: defines.MORPCVersion118,
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			return versions.CheckTableDefinition(txn, accountID, catalog.MO_CATALOG, catalog.MODatabaseDefaults)
		},
	}
}

// Released 4.1 catalogs can lack sql_mode even when their recorded 4.0.1
// upgrade has already completed. Preserve explicit modes when the column exists.
func routineSQLModeColumn(table, after string) versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    catalog.MO_CATALOG,
		TableName: table,
		UpgType:   versions.ADD_COLUMN,
		UpgSql: fmt.Sprintf("alter table mo_catalog.%s add column sql_mode varchar(1024) not null default 'PIPES_AS_CONCAT' after %s",
			table, after),
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			column, err := versions.CheckTableColumn(txn, accountID, catalog.MO_CATALOG, table, "sql_mode")
			return column.IsExits, err
		},
	}
}

// UpgradeInformationSchemaViewsAfterProtocolCheck replaces VIEWS for the
// bounded maintenance owner, independently of the historical upgrade list.
// The caller checks the protocol immediately before calling this function so
// an ErrNotSupported from the DDL cannot be mistaken for a retryable protocol gate.
func UpgradeInformationSchemaViewsAfterProtocolCheck(
	txn executor.TxnExecutor,
	accountID uint32,
) error {
	entry := upgradeInformationSchemaViews()
	return entry.UpgradeAfterProtocolCheck(txn, accountID)
}

// CheckInformationSchemaViewsDefinition recognizes the two catalog names
// emitted by the system DDL: VIEWS with case preservation, and views with the
// default lower_case_table_names setting. Catalog string predicates are case
// sensitive; missing the folded name would make every maintenance pass repeat
// DROP/CREATE even after the current definition was successfully installed.
func CheckInformationSchemaViewsDefinition(txn executor.TxnExecutor, accountID uint32) (bool, string, error) {
	return versions.CheckInformationSchemaViewDefinition(txn, accountID, "VIEWS")
}

// InformationSchemaViewsAuthoringProtocolReady reports the local write-side
// admission fence. The common protocol check only proves that every CN can
// decode v126; the VIEWS DDL also persists function IDs that must not be
// authored until this CN has consumed the catalog-fenced admission snapshot.
// A missing runtime or key is retained as ready for bootstrap unit tests and
// older standalone callers that predate the admission lifecycle.
func InformationSchemaViewsAuthoringProtocolReady(txn executor.TxnExecutor) bool {
	if txn == nil || txn.Txn() == nil {
		return true
	}
	rt := runtime.ServiceRuntime(txn.Txn().TxnOptions().CN)
	if rt == nil {
		return true
	}
	value, present := rt.GetGlobalVariables(runtime.PersistedExpressionProtocolAuthoringFloor)
	if !present {
		return true
	}
	floor, valid := value.(int64)
	return valid && floor >= defines.MORPCVersion126
}

// upgradeInformationSchemaViews describes the guarded maintenance repair. The
// account scan uses the persisted DDL as its work marker, so it also reaches
// tenants already at v4.0.6, v4.0.7, or the final version without changing any
// historical handler's offset or delaying its pending provenance migration.
func upgradeInformationSchemaViews() versions.UpgradeEntry {
	return versions.InformationSchemaViewUpgradeEntry("VIEWS", sysview.InformationSchemaViewsDDL, defines.MORPCVersion126)
}
