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
	"github.com/matrixorigin/matrixone/pkg/defines"
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
	versions.InformationSchemaViewsUpgradeEntry(),
	upgradeInformationSchemaMetadataVisibilityView("PARTITIONS", sysview.InformationSchemaPartitionsDDL),
	upgradeInformationSchemaTablePrivileges(),
	addIcebergCatalogIDAllocatorIndex(),
	createDatabaseDefaults(),
	upgradeInformationSchemaMetadataVisibilityView("SCHEMATA", sysview.InformationSchemaSchemataDDL),
	upgradeDatabaseDefaultsColumns(),
	versions.InformationSchemaTablesUpgradeEntry(),
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
