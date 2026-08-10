// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package v4_0_6

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/mongodb"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

var tenantUpgEntries = []versions.UpgradeEntry{
	newMongoDBCatalogTable(mongodb.TableConnections, mongodb.ConnectionsDDL),
	newMongoDBCatalogTable(mongodb.TableMappings, mongodb.MappingsDDL),
	addForeignKeyMetadataColumn("referenced_index_name", "varchar(5000) not null default ''", "on_update"),
	addForeignKeyMetadataColumn("on_delete_origin", "varchar(64) not null default 'ACTION_ORIGIN_LEGACY_AMBIGUOUS'", "referenced_index_name"),
	addForeignKeyMetadataColumn("on_update_origin", "varchar(64) not null default 'ACTION_ORIGIN_LEGACY_AMBIGUOUS'", "on_delete_origin"),
	upgradeInformationSchemaKeyColumnUsage(),
	upgradeInformationSchemaReferentialConstraints(),
	populateInformationSchemaCharacterSets(),
	upgradeInformationSchemaColumns(),
	upgradeInformationSchemaCheckConstraints(),
	upgradeInformationSchemaTableConstraints(),
}

// Keep this as a separate upgrade entry so tenants that already completed
// v4.0.6 refresh COLUMNS and expose MySQL-compatible base DATA_TYPE names.
func upgradeInformationSchemaColumns() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    sysview.InformationDBConst,
		TableName: "COLUMNS",
		UpgType:   versions.MODIFY_VIEW,
		UpgSql:    sysview.InformationSchemaColumnsDDL,
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			exists, viewDef, err := versions.CheckViewDefinition(txn, accountID, sysview.InformationDBConst, "COLUMNS")
			if err != nil {
				return false, err
			}
			return exists && viewDef == sysview.InformationSchemaColumnsDDL, nil
		},
		PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.COLUMNS;", sysview.InformationDBConst),
	}
}

func addForeignKeyMetadataColumn(column, definition, after string) versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    catalog.MO_CATALOG,
		TableName: catalog.MOForeignKeys,
		UpgType:   versions.ADD_COLUMN,
		UpgSql:    fmt.Sprintf("alter table %s.%s add column %s %s after %s", catalog.MO_CATALOG, catalog.MOForeignKeys, column, definition, after),
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			columnInfo, err := versions.CheckTableColumn(txn, accountID, catalog.MO_CATALOG, catalog.MOForeignKeys, column)
			return columnInfo.IsExits, err
		},
	}
}

func populateInformationSchemaCharacterSets() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    sysview.InformationDBConst,
		TableName: "CHARACTER_SETS",
		UpgType:   versions.MODIFY_METADATA,
		PreSql: "DELETE FROM information_schema.CHARACTER_SETS " +
			"WHERE lower(CHARACTER_SET_NAME) IN ('binary','utf8','utf8mb4')",
		UpgSql: sysview.InformationSchemaCharacterSetsData,
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			return versions.CheckTableDataExist(txn, accountID,
				"SELECT 1 FROM information_schema.CHARACTER_SETS "+
					"WHERE CHARACTER_SET_NAME = 'binary' AND DEFAULT_COLLATE_NAME = 'binary' AND MAXLEN = 1 "+
					"AND EXISTS (SELECT 1 FROM information_schema.CHARACTER_SETS "+
					"WHERE CHARACTER_SET_NAME = 'utf8' AND DEFAULT_COLLATE_NAME = 'utf8_bin' AND MAXLEN = 4) "+
					"AND EXISTS (SELECT 1 FROM information_schema.CHARACTER_SETS "+
					"WHERE CHARACTER_SET_NAME = 'utf8mb4' AND DEFAULT_COLLATE_NAME = 'utf8mb4_bin' AND MAXLEN = 4) "+
					"LIMIT 1")
		},
	}
}

func newMongoDBCatalogTable(name, ddl string) versions.UpgradeEntry {
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

func upgradeInformationSchemaKeyColumnUsage() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    sysview.InformationDBConst,
		TableName: "KEY_COLUMN_USAGE",
		UpgType:   versions.CREATE_VIEW,
		UpgSql:    sysview.InformationSchemaKeyColumnUsageDDL,
		CheckFunc: checkViewDefinition("KEY_COLUMN_USAGE", sysview.InformationSchemaKeyColumnUsageDDL),
		PreSql:    fmt.Sprintf("DROP TABLE IF EXISTS %s.%s;", sysview.InformationDBConst, "KEY_COLUMN_USAGE"),
	}
}

func upgradeInformationSchemaReferentialConstraints() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    sysview.InformationDBConst,
		TableName: "REFERENTIAL_CONSTRAINTS",
		UpgType:   versions.MODIFY_VIEW,
		UpgSql:    sysview.InformationSchemaReferentialConstraintsDDL,
		CheckFunc: checkViewDefinition("REFERENTIAL_CONSTRAINTS", sysview.InformationSchemaReferentialConstraintsDDL),
		PreSql:    fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;", sysview.InformationDBConst, "REFERENTIAL_CONSTRAINTS"),
	}
}

func upgradeInformationSchemaCheckConstraints() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:                  sysview.InformationDBConst,
		TableName:               "CHECK_CONSTRAINTS",
		UpgType:                 versions.CREATE_VIEW,
		UpgSql:                  sysview.InformationSchemaCheckConstraintsDDL,
		RequiredProtocolVersion: defines.MORPCVersion14,
		CheckFunc: checkViewDefinition("CHECK_CONSTRAINTS",
			sysview.InformationSchemaCheckConstraintsDDL),
		PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;",
			sysview.InformationDBConst, "CHECK_CONSTRAINTS"),
	}
}

func upgradeInformationSchemaTableConstraints() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:                  sysview.InformationDBConst,
		TableName:               "TABLE_CONSTRAINTS",
		UpgType:                 versions.MODIFY_VIEW,
		UpgSql:                  sysview.InformationSchemaTableConstraintsDDL,
		RequiredProtocolVersion: defines.MORPCVersion14,
		CheckFunc: checkViewDefinition("TABLE_CONSTRAINTS",
			sysview.InformationSchemaTableConstraintsDDL),
		PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;",
			sysview.InformationDBConst, "TABLE_CONSTRAINTS"),
	}
}

func checkViewDefinition(viewName, definition string) func(executor.TxnExecutor, uint32) (bool, error) {
	return func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
		exists, viewDef, err := versions.CheckViewDefinition(txn, accountID, sysview.InformationDBConst, viewName)
		if err != nil {
			return false, err
		}
		return exists && viewDef == definition, nil
	}
}
