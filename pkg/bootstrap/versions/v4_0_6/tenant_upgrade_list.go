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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
	ensureInformationSchemaCharacterSetsTable(),
	populateInformationSchemaCollations(),
	populateInformationSchemaCharacterSets(),
	upgradeInformationSchemaColumns(),
	upgradeInformationSchemaCheckConstraints(),
	upgradeInformationSchemaTableConstraints(),
	upgradeInformationSchemaColumnsHideInternalColumns(),
	dropUserDefinedFunctionNameIndex(),
	addUserDefinedFunctionArgumentTypesColumn(),
	backfillUserDefinedFunctionArgumentTypes(),
	addUserDefinedFunctionSignatureIndex(),
	upgradeInformationSchemaCollationCharacterSetApplicability(),
	backfillMoColumnsAttIsUnsigned(),
	upgradeInformationSchemaStatistics(),
	addMoRoleGrantGranteeIndex(),
	upgradeInformationSchemaMetadataVisibilityView("TABLES", sysview.InformationSchemaTablesDDL),
	upgradeInformationSchemaMetadataVisibilityView("COLUMNS", sysview.InformationSchemaColumnsDDL),
	upgradeInformationSchemaMetadataVisibilityView("STATISTICS", sysview.InformationSchemaStatisticsDDL),
	upgradeInformationSchemaMetadataVisibilityTableConstraints(),
	upgradeInformationSchemaMetadataVisibilityView("KEY_COLUMN_USAGE", sysview.InformationSchemaKeyColumnUsageDDL),
	upgradeInformationSchemaMetadataVisibilityView("REFERENTIAL_CONSTRAINTS", sysview.InformationSchemaReferentialConstraintsDDL),
	upgradeInformationSchemaMetadataVisibilityCheckConstraints(),
	upgradeInformationSchemaMetadataVisibilityView("VIEWS", sysview.InformationSchemaViewsDDL),
	upgradeInformationSchemaMetadataVisibilityView("PARTITIONS", sysview.InformationSchemaPartitionsDDL),
	upgradeInformationSchemaMetadataVisibilityView("SCHEMATA", sysview.InformationSchemaSchemataDDL),
	upgradeInformationSchemaTablePrivileges(),
	addIcebergCatalogIDAllocatorIndex(),
}

// The catalog ID allocator is storage-owned.  MatrixOne only materializes an
// auto-increment allocator when the column is a leading index part; the
// account-first primary key is retained for account-local lookups, while this
// narrow secondary index supplies that allocator contract.
func addIcebergCatalogIDAllocatorIndex() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    catalog.MO_CATALOG,
		TableName: "mo_iceberg_catalogs",
		UpgType:   versions.ADD_INDEX,
		UpgSql:    "create index catalog_id_allocator on mo_catalog.mo_iceberg_catalogs(catalog_id)",
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			return versions.CheckIndexDefinition(txn, accountID, catalog.MO_CATALOG, "mo_iceberg_catalogs", "catalog_id_allocator")
		},
	}
}

func upgradeInformationSchemaMetadataVisibilityView(viewName, viewDDL string) versions.UpgradeEntry {
	requiredProtocol := defines.MORPCVersion41
	if viewName == "TABLES" || viewName == "COLUMNS" {
		requiredProtocol = defines.MORPCVersion46
	}
	return versions.UpgradeEntry{
		Schema:                  sysview.InformationDBConst,
		TableName:               viewName,
		UpgType:                 versions.MODIFY_VIEW,
		UpgSql:                  viewDDL,
		CheckFunc:               checkViewDefinition(viewName, viewDDL),
		PreSql:                  fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;", sysview.InformationDBConst, viewName),
		RequiredProtocolVersion: requiredProtocol,
	}
}

func upgradeInformationSchemaMetadataVisibilityTableConstraints() versions.UpgradeEntry {
	return upgradeInformationSchemaMetadataVisibilityView(
		"TABLE_CONSTRAINTS", sysview.InformationSchemaTableConstraintsDDL)
}

func upgradeInformationSchemaMetadataVisibilityCheckConstraints() versions.UpgradeEntry {
	return upgradeInformationSchemaMetadataVisibilityView(
		"CHECK_CONSTRAINTS", sysview.InformationSchemaCheckConstraintsDDL)
}

// upgradeInformationSchemaTablePrivileges converges the legacy empty base
// table, a stale view, or an absent object to the canonical derived view.
func upgradeInformationSchemaTablePrivileges() versions.UpgradeEntry {
	const (
		viewName        = "TABLE_PRIVILEGES"
		catalogViewName = "table_privileges"
	)
	return versions.UpgradeEntry{
		Schema:                  sysview.InformationDBConst,
		TableName:               viewName,
		UpgType:                 versions.MODIFY_VIEW,
		UpgSql:                  fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;", sysview.InformationDBConst, viewName),
		CheckFunc:               checkViewDefinition(catalogViewName, sysview.InformationSchemaTablePrivilegesDDL),
		RequiredProtocolVersion: defines.MORPCVersion41,
		PreSql:                  fmt.Sprintf("DROP TABLE IF EXISTS %s.%s;", sysview.InformationDBConst, viewName),
		PostSql:                 sysview.InformationSchemaTablePrivilegesDDL,
	}
}

const moColumnsUnsignedMismatchPredicate = "account_id = current_account_id() " +
	"AND (att_is_unsigned IS NULL OR att_is_unsigned = 0) " +
	"AND mo_show_visible_bin(atttyp, 2) IN ('TINYINT UNSIGNED', 'SMALLINT UNSIGNED', 'INT UNSIGNED', 'BIGINT UNSIGNED')"

func backfillMoColumnsAttIsUnsigned() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:                  catalog.MO_CATALOG,
		TableName:               catalog.MO_COLUMNS,
		UpgType:                 versions.MODIFY_METADATA,
		UpgSql:                  "UPDATE mo_catalog.mo_columns SET att_is_unsigned = 1 WHERE " + moColumnsUnsignedMismatchPredicate,
		RequiredProtocolVersion: defines.MORPCVersion34,
		AllowMoColumnsUpdate:    true,
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			mismatch, err := versions.CheckTableDataExist(txn, accountID,
				"SELECT 1 FROM mo_catalog.mo_columns WHERE "+moColumnsUnsignedMismatchPredicate+" LIMIT 1")
			return !mismatch, err
		},
	}
}

// Keep this as a separate upgrade entry so tenants that already completed
// v4.0.6 refresh COLUMNS and expose MySQL-compatible base DATA_TYPE names.
func upgradeInformationSchemaColumns() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:                  sysview.InformationDBConst,
		TableName:               "COLUMNS",
		UpgType:                 versions.MODIFY_VIEW,
		UpgSql:                  sysview.InformationSchemaColumnsDDL,
		RequiredProtocolVersion: defines.MORPCVersion46,
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

// Keep a separate entry so tenants that already completed v4.0.6 rerun the
// COLUMNS upgrade after the view starts filtering att_is_hidden columns.
func upgradeInformationSchemaColumnsHideInternalColumns() versions.UpgradeEntry {
	return upgradeInformationSchemaColumns()
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

func ensureInformationSchemaCharacterSetsTable() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    sysview.InformationDBConst,
		TableName: "CHARACTER_SETS",
		UpgType:   versions.CREATE_NEW_TABLE,
		UpgSql:    sysview.InformationSchemaCharacterSetsDDL,
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			return versions.CheckTableDefinition(txn, accountID, sysview.InformationDBConst, "character_sets")
		},
	}
}

func populateInformationSchemaCollations() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    sysview.InformationDBConst,
		TableName: "COLLATIONS",
		UpgType:   versions.MODIFY_METADATA,
		UpgSql:    sysview.InformationSchemaCollationsData,
		PreSql:    "DELETE FROM information_schema.COLLATIONS",
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			return versions.CheckTableDataExist(txn, accountID, informationSchemaCollationsCheckSQL())
		},
	}
}

func informationSchemaCollationsCheckSQL() string {
	if len(sysview.SupportedCollationDefinitions) == 0 {
		return "SELECT 1 WHERE FALSE"
	}

	conditions := make([]string, 0, len(sysview.SupportedCollationDefinitions))
	for _, collation := range sysview.SupportedCollationDefinitions {
		conditions = append(conditions, fmt.Sprintf(
			"COLLATION_NAME = '%s' AND CHARACTER_SET_NAME = '%s' AND ID = %d AND IS_DEFAULT = '%s' AND IS_COMPILED = '%s' AND SORTLEN = %d AND PAD_ATTRIBUTE = '%s'",
			collation.Name,
			collation.Charset,
			collation.ID,
			collation.IsDefault,
			collation.IsCompiled,
			collation.SortLen,
			collation.PadAttribute,
		))
	}

	checks := make([]string, 0, len(conditions))
	checks = append(checks, conditions[0])
	for _, condition := range conditions[1:] {
		checks = append(checks, fmt.Sprintf(
			"EXISTS (SELECT 1 FROM information_schema.COLLATIONS WHERE %s)", condition,
		))
	}
	return fmt.Sprintf("SELECT 1 FROM information_schema.COLLATIONS WHERE (SELECT COUNT(*) FROM information_schema.COLLATIONS) = %d AND %s LIMIT 1", len(sysview.SupportedCollationDefinitions), strings.Join(checks, " AND "))
}

func upgradeInformationSchemaCollationCharacterSetApplicability() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    sysview.InformationDBConst,
		TableName: "COLLATION_CHARACTER_SET_APPLICABILITY",
		UpgType:   versions.CREATE_VIEW,
		UpgSql:    sysview.InformationSchemaCollationCharacterSetApplicabilityDDL,
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			exists, viewDef, err := versions.CheckViewDefinition(txn, accountID,
				sysview.InformationDBConst, "COLLATION_CHARACTER_SET_APPLICABILITY")
			if err != nil {
				return false, err
			}
			return exists && viewDef == sysview.InformationSchemaCollationCharacterSetApplicabilityDDL, nil
		},
		PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.COLLATION_CHARACTER_SET_APPLICABILITY;",
			sysview.InformationDBConst),
	}
}

// User-defined function lookup is scoped by database and argument signature.
// A global unique name index prevents a database clone from retaining the
// source and destination function definitions in the same account.
func dropUserDefinedFunctionNameIndex() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    catalog.MO_CATALOG,
		TableName: "mo_user_defined_function",
		UpgType:   versions.DROP_INDEX,
		UpgSql:    "alter table mo_catalog.mo_user_defined_function drop index name",
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			exists, err := versions.CheckIndexDefinition(
				txn, accountID, catalog.MO_CATALOG, "mo_user_defined_function", "name",
			)
			return !exists, err
		},
	}
}

func addUserDefinedFunctionArgumentTypesColumn() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    catalog.MO_CATALOG,
		TableName: "mo_user_defined_function",
		UpgType:   versions.ADD_COLUMN,
		UpgSql: fmt.Sprintf("alter table mo_catalog.mo_user_defined_function "+
			"add column arg_types varchar(%d) not null default '' after args", types.MaxStringSize),
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			column, err := versions.CheckTableColumn(
				txn, accountID, catalog.MO_CATALOG, "mo_user_defined_function", "arg_types",
			)
			return column.IsExits, err
		},
	}
}

func backfillUserDefinedFunctionArgumentTypes() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    catalog.MO_CATALOG,
		TableName: "mo_user_defined_function",
		UpgType:   versions.MODIFY_METADATA,
		UpgSql:    "update mo_catalog.mo_user_defined_function set arg_types = " + catalog.UserDefinedFunctionArgumentTypesSQL,
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			if err := validateUserDefinedFunctionArgumentTypesFit(txn, accountID); err != nil {
				return false, err
			}
			mismatch, err := versions.CheckTableDataExist(txn, accountID,
				"select 1 from mo_catalog.mo_user_defined_function where arg_types != "+catalog.UserDefinedFunctionArgumentTypesSQL+" limit 1",
			)
			return !mismatch, err
		},
	}
}

// validateUserDefinedFunctionArgumentTypesFit prevents the upgrade backfill
// from truncating a legacy signature before the unique overload index is built.
func validateUserDefinedFunctionArgumentTypesFit(txn executor.TxnExecutor, accountID uint32) error {
	overLimit, err := versions.CheckTableDataExist(txn, accountID, fmt.Sprintf(
		"select 1 from mo_catalog.mo_user_defined_function where length(%s) > %d limit 1",
		catalog.UserDefinedFunctionArgumentTypesSQL, types.MaxStringSize,
	))
	if err != nil {
		return err
	}
	if overLimit {
		return moerr.NewInvalidInputNoCtxf(
			"function argument type signature exceeds the %d-byte catalog limit", types.MaxStringSize,
		)
	}
	return nil
}

func addUserDefinedFunctionSignatureIndex() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    catalog.MO_CATALOG,
		TableName: "mo_user_defined_function",
		UpgType:   versions.ADD_INDEX,
		UpgSql:    "create unique index name_db_arg_types on mo_catalog.mo_user_defined_function(name, db, arg_types)",
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			return versions.CheckIndexDefinition(
				txn, accountID, catalog.MO_CATALOG, "mo_user_defined_function", "name_db_arg_types",
			)
		},
	}
}

func addMoRoleGrantGranteeIndex() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:                  catalog.MO_CATALOG,
		TableName:               "mo_role_grant",
		UpgType:                 versions.ADD_INDEX,
		UpgSql:                  "create index idx_mo_role_grant_grantee_id on mo_catalog.mo_role_grant(grantee_id)",
		RequiredProtocolVersion: defines.MORPCVersion41,
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			return versions.CheckIndexDefinition(
				txn, accountID, catalog.MO_CATALOG, "mo_role_grant", "idx_mo_role_grant_grantee_id",
			)
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
			return versions.CheckTableDataExist(txn, accountID, informationSchemaCharacterSetsCheckSQL())
		},
	}
}

func informationSchemaCharacterSetsCheckSQL() string {
	return fmt.Sprintf(
		"SELECT 1 FROM information_schema.CHARACTER_SETS "+
			"WHERE CHARACTER_SET_NAME = 'binary' AND DEFAULT_COLLATE_NAME = '%s' AND MAXLEN = 1 "+
			"AND EXISTS (SELECT 1 FROM information_schema.CHARACTER_SETS "+
			"WHERE CHARACTER_SET_NAME = 'utf8' AND DEFAULT_COLLATE_NAME = '%s' AND MAXLEN = 4) "+
			"AND EXISTS (SELECT 1 FROM information_schema.CHARACTER_SETS "+
			"WHERE CHARACTER_SET_NAME = 'utf8mb4' AND DEFAULT_COLLATE_NAME = '%s' AND MAXLEN = 4) "+
			"LIMIT 1",
		sysview.DefaultCollationForCharset("binary"),
		sysview.DefaultCollationForCharset("utf8"),
		sysview.DefaultCollationForCharset("utf8mb4"),
	)
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
		Schema:                  sysview.InformationDBConst,
		TableName:               "KEY_COLUMN_USAGE",
		UpgType:                 versions.CREATE_VIEW,
		UpgSql:                  fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;", sysview.InformationDBConst, "KEY_COLUMN_USAGE"),
		RequiredProtocolVersion: defines.MORPCVersion41,
		CheckFunc:               checkViewDefinition("KEY_COLUMN_USAGE", sysview.InformationSchemaKeyColumnUsageDDL),
		PreSql:                  fmt.Sprintf("DROP TABLE IF EXISTS %s.%s;", sysview.InformationDBConst, "KEY_COLUMN_USAGE"),
		PostSql:                 sysview.InformationSchemaKeyColumnUsageDDL,
	}
}

func upgradeInformationSchemaReferentialConstraints() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:                  sysview.InformationDBConst,
		TableName:               "REFERENTIAL_CONSTRAINTS",
		UpgType:                 versions.MODIFY_VIEW,
		UpgSql:                  sysview.InformationSchemaReferentialConstraintsDDL,
		RequiredProtocolVersion: defines.MORPCVersion41,
		CheckFunc:               checkViewDefinition("REFERENTIAL_CONSTRAINTS", sysview.InformationSchemaReferentialConstraintsDDL),
		PreSql:                  fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;", sysview.InformationDBConst, "REFERENTIAL_CONSTRAINTS"),
	}
}

func upgradeInformationSchemaCheckConstraints() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:                  sysview.InformationDBConst,
		TableName:               "CHECK_CONSTRAINTS",
		UpgType:                 versions.CREATE_VIEW,
		UpgSql:                  sysview.InformationSchemaCheckConstraintsDDL,
		RequiredProtocolVersion: defines.MORPCVersion41,
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
		RequiredProtocolVersion: defines.MORPCVersion41,
		CheckFunc: checkViewDefinition("TABLE_CONSTRAINTS",
			sysview.InformationSchemaTableConstraintsDDL),
		PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;",
			sysview.InformationDBConst, "TABLE_CONSTRAINTS"),
	}
}

// Refresh STATISTICS for tenants that completed an earlier v4.0.6 offset.
func upgradeInformationSchemaStatistics() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:                  sysview.InformationDBConst,
		TableName:               "STATISTICS",
		UpgType:                 versions.MODIFY_VIEW,
		UpgSql:                  sysview.InformationSchemaStatisticsDDL,
		RequiredProtocolVersion: defines.MORPCVersion41,
		CheckFunc:               checkViewDefinition("STATISTICS", sysview.InformationSchemaStatisticsDDL),
		PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;",
			sysview.InformationDBConst, "STATISTICS"),
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
