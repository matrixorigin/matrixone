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
package v4_0_7

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

const moColumnsUnsignedValueMismatchPredicate = "(att_is_unsigned IS NULL OR att_is_unsigned = 0) " +
	"AND mo_show_visible_bin(atttyp, 2) IN ('TINYINT UNSIGNED', 'SMALLINT UNSIGNED', 'INT UNSIGNED', 'BIGINT UNSIGNED')"

const moColumnsUnsignedMismatchPredicate = "account_id = current_account_id() AND " + moColumnsUnsignedValueMismatchPredicate

func addIcebergCatalogIDAllocatorIndex() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    catalog.MO_CATALOG,
		TableName: "mo_iceberg_catalogs",
		UpgType:   versions.ADD_INDEX,
		UpgSql:    "create index catalog_id_allocator on mo_catalog.mo_iceberg_catalogs(catalog_id)",
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			return checkCatalogIndex(txn, accountID, catalog.MO_CATALOG, "mo_iceberg_catalogs", "catalog_id_allocator", false, "catalog_id")
		},
	}
}

func upgradeInformationSchemaMetadataVisibilityView(viewName, viewDDL string) versions.UpgradeEntry {
	requiredProtocol := defines.MORPCVersion41
	if viewName == "SCHEMATA" {
		requiredProtocol = defines.MORPCVersion118
	}
	return versions.InformationSchemaViewUpgradeEntry(viewName, viewDDL, requiredProtocol)
}

func upgradeInformationSchemaTablePrivileges() versions.UpgradeEntry {
	entry := versions.InformationSchemaViewUpgradeEntry("TABLE_PRIVILEGES", sysview.InformationSchemaTablePrivilegesDDL, defines.MORPCVersion41)
	entry.UpgType = versions.RECONCILE_LEGACY_INFORMATION_SCHEMA_VIEW
	return entry
}

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

func populateInformationSchemaCollations() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:                  sysview.InformationDBConst,
		TableName:               "COLLATIONS",
		UpgType:                 versions.MODIFY_METADATA,
		UpgSql:                  sysview.InformationSchemaCollationsData,
		PreSql:                  "DELETE FROM information_schema.COLLATIONS",
		RequiredProtocolVersion: defines.MORPCVersion118,
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			return versions.CheckTableDataExist(txn, accountID, informationSchemaCollationsCheckSQL())
		},
	}
}

func informationSchemaCollationsCheckSQL() string {
	return sysview.InformationSchemaCollationsCheckSQL()
}

func upgradeInformationSchemaCollationCharacterSetApplicability() versions.UpgradeEntry {
	return versions.InformationSchemaViewUpgradeEntry("COLLATION_CHARACTER_SET_APPLICABILITY", sysview.InformationSchemaCollationCharacterSetApplicabilityDDL, 0)
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
			ready, err := checkCatalogIndex(txn, accountID, catalog.MO_CATALOG, "mo_user_defined_function", "name_db_arg_types", true, "name", "db", "arg_types")
			if err != nil || ready {
				return ready, err
			}
			// The later routine entry replaces this index in the same pending
			// migration. A restart must not recreate an already superseded index.
			return checkCatalogIndex(txn, accountID, catalog.MO_CATALOG, "mo_user_defined_function", "name_db_arg_types_descriptor", true, "name", "db", "arg_types", "canonical_input_descriptor")
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
			return checkCatalogIndex(txn, accountID, catalog.MO_CATALOG, "mo_role_grant", "idx_mo_role_grant_grantee_id", false, "grantee_id")
		},
	}
}

func upgradeInformationSchemaKeyColumnUsage() versions.UpgradeEntry {
	entry := versions.InformationSchemaViewUpgradeEntry("KEY_COLUMN_USAGE", sysview.InformationSchemaKeyColumnUsageDDL, defines.MORPCVersion41)
	entry.UpgType = versions.RECONCILE_LEGACY_INFORMATION_SCHEMA_VIEW
	return entry
}

func upgradeInformationSchemaReferentialConstraints() versions.UpgradeEntry {
	return versions.InformationSchemaViewUpgradeEntry("REFERENTIAL_CONSTRAINTS", sysview.InformationSchemaReferentialConstraintsDDL, defines.MORPCVersion41)
}

func upgradeInformationSchemaCheckConstraints() versions.UpgradeEntry {
	return versions.InformationSchemaViewUpgradeEntry("CHECK_CONSTRAINTS", sysview.InformationSchemaCheckConstraintsDDL, defines.MORPCVersion41)
}

func upgradeInformationSchemaTableConstraints() versions.UpgradeEntry {
	return versions.InformationSchemaViewUpgradeEntry("TABLE_CONSTRAINTS", sysview.InformationSchemaTableConstraintsDDL, defines.MORPCVersion41)
}

// Refresh STATISTICS for tenants that completed an earlier v4.0.6 offset.

func upgradeInformationSchemaStatistics() versions.UpgradeEntry {
	return versions.InformationSchemaViewUpgradeEntry("STATISTICS", sysview.InformationSchemaStatisticsPrefixSubPartDDL, defines.MORPCVersion107)
}
