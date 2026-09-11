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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
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
	upgradeInformationSchemaViews(),
	upgradeInformationSchemaMetadataVisibilityView("PARTITIONS", sysview.InformationSchemaPartitionsDDL),
	upgradeInformationSchemaTablePrivileges(),
	addIcebergCatalogIDAllocatorIndex(),
	createDatabaseDefaults(),
	upgradeInformationSchemaMetadataVisibilityView("SCHEMATA", sysview.InformationSchemaSchemataDDL),
	upgradeDatabaseDefaultsColumns(),
	versions.InformationSchemaTablesUpgradeEntry(),
}

var pythonRevisionUpgEntries = []versions.UpgradeEntry{
	addPythonRevisionHeadColumn("active_revision", "bigint unsigned not null default 0"),
	addPythonRevisionHeadColumn("namespace_version", "bigint unsigned not null default 0"),
	addPythonSignatureColumn("canonical_input_descriptor", fmt.Sprintf("varchar(%d) not null default ''", types.MaxStringSize), "arg_types"),
	addPythonSignatureColumn("return_descriptor", fmt.Sprintf("varchar(%d) not null default ''", types.MaxStringSize), "canonical_input_descriptor"),
	addPythonSignatureColumn("signature_key_schema_version", "int not null default 0", "return_descriptor"),
	addPythonSignatureColumn("signature_fingerprint", "varchar(128) not null default ''", "signature_key_schema_version"),
	addPythonSignatureIndex(),
	dropLegacyPythonSignatureIndex(),
	{
		Schema:    catalog.MO_CATALOG,
		TableName: "mo_function_revisions",
		UpgType:   versions.CREATE_NEW_TABLE,
		UpgSql: fmt.Sprintf(`create table mo_catalog.mo_function_revisions (
			function_id bigint unsigned not null,
			revision bigint unsigned not null,
			namespace_version bigint unsigned not null,
			name varchar(100) not null,
			args json not null,
			arg_types varchar(%d) not null default '',
			rettype varchar(20) not null,
			body text not null,
			language varchar(20) not null,
			definition_schema_version int not null default 0,
			abi_contract varchar(64) not null default '',
			adapter_version varchar(64) not null default '',
			artifact_digest varchar(128) not null default '',
			environment_digest varchar(128) not null default '',
			sdk_version varchar(64) not null default '',
			null_policy varchar(64) not null default '',
			volatility varchar(20) not null default 'VOLATILE',
			definition_fingerprint varchar(128) not null default '',
			created_time timestamp,
			security_type varchar(10) not null default 'DEFINER',
			primary key(function_id, revision)
		)`, types.MaxStringSize),
		RequiredProtocolVersion: defines.MORPCVersion48,
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			return versions.CheckTableDefinition(txn, accountID, catalog.MO_CATALOG, "mo_function_revisions")
		},
	},
}

// addPythonSignatureIndex makes the shared function namespace distinguish the
// exact current Python descriptor while preserving the historical logical
// arg_types column for SQL UDFs and catalog readers.
func addPythonSignatureIndex() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:                  catalog.MO_CATALOG,
		TableName:               "mo_user_defined_function",
		UpgType:                 versions.ADD_INDEX,
		UpgSql:                  "create unique index name_db_arg_types_descriptor on mo_catalog.mo_user_defined_function(name, db, arg_types, canonical_input_descriptor)",
		RequiredProtocolVersion: defines.MORPCVersion48,
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			return versions.CheckIndexDefinition(
				txn, accountID, catalog.MO_CATALOG, "mo_user_defined_function", "name_db_arg_types_descriptor",
			)
		},
	}
}

func dropLegacyPythonSignatureIndex() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:                  catalog.MO_CATALOG,
		TableName:               "mo_user_defined_function",
		UpgType:                 versions.DROP_INDEX,
		UpgSql:                  "alter table mo_catalog.mo_user_defined_function drop index name_db_arg_types",
		RequiredProtocolVersion: defines.MORPCVersion48,
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			exists, err := versions.CheckIndexDefinition(
				txn, accountID, catalog.MO_CATALOG, "mo_user_defined_function", "name_db_arg_types",
			)
			return !exists, err
		},
	}
}

func addPythonRevisionHeadColumn(name, definition string) versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    catalog.MO_CATALOG,
		TableName: "mo_user_defined_function",
		UpgType:   versions.ADD_COLUMN,
		UpgSql: fmt.Sprintf(
			"alter table mo_catalog.mo_user_defined_function add column %s %s after function_id",
			name, definition,
		),
		RequiredProtocolVersion: defines.MORPCVersion48,
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			column, err := versions.CheckTableColumn(txn, accountID, catalog.MO_CATALOG, "mo_user_defined_function", name)
			if err != nil {
				return false, err
			}
			if !column.IsExits {
				return false, nil
			}
			if column.ColType == "" {
				return false, moerr.NewInternalErrorNoCtxf("catalog column %s has no type", name)
			}
			return true, nil
		},
	}
}

func addPythonSignatureColumn(name, definition, after string) versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    catalog.MO_CATALOG,
		TableName: "mo_user_defined_function",
		UpgType:   versions.ADD_COLUMN,
		UpgSql: fmt.Sprintf(
			"alter table mo_catalog.mo_user_defined_function add column %s %s after %s",
			name, definition, after,
		),
		RequiredProtocolVersion: defines.MORPCVersion48,
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			column, err := versions.CheckTableColumn(txn, accountID, catalog.MO_CATALOG, "mo_user_defined_function", name)
			if err != nil {
				return false, err
			}
			if !column.IsExits {
				return false, nil
			}
			if column.ColType == "" {
				return false, moerr.NewInternalErrorNoCtxf("catalog column %s has no type", name)
			}
			return true, nil
		},
	}
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
