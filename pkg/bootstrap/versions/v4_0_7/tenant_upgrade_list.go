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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// The index metadata provenance migration is NOT an entry: the set of tables it alters is
// only known at runtime (one metadata table per index, per account), which a fixed UpgSql
// cannot express. It runs from HandleTenantUpgrade, the same escape hatch v4_0_6 uses for
// upgradeLegacyForeignKeyMetadata.
var tenantUpgEntries = []versions.UpgradeEntry{
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
