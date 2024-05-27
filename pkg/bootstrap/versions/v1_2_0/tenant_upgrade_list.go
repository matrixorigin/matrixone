// Copyright 2024 Matrix Origin
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

package v1_2_0

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

var tenantUpgEntries = []versions.UpgradeEntry{
	upg_mo_indexes_add_IndexAlgoName,
	upg_mo_indexes_add_IndexAlgoTableType,
	upg_mo_indexes_add_IndexAlgoParams,
	upg_mo_foreign_keys,
	upg_system_metrics_sql_statement_duration_total,
	upg_mo_snapshots,
	upg_sql_statement_cu,
	upg_mysql_role_edges,
	upg_information_schema_schema_privileges,
	upg_information_schema_table_privileges,
	upg_information_schema_column_privileges,
	upg_information_schema_collations,
	upg_information_schema_table_constraints,
	upg_information_schema_events,
	upg_information_schema_tables,
	upg_information_schema_processlist,
	upg_information_schema_referenctial_constraints,
	upg_information_schema_columns,
	upg_information_schema_views,
	upg_information_schema_partitions,
	upg_mo_catalog_mo_sessions,
	upg_mo_catalog_mo_configurations,
	upg_mo_catalog_mo_locks,
	upg_mo_catalog_mo_variables,
	upg_mo_catalog_mo_transactions,
	upg_mo_catalog_mo_cache,
	upg_mo_pub,
}

var UpgPrepareEntres = []versions.UpgradeEntry{
	upg_mo_indexes_add_IndexAlgoName,
	upg_mo_indexes_add_IndexAlgoTableType,
	upg_mo_indexes_add_IndexAlgoParams,
	upg_mo_foreign_keys,
}

var createFrameworkDepsEntres = []versions.UpgradeEntry{
	upg_mo_indexes_add_IndexAlgoName,
	upg_mo_indexes_add_IndexAlgoTableType,
	upg_mo_indexes_add_IndexAlgoParams,
	upg_mo_foreign_keys,
}

// MOForeignKeys = "mo_foreign_keys"
var upg_mo_foreign_keys = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MOForeignKeys,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    frontend.MoCatalogMoForeignKeysDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		isExist, err := versions.CheckTableDefinition(txn, accountId, catalog.MO_CATALOG, catalog.MOForeignKeys)
		if err != nil {
			return false, err
		}

		if isExist {
			return true, nil
		}
		return false, nil
	},
}

var upg_mo_indexes_add_IndexAlgoName = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_INDEXES,
	UpgType:   versions.ADD_COLUMN,
	UpgSql:    fmt.Sprintf(`alter table %s.%s add column %s varchar(11) after type;`, catalog.MO_CATALOG, catalog.MO_INDEXES, catalog.IndexAlgoName),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		colInfo, err := versions.CheckTableColumn(txn, accountId, catalog.MO_CATALOG, catalog.MO_INDEXES, catalog.IndexAlgoName)
		if err != nil {
			return false, err
		}

		if colInfo.IsExits {
			return true, nil
		}
		return false, nil
	},
}

var upg_mo_indexes_add_IndexAlgoTableType = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_INDEXES,
	UpgType:   versions.ADD_COLUMN,
	UpgSql:    fmt.Sprintf(`alter table %s.%s add column %s varchar(11) after %s;`, catalog.MO_CATALOG, catalog.MO_INDEXES, catalog.IndexAlgoTableType, catalog.IndexAlgoName),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		colInfo, err := versions.CheckTableColumn(txn, accountId, catalog.MO_CATALOG, catalog.MO_INDEXES, catalog.IndexAlgoTableType)
		if err != nil {
			return false, err
		}

		if colInfo.IsExits {
			return true, nil
		}
		return false, nil
	},
}

var upg_mo_indexes_add_IndexAlgoParams = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_INDEXES,
	UpgType:   versions.ADD_COLUMN,
	UpgSql:    fmt.Sprintf(`alter table %s.%s add column %s varchar(2048) after %s;`, catalog.MO_CATALOG, catalog.MO_INDEXES, catalog.IndexAlgoParams, catalog.IndexAlgoTableType),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		colInfo, err := versions.CheckTableColumn(txn, accountId, catalog.MO_CATALOG, catalog.MO_INDEXES, catalog.IndexAlgoParams)
		if err != nil {
			return false, err
		}

		if colInfo.IsExits {
			return true, nil
		}
		return false, nil
	},
}

var upg_system_metrics_sql_statement_duration_total = versions.UpgradeEntry{
	Schema:    catalog.MO_SYSTEM_METRICS,
	TableName: "sql_statement_duration_total",
	UpgType:   versions.CREATE_VIEW,
	UpgSql: fmt.Sprintf("CREATE VIEW IF NOT EXISTS `%s`.`%s` as "+
		"SELECT `collecttime`, `value`, `node`, `role`, `account`, `type` "+
		"from `system_metrics`.`metric` "+
		"where `metric_name` = 'sql_statement_duration_total'",
		catalog.MO_SYSTEM_METRICS, "sql_statement_duration_total"),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return false, nil
	},
	PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;", catalog.MO_SYSTEM_METRICS, "sql_statement_duration_total"),
}

var upg_mo_snapshots = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_SNAPSHOTS,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    frontend.MoCatalogMoSnapshotsDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		isExist, err := versions.CheckTableDefinition(txn, accountId, catalog.MO_CATALOG, catalog.MO_SNAPSHOTS)
		if err != nil {
			return false, err
		}

		if isExist {
			return true, nil
		}
		return false, nil
	},
}

var upg_sql_statement_cu = versions.UpgradeEntry{
	Schema:    catalog.MO_SYSTEM_METRICS,
	TableName: catalog.MO_SQL_STMT_CU,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql: fmt.Sprintf(`CREATE TABLE %s.%s (
		account VARCHAR(1024) DEFAULT 'sys' COMMENT 'account name',
		collecttime DATETIME NOT NULL COMMENT 'metric data collect time',
		value DOUBLE DEFAULT '0.0' COMMENT 'metric value',
		node VARCHAR(1024) DEFAULT 'monolithic' COMMENT 'mo node uuid',
		role VARCHAR(1024) DEFAULT 'monolithic' COMMENT 'mo node role, like: CN, DN, LOG',
		sql_source_type VARCHAR(1024) NOT NULL COMMENT 'sql_source_type, val like: external_sql, cloud_nonuser_sql, cloud_user_sql, internal_sql, ...'
		) CLUSTER BY (account, collecttime);`, catalog.MO_SYSTEM_METRICS, catalog.MO_SQL_STMT_CU),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		isExist, err := versions.CheckTableDefinition(txn, accountId, catalog.MO_SYSTEM_METRICS, catalog.MO_SQL_STMT_CU)
		if err != nil {
			return false, err
		}

		if isExist {
			return true, nil
		}
		return false, nil
	},
}

// ------------------------------------------------------------------------------------------------------------------
var upg_mysql_role_edges = versions.UpgradeEntry{
	Schema:    sysview.MysqlDBConst,
	TableName: "role_edges",
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    sysview.MysqlRoleEdgesDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, sysview.MysqlDBConst, "role_edges")
	},
}

var upg_information_schema_schema_privileges = versions.UpgradeEntry{
	Schema:    sysview.InformationDBConst,
	TableName: "schema_privileges",
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    sysview.InformationSchemaSchemaPrivilegesDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, sysview.InformationDBConst, "schema_privileges")
	},
}

var upg_information_schema_table_privileges = versions.UpgradeEntry{
	Schema:    sysview.InformationDBConst,
	TableName: "table_privileges",
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    sysview.InformationSchemaTablePrivilegesDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, sysview.InformationDBConst, "table_privileges")
	},
}

var upg_information_schema_column_privileges = versions.UpgradeEntry{
	Schema:    sysview.InformationDBConst,
	TableName: "column_privileges",
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    sysview.InformationSchemaColumnPrivilegesDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, sysview.InformationDBConst, "column_privileges")
	},
}

var upg_information_schema_collations = versions.UpgradeEntry{
	Schema:    sysview.InformationDBConst,
	TableName: "collations",
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    sysview.InformationSchemaCollationsDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, sysview.InformationDBConst, "collations")
	},
}

var upg_information_schema_table_constraints = versions.UpgradeEntry{
	Schema:    sysview.InformationDBConst,
	TableName: "table_constraints",
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    sysview.InformationSchemaTableConstraintsDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, sysview.InformationDBConst, "table_constraints")
	},
}

var upg_information_schema_events = versions.UpgradeEntry{
	Schema:    sysview.InformationDBConst,
	TableName: "events",
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql:    sysview.InformationSchemaEventsDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, sysview.InformationDBConst, "events")
	},
}

var upg_information_schema_tables = versions.UpgradeEntry{
	Schema:    sysview.InformationDBConst,
	TableName: "TABLES",
	UpgType:   versions.MODIFY_VIEW,
	UpgSql:    sysview.InformationSchemaTablesDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, viewDef, err := versions.CheckViewDefinition(txn, accountId, sysview.InformationDBConst, "TABLES")
		if err != nil {
			return false, err
		}

		if exists && viewDef == sysview.InformationSchemaTablesDDL {
			return true, nil
		}
		return false, nil
	},
	PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;", sysview.InformationDBConst, "TABLES"),
}

var upg_information_schema_processlist = versions.UpgradeEntry{
	Schema:    sysview.InformationDBConst,
	TableName: "processlist",
	UpgType:   versions.MODIFY_VIEW,
	UpgSql:    sysview.InformationSchemaProcesslistDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, viewDef, err := versions.CheckViewDefinition(txn, accountId, sysview.InformationDBConst, "processlist")
		if err != nil {
			return false, err
		}

		if exists && viewDef == sysview.InformationSchemaProcesslistDDL {
			return true, nil
		}
		return false, nil
	},
	PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;", sysview.InformationDBConst, "processlist"),
}

var upg_information_schema_referenctial_constraints = versions.UpgradeEntry{
	Schema:    sysview.InformationDBConst,
	TableName: "REFERENTIAL_CONSTRAINTS",
	UpgType:   versions.CREATE_VIEW,
	UpgSql:    sysview.InformationSchemaReferentialConstraintsDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, viewDef, err := versions.CheckViewDefinition(txn, accountId, sysview.InformationDBConst, "REFERENTIAL_CONSTRAINTS")
		if err != nil {
			return false, err
		}

		if exists && viewDef == sysview.InformationSchemaReferentialConstraintsDDL {
			return true, nil
		}
		return false, nil
	},
	PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;", sysview.InformationDBConst, "REFERENTIAL_CONSTRAINTS"),
}

var upg_information_schema_columns = versions.UpgradeEntry{
	Schema:    sysview.InformationDBConst,
	TableName: "COLUMNS",
	UpgType:   versions.MODIFY_VIEW,
	UpgSql:    sysview.InformationSchemaColumnsDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, viewDef, err := versions.CheckViewDefinition(txn, accountId, sysview.InformationDBConst, "COLUMNS")
		if err != nil {
			return false, err
		}

		if exists && viewDef == sysview.InformationSchemaColumnsDDL {
			return true, nil
		}
		return false, nil
	},
	PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;", sysview.InformationDBConst, "COLUMNS"),
}

var upg_information_schema_views = versions.UpgradeEntry{
	Schema:    sysview.InformationDBConst,
	TableName: "VIEWS",
	UpgType:   versions.MODIFY_VIEW,
	UpgSql:    sysview.InformationSchemaViewsDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, viewDef, err := versions.CheckViewDefinition(txn, accountId, sysview.InformationDBConst, "VIEWS")
		if err != nil {
			return false, err
		}

		if exists && viewDef == sysview.InformationSchemaViewsDDL {
			return true, nil
		}
		return false, nil
	},
	PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;", sysview.InformationDBConst, "VIEWS"),
}

var upg_information_schema_partitions = versions.UpgradeEntry{
	Schema:    sysview.InformationDBConst,
	TableName: "PARTITIONS",
	UpgType:   versions.MODIFY_VIEW,
	UpgSql:    sysview.InformationSchemaPartitionsDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, viewDef, err := versions.CheckViewDefinition(txn, accountId, sysview.InformationDBConst, "PARTITIONS")
		if err != nil {
			return false, err
		}

		if exists && viewDef == sysview.InformationSchemaPartitionsDDL {
			return true, nil
		}
		return false, nil
	},
	PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;", sysview.InformationDBConst, "PARTITIONS"),
}

// ----------------------------------------------------------------------------------------------------------------------
var upg_mo_catalog_mo_sessions = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: "mo_sessions",
	UpgType:   versions.MODIFY_VIEW,
	UpgSql:    frontend.MoCatalogMoSessionsDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, viewDef, err := versions.CheckViewDefinition(txn, accountId, catalog.MO_CATALOG, "mo_sessions")
		if err != nil {
			return false, err
		}

		if exists && viewDef == frontend.MoCatalogMoSessionsDDL {
			return true, nil
		}
		return false, nil
	},
	PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;", catalog.MO_CATALOG, "mo_sessions"),
}

var upg_mo_catalog_mo_configurations = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: "mo_configurations",
	UpgType:   versions.MODIFY_VIEW,
	UpgSql:    frontend.MoCatalogMoConfigurationsDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, viewDef, err := versions.CheckViewDefinition(txn, accountId, catalog.MO_CATALOG, "mo_configurations")
		if err != nil {
			return false, err
		}

		if exists && viewDef == frontend.MoCatalogMoConfigurationsDDL {
			return true, nil
		}
		return false, nil
	},
	PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;", catalog.MO_CATALOG, "mo_configurations"),
}

var upg_mo_catalog_mo_locks = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: "mo_locks",
	UpgType:   versions.MODIFY_VIEW,
	UpgSql:    frontend.MoCatalogMoLocksDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, viewDef, err := versions.CheckViewDefinition(txn, accountId, catalog.MO_CATALOG, "mo_locks")
		if err != nil {
			return false, err
		}

		if exists && viewDef == frontend.MoCatalogMoLocksDDL {
			return true, nil
		}
		return false, nil
	},
	PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;", catalog.MO_CATALOG, "mo_locks"),
}

var upg_mo_catalog_mo_variables = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: "mo_variables",
	UpgType:   versions.MODIFY_VIEW,
	UpgSql:    frontend.MoCatalogMoVariablesDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, viewDef, err := versions.CheckViewDefinition(txn, accountId, catalog.MO_CATALOG, "mo_variables")
		if err != nil {
			return false, err
		}

		if exists && viewDef == frontend.MoCatalogMoVariablesDDL {
			return true, nil
		}
		return false, nil
	},
	PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;", catalog.MO_CATALOG, "mo_variables"),
}

var upg_mo_catalog_mo_transactions = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: "mo_transactions",
	UpgType:   versions.MODIFY_VIEW,
	UpgSql:    frontend.MoCatalogMoTransactionsDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, viewDef, err := versions.CheckViewDefinition(txn, accountId, catalog.MO_CATALOG, "mo_transactions")
		if err != nil {
			return false, err
		}

		if exists && viewDef == frontend.MoCatalogMoTransactionsDDL {
			return true, nil
		}
		return false, nil
	},
	PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;", catalog.MO_CATALOG, "mo_transactions"),
}

var upg_mo_catalog_mo_cache = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: "mo_cache",
	UpgType:   versions.MODIFY_VIEW,
	UpgSql:    frontend.MoCatalogMoCacheDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, viewDef, err := versions.CheckViewDefinition(txn, accountId, catalog.MO_CATALOG, "mo_cache")
		if err != nil {
			return false, err
		}

		if exists && viewDef == frontend.MoCatalogMoCacheDDL {
			return true, nil
		}
		return false, nil
	},
	PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;", catalog.MO_CATALOG, "mo_cache"),
}

var upg_mo_pub = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_PUBS,
	UpgType:   versions.ADD_COLUMN,
	UpgSql:    "alter table `mo_catalog`.`mo_pubs` add column `update_time` timestamp",
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		colInfo, err := versions.CheckTableColumn(txn, accountId, "mo_catalog", catalog.MO_PUBS, "update_time")
		if err != nil {
			return false, err
		}

		if colInfo.IsExits {
			return true, nil
		}
		return false, nil
	},
}
