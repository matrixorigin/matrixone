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
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var tenantUpgEntries = []versions.UpgradeEntry{
	upg_mo_indexes_add_IndexAlgoName,
	upg_mo_indexes_add_IndexAlgoTableType,
	upg_mo_indexes_add_IndexAlgoParams,
	upg_mo_foreign_keys,
	upg_system_metrics_sql_statement_duration_total,
	upg_mo_snapshots,
	upg_sql_statement_cu,
}

var tenantUpgPrepareEntres = []versions.UpgradeEntry{
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
	UpgSql: fmt.Sprintf(`create table %s.%s(
				constraint_name varchar(5000) not null,
				constraint_id BIGINT UNSIGNED not null,
				db_name varchar(5000) not null,
				db_id BIGINT UNSIGNED not null,
				table_name varchar(5000) not null,
				table_id BIGINT UNSIGNED not null,
				column_name varchar(256) not null,
				column_id BIGINT UNSIGNED not null,
				refer_db_name varchar(5000) not null,
				refer_db_id BIGINT UNSIGNED not null,
				refer_table_name varchar(5000) not null,
				refer_table_id BIGINT UNSIGNED not null,
				refer_column_name varchar(256) not null,
				refer_column_id BIGINT UNSIGNED not null,
				on_delete varchar(128) not null,
				on_update varchar(128) not null,
				primary key(
					constraint_name,
					constraint_id,
					db_name,
					db_id,
					table_name,
					table_id,
					column_name,
					column_id,
					refer_db_name,
					refer_db_id,
					refer_table_name,
					refer_table_id,
					refer_column_name,
					refer_column_id)
			);`, catalog.MO_CATALOG, catalog.MOForeignKeys),
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
}

var upg_mo_snapshots = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_SNAPSHOTS,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql: fmt.Sprintf(`CREATE TABLE %s.%s (
			snapshot_id uuid unique key,
			sname varchar(64) primary key,
			ts timestamp,
			level enum('cluster','account','database','table'),
	        account_name varchar(300),
			database_name varchar(5000),
			table_name  varchar(5000)
			);`, catalog.MO_CATALOG, catalog.MO_SNAPSHOTS),
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
