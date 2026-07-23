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

package v4_0_5

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	icebergsql "github.com/matrixorigin/matrixone/pkg/sql/iceberg"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var tenantUpgEntries = []versions.UpgradeEntry{
	upgradeIcebergCatalogIDAllocator(),
	addOrphanFileColumn("namespace", "varchar(2048) not null default ''", "catalog_id"),
	addOrphanFileColumn("table_name", "varchar(1024) not null default ''", "namespace"),
	addOrphanFileColumn("file_path", "varchar(4096) not null default ''", "table_location_hash"),
	addWorkloadPolicyAccountColumn(),
	addWorkloadPolicyAccountUniqueIndex(),
}

func upgradeIcebergCatalogIDAllocator() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    catalog.MO_CATALOG,
		TableName: icebergsql.TableCatalogs,
		UpgType:   versions.MODIFY_COLUMN,
		// Existing deployments used an account-local MAX(id)+1 allocator. Preserve
		// the account-first composite key (and its existing duplicate IDs across
		// accounts); only move allocation into the storage engine. MatrixOne permits
		// an auto-increment column inside this composite primary key.
		UpgSql: fmt.Sprintf(
			"alter table %s.%s modify catalog_id bigint unsigned not null auto_increment",
			catalog.MO_CATALOG,
			icebergsql.TableCatalogs,
		),
		CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
			column, err := versions.CheckTableColumn(txn, accountId, catalog.MO_CATALOG, icebergsql.TableCatalogs, "catalog_id")
			if err != nil || !column.IsExits || column.Extra != "auto_increment" {
				return false, err
			}
			return true, nil
		},
	}
}

func addOrphanFileColumn(column, definition, after string) versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    catalog.MO_CATALOG,
		TableName: icebergsql.TableOrphanFiles,
		UpgType:   versions.ADD_COLUMN,
		UpgSql:    fmt.Sprintf("alter table %s.%s add column %s %s after %s", catalog.MO_CATALOG, icebergsql.TableOrphanFiles, column, definition, after),
		CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
			info, err := versions.CheckTableColumn(txn, accountId, catalog.MO_CATALOG, icebergsql.TableOrphanFiles, column)
			if err != nil {
				return false, err
			}
			return info.IsExits, nil
		},
	}
}

func addWorkloadPolicyAccountColumn() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    catalog.MO_CATALOG,
		TableName: "mo_mysql_compatibility_mode",
		UpgType:   versions.ADD_COLUMN,
		UpgSql: fmt.Sprintf(
			"alter table %s.mo_mysql_compatibility_mode add column %s int generated always as (%s) stored",
			catalog.MO_CATALOG,
			frontend.MoMysqlCompatWorkloadPolicyAccountColumn,
			frontend.MoMysqlCompatWorkloadPolicyExpression,
		),
		CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
			info, err := versions.CheckTableColumn(
				txn,
				accountId,
				catalog.MO_CATALOG,
				"mo_mysql_compatibility_mode",
				frontend.MoMysqlCompatWorkloadPolicyAccountColumn,
			)
			if err != nil {
				return false, err
			}
			return info.IsExits, nil
		},
	}
}

func addWorkloadPolicyAccountUniqueIndex() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    catalog.MO_CATALOG,
		TableName: "mo_mysql_compatibility_mode",
		UpgType:   versions.ADD_CONSTRAINT_UNIQUE_INDEX,
		UpgSql: fmt.Sprintf(
			"alter table %s.mo_mysql_compatibility_mode add unique key %s (%s)",
			catalog.MO_CATALOG,
			frontend.MoMysqlCompatWorkloadPolicyUniqueIndex,
			frontend.MoMysqlCompatWorkloadPolicyAccountColumn,
		),
		CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
			return versions.CheckIndexDefinition(
				txn,
				accountId,
				catalog.MO_CATALOG,
				"mo_mysql_compatibility_mode",
				frontend.MoMysqlCompatWorkloadPolicyUniqueIndex,
			)
		},
	}
}
