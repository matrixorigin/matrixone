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
	"context"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// retiredKafkaSinkTaskCode is the wire value formerly assigned to
// TaskCode_ConnectorKafkaSink. The protobuf value is reserved, so persisted
// daemon tasks can still be identified without restoring the removed feature.
const retiredKafkaSinkTaskCode = 4

var clusterUpgEntries = []versions.UpgradeEntry{
	retireKafkaSinkDaemonTasks,
	createMoViewDependencies,
	createMoViewRefresh,
	addMoViewDependenciesTargetNameIndex,
	addMoViewRefreshTargetNameIndex,
	seedViewMetadataRevalidation,
	addSQLTaskAccountIndex,
	addSQLTaskRunAccountIndex,
	addAsyncTaskParentIndex,
	cleanupLegacyOrphanSQLTaskChildren,
	createMoCdcSnapshot,
	addCdcWatermarkSourceTableID,
	addCdcWatermarkOwnerGeneration,
	upgradeDaemonClaimPrecision,
}

// A daemon claim must survive the SQL round trip and distinguish successive
// owners within one second. Widening preserves existing second-aligned rows.
var upgradeDaemonClaimPrecision = versions.UpgradeEntry{
	Schema:    catalog.MOTaskDB,
	TableName: catalog.MOSysDaemonTask,
	UpgType:   versions.MODIFY_COLUMN,
	UpgSql:    "alter table mo_task.sys_daemon_task modify last_run timestamp(6)",
	CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
		res, err := txn.Exec(
			"select atttyp from mo_catalog.mo_columns where account_id = 0 "+
				"and att_database = 'mo_task' and att_relname = 'sys_daemon_task' "+
				"and attname = 'last_run'", executor.StatementOption{}.WithAccountID(accountID))
		if err != nil {
			return false, err
		}
		defer res.Close()
		var typ types.Type
		res.ReadRows(func(rows int, cols []*vector.Vector) bool {
			if rows == 1 && len(cols) == 1 {
				encoded := cols[0].GetBytesAt(0)
				if len(encoded) < typ.ProtoSize() {
					err = moerr.NewInternalErrorNoCtx("invalid daemon claim column type")
				} else {
					err = typ.Unmarshal(encoded)
				}
			}
			return false
		})
		return typ.Oid == types.T_timestamp && typ.Scale == 6, err
	},
	RequiredProtocolVersion: defines.MORPCVersion48,
}

var addCdcWatermarkSourceTableID = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_CDC_WATERMARK,
	UpgType:   versions.ADD_COLUMN,
	UpgSql: "alter table mo_catalog.mo_cdc_watermark " +
		"add column source_table_id bigint unsigned not null default 0 after watermark",
	CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
		column, err := versions.CheckTableColumn(
			txn, accountID, catalog.MO_CATALOG, catalog.MO_CDC_WATERMARK, "source_table_id")
		return column.IsExits, err
	},
	// Older CNs insert six positional values into mo_cdc_watermark. Delay the
	// seventh column until every CN writer uses an explicit column list.
	RequiredProtocolVersion: defines.MORPCVersion48,
}

var addCdcWatermarkOwnerGeneration = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MO_CDC_WATERMARK,
	UpgType:   versions.ADD_COLUMN,
	UpgSql: "alter table mo_catalog.mo_cdc_watermark " +
		"add column owner_generation bigint unsigned not null default 0 after source_table_id",
	CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
		column, err := versions.CheckTableColumn(
			txn, accountID, catalog.MO_CATALOG, catalog.MO_CDC_WATERMARK, "owner_generation")
		return column.IsExits, err
	},
	// This column is used only by stable-task writers, but the table shape is
	// still shared with every CN. Keep its rollout behind the same mixed-version
	// gate as source_table_id.
	RequiredProtocolVersion: defines.MORPCVersion48,
}

var createMoCdcSnapshot = newCatalogTable(
	catalog.MO_CDC_SNAPSHOT, frontend.MoCatalogMoCdcSnapshotDDL)

var addSQLTaskAccountIndex = newTaskMetadataIndex(
	catalog.MOSQLTask, "idx_account_id", "account_id")

var addSQLTaskRunAccountIndex = newTaskMetadataIndex(
	catalog.MOSQLTaskRun, "idx_account_id", "account_id")

var addAsyncTaskParentIndex = newTaskMetadataIndex(
	catalog.MOSysAsyncTask, "idx_task_parent_id", "task_parent_id")

const legacyOrphanSQLTaskChildPredicate = "task_parent_id like 'sql-task:%' and task_parent_id not in (" +
	"select concat('sql-task:', task_id) from mo_task.sql_task " +
	"union select concat('sql-task:', task_id) from mo_task.sql_task_run)"

var cleanupLegacyOrphanSQLTaskChildren = versions.UpgradeEntry{
	Schema: catalog.MOTaskDB, TableName: catalog.MOSysAsyncTask, UpgType: versions.MODIFY_METADATA,
	UpgSql: fmt.Sprintf("delete from %s.%s where %s", catalog.MOTaskDB, catalog.MOSysAsyncTask,
		legacyOrphanSQLTaskChildPredicate),
	CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
		exists, err := versions.CheckTableDataExist(txn, accountID, fmt.Sprintf(
			"select 1 from %s.%s where %s limit 1", catalog.MOTaskDB,
			catalog.MOSysAsyncTask, legacyOrphanSQLTaskChildPredicate))
		if err != nil || exists {
			return false, err
		}
		if err := versions.CheckCommonProtocolVersion(txn, defines.MORPCVersion42); err != nil {
			return false, err
		}
		return true, nil
	},
	RequiredProtocolVersion: defines.MORPCVersion42,
}

func newTaskMetadataIndex(tableName, indexName, columnName string) versions.UpgradeEntry {
	return versions.UpgradeEntry{Schema: catalog.MOTaskDB, TableName: tableName, UpgType: versions.ADD_INDEX,
		UpgSql: fmt.Sprintf("create index %s on %s.%s(%s)", indexName, catalog.MOTaskDB, tableName, columnName),
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			res, err := txn.Exec("SHOW CREATE TABLE "+sqlquote.QualifiedIdent(catalog.MOTaskDB, tableName), executor.StatementOption{}.WithAccountID(accountID))
			if err != nil {
				return false, err
			}
			defer res.Close()
			var createSQL string
			res.ReadRows(func(rows int, cols []*vector.Vector) bool {
				if rows == 1 && len(cols) == 2 {
					createSQL = cols[1].GetStringAt(0)
				}
				return false
			})
			statements, err := mysql.Parse(context.Background(), createSQL, 1)
			if err != nil {
				return false, err
			}
			defer func() {
				for _, stmt := range statements {
					stmt.Free()
				}
			}()
			if len(statements) != 1 {
				return false, moerr.NewInternalErrorNoCtx("missing task table definition during index upgrade")
			}
			definition, ok := statements[0].(*tree.CreateTable)
			if !ok {
				return false, moerr.NewInternalErrorNoCtx("invalid task table definition during index upgrade")
			}
			for _, def := range definition.Defs {
				if index, ok := def.(*tree.Index); ok && strings.EqualFold(index.Name, indexName) {
					return true, nil
				}
			}
			return false, nil
		},
	}
}

var createMoViewDependencies = newViewMetadataCatalogTable(
	catalog.MO_VIEW_DEPENDENCIES, catalog.MoViewDependenciesDDL)

var createMoViewRefresh = newViewMetadataCatalogTable(
	catalog.MO_VIEW_REFRESH, catalog.MoViewRefreshDDL)

var addMoViewDependenciesTargetNameIndex = newViewMetadataTargetNameIndex(
	catalog.MO_VIEW_DEPENDENCIES, "idx_view_dependency_target_name")

var addMoViewRefreshTargetNameIndex = newViewMetadataTargetNameIndex(
	catalog.MO_VIEW_REFRESH, "idx_view_refresh_target_name")

func newViewMetadataTargetNameIndex(tableName, indexName string) versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    catalog.MO_CATALOG,
		TableName: tableName,
		UpgType:   versions.MODIFY_METADATA,
		UpgSql: fmt.Sprintf("alter table %s.%s add index %s("+
			"account_id,target_database_name(256),target_relation_name(256))",
			catalog.MO_CATALOG, tableName, indexName),
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			return versions.CheckIndexDefinition(
				txn, accountID, catalog.MO_CATALOG, tableName, indexName)
		},
	}
}

var seedViewMetadataRevalidation = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: "mo_view_metadata_revalidation",
	UpgType:   versions.MODIFY_METADATA,
	UpgSql: fmt.Sprintf(
		"replace into %s.%s (%s) select a.account_id,0,0,0,'%s','%s',0,0,0,0,0,"+
			"'','','','','%s','',0,null,0,1 from %s.%s a",
		catalog.MO_CATALOG, catalog.MO_VIEW_DEPENDENCIES, catalog.MoViewDependenciesColumns,
		catalog.LegacyViewScanCursorDatabase, catalog.LegacyViewScanCursorRelation,
		catalog.ViewRefreshStatusRevalidateScan,
		catalog.MO_CATALOG, catalog.MOAccountTable),
	CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
		return versions.CheckTableDataExist(txn, accountID, fmt.Sprintf(
			"select 1 from %s.%s where account_id=0 and target_relation_id=0 "+
				"and dependency_ordinal=0 and source_relation_kind='%s' limit 1",
			catalog.MO_CATALOG, catalog.MO_VIEW_DEPENDENCIES,
			catalog.ViewRefreshStatusRevalidateScan))
	},
}

func newViewMetadataCatalogTable(name, ddl string) versions.UpgradeEntry {
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

func newCatalogTable(name, ddl string) versions.UpgradeEntry {
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

var retireKafkaSinkDaemonTasks = versions.UpgradeEntry{
	Schema:    catalog.MOTaskDB,
	TableName: catalog.MOSysDaemonTask,
	UpgType:   versions.MODIFY_METADATA,
	UpgSql: fmt.Sprintf(
		"update %s.%s set task_status = %d, update_at = current_timestamp() where %s",
		catalog.MOTaskDB,
		catalog.MOSysDaemonTask,
		task.TaskStatus_CancelRequested,
		activeKafkaSinkTaskFilter(),
	),
	CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
		exists, err := versions.CheckTableDataExist(
			txn,
			accountID,
			fmt.Sprintf(
				"select 1 from %s.%s where %s limit 1",
				catalog.MOTaskDB,
				catalog.MOSysDaemonTask,
				activeKafkaSinkTaskFilter(),
			),
		)
		return !exists, err
	},
}

// activeKafkaSinkTaskFilter deliberately enumerates known non-terminal states.
// Unknown future states and historical terminal rows must not be rewritten by
// a compatibility migration.
func activeKafkaSinkTaskFilter() string {
	return fmt.Sprintf(
		"task_metadata_executor = %d and task_status in (%d, %d, %d, %d, %d, %d)",
		retiredKafkaSinkTaskCode,
		task.TaskStatus_Created,
		task.TaskStatus_Running,
		task.TaskStatus_Paused,
		task.TaskStatus_ResumeRequested,
		task.TaskStatus_PauseRequested,
		task.TaskStatus_RestartRequested,
	)
}
