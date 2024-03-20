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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/txn/trace"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var clusterUpgEntries = []versions.UpgradeEntry{
	upg_mo_account,
	upg_mo_pub,
	upg_sys_async_task,
	upg_mo_role_privs,
	upg_mo_debug_eventTxnTable,
	upg_mo_debug_eventDataTable,
	upg_mo_debug_traceTableFilterTable,
	upg_mo_debug_traceTxnFilterTable,
	upg_mo_debug_traceStatementFilterTable,
	upg_mo_debug_eventErrorTable,
	upg_mo_debug_traceStatementTable,
	upg_mo_debug_eventTxnActionTable,
	upg_mo_debug_featuresTables,
}

var upg_mo_account = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MOAccountTable,
	UpgType:   versions.ADD_COLUMN,
	UpgSql:    "alter table `mo_account` add column `create_version` varchar(50) default '1.2.0' after suspended_time",
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		colInfo, err := versions.CheckTableColumn(txn, accountId, catalog.MO_CATALOG, catalog.MOAccountTable, "create_version")
		if err != nil {
			return false, err
		}

		if colInfo.IsExits {
			return true, nil
		}
		return false, nil
	},
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

var upg_sys_async_task = versions.UpgradeEntry{
	Schema:    catalog.MOTaskDB,
	TableName: catalog.MOSysAsyncTask,
	UpgType:   versions.MODIFY_COLUMN,
	UpgSql:    "alter table `mo_task`.`sys_async_task` modify task_id bigint auto_increment",
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		colInfo, err := versions.CheckTableColumn(txn, accountId, catalog.MOTaskDB, catalog.MOSysAsyncTask, "task_id")
		if err != nil {
			return false, err
		}

		if colInfo.IsExits {
			if strings.EqualFold(colInfo.ColType, versions.T_int64) {
				return true, nil
			}
		}
		return false, nil
	},
}

var upg_mo_role_privs = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: "mo_role_privs",
	UpgType:   versions.MODIFY_METADATA,
	UpgSql:    frontend.GenSQLForInsertUpgradeAccountPrivilege(),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		sql := frontend.GenSQLForCheckUpgradeAccountPrivilegeExist()
		return versions.CheckTableDataExist(txn, accountId, sql)
	},
}

var upg_mo_debug_eventTxnTable = versions.UpgradeEntry{
	Schema:    trace.DebugDB,
	TableName: trace.EventTxnTable,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql: fmt.Sprintf(`create table %s.%s(
			ts 			          bigint       not null,
			txn_id                varchar(50)  not null,
			cn                    varchar(100) not null,
			event_type            varchar(50)  not null,
			txn_status			  varchar(10),
			snapshot_ts           varchar(50),
			commit_ts             varchar(50),
			info                  varchar(1000)
		)`, trace.DebugDB, trace.EventTxnTable),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, trace.DebugDB, trace.EventTxnTable)
	},
	PreSql: fmt.Sprintf("create database if not exists %s", trace.DebugDB),
}

var upg_mo_debug_eventDataTable = versions.UpgradeEntry{
	Schema:    trace.DebugDB,
	TableName: trace.EventDataTable,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql: fmt.Sprintf(`create table %s.%s(
			ts 			          bigint          not null,
			cn                    varchar(100)    not null,
			event_type            varchar(50)     not null,
			entry_type			  varchar(50)     not null,
			table_id 	          bigint UNSIGNED not null,
			txn_id                varchar(50),
			row_data              varchar(500)    not null, 
			committed_ts          varchar(50),
			snapshot_ts           varchar(50)
		)`, trace.DebugDB, trace.EventDataTable),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, trace.DebugDB, trace.EventDataTable)
	},
	PreSql: fmt.Sprintf("create database if not exists %s", trace.DebugDB),
}

var upg_mo_debug_traceTableFilterTable = versions.UpgradeEntry{
	Schema:    trace.DebugDB,
	TableName: trace.TraceTableFilterTable,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql: fmt.Sprintf(`create table %s.%s(
			id                    bigint UNSIGNED primary key auto_increment,
			table_id			  bigint UNSIGNED not null,
			table_name            varchar(50)     not null,
			columns               varchar(200)
		)`, trace.DebugDB, trace.TraceTableFilterTable),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, trace.DebugDB, trace.TraceTableFilterTable)
	},
	PreSql: fmt.Sprintf("create database if not exists %s", trace.DebugDB),
}

var upg_mo_debug_traceTxnFilterTable = versions.UpgradeEntry{
	Schema:    trace.DebugDB,
	TableName: trace.TraceTxnFilterTable,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql: fmt.Sprintf(`create table %s.%s(
			id             bigint UNSIGNED primary key auto_increment,
			method         varchar(50)     not null,
			value          varchar(500)    not null
		)`, trace.DebugDB, trace.TraceTxnFilterTable),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, trace.DebugDB, trace.TraceTxnFilterTable)
	},
	PreSql: fmt.Sprintf("create database if not exists %s", trace.DebugDB),
}

var upg_mo_debug_traceStatementFilterTable = versions.UpgradeEntry{
	Schema:    trace.DebugDB,
	TableName: trace.TraceStatementFilterTable,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql: fmt.Sprintf(`create table %s.%s(
			id             bigint UNSIGNED primary key auto_increment,
			method         varchar(50)     not null,
			value          varchar(500)    not null
		)`, trace.DebugDB, trace.TraceStatementFilterTable),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, trace.DebugDB, trace.TraceStatementFilterTable)
	},
	PreSql: fmt.Sprintf("create database if not exists %s", trace.DebugDB),
}

var upg_mo_debug_eventErrorTable = versions.UpgradeEntry{
	Schema:    trace.DebugDB,
	TableName: trace.EventErrorTable,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql: fmt.Sprintf(`create table %s.%s(
			ts 			          bigint          not null,
			txn_id                varchar(50)     not null,
			error_info            varchar(1000)   not null
		)`, trace.DebugDB, trace.EventErrorTable),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, trace.DebugDB, trace.EventErrorTable)
	},
	PreSql: fmt.Sprintf("create database if not exists %s", trace.DebugDB),
}

var upg_mo_debug_traceStatementTable = versions.UpgradeEntry{
	Schema:    trace.DebugDB,
	TableName: trace.TraceStatementTable,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql: fmt.Sprintf(`create table %s.%s(
			ts 			   bigint          not null,
			txn_id         varchar(50)     not null,
			sql            varchar(1000)   not null,
			cost_us        bigint          not null
		)`, trace.DebugDB, trace.TraceStatementTable),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, trace.DebugDB, trace.TraceStatementTable)
	},
	PreSql: fmt.Sprintf("create database if not exists %s", trace.DebugDB),
}

var upg_mo_debug_eventTxnActionTable = versions.UpgradeEntry{
	Schema:    trace.DebugDB,
	TableName: trace.EventTxnActionTable,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql: fmt.Sprintf(`create table %s.%s(
			ts 			          bigint          not null,
			txn_id                varchar(50)     not null,
			cn                    varchar(50)     not null,
			table_id              bigint UNSIGNED,
			action                varchar(100)    not null,
			action_sequence       bigint UNSIGNED not null,
			value                 bigint,
			unit                  varchar(10),
			err                   varchar(100) 
		)`, trace.DebugDB, trace.EventTxnActionTable),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, trace.DebugDB, trace.EventTxnActionTable)
	},
	PreSql: fmt.Sprintf("create database if not exists %s", trace.DebugDB),
}

var upg_mo_debug_featuresTables = versions.UpgradeEntry{
	Schema:    trace.DebugDB,
	TableName: trace.FeaturesTables,
	UpgType:   versions.CREATE_NEW_TABLE,
	UpgSql: fmt.Sprintf(`create table %s.%s(
			name    varchar(50) not null primary key,
			state   varchar(20) not null
		)`, trace.DebugDB, trace.FeaturesTables),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		return versions.CheckTableDefinition(txn, accountId, trace.DebugDB, trace.FeaturesTables)
	},
	PostSql: fmt.Sprintf(`insert into %s.%s (name, state) values 
			('%s', '%s'), ('%s', '%s'), ('%s', '%s'), ('%s', '%s')`,
		trace.DebugDB, trace.FeaturesTables,
		trace.FeatureTraceTxn, trace.StateDisable,
		trace.FeatureTraceTxnAction, trace.StateDisable,
		trace.FeatureTraceData, trace.StateDisable,
		trace.FeatureTraceStatement, trace.StateDisable),
}
