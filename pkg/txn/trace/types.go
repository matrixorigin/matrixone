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

// Package trace retains only catalog definitions for stopped-version rollback.
// No transaction trace runtime or collection API remains. Historical rows and
// files are not consumed or cleaned up by this package.
// TODO(retire-txn-trace, #29249): after the release rollback window no longer
// includes a collector-bearing version, remove these declarations together with
// bootstrap/upgrade registrations, the planner's mo_debug protection and their
// retirement tests. Removing declarations must not implicitly delete user data.
package trace

import "fmt"

const (
	DebugDB                   = "mo_debug"
	FeaturesTables            = "trace_features"
	TraceTableFilterTable     = "trace_table_filters"
	TraceTxnFilterTable       = "trace_txn_filters"
	TraceStatementFilterTable = "trace_statement_filters"
	TraceStatementTable       = "trace_statement"
	EventTxnTable             = "trace_event_txn"
	EventDataTable            = "trace_event_data"
	EventErrorTable           = "trace_event_error"
	EventTxnActionTable       = "trace_event_txn_action"

	FeatureTraceStatement    = "statement"
	FeatureTraceTxn          = "txn"
	FeatureTraceTxnWorkspace = "txn-workspace"
	FeatureTraceTxnAction    = "txn-action"
	FeatureTraceData         = "data"
	StateDisable             = "disable"
)

var (
	EventTxnTableSQL = fmt.Sprintf(`create table %s.%s(
		ts 			          bigint       not null,
		txn_id                varchar(50)  not null,
		cn                    varchar(100) not null,
		event_type            varchar(50)  not null,
		txn_status			  varchar(10),
		snapshot_ts           varchar(50),
		commit_ts             varchar(50),
		info                  varchar(1000)
	)`, DebugDB, EventTxnTable)

	EventDataTableSQL = fmt.Sprintf(`create table %s.%s(
		ts 			          bigint          not null,
		cn                    varchar(100)    not null,
		event_type            varchar(50)     not null,
		entry_type			  varchar(50)     not null,
		table_id 	          bigint UNSIGNED not null,
		txn_id                varchar(50),
		row_data              varchar(500)    not null, 
		committed_ts          varchar(50),
		snapshot_ts           varchar(50)
	)`, DebugDB, EventDataTable)

	TraceTableFilterTableSQL = fmt.Sprintf(`create table %s.%s(
		id                    bigint UNSIGNED primary key auto_increment,
		table_id			  bigint UNSIGNED not null,
		table_name            varchar(50)     not null,
		columns               varchar(200)
	)`, DebugDB, TraceTableFilterTable)

	TraceTxnFilterTableSQL = fmt.Sprintf(`create table %s.%s(
		id             bigint UNSIGNED primary key auto_increment,
		method         varchar(50)     not null,
		value          varchar(500)    not null
	)`, DebugDB, TraceTxnFilterTable)

	TraceStatementFilterTableSQL = fmt.Sprintf(`create table %s.%s(
		id             bigint UNSIGNED primary key auto_increment,
		method         varchar(50)     not null,
		value          varchar(500)    not null
	)`, DebugDB, TraceStatementFilterTable)

	EventErrorTableSQL = fmt.Sprintf(`create table %s.%s(
		ts 			          bigint          not null,
		txn_id                varchar(50)     not null,
		error_info            varchar(1000)   not null
	)`, DebugDB, EventErrorTable)

	TraceStatementTableSQL = fmt.Sprintf(`create table %s.%s(
		ts 			   bigint          not null,
		txn_id         varchar(50)     not null,
		sql            varchar(1000)   not null,
		cost_us        bigint          not null
	)`, DebugDB, TraceStatementTable)

	EventTxnActionTableSQL = fmt.Sprintf(`create table %s.%s(
		ts 			          bigint          not null,
		txn_id                varchar(50)     not null,
		cn                    varchar(50)     not null,
		table_id              bigint UNSIGNED,
		action                varchar(100)    not null,
		action_sequence       bigint UNSIGNED not null,
		value                 bigint,
		unit                  varchar(10),
		err                   varchar(100) 
	)`, DebugDB, EventTxnActionTable)

	FeaturesTablesSQL = fmt.Sprintf(`create table %s.%s(
		name    varchar(50) not null primary key,
		state   varchar(20) not null
	)`, DebugDB, FeaturesTables)

	InitFeatureTraceTxnSQL = fmt.Sprintf(`insert into %s.%s (name, state) values ('%s', '%s')`,
		DebugDB,
		FeaturesTables,
		FeatureTraceTxn,
		StateDisable)

	InitFeatureTraceTxnActionSQL = fmt.Sprintf(`insert into %s.%s (name, state) values ('%s', '%s')`,
		DebugDB,
		FeaturesTables,
		FeatureTraceTxnAction,
		StateDisable)

	InitFeatureTraceDataSQL = fmt.Sprintf(`insert into %s.%s (name, state) values ('%s', '%s')`,
		DebugDB,
		FeaturesTables,
		FeatureTraceData,
		StateDisable)

	InitFeatureTraceStatementSQL = fmt.Sprintf(`insert into %s.%s (name, state) values ('%s', '%s')`,
		DebugDB,
		FeaturesTables,
		FeatureTraceStatement,
		StateDisable)

	InitFeatureTraceTxnWorkspaceSQL = fmt.Sprintf(`insert into %s.%s (name, state) values ('%s', '%s')`,
		DebugDB,
		FeaturesTables,
		FeatureTraceTxnWorkspace,
		StateDisable)
)

var (
	InitSQLs = []string{
		fmt.Sprintf("create database %s", DebugDB),
		EventTxnTableSQL,
		EventDataTableSQL,
		TraceTableFilterTableSQL,
		TraceTxnFilterTableSQL,
		TraceStatementFilterTableSQL,
		EventErrorTableSQL,
		TraceStatementTableSQL,
		EventTxnActionTableSQL,
		FeaturesTablesSQL,
		InitFeatureTraceTxnSQL,
		InitFeatureTraceTxnActionSQL,
		InitFeatureTraceDataSQL,
		InitFeatureTraceStatementSQL,
		InitFeatureTraceTxnWorkspaceSQL,
	}
)
