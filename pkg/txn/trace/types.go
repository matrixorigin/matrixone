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

package trace

import (
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

var (
	DebugDB                   = "mo_debug"
	featuresTables            = "trace_features"
	traceTableFilterTable     = "trace_table_filters"
	traceTxnFilterTable       = "trace_txn_filters"
	traceStatementFilterTable = "trace_statement_filters"
	traceStatementTable       = "trace_statement"
	eventTxnTable             = "trace_event_txn"
	eventDataTable            = "trace_event_data"
	eventErrorTable           = "trace_event_error"
	eventTxnActionTable       = "trace_event_txn_action"

	FeatureTraceStatement    = "statement"
	FeatureTraceTxn          = "txn"
	FeatureTraceTxnWorkspace = "txn-workspace"
	FeatureTraceTxnAction    = "txn-action"
	FeatureTraceData         = "data"
	stateEnable              = "enable"
	stateDisable             = "disable"

	InitSQLs = []string{
		fmt.Sprintf("create database %s", DebugDB),

		fmt.Sprintf(`create table %s.%s(
			ts 			          bigint       not null,
			txn_id                varchar(50)  not null,
			cn                    varchar(100) not null,
			event_type            varchar(50)  not null,
			txn_status			  varchar(10),
			snapshot_ts           varchar(50),
			commit_ts             varchar(50),
			info                  varchar(1000)
		)`, DebugDB, eventTxnTable),

		fmt.Sprintf(`create table %s.%s(
			ts 			          bigint          not null,
			cn                    varchar(100)    not null,
			event_type            varchar(50)     not null,
			entry_type			  varchar(50)     not null,
			table_id 	          bigint UNSIGNED not null,
			txn_id                varchar(50),
			row_data              varchar(500)    not null, 
			committed_ts          varchar(50),
			snapshot_ts           varchar(50)
		)`, DebugDB, eventDataTable),

		fmt.Sprintf(`create table %s.%s(
			id                    bigint UNSIGNED primary key auto_increment,
			table_id			  bigint UNSIGNED not null,
			table_name            varchar(50)     not null,
			columns               varchar(200)
		)`, DebugDB, traceTableFilterTable),

		fmt.Sprintf(`create table %s.%s(
			id             bigint UNSIGNED primary key auto_increment,
			method         varchar(50)     not null,
			value          varchar(500)    not null
		)`, DebugDB, traceTxnFilterTable),

		fmt.Sprintf(`create table %s.%s(
			id             bigint UNSIGNED primary key auto_increment,
			method         varchar(50)     not null,
			value          varchar(500)    not null
		)`, DebugDB, traceStatementFilterTable),

		fmt.Sprintf(`create table %s.%s(
			ts 			          bigint          not null,
			txn_id                varchar(50)     not null,
			error_info            varchar(1000)   not null
		)`, DebugDB, eventErrorTable),

		fmt.Sprintf(`create table %s.%s(
			ts 			   bigint          not null,
			txn_id         varchar(50)     not null,
			sql            varchar(1000)   not null,
			cost_us        bigint          not null
		)`, DebugDB, traceStatementTable),

		fmt.Sprintf(`create table %s.%s(
			ts 			          bigint          not null,
			txn_id                varchar(50)     not null,
			cn                    varchar(50)     not null,
			table_id              bigint UNSIGNED,
			action                varchar(100)    not null,
			action_sequence       bigint UNSIGNED not null,
			value                 bigint,
			unit                  varchar(10),
			err                   varchar(100) 
		)`, DebugDB, eventTxnActionTable),

		fmt.Sprintf(`create table %s.%s(
			name    varchar(50) not null primary key,
			state   varchar(20) not null
		)`, DebugDB, featuresTables),

		fmt.Sprintf(`insert into %s.%s (name, state) values ('%s', '%s')`,
			DebugDB,
			featuresTables,
			FeatureTraceTxn,
			stateDisable),

		fmt.Sprintf(`insert into %s.%s (name, state) values ('%s', '%s')`,
			DebugDB,
			featuresTables,
			FeatureTraceTxnAction,
			stateDisable),

		fmt.Sprintf(`insert into %s.%s (name, state) values ('%s', '%s')`,
			DebugDB,
			featuresTables,
			FeatureTraceData,
			stateDisable),

		fmt.Sprintf(`insert into %s.%s (name, state) values ('%s', '%s')`,
			DebugDB,
			featuresTables,
			FeatureTraceStatement,
			stateDisable),

		fmt.Sprintf(`insert into %s.%s (name, state) values ('%s', '%s')`,
			DebugDB,
			featuresTables,
			FeatureTraceTxnWorkspace,
			stateDisable),
	}
)

func GetService() Service {
	v, ok := runtime.ProcessLevelRuntime().GetGlobalVariables(runtime.TxnTraceService)
	if !ok {
		return &service{}
	}
	return v.(Service)
}

type txnEventService interface {
	TxnCreated(op client.TxnOperator)
	TxnNoConflictChanged(op client.TxnOperator, tableID uint64, lockedAt, newSnapshotTS timestamp.Timestamp)
	TxnConflictChanged(op client.TxnOperator, tableID uint64, lastCommitAt timestamp.Timestamp)
	TxnUpdateSnapshot(op client.TxnOperator, tableID uint64, why string)
	TxnCommit(op client.TxnOperator, entries []*api.Entry)
	TxnRead(op client.TxnOperator, snapshotTS timestamp.Timestamp, tableID uint64, columns []string, bat *batch.Batch)
	TxnReadBlock(op client.TxnOperator, tableID uint64, block []byte)
	TxnWrite(op client.TxnOperator, tableID uint64, typ string, bat *batch.Batch)
	TxnAdjustWorkspace(op client.TxnOperator, index int, writes func() (tableID uint64, typ string, bat *batch.Batch, more bool))
	TxnError(op client.TxnOperator, err error)

	TxnStatementStart(op client.TxnOperator, sql string, seq uint64)
	TxnStatementCompleted(op client.TxnOperator, sql string, cost time.Duration, seq uint64, err error)

	AddTxnDurationAction(op client.TxnOperator, eventType client.EventType, seq uint64, tableID uint64, value time.Duration, err error)
	AddTxnAction(op client.TxnOperator, eventType client.EventType, seq uint64, tableID uint64, value int64, unit string, err error)

	AddTxnFilter(method, value string) error
	ClearTxnFilters() error
	RefreshTxnFilters() error
}

type dataEventService interface {
	ApplyLogtail(logtail *api.Entry, commitTSIndex int)
	ApplyFlush(txnID []byte, tableID uint64, from, to timestamp.Timestamp, count int)
	ApplyTransferRowID(txnID []byte, tableID uint64, fromRowID, toRowID, fromBlockID, toBlockID []byte, vec *vector.Vector, row int)
	ApplyDeleteObject(tableID uint64, ts timestamp.Timestamp, objName string, tag string)

	AddTableFilter(name string, columns []string) error
	ClearTableFilters() error
	RefreshTableFilters() error
}

type statementService interface {
	AddStatement(op client.TxnOperator, statement string, cost time.Duration)
	AddStatementFilter(method, value string) error
	ClearStatementFilters() error
	RefreshStatementFilters() error
}

type Service interface {
	txnEventService
	dataEventService
	statementService

	EnableFlush()
	Enable(feature string) error
	Disable(feature string) error
	Enabled(feature string) bool
	Sync()

	DecodeHexComplexPK(hex string) (string, error)

	Close()
}

// Option options to create trace service
type Option func(*service)

// EntryFilter entry filter to hold the entries we care about, to reduce the
// amount size of trace data.
type EntryFilter interface {
	// Filter returns true means the entry should be skipped.
	Filter(entry *EntryData) bool
	Name() string
}

type TxnFilter interface {
	// Filter returns true means the txn should be skipped.
	Filter(op client.TxnOperator) bool
}

type StatementFilter interface {
	// Filter returns true means the txn should be skipped.
	Filter(op client.TxnOperator, sql string, cost time.Duration) bool
}

type csvEvent interface {
	toCSVRecord(
		cn string,
		buf *buffer,
		records []string)
}
