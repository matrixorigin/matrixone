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

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

var (
	DebugDB               = "mo_debug"
	TxnTable              = "txn_event"
	TxnEntryTable         = "txn_entry_event"
	TraceEntryFilterTable = "trace_entry_filter"

	InitSQLs = []string{
		fmt.Sprintf("create database %s", DebugDB),

		fmt.Sprintf(`create table %s.%s(
			ts 			          bigint       not null,
			txn_id                varchar(50)  not null,
			cn                    varchar(100) not null,
			event_type            varchar(50)  not null,
			txn_status			  varchar(10),
			snapshot_ts           varchar(50),
			commit_ts             varchar(50)
		)`, DebugDB, TxnTable),

		fmt.Sprintf(`create table %s.%s(
			ts 			          bigint       not null,
			cn                    varchar(100) not null,
			event_type            varchar(50)  not null,
			entry_type			  varchar(50)  not null,
			table_id 	          bigint UNSIGNED not null,
			txn_id                varchar(50),
			row_data              varchar(500) not null, 
			committed_ts          varchar(50),
			snapshot_ts           varchar(50)
		)`, DebugDB, TxnEntryTable),

		fmt.Sprintf(`create table %s.%s(
			id                    bigint UNSIGNED primary key auto_increment,
			table_id			  bigint UNSIGNED not null,
			table_name            varchar(50)     not null,
			columns               varchar(200)
		)`, DebugDB, TraceEntryFilterTable),
	}
)

func GetService() Service {
	v, ok := runtime.ProcessLevelRuntime().GetGlobalVariables(runtime.TxnTraceService)
	if !ok {
		panic("txn trace service not found")
	}
	return v.(Service)
}

type Service interface {
	TxnCreated(op client.TxnOperator)
	CommitEntries(txnID []byte, entries []*api.Entry)
	ApplyLogtail(logtail *api.Entry, commitTSIndex int)
	TxnRead(txnID []byte, snapshotTS timestamp.Timestamp, tableID uint64, columns []string, bat *batch.Batch)

	Enable() error
	Disable()

	AddEntryFilter(name string, columns []string) error
	RefreshFilters() error
	ClearFilters() error

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
