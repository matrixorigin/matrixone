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

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
)

var (
	db            = "mo_debug"
	txnTable      = "txn_event"
	txnEntryTable = "txn_entry_event"

	tables = []string{
		fmt.Sprintf(`create table %s.%s(
			ts 			          bigint       not null,
			txn_id                varchar(50)  not null,
			cn                    varchar(100) not null,
			event_type            varchar(10)  not null,
			txn_status			  varchar(10),
			snapshot_ts_physical  bigint,
			snapshot_ts_logical   int,
			commit_ts_physical    bigint,
			commit_ts_logical     int,
			primary key(ts, txn_id)
		);`, db, txnTable),

		fmt.Sprintf(`create table %s.%s(
			ts 			          bigint       not null,
			cn                    varchar(100) not null
			event_type            varchar(10)  not null,
			table_id 	          bigint       not null,
			txn_id                varchar(50),
			row_data              varchar(500) not null,
			committed_ts_physical bigint,
			committed_ts_logical  int,
			primary key(ts, cn)
		);`, db, txnEntryTable),
	}
)

type txnEvent struct {
	ts         int64
	eventType  string
	txnID      []byte
	txnStatus  string
	snapshotTS timestamp.Timestamp
	commitTS   timestamp.Timestamp
}

func newTxnCreated(txn txn.TxnMeta) txnEvent {
	return newTxnEvent(txn, "created")
}

func newTxnActive(txn txn.TxnMeta) txnEvent {
	return newTxnEvent(txn, "active")
}

func newTxnClosed(txn txn.TxnMeta) txnEvent {
	return newTxnEvent(txn, "closed")
}

func newTxnSnapshotUpdated(txn txn.TxnMeta) txnEvent {
	return newTxnEvent(txn, "snapshot-updated")
}

func newTxnEvent(
	txn txn.TxnMeta,
	event string) txnEvent {
	return txnEvent{
		ts:         time.Now().UnixNano(),
		eventType:  event,
		txnID:      txn.ID,
		snapshotTS: txn.SnapshotTS,
		commitTS:   txn.CommitTS,
		txnStatus:  txn.Status.String(),
	}
}

func (e txnEvent) toSQL(cn string) string {
	return fmt.Sprintf(`insert into %s (
							ts, 
							cn, 
							event_type,
							txn_id, 
							txn_status, 
							snapshot_ts_physical, 
							snapshot_ts_logical, 
							commit_ts_physical, 
							commit_ts_logical, 
							statement) values (%d, %s, %s, %x, %s, %d, %d, %d, %d, %s)`,
		txnTable,
		e.ts,
		cn,
		e.eventType,
		e.txnID,
		e.txnStatus,
		e.snapshotTS.PhysicalTime,
		e.snapshotTS.LogicalTime,
		e.commitTS.PhysicalTime,
		e.commitTS.LogicalTime,
		"",
	)
}

type entryEvent struct {
	ts          int64
	eventType   string
	tableID     uint64
	txnID       []byte
	row         []byte
	committedAt timestamp.Timestamp
}

func newApplyLogtailEvent(
	ts int64,
	tableID uint64,
	row []byte,
	committedAt timestamp.Timestamp) entryEvent {
	return entryEvent{
		ts:          ts,
		tableID:     tableID,
		row:         row,
		committedAt: committedAt,
		eventType:   "apply",
	}
}

func newCommitEntryEvent(
	ts int64,
	txnID []byte,
	tableID uint64,
	row []byte) entryEvent {
	return entryEvent{
		ts:        ts,
		tableID:   tableID,
		txnID:     txnID,
		row:       row,
		eventType: "commit",
	}
}

func (e entryEvent) toSQL(cn string) string {
	return fmt.Sprintf(`insert into %s (
							ts, 
							cn, 
							event_type,
							table_id,
							txn_id,  
							row_data,
							committed_ts_physical, 
							committed_ts_logical) values (%d, %s, %s, %d, %x, %s, %d, %d)`,
		txnEntryTable,
		e.ts,
		cn,
		e.eventType,
		e.tableID,
		e.txnID,
		e.row,
		e.committedAt.PhysicalTime,
		e.committedAt.LogicalTime,
	)
}
