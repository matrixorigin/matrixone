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

	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
)

var (
	// cannot import catalog, because cycle import

	completedPKColumnName = "__mo_cpkey_col"
	rowIDColumn           = "__mo_rowid"
	deletePKColumn        = "pk"

	txnCreateEvent         = "created"
	txnActiveEvent         = "active"
	txnClosedEvent         = "closed"
	txnUpdateSnapshotEvent = "update-snapshot"
	txnCheckChangedEvent   = "check-changed"

	entryApplyEvent  = "apply"
	entryCommitEvent = "commit"
	entryReadEvent   = "read"
)

func isCompletedPK(name string) bool {
	return name == completedPKColumnName
}

func isDeletePKColumn(name string) bool {
	return name == deletePKColumn
}

type txnEvent struct {
	ts         int64
	eventType  string
	txnID      []byte
	txnStatus  string
	snapshotTS timestamp.Timestamp
	commitTS   timestamp.Timestamp

	from, to timestamp.Timestamp
	changed  bool
}

func newTxnCreated(txn txn.TxnMeta) txnEvent {
	return newTxnEvent(txn, txnCreateEvent)
}

func newTxnActive(txn txn.TxnMeta) txnEvent {
	return newTxnEvent(txn, txnActiveEvent)
}

func newTxnClosed(txn txn.TxnMeta) txnEvent {
	return newTxnEvent(txn, txnClosedEvent)
}

func newTxnSnapshotUpdated(txn txn.TxnMeta) txnEvent {
	return newTxnEvent(txn, txnUpdateSnapshotEvent)
}

func newTxnCheckChanged(
	txnID []byte,
	from, to timestamp.Timestamp,
	changed bool) txnEvent {
	return txnEvent{
		ts:        time.Now().UnixNano(),
		eventType: txnCheckChangedEvent,
		txnID:     txnID,
		from:      from,
		to:        to,
		changed:   changed,
	}
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
							snapshot_ts,
							commit_ts,
							check_changed) values (%d, '%s', '%s', '%x', '%s', '%d-%d', '%d-%d', '[%d-%d, %d-%d, %v]')`,
		TxnTable,
		e.ts,
		cn,
		e.eventType,
		e.txnID,
		e.txnStatus,
		e.snapshotTS.PhysicalTime,
		e.snapshotTS.LogicalTime,
		e.commitTS.PhysicalTime,
		e.commitTS.LogicalTime,
		e.from.PhysicalTime,
		e.from.LogicalTime,
		e.to.PhysicalTime,
		e.to.LogicalTime,
		e.changed,
	)
}

type entryEvent struct {
	ts         int64
	eventType  string
	entryType  api.Entry_EntryType
	tableID    uint64
	txnID      []byte
	row        []byte
	commitTS   timestamp.Timestamp
	snapshotTS timestamp.Timestamp
}

func newApplyLogtailEvent(
	ts int64,
	tableID uint64,
	entryType api.Entry_EntryType,
	row []byte,
	commitTS timestamp.Timestamp) entryEvent {
	return entryEvent{
		ts:        ts,
		tableID:   tableID,
		entryType: entryType,
		row:       row,
		commitTS:  commitTS,
		eventType: entryApplyEvent,
	}
}

func newCommitEntryEvent(
	ts int64,
	txnID []byte,
	tableID uint64,
	entryType api.Entry_EntryType,
	row []byte) entryEvent {
	return entryEvent{
		ts:        ts,
		tableID:   tableID,
		entryType: entryType,
		txnID:     txnID,
		row:       row,
		eventType: entryCommitEvent,
	}
}

func newReadEntryEvent(
	ts int64,
	txnID []byte,
	tableID uint64,
	entryType api.Entry_EntryType,
	row []byte,
	snapshotTS timestamp.Timestamp) entryEvent {
	return entryEvent{
		ts:         ts,
		tableID:    tableID,
		entryType:  entryType,
		txnID:      txnID,
		row:        row,
		eventType:  entryReadEvent,
		snapshotTS: snapshotTS,
	}
}

func (e entryEvent) toSQL(cn string) string {
	return fmt.Sprintf(`insert into %s (
							ts, 
							cn, 
							event_type,
							entry_type,
							table_id,
							txn_id,  
							row_data,
							committed_ts,
							snapshot_ts) values (%d, '%s', '%s', '%s', %d, '%x', '%s', '%d-%d', '%d-%d')`,
		TxnEntryTable,
		e.ts,
		cn,
		e.eventType,
		e.entryType,
		e.tableID,
		e.txnID,
		e.row,
		e.commitTS.PhysicalTime,
		e.commitTS.LogicalTime,
		e.snapshotTS.PhysicalTime,
		e.snapshotTS.LogicalTime,
	)
}
