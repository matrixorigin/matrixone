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
							snapshot_ts,
							commit_ts) values (%d, '%s', '%s', '%x', '%s', '%d-%d', '%d-%d')`,
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
	)
}

type entryEvent struct {
	ts         int64
	eventType  string
	entryType  string
	tableID    uint64
	txnID      []byte
	row        []byte
	commitTS   timestamp.Timestamp
	snapshotTS timestamp.Timestamp
}

func newApplyLogtailEvent(
	ts int64,
	tableID uint64,
	entryType string,
	row []byte,
	commitTS timestamp.Timestamp) entryEvent {
	return entryEvent{
		ts:        ts,
		tableID:   tableID,
		entryType: entryType,
		row:       row,
		commitTS:  commitTS,
		eventType: "apply",
	}
}

func newCommitEntryEvent(
	ts int64,
	txnID []byte,
	tableID uint64,
	entryType string,
	row []byte) entryEvent {
	return entryEvent{
		ts:        ts,
		tableID:   tableID,
		entryType: entryType,
		txnID:     txnID,
		row:       row,
		eventType: "commit",
	}
}

func newReadEntryEvent(
	ts int64,
	txnID []byte,
	tableID uint64,
	entryType string,
	row []byte,
	snapshotTS timestamp.Timestamp) entryEvent {
	return entryEvent{
		ts:         ts,
		tableID:    tableID,
		entryType:  entryType,
		txnID:      txnID,
		row:        row,
		eventType:  "read",
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
