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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
)

var (
	// cannot import catalog, because cycle import

	complexPKColumnName = "__mo_cpkey_col"
	rowIDColumn         = "__mo_rowid"
	deletePKColumn      = "pk"
	disableColumns      = map[string]struct{}{
		"object_stats":        {},
		"__mo_%1_commit_time": {},
		"%!%mo__meta_loc":     {},
		"delta_loc":           {},
		"segment_id":          {},
		"trunc_pointt":        {},
	}

	txnCreateEvent               = "created"
	txnActiveEvent               = "active"
	txnClosedEvent               = "closed"
	txnUpdateSnapshotEvent       = "update-snapshot"
	txnExecuteEvent              = "execute"
	txnUpdateSnapshotReasonEvent = "update-snapshot-reason"
	txnNoConflictChanged         = "no-conflict-changed"
	txnConflictChanged           = "conflict-changed"

	entryApplyEvent         = "apply"
	entryCommitEvent        = "commit"
	entryReadEvent          = "read"
	entryReadBlockEvent     = "read-block"
	entryApplyFlushEvent    = "apply-flush"
	entryTransferRowIDEvent = "transfer-row-id"
	entryDeleteObjectEvent  = "apply-delete-object"
)

func isComplexColumn(name string) bool {
	return name == complexPKColumnName
}

func isRowIDColumn(name string) bool {
	return name == rowIDColumn
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
	info       string
}

func newTxnInfoEvent(
	txn txn.TxnMeta,
	eventType string,
	info string) txnEvent {
	e := newTxnEvent(txn, eventType)
	e.info = info
	return e
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

func (e txnEvent) toCSVRecord(
	cn string,
	buf *buffer,
	records []string) {
	records[0] = buf.writeInt(e.ts)
	records[1] = buf.writeHex(e.txnID)
	records[2] = cn
	records[3] = e.eventType
	records[4] = e.txnStatus
	records[5] = buf.writeTimestamp(e.snapshotTS)
	records[6] = buf.writeTimestamp(e.commitTS)
	records[7] = e.info
}

type dataEvent struct {
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
	commitTS timestamp.Timestamp) dataEvent {
	return dataEvent{
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
	row []byte) dataEvent {
	return dataEvent{
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
	snapshotTS timestamp.Timestamp) dataEvent {
	return dataEvent{
		ts:         ts,
		tableID:    tableID,
		entryType:  entryType,
		txnID:      txnID,
		row:        row,
		eventType:  entryReadEvent,
		snapshotTS: snapshotTS,
	}
}

func newFlushEvent(
	ts int64,
	txnID []byte,
	tableID uint64,
	row []byte) dataEvent {
	return dataEvent{
		ts:        ts,
		txnID:     txnID,
		tableID:   tableID,
		row:       row,
		eventType: entryApplyFlushEvent,
	}
}

func newReadBlockEvent(
	ts int64,
	txnID []byte,
	tableID uint64,
	row []byte) dataEvent {
	return dataEvent{
		ts:        ts,
		txnID:     txnID,
		tableID:   tableID,
		row:       row,
		eventType: entryReadBlockEvent,
	}
}

func newTransferEvent(
	ts int64,
	txnID []byte,
	tableID uint64,
	row []byte) dataEvent {
	return dataEvent{
		ts:        ts,
		txnID:     txnID,
		tableID:   tableID,
		row:       row,
		eventType: entryTransferRowIDEvent,
	}
}

func newDeleteObjectEvent(
	ts int64,
	tableID uint64,
	row []byte) dataEvent {
	return dataEvent{
		ts:        ts,
		tableID:   tableID,
		row:       row,
		eventType: entryDeleteObjectEvent,
	}
}

func (e dataEvent) toCSVRecord(
	cn string,
	buf *buffer,
	records []string) {
	records[0] = buf.writeInt(e.ts)
	records[1] = cn
	records[2] = e.eventType
	records[3] = e.entryType.String()
	records[4] = buf.writeUint(e.tableID)
	records[5] = buf.writeHex(e.txnID)
	records[6] = util.UnsafeBytesToString(e.row)
	records[7] = buf.writeTimestamp(e.commitTS)
	records[8] = buf.writeTimestamp(e.snapshotTS)
}

type actionEvent struct {
	ts        int64
	txnID     []byte
	action    string
	tableID   uint64
	actionSeq uint64
	value     int64
	unit      string
	err       string
}

func (e actionEvent) toCSVRecord(
	cn string,
	buf *buffer,
	records []string) {
	records[0] = buf.writeInt(e.ts)
	records[1] = buf.writeHex(e.txnID)
	records[2] = cn
	records[3] = buf.writeUint(e.tableID)
	records[4] = e.action
	records[5] = buf.writeUint(e.actionSeq)
	records[6] = buf.writeInt(e.value)
	records[7] = e.unit
	if len(e.err) > 100 {
		e.err = e.err[:100]
	}
	records[8] = e.err
}

type statement struct {
	ts    int64
	txnID []byte
	sql   string
	cost  int64
}

func newStatement(
	txnID []byte,
	sql string,
	cost time.Duration) statement {
	return statement{
		ts:    time.Now().UnixNano(),
		txnID: txnID,
		sql:   sql,
		cost:  cost.Microseconds(),
	}
}

func (e statement) toCSVRecord(
	cn string,
	buf *buffer,
	records []string) {
	records[0] = buf.writeInt(e.ts)
	records[1] = buf.writeHex(e.txnID)
	records[2] = e.sql
	records[3] = buf.writeInt(e.cost)
}

func truncateSQL(sql string) string {
	if len(sql) > 1000 {
		return sql[:1000]
	}
	return sql
}
