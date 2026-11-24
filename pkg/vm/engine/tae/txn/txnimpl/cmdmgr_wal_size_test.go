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

package txnimpl

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/wal"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/require"
)

type recordingWALStore struct {
	wal.Store
	appends         int
	payload         []byte
	maxRecordEntry  int
	lastRecordEntry int
}

func (w *recordingWALStore) AppendEntry(_ uint32, e entry.Entry) (uint64, error) {
	w.appends++
	w.payload = append(w.payload[:0], e.GetPayload()...)
	return 1, nil
}

func (w *recordingWALStore) ValidateUserTxnRecordSize(size int) error {
	w.lastRecordEntry = size
	if w.maxRecordEntry > 0 && size > w.maxRecordEntry {
		return wal.NewRecordTooLargeError(size, w.maxRecordEntry)
	}
	return nil
}

func encodedUserTxnEntrySize(t *testing.T, payload []byte) int {
	t.Helper()
	e := entry.GetBase()
	info := &entry.Info{Group: wal.GroupUserTxn}
	e.SetInfo(info)
	e.SetType(IOET_WALEntry_TxnRecord)
	require.NoError(t, e.SetPayload(payload))
	infoBuf, err := info.Marshal()
	require.NoError(t, err)
	e.SetInfoBuf(infoBuf)
	size := e.TotalSize() + 8 // driver entry's DSN
	e.Free()
	return size
}

func TestCommandManagerRejectsExactWALSizeBeforeAppend(t *testing.T) {
	w := &recordingWALStore{}
	mgr := newCommandManager(w)
	txn := txnbase.NewTxn(nil, &txnbase.NoopTxnStore{}, []byte("wal-size"), types.BuildTS(1, 0), types.TS{})
	store := &txnStore{txn: txn}
	mgr.AddCmd(txnbase.NewTxnStateCmd("state", 0, types.TS{}))
	mgr.cmd.SetTxn(txn)
	encoded, err := mgr.cmd.MarshalBinary()
	require.NoError(t, err)
	require.Positive(t, len(encoded))

	mgr.maxWalSize = len(encoded) - 1
	_, err = mgr.ApplyTxnRecord(store)
	var tooLarge *wal.RecordTooLargeError
	require.ErrorAs(t, err, &tooLarge)
	require.Zero(t, w.appends)

	mgr.maxWalSize = len(encoded)
	e, err := mgr.ApplyTxnRecord(store)
	require.NoError(t, err)
	require.Equal(t, 1, w.appends)
	require.Equal(t, encoded, w.payload)
	e.DoneWithErr(nil)
	e.Free()
}

func TestCommandManagerChecksConfiguredRecordLimitBeforeAppend(t *testing.T) {
	w := &recordingWALStore{}
	mgr := newCommandManager(w)
	txn := txnbase.NewTxn(nil, &txnbase.NoopTxnStore{}, []byte("wal-size"), types.BuildTS(1, 0), types.TS{})
	store := &txnStore{txn: txn}
	mgr.AddCmd(txnbase.NewTxnStateCmd("state", 0, types.TS{}))
	mgr.cmd.SetTxn(txn)
	payload, err := mgr.cmd.MarshalBinary()
	require.NoError(t, err)
	encodedSize := encodedUserTxnEntrySize(t, payload)
	w.maxRecordEntry = encodedSize - 1

	err = mgr.PreflightTxnRecord(store)
	var tooLarge *wal.RecordTooLargeError
	require.ErrorAs(t, err, &tooLarge)
	require.Equal(t, encodedSize, w.lastRecordEntry)
	require.Zero(t, w.appends)
}

func TestCommandManagerPreflightDoesNotRetainSerializedRecord(t *testing.T) {
	w := &recordingWALStore{}
	mgr := newCommandManager(w)
	txn := txnbase.NewTxn(nil, &txnbase.NoopTxnStore{}, bytes.Repeat([]byte("x"), 8*1024), types.BuildTS(1, 0), types.TS{})
	store := &txnStore{txn: txn}
	mgr.AddCmd(txnbase.NewTxnStateCmd("first", 0, types.TS{}))
	require.NoError(t, mgr.PreflightTxnRecord(store))
	require.Nil(t, mgr.preparedEntry)
	// The queued transaction may still be held after preflight. Its serialized
	// bytes must be released, and the WAL stage must use the final command.
	mgr.AddCmd(txnbase.NewTxnStateCmd("second", 0, types.TS{}))
	want, err := mgr.cmd.MarshalBinary()
	require.NoError(t, err)
	e, err := mgr.ApplyTxnRecord(store)
	require.NoError(t, err)
	require.Equal(t, want, w.payload)
	require.Equal(t, 1, w.appends)
	e.DoneWithErr(nil)
	e.Free()
}

func TestCommandManagerRetainsOnlySmallPreflightRecord(t *testing.T) {
	w := &recordingWALStore{}
	mgr := newCommandManager(w)
	txn := txnbase.NewTxn(nil, &txnbase.NoopTxnStore{}, []byte("small"), types.BuildTS(1, 0), types.TS{})
	store := &txnStore{txn: txn}
	mgr.AddCmd(txnbase.NewTxnStateCmd("state", 0, types.TS{}))
	require.NoError(t, mgr.PreflightTxnRecord(store))
	require.NotNil(t, mgr.preparedEntry)
	require.LessOrEqual(t, mgr.preparedSize, maxRetainedPreflightRecordSize)
	e, err := mgr.ApplyTxnRecord(store)
	require.NoError(t, err)
	require.Nil(t, mgr.preparedEntry)
	require.Equal(t, 1, w.appends)
	e.DoneWithErr(nil)
	e.Free()
}

func TestStoreCloseDiscardsUnconsumedPreflightRecord(t *testing.T) {
	w := &recordingWALStore{}
	mgr := newCommandManager(w)
	txn := txnbase.NewTxn(nil, &txnbase.NoopTxnStore{}, []byte("rollback"), types.BuildTS(1, 0), types.TS{})
	store := &txnStore{txn: txn, cmdMgr: mgr}
	mgr.AddCmd(txnbase.NewTxnStateCmd("state", 0, types.TS{}))
	require.NoError(t, mgr.PreflightTxnRecord(store))
	require.NotNil(t, mgr.preparedEntry)
	require.NoError(t, store.Close())
	require.Nil(t, mgr.preparedEntry)
}
