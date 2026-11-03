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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/wal"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/require"
)

type recordingWALStore struct {
	wal.Store
	appends int
	payload []byte
}

func (w *recordingWALStore) AppendEntry(_ uint32, e entry.Entry) (uint64, error) {
	w.appends++
	w.payload = append(w.payload[:0], e.GetPayload()...)
	return 1, nil
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
