// Copyright 2021 - 2022 Matrix Origin
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

package mem

import (
	"context"
	"math"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/stretchr/testify/require"
)

type closeTrackingLogClient struct {
	logservice.Client
	closed atomic.Int32
}

func (c *closeTrackingLogClient) Close() error {
	c.closed.Add(1)
	return c.Client.Close()
}

func TestCloseClosesLogClientOnce(t *testing.T) {
	client := &closeTrackingLogClient{Client: NewMemLog()}
	storage := NewKVTxnStorage(0, client, newTestClock(1))

	require.NoError(t, storage.Close(context.Background()))
	require.NoError(t, storage.Destroy(context.Background()))
	require.Equal(t, int32(1), client.closed.Load())
}

func TestWriteConflict(t *testing.T) {
	storage := NewKVTxnStorage(0, NewMemLog(), newTestClock(1))
	first := newTxnMeta(1, 1)
	second := newTxnMeta(2, 1)

	require.NoError(t, writeValue(storage, first, "key", "first"))
	err := writeValue(storage, second, "key", "second")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWriteConflict), err)
}

func TestSingleTNCommitAndRecovery(t *testing.T) {
	logClient := NewMemLog()
	storage := NewKVTxnStorage(0, logClient, newTestClock(1))
	meta := newTxnMeta(1, 1)
	require.NoError(t, writeValue(storage, meta, "key", "value"))

	commitTS, err := storage.Commit(context.Background(), meta, nil, nil)
	require.NoError(t, err)
	require.False(t, commitTS.IsEmpty())
	require.Equal(t, "value", readValue(t, storage, "key", commitTS.Next()))

	recovered := NewKVTxnStorage(1, logClient, newTestClock(100))
	require.NoError(t, recovered.Start())
	require.Equal(t, "value", readValue(t, recovered, "key", commitTS.Next()))
}

func TestRollbackRemovesUncommittedValue(t *testing.T) {
	storage := NewKVTxnStorage(0, NewMemLog(), newTestClock(1))
	meta := newTxnMeta(1, 1)
	require.NoError(t, writeValue(storage, meta, "key", "value"))
	require.NoError(t, storage.Rollback(context.Background(), meta))

	require.Nil(t, storage.GetUncommittedTxn(meta.ID))
	_, ok := storage.GetUncommittedKV().Get([]byte("key"))
	require.False(t, ok)
}

func TestCommitAndRollbackEvents(t *testing.T) {
	storage := NewKVTxnStorage(0, NewMemLog(), newTestClock(1))
	committed := newTxnMeta(1, 1)
	require.NoError(t, writeValue(storage, committed, "commit", "value"))
	_, err := storage.Commit(context.Background(), committed, nil, nil)
	require.NoError(t, err)
	require.Equal(t, CommitType, (<-storage.GetEventC()).Type)

	rolledBack := newTxnMeta(2, 2)
	require.NoError(t, writeValue(storage, rolledBack, "rollback", "value"))
	require.NoError(t, storage.Rollback(context.Background(), rolledBack))
	require.Equal(t, RollbackType, (<-storage.GetEventC()).Type)
}

func writeValue(storage *KVTxnStorage, meta txn.TxnMeta, key, value string) error {
	request := &message{
		Keys:   [][]byte{[]byte(key)},
		Values: [][]byte{[]byte(value)},
	}
	_, err := storage.Write(context.Background(), meta, setOpCode, request.mustMarshal())
	return err
}

func readValue(
	t *testing.T,
	storage *KVTxnStorage,
	key string,
	snapshot timestamp.Timestamp,
) string {
	t.Helper()
	request := &message{Keys: [][]byte{[]byte(key)}}
	result, err := storage.Read(
		context.Background(),
		txn.TxnMeta{ID: []byte("reader"), SnapshotTS: snapshot},
		getOpCode,
		request.mustMarshal(),
	)
	require.NoError(t, err)
	defer result.Release()
	data, err := result.Read()
	require.NoError(t, err)
	response := new(message)
	response.mustUnmarshal(data)
	require.Len(t, response.Values, 1)
	return string(response.Values[0])
}

func newTxnMeta(id byte, snapshot int64) txn.TxnMeta {
	return txn.TxnMeta{
		ID:         []byte{id},
		Status:     txn.TxnStatus_Active,
		SnapshotTS: timestamp.Timestamp{PhysicalTime: snapshot},
	}
}

func newTestClock(start int64) clock.Clock {
	ts := start
	return clock.NewHLCClock(func() int64 {
		return atomic.AddInt64(&ts, 1)
	}, math.MaxInt64)
}
