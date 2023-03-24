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
	"bytes"
	"context"
	"math"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/stretchr/testify/assert"
)

func TestWrite(t *testing.T) {
	l := NewMemLog()
	s := NewKVTxnStorage(0, l, newTestClock(1))

	wTxn := writeTestData(t, s, 1, moerr.Ok, 1, 2)
	checkUncommitted(t, s, wTxn, 1, 2)
	checkLogCount(t, l, 0)
}

func TestWriteWithConflict(t *testing.T) {
	l := NewMemLog()
	s := NewKVTxnStorage(0, l, newTestClock(1))

	wTxn1 := writeTestData(t, s, 1, moerr.Ok, 1)
	checkUncommitted(t, s, wTxn1, 1)

	writeTestData(t, s, 1, moerr.ErrTxnWriteConflict, 1)

	wTxn3 := writeTestData(t, s, 1, moerr.Ok, 2)
	checkUncommitted(t, s, wTxn3, 2)
}

func TestPrepare(t *testing.T) {
	l := NewMemLog()
	s := NewKVTxnStorage(0, l, newTestClock(1))

	wTxn := writeTestData(t, s, 1, moerr.Ok, 1)

	prepareTestTxn(t, s, &wTxn, 2, moerr.Ok)

	checkUncommitted(t, s, wTxn, 1)
	checkLogCount(t, l, 1)
	checkLog(t, l, 1, wTxn, 1)
}

func TestPrepareWithConflict(t *testing.T) {
	l := NewMemLog()
	s := NewKVTxnStorage(0, l, newTestClock(1))

	writeCommittedData(t, s, 1, 100, 1) // commit at 100

	wTxn := writeTestData(t, s, 1, moerr.Ok, 1)
	prepareTestTxn(t, s, &wTxn, 5, moerr.ErrTxnWriteConflict) // prepare at 5
}

func TestCommit(t *testing.T) {
	l := NewMemLog()
	s := NewKVTxnStorage(0, l, newTestClock(1))

	wTxn := writeTestData(t, s, 1, moerr.Ok, 1)
	commitTestTxn(t, s, &wTxn, 2, moerr.Ok)

	checkCommitted(t, s, wTxn, 1)
	checkLogCount(t, l, 1)
	checkLog(t, l, 1, wTxn, 1)
}

func TestCommitWithTxnNotExist(t *testing.T) {
	l := NewMemLog()
	s := NewKVTxnStorage(0, l, newTestClock(1))

	commitTestTxn(t, s, &txn.TxnMeta{}, 2, moerr.Ok)
}

func TestCommitWithConflict(t *testing.T) {
	l := NewMemLog()
	s := NewKVTxnStorage(0, l, newTestClock(1))

	writeCommittedData(t, s, 1, 2, 1)

	wTxn := writeTestData(t, s, 1, moerr.Ok, 1)
	commitTestTxn(t, s, &wTxn, 5, moerr.ErrTxnWriteConflict)
}

func TestCommitAfterPrepared(t *testing.T) {
	l := NewMemLog()
	s := NewKVTxnStorage(0, l, newTestClock(1))

	wTxn := writeTestData(t, s, 1, moerr.Ok, 1)
	prepareTestTxn(t, s, &wTxn, 2, moerr.Ok)
	commitTestTxn(t, s, &wTxn, 3, moerr.Ok)

	checkCommitted(t, s, wTxn, 1)
	checkLogCount(t, l, 2)
	checkLog(t, l, 2, wTxn)
}

func TestRollback(t *testing.T) {
	l := NewMemLog()
	s := NewKVTxnStorage(0, l, newTestClock(1))

	wTxn := writeTestData(t, s, 1, moerr.Ok, 1)
	checkUncommitted(t, s, wTxn, 1)

	assert.NoError(t, s.Rollback(context.TODO(), wTxn))
	checkRollback(t, s, wTxn, 1)
}

func TestRollbackWithTxnNotExist(t *testing.T) {
	l := NewMemLog()
	s := NewKVTxnStorage(0, l, newTestClock(1))

	assert.NoError(t, s.Rollback(context.TODO(), txn.TxnMeta{}))
	checkRollback(t, s, txn.TxnMeta{})
}

func TestReadCommittedWithLessVersion(t *testing.T) {
	l := NewMemLog()
	s := NewKVTxnStorage(0, l, newTestClock(1))

	writeCommittedData(t, s, 0, 1, 1)

	_, rs := readTestData(t, s, 2, nil, 1)
	checkReadResult(t, 0, rs, 1)
}

func TestReadSelfUncommitted(t *testing.T) {
	l := NewMemLog()
	s := NewKVTxnStorage(0, l, newTestClock(1))

	wTxn := writeTestData(t, s, 1, moerr.Ok, 1)
	rs := readTestDataWithTxn(t, s, &wTxn, nil, 1)
	checkReadResult(t, 1, rs, 1)
}

func TestReadCommittedWithGTVersion(t *testing.T) {
	l := NewMemLog()
	s := NewKVTxnStorage(0, l, newTestClock(1))

	writeCommittedData(t, s, 0, 1, 1)

	_, rs := readTestData(t, s, 1, nil, 1)
	checkReadResult(t, 0, rs, 0)
}

func TestWaitReadByPreparedTxn(t *testing.T) {
	l := NewMemLog()
	s := NewKVTxnStorage(0, l, newTestClock(1))

	writeCommittedData(t, s, 0, 1, 1)

	wTxn := writeTestData(t, s, 2, moerr.Ok, 1)
	prepareTestTxn(t, s, &wTxn, 5, moerr.Ok)

	readTestData(t, s, 6, [][]byte{wTxn.ID}, 1)
}

func TestReadByGTPreparedTxnCanNotWait(t *testing.T) {
	l := NewMemLog()
	s := NewKVTxnStorage(0, l, newTestClock(1))

	writeCommittedData(t, s, 0, 1, 1)

	wTxn := writeTestData(t, s, 2, moerr.Ok, 1)
	prepareTestTxn(t, s, &wTxn, 5, moerr.Ok)

	readTestData(t, s, 2, nil, 1)
}

func TestWaitReadByCommittingTxn(t *testing.T) {
	l := NewMemLog()
	s := NewKVTxnStorage(0, l, newTestClock(1))

	writeCommittedData(t, s, 0, 1, 1)

	wTxn := writeTestData(t, s, 2, moerr.Ok, 1)
	prepareTestTxn(t, s, &wTxn, 5, moerr.Ok)
	committingTestTxn(t, s, &wTxn, 6)

	readTestData(t, s, 7, [][]byte{wTxn.ID}, 1)
}

func TestReadByGTCommittingTxnCanNotWait(t *testing.T) {
	l := NewMemLog()
	s := NewKVTxnStorage(0, l, newTestClock(1))

	writeCommittedData(t, s, 0, 1, 1)

	wTxn := writeTestData(t, s, 2, moerr.Ok, 1)
	prepareTestTxn(t, s, &wTxn, 3, moerr.Ok)
	committingTestTxn(t, s, &wTxn, 4)

	readTestData(t, s, 3, nil, 1)
}

func TestReadAfterWaitTxnResloved(t *testing.T) {
	l := NewMemLog()
	s := NewKVTxnStorage(0, l, newTestClock(1))

	wTxn1 := writeTestData(t, s, 1, moerr.Ok, 1)
	prepareTestTxn(t, s, &wTxn1, 2, moerr.Ok)

	wTxn2 := writeTestData(t, s, 1, moerr.Ok, 2)
	prepareTestTxn(t, s, &wTxn2, 2, moerr.Ok)

	_, rs := readTestData(t, s, 5, [][]byte{wTxn1.ID, wTxn2.ID}, 1, 2)
	_, err := rs.Read()
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrMissingTxn))

	commitTestTxn(t, s, &wTxn2, 6, moerr.Ok)
	_, err = rs.Read()
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrMissingTxn))

	commitTestTxn(t, s, &wTxn1, 4, moerr.Ok)

	checkReadResult(t, 1, rs, 1, 0)
}

func TestRecovery(t *testing.T) {
	l := NewMemLog()
	s := NewKVTxnStorage(0, l, newTestClock(1))

	prepareTxn := writeTestData(t, s, 1, moerr.Ok, 1)
	prepareTestTxn(t, s, &prepareTxn, 2, moerr.Ok)
	checkLog(t, l, 1, prepareTxn, 1)

	committedTxn := writeTestData(t, s, 1, moerr.Ok, 2)
	commitTestTxn(t, s, &committedTxn, 3, moerr.Ok)
	checkLog(t, l, 2, committedTxn, 2)

	committedAndPreparedTxn := writeTestData(t, s, 1, moerr.Ok, 3)
	prepareTestTxn(t, s, &committedAndPreparedTxn, 2, moerr.Ok)
	checkLog(t, l, 3, committedAndPreparedTxn, 3)
	commitTestTxn(t, s, &committedAndPreparedTxn, 3, moerr.Ok)
	checkLog(t, l, 4, committedAndPreparedTxn)

	committingTxn := writeTestData(t, s, 1, moerr.Ok, 4)
	prepareTestTxn(t, s, &committingTxn, 2, moerr.Ok)
	checkLog(t, l, 5, committingTxn, 4)
	committingTestTxn(t, s, &committingTxn, 3)
	checkLog(t, l, 6, committingTxn)

	checkLogCount(t, l, 6)

	c := make(chan txn.TxnMeta, 10)
	s2 := NewKVTxnStorage(1, l, newTestClock(1))
	s2.StartRecovery(context.TODO(), c)

	checkUncommitted(t, s2, prepareTxn, 1)
	checkCommitted(t, s2, committedTxn, 2)
	checkCommitted(t, s2, committedAndPreparedTxn, 3)
	checkUncommitted(t, s2, committingTxn, 4)

	txns := make([]txn.TxnMeta, 0, 6)
	for v := range c {
		txns = append(txns, v)
	}
	assert.Equal(t, 6, len(txns))
}

func TestEvent(t *testing.T) {
	l := NewMemLog()
	s := NewKVTxnStorage(0, l, newTestClock(1))

	wTxn := writeTestData(t, s, 1, moerr.Ok, 1)
	prepareTestTxn(t, s, &wTxn, 2, moerr.Ok)
	e := <-s.GetEventC()
	assert.Equal(t, e, Event{Type: PrepareType, Txn: wTxn})

	committingTestTxn(t, s, &wTxn, 3)
	e = <-s.GetEventC()
	assert.Equal(t, e, Event{Type: CommittingType, Txn: wTxn})

	commitTestTxn(t, s, &wTxn, 3, moerr.Ok)
	e = <-s.GetEventC()
	assert.Equal(t, e, Event{Type: CommitType, Txn: wTxn})

	wTxn = writeTestData(t, s, 1, moerr.Ok, 2)
	assert.NoError(t, s.Rollback(context.TODO(), wTxn))
	checkRollback(t, s, wTxn, 2)
	e = <-s.GetEventC()
	assert.Equal(t, e, Event{Type: RollbackType, Txn: wTxn})
}

func prepareTestTxn(t *testing.T, s *KVTxnStorage, wTxn *txn.TxnMeta, ts int64, errCode uint16) {
	wTxn.PreparedTS = newTimestamp(ts)
	pts, perr := s.Prepare(context.TODO(), *wTxn)
	assert.True(t, moerr.IsMoErrCode(perr, errCode))
	wTxn.Status = txn.TxnStatus_Prepared
	wTxn.PreparedTS = pts
}

func committingTestTxn(t *testing.T, s *KVTxnStorage, wTxn *txn.TxnMeta, ts int64) {
	wTxn.CommitTS = newTimestamp(ts)
	assert.NoError(t, s.Committing(context.TODO(), *wTxn))
	wTxn.Status = txn.TxnStatus_Committing
}

func commitTestTxn(t *testing.T, s *KVTxnStorage, wTxn *txn.TxnMeta, ts int64, errCode uint16) {
	wTxn.CommitTS = newTimestamp(ts)
	_, e := s.Commit(context.TODO(), *wTxn)
	assert.True(t, moerr.IsMoErrCode(e, errCode))
	wTxn.Status = txn.TxnStatus_Committed
}

func checkLogCount(t *testing.T, ll logservice.Client, expect int) {
	l := ll.(*memLogClient)

	l.RLock()
	defer l.RUnlock()

	assert.Equal(t, expect, len(l.logs))
}

func checkLog(t *testing.T, ll logservice.Client, offset int, wTxn txn.TxnMeta, keys ...byte) {
	l := ll.(*memLogClient)

	l.RLock()
	defer l.RUnlock()

	klog := &KVLog{}
	klog.Txn = wTxn
	for _, k := range keys {
		key := []byte{k}
		value := []byte{k, byte(wTxn.SnapshotTS.PhysicalTime)}

		klog.Keys = append(klog.Keys, key)
		klog.Values = append(klog.Values, value)
	}

	assert.Equal(t, klog.MustMarshal(), l.logs[offset-1].Data)
}

func checkUncommitted(t *testing.T, s *KVTxnStorage, wTxn txn.TxnMeta, keys ...byte) {
	for _, k := range keys {
		key := []byte{k}
		value := []byte{k, byte(wTxn.SnapshotTS.PhysicalTime)}
		uTxn, ok := s.uncommittedKeyTxnMap[string(key)]
		assert.True(t, ok)
		assert.Equal(t, wTxn, *uTxn)

		v, ok := s.uncommitted.Get(key)
		assert.True(t, ok)
		assert.Equal(t, value, v)

		n := 0
		s.committed.AscendRange(key,
			newTimestamp(0),
			newTimestamp(math.MaxInt64),
			func(b []byte, _ timestamp.Timestamp) {
				if bytes.Equal(b, value) {
					n++
				}
			})
		assert.Equal(t, 0, n)
	}
}

func checkRollback(t *testing.T, s *KVTxnStorage, wTxn txn.TxnMeta, keys ...byte) {
	for _, k := range keys {
		key := []byte{k}
		value := []byte{k, byte(wTxn.SnapshotTS.PhysicalTime)}
		uTxn, ok := s.uncommittedKeyTxnMap[string(key)]
		assert.False(t, ok)
		assert.Nil(t, uTxn)

		v, ok := s.uncommitted.Get(key)
		assert.False(t, ok)
		assert.Empty(t, v)

		n := 0
		s.committed.AscendRange(key,
			newTimestamp(0),
			newTimestamp(math.MaxInt64),
			func(b []byte, _ timestamp.Timestamp) {
				if bytes.Equal(b, value) {
					n++
				}
			})
		assert.Equal(t, 0, n)
	}

	uTxn, ok := s.uncommittedTxn[string(wTxn.ID)]
	assert.False(t, ok)
	assert.Nil(t, uTxn)
}

func writeTestData(t *testing.T, s *KVTxnStorage, ts int64, expectError uint16, keys ...byte) txn.TxnMeta {
	req := &message{}
	for _, v := range keys {
		req.Keys = append(req.Keys, []byte{v})
		req.Values = append(req.Values, []byte{v, byte(ts)})
	}
	wTxn := newTxnMeta(ts)
	_, err := s.Write(context.TODO(), wTxn, setOpCode, req.mustMarshal())
	assert.True(t, moerr.IsMoErrCode(err, expectError))
	return wTxn
}

func checkCommitted(t *testing.T, s *KVTxnStorage, wTxn txn.TxnMeta, keys ...byte) {
	for _, k := range keys {
		key := []byte{k}
		value := []byte{k, byte(wTxn.SnapshotTS.PhysicalTime)}

		v, ok := s.uncommittedKeyTxnMap[string(key)]
		assert.False(t, ok)
		assert.Nil(t, v)

		hasCommitted := false
		s.committed.AscendRange(key,
			newTimestamp(0),
			newTimestamp(math.MaxInt64),
			func(b []byte, _ timestamp.Timestamp) {
				if bytes.Equal(value, b) {
					hasCommitted = true
				}
			})
		assert.True(t, hasCommitted)
	}

	v, ok := s.uncommittedTxn[string(wTxn.ID)]
	assert.False(t, ok)
	assert.Nil(t, v)
}

func writeCommittedData(t *testing.T, s *KVTxnStorage, sts, cts int64, keys ...byte) {
	for _, k := range keys {
		key := []byte{k}
		value := []byte{k, byte(sts)}
		s.committed.Set(key, newTimestamp(cts), value)
	}
}

func readTestData(t *testing.T, s *KVTxnStorage, ts int64, waitTxns [][]byte, keys ...byte) (txn.TxnMeta, *readResult) {
	rTxn := newTxnMeta(ts)
	return rTxn, readTestDataWithTxn(t, s, &rTxn, waitTxns, keys...)
}

func readTestDataWithTxn(t *testing.T, s *KVTxnStorage, rTxn *txn.TxnMeta, waitTxns [][]byte, keys ...byte) *readResult {
	req := &message{}
	for _, k := range keys {
		key := []byte{k}
		req.Keys = append(req.Keys, key)
	}

	rs, err := s.Read(context.TODO(), *rTxn, getOpCode, req.mustMarshal())
	assert.NoError(t, err)
	assert.Equal(t, waitTxns, rs.WaitTxns())
	return rs.(*readResult)
}

func checkReadResult(t *testing.T, sts byte, rs *readResult, keys ...byte) {
	data, err := rs.Read()
	assert.NoError(t, err)
	resp := &message{}
	resp.mustUnmarshal(data)

	var values [][]byte
	for _, k := range keys {
		if k == 0 {
			values = append(values, nil)
		} else {
			values = append(values, []byte{k, sts})
		}
	}

	assert.Equal(t, values, resp.Values)
}

func newTimestamp(v int64) timestamp.Timestamp {
	return timestamp.Timestamp{PhysicalTime: v}
}

func newTxnMeta(snapshotTS int64) txn.TxnMeta {
	id := uuid.New()
	return txn.TxnMeta{
		ID:         id[:],
		Status:     txn.TxnStatus_Active,
		SnapshotTS: newTimestamp(snapshotTS),
	}
}

func newTestClock(start int64) clock.Clock {
	ts := start
	return clock.NewHLCClock(func() int64 {
		return atomic.AddInt64(&ts, 1)
	}, math.MaxInt64)
}
