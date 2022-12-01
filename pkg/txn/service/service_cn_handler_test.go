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

package service

import (
	"context"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/mem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadBasic(t *testing.T) {
	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s := NewTestTxnService(t, 1, sender, NewTestClock(0))
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close(false))
	}()

	sender.AddTxnService(s)

	rTxn := NewTestTxn(1, 1)
	resp := readTestData(t, sender, 1, rTxn, 1)
	checkReadResponses(t, resp, "")
}

func TestReadWithDNShardNotMatch(t *testing.T) {
	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()
	sender.setFilter(func(req *txn.TxnRequest) bool {
		req.CNRequest.Target.ReplicaID = 0
		return true
	})

	s := NewTestTxnService(t, 1, sender, NewTestClock(0))
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close(false))
	}()

	sender.AddTxnService(s)

	rTxn := NewTestTxn(1, 1)
	resp := readTestData(t, sender, 1, rTxn, 1)
	checkResponses(t, resp,
		txn.WrapError(moerr.NewDNShardNotFound(context.TODO(), "", 1), 0))
	// newTxnError(moerr.ErrDNShardNotFound, "txn not active"))
}

func TestReadWithSelfWrite(t *testing.T) {
	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s := NewTestTxnService(t, 1, sender, NewTestClock(0))
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close(false))
	}()
	sender.AddTxnService(s)

	rwTxn := NewTestTxn(1, 1)
	checkResponses(t, writeTestData(t, sender, 1, rwTxn, 1))
	checkReadResponses(t, readTestData(t, sender, 1, rwTxn, 1), "1-1-1")
	checkResponses(t, writeTestData(t, sender, 1, rwTxn, 2))
	checkReadResponses(t, readTestData(t, sender, 1, rwTxn, 2), "2-1-1")
}

func TestReadBlockWithClock(t *testing.T) {
	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	ts := int64(0)
	s := NewTestTxnService(t, 1, sender, NewTestSpecClock(func() int64 {
		return atomic.AddInt64(&ts, 1)
	}))
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close(false))
	}()
	sender.AddTxnService(s)

	c := make(chan struct{})
	go func() {
		rwTxn := NewTestTxn(1, 3)
		checkReadResponses(t, readTestData(t, sender, 1, rwTxn, 1), "")
		c <- struct{}{}
	}()
	<-c
	assert.Equal(t, int64(3), ts)
}

func TestReadCannotBlockByUncomitted(t *testing.T) {
	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s := NewTestTxnService(t, 1, sender, NewTestClock(1))
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close(false))
	}()
	sender.AddTxnService(s)

	wTxn := NewTestTxn(1, 1)
	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1))

	rTxn := NewTestTxn(2, 1)
	checkReadResponses(t, readTestData(t, sender, 1, rTxn, 1), "")
}

func TestReadCannotBlockByPreparedIfSnapshotTSIsLEPreparedTS(t *testing.T) {
	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s := NewTestTxnService(t, 1, sender, NewTestClock(1))
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close(false))
	}()
	sender.AddTxnService(s)

	wTxn := NewTestTxn(1, 1)
	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1))
	prepareTestTxn(t, sender, wTxn, 1) // prepare at 2

	rTxn := NewTestTxn(2, 1)
	checkReadResponses(t, readTestData(t, sender, 1, rTxn, 1), "")

	rTxn = NewTestTxn(2, 2)
	checkReadResponses(t, readTestData(t, sender, 1, rTxn, 1), "")
}

func TestReadWillBlockByPreparedIfSnapshotTSIsGTPreparedTS(t *testing.T) {
	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s := NewTestTxnService(t, 1, sender, NewTestClock(1))
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close(false))
	}()
	sender.AddTxnService(s)

	wTxn := NewTestTxn(1, 1)
	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1))
	prepareTestTxn(t, sender, wTxn, 1) // prepare at 2

	c := make(chan struct{})
	go func() {
		rTxn := NewTestTxn(2, 3)
		readTestData(t, sender, 1, rTxn, 1)
		close(c)
	}()
	select {
	case <-c:
		assert.Fail(t, "cannot read")
	case <-time.After(time.Second):

	}
}

func TestReadAfterBlockTxnCommitted(t *testing.T) {
	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s := NewTestTxnService(t, 1, sender, NewTestClock(1))
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close(false))
	}()
	sender.AddTxnService(s)

	wTxn := NewTestTxn(1, 1, 1)
	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1))
	prepareTestTxn(t, sender, wTxn, 1) // prepare at 2

	c := make(chan struct{})
	go func() {
		rTxn := NewTestTxn(2, 3)
		checkReadResponses(t, readTestData(t, sender, 1, rTxn, 1), "1-1-1")
		close(c)
	}()
	go func() {
		time.Sleep(time.Second)
		wTxn.CommitTS = NewTestTimestamp(2) // commit at 2
		checkResponses(t, commitShardWriteData(t, sender, wTxn))
	}()

	select {
	case <-c:
	case <-time.After(time.Minute):
		assert.Fail(t, "cannot read")
	}
}

func TestReadAfterBlockTxnCommittedAndCannotReadCommittedValue(t *testing.T) {
	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s := NewTestTxnService(t, 1, sender, NewTestClock(1))
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close(false))
	}()
	sender.AddTxnService(s)

	wTxn := NewTestTxn(1, 1, 1)
	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1))
	prepareTestTxn(t, sender, wTxn, 1) // prepare at 2

	c := make(chan struct{})
	go func() {
		rTxn := NewTestTxn(2, 3)
		checkReadResponses(t, readTestData(t, sender, 1, rTxn, 1), "")
		close(c)
	}()
	go func() {
		time.Sleep(time.Second)
		wTxn.CommitTS = NewTestTimestamp(3) // commit at 3
		checkResponses(t, commitShardWriteData(t, sender, wTxn))
	}()

	select {
	case <-c:
	case <-time.After(time.Minute):
		assert.Fail(t, "cannot read")
	}
}

func TestReadAfterBlockTxnAborted(t *testing.T) {
	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s := NewTestTxnService(t, 1, sender, NewTestClock(1))
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close(false))
	}()
	sender.AddTxnService(s)

	wTxn := NewTestTxn(1, 1, 1)
	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1))
	prepareTestTxn(t, sender, wTxn, 1) // prepare at 2

	c := make(chan struct{})
	go func() {
		rTxn := NewTestTxn(2, 3)
		checkReadResponses(t, readTestData(t, sender, 1, rTxn, 1), "")
		close(c)
	}()
	go func() {
		time.Sleep(time.Second)
		checkResponses(t, rollbackShardWriteData(t, sender, wTxn))
	}()

	select {
	case <-c:
	case <-time.After(time.Minute):
		assert.Fail(t, "cannot read")
	}
}

func TestReadCannotBlockByCommittingIfSnapshotTSIsLECommitTS(t *testing.T) {
	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s := NewTestTxnService(t, 1, sender, NewTestClock(1)).(*service)
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close(false))
	}()
	sender.AddTxnService(s)

	wTxn := NewTestTxn(1, 1)
	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1))
	prepareTestTxn(t, sender, wTxn, 1) // prepare at 2

	wTxn.CommitTS = NewTestTimestamp(2)
	assert.NoError(t, s.storage.(*mem.KVTxnStorage).Committing(context.TODO(), wTxn))

	rTxn := NewTestTxn(2, 1)
	checkReadResponses(t, readTestData(t, sender, 1, rTxn, 1), "")

	rTxn = NewTestTxn(2, 2)
	checkReadResponses(t, readTestData(t, sender, 1, rTxn, 1), "")
}

func TestReadWillBlockByCommittingIfSnapshotTSIsGTCommitTS(t *testing.T) {
	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s := NewTestTxnService(t, 1, sender, NewTestClock(1)).(*service)
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close(false))
	}()
	sender.AddTxnService(s)

	wTxn := NewTestTxn(1, 1)
	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1))
	prepareTestTxn(t, sender, wTxn, 1) // prepare at 2

	wTxn.CommitTS = NewTestTimestamp(2)
	assert.NoError(t, s.storage.(*mem.KVTxnStorage).Committing(context.TODO(), wTxn))

	c := make(chan struct{})
	go func() {
		rTxn := NewTestTxn(2, 3)
		readTestData(t, sender, 1, rTxn, 1)
		close(c)
	}()
	select {
	case <-c:
		assert.Fail(t, "cannot read")
	case <-time.After(time.Second):

	}
}

func TestReadCommitted(t *testing.T) {
	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s := NewTestTxnService(t, 1, sender, NewTestClock(1))
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close(false))
	}()
	sender.AddTxnService(s)

	wTxn1 := NewTestTxn(1, 1, 1) // commit at 2
	checkResponses(t, writeTestData(t, sender, 1, wTxn1, 1))
	checkResponses(t, commitWriteData(t, sender, wTxn1))

	wTxn2 := NewTestTxn(2, 1, 1) // commit at 3
	checkResponses(t, writeTestData(t, sender, 1, wTxn2, 2))
	checkResponses(t, commitWriteData(t, sender, wTxn2))

	rTxn := NewTestTxn(3, 2)
	checkReadResponses(t, readTestData(t, sender, 1, rTxn, 1), "")
	checkReadResponses(t, readTestData(t, sender, 1, rTxn, 2), "")

	rTxn = NewTestTxn(3, 3)
	checkReadResponses(t, readTestData(t, sender, 1, rTxn, 1), string(GetTestValue(1, wTxn1)))
	checkReadResponses(t, readTestData(t, sender, 1, rTxn, 2), "")

	rTxn = NewTestTxn(3, 4)
	checkReadResponses(t, readTestData(t, sender, 1, rTxn, 1), string(GetTestValue(1, wTxn1)))
	checkReadResponses(t, readTestData(t, sender, 1, rTxn, 2), string(GetTestValue(2, wTxn2)))
}

func TestWriteBasic(t *testing.T) {
	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s := NewTestTxnService(t, 1, sender, NewTestClock(0)).(*service)
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close(false))
	}()

	sender.AddTxnService(s)

	wTxn := NewTestTxn(1, 1)
	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1))

	kv := s.storage.(*mem.KVTxnStorage).GetUncommittedKV()
	v, ok := kv.Get(GetTestKey(1))
	assert.True(t, ok)
	assert.Equal(t, GetTestValue(1, wTxn), v)
}

func TestWriteWithDNShardNotMatch(t *testing.T) {
	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()
	sender.setFilter(func(req *txn.TxnRequest) bool {
		req.CNRequest.Target.ReplicaID = 0
		return true
	})

	s := NewTestTxnService(t, 1, sender, NewTestClock(0)).(*service)
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close(false))
	}()

	sender.AddTxnService(s)

	wTxn := NewTestTxn(1, 1)
	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1),
		txn.WrapError(moerr.NewDNShardNotFound(context.TODO(), "", 1), 0))
}

func TestWriteWithWWConflict(t *testing.T) {
	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s := NewTestTxnService(t, 1, sender, NewTestClock(0))
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close(false))
	}()

	sender.AddTxnService(s)

	wTxn := NewTestTxn(1, 1)
	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1))

	wTxn2 := NewTestTxn(2, 1)
	checkResponses(t, writeTestData(t, sender, 1, wTxn2, 1),
		txn.WrapError(moerr.NewTAEWrite(context.TODO()), 0))
	// newTxnError(moerr.ErrTAEWrite, "write conlict"))
}

func TestCommitWithSingleDNShard(t *testing.T) {
	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s := NewTestTxnService(t, 1, sender, NewTestClock(1)).(*service)
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close(false))
	}()

	sender.AddTxnService(s)

	n := byte(10)
	wTxn := NewTestTxn(1, 1, 1)
	for i := byte(0); i < n; i++ {
		checkResponses(t, writeTestData(t, sender, 1, wTxn, i))
	}
	checkResponses(t, commitWriteData(t, sender, wTxn))

	for i := byte(0); i < n; i++ {
		var values [][]byte
		var timestamps []timestamp.Timestamp
		kv := s.storage.(*mem.KVTxnStorage).GetCommittedKV()

		kv.AscendRange(GetTestKey(i), NewTestTimestamp(0), NewTestTimestamp(math.MaxInt64), func(value []byte, ts timestamp.Timestamp) {
			values = append(values, value)
			timestamps = append(timestamps, ts)
		})
		assert.Equal(t, [][]byte{GetTestValue(i, wTxn)}, values)
		assert.Equal(t, []timestamp.Timestamp{NewTestTimestamp(2)}, timestamps)
	}
}

func TestCommitWithDNShardNotMatch(t *testing.T) {
	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()
	sender.setFilter(func(req *txn.TxnRequest) bool {
		if req.CommitRequest != nil {
			req.Txn.DNShards[0].ReplicaID = 0
		}
		return true
	})

	s := NewTestTxnService(t, 1, sender, NewTestClock(1)).(*service)
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close(false))
	}()

	sender.AddTxnService(s)

	n := byte(10)
	wTxn := NewTestTxn(1, 1, 1)
	for i := byte(0); i < n; i++ {
		checkResponses(t, writeTestData(t, sender, 1, wTxn, i))
	}
	checkResponses(t, commitWriteData(t, sender, wTxn),
		txn.WrapError(moerr.NewDNShardNotFound(context.TODO(), "", 1), 0))
}

func TestCommitWithMultiDNShards(t *testing.T) {
	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s1 := NewTestTxnService(t, 1, sender, NewTestClock(1)).(*service)
	assert.NoError(t, s1.Start())
	defer func() {
		assert.NoError(t, s1.Close(false))
	}()
	s2 := NewTestTxnService(t, 2, sender, NewTestClock(1)).(*service)
	assert.NoError(t, s2.Start())
	defer func() {
		assert.NoError(t, s2.Close(false))
	}()

	sender.AddTxnService(s1)
	sender.AddTxnService(s2)

	wTxn := NewTestTxn(1, 1, 1, 2)
	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1))
	checkResponses(t, writeTestData(t, sender, 2, wTxn, 2))

	w1 := addTestWaiter(t, s1, wTxn, txn.TxnStatus_Committed)
	defer w1.close()
	w2 := addTestWaiter(t, s2, wTxn, txn.TxnStatus_Committed)
	defer w2.close()

	checkResponses(t, commitWriteData(t, sender, wTxn))

	checkWaiter(t, w1, txn.TxnStatus_Committed)
	checkWaiter(t, w2, txn.TxnStatus_Committed)

	checkData(t, wTxn, s1, 2, 1, true)
	checkData(t, wTxn, s2, 2, 2, true)
}

func TestCommitWithRollbackIfAnyPrepareFailed(t *testing.T) {
	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s1 := NewTestTxnService(t, 1, sender, NewTestClock(1)).(*service)
	assert.NoError(t, s1.Start())
	defer func() {
		assert.NoError(t, s1.Close(false))
	}()
	s2 := NewTestTxnService(t, 2, sender, NewTestClock(1)).(*service)
	assert.NoError(t, s2.Start())
	defer func() {
		assert.NoError(t, s2.Close(false))
	}()

	sender.AddTxnService(s1)
	sender.AddTxnService(s2)

	wTxn1 := NewTestTxn(1, 1, 1)
	writeTestData(t, sender, 1, wTxn1, 1)
	checkResponses(t, commitWriteData(t, sender, wTxn1)) // commit at 2

	wTxn := NewTestTxn(1, 1, 1, 2)
	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1))
	checkResponses(t, writeTestData(t, sender, 2, wTxn, 2))

	w1 := addTestWaiter(t, s1, wTxn, txn.TxnStatus_Aborted)
	defer w1.close()
	w2 := addTestWaiter(t, s2, wTxn, txn.TxnStatus_Aborted)
	defer w2.close()

	checkResponses(t, commitWriteData(t, sender, wTxn),
		txn.WrapError(moerr.NewTAEPrepare(context.TODO(), "cannot prepare"), 0))

	checkWaiter(t, w1, txn.TxnStatus_Aborted)
	checkWaiter(t, w2, txn.TxnStatus_Aborted)

	checkData(t, wTxn, s1, 2, 2, false)
	checkData(t, wTxn, s2, 2, 2, false)
}

func TestRollback(t *testing.T) {
	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s1 := NewTestTxnService(t, 1, sender, NewTestClock(1)).(*service)
	assert.NoError(t, s1.Start())
	defer func() {
		assert.NoError(t, s1.Close(false))
	}()
	s2 := NewTestTxnService(t, 2, sender, NewTestClock(1)).(*service)
	assert.NoError(t, s2.Start())
	defer func() {
		assert.NoError(t, s2.Close(false))
	}()

	sender.AddTxnService(s1)
	sender.AddTxnService(s2)

	wTxn := NewTestTxn(1, 1, 1)
	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1))
	wTxn.DNShards = append(wTxn.DNShards, NewTestDNShard(2))
	checkResponses(t, writeTestData(t, sender, 2, wTxn, 2))

	w1 := addTestWaiter(t, s1, wTxn, txn.TxnStatus_Aborted)
	defer w1.close()
	w2 := addTestWaiter(t, s2, wTxn, txn.TxnStatus_Aborted)
	defer w2.close()

	responses := rollbackWriteData(t, sender, wTxn)
	checkResponses(t, responses)
	for _, resp := range responses {
		assert.Equal(t, txn.TxnStatus_Aborted, resp.Txn.Status)
	}

	checkWaiter(t, w1, txn.TxnStatus_Aborted)
	checkWaiter(t, w2, txn.TxnStatus_Aborted)

	checkData(t, wTxn, s1, 2, 0, false)
	checkData(t, wTxn, s2, 2, 0, false)
}

func TestRollbackWithDNShardNotFound(t *testing.T) {
	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	sender.setFilter(func(req *txn.TxnRequest) bool {
		if req.RollbackRequest != nil {
			req.Txn.DNShards[0].ReplicaID = 0
		}
		return true
	})

	s1 := NewTestTxnService(t, 1, sender, NewTestClock(1)).(*service)
	assert.NoError(t, s1.Start())
	defer func() {
		assert.NoError(t, s1.Close(false))
	}()
	s2 := NewTestTxnService(t, 2, sender, NewTestClock(1)).(*service)
	assert.NoError(t, s2.Start())
	defer func() {
		assert.NoError(t, s2.Close(false))
	}()

	sender.AddTxnService(s1)
	sender.AddTxnService(s2)

	wTxn := NewTestTxn(1, 1, 1)
	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1))
	wTxn.DNShards = append(wTxn.DNShards, NewTestDNShard(2))
	checkResponses(t, writeTestData(t, sender, 2, wTxn, 2))

	checkResponses(t, rollbackWriteData(t, sender, wTxn),
		txn.WrapError(moerr.NewDNShardNotFound(context.TODO(), "", 1), 0))
}

func writeTestData(t *testing.T, sender rpc.TxnSender, toShard uint64, wTxn txn.TxnMeta, keys ...byte) []txn.TxnResponse {
	requests := make([]txn.TxnRequest, 0, len(keys))
	for _, k := range keys {
		requests = append(requests, NewTestWriteRequest(k, wTxn, toShard))
	}
	result, err := sender.Send(context.Background(), requests)
	assert.NoError(t, err)
	responses := result.Responses
	assert.Equal(t, len(keys), len(responses))
	return responses
}

func TestDebug(t *testing.T) {
	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s := NewTestTxnService(t, 1, sender, NewTestClock(0))
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close(false))
	}()

	sender.AddTxnService(s)

	data := []byte("OK")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	result, err := sender.Send(ctx, []txn.TxnRequest{
		{
			Method: txn.TxnMethod_DEBUG,
			CNRequest: &txn.CNOpRequest{
				Payload: data,
				Target:  NewTestDNShard(1),
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, data, result.Responses[0].CNOpResponse.Payload)
}

func commitShardWriteData(t *testing.T, sender rpc.TxnSender, wTxn txn.TxnMeta) []txn.TxnResponse {
	result, err := sender.Send(context.Background(), []txn.TxnRequest{NewTestCommitShardRequest(wTxn)})
	assert.NoError(t, err)
	responses := result.Responses
	return responses
}

func rollbackShardWriteData(t *testing.T, sender rpc.TxnSender, wTxn txn.TxnMeta) []txn.TxnResponse {
	result, err := sender.Send(context.Background(), []txn.TxnRequest{NewTestRollbackShardRequest(wTxn)})
	assert.NoError(t, err)
	responses := result.Responses
	return responses
}

func commitWriteData(t *testing.T, sender rpc.TxnSender, wTxn txn.TxnMeta) []txn.TxnResponse {
	result, err := sender.Send(context.Background(), []txn.TxnRequest{NewTestCommitRequest(wTxn)})
	assert.NoError(t, err)
	responses := result.Responses
	return responses
}

func rollbackWriteData(t *testing.T, sender rpc.TxnSender, wTxn txn.TxnMeta) []txn.TxnResponse {
	result, err := sender.Send(context.Background(), []txn.TxnRequest{NewTestRollbackRequest(wTxn)})
	assert.NoError(t, err)
	responses := result.Responses
	return responses
}

func readTestData(t *testing.T, sender rpc.TxnSender, toShard uint64, rTxn txn.TxnMeta, keys ...byte) []txn.TxnResponse {
	requests := make([]txn.TxnRequest, 0, len(keys))
	for _, k := range keys {
		requests = append(requests, NewTestReadRequest(k, rTxn, toShard))
	}
	result, err := sender.Send(context.Background(), requests)
	assert.NoError(t, err)
	responses := result.Responses
	assert.Equal(t, len(keys), len(responses))
	return responses
}

func checkReadResponses(t *testing.T, response []txn.TxnResponse, expectValues ...string) {
	for idx, resp := range response {
		values := mem.MustParseGetPayload(resp.CNOpResponse.Payload)
		assert.Equal(t, expectValues[idx], string(values[0]))
		assert.NotNil(t, resp.Txn)
	}
}

func checkResponses(t *testing.T, response []txn.TxnResponse, expectErrors ...*txn.TxnError) {
	if len(expectErrors) == 0 {
		expectErrors = make([]*txn.TxnError, len(response))
	}
	for idx, resp := range response {
		if resp.TxnError == nil {
			assert.Equal(t, expectErrors[idx], resp.TxnError)
		} else {
			assert.Equal(t, expectErrors[idx].TxnErrCode, resp.TxnError.TxnErrCode)
		}
	}
}

func checkData(t *testing.T, wTxn txn.TxnMeta, s *service, commitTS int64, k byte, committed bool) {
	for {
		v := s.getTxnContext(wTxn.ID)
		if v != nil {
			time.Sleep(time.Millisecond * 100)
			continue
		}
		break
	}

	kv := s.storage.(*mem.KVTxnStorage)

	if committed {
		kv.RLock()
		v, ok := kv.GetCommittedKV().Get(GetTestKey(k), NewTestTimestamp(commitTS))
		assert.True(t, ok)
		assert.Equal(t, GetTestValue(k, wTxn), v)
		kv.RUnlock()
	} else {
		kv.RLock()
		n := 0
		kv.GetCommittedKV().AscendRange(GetTestKey(k),
			NewTestTimestamp(commitTS).Next(),
			NewTestTimestamp(math.MaxInt64), func(_ []byte, _ timestamp.Timestamp) {
				n++
			})
		assert.Equal(t, 0, n)
		kv.RUnlock()
	}

	kv.RLock()
	v, ok := kv.GetUncommittedKV().Get(GetTestKey(k))
	assert.False(t, ok)
	assert.Empty(t, v)
	kv.RUnlock()

	assert.Nil(t, kv.GetUncommittedTxn(wTxn.ID))
}

func addTestWaiter(t *testing.T, s *service, wTxn txn.TxnMeta, status txn.TxnStatus) *waiter {
	txnCtx := s.getTxnContext(wTxn.ID)
	assert.NotNil(t, txnCtx)
	w := acquireWaiter()
	assert.True(t, txnCtx.addWaiter(wTxn.ID, w, status))
	return w
}

func checkWaiter(t *testing.T, w *waiter, expectStatus txn.TxnStatus) {
	status, err := w.wait(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, expectStatus, status)
}
