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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/mem"
	"github.com/stretchr/testify/assert"
)

func TestRecoveryFromCommittedWithData(t *testing.T) {
	mlog := mem.NewMemLog()
	wTxn := NewTestTxn(1, 1, 1)
	wTxn.Status = txn.TxnStatus_Committed
	wTxn.CommitTS = NewTestTimestamp(2)
	addLog(t, mlog, wTxn, 1, 2)

	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s := NewTestTxnServiceWithLog(t, 1, sender, NewTestClock(0), mlog).(*service)
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close(false))
	}()

	checkData(t, wTxn, s, 2, 1, true)
	checkData(t, wTxn, s, 2, 2, true)
}

func TestRecoveryFromMultiCommittedWithData(t *testing.T) {
	mlog := mem.NewMemLog()
	wTxn := NewTestTxn(1, 1, 1)
	wTxn.Status = txn.TxnStatus_Committed
	wTxn.CommitTS = NewTestTimestamp(2)
	addLog(t, mlog, wTxn, 1, 2)
	addLog(t, mlog, wTxn, 1, 2)
	addLog(t, mlog, wTxn, 1, 2)

	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s := NewTestTxnServiceWithLog(t, 1, sender, NewTestClock(0), mlog).(*service)
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close(false))
	}()

	checkData(t, wTxn, s, 2, 1, true)
	checkData(t, wTxn, s, 2, 2, true)
}

func TestRecoveryFromCommittedAfterPrepared(t *testing.T) {
	mlog := mem.NewMemLog()
	wTxn := NewTestTxn(1, 1, 1)
	wTxn.Status = txn.TxnStatus_Prepared
	wTxn.PreparedTS = NewTestTimestamp(2)
	addLog(t, mlog, wTxn, 1, 2)

	wTxn.Status = txn.TxnStatus_Committed
	wTxn.CommitTS = NewTestTimestamp(3)
	addLog(t, mlog, wTxn)

	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s := NewTestTxnServiceWithLog(t, 1, sender, NewTestClock(0), mlog).(*service)
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close(false))
	}()

	checkData(t, wTxn, s, 3, 1, true)
	checkData(t, wTxn, s, 3, 2, true)
}

func TestRecoveryFromMultiCommittedAfterPrepared(t *testing.T) {
	mlog := mem.NewMemLog()
	wTxn := NewTestTxn(1, 1, 1)
	wTxn.Status = txn.TxnStatus_Prepared
	wTxn.PreparedTS = NewTestTimestamp(2)
	addLog(t, mlog, wTxn, 1, 2)
	addLog(t, mlog, wTxn, 1, 2)
	addLog(t, mlog, wTxn, 1, 2)

	wTxn.Status = txn.TxnStatus_Committed
	wTxn.CommitTS = NewTestTimestamp(3)
	addLog(t, mlog, wTxn)
	addLog(t, mlog, wTxn)
	addLog(t, mlog, wTxn)

	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s := NewTestTxnServiceWithLog(t, 1, sender, NewTestClock(0), mlog).(*service)
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close(false))
	}()

	checkData(t, wTxn, s, 3, 1, true)
	checkData(t, wTxn, s, 3, 2, true)
}

func TestRecoveryFromMultiDNShardWithAllPrepared(t *testing.T) {
	mlog1 := mem.NewMemLog()
	mlog2 := mem.NewMemLog()

	wTxn := NewTestTxn(1, 1, 1, 2)
	wTxn.Status = txn.TxnStatus_Prepared
	wTxn.PreparedTS = NewTestTimestamp(2)

	addLog(t, mlog1, wTxn, 1)
	addLog(t, mlog2, wTxn, 2)

	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s1 := NewTestTxnServiceWithLog(t, 1, sender, NewTestClock(0), mlog1).(*service)
	s2 := NewTestTxnServiceWithLog(t, 2, sender, NewTestClock(0), mlog2).(*service)
	sender.AddTxnService(s1)
	sender.AddTxnService(s2)

	assert.NoError(t, s1.Start())
	defer func() {
		assert.NoError(t, s1.Close(false))
	}()

	assert.NoError(t, s2.Start())
	defer func() {
		assert.NoError(t, s2.Close(false))
	}()

	for e := range s1.storage.(*mem.KVTxnStorage).GetEventC() {
		if e.Type == mem.CommitType {
			break
		}
	}

	for e := range s2.storage.(*mem.KVTxnStorage).GetEventC() {
		if e.Type == mem.CommitType {
			break
		}
	}

	checkData(t, wTxn, s1, 2, 1, true)
	checkData(t, wTxn, s2, 2, 2, true)
}

func TestRecoveryFromMultiDNShardWithAnyNotPrepared(t *testing.T) {
	mlog1 := mem.NewMemLog()
	mlog2 := mem.NewMemLog()

	wTxn := NewTestTxn(1, 1, 1, 2)
	wTxn.Status = txn.TxnStatus_Prepared
	wTxn.PreparedTS = NewTestTimestamp(2)

	addLog(t, mlog1, wTxn, 1)

	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s1 := NewTestTxnServiceWithLog(t, 1, sender, NewTestClock(0), mlog1).(*service)
	s2 := NewTestTxnServiceWithLog(t, 2, sender, NewTestClock(0), mlog2).(*service)
	sender.AddTxnService(s1)
	sender.AddTxnService(s2)

	assert.NoError(t, s1.Start())
	defer func() {
		assert.NoError(t, s1.Close(false))
	}()

	assert.NoError(t, s2.Start())
	defer func() {
		assert.NoError(t, s2.Close(false))
	}()

	for e := range s1.storage.(*mem.KVTxnStorage).GetEventC() {
		if e.Type == mem.RollbackType {
			break
		}
	}

	checkData(t, wTxn, s1, 0, 1, false)
	checkData(t, wTxn, s2, 0, 2, false)
}

func TestRecoveryFromMultiDNShardWithCommitting(t *testing.T) {
	mlog1 := mem.NewMemLog()
	mlog2 := mem.NewMemLog()

	wTxn := NewTestTxn(1, 1, 1, 2)
	wTxn.Status = txn.TxnStatus_Prepared
	wTxn.PreparedTS = NewTestTimestamp(2)

	addLog(t, mlog1, wTxn, 1)
	addLog(t, mlog2, wTxn, 2)

	wTxn.CommitTS = NewTestTimestamp(2)
	wTxn.Status = txn.TxnStatus_Committing
	addLog(t, mlog1, wTxn, 1)

	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s1 := NewTestTxnServiceWithLog(t, 1, sender, NewTestClock(0), mlog1).(*service)
	s2 := NewTestTxnServiceWithLog(t, 2, sender, NewTestClock(0), mlog2).(*service)
	sender.AddTxnService(s1)
	sender.AddTxnService(s2)

	assert.NoError(t, s1.Start())
	defer func() {
		assert.NoError(t, s1.Close(false))
	}()

	assert.NoError(t, s2.Start())
	defer func() {
		assert.NoError(t, s2.Close(false))
	}()

	for e := range s1.storage.(*mem.KVTxnStorage).GetEventC() {
		if e.Type == mem.CommitType {
			break
		}
	}

	for e := range s2.storage.(*mem.KVTxnStorage).GetEventC() {
		if e.Type == mem.CommitType {
			break
		}
	}

	checkData(t, wTxn, s1, 2, 1, true)
	checkData(t, wTxn, s2, 2, 2, true)
}

func addLog(t *testing.T, l logservice.Client, wTxn txn.TxnMeta, keys ...byte) {
	klog := mem.KVLog{
		Txn: wTxn,
	}
	for _, k := range keys {
		klog.Keys = append(klog.Keys, GetTestKey(k))
		klog.Values = append(klog.Values, GetTestValue(k, wTxn))
	}

	_, err := l.Append(context.Background(), logservice.LogRecord{
		Data: klog.MustMarshal(),
	})
	assert.NoError(t, err)
}
