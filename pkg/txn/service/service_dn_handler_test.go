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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/mem"
	"github.com/stretchr/testify/assert"
)

func TestPrepare(t *testing.T) {
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

	wTxn := NewTestTxn(1, 1, 1, 2)
	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1))
	checkResponses(t, prepareTestTxn(t, sender, wTxn, 1))

	value := s.storage.(*mem.KVTxnStorage).GetUncommittedTxn(wTxn.ID)
	assert.NotNil(t, value)
	assert.Equal(t, txn.TxnStatus_Prepared, value.Status)
	assert.Equal(t, s.getTxnContext(wTxn.ID).getTxn(), *value)
}

func TestPrepareWithAlreadyPreparedTxn(t *testing.T) {
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

	wTxn := NewTestTxn(1, 1, 1, 2)
	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1))
	checkResponses(t, prepareTestTxn(t, sender, wTxn, 1))
	checkResponses(t, prepareTestTxn(t, sender, wTxn, 1))

	value := s.storage.(*mem.KVTxnStorage).GetUncommittedTxn(wTxn.ID)
	assert.NotNil(t, value)
	assert.Equal(t, txn.TxnStatus_Prepared, value.Status)
	assert.Equal(t, s.getTxnContext(wTxn.ID).getTxn(), *value)
}

func TestPrepareWithTxnNotExist(t *testing.T) {
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

	wTxn := NewTestTxn(1, 1, 1, 2)
	checkResponses(t, prepareTestTxn(t, sender, wTxn, 1),
		txn.WrapError(moerr.NewTxnNotFound(), 0))
}

func TestGetStatus(t *testing.T) {
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

	wTxn := NewTestTxn(1, 1, 1, 2)
	responses := getTestTxnStatus(t, sender, wTxn, 1)
	assert.Nil(t, responses[0].Txn)
	assert.Nil(t, responses[0].TxnError)

	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1))
	responses = getTestTxnStatus(t, sender, wTxn, 1)
	assert.NotNil(t, responses[0].Txn)
	assert.Equal(t, s.getTxnContext(wTxn.ID).getTxn(), *responses[0].Txn)
	assert.Nil(t, responses[0].TxnError)

	checkResponses(t, prepareTestTxn(t, sender, wTxn, 1))
	responses = getTestTxnStatus(t, sender, wTxn, 1)
	assert.NotNil(t, responses[0].Txn)
	assert.Equal(t, s.getTxnContext(wTxn.ID).getTxn(), *responses[0].Txn)
	assert.Nil(t, responses[0].TxnError)

	wTxn2 := NewTestTxn(2, 1, 1)
	checkResponses(t, writeTestData(t, sender, 1, wTxn2, 2))
	checkResponses(t, commitWriteData(t, sender, wTxn2))
	responses = getTestTxnStatus(t, sender, wTxn2, 1)
	assert.Nil(t, responses[0].Txn)
	assert.Nil(t, responses[0].TxnError)
}

func prepareTestTxn(t *testing.T, sender rpc.TxnSender, wTxn txn.TxnMeta, shard uint64) []txn.TxnResponse {
	result, err := sender.Send(context.Background(), []txn.TxnRequest{NewTestPrepareRequest(wTxn, shard)})
	assert.NoError(t, err)
	return result.Responses
}

func getTestTxnStatus(t *testing.T, sender rpc.TxnSender, wTxn txn.TxnMeta, shard uint64) []txn.TxnResponse {
	result, err := sender.Send(context.Background(), []txn.TxnRequest{NewTestGetStatusRequest(wTxn, shard)})
	assert.NoError(t, err)
	return result.Responses
}
