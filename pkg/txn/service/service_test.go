// Copyright 2022 Matrix Origin
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
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
)

type retryTestSender struct {
	send func(context.Context, []txn.TxnRequest) (*rpc.SendResult, error)
}

func (s *retryTestSender) Send(ctx context.Context, requests []txn.TxnRequest) (*rpc.SendResult, error) {
	return s.send(ctx, requests)
}

func (s *retryTestSender) Close() error {
	return nil
}

func TestGCZombie(t *testing.T) {
	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	zombie := time.Millisecond * 100
	s := NewTestTxnServiceWithLogAndZombie(t, 1, sender, NewTestClock(1), nil, zombie).(*service)
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close(false))
	}()

	sender.AddTxnService(s)

	wTxn := NewTestTxn(1, 1, 1)
	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1))

	w1 := addTestWaiter(t, s, wTxn, txn.TxnStatus_Aborted)
	defer w1.close()

	checkWaiter(t, w1, txn.TxnStatus_Aborted)
	checkData(t, wTxn, s, 0, 0, false)
}

func TestGCZombieWithDistributedTxn(t *testing.T) {
	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	zombie := time.Millisecond * 100
	s1 := NewTestTxnServiceWithLogAndZombie(t, 1, sender, NewTestClock(1), nil, zombie).(*service)
	assert.NoError(t, s1.Start())
	defer func() {
		assert.NoError(t, s1.Close(false))
	}()

	s2 := NewTestTxnServiceWithLogAndZombie(t, 2, sender, NewTestClock(1), nil, zombie).(*service)
	assert.NoError(t, s2.Start())
	defer func() {
		assert.NoError(t, s2.Close(false))
	}()

	sender.AddTxnService(s1)
	sender.AddTxnService(s2)

	// 1 is coordinator
	wTxn := NewTestTxn(1, 1, 1, 2)
	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1))
	checkResponses(t, writeTestData(t, sender, 2, wTxn, 2))

	w1 := addTestWaiter(t, s1, wTxn, txn.TxnStatus_Aborted)
	defer w1.close()

	w2 := addTestWaiter(t, s2, wTxn, txn.TxnStatus_Aborted)
	defer w2.close()

	checkWaiter(t, w1, txn.TxnStatus_Aborted)
	checkWaiter(t, w2, txn.TxnStatus_Aborted)

	checkData(t, wTxn, s1, 0, 0, false)
	checkData(t, wTxn, s2, 0, 0, false)
}

func TestGCZombieNonCoordinatorTxn(t *testing.T) {
	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	zombie := time.Millisecond * 100
	s := NewTestTxnServiceWithLogAndZombie(t, 1, sender, NewTestClock(1), nil, zombie).(*service)
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close(false))
	}()

	sender.AddTxnService(s)

	wTxn := NewTestTxn(1, 1, 1)
	wTxn.TNShards = append(wTxn.TNShards, NewTestTNShard(2))
	// make shard 2 is coordinator
	wTxn.TNShards[0], wTxn.TNShards[1] = wTxn.TNShards[1], wTxn.TNShards[0]

	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1))

	w1 := addTestWaiter(t, s, wTxn, txn.TxnStatus_Aborted)
	defer w1.close()

	ctx, cancel := context.WithTimeout(context.Background(), zombie*5)
	defer cancel()
	_, err := w1.wait(ctx)
	assert.Error(t, err)
	assert.Equal(t, moerr.ConvertGoError(ctx, ctx.Err()), err)
}

func Test_parallelSendWithRetry(t *testing.T) {
	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	zombie := time.Millisecond * 3
	s := NewTestTxnServiceWithLogAndZombie(t, 1, sender, NewTestClock(1), nil, zombie).(*service)
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close(false))
	}()

	sender.AddTxnService(s)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sender.action = "return_err_and_reset"
	s.parallelSendWithRetry(ctx, nil, nil)
}

func Test_parallelSendWithRetryStopsDuringBackoffOnContextCancel(t *testing.T) {
	firstSend := make(chan struct{})
	sender := &retryTestSender{
		send: func(ctx context.Context, requests []txn.TxnRequest) (*rpc.SendResult, error) {
			select {
			case <-firstSend:
				return &rpc.SendResult{}, nil
			default:
				close(firstSend)
				return nil, moerr.NewInternalErrorNoCtx("return error")
			}
		},
	}
	s := NewTestTxnServiceWithLogAndZombie(t, 1, sender, NewTestClock(1), nil, time.Millisecond*3).(*service)
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close(false))
	}()

	ctx, cancel := context.WithCancel(context.Background())
	resultC := make(chan *rpc.SendResult, 1)
	go func() {
		resultC <- s.parallelSendWithRetry(ctx, nil, nil)
	}()

	<-firstSend
	cancel()
	assert.Nil(t, <-resultC)
}

func Test_parallelSendWithRetryReturnsNilWhenContextAlreadyCanceled(t *testing.T) {
	called := false
	sender := &retryTestSender{
		send: func(ctx context.Context, requests []txn.TxnRequest) (*rpc.SendResult, error) {
			called = true
			return &rpc.SendResult{}, nil
		},
	}
	s := NewTestTxnServiceWithLogAndZombie(t, 1, sender, NewTestClock(1), nil, time.Millisecond*3).(*service)
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close(false))
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	assert.Nil(t, s.parallelSendWithRetry(ctx, nil, nil))
	assert.False(t, called)
}
