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

package tnservice

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type leaseCancelReadTxnService struct {
	service.TxnService
	shard   metadata.TNShard
	entered chan struct{}
	readErr error
}

func (s *leaseCancelReadTxnService) Shard() metadata.TNShard {
	return s.shard
}

func (s *leaseCancelReadTxnService) Start() error {
	return nil
}

func (s *leaseCancelReadTxnService) CancelRecovery() {}

func (s *leaseCancelReadTxnService) Close(bool) error {
	return nil
}

func (s *leaseCancelReadTxnService) Read(ctx context.Context, _ *txn.TxnRequest, _ *txn.TxnResponse) error {
	if s.entered != nil {
		close(s.entered)
	}
	if s.readErr != nil {
		return s.readErr
	}
	<-ctx.Done()
	return context.Cause(ctx)
}

func TestRetryWaitHonorsContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		req := txn.TxnRequest{Options: &txn.TxnRequestOptions{
			RetryCodes:    []int32{int32(moerr.ErrTNShardNotFound)},
			RetryInterval: int64(time.Hour),
		}}
		resp := &txn.TxnResponse{}
		var calls atomic.Int32
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var resultErr error
		done := false
		go func() {
			resultErr = (&store{}).handleWithRetry(ctx, &req, resp,
				func(context.Context, *txn.TxnRequest, *txn.TxnResponse) error {
					calls.Add(1)
					resp.TxnError = txn.WrapError(moerr.NewTNShardNotFoundNoCtx("tn", 1), 0)
					return nil
				})
			done = true
		}()

		// The fake clock cannot advance while Wait is active, so returning here
		// proves the request is blocked in the one-hour retry wait.
		synctest.Wait()
		require.False(t, done)
		require.Equal(t, int32(1), calls.Load())

		cancel()
		synctest.Wait()
		require.True(t, done)
		require.NoError(t, resultErr)
		require.Equal(t, int32(1), calls.Load())
		require.Equal(t, uint32(moerr.ErrTNShardNotFound), resp.TxnError.Code)
	})
}

func TestRetryDoesNotRedispatchWhenContextAndTimerAreReady(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	req := txn.TxnRequest{Options: &txn.TxnRequestOptions{
		RetryCodes:    []int32{int32(moerr.ErrTNShardNotFound)},
		RetryInterval: -1,
	}}
	resp := &txn.TxnResponse{}
	var calls atomic.Int32

	err := (&store{}).handleWithRetry(ctx, &req, resp,
		func(context.Context, *txn.TxnRequest, *txn.TxnResponse) error {
			calls.Add(1)
			resp.TxnError = txn.WrapError(moerr.NewTNShardNotFoundNoCtx("tn", 1), 0)
			cancel()
			return nil
		})

	assert.NoError(t, err)
	assert.Equal(t, int32(1), calls.Load())
}

func TestStartedTNReplicaLifecycle(t *testing.T) {
	newStore := func() *store {
		return &store{cfg: &Config{UUID: "test"}, replicas: &sync.Map{}}
	}
	newRequest := func(shardID, replicaID uint64) txn.TxnRequest {
		req := service.NewTestReadRequest(1, service.NewTestTxn(1, 1, shardID), shardID)
		req.CNRequest.Target.ReplicaID = replicaID
		return req
	}
	newReadyReplica := func(t *testing.T, shardID, replicaID uint64) *replica {
		r := newReplica(newTestTNShard(shardID, replicaID, shardID), runtime.DefaultRuntime())
		require.True(t, r.reserveStart())
		r.mu.Lock()
		r.service = &closeTrackingTxnService{}
		r.mu.Unlock()
		r.finishStart(nil)
		return r
	}
	requireNotFound := func(t *testing.T, response *txn.TxnResponse) {
		require.NotNil(t, response.TxnError)
		require.Equal(t, uint32(moerr.ErrTNShardNotFound), response.TxnError.Code)
	}

	t.Run("ready", func(t *testing.T) {
		s := newStore()
		r := newReadyReplica(t, 1, 2)
		s.replicas.Store(uint64(1), r)
		req := newRequest(1, 2)

		got, err := s.acquireTNReplica(context.Background(), &req, &txn.TxnResponse{})
		require.NoError(t, err)
		require.NotNil(t, got)
		defer got.release()
		require.Same(t, r.service, got.service)
	})

	t.Run("wait then ready", func(t *testing.T) {
		s := newStore()
		r := newReplica(newTestTNShard(1, 2, 1), runtime.DefaultRuntime())
		require.True(t, r.reserveStart())
		s.replicas.Store(uint64(1), r)
		req := newRequest(1, 2)
		entered := make(chan struct{})
		result := make(chan *txnServiceLease, 1)
		errC := make(chan error, 1)
		go func() {
			close(entered)
			got, err := s.acquireTNReplica(context.Background(), &req, &txn.TxnResponse{})
			result <- got
			errC <- err
		}()

		<-entered
		r.mu.Lock()
		r.service = &closeTrackingTxnService{}
		r.mu.Unlock()
		r.finishStart(nil)
		lease := <-result
		require.NotNil(t, lease)
		defer lease.release()
		require.Same(t, r.service, lease.service)
		require.NoError(t, <-errC)
	})

	t.Run("caller cancel", func(t *testing.T) {
		s := newStore()
		r := newReplica(newTestTNShard(1, 2, 1), runtime.DefaultRuntime())
		require.True(t, r.reserveStart())
		s.replicas.Store(uint64(1), r)
		req := newRequest(1, 2)
		ctx, cancel := context.WithCancel(context.Background())
		entered := make(chan struct{})
		errC := make(chan error, 1)
		go func() {
			close(entered)
			_, err := s.acquireTNReplica(ctx, &req, &txn.TxnResponse{})
			errC <- err
		}()

		<-entered
		cancel()
		require.ErrorIs(t, <-errC, context.Canceled)
	})

	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "terminal start failure", err: errors.New("start failed")},
		{name: "terminal start cancellation", err: context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := newStore()
			r := newReplica(newTestTNShard(1, 2, 1), runtime.DefaultRuntime())
			require.True(t, r.reserveStart())
			r.finishStart(test.err)
			s.replicas.Store(uint64(1), r)
			req := newRequest(1, 2)
			response := &txn.TxnResponse{}

			got, err := s.acquireTNReplica(context.Background(), &req, response)
			require.NoError(t, err)
			require.Nil(t, got)
			requireNotFound(t, response)
		})
	}

	t.Run("remove and re-add generation", func(t *testing.T) {
		s := newStore()
		oldReplica := newReadyReplica(t, 1, 2)
		s.replicas.Store(uint64(1), oldReplica)
		oldRequest := newRequest(1, 2)
		got, err := s.acquireTNReplica(context.Background(), &oldRequest, &txn.TxnResponse{})
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Same(t, oldReplica.service, got.service)
		got.release()

		s.replicas.Delete(uint64(1))
		replacement := newReadyReplica(t, 1, 4)
		s.replicas.Store(uint64(1), replacement)
		oldResponse := &txn.TxnResponse{}
		got, err = s.acquireTNReplica(context.Background(), &oldRequest, oldResponse)
		require.NoError(t, err)
		require.Nil(t, got)
		requireNotFound(t, oldResponse)

		newRequest := newRequest(1, 4)
		got, err = s.acquireTNReplica(context.Background(), &newRequest, &txn.TxnResponse{})
		require.NoError(t, err)
		require.NotNil(t, got)
		defer got.release()
		require.Same(t, replacement.service, got.service)
	})
}

func TestHandleRead(t *testing.T) {
	runTNStoreTest(t, func(s *store) {
		shard := newTestTNShard(1, 2, 3)
		assert.NoError(t, s.StartTNReplica(shard))

		req := service.NewTestReadRequest(1, service.NewTestTxn(1, 1, 1), 1)
		req.CNRequest.Target.ReplicaID = 2
		assert.NoError(t, s.handleRead(context.Background(), &req, &txn.TxnResponse{}))
	})
}

func TestDoReadReturnsCallerCancellation(t *testing.T) {
	s := &store{cfg: &Config{UUID: "test"}, replicas: &sync.Map{}}
	shard := newTestTNShard(1, 2, 3)
	r := newReplica(shard, runtime.DefaultRuntime())
	require.NoError(t, r.start(&leaseCancelReadTxnService{
		shard:   shard,
		entered: make(chan struct{}),
	}))
	t.Cleanup(func() { require.NoError(t, r.close(false)) })
	s.replicas.Store(uint64(1), r)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := service.NewTestReadRequest(1, service.NewTestTxn(1, 1, 1), 1)
	req.CNRequest.Target.ReplicaID = 2
	resp := &txn.TxnResponse{}

	require.ErrorIs(t, s.doRead(ctx, &req, resp), context.Canceled)
	require.Nil(t, resp.TxnError)
}

func TestDoReadMapsLeaseCancellationToTNShardNotFound(t *testing.T) {
	s := &store{cfg: &Config{UUID: "test"}, replicas: &sync.Map{}}
	shard := newTestTNShard(1, 2, 3)
	entered := make(chan struct{})
	r := newReplica(shard, runtime.DefaultRuntime())
	require.NoError(t, r.start(&leaseCancelReadTxnService{
		shard:   shard,
		entered: entered,
	}))
	s.replicas.Store(uint64(1), r)

	req := service.NewTestReadRequest(1, service.NewTestTxn(1, 1, 1), 1)
	req.CNRequest.Target.ReplicaID = 2
	resp := &txn.TxnResponse{}
	done := make(chan error, 1)
	go func() {
		done <- s.doRead(context.Background(), &req, resp)
	}()

	select {
	case <-entered:
	case <-time.After(replicaTestTimeout):
		t.Fatal("Read did not enter the lease context wait")
	}
	closed := make(chan error, 1)
	go func() {
		closed <- r.close(false)
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(replicaTestTimeout):
		t.Fatal("doRead did not return after lease cancellation")
	}
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(replicaTestTimeout):
		t.Fatal("replica close did not drain")
	}
	require.NotNil(t, resp.TxnError)
	require.Equal(t, uint32(moerr.ErrTNShardNotFound), resp.TxnError.TxnErrCode)
}

func TestDoReadReturnsReadError(t *testing.T) {
	s := &store{cfg: &Config{UUID: "test"}, replicas: &sync.Map{}}
	shard := newTestTNShard(1, 2, 3)
	readErr := errors.New("read failed")
	r := newReplica(shard, runtime.DefaultRuntime())
	require.NoError(t, r.start(&leaseCancelReadTxnService{
		shard:   shard,
		readErr: readErr,
	}))
	t.Cleanup(func() { require.NoError(t, r.close(false)) })
	s.replicas.Store(uint64(1), r)

	req := service.NewTestReadRequest(1, service.NewTestTxn(1, 1, 1), 1)
	req.CNRequest.Target.ReplicaID = 2
	resp := &txn.TxnResponse{}

	require.ErrorIs(t, s.doRead(context.Background(), &req, resp), readErr)
	require.Nil(t, resp.TxnError)
}

func TestHandleReadWithRetry(t *testing.T) {
	runTNStoreTest(t, func(s *store) {
		req := service.NewTestReadRequest(1, service.NewTestTxn(1, 1, 1), 1)
		req.CNRequest.Target.ReplicaID = 2
		req.Options = &txn.TxnRequestOptions{
			RetryCodes: []int32{int32(moerr.ErrTNShardNotFound)},
		}
		go func() {
			time.Sleep(time.Second)
			shard := newTestTNShard(1, 2, 3)
			assert.NoError(t, s.StartTNReplica(shard))
		}()
		assert.NoError(t, s.handleRead(context.Background(), &req, &txn.TxnResponse{}))
	})
}

func TestHandleReadWithRetryWithTimeout(t *testing.T) {
	runTNStoreTest(t, func(s *store) {
		req := service.NewTestReadRequest(1, service.NewTestTxn(1, 1, 1), 1)
		req.CNRequest.Target.ReplicaID = 2
		req.Options = &txn.TxnRequestOptions{
			RetryCodes: []int32{int32(moerr.ErrTNShardNotFound)},
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		resp := &txn.TxnResponse{}
		assert.NoError(t, s.handleRead(ctx, &req, resp))
		assert.Equal(t, uint32(moerr.ErrTNShardNotFound), resp.TxnError.Code)
	})
}

func TestHandleWrite(t *testing.T) {
	runTNStoreTest(t, func(s *store) {
		shard := newTestTNShard(1, 2, 3)
		assert.NoError(t, s.StartTNReplica(shard))

		req := service.NewTestWriteRequest(1, service.NewTestTxn(1, 1, 1), 1)
		req.CNRequest.Target.ReplicaID = 2
		assert.NoError(t, s.handleWrite(context.Background(), &req, &txn.TxnResponse{}))
	})
}

func TestHandleCommit(t *testing.T) {
	runTNStoreTest(t, func(s *store) {
		shard := newTestTNShard(1, 2, 3)
		assert.NoError(t, s.StartTNReplica(shard))

		req := service.NewTestCommitRequest(service.NewTestTxn(1, 1, 1))
		req.Txn.TNShards[0].ReplicaID = 2
		assert.NoError(t, s.handleCommit(context.Background(), &req, &txn.TxnResponse{}))
	})
}

func TestHandleRollback(t *testing.T) {
	runTNStoreTest(t, func(s *store) {
		shard := newTestTNShard(1, 2, 3)
		assert.NoError(t, s.StartTNReplica(shard))

		req := service.NewTestRollbackRequest(service.NewTestTxn(1, 1, 1))
		req.Txn.TNShards[0].ReplicaID = 2
		assert.NoError(t, s.handleRollback(context.Background(), &req, &txn.TxnResponse{}))
	})
}

func TestHandlePrepare(t *testing.T) {
	runTNStoreTest(t, func(s *store) {
		shard := newTestTNShard(1, 2, 3)
		assert.NoError(t, s.StartTNReplica(shard))

		req := service.NewTestPrepareRequest(service.NewTestTxn(1, 1, 1), 1)
		req.PrepareRequest.TNShard.ReplicaID = 2
		assert.NoError(t, s.handlePrepare(context.Background(), &req, &txn.TxnResponse{}))

		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, err := s.sender.Send(ctx, []txn.TxnRequest{req})
		assert.NoError(t, err)
	})
}

func TestHandleGetStatus(t *testing.T) {
	runTNStoreTest(t, func(s *store) {
		shard := newTestTNShard(1, 2, 3)
		assert.NoError(t, s.StartTNReplica(shard))

		req := service.NewTestGetStatusRequest(service.NewTestTxn(1, 1, 1), 1)
		req.GetStatusRequest.TNShard.ReplicaID = 2
		assert.NoError(t, s.handleGetStatus(context.Background(), &req, &txn.TxnResponse{}))

		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, err := s.sender.Send(ctx, []txn.TxnRequest{req})
		assert.NoError(t, err)
	})
}

func TestHandleCommitTNShard(t *testing.T) {
	runTNStoreTest(t, func(s *store) {
		shard := newTestTNShard(1, 2, 3)
		assert.NoError(t, s.StartTNReplica(shard))

		req := service.NewTestCommitShardRequest(service.NewTestTxn(1, 1, 1))
		req.CommitTNShardRequest.TNShard.ReplicaID = 2
		assert.NoError(t, s.handleCommitTNShard(context.Background(), &req, &txn.TxnResponse{}))

		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, err := s.sender.Send(ctx, []txn.TxnRequest{req})
		assert.NoError(t, err)
	})
}

func TestHandleRollbackTNShard(t *testing.T) {
	runTNStoreTest(t, func(s *store) {
		shard := newTestTNShard(1, 2, 3)
		assert.NoError(t, s.StartTNReplica(shard))

		req := service.NewTestRollbackShardRequest(service.NewTestTxn(1, 1, 1))
		req.RollbackTNShardRequest.TNShard.ReplicaID = 2
		assert.NoError(t, s.handleRollbackTNShard(context.Background(), &req, &txn.TxnResponse{}))

		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, err := s.sender.Send(ctx, []txn.TxnRequest{req})
		assert.NoError(t, err)
	})
}

func TestHandleTNShartnotFound(t *testing.T) {
	runTNStoreTest(t, func(s *store) {
		req := service.NewTestRollbackShardRequest(service.NewTestTxn(1, 1, 1))
		resp := &txn.TxnResponse{}
		assert.NoError(t, s.handleRollbackTNShard(context.Background(), &req, resp))
		assert.Equal(t, uint32(moerr.ErrTNShardNotFound), resp.TxnError.Code)
	})
}

func TestHandleDebug(t *testing.T) {
	runTNStoreTest(t, func(s *store) {
		shard := newTestTNShard(1, 2, 3)
		assert.NoError(t, s.StartTNReplica(shard))

		req := service.NewTestReadRequest(1, service.NewTestTxn(1, 1, 1), 1)
		req.Method = txn.TxnMethod_DEBUG
		req.CNRequest.Target.ReplicaID = 2
		assert.NoError(t, s.handleDebug(context.Background(), &req, &txn.TxnResponse{}))
	})
}
