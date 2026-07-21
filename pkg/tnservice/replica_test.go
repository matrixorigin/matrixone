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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/service"
	"github.com/matrixorigin/matrixone/pkg/txn/storage"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/mem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const replicaTestTimeout = 30 * time.Second

type startErrorStorage struct {
	storage.TxnStorage
	startErr     error
	closeErr     error
	destroyErr   error
	closeCalls   int
	destroyCalls int
}

type closeUnblocksStartTxnService struct {
	service.TxnService
	started   chan struct{}
	closed    chan struct{}
	startOnce sync.Once
	closeOnce sync.Once
}

type signalingRecoveryCluster struct {
	clusterservice.MOCluster
	entered chan struct{}
	once    sync.Once
}

func (c *signalingRecoveryCluster) GetAllTNServices() []metadata.TNService {
	c.once.Do(func() { close(c.entered) })
	return c.MOCluster.GetAllTNServices()
}

func (s *closeUnblocksStartTxnService) Start() error {
	s.startOnce.Do(func() { close(s.started) })
	<-s.closed
	return context.Canceled
}

func (s *closeUnblocksStartTxnService) CancelRecovery() {
	s.closeOnce.Do(func() { close(s.closed) })
}

func (s *closeUnblocksStartTxnService) Close(bool) error {
	return nil
}

type closeTrackingTxnService struct {
	service.TxnService
	closeCalls int
}

func (s *closeTrackingTxnService) Close(destroy bool) error {
	s.closeCalls++
	return s.TxnService.Close(destroy)
}

func (s *startErrorStorage) Start() error {
	return s.startErr
}

func (s *startErrorStorage) Close(context.Context) error {
	s.closeCalls++
	return s.closeErr
}

func (s *startErrorStorage) Destroy(context.Context) error {
	s.destroyCalls++
	return s.destroyErr
}

func TestNewReplica(t *testing.T) {
	r := newReplica(newTestTNShard(1, 2, 3), runtime.DefaultRuntime())
	select {
	case <-r.startedC:
		assert.Fail(t, "cannot started")
	default:
	}
}

func TestCloseNotStartedReplica(t *testing.T) {
	r := newReplica(newTestTNShard(1, 2, 3), runtime.DefaultRuntime())
	assert.NoError(t, r.close(false))
}

func TestCloseStartedReplicaIsIdempotent(t *testing.T) {
	r := newReplica(newTestTNShard(1, 2, 3), runtime.DefaultRuntime())
	sender := service.NewTestSender()
	t.Cleanup(func() { require.NoError(t, sender.Close()) })
	base := service.NewTestTxnService(t, 1, sender, service.NewTestClock(1))
	txnService := &closeTrackingTxnService{TxnService: base}
	require.NoError(t, r.start(txnService))

	require.NoError(t, r.close(false))
	require.NoError(t, r.close(true))
	require.Equal(t, 1, txnService.closeCalls)
}

func TestCloseStartedReplicaCancelsAndDrainsActiveCalls(t *testing.T) {
	r := newReplica(newTestTNShard(1, 2, 3), runtime.DefaultRuntime())
	sender := service.NewTestSender()
	t.Cleanup(func() { require.NoError(t, sender.Close()) })
	txnService := &closeTrackingTxnService{
		TxnService: service.NewTestTxnService(t, 1, sender, service.NewTestClock(1)),
	}
	require.NoError(t, r.start(txnService))

	lease, err := r.acquireService(context.Background())
	require.NoError(t, err)
	closed := make(chan error, 1)
	go func() {
		closed <- r.close(false)
	}()

	select {
	case <-lease.ctx.Done():
		require.ErrorIs(t, context.Cause(lease.ctx), context.Canceled)
	case <-time.After(replicaTestTimeout):
		t.Fatal("active call context was not canceled")
	}
	select {
	case err := <-closed:
		t.Fatalf("replica closed before active call was released: %v", err)
	default:
	}
	nestedDone := make(chan error, 1)
	go func() {
		_, err := r.acquireService(context.Background())
		nestedDone <- err
	}()
	select {
	case err := <-nestedDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(replicaTestTimeout):
		t.Fatal("nested local acquire blocked while close was draining")
	}

	lease.release()
	lease.release()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(replicaTestTimeout):
		t.Fatal("replica close did not drain")
	}
	require.Equal(t, 1, txnService.closeCalls)

	_, err = r.acquireService(context.Background())
	require.ErrorIs(t, err, context.Canceled)
}

func TestAcquireServiceRejectsCanceledCaller(t *testing.T) {
	r := newReplica(newTestTNShard(1, 2, 3), runtime.DefaultRuntime())
	sender := service.NewTestSender()
	t.Cleanup(func() { require.NoError(t, sender.Close()) })
	txnService := service.NewTestTxnService(t, 1, sender, service.NewTestClock(1))
	require.NoError(t, r.start(txnService))
	t.Cleanup(func() { require.NoError(t, r.close(false)) })

	cause := errors.New("caller canceled")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(cause)
	for range 100 {
		lease, err := r.acquireService(ctx)
		require.Nil(t, lease)
		require.ErrorIs(t, err, cause)
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	require.Zero(t, r.mu.activeCalls)
}

func TestCloseFailedStartReplica(t *testing.T) {
	startErr := errors.New("storage start failed")
	runtime.SetupServiceBasedRuntime("test", runtime.DefaultRuntime())
	for _, test := range []struct {
		name    string
		destroy bool
	}{
		{name: "close", destroy: false},
		{name: "destroy", destroy: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			cleanupErr := errors.New("storage cleanup failed")
			storage := &startErrorStorage{startErr: startErr}
			if test.destroy {
				storage.destroyErr = cleanupErr
			} else {
				storage.closeErr = cleanupErr
			}
			txnService := service.NewTxnService(
				"test", newTestTNShard(1, 2, 3), storage, service.NewTestSender(), time.Hour, nil)
			r := newReplica(newTestTNShard(1, 2, 3), runtime.DefaultRuntime())

			require.ErrorIs(t, r.start(txnService), startErr)
			closed := make(chan error, 1)
			go func() {
				closed <- r.close(test.destroy)
			}()

			select {
			case err := <-closed:
				require.ErrorIs(t, err, startErr)
				require.ErrorIs(t, err, cleanupErr)
				if test.destroy {
					require.Equal(t, 0, storage.closeCalls)
					require.Equal(t, 1, storage.destroyCalls)
				} else {
					require.Equal(t, 1, storage.closeCalls)
					require.Equal(t, 0, storage.destroyCalls)
				}
			case <-time.After(time.Second):
				t.Fatal("close hung after failed start")
			}
		})
	}
}

func TestCloseCancelsBlockedReplicaStart(t *testing.T) {
	txnService := &closeUnblocksStartTxnService{
		started: make(chan struct{}),
		closed:  make(chan struct{}),
	}
	r := newReplica(newTestTNShard(1, 2, 3), runtime.DefaultRuntime())
	startResult := make(chan error, 1)
	go func() {
		startResult <- r.start(txnService)
	}()

	select {
	case <-txnService.started:
	case <-time.After(time.Second):
		t.Fatal("replica start did not begin")
	}

	closeResult := make(chan error, 1)
	go func() {
		closeResult <- r.close(false)
	}()

	select {
	case err := <-closeResult:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("close did not cancel blocked replica start")
	}
	require.ErrorIs(t, <-startResult, context.Canceled)
}

func TestCloseCancelsReplicaBlockedInRecovery(t *testing.T) {
	meta := service.NewTestTxn(1, 1, 1)
	meta.Status = txn.TxnStatus_Prepared
	meta.PreparedTS = service.NewTestTimestamp(2)
	meta.TNShards = append(meta.TNShards, metadata.TNShard{
		TNShardRecord: metadata.TNShardRecord{ShardID: 99},
	})
	mlog := mem.NewMemLog()
	data := (&mem.KVLog{Txn: meta}).MustMarshal()
	record := mlog.GetLogRecord(len(data))
	record.Type = logpb.UserRecord
	record.Data = data
	_, err := mlog.Append(context.Background(), record)
	require.NoError(t, err)

	sender := service.NewTestSender()
	t.Cleanup(func() { require.NoError(t, sender.Close()) })
	txnService := service.NewTestTxnServiceWithLog(
		t, 1, sender, service.NewTestClock(0), mlog)
	baseCluster := clusterservice.NewMOCluster(
		"dn-uuid", nil, time.Hour,
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices(nil, nil),
	)
	t.Cleanup(baseCluster.Close)
	cluster := &signalingRecoveryCluster{
		MOCluster: baseCluster,
		entered:   make(chan struct{}),
	}
	runtime.ServiceRuntime("dn-uuid").SetGlobalVariables(runtime.ClusterService, cluster)

	r := newReplica(newTestTNShard(1, 2, 3), runtime.DefaultRuntime())
	startResult := make(chan error, 1)
	go func() { startResult <- r.start(txnService) }()
	select {
	case <-cluster.entered:
	case <-time.After(time.Second):
		t.Fatal("recovery did not reach the missing participant route wait")
	}

	closeResult := make(chan error, 1)
	go func() { closeResult <- r.close(false) }()
	select {
	case err := <-closeResult:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("replica close did not cancel real transaction recovery")
	}
	select {
	case err := <-startResult:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("replica start remained blocked after recovery cancellation")
	}
}

func TestWaitStarted(t *testing.T) {
	r := newReplica(newTestTNShard(1, 2, 3), runtime.DefaultRuntime())
	c := make(chan struct{})
	go func() {
		assert.NoError(t, r.waitStarted(context.Background()))
		c <- struct{}{}
	}()

	ts := service.NewTestTxnService(t, 1, service.NewTestSender(), service.NewTestClock(1))
	defer func() {
		assert.NoError(t, ts.Close(false))
	}()

	assert.NoError(t, r.start(ts))
	defer func() {
		assert.NoError(t, r.close(false))
	}()
	select {
	case <-c:
	case <-time.After(time.Minute):
		assert.Fail(t, "wait started failed")
	}
}

func TestHandleLocalCNRequestsWillReturnError(t *testing.T) {
	r := newReplica(newTestTNShard(1, 2, 3), runtime.DefaultRuntime())
	ts := service.NewTestTxnService(t, 1, service.NewTestSender(), service.NewTestClock(1))
	assert.NoError(t, r.start(ts))
	defer func() {
		assert.NoError(t, r.close(false))
		assert.NoError(t, ts.Close(false))
	}()

	req := service.NewTestReadRequest(1, txn.TxnMeta{}, 1)
	assert.Error(t, r.handleLocalRequest(context.Background(), &req, &txn.TxnResponse{}))
}
