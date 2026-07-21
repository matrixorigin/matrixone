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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/mem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recoveryRouteSender struct {
	mu       sync.Mutex
	requests [][]txn.TxnRequest
	notifyC  chan struct{}
	block    bool
}

func newRecoveryRouteSender() *recoveryRouteSender {
	return &recoveryRouteSender{notifyC: make(chan struct{}, 1)}
}

func (s *recoveryRouteSender) Send(ctx context.Context, requests []txn.TxnRequest) (*rpc.SendResult, error) {
	copied := append([]txn.TxnRequest(nil), requests...)
	s.mu.Lock()
	s.requests = append(s.requests, copied)
	s.mu.Unlock()
	responses := make([]txn.TxnResponse, len(requests))
	for i := range responses {
		meta := requests[i].Txn
		meta.Status = txn.TxnStatus_Aborted
		responses[i].Txn = &meta
	}
	select {
	case s.notifyC <- struct{}{}:
	default:
	}
	if s.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &rpc.SendResult{Responses: responses}, nil
}

func (s *recoveryRouteSender) Close() error { return nil }

func installRecoveryCluster(t *testing.T, sid string, services ...metadata.TNService) {
	t.Helper()
	c := clusterservice.NewMOCluster(
		sid,
		nil,
		time.Hour,
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices(nil, services),
	)
	t.Cleanup(c.Close)
	runtime.ServiceRuntime(sid).SetGlobalVariables(runtime.ClusterService, c)
}

func installRecoveryRoutesForServices(t *testing.T, services ...*service) {
	t.Helper()
	require.NotEmpty(t, services)
	tnServices := make([]metadata.TNService, 0, len(services))
	for _, service := range services {
		tnServices = append(tnServices, metadata.TNService{
			TxnServiceAddress: service.shard.Address,
			Shards:            []metadata.TNShard{service.shard},
		})
	}
	installRecoveryCluster(t, services[0].sid, tnServices...)
}

type recoveryClusterClient struct {
	mu      sync.RWMutex
	details logpb.ClusterDetails
}

type staleRecoveryCluster struct {
	clusterservice.MOCluster
	mu           sync.Mutex
	stale        []metadata.TNService
	current      []metadata.TNService
	refreshed    bool
	refreshCalls int
	refreshFails int
}

func (c *staleRecoveryCluster) GetAllTNServices() []metadata.TNService {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.refreshed {
		return c.current
	}
	return c.stale
}

func (c *staleRecoveryCluster) Refresh(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.refreshCalls++
	if c.refreshFails > 0 {
		c.refreshFails--
		return errors.New("transient refresh failure")
	}
	c.refreshed = true
	return nil
}

func (c *recoveryClusterClient) GetClusterDetails(context.Context) (logpb.ClusterDetails, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.details, nil
}

func (c *recoveryClusterClient) setTNRoute(shardID, replicaID uint64, address string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.details.TNStores = []logpb.TNStore{{
		UUID:           "tn",
		ServiceAddress: address,
		Shards: []logpb.TNShardInfo{{
			ShardID:   shardID,
			ReplicaID: replicaID,
		}},
	}}
}

func TestRecoveryRestoresRemoteParticipantRoutes(t *testing.T) {
	sender := newRecoveryRouteSender()
	s := NewTestTxnServiceWithLog(t, 1, sender, NewTestClock(0), nil).(*service)
	defer s.stopper.Stop()
	remote := metadata.TNShard{
		TNShardRecord: metadata.TNShardRecord{ShardID: 2},
		ReplicaID:     200,
	}
	installRecoveryCluster(t, s.sid, metadata.TNService{
		TxnServiceAddress: "tn-2",
		Shards:            []metadata.TNShard{remote},
	})

	meta := NewTestTxn(1, 1, 1)
	meta.Status = txn.TxnStatus_Prepared
	meta.PreparedTS = NewTestTimestamp(2)
	meta.TNShards = append(meta.TNShards, metadata.TNShard{
		TNShardRecord: metadata.TNShardRecord{ShardID: 2, LogShardID: 20},
	})

	require.NoError(t, s.stopper.RunTask(s.doRecovery))
	s.txnC <- meta
	close(s.txnC)
	s.waitRecoveryCompleted()

	select {
	case <-sender.notifyC:
	case <-time.After(time.Second):
		t.Fatal("recovery did not query the remote participant")
	}
	sender.mu.Lock()
	defer sender.mu.Unlock()
	require.NotEmpty(t, sender.requests)
	require.Len(t, sender.requests[0], 1)
	assert.Equal(t, metadata.TNShard{
		TNShardRecord: metadata.TNShardRecord{ShardID: 2, LogShardID: 20},
		ReplicaID:     200,
		Address:       "tn-2",
	}, sender.requests[0][0].GetTargetTN())
}

func TestRecoveryRefreshesStaleParticipantRoutes(t *testing.T) {
	for _, test := range []struct {
		name            string
		persistedRoute  metadata.TNShard
		refreshFails    int
		expectRefreshes int
	}{
		{
			name:            "incomplete persisted route with complete stale cache",
			expectRefreshes: 1,
			persistedRoute: metadata.TNShard{
				TNShardRecord: metadata.TNShardRecord{ShardID: 2},
			},
		},
		{
			name:            "complete stale persisted route",
			expectRefreshes: 1,
			persistedRoute: metadata.TNShard{
				TNShardRecord: metadata.TNShardRecord{ShardID: 2},
				ReplicaID:     100,
				Address:       "old-tn-2",
			},
		},
		{
			name:            "transient authoritative refresh failure",
			refreshFails:    1,
			expectRefreshes: 2,
			persistedRoute: metadata.TNShard{
				TNShardRecord: metadata.TNShardRecord{ShardID: 2},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			sender := newRecoveryRouteSender()
			s := NewTestTxnServiceWithLog(t, 1, sender, NewTestClock(0), nil).(*service)
			defer s.stopper.Stop()
			cluster := &staleRecoveryCluster{
				stale: []metadata.TNService{{
					TxnServiceAddress: "old-tn-2",
					Shards: []metadata.TNShard{{
						TNShardRecord: metadata.TNShardRecord{ShardID: 2},
						ReplicaID:     100,
					}},
				}},
				current: []metadata.TNService{{
					TxnServiceAddress: "current-tn-2",
					Shards: []metadata.TNShard{{
						TNShardRecord: metadata.TNShardRecord{ShardID: 2},
						ReplicaID:     200,
					}},
				}},
				refreshFails: test.refreshFails,
			}
			runtime.ServiceRuntime(s.sid).SetGlobalVariables(runtime.ClusterService, cluster)

			meta := NewTestTxn(1, 1, 1)
			meta.Status = txn.TxnStatus_Prepared
			meta.PreparedTS = NewTestTimestamp(2)
			meta.TNShards = append(meta.TNShards, test.persistedRoute)
			require.NoError(t, s.stopper.RunTask(s.doRecovery))
			s.txnC <- meta
			close(s.txnC)
			s.waitRecoveryCompleted()

			select {
			case <-sender.notifyC:
			case <-time.After(time.Second):
				t.Fatal("recovery did not query the remote participant")
			}
			sender.mu.Lock()
			require.NotEmpty(t, sender.requests)
			assert.Equal(t, uint64(200), sender.requests[0][0].GetTargetTN().ReplicaID)
			assert.Equal(t, "current-tn-2", sender.requests[0][0].GetTargetTN().Address)
			sender.mu.Unlock()

			cluster.mu.Lock()
			assert.Equal(t, test.expectRefreshes, cluster.refreshCalls)
			cluster.mu.Unlock()
		})
	}
}

func TestRecoveryRestoresRoutesBeforeCommitTNShard(t *testing.T) {
	sender := newRecoveryRouteSender()
	sender.block = true
	s := NewTestTxnServiceWithLog(t, 1, sender, NewTestClock(0), nil).(*service)
	remote := metadata.TNShard{
		TNShardRecord: metadata.TNShardRecord{ShardID: 2},
		ReplicaID:     200,
	}
	installRecoveryCluster(t, s.sid, metadata.TNService{
		TxnServiceAddress: "tn-2",
		Shards:            []metadata.TNShard{remote},
	})

	meta := NewTestTxn(1, 1, 1)
	meta.Status = txn.TxnStatus_Committing
	meta.PreparedTS = NewTestTimestamp(2)
	meta.CommitTS = NewTestTimestamp(3)
	meta.TNShards = append(meta.TNShards, metadata.TNShard{
		TNShardRecord: metadata.TNShardRecord{ShardID: 2},
	})

	require.NoError(t, s.stopper.RunTask(s.doRecovery))
	s.txnC <- meta
	close(s.txnC)
	s.waitRecoveryCompleted()

	select {
	case <-sender.notifyC:
	case <-time.After(time.Second):
		t.Fatal("recovery did not send the commit request")
	}
	sender.mu.Lock()
	require.NotEmpty(t, sender.requests)
	require.Len(t, sender.requests[0], 1)
	assert.Equal(t, txn.TxnMethod_CommitTNShard, sender.requests[0][0].Method)
	assert.Equal(t, metadata.TNShard{
		TNShardRecord: metadata.TNShardRecord{ShardID: 2},
		ReplicaID:     200,
		Address:       "tn-2",
	}, sender.requests[0][0].GetTargetTN())
	sender.mu.Unlock()

	require.NoError(t, s.Close(false))
}

func TestRecoveryWaitsUntilParticipantRouteAppears(t *testing.T) {
	sender := newRecoveryRouteSender()
	s := NewTestTxnServiceWithLog(t, 1, sender, NewTestClock(0), nil).(*service)
	defer s.stopper.Stop()
	client := &recoveryClusterClient{}
	c := clusterservice.NewMOCluster(s.sid, client, 50*time.Millisecond)
	t.Cleanup(c.Close)
	runtime.ServiceRuntime(s.sid).SetGlobalVariables(runtime.ClusterService, c)

	meta := NewTestTxn(1, 1, 1)
	meta.Status = txn.TxnStatus_Prepared
	meta.PreparedTS = NewTestTimestamp(2)
	meta.TNShards = append(meta.TNShards, metadata.TNShard{
		TNShardRecord: metadata.TNShardRecord{ShardID: 2},
	})
	require.NoError(t, s.stopper.RunTask(s.doRecovery))
	s.txnC <- meta
	close(s.txnC)

	select {
	case <-sender.notifyC:
		t.Fatal("recovery sent a request before the participant route was available")
	case <-time.After(150 * time.Millisecond):
	}
	client.setTNRoute(2, 200, "tn-2")
	s.waitRecoveryCompleted()

	select {
	case <-sender.notifyC:
	case <-time.After(time.Second):
		t.Fatal("recovery did not resume after the participant route appeared")
	}
	sender.mu.Lock()
	defer sender.mu.Unlock()
	require.NotEmpty(t, sender.requests)
	assert.Equal(t, "tn-2", sender.requests[0][0].GetTargetTN().Address)
}

func TestRecoveryMissingParticipantRouteStopsWhenCanceled(t *testing.T) {
	sender := newRecoveryRouteSender()
	meta := NewTestTxn(1, 1, 1)
	meta.Status = txn.TxnStatus_Prepared
	meta.PreparedTS = NewTestTimestamp(2)
	meta.TNShards = append(meta.TNShards, metadata.TNShard{
		TNShardRecord: metadata.TNShardRecord{ShardID: 99},
	})
	mlog := mem.NewMemLog()
	addLog(t, mlog, meta, 1)
	s := NewTestTxnServiceWithLog(t, 1, sender, NewTestClock(0), mlog).(*service)
	installRecoveryCluster(t, s.sid)

	started := make(chan error, 1)
	go func() { started <- s.Start() }()
	select {
	case <-s.recoveryC:
		t.Fatal("recovery silently completed with an unresolved participant")
	case <-time.After(100 * time.Millisecond):
	}

	closed := make(chan error, 1)
	go func() { closed <- s.Close(false) }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Close deadlocked while recovery waited for cluster metadata")
	}
	select {
	case err := <-started:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Start remained blocked after Close canceled recovery")
	}

	sender.mu.Lock()
	defer sender.mu.Unlock()
	assert.Empty(t, sender.requests, "must not send to an unresolved or empty address")
}

func TestRecoveryRetriesTransientClusterLookupError(t *testing.T) {
	sender := newRecoveryRouteSender()
	s := NewTestTxnServiceWithLog(t, 1, sender, NewTestClock(0), nil).(*service)
	defer s.stopper.Stop()
	runtime.ServiceRuntime(s.sid).SetGlobalVariables(runtime.ClusterService, "not-a-cluster")

	meta := NewTestTxn(1, 1, 1)
	meta.Status = txn.TxnStatus_Prepared
	meta.PreparedTS = NewTestTimestamp(2)
	meta.TNShards = append(meta.TNShards, metadata.TNShard{
		TNShardRecord: metadata.TNShardRecord{ShardID: 2},
	})
	require.NoError(t, s.stopper.RunTask(s.doRecovery))
	s.txnC <- meta
	close(s.txnC)
	select {
	case <-s.recoveryC:
		t.Fatal("recovery silently completed after a transient cluster lookup error")
	case <-time.After(100 * time.Millisecond):
	}

	installRecoveryCluster(t, s.sid, metadata.TNService{
		TxnServiceAddress: "tn-2",
		Shards: []metadata.TNShard{{
			TNShardRecord: metadata.TNShardRecord{ShardID: 2},
			ReplicaID:     200,
		}},
	})
	s.waitRecoveryCompleted()
	select {
	case <-sender.notifyC:
	case <-time.After(time.Second):
		t.Fatal("recovery did not resume after cluster lookup recovered")
	}
	sender.mu.Lock()
	defer sender.mu.Unlock()
	require.NotEmpty(t, sender.requests)
	assert.Equal(t, "tn-2", sender.requests[0][0].GetTargetTN().Address)
}

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

func TestRecoveryFromMultiTNShardWithAllPrepared(t *testing.T) {
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
	installRecoveryRoutesForServices(t, s1, s2)
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

func TestRecoveryFromMultiTNShardWithAnyNotPrepared(t *testing.T) {
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
	installRecoveryRoutesForServices(t, s1, s2)
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

func TestRecoveryFromMultiTNShardWithCommitting(t *testing.T) {
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
	installRecoveryRoutesForServices(t, s1, s2)
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

func TestRecoveryEndDoesNotPanicWhenStopperUnavailable(t *testing.T) {
	for _, status := range []txn.TxnStatus{
		txn.TxnStatus_Prepared,
		txn.TxnStatus_Committing,
	} {
		t.Run(status.String(), func(t *testing.T) {
			sender := NewTestSender()
			defer func() {
				assert.NoError(t, sender.Close())
			}()

			s := NewTestTxnServiceWithLog(t, 1, sender, NewTestClock(0), nil).(*service)
			s.stopper.Stop()

			wTxn := NewTestTxn(1, 1, 1, 2)
			wTxn.Status = status
			wTxn.PreparedTS = NewTestTimestamp(2)
			wTxn.CommitTS = NewTestTimestamp(2)
			s.maybeAddTxn(wTxn)

			assert.NotPanics(t, s.end)
			select {
			case <-s.recoveryC:
			case <-time.After(time.Second):
				t.Fatal("recovery end did not close recovery channel")
			}

			assert.NotNil(t, s.getTxnContext(wTxn.ID))
		})
	}
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
