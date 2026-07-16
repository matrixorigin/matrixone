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
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logservice"
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
	return &recoveryRouteSender{notifyC: make(chan struct{}, 4)}
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

func (s *recoveryRouteSender) firstRequests(t *testing.T) []txn.TxnRequest {
	t.Helper()
	select {
	case <-s.notifyC:
	case <-time.After(time.Second):
		t.Fatal("recovery did not send requests")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]txn.TxnRequest(nil), s.requests[0]...)
}

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

func TestRecoveryCompletesRemoteParticipantRoutesBeforeGetStatus(t *testing.T) {
	sender := newRecoveryRouteSender()
	s := NewTestTxnServiceWithLog(t, 1, sender, NewTestClock(0), nil).(*service)
	defer s.stopper.Stop()

	remote2 := metadata.TNShard{
		// Dynamic HAKeeper TN snapshots do not carry LogShardID.
		TNShardRecord: metadata.TNShardRecord{ShardID: 2},
		ReplicaID:     200,
	}
	remote3 := metadata.TNShard{
		TNShardRecord: metadata.TNShardRecord{ShardID: 3, LogShardID: 30},
		ReplicaID:     300,
	}
	installRecoveryCluster(t, s.sid,
		metadata.TNService{TxnServiceAddress: "tn-2", Shards: []metadata.TNShard{remote2}},
		metadata.TNService{TxnServiceAddress: "tn-3", Shards: []metadata.TNShard{remote3}},
	)

	meta := NewTestTxn(1, 1, 1)
	meta.Status = txn.TxnStatus_Prepared
	meta.PreparedTS = NewTestTimestamp(2)
	meta.TNShards = append(meta.TNShards,
		metadata.TNShard{TNShardRecord: metadata.TNShardRecord{ShardID: 2}},
		metadata.TNShard{TNShardRecord: metadata.TNShardRecord{ShardID: 3}},
	)

	require.NoError(t, s.stopper.RunTask(s.doRecovery))
	s.txnC <- meta
	close(s.txnC)
	s.waitRecoveryCompleted()

	requests := sender.firstRequests(t)
	require.Len(t, requests, 2)
	assert.Equal(t, []uint64{2, 3}, []uint64{
		requests[0].GetTargetTN().ShardID,
		requests[1].GetTargetTN().ShardID,
	})
	assert.Equal(t, metadata.TNShard{
		TNShardRecord: metadata.TNShardRecord{ShardID: 2},
		ReplicaID:     200,
		Address:       "tn-2",
	}, requests[0].GetTargetTN())
	assert.Equal(t, metadata.TNShard{
		TNShardRecord: metadata.TNShardRecord{ShardID: 3, LogShardID: 30},
		ReplicaID:     300,
		Address:       "tn-3",
	}, requests[1].GetTargetTN())
	for _, request := range requests {
		require.Len(t, request.Txn.TNShards, 3)
		assert.Equal(t, s.shard, request.Txn.TNShards[0])
		assert.NotEmpty(t, request.GetTargetTN().Address)
	}
}

func TestRecoveryCompletesRemoteParticipantRoutesBeforeCommitTNShard(t *testing.T) {
	sender := newRecoveryRouteSender()
	sender.block = true
	s := NewTestTxnServiceWithLog(t, 1, sender, NewTestClock(0), nil).(*service)

	remote := metadata.TNShard{
		TNShardRecord: metadata.TNShardRecord{ShardID: 2},
		ReplicaID:     200,
	}
	installRecoveryCluster(t, s.sid,
		metadata.TNService{TxnServiceAddress: "tn-2", Shards: []metadata.TNShard{remote}},
	)

	meta := NewTestTxn(1, 1, 1)
	meta.Status = txn.TxnStatus_Committing
	meta.PreparedTS = NewTestTimestamp(2)
	meta.CommitTS = NewTestTimestamp(3)
	meta.TNShards = append(meta.TNShards,
		metadata.TNShard{TNShardRecord: metadata.TNShardRecord{ShardID: 2}},
	)

	require.NoError(t, s.stopper.RunTask(s.doRecovery))
	s.txnC <- meta
	close(s.txnC)
	s.waitRecoveryCompleted()

	requests := sender.firstRequests(t)
	require.Len(t, requests, 1)
	assert.Equal(t, txn.TxnMethod_CommitTNShard, requests[0].Method)
	assert.Equal(t, metadata.TNShard{
		TNShardRecord: metadata.TNShardRecord{ShardID: 2},
		ReplicaID:     200,
		Address:       "tn-2",
	}, requests[0].GetTargetTN())
	require.Len(t, requests[0].Txn.TNShards, 2)
	assert.Equal(t, s.shard, requests[0].Txn.TNShards[0])

	require.NoError(t, s.Close(false))
}

func TestRecoveryMissingParticipantRouteWaitsUntilCloseCancels(t *testing.T) {
	sender := newRecoveryRouteSender()
	meta := NewTestTxn(1, 1, 1)
	meta.Status = txn.TxnStatus_Prepared
	meta.PreparedTS = NewTestTimestamp(2)
	meta.TNShards = append(meta.TNShards,
		metadata.TNShard{TNShardRecord: metadata.TNShardRecord{ShardID: 99}},
	)
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

	closed := make(chan struct{})
	go func() {
		_ = s.Close(false)
		close(closed)
	}()
	select {
	case <-closed:
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
	meta.TNShards = append(meta.TNShards,
		metadata.TNShard{TNShardRecord: metadata.TNShardRecord{ShardID: 2}},
	)
	require.NoError(t, s.stopper.RunTask(s.doRecovery))
	s.txnC <- meta
	close(s.txnC)
	select {
	case <-s.recoveryC:
		t.Fatal("recovery silently completed after a transient cluster lookup error")
	case <-time.After(100 * time.Millisecond):
	}

	remote := metadata.TNShard{
		TNShardRecord: metadata.TNShardRecord{ShardID: 2, LogShardID: 20},
		ReplicaID:     200,
	}
	installRecoveryCluster(t, s.sid,
		metadata.TNService{TxnServiceAddress: "tn-2", Shards: []metadata.TNShard{remote}},
	)
	s.waitRecoveryCompleted()
	requests := sender.firstRequests(t)
	require.Len(t, requests, 1)
	assert.Equal(t, "tn-2", requests[0].GetTargetTN().Address)
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
	mlog3 := mem.NewMemLog()

	coordinatorTxn := NewTestTxn(1, 1, 1, 2, 3)
	coordinatorTxn.Status = txn.TxnStatus_Prepared
	coordinatorTxn.PreparedTS = NewTestTimestamp(10)
	remoteTxn2 := coordinatorTxn
	remoteTxn2.PreparedTS = NewTestTimestamp(20)
	remoteTxn3 := coordinatorTxn
	remoteTxn3.PreparedTS = NewTestTimestamp(15)

	addLog(t, mlog1, coordinatorTxn, 1)
	addLog(t, mlog2, remoteTxn2, 2)
	addLog(t, mlog3, remoteTxn3, 3)

	sender := NewTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()
	var mu sync.Mutex
	var commitRequests []txn.TxnRequest
	sender.setFilter(func(request *txn.TxnRequest) bool {
		if request.Method == txn.TxnMethod_CommitTNShard {
			mu.Lock()
			commitRequests = append(commitRequests, *request)
			mu.Unlock()
		}
		return true
	})

	s1 := NewTestTxnServiceWithLog(t, 1, sender, NewTestClock(0), mlog1).(*service)
	s2 := NewTestTxnServiceWithLog(t, 2, sender, NewTestClock(0), mlog2).(*service)
	s3 := NewTestTxnServiceWithLog(t, 3, sender, NewTestClock(0), mlog3).(*service)
	sender.AddTxnService(s1)
	sender.AddTxnService(s2)
	sender.AddTxnService(s3)

	assert.NoError(t, s1.Start())
	defer func() {
		assert.NoError(t, s1.Close(false))
	}()

	assert.NoError(t, s2.Start())
	defer func() {
		assert.NoError(t, s2.Close(false))
	}()
	assert.NoError(t, s3.Start())
	defer func() {
		assert.NoError(t, s3.Close(false))
	}()

	commitEvent := func(s *service) mem.Event {
		for e := range s.storage.(*mem.KVTxnStorage).GetEventC() {
			if e.Type == mem.CommitType {
				return e
			}
		}
		t.Fatal("storage event channel closed before commit")
		return mem.Event{}
	}
	coordinatorCommit := commitEvent(s1)
	remoteCommit2 := commitEvent(s2)
	remoteCommit3 := commitEvent(s3)

	maxPreparedTS := NewTestTimestamp(20)
	assert.Equal(t, maxPreparedTS, coordinatorCommit.Txn.CommitTS)
	assert.Equal(t, NewTestTimestamp(10), coordinatorCommit.Txn.PreparedTS)
	assert.Equal(t, maxPreparedTS, remoteCommit2.Txn.CommitTS)
	assert.Equal(t, maxPreparedTS, remoteCommit3.Txn.CommitTS)

	mu.Lock()
	recorded := append([]txn.TxnRequest(nil), commitRequests...)
	mu.Unlock()
	require.Len(t, recorded, 2)
	assert.Equal(t, []uint64{2, 3}, []uint64{
		recorded[0].GetTargetTN().ShardID,
		recorded[1].GetTargetTN().ShardID,
	})
	for _, request := range recorded {
		assert.Equal(t, maxPreparedTS, request.Txn.CommitTS)
		assert.Equal(t, NewTestTimestamp(10), request.Txn.PreparedTS)
	}

	checkData(t, coordinatorTxn, s1, 20, 1, true)
	checkData(t, coordinatorTxn, s2, 20, 2, true)
	checkData(t, coordinatorTxn, s3, 20, 3, true)
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
