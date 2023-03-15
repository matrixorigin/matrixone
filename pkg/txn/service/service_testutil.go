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
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/txn/storage"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/mem"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// NewTestTxnService create a test TxnService for test
func NewTestTxnService(
	t *testing.T,
	shard uint64,
	sender rpc.TxnSender,
	clock clock.Clock) TxnService {
	return NewTestTxnServiceWithLog(t, shard, sender, clock, nil)
}

// NewTestTxnServiceWithAllocator create a test TxnService for test
func NewTestTxnServiceWithAllocator(
	t *testing.T,
	shard uint64,
	sender rpc.TxnSender,
	clock clock.Clock,
	allocator lockservice.LockTableAllocator) TxnService {
	return NewTestTxnServiceWithLogAndZombieAndLockTabkeAllocator(
		t,
		shard,
		sender,
		clock,
		nil,
		time.Minute,
		allocator)
}

// NewTestTxnServiceWithLog is similar to NewTestTxnService, used to recovery tests
func NewTestTxnServiceWithLog(
	t *testing.T,
	shard uint64,
	sender rpc.TxnSender,
	clock clock.Clock,
	log logservice.Client) TxnService {
	return NewTestTxnServiceWithLogAndZombie(
		t,
		shard,
		sender,
		clock,
		log,
		time.Minute)
}

// NewTestTxnServiceWithLogAndZombie is similar to NewTestTxnService, but with more args
func NewTestTxnServiceWithLogAndZombie(
	t *testing.T,
	shard uint64,
	sender rpc.TxnSender,
	clock clock.Clock,
	log logservice.Client,
	zombie time.Duration) TxnService {
	return NewTestTxnServiceWithLogAndZombieAndLockTabkeAllocator(
		t,
		shard,
		sender,
		clock,
		log,
		zombie,
		nil,
	)
}

// NewTestTxnServiceWithLogAndZombieAndLockTabkeAllocator is similar to NewTestTxnService, but with more args
func NewTestTxnServiceWithLogAndZombieAndLockTabkeAllocator(
	t *testing.T,
	shard uint64,
	sender rpc.TxnSender,
	clock clock.Clock,
	log logservice.Client,
	zombie time.Duration,
	allocator lockservice.LockTableAllocator) TxnService {
	rt := runtime.NewRuntime(
		metadata.ServiceType_DN,
		"dn-uuid",
		logutil.GetPanicLoggerWithLevel(zapcore.DebugLevel).With(zap.String("case", t.Name())),
		runtime.WithClock(clock))
	return NewTxnService(rt,
		NewTestDNShard(shard),
		NewTestTxnStorage(log, clock),
		sender,
		zombie,
		allocator).(*service)
}

// NewTestTxnStorage create a TxnStorage used to recovery tests
func NewTestTxnStorage(log logservice.Client, clock clock.Clock) storage.TxnStorage {
	if log == nil {
		log = mem.NewMemLog()
	}
	return mem.NewKVTxnStorage(1, log, clock)
}

// NewTestDNShard create a test DNShard
func NewTestDNShard(id uint64) metadata.DNShard {
	return metadata.DNShard{
		DNShardRecord: metadata.DNShardRecord{
			ShardID:    id,
			LogShardID: id,
		},
		ReplicaID: id,
		Address:   fmt.Sprintf("dn-%d", id),
	}
}

// NewTestClock create test clock with start timestamp
func NewTestClock(start int64) clock.Clock {
	ts := start
	return clock.NewHLCClock(func() int64 {
		return atomic.AddInt64(&ts, 1)
	}, math.MaxInt64)
}

// NewTestSpecClock create test clock with timestamp factory
func NewTestSpecClock(fn func() int64) clock.Clock {
	return clock.NewHLCClock(fn, math.MaxInt64)
}

// TestSender test TxnSender for sending messages between TxnServices
type TestSender struct {
	router map[string]rpc.TxnRequestHandleFunc
	filter func(*txn.TxnRequest) bool

	mu struct {
		sync.Mutex
		cancels []context.CancelFunc
	}
}

// NewTestSender create test TxnSender
func NewTestSender(services ...TxnService) *TestSender {
	s := &TestSender{
		router: make(map[string]rpc.TxnRequestHandleFunc),
	}
	for _, ts := range services {
		s.AddTxnService(ts)
	}
	return s
}

// AddTxnService add txnservice into test TxnSender
func (s *TestSender) AddTxnService(ts TxnService) {
	s.router[s.getRouteKey(txn.TxnMethod_Read, ts.Shard())] = ts.Read
	s.router[s.getRouteKey(txn.TxnMethod_Write, ts.Shard())] = ts.Write
	s.router[s.getRouteKey(txn.TxnMethod_Commit, ts.Shard())] = ts.Commit
	s.router[s.getRouteKey(txn.TxnMethod_Rollback, ts.Shard())] = ts.Rollback
	s.router[s.getRouteKey(txn.TxnMethod_Prepare, ts.Shard())] = ts.Prepare
	s.router[s.getRouteKey(txn.TxnMethod_GetStatus, ts.Shard())] = ts.GetStatus
	s.router[s.getRouteKey(txn.TxnMethod_CommitDNShard, ts.Shard())] = ts.CommitDNShard
	s.router[s.getRouteKey(txn.TxnMethod_RollbackDNShard, ts.Shard())] = ts.RollbackDNShard
	s.router[s.getRouteKey(txn.TxnMethod_DEBUG, ts.Shard())] = ts.Debug
}

func (s *TestSender) setFilter(filter func(*txn.TxnRequest) bool) {
	s.filter = filter
}

// Send TxnSender send
func (s *TestSender) Send(ctx context.Context, requests []txn.TxnRequest) (*rpc.SendResult, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	s.mu.Lock()
	s.mu.cancels = append(s.mu.cancels, cancel)
	s.mu.Unlock()

	responses := make([]txn.TxnResponse, 0, len(requests))
	for _, req := range requests {
		if s.filter != nil && !s.filter(&req) {
			continue
		}

		resp := txn.TxnResponse{}
		h := s.router[s.getRouteKey(req.Method, req.GetTargetDN())]
		if err := h(ctx, &req, &resp); err != nil {
			return nil, err
		}
		responses = append(responses, resp)
	}
	return &rpc.SendResult{Responses: responses}, nil
}

// Close close the test TxnSender
func (s *TestSender) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cancel := range s.mu.cancels {
		cancel()
	}
	return nil
}

func (s *TestSender) getRouteKey(method txn.TxnMethod, shard metadata.DNShard) string {
	return fmt.Sprintf("%d-%s", shard.ShardID, method.String())
}

// NewTestTxn create a transaction, specifying both the transaction snapshot time and the DNShard
// for the transaction operation.
func NewTestTxn(txnID byte, ts int64, shards ...uint64) txn.TxnMeta {
	txnMeta := txn.TxnMeta{
		ID:         []byte{txnID},
		Status:     txn.TxnStatus_Active,
		SnapshotTS: NewTestTimestamp(ts),
	}
	for _, shard := range shards {
		txnMeta.DNShards = append(txnMeta.DNShards, NewTestDNShard(shard))
	}
	return txnMeta
}

// NewTestTimestamp create a test timestamp and set only the PhysicalTime field
func NewTestTimestamp(ts int64) timestamp.Timestamp {
	return timestamp.Timestamp{PhysicalTime: ts}
}

// NewTestWriteRequest create a Write request, using GetTestKey and GetTestValue as the KV data for
// the test.
func NewTestWriteRequest(k byte, wTxn txn.TxnMeta, toShard uint64) txn.TxnRequest {
	key := GetTestKey(k)
	value := GetTestValue(k, wTxn)
	req := mem.NewSetTxnRequest([][]byte{key}, [][]byte{value})
	req.Txn = wTxn
	req.CNRequest.Target = NewTestDNShard(toShard)
	return req
}

// NewTestReadRequest create a read request, using GetTestKey as the KV data for the test.
func NewTestReadRequest(k byte, rTxn txn.TxnMeta, toShard uint64) txn.TxnRequest {
	key := GetTestKey(k)
	req := mem.NewGetTxnRequest([][]byte{key})
	req.Txn = rTxn
	req.CNRequest.Target = NewTestDNShard(toShard)
	return req
}

// NewTestCommitRequest create a commit request
func NewTestCommitRequest(wTxn txn.TxnMeta) txn.TxnRequest {
	return txn.TxnRequest{
		Method:        txn.TxnMethod_Commit,
		Txn:           wTxn,
		CommitRequest: &txn.TxnCommitRequest{},
	}
}

// NewTestCommitShardRequest create a commit DNShard request
func NewTestCommitShardRequest(wTxn txn.TxnMeta) txn.TxnRequest {
	return txn.TxnRequest{
		Method: txn.TxnMethod_CommitDNShard,
		Txn:    wTxn,
		CommitDNShardRequest: &txn.TxnCommitDNShardRequest{
			DNShard: wTxn.DNShards[0],
		},
	}
}

// NewTestRollbackShardRequest create a rollback DNShard request
func NewTestRollbackShardRequest(wTxn txn.TxnMeta) txn.TxnRequest {
	return txn.TxnRequest{
		Method: txn.TxnMethod_RollbackDNShard,
		Txn:    wTxn,
		RollbackDNShardRequest: &txn.TxnRollbackDNShardRequest{
			DNShard: wTxn.DNShards[0],
		},
	}
}

// NewTestRollbackRequest create a rollback request
func NewTestRollbackRequest(wTxn txn.TxnMeta) txn.TxnRequest {
	return txn.TxnRequest{
		Method:          txn.TxnMethod_Rollback,
		Txn:             wTxn,
		RollbackRequest: &txn.TxnRollbackRequest{},
	}
}

// NewTestPrepareRequest create a prepare request
func NewTestPrepareRequest(wTxn txn.TxnMeta, shard uint64) txn.TxnRequest {
	return txn.TxnRequest{
		Method: txn.TxnMethod_Prepare,
		Txn:    wTxn,
		PrepareRequest: &txn.TxnPrepareRequest{
			DNShard: NewTestDNShard(shard),
		},
	}
}

// NewTestGetStatusRequest  create a get status request
func NewTestGetStatusRequest(wTxn txn.TxnMeta, shard uint64) txn.TxnRequest {
	return txn.TxnRequest{
		Method: txn.TxnMethod_GetStatus,
		Txn:    wTxn,
		GetStatusRequest: &txn.TxnGetStatusRequest{
			DNShard: NewTestDNShard(shard),
		},
	}
}

// GetTestKey encode test key
func GetTestKey(k byte) []byte {
	return []byte{k}
}

// GetTestValue encode test value based on the key and txn's snapshot timestamp
func GetTestValue(k byte, wTxn txn.TxnMeta) []byte {
	return []byte(fmt.Sprintf("%d-%d-%d", k, wTxn.ID[0], wTxn.SnapshotTS.PhysicalTime))
}
