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

func newTestTxnService(t *testing.T, shard uint64, sender rpc.TxnSender, clocker clock.Clock) *service {
	return newTestTxnServiceWithLog(t, shard, sender, clocker, nil)
}

func newTestTxnServiceWithLog(t *testing.T, shard uint64, sender rpc.TxnSender, clocker clock.Clock, log logservice.Client) *service {
	return NewTxnService(logutil.GetPanicLoggerWithLevel(zapcore.DebugLevel).With(zap.String("case", t.Name())),
		newTestDNShard(shard),
		newTestTxnStorage(log),
		sender,
		clocker,
		time.Minute).(*service)
}

func newTestTxnStorage(log logservice.Client) storage.TxnStorage {
	if log == nil {
		log = mem.NewMemLog()
	}
	return mem.NewKVTxnStorage(0, log)
}

func newTestDNShard(id uint64) metadata.DNShard {
	return metadata.DNShard{
		DNShardRecord: metadata.DNShardRecord{
			ShardID:    id,
			LogShardID: id,
		},
		ReplicaID: id,
		Address:   fmt.Sprintf("dn-%d", id),
	}
}

func newTestClock(start int64) clock.Clock {
	ts := start
	return clock.NewHLCClock(func() int64 {
		return atomic.AddInt64(&ts, 1)
	}, math.MaxInt64)
}

func newTestSpecClock(fn func() int64) clock.Clock {
	return clock.NewHLCClock(fn, math.MaxInt64)
}

type testSender struct {
	router map[string]rpc.TxnRequestHandleFunc

	mu struct {
		sync.Mutex
		cancels []context.CancelFunc
	}
}

func newTestSender(services ...TxnService) *testSender {
	s := &testSender{
		router: make(map[string]rpc.TxnRequestHandleFunc),
	}
	for _, ts := range services {
		s.addTxnService(ts)
	}
	return s
}

func (s *testSender) addTxnService(ts TxnService) {
	s.router[s.getRouteKey(txn.TxnMethod_Read, ts.Shard())] = ts.Read
	s.router[s.getRouteKey(txn.TxnMethod_Write, ts.Shard())] = ts.Write
	s.router[s.getRouteKey(txn.TxnMethod_Commit, ts.Shard())] = ts.Commit
	s.router[s.getRouteKey(txn.TxnMethod_Rollback, ts.Shard())] = ts.Rollback
	s.router[s.getRouteKey(txn.TxnMethod_Prepare, ts.Shard())] = ts.Prepare
	s.router[s.getRouteKey(txn.TxnMethod_GetStatus, ts.Shard())] = ts.GetStatus
	s.router[s.getRouteKey(txn.TxnMethod_CommitDNShard, ts.Shard())] = ts.CommitDNShard
	s.router[s.getRouteKey(txn.TxnMethod_RollbackDNShard, ts.Shard())] = ts.RollbackDNShard
}

func (s *testSender) Send(ctx context.Context, requests []txn.TxnRequest) (*rpc.SendResult, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	s.mu.Lock()
	s.mu.cancels = append(s.mu.cancels, cancel)
	s.mu.Unlock()

	responses := make([]txn.TxnResponse, 0, len(requests))
	for _, req := range requests {
		v, _ := ctx.Deadline()
		req.TimeoutAt = v.UnixNano()
		resp := txn.TxnResponse{}
		h := s.router[s.getRouteKey(req.Method, req.GetTargetDN())]
		if err := h(ctx, &req, &resp); err != nil {
			return nil, err
		}
		responses = append(responses, resp)
	}
	return &rpc.SendResult{Responses: responses}, nil
}

func (s *testSender) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cancel := range s.mu.cancels {
		cancel()
	}
	return nil
}

func (s *testSender) getRouteKey(method txn.TxnMethod, shard metadata.DNShard) string {
	return fmt.Sprintf("%d-%s", shard.ShardID, method.String())
}

func newTestTxn(txnID byte, ts int64, shards ...uint64) txn.TxnMeta {
	txnMeta := txn.TxnMeta{
		ID:         []byte{txnID},
		Status:     txn.TxnStatus_Active,
		SnapshotTS: newTestTimestamp(ts),
	}
	for _, shard := range shards {
		txnMeta.DNShards = append(txnMeta.DNShards, newTestDNShard(shard))
	}
	return txnMeta
}

func newTestTimestamp(ts int64) timestamp.Timestamp {
	return timestamp.Timestamp{PhysicalTime: ts}
}

func newTestWriteRequest(k byte, wTxn txn.TxnMeta, toShard uint64) txn.TxnRequest {
	key := getTestKey(k)
	value := getTestValue(k, wTxn)
	req := mem.NewSetTxnRequest([][]byte{key}, [][]byte{value})
	req.Txn = wTxn
	req.CNRequest.Target = newTestDNShard(toShard)
	return req
}

func newTestReadRequest(k byte, rTxn txn.TxnMeta, toShard uint64) txn.TxnRequest {
	key := getTestKey(k)
	req := mem.NewGetTxnRequest([][]byte{key})
	req.Txn = rTxn
	req.CNRequest.Target = newTestDNShard(toShard)
	return req
}

func newTestCommitRequest(wTxn txn.TxnMeta) txn.TxnRequest {
	return txn.TxnRequest{
		Method:        txn.TxnMethod_Commit,
		Txn:           wTxn,
		CommitRequest: &txn.TxnCommitRequest{},
	}
}

func newTestCommitShardRequest(wTxn txn.TxnMeta) txn.TxnRequest {
	return txn.TxnRequest{
		Method: txn.TxnMethod_CommitDNShard,
		Txn:    wTxn,
		CommitDNShardRequest: &txn.TxnCommitDNShardRequest{
			DNShard: wTxn.DNShards[0],
		},
	}
}

func newTestRollbackShardRequest(wTxn txn.TxnMeta) txn.TxnRequest {
	return txn.TxnRequest{
		Method: txn.TxnMethod_RollbackDNShard,
		Txn:    wTxn,
		RollbackDNShardRequest: &txn.TxnRollbackDNShardRequest{
			DNShard: wTxn.DNShards[0],
		},
	}
}

func newTestRollbackRequest(wTxn txn.TxnMeta) txn.TxnRequest {
	return txn.TxnRequest{
		Method:          txn.TxnMethod_Rollback,
		Txn:             wTxn,
		RollbackRequest: &txn.TxnRollbackRequest{},
	}
}

func newTestPrepareRequest(wTxn txn.TxnMeta, shard uint64) txn.TxnRequest {
	return txn.TxnRequest{
		Method: txn.TxnMethod_Prepare,
		Txn:    wTxn,
		PrepareRequest: &txn.TxnPrepareRequest{
			DNShard: newTestDNShard(shard),
		},
	}
}

func newTestGetStatusRequest(wTxn txn.TxnMeta, shard uint64) txn.TxnRequest {
	return txn.TxnRequest{
		Method: txn.TxnMethod_GetStatus,
		Txn:    wTxn,
		GetStatusRequest: &txn.TxnGetStatusRequest{
			DNShard: newTestDNShard(shard),
		},
	}
}

func getTestKey(k byte) []byte {
	return []byte{k}
}

func getTestValue(k byte, wTxn txn.TxnMeta) []byte {
	return []byte(fmt.Sprintf("%d-%d-%d", k, wTxn.ID[0], wTxn.SnapshotTS.PhysicalTime))
}
