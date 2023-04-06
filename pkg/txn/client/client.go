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

package client

import (
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/txn/util"
)

// WithTxnIDGenerator setup txn id generator
func WithTxnIDGenerator(generator TxnIDGenerator) TxnClientCreateOption {
	return func(tc *txnClient) {
		tc.generator = generator
	}
}

// WithLockService setup lock service
func WithLockService(lockService lockservice.LockService) TxnClientCreateOption {
	return func(tc *txnClient) {
		tc.lockService = lockService
	}
}

// WithTimestampWaiter setup timestamp waiter
func WithTimestampWaiter(timestampWaiter TimestampWaiter) TxnClientCreateOption {
	return func(tc *txnClient) {
		tc.timestampWaiter = timestampWaiter
	}
}

var _ TxnClient = (*txnClient)(nil)

type txnClient struct {
	clock           clock.Clock
	sender          rpc.TxnSender
	generator       TxnIDGenerator
	lockService     lockservice.LockService
	timestampWaiter TimestampWaiter

	mu struct {
		sync.RWMutex
		// we maintain a CN-based last commit timestamp to ensure that
		// a txn with that CN can see previous writes.
		// FIXME(fagongzi): this is a remedial solution to disable the
		// cn-based commit ts when the session-level last commit ts have
		// been processed.
		latestCommitTS timestamp.Timestamp
	}
}

// NewTxnClient create a txn client with TxnSender and Options
func NewTxnClient(
	sender rpc.TxnSender,
	options ...TxnClientCreateOption) TxnClient {
	c := &txnClient{
		clock:  runtime.ProcessLevelRuntime().Clock(),
		sender: sender,
	}
	for _, opt := range options {
		opt(c)
	}
	c.adjust()
	return c
}

func (client *txnClient) adjust() {
	if client.generator == nil {
		client.generator = newUUIDTxnIDGenerator()
	}
	if runtime.ProcessLevelRuntime().Clock() == nil {
		panic("txn clock not set")
	}
}

func (client *txnClient) New(
	ctx context.Context,
	minTS timestamp.Timestamp,
	options ...TxnOption) (TxnOperator, error) {
	txnMeta := txn.TxnMeta{}
	txnMeta.ID = client.generator.Generate()
	now, _ := client.clock.Now()
	// TODO: Consider how to handle clock offsets. If use Clock-SI, can use the current
	// time minus the maximum clock offset as the transaction's snapshotTimestamp to avoid
	// conflicts due to clock uncertainty.
	txnMeta.SnapshotTS = now
	if client.timestampWaiter != nil {
		minTS = client.adjustTimestamp(minTS)
		ts, err := client.timestampWaiter.GetTimestamp(ctx, minTS)
		if err != nil {
			return nil, err
		}
		util.LogTxnSnapshotTimestamp(
			minTS,
			ts)
		txnMeta.SnapshotTS = ts
		options = append(options,
			WithUpdateLastCommitTSFunc(client.updateLastCommitTS))
	}
	txnMeta.Mode = client.getTxnMode()
	txnMeta.Isolation = client.getTxnIsolation()
	options = append(options,
		WithTxnCNCoordinator(),
		WithTxnLockService(client.lockService))
	return newTxnOperator(
		client.sender,
		txnMeta,
		options...), nil
}

func (client *txnClient) NewWithSnapshot(snapshot []byte) (TxnOperator, error) {
	return newTxnOperatorWithSnapshot(client.sender, snapshot)
}

func (client *txnClient) Close() error {
	return client.sender.Close()
}

func (client *txnClient) getTxnIsolation() txn.TxnIsolation {
	if v, ok := runtime.ProcessLevelRuntime().GetGlobalVariables(runtime.TxnIsolation); ok {
		return v.(txn.TxnIsolation)
	}
	return txn.TxnIsolation_RC
}

func (client *txnClient) getTxnMode() txn.TxnMode {
	if v, ok := runtime.ProcessLevelRuntime().GetGlobalVariables(runtime.TxnMode); ok {
		return v.(txn.TxnMode)
	}
	return txn.TxnMode_Pessimistic
}

func (client *txnClient) updateLastCommitTS(ts timestamp.Timestamp) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.mu.latestCommitTS.Less(ts) {
		client.mu.latestCommitTS = ts
	}
}

func (client *txnClient) adjustTimestamp(ts timestamp.Timestamp) timestamp.Timestamp {
	client.mu.RLock()
	defer client.mu.RUnlock()
	if ts.Less(client.mu.latestCommitTS) {
		return client.mu.latestCommitTS
	}
	return ts
}
