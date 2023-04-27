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
	"bytes"
	"context"
	"sort"
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

// WithEnableSacrificingFreshness sacrifice freshness to reduce the waiting time for transaction start,
// which will help to improve the latency of the transaction, but will sacrifice some data freshness.
//
// In Push mode, DN will bba to push Logtail to CN, if we need to guarantee the freshness of data, then
// we need to use the current latest time as the start time of the transaction, this will ensure that
// enough logtail is collected before the transaction read/write starts, but this will have some delayed
// waiting time.
//
// But if we can accept to sacrifice some data freshness, we can optimize this waiting time, we just need to
// use the latest logtail timestamp received + 1 as the transaction start timestamp, so we can not wait.

// When making this optimization, there are some scenarios where data consistency must be guaranteed, such as
// a database connection in a session where the latter transaction must be able to read the data committed by
// the previous transaction, then it is necessary to maintain a Session-level transaction last commit time, and
// the start time of the next transaction cannot be less than this value.
//
// If we need to ensure that all the transactions on a CN can read the writes of the previous committed
// transaction, then we can use WithEenableCNBasedConsistency to turn on.
func WithEnableSacrificingFreshness(timestampWaiter TimestampWaiter) TxnClientCreateOption {
	return func(tc *txnClient) {
		tc.timestampWaiter = timestampWaiter
		tc.enableSacrificingFreshness = true
	}
}

// WithEnableCNBasedConsistency let all transactions on a CN see writes committed by other
// transactions before them. When this feature is enabled, the client maintains a CN-Based
// commit timestamp, and when opening a new transaction, it adjusts the transaction's snapshot
// timestamp to at least >= lastCommitTimestamp, so that it can see the writes of the previously
// committed transaction
func WithEnableCNBasedConsistency() TxnClientCreateOption {
	return func(tc *txnClient) {
		tc.enableCNBasedConsistency = true
	}
}

// WithEnableRefreshExpression in RC mode, in the event of a conflict, the later transaction needs
// to see the latest data after the previous transaction commits. At this time we need to re-read
// the data, re-read the latest data, and re-compute the expression.
func WithEnableRefreshExpression() TxnClientCreateOption {
	return func(tc *txnClient) {
		tc.enableRefreshExpression = true
	}
}

var _ TxnClient = (*txnClient)(nil)

type txnClient struct {
	clock                      clock.Clock
	sender                     rpc.TxnSender
	generator                  TxnIDGenerator
	lockService                lockservice.LockService
	timestampWaiter            TimestampWaiter
	enableCNBasedConsistency   bool
	enableSacrificingFreshness bool
	enableRefreshExpression    bool

	mu struct {
		sync.RWMutex
		txns []txn.TxnMeta

		// Minimum Active Transaction Timestamp
		minTS timestamp.Timestamp

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
	ts, err := client.determineTxnSnapshot(ctx, minTS)
	if err != nil {
		return nil, err
	}

	txnMeta := txn.TxnMeta{}
	txnMeta.ID = client.generator.Generate()
	txnMeta.SnapshotTS = ts
	txnMeta.Mode = client.getTxnMode()
	txnMeta.Isolation = client.getTxnIsolation()

	client.pushTransaction(txnMeta)

	options = append(options,
		WithTxnCNCoordinator(),
		WithTxnClose(client),
		WithUpdateLastCommitTSFunc(client.updateLastCommitTS),
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

func (client *txnClient) MinTimestamp() timestamp.Timestamp {
	client.mu.Lock()
	defer client.mu.Unlock()
	return client.mu.minTS
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

func (client *txnClient) determineTxnSnapshot(
	ctx context.Context,
	minTS timestamp.Timestamp) (timestamp.Timestamp, error) {
	if !client.enableSacrificingFreshness {
		// TODO: Consider how to handle clock offsets. If use Clock-SI, can use the current
		// time minus the maximum clock offset as the transaction's snapshotTimestamp to avoid
		// conflicts due to clock uncertainty.
		now, _ := client.clock.Now()
		return now, nil
	}

	if client.enableCNBasedConsistency {
		minTS = client.adjustTimestamp(minTS)
	}

	ts, err := client.timestampWaiter.GetTimestamp(ctx, minTS)
	if err != nil {
		return ts, err
	}

	util.LogTxnSnapshotTimestamp(
		minTS,
		ts)
	return ts, nil
}

func (client *txnClient) adjustTimestamp(ts timestamp.Timestamp) timestamp.Timestamp {
	client.mu.RLock()
	defer client.mu.RUnlock()
	if ts.Less(client.mu.latestCommitTS) {
		return client.mu.latestCommitTS
	}
	return ts
}

func (client *txnClient) GetLatestCommitTS() timestamp.Timestamp {
	return client.adjustTimestamp(timestamp.Timestamp{})
}

func (client *txnClient) SetLatestCommitTS(ts timestamp.Timestamp) {
	client.updateLastCommitTS(ts)
}

func (client *txnClient) popTransaction(txn txn.TxnMeta) {
	var i int

	client.mu.Lock()
	defer client.mu.Unlock()
	for i = 0; i < len(client.mu.txns); i++ {
		if bytes.Equal(client.mu.txns[i].ID, txn.ID) {
			break
		}
	}
	client.mu.txns = append(client.mu.txns[:i], client.mu.txns[i+1:]...)
	switch {
	case len(client.mu.txns) == 0:
		client.mu.minTS = timestamp.Timestamp{}
	case i == 0:
		client.mu.minTS = client.mu.txns[i].SnapshotTS
	}
}

func (client *txnClient) pushTransaction(txn txn.TxnMeta) {
	client.mu.Lock()
	defer client.mu.Unlock()
	i := sort.Search(len(client.mu.txns), func(i int) bool {
		return client.mu.txns[i].SnapshotTS.GreaterEq(txn.SnapshotTS)
	})
	if i == len(client.mu.txns) {
		client.mu.txns = append(client.mu.txns, txn)
	} else {
		client.mu.txns = append(client.mu.txns[:i+1], client.mu.txns[i:]...)
		client.mu.txns[i] = txn
	}
	if client.mu.minTS.IsEmpty() || txn.SnapshotTS.Less(client.mu.minTS) {
		client.mu.minTS = txn.SnapshotTS
	}
}
