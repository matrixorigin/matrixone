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
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	cutil "github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/txn/util"
	"go.uber.org/ratelimit"
	"go.uber.org/zap"
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
// transaction, then we can use WithEnableCNBasedConsistency to turn on.
func WithEnableSacrificingFreshness() TxnClientCreateOption {
	return func(tc *txnClient) {
		tc.enableSacrificingFreshness = true
	}
}

// WithTimestampWaiter setup timestamp waiter to get the latest applied committed timestamp from logtail.
func WithTimestampWaiter(waiter TimestampWaiter) TxnClientCreateOption {
	return func(tc *txnClient) {
		tc.timestampWaiter = waiter
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

// WithEnableLeakCheck enable txn leak check. Used to found any txn is not committed or rolled back.
func WithEnableLeakCheck(
	maxActiveAges time.Duration,
	leakHandleFunc func(txnID []byte, createAt time.Time, createBy string)) TxnClientCreateOption {
	return func(tc *txnClient) {
		tc.leakChecker = newLeakCheck(maxActiveAges, leakHandleFunc)
	}
}

// WithTxnLimit flow control of transaction creation, maximum number of transactions per second
func WithTxnLimit(n int) TxnClientCreateOption {
	return func(tc *txnClient) {
		tc.limiter = ratelimit.New(n, ratelimit.Per(time.Second))
	}
}

// WithMaxActiveTxn is the count of max active txn in current cn.  If reached max value, the txn is
// added to a FIFO queue. Default is unlimited.
func WithMaxActiveTxn(n int) TxnClientCreateOption {
	return func(tc *txnClient) {
		tc.maxActiveTxn = n
	}
}

var _ TxnClient = (*txnClient)(nil)

type status int

const (
	paused        = status(0)
	normal status = status(1)
)

type txnClient struct {
	clock                      clock.Clock
	sender                     rpc.TxnSender
	generator                  TxnIDGenerator
	lockService                lockservice.LockService
	timestampWaiter            TimestampWaiter
	leakChecker                *leakChecker
	limiter                    ratelimit.Limiter
	maxActiveTxn               int
	enableCNBasedConsistency   bool
	enableSacrificingFreshness bool
	enableRefreshExpression    bool

	atomic struct {
		// we maintain a CN-based last commit timestamp to ensure that
		// a txn with that CN can see previous writes.
		// FIXME(fagongzi): this is a remedial solution to disable the
		// cn-based commit ts when the session-level last commit ts have
		// been processed.
		latestCommitTS atomic.Pointer[timestamp.Timestamp]
	}

	mu struct {
		sync.RWMutex
		// indicate whether the CN can provide service normally.
		state status
		// order by snapshot ts
		activeTxns map[string]*txnOperator
		// FIFO queue for ready to active txn
		waitActiveTxns []*txnOperator
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
	c.mu.state = paused
	c.mu.activeTxns = make(map[string]*txnOperator, 100000)
	for _, opt := range options {
		opt(c)
	}
	c.adjust()
	c.startLeakChecker()
	return c
}

func (client *txnClient) adjust() {
	if client.generator == nil {
		client.generator = newUUIDTxnIDGenerator()
	}
	if runtime.ProcessLevelRuntime().Clock() == nil {
		panic("txn clock not set")
	}
	if client.limiter == nil {
		client.limiter = ratelimit.NewUnlimited()
	}
	if client.maxActiveTxn == 0 {
		client.maxActiveTxn = math.MaxInt
	}
}

func (client *txnClient) New(
	ctx context.Context,
	minTS timestamp.Timestamp,
	options ...TxnOption) (TxnOperator, error) {
	// we take a token from the limiter to control the number of transactions created per second.
	client.limiter.Take()

	ts, err := client.determineTxnSnapshot(ctx, minTS)
	if err != nil {
		return nil, err
	}

	txnMeta := txn.TxnMeta{}
	txnMeta.ID = client.generator.Generate()
	txnMeta.SnapshotTS = ts
	txnMeta.Mode = client.getTxnMode()
	txnMeta.Isolation = client.getTxnIsolation()
	if client.lockService != nil {
		txnMeta.LockService = client.lockService.GetConfig().ServiceID
	}

	options = append(options,
		WithTxnCNCoordinator(),
		WithTxnLockService(client.lockService))
	op := newTxnOperator(
		client.clock,
		client.sender,
		txnMeta,
		options...)
	op.timestampWaiter = client.timestampWaiter
	op.AppendEventCallback(ClosedEvent,
		client.updateLastCommitTS,
		client.closeTxn)

	if err := client.openTxn(op); err != nil {
		return nil, err
	}
	if err := op.waitActive(ctx); err != nil {
		_ = op.Rollback(ctx)
		return nil, err
	}
	return op, nil
}

func (client *txnClient) NewWithSnapshot(snapshot []byte) (TxnOperator, error) {
	op, err := newTxnOperatorWithSnapshot(client.sender, snapshot)
	if err != nil {
		return nil, err
	}
	op.timestampWaiter = client.timestampWaiter
	return op, nil
}

func (client *txnClient) Close() error {
	if client.leakChecker != nil {
		client.leakChecker.close()
	}
	return client.sender.Close()
}

func (client *txnClient) MinTimestamp() timestamp.Timestamp {
	client.mu.RLock()
	defer client.mu.RUnlock()

	min := timestamp.Timestamp{}
	for _, op := range client.mu.activeTxns {
		if min.IsEmpty() ||
			op.Txn().SnapshotTS.Less(min) {
			min = op.Txn().SnapshotTS
		}
	}
	return min
}

func (client *txnClient) WaitLogTailAppliedAt(
	ctx context.Context,
	ts timestamp.Timestamp) (timestamp.Timestamp, error) {
	if client.timestampWaiter == nil {
		return timestamp.Timestamp{}, nil
	}
	return client.timestampWaiter.GetTimestamp(ctx, ts)
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

func (client *txnClient) updateLastCommitTS(txn txn.TxnMeta) {
	var old *timestamp.Timestamp
	new := &txn.CommitTS
	for {
		old = client.atomic.latestCommitTS.Load()
		if old != nil && old.GreaterEq(txn.CommitTS) {
			return
		}

		if client.atomic.latestCommitTS.CompareAndSwap(old, new) {
			return
		}
	}
}

// determineTxnSnapshot assuming we determine the timestamp to be ts, the final timestamp
// returned will be ts+1. This is because we need to see the submitted data for ts, and the
// timestamp for all things is ts+1.
func (client *txnClient) determineTxnSnapshot(
	ctx context.Context,
	minTS timestamp.Timestamp) (timestamp.Timestamp, error) {
	// always use the current ts as txn's snapshot ts is enableSacrificingFreshness
	if !client.enableSacrificingFreshness {
		// TODO: Consider how to handle clock offsets. If use Clock-SI, can use the current
		// time minus the maximum clock offset as the transaction's snapshotTimestamp to avoid
		// conflicts due to clock uncertainty.
		now, _ := client.clock.Now()
		minTS = now
	} else if client.enableCNBasedConsistency {
		minTS = client.adjustTimestamp(minTS)
	}

	if client.timestampWaiter == nil {
		return minTS, nil
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
	v := client.atomic.latestCommitTS.Load()
	if v != nil && v.Greater(ts) {
		return *v
	}
	return ts
}

func (client *txnClient) GetLatestCommitTS() timestamp.Timestamp {
	return client.adjustTimestamp(timestamp.Timestamp{})
}

func (client *txnClient) SetLatestCommitTS(ts timestamp.Timestamp) {
	client.updateLastCommitTS(txn.TxnMeta{CommitTS: ts})
	if client.timestampWaiter != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
		defer cancel()
		_, err := client.timestampWaiter.GetTimestamp(ctx, ts)
		if err != nil {
			util.GetLogger().Fatal("wait latest commit ts failed", zap.Error(err))
		}
	}
}

func (client *txnClient) openTxn(op *txnOperator) error {
	client.mu.Lock()
	defer client.mu.Unlock()

	if client.mu.state == normal {
		n := len(client.mu.activeTxns)
		if n < client.maxActiveTxn {
			client.addActiveTxnLocked(op)
			return nil
		}
		op.waiter = newWaiter(timestamp.Timestamp{})
		op.waiter.ref()
		client.mu.waitActiveTxns = append(client.mu.waitActiveTxns, op)
		return nil
	}
	return moerr.NewInternalErrorNoCtx("cn service is not ready, retry later")
}

func (client *txnClient) closeTxn(txn txn.TxnMeta) {
	client.mu.Lock()
	defer client.mu.Unlock()

	delete(client.mu.activeTxns, cutil.UnsafeBytesToString(txn.ID))
	client.removeFromLeakCheck(txn.ID)

	if len(client.mu.waitActiveTxns) > 0 {
		newCanAdded := client.maxActiveTxn - len(client.mu.activeTxns)
		for i := 0; i < newCanAdded; i++ {
			op := client.fetchWaitActiveOpLocked()
			if op == nil {
				return
			}
			client.addActiveTxnLocked(op)
			op.notifyActive()
		}
	}
}

func (client *txnClient) addActiveTxnLocked(op *txnOperator) {
	client.mu.activeTxns[cutil.UnsafeBytesToString(op.txnID)] = op
	client.addToLeakCheck(op)
}

func (client *txnClient) fetchWaitActiveOpLocked() *txnOperator {
	if len(client.mu.waitActiveTxns) == 0 {
		return nil
	}
	op := client.mu.waitActiveTxns[0]
	client.mu.waitActiveTxns = append(client.mu.waitActiveTxns[:0], client.mu.waitActiveTxns[1:]...)
	return op
}

func (client *txnClient) Pause() {
	client.mu.Lock()
	defer client.mu.Unlock()

	util.GetLogger().Info("txn client status changed to paused")
	client.mu.state = paused
}

func (client *txnClient) Resume() {
	client.mu.Lock()
	defer client.mu.Unlock()

	util.GetLogger().Info("txn client status changed to normal")
	client.mu.state = normal
}

func (client *txnClient) AbortAllRunningTxn() {
	client.mu.Lock()
	ops := make([]*txnOperator, 0, len(client.mu.activeTxns))
	for key, op := range client.mu.activeTxns {
		ops = append(ops, op)
		delete(client.mu.activeTxns, key)
	}
	waitOps := append(([]*txnOperator)(nil), client.mu.waitActiveTxns...)
	client.mu.waitActiveTxns = client.mu.waitActiveTxns[:0]
	client.mu.Unlock()

	for _, op := range ops {
		_ = op.Rollback(context.Background())
	}
	for _, op := range waitOps {
		_ = op.Rollback(context.Background())
		op.notifyActive()
	}
}

func (client *txnClient) startLeakChecker() {
	if client.leakChecker != nil {
		client.leakChecker.start()
	}
}

func (client *txnClient) addToLeakCheck(op *txnOperator) {
	if client.leakChecker != nil {
		client.leakChecker.txnOpened(op.txnID, op.option.createBy)
	}
}

func (client *txnClient) removeFromLeakCheck(id []byte) {
	if client.leakChecker != nil {
		client.leakChecker.txnClosed(id)
	}
}
