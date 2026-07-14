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
	"encoding/hex"
	"errors"
	"math"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/txn/util"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
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
// In Push mode, TN will bba to push Logtail to CN, if we need to guarantee the freshness of data, then
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
	leakHandleFunc func([]ActiveTxn)) TxnClientCreateOption {
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

// WithNormalStateNoWait sets the normalStateNoWait value of txnClient.
func WithNormalStateNoWait(t bool) TxnClientCreateOption {
	return func(tc *txnClient) {
		tc.normalStateNoWait = t
	}
}

func WithTxnOpenedCallback(callbacks []func(op TxnOperator)) TxnClientCreateOption {
	return func(tc *txnClient) {
		tc.txnOpenedCallbacks = callbacks
	}
}

func WithCheckDup() TxnClientCreateOption {
	return func(tc *txnClient) {
		tc.enableCheckDup = true
	}
}

var _ TxnClient = (*txnClient)(nil)

type status int

const (
	paused        = status(0)
	normal status = status(1)
)

const (
	// activeTxnShards is the number of shards for activeTxns map.
	// Using sharded locks to reduce contention on high-concurrency workloads.
	activeTxnShards = 16
)

// activeTxnShard is a shard of active transactions with its own lock.
type activeTxnShard struct {
	sync.RWMutex
	txns map[string]*txnOperator
}

type txnClient struct {
	sid                        string
	stopper                    *stopper.Stopper
	logger                     *log.MOLogger
	clock                      clock.Clock
	sender                     rpc.TxnSender
	generator                  TxnIDGenerator
	lockService                lockservice.LockService
	timestampWaiter            TimestampWaiter
	leakChecker                *leakChecker
	limiter                    ratelimit.Limiter
	maxActiveTxn               int
	enableCheckDup             bool
	enableCNBasedConsistency   bool
	enableSacrificingFreshness bool
	enableRefreshExpression    bool
	txnOpenedCallbacks         []func(TxnOperator)

	// normalStateNoWait is used to control if wait for the txn client's
	// state to be normal. If it is false, which is default value, wait
	// until the txn client's state to be normal; otherwise, if it is true,
	// do not wait, and just return an error.
	normalStateNoWait bool

	atomic struct {
		// we maintain a CN-based last commit timestamp to ensure that
		// a txn with that CN can see previous writes.
		// FIXME(fagongzi): this is a remedial solution to disable the
		// cn-based commit ts when the session-level last commit ts have
		// been processed.
		latestCommitTS atomic.Pointer[timestamp.Timestamp]
		// just for bvt testing
		forceSyncCommitTimes atomic.Uint64
		// activeTxnCount is the total count of active transactions across all shards.
		activeTxnCount atomic.Int64
		// latestAbortAt is the newest invalid-CN observation awaiting handling.
		// Keep the full time.Time so cutoff comparisons retain Go's monotonic
		// clock component across wall-clock adjustments.
		latestAbortAt atomic.Pointer[time.Time]
	}

	// activeTxns is sharded to reduce lock contention.
	// Use getActiveTxnShard() to get the shard for a given txnID.
	activeTxns [activeTxnShards]activeTxnShard

	mu struct {
		sync.RWMutex
		// indicate whether the CN can provide service normally.
		state     status
		pausedC   chan struct{}
		closed    bool
		closedC   chan struct{}
		closeDone chan struct{}
		closeErr  error
		// user active txns
		users int
		// FIFO queue for ready to active txn
		waitActiveTxns            []*txnOperator
		waitMarkAllActiveAbortedC chan struct{}
	}

	// abortC only carries wakeups. latestAbortAt retains the newest invalid-CN
	// observation while the worker is scanning active transactions.
	abortC      chan struct{}
	closeCtx    context.Context
	closeCancel context.CancelFunc
}

// getActiveTxnShard returns the shard for a given txnID.
func (client *txnClient) getActiveTxnShard(txnID string) *activeTxnShard {
	// Use FNV-1a hash for better distribution
	h := uint32(2166136261)
	for i := 0; i < len(txnID); i++ {
		h ^= uint32(txnID[i])
		h *= 16777619
	}
	return &client.activeTxns[h%activeTxnShards]
}

func (client *txnClient) isClosed() bool {
	client.mu.RLock()
	defer client.mu.RUnlock()
	return client.mu.closed
}

// addActiveTxn adds a transaction to the sharded map.
func (client *txnClient) addActiveTxn(op *txnOperator) {
	key := string(op.reset.txnID)
	shard := client.getActiveTxnShard(key)
	shard.Lock()
	shard.txns[key] = op
	shard.Unlock()
	client.atomic.activeTxnCount.Add(1)
	client.addToLeakCheck(op)
}

// removeActiveTxn removes a transaction from the sharded map.
// Returns the removed operator and whether it existed.
func (client *txnClient) removeActiveTxn(txnID string) (*txnOperator, bool) {
	shard := client.getActiveTxnShard(txnID)
	shard.Lock()
	op, ok := shard.txns[txnID]
	if ok {
		delete(shard.txns, txnID)
	}
	shard.Unlock()
	if ok {
		client.atomic.activeTxnCount.Add(-1)
		client.removeFromLeakCheck([]byte(txnID))
	}
	return op, ok
}

// getActiveTxn gets a transaction from the sharded map.
func (client *txnClient) getActiveTxn(txnID string) (*txnOperator, bool) {
	shard := client.getActiveTxnShard(txnID)
	shard.RLock()
	op, ok := shard.txns[txnID]
	shard.RUnlock()
	return op, ok
}

// forEachActiveTxn iterates over all active transactions.
// The callback is called with each shard's lock held (read lock).
func (client *txnClient) forEachActiveTxn(fn func(op *txnOperator) bool) {
	for i := range client.activeTxns {
		shard := &client.activeTxns[i]
		shard.RLock()
		for _, op := range shard.txns {
			if !fn(op) {
				shard.RUnlock()
				return
			}
		}
		shard.RUnlock()
	}
}

// collectActiveTxns collects all active transactions into a slice.
func (client *txnClient) collectActiveTxns() []*txnOperator {
	count := client.atomic.activeTxnCount.Load()
	ops := make([]*txnOperator, 0, count)
	for i := range client.activeTxns {
		shard := &client.activeTxns[i]
		shard.RLock()
		for _, op := range shard.txns {
			ops = append(ops, op)
		}
		shard.RUnlock()
	}
	return ops
}

func (client *txnClient) GetState() TxnState {
	// Note: ActiveTxns is collected from sharded maps without holding mu,
	// so it may not be exactly consistent with state/users which are read
	// under mu lock. This is acceptable for monitoring/debugging purposes.
	// If exact consistency is needed, all reads should be under a single lock.

	// Collect active txn IDs from sharded map
	at := make([]string, 0, client.atomic.activeTxnCount.Load())
	client.forEachActiveTxn(func(op *txnOperator) bool {
		at = append(at, hex.EncodeToString(op.reset.txnID))
		return true
	})

	client.mu.RLock()
	wt := make([]string, 0, len(client.mu.waitActiveTxns))
	for _, v := range client.mu.waitActiveTxns {
		wt = append(wt, hex.EncodeToString(v.reset.txnID))
	}
	state := client.mu.state
	users := client.mu.users
	client.mu.RUnlock()

	return TxnState{
		State:          int(state),
		Users:          users,
		ActiveTxns:     at,
		WaitActiveTxns: wt,
		LatestTS:       client.timestampWaiter.LatestTS(),
	}
}

// NewTxnClient create a txn client with TxnSender and Options
func NewTxnClient(
	sid string,
	sender rpc.TxnSender,
	options ...TxnClientCreateOption,
) TxnClient {
	c := &txnClient{
		sid:    sid,
		logger: util.GetLogger(sid),
		clock:  runtime.ServiceRuntime(sid).Clock(),
		sender: sender,
		abortC: make(chan struct{}, 1),
	}
	c.stopper = stopper.NewStopper("txn-client", stopper.WithLogger(c.logger.RawLogger()))
	c.closeCtx, c.closeCancel = context.WithCancel(context.Background())
	c.mu.state = paused
	c.mu.pausedC = make(chan struct{})
	c.mu.closedC = make(chan struct{})
	c.mu.closeDone = make(chan struct{})
	// Initialize sharded activeTxns
	for i := range c.activeTxns {
		c.activeTxns[i].txns = make(map[string]*txnOperator, 100000/activeTxnShards)
	}
	for _, opt := range options {
		opt(c)
	}
	c.adjust()
	c.startLeakChecker()
	if err := c.stopper.RunTask(c.handleMarkActiveTxnAborted); err != nil {
		panic(err)
	}
	return c
}

func (client *txnClient) adjust() {
	if client.generator == nil {
		client.generator = newUUIDTxnIDGenerator(client.sid)
	}
	if runtime.ServiceRuntime(client.sid).Clock() == nil {
		panic("txn clock not set")
	}
	if client.limiter == nil {
		client.limiter = ratelimit.NewUnlimited()
	}
	if client.maxActiveTxn == 0 {
		client.maxActiveTxn = math.MaxInt
	}
	// Initialize sharded activeTxns if not already initialized
	for i := range client.activeTxns {
		if client.activeTxns[i].txns == nil {
			client.activeTxns[i].txns = make(map[string]*txnOperator)
		}
	}
}

func (client *txnClient) New(
	ctx context.Context,
	minTS timestamp.Timestamp,
	options ...TxnOption,
) (TxnOperator, error) {
	op := newTxnOperator(
		client.sid,
		client.clock,
		client.sender,
		client.newTxnMeta(),
		client.getTxnOptions(options)...,
	)
	return client.doCreateTxn(
		ctx,
		op,
		minTS,
	)
}

func (client *txnClient) RestartTxn(
	ctx context.Context,
	txnOp TxnOperator,
	minTS timestamp.Timestamp,
	options ...TxnOption,
) (TxnOperator, error) {
	op := txnOp.(*txnOperator)
	if err := op.claimRestart(); err != nil {
		return nil, err
	}
	op.initForRestart(
		client.newTxnMeta(),
		client.getTxnOptions(options)...,
	)
	restarted, err := client.doCreateTxn(
		ctx,
		op,
		minTS,
	)
	if err != nil {
		return nil, err
	}
	if err := op.openRunSQLAfterRestart(); err != nil {
		return nil, err
	}
	return restarted, nil
}

func (client *txnClient) doCreateTxn(
	ctx context.Context,
	op *txnOperator,
	minTS timestamp.Timestamp,
) (created TxnOperator, err error) {
	start := time.Now()
	defer func() {
		v2.TxnCreateTotalDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	// we take a token from the limiter to control the number of transactions created per second.
	client.limiter.Take()

	op.timestampWaiter = client.timestampWaiter
	op.AppendEventCallback(
		ClosedEvent,
		TxnEventCallback{
			Func: client.updateLastCommitTS,
		},
		TxnEventCallback{
			Func: client.closeTxn,
		},
	)

	if err = client.openTxn(ctx, op); err != nil {
		op.closeUnadmitted(err)
		return nil, err
	}
	// From this point until the successful return, openTxn has transferred the
	// operator into the client's admission ownership graph. One deferred owner
	// closes every failure/panic path and first crosses the admission barrier;
	// future creation steps cannot accidentally add an unpaired error return.
	admissionComplete := false
	defer func() {
		if !admissionComplete {
			err = client.abortCreatedTxn(ctx, op, err)
			created = nil
		}
	}()
	client.mu.RLock()
	closed := client.mu.closed
	client.mu.RUnlock()
	// A queued transaction must consume its admission result. Close completes
	// that gate with ErrClientClosed, while a directly admitted transaction can
	// be closed immediately.
	if closed && op.reset.waiter == nil {
		err := moerr.NewClientClosedNoCtx()
		return nil, err
	}

	for _, cb := range client.txnOpenedCallbacks {
		cb(op)
	}

	ts := client.determineTxnSnapshot(minTS)
	if !op.opts.skipWaitPushClient {
		snapshotCtx := ctx
		var closeC <-chan struct{}
		if op.timestampWaiter != nil {
			client.mu.RLock()
			closeC = client.mu.closedC
			client.mu.RUnlock()
			if _, ok := op.timestampWaiter.(closeAwareTimestampWaiter); !ok {
				var cancelOnClose context.CancelFunc
				snapshotCtx, cancelOnClose = client.withCloseContext(ctx)
				defer cancelOnClose()
			}
		}
		if err := op.updateSnapshotWithClose(snapshotCtx, ts, closeC); err != nil {
			if op.reset.waiter != nil {
				if waitErr, completed := op.reset.waiter.result(); completed && waitErr != nil {
					return nil, waitErr
				}
			}
			if client.isClosed() {
				err := moerr.NewClientClosedNoCtx()
				return nil, err
			}
			createErr := errors.Join(err, moerr.NewTxnError(ctx, "update txn snapshot"))
			// openTxn has transferred ownership to the client before snapshot
			// acquisition. Creation failure owns a private abort transition: public
			// Rollback may be sealed while RestartTxn is being admitted and cannot
			// be relied on to release active/queued client ownership.
			return nil, createErr
		}
	}

	util.LogTxnSnapshotTimestamp(
		client.logger,
		minTS,
		ts,
	)

	if err := op.waitActive(ctx); err != nil {
		// The failed creator still owns cleanup even when the public terminal
		// gate is sealed by RestartTxn. ClosedEvent removes either direct active
		// ownership or the queued admission entry.
		if moerr.IsMoErrCode(err, moerr.ErrClientClosed) {
			return nil, err
		}
		return nil, errors.Join(err, moerr.NewTxnError(ctx, "wait active"))
	}
	client.mu.RLock()
	closed = client.mu.closed
	client.mu.RUnlock()
	if closed {
		err := moerr.NewClientClosedNoCtx()
		return nil, err
	}
	admissionComplete = true
	return op, nil
}

// abortCreatedTxn closes an operator after openTxn transferred ownership to the
// client. Admission abort is the ownership barrier: queued work becomes
// non-promotable, while a claimed promotion must publish active ownership
// before the sole ClosedEvent is allowed to remove it.
func (client *txnClient) abortCreatedTxn(
	ctx context.Context,
	op *txnOperator,
	err error,
) error {
	if op.reset.waiter != nil {
		if admissionErr := op.reset.waiter.abort(err); admissionErr != nil {
			err = admissionErr
		}
	}
	op.closeAsAborted(context.WithoutCancel(ctx), err)
	return err
}

func (client *txnClient) NewWithSnapshot(
	snapshot txn.CNTxnSnapshot,
) (TxnOperator, error) {
	client.mu.RLock()
	closed := client.mu.closed
	client.mu.RUnlock()
	if closed {
		return nil, moerr.NewClientClosedNoCtx()
	}
	op := newTxnOperatorWithSnapshot(
		client.logger,
		client.sender,
		snapshot,
	)
	op.timestampWaiter = client.timestampWaiter
	return op, nil
}

func (client *txnClient) Close() error {
	var waiting []*txnOperator
	client.mu.Lock()
	if client.mu.closed {
		done := client.mu.closeDone
		client.mu.Unlock()
		if done != nil {
			<-done
		}
		client.mu.RLock()
		err := client.mu.closeErr
		client.mu.RUnlock()
		return err
	}
	client.mu.closed = true
	if client.mu.pausedC != nil {
		close(client.mu.pausedC)
		client.mu.pausedC = nil
	}
	if client.mu.closedC != nil {
		close(client.mu.closedC)
	}
	if client.closeCancel != nil {
		client.closeCancel()
	}
	waiting = client.mu.waitActiveTxns
	client.mu.waitActiveTxns = nil
	client.mu.Unlock()
	for _, op := range waiting {
		op.failActiveWait(moerr.NewClientClosedNoCtx())
	}

	client.stopper.Stop()
	if client.leakChecker != nil {
		client.leakChecker.close()
	}
	err := client.sender.Close()
	client.mu.Lock()
	client.mu.closeErr = err
	if client.mu.closeDone != nil {
		close(client.mu.closeDone)
	}
	client.mu.Unlock()
	return err
}

// withCloseContext binds creation-time blocking work to the transaction
// client's shutdown without changing the caller's context contract.
func (client *txnClient) withCloseContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if client.closeCtx == nil {
		return ctx, func() {}
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(client.closeCtx, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

func (client *txnClient) MinTimestamp() timestamp.Timestamp {
	ops := client.collectActiveTxns()

	min := timestamp.Timestamp{}
	for _, op := range ops {
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
	ctx, cancel := client.withCloseContext(ctx)
	defer cancel()
	value, err := client.timestampWaiter.GetTimestamp(ctx, ts)
	if err != nil && client.isClosed() {
		return timestamp.Timestamp{}, moerr.NewClientClosedNoCtx()
	}
	return value, err
}

func (client *txnClient) getTxnIsolation() txn.TxnIsolation {
	if v, ok := runtime.ServiceRuntime(client.sid).GetGlobalVariables(runtime.TxnIsolation); ok {
		return v.(txn.TxnIsolation)
	}
	return txn.TxnIsolation_RC
}

func (client *txnClient) getTxnMode() txn.TxnMode {
	if v, ok := runtime.ServiceRuntime(client.sid).GetGlobalVariables(runtime.TxnMode); ok {
		return v.(txn.TxnMode)
	}
	return txn.TxnMode_Pessimistic
}

func (client *txnClient) updateLastCommitTS(ctx context.Context, txnOp TxnOperator, event TxnEvent, value any) (err error) {
	if event.Txn.CommitTS.IsEmpty() {
		return
	}

	var old *timestamp.Timestamp
	new := &event.Txn.CommitTS
	for {
		old = client.atomic.latestCommitTS.Load()
		if old != nil && old.GreaterEq(event.Txn.CommitTS) {
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
func (client *txnClient) determineTxnSnapshot(minTS timestamp.Timestamp) timestamp.Timestamp {
	start := time.Now()
	defer func() {
		v2.TxnDetermineSnapshotDurationHistogram.Observe(time.Since(start).Seconds())
	}()

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

	return minTS
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

func (client *txnClient) SyncLatestCommitTS(ts timestamp.Timestamp) {
	client.updateLastCommitTS(context.TODO(), nil, TxnEvent{Txn: txn.TxnMeta{CommitTS: ts}}, nil)
	if client.timestampWaiter != nil {
		ctx, cancel := context.WithTimeoutCause(context.Background(), time.Minute*5, moerr.CauseSyncLatestCommitT)
		defer cancel()
		for {
			_, err := client.timestampWaiter.GetTimestamp(ctx, ts)
			if err == nil {
				break
			}
			err = moerr.AttachCause(ctx, err)
			client.logger.Fatal("wait latest commit ts failed", zap.Error(err))
		}
	}
	client.atomic.forceSyncCommitTimes.Add(1)
}

func (client *txnClient) GetSyncLatestCommitTSTimes() uint64 {
	return client.atomic.forceSyncCommitTimes.Load()
}

func (client *txnClient) openTxn(ctx context.Context, op *txnOperator) error {
	client.mu.Lock()
	if client.mu.closed {
		client.mu.Unlock()
		return moerr.NewClientClosedNoCtx()
	}

	if err := client.waitMarkAllActiveAbortedLocked(ctx); err != nil {
		client.mu.Unlock()
		return err
	}

	if !op.opts.skipWaitPushClient {
		for client.mu.state == paused {
			if client.mu.closed {
				client.mu.Unlock()
				return moerr.NewClientClosedNoCtx()
			}
			if err := ctx.Err(); err != nil {
				client.mu.Unlock()
				return err
			}
			if client.normalStateNoWait {
				activeCount := client.atomic.activeTxnCount.Load()
				waitQueueSize := len(client.mu.waitActiveTxns)
				client.mu.Unlock()
				v2.TxnActiveQueueSizeGauge.Set(float64(activeCount))
				v2.TxnWaitActiveQueueSizeGauge.Set(float64(waitQueueSize))
				return moerr.NewInternalErrorNoCtx("cn service is not ready, retry later")
			}

			if op.opts.options.WaitPausedDisabled() {
				activeCount := client.atomic.activeTxnCount.Load()
				waitQueueSize := len(client.mu.waitActiveTxns)
				client.mu.Unlock()
				v2.TxnActiveQueueSizeGauge.Set(float64(activeCount))
				v2.TxnWaitActiveQueueSizeGauge.Set(float64(waitQueueSize))
				return moerr.NewInvalidStateNoCtx("txn client is in pause state")
			}

			client.logger.Warn("txn client is in pause state, wait for it to be ready",
				zap.String("txn ID", hex.EncodeToString(op.reset.txnID)))
			if client.mu.pausedC == nil {
				client.mu.pausedC = make(chan struct{})
			}
			pausedC := client.mu.pausedC
			closedC := client.mu.closedC
			client.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-closedC:
				return moerr.NewClientClosedNoCtx()
			case <-pausedC:
			}
			client.mu.Lock()
			client.logger.Warn("txn client is in ready state",
				zap.String("txn ID", hex.EncodeToString(op.reset.txnID)))
		}
		if client.mu.closed {
			client.mu.Unlock()
			return moerr.NewClientClosedNoCtx()
		}
		if err := ctx.Err(); err != nil {
			client.mu.Unlock()
			return err
		}
	}

	if !op.opts.options.UserTxn() ||
		client.mu.users < client.maxActiveTxn {
		if op.opts.options.UserTxn() {
			client.mu.users++
		}
		waitQueueSize := len(client.mu.waitActiveTxns)
		client.mu.Unlock()
		// Add to sharded map outside mu lock
		client.addActiveTxn(op)
		// Update metrics after actual operation
		v2.TxnActiveQueueSizeGauge.Set(float64(client.atomic.activeTxnCount.Load()))
		v2.TxnWaitActiveQueueSizeGauge.Set(float64(waitQueueSize))
		return nil
	}

	op.reset.waiter = newActiveTxnWaiter()
	client.mu.waitActiveTxns = append(client.mu.waitActiveTxns, op)
	activeCount := client.atomic.activeTxnCount.Load()
	waitQueueSize := len(client.mu.waitActiveTxns)
	client.mu.Unlock()
	v2.TxnActiveQueueSizeGauge.Set(float64(activeCount))
	v2.TxnWaitActiveQueueSizeGauge.Set(float64(waitQueueSize))
	return nil
}

func (client *txnClient) closeTxn(ctx context.Context, txnOp TxnOperator, event TxnEvent, value any) (err error) {
	txn := event.Txn
	key := string(txn.ID)

	// markAllActiveTxnAborted only sends a non-blocking signal to abortC and
	// does not touch client.mu, so no need to hold the lock here. Holding it
	// on every close caused heavy mutex contention under high-concurrency
	// autocommit workloads (sysbench point_select 1000vu). Preserve the
	// original signal-before-remove ordering. The abort signal is
	// asynchronous, so correctness does not rely on the closing txn being
	// observed by the later active-txn traversal. removeActiveTxn uses its
	// own sharded lock.
	if moerr.IsMoErrCode(event.Err, moerr.ErrCannotCommitOnInvalidCN) {
		client.markAllActiveTxnAborted()
	}

	// Remove from sharded map after abort check
	op, ok := client.removeActiveTxn(key)

	client.mu.Lock()

	if ok {
		v2.TxnLifeCycleDurationHistogram.Observe(time.Since(op.reset.createAt).Seconds())

		if !op.opts.options.UserTxn() {
			v2.TxnActiveQueueSizeGauge.Set(float64(client.atomic.activeTxnCount.Load()))
			v2.TxnWaitActiveQueueSizeGauge.Set(float64(len(client.mu.waitActiveTxns)))
			client.mu.Unlock()
			return
		}
		client.mu.users--
		if client.mu.users < 0 {
			panic("BUG: user txns < 0")
		}
		if client.mu.closed {
			v2.TxnActiveQueueSizeGauge.Set(float64(client.atomic.activeTxnCount.Load()))
			v2.TxnWaitActiveQueueSizeGauge.Set(float64(len(client.mu.waitActiveTxns)))
			client.mu.Unlock()
			return
		}

		// Claim queued ownership before removing an operator from the queue.
		// Cancellation competes with this claim in activeTxnWaiter: if it wins,
		// the entry is discarded without reserving a user slot; if promotion
		// wins, the canceled creator cannot return until active publication.
		var toActivate []*txnOperator
		if len(client.mu.waitActiveTxns) > 0 {
			newCanAdded := client.maxActiveTxn - client.mu.users
			toActivate = client.claimWaitActiveOpsLocked(newCanAdded)
			client.mu.users += len(toActivate)
		}

		v2.TxnActiveQueueSizeGauge.Set(float64(client.atomic.activeTxnCount.Load() + int64(len(toActivate))))
		v2.TxnWaitActiveQueueSizeGauge.Set(float64(len(client.mu.waitActiveTxns)))
		client.mu.Unlock()

		// Add to sharded map and notify outside mu lock
		for _, waitOp := range toActivate {
			client.addActiveTxn(waitOp)
			waitOp.notifyActive()
		}
		return
	}

	if ok = client.removeFromWaitActiveLocked(txn.ID); ok {
		client.removeFromLeakCheck(txn.ID)
	} else if txnOp.(*txnOperator).reset.waiter != nil &&
		txnOp.(*txnOperator).reset.waiter.canceled() {
		// A promoter may have discarded a canceled queue entry before this
		// ClosedEvent acquired client.mu. No client ownership remains.
	} else if !client.mu.closed {
		client.logger.Warn("txn closed",
			zap.String("txn ID", hex.EncodeToString(txn.ID)),
			zap.String("stack", string(debug.Stack())))
	}

	v2.TxnActiveQueueSizeGauge.Set(float64(client.atomic.activeTxnCount.Load()))
	v2.TxnWaitActiveQueueSizeGauge.Set(float64(len(client.mu.waitActiveTxns)))
	client.mu.Unlock()
	return
}

// claimWaitActiveOpsLocked performs one stable O(n) queue compaction. It drops
// canceled ownership, claims at most limit live entries in FIFO order, retains
// the rest, and clears detached backing-array references.
func (client *txnClient) claimWaitActiveOpsLocked(limit int) []*txnOperator {
	queued := client.mu.waitActiveTxns
	remaining := queued[:0]
	claimed := make([]*txnOperator, 0, limit)
	for _, waitOp := range queued {
		if len(claimed) < limit {
			if waitOp.reset.waiter.claimPromotion() {
				claimed = append(claimed, waitOp)
			}
			// A failed claim means cancellation already owns terminal cleanup;
			// either way this entry no longer belongs in the queue.
			continue
		}
		if waitOp.reset.waiter.canceled() {
			continue
		}
		remaining = append(remaining, waitOp)
	}
	clear(queued[len(remaining):])
	client.mu.waitActiveTxns = remaining
	return claimed
}

func (client *txnClient) Pause() {
	client.mu.Lock()
	defer client.mu.Unlock()

	if !client.mu.closed && client.mu.state != paused {
		client.logger.Info("txn client status changed to paused")
		client.mu.state = paused
		client.mu.pausedC = make(chan struct{})
	}
}

func (client *txnClient) Resume() {
	client.mu.Lock()
	defer client.mu.Unlock()

	if !client.mu.closed && client.mu.state != normal {
		client.logger.Info("txn client status changed to normal")
		client.mu.state = normal
		if client.mu.pausedC != nil {
			close(client.mu.pausedC)
			client.mu.pausedC = nil
		}
	}
}

func (client *txnClient) startLeakChecker() {
	if client.leakChecker != nil {
		client.leakChecker.start()
	}
}

func (client *txnClient) addToLeakCheck(op *txnOperator) {
	if client.leakChecker != nil {
		client.leakChecker.txnOpened(op, op.reset.txnID, op.opts.options)
	}
}

func (client *txnClient) removeFromLeakCheck(id []byte) {
	if client.leakChecker != nil {
		client.leakChecker.txnClosed(id)
	}
}

func (client *txnClient) IterTxns(fn func(TxnOverview) bool) {
	ops := client.getAllTxnOperators()

	for _, op := range ops {
		if !fn(op.GetOverview()) {
			return
		}
	}
}

func (client *txnClient) IterTxnIDs(fn func([]byte) bool) {
	for i := range client.activeTxns {
		shard := &client.activeTxns[i]
		shard.RLock()
		ids := make([][]byte, 0, len(shard.txns))
		for txnID := range shard.txns {
			ids = append(ids, []byte(txnID))
		}
		shard.RUnlock()

		for _, txnID := range ids {
			if !fn(txnID) {
				return
			}
		}
	}

	client.mu.RLock()
	waitActiveTxnIDs := make([][]byte, 0, len(client.mu.waitActiveTxns))
	for _, op := range client.mu.waitActiveTxns {
		waitActiveTxnIDs = append(waitActiveTxnIDs, append([]byte(nil), op.reset.txnID...))
	}
	client.mu.RUnlock()
	for _, txnID := range waitActiveTxnIDs {
		if !fn(txnID) {
			return
		}
	}
}

func (client *txnClient) getAllTxnOperators() []*txnOperator {
	// Collect from sharded map
	ops := client.collectActiveTxns()

	// Also include waiting txns
	client.mu.RLock()
	ops = append(ops, client.mu.waitActiveTxns...)
	client.mu.RUnlock()

	return ops
}

func (client *txnClient) newTxnMeta() txn.TxnMeta {
	txnMeta := txn.TxnMeta{}
	txnMeta.ID = client.generator.Generate()
	txnMeta.Mode = client.getTxnMode()
	txnMeta.Isolation = client.getTxnIsolation()
	if client.lockService != nil {
		txnMeta.LockService = client.lockService.GetServiceID()
	}
	return txnMeta
}

func (client *txnClient) getTxnOptions(
	options []TxnOption,
) []TxnOption {
	options = append(options,
		WithTxnCNCoordinator(),
		WithTxnLockService(client.lockService))
	if client.enableCheckDup {
		options = append(options, WithTxnEnableCheckDup())
	}
	return options
}

func (client *txnClient) markAllActiveTxnAborted() {
	now := time.Now()
	for {
		latest := client.atomic.latestAbortAt.Load()
		if latest != nil && !latest.Before(now) {
			break
		}
		if client.atomic.latestAbortAt.CompareAndSwap(latest, &now) {
			break
		}
	}
	select {
	case client.abortC <- struct{}{}:
	default:
	}
}

func (client *txnClient) handleMarkActiveTxnAborted(
	ctx context.Context,
) {
	defer client.logger.InfoAction("mark active txn aborted task")()

	for {
		select {
		case <-ctx.Done():
			return
		case <-client.abortC:
			fn := func() {
				latest := client.atomic.latestAbortAt.Load()
				if latest == nil {
					return
				}
				from := *latest
				client.mu.Lock()
				client.mu.waitMarkAllActiveAbortedC = make(chan struct{})
				client.mu.Unlock()

				// Collect ops from sharded map
				ops := make([]*txnOperator, 0)
				client.forEachActiveTxn(func(op *txnOperator) bool {
					if op.reset.createAt.Before(from) {
						ops = append(ops, op)
					}
					return true
				})

				for _, op := range ops {
					op.addFlag(AbortedFlag)
				}

				client.mu.Lock()
				close(client.mu.waitMarkAllActiveAbortedC)
				client.mu.waitMarkAllActiveAbortedC = nil
				client.mu.Unlock()
			}
			fn()

			if service, ok := client.lockService.(lockservice.ResumeLockService); ok {
				if err := service.Resume(); err != nil {
					client.logger.Error(
						"resume lock service failed",
						zap.Error(err),
					)
				}
			}
		}
	}
}

func (client *txnClient) removeFromWaitActiveLocked(txnID []byte) bool {
	var ok bool
	values := client.mu.waitActiveTxns[:0]
	for _, op := range client.mu.waitActiveTxns {
		if bytes.Equal(op.reset.txnID, txnID) {
			ok = true
			continue
		}
		values = append(values, op)
	}
	clear(client.mu.waitActiveTxns[len(values):])
	client.mu.waitActiveTxns = values
	return ok
}

func (client *txnClient) waitMarkAllActiveAbortedLocked(ctx context.Context) error {
	for client.mu.waitMarkAllActiveAbortedC != nil {
		c := client.mu.waitMarkAllActiveAbortedC
		closedC := client.mu.closedC
		client.mu.Unlock()
		select {
		case <-c:
		case <-ctx.Done():
			client.mu.Lock()
			return ctx.Err()
		case <-closedC:
			client.mu.Lock()
			return moerr.NewClientClosedNoCtx()
		}
		client.mu.Lock()
		// The worker clears this field under client.mu immediately after closing
		// the channel. Clearing the same stale channel here also keeps this helper
		// safe for callers that only signal completion by closing it.
		if client.mu.waitMarkAllActiveAbortedC == c {
			client.mu.waitMarkAllActiveAbortedC = nil
		}
	}
	return nil
}
