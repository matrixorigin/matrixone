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
	"errors"
	goruntime "runtime"
	"sync"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

type blockingTimestampWaiter struct {
	entered chan struct{}
}

type legacyBlockingTimestampWaiter struct {
	entered chan struct{}
}

type observedWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

type blockingTxnRateLimiter struct {
	entered chan struct{}
}

type txnCreateResult struct {
	op  TxnOperator
	err error
}

func (l *blockingTxnRateLimiter) Wait(ctx, closeCtx context.Context) error {
	select {
	case l.entered <- struct{}{}:
	default:
	}
	if closeCtx == nil {
		<-ctx.Done()
		return ctx.Err()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-closeCtx.Done():
		return closeCtx.Err()
	}
}

func finishAbortScanWhileShardLocked(
	t *testing.T,
	client *txnClient,
	shard *activeTxnShard,
) {
	t.Helper()
	shard.Lock()
	locked := true
	defer func() {
		if locked {
			shard.Unlock()
		}
	}()

	client.markAllActiveTxnAborted()
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		return client.mu.waitMarkAllActiveAbortedC != nil
	}, 5*time.Second, 10*time.Millisecond)
	shard.Unlock()
	locked = false
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		return client.mu.waitMarkAllActiveAbortedC == nil
	}, 5*time.Second, 10*time.Millisecond)
}

func txnMarkedAborted(op *txnOperator) bool {
	op.mu.RLock()
	defer op.mu.RUnlock()
	return op.markAbortedLocked()
}

func (c *observedWaitContext) Done() <-chan struct{} {
	c.once.Do(func() {
		close(c.waiting)
	})
	return c.Context.Done()
}

type blockingCloseTxnSender struct {
	*testTxnSender
	closeStarted chan struct{}
	closeRelease chan struct{}
	closeErr     error
	mu           sync.Mutex
	closeCalls   int
}

func (s *blockingCloseTxnSender) Close() error {
	s.mu.Lock()
	s.closeCalls++
	s.mu.Unlock()
	select {
	case s.closeStarted <- struct{}{}:
	default:
	}
	<-s.closeRelease
	return s.closeErr
}

func (s *blockingCloseTxnSender) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeCalls
}

func (w *blockingTimestampWaiter) GetTimestamp(ctx context.Context, _ timestamp.Timestamp) (timestamp.Timestamp, error) {
	return w.GetTimestampWithClose(ctx, timestamp.Timestamp{}, nil)
}

func (w *blockingTimestampWaiter) GetTimestampWithClose(
	ctx context.Context,
	_ timestamp.Timestamp,
	closeC <-chan struct{},
) (timestamp.Timestamp, error) {
	select {
	case w.entered <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return timestamp.Timestamp{}, ctx.Err()
	case <-closeC:
		return timestamp.Timestamp{}, moerr.NewClientClosedNoCtx()
	}
}

func (w *blockingTimestampWaiter) NotifyLatestCommitTS(timestamp.Timestamp) {}
func (w *blockingTimestampWaiter) Close()                                   {}
func (w *blockingTimestampWaiter) LatestTS() timestamp.Timestamp            { return timestamp.Timestamp{} }

func (w *legacyBlockingTimestampWaiter) GetTimestamp(ctx context.Context, _ timestamp.Timestamp) (timestamp.Timestamp, error) {
	select {
	case w.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return timestamp.Timestamp{}, ctx.Err()
}

func (w *legacyBlockingTimestampWaiter) NotifyLatestCommitTS(timestamp.Timestamp) {}
func (w *legacyBlockingTimestampWaiter) Close()                                   {}
func (w *legacyBlockingTimestampWaiter) LatestTS() timestamp.Timestamp            { return timestamp.Timestamp{} }

func TestAdjustClient(t *testing.T) {
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	c := &txnClient{}
	c.adjust()
	assert.NotNil(t, c.generator)
	assert.NotNil(t, c.limiter)
	// Verify sharded activeTxns are initialized
	for i := range c.activeTxns {
		assert.NotNil(t, c.activeTxns[i].txns)
	}
}

func TestZeroValueClientShardedMaps(t *testing.T) {
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	c := &txnClient{}
	c.adjust()

	// Create a mock txnOperator
	op := &txnOperator{}
	op.reset.txnID = []byte("test1")

	// Should not panic when accessing sharded maps
	c.addActiveTxn(op)
	assert.Equal(t, int64(1), c.atomic.activeTxnCount.Load())

	gotOp, ok := c.getActiveTxn("test1")
	assert.True(t, ok)
	assert.NotNil(t, gotOp)

	c.removeActiveTxn("test1")
	assert.Equal(t, int64(0), c.atomic.activeTxnCount.Load())
}

func TestIterTxnIDs(t *testing.T) {
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	c := &txnClient{}
	c.adjust()

	activeOp := &txnOperator{}
	activeOp.reset.txnID = []byte("active")
	c.addActiveTxn(activeOp)

	waitOp := &txnOperator{}
	waitOp.reset.txnID = []byte("waiting")
	c.mu.Lock()
	c.mu.waitActiveTxns = append(c.mu.waitActiveTxns, waitOp)
	c.mu.Unlock()

	got := make(map[string]bool)
	c.IterTxnIDs(func(txnID []byte) bool {
		got[string(txnID)] = true
		txnID[0] = 'x'
		return true
	})

	require.True(t, got["active"])
	require.True(t, got["waiting"])
	require.Equal(t, []byte("waiting"), waitOp.reset.txnID)
	_, ok := c.getActiveTxn("active")
	require.True(t, ok)
}

func TestWaitActiveQueueClearsRemovedSlots(t *testing.T) {
	first := &txnOperator{}
	first.reset.txnID = []byte("first")
	second := &txnOperator{}
	second.reset.txnID = []byte("second")
	c := &txnClient{}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.waitActiveTxns = make([]*txnOperator, 2, 4)
	c.mu.waitActiveTxns[0] = first
	c.mu.waitActiveTxns[1] = second
	require.True(t, c.removeFromWaitActiveLocked(first.reset.txnID))
	require.Len(t, c.mu.waitActiveTxns, 1)
	require.Same(t, second, c.mu.waitActiveTxns[0])
	require.Nil(t, c.mu.waitActiveTxns[:cap(c.mu.waitActiveTxns)][1])
}

func TestClaimWaitActiveOpsSkipsCanceledAndPreservesFIFO(t *testing.T) {
	client := &txnClient{}
	newWaiting := func(id string) *txnOperator {
		op := &txnOperator{}
		op.reset.txnID = []byte(id)
		op.reset.waiter = newActiveTxnWaiter()
		return op
	}
	firstCanceled := newWaiting("first-canceled")
	first := newWaiting("first")
	secondCanceled := newWaiting("second-canceled")
	second := newWaiting("second")
	third := newWaiting("third")
	require.ErrorIs(t, firstCanceled.reset.waiter.abort(context.Canceled), context.Canceled)
	require.ErrorIs(t, secondCanceled.reset.waiter.abort(context.Canceled), context.Canceled)

	client.mu.Lock()
	client.mu.waitActiveTxns = make([]*txnOperator, 5, 8)
	copy(client.mu.waitActiveTxns, []*txnOperator{
		firstCanceled, first, secondCanceled, second, third,
	})
	claimed := client.claimWaitActiveOpsLocked(2)
	client.mu.Unlock()

	require.Equal(t, []*txnOperator{first, second}, claimed)
	require.Equal(t, []*txnOperator{third}, client.mu.waitActiveTxns)
	require.Nil(t, client.mu.waitActiveTxns[:cap(client.mu.waitActiveTxns)][1])
	require.Nil(t, client.mu.waitActiveTxns[:cap(client.mu.waitActiveTxns)][2])
	require.Nil(t, client.mu.waitActiveTxns[:cap(client.mu.waitActiveTxns)][3])
	require.Nil(t, client.mu.waitActiveTxns[:cap(client.mu.waitActiveTxns)][4])
}

func TestNewTxnAndReset(t *testing.T) {
	rt := runtime.NewRuntime(metadata.ServiceType_CN, "",
		logutil.GetPanicLogger(),
		runtime.WithClock(clock.NewHLCClock(func() int64 {
			return 1
		}, 0)))
	runtime.SetupServiceBasedRuntime("", rt)
	c := NewTxnClient("", newTestTxnSender())
	c.Resume()
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	tx, err := c.New(ctx, newTestTimestamp(0))
	assert.Nil(t, err)
	txnMeta := tx.(*txnOperator).mu.txn
	assert.Equal(t, timestamp.Timestamp{PhysicalTime: 0}, txnMeta.SnapshotTS)
	assert.NotEmpty(t, txnMeta.ID)
	assert.Equal(t, txn.TxnStatus_Active, txnMeta.Status)

	require.NoError(t, tx.Rollback(ctx))

	// Create a new transaction (object pool will reuse the freed operator)
	tx, err = c.New(ctx, newTestTimestamp(0))
	assert.Nil(t, err)
	txnMeta = tx.(*txnOperator).mu.txn
	assert.Equal(t, timestamp.Timestamp{PhysicalTime: 0}, txnMeta.SnapshotTS)
	assert.NotEmpty(t, txnMeta.ID)
	assert.Equal(t, txn.TxnStatus_Active, txnMeta.Status)
}

func TestRestartTxnRejectsPendingRunningSQLCleanup(t *testing.T) {
	RunTxnTests(func(c TxnClient, _ rpc.TxnSender) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		client := c.(*txnClient)
		op, err := c.New(ctx, timestamp.Timestamp{})
		require.NoError(t, err)
		tc := op.(*txnOperator)
		closedC := make(chan struct{})
		tc.AppendEventCallback(ClosedEvent, TxnEventCallback{Func: func(context.Context, TxnOperator, TxnEvent, any) error {
			close(closedC)
			return nil
		}})

		_, runningCancel := context.WithCancel(context.Background())
		runningToken := mustEnterRunSQL(t, tc, runningCancel, "stuck sql")
		commitCtx, cancelCommit := context.WithCancel(ctx)
		cancelCommit()
		require.ErrorIs(t, tc.Commit(commitCtx), context.Canceled)

		_, err = client.RestartTxn(ctx, op, timestamp.Timestamp{})
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed))

		tc.ExitRunSqlWithToken(runningToken)
		select {
		case <-closedC:
		case <-time.After(time.Second):
			t.Fatal("deferred rollback did not close the transaction")
		}
		require.Eventually(t, func() bool {
			tc.mu.RLock()
			defer tc.mu.RUnlock()
			return tc.mu.terminalOutcome == txnTerminalOutcomeSucceeded
		}, time.Second, time.Millisecond)

		restarted, err := client.RestartTxn(ctx, op, timestamp.Timestamp{})
		require.NoError(t, err)
		require.NoError(t, restarted.Rollback(ctx))
	})
}

func TestRestartTxnRejectsFailedDeferredRollback(t *testing.T) {
	tests := []struct {
		name         string
		workspaceErr error
		sendErr      error
		unlockErr    error
	}{
		{name: "workspace", workspaceErr: assert.AnError},
		{name: "tn rollback", sendErr: assert.AnError},
		{name: "unlock", unlockErr: assert.AnError},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			RunTxnTests(func(c TxnClient, sender rpc.TxnSender) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				client := c.(*txnClient)
				op, err := c.New(ctx, timestamp.Timestamp{})
				require.NoError(t, err)
				tc := op.(*txnOperator)
				workspace := &trackingWorkspace{rollbackErr: test.workspaceErr}
				lockService := &trackingUnlockLockService{unlockErr: test.unlockErr}
				tc.AddWorkspace(workspace)
				tc.lockService = lockService
				tc.mu.Lock()
				tc.mu.txn.TNShards = []metadata.TNShard{{TNShardRecord: metadata.TNShardRecord{ShardID: 1}}}
				tc.mu.txn.Mode = txn.TxnMode_Pessimistic
				tc.mu.Unlock()

				if test.sendErr != nil {
					sender.(*testTxnSender).setManual(func(result *rpc.SendResult, _ error) (*rpc.SendResult, error) {
						result.Release()
						return nil, test.sendErr
					})
				}

				_, runningCancel := context.WithCancel(context.Background())
				runningToken := mustEnterRunSQL(t, tc, runningCancel, "stuck sql")
				commitCtx, cancelCommit := context.WithCancel(ctx)
				cancelCommit()
				require.ErrorIs(t, tc.Commit(commitCtx), context.Canceled)
				tc.ExitRunSqlWithToken(runningToken)

				require.Eventually(t, func() bool {
					tc.mu.RLock()
					defer tc.mu.RUnlock()
					return tc.mu.terminalOutcome == txnTerminalOutcomeFailed
				}, time.Second, time.Millisecond)
				require.Equal(t, 1, workspace.rollbackCount)
				require.Equal(t, 1, lockService.unlockCount)
				requests := sender.(*testTxnSender).getLastRequests()
				require.Len(t, requests, 1)
				require.Equal(t, txn.TxnMethod_Rollback, requests[0].Method)

				for range 2 {
					_, err = client.RestartTxn(ctx, op, timestamp.Timestamp{})
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed), err)
				}
				_, err = tc.TryEnterRunSqlWithTokenAndSQL(nil, "after failed cleanup")
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed), err)
			})
		})
	}
}

func TestRestartTxnRejectsFailedSynchronousTerminalCall(t *testing.T) {
	actions := []struct {
		name string
		run  func(context.Context, *txnOperator) error
	}{
		{name: "commit", run: func(ctx context.Context, tc *txnOperator) error {
			return tc.Commit(ctx)
		}},
		{name: "rollback", run: func(ctx context.Context, tc *txnOperator) error {
			return tc.Rollback(ctx)
		}},
		{name: "write-and-commit", run: func(ctx context.Context, tc *txnOperator) error {
			result, err := tc.WriteAndCommit(ctx, []txn.TxnRequest{newTNRequest(1, 1)})
			if result != nil {
				result.Release()
			}
			return err
		}},
	}

	for _, action := range actions {
		t.Run(action.name, func(t *testing.T) {
			RunTxnTests(func(c TxnClient, sender rpc.TxnSender) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				client := c.(*txnClient)
				op, err := c.New(ctx, timestamp.Timestamp{})
				require.NoError(t, err)
				tc := op.(*txnOperator)
				tc.mu.Lock()
				tc.mu.txn.TNShards = []metadata.TNShard{{TNShardRecord: metadata.TNShardRecord{ShardID: 1}}}
				oldTxnID := append([]byte(nil), tc.reset.txnID...)
				tc.mu.Unlock()
				sender.(*testTxnSender).setManual(func(result *rpc.SendResult, _ error) (*rpc.SendResult, error) {
					result.Release()
					return nil, assert.AnError
				})

				require.ErrorIs(t, action.run(ctx, tc), assert.AnError)
				tc.mu.RLock()
				require.True(t, tc.mu.closed)
				require.Equal(t, txnTerminalOutcomeFailed, tc.mu.terminalOutcome)
				tc.mu.RUnlock()

				for range 2 {
					_, err = client.RestartTxn(ctx, op, timestamp.Timestamp{})
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed), err)
				}
				require.Equal(t, oldTxnID, tc.reset.txnID)
				_, err = tc.TryEnterRunSqlWithTokenAndSQL(nil, "after failed terminal call")
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed), err)
			})
		})
	}
}

func TestRestartTxnRejectsAbnormalTerminalFinalization(t *testing.T) {
	panicValue := "injected terminal panic"
	exits := []struct {
		name          string
		action        func()
		expectedPanic any
	}{
		{
			name: "panic",
			action: func() {
				panic(panicValue)
			},
			expectedPanic: panicValue,
		},
		{
			name:   "goexit",
			action: goruntime.Goexit,
		},
	}
	terminalCalls := []struct {
		name string
		run  func(context.Context, *txnOperator) error
	}{
		{
			name: "commit",
			run:  func(ctx context.Context, tc *txnOperator) error { return tc.Commit(ctx) },
		},
		{
			name: "write-and-commit",
			run: func(ctx context.Context, tc *txnOperator) error {
				result, err := tc.WriteAndCommit(ctx, []txn.TxnRequest{newTNRequest(1, 1)})
				if result != nil {
					result.Release()
				}
				return err
			},
		},
	}
	phases := []struct {
		name   string
		inject func(*txnOperator, *trackingWorkspace, func())
	}{
		{
			name: "workspace-finalize",
			inject: func(_ *txnOperator, workspace *trackingWorkspace, action func()) {
				workspace.finalizeAction = action
			},
		},
		{
			name: "closed-callback",
			inject: func(tc *txnOperator, _ *trackingWorkspace, action func()) {
				tc.AppendEventCallback(ClosedEvent, TxnEventCallback{Func: func(context.Context, TxnOperator, TxnEvent, any) error {
					action()
					return nil
				}})
			},
		},
	}

	for _, terminalCall := range terminalCalls {
		for _, phase := range phases {
			for _, exit := range exits {
				t.Run(terminalCall.name+"/"+phase.name+"/"+exit.name, func(t *testing.T) {
					RunTxnTests(func(c TxnClient, _ rpc.TxnSender) {
						ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()
						client := c.(*txnClient)
						op, err := client.New(ctx, timestamp.Timestamp{})
						require.NoError(t, err)
						tc := op.(*txnOperator)
						workspace := &trackingWorkspace{
							commitRequests: []txn.TxnRequest{newTNRequest(1, 1)},
						}
						phase.inject(tc, workspace, exit.action)
						tc.AddWorkspace(workspace)
						oldTxnID := append([]byte(nil), tc.reset.txnID...)

						returned := make(chan error, 1)
						recovered := make(chan any, 1)
						done := make(chan struct{})
						go func() {
							defer close(done)
							defer func() { recovered <- recover() }()
							returned <- terminalCall.run(ctx, tc)
						}()
						select {
						case <-done:
						case <-ctx.Done():
							t.Fatal("terminal call did not finish its abnormal exit")
						}
						require.Equal(t, exit.expectedPanic, <-recovered)
						select {
						case err := <-returned:
							t.Fatalf("terminal call returned normally: %v", err)
						default:
						}

						tc.mu.RLock()
						require.True(t, tc.mu.closed)
						require.Equal(t, txnTerminalOutcomeFailed, tc.mu.terminalOutcome)
						tc.mu.RUnlock()
						require.Equal(t, 1, workspace.finalizeCount)
						require.Equal(t, oldTxnID, tc.reset.txnID)
						require.Same(t, workspace, tc.reset.workspace)

						for range 2 {
							_, err = client.RestartTxn(ctx, op, timestamp.Timestamp{})
							require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed), err)
						}
						_, err = tc.TryEnterRunSqlWithTokenAndSQL(nil, "after abnormal terminal exit")
						require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed), err)
					})
				})
			}
		}
	}
}

func TestRestartTxnRejectsPanicAfterWorkspaceFinalization(t *testing.T) {
	RunTxnTests(func(c TxnClient, _ rpc.TxnSender) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		client := c.(*txnClient)
		op, err := client.New(ctx, timestamp.Timestamp{})
		require.NoError(t, err)
		tc := op.(*txnOperator)
		workspace := &trackingWorkspace{commitRequests: []txn.TxnRequest{newTNRequest(1, 1)}}
		tc.AddWorkspace(workspace)
		panicValue := "injected commit cost callback panic"
		costEventCalled := false
		tc.AppendEventCallback(CommitEvent, TxnEventCallback{Func: func(_ context.Context, _ TxnOperator, event TxnEvent, _ any) error {
			if event.CostEvent {
				costEventCalled = true
				panic(panicValue)
			}
			return nil
		}})

		var recovered any
		func() {
			defer func() { recovered = recover() }()
			err = tc.Commit(ctx)
		}()
		require.Equal(t, panicValue, recovered)
		require.NoError(t, err)
		require.True(t, costEventCalled)
		require.Equal(t, 1, workspace.finalizeCount)

		tc.mu.RLock()
		require.True(t, tc.mu.closed)
		require.Equal(t, txnTerminalOutcomeFailed, tc.mu.terminalOutcome)
		tc.mu.RUnlock()
		_, err = client.RestartTxn(ctx, op, timestamp.Timestamp{})
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed), err)
	})
}

func TestCloseAsAbortedDoesNotPublishSuccessAfterAbnormalCallback(t *testing.T) {
	panicValue := "injected closed callback panic"
	exits := []struct {
		name          string
		callback      func()
		expectedPanic any
	}{
		{
			name: "panic",
			callback: func() {
				panic(panicValue)
			},
			expectedPanic: panicValue,
		},
		{
			name:     "goexit",
			callback: goruntime.Goexit,
		},
	}

	for _, exit := range exits {
		t.Run(exit.name, func(t *testing.T) {
			RunTxnTests(func(c TxnClient, _ rpc.TxnSender) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				client := c.(*txnClient)
				op, err := client.New(ctx, timestamp.Timestamp{})
				require.NoError(t, err)
				tc := op.(*txnOperator)
				tc.AppendEventCallback(ClosedEvent, TxnEventCallback{Func: func(context.Context, TxnOperator, TxnEvent, any) error {
					exit.callback()
					return nil
				}})

				returned := make(chan struct{}, 1)
				recovered := make(chan any, 1)
				done := make(chan struct{})
				go func() {
					defer close(done)
					defer func() { recovered <- recover() }()
					tc.closeAsAborted(ctx, assert.AnError)
					returned <- struct{}{}
				}()
				select {
				case <-done:
				case <-ctx.Done():
					t.Fatal("closeAsAborted did not finish its abnormal exit")
				}
				require.Equal(t, exit.expectedPanic, <-recovered)
				select {
				case <-returned:
					t.Fatal("closeAsAborted returned normally")
				default:
				}

				tc.mu.RLock()
				require.True(t, tc.mu.closed)
				require.Equal(t, txnTerminalOutcomeFailed, tc.mu.terminalOutcome)
				tc.mu.RUnlock()
				_, err = client.RestartTxn(ctx, op, timestamp.Timestamp{})
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed), err)
			})
		})
	}
}

func TestRestartTxnRejectsCommitWithFailedUnlock(t *testing.T) {
	RunTxnTests(func(c TxnClient, _ rpc.TxnSender) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		client := c.(*txnClient)
		op, err := c.New(ctx, timestamp.Timestamp{})
		require.NoError(t, err)
		tc := op.(*txnOperator)
		workspace := &trackingWorkspace{commitRequests: []txn.TxnRequest{newTNRequest(1, 1)}}
		lockService := &trackingUnlockLockService{unlockErr: assert.AnError}
		tc.AddWorkspace(workspace)
		tc.lockService = lockService
		tc.mu.Lock()
		tc.mu.txn.Mode = txn.TxnMode_Pessimistic
		tc.mu.Unlock()

		// TN commit is authoritative, so preserve the successful Commit result
		// and workspace finalization. Only reuse is rejected because old locks
		// still have no confirmed cleanup owner.
		require.NoError(t, tc.Commit(ctx))
		require.Equal(t, 1, workspace.finalizeCount)
		require.Zero(t, workspace.rollbackCount)
		require.Equal(t, 1, lockService.unlockCount)
		tc.mu.RLock()
		require.True(t, tc.mu.closed)
		require.Equal(t, txn.TxnStatus_Committed, tc.mu.txn.Status)
		require.Equal(t, txnTerminalOutcomeFailed, tc.mu.terminalOutcome)
		tc.mu.RUnlock()

		_, err = client.RestartTxn(ctx, op, timestamp.Timestamp{})
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed), err)
	})
}

func TestRestartTxnAfterSuccessfulWriteAndCommit(t *testing.T) {
	RunTxnTests(func(c TxnClient, _ rpc.TxnSender) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		client := c.(*txnClient)
		op, err := c.New(ctx, timestamp.Timestamp{})
		require.NoError(t, err)
		result, err := op.WriteAndCommit(ctx, []txn.TxnRequest{newTNRequest(1, 1)})
		require.NoError(t, err)
		if result != nil {
			result.Release()
		}

		restarted, err := client.RestartTxn(ctx, op, timestamp.Timestamp{})
		require.NoError(t, err)
		require.NoError(t, restarted.Rollback(ctx))
	})
}

func TestRestartTxnRejectsActiveOperator(t *testing.T) {
	RunTxnTests(func(c TxnClient, _ rpc.TxnSender) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		op, err := c.New(ctx, timestamp.Timestamp{})
		require.NoError(t, err)

		_, err = c.(*txnClient).RestartTxn(ctx, op, timestamp.Timestamp{})
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed))
		require.NoError(t, op.Rollback(ctx))
	})
}

func TestRestartTxnImmediatelyAfterLastRunSQLExit(t *testing.T) {
	RunTxnTests(func(c TxnClient, _ rpc.TxnSender) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		client := c.(*txnClient)

		for range 128 {
			op, err := client.New(ctx, timestamp.Timestamp{})
			require.NoError(t, err)
			tc := op.(*txnOperator)

			runningCtx, runningCancel := context.WithCancel(context.Background())
			token := mustEnterRunSQL(t, tc, runningCancel, "last old-generation sql")
			commitC := make(chan error, 1)
			go func() {
				commitC <- tc.Commit(ctx)
			}()

			// Cancellation proves Commit sealed the tracker and is waiting for
			// this token, without relying on scheduler timing.
			select {
			case <-runningCtx.Done():
			case <-ctx.Done():
				t.Fatal("commit did not wait for running SQL")
			}

			exitDone := make(chan struct{})
			go func() {
				tc.ExitRunSqlWithToken(token)
				close(exitDone)
			}()
			require.NoError(t, <-commitC)

			// Restart as soon as the drain notification releases Commit. Do not
			// wait for the Exit caller to return: it must have no old-generation
			// state access remaining after publishing the drain.
			restarted, err := client.RestartTxn(ctx, tc, timestamp.Timestamp{})
			require.NoError(t, err)
			<-exitDone

			newTC := restarted.(*txnOperator)
			newToken := mustEnterRunSQL(t, newTC, nil, "new-generation sql")
			require.NotEqual(t, token, newToken)
			newTC.ExitRunSqlWithToken(newToken)
			require.NoError(t, restarted.Rollback(ctx))
		}
	})
}

func TestRestartTxnKeepsRunSQLSealedUntilAdmissionCompletes(t *testing.T) {
	RunTxnTests(func(c TxnClient, _ rpc.TxnSender) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		client := c.(*txnClient)
		op, err := client.New(ctx, timestamp.Timestamp{})
		require.NoError(t, err)
		require.NoError(t, op.Rollback(ctx))

		registrationErr := make(chan error, 1)
		terminalErr := make(chan error, 1)
		client.txnOpenedCallbacks = []func(TxnOperator){func(opened TxnOperator) {
			_, sqlCancel := context.WithCancel(context.Background())
			token, err := TryEnterRunSqlWithTokenAndSQL(opened, sqlCancel, "late old-generation sql")
			require.Zero(t, token)
			registrationErr <- err
			terminalErr <- opened.Commit(ctx)
		}}

		restarted, err := client.RestartTxn(ctx, op, timestamp.Timestamp{})
		require.NoError(t, err)
		require.True(t, moerr.IsMoErrCode(<-registrationErr, moerr.ErrTxnClosed))
		require.True(t, moerr.IsMoErrCode(<-terminalErr, moerr.ErrTxnClosed))

		_, sqlCancel := context.WithCancel(context.Background())
		token, err := TryEnterRunSqlWithTokenAndSQL(restarted, sqlCancel, "new-generation sql")
		require.NoError(t, err)
		require.NotZero(t, token)
		restarted.ExitRunSqlWithToken(token)
		sqlCancel()
		require.NoError(t, restarted.Rollback(ctx))
	})
}

func TestRestartTxnDoesNotRetainAbortFlag(t *testing.T) {
	RunTxnTests(func(tc TxnClient, _ rpc.TxnSender) {
		client := tc.(*txnClient)
		op, err := client.New(context.Background(), timestamp.Timestamp{})
		require.NoError(t, err)
		old := op.(*txnOperator)
		old.addFlag(AbortedFlag)
		require.NoError(t, old.Rollback(context.Background()))

		restarted, err := client.RestartTxn(
			context.Background(),
			old,
			timestamp.Timestamp{})
		require.NoError(t, err)
		current := restarted.(*txnOperator)
		require.False(t, txnMarkedAborted(current))
		require.NoError(t, current.Rollback(context.Background()))
	})
}

func TestRestartTxnAdmissionFailureLeavesOperatorClosed(t *testing.T) {
	RunTxnTests(func(c TxnClient, _ rpc.TxnSender) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		client := c.(*txnClient)
		op, err := client.New(ctx, timestamp.Timestamp{})
		require.NoError(t, err)
		require.NoError(t, op.Rollback(ctx))

		canceledCtx, cancelRestart := context.WithCancel(ctx)
		cancelRestart()
		_, err = client.RestartTxn(canceledCtx, op, timestamp.Timestamp{})
		require.ErrorIs(t, err, context.Canceled)

		tc := op.(*txnOperator)
		tc.mu.RLock()
		require.True(t, tc.mu.closed)
		require.Equal(t, txn.TxnStatus_Aborted, tc.mu.txn.Status)
		tc.mu.RUnlock()

		rejectedCtx, rejectedCancel := context.WithCancel(context.Background())
		token, err := tc.TryEnterRunSqlWithTokenAndSQL(rejectedCancel, "sql after failed restart")
		require.Zero(t, token)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed))
		select {
		case <-rejectedCtx.Done():
		case <-time.After(time.Second):
			t.Fatal("failed restart did not keep the SQL gate sealed")
		}

		restarted, err := client.RestartTxn(ctx, op, timestamp.Timestamp{})
		require.NoError(t, err)
		require.NoError(t, restarted.Rollback(ctx))
	})
}

func TestRestartTxnCanceledSnapshotReleasesAdmission(t *testing.T) {
	waiter := &blockingTimestampWaiter{entered: make(chan struct{}, 1)}
	RunTxnTests(func(c TxnClient, _ rpc.TxnSender) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		client := c.(*txnClient)
		op, err := client.New(ctx, timestamp.Timestamp{}, WithUserTxn())
		require.NoError(t, err)
		require.NoError(t, op.Rollback(ctx))

		client.timestampWaiter = waiter
		restartCtx, cancelRestart := context.WithCancel(ctx)
		errC := make(chan error, 1)
		go func() {
			_, err := client.RestartTxn(restartCtx, op, timestamp.Timestamp{}, WithUserTxn())
			errC <- err
		}()

		select {
		case <-waiter.entered:
		case <-ctx.Done():
			t.Fatal("restart did not enter snapshot acquisition")
		}
		cancelRestart()
		require.ErrorIs(t, <-errC, context.Canceled)

		require.Zero(t, client.atomic.activeTxnCount.Load())
		client.mu.RLock()
		require.Zero(t, client.mu.users)
		require.Empty(t, client.mu.waitActiveTxns)
		client.mu.RUnlock()

		// Failure closes the generation without poisoning operator reuse.
		client.timestampWaiter = nil
		restarted, err := client.RestartTxn(ctx, op, timestamp.Timestamp{}, WithUserTxn())
		require.NoError(t, err)
		require.NoError(t, restarted.Rollback(ctx))
	}, WithMaxActiveTxn(1))
}

func TestRestartTxnCanceledMaxActiveWaitReleasesAdmission(t *testing.T) {
	RunTxnTests(func(c TxnClient, _ rpc.TxnSender) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		client := c.(*txnClient)

		reusable, err := client.New(ctx, timestamp.Timestamp{})
		require.NoError(t, err)
		require.NoError(t, reusable.Rollback(ctx))
		holder, err := client.New(ctx, timestamp.Timestamp{}, WithUserTxn())
		require.NoError(t, err)

		restartCtx, cancelRestart := context.WithCancel(ctx)
		errC := make(chan error, 1)
		go func() {
			_, err := client.RestartTxn(
				restartCtx,
				reusable,
				timestamp.Timestamp{},
				WithUserTxn(),
				WithSkipPushClientReady(),
			)
			errC <- err
		}()

		require.Eventually(t, func() bool {
			client.mu.RLock()
			defer client.mu.RUnlock()
			return len(client.mu.waitActiveTxns) == 1
		}, time.Second, time.Millisecond)
		cancelRestart()
		require.ErrorIs(t, <-errC, context.Canceled)

		client.mu.RLock()
		require.Equal(t, 1, client.mu.users)
		require.Empty(t, client.mu.waitActiveTxns)
		client.mu.RUnlock()
		require.Equal(t, int64(1), client.atomic.activeTxnCount.Load())

		require.NoError(t, holder.Rollback(ctx))
		restarted, err := client.RestartTxn(ctx, reusable, timestamp.Timestamp{}, WithUserTxn())
		require.NoError(t, err)
		require.NoError(t, restarted.Rollback(ctx))
	}, WithMaxActiveTxn(1))
}

func TestRestartTxnCannotReopenRunSQLAfterConcurrentClose(t *testing.T) {
	op := &txnOperator{}
	op.reset.txnID = []byte("restart")
	op.reset.runSQLTracker.seal()
	op.mu.closed = true

	err := op.openRunSQLAfterRestart()
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed))
	_, err = op.TryEnterRunSqlWithTokenAndSQL(nil, "select 1")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed))
}

func TestRestartTxnFinalizationRejectsClosedClient(t *testing.T) {
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	client := NewTxnClient("", newTestTxnSender()).(*txnClient)
	client.Resume()

	op, err := client.New(
		context.Background(),
		timestamp.Timestamp{},
		WithUserTxn())
	require.NoError(t, err)
	operator := op.(*txnOperator)
	require.NoError(t, operator.Rollback(context.Background()))
	require.NoError(t, operator.claimRestart())
	operator.initForRestart(
		client.newTxnMeta(),
		client.getTxnOptions([]TxnOption{WithUserTxn()})...)
	admitted, err := client.doCreateTxn(
		context.Background(),
		operator,
		timestamp.Timestamp{})
	require.NoError(t, err)
	require.Same(t, operator, admitted)
	require.Equal(t, int64(1), client.atomic.activeTxnCount.Load())
	client.mu.RLock()
	users := client.mu.users
	client.mu.RUnlock()
	require.Equal(t, 1, users)

	require.NoError(t, client.Close())
	err = client.completeRestartTxn(context.Background(), operator)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrClientClosed))
	require.Zero(t, client.atomic.activeTxnCount.Load())
	client.mu.RLock()
	users = client.mu.users
	client.mu.RUnlock()
	require.Zero(t, users)
	operator.mu.RLock()
	closed := operator.mu.closed
	operator.mu.RUnlock()
	require.True(t, closed)
	_, err = operator.TryEnterRunSqlWithTokenAndSQL(nil, "select 1")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed))
}

func TestRestartTxnClaimsClosedOperatorOnce(t *testing.T) {
	op := &txnOperator{}
	op.reset.runSQLTracker.seal()
	op.mu.closed = true
	op.mu.terminalOutcome = txnTerminalOutcomeSucceeded

	start := make(chan struct{})
	results := make(chan error, 2)
	for i := 0; i < cap(results); i++ {
		go func() {
			<-start
			results <- op.claimRestart()
		}()
	}
	close(start)

	successes := 0
	for i := 0; i < cap(results); i++ {
		err := <-results
		if err == nil {
			successes++
		} else {
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed))
		}
	}
	require.Equal(t, 1, successes)
}

func TestNewTxnWithNormalStateWait(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rt := runtime.NewRuntime(metadata.ServiceType_CN, "",
		logutil.GetPanicLogger(),
		runtime.WithClock(clock.NewHLCClock(func() int64 {
			return 1
		}, 0)))
	runtime.SetupServiceBasedRuntime("", rt)
	c := NewTxnClient("", newTestTxnSender())
	defer func() {
		require.NoError(t, c.Close())
	}()

	// Do not resume the txn client for now.
	// c.Resume()
	const waiters = 4
	var wg sync.WaitGroup
	errs := make(chan error, waiters)
	waitContexts := make([]*observedWaitContext, 0, waiters)
	for i := 0; i < waiters; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		waitCtx := &observedWaitContext{
			Context: ctx,
			waiting: make(chan struct{}),
		}
		waitContexts = append(waitContexts, waitCtx)
		wg.Add(1)
		go func(ctx context.Context) {
			defer wg.Done()
			tx, err := c.New(ctx, newTestTimestamp(0))
			if err != nil {
				errs <- err
				return
			}
			txnMeta := tx.(*txnOperator).mu.txn
			if txnMeta.SnapshotTS.PhysicalTime != 0 || len(txnMeta.ID) == 0 || txnMeta.Status != txn.TxnStatus_Active {
				errs <- assert.AnError
			}
		}(waitCtx)
	}
	for _, waitCtx := range waitContexts {
		select {
		case <-waitCtx.waiting:
		case <-time.After(time.Second):
			t.Fatal("New did not reach the paused wait")
		}
	}
	// Resume it now.
	c.Resume()
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

func TestNewTxnWithNormalStateNoWait(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rt := runtime.NewRuntime(metadata.ServiceType_CN, "",
		logutil.GetPanicLogger(),
		runtime.WithClock(clock.NewHLCClock(func() int64 {
			return 1
		}, 0)))
	runtime.SetupServiceBasedRuntime("", rt)
	c := NewTxnClient("", newTestTxnSender(), WithNormalStateNoWait(true))
	defer func() {
		require.NoError(t, c.Close())
	}()

	// Do not resume the txn client.
	// c.Resume()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
			defer cancel()
			tx, err := c.New(ctx, newTestTimestamp(0))
			assert.Error(t, err)
			assert.Nil(t, tx)
		}()
	}
	wg.Wait()
}

func TestNewTxnWithSnapshotTS(t *testing.T) {
	rt := runtime.NewRuntime(metadata.ServiceType_CN, "",
		logutil.GetPanicLogger(),
		runtime.WithClock(clock.NewHLCClock(func() int64 {
			return 1
		}, 0)))
	runtime.SetupServiceBasedRuntime("", rt)
	c := NewTxnClient("", newTestTxnSender())
	c.Resume()
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	tx, err := c.New(ctx, newTestTimestamp(0), WithSnapshotTS(timestamp.Timestamp{PhysicalTime: 10}))
	assert.Nil(t, err)
	txnMeta := tx.(*txnOperator).mu.txn
	assert.Equal(t, timestamp.Timestamp{PhysicalTime: 10}, txnMeta.SnapshotTS)
	assert.NotEmpty(t, txnMeta.ID)
	assert.Equal(t, txn.TxnStatus_Active, txnMeta.Status)
}

func TestTxnClientPauseAndResume(t *testing.T) {
	rt := runtime.NewRuntime(metadata.ServiceType_CN, "",
		logutil.GetPanicLogger(),
		runtime.WithClock(clock.NewHLCClock(func() int64 {
			return 1
		}, 0)))
	runtime.SetupServiceBasedRuntime("", rt)
	c := NewTxnClient("", newTestTxnSender())

	c.Pause()
	require.Equal(t, paused, c.(*txnClient).mu.state)
	c.Resume()
	require.Equal(t, normal, c.(*txnClient).mu.state)
}

func TestLimit(t *testing.T) {
	RunTxnTests(
		func(tc TxnClient, ts rpc.TxnSender) {
			ctx := context.Background()

			c := make(chan struct{})
			c2 := make(chan struct{})
			n := 0
			go func() {
				defer close(c2)
				for {
					select {
					case <-c:
						return
					default:
						op, err := tc.New(ctx, newTestTimestamp(0))
						require.NoError(t, err)
						require.NoError(t, op.Rollback(ctx))
						n++
					}
				}
			}()
			time.Sleep(time.Millisecond * 200)
			close(c)
			<-c2
			require.True(t, n < 5)
		},
		WithTxnLimit(1))
}

func TestTxnLimitWaitCancellationClosesUnadmittedRestart(t *testing.T) {
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	tc := NewTxnClient("", newTestTxnSender())
	defer func() { require.NoError(t, tc.Close()) }()
	tc.Resume()
	client := tc.(*txnClient)

	op, err := tc.New(context.Background(), timestamp.Timestamp{})
	require.NoError(t, err)
	require.NoError(t, op.Rollback(context.Background()))

	limiter := &blockingTxnRateLimiter{entered: make(chan struct{}, 1)}
	client.limiter = limiter
	ctx, cancel := context.WithCancel(context.Background())
	resultC := make(chan txnCreateResult, 1)
	go func() {
		restarted, err := client.RestartTxn(ctx, op, timestamp.Timestamp{})
		resultC <- txnCreateResult{op: restarted, err: err}
	}()

	select {
	case <-limiter.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("RestartTxn did not enter the rate-limit wait")
	}
	cancel()
	var result txnCreateResult
	select {
	case result = <-resultC:
	case <-time.After(5 * time.Second):
		t.Fatal("RestartTxn did not return after rate-limit cancellation")
	}
	require.Nil(t, result.op)
	require.ErrorIs(t, result.err, context.Canceled)
	require.Zero(t, client.atomic.activeTxnCount.Load())
	client.mu.RLock()
	require.Zero(t, client.mu.users)
	require.Empty(t, client.mu.waitActiveTxns)
	client.mu.RUnlock()

	// The canceled, unadmitted generation is terminally closed and can be
	// reused by a later explicit restart.
	client.limiter = unlimitedTxnRateLimiter{}
	restarted, err := client.RestartTxn(context.Background(), op, timestamp.Timestamp{})
	require.NoError(t, err)
	require.NoError(t, restarted.Rollback(context.Background()))
}

func TestTxnLimitWaitUnblockedByClientClose(t *testing.T) {
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	tc := NewTxnClient("", newTestTxnSender())
	defer func() { require.NoError(t, tc.Close()) }()
	tc.Resume()
	client := tc.(*txnClient)
	limiter := &blockingTxnRateLimiter{entered: make(chan struct{}, 1)}
	client.limiter = limiter

	errC := make(chan error, 1)
	go func() {
		_, err := tc.New(context.Background(), timestamp.Timestamp{})
		errC <- err
	}()
	select {
	case <-limiter.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("New did not enter the rate-limit wait")
	}

	require.NoError(t, tc.Close())
	select {
	case err := <-errC:
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrClientClosed))
	case <-time.After(5 * time.Second):
		t.Fatal("client Close did not unblock the rate-limit wait")
	}
	require.Zero(t, client.atomic.activeTxnCount.Load())
}

func TestWaitTxnRateLimitHonorsContextDeadline(t *testing.T) {
	limiter := rate.NewLimiter(0, 1)
	require.True(t, limiter.Allow())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := waitTxnRateLimit(limiter, ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestMaxActiveTxnWithWaitPrevClosed(t *testing.T) {
	RunTxnTests(
		func(tc TxnClient, ts rpc.TxnSender) {
			ctx := context.Background()
			op1, err := tc.New(ctx, newTestTimestamp(0), WithUserTxn())
			require.NoError(t, err)

			c := make(chan struct{})
			go func() {
				defer close(c)
				_, err = tc.New(ctx, newTestTimestamp(0), WithUserTxn())
				require.NoError(t, err)
			}()

			require.NoError(t, op1.Rollback(ctx))
			<-c
		},
		WithMaxActiveTxn(1))
}

func TestConcurrentOpenCloseTxn(t *testing.T) {
	RunTxnTests(
		func(tc TxnClient, ts rpc.TxnSender) {
			ctx := context.Background()
			const goroutines = 50
			const iterations = 100

			var wg sync.WaitGroup
			wg.Add(goroutines)

			for i := 0; i < goroutines; i++ {
				go func() {
					defer wg.Done()
					for j := 0; j < iterations; j++ {
						op, err := tc.New(ctx, newTestTimestamp(0))
						require.NoError(t, err)
						require.NoError(t, op.Rollback(ctx))
					}
				}()
			}

			wg.Wait()

			// Verify final state is consistent
			v := tc.(*txnClient)
			assert.Equal(t, int64(0), v.atomic.activeTxnCount.Load())
			v.mu.RLock()
			assert.Equal(t, 0, v.mu.users)
			assert.Equal(t, 0, len(v.mu.waitActiveTxns))
			v.mu.RUnlock()
		})
}

func TestMaxActiveTxnWithWaitTimeout(t *testing.T) {
	RunTxnTests(
		func(tc TxnClient, ts rpc.TxnSender) {
			ctx := context.Background()
			op1, err := tc.New(ctx, newTestTimestamp(0), WithUserTxn())
			require.NoError(t, err)
			defer func() {
				require.NoError(t, op1.Rollback(ctx))
			}()

			ctx2, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err = tc.New(ctx2, newTestTimestamp(0), WithUserTxn())
			require.Error(t, err)

			v := tc.(*txnClient)
			v.mu.Lock()
			defer v.mu.Unlock()
			require.Equal(t, 0, len(v.mu.waitActiveTxns))
		},
		WithMaxActiveTxn(1),
	)
}

func TestCloseUnblocksMaxActiveNew(t *testing.T) {
	RunTxnTests(
		func(tc TxnClient, _ rpc.TxnSender) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_, err := tc.New(ctx, timestamp.Timestamp{}, WithUserTxn())
			require.NoError(t, err)

			errC := make(chan error, 1)
			go func() {
				_, err := tc.New(context.Background(), timestamp.Timestamp{}, WithUserTxn())
				errC <- err
			}()

			client := tc.(*txnClient)
			require.Eventually(t, func() bool {
				client.mu.RLock()
				defer client.mu.RUnlock()
				return len(client.mu.waitActiveTxns) == 1
			}, time.Second, time.Millisecond)

			require.NoError(t, tc.Close())
			select {
			case err := <-errC:
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrClientClosed))
			case <-time.After(time.Second):
				t.Fatal("max-active New did not return after client close")
			}

			client.mu.RLock()
			defer client.mu.RUnlock()
			require.Empty(t, client.mu.waitActiveTxns)
		},
		WithMaxActiveTxn(1),
	)
}

func TestCloseUnblocksAllMaxActiveWaiters(t *testing.T) {
	RunTxnTests(
		func(tc TxnClient, _ rpc.TxnSender) {
			_, err := tc.New(context.Background(), timestamp.Timestamp{}, WithUserTxn())
			require.NoError(t, err)

			const waiters = 8
			errs := make(chan error, waiters)
			for range waiters {
				go func() {
					_, err := tc.New(context.Background(), timestamp.Timestamp{}, WithUserTxn())
					errs <- err
				}()
			}

			client := tc.(*txnClient)
			require.Eventually(t, func() bool {
				client.mu.RLock()
				defer client.mu.RUnlock()
				return len(client.mu.waitActiveTxns) == waiters
			}, time.Second, time.Millisecond)

			require.NoError(t, tc.Close())
			for range waiters {
				select {
				case err := <-errs:
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrClientClosed))
				case <-time.After(time.Second):
					t.Fatal("queued New did not return after client close")
				}
			}
			client.mu.RLock()
			defer client.mu.RUnlock()
			require.Empty(t, client.mu.waitActiveTxns)
		},
		WithMaxActiveTxn(1),
	)
}

func TestCloseCancelsQueuedSnapshotWait(t *testing.T) {
	waiter := &blockingTimestampWaiter{entered: make(chan struct{}, 1)}
	RunTxnTests(
		func(tc TxnClient, _ rpc.TxnSender) {
			_, err := tc.New(
				context.Background(),
				timestamp.Timestamp{},
				WithUserTxn(),
				WithSkipPushClientReady())
			require.NoError(t, err)

			errC := make(chan error, 1)
			go func() {
				_, err := tc.New(context.Background(), timestamp.Timestamp{}, WithUserTxn())
				errC <- err
			}()

			select {
			case <-waiter.entered:
			case <-time.After(time.Second):
				t.Fatal("queued transaction did not enter snapshot wait")
			}

			require.NoError(t, tc.Close())
			select {
			case err := <-errC:
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrClientClosed))
			case <-time.After(time.Second):
				t.Fatal("queued snapshot wait did not return after client close")
			}
		},
		WithMaxActiveTxn(1),
		WithTimestampWaiter(waiter),
	)
}

func TestCloseCancelsAdmittedSnapshotWait(t *testing.T) {
	waiter := &blockingTimestampWaiter{entered: make(chan struct{}, 1)}
	RunTxnTests(
		func(tc TxnClient, _ rpc.TxnSender) {
			errC := make(chan error, 1)
			go func() {
				_, err := tc.New(context.Background(), timestamp.Timestamp{}, WithUserTxn())
				errC <- err
			}()

			select {
			case <-waiter.entered:
			case <-time.After(time.Second):
				t.Fatal("admitted transaction did not enter snapshot wait")
			}

			require.NoError(t, tc.Close())
			select {
			case err := <-errC:
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrClientClosed))
			case <-time.After(time.Second):
				t.Fatal("admitted snapshot wait did not return after client close")
			}
			require.Zero(t, tc.(*txnClient).atomic.activeTxnCount.Load())
		},
		WithTimestampWaiter(waiter),
	)
}

func TestCloseCancelsRealTimestampWait(t *testing.T) {
	tw := NewTimestampWaiter(runtime.DefaultRuntime().Logger()).(*timestampWaiter)
	defer tw.Close()
	RunTxnTests(
		func(tc TxnClient, _ rpc.TxnSender) {
			errC := make(chan error, 1)
			go func() {
				_, err := tc.New(context.Background(), timestamp.Timestamp{}, WithUserTxn())
				errC <- err
			}()

			require.Eventually(t, func() bool {
				tw.mu.Lock()
				defer tw.mu.Unlock()
				return len(tw.mu.waiters) == 1
			}, time.Second, time.Millisecond)
			require.NoError(t, tc.Close())
			select {
			case err := <-errC:
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrClientClosed))
			case <-time.After(time.Second):
				t.Fatal("real timestamp wait did not return after client close")
			}
			tw.mu.Lock()
			defer tw.mu.Unlock()
			require.Empty(t, tw.mu.waiters)
		},
		WithTimestampWaiter(tw),
	)
}

func TestCloseDuringAdmittedNewCleansActiveState(t *testing.T) {
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	opened := make(chan struct{}, 1)
	release := make(chan struct{})
	c := NewTxnClient(
		"",
		newTestTxnSender(),
		WithTxnOpenedCallback([]func(TxnOperator){func(TxnOperator) {
			opened <- struct{}{}
			<-release
		}}),
	)
	c.Resume()

	errC := make(chan error, 1)
	go func() {
		_, err := c.New(context.Background(), timestamp.Timestamp{}, WithUserTxn())
		errC <- err
	}()
	select {
	case <-opened:
	case <-time.After(time.Second):
		t.Fatal("New did not finish admission")
	}

	require.NoError(t, c.Close())
	close(release)
	select {
	case err := <-errC:
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrClientClosed))
	case <-time.After(time.Second):
		t.Fatal("admitted New did not return after client close")
	}

	client := c.(*txnClient)
	require.Zero(t, client.atomic.activeTxnCount.Load())
	client.mu.RLock()
	defer client.mu.RUnlock()
	require.Zero(t, client.mu.users)
	require.Empty(t, client.mu.waitActiveTxns)
}

func TestCloseCancelsLegacySnapshotWait(t *testing.T) {
	waiter := &legacyBlockingTimestampWaiter{entered: make(chan struct{}, 1)}
	RunTxnTests(
		func(tc TxnClient, _ rpc.TxnSender) {
			errC := make(chan error, 1)
			go func() {
				_, err := tc.New(context.Background(), timestamp.Timestamp{}, WithUserTxn())
				errC <- err
			}()

			select {
			case <-waiter.entered:
			case <-time.After(time.Second):
				t.Fatal("legacy waiter did not enter snapshot wait")
			}

			require.NoError(t, tc.Close())
			select {
			case err := <-errC:
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrClientClosed))
			case <-time.After(time.Second):
				t.Fatal("legacy snapshot wait did not return after client close")
			}
		},
		WithTimestampWaiter(waiter),
	)
}

func TestCloseCancelsWaitLogTailAppliedAt(t *testing.T) {
	waiter := &legacyBlockingTimestampWaiter{entered: make(chan struct{}, 1)}
	RunTxnTests(
		func(tc TxnClient, _ rpc.TxnSender) {
			client := tc.(*txnClient)
			errC := make(chan error, 1)
			go func() {
				_, err := client.WaitLogTailAppliedAt(context.Background(), timestamp.Timestamp{})
				errC <- err
			}()

			select {
			case <-waiter.entered:
			case <-time.After(time.Second):
				t.Fatal("WaitLogTailAppliedAt did not enter timestamp wait")
			}

			require.NoError(t, tc.Close())
			select {
			case err := <-errC:
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrClientClosed))
			case <-time.After(time.Second):
				t.Fatal("WaitLogTailAppliedAt did not return after client close")
			}
		},
		WithTimestampWaiter(waiter),
	)
}

func TestCanceledMaxActiveWaitRemovesQueueEntry(t *testing.T) {
	RunTxnTests(
		func(tc TxnClient, _ rpc.TxnSender) {
			_, err := tc.New(context.Background(), timestamp.Timestamp{}, WithUserTxn())
			require.NoError(t, err)

			client := tc.(*txnClient)
			op := newTxnOperator(
				client.sid,
				client.clock,
				client.sender,
				client.newTxnMeta(),
				client.getTxnOptions([]TxnOption{WithUserTxn()})...)
			waitCtx, cancel := context.WithCancel(context.Background())
			errC := make(chan error, 1)
			go func() {
				_, err := client.doCreateTxn(waitCtx, op, timestamp.Timestamp{})
				errC <- err
			}()

			require.Eventually(t, func() bool {
				client.mu.RLock()
				defer client.mu.RUnlock()
				return len(client.mu.waitActiveTxns) == 1
			}, time.Second, time.Millisecond)
			cancel()

			select {
			case err = <-errC:
			case <-time.After(time.Second):
				t.Fatal("canceled max-active New did not return")
			}
			require.ErrorIs(t, err, context.Canceled)

			require.Eventually(t, func() bool {
				client.mu.RLock()
				defer client.mu.RUnlock()
				return len(client.mu.waitActiveTxns) == 0
			}, time.Second, time.Millisecond)
		},
		WithMaxActiveTxn(1),
	)
}

func TestCanceledMaxActivePromotionReleasesOwnership(t *testing.T) {
	RunTxnTests(
		func(tc TxnClient, _ rpc.TxnSender) {
			ctx := context.Background()
			active, err := tc.New(ctx, timestamp.Timestamp{}, WithUserTxn())
			require.NoError(t, err)

			client := tc.(*txnClient)
			activeShard := client.getActiveTxnShard(string(active.Txn().ID))
			var meta txn.TxnMeta
			for {
				meta = client.newTxnMeta()
				if client.getActiveTxnShard(string(meta.ID)) != activeShard {
					break
				}
			}

			waitEntered := make(chan struct{})
			waiting := newTxnOperator(
				client.sid,
				client.clock,
				client.sender,
				meta,
				client.getTxnOptions([]TxnOption{
					WithUserTxn(),
					WithWaitActiveHandle(func() { close(waitEntered) }),
				})...)
			waitCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			waitErrC := make(chan error, 1)
			go func() {
				_, err := client.doCreateTxn(waitCtx, waiting, timestamp.Timestamp{})
				waitErrC <- err
			}()

			select {
			case <-waitEntered:
			case <-time.After(time.Second):
				t.Fatal("queued transaction did not enter active wait")
			}

			// Stop promotion exactly after it claims/dequeues the waiter and before
			// addActiveTxn can publish ownership.
			waitingShard := client.getActiveTxnShard(string(meta.ID))
			waitingShard.Lock()
			shardLocked := true
			defer func() {
				if shardLocked {
					waitingShard.Unlock()
				}
			}()

			activeCloseErrC := make(chan error, 1)
			go func() { activeCloseErrC <- active.Rollback(ctx) }()
			require.Eventually(t, func() bool {
				waiting.reset.waiter.mu.Lock()
				defer waiting.reset.waiter.mu.Unlock()
				return waiting.reset.waiter.mu.state == activeTxnPromoting
			}, time.Second, time.Millisecond)

			cancel()
			require.Eventually(t, func() bool {
				waiting.reset.waiter.mu.Lock()
				defer waiting.reset.waiter.mu.Unlock()
				return waiting.reset.waiter.mu.state == activeTxnPromotionCanceled
			}, time.Second, time.Millisecond)
			select {
			case err := <-waitErrC:
				t.Fatalf("canceled creator returned before active publication: %v", err)
			default:
			}

			waitingShard.Unlock()
			shardLocked = false
			select {
			case err := <-activeCloseErrC:
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Fatal("active rollback did not finish after promotion publication")
			}
			select {
			case err := <-waitErrC:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("canceled promotion did not finish after active publication")
			}

			require.Zero(t, client.atomic.activeTxnCount.Load())
			client.mu.RLock()
			require.Zero(t, client.mu.users)
			require.Empty(t, client.mu.waitActiveTxns)
			client.mu.RUnlock()
			_, exists := client.getActiveTxn(string(meta.ID))
			require.False(t, exists)

			// The canceled promotion must return its max-active capacity.
			next, err := tc.New(ctx, timestamp.Timestamp{}, WithUserTxn())
			require.NoError(t, err)
			require.NoError(t, next.Rollback(ctx))
		},
		WithMaxActiveTxn(1),
	)
}

func TestQueuedSnapshotFailureDuringPromotionReleasesOwnership(t *testing.T) {
	waiter := &blockingTimestampWaiter{entered: make(chan struct{}, 1)}
	RunTxnTests(
		func(tc TxnClient, _ rpc.TxnSender) {
			ctx := context.Background()
			active, err := tc.New(
				ctx,
				timestamp.Timestamp{},
				WithUserTxn(),
				WithSkipPushClientReady())
			require.NoError(t, err)

			client := tc.(*txnClient)
			activeShard := client.getActiveTxnShard(string(active.Txn().ID))
			var meta txn.TxnMeta
			for {
				meta = client.newTxnMeta()
				if client.getActiveTxnShard(string(meta.ID)) != activeShard {
					break
				}
			}
			waiting := newTxnOperator(
				client.sid,
				client.clock,
				client.sender,
				meta,
				client.getTxnOptions([]TxnOption{WithUserTxn()})...)
			waitCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			waitErrC := make(chan error, 1)
			go func() {
				_, err := client.doCreateTxn(waitCtx, waiting, timestamp.Timestamp{})
				waitErrC <- err
			}()

			select {
			case <-waiter.entered:
			case <-time.After(time.Second):
				t.Fatal("queued transaction did not enter snapshot acquisition")
			}

			// Hold the target shard so the active owner can claim and dequeue the
			// transaction but cannot publish it yet.
			waitingShard := client.getActiveTxnShard(string(meta.ID))
			waitingShard.Lock()
			shardLocked := true
			defer func() {
				if shardLocked {
					waitingShard.Unlock()
				}
			}()

			activeCloseErrC := make(chan error, 1)
			go func() { activeCloseErrC <- active.Rollback(ctx) }()
			require.Eventually(t, func() bool {
				waiting.reset.waiter.mu.Lock()
				defer waiting.reset.waiter.mu.Unlock()
				return waiting.reset.waiter.mu.state == activeTxnPromoting
			}, time.Second, time.Millisecond)

			cancel()
			require.Eventually(t, func() bool {
				waiting.reset.waiter.mu.Lock()
				defer waiting.reset.waiter.mu.Unlock()
				return waiting.reset.waiter.mu.state == activeTxnPromotionCanceled
			}, time.Second, time.Millisecond)
			select {
			case err := <-waitErrC:
				t.Fatalf("snapshot failure returned before active publication: %v", err)
			default:
			}

			waitingShard.Unlock()
			shardLocked = false
			select {
			case err := <-activeCloseErrC:
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Fatal("active rollback did not finish after snapshot-failure promotion")
			}
			select {
			case err := <-waitErrC:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("snapshot failure did not finish after active publication")
			}

			require.Zero(t, client.atomic.activeTxnCount.Load())
			client.mu.RLock()
			require.Zero(t, client.mu.users)
			require.Empty(t, client.mu.waitActiveTxns)
			client.mu.RUnlock()
			_, exists := client.getActiveTxn(string(meta.ID))
			require.False(t, exists)

			next, err := tc.New(
				ctx,
				timestamp.Timestamp{},
				WithUserTxn(),
				WithSkipPushClientReady())
			require.NoError(t, err)
			require.NoError(t, next.Rollback(ctx))
		},
		WithMaxActiveTxn(1),
		WithTimestampWaiter(waiter),
	)
}

func TestOpenTxnWithWaitPausedDisabled(t *testing.T) {
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	c := &txnClient{}
	c.adjust()
	c.mu.state = paused

	op := &txnOperator{}
	op.opts.options = op.opts.options.WithDisableWaitPaused()

	require.Error(t, c.openTxn(context.Background(), op))
}

func TestOpenTxnSkipReadyStillRejectsCanceledContext(t *testing.T) {
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	c := &txnClient{}
	c.adjust()
	c.mu.state = normal

	op := &txnOperator{}
	op.reset.txnID = []byte("canceled-skip-ready")
	WithSkipPushClientReady()(op)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, c.openTxn(ctx, op), context.Canceled)
	require.Zero(t, c.atomic.activeTxnCount.Load())
	c.mu.RLock()
	require.Zero(t, c.mu.users)
	require.Empty(t, c.mu.waitActiveTxns)
	c.mu.RUnlock()
}

func TestCloseTxnWithAbortAllCheck(t *testing.T) {
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	c := &txnClient{}
	c.adjust()
	c.mu.state = normal

	// Create and add a txn
	op := &txnOperator{}
	op.reset.txnID = []byte("test-txn")
	op.reset.createAt = time.Now()
	c.addActiveTxn(op)

	// Verify txn is in active map
	_, ok := c.getActiveTxn("test-txn")
	require.True(t, ok)

	// Close with ErrCannotCommitOnInvalidCN should mark all active txns aborted
	// The txn should still be in map when markAllActiveTxnAborted is called
	event := TxnEvent{
		Txn: txn.TxnMeta{ID: []byte("test-txn")},
		Err: moerr.NewCannotCommitOnInvalidCNNoCtx(),
	}
	_ = c.closeTxn(context.Background(), op, event, nil)

	// Verify txn is removed after close
	_, ok = c.getActiveTxn("test-txn")
	require.False(t, ok)
}

func TestNewWithUpdateSnapshotTimeout(t *testing.T) {
	rt := runtime.NewRuntime(metadata.ServiceType_CN, "",
		logutil.GetPanicLogger(),
		runtime.WithClock(clock.NewHLCClock(func() int64 {
			return 1
		}, 0)))
	runtime.SetupServiceBasedRuntime("", rt)
	c := NewTxnClient(
		"",
		newTestTxnSender(),
		WithEnableSacrificingFreshness(),
		WithTimestampWaiter(NewTimestampWaiter(rt.Logger())),
	)
	c.Resume()
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	_, err := c.New(ctx, newTestTimestamp(10000))
	assert.Error(t, err)
	v := c.(*txnClient)
	v.mu.Lock()
	assert.Equal(t, 0, len(v.mu.waitActiveTxns))
	v.mu.Unlock()
}

func TestWaitAbortMarked(t *testing.T) {
	var fp *FootPrints
	assert.Equal(t, "", fp.String())
	c := make(chan struct{})
	tc := &txnClient{}
	tc.mu.waitMarkAllActiveAbortedC = c
	tc.mu.state = normal
	// Initialize sharded activeTxns
	for i := range tc.activeTxns {
		tc.activeTxns[i].txns = make(map[string]*txnOperator)
	}
	go func() {
		close(c)
	}()
	op := &txnOperator{}
	require.NoError(t, tc.openTxn(context.Background(), op))
}

func TestDirectAdmissionPublishedAfterAbortScanIsMarked(t *testing.T) {
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	tc := NewTxnClient("", newTestTxnSender())
	defer func() { require.NoError(t, tc.Close()) }()
	tc.Resume()
	client := tc.(*txnClient)

	active, err := tc.New(context.Background(), timestamp.Timestamp{})
	require.NoError(t, err)
	client.Pause()

	waitCtx := &observedWaitContext{
		Context: context.Background(),
		waiting: make(chan struct{}),
	}
	resultC := make(chan txnCreateResult, 1)
	go func() {
		created, err := tc.New(waitCtx, timestamp.Timestamp{})
		resultC <- txnCreateResult{op: created, err: err}
	}()

	select {
	case <-waitCtx.waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("direct admission did not pause before publication")
	}

	// Keep the abort worker inside its scan until its barrier is observable.
	// The paused transaction is older than the abort observation but is not in
	// the active map, so only the creator-side handshake can mark it.
	activeShard := client.getActiveTxnShard(string(active.Txn().ID))
	finishAbortScanWhileShardLocked(t, client, activeShard)

	client.Resume()
	var result txnCreateResult
	select {
	case result = <-resultC:
	case <-time.After(5 * time.Second):
		t.Fatal("direct admission did not finish after resume")
	}
	require.NoError(t, result.err)
	created := result.op
	op := created.(*txnOperator)
	require.True(t, txnMarkedAborted(op))

	require.NoError(t, active.Rollback(context.Background()))
	require.NoError(t, created.Rollback(context.Background()))
}

func TestQueuedAdmissionPublishedAfterAbortScanIsMarked(t *testing.T) {
	RunTxnTests(
		func(tc TxnClient, _ rpc.TxnSender) {
			client := tc.(*txnClient)
			active, err := tc.New(
				context.Background(),
				timestamp.Timestamp{},
				WithUserTxn())
			require.NoError(t, err)

			waitEntered := make(chan struct{})
			resultC := make(chan txnCreateResult, 1)
			go func() {
				created, err := tc.New(
					context.Background(),
					timestamp.Timestamp{},
					WithUserTxn(),
					WithWaitActiveHandle(func() { close(waitEntered) }))
				resultC <- txnCreateResult{op: created, err: err}
			}()

			select {
			case <-waitEntered:
			case <-time.After(5 * time.Second):
				t.Fatal("queued admission did not reach the active wait")
			}

			// Finish an abort scan while the queued transaction is unpublished.
			// Promotion after this point must retain the abort observation.
			activeShard := client.getActiveTxnShard(string(active.Txn().ID))
			finishAbortScanWhileShardLocked(t, client, activeShard)

			require.NoError(t, active.Rollback(context.Background()))
			var result txnCreateResult
			select {
			case result = <-resultC:
			case <-time.After(5 * time.Second):
				t.Fatal("queued admission did not finish after promotion")
			}
			require.NoError(t, result.err)
			created := result.op
			op := created.(*txnOperator)
			require.True(t, txnMarkedAborted(op))
			require.NoError(t, created.Rollback(context.Background()))
		},
		WithMaxActiveTxn(1))
}

func TestOpenTxnReturnsWhenPausedContextCanceled(t *testing.T) {
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	c := NewTxnClient("", newTestTxnSender())
	defer func() { require.NoError(t, c.Close()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := c.New(ctx, timestamp.Timestamp{})
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestCloseUnblocksPausedNew(t *testing.T) {
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	c := NewTxnClient("", newTestTxnSender())
	ctx := &observedWaitContext{
		Context: context.Background(),
		waiting: make(chan struct{}),
	}
	errC := make(chan error, 1)
	go func() {
		_, err := c.New(ctx, timestamp.Timestamp{})
		errC <- err
	}()

	select {
	case <-ctx.waiting:
	case <-time.After(time.Second):
		t.Fatal("paused New did not reach the wait")
	}
	require.NoError(t, c.Close())
	select {
	case err := <-errC:
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrClientClosed))
	case <-time.After(time.Second):
		t.Fatal("paused New did not return after client close")
	}
}

func TestCloseUnblocksAbortMarkingNew(t *testing.T) {
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	c := NewTxnClient("", newTestTxnSender())
	c.Resume()
	client := c.(*txnClient)
	client.mu.Lock()
	client.mu.waitMarkAllActiveAbortedC = make(chan struct{})
	client.mu.Unlock()

	ctx := &observedWaitContext{
		Context: context.Background(),
		waiting: make(chan struct{}),
	}
	errC := make(chan error, 1)
	go func() {
		_, err := c.New(ctx, timestamp.Timestamp{})
		errC <- err
	}()

	select {
	case <-ctx.waiting:
	case <-time.After(time.Second):
		t.Fatal("New did not reach the abort-marking wait")
	}
	require.NoError(t, c.Close())
	select {
	case err := <-errC:
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrClientClosed))
	case <-time.After(time.Second):
		t.Fatal("abort-marking New did not return after client close")
	}
}

func TestClosedClientRejectsNewAndSnapshot(t *testing.T) {
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	c := NewTxnClient("", newTestTxnSender())
	require.NoError(t, c.Close())

	_, err := c.New(context.Background(), timestamp.Timestamp{})
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrClientClosed))
	_, err = c.NewWithSnapshot(txn.CNTxnSnapshot{})
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrClientClosed))
}

func TestConcurrentCloseClosesSenderOnce(t *testing.T) {
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	sender := &blockingCloseTxnSender{
		testTxnSender: newTestTxnSender(),
		closeStarted:  make(chan struct{}, 1),
		closeRelease:  make(chan struct{}),
	}
	c := NewTxnClient("", sender)

	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() { first <- c.Close() }()
	select {
	case <-sender.closeStarted:
	case <-time.After(time.Second):
		t.Fatal("first Close did not reach sender close")
	}
	secondStarted := make(chan struct{})
	go func() {
		close(secondStarted)
		second <- c.Close()
	}()
	<-secondStarted
	close(sender.closeRelease)
	require.NoError(t, <-first)
	require.NoError(t, <-second)
	require.Equal(t, 1, sender.calls())
}

func TestCloseIsIdempotentAndPauseResumeAfterCloseAreNoOps(t *testing.T) {
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	release := make(chan struct{})
	close(release)
	sender := &blockingCloseTxnSender{
		testTxnSender: newTestTxnSender(),
		closeStarted:  make(chan struct{}, 1),
		closeRelease:  release,
		closeErr:      assert.AnError,
	}
	client := NewTxnClient("", sender).(*txnClient)

	require.ErrorIs(t, client.Close(), assert.AnError)
	require.ErrorIs(t, client.Close(), assert.AnError)
	require.Equal(t, 1, sender.calls())

	client.mu.RLock()
	state := client.mu.state
	client.mu.RUnlock()
	client.Pause()
	client.Resume()
	client.mu.RLock()
	require.True(t, client.mu.closed)
	require.Equal(t, state, client.mu.state)
	client.mu.RUnlock()
}

func TestActiveTxnWaiterConcurrentComplete(t *testing.T) {
	w := newActiveTxnWaiter()
	start := make(chan struct{})
	done := make(chan struct{}, 2)
	for _, err := range []error{assert.AnError, context.Canceled} {
		go func() {
			<-start
			w.complete(err)
			done <- struct{}{}
		}()
	}
	close(start)
	<-done
	<-done

	err := w.wait(context.Background())
	require.True(t, errors.Is(err, assert.AnError) || errors.Is(err, context.Canceled))
	result, completed := w.result()
	require.True(t, completed)
	require.ErrorIs(t, result, err)
}

func TestWithCloseContextPropagatesClientClose(t *testing.T) {
	client := &txnClient{}
	client.closeCtx, client.closeCancel = context.WithCancel(context.Background())
	ctx, cancel := client.withCloseContext(context.Background())
	defer cancel()

	client.closeCancel()
	select {
	case <-ctx.Done():
		require.ErrorIs(t, ctx.Err(), context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("derived context did not observe client close")
	}
}

func TestOpenTxnReturnsWhenAbortMarkingContextCanceled(t *testing.T) {
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	c := &txnClient{}
	c.adjust()
	c.mu.state = normal
	c.mu.waitMarkAllActiveAbortedC = make(chan struct{})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, c.openTxn(ctx, &txnOperator{}), context.DeadlineExceeded)
}

func TestMarkAllActiveTxnAbortedRetainsLatestObservation(t *testing.T) {
	c := &txnClient{abortC: make(chan struct{}, 1)}
	c.markAllActiveTxnAborted()
	latest := time.Now().Add(time.Hour)
	c.atomic.latestAbortAt.Store(&latest)
	c.markAllActiveTxnAborted()

	require.Len(t, c.abortC, 1)
	require.Equal(t, latest, *c.atomic.latestAbortAt.Load())
}
