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
)

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

	tx, err = c.RestartTxn(ctx, tx, newTestTimestamp(0))
	assert.Nil(t, err)
	txnMeta = tx.(*txnOperator).mu.txn
	assert.Equal(t, timestamp.Timestamp{PhysicalTime: 0}, txnMeta.SnapshotTS)
	assert.NotEmpty(t, txnMeta.ID)
	assert.Equal(t, txn.TxnStatus_Active, txnMeta.Status)
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
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
			defer cancel()
			tx, err := c.New(ctx, newTestTimestamp(0))
			assert.Nil(t, err)
			txnMeta := tx.(*txnOperator).mu.txn
			assert.Equal(t, int64(0), txnMeta.SnapshotTS.PhysicalTime)
			assert.NotEmpty(t, txnMeta.ID)
			assert.Equal(t, txn.TxnStatus_Active, txnMeta.Status)
		}()
	}
	// Resume it now.
	c.Resume()
	wg.Wait()
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

func TestOpenTxnWithWaitPausedDisabled(t *testing.T) {
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	c := &txnClient{}
	c.adjust()
	c.mu.state = paused

	op := &txnOperator{}
	op.opts.options = op.opts.options.WithDisableWaitPaused()

	require.Error(t, c.openTxn(op))
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
	require.NoError(t, tc.openTxn(op))
}
