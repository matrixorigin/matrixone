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

package service

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/txn/util"
	"github.com/stretchr/testify/require"
)

type closeTrackingSender struct {
	closed atomic.Int32
}

func (s *closeTrackingSender) Send(
	context.Context,
	[]txn.TxnRequest,
) (*rpc.SendResult, error) {
	return &rpc.SendResult{}, nil
}

func (s *closeTrackingSender) Close() error {
	s.closed.Add(1)
	return nil
}

func TestTxnServiceDoesNotCloseBorrowedSender(t *testing.T) {
	sender := new(closeTrackingSender)
	service := NewTestTxnService(t, 1, sender, NewTestClock(1))
	require.NoError(t, service.Start())
	require.NoError(t, service.Close(false))
	require.Zero(t, sender.closed.Load())
}

func TestMaybeAddTxnPublishesInitializedContext(t *testing.T) {
	t.Run("fresh_competitors", func(t *testing.T) {
		s := &service{logger: util.GetLogger("")}
		meta := NewTestTxn(1, 1, 1)
		id := string(meta.ID)
		t.Cleanup(func() {
			if value, ok := s.transactions.LoadAndDelete(id); ok {
				ctx := value.(*txnContext)
				ctx.mu.Lock()
				s.releaseTxnContextLocked(ctx)
				ctx.mu.Unlock()
			}
		})

		// Block both pool allocations so both callers pass the initial Load before
		// either one reaches the competing LoadOrStore.
		allocated := make(chan *txnContext, 2)
		release := make(chan struct{})
		var releaseOnce sync.Once
		unblock := func() { releaseOnce.Do(func() { close(release) }) }
		var workers sync.WaitGroup
		t.Cleanup(func() {
			unblock()
			workers.Wait()
		})
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		s.pool = sync.Pool{
			New: func() any {
				ctx := &txnContext{}
				allocated <- ctx
				<-release
				return ctx
			},
		}

		type outcome struct {
			ctx        *txnContext
			added      bool
			panicValue any
		}
		results := make(chan outcome, 2)
		for i := 0; i < 2; i++ {
			workers.Add(1)
			go func() {
				defer workers.Done()
				var result outcome
				defer func() {
					result.panicValue = recover()
					results <- result
				}()
				result.ctx, result.added = s.maybeAddTxn(meta)
			}()
		}

		receiveAllocated := func() *txnContext {
			select {
			case allocated := <-allocated:
				return allocated
			case <-ctx.Done():
				t.Fatal("competing transaction allocation did not reach the barrier")
				return nil
			}
		}
		receiveResult := func() outcome {
			select {
			case result := <-results:
				return result
			case <-ctx.Done():
				t.Fatal("competing transaction creation did not finish")
				return outcome{}
			}
		}
		ctxA := receiveAllocated()
		ctxB := receiveAllocated()
		unblock()
		resultA := receiveResult()
		resultB := receiveResult()

		require.Nil(t, resultA.panicValue)
		require.Nil(t, resultB.panicValue)
		require.NotEqual(t, resultA.added, resultB.added)
		require.Same(t, resultA.ctx, resultB.ctx)

		value, ok := s.transactions.Load(id)
		require.True(t, ok)
		stored := value.(*txnContext)
		require.Same(t, resultA.ctx, stored)

		func() {
			stored.mu.RLock()
			defer stored.mu.RUnlock()
			require.Equal(t, meta.ID, stored.mu.txn.ID)
			require.NotNil(t, stored.nt)
			require.False(t, stored.createAt.IsZero())
		}()

		loser := ctxA
		if loser == stored {
			loser = ctxB
		}
		func() {
			loser.mu.RLock()
			defer loser.mu.RUnlock()
			require.Nil(t, loser.nt)
			require.Empty(t, loser.mu.txn.ID)
		}()
	})
	t.Run("recycled_loser", func(t *testing.T) {
		s := &service{logger: util.GetLogger("")}
		oldMeta := NewTestTxn(1, 1, 1)
		meta := NewTestTxn(2, 1, 1)
		recycled := &txnContext{logger: s.logger}
		recycled.init(oldMeta, acquireNotifier())
		oldReference := recycled
		s.releaseTxnContext(recycled)

		winner := &txnContext{logger: s.logger}
		winner.init(meta, acquireNotifier())
		t.Cleanup(func() {
			s.transactions.Delete(string(meta.ID))
			s.releaseTxnContext(winner)
		})
		// Force reuse and a competing publication after the initial Load misses.
		// Pool.Get alone cannot guarantee which cached object will be selected.
		s.pool = sync.Pool{New: func() any {
			s.transactions.Store(string(meta.ID), winner)
			return recycled
		}}
		result, added := s.maybeAddTxn(meta)
		require.False(t, added)
		require.Same(t, winner, result)
		require.Same(t, winner, s.getTxnContext(meta.ID))
		require.Equal(t, meta.ID, winner.getTxn().ID)
		require.NotNil(t, winner.nt)
		require.False(t, winner.createAt.IsZero())
		require.Empty(t, oldReference.getTxn().ID)
		require.Nil(t, oldReference.nt)
		w := acquireWaiter()
		t.Cleanup(w.close)
		require.False(t, oldReference.addWaiter(oldMeta.ID, w, txn.TxnStatus_Committed))
	})
}

func TestReleaseTxnContextExcludesStaleReaders(t *testing.T) {
	for _, recycled := range []bool{false, true} {
		name := "fresh"
		if recycled {
			name = "recycled"
		}
		t.Run(name, func(t *testing.T) {
			s := &service{logger: util.GetLogger("")}
			c := &txnContext{logger: s.logger}
			oldReference := c
			if recycled {
				c.init(NewTestTxn(1, 1, 1), acquireNotifier())
				s.releaseTxnContext(c)
				s.pool = sync.Pool{New: func() any { return oldReference }}
				c = s.acquireTxnContext()
			}
			c.init(NewTestTxn(2, 1, 1), acquireNotifier())
			t.Cleanup(func() {
				if c.nt != nil {
					s.releaseTxnContext(c)
				}
			})
			first, last := acquireWaiter(), acquireWaiter()
			t.Cleanup(first.close)
			t.Cleanup(last.close)
			c.nt.addWaiter(first, txn.TxnStatus_Committed)
			c.nt.addWaiter(last, txn.TxnStatus_Committed)
			last.mu.Lock()
			var unblockOnce sync.Once
			unblock := func() { unblockOnce.Do(last.mu.Unlock) }
			done := make(chan struct{})
			t.Cleanup(func() {
				unblock()
				<-done
			})
			go func() {
				defer close(done)
				s.releaseTxnContext(c)
			}()
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			// The first notification proves cleanup has entered; the second
			// waiter's lock holds it there without blocking on the context lock.
			status, err := first.wait(ctx)
			require.NoError(t, err)
			require.Equal(t, txn.TxnStatus_Active, status)
			readable := oldReference.mu.TryRLock()
			if readable {
				oldReference.mu.RUnlock()
			}
			require.False(t, readable, "reset must exclude readers retaining an old pointer")
			unblock()
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("context release did not finish")
			}
			require.Empty(t, oldReference.getTxn().ID)
			require.Nil(t, oldReference.nt)
			status, err = last.wait(ctx)
			require.NoError(t, err)
			require.Equal(t, txn.TxnStatus_Active, status)
		})
	}
}
