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

	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/mem"
	"github.com/matrixorigin/matrixone/pkg/txn/util"
	"github.com/stretchr/testify/assert"
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

func TestZombieGCUsesTransactionCreationSnapshot(t *testing.T) {
	sender := NewTestSender()
	t.Cleanup(func() { assert.NoError(t, sender.Close()) })
	txnService := NewTestTxnServiceWithLogAndZombie(t, 1, sender, NewTestClock(0), nil, 10*time.Millisecond)
	s := txnService.(*service)
	expired := NewTestTxn(1, 1, 1)
	current := NewTestTxn(2, 2, 1)
	t.Cleanup(func() {
		s.stopper.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, meta := range []txn.TxnMeta{expired, current} {
			if s.getTxnContext(meta.ID) != nil {
				request := NewTestRollbackRequest(meta)
				response := txn.TxnResponse{}
				assert.NoError(t, s.Rollback(ctx, &request, &response))
				assert.Nil(t, response.TxnError)
			}
		}
		assert.NoError(t, txnService.Close(false))
	})
	// The constructor starts GC; join it before admitting fixture writes.
	s.stopper.Stop()
	require.NoError(t, txnService.Start())
	sender.AddTxnService(txnService)
	for _, meta := range []txn.TxnMeta{expired, current} {
		result, err := sender.Send(t.Context(), []txn.TxnRequest{NewTestWriteRequest(meta.ID[0], meta, 1)})
		require.NoError(t, err)
		require.Len(t, result.Responses, 1)
		require.Nil(t, result.Responses[0].TxnError)
	}

	storage := s.storage.(*mem.KVTxnStorage)
	for _, tc := range []struct {
		meta      txn.TxnMeta
		createdAt time.Time
	}{
		{expired, time.Now().Add(-time.Hour)},
		{current, time.Now().Add(time.Hour)},
	} {
		ctx := s.getTxnContext(tc.meta.ID)
		require.NotNil(t, ctx)
		require.NotNil(t, storage.GetUncommittedTxn(tc.meta.ID))
		ctx.mu.Lock()
		ctx.createAt = tc.createdAt
		ctx.mu.Unlock()
	}
	// Start the real collector only after both transaction ages are prepared.
	s.stopper = stopper.NewStopper(t.Name(), stopper.WithLogger(s.logger.RawLogger()))
	require.NoError(t, s.stopper.RunTask(s.gcZombieTxn))

	require.Eventually(t, func() bool {
		return s.getTxnContext(expired.ID) == nil
	}, 5*time.Second, 5*time.Millisecond, "GC must roll back the expired coordinator")
	require.Nil(t, storage.GetUncommittedTxn(expired.ID))
	require.NotNil(t, s.getTxnContext(current.ID))
	require.NotNil(t, storage.GetUncommittedTxn(current.ID))

	result, err := sender.Send(t.Context(), []txn.TxnRequest{NewTestRollbackRequest(current)})
	require.NoError(t, err)
	require.Len(t, result.Responses, 1)
	require.Nil(t, result.Responses[0].TxnError)
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
	for _, tc := range []struct {
		name   string
		sameID bool
		win    bool
	}{
		{"recycled_same_id_winner", true, true},
		{"recycled_same_id_loser", true, false},
		{"recycled_different_id_loser", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &service{logger: util.GetLogger("")}
			oldMeta := NewTestTxn(1, 1, 1)
			meta := NewTestTxn(2, 1, 1)
			if tc.sameID {
				meta = oldMeta
			}
			recycled := &txnContext{logger: s.logger}
			contexts := []*txnContext{recycled}
			winner := recycled
			if !tc.win {
				winner = &txnContext{logger: s.logger}
				contexts = append(contexts, winner)
				winner.mu.Lock()
				winner.initLocked(meta, acquireNotifier())
				winner.mu.Unlock()
			}
			t.Cleanup(func() {
				s.transactions.Delete(string(meta.ID))
				for _, c := range contexts {
					c.mu.Lock()
					if c.nt != nil {
						s.releaseTxnContextLocked(c)
					}
					c.mu.Unlock()
				}
			})
			recycled.mu.Lock()
			recycled.initLocked(oldMeta, acquireNotifier())
			s.releaseTxnContextLocked(recycled)
			recycled.mu.Unlock()
			// Force reuse and, for losers, a competing publication after Load
			// misses. Pool.Get alone cannot select a particular cached object.
			s.pool = sync.Pool{New: func() any {
				if !tc.win {
					s.transactions.Store(string(meta.ID), winner)
				}
				return recycled
			}}
			result, added := s.maybeAddTxn(meta)
			require.Equal(t, tc.win, added)
			for _, c := range contexts {
				unlocked := c.mu.TryLock()
				// No workers remain in this fixture; on failure the creation
				// call retained the lock. Release it before any fatal assertion.
				c.mu.Unlock()
				require.True(t, unlocked, "creation must release both outcome locks")
			}
			require.Same(t, winner, result)
			require.Same(t, winner, s.getTxnContext(meta.ID))
			winnerMeta, _ := winner.getTxnSnapshot()
			require.Equal(t, meta.ID, winnerMeta.ID)
			require.NotNil(t, winner.nt)
			require.False(t, winner.createAt.IsZero())
			if !tc.win {
				recycledMeta, _ := recycled.getTxnSnapshot()
				require.Empty(t, recycledMeta.ID)
				require.Nil(t, recycled.nt)
				w := acquireWaiter()
				t.Cleanup(w.close)
				require.False(t, recycled.addWaiter(oldMeta.ID, w, txn.TxnStatus_Committed))
			}
		})
	}
}

func TestTxnContextOwnershipExcludesStaleRequests(t *testing.T) {
	for _, recycled := range []bool{false, true} {
		name := "fresh"
		if recycled {
			name = "recycled"
		}
		t.Run(name, func(t *testing.T) {
			s := &service{logger: util.GetLogger("")}
			c := &txnContext{logger: s.logger}
			oldReference := c
			meta := NewTestTxn(1, 1, 1)
			if recycled {
				c.mu.Lock()
				c.initLocked(meta, acquireNotifier())
				s.releaseTxnContextLocked(c)
				c.mu.Unlock()
			}
			s.pool = sync.Pool{New: func() any { return oldReference }}
			c = s.acquireTxnContext()
			readable := oldReference.mu.TryRLock()
			if readable {
				oldReference.mu.RUnlock()
			}
			writable := oldReference.mu.TryLock()
			// Either acquisition or a successful write probe owns the lock.
			// Keep teardown safe even when testing an unlocked acquisition.
			owned := true
			t.Cleanup(func() {
				if owned {
					c.mu.Unlock()
				}
				c.mu.Lock()
				defer c.mu.Unlock()
				if c.nt != nil {
					s.releaseTxnContextLocked(c)
				}
			})
			require.False(t, readable, "acquisition must exclude stale readers")
			require.False(t, writable, "acquisition must exclude stale Rollback")
			assertExcluded := func() {
				readable := oldReference.mu.TryRLock()
				if readable {
					oldReference.mu.RUnlock()
				}
				writable := oldReference.mu.TryLock()
				if writable {
					oldReference.mu.Unlock()
				}
				require.False(t, readable)
				require.False(t, writable)
			}
			c.initLocked(meta, acquireNotifier())
			assertExcluded()
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
			owned = false // Transfer the locked cleanup phase to the worker.
			go func() {
				defer close(done)
				defer c.mu.Unlock()
				s.releaseTxnContextLocked(c)
			}()
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			// The first notification proves cleanup has entered; the second
			// waiter's lock holds it there without blocking on the context lock.
			status, err := first.wait(ctx)
			require.NoError(t, err)
			require.Equal(t, txn.TxnStatus_Active, status)
			assertExcluded()
			unblock()
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("context release did not finish")
			}
			retiredMeta, _ := oldReference.getTxnSnapshot()
			require.Empty(t, retiredMeta.ID)
			require.Nil(t, oldReference.nt)
			status, err = last.wait(ctx)
			require.NoError(t, err)
			require.Equal(t, txn.TxnStatus_Active, status)
		})
	}
}

func TestTxnContextSnapshotAcrossReuse(t *testing.T) {
	c := &txnContext{}
	first, second := NewTestTxn(1, 1, 1), NewTestTxn(2, 2, 1)
	c.mu.Lock()
	c.initLocked(first, acquireNotifier())
	c.mu.Unlock()
	t.Cleanup(func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.nt != nil {
			c.resetLocked()
		}
	})
	created := time.Unix(100, 0)
	c.mu.Lock()
	c.createAt = created
	c.mu.Unlock()
	meta, createdAt := c.getTxnSnapshot()
	require.Equal(t, first, meta)
	require.Equal(t, created, createdAt)

	c.mu.Lock()
	c.resetLocked()
	c.mu.Unlock()
	resetMeta, resetAt := c.getTxnSnapshot()
	// Reset metadata makes the GC coordinator filter skip this context;
	// the old timestamp alone must not identify an active transaction.
	require.Equal(t, txn.TxnMeta{}, resetMeta)
	require.Equal(t, created, resetAt)
	c.mu.Lock()
	c.initLocked(second, acquireNotifier())
	c.mu.Unlock()
	current, currentAt := c.getTxnSnapshot()
	require.Equal(t, second, current)
	c.mu.RLock()
	expectedAt := c.createAt
	c.mu.RUnlock()
	require.Equal(t, expectedAt, currentAt)
	require.Equal(t, first, meta, "a retained snapshot must survive reset/reuse")
	require.Equal(t, created, createdAt)
	unlocked := c.mu.TryLock()
	if unlocked {
		c.mu.Unlock()
	}
	require.True(t, unlocked, "snapshot must release its lock before GC can roll back")
}

func TestTxnContextSnapshotConcurrentReuse(t *testing.T) {
	const generations = 32
	c := &txnContext{}
	c.mu.Lock()
	c.initLocked(NewTestTxn(1, 1, 1), acquireNotifier())
	c.mu.Unlock()
	expected := make([]time.Time, generations)
	expected[0] = c.createAt
	metas := make([]txn.TxnMeta, generations*2)
	times := make([]time.Time, len(metas))
	ready, start := make(chan struct{}, 2), make(chan struct{})
	var workers sync.WaitGroup
	var startOnce sync.Once
	unblock := func() { startOnce.Do(func() { close(start) }) }
	t.Cleanup(func() {
		unblock()
		workers.Wait()
		c.mu.Lock()
		defer c.mu.Unlock()
		c.resetLocked()
	})
	workers.Add(2)
	go func() {
		defer workers.Done()
		ready <- struct{}{}
		<-start
		for i := 1; i < generations; i++ {
			c.mu.Lock()
			c.resetLocked()
			c.mu.Unlock()
			c.mu.Lock()
			c.initLocked(NewTestTxn(byte(i%2+1), int64(i+1), 1), acquireNotifier())
			c.mu.Unlock()
			c.mu.RLock()
			expected[i] = c.createAt
			c.mu.RUnlock()
		}
	}()
	go func() {
		defer workers.Done()
		ready <- struct{}{}
		<-start
		for i := range metas {
			metas[i], times[i] = c.getTxnSnapshot()
		}
	}()
	<-ready
	<-ready
	unblock()
	workers.Wait()
	for i, meta := range metas {
		if len(meta.ID) == 0 {
			continue // An observation between reset and init is valid.
		}
		generation := int(meta.SnapshotTS.PhysicalTime) - 1
		require.GreaterOrEqual(t, generation, 0)
		require.Less(t, generation, generations)
		require.Equal(t, expected[generation], times[i], "metadata and age must describe one initialization")
	}
}
