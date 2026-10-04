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
	sender := NewTestSender()
	t.Cleanup(func() { require.NoError(t, sender.Close()) })
	txnService := NewTestTxnService(t, 1, sender, NewTestClock(0))
	t.Cleanup(func() { require.NoError(t, txnService.Close(false)) })
	require.NoError(t, txnService.Start())

	s := txnService.(*service)
	meta := NewTestTxn(1, 1, 1)
	id := string(meta.ID)
	t.Cleanup(func() {
		if value, ok := s.transactions.LoadAndDelete(id); ok {
			ctx := value.(*txnContext)
			ctx.mu.Lock()
			s.releaseTxnContext(ctx)
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
}
