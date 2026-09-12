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
	txnService := NewTestTxnService(t, 1, sender, NewTestClock(0))
	require.NoError(t, txnService.Start())
	t.Cleanup(func() {
		require.NoError(t, txnService.Close(false))
		require.NoError(t, sender.Close())
	})

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
		go func() {
			var result outcome
			defer func() {
				result.panicValue = recover()
				results <- result
			}()
			result.ctx, result.added = s.maybeAddTxn(meta)
		}()
	}

	ctxA := <-allocated
	ctxB := <-allocated
	close(release)
	resultA := <-results
	resultB := <-results

	require.Nil(t, resultA.panicValue)
	require.Nil(t, resultB.panicValue)
	require.NotEqual(t, resultA.added, resultB.added)
	require.Same(t, resultA.ctx, resultB.ctx)

	value, ok := s.transactions.Load(id)
	require.True(t, ok)
	stored := value.(*txnContext)
	require.Same(t, resultA.ctx, stored)

	stored.mu.RLock()
	require.Equal(t, meta.ID, stored.mu.txn.ID)
	require.NotNil(t, stored.nt)
	require.False(t, stored.createAt.IsZero())
	stored.mu.RUnlock()

	loser := ctxA
	if loser == stored {
		loser = ctxB
	}
	loser.mu.RLock()
	require.Nil(t, loser.nt)
	require.Empty(t, loser.mu.txn.ID)
	loser.mu.RUnlock()
}
