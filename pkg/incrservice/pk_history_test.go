// Copyright 2023 Matrix Origin
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

package incrservice

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/stretchr/testify/require"
)

func TestReloadForPKRetryProtectsOwners(t *testing.T) {
	runServiceTests(t, 2, func(ctx context.Context, ss []*service, ops []client.TxnOperator) {
		s := ss[0]
		def := newTestTableDef(1)
		require.NoError(t, s.Create(ctx, 0, def, ops[0]))
		original := mustGetTableCache(t, s, ctx, 0)
		for _, op := range ops {
			refreshed, err := s.ReloadForPKRetry(ctx, 0, 0, op)
			require.NoError(t, err)
			require.False(t, refreshed)
			require.Same(t, original, mustGetTableCache(t, s, ctx, 0))
		}
		require.NoError(t, ops[0].Commit(ctx))
		held, err := s.acquireCommittedTableCache(ctx, 0)
		require.NoError(t, err)
		defer held.release()
		refreshed, err := s.ReloadForPKRetry(ctx, 0, 1, ops[1])
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged))
		require.False(t, refreshed)
		require.Same(t, held, mustGetTableCache(t, s, ctx, 0))
		refreshed, err = s.ReloadForPKRetry(ctx, 0, 0, ops[1])
		require.NoError(t, err)
		require.True(t, refreshed)
		old := held.(*tableCache)
		old.lifecycle.Lock()
		retired, closed := old.lifecycle.retired, old.lifecycle.closed
		old.lifecycle.Unlock()
		require.True(t, retired)
		require.False(t, closed, "an existing holder retains its allocation until release")
		replacement, err := s.acquireCommittedTableCache(ctx, 0)
		require.NoError(t, err)
		replacement.release()
		require.NotSame(t, held, replacement)
		require.NoError(t, s.SetOffset(ctx, 0, def[0].ColIndex, def[0].ColName, 100, ops[1]))
		key := privateResetKey{txnID: string(ops[1].Txn().ID), tableID: 0}
		s.mu.Lock()
		private := s.mu.private[key]
		committedBefore := s.mu.tables[0]
		s.mu.Unlock()
		require.NotNil(t, private)
		refreshed, err = s.ReloadForPKRetry(ctx, 0, 1, ops[1])
		require.NoError(t, err)
		require.False(t, refreshed)
		s.mu.Lock()
		preserved := s.mu.private[key]
		committed := s.mu.tables[0]
		s.mu.Unlock()
		require.Same(t, private, preserved)
		if committedBefore == nil {
			require.Nil(t, committed)
		} else {
			require.Same(t, committedBefore, committed)
		}
		require.NoError(t, ops[1].Rollback(ctx))
	})
}

func TestReloadForPKRetryPendingCancellation(t *testing.T) {
	runServiceTests(t, 1, func(ctx context.Context, ss []*service, ops []client.TxnOperator) {
		s := ss[0]
		pending := &pendingTableCacheCommit{ready: make(chan struct{})}
		s.mu.Lock()
		s.mu.pendingCommits[42] = pending
		s.mu.Unlock()
		t.Cleanup(func() { s.mu.Lock(); delete(s.mu.pendingCommits, 42); s.mu.Unlock() })
		waitCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		observed := &observedDoneContext{Context: waitCtx, observed: make(chan struct{})}
		result := make(chan error, 1)
		go func() { _, err := s.ReloadForPKRetry(observed, 42, 0, ops[0]); result <- err }()
		<-observed.observed
		cancel()
		require.ErrorIs(t, <-result, context.Canceled)
		s.mu.Lock()
		same := s.mu.pendingCommits[42]
		s.mu.Unlock()
		require.Same(t, pending, same)
		close(pending.ready)
		s.mu.Lock()
		delete(s.mu.pendingCommits, 42)
		s.mu.Unlock()
		refreshed, err := s.ReloadForPKRetry(ctx, 42, 0, ops[0])
		require.NoError(t, err)
		require.True(t, refreshed)
		s.Close()
		refreshed, err = s.ReloadForPKRetry(ctx, 42, 0, ops[0])
		require.False(t, refreshed)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged))
	})
}

func TestReloadForPKRetryFencesBuilder(t *testing.T) { testBlockedBuilderInvalidation(t, false, true) }
