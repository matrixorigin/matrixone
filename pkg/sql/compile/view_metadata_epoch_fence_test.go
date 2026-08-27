// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func waitViewMetadataFenceAdvancing(t *testing.T, fence *ViewMetadataEpochFence) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		fence.mu.Lock()
		advancing := fence.advancing
		fence.mu.Unlock()
		if advancing {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("epoch fence did not enter advancing state")
		}
		runtime.Gosched()
	}
}

func TestViewMetadataEpochFenceDrainsBeforePublishing(t *testing.T) {
	fence := NewViewMetadataEpochFence()
	lease, err := fence.Acquire(context.Background())
	require.NoError(t, err)
	require.Zero(t, lease.Epoch())

	done := make(chan error, 1)
	go func() {
		done <- fence.Advance(context.Background(), 4)
	}()
	waitViewMetadataFenceAdvancing(t, fence)
	select {
	case err := <-done:
		t.Fatalf("epoch advanced before old lease drained: %v", err)
	default:
	}

	blockedCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = fence.Acquire(blockedCtx)
	require.ErrorIs(t, err, context.Canceled)

	lease.Release()
	lease.Release()
	require.NoError(t, <-done)
	require.Equal(t, uint64(4), fence.Epoch())
	newLease, err := fence.Acquire(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(4), newLease.Epoch())
	newLease.Release()
}

func TestViewMetadataEpochFenceCancellationKeepsOldEpoch(t *testing.T) {
	fence := NewViewMetadataEpochFence()
	lease, err := fence.Acquire(context.Background())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- fence.Advance(ctx, 2)
	}()
	waitViewMetadataFenceAdvancing(t, fence)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Zero(t, fence.Epoch())

	lease.Release()
	require.NoError(t, fence.Advance(context.Background(), 2))
	require.Equal(t, uint64(2), fence.Epoch())
}

func TestViewMetadataRefreshLeaseWaitsForCatalogFenceThenFailsClosed(t *testing.T) {
	fence := NewViewMetadataEpochFence()
	require.NoError(t, fence.Advance(context.Background(), 3))

	type result struct {
		lease    *ViewMetadataEpochLease
		acquired bool
		err      error
	}
	done := make(chan result, 1)
	go func() {
		lease, acquired, err := fence.AcquireRefresh(context.Background())
		done <- result{lease: lease, acquired: acquired, err: err}
	}()
	select {
	case got := <-done:
		t.Fatalf("refresh acquisition crossed the unfenced publication gap: %+v", got)
	default:
	}

	require.True(t, fence.MarkCatalogFenced(3))
	got := <-done
	require.NoError(t, got.err)
	require.False(t, got.acquired)
	require.NotNil(t, got.lease)
	require.Equal(t, uint64(3), got.lease.Epoch())
	got.lease.Release()
}

func TestViewMetadataRefreshLeaseDrainsEnabledEpoch(t *testing.T) {
	fence := NewViewMetadataEpochFence()
	require.NoError(t, fence.Advance(context.Background(), 3))
	require.True(t, fence.MarkCatalogFenced(3))
	require.True(t, fence.MarkRefreshReady(3))
	require.True(t, fence.EnableRefresh(3))
	require.True(t, fence.RefreshEnabled())

	lease, acquired, err := fence.AcquireRefresh(context.Background())
	require.NoError(t, err)
	require.True(t, acquired)
	require.Equal(t, uint64(3), lease.Epoch())

	done := make(chan error, 1)
	go func() { done <- fence.Advance(context.Background(), 4) }()
	waitViewMetadataFenceAdvancing(t, fence)
	select {
	case err := <-done:
		t.Fatalf("epoch advanced before refresh lease drained: %v", err)
	default:
	}
	lease.Release()
	require.NoError(t, <-done)
	require.False(t, fence.RefreshEnabled())
	require.False(t, fence.EnableRefresh(3), "stale completion must not reopen a newer epoch")
}

func TestViewMetadataRefreshLeaseStaysSealedAfterCanceledAdvance(t *testing.T) {
	fence := NewViewMetadataEpochFence()
	require.NoError(t, fence.Advance(context.Background(), 1))
	require.True(t, fence.MarkCatalogFenced(1))
	require.True(t, fence.MarkRefreshReady(1))
	require.True(t, fence.EnableRefresh(1))
	blocker, err := fence.Acquire(context.Background())
	require.NoError(t, err)

	advanceCtx, cancelAdvance := context.WithCancel(context.Background())
	advanceDone := make(chan error, 1)
	go func() { advanceDone <- fence.Advance(advanceCtx, 2) }()
	waitViewMetadataFenceAdvancing(t, fence)
	cancelAdvance()
	require.ErrorIs(t, <-advanceDone, context.Canceled)
	require.False(t, fence.RefreshEnabled())

	type acquireResult struct {
		lease    *ViewMetadataEpochLease
		acquired bool
		err      error
	}
	leaseDone := make(chan acquireResult, 1)
	go func() {
		lease, acquired, acquireErr := fence.AcquireRefresh(context.Background())
		leaseDone <- acquireResult{lease: lease, acquired: acquired, err: acquireErr}
	}()
	select {
	case result := <-leaseDone:
		t.Fatalf("refresh reopened after canceled authoritative advance: %+v", result)
	default:
	}

	blocker.Release()
	require.NoError(t, fence.Advance(context.Background(), 2))
	require.True(t, fence.MarkCatalogFenced(2))
	result := <-leaseDone
	require.NoError(t, result.err)
	require.False(t, result.acquired)
	require.Equal(t, uint64(2), result.lease.Epoch())
	result.lease.Release()
}

func TestViewMetadataEpochFenceCloseTerminatesWaiters(t *testing.T) {
	fence := NewViewMetadataEpochFence()
	lease, err := fence.Acquire(context.Background())
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		done <- fence.Advance(context.Background(), 1)
	}()
	waitViewMetadataFenceAdvancing(t, fence)
	fence.Close()
	require.ErrorIs(t, <-done, context.Canceled)
	lease.Release()
	_, err = fence.Acquire(context.Background())
	require.ErrorIs(t, err, context.Canceled)
}
