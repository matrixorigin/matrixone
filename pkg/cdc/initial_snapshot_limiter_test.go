// Copyright 2026 Matrix Origin
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

package cdc

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInitialSnapshotLimiterAdaptsToMemoryAndBatchSize(t *testing.T) {
	var available atomic.Uint64
	available.Store(800)
	limiter := newInitialSnapshotLimiter(1, 4, 2, 100, func() (uint64, bool) {
		return available.Load(), true
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, limiter.Acquire(ctx))
	require.NoError(t, limiter.Acquire(ctx))

	acquired := make(chan struct{})
	go func() {
		if limiter.Acquire(ctx) == nil {
			close(acquired)
		}
	}()

	require.Eventually(t, func() bool {
		limiter.mu.Lock()
		defer limiter.mu.Unlock()
		return limiter.waiters == 1
	}, time.Second, time.Millisecond, "third batch did not reach the admission wait")

	available.Store(1200)
	select {
	case <-acquired:
	case <-ctx.Done():
		t.Fatal("limiter did not expand after memory headroom increased")
	}

	limiter.ObserveBatchBytes(400)
	limiter.mu.Lock()
	concurrency := limiter.concurrencyLocked(available.Load(), true)
	limiter.mu.Unlock()
	assert.Equal(t, 1, concurrency, "a wider observed batch must reduce admission")

	limiter.Release()
	limiter.Release()
	limiter.Release()
}

func TestInitialSnapshotLimiterFallbackAndCancellation(t *testing.T) {
	limiter := newInitialSnapshotLimiter(1, 4, 2, 100, func() (uint64, bool) {
		return 0, false
	})
	require.NoError(t, limiter.Acquire(context.Background()))
	require.NoError(t, limiter.Acquire(context.Background()))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := limiter.Acquire(ctx)
	require.ErrorIs(t, err, context.Canceled)

	limiter.Release()
	limiter.Release()
}

func TestInitialSnapshotLimiterReleaseWakesWaiter(t *testing.T) {
	limiter := newInitialSnapshotLimiter(1, 1, 1, 100, func() (uint64, bool) {
		return 0, false
	})
	require.NoError(t, limiter.Acquire(context.Background()))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	acquired := make(chan struct{})
	go func() {
		if limiter.Acquire(ctx) == nil {
			close(acquired)
		}
	}()

	require.Eventually(t, func() bool {
		limiter.mu.Lock()
		defer limiter.mu.Unlock()
		return limiter.waiters == 1
	}, time.Second, time.Millisecond, "waiter did not reach the admission wait")
	limiter.Release()
	select {
	case <-acquired:
	case <-ctx.Done():
		t.Fatal("release did not wake a blocked waiter")
	}
	limiter.Release()
}

func TestInitialSnapshotLimiterRejectsOverRelease(t *testing.T) {
	limiter := newInitialSnapshotLimiter(1, 1, 1, 100, func() (uint64, bool) {
		return 0, false
	})
	require.Panics(t, limiter.Release)
}

func TestInitialSnapshotLimiterPreservesWaiterOrder(t *testing.T) {
	limiter := newInitialSnapshotLimiter(1, 1, 1, 100, func() (uint64, bool) {
		return 0, false
	})
	require.NoError(t, limiter.Acquire(context.Background()))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	acquired := make(chan int, 3)
	releases := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	for i := range 3 {
		go func(id int) {
			if limiter.Acquire(ctx) != nil {
				return
			}
			acquired <- id
			<-releases[id]
			limiter.Release()
		}(i)
		require.Eventually(t, func() bool {
			limiter.mu.Lock()
			defer limiter.mu.Unlock()
			return limiter.waiters == i+1
		}, time.Second, time.Millisecond)
	}

	limiter.Release()
	for want := range 3 {
		select {
		case got := <-acquired:
			require.Equal(t, want, got)
			close(releases[got])
		case <-ctx.Done():
			t.Fatalf("waiter %d was not admitted", want)
		}
	}
}

func TestInitialSnapshotLimiterCancellationAdvancesQueue(t *testing.T) {
	limiter := newInitialSnapshotLimiter(1, 1, 1, 100, func() (uint64, bool) {
		return 0, false
	})
	require.NoError(t, limiter.Acquire(context.Background()))

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstResult := make(chan error, 1)
	go func() { firstResult <- limiter.Acquire(firstCtx) }()
	require.Eventually(t, func() bool {
		limiter.mu.Lock()
		defer limiter.mu.Unlock()
		return limiter.waiters == 1
	}, time.Second, time.Millisecond)

	secondCtx, cancelSecond := context.WithTimeout(context.Background(), time.Second)
	defer cancelSecond()
	secondAcquired := make(chan struct{})
	go func() {
		if limiter.Acquire(secondCtx) == nil {
			close(secondAcquired)
		}
	}()
	require.Eventually(t, func() bool {
		limiter.mu.Lock()
		defer limiter.mu.Unlock()
		return limiter.waiters == 2
	}, time.Second, time.Millisecond)

	cancelFirst()
	require.ErrorIs(t, <-firstResult, context.Canceled)
	limiter.Release()
	select {
	case <-secondAcquired:
	case <-secondCtx.Done():
		t.Fatal("canceled FIFO head prevented the next waiter from acquiring")
	}
	limiter.Release()
}
