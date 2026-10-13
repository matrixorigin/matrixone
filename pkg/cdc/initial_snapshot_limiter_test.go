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
	"sync"
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
	first, err := limiter.acquire(ctx)
	require.NoError(t, err)
	first.ObserveBatchBytes(100)
	second, err := limiter.acquire(ctx)
	require.NoError(t, err)
	second.ObserveBatchBytes(100)

	acquired := make(chan *snapshotPermit)
	go func() {
		if permit, acquireErr := limiter.acquire(ctx); acquireErr == nil {
			acquired <- permit
		}
	}()

	require.Eventually(t, func() bool {
		limiter.mu.Lock()
		defer limiter.mu.Unlock()
		return limiter.waiters == 1
	}, time.Second, time.Millisecond, "third batch did not reach the admission wait")

	available.Store(1200)
	var third *snapshotPermit
	select {
	case third = <-acquired:
	case <-ctx.Done():
		t.Fatal("limiter did not expand after memory headroom increased")
	}

	third.ObserveBatchBytes(400)
	limiter.mu.Lock()
	concurrency := limiter.concurrencyLocked(available.Load(), true)
	limiter.mu.Unlock()
	assert.Equal(t, 1, concurrency, "a wider observed batch must reduce admission")

	first.Release()
	second.Release()
	third.Release()
}

func TestInitialSnapshotLimiterSerializesUnobservedBatches(t *testing.T) {
	limiter := newInitialSnapshotLimiter(1, 8, 2, 256, func() (uint64, bool) {
		return 8192, true
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	first, err := limiter.acquire(ctx)
	require.NoError(t, err)

	acquired := make(chan *snapshotPermit, 7)
	for range 7 {
		go func() {
			permit, acquireErr := limiter.acquire(ctx)
			if acquireErr == nil {
				acquired <- permit
			}
		}()
	}
	require.Eventually(t, func() bool {
		limiter.mu.Lock()
		defer limiter.mu.Unlock()
		return limiter.waiters == 7
	}, time.Second, time.Millisecond)

	limiter.mu.Lock()
	assert.Equal(t, 1, limiter.inFlight)
	assert.Equal(t, 1, limiter.unobserved)
	limiter.mu.Unlock()
	assert.Empty(t, acquired, "all estimated slots were granted before a real batch size was known")

	first.ObserveBatchBytes(0)
	limiter.mu.Lock()
	assert.Equal(t, 1, limiter.unobserved)
	limiter.mu.Unlock()
	assert.Empty(t, acquired, "a zero-byte observation expanded admission without a real batch size")

	first.ObserveBatchBytes(1024)
	nextPermit := func() *snapshotPermit {
		select {
		case permit := <-acquired:
			return permit
		case <-ctx.Done():
			t.Fatal("waiting batch was not admitted")
			return nil
		}
	}
	second := nextPermit()
	limiter.mu.Lock()
	assert.Equal(t, 2, limiter.inFlight)
	assert.Equal(t, 1, limiter.unobserved)
	assert.Equal(t, 6, limiter.waiters)
	limiter.mu.Unlock()
	assert.Empty(t, acquired, "admission expanded again before the newly granted batch was observed")

	first.Release()
	second.ObserveBatchBytes(1024)
	second.Release()
	for range 6 {
		permit := nextPermit()
		permit.ObserveBatchBytes(1024)
		permit.Release()
	}
}

func TestInitialSnapshotPermitObserveAndReleaseRace(t *testing.T) {
	for range 100 {
		limiter := newInitialSnapshotLimiter(1, 1, 1, 100, func() (uint64, bool) {
			return 0, false
		})
		permit, err := limiter.acquire(context.Background())
		require.NoError(t, err)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			permit.ObserveBatchBytes(100)
		}()
		go func() {
			defer wg.Done()
			permit.Release()
		}()
		wg.Wait()

		limiter.mu.Lock()
		assert.Zero(t, limiter.inFlight)
		assert.Zero(t, limiter.unobserved)
		limiter.mu.Unlock()
	}
}

func TestInitialSnapshotLimiterFallbackAndCancellation(t *testing.T) {
	limiter := newInitialSnapshotLimiter(1, 4, 2, 100, func() (uint64, bool) {
		return 0, false
	})
	first, err := limiter.acquire(context.Background())
	require.NoError(t, err)
	first.ObserveBatchBytes(100)
	second, err := limiter.acquire(context.Background())
	require.NoError(t, err)
	second.ObserveBatchBytes(100)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = limiter.acquire(ctx)
	require.ErrorIs(t, err, context.Canceled)

	first.Release()
	second.Release()
}

func TestInitialSnapshotLimiterTryAcquireRespectsCapacityAndFIFO(t *testing.T) {
	limiter := newInitialSnapshotLimiter(1, 2, 2, 100, func() (uint64, bool) {
		return 800, true
	})
	first, err := limiter.acquire(context.Background())
	require.NoError(t, err)
	first.ObserveBatchBytes(100)
	second, ok := limiter.tryAcquire()
	require.True(t, ok)
	second.ObserveBatchBytes(100)
	_, ok = limiter.tryAcquire()
	require.False(t, ok, "try-acquire exceeded the adaptive capacity")
	first.Release()
	second.Release()

	fifo := newInitialSnapshotLimiter(1, 1, 1, 100, func() (uint64, bool) {
		return 400, true
	})
	held, err := fifo.acquire(context.Background())
	require.NoError(t, err)
	held.ObserveBatchBytes(100)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	waiting := make(chan *snapshotPermit, 1)
	go func() {
		permit, acquireErr := fifo.acquire(ctx)
		if acquireErr == nil {
			waiting <- permit
		}
	}()
	require.Eventually(t, func() bool {
		fifo.mu.Lock()
		defer fifo.mu.Unlock()
		return fifo.waiters == 1
	}, time.Second, time.Millisecond)
	_, ok = fifo.tryAcquire()
	require.False(t, ok, "try-acquire bypassed an existing FIFO waiter")
	held.Release()
	select {
	case permit := <-waiting:
		permit.Release()
	case <-ctx.Done():
		t.Fatal("FIFO waiter was not admitted")
	}
}

func TestInitialSnapshotLimiterReleaseWakesWaiter(t *testing.T) {
	limiter := newInitialSnapshotLimiter(1, 1, 1, 100, func() (uint64, bool) {
		return 0, false
	})
	first, err := limiter.acquire(context.Background())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	acquired := make(chan *snapshotPermit)
	go func() {
		if permit, acquireErr := limiter.acquire(ctx); acquireErr == nil {
			acquired <- permit
		}
	}()

	require.Eventually(t, func() bool {
		limiter.mu.Lock()
		defer limiter.mu.Unlock()
		return limiter.waiters == 1
	}, time.Second, time.Millisecond, "waiter did not reach the admission wait")
	first.Release()
	var second *snapshotPermit
	select {
	case second = <-acquired:
	case <-ctx.Done():
		t.Fatal("release did not wake a blocked waiter")
	}
	second.Release()
}

func TestInitialSnapshotLimiterRejectsOverRelease(t *testing.T) {
	limiter := newInitialSnapshotLimiter(1, 1, 1, 100, func() (uint64, bool) {
		return 0, false
	})
	require.Panics(t, func() { limiter.release(false) })
}

func TestInitialSnapshotLimiterPreservesWaiterOrder(t *testing.T) {
	limiter := newInitialSnapshotLimiter(1, 1, 1, 100, func() (uint64, bool) {
		return 0, false
	})
	first, err := limiter.acquire(context.Background())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	acquired := make(chan int, 3)
	releases := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	for i := range 3 {
		go func(id int) {
			permit, acquireErr := limiter.acquire(ctx)
			if acquireErr != nil {
				return
			}
			acquired <- id
			<-releases[id]
			permit.Release()
		}(i)
		require.Eventually(t, func() bool {
			limiter.mu.Lock()
			defer limiter.mu.Unlock()
			return limiter.waiters == i+1
		}, time.Second, time.Millisecond)
	}

	first.Release()
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
	initial, err := limiter.acquire(context.Background())
	require.NoError(t, err)

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstResult := make(chan error, 1)
	go func() {
		_, acquireErr := limiter.acquire(firstCtx)
		firstResult <- acquireErr
	}()
	require.Eventually(t, func() bool {
		limiter.mu.Lock()
		defer limiter.mu.Unlock()
		return limiter.waiters == 1
	}, time.Second, time.Millisecond)

	secondCtx, cancelSecond := context.WithTimeout(context.Background(), time.Second)
	defer cancelSecond()
	secondAcquired := make(chan *snapshotPermit)
	go func() {
		if permit, acquireErr := limiter.acquire(secondCtx); acquireErr == nil {
			secondAcquired <- permit
		}
	}()
	require.Eventually(t, func() bool {
		limiter.mu.Lock()
		defer limiter.mu.Unlock()
		return limiter.waiters == 2
	}, time.Second, time.Millisecond)

	cancelFirst()
	require.ErrorIs(t, <-firstResult, context.Canceled)
	initial.Release()
	var second *snapshotPermit
	select {
	case second = <-secondAcquired:
	case <-secondCtx.Done():
		t.Fatal("canceled FIFO head prevented the next waiter from acquiring")
	}
	second.Release()
}

func TestInitialSnapshotLimiterCancellationDuringMemoryDiscovery(t *testing.T) {
	entered := make(chan struct{})
	unblock := make(chan struct{})
	limiter := newInitialSnapshotLimiter(1, 1, 1, 100, func() (uint64, bool) {
		close(entered)
		<-unblock
		return 1024, true
	})

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		permit, err := limiter.acquire(ctx)
		if permit != nil {
			permit.Release()
		}
		result <- err
	}()

	<-entered
	cancel()
	close(unblock)
	require.ErrorIs(t, <-result, context.Canceled)

	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	assert.Zero(t, limiter.inFlight)
	assert.Zero(t, limiter.unobserved)
	assert.Zero(t, limiter.waiters)
}
