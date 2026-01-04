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

package morpc

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetBackendWithSlowConnection tests that getBackend doesn't block
// when backend creation is slow (simulating network delay or crashed node)
func TestGetBackendWithSlowConnection(t *testing.T) {
	// Create a factory that simulates slow connection
	slowFactory := &testBackendFactoryWithDelay{
		delay: 100 * time.Millisecond,
	}

	rc, err := NewClient(
		"test-client",
		slowFactory,
		WithClientMaxBackendPerHost(2),
		WithClientEnableAutoCreateBackend(),
	)
	require.NoError(t, err)
	defer rc.Close()

	c := rc.(*client)

	// First call should trigger async creation and return error quickly
	start := time.Now()
	b, err := c.getBackend("slow-backend", false)
	elapsed := time.Since(start)

	// Should return error immediately without blocking
	assert.Error(t, err)
	assert.Nil(t, b)
	assert.Less(t, elapsed, 50*time.Millisecond, "getBackend should not block")

	// Wait for async creation to complete
	time.Sleep(150 * time.Millisecond)

	// Second call should succeed with the created backend
	b, err = c.getBackend("slow-backend", false)
	assert.NoError(t, err)
	assert.NotNil(t, b)
}

// TestGetBackendConcurrentWithSlowConnection tests that multiple concurrent
// getBackend calls don't serialize when connection is slow
func TestGetBackendConcurrentWithSlowConnection(t *testing.T) {
	slowFactory := &testBackendFactoryWithDelay{
		delay: 100 * time.Millisecond,
	}

	rc, err := NewClient(
		"test-client",
		slowFactory,
		WithClientMaxBackendPerHost(5),
		WithClientEnableAutoCreateBackend(),
	)
	require.NoError(t, err)
	defer rc.Close()

	c := rc.(*client)

	// Launch 10 concurrent getBackend calls
	const numGoroutines = 10
	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b, err := c.getBackend("slow-backend", false)
			if b != nil {
				b.Close()
			}
			// Don't check error - some may succeed, some may fail
			_ = err
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	// All goroutines should complete quickly without serializing
	// If they serialized, it would take 10 * 100ms = 1000ms
	// With async creation, should complete in ~100-200ms
	assert.Less(t, elapsed, 500*time.Millisecond,
		"Concurrent calls should not serialize (elapsed: %v)", elapsed)
}

// TestGetBackendWithCircuitBreaker tests that circuit breaker prevents
// repeated attempts to bad backends
func TestGetBackendWithCircuitBreaker(t *testing.T) {
	factory := &testBackendFactoryWithDelay{
		delay:      10 * time.Millisecond,
		shouldFail: true,
	}

	rc, err := NewClient(
		"test-client",
		factory,
		WithClientMaxBackendPerHost(2),
		WithClientEnableAutoCreateBackend(),
	)
	require.NoError(t, err)
	defer rc.Close()

	c := rc.(*client)

	// Trigger circuit breaker by recording failures
	for i := 0; i < 10; i++ {
		c.circuitBreakers.RecordFailure("bad-backend")
	}

	// getBackend should fail fast
	start := time.Now()
	b, err := c.getBackend("bad-backend", false)
	elapsed := time.Since(start)

	assert.Error(t, err)
	assert.Nil(t, b)
	// Should fail fast (either circuit breaker or no backend available)
	assert.Less(t, elapsed, 20*time.Millisecond,
		"Should fail fast with circuit breaker or no backend")
}

// TestGetBackendWithClientClosed tests that getBackend handles
// client closure gracefully
func TestGetBackendWithClientClosed(t *testing.T) {
	rc, err := NewClient(
		"test-client",
		newTestBackendFactory(),
		WithClientMaxBackendPerHost(2),
	)
	require.NoError(t, err)

	c := rc.(*client)

	// Close client
	err = c.Close()
	require.NoError(t, err)

	// getBackend should return error immediately
	b, err := c.getBackend("any-backend", false)
	assert.Error(t, err)
	assert.Nil(t, b)
}

// TestGetBackendAsyncCreationEventualSuccess tests that async creation
// eventually succeeds and subsequent calls work
func TestGetBackendAsyncCreationEventualSuccess(t *testing.T) {
	factory := &testBackendFactoryWithDelay{
		delay: 50 * time.Millisecond,
	}

	rc, err := NewClient(
		"test-client",
		factory,
		WithClientMaxBackendPerHost(3),
		WithClientEnableAutoCreateBackend(),
	)
	require.NoError(t, err)
	defer rc.Close()

	c := rc.(*client)

	// First call triggers async creation
	b, err := c.getBackend("test-backend", false)
	assert.Error(t, err)
	assert.Nil(t, b)

	// Poll until backend is created (with timeout)
	var createdBackend Backend
	for i := 0; i < 20; i++ {
		time.Sleep(10 * time.Millisecond)
		b, err := c.getBackend("test-backend", false)
		if err == nil && b != nil {
			createdBackend = b
			break
		}
	}

	require.NotNil(t, createdBackend, "Backend should be created within 200ms")

	// Subsequent calls should succeed immediately
	for i := 0; i < 5; i++ {
		b, err := c.getBackend("test-backend", false)
		assert.NoError(t, err)
		assert.NotNil(t, b)
	}
}

// TestGetBackendRespectMaxBackends tests that async creation respects
// maxBackendsPerHost limit
// testBackendFactoryForCrash simulates a crashed node that always fails to connect
type testBackendFactoryForCrash struct {
	sync.RWMutex
}

func (bf *testBackendFactoryForCrash) Create(backend string, opts ...BackendOption) (Backend, error) {
	// Simulate network timeout when connecting to crashed node
	time.Sleep(100 * time.Millisecond)
	return nil, moerr.NewInternalErrorNoCtx("connection refused: node crashed")
}

// TestCrashedNodeRecovery tests the core problem scenario:
// Multiple goroutines requesting a crashed CN node should not serialize
// and should recover quickly via circuit breaker
func TestCrashedNodeRecovery(t *testing.T) {
	// Create a factory that always fails (simulating crashed node)
	crashedFactory := &testBackendFactoryForCrash{}

	rc, err := NewClient(
		"test-client",
		crashedFactory,
		WithClientMaxBackendPerHost(5),
		WithClientEnableAutoCreateBackend(),
	)
	require.NoError(t, err)
	defer rc.Close()

	c := rc.(*client)

	// Simulate 27 goroutines (like in production) requesting crashed node
	const numGoroutines = 27
	var wg sync.WaitGroup
	var totalBlockTime atomic.Int64
	var successCount atomic.Int32
	var noBackendCount atomic.Int32

	start := time.Now()

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			goroutineStart := time.Now()

			// Try to get backend (should fail quickly)
			b, err := c.getBackend("crashed-node", false)

			elapsed := time.Since(goroutineStart)
			totalBlockTime.Add(int64(elapsed))

			if err == nil {
				successCount.Add(1)
				if b != nil {
					b.Close()
				}
			} else if moerr.IsMoErrCode(err, moerr.ErrNoAvailableBackend) || errors.Is(err, ErrBackendCreating) {
				noBackendCount.Add(1)
			}
		}(i)
	}

	wg.Wait()
	totalElapsed := time.Since(start)

	// Verify: Total time should be much less than serial execution (27 * 100ms = 2.7s)
	// With async creation, should complete in < 1 second
	assert.Less(t, totalElapsed, 1500*time.Millisecond,
		"Total time should be < 1.5s (not serialized)")

	// Verify: Average blocking time per goroutine should be small
	avgBlockTime := time.Duration(totalBlockTime.Load() / int64(numGoroutines))
	assert.Less(t, avgBlockTime, 500*time.Millisecond,
		"Average blocking time should be < 500ms per goroutine")

	// Verify: Most goroutines should see ErrNoAvailableBackend (async creation)
	// This proves they didn't block waiting for backend creation
	assert.Greater(t, int(noBackendCount.Load()), numGoroutines/2,
		"Most requests should return immediately with ErrNoAvailableBackend/ErrBackendCreating")

	// Verify: No successful connections (node is crashed)
	assert.Equal(t, int32(0), successCount.Load(),
		"Should have no successful connections to crashed node")

	t.Logf("Recovery test completed:")
	t.Logf("  Total time: %v (vs 2.7s if serialized)", totalElapsed)
	t.Logf("  Avg block time: %v per goroutine", avgBlockTime)
	t.Logf("  NoBackend count: %d/%d (proves async behavior)", noBackendCount.Load(), numGoroutines)
}

// testBackendFactoryForRetry creates backends after a delay
type testBackendFactoryForRetry struct {
	sync.RWMutex
	creationCount atomic.Int32
	delay         time.Duration
}

func (bf *testBackendFactoryForRetry) Create(backend string, opts ...BackendOption) (Backend, error) {
	bf.creationCount.Add(1)

	// Simulate backend creation delay
	time.Sleep(bf.delay)

	// Create a simple test backend
	return &testBackend{
		id:         int(bf.creationCount.Load()),
		activeTime: time.Now(),
	}, nil
}

// TestSendRetryMechanism tests that Send() correctly retries on ErrNoAvailableBackend
func TestSendRetryMechanism(t *testing.T) {
	// Factory that succeeds after a delay (simulating async creation)
	delayedFactory := &testBackendFactoryForRetry{
		delay: 200 * time.Millisecond,
	}

	rc, err := NewClient(
		"test-client",
		delayedFactory,
		WithClientMaxBackendPerHost(2),
		WithClientEnableAutoCreateBackend(),
	)
	require.NoError(t, err)
	defer rc.Close()

	c := rc.(*client)

	// First Send should trigger async creation and retry
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &testMessage{id: 1}
	future, err := c.Send(ctx, "backend-1", req)

	elapsed := time.Since(start)

	// Should succeed after retry
	require.NoError(t, err, "Send should succeed after retry")
	require.NotNil(t, future, "Future should not be nil")

	// Should take at least one retry cycle (200ms creation + backoff)
	assert.GreaterOrEqual(t, elapsed, 200*time.Millisecond,
		"Should wait for async creation")

	// Should not take too long (max 2-3 retries)
	assert.Less(t, elapsed, 2*time.Second,
		"Should succeed within reasonable time")

	// Verify backend was created exactly once (not multiple times)
	assert.Equal(t, int32(1), delayedFactory.creationCount.Load(),
		"Backend should be created exactly once")

	// Second Send should use existing backend (no retry)
	start = time.Now()
	req2 := &testMessage{id: 2}
	future2, err := c.Send(ctx, "backend-1", req2)
	elapsed2 := time.Since(start)

	require.NoError(t, err)
	require.NotNil(t, future2)

	// Should be very fast (no creation, no retry)
	assert.Less(t, elapsed2, 50*time.Millisecond,
		"Second Send should be fast (backend already exists)")

	// Verify no additional backend creation
	assert.Equal(t, int32(1), delayedFactory.creationCount.Load(),
		"Should still have only one backend")

	t.Logf("First Send: %v (with retry)", elapsed)
	t.Logf("Second Send: %v (no retry)", elapsed2)
	t.Logf("Backend created: %d times", delayedFactory.creationCount.Load())
}

func TestGetBackendRespectMaxBackends(t *testing.T) {
	factory := &testBackendFactoryWithDelay{
		delay: 20 * time.Millisecond,
	}

	const maxBackends = 2
	rc, err := NewClient(
		"test-client",
		factory,
		WithClientMaxBackendPerHost(maxBackends),
		WithClientEnableAutoCreateBackend(),
	)
	require.NoError(t, err)
	defer rc.Close()

	c := rc.(*client)

	// Trigger multiple async creations
	for i := 0; i < 10; i++ {
		c.getBackend("test-backend", false)
	}

	// Wait for creations to complete
	time.Sleep(100 * time.Millisecond)

	// Check that we don't exceed max backends
	c.mu.Lock()
	backendCount := len(c.mu.backends["test-backend"])
	c.mu.Unlock()

	assert.LessOrEqual(t, backendCount, maxBackends,
		"Should not exceed maxBackendsPerHost")
}

// Helper types for testing

type testBackendFactoryWithDelay struct {
	delay       time.Duration
	shouldFail  bool
	createCount atomic.Int32
}

func (f *testBackendFactoryWithDelay) Create(address string, opts ...BackendOption) (Backend, error) {
	f.createCount.Add(1)

	if f.shouldFail {
		return nil, moerr.NewBackendCannotConnectNoCtx()
	}

	// Simulate slow connection
	if f.delay > 0 {
		time.Sleep(f.delay)
	}

	return &testBackend{
		id:         int(f.createCount.Load()),
		activeTime: time.Now(),
	}, nil
}

// Benchmark to verify no performance regression

func BenchmarkGetBackendWithAvailableBackend(b *testing.B) {
	rc, err := NewClient(
		"bench-client",
		newTestBackendFactory(),
		WithClientMaxBackendPerHost(10),
	)
	require.NoError(b, err)
	defer rc.Close()

	c := rc.(*client)

	// Pre-create backends
	for i := 0; i < 5; i++ {
		c.mu.Lock()
		backend := &testBackend{id: i, activeTime: time.Now()}
		c.mu.backends["bench-backend"] = append(c.mu.backends["bench-backend"], backend)
		c.mu.Unlock()
	}
	c.mu.Lock()
	c.mu.ops["bench-backend"] = &op{}
	c.mu.Unlock()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			backend, err := c.getBackend("bench-backend", false)
			if err != nil {
				b.Fatal(err)
			}
			if backend == nil {
				b.Fatal("expected backend")
			}
		}
	})
}

func BenchmarkGetBackendWithCircuitBreaker(b *testing.B) {
	rc, err := NewClient(
		"bench-client",
		newTestBackendFactory(),
		WithClientMaxBackendPerHost(10),
	)
	require.NoError(b, err)
	defer rc.Close()

	c := rc.(*client)

	// Trip circuit breaker
	for i := 0; i < 10; i++ {
		c.circuitBreakers.RecordFailure("bad-backend")
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := c.getBackend("bad-backend", false)
			if err != ErrCircuitOpen {
				b.Fatalf("expected ErrCircuitOpen, got %v", err)
			}
		}
	})
}
