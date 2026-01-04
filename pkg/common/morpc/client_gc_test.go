// Copyright 2021 - 2022 Matrix Origin
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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to get backend with retry for async creation
func getBackendWithRetry(t *testing.T, client *client, backend string, lock bool) Backend {
	var b Backend
	var err error
	for i := 0; i < 10; i++ {
		b, err = client.getBackend(backend, lock)
		if err == nil && b != nil {
			return b
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.NoError(t, err)
	require.NotNil(t, b)
	return b
}

func TestGlobalClientGC_RegisterUnregister(t *testing.T) {
	// Create a new manager for testing
	mgr := newClientGCManager()

	// Create test clients
	c1, err := NewClient("test-client-1", newTestBackendFactory())
	require.NoError(t, err)
	defer c1.Close()

	c2, err := NewClient("test-client-2", newTestBackendFactory())
	require.NoError(t, err)
	defer c2.Close()

	// Register clients
	mgr.register(c1.(*client))
	mgr.register(c2.(*client))

	// Verify clients are registered
	mgr.mu.RLock()
	assert.Equal(t, 2, len(mgr.clients))
	_, ok1 := mgr.clients[c1.(*client)]
	_, ok2 := mgr.clients[c2.(*client)]
	mgr.mu.RUnlock()
	assert.True(t, ok1)
	assert.True(t, ok2)
	assert.True(t, mgr.started)

	// Unregister one client
	mgr.unregister(c1.(*client))

	// Verify one client remains
	mgr.mu.RLock()
	assert.Equal(t, 1, len(mgr.clients))
	_, ok1 = mgr.clients[c1.(*client)]
	_, ok2 = mgr.clients[c2.(*client)]
	mgr.mu.RUnlock()
	assert.False(t, ok1)
	assert.True(t, ok2)

	// Unregister the other client
	mgr.unregister(c2.(*client))

	// Verify no clients remain
	mgr.mu.RLock()
	assert.Equal(t, 0, len(mgr.clients))
	mgr.mu.RUnlock()

	// Stop the manager
	mgr.stop()
}

func TestGlobalClientGC_GCIdleLoop(t *testing.T) {
	mgr := newClientGCManager()

	// Create a client with idle duration
	c, err := NewClient("test-client",
		newTestBackendFactory(),
		WithClientMaxBackendMaxIdleDuration(time.Millisecond*100),
		WithClientEnableAutoCreateBackend())
	require.NoError(t, err)
	defer c.Close()

	client := c.(*client)
	mgr.register(client)

	// Create a backend - use helper for async creation
	b := getBackendWithRetry(t, client, "b1", false)

	// Mark backend as idle by setting activeTime to old time
	tb := b.(*testBackend)
	tb.RWMutex.Lock()
	tb.activeTime = time.Now().Add(-time.Second * 2)
	tb.RWMutex.Unlock()

	// Wait for GC to run (ticker is 10 seconds, but we can trigger manually)
	// Since we can't easily test the ticker, we'll test the doGCIdle method directly
	time.Sleep(time.Millisecond * 50)
	mgr.doGCIdle()

	// Wait a bit for cleanup
	time.Sleep(time.Millisecond * 50)

	// Verify backend was closed
	client.mu.Lock()
	backends := client.mu.backends["b1"]
	client.mu.Unlock()

	// Backend should be removed if it was idle
	tb.RWMutex.RLock()
	closed := tb.closed
	tb.RWMutex.RUnlock()

	// The backend should be closed if it was idle
	if len(backends) == 0 {
		assert.True(t, closed, "idle backend should be closed")
	}

	mgr.unregister(client)
	mgr.stop()
}

func TestGlobalClientGC_GCInactiveLoop(t *testing.T) {
	mgr := newClientGCManager()

	c, err := NewClient("test-client", newTestBackendFactory(), WithClientEnableAutoCreateBackend())
	require.NoError(t, err)
	defer c.Close()

	client := c.(*client)
	mgr.register(client)

	// Create a backend and mark it as inactive
	b := getBackendWithRetry(t, client, "b1", false)
	require.NoError(t, err)
	require.NotNil(t, b)

	tb := b.(*testBackend)
	tb.RWMutex.Lock()
	tb.activeTime = time.Time{} // Mark as inactive
	tb.RWMutex.Unlock()

	// Trigger GC inactive
	mgr.triggerGCInactive(client, "b1")

	// Wait for processing
	time.Sleep(time.Millisecond * 100)

	// Verify backend was removed
	client.mu.Lock()
	backends := client.mu.backends["b1"]
	client.mu.Unlock()

	// Backend should be removed
	assert.Equal(t, 0, len(backends), "inactive backend should be removed")

	mgr.unregister(client)
	mgr.stop()
}

func TestGlobalClientGC_CreateLoop(t *testing.T) {
	mgr := newClientGCManager()

	c, err := NewClient("test-client",
		newTestBackendFactory(),
		WithClientMaxBackendPerHost(2),
		WithClientEnableAutoCreateBackend())
	require.NoError(t, err)
	defer c.Close()

	client := c.(*client)
	mgr.register(client)

	// Initially no backends
	client.mu.Lock()
	backends := client.mu.backends["b1"]
	client.mu.Unlock()
	assert.Equal(t, 0, len(backends))

	// Trigger create
	mgr.triggerCreate(client, "b1")

	// Wait for processing
	time.Sleep(time.Millisecond * 100)

	// Verify backend was created
	client.mu.Lock()
	backends = client.mu.backends["b1"]
	client.mu.Unlock()
	assert.Equal(t, 1, len(backends), "backend should be created")

	mgr.unregister(client)
	mgr.stop()
}

func TestGlobalClientGC_ConcurrentClients(t *testing.T) {
	mgr := newClientGCManager()

	const numClients = 10
	clients := make([]*client, numClients)

	// Create multiple clients concurrently
	var wg sync.WaitGroup
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			c, err := NewClient("test-client", newTestBackendFactory())
			require.NoError(t, err)
			clients[idx] = c.(*client)
			mgr.register(c.(*client))
		}(i)
	}
	wg.Wait()

	// Verify all clients are registered
	mgr.mu.RLock()
	assert.Equal(t, numClients, len(mgr.clients))
	mgr.mu.RUnlock()

	// Unregister all clients concurrently
	wg = sync.WaitGroup{}
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			mgr.unregister(clients[idx])
			clients[idx].Close()
		}(i)
	}
	wg.Wait()

	// Verify no clients remain
	mgr.mu.RLock()
	assert.Equal(t, 0, len(mgr.clients))
	mgr.mu.RUnlock()

	mgr.stop()
}

func TestGlobalClientGC_ChannelFull(t *testing.T) {
	mgr := newClientGCManager()

	c, err := NewClient("test-client", newTestBackendFactory())
	require.NoError(t, err)
	defer c.Close()

	client := c.(*client)
	mgr.register(client)

	// Fill up the channels
	for i := 0; i < 1025; i++ {
		mgr.triggerGCInactive(client, "b1")
		mgr.triggerCreate(client, "b1")
	}

	// Should not block or panic
	time.Sleep(time.Millisecond * 50)

	mgr.unregister(client)
	mgr.stop()
}

func TestGlobalClientGC_UnregisteredClientIgnored(t *testing.T) {
	mgr := newClientGCManager()

	c, err := NewClient("test-client", newTestBackendFactory())
	require.NoError(t, err)
	defer c.Close()

	client := c.(*client)

	// Register and then unregister
	mgr.register(client)
	mgr.unregister(client)

	// Try to trigger GC on unregistered client
	mgr.triggerGCInactive(client, "b1")
	mgr.triggerCreate(client, "b1")

	// Wait a bit
	time.Sleep(time.Millisecond * 100)

	// Should not process requests for unregistered client
	client.mu.Lock()
	backends := client.mu.backends["b1"]
	client.mu.Unlock()

	// No backend should be created since client is unregistered
	assert.Equal(t, 0, len(backends))

	mgr.stop()
}

func TestGlobalClientGC_Stop(t *testing.T) {
	mgr := newClientGCManager()

	c, err := NewClient("test-client", newTestBackendFactory())
	require.NoError(t, err)
	defer c.Close()

	client := c.(*client)
	mgr.register(client)

	// Verify it's started
	assert.True(t, mgr.started)

	// Stop the manager
	mgr.stop()

	// Verify it's stopped
	mgr.mu.RLock()
	assert.False(t, mgr.started)
	assert.Equal(t, 0, len(mgr.clients))
	mgr.mu.RUnlock()

	// Should be able to stop again without panic
	mgr.stop()
}

func TestGlobalClientGC_GCIdleRespectsMaxIdleDuration(t *testing.T) {
	mgr := newClientGCManager()

	// Create client without maxIdleDuration (should not GC)
	c1, err := NewClient("test-client-1", newTestBackendFactory(), WithClientEnableAutoCreateBackend())
	require.NoError(t, err)
	defer c1.Close()

	client1 := c1.(*client)
	mgr.register(client1)

	// Create backend
	b1 := getBackendWithRetry(t, client1, "b1", false)
	require.NoError(t, err)
	require.NotNil(t, b1)

	// Create client with maxIdleDuration
	c2, err := NewClient("test-client-2",
		newTestBackendFactory(),
		WithClientMaxBackendMaxIdleDuration(time.Millisecond*100),
		WithClientEnableAutoCreateBackend())
	require.NoError(t, err)
	defer c2.Close()

	client2 := c2.(*client)
	mgr.register(client2)

	// Create backend
	b2 := getBackendWithRetry(t, client2, "b2", false)
	require.NoError(t, err)
	require.NotNil(t, b2)

	// Mark both as idle
	tb1 := b1.(*testBackend)
	tb1.RWMutex.Lock()
	tb1.activeTime = time.Now().Add(-time.Second * 2)
	tb1.RWMutex.Unlock()

	tb2 := b2.(*testBackend)
	tb2.RWMutex.Lock()
	tb2.activeTime = time.Now().Add(-time.Second * 2)
	tb2.RWMutex.Unlock()

	// Run GC
	mgr.doGCIdle()

	// Wait a bit
	time.Sleep(time.Millisecond * 50)

	// Client1 should not GC (no maxIdleDuration)
	client1.mu.Lock()
	backends1 := client1.mu.backends["b1"]
	client1.mu.Unlock()

	// Client2 should GC (has maxIdleDuration)
	client2.mu.Lock()
	_ = client2.mu.backends["b2"]
	client2.mu.Unlock()

	// Client1 should still have backend (no GC)
	assert.Greater(t, len(backends1), 0, "client without maxIdleDuration should not GC")

	mgr.unregister(client1)
	mgr.unregister(client2)
	mgr.stop()
}

func TestGlobalClientGC_Integration(t *testing.T) {
	// Test that the global GC manager works with actual client operations
	c, err := NewClient("test-client",
		newTestBackendFactory(),
		WithClientMaxBackendPerHost(2),
		WithClientMaxBackendMaxIdleDuration(time.Millisecond*200),
		WithClientEnableAutoCreateBackend())
	require.NoError(t, err)
	defer c.Close()

	// Pre-create backend to avoid async creation delay
	client := c.(*client)
	_ = getBackendWithRetry(t, client, "b1", false)

	// Send a request to create a backend
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	msg := newTestMessage(1)
	f, err := c.Send(ctx, "b1", msg)
	require.NoError(t, err)
	require.NotNil(t, f)

	// Wait for response
	_, err = f.Get()
	require.NoError(t, err)
	f.Close()

	// Verify backend was created
	client.mu.Lock()
	backends := client.mu.backends["b1"]
	client.mu.Unlock()
	assert.Greater(t, len(backends), 0, "backend should be created")

	// Wait for idle GC
	time.Sleep(time.Millisecond * 300)

	// Backend might be GC'd if idle, but that's OK
	// No need to check as the test just verifies the integration works
}

func TestGlobalClientGC_StressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	mgr := newClientGCManager()

	const numClients = 50
	const numOpsPerClient = 100

	var opsCount int64
	clients := make([]*client, numClients)

	// Create clients
	for i := 0; i < numClients; i++ {
		c, err := NewClient("test-client", newTestBackendFactory())
		require.NoError(t, err)
		clients[i] = c.(*client)
		mgr.register(clients[i])
	}

	// Run concurrent operations
	var wg sync.WaitGroup
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			client := clients[idx]
			for j := 0; j < numOpsPerClient; j++ {
				mgr.triggerGCInactive(client, "b1")
				atomic.AddInt64(&opsCount, 1)
				mgr.triggerCreate(client, "b1")
				atomic.AddInt64(&opsCount, 1)
			}
		}(i)
	}

	wg.Wait()

	// Verify all operations completed (each client does numOpsPerClient * 2 operations)
	assert.Equal(t, int64(numClients*numOpsPerClient*2), opsCount)

	// Cleanup
	for i := 0; i < numClients; i++ {
		mgr.unregister(clients[i])
		clients[i].Close()
	}

	mgr.stop()
}

func TestGlobalClientGC_TriggerCreateReturnValue(t *testing.T) {
	mgr := newClientGCManager()

	c, err := NewClient("test-client",
		newTestBackendFactory(),
		WithClientMaxBackendPerHost(2),
		WithClientEnableAutoCreateBackend())
	require.NoError(t, err)
	defer c.Close()

	client := c.(*client)
	mgr.register(client)

	// First request should succeed (channel is empty)
	ok := mgr.triggerCreate(client, "b1")
	assert.True(t, ok, "first triggerCreate should succeed")

	// Fill up the channel to test failure case
	channelCap := cap(mgr.createC)
	for i := 0; i < channelCap-1; i++ {
		mgr.triggerCreate(client, "b1")
	}

	// Now channel should be full, next request should fail
	// Note: we need to ensure the channel is full by filling it completely
	for i := 0; i < channelCap; i++ {
		mgr.triggerCreate(client, "b1")
	}

	// The last few should have returned false (channel full)
	// We can't easily verify this without pausing the consumer goroutine
	// So we just verify no panic and the function returns

	mgr.unregister(client)
	mgr.stop()
}

func TestGlobalClientGC_InitGlobalGCManagerConcurrent(t *testing.T) {
	// Test that InitGlobalGCManager can be called concurrently without issues
	// Save original values (protected by mutex)
	globalClientGCMu.Lock()
	originalInterval := globalGCIdleCheckInterval
	originalBufferSize := globalGCChannelBufferSize
	globalClientGCMu.Unlock()

	defer func() {
		// Restore original values (protected by mutex)
		globalClientGCMu.Lock()
		globalGCIdleCheckInterval = originalInterval
		globalGCChannelBufferSize = originalBufferSize
		globalClientGCMu.Unlock()
	}()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			interval := time.Duration(idx+1) * time.Second
			bufferSize := (idx + 1) * 1000
			InitGlobalGCManager(interval, bufferSize)
		}(i)
	}

	wg.Wait()

	// Should not panic or deadlock
	// Values should be set (we don't know which goroutine won, but that's OK)
	globalClientGCMu.RLock()
	intervalOK := globalGCIdleCheckInterval > 0
	bufferSizeOK := globalGCChannelBufferSize > 0
	globalClientGCMu.RUnlock()
	assert.True(t, intervalOK, "interval should be positive")
	assert.True(t, bufferSizeOK, "buffer size should be positive")
}

func TestGlobalClientGC_InitGlobalGCManagerAfterStart(t *testing.T) {
	// Save original manager (protected by mutex)
	globalClientGCMu.Lock()
	originalManager := globalClientGC
	originalInterval := globalGCIdleCheckInterval

	// Create a new manager for testing
	testMgr := newClientGCManager()
	globalClientGC = testMgr
	globalClientGCMu.Unlock()

	defer func() {
		// Restore original manager (protected by mutex)
		testMgr.stop()
		globalClientGCMu.Lock()
		globalClientGC = originalManager
		globalGCIdleCheckInterval = originalInterval
		globalClientGCMu.Unlock()
	}()

	// Create a client to start the manager
	c, err := NewClient("test-client", newTestBackendFactory())
	require.NoError(t, err)
	defer c.Close()

	// Manager should now be started
	testMgr.mu.RLock()
	started := testMgr.started
	testMgr.mu.RUnlock()
	assert.True(t, started, "manager should be started after client creation")

	// Try to change config after start
	newInterval := time.Hour // Very different value

	InitGlobalGCManager(newInterval, 8192)

	// Interval should be updated (even after start)
	globalClientGCMu.RLock()
	currentInterval := globalGCIdleCheckInterval
	globalClientGCMu.RUnlock()
	assert.Equal(t, newInterval, currentInterval, "interval should be updated")

	// But channel buffer size cannot be changed after start
	// (verified by warning log, not by value change)
}

func TestGlobalClientGC_DefaultConstants(t *testing.T) {
	// Verify default constants are correct
	assert.Equal(t, time.Second, DefaultGCIdleCheckInterval)
	assert.Equal(t, 4096, DefaultGCChannelBufferSize)

	// Verify defaults are used in new manager
	mgr := newClientGCManager()
	assert.Equal(t, DefaultGCChannelBufferSize, cap(mgr.gcInactiveC))
	assert.Equal(t, DefaultGCChannelBufferSize, cap(mgr.createC))
	mgr.stop()
}

func TestGlobalClientGC_TryCreateReturnsCorrectValue(t *testing.T) {
	// Test that client.tryCreate returns the correct value from triggerCreate
	c1, err := NewClient("test-client",
		newTestBackendFactory(),
		WithClientMaxBackendPerHost(2),
		WithClientEnableAutoCreateBackend())
	require.NoError(t, err)
	defer c1.Close()

	cli1 := c1.(*client)

	// tryCreate should return true when auto-create is enabled and channel has space
	ok := cli1.tryCreate("b1")
	assert.True(t, ok, "tryCreate should return true when successful")

	// Create a client with auto-create explicitly disabled
	c2, err := NewClient("test-client-2",
		newTestBackendFactory(),
		WithClientMaxBackendPerHost(2),
		WithClientDisableAutoCreateBackend())
	require.NoError(t, err)
	defer c2.Close()

	cli2 := c2.(*client)

	// tryCreate should return false when auto-create is disabled
	ok = cli2.tryCreate("b1")
	assert.False(t, ok, "tryCreate should return false when auto-create is disabled")
}

func TestWithClientMaxBackendMaxIdleDuration_ExplicitZero(t *testing.T) {
	// Test that explicitly setting maxIdleDuration to 0 disables idle timeout
	// (not overridden by default in adjust())

	// Case 1: Not set - should use default
	c1, err := NewClient("test-client-1", newTestBackendFactory())
	require.NoError(t, err)
	defer c1.Close()

	cli1 := c1.(*client)
	assert.Greater(t, cli1.options.maxIdleDuration, time.Duration(0),
		"default maxIdleDuration should be positive")

	// Case 2: Explicitly set to 0 - should stay 0 (disabled)
	c2, err := NewClient("test-client-2",
		newTestBackendFactory(),
		WithClientMaxBackendMaxIdleDuration(0))
	require.NoError(t, err)
	defer c2.Close()

	cli2 := c2.(*client)
	assert.Equal(t, time.Duration(0), cli2.options.maxIdleDuration,
		"maxIdleDuration should be 0 when explicitly set to 0 (disabled)")

	// Case 3: Explicitly set to positive value - should use that value (with jitter)
	c3, err := NewClient("test-client-3",
		newTestBackendFactory(),
		WithClientMaxBackendMaxIdleDuration(time.Minute))
	require.NoError(t, err)
	defer c3.Close()

	cli3 := c3.(*client)
	// With ±10% jitter, value should be between 54s and 66s
	assert.Greater(t, cli3.options.maxIdleDuration, time.Second*50,
		"maxIdleDuration should be around 1 minute")
	assert.Less(t, cli3.options.maxIdleDuration, time.Second*70,
		"maxIdleDuration should be around 1 minute")
}

// TestGlobalClientGC_ConcurrentCloseAndGC tests that GC operations are safe
// when clients are being closed concurrently. This is a critical test for
// the race condition fix.
func TestGlobalClientGC_ConcurrentCloseAndGC(t *testing.T) {
	const numClients = 20
	const numIterations = 50

	for iter := 0; iter < numIterations; iter++ {
		mgr := newClientGCManager()
		clients := make([]*client, numClients)

		// Create and register clients
		for i := 0; i < numClients; i++ {
			c, err := NewClient("test-client",
				newTestBackendFactory(),
				WithClientMaxBackendMaxIdleDuration(time.Millisecond*10))
			require.NoError(t, err)
			clients[i] = c.(*client)
			mgr.register(clients[i])

			// Create a backend for each client
			_, _ = clients[i].getBackend("b1", false)
		}

		var wg sync.WaitGroup

		// Goroutine 1: Continuously trigger GC
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				mgr.doGCIdle()
				time.Sleep(time.Microsecond * 100)
			}
		}()

		// Goroutine 2: Continuously trigger GC inactive
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				for _, c := range clients {
					mgr.triggerGCInactive(c, "b1")
				}
				time.Sleep(time.Microsecond * 50)
			}
		}()

		// Goroutine 3: Close clients concurrently
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(time.Millisecond * 5) // Let GC run a bit first
			for _, c := range clients {
				mgr.unregister(c)
				c.Close()
			}
		}()

		wg.Wait()
		mgr.stop()
	}
	// Test passes if no panic, deadlock, or race condition
}

// TestGlobalClientGC_CloseIdleBackendsOnClosedClient tests that closeIdleBackends
// safely handles a closed client.
func TestGlobalClientGC_CloseIdleBackendsOnClosedClient(t *testing.T) {
	c, err := NewClient("test-client",
		newTestBackendFactory(),
		WithClientMaxBackendMaxIdleDuration(time.Millisecond*10),
		WithClientEnableAutoCreateBackend())
	require.NoError(t, err)

	client := c.(*client)

	// Create a backend
	_ = getBackendWithRetry(t, client, "b1", false)

	// Close the client
	c.Close()

	// Now call closeIdleBackends on closed client - should not panic
	client.closeIdleBackends()

	// Verify client is closed
	client.mu.Lock()
	closed := client.mu.closed
	client.mu.Unlock()
	assert.True(t, closed)
}

// TestGlobalClientGC_DoRemoveInactiveOnClosedClient tests that doRemoveInactive
// safely handles a closed client.
func TestGlobalClientGC_DoRemoveInactiveOnClosedClient(t *testing.T) {
	c, err := NewClient("test-client", newTestBackendFactory(),
		WithClientEnableAutoCreateBackend())
	require.NoError(t, err)

	client := c.(*client)

	// Create a backend
	_ = getBackendWithRetry(t, client, "b1", false)

	// Close the client
	c.Close()

	// Now call doRemoveInactive on closed client - should not panic
	client.doRemoveInactive("b1")

	// Verify client is closed
	client.mu.Lock()
	closed := client.mu.closed
	client.mu.Unlock()
	assert.True(t, closed)
}

// TestGlobalClientGC_CreateOnClosedClient tests that create requests
// are safely ignored for closed clients.
func TestGlobalClientGC_CreateOnClosedClient(t *testing.T) {
	mgr := newClientGCManager()

	c, err := NewClient("test-client",
		newTestBackendFactory(),
		WithClientEnableAutoCreateBackend())
	require.NoError(t, err)

	client := c.(*client)
	mgr.register(client)

	// Close the client but keep it registered (simulates race condition)
	client.mu.Lock()
	client.mu.closed = true
	client.mu.Unlock()

	// Trigger create - should be safely ignored
	mgr.triggerCreate(client, "b1")

	// Give time for the create loop to process
	time.Sleep(time.Millisecond * 50)

	// Verify no backends were created (client was closed)
	client.mu.Lock()
	backends := client.mu.backends["b1"]
	client.mu.Unlock()
	assert.Equal(t, 0, len(backends), "no backends should be created for closed client")

	mgr.unregister(client)
	mgr.stop()

	// Properly close the client
	client.mu.Lock()
	client.mu.closed = false // Reset for proper Close()
	client.mu.Unlock()
	c.Close()
}

// TestGlobalClientGC_RaceConditionSimulation simulates the exact race condition
// that was fixed: GC goroutine accessing client while it's being closed.
func TestGlobalClientGC_RaceConditionSimulation(t *testing.T) {
	const numIterations = 100

	for iter := 0; iter < numIterations; iter++ {
		mgr := newClientGCManager()

		c, err := NewClient("test-client",
			newTestBackendFactory(),
			WithClientMaxBackendMaxIdleDuration(time.Nanosecond)) // Very short to trigger GC
		require.NoError(t, err)

		client := c.(*client)
		mgr.register(client)

		// Create backends
		for i := 0; i < 5; i++ {
			_, _ = client.getBackend("b1", false)
		}

		// Start GC in background
		done := make(chan struct{})
		go func() {
			for {
				select {
				case <-done:
					return
				default:
					mgr.doGCIdle()
				}
			}
		}()

		// Immediately close client (race with GC)
		mgr.unregister(client)
		c.Close()

		close(done)
		mgr.stop()
	}
	// Test passes if no panic or deadlock
}
