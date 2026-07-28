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
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAutoCreateDisabled verifies that when auto-create is explicitly disabled,
// no backends are created and proper error is returned
func TestAutoCreateDisabled(t *testing.T) {
	// Create client with auto-create explicitly disabled
	rpcClient, err := NewClient("test", &testBackendFactory{}, WithClientDisableAutoCreateBackend())
	require.NoError(t, err)
	defer rpcClient.Close()

	c := rpcClient.(*client)

	// Verify auto-create is disabled
	assert.False(t, c.options.enableAutoCreate)

	// getBackend should return ErrNoAvailableBackend without creating anything
	_, err = c.getBackend("test-addr", false)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no available backend")

	// Verify no backend was created
	c.mu.Lock()
	backends := c.mu.backends["test-addr"]
	c.mu.Unlock()
	assert.Empty(t, backends)
}

// TestAutoCreateEnabledByDefault verifies that auto-create is enabled by default
func TestAutoCreateEnabledByDefault(t *testing.T) {
	// Create client without any auto-create option
	rpcClient, err := NewClient("test", &testBackendFactory{})
	require.NoError(t, err)
	defer rpcClient.Close()

	c := rpcClient.(*client)

	// Verify auto-create is enabled by default
	assert.True(t, c.options.enableAutoCreate)
}

// TestAutoCreateEnabled verifies that when auto-create is enabled,
// backends are created asynchronously
func TestAutoCreateEnabled(t *testing.T) {
	// Create client with auto-create enabled
	rpcClient, err := NewClient("test", &testBackendFactory{}, WithClientEnableAutoCreateBackend())
	require.NoError(t, err)
	defer rpcClient.Close()

	c := rpcClient.(*client)

	// Verify auto-create is enabled
	assert.True(t, c.options.enableAutoCreate)

	// First call should trigger async creation
	_, err = c.getBackend("test-addr", false)
	assert.Error(t, err) // No backend available yet
	assert.True(t, errors.Is(err, ErrBackendCreating))

	// Wait for async creation
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	for {
		b, err := c.getBackend("test-addr", false)
		if err == nil && b != nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("Backend creation timed out")
		case <-time.After(10 * time.Millisecond):
		}
	}

	// Verify backend was created
	c.mu.Lock()
	backends := c.mu.backends["test-addr"]
	ops := c.mu.ops["test-addr"]
	c.mu.Unlock()
	assert.Len(t, backends, 1)
	assert.NotNil(t, ops) // ops should be initialized
}

// TestCreateQueueFullFallback verifies fallback behavior when create queue is full
func TestCreateQueueFullFallback(t *testing.T) {
	// Create client with small queue size and auto-create enabled
	rpcClient, err := NewClient("test", &testBackendFactory{},
		WithClientEnableAutoCreateBackend(),
		WithClientCreateTaskChanSize(1))
	require.NoError(t, err)
	defer rpcClient.Close()

	c := rpcClient.(*client)

	// Fill the queue by triggering creation for multiple backends
	// This should fill the queue and trigger fallback for subsequent calls
	_, err1 := c.getBackend("addr1", false)
	_, err2 := c.getBackend("addr2", false)

	// Both should return "no available backend" but trigger different paths
	assert.Error(t, err1)
	assert.Error(t, err2)
	assert.True(t, errors.Is(err1, ErrBackendCreating) || moerr.IsMoErrCode(err1, moerr.ErrNoAvailableBackend))
	assert.True(t, errors.Is(err2, ErrBackendCreating) || moerr.IsMoErrCode(err2, moerr.ErrNoAvailableBackend))

	// Wait a bit for async creation
	time.Sleep(50 * time.Millisecond)

	// Verify at least one backend was created (either async or sync fallback)
	c.mu.Lock()
	n1 := len(c.mu.backends["addr1"])
	n2 := len(c.mu.backends["addr2"])
	totalBackends := n1 + n2
	ops1 := c.mu.ops["addr1"]
	ops2 := c.mu.ops["addr2"]
	c.mu.Unlock()

	assert.Greater(t, totalBackends, 0, "At least one backend should be created")
	// ops should be initialized for any created backend
	if n1 > 0 {
		assert.NotNil(t, ops1, "ops should be initialized for addr1")
	}
	if n2 > 0 {
		assert.NotNil(t, ops2, "ops should be initialized for addr2")
	}
}

// TestSyncFallbackRespectsLimits verifies that sync fallback respects pool limits
func TestSyncFallbackRespectsLimits(t *testing.T) {
	// Create client with max 1 backend per host
	rpcClient, err := NewClient("test", &testBackendFactory{},
		WithClientEnableAutoCreateBackend(),
		WithClientMaxBackendPerHost(1))
	require.NoError(t, err)
	defer rpcClient.Close()

	c := rpcClient.(*client)

	// Pre-create a backend to reach the limit
	c.mu.Lock()
	backend := &testBackend{id: 1}
	c.mu.backends["test-addr"] = []Backend{backend}
	c.mu.ops["test-addr"] = &op{}
	c.mu.Unlock()

	// Now try to create another - should fail due to limits
	b, err := c.createBackendWithBookkeeping("test-addr", false)
	assert.Error(t, err)
	assert.Nil(t, b)
	assert.Contains(t, err.Error(), "backend connection closed")

	// Verify no additional backend was created
	c.mu.Lock()
	backends := c.mu.backends["test-addr"]
	c.mu.Unlock()
	assert.Len(t, backends, 1) // Still only the original backend
}

// TestCircuitBreakerFastPath verifies circuit breaker check before lock
func TestCircuitBreakerFastPath(t *testing.T) {
	// Create client with circuit breaker
	config := CircuitBreakerConfig{
		Enabled:          true,
		FailureThreshold: 1,
		ResetTimeout:     time.Second,
	}
	rpcClient, err := NewClient("test", &testBackendFactory{},
		WithClientCircuitBreaker(config))
	require.NoError(t, err)
	defer rpcClient.Close()

	c := rpcClient.(*client)

	// Trigger circuit breaker to open
	c.circuitBreakers.RecordFailure("test-addr")

	// getBackend should return ErrCircuitOpen immediately
	_, err = c.getBackend("test-addr", false)
	assert.Error(t, err)
	assert.Equal(t, ErrCircuitOpen, err)

	// Verify no backend creation was attempted
	c.mu.Lock()
	backends := c.mu.backends["test-addr"]
	c.mu.Unlock()
	assert.Empty(t, backends)
}
