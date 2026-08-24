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

func TestCreateQueueFullReturnsLocalCongestion(t *testing.T) {
	factory := &failingCreateFactory{}
	rpcClient, err := NewClient(
		"queue-full-backpressure",
		factory,
		WithClientEnableAutoCreateBackend(),
		WithClientCircuitBreaker(CircuitBreakerConfig{
			Enabled:             true,
			FailureThreshold:    1,
			ResetTimeout:        time.Hour,
			HalfOpenMaxRequests: 1,
		}),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rpcClient.Close())
	}()
	c := rpcClient.(*client)

	// Use a stopped manager with one occupied slot so queue saturation is
	// deterministic and no worker can drain it between observation and send.
	congested := newClientGCManager()
	congested.createC = make(chan createRequest, 1)
	c.mu.Lock()
	occupiedGeneration := c.backendGenerationLocked("occupied")
	occupiedState := newBackendCreateState(occupiedGeneration)
	c.mu.creating["occupied"] = occupiedState
	c.mu.Unlock()
	congested.createC <- createRequest{
		c:       c,
		backend: "occupied",
		state:   occupiedState,
	}

	var pingErr error
	func() {
		original := c.gcManager
		c.gcManager = congested
		defer func() {
			c.gcManager = original
		}()
		pingErr = rpcClient.Ping(context.Background(), "target")
	}()
	congested.stop()

	require.ErrorIs(t, pingErr, ErrBackendCreateQueueFull)
	require.Equal(t, StatusTransient, GetStatusCategory(pingErr))
	require.Zero(t, factory.attempts.Load(),
		"queue saturation must not bypass the factory concurrency bound")
	stats := c.circuitBreakers.GetBreaker("target").Stats()
	require.Equal(t, CircuitClosed, stats.State)
	require.Zero(t, stats.FailureCount,
		"process-local queue congestion must not poison a peer breaker")
}

func TestHalfOpenCreateQueueCongestionReleasesProbe(t *testing.T) {
	operations := []struct {
		name string
		call func(RPCClient, string) error
	}{
		{
			name: "send",
			call: func(c RPCClient, remote string) error {
				_, err := c.Send(
					context.Background(),
					remote,
					&testMessage{id: 1},
				)
				return err
			},
		},
		{
			name: "new-stream",
			call: func(c RPCClient, remote string) error {
				stream, err := c.NewStream(context.Background(), remote, false)
				if stream != nil {
					_ = stream.Close(false)
				}
				return err
			},
		},
		{
			name: "ping",
			call: func(c RPCClient, remote string) error {
				return c.Ping(context.Background(), remote)
			},
		},
	}
	congestionCases := []struct {
		name      string
		queueSize int
		fillQueue bool
		wantErr   error
		queueWait time.Duration
	}{
		{
			name:      "queue-full",
			queueSize: 1,
			fillQueue: true,
			wantErr:   ErrBackendCreateQueueFull,
		},
		{
			name:      "queue-timeout",
			queueSize: 2,
			wantErr:   ErrBackendCreateQueueTimeout,
			queueWait: 10 * time.Millisecond,
		},
	}

	for _, operation := range operations {
		for _, congestion := range congestionCases {
			t.Run(operation.name+"/"+congestion.name, func(t *testing.T) {
				factory := &failingCreateFactory{}
				rpcClient, err := NewClient(
					"half-open-"+operation.name+"-"+congestion.name,
					factory,
					WithClientEnableAutoCreateBackend(),
					WithClientAutoCreateQueueWaitTimeout(congestion.queueWait),
					WithClientCircuitBreaker(CircuitBreakerConfig{
						Enabled:             true,
						FailureThreshold:    1,
						ResetTimeout:        0,
						HalfOpenMaxRequests: 3,
					}),
				)
				require.NoError(t, err)
				defer func() {
					require.NoError(t, rpcClient.Close())
				}()
				c := rpcClient.(*client)

				congested := newClientGCManager()
				congested.createC = make(chan createRequest, congestion.queueSize)
				if congestion.fillQueue {
					c.mu.Lock()
					generation := c.backendGenerationLocked("occupied")
					state := newBackendCreateState(generation)
					c.mu.creating["occupied"] = state
					c.mu.Unlock()
					congested.createC <- createRequest{
						c:       c,
						backend: "occupied",
						state:   state,
					}
				}

				original := c.gcManager
				c.gcManager = congested
				defer func() {
					c.gcManager = original
					congested.stop()
				}()

				const remote = "target"
				c.circuitBreakers.RecordFailure(remote)
				for range 2 {
					err := operation.call(rpcClient, remote)
					require.ErrorIs(t, err, congestion.wantErr)
				}

				stats := c.circuitBreakers.GetBreaker(remote).Stats()
				require.Equal(t, CircuitHalfOpen, stats.State)
				require.EqualValues(t, 1, stats.FailureCount,
					"local congestion must neither fail nor exhaust half-open probes")
				require.Zero(t, factory.attempts.Load())
			})
		}
	}
}

// TestExplicitCreateRespectsLimits verifies that explicit synchronous creation
// still respects pool limits.
func TestExplicitCreateRespectsLimits(t *testing.T) {
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
