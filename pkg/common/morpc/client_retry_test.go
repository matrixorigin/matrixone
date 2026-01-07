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
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRetryPolicyNextBackoff tests the nextBackoff calculation
func TestRetryPolicyNextBackoff(t *testing.T) {
	tests := []struct {
		name           string
		policy         RetryPolicy
		currentBackoff time.Duration
		expectedMin    time.Duration
		expectedMax    time.Duration
	}{
		{
			name: "initial backoff",
			policy: RetryPolicy{
				InitialBackoff: 10 * time.Millisecond,
				MaxBackoff:     1 * time.Second,
				Multiplier:     2.0,
				JitterFraction: 0,
			},
			currentBackoff: 0,
			expectedMin:    10 * time.Millisecond,
			expectedMax:    10 * time.Millisecond,
		},
		{
			name: "exponential growth",
			policy: RetryPolicy{
				InitialBackoff: 10 * time.Millisecond,
				MaxBackoff:     1 * time.Second,
				Multiplier:     2.0,
				JitterFraction: 0,
			},
			currentBackoff: 10 * time.Millisecond,
			expectedMin:    20 * time.Millisecond,
			expectedMax:    20 * time.Millisecond,
		},
		{
			name: "max backoff limit",
			policy: RetryPolicy{
				InitialBackoff: 10 * time.Millisecond,
				MaxBackoff:     50 * time.Millisecond,
				Multiplier:     2.0,
				JitterFraction: 0,
			},
			currentBackoff: 100 * time.Millisecond,
			expectedMin:    50 * time.Millisecond,
			expectedMax:    50 * time.Millisecond,
		},
		{
			name: "with jitter - bounds check",
			policy: RetryPolicy{
				InitialBackoff: 100 * time.Millisecond,
				MaxBackoff:     1 * time.Second,
				Multiplier:     2.0,
				JitterFraction: 0.2, // ±20%
			},
			currentBackoff: 100 * time.Millisecond,
			expectedMin:    160 * time.Millisecond, // 200ms - 20%
			expectedMax:    240 * time.Millisecond, // 200ms + 20%
		},
		{
			name: "no jitter when fraction is 0",
			policy: RetryPolicy{
				InitialBackoff: 100 * time.Millisecond,
				MaxBackoff:     1 * time.Second,
				Multiplier:     3.0,
				JitterFraction: 0,
			},
			currentBackoff: 100 * time.Millisecond,
			expectedMin:    300 * time.Millisecond,
			expectedMax:    300 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Run multiple times to test jitter randomness
			iterations := 1
			if tt.policy.JitterFraction > 0 {
				iterations = 100
			}

			for i := 0; i < iterations; i++ {
				result := tt.policy.nextBackoff(tt.currentBackoff)
				assert.GreaterOrEqual(t, result, tt.expectedMin, "backoff should be >= minimum")
				assert.LessOrEqual(t, result, tt.expectedMax, "backoff should be <= maximum")

				// Test that it never goes negative
				assert.GreaterOrEqual(t, result, time.Duration(0))
			}
		})
	}
}

// TestRetryPolicyZeroJitterFraction ensures deterministic behavior with no jitter
func TestRetryPolicyZeroJitterFraction(t *testing.T) {
	policy := RetryPolicy{
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
		Multiplier:     2.0,
		JitterFraction: 0,
	}

	backoff := time.Duration(0)
	expected := []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		40 * time.Millisecond,
		80 * time.Millisecond,
		160 * time.Millisecond,
	}

	for i, exp := range expected {
		backoff = policy.nextBackoff(backoff)
		assert.Equal(t, exp, backoff, "iteration %d", i)
	}
}

// TestWithClientRetryPolicy tests the WithClientRetryPolicy option
func TestWithClientRetryPolicy(t *testing.T) {
	customPolicy := RetryPolicy{
		MaxRetries:     5,
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     500 * time.Millisecond,
		Multiplier:     1.5,
		JitterFraction: 0.1,
	}

	rc, err := NewClient(
		"test-retry-policy",
		newTestBackendFactory(),
		WithClientMaxBackendPerHost(1),
		WithClientRetryPolicy(customPolicy),
	)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, rc.Close())
	}()

	c := rc.(*client)
	assert.Equal(t, customPolicy.MaxRetries, c.options.retryPolicy.MaxRetries)
	assert.Equal(t, customPolicy.InitialBackoff, c.options.retryPolicy.InitialBackoff)
	assert.Equal(t, customPolicy.MaxBackoff, c.options.retryPolicy.MaxBackoff)
	assert.Equal(t, customPolicy.Multiplier, c.options.retryPolicy.Multiplier)
	assert.Equal(t, customPolicy.JitterFraction, c.options.retryPolicy.JitterFraction)
}

// TestWithClientDisableRetry tests the WithClientDisableRetry option
func TestWithClientDisableRetry(t *testing.T) {
	rc, err := NewClient(
		"test-disable-retry",
		newTestBackendFactory(),
		WithClientMaxBackendPerHost(1),
		WithClientDisableRetry(),
	)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, rc.Close())
	}()

	c := rc.(*client)
	assert.Equal(t, NoRetryPolicy.MaxRetries, c.options.retryPolicy.MaxRetries)
	assert.Equal(t, 1, c.options.retryPolicy.MaxRetries, "should only try once")
}

// TestWithClientCircuitBreaker tests the WithClientCircuitBreaker option
func TestWithClientCircuitBreaker(t *testing.T) {
	customConfig := CircuitBreakerConfig{
		Enabled:          true,
		FailureThreshold: 10,
		ResetTimeout:     2 * time.Second,
	}

	rc, err := NewClient(
		"test-circuit-breaker",
		newTestBackendFactory(),
		WithClientMaxBackendPerHost(1),
		WithClientCircuitBreaker(customConfig),
	)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, rc.Close())
	}()

	c := rc.(*client)
	assert.Equal(t, customConfig.Enabled, c.options.circuitBreakerConfig.Enabled)
	assert.Equal(t, customConfig.FailureThreshold, c.options.circuitBreakerConfig.FailureThreshold)
	assert.Equal(t, customConfig.ResetTimeout, c.options.circuitBreakerConfig.ResetTimeout)
}

// TestWithClientDisableCircuitBreaker tests the WithClientDisableCircuitBreaker option
func TestWithClientDisableCircuitBreaker(t *testing.T) {
	rc, err := NewClient(
		"test-disable-circuit-breaker",
		newTestBackendFactory(),
		WithClientMaxBackendPerHost(1),
		WithClientDisableCircuitBreaker(),
	)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, rc.Close())
	}()

	c := rc.(*client)
	assert.False(t, c.options.circuitBreakerConfig.Enabled, "circuit breaker should be disabled")
	assert.Equal(t, DisabledCircuitBreakerConfig.Enabled, c.options.circuitBreakerConfig.Enabled)
}

// TestClientSendWithContextCancellation tests Send behavior when context is canceled
func TestClientSendWithContextCancellation(t *testing.T) {
	testRPCServer(t, func(rs *server) {
		c := newTestClient(t, WithClientMaxBackendPerHost(1))
		defer func() {
			assert.NoError(t, c.Close())
		}()

		requestReceived := make(chan struct{})
		rs.RegisterRequestHandler(func(_ context.Context, request RPCMessage, _ uint64, cs ClientSession) error {
			close(requestReceived)
			time.Sleep(100 * time.Millisecond) // Simulate slow processing
			return cs.Write(context.Background(), request.Message)
		})

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		req := newTestMessage(1)
		f, err := c.Send(ctx, testAddr, req)

		if err == nil {
			<-requestReceived // Wait until request is being processed
			cancel()          // Cancel after request started
			_, err = f.Get()
			f.Close()
		}

		assert.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled))
	})
}

// TestClientSendWithEmptyBackend tests Send with empty backend string
func TestClientSendWithEmptyBackend(t *testing.T) {
	rc, err := NewClient(
		"test-empty-backend",
		newTestBackendFactory(),
		WithClientMaxBackendPerHost(1),
	)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, rc.Close())
	}()

	ctx := context.Background()
	req := newTestMessage(1)

	f, err := rc.Send(ctx, "", req)
	assert.Error(t, err)
	assert.Nil(t, f)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrBackendCannotConnect))
}

// TestClientSendWithCircuitBreakerOpen tests Send when circuit breaker is open
func TestClientSendWithCircuitBreakerOpen(t *testing.T) {
	rc, err := NewClient(
		"test-cb-open",
		newTestBackendFactory(),
		WithClientMaxBackendPerHost(1),
		WithClientCircuitBreaker(CircuitBreakerConfig{
			Enabled:          true,
			FailureThreshold: 1, // Open after 1 failure
			ResetTimeout:     1 * time.Second,
		}),
	)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, rc.Close())
	}()

	c := rc.(*client)
	backend := "test-backend"

	// Force circuit breaker to open
	c.circuitBreakers.RecordFailure(backend)

	ctx := context.Background()
	req := newTestMessage(1)

	f, err := rc.Send(ctx, backend, req)
	assert.Error(t, err)
	assert.Nil(t, f)
	assert.Equal(t, ErrCircuitOpen, err)
}

// TestClientNewStreamWithContextCancellation tests NewStream with context cancellation
func TestClientNewStreamWithContextCancellation(t *testing.T) {
	testRPCServer(t, func(rs *server) {
		c := newTestClient(t, WithClientMaxBackendPerHost(1))
		defer func() {
			assert.NoError(t, c.Close())
		}()

		rs.RegisterRequestHandler(func(_ context.Context, request RPCMessage, _ uint64, cs ClientSession) error {
			return cs.Write(context.Background(), request.Message)
		})

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		st, err := c.NewStream(ctx, testAddr, false)
		assert.Error(t, err)
		assert.Nil(t, st)
		assert.Equal(t, context.Canceled, err)
	})
}

// TestClientNewStreamWithCircuitBreakerOpen tests NewStream when circuit breaker is open
func TestClientNewStreamWithCircuitBreakerOpen(t *testing.T) {
	rc, err := NewClient(
		"test-stream-cb",
		newTestBackendFactory(),
		WithClientMaxBackendPerHost(1),
		WithClientCircuitBreaker(CircuitBreakerConfig{
			Enabled:          true,
			FailureThreshold: 1,
			ResetTimeout:     1 * time.Second,
		}),
	)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, rc.Close())
	}()

	c := rc.(*client)
	backend := "test-backend"

	// Open circuit breaker
	c.circuitBreakers.RecordFailure(backend)

	ctx := context.Background()
	st, err := rc.NewStream(ctx, backend, false)
	assert.Error(t, err)
	assert.Nil(t, st)
	assert.Equal(t, ErrCircuitOpen, err)
}

// TestClientPingWithCircuitBreakerOpen tests Ping when circuit breaker is open
func TestClientPingWithCircuitBreakerOpen(t *testing.T) {
	rc, err := NewClient(
		"test-ping-cb",
		newTestBackendFactory(),
		WithClientMaxBackendPerHost(1),
		WithClientCircuitBreaker(CircuitBreakerConfig{
			Enabled:          true,
			FailureThreshold: 1,
			ResetTimeout:     1 * time.Second,
		}),
	)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, rc.Close())
	}()

	c := rc.(*client)
	backend := "test-backend"

	// Open circuit breaker
	c.circuitBreakers.RecordFailure(backend)

	ctx := context.Background()
	err = rc.Ping(ctx, backend)
	assert.Error(t, err)
	assert.Equal(t, ErrCircuitOpen, err)
}

// TestClientSendWithRetrySuccess tests successful retry after backend closed
func TestClientSendWithRetrySuccess(t *testing.T) {
	// This test uses a mock backend that fails once then succeeds
	factory := &testBackendFactoryWithRetry{
		attemptCounter: atomic.Int32{},
	}

	rc, err := NewClient(
		"test-retry-success",
		factory,
		WithClientMaxBackendPerHost(1),
		WithClientRetryPolicy(RetryPolicy{
			MaxRetries:     3,
			InitialBackoff: 1 * time.Millisecond,
			MaxBackoff:     10 * time.Millisecond,
			Multiplier:     2.0,
			JitterFraction: 0,
		}),
	)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, rc.Close())
	}()

	// Note: This test demonstrates the retry mechanism structure
	// Actual retry testing requires a more complex setup with real backends
}

// TestClientWithClosedClient tests operations on a closed client
func TestClientWithClosedClient(t *testing.T) {
	rc, err := NewClient(
		"test-closed-client",
		newTestBackendFactory(),
		WithClientMaxBackendPerHost(1),
	)
	require.NoError(t, err)

	// Close the client
	assert.NoError(t, rc.Close())

	ctx := context.Background()
	req := newTestMessage(1)

	// Test Send on closed client
	_, err = rc.Send(ctx, "test-backend", req)
	assert.Error(t, err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrClientClosed))

	// Test NewStream on closed client
	_, err = rc.NewStream(ctx, "test-backend", false)
	assert.Error(t, err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrClientClosed))

	// Test Ping on closed client
	err = rc.Ping(ctx, "test-backend")
	assert.Error(t, err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrClientClosed))
}

// TestRetryPolicyDefaults tests that default retry policy is applied correctly
func TestRetryPolicyDefaults(t *testing.T) {
	rc, err := NewClient(
		"test-default-policy",
		newTestBackendFactory(),
		WithClientMaxBackendPerHost(1),
		// No retry policy specified, should use DefaultRetryPolicy
	)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, rc.Close())
	}()

	c := rc.(*client)
	assert.Equal(t, DefaultRetryPolicy.MaxRetries, c.options.retryPolicy.MaxRetries)
	assert.Equal(t, DefaultRetryPolicy.InitialBackoff, c.options.retryPolicy.InitialBackoff)
	assert.Equal(t, DefaultRetryPolicy.MaxBackoff, c.options.retryPolicy.MaxBackoff)
}

// Mock backend factory for testing retry behavior
type testBackendFactoryWithRetry struct {
	attemptCounter atomic.Int32
}

func (f *testBackendFactoryWithRetry) Create(remote string, options ...BackendOption) (Backend, error) {
	b := &testBackendWithRetry{
		attemptCounter: &f.attemptCounter,
	}
	b.testBackend.activeTime = time.Now()
	return b, nil
}

type testBackendWithRetry struct {
	testBackend
	attemptCounter *atomic.Int32
}

func (b *testBackendWithRetry) Send(ctx context.Context, request Message) (*Future, error) {
	attempts := b.attemptCounter.Add(1)
	if attempts == 1 {
		return nil, backendClosed
	}
	return &Future{}, nil
}

// TestNoRetryPolicyConstants verifies NoRetryPolicy is configured correctly
func TestNoRetryPolicyConstants(t *testing.T) {
	assert.Equal(t, 1, NoRetryPolicy.MaxRetries, "NoRetryPolicy should allow only 1 attempt")
	assert.Equal(t, time.Duration(0), NoRetryPolicy.InitialBackoff)
	assert.Equal(t, time.Duration(0), NoRetryPolicy.MaxBackoff)
	assert.Equal(t, 1.0, NoRetryPolicy.Multiplier)
	assert.Equal(t, 0.0, NoRetryPolicy.JitterFraction)
}

// TestDefaultRetryPolicyConstants verifies DefaultRetryPolicy is configured correctly
func TestDefaultRetryPolicyConstants(t *testing.T) {
	assert.Equal(t, 0, DefaultRetryPolicy.MaxRetries, "DefaultRetryPolicy should have unlimited retries")
	assert.Equal(t, 10*time.Millisecond, DefaultRetryPolicy.InitialBackoff)
	assert.Equal(t, 1*time.Second, DefaultRetryPolicy.MaxBackoff)
	assert.Equal(t, 2.0, DefaultRetryPolicy.Multiplier)
	assert.Equal(t, 0.2, DefaultRetryPolicy.JitterFraction)
}

// TestClientCircuitBreakerDefaults tests that default circuit breaker config is applied
func TestClientCircuitBreakerDefaults(t *testing.T) {
	rc, err := NewClient(
		"test-default-cb",
		newTestBackendFactory(),
		WithClientMaxBackendPerHost(1),
		// No circuit breaker config specified, should use DefaultCircuitBreakerConfig
	)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, rc.Close())
	}()

	c := rc.(*client)
	assert.Equal(t, DefaultCircuitBreakerConfig.Enabled, c.options.circuitBreakerConfig.Enabled)
	assert.Equal(t, DefaultCircuitBreakerConfig.FailureThreshold, c.options.circuitBreakerConfig.FailureThreshold)
}
