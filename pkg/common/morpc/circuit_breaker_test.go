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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestCircuitBreakerInitialState(t *testing.T) {
	logger := zap.NewNop()
	cb := NewCircuitBreaker(DefaultCircuitBreakerConfig, logger)

	assert.Equal(t, CircuitClosed, cb.State())
	assert.True(t, cb.Allow())
}

func TestCircuitBreakerDisabled(t *testing.T) {
	logger := zap.NewNop()
	cb := NewCircuitBreaker(DisabledCircuitBreakerConfig, logger)

	// Always allows when disabled
	assert.True(t, cb.Allow())

	// Recording failures shouldn't affect anything
	for i := 0; i < 100; i++ {
		cb.RecordFailure()
	}
	assert.True(t, cb.Allow())
}

func TestCircuitBreakerOpensAfterThreshold(t *testing.T) {
	logger := zap.NewNop()
	config := CircuitBreakerConfig{
		Enabled:             true,
		FailureThreshold:    3,
		ResetTimeout:        100 * time.Millisecond,
		HalfOpenMaxRequests: 1,
	}
	cb := NewCircuitBreaker(config, logger)

	// Initial state is closed
	assert.Equal(t, CircuitClosed, cb.State())

	// Record failures up to threshold
	for i := 0; i < 3; i++ {
		assert.True(t, cb.Allow())
		cb.RecordFailure()
	}

	// Circuit should be open now
	assert.Equal(t, CircuitOpen, cb.State())
	assert.False(t, cb.Allow())
}

func TestCircuitBreakerSuccessResetsFailures(t *testing.T) {
	logger := zap.NewNop()
	config := CircuitBreakerConfig{
		Enabled:             true,
		FailureThreshold:    3,
		ResetTimeout:        100 * time.Millisecond,
		HalfOpenMaxRequests: 1,
	}
	cb := NewCircuitBreaker(config, logger)

	// Record 2 failures
	cb.RecordFailure()
	cb.RecordFailure()

	// Record a success - should reset failure count
	cb.RecordSuccess()

	// Record 2 more failures - should not open circuit
	cb.RecordFailure()
	cb.RecordFailure()

	assert.Equal(t, CircuitClosed, cb.State())
	assert.True(t, cb.Allow())
}

func TestCircuitBreakerHalfOpenAfterTimeout(t *testing.T) {
	logger := zap.NewNop()
	config := CircuitBreakerConfig{
		Enabled:             true,
		FailureThreshold:    2,
		ResetTimeout:        50 * time.Millisecond,
		HalfOpenMaxRequests: 2,
	}
	cb := NewCircuitBreaker(config, logger)

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()
	assert.Equal(t, CircuitOpen, cb.State())
	assert.False(t, cb.Allow())

	// Wait for reset timeout
	time.Sleep(60 * time.Millisecond)

	// First request should be allowed (transitions to half-open)
	assert.True(t, cb.Allow())
	assert.Equal(t, CircuitHalfOpen, cb.State())
}

func TestCircuitBreakerHalfOpenSuccess(t *testing.T) {
	logger := zap.NewNop()
	config := CircuitBreakerConfig{
		Enabled:             true,
		FailureThreshold:    2,
		ResetTimeout:        50 * time.Millisecond,
		HalfOpenMaxRequests: 2,
	}
	cb := NewCircuitBreaker(config, logger)

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	// Wait for reset timeout
	time.Sleep(60 * time.Millisecond)

	// Transition to half-open
	assert.True(t, cb.Allow())
	assert.Equal(t, CircuitHalfOpen, cb.State())

	// Record enough successes to close the circuit
	cb.RecordSuccess()
	cb.RecordSuccess()

	assert.Equal(t, CircuitClosed, cb.State())
	assert.True(t, cb.Allow())
}

func TestCircuitBreakerHalfOpenFailure(t *testing.T) {
	logger := zap.NewNop()
	config := CircuitBreakerConfig{
		Enabled:             true,
		FailureThreshold:    2,
		ResetTimeout:        50 * time.Millisecond,
		HalfOpenMaxRequests: 2,
	}
	cb := NewCircuitBreaker(config, logger)

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	// Wait for reset timeout
	time.Sleep(60 * time.Millisecond)

	// Transition to half-open
	assert.True(t, cb.Allow())
	assert.Equal(t, CircuitHalfOpen, cb.State())

	// Record a failure - should reopen the circuit
	cb.RecordFailure()

	assert.Equal(t, CircuitOpen, cb.State())
	assert.False(t, cb.Allow())
}

func TestCircuitBreakerHalfOpenLimitedRequests(t *testing.T) {
	logger := zap.NewNop()
	config := CircuitBreakerConfig{
		Enabled:             true,
		FailureThreshold:    2,
		ResetTimeout:        50 * time.Millisecond,
		HalfOpenMaxRequests: 2,
	}
	cb := NewCircuitBreaker(config, logger)

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	// Wait for reset timeout
	time.Sleep(60 * time.Millisecond)

	// First 2 requests should be allowed
	assert.True(t, cb.Allow())
	assert.True(t, cb.Allow())

	// Third request should be rejected
	assert.False(t, cb.Allow())
}

func TestCircuitBreakerReset(t *testing.T) {
	logger := zap.NewNop()
	config := CircuitBreakerConfig{
		Enabled:             true,
		FailureThreshold:    2,
		ResetTimeout:        1 * time.Hour, // Long timeout
		HalfOpenMaxRequests: 2,
	}
	cb := NewCircuitBreaker(config, logger)

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()
	assert.Equal(t, CircuitOpen, cb.State())

	// Reset should close the circuit
	cb.Reset()
	assert.Equal(t, CircuitClosed, cb.State())
	assert.True(t, cb.Allow())
}

func TestCircuitBreakerStats(t *testing.T) {
	logger := zap.NewNop()
	config := CircuitBreakerConfig{
		Enabled:             true,
		FailureThreshold:    5,
		ResetTimeout:        100 * time.Millisecond,
		HalfOpenMaxRequests: 2,
	}
	cb := NewCircuitBreaker(config, logger)

	// Record some events
	cb.RecordSuccess()
	cb.RecordSuccess()
	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordFailure()

	stats := cb.Stats()
	assert.Equal(t, CircuitClosed, stats.State)
	assert.Equal(t, int32(3), stats.Failures)
	assert.Equal(t, int64(2), stats.SuccessCount)
	assert.Equal(t, int64(3), stats.FailureCount)
}

func TestCircuitBreakerManager(t *testing.T) {
	logger := zap.NewNop()
	config := CircuitBreakerConfig{
		Enabled:             true,
		FailureThreshold:    2,
		ResetTimeout:        100 * time.Millisecond,
		HalfOpenMaxRequests: 1,
	}
	manager := NewCircuitBreakerManager(config, logger)

	// Different backends have independent circuit breakers
	assert.True(t, manager.Allow("backend1"))
	assert.True(t, manager.Allow("backend2"))

	// Open circuit for backend1
	manager.RecordFailure("backend1")
	manager.RecordFailure("backend1")

	// backend1 should be blocked, backend2 should work
	assert.False(t, manager.Allow("backend1"))
	assert.True(t, manager.Allow("backend2"))

	// Check stats
	stats := manager.Stats()
	assert.Equal(t, CircuitOpen, stats["backend1"].State)
	assert.Equal(t, CircuitClosed, stats["backend2"].State)
}

func TestCircuitBreakerManagerRemoveBreaker(t *testing.T) {
	logger := zap.NewNop()
	config := CircuitBreakerConfig{
		Enabled:             true,
		FailureThreshold:    2,
		ResetTimeout:        100 * time.Millisecond,
		HalfOpenMaxRequests: 1,
	}
	manager := NewCircuitBreakerManager(config, logger)

	// Open circuit for backend1
	manager.RecordFailure("backend1")
	manager.RecordFailure("backend1")
	assert.False(t, manager.Allow("backend1"))

	// Remove breaker
	manager.RemoveBreaker("backend1")

	// New breaker should be created in closed state
	assert.True(t, manager.Allow("backend1"))
}

func TestCircuitBreakerManagerDisabled(t *testing.T) {
	logger := zap.NewNop()
	manager := NewCircuitBreakerManager(DisabledCircuitBreakerConfig, logger)

	// Always allows when disabled
	for i := 0; i < 100; i++ {
		manager.RecordFailure("backend1")
	}
	assert.True(t, manager.Allow("backend1"))
}

func TestCircuitStateString(t *testing.T) {
	assert.Equal(t, "closed", CircuitClosed.String())
	assert.Equal(t, "open", CircuitOpen.String())
	assert.Equal(t, "half-open", CircuitHalfOpen.String())
	assert.Equal(t, "unknown", CircuitState(-1).String())
}

func TestIsCircuitOpen(t *testing.T) {
	assert.True(t, IsCircuitOpen(ErrCircuitOpen))
	assert.False(t, IsCircuitOpen(backendClosed))
	assert.False(t, IsCircuitOpen(nil))

	// Test with wrapped error message containing "circuit breaker"
	wrappedErr := moerr.NewServiceUnavailableNoCtx("circuit breaker is open")
	assert.True(t, IsCircuitOpen(wrappedErr))

	// Test with different ServiceUnavailable error (should not match)
	otherErr := moerr.NewServiceUnavailableNoCtx("server overloaded")
	assert.False(t, IsCircuitOpen(otherErr))
}
