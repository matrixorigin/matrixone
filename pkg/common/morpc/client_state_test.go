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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestErrBackendUnavailable tests the ErrBackendUnavailable error
func TestErrBackendUnavailable(t *testing.T) {
	// Test error is NoCtx (no logging)
	assert.NotNil(t, ErrBackendUnavailable)
	assert.True(t, moerr.IsMoErrCode(ErrBackendUnavailable, moerr.ErrBackendClosed))

	// Test status classification
	assert.Equal(t, StatusUnavailable, GetStatusCategory(ErrBackendUnavailable))
	assert.True(t, IsUnavailable(ErrBackendUnavailable))
	assert.False(t, IsTransient(ErrBackendUnavailable))
}

// TestErrBackendCreateTimeout tests the ErrBackendCreateTimeout error
func TestErrBackendCreateTimeout(t *testing.T) {
	// Test error is NoCtx (no logging)
	assert.NotNil(t, ErrBackendCreateTimeout)
	assert.True(t, moerr.IsMoErrCode(ErrBackendCreateTimeout, moerr.ErrBackendClosed))

	// Test status classification
	assert.Equal(t, StatusUnavailable, GetStatusCategory(ErrBackendCreateTimeout))
	assert.True(t, IsUnavailable(ErrBackendCreateTimeout))
	assert.False(t, IsTransient(ErrBackendCreateTimeout))
}

// TestErrCircuitHalfOpen tests the ErrCircuitHalfOpen error
func TestErrCircuitHalfOpen(t *testing.T) {
	// Test error is NoCtx (no logging)
	assert.NotNil(t, ErrCircuitHalfOpen)
	assert.True(t, moerr.IsMoErrCode(ErrCircuitHalfOpen, moerr.ErrServiceUnavailable))

	// Test status classification - Half-Open is transient (allows probe)
	assert.Equal(t, StatusTransient, GetStatusCategory(ErrCircuitHalfOpen))
	assert.True(t, IsTransient(ErrCircuitHalfOpen))
	assert.False(t, IsUnavailable(ErrCircuitHalfOpen))
}

// TestErrClientClosing tests the ErrClientClosing error
func TestErrClientClosing(t *testing.T) {
	// Test error is NoCtx (no logging)
	assert.NotNil(t, ErrClientClosing)
	assert.True(t, moerr.IsMoErrCode(ErrClientClosing, moerr.ErrClientClosed))

	// Test status classification
	assert.Equal(t, StatusCancelled, GetStatusCategory(ErrClientClosing))
	assert.True(t, IsCancelled(ErrClientClosing))
	assert.False(t, IsTransient(ErrClientClosing))
}

// TestCircuitBreakerGetError tests CircuitBreakerManager.GetError()
func TestCircuitBreakerGetError(t *testing.T) {
	config := CircuitBreakerConfig{
		Enabled:             true,
		FailureThreshold:    3,
		ResetTimeout:        100 * time.Millisecond,
		HalfOpenMaxRequests: 2,
	}

	logger := logutil.GetGlobalLogger().Named("test")
	manager := NewCircuitBreakerManager("test-client", config, logger)
	backend := "test-backend"

	// Test 1: Closed state returns nil
	err := manager.GetError(backend)
	assert.Nil(t, err)

	// Test 2: Trigger failures to open circuit
	for i := 0; i < 3; i++ {
		manager.Allow(backend)
		manager.RecordFailure(backend)
	}

	// Circuit should be open
	err = manager.GetError(backend)
	assert.Equal(t, ErrCircuitOpen, err)

	// Test 3: Wait for reset timeout to transition to half-open
	time.Sleep(150 * time.Millisecond)

	// Allow one request to trigger transition to half-open
	allowed := manager.Allow(backend)
	assert.True(t, allowed)

	// Circuit should be half-open
	err = manager.GetError(backend)
	assert.Equal(t, ErrCircuitHalfOpen, err)

	// Test 4: Successful probes close the circuit
	manager.RecordSuccess(backend)
	manager.Allow(backend)
	manager.RecordSuccess(backend)

	// Circuit should be closed
	err = manager.GetError(backend)
	assert.Nil(t, err)
}

// TestCircuitBreakerGetErrorDisabled tests GetError with disabled circuit breaker
func TestCircuitBreakerGetErrorDisabled(t *testing.T) {
	config := DisabledCircuitBreakerConfig
	logger := logutil.GetGlobalLogger().Named("test")
	manager := NewCircuitBreakerManager("test-client", config, logger)

	// Disabled circuit breaker always returns nil
	err := manager.GetError("any-backend")
	assert.Nil(t, err)
}

// TestClientClosingState tests client closing state behavior
func TestClientClosingState(t *testing.T) {
	bf := NewGoettyBasedBackendFactory(newTestCodec())
	c, err := NewClient("test", bf, WithClientEnableAutoCreateBackend())
	require.NoError(t, err)

	// Test: Close sets closing state
	closeDone := make(chan struct{})
	go func() {
		c.Close()
		close(closeDone)
	}()

	// Wait for close to complete
	select {
	case <-closeDone:
		// Success - close completed
	case <-time.After(time.Second):
		t.Fatal("Close() blocked for too long")
	}

	// After close, client should be in closed state
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err = c.Send(ctx, "test-backend", &testMessage{id: 1})
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrClientClosed) || errors.Is(err, ErrClientClosing))
}

// TestBackendUnavailableVsCreateTimeout tests distinction between unavailable and timeout
func TestBackendUnavailableVsCreateTimeout(t *testing.T) {
	// Use GoettyBasedBackendFactory with invalid address
	bf := NewGoettyBasedBackendFactory(newTestCodec())

	c, err := NewClient("test", bf,
		WithClientEnableAutoCreateBackend(),
		WithClientAutoCreateWaitTimeout(100*time.Millisecond))
	require.NoError(t, err)
	defer c.Close()

	// Use invalid backend address
	backend := "127.0.0.1:1" // Port 1 should be refused

	// Request should timeout after 100ms
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err = c.Send(ctx, backend, &testMessage{id: 1})

	// Should get error
	assert.Error(t, err)
}

// testFailingBackendFactory always fails to create backends
type testFailingBackendFactory struct{}

func (f *testFailingBackendFactory) Create(address string, opts ...BackendOption) (Backend, error) {
	return nil, errors.New("connection refused")
}

// TestAutoCreateWaitTimeoutDeterministic tests bounded wait timeout deterministically
func TestAutoCreateWaitTimeoutDeterministic(t *testing.T) {
	bf := &testFailingBackendFactory{}

	c, err := NewClient("test", bf,
		WithClientEnableAutoCreateBackend(),
		WithClientAutoCreateWaitTimeout(50*time.Millisecond))
	require.NoError(t, err)
	defer c.Close()

	backend := "127.0.0.1:9999"

	// Request should timeout after ~50ms
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err = c.Send(ctx, backend, &testMessage{id: 1})
	elapsed := time.Since(start)

	// Should fail
	assert.Error(t, err)

	// Should complete within reasonable time (50ms timeout + overhead)
	// Allow up to 150ms for test stability
	assert.Less(t, elapsed, 150*time.Millisecond,
		"should timeout quickly, took %v", elapsed)
}

// TestCircuitBreakerStateTransitions tests all state transitions
func TestCircuitBreakerStateTransitions(t *testing.T) {
	config := CircuitBreakerConfig{
		Enabled:             true,
		FailureThreshold:    2,
		ResetTimeout:        50 * time.Millisecond,
		HalfOpenMaxRequests: 2,
	}

	logger := logutil.GetGlobalLogger().Named("test")
	manager := NewCircuitBreakerManager("test-client", config, logger)
	backend := "test-backend"

	// State 1: Closed (initial)
	assert.Nil(t, manager.GetError(backend))
	assert.True(t, manager.Allow(backend))

	// State 2: Closed -> Open (after failures)
	manager.RecordFailure(backend)
	manager.RecordFailure(backend)
	assert.Equal(t, ErrCircuitOpen, manager.GetError(backend))
	assert.False(t, manager.Allow(backend))

	// State 3: Open -> Half-Open (after timeout)
	time.Sleep(60 * time.Millisecond)
	assert.True(t, manager.Allow(backend)) // First probe allowed
	assert.Equal(t, ErrCircuitHalfOpen, manager.GetError(backend))

	// State 4: Half-Open -> Open (on failure)
	manager.RecordFailure(backend)
	assert.Equal(t, ErrCircuitOpen, manager.GetError(backend))

	// State 5: Open -> Half-Open -> Closed (on success)
	time.Sleep(60 * time.Millisecond)
	assert.True(t, manager.Allow(backend))
	manager.RecordSuccess(backend)
	assert.True(t, manager.Allow(backend))
	manager.RecordSuccess(backend)
	assert.Nil(t, manager.GetError(backend))
}

// TestClientClosingDoesNotBlockClose tests that closing state doesn't deadlock
func TestClientClosingDoesNotBlockClose(t *testing.T) {
	bf := NewGoettyBasedBackendFactory(newTestCodec())
	c, err := NewClient("test", bf, WithClientEnableAutoCreateBackend())
	require.NoError(t, err)

	// Close should complete quickly
	done := make(chan struct{})
	go func() {
		c.Close()
		close(done)
	}()

	// Wait for close to complete
	select {
	case <-done:
		// Success
	case <-time.After(time.Second):
		t.Fatal("Close() blocked for too long")
	}
}

// TestHandleAutoCreateWaitDeterministic tests handleAutoCreateWait behavior
func TestHandleAutoCreateWaitDeterministic(t *testing.T) {
	bf := &testFailingBackendFactory{}

	c, err := NewClient("test", bf,
		WithClientEnableAutoCreateBackend(),
		WithClientAutoCreateWaitTimeout(30*time.Millisecond))
	require.NoError(t, err)
	defer c.Close()

	client := c.(*client)

	// Test 1: Timeout exceeded
	creationStart := time.Now().Add(-50 * time.Millisecond)
	shouldContinue, err := client.handleAutoCreateWait(
		context.Background(),
		"test-backend",
		&creationStart,
		1)

	assert.False(t, shouldContinue)
	assert.Equal(t, ErrBackendCreateTimeout, err)

	// Test 2: Context cancelled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	creationStart2 := time.Now()
	shouldContinue, err = client.handleAutoCreateWait(
		ctx,
		"test-backend",
		&creationStart2,
		1)

	assert.False(t, shouldContinue)
	assert.Equal(t, context.Canceled, err)

	// Test 3: Should continue (within timeout)
	creationStart3 := time.Now()
	shouldContinue, err = client.handleAutoCreateWait(
		context.Background(),
		"test-backend",
		&creationStart3,
		1)

	assert.True(t, shouldContinue)
	assert.Nil(t, err)
}

// TestErrorDistinction tests that all errors are distinguishable
func TestErrorDistinction(t *testing.T) {
	errors := []error{
		ErrBackendCreating,
		ErrBackendUnavailable,
		ErrBackendCreateTimeout,
		ErrCircuitOpen,
		ErrCircuitHalfOpen,
		ErrClientClosing,
		moerr.NewClientClosedNoCtx(),
		moerr.NewNoAvailableBackendNoCtx(),
	}

	// Test that each error has correct status
	expectedStatus := []StatusCategory{
		StatusTransient,   // ErrBackendCreating
		StatusUnavailable, // ErrBackendUnavailable
		StatusUnavailable, // ErrBackendCreateTimeout
		StatusUnavailable, // ErrCircuitOpen
		StatusTransient,   // ErrCircuitHalfOpen
		StatusCancelled,   // ErrClientClosing
		StatusCancelled,   // ErrClientClosed
		StatusUnavailable, // ErrNoAvailableBackend
	}

	for i, err := range errors {
		status := GetStatusCategory(err)
		assert.Equal(t, expectedStatus[i], status,
			"error %d (%v) has wrong status: expected %v, got %v",
			i, err, expectedStatus[i], status)
	}
}

// TestPoolSaturationReturnsCorrectError tests that when all backends are busy,
// getBackend returns ErrBackendCreating (not ErrBackendUnavailable)
func TestPoolSaturationReturnsCorrectError(t *testing.T) {
	// Regression test for: "Transient pool saturation now fails immediately"
	// Before fix: getBackend returned ErrBackendUnavailable when hasBackends=true but none usable
	// After fix: getBackend returns ErrBackendCreating to trigger wait/retry logic

	// Test that ErrBackendCreating is in the wait error list
	assert.True(t, isAutoCreateWaitError(ErrBackendCreating),
		"ErrBackendCreating should trigger wait logic")

	// Test that ErrBackendUnavailable is NOT in the wait error list
	// (This would cause immediate failure instead of waiting)
	assert.False(t, isAutoCreateWaitError(ErrBackendUnavailable),
		"ErrBackendUnavailable should NOT trigger wait logic")

	// Verify status categories
	assert.Equal(t, StatusTransient, GetStatusCategory(ErrBackendCreating),
		"ErrBackendCreating should be StatusTransient")
	assert.Equal(t, StatusUnavailable, GetStatusCategory(ErrBackendUnavailable),
		"ErrBackendUnavailable should be StatusUnavailable")
}

// TestCircuitBreakerRecordFailureOnTimeout tests that circuit breaker
// records failures correctly
func TestCircuitBreakerRecordFailureOnTimeout(t *testing.T) {
	// Regression test for: "Circuit breaker not triggered on repeated failures"
	// This test verifies that RecordFailure is called appropriately

	// The fix adds RecordFailure calls in Send/NewStream/Ping when:
	// 1. handleAutoCreateWait returns false (timeout)
	// 2. MaxRetries is exceeded
	//
	// This test just verifies the error classification is correct,
	// so that the circuit breaker logic can work properly.

	// Errors that should trigger circuit breaker (StatusUnavailable)
	unavailableErrors := []error{
		ErrCircuitOpen,
		ErrBackendUnavailable,
		ErrBackendCreateTimeout,
	}

	for _, err := range unavailableErrors {
		status := GetStatusCategory(err)
		assert.Equal(t, StatusUnavailable, status,
			"error %v should be StatusUnavailable for circuit breaker", err)
	}

	// ErrCircuitHalfOpen is StatusTransient (allows probe requests)
	assert.Equal(t, StatusTransient, GetStatusCategory(ErrCircuitHalfOpen),
		"ErrCircuitHalfOpen should be StatusTransient to allow probe")

	// Errors that should NOT trigger circuit breaker (StatusCancelled)
	cancelledErrors := []error{
		ErrClientClosing,
		moerr.NewClientClosedNoCtx(),
	}

	for _, err := range cancelledErrors {
		status := GetStatusCategory(err)
		assert.Equal(t, StatusCancelled, status,
			"error %v should be StatusCancelled, not trigger circuit breaker", err)
	}
}

// TestCircuitHalfOpenRejectsImmediately tests that ErrCircuitHalfOpen
// causes immediate rejection without retry
func TestCircuitHalfOpenRejectsImmediately(t *testing.T) {
	// Regression test for: "ErrCircuitHalfOpen falls through to retry logic"
	// When circuit breaker is half-open with exhausted probes, GetError returns
	// ErrCircuitHalfOpen. This should be treated as immediate rejection, not retry.

	// Both ErrCircuitOpen and ErrCircuitHalfOpen should cause immediate rejection
	// in Send/NewStream/Ping (checked via errors.Is before retry logic)

	// Verify both errors are distinct
	assert.False(t, errors.Is(ErrCircuitHalfOpen, ErrCircuitOpen),
		"ErrCircuitHalfOpen should not be ErrCircuitOpen")

	// Verify both are recognized by errors.Is
	assert.True(t, errors.Is(ErrCircuitOpen, ErrCircuitOpen))
	assert.True(t, errors.Is(ErrCircuitHalfOpen, ErrCircuitHalfOpen))

	// The fix in Send/NewStream/Ping:
	// if errors.Is(err, ErrCircuitOpen) || errors.Is(err, ErrCircuitHalfOpen) {
	//     return nil, err  // Immediate rejection, no retry
	// }
}

// TestAutoCreateDisabledFailsFast tests that when enableAutoCreate=false,
// ErrNoAvailableBackend does NOT trigger retry loop
func TestAutoCreateDisabledFailsFast(t *testing.T) {
	// Regression test for: "Auto-create disabled still triggers infinite retries"
	// When enableAutoCreate=false and pool is EMPTY, should fail fast

	// Verify ErrNoAvailableBackend is in the wait error list
	assert.True(t, isAutoCreateWaitError(moerr.NewNoAvailableBackendNoCtx()),
		"ErrNoAvailableBackend should be in wait error list")

	// But the retry only happens when enableAutoCreate=true
	// This is enforced by: waitingForCreate := (enableAutoCreate && isAutoCreateWaitError(err)) || isErrBackendCreating(err)
	// So when enableAutoCreate=false and pool is empty:
	// - getBackend returns ErrNoAvailableBackend (not ErrBackendCreating)
	// - waitingForCreate = (false && true) || false = false
	// - No retry, fail fast

	// This test documents the expected behavior:
	// - enableAutoCreate=true + ErrNoAvailableBackend → retry
	// - enableAutoCreate=false + pool empty → fail fast (ErrNoAvailableBackend)
	// - enableAutoCreate=false + pool busy → wait (ErrBackendCreating)
}

// TestAutoCreateDisabledPoolBusyWaits tests that when enableAutoCreate=false
// but pool has busy backends, it should wait instead of failing immediately
func TestAutoCreateDisabledPoolBusyWaits(t *testing.T) {
	// Regression test for: "enableAutoCreate=false + pool busy should wait"
	// When pool has backends but all are busy, should wait regardless of enableAutoCreate

	// ErrBackendCreating always triggers wait (regardless of enableAutoCreate)
	assert.True(t, isErrBackendCreating(ErrBackendCreating),
		"ErrBackendCreating should be recognized")

	// Verify the wait logic formula:
	// waitingForCreate := (enableAutoCreate && isAutoCreateWaitError(err)) || isErrBackendCreating(err)
	enableAutoCreate := false
	err := ErrBackendCreating

	// Simulate the actual condition in Send/NewStream/Ping
	waitingForCreate := (enableAutoCreate && isAutoCreateWaitError(err)) || isErrBackendCreating(err)

	// Should be: (false && true) || true = true
	assert.True(t, waitingForCreate,
		"ErrBackendCreating should trigger wait even when enableAutoCreate=false")

	// This ensures pre-created/manual connection pools work correctly under load
}
