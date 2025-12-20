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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"go.uber.org/zap"
)

// CircuitState represents the state of a circuit breaker.
type CircuitState int32

const (
	// CircuitClosed is the normal state where requests are allowed.
	CircuitClosed CircuitState = iota
	// CircuitOpen is the state where all requests are rejected.
	CircuitOpen
	// CircuitHalfOpen is the state where limited requests are allowed for probing.
	CircuitHalfOpen
)

// String returns the string representation of the circuit state.
func (s CircuitState) String() string {
	switch s {
	case CircuitClosed:
		return "closed"
	case CircuitOpen:
		return "open"
	case CircuitHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

var (
	// DefaultCircuitBreakerConfig disables circuit breaker by default.
	// This preserves existing behavior where callers rely on context timeout
	// and retry logic. Use EnabledCircuitBreakerConfig to opt-in.
	DefaultCircuitBreakerConfig = CircuitBreakerConfig{
		Enabled: false,
	}

	// EnabledCircuitBreakerConfig is a recommended circuit breaker configuration.
	// It opens the circuit after 5 consecutive failures, waits 10 seconds before
	// allowing probe requests, and allows 3 probe requests in half-open state.
	// Use WithClientCircuitBreaker(EnabledCircuitBreakerConfig) to enable.
	EnabledCircuitBreakerConfig = CircuitBreakerConfig{
		Enabled:             true,
		FailureThreshold:    5,
		ResetTimeout:        10 * time.Second,
		HalfOpenMaxRequests: 3,
	}

	// DisabledCircuitBreakerConfig explicitly disables the circuit breaker.
	DisabledCircuitBreakerConfig = CircuitBreakerConfig{
		Enabled: false,
	}
)

// CircuitBreakerConfig defines the configuration for a circuit breaker.
type CircuitBreakerConfig struct {
	// Enabled determines if circuit breaker is active.
	Enabled bool
	// FailureThreshold is the number of consecutive failures before opening the circuit.
	FailureThreshold int32
	// ResetTimeout is the duration to wait before transitioning from open to half-open.
	ResetTimeout time.Duration
	// HalfOpenMaxRequests is the maximum number of requests allowed in half-open state.
	HalfOpenMaxRequests int32
}

// CircuitBreaker implements the circuit breaker pattern for a single backend.
// It tracks failures and prevents cascading failures by temporarily rejecting
// requests to a failing backend.
type CircuitBreaker struct {
	config CircuitBreakerConfig
	logger *zap.Logger

	mu sync.RWMutex
	// state is the current circuit state
	state CircuitState
	// failures is the count of consecutive failures in closed state
	failures int32
	// lastFailure is the timestamp of the last failure
	lastFailure time.Time
	// halfOpenRequests is the count of requests in half-open state
	halfOpenRequests int32
	// halfOpenSuccesses is the count of successful requests in half-open state
	halfOpenSuccesses int32
	// metrics
	openCount    int64
	rejectCount  int64
	successCount int64
	failureCount int64
}

// NewCircuitBreaker creates a new CircuitBreaker with the given configuration.
func NewCircuitBreaker(config CircuitBreakerConfig, logger *zap.Logger) *CircuitBreaker {
	return &CircuitBreaker{
		config: config,
		logger: logger,
		state:  CircuitClosed,
	}
}

// Allow checks if a request should be allowed.
// Returns true if the request is allowed, false if rejected due to open circuit.
func (cb *CircuitBreaker) Allow() bool {
	if !cb.config.Enabled {
		return true
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		return true

	case CircuitOpen:
		// Check if reset timeout has passed
		if time.Since(cb.lastFailure) >= cb.config.ResetTimeout {
			cb.transitionToHalfOpenLocked()
			// Allow the first probe request
			cb.halfOpenRequests++
			return true
		}
		atomic.AddInt64(&cb.rejectCount, 1)
		return false

	case CircuitHalfOpen:
		// Allow limited requests for probing
		if cb.halfOpenRequests < cb.config.HalfOpenMaxRequests {
			cb.halfOpenRequests++
			return true
		}
		atomic.AddInt64(&cb.rejectCount, 1)
		return false

	default:
		return true
	}
}

// RecordSuccess records a successful request.
func (cb *CircuitBreaker) RecordSuccess() {
	if !cb.config.Enabled {
		return
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	atomic.AddInt64(&cb.successCount, 1)

	switch cb.state {
	case CircuitClosed:
		// Reset failure count on success
		cb.failures = 0

	case CircuitHalfOpen:
		cb.halfOpenSuccesses++
		// If enough successful probes, close the circuit
		if cb.halfOpenSuccesses >= cb.config.HalfOpenMaxRequests {
			cb.transitionToClosedLocked()
		}

	case CircuitOpen:
		// Shouldn't happen, but reset to closed if we get success
		cb.transitionToClosedLocked()
	}
}

// RecordFailure records a failed request.
func (cb *CircuitBreaker) RecordFailure() {
	if !cb.config.Enabled {
		return
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	atomic.AddInt64(&cb.failureCount, 1)
	cb.lastFailure = time.Now()

	switch cb.state {
	case CircuitClosed:
		cb.failures++
		if cb.failures >= cb.config.FailureThreshold {
			cb.transitionToOpenLocked()
		}

	case CircuitHalfOpen:
		// Any failure in half-open state reopens the circuit
		cb.transitionToOpenLocked()

	case CircuitOpen:
		// Already open, just update last failure time
	}
}

// State returns the current circuit state.
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// Stats returns circuit breaker statistics.
func (cb *CircuitBreaker) Stats() CircuitBreakerStats {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return CircuitBreakerStats{
		State:        cb.state,
		Failures:     cb.failures,
		LastFailure:  cb.lastFailure,
		OpenCount:    atomic.LoadInt64(&cb.openCount),
		RejectCount:  atomic.LoadInt64(&cb.rejectCount),
		SuccessCount: atomic.LoadInt64(&cb.successCount),
		FailureCount: atomic.LoadInt64(&cb.failureCount),
	}
}

// Reset resets the circuit breaker to closed state.
func (cb *CircuitBreaker) Reset() {
	if !cb.config.Enabled {
		return
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.transitionToClosedLocked()
}

func (cb *CircuitBreaker) transitionToOpenLocked() {
	if cb.state != CircuitOpen {
		atomic.AddInt64(&cb.openCount, 1)
		cb.logger.Warn("circuit breaker opened",
			zap.Int32("failures", cb.failures),
			zap.Duration("reset_timeout", cb.config.ResetTimeout))
	}
	cb.state = CircuitOpen
}

func (cb *CircuitBreaker) transitionToHalfOpenLocked() {
	cb.state = CircuitHalfOpen
	cb.halfOpenRequests = 0
	cb.halfOpenSuccesses = 0
	cb.logger.Info("circuit breaker half-open, probing backend")
}

func (cb *CircuitBreaker) transitionToClosedLocked() {
	if cb.state != CircuitClosed {
		cb.logger.Info("circuit breaker closed, backend recovered",
			zap.String("previous_state", cb.state.String()))
	}
	cb.state = CircuitClosed
	cb.failures = 0
	cb.halfOpenRequests = 0
	cb.halfOpenSuccesses = 0
}

// CircuitBreakerStats contains statistics for a circuit breaker.
type CircuitBreakerStats struct {
	State        CircuitState
	Failures     int32
	LastFailure  time.Time
	OpenCount    int64
	RejectCount  int64
	SuccessCount int64
	FailureCount int64
}

// CircuitBreakerManager manages circuit breakers for multiple backends.
type CircuitBreakerManager struct {
	config CircuitBreakerConfig
	logger *zap.Logger

	mu       sync.RWMutex
	breakers map[string]*CircuitBreaker
}

// NewCircuitBreakerManager creates a new CircuitBreakerManager.
func NewCircuitBreakerManager(config CircuitBreakerConfig, logger *zap.Logger) *CircuitBreakerManager {
	return &CircuitBreakerManager{
		config:   config,
		logger:   logger,
		breakers: make(map[string]*CircuitBreaker),
	}
}

// GetBreaker returns the circuit breaker for the given backend address.
// Creates a new breaker if one doesn't exist.
func (m *CircuitBreakerManager) GetBreaker(backend string) *CircuitBreaker {
	if !m.config.Enabled {
		// Return a disabled breaker that always allows
		return &CircuitBreaker{config: DisabledCircuitBreakerConfig}
	}

	m.mu.RLock()
	cb, ok := m.breakers[backend]
	m.mu.RUnlock()

	if ok {
		return cb
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if cb, ok = m.breakers[backend]; ok {
		return cb
	}

	cb = NewCircuitBreaker(m.config, m.logger.With(zap.String("backend", backend)))
	m.breakers[backend] = cb
	return cb
}

// Allow checks if a request to the given backend should be allowed.
func (m *CircuitBreakerManager) Allow(backend string) bool {
	return m.GetBreaker(backend).Allow()
}

// RecordSuccess records a successful request to the given backend.
func (m *CircuitBreakerManager) RecordSuccess(backend string) {
	m.GetBreaker(backend).RecordSuccess()
}

// RecordFailure records a failed request to the given backend.
func (m *CircuitBreakerManager) RecordFailure(backend string) {
	m.GetBreaker(backend).RecordFailure()
}

// RemoveBreaker removes the circuit breaker for the given backend.
func (m *CircuitBreakerManager) RemoveBreaker(backend string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.breakers, backend)
}

// Stats returns statistics for all circuit breakers.
func (m *CircuitBreakerManager) Stats() map[string]CircuitBreakerStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make(map[string]CircuitBreakerStats, len(m.breakers))
	for backend, cb := range m.breakers {
		stats[backend] = cb.Stats()
	}
	return stats
}

// ErrCircuitOpen is returned when the circuit breaker is open and rejecting requests.
var ErrCircuitOpen = moerr.NewServiceUnavailableNoCtx("circuit breaker is open")

// IsCircuitOpen checks if the error indicates a circuit breaker rejection.
// It checks both identity comparison and error code for wrapped errors.
func IsCircuitOpen(err error) bool {
	if err == nil {
		return false
	}
	// Identity check for direct comparison
	if err == ErrCircuitOpen {
		return true
	}
	// Check error code for wrapped errors or recreated errors
	if moerr.IsMoErrCode(err, moerr.ErrServiceUnavailable) {
		// Also check the message to distinguish from other ServiceUnavailable errors
		return strings.Contains(err.Error(), "circuit breaker")
	}
	return false
}
