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

// Package morpc provides a high-performance RPC client with automatic backend management,
// circuit breaker, retry policies, and bounded wait for backend creation.
//
// # Backend State Machine & Error Semantics
//
// The client maintains a pool of backends for each remote address. Backend states and
// corresponding errors guide retry behavior:
//
//	┌──────────────────────────────────────────────────────────────────────────────┐
//	│ Backend State                │ Error                      │ Should Retry?   │
//	├──────────────────────────────┼────────────────────────────┼─────────────────┤
//	│ Creating (async)             │ ErrBackendCreating         │ Yes (transient) │
//	│ Pool empty, cannot create    │ ErrNoAvailableBackend      │ Yes (transient) │
//	│ Pool has backends but down   │ ErrBackendUnavailable      │ No (permanent)  │
//	│ Create timeout exceeded      │ ErrBackendCreateTimeout    │ No (permanent)  │
//	│ Circuit breaker open         │ ErrCircuitOpen             │ No (permanent)  │
//	│ Circuit breaker half-open    │ ErrCircuitHalfOpen         │ Maybe (probe)   │
//	│ Client closing               │ ErrClientClosing           │ No (permanent)  │
//	│ Client closed                │ ErrClientClosed            │ No (permanent)  │
//	└──────────────────────────────────────────────────────────────────────────────┘
//
// # Bounded Wait for Auto-Create
//
// When auto-create is enabled and a backend is being created asynchronously, callers
// can configure a bounded wait timeout:
//
//   - autoCreateWaitTimeout = 0 (default): Wait until context deadline (legacy behavior)
//   - autoCreateWaitTimeout > 0: Wait up to specified duration, then return ErrBackendClosed
//
// Example: lockservice sets 500ms timeout for fast failure detection in orphan transaction cleanup.
//
// # Retry Policy
//
// DefaultRetryPolicy retries indefinitely (MaxRetries=0) with exponential backoff.
// The retry loop exits when:
//   - Context is cancelled/timeout
//   - Non-retryable error (ErrBackendClosed, ErrCircuitOpen, ErrClientClosed)
//   - Bounded wait timeout exceeded (if configured)
//
// # Usage Example
//
//	// Default behavior (wait until context timeout)
//	client, _ := NewClient("my-service", cfg, factory)
//
//	// With bounded wait (fast failure detection)
//	client, _ := NewClient("my-service", cfg, factory,
//	    WithClientAutoCreateWaitTimeout(500*time.Millisecond))
//
// # Observability
//
// Metrics:
//   - mo_rpc_backend_auto_create_timeout_total: Auto-create wait timeouts
//   - mo_rpc_backend_create_total: Backend creation attempts
//   - mo_rpc_backend_connect_total: Connection attempts (total/failed)
//
// Logs:
//   - "waiting for backend creation": Sparse logging (1st, then every 10th retry)
//   - "auto-create backend timed out": When bounded wait timeout exceeded
package morpc

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"go.uber.org/zap"
)

var (
	// DefaultRetryPolicy is the default retry policy for morpc client.
	// It retries indefinitely (MaxRetries=0) with exponential backoff starting at 10ms,
	// maxing out at 1s, with 20% jitter. The retry loop exits when context is cancelled.
	// This matches the original design intent where context timeout is the exit mechanism.
	DefaultRetryPolicy = RetryPolicy{
		MaxRetries:     0, // 0 means unlimited, rely on context timeout
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
		Multiplier:     2.0,
		JitterFraction: 0.2,
	}

	// NoRetryPolicy disables retry (only 1 attempt).
	NoRetryPolicy = RetryPolicy{
		MaxRetries:     1,
		InitialBackoff: 0,
		MaxBackoff:     0,
		Multiplier:     1.0,
		JitterFraction: 0,
	}

	// ErrBackendCreating indicates that the backend is being created asynchronously.
	// Callers can distinguish "creation in progress" from "backend closed/unavailable".
	// This is a high-frequency expected error (NoCtx to avoid log spam).
	ErrBackendCreating = moerr.NewInternalErrorNoCtx("morpc backend is being created")

	// ErrBackendUnavailable indicates that the pool has backends but all are unavailable.
	// This typically means network partition, service crash, or all backends inactive.
	// This is a high-frequency expected error (NoCtx to avoid log spam).
	// Uses ErrBackendClosed code for compatibility with existing error handling.
	ErrBackendUnavailable = moerr.NewBackendClosedNoCtx()

	// ErrBackendCreateTimeout indicates that auto-create wait timeout exceeded.
	// This typically means backend creation is too slow or queue congestion.
	// This is a boundary condition error (NoCtx + Counter for monitoring).
	// Uses ErrBackendClosed code for compatibility with existing error handling.
	ErrBackendCreateTimeout = moerr.NewBackendClosedNoCtx()

	// ErrClientClosing indicates that the client is in the process of closing.
	// New requests should fail fast rather than waiting for backend creation.
	// This is a high-frequency expected error during shutdown (NoCtx to avoid log spam).
	ErrClientClosing = moerr.NewClientClosedNoCtx()
)

const defaultAutoCreateWaitTimeout = 0 // 0 means wait until context deadline (legacy behavior)

// RetryPolicy defines retry behavior for morpc client operations.
type RetryPolicy struct {
	// MaxRetries is the maximum number of retry attempts.
	// 0 means unlimited (rely on context timeout), which is the default behavior.
	MaxRetries int
	// InitialBackoff is the initial backoff duration before the first retry.
	InitialBackoff time.Duration
	// MaxBackoff is the maximum backoff duration.
	MaxBackoff time.Duration
	// Multiplier is the factor by which backoff increases after each retry.
	Multiplier float64
	// JitterFraction adds randomness to backoff (0.2 means ±20%).
	JitterFraction float64
}

// nextBackoff calculates the next backoff duration with jitter.
func (p RetryPolicy) nextBackoff(currentBackoff time.Duration) time.Duration {
	if currentBackoff == 0 {
		currentBackoff = p.InitialBackoff
	} else {
		currentBackoff = time.Duration(float64(currentBackoff) * p.Multiplier)
	}
	if currentBackoff > p.MaxBackoff {
		currentBackoff = p.MaxBackoff
	}
	// Add jitter: ±JitterFraction
	if p.JitterFraction > 0 {
		jitter := float64(currentBackoff) * p.JitterFraction * (2*rand.Float64() - 1)
		currentBackoff = time.Duration(float64(currentBackoff) + jitter)
		if currentBackoff < 0 {
			currentBackoff = 0
		}
	}
	return currentBackoff
}

// WithClientMaxBackendPerHost maximum number of connections per host
func WithClientMaxBackendPerHost(maxBackendsPerHost int) ClientOption {
	return func(c *client) {
		c.options.maxBackendsPerHost = maxBackendsPerHost
	}
}

// WithClientRetryPolicy sets the retry policy for the client.
// If not set, DefaultRetryPolicy is used.
func WithClientRetryPolicy(policy RetryPolicy) ClientOption {
	return func(c *client) {
		c.options.retryPolicy = policy
	}
}

// WithClientDisableRetry disables retry for the client.
func WithClientDisableRetry() ClientOption {
	return func(c *client) {
		c.options.retryPolicy = NoRetryPolicy
	}
}

// WithClientLogger set client logger
func WithClientLogger(logger *zap.Logger) ClientOption {
	return func(c *client) {
		c.logger = logutil.GetPanicLoggerWithLevel(zap.FatalLevel)
	}
}

// WithClientInitBackends set the number of connections for the initialized backends.
func WithClientInitBackends(backends []string, counts []int) ClientOption {
	return func(c *client) {
		if len(backends) != len(counts) {
			panic("backend and count mismatch")
		}

		c.options.initBackends = backends
		c.options.initBackendCounts = counts
	}
}

// WithClientCreateTaskChanSize set the buffer size of the chan that creates the Backend Task.
func WithClientCreateTaskChanSize(size int) ClientOption {
	return func(c *client) {
		c.createC = make(chan string, size)
	}
}

// WithClientAutoCreateWaitTimeout sets how long Send/NewStream/Ping will wait
// for an auto-created backend before giving up. Zero keeps legacy behavior
// (wait until context deadline).
func WithClientAutoCreateWaitTimeout(timeout time.Duration) ClientOption {
	return func(c *client) {
		c.options.autoCreateWaitTimeout = timeout
		c.options.autoCreateWaitTimeoutSet = true
	}
}

// WithClientMaxBackendMaxIdleDuration set the maximum idle duration of the backend connection.
// Backend connection that exceed this time will be automatically closed. 0 means no idle time
// limit.
//
// Note: To avoid "thundering herd" effect where many connections expire simultaneously,
// a small random jitter (±10%) is automatically applied to positive durations. This spreads
// connection expiration times across a time window, reducing the impact of simultaneous
// connection closures. When value is 0 (disabled), no jitter is applied.
func WithClientMaxBackendMaxIdleDuration(value time.Duration) ClientOption {
	return func(c *client) {
		c.options.maxIdleDuration = applyJitter(value)
		c.options.maxIdleDurationSet = true // Mark as explicitly set (even if 0)
	}
}

// applyJitter applies a small random jitter (±10%) to the duration to avoid thundering herd effect.
// This spreads connection expiration times across a time window, reducing simultaneous closures.
func applyJitter(duration time.Duration) time.Duration {
	if duration <= 0 {
		return duration
	}
	// Apply ±10% jitter
	jitterPercent := 0.1
	jitter := time.Duration(float64(duration) * jitterPercent * (2*rand.Float64() - 1))
	result := duration + jitter
	if result < 0 {
		return duration // Fallback to original if jitter makes it negative
	}
	return result
}

// WithClientEnableAutoCreateBackend enable client to automatically create a backend
// in the background, when the links in the connection pool are used, if the pool has
// not reached the maximum number of links, it will automatically create them in the
// background to improve the latency of link creation.
func WithClientEnableAutoCreateBackend() ClientOption {
	return func(c *client) {
		c.options.enableAutoCreate = true
		c.options.enableAutoCreateSet = true
	}
}

// WithClientDisableAutoCreateBackend disable client from automatically creating backends.
// By default, auto-create is enabled. Use this option to disable it.
func WithClientDisableAutoCreateBackend() ClientOption {
	return func(c *client) {
		c.options.enableAutoCreate = false
		c.options.enableAutoCreateSet = true
	}
}

// WithClientCircuitBreaker sets the circuit breaker configuration for the client.
// If not set, DefaultCircuitBreakerConfig is used.
func WithClientCircuitBreaker(config CircuitBreakerConfig) ClientOption {
	return func(c *client) {
		c.options.circuitBreakerConfig = config
	}
}

// WithClientDisableCircuitBreaker disables the circuit breaker for the client.
func WithClientDisableCircuitBreaker() ClientOption {
	return func(c *client) {
		c.options.circuitBreakerConfig = DisabledCircuitBreakerConfig
	}
}

type client struct {
	name        string
	metrics     *metrics
	logger      *zap.Logger
	stopper     *stopper.Stopper
	factory     BackendFactory
	createC     chan string
	gcInactiveC chan string

	mu struct {
		sync.Mutex
		closing  bool // true when Close() is called but not yet completed
		closed   bool // true when Close() is completed
		backends map[string][]Backend
		ops      map[string]*op
	}

	circuitBreakers *CircuitBreakerManager

	options struct {
		maxBackendsPerHost       int
		maxIdleDuration          time.Duration
		maxIdleDurationSet       bool // true if user explicitly set maxIdleDuration (even to 0)
		initBackends             []string
		initBackendCounts        []int
		enableAutoCreate         bool
		enableAutoCreateSet      bool // true if user explicitly set enableAutoCreate
		retryPolicy              RetryPolicy
		circuitBreakerConfig     CircuitBreakerConfig
		autoCreateWaitTimeout    time.Duration
		autoCreateWaitTimeoutSet bool
	}
}

// NewClient create rpc client with options
func NewClient(
	name string,
	factory BackendFactory,
	options ...ClientOption) (RPCClient, error) {
	v2.RPCClientCreateCounter.WithLabelValues(name).Inc()
	c := &client{
		name:        name,
		metrics:     newMetrics(name),
		factory:     factory,
		gcInactiveC: make(chan string),
	}
	c.mu.backends = make(map[string][]Backend)
	c.mu.ops = make(map[string]*op)

	for _, opt := range options {
		opt(c)
	}
	c.adjust()
	c.stopper = stopper.NewStopper(c.name, stopper.WithLogger(c.logger))

	if err := c.maybeInitBackends(); err != nil {
		c.Close()
		return nil, err
	}

	// Register with global GC manager instead of creating per-client goroutines
	globalClientGC.register(c)

	// Update active client count (only after successful creation)
	activeGauge := v2.NewRPCClientActiveGaugeByName(name)
	activeGauge.Inc()

	return c, nil
}

func (c *client) adjust() {
	c.logger = logutil.Adjust(c.logger).Named(c.name)
	if c.createC == nil {
		c.createC = make(chan string, 16)
	}
	if c.options.maxBackendsPerHost == 0 {
		c.options.maxBackendsPerHost = 1
	}
	if len(c.options.initBackendCounts) > 0 {
		for _, cnt := range c.options.initBackendCounts {
			if cnt > c.options.maxBackendsPerHost {
				c.options.maxBackendsPerHost = cnt
			}
		}
	}
	if !c.options.maxIdleDurationSet && c.options.maxIdleDuration == 0 {
		// Only apply default if user didn't explicitly set it
		// If user set it to 0, it means "no idle time limit" per documentation
		c.options.maxIdleDuration = applyJitter(defaultMaxIdleDuration)
	}
	// Default enableAutoCreate to true for backward compatibility
	if !c.options.enableAutoCreateSet {
		c.options.enableAutoCreate = true
	}
	// Set default retry policy if not configured
	if c.options.retryPolicy.MaxRetries == 0 && c.options.retryPolicy.InitialBackoff == 0 {
		c.options.retryPolicy = DefaultRetryPolicy
	}
	// Set default circuit breaker config if not configured
	if !c.options.circuitBreakerConfig.Enabled && c.options.circuitBreakerConfig.FailureThreshold == 0 {
		c.options.circuitBreakerConfig = DefaultCircuitBreakerConfig
	}
	// Default bounded wait for auto-create unless user overrides (0 means legacy infinite wait)
	if !c.options.autoCreateWaitTimeoutSet && c.options.autoCreateWaitTimeout == 0 {
		c.options.autoCreateWaitTimeout = defaultAutoCreateWaitTimeout
	}
	c.circuitBreakers = NewCircuitBreakerManager(c.name, c.options.circuitBreakerConfig, c.logger)
}

func (c *client) maybeInitBackends() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.options.initBackends) > 0 {
		for idx, backend := range c.options.initBackends {
			for i := 0; i < c.options.initBackendCounts[idx]; i++ {
				_, err := c.createBackendLocked(backend)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (c *client) Send(ctx context.Context, backend string, request Message) (*Future, error) {
	if backend == "" {
		return nil, moerr.NewBackendCannotConnectNoCtx()
	}

	if ctx == nil {
		panic("client Send nil context")
	}

	// Check circuit breaker before attempting
	if !c.circuitBreakers.Allow(backend) {
		return nil, c.circuitBreakers.GetError(backend)
	}

	policy := c.options.retryPolicy
	var backoff time.Duration
	retryCount := 0
	var creationStart time.Time

	for {
		// Check circuit breaker before each retry attempt
		if !c.circuitBreakers.Allow(backend) {
			// Don't record failure - circuit is already open
			return nil, c.circuitBreakers.GetError(backend)
		}

		b, err := c.getBackend(backend, false)
		if err != nil {
			// Handle circuit breaker errors - both open and half-open (exhausted probes) reject immediately
			if errors.Is(err, ErrCircuitOpen) || errors.Is(err, ErrCircuitHalfOpen) {
				return nil, err
			}

			// Wait for backend if:
			// 1. Auto-create enabled and waiting for creation, OR
			// 2. ErrBackendCreating (pool has backends but all busy - wait regardless of auto-create)
			waitingForCreate := (c.options.enableAutoCreate && isAutoCreateWaitError(err)) ||
				isErrBackendCreating(err)

			// Handle backend creation-in-progress with bounded wait
			if waitingForCreate {
				shouldContinue, waitErr := c.handleAutoCreateWait(ctx, backend, &creationStart, retryCount)
				if !shouldContinue {
					// Record circuit breaker failure on timeout
					c.circuitBreakers.RecordFailure(backend)
					return nil, waitErr
				}

				retryCount++
				// Check if max retries exceeded (0 means unlimited)
				if policy.MaxRetries > 0 && retryCount >= policy.MaxRetries {
					c.logger.Warn("max retries exceeded for Send",
						zap.String("backend", backend),
						zap.Int("retries", retryCount),
						zap.Error(err))
					// Record circuit breaker failure on max retries
					c.circuitBreakers.RecordFailure(backend)
					return nil, err
				}

				// Calculate next backoff with jitter
				backoff = policy.nextBackoff(backoff)
				if backoff > 0 {
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-time.After(backoff):
					}
				}
				continue
			}

			// Don't count client-level errors (like ErrClientClosed) as circuit breaker failures
			// Only count backend-related errors
			if !moerr.IsMoErrCode(err, moerr.ErrClientClosed) {
				c.circuitBreakers.RecordFailure(backend)
			}
			return nil, err
		}

		f, err := b.Send(ctx, request)
		if err != nil && err == backendClosed {
			c.circuitBreakers.RecordFailure(backend)
			retryCount++
			// Check if max retries exceeded (0 means unlimited)
			if policy.MaxRetries > 0 && retryCount >= policy.MaxRetries {
				c.logger.Warn("max retries exceeded for Send",
					zap.String("backend", backend),
					zap.Int("retries", retryCount),
					zap.Error(err))
				return nil, err
			}

			// Check circuit breaker after failure
			if !c.circuitBreakers.Allow(backend) {
				return nil, c.circuitBreakers.GetError(backend)
			}

			// Calculate next backoff with jitter
			backoff = policy.nextBackoff(backoff)
			if backoff > 0 {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(backoff):
				}
			}

			if retryCount <= 3 || retryCount%10 == 0 {
				c.logger.Debug("retrying Send after backend closed",
					zap.String("backend", backend),
					zap.Int("retry", retryCount),
					zap.Duration("backoff", backoff))
			}
			continue
		}
		if err == nil {
			c.circuitBreakers.RecordSuccess(backend)
		} else {
			c.circuitBreakers.RecordFailure(backend)
		}
		return f, err
	}
}

func (c *client) NewStream(ctx context.Context, backend string, lock bool) (Stream, error) {
	if ctx == nil {
		panic("client NewStream nil context")
	}

	// Check circuit breaker before attempting
	if !c.circuitBreakers.Allow(backend) {
		return nil, c.circuitBreakers.GetError(backend)
	}

	policy := c.options.retryPolicy
	var backoff time.Duration
	retryCount := 0
	var creationStart time.Time

	for {
		// Check circuit breaker before each retry attempt
		if !c.circuitBreakers.Allow(backend) {
			// Don't record failure - circuit is already open
			return nil, c.circuitBreakers.GetError(backend)
		}

		// Check context before attempting
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		b, err := c.getBackend(backend, lock)
		if err != nil {
			// Handle circuit breaker errors - both open and half-open (exhausted probes) reject immediately
			if errors.Is(err, ErrCircuitOpen) || errors.Is(err, ErrCircuitHalfOpen) {
				return nil, err
			}

			// Wait for backend if:
			// 1. Auto-create enabled and waiting for creation, OR
			// 2. ErrBackendCreating (pool has backends but all busy - wait regardless of auto-create)
			waitingForCreate := (c.options.enableAutoCreate && isAutoCreateWaitError(err)) ||
				isErrBackendCreating(err)

			// Handle backend creation-in-progress with bounded wait
			if waitingForCreate {
				shouldContinue, waitErr := c.handleAutoCreateWait(ctx, backend, &creationStart, retryCount)
				if !shouldContinue {
					// Record circuit breaker failure on timeout
					c.circuitBreakers.RecordFailure(backend)
					return nil, waitErr
				}

				retryCount++
				// Check if max retries exceeded
				if policy.MaxRetries > 0 && retryCount >= policy.MaxRetries {
					c.logger.Warn("max retries exceeded for NewStream",
						zap.String("backend", backend),
						zap.Int("retries", retryCount),
						zap.Error(err))
					// Record circuit breaker failure on max retries
					c.circuitBreakers.RecordFailure(backend)
					return nil, err
				}

				// Calculate next backoff with jitter
				backoff = policy.nextBackoff(backoff)
				if backoff > 0 {
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-time.After(backoff):
					}
				}
				continue
			}

			// Don't count client-level errors (like ErrClientClosed) as circuit breaker failures
			if !moerr.IsMoErrCode(err, moerr.ErrClientClosed) {
				c.circuitBreakers.RecordFailure(backend)
			}
			return nil, err
		}

		st, err := b.NewStream(lock)
		if err != nil && err == backendClosed {
			c.circuitBreakers.RecordFailure(backend)
			retryCount++
			// Check if max retries exceeded
			if policy.MaxRetries > 0 && retryCount >= policy.MaxRetries {
				c.logger.Warn("max retries exceeded for NewStream",
					zap.String("backend", backend),
					zap.Int("retries", retryCount),
					zap.Error(err))
				return nil, err
			}

			// Check circuit breaker after failure
			if !c.circuitBreakers.Allow(backend) {
				return nil, c.circuitBreakers.GetError(backend)
			}

			// Calculate next backoff with jitter
			backoff = policy.nextBackoff(backoff)
			if backoff > 0 {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(backoff):
				}
			}

			if retryCount <= 3 || retryCount%10 == 0 {
				c.logger.Debug("retrying NewStream after backend closed",
					zap.String("backend", backend),
					zap.Int("retry", retryCount),
					zap.Duration("backoff", backoff))
			}
			continue
		}
		if err == nil {
			c.circuitBreakers.RecordSuccess(backend)
		} else {
			c.circuitBreakers.RecordFailure(backend)
		}
		return st, err
	}
}

func (c *client) Ping(ctx context.Context, backend string) error {
	if ctx == nil {
		panic("client Ping nil context")
	}

	// Check circuit breaker before attempting
	if !c.circuitBreakers.Allow(backend) {
		return c.circuitBreakers.GetError(backend)
	}

	policy := c.options.retryPolicy
	var backoff time.Duration
	retryCount := 0
	var creationStart time.Time

	for {
		b, err := c.getBackend(backend, false)
		if err != nil {
			// Handle circuit breaker errors - both open and half-open (exhausted probes) reject immediately
			if errors.Is(err, ErrCircuitOpen) || errors.Is(err, ErrCircuitHalfOpen) {
				return err
			}

			// Wait for backend if:
			// 1. Auto-create enabled and waiting for creation, OR
			// 2. ErrBackendCreating (pool has backends but all busy - wait regardless of auto-create)
			waitingForCreate := (c.options.enableAutoCreate && isAutoCreateWaitError(err)) ||
				isErrBackendCreating(err)

			// Handle backend creation-in-progress with bounded wait
			if waitingForCreate {
				shouldContinue, waitErr := c.handleAutoCreateWait(ctx, backend, &creationStart, retryCount)
				if !shouldContinue {
					// Record circuit breaker failure on timeout
					c.circuitBreakers.RecordFailure(backend)
					return waitErr
				}

				retryCount++
				// Check if max retries exceeded
				if policy.MaxRetries > 0 && retryCount >= policy.MaxRetries {
					c.logger.Warn("max retries exceeded for Ping",
						zap.String("backend", backend),
						zap.Int("retries", retryCount),
						zap.Error(err))
					// Record circuit breaker failure on max retries
					c.circuitBreakers.RecordFailure(backend)
					return err
				}

				// Calculate next backoff with jitter
				backoff = policy.nextBackoff(backoff)
				if backoff > 0 {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(backoff):
					}
				}
				continue
			}

			// Don't count client-level errors (like ErrClientClosed) as circuit breaker failures
			if !moerr.IsMoErrCode(err, moerr.ErrClientClosed) {
				c.circuitBreakers.RecordFailure(backend)
			}
			return err
		}

		f, err := b.SendInternal(ctx, &flagOnlyMessage{flag: flagPing})
		if err != nil {
			if err == backendClosed {
				c.circuitBreakers.RecordFailure(backend)
				retryCount++
				// Check if max retries exceeded
				if policy.MaxRetries > 0 && retryCount >= policy.MaxRetries {
					c.logger.Warn("max retries exceeded for Ping",
						zap.String("backend", backend),
						zap.Int("retries", retryCount),
						zap.Error(err))
					return err
				}

				// Check circuit breaker after failure
				if !c.circuitBreakers.Allow(backend) {
					return c.circuitBreakers.GetError(backend)
				}

				// Calculate next backoff with jitter
				backoff = policy.nextBackoff(backoff)
				if backoff > 0 {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(backoff):
					}
				}

				if retryCount <= 3 || retryCount%10 == 0 {
					c.logger.Debug("retrying Ping after backend closed",
						zap.String("backend", backend),
						zap.Int("retry", retryCount),
						zap.Duration("backoff", backoff))
				}
				continue
			}
			c.circuitBreakers.RecordFailure(backend)
			return err
		}
		_, err = f.Get()
		f.Close()
		if err == nil {
			c.circuitBreakers.RecordSuccess(backend)
		} else {
			c.circuitBreakers.RecordFailure(backend)
		}
		return err
	}
}

func (c *client) Close() error {
	c.mu.Lock()
	wasClosed := c.mu.closed
	if wasClosed {
		c.mu.Unlock()
		return nil
	}
	// Set closing state first
	c.mu.closing = true
	c.mu.Unlock()

	// Close all backends
	c.mu.Lock()
	for _, backends := range c.mu.backends {
		for _, b := range backends {
			b.Close()
		}
	}
	// Mark as fully closed
	c.mu.closed = true
	c.mu.Unlock()

	// Unregister from global GC manager
	globalClientGC.unregister(c)

	// Update active client count (only if client was successfully created)
	if !wasClosed {
		activeGauge := v2.NewRPCClientActiveGaugeByName(c.name)
		activeGauge.Dec()
	}

	c.stopper.Stop()
	close(c.createC)
	return nil
}

func (c *client) CloseBackend() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, backends := range c.mu.backends {
		for _, b := range backends {
			b.Close()
		}
	}
	return nil
}

func (c *client) getBackend(backend string, lock bool) (Backend, error) {
	// Fast-fail: check circuit breaker before acquiring lock
	// This prevents blocking on lock acquisition for known-bad backends
	if !c.circuitBreakers.Allow(backend) {
		return nil, c.circuitBreakers.GetError(backend)
	}

	c.mu.Lock()
	b, err := c.getBackendLocked(backend, lock)
	poolSize := len(c.mu.backends[backend])
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	if b != nil {
		c.mu.Unlock()
		return b, nil
	}

	// No backend available in pool
	canCreate := c.canCreateLocked(backend)
	enableAutoCreate := c.options.enableAutoCreate
	hasBackends := poolSize > 0
	c.mu.Unlock() // Release lock before any potentially blocking operation

	// If pool has backends but all are busy, wait for one to become available
	// This applies regardless of enableAutoCreate setting
	if hasBackends && !canCreate {
		c.metrics.backendUnavailableCounter.Inc()
		return nil, ErrBackendCreating // Triggers wait/retry logic
	}

	// Strictly gate creation on enableAutoCreate flag
	if !enableAutoCreate {
		// No backends exist and auto-create is disabled - fail fast
		return nil, moerr.NewNoAvailableBackendNoCtx()
	}

	creationQueued := false
	if canCreate {
		// Try to enqueue creation task with backpressure handling
		if !globalClientGC.triggerCreate(c, backend) {
			// Queue is full - fallback to existing creation path with proper bookkeeping
			return c.createBackendWithBookkeeping(backend, lock)
		}
		creationQueued = true
	}

	if creationQueued {
		return nil, ErrBackendCreating
	}

	// Pool is empty and cannot create - return ErrNoAvailableBackend to trigger wait logic
	return nil, moerr.NewNoAvailableBackendNoCtx()
}

func (c *client) getBackendLocked(backend string, lock bool) (Backend, error) {
	if c.mu.closing {
		return nil, ErrClientClosing
	}
	if c.mu.closed {
		return nil, moerr.NewClientClosedNoCtx()
	}
	defer func() {
		c.updatePoolSizeMetricsLocked()
	}()

	lockedCnt := 0
	inactiveCnt := 0
	if backends, ok := c.mu.backends[backend]; ok {
		n := uint64(len(backends))
		var b Backend
		for i := uint64(0); i < n; i++ {
			seq := c.mu.ops[backend].next()
			b = backends[seq%n]
			if !b.Locked() && b.LastActiveTime() != (time.Time{}) {
				break
			}

			if b.Locked() {
				lockedCnt++
			}
			if b.LastActiveTime() == (time.Time{}) {
				inactiveCnt++
			}
			b = nil
		}

		// all backend inactived, trigger gc inactive.
		if b == nil && n > 0 {
			c.triggerGCInactive(backend)
			c.logger.Debug("no available backends",
				zap.String("backend", backend),
				zap.Int("locked", lockedCnt),
				zap.Int("inactive", inactiveCnt),
				zap.Int("max", c.options.maxBackendsPerHost))
			if !c.canCreateLocked(backend) {
				return nil, moerr.NewBackendClosedNoCtx()
			}
		}

		if lock && b != nil {
			b.Lock()
		}
		c.maybeCreateLocked(backend)
		return b, nil
	}
	return nil, nil
}

func (c *client) maybeCreateLocked(backend string) bool {
	if len(c.mu.backends[backend]) == 0 {
		return c.tryCreate(backend)
	}

	if !c.canCreateLocked(backend) {
		return false
	}

	for _, b := range c.mu.backends[backend] {
		if b.Busy() || b.Locked() {
			return c.tryCreate(backend)
		}
	}
	return false
}

func (c *client) tryCreate(backend string) bool {
	if !c.options.enableAutoCreate {
		return false
	}

	return globalClientGC.triggerCreate(c, backend)
}

func (c *client) createBackendWithBookkeeping(backend string, lock bool) (Backend, error) {
	// Use existing creation path with proper bookkeeping, but avoid holding the lock
	// during network I/O. We double-check limits after creation to avoid overfilling
	// if another goroutine created a backend in the meantime.
	c.mu.Lock()
	if c.mu.closed {
		c.mu.Unlock()
		return nil, moerr.NewClientClosedNoCtx()
	}
	if !c.canCreateLocked(backend) {
		c.mu.Unlock()
		return nil, moerr.NewBackendClosedNoCtx()
	}
	c.mu.Unlock()

	// Create backend using factory with metrics (same as doCreate) without holding the lock.
	b, err := c.doCreate(backend)
	if err != nil {
		return nil, err
	}

	// Re-acquire lock to add to pool, validating limits again.
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mu.closed {
		b.Close()
		return nil, moerr.NewClientClosedNoCtx()
	}
	if !c.canCreateLocked(backend) {
		// Another goroutine may have filled the pool while we were creating.
		b.Close()
		return nil, moerr.NewBackendClosedNoCtx()
	}

	// Apply lock if requested (only after we know the backend will be kept)
	if lock {
		b.Lock()
	}

	// Add to pool with proper bookkeeping (same as existing creation path)
	c.mu.backends[backend] = append(c.mu.backends[backend], b)

	// Initialize ops if needed (same as existing creation path)
	if c.mu.ops[backend] == nil {
		c.mu.ops[backend] = &op{}
	}

	// Update metrics (same as existing creation path)
	c.updatePoolSizeMetricsLocked()

	return b, nil
}

func (c *client) triggerGCInactive(remote string) {
	globalClientGC.triggerGCInactive(c, remote)
	c.logger.Debug("try to remove all inactived backends",
		zap.String("remote", remote))
}

func (c *client) doRemoveInactive(remote string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if client is closed
	if c.mu.closed {
		return
	}

	backends, ok := c.mu.backends[remote]
	if !ok {
		return
	}

	newBackends := backends[:0]
	for _, backend := range backends {
		if backend.LastActiveTime() == (time.Time{}) {
			backend.Close()
			continue
		}
		newBackends = append(newBackends, backend)
	}
	c.mu.backends[remote] = newBackends

	c.updatePoolSizeMetricsLocked()
}

// doRemoveInactiveAll removes all explicitly closed (inactive) backends for every remote.
// Used by the periodic GC to clean up closed backends within ~10s without waiting for
// the idle timeout (e.g. 1 minute). Safe to call on closed client (no-op).
func (c *client) doRemoveInactiveAll() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mu.closed {
		return
	}

	for remote, backends := range c.mu.backends {
		newBackends := backends[:0]
		for _, backend := range backends {
			if backend.LastActiveTime() == (time.Time{}) {
				backend.Close()
				continue
			}
			newBackends = append(newBackends, backend)
		}
		c.mu.backends[remote] = newBackends
	}
	c.updatePoolSizeMetricsLocked()
}

func (c *client) closeIdleBackends() int {
	// Check if client is closed before processing
	c.mu.Lock()
	if c.mu.closed {
		c.mu.Unlock()
		return 0
	}

	var idleBackends []Backend
	for k, backends := range c.mu.backends {
		var newBackends []Backend
		for _, b := range backends {
			if !b.Locked() &&
				time.Since(b.LastActiveTime()) > c.options.maxIdleDuration {
				idleBackends = append(idleBackends, b)
				continue
			}
			newBackends = append(newBackends, b)
		}
		c.mu.backends[k] = newBackends
	}
	c.updatePoolSizeMetricsLocked()
	c.mu.Unlock()

	for _, b := range idleBackends {
		b.Close()
	}
	return len(idleBackends)
}

func (c *client) createBackendLocked(backend string) (Backend, error) {
	if !c.canCreateLocked(backend) {
		return nil, moerr.NewBackendClosedNoCtx()
	}

	b, err := c.doCreate(backend)
	if err != nil {
		return nil, err
	}
	c.mu.backends[backend] = append(c.mu.backends[backend], b)
	if _, ok := c.mu.ops[backend]; !ok {
		c.mu.ops[backend] = &op{}
	}
	return b, nil
}

func (c *client) doCreate(backend string) (Backend, error) {
	b, err := c.factory.Create(backend, WithBackendMetrics(c.metrics))
	if err != nil {
		c.logger.Error("create backend failed",
			zap.String("backend", backend),
			zap.Error(err))
		return nil, err
	}
	return b, nil
}

func (c *client) canCreateLocked(backend string) bool {
	return len(c.mu.backends[backend]) < c.options.maxBackendsPerHost
}

func (c *client) updatePoolSizeMetricsLocked() {
	n := 0
	for _, backends := range c.mu.backends {
		n += len(backends)
	}
	c.metrics.poolSizeGauge.Set(float64(n))
}

func isErrBackendCreating(err error) bool {
	if err == nil {
		return false
	}
	// Compare directly since ErrBackendCreating is now a moerr error
	return err == ErrBackendCreating || errors.Is(err, ErrBackendCreating)
}

// isAutoCreateWaitError checks if the error indicates we should wait for backend creation.
// State mapping:
//   - ErrBackendCreating: Backend is being created asynchronously, should wait
//   - ErrNoAvailableBackend: Pool is empty and cannot create (at capacity), should wait
//   - ErrBackendClosed: Backend unavailable or wait timeout exceeded, should NOT wait
func isAutoCreateWaitError(err error) bool {
	return isErrBackendCreating(err) ||
		moerr.IsMoErrCode(err, moerr.ErrNoAvailableBackend)
}

// handleAutoCreateWait implements bounded wait logic for backend creation.
// Returns:
//   - true: should continue waiting/retrying
//   - false: should stop (timeout exceeded or context cancelled)
//   - error: ErrBackendCreateTimeout if timeout exceeded, ctx.Err() if context cancelled
func (c *client) handleAutoCreateWait(ctx context.Context, backend string, creationStart *time.Time, retryCount int) (bool, error) {
	// Check context first
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	// Initialize creation start time
	if creationStart.IsZero() {
		*creationStart = time.Now()
	}

	// Check bounded wait timeout
	if timeout := c.options.autoCreateWaitTimeout; timeout > 0 {
		elapsed := time.Since(*creationStart)
		if elapsed >= timeout {
			c.logger.Warn("auto-create backend timed out",
				zap.String("backend", backend),
				zap.Duration("waited", elapsed),
				zap.Duration("timeout", timeout))
			c.metrics.autoCreateTimeoutCounter.Inc()
			return false, ErrBackendCreateTimeout
		}
	}

	// Log creation wait progress (sparse logging)
	if retryCount == 1 || retryCount%10 == 0 {
		c.logger.Debug("waiting for backend creation",
			zap.String("backend", backend),
			zap.Int("retry", retryCount),
			zap.Duration("waited", time.Since(*creationStart)))
	}

	return true, nil
}

type op struct {
	seq uint64
}

func (o *op) next() uint64 {
	return atomic.AddUint64(&o.seq, 1)
}
