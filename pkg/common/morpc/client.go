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

const (
	defaultAutoCreateWaitTimeout = 0 // 0 means wait until context deadline (legacy behavior)
	maxConcurrentBackendCleanups = 64
)

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
	closedC     chan struct{}

	// backendCleanup owns asynchronous closes after a backend has been detached
	// from pool admission. Add is serialized by mu and is forbidden after
	// mu.closing becomes true, so Close can safely wait after sealing admission.
	backendCleanup      sync.WaitGroup
	backendCleanupSlots chan struct{}

	mu struct {
		sync.Mutex
		closing  bool // true when Close() is called but not yet completed
		closed   bool // true when Close() is completed
		backends map[string][]Backend
		ops      map[string]*op
		// backendGeneration invalidates create requests captured before a
		// targeted reset. Pointer identity avoids ABA when an entry is evicted
		// and later recreated for the same remote.
		backendGeneration map[string]*backendGeneration
		// creating deduplicates factory I/O per remote generation without
		// holding mu across DNS or network connection work. Its completion
		// channel lets top-level operations wait for an actual state change
		// instead of polling with exponential backoff.
		creating map[string]*backendCreateState
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
		closedC:     make(chan struct{}),
		backendCleanupSlots: make(
			chan struct{},
			maxConcurrentBackendCleanups,
		),
	}
	c.mu.backends = make(map[string][]Backend)
	c.mu.ops = make(map[string]*op)
	c.mu.backendGeneration = make(map[string]*backendGeneration)
	c.mu.creating = make(map[string]*backendCreateState)

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

	// Pin the breaker incarnation for this request. A targeted backend reset
	// detaches it, so a late result cannot affect the replacement generation.
	breaker := c.circuitBreakers.newHandle(backend)
	if !breaker.Allow() {
		return nil, breaker.Error()
	}

	policy := c.options.retryPolicy
	var backoff time.Duration
	retryCount := 0
	var creationStart time.Time

	for {
		// Check circuit breaker before each retry attempt
		if !breaker.Allow() {
			// Don't record failure - circuit is already open
			return nil, breaker.Error()
		}

		b, backendCreate, err := c.getBackendForOperation(backend, false)
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
					breaker.RecordFailure()
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
					breaker.RecordFailure()
					return nil, err
				}

				// Calculate next backoff with jitter
				backoff = policy.nextBackoff(backoff)
				if waitErr := c.waitBackendChange(
					ctx,
					backend,
					creationStart,
					backendCreate,
					backoff,
				); waitErr != nil {
					if !errors.Is(waitErr, context.Canceled) &&
						!errors.Is(waitErr, context.DeadlineExceeded) {
						breaker.RecordFailure()
					}
					return nil, waitErr
				}
				continue
			}

			// Don't count client-level errors (like ErrClientClosed) as circuit breaker failures
			// Only count backend-related errors
			if !moerr.IsMoErrCode(err, moerr.ErrClientClosed) {
				breaker.RecordFailure()
			}
			return nil, err
		}

		f, err := b.Send(ctx, request)
		if isBackendClosedError(err) {
			c.retireBackend(backend, b)
			breaker.RecordFailure()
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
			if !breaker.Allow() {
				return nil, breaker.Error()
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
			breaker.RecordSuccess()
		} else {
			breaker.RecordFailure()
		}
		return f, err
	}
}

func (c *client) NewStream(ctx context.Context, backend string, lock bool) (Stream, error) {
	if ctx == nil {
		panic("client NewStream nil context")
	}

	breaker := c.circuitBreakers.newHandle(backend)
	if !breaker.Allow() {
		return nil, breaker.Error()
	}

	policy := c.options.retryPolicy
	var backoff time.Duration
	retryCount := 0
	var creationStart time.Time

	for {
		// Check circuit breaker before each retry attempt
		if !breaker.Allow() {
			// Don't record failure - circuit is already open
			return nil, breaker.Error()
		}

		// Check context before attempting
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		b, backendCreate, err := c.getBackendForOperation(backend, lock)
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
					breaker.RecordFailure()
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
					breaker.RecordFailure()
					return nil, err
				}

				// Calculate next backoff with jitter
				backoff = policy.nextBackoff(backoff)
				if waitErr := c.waitBackendChange(
					ctx,
					backend,
					creationStart,
					backendCreate,
					backoff,
				); waitErr != nil {
					if !errors.Is(waitErr, context.Canceled) &&
						!errors.Is(waitErr, context.DeadlineExceeded) {
						breaker.RecordFailure()
					}
					return nil, waitErr
				}
				continue
			}

			// Don't count client-level errors (like ErrClientClosed) as circuit breaker failures
			if !moerr.IsMoErrCode(err, moerr.ErrClientClosed) {
				breaker.RecordFailure()
			}
			return nil, err
		}

		st, err := b.NewStream(lock)
		if err != nil && lock {
			// getBackendForOperation acquired this lock on behalf of the stream.
			// Ownership transfers only after NewStream succeeds; on every error the
			// client remains the owner and must release it.
			b.Unlock()
		}
		if isBackendClosedError(err) {
			c.retireBackend(backend, b)
			breaker.RecordFailure()
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
			if !breaker.Allow() {
				return nil, breaker.Error()
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
			breaker.RecordSuccess()
		} else {
			breaker.RecordFailure()
		}
		return st, err
	}
}

func (c *client) Ping(ctx context.Context, backend string) error {
	if ctx == nil {
		panic("client Ping nil context")
	}

	breaker := c.circuitBreakers.newHandle(backend)
	if !breaker.Allow() {
		return breaker.Error()
	}

	policy := c.options.retryPolicy
	var backoff time.Duration
	retryCount := 0
	var creationStart time.Time

	for {
		b, backendCreate, err := c.getBackendForOperation(backend, false)
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
					breaker.RecordFailure()
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
					breaker.RecordFailure()
					return err
				}

				// Calculate next backoff with jitter
				backoff = policy.nextBackoff(backoff)
				if waitErr := c.waitBackendChange(
					ctx,
					backend,
					creationStart,
					backendCreate,
					backoff,
				); waitErr != nil {
					if !errors.Is(waitErr, context.Canceled) &&
						!errors.Is(waitErr, context.DeadlineExceeded) {
						breaker.RecordFailure()
					}
					return waitErr
				}
				continue
			}

			// Don't count client-level errors (like ErrClientClosed) as circuit breaker failures
			if !moerr.IsMoErrCode(err, moerr.ErrClientClosed) {
				breaker.RecordFailure()
			}
			return err
		}

		f, err := b.SendInternal(ctx, &flagOnlyMessage{flag: flagPing})
		if err != nil {
			if isBackendClosedError(err) {
				c.retireBackend(backend, b)
				breaker.RecordFailure()
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
				if !breaker.Allow() {
					return breaker.Error()
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
			breaker.RecordFailure()
			return err
		}
		_, err = f.Get()
		f.Close()
		if err == nil {
			breaker.RecordSuccess()
		} else {
			breaker.RecordFailure()
		}
		return err
	}
}

func (c *client) Close() error {
	c.mu.Lock()
	if c.mu.closed {
		c.mu.Unlock()
		return nil
	}
	if c.mu.closing {
		closedC := c.closedC
		c.mu.Unlock()
		<-closedC
		return nil
	}
	// Set closing state first
	c.mu.closing = true
	for remote := range c.mu.creating {
		c.invalidateBackendCreateLocked(remote)
	}
	backends := c.detachAllBackendsLocked()
	c.updatePoolSizeMetricsLocked()
	c.mu.Unlock()

	// Explicit client shutdown remains synchronous. Backends already retired by
	// an operation are absent from this snapshot and are joined below instead of
	// being closed twice.
	for _, backend := range backends {
		backend.Close()
	}
	c.backendCleanup.Wait()

	// Unregister from global GC manager
	globalClientGC.unregister(c)

	// Update active client count (only the Close owner reaches here).
	activeGauge := v2.NewRPCClientActiveGaugeByName(c.name)
	activeGauge.Dec()

	c.stopper.Stop()
	close(c.createC)

	c.mu.Lock()
	c.mu.closed = true
	close(c.closedC)
	c.mu.Unlock()
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

// CloseBackendFor synchronously detaches one remote and invalidates any
// asynchronous create requests queued before this call.
func (c *client) CloseBackendFor(remote string) error {
	c.mu.Lock()
	backends := c.mu.backends[remote]
	delete(c.mu.backendGeneration, remote)
	c.invalidateBackendCreateLocked(remote)
	delete(c.mu.backends, remote)
	delete(c.mu.ops, remote)
	c.updatePoolSizeMetricsLocked()
	// Detach breaker state in the same reset critical section as the backend
	// generation. A request either captures the complete old incarnation or a
	// complete new one, never a new backend generation with the old breaker.
	c.circuitBreakers.RemoveBreaker(remote)
	c.mu.Unlock()

	// Close outside c.mu: backend shutdown can wait for worker goroutines, and
	// no new caller should be blocked from creating the replacement meanwhile.
	for _, backend := range backends {
		backend.Close()
	}
	return nil
}

func (c *client) getBackend(backend string, lock bool) (Backend, error) {
	b, _, err := c.getBackendForOperation(backend, lock)
	return b, err
}

// getBackendForOperation returns a completion signal when the lookup is
// waiting on an asynchronous backend create. The signal is captured from the
// same remote generation that produced ErrBackendCreating, so reset and create
// completion can wake callers without polling or stale-generation confusion.
func (c *client) getBackendForOperation(
	backend string,
	lock bool,
) (Backend, *backendCreateState, error) {
	// Fast-fail: check circuit breaker before acquiring lock
	// This prevents blocking on lock acquisition for known-bad backends
	if !c.circuitBreakers.Allow(backend) {
		return nil, nil, c.circuitBreakers.GetError(backend)
	}

	c.mu.Lock()
	// Preserve the healthy-backend fast path. Only compact terminal capacity
	// after selection misses; doing a full slice rewrite before every Send/Ping
	// would add avoidable work to the per-operation hot path.
	b, err := c.getBackendLockedWithCreate(backend, lock, false)
	inactive := 0
	if b == nil && (err == nil || isBackendClosedError(err)) {
		inactive = c.detachInactiveForCleanupLocked(backend)
		if inactive > 0 {
			// Re-evaluate selection and capacity in the same client-state snapshot
			// after terminal entries have been removed.
			b, err = c.getBackendLockedWithCreate(backend, lock, false)
		}
	}
	// Cleanup ownership was transferred before each backend was detached. The
	// foreground operation only releases the state snapshot here; it never waits
	// for backend shutdown.
	unlock := func() {
		c.mu.Unlock()
	}
	// Selection and create admission must use the same client-state snapshot.
	// Otherwise a backend can be published after selection returns nil but
	// before creation is queued, causing a stale lookup to overgrow the pool.
	poolSize := len(c.mu.backends[backend])
	if err != nil {
		unlock()
		return nil, nil, err
	}
	if b != nil {
		unlock()
		return b, nil, nil
	}

	// No backend available in pool
	canCreate := c.canCreateLocked(backend)
	enableAutoCreate := c.options.enableAutoCreate
	hasBackends := poolSize > 0
	var generation *backendGeneration
	if canCreate && enableAutoCreate {
		generation = c.backendGenerationLocked(backend)
	}

	// If pool has backends but all are busy, wait for one to become available
	// This applies regardless of enableAutoCreate setting
	if hasBackends && !canCreate {
		c.metrics.backendUnavailableCounter.Inc()
		unlock()
		return nil, nil, ErrBackendCreating // Triggers wait/retry logic
	}

	// Strictly gate creation on enableAutoCreate flag
	if !enableAutoCreate {
		// No backends exist and auto-create is disabled - fail fast
		unlock()
		return nil, nil, moerr.NewNoAvailableBackendNoCtx()
	}

	if canCreate {
		// Admit creation while the lookup snapshot is still protected by c.mu.
		// The non-blocking queue send is safe under the lock; factory I/O remains
		// outside the lock in both the worker and synchronous-fallback paths.
		backendCreate, queued := globalClientGC.triggerCreateAtGenerationLocked(
			c,
			backend,
			generation,
		)
		if queued {
			unlock()
			return nil, backendCreate, ErrBackendCreating
		}

		// The async queue is full. Claim the same observed demand before
		// releasing c.mu, then perform only the factory I/O outside the lock.
		// No successful publication can slip between observation and claim.
		backendCreate = newBackendCreateState(generation)
		c.mu.creating[backend] = backendCreate
		unlock()
		b, err = c.createBackendForClaimedGeneration(backend, lock, generation)
		return b, nil, err
	}

	// Pool is empty and cannot create - return ErrNoAvailableBackend to trigger wait logic
	unlock()
	return nil, nil, moerr.NewNoAvailableBackendNoCtx()
}

func (c *client) getBackendLocked(backend string, lock bool) (Backend, error) {
	return c.getBackendLockedWithCreate(backend, lock, true)
}

func (c *client) getBackendLockedWithCreate(
	backend string,
	lock bool,
	create bool,
) (Backend, error) {
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
		// Only try to create when no available backend was found; avoid unbounded growth when backends are locked.
		if create && b == nil {
			c.maybeCreateLocked(backend)
		}
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

	_, ok := globalClientGC.triggerCreateAtGenerationLocked(
		c,
		backend,
		c.backendGenerationLocked(backend),
	)
	return ok
}

func (c *client) createBackendWithBookkeeping(backend string, lock bool) (Backend, error) {
	c.mu.Lock()
	generation := c.backendGenerationLocked(backend)
	c.mu.Unlock()
	return c.createBackendWithBookkeepingAtGeneration(backend, lock, generation)
}

func (c *client) createBackendWithBookkeepingAtGeneration(
	backend string,
	lock bool,
	generation *backendGeneration,
) (Backend, error) {
	if generation == nil {
		return nil, moerr.NewBackendClosedNoCtx()
	}
	c.mu.Lock()
	if c.mu.closing || c.mu.closed {
		c.mu.Unlock()
		return nil, moerr.NewClientClosedNoCtx()
	}
	if c.mu.backendGeneration[backend] != generation {
		c.mu.Unlock()
		return nil, moerr.NewBackendClosedNoCtx()
	}
	if c.mu.creating == nil {
		c.mu.creating = make(map[string]*backendCreateState)
	}
	if c.mu.creating[backend] != nil {
		c.mu.Unlock()
		return nil, ErrBackendCreating
	}
	if !c.canCreateLocked(backend) {
		c.mu.Unlock()
		return nil, moerr.NewBackendClosedNoCtx()
	}
	c.mu.creating[backend] = newBackendCreateState(generation)
	c.mu.Unlock()
	return c.createBackendForClaimedGeneration(backend, lock, generation)
}

// createBackendForClaimedGeneration performs factory I/O for a create request
// that already owns the queued/in-flight slot for this remote generation.
func (c *client) createBackendForClaimedGeneration(
	backend string,
	lock bool,
	generation *backendGeneration,
) (Backend, error) {
	if generation == nil {
		return nil, moerr.NewBackendClosedNoCtx()
	}
	c.mu.Lock()
	if c.mu.closing || c.mu.closed {
		c.releaseBackendCreateLocked(backend, generation)
		c.mu.Unlock()
		return nil, moerr.NewClientClosedNoCtx()
	}
	if c.mu.backendGeneration[backend] != generation ||
		!c.hasBackendCreateLocked(backend, generation) {
		c.mu.Unlock()
		return nil, moerr.NewBackendClosedNoCtx()
	}
	if !c.canCreateLocked(backend) {
		c.releaseBackendCreateLocked(backend, generation)
		c.mu.Unlock()
		return nil, moerr.NewBackendClosedNoCtx()
	}
	c.mu.Unlock()

	// Create backend using factory with metrics (same as doCreate) without holding the lock.
	b, err := c.doCreate(backend)

	// Re-acquire lock to add to pool, validating limits again.
	c.mu.Lock()
	claimActive := c.hasBackendCreateLocked(backend, generation)
	if err != nil {
		c.failBackendCreateLocked(backend, generation)
		c.mu.Unlock()
		return nil, err
	}

	clientClosed := c.mu.closing || c.mu.closed
	if !claimActive || clientClosed {
		c.releaseBackendCreateLocked(backend, generation)
		c.mu.Unlock()
		b.Close()
		if clientClosed {
			return nil, moerr.NewClientClosedNoCtx()
		}
		return nil, moerr.NewBackendClosedNoCtx()
	}
	if c.mu.backendGeneration[backend] != generation {
		c.releaseBackendCreateLocked(backend, generation)
		c.mu.Unlock()
		b.Close()
		return nil, moerr.NewBackendClosedNoCtx()
	}
	if !c.canCreateLocked(backend) {
		// Another goroutine may have filled the pool while we were creating.
		c.releaseBackendCreateLocked(backend, generation)
		c.mu.Unlock()
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
	// Publish the backend before waking waiters. They can observe the closed
	// completion channel immediately, but must acquire c.mu after this unlock
	// before selecting the newly available backend.
	c.releaseBackendCreateLocked(backend, generation)
	c.mu.Unlock()

	return b, nil
}

func (c *client) releaseBackendCreate(backend string, generation *backendGeneration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.releaseBackendCreateLocked(backend, generation)
}

const maxBackendGenerationEntries = 4096

type backendGeneration struct {
	// Keep the allocation non-zero-sized so distinct generations always have
	// distinct pointer identities.
	_ byte
}

type backendCreateState struct {
	generation *backendGeneration
	done       chan struct{}
	// failed is published before done is closed. Waiters retain retry backoff
	// after factory failures, while successful creates and invalidations wake
	// immediately to re-evaluate client state.
	failed bool
}

func newBackendCreateState(generation *backendGeneration) *backendCreateState {
	return &backendCreateState{
		generation: generation,
		done:       make(chan struct{}),
	}
}

func (c *client) hasBackendCreateLocked(
	backend string,
	generation *backendGeneration,
) bool {
	state := c.mu.creating[backend]
	return state != nil && state.generation == generation
}

func (c *client) releaseBackendCreateLocked(
	backend string,
	generation *backendGeneration,
) {
	state := c.mu.creating[backend]
	if state == nil || state.generation != generation {
		return
	}
	delete(c.mu.creating, backend)
	close(state.done)
}

func (c *client) failBackendCreateLocked(
	backend string,
	generation *backendGeneration,
) {
	state := c.mu.creating[backend]
	if state == nil || state.generation != generation {
		return
	}
	state.failed = true
	delete(c.mu.creating, backend)
	close(state.done)
}

func (c *client) invalidateBackendCreateLocked(backend string) {
	state := c.mu.creating[backend]
	if state == nil {
		return
	}
	delete(c.mu.creating, backend)
	close(state.done)
}

func (c *client) backendGenerationLocked(remote string) *backendGeneration {
	if c.mu.backendGeneration == nil {
		c.mu.backendGeneration = make(map[string]*backendGeneration)
	}
	if generation := c.mu.backendGeneration[remote]; generation != nil {
		return generation
	}
	if len(c.mu.backendGeneration) >= maxBackendGenerationEntries {
		// Eviction invalidates outstanding creates for the victim. A later request
		// allocates a distinct token, so an evicted stale request cannot be
		// re-admitted even when the same address returns (no ABA).
		for victim := range c.mu.backendGeneration {
			delete(c.mu.backendGeneration, victim)
			c.invalidateBackendCreateLocked(victim)
			break
		}
	}
	generation := &backendGeneration{}
	c.mu.backendGeneration[remote] = generation
	return generation
}

func (c *client) triggerGCInactive(remote string) {
	globalClientGC.triggerGCInactive(c, remote)
	c.logger.Debug("try to remove all inactived backends",
		zap.String("remote", remote))
}

func isBackendClosedError(err error) bool {
	return err != nil &&
		(errors.Is(err, backendClosed) || moerr.IsMoErrCode(err, moerr.ErrBackendClosed))
}

// retireBackend makes an operation-level closed result visible to pool
// selection immediately. Background inactive GC remains a safety net, not a
// prerequisite for foreground recovery.
func (c *client) retireBackend(remote string, backend Backend) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mu.closing || c.mu.closed {
		// Close owns the pool snapshot once closing is published.
		return
	}
	if c.detachBackendForCleanupLocked(remote, backend) {
		c.updatePoolSizeMetricsLocked()
	}
}

func (c *client) doRemoveInactive(remote string) {
	c.mu.Lock()

	// Check if client is closed
	if c.mu.closing || c.mu.closed {
		c.mu.Unlock()
		return
	}

	_, ok := c.mu.backends[remote]
	if !ok {
		c.mu.Unlock()
		return
	}

	c.detachInactiveForCleanupLocked(remote)

	c.updatePoolSizeMetricsLocked()
	c.mu.Unlock()
}

// detachBackendForCleanupLocked removes every pool slot that refers to backend
// only after cleanup ownership is admitted. If cleanup is saturated, keeping
// the backend in the pool preserves a hard resource bound by making it continue
// to count against replacement capacity.
func (c *client) detachBackendForCleanupLocked(remote string, backend Backend) bool {
	backends, ok := c.mu.backends[remote]
	if !ok {
		return false
	}

	found := false
	for _, candidate := range backends {
		if candidate == backend {
			found = true
			break
		}
	}
	if !found || !c.tryStartBackendCleanupLocked(backend) {
		return false
	}

	active := backends[:0]
	for _, candidate := range backends {
		if candidate != backend {
			active = append(active, candidate)
		}
	}
	clear(backends[len(active):])
	c.mu.backends[remote] = active
	return true
}

func (c *client) detachAllBackendsLocked() []Backend {
	var detached []Backend
	for remote, backends := range c.mu.backends {
		detached = append(detached, backends...)
		clear(backends)
		delete(c.mu.backends, remote)
	}
	return detached
}

// tryStartBackendCleanupLocked transfers cleanup ownership to the client. The
// fixed-size slot channel bounds stuck cleanup goroutines. The caller must keep
// a backend in the pool when admission fails so it still consumes capacity.
func (c *client) tryStartBackendCleanupLocked(backend Backend) bool {
	select {
	case c.backendCleanupSlots <- struct{}{}:
		c.backendCleanup.Add(1)
		go func() {
			defer c.backendCleanup.Done()
			defer func() { <-c.backendCleanupSlots }()
			backend.Close()
		}()
		return true
	default:
		return false
	}
}

// detachInactiveForCleanupLocked removes terminal backends from the pool's
// capacity model only when their bounded cleanup has been admitted.
func (c *client) detachInactiveForCleanupLocked(remote string) int {
	backends, ok := c.mu.backends[remote]
	if !ok {
		return 0
	}

	detached := 0
	active := backends[:0]
	for _, backend := range backends {
		if backend.LastActiveTime() == (time.Time{}) &&
			c.tryStartBackendCleanupLocked(backend) {
			detached++
			continue
		}
		active = append(active, backend)
	}
	// A shorter slice still keeps its entire pointer-containing backing array
	// reachable. Clear the compacted tail so removed backends (and their stream
	// pools/connections) can be collected while this remote remains in the map.
	clear(backends[len(active):])
	c.mu.backends[remote] = active
	return detached
}

// doRemoveInactiveAll removes all explicitly closed (inactive) backends for every remote.
// Used by the periodic GC to clean up closed backends within ~10s without waiting for
// the idle timeout (e.g. 1 minute). Safe to call on closed client (no-op).
func (c *client) doRemoveInactiveAll() {
	c.mu.Lock()

	if c.mu.closing || c.mu.closed {
		c.mu.Unlock()
		return
	}

	for remote := range c.mu.backends {
		c.detachInactiveForCleanupLocked(remote)
	}
	c.updatePoolSizeMetricsLocked()
	c.mu.Unlock()
}

func (c *client) closeIdleBackends() int {
	// Check if client is closed before processing
	c.mu.Lock()
	if c.mu.closing || c.mu.closed {
		c.mu.Unlock()
		return 0
	}

	closed := 0
	for k, backends := range c.mu.backends {
		newBackends := backends[:0]
		for _, b := range backends {
			if !b.Locked() &&
				time.Since(b.LastActiveTime()) > c.options.maxIdleDuration &&
				c.tryStartBackendCleanupLocked(b) {
				closed++
				continue
			}
			newBackends = append(newBackends, b)
		}
		clear(backends[len(newBackends):])
		c.mu.backends[k] = newBackends
	}
	c.updatePoolSizeMetricsLocked()
	c.mu.Unlock()
	return closed
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
			return false, c.autoCreateTimeoutError(backend, elapsed, timeout)
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

// waitBackendChange waits on a real asynchronous-create state transition when
// one exists. Backoff remains the fallback for states that have no completion
// owner (for example, a fully busy pool). This preserves retry throttling while
// removing the latency amplification where a backend becomes ready near the
// start of a long exponential-backoff interval.
func (c *client) waitBackendChange(
	ctx context.Context,
	backend string,
	creationStart time.Time,
	backendCreate *backendCreateState,
	backoff time.Duration,
) error {
	if backendCreate != nil {
		if err := c.waitBackendCreateCompletion(
			ctx,
			backend,
			creationStart,
			backendCreate,
		); err != nil {
			return err
		}
		if !backendCreate.failed {
			return nil
		}
	}

	return c.waitBackendRetryBackoff(ctx, backend, creationStart, backoff)
}

func (c *client) waitBackendCreateCompletion(
	ctx context.Context,
	backend string,
	creationStart time.Time,
	backendCreate *backendCreateState,
) error {
	select {
	case <-backendCreate.done:
		return nil
	default:
	}

	timeout := c.options.autoCreateWaitTimeout
	if timeout <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-backendCreate.done:
			return nil
		}
	}

	remaining := timeout - time.Since(creationStart)
	if remaining <= 0 {
		return c.autoCreateTimeoutError(backend, time.Since(creationStart), timeout)
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-backendCreate.done:
		return nil
	case <-timer.C:
		return c.autoCreateTimeoutError(backend, time.Since(creationStart), timeout)
	}
}

func (c *client) waitBackendRetryBackoff(
	ctx context.Context,
	backend string,
	creationStart time.Time,
	backoff time.Duration,
) error {
	if backoff <= 0 {
		return nil
	}

	wait := backoff
	timedByCreateTimeout := false
	if timeout := c.options.autoCreateWaitTimeout; timeout > 0 {
		remaining := timeout - time.Since(creationStart)
		if remaining <= 0 {
			return c.autoCreateTimeoutError(backend, time.Since(creationStart), timeout)
		}
		if remaining < wait {
			wait = remaining
			timedByCreateTimeout = true
		}
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		if timedByCreateTimeout {
			timeout := c.options.autoCreateWaitTimeout
			return c.autoCreateTimeoutError(backend, time.Since(creationStart), timeout)
		}
		return nil
	}
}

func (c *client) autoCreateTimeoutError(
	backend string,
	waited time.Duration,
	timeout time.Duration,
) error {
	c.logger.Warn("auto-create backend timed out",
		zap.String("backend", backend),
		zap.Duration("waited", waited),
		zap.Duration("timeout", timeout))
	c.metrics.autoCreateTimeoutCounter.Inc()
	return ErrBackendCreateTimeout
}

type op struct {
	seq uint64
}

func (o *op) next() uint64 {
	return atomic.AddUint64(&o.seq, 1)
}
