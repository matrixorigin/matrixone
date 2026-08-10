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
//	│ Create queue full            │ ErrBackendCreateQueueFull  │ Yes (caller)    │
//	│ Create queue wait exceeded   │ ErrBackendCreateQueueTimeout│ Yes (caller)   │
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
//   - autoCreateWaitTimeout > 0: For asynchronous creation, start the bounded
//     wait when backend factory work starts. Initial admission queue time is
//     excluded; after the first attempt starts, retries share the same budget.
//     Capacity waits with no asynchronous completion owner start immediately.
//   - autoCreateQueueWaitTimeout = 0 (default): Queue admission is bounded only
//     by the caller context.
//   - autoCreateQueueWaitTimeout > 0: Bound each queued factory admission
//     independently. Queue congestion is a local transient condition and does
//     not count as a peer or circuit-breaker failure.
//
// Example: a service may set bounded factory and queue waits for fast failure
// detection in orphan transaction cleanup.
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
	// This typically means backend creation or recovery is too slow.
	// This is a boundary condition error (NoCtx + Counter for monitoring).
	// Uses ErrBackendClosed code for compatibility with existing error handling.
	ErrBackendCreateTimeout = moerr.NewBackendClosedNoCtx()

	// ErrBackendCreateQueueFull indicates that the process-wide backend-create
	// queue cannot admit more work. It is a local transient overload signal and
	// must not be attributed to the target peer.
	ErrBackendCreateQueueFull = moerr.NewInternalErrorNoCtx(
		"morpc backend create queue is full",
	)

	// ErrBackendCreateQueueTimeout indicates that a queued create did not reach
	// a factory worker within the configured admission budget. It is a local
	// transient overload signal and must not trip the peer circuit breaker.
	ErrBackendCreateQueueTimeout = moerr.NewInternalErrorNoCtx(
		"morpc backend create queue wait timed out",
	)

	// ErrClientClosing indicates that the client is in the process of closing.
	// New requests should fail fast rather than waiting for backend creation.
	// This is a high-frequency expected error during shutdown (NoCtx to avoid log spam).
	ErrClientClosing = moerr.NewClientClosedNoCtx()
)

const (
	defaultAutoCreateWaitTimeout      = 0 // 0 means wait until context deadline (legacy behavior)
	defaultAutoCreateQueueWaitTimeout = 0 // 0 means wait until context deadline (legacy behavior)
	maxConcurrentBackendCleanups      = 64
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
		// Keep the general client logger quiet for compatibility, but retain the
		// caller's logger for explicitly rate-limited lifecycle diagnostics.
		c.logger = logutil.GetPanicLoggerWithLevel(zap.FatalLevel)
		c.diagnosticLogger = logger
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

// WithClientAutoCreateWaitTimeout sets the bounded wait used by
// Send/NewStream/Ping. For asynchronous creation, initial admission queue time
// is excluded and retries after the first factory attempt share the same
// budget. Capacity waits with no asynchronous completion owner start
// immediately. Zero keeps legacy behavior (wait until context deadline).
func WithClientAutoCreateWaitTimeout(timeout time.Duration) ClientOption {
	return func(c *client) {
		c.options.autoCreateWaitTimeout = timeout
		c.options.autoCreateWaitTimeoutSet = true
	}
}

// WithClientAutoCreateQueueWaitTimeout bounds how long an asynchronous backend
// create may wait for a process-wide factory worker. The queue budget is
// independent from WithClientAutoCreateWaitTimeout, which starts only after
// factory admission. Zero preserves the legacy behavior of relying on the
// caller context for queue admission.
func WithClientAutoCreateQueueWaitTimeout(timeout time.Duration) ClientOption {
	return func(c *client) {
		c.options.autoCreateQueueWaitTimeout = timeout
		c.options.autoCreateQueueWaitTimeoutSet = true
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

	// backendCleanup owns closes after a backend has been detached from pool
	// admission, including asynchronous retirement and synchronous targeted
	// reset. Add is serialized by mu and is forbidden after mu.closing becomes
	// true, so Close can safely wait after sealing admission.
	backendCleanup      sync.WaitGroup
	backendCleanupSlots chan struct{}
	// backendCreate covers factory I/O that passed the closing/generation gate.
	// Add is performed under mu before the I/O starts, so Close can seal new
	// admissions and then safely join every already-started create.
	backendCreate sync.WaitGroup

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
	// gcManager is pinned at registration. Global reconfiguration must never
	// make this client unregister from, or enqueue work on, another manager
	// incarnation.
	gcManager     *clientGCManager
	countedActive bool

	options struct {
		maxBackendsPerHost            int
		maxIdleDuration               time.Duration
		maxIdleDurationSet            bool // true if user explicitly set maxIdleDuration (even to 0)
		initBackends                  []string
		initBackendCounts             []int
		enableAutoCreate              bool
		enableAutoCreateSet           bool // true if user explicitly set enableAutoCreate
		retryPolicy                   RetryPolicy
		circuitBreakerConfig          CircuitBreakerConfig
		autoCreateWaitTimeout         time.Duration
		autoCreateWaitTimeoutSet      bool
		autoCreateQueueWaitTimeout    time.Duration
		autoCreateQueueWaitTimeoutSet bool
	}

	// Keep failure-only diagnostics after the pre-existing hot client state so
	// adding observability does not shift the mutex/pool fields used by every
	// Send.
	diagnosticLogger        *zap.Logger
	autoCreateTimeoutLogger *logutil.RateLimitedLogger
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

	// Pin and register with one manager while replacement is excluded. Close
	// and every trigger path below use this exact incarnation.
	globalClientGCMu.RLock()
	c.gcManager = globalClientGC
	c.gcManager.register(c)
	globalClientGCMu.RUnlock()

	// Update active client count (only after successful creation)
	activeGauge := v2.NewRPCClientActiveGaugeByName(name)
	activeGauge.Inc()
	c.countedActive = true

	return c, nil
}

func (c *client) adjust() {
	c.logger = logutil.Adjust(c.logger).Named(c.name)
	c.diagnosticLogger = logutil.Adjust(c.diagnosticLogger)
	// This logger has exactly one static event population. Retain only one keyed
	// limiter state so a future accidental dynamic key cannot grow per-client
	// diagnostic memory.
	c.autoCreateTimeoutLogger = logutil.NewRateLimitedLoggerWithConfig(
		c.diagnosticLogger,
		logutil.RateLimitedLoggerConfig{MaxKeys: 1},
	)
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
	if !c.options.autoCreateQueueWaitTimeoutSet &&
		c.options.autoCreateQueueWaitTimeout == 0 {
		c.options.autoCreateQueueWaitTimeout = defaultAutoCreateQueueWaitTimeout
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
	var permit circuitBreakerPermit
	defer func() {
		permit.Release()
	}()

	policy := c.options.retryPolicy
	var backoff time.Duration
	retryCount := 0
	var creationStart time.Time

	for {
		var admitErr error
		permit, admitErr = breaker.Admit()
		if admitErr != nil {
			return nil, admitErr
		}

		b, backendCreate, err := c.getBackendForOperation(backend, false)
		if err != nil {
			// A full process-local create queue is backpressure, not evidence
			// that this peer failed. Return it to the caller without poisoning
			// the peer's breaker generation.
			if isBackendCreateQueueCongestion(err) {
				return nil, err
			}

			// Wait for backend if:
			// 1. Auto-create enabled and waiting for creation, OR
			// 2. ErrBackendCreating (pool has backends but all busy - wait regardless of auto-create)
			waitingForCreate := (c.options.enableAutoCreate && isAutoCreateWaitError(err)) ||
				isErrBackendCreating(err)

			// Handle backend creation-in-progress with bounded wait
			if waitingForCreate {
				shouldContinue, waitErr := c.handleAutoCreateWait(
					ctx,
					backend,
					&creationStart,
					backendCreate,
					retryCount,
				)
				if !shouldContinue {
					// Record circuit breaker failure on timeout
					if !errors.Is(waitErr, context.Canceled) &&
						!errors.Is(waitErr, context.DeadlineExceeded) {
						permit.RecordFailure()
					}
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
					permit.RecordFailure()
					return nil, err
				}

				// Calculate next backoff with jitter
				backoff = policy.nextBackoff(backoff)
				if waitErr := c.waitBackendChange(
					ctx,
					backend,
					&creationStart,
					backendCreate,
					backoff,
				); waitErr != nil {
					if !errors.Is(waitErr, context.Canceled) &&
						!errors.Is(waitErr, context.DeadlineExceeded) &&
						!isBackendCreateQueueCongestion(waitErr) {
						permit.RecordFailure()
					}
					return nil, waitErr
				}
				permit.Release()
				continue
			}

			// Don't count client-level errors (like ErrClientClosed) as circuit breaker failures
			// Only count backend-related errors
			if !moerr.IsMoErrCode(err, moerr.ErrClientClosed) {
				permit.RecordFailure()
			}
			return nil, err
		}

		f, err := b.Send(ctx, request)
		if errors.Is(err, backendDraining) {
			// Drain is a healthy generation handoff, not a peer failure. Retry
			// selection so new work moves to the replacement without closing
			// the old backend or poisoning the circuit breaker.
			permit.Release()
			continue
		}
		if isBackendClosedError(err) {
			c.retireBackend(backend, b)
			permit.RecordFailure()
			retryCount++
			// Check if max retries exceeded (0 means unlimited)
			if policy.MaxRetries > 0 && retryCount >= policy.MaxRetries {
				c.logger.Warn("max retries exceeded for Send",
					zap.String("backend", backend),
					zap.Int("retries", retryCount),
					zap.Error(err))
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

			if retryCount <= 3 || retryCount%10 == 0 {
				c.logger.Debug("retrying Send after backend closed",
					zap.String("backend", backend),
					zap.Int("retry", retryCount),
					zap.Duration("backoff", backoff))
			}
			continue
		}
		if err == nil {
			permit.RecordSuccess()
		} else {
			permit.RecordFailure()
		}
		return f, err
	}
}

func (c *client) NewStream(ctx context.Context, backend string, lock bool) (Stream, error) {
	if ctx == nil {
		panic("client NewStream nil context")
	}

	breaker := c.circuitBreakers.newHandle(backend)
	var permit circuitBreakerPermit
	defer func() {
		permit.Release()
	}()

	policy := c.options.retryPolicy
	var backoff time.Duration
	retryCount := 0
	var creationStart time.Time

	for {
		// Check context before attempting
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		var admitErr error
		permit, admitErr = breaker.Admit()
		if admitErr != nil {
			return nil, admitErr
		}

		b, backendCreate, err := c.getBackendForOperation(backend, lock)
		if err != nil {
			if isBackendCreateQueueCongestion(err) {
				return nil, err
			}

			// Wait for backend if:
			// 1. Auto-create enabled and waiting for creation, OR
			// 2. ErrBackendCreating (pool has backends but all busy - wait regardless of auto-create)
			waitingForCreate := (c.options.enableAutoCreate && isAutoCreateWaitError(err)) ||
				isErrBackendCreating(err)

			// Handle backend creation-in-progress with bounded wait
			if waitingForCreate {
				shouldContinue, waitErr := c.handleAutoCreateWait(
					ctx,
					backend,
					&creationStart,
					backendCreate,
					retryCount,
				)
				if !shouldContinue {
					// Record circuit breaker failure on timeout
					if !errors.Is(waitErr, context.Canceled) &&
						!errors.Is(waitErr, context.DeadlineExceeded) {
						permit.RecordFailure()
					}
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
					permit.RecordFailure()
					return nil, err
				}

				// Calculate next backoff with jitter
				backoff = policy.nextBackoff(backoff)
				if waitErr := c.waitBackendChange(
					ctx,
					backend,
					&creationStart,
					backendCreate,
					backoff,
				); waitErr != nil {
					if !errors.Is(waitErr, context.Canceled) &&
						!errors.Is(waitErr, context.DeadlineExceeded) &&
						!isBackendCreateQueueCongestion(waitErr) {
						permit.RecordFailure()
					}
					return nil, waitErr
				}
				permit.Release()
				continue
			}

			// Don't count client-level errors (like ErrClientClosed) as circuit breaker failures
			if !moerr.IsMoErrCode(err, moerr.ErrClientClosed) {
				permit.RecordFailure()
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
		if errors.Is(err, backendDraining) {
			permit.Release()
			continue
		}
		if isBackendClosedError(err) {
			c.retireBackend(backend, b)
			permit.RecordFailure()
			retryCount++
			// Check if max retries exceeded
			if policy.MaxRetries > 0 && retryCount >= policy.MaxRetries {
				c.logger.Warn("max retries exceeded for NewStream",
					zap.String("backend", backend),
					zap.Int("retries", retryCount),
					zap.Error(err))
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

			if retryCount <= 3 || retryCount%10 == 0 {
				c.logger.Debug("retrying NewStream after backend closed",
					zap.String("backend", backend),
					zap.Int("retry", retryCount),
					zap.Duration("backoff", backoff))
			}
			continue
		}
		if err == nil {
			permit.RecordSuccess()
		} else {
			permit.RecordFailure()
		}
		return st, err
	}
}

func (c *client) Ping(ctx context.Context, backend string) error {
	if ctx == nil {
		panic("client Ping nil context")
	}

	breaker := c.circuitBreakers.newHandle(backend)
	var permit circuitBreakerPermit
	defer func() {
		permit.Release()
	}()

	policy := c.options.retryPolicy
	var backoff time.Duration
	retryCount := 0
	var creationStart time.Time

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var admitErr error
		permit, admitErr = breaker.Admit()
		if admitErr != nil {
			return admitErr
		}

		b, backendCreate, err := c.getBackendForOperation(backend, false)
		if err != nil {
			if isBackendCreateQueueCongestion(err) {
				return err
			}

			// Wait for backend if:
			// 1. Auto-create enabled and waiting for creation, OR
			// 2. ErrBackendCreating (pool has backends but all busy - wait regardless of auto-create)
			waitingForCreate := (c.options.enableAutoCreate && isAutoCreateWaitError(err)) ||
				isErrBackendCreating(err)

			// Handle backend creation-in-progress with bounded wait
			if waitingForCreate {
				shouldContinue, waitErr := c.handleAutoCreateWait(
					ctx,
					backend,
					&creationStart,
					backendCreate,
					retryCount,
				)
				if !shouldContinue {
					// Record circuit breaker failure on timeout
					if !errors.Is(waitErr, context.Canceled) &&
						!errors.Is(waitErr, context.DeadlineExceeded) {
						permit.RecordFailure()
					}
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
					permit.RecordFailure()
					return err
				}

				// Calculate next backoff with jitter
				backoff = policy.nextBackoff(backoff)
				if waitErr := c.waitBackendChange(
					ctx,
					backend,
					&creationStart,
					backendCreate,
					backoff,
				); waitErr != nil {
					if !errors.Is(waitErr, context.Canceled) &&
						!errors.Is(waitErr, context.DeadlineExceeded) &&
						!isBackendCreateQueueCongestion(waitErr) {
						permit.RecordFailure()
					}
					return waitErr
				}
				permit.Release()
				continue
			}

			// Don't count client-level errors (like ErrClientClosed) as circuit breaker failures
			if !moerr.IsMoErrCode(err, moerr.ErrClientClosed) {
				permit.RecordFailure()
			}
			return err
		}

		f, err := b.SendInternal(ctx, &flagOnlyMessage{flag: flagPing})
		if err != nil {
			if errors.Is(err, backendDraining) {
				permit.Release()
				continue
			}
			if isBackendClosedError(err) {
				c.retireBackend(backend, b)
				permit.RecordFailure()
				retryCount++
				// Check if max retries exceeded
				if policy.MaxRetries > 0 && retryCount >= policy.MaxRetries {
					c.logger.Warn("max retries exceeded for Ping",
						zap.String("backend", backend),
						zap.Int("retries", retryCount),
						zap.Error(err))
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

				if retryCount <= 3 || retryCount%10 == 0 {
					c.logger.Debug("retrying Ping after backend closed",
						zap.String("backend", backend),
						zap.Int("retry", retryCount),
						zap.Duration("backoff", backoff))
				}
				continue
			}
			permit.RecordFailure()
			return err
		}
		_, err = f.Get()
		f.Close()
		if err == nil {
			permit.RecordSuccess()
		} else {
			permit.RecordFailure()
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
	c.backendCreate.Wait()

	// Unregister from the exact manager incarnation selected by NewClient.
	if c.gcManager != nil {
		c.gcManager.unregister(c)
	}

	// Update active client count (only the Close owner reaches here).
	if c.countedActive {
		activeGauge := v2.NewRPCClientActiveGaugeByName(c.name)
		activeGauge.Dec()
	}

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
	if c.mu.closing || c.mu.closed {
		c.mu.Unlock()
		return nil
	}

	// Reset is a generation boundary for the complete data-transport pool.
	// Terminate every queued/in-flight state before dropping its token so
	// waiters can re-evaluate immediately against a fresh generation.
	for remote := range c.mu.creating {
		c.invalidateBackendCreateLocked(remote)
	}
	clear(c.mu.backendGeneration)

	backends := c.detachAllBackendsLocked()
	clear(c.mu.ops)
	// CloseBackend remains synchronous for its caller, but cleanup ownership
	// must be visible before c.mu is released. A concurrent client Close either
	// owns the original pool snapshot or joins these closes, never both.
	c.backendCleanup.Add(len(backends))
	c.updatePoolSizeMetricsLocked()
	c.mu.Unlock()

	// Backend.Close may wait for transport goroutines or network teardown. New
	// operations can create and publish the replacement generation while that
	// old teardown is still in progress.
	for _, backend := range backends {
		func() {
			defer c.backendCleanup.Done()
			backend.Close()
		}()
	}
	return nil
}

// CloseBackendFor synchronously detaches one remote and invalidates any
// asynchronous create requests queued before this call.
func (c *client) CloseBackendFor(remote string) error {
	c.mu.Lock()
	backends := c.mu.backends[remote]
	// The reset remains synchronous for its caller, but register ownership before
	// detaching so a concurrent client Close cannot miss this teardown.
	c.backendCleanup.Add(len(backends))
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
		func() {
			defer c.backendCleanup.Done()
			backend.Close()
		}()
	}
	return nil
}

func (c *client) getBackend(backend string, lock bool) (Backend, error) {
	breaker := c.circuitBreakers.newHandle(backend)
	permit, err := breaker.Admit()
	if err != nil {
		return nil, err
	}
	defer permit.Release()

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
	c.mu.Lock()
	// Preserve the healthy-backend fast path. Only compact terminal capacity
	// after selection misses; doing a full slice rewrite before every Send/Ping
	// would add avoidable work to the per-operation hot path.
	b, err := c.getBackendLockedWithCreate(backend, lock, false)
	if b == nil && (err == nil || isBackendClosedError(err)) {
		if c.detachInactiveForCleanupLocked(backend) > 0 {
			// Re-evaluate selection and capacity in the same client-state
			// snapshot after terminal entries have been removed.
			b, err = c.getBackendLockedWithCreate(backend, lock, false)
		}
		if b == nil &&
			(err == nil || isBackendClosedError(err)) &&
			!c.canCreateLocked(backend) {
			// At most one draining generation may temporarily exceed the
			// configured active capacity. If its replacement also loses data
			// progress, retire the oldest drain rather than make the remote
			// permanently unavailable or grow generations without bound.
			if c.detachOldestDrainingForCleanupLocked(backend) > 0 {
				b, err = c.getBackendLockedWithCreate(backend, lock, false)
			}
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
		// outside the lock in the fixed process-wide worker pool.
		backendCreate, queued := c.gcManager.triggerCreateAtGenerationLocked(
			c,
			backend,
			generation,
		)
		if queued {
			unlock()
			return nil, backendCreate, ErrBackendCreating
		}

		// Never bypass the process-wide factory concurrency bound when the
		// queue is full. Synchronous fallback here turns local overload into
		// unbounded caller-goroutine DNS/dial fan-out.
		unlock()
		return nil, nil, ErrBackendCreateQueueFull
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
			if !b.Locked() &&
				b.LastActiveTime() != (time.Time{}) &&
				backendAdmissionAvailable(b) {
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

	_, ok := c.gcManager.triggerCreateAtGenerationLocked(
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
	state := newBackendCreateState(generation)
	c.mu.creating[backend] = state
	c.mu.Unlock()
	return c.createBackendForClaimedState(backend, lock, state, false)
}

// createBackendForClaimedState performs factory I/O for a create request that
// owns the exact queued/in-flight state for this remote generation. State
// identity, in addition to generation identity, prevents an expired queue item
// from claiming a replacement state in the same generation.
func (c *client) createBackendForClaimedState(
	backend string,
	lock bool,
	state *backendCreateState,
	enforceQueueTimeout bool,
) (Backend, error) {
	if state == nil || state.generation == nil {
		return nil, moerr.NewBackendClosedNoCtx()
	}
	c.mu.Lock()
	if c.mu.closing || c.mu.closed {
		c.releaseBackendCreateLocked(backend, state)
		c.mu.Unlock()
		return nil, moerr.NewClientClosedNoCtx()
	}
	if !c.hasBackendCreateLocked(backend, state) {
		c.mu.Unlock()
		return nil, moerr.NewBackendClosedNoCtx()
	}
	if !c.canCreateLocked(backend) {
		c.releaseBackendCreateLocked(backend, state)
		c.mu.Unlock()
		return nil, moerr.NewBackendClosedNoCtx()
	}
	if enforceQueueTimeout &&
		c.backendCreateQueueExpiredLocked(state, time.Now()) {
		c.expireBackendCreateQueueLocked(backend, state)
		c.mu.Unlock()
		return nil, ErrBackendCreateQueueTimeout
	}
	c.backendCreate.Add(1)
	// Publish the point at which the global worker actually admits this request
	// to factory I/O. Queue residence before this transition must not consume a
	// peer-health timeout.
	state.markStarted()
	c.mu.Unlock()
	defer c.backendCreate.Done()

	// Create backend using factory with metrics (same as doCreate) without holding the lock.
	b, err := c.doCreate(state.ctx, backend)
	state.markCompleted(time.Now())

	// Re-acquire lock to add to pool, validating limits again.
	c.mu.Lock()
	claimActive := c.hasBackendCreateLocked(backend, state)
	if err != nil {
		if state.ctx.Err() != nil {
			// A cancelled generation is local lifecycle evidence. Wake coalesced
			// waiters without recording a factory or peer failure.
			c.releaseBackendCreateLocked(backend, state)
		} else {
			c.failBackendCreateLocked(backend, state, err)
		}
		c.mu.Unlock()
		return nil, err
	}

	clientClosed := c.mu.closing || c.mu.closed
	if !claimActive || clientClosed {
		c.releaseBackendCreateLocked(backend, state)
		c.mu.Unlock()
		b.Close()
		if clientClosed {
			return nil, moerr.NewClientClosedNoCtx()
		}
		return nil, moerr.NewBackendClosedNoCtx()
	}
	if c.mu.backendGeneration[backend] != state.generation {
		c.releaseBackendCreateLocked(backend, state)
		c.mu.Unlock()
		b.Close()
		return nil, moerr.NewBackendClosedNoCtx()
	}
	if !c.canCreateLocked(backend) {
		// Another goroutine may have filled the pool while we were creating.
		c.releaseBackendCreateLocked(backend, state)
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
	c.releaseBackendCreateLocked(backend, state)
	c.mu.Unlock()

	return b, nil
}

func (c *client) releaseBackendCreate(backend string, state *backendCreateState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.releaseBackendCreateLocked(backend, state)
}

const maxBackendGenerationEntries = 4096

type backendGeneration struct {
	// Keep the allocation non-zero-sized so distinct generations always have
	// distinct pointer identities.
	_ byte
}

type backendCreateState struct {
	generation *backendGeneration
	ctx        context.Context
	cancel     context.CancelFunc
	queuedAt   time.Time
	started    chan struct{}
	startedAt  time.Time
	// completedAt is published immediately after factory.Create returns, before
	// pool-publication bookkeeping. A waiter whose timer and done signal become
	// ready together can therefore classify the result by event time rather
	// than scheduler selection order.
	completedAt atomic.Pointer[time.Time]
	// timeoutObserved deduplicates diagnostics and event metrics for callers
	// coalesced on this exact create state. The request-impact counter remains
	// per caller.
	timeoutObserved atomic.Bool
	done            chan struct{}
	// factoryErr is set only when factory I/O completed with an error while
	// this exact state still owned the remote generation. Closing done
	// publishes it to waiters. Definitive connection errors are returned
	// precisely; generic factory errors retain the configured retry/backoff.
	// Reset, queue expiry, and client/manager shutdown close done without
	// setting it, so local invalidation remains distinct from factory evidence.
	factoryErr    error
	queueTimedOut bool
}

func newBackendCreateState(
	generation *backendGeneration,
	parents ...context.Context,
) *backendCreateState {
	parent := context.Background()
	if len(parents) > 0 && parents[0] != nil {
		parent = parents[0]
	}
	ctx, cancel := context.WithCancel(parent)
	return &backendCreateState{
		generation: generation,
		ctx:        ctx,
		cancel:     cancel,
		queuedAt:   time.Now(),
		started:    make(chan struct{}),
		done:       make(chan struct{}),
	}
}

func (s *backendCreateState) stop() {
	if s != nil && s.cancel != nil {
		s.cancel()
	}
}

// markStarted publishes startedAt before closing started. Callers serialize
// this transition with c.mu; channel close provides the happens-before edge for
// waiters that read startedAt.
func (s *backendCreateState) markStarted() {
	select {
	case <-s.started:
		return
	default:
		s.startedAt = time.Now()
		close(s.started)
	}
}

func (s *backendCreateState) startTime() (time.Time, bool) {
	select {
	case <-s.started:
		return s.startedAt, true
	default:
		return time.Time{}, false
	}
}

func (s *backendCreateState) markCompleted(at time.Time) {
	completedAt := at
	s.completedAt.Store(&completedAt)
}

func (s *backendCreateState) completionTime() (time.Time, bool) {
	completedAt := s.completedAt.Load()
	if completedAt == nil {
		return time.Time{}, false
	}
	return *completedAt, true
}

func (c *client) hasBackendCreateLocked(
	backend string,
	state *backendCreateState,
) bool {
	return state != nil &&
		c.mu.creating[backend] == state &&
		c.mu.backendGeneration[backend] == state.generation
}

func (c *client) releaseBackendCreateLocked(
	backend string,
	state *backendCreateState,
) {
	if state == nil || c.mu.creating[backend] != state {
		return
	}
	delete(c.mu.creating, backend)
	state.stop()
	close(state.done)
}

func (c *client) failBackendCreateLocked(
	backend string,
	state *backendCreateState,
	err error,
) {
	if state == nil || c.mu.creating[backend] != state {
		return
	}
	state.factoryErr = err
	delete(c.mu.creating, backend)
	state.stop()
	close(state.done)
}

func (c *client) backendCreateQueueExpiredLocked(
	state *backendCreateState,
	now time.Time,
) bool {
	timeout := c.options.autoCreateQueueWaitTimeout
	return timeout > 0 &&
		!state.queuedAt.IsZero() &&
		!now.Before(state.queuedAt.Add(timeout))
}

func (c *client) expireBackendCreateQueueLocked(
	backend string,
	state *backendCreateState,
) bool {
	return c.invalidateQueuedBackendCreateLocked(backend, state, true)
}

func (c *client) invalidateQueuedBackendCreateLocked(
	backend string,
	state *backendCreateState,
	queueTimedOut bool,
) bool {
	if state == nil || c.mu.creating[backend] != state {
		return false
	}
	if _, started := state.startTime(); started {
		return false
	}
	state.queueTimedOut = queueTimedOut
	delete(c.mu.creating, backend)
	state.stop()
	close(state.done)
	return true
}

func (c *client) expireBackendCreateQueue(
	backend string,
	state *backendCreateState,
) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.expireBackendCreateQueueLocked(backend, state)
}

func (c *client) invalidateQueuedBackendCreate(
	backend string,
	state *backendCreateState,
) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.invalidateQueuedBackendCreateLocked(backend, state, false)
}

func (c *client) invalidateBackendCreateLocked(backend string) {
	state := c.mu.creating[backend]
	if state == nil {
		return
	}
	delete(c.mu.creating, backend)
	state.stop()
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
	c.gcManager.triggerGCInactive(c, remote)
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

// detachOldestDrainingForCleanupLocked keeps replacement growth bounded. It is
// used only after selection found no admissible backend and the one-generation
// drain allowance is already full.
func (c *client) detachOldestDrainingForCleanupLocked(remote string) int {
	for _, backend := range c.mu.backends[remote] {
		if !backendAdmissionAvailable(backend) &&
			c.detachBackendForCleanupLocked(remote, backend) {
			return 1
		}
	}
	return 0
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
			lastActive := b.LastActiveTime()
			if !backendAdmissionAvailable(b) &&
				lastActive != (time.Time{}) {
				// A draining generation still owns Futures/streams. Their
				// contexts, terminal responses, or explicit Close own the
				// lifetime; idle GC must not cancel them.
				newBackends = append(newBackends, b)
				continue
			}
			if !b.Locked() &&
				time.Since(lastActive) > c.options.maxIdleDuration &&
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

	b, err := c.doCreate(context.Background(), backend)
	if err != nil {
		return nil, err
	}
	c.mu.backends[backend] = append(c.mu.backends[backend], b)
	if _, ok := c.mu.ops[backend]; !ok {
		c.mu.ops[backend] = &op{}
	}
	return b, nil
}

func (c *client) doCreate(ctx context.Context, backend string) (Backend, error) {
	var b Backend
	var err error
	if factory, ok := c.factory.(ContextBackendFactory); ok {
		b, err = factory.CreateWithContext(
			ctx,
			backend,
			WithBackendMetrics(c.metrics),
		)
	} else {
		b, err = c.factory.Create(backend, WithBackendMetrics(c.metrics))
	}
	if err != nil {
		// Generation and lifecycle cancellation is local scheduler evidence, not
		// a remote failure. Avoid both error-log storms and breaker poisoning.
		if ctx.Err() == nil {
			c.logger.Error("create backend failed",
				zap.String("backend", backend),
				zap.Error(err))
		}
		return nil, err
	}
	return b, nil
}

func (c *client) canCreateLocked(backend string) bool {
	backends := c.mu.backends[backend]
	admissible := 0
	for _, backend := range backends {
		if backendAdmissionAvailable(backend) {
			admissible++
		}
	}
	// Permit one draining generation beyond the configured active capacity so
	// a valid slow request can finish while new traffic moves to a fresh data
	// transport. The extra physical generation is a hard per-remote bound.
	return admissible < c.options.maxBackendsPerHost &&
		len(backends) < c.options.maxBackendsPerHost+1
}

type backendAdmission interface {
	admissionAvailable() bool
}

func backendAdmissionAvailable(backend Backend) bool {
	value, ok := backend.(backendAdmission)
	return !ok || value.admissionAvailable()
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

func isBackendCreateQueueCongestion(err error) bool {
	return errors.Is(err, ErrBackendCreateQueueFull) ||
		errors.Is(err, ErrBackendCreateQueueTimeout)
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
func (c *client) handleAutoCreateWait(
	ctx context.Context,
	backend string,
	creationStart *time.Time,
	backendCreate *backendCreateState,
	retryCount int,
) (bool, error) {
	// Check context first
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	// A state with no completion owner (for example, a fully busy pool) needs a
	// bounded retry budget immediately. For asynchronous creation, exclude only
	// the initial global-manager queue residence. Once factory work has started,
	// all later retries and queue waits retain the same budget.
	if creationStart.IsZero() {
		if backendCreate == nil {
			*creationStart = time.Now()
		} else if startedAt, ok := backendCreate.startTime(); ok {
			*creationStart = startedAt
		}
	}

	// A concrete asynchronous state owns completion classification. Its
	// completedAt event may precede the deadline even if this goroutine is not
	// scheduled until later, so do not classify it by observation time here.
	// Capacity waits have no such event and retain the wall-clock check.
	if timeout := c.options.autoCreateWaitTimeout; timeout > 0 &&
		backendCreate == nil &&
		!creationStart.IsZero() {
		elapsed := time.Since(*creationStart)
		if elapsed >= timeout {
			return false, c.autoCreateTimeoutError(backend, elapsed, timeout, nil)
		}
	}

	// Log creation wait progress (sparse logging)
	if !creationStart.IsZero() && (retryCount == 1 || retryCount%10 == 0) {
		c.logger.Debug("waiting for backend creation",
			zap.String("backend", backend),
			zap.Int("retry", retryCount),
			zap.Duration("waited", time.Since(*creationStart)))
	} else if creationStart.IsZero() && (retryCount == 1 || retryCount%10 == 0) {
		c.logger.Debug("backend creation queued",
			zap.String("backend", backend),
			zap.Int("retry", retryCount))
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
	creationStart *time.Time,
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
		if backendCreate.factoryErr == nil {
			return nil
		}
	}

	return c.waitBackendRetryBackoff(ctx, backend, *creationStart, backoff)
}

func (c *client) waitBackendCreateCompletion(
	ctx context.Context,
	backend string,
	creationStart *time.Time,
	backendCreate *backendCreateState,
) error {
	startedAt, started, err := c.waitBackendCreateAdmission(
		ctx,
		backend,
		*creationStart,
		backendCreate,
	)
	if err != nil || !started {
		return err
	}
	if creationStart.IsZero() {
		*creationStart = startedAt
	}
	return c.waitBackendFactoryCompletion(
		ctx,
		backend,
		*creationStart,
		backendCreate,
	)
}

// waitBackendCreateAdmission waits for the exact queued state to either enter
// the fixed factory-worker pool or terminate. Expiring the state removes it
// from c.mu.creating in O(1), so a later retry can enqueue a replacement
// without waiting for the stale queue item to reach a worker.
func (c *client) waitBackendCreateAdmission(
	ctx context.Context,
	backend string,
	creationStart time.Time,
	state *backendCreateState,
) (time.Time, bool, error) {
	if startedAt, ok := state.startTime(); ok {
		return startedAt, true, nil
	}
	if backendCreateDone(state) {
		return c.backendCreateAdmissionResult(state)
	}

	deadline, deadlineKind := c.backendCreateAdmissionDeadline(
		creationStart,
		state,
	)
	if deadline.IsZero() {
		select {
		case <-ctx.Done():
			return time.Time{}, false, ctx.Err()
		case <-state.started:
			return state.startedAt, true, nil
		case <-state.done:
			return c.backendCreateAdmissionResult(state)
		}
	}

	remaining := time.Until(deadline)
	if remaining <= 0 {
		return c.handleBackendCreateAdmissionDeadline(
			backend,
			creationStart,
			state,
			deadlineKind,
		)
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return time.Time{}, false, ctx.Err()
	case <-state.started:
		return state.startedAt, true, nil
	case <-state.done:
		return c.backendCreateAdmissionResult(state)
	case <-timer.C:
		return c.handleBackendCreateAdmissionDeadline(
			backend,
			creationStart,
			state,
			deadlineKind,
		)
	}
}

type backendCreateAdmissionDeadlineKind uint8

const (
	backendCreateNoAdmissionDeadline backendCreateAdmissionDeadlineKind = iota
	backendCreateQueueAdmissionDeadline
	backendCreateFactoryBudgetDeadline
)

func (c *client) backendCreateAdmissionDeadline(
	creationStart time.Time,
	state *backendCreateState,
) (time.Time, backendCreateAdmissionDeadlineKind) {
	var deadline time.Time
	kind := backendCreateNoAdmissionDeadline
	if timeout := c.options.autoCreateQueueWaitTimeout; timeout > 0 &&
		!state.queuedAt.IsZero() {
		deadline = state.queuedAt.Add(timeout)
		kind = backendCreateQueueAdmissionDeadline
	}
	// Only the initial queue wait is excluded from the peer-health budget.
	// Once the first factory attempt starts, retries (including their queue
	// residence) share the original auto-create deadline.
	if timeout := c.options.autoCreateWaitTimeout; timeout > 0 &&
		!creationStart.IsZero() {
		factoryDeadline := creationStart.Add(timeout)
		if deadline.IsZero() || !factoryDeadline.After(deadline) {
			deadline = factoryDeadline
			kind = backendCreateFactoryBudgetDeadline
		}
	}
	return deadline, kind
}

func backendCreateDone(state *backendCreateState) bool {
	select {
	case <-state.done:
		return true
	default:
		return false
	}
}

func (c *client) backendCreateAdmissionResult(
	state *backendCreateState,
) (time.Time, bool, error) {
	if startedAt, ok := state.startTime(); ok {
		return startedAt, true, nil
	}
	if state.queueTimedOut {
		return time.Time{}, false, ErrBackendCreateQueueTimeout
	}
	// Reset, close, or manager shutdown can invalidate a queued request without
	// starting it. Wake the caller to re-evaluate the current generation.
	return time.Time{}, false, nil
}

func (c *client) handleBackendCreateAdmissionDeadline(
	backend string,
	creationStart time.Time,
	state *backendCreateState,
	kind backendCreateAdmissionDeadlineKind,
) (time.Time, bool, error) {
	if kind == backendCreateQueueAdmissionDeadline {
		// Worker admission and queue expiry linearize under c.mu. If the worker
		// won, startedAt is visible through the closed started channel; if
		// expiry won, only this exact state is removed.
		if c.expireBackendCreateQueue(backend, state) {
			return time.Time{}, false, ErrBackendCreateQueueTimeout
		}
	} else if kind == backendCreateFactoryBudgetDeadline {
		// This caller's shared retry budget expired while the exact request was
		// still queued. Remove the stale work before returning so it can never
		// dial later. Do not mark a queue timeout: coalesced callers may have a
		// different factory budget and should simply wake and re-evaluate.
		if c.invalidateQueuedBackendCreate(backend, state) {
			timeout := c.options.autoCreateWaitTimeout
			return time.Time{}, false, c.autoCreateTimeoutError(
				backend,
				time.Since(creationStart),
				timeout,
				state,
			)
		}
	}
	if startedAt, ok := state.startTime(); ok {
		return startedAt, true, nil
	}
	if backendCreateDone(state) {
		return c.backendCreateAdmissionResult(state)
	}
	if kind == backendCreateFactoryBudgetDeadline {
		timeout := c.options.autoCreateWaitTimeout
		return time.Time{}, false, c.autoCreateTimeoutError(
			backend,
			time.Since(creationStart),
			timeout,
			state,
		)
	}
	// The state was replaced without this waiter owning its completion. Treat
	// that as invalidation and let the caller re-read current client state.
	return time.Time{}, false, nil
}

func (c *client) waitBackendFactoryCompletion(
	ctx context.Context,
	backend string,
	creationStart time.Time,
	state *backendCreateState,
) error {
	timeout := c.options.autoCreateWaitTimeout
	if timeout <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-state.done:
			return definitiveBackendCreateError(state.factoryErr)
		}
	}

	deadline := creationStart.Add(timeout)
	for {
		if backendCreateDone(state) {
			return c.classifyBackendFactoryCompletion(
				backend,
				creationStart,
				deadline,
				timeout,
				state,
			)
		}

		// Factory completion is recorded before publication bookkeeping. If it
		// occurred within budget, wait for done even after the deadline; if it
		// occurred late, report the timeout independent of select scheduling.
		if completedAt, ok := state.completionTime(); ok {
			if completedAt.After(deadline) {
				return c.autoCreateTimeoutError(
					backend,
					completedAt.Sub(creationStart),
					timeout,
					state,
				)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-state.done:
				return c.classifyBackendFactoryCompletion(
					backend,
					creationStart,
					deadline,
					timeout,
					state,
				)
			}
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return c.autoCreateTimeoutError(
				backend,
				time.Since(creationStart),
				timeout,
				state,
			)
		}
		timer := time.NewTimer(remaining)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-state.done:
			timer.Stop()
			return c.classifyBackendFactoryCompletion(
				backend,
				creationStart,
				deadline,
				timeout,
				state,
			)
		case <-timer.C:
			// A simultaneous factory return must be classified by completedAt,
			// not by which ready select case the runtime happened to choose.
		}
	}
}

func (c *client) classifyBackendFactoryCompletion(
	backend string,
	creationStart time.Time,
	deadline time.Time,
	timeout time.Duration,
	state *backendCreateState,
) error {
	if completedAt, ok := state.completionTime(); ok &&
		completedAt.After(deadline) {
		return c.autoCreateTimeoutError(
			backend,
			completedAt.Sub(creationStart),
			timeout,
			state,
		)
	}
	return definitiveBackendCreateError(state.factoryErr)
}

func definitiveBackendCreateError(err error) error {
	if moerr.IsMoErrCode(err, moerr.ErrBackendCannotConnect) {
		return err
	}
	return nil
}

func (c *client) waitBackendRetryBackoff(
	ctx context.Context,
	backend string,
	creationStart time.Time,
	backoff time.Duration,
) error {
	wait := backoff
	timedByCreateTimeout := false
	if timeout := c.options.autoCreateWaitTimeout; timeout > 0 {
		remaining := timeout - time.Since(creationStart)
		if remaining <= 0 {
			return c.autoCreateTimeoutError(backend, time.Since(creationStart), timeout, nil)
		}
		if remaining < wait {
			wait = remaining
			timedByCreateTimeout = true
		}
	}
	if wait <= 0 {
		return nil
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		if timedByCreateTimeout {
			timeout := c.options.autoCreateWaitTimeout
			return c.autoCreateTimeoutError(backend, time.Since(creationStart), timeout, nil)
		}
		return nil
	}
}

func (c *client) autoCreateTimeoutError(
	backend string,
	waited time.Duration,
	timeout time.Duration,
	state *backendCreateState,
) error {
	c.metrics.autoCreateTimeoutCounter.Inc()
	scope := "capacity-or-retry"
	if state != nil {
		scope = "create-state"
		if !state.timeoutObserved.CompareAndSwap(false, true) {
			return ErrBackendCreateTimeout
		}
		c.metrics.autoCreateTimeoutEventCounter.Inc()
	}
	c.autoCreateTimeoutLogger.WarnWithConfig(
		"backend-auto-create-timeout",
		"auto-create backend timed out",
		logutil.RateLimitConfig{
			Interval:   time.Minute,
			BurstCount: 1,
		},
		zap.String("client", c.name),
		zap.String("backend", backend),
		zap.String("scope", scope),
		zap.Duration("waited", waited),
		zap.Duration("timeout", timeout))
	return ErrBackendCreateTimeout
}

type op struct {
	seq uint64
}

func (o *op) next() uint64 {
	return atomic.AddUint64(&o.seq, 1)
}
