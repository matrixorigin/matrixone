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
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	// DefaultGCIdleCheckInterval is the default interval for checking idle backends.
	// This value is shared between client_gc.go and cfg.go.
	DefaultGCIdleCheckInterval = time.Second

	// DefaultGCInactiveCheckInterval is the interval for proactively removing
	// explicitly closed (inactive) backends. Such backends are removed within
	// this period instead of waiting for the normal idle timeout (e.g. 1 minute).
	DefaultGCInactiveCheckInterval = 10 * time.Second

	// DefaultGCChannelBufferSize is the default buffer size for GC task channels.
	// Increased from 1024 to 4096 to reduce request drops in high-load scenarios.
	DefaultGCChannelBufferSize = 4096
)

var (
	// globalGCIdleCheckInterval controls how often the global GC manager
	// checks for idle backends. Default is 1 second for timely cleanup.
	// This can be overridden via:
	// 1. MORPC_GC_IDLE_INTERVAL environment variable
	// 2. Config.GCIdleCheckInterval (takes precedence)
	// Note: The actual GC decision still uses each client's maxIdleDuration,
	// this only controls the check frequency.
	globalGCIdleCheckInterval = getGCIdleCheckInterval()

	// globalGCChannelBufferSize controls the buffer size for GC task channels.
	// Default is 4096. Can be overridden via Config.GCChannelBufferSize.
	globalGCChannelBufferSize = getGCChannelBufferSize()

	// globalClientGCMu protects initialization of globalClientGC
	globalClientGCMu sync.RWMutex
	globalClientGC   = newClientGCManager()
)

// getGCIdleCheckInterval reads GC idle check interval from environment variable.
// Default is DefaultGCIdleCheckInterval (1 second). This allows fine-tuning for different environments.
// Config values take precedence and should be set via InitGlobalGCManager.
func getGCIdleCheckInterval() time.Duration {
	if v := os.Getenv("MORPC_GC_IDLE_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return DefaultGCIdleCheckInterval
}

// getGCChannelBufferSize reads GC channel buffer size from environment variable.
// Default is DefaultGCChannelBufferSize (4096).
// Config values take precedence and should be set via InitGlobalGCManager.
func getGCChannelBufferSize() int {
	if v := os.Getenv("MORPC_GC_CHANNEL_BUFFER_SIZE"); v != "" {
		if size, err := strconv.Atoi(v); err == nil && size > 0 {
			return size
		}
	}
	return DefaultGCChannelBufferSize
}

// InitGlobalGCManager initializes the global GC manager with config values.
// This should be called before creating any clients if you want to use config values.
// If not called, environment variables or defaults will be used.
//
// Note: This function should be called before any clients are created. If called
// after the GC manager has started, it will only update the interval (if manager
// hasn't started yet), but channel buffer size cannot be changed after initialization.
//
// Thread-safety: This function is safe to call concurrently. It uses a separate mutex
// to protect the global manager initialization to avoid lock issues when replacing
// the manager instance.
func InitGlobalGCManager(checkInterval time.Duration, channelBufferSize int) {
	// Use global mutex to protect manager replacement
	globalClientGCMu.Lock()
	defer globalClientGCMu.Unlock()

	// Check if manager has started (need to lock manager's mutex for this check)
	globalClientGC.mu.RLock()
	started := globalClientGC.started
	globalClientGC.mu.RUnlock()

	if !started {
		// Manager not started yet, safe to reconfigure
		if checkInterval > 0 {
			globalGCIdleCheckInterval = checkInterval
		}
		if channelBufferSize > 0 {
			globalGCChannelBufferSize = channelBufferSize
			// Recreate manager with new channel buffer size
			// Safe because manager is not started (no goroutines running)
			globalClientGC = newClientGCManager()
		}
	} else {
		// Manager already started, can only update interval (channels already created)
		if checkInterval > 0 {
			globalGCIdleCheckInterval = checkInterval
			globalClientGC.logger.Info("GC idle check interval updated",
				zap.Duration("new-interval", checkInterval))
		}
		if channelBufferSize > 0 {
			globalClientGC.logger.Warn("InitGlobalGCManager: channel buffer size cannot be changed after GC manager started",
				zap.Int("requested-size", channelBufferSize),
				zap.Int("current-size", cap(globalClientGC.gcInactiveC)))
		}
	}
}

// clientGCManager manages GC tasks for all morpc clients globally.
// Instead of each client creating 3 goroutines (gcIdleTask, gcInactiveTask, createTask),
// all clients share a single global GC manager that handles these tasks.
//
// This design is similar to mainstream RPC frameworks (gRPC, Thrift, etc.) which use
// global connection pools and resource managers rather than per-client goroutines.
//
// Thread-safety: All operations are thread-safe and support concurrent client registration/
// unregistration. This works correctly even in single-process multi-service scenarios
// (e.g., multiple CN/TN/Proxy nodes in integration tests).
type clientGCManager struct {
	mu      sync.RWMutex
	clients map[*client]struct{}
	started bool
	stopC   chan struct{}
	wg      sync.WaitGroup
	logger  *zap.Logger

	// Config values captured at creation time to avoid race conditions
	gcIdleCheckInterval     time.Duration
	gcInactiveCheckInterval time.Duration
	channelBufferSize       int

	// Channels for coordinating GC tasks
	gcInactiveC chan gcInactiveRequest
	createC     chan createRequest

	// Metrics for monitoring channel drops
	gcInactiveDropCounter prometheus.Counter
	createDropCounter     prometheus.Counter

	// Metrics for monitoring GC operations
	registeredClientsGauge     prometheus.Gauge
	gcInactiveQueueLengthGauge prometheus.Gauge
	createQueueLengthGauge     prometheus.Gauge
	gcInactiveProcessedCounter prometheus.Counter
	createProcessedCounter     prometheus.Counter
	idleBackendsCleanedCounter prometheus.Counter
}

type gcInactiveRequest struct {
	c      *client
	remote string
}

type createRequest struct {
	c       *client
	backend string
}

func newClientGCManager() *clientGCManager {
	// Capture config values at creation time to avoid race conditions
	// These values are read from global variables which are protected by globalClientGCMu
	interval := globalGCIdleCheckInterval
	bufferSize := globalGCChannelBufferSize

	// Initialize counters to ensure they appear in Prometheus even when value is 0
	gcInactiveDropCounter := v2.NewRPCGCChannelDropCounter("gc_inactive")
	createDropCounter := v2.NewRPCGCChannelDropCounter("create")
	gcInactiveDropCounter.Add(0)
	createDropCounter.Add(0)

	// Initialize gauges
	registeredClientsGauge := v2.GetRPCGCRegisteredClientsGauge()
	registeredClientsGauge.Set(0)
	gcInactiveQueueLengthGauge := v2.NewRPCGCChannelQueueLengthGauge("gc_inactive")
	gcInactiveQueueLengthGauge.Set(0)
	createQueueLengthGauge := v2.NewRPCGCChannelQueueLengthGauge("create")
	createQueueLengthGauge.Set(0)

	// Initialize counters
	gcInactiveProcessedCounter := v2.GetRPCGCInactiveProcessedCounter()
	gcInactiveProcessedCounter.Add(0)
	createProcessedCounter := v2.GetRPCGCCreateProcessedCounter()
	createProcessedCounter.Add(0)
	idleBackendsCleanedCounter := v2.GetRPCGCIdleBackendsCleanedCounter()
	idleBackendsCleanedCounter.Add(0)

	return &clientGCManager{
		clients:                    make(map[*client]struct{}),
		stopC:                      make(chan struct{}),
		gcIdleCheckInterval:        interval,
		gcInactiveCheckInterval:    DefaultGCInactiveCheckInterval,
		channelBufferSize:          bufferSize,
		gcInactiveC:                make(chan gcInactiveRequest, bufferSize),
		createC:                    make(chan createRequest, bufferSize),
		logger:                     logutil.GetPanicLoggerWithLevel(zap.ErrorLevel).Named("morpc-gc"),
		gcInactiveDropCounter:      gcInactiveDropCounter,
		createDropCounter:          createDropCounter,
		registeredClientsGauge:     registeredClientsGauge,
		gcInactiveQueueLengthGauge: gcInactiveQueueLengthGauge,
		createQueueLengthGauge:     createQueueLengthGauge,
		gcInactiveProcessedCounter: gcInactiveProcessedCounter,
		createProcessedCounter:     createProcessedCounter,
		idleBackendsCleanedCounter: idleBackendsCleanedCounter,
	}
}

// register registers a client with the global GC manager and starts the GC loop if needed.
func (m *clientGCManager) register(c *client) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.clients[c] = struct{}{}
	m.registeredClientsGauge.Set(float64(len(m.clients)))
	m.ensureStartedLocked()
}

// ensureStartedLocked starts the GC goroutines if not already started.
// Must be called with m.mu held.
func (m *clientGCManager) ensureStartedLocked() {
	if !m.started {
		m.started = true
		m.wg.Add(3)
		go m.runGCIdleLoop()
		go m.runGCInactiveLoop()
		go m.runCreateLoop()
	}
}

// unregister removes a client from the global GC manager.
func (m *clientGCManager) unregister(c *client) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.clients, c)
	m.registeredClientsGauge.Set(float64(len(m.clients)))
}

func init() {
	// Pre-start the GC manager so leaktest.AfterTest() snapshot includes it.
	// This prevents false positive goroutine leak reports in tests.
	globalClientGC.mu.Lock()
	globalClientGC.ensureStartedLocked()
	globalClientGC.mu.Unlock()
}

// ResetGlobalClientGCManagerForTest resets the global GC manager for testing purposes.
// This stops the current manager and creates a new one.
// WARNING: Only use this in tests!
func ResetGlobalClientGCManagerForTest() {
	globalClientGCMu.Lock()
	mgr := globalClientGC
	globalClientGCMu.Unlock()

	// Stop outside of lock to avoid deadlock
	mgr.stop()

	globalClientGCMu.Lock()
	defer globalClientGCMu.Unlock()
	globalClientGC = newClientGCManager()
}

// triggerGCInactive triggers GC inactive task for a specific client and backend.
// If channel is full, the request is dropped to avoid blocking.
func (m *clientGCManager) triggerGCInactive(c *client, remote string) {
	select {
	case m.gcInactiveC <- gcInactiveRequest{c: c, remote: remote}:
	default:
		// Channel is full, skip to avoid blocking
		m.gcInactiveDropCounter.Inc()
		m.logger.Debug("gc inactive channel full, skipping",
			zap.String("remote", remote),
			zap.Int("channel-capacity", cap(m.gcInactiveC)))
	}
}

// triggerCreate triggers create task for a specific client and backend.
// Returns true if the request was successfully queued, false if channel was full.
func (m *clientGCManager) triggerCreate(c *client, backend string) bool {
	select {
	case m.createC <- createRequest{c: c, backend: backend}:
		return true
	default:
		// Channel is full, skip to avoid blocking
		// This is acceptable because create will be retried when needed
		m.createDropCounter.Inc()
		m.logger.Debug("create channel full, skipping",
			zap.String("backend", backend),
			zap.Int("channel-capacity", cap(m.createC)))
		return false
	}
}

// runGCIdleLoop periodically closes idle backends for all registered clients,
// and every gcInactiveCheckInterval (10s) also runs inactive cleanup so explicitly
// closed backends are removed without waiting for the 1-minute idle timeout.
// Uses a single goroutine: 1s tick for idle GC, 10s tick for inactive GC.
//
// Note: The check frequency is global, but each client's maxIdleDuration is still
// used to determine if a backend should be GC'd. This means:
// - Backends are checked more frequently (every 1s by default vs every maxIdleDuration)
// - But GC decision still respects each client's maxIdleDuration setting
// - This provides more timely cleanup without changing the GC semantics
func (m *clientGCManager) runGCIdleLoop() {
	defer m.wg.Done()
	m.logger.Debug("global GC idle loop started",
		zap.Duration("idle-check", m.gcIdleCheckInterval),
		zap.Duration("inactive-check", m.gcInactiveCheckInterval))
	defer m.logger.Debug("global GC idle loop stopped")

	idleTicker := time.NewTicker(m.gcIdleCheckInterval)
	inactiveTicker := time.NewTicker(m.gcInactiveCheckInterval)
	defer idleTicker.Stop()
	defer inactiveTicker.Stop()

	for {
		select {
		case <-m.stopC:
			return
		case <-idleTicker.C:
			m.doGCIdle()
		case <-inactiveTicker.C:
			m.doGCInactivePeriodic()
		}
	}
}

// doGCInactivePeriodic runs doRemoveInactiveAll for every registered client.
// Removes explicitly closed (LastActiveTime == zero) backends per remote; does not
// mix backends across different remotes (each remote has its own slice in backends map).
func (m *clientGCManager) doGCInactivePeriodic() {
	m.mu.RLock()
	clients := make([]*client, 0, len(m.clients))
	for c := range m.clients {
		clients = append(clients, c)
	}
	m.mu.RUnlock()

	for _, c := range clients {
		c.doRemoveInactiveAll()
	}
}

// runGCInactiveLoop handles inactive backend cleanup requests.
func (m *clientGCManager) runGCInactiveLoop() {
	defer m.wg.Done()
	m.logger.Debug("global GC inactive loop started")
	defer m.logger.Debug("global GC inactive loop stopped")

	// Update queue length periodically
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopC:
			return
		case <-ticker.C:
			// Update queue length gauge
			m.gcInactiveQueueLengthGauge.Set(float64(len(m.gcInactiveC)))
		case req, ok := <-m.gcInactiveC:
			if !ok {
				// Channel closed, exit
				return
			}
			// Update queue length after receiving
			m.gcInactiveQueueLengthGauge.Set(float64(len(m.gcInactiveC)))
			// Check if client is still registered
			m.mu.RLock()
			_, registered := m.clients[req.c]
			m.mu.RUnlock()
			if registered {
				// doRemoveInactive safely handles closed clients
				req.c.doRemoveInactive(req.remote)
				m.gcInactiveProcessedCounter.Inc()
			}
		}
	}
}

// runCreateLoop handles backend creation requests.
func (m *clientGCManager) runCreateLoop() {
	defer m.wg.Done()
	m.logger.Debug("global create loop started")
	defer m.logger.Debug("global create loop stopped")

	// Update queue length periodically
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopC:
			return
		case <-ticker.C:
			// Update queue length gauge
			m.createQueueLengthGauge.Set(float64(len(m.createC)))
		case req, ok := <-m.createC:
			if !ok {
				// Channel closed, exit
				return
			}
			// Update queue length after receiving
			m.createQueueLengthGauge.Set(float64(len(m.createC)))
			// Check if client is still registered
			m.mu.RLock()
			_, registered := m.clients[req.c]
			m.mu.RUnlock()
			if registered {
				// createBackendLocked is called under lock, check closed there
				req.c.mu.Lock()
				if !req.c.mu.closed {
					if _, err := req.c.createBackendLocked(req.backend); err != nil {
						req.c.logger.Error("create backend failed",
							zap.String("backend", req.backend),
							zap.Error(err))
					} else {
						m.createProcessedCounter.Inc()
					}
				}
				req.c.mu.Unlock()
			}
		}
	}
}

// doGCIdle performs idle backend cleanup for all registered clients.
// This uses Copy-on-Write pattern to minimize lock contention:
// 1. Acquire read lock and copy client list (fast, allows concurrent register/unregister)
// 2. Release lock before iterating
// 3. Call closeIdleBackends() without holding global lock (each client has its own lock)
//
// Thread-safety: closeIdleBackends() checks c.mu.closed under lock, so it's safe
// even if client is closed after we copy the list.
func (m *clientGCManager) doGCIdle() {
	// Copy-on-Write: acquire read lock, copy client list, release lock
	m.mu.RLock()
	clients := make([]*client, 0, len(m.clients))
	for c := range m.clients {
		clients = append(clients, c)
	}
	m.mu.RUnlock()

	// Iterate without holding global lock
	// closeIdleBackends() safely handles closed clients by checking c.mu.closed under lock
	totalCleaned := 0
	for _, c := range clients {
		if c.options.maxIdleDuration > 0 {
			cleaned := c.closeIdleBackends()
			totalCleaned += cleaned
		}
	}
	if totalCleaned > 0 {
		m.idleBackendsCleanedCounter.Add(float64(totalCleaned))
	}
}

// stop stops the global GC manager. This is mainly for testing purposes.
func (m *clientGCManager) stop() {
	m.mu.Lock()
	if !m.started {
		m.mu.Unlock()
		return
	}
	close(m.stopC)
	m.mu.Unlock()

	m.wg.Wait()

	m.mu.Lock()
	defer m.mu.Unlock()

	close(m.gcInactiveC)
	close(m.createC)

	m.started = false
	m.clients = make(map[*client]struct{})
	m.stopC = make(chan struct{})
	m.gcInactiveC = make(chan gcInactiveRequest, m.channelBufferSize)
	m.createC = make(chan createRequest, m.channelBufferSize)
}
