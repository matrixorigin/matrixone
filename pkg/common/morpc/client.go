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
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"go.uber.org/zap"
)

// WithClientMaxBackendPerHost maximum number of connections per host
func WithClientMaxBackendPerHost(maxBackendsPerHost int) ClientOption {
	return func(c *client) {
		c.options.maxBackendsPerHost = maxBackendsPerHost
	}
}

// WithClientLogger set client logger
func WithClientLogger(logger *zap.Logger) ClientOption {
	return func(c *client) {
		c.logger = logger
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

// WithClientMaxBackendMaxIdleDuration set the maximum idle duration of the backend connection.
// Backend connection that exceed this time will be automatically closed. 0 means no idle time
// limit.
func WithClientMaxBackendMaxIdleDuration(value time.Duration) ClientOption {
	return func(c *client) {
		c.options.maxIdleDuration = value
	}
}

// WithClientEnableAutoCreateBackend enable client to automatically create a backend
// in the background, when the links in the connection pool are used, if the pool has
// not reached the maximum number of links, it will automatically create them in the
// background to improve the latency of link creation.
func WithClientEnableAutoCreateBackend() ClientOption {
	return func(c *client) {
		c.options.enableAutoCreate = true
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
		closed   bool
		backends map[string][]Backend
		ops      map[string]*op
	}

	options struct {
		maxBackendsPerHost int
		maxIdleDuration    time.Duration
		initBackends       []string
		initBackendCounts  []int
		enableAutoCreate   bool
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

	if err := c.stopper.RunTask(c.createTask); err != nil {
		return nil, err
	}
	if c.options.maxIdleDuration > 0 {
		if err := c.stopper.RunTask(c.gcIdleTask); err != nil {
			return nil, err
		}
	}
	if err := c.stopper.RunTask(c.gcInactiveTask); err != nil {
		return nil, err
	}
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
	if c.options.maxIdleDuration == 0 {
		c.options.maxIdleDuration = defaultMaxIdleDuration
	}
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
	for {
		b, err := c.getBackend(backend, false)
		if err != nil {
			return nil, err
		}

		f, err := b.Send(ctx, request)
		if err != nil && err == backendClosed {
			continue
		}
		return f, err
	}
}

func (c *client) NewStream(backend string, lock bool) (Stream, error) {
	for {
		b, err := c.getBackend(backend, lock)
		if err != nil {
			return nil, err
		}

		st, err := b.NewStream(lock)
		if err != nil && err == backendClosed {
			continue
		}
		return st, err
	}
}

func (c *client) Ping(ctx context.Context, backend string) error {
	if ctx == nil {
		panic("client Ping nil context")
	}
	for {
		b, err := c.getBackend(backend, false)
		if err != nil {
			return err
		}

		f, err := b.SendInternal(ctx, &flagOnlyMessage{flag: flagPing})
		if err != nil {
			if err == backendClosed {
				continue
			}
			return err
		}
		_, err = f.Get()
		f.Close()
		return err
	}
}

func (c *client) Close() error {
	c.mu.Lock()
	if c.mu.closed {
		c.mu.Unlock()
		return nil
	}
	c.mu.closed = true

	for _, backends := range c.mu.backends {
		for _, b := range backends {
			b.Close()
		}
	}
	c.mu.Unlock()

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
	c.mu.Lock()
	b, err := c.getBackendLocked(backend, lock)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	if b != nil {
		c.mu.Unlock()
		return b, nil
	}
	c.mu.Unlock()

	return c.createBackend(backend, lock)
}

func (c *client) getBackendLocked(backend string, lock bool) (Backend, error) {
	if c.mu.closed {
		return nil, moerr.NewClientClosedNoCtx()
	}
	defer func() {
		n := 0
		for _, backends := range c.mu.backends {
			n += len(backends)
		}
		c.metrics.poolSizeGauge.Set(float64(n))
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
				return nil, moerr.NewNoAvailableBackendNoCtx()
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

	select {
	case c.createC <- backend:
		return true
	default:
		return false
	}
}

func (c *client) gcIdleTask(ctx context.Context) {
	c.logger.Info("gc idle backends task started")
	defer c.logger.Error("gc idle backends task stopped")

	ticker := time.NewTicker(c.options.maxIdleDuration)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.closeIdleBackends()
		}
	}
}

func (c *client) triggerGCInactive(remote string) {
	select {
	case c.gcInactiveC <- remote:
		c.logger.Debug("try to remove all inactived backends",
			zap.String("remote", remote))
	default:
	}
}

func (c *client) gcInactiveTask(ctx context.Context) {
	c.logger.Debug("gc inactive backends task started")
	defer c.logger.Error("gc inactive backends task stopped")

	for {
		select {
		case <-ctx.Done():
			return
		case remote := <-c.gcInactiveC:
			c.doRemoveInactive(remote)
		}
	}
}

func (c *client) doRemoveInactive(remote string) {
	c.mu.Lock()
	defer c.mu.Unlock()
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
}

func (c *client) closeIdleBackends() {
	var idleBackends []Backend
	c.mu.Lock()
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
	c.mu.Unlock()

	for _, b := range idleBackends {
		b.Close()
	}
}

func (c *client) createTask(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case backend, ok := <-c.createC:
			if ok {
				c.mu.Lock()
				if _, err := c.createBackendLocked(backend); err != nil {
					c.logger.Error("create backend failed",
						zap.String("backend", backend),
						zap.Error(err))
				}
				c.mu.Unlock()
			}
		}
	}
}

func (c *client) createBackend(backend string, lock bool) (Backend, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	b, err := c.getBackendLocked(backend, lock)
	if err != nil {
		return nil, err
	}
	if b != nil {
		return b, nil
	}

	b, err = c.createBackendLocked(backend)
	if err != nil {
		return nil, err
	}
	if lock {
		b.Lock()
	}
	return b, nil
}

func (c *client) createBackendLocked(backend string) (Backend, error) {
	if !c.canCreateLocked(backend) {
		return nil, moerr.NewNoAvailableBackendNoCtx()
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

type op struct {
	seq uint64
}

func (o *op) next() uint64 {
	return atomic.AddUint64(&o.seq, 1)
}
