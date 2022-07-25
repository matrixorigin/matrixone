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

	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
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

// WithClientDisableCreateTask set disable create backend task. The client has a task to create
// backends asynchronously, but when the client finds that there are not enough backends and
// has busy backend, it will automatically create backends until maxBackendsPerHost is reached.
func WithClientDisableCreateTask() ClientOption {
	return func(c *client) {
		c.options.disableCreateTask = true
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

type client struct {
	logger  *zap.Logger
	stopper *stopper.Stopper
	factory BackendFactory
	createC chan string

	mu struct {
		sync.RWMutex
		closed   bool
		backends map[string][]Backend
		ops      map[string]*op
	}

	options struct {
		maxBackendsPerHost int
		maxIdleDuration    time.Duration
		disableCreateTask  bool
		initBackends       []string
		initBackendCounts  []int
	}
}

// NewClient create rpc client with options
func NewClient(factory BackendFactory, options ...ClientOption) (RPCClient, error) {
	c := &client{
		factory: factory,
	}
	c.mu.backends = make(map[string][]Backend)
	c.mu.ops = make(map[string]*op)

	for _, opt := range options {
		opt(c)
	}
	c.adjust()
	c.stopper = stopper.NewStopper("rpc client", stopper.WithLogger(c.logger))

	if err := c.maybeInitBackends(); err != nil {
		c.Close()
		return nil, err
	}

	if !c.options.disableCreateTask {
		if err := c.stopper.RunTask(c.createTask); err != nil {
			return nil, err
		}
	}
	if c.options.maxIdleDuration > 0 {
		if err := c.stopper.RunTask(c.gcIdleTask); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func (c *client) adjust() {
	c.logger = logutil.Adjust(c.logger)
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

func (c *client) Send(ctx context.Context, backend string, request Message, opts SendOptions) (*Future, error) {
	b, err := c.getBackend(backend)
	if err != nil {
		return nil, err
	}

	f, err := b.Send(ctx, request, opts)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (c *client) NewStream(backend string) (Stream, error) {
	b, err := c.getBackend(backend)
	if err != nil {
		return nil, err
	}

	return b.NewStream()
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

func (c *client) getBackend(backend string) (Backend, error) {
	c.mu.RLock()
	b, err := c.getBackendLocked(backend)
	if err != nil {
		c.mu.RUnlock()
		return nil, err
	}
	if b != nil {
		c.mu.RUnlock()
		return b, nil
	}
	c.mu.RUnlock()

	return c.createBackend(backend)
}

func (c *client) getBackendLocked(backend string) (Backend, error) {
	if c.mu.closed {
		return nil, errClientClosed
	}

	if backends, ok := c.mu.backends[backend]; ok {
		n := uint64(len(backends))
		seq := c.mu.ops[backend].next()
		b := backends[seq%n]
		c.maybeCreateLocked(backend)
		return b, nil
	}
	return nil, nil
}

func (c *client) maybeCreateLocked(backend string) bool {
	if c.options.disableCreateTask {
		return false
	}

	if len(c.mu.backends[backend]) == 0 {
		return c.tryCreate(backend)
	}

	if !c.canCreateLocked(backend) {
		return false
	}

	for _, b := range c.mu.backends[backend] {
		if b.Busy() {
			return c.tryCreate(backend)
		}
	}
	return false
}

func (c *client) tryCreate(backend string) bool {
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

func (c *client) closeIdleBackends() {
	var idleBackends []Backend
	c.mu.Lock()
	for k, backends := range c.mu.backends {
		var newBackends []Backend
		for _, b := range backends {
			if time.Since(b.LastActiveTime()) > c.options.maxIdleDuration {
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

func (c *client) createBackend(backend string) (Backend, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	b, err := c.getBackendLocked(backend)
	if err != nil {
		return nil, err
	}
	if b != nil {
		return b, nil
	}

	return c.createBackendLocked(backend)
}

func (c *client) createBackendLocked(backend string) (Backend, error) {
	if !c.canCreateLocked(backend) {
		return nil, errNoAvailableBackend
	}

	b, err := c.factory.Create(backend)
	if err != nil {
		c.logger.Error("create backend failed",
			zap.String("backend", backend),
			zap.Error(err))
		return nil, err
	}
	c.mu.backends[backend] = append(c.mu.backends[backend], b)
	if _, ok := c.mu.ops[backend]; !ok {
		c.mu.ops[backend] = &op{}
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
