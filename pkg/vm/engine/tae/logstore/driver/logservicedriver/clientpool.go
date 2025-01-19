// Copyright 2021 Matrix Origin
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

package logservicedriver

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

const (
	DefaultRecordSize = mpool.MB
)

var ErrNoClientAvailable = moerr.NewInternalErrorNoCtx("no client available")
var ErrClientPoolClosed = moerr.NewInternalErrorNoCtx("client pool closed")

type clientConfig struct {
	bufSize    int
	factory    LogServiceClientFactory
	maxTimeout time.Duration
}

type MockBackend interface {
	Read(
		ctx context.Context, firstPSN, maxSize uint64,
	) ([]logservice.LogRecord, uint64, error)
	GetTruncatedLsn(ctx context.Context) (uint64, error)

	Append(
		ctx context.Context,
		record logservice.LogRecord,
	) (psn uint64, err error)
	Truncate(ctx context.Context, psn uint64) error
}

type BackendClient interface {
	io.Closer

	GetLogRecord(size int) logservice.LogRecord
	Read(
		ctx context.Context, firstPSN, maxSize uint64,
	) ([]logservice.LogRecord, uint64, error)
	GetTruncatedLsn(ctx context.Context) (uint64, error)

	Append(
		ctx context.Context,
		record logservice.LogRecord,
	) (psn uint64, err error)
	Truncate(ctx context.Context, psn uint64) error
}

type wrappedClient struct {
	c      BackendClient
	record logservice.LogRecord
	id     int
}

func newClient(
	factory LogServiceClientFactory, bufSize int,
) *wrappedClient {
	var (
		err    error
		client BackendClient
	)
	if client, err = factory(); err != nil {
		panic(err)
	}
	return &wrappedClient{
		c:      client,
		record: client.GetLogRecord(bufSize),
	}
}

func (c *wrappedClient) Close() {
	c.c.Close()
}

func (c *wrappedClient) tryResize(size int) {
	if len(c.record.Payload()) < size {
		c.record = c.c.GetLogRecord(size)
	}
}

func (c *wrappedClient) Append(
	ctx context.Context,
	e LogEntry,
	timeout time.Duration,
	maxRetry int,
	timeoutCause error,
) (psn uint64, err error) {
	if e.Size() > len(c.record.Payload()) {
		c.record = c.c.GetLogRecord(e.Size())
	} else {
		c.record.ResizePayload(e.Size())
	}
	c.record.SetPayload(e[:])

	var (
		retryTimes int
		now        = time.Now()
	)

	defer func() {
		if err != nil || retryTimes > 0 || time.Since(now) > time.Second*5 {
			logger := logutil.Info
			if err != nil {
				logger = logutil.Error
			}
			logger(
				"WAL-Commit-Append",
				zap.Uint64("psn", psn),
				zap.Int("size", len(e[:])),
				zap.Int("retry-times", retryTimes),
				zap.Duration("duration", time.Since(now)),
				zap.Error(err),
			)
		}
	}()

	for ; retryTimes < maxRetry+1; retryTimes++ {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeoutCause(
			ctx, timeout, moerr.CauseDriverAppender1,
		)
		psn, err = c.c.Append(ctx, c.record)
		cancel()
		if err == nil {
			break
		}
	}
	return
}

type clientpool struct {
	maxCount int
	count    int

	closed atomic.Int32

	freeClients   []*wrappedClient
	clientFactory func() *wrappedClient
	closefn       func(*wrappedClient)
	mu            sync.Mutex
	cfg           *clientConfig
}

func newClientPool(size int, cfg *clientConfig) *clientpool {
	pool := &clientpool{
		maxCount:    size,
		freeClients: make([]*wrappedClient, size),
		cfg:         cfg,
	}
	pool.clientFactory = pool.createClientFactory(cfg)
	pool.closefn = pool.onClose

	for i := 0; i < size; i++ {
		pool.freeClients[i] = pool.clientFactory()
	}
	return pool
}

func (c *clientpool) createClientFactory(cfg *clientConfig) func() *wrappedClient {
	return func() *wrappedClient {
		c.count++
		client := newClient(cfg.factory, cfg.bufSize)
		client.id = c.count
		return client
	}
}

func (c *clientpool) Close() {
	if c.IsClosed() {
		return
	}
	c.mu.Lock()
	if c.IsClosed() {
		c.mu.Unlock()
		return
	}
	var wg sync.WaitGroup
	for _, client := range c.freeClients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.closefn(client)
		}()
	}
	wg.Wait()
	c.closed.Store(1)
	c.mu.Unlock()
}

func (c *clientpool) onClose(client *wrappedClient) {
	client.Close()
}

func (c *clientpool) Get() (*wrappedClient, error) {
	if c.IsClosed() {
		return nil, ErrClientPoolClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.IsClosed() {
		return nil, ErrClientPoolClosed
	}
	if len(c.freeClients) == 0 {
		if c.count == c.maxCount {
			return nil, ErrNoClientAvailable
		}
		return c.clientFactory(), nil
	}
	client := c.freeClients[len(c.freeClients)-1]
	c.freeClients = c.freeClients[:len(c.freeClients)-1]
	return client, nil
}

func (c *clientpool) IsClosed() bool {
	return c.closed.Load() == 1
}

func (c *clientpool) Put(client *wrappedClient) {
	if len(client.record.Payload()) > DefaultRecordSize {
		client.record = client.c.GetLogRecord(DefaultRecordSize)
	}
	if c.IsClosed() {
		c.closefn(client)
		return
	}
	c.mu.Lock()
	if c.IsClosed() {
		c.closefn(client)
		c.mu.Unlock()
		return
	}
	c.count--
	c.freeClients = append(c.freeClients, client)
	defer c.mu.Unlock()
}
