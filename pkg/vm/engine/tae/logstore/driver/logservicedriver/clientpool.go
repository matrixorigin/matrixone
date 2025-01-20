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

type MockBackend interface {
	Read(
		ctx context.Context, firstPSN, maxSize uint64,
	) ([]logservice.LogRecord, uint64, error)
	GetTruncatedLsn(ctx context.Context) (uint64, error)

	Append(
		ctx context.Context,
		buf logservice.LogRecord,
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
		buf logservice.LogRecord,
	) (psn uint64, err error)
	Truncate(ctx context.Context, psn uint64) error
}

type wrappedClient struct {
	wrapped BackendClient
	buf     logservice.LogRecord
	pool    *clientpool
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
		wrapped: client,
		buf:     client.GetLogRecord(bufSize),
	}
}

func (c *wrappedClient) Close() {
	c.wrapped.Close()
}

func (c *wrappedClient) Append(
	ctx context.Context,
	e LogEntry,
	timeoutCause error,
) (psn uint64, err error) {
	if e.Size() > len(c.buf.Payload()) {
		c.buf = c.wrapped.GetLogRecord(e.Size())
	} else {
		c.buf.ResizePayload(e.Size())
	}
	c.buf.SetPayload(e[:])
	// logutil.Infof("Client Append: %s", e.String())

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

	for ; retryTimes < c.pool.cfg.MaxRetryCount+1; retryTimes++ {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeoutCause(
			ctx, c.pool.cfg.MaxTimeout, timeoutCause,
		)
		psn, err = c.wrapped.Append(ctx, c.buf)
		cancel()
		if err == nil {
			break
		}
		time.Sleep(c.pool.cfg.RetryInterval())
	}
	return
}

func (c *wrappedClient) Putback() {
	if c.pool != nil {
		pool := c.pool
		c.pool = nil
		pool.Put(c)
	} else {
		c.wrapped.Close()
		c.wrapped = nil
	}
}

type clientpool struct {
	closed bool
	cfg    *Config

	cond    sync.Cond
	clients []*wrappedClient
}

func newClientPool(cfg *Config) *clientpool {
	pool := &clientpool{
		cfg:     cfg,
		clients: make([]*wrappedClient, cfg.ClientMaxCount),
		cond:    sync.Cond{L: new(sync.Mutex)},
	}

	for i := 0; i < cfg.ClientMaxCount; i++ {
		pool.clients[i] = newClient(cfg.ClientFactory, cfg.ClientBufSize)
	}
	return pool
}

func (c *clientpool) Close() {
	var wg sync.WaitGroup
	c.cond.L.Lock()
	defer c.cond.L.Unlock()
	// pool is already closed
	if c.closed {
		return
	}

	for _, client := range c.clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client.Close()
		}()
	}
	wg.Wait()
	c.clients = nil
	c.closed = true
	c.cond.Broadcast()
}

func (c *clientpool) GetOnFly() (*wrappedClient, error) {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()
	if c.closed {
		return nil, ErrClientPoolClosed
	}
	client := newClient(c.cfg.ClientFactory, c.cfg.ClientBufSize)
	return client, nil
}

func (c *clientpool) Get() (client *wrappedClient, err error) {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()
	for {
		if c.closed {
			err = ErrClientPoolClosed
			return
		}
		if len(c.clients) > 0 {
			client = c.clients[len(c.clients)-1]
			client.pool = c
			c.clients = c.clients[:len(c.clients)-1]
			return
		}
		c.cond.Wait()
	}
}

func (c *clientpool) Put(client *wrappedClient) {
	if len(client.buf.Payload()) > DefaultRecordSize {
		client.buf = client.wrapped.GetLogRecord(DefaultRecordSize)
	}
	c.cond.L.Lock()
	defer c.cond.L.Unlock()
	if c.closed {
		client.Close()
		return
	}
	c.clients = append(c.clients, client)
	c.cond.Broadcast()
}
