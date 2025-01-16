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
	cancelDuration        time.Duration
	recordSize            int
	clientFactory         LogServiceClientFactory
	GetClientRetryTimeOut time.Duration
	retryDuration         time.Duration
}

type wrappedClient struct {
	c      logservice.Client
	record logservice.LogRecord
	id     int
}

func newClient(factory LogServiceClientFactory, recordSize int, retryDuration time.Duration) *wrappedClient {
	logserviceClient, err := factory()
	if err != nil {
		RetryWithTimeout(retryDuration, func() (shouldReturn bool) {
			logserviceClient, err = factory()
			return err == nil
		})
		if err != nil {
			panic(err)
		}
	}
	c := &wrappedClient{
		c:      logserviceClient,
		record: logserviceClient.GetLogRecord(recordSize),
	}
	return c
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
	c.tryResize(len(e[:]))
	copy(c.record.Payload(), e[:])
	c.record.ResizePayload(len(e[:]))

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
	maxCount   int
	count      int
	getTimeout time.Duration

	closed atomic.Int32

	freeClients   []*wrappedClient
	clientFactory func() *wrappedClient
	closefn       func(*wrappedClient)
	mu            sync.Mutex
	cfg           *clientConfig
}

func newClientPool(maxsize int, cfg *clientConfig) *clientpool {
	pool := &clientpool{
		maxCount:    maxsize,
		getTimeout:  cfg.GetClientRetryTimeOut,
		freeClients: make([]*wrappedClient, maxsize),
		mu:          sync.Mutex{},
		cfg:         cfg,
	}
	pool.clientFactory = pool.createClientFactory(cfg)
	pool.closefn = pool.onClose

	for i := 0; i < maxsize; i++ {
		pool.freeClients[i] = pool.clientFactory()
	}
	return pool
}

func (c *clientpool) createClientFactory(cfg *clientConfig) func() *wrappedClient {
	return func() *wrappedClient {
		c.count++
		client := newClient(cfg.clientFactory, cfg.recordSize, cfg.retryDuration)
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
	for _, client := range c.freeClients {
		c.closefn(client)
	}
	c.closed.Store(1)
	c.mu.Unlock()
}

func (c *clientpool) onClose(client *wrappedClient) {
	client.Close()
}

// func (c *clientpool) GetAndWait() (*wrappedClient, error) {
// 	client, err := c.Get()
// 	if err == ErrNoClientAvailable {
// 		retryWithTimeout(c.getTimeout, func() (shouldReturn bool) {
// 			client, err = c.Get()
// 			return err != ErrNoClientAvailable
// 		})
// 	}
// 	return client, nil
// }

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
