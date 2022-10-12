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
	"github.com/matrixorigin/matrixone/pkg/logservice"
)

var ErrNoClientAvailable = moerr.NewInternalError("no client available")
var ErrClientPoolClosed = moerr.NewInternalError("client pool closed")

type clientConfig struct {
	cancelDuration         time.Duration
	recordSize             int
	logserviceClientConfig *logservice.ClientConfig
	GetClientRetryTimeOut  time.Duration
	retryDuration          time.Duration
}

type clientWithRecord struct {
	c      logservice.Client
	record logservice.LogRecord
	id     int
}

func newClient(cancelDuration, retryDuration time.Duration, recordsize int, cfg *logservice.ClientConfig) *clientWithRecord {
	ctx, cancel := context.WithTimeout(context.Background(), cancelDuration)
	logserviceClient, err := logservice.NewClient(ctx, *cfg)
	cancel()
	if err != nil {
		err = RetryWithTimeout(retryDuration, func() (shouldReturn bool) {
			ctx, cancel := context.WithTimeout(context.Background(), cancelDuration)
			logserviceClient, err = logservice.NewClient(ctx, *cfg)
			cancel()
			return err == nil
		})
		if err != nil {
			panic(err)
		}
	}
	c := &clientWithRecord{
		c:      logserviceClient,
		record: logserviceClient.GetLogRecord(recordsize),
	}
	return c
}

func (c *clientWithRecord) Close() {
	c.c.Close()
}

func (c *clientWithRecord) TryResize(size int) {
	if c.record.Size() < size {
		c.record = c.c.GetLogRecord(size)
	}
}

type clientpool struct {
	maxCount   int
	count      int
	getTimeout time.Duration

	closed atomic.Int32

	freeClients   []*clientWithRecord
	clientFactory func() *clientWithRecord
	closefn       func(*clientWithRecord)
	mu            sync.Mutex
}

func newClientPool(maxsize, initsize int, cfg *clientConfig) *clientpool {
	pool := &clientpool{
		maxCount:    maxsize,
		getTimeout:  cfg.GetClientRetryTimeOut,
		freeClients: make([]*clientWithRecord, initsize),
		mu:          sync.Mutex{},
	}
	pool.clientFactory = pool.newClientFactory(cfg)
	pool.closefn = pool.onClose

	for i := 0; i < initsize; i++ {
		pool.freeClients[i] = pool.clientFactory()
	}
	return pool
}

func (c *clientpool) newClientFactory(cfg *clientConfig) func() *clientWithRecord {
	return func() *clientWithRecord {
		c.count++
		client := newClient(cfg.retryDuration, cfg.cancelDuration, cfg.recordSize, cfg.logserviceClientConfig)
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

func (c *clientpool) onClose(client *clientWithRecord) {
	client.Close()
}

// func (c *clientpool) GetAndWait() (*clientWithRecord, error) {
// 	client, err := c.Get()
// 	if err == ErrNoClientAvailable {
// 		retryWithTimeout(c.getTimeout, func() (shouldReturn bool) {
// 			client, err = c.Get()
// 			return err != ErrNoClientAvailable
// 		})
// 	}
// 	return client, nil
// }

func (c *clientpool) Get() (*clientWithRecord, error) {
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

func (c *clientpool) Put(client *clientWithRecord) {
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
