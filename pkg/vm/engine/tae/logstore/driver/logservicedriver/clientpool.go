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

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

const (
	DefaultRecordSize = mpool.MB

	DefaultRetryDuration = time.Minute * 5
	DefaultRetryTimes    = 300
	DefaultRetryInterval = time.Second
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

type writeTokenController struct {
	sync.Cond
	nextToken uint64
	bm        roaring64.Bitmap
	maxCount  uint64
}

func newTokenController(maxCount uint64) *writeTokenController {
	return &writeTokenController{
		nextToken: uint64(1),
		maxCount:  maxCount,
		Cond:      *sync.NewCond(new(sync.Mutex)),
	}
}

func (rc *writeTokenController) Putback(tokens ...uint64) {
	rc.L.Lock()
	defer rc.L.Unlock()
	for _, token := range tokens {
		rc.bm.Remove(token)
	}
	rc.Broadcast()
}

func (rc *writeTokenController) Apply() (token uint64) {
	rc.L.Lock()
	defer rc.L.Unlock()
	for {
		if rc.bm.IsEmpty() {
			token = rc.nextToken
			rc.nextToken++
			rc.bm.Add(token)
			return
		}
		minimum := rc.bm.Minimum()
		if rc.nextToken < rc.maxCount+minimum {
			token = rc.nextToken
			rc.nextToken++
			rc.bm.Add(token)
			return
		}
		// logutil.Infof("too much pendding writes: %d, %d, %d", rc.bm.Minimum(), rc.bm.Maximum(), rc.bm.GetCardinality())
		rc.Wait()
	}
}

type wrappedClient struct {
	wrapped    BackendClient
	buf        logservice.LogRecord
	pool       *clientPool
	writeToken uint64
}

func NewClient(
	factory LogServiceClientFactory,
	bufSize int,
	retryTimes int,
	retryInterval time.Duration,
	retryDuration time.Duration,
) (client *wrappedClient, err error) {
	client = new(wrappedClient)
	var (
		startTime = time.Now()
		wrapped   BackendClient
	)
	for i := 0; i < retryTimes; i++ {
		if wrapped, err = factory(); err == nil {
			break
		}
		logutil.Errorf("WAL-Replay failed to create log service client: %v", err)
		if time.Since(startTime) > retryDuration {
			break
		}
		time.Sleep(retryInterval)
	}
	if err != nil {
		return nil, err
	}
	client.wrapped = wrapped
	client.buf = wrapped.GetLogRecord(bufSize)
	return client, nil
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

	for {
		ctx, cancel := context.WithTimeoutCause(
			ctx, DefaultOneTryTimeout, timeoutCause,
		)
		psn, err = c.wrapped.Append(ctx, c.buf)
		cancel()
		if err == nil {
			break
		}
		retryTimes++
		if time.Since(now) > c.pool.cfg.MaxTimeout {
			break
		}
		time.Sleep(c.pool.cfg.RetryInterval(retryTimes))
	}
	return
}

func (c *wrappedClient) Putback() {
	if c.pool != nil {
		pool := c.pool
		if c.writeToken > 0 {
			pool.PutbackTokens(c.writeToken)
			c.writeToken = 0
		}
		c.pool = nil
		pool.Put(c)
	} else {
		c.wrapped.Close()
		c.wrapped = nil
	}
}

// ============================================================================
// ----------------------------- Client Pool ---------------------------------
// ============================================================================

type clientPool struct {
	cond   sync.Cond
	cfg    *Config
	closed bool

	clients []*wrappedClient

	// writeTokenController is used to control the write token
	// it controles the max write token issued and all finished write tokens
	// to avoid too much pendding writes
	// then we can only issue another 10 write token to avoid too much pendding writes
	// In the real world, the maxFinishedToken is always being updated and it is very
	// rare to reach the maxPendding
	writeTokenController *writeTokenController
}

func newClientPool(cfg *Config) *clientPool {
	maxPenddingWrites := cfg.ClientMaxCount
	if maxPenddingWrites < DefaultMaxClient {
		maxPenddingWrites = DefaultMaxClient
	}
	pool := &clientPool{
		cfg:                  cfg,
		clients:              make([]*wrappedClient, cfg.ClientMaxCount),
		cond:                 sync.Cond{L: new(sync.Mutex)},
		writeTokenController: newTokenController(uint64(maxPenddingWrites)),
	}

	type initResult struct {
		idx    int
		client *wrappedClient
		err    error
	}
	results := make(chan initResult, cfg.ClientMaxCount)
	var wg sync.WaitGroup
	for i := 0; i < cfg.ClientMaxCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			client, err := NewClient(cfg.ClientFactory, cfg.ClientBufSize, cfg.ClientRetryTimes, cfg.ClientRetryInterval, cfg.ClientRetryDuration)
			results <- initResult{
				idx:    idx,
				client: client,
				err:    err,
			}
		}(i)
	}
	wg.Wait()
	close(results)
	for res := range results {
		if res.err != nil {
			panic(res.err)
		}
		pool.clients[res.idx] = res.client
	}
	return pool
}

func (c *clientPool) Close() {
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

func (c *clientPool) GetOnFly() (*wrappedClient, error) {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()
	if c.closed {
		return nil, ErrClientPoolClosed
	}
	return NewClient(c.cfg.ClientFactory, c.cfg.ClientBufSize, c.cfg.ClientRetryTimes, c.cfg.ClientRetryInterval, c.cfg.ClientRetryDuration)
}

func (c *clientPool) Get() (client *wrappedClient, err error) {
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

func (c *clientPool) Put(client *wrappedClient) {
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

func (c *clientPool) GetWithWriteToken() (client *wrappedClient, err error) {
	if client, err = c.Get(); err != nil {
		return
	}
	token := c.writeTokenController.Apply()
	if token == 0 {
		logutil.Fatal("token is 0")
	}
	client.pool = c
	client.writeToken = token
	return
}

func (c *clientPool) PutbackTokens(tokens ...uint64) {
	c.writeTokenController.Putback(tokens...)
}
