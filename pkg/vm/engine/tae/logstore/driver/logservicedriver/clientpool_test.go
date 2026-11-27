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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/stretchr/testify/require"
)

func TestNewClientPoolRetriesThenSucceeds(t *testing.T) {
	var attempts atomic.Int32
	backend := NewMockBackend()

	cfg := &Config{
		ClientMaxCount:      1,
		ClientBufSize:       128,
		MaxTimeout:          time.Second,
		ClientRetryTimes:    2,
		ClientRetryInterval: time.Millisecond,       // 1ms between retries
		ClientRetryDuration: 100 * time.Millisecond, // 100ms budget (enough for any CI)
		ClientFactory: func(ctx context.Context) (logservice.Client, error) {
			if attempts.Add(1) == 1 {
				return nil, moerr.NewInternalErrorNoCtx("logservice client creation failed")
			}
			return newMockBackendClient(backend), nil
		},
	}
	cfg.fillDefaults()
	cfg.validate()

	pool, err := openClientPool(t.Context(), cfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	require.GreaterOrEqual(t, attempts.Load(), int32(2))
	require.Len(t, pool.clients, 1)
	require.NotNil(t, pool.clients[0])
	require.NotNil(t, pool.clients[0].buf)
}

func TestOpenClientPoolReturnsLeaseRejection(t *testing.T) {
	cfg := &Config{
		ClientMaxCount:      2,
		ClientBufSize:       64,
		MaxTimeout:          time.Second,
		ClientRetryTimes:    3,
		ClientRetryInterval: time.Nanosecond,
		ClientRetryDuration: time.Nanosecond,
	}
	var calls atomic.Int32
	var closed atomic.Int32
	cfg.ClientFactory = func(ctx context.Context) (logservice.Client, error) {
		if calls.Add(1) == 1 {
			return &closeCountingClient{Client: newMockBackendClient(NewMockBackend()), closed: &closed}, nil
		}
		return nil, moerr.NewNotLeaseHolder(context.Background(), 2)
	}
	cfg.fillDefaults()
	cfg.validate()

	pool, err := openClientPool(t.Context(), cfg)
	require.Nil(t, pool)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotLeaseHolder))
	require.Equal(t, int32(2), calls.Load())
	require.Equal(t, int32(1), closed.Load())
}

func TestOpenClientPoolDoesNotRetryUncertainLease(t *testing.T) {
	var attempts atomic.Int32
	claimErr := errors.Join(logservice.ErrTNLeaseClaimUncertain, context.DeadlineExceeded)
	cfg := NewConfig("", WithConfigOptMaxClient(1),
		WithConfigOptClientFactory(func(context.Context) (logservice.Client, error) {
			attempts.Add(1)
			return nil, claimErr
		}))
	cfg.ClientRetryTimes = 3
	cfg.ClientRetryInterval = time.Millisecond
	cfg.ClientRetryDuration = time.Second

	pool, err := openClientPool(t.Context(), &cfg)
	require.Nil(t, pool)
	require.ErrorIs(t, err, claimErr)
	require.Equal(t, int32(1), attempts.Load())
}

func TestOpenClientPoolCancelsRetryBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var attempts atomic.Int32
		cfg := NewConfig("", WithConfigOptMaxClient(1),
			WithConfigOptClientFactory(func(context.Context) (logservice.Client, error) {
				attempts.Add(1)
				return nil, moerr.NewNoHAKeeper(ctx)
			}))
		cfg.ClientRetryInterval = time.Hour
		done := make(chan error, 1)
		go func() {
			_, err := openClientPool(ctx, &cfg)
			done <- err
		}()
		synctest.Wait()
		require.Equal(t, int32(1), attempts.Load())
		cancel()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("WAL retry backoff did not stop")
		}
	})
}

func TestGetOnFlyDoesNotBlockPoolGetWhileCreatingClient(t *testing.T) {
	backend := NewMockBackend()
	var attempts atomic.Int32
	factoryEntered := make(chan struct{})
	releaseFactory := make(chan struct{})
	var releaseOnce sync.Once

	cfg := &Config{
		ClientMaxCount:      1,
		ClientBufSize:       128,
		MaxTimeout:          time.Second,
		ClientRetryTimes:    1,
		ClientRetryInterval: time.Nanosecond,
		ClientRetryDuration: time.Second,
		ClientFactory: func(ctx context.Context) (logservice.Client, error) {
			if attempts.Add(1) == 1 {
				return newMockBackendClient(backend), nil
			}
			close(factoryEntered)
			<-releaseFactory
			return newMockBackendClient(backend), nil
		},
	}
	cfg.fillDefaults()
	cfg.validate()

	pool, err := openClientPool(t.Context(), cfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	var wg sync.WaitGroup
	wg.Add(1)
	var onFlyClient *wrappedClient
	var onFlyErr error
	go func() {
		defer wg.Done()
		onFlyClient, onFlyErr = pool.GetOnFly(t.Context())
	}()
	defer func() {
		releaseOnce.Do(func() { close(releaseFactory) })
		wg.Wait()
	}()

	select {
	case <-factoryEntered:
	case <-time.After(time.Second):
		t.Fatal("GetOnFly did not enter the blocking client factory")
	}

	type getResult struct {
		client *wrappedClient
		err    error
	}
	got := make(chan getResult, 1)
	go func() {
		client, err := pool.Get()
		got <- getResult{client: client, err: err}
	}()

	var pooled *wrappedClient
	select {
	case result := <-got:
		require.NoError(t, result.err)
		pooled = result.client
	case <-time.After(100 * time.Millisecond):
		t.Fatal("pool Get blocked while GetOnFly was creating a client")
	}
	pooled.Putback()

	releaseOnce.Do(func() { close(releaseFactory) })
	wg.Wait()
	require.NoError(t, onFlyErr)
	require.NotNil(t, onFlyClient)
	onFlyClient.Close()
}

func TestGetOnFlyClosesCreatedClientWhenPoolClosesConcurrently(t *testing.T) {
	backend := NewMockBackend()
	var attempts atomic.Int32
	var closed atomic.Int32
	factoryEntered := make(chan struct{})
	releaseFactory := make(chan struct{})
	var releaseOnce sync.Once

	cfg := &Config{
		ClientMaxCount:      1,
		ClientBufSize:       128,
		MaxTimeout:          time.Second,
		ClientRetryTimes:    1,
		ClientRetryInterval: time.Nanosecond,
		ClientRetryDuration: time.Second,
		ClientFactory: func(ctx context.Context) (logservice.Client, error) {
			if attempts.Add(1) == 1 {
				return newMockBackendClient(backend), nil
			}
			close(factoryEntered)
			<-releaseFactory
			return &closeCountingClient{
				Client: newMockBackendClient(backend),
				closed: &closed,
			}, nil
		},
	}
	cfg.fillDefaults()
	cfg.validate()

	pool, err := openClientPool(t.Context(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseFactory) })
		pool.Close()
	})

	result := make(chan struct {
		client *wrappedClient
		err    error
	}, 1)
	go func() {
		client, err := pool.GetOnFly(t.Context())
		result <- struct {
			client *wrappedClient
			err    error
		}{client: client, err: err}
	}()

	select {
	case <-factoryEntered:
	case <-time.After(time.Second):
		t.Fatal("GetOnFly did not enter the blocking client factory")
	}

	pool.Close()
	releaseOnce.Do(func() { close(releaseFactory) })

	select {
	case res := <-result:
		require.ErrorIs(t, res.err, ErrClientPoolClosed)
		require.Nil(t, res.client)
	case <-time.After(time.Second):
		t.Fatal("GetOnFly did not return after the pool was closed")
	}
	require.Equal(t, int32(1), closed.Load())
}

type closeCountingClient struct {
	logservice.Client
	closed *atomic.Int32
}

func (c *closeCountingClient) Close() error {
	c.closed.Add(1)
	return c.Client.Close()
}
