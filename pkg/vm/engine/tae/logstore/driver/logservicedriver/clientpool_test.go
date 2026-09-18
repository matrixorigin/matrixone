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
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
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
		ClientFactory: func() (logservice.Client, error) {
			if attempts.Add(1) == 1 {
				return nil, moerr.NewInternalErrorNoCtx("logservice client creation failed")
			}
			return newMockBackendClient(backend), nil
		},
	}
	cfg.fillDefaults()
	cfg.validate()

	pool := newClientPool(cfg)
	t.Cleanup(pool.Close)

	require.GreaterOrEqual(t, attempts.Load(), int32(2))
	require.Len(t, pool.clients, 1)
	require.NotNil(t, pool.clients[0])
	require.NotNil(t, pool.clients[0].wrapped)
	require.Nil(t, pool.clients[0].buf.Data)
}

type recordingPayloadClient struct {
	BackendClient
	sizes   []int
	records []logservice.LogRecord
	err     error
}

func (c *recordingPayloadClient) GetLogRecord(size int) logservice.LogRecord {
	c.sizes = append(c.sizes, size)
	return logservice.NewUserLogRecord(42, size)
}

func (c *recordingPayloadClient) Append(_ context.Context, record logservice.LogRecord) (uint64, error) {
	c.records = append(c.records, record.Clone())
	return uint64(len(c.records)), c.err
}

func TestClientPayloadAllocatedOnlyForAppend(t *testing.T) {
	backend := NewMockBackend()
	pool := newClientPool(&Config{
		ClientMaxCount: 1, ClientBufSize: 128, ClientRetryTimes: 1,
		ClientFactory: func() (logservice.Client, error) {
			return newMockBackendClient(backend), nil
		},
	})
	t.Cleanup(pool.Close)
	c, err := pool.Get()
	require.NoError(t, err)
	recorder := &recordingPayloadClient{BackendClient: c.wrapped}
	c.wrapped = recorder
	pool.Put(c) // An unused checkout must not create a record, even on return.
	require.Empty(t, recorder.sizes)
	require.Nil(t, c.buf.Data)

	for _, size := range []int{0, 16, 256, 8} {
		c, err = pool.Get()
		require.NoError(t, err)
		payload := bytes.Repeat([]byte{byte(size + 1)}, size)
		_, err = c.Append(context.Background(), LogEntry(payload), nil)
		require.NoError(t, err)
		record := recorder.records[len(recorder.records)-1]
		require.Equal(t, uint64(42), record.GetReplicaID())
		require.Equal(t, logservice.NewUserLogRecord(42, 0).GetUpdateType(), record.GetUpdateType())
		require.Equal(t, payload, record.Payload())
		pool.Put(c)
	}
	// Keep the existing growth policy after the first configured reserve.
	require.Equal(t, []int{128, 16, 256}, recorder.sizes)

	c, err = pool.Get()
	require.NoError(t, err)
	recorder.err = context.Canceled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = c.Append(ctx, LogEntry("cancel"), nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []byte("cancel"), recorder.records[len(recorder.records)-1].Payload())
	recorder.err = nil
	_, err = c.Append(context.Background(), LogEntry("retry"), nil)
	require.NoError(t, err)
	require.Equal(t, []int{128, 16, 256}, recorder.sizes)

	payload := bytes.Repeat([]byte{42}, DefaultRecordSize+1)
	_, err = c.Append(context.Background(), LogEntry(payload), nil)
	require.NoError(t, err)
	require.Equal(t, payload, recorder.records[len(recorder.records)-1].Payload())
	pool.Put(c)
	require.Len(t, c.buf.Payload(), DefaultRecordSize)
	require.Equal(t, []int{128, 16, 256, DefaultRecordSize + 1, DefaultRecordSize}, recorder.sizes)

	onFly, err := pool.GetOnFly()
	require.NoError(t, err)
	t.Cleanup(onFly.Close)
	require.Nil(t, onFly.buf.Data)
}

func TestNewClientPoolPanicsWhenFactoryAlwaysFail(t *testing.T) {
	cfg := &Config{
		ClientMaxCount:      1,
		ClientBufSize:       64,
		MaxTimeout:          time.Second,
		ClientRetryTimes:    1,
		ClientRetryInterval: time.Nanosecond,
		ClientRetryDuration: time.Nanosecond,
		ClientFactory: func() (logservice.Client, error) {
			return nil, moerr.NewInternalErrorNoCtx("always fail")
		},
	}
	cfg.fillDefaults()
	cfg.validate()

	require.PanicsWithError(t, moerr.NewInternalErrorNoCtx("always fail").Error(), func() {
		newClientPool(cfg)
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
		ClientFactory: func() (logservice.Client, error) {
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

	pool := newClientPool(cfg)
	t.Cleanup(pool.Close)

	var wg sync.WaitGroup
	wg.Add(1)
	var onFlyClient *wrappedClient
	var onFlyErr error
	go func() {
		defer wg.Done()
		onFlyClient, onFlyErr = pool.GetOnFly()
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
		ClientFactory: func() (logservice.Client, error) {
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

	pool := newClientPool(cfg)
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseFactory) })
		pool.Close()
	})

	result := make(chan struct {
		client *wrappedClient
		err    error
	}, 1)
	go func() {
		client, err := pool.GetOnFly()
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
