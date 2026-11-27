// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestBackendWaitDeadlineKeepsEarliestNestedBudget(t *testing.T) {
	early := time.Now().Add(time.Second)
	late := early.Add(time.Second)
	parent := WithBackendWaitDeadline(context.Background(), early)
	got, ok := BackendWaitDeadline(WithBackendWaitDeadline(parent, late))
	require.True(t, ok)
	require.Equal(t, early, got)
	got, ok = BackendWaitDeadline(WithBackendWaitDeadline(WithBackendWaitDeadline(context.Background(), late), early))
	require.True(t, ok)
	require.Equal(t, early, got)
	got, ok = BackendWaitDeadline(WithBackendWaitDeadline(parent, time.Time{}))
	require.True(t, ok)
	require.Equal(t, early, got)
}

func TestBackendWaitDeadlineBoundsUnavailableBackend(t *testing.T) {
	factory := &lateSuccessfulBackendFactory{entered: make(chan struct{}), release: make(chan struct{})}
	rpcClient, err := NewClient(t.Name(), factory, WithClientEnableAutoCreateBackend(),
		WithClientAutoCreateWaitTimeout(0), WithClientDisableCircuitBreaker(), WithClientLogger(zap.NewNop()))
	require.NoError(t, err)
	t.Cleanup(func() { close(factory.release); require.NoError(t, rpcClient.Close()) })
	parent, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ctx := WithBackendWaitDeadline(parent, time.Now().Add(200*time.Millisecond))
	start := time.Now()
	type result struct {
		future *Future
		err    error
	}
	done := make(chan result, 1)
	go func() {
		future, err := rpcClient.Send(ctx, "unavailable-backend", newTestMessage(1))
		done <- result{future, err}
	}()
	select {
	case <-factory.entered:
	case got := <-done:
		if got.future != nil {
			got.future.Close()
		}
		t.Fatal("backend wait ended before the factory started")
	case <-time.After(time.Second):
		t.Fatal("backend factory did not start")
	}
	var got result
	select {
	case got = <-done:
	case <-time.After(time.Second):
		t.Fatal("backend wait exceeded its budget")
	}
	require.Nil(t, got.future)
	require.True(t, moerr.IsMoErrCode(got.err, moerr.ErrBackendCannotConnect), "%v", got.err)
	require.Less(t, time.Since(start), time.Second)
	require.NoError(t, parent.Err(), "backend budget must not cancel the caller")
}

func TestBackendWaitDeadlinePreservesCallerCancellation(t *testing.T) {
	factory := &lateSuccessfulBackendFactory{entered: make(chan struct{}), release: make(chan struct{})}
	rpcClient, err := NewClient(t.Name(), factory, WithClientEnableAutoCreateBackend(),
		WithClientAutoCreateWaitTimeout(0), WithClientDisableCircuitBreaker(), WithClientLogger(zap.NewNop()))
	require.NoError(t, err)
	t.Cleanup(func() { close(factory.release); require.NoError(t, rpcClient.Close()) })
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := WithBackendWaitDeadline(parent, time.Now().Add(2*time.Second))
	done := make(chan error, 1)
	go func() {
		future, err := rpcClient.Send(ctx, "unavailable-backend", newTestMessage(1))
		if future != nil {
			future.Close()
		}
		done <- err
	}()
	select {
	case <-factory.entered:
	case <-time.After(time.Second):
		t.Fatal("backend factory did not start")
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("backend wait ignored caller cancellation")
	}
}

type backendWaitControl struct {
	*testBackend
	started chan struct{}
	release chan struct{}
}

func (b *backendWaitControl) Send(ctx context.Context, request Message) (*Future, error) {
	close(b.started)
	<-b.release
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return b.testBackend.Send(ctx, request)
}

func TestBackendWaitDeadlineDoesNotCancelHealthyBackendRequest(t *testing.T) {
	rpcClient, err := NewClient(t.Name(), newTestBackendFactory(),
		WithClientDisableAutoCreateBackend(), WithClientDisableCircuitBreaker())
	require.NoError(t, err)
	c := rpcClient.(*client)
	backend := &backendWaitControl{testBackend: &testBackend{activeTime: time.Now()},
		started: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(backend.release) }) }
	c.mu.Lock()
	c.mu.backends["remote"] = []Backend{backend}
	c.mu.ops["remote"] = &op{}
	c.mu.Unlock()
	defer func() { release(); require.NoError(t, rpcClient.Close()) }()

	parent, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ctx := WithBackendWaitDeadline(parent, time.Now().Add(50*time.Millisecond))
	type result struct {
		future *Future
		err    error
	}
	done := make(chan result, 1)
	go func() { f, err := rpcClient.Send(ctx, "remote", newTestMessage(1)); done <- result{f, err} }()
	select {
	case <-backend.started:
	case <-time.After(time.Second):
		t.Fatal("healthy backend was not selected")
	}
	time.Sleep(100 * time.Millisecond)
	release()
	select {
	case got := <-done:
		require.NoError(t, got.err)
		require.NotNil(t, got.future)
		got.future.Close()
	case <-time.After(time.Second):
		t.Fatal("healthy backend request did not finish")
	}
}
