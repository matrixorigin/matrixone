// Copyright 2026 Matrix Origin
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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type cancellableCreateFactory struct {
	entered   chan struct{}
	release   chan struct{}
	enterOnce sync.Once
	active    atomic.Int32
	cancelled atomic.Int32
}

type cancellationStressFactory struct {
	active atomic.Int32
	max    atomic.Int32
}

type factoryDeadlineError struct{}

func (*factoryDeadlineError) Create(
	string,
	...BackendOption,
) (Backend, error) {
	return nil, context.DeadlineExceeded
}

func (f *cancellationStressFactory) Create(
	address string,
	options ...BackendOption,
) (Backend, error) {
	return f.CreateWithContext(context.Background(), address, options...)
}

func (f *cancellationStressFactory) CreateWithContext(
	ctx context.Context,
	_ string,
	_ ...BackendOption,
) (Backend, error) {
	active := f.active.Add(1)
	defer f.active.Add(-1)
	for {
		old := f.max.Load()
		if active <= old || f.max.CompareAndSwap(old, active) {
			break
		}
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(time.Millisecond):
		return &testBackend{activeTime: time.Now()}, nil
	}
}

func newCancellableCreateFactory() *cancellableCreateFactory {
	return &cancellableCreateFactory{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (f *cancellableCreateFactory) begin() {
	f.active.Add(1)
	f.enterOnce.Do(func() { close(f.entered) })
}

func (f *cancellableCreateFactory) Create(
	string,
	...BackendOption,
) (Backend, error) {
	f.begin()
	<-f.release
	f.active.Add(-1)
	return &testBackend{activeTime: time.Now()}, nil
}

func (f *cancellableCreateFactory) CreateWithContext(
	ctx context.Context,
	_ string,
	_ ...BackendOption,
) (Backend, error) {
	f.begin()
	select {
	case <-ctx.Done():
		f.cancelled.Add(1)
		f.active.Add(-1)
		return nil, ctx.Err()
	case <-f.release:
		f.active.Add(-1)
		return &testBackend{activeTime: time.Now()}, nil
	}
}

func useClientGCManagerForTest(t *testing.T, c *client, manager *clientGCManager) {
	t.Helper()
	c.gcManager.unregister(c)
	c.gcManager = manager
	manager.register(c)
}

func TestBackendCreateContextCancelledByClientClose(t *testing.T) {
	manager := newClientGCManager()
	factory := newCancellableCreateFactory()
	rpcClient, err := NewClient(
		t.Name(),
		factory,
		WithClientEnableAutoCreateBackend(),
		WithClientCircuitBreaker(CircuitBreakerConfig{
			Enabled:             true,
			FailureThreshold:    1,
			ResetTimeout:        time.Hour,
			HalfOpenMaxRequests: 1,
		}),
	)
	require.NoError(t, err)
	client := rpcClient.(*client)
	useClientGCManagerForTest(t, client, manager)

	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(factory.release) }) }
	closeDone := make(chan error, 1)
	t.Cleanup(func() {
		release()
		select {
		case <-closeDone:
		default:
			_ = rpcClient.Close()
		}
		manager.stop()
	})

	sendDone := make(chan error, 1)
	go func() {
		_, sendErr := rpcClient.Send(
			context.Background(),
			"blocked",
			newTestMessage(1),
		)
		sendDone <- sendErr
	}()
	select {
	case <-factory.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("factory was not dequeued")
	}
	client.mu.Lock()
	state := client.mu.creating["blocked"]
	client.mu.Unlock()
	require.NotNil(t, state)

	go func() { closeDone <- rpcClient.Close() }()
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("client close did not cancel in-flight factory creation")
	}
	select {
	case err := <-sendDone:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("send did not observe client cancellation")
	}

	require.Equal(t, int32(1), factory.cancelled.Load())
	require.Zero(t, factory.active.Load())
	require.True(t, backendCreateDone(state))
	require.NoError(t, state.factoryErr)
	require.Empty(t, client.mu.backends["blocked"])
	require.Equal(t, CircuitClosed,
		client.circuitBreakers.GetBreaker("blocked").State())
	require.Zero(t,
		client.circuitBreakers.GetBreaker("blocked").Stats().FailureCount)
}

func TestBoundedBackendCreateClosesLateResultBeforeReleasingSlot(t *testing.T) {
	slots := make(chan struct{}, 1)
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseCreate := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseCreate)
	lateBackend := &testBackend{activeTime: time.Now()}
	ctx, cancel := context.WithCancel(context.Background())
	resultC := make(chan error, 1)
	go func() {
		_, err := boundedBackendCreate(ctx, slots, func() (Backend, error) {
			close(entered)
			<-release
			return lateBackend, nil
		})
		resultC <- err
	}()
	<-entered
	cancel()
	require.ErrorIs(t, <-resultC, context.Canceled)
	require.Len(t, slots, 1, "late lower-level create still owns the bound")

	secondStarted := atomic.Bool{}
	secondCtx, secondCancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer secondCancel()
	_, err := boundedBackendCreate(secondCtx, slots, func() (Backend, error) {
		secondStarted.Store(true)
		return &testBackend{}, nil
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.False(t, secondStarted.Load())

	releaseCreate()
	require.Eventually(t, func() bool {
		lateBackend.RLock()
		closed := lateBackend.closed
		lateBackend.RUnlock()
		return closed && len(slots) == 0
	}, time.Second, time.Millisecond)
}

func TestFactoryDeadlineWithoutLifecycleCancelRemainsFailureEvidence(t *testing.T) {
	rpcClient, err := NewClient(
		t.Name(),
		&factoryDeadlineError{},
		WithClientEnableAutoCreateBackend(),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, rpcClient.Close()) }()
	client := rpcClient.(*client)

	client.mu.Lock()
	generation := client.backendGenerationLocked("remote-timeout")
	client.mu.Unlock()
	state, queued := client.gcManager.triggerCreateAtGenerationState(
		client,
		"remote-timeout",
		generation,
	)
	require.True(t, queued)
	select {
	case <-state.done:
	case <-time.After(5 * time.Second):
		t.Fatal("factory deadline result was not published")
	}
	require.ErrorIs(t, state.factoryErr, context.DeadlineExceeded)
}

func TestBackendCreateContextCancelledByManagerStop(t *testing.T) {
	manager := newClientGCManager()
	factory := newCancellableCreateFactory()
	rpcClient, err := NewClient(
		t.Name(),
		factory,
		WithClientEnableAutoCreateBackend(),
	)
	require.NoError(t, err)
	client := rpcClient.(*client)
	useClientGCManagerForTest(t, client, manager)

	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(factory.release) }) }
	stopDone := make(chan struct{})
	t.Cleanup(func() {
		release()
		select {
		case <-stopDone:
		default:
			manager.stop()
		}
		require.NoError(t, rpcClient.Close())
	})

	state, queued := manager.triggerCreateAtGenerationState(
		client,
		"blocked",
		func() *backendGeneration {
			client.mu.Lock()
			defer client.mu.Unlock()
			return client.backendGenerationLocked("blocked")
		}(),
	)
	require.True(t, queued)
	select {
	case <-factory.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("factory was not dequeued")
	}

	go func() {
		manager.stop()
		close(stopDone)
	}()
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("manager stop did not cancel in-flight factory creation")
	}

	require.Equal(t, int32(1), factory.cancelled.Load())
	require.Zero(t, factory.active.Load())
	require.True(t, backendCreateDone(state))
	require.NoError(t, state.factoryErr)
	require.Empty(t, client.mu.backends["blocked"])
}

func TestBackendCreateContextCancelledByGenerationReset(t *testing.T) {
	manager := newClientGCManager()
	factory := newCancellableCreateFactory()
	rpcClient, err := NewClient(
		t.Name(),
		factory,
		WithClientEnableAutoCreateBackend(),
	)
	require.NoError(t, err)
	client := rpcClient.(*client)
	useClientGCManagerForTest(t, client, manager)
	t.Cleanup(func() {
		select {
		case <-factory.release:
		default:
			close(factory.release)
		}
		manager.stop()
		require.NoError(t, rpcClient.Close())
	})

	state, queued := manager.triggerCreateAtGenerationState(
		client,
		"blocked",
		func() *backendGeneration {
			client.mu.Lock()
			defer client.mu.Unlock()
			return client.backendGenerationLocked("blocked")
		}(),
	)
	require.True(t, queued)
	select {
	case <-factory.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("factory was not dequeued")
	}

	require.NoError(t, client.CloseBackendFor("blocked"))
	require.Eventually(t, func() bool {
		return factory.cancelled.Load() == 1 && factory.active.Load() == 0
	}, time.Second, time.Millisecond)
	require.True(t, backendCreateDone(state))
	require.NoError(t, state.factoryErr)
	require.Empty(t, client.mu.backends["blocked"])
}

func TestBackendCreateCancellationStressStaysWithinWorkerBound(t *testing.T) {
	manager := newClientGCManager()
	factory := &cancellationStressFactory{}
	rpcClient, err := NewClient(
		t.Name(),
		factory,
		WithClientEnableAutoCreateBackend(),
		WithClientMaxBackendPerHost(2),
	)
	require.NoError(t, err)
	client := rpcClient.(*client)
	useClientGCManagerForTest(t, client, manager)
	t.Cleanup(func() {
		manager.stop()
		_ = rpcClient.Close()
	})

	var operations sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		operations.Add(1)
		go func(offset int) {
			defer operations.Done()
			for i := 0; i < 100; i++ {
				remote := string(rune('a' + (i+offset)%16))
				client.mu.Lock()
				generation := client.backendGenerationLocked(remote)
				client.mu.Unlock()
				manager.triggerCreateAtGeneration(client, remote, generation)
				if i%3 == 0 {
					_ = client.CloseBackendFor(remote)
				}
			}
		}(worker)
	}
	operations.Wait()
	manager.stop()
	require.NoError(t, rpcClient.Close())
	require.Zero(t, factory.active.Load())
	require.LessOrEqual(t, factory.max.Load(), int32(backendCreateWorkerCount))
}
