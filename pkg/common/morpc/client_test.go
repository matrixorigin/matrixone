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
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createNotifyFactory wraps a BackendFactory and signals on created each time Create returns (for event-driven tests, no Sleep).
type createNotifyFactory struct {
	inner   *testBackendFactory
	created chan struct{}
}

func (f *createNotifyFactory) Create(backend string, opts ...BackendOption) (Backend, error) {
	b, err := f.inner.Create(backend, opts...)
	select {
	case f.created <- struct{}{}:
	default:
	}
	return b, err
}

type blockingCreateFactory struct {
	entered chan struct{}
	release chan struct{}
	backend *testBackend
}

type failingCreateFactory struct {
	attempts atomic.Int32
}

type newStreamErrorBackend struct {
	*testBackend
	err error
}

func (b *newStreamErrorBackend) NewStream(bool) (Stream, error) {
	return nil, b.err
}

type operationClosedBackend struct {
	*testBackend
}

func (b *operationClosedBackend) Send(context.Context, Message) (*Future, error) {
	return nil, backendClosed
}

func (b *operationClosedBackend) SendInternal(context.Context, Message) (*Future, error) {
	return nil, backendClosed
}

type operationClosedBlockingCloseBackend struct {
	*testBackend
	started    chan struct{}
	release    chan struct{}
	closeOnce  sync.Once
	closeCalls atomic.Int32
}

func (b *operationClosedBlockingCloseBackend) Send(context.Context, Message) (*Future, error) {
	return nil, backendClosed
}

func (b *operationClosedBlockingCloseBackend) SendInternal(context.Context, Message) (*Future, error) {
	return nil, backendClosed
}

func (b *operationClosedBlockingCloseBackend) NewStream(bool) (Stream, error) {
	return nil, backendClosed
}

func (b *operationClosedBlockingCloseBackend) Close() {
	b.closeCalls.Add(1)
	b.closeOnce.Do(func() {
		close(b.started)
		<-b.release
		b.testBackend.Close()
	})
}

type blockingCloseBackend struct {
	*testBackend
	started chan struct{}
	release chan struct{}
}

func (b *blockingCloseBackend) Close() {
	close(b.started)
	<-b.release
	b.testBackend.Close()
}

type lastActiveCountingBackend struct {
	*testBackend
	loads atomic.Int32
}

func (b *lastActiveCountingBackend) LastActiveTime() time.Time {
	b.loads.Add(1)
	return b.testBackend.LastActiveTime()
}

func (f *failingCreateFactory) Create(string, ...BackendOption) (Backend, error) {
	f.attempts.Add(1)
	return nil, fmt.Errorf("create failed")
}

func (f *blockingCreateFactory) Create(string, ...BackendOption) (Backend, error) {
	close(f.entered)
	<-f.release
	return f.backend, nil
}

func TestCreateBackendLocked(t *testing.T) {
	rc, err := NewClient("", newTestBackendFactory(), WithClientMaxBackendPerHost(1))
	assert.NoError(t, err)
	c := rc.(*client)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	b, err := c.createBackendLocked("b1")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(c.mu.backends["b1"]))
	assert.NotNil(t, b)

	b, err = c.createBackendLocked("b1")
	assert.Error(t, err)
	assert.Nil(t, b)
	assert.Equal(t, 1, len(c.mu.backends["b1"]))
}

func TestGetBackendLockedWithClosed(t *testing.T) {
	rc, err := NewClient("", newTestBackendFactory(), WithClientMaxBackendPerHost(1))
	assert.NoError(t, err)
	c := rc.(*client)
	assert.NoError(t, c.Close())

	b, err := c.getBackendLocked("b1", false)
	assert.Error(t, err)
	assert.Nil(t, b)
}

func TestGetBackendLockedWithEmptyBackends(t *testing.T) {
	rc, err := NewClient("", newTestBackendFactory(), WithClientMaxBackendPerHost(1 /*disable create*/))
	assert.NoError(t, err)
	c := rc.(*client)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	b, err := c.getBackendLocked("b1", false)
	assert.NoError(t, err)
	assert.Nil(t, b)
}

func TestNewStreamDoesNotSleepPastBackendCreation(t *testing.T) {
	factory := &blockingCreateFactory{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		backend: &testBackend{id: 1, activeTime: time.Now()},
	}
	rc, err := NewClient(
		"event-driven-create-wait",
		factory,
		WithClientMaxBackendPerHost(1),
		WithClientEnableAutoCreateBackend(),
		WithClientDisableCircuitBreaker(),
		WithClientRetryPolicy(RetryPolicy{
			InitialBackoff: 5 * time.Second,
			MaxBackoff:     5 * time.Second,
			Multiplier:     1,
		}),
	)
	require.NoError(t, err)
	c := rc.(*client)
	defer func() {
		select {
		case <-factory.release:
		default:
			close(factory.release)
		}
		require.NoError(t, c.Close())
	}()

	type result struct {
		stream Stream
		err    error
	}
	resultC := make(chan result, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() {
		stream, err := c.NewStream(ctx, "remote", true)
		resultC <- result{stream: stream, err: err}
	}()

	select {
	case <-factory.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("backend create did not reach the factory")
	}
	close(factory.release)

	select {
	case result := <-resultC:
		require.NoError(t, result.err)
		require.NotNil(t, result.stream)
		require.NoError(t, result.stream.Close(false))
	case <-time.After(time.Second):
		t.Fatal("NewStream slept on retry backoff after the backend was ready")
	}
}

func TestNewStreamRetainsBackoffAfterBackendCreateFailure(t *testing.T) {
	factory := &failingCreateFactory{}
	rc, err := NewClient(
		"failed-create-backoff",
		factory,
		WithClientMaxBackendPerHost(1),
		WithClientEnableAutoCreateBackend(),
		WithClientDisableCircuitBreaker(),
		WithClientRetryPolicy(RetryPolicy{
			InitialBackoff: 5 * time.Second,
			MaxBackoff:     5 * time.Second,
			Multiplier:     1,
		}),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, rc.Close()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = rc.NewStream(ctx, "remote", true)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, int32(1), factory.attempts.Load(),
		"a failed factory call must not trigger an immediate retry loop")
}

func TestNewStreamErrorReturnsBackendLockToClient(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		retired bool
	}{
		{name: "closed backend is retired", err: backendClosed, retired: true},
		{name: "other error remains selectable", err: fmt.Errorf("new stream failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rc, err := NewClient(
				"new-stream-lock-ownership",
				newTestBackendFactory(),
				WithClientMaxBackendPerHost(1),
				WithClientDisableAutoCreateBackend(),
				WithClientDisableCircuitBreaker(),
				WithClientRetryPolicy(NoRetryPolicy),
			)
			require.NoError(t, err)
			c := rc.(*client)
			defer func() { require.NoError(t, c.Close()) }()

			base := &testBackend{id: 1, activeTime: time.Now()}
			backend := &newStreamErrorBackend{testBackend: base, err: tc.err}
			c.mu.Lock()
			c.mu.backends["remote"] = []Backend{backend}
			c.mu.ops["remote"] = &op{}
			c.mu.Unlock()

			stream, err := c.NewStream(t.Context(), "remote", true)
			require.ErrorIs(t, err, tc.err)
			require.Nil(t, stream)
			require.False(t, base.Locked(), "failed stream creation must not retain the pool lock")

			c.mu.Lock()
			backends := append([]Backend(nil), c.mu.backends["remote"]...)
			c.mu.Unlock()
			if tc.retired {
				require.Empty(t, backends, "closed backend must not block replacement capacity")
				c.backendCleanup.Wait()
				base.RLock()
				closed := base.closed
				base.RUnlock()
				require.True(t, closed)
			} else {
				require.Equal(t, []Backend{backend}, backends)
			}
		})
	}
}

func TestClosedOperationRetiresBackendBeforeReturn(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(context.Context, RPCClient) error
	}{
		{
			name: "send",
			call: func(ctx context.Context, client RPCClient) error {
				_, err := client.Send(ctx, "remote", newTestMessage(1))
				return err
			},
		},
		{
			name: "ping",
			call: func(ctx context.Context, client RPCClient) error {
				return client.Ping(ctx, "remote")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rc, err := NewClient(
				"closed-operation-retirement",
				newTestBackendFactory(),
				WithClientMaxBackendPerHost(1),
				WithClientDisableAutoCreateBackend(),
				WithClientDisableCircuitBreaker(),
				WithClientRetryPolicy(NoRetryPolicy),
			)
			require.NoError(t, err)
			c := rc.(*client)
			defer func() { require.NoError(t, c.Close()) }()

			base := &testBackend{id: 1, activeTime: time.Now()}
			backend := &operationClosedBackend{testBackend: base}
			c.mu.Lock()
			c.mu.backends["remote"] = []Backend{backend}
			c.mu.ops["remote"] = &op{}
			c.mu.Unlock()

			err = tc.call(t.Context(), rc)
			require.ErrorIs(t, err, backendClosed)
			c.mu.Lock()
			backends := append([]Backend(nil), c.mu.backends["remote"]...)
			c.mu.Unlock()
			require.Empty(t, backends)
		})
	}
}

func TestClosedOperationDoesNotWaitForBackendCleanup(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(context.Context, RPCClient) error
	}{
		{
			name: "send",
			call: func(ctx context.Context, client RPCClient) error {
				_, err := client.Send(ctx, "remote", newTestMessage(1))
				return err
			},
		},
		{
			name: "ping",
			call: func(ctx context.Context, client RPCClient) error {
				return client.Ping(ctx, "remote")
			},
		},
		{
			name: "new stream",
			call: func(ctx context.Context, client RPCClient) error {
				_, err := client.NewStream(ctx, "remote", true)
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rc, err := NewClient(
				"closed-operation-cleanup",
				newTestBackendFactory(),
				WithClientMaxBackendPerHost(1),
				WithClientDisableAutoCreateBackend(),
				WithClientDisableCircuitBreaker(),
				WithClientRetryPolicy(NoRetryPolicy),
			)
			require.NoError(t, err)
			c := rc.(*client)

			backend := &operationClosedBlockingCloseBackend{
				testBackend: &testBackend{id: 1, activeTime: time.Now()},
				started:     make(chan struct{}),
				release:     make(chan struct{}),
			}
			var releaseOnce sync.Once
			defer func() {
				releaseOnce.Do(func() { close(backend.release) })
				require.NoError(t, c.Close())
			}()
			c.mu.Lock()
			c.mu.backends["remote"] = []Backend{backend}
			c.mu.ops["remote"] = &op{}
			c.mu.Unlock()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			resultC := make(chan error, 1)
			go func() { resultC <- tc.call(ctx, rc) }()

			select {
			case <-backend.started:
			case <-ctx.Done():
				t.Fatal("backend cleanup did not start")
			}
			select {
			case err := <-resultC:
				require.ErrorIs(t, err, backendClosed)
			case <-ctx.Done():
				t.Fatal("operation waited for blocked backend cleanup")
			}

			c.mu.Lock()
			backends := append([]Backend(nil), c.mu.backends["remote"]...)
			c.mu.Unlock()
			require.Empty(t, backends, "retired backend must release pool capacity before cleanup finishes")

			releaseOnce.Do(func() { close(backend.release) })
			c.backendCleanup.Wait()
			require.EqualValues(t, 1, backend.closeCalls.Load())
		})
	}
}

func TestClientCloseJoinsRetiredBackendCleanupOnce(t *testing.T) {
	rc, err := NewClient(
		"retired-cleanup-join",
		newTestBackendFactory(),
		WithClientDisableAutoCreateBackend(),
		WithClientDisableCircuitBreaker(),
	)
	require.NoError(t, err)
	c := rc.(*client)
	backend := &operationClosedBlockingCloseBackend{
		testBackend: &testBackend{id: 1, activeTime: time.Now()},
		started:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	var releaseOnce sync.Once
	defer func() {
		releaseOnce.Do(func() { close(backend.release) })
		require.NoError(t, c.Close())
	}()
	c.mu.Lock()
	c.mu.backends["remote"] = []Backend{backend}
	c.mu.ops["remote"] = &op{}
	c.mu.Unlock()

	c.retireBackend("remote", backend)
	select {
	case <-backend.started:
	case <-time.After(time.Second):
		t.Fatal("retired backend cleanup did not start")
	}

	closeC := make(chan error, 2)
	go func() { closeC <- c.Close() }()
	go func() { closeC <- c.Close() }()
	select {
	case err := <-closeC:
		require.NoError(t, err)
		t.Fatal("client close returned before owned backend cleanup completed")
	case <-time.After(50 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(backend.release) })
	for range 2 {
		select {
		case err := <-closeC:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("client close did not finish after backend cleanup completed")
		}
	}
	require.EqualValues(t, 1, backend.closeCalls.Load())
}

func TestBackendCleanupSaturationPreservesPoolBound(t *testing.T) {
	rc, err := NewClient(
		"retired-cleanup-bound",
		newTestBackendFactory(),
		WithClientMaxBackendPerHost(2),
		WithClientDisableAutoCreateBackend(),
		WithClientDisableCircuitBreaker(),
	)
	require.NoError(t, err)
	c := rc.(*client)
	// Reduce the production bound to one so saturation is deterministic.
	c.backendCleanupSlots = make(chan struct{}, 1)
	first := &operationClosedBlockingCloseBackend{
		testBackend: &testBackend{id: 1, activeTime: time.Now()},
		started:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	second := &operationClosedBlockingCloseBackend{
		testBackend: &testBackend{id: 2, activeTime: time.Now()},
		started:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	var releaseFirst sync.Once
	var releaseSecond sync.Once
	defer func() {
		releaseFirst.Do(func() { close(first.release) })
		releaseSecond.Do(func() { close(second.release) })
		require.NoError(t, c.Close())
	}()
	c.mu.Lock()
	c.mu.backends["remote"] = []Backend{first, second}
	c.mu.ops["remote"] = &op{}
	c.mu.Unlock()

	c.retireBackend("remote", first)
	select {
	case <-first.started:
	case <-time.After(time.Second):
		t.Fatal("first backend cleanup did not start")
	}
	c.retireBackend("remote", second)
	select {
	case <-second.started:
		t.Fatal("cleanup admission exceeded its hard bound")
	default:
	}
	c.mu.Lock()
	backends := append([]Backend(nil), c.mu.backends["remote"]...)
	c.mu.Unlock()
	require.Equal(t, []Backend{second}, backends,
		"cleanup-saturated backend must keep consuming pool capacity")

	releaseFirst.Do(func() { close(first.release) })
	c.backendCleanup.Wait()
	c.retireBackend("remote", second)
	select {
	case <-second.started:
	case <-time.After(time.Second):
		t.Fatal("backend cleanup was not admitted after capacity returned")
	}
	c.mu.Lock()
	backends = append([]Backend(nil), c.mu.backends["remote"]...)
	c.mu.Unlock()
	require.Empty(t, backends)

	releaseSecond.Do(func() { close(second.release) })
	c.backendCleanup.Wait()
	require.EqualValues(t, 1, first.closeCalls.Load())
	require.EqualValues(t, 1, second.closeCalls.Load())
}

func TestRetireBackendPreservesHealthyPeer(t *testing.T) {
	rc, err := NewClient(
		"targeted-backend-retirement",
		newTestBackendFactory(),
		WithClientMaxBackendPerHost(2),
		WithClientDisableCircuitBreaker(),
	)
	require.NoError(t, err)
	c := rc.(*client)
	defer func() { require.NoError(t, c.Close()) }()

	healthy := &testBackend{id: 1, activeTime: time.Now()}
	closed := &testBackend{id: 2, activeTime: time.Now()}
	c.mu.Lock()
	c.mu.backends["remote"] = []Backend{healthy, closed}
	c.mu.ops["remote"] = &op{}
	c.mu.Unlock()

	c.retireBackend("remote", closed)

	c.mu.Lock()
	backends := append([]Backend(nil), c.mu.backends["remote"]...)
	c.mu.Unlock()
	require.Equal(t, []Backend{healthy}, backends)
	healthy.RLock()
	healthyClosed := healthy.closed
	healthy.RUnlock()
	require.False(t, healthyClosed)
}

func TestBackendLookupDetachesInactiveWithoutWaitingForCleanup(t *testing.T) {
	rc, err := NewClient(
		"inactive-capacity",
		newTestBackendFactory(),
		WithClientMaxBackendPerHost(1),
		WithClientDisableAutoCreateBackend(),
		WithClientDisableCircuitBreaker(),
	)
	require.NoError(t, err)
	c := rc.(*client)
	defer func() { require.NoError(t, c.Close()) }()

	base := &testBackend{id: 1}
	inactive := &blockingCloseBackend{
		testBackend: base,
		started:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	defer func() {
		select {
		case <-inactive.release:
		default:
			close(inactive.release)
		}
	}()
	backing := []Backend{inactive}
	c.mu.Lock()
	c.mu.backends["remote"] = backing
	c.mu.ops["remote"] = &op{}
	c.mu.Unlock()

	type lookupResult struct {
		backend Backend
		err     error
	}
	resultC := make(chan lookupResult, 1)
	go func() {
		backend, _, err := c.getBackendForOperation("remote", false)
		resultC <- lookupResult{backend: backend, err: err}
	}()

	select {
	case <-inactive.started:
	case <-time.After(time.Second):
		t.Fatal("inactive backend close did not start")
	}

	// Close is intentionally blocked. Capacity must already be detached and the
	// lookup itself must complete so callers can admit a replacement without
	// waiting for backend shutdown.
	locked := make(chan []Backend, 1)
	go func() {
		c.mu.Lock()
		locked <- append([]Backend(nil), c.mu.backends["remote"]...)
		c.mu.Unlock()
	}()
	select {
	case backends := <-locked:
		require.Empty(t, backends)
		require.Nil(t, backing[0], "detached backend must not remain in the slice tail")
	case <-time.After(time.Second):
		t.Fatal("backend cleanup held the client lock")
	}

	select {
	case result := <-resultC:
		require.Nil(t, result.backend)
		require.True(t, moerr.IsMoErrCode(result.err, moerr.ErrNoAvailableBackend), result.err)
	case <-time.After(time.Second):
		t.Fatal("backend lookup waited for detached backend cleanup")
	}

	close(inactive.release)
	c.backendCleanup.Wait()
	base.RLock()
	closed := base.closed
	base.RUnlock()
	require.True(t, closed, "detached backend must be closed outside the client lock")
}

func TestBackendLookupDoesNotRescanHealthyPool(t *testing.T) {
	rc, err := NewClient(
		"healthy-fast-path",
		newTestBackendFactory(),
		WithClientDisableAutoCreateBackend(),
		WithClientDisableCircuitBreaker(),
	)
	require.NoError(t, err)
	c := rc.(*client)
	defer func() { require.NoError(t, c.Close()) }()

	backend := &lastActiveCountingBackend{
		testBackend: &testBackend{id: 1, activeTime: time.Now()},
	}
	c.mu.Lock()
	c.mu.backends["remote"] = []Backend{backend}
	c.mu.ops["remote"] = &op{}
	c.mu.Unlock()

	selected, _, err := c.getBackendForOperation("remote", false)
	require.NoError(t, err)
	require.Same(t, backend, selected)
	require.EqualValues(t, 1, backend.loads.Load(), "healthy lookup must keep the single-scan fast path")
}

func TestCloseBackendForSynchronouslyDetachesOnlyTarget(t *testing.T) {
	rc, err := NewClient("", newTestBackendFactory(), WithClientMaxBackendPerHost(1))
	require.NoError(t, err)
	c := rc.(*client)
	defer func() { require.NoError(t, c.Close()) }()

	target := &testBackend{id: 1, activeTime: time.Now()}
	other := &testBackend{id: 2, activeTime: time.Now()}
	c.mu.Lock()
	c.mu.backends["target"] = []Backend{target}
	c.mu.backends["other"] = []Backend{other}
	c.mu.ops["target"] = &op{}
	c.mu.ops["other"] = &op{}
	c.mu.Unlock()
	c.circuitBreakers.RecordFailure("target")

	require.NoError(t, c.CloseBackendFor("target"))

	c.mu.Lock()
	targetBackends := append([]Backend(nil), c.mu.backends["target"]...)
	targetOp := c.mu.ops["target"]
	otherBackends := append([]Backend(nil), c.mu.backends["other"]...)
	c.mu.Unlock()
	require.Empty(t, targetBackends)
	require.Nil(t, targetOp)
	require.Equal(t, []Backend{other}, otherBackends)
	require.NotContains(t, c.circuitBreakers.Stats(), "target")

	target.RWMutex.RLock()
	targetClosed := target.closed
	target.RWMutex.RUnlock()
	other.RWMutex.RLock()
	otherClosed := other.closed
	other.RWMutex.RUnlock()
	require.True(t, targetClosed)
	require.False(t, otherClosed)

	c.mu.Lock()
	replacement, err := c.createBackendLocked("target")
	c.mu.Unlock()
	require.NoError(t, err)
	require.NotNil(t, replacement)
}

func TestCloseBackendForRejectsConcurrentSynchronousCreate(t *testing.T) {
	factory := &blockingCreateFactory{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		backend: &testBackend{id: 1, activeTime: time.Now()},
	}
	defer func() {
		select {
		case <-factory.release:
		default:
			close(factory.release)
		}
	}()
	rc, err := NewClient("", factory, WithClientMaxBackendPerHost(1))
	require.NoError(t, err)
	c := rc.(*client)
	defer func() { require.NoError(t, c.Close()) }()

	result := make(chan error, 1)
	go func() {
		_, err := c.createBackendWithBookkeeping("target", false)
		result <- err
	}()
	select {
	case <-factory.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("backend creation did not start")
	}

	require.NoError(t, c.CloseBackendFor("target"))
	close(factory.release)
	select {
	case err := <-result:
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrBackendClosed))
	case <-time.After(5 * time.Second):
		t.Fatal("backend creation did not finish")
	}

	c.mu.Lock()
	targetBackends := append([]Backend(nil), c.mu.backends["target"]...)
	c.mu.Unlock()
	factory.backend.RWMutex.RLock()
	backendClosed := factory.backend.closed
	factory.backend.RWMutex.RUnlock()
	require.Empty(t, targetBackends)
	require.True(t, backendClosed)
}

func TestQueueFullFallbackRejectsGenerationCapturedBeforeReset(t *testing.T) {
	factory := &createNotifyFactory{
		inner:   newTestBackendFactory(),
		created: make(chan struct{}, 1),
	}
	rc, err := NewClient("", factory, WithClientMaxBackendPerHost(1))
	require.NoError(t, err)
	c := rc.(*client)
	defer func() { require.NoError(t, c.Close()) }()

	c.mu.Lock()
	staleGeneration := c.backendGenerationLocked("target")
	c.mu.Unlock()
	require.NoError(t, c.CloseBackendFor("target"))

	_, err = c.createBackendWithBookkeepingAtGeneration(
		"target",
		false,
		staleGeneration,
	)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrBackendClosed))
	select {
	case <-factory.created:
		t.Fatal("stale queue-full fallback reached backend factory")
	default:
	}
	c.mu.Lock()
	targetBackends := append([]Backend(nil), c.mu.backends["target"]...)
	c.mu.Unlock()
	require.Empty(t, targetBackends)
}

func TestBackendGenerationIsBoundedAndDoesNotABA(t *testing.T) {
	rc, err := NewClient("", newTestBackendFactory(), WithClientMaxBackendPerHost(1))
	require.NoError(t, err)
	c := rc.(*client)
	defer func() { require.NoError(t, c.Close()) }()

	c.mu.Lock()
	old := c.backendGenerationLocked("target")
	c.mu.Unlock()
	require.NoError(t, c.CloseBackendFor("target"))
	c.mu.Lock()
	current := c.backendGenerationLocked("target")
	c.mu.backendGeneration = make(map[string]*backendGeneration)
	generations := make(map[string]*backendGeneration, maxBackendGenerationEntries)
	for i := 0; i < maxBackendGenerationEntries; i++ {
		remote := fmt.Sprintf("remote-%d", i)
		generations[remote] = c.backendGenerationLocked(remote)
	}
	c.backendGenerationLocked("overflow")
	generationCount := len(c.mu.backendGeneration)
	var evictedRemote string
	var evictedGeneration *backendGeneration
	for remote, generation := range generations {
		if c.mu.backendGeneration[remote] != generation {
			evictedRemote = remote
			evictedGeneration = generation
			break
		}
	}
	c.mu.Unlock()
	require.NotSame(t, old, current)
	require.LessOrEqual(t, generationCount, maxBackendGenerationEntries)
	require.NotEmpty(t, evictedRemote)

	_, err = c.createBackendWithBookkeepingAtGeneration(
		evictedRemote,
		false,
		evictedGeneration,
	)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrBackendClosed),
		"evicted generation was re-admitted")
}

// TestGetBackendLockedDoesNotCreateWhenAvailable covers the fix: maybeCreateLocked must only
// be called when no available backend was found (b == nil). When at least one backend is
// available (not locked, has LastActiveTime), we must not create more backends.
func TestGetBackendLockedDoesNotCreateWhenAvailable(t *testing.T) {
	rc, err := NewClient("",
		newTestBackendFactory(),
		WithClientMaxBackendPerHost(2),
		WithClientEnableAutoCreateBackend())
	assert.NoError(t, err)
	c := rc.(*client)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	c.mu.Lock()
	c.mu.backends["b1"] = []Backend{&testBackend{id: 0, busy: false, activeTime: time.Now()}}
	c.mu.ops["b1"] = &op{}
	c.mu.Unlock()

	for i := 0; i < 10; i++ {
		c.mu.Lock()
		b, err := c.getBackendLocked("b1", false)
		n := len(c.mu.backends["b1"])
		c.mu.Unlock()
		assert.NoError(t, err)
		assert.NotNil(t, b, "iteration %d: must return the available backend", i)
		assert.Equal(t, 1, n, "getBackendLocked must not call maybeCreateLocked when b != nil; iteration %d had %d backends", i, n)
	}
}

func TestGetBackend(t *testing.T) {
	rc, err := NewClient("", newTestBackendFactory(), WithClientMaxBackendPerHost(1))
	assert.NoError(t, err)
	c := rc.(*client)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	n := 2
	for i := 0; i < n; i++ {
		c.mu.backends["b1"] = append(c.mu.backends["b1"], &testBackend{id: i, busy: false, activeTime: time.Now()})
	}
	c.mu.ops["b1"] = &op{}

	for i := 0; i < n; i++ {
		b, err := c.getBackend("b1", false)
		assert.NoError(t, err)
		assert.Equal(t, c.mu.backends["b1"][(i+1)%n], b)
	}
}

func TestCannotGetLocedBackend(t *testing.T) {
	rc, err := NewClient("", newTestBackendFactory(), WithClientMaxBackendPerHost(1))
	assert.NoError(t, err)
	c := rc.(*client)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	n := 10
	for i := 0; i < n; i++ {
		c.mu.backends["b1"] = append(c.mu.backends["b1"], &testBackend{id: i, locked: i == 0, busy: false, activeTime: time.Now()})
	}
	c.mu.ops["b1"] = &op{}

	for i := 0; i < n; i++ {
		b, err := c.getBackend("b1", false)
		assert.NoError(t, err)
		assert.False(t, b.Locked())
	}
}

func TestCanGetBackendIfALLLockedAndNotReachMaxPerHost(t *testing.T) {
	rc, err := NewClient("", newTestBackendFactory(),
		WithClientMaxBackendPerHost(3),
		WithClientEnableAutoCreateBackend())
	assert.NoError(t, err)
	c := rc.(*client)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	c.mu.backends["b1"] = append(c.mu.backends["b1"],
		&testBackend{id: 1, locked: true, busy: false, activeTime: time.Now()},
		&testBackend{id: 2, locked: true, busy: false, activeTime: time.Now()})
	c.mu.ops["b1"] = &op{}

	// With async creation, wait for backend to be available
	var b Backend
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	for {
		b, err = c.getBackend("b1", false)
		if err == nil && b != nil {
			break
		}

		select {
		case <-ctx.Done():
			t.Fatal("Backend creation timed out")
		case <-time.After(10 * time.Millisecond):
			// Small delay for retry
		}
	}
	assert.NoError(t, err)
	assert.NotNil(t, b)
	assert.False(t, b.Locked())
}

func TestMaybeCreateLockedWithEmptyBackends(t *testing.T) {
	rc, err := NewClient("",
		newTestBackendFactory(),
		WithClientMaxBackendPerHost(1),
		WithClientEnableAutoCreateBackend())
	assert.NoError(t, err)
	c := rc.(*client)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	assert.True(t, c.maybeCreateLocked("b1"))
}

func TestMaybeCreateLockedWithNotFullBackendsAndHasAnyBusy(t *testing.T) {
	rc, err := NewClient("",
		newTestBackendFactory(),
		WithClientMaxBackendPerHost(3),
		WithClientEnableAutoCreateBackend())
	assert.NoError(t, err)
	c := rc.(*client)
	defer func() {
		assert.NoError(t, c.Close())
	}()
	c.mu.backends["b1"] = []Backend{
		&testBackend{busy: false},
		&testBackend{busy: true},
	}
	assert.True(t, c.maybeCreateLocked("b1"))
}

func TestMaybeCreateLockedWithNotFullBackendsAndNoBusy(t *testing.T) {
	rc, err := NewClient("", newTestBackendFactory(), WithClientMaxBackendPerHost(3))
	assert.NoError(t, err)
	c := rc.(*client)
	defer func() {
		assert.NoError(t, c.Close())
	}()
	c.mu.backends["b1"] = []Backend{
		&testBackend{busy: false},
	}
	assert.False(t, c.maybeCreateLocked("b1"))
}

func TestMaybeCreateLockedWithFullBackends(t *testing.T) {
	rc, err := NewClient("",
		newTestBackendFactory(),
		WithClientMaxBackendPerHost(1),
		WithClientEnableAutoCreateBackend())
	assert.NoError(t, err)
	c := rc.(*client)
	defer func() {
		assert.NoError(t, c.Close())
	}()
	c.mu.backends["b1"] = []Backend{
		&testBackend{busy: false},
	}
	assert.False(t, c.maybeCreateLocked("b1"))
}

func TestGetBackendAutoCreateDisabled(t *testing.T) {
	rc, err := NewClient("",
		newTestBackendFactory(),
		WithClientDisableAutoCreateBackend(),
		WithClientMaxBackendPerHost(1))
	assert.NoError(t, err)
	c := rc.(*client)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	b, err := c.getBackend("b1", false)
	assert.Nil(t, b)
	if err != nil {
		t.Logf("Error: %v, Type: %T", err, err)
	}
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrNoAvailableBackend))

	c.mu.Lock()
	defer c.mu.Unlock()
	assert.Equal(t, 0, len(c.mu.backends["b1"]))
}

func TestCreateBackendWithBookkeeping(t *testing.T) {
	rc, err := NewClient("",
		newTestBackendFactory(),
		WithClientMaxBackendPerHost(1))
	assert.NoError(t, err)
	c := rc.(*client)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	// First creation succeeds and initializes ops/bookkeeping.
	b, err := c.createBackendWithBookkeeping("b1", true)
	assert.NoError(t, err)
	assert.NotNil(t, b)

	c.mu.Lock()
	opsInitialized := c.mu.ops["b1"] != nil
	backendCount := len(c.mu.backends["b1"])
	var lockedState bool
	if tb, ok := b.(*testBackend); ok {
		tb.RLock()
		lockedState = tb.locked
		tb.RUnlock()
	}
	c.mu.Unlock()

	assert.True(t, opsInitialized, "ops should be initialized")
	assert.Equal(t, 1, backendCount)
	assert.True(t, lockedState, "backend should be locked when requested")

	// Second creation should respect maxBackendsPerHost and fail.
	b2, err := c.createBackendWithBookkeeping("b1", false)
	assert.Nil(t, b2)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrBackendClosed))
}

func TestInitBackendsAndMaxBackendsPerHostNotMatch(t *testing.T) {
	rc, err := NewClient(
		"",
		newTestBackendFactory(),
		WithClientCreateTaskChanSize(2),
		WithClientInitBackends([]string{"b1", "b2"}, []int{3, 1}))
	assert.NoError(t, err)
	c := rc.(*client)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	assert.Equal(t, 3, c.options.maxBackendsPerHost)
}

func TestGetBackendWithCreateBackend(t *testing.T) {
	rc, err := NewClient(
		"",
		newTestBackendFactory(),
		WithClientCreateTaskChanSize(1),
		WithClientEnableAutoCreateBackend())
	assert.NoError(t, err)
	c := rc.(*client)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	// With async creation, first call may fail
	b, err := c.getBackend("b1", false)
	if err != nil {
		// Wait for async creation
		for i := 0; i < 10; i++ {
			time.Sleep(10 * time.Millisecond)
			b, err = c.getBackend("b1", false)
			if err == nil && b != nil {
				break
			}
		}
	}
	assert.NoError(t, err)
	assert.NotNil(t, b)
	assert.Equal(t, 1, len(c.mu.backends["b1"]))
}

func TestCloseIdleBackends(t *testing.T) {
	rc, err := NewClient(
		"",
		newTestBackendFactory(),
		WithClientMaxBackendPerHost(2),
		WithClientMaxBackendMaxIdleDuration(time.Millisecond*100),
		WithClientCreateTaskChanSize(1),
		WithClientEnableAutoCreateBackend(),
		WithClientDisableCircuitBreaker())
	require.NoError(t, err)
	c := rc.(*client)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	// First backend; lock it so getBackendLocked does not select it (b==nil) and will create second.
	var b Backend
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		b, err = c.getBackend("b1", true)
		if err == nil && b != nil {
			break
		}
		runtime.Gosched()
	}
	require.NoError(t, err)
	require.NotNil(t, b, "timeout waiting for first backend")

	// Create the second backend synchronously through the complete bookkeeping
	// path. A factory-level Create notification is too early for this assertion:
	// the backend is published to the pool only after Create returns and the
	// client re-acquires c.mu.
	activeBackend, err := c.createBackendWithBookkeeping("b1", false)
	require.NoError(t, err)
	require.NotNil(t, activeBackend)
	require.NotSame(t, b, activeBackend)
	c.mu.Lock()
	backendCount := len(c.mu.backends["b1"])
	c.mu.Unlock()
	require.Equal(t, 2, backendCount, "second backend must be created")

	// b is the idle backend: unlock it and zero activeTime so GC will close it
	b.Unlock()
	tb := b.(*testBackend)
	tb.setActiveTime(time.Time{})

	gcDeadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(gcDeadline) {
		// Refresh the non-idle backend inline so GC deterministically closes only the
		// backend we explicitly marked idle, without relying on goroutine scheduling.
		activeBackend.(*testBackend).active()
		globalClientGC.doGCIdle()
		runtime.Gosched()
		c.mu.Lock()
		v := len(c.mu.backends["b1"])
		c.mu.Unlock()
		if v == 1 {
			tb.RLock()
			closed := tb.closed
			tb.RUnlock()
			require.True(t, closed, "idle backend must be closed by GC")
			ab := activeBackend.(*testBackend)
			ab.RLock()
			assert.False(t, ab.closed)
			ab.RUnlock()
			c.mu.Lock()
			assert.Equal(t, 1, len(c.mu.backends["b1"]))
			c.mu.Unlock()
			return
		}
	}
	t.Fatal("idle backend was not closed by GC within 10s")
}

func TestLockedBackendCannotClosedWithGCIdleTask(t *testing.T) {
	rc, err := NewClient(
		"",
		newTestBackendFactory(),
		WithClientMaxBackendPerHost(2),
		WithClientMaxBackendMaxIdleDuration(time.Millisecond*100))
	assert.NoError(t, err)
	c := rc.(*client)
	defer func() {
		assert.NoError(t, c.Close())
	}()
	// This unit test exercises idle GC directly. Do not let the independent
	// global inactive-GC loop remove the zero-active-time backend first.
	globalClientGC.unregister(c)

	c.mu.Lock()
	b, err := c.createBackendLocked("b1")
	c.mu.Unlock()
	assert.NoError(t, err)
	assert.NotNil(t, b)
	b.Lock()
	assert.True(t, b.Locked())
	b.(*testBackend).setActiveTime(time.Time{})

	assert.Zero(t, c.closeIdleBackends())
	c.mu.Lock()
	assert.Equal(t, 1, len(c.mu.backends["b1"]))
	c.mu.Unlock()
}

func TestGetBackendsWithAllInactiveAndWillCreateNew(t *testing.T) {
	rc, err := NewClient(
		"",
		newTestBackendFactory(),
		WithClientMaxBackendPerHost(1),
		WithClientCreateTaskChanSize(1),
		WithClientEnableAutoCreateBackend())
	assert.NoError(t, err)
	c := rc.(*client)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	b, err := c.getBackend("b1", false)
	// With async creation, first call may fail
	if err != nil {
		// Wait for async creation
		time.Sleep(50 * time.Millisecond)
		b, err = c.getBackend("b1", false)
	}
	assert.NoError(t, err)
	assert.NotNil(t, b)

	b.(*testBackend).setActiveTime(time.Time{})

	// Backend is now inactive, next call triggers async recreation
	b, err = c.getBackend("b1", false)
	// May return error initially
	if err != nil {
		// Wait for async creation and retry
		for i := 0; i < 10; i++ {
			time.Sleep(10 * time.Millisecond)
			b, err = c.getBackend("b1", false)
			if err == nil && b != nil {
				break
			}
		}
	}
	assert.NoError(t, err)
	assert.NotNil(t, b)
}
