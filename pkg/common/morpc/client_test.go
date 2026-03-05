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
	"runtime"
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
	// Event-driven: factory signals on Create so we wait for 2 backends without Sleep.
	created := make(chan struct{}, 2)
	factory := &createNotifyFactory{inner: newTestBackendFactory(), created: created}
	rc, err := NewClient(
		"",
		factory,
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

	// Trigger second create; wait for two Create() completions (event-driven, with timeout)
	_, _ = c.getBackend("b1", false)
	for _, ch := range []chan struct{}{created, created} {
		select {
		case <-ch:
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for backend create (second backend may not have been created)")
		}
	}
	c.mu.Lock()
	require.Equal(t, 2, len(c.mu.backends["b1"]), "second backend must be created")
	idleBackend := c.mu.backends["b1"][0]
	activeBackend := c.mu.backends["b1"][1]
	c.mu.Unlock()

	idleBackend.Unlock()
	tb := idleBackend.(*testBackend)
	tb.Lock()
	tb.activeTime = time.Time{}
	tb.Unlock()

	go func() {
		ctx, cancel := context.WithTimeout(context.TODO(), 2*time.Second)
		defer cancel()
		st, err := activeBackend.NewStream(false)
		assert.NoError(t, err)
		for i := 0; i < 50; i++ {
			_ = st.Send(ctx, newTestMessage(1))
			runtime.Gosched()
		}
	}()

	gcDeadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(gcDeadline) {
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
		WithClientMaxBackendMaxIdleDuration(time.Millisecond*100),
		WithClientCreateTaskChanSize(1),
		WithClientEnableAutoCreateBackend())
	assert.NoError(t, err)
	c := rc.(*client)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	// Get backend with lock, may need retry
	var b Backend
	for i := 0; i < 10; i++ {
		b, err = c.getBackend("b1", true)
		if err == nil && b != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	assert.NoError(t, err)
	assert.NotNil(t, b)
	assert.True(t, b.Locked())
	b.(*testBackend).RWMutex.Lock()
	b.(*testBackend).activeTime = time.Time{}
	b.(*testBackend).RWMutex.Unlock()

	time.Sleep(time.Second * 1)
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

	b.(*testBackend).activeTime = time.Time{}

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
