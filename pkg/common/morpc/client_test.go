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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/assert"
)

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
		WithClientEnableAutoCreateBackend())
	assert.NoError(t, err)
	c := rc.(*client)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	// First backend
	b, err := c.getBackend("b1", false)
	if err != nil {
		time.Sleep(50 * time.Millisecond)
		b, err = c.getBackend("b1", false)
	}
	assert.NoError(t, err)
	assert.NotNil(t, b)
	b.(*testBackend).busy = true

	// Second backend - trigger async creation
	_, _ = c.getBackend("b1", false)
	// Wait for async creation
	for i := 0; i < 20; i++ {
		c.mu.Lock()
		v := len(c.mu.backends["b1"])
		c.mu.Unlock()
		if v == 2 {
			break
		}
		time.Sleep(time.Millisecond * 10)
	}

	b, err = c.getBackend("b1", false)
	assert.NoError(t, err)
	assert.NotNil(t, b)

	b2, err := c.getBackend("b1", false)
	assert.NoError(t, err)
	assert.NotNil(t, b2)
	assert.NotEqual(t, b, b2)

	go func() {
		ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
		defer cancel()
		st, err := b2.NewStream(false)
		assert.NoError(t, err)
		for {
			assert.NoError(t, st.Send(ctx, newTestMessage(1)))
			time.Sleep(time.Millisecond * 10)
		}
	}()

	for {
		c.mu.Lock()
		v := len(c.mu.backends["b1"])
		c.mu.Unlock()
		if v == 1 {
			tb := b.(*testBackend)
			tb.RLock()
			closed := tb.closed
			tb.RUnlock()
			if closed {
				tb2 := b2.(*testBackend)
				tb2.RLock()
				assert.False(t, tb2.closed)
				tb2.RUnlock()

				c.mu.Lock()
				assert.Equal(t, 1, len(c.mu.backends["b1"]))
				c.mu.Unlock()
				return
			}
		}
		time.Sleep(time.Millisecond * 10)
	}
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
