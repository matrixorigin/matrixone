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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// TestGetBackendLockedDoesNotCreateWhenAvailable ensures maybeCreateLocked is only called when no available backend was found (b == nil).
func TestGetBackendLockedDoesNotCreateWhenAvailable(t *testing.T) {
	rc, err := NewClient("",
		newTestBackendFactory(),
		WithClientMaxBackendPerHost(2),
		WithClientEnableAutoCreateBackend())
	require.NoError(t, err)
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
		require.NoError(t, err)
		require.NotNil(t, b, "iteration %d", i)
		require.Equal(t, 1, n, "getBackendLocked must not call maybeCreateLocked when b != nil; iteration %d had %d backends", i, n)
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
	rc, err := NewClient("", newTestBackendFactory(), WithClientMaxBackendPerHost(3))
	assert.NoError(t, err)
	c := rc.(*client)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	c.mu.backends["b1"] = append(c.mu.backends["b1"],
		&testBackend{id: 1, locked: true, busy: false, activeTime: time.Now()},
		&testBackend{id: 2, locked: true, busy: false, activeTime: time.Now()})
	c.mu.ops["b1"] = &op{}

	b, err := c.getBackend("b1", false)
	assert.NoError(t, err)
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
		WithClientCreateTaskChanSize(1))
	assert.NoError(t, err)
	c := rc.(*client)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	b, err := c.getBackend("b1", false)
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
	require.NoError(t, err)
	c := rc.(*client)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	// First backend with lock so getBackendLocked does not select it (b==nil) and createBackend creates second.
	_, err = c.getBackend("b1", true)
	require.NoError(t, err)
	require.NotNil(t, c.mu.backends["b1"])
	require.Equal(t, 1, len(c.mu.backends["b1"]))

	// Second backend: getBackendLocked returns nil (only backend is locked), getBackend calls createBackend.
	_, err = c.getBackend("b1", false)
	require.NoError(t, err)
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
			time.Sleep(time.Millisecond * 10)
		}
	}()

	// gcIdleTask runs every maxIdleDuration (100ms); wait for idle backend to be closed (deadline 5s).
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		v := len(c.mu.backends["b1"])
		c.mu.Unlock()
		if v == 1 {
			tb.RLock()
			closed := tb.closed
			tb.RUnlock()
			if closed {
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
		time.Sleep(time.Millisecond * 10)
	}
	t.Fatal("idle backend was not closed by GC within 5s")
}

func TestLockedBackendCannotClosedWithGCIdleTask(t *testing.T) {
	rc, err := NewClient(
		"",
		newTestBackendFactory(),
		WithClientMaxBackendPerHost(2),
		WithClientMaxBackendMaxIdleDuration(time.Millisecond*100),
		WithClientCreateTaskChanSize(1))
	assert.NoError(t, err)
	c := rc.(*client)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	b, err := c.getBackend("b1", true)
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
		WithClientCreateTaskChanSize(1))
	assert.NoError(t, err)
	c := rc.(*client)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	b, err := c.getBackend("b1", false)
	assert.NoError(t, err)
	assert.NotNil(t, b)

	b.(*testBackend).activeTime = time.Time{}
	b, _ = c.getBackend("b1", false)
	assert.Nil(t, b)

	for {
		b, err := c.getBackend("b1", false)
		if err == nil {
			assert.NotNil(t, b)
			return
		}
	}
}
