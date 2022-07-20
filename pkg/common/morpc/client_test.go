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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCreateBackendLocked(t *testing.T) {
	rc, err := NewClient(newTestBackendFactory(), WithClientMaxBackendPerHost(1))
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
	assert.Equal(t, errNoAvailableBackend, err)
	assert.Nil(t, b)
	assert.Equal(t, 1, len(c.mu.backends["b1"]))
}

func TestGetBackendLockedWithClosed(t *testing.T) {
	rc, err := NewClient(newTestBackendFactory(), WithClientMaxBackendPerHost(1))
	assert.NoError(t, err)
	c := rc.(*client)
	assert.NoError(t, c.Close())

	b, err := c.getBackendLocked("b1")
	assert.Error(t, err)
	assert.Equal(t, errClientClosed, err)
	assert.Nil(t, b)
}

func TestGetBackendLockedWithEmptyBackends(t *testing.T) {
	rc, err := NewClient(newTestBackendFactory(), WithClientMaxBackendPerHost(1 /*disable create*/))
	assert.NoError(t, err)
	c := rc.(*client)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	b, err := c.getBackendLocked("b1")
	assert.NoError(t, err)
	assert.Nil(t, b)
}

func TestGetBackend(t *testing.T) {
	rc, err := NewClient(newTestBackendFactory(), WithClientMaxBackendPerHost(1 /*disable create*/))
	assert.NoError(t, err)
	c := rc.(*client)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	n := 100
	for i := 0; i < n; i++ {
		c.mu.backends["b1"] = append(c.mu.backends["b1"], &testBackend{id: i, busy: false})
	}
	c.mu.ops["b1"] = &op{}

	for i := 0; i < n; i++ {
		b, err := c.getBackend("b1")
		assert.NoError(t, err)
		assert.Equal(t, c.mu.backends["b1"][(i+1)%n], b)
	}
}

func TestMaybeCreateLockedWithEmptyBackends(t *testing.T) {
	rc, err := NewClient(newTestBackendFactory(), WithClientMaxBackendPerHost(1))
	assert.NoError(t, err)
	c := rc.(*client)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	assert.True(t, c.maybeCreateLocked("b1"))
}

func TestMaybeCreateLockedWithDisableCreateTask(t *testing.T) {
	rc, err := NewClient(newTestBackendFactory(), WithClientMaxBackendPerHost(1), WithClientDisableCreateTask())
	assert.NoError(t, err)
	c := rc.(*client)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	assert.False(t, c.maybeCreateLocked("b1"))
}

func TestMaybeCreateLockedWithNotFullBackendsAndHasAnyBusy(t *testing.T) {
	rc, err := NewClient(newTestBackendFactory(), WithClientMaxBackendPerHost(3))
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
	rc, err := NewClient(newTestBackendFactory(), WithClientMaxBackendPerHost(3))
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
	rc, err := NewClient(newTestBackendFactory(), WithClientMaxBackendPerHost(1))
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

func TestTryCreateWithFullBufferedChan(t *testing.T) {
	rc, err := NewClient(newTestBackendFactory(), WithClientCreateTaskChanSize(1), WithClientDisableCreateTask())
	assert.NoError(t, err)
	c := rc.(*client)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	assert.True(t, c.tryCreate("b1"))
	assert.False(t, c.tryCreate("b1"))
}

func TestInitBackendsAndMaxBackendsPerHostNotMatch(t *testing.T) {
	rc, err := NewClient(newTestBackendFactory(),
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
	rc, err := NewClient(newTestBackendFactory(),
		WithClientCreateTaskChanSize(1))
	assert.NoError(t, err)
	c := rc.(*client)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	b, err := c.getBackend("b1")
	assert.NoError(t, err)
	assert.NotNil(t, b)
	assert.Equal(t, 1, len(c.mu.backends["b1"]))
}

func TestCloseIdleBackends(t *testing.T) {
	rc, err := NewClient(newTestBackendFactory(),
		WithClientMaxBackendPerHost(2),
		WithClientMaxBackendMaxIdleDuration(time.Millisecond*100),
		WithClientCreateTaskChanSize(1))
	assert.NoError(t, err)
	c := rc.(*client)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	b, err := c.getBackend("b1")
	assert.NoError(t, err)
	assert.NotNil(t, b)
	b.(*testBackend).busy = true

	_, err = c.getBackend("b1")
	assert.NoError(t, err)
	for {
		c.mu.RLock()
		v := len(c.mu.backends["b1"])
		c.mu.RUnlock()
		if v == 2 {
			break
		}
	}

	b, err = c.getBackend("b1")
	assert.NoError(t, err)
	assert.NotNil(t, b)

	b2, err := c.getBackend("b1")
	assert.NoError(t, err)
	assert.NotNil(t, b2)
	assert.NotEqual(t, b, b2)

	go func() {
		st, err := b2.NewStream()
		assert.NoError(t, err)
		for {
			assert.NoError(t, st.Send(newTestMessage(1), SendOptions{}))
		}
	}()

	for {
		c.mu.RLock()
		v := len(c.mu.backends["b1"])
		c.mu.RUnlock()
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

				c.mu.RLock()
				assert.Equal(t, 1, len(c.mu.backends["b1"]))
				c.mu.RUnlock()
				return
			}
		}
	}
}
