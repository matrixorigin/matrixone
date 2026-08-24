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

package cache

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type observerMock struct {
	MockSearch
	waiters  int64
	finished bool
}

type invalidationMock struct {
	MockSearch
	reasons []string
}

func (m *invalidationMock) OnCacheInvalidated(reason string) {
	m.reasons = append(m.reasons, reason)
}

func (m *observerMock) SetLoadWaiters(n int64) { m.waiters = n }
func (m *observerMock) FinishLoadObservation() { m.finished = true }

func TestVectorIndexSearchCompletesLoadObserverAfterWaiterSample(t *testing.T) {
	mock := &observerMock{}
	s := &VectorIndexSearch{Algo: mock}
	s.Cond = sync.NewCond(s.Mutex.RLocker())
	s.loadWaiters.Store(5)
	require.NoError(t, s.Load(nil))
	require.Equal(t, int64(5), mock.waiters)
	require.True(t, mock.finished)
	s.Destroy()
}

func TestVectorIndexSearchDoesNotNotifyEmptyInvalidationReason(t *testing.T) {
	mock := &invalidationMock{}
	s := &VectorIndexSearch{Algo: mock}
	s.Cond = sync.NewCond(s.Mutex.RLocker())
	s.Destroy()
	require.Empty(t, mock.reasons)
}

func TestVectorIndexCacheLifecycleHookRunsForEmptyShutdown(t *testing.T) {
	var shutdown atomic.Bool
	RegisterLifecycleHook(func(isShutdown bool) {
		if isShutdown {
			shutdown.Store(true)
		}
	})

	c := NewVectorIndexCache()
	c.Destroy()
	require.True(t, shutdown.Load())
}

func TestVectorIndexCacheLifecycleHookPanicDoesNotStopLaterHooks(t *testing.T) {
	var laterCalled atomic.Bool
	lifecycleHooks.Lock()
	previous := lifecycleHooks.hooks
	lifecycleHooks.hooks = []func(bool){
		func(bool) { panic("synthetic lifecycle hook panic") },
		func(shutdown bool) {
			if shutdown {
				laterCalled.Store(true)
			}
		},
	}
	lifecycleHooks.Unlock()
	t.Cleanup(func() {
		lifecycleHooks.Lock()
		lifecycleHooks.hooks = previous
		lifecycleHooks.Unlock()
	})

	require.NotPanics(t, func() { NewVectorIndexCache().Destroy() })
	require.True(t, laterCalled.Load())
}

type blockingInvalidationMock struct {
	invalidationMock
	started chan struct{}
	release chan struct{}
}

func (m *blockingInvalidationMock) OnCacheInvalidated(reason string) {
	close(m.started)
	<-m.release
	m.invalidationMock.OnCacheInvalidated(reason)
}

func TestVectorIndexCacheHouseKeepingPublishesBeforeDelete(t *testing.T) {
	mock := &blockingInvalidationMock{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	s := &VectorIndexSearch{Algo: mock}
	s.Cond = sync.NewCond(s.Mutex.RLocker())
	s.ExpireAt.Store(time.Now().Add(-time.Second).UnixMicro())
	c := NewVectorIndexCache()
	c.IndexMap.Store("key", s)

	done := make(chan struct{})
	go func() {
		c.HouseKeeping()
		close(done)
	}()

	<-mock.started
	replacement := &VectorIndexSearch{Algo: &MockSearch{}}
	replacement.Cond = sync.NewCond(replacement.Mutex.RLocker())
	value, loaded := c.IndexMap.LoadOrStore("key", replacement)
	require.True(t, loaded)
	require.Same(t, s, value)

	close(mock.release)
	<-done
	_, loaded = c.IndexMap.Load("key")
	require.False(t, loaded)
	require.Equal(t, []string{"ttl_expired"}, mock.reasons)
}
