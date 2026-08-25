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

	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
)

type observerMock struct {
	MockSearch
	waiters           int64
	finished          bool
	invalidated       string
	invalidatedCalls  int
	invalidatedReady  chan struct{}
	allowInvalidation chan struct{}
	destroyStarted    chan struct{}
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
func (m *observerMock) OnCacheInvalidated(reason string) {
	m.invalidated = reason
	m.invalidatedCalls++
	if m.invalidatedReady != nil {
		close(m.invalidatedReady)
		<-m.allowInvalidation
	}
}
func (m *observerMock) Destroy() {
	if m.destroyStarted != nil {
		close(m.destroyStarted)
	}
}

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

func TestVectorIndexSearchDestroyWithReasonNotifiesOptionalHook(t *testing.T) {
	mock := &observerMock{}
	s := &VectorIndexSearch{Algo: mock}
	s.Cond = sync.NewCond(s.Mutex.RLocker())
	s.DestroyWithReason("cdc_flush")
	require.Equal(t, "cdc_flush", mock.invalidated)

	mock = &observerMock{}
	s = &VectorIndexSearch{Algo: mock}
	s.Cond = sync.NewCond(s.Mutex.RLocker())
	s.Destroy()
	require.Empty(t, mock.invalidated)
}

func TestVectorIndexCacheRemoveDoesNotNotifyOptionalHook(t *testing.T) {
	mock := &observerMock{}
	s := &VectorIndexSearch{Algo: mock}
	s.Cond = sync.NewCond(s.Mutex.RLocker())
	c := NewVectorIndexCache()
	c.IndexMap.Store("key", s)

	c.Remove("key")
	require.Empty(t, mock.invalidated)
}

func TestVectorIndexCacheExplicitInvalidationThenRemoveNotifiesOnce(t *testing.T) {
	mock := &observerMock{}
	s := &VectorIndexSearch{Algo: mock}
	s.Cond = sync.NewCond(s.Mutex.RLocker())
	c := NewVectorIndexCache()
	c.IndexMap.Store("key", s)

	mock.OnCacheInvalidated("merge")
	c.Remove("key")

	require.Equal(t, 1, mock.invalidatedCalls)
	require.Equal(t, "merge", mock.invalidated)
}

func TestVectorIndexCacheHouseKeepingPublishesReasonBeforeRemovingEntry(t *testing.T) {
	mock := &observerMock{
		invalidatedReady:  make(chan struct{}),
		allowInvalidation: make(chan struct{}),
		destroyStarted:    make(chan struct{}),
	}
	s := &VectorIndexSearch{Algo: mock}
	s.Cond = sync.NewCond(s.Mutex.RLocker())
	s.ExpireAt.Store(time.Now().Add(-time.Second).UnixMicro())
	c := NewVectorIndexCache()
	c.IndexMap.Store("key", s)

	s.Mutex.Lock()
	mutexHeld := true
	releasedInvalidation := false
	done := make(chan struct{})
	defer func() {
		if !releasedInvalidation {
			close(mock.allowInvalidation)
		}
		if mutexHeld {
			s.Mutex.Unlock()
		}
		select {
		case <-done:
		case <-time.After(time.Second):
		}
	}()
	go func() {
		c.HouseKeeping()
		close(done)
	}()

	select {
	case <-mock.invalidatedReady:
	case <-time.After(time.Second):
		t.Fatal("housekeeping did not publish the invalidation before waiting on the old entry")
	}
	value, loaded := c.IndexMap.Load("key")
	require.True(t, loaded, "the old entry must still occupy the key while its reason is published")
	require.Same(t, s, value)
	replacement := &VectorIndexSearch{Algo: &observerMock{}}
	replacement.Cond = sync.NewCond(replacement.Mutex.RLocker())
	actual, loaded := c.IndexMap.LoadOrStore("key", replacement)
	require.True(t, loaded, "a replacement must not start before the old entry is removed")
	require.Same(t, s, actual)

	close(mock.allowInvalidation)
	releasedInvalidation = true
	require.Eventually(t, func() bool {
		_, ok := c.IndexMap.Load("key")
		return !ok
	}, time.Second, time.Millisecond)
	select {
	case <-mock.destroyStarted:
		t.Fatal("destruction must remain behind the active reader lock")
	default:
	}
	s.Mutex.Unlock()
	mutexHeld = false

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("housekeeping did not finish after the active reader released")
	}
	require.Equal(t, 1, mock.invalidatedCalls)
	require.Equal(t, "ttl_expired", mock.invalidated)
}

func TestVectorIndexCacheHouseKeepingSkipsReplacedSnapshotEntry(t *testing.T) {
	makeExpired := func(mock *observerMock) *VectorIndexSearch {
		s := &VectorIndexSearch{Algo: mock}
		s.Cond = sync.NewCond(s.Mutex.RLocker())
		s.ExpireAt.Store(time.Now().Add(-time.Second).UnixMicro())
		return s
	}
	oldA := makeExpired(&observerMock{
		invalidatedReady:  make(chan struct{}),
		allowInvalidation: make(chan struct{}),
	})
	oldB := makeExpired(&observerMock{
		invalidatedReady:  make(chan struct{}),
		allowInvalidation: make(chan struct{}),
	})
	c := NewVectorIndexCache()
	c.IndexMap.Store("a", oldA)
	c.IndexMap.Store("b", oldB)

	done := make(chan struct{})
	go func() {
		c.HouseKeeping()
		close(done)
	}()

	var capturedKey string
	var capturedMock *observerMock
	select {
	case <-oldA.Algo.(*observerMock).invalidatedReady:
		capturedKey = "a"
		capturedMock = oldA.Algo.(*observerMock)
		c.Remove("b")
	case <-oldB.Algo.(*observerMock).invalidatedReady:
		capturedKey = "b"
		capturedMock = oldB.Algo.(*observerMock)
		c.Remove("a")
	}

	replacementKey := "a"
	if capturedKey == "a" {
		replacementKey = "b"
	}
	replacement := &VectorIndexSearch{Algo: &observerMock{}}
	replacement.Cond = sync.NewCond(replacement.Mutex.RLocker())
	replacement.ExpireAt.Store(time.Now().Add(time.Hour).UnixMicro())
	c.IndexMap.Store(replacementKey, replacement)

	close(capturedMock.allowInvalidation)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("housekeeping did not finish after the captured entry was released")
	}

	value, loaded := c.IndexMap.Load(replacementKey)
	require.True(t, loaded)
	require.Same(t, replacement, value)
	_, loaded = c.IndexMap.Load(capturedKey)
	require.False(t, loaded, "the captured expired entry should be evicted")
}

func TestVectorIndexCacheHouseKeepingSkipsConcurrentlyRenewedSnapshotEntry(t *testing.T) {
	makeExpiredLoaded := func(mock *observerMock) *VectorIndexSearch {
		s := &VectorIndexSearch{Algo: mock}
		s.Cond = sync.NewCond(s.Mutex.RLocker())
		s.Status.Store(STATUS_LOADED)
		s.ExpireAt.Store(time.Now().Add(-time.Second).UnixMicro())
		return s
	}
	firstMock := &observerMock{
		invalidatedReady:  make(chan struct{}),
		allowInvalidation: make(chan struct{}),
	}
	secondMock := &observerMock{
		invalidatedReady:  make(chan struct{}),
		allowInvalidation: make(chan struct{}),
	}
	first := makeExpiredLoaded(firstMock)
	second := makeExpiredLoaded(secondMock)
	c := NewVectorIndexCache()
	c.IndexMap.Store("first", first)
	c.IndexMap.Store("second", second)

	done := make(chan struct{})
	go func() {
		c.HouseKeeping()
		close(done)
	}()

	var renewedKey string
	select {
	case <-firstMock.invalidatedReady:
		renewedKey = "second"
	case <-secondMock.invalidatedReady:
		renewedKey = "first"
	case <-time.After(time.Second):
		t.Fatal("housekeeping did not start evicting an expired snapshot entry")
	}

	_, _, err := c.Search(nil, renewedKey, &MockSearch{}, nil, vectorindex.RuntimeConfig{})
	require.NoError(t, err)
	value, loaded := c.IndexMap.Load(renewedKey)
	require.True(t, loaded)
	renewed := value.(*VectorIndexSearch)
	require.False(t, renewed.Expired(), "the concurrent search must renew the snapshot entry")

	close(firstMock.allowInvalidation)
	close(secondMock.allowInvalidation)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("housekeeping did not finish after invalidation was released")
	}

	value, loaded = c.IndexMap.Load(renewedKey)
	require.True(t, loaded, "a successful concurrent search must prevent snapshot eviction")
	require.Same(t, renewed, value)
	c.Remove("first")
	c.Remove("second")
}
