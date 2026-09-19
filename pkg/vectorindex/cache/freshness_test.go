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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

type freshnessSearch struct {
	MockSearch
	checks, destroys atomic.Int32
	check            func() (bool, error)
	destroy          func()
	invalidated      func(string)
	search           func()
}

func (m *freshnessSearch) IsStale() (bool, error) {
	m.checks.Add(1)
	return m.check()
}

func (m *freshnessSearch) Destroy() {
	m.destroys.Add(1)
	if m.destroy != nil {
		m.destroy()
	}
}

func (m *freshnessSearch) OnCacheInvalidated(reason string) {
	if m.invalidated != nil {
		m.invalidated(reason)
	}
}

func (m *freshnessSearch) Search(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig) (any, []float64, error) {
	if m.search != nil {
		m.search()
	}
	return []int64{1}, []float64{0}, nil
}

func residentFreshnessEntry(c *VectorIndexCache, key string, algo VectorIndexSearchIf) *VectorIndexSearch {
	s := newVectorIndexSearch(algo)
	s.Status.Store(STATUS_LOADED)
	s.extend(true)
	c.IndexMap.Store(key, s)
	return s
}

// Release blocking mocks even if an assertion aborts the test body.
func freshnessGate(t *testing.T) (chan struct{}, func()) {
	t.Helper()
	ch := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(ch) }) }
	t.Cleanup(release)
	return ch, release
}

func TestFreshnessTicksSingleflight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		checkGate, releaseCheck := freshnessGate(t)
		destroyGate, releaseDestroy := freshnessGate(t)
		m := &freshnessSearch{
			check:   func() (bool, error) { <-checkGate; return true, nil },
			destroy: func() { <-destroyGate },
		}
		c := NewVectorIndexCache()
		entry := residentFreshnessEntry(c, "current", m)
		c.done = make(chan bool)
		ticks := make(chan time.Time)
		go c.serveTicks(nil, ticks) // No housekeeping event or idle expiration.
		t.Cleanup(func() { c.done <- true })
		require.Equal(t, 30*time.Second, stalenessCheckInterval)
		require.Equal(t, VectorIndexCacheTTL/2, c.TickerInterval)
		ticks <- time.Now()
		synctest.Wait()
		require.EqualValues(t, 1, m.checks.Load())
		require.False(t, entry.Expired())
		for i := 0; i < 10; i++ {
			ticks <- time.Now()
		}
		synctest.Wait()
		require.EqualValues(t, 1, m.checks.Load(), "busy metadata ticks must be dropped")
		releaseCheck()
		synctest.Wait()
		_, loaded := c.IndexMap.Load("current")
		require.False(t, loaded, "retire without a housekeeping tick")
		require.True(t, c.staleChecking.Load(), "singleflight covers blocked destruction")
		for i := 0; i < 10; i++ {
			ticks <- time.Now()
		}
		synctest.Wait()
		require.EqualValues(t, 1, m.destroys.Load())
		releaseDestroy()
		synctest.Wait()
		require.False(t, c.staleChecking.Load())
		fresh := &freshnessSearch{check: func() (bool, error) { return false, nil }}
		residentFreshnessEntry(c, "current", fresh)
		ticks <- time.Now()
		synctest.Wait()
		require.EqualValues(t, 1, fresh.checks.Load(), "later ticks must resume checking")
		c.exited.Store(true)
		c.startStaleCheck()
		synctest.Wait()
		require.EqualValues(t, 1, fresh.checks.Load(), "exit seals further sweep admission")
		c.Destroy()
	})
}

func TestFreshnessCheckProtectsLifetime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gate, release := freshnessGate(t)
		invalidated := make(chan struct{})
		m := &freshnessSearch{
			check:       func() (bool, error) { <-gate; return true, nil },
			invalidated: func(string) { close(invalidated) },
		}
		c := NewVectorIndexCache()
		s := residentFreshnessEntry(c, "key", m)
		c.startStaleCheck()
		synctest.Wait()
		locked := s.Mutex.TryLock()
		if locked {
			s.Mutex.Unlock()
		}
		require.False(t, locked, "checker must pin the algorithm against teardown")
		go c.RemoveWithReason("key", "test_remove")
		<-invalidated
		require.Zero(t, m.destroys.Load())
		release()
		synctest.Wait()
		require.EqualValues(t, 1, m.destroys.Load(), "Remove and freshness share one eviction owner")
		require.Equal(t, int32(STATUS_DESTROYED), s.Status.Load())
	})
}

func TestFreshnessSweepPreservesReplacement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gate, release := freshnessGate(t)
		old := &freshnessSearch{check: func() (bool, error) { <-gate; return true, nil }}
		c := NewVectorIndexCache()
		entry := residentFreshnessEntry(c, "key", old)
		c.startStaleCheck()
		synctest.Wait()
		fresh := &freshnessSearch{check: func() (bool, error) { return false, nil }}
		replacement := residentFreshnessEntry(c, "key", fresh)
		release()
		synctest.Wait()
		value, loaded := c.IndexMap.Load("key")
		require.True(t, loaded)
		require.Same(t, replacement, value)
		require.Zero(t, fresh.destroys.Load())
		require.Zero(t, old.destroys.Load(), "lost identity must not claim another owner's teardown")
		entry.Destroy() // The test removed the old map owner directly.
		c.Destroy()
	})
}

func TestFreshnessSweepStopsOnExit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gate, release := freshnessGate(t)
		check := func() (bool, error) { <-gate; return true, nil }
		first, second := &freshnessSearch{check: check}, &freshnessSearch{check: check}
		c := NewVectorIndexCache()
		residentFreshnessEntry(c, "first", first)
		residentFreshnessEntry(c, "second", second)
		c.startStaleCheck()
		synctest.Wait()
		c.exited.Store(true)
		release()
		synctest.Wait()
		require.EqualValues(t, 1, first.checks.Load()+second.checks.Load(), "stop before another metadata call")
		require.Zero(t, first.destroys.Load()+second.destroys.Load(), "shutdown owns remaining entries")
		go c.Destroy()
		go c.Destroy()
		synctest.Wait()
		require.EqualValues(t, 1, first.destroys.Load())
		require.EqualValues(t, 1, second.destroys.Load())
	})
}

func TestFreshnessEvictionWaitsForSearch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gate, release := freshnessGate(t)
		searching, invalidated := make(chan struct{}), make(chan struct{})
		m := &freshnessSearch{
			check:       func() (bool, error) { return true, nil },
			search:      func() { close(searching); <-gate },
			invalidated: func(string) { close(invalidated) },
		}
		c := NewVectorIndexCache()
		s := residentFreshnessEntry(c, "key", m)
		var searchErr error
		go func() { _, _, searchErr = c.Search(nil, "key", &MockSearch{}, nil, vectorindex.RuntimeConfig{}) }()
		<-searching
		c.startStaleCheck()
		<-invalidated
		require.Zero(t, m.destroys.Load(), "active search must keep its generation alive")
		require.False(t, s.extendForSearch(), "sealed generation cannot admit another search")
		release()
		synctest.Wait()
		require.NoError(t, searchErr)
		require.EqualValues(t, 1, m.destroys.Load())
		require.False(t, c.staleChecking.Load())
	})
}

func TestFreshnessRunningShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gate, release := freshnessGate(t)
		invalidated := make(chan struct{})
		m := &freshnessSearch{
			check:       func() (bool, error) { <-gate; return true, nil },
			invalidated: func(string) { close(invalidated) },
		}
		c := NewVectorIndexCache()
		s := residentFreshnessEntry(c, "key", m)
		c.done = make(chan bool)
		c.started.Store(true)
		loopStopped := make(chan struct{})
		go func() {
			c.serveTicks(nil, nil)
			close(loopStopped)
		}()
		c.startStaleCheck()
		synctest.Wait()
		go c.Destroy()
		<-invalidated // Destroy has claimed the checked entry.
		<-loopStopped // The done send alone does not acknowledge the exit store.
		require.True(t, c.exited.Load())
		require.Zero(t, m.destroys.Load(), "shutdown must wait for the checker read lock")
		release()
		synctest.Wait()
		require.EqualValues(t, 1, m.destroys.Load())
		require.Equal(t, int32(STATUS_DESTROYED), s.Status.Load())
		require.False(t, c.staleChecking.Load())
	})
}

func TestFreshnessEligibilityAndErrors(t *testing.T) {
	for _, state := range []int32{STATUS_NOT_INIT, STATUS_ERROR, STATUS_DESTROYED} {
		c := NewVectorIndexCache()
		m := &freshnessSearch{check: func() (bool, error) { return true, nil }}
		s := residentFreshnessEntry(c, "key", m)
		s.Status.Store(state)
		c.checkStale()
		require.Zero(t, m.checks.Load(), "skip state %d", state)
		c.Destroy()
	}
	for _, stale := range []bool{false, true} {
		c := NewVectorIndexCache()
		m := &freshnessSearch{check: func() (bool, error) { return stale, errors.New("metadata unavailable") }}
		s := residentFreshnessEntry(c, "key", m)
		s.Mutex.Lock()
		c.checkStale() // Must skip a loading/teardown writer without blocking.
		s.Mutex.Unlock()
		require.Zero(t, m.checks.Load())
		c.checkStale()
		require.EqualValues(t, 1, m.checks.Load(), "retry after contention clears")
		_, loaded := c.IndexMap.Load("key")
		require.Equal(t, !stale, loaded, "preserve the checker's boolean/error contract")
		c.Destroy()
		require.EqualValues(t, 1, m.destroys.Load())
	}
}
