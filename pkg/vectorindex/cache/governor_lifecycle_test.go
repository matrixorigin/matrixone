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

package cache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

type governedSearch struct {
	countingSearch
	search   func() error
	destroys atomic.Int64
}

func (s *governedSearch) Search(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig) (any, []float64, error) {
	err := s.search()
	return []int64{7}, []float64{1}, err
}

func (s *governedSearch) SearchInto(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, *vectorindex.SearchOutput) error {
	return s.search()
}

func (s *governedSearch) Destroy() { s.destroys.Add(1) }

func TestGovernorAllVictimsBusyDoesNotBlockMiss(t *testing.T) {
	c := newBoundCache(t)
	sp := govProc(t, c, 1, hostCap(100), caps{})
	loadInto(t, c, sp, "busy", 100, 0)
	busy := entryOf(t, c, "busy")
	busy.Mutex.RLock()
	defer busy.Mutex.RUnlock()

	done := make(chan error, 1)
	algo := &countingSearch{host: 100}
	go func() { done <- searchWithProc(c, sp, "incoming", algo) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("a miss waited for an unrelated active search")
	}
	require.EqualValues(t, 1, algo.loads.Load())
	require.True(t, isResident(c, "busy"))
	require.False(t, isResident(c, "incoming"), "transient query data must not remain over budget")
}

func searchWithProc(c *VectorIndexCache, sp *sqlexec.SqlProcess, key string, algo VectorIndexSearchIf) error {
	_, _, err := c.Search(sp, key, algo, nil, vectorindex.RuntimeConfig{})
	return err
}

func TestGovernorOversizedQueryRetiresOnSuccessAndError(t *testing.T) {
	for _, into := range []bool{false, true} {
		for _, queryErr := range []error{nil, errors.New("search failed")} {
			for _, size := range []caps{hostCap(101), gpuCap(101)} {
				c := newBoundCache(t)
				sp := govProc(t, c, 1, caps{host: 100, device: 100}, caps{})
				algo := &governedSearch{countingSearch: countingSearch{host: size.host, device: size.device},
					search: func() error { return queryErr }}
				var err error
				if into {
					err = c.SearchInto(sp, "oversized", algo, nil, vectorindex.RuntimeConfig{}, &vectorindex.SearchOutput{})
				} else {
					err = searchWithProc(c, sp, "oversized", algo)
				}
				require.ErrorIs(t, err, queryErr)
				require.EqualValues(t, 1, algo.loads.Load())
				require.EqualValues(t, 1, algo.destroys.Load())
				require.False(t, isResident(c, "oversized"))
				entries, bytes := c.EvictionStats()
				require.EqualValues(t, 1, entries)
				require.Equal(t, size.host+size.device, bytes)
			}
		}
	}
}

func TestGovernorRetiresAfterLastSharedReader(t *testing.T) {
	c := newBoundCache(t)
	sp := govProc(t, c, 1, hostCap(100), caps{})
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	algo := &governedSearch{countingSearch: countingSearch{host: 101}, search: func() error {
		entered <- struct{}{}
		<-release
		return nil
	}}
	done := make(chan error, 2)
	go func() { done <- searchWithProc(c, sp, "shared", algo) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("loader did not reach search")
	}
	go func() { done <- searchWithProc(c, sp, "shared", &countingSearch{}) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("same-key reader did not share the loaded entry")
	}
	require.EqualValues(t, 1, algo.loads.Load())
	require.Zero(t, algo.destroys.Load())
	once.Do(func() { close(release) })
	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("reader did not return")
		}
	}
	require.False(t, isResident(c, "shared"))
	require.EqualValues(t, 1, algo.destroys.Load())
	entries, bytes := c.EvictionStats()
	require.EqualValues(t, 1, entries, "shared readers must not double-count retirement")
	require.EqualValues(t, 101, bytes)
}

func TestGovernorMaintenanceRefreshesCatalogWithoutMiss(t *testing.T) {
	c := newBoundCache(t)
	sp := govProc(t, c, 1, caps{}, hostCap(1000))
	loadInto(t, c, sp, "warm1", 100, 0)
	loadInto(t, c, sp, "warm2", 100, 0)
	entryOf(t, c, "warm1").ExpireAt.Store(time.Now().Add(time.Hour).UnixMicro())
	entryOf(t, c, "warm2").ExpireAt.Store(time.Now().Add(2 * time.Hour).UnixMicro())
	// Change only the catalog producer, not the memoized cap under test.
	c.sysLimit.fetched = time.Now().Add(-2 * sysLimitTTL)
	var reads int
	runSysSql = func(ctx context.Context, sid string, account uint32, db, sql string) (executor.Result, error) {
		reads++
		require.Equal(t, "gov-test-cn", sid)
		require.Zero(t, account)
		_, deadline := ctx.Deadline()
		require.True(t, deadline)
		return varRows(t, mpool.MustNewZero(), maxIndexCacheSizeVar, "100"), nil
	}
	c.refreshCacheLimits()
	require.Equal(t, 1, reads)
	require.False(t, isResident(c, "warm1"))
	require.True(t, isResident(c, "warm2"))
	c.refreshCacheLimits()
	require.Equal(t, 1, reads, "maintenance respects the same refresh cadence as queries")
}

func TestGovernorOversizedQueryRetiresOnPanic(t *testing.T) {
	for _, into := range []bool{false, true} {
		c := newBoundCache(t)
		sp := govProc(t, c, 1, hostCap(100), caps{})
		algo := &governedSearch{countingSearch: countingSearch{host: 101},
			search: func() error { panic("search panic") }}
		require.PanicsWithValue(t, "search panic", func() {
			if into {
				_ = c.SearchInto(sp, "panic", algo, nil, vectorindex.RuntimeConfig{}, &vectorindex.SearchOutput{})
			} else {
				_ = searchWithProc(c, sp, "panic", algo)
			}
		})
		require.EqualValues(t, 1, algo.destroys.Load())
		require.False(t, isResident(c, "panic"))
	}
}

func TestGovernorProtectsFirstSearchFromCompetingMiss(t *testing.T) {
	c := newBoundCache(t)
	sp := govProc(t, c, 1, hostCap(100), caps{})
	first := newVectorIndexSearch(&countingSearch{host: 100})
	c.IndexMap.Store("first", first)
	require.NoError(t, first.Preload(sp))
	require.NoError(t, first.Load(sp))
	c.chargeAndEnforce(sp, "first", first)
	// Pause between publication of LOADED and the loader's first Search.
	// A competing miss must not steal that entry even though no reader holds it.
	loadInto(t, c, sp, "second", 100, 0)
	require.True(t, isResident(c, "first"))
	require.False(t, isResident(c, "second"))
	_, _, err := c.searchEntry(sp, "first", first, first.Algo, nil, vectorindex.RuntimeConfig{}, true)
	require.NoError(t, err)
	require.False(t, first.loadingQuery.Load())
	require.True(t, isResident(c, "first"))
}

func TestGovernorBackendInvalidStateIsNotRetriedAfterRetirement(t *testing.T) {
	for _, into := range []bool{false, true} {
		for _, size := range []int64{1, 101} {
			c := newBoundCache(t)
			sp := govProc(t, c, 1, hostCap(100), caps{})
			cause := moerr.NewInvalidStateNoCtx("backend unavailable")
			var calls int
			algo := &governedSearch{countingSearch: countingSearch{host: size}, search: func() error {
				calls++
				if calls > 1 {
					// A finite counterexample also fails under the old retry loop.
					return errors.New("unexpected retry")
				}
				return cause
			}}
			var err error
			if into {
				err = c.SearchInto(sp, "backend-error", algo, nil, vectorindex.RuntimeConfig{}, &vectorindex.SearchOutput{})
			} else {
				err = searchWithProc(c, sp, "backend-error", algo)
			}
			require.Same(t, cause, err)
			require.Equal(t, 1, calls)
			require.Equal(t, size <= 100, isResident(c, "backend-error"))
		}
	}
}

func BenchmarkCacheWarmSearchInto(b *testing.B) {
	c := NewVectorIndexCache()
	algo := &countingSearch{host: 1}
	var out vectorindex.SearchOutput
	rt := vectorindex.RuntimeConfig{}
	if err := c.SearchInto(nil, "warm", algo, nil, rt, &out); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { c.Remove("warm") })
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := c.SearchInto(nil, "warm", algo, nil, rt, &out); err != nil {
			b.Fatal(err)
		}
	}
}
