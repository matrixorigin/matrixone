// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package cache

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	usearch "github.com/unum-cloud/usearch/golang"
)

const fastStaleRegistryTestSize = 1024

type MockSearch struct {
	Idxcfg vectorindex.IndexConfig
	Tblcfg vectorindex.IndexTableConfig
}

type transientMockSearch struct {
	loads     int
	searches  int
	destroys  int
	transient bool
	loadErr   error
}

func (m *transientMockSearch) UseTransientLoad(*sqlexec.SqlProcess) (bool, error) {
	return m.transient, nil
}

func (m *transientMockSearch) Load(*sqlexec.SqlProcess) error { m.loads++; return m.loadErr }
func (m *transientMockSearch) Destroy()                       { m.destroys++ }
func (m *transientMockSearch) CoherenceRetryPolicy() (int, []time.Duration) {
	return 4, []time.Duration{0, 0, 0}
}
func (m *transientMockSearch) Search(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig) (any, []float64, error) {
	m.searches++
	return []int64{1}, []float64{2}, nil
}
func (m *transientMockSearch) SearchFloat32(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, []int64, []float32) error {
	return nil
}
func (m *transientMockSearch) SearchInto(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, *vectorindex.SearchOutput) error {
	m.searches++
	return nil
}

func TestTransientLoadNeverPublishesGlobalEntry(t *testing.T) {
	c := NewVectorIndexCache()
	algo := &transientMockSearch{transient: true}

	keys, distances, err := c.Search(nil, "retired", algo, nil, vectorindex.RuntimeConfig{})
	require.NoError(t, err)
	require.Equal(t, []int64{1}, keys)
	require.Equal(t, []float64{2}, distances)
	_, published := c.IndexMap.Load("retired")
	require.False(t, published)
	require.Equal(t, 1, algo.loads)
	require.Equal(t, 1, algo.searches)
	require.Equal(t, 1, algo.destroys)

	require.NoError(t, c.SearchInto(nil, "retired", algo, nil, vectorindex.RuntimeConfig{}, &vectorindex.SearchOutput{}))
	_, published = c.IndexMap.Load("retired")
	require.False(t, published)
	require.Equal(t, 2, algo.loads)
	require.Equal(t, 2, algo.searches)
	require.Equal(t, 2, algo.destroys)
}

func TestTransientLoadRetryIsBounded(t *testing.T) {
	c := NewVectorIndexCache()
	algo := &transientMockSearch{
		transient: true,
		loadErr:   NewRetryableLoadError(moerr.NewInvalidStateNoCtx("generation superseded")),
	}

	_, _, err := c.Search(nil, "retired", algo, nil, vectorindex.RuntimeConfig{})
	require.ErrorContains(t, err, "cache coherence retry exhausted")
	require.Equal(t, 4, algo.loads)
	require.Equal(t, 4, algo.destroys)
	_, published := c.IndexMap.Load("retired")
	require.False(t, published)
}

func (m *MockSearch) Search(sqlproc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {
	//time.Sleep(2 * time.Millisecond)
	return []int64{1}, []float64{2.0}, nil
}

func (m *MockSearch) Destroy() {
}

func (m *MockSearch) Load(*sqlexec.SqlProcess) error {
	//time.Sleep(6 * time.Second)
	return nil
}

func (m *MockSearch) SearchFloat32(sqlproc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig, outKeys []int64, outDists []float32) error {
	return nil
}

type MockAnySearch struct {
	Idxcfg vectorindex.IndexConfig
	Tblcfg vectorindex.IndexTableConfig
}

func (m *MockAnySearch) Search(sqlproc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {
	//time.Sleep(2 * time.Millisecond)
	return []any{any(1)}, []float64{2.0}, nil
}

func (m *MockAnySearch) Destroy() {
}

func (m *MockAnySearch) Load(*sqlexec.SqlProcess) error {
	//time.Sleep(6 * time.Second)
	return nil
}

func (m *MockAnySearch) SearchFloat32(sqlproc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig, outKeys []int64, outDists []float32) error {
	return nil
}

// Load Error
type MockSearchLoadError struct {
	Idxcfg vectorindex.IndexConfig
	Tblcfg vectorindex.IndexTableConfig
}

func (m *MockSearchLoadError) Search(sqlproc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {
	return []int64{1}, []float64{2.0}, nil
}

func (m *MockSearchLoadError) Destroy() {

}

func (m *MockSearchLoadError) Load(*sqlexec.SqlProcess) error {
	return moerr.NewInternalErrorNoCtx("Load from database error")
}

func (m *MockSearchLoadError) SearchFloat32(sqlproc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig, outKeys []int64, outDists []float32) error {
	return nil
}

// Search Error
type MockSearchSearchError struct {
	Idxcfg vectorindex.IndexConfig
	Tblcfg vectorindex.IndexTableConfig
}

type boundedRetrySearch struct {
	loads atomic.Int32
}

type staleSweepTracker struct {
	active  atomic.Int32
	maximum atomic.Int32
	started atomic.Int32
	release chan struct{}
}

type fastStaleSearch struct {
	MockSearch
	tracker   *staleSweepTracker
	uncertain atomic.Bool
}

type scriptedFastStaleSearch struct {
	MockSearch
	stale bool
	err   error
}

type freshnessUncertaintySearch struct {
	MockSearch
	uncertain atomic.Bool
	fail      atomic.Bool
	loads     atomic.Int32
}

func (m *freshnessUncertaintySearch) Load(*sqlexec.SqlProcess) error {
	m.loads.Add(1)
	return nil
}

func (m *freshnessUncertaintySearch) IsStale() (bool, error) {
	return m.IsStaleWithContext(context.Background())
}

func (m *freshnessUncertaintySearch) IsStaleWithContext(context.Context) (bool, error) {
	if m.fail.Load() {
		return false, moerr.NewInternalErrorNoCtx("freshness query failed")
	}
	return false, nil
}

func (m *freshnessUncertaintySearch) OnFreshnessUncertain() {
	m.uncertain.Store(true)
}

func (m *freshnessUncertaintySearch) OnFreshnessConfirmed() {
	m.uncertain.Store(false)
}

func (m *freshnessUncertaintySearch) UseTransientLoad(*sqlexec.SqlProcess) (bool, error) {
	return m.uncertain.Load(), nil
}

type controlledFastStaleSearch struct {
	MockSearch
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	stale   bool
	err     error
}

type historicalStaleSearch struct {
	MockSearch
	checks atomic.Int32
}

type blockingLoadSearch struct {
	entered   chan struct{}
	release   chan struct{}
	destroyed chan struct{}
	once      sync.Once
}

type blockingFastStaleSearch struct {
	entered   chan struct{}
	release   chan struct{}
	destroyed chan struct{}
	once      sync.Once
}

func (*blockingFastStaleSearch) Load(*sqlexec.SqlProcess) error { return nil }
func (s *blockingFastStaleSearch) Search(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig) (any, []float64, error) {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return nil, nil, nil
}
func (s *blockingFastStaleSearch) SearchInto(proc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig, _ *vectorindex.SearchOutput) error {
	_, _, err := s.Search(proc, query, rt)
	return err
}
func (*blockingFastStaleSearch) SearchFloat32(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, []int64, []float32) error {
	return nil
}
func (s *blockingFastStaleSearch) Destroy()                                       { close(s.destroyed) }
func (*blockingFastStaleSearch) IsStale() (bool, error)                           { return true, nil }
func (*blockingFastStaleSearch) IsStaleWithContext(context.Context) (bool, error) { return true, nil }

func (s *blockingLoadSearch) Load(*sqlexec.SqlProcess) error {
	close(s.entered)
	<-s.release
	return nil
}
func (*blockingLoadSearch) Search(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig) (any, []float64, error) {
	return nil, nil, nil
}
func (*blockingLoadSearch) SearchInto(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, *vectorindex.SearchOutput) error {
	return nil
}
func (*blockingLoadSearch) SearchFloat32(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, []int64, []float32) error {
	return nil
}
func (s *blockingLoadSearch) Destroy()                                   { s.once.Do(func() { close(s.destroyed) }) }
func (*blockingLoadSearch) CoherenceRetryPolicy() (int, []time.Duration) { return 1, nil }

func (m *historicalStaleSearch) IsStale() (bool, error) {
	m.checks.Add(1)
	return true, nil
}

func (m *fastStaleSearch) IsStale() (bool, error) {
	return m.IsStaleWithContext(context.Background())
}

func (m *fastStaleSearch) IsStaleWithContext(ctx context.Context) (bool, error) {
	active := m.tracker.active.Add(1)
	defer m.tracker.active.Add(-1)
	m.tracker.started.Add(1)
	for {
		maximum := m.tracker.maximum.Load()
		if active <= maximum || m.tracker.maximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	select {
	case <-ctx.Done():
		return true, ctx.Err()
	case <-m.tracker.release:
		return true, nil
	}
}

func (m *fastStaleSearch) OnFreshnessUncertain() {
	m.uncertain.Store(true)
}

func (m *fastStaleSearch) OnFreshnessConfirmed() {
	m.uncertain.Store(false)
}

func (m *scriptedFastStaleSearch) IsStale() (bool, error) {
	return m.stale, m.err
}

func (m *scriptedFastStaleSearch) IsStaleWithContext(context.Context) (bool, error) {
	return m.stale, m.err
}

func (m *controlledFastStaleSearch) IsStale() (bool, error) {
	return m.IsStaleWithContext(context.Background())
}

func (m *controlledFastStaleSearch) IsStaleWithContext(ctx context.Context) (bool, error) {
	m.once.Do(func() { close(m.entered) })
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-m.release:
		return m.stale, m.err
	}
}

func (m *boundedRetrySearch) Load(*sqlexec.SqlProcess) error {
	m.loads.Add(1)
	return NewRetryableLoadError(moerr.NewInvalidStateNoCtx("superseded"))
}
func (*boundedRetrySearch) Destroy() {}
func (*boundedRetrySearch) Search(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig) (any, []float64, error) {
	return nil, nil, nil
}
func (*boundedRetrySearch) SearchInto(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, *vectorindex.SearchOutput) error {
	return nil
}
func (*boundedRetrySearch) SearchFloat32(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, []int64, []float32) error {
	return nil
}
func (*boundedRetrySearch) CoherenceRetryPolicy() (int, []time.Duration) {
	return 4, []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}
}

func TestCacheBoundsOptInCoherenceRetry(t *testing.T) {
	proc := sqlexec.NewSqlProcess(testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()))
	for _, searchInto := range []bool{false, true} {
		c := NewVectorIndexCache()
		algo := &boundedRetrySearch{}
		if searchInto {
			err := c.SearchInto(proc, "bounded", algo, nil, vectorindex.RuntimeConfig{}, &vectorindex.SearchOutput{})
			require.ErrorContains(t, err, "coherence retry exhausted")
		} else {
			_, _, err := c.Search(proc, "bounded", algo, nil, vectorindex.RuntimeConfig{})
			require.ErrorContains(t, err, "coherence retry exhausted")
		}
		require.Equal(t, int32(4), algo.loads.Load())
	}
}

func TestCacheCoherenceRetryHonorsCancellation(t *testing.T) {
	proc := sqlexec.NewSqlProcess(testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()))
	ctx, cancel := context.WithCancel(context.Background())
	proc.Proc.Ctx = ctx
	cancel()
	c := NewVectorIndexCache()
	algo := &boundedRetrySearch{}
	_, _, err := c.Search(proc, "canceled", algo, nil, vectorindex.RuntimeConfig{})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int32(1), algo.loads.Load())
}

func TestClaimRemoveDefersInFlightLoadDestructionWithoutBlocking(t *testing.T) {
	c := NewVectorIndexCache()
	algo := &blockingLoadSearch{entered: make(chan struct{}), release: make(chan struct{}), destroyed: make(chan struct{})}
	searchDone := make(chan error, 1)
	go func() {
		_, _, err := c.Search(nil, "loading", algo, nil, vectorindex.RuntimeConfig{})
		searchDone <- err
	}()
	select {
	case <-algo.entered:
	case <-time.After(time.Second):
		t.Fatal("load did not start")
	}
	claimDone := make(chan struct{})
	go func() {
		c.ClaimRemoveWithReason("loading", "generation_changed")
		close(claimDone)
	}()
	select {
	case <-claimDone:
	case <-time.After(time.Second):
		t.Fatal("claim waited for the in-flight load")
	}
	_, present := c.IndexMap.Load("loading")
	require.False(t, present)
	close(algo.release)
	require.ErrorContains(t, <-searchDone, "coherence retry exhausted")
	select {
	case <-algo.destroyed:
	case <-time.After(time.Second):
		t.Fatal("superseded load object was not destroyed")
	}
}

func TestFastStaleSweepIsSingleFlightBoundedAndRetainsUnknownEntries(t *testing.T) {
	proc := sqlexec.NewSqlProcess(testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()))
	c := NewVectorIndexCache()
	c.fastStaleTimeout = 50 * time.Millisecond
	tracker := &staleSweepTracker{release: make(chan struct{})}
	t.Cleanup(func() { close(tracker.release) })
	algos := make([]*fastStaleSearch, 0, 17)
	for i := 0; i < 17; i++ {
		key := fmt.Sprintf("fulltext2:%d", i)
		algo := &fastStaleSearch{tracker: tracker}
		algos = append(algos, algo)
		_, _, err := c.Search(proc, key, algo, nil, vectorindex.RuntimeConfig{})
		require.NoError(t, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.True(t, c.startStaleCheck(ctx, true))
	require.False(t, c.startStaleCheck(ctx, true))
	require.Eventually(t, func() bool { return tracker.started.Load() == 16 }, time.Second, time.Millisecond)
	require.Equal(t, int32(16), tracker.maximum.Load())
	require.Eventually(t, func() bool { return !c.fastStaleChecking.Load() }, time.Second, time.Millisecond)
	require.Equal(t, int32(16), tracker.started.Load())
	for i := 0; i < 17; i++ {
		_, ok := c.IndexMap.Load(fmt.Sprintf("fulltext2:%d", i))
		require.True(t, ok)
		require.True(t, algos[i].uncertain.Load())
	}
}

func TestFastStaleSweepBoundsWholeRegistryWithoutEvictionStorm(t *testing.T) {
	proc := sqlexec.NewSqlProcess(testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()))
	c := NewVectorIndexCache()
	c.fastStaleTimeout = 50 * time.Millisecond
	tracker := &staleSweepTracker{release: make(chan struct{})}
	t.Cleanup(func() { close(tracker.release) })
	beforeDeadline := promtestutil.ToFloat64(metricv2.VectorIndexCacheFreshnessSweepEntriesCounter.WithLabelValues(freshnessOutcomeDeadline))
	for i := 0; i < fastStaleRegistryTestSize; i++ {
		key := fmt.Sprintf("fulltext2:blocked:%d", i)
		_, _, err := c.Search(proc, key, &fastStaleSearch{tracker: tracker}, nil, vectorindex.RuntimeConfig{})
		require.NoError(t, err)
	}

	started := time.Now()
	stats := c.checkStale(context.Background(), true)
	require.Less(t, time.Since(started), time.Second)
	require.Equal(t, fastStaleCheckConcurrency, int(tracker.started.Load()))
	require.Equal(t, int32(fastStaleCheckConcurrency), tracker.maximum.Load())
	require.Equal(t, fastStaleRegistryTestSize, stats.deadline)
	require.Equal(t, fastStaleRegistryTestSize, stats.fresh+stats.stale+stats.queryError+stats.deadline)
	require.Equal(t, beforeDeadline+fastStaleRegistryTestSize,
		promtestutil.ToFloat64(metricv2.VectorIndexCacheFreshnessSweepEntriesCounter.WithLabelValues(freshnessOutcomeDeadline)))
	require.Zero(t, stats.fresh)
	require.Zero(t, stats.stale)
	require.Zero(t, stats.queryError)
	for i := 0; i < fastStaleRegistryTestSize; i++ {
		_, ok := c.IndexMap.Load(fmt.Sprintf("fulltext2:blocked:%d", i))
		require.True(t, ok)
	}
}

func TestFastStaleSweepChecksWholeFreshRegistry(t *testing.T) {
	proc := sqlexec.NewSqlProcess(testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()))
	c := NewVectorIndexCache()
	for i := 0; i < fastStaleRegistryTestSize; i++ {
		key := fmt.Sprintf("fulltext2:fresh:%d", i)
		_, _, err := c.Search(proc, key, &scriptedFastStaleSearch{}, nil, vectorindex.RuntimeConfig{})
		require.NoError(t, err)
	}

	stats := c.checkStale(context.Background(), true)
	require.Equal(t, fastStaleRegistryTestSize, stats.fresh)
	require.Zero(t, stats.stale)
	require.Zero(t, stats.queryError)
	require.Zero(t, stats.deadline)
	for i := 0; i < fastStaleRegistryTestSize; i++ {
		_, ok := c.IndexMap.Load(fmt.Sprintf("fulltext2:fresh:%d", i))
		require.True(t, ok)
	}
}

func TestFastStaleSweepRecordsTerminalOutcomes(t *testing.T) {
	proc := sqlexec.NewSqlProcess(testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()))
	c := NewVectorIndexCache()
	beforeFresh := promtestutil.ToFloat64(metricv2.VectorIndexCacheFreshnessSweepEntriesCounter.WithLabelValues(freshnessOutcomeFresh))
	beforeStale := promtestutil.ToFloat64(metricv2.VectorIndexCacheFreshnessSweepEntriesCounter.WithLabelValues(freshnessOutcomeStale))
	beforeError := promtestutil.ToFloat64(metricv2.VectorIndexCacheFreshnessSweepEntriesCounter.WithLabelValues(freshnessOutcomeQueryError))

	for key, algo := range map[string]*scriptedFastStaleSearch{
		"fulltext2:outcome:fresh": {},
		"fulltext2:outcome:stale": {stale: true},
		"fulltext2:outcome:error": {err: moerr.NewInternalErrorNoCtx("freshness query failed")},
	} {
		_, _, err := c.Search(proc, key, algo, nil, vectorindex.RuntimeConfig{})
		require.NoError(t, err)
	}

	stats := c.checkStale(context.Background(), true)
	require.Equal(t, freshnessSweepStats{fresh: 1, stale: 1, queryError: 1}, stats)
	require.Equal(t, beforeFresh+1, promtestutil.ToFloat64(metricv2.VectorIndexCacheFreshnessSweepEntriesCounter.WithLabelValues(freshnessOutcomeFresh)))
	require.Equal(t, beforeStale+1, promtestutil.ToFloat64(metricv2.VectorIndexCacheFreshnessSweepEntriesCounter.WithLabelValues(freshnessOutcomeStale)))
	require.Equal(t, beforeError+1, promtestutil.ToFloat64(metricv2.VectorIndexCacheFreshnessSweepEntriesCounter.WithLabelValues(freshnessOutcomeQueryError)))
	metricCount, err := promtestutil.GatherAndCount(metricv2.GetPrometheusGatherer(), "mo_vector_index_cache_freshness_sweep_duration_seconds")
	require.NoError(t, err)
	require.Equal(t, 1, metricCount)
	_, stalePresent := c.IndexMap.Load("fulltext2:outcome:stale")
	_, errorPresent := c.IndexMap.Load("fulltext2:outcome:error")
	require.False(t, stalePresent)
	require.True(t, errorPresent, "a query error must not trigger a global reload")
}

func TestFastStaleUncertaintyDoesNotEvictOrRepublish(t *testing.T) {
	proc := sqlexec.NewSqlProcess(testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()))
	c := NewVectorIndexCache()
	algo := &freshnessUncertaintySearch{}
	algo.fail.Store(true)
	const key = "fulltext2:uncertain"

	_, _, err := c.Search(proc, key, algo, nil, vectorindex.RuntimeConfig{})
	require.NoError(t, err)
	require.Equal(t, int32(1), algo.loads.Load())
	before, ok := c.IndexMap.Load(key)
	require.True(t, ok)

	stats := c.checkStale(context.Background(), true)
	require.Equal(t, 1, stats.queryError)
	require.True(t, algo.uncertain.Load())
	after, ok := c.IndexMap.Load(key)
	require.True(t, ok, "an unknown generation must not trigger periodic global eviction")
	require.Same(t, before, after)

	_, _, err = c.Search(proc, key, algo, nil, vectorindex.RuntimeConfig{})
	require.NoError(t, err)
	require.Equal(t, int32(2), algo.loads.Load(), "an uncertain query may load transiently")
	after, ok = c.IndexMap.Load(key)
	require.True(t, ok)
	require.Same(t, before, after, "an old transaction snapshot must not replace the global entry")

	algo.fail.Store(false)
	stats = c.checkStale(context.Background(), true)
	require.Equal(t, 1, stats.fresh)
	require.False(t, algo.uncertain.Load())
	_, _, err = c.Search(proc, key, algo, nil, vectorindex.RuntimeConfig{})
	require.NoError(t, err)
	require.Equal(t, int32(2), algo.loads.Load(), "a successful durable read should restore the warm cache")
}

func TestFastStaleSweepDoesNotEvictReplacement(t *testing.T) {
	for _, tc := range []struct {
		name    string
		stale   bool
		err     error
		release bool
		outcome string
		timeout time.Duration
	}{
		{name: "stale", stale: true, release: true, outcome: freshnessOutcomeStale},
		{name: "query-error", err: moerr.NewInternalErrorNoCtx("freshness query failed"), release: true, outcome: freshnessOutcomeQueryError},
		{name: "deadline", outcome: freshnessOutcomeDeadline, timeout: 50 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := sqlexec.NewSqlProcess(testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()))
			c := NewVectorIndexCache()
			if tc.timeout > 0 {
				c.fastStaleTimeout = tc.timeout
			}
			old := &controlledFastStaleSearch{
				entered: make(chan struct{}), release: make(chan struct{}), stale: tc.stale, err: tc.err,
			}
			key := "fulltext2:replacement:" + tc.name
			_, _, err := c.Search(proc, key, old, nil, vectorindex.RuntimeConfig{})
			require.NoError(t, err)

			done := make(chan freshnessSweepStats, 1)
			go func() { done <- c.checkStale(context.Background(), true) }()
			select {
			case <-old.entered:
			case <-time.After(time.Second):
				t.Fatal("freshness check did not reach the old entry")
			}
			c.ClaimRemoveWithReason(key, "generation_changed")
			_, _, err = c.Search(proc, key, &scriptedFastStaleSearch{}, nil, vectorindex.RuntimeConfig{})
			require.NoError(t, err)
			replacement, ok := c.IndexMap.Load(key)
			require.True(t, ok)
			if tc.release {
				close(old.release)
			}
			stats := <-done
			if !tc.release {
				close(old.release)
			}
			switch tc.outcome {
			case freshnessOutcomeStale:
				require.Equal(t, 1, stats.stale)
			case freshnessOutcomeQueryError:
				require.Equal(t, 1, stats.queryError)
			case freshnessOutcomeDeadline:
				require.Equal(t, 1, stats.deadline)
			}
			current, ok := c.IndexMap.Load(key)
			require.True(t, ok)
			require.Same(t, replacement, current)
		})
	}
}

func TestFastStaleSweepDoesNotChangeHistoricalVectorCheckerSemantics(t *testing.T) {
	proc := sqlexec.NewSqlProcess(testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()))
	c := NewVectorIndexCache()
	algo := &historicalStaleSearch{}
	_, _, err := c.Search(proc, "historical-vector", algo, nil, vectorindex.RuntimeConfig{})
	require.NoError(t, err)

	c.checkStale(context.Background(), true)
	require.Zero(t, algo.checks.Load())
	_, ok := c.IndexMap.Load("historical-vector")
	require.True(t, ok)

	c.checkStale(context.Background(), false)
	require.Equal(t, int32(1), algo.checks.Load())
	_, ok = c.IndexMap.Load("historical-vector")
	require.True(t, ok, "historical sweep only marks for the next housekeeping pass")
	c.HouseKeeping()
	_, ok = c.IndexMap.Load("historical-vector")
	require.False(t, ok)
}

func TestFastStaleSweepClaimsWithoutWaitingForOldReader(t *testing.T) {
	proc := sqlexec.NewSqlProcess(testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()))
	c := NewVectorIndexCache()
	algo := &blockingFastStaleSearch{
		entered: make(chan struct{}), release: make(chan struct{}), destroyed: make(chan struct{}),
	}
	searchDone := make(chan error, 1)
	go func() {
		_, _, err := c.Search(proc, "fulltext2:blocking", algo, nil, vectorindex.RuntimeConfig{})
		searchDone <- err
	}()
	<-algo.entered

	sweepDone := make(chan struct{})
	go func() {
		c.checkStale(context.Background(), true)
		close(sweepDone)
	}()
	select {
	case <-sweepDone:
	case <-time.After(time.Second):
		t.Fatal("fast stale sweep waited for an old reader lease")
	}
	_, ok := c.IndexMap.Load("fulltext2:blocking")
	require.False(t, ok)
	select {
	case <-algo.destroyed:
		t.Fatal("old object destroyed before its reader released")
	default:
	}

	close(algo.release)
	require.NoError(t, <-searchDone)
	select {
	case <-algo.destroyed:
	case <-time.After(time.Second):
		t.Fatal("last reader did not destroy the claimed object")
	}
}

func (m *MockSearchSearchError) Search(sqlproc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {
	return nil, nil, moerr.NewInternalErrorNoCtx("Search error")
}

func (m *MockSearchSearchError) Destroy() {

}

func (m *MockSearchSearchError) Load(*sqlexec.SqlProcess) error {
	return nil
}

func (m *MockSearchSearchError) SearchFloat32(sqlproc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig, outKeys []int64, outDists []float32) error {
	return nil
}

func (m *MockSearchSearchError) UpdateConfig(newalgo VectorIndexSearchIf) error {
	return nil
}

type runtimeSearchCall struct {
	RequestedIncludeColumns []string
	PushdownFilterSQL       string
	SearchCursor            *vectorindex.IvfSearchCursor
}

type MockRuntimeSearch struct {
	loads       int
	searchCalls []runtimeSearchCall
}

func (m *MockRuntimeSearch) Search(proc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {
	m.searchCalls = append(m.searchCalls, runtimeSearchCall{
		RequestedIncludeColumns: append([]string(nil), rt.RequestedIncludeColumns...),
		PushdownFilterSQL:       rt.PushdownFilterSQL,
		SearchCursor:            rt.SearchCursor,
	})
	if rt.SearchCursor != nil {
		rt.SearchCursor.Round = uint(len(m.searchCalls))
	}
	return []int64{1}, []float64{2.0}, nil
}

func (m *MockRuntimeSearch) SearchFloat32(proc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig, outKeys []int64, outDists []float32) error {
	keys, distances, err := m.Search(proc, query, rt)
	if err != nil {
		return err
	}
	if typedKeys, ok := keys.([]int64); ok {
		for i, key := range typedKeys {
			outKeys[i] = key
		}
	}
	for i, dist := range distances {
		outDists[i] = float32(dist)
	}
	return nil
}

func (m *MockRuntimeSearch) Destroy() {}

func (m *MockRuntimeSearch) Load(*sqlexec.SqlProcess) error {
	m.loads++
	return nil
}

func (m *MockRuntimeSearch) UpdateConfig(newalgo VectorIndexSearchIf) error {
	return nil
}

func TestCacheServe(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	sqlproc := sqlexec.NewSqlProcess(proc)
	Cache = NewVectorIndexCache()
	Cache.serve()
	Cache.serve()
	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(8)}
	idxcfg.Usearch.Metric = usearch.L2sq
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src", MetadataTable: "__secondary_meta", IndexTable: "__secondary_index"}
	m := &MockSearch{Idxcfg: idxcfg, Tblcfg: tblcfg}
	fp32a := []float32{1, 2, 3, 4, 5, 6, 7, 8}
	anykeys, distances, err := Cache.Search(sqlproc, tblcfg.IndexTable, m, fp32a, vectorindex.RuntimeConfig{Limit: 4})
	require.Nil(t, err)
	if keys, ok := anykeys.([]int64); ok {
		require.Equal(t, len(keys), 1)
		require.Equal(t, keys[0], int64(1))
	}
	require.Equal(t, distances[0], float64(2.0))

	Cache.Remove(tblcfg.IndexTable)

	Cache.Destroy()
}

// IVF-FLAT keys its cache entries "<indexTable>:<version>" (plus a
// "/cnIdx/cnCnt" suffix when the read is split across CNs), so DROP INDEX /
// DROP TABLE cannot name the live key — it evicts by prefix. Every generation
// of the dropped index must go, and no other index table may be touched.
func TestCacheRemovePrefix(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	sqlproc := sqlexec.NewSqlProcess(proc)
	Cache = NewVectorIndexCache()
	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(8)}
	idxcfg.Usearch.Metric = usearch.L2sq
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src", MetadataTable: "__secondary_meta", IndexTable: "__secondary_index"}
	fp32a := []float32{1, 2, 3, 4, 5, 6, 7, 8}

	// two generations of the dropped index + one split-read entry, and one
	// entry of a different index table that shares no prefix.
	keys := []string{
		"__secondary_index:0",
		"__secondary_index:7",
		"__secondary_index:7/1/2",
		"__other_index:0",
	}
	for _, k := range keys {
		m := &MockSearch{Idxcfg: idxcfg, Tblcfg: tblcfg}
		_, _, err := Cache.Search(sqlproc, k, m, fp32a, vectorindex.RuntimeConfig{Limit: 4})
		require.Nil(t, err)
	}

	Cache.RemovePrefix("__secondary_index:")

	for _, k := range keys[:3] {
		_, ok := Cache.IndexMap.Load(k)
		require.False(t, ok, "key %s should have been evicted", k)
	}
	_, ok := Cache.IndexMap.Load("__other_index:0")
	require.True(t, ok, "unrelated index table must not be evicted")

	// removing a prefix with no match is a no-op, not a panic
	Cache.RemovePrefix("__no_such_index:")

	Cache.Destroy()
}

func TestCacheAny(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	sqlproc := sqlexec.NewSqlProcess(proc)
	Cache = NewVectorIndexCache()
	Cache.serve()
	Cache.serve()
	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(8)}
	idxcfg.Usearch.Metric = usearch.L2sq
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src", MetadataTable: "__secondary_meta", IndexTable: "__secondary_index"}
	m := &MockAnySearch{Idxcfg: idxcfg, Tblcfg: tblcfg}
	fp32a := []float32{1, 2, 3, 4, 5, 6, 7, 8}
	anykeys, distances, err := Cache.Search(sqlproc, tblcfg.IndexTable, m, fp32a, vectorindex.RuntimeConfig{Limit: 4})
	require.Nil(t, err)
	keys, ok := anykeys.([]any)
	require.True(t, ok)
	require.Equal(t, len(keys), 1)
	require.Equal(t, keys[0], any(1))
	require.Equal(t, distances[0], float64(2.0))

	Cache.Remove(tblcfg.IndexTable)

	Cache.Destroy()
}

func TestCache(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	sqlproc := sqlexec.NewSqlProcess(proc)

	VectorIndexCacheTTL = 5 * time.Second
	VectorIndexCacheTTL = 5 * time.Second
	Cache = NewVectorIndexCache()
	Cache.TickerInterval = 5 * time.Second

	Cache.Once()
	Cache.Once()
	Cache.Once()
	Cache.Once()
	Cache.Once()

	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(8)}
	idxcfg.Usearch.Metric = usearch.L2sq
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src", MetadataTable: "__secondary_meta", IndexTable: "__secondary_index"}
	os.Stderr.WriteString("cache getindex\n")
	m := &MockSearch{Idxcfg: idxcfg, Tblcfg: tblcfg}
	os.Stderr.WriteString("cache search\n")
	fp32a := []float32{1, 2, 3, 4, 5, 6, 7, 8}
	anykeys1, distances, err := Cache.Search(sqlproc, tblcfg.IndexTable, m, fp32a, vectorindex.RuntimeConfig{Limit: 4})
	require.Nil(t, err)
	if keys1, ok := anykeys1.([]int64); ok {
		require.Equal(t, len(keys1), 1)
		require.Equal(t, keys1[0], int64(1))
	}
	require.Equal(t, distances[0], float64(2.0))

	os.Stderr.WriteString("cache sleep\n")
	time.Sleep(8 * time.Second)

	// cache expired

	// new search
	m3 := &MockSearch{Idxcfg: idxcfg, Tblcfg: tblcfg}
	anykeys2, distances, err := Cache.Search(sqlproc, tblcfg.IndexTable, m3, fp32a, vectorindex.RuntimeConfig{Limit: 4})
	require.Nil(t, err)
	if keys2, ok := anykeys2.([]int64); ok {
		require.Equal(t, len(keys2), 1)
		require.Equal(t, keys2[0], int64(1))
	}
	require.Equal(t, distances[0], float64(2.0))

	os.Stderr.WriteString("cache.Destroy\n")
	Cache.Destroy()
	os.Stderr.WriteString("cache.Destroy end\n")
	Cache = nil
}

func TestCacheConcurrent(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	sqlproc := sqlexec.NewSqlProcess(proc)

	VectorIndexCacheTTL = 2 * time.Second
	VectorIndexCacheTTL = 2 * time.Second
	Cache = NewVectorIndexCache()
	Cache.TickerInterval = 1 * time.Second

	Cache.Once()
	Cache.Once()
	Cache.Once()
	Cache.Once()
	Cache.Once()

	time.Sleep(1999 * time.Millisecond)
	var wg sync.WaitGroup
	nthread := 8
	for i := 0; i < nthread; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 2000; j++ {
				idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(8)}
				idxcfg.Usearch.Metric = usearch.L2sq
				tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src", MetadataTable: "__secondary_meta", IndexTable: "__secondary_index"}
				//os.Stderr.WriteString("cache getindex\n")
				m := &MockSearch{Idxcfg: idxcfg, Tblcfg: tblcfg}
				//os.Stderr.WriteString("cache search\n")
				fp32a := []float32{1, 2, 3, 4, 5, 6, 7, 8}
				anykeys, distances, err := Cache.Search(sqlproc, tblcfg.IndexTable, m, fp32a, vectorindex.RuntimeConfig{Limit: 4})
				require.Nil(t, err)
				if keys, ok := anykeys.([]int64); ok {
					require.Equal(t, len(keys), 1)
					require.Equal(t, keys[0], int64(1))
				}
				require.Equal(t, distances[0], float64(2.0))
			}
		}()
	}

	wg.Wait()
	time.Sleep(4 * time.Second)

	os.Stderr.WriteString("cache.Destroy\n")
	Cache.Destroy()
	os.Stderr.WriteString("cache.Destroy end\n")
	Cache = nil
}

func TestCacheConcurrentNewSearchAndDelete(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	sqlproc := sqlexec.NewSqlProcess(proc)

	VectorIndexCacheTTL = 2 * time.Second
	VectorIndexCacheTTL = 2 * time.Second
	Cache = NewVectorIndexCache()
	Cache.TickerInterval = 1 * time.Second

	Cache.Once()
	Cache.Once()
	Cache.Once()
	Cache.Once()
	Cache.Once()

	time.Sleep(1999 * time.Millisecond)
	var wg sync.WaitGroup
	nthread := 8
	for i := 0; i < nthread; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 2000; j++ {
				idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(8)}
				idxcfg.Usearch.Metric = usearch.L2sq
				tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src", MetadataTable: "__secondary_meta", IndexTable: "__secondary_index"}
				//os.Stderr.WriteString("cache getindex\n")
				m := &MockSearch{Idxcfg: idxcfg, Tblcfg: tblcfg}
				//os.Stderr.WriteString("cache search\n")
				fp32a := []float32{1, 2, 3, 4, 5, 6, 7, 8}
				anykeys, distances, err := Cache.Search(sqlproc, tblcfg.IndexTable, m, fp32a, vectorindex.RuntimeConfig{Limit: 4})
				require.Nil(t, err)
				if keys, ok := anykeys.([]int64); ok {
					require.Equal(t, len(keys), 1)
					require.Equal(t, keys[0], int64(1))
				}
				require.Equal(t, distances[0], float64(2.0))
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 4000; j++ {
			Cache.Remove("__secondary_index")
		}
	}()

	wg.Wait()

	os.Stderr.WriteString("cache.Destroy\n")
	Cache.Destroy()
	os.Stderr.WriteString("cache.Destroy end\n")
	Cache = nil
}

func TestCacheLoadError(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	sqlproc := sqlexec.NewSqlProcess(proc)

	VectorIndexCacheTTL = 5 * time.Second
	Cache = NewVectorIndexCache()
	Cache.TickerInterval = 5 * time.Second

	Cache.Once()
	Cache.Once()
	Cache.Once()
	Cache.Once()
	Cache.Once()

	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(8)}
	idxcfg.Usearch.Metric = usearch.L2sq
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src", MetadataTable: "__secondary_meta", IndexTable: "__secondary_index"}
	os.Stderr.WriteString("cache getindex\n")
	m1 := &MockSearchLoadError{Idxcfg: idxcfg, Tblcfg: tblcfg}
	fp32a := []float32{1, 2, 3, 4, 5, 6, 7, 8}
	_, _, err := Cache.Search(sqlproc, tblcfg.IndexTable, m1, fp32a, vectorindex.RuntimeConfig{Limit: 4})
	require.NotNil(t, err)

	os.Stderr.WriteString(fmt.Sprintf("error : %v\n", err))
	os.Stderr.WriteString("cache.Destroy\n")
	Cache.Destroy()
	os.Stderr.WriteString("cache.Destroy end\n")
	Cache = nil
}

func TestCacheSearchError(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	sqlproc := sqlexec.NewSqlProcess(proc)

	VectorIndexCacheTTL = 5 * time.Second
	Cache = NewVectorIndexCache()
	Cache.TickerInterval = 5 * time.Second

	Cache.Once()
	Cache.Once()
	Cache.Once()
	Cache.Once()
	Cache.Once()

	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(8)}
	idxcfg.Usearch.Metric = usearch.L2sq
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src", MetadataTable: "__secondary_meta", IndexTable: "__secondary_index"}
	os.Stderr.WriteString("cache getindex\n")
	m1 := &MockSearchSearchError{Idxcfg: idxcfg, Tblcfg: tblcfg}
	fp32a := []float32{1, 2, 3, 4, 5, 6, 7, 8}
	_, _, err := Cache.Search(sqlproc, tblcfg.IndexTable, m1, fp32a, vectorindex.RuntimeConfig{Limit: 4})
	require.NotNil(t, err)

	os.Stderr.WriteString(fmt.Sprintf("error : %v\n", err))
	os.Stderr.WriteString("cache.Destroy\n")
	Cache.Destroy()
	os.Stderr.WriteString("cache.Destroy end\n")
	Cache = nil
}

func TestCacheReuseKeepsRuntimeConfigQueryScoped(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	sqlproc := sqlexec.NewSqlProcess(proc)

	Cache = NewVectorIndexCache()
	Cache.Once()
	defer func() {
		Cache.Destroy()
		Cache = nil
	}()

	cachedAlgo := &MockRuntimeSearch{}
	_, _, err := Cache.Search(sqlproc, "__ivf_entries", cachedAlgo, []float32{1, 2, 3}, vectorindex.RuntimeConfig{
		Limit:                   4,
		RequestedIncludeColumns: []string{"title"},
		PushdownFilterSQL:       "`__mo_index_include_title` = 'alpha'",
		SearchCursor:            &vectorindex.IvfSearchCursor{},
	})
	require.NoError(t, err)

	freshAlgo := &MockRuntimeSearch{}
	_, _, err = Cache.Search(sqlproc, "__ivf_entries", freshAlgo, []float32{1, 2, 3}, vectorindex.RuntimeConfig{
		Limit:                   4,
		RequestedIncludeColumns: []string{"category"},
		PushdownFilterSQL:       "",
		SearchCursor:            &vectorindex.IvfSearchCursor{},
	})
	require.NoError(t, err)

	require.Equal(t, 1, cachedAlgo.loads)
	require.Zero(t, freshAlgo.loads)
	require.Len(t, cachedAlgo.searchCalls, 2)
	require.Equal(t, []string{"title"}, cachedAlgo.searchCalls[0].RequestedIncludeColumns)
	require.Equal(t, "`__mo_index_include_title` = 'alpha'", cachedAlgo.searchCalls[0].PushdownFilterSQL)
	require.Equal(t, uint(1), cachedAlgo.searchCalls[0].SearchCursor.Round)
	require.Equal(t, []string{"category"}, cachedAlgo.searchCalls[1].RequestedIncludeColumns)
	require.Empty(t, cachedAlgo.searchCalls[1].PushdownFilterSQL)
	require.Equal(t, uint(2), cachedAlgo.searchCalls[1].SearchCursor.Round)
}

func (m *MockSearch) SearchInto(_ *sqlexec.SqlProcess, _ any, _ vectorindex.RuntimeConfig, _ *vectorindex.SearchOutput) error {
	return nil
}

func (m *MockAnySearch) SearchInto(_ *sqlexec.SqlProcess, _ any, _ vectorindex.RuntimeConfig, _ *vectorindex.SearchOutput) error {
	return nil
}

func (m *MockSearchLoadError) SearchInto(_ *sqlexec.SqlProcess, _ any, _ vectorindex.RuntimeConfig, _ *vectorindex.SearchOutput) error {
	return nil
}

func (m *MockSearchSearchError) SearchInto(_ *sqlexec.SqlProcess, _ any, _ vectorindex.RuntimeConfig, _ *vectorindex.SearchOutput) error {
	return nil
}

func (m *MockRuntimeSearch) SearchInto(_ *sqlexec.SqlProcess, _ any, _ vectorindex.RuntimeConfig, _ *vectorindex.SearchOutput) error {
	return nil
}
