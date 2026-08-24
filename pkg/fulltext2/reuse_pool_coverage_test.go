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

package fulltext2

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

func TestReusableBasePoolBoundaryStates(t *testing.T) {
	p := &immutableBasePool{entries: make(map[baseKey]*baseEntry)}
	maxEntries, maxBytes, idleTTL := p.limits()
	require.Equal(t, basePoolMaxEntries, maxEntries)
	require.Equal(t, basePoolMaxBytes, maxBytes)
	require.Equal(t, basePoolIdleTTL, idleTTL)
	require.Zero(t, baseEntryBytes(baseKey{}))
	require.Equal(t, int64(7), baseEntryBytes(baseKey{filesize: 7}))

	zeroLease := &segmentLease{}
	zeroLease.release()
	zeroLease.retire()
	lease := newSegmentLease(NewSegment("lease", 0))
	view := lease.acquire(0)
	require.NotNil(t, view)
	view.Free()
	lease.retire()
	lease.release()
	lease.retire()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	key := baseKey{index: "db.store", id: "cancelled", filesize: 1}
	_, err := p.acquire(ctx, key, func() (*Segment, error) {
		return nil, errors.New("cancelled context became the loader")
	}, 0)
	require.ErrorIs(t, err, context.Canceled)

	_, err = p.acquire(nil, key, func() (*Segment, error) {
		return nil, nil
	}, 0)
	require.ErrorIs(t, err, errBaseLeaseGone)
	require.Empty(t, p.entries)

	_, err = p.acquire(nil, key, func() (*Segment, error) {
		panic("synthetic loader panic")
	}, 0)
	require.ErrorContains(t, err, "base load panic")
	require.Empty(t, p.entries)

	p.entries[baseKey{index: "loading"}] = &baseEntry{loading: true}
	p.entries[baseKey{index: "empty"}] = &baseEntry{}
	p.evict(time.Now())
	loading := p.entries[baseKey{index: "loading"}]
	require.NotNil(t, loading)
	p.commit("loading", nil)
	require.True(t, loading.retired)
	loading.loading = false
	p.clearAll()
	require.Empty(t, p.entries)

	key = baseKey{index: "db.store", id: "kept", filesize: 1}
	view, err = p.acquire(context.Background(), key, func() (*Segment, error) {
		return NewSegment(key.id, 0), nil
	}, 0)
	require.NoError(t, err)
	view.Free()
	p.commit(key.index, map[baseKey]struct{}{key: {}})
	require.Contains(t, p.entries, key)
	p.clearIndex(key.index)
	require.Empty(t, p.entries)
}

func TestLoadReasonRegistryBoundariesAndInvalidation(t *testing.T) {
	restore := setLoadObserver(func(LoadEvent) {})
	defer restore()

	pendingLoadReasons.Lock()
	old := pendingLoadReasons.m
	pendingLoadReasons.m = make(map[string]pendingLoadReason)
	pendingLoadReasons.Unlock()
	t.Cleanup(func() {
		pendingLoadReasons.Lock()
		pendingLoadReasons.m = old
		pendingLoadReasons.Unlock()
	})

	require.Equal(t, "store", loadReasonKey("", "store"))
	rememberLoadReason("", LoadMissCDCFlush)
	rememberLoadReason("store", "")
	reason, at := peekLoadReason("store")
	require.Empty(t, reason)
	require.True(t, at.IsZero())

	pendingLoadReasons.Lock()
	pendingLoadReasons.m["expired"] = pendingLoadReason{
		reason: LoadMissCDCFlush,
		at:     time.Now().Add(-loadReasonTTL),
	}
	pendingLoadReasons.Unlock()
	rememberLoadReason("fresh", LoadMissMerge)
	reason, at = peekLoadReason("expired")
	require.Empty(t, reason)
	require.True(t, at.IsZero())
	reason, at = peekLoadReason("fresh")
	require.Equal(t, LoadMissMerge, reason)
	consumeLoadReason("fresh", at)

	for i := 0; i < loadReasonSize+1; i++ {
		rememberLoadReason(fmt.Sprintf("db.%d", i), LoadMissCDCFlush)
	}
	pendingLoadReasons.Lock()
	require.LessOrEqual(t, len(pendingLoadReasons.m), loadReasonSize)
	pendingLoadReasons.Unlock()

	cfg := TableConfig{DbName: "db", IndexTable: "store"}
	invalidateLoadGeneration(cfg, LoadMissCDCFlush)
	reason, at = peekLoadReason(loadReasonKey(cfg.DbName, cfg.IndexTable))
	require.Equal(t, LoadMissCDCFlush, reason)
	consumeLoadReason(loadReasonKey(cfg.DbName, cfg.IndexTable), at)
	invalidateLoadGeneration(cfg, LoadMissTTLExpired)
	invalidateLoadGeneration(cfg, LoadMissGenerationChange)
	invalidateLoadGeneration(cfg, LoadMissMerge)
	invalidateLoadGeneration(cfg, LoadMissRebuild)
	invalidateLoadGeneration(cfg, LoadMissReason("process_shutdown"))
}

func TestLoadReasonRegistryDoesNotConsumeNewerInvalidation(t *testing.T) {
	restore := setLoadObserver(func(LoadEvent) {})
	defer restore()

	pendingLoadReasons.Lock()
	old := pendingLoadReasons.m
	pendingLoadReasons.m = make(map[string]pendingLoadReason)
	pendingLoadReasons.Unlock()
	t.Cleanup(func() {
		pendingLoadReasons.Lock()
		pendingLoadReasons.m = old
		pendingLoadReasons.Unlock()
	})

	key := loadReasonKey("db", "store")
	rememberLoadReason(key, LoadMissCDCFlush)
	firstReason, firstAt := peekLoadReason(key)
	require.Equal(t, LoadMissCDCFlush, firstReason)
	rememberLoadReason(key, LoadMissRebuild)
	consumeLoadReason(key, firstAt)
	secondReason, secondAt := peekLoadReason(key)
	require.Equal(t, LoadMissRebuild, secondReason)
	consumeLoadReason(key, secondAt)
}

func TestReusableLoadLifecycleHookRunsHousekeepingAndShutdown(t *testing.T) {
	ensureReusableLoadLifecycle()
	c := veccache.NewVectorIndexCache()
	c.HouseKeeping()
	c.Destroy()
}

func TestLoadTailWithReuseReusesUnchangedState(t *testing.T) {
	index := "db.store"
	loadedTailPool.clear(index)
	t.Cleanup(func() { loadedTailPool.clear(index) })
	state := newTailState(7, 11, []*Segment{NewSegment("tail", 11)}, nil)
	views, _ := loadedTailPool.installAndAcquire(index, state)
	freeSegs(views)

	segs, deletes, maxChunk, err := loadTailWithReuse(nil, TableConfig{DbName: "db", IndexTable: "store"}, 7, 11, nil)
	require.NoError(t, err)
	require.Len(t, segs, 1)
	require.Nil(t, deletes)
	require.Equal(t, int64(11), maxChunk)
	freeSegs(segs)
}

func TestTailPoolBoundaryAccountingAndReplacement(t *testing.T) {
	require.Zero(t, estimateSegmentBytes(nil))
	seg := &Segment{
		pks:               []any{int64(1)},
		pkOffsets:         []int32{0},
		pkRaw:             []byte("pk"),
		docLen:            []int32{2},
		includeTypes:      []int32{int32(types.T_int64)},
		includeVals:       [][]any{{int64(7)}},
		includeRaw:        []byte("include"),
		includeVarOffsets: []int32{0},
		terms: map[string]*termPostings{
			"term": {docIDs: []int64{1}, tfs: []uint8{1}, positions: [][]int32{{2, 3}}},
		},
	}
	require.Positive(t, estimateSegmentBytes(seg))
	owned := *seg
	owned.ownedBytes = 64
	require.Greater(t, estimateSegmentBytes(&owned), int64(64))
	require.Equal(t, int64(35), estimateDeleteKeyBytes("abc"))
	require.Equal(t, int64(35), estimateDeleteKeyBytes([]byte("abc")))
	require.Equal(t, int64(32), estimateDeleteKeyBytes(7))
	require.Nil(t, cloneDeletes(nil))
	require.Equal(t, map[any]int64{"old": 2, "new": 1}, mergeTailDeletes(
		map[any]int64{"old": 1}, map[any]int64{"old": 2, "new": 1}))
	views, deletes := acquireTailViews(nil)
	require.Nil(t, views)
	require.Nil(t, deletes)

	p := &tailPool{states: make(map[string]*tailState)}
	expected := newTailState(1, 10, []*Segment{NewSegment("old", 0)}, nil)
	_, _, ok := p.acquireViews("missing", expected)
	require.False(t, ok)
	require.False(t, p.current("missing", expected))
	state := newTailState(1, 11, []*Segment{NewSegment("delta", 0)}, nil)
	_, _, ok = p.installDeltaAndAcquire("missing", expected, state)
	require.False(t, ok)
	retireTailStates([]*tailState{state})

	views, _ = p.installAndAcquire("db.store", expected)
	for _, view := range views {
		view.Free()
	}
	state = p.state("db.store")
	require.Same(t, expected, state)
	views, deletes, ok = p.acquireViews("db.store", expected)
	require.True(t, ok)
	require.Nil(t, deletes)
	for _, view := range views {
		view.Free()
	}
	require.True(t, p.current("db.store", expected))
	views, _ = p.installAndAcquire("db.store", expected)
	for _, view := range views {
		view.Free()
	}

	replacement := newTailState(1, 12, []*Segment{NewSegment("replacement", 0)}, nil)
	views, _ = p.installAndAcquire("db.store", replacement)
	for _, view := range views {
		view.Free()
	}
	p.clear("missing")
	p.clear("db.store")
	p.clearAll()
}

func TestSearchLifecycleInvalidationAndPrefilterErrors(t *testing.T) {
	s := NewFulltext2Search(TableConfig{DbName: "db", IndexTable: "store"})
	s.OnCacheInvalidated("")
	s.OnCacheInvalidated(string(LoadMissCDCFlush))
	s.OnCacheInvalidated(string(LoadMissTTLExpired))
	s.OnCacheInvalidated(string(LoadMissGenerationChange))
	s.FinishLoadObservation()
	s.SetLoadWaiters(3)

	proc := newSearchProc(t)
	loaded := loadedSearch(t)
	defer loaded.Destroy()
	_, _, err := loaded.Search(proc, Fulltext2Query{
		Pattern:          []byte("fox"),
		IncludePredsJSON: []byte("{"),
	}, vectorindex.RuntimeConfig{Limit: 10})
	require.Error(t, err)
	keys, _, err := loaded.Search(proc, Fulltext2Query{
		Pattern:    []byte("fox"),
		Algo:       BM25,
		ScoreRange: &ScoreRange{HasMin: true, Min: -1},
	}, vectorindex.RuntimeConfig{Limit: 10})
	require.NoError(t, err)
	require.NotEmpty(t, keys)
}

func TestLoadTraceConcurrentPhaseUpdates(t *testing.T) {
	events := make(chan LoadEvent, 1)
	restore := setLoadObserver(func(event LoadEvent) { events <- event })
	defer restore()

	trace := newLoadTrace("db.store", LoadMissProcessStart)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			trace.addInternalSQL(time.Microsecond)
			trace.addTempWrite(time.Microsecond)
			trace.addMmap(time.Microsecond)
			trace.addChecksum(time.Microsecond)
			trace.addBaseBytes(1)
			trace.addTailBytes(1)
		}()
	}
	wg.Wait()
	trace.finish(nil, false, 2)

	event := <-events
	require.Equal(t, int64(4), event.BaseBytes)
	require.Equal(t, int64(4), event.TailBytes)
	require.Equal(t, int64(2), event.SingleflightWaiters)
	require.NotZero(t, event.InternalSQLTimeMicros)
	require.NotZero(t, event.TempFileWriteMicros)
	require.NotZero(t, event.MmapMicros)
	require.NotZero(t, event.ChecksumMicros)
}

func TestStorageBudgetAfterAndEmptyFilesize(t *testing.T) {
	sp, mp := mockSqlProc(t)
	cfg := testStorageCfg()
	trace := &loadTrace{}
	var seenSQL string
	swapRunSql(t, func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		seenSQL = sql
		return executor.Result{Mp: mp, Batches: []*batch.Batch{int64Batch(mp, 0)}}, nil
	})
	require.NoError(t, checkTailLoadBudgetAfter(sp, cfg, 7, trace))
	require.Contains(t, seenSQL, "chunk_id > 7")

	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: mp, Batches: []*batch.Batch{metaBatch(mp, "sum", 0, 1)}}, nil
	})
	_, err := LoadFromStorage(sp, cfg, "empty")
	require.ErrorContains(t, err, "empty filesize")
}
