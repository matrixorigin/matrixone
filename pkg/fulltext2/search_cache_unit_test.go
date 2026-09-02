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
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

func newSearchProc(t *testing.T) *sqlexec.SqlProcess {
	return sqlexec.NewSqlProcess(testutil.NewProc(t))
}

// loadedSearch builds a Fulltext2Search whose Index is assembled in memory (via the
// serialize→deserialize loadedSeg path), bypassing Load/DB.
func loadedSearch(t *testing.T) *Fulltext2Search {
	bb := NewBuilder("base", int32(types.T_int64))
	feed(t, bb, int64(0), "quick", "brown", "fox")
	feed(t, bb, int64(1), "quick", "brown", "dog")
	feed(t, bb, int64(2), "lazy", "fox", "sleeps")
	seg := loadedSeg(t, bb)
	s := NewFulltext2Search(TableConfig{IndexTable: "__store", Parser: ParserDefault})
	s.idx = NewIndex([]*Segment{seg}, nil)
	s.loaded = true
	return s
}

func TestFulltext2SearchNewAndUnloaded(t *testing.T) {
	proc := newSearchProc(t)
	s := NewFulltext2Search(TableConfig{IndexTable: "__store"})
	require.Equal(t, "__store", s.cfg.IndexTable)
	require.False(t, s.loaded)

	// Search before Load → "not loaded".
	_, _, err := s.Search(proc, Fulltext2Query{Pattern: []byte("fox")}, vectorindex.RuntimeConfig{})
	require.ErrorContains(t, err, "not loaded")

	// SearchFloat32 is unsupported.
	require.ErrorContains(t, s.SearchFloat32(proc, nil, vectorindex.RuntimeConfig{}, nil, nil), "not supported")
}

func TestFulltext2SearchLoadFailurePreservesOtherGeneration(t *testing.T) {
	loadedBasePool.clearAll()
	loadedTailPool.clearAll()
	t.Cleanup(func() {
		loadedBasePool.clearAll()
		loadedTailPool.clearAll()
	})

	cfg := testStorageCfg()
	baseKey := baseKey{index: "db.__store", id: "base-0", checksum: "sum", filesize: 1}
	view, err := loadedBasePool.acquire(context.Background(), baseKey, func() (*Segment, error) {
		return NewSegment("base-0", 0), nil
	}, 0)
	require.NoError(t, err)
	view.Free()
	tailViews, _ := loadedTailPool.installAndAcquire("db.__store", newTailState(1, 1, []*Segment{NewSegment("tail-1", 0)}, nil))
	freeSegs(tailViews)

	sp, mp := mockSqlProc(t)
	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: mp, Batches: []*batch.Batch{int64Batch(mp, int64(1)<<50)}}, nil
	})
	s := NewFulltext2Search(cfg)
	require.Error(t, s.Load(sp))

	loadedBasePool.mu.Lock()
	baseEntries := len(loadedBasePool.entries)
	loadedBasePool.mu.Unlock()
	loadedTailPool.mu.Lock()
	tailStates := len(loadedTailPool.states)
	loadedTailPool.mu.Unlock()
	require.Equal(t, 1, baseEntries)
	require.Equal(t, 1, tailStates)
}

func TestFulltext2SearchLoadConsumesInvalidationReasonAfterSuccess(t *testing.T) {
	for _, reason := range []LoadMissReason{LoadMissCDCFlush, LoadMissMerge, LoadMissRebuild} {
		t.Run(string(reason), func(t *testing.T) {
			sp := &sqlexec.SqlProcess{SqlCtx: sqlexec.NewSqlContext(context.Background(), "cn-1", nil, 7, nil)}
			mp := mpool.MustNewZero()
			cfg := testStorageCfg()
			cfg.AccountID = 7
			var events []LoadEvent
			restore := setLoadObserver(func(event LoadEvent) { events = append(events, event) })
			defer restore()

			pendingLoadReasons.Lock()
			oldReasons := pendingLoadReasons.m
			pendingLoadReasons.m = make(map[string]pendingLoadReason)
			pendingLoadReasons.Unlock()
			loadGenerations.Lock()
			oldGenerations := loadGenerations.m
			loadGenerations.m = make(map[string]loadGenerationState)
			loadGenerations.Unlock()
			t.Cleanup(func() {
				pendingLoadReasons.Lock()
				pendingLoadReasons.m = oldReasons
				pendingLoadReasons.Unlock()
				loadGenerations.Lock()
				loadGenerations.m = oldGenerations
				loadGenerations.Unlock()
			})

			swapRunSql(t, func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
				switch {
				case strings.Contains(sql, "CAST(COALESCE"):
					return executor.Result{Mp: mp, Batches: []*batch.Batch{int64Batch(mp, 0)}}, nil
				case strings.Contains(sql, "MAX(timestamp)") && strings.Contains(sql, "MAX(chunk_id)"):
					return executor.Result{Mp: mp, Batches: []*batch.Batch{generationBatch(mp, 11, 22)}}, nil
				case strings.Contains(sql, "LENGTH("):
					return executor.Result{Mp: mp, Batches: []*batch.Batch{int64Batch(mp, 0)}}, nil
				default:
					return executor.Result{Mp: mp}, nil
				}
			})
			invalidateLoadGeneration(cfg, reason)

			s := NewFulltext2Search(cfg)
			require.NoError(t, s.Load(sp))
			s.FinishLoadObservation()
			require.Len(t, events, 1)
			require.Equal(t, reason, events[0].MissReason)
			require.Equal(t, int64(11), events[0].BaseGeneration)
			require.Equal(t, int64(22), events[0].TailGeneration)
			require.True(t, events[0].LoadSuccess)
			gotReason, generation := peekLoadReason(cfg.cacheIdentity().Key())
			require.Empty(t, gotReason)
			require.Zero(t, generation)
			s.Destroy()
		})
	}
}

func TestFulltext2SearchLoadClassifiesQueryInterruptionAsCancel(t *testing.T) {
	sp := &sqlexec.SqlProcess{SqlCtx: sqlexec.NewSqlContext(context.Background(), "cn-1", nil, 7, nil)}
	var event LoadEvent
	restore := setLoadObserver(func(got LoadEvent) { event = got })
	defer restore()
	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{}, moerr.NewQueryInterrupted(context.Background())
	})

	s := NewFulltext2Search(testStorageCfg())
	require.Error(t, s.Load(sp))
	s.FinishLoadObservation()
	require.True(t, event.LoadCancel)
	require.False(t, event.LoadError)
	require.False(t, event.LoadSuccess)
}

func TestFulltext2SearchLoadRetainsInvalidationReasonAfterFailure(t *testing.T) {
	for _, reason := range []LoadMissReason{LoadMissCDCFlush, LoadMissMerge, LoadMissRebuild} {
		t.Run(string(reason), func(t *testing.T) {
			sp := &sqlexec.SqlProcess{SqlCtx: sqlexec.NewSqlContext(context.Background(), "cn-1", nil, 7, nil)}
			cfg := testStorageCfg()
			cfg.AccountID = 7
			var events []LoadEvent
			restore := setLoadObserver(func(got LoadEvent) { events = append(events, got) })
			defer restore()

			pendingLoadReasons.Lock()
			oldReasons := pendingLoadReasons.m
			pendingLoadReasons.m = make(map[string]pendingLoadReason)
			pendingLoadReasons.Unlock()
			loadGenerations.Lock()
			oldGenerations := loadGenerations.m
			loadGenerations.m = make(map[string]loadGenerationState)
			loadGenerations.Unlock()
			t.Cleanup(func() {
				pendingLoadReasons.Lock()
				pendingLoadReasons.m = oldReasons
				pendingLoadReasons.Unlock()
				loadGenerations.Lock()
				loadGenerations.m = oldGenerations
				loadGenerations.Unlock()
			})

			swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
				return executor.Result{}, moerr.NewQueryInterrupted(context.Background())
			})
			invalidateLoadGeneration(cfg, reason)

			s := NewFulltext2Search(cfg)
			require.Error(t, s.Load(sp))
			s.FinishLoadObservation()
			require.Error(t, s.Load(sp))
			s.FinishLoadObservation()

			require.Len(t, events, 2)
			require.Equal(t, reason, events[0].MissReason)
			require.Equal(t, reason, events[1].MissReason)
			require.True(t, events[0].LoadCancel)
			require.True(t, events[1].LoadCancel)
			s.Destroy()
		})
	}
}

func TestFulltext2SearchInvalidationEvictionRecordsOneReason(t *testing.T) {
	cleanupObserver := setLoadObserver(func(LoadEvent) {})
	defer cleanupObserver()

	pendingLoadReasons.Lock()
	previousReasons := pendingLoadReasons.m
	pendingLoadReasons.m = make(map[string]pendingLoadReason)
	pendingLoadReasons.Unlock()
	t.Cleanup(func() {
		pendingLoadReasons.Lock()
		pendingLoadReasons.m = previousReasons
		pendingLoadReasons.Unlock()
	})

	previousCache := veccache.Cache
	veccache.Cache = veccache.NewVectorIndexCache()
	t.Cleanup(func() { veccache.Cache = previousCache })

	cfg := testStorageCfg()
	old := NewFulltext2Search(cfg)
	old.idx = NewIndex(nil, nil)
	old.loaded = true
	oldEntry := &veccache.VectorIndexSearch{Algo: old}
	oldEntry.Cond = sync.NewCond(oldEntry.Mutex.RLocker())
	veccache.Cache.IndexMap.Store(cfg.IndexTable, oldEntry)

	// Production mutation order: record the reason before evicting the local
	// entry. Hold the old entry lock so Remove has deleted the old map entry but
	// is blocked before destruction; a replacement can therefore start while
	// the old eviction is still in flight.
	oldEntry.Mutex.Lock()
	old.OnCacheInvalidated(string(LoadMissCDCFlush))
	removed := make(chan struct{})
	go func() {
		veccache.Cache.Remove(cfg.IndexTable)
		close(removed)
	}()
	require.Eventually(t, func() bool {
		_, loaded := veccache.Cache.IndexMap.Load(cfg.IndexTable)
		return !loaded
	}, time.Second, time.Millisecond)
	replacement := NewFulltext2Search(cfg)
	replacement.idx = NewIndex(nil, nil)
	replacement.loaded = true
	replacementEntry := &veccache.VectorIndexSearch{Algo: replacement}
	replacementEntry.Cond = sync.NewCond(replacementEntry.Mutex.RLocker())
	veccache.Cache.IndexMap.Store(cfg.IndexTable, replacementEntry)

	reason, generation := peekLoadReason(cfg.cacheIdentity().Key())
	require.Equal(t, LoadMissCDCFlush, reason)
	require.NotZero(t, generation)
	consumeLoadReason(cfg.cacheIdentity().Key(), generation)
	oldEntry.Mutex.Unlock()
	select {
	case <-removed:
	case <-time.After(time.Second):
		t.Fatal("old cache eviction did not finish")
	}
	reason, generation = peekLoadReason(cfg.cacheIdentity().Key())
	require.Empty(t, reason)
	require.Zero(t, generation)
	veccache.Cache.Remove(cfg.IndexTable)
}

func TestHouseKeepingPublishesGenerationBeforeReplacementLoad(t *testing.T) {
	cleanupObserver := setLoadObserver(func(LoadEvent) {})
	defer cleanupObserver()

	previousCache := veccache.Cache
	veccache.Cache = veccache.NewVectorIndexCache()
	defer func() { veccache.Cache = previousCache }()

	loadGenerations.Lock()
	previousGenerations := loadGenerations.m
	loadGenerations.m = make(map[string]loadGenerationState)
	loadGenerations.Unlock()
	defer func() {
		loadGenerations.Lock()
		loadGenerations.m = previousGenerations
		loadGenerations.Unlock()
	}()

	cfg := testStorageCfg()
	old := NewFulltext2Search(cfg)
	old.idx = NewIndex(nil, nil)
	old.loaded = true
	oldEntry := &veccache.VectorIndexSearch{Algo: old}
	oldEntry.Cond = sync.NewCond(oldEntry.Mutex.RLocker())
	oldEntry.ExpireAt.Store(time.Now().Add(-time.Second).UnixMicro())
	veccache.Cache.IndexMap.Store(cfg.IndexTable, oldEntry)

	oldEntry.Mutex.Lock()
	done := make(chan struct{})
	go func() {
		veccache.Cache.HouseKeeping()
		close(done)
	}()
	require.Eventually(t, func() bool {
		_, loaded := veccache.Cache.IndexMap.Load(cfg.IndexTable)
		return !loaded
	}, time.Second, time.Millisecond)
	reason, observedGeneration := peekLoadReason(cfg.cacheIdentity().Key())
	require.Equal(t, LoadMissTTLExpired, reason)
	require.NotZero(t, observedGeneration)
	consumeLoadReason(cfg.cacheIdentity().Key(), observedGeneration)

	replacement := NewFulltext2Search(cfg)
	replacement.idx = NewIndex(nil, nil)
	replacement.loaded = true
	replacementEntry := &veccache.VectorIndexSearch{Algo: replacement}
	replacementEntry.Cond = sync.NewCond(replacementEntry.Mutex.RLocker())
	veccache.Cache.IndexMap.Store(cfg.IndexTable, replacementEntry)

	loadGen := beginLoadGeneration(cfg.cacheIdentity().Key())
	require.True(t, loadGenerationCurrent(loadGen))
	endLoadGeneration(loadGen)

	oldEntry.Mutex.Unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("housekeeping eviction did not finish")
	}
	require.True(t, loadGenerationCurrent(loadGen))
	veccache.Cache.Remove(cfg.IndexTable)
}

func TestFulltext2SupersededLoadIsRetryable(t *testing.T) {
	require.True(t, veccache.IsRetryableLoadError(errLoadGenerationSuperseded))
	require.Contains(t, errLoadGenerationSuperseded.Error(), "fulltext2 load superseded by a newer generation")
}

func TestFulltext2LoadCannotPublishBelowRequiredGeneration(t *testing.T) {
	previousCache := veccache.Cache
	previousFences := localFences
	veccache.Cache = veccache.NewVectorIndexCache()
	localFences = newFenceRegistry(8)
	t.Cleanup(func() {
		veccache.Cache = previousCache
		localFences = previousFences
	})

	cfg := testStorageCfg()
	const accountID = uint32(17)
	id := cfg.CacheIdentity(accountID)
	proc, mp := mockSqlProc(t)
	var generationReads atomic.Int32
	var fenceInstalled atomic.Bool
	swapRunSql(t, func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		if strings.HasPrefix(sql, "SELECT (SELECT") {
			read := generationReads.Add(1)
			if read == 1 {
				return executor.Result{Mp: mp, Batches: []*batch.Batch{generationBatch(mp, 1, 1)}}, nil
			}
			return executor.Result{Mp: mp, Batches: []*batch.Batch{generationBatch(mp, 1, 2)}}, nil
		}
		if generationReads.Load() == 1 && fenceInstalled.CompareAndSwap(false, true) {
			claim, _, overflow := localFences.install(id, Generation{BaseTimestamp: 1, TailChunk: 2})
			require.True(t, claim)
			require.False(t, overflow)
		}
		return executor.Result{Mp: mp}, nil
	})

	loader := NewFulltext2SearchForAccount(cfg, accountID)
	_, _, err := veccache.Cache.Search(proc, id.Key(), loader,
		Fulltext2Query{Pattern: []byte("x")}, vectorindex.RuntimeConfig{Limit: 1})
	require.NoError(t, err)
	require.True(t, fenceInstalled.Load())
	// One read for the superseded attempt, then the successful attempt's
	// pre-load snapshot plus its fresh global-admission read.
	require.Equal(t, int32(3), generationReads.Load())
	value, ok := veccache.Cache.IndexMap.Load(id.Key())
	require.True(t, ok)
	loaded := value.(*veccache.VectorIndexSearch).Algo.(*Fulltext2Search)
	require.Equal(t, int64(2), loaded.loadedTail)
}

func TestFreshnessUncertaintyKeepsOldSnapshotTransient(t *testing.T) {
	previousCache := veccache.Cache
	previousFences := localFences
	veccache.Cache = veccache.NewVectorIndexCache()
	localFences = newFenceRegistry(1)
	t.Cleanup(func() {
		veccache.Cache = previousCache
		localFences = previousFences
	})

	cfg := testStorageCfg()
	const accountID = uint32(19)
	id := cfg.CacheIdentity(accountID)
	proc, mp := mockSqlProc(t)
	swapRunSql(t, func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		if strings.HasPrefix(sql, "SELECT (SELECT") {
			return executor.Result{Mp: mp, Batches: []*batch.Batch{generationBatch(mp, 1, -1)}}, nil
		}
		return executor.Result{Mp: mp}, nil
	})

	query := Fulltext2Query{Pattern: []byte("x")}
	_, _, err := veccache.Cache.Search(proc, id.Key(), NewFulltext2SearchForAccount(cfg, accountID), query, vectorindex.RuntimeConfig{Limit: 1})
	require.NoError(t, err)
	warm, ok := veccache.Cache.IndexMap.Load(id.Key())
	require.True(t, ok)
	warmSearch := warm.(*veccache.VectorIndexSearch).Algo.(*Fulltext2Search)

	warmSearch.OnFreshnessUncertain()
	require.True(t, requiresTransientLoad(id))
	_, _, err = veccache.Cache.Search(proc, id.Key(), NewFulltext2SearchForAccount(cfg, accountID), query, vectorindex.RuntimeConfig{Limit: 1})
	require.NoError(t, err)
	after, ok := veccache.Cache.IndexMap.Load(id.Key())
	require.True(t, ok)
	require.Same(t, warm, after, "the old transaction snapshot must not replace the global entry")

	warmSearch.OnFreshnessConfirmed()
	require.False(t, requiresTransientLoad(id))
	_, _, err = veccache.Cache.Search(proc, id.Key(), NewFulltext2SearchForAccount(cfg, accountID), query, vectorindex.RuntimeConfig{Limit: 1})
	require.NoError(t, err)
	after, ok = veccache.Cache.IndexMap.Load(id.Key())
	require.True(t, ok)
	require.Same(t, warm, after)
}

func TestExactFenceSurvivesCapacityGrowthAndRejectsOlderSnapshot(t *testing.T) {
	previousCache := veccache.Cache
	previousFences := localFences
	veccache.Cache = veccache.NewVectorIndexCache()
	localFences = newFenceRegistry(1)
	t.Cleanup(func() {
		veccache.Cache = previousCache
		localFences = previousFences
	})

	cfg := testStorageCfg()
	const accountID = uint32(23)
	id := cfg.CacheIdentity(accountID)
	other := CacheIdentity{AccountID: accountID, Database: cfg.DbName, StorageTable: "other", MetadataTable: "other_meta"}
	required := Generation{BaseTimestamp: 2, TailChunk: 1}
	claim, _, overflow := localFences.install(id, required)
	require.True(t, claim)
	require.False(t, overflow)
	require.True(t, localFences.finishClaim(id, required))

	proc, mp := mockSqlProc(t)
	var growOnce sync.Once
	swapRunSql(t, func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		growOnce.Do(func() {
			claim, _, overflow = localFences.install(other, required)
			require.True(t, claim)
			require.False(t, overflow)
		})
		if strings.HasPrefix(sql, "SELECT (SELECT") {
			return executor.Result{Mp: mp, Batches: []*batch.Batch{generationBatch(mp, 1, -1)}}, nil
		}
		return executor.Result{Mp: mp}, nil
	})

	loader := NewFulltext2SearchForAccount(cfg, accountID)
	_, _, err := veccache.Cache.Search(proc, id.Key(), loader,
		Fulltext2Query{Pattern: []byte("x")}, vectorindex.RuntimeConfig{Limit: 1})
	require.ErrorContains(t, err, "coherence retry exhausted")
	require.Equal(t, required, localFences.required(id))
	require.False(t, requiresTransientLoad(id))
	_, published := veccache.Cache.IndexMap.Load(id.Key())
	require.False(t, published, "an old transaction snapshot must never republish a fenced identity")
}

func TestPrunedFenceStillRejectsOlderSnapshotAtGlobalAdmission(t *testing.T) {
	previousCache := veccache.Cache
	previousFences := localFences
	veccache.Cache = veccache.NewVectorIndexCache()
	localFences = newFenceRegistry(1)
	t.Cleanup(func() {
		veccache.Cache = previousCache
		localFences = previousFences
	})

	cfg := testStorageCfg()
	const accountID = uint32(29)
	id := cfg.CacheIdentity(accountID)
	oldGeneration := Generation{BaseTimestamp: 1, TailChunk: -1}
	currentGeneration := Generation{BaseTimestamp: 2, TailChunk: -1}
	claim, _, overflow := localFences.install(id, currentGeneration)
	require.True(t, claim)
	require.False(t, overflow)
	require.True(t, localFences.finishClaim(id, currentGeneration))
	localFences.pruneInactive(func(string) bool { return false })
	require.Zero(t, localFences.required(id))

	proc, mp := mockSqlProc(t)
	swapRunSql(t, func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		if strings.HasPrefix(sql, "SELECT (SELECT") {
			return executor.Result{Mp: mp, Batches: []*batch.Batch{
				generationBatch(mp, oldGeneration.BaseTimestamp, oldGeneration.TailChunk),
			}}, nil
		}
		return executor.Result{Mp: mp}, nil
	})
	previousQueryCurrent := queryCurrentGeneration
	queryCurrentGeneration = func(context.Context, string, uint32, TableConfig) (int64, int64, error) {
		return currentGeneration.BaseTimestamp, currentGeneration.TailChunk, nil
	}
	t.Cleanup(func() { queryCurrentGeneration = previousQueryCurrent })

	_, _, err := veccache.Cache.Search(proc, id.Key(), NewFulltext2SearchForAccount(cfg, accountID),
		Fulltext2Query{Pattern: []byte("x")}, vectorindex.RuntimeConfig{Limit: 1})
	require.ErrorContains(t, err, "coherence retry exhausted")
	require.Equal(t, currentGeneration, localFences.required(id))
	_, published := veccache.Cache.IndexMap.Load(id.Key())
	require.False(t, published, "durable admission must reject an old snapshot after registry pruning")
}

func TestCapPlusOneFencesReturnToWarmSteadyState(t *testing.T) {
	previousCache := veccache.Cache
	previousFences := localFences
	veccache.Cache = veccache.NewVectorIndexCache()
	localFences = newFenceRegistry(2)
	t.Cleanup(func() {
		veccache.Cache = previousCache
		localFences = previousFences
	})

	const accountID = uint32(31)
	configs := []TableConfig{
		{DbName: "db", IndexTable: "s1", MetadataTable: "m1", AccountID: accountID},
		{DbName: "db", IndexTable: "s2", MetadataTable: "m2", AccountID: accountID},
		{DbName: "db", IndexTable: "s3", MetadataTable: "m3", AccountID: accountID},
	}
	generation := Generation{BaseTimestamp: 1, TailChunk: -1}
	for _, cfg := range configs {
		_, claimed, overflow := InstallGenerationFence(cfg.CacheIdentity(accountID), generation)
		require.True(t, claimed)
		require.False(t, overflow)
	}

	proc, mp := mockSqlProc(t)
	var generationReads atomic.Int32
	swapRunSql(t, func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		if strings.HasPrefix(sql, "SELECT (SELECT") {
			generationReads.Add(1)
			return executor.Result{Mp: mp, Batches: []*batch.Batch{
				generationBatch(mp, generation.BaseTimestamp, generation.TailChunk),
			}}, nil
		}
		return executor.Result{Mp: mp}, nil
	})
	query := Fulltext2Query{Pattern: []byte("x")}
	for _, cfg := range configs {
		id := cfg.CacheIdentity(accountID)
		_, _, err := veccache.Cache.Search(proc, id.Key(), NewFulltext2SearchForAccount(cfg, accountID),
			query, vectorindex.RuntimeConfig{Limit: 1})
		require.NoError(t, err)
	}
	coldReads := generationReads.Load()
	require.Equal(t, int32(2*len(configs)), coldReads)

	for _, cfg := range configs {
		id := cfg.CacheIdentity(accountID)
		_, _, err := veccache.Cache.Search(proc, id.Key(), NewFulltext2SearchForAccount(cfg, accountID),
			query, vectorindex.RuntimeConfig{Limit: 1})
		require.NoError(t, err)
	}
	require.Equal(t, coldReads, generationReads.Load(), "cap+1 identities must return to warm-hit cost")
}

func TestGlobalAdmissionErrorFallsBackToTransientWithoutPublishing(t *testing.T) {
	previousCache := veccache.Cache
	previousFences := localFences
	veccache.Cache = veccache.NewVectorIndexCache()
	localFences = newFenceRegistry(1)
	t.Cleanup(func() {
		veccache.Cache = previousCache
		localFences = previousFences
	})

	cfg := testStorageCfg()
	const accountID = uint32(37)
	id := cfg.CacheIdentity(accountID)
	proc, mp := mockSqlProc(t)
	swapRunSql(t, func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		if strings.HasPrefix(sql, "SELECT (SELECT") {
			return executor.Result{Mp: mp, Batches: []*batch.Batch{generationBatch(mp, 1, -1)}}, nil
		}
		return executor.Result{Mp: mp}, nil
	})
	previousQueryCurrent := queryCurrentGeneration
	queryCurrentGeneration = func(context.Context, string, uint32, TableConfig) (int64, int64, error) {
		return 0, 0, moerr.NewInternalErrorNoCtx("injected admission read failure")
	}
	t.Cleanup(func() { queryCurrentGeneration = previousQueryCurrent })

	_, _, err := veccache.Cache.Search(proc, id.Key(), NewFulltext2SearchForAccount(cfg, accountID),
		Fulltext2Query{Pattern: []byte("x")}, vectorindex.RuntimeConfig{Limit: 1})
	require.NoError(t, err)
	require.True(t, requiresTransientLoad(id))
	_, published := veccache.Cache.IndexMap.Load(id.Key())
	require.False(t, published, "an unknown durable generation must remain one-shot")
}

func TestVectorIndexCacheRetriesSupersededFulltext2Load(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(*sqlexec.SqlProcess, *Fulltext2Search, TableConfig) error
	}{
		{
			name: "Search",
			call: func(proc *sqlexec.SqlProcess, loader *Fulltext2Search, cfg TableConfig) error {
				_, _, err := veccache.Cache.Search(proc, cfg.IndexTable, loader,
					Fulltext2Query{Pattern: []byte("x")}, vectorindex.RuntimeConfig{Limit: 1})
				return err
			},
		},
		{
			name: "SearchInto",
			call: func(proc *sqlexec.SqlProcess, loader *Fulltext2Search, cfg TableConfig) error {
				return veccache.Cache.SearchInto(proc, cfg.IndexTable, loader,
					Fulltext2Query{Pattern: []byte("x")}, vectorindex.RuntimeConfig{Limit: 1}, &vectorindex.SearchOutput{})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			previousCache := veccache.Cache
			veccache.Cache = veccache.NewVectorIndexCache()
			t.Cleanup(func() { veccache.Cache = previousCache })

			pendingLoadReasons.Lock()
			previousReasons := pendingLoadReasons.m
			pendingLoadReasons.m = make(map[string]pendingLoadReason)
			pendingLoadReasons.Unlock()
			t.Cleanup(func() {
				pendingLoadReasons.Lock()
				pendingLoadReasons.m = previousReasons
				pendingLoadReasons.Unlock()
			})
			loadGenerations.Lock()
			previousGenerations := loadGenerations.m
			loadGenerations.m = make(map[string]loadGenerationState)
			loadGenerations.Unlock()
			t.Cleanup(func() {
				loadGenerations.Lock()
				loadGenerations.m = previousGenerations
				loadGenerations.Unlock()
			})

			cfg := testStorageCfg()
			proc, mp := mockSqlProc(t)
			var invalidated atomic.Bool
			swapRunSql(t, func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
				if strings.Contains(sql, "index_id") && strings.Contains(sql, "__meta") {
					if invalidated.CompareAndSwap(false, true) {
						// This is the production publication-before-remove window: the
						// loading cache entry still owns the key while the generation bump
						// becomes visible to its Fulltext2Search.Load.
						NewFulltext2Search(cfg).OnCacheInvalidated(string(LoadMissCDCFlush))
					}
					return executor.Result{Mp: mp}, nil
				}
				if strings.Contains(sql, "chunk_id, data") {
					return executor.Result{Mp: mp}, nil
				}
				return executor.Result{Mp: mp, Batches: []*batch.Batch{int64Batch(mp, 0)}}, nil
			})

			loader := NewFulltext2Search(cfg)
			require.NoError(t, tc.call(proc, loader, cfg))
			require.True(t, invalidated.Load())
			veccache.Cache.Remove(cfg.IndexTable)
		})
	}
}

// TestGenerationSQL pins the single-snapshot cache-freshness generation query: MAX(timestamp) over the
// metadata table (REBUILD/MERGE signal) and MAX(chunk_id) over the tag=1 CdcTail (CDC-append
// signal), scoped to (CdcTailId, tag=1) so a base sub-index cannot mask a fresh append.
func TestGenerationSQL(t *testing.T) {
	cfg := TableConfig{DbName: "db", IndexTable: "__store", MetadataTable: "__meta"}
	sql := GenerationSQL(cfg)
	require.Contains(t, sql, "MAX(timestamp)")
	require.Contains(t, sql, "`db`.`__meta`")
	require.Contains(t, sql, "MAX(chunk_id)")
	require.Contains(t, sql, "`db`.`__store`")
	require.Contains(t, sql, "tag = 1")             // tag=Tag_CdcEvents
	require.Contains(t, sql, vectorindex.CdcTailId) // scoped to the single CDC tail
}

// An entry with no captured generation must enter the same uncertainty path as
// a failed durable query. It stays available only through transient loads until
// a current-generation probe succeeds.
func TestIsStaleUncheckableBecomesUncertain(t *testing.T) {
	previousFences := localFences
	localFences = newFenceRegistry(1)
	t.Cleanup(func() { localFences = previousFences })
	s := loadedSearch(t)
	s.identity = CacheIdentity{AccountID: 1, Database: "db", StorageTable: "s", MetadataTable: "m"}
	s.identitySet = true
	require.False(t, s.genValid) // loaded, but generation never captured
	stale, err := s.IsStale()
	require.Error(t, err)
	require.False(t, stale)
	s.OnFreshnessUncertain()
	require.True(t, requiresTransientLoad(s.identity))
}

func TestFulltext2SearchEmptyIndex(t *testing.T) {
	proc := newSearchProc(t)
	s := NewFulltext2Search(TableConfig{IndexTable: "__store", Parser: ParserDefault})
	s.idx = NewIndex(nil, nil) // loaded but doc-less
	s.loaded = true

	keys, dists, err := s.Search(proc, Fulltext2Query{Pattern: []byte("fox")}, vectorindex.RuntimeConfig{})
	require.NoError(t, err)
	require.Empty(t, keys)
	require.Empty(t, dists)
}

func TestFulltext2SearchInvalidPayload(t *testing.T) {
	proc := newSearchProc(t)
	s := loadedSearch(t)
	defer s.Destroy()

	_, _, err := s.Search(proc, "not a query", vectorindex.RuntimeConfig{})
	require.ErrorContains(t, err, "invalid query payload")
}

func TestFulltext2SearchTopK(t *testing.T) {
	proc := newSearchProc(t)
	s := loadedSearch(t)
	defer s.Destroy()

	// single-term NL query with a pushed LIMIT.
	keys, dists, err := s.Search(proc, Fulltext2Query{Pattern: []byte("fox"), Algo: BM25},
		vectorindex.RuntimeConfig{Limit: 10})
	require.NoError(t, err)
	ks, ok := keys.([]any)
	require.True(t, ok)
	require.Len(t, dists, len(ks))
	require.NotEmpty(t, ks) // "fox" hits docs 0 and 2

	// k <= 0 (no pushed LIMIT) falls back to NumDocs.
	keys, _, err = s.Search(proc, Fulltext2Query{Pattern: []byte("fox"), Algo: BM25},
		vectorindex.RuntimeConfig{Limit: 0})
	require.NoError(t, err)
	require.NotEmpty(t, keys.([]any))

	// an absurd LIMIT past MaxInt32 is clamped, not wrapped negative.
	keys, _, err = s.Search(proc, Fulltext2Query{Pattern: []byte("fox"), Algo: BM25},
		vectorindex.RuntimeConfig{Limit: uint(math.MaxInt32) + 100})
	require.NoError(t, err)
	require.NotEmpty(t, keys.([]any))

	// bag-of-words (IN BM25 MODE) path.
	keys, _, err = s.Search(proc, Fulltext2Query{Pattern: []byte("quick fox"), BagOfWords: true, Algo: BM25},
		vectorindex.RuntimeConfig{Limit: 10})
	require.NoError(t, err)
	require.NotEmpty(t, keys.([]any))
}

func TestFulltext2SearchStreamingEmit(t *testing.T) {
	proc := newSearchProc(t)
	s := loadedSearch(t)
	defer s.Destroy()

	// Emit set + no pushed LIMIT → streaming: results handed off via Emit, empty return.
	for _, bagOfWords := range []bool{false, true} {
		emitted := 0
		emit := func(o *vectorindex.SearchOutput) error {
			emitted += o.Keys.N
			PutColumnBuffer(o.Keys) // recycle like the real consumer
			return nil
		}
		keys, dists, err := s.Search(proc,
			Fulltext2Query{Pattern: []byte("fox"), BagOfWords: bagOfWords, Algo: BM25},
			vectorindex.RuntimeConfig{Emit: emit})
		require.NoError(t, err)
		require.Empty(t, keys)
		require.Empty(t, dists)
		require.Positive(t, emitted, "bagOfWords=%v should emit docs", bagOfWords)
	}
}

func TestFulltext2SearchDestroy(t *testing.T) {
	s := loadedSearch(t)

	// The cached config is immutable for the entry's lifetime (no UpdateConfig hook —
	// a config change evicts the entry), so Search is pure-read; here we just pin that
	// the constructed cfg is what Load queries with and that Destroy tears down cleanly.
	require.Equal(t, ParserDefault, s.cfg.Parser)

	// Destroy frees and clears the loaded index.
	s.Destroy()
	require.Nil(t, s.idx)
	require.False(t, s.loaded)
}

// TestLoadGenerationHappy stubs the package runSql to cover the fulltext2 generation reader
// (GenerationSQL + resultGeneration + the one-read LoadGeneration body).
func TestLoadGenerationHappy(t *testing.T) {
	mp := mpool.MustNewZero()
	old := runSql
	defer func() { runSql = old }()
	runSql = func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		require.Equal(t, GenerationSQL(TableConfig{DbName: "db", MetadataTable: "m", IndexTable: "s"}), sql)
		bat := batch.NewWithSize(2)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed[int64](bat.Vecs[0], 11, false, mp))
		require.NoError(t, vector.AppendFixed[int64](bat.Vecs[1], 22, false, mp))
		bat.SetRowCount(1)
		return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}, nil
	}
	ts, tail, err := LoadGeneration(nil, TableConfig{DbName: "db", MetadataTable: "m", IndexTable: "s"})
	require.NoError(t, err)
	require.Equal(t, int64(11), ts)   // MAX(timestamp)
	require.Equal(t, int64(22), tail) // MAX(chunk_id) tag=1
}

// TestLoadGenerationRecover: if the generation read panics (e.g. a background housekeeping call
// hits a torn-down executor), LoadGeneration must recover it into an error — never let it crash
// the caller. The caller then leaves genValid=false, and IsStale enters the
// uncertainty fence (see TestIsStaleUncheckableBecomesUncertain).
func TestLoadGenerationRecover(t *testing.T) {
	old := runSql
	defer func() { runSql = old }()
	runSql = func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		panic("simulated executor teardown")
	}
	_, _, err := LoadGeneration(nil, TableConfig{DbName: "db", MetadataTable: "m", IndexTable: "s"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "LoadGeneration recovered")
}

func TestLoadGenerationRejectsMissingResultRow(t *testing.T) {
	old := runSql
	defer func() { runSql = old }()
	runSql = func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{}, nil
	}
	_, _, err := LoadGeneration(nil, TableConfig{DbName: "db", MetadataTable: "m", IndexTable: "s"})
	require.ErrorContains(t, err, "generation query returned no row")
}

// With a captured generation but an unresolvable CN service, the background
// query error must enter uncertainty rather than masquerade as a confirmed
// stale generation.
func TestFulltext2IsStaleQueryError(t *testing.T) {
	s := NewFulltext2Search(TableConfig{DbName: "db", MetadataTable: "m", IndexTable: "s"})
	s.genValid = true
	s.cnUUID = "no-such-cn-uuid"
	stale, err := s.IsStale()
	require.Error(t, err)
	require.False(t, stale)
}

func TestGenerationDropErrorClassification(t *testing.T) {
	require.True(t, isDefinitiveGenerationDrop(moerr.NewNoSuchTableNoCtx("db", "t")))
	require.True(t, isDefinitiveGenerationDrop(moerr.NewBadDBNoCtx("db")))
	require.False(t, isDefinitiveGenerationDrop(moerr.NewInternalErrorNoCtx("temporary failure")))
}

// TestFulltext2SearchInto pins the box-free LIMIT path: SearchInto fills the caller-owned
// SearchOutput (pk column, float32 scores, one nullable ColumnBuffer per FULL INCLUDE column)
// — box-free and reused across calls. incIdx has 5 docs all matching "x", includes
// [status varchar, prio int64] with a NULL status (pk4).
func TestFulltext2SearchInto(t *testing.T) {
	proc := newSearchProc(t)
	idx := incIdx(t)
	s := &Fulltext2Search{idx: idx, loaded: true, cfg: TableConfig{IndexTable: "__store", Parser: ParserDefault}}
	mp := mpool.MustNewZero()

	out := &vectorindex.SearchOutput{}
	rt := vectorindex.RuntimeConfig{Limit: 10, RequestedIncludeColumns: []string{"status", "prio"}}
	require.NoError(t, s.SearchInto(proc, Fulltext2Query{Pattern: []byte("x"), Algo: BM25}, rt, out))

	require.Equal(t, 5, out.Keys.N) // all 5 docs contain "x"
	require.Len(t, out.Dists, 5)
	require.Len(t, out.Include, 2) // status, prio (FULL include order)

	// Decode the box-free buffers into vectors and zip by row (Keys[i] <-> Include[*][i]) into
	// a pk -> (status, prio) map (result order is score-desc; equal scores are unspecified).
	keyVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vectorindex.AppendColumnBuffer(out.Keys, keyVec, mp))
	statusVec := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vectorindex.AppendColumnBuffer(out.Include[0], statusVec, mp))
	prioVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vectorindex.AppendColumnBuffer(out.Include[1], prioVec, mp))

	pks := vector.MustFixedColWithTypeCheck[int64](keyVec)
	prios := vector.MustFixedColWithTypeCheck[int64](prioVec)
	require.Len(t, pks, 5)
	type sv struct {
		status any
		prio   int64
	}
	got := map[int64]sv{}
	for i := range pks {
		var st any
		if !statusVec.IsNull(uint64(i)) {
			st = statusVec.GetStringAt(i)
		}
		got[pks[i]] = sv{st, prios[i]}
	}
	require.Equal(t, sv{"active", int64(10)}, got[1])
	require.Equal(t, sv{"inactive", int64(20)}, got[2])
	require.Equal(t, sv{"active", int64(30)}, got[3])
	require.Equal(t, sv{nil, int64(40)}, got[4]) // NULL status preserved
	require.Equal(t, sv{"archived", int64(5)}, got[5])

	// Reuse: a second SearchInto Resets out and refills (no stale rows accumulated).
	require.NoError(t, s.SearchInto(proc, Fulltext2Query{Pattern: []byte("x"), Algo: BM25}, rt, out))
	require.Equal(t, 5, out.Keys.N)
	require.Len(t, out.Dists, 5)

	// No requested INCLUDE columns → out.Include emptied.
	require.NoError(t, s.SearchInto(proc, Fulltext2Query{Pattern: []byte("x"), Algo: BM25},
		vectorindex.RuntimeConfig{Limit: 10}, out))
	require.Equal(t, 5, out.Keys.N)
	require.Empty(t, out.Include)

	// nil out → error, not a nil-deref.
	require.Error(t, s.SearchInto(proc, Fulltext2Query{Pattern: []byte("x")}, rt, nil))
}

// TestFulltext2SearchIntoNotLoaded / empty: the two prepare() early-outs on the SearchInto path.
func TestFulltext2SearchIntoNotLoadedAndEmpty(t *testing.T) {
	proc := newSearchProc(t)
	out := &vectorindex.SearchOutput{}

	// not loaded → error.
	s := NewFulltext2Search(TableConfig{IndexTable: "__store"})
	require.ErrorContains(t, s.SearchInto(proc, Fulltext2Query{Pattern: []byte("x")}, vectorindex.RuntimeConfig{Limit: 10}, out), "not loaded")

	// loaded but empty index → no rows, no error, out emptied.
	s2 := &Fulltext2Search{idx: NewIndex(nil, nil), loaded: true, cfg: TableConfig{IndexTable: "__store"}}
	require.NoError(t, s2.SearchInto(proc, Fulltext2Query{Pattern: []byte("x")}, vectorindex.RuntimeConfig{Limit: 10}, out))
}
