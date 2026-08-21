// Copyright 2022 Matrix Origin
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
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// stalenessCheckEveryNTicks runs the IsStale freshness sweep every Nth HouseKeeping tick.
// The ticker is VectorIndexCacheTTL/2 (2.5m), so N=4 ≈ a 10-minute cross-CN freshness
// cadence — the bound on how long a remote CN can serve a stale index before it ages out.
const stalenessCheckEveryNTicks = 4

/*
   VectorIndexCache is the generalized cache structure for various algorithm types that share the VectorIndexSearchIf interface.
   Implement the VectorIndexSearchIf such as HnswSearch to able to use VectorIndexCache.

   VectorIndexCache allows to search the vector index concurrently.  Usually vector index model is huge in size and it is not possible
   to load the whole model to memory for each user.  We need a cache that can run concurrently and able to refresh automatically.

   1. When the index is loaded into memory, index can be shared with RWMutex.Rlock() (Read-Only)
   2. With RWMutex.Lock (Write),  index can be loaded from database without race.
   3. HouseKeeping. Index will have time-to-live interval (see VectorIndexCacheTTL).
      3.1 When the index is expired (ExpireAt > 0 && ExpiredAt < Now), index will be deleted from the cache. Ticker go routine will manage the house keeping.
      3.2 ExpiredAt == 0 means index is loading from database so cannot be deleted from housekeeping
      3.3 Every time index is visited by Search/LoadFromDatabase, ExpireAt will be extended to time.Now() + VectorIndexCacheTTL.
*/

const (
	STATUS_NOT_INIT  = 0
	STATUS_LOADED    = 1
	STATUS_DESTROYED = 2
	STATUS_ERROR     = 3
)

var (
	VectorIndexCacheTTL time.Duration     = 5 * time.Minute
	Cache               *VectorIndexCache = NewVectorIndexCache()
)

// Various vector index algorithm wants to share with VectorIndexCache need to implement VectorIndexSearchIf interface (see HnswSearch)
type VectorIndexSearchIf interface {
	Search(proc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error)
	// SearchFloat32 writes results into caller-provided slices to avoid heap allocation.
	// outKeys and outDists must be pre-allocated to nQueries*rt.Limit elements.
	// GPU implementations write float32 distances directly; CPU implementations convert on write.
	SearchFloat32(proc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig, outKeys []int64, outDists []float32) error
	// SearchInto is the box-free, alloc-free-after-warmup twin of Search: instead of
	// returning keys as []any (a heap box per key), it fills a CALLER-OWNED SearchResult —
	// pk column, scores, and covered-INCLUDE columns, all as reusable ColumnBuffers/slices
	// the caller pools and Resets across queries, so a warm query allocates nothing for its
	// results. It is the arbitrary-pk generalization of SearchFloat32 (whose outKeys []int64
	// cannot hold varchar/uuid/decimal pks). The callee Resets out before filling; on return
	// out.Keys.N is the result count, len(out.Dists) == out.Keys.N, and out.Include (when
	// rt.RequestedIncludeColumns is set) holds one buffer per FULL index INCLUDE column.
	// Implemented by fulltext2; the vector algos stub it "not supported" until each migrates
	// (mirrors how fulltext2 stubs SearchFloat32).
	SearchInto(proc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig, out *vectorindex.SearchOutput) error
	Load(*sqlexec.SqlProcess) error
	Destroy()
}

// cacheInvalidationAware is an optional lifecycle hook. It is intentionally
// not part of VectorIndexSearchIf so existing vector algorithms keep their
// contract unchanged; FULLTEXT2 uses it to classify the next load miss.
type cacheInvalidationAware interface {
	OnCacheInvalidated(reason string)
}

type loadWaiterAware interface {
	SetLoadWaiters(int64)
}

type loadObservationFinisher interface {
	FinishLoadObservation()
}

// StaleChecker is an OPTIONAL capability an algo's search impl may implement (currently
// fulltext2). It reports whether the loaded index has fallen behind the persisted index —
// e.g. a CDC append / REBUILD applied on ANOTHER CN, which the local process-scoped Remove
// never sees. HouseKeeping calls it periodically and force-expires stale entries so the next
// search reloads; this is how cross-CN cache coherence is maintained by PULL (each CN checks
// its own entries) with no invalidation broadcast. Impls MUST run their own short background
// txn (the check fires on the housekeeping goroutine, not the search path) and MUST return an
// error rather than "stale" on a transient failure, so a meta-read blip can't trigger a
// reload storm. An impl that cannot determine freshness returns (false, nil).
//
// DESIGN DECISION — EVENTUAL consistency, not immediate (won't-fix, by design): Search serves a
// warmed entry directly and does NOT validate freshness on the query path (that would put a meta
// read on every search — the perf floor this cache exists to avoid). Coherence is the periodic
// PULL sweep only: it runs every stalenessCheckEveryNTicks ticks of the TTL/2 ticker and evicts a
// stale entry on the NEXT housekeeping pass, so after a writer commits on another CN a warm remote
// entry can keep answering the pre-CDC/MERGE/REBUILD snapshot for up to ~10–12.5 minutes before it
// is reloaded. This is intentional: the contract is that a stale entry is EVENTUALLY removed, not
// that reads are correct the instant the writer commits. Do NOT "fix" it by validating on the
// search path or by broadcasting invalidations; if a caller ever needs read-your-writes across CNs,
// that is a separate, opt-in requirement (or disable this cache for that index in multi-CN).
type StaleChecker interface {
	IsStale() (bool, error)
}

// base VectorIndex Search structure for VectorIndexSearchIf (see HnswSearch)
type VectorIndexSearch struct {
	Mutex       sync.RWMutex
	ExpireAt    atomic.Int64
	LastUpdate  atomic.Int64
	Status      atomic.Int32 // 0 - NOT INIT, 1 - LOADED, 2 - marked as outdated,  3 - DESTROYED,  4 or above ERRCODE
	Algo        VectorIndexSearchIf
	Cond        *sync.Cond // NOTE: this is RWCond. Wait() will use mutex.RLock() and mutex.RUnlock()
	loadWaiters atomic.Int64
	stale       atomic.Bool // set by the IsStale freshness check; reclaimed next sweep. Separate from
	// ExpireAt so a concurrent Search's extend() (sliding TTL) can't un-mark a stale entry.
}

func (s *VectorIndexSearch) Destroy() {
	s.DestroyWithReason("")
}

func (s *VectorIndexSearch) DestroyWithReason(reason string) {
	s.Mutex.Lock()
	defer func() {
		s.Mutex.Unlock()
		s.Cond.Broadcast()
	}()
	if aware, ok := s.Algo.(cacheInvalidationAware); ok && reason != "" {
		aware.OnCacheInvalidated(reason)
	}
	s.Algo.Destroy()
	// destroyed
	s.Status.Store(STATUS_DESTROYED)
}

func (s *VectorIndexSearch) Load(sqlproc *sqlexec.SqlProcess) error {
	s.Mutex.Lock()
	defer func() {
		s.Mutex.Unlock()
		s.Cond.Broadcast()
	}()

	err := s.Algo.Load(sqlproc)
	if aware, ok := s.Algo.(loadWaiterAware); ok {
		aware.SetLoadWaiters(s.loadWaiters.Load())
	}
	if finisher, ok := s.Algo.(loadObservationFinisher); ok {
		finisher.FinishLoadObservation()
	}
	if err != nil {
		// load error
		s.Status.Store(STATUS_ERROR)
		return err
	}
	// Loaded
	s.Status.Store(STATUS_LOADED)
	s.extend(true)
	return nil
}

func (s *VectorIndexSearch) Expired() bool {
	//s.Mutex.RLock()
	//defer s.Mutex.RUnlock()

	ts := s.ExpireAt.Load()
	now := time.Now().UnixMicro()
	return (ts > 0 && ts < now)
}

// markStale flags this entry for reclamation by the NEXT HouseKeeping sweep. Used by the
// IsStale freshness check to schedule eviction of a stale index without evicting inline (the
// removal always goes through the single expired/stale-sweep path, keeping Search pure-read).
// A dedicated flag (not ExpireAt) so a concurrent Search's extend() sliding TTL cannot
// un-mark a hot stale entry.
func (s *VectorIndexSearch) markStale() {
	s.stale.Store(true)
}

func (s *VectorIndexSearch) extend(update bool) {
	now := time.Now()
	if update {
		s.LastUpdate.Store(now.UnixMicro())
	}
	ts := time.Now().Add(VectorIndexCacheTTL).UnixMicro()
	s.ExpireAt.Store(ts)
}

func (s *VectorIndexSearch) Search(sqlproc *sqlexec.SqlProcess, newalgo VectorIndexSearchIf, query any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {

	preloadWaiter := s.Status.Load() == STATUS_NOT_INIT
	if preloadWaiter {
		s.loadWaiters.Add(1)
	}
	s.Cond.L.Lock()
	defer s.Cond.L.Unlock()
	if preloadWaiter {
		s.loadWaiters.Add(-1)
	}
	for s.Status.Load() == 0 {
		s.loadWaiters.Add(1)
		s.Cond.Wait()
		s.loadWaiters.Add(-1)
	}

	// entry may be removed already
	status := s.Status.Load()
	if status >= STATUS_DESTROYED {
		if status == STATUS_DESTROYED {
			return nil, nil, moerr.NewInvalidStateNoCtx("Index destroyed")
		} else {
			return nil, nil, moerr.NewInternalErrorNoCtx("Load index error")
		}
	}

	// The cached index's configuration is immutable for its lifetime (a config change
	// evicts the entry via Cache.Remove), so there is nothing to refresh from newalgo
	// here. Search is therefore pure-read under the shared read lock — no mutation of the
	// cached algo, so concurrent searches on one entry cannot race on its config.
	s.extend(false)
	return s.Algo.Search(sqlproc, query, rt)
}

// SearchInto mirrors Search but routes to the box-free SearchInto (caller-owned out
// SearchResult). Same shared-read-lock / status discipline.
func (s *VectorIndexSearch) SearchInto(sqlproc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig, out *vectorindex.SearchOutput) error {
	preloadWaiter := s.Status.Load() == STATUS_NOT_INIT
	if preloadWaiter {
		s.loadWaiters.Add(1)
	}
	s.Cond.L.Lock()
	defer s.Cond.L.Unlock()
	if preloadWaiter {
		s.loadWaiters.Add(-1)
	}
	for s.Status.Load() == 0 {
		s.loadWaiters.Add(1)
		s.Cond.Wait()
		s.loadWaiters.Add(-1)
	}
	status := s.Status.Load()
	if status >= STATUS_DESTROYED {
		if status == STATUS_DESTROYED {
			return moerr.NewInvalidStateNoCtx("Index destroyed")
		}
		return moerr.NewInternalErrorNoCtx("Load index error")
	}
	s.extend(false)
	return s.Algo.SearchInto(sqlproc, query, rt, out)
}

// implementation of VectorIndexCache
type VectorIndexCache struct {
	IndexMap       sync.Map
	TickerInterval time.Duration
	ticker         *time.Ticker
	done           chan bool
	sigc           chan os.Signal
	started        atomic.Bool
	exited         atomic.Bool
	once           sync.Once
	hkTicks        int         // HouseKeeping tick counter, gates the IsStale sweep cadence
	staleChecking  atomic.Bool // single-flight guard for the async freshness sweep
}

func NewVectorIndexCache() *VectorIndexCache {
	c := &VectorIndexCache{}
	c.TickerInterval = VectorIndexCacheTTL / 2
	return c
}

func (c *VectorIndexCache) serve() {
	if c.started.Load() {
		return
	}

	// try clean up the temp directory. set tempdir to /tmp/hnsw
	c.ticker = time.NewTicker(c.TickerInterval)
	c.done = make(chan bool)
	c.sigc = make(chan os.Signal, 3)
	signal.Notify(c.sigc, syscall.SIGTERM, syscall.SIGINT, os.Interrupt)

	// channel initizalized.  set started to true
	c.started.Store(true)

	go func() {
		defer c.ticker.Stop()
		for {
			select {
			case <-c.done:
				c.exited.Store(true)
				return
			case <-c.sigc:
				// sig can be syscall.SIGTERM or syscall.SIGINT
				c.exited.Store(true)
				c.Destroy()
				return
			case <-c.ticker.C:
				// delete expired index (fast, no SQL) — always runs synchronously so TTL
				// reclamation and shutdown never wait on a freshness read.
				c.HouseKeeping()
				c.hkTicks++
				if c.hkTicks%stalenessCheckEveryNTicks == 0 && c.staleChecking.CompareAndSwap(false, true) {
					go func() {
						defer c.staleChecking.Store(false)
						c.checkStale()
					}()
				}
			}
		}
	}()
}

// initialize the Cache and only call once
func (c *VectorIndexCache) Once() {
	c.once.Do(func() { c.serve() })
}

// house keeping to check expired keys and delete from cache
func (c *VectorIndexCache) HouseKeeping() {

	expiredkeys := make([]string, 0, 16)

	c.IndexMap.Range(func(key, value any) bool {
		algo := value.(*VectorIndexSearch)
		if algo.Expired() || algo.stale.Load() {
			expiredkeys = append(expiredkeys, key.(string))
		}
		return true
	})

	for _, k := range expiredkeys {
		value, loaded := c.IndexMap.LoadAndDelete(k)
		if loaded {
			algo := value.(*VectorIndexSearch)
			reason := "ttl_expired"
			if algo.stale.Load() {
				reason = "generation_changed"
			}
			algo.DestroyWithReason(reason)
			algo = nil
			logutil.Debugf("[veccache] evicted expired/stale index %s from cache", k)
		}
	}
}

// checkStale asks every loaded StaleChecker entry whether it is stale and marks the stale ones
// (the next HouseKeeping sweep reclaims them; Search stays pure-read). Runs on its own
// goroutine off the ticker (see serve), single-flighted, because each IsStale opens a short
// background txn — so a slow/stalled executor delays only the next freshness sweep, never TTL
// eviction or shutdown. The IsStale calls are collected out of the IndexMap.Range callback so a
// slow meta read never holds up the map iteration, and the loop bails on shutdown.
func (c *VectorIndexCache) checkStale() {
	type staleEntry struct {
		s   *VectorIndexSearch
		sc  StaleChecker
		key any
	}
	entries := make([]staleEntry, 0, 16)
	c.IndexMap.Range(func(key, value any) bool {
		algo := value.(*VectorIndexSearch)
		if algo.Status.Load() != STATUS_LOADED {
			return true // skip loading/errored/destroyed entries
		}
		if sc, ok := algo.Algo.(StaleChecker); ok {
			entries = append(entries, staleEntry{algo, sc, key})
		}
		return true
	})
	for _, e := range entries {
		// Bail promptly on shutdown so a K-entry sweep of ≤1-min SQL reads can't keep this
		// goroutine (and any resources it pins) alive long after Destroy.
		if c.exited.Load() {
			return
		}
		stale, err := e.sc.IsStale()
		if err != nil {
			// A query error usually means the index was dropped/rebuilt out from under us —
			// IsStale returns stale=true so the dead entry is reclaimed; log the cause.
			logutil.Warnf("[veccache] IsStale for index %v errored (treating as stale): %v", e.key, err)
		}
		if stale {
			logutil.Infof("[veccache] index %v is stale — marking for eviction on next sweep", e.key)
			e.s.markStale()
		}
	}
}

// destroy the cache
func (c *VectorIndexCache) Destroy() {
	if c.started.Load() {
		//c.ticker.Stop()
		if !c.exited.Load() {
			c.done <- true
		}
	}
	// remove all keys
	c.IndexMap.Range(func(key, value any) bool {
		c.IndexMap.Delete(key)
		algo := value.(*VectorIndexSearch)
		algo.DestroyWithReason("process_shutdown")
		algo = nil
		return true
	})
}

// Get index from cache and return VectorIndexSearchIf interface
func (c *VectorIndexCache) Search(sqlproc *sqlexec.SqlProcess, key string, newalgo VectorIndexSearchIf,
	query any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {
	for {
		s := &VectorIndexSearch{Algo: newalgo}
		// use RLocker to let Cond.Wait() to use Rlock() and RUnlock()
		s.Cond = sync.NewCond(s.Mutex.RLocker())
		value, loaded := c.IndexMap.LoadOrStore(key, s)
		algo := value.(*VectorIndexSearch)
		if !loaded {
			// load model from database and if error during loading, remove the entry from gIndexMap
			err := algo.Load(sqlproc)
			if err != nil {
				c.IndexMap.Delete(key)
				return nil, nil, err
			}
		}
		keys, distances, err = algo.Search(sqlproc, newalgo, query, rt)
		if err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrInvalidState) {
				// index destroyed by Remove() or HouseKeeping.  Retry!
				continue
			}
			return nil, nil, err
		}

		return keys, distances, nil
	}
}

// SearchInto is the box-free twin of Search: it fills the caller-owned out SearchResult
// (pk/scores/includes as reusable ColumnBuffers) instead of returning boxed []any keys.
// Same LoadOrStore / Load / ErrInvalidState-retry discipline as Search.
func (c *VectorIndexCache) SearchInto(sqlproc *sqlexec.SqlProcess, key string, newalgo VectorIndexSearchIf,
	query any, rt vectorindex.RuntimeConfig, out *vectorindex.SearchOutput) error {
	for {
		s := &VectorIndexSearch{Algo: newalgo}
		s.Cond = sync.NewCond(s.Mutex.RLocker())
		value, loaded := c.IndexMap.LoadOrStore(key, s)
		algo := value.(*VectorIndexSearch)
		if !loaded {
			if err := algo.Load(sqlproc); err != nil {
				c.IndexMap.Delete(key)
				return err
			}
		}
		err := algo.SearchInto(sqlproc, query, rt, out)
		if err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrInvalidState) {
				continue // index destroyed by Remove()/HouseKeeping — retry
			}
			return err
		}
		return nil
	}
}

// remove key from cache
// Remove drops a cached index by key so the next Search reloads it. Callers use
// it after a mutation (CDC append, CREATE/REBUILD/MERGE) makes the cached copy
// stale. It is LOCAL to this process — a prompt local optimization only; cross-CN
// coherence is handled by the pull-based freshness check (StaleChecker/IsStale via
// HouseKeeping), which evicts a remote CN's warm-but-stale entry on its own.
func (c *VectorIndexCache) Remove(key string) {
	c.RemoveWithReason(key, "")
}

// RemoveWithReason is the internal reason-aware variant used by FULLTEXT2.
// The empty reason preserves the historical behavior for all other algorithms.
func (c *VectorIndexCache) RemoveWithReason(key, reason string) {
	value, loaded := c.IndexMap.LoadAndDelete(key)
	if loaded {
		algo := value.(*VectorIndexSearch)
		algo.DestroyWithReason(reason)
		algo = nil
	}
}

// RemovePrefix removes every cached index whose key starts with prefix.
//
// Algorithms that qualify their cache key with mutable state cannot name the
// live key from the DDL path: IVF-FLAT caches under "<indexTable>:<version>"
// (plus "/<cnIdx>/<cnCnt>" when the read is split across CNs), and the version
// comes from the meta table at search time, so a DROP that guessed ":0" evicted
// nothing once the index had been rebuilt. Such callers pass "<indexTable>:"
// and drop every generation at once.
func (c *VectorIndexCache) RemovePrefix(prefix string) {
	keys := make([]string, 0, 4)
	c.IndexMap.Range(func(key, value any) bool {
		if k, ok := key.(string); ok && strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
		return true
	})
	for _, k := range keys {
		c.Remove(k)
	}
}
