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
	"context"
	"errors"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

const (
	defaultStalenessCheckInterval = 30 * time.Second
	defaultFastStaleCheckTimeout  = 30 * time.Second
	defaultStaleEntryTimeout      = 5 * time.Second
	fastStaleCheckConcurrency     = 16
	stalenessCheckEveryNTicks     = 4
)

const (
	freshnessOutcomeFresh      = "fresh"
	freshnessOutcomeStale      = "stale"
	freshnessOutcomeQueryError = "query_error"
	freshnessOutcomeDeadline   = "deadline"
)

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

type retryableLoadError struct {
	cause error
}

func (e retryableLoadError) Error() string {
	return e.cause.Error()
}

func (e retryableLoadError) Unwrap() error {
	return e.cause
}

// NewRetryableLoadError marks a cache-internal load outcome that can be retried
// after the exact failed entry is destroyed. Ordinary algorithm load errors,
// including moerr.ErrInvalidState, must not use this marker.
func NewRetryableLoadError(err error) error {
	if err == nil {
		return nil
	}
	return retryableLoadError{cause: err}
}

func IsRetryableLoadError(err error) bool {
	var marker retryableLoadError
	return errors.As(err, &marker)
}

var lifecycleHooks struct {
	sync.RWMutex
	hooks []func(shutdown bool)
}

// RegisterLifecycleHook lets an algorithm-owned pool attach cleanup to the
// cache's housekeeping and process-shutdown lifecycle. Hooks are invoked
// outside the cache map operations, including when the map is empty.
func RegisterLifecycleHook(hook func(shutdown bool)) {
	if hook == nil {
		return
	}
	lifecycleHooks.Lock()
	lifecycleHooks.hooks = append(lifecycleHooks.hooks, hook)
	lifecycleHooks.Unlock()
}

func runLifecycleHooks(shutdown bool) {
	lifecycleHooks.RLock()
	hooks := append([]func(bool){}, lifecycleHooks.hooks...)
	lifecycleHooks.RUnlock()
	for _, hook := range hooks {
		func() {
			defer func() {
				if r := recover(); r != nil {
					logutil.Errorf("[veccache] lifecycle hook panicked (shutdown=%v): %v", shutdown, r)
				}
			}()
			hook(shutdown)
		}()
	}
}

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

// coherenceRetryPolicy is optional. FULLTEXT2 uses it to bound retryable
// generation supersession; algorithms that do not implement it retain the
// historical destroyed-entry retry behavior.
type coherenceRetryPolicy interface {
	CoherenceRetryPolicy() (maxAttempts int, backoff []time.Duration)
}

// transientLoadPolicy is optional. An algorithm can keep an identity queryable
// while forbidding publication of transaction-snapshot state into this
// process-global cache. Other vector algorithms retain the normal cache path.
type transientLoadPolicy interface {
	UseTransientLoad(*sqlexec.SqlProcess) (bool, error)
}

func coherenceRetry(sqlproc *sqlexec.SqlProcess, algo VectorIndexSearchIf, attempts int) error {
	policy, ok := algo.(coherenceRetryPolicy)
	if !ok {
		return nil
	}
	ctx := context.Background()
	if sqlproc != nil {
		if candidate := sqlproc.GetContext(); candidate != nil {
			ctx = candidate
		}
	}
	maxAttempts, backoff := policy.CoherenceRetryPolicy()
	if maxAttempts <= 0 || attempts >= maxAttempts {
		return moerr.NewServiceUnavailableNoCtx("FULLTEXT2 cache coherence retry exhausted")
	}
	delayIndex := attempts - 1
	if delayIndex < 0 || delayIndex >= len(backoff) || backoff[delayIndex] <= 0 {
		return nil
	}
	timer := time.NewTimer(backoff[delayIndex])
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func newVectorIndexSearch(algo VectorIndexSearchIf) *VectorIndexSearch {
	s := &VectorIndexSearch{Algo: algo}
	s.Cond = sync.NewCond(s.Mutex.RLocker())
	return s
}

func useTransientLoad(sqlproc *sqlexec.SqlProcess, algo VectorIndexSearchIf) (bool, error) {
	policy, ok := algo.(transientLoadPolicy)
	if !ok {
		return false, nil
	}
	return policy.UseTransientLoad(sqlproc)
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
// Search serves a warmed entry without catalog I/O. A context-aware checker
// opts into FULLTEXT2's 30-second bounded pull fallback and same-sweep eviction.
// Existing context-free checkers keep their historical cadence and delayed
// housekeeping eviction.
type StaleChecker interface {
	IsStale() (bool, error)
}

type contextStaleChecker interface {
	IsStaleWithContext(context.Context) (bool, error)
}

// freshnessUncertaintyAware is optional. A context-aware algorithm uses it to
// fail closed after a pull timeout/query error without forcing a periodic
// global eviction. FULLTEXT2 routes subsequent callers through transient loads
// until one durable-generation read succeeds.
type freshnessUncertaintyAware interface {
	OnFreshnessUncertain()
	OnFreshnessConfirmed()
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
	ttlMu       sync.Mutex  // serializes sliding TTL renewal with eviction claims
	stale       atomic.Bool // set by the IsStale freshness check; reclaimed next sweep. Separate from
	// ExpireAt so a concurrent Search's extend() (sliding TTL) can't un-mark a stale entry.
	evicting         atomic.Bool
	deferredDestroy  atomic.Bool
	invalidationOnce sync.Once
}

func (s *VectorIndexSearch) Destroy() {
	s.DestroyWithReason("")
}

func (s *VectorIndexSearch) notifyCacheInvalidated(reason string) {
	if reason == "" {
		return
	}
	s.invalidationOnce.Do(func() {
		if aware, ok := s.Algo.(cacheInvalidationAware); ok {
			aware.OnCacheInvalidated(reason)
		}
	})
}

func (s *VectorIndexSearch) beginEviction(recheckTTL bool) bool {
	s.ttlMu.Lock()
	defer s.ttlMu.Unlock()
	if recheckTTL {
		// Search renews ExpireAt under the same gate. The final TTL check and
		// claim are therefore atomic with respect to a sliding renewal.
		if !s.stale.Load() && !s.Expired() {
			return false
		}
	}
	return s.evicting.CompareAndSwap(false, true)
}

// finishDeferredDestroy lets the first caller that observes no remaining
// reader/load lease become the unique destruction owner. It never waits: a
// failed TryLock means an existing lease will retry this method on release.
func (s *VectorIndexSearch) finishDeferredDestroy() {
	if !s.deferredDestroy.Load() || !s.Mutex.TryLock() {
		return
	}
	if !s.deferredDestroy.CompareAndSwap(true, false) {
		s.Mutex.Unlock()
		return
	}
	s.Algo.Destroy()
	s.Status.Store(STATUS_DESTROYED)
	s.Mutex.Unlock()
	s.Cond.Broadcast()
}

func (s *VectorIndexSearch) DestroyWithReason(reason string) {
	s.Mutex.Lock()
	defer func() {
		s.Mutex.Unlock()
		s.Cond.Broadcast()
	}()
	s.notifyCacheInvalidated(reason)
	s.Algo.Destroy()
	// destroyed
	s.Status.Store(STATUS_DESTROYED)
}

// destroyFailedLoad releases an entry whose load failed after the caller has
// removed that exact entry from the map. It deliberately skips the optional
// invalidation hook: FULLTEXT2 clears reusable state in Load's error defer, and
// invoking a key-wide hook after a replacement load starts could clear the new
// generation. The lock still serializes destruction with waiters/searchers.
func (s *VectorIndexSearch) destroyFailedLoad() {
	s.Mutex.Lock()
	defer func() {
		s.Mutex.Unlock()
		s.Cond.Broadcast()
	}()
	s.Algo.Destroy()
	s.Status.Store(STATUS_DESTROYED)
}

func (s *VectorIndexSearch) Load(sqlproc *sqlexec.SqlProcess) error {
	s.Mutex.Lock()
	defer func() {
		s.Mutex.Unlock()
		s.Cond.Broadcast()
		s.finishDeferredDestroy()
	}()
	if s.evicting.Load() {
		return moerr.NewInvalidStateNoCtx("Index destroyed")
	}

	err := s.Algo.Load(sqlproc)
	if aware, ok := s.Algo.(loadWaiterAware); ok {
		aware.SetLoadWaiters(s.loadWaiters.Load())
	}
	if finisher, ok := s.Algo.(loadObservationFinisher); ok {
		finisher.FinishLoadObservation()
	}
	if err != nil {
		// Superseded loads are retryable for both the initiating caller and
		// waiters already blocked on this entry. Publish the destroyed state
		// before Broadcast so every waiter takes the retry path. Other load
		// errors, including ordinary ErrInvalidState, remain terminal.
		if IsRetryableLoadError(err) {
			s.Status.Store(STATUS_DESTROYED)
		} else {
			s.Status.Store(STATUS_ERROR)
		}
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
	s.ttlMu.Lock()
	defer s.ttlMu.Unlock()
	s.extendLocked(update)
}

func (s *VectorIndexSearch) extendLocked(update bool) {
	now := time.Now()
	if update {
		s.LastUpdate.Store(now.UnixMicro())
	}
	ts := time.Now().Add(VectorIndexCacheTTL).UnixMicro()
	s.ExpireAt.Store(ts)
}

func (s *VectorIndexSearch) extendForSearch() bool {
	s.ttlMu.Lock()
	defer s.ttlMu.Unlock()
	if s.evicting.Load() {
		return false
	}
	s.extendLocked(false)
	return true
}

func (s *VectorIndexSearch) Search(sqlproc *sqlexec.SqlProcess, newalgo VectorIndexSearchIf, query any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {

	preloadWaiter := s.Status.Load() == STATUS_NOT_INIT
	if preloadWaiter {
		s.loadWaiters.Add(1)
	}
	s.Cond.L.Lock()
	defer func() {
		s.Cond.L.Unlock()
		s.finishDeferredDestroy()
	}()
	if preloadWaiter {
		s.loadWaiters.Add(-1)
	}
	if s.evicting.Load() {
		return nil, nil, moerr.NewInvalidStateNoCtx("Index destroyed")
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
	if !s.extendForSearch() {
		return nil, nil, moerr.NewInvalidStateNoCtx("Index destroyed")
	}
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
	defer func() {
		s.Cond.L.Unlock()
		s.finishDeferredDestroy()
	}()
	if preloadWaiter {
		s.loadWaiters.Add(-1)
	}
	if s.evicting.Load() {
		return moerr.NewInvalidStateNoCtx("Index destroyed")
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
	if !s.extendForSearch() {
		return moerr.NewInvalidStateNoCtx("Index destroyed")
	}
	return s.Algo.SearchInto(sqlproc, query, rt, out)
}

// implementation of VectorIndexCache
type VectorIndexCache struct {
	IndexMap           sync.Map
	TickerInterval     time.Duration
	StaleCheckInterval time.Duration
	ticker             *time.Ticker
	staleTicker        *time.Ticker
	done               chan bool
	sigc               chan os.Signal
	started            atomic.Bool
	exited             atomic.Bool
	once               sync.Once
	hkTicks            int
	staleChecking      atomic.Bool // single-flight guard for the historical vector-index sweep
	fastStaleChecking  atomic.Bool // independent FULLTEXT2 30-second sweep
	staleCancel        context.CancelFunc
	fastStaleTimeout   time.Duration
}

func NewVectorIndexCache() *VectorIndexCache {
	c := &VectorIndexCache{}
	c.TickerInterval = VectorIndexCacheTTL / 2
	c.StaleCheckInterval = defaultStalenessCheckInterval
	c.fastStaleTimeout = defaultFastStaleCheckTimeout
	return c
}

func (c *VectorIndexCache) serve() {
	if c.started.Load() {
		return
	}

	// try clean up the temp directory. set tempdir to /tmp/hnsw
	c.ticker = time.NewTicker(c.TickerInterval)
	c.staleTicker = time.NewTicker(c.StaleCheckInterval)
	staleCtx, staleCancel := context.WithCancel(context.Background())
	c.staleCancel = staleCancel
	c.done = make(chan bool)
	c.sigc = make(chan os.Signal, 3)
	signal.Notify(c.sigc, syscall.SIGTERM, syscall.SIGINT, os.Interrupt)

	// channel initizalized.  set started to true
	c.started.Store(true)

	go func() {
		defer c.ticker.Stop()
		defer c.staleTicker.Stop()
		for {
			select {
			case <-c.done:
				staleCancel()
				c.exited.Store(true)
				return
			case <-c.sigc:
				staleCancel()
				// sig can be syscall.SIGTERM or syscall.SIGINT
				c.exited.Store(true)
				c.Destroy()
				return
			case <-c.ticker.C:
				// delete expired index (fast, no SQL) — always runs synchronously so TTL
				// reclamation and shutdown never wait on a freshness read.
				c.HouseKeeping()
				c.hkTicks++
				if c.hkTicks%stalenessCheckEveryNTicks == 0 {
					c.startStaleCheck(staleCtx, false)
				}
			case <-c.staleTicker.C:
				c.startStaleCheck(staleCtx, true)
			}
		}
	}()
}

// initialize the Cache and only call once
func (c *VectorIndexCache) Once() {
	c.once.Do(func() { c.serve() })
}

func (c *VectorIndexCache) evictEntry(key string, expected *VectorIndexSearch, reason string) bool {
	value, loaded := c.IndexMap.Load(key)
	if !loaded {
		return false
	}
	algo, ok := value.(*VectorIndexSearch)
	if !ok || (expected != nil && algo != expected) || !algo.beginEviction(reason == "ttl_expired") {
		return false
	}
	value, loaded = c.IndexMap.Load(key)
	if !loaded || value != algo {
		return false
	}
	// Publish the invalidation while the old entry still occupies the key. A
	// replacement can only be inserted after CompareAndDelete, so it cannot
	// observe a later generation bump from the old entry's blocked destroy.
	algo.notifyCacheInvalidated(reason)
	if !c.IndexMap.CompareAndDelete(key, algo) {
		return false
	}
	algo.Destroy()
	return true
}

func (c *VectorIndexCache) discardFailedLoad(key string, algo *VectorIndexSearch) {
	if c.IndexMap.CompareAndDelete(key, algo) {
		algo.destroyFailedLoad()
	}
}

// house keeping to check expired keys and delete from cache
func (c *VectorIndexCache) HouseKeeping() {

	type expiredEntry struct {
		key  string
		algo *VectorIndexSearch
	}
	expiredkeys := make([]expiredEntry, 0, 16)

	c.IndexMap.Range(func(key, value any) bool {
		algo := value.(*VectorIndexSearch)
		if algo.Expired() || algo.stale.Load() {
			expiredkeys = append(expiredkeys, expiredEntry{key: key.(string), algo: algo})
		}
		return true
	})

	for _, entry := range expiredkeys {
		if !entry.algo.Expired() && !entry.algo.stale.Load() {
			continue
		}
		reason := "ttl_expired"
		if entry.algo.stale.Load() {
			reason = "generation_changed"
		}
		if c.evictEntry(entry.key, entry.algo, reason) {
			logutil.Debugf("[veccache] evicted expired/stale index %s from cache", entry.key)
		}
	}
	runLifecycleHooks(false)
}

// checkStale asks loaded StaleChecker entries whether they are stale. The fast
// context-aware cohort is FULLTEXT2-only and is evicted in the same sweep; the
// historical cohort is only marked for the next HouseKeeping pass. Both modes
// are independently single-flighted.
func (c *VectorIndexCache) startStaleCheck(ctx context.Context, fast bool) bool {
	guard := &c.staleChecking
	if fast {
		guard = &c.fastStaleChecking
	}
	if !guard.CompareAndSwap(false, true) {
		return false
	}
	go func() {
		defer guard.Store(false)
		c.checkStale(ctx, fast)
	}()
	return true
}

type staleCheckEntry struct {
	s   *VectorIndexSearch
	sc  StaleChecker
	key string
}

type freshnessSweepStats struct {
	fresh      int
	stale      int
	queryError int
	deadline   int
}

func (s *freshnessSweepStats) record(outcome string) {
	switch outcome {
	case freshnessOutcomeFresh:
		s.fresh++
	case freshnessOutcomeStale:
		s.stale++
	case freshnessOutcomeQueryError:
		s.queryError++
	case freshnessOutcomeDeadline:
		s.deadline++
	}
	metricv2.VectorIndexCacheFreshnessSweepEntriesCounter.WithLabelValues(outcome).Inc()
}

func (s *freshnessSweepStats) add(other freshnessSweepStats) {
	s.fresh += other.fresh
	s.stale += other.stale
	s.queryError += other.queryError
	s.deadline += other.deadline
}

func (c *VectorIndexCache) checkStale(ctx context.Context, fast bool) freshnessSweepStats {
	entries := make([]staleCheckEntry, 0, 16)
	c.IndexMap.Range(func(key, value any) bool {
		algo := value.(*VectorIndexSearch)
		if algo.Status.Load() != STATUS_LOADED {
			return true // skip loading/errored/destroyed entries
		}
		sc, ok := algo.Algo.(StaleChecker)
		if !ok {
			return true
		}
		_, contextAware := sc.(contextStaleChecker)
		if contextAware == fast {
			entries = append(entries, staleCheckEntry{algo, sc, key.(string)})
		}
		return true
	})
	if !fast {
		for _, e := range entries {
			if c.exited.Load() || ctx.Err() != nil {
				return freshnessSweepStats{}
			}
			stale, err := e.sc.IsStale()
			if err != nil {
				logutil.Warnf("[veccache] IsStale for index %v errored (treating as stale): %v", e.key, err)
			}
			if stale {
				e.s.markStale()
			}
		}
		return freshnessSweepStats{}
	}
	return c.checkFastStale(ctx, entries)
}

func (c *VectorIndexCache) checkFastStale(ctx context.Context, entries []staleCheckEntry) freshnessSweepStats {
	started := time.Now()
	timeout := c.fastStaleTimeout
	if timeout <= 0 {
		timeout = defaultFastStaleCheckTimeout
	}
	sweepCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	workerCount := min(fastStaleCheckConcurrency, len(entries))
	workerStats := make(chan freshnessSweepStats, workerCount)
	var next atomic.Uint64
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var stats freshnessSweepStats
			defer func() { workerStats <- stats }()
			for {
				i := int(next.Add(1) - 1)
				if i >= len(entries) {
					return
				}
				e := entries[i]
				if sweepCtx.Err() != nil {
					if aware, ok := e.sc.(freshnessUncertaintyAware); ok {
						aware.OnFreshnessUncertain()
					}
					stats.record(freshnessOutcomeDeadline)
					continue
				}
				stats.record(c.checkFastStaleEntry(sweepCtx, e))
			}
		}()
	}
	wg.Wait()
	close(workerStats)

	var stats freshnessSweepStats
	for worker := range workerStats {
		stats.add(worker)
	}
	duration := time.Since(started)
	metricv2.VectorIndexCacheFreshnessSweepDurationHistogram.Observe(duration.Seconds())
	if stats.queryError > 0 || stats.deadline > 0 {
		logutil.Warnf("[veccache] freshness sweep completed: entries=%d fresh=%d stale=%d query_error=%d deadline=%d duration=%s",
			len(entries), stats.fresh, stats.stale, stats.queryError, stats.deadline, duration)
	} else {
		logutil.Debugf("[veccache] freshness sweep completed: entries=%d fresh=%d stale=%d duration=%s",
			len(entries), stats.fresh, stats.stale, duration)
	}
	return stats
}

func (c *VectorIndexCache) checkFastStaleEntry(ctx context.Context, e staleCheckEntry) string {
	checkCtx, cancel := context.WithTimeout(ctx, defaultStaleEntryTimeout)
	defer cancel()
	checker := e.sc.(contextStaleChecker)
	stale, err := checker.IsStaleWithContext(checkCtx)
	if err != nil {
		if aware, ok := e.sc.(freshnessUncertaintyAware); ok {
			aware.OnFreshnessUncertain()
		}
		if checkCtx.Err() != nil {
			return freshnessOutcomeDeadline
		}
		return freshnessOutcomeQueryError
	}
	if aware, ok := e.sc.(freshnessUncertaintyAware); ok {
		aware.OnFreshnessConfirmed()
	}
	if stale {
		c.claimRemoveEntryWithReason(e.key, e.s, "generation_changed")
		return freshnessOutcomeStale
	}
	return freshnessOutcomeFresh
}

// destroy the cache
func (c *VectorIndexCache) Destroy() {
	if c.staleCancel != nil {
		c.staleCancel()
	}
	if c.started.Load() {
		//c.ticker.Stop()
		if !c.exited.Load() {
			c.done <- true
		}
	}
	// remove all keys
	c.IndexMap.Range(func(key, value any) bool {
		if k, ok := key.(string); ok {
			c.evictEntry(k, value.(*VectorIndexSearch), "process_shutdown")
		}
		return true
	})
	runLifecycleHooks(true)
}

// Get index from cache and return VectorIndexSearchIf interface
func (c *VectorIndexCache) Search(sqlproc *sqlexec.SqlProcess, key string, newalgo VectorIndexSearchIf,
	query any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {
	transient, err := useTransientLoad(sqlproc, newalgo)
	if err != nil {
		return nil, nil, err
	}
	if transient {
		return searchTransient(sqlproc, newalgo, query, rt, 0)
	}
	attempts := 0
	for {
		attempts++
		if attempts > 1 {
			transient, err = useTransientLoad(sqlproc, newalgo)
			if err != nil {
				return nil, nil, err
			}
			if transient {
				return searchTransient(sqlproc, newalgo, query, rt, attempts-1)
			}
		}
		s := newVectorIndexSearch(newalgo)
		value, loaded := c.IndexMap.LoadOrStore(key, s)
		algo := value.(*VectorIndexSearch)
		if !loaded {
			// Remove only this exact failed entry, then destroy it without a
			// key-wide invalidation hook; the loader owns reusable-state rollback.
			err := algo.Load(sqlproc)
			if err != nil {
				if algo.evicting.Load() {
					if retryErr := coherenceRetry(sqlproc, newalgo, attempts); retryErr != nil {
						return nil, nil, retryErr
					}
					continue
				}
				if IsRetryableLoadError(err) {
					c.discardFailedLoad(key, algo)
					if retryErr := coherenceRetry(sqlproc, newalgo, attempts); retryErr != nil {
						return nil, nil, retryErr
					}
					continue
				}
				if c.IndexMap.CompareAndDelete(key, algo) {
					algo.destroyFailedLoad()
				}
				return nil, nil, err
			}
		}
		keys, distances, err = algo.Search(sqlproc, newalgo, query, rt)
		if err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrInvalidState) {
				// index destroyed by Remove() or HouseKeeping.  Retry!
				if retryErr := coherenceRetry(sqlproc, newalgo, attempts); retryErr != nil {
					return nil, nil, retryErr
				}
				continue
			}
			return nil, nil, err
		}

		return keys, distances, nil
	}
}

func searchTransient(sqlproc *sqlexec.SqlProcess, algo VectorIndexSearchIf, query any, rt vectorindex.RuntimeConfig, attempts int) (keys any, distances []float64, err error) {
	for {
		attempts++
		s := newVectorIndexSearch(algo)
		if err = s.Load(sqlproc); err != nil {
			s.destroyFailedLoad()
			if IsRetryableLoadError(err) {
				if err = coherenceRetry(sqlproc, algo, attempts); err == nil {
					continue
				}
			}
			return nil, nil, err
		}
		keys, distances, err = s.Search(sqlproc, algo, query, rt)
		s.Destroy()
		if err != nil && moerr.IsMoErrCode(err, moerr.ErrInvalidState) {
			if err = coherenceRetry(sqlproc, algo, attempts); err == nil {
				continue
			}
		}
		return keys, distances, err
	}
}

// SearchInto is the box-free twin of Search: it fills the caller-owned out SearchResult
// (pk/scores/includes as reusable ColumnBuffers) instead of returning boxed []any keys.
// Same LoadOrStore / retryable-load discipline as Search.
func (c *VectorIndexCache) SearchInto(sqlproc *sqlexec.SqlProcess, key string, newalgo VectorIndexSearchIf,
	query any, rt vectorindex.RuntimeConfig, out *vectorindex.SearchOutput) error {
	transient, err := useTransientLoad(sqlproc, newalgo)
	if err != nil {
		return err
	}
	if transient {
		return searchIntoTransient(sqlproc, newalgo, query, rt, out, 0)
	}
	attempts := 0
	for {
		attempts++
		if attempts > 1 {
			transient, err = useTransientLoad(sqlproc, newalgo)
			if err != nil {
				return err
			}
			if transient {
				return searchIntoTransient(sqlproc, newalgo, query, rt, out, attempts-1)
			}
		}
		s := newVectorIndexSearch(newalgo)
		value, loaded := c.IndexMap.LoadOrStore(key, s)
		algo := value.(*VectorIndexSearch)
		if !loaded {
			if err := algo.Load(sqlproc); err != nil {
				if algo.evicting.Load() {
					if retryErr := coherenceRetry(sqlproc, newalgo, attempts); retryErr != nil {
						return retryErr
					}
					continue
				}
				if IsRetryableLoadError(err) {
					c.discardFailedLoad(key, algo)
					if retryErr := coherenceRetry(sqlproc, newalgo, attempts); retryErr != nil {
						return retryErr
					}
					continue
				}
				if c.IndexMap.CompareAndDelete(key, algo) {
					algo.destroyFailedLoad()
				}
				return err
			}
		}
		err := algo.SearchInto(sqlproc, query, rt, out)
		if err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrInvalidState) {
				if retryErr := coherenceRetry(sqlproc, newalgo, attempts); retryErr != nil {
					return retryErr
				}
				continue // index destroyed by Remove()/HouseKeeping — retry
			}
			return err
		}
		return nil
	}
}

func searchIntoTransient(sqlproc *sqlexec.SqlProcess, algo VectorIndexSearchIf, query any, rt vectorindex.RuntimeConfig, out *vectorindex.SearchOutput, attempts int) (err error) {
	for {
		attempts++
		s := newVectorIndexSearch(algo)
		if err = s.Load(sqlproc); err != nil {
			s.destroyFailedLoad()
			if IsRetryableLoadError(err) {
				if err = coherenceRetry(sqlproc, algo, attempts); err == nil {
					continue
				}
			}
			return err
		}
		err = s.SearchInto(sqlproc, query, rt, out)
		s.Destroy()
		if err != nil && moerr.IsMoErrCode(err, moerr.ErrInvalidState) {
			if err = coherenceRetry(sqlproc, algo, attempts); err == nil {
				continue
			}
		}
		return err
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
	c.evictEntry(key, nil, reason)
}

// ClaimRemoveWithReason publishes an eviction claim without waiting for
// searches that already hold the entry's read lease. Once beginEviction and
// CompareAndDelete succeed, new searches cannot acquire the old object; the
// object records deferred destruction; the last reader/load lease becomes its
// unique cleanup owner. FULLTEXT2 generation fences use this path so an RPC ACK means the
// admission barrier is installed, not that pre-claim queries were canceled.
func (c *VectorIndexCache) ClaimRemoveWithReason(key, reason string) {
	c.claimRemoveEntryWithReason(key, nil, reason)
}

// claimRemoveEntryWithReason is the snapshot-safe form used by freshness
// sweeps. A result obtained from an old entry must not evict a replacement that
// was published under the same key while the check was in flight.
func (c *VectorIndexCache) claimRemoveEntryWithReason(key string, expected *VectorIndexSearch, reason string) {
	value, loaded := c.IndexMap.Load(key)
	if !loaded {
		return
	}
	algo, ok := value.(*VectorIndexSearch)
	if !ok || (expected != nil && algo != expected) || !algo.beginEviction(false) {
		return
	}
	value, loaded = c.IndexMap.Load(key)
	if !loaded || value != algo {
		return
	}
	algo.notifyCacheInvalidated(reason)
	if !c.IndexMap.CompareAndDelete(key, algo) {
		return
	}
	algo.deferredDestroy.Store(true)
	algo.finishDeferredDestroy()
}

// ClaimRemovePrefixWithReason publishes non-blocking eviction claims for all
// currently visible entries below one algorithm-owned prefix.
func (c *VectorIndexCache) ClaimRemovePrefixWithReason(prefix, reason string) {
	keys := make([]string, 0, 4)
	c.IndexMap.Range(func(key, value any) bool {
		if k, ok := key.(string); ok && strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
		return true
	})
	for _, key := range keys {
		c.ClaimRemoveWithReason(key, reason)
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
