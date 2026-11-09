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
	"math/rand/v2"
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

const (
	evictionTTL        = "ttl_expired"
	evictionGeneration = "generation_changed"
	evictionShutdown   = "process_shutdown"
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
	errIndexDestroyed                     = moerr.NewInvalidStateNoCtx("Index destroyed")
	errCacheClosed                        = moerr.NewInvalidStateNoCtx("Index cache is shut down")
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

func exhaustedLoadError(err error) error {
	var marker retryableLoadError
	if errors.As(err, &marker) {
		return marker.cause
	}
	if errors.Is(err, errIndexDestroyed) {
		return moerr.NewInvalidStateNoCtx("index changed while loading; retry the query")
	}
	return err
}

var lifecycleHooks struct {
	sync.RWMutex
	hooks []func(shutdown bool)
}

// RegisterLifecycleHook lets an algorithm-owned pool attach cleanup to the
// cache's housekeeping and process-shutdown lifecycle. Hooks are invoked
// outside the cache map operations, including when the map is empty. A hook
// must not synchronously shut down the cache from which it is invoked.
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

// VectorIndexSearchIf is implemented by algorithms shared through the cache.
// Its methods must not synchronously shut down the containing cache.
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
	// OnCacheInvalidated must not synchronously shut down the invoking cache.
	OnCacheInvalidated(reason string)
}

// A recoverer can repair only the named entry after its failed callback, with
// no effects on resources belonging to other entries. It runs behind the old
// entry's write lock, before a replacement can be admitted.
type cacheInvalidationRecoverer interface {
	RecoverCacheInvalidation(reason string) error
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
	Status      atomic.Int32 // one of the STATUS_* constants
	Algo        VectorIndexSearchIf
	loadWaiters atomic.Int64
	ttlMu       sync.Mutex  // serializes sliding TTL renewal with eviction claims
	stale       atomic.Bool // set by the IsStale freshness check; reclaimed next sweep. Separate from
	// ExpireAt so a concurrent Search's extend() (sliding TTL) can't un-mark a stale entry.
	evicting         atomic.Bool
	invalidationOnce sync.Once
	destroyed        chan struct{}
	destroyOnce      sync.Once
	ready            chan struct{}
	readyOnce        sync.Once
	// Closed after removal or after an invalidation failure is published.
	evictionResultReady chan struct{}
	evictionResultOnce  sync.Once
	done                <-chan struct{} // owned and closed by the containing cache
	loadResult          atomic.Pointer[entryLoadResult]
	destroyResult       atomic.Pointer[entryLoadResult]
	invalidationResult  atomic.Pointer[entryLoadResult]
}

type entryLoadResult struct{ err error }

func newVectorIndexSearch(algo VectorIndexSearchIf) *VectorIndexSearch {
	s := &VectorIndexSearch{Algo: algo, destroyed: make(chan struct{}), ready: make(chan struct{}), evictionResultReady: make(chan struct{})}
	return s
}

func (s *VectorIndexSearch) markReady() {
	s.readyOnce.Do(func() { close(s.ready) })
}

func (s *VectorIndexSearch) markEvictionResultReady() {
	s.evictionResultOnce.Do(func() { close(s.evictionResultReady) })
}

func (s *VectorIndexSearch) terminalLoadError() error {
	if result := s.loadResult.Load(); result != nil && result.err != nil && !IsRetryableLoadError(result.err) && !errors.Is(result.err, errIndexDestroyed) {
		return result.err
	}
	return nil
}

func (s *VectorIndexSearch) invalidationError(ctx context.Context) error {
	if result := s.invalidationResult.Load(); result != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
		return result.err
	}
	return nil
}

func (s *VectorIndexSearch) observedLoadError(ctx context.Context) error {
	if err := s.terminalLoadError(); err != nil {
		if invalidationErr := s.invalidationError(ctx); invalidationErr != nil {
			return invalidationErr
		}
		return err
	}
	return nil
}

func (s *VectorIndexSearch) awaitReady(ctx context.Context) error {
	select {
	case <-s.ready:
		if err := s.terminalLoadError(); err != nil {
			if invalidationErr := s.invalidationError(ctx); invalidationErr != nil {
				return invalidationErr
			}
			return err
		}
		// An invalidation result is published only after evicting becomes true.
		if s.evicting.Load() {
			if err := s.invalidationError(ctx); err != nil {
				return err
			}
			return errIndexDestroyed
		}
		return nil
	default:
	}
	if s.evicting.Load() {
		if err := s.invalidationError(ctx); err != nil {
			return err
		}
		if err := s.observedLoadError(ctx); err != nil {
			return err
		}
		return errIndexDestroyed
	}
	if err := s.observedLoadError(ctx); err != nil {
		return err
	}
	s.loadWaiters.Add(1)
	defer s.loadWaiters.Add(-1)
	select {
	case <-s.ready:
		if err := s.invalidationError(ctx); err != nil {
			return err
		}
		if err := s.observedLoadError(ctx); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case <-s.done:
			if err := s.invalidationError(ctx); err != nil {
				return err
			}
			return errCacheClosed
		default:
			return nil
		}
	case <-s.evictionResultReady:
		if err := s.invalidationError(ctx); err != nil {
			return err
		}
		if err := s.observedLoadError(ctx); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return errIndexDestroyed
	case <-s.done:
		if err := s.invalidationError(ctx); err != nil {
			return err
		}
		if err := s.observedLoadError(ctx); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return errCacheClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *VectorIndexSearch) markDestroyed() {
	if s.destroyed != nil {
		s.destroyOnce.Do(func() { close(s.destroyed) })
	}
}

func searchContext(proc *sqlexec.SqlProcess) context.Context {
	if proc != nil && (proc.Proc != nil || proc.SqlCtx != nil) {
		if ctx := proc.GetContext(); ctx != nil {
			return ctx
		}
	}
	return context.Background()
}

// Only the creator of the wrapper may wait: other callers have their own
// algorithm instances and do not need to wait for this one's destruction.
func (s *VectorIndexSearch) awaitDestroyed(ctx context.Context) error {
	if s.destroyed == nil {
		return nil
	}
	evictionResult := s.evictionResultReady
	for {
		select {
		case <-s.destroyed:
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := s.invalidationError(ctx); err != nil {
				return err
			}
			select {
			case <-s.done:
				return errCacheClosed
			default:
			}
			if result := s.destroyResult.Load(); result != nil {
				return result.err
			}
			return nil
		case <-evictionResult:
			if err := s.invalidationError(ctx); err != nil {
				return err
			}
			// Normal eviction removes the map entry before destruction finishes.
			evictionResult = nil
		case <-ctx.Done():
			return ctx.Err()
		case <-s.done:
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := s.invalidationError(ctx); err != nil {
				return err
			}
			return errCacheClosed
		}
	}
}

// Destroy releases a detached entry. Mapped entries must be removed through
// their VectorIndexCache so that map removal and waiter notification agree.
func (s *VectorIndexSearch) Destroy() {
	s.DestroyWithReason("")
}

func (s *VectorIndexSearch) notifyCacheInvalidated(reason string) (err error) {
	if reason == "" {
		return nil
	}
	defer func() {
		if p := recover(); p != nil {
			err = moerr.ConvertPanicError(context.Background(), p)
		}
	}()
	s.invalidationOnce.Do(func() {
		if aware, ok := s.Algo.(cacheInvalidationAware); ok {
			aware.OnCacheInvalidated(reason)
		}
	})
	return nil
}

// recoverInvalidation fences the old load and readers before asking the
// algorithm owner to repair its own state. The entry still occupies its key.
func (s *VectorIndexSearch) recoverInvalidation(reason string) (supported bool, err error) {
	recoverer, ok := s.Algo.(cacheInvalidationRecoverer)
	if !ok {
		return false, nil
	}
	supported = true
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	defer func() {
		if p := recover(); p != nil {
			err = moerr.ConvertPanicError(context.Background(), p)
		}
	}()
	err = recoverer.RecoverCacheInvalidation(reason)
	return
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

func (s *VectorIndexSearch) DestroyWithReason(reason string) {
	s.destroy(reason)
}

// destroyFailedLoad releases an entry whose load failed after the caller has
// removed that exact entry from the map. It deliberately skips the optional
// invalidation hook: FULLTEXT2 clears reusable state in Load's error defer, and
// invoking a key-wide hook after a replacement load starts could clear the new
// generation. The lock still serializes destruction with waiters/searchers.
func (s *VectorIndexSearch) destroyFailedLoad() {
	s.destroy("")
}

func (s *VectorIndexSearch) destroy(reason string) {
	// Standalone destruction must use the same admission gate as cache eviction.
	s.beginEviction(false)
	s.Mutex.Lock()
	defer func() {
		if p := recover(); p != nil {
			err := moerr.ConvertPanicError(context.Background(), p)
			s.destroyResult.CompareAndSwap(nil, &entryLoadResult{err: err})
			logutil.Errorf("[veccache] index destruction failed: %v", err)
		}
		s.Status.Store(STATUS_DESTROYED)
		s.Mutex.Unlock()
		s.markDestroyed()
		s.markReady()
	}()
	if err := s.notifyCacheInvalidated(reason); err != nil {
		s.destroyResult.CompareAndSwap(nil, &entryLoadResult{err: err})
		logutil.Errorf("[veccache] index invalidation failed: %v", err)
	}
	s.Algo.Destroy()
}

func (s *VectorIndexSearch) Load(sqlproc *sqlexec.SqlProcess) (err error) {
	s.Mutex.Lock()
	if status := s.Status.Load(); status != STATUS_NOT_INIT {
		s.Mutex.Unlock()
		if status == STATUS_DESTROYED || s.evicting.Load() {
			return errIndexDestroyed
		}
		if err := s.terminalLoadError(); err != nil {
			return err
		}
		return moerr.NewInvalidStateNoCtx("index entry already initialized")
	}
	defer func() {
		if p := recover(); p != nil {
			err = moerr.ConvertPanicError(searchContext(sqlproc), p)
			s.Status.Store(STATUS_ERROR)
		}
		s.loadResult.Store(&entryLoadResult{err: err})
		s.Mutex.Unlock()
		s.markReady()
	}()
	if s.evicting.Load() {
		return errIndexDestroyed
	}

	err = s.Algo.Load(sqlproc)
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
	ts := now.Add(VectorIndexCacheTTL).UnixMicro()
	s.ExpireAt.Store(ts)
}

// lockForSearch renews the TTL and returns with the entry read lock held.
// An evictor must claim ttlMu before it can wait for the entry write lock.
func (s *VectorIndexSearch) lockForSearch() error {
	s.ttlMu.Lock()
	defer s.ttlMu.Unlock()
	if s.evicting.Load() {
		return errIndexDestroyed
	}
	s.Mutex.RLock()
	if status := s.Status.Load(); status >= STATUS_DESTROYED {
		s.Mutex.RUnlock()
		if status == STATUS_DESTROYED {
			return errIndexDestroyed
		}
		return s.loadResult.Load().err
	}
	s.extendLocked(false)
	return nil
}

func (s *VectorIndexSearch) Search(sqlproc *sqlexec.SqlProcess, newalgo VectorIndexSearchIf, query any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {
	if err := s.awaitReady(searchContext(sqlproc)); err != nil {
		return nil, nil, err
	}
	if err := s.lockForSearch(); err != nil {
		return nil, nil, err
	}
	defer s.Mutex.RUnlock()

	// The cached index's configuration is immutable for its lifetime (a config change
	// evicts the entry via Cache.Remove), so there is nothing to refresh from newalgo
	// here. Search is therefore pure-read under the shared read lock — no mutation of the
	// cached algo, so concurrent searches on one entry cannot race on its config.
	return s.Algo.Search(sqlproc, query, rt)
}

// SearchInto mirrors Search but routes to the box-free SearchInto (caller-owned out
// SearchResult). Same shared-read-lock / status discipline.
func (s *VectorIndexSearch) SearchInto(sqlproc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig, out *vectorindex.SearchOutput) error {
	if err := s.awaitReady(searchContext(sqlproc)); err != nil {
		return err
	}
	if err := s.lockForSearch(); err != nil {
		return err
	}
	defer s.Mutex.RUnlock()
	return s.Algo.SearchInto(sqlproc, query, rt, out)
}

// implementation of VectorIndexCache
type VectorIndexCache struct {
	// IndexMap is exposed for inspection. The cache owns its entries; use
	// Search and Remove rather than changing the map or destroying entries.
	IndexMap       sync.Map
	TickerInterval time.Duration
	ticker         *time.Ticker
	done           chan struct{}
	sigc           chan os.Signal
	started        atomic.Bool
	exited         atomic.Bool
	once           sync.Once
	shutdownOnce   sync.Once
	lifecycleMu    sync.Mutex
	shutdownErr    error          // protected by lifecycleMu
	cleanups       sync.WaitGroup // admission is protected by lifecycleMu
	hkTicks        int            // HouseKeeping tick counter, gates the IsStale sweep cadence
	staleChecking  atomic.Bool    // single-flight guard for the async freshness sweep
}

func NewVectorIndexCache() *VectorIndexCache {
	c := &VectorIndexCache{done: make(chan struct{})}
	c.TickerInterval = VectorIndexCacheTTL / 2
	return c
}

// beginCleanup registers an operation before shutdown seals cleanup admission.
func (c *VectorIndexCache) beginCleanup() bool {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	if c.exited.Load() {
		return false
	}
	c.cleanups.Add(1)
	return true
}

// beginShutdown publishes the first failure before closing the cache signal.
func (c *VectorIndexCache) beginShutdown(err error) bool {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	if err != nil && c.shutdownErr == nil {
		c.shutdownErr = err
	}
	if c.exited.Load() {
		return false
	}
	c.exited.Store(true)
	if c.done != nil {
		close(c.done)
	}
	return true
}

func (c *VectorIndexCache) resolveShutdown(err error) error {
	if err != errCacheClosed {
		return err
	}
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	if c.shutdownErr != nil {
		return c.shutdownErr
	}
	return errCacheClosed
}

func (c *VectorIndexCache) serve() {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	if c.exited.Load() {
		return
	}
	if c.started.Load() {
		return
	}
	if c.done == nil {
		c.done = make(chan struct{})
	}

	// try clean up the temp directory. set tempdir to /tmp/hnsw
	c.ticker = time.NewTicker(c.TickerInterval)
	c.sigc = make(chan os.Signal, 3)
	signal.Notify(c.sigc, syscall.SIGTERM, syscall.SIGINT, os.Interrupt)

	// channel initizalized.  set started to true
	c.started.Store(true)

	go func() {
		defer c.ticker.Stop()
		defer signal.Stop(c.sigc)
		for {
			select {
			case <-c.done:
				return
			case <-c.sigc:
				// sig can be syscall.SIGTERM or syscall.SIGINT
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

func (c *VectorIndexCache) evictEntry(key string, expected *VectorIndexSearch, reason string) bool {
	if !c.beginCleanup() {
		return false
	}
	defer c.cleanups.Done()
	return c.evictEntryOwned(key, expected, reason)
}

// evictEntryOwned runs under a registered cleanup or the shutdown sweep.
func (c *VectorIndexCache) evictEntryOwned(key string, expected *VectorIndexSearch, reason string) bool {
	value, loaded := c.IndexMap.Load(key)
	if !loaded {
		return false
	}
	algo, ok := value.(*VectorIndexSearch)
	if !ok || (expected != nil && algo != expected) || !algo.beginEviction(reason == evictionTTL) {
		return false
	}
	value, loaded = c.IndexMap.Load(key)
	if !loaded || value != algo {
		return false
	}
	// Publish the invalidation while the old entry still occupies the key. A
	// replacement can only be inserted after CompareAndDelete, so it cannot
	// observe a later generation bump from the old entry's blocked destroy.
	invalidationErr := algo.notifyCacheInvalidated(reason)
	if invalidationErr != nil {
		// Waiters can fail immediately while recovery fences the old entry.
		algo.invalidationResult.Store(&entryLoadResult{err: invalidationErr})
		algo.markEvictionResultReady()
		if (reason == evictionTTL || reason == evictionGeneration) && !c.exited.Load() {
			if _, ok := algo.Algo.(cacheInvalidationRecoverer); ok && c.beginCleanup() {
				// A slow reader/load must not stall housekeeping for other keys.
				// The registered cleanup retains this key until repair and destroy.
				go func() {
					defer c.cleanups.Done()
					defer func() {
						if p := recover(); p != nil {
							logutil.Errorf("[veccache] index invalidation cleanup panicked: %v", p)
							c.failInvalidation(invalidationErr)
							if value, ok := c.IndexMap.Load(key); ok && value == algo {
								c.finishEviction(key, algo, invalidationErr)
							}
						}
					}()
					c.recoverEviction(key, algo, reason, invalidationErr)
				}()
				return false
			}
		}
		c.failInvalidation(invalidationErr)
	}
	return c.finishEviction(key, algo, invalidationErr)
}

// recoverEviction retains the cleanup registration and key reservation while
// waiting for old users to leave. Shutdown joins it before lifecycle hooks.
func (c *VectorIndexCache) recoverEviction(key string, algo *VectorIndexSearch, reason string, invalidationErr error) {
	if c.exited.Load() {
		c.beginShutdown(invalidationErr)
		c.finishEviction(key, algo, invalidationErr)
		return
	}
	supported, err := algo.recoverInvalidation(reason)
	if err != nil {
		logutil.Errorf("[veccache] index invalidation recovery failed: %v", err)
	}
	if supported && err == nil {
		logutil.Errorf("[veccache] index invalidation failed; recovered entry: %v", invalidationErr)
	} else {
		c.failInvalidation(invalidationErr)
	}
	c.finishEviction(key, algo, invalidationErr)
}

func (c *VectorIndexCache) failInvalidation(err error) {
	logutil.Errorf("[veccache] index invalidation failed; shutting down cache: %v", err)
	if c.beginShutdown(err) {
		go c.Destroy()
	}
}

func (c *VectorIndexCache) finishEviction(key string, algo *VectorIndexSearch, invalidationErr error) bool {
	if !c.IndexMap.CompareAndDelete(key, algo) {
		if invalidationErr != nil {
			c.failInvalidation(invalidationErr)
		}
		return false
	}
	// Once detached, this owner must attempt destruction even if signaling fails.
	defer algo.Destroy()
	algo.markEvictionResultReady()
	return true
}

func (c *VectorIndexCache) discardFailedLoad(key string, algo *VectorIndexSearch) {
	if !c.beginCleanup() {
		return
	}
	defer c.cleanups.Done()
	if !algo.beginEviction(false) {
		return
	}
	if c.IndexMap.CompareAndDelete(key, algo) {
		algo.markEvictionResultReady()
		algo.destroyFailedLoad()
	}
}

// house keeping to check expired keys and delete from cache
func (c *VectorIndexCache) HouseKeeping() {
	if !c.beginCleanup() {
		return
	}
	defer c.cleanups.Done()

	// Sweep algorithm-owned idle resources before an entry's synchronous
	// destruction can block this housekeeping pass.
	if !c.exited.Load() {
		runLifecycleHooks(false)
	}
	if c.exited.Load() {
		return
	}

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
		if c.exited.Load() {
			return
		}
		if !entry.algo.Expired() && !entry.algo.stale.Load() {
			continue
		}
		reason := evictionTTL
		if entry.algo.stale.Load() {
			reason = evictionGeneration
		}
		if c.evictEntryOwned(entry.key, entry.algo, reason) {
			logutil.Debugf("[veccache] evicted expired/stale index %s from cache", entry.key)
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
	c.shutdownOnce.Do(func() {
		c.beginShutdown(nil)
		// remove all keys
		c.IndexMap.Range(func(key, value any) bool {
			if k, ok := key.(string); ok {
				c.evictEntryOwned(k, value.(*VectorIndexSearch), evictionShutdown)
			}
			return true
		})
		c.cleanups.Wait()
		runLifecycleHooks(true)
	})
}

// Search returns the result using the shared entry for this index.
func (c *VectorIndexCache) Search(sqlproc *sqlexec.SqlProcess, key string, newalgo VectorIndexSearchIf,
	query any, rt vectorindex.RuntimeConfig) (any, []float64, error) {
	return c.search(sqlproc, key, newalgo, query, rt, nil, false)
}

// SearchInto fills a caller-owned output using the same entry lifecycle as Search.
func (c *VectorIndexCache) SearchInto(sqlproc *sqlexec.SqlProcess, key string, newalgo VectorIndexSearchIf,
	query any, rt vectorindex.RuntimeConfig, out *vectorindex.SearchOutput) error {
	_, _, err := c.search(sqlproc, key, newalgo, query, rt, out, true)
	return err
}

// search limits each request to one retry. The map remains the single-flight
// owner; followers wait for an eviction result, while the creator waits for its
// algorithm's destruction before reusing it.
func (c *VectorIndexCache) search(sqlproc *sqlexec.SqlProcess, key string, newalgo VectorIndexSearchIf,
	query any, rt vectorindex.RuntimeConfig, out *vectorindex.SearchOutput, into bool) (any, []float64, error) {
	ctx := searchContext(sqlproc)
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if c.exited.Load() {
			return nil, nil, c.resolveShutdown(errCacheClosed)
		}
		value, loaded := c.IndexMap.Load(key)
		if !loaded {
			c.lifecycleMu.Lock()
			if c.exited.Load() {
				c.lifecycleMu.Unlock()
				return nil, nil, c.resolveShutdown(errCacheClosed)
			}
			if c.done == nil {
				c.done = make(chan struct{})
			}
			newEntry := newVectorIndexSearch(newalgo)
			newEntry.done = c.done
			value, loaded = c.IndexMap.LoadOrStore(key, newEntry)
			c.lifecycleMu.Unlock()
		}
		entry := value.(*VectorIndexSearch)
		var err error
		if !loaded {
			err = entry.Load(sqlproc)
			if e := entry.invalidationError(ctx); e != nil {
				return nil, nil, e
			}
			if err != nil {
				retry := errors.Is(err, errIndexDestroyed) || IsRetryableLoadError(err)
				if entry.evicting.Load() {
					if !retry {
						return nil, nil, err
					}
				} else {
					c.discardFailedLoad(key, entry)
					if !retry {
						return nil, nil, err
					}
				}
				if attempt >= 1 {
					if e := ctx.Err(); e != nil {
						return nil, nil, e
					}
					if c.exited.Load() {
						if e := entry.invalidationError(ctx); e != nil {
							return nil, nil, e
						}
						return nil, nil, c.resolveShutdown(errCacheClosed)
					}
					return nil, nil, exhaustedLoadError(err)
				}
				if e := entry.awaitDestroyed(ctx); e != nil {
					return nil, nil, c.resolveShutdown(e)
				}
			}
			if err == nil {
				if e := ctx.Err(); e != nil {
					return nil, nil, e
				}
				if c.exited.Load() {
					if e := entry.invalidationError(ctx); e != nil {
						return nil, nil, e
					}
					return nil, nil, c.resolveShutdown(errCacheClosed)
				}
			}
		}
		if err == nil {
			var keys any
			var distances []float64
			if !into {
				keys, distances, err = entry.Search(sqlproc, newalgo, query, rt)
			} else {
				err = entry.SearchInto(sqlproc, query, rt, out)
			}
			if err == nil {
				return keys, distances, nil
			}
			if !errors.Is(err, errIndexDestroyed) {
				return nil, nil, c.resolveShutdown(err)
			}
			if e := entry.invalidationError(ctx); e != nil {
				return nil, nil, e
			}
			if attempt >= 1 {
				if e := ctx.Err(); e != nil {
					return nil, nil, e
				}
				if c.exited.Load() {
					if e := entry.invalidationError(ctx); e != nil {
						return nil, nil, e
					}
					return nil, nil, c.resolveShutdown(errCacheClosed)
				}
				if result := entry.loadResult.Load(); result != nil && IsRetryableLoadError(result.err) {
					err = result.err
				}
				return nil, nil, exhaustedLoadError(err)
			}
			if !loaded {
				if e := entry.awaitDestroyed(ctx); e != nil {
					return nil, nil, c.resolveShutdown(e)
				}
			} else {
				select {
				case <-entry.evictionResultReady:
					if e := entry.invalidationError(ctx); e != nil {
						return nil, nil, e
					}
					if e := ctx.Err(); e != nil {
						return nil, nil, e
					}
				case <-entry.done:
					if e := ctx.Err(); e != nil {
						return nil, nil, e
					}
					if e := entry.invalidationError(ctx); e != nil {
						return nil, nil, e
					}
					return nil, nil, c.resolveShutdown(errCacheClosed)
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				}
			}
		}
		// A short jitter spreads retrying queries without affecting warm hits.
		timer := time.NewTimer(time.Duration(10+rand.IntN(11)) * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, nil, ctx.Err()
		case <-entry.done:
			timer.Stop()
			if e := ctx.Err(); e != nil {
				return nil, nil, e
			}
			if e := entry.invalidationError(ctx); e != nil {
				return nil, nil, e
			}
			return nil, nil, c.resolveShutdown(errCacheClosed)
		}
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
// If a recoverable invalidation callback panics, the old key remains reserved
// and searches receive that error until asynchronous recovery finishes.
func (c *VectorIndexCache) RemoveWithReason(key, reason string) {
	c.evictEntry(key, nil, reason)
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
