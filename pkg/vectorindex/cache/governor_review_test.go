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
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// A cross-account snapshot read EXECUTES as the snapshot's tenant, so the resident bytes belong
// to that tenant's budget -- not to the caller's. Charging the caller lets a SYS session read
// tenant data whose residency the tenant's own cap never governs.
func TestGovernorChargesTheSnapshotOwnerNotTheCaller(t *testing.T) {
	c := newBoundCache(t)

	// Caller is account 1; the snapshot it reads belongs to account 42.
	sp := govProc(t, c, 1, caps{}, caps{})
	const owner = uint32(42)
	sp.WithExecutionIdentity(owner, "")

	key := "__mo_index_secondary_snapshot_owned"
	loadInto(t, c, sp, key, 100, 0)

	require.EqualValues(t, owner, entryOf(t, c, key).accountID.Load(),
		"the entry belongs to the account the read executed as")
}

// Eviction destroys synchronously under the entry's write lock, so taking a victim with a search
// in flight parks the cache MISS behind that search. An idle victim holding the same bytes must
// be preferred.
func TestGovernorPrefersAnIdleVictimOverABusyOne(t *testing.T) {
	c := newBoundCache(t)
	sp := govProc(t, c, 1, hostCap(250), caps{})

	busy := "__mo_index_secondary_busy"
	idle := "__mo_index_secondary_idle"
	loadInto(t, c, sp, busy, 100, 0)
	loadInto(t, c, sp, idle, 100, 0)

	// The busy one is COLDEST, so coldest-first would take it.
	entryOf(t, c, busy).ExpireAt.Store(1)
	entryOf(t, c, idle).ExpireAt.Store(2)

	// Hold a read lock on the cold victim, as an in-flight search does.
	held := entryOf(t, c, busy)
	held.Mutex.RLock()
	defer held.Mutex.RUnlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		loadInto(t, c, sp, "__mo_index_secondary_newcomer", 100, 0)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the load blocked behind the busy victim instead of taking the idle one")
	}

	require.True(t, isResident(c, busy), "the busy victim is skipped, not waited on")
	require.False(t, isResident(c, idle), "the idle victim of equal size is taken instead")
}

// Per-victim eviction logging is DEBUG; the counters are what an operator watches. Two indexes
// alternating under a tight cap evict on every miss, so one INFO line per victim is a log storm.
func TestGovernorEvictionCounters(t *testing.T) {
	c := newBoundCache(t)
	sp := govProc(t, c, 1, hostCap(250), caps{})

	entries, bytes := c.EvictionStats()
	require.Zero(t, entries)
	require.Zero(t, bytes)

	loadInto(t, c, sp, "__mo_index_secondary_v1", 200, 0)
	entryOf(t, c, "__mo_index_secondary_v1").ExpireAt.Store(1)
	loadInto(t, c, sp, "__mo_index_secondary_v2", 200, 0)

	entries, bytes = c.EvictionStats()
	require.EqualValues(t, 1, entries, "one victim reclaimed")
	require.EqualValues(t, 200, bytes)
}

// A lowered cap must take effect on a merely-warm cache. Enforcement used to run only on a
// miss, so a hot working set renewing its TTL never shrank until traffic happened to miss.
func TestGovernorHousekeepingAppliesALoweredCap(t *testing.T) {
	c := newBoundCache(t)
	sp := govProc(t, c, 1, caps{}, caps{})

	// Ordered but NOT expired: ExpireAt is compared against time.Now().UnixMicro(), and an
	// expired entry would be reclaimed by the TTL sweep instead of by the cap, which is the
	// thing under test.
	future := time.Now().Add(time.Hour).UnixMicro()
	keys := []string{"__mo_index_secondary_h1", "__mo_index_secondary_h2"}
	for i, k := range keys {
		loadInto(t, c, sp, k, 200, 0)
		entryOf(t, c, k).ExpireAt.Store(future + int64(i))
	}
	for _, k := range keys {
		require.True(t, isResident(c, k), "nothing configured yet, so nothing is evicted")
	}

	// The operator lowers the CN-wide cap; the memo is what housekeeping can see.
	c.gov().sysLimit.mu.Lock()
	c.gov().sysLimit.value = hostCap(250)
	c.gov().sysLimit.fetched = time.Now()
	c.gov().sysLimit.mu.Unlock()

	houseKeepingSync(t, c)

	require.False(t, isResident(c, keys[0]), "the coldest is reclaimed without waiting for a miss")
	require.True(t, isResident(c, keys[1]), "and the pass stops once under the cap")
}

// A warm cache must refresh the SYS catalog row itself. Merely copying a value
// that a previous miss happened to memoize does not observe SET GLOBAL after
// the memo TTL expires.
func TestGovernorHousekeepingRefreshesWarmSysCap(t *testing.T) {
	c := newBoundCache(t)
	mp := mpool.MustNewZero()
	capBytes := "500"
	withSysSql(t, c, func(_ context.Context, _ string, account uint32, _ string, _ string) (executor.Result, error) {
		if account != catalog.System_Account {
			return varRows(t, mp), nil
		}
		return varRows(t, mp, maxIndexCacheSizeVar, capBytes), nil
	})

	sp := sysProc()
	first := "__mo_index_secondary_refresh_a"
	second := "__mo_index_secondary_refresh_b"
	loadInto(t, c, sp, first, 200, 0)
	loadInto(t, c, sp, second, 200, 0)
	entryOf(t, c, first).ExpireAt.Store(time.Now().Add(time.Minute).UnixMicro())
	entryOf(t, c, second).ExpireAt.Store(time.Now().Add(2 * time.Minute).UnixMicro())

	capBytes = "250"
	c.gov().sysLimit.mu.Lock()
	c.gov().sysLimit.fetched = time.Now().Add(-2 * sysLimitTTL)
	c.gov().sysLimit.mu.Unlock()
	houseKeepingSync(t, c)

	require.False(t, isResident(c, first), "housekeeping refreshes and applies the lowered SYS cap")
	require.True(t, isResident(c, second), "the colder entry is reclaimed first")
}

// Tenant caps have the same warm-cache obligation as SYS caps. A miss records
// the account/service pair; housekeeping must refresh that catalog row and
// apply the lower value without waiting for a new key.
func TestGovernorHousekeepingRefreshesWarmTenantCap(t *testing.T) {
	c := newBoundCache(t)
	mp := mpool.MustNewZero()
	tenantCap := "500"
	sp := govProc(t, c, 7, hostCap(500), caps{})
	withSysSql(t, c, func(_ context.Context, _ string, account uint32, _ string, _ string) (executor.Result, error) {
		switch account {
		case catalog.System_Account:
			return varRows(t, mp), nil
		case 7:
			return varRows(t, mp, maxIndexCacheSizeVar, tenantCap), nil
		default:
			return varRows(t, mp), nil
		}
	})

	first := "__mo_index_secondary_tenant_refresh_a"
	second := "__mo_index_secondary_tenant_refresh_b"
	loadInto(t, c, sp, first, 200, 0)
	loadInto(t, c, sp, second, 200, 0)
	entryOf(t, c, first).ExpireAt.Store(time.Now().Add(time.Minute).UnixMicro())
	entryOf(t, c, second).ExpireAt.Store(time.Now().Add(2 * time.Minute).UnixMicro())

	tenantCap = "250"
	v, ok := c.gov().acctLimits.Load(uint32(7))
	require.True(t, ok)
	e := v.(acctLimitEntry)
	e.fetched = time.Now().Add(-2 * sysLimitTTL)
	c.gov().acctLimits.Store(uint32(7), e)
	houseKeepingSync(t, c)

	require.False(t, isResident(c, first), "housekeeping refreshes and applies the lowered tenant cap")
	require.True(t, isResident(c, second), "the colder tenant entry is reclaimed first")
}

// Each ARENA has to reach a positive cap on its own. enforce() skips an arena whose tenant and
// sys caps are both <= 0, so resolving the ceiling only when the whole PAIR is unset left
// `set global max_index_cache_size = 0` unbounded whenever the other arena happened to be set --
// and since max_gpu_index_cache_size defaults to a non-zero ceiling, that is the ordinary case.
func TestGovernorResolvesEachArenaIndependently(t *testing.T) {
	c := newBoundCache(t)

	auto, aerr, aerr2 := c.gov().defaultLimits()
	require.NoError(t, aerr2)
	require.NoError(t, aerr)

	// Host zeroed at BOTH scopes, device set. Each arena resolves on its own.
	sp := govProc(t, c, 1, caps{host: 0, device: auto.device},
		caps{host: 0, device: auto.device})
	_, sys, serrs := c.gov().limits(sp)
	require.NoError(t, serrs.host)
	require.NoError(t, serrs.device)
	require.EqualValues(t, auto.host, sys.host,
		"an explicit host 0 resolves to the automatic host budget even though device is set")
	require.EqualValues(t, auto.device, sys.device, "and the set device arena is left alone")

	// The mirror: device zeroed, host set.
	sp = govProc(t, c, 1, caps{host: auto.host, device: 0}, caps{host: auto.host, device: 0})
	_, sys, serrs = c.gov().limits(sp)
	require.NoError(t, serrs.host)
	require.NoError(t, serrs.device)
	require.EqualValues(t, auto.device, sys.device,
		"an explicit device 0 resolves to the automatic device budget")
	require.EqualValues(t, auto.host, sys.host)

	// An operator-chosen value is never overwritten by a ceiling.
	sp = govProc(t, c, 1, caps{}, caps{host: 4096, device: 8192})
	_, sys, serrs = c.gov().limits(sp)
	require.NoError(t, serrs.host)
	require.NoError(t, serrs.device)
	require.EqualValues(t, 4096, sys.host, "a real cap survives the resolution")
	require.EqualValues(t, 8192, sys.device)
}

// A cross-account cap read that fails must keep the last known good value. Returning empty caps
// would let a transient catalog error unbound a tenant that HAS a cap -- and for as long as the
// catalog stayed unreachable, since every TTL window would re-stamp the zero. sysCacheLimit has
// held this invariant since it was written; accountCacheLimit has to hold it too.
func TestGovernorAccountCapReadFailureKeepsLastKnownCap(t *testing.T) {
	c := newBoundCache(t)
	const owner = uint32(42)
	sp := sysProc()

	fail := false
	withSysSql(t, c, func(_ context.Context, _ string, _ uint32, _ string, _ string) (executor.Result, error) {
		if fail {
			return executor.Result{}, moerr.NewInternalErrorNoCtx("catalog unreachable")
		}
		return varRows(t, mpool.MustNewZero(), maxIndexCacheSizeVar, "1073741824"), nil
	})

	require.EqualValues(t, int64(1<<30), c.gov().accountCacheLimit(sp, owner).host,
		"the first read memoizes the tenant's real cap")

	// Expire the memo, then fail the refresh.
	prev, _ := c.gov().acctLimits.Load(owner)
	c.gov().acctLimits.Store(owner, acctLimitEntry{
		value:   prev.(acctLimitEntry).value,
		fetched: time.Now().Add(-2 * sysLimitTTL),
	})
	fail = true

	require.EqualValues(t, int64(1<<30), c.gov().accountCacheLimit(sp, owner).host,
		"a failed refresh keeps the cap rather than falling open to unlimited")

	v, ok := c.gov().acctLimits.Load(owner)
	require.True(t, ok)
	require.EqualValues(t, int64(1<<30), v.(acctLimitEntry).value.host,
		"and the memo still carries it, so the next window does not re-stamp a zero")
}

// Housekeeping has no asking account, so no account may be made to pay first: the pass must
// reclaim strictly coldest-first across the whole cache. Charging account 0 made a sys-session
// or idxcron entry the only thing a binding CN-wide cap ever evicted, however warm.
func TestGovernorHousekeepingEvictsColdestNotTheSysAccount(t *testing.T) {
	c := newBoundCache(t)

	// Warm entry owned by account 0; far colder entry owned by account 1.
	warmSys := "__mo_index_secondary_sys_warm"
	coldTenant := "__mo_index_secondary_tenant_cold"
	loadInto(t, c, govProc(t, c, 0, caps{}, caps{}), warmSys, 200, 0)
	loadInto(t, c, govProc(t, c, 1, caps{}, caps{}), coldTenant, 200, 0)

	now := time.Now()
	entryOf(t, c, warmSys).ExpireAt.Store(now.Add(time.Hour).UnixMicro())
	entryOf(t, c, coldTenant).ExpireAt.Store(now.Add(time.Minute).UnixMicro())

	c.gov().sysLimit.mu.Lock()
	c.gov().sysLimit.value = hostCap(250)
	c.gov().sysLimit.fetched = time.Now()
	c.gov().sysLimit.mu.Unlock()

	houseKeepingSync(t, c)

	require.False(t, isResident(c, coldTenant), "the coldest entry is reclaimed, whoever owns it")
	require.True(t, isResident(c, warmSys), "and the warm account-0 entry is not sacrificed for it")
}

// destroyedAlgo always reports the entry as gone, which is what an evicted generation looks
// like to a searcher that loaded the map value just before the claim.
type destroyedAlgo struct{ countingSearch }

func (d *destroyedAlgo) Search(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig) (any, []float64, error) {
	return nil, nil, moerr.NewInvalidStateNoCtx("Index destroyed")
}

func (d *destroyedAlgo) SearchInto(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, *vectorindex.SearchOutput) error {
	return moerr.NewInvalidStateNoCtx("Index destroyed")
}

func cancellableProc(ctx context.Context) *sqlexec.SqlProcess {
	return &sqlexec.SqlProcess{SqlCtx: &sqlexec.SqlContext{
		Ctx: ctx, CNUuid: "gov-test-cn", AccountId: 1,
		ResolveVariableFunc: func(string, bool, bool) (interface{}, error) { return int64(0), nil },
	}}
}

// A reader that finds an entry mid-eviction must not inherit an UNRELATED goroutine's runtime.
// awaitDestroyed waited on the teardown unconditionally, and Destroy takes the write lock -- so
// the caller was parked behind another query's in-flight search for an entry it shares nothing
// with, unable to honour its own cancellation. Here the entry stays resident and claimed with a
// read lock held, so nothing will ever close `destroyed`; the caller must still return.
func TestSearchCancelledReaderDoesNotWaitForAnUnrelatedReader(t *testing.T) {
	c := newBoundCache(t)
	key := "__mo_index_secondary_evicting"

	victim := newVectorIndexSearch(&destroyedAlgo{})
	c.IndexMap.Store(key, victim)
	victim.Status.Store(STATUS_LOADED)
	require.True(t, victim.beginEviction(false), "claim the entry, as an evictor would")

	victim.Mutex.RLock()
	defer victim.Mutex.RUnlock()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() {
		_, _, err := c.Search(cancellableProc(ctx), key, &countingSearch{}, nil, vectorindex.RuntimeConfig{})
		done <- err
	}()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled,
			"the cancelled caller returns its own error, not a stranger's teardown")
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled request is stuck waiting for the old entry's unrelated reader")
	}
}

// The same for the box-free twin, which carries its own copy of the retry loop.
func TestSearchIntoCancelledReaderDoesNotWaitForAnUnrelatedReader(t *testing.T) {
	c := newBoundCache(t)
	key := "__mo_index_secondary_evicting_into"

	victim := newVectorIndexSearch(&destroyedAlgo{})
	c.IndexMap.Store(key, victim)
	victim.Status.Store(STATUS_LOADED)
	require.True(t, victim.beginEviction(false))

	victim.Mutex.RLock()
	defer victim.Mutex.RUnlock()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() {
		var out vectorindex.SearchOutput
		done <- c.SearchInto(cancellableProc(ctx), key, &countingSearch{}, nil, vectorindex.RuntimeConfig{}, &out)
	}()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled SearchInto is stuck waiting for the old entry's unrelated reader")
	}
}

// mappedHeavy is the hnsw shape: a tiny allocation and a model file that dominates it. usearch's
// memory_usage() reports ~1.2% of a viewed index, so charging only that let N named-snapshot
// generations retain N whole models while the governor saw a hundredth of them.
type mappedHeavy struct {
	countingSearch
	searching chan struct{} // closed while a search is in flight, if non-nil
	release   chan struct{}
}

func (m *mappedHeavy) Search(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig) (any, []float64, error) {
	if m.searching != nil {
		close(m.searching)
		<-m.release
	}
	return []int64{1}, []float64{1}, nil
}

// N+1 generations whose FileSize dominates cannot exceed the budget, and the one with a search
// in flight is never taken to achieve that.
func TestGovernorBoundsMappedBytesAcrossGenerations(t *testing.T) {
	c := newBoundCache(t)

	// Each generation costs 100: a tiny allocation plus a model file that is nearly all of it.
	const per, budget = int64(100), int64(250)
	sp := govProc(t, c, 1, hostCap(budget), caps{})

	// A busy generation: its search is parked, so it must never be evicted.
	busy := "__mo_index_secondary_gen_busy"
	held := &mappedHeavy{countingSearch: countingSearch{host: per},
		searching: make(chan struct{}), release: make(chan struct{})}
	go func() { _, _, _ = c.Search(sp, busy, held, nil, vectorindex.RuntimeConfig{}) }()
	<-held.searching
	defer close(held.release)

	// Fill with idle generations, then keep adding: the budget must hold across all of them.
	admitted := 0
	for i := 0; i < 6; i++ {
		key := fmt.Sprintf("__mo_index_secondary_gen%d", i)
		_, _, err := c.Search(sp, key, &countingSearch{host: per}, nil, vectorindex.RuntimeConfig{})
		if err == nil {
			admitted++
			entryOf(t, c, key).ExpireAt.Store(int64(i + 1)) // idle and cold
		}
		_, _, total := c.gov().snapshotResidents("")
		require.LessOrEqual(t, total.host, budget,
			"resident mapped bytes must never exceed the budget, however many generations arrive")
	}

	require.Positive(t, admitted, "generations that fit are still admitted")
	require.True(t, isResident(c, busy), "the generation with a search in flight is never a victim")
}

// A sizing failure must not refuse loads it has no bearing on.
//
// Two independent bugs met here. limits() returned the defaultLimits error BEFORE testing
// whether the operator had already set the caps, so the remedy the error names ("set
// max_index_cache_size") did not work -- and because the probe result is memoized on a
// process-global cache, the CN stayed bricked until restart. And the two arenas' errors were
// joined, so a GPU that could not be queried refused every hnsw and fulltext2 load on a CN
// whose RAM was perfectly well known.
func TestGovernorSizingFailureDoesNotRefuseConfiguredOrUnrelatedArenas(t *testing.T) {
	t.Run("a configured cap does not consult the failed probe", func(t *testing.T) {
		c := newBoundCache(t)
		sp := govProc(t, c, 1, caps{}, caps{host: 1 << 30, device: 1 << 30})
		_, sys, serrs := c.gov().limits(sp)
		require.NoError(t, serrs.host, "both arenas are configured, so nothing needs deriving")
		require.EqualValues(t, 1<<30, sys.host)
		require.EqualValues(t, 1<<30, sys.device)
	})

	t.Run("a device probe failure does not refuse a host-only load", func(t *testing.T) {
		c := newBoundCache(t)
		// Only the DEVICE probe failed; host sizing is fine. Crucially the GPU cap is NOT
		// configured -- that is the state a GPU build with a failing CUDA query is actually in,
		// and preconfiguring it would mean the poisoned probe is never consulted at all.
		c.gov().defaultLimitMu.Lock()
		c.gov().defaultLimitDeviceErr = moerr.NewInternalErrorNoCtx("device probe failed")
		c.gov().defaultLimitDeviceReady = true
		c.gov().defaultLimitMu.Unlock()

		sp := govProc(t, c, 1, caps{}, caps{}) // nothing configured, either arena
		_, sys, serrs := c.gov().limits(sp)
		require.NoError(t, serrs.host, "the host arena derived fine")
		require.Error(t, serrs.device, "and the device failure is reported, not hidden")
		auto, hostErr, _ := c.gov().defaultLimits()
		require.NoError(t, hostErr)
		require.EqualValues(t, auto.host, sys.host)

		// The load is host-only, so the device failure is none of its business.
		_, _, err := c.Search(sp, "__mo_index_secondary_hostonly",
			&countingSearch{host: 4096}, nil, vectorindex.RuntimeConfig{})
		require.NoError(t, err, "an unqueryable GPU must not refuse a CPU index")
		require.True(t, isResident(c, "__mo_index_secondary_hostonly"))
	})

	t.Run("a device-using load still gets the device failure", func(t *testing.T) {
		c := newBoundCache(t)
		c.gov().defaultLimitMu.Lock()
		c.gov().defaultLimitDeviceErr = moerr.NewInternalErrorNoCtx("device probe failed")
		c.gov().defaultLimitDeviceReady = true
		c.gov().defaultLimitMu.Unlock()
		sp := govProc(t, c, 1, caps{}, caps{})

		_, _, err := c.Search(sp, "__mo_index_secondary_gpuload",
			&countingSearch{host: 4096, device: 4096}, nil, vectorindex.RuntimeConfig{})
		require.Error(t, err, "this arrival occupies the arena that could not be sized")
		require.Contains(t, err.Error(), "device probe failed", "and it carries the probe's own cause")
		require.False(t, isResident(c, "__mo_index_secondary_gpuload"))
	})
}

// The refusal still has teeth: it fires when admitting WOULD hurt someone. A busy incumbent
// cannot be reclaimed (a search is in flight on it) and must not be preempted, so an arrival
// that does not fit alongside it is refused rather than served at its expense.
func TestGovernorRefusesOnlyToProtectABusyIncumbent(t *testing.T) {
	c := newBoundCache(t)
	sp := govProc(t, c, 1, hostCap(250), caps{})

	// An incumbent with a search parked mid-flight: not idle, so not reclaimable.
	busy := "__mo_index_secondary_busy_incumbent"
	held := &mappedHeavy{countingSearch: countingSearch{host: 200},
		searching: make(chan struct{}), release: make(chan struct{})}
	go func() { _, _, _ = c.Search(sp, busy, held, nil, vectorindex.RuntimeConfig{}) }()
	<-held.searching
	defer close(held.release)

	// 200+200 exceeds 250 and the incumbent is busy, so the arrival is refused.
	loadRefused(t, c, sp, "__mo_index_secondary_arrival", 200, 0)
	require.True(t, isResident(c, busy), "the live search is never sacrificed for an arrival")
}

// permanentInvalidState is a RESIDENT algorithm whose backend keeps failing with the public
// ErrInvalidState code -- a paused txn client, a failed remote run. It is not eviction, and it
// does not get better on retry.
type permanentInvalidState struct {
	countingSearch
	calls atomic.Int32
}

func (p *permanentInvalidState) Search(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig) (any, []float64, error) {
	p.calls.Add(1)
	return nil, nil, moerr.NewInvalidStateNoCtx("backend is wedged")
}

func (p *permanentInvalidState) SearchInto(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, *vectorindex.SearchOutput) error {
	p.calls.Add(1)
	return moerr.NewInvalidStateNoCtx("backend is wedged")
}

// A permanent BACKEND error must propagate on the first attempt, not spin the retry loop.
//
// The retry used to fire on the moerr.ErrInvalidState CODE, which an algorithm raises for its
// own reasons. With the entry resident (loaded), still STATUS_LOADED and still in the map,
// nothing about the next attempt differed -- so the loop re-invoked the same failing backend
// forever, burning CPU until the context died, or without end on a background context.
func TestSearchPermanentBackendErrorDoesNotRetry(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*VectorIndexCache, *sqlexec.SqlProcess, string, *permanentInvalidState) error
	}{
		{"Search", func(c *VectorIndexCache, sp *sqlexec.SqlProcess, k string, a *permanentInvalidState) error {
			_, _, err := c.Search(sp, k, a, nil, vectorindex.RuntimeConfig{})
			return err
		}},
		{"SearchInto", func(c *VectorIndexCache, sp *sqlexec.SqlProcess, k string, a *permanentInvalidState) error {
			var out vectorindex.SearchOutput
			return c.SearchInto(sp, k, a, nil, vectorindex.RuntimeConfig{}, &out)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newBoundCache(t)
			sp := govProc(t, c, 1, caps{}, caps{})
			key := "__mo_index_secondary_wedged_" + tc.name
			algo := &permanentInvalidState{}

			done := make(chan error, 1)
			go func() { done <- tc.run(c, sp, key, algo) }()

			select {
			case err := <-done:
				require.Error(t, err, "the backend error propagates")
				require.Contains(t, err.Error(), "backend is wedged")
			case <-time.After(5 * time.Second):
				t.Fatalf("permanent backend error spun the retry loop (%d backend calls)", algo.calls.Load())
			}
			require.EqualValues(t, 1, algo.calls.Load(), "the backend is invoked exactly once")
		})
	}
}

// A victim chosen as idle must STAY the caller's: claiming it and HOLDING that claim through
// the destroy is what keeps a cache miss from parking behind a search that started a moment
// after the check. The seam below lands exactly that search in the window between the two.
func TestGovernorIdleVictimIsClaimedNotJustChecked(t *testing.T) {
	c := newBoundCache(t)
	sp := govProc(t, c, 1, hostCap(250), caps{})

	victim := "__mo_index_secondary_victim"
	spare := "__mo_index_secondary_spare"
	loadInto(t, c, sp, victim, 100, 0)
	loadInto(t, c, sp, spare, 100, 0)
	entryOf(t, c, victim).ExpireAt.Store(1) // coldest, so it is chosen first
	entryOf(t, c, spare).ExpireAt.Store(2)

	release := make(chan struct{})
	defer close(release)
	var once sync.Once
	held := entryOf(t, c, victim)
	afterIdleClaim = func(key string) {
		if key != victim {
			return
		}
		once.Do(func() {
			// A search arrives the instant the victim is picked, and holds past the test's
			// patience. Under a held claim it cannot get in, so this waits out its budget
			// and the destroy proceeds; under check-then-destroy it gets the lock at once
			// and the destroy below blocks behind it.
			got := make(chan struct{})
			go func() {
				held.Mutex.RLock()
				close(got)
				<-release
				held.Mutex.RUnlock()
			}()
			select {
			case <-got:
			case <-time.After(200 * time.Millisecond):
			}
		})
	}
	t.Cleanup(func() { afterIdleClaim = nil })

	done := make(chan struct{})
	go func() {
		defer close(done)
		loadInto(t, c, sp, "__mo_index_secondary_newcomer", 100, 0)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the destroy blocked behind a search that started after the idle check")
	}

	require.False(t, isResident(c, victim), "the claimed victim is destroyed, not lost to a racing search")
	require.True(t, isResident(c, spare), "one victim was enough")
	require.True(t, isResident(c, "__mo_index_secondary_newcomer"))
}

// gatedSearch is a load the test can hold at two points: every instance waits in Preload until
// they have ALL arrived (so their admissions overlap), and then waits in Load until released
// (so an admitted one stays busy and cannot simply be evicted by the next arrival).
type gatedSearch struct {
	countingSearch
	arrived *sync.WaitGroup
	start   <-chan struct{}
	gate    <-chan struct{}
}

func (g *gatedSearch) Preload(*sqlexec.SqlProcess) error {
	g.arrived.Done()
	<-g.start
	return nil
}

func (g *gatedSearch) Load(*sqlexec.SqlProcess) error {
	g.loads.Add(1)
	<-g.gate
	return nil
}

// admitConcurrently races n cold misses for distinct keys, each costing host bytes, and reports
// how many were admitted and how many were refused as overloaded.
func admitConcurrently(t *testing.T, c *VectorIndexCache, sp *sqlexec.SqlProcess, n int, host int64) (admitted, refused int) {
	t.Helper()

	var arrived sync.WaitGroup
	arrived.Add(n)
	start := make(chan struct{})
	gate := make(chan struct{})

	var once sync.Once
	defer once.Do(func() { close(gate) })

	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("__mo_index_secondary_race_%d", i)
		algo := &gatedSearch{
			countingSearch: countingSearch{host: host},
			arrived:        &arrived,
			start:          start,
			gate:           gate,
		}
		go func() {
			_, _, err := c.Search(sp, key, algo, nil, vectorindex.RuntimeConfig{})
			errs <- err
		}()
	}

	arrived.Wait() // every arrival is measured and none has been admitted yet
	close(start)   // ... now let them all contend for the same room

	// The refusals come back while the admitted load is still parked in Load, which is the
	// point: the winner is BUSY, so a later arrival cannot evict it and admit itself instead.
	for i := 0; i < n; i++ {
		select {
		case err := <-errs:
			if err == nil {
				admitted++
				continue
			}
			require.Contains(t, err.Error(), "index cache is full",
				"a loser is refused as overloaded, not failed some other way")
			refused++
		case <-time.After(10 * time.Second):
			once.Do(func() { close(gate) })
			t.Fatal("an arrival neither loaded nor was refused")
		}
		if refused == n-1 {
			once.Do(func() { close(gate) }) // everyone else answered; release the winner
		}
	}
	return admitted, refused
}

// Concurrent cold misses must see each other. Each one alone is the arena's only occupant and
// takes the sole-occupant bypass; if that bypass is decided on residency alone, every arrival in
// a burst takes it at once and the budget admits N indexes sized for one.
func TestGovernorConcurrentColdMissesDoNotAllTakeTheSoleOccupantBypass(t *testing.T) {
	t.Run("two keys", func(t *testing.T) {
		c := newBoundCache(t)
		sp := govProc(t, c, 1, caps{}, hostCap(150))
		admitted, refused := admitConcurrently(t, c, sp, 2, 100)
		require.Equal(t, 1, admitted, "one fits under the 150 byte cap")
		require.Equal(t, 1, refused, "the second is overload, not a second sole occupant")
	})

	t.Run("eight keys", func(t *testing.T) {
		c := newBoundCache(t)
		sp := govProc(t, c, 1, caps{}, hostCap(150))
		admitted, refused := admitConcurrently(t, c, sp, 8, 100)
		require.Equal(t, 1, admitted, "the burst does not multiply the budget by its width")
		require.Equal(t, 7, refused)
	})

	t.Run("the first query is still admitted alone", func(t *testing.T) {
		c := newBoundCache(t)
		sp := govProc(t, c, 1, caps{}, hostCap(150))
		// Nothing resident, nothing in flight: an index larger than the whole budget still
		// loads. Bounding how many stay resident together must not make one unloadable.
		loadInto(t, c, sp, "__mo_index_secondary_lone", 10_000, 0)
		require.True(t, isResident(c, "__mo_index_secondary_lone"))
	})
}

// Remove is the APPEND path: every CDC/ISCP flush calls it (see each algorithm's sync.go). A
// snapshot generation is a read at a past timestamp, which new rows cannot change, so an append
// must leave it resident. Only DDL, where the index table itself goes away or is rebuilt, clears
// history.
func TestRemoveKeepsSnapshotGenerationsAndDDLClearsThem(t *testing.T) {
	c := newBoundCache(t)
	sp := govProc(t, c, 1, caps{}, caps{})

	table := "__mo_index_secondary_gen"
	snapA := SnapshotKey(table, snapshotTS(100))
	snapB := SnapshotKey(table, snapshotTS(200))
	for _, k := range []string{table, snapA, snapB} {
		loadInto(t, c, sp, k, 100, 0)
	}

	c.Remove(table) // an ordinary append
	require.False(t, isResident(c, table), "the current generation is invalidated")
	require.True(t, isResident(c, snapA), "a snapshot the append cannot have changed stays")
	require.True(t, isResident(c, snapB))

	loadInto(t, c, sp, table, 100, 0)
	c.RemoveAllGenerations(table, "ddl") // DROP INDEX
	require.False(t, isResident(c, table))
	require.False(t, isResident(c, snapA), "a dropped index keeps no history")
	require.False(t, isResident(c, snapB))
}

// A binding cap evicts on every miss, so an INFO line per pass is a log storm. INFO is
// rate-limited per arena; the counters stay exact.
func TestGovernorEvictionLoggingIsRateLimited(t *testing.T) {
	c := newBoundCache(t)

	c.gov().logReclaim(arenaHost, 100, 1, 250)
	first := c.gov().lastEvictLog[arenaHost].Load()
	require.NotZero(t, first, "the first eviction is always reported")

	c.gov().logReclaim(arenaHost, 100, 1, 250)
	require.Equal(t, first, c.gov().lastEvictLog[arenaHost].Load(),
		"a second eviction inside the interval is counted, not logged")

	c.gov().logReclaim(arenaDevice, 100, 1, 250)
	require.NotZero(t, c.gov().lastEvictLog[arenaDevice].Load(), "the other arena has its own budget")

	c.gov().lastEvictLog[arenaHost].Store(time.Now().Add(-2 * evictionLogInterval).UnixNano())
	c.gov().logReclaim(arenaHost, 100, 1, 250)
	require.Greater(t, c.gov().lastEvictLog[arenaHost].Load(), first,
		"and a still-evicting cache reports again once the interval has passed")
}

// Housekeeping must stay bounded on a CN that has served many tenants against a slow catalog.
//
// acctLimits accumulates an entry per account that has ever loaded and nothing removed them, so
// refreshing "every account we have seen" is a per-account catalog call, serially, with an
// independent timeout each -- and it ran on the lifecycle goroutine, so TTL eviction, lifecycle
// hooks and Stop/SIGTERM all queued behind it. Accounts holding nothing have nothing to enforce
// and are the ones that accumulate.
func TestGovernorHousekeepingStaysBoundedWithManyDeadTenants(t *testing.T) {
	const tenants = 40
	const catalogDelay = 50 * time.Millisecond

	c := newBoundCache(t)
	mp := mpool.MustNewZero()
	var reads atomic.Int64
	withSysSql(t, c, func(ctx context.Context, _ string, _ uint32, _, _ string) (executor.Result, error) {
		reads.Add(1)
		select {
		case <-time.After(catalogDelay):
		case <-ctx.Done():
			return executor.Result{}, ctx.Err()
		}
		return varRows(t, mp), nil
	})

	// Tenants that used this CN once and hold nothing now: expired memos, no residency.
	for i := 1; i <= tenants; i++ {
		c.gov().acctLimits.Store(uint32(i), acctLimitEntry{
			value:   hostCap(1 << 20),
			fetched: time.Now().Add(-2 * sysLimitTTL),
			service: "gov-test-cn",
		})
	}
	c.gov().sysLimit.mu.Lock()
	c.gov().sysLimit.fetched = time.Now() // SYS memo fresh, so only the tenant walk is under test
	c.gov().sysLimit.mu.Unlock()

	start := time.Now()
	c.HouseKeeping()
	dispatch := time.Since(start)
	require.Less(t, dispatch, catalogDelay,
		"HouseKeeping returned in %s: the lifecycle goroutine must not wait on the catalog", dispatch)

	require.Eventually(t, func() bool { return !c.capRefreshing.Load() }, 30*time.Second, time.Millisecond)

	require.Zero(t, reads.Load(),
		"a tenant holding nothing resident is not worth a catalog read")
	for i := 1; i <= tenants; i++ {
		_, still := c.gov().acctLimits.Load(uint32(i))
		require.False(t, still, "and its memo is pruned rather than refreshed forever")
	}
}

// The same pass with tenants that DO hold bytes reads the catalog for them, and the whole pass
// is bounded by one budget rather than by the sum of its calls.
func TestGovernorHousekeepingRefreshesOnlyResidentTenantsUnderOneBudget(t *testing.T) {
	c := newBoundCache(t)
	mp := mpool.MustNewZero()
	var reads atomic.Int64
	seen := make(chan uint32, 16)

	sp := govProc(t, c, 3, caps{}, caps{})
	withSysSql(t, c, func(_ context.Context, _ string, account uint32, _, _ string) (executor.Result, error) {
		if account != catalog.System_Account {
			reads.Add(1)
			seen <- account
		}
		return varRows(t, mp), nil
	})

	loadInto(t, c, sp, "__mo_index_secondary_resident", 100, 0)
	entryOf(t, c, "__mo_index_secondary_resident").accountID.Store(3)

	// One resident tenant, one that has gone away.
	c.gov().acctLimits.Store(uint32(3), acctLimitEntry{
		value: hostCap(1 << 20), fetched: time.Now().Add(-2 * sysLimitTTL), service: "gov-test-cn"})
	c.gov().acctLimits.Store(uint32(99), acctLimitEntry{
		value: hostCap(1 << 20), fetched: time.Now().Add(-2 * sysLimitTTL), service: "gov-test-cn"})

	houseKeepingSync(t, c)

	require.EqualValues(t, 1, reads.Load(), "exactly the resident tenant")
	require.EqualValues(t, 3, <-seen)
	_, kept := c.gov().acctLimits.Load(uint32(3))
	require.True(t, kept, "the resident tenant keeps its memo")
	_, dead := c.gov().acctLimits.Load(uint32(99))
	require.False(t, dead, "the departed one does not")
}

// A tenant's cap is memoized -- tenantCacheLimits writes the resolver's answer into acctLimits
// so a later housekeeping pass can apply it to a warm cache. That memo must never become the
// value a MISS decides on: the resolver is authoritative and current, and a stale memo would let
// a tenant keep loading against a cap it has already lowered.
func TestTenantCapOnAMissComesFromTheResolverNotTheMemo(t *testing.T) {
	c := newBoundCache(t)

	// The memo holds a LARGE stale cap, as it would right after a miss taken before the
	// operator lowered the value.
	const account = uint32(4)
	c.gov().acctLimits.Store(account, acctLimitEntry{
		value: hostCap(1 << 30), fetched: time.Now(), service: "gov-test-cn"})

	// The session resolves the CURRENT, much smaller cap.
	sp := govProc(t, c, account, hostCap(250), caps{})

	first := "__mo_index_secondary_tenant_a"
	second := "__mo_index_secondary_tenant_b"
	loadInto(t, c, sp, first, 200, 0)
	entryOf(t, c, first).accountID.Store(account)
	loadInto(t, c, sp, second, 200, 0)
	entryOf(t, c, second).accountID.Store(account)

	require.False(t, isResident(c, first),
		"400 bytes exceeds the resolver's 250 byte cap, so the first entry is reclaimed")
	require.True(t, isResident(c, second))

	// And the miss refreshed the memo, so housekeeping enforces the same number rather than
	// the one it was seeded with.
	v, ok := c.gov().acctLimits.Load(account)
	require.True(t, ok)
	require.EqualValues(t, 250, v.(acctLimitEntry).value.host,
		"the miss writes the resolver's answer back, which is what keeps the two paths agreeing")
}

// The memo's TTL therefore cannot delay a cap: the only thing that keeps it fresh is a miss, and
// a miss enforces the current value itself. So a changed tenant cap is applied at the tenant's
// next miss, or at a housekeeping tick, whichever comes first -- never later than one tick.
func TestAFreshMemoDoesNotOutrankALoweredTenantCap(t *testing.T) {
	c := newBoundCache(t)
	const account = uint32(5)

	sp := govProc(t, c, account, hostCap(250), caps{})
	loadInto(t, c, sp, "__mo_index_secondary_warm", 200, 0)
	entryOf(t, c, "__mo_index_secondary_warm").accountID.Store(account)

	// Whatever the memo says now, it says what the resolver said -- not what it held before.
	v, ok := c.gov().acctLimits.Load(account)
	require.True(t, ok, "a miss records the account so housekeeping can reach it")
	require.EqualValues(t, 250, v.(acctLimitEntry).value.host)
	require.Equal(t, "gov-test-cn", v.(acctLimitEntry).service)
	require.WithinDuration(t, time.Now(), v.(acctLimitEntry).fetched, 5*time.Second,
		"and stamps it, so the next housekeeping pass trusts it for one TTL")
}

// Housekeeping reads the catalog with the CN uuid a miss remembered. That uuid can outlive the
// service it names -- a CN shutting down, and, in a test binary, any embedded cluster torn down
// while this process-wide cache keeps ticking. moruntime.ServiceRuntime returns a nil Runtime for
// a service it does not know, so the read used to dereference nil and take the process with it,
// from a goroutine no test could recover.
//
// It must degrade to "unavailable", keeping the last known caps, exactly as a failed read does.
func TestHousekeepingSurvivesACnUuidWhoseServiceIsGone(t *testing.T) {
	c := newBoundCache(t)

	// The REAL reader, not the unit stub: the panic was inside it.
	orig := runSysSql
	runSysSql = sqlexec.RunSqlAutoCommit
	t.Cleanup(func() { runSysSql = orig })

	c.gov().sysLimit.mu.Lock()
	c.gov().sysLimit.value = hostCap(4096)
	c.gov().sysLimit.service = "a-cn-that-has-gone-away"
	c.gov().sysLimit.fetched = time.Now().Add(-2 * sysLimitTTL)
	c.gov().sysLimit.mu.Unlock()

	require.NotPanics(t, func() { houseKeepingSync(t, c) },
		"a dead service is an error, not a crash")

	c.gov().sysLimit.mu.Lock()
	kept := c.gov().sysLimit.value
	c.gov().sysLimit.mu.Unlock()
	require.EqualValues(t, 4096, kept.host,
		"and an unreachable service must not silently unbound the cache")
}

// The prune walks residency BEFORE the catalog I/O and deletes after it, and that I/O can take
// seconds. An account that cold-loads in that window writes itself a fresh memo, which a
// delete-by-key would then throw away -- leaving it resident but unknown to acctLimits, so no
// later pass refreshes or applies its cap and a reduction waits for a miss that a warm tenant
// never takes.
func TestHousekeepingDoesNotPruneAMemoWrittenDuringItsOwnPass(t *testing.T) {
	c := newBoundCache(t)
	mp := mpool.MustNewZero()

	const resident = uint32(11) // holds bytes, so it is refreshed
	const arriving = uint32(12) // absent at walk time, arrives mid-pass

	sp := govProc(t, c, resident, caps{}, caps{})
	loadInto(t, c, sp, "__mo_index_secondary_res", 100, 0)
	entryOf(t, c, "__mo_index_secondary_res").accountID.Store(resident)

	// arriving holds nothing right now, so the walk marks its memo prunable.
	c.gov().acctLimits.Store(arriving, acctLimitEntry{
		value: hostCap(1 << 30), fetched: time.Now().Add(-2 * sysLimitTTL), service: "gov-test-cn"})
	c.gov().acctLimits.Store(resident, acctLimitEntry{
		value: hostCap(1 << 20), fetched: time.Now().Add(-2 * sysLimitTTL), service: "gov-test-cn"})

	fresh := acctLimitEntry{value: hostCap(4096), fetched: time.Now(), service: "gov-test-cn"}
	withSysSql(t, c, func(_ context.Context, _ string, account uint32, _, _ string) (executor.Result, error) {
		if account == resident {
			// arriving cold-loads while the pass is inside its catalog read, exactly as
			// another goroutine's miss would, and writes itself a current memo.
			c.gov().acctLimits.Store(arriving, fresh)
		}
		return varRows(t, mp), nil
	})

	houseKeepingSync(t, c)

	got, ok := c.gov().acctLimits.Load(arriving)
	require.True(t, ok, "the memo written during the pass must survive it")
	require.Equal(t, fresh, got, "and must be the value the arrival wrote")
}

// reservationSpy records what the governor tells it is already promised.
type reservationSpy struct {
	countingSearch
	arrived *sync.WaitGroup
	start   <-chan struct{}
	gate    <-chan struct{}
	ahead   atomic.Int64
	told    atomic.Bool
}

func (r *reservationSpy) SetReservedAhead(n int64) {
	r.ahead.Store(n)
	r.told.Store(true)
}

func (r *reservationSpy) Preload(*sqlexec.SqlProcess) error {
	r.arrived.Done()
	<-r.start
	return nil
}

func (r *reservationSpy) Load(*sqlexec.SqlProcess) error {
	r.loads.Add(1)
	<-r.gate
	return nil
}

// An algorithm's own load-time memory gate cannot see a load that has been admitted but has not
// allocated yet, so two concurrent loads hand each other the same free bytes. The governor knows
// -- it ordered the arrivals -- and now tells each one what the arrivals AHEAD of it promised.
// This is what closes the race under a budget that does not bind, which is the default.
func TestGovernorTellsEachArrivalWhatIsPromisedAhead(t *testing.T) {
	c := newBoundCache(t)
	sp := govProc(t, c, 1, caps{}, caps{}) // no operator cap: the derived budget, which is huge

	base := "__mo_index_secondary_promised"
	keys := []string{SnapshotKey(base, snapshotTS(100)), SnapshotKey(base, snapshotTS(200))}

	var arrived sync.WaitGroup
	arrived.Add(len(keys))
	start := make(chan struct{})
	gate := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(gate) })

	const tail = 24 << 20
	spies := make([]*reservationSpy, 0, len(keys))
	for _, key := range keys {
		spy := &reservationSpy{
			countingSearch: countingSearch{host: tail},
			arrived:        &arrived, start: start, gate: gate,
		}
		spies = append(spies, spy)
		go func() { _, _, _ = c.Search(sp, key, spy, nil, vectorindex.RuntimeConfig{}) }()
	}
	arrived.Wait()
	close(start)

	require.Eventually(t, func() bool {
		return spies[0].told.Load() && spies[1].told.Load()
	}, 10*time.Second, time.Millisecond, "both arrivals must be told before they load")

	// Exactly one is first in line and owes nothing; the other is told the first one's bytes.
	first, second := spies[0].ahead.Load(), spies[1].ahead.Load()
	if first > second {
		first, second = second, first
	}
	require.EqualValues(t, 0, first, "the arrival at the head of the line owes nothing")
	require.EqualValues(t, tail, second,
		"the one behind it is told exactly what the first promised, so its own gate can refuse")

	once.Do(func() { close(gate) })
}

// placedSearch reports its device bytes BY CARD, the way cagra/ivfpq do once they know their
// participants and distribution mode.
type placedSearch struct {
	countingSearch
	perCard map[int]int64
}

func (p *placedSearch) DeviceResidency() map[int]int64 { return p.perCard }

// onDevice builds a SINGLE_GPU-shaped index: all of its device bytes on one card.
func onDevice(device int, bytes int64) *placedSearch {
	return &placedSearch{
		countingSearch: countingSearch{device: bytes},
		perCard:        map[int]int64{device: bytes},
	}
}

// twoCards fabricates two 8 GiB GPUs. No CUDA involved: the capacity probe takes the device
// count and per-device memory as functions precisely so placement can be tested without them.
func twoCards(t *testing.T, c *VectorIndexCache) {
	t.Helper()
	perCard, err := automaticDeviceCapacityPerCard(
		func() (int, error) { return 2, nil },
		func(int) (uint64, error) { return 8 << 30, nil })
	require.NoError(t, err)
	aggregate, err := automaticDeviceCapacity(
		func() (int, error) { return 2, nil },
		func(int) (uint64, error) { return 8 << 30, nil })
	require.NoError(t, err)

	g := c.gov()
	g.defaultLimitMu.Lock()
	defer g.defaultLimitMu.Unlock()
	g.defaultLimit.device = aggregate
	g.defaultLimitPerCard = perCard
	g.defaultLimitDeviceReady = true
}

// A budget that sums every card cannot govern a placement that uses one. With two 8 GiB cards
// the aggregate is 14.4 GiB, so an idle 5 GiB entry on GPU0 plus a 5 GiB arrival for GPU0 is
// under it and the governor evicts nothing -- and the load then fails its own free-VRAM gate,
// because GPU0 lacks the room and GPU1's capacity is no use to an index pinned to devices[0].
func TestGovernorEvictsOnTheCardTheArrivalNeeds(t *testing.T) {
	c := newBoundCache(t)
	twoCards(t, c)
	sp := govProc(t, c, 1, caps{}, caps{})

	const five = int64(5) << 30
	idle := "__mo_index_secondary_gpu0_idle"
	_, _, err := c.Search(sp, idle, onDevice(0, five), nil, vectorindex.RuntimeConfig{})
	require.NoError(t, err)
	require.True(t, isResident(c, idle))

	// 5 + 5 GiB on GPU0 exceeds that card's 7.2 GiB budget, though not the 14.4 GiB aggregate.
	_, _, err = c.Search(sp, "__mo_index_secondary_gpu0_new", onDevice(0, five), nil, vectorindex.RuntimeConfig{})
	require.NoError(t, err, "evicting the idle GPU0 entry admits it")
	require.False(t, isResident(c, idle), "the victim is the one on the pressured card")
	require.True(t, isResident(c, "__mo_index_secondary_gpu0_new"))
}

// And an entry on ANOTHER card is not a victim: evicting it frees nothing where the pressure is.
func TestGovernorDoesNotEvictAcrossCards(t *testing.T) {
	c := newBoundCache(t)
	twoCards(t, c)
	sp := govProc(t, c, 1, caps{}, caps{})

	const five = int64(5) << 30
	other := "__mo_index_secondary_gpu1_idle"
	_, _, err := c.Search(sp, other, onDevice(1, five), nil, vectorindex.RuntimeConfig{})
	require.NoError(t, err)

	// GPU0 is empty, so this arrival needs no eviction at all.
	_, _, err = c.Search(sp, "__mo_index_secondary_gpu0_new", onDevice(0, five), nil, vectorindex.RuntimeConfig{})
	require.NoError(t, err)
	require.True(t, isResident(c, other), "an index on GPU1 is untouched by pressure on GPU0")

	// A second GPU1 arrival, though, must take the GPU1 incumbent.
	_, _, err = c.Search(sp, "__mo_index_secondary_gpu1_new", onDevice(1, five), nil, vectorindex.RuntimeConfig{})
	require.NoError(t, err)
	require.False(t, isResident(c, other))
}

// An index too large for the card it is pinned to is still admitted when it is that card's only
// occupant -- the same rule the arena applies. Refusing would make it permanently unloadable.
func TestGovernorAdmitsALoneIndexTooBigForItsCard(t *testing.T) {
	c := newBoundCache(t)
	twoCards(t, c)
	sp := govProc(t, c, 1, caps{}, caps{})

	huge := int64(20) << 30 // well past one card's 7.2 GiB budget
	_, _, err := c.Search(sp, "__mo_index_secondary_huge", onDevice(0, huge), nil, vectorindex.RuntimeConfig{})
	require.NoError(t, err, "nobody to protect on that card, so nothing to refuse for")
	require.True(t, isResident(c, "__mo_index_secondary_huge"))
}
