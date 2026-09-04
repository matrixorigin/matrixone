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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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
	c.sysLimit.mu.Lock()
	c.sysLimit.value = hostCap(250)
	c.sysLimit.fetched = time.Now()
	c.sysLimit.mu.Unlock()

	c.HouseKeeping()

	require.False(t, isResident(c, keys[0]), "the coldest is reclaimed without waiting for a miss")
	require.True(t, isResident(c, keys[1]), "and the pass stops once under the cap")
}

// Each ARENA has to reach a positive cap on its own. enforce() skips an arena whose tenant and
// sys caps are both <= 0, so resolving the ceiling only when the whole PAIR is unset left
// `set global max_index_cache_size = 0` unbounded whenever the other arena happened to be set --
// and since max_gpu_index_cache_size defaults to a non-zero ceiling, that is the ordinary case.
func TestGovernorResolvesEachArenaIndependently(t *testing.T) {
	c := newBoundCache(t)

	// Host zeroed at BOTH scopes, device left at its default ceiling.
	sp := govProc(t, c, 1, caps{host: 0, device: absoluteDeviceCacheCeiling},
		caps{host: 0, device: absoluteDeviceCacheCeiling})
	_, sys := c.limits(sp)
	require.EqualValues(t, absoluteHostCacheCeiling, sys.host,
		"an explicit host 0 resolves to the host ceiling even though the device arena is set")
	require.EqualValues(t, absoluteDeviceCacheCeiling, sys.device,
		"and the set device arena is left alone")

	// The mirror: device zeroed, host set.
	sp = govProc(t, c, 1, caps{host: absoluteHostCacheCeiling, device: 0},
		caps{host: absoluteHostCacheCeiling, device: 0})
	_, sys = c.limits(sp)
	require.EqualValues(t, absoluteDeviceCacheCeiling, sys.device,
		"an explicit device 0 resolves to the device ceiling")
	require.EqualValues(t, absoluteHostCacheCeiling, sys.host)

	// An operator-chosen value is never overwritten by a ceiling.
	sp = govProc(t, c, 1, caps{}, caps{host: 4096, device: 8192})
	_, sys = c.limits(sp)
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

	require.EqualValues(t, int64(1<<30), c.accountCacheLimit(sp, owner).host,
		"the first read memoizes the tenant's real cap")

	// Expire the memo, then fail the refresh.
	prev, _ := c.acctLimits.Load(owner)
	c.acctLimits.Store(owner, acctLimitEntry{
		value:   prev.(acctLimitEntry).value,
		fetched: time.Now().Add(-2 * sysLimitTTL),
	})
	fail = true

	require.EqualValues(t, int64(1<<30), c.accountCacheLimit(sp, owner).host,
		"a failed refresh keeps the cap rather than falling open to unlimited")

	v, ok := c.acctLimits.Load(owner)
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

	c.sysLimit.mu.Lock()
	c.sysLimit.value = hostCap(250)
	c.sysLimit.fetched = time.Now()
	c.sysLimit.mu.Unlock()

	c.HouseKeeping()

	require.False(t, isResident(c, coldTenant), "the coldest entry is reclaimed, whoever owns it")
	require.True(t, isResident(c, warmSys), "and the warm account-0 entry is not sacrificed for it")
}
