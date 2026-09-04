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

// Tests for the max_index_cache_size byte governor: what a loaded entry is charged, whose
// budget it lands in, and which entry is reclaimed when a budget is exceeded.

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

// govProc returns a SqlProcess for account with tenantLimit as its caps, and stubs the SYS
// catalog read to report sysLimit. A 0 in either arena means unset (unlimited).
func govProc(t *testing.T, c *VectorIndexCache, account uint32, tenantLimit, sysLimit caps) *sqlexec.SqlProcess {
	t.Helper()
	stubSysLimit(t, c, sysLimit)
	return &sqlexec.SqlProcess{SqlCtx: &sqlexec.SqlContext{
		Ctx:       context.Background(),
		CNUuid:    "gov-test-cn",
		AccountId: account,
		ResolveVariableFunc: func(name string, _, isGlobal bool) (interface{}, error) {
			require.True(t, isGlobal, "the caps are global-scope and must be read as such")
			switch name {
			case maxIndexCacheSizeVar:
				return tenantLimit.host, nil
			case maxGpuIndexCacheSizeVar:
				return tenantLimit.device, nil
			}
			require.Fail(t, "unexpected variable", name)
			return nil, nil
		},
	}}
}

// hostCap and gpuCap name a one-arena budget at the call sites, so a bare pair of numbers never
// has to be read positionally.
func hostCap(n int64) caps { return caps{host: n} }
func gpuCap(n int64) caps  { return caps{device: n} }

// stubSysLimit replaces the SYS catalog read for the duration of the test and clears the
// memoized value so each test starts from a cold read.
func stubSysLimit(t *testing.T, c *VectorIndexCache, value caps) {
	t.Helper()
	orig := runSysSql
	runSysSql = func(context.Context, string, uint32, string, string) (executor.Result, error) {
		return executor.Result{}, moerr.NewInternalErrorNoCtx("no catalog in unit test")
	}
	t.Cleanup(func() { runSysSql = orig })
	c.sysLimit.value, c.sysLimit.fetched = value, time.Now()
}

// loadInto puts one sized entry into the cache under key, as account would have loaded it.
func loadInto(t *testing.T, c *VectorIndexCache, sp *sqlexec.SqlProcess, key string, host, device int64) *countingSearch {
	t.Helper()
	algo := &countingSearch{host: host, device: device}
	_, _, err := c.Search(sp, key, algo, nil, vectorindex.RuntimeConfig{})
	require.NoError(t, err)
	return algo
}

func isResident(c *VectorIndexCache, key string) bool {
	_, ok := c.IndexMap.Load(key)
	return ok
}

// A successful load is charged to the entry, split by arena, and attributed to its tenant.
func TestGovernorChargesLoadedEntry(t *testing.T) {
	c := newBoundCache(t)
	sp := govProc(t, c, 7, caps{}, caps{})

	loadInto(t, c, sp, "__mo_index_secondary_charge", 300, 40)

	value, ok := c.IndexMap.Load("__mo_index_secondary_charge")
	require.True(t, ok)
	entry := value.(*VectorIndexSearch)
	require.EqualValues(t, 300, entry.hostBytes.Load())
	require.EqualValues(t, 40, entry.deviceBytes.Load(), "device bytes must not be folded into host")
	require.EqualValues(t, 7, entry.accountID.Load())
}

// Nothing configured falls back to absoluteCacheCeiling, not to "unlimited". At realistic
// sizes the ceiling does not bind, so an unconfigured deployment still evicts nothing --
// but the entries are CHARGED and enumerable rather than invisible, which is what makes a
// later SET GLOBAL govern an already-warm cache instead of switching accounting on.
func TestGovernorUnsetCapChargesButDoesNotEvict(t *testing.T) {
	c := newBoundCache(t)
	sp := govProc(t, c, 1, caps{}, caps{})

	keys := []string{"__mo_index_secondary_a", "__mo_index_secondary_b", "__mo_index_secondary_c"}
	for _, k := range keys {
		loadInto(t, c, sp, k, 1, 0)
	}
	for _, k := range keys {
		require.True(t, isResident(c, k), "%q must survive: three bytes fit the automatic budget", k)
		require.EqualValues(t, 1, entryOf(t, c, k).hostBytes.Load(),
			"%q is charged even with nothing configured", k)
	}

	// And the governor sees them: snapshotResidents is what every eviction pass walks.
	list, perAccount, total := c.snapshotResidents("")
	require.Len(t, list, len(keys), "an unconfigured cache is still enumerated")
	require.EqualValues(t, len(keys), total.host)
	require.EqualValues(t, len(keys), perAccount[1].host)
}

// The fallback is a real, finite cap: an entry above the ceiling is still admitted (the
// governor never fails a query on an accounting rule) but the pass runs rather than being
// skipped, so colder entries are reclaimed.
func TestGovernorAbsoluteCeilingIsFiniteNotUnlimited(t *testing.T) {
	c := newBoundCache(t)
	sp := govProc(t, c, 1, caps{}, caps{})

	tenant, sys := c.limits(sp)
	require.True(t, tenant.unset(), "nothing configured for the tenant")
	require.False(t, sys.unset(), "but the CN-wide fallback is set, not unlimited")
	require.EqualValues(t, c.defaultLimits().host, sys.host)
	require.EqualValues(t, c.defaultLimits().device, sys.device)
	require.Less(t, absoluteDeviceCacheCeiling, absoluteHostCacheCeiling,
		"VRAM's physical maximum is far below host RAM's, so one number cannot serve both")
}

// Over the tenant cap, the coldest entry of that tenant is reclaimed and the entry that was
// just loaded is not.
func TestGovernorTenantCapEvictsColdestNotNewest(t *testing.T) {
	c := newBoundCache(t)
	sp := govProc(t, c, 1, hostCap(250), caps{})

	cold := "__mo_index_secondary_cold"
	warm := "__mo_index_secondary_warm"
	newest := "__mo_index_secondary_newest"

	loadInto(t, c, sp, cold, 100, 0)
	loadInto(t, c, sp, warm, 100, 0)
	// Make the coldness ordering explicit rather than relying on load timing.
	entryOf(t, c, cold).ExpireAt.Store(1)
	entryOf(t, c, warm).ExpireAt.Store(2)

	loadInto(t, c, sp, newest, 100, 0)

	require.False(t, isResident(c, cold), "300 > 250, so the coldest entry is reclaimed")
	require.True(t, isResident(c, warm), "reclaiming stops as soon as the cap is met")
	require.True(t, isResident(c, newest), "the entry just loaded is never the victim")
}

// One tenant's residency does not consume another's budget.
func TestGovernorTenantCapIsPerTenant(t *testing.T) {
	c := newBoundCache(t)
	spA := govProc(t, c, 1, hostCap(250), caps{})

	loadInto(t, c, spA, "__mo_index_secondary_a1", 200, 0)

	spB := govProc(t, c, 2, hostCap(250), caps{})
	loadInto(t, c, spB, "__mo_index_secondary_b1", 200, 0)

	require.True(t, isResident(c, "__mo_index_secondary_a1"), "tenant 2's load must not evict tenant 1")
	require.True(t, isResident(c, "__mo_index_secondary_b1"))
}

// The SYS cap bounds the CN as a whole and reclaims across tenants.
func TestGovernorSysCapSpansTenants(t *testing.T) {
	c := newBoundCache(t)
	spA := govProc(t, c, 1, caps{}, hostCap(250))
	loadInto(t, c, spA, "__mo_index_secondary_sysa", 200, 0)
	entryOf(t, c, "__mo_index_secondary_sysa").ExpireAt.Store(1)

	spB := govProc(t, c, 2, caps{}, hostCap(250))
	loadInto(t, c, spB, "__mo_index_secondary_sysb", 200, 0)

	require.False(t, isResident(c, "__mo_index_secondary_sysa"),
		"400 > the SYS cap of 250, so the coldest entry goes even though it belongs to another tenant")
	require.True(t, isResident(c, "__mo_index_secondary_sysb"))
}

// The two arenas have their own variables and never cross: a host cap ignores device-resident
// bytes, and a device cap ignores host-resident ones -- evicting a host-only index to relieve
// VRAM pressure would free no VRAM at all.
func TestGovernorArenasAreBudgetedSeparately(t *testing.T) {
	t.Run("a host cap does not reclaim device bytes", func(t *testing.T) {
		c := newBoundCache(t)
		sp := govProc(t, c, 1, hostCap(100), caps{})

		deviceOnly := "__mo_index_secondary_devonly"
		loadInto(t, c, sp, deviceOnly, 0, 5000)
		entryOf(t, c, deviceOnly).ExpireAt.Store(1)

		// 150 host bytes against a host cap of 100, with the device budget unset.
		loadInto(t, c, sp, "__mo_index_secondary_hostheavy", 150, 0)

		require.True(t, isResident(c, deviceOnly),
			"max_index_cache_size bounds RAM; a device-only entry frees none of it")
	})

	t.Run("a device cap does not reclaim host bytes", func(t *testing.T) {
		c := newBoundCache(t)
		sp := govProc(t, c, 1, gpuCap(100), caps{})

		hostOnly := "__mo_index_secondary_hostonly"
		loadInto(t, c, sp, hostOnly, 5000, 0)
		entryOf(t, c, hostOnly).ExpireAt.Store(1)

		// 150 device bytes against a device cap of 100, with the host budget unset.
		loadInto(t, c, sp, "__mo_index_secondary_devheavy", 0, 150)

		require.True(t, isResident(c, hostOnly),
			"max_gpu_index_cache_size bounds VRAM; a host-only entry frees none of it")
	})

	t.Run("the device cap binds its own arena", func(t *testing.T) {
		c := newBoundCache(t)
		sp := govProc(t, c, 1, gpuCap(250), caps{})

		cold := "__mo_index_secondary_gpucold"
		loadInto(t, c, sp, cold, 0, 200)
		entryOf(t, c, cold).ExpireAt.Store(1)
		loadInto(t, c, sp, "__mo_index_secondary_gpunew", 0, 200)

		require.False(t, isResident(c, cold), "400 > the device cap of 250")
		require.True(t, isResident(c, "__mo_index_secondary_gpunew"))
	})
}

// An entry holding nothing is charged nothing and is never chosen as a victim.
func TestGovernorIgnoresZeroSizedEntries(t *testing.T) {
	c := newBoundCache(t)
	sp := govProc(t, c, 1, hostCap(50), caps{})

	free := "__mo_index_secondary_free"
	loadInto(t, c, sp, free, 0, 0)
	entryOf(t, c, free).ExpireAt.Store(1)

	loadInto(t, c, sp, "__mo_index_secondary_big", 500, 0)
	require.True(t, isResident(c, free), "evicting a zero-byte entry would free nothing")
}

// A load with no session at all is charged but bounded by nothing, and must not panic:
// SqlProcess dereferences whichever of Proc/SqlCtx is set, and a zero one has neither.
func TestGovernorHandlesSessionlessLoad(t *testing.T) {
	c := newBoundCache(t)
	require.NoError(t, searchAt(c, "__mo_index_secondary_nosession", &countingSearch{host: 10}))
	require.NoError(t, searchAt(c, "__mo_index_secondary_nosession2", &countingSearch{host: 10}))

	require.False(t, hasSession(nil))
	require.False(t, hasSession(&sqlexec.SqlProcess{}))
	require.EqualValues(t, 10, entryOf(t, c, "__mo_index_secondary_nosession").hostBytes.Load())
}

// An unreadable variable resolves to unlimited: the governor is a memory policy, not a
// correctness gate, and must never fail or bound a query because a read failed.
func TestGovernorUnreadableLimitIsUnlimited(t *testing.T) {
	boom := &sqlexec.SqlProcess{SqlCtx: &sqlexec.SqlContext{
		Ctx: context.Background(), CNUuid: "gov-test-cn", AccountId: 1,
		ResolveVariableFunc: func(string, bool, bool) (interface{}, error) {
			return nil, moerr.NewInternalErrorNoCtx("boom")
		},
	}}
	require.Equal(t, caps{}, newBoundCache(t).tenantCacheLimits(boom), "resolver error")

	wrong := &sqlexec.SqlProcess{SqlCtx: &sqlexec.SqlContext{
		Ctx: context.Background(), CNUuid: "gov-test-cn", AccountId: 1,
		ResolveVariableFunc: func(string, bool, bool) (interface{}, error) { return "not an int", nil },
	}}
	require.Equal(t, caps{}, newBoundCache(t).tenantCacheLimits(wrong), "wrong type")

	require.Equal(t, caps{}, newBoundCache(t).tenantCacheLimits(nil), "no session")
	require.Equal(t, caps{}, newBoundCache(t).tenantCacheLimits(&sqlexec.SqlProcess{}), "no proc or sqlctx")
}

// A failed SYS read keeps the last known good cap rather than falling open to unlimited.
func TestGovernorSysReadFailureKeepsLastKnownCap(t *testing.T) {
	c := newBoundCache(t)
	sp := govProc(t, c, 1, caps{}, hostCap(250))

	// Expire the memoized value so the next call re-reads, and let that read fail.
	c.sysLimit.fetched = time.Now().Add(-2 * sysLimitTTL)
	require.Equal(t, hostCap(250), c.sysCacheLimit(sp),
		"a catalog blip must not silently unbound the cache")
}

func TestParseByteLimit(t *testing.T) {
	n, err := parseByteLimit(" 1048576 ")
	require.NoError(t, err)
	require.EqualValues(t, 1048576, n)

	_, err = parseByteLimit("")
	require.Error(t, err)
	_, err = parseByteLimit("8MB")
	require.Error(t, err)
	_, err = parseByteLimit("-1")
	require.Error(t, err, "a negative cap is rejected, not read as unlimited")
}

func entryOf(t *testing.T, c *VectorIndexCache, key string) *VectorIndexSearch {
	t.Helper()
	value, ok := c.IndexMap.Load(key)
	require.True(t, ok, "%q must be resident", key)
	return value.(*VectorIndexSearch)
}

// varRows builds the shape the SYS read actually gets back: (variable_name, variable_value),
// both varchar columns of mo_mysql_compatibility_mode, one row per variable the SYS account has
// SET. No rows means it set neither cap.
func varRows(t *testing.T, mp *mpool.MPool, nameValue ...string) executor.Result {
	t.Helper()
	require.Zero(t, len(nameValue)%2, "varRows takes name/value pairs")
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	for i := 0; i < len(nameValue); i += 2 {
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte(nameValue[i]), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte(nameValue[i+1]), false, mp))
	}
	bat.SetRowCount(len(nameValue) / 2)
	return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
}

// sysProc is a session with no tenant cap, so only the SYS read is under test.
func sysProc() *sqlexec.SqlProcess {
	return &sqlexec.SqlProcess{SqlCtx: &sqlexec.SqlContext{
		Ctx: context.Background(), CNUuid: "gov-test-cn", AccountId: 3,
		ResolveVariableFunc: func(string, bool, bool) (interface{}, error) { return int64(0), nil },
	}}
}

// withSysSql installs a SYS-read stub and starts the memo cold.
func withSysSql(t *testing.T, c *VectorIndexCache, f func(context.Context, string, uint32, string, string) (executor.Result, error)) {
	t.Helper()
	orig := runSysSql
	runSysSql = f
	t.Cleanup(func() { runSysSql = orig })
	c.sysLimit = sysLimitCache{}
}

// The SYS read extracts the cap from the catalog row, as the SYS account and on its own context.
func TestGovernorSysReadExtractsValue(t *testing.T) {
	c := newBoundCache(t)
	mp := mpool.MustNewZero()

	var gotAccount uint32
	var gotCN, gotSQL string
	calls := 0
	withSysSql(t, c, func(_ context.Context, cn string, account uint32, _, sql string) (executor.Result, error) {
		calls++
		gotCN, gotAccount, gotSQL = cn, account, sql
		return varRows(t, mp,
			maxIndexCacheSizeVar, "1048576",
			maxGpuIndexCacheSizeVar, "4096"), nil
	})

	want := caps{host: 1048576, device: 4096}
	require.Equal(t, want, c.sysCacheLimit(sysProc()),
		"each variable must land in its own arena, keyed by name not row order")
	require.EqualValues(t, 0, gotAccount, "the CN-wide caps must be read as the SYS account, not the caller")
	require.Equal(t, "gov-test-cn", gotCN)
	require.Contains(t, gotSQL, "account_id = 0")
	require.Contains(t, gotSQL, maxIndexCacheSizeVar)
	require.Contains(t, gotSQL, maxGpuIndexCacheSizeVar)

	// Memoized: a second call inside the TTL does not re-query.
	require.Equal(t, want, c.sysCacheLimit(sysProc()))
	require.Equal(t, 1, calls)

	// Past the TTL it re-reads and picks up a new value.
	c.sysLimit.fetched = time.Now().Add(-2 * sysLimitTTL)
	require.Equal(t, want, c.sysCacheLimit(sysProc()))
	require.Equal(t, 2, calls, "the memo must expire so SET GLOBAL takes effect without a restart")
}

// No row means the SYS account never set that cap: unlimited. Setting only one leaves the other
// unlimited rather than defaulting it to the one that was set.
func TestGovernorSysReadNoRowIsUnlimited(t *testing.T) {
	c := newBoundCache(t)
	mp := mpool.MustNewZero()
	withSysSql(t, c, func(context.Context, string, uint32, string, string) (executor.Result, error) {
		return varRows(t, mp), nil
	})
	require.Equal(t, caps{}, c.sysCacheLimit(sysProc()))

	c2 := newBoundCache(t)
	withSysSql(t, c2, func(context.Context, string, uint32, string, string) (executor.Result, error) {
		return varRows(t, mp, maxGpuIndexCacheSizeVar, "512"), nil
	})
	require.Equal(t, gpuCap(512), c2.sysCacheLimit(sysProc()),
		"an unset host cap stays unlimited when only the device cap is set")
}

// A value that does not parse is ignored rather than guessed at.
func TestGovernorSysReadUnparseableValueIsIgnored(t *testing.T) {
	c := newBoundCache(t)
	mp := mpool.MustNewZero()
	withSysSql(t, c, func(context.Context, string, uint32, string, string) (executor.Result, error) {
		return varRows(t, mp, maxIndexCacheSizeVar, "8MB"), nil
	})
	require.Equal(t, caps{}, c.sysCacheLimit(sysProc()))
}

// The SYS cap read through the catalog binds a real load, end to end.
func TestGovernorSysReadDrivesEviction(t *testing.T) {
	c := newBoundCache(t)
	mp := mpool.MustNewZero()
	withSysSql(t, c, func(context.Context, string, uint32, string, string) (executor.Result, error) {
		return varRows(t, mp, maxIndexCacheSizeVar, "250"), nil
	})

	sp := sysProc()
	loadInto(t, c, sp, "__mo_index_secondary_syssql_a", 200, 0)
	entryOf(t, c, "__mo_index_secondary_syssql_a").ExpireAt.Store(1)
	loadInto(t, c, sp, "__mo_index_secondary_syssql_b", 200, 0)

	require.False(t, isResident(c, "__mo_index_secondary_syssql_a"),
		"400 > the catalog-read SYS cap of 250")
	require.True(t, isResident(c, "__mo_index_secondary_syssql_b"))
}

// observeAtLoad reports its size from Preload onward and records cache state at the moment Load
// is called, so the ORDER of reclaim vs load is observable rather than inferred.
type observeAtLoad struct {
	countingSearch
	c           *VectorIndexCache
	watch       string
	watchAtLoad bool
	preloadRan  bool
	loadRan     bool
}

func (m *observeAtLoad) Preload(*sqlexec.SqlProcess) error {
	m.preloadRan = true
	return nil
}

func (m *observeAtLoad) Load(*sqlexec.SqlProcess) error {
	m.loadRan = true
	m.watchAtLoad = isResident(m.c, m.watch)
	return nil
}

// Room is reclaimed BETWEEN Preload and Load: by the time the new index is being materialized
// the victim is already gone, so peak residency tracks the cap instead of exceeding it by a
// whole index.
func TestGovernorMakesRoomBeforeLoadNotAfter(t *testing.T) {
	c := newBoundCache(t)
	sp := govProc(t, c, 1, hostCap(250), caps{})

	victim := "__mo_index_secondary_victim"
	loadInto(t, c, sp, victim, 200, 0)
	entryOf(t, c, victim).ExpireAt.Store(1)

	// 200 more against a cap of 250: the victim must go before this one is loaded.
	newcomer := &observeAtLoad{countingSearch: countingSearch{host: 200}, c: c, watch: victim}
	_, _, err := c.Search(sp, "__mo_index_secondary_newcomer", newcomer, nil, vectorindex.RuntimeConfig{})
	require.NoError(t, err)

	require.True(t, newcomer.preloadRan, "Preload must run on a miss")
	require.True(t, newcomer.loadRan)
	require.False(t, newcomer.watchAtLoad,
		"the victim must already be evicted when Load is entered, not reclaimed afterwards")
	require.True(t, isResident(c, "__mo_index_secondary_newcomer"))
}

// The pre-load pass also empties when the incoming index is at or over the whole cap. That is
// the case caps.less reduces to nothing, and flooring the reduced cap at 0 would have made
// enforce read the arena as unlimited and reclaim nothing -- leaving peak residency at the
// victim plus the whole newcomer, which is exactly what makeRoom exists to avoid.
func TestGovernorMakesRoomWhenIncomingFillsTheCap(t *testing.T) {
	for _, incoming := range []int64{250, 300, 10_000} {
		t.Run(fmt.Sprintf("incoming=%d", incoming), func(t *testing.T) {
			c := newBoundCache(t)
			sp := govProc(t, c, 1, hostCap(250), caps{})

			victim := "__mo_index_secondary_victim"
			loadInto(t, c, sp, victim, 200, 0)
			entryOf(t, c, victim).ExpireAt.Store(1)

			newcomer := &observeAtLoad{countingSearch: countingSearch{host: incoming}, c: c, watch: victim}
			_, _, err := c.Search(sp, "__mo_index_secondary_newcomer", newcomer, nil, vectorindex.RuntimeConfig{})
			require.NoError(t, err)

			require.False(t, newcomer.watchAtLoad,
				"the victim must be gone before Load, not reclaimed after it")
			require.Equal(t, incoming <= 250, isResident(c, "__mo_index_secondary_newcomer"),
				"retain an exact-fit entry; retire an oversized entry after its successful query")
		})
	}
}

// An index larger than the whole budget still loads: the governor empties what it can and gets
// out of the way rather than failing a query on an accounting rule.
func TestGovernorOversizedIndexStillLoads(t *testing.T) {
	c := newBoundCache(t)
	sp := govProc(t, c, 1, hostCap(250), caps{})

	old := "__mo_index_secondary_old"
	loadInto(t, c, sp, old, 200, 0)
	entryOf(t, c, old).ExpireAt.Store(1)

	loadInto(t, c, sp, "__mo_index_secondary_huge", 10_000, 0)
	require.False(t, isResident(c, old), "everything reclaimable is reclaimed")
	require.False(t, isResident(c, "__mo_index_secondary_huge"), "the successful load is retired after use")
}

// caps.less floors at zero and leaves an unset arena unlimited.
func TestCapsLess(t *testing.T) {
	require.Equal(t, caps{host: 50}, caps{host: 250}.less(caps{host: 200}))
	require.Equal(t, caps{host: 1}, caps{host: 250}.less(caps{host: 10_000}),
		"a set arena floors at 1, never at the 0 that means unlimited")
	require.Equal(t, caps{}, caps{}.less(caps{host: 99}), "unlimited minus anything is unlimited")
	require.Equal(t, caps{device: 5}, caps{device: 20}.less(caps{host: 99, device: 15}),
		"each arena is reduced by its own incoming bytes")
}

// When the CN-wide cap binds, the account asking for room gives up its own entries before a
// quiet neighbour's, even when the neighbour's is colder.
func TestGovernorSysCapChargesTheLoadingAccountFirst(t *testing.T) {
	c := newBoundCache(t)

	// A cap of 300 with 200-byte loads: the first greedy load fits beside the quiet entry
	// (60 <= 300-200), so tenant 1 arrives at the second one already holding enough of its
	// own to cover the overage. That is what isolates WHOSE entry is taken -- with a tighter
	// cap the widening would be legitimate, because tenant 1 would have nothing to give.
	spB := govProc(t, c, 2, caps{}, hostCap(300))
	loadInto(t, c, spB, "__mo_index_secondary_quiet", 60, 0)
	entryOf(t, c, "__mo_index_secondary_quiet").ExpireAt.Store(1)

	// Tenant 1 floods. Its own older entry is WARMER than tenant 2's.
	spA := govProc(t, c, 1, caps{}, hostCap(300))
	loadInto(t, c, spA, "__mo_index_secondary_greedy1", 200, 0)
	require.True(t, isResident(c, "__mo_index_secondary_quiet"), "precondition: nothing evicted yet")
	entryOf(t, c, "__mo_index_secondary_greedy1").ExpireAt.Store(9)
	loadInto(t, c, spA, "__mo_index_secondary_greedy2", 200, 0)

	require.True(t, isResident(c, "__mo_index_secondary_quiet"),
		"the quiet tenant keeps its entry: the flooding account had bytes of its own to give")
	require.False(t, isResident(c, "__mo_index_secondary_greedy1"),
		"the account asking for room pays first, even though its entry is warmer")
	require.True(t, isResident(c, "__mo_index_secondary_greedy2"), "never the entry just loaded")
}

// If the loading account cannot free enough on its own, the pass widens to everyone.
func TestGovernorSysCapWidensWhenLoaderHasNothingLeft(t *testing.T) {
	c := newBoundCache(t)

	spB := govProc(t, c, 2, caps{}, hostCap(150))
	loadInto(t, c, spB, "__mo_index_secondary_other", 100, 0)
	entryOf(t, c, "__mo_index_secondary_other").ExpireAt.Store(1)

	// Tenant 1 arrives holding nothing, and its single load already exceeds the cap on its
	// own, so there is no entry of its own to reclaim.
	spA := govProc(t, c, 1, caps{}, hostCap(150))
	loadInto(t, c, spA, "__mo_index_secondary_newcomer", 120, 0)

	require.False(t, isResident(c, "__mo_index_secondary_other"),
		"with nothing of its own to give, the pass widens to the coldest entry anywhere")
	require.True(t, isResident(c, "__mo_index_secondary_newcomer"))
}

// mutatingSizeSearch has algorithm state that Destroy tears down and GetIndexSize walks -- the
// exact shape (hnsw's s.Indexes, cagra's sub-index slice) that a governor reading sizes outside
// the entry lock would race.
type mutatingSizeSearch struct {
	countingSearch
	parts []int64
}

func (m *mutatingSizeSearch) Preload(*sqlexec.SqlProcess) error {
	m.parts = []int64{40, 60}
	return nil
}
func (m *mutatingSizeSearch) Load(*sqlexec.SqlProcess) error {
	m.parts = []int64{40, 60, 100}
	return nil
}
func (m *mutatingSizeSearch) GetIndexSize() (int64, int64) {
	var n int64
	for _, p := range m.parts {
		n += p
	}
	return n, 0
}
func (m *mutatingSizeSearch) Destroy() { m.parts = nil }

// Loads racing evictions must not touch algorithm state from outside the entry lock. The size is
// published to atomics by captureSize under the lock; if the governor ever went back to calling
// Algo.GetIndexSize from makeRoom or chargeAndEnforce, -race would flag it here against Destroy.
func TestGovernorSizeReadsDoNotRaceEviction(t *testing.T) {
	c := newBoundCache(t)
	keys := []string{
		"__mo_index_secondary_race0",
		"__mo_index_secondary_race1",
		"__mo_index_secondary_race2",
	}

	// Stub the SYS read and build every session UP FRONT: govProc mutates package-level state
	// (runSysSql) and the cache's memo, neither of which is safe to touch from the goroutines.
	stubSysLimit(t, c, hostCap(400))
	procs := make([]*sqlexec.SqlProcess, 3)
	for i := range procs {
		procs[i] = &sqlexec.SqlProcess{SqlCtx: &sqlexec.SqlContext{
			Ctx: context.Background(), CNUuid: "gov-test-cn", AccountId: uint32(i),
			ResolveVariableFunc: func(name string, _, _ bool) (interface{}, error) {
				if name == maxIndexCacheSizeVar {
					return int64(150), nil
				}
				return int64(0), nil
			},
		}}
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sp := procs[i%3]
			for n := 0; n < 40; n++ {
				_, _, _ = c.Search(sp, keys[n%len(keys)], &mutatingSizeSearch{}, nil, vectorindex.RuntimeConfig{})
			}
		}(i)
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for n := 0; n < 40; n++ {
				c.Remove(keys[n%len(keys)])
				c.HouseKeeping()
			}
		}(i)
	}
	wg.Wait()
}

// A failing SYS read is rate-limited like a successful one. Without stamping the attempt, a
// catalog that is down would be re-queried on every cache miss -- twice per miss, each up to
// the 10s query timeout, serialized on sysLimit.mu -- turning an outage into a per-miss stall.
func TestGovernorSysReadFailureIsRateLimited(t *testing.T) {
	c := newBoundCache(t)
	calls := 0
	withSysSql(t, c, func(context.Context, string, uint32, string, string) (executor.Result, error) {
		calls++
		return executor.Result{}, moerr.NewInternalErrorNoCtx("catalog down")
	})

	for i := 0; i < 5; i++ {
		require.Equal(t, caps{}, c.sysCacheLimit(sysProc()),
			"never having read a value is unlimited, the unconfigured behaviour")
	}
	require.Equal(t, 1, calls, "the failed attempt must suppress retries for sysLimitTTL")

	// Past the TTL it tries again, and a recovered catalog is picked up.
	c.sysLimit.fetched = time.Now().Add(-2 * sysLimitTTL)
	mp := mpool.MustNewZero()
	runSysSql = func(context.Context, string, uint32, string, string) (executor.Result, error) {
		calls++
		return varRows(t, mp, maxIndexCacheSizeVar, "512"), nil
	}
	require.Equal(t, hostCap(512), c.sysCacheLimit(sysProc()))
	require.Equal(t, 2, calls)
}

// One arena's evictions must not be charged to the next. With a shared pre-pass snapshot the
// device pass still counted bytes the host pass had already freed -- and got no credit when it
// reached those entries, because evictEntry refuses an entry already claimed -- so it evicted a
// warm index for room that was free.
func TestGovernorArenaPassesDoNotDoubleCountFreedBytes(t *testing.T) {
	c := newBoundCache(t)
	// Host cap 10000 binds (12000 resident); device cap 4000 does NOT once the host pass has
	// evicted A (3000 device left, under 4000).
	sp := govProc(t, c, 1, caps{host: 10000, device: 4000}, caps{})

	a := "__mo_index_secondary_arena_a"
	b := "__mo_index_secondary_arena_b"
	loadInto(t, c, sp, a, 6000, 3000)
	entryOf(t, c, a).ExpireAt.Store(1) // coldest, so the host pass takes it
	loadInto(t, c, sp, b, 6000, 3000)

	require.False(t, isResident(c, a), "the host pass evicts the coldest entry to meet 10000")
	require.True(t, isResident(c, b),
		"after A is gone only 3000 device bytes remain, under the 4000 device cap: B must survive")
}

// A DROP releases the index's named-snapshot generations too. They are keyed <table>@<ts>, which
// no exact-key Remove matches, and the staleness sweep skips them -- so before this they stayed
// resident (pinning VRAM for the cuVS algorithms) until their TTL ran out.
func TestRemoveAlsoDropsSnapshotGenerations(t *testing.T) {
	c := newBoundCache(t)
	const tbl = "__mo_index_secondary_dropme"
	other := "__mo_index_secondary_keepme"

	require.NoError(t, searchAt(c, tbl, &countingSearch{host: 10}))
	snapA := SnapshotKey(tbl, snapshotTS(100))
	snapB := SnapshotKey(tbl, snapshotTS(200))
	require.NoError(t, searchAt(c, snapA, &countingSearch{host: 10}))
	require.NoError(t, searchAt(c, snapB, &countingSearch{host: 10}))
	require.NoError(t, searchAt(c, other, &countingSearch{host: 10}))
	require.NoError(t, searchAt(c, SnapshotKey(other, snapshotTS(100)), &countingSearch{host: 10}))

	c.Remove(tbl)

	require.False(t, isResident(c, tbl), "the current generation goes")
	require.False(t, isResident(c, snapA), "and every snapshot generation of the same index")
	require.False(t, isResident(c, snapB))
	require.True(t, isResident(c, other), "another index is untouched")
	require.True(t, isResident(c, SnapshotKey(other, snapshotTS(100))),
		"including its snapshot generations -- the prefix must not over-match")
}

// invalidStateSearch fails Search with ErrInvalidState from INSIDE the algorithm, the way a
// paused txn client or a failed remote run does -- not because the entry was evicted.
type invalidStateSearch struct {
	countingSearch
	calls atomic.Int64
}

func (m *invalidStateSearch) Search(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig) (any, []float64, error) {
	if m.calls.Add(1) > 1 {
		return nil, nil, moerr.NewInternalErrorNoCtx("unexpected backend retry")
	}
	return nil, nil, moerr.NewInvalidStateNoCtx("txn client is in pause state")
}

// A backend error is not evidence of cache eviction. Retrying a permanent error
// spins forever; waiting for an unclaimed destruction hangs forever.
func TestSearchReturnsBackendInvalidState(t *testing.T) {
	c := newBoundCache(t)
	algo := &invalidStateSearch{}

	done := make(chan error, 1)
	go func() {
		_, _, err := c.Search(nil, "__mo_index_secondary_notevicted", algo, nil, vectorindex.RuntimeConfig{})
		done <- err
	}()

	select {
	case err := <-done:
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidState))
	case <-time.After(20 * time.Second):
		t.Fatal("Search blocked on a teardown that will never happen")
	}
	require.EqualValues(t, 1, algo.calls.Load())
}

// The FIRST sys-cap read has no last-known value to fall back on: c.sysLimit.value is still
// the zero caps, which every reader interprets as unlimited. A concurrent miss arriving while
// that first query is in flight must therefore WAIT for the real cap rather than be told the
// governor is unconfigured -- otherwise the whole window (up to the query timeout, at CN
// startup, with a cold cache) runs with makeRoom and chargeAndEnforce both short-circuited.
func TestGovernorFirstSysReadBlocksConcurrentReaders(t *testing.T) {
	c := newBoundCache(t)
	mp := mpool.MustNewZero()

	release := make(chan struct{})
	entered := make(chan struct{})
	var calls atomic.Int32

	withSysSql(t, c, func(context.Context, string, uint32, string, string) (executor.Result, error) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release // hold the first read open until the second caller is parked
		}
		return varRows(t, mp, maxIndexCacheSizeVar, "512"), nil
	})

	firstDone := make(chan caps, 1)
	go func() { firstDone <- c.sysCacheLimit(sysProc()) }()
	<-entered // the first read is now in flight, holding the claim

	secondDone := make(chan caps, 1)
	go func() { secondDone <- c.sysCacheLimit(sysProc()) }()

	// The second caller must not have answered yet: an answer here could only be the zero
	// caps, i.e. "unlimited".
	select {
	case got := <-secondDone:
		t.Fatalf("a concurrent reader answered %v during the first fetch instead of waiting", got)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	require.Equal(t, hostCap(512), <-firstDone)
	require.Equal(t, hostCap(512), <-secondDone, "the waiter gets the real cap, never unlimited")
}

// A REFRESH does have a last-known value, so it must NOT block: the point of releasing the
// lock around the query is that an unreachable catalog cannot stall every cache miss.
func TestGovernorSysRefreshServesLastKnownWithoutBlocking(t *testing.T) {
	c := newBoundCache(t)
	mp := mpool.MustNewZero()

	release := make(chan struct{})
	entered := make(chan struct{})
	var calls atomic.Int32

	withSysSql(t, c, func(context.Context, string, uint32, string, string) (executor.Result, error) {
		if calls.Add(1) == 2 {
			close(entered)
			<-release
		}
		return varRows(t, mp, maxIndexCacheSizeVar, "512"), nil
	})

	// First read completes and establishes the known cap.
	require.Equal(t, hostCap(512), c.sysCacheLimit(sysProc()))

	// Age it out so the next call refreshes, and hold that refresh open.
	c.sysLimit.fetched = time.Now().Add(-2 * sysLimitTTL)
	go func() { c.sysCacheLimit(sysProc()) }()
	<-entered

	// A concurrent reader is served the last-known cap immediately.
	done := make(chan caps, 1)
	go func() { done <- c.sysCacheLimit(sysProc()) }()
	select {
	case got := <-done:
		require.Equal(t, hostCap(512), got, "served from the memo, not blocked on the refresh")
	case <-time.After(2 * time.Second):
		t.Fatal("a refresh must not park readers that already have a value")
	}
	close(release)
}
