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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

// govProc returns a SqlProcess for account with tenantLimit as its max_index_cache_size, and
// stubs the SYS catalog read to report sysLimit. A limit of 0 means unset (unlimited).
func govProc(t *testing.T, c *VectorIndexCache, account uint32, tenantLimit, sysLimit int64) *sqlexec.SqlProcess {
	t.Helper()
	stubSysLimit(t, c, sysLimit)
	return &sqlexec.SqlProcess{SqlCtx: &sqlexec.SqlContext{
		Ctx:       context.Background(),
		CNUuid:    "gov-test-cn",
		AccountId: account,
		ResolveVariableFunc: func(name string, _, isGlobal bool) (interface{}, error) {
			require.Equal(t, maxIndexCacheSizeVar, name)
			require.True(t, isGlobal, "the cap is global-scope and must be read as such")
			return tenantLimit, nil
		},
	}}
}

// stubSysLimit replaces the SYS catalog read for the duration of the test and clears the
// memoized value so each test starts from a cold read.
func stubSysLimit(t *testing.T, c *VectorIndexCache, value int64) {
	t.Helper()
	orig := runSysSql
	runSysSql = func(context.Context, string, uint32, string, string) (executor.Result, error) {
		return executor.Result{}, moerr.NewInternalErrorNoCtx("no catalog in unit test")
	}
	t.Cleanup(func() { runSysSql = orig })
	c.sysLimit.value, c.sysLimit.fetched, c.sysLimit.valid = value, time.Now(), true
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
	sp := govProc(t, c, 7, 0, 0)

	loadInto(t, c, sp, "__mo_index_secondary_charge", 300, 40)

	value, ok := c.IndexMap.Load("__mo_index_secondary_charge")
	require.True(t, ok)
	entry := value.(*VectorIndexSearch)
	require.EqualValues(t, 300, entry.hostBytes.Load())
	require.EqualValues(t, 40, entry.deviceBytes.Load(), "device bytes must not be folded into host")
	require.EqualValues(t, 7, entry.accountID.Load())
}

// Both caps unset is the default, and it evicts nothing however much is resident.
func TestGovernorUnsetCapNeverEvicts(t *testing.T) {
	c := newBoundCache(t)
	sp := govProc(t, c, 1, 0, 0)

	keys := []string{"__mo_index_secondary_a", "__mo_index_secondary_b", "__mo_index_secondary_c"}
	for _, k := range keys {
		loadInto(t, c, sp, k, 1<<30, 0)
	}
	for _, k := range keys {
		require.True(t, isResident(c, k), "%q must survive: max_index_cache_size defaults to unlimited", k)
	}
}

// Over the tenant cap, the coldest entry of that tenant is reclaimed and the entry that was
// just loaded is not.
func TestGovernorTenantCapEvictsColdestNotNewest(t *testing.T) {
	c := newBoundCache(t)
	sp := govProc(t, c, 1, 250, 0)

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
	spA := govProc(t, c, 1, 250, 0)

	loadInto(t, c, spA, "__mo_index_secondary_a1", 200, 0)

	spB := govProc(t, c, 2, 250, 0)
	loadInto(t, c, spB, "__mo_index_secondary_b1", 200, 0)

	require.True(t, isResident(c, "__mo_index_secondary_a1"), "tenant 2's load must not evict tenant 1")
	require.True(t, isResident(c, "__mo_index_secondary_b1"))
}

// The SYS cap bounds the CN as a whole and reclaims across tenants.
func TestGovernorSysCapSpansTenants(t *testing.T) {
	c := newBoundCache(t)
	spA := govProc(t, c, 1, 0, 250)
	loadInto(t, c, spA, "__mo_index_secondary_sysa", 200, 0)
	entryOf(t, c, "__mo_index_secondary_sysa").ExpireAt.Store(1)

	spB := govProc(t, c, 2, 0, 250)
	loadInto(t, c, spB, "__mo_index_secondary_sysb", 200, 0)

	require.False(t, isResident(c, "__mo_index_secondary_sysa"),
		"400 > the SYS cap of 250, so the coldest entry goes even though it belongs to another tenant")
	require.True(t, isResident(c, "__mo_index_secondary_sysb"))
}

// Host and device are separate budgets: device pressure never reclaims a host-only entry,
// whose eviction would free no VRAM at all.
func TestGovernorArenasAreBudgetedSeparately(t *testing.T) {
	c := newBoundCache(t)
	sp := govProc(t, c, 1, 100, 0)

	hostOnly := "__mo_index_secondary_hostonly"
	loadInto(t, c, sp, hostOnly, 10, 0)
	entryOf(t, c, hostOnly).ExpireAt.Store(1)

	// 150 device bytes against a cap of 100, while host is only 10+0 and well under.
	deviceHeavy := "__mo_index_secondary_deviceheavy"
	loadInto(t, c, sp, deviceHeavy, 0, 150)

	require.True(t, isResident(c, hostOnly),
		"a host-only entry frees no VRAM, so device pressure must not take it")
	require.True(t, isResident(c, deviceHeavy), "the entry just loaded is never the victim")
}

// An entry holding nothing is charged nothing and is never chosen as a victim.
func TestGovernorIgnoresZeroSizedEntries(t *testing.T) {
	c := newBoundCache(t)
	sp := govProc(t, c, 1, 50, 0)

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
	require.EqualValues(t, 0, tenantCacheLimit(boom), "resolver error")

	wrong := &sqlexec.SqlProcess{SqlCtx: &sqlexec.SqlContext{
		Ctx: context.Background(), CNUuid: "gov-test-cn", AccountId: 1,
		ResolveVariableFunc: func(string, bool, bool) (interface{}, error) { return "not an int", nil },
	}}
	require.EqualValues(t, 0, tenantCacheLimit(wrong), "wrong type")

	require.EqualValues(t, 0, tenantCacheLimit(nil), "no session")
	require.EqualValues(t, 0, tenantCacheLimit(&sqlexec.SqlProcess{}), "no proc or sqlctx")
}

// A failed SYS read keeps the last known good cap rather than falling open to unlimited.
func TestGovernorSysReadFailureKeepsLastKnownCap(t *testing.T) {
	c := newBoundCache(t)
	sp := govProc(t, c, 1, 0, 250)

	// Expire the memoized value so the next call re-reads, and let that read fail.
	c.sysLimit.fetched = time.Now().Add(-2 * sysLimitTTL)
	require.EqualValues(t, 250, c.sysCacheLimit(sp),
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
