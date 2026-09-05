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

// The max_index_cache_size governor.
//
// Two BYTE budgets on resident index bytes -- max_index_cache_size for HOST memory and
// max_gpu_index_cache_size for DEVICE memory -- each read per account:
//
//   - its value on the SYS account (id 0) caps every tenant's indexes on this CN together
//   - its value on a tenant caps that tenant alone
//
// All four apply; whichever binds first evicts. 0 means "not set by an operator", not
// "unbounded": when no cap is set anywhere the governor substitutes the arena ceiling
// (absoluteHostCacheCeiling / absoluteDeviceCacheCeiling), so the accounting always runs and
// every entry stays evictable. The ceilings sit above any real machine, so an unconfigured
// deployment is not constrained by them -- what they remove is the state where the governor
// short-circuits and residency has no bound at all.
//
// The arenas get their OWN variables rather than sharing one number, because a CN has far more
// RAM than VRAM: a single figure large enough to be a sane host budget would never bind on the
// device, and one small enough to bound VRAM would cripple the host cache. They are likewise
// never summed when charged -- evicting a host-only index to relieve device pressure would
// free no VRAM at all.
//
// The bound is enforced by EVICTION, not refusal. A refusal would fail an ordinary query on a
// cache accounting rule, and there is nothing special about a named-snapshot generation here:
// every resident index is charged and every resident index is evictable. Eviction reuses the
// existing claim path (beginEviction / evictEntry), which Search already retries around, so
// reclaiming an entry under a live query is safe.
//
// The just-loaded entry is never the one evicted: its caller is about to search it.

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

const (
	// maxIndexCacheSizeVar and maxGpuIndexCacheSizeVar are the host and device byte budgets,
	// both declared in pkg/frontend/variables.go with Scope ScopeGlobal, defaulting to the
	// arena ceilings mirrored below.
	maxIndexCacheSizeVar    = "max_index_cache_size"
	maxGpuIndexCacheSizeVar = "max_gpu_index_cache_size"

	// sysLimitTTL bounds how stale the SYS account's value may be. The read costs one
	// auto-commit SQL and only ever runs on a cache MISS, which has just paid for a full
	// index load, so the cadence is about bounding SET GLOBAL latency, not query cost.
	sysLimitTTL = 15 * time.Second

	// absoluteHostCacheCeiling / absoluteDeviceCacheCeiling MIRROR the defaults of
	// max_index_cache_size / max_gpu_index_cache_size (pkg/frontend/variables.go). The
	// variable default gives a NEW deployment a readable advertised maximum; these give the
	// same bound to everything the default cannot reach -- an upgraded cluster whose 0 is
	// already persisted, an explicit 0, and the sessionless load paths. Each is sized above
	// the physical maximum of its own arena, so neither ever refuses a load a real deployment
	// could serve:
	//
	//   host   -- high-end boards reach ~4 TiB and enterprise servers a few dozen TB.
	//   device -- a single card is tens of GB; the ceiling is 8 cards pooled over NVLink,
	//             ~1,440 GB, which is the largest single-node VRAM pool built today.
	//
	// They are separate because the two arenas are orders of magnitude apart: one number
	// sized for host RAM would be meaningless as a VRAM bound.
	//
	// Its purpose is not to size the cache. It is to remove "unlimited" as a reachable state:
	// with a zero cap the governor short-circuits before enforce(), so nothing is charged,
	// nothing is enumerated, and residency is genuinely unbounded -- one resident generation
	// per distinct snapshot timestamp, with no ceiling of any kind. With a finite ceiling the
	// accounting always runs and the eviction path is always live, so an operator lowering
	// SET GLOBAL max_index_cache_size gets a governed cache rather than switching one on.
	//
	// It does NOT make an unconfigured deployment safe on its own: a machine will exhaust its
	// own memory long before this binds. Configuring max_index_cache_size is still what bounds
	// a cache to a machine.
	absoluteHostCacheCeiling   int64 = 64 << 40   // 64 TiB
	absoluteDeviceCacheCeiling int64 = 1440 << 30 // 1440 GiB: 8 pooled GPUs
)

// sysLimitSQL reads both of the SYS account's caps straight from the catalog, in one query.
// The session resolver cannot answer this: it resolves for the CALLING tenant, and the CN-wide
// caps live on account 0.
var sysLimitSQL = accountLimitSQL(catalog.System_Account)

// accountLimitSQL reads one account's two caps straight from the catalog. The session
// resolver cannot answer for another account: it resolves for the CALLING tenant, which is
// the wrong one both for the CN-wide caps (account 0) and for a cross-account snapshot read,
// where the bytes belong to the snapshot's owning tenant rather than to the caller.
func accountLimitSQL(accountID uint32) string {
	return fmt.Sprintf(
		"select variable_name, variable_value from mo_catalog.mo_mysql_compatibility_mode "+
			"where account_id = %d and system_variables = true and variable_name in ('%s', '%s')",
		accountID, maxIndexCacheSizeVar, maxGpuIndexCacheSizeVar)
}

// runSysSql is indirected so the governor's catalog read is testable without a CN.
var runSysSql = sqlexec.RunSqlAutoCommit

// sysLimitCache memoizes the SYS account's cap for sysLimitTTL. A failed read keeps the last
// known good value rather than falling open to "no limit", so a transient catalog error cannot
// silently unbound the cache.
type sysLimitCache struct {
	mu sync.Mutex
	// value is the last answer, good or fallback; fetched stamps the last ATTEMPT, success or
	// failure. Rate-limiting failures matters as much as successes: without it a catalog that
	// is down makes every cache miss re-attempt a 10s-timeout query, twice per miss and
	// serialized on mu, turning an outage into a per-miss stall.
	value   caps
	fetched time.Time
}

// caps is one budget pair: the bytes allowed in each arena, 0 meaning unlimited.
type caps struct {
	host   int64
	device int64
}

func (c caps) of(a arena) int64 {
	if a == arenaHost {
		return c.host
	}
	return c.device
}

func (c caps) unset() bool { return c.host <= 0 && c.device <= 0 }

// less returns the caps with incoming subtracted from each set arena. An unset (0) arena stays
// unset: unlimited minus anything is still unlimited.
//
// A set arena floors at 1, NOT at 0, because 0 is the unset sentinel everywhere else in the
// governor -- enforce skips an arena whose cap is <= 0. Flooring at 0 would therefore turn
// "this index alone fills the budget" into "this arena is unlimited" and reclaim nothing, which
// is the one case the pre-load pass exists for. A floor of 1 keeps the arena bound, so an index
// at or over the whole budget empties what it can and then loads anyway -- refusing it would
// fail a query on an accounting rule, and its own memory gate is what decides whether it
// physically fits.
func (c caps) less(incoming caps) caps {
	out := c
	if out.host > 0 {
		out.host = max(out.host-incoming.host, 1)
	}
	if out.device > 0 {
		out.device = max(out.device-incoming.device, 1)
	}
	return out
}

// arena names the two budgets, kept apart because RAM and VRAM are not interchangeable.
type arena int

const (
	arenaHost arena = iota
	arenaDevice
)

func (a arena) String() string {
	if a == arenaHost {
		return "host"
	}
	return "device"
}

// usage is one account's resident bytes, or the CN's when summed over all accounts.
type usage struct {
	host   int64
	device int64
}

func (u usage) of(a arena) int64 {
	if a == arenaHost {
		return u.host
	}
	return u.device
}

// resident is one evictable entry, carrying what the governor needs to choose a victim
// without holding anything: its key, its cost, and its coldness.
type resident struct {
	key      string
	entry    *VectorIndexSearch
	account  uint32
	expireAt int64
	size     usage
}

// makeRoom reclaims for an index that is measured but NOT yet resident, between its Preload and
// its Load. It evicts until the incoming size fits under every cap, so peak residency tracks the
// budget rather than exceeding it by one whole index -- the best a post-load charge alone can do
// is notice the overshoot after paying for it.
//
// It also runs BEFORE the algorithm's own per-load memory gate. Those gates sample FREE memory
// (memory.HostRowsFitting reads MemoryAvailableIncludingCache; DeviceAggregateFitsFree re-samples
// free VRAM), so entries this governor is about to reclaim would otherwise read as memory that is
// gone, and could veto a load that in fact fits.
//
// Called with NO entry lock held -- see VectorIndexSearch.Preload for why that matters.
func (c *VectorIndexCache) makeRoom(sqlproc *sqlexec.SqlProcess, key string, entry *VectorIndexSearch) error {
	// Preload published its estimate to these atomics under the entry lock; never call
	// GetIndexSize from here, where no lock is held and Destroy may be nilling algo state.
	host, device := entry.hostBytes.Load(), entry.deviceBytes.Load()
	if host <= 0 && device <= 0 {
		return nil
	}
	// The account that OWNS the bytes, not the one that asked: a cross-account snapshot read
	// executes as the snapshot's tenant, so the resident entry belongs to that tenant's budget.
	account := uint32(catalog.System_Account)
	if hasSession(sqlproc) {
		if a, err := sqlproc.EffectiveAccountID(); err == nil {
			account = a
		}
	}
	tenant, sys, lerr := c.limits(sqlproc)
	if lerr != nil {
		// Cannot size the cache, so cannot decide whether this arrival fits. Refuse rather than
		// admit it blind -- the error names the variable to set.
		return lerr
	}
	if tenant.unset() && sys.unset() {
		return nil
	}
	// Reclaim against caps reduced by what is about to arrive, so the room freed is room the
	// incoming index can actually occupy.
	incoming := caps{host: host, device: device}
	c.enforce(account, tenant.less(incoming), sys.less(incoming), key)

	// ADMISSION CONTROL. The reclaim above took every IDLE entry it could; if the arrival still
	// does not fit, the only way to seat it would be to destroy entries with searches running on
	// them. An overloaded server refuses the new request rather than killing the ones already in
	// flight, so this load is rejected here -- before it allocates anything, and without having
	// disturbed a single live query.
	//
	// The check is deliberately AFTER the reclaim: a cache merely full of cold entries admits
	// the newcomer normally. Only genuine overload -- nothing idle left to give -- refuses.
	if over := c.overBudget(account, tenant, sys, key, incoming); over != nil {
		return over
	}
	return nil
}

// overBudget reports why an arrival cannot be seated, or nil when it can.
//
// It re-snapshots rather than trusting the pre-reclaim numbers: the reclaim pass may have freed
// exactly enough, and a concurrent load may have taken some of it back.
func (c *VectorIndexCache) overBudget(account uint32, tenant, sys caps, key string, incoming caps) error {
	_, perAccount, total := c.snapshotResidents(key)
	for _, a := range []arena{arenaHost, arenaDevice} {
		want := incoming.of(a)
		if want <= 0 {
			continue
		}
		if limit := tenant.of(a); limit > 0 && perAccount[account].of(a)+want > limit {
			return moerr.NewInternalErrorNoCtxf(
				"index cache is full: loading %q needs %d more %s bytes, but account %d already holds "+
					"%d of its %d byte budget and nothing idle is left to reclaim -- retry, or raise the "+
					"per-account cache budget",
				key, want, a, account, perAccount[account].of(a), limit)
		}
		if limit := sys.of(a); limit > 0 && total.of(a)+want > limit {
			return moerr.NewInternalErrorNoCtxf(
				"index cache is full: loading %q needs %d more %s bytes, but this CN already holds %d of "+
					"its %d byte budget and nothing idle is left to reclaim -- retry, or raise the "+
					"CN-wide cache budget",
				key, want, a, total.of(a), limit)
		}
	}
	return nil
}

// chargeAndEnforce records what a freshly loaded entry costs, then brings the cache back under
// the caps. Called once per successful load, from the miss path only.
func (c *VectorIndexCache) chargeAndEnforce(sqlproc *sqlexec.SqlProcess, key string, entry *VectorIndexSearch) {
	// The size was captured under the entry lock by Load (see captureSize); read the atomics
	// rather than the algorithm, which a concurrent eviction may be tearing down.
	if hasSession(sqlproc) {
		if account, err := sqlproc.EffectiveAccountID(); err == nil {
			entry.accountID.Store(account)
		}
	}
	// A load with no tenant in context is charged to the SYS account, the same bucket
	// idxcron's background work already reports (idxcron/cmd.go builds its SqlContext with
	// catalog.System_Account). Leaving it unattributed would exempt it from every cap.

	tenant, sys, lerr := c.limits(sqlproc)
	if lerr != nil {
		// The load already happened; failing here would not un-spend it. Enforcement is simply
		// not possible until the operator sets a budget, which the error names.
		logutil.Warnf("[veccache] cannot enforce the cache budget: %v", lerr)
		return
	}
	if tenant.unset() && sys.unset() {
		return
	}
	c.enforce(entry.accountID.Load(), tenant, sys, key)
}

// limits returns the calling tenant's caps and the CN-wide SYS caps, host and device. A 0 from
// either source means "not set by an operator", and when NEITHER is set the SYS pair resolves to
// the arena ceilings below rather than to unlimited -- so an unreadable variable still yields a
// governed cache. Unreadable never FAILS the load: the governor is a memory policy, not a
// correctness gate, and must not fail a query because a variable could not be read.
func (c *VectorIndexCache) limits(sqlproc *sqlexec.SqlProcess) (tenant, sys caps, err error) {
	tenant, sys = c.tenantCacheLimits(sqlproc), c.sysCacheLimit(sqlproc)

	// Resolved PER ARENA, not per pair. enforce() skips an arena whose tenant and sys caps are
	// both <= 0, so each arena has to reach a positive number on its own; a pair-wide test
	// leaves `set global max_index_cache_size = 0` unbounded whenever the OTHER arena happens
	// to be set, and since max_gpu_index_cache_size now defaults to a non-zero ceiling that is
	// the ordinary case rather than a corner one.
	//
	// This covers the cases that all have to end up bounded:
	//   * a sessionless load (idxcron, an internal rebuild), which has no resolver;
	//   * an explicit `set global max_index_cache_size = 0`, on either arena, at either scope;
	//   * an UPGRADED cluster, which is the case the variable default alone cannot reach --
	//     the value is persisted in mo_mysql_compatibility_mode at bootstrap, so a cluster
	//     created before the default changed keeps its stored 0 forever and would otherwise
	//     stay unbounded no matter what the code default says.
	//
	// So 0 means "no limit I chose", not "no limit at all": the accounting always runs and every
	// entry stays evictable. What an unconfigured arena resolves TO is a share of what this
	// machine actually has (see automaticCachePercent) rather than a fixed ceiling -- a constant
	// large enough to never refuse a real deployment is also a constant that never binds, which
	// would leave the admission check below unreachable.
	// Per ARENA, and on the SYS value ALONE. Gating this on the tenant too would let a tenant
	// setting any value at all leave the CN-wide budget at 0 for that arena -- i.e. bypass the
	// CN limit by naming a bigger one of its own. The tenant cap is an ADDITIONAL bound that
	// enforce applies alongside this one, never a replacement for it.
	// Derive ONLY the arenas the operator has not already set, and surface a sizing error only
	// for an arena that actually needed deriving.
	//
	// Both halves matter. Returning the error unconditionally would refuse every load even when
	// both variables are configured -- so the remedy the error names ("set max_index_cache_size")
	// would not work, and since the probe result is memoized for the process, the CN would be
	// bricked until restart. Keeping the two arenas' errors apart matters just as much: a GPU
	// that cannot be queried says nothing about host memory, and joining them would refuse every
	// hnsw and fulltext2 load on a CN whose RAM is perfectly well known.
	if sys.host > 0 && sys.device > 0 {
		return tenant, sys, nil
	}
	auto, hostErr, deviceErr := c.defaultLimits()
	if sys.host <= 0 {
		if hostErr != nil {
			return tenant, sys, hostErr
		}
		sys.host = auto.host
	}
	if sys.device <= 0 {
		if deviceErr != nil {
			return tenant, sys, deviceErr
		}
		sys.device = auto.device
	}
	return tenant, sys, nil
}

// accountCacheLimit reads ONE account's caps from the catalog, memoized per account for
// sysLimitTTL. Used for a cross-account snapshot read, where the resident bytes belong to the
// snapshot's owning tenant and the session resolver -- which answers for the caller -- cannot
// produce that tenant's cap.
//
// Deliberately simpler than sysCacheLimit: a miss returns the unconfigured caps rather than
// holding a lock across the query. The SYS read is on every miss and must not stall a cold
// start; this one is on the cross-account snapshot path only, so a concurrent duplicate query
// is cheaper than the machinery to avoid it.
func (c *VectorIndexCache) accountCacheLimit(sqlproc *sqlexec.SqlProcess, accountID uint32) caps {
	cnUUID := sqlproc.GetService()
	if cnUUID == "" {
		return caps{}
	}

	var last caps
	if v, ok := c.acctLimits.Load(accountID); ok {
		e := v.(acctLimitEntry)
		if time.Since(e.fetched) < sysLimitTTL {
			return e.value
		}
		last = e.value
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := runSysSql(ctx, cnUUID, accountID, "", accountLimitSQL(accountID))
	if err != nil {
		logutil.Warnf("index cache governor: reading account %d cache caps failed: %v", accountID, err)
		// KEEP the last known good value, like the SYS read does: returning caps{} here would
		// let a transient catalog error silently unbound a tenant that HAS a cap, for as long
		// as the catalog stays unreachable (every window would re-stamp the zero). Only the
		// attempt time is refreshed, so an unreachable catalog is retried at the TTL cadence
		// rather than on every miss.
		c.acctLimits.Store(accountID, acctLimitEntry{value: last, fetched: time.Now()})
		return last
	}
	defer res.Close()

	value := capsFromVarRows(res)
	c.acctLimits.Store(accountID, acctLimitEntry{value: value, fetched: time.Now()})
	return value
}

// acctLimitEntry is one account's memoized caps plus the time of the last ATTEMPT.
type acctLimitEntry struct {
	value   caps
	fetched time.Time
}

// hasSession reports whether sqlproc can be asked anything at all. SqlProcess delegates to
// whichever of Proc / SqlCtx is set and dereferences it unguarded, so a zero SqlProcess -- which
// the cache is legitimately called with, e.g. an internal load with no session -- would panic in
// GetService and GetAccountID.
func hasSession(sqlproc *sqlexec.SqlProcess) bool {
	return sqlproc != nil && (sqlproc.Proc != nil || sqlproc.SqlCtx != nil)
}

// tenantCacheLimits reads both caps for the CALLING account through the request's own
// resolver, at global scope so SET GLOBAL takes effect without a reconnect.
func (c *VectorIndexCache) tenantCacheLimits(sqlproc *sqlexec.SqlProcess) caps {
	if !hasSession(sqlproc) {
		return caps{}
	}
	// A cross-account snapshot read executes as the snapshot's tenant, so its cap is that
	// tenant's -- and the session resolver cannot produce it, because it answers for the
	// caller. Read that account's row from the catalog instead, memoized like the SYS read.
	if effective, err := sqlproc.EffectiveAccountID(); err == nil {
		if caller, cerr := sqlproc.GetAccountID(); cerr == nil && caller != effective {
			return c.accountCacheLimit(sqlproc, effective)
		}
	}
	resolve := sqlproc.GetResolveVariableFunc()
	if resolve == nil {
		return caps{}
	}
	return caps{
		host:   resolveByteVar(resolve, maxIndexCacheSizeVar),
		device: resolveByteVar(resolve, maxGpuIndexCacheSizeVar),
	}
}

// resolveByteVar reads one global-scope byte budget, 0 for anything it cannot read as a
// non-negative int64.
func resolveByteVar(resolve func(string, bool, bool) (interface{}, error), name string) int64 {
	val, err := resolve(name, true, true)
	if err != nil || val == nil {
		return 0
	}
	n, ok := val.(int64)
	if !ok || n < 0 {
		return 0
	}
	return n
}

// sysCacheLimit reads the SYS account's cap, memoized for sysLimitTTL.
//
// The read runs as the SYS account on a FRESH context: sqlexec.RunSqlAutoCommit rebinds
// defines.TenantIDKey to the account it is given, so the caller's tenant-bound context is never
// reused to read another account's catalog row.
func (c *VectorIndexCache) sysCacheLimit(sqlproc *sqlexec.SqlProcess) caps {
	if !hasSession(sqlproc) {
		return caps{}
	}
	cnUUID := sqlproc.GetService()
	if cnUUID == "" {
		return caps{}
	}

	c.sysLimit.mu.Lock()
	if !c.sysLimit.fetched.IsZero() && time.Since(c.sysLimit.fetched) < sysLimitTTL {
		value := c.sysLimit.value
		c.sysLimit.mu.Unlock()
		return value
	}
	// Stamp the attempt and claim the refresh, so a failing catalog is retried at the same
	// cadence as a healthy one rather than on every miss, and only one caller queries per
	// window.
	firstFetch := c.sysLimit.fetched.IsZero()
	c.sysLimit.fetched = time.Now()
	last := c.sysLimit.value

	// A REFRESH releases the lock across the query: every concurrent miss already has a
	// last-known cap to use, so parking them behind a catalog that can take the full timeout
	// buys nothing.
	//
	// The FIRST fetch keeps it. There is no last-known value yet -- c.sysLimit.value is the
	// zero caps, which every reader would interpret as "unlimited" -- so releasing here would
	// let every concurrent miss bypass the governor entirely until the query returns. That
	// window is CN startup, when the cache is cold and loads arrive together, i.e. exactly
	// when the cap matters most. Waiting for the real value is the lesser cost.
	if !firstFetch {
		c.sysLimit.mu.Unlock()
	} else {
		defer c.sysLimit.mu.Unlock()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := runSysSql(ctx, cnUUID, catalog.System_Account, "", sysLimitSQL)
	if err != nil {
		// Keep the last known good value; a catalog blip must not unbound the cache. With no
		// value ever read, that is the zero caps -- which limits() then resolves to the arena
		// ceiling, so an unreadable catalog leaves the cache governed rather than unlimited.
		logutil.Warnf("index cache governor: reading the sys index cache caps failed: %v", err)
		return last
	}
	defer res.Close()

	value := capsFromVarRows(res)
	if firstFetch {
		c.sysLimit.value = value // still holding the lock
	} else {
		c.sysLimit.mu.Lock()
		c.sysLimit.value = value
		c.sysLimit.mu.Unlock()
	}
	return value
}

// snapshotResidents lists every charged, live entry with its account and coldness. Entries
// mid-load or already claimed for eviction are skipped: they hold no charged bytes yet, or
// their bytes are already on their way back.
func (c *VectorIndexCache) snapshotResidents(protect string) (list []resident, perAccount map[uint32]usage, total usage) {
	perAccount = make(map[uint32]usage)
	c.IndexMap.Range(func(key, value any) bool {
		k, ok := key.(string)
		if !ok {
			return true
		}
		entry, ok := value.(*VectorIndexSearch)
		if !ok || entry.evicting.Load() || entry.Status.Load() != STATUS_LOADED {
			return true
		}
		size := usage{host: entry.hostBytes.Load(), device: entry.deviceBytes.Load()}
		if size.host == 0 && size.device == 0 {
			return true
		}
		account := entry.accountID.Load()
		acc := perAccount[account]
		acc.host += size.host
		acc.device += size.device
		perAccount[account] = acc
		total.host += size.host
		total.device += size.device
		if k != protect {
			list = append(list, resident{key: k, entry: entry, account: account,
				expireAt: entry.ExpireAt.Load(), size: size})
		}
		return true
	})
	return list, perAccount, total
}

// enforce evicts coldest-first until the charging account is under its own cap and the CN is
// under the SYS cap, in both arenas. protect is the key just loaded, never a victim.
// noAskingAccount is the account id enforce() is given when no load triggered the pass, so its
// "the account asking for room pays first" sub-pass matches no resident and coldest-first
// ordering applies to the whole cache. Real account ids are small and dense; this one cannot
// collide with one.
const noAskingAccount = ^uint32(0)

func (c *VectorIndexCache) enforce(account uint32, tenant, sys caps, protect string) {
	for _, a := range []arena{arenaHost, arenaDevice} {
		if tenant.of(a) <= 0 && sys.of(a) <= 0 {
			continue
		}
		// Re-snapshot per arena rather than reusing one pre-pass. An entry evicted by the
		// PREVIOUS arena is gone, but it still carries bytes in this one, and reclaim gives
		// it no credit here: evictEntry returns false for an entry already claimed, so the
		// loop skips it without decrementing. A shared snapshot therefore over-states this
		// arena's usage by exactly the bytes the other arena just freed, and evicts a warm
		// index to make room that is already free.
		list, perAccount, total := c.snapshotResidents(protect)
		if len(list) == 0 {
			return
		}
		// Coldest first. ExpireAt slides forward on every search, so it is the cache's
		// least-recently-used ordering already.
		sort.Slice(list, func(i, j int) bool { return list[i].expireAt < list[j].expireAt })

		var freed int64
		if tenantLimit := tenant.of(a); tenantLimit > 0 {
			freed = c.reclaim(list, a, tenantLimit, perAccount[account].of(a), func(r resident) bool {
				return r.account == account
			})
		}
		if sysLimit := sys.of(a); sysLimit > 0 {
			// The tenant pass already gave bytes back to the CN total; charging them twice
			// would evict a second, innocent tenant's entries for room that is already free.
			used := total.of(a) - freed
			// The account asking for room pays for it first. Coldest-first alone lets a
			// tenant that floods the CN evict a quiet neighbour's older entry before its own
			// -- the CN-wide cap still held, but the cost of holding it landed on the wrong
			// tenant. Only once this account has nothing left to give does the pass widen.
			used -= c.reclaim(list, a, sysLimit, used, func(r resident) bool {
				return r.account == account
			})
			c.reclaim(list, a, sysLimit, used, func(resident) bool { return true })
		}
	}
}

// parseByteLimit reads the catalog's text representation of a byte budget. A value that does
// not parse is treated as unset by the caller rather than guessed at.
func parseByteLimit(raw string) (int64, error) {
	n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0, err
	}
	if n < 0 {
		return 0, moerr.NewInternalErrorNoCtx("negative index cache byte budget")
	}
	return n, nil
}

// reclaim evicts entries matching eligible, coldest first, until used drops to limit in arena
// a. Only entries that actually hold bytes in that arena are taken -- evicting a host-only
// index to relieve VRAM pressure would free nothing and lose a warm index for it.
// It returns the bytes it freed in that arena.
func (c *VectorIndexCache) reclaim(list []resident, a arena, limit, used int64, eligible func(resident) bool) int64 {
	// IDLE VICTIMS ONLY. A busy entry is a live request, and it wins.
	//
	// This is an overloaded HTTP server, not a scheduler: when the cache cannot make room, the
	// NEW caller is refused (see makeRoom) and the in-flight ones are left alone. Reclaiming an
	// idle entry is the cache doing its job; taking one with a search in flight is preempting
	// work that is already running to serve work that has not started -- and it costs the
	// newcomer the wait anyway, because evictEntry destroys synchronously and Destroy takes the
	// victim's write lock, so the miss queues behind the very search it interrupted.
	//
	// The old second pass did exactly that: if every idle candidate was exhausted it went back
	// and took busy ones. That kept the cache under its limit at the cost of killing live
	// queries for an arrival, which is the wrong trade for a server.
	before := c.evictions.Load()
	freed := c.reclaimPass(list, a, limit, used, eligible, true)
	if n := c.evictions.Load() - before; n > 0 {
		logutil.Infof("index cache governor: reclaimed %d bytes from %d idle %s entries to stay under %d bytes",
			freed, n, a, limit)
	}
	return freed
}

func (c *VectorIndexCache) reclaimPass(
	list []resident, a arena, limit, used int64, eligible func(resident) bool, idleOnly bool,
) int64 {
	var freed int64
	for i := range list {
		if used <= limit {
			return freed
		}
		r := list[i]
		if r.size.of(a) == 0 || !eligible(r) {
			continue
		}
		if idleOnly && !r.entry.idle() {
			continue
		}
		reason := fmt.Sprintf("%s_cache_size_limit", a)
		limitVar := maxIndexCacheSizeVar
		if a == arenaDevice {
			limitVar = maxGpuIndexCacheSizeVar
		}
		// A false return means someone else already claimed the entry -- housekeeping, a
		// stale sweep, or the other arena's pass. Its bytes are on their way back either
		// way, but they are not ours to count.
		if !c.evictEntry(r.key, r.entry, reason) {
			continue
		}
		used -= r.size.of(a)
		freed += r.size.of(a)
		c.evictions.Add(1)
		c.evictedBytes.Add(r.size.of(a))
		// Per-victim detail is DEBUG: two indexes alternating under a tight cap evict on
		// every miss, and one INFO line per victim turns a steady state into a log storm.
		// The pass logs one aggregated line instead, and the counters below are the thing
		// to alert on.
		logutil.Debugf("index cache governor: evicted %q (account %d, %s %d bytes) to stay under %s of %d bytes",
			r.key, r.account, a, r.size.of(a), limitVar, limit)
	}
	return freed
}

// capsFromVarRows decodes (variable_name, variable_value) rows into caps. A name the account
// never SET has no row and stays 0.
func capsFromVarRows(res executor.Result) caps {
	var value caps
	for _, bat := range res.Batches {
		if bat == nil {
			continue
		}
		for i := 0; i < bat.RowCount(); i++ {
			n, perr := parseByteLimit(bat.Vecs[1].GetStringAt(i))
			if perr != nil {
				continue
			}
			switch strings.TrimSpace(bat.Vecs[0].GetStringAt(i)) {
			case maxIndexCacheSizeVar:
				value.host = n
			case maxGpuIndexCacheSizeVar:
				value.device = n
			}
		}
	}
	return value
}

// EvictionStats reports how much the governor has reclaimed since start: entries evicted and
// bytes freed, across both arenas. Per-victim detail is logged at DEBUG, so these counters are
// what an operator watches to see whether a cap is binding and how hard.
func (c *VectorIndexCache) EvictionStats() (entries int64, bytes int64) {
	return c.evictions.Load(), c.evictedBytes.Load()
}

// enforceMemoizedCaps applies the LAST KNOWN CN-wide caps from the housekeeping ticker, so a
// lowered SET GLOBAL takes effect on a cache that is merely warm.
//
// Without it the caps are consulted only on a miss, and a hot working set renews its TTL
// indefinitely: an operator lowering max_index_cache_size on a busy CN would see nothing shrink
// until traffic happened to miss. The 15s memo TTL does not help -- it bounds how stale the
// VALUE is, not when it is next applied.
//
// Uses the memoized value only: housekeeping has no session, so it cannot read the catalog or
// resolve a tenant's variables. That makes this the CN-wide (SYS) bound only, and only once the
// first miss has populated it; per-tenant caps still apply at the next miss for that tenant.
// A cache that has never been asked for a limit is left alone rather than enforced against the
// absolute ceiling, which no housekeeping pass could usefully act on anyway.
func (c *VectorIndexCache) enforceMemoizedCaps() {
	c.sysLimit.mu.Lock()
	sys := c.sysLimit.value
	fetched := c.sysLimit.fetched
	c.sysLimit.mu.Unlock()

	if fetched.IsZero() || sys.unset() {
		return
	}
	// NOBODY is asking for room on a housekeeping pass, so no account should pay first.
	// enforce()'s pay-first sub-pass filters residents on this id; passing System_Account made
	// account-0 entries (a sys session's, and every sessionless idxcron load, which is charged
	// there) the only ones a binding CN-wide cap ever reclaimed, however warm, while a colder
	// tenant's entries survived -- inverting the coldest-first ordering housekeeping exists to
	// apply. A sentinel no resident can carry makes that sub-pass a no-op and leaves the
	// widened pass to reclaim strictly coldest-first. enforce() protects nothing here (no key
	// is being loaded) and skips entries already claimed for eviction.
	c.enforce(noAskingAccount, caps{}, sys, "")
}
