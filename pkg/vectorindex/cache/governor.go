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
// All four budgets apply. Zero selects a machine-sized CN retention target;
// zero on a tenant adds no tenant-specific restriction. Host and device bytes
// are independent: evicting host-only data cannot relieve device pressure.
//
// Reclamation claims idle entries atomically and never waits for busy queries.
// A loader keeps its entry through its first search. If reclaim cannot make
// room, the entry is transient and retired after its readers finish. This is
// a retention policy, not a reservation of concurrent loaders or SQL workspace.

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
	// Global-scope operator overrides; zero selects automatic sizing.
	maxIndexCacheSizeVar    = "max_index_cache_size"
	maxGpuIndexCacheSizeVar = "max_gpu_index_cache_size"

	// Catalog reads are memoized across misses and background maintenance.
	sysLimitTTL = 15 * time.Second

	// Saturation guards for automatic sizing, NOT the default cache budgets.
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
	mu     sync.Mutex
	cnUUID string // service identity only; never retain a query/transaction/session
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
func (c *VectorIndexCache) makeRoom(sqlproc *sqlexec.SqlProcess, key string, entry *VectorIndexSearch) {
	// Preload published its estimate to these atomics under the entry lock; never call
	// GetIndexSize from here, where no lock is held and Destroy may be nilling algo state.
	host, device := entry.hostBytes.Load(), entry.deviceBytes.Load()
	if host <= 0 && device <= 0 {
		return
	}
	// The account that OWNS the bytes, not the one that asked: a cross-account snapshot read
	// executes as the snapshot's tenant, so the resident entry belongs to that tenant's budget.
	account := uint32(catalog.System_Account)
	if hasSession(sqlproc) {
		if a, err := sqlproc.EffectiveAccountID(); err == nil {
			account = a
		}
	}
	tenant, sys := c.limits(sqlproc)
	if tenant.unset() && sys.unset() {
		return
	}
	// Reclaim against caps reduced by what is about to arrive, so the room freed is room the
	// incoming index can actually occupy.
	c.enforce(account, tenant.less(caps{host: host, device: device}),
		sys.less(caps{host: host, device: device}), key)
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

	tenant, sys := c.limits(sqlproc)
	if tenant.unset() && sys.unset() {
		return
	}
	c.enforce(entry.accountID.Load(), tenant, sys, key)
	// The cache target cannot prevent execution of an oversized query, and a
	// miss must not wait on a busy victim. Such an entry may serve its current
	// readers, but must not turn temporary query memory into retained cache
	// memory. The last search to release its read lock retires it.
	_, perAccount, total := c.snapshotResidents("")
	owned := perAccount[entry.accountID.Load()]
	if exceedsCaps(owned, tenant) || exceedsCaps(total, sys) {
		entry.retireAfterSearch.Store(true)
	}
}

func exceedsCaps(used usage, limit caps) bool {
	return (limit.host > 0 && used.host > limit.host) ||
		(limit.device > 0 && used.device > limit.device)
}

// limits resolves tenant overrides and the independent CN-wide retention target.
// Missing or unreadable SYS values use automatic sizing; a tenant cannot bypass
// that target by setting a larger local limit. Catalog errors do not fail SQL.
func (c *VectorIndexCache) limits(sqlproc *sqlexec.SqlProcess) (tenant, sys caps) {
	tenant, sys = c.tenantCacheLimits(sqlproc), c.sysCacheLimit(sqlproc)

	if sys.host <= 0 {
		sys.host = c.defaultLimits().host
	}
	if sys.device <= 0 {
		sys.device = c.defaultLimits().device
	}
	return tenant, sys
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
	c.sysLimit.cnUUID = cnUUID
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
	// The FIRST fetch keeps it: learn an operator's potentially smaller override
	// before concurrent cold loads start using the automatic target. This wait is
	// bounded by the catalog timeout below; later refreshes never park misses.
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
		// value ever read, limits() resolves the zero caps to automatic sizing.
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
		if (tenant.of(a) <= 0 || perAccount[account].of(a) <= tenant.of(a)) &&
			(sys.of(a) <= 0 || total.of(a) <= sys.of(a)) {
			continue
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
	before := c.evictions.Load()
	freed := c.reclaimPass(list, a, limit, used, eligible)
	if n := c.evictions.Load() - before; n > 0 {
		logutil.Debugf("index cache governor: reclaimed %d bytes from %d %s entries to stay under %d bytes",
			freed, n, a, limit)
	}
	return freed
}

func (c *VectorIndexCache) reclaimPass(
	list []resident, a arena, limit, used int64, eligible func(resident) bool,
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
		reason := fmt.Sprintf("%s_cache_size_limit", a)
		limitVar := maxIndexCacheSizeVar
		if a == arenaDevice {
			limitVar = maxGpuIndexCacheSizeVar
		}
		// A false return means someone else already claimed the entry -- housekeeping, a
		// stale sweep, or the other arena's pass. Its bytes are on their way back either
		// way, but they are not ours to count.
		if !c.evictIdle(r.key, r.entry, reason) {
			continue
		}
		used -= r.size.of(a)
		freed += r.size.of(a)
		c.evictions.Add(1)
		c.evictedBytes.Add(r.size.host + r.size.device)
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

// enforceMemoizedCaps applies the last successfully refreshed SYS target.
// refreshCacheLimits supplies fresh catalog values without needing a cache miss;
// tenant-specific changes take effect on that tenant's next miss.
func (c *VectorIndexCache) enforceMemoizedCaps() {
	c.sysLimit.mu.Lock()
	sys := c.sysLimit.value
	fetched := c.sysLimit.fetched
	c.sysLimit.mu.Unlock()

	if fetched.IsZero() {
		return
	}
	if sys.host <= 0 {
		sys.host = c.defaultLimits().host
	}
	if sys.device <= 0 {
		sys.device = c.defaultLimits().device
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

// refreshCacheLimits runs in the existing maintenance task. A warm cache has
// no misses to refresh sysLimit, so replaying its stale value cannot implement
// SET GLOBAL. Keep only the service ID and use a fresh, bounded SYS read.
func (c *VectorIndexCache) refreshCacheLimits() {
	c.sysLimit.mu.Lock()
	sid := c.sysLimit.cnUUID
	c.sysLimit.mu.Unlock()
	if sid == "" || c.exited.Load() {
		return
	}
	c.sysCacheLimit(&sqlexec.SqlProcess{SqlCtx: &sqlexec.SqlContext{
		Ctx: context.Background(), CNUuid: sid, AccountId: catalog.System_Account,
	}})
	if !c.exited.Load() {
		c.enforceMemoizedCaps()
	}
}
