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
// max_index_cache_size is a BYTE budget on resident index bytes, read per account:
//
//   - its value on the SYS account (id 0) caps every tenant's indexes on this CN together
//   - its value on a tenant caps that tenant alone
//
// Both apply; whichever binds first evicts. 0 means no limit and is the default, so an
// unconfigured deployment pays nothing: with both limits 0 the governor returns before it
// walks the map.
//
// Host and device bytes are budgeted SEPARATELY against the same number, never summed. A CN
// has far more RAM than VRAM, so one conflated total would bound neither arena; charging each
// arena its own sum means the cap is whichever of the two fills first.
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
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

const (
	// maxIndexCacheSizeVar is the byte budget, declared in pkg/frontend/variables.go with
	// Scope ScopeGlobal and Default int64(0).
	maxIndexCacheSizeVar = "max_index_cache_size"

	// sysLimitTTL bounds how stale the SYS account's value may be. The read costs one
	// auto-commit SQL and only ever runs on a cache MISS, which has just paid for a full
	// index load, so the cadence is about bounding SET GLOBAL latency, not query cost.
	sysLimitTTL = 15 * time.Second
)

// sysLimitSQL reads the SYS account's max_index_cache_size straight from the catalog. The
// session resolver cannot answer this: it resolves for the CALLING tenant, and the CN-wide cap
// lives on account 0.
var sysLimitSQL = fmt.Sprintf(
	"select variable_value from mo_catalog.mo_mysql_compatibility_mode "+
		"where account_id = %d and system_variables = true and variable_name = '%s'",
	catalog.System_Account, maxIndexCacheSizeVar)

// runSysSql is indirected so the governor's catalog read is testable without a CN.
var runSysSql = sqlexec.RunSqlAutoCommit

// sysLimitCache memoizes the SYS account's cap for sysLimitTTL. A failed read keeps the last
// known good value rather than falling open to "no limit", so a transient catalog error cannot
// silently unbound the cache.
type sysLimitCache struct {
	mu      sync.Mutex
	value   int64
	fetched time.Time
	valid   bool
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

// chargeAndEnforce records what a freshly loaded entry costs, then brings the cache back under
// the caps. Called once per successful load, from the miss path only.
func (c *VectorIndexCache) chargeAndEnforce(sqlproc *sqlexec.SqlProcess, key string, entry *VectorIndexSearch) {
	host, device := entry.Algo.GetIndexSize()
	entry.hostBytes.Store(host)
	entry.deviceBytes.Store(device)
	if hasSession(sqlproc) {
		if account, err := sqlproc.GetAccountID(); err == nil {
			entry.accountID.Store(account)
		}
	}
	// A load with no tenant in context is charged to the SYS account, the same bucket
	// idxcron's background work already reports (idxcron/cmd.go builds its SqlContext with
	// catalog.System_Account). Leaving it unattributed would exempt it from every cap.

	tenant, sys := c.limits(sqlproc)
	if tenant <= 0 && sys <= 0 {
		return
	}
	c.enforce(entry.accountID.Load(), tenant, sys, key)
}

// limits returns the calling tenant's cap and the CN-wide SYS cap, both in bytes, 0 meaning
// unlimited. Unreadable resolves to unlimited: the governor is a memory policy, not a
// correctness gate, and must never fail a query because a variable could not be read.
func (c *VectorIndexCache) limits(sqlproc *sqlexec.SqlProcess) (tenant, sys int64) {
	return tenantCacheLimit(sqlproc), c.sysCacheLimit(sqlproc)
}

// hasSession reports whether sqlproc can be asked anything at all. SqlProcess delegates to
// whichever of Proc / SqlCtx is set and dereferences it unguarded, so a zero SqlProcess -- which
// the cache is legitimately called with, e.g. an internal load with no session -- would panic in
// GetService and GetAccountID.
func hasSession(sqlproc *sqlexec.SqlProcess) bool {
	return sqlproc != nil && (sqlproc.Proc != nil || sqlproc.SqlCtx != nil)
}

// tenantCacheLimit reads max_index_cache_size for the CALLING account through the request's
// own resolver, at global scope so SET GLOBAL takes effect without a reconnect.
func tenantCacheLimit(sqlproc *sqlexec.SqlProcess) int64 {
	if !hasSession(sqlproc) {
		return 0
	}
	resolve := sqlproc.GetResolveVariableFunc()
	if resolve == nil {
		return 0
	}
	val, err := resolve(maxIndexCacheSizeVar, true, true)
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
func (c *VectorIndexCache) sysCacheLimit(sqlproc *sqlexec.SqlProcess) int64 {
	if !hasSession(sqlproc) {
		return 0
	}
	cnUUID := sqlproc.GetService()
	if cnUUID == "" {
		return 0
	}

	c.sysLimit.mu.Lock()
	defer c.sysLimit.mu.Unlock()
	if c.sysLimit.valid && time.Since(c.sysLimit.fetched) < sysLimitTTL {
		return c.sysLimit.value
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := runSysSql(ctx, cnUUID, catalog.System_Account, "", sysLimitSQL)
	if err != nil {
		// Keep the last known good value; a catalog blip must not unbound the cache.
		logutil.Warnf("index cache governor: reading sys %s failed: %v", maxIndexCacheSizeVar, err)
		return c.sysLimit.value
	}
	defer res.Close()

	value := int64(0) // no row means the SYS account never SET it: unlimited
	for _, bat := range res.Batches {
		for i := 0; i < bat.RowCount(); i++ {
			if n, perr := parseByteLimit(bat.Vecs[0].GetStringAt(i)); perr == nil {
				value = n
			}
		}
	}
	c.sysLimit.value, c.sysLimit.fetched, c.sysLimit.valid = value, time.Now(), true
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
func (c *VectorIndexCache) enforce(account uint32, tenant, sys int64, protect string) {
	list, perAccount, total := c.snapshotResidents(protect)
	if len(list) == 0 {
		return
	}
	// Coldest first. ExpireAt slides forward on every search, so it is the cache's
	// least-recently-used ordering already.
	sort.Slice(list, func(i, j int) bool { return list[i].expireAt < list[j].expireAt })

	for _, a := range []arena{arenaHost, arenaDevice} {
		var freed int64
		if tenant > 0 {
			freed = c.reclaim(list, a, tenant, perAccount[account].of(a), func(r resident) bool {
				return r.account == account
			})
		}
		if sys > 0 {
			// The tenant pass already gave bytes back to the CN total; charging them twice
			// would evict a second, innocent tenant's entries for room that is already free.
			c.reclaim(list, a, sys, total.of(a)-freed, func(resident) bool { return true })
		}
	}
}

// parseByteLimit reads the catalog's text representation of max_index_cache_size. A value that
// does not parse is treated as unset by the caller rather than guessed at.
func parseByteLimit(raw string) (int64, error) {
	n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0, err
	}
	if n < 0 {
		return 0, moerr.NewInternalErrorNoCtx("negative " + maxIndexCacheSizeVar)
	}
	return n, nil
}

// reclaim evicts entries matching eligible, coldest first, until used drops to limit in arena
// a. Only entries that actually hold bytes in that arena are taken -- evicting a host-only
// index to relieve VRAM pressure would free nothing and lose a warm index for it.
// It returns the bytes it freed in that arena.
func (c *VectorIndexCache) reclaim(list []resident, a arena, limit, used int64, eligible func(resident) bool) int64 {
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
		// A false return means someone else already claimed the entry -- housekeeping, a
		// stale sweep, or the other arena's pass. Its bytes are on their way back either
		// way, but they are not ours to count.
		if !c.evictEntry(r.key, r.entry, reason) {
			continue
		}
		used -= r.size.of(a)
		freed += r.size.of(a)
		logutil.Infof("index cache governor: evicted %q (account %d, %s %d bytes) to stay under %s of %d bytes",
			r.key, r.account, a, r.size.of(a), maxIndexCacheSizeVar, limit)
	}
	return freed
}
