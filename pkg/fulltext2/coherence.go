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

package fulltext2

import (
	"strconv"
	"strings"
	"sync"
	"time"

	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
)

const (
	cacheKeyPrefix                      = "fulltext2:"
	defaultFenceRegistryInitialCapacity = 1024
	freshnessRecoveryInitialBackoff     = 100 * time.Millisecond
	freshnessRecoveryMaxBackoff         = 30 * time.Second
)

// CacheIdentity is the stable tenant and lifecycle scoped identity of one
// FULLTEXT2 storage generation. Hidden table names make DROP/recreate distinct.
type CacheIdentity struct {
	AccountID     uint32
	Database      string
	StorageTable  string
	MetadataTable string
}

func appendIdentityPart(b *strings.Builder, value string) {
	b.WriteString(strconv.Itoa(len(value)))
	b.WriteByte(':')
	b.WriteString(value)
}

// Key returns a collision-free process-local cache key.
func (i CacheIdentity) Key() string {
	var b strings.Builder
	b.Grow(len(cacheKeyPrefix) + len(i.Database) + len(i.StorageTable) + len(i.MetadataTable) + 40)
	b.WriteString(cacheKeyPrefix)
	b.WriteString(strconv.FormatUint(uint64(i.AccountID), 10))
	b.WriteByte(':')
	appendIdentityPart(&b, i.Database)
	appendIdentityPart(&b, i.StorageTable)
	appendIdentityPart(&b, i.MetadataTable)
	return b.String()
}

func (i CacheIdentity) TableConfig() TableConfig {
	return TableConfig{AccountID: i.AccountID, DbName: i.Database, IndexTable: i.StorageTable, MetadataTable: i.MetadataTable}
}

// Generation is ordered lexicographically. A base rewrite therefore dominates
// a reset of the CDC tail chunk sequence.
type Generation struct {
	BaseTimestamp int64
	TailChunk     int64
}

func (g Generation) AtLeast(other Generation) bool {
	return g.BaseTimestamp > other.BaseTimestamp ||
		(g.BaseTimestamp == other.BaseTimestamp && g.TailChunk >= other.TailChunk)
}

func (g Generation) IsZero() bool { return g.BaseTimestamp == 0 && g.TailChunk == 0 }

type fenceEntry struct {
	required           Generation
	hasRequired        bool
	claiming           bool
	claimed            bool
	uncertain          bool
	uncertaintyVersion uint64
	recovering         bool
	retryAfter         time.Time
	backoff            time.Duration
}

type fenceRegistry struct {
	mu              sync.Mutex
	entries         map[string]*fenceEntry
	initialCapacity int
}

func newFenceRegistry(initialCapacity int) *fenceRegistry {
	if initialCapacity <= 0 {
		initialCapacity = defaultFenceRegistryInitialCapacity
	}
	return &fenceRegistry{
		entries:         make(map[string]*fenceEntry, initialCapacity),
		initialCapacity: initialCapacity,
	}
}

// install returns whether the caller owns the eviction claim and the monotonic
// current requirement. Exact lower bounds grow with the active cached identity
// set; housekeeping reclaims inactive entries only because every later cold
// load has a durable global-admission check.
func (r *fenceRegistry) install(id CacheIdentity, generation Generation) (bool, Generation, bool) {
	key := id.Key()
	r.mu.Lock()
	defer r.mu.Unlock()
	if entry, ok := r.entries[key]; ok {
		if entry.hasRequired && !generation.AtLeast(entry.required) {
			return false, entry.required, false
		}
		if entry.hasRequired && generation == entry.required {
			if entry.claimed || entry.claiming {
				return false, entry.required, false
			}
			entry.claiming = true
			return true, entry.required, false
		}
		entry.required = generation
		entry.hasRequired = true
		entry.claimed = false
		entry.claiming = true
		return true, entry.required, false
	}
	r.entries[key] = &fenceEntry{required: generation, hasRequired: true, claiming: true}
	return true, generation, false
}

func (r *fenceRegistry) finishClaim(id CacheIdentity, generation Generation) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.entries[id.Key()]
	if !ok || !entry.hasRequired || entry.required != generation || !entry.claiming {
		return false
	}
	entry.claiming = false
	entry.claimed = true
	return true
}

func (r *fenceRegistry) required(id CacheIdentity) Generation {
	generation, _ := r.lookupRequired(id)
	return generation
}

func (r *fenceRegistry) lookupRequired(id CacheIdentity) (Generation, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if entry := r.entries[id.Key()]; entry != nil && entry.hasRequired {
		return entry.required, true
	}
	return Generation{}, false
}

func (r *fenceRegistry) claimedAtLeast(id CacheIdentity, generation Generation) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := r.entries[id.Key()]
	return entry != nil && entry.hasRequired && entry.claimed && entry.required.AtLeast(generation)
}

func (r *fenceRegistry) markUncertain(id CacheIdentity) {
	key := id.Key()
	r.mu.Lock()
	entry := r.entries[key]
	if entry == nil {
		entry = &fenceEntry{}
		r.entries[key] = entry
	}
	entry.uncertaintyVersion++
	if !entry.uncertain {
		entry.retryAfter = time.Time{}
		entry.backoff = 0
	}
	entry.uncertain = true
	r.mu.Unlock()
}

func (r *fenceRegistry) clearUncertain(id CacheIdentity) {
	key := id.Key()
	r.mu.Lock()
	if entry := r.entries[key]; entry != nil {
		entry.uncertain = false
		entry.recovering = false
		entry.retryAfter = time.Time{}
		entry.backoff = 0
		if !entry.hasRequired && !entry.claiming && !entry.claimed {
			delete(r.entries, key)
		}
	}
	r.mu.Unlock()
}

func (r *fenceRegistry) uncertain(id CacheIdentity) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := r.entries[id.Key()]
	return entry != nil && entry.uncertain
}

// pruneInactive removes safety state only after the generic cache no longer has
// a loading or loaded object for the identity. A later cold load is still safe:
// Fulltext2Search performs a fresh auto-commit generation read before it may
// remain in the process-global cache.
func (r *fenceRegistry) pruneInactive(cachePresent func(string) bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, entry := range r.entries {
		if !entry.claiming && !entry.recovering && !cachePresent(key) {
			delete(r.entries, key)
		}
	}
}

func (r *fenceRegistry) clear() {
	r.mu.Lock()
	clear(r.entries)
	r.mu.Unlock()
}

func (r *fenceRegistry) beginRecovery(id CacheIdentity, now time.Time) (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := r.entries[id.Key()]
	if entry == nil || !entry.uncertain || entry.recovering || now.Before(entry.retryAfter) {
		return 0, false
	}
	entry.recovering = true
	return entry.uncertaintyVersion, true
}

func (r *fenceRegistry) finishRecovery(id CacheIdentity, version uint64, succeeded bool, now time.Time) {
	key := id.Key()
	r.mu.Lock()
	entry := r.entries[key]
	if entry == nil {
		r.mu.Unlock()
		return
	}
	entry.recovering = false
	if !entry.uncertain {
		r.mu.Unlock()
		return
	}
	if succeeded && entry.uncertaintyVersion == version {
		entry.uncertain = false
		entry.retryAfter = time.Time{}
		entry.backoff = 0
		if !entry.hasRequired && !entry.claiming && !entry.claimed {
			delete(r.entries, key)
		}
		r.mu.Unlock()
		return
	}
	if entry.backoff <= 0 {
		entry.backoff = freshnessRecoveryInitialBackoff
	} else {
		entry.backoff = min(entry.backoff*2, freshnessRecoveryMaxBackoff)
	}
	entry.retryAfter = now.Add(entry.backoff)
	r.mu.Unlock()
}

func (r *fenceRegistry) drop(id CacheIdentity) {
	r.mu.Lock()
	delete(r.entries, id.Key())
	r.mu.Unlock()
}

var localFences = newFenceRegistry(defaultFenceRegistryInitialCapacity)

func init() {
	veccache.RegisterLifecycleHook(func(shutdown bool) {
		if shutdown {
			localFences.clear()
			return
		}
		localFences.pruneInactive(func(key string) bool {
			_, ok := veccache.Cache.IndexMap.Load(key)
			return ok
		})
	})
}

type FencePublisher interface {
	Enqueue(CacheIdentity, Generation)
	Close()
}

// InstallGenerationFence installs the requirement and claims exact eviction.
// The overflow result is retained for the mixed-version RPC contract and is
// always false now that the exact registry grows with active identities.
func InstallGenerationFence(id CacheIdentity, generation Generation) (current Generation, claimed, overflow bool) {
	claim, current, _ := localFences.install(id, generation)
	if !claim {
		entry := localFences.required(id)
		return entry, localFences.claimedAtLeast(id, generation), false
	}
	veccache.Cache.ClaimRemoveWithReason(id.Key(), string(LoadMissGenerationChange))
	claimed = localFences.finishClaim(id, current)
	return current, claimed, false
}

// DropCacheIdentity removes all local state for an index lifecycle identity.
func DropCacheIdentity(id CacheIdentity) {
	veccache.Cache.Remove(id.Key())
	clearReusableLoadGeneration(id.TableConfig())
	localFences.drop(id)
}

func requiredGeneration(id CacheIdentity) (Generation, bool) {
	required, exists := localFences.lookupRequired(id)
	return required, exists
}

func markGenerationUncertain(id CacheIdentity) { localFences.markUncertain(id) }

func clearGenerationUncertain(id CacheIdentity) { localFences.clearUncertain(id) }

func requiresTransientLoad(id CacheIdentity) bool { return localFences.uncertain(id) }
