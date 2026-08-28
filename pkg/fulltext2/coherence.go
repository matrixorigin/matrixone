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
	"sync/atomic"
	"time"

	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
)

const (
	cacheKeyPrefix          = "fulltext2:"
	defaultFenceRegistryCap = 1024
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
	required Generation
	claiming bool
	claimed  bool
	lastUsed time.Time
}

type fenceRegistry struct {
	mu      sync.Mutex
	entries map[string]*fenceEntry
	max     int
}

func newFenceRegistry(max int) *fenceRegistry {
	if max <= 0 {
		max = defaultFenceRegistryCap
	}
	return &fenceRegistry{entries: make(map[string]*fenceEntry), max: max}
}

// reclaimClaimedLocked makes one slot without ever dropping an unfinished fence.
func (r *fenceRegistry) reclaimClaimedLocked() bool {
	var oldestKey string
	var oldest time.Time
	for key, entry := range r.entries {
		if !entry.claimed || entry.claiming {
			continue
		}
		if oldestKey == "" || entry.lastUsed.Before(oldest) {
			oldestKey, oldest = key, entry.lastUsed
		}
	}
	if oldestKey == "" {
		return false
	}
	delete(r.entries, oldestKey)
	return true
}

// install returns whether the caller owns the eviction claim, the monotonic
// current requirement, and whether capacity forced the global fail-closed path.
func (r *fenceRegistry) install(id CacheIdentity, generation Generation) (bool, Generation, bool) {
	now := time.Now()
	key := id.Key()
	r.mu.Lock()
	defer r.mu.Unlock()
	if entry, ok := r.entries[key]; ok {
		entry.lastUsed = now
		if !generation.AtLeast(entry.required) {
			return false, entry.required, false
		}
		if generation == entry.required {
			if entry.claimed || entry.claiming {
				return false, entry.required, false
			}
			entry.claiming = true
			return true, entry.required, false
		}
		entry.required = generation
		entry.claimed = false
		entry.claiming = true
		return true, entry.required, false
	}
	if len(r.entries) >= r.max && !r.reclaimClaimedLocked() {
		return false, Generation{}, true
	}
	r.entries[key] = &fenceEntry{required: generation, claiming: true, lastUsed: now}
	return true, generation, false
}

func (r *fenceRegistry) finishClaim(id CacheIdentity, generation Generation) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.entries[id.Key()]
	if !ok || entry.required != generation || !entry.claiming {
		return false
	}
	entry.claiming = false
	entry.claimed = true
	entry.lastUsed = time.Now()
	return true
}

func (r *fenceRegistry) required(id CacheIdentity) Generation {
	generation, _ := r.lookupRequired(id)
	return generation
}

func (r *fenceRegistry) lookupRequired(id CacheIdentity) (Generation, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if entry := r.entries[id.Key()]; entry != nil {
		entry.lastUsed = time.Now()
		return entry.required, true
	}
	return Generation{}, false
}

func (r *fenceRegistry) claimedAtLeast(id CacheIdentity, generation Generation) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := r.entries[id.Key()]
	return entry != nil && entry.claimed && entry.required.AtLeast(generation)
}

func (r *fenceRegistry) drop(id CacheIdentity) {
	r.mu.Lock()
	delete(r.entries, id.Key())
	r.mu.Unlock()
}

var localFences = newFenceRegistry(defaultFenceRegistryCap)
var coherenceEpoch atomic.Uint64

type FencePublisher interface {
	Enqueue(CacheIdentity, Generation)
	Close()
}

// InstallGenerationFence installs the requirement and claims exact eviction.
// overflow is fail-closed and intentionally not acknowledged by the RPC layer.
func InstallGenerationFence(id CacheIdentity, generation Generation) (current Generation, claimed, overflow bool) {
	claim, current, overflow := localFences.install(id, generation)
	if overflow {
		// A bump before the prefix claim covers entries concurrently loading but
		// not yet visible to the range: their pre-publish check observes the new
		// epoch and abandons the old object. The prefix claim prevents new readers
		// from acquiring every already-visible FULLTEXT2 object without waiting on
		// reader leases. The sender receives no ACK and retries the exact identity.
		coherenceEpoch.Add(1)
		veccache.Cache.ClaimRemovePrefixWithReason(cacheKeyPrefix, string(LoadMissGenerationChange))
		return current, false, true
	}
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

func currentCoherenceEpoch() uint64 { return coherenceEpoch.Load() }
