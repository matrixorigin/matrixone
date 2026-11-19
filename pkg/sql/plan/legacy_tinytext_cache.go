// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"strings"
	"sync"
)

const (
	legacyTextCacheEntries    = 32
	legacyTextCacheSQLBytes   = 16 << 10
	legacyTextCacheColumns    = 256
	legacyTextCacheEntryBytes = 64 << 10
)

type legacyTextParseCacheEntry struct {
	createSQL string
	evidence  legacyTinyTextCreateEvidence
}

// Cache syntax evidence only. Identity, Seqnum and CREATE LIKE resolution must
// still be checked against the current transaction on every recovery. Entries
// own detached immutable strings/slices, never ASTs or catalog definitions.
// Round-robin eviction bounds retained payload to 2MiB plus fixed headers.
type legacyTextParseCache struct {
	mu      sync.RWMutex
	entries [legacyTextCacheEntries]legacyTextParseCacheEntry
	next    int
}

var legacyTextEvidenceCache legacyTextParseCache

func (cache *legacyTextParseCache) get(createSQL string) (legacyTinyTextCreateEvidence, bool) {
	if len(createSQL) == 0 || len(createSQL) > legacyTextCacheSQLBytes {
		return legacyTinyTextCreateEvidence{}, false
	}
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	for _, entry := range cache.entries {
		if entry.createSQL == createSQL {
			return entry.evidence, true
		}
	}
	return legacyTinyTextCreateEvidence{}, false
}

func (cache *legacyTextParseCache) put(createSQL string, evidence legacyTinyTextCreateEvidence) {
	if len(createSQL) == 0 || len(createSQL) > legacyTextCacheSQLBytes || len(evidence.columns) > legacyTextCacheColumns {
		return
	}
	// The conservative per-column allowance includes the slice element storage.
	remaining := legacyTextCacheEntryBytes - len(createSQL) - 64*len(evidence.columns)
	for _, text := range []string{evidence.likeDB, evidence.likeName} {
		if len(text) > remaining {
			return
		}
		remaining -= len(text)
	}
	for _, column := range evidence.columns {
		if len(column.name) > remaining {
			return
		}
		remaining -= len(column.name)
	}

	owned := legacyTinyTextCreateEvidence{
		likeDB: strings.Clone(evidence.likeDB), likeName: strings.Clone(evidence.likeName),
	}
	if evidence.columns != nil {
		owned.columns = make([]legacyTinyTextColumn, len(evidence.columns))
		copy(owned.columns, evidence.columns)
		for i := range owned.columns {
			owned.columns[i].name = strings.Clone(owned.columns[i].name)
		}
	}
	key := strings.Clone(createSQL)
	cache.mu.Lock()
	defer cache.mu.Unlock()
	// Concurrent misses parse independently outside the lock. Do not evict an
	// unrelated entry when another caller has already stored the same evidence.
	for _, entry := range cache.entries {
		if entry.createSQL == createSQL {
			return
		}
	}
	cache.entries[cache.next] = legacyTextParseCacheEntry{createSQL: key, evidence: owned}
	cache.next = (cache.next + 1) % len(cache.entries)
}

func parseCachedLegacyTinyTextEvidence(ctx context.Context, createSQL string) (legacyTinyTextCreateEvidence, error) {
	if evidence, ok := legacyTextEvidenceCache.get(createSQL); ok {
		return evidence, nil
	}
	evidence, err := parseLegacyTinyTextEvidence(ctx, createSQL)
	if err == nil {
		legacyTextEvidenceCache.put(createSQL, evidence)
	}
	return evidence, err
}
