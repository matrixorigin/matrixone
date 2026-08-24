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
	"sync"
	"time"

	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
)

const (
	loadReasonTTL  = 15 * time.Minute
	loadReasonSize = 1024
)

type pendingLoadReason struct {
	reason     LoadMissReason
	generation uint64
	at         time.Time
}

// loadGeneration identifies one cache-load attempt and the invalidation epoch
// it observed. The invalidation epoch is monotonic per index; the attempt is
// unique per index so an older replacement load cannot publish over a newer
// one even when both observe the same invalidation.
type loadGeneration struct {
	index        string
	invalidation uint64
	attempt      uint64
}

var loadGenerations = struct {
	sync.Mutex
	m map[string]struct {
		invalidation uint64
		attempt      uint64
	}
}{m: make(map[string]struct {
	invalidation uint64
	attempt      uint64
})}

// pendingLoadReasons bridges an invalidation event and the next cache miss. A
// failed load leaves the cause available for its retry; a successful load
// consumes only the entry observed by that load. It is bounded and expiring
// because an index may be invalidated without being queried again. The
// registry is touched only on invalidation/load paths, never by a warm search.
var pendingLoadReasons = struct {
	sync.Mutex
	m map[string]pendingLoadReason
}{m: make(map[string]pendingLoadReason)}

var reusableLoadLifecycleOnce sync.Once

func ensureReusableLoadLifecycle() {
	reusableLoadLifecycleOnce.Do(func() {
		veccache.RegisterLifecycleHook(func(shutdown bool) {
			if shutdown {
				loadedBasePool.clearAll()
				loadedTailPool.clearAll()
				return
			}
			now := time.Now()
			loadedBasePool.evict(now)
			loadedTailPool.evict(now)
		})
	})
}

// loadReasonKey is the bounded registry key used by cache invalidation hooks.
// The database qualifier avoids conflating identically named hidden stores in
// different databases while keeping the key free of query or primary-key data.
func loadReasonKey(db, index string) string {
	if db == "" {
		return index
	}
	return db + "." + index
}

func rememberLoadReason(index string, reason LoadMissReason) {
	pendingLoadReasons.Lock()
	defer pendingLoadReasons.Unlock()
	loadGenerations.Lock()
	defer loadGenerations.Unlock()
	state := loadGenerations.m[index]
	state.invalidation++
	loadGenerations.m[index] = state
	rememberLoadReasonLocked(index, reason, state.invalidation)
}

func rememberLoadReasonLocked(index string, reason LoadMissReason, generation uint64) {
	if !loadObservationEnabled() || index == "" || reason == "" || reason == LoadMissReason("process_shutdown") {
		return
	}
	now := time.Now()
	for k, v := range pendingLoadReasons.m {
		if now.Sub(v.at) >= loadReasonTTL {
			delete(pendingLoadReasons.m, k)
		}
	}
	if len(pendingLoadReasons.m) >= loadReasonSize {
		var oldestKey string
		var oldest time.Time
		for k, v := range pendingLoadReasons.m {
			if oldestKey == "" || v.at.Before(oldest) {
				oldestKey, oldest = k, v.at
			}
		}
		if oldestKey != "" {
			delete(pendingLoadReasons.m, oldestKey)
		}
	}
	pendingLoadReasons.m[index] = pendingLoadReason{reason: reason, generation: generation, at: now}
}

func peekLoadReason(index string) (LoadMissReason, uint64) {
	if !loadObservationEnabled() {
		return "", 0
	}
	now := time.Now()
	pendingLoadReasons.Lock()
	defer pendingLoadReasons.Unlock()
	v, ok := pendingLoadReasons.m[index]
	if !ok {
		return "", 0
	}
	if now.Sub(v.at) >= loadReasonTTL {
		delete(pendingLoadReasons.m, index)
		return "", 0
	}
	return v.reason, v.generation
}

func consumeLoadReason(index string, generation uint64) {
	if index == "" || generation == 0 {
		return
	}
	pendingLoadReasons.Lock()
	defer pendingLoadReasons.Unlock()
	if v, ok := pendingLoadReasons.m[index]; ok && v.generation == generation {
		delete(pendingLoadReasons.m, index)
	}
}

func consumeLoadReasonIfCurrent(index string, reasonGeneration uint64, current loadGeneration) bool {
	if index == "" || reasonGeneration == 0 {
		return false
	}
	pendingLoadReasons.Lock()
	defer pendingLoadReasons.Unlock()
	loadGenerations.Lock()
	defer loadGenerations.Unlock()
	state, ok := loadGenerations.m[current.index]
	if !ok || state.invalidation != current.invalidation || state.attempt != current.attempt {
		return false
	}
	if v, ok := pendingLoadReasons.m[index]; ok && v.generation == reasonGeneration {
		delete(pendingLoadReasons.m, index)
		return true
	}
	return false
}

func beginLoadGeneration(index string) loadGeneration {
	loadGenerations.Lock()
	defer loadGenerations.Unlock()
	state := loadGenerations.m[index]
	state.attempt++
	loadGenerations.m[index] = state
	return loadGeneration{index: index, invalidation: state.invalidation, attempt: state.attempt}
}

func loadGenerationCurrent(g loadGeneration) bool {
	if g.index == "" || g.attempt == 0 {
		return true
	}
	loadGenerations.Lock()
	defer loadGenerations.Unlock()
	state, ok := loadGenerations.m[g.index]
	return ok && state.invalidation == g.invalidation && state.attempt == g.attempt
}

func clearReusableLoadGeneration(cfg TableConfig) {
	index := loadReasonKey(cfg.DbName, cfg.IndexTable)
	loadedBasePool.clearIndex(index)
	loadedTailPool.clear(index)
}

// invalidateLoadGeneration records why the next load will miss and clears
// reusable immutable state only when the base itself is known to have changed.
// It runs from the generic cache's invalidation hook, so the existing cache
// lifecycle remains the only integration surface for other index algorithms.
func invalidateLoadGeneration(cfg TableConfig, reason LoadMissReason) {
	index := loadReasonKey(cfg.DbName, cfg.IndexTable)
	rememberLoadReason(index, reason)
	switch reason {
	case LoadMissMerge, LoadMissRebuild, LoadMissReason("process_shutdown"):
		if reason == LoadMissReason("process_shutdown") {
			loadedBasePool.clearAll()
			loadedTailPool.clearAll()
		} else {
			clearReusableLoadGeneration(cfg)
		}
	case LoadMissTTLExpired, LoadMissGenerationChange:
		// Keep the current index's immutable base/tail reusable across CDC and
		// TTL refreshes, but sweep stale entries belonging to indexes that have
		// not been queried recently. This gives the process-global pools a
		// bounded idle lifetime without making every CDC refresh cold-load its
		// own base.
		loadedBasePool.evict(time.Now())
		loadedTailPool.evict(time.Now())
	}
}
