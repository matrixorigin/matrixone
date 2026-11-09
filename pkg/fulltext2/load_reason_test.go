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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoadReasonRegistryDisabledObserverIsNoOp(t *testing.T) {
	cleanup := setLoadObserver(nil)
	defer cleanup()
	key := loadReasonKey("db", "store")
	rememberLoadReason(key, LoadMissCDCFlush)
	reason, generation := peekLoadReason(key)
	require.Empty(t, reason)
	require.Zero(t, generation)
}

func TestLoadReasonRegistryStoresOnlyWhenEnabled(t *testing.T) {
	cleanup := setLoadObserver(func(LoadEvent) {})
	defer cleanup()
	key := loadReasonKey("db", "store")
	rememberLoadReason(key, LoadMissCDCFlush)
	reason, generation := peekLoadReason(key)
	require.Equal(t, LoadMissCDCFlush, reason)
	require.NotZero(t, generation)
	consumeLoadReason(key, generation)
	reason, generation = peekLoadReason(key)
	require.Empty(t, reason)
	require.Zero(t, generation)
}

func TestLoadReasonRegistryBoundsExpiryAndInvalidation(t *testing.T) {
	cleanupObserver := setLoadObserver(func(LoadEvent) {})
	defer cleanupObserver()

	pendingLoadReasons.Lock()
	previous := pendingLoadReasons.m
	pendingLoadReasons.m = make(map[string]pendingLoadReason)
	pendingLoadReasons.Unlock()
	t.Cleanup(func() {
		pendingLoadReasons.Lock()
		pendingLoadReasons.m = previous
		pendingLoadReasons.Unlock()
	})

	require.Equal(t, "index", loadReasonKey("", "index"))
	pendingLoadReasons.Lock()
	pendingLoadReasons.m["expired"] = pendingLoadReason{reason: LoadMissTTLExpired, at: time.Now().Add(-loadReasonTTL - time.Second)}
	pendingLoadReasons.m["oldest"] = pendingLoadReason{reason: LoadMissCDCFlush, at: time.Now().Add(-time.Second)}
	for i := 0; i < loadReasonSize-1; i++ {
		pendingLoadReasons.m["seed-"+string(rune(i))] = pendingLoadReason{reason: LoadMissMerge, at: time.Now()}
	}
	pendingLoadReasons.Unlock()

	rememberLoadReason("overflow", LoadMissRebuild)
	reason, generation := peekLoadReason("expired")
	require.Empty(t, reason)
	require.Zero(t, generation)
	reason, generation = peekLoadReason("oldest")
	require.Empty(t, reason)
	require.Zero(t, generation)
	reason, generation = peekLoadReason("overflow")
	require.Equal(t, LoadMissRebuild, reason)
	consumeLoadReason("overflow", generation)
	reason, generation = peekLoadReason("missing")
	require.Empty(t, reason)
	require.Zero(t, generation)

	cfg := TableConfig{DbName: "db", IndexTable: "store"}
	invalidateLoadGeneration(cfg, LoadMissCDCFlush)
	reason, generation = peekLoadReason("db.store")
	require.Equal(t, LoadMissCDCFlush, reason)
	consumeLoadReason("db.store", generation)
	invalidateLoadGeneration(cfg, LoadMissReason("process_shutdown"))
	reason, generation = peekLoadReason("db.store")
	require.Empty(t, reason)
	require.Zero(t, generation)
}

func TestLoadReasonRegistryDoesNotConsumeNewerInvalidationAfterReasonReplacement(t *testing.T) {
	cleanupObserver := setLoadObserver(func(LoadEvent) {})
	defer cleanupObserver()

	pendingLoadReasons.Lock()
	previous := pendingLoadReasons.m
	pendingLoadReasons.m = make(map[string]pendingLoadReason)
	pendingLoadReasons.Unlock()
	t.Cleanup(func() {
		pendingLoadReasons.Lock()
		pendingLoadReasons.m = previous
		pendingLoadReasons.Unlock()
	})

	key := loadReasonKey("db", "store")
	rememberLoadReason(key, LoadMissCDCFlush)
	firstReason, firstGeneration := peekLoadReason(key)
	require.Equal(t, LoadMissCDCFlush, firstReason)
	rememberLoadReason(key, LoadMissRebuild)
	consumeLoadReason(key, firstGeneration)
	secondReason, secondGeneration := peekLoadReason(key)
	require.Equal(t, LoadMissRebuild, secondReason)
	consumeLoadReason(key, secondGeneration)
}

func TestRecoverCacheInvalidationFencesOnlyItsGeneration(t *testing.T) {
	restore := setLoadObserver(func(LoadEvent) {})
	defer restore()
	cfg := TableConfig{DbName: "recovery-test", IndexTable: "first"}
	index := loadReasonKey(cfg.DbName, cfg.IndexTable)
	other := loadReasonKey(cfg.DbName, "other")
	t.Cleanup(func() { clearLoadGeneration(index); clearLoadGeneration(other) })
	rememberLoadReason(index, LoadMissCDCFlush)
	first := beginLoadGeneration(index)
	second := beginLoadGeneration(other)
	defer endLoadGeneration(first)
	defer endLoadGeneration(second)
	reason, generation := peekLoadReason(index)

	s := NewFulltext2Search(cfg)
	require.NoError(t, s.RecoverCacheInvalidation(string(LoadMissTTLExpired)))
	require.False(t, loadGenerationCurrent(first))
	require.True(t, loadGenerationCurrent(second))
	gotReason, gotGeneration := peekLoadReason(index)
	require.Equal(t, reason, gotReason)
	require.Equal(t, generation, gotGeneration)
	require.Error(t, s.RecoverCacheInvalidation(string(LoadMissRebuild)))
}

func TestRecoverCacheInvalidationRejectsBrokenRegistry(t *testing.T) {
	pendingLoadReasons.Lock()
	old := pendingLoadReasons.m
	pendingLoadReasons.m = nil
	pendingLoadReasons.Unlock()
	t.Cleanup(func() {
		pendingLoadReasons.Lock()
		pendingLoadReasons.m = old
		pendingLoadReasons.Unlock()
	})
	s := NewFulltext2Search(TableConfig{DbName: "recovery-test", IndexTable: "first"})
	require.Error(t, s.RecoverCacheInvalidation(string(LoadMissGenerationChange)))
}

func TestTTLInvalidationDoesNotSweepOtherIndexPools(t *testing.T) {
	index := loadReasonKey("recovery-test", "other")
	key := baseKey{index: index, id: "base", filesize: 1}
	old := time.Now().Add(-2 * basePoolIdleTTL)
	base := &baseEntry{lease: newSegmentLease(NewSegment("base", 0)), lastUsed: old}
	loadedBasePool.mu.Lock()
	loadedBasePool.entries[key] = base
	loadedBasePool.totalBytes++
	loadedBasePool.mu.Unlock()
	tail := &tailState{lastUsed: old}
	loadedTailPool.mu.Lock()
	loadedTailPool.states[index] = tail
	loadedTailPool.mu.Unlock()
	t.Cleanup(func() {
		loadedBasePool.clearIndex(index)
		loadedTailPool.clear(index)
		clearLoadGeneration(loadReasonKey("recovery-test", "first"))
	})

	NewFulltext2Search(TableConfig{DbName: "recovery-test", IndexTable: "first"}).OnCacheInvalidated(string(LoadMissTTLExpired))
	loadedBasePool.mu.Lock()
	gotBase := loadedBasePool.entries[key]
	loadedBasePool.mu.Unlock()
	loadedTailPool.mu.Lock()
	gotTail := loadedTailPool.states[index]
	loadedTailPool.mu.Unlock()
	require.Same(t, base, gotBase)
	require.Same(t, tail, gotTail)
}
