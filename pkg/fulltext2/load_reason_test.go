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
	key := (TableConfig{DbName: "db", IndexTable: "store"}).cacheIdentity().Key()
	rememberLoadReason(key, LoadMissCDCFlush)
	reason, generation := peekLoadReason(key)
	require.Empty(t, reason)
	require.Zero(t, generation)
}

func TestLoadReasonRegistryStoresOnlyWhenEnabled(t *testing.T) {
	cleanup := setLoadObserver(func(LoadEvent) {})
	defer cleanup()
	key := (TableConfig{DbName: "db", IndexTable: "store"}).cacheIdentity().Key()
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

	require.NotEmpty(t, (TableConfig{IndexTable: "index"}).cacheIdentity().Key())
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
	reason, generation = peekLoadReason(cfg.cacheIdentity().Key())
	require.Equal(t, LoadMissCDCFlush, reason)
	consumeLoadReason(cfg.cacheIdentity().Key(), generation)
	invalidateLoadGeneration(cfg, LoadMissReason("process_shutdown"))
	reason, generation = peekLoadReason(cfg.cacheIdentity().Key())
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

	key := (TableConfig{DbName: "db", IndexTable: "store"}).cacheIdentity().Key()
	rememberLoadReason(key, LoadMissCDCFlush)
	firstReason, firstGeneration := peekLoadReason(key)
	require.Equal(t, LoadMissCDCFlush, firstReason)
	rememberLoadReason(key, LoadMissRebuild)
	consumeLoadReason(key, firstGeneration)
	secondReason, secondGeneration := peekLoadReason(key)
	require.Equal(t, LoadMissRebuild, secondReason)
	consumeLoadReason(key, secondGeneration)
}
