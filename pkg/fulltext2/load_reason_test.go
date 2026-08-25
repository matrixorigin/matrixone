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
	reason, at := peekLoadReason(key)
	require.Empty(t, reason)
	require.True(t, at.IsZero())
}

func TestLoadReasonRegistryStoresOnlyWhenEnabled(t *testing.T) {
	cleanup := setLoadObserver(func(LoadEvent) {})
	defer cleanup()
	key := loadReasonKey("db", "store")
	rememberLoadReason(key, LoadMissCDCFlush)
	reason, at := peekLoadReason(key)
	require.Equal(t, LoadMissCDCFlush, reason)
	require.NotZero(t, at)
	consumeLoadReason(key, at)
	reason, at = peekLoadReason(key)
	require.Empty(t, reason)
	require.True(t, at.IsZero())
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
	reason, at := peekLoadReason("expired")
	require.Empty(t, reason)
	require.True(t, at.IsZero())
	reason, at = peekLoadReason("oldest")
	require.Empty(t, reason)
	require.True(t, at.IsZero())
	reason, at = peekLoadReason("overflow")
	require.Equal(t, LoadMissRebuild, reason)
	consumeLoadReason("overflow", at)
	reason, at = peekLoadReason("missing")
	require.Empty(t, reason)
	require.True(t, at.IsZero())

	cfg := TableConfig{DbName: "db", IndexTable: "store"}
	invalidateLoadGeneration(cfg, LoadMissCDCFlush)
	reason, at = peekLoadReason("db.store")
	require.Equal(t, LoadMissCDCFlush, reason)
	consumeLoadReason("db.store", at)
	invalidateLoadGeneration(cfg, LoadMissReason("process_shutdown"))
	reason, at = peekLoadReason("db.store")
	require.Empty(t, reason)
	require.True(t, at.IsZero())
}

func TestLoadReasonRegistryDoesNotConsumeNewerInvalidation(t *testing.T) {
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
	firstReason, firstAt := peekLoadReason(key)
	require.Equal(t, LoadMissCDCFlush, firstReason)
	rememberLoadReason(key, LoadMissRebuild)
	consumeLoadReason(key, firstAt)
	secondReason, secondAt := peekLoadReason(key)
	require.Equal(t, LoadMissRebuild, secondReason)
	consumeLoadReason(key, secondAt)
}
