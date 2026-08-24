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
	require.Empty(t, takeLoadReason(key))
}

func TestLoadReasonRegistryStoresOnlyWhenEnabled(t *testing.T) {
	cleanup := setLoadObserver(func(LoadEvent) {})
	defer cleanup()
	key := loadReasonKey("db", "store")
	rememberLoadReason(key, LoadMissCDCFlush)
	require.Equal(t, LoadMissCDCFlush, takeLoadReason(key))
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
	require.Empty(t, takeLoadReason("expired"))
	require.Empty(t, takeLoadReason("oldest"))
	require.Equal(t, LoadMissRebuild, takeLoadReason("overflow"))
	require.Empty(t, takeLoadReason("missing"))

	cfg := TableConfig{DbName: "db", IndexTable: "store"}
	invalidateLoadGeneration(cfg, LoadMissCDCFlush)
	require.Equal(t, LoadMissCDCFlush, takeLoadReason("db.store"))
	invalidateLoadGeneration(cfg, LoadMissReason("process_shutdown"))
	require.Empty(t, takeLoadReason("db.store"))
}
