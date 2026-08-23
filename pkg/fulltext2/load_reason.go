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
)

const (
	loadReasonTTL  = 15 * time.Minute
	loadReasonSize = 1024
)

type pendingLoadReason struct {
	reason LoadMissReason
	at     time.Time
}

// pendingLoadReasons bridges an invalidation event and the next cache miss.
// It is bounded and expiring because an index may be invalidated without being
// queried again. The registry is touched only on invalidation/load paths, never
// by a warm search.
var pendingLoadReasons = struct {
	sync.Mutex
	m map[string]pendingLoadReason
}{m: make(map[string]pendingLoadReason)}

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
	if !loadObservationEnabled() || index == "" || reason == "" {
		return
	}
	now := time.Now()
	pendingLoadReasons.Lock()
	defer pendingLoadReasons.Unlock()
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
	pendingLoadReasons.m[index] = pendingLoadReason{reason: reason, at: now}
}

func takeLoadReason(index string) LoadMissReason {
	if !loadObservationEnabled() {
		return ""
	}
	now := time.Now()
	pendingLoadReasons.Lock()
	defer pendingLoadReasons.Unlock()
	v, ok := pendingLoadReasons.m[index]
	if !ok {
		return ""
	}
	delete(pendingLoadReasons.m, index)
	if now.Sub(v.at) >= loadReasonTTL {
		return ""
	}
	return v.reason
}

// invalidateLoadGeneration records why the next load will miss. The
// observability layer deliberately leaves cache eviction semantics unchanged;
// generation reuse clears its pools in the follow-up implementation.
func invalidateLoadGeneration(cfg TableConfig, reason LoadMissReason) {
	if reason != LoadMissReason("process_shutdown") {
		rememberLoadReason(loadReasonKey(cfg.DbName, cfg.IndexTable), reason)
	}
}
