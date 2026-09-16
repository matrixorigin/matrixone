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

package runtime

import "time"

// vectorIndexStaleCheckIntervalSetter overrides the vector/fulltext2 index cache's cross-CN
// freshness sweep cadence. It is a dependency-inversion hook: the cache registers its setter here
// at init and the query-service handler (mo_ctl SetVectorIndexFreshnessInterval) invokes it,
// so the query service does not import pkg/vectorindex/cache -- that edge closes a test import
// cycle through catalog/fileservice. nil until the cache package is linked into the binary.
var vectorIndexStaleCheckIntervalSetter func(time.Duration)

// RegisterVectorIndexStaleCheckIntervalSetter is called once by the cache package's init.
func RegisterVectorIndexStaleCheckIntervalSetter(fn func(time.Duration)) {
	vectorIndexStaleCheckIntervalSetter = fn
}

// SetVectorIndexStaleCheckInterval applies the override if the cache is linked; a no-op otherwise
// (e.g. a component built without the cache). d<=0 restores the default cadence.
func SetVectorIndexStaleCheckInterval(d time.Duration) {
	if vectorIndexStaleCheckIntervalSetter != nil {
		vectorIndexStaleCheckIntervalSetter(d)
	}
}

// vectorIndexCacheKeyCounter reports how many vector/fulltext2 index cache entries this process
// holds for an index key. Same dependency-inversion pattern as the setter above: registered by the
// cache package at init, invoked by the query-service handler (GetVectorIndexCacheInfo).
var vectorIndexCacheKeyCounter func(string) int64

// RegisterVectorIndexCacheKeyCounter is called once by the cache package's init.
func RegisterVectorIndexCacheKeyCounter(fn func(string) int64) {
	vectorIndexCacheKeyCounter = fn
}

// VectorIndexCacheCountKey returns (count, true) if the cache is linked; (0, false) otherwise.
func VectorIndexCacheCountKey(key string) (int64, bool) {
	if vectorIndexCacheKeyCounter != nil {
		return vectorIndexCacheKeyCounter(key), true
	}
	return 0, false
}

// vectorIndexCacheEvictor drops a vector/fulltext2 index's cache entries and returns how many it
// removed. Same dependency-inversion pattern; registered by the cache at init, invoked by the
// query-service handler (EvictVectorIndexCache).
var vectorIndexCacheEvictor func(string) int64

// RegisterVectorIndexCacheEvictor is called once by the cache package's init.
func RegisterVectorIndexCacheEvictor(fn func(string) int64) {
	vectorIndexCacheEvictor = fn
}

// EvictVectorIndexCache evicts key if the cache is linked and returns (evicted, true); (0, false)
// otherwise.
func EvictVectorIndexCache(key string) (int64, bool) {
	if vectorIndexCacheEvictor != nil {
		return vectorIndexCacheEvictor(key), true
	}
	return 0, false
}

// vectorIndexCacheKeyLister returns the exact cache keys this process currently holds. Same
// dependency-inversion pattern; registered by the cache at init, invoked by the query-service
// handler (GetVectorIndexCacheKeys).
var vectorIndexCacheKeyLister func() []string

// RegisterVectorIndexCacheKeyLister is called once by the cache package's init.
func RegisterVectorIndexCacheKeyLister(fn func() []string) {
	vectorIndexCacheKeyLister = fn
}

// VectorIndexCacheKeys returns (keys, true) if the cache is linked; (nil, false) otherwise.
func VectorIndexCacheKeys() ([]string, bool) {
	if vectorIndexCacheKeyLister != nil {
		return vectorIndexCacheKeyLister(), true
	}
	return nil, false
}
