// Copyright 2022 Matrix Origin
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

package fileservice

type Policy uint64

const (
	SkipMemoryCacheReads = 1 << iota
	SkipMemoryCacheWrites
	SkipDiskCacheReads
	SkipDiskCacheWrites
	SkipRemoteCacheReads
	SkipRemoteCacheWrites
	SkipFullFilePreloads
)

const (
	SkipDiskCache   = SkipDiskCacheReads | SkipDiskCacheWrites
	SkipMemoryCache = SkipMemoryCacheReads | SkipMemoryCacheWrites
	SkipRemoteCache = SkipRemoteCacheReads | SkipRemoteCacheWrites
	SkipCacheReads  = SkipMemoryCacheReads | SkipDiskCacheReads | SkipRemoteCacheReads
	SkipCacheWrites = SkipMemoryCacheWrites | SkipDiskCacheWrites | SkipRemoteCacheWrites
	SkipAllCache    = SkipDiskCache | SkipMemoryCache | SkipRemoteCache
)

func (c Policy) Any(policies ...Policy) bool {
	for _, policy := range policies {
		if policy&c > 0 {
			return true
		}
	}
	return false
}

func (c Policy) CacheIOEntry() bool {
	// cache IOEntry if not caching full file
	return c.Any(SkipFullFilePreloads)
}

func (c Policy) CacheFullFile() bool {
	return !c.Any(SkipFullFilePreloads)
}
