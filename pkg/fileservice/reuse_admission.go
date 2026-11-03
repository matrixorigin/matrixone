// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fileservice

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fifocache"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

func reuseHistoryAllowance(capacity int64) int64 { return max(0, min(capacity/1024, 64<<20)) }

type transientDecodeAllocator struct{ cache *MemCache }

func (a *transientDecodeAllocator) AllocateCacheData(ctx context.Context, size int) fscache.Data {
	return a.AllocateCacheDataWithHint(ctx, size, malloc.NoHints)
}
func (a *transientDecodeAllocator) AllocateCacheDataWithHint(_ context.Context, size int, hints malloc.Hints) fscache.Data {
	data := a.cache.uncachedAllocator.allocateCacheBytes(size, hints)
	data.cacheAdmissionOwner = a.cache.allocator.owner
	return data
}
func (a *transientDecodeAllocator) CopyToCacheData(ctx context.Context, raw []byte) fscache.Data {
	data := a.AllocateCacheDataWithHint(ctx, len(raw), malloc.NoClear)
	copy(data.Bytes(), raw)
	return data
}
func (a *transientDecodeAllocator) BackingSize(size int) int {
	return a.cache.uncachedAllocator.BackingSize(size)
}

func (i *IOEntry) allocatorForDecode(fallback CacheDataAllocator) CacheDataAllocator {
	if i.cacheAdmissionBypass && i.cacheAdmissionOwner != nil {
		return &i.cacheAdmissionOwner.transientDecoder
	}
	return fallback
}

func (i *IOVector) resetCacheAdmission() {
	for idx := range i.Entries {
		entry := &i.Entries[idx]
		entry.cacheAdmissionOwner = nil
		entry.cacheAdmissionBypass = false
	}
}

func (m *MemCache) prepareReuseAdmission(ctx context.Context, v *IOVector, path string) {
	if !v.Policy.Any(SkipMemoryCacheWrites, MemoryCacheReuseOnly) {
		return
	}
	for i := range v.Entries {
		e := &v.Entries[i]
		if e.done || e.cacheAdmissionOwner == m {
			continue
		}
		e.cacheAdmissionOwner = m
		admit := false
		if !v.Policy.Any(SkipMemoryCacheWrites) && e.CachedDataSize > 0 &&
			e.CachedDataSize <= int64(^uint(0)>>1) && e.Offset >= 0 && e.Size > 0 && e.ToCacheData != nil {
			if cache, ok := m.cache.(*fifocache.DataCache); ok {
				if m.reuseHistoryEnabled.CompareAndSwap(false, true) {
					cache.EvictToTargetWithWait(ctx, max(0, cache.Capacity()-m.reservedBytes.Load()))
				}
				admit = cache.ObserveReuse(fscache.CacheKey{Path: path, Offset: e.Offset, Sz: e.Size}, int64(m.BackingSize(int(e.CachedDataSize))))
			}
		}
		e.cacheAdmissionBypass = !admit
	}
}
