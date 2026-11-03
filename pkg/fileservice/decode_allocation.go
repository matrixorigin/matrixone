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
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

type cacheAllocationMode uint8

const (
	cacheAllocationNormal cacheAllocationMode = iota
	cacheAllocationNoEvict
	cacheAllocationTransient
)

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

type nonEvictingDecodeAllocator struct{ cache *MemCache }

func (a *nonEvictingDecodeAllocator) AllocateCacheData(ctx context.Context, size int) fscache.Data {
	return a.AllocateCacheDataWithHint(ctx, size, malloc.NoHints)
}

func (a *nonEvictingDecodeAllocator) AllocateCacheDataWithHint(ctx context.Context, size int, hints malloc.Hints) fscache.Data {
	m := a.cache
	backing := m.BackingSize(size)
	m.capacityMu.Lock()
	_, pressure := memoryCachePressureTarget(m.cache.Capacity())
	if !m.closed.Load() && !pressure && m.backingBytes.Load() <= m.cache.Capacity() {
		if data := m.reuseReservedCacheData(size, backing, hints); data != nil {
			m.capacityMu.Unlock()
			return data
		}
		if int64(backing) <= m.cache.Capacity()-m.backingBytes.Load() &&
			int64(backing) <= m.cache.Capacity()-m.cache.Used()-m.reservedBytes.Load() {
			m.reservedBytes.Add(int64(backing))
			m.backingBytes.Add(int64(backing))
			reservation := &memoryCacheReservation{cache: m, bytes: int64(backing)}
			m.capacityMu.Unlock()
			data, err := m.allocator.tryAllocateCacheBytes(size, hints)
			if err != nil {
				reservation.release()
				panic(err)
			}
			data.reservation = reservation
			return data
		}
	}
	m.capacityMu.Unlock()
	return m.transientDecoder.AllocateCacheDataWithHint(ctx, size, hints)
}

func (a *nonEvictingDecodeAllocator) CopyToCacheData(ctx context.Context, raw []byte) fscache.Data {
	data := a.AllocateCacheDataWithHint(ctx, len(raw), malloc.NoClear)
	copy(data.Bytes(), raw)
	return data
}

func (a *nonEvictingDecodeAllocator) BackingSize(size int) int { return a.cache.BackingSize(size) }

// Called under capacityMu by conditional cache publication. The backing charge
// also covers this fill, so it must fit without releasing any resident buffer.
func (m *MemCache) nonEvictingPublicationAllowed() bool {
	return !m.closed.Load() && m.backingBytes.Load() <= m.cache.Capacity()
}

func (i *IOEntry) allocatorForDecode(fallback CacheDataAllocator) CacheDataAllocator {
	if i.cacheAdmissionOwner != nil {
		switch i.cacheAllocationMode {
		case cacheAllocationNoEvict:
			return &i.cacheAdmissionOwner.nonEvictingDecoder
		case cacheAllocationTransient:
			return &i.cacheAdmissionOwner.transientDecoder
		}
	}
	return fallback
}

func (i *IOVector) resetCacheAdmission() {
	for idx := range i.Entries {
		entry := &i.Entries[idx]
		entry.cacheAdmissionOwner = nil
		entry.cacheAllocationMode = cacheAllocationNormal
	}
}

func (m *MemCache) prepareDecodeAllocation(v *IOVector) {
	mode := cacheAllocationNormal
	if v.Policy.Any(MemoryCacheNoEvict) {
		mode = cacheAllocationNoEvict
	}
	if v.Policy.Any(SkipMemoryCacheWrites) {
		mode = cacheAllocationTransient
	}
	if mode == cacheAllocationNormal {
		return
	}
	for idx := range v.Entries {
		entry := &v.Entries[idx]
		if !entry.done {
			entry.cacheAdmissionOwner = m
			entry.cacheAllocationMode = mode
		}
	}
}
