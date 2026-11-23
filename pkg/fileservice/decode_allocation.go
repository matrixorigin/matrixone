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

// privateCacheDataAllocator allocates fresh, non-recycled buffers for a
// producer that keeps every writable view private until publication. Unknown
// allocator implementations cannot supply such a buffer: they may retain an
// alias, so use the FileService-owned default instead. This adapter never
// upgrades existing data or certifies a caller's completed allocation.
type privateCacheDataAllocator struct{ allocator CacheDataAllocator }

func NewPrivateCacheDataAllocator(allocator CacheDataAllocator) CacheDataAllocator {
	switch allocator.(type) {
	case *bytesAllocator, *MemCache, *LocalFS, *S3FS, *transientDecodeAllocator, *nonEvictingDecodeAllocator:
	default:
		allocator = DefaultCacheDataAllocator()
	}
	return privateCacheDataAllocator{allocator: allocator}
}

func (a privateCacheDataAllocator) BackingSize(size int) int {
	return a.allocator.BackingSize(size)
}

func (a privateCacheDataAllocator) AllocateCacheData(ctx context.Context, size int) fscache.Data {
	return a.AllocateCacheDataWithHint(ctx, size, malloc.NoHints)
}

func (a privateCacheDataAllocator) AllocateCacheDataWithHint(ctx context.Context, size int, hints malloc.Hints) fscache.Data {
	return a.allocator.AllocateCacheDataWithHint(ctx, size, hints|malloc.DoNotReuse)
}

func (a privateCacheDataAllocator) CopyToCacheData(ctx context.Context, raw []byte) fscache.Data {
	data := a.AllocateCacheDataWithHint(ctx, len(raw), malloc.NoClear)
	copy(data.Bytes(), raw)
	return data
}

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
	if m.cannotReserveWithoutEviction(int64(size)) {
		return m.transientDecoder.AllocateCacheDataWithHint(ctx, size, hints)
	}
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

// This is a negative hint, never an admission grant. Pending reservations may
// include reusable idle buffers or an active fill another read can join. When
// there are none, a known full cache needs no allocator/pressure locks or fill
// coordination: this read will remain transient. Concurrent release may make
// this conservative; actual allocation still validates and reserves capacity.
func (m *MemCache) cannotReserveWithoutEviction(size int64) bool {
	return size <= 0 || m.closed.Load() ||
		(m.reservedBytes.Load() == 0 && size > m.cache.Capacity()-m.backingBytes.Load())
}

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
