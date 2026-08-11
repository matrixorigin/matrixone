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

import (
	"context"
	"errors"
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fifocache"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

type MemCache struct {
	cache       fscache.DataCache
	counterSets []*perfcounter.CounterSet
	callbacksMu [256]sync.Mutex

	// allocator is dedicated to this cache. Do not use DefaultCacheDataAllocator
	// here: that would merge unrelated FileService caches into one allocator
	// arena and make fragmentation metrics untrustworthy.
	allocator       *bytesAllocator
	jemalloc        *malloc.JemallocAllocator
	allocatorGauges metric.FsCacheAllocatorStatsGauges
	lastStatsUpdate atomic.Int64
}

var memCacheCallbackSeed = maphash.MakeSeed()

var (
	memCachePressureMu                sync.Mutex
	memCachePressureTargets           = make(map[string]memCachePressureTargetState)
	memCachePressureEffectivePercent  atomic.Int64
	memCachePressureEffectiveDeadline atomic.Int64
)

type memCachePressureTargetState struct {
	percent  int64
	deadline int64
}

func SetMemoryCachePressureTargetPercent(percent int64, until time.Time) {
	SetMemoryCachePressureTargetPercentByOwner("", percent, until)
}

func SetMemoryCachePressureTargetPercentByOwner(owner string, percent int64, until time.Time) {
	now := time.Now()
	if percent <= 0 || !until.After(now) {
		ClearMemoryCachePressureTargetByOwner(owner)
		return
	}
	if percent > 100 {
		percent = 100
	}

	memCachePressureMu.Lock()
	defer memCachePressureMu.Unlock()

	old := memCachePressureTargets[owner]
	oldDeadline := old.deadline
	oldPercent := old.percent
	if oldDeadline > now.UnixNano() && oldPercent > 0 && oldPercent < percent {
		return
	}
	memCachePressureTargets[owner] = memCachePressureTargetState{
		percent:  percent,
		deadline: until.UnixNano(),
	}
	recomputeMemoryCachePressureTargetLocked(now.UnixNano())
}

func ClearMemoryCachePressureTarget() {
	memCachePressureMu.Lock()
	clear(memCachePressureTargets)
	recomputeMemoryCachePressureTargetLocked(time.Now().UnixNano())
	memCachePressureMu.Unlock()
}

func ClearMemoryCachePressureTargetByOwner(owner string) {
	memCachePressureMu.Lock()
	delete(memCachePressureTargets, owner)
	recomputeMemoryCachePressureTargetLocked(time.Now().UnixNano())
	memCachePressureMu.Unlock()
}

func clearMemoryCachePressureTargetForTest() {
	ClearMemoryCachePressureTarget()
}

func memoryCachePressureTarget(capacity int64) (int64, bool) {
	now := time.Now().UnixNano()

	deadline := memCachePressureEffectiveDeadline.Load()
	percent := memCachePressureEffectivePercent.Load()
	if deadline > now && percent > 0 {
		if percent > 100 {
			percent = 100
		}
		return capacity * percent / 100, true
	}

	memCachePressureMu.Lock()
	defer memCachePressureMu.Unlock()

	percent, _ = recomputeMemoryCachePressureTargetLocked(now)
	if percent == 0 {
		return 0, false
	}
	if percent > 100 {
		percent = 100
	}
	return capacity * percent / 100, true
}

func recomputeMemoryCachePressureTargetLocked(now int64) (int64, int64) {
	percent := int64(0)
	deadline := int64(0)
	for owner, target := range memCachePressureTargets {
		if target.deadline == 0 || now > target.deadline {
			delete(memCachePressureTargets, owner)
			continue
		}
		if target.percent <= 0 {
			continue
		}
		if percent == 0 || target.percent < percent {
			percent = target.percent
			deadline = target.deadline
		} else if target.percent == percent && target.deadline < deadline {
			deadline = target.deadline
		}
	}
	memCachePressureEffectivePercent.Store(percent)
	memCachePressureEffectiveDeadline.Store(deadline)
	return percent, deadline
}

func (m *MemCache) callbacksLock(key fscache.CacheKey) *sync.Mutex {
	var hasher maphash.Hash
	hasher.SetSeed(memCacheCallbackSeed)
	hasher.Write(util.UnsafeToBytes(&key.Offset))
	hasher.Write(util.UnsafeToBytes(&key.Sz))
	hasher.WriteString(key.Path)
	return &m.callbacksMu[hasher.Sum64()%uint64(len(m.callbacksMu))]
}

func NewMemCache(
	capacity fscache.CapacityFunc,
	callbacks *CacheCallbacks,
	counterSets []*perfcounter.CounterSet,
	name string,
) *MemCache {

	inuseBytes, capacityBytes := metric.GetFsCacheBytesGauge(name, "mem")
	logicalInuseBytes := metric.GetFsCacheLogicalBytesGauge(name, "mem")
	backingOverheadBytes := metric.GetFsCacheBackingOverheadBytesGauge(name, "mem")
	allocatorGauges := metric.GetFsCacheAllocatorStatsGauges(name, "mem")
	capacityBytes.Set(float64(capacity()))

	capacityFunc := func() int64 {
		// read from global hint
		if n := GlobalMemoryCacheSizeHint.Load(); n > 0 {
			return n
		}
		// fallback
		return capacity()
	}

	var dataCache *fifocache.DataCache
	cacheAllocator, jemallocAllocator := newMemoryCacheDataAllocator()
	ret := &MemCache{
		counterSets:     counterSets,
		allocator:       cacheAllocator,
		jemalloc:        jemallocAllocator,
		allocatorGauges: allocatorGauges,
	}

	prepareSetFn := func(_ context.Context, _ fscache.CacheKey, value fscache.Data, _, _ int64, _ uint64) func(inserted bool) {
		value.Retain()
		return func(inserted bool) {
			if !inserted {
				value.Release()
			}
		}
	}

	postSetFn := func(ctx context.Context, key fscache.CacheKey, value fscache.Data, logicalSize, size int64, seq uint64) {
		// events
		LogEvent(ctx, str_memory_cache_post_set_begin)
		defer LogEvent(ctx, str_memory_cache_post_set_end)

		// metrics
		LogEvent(ctx, str_update_metrics_begin)
		inuseBytes.Add(float64(size))
		logicalInuseBytes.Add(float64(logicalSize))
		backingOverheadBytes.Add(float64(size - logicalSize))
		capacityBytes.Set(float64(capacityFunc()))
		ret.refreshAllocatorMetrics(false)
		LogEvent(ctx, str_update_metrics_end)

		// callbacks
		if callbacks != nil {
			callbackLock := ret.callbacksLock(key)
			callbackLock.Lock()
			defer callbackLock.Unlock()
			if dataCache != nil {
				if currentSeq, ok := dataCache.CurrentSeq(key); !ok || currentSeq != seq {
					return
				}
			}
			LogEvent(ctx, str_memory_cache_callbacks_begin)
			for _, fn := range callbacks.PostSet {
				fn(key, value)
			}
			LogEvent(ctx, str_memory_cache_callbacks_end)
		}
	}

	postGetFn := func(ctx context.Context, key fscache.CacheKey, value fscache.Data, size int64) {
		// events
		LogEvent(ctx, str_memory_cache_post_get_begin)
		defer LogEvent(ctx, str_memory_cache_post_get_end)

		// retain
		value.Retain()

		// callbacks
		if callbacks != nil {
			LogEvent(ctx, str_memory_cache_callbacks_begin)
			for _, fn := range callbacks.PostGet {
				fn(key, value)
			}
			LogEvent(ctx, str_memory_cache_callbacks_end)
		}
	}

	postEvictFn := func(ctx context.Context, key fscache.CacheKey, value fscache.Data, logicalSize, size int64, seq uint64) {
		// events
		LogEvent(ctx, str_memory_cache_post_evict_begin)
		defer LogEvent(ctx, str_memory_cache_post_evict_end)

		// metrics
		LogEvent(ctx, str_update_metrics_begin)
		inuseBytes.Add(float64(-size))
		logicalInuseBytes.Add(float64(-logicalSize))
		backingOverheadBytes.Add(float64(logicalSize - size))
		capacityBytes.Set(float64(capacityFunc()))
		LogEvent(ctx, str_update_metrics_end)

		// release
		value.Release()
		ret.refreshAllocatorMetrics(false)

		// callbacks
		if callbacks != nil {
			callbackLock := ret.callbacksLock(key)
			callbackLock.Lock()
			defer callbackLock.Unlock()
			if dataCache != nil {
				if currentSeq, ok := dataCache.CurrentSeq(key); ok && currentSeq != seq {
					return
				}
			}
			LogEvent(ctx, str_memory_cache_callbacks_begin)
			for _, fn := range callbacks.PostEvict {
				fn(key, value)
			}
			LogEvent(ctx, str_memory_cache_callbacks_end)
		}
	}

	dataCache = fifocache.NewDataCacheWithPrepareSet(capacityFunc, prepareSetFn, postSetFn, postGetFn, postEvictFn)
	dataCache.SetAdmissionTarget(memoryCachePressureTarget)

	ret.cache = dataCache
	ret.refreshAllocatorMetrics(true)

	if name != "" {
		allMemoryCaches.Store(ret, name)
	}

	return ret
}

var _ IOVectorCache = new(MemCache)
var _ CacheDataAllocator = new(MemCache)

// cacheDataAllocationCapacityGuarded marks allocators that reserve FIFO cache
// capacity before allocating. DiskCache uses it to avoid a second eviction
// pass when its cache data is allocated directly into this MemCache.
func (*MemCache) cacheDataAllocationCapacityGuarded() {}

func (m *MemCache) AllocateCacheData(ctx context.Context, size int) fscache.Data {
	ensureCacheDataCapacity(ctx, m.cache, m.allocator, size)
	return m.allocator.AllocateCacheData(ctx, size)
}

func (m *MemCache) AllocateCacheDataWithHint(ctx context.Context, size int, hints malloc.Hints) fscache.Data {
	ensureCacheDataCapacity(ctx, m.cache, m.allocator, size)
	return m.allocator.AllocateCacheDataWithHint(ctx, size, hints)
}

func (m *MemCache) CopyToCacheData(ctx context.Context, data []byte) fscache.Data {
	ensureCacheDataCapacity(ctx, m.cache, m.allocator, len(data))
	return m.allocator.CopyToCacheData(ctx, data)
}

func (m *MemCache) BackingSize(size int) int {
	return m.allocator.BackingSize(size)
}

func (m *MemCache) refreshAllocatorMetrics(force bool) {
	if m.jemalloc == nil {
		return
	}

	now := time.Now().UnixNano()
	if !force {
		for {
			last := m.lastStatsUpdate.Load()
			if now-last < int64(time.Second) {
				return
			}
			if m.lastStatsUpdate.CompareAndSwap(last, now) {
				break
			}
		}
	} else {
		m.lastStatsUpdate.Store(now)
	}

	stats, err := m.jemalloc.Stats()
	if err != nil {
		return
	}
	fragmentation := uint64(0)
	if stats.Active > stats.Allocated {
		fragmentation = stats.Active - stats.Allocated
	}
	m.allocatorGauges.Allocated.Set(float64(stats.Allocated))
	m.allocatorGauges.Active.Set(float64(stats.Active))
	m.allocatorGauges.Fragmentation.Set(float64(fragmentation))
	m.allocatorGauges.Metadata.Set(float64(stats.Metadata))
	m.allocatorGauges.Resident.Set(float64(stats.Resident))
	m.allocatorGauges.Mapped.Set(float64(stats.Mapped))
	m.allocatorGauges.Retained.Set(float64(stats.Retained))
	m.allocatorGauges.Dirty.Set(float64(stats.Dirty))
	m.allocatorGauges.Muzzy.Set(float64(stats.Muzzy))
}

func (m *MemCache) Read(
	ctx context.Context,
	vector *IOVector,
) (
	err error,
) {

	if vector.Policy.Any(SkipMemoryCacheReads) {
		return nil
	}

	var numHit, numRead int64
	defer func() {
		metric.FSReadHitMemCounter.Add(float64(numHit))
		metric.FSReadReadMemCounter.Add(float64(numRead))
		perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
			c.FileService.Cache.Read.Add(numRead)
			c.FileService.Cache.Hit.Add(numHit)
			c.FileService.Cache.Memory.Read.Add(numRead)
			c.FileService.Cache.Memory.Hit.Add(numHit)
		}, m.counterSets...)
	}()

	path, err := ParsePath(vector.FilePath)
	if err != nil {
		return err
	}

	for i, entry := range vector.Entries {
		if entry.done {
			continue
		}
		key := fscache.CacheKey{
			Path:   path.File,
			Offset: entry.Offset,
			Sz:     entry.Size,
		}
		bs, ok := m.cache.Get(ctx, key)
		numRead++
		if ok {
			vector.Entries[i].CachedData = bs
			vector.Entries[i].done = true
			vector.Entries[i].fromCache = m
			numHit++
		}
	}

	return
}

func (m *MemCache) Update(
	ctx context.Context,
	vector *IOVector,
	async bool,
) error {

	if vector.Policy.Any(SkipMemoryCacheWrites) {
		return nil
	}

	path, err := ParsePath(vector.FilePath)
	if err != nil {
		return err
	}

	for _, entry := range vector.Entries {
		if entry.CachedData == nil {
			continue
		}
		if entry.fromCache == m {
			continue
		}

		key := fscache.CacheKey{
			Path:   path.File,
			Offset: entry.Offset,
			Sz:     entry.Size,
		}
		LogEvent(ctx, str_set_memory_cache_entry_begin)
		err := m.cache.Set(ctx, key, entry.CachedData)
		LogEvent(ctx, str_set_memory_cache_entry_end)
		if errors.Is(err, fscache.ErrCacheAdmissionRejected) {
			metric.FSCachePressureMemorySkipCounter.Inc()
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *MemCache) Flush(ctx context.Context) {
	m.cache.Flush(ctx)
	m.refreshAllocatorMetrics(true)
}

func (m *MemCache) DeletePaths(
	ctx context.Context,
	paths []string,
) error {
	canonical, err := canonicalFilePaths(paths)
	if err != nil {
		return err
	}
	m.cache.DeletePaths(ctx, canonical)
	return nil
}

func (m *MemCache) Evict(ctx context.Context, done chan int64) {
	m.cache.Evict(ctx, done)
}

func (m *MemCache) EvictToTarget(ctx context.Context, target int64) int64 {
	return m.cache.EvictToTargetWithWait(ctx, target)
}

func (m *MemCache) EvictToCapacityPercent(ctx context.Context, percent int64) int64 {
	if percent < 0 {
		percent = 0
	}
	if percent > 100 {
		percent = 100
	}
	target := m.cache.Capacity() * percent / 100
	return m.EvictToTarget(ctx, target)
}

func (m *MemCache) Close(ctx context.Context) {
	m.Flush(ctx)
	m.refreshAllocatorMetrics(true)
	allMemoryCaches.Delete(m)
}
