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
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"

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
}

var memCacheCallbackSeed = maphash.MakeSeed()

var (
	memCachePressureMu            sync.Mutex
	memCachePressureTargetPercent atomic.Int64
	memCachePressureDeadline      atomic.Int64
)

func SetMemoryCachePressureTargetPercent(percent int64, until time.Time) {
	now := time.Now()
	if percent <= 0 || !until.After(now) {
		memCachePressureMu.Lock()
		memCachePressureTargetPercent.Store(0)
		memCachePressureDeadline.Store(0)
		memCachePressureMu.Unlock()
		return
	}
	if percent > 100 {
		percent = 100
	}

	memCachePressureMu.Lock()
	defer memCachePressureMu.Unlock()

	oldDeadline := memCachePressureDeadline.Load()
	oldPercent := memCachePressureTargetPercent.Load()
	if oldDeadline > now.UnixNano() && oldPercent > 0 && oldPercent < percent {
		return
	}
	memCachePressureTargetPercent.Store(percent)
	memCachePressureDeadline.Store(until.UnixNano())
}

func clearMemoryCachePressureTargetForTest() {
	memCachePressureTargetPercent.Store(0)
	memCachePressureDeadline.Store(0)
}

func memoryCachePressureTarget(capacity int64) (int64, bool) {
	deadline := memCachePressureDeadline.Load()
	if deadline == 0 || time.Now().UnixNano() > deadline {
		return 0, false
	}
	percent := memCachePressureTargetPercent.Load()
	if percent <= 0 {
		return 0, false
	}
	if percent > 100 {
		percent = 100
	}
	return capacity * percent / 100, true
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
	ret := &MemCache{
		counterSets: counterSets,
	}

	prepareSetFn := func(ctx context.Context, key fscache.CacheKey, value fscache.Data, size int64, seq uint64) func(inserted bool) {
		value.Retain()
		return func(inserted bool) {
			if !inserted {
				value.Release()
			}
		}
	}

	postSetFn := func(ctx context.Context, key fscache.CacheKey, value fscache.Data, size int64, seq uint64) {
		// events
		LogEvent(ctx, str_memory_cache_post_set_begin)
		defer LogEvent(ctx, str_memory_cache_post_set_end)

		// metrics
		LogEvent(ctx, str_update_metrics_begin)
		inuseBytes.Add(float64(size))
		capacityBytes.Set(float64(capacityFunc()))
		LogEvent(ctx, str_update_metrics_end)

		// callbacks
		if callbacks != nil {
			callbackLock := ret.callbacksLock(key)
			callbackLock.Lock()
			defer callbackLock.Unlock()
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

	postEvictFn := func(ctx context.Context, key fscache.CacheKey, value fscache.Data, size int64, seq uint64) {
		// events
		LogEvent(ctx, str_memory_cache_post_evict_begin)
		defer LogEvent(ctx, str_memory_cache_post_evict_end)

		// relaese
		value.Release()

		// metrics
		LogEvent(ctx, str_update_metrics_begin)
		inuseBytes.Add(float64(-size))
		capacityBytes.Set(float64(capacityFunc()))
		LogEvent(ctx, str_update_metrics_end)

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

	if name != "" {
		allMemoryCaches.Store(ret, name)
	}

	return ret
}

var _ IOVectorCache = new(MemCache)

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
		if target, ok := memoryCachePressureTarget(m.cache.Capacity()); ok &&
			m.cache.Used()+int64(len(entry.CachedData.Bytes())) > target {
			metric.FSCachePressureMemorySkipCounter.Inc()
			continue
		}

		LogEvent(ctx, str_set_memory_cache_entry_begin)
		m.cache.Set(ctx, key, entry.CachedData)
		LogEvent(ctx, str_set_memory_cache_entry_end)
	}
	return nil
}

func (m *MemCache) Flush(ctx context.Context) {
	m.cache.Flush(ctx)
}

func (m *MemCache) DeletePaths(
	ctx context.Context,
	paths []string,
) error {
	m.cache.DeletePaths(ctx, paths)
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
	allMemoryCaches.Delete(m)
}
