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
	allocator           *bytesAllocator
	arenaAllocator      malloc.MemoryCacheAllocator
	allocatorGauges     metric.FsCacheAllocatorStatsGauges
	metricKey           memoryCacheMetricKey
	lastStatsUpdate     atomic.Int64
	statsRefreshPending atomic.Bool
	closed              atomic.Bool

	capacityMu      sync.Mutex
	reservedBytes   int64
	capacityChanged chan struct{}
}

// memoryCacheReservation accounts for an allocated buffer until FIFO insertion
// transfers its capacity into cache.Used(), or buffer Release deallocates it.
// This closes the allocation-before-Set window for foreign cache data.
type memoryCacheReservation struct {
	cache *MemCache
	bytes int64
	state atomic.Uint32
}

const (
	memoryCacheReservationPending uint32 = iota
	memoryCacheReservationCommitted
	memoryCacheReservationReleased
)

var _ cacheDataReservation = (*memoryCacheReservation)(nil)

func (r *memoryCacheReservation) commit() {
	if r.state.CompareAndSwap(memoryCacheReservationPending, memoryCacheReservationCommitted) {
		r.cache.releaseReservedBytes(r.bytes)
	}
}

func (r *memoryCacheReservation) release() {
	if r.state.CompareAndSwap(memoryCacheReservationPending, memoryCacheReservationReleased) {
		r.cache.releaseReservedBytes(r.bytes)
	}
}

type memoryCacheMetricKey struct {
	scope string
	name  string
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
	return newMemCacheWithMetricScope(capacity, callbacks, counterSets, name, "")
}

func newMemCacheWithMetricScope(
	capacity fscache.CapacityFunc,
	callbacks *CacheCallbacks,
	counterSets []*perfcounter.CounterSet,
	name string,
	metricScope string,
) *MemCache {

	inuseBytes, capacityBytes := metric.GetFsCacheBytesGaugeWithScope(metricScope, name, "mem")
	logicalInuseBytes := metric.GetFsCacheLogicalBytesGaugeWithScope(metricScope, name, "mem")
	backingOverheadBytes := metric.GetFsCacheBackingOverheadBytesGaugeWithScope(metricScope, name, "mem")
	allocatorGauges := metric.GetFsCacheAllocatorStatsGaugesWithScope(metricScope, name, "mem")
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
	cacheAllocator, arenaAllocator := newMemoryCacheDataAllocator()
	ret := &MemCache{
		counterSets:     counterSets,
		allocator:       cacheAllocator,
		arenaAllocator:  arenaAllocator,
		allocatorGauges: allocatorGauges,
		metricKey:       memoryCacheMetricKey{scope: metricScope, name: name},
		capacityChanged: make(chan struct{}),
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
		ret.signalCapacityChanged()
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
		allMemoryCaches.Store(ret, memoryCacheRegistration{
			name:      name,
			metricKey: ret.metricKey,
		})
		ret.refreshAllocatorMetrics(true)
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
	return m.allocateCacheData(ctx, size, malloc.NoHints)
}

func (m *MemCache) AllocateCacheDataWithHint(ctx context.Context, size int, hints malloc.Hints) fscache.Data {
	return m.allocateCacheData(ctx, size, hints)
}

func (m *MemCache) CopyToCacheData(ctx context.Context, data []byte) fscache.Data {
	ret := m.allocateCacheData(ctx, len(data), malloc.NoClear)
	copy(ret.Bytes(), data)
	return ret
}

func (m *MemCache) BackingSize(size int) int {
	return m.allocator.BackingSize(size)
}

func (m *MemCache) allocateCacheData(ctx context.Context, size int, hints malloc.Hints) fscache.Data {
	reservation := m.reserveCacheData(ctx, m.allocator.BackingSize(size))
	ret := m.allocator.allocateCacheBytes(size, hints)
	ret.reservation = reservation
	return ret
}

func (m *MemCache) reserveCacheData(ctx context.Context, bytes int) *memoryCacheReservation {
	if bytes <= 0 {
		panic("memory cache reservation requires positive bytes")
	}

	want := int64(bytes)
	for {
		m.capacityMu.Lock()
		capacity := m.cache.Capacity()
		used := m.cache.Used()
		if want <= capacity-used-m.reservedBytes {
			m.reservedBytes += want
			m.capacityMu.Unlock()
			return &memoryCacheReservation{cache: m, bytes: want}
		}
		target := capacity - m.reservedBytes - want
		changed := m.capacityChanged
		m.capacityMu.Unlock()

		if target >= 0 {
			if m.cache.EvictToTargetWithWait(withoutEventLogger(ctx), target) <= target {
				continue
			}
		}

		select {
		case <-ctx.Done():
			panic(ctx.Err())
		case <-changed:
		}
	}
}

func (m *MemCache) releaseReservedBytes(bytes int64) {
	m.capacityMu.Lock()
	m.reservedBytes -= bytes
	if m.reservedBytes < 0 {
		m.capacityMu.Unlock()
		panic("memory cache reservation underflow")
	}
	m.signalCapacityChangedLocked()
	m.capacityMu.Unlock()
}

func (m *MemCache) signalCapacityChanged() {
	m.capacityMu.Lock()
	m.signalCapacityChangedLocked()
	m.capacityMu.Unlock()
}

func (m *MemCache) signalCapacityChangedLocked() {
	close(m.capacityChanged)
	m.capacityChanged = make(chan struct{})
}

func (m *MemCache) refreshAllocatorMetrics(force bool) {
	if m.arenaAllocator == nil {
		return
	}

	now := time.Now().UnixNano()
	if !force {
		for {
			last := m.lastStatsUpdate.Load()
			if now-last < int64(time.Second) {
				m.scheduleAllocatorMetricsRefresh(time.Duration(int64(time.Second) - (now - last)))
				return
			}
			if m.lastStatsUpdate.CompareAndSwap(last, now) {
				break
			}
		}
	} else {
		m.lastStatsUpdate.Store(now)
	}

	stats, err := m.allocatorStats()
	if err != nil {
		return
	}
	fragmentation := uint64(0)
	if stats.Active > stats.Allocated {
		fragmentation = stats.Active - stats.Allocated
	}
	m.allocatorGauges.Allocated.Set(float64(stats.Allocated))
	m.allocatorGauges.Arenas.Set(float64(m.allocatorArenaCount()))
	m.allocatorGauges.Active.Set(float64(stats.Active))
	m.allocatorGauges.Fragmentation.Set(float64(fragmentation))
	m.allocatorGauges.Metadata.Set(float64(stats.Metadata))
	m.allocatorGauges.Resident.Set(float64(stats.Resident))
	m.allocatorGauges.Mapped.Set(float64(stats.Mapped))
	m.allocatorGauges.Retained.Set(float64(stats.Retained))
	m.allocatorGauges.Dirty.Set(float64(stats.Dirty))
	m.allocatorGauges.Muzzy.Set(float64(stats.Muzzy))
}

// allocatorStats aggregates every arena contributing to this metric component.
// Cache capacity gauges already aggregate values from same-named MemCaches, so
// publishing only the last arena would make allocator fragmentation appear
// smaller than the logical cache it is meant to explain.
func (m *MemCache) allocatorStats() (malloc.MemoryCacheStats, error) {
	if m.metricKey.name == "" {
		return m.arenaAllocator.Stats()
	}

	var total malloc.MemoryCacheStats
	var statsErr error
	allMemoryCaches.Range(func(key, value any) bool {
		if value.(memoryCacheRegistration).metricKey != m.metricKey {
			return true
		}
		stats, err := key.(*MemCache).arenaAllocator.Stats()
		if err != nil {
			statsErr = err
			return false
		}
		total.Allocated += stats.Allocated
		total.Active += stats.Active
		total.Metadata += stats.Metadata
		total.Resident += stats.Resident
		total.Mapped += stats.Mapped
		total.Retained += stats.Retained
		total.Dirty += stats.Dirty
		total.Muzzy += stats.Muzzy
		return true
	})
	if statsErr != nil {
		return malloc.MemoryCacheStats{}, statsErr
	}
	return total, nil
}

func (m *MemCache) allocatorArenaCount() int {
	if m.metricKey.name == "" {
		return 1
	}
	count := 0
	allMemoryCaches.Range(func(_, value any) bool {
		if value.(memoryCacheRegistration).metricKey == m.metricKey {
			count++
		}
		return true
	})
	return count
}

// scheduleAllocatorMetricsRefresh keeps allocator metrics cheap on the hot
// cache-set path while ensuring a burst's final state is eventually exported.
func (m *MemCache) scheduleAllocatorMetricsRefresh(delay time.Duration) {
	if !m.statsRefreshPending.CompareAndSwap(false, true) {
		return
	}
	time.AfterFunc(delay, func() {
		m.statsRefreshPending.Store(false)
		if !m.closed.Load() {
			m.refreshAllocatorMetrics(true)
		}
	})
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

	for i := range vector.Entries {
		entry := &vector.Entries[i]
		if entry.CachedData == nil {
			continue
		}
		if entry.fromCache == m {
			continue
		}

		data := m.admitCacheData(ctx, entry.CachedData)
		if data != entry.CachedData {
			entry.CachedData.Release()
			entry.CachedData = data
		}

		key := fscache.CacheKey{
			Path:   path.File,
			Offset: entry.Offset,
			Sz:     entry.Size,
		}
		LogEvent(ctx, str_set_memory_cache_entry_begin)
		inserted, err := m.cache.Set(ctx, key, entry.CachedData)
		LogEvent(ctx, str_set_memory_cache_entry_end)
		if errors.Is(err, fscache.ErrCacheAdmissionRejected) {
			metric.FSCachePressureMemorySkipCounter.Inc()
			continue
		}
		if err != nil {
			return err
		}
		if inserted {
			if reserved, ok := entry.CachedData.(fscache.DataCacheReservation); ok {
				reserved.CommitCacheReservation()
			}
		}
	}
	return nil
}

// admitCacheData makes this cache the physical owner of every admitted entry.
// Reads normally allocate through the FileService's MemCache and take the fast
// path. Data from a vector cache, disk cache, or a legacy caller can originate
// elsewhere; it is copied once here before MemCache retains it.
func (m *MemCache) admitCacheData(ctx context.Context, data fscache.Data) fscache.Data {
	// CopyToCacheData reserves capacity before allocation. The reservation remains
	// attached until postSet transfers it to FIFO accounting or the data releases.
	copyData := func(bytes []byte) fscache.Data {
		return m.CopyToCacheData(ctx, bytes)
	}
	if owned, ok := data.(fscache.DataOwnership); ok {
		if owned.CacheDataOwner() == m.allocator.owner {
			return data
		}
		return owned.RehomeCacheData(copyData)
	}
	return copyData(data.Bytes())
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
	m.closed.Store(true)
	m.Flush(ctx)
	allMemoryCaches.Delete(m)
	m.refreshAllocatorMetrics(true)
}
