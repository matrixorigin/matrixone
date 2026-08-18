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
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func requireDataCacheSet(t *testing.T, cache fscache.DataCache, ctx context.Context, key fscache.CacheKey, data fscache.Data) {
	t.Helper()
	_, err := cache.Set(ctx, key, data)
	require.NoError(t, err)
}

type reclaimTrackingAllocator struct {
	malloc.MemoryCacheAllocator
	reclaims atomic.Int64
}

func (a *reclaimTrackingAllocator) Reclaim() error {
	a.reclaims.Add(1)
	return nil
}

type closeTrackingAllocator struct {
	malloc.MemoryCacheAllocator
	closes atomic.Int64
}

func (a *closeTrackingAllocator) Close() error {
	a.closes.Add(1)
	return a.MemoryCacheAllocator.Close()
}

type oomExactAllocator struct{}

func (oomExactAllocator) Allocate(uint64, malloc.Hints) ([]byte, malloc.Deallocator, error) {
	return nil, nil, moerr.NewOOMNoCtx()
}

func (oomExactAllocator) BackingSize(size uint64) (uint64, error) {
	return size, nil
}

type blockingCacheDataReservation struct {
	cacheDataReservation
	committed chan struct{}
	unblock   chan struct{}
	once      sync.Once
}

func (r *blockingCacheDataReservation) commit() {
	r.cacheDataReservation.commit()
	r.once.Do(func() {
		close(r.committed)
	})
	<-r.unblock
}

func TestMemCacheLeak(t *testing.T) {
	ctx := context.Background()
	var counter perfcounter.CounterSet
	ctx = perfcounter.WithCounterSet(ctx, &counter)

	fs, err := NewMemoryFS("test", DisabledCacheConfig, nil)
	assert.Nil(t, err)
	err = fs.Write(ctx, IOVector{
		FilePath: "foo",
		Entries: []IOEntry{
			{
				Size: 3,
				Data: []byte("foo"),
			},
		},
	})
	assert.Nil(t, err)

	size := int64(DefaultCacheDataAllocator().BackingSize(1))
	m := NewMemCache(fscache.ConstCapacity(size), nil, nil, "")
	defer m.Close(ctx)

	newReadVec := func() *IOVector {
		vec := &IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Size: 3,
					ToCacheData: func(ctx context.Context, reader io.Reader, data []byte, allocator CacheDataAllocator) (fscache.Data, error) {
						cacheData := allocator.CopyToCacheData(ctx, []byte{42})
						return cacheData, nil
					},
				},
			},
		}
		return vec
	}

	vec := newReadVec()
	err = m.Read(ctx, vec)
	assert.Nil(t, err)
	vec.Release()

	vec = newReadVec()
	err = fs.Read(ctx, vec)
	assert.Nil(t, err)
	err = m.Update(ctx, vec, false)
	assert.Nil(t, err)
	vec.Release()

	assert.Equal(t, size, m.cache.Used())
	assert.Equal(t, size, m.cache.Capacity()-m.cache.Available())
	assert.Equal(t, int64(0), m.cache.Available())

	// read from cache
	newReadVec = func() *IOVector {
		vec := &IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Size: 3,
					ToCacheData: func(ctx context.Context, reader io.Reader, data []byte, allocator CacheDataAllocator) (fscache.Data, error) {
						cacheData := allocator.CopyToCacheData(ctx, []byte{42})
						return cacheData, nil
					},
				},
			},
		}
		return vec
	}
	vec = newReadVec()
	err = m.Read(ctx, vec)
	assert.Nil(t, err)
	vec.Release()

	vec = newReadVec()
	err = fs.Read(ctx, vec)
	assert.Nil(t, err)
	err = m.Update(ctx, vec, false)
	assert.Nil(t, err)
	vec.Release()

	assert.Equal(t, size, m.cache.Capacity()-m.cache.Available())
	assert.Equal(t, int64(0), m.cache.Available())
	assert.Equal(t, size, m.cache.Used())

}

func TestMemCacheUpdateRehomesForeignData(t *testing.T) {
	ctx := context.Background()
	foreign := DefaultCacheDataAllocator().CopyToCacheData(ctx, []byte("cache-data"))
	foreignBytes := foreign.(*Bytes)

	cache := NewMemCache(fscache.ConstCapacity(foreign.Capacity()), nil, nil, "")
	defer cache.Close(ctx)
	require.NotSame(t, cache.allocator.owner, foreignBytes.CacheDataOwner())

	vector := &IOVector{
		FilePath: "shared:/object",
		Entries: []IOEntry{{
			Size:       int64(len("cache-data")),
			CachedData: foreign,
		}},
	}
	require.NoError(t, cache.Update(ctx, vector, false))

	rehomed := vector.Entries[0].CachedData.(*Bytes)
	require.NotSame(t, foreign, rehomed)
	require.Same(t, cache.allocator.owner, rehomed.CacheDataOwner())
	require.Equal(t, []byte("cache-data"), rehomed.Bytes())

	vector.Release()
	read := &IOVector{FilePath: "shared:/object", Entries: []IOEntry{{Size: int64(len("cache-data"))}}}
	require.NoError(t, cache.Read(ctx, read))
	require.Same(t, rehomed, read.Entries[0].CachedData)
	read.Release()
}

func TestMemCacheUpdateRetainsOwnedDataWithoutCopy(t *testing.T) {
	ctx := context.Background()
	cache := NewMemCache(fscache.ConstCapacity(1024), nil, nil, "")
	defer cache.Close(ctx)

	data := cache.CopyToCacheData(ctx, []byte("cache-data"))
	vector := &IOVector{
		FilePath: "shared:/object",
		Entries: []IOEntry{{
			Size:       int64(len("cache-data")),
			CachedData: data,
		}},
	}
	require.NoError(t, cache.Update(ctx, vector, false))
	require.Same(t, data, vector.Entries[0].CachedData)
	vector.Release()
}

func TestMemCacheRehomeReservesCapacityBeforeCopy(t *testing.T) {
	ctx := context.Background()
	const capacity = 1 << 20
	const request = 700 << 10

	cache := NewMemCache(fscache.ConstCapacity(capacity), nil, nil, "")
	defer cache.Close(ctx)
	backing := cache.BackingSize(request)

	seed := &IOVector{
		FilePath: "shared:/seed",
		Entries: []IOEntry{{
			Size:       capacity,
			CachedData: cache.CopyToCacheData(ctx, make([]byte, capacity)),
		}},
	}
	require.NoError(t, cache.Update(ctx, seed, false))
	seed.Release()
	require.Equal(t, int64(capacity), cache.cache.Used())

	foreign := DefaultCacheDataAllocator().CopyToCacheData(ctx, make([]byte, request))
	defer foreign.Release()
	rehomed := cache.admitCacheData(ctx, foreign)
	rehomedBytes := rehomed.(*Bytes)
	require.Same(t, cache.allocator.owner, rehomedBytes.CacheDataOwner())
	require.Equal(t, int64(0), cache.cache.Used())

	stats, err := cache.arenaAllocator.Stats()
	require.NoError(t, err)
	require.LessOrEqual(t, stats.Allocated, uint64(capacity))
	require.Equal(t, int64(backing), cache.reservedBytes)

	rehomed.Release()
	require.Equal(t, int64(0), cache.reservedBytes)
}

func TestMemCacheCapacityReservationBypassesPendingAllocation(t *testing.T) {
	ctx := context.Background()
	const capacity = 1 << 20
	const request = 700 << 10

	cache := NewMemCache(fscache.ConstCapacity(capacity), nil, nil, "")
	defer cache.Close(ctx)
	backing := cache.BackingSize(request)
	first := cache.reserveCacheData(ctx, backing)

	second := cache.AllocateCacheDataWithHint(ctx, request, malloc.NoClear)
	admission, ok := second.(fscache.DataCacheAdmission)
	require.True(t, ok)
	require.False(t, admission.CacheAdmissionAllowed(cache.allocator.owner))
	require.Equal(t, int64(backing), cache.reservedBytes)
	second.Release()
	first.release()
	require.Equal(t, int64(0), cache.reservedBytes)
}

func TestMemCacheAllocationFailureReleasesCapacityReservation(t *testing.T) {
	ctx := context.Background()
	const capacity = 4 << 10

	cache := NewMemCache(fscache.ConstCapacity(capacity), nil, nil, "")
	defer cache.Close(ctx)
	cache.allocator.allocator = oomExactAllocator{}

	require.Panics(t, func() {
		cache.AllocateCacheData(ctx, capacity)
	})
	require.Zero(t, cache.reservedBytes)

	reservation := cache.reserveCacheData(ctx, capacity)
	require.NotNil(t, reservation)
	reservation.release()
	require.Zero(t, cache.reservedBytes)
}

func TestMemCacheCloseRetiresDedicatedAllocator(t *testing.T) {
	ctx := context.Background()
	cache := NewMemCache(fscache.ConstCapacity(4<<10), nil, nil, "")
	tracking := &closeTrackingAllocator{MemoryCacheAllocator: cache.arenaAllocator}
	cache.arenaAllocator = tracking

	cache.Close(ctx)
	require.Equal(t, int64(1), tracking.closes.Load())
}

func TestMemCacheReusesSmallPendingAllocation(t *testing.T) {
	ctx := context.Background()
	const request = 4 << 10

	cache := NewMemCache(fscache.ConstCapacity(1<<20), nil, nil, "")
	defer cache.Close(ctx)
	backing := cache.BackingSize(request)
	require.LessOrEqual(t, backing, maxIdleCacheDataBackingSize)

	first := cache.AllocateCacheData(ctx, request).(*Bytes)
	first.Release()
	require.Equal(t, int64(backing), cache.reservedBytes)

	second := cache.AllocateCacheData(ctx, request).(*Bytes)
	require.Same(t, first, second)
	require.Equal(t, int64(backing), cache.reservedBytes)
	second.Release()

	allocations := testing.AllocsPerRun(1000, func() {
		data := cache.AllocateCacheData(ctx, request)
		data.Release()
	})
	require.Zero(t, allocations)
}

func TestMemCacheReusedPendingAllocationIsCleared(t *testing.T) {
	ctx := context.Background()
	const request = 4 << 10

	cache := NewMemCache(fscache.ConstCapacity(1<<20), nil, nil, "")
	defer cache.Close(ctx)

	first := cache.AllocateCacheDataWithHint(ctx, request, malloc.NoClear).(*Bytes)
	for i := range first.Bytes() {
		first.Bytes()[i] = 0xff
	}
	first.Release()

	second := cache.AllocateCacheData(ctx, request).(*Bytes)
	for _, value := range second.Bytes() {
		require.Zero(t, value)
	}
	second.Release()
}

func TestMemCacheCapacityPressureDrainsIdlePendingAllocation(t *testing.T) {
	ctx := context.Background()
	const (
		firstRequest  = 4 << 10
		secondRequest = 8 << 10
	)

	cache := NewMemCache(fscache.ConstCapacity(8<<10), nil, nil, "")
	defer cache.Close(ctx)
	firstBacking := cache.BackingSize(firstRequest)
	secondBacking := cache.BackingSize(secondRequest)
	require.Less(t, firstBacking, secondBacking)
	require.Equal(t, cache.cache.Capacity(), int64(secondBacking))

	first := cache.AllocateCacheData(ctx, firstRequest).(*Bytes)
	first.Release()
	require.Equal(t, int64(firstBacking), cache.reservedBytes)

	second := cache.AllocateCacheData(ctx, secondRequest).(*Bytes)
	require.Same(t, cache.allocator.owner, second.CacheDataOwner())
	require.Equal(t, int64(secondBacking), cache.reservedBytes)
	second.Release()
}

func TestMemCacheFlushReleasesIdlePendingAllocation(t *testing.T) {
	ctx := context.Background()
	const request = 4 << 10

	cache := NewMemCache(fscache.ConstCapacity(1<<20), nil, nil, "")
	defer cache.Close(ctx)
	backing := cache.BackingSize(request)

	first := cache.AllocateCacheData(ctx, request).(*Bytes)
	first.Release()
	require.Equal(t, int64(backing), cache.reservedBytes)

	cache.Flush(ctx)
	require.Zero(t, cache.reservedBytes)

	stats, err := cache.arenaAllocator.Stats()
	require.NoError(t, err)
	require.Zero(t, stats.Allocated)

	second := cache.AllocateCacheData(ctx, request).(*Bytes)
	require.NotSame(t, first, second)
	second.Release()
}

func TestMemCacheForcedEvictionReleasesIdlePendingAllocation(t *testing.T) {
	ctx := context.Background()
	const request = 4 << 10

	cache := NewMemCache(fscache.ConstCapacity(1<<20), nil, nil, "")
	defer cache.Close(ctx)
	backing := cache.BackingSize(request)

	data := cache.AllocateCacheData(ctx, request)
	data.Release()
	require.Equal(t, int64(backing), cache.reservedBytes)

	done := make(chan int64, 1)
	cache.Evict(ctx, done)
	<-done
	require.Zero(t, cache.reservedBytes)
}

func TestMemCacheForcedEvictionReclaimsFIFOAllocation(t *testing.T) {
	ctx := context.Background()
	const request = 4 << 10

	cache := NewMemCache(fscache.ConstCapacity(1<<20), nil, nil, "")
	defer cache.Close(ctx)

	data := cache.AllocateCacheData(ctx, request)
	requireDataCacheSet(t, cache.cache, ctx, fscache.CacheKey{Path: "cached", Sz: request}, data)
	data.Release()
	require.Equal(t, int64(cache.BackingSize(request)), cache.cache.Used())
	GlobalMemoryCacheSizeHint.Store(1)
	defer GlobalMemoryCacheSizeHint.Store(0)

	tracking := &reclaimTrackingAllocator{MemoryCacheAllocator: cache.arenaAllocator}
	cache.arenaAllocator = tracking

	done := make(chan int64, 1)
	cache.Evict(ctx, done)
	<-done
	require.Zero(t, cache.cache.Used())
	require.Equal(t, int64(1), tracking.reclaims.Load())
}

func TestMemCacheCommitsReservationAfterFIFOUsageIsCharged(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const capacity = 1 << 20
	const request = 700 << 10

	cache := NewMemCache(fscache.ConstCapacity(capacity), nil, nil, "")
	defer cache.Close(context.Background())
	backing := cache.BackingSize(request)
	require.LessOrEqual(t, backing, capacity)

	first := cache.AllocateCacheDataWithHint(ctx, request, malloc.NoClear).(*Bytes)
	require.NotNil(t, first.reservation)
	reservation := &blockingCacheDataReservation{
		cacheDataReservation: first.reservation,
		committed:            make(chan struct{}),
		unblock:              make(chan struct{}),
	}
	first.reservation = reservation

	vector := &IOVector{
		FilePath: "shared:/reservation-handoff",
		Entries: []IOEntry{{
			Size:       request,
			CachedData: first,
		}},
	}
	defer vector.Release()

	updateDone := make(chan error, 1)
	go func() {
		updateDone <- cache.Update(ctx, vector, false)
	}()

	var unblockOnce sync.Once
	unblock := func() {
		unblockOnce.Do(func() {
			close(reservation.unblock)
		})
	}
	defer func() {
		unblock()
		require.NoError(t, <-updateDone)
	}()

	select {
	case <-reservation.committed:
	case <-ctx.Done():
		t.Fatal("cache reservation was not committed")
	}

	// The reservation hook runs after the FIFO has charged either used or
	// pending bytes. Cancel the second allocation context to prevent this test
	// from evicting the first item while checking the handoff boundary.
	require.Equal(t, int64(backing), cache.cache.Used())
	require.Equal(t, int64(0), cache.reservedBytes)
	secondCtx, cancelSecond := context.WithCancel(ctx)
	cancelSecond()
	second := cache.AllocateCacheDataWithHint(secondCtx, request, malloc.NoClear)
	defer second.Release()
	admission, ok := second.(fscache.DataCacheAdmission)
	require.True(t, ok)
	require.False(t, admission.CacheAdmissionAllowed(cache.allocator.owner))

	stats, err := cache.arenaAllocator.Stats()
	require.NoError(t, err)
	require.LessOrEqual(t, stats.Allocated, uint64(capacity))
}

func TestMemCacheOversizedDataBypassesAdmission(t *testing.T) {
	ctx := context.Background()
	const capacity = 1 << 20
	const request = 2 << 20

	cache := NewMemCache(fscache.ConstCapacity(capacity), nil, nil, "")
	defer cache.Close(ctx)
	require.Greater(t, cache.BackingSize(request), capacity)

	vector := &IOVector{
		FilePath: "shared:/oversized",
		Entries: []IOEntry{{
			Size:       request,
			CachedData: cache.AllocateCacheDataWithHint(ctx, request, malloc.NoClear),
		}},
	}
	admission, ok := vector.Entries[0].CachedData.(fscache.DataCacheAdmission)
	require.True(t, ok)
	require.False(t, admission.CacheAdmissionAllowed(cache.allocator.owner))

	require.NoError(t, cache.Update(ctx, vector, false))
	require.Equal(t, int64(0), cache.cache.Used())
	require.Equal(t, int64(0), cache.reservedBytes)
	vector.Release()
}

func TestMemCacheDuplicateSetKeepsReservationUntilDataRelease(t *testing.T) {
	ctx := context.Background()
	const capacity = 2 << 20
	const request = 700 << 10

	cache := NewMemCache(fscache.ConstCapacity(capacity), nil, nil, "")
	defer cache.Close(ctx)
	backing := cache.BackingSize(request)

	first := &IOVector{
		FilePath: "shared:/same-key",
		Entries: []IOEntry{{
			Size:       request,
			CachedData: cache.CopyToCacheData(ctx, make([]byte, request)),
		}},
	}
	require.NoError(t, cache.Update(ctx, first, false))
	first.Release()
	require.Equal(t, int64(0), cache.reservedBytes)

	duplicate := &IOVector{
		FilePath: "shared:/same-key",
		Entries: []IOEntry{{
			Size:       request,
			CachedData: cache.CopyToCacheData(ctx, make([]byte, request)),
		}},
	}
	require.Equal(t, int64(backing), cache.reservedBytes)
	require.NoError(t, cache.Update(ctx, duplicate, false))
	require.Equal(t, int64(backing), cache.reservedBytes)

	duplicate.Release()
	require.Equal(t, int64(0), cache.reservedBytes)
}

func TestMemCacheAllocatorMetricsRefreshAfterBurst(t *testing.T) {
	ctx := context.Background()
	name := t.Name()
	cache := NewMemCache(fscache.ConstCapacity(4<<20), nil, nil, name)
	defer cache.Close(ctx)

	for i := range 2 {
		vector := &IOVector{
			FilePath: "shared:/object",
			Entries: []IOEntry{{
				Offset:     int64(i) << 20,
				Size:       700 << 10,
				CachedData: NewBytes(make([]byte, 700<<10)),
			}},
		}
		require.NoError(t, cache.Update(ctx, vector, false))
		vector.Release()
	}

	physical, _ := metric.GetFsCacheBytesGauge(name, "mem")
	allocator := metric.GetFsCacheAllocatorStatsGauges(name, "mem")
	expected := float64(cache.cache.Used())
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(physical) == expected &&
			testutil.ToFloat64(allocator.Allocated) >= expected
	}, 3*time.Second, 10*time.Millisecond)
}

func TestMemCacheAllocatorMetricsAggregateSameComponent(t *testing.T) {
	ctx := context.Background()
	name := t.Name()
	first := NewMemCache(fscache.ConstCapacity(2<<20), nil, nil, name)
	defer first.Close(ctx)
	second := NewMemCache(fscache.ConstCapacity(2<<20), nil, nil, name)
	defer second.Close(ctx)

	for index, cache := range []*MemCache{first, second} {
		vector := &IOVector{
			FilePath: "shared:/object",
			Entries: []IOEntry{{
				Offset:     int64(index) << 20,
				Size:       700 << 10,
				CachedData: NewBytes(make([]byte, 700<<10)),
			}},
		}
		require.NoError(t, cache.Update(ctx, vector, false))
		vector.Release()
	}

	first.refreshAllocatorMetrics(true)
	physical, _ := metric.GetFsCacheBytesGauge(name, "mem")
	allocator := metric.GetFsCacheAllocatorStatsGauges(name, "mem")
	expected := float64(first.cache.Used() + second.cache.Used())
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(physical) == expected
	}, time.Second, 10*time.Millisecond)
	first.refreshAllocatorMetrics(true)
	require.Equal(t, float64(2), testutil.ToFloat64(allocator.Arenas))
	require.GreaterOrEqual(t, testutil.ToFloat64(allocator.Allocated), expected)
}

func TestMemCacheAllocatorMetricsSeparateServiceScopes(t *testing.T) {
	ctx := context.Background()
	name := t.Name()
	firstCN := newMemCacheWithMetricScope(fscache.ConstCapacity(2<<20), nil, nil, name, "CN/cn-1")
	defer firstCN.Close(ctx)
	secondCN := newMemCacheWithMetricScope(fscache.ConstCapacity(4<<20), nil, nil, name, "CN/cn-2")
	defer secondCN.Close(ctx)

	for index, cache := range []*MemCache{firstCN, secondCN} {
		vector := &IOVector{
			FilePath: "shared:/object",
			Entries: []IOEntry{{
				Offset:     int64(index) << 20,
				Size:       700 << 10,
				CachedData: NewBytes(make([]byte, 700<<10)),
			}},
		}
		require.NoError(t, cache.Update(ctx, vector, false))
		vector.Release()
		cache.refreshAllocatorMetrics(true)
	}

	firstInuse, firstCap := metric.GetFsCacheBytesGaugeWithScope("CN/cn-1", name, "mem")
	secondInuse, secondCap := metric.GetFsCacheBytesGaugeWithScope("CN/cn-2", name, "mem")
	firstAllocator := metric.GetFsCacheAllocatorStatsGaugesWithScope("CN/cn-1", name, "mem")
	secondAllocator := metric.GetFsCacheAllocatorStatsGaugesWithScope("CN/cn-2", name, "mem")
	require.Equal(t, float64(firstCN.cache.Used()), testutil.ToFloat64(firstInuse))
	require.Equal(t, float64(secondCN.cache.Used()), testutil.ToFloat64(secondInuse))
	require.Equal(t, float64(2<<20), testutil.ToFloat64(firstCap))
	require.Equal(t, float64(4<<20), testutil.ToFloat64(secondCap))
	require.Equal(t, float64(1), testutil.ToFloat64(firstAllocator.Arenas))
	require.Equal(t, float64(1), testutil.ToFloat64(secondAllocator.Arenas))
	require.GreaterOrEqual(t, testutil.ToFloat64(firstAllocator.Allocated), float64(firstCN.cache.Used()))
	require.GreaterOrEqual(t, testutil.ToFloat64(secondAllocator.Allocated), float64(secondCN.cache.Used()))
}

func TestMemCacheSeparatesPhysicalAndLogicalBytesMetrics(t *testing.T) {
	ctx := context.Background()
	name := t.Name()
	cache := NewMemCache(fscache.ConstCapacity(1024), nil, nil, name)
	defer cache.Close(ctx)

	physical, _ := metric.GetFsCacheBytesGauge(name, "mem")
	logical := metric.GetFsCacheLogicalBytesGauge(name, "mem")
	backingOverhead := metric.GetFsCacheBackingOverheadBytesGauge(name, "mem")
	data := NewBytes(make([]byte, 3, 8))
	key := fscache.CacheKey{Path: "foo", Sz: 3}
	requireDataCacheSet(t, cache.cache, ctx, key, data)
	data.Release()

	require.Equal(t, int64(8), cache.cache.Used())
	require.Equal(t, float64(8), testutil.ToFloat64(physical))
	require.Equal(t, float64(3), testutil.ToFloat64(logical))
	require.Equal(t, float64(5), testutil.ToFloat64(backingOverhead))

	cache.EvictToTarget(ctx, 0)
	require.Equal(t, float64(0), testutil.ToFloat64(physical))
	require.Equal(t, float64(0), testutil.ToFloat64(logical))
	require.Equal(t, float64(0), testutil.ToFloat64(backingOverhead))
}

// TestHighConcurrency this test is to mainly test concurrency issue in objectCache
// and dataOverlap-checker.
func TestHighConcurrency(t *testing.T) {
	ctx := context.Background()
	m := NewMemCache(fscache.ConstCapacity(2), nil, nil, "")
	defer m.Close(ctx)

	n := 10

	var vecArr []*IOVector
	for i := 0; i < n; i++ {
		vecArr = append(vecArr,
			&IOVector{
				FilePath: fmt.Sprintf("key%d", i),
				Entries: []IOEntry{
					{
						Size: 4,
						Data: []byte(fmt.Sprintf("val%d", i)),
					},
				},
			},
		)
	}

	// n go-routines are spun.
	wg := sync.WaitGroup{}
	wg.Add(n)

	for i := 0; i < n; i++ {
		// each go-routine tries to insert vecArr[i] for 10 times
		// these updates should invoke postSet and postEvict inside objectCache.
		// Since postSet and postEvict are guarded by objectCache mutex, this test
		// should not throw concurrency related panics.
		go func(idx int) {
			for j := 0; j < 10; j++ {
				_ = m.Update(ctx, vecArr[idx], false)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}

type blockingReleaseData struct {
	bytes   []byte
	started chan struct{}
	unblock chan struct{}
}

var _ fscache.Data = (*blockingReleaseData)(nil)

func (d *blockingReleaseData) Bytes() []byte {
	return d.bytes
}

func (d *blockingReleaseData) Size() int64 {
	return int64(len(d.bytes))
}

func (d *blockingReleaseData) Capacity() int64 {
	return int64(cap(d.bytes))
}

func (d *blockingReleaseData) Slice(length int) fscache.Data {
	return &blockingReleaseData{
		bytes:   d.bytes[:length],
		started: d.started,
		unblock: d.unblock,
	}
}

func (d *blockingReleaseData) Retain() {}

func (d *blockingReleaseData) Release() {
	select {
	case <-d.started:
	default:
		close(d.started)
	}
	<-d.unblock
}

type staticTestData []byte

var _ fscache.Data = staticTestData{}

func (d staticTestData) Bytes() []byte {
	return d
}

func (d staticTestData) Size() int64 {
	return int64(len(d))
}

func (d staticTestData) Capacity() int64 {
	return int64(cap(d))
}

func (d staticTestData) Slice(length int) fscache.Data {
	return d[:length]
}

func (d staticTestData) Retain() {}

func (d staticTestData) Release() {}

type blockingRetainData struct {
	bytes               []byte
	retainStarted       chan struct{}
	unblockRetain       chan struct{}
	releaseCalled       chan struct{}
	retainDone          atomic.Bool
	releaseBeforeRetain atomic.Bool
}

var _ fscache.Data = (*blockingRetainData)(nil)

func (d *blockingRetainData) Bytes() []byte {
	return d.bytes
}

func (d *blockingRetainData) Size() int64 {
	return int64(len(d.bytes))
}

func (d *blockingRetainData) Capacity() int64 {
	return int64(cap(d.bytes))
}

func (d *blockingRetainData) Slice(length int) fscache.Data {
	return &blockingRetainData{
		bytes:         d.bytes[:length],
		retainStarted: d.retainStarted,
		unblockRetain: d.unblockRetain,
		releaseCalled: d.releaseCalled,
	}
}

func (d *blockingRetainData) Retain() {
	select {
	case <-d.retainStarted:
	default:
		close(d.retainStarted)
	}
	<-d.unblockRetain
	d.retainDone.Store(true)
}

func (d *blockingRetainData) Release() {
	if !d.retainDone.Load() {
		d.releaseBeforeRetain.Store(true)
	}
	select {
	case <-d.releaseCalled:
	default:
		close(d.releaseCalled)
	}
}

func TestMemCacheRetainsBeforeSetVisible(t *testing.T) {
	ctx := context.Background()
	cache := NewMemCache(fscache.ConstCapacity(1), nil, nil, "")
	defer cache.Close(ctx)

	data := &blockingRetainData{
		bytes:         []byte("a"),
		retainStarted: make(chan struct{}),
		unblockRetain: make(chan struct{}),
		releaseCalled: make(chan struct{}),
	}
	key := fscache.CacheKey{Path: "foo", Offset: 0, Sz: 1}
	setDone := make(chan error, 1)
	go func() {
		_, err := cache.cache.Set(ctx, key, data)
		setDone <- err
	}()

	<-data.retainStarted
	cache.DeletePaths(ctx, []string{"foo"})
	select {
	case <-data.releaseCalled:
		t.Fatal("cache value was released before cache ownership was retained")
	default:
	}
	close(data.unblockRetain)
	assert.NoError(t, <-setDone)
	assert.False(t, data.releaseBeforeRetain.Load())
}

func TestMemCacheCanonicalizesDeletePaths(t *testing.T) {
	ctx := context.Background()
	for _, filePath := range []string{"shared:/foo", "/foo"} {
		t.Run(filePath, func(t *testing.T) {
			capacity := int64(DefaultCacheDataAllocator().BackingSize(3))
			cache := NewMemCache(fscache.ConstCapacity(capacity), nil, nil, "")
			defer cache.Close(ctx)

			vector := &IOVector{
				FilePath: filePath,
				Entries: []IOEntry{{
					Size:       3,
					CachedData: DefaultCacheDataAllocator().CopyToCacheData(ctx, []byte("foo")),
				}},
			}
			defer vector.Release()
			require.NoError(t, cache.Update(ctx, vector, false))

			require.NoError(t, cache.DeletePaths(ctx, []string{"foo"}))
			readVector := &IOVector{
				FilePath: "foo",
				Entries:  []IOEntry{{Size: 3}},
			}
			defer readVector.Release()
			require.NoError(t, cache.Read(ctx, readVector))
			require.False(t, readVector.Entries[0].done)
		})
	}
}

func TestMemCacheDeletePathsRejectsInvalidListAtomically(t *testing.T) {
	ctx := context.Background()
	capacity := int64(DefaultCacheDataAllocator().BackingSize(3))
	cache := NewMemCache(fscache.ConstCapacity(capacity), nil, nil, "")
	defer cache.Close(ctx)

	vector := &IOVector{
		FilePath: "shared:/foo",
		Entries: []IOEntry{{
			Size:       3,
			CachedData: DefaultCacheDataAllocator().CopyToCacheData(ctx, []byte("foo")),
		}},
	}
	defer vector.Release()
	require.NoError(t, cache.Update(ctx, vector, false))

	err := cache.DeletePaths(ctx, []string{"foo", "foo#bar"})
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidPath), "unexpected error: %v", err)

	readVector := &IOVector{
		FilePath: "foo",
		Entries:  []IOEntry{{Size: 3}},
	}
	defer readVector.Release()
	require.NoError(t, cache.Read(ctx, readVector))
	require.True(t, readVector.Entries[0].done)
	require.Equal(t, []byte("foo"), readVector.Entries[0].CachedData.Bytes())
}

func TestMemCacheSkipsStalePostEvictAfterReinsert(t *testing.T) {
	ctx := context.Background()
	releaseStarted := make(chan struct{})
	releaseUnblock := make(chan struct{})

	var mu sync.Mutex
	evicted := make(map[fscache.CacheKey]int)
	cache := NewMemCache(
		fscache.ConstCapacity(1),
		&CacheCallbacks{
			PostEvict: []CacheCallbackFunc{
				func(key fscache.CacheKey, _ fscache.Data) {
					mu.Lock()
					evicted[key]++
					mu.Unlock()
				},
			},
		},
		nil,
		"",
	)
	defer cache.Close(ctx)

	key1 := fscache.CacheKey{Path: "foo", Offset: 0, Sz: 1}
	key2 := fscache.CacheKey{Path: "bar", Offset: 0, Sz: 1}
	requireDataCacheSet(t, cache.cache, ctx, key1, &blockingReleaseData{
		bytes:   []byte("a"),
		started: releaseStarted,
		unblock: releaseUnblock,
	})

	setDone := make(chan error, 1)
	go func() {
		_, err := cache.cache.Set(ctx, key2, staticTestData([]byte("b")))
		setDone <- err
	}()

	<-releaseStarted
	requireDataCacheSet(t, cache.cache, ctx, key1, staticTestData([]byte("c")))
	close(releaseUnblock)
	assert.NoError(t, <-setDone)

	_, ok := cache.cache.Get(ctx, key1)
	assert.True(t, ok)

	mu.Lock()
	defer mu.Unlock()
	assert.Zero(t, evicted[key1])
	assert.Equal(t, 1, evicted[key2])
}

func TestMemCacheSerializesSameKeyCallbacks(t *testing.T) {
	ctx := context.Background()
	evictStarted := make(chan struct{})
	evictUnblock := make(chan struct{})
	events := make(chan string, 4)
	var evictOnce sync.Once
	cache := NewMemCache(
		fscache.ConstCapacity(1),
		&CacheCallbacks{
			PostSet: []CacheCallbackFunc{
				func(key fscache.CacheKey, _ fscache.Data) {
					if key.Path == "foo" {
						events <- "set"
					}
				},
			},
			PostEvict: []CacheCallbackFunc{
				func(key fscache.CacheKey, _ fscache.Data) {
					if key.Path != "foo" {
						return
					}
					triggered := false
					evictOnce.Do(func() {
						triggered = true
						close(evictStarted)
						<-evictUnblock
						events <- "evict"
					})
					if !triggered {
						return
					}
				},
			},
		},
		nil,
		"",
	)
	defer cache.Close(ctx)

	key1 := fscache.CacheKey{Path: "foo", Offset: 0, Sz: 1}
	key2 := fscache.CacheKey{Path: "bar", Offset: 0, Sz: 1}
	requireDataCacheSet(t, cache.cache, ctx, key1, staticTestData([]byte("a")))
	assert.Equal(t, "set", <-events)

	firstSetDone := make(chan error, 1)
	go func() {
		_, err := cache.cache.Set(ctx, key2, staticTestData([]byte("b")))
		firstSetDone <- err
	}()

	<-evictStarted
	secondSetDone := make(chan error, 1)
	go func() {
		_, err := cache.cache.Set(ctx, key1, staticTestData([]byte("c")))
		secondSetDone <- err
	}()

	select {
	case err := <-secondSetDone:
		assert.NoError(t, err)
		t.Fatal("same-key post-set callback was not serialized behind post-evict")
	default:
	}

	close(evictUnblock)
	assert.NoError(t, <-firstSetDone)
	assert.NoError(t, <-secondSetDone)
	assert.Equal(t, "evict", <-events)
	assert.Equal(t, "set", <-events)
}

func BenchmarkMemoryCacheUpdate(b *testing.B) {
	ctx := context.Background()

	cache := NewMemCache(
		fscache.ConstCapacity(1024),
		nil,
		nil,
		"",
	)
	defer cache.Flush(ctx)

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			vec := &IOVector{
				FilePath: fmt.Sprintf("%d", i),
				Entries: []IOEntry{
					{
						Data:       []byte("a"),
						Size:       1,
						CachedData: DefaultCacheDataAllocator().AllocateCacheData(ctx, 1),
					},
				},
			}
			if err := cache.Update(ctx, vec, false); err != nil {
				b.Fatal(err)
			}
			vec.Release()
		}
	})

}

func BenchmarkMemoryCacheRead(b *testing.B) {
	ctx := context.Background()

	cache := NewMemCache(
		fscache.ConstCapacity(1024),
		nil,
		nil,
		"",
	)
	defer cache.Flush(ctx)

	vec := &IOVector{
		FilePath: "foo",
		Entries: []IOEntry{
			{
				Data:       []byte("a"),
				Size:       1,
				CachedData: DefaultCacheDataAllocator().AllocateCacheData(ctx, 1),
			},
		},
	}
	if err := cache.Update(ctx, vec, false); err != nil {
		b.Fatal(err)
	}
	vec.Release()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			vec := &IOVector{
				FilePath: "foo",
				Entries: []IOEntry{
					{
						Size:        1,
						ToCacheData: CacheOriginalData,
					},
				},
			}
			if err := cache.Read(ctx, vec); err != nil {
				b.Fatal(err)
			}
			vec.Release()
		}
	})

}

func BenchmarkMemCacheSetSequential(b *testing.B) {
	benchmarkMemCacheSet(b, false)
}

func BenchmarkMemCacheSetParallel(b *testing.B) {
	benchmarkMemCacheSet(b, true)
}

func benchmarkMemCacheSet(b *testing.B, parallel bool) {
	ctx := context.Background()
	cache := NewMemCache(fscache.ConstCapacity(1024), nil, nil, "")
	b.Cleanup(func() { cache.Close(ctx) })

	keys := make([]fscache.CacheKey, 2048)
	for i := range keys {
		keys[i] = fscache.CacheKey{Path: fmt.Sprintf("benchmark-%d", i), Sz: 1, Offset: int64(i)}
	}
	data := staticTestData([]byte("a"))

	b.ReportAllocs()
	b.ResetTimer()
	if !parallel {
		for i := 0; i < b.N; i++ {
			if _, err := cache.cache.Set(ctx, keys[i%len(keys)], data); err != nil {
				b.Fatal(err)
			}
		}
		return
	}

	var next atomic.Uint64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := next.Add(1) - 1
			if _, err := cache.cache.Set(ctx, keys[i%uint64(len(keys))], data); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestMemoryCacheGlobalSizeHint(t *testing.T) {
	ctx := context.Background()

	cache := NewMemCache(
		fscache.ConstCapacity(1<<20),
		nil,
		nil,
		"test",
	)
	defer cache.Close(ctx)

	ch := make(chan int64, 1)
	cache.Evict(ctx, ch)
	n := <-ch
	if n > 1<<20 {
		t.Fatalf("got %v", n)
	}

	// shrink
	GlobalMemoryCacheSizeHint.Store(1 << 10)
	defer GlobalMemoryCacheSizeHint.Store(0)
	cache.Evict(ctx, ch)
	n = <-ch
	if n > 1<<10 {
		t.Fatalf("got %v", n)
	}

	// shrink
	GlobalMemoryCacheSizeHint.Store(1 << 9)
	defer GlobalMemoryCacheSizeHint.Store(0)
	ret := EvictMemoryCaches(ctx)
	if ret["test"] > 1<<9 {
		t.Fatalf("got %v", ret)
	}

}

func TestMemoryCacheEvictToCapacityPercent(t *testing.T) {
	ctx := context.Background()
	cache := NewMemCache(
		fscache.ConstCapacity(10),
		nil,
		nil,
		"test-target-evict",
	)
	defer cache.Close(ctx)

	for i := 0; i < 10; i++ {
		key := fscache.CacheKey{Path: fmt.Sprintf("key-%d", i), Offset: int64(i), Sz: 1}
		requireDataCacheSet(t, cache.cache, ctx, key, staticTestData([]byte{byte(i)}))
	}
	assert.Equal(t, int64(10), cache.cache.Used())

	ret := EvictMemoryCachesToCapacityPercent(ctx, 50)
	assert.Equal(t, int64(5), ret["test-target-evict"])
	assert.LessOrEqual(t, cache.cache.Used(), int64(5))
}

func TestMemoryCacheExplicitEvictionReclaimsAllocator(t *testing.T) {
	ctx := context.Background()
	cache := NewMemCache(fscache.ConstCapacity(10), nil, nil, "")
	defer cache.Close(ctx)

	tracking := &reclaimTrackingAllocator{MemoryCacheAllocator: cache.arenaAllocator}
	cache.arenaAllocator = tracking

	cache.EvictToCapacityPercent(ctx, 50)
	assert.Equal(t, int64(1), tracking.reclaims.Load())

	cache.Flush(ctx)
	assert.Equal(t, int64(2), tracking.reclaims.Load())
}

func TestMemoryCacheReservationReleaseDoesNotAllocate(t *testing.T) {
	cache := NewMemCache(fscache.ConstCapacity(1), nil, nil, "")
	defer cache.Close(context.Background())

	allocations := testing.AllocsPerRun(1000, func() {
		cache.capacityMu.Lock()
		cache.reservedBytes = 1
		cache.capacityMu.Unlock()
		cache.releaseReservedBytes(1)
	})
	assert.Zero(t, allocations)
}

func TestMemoryCachePressureAdmissionSkipsWritesAboveTarget(t *testing.T) {
	ctx := context.Background()
	clearMemoryCachePressureTargetForTest()
	defer clearMemoryCachePressureTargetForTest()

	unit := int64(DefaultCacheDataAllocator().BackingSize(1))
	cache := NewMemCache(
		fscache.ConstCapacity(10*unit),
		nil,
		nil,
		"",
	)
	defer cache.Close(ctx)
	assert.Equal(t, unit, int64(cache.BackingSize(1)))

	SetMemoryCachePressureTargetPercent(50, time.Now().Add(time.Minute))

	for i := 0; i < 5; i++ {
		vec := &IOVector{
			FilePath: "foo",
			Entries: []IOEntry{{
				Offset:     int64(i),
				Size:       1,
				CachedData: staticTestData([]byte{byte(i)}),
			}},
		}
		assert.NoError(t, cache.Update(ctx, vec, false))
		vec.Release()
	}
	assert.Equal(t, 5*unit, cache.cache.Used())

	vec := &IOVector{
		FilePath: "foo",
		Entries: []IOEntry{{
			Offset:     5,
			Size:       1,
			CachedData: staticTestData([]byte{5}),
		}},
	}
	assert.NoError(t, cache.Update(ctx, vec, false))
	assert.Equal(t, 5*unit, cache.cache.Used())

	_, ok := cache.cache.Get(ctx, fscache.CacheKey{Path: "foo", Offset: 5, Sz: 1})
	assert.False(t, ok)
}

func TestClearMemoryCachePressureTarget(t *testing.T) {
	clearMemoryCachePressureTargetForTest()
	defer clearMemoryCachePressureTargetForTest()

	SetMemoryCachePressureTargetPercent(50, time.Now().Add(time.Minute))
	target, ok := memoryCachePressureTarget(10)
	assert.True(t, ok)
	assert.Equal(t, int64(5), target)

	ClearMemoryCachePressureTarget()
	_, ok = memoryCachePressureTarget(10)
	assert.False(t, ok)
}

func TestMemoryCachePressureTargetOwnerIsolation(t *testing.T) {
	clearMemoryCachePressureTargetForTest()
	defer clearMemoryCachePressureTargetForTest()

	SetMemoryCachePressureTargetPercentByOwner("cn-rss", 50, time.Now().Add(time.Minute))
	SetMemoryCachePressureTargetPercentByOwner("workspace-rss", 80, time.Now().Add(time.Minute))

	target, ok := memoryCachePressureTarget(100)
	assert.True(t, ok)
	assert.Equal(t, int64(50), target)

	ClearMemoryCachePressureTargetByOwner("workspace-rss")
	target, ok = memoryCachePressureTarget(100)
	assert.True(t, ok)
	assert.Equal(t, int64(50), target)

	ClearMemoryCachePressureTargetByOwner("cn-rss")
	_, ok = memoryCachePressureTarget(100)
	assert.False(t, ok)
}

func TestMemoryCachePressureAdmissionAdmitsGhostEntry(t *testing.T) {
	ctx := context.Background()
	clearMemoryCachePressureTargetForTest()
	defer clearMemoryCachePressureTargetForTest()

	unit := int64(DefaultCacheDataAllocator().BackingSize(1))
	cache := NewMemCache(
		fscache.ConstCapacity(4*unit),
		nil,
		nil,
		"",
	)
	defer cache.Close(ctx)
	assert.Equal(t, unit, int64(cache.BackingSize(1)))

	for i := 0; i < 3; i++ {
		vec := &IOVector{
			FilePath: "foo",
			Entries: []IOEntry{{
				Offset:     int64(i),
				Size:       1,
				CachedData: staticTestData([]byte{byte(i)}),
			}},
		}
		assert.NoError(t, cache.Update(ctx, vec, false))
	}

	assert.Equal(t, 2*unit, cache.cache.EvictToTargetWithWait(ctx, 2*unit))
	assert.Equal(t, 2*unit, cache.cache.Used())
	key0 := fscache.CacheKey{Path: "foo", Offset: 0, Sz: 1}
	key1 := fscache.CacheKey{Path: "foo", Offset: 1, Sz: 1}
	key2 := fscache.CacheKey{Path: "foo", Offset: 2, Sz: 1}
	key3 := fscache.CacheKey{Path: "foo", Offset: 3, Sz: 1}
	_, ok := cache.cache.Get(ctx, key0)
	assert.False(t, ok)
	_, ok = cache.cache.Get(ctx, key1)
	assert.True(t, ok)
	_, ok = cache.cache.Get(ctx, key2)
	assert.True(t, ok)

	SetMemoryCachePressureTargetPercent(50, time.Now().Add(time.Minute))

	coldVec := &IOVector{
		FilePath: "foo",
		Entries: []IOEntry{{
			Offset:     3,
			Size:       1,
			CachedData: staticTestData([]byte{3}),
		}},
	}
	assert.NoError(t, cache.Update(ctx, coldVec, false))
	coldVec.Release()
	assert.Equal(t, 2*unit, cache.cache.Used())
	_, ok = cache.cache.Get(ctx, key3)
	assert.False(t, ok)

	ghostVec := &IOVector{
		FilePath: "foo",
		Entries: []IOEntry{{
			Offset:     0,
			Size:       1,
			CachedData: staticTestData([]byte{0}),
		}},
	}
	assert.NoError(t, cache.Update(ctx, ghostVec, false))
	ghostVec.Release()
	assert.Equal(t, 2*unit, cache.cache.Used())
	_, ok = cache.cache.Get(ctx, key0)
	assert.True(t, ok)
	_, ok = cache.cache.Get(ctx, key1)
	assert.False(t, ok)
}

func TestMemoryCachePressureAdmissionExpires(t *testing.T) {
	ctx := context.Background()
	clearMemoryCachePressureTargetForTest()
	defer clearMemoryCachePressureTargetForTest()

	unit := int64(DefaultCacheDataAllocator().BackingSize(1))
	cache := NewMemCache(
		fscache.ConstCapacity(10*unit),
		nil,
		nil,
		"",
	)
	defer cache.Close(ctx)
	assert.Equal(t, unit, int64(cache.BackingSize(1)))

	SetMemoryCachePressureTargetPercent(50, time.Now().Add(-time.Second))

	vec := &IOVector{
		FilePath: "foo",
		Entries: []IOEntry{{
			Offset:     0,
			Size:       1,
			CachedData: staticTestData([]byte{1}),
		}},
	}
	assert.NoError(t, cache.Update(ctx, vec, false))
	vec.Release()
	assert.Equal(t, unit, cache.cache.Used())
}
