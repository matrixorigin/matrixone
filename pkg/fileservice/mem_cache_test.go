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

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/stretchr/testify/assert"
)

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

	size := int64(128)
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

	assert.Equal(t, int64(1), m.cache.Used())
	assert.Equal(t, int64(1), m.cache.Capacity()-m.cache.Available())
	assert.Equal(t, int64(size-1), m.cache.Available())

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

	assert.Equal(t, int64(1), m.cache.Capacity()-m.cache.Available())
	assert.Equal(t, int64(size)-1, m.cache.Available())
	assert.Equal(t, int64(1), m.cache.Used())

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
		setDone <- cache.cache.Set(ctx, key, data)
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
	assert.NoError(t, cache.cache.Set(ctx, key1, &blockingReleaseData{
		bytes:   []byte("a"),
		started: releaseStarted,
		unblock: releaseUnblock,
	}))

	setDone := make(chan error, 1)
	go func() {
		setDone <- cache.cache.Set(ctx, key2, staticTestData([]byte("b")))
	}()

	<-releaseStarted
	assert.NoError(t, cache.cache.Set(ctx, key1, staticTestData([]byte("c"))))
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
	assert.NoError(t, cache.cache.Set(ctx, key1, staticTestData([]byte("a"))))
	assert.Equal(t, "set", <-events)

	firstSetDone := make(chan error, 1)
	go func() {
		firstSetDone <- cache.cache.Set(ctx, key2, staticTestData([]byte("b")))
	}()

	<-evictStarted
	secondSetDone := make(chan error, 1)
	go func() {
		secondSetDone <- cache.cache.Set(ctx, key1, staticTestData([]byte("c")))
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
		assert.NoError(t, cache.cache.Set(ctx, key, staticTestData([]byte{byte(i)})))
	}
	assert.Equal(t, int64(10), cache.cache.Used())

	ret := EvictMemoryCachesToCapacityPercent(ctx, 50)
	assert.Equal(t, int64(5), ret["test-target-evict"])
	assert.LessOrEqual(t, cache.cache.Used(), int64(5))
}

func TestMemoryCachePressureAdmissionSkipsWritesAboveTarget(t *testing.T) {
	ctx := context.Background()
	clearMemoryCachePressureTargetForTest()
	defer clearMemoryCachePressureTargetForTest()

	cache := NewMemCache(
		fscache.ConstCapacity(10),
		nil,
		nil,
		"",
	)
	defer cache.Close(ctx)

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
	}
	assert.Equal(t, int64(5), cache.cache.Used())

	vec := &IOVector{
		FilePath: "foo",
		Entries: []IOEntry{{
			Offset:     5,
			Size:       1,
			CachedData: staticTestData([]byte{5}),
		}},
	}
	assert.NoError(t, cache.Update(ctx, vec, false))
	assert.Equal(t, int64(5), cache.cache.Used())

	_, ok := cache.cache.Get(ctx, fscache.CacheKey{Path: "foo", Offset: 5, Sz: 1})
	assert.False(t, ok)
}

func TestMemoryCachePressureAdmissionAdmitsGhostEntry(t *testing.T) {
	ctx := context.Background()
	clearMemoryCachePressureTargetForTest()
	defer clearMemoryCachePressureTargetForTest()

	cache := NewMemCache(
		fscache.ConstCapacity(4),
		nil,
		nil,
		"",
	)
	defer cache.Close(ctx)

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

	assert.Equal(t, int64(2), cache.cache.EvictToTargetWithWait(ctx, 2))
	assert.Equal(t, int64(2), cache.cache.Used())
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
	assert.Equal(t, int64(2), cache.cache.Used())
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
	assert.Equal(t, int64(2), cache.cache.Used())
	_, ok = cache.cache.Get(ctx, key0)
	assert.True(t, ok)
	_, ok = cache.cache.Get(ctx, key1)
	assert.False(t, ok)
}

func TestMemoryCachePressureAdmissionExpires(t *testing.T) {
	ctx := context.Background()
	clearMemoryCachePressureTargetForTest()
	defer clearMemoryCachePressureTargetForTest()

	cache := NewMemCache(
		fscache.ConstCapacity(10),
		nil,
		nil,
		"",
	)
	defer cache.Close(ctx)

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
	assert.Equal(t, int64(1), cache.cache.Used())
}
