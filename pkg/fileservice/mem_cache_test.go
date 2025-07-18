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
	"testing"

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
	m := NewMemCache(fscache.ConstCapacity(size), nil, nil, "", false)
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

	// check double free
	// delete path will remove items from hashtable but items are still in queus and have reference counter 0.
	m.DeletePaths(ctx, []string{"foo"})

}

// TestHighConcurrency this test is to mainly test concurrency issue in objectCache
// and dataOverlap-checker.
func TestHighConcurrency(t *testing.T) {
	ctx := context.Background()
	m := NewMemCache(fscache.ConstCapacity(2), nil, nil, "", false)
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

func BenchmarkMemoryCacheUpdate(b *testing.B) {
	ctx := context.Background()

	cache := NewMemCache(
		fscache.ConstCapacity(1024),
		nil,
		nil,
		"",
		false,
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
		false,
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
		false,
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
