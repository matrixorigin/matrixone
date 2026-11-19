// Copyright 2026 Matrix Origin
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

package s3fifo

// The benchmarks of the fifocache package this package replaces, with their names and
// workloads, so that results compare with benchstat across versions.

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

type intValue int

func (intValue) Retain()  {}
func (intValue) Release() {}

func newIntCache(capacity int64, hash func(int) uint64) *Cache[int, intValue] {
	return New(Options[int, intValue]{Capacity: fscache.ConstCapacity(capacity), Hash: hash})
}

func shardInt(v int) uint64 { return uint64(v) }

func BenchmarkCacheConstruction(b *testing.B) {
	for _, entries := range []int{0, 64, 2048} {
		for _, collide := range []bool{false, true} {
			b.Run(fmt.Sprintf("entries=%d/collide=%t", entries, collide), func(b *testing.B) {
				hash := shardInt
				if collide {
					hash = func(int) uint64 { return 0 }
				}
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					cache := newIntCache(1<<20, hash)
					for key := 0; key < entries; key++ {
						cache.Set(key, intValue(key), 1)
					}
					if entries > 0 {
						if value, _, ok, _ := cache.Get(entries-1, nil); !ok || int(value) != entries-1 {
							b.Fatal("missing inserted value")
						}
					}
				}
			})
		}
	}
}

func BenchmarkSequentialSet(b *testing.B) {
	size := 65536
	cache := newIntCache(int64(size), shardInt)
	nElements := size * 16
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Set(i%nElements, intValue(i), int64(1+i%3))
	}
}

func BenchmarkParallelSet(b *testing.B) {
	size := 65536
	cache := newIntCache(int64(size), shardInt)
	nElements := size * 16
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			cache.Set(i%nElements, intValue(i), int64(1+i%3))
		}
	})
}

func BenchmarkGet(b *testing.B) {
	size := 65536
	cache := newIntCache(int64(size), shardInt)
	nElements := size * 16
	for i := 0; i < nElements; i++ {
		cache.Set(i, intValue(i), int64(1+i%3))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get(i%nElements, nil)
	}
}

func BenchmarkParallelGet(b *testing.B) {
	size := 65536
	cache := newIntCache(int64(size), shardInt)
	nElements := size * 16
	for i := 0; i < nElements; i++ {
		cache.Set(i, intValue(i), int64(1+i%3))
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			cache.Get(i%nElements, nil)
		}
	})
}

func BenchmarkParallelGetOrSet(b *testing.B) {
	size := 65536
	cache := newIntCache(int64(size), shardInt)
	nElements := size * 16
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			if i%2 == 0 {
				cache.Get(i%nElements, nil)
			} else {
				cache.Set(i%nElements, intValue(i), int64(1+i%3))
			}
		}
	})
}

func BenchmarkParallelEvict(b *testing.B) {
	size := 65536
	cache := newIntCache(int64(size), shardInt)
	nElements := size * 16
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			cache.Set(i%nElements, intValue(i), int64(1+i%3))
			cache.EvictTo(context.Background(), cache.Capacity())
		}
	})
}

func BenchmarkShardCacheKey(b *testing.B) {
	key := fscache.CacheKey{
		Sz:     42,
		Offset: 3,
		Path:   strings.Repeat("abc", 42),
	}
	b.ResetTimer()
	for range b.N {
		hashKey(key)
	}
}

type testBytes []byte

func (t testBytes) Bytes() []byte            { return t }
func (t testBytes) Size() int64              { return int64(len(t)) }
func (t testBytes) Capacity() int64          { return int64(cap(t)) }
func (t testBytes) Slice(n int) fscache.Data { return t[:n] }
func (t testBytes) Retain()                  {}
func (t testBytes) Release()                 {}
func newTestDataCache(c int64) *DataCache {
	return NewDataCache(DataOptions{Capacity: fscache.ConstCapacity(c)})
}
func benchDataKey(i int) fscache.CacheKey {
	return fscache.CacheKey{Path: "foo" + strconv.Itoa(i), Sz: int64(i % 65536), Offset: int64(i)}
}
func setTestBytes(c *DataCache, k fscache.CacheKey) {
	c.Set(context.Background(), k, testBytes([]byte("foo")))
}

func BenchmarkEnsureNBytesAndSet(b *testing.B) {
	ctx := context.Background()
	cache := newTestDataCache(1024)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cache.EnsureNBytes(ctx, 1)
		}
	})
}

func BenchmarkDataCacheSet(b *testing.B) {
	cache := newTestDataCache(1024)
	b.ResetTimer()
	for i := range b.N {
		setTestBytes(cache, benchDataKey(i))
	}
}

func BenchmarkDataCacheGet(b *testing.B) {
	cache := newTestDataCache(1024)
	key := fscache.CacheKey{Path: "foo", Sz: 42, Offset: 42}
	setTestBytes(cache, key)
	b.ResetTimer()
	for range b.N {
		cache.Get(context.Background(), key)
	}
}
