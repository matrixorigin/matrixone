// Copyright 2024 Matrix Origin
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

package fifocache

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

func BenchmarkSequentialSet(b *testing.B) {
	ctx := context.Background()
	size := 65536
	cache := New[int, int](fscache.ConstCapacity(int64(size)), ShardInt[int], nil, nil, nil)
	nElements := size * 16
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Set(ctx, i%nElements, i, int64(1+i%3))
	}
}

func BenchmarkParallelSet(b *testing.B) {
	ctx := context.Background()
	size := 65536
	cache := New[int, int](fscache.ConstCapacity(int64(size)), ShardInt[int], nil, nil, nil)
	nElements := size * 16
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			cache.Set(ctx, i%nElements, i, int64(1+i%3))
		}
	})
}

func BenchmarkGet(b *testing.B) {
	ctx := context.Background()
	size := 65536
	cache := New[int, int](fscache.ConstCapacity(int64(size)), ShardInt[int], nil, nil, nil)
	nElements := size * 16
	for i := 0; i < nElements; i++ {
		cache.Set(ctx, i, i, int64(1+i%3))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get(ctx, i%nElements)
	}
}

func BenchmarkParallelGet(b *testing.B) {
	ctx := context.Background()
	size := 65536
	cache := New[int, int](fscache.ConstCapacity(int64(size)), ShardInt[int], nil, nil, nil)
	nElements := size * 16
	for i := 0; i < nElements; i++ {
		cache.Set(ctx, i, i, int64(1+i%3))
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			cache.Get(ctx, i%nElements)
		}
	})
}

func BenchmarkParallelGetOrSet(b *testing.B) {
	ctx := context.Background()
	size := 65536
	cache := New[int, int](fscache.ConstCapacity(int64(size)), ShardInt[int], nil, nil, nil)
	nElements := size * 16
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			if i%2 == 0 {
				cache.Get(ctx, i%nElements)
			} else {
				cache.Set(ctx, i%nElements, i, int64(1+i%3))
			}
		}
	})
}

func BenchmarkParallelEvict(b *testing.B) {
	ctx := context.Background()
	size := 65536
	cache := New[int, int](fscache.ConstCapacity(int64(size)), ShardInt[int], nil, nil, nil)
	nElements := size * 16
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		ch := make(chan int64, 1)
		for i := 0; pb.Next(); i++ {
			cache.Set(ctx, i%nElements, i, int64(1+i%3))
			cache.Evict(ctx, ch, 0)
			target := <-ch
			if target != 65536 {
				panic(fmt.Sprintf("got %v", target))
			}
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
		shardCacheKey(key)
	}
}
