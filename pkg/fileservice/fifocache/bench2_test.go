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
	"math/rand/v2"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

const g_cache_size = 51200000 // 512M
const g_item_size = 128000    // 128K
const g_io_read_time = 20 * time.Microsecond

func cache_read(ctx context.Context, cache *Cache[int64, int64], key int64) {

	_, ok := cache.Get(ctx, key)
	if !ok {
		// cache miss and sleep penalty as IO read
		time.Sleep(g_io_read_time)
		cache.Set(ctx, key, int64(0), g_item_size)
	}
}

func get_rand(start int64, end int64, r *rand.Rand, mutex *sync.Mutex) int64 {
	mutex.Lock()
	defer mutex.Unlock()
	return start + r.Int64N(end-start)
}

func dataset_read(b *testing.B, ctx context.Context, cache *Cache[int64, int64], startkey int64, endkey int64, r *rand.Rand, mutex *sync.Mutex) {

	ncpu := runtime.NumCPU()
	var wg sync.WaitGroup

	for i := 0; i < ncpu; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < b.N; n++ {

				if n%ncpu != i {
					continue
				}

				//fmt.Printf("start = %d, end = %d\n", startkey, endkey)
				for range endkey - startkey {
					key := get_rand(startkey, endkey, r, mutex)
					cache_read(ctx, cache, key)
				}
			}
		}()
	}

	wg.Wait()
}

func data_shift(b *testing.B, time int64) {
	var mutex sync.Mutex
	ctx := context.Background()
	cache_size := g_cache_size
	cache := New[int64, int64](fscache.ConstCapacity(int64(cache_size)), ShardInt[int64], nil, nil, nil, false)
	r := rand.New(rand.NewPCG(1, 2))

	offset := int64(0)
	start := int64(0)
	end := int64(g_cache_size) / int64(g_item_size) * time
	d1 := []int64{start, end}
	offset += end
	d2 := []int64{offset + start, offset + end}
	offset += end
	d3 := []int64{offset + start, offset + end}

	b.ResetTimer()

	dataset_read(b, ctx, cache, d1[0], d1[1], r, &mutex)
	dataset_read(b, ctx, cache, d2[0], d2[1], r, &mutex)
	dataset_read(b, ctx, cache, d3[0], d3[1], r, &mutex)
}

func data_readNx(b *testing.B, time int64) {
	var mutex sync.Mutex
	ctx := context.Background()
	cache_size := g_cache_size
	cache := New[int64, int64](fscache.ConstCapacity(int64(cache_size)), ShardInt[int64], nil, nil, nil, false)
	start := int64(0)
	end := int64(g_cache_size) / int64(g_item_size) * time
	r := rand.New(rand.NewPCG(1, 2))

	b.ResetTimer()
	dataset_read(b, ctx, cache, start, end, r, &mutex)
}

func BenchmarkSimCacheRead1x(b *testing.B) {
	data_readNx(b, 1)
}

func BenchmarkSimCacheRead1xShift(b *testing.B) {
	data_shift(b, 1)
}

func BenchmarkSimCacheRead2x(b *testing.B) {
	data_readNx(b, 2)
}

func BenchmarkSimCacheRead2xShift(b *testing.B) {
	data_shift(b, 2)
}

func BenchmarkSimCacheRead4x(b *testing.B) {
	data_readNx(b, 4)
}

func BenchmarkSimCacheRead4xShift(b *testing.B) {
	data_shift(b, 4)
}
