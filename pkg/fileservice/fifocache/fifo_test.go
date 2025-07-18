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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/stretchr/testify/assert"
)

func TestCacheSetGet(t *testing.T) {
	ctx := context.Background()
	cache := New[int, int](fscache.ConstCapacity(8), ShardInt[int], nil, nil, nil)

	cache.Set(ctx, 1, 1, 1)
	n, ok := cache.Get(ctx, 1)
	assert.True(t, ok)
	assert.Equal(t, 1, n)

	cache.Set(ctx, 1, 1, 1)
	n, ok = cache.Get(ctx, 1)
	assert.True(t, ok)
	assert.Equal(t, 1, n)

	_, ok = cache.Get(ctx, 2)
	assert.False(t, ok)
}

func TestCacheEvict(t *testing.T) {
	ctx := context.Background()
	cache := New[int, int](fscache.ConstCapacity(8), ShardInt[int], nil, nil, nil)
	for i := 0; i < 64; i++ {
		cache.Set(ctx, i, i, 1)
		if cache.Used() > cache.capacity() {
			t.Fatalf("capacity %v, usedSmall %v usedMain %v", cache.capacity(), cache.usedSmall.Load(), cache.usedMain.Load())
		}
	}
}

func TestCacheEvict2(t *testing.T) {
	ctx := context.Background()
	cache := New[int, int](fscache.ConstCapacity(20), ShardInt[int], nil, nil, nil)
	cache.Set(ctx, 1, 1, 1)
	cache.Set(ctx, 2, 2, 1)

	// 1 will be evicted
	cache.Set(ctx, 3, 3, 1)
	v, ok := cache.Get(ctx, 2)
	assert.True(t, ok)
	assert.Equal(t, 2, v)
	v, ok = cache.Get(ctx, 3)
	assert.True(t, ok)
	assert.Equal(t, 3, v)

	// get 2, set 4, 3 will be evicted first
	cache.Get(ctx, 2)
	cache.Get(ctx, 2)
	cache.Set(ctx, 4, 4, 1)
	v, ok = cache.Get(ctx, 2)
	assert.True(t, ok)
	assert.Equal(t, 2, v)
	v, ok = cache.Get(ctx, 4)
	assert.True(t, ok)
	assert.Equal(t, 4, v)
	assert.Equal(t, int64(4), cache.usedSmall.Load())
	assert.Equal(t, int64(0), cache.usedMain.Load())
}

func TestCacheEvict3(t *testing.T) {
	ctx := context.Background()
	var nEvict, nGet, nSet int
	cache := New(fscache.ConstCapacity(1024),
		ShardInt[int],
		func(_ context.Context, _ int, _ bool, _ int64) {
			nSet++
		},
		func(_ context.Context, _ int, _ bool, _ int64) {
			nGet++
		},
		func(_ context.Context, _ int, _ bool, _ int64) {
			nEvict++
		},
	)
	for i := 0; i < 1024; i++ {
		cache.Set(ctx, i, true, 1)
		cache.Get(ctx, i)
		cache.Get(ctx, i)
		assert.True(t, cache.Used() <= 1024)
	}
	assert.Equal(t, 0, nEvict)
	assert.Equal(t, 1024, nSet)
	assert.Equal(t, 2048, nGet)

	for i := 0; i < 1024; i++ {
		cache.Set(ctx, 10000+i, true, 1)
		assert.True(t, cache.Used() <= 1024)
	}
	assert.Equal(t, int64(102), cache.usedSmall.Load())
	assert.Equal(t, int64(922), cache.usedMain.Load())
	assert.Equal(t, 1024, nEvict)
	assert.Equal(t, 2048, nSet)
	assert.Equal(t, 2048, nGet)
}

func TestDoubleFree(t *testing.T) {
	evicts := make(map[int]int)
	cache := New(
		fscache.ConstCapacity(1),
		ShardInt,
		nil, nil,
		func(ctx context.Context, key int, value int, size int64) {
			evicts[key]++
		},
	)
	// set
	cache.Set(t.Context(), 1, 1, 1)
	// delete, item still in queue
	cache.Delete(t.Context(), 1)
	// set to evict 1
	cache.Set(t.Context(), 2, 2, 1)
	// check
	assert.Equal(t, 1, evicts[1])
}
