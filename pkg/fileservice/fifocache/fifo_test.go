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
	cache := New[int, int](fscache.ConstCapacity(8), ShardInt[int], nil, nil, nil, false)

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
	cache := New[int, int](fscache.ConstCapacity(8), ShardInt[int], nil, nil, nil, false)
	for i := 0; i < 64; i++ {
		cache.Set(ctx, i, i, 1)
		if cache.Used() > cache.capacity() {
			t.Fatalf("capacity %v, usedSmall %v usedMain %v", cache.capacity(), cache.usedSmall.Load(), cache.usedMain.Load())
		}
	}
}

func TestCacheEvict2(t *testing.T) {
	ctx := context.Background()
	cache := New[int, int](fscache.ConstCapacity(20), ShardInt[int], nil, nil, nil, false)
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
		false,
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

func TestCacheOneHitWonder(t *testing.T) {
	ctx := context.Background()
	cache := New[int64, int64](fscache.ConstCapacity(1000), ShardInt[int64], nil, nil, nil, false)

	capsmall := int64(1000)
	for i := range capsmall {
		cache.Set(ctx, i, i, 1)
	}
	assert.Equal(t, int64(0), cache.usedMain.Load())
	//fmt.Printf("cache main %d, small %d\n", cache.usedMain.Load(), cache.usedSmall.Load())
}

func TestCacheMoveMain(t *testing.T) {
	ctx := context.Background()
	cache := New[int64, int64](fscache.ConstCapacity(100), ShardInt[int64], nil, nil, nil, false)

	// fill small fifo to 90
	for i := range int64(90) {
		cache.Set(ctx, 10000+i, 10000+i, 1)
	}

	results := [][]int64{{0, 100}, {0, 100}, {0, 100}, {0, 100}, {0, 100},
		{20, 80}, {40, 60}, {60, 40}, {80, 20},
		{90, 10}, {90, 10}, {90, 10}, {90, 10}, {90, 10}, {90, 10}, {90, 10}, {90, 10}, {90, 10}, {90, 10}, {90, 10}}

	for k := range int64(20) {
		//fmt.Printf("cache set 10 items\n")
		capsmall := int64(10)
		for i := range capsmall {
			cache.Set(ctx, k*10+i, k*10+i, 1)
		}

		//fmt.Printf("increment freq 2\n")
		// increment the freq
		for j := range 2 {
			for i := range capsmall {
				_, ok := cache.Get(ctx, k*10+i)
				assert.Equal(t, ok, true)
			}
			_ = j
		}
		//fmt.Printf("Add %d to %d and Try move to main\n", (k+1)*200, (k+1)*200+capsmall)
		// move to main
		for i := range capsmall {
			cache.Set(ctx, (k+1)*200+i, (k+1)*200+i, 1)
		}
		//fmt.Printf("cache main %d, small %d\n", cache.usedMain.Load(), cache.usedSmall.Load())

		assert.Equal(t, results[k][0], cache.usedMain.Load())
		assert.Equal(t, results[k][1], cache.usedSmall.Load())
	}

	assert.Equal(t, int64(90), cache.usedMain.Load())
	assert.Equal(t, int64(10), cache.usedSmall.Load())
	// remove all main 0 - 99
	//fmt.Printf("remove all main\n")
}

func TestCacheMoveGhost(t *testing.T) {
	ctx := context.Background()
	cache := New[int64, int64](fscache.ConstCapacity(100), ShardInt[int64], nil, nil, nil, false)

	// fill small fifo to 90
	for i := range int64(90) {
		cache.Set(ctx, 10000+i, 10000+i, 1)
	}

	for k := range int64(2) {
		//fmt.Printf("cache set 10 items\n")
		capsmall := int64(10)
		for i := range capsmall {
			cache.Set(ctx, k*10+i, k*10+i, 1)
		}

		//fmt.Printf("increment freq 2\n")
		// increment the freq
		for j := range 2 {
			for i := range capsmall {
				_, ok := cache.Get(ctx, k*10+i)
				assert.Equal(t, ok, true)
			}
			_ = j
		}
		//fmt.Printf("Add %d to %d and Try move to main\n", (k+1)*200, (k+1)*200+capsmall)
		// move to main
		for i := range capsmall {
			cache.Set(ctx, (k+1)*200+i, (k+1)*200+i, 1)
		}
		//fmt.Printf("cache main %d, small %d\n", cache.usedMain.Load(), cache.usedSmall.Load())
	}

	for i := 10000; i < 10020; i++ {
		assert.Equal(t, cache.ghost.contains(int64(i)), true)

	}

	//fmt.Printf("cache main %d, small %d\n", cache.usedMain.Load(), cache.usedSmall.Load())
	for i := 10000; i < 10020; i++ {
		cache.Set(ctx, int64(i), int64(i), 1)
		assert.Equal(t, cache.ghost.contains(int64(i)), false)
	}

	assert.Equal(t, cache.usedMain.Load(), int64(20))
	assert.Equal(t, cache.usedSmall.Load(), int64(80))
	//fmt.Printf("cache main %d, small %d\n", cache.usedMain.Load(), cache.usedSmall.Load())
	//assert.Equal(t, int64(10), cache.usedMain.Load())
	//assert.Equal(t, int64(10), cache.usedSmall.Load())

	// remove all main 0 - 99
	//fmt.Printf("remove all main\n")
}
