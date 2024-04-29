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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCacheSetGet(t *testing.T) {
	cache := New[int, int](8, nil, ShardInt[int])

	cache.Set(1, 1, 1)
	n, ok := cache.Get(1)
	assert.True(t, ok)
	assert.Equal(t, 1, n)

	cache.Set(1, 1, 1)
	n, ok = cache.Get(1)
	assert.True(t, ok)
	assert.Equal(t, 1, n)

	_, ok = cache.Get(2)
	assert.False(t, ok)
}

func TestCacheEvict(t *testing.T) {
	cache := New[int, int](8, nil, ShardInt[int])
	for i := 0; i < 64; i++ {
		cache.Set(i, i, 1)
		if cache.used1+cache.used2 > cache.capacity {
			t.Fatalf("capacity %v, used1 %v used2 %v", cache.capacity, cache.used1, cache.used2)
		}
	}
}

func TestCacheEvict2(t *testing.T) {
	cache := New[int, int](2, nil, ShardInt[int])
	cache.Set(1, 1, 1)
	cache.Set(2, 2, 1)

	// 1 will be evicted
	cache.Set(3, 3, 1)
	v, ok := cache.Get(2)
	assert.True(t, ok)
	assert.Equal(t, 2, v)
	v, ok = cache.Get(3)
	assert.True(t, ok)
	assert.Equal(t, 3, v)

	// get 2, set 4, 3 will be evicted first
	cache.Get(2)
	cache.Get(2)
	cache.Set(4, 4, 1)
	v, ok = cache.Get(2)
	assert.True(t, ok)
	assert.Equal(t, 2, v)
	v, ok = cache.Get(4)
	assert.True(t, ok)
	assert.Equal(t, 4, v)
	assert.Equal(t, 1, cache.used1)
	assert.Equal(t, 1, cache.used2)
}

func TestCacheEvict3(t *testing.T) {
	nEvict := 0
	cache := New[int, bool](1024, func(_ int, _ bool) {
		nEvict++
	}, ShardInt[int])
	for i := 0; i < 1024; i++ {
		cache.Set(i, true, 1)
		cache.Get(i)
		cache.Get(i)
		assert.True(t, cache.used1+cache.used2 <= 1024)
		//assert.True(t, len(cache.values) <= 1024)
	}
	assert.Equal(t, 0, nEvict)

	for i := 0; i < 1024; i++ {
		cache.Set(10000+i, true, 1)
		assert.True(t, cache.used1+cache.used2 <= 1024)
		//assert.True(t, len(cache.values) <= 1024)
	}
	assert.Equal(t, 102, cache.used1)
	assert.Equal(t, 922, cache.used2)
	assert.Equal(t, 1024, nEvict)
}
