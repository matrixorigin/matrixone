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
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

// Cache implements an in-memory cache with FIFO-based eviction
// it's mostly like the S3-fifo, only without the ghost queue part
type Cache[K comparable, V any] struct {
	capacity  fscache.CapacityFunc
	capacity1 fscache.CapacityFunc

	postSet   func(ctx context.Context, key K, value V, size int64)
	postGet   func(ctx context.Context, key K, value V, size int64)
	postEvict func(ctx context.Context, key K, value V, size int64)

	htab *ShardMap[K, *_CacheItem[K, V]]

	usedSmall atomic.Int64
	small     Queue[*_CacheItem[K, V]]
	usedMain  atomic.Int64
	main      Queue[*_CacheItem[K, V]]
	ghost     *ghost[K]
}

type _CacheItem[K comparable, V any] struct {
	mu    sync.Mutex
	key   K
	value V
	size  int64
	freq  int8
}

func (c *_CacheItem[K, V]) inc() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.freq < 3 {
		c.freq += 1
	}
}

func (c *_CacheItem[K, V]) dec() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.freq > 0 {
		c.freq -= 1
	}
}

func (c *_CacheItem[K, V]) getFreq() int8 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.freq
}

func (c *_CacheItem[K, V]) postFunc(ctx context.Context, fn func(ctx context.Context, key K, value V, size int64)) {
	if fn == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	fn(ctx, c.key, c.value, c.size)
}

// assume cache size is 128K
// if cache capacity is smaller than 4G, ghost size is 100%.  Otherwise, 50%
func estimateGhostSize(capacity int64) int {
	estimate := int(capacity / int64(128000))
	if capacity > 8000000000 { // 8G
		// only 50%
		estimate /= 2
	}
	if estimate < 12800 {
		estimate = 12800
	}
	return estimate
}

func New[K comparable, V any](
	capacity fscache.CapacityFunc,
	keyShardFunc func(K) uint64,
	postSet func(ctx context.Context, key K, value V, size int64),
	postGet func(ctx context.Context, key K, value V, size int64),
	postEvict func(ctx context.Context, key K, value V, size int64),
) *Cache[K, V] {

	ghostsize := estimateGhostSize(capacity())
	ret := &Cache[K, V]{
		capacity: capacity,
		capacity1: func() int64 {
			return capacity() / 10
		},
		small:     *NewQueue[*_CacheItem[K, V]](),
		main:      *NewQueue[*_CacheItem[K, V]](),
		ghost:     newGhost[K](ghostsize),
		postSet:   postSet,
		postGet:   postGet,
		postEvict: postEvict,
		htab:      NewShardMap[K, *_CacheItem[K, V]](keyShardFunc),
	}
	return ret
}

func (c *Cache[K, V]) set(ctx context.Context, key K, value V, size int64) *_CacheItem[K, V] {
	item := &_CacheItem[K, V]{
		key:   key,
		value: value,
		size:  size,
	}

	ok := c.htab.Set(key, item)
	if !ok {
		// existed
		return nil
	}

	return item
}

func (c *Cache[K, V]) Set(ctx context.Context, key K, value V, size int64) {
	if item := c.set(ctx, key, value, size); item != nil {

		// post set
		item.postFunc(ctx, c.postSet)

		// enqueue
		c.enqueue(ctx, item)
		// evict
		c.evictAll(ctx, nil, 0)
	}
}

func (c *Cache[K, V]) enqueue(ctx context.Context, item *_CacheItem[K, V]) {

	// enqueue
	if c.ghost.contains(item.key) {
		c.ghost.remove(item.key)
		c.main.enqueue(item)
		c.usedMain.Add(item.size)
	} else {
		c.small.enqueue(item)
		c.usedSmall.Add(item.size)
	}
}

func (c *Cache[K, V]) Get(ctx context.Context, key K) (value V, ok bool) {
	var item *_CacheItem[K, V]
	if item, ok = c.get(ctx, key); ok {

		// postGet
		item.postFunc(ctx, c.postGet)

		// increment
		item.inc()
		return item.value, true
	}
	return
}

func (c *Cache[K, V]) get(ctx context.Context, key K) (value *_CacheItem[K, V], ok bool) {
	var item *_CacheItem[K, V]
	item, ok = c.htab.Get(key)
	if !ok {
		return
	}
	return item, true
}

func (c *Cache[K, V]) Delete(ctx context.Context, key K) {
	item, ok := c.htab.Get(key)
	if !ok {
		return
	}
	c.htab.Remove(key)

	// post evict
	item.postFunc(ctx, c.postEvict)
	// queues will be update in evict
}

func (c *Cache[K, V]) Evict(ctx context.Context, done chan int64, capacityCut int64) {
	target := c.evictAll(ctx, done, capacityCut)
	if done != nil {
		done <- target
	}
}

// ForceEvict evicts n bytes despite capacity
func (c *Cache[K, V]) ForceEvict(ctx context.Context, n int64) {
	capacityCut := c.capacity() - c.used() + n
	c.Evict(ctx, nil, capacityCut)
}

func (c *Cache[K, V]) Used() int64 {
	return c.used()
}

func (c *Cache[K, V]) used() int64 {
	return c.usedSmall.Load() + c.usedMain.Load()
}

func (c *Cache[K, V]) evictAll(ctx context.Context, done chan int64, capacityCut int64) int64 {
	var target int64
	for {
		target = c.capacity() - capacityCut
		if target < 0 {
			target = 0
		}
		if c.used() <= target {
			break
		}
		target1 := c.capacity1() - capacityCut
		if target1 < 0 {
			target1 = 0
		}
		if c.usedSmall.Load() > target1 {
			c.evictSmall(ctx)
		} else {
			c.evictMain(ctx)
		}
	}

	return target
}

func (c *Cache[K, V]) evictSmall(ctx context.Context) {
	// queue 1
	for {
		item, ok := c.small.dequeue()
		if !ok {
			// queue empty
			return
		}
		if item.getFreq() > 1 {
			// put main
			c.main.enqueue(item)
			c.usedSmall.Add(-item.size)
			c.usedMain.Add(item.size)
		} else {
			// evict
			c.deleteItem(ctx, item)
			c.usedSmall.Add(-item.size)
			c.ghost.add(item.key)
			return
		}
	}
}

func (c *Cache[K, V]) deleteItem(ctx context.Context, item *_CacheItem[K, V]) {
	c.htab.Remove(item.key)

	// post evict
	item.postFunc(ctx, c.postEvict)
}

func (c *Cache[K, V]) evictMain(ctx context.Context) {
	// queue 2
	for {
		item, ok := c.main.dequeue()
		if !ok {
			// empty queue
			break
		}
		if item.getFreq() > 0 {
			// re-enqueue
			item.dec()
			c.main.enqueue(item)
		} else {
			// evict
			c.deleteItem(ctx, item)
			c.usedMain.Add(-item.size)
			return
		}
	}
}
