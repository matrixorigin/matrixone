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

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

// Cache implements an in-memory cache with FIFO-based eviction
// it's mostly like the S3-fifo, only without the ghost queue part
type Cache[K comparable, V any] struct {
	capacity     fscache.CapacityFunc
	capacity1    fscache.CapacityFunc
	keyShardFunc func(K) uint64

	postSet   func(ctx context.Context, key K, value V, size int64)
	postGet   func(ctx context.Context, key K, value V, size int64)
	postEvict func(ctx context.Context, key K, value V, size int64)

	htab  map[K]*_CacheItem[K, V]
	mutex sync.Mutex

	usedSmall int64
	small     Queue[*_CacheItem[K, V]]
	usedMain  int64
	main      Queue[*_CacheItem[K, V]]
	ghost     *ghost[K]
}

type _CacheItem[K comparable, V any] struct {
	key   K
	value V
	size  int64
	freq  int32
}

// not thread safe. Internal use only
func (c *_CacheItem[K, V]) inc() {
	if c.freq < 3 {
		c.freq += 1
	}
}

// not thread safe. Internal use only
func (c *_CacheItem[K, V]) dec() {
	if c.freq > 0 {
		c.freq -= 1
	}
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
		small:        *NewQueue[*_CacheItem[K, V]](),
		main:         *NewQueue[*_CacheItem[K, V]](),
		ghost:        newGhost[K](ghostsize),
		keyShardFunc: keyShardFunc,
		postSet:      postSet,
		postGet:      postGet,
		postEvict:    postEvict,
		htab:         make(map[K]*_CacheItem[K, V]),
	}
	return ret
}

// not thread safe. Internal use only
func (c *Cache[K, V]) set(ctx context.Context, key K, value V, size int64) *_CacheItem[K, V] {
	_, ok := c.htab[key]
	if ok {
		// existed
		return nil
	}

	item := &_CacheItem[K, V]{
		key:   key,
		value: value,
		size:  size,
	}
	c.htab[key] = item
	if c.postSet != nil {
		c.postSet(ctx, key, value, size)
	}

	return item
}

func (c *Cache[K, V]) Set(ctx context.Context, key K, value V, size int64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if item := c.set(ctx, key, value, size); item != nil {
		// enqueue
		c.enqueue(ctx, item)
		// evict
		c.evictAll(ctx, nil, 0)
	}
}

// not thread safe. Internal use only
func (c *Cache[K, V]) enqueue(ctx context.Context, item *_CacheItem[K, V]) {

	// enqueue
	if c.ghost.contains(item.key) {
		c.ghost.remove(item.key)
		c.main.enqueue(item)
		c.usedMain += item.size
	} else {
		c.small.enqueue(item)
		c.usedSmall += item.size
	}
}

func (c *Cache[K, V]) Get(ctx context.Context, key K) (value V, ok bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var item *_CacheItem[K, V]
	if item, ok = c.get(ctx, key); ok {
		item.inc()
		return item.value, true
	}
	return
}

// not thread safe. Internal use only
func (c *Cache[K, V]) get(ctx context.Context, key K) (value *_CacheItem[K, V], ok bool) {
	var item *_CacheItem[K, V]
	item, ok = c.htab[key]
	if !ok {
		return
	}
	if c.postGet != nil {
		c.postGet(ctx, item.key, item.value, item.size)
	}
	return item, true
}

func (c *Cache[K, V]) Delete(ctx context.Context, key K) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	item, ok := c.htab[key]
	if !ok {
		return
	}
	delete(c.htab, key)
	if c.postEvict != nil {
		c.postEvict(ctx, item.key, item.value, item.size)
	}
	// queues will be update in evict
}

func (c *Cache[K, V]) Evict(ctx context.Context, done chan int64, capacityCut int64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

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
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.used()
}

// not thread safe. Internal use only
func (c *Cache[K, V]) used() int64 {
	return c.usedSmall + c.usedMain
}

// not thread safe. Internal use only
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
		if c.usedSmall > target1 {
			c.evictSmall(ctx)
		} else {
			c.evictMain(ctx)
		}
	}

	return target
}

// not thread safe. Internal use only
func (c *Cache[K, V]) evictSmall(ctx context.Context) {
	// queue 1
	for {
		item, ok := c.small.dequeue()
		if !ok {
			// queue empty
			return
		}
		if item.freq > 1 {
			// put main
			c.main.enqueue(item)
			c.usedSmall -= item.size
			c.usedMain += item.size
		} else {
			// evict
			c.deleteItem(ctx, item)
			c.usedSmall -= item.size
			c.ghost.add(item.key)
			return
		}
	}
}

// not thread safe. Internal use only
func (c *Cache[K, V]) deleteItem(ctx context.Context, item *_CacheItem[K, V]) {
	delete(c.htab, item.key)
	if c.postEvict != nil {
		c.postEvict(ctx, item.key, item.value, item.size)
	}
}

// not thread safe. Internal use only
func (c *Cache[K, V]) evictMain(ctx context.Context) {
	// queue 2
	for {
		item, ok := c.main.dequeue()
		if !ok {
			// empty queue
			break
		}
		if item.freq > 0 {
			// re-enqueue
			item.dec()
			c.main.enqueue(item)
		} else {
			// evict
			c.deleteItem(ctx, item)
			c.usedMain -= item.size
			return
		}
	}
}
