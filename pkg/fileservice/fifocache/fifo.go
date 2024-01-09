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
	"sync"
	"sync/atomic"
)

// Cache implements an in-memory cache with FIFO-based eviction
// it's mostly like the S3-fifo, only without the ghost queue part
type Cache[K comparable, V any] struct {
	capacity  int
	capacity1 int
	onEvict   func(K, V)

	mapLock sync.RWMutex
	values  map[K]*_CacheItem[K, V]

	queueLock sync.Mutex
	used1     int
	queue1    Queue[*_CacheItem[K, V]]
	used2     int
	queue2    Queue[*_CacheItem[K, V]]
}

type _CacheItem[K comparable, V any] struct {
	key   K
	value V
	size  int
	count atomic.Int32
}

func (c *_CacheItem[K, V]) inc() {
	for {
		cur := c.count.Load()
		if cur >= 3 {
			return
		}
		if c.count.CompareAndSwap(cur, cur+1) {
			return
		}
	}
}

func (c *_CacheItem[K, V]) dec() {
	for {
		cur := c.count.Load()
		if cur <= 0 {
			return
		}
		if c.count.CompareAndSwap(cur, cur-1) {
			return
		}
	}
}

func New[K comparable, V any](
	capacity int,
	onEvict func(K, V),
) *Cache[K, V] {
	return &Cache[K, V]{
		capacity:  capacity,
		capacity1: capacity / 10,
		values:    make(map[K]*_CacheItem[K, V]),
		queue1:    *NewQueue[*_CacheItem[K, V]](),
		queue2:    *NewQueue[*_CacheItem[K, V]](),
		onEvict:   onEvict,
	}
}

func (c *Cache[K, V]) Set(key K, value V, size int) {
	c.mapLock.Lock()
	_, ok := c.values[key]
	if ok {
		// existed
		c.mapLock.Unlock()
		return
	}

	item := &_CacheItem[K, V]{
		key:   key,
		value: value,
		size:  size,
	}
	c.values[key] = item
	c.mapLock.Unlock()

	c.queueLock.Lock()
	defer c.queueLock.Unlock()
	c.queue1.enqueue(item)
	c.used1 += size
	if c.used1+c.used2 > c.capacity {
		c.evict()
	}
}

func (c *Cache[K, V]) Get(key K) (value V, ok bool) {
	c.mapLock.RLock()
	var item *_CacheItem[K, V]
	item, ok = c.values[key]
	if !ok {
		c.mapLock.RUnlock()
		return
	}
	c.mapLock.RUnlock()
	item.inc()
	return item.value, true
}

func (c *Cache[K, V]) evict() {
	for c.used1+c.used2 > c.capacity {
		if c.used1 > c.capacity1 {
			c.evict1()
		} else {
			c.evict2()
		}
	}
}

func (c *Cache[K, V]) evict1() {
	// queue 1
	for {
		item, ok := c.queue1.dequeue()
		if !ok {
			// queue empty
			return
		}
		if item.count.Load() > 1 {
			// put queue2
			c.queue2.enqueue(item)
			c.used1 -= item.size
			c.used2 += item.size
		} else {
			// evict
			c.mapLock.Lock()
			delete(c.values, item.key)
			c.mapLock.Unlock()
			if c.onEvict != nil {
				c.onEvict(item.key, item.value)
			}
			c.used1 -= item.size
			return
		}
	}
}

func (c *Cache[K, V]) evict2() {
	// queue 2
	for {
		item, ok := c.queue2.dequeue()
		if !ok {
			// empty queue
			break
		}
		if item.count.Load() > 0 {
			// re-enqueue
			c.queue2.enqueue(item)
			item.dec()
		} else {
			// evict
			c.mapLock.Lock()
			delete(c.values, item.key)
			c.mapLock.Unlock()
			if c.onEvict != nil {
				c.onEvict(item.key, item.value)
			}
			c.used2 -= item.size
			return
		}
	}
}
