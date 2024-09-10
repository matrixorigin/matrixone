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

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"golang.org/x/sys/cpu"
)

// Cache implements an in-memory cache with FIFO-based eviction
// it's mostly like the S3-fifo, only without the ghost queue part
type Cache[K comparable, V any] struct {
	capacity     fscache.CapacityFunc
	capacity1    fscache.CapacityFunc
	keyShardFunc func(K) uint8

	postSet   func(key K, value V)
	postGet   func(key K, value V)
	postEvict func(key K, value V)

	shards [256]struct {
		sync.RWMutex
		values map[K]*_CacheItem[K, V]
		_      cpu.CacheLinePad
	}

	queueLock sync.Mutex
	used1     int64
	queue1    Queue[*_CacheItem[K, V]]
	used2     int64
	queue2    Queue[*_CacheItem[K, V]]
}

type _CacheItem[K comparable, V any] struct {
	key   K
	value V
	size  int64
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
	capacity fscache.CapacityFunc,
	keyShardFunc func(K) uint8,
	postSet func(key K, value V),
	postGet func(key K, value V),
	postEvict func(key K, value V),
) *Cache[K, V] {
	ret := &Cache[K, V]{
		capacity: capacity,
		capacity1: func() int64 {
			return capacity() / 10
		},
		queue1:       *NewQueue[*_CacheItem[K, V]](),
		queue2:       *NewQueue[*_CacheItem[K, V]](),
		keyShardFunc: keyShardFunc,
		postSet:      postSet,
		postGet:      postGet,
		postEvict:    postEvict,
	}
	for i := range ret.shards {
		ret.shards[i].values = make(map[K]*_CacheItem[K, V], 1024)
	}
	return ret
}

func (c *Cache[K, V]) Set(key K, value V, size int64) {
	shard := &c.shards[c.keyShardFunc(key)]
	shard.Lock()
	_, ok := shard.values[key]
	if ok {
		// existed
		shard.Unlock()
		return
	}

	item := &_CacheItem[K, V]{
		key:   key,
		value: value,
		size:  size,
	}
	shard.values[key] = item
	if c.postSet != nil {
		c.postSet(key, value)
	}
	shard.Unlock()

	c.queueLock.Lock()
	defer c.queueLock.Unlock()
	c.queue1.enqueue(item)
	c.used1 += size
	if c.used1+c.used2 > c.capacity() {
		c.evict(nil, 0)
	}
}

func (c *Cache[K, V]) Get(key K) (value V, ok bool) {
	shard := &c.shards[c.keyShardFunc(key)]
	shard.RLock()
	var item *_CacheItem[K, V]
	item, ok = shard.values[key]
	if !ok {
		shard.RUnlock()
		return
	}
	if c.postGet != nil {
		c.postGet(item.key, item.value)
	}
	shard.RUnlock()
	item.inc()
	return item.value, true
}

func (c *Cache[K, V]) Delete(key K) {
	shard := &c.shards[c.keyShardFunc(key)]
	shard.Lock()
	defer shard.Unlock()
	item, ok := shard.values[key]
	if !ok {
		return
	}
	delete(shard.values, key)
	if c.postEvict != nil {
		c.postEvict(item.key, item.value)
	}
	// queues will be update in evict
}

func (c *Cache[K, V]) evict(done chan int64, overEvict int64) {
	var target int64
	for {
		target = c.capacity() - overEvict
		if target < 0 {
			target = 0
		}
		if c.used1+c.used2 <= target {
			break
		}
		target1 := c.capacity1() - overEvict
		if target1 < 0 {
			target1 = 0
		}
		if c.used1 > target1 {
			c.evict1()
		} else {
			c.evict2()
		}
	}
	if done != nil {
		done <- target
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
			shard := &c.shards[c.keyShardFunc(item.key)]
			shard.Lock()
			delete(shard.values, item.key)
			if c.postEvict != nil {
				c.postEvict(item.key, item.value)
			}
			shard.Unlock()
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
			shard := &c.shards[c.keyShardFunc(item.key)]
			shard.Lock()
			delete(shard.values, item.key)
			if c.postEvict != nil {
				c.postEvict(item.key, item.value)
			}
			shard.Unlock()
			c.used2 -= item.size
			return
		}
	}
}

func (c *Cache[K, V]) Evict(done chan int64) {
	if done != nil && cap(done) < 1 {
		panic("should be buffered chan")
	}
	c.queueLock.Lock()
	defer c.queueLock.Unlock()
	c.evict(done, 0)
}
