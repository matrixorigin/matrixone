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
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"golang.org/x/sys/cpu"
)

const numShards = 256

// Cache implements an in-memory cache with FIFO-based eviction
type Cache[K comparable, V any] struct {
	capacity     fscache.CapacityFunc
	capacity1    fscache.CapacityFunc
	keyShardFunc func(K) uint64

	postSet   func(ctx context.Context, key K, value V, size int64)
	postGet   func(ctx context.Context, key K, value V, size int64)
	postEvict func(ctx context.Context, key K, value V, size int64)

	shards [numShards]struct {
		sync.Mutex
		values map[K]*_CacheItem[K, V]
		_      cpu.CacheLinePad
	}

	itemQueue chan *_CacheItem[K, V]

	queueLock sync.RWMutex
	used1     int64
	queue1    Queue[*_CacheItem[K, V]]
	used2     int64
	queue2    Queue[*_CacheItem[K, V]]
	ghostSize int64
	ghost     Queue[*_CacheItem[K, V]]

	capacityCut atomic.Int64
}

type _CacheItem[K comparable, V any] struct {
	key     K
	value   V
	valueOK bool
	size    int64
	count   atomic.Int32
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
	keyShardFunc func(K) uint64,
	postSet func(ctx context.Context, key K, value V, size int64),
	postGet func(ctx context.Context, key K, value V, size int64),
	postEvict func(ctx context.Context, key K, value V, size int64),
) *Cache[K, V] {
	ret := &Cache[K, V]{
		capacity: capacity,
		capacity1: func() int64 {
			return capacity() / 10
		},
		itemQueue:    make(chan *_CacheItem[K, V], runtime.GOMAXPROCS(0)*2),
		queue1:       *NewQueue[*_CacheItem[K, V]](),
		queue2:       *NewQueue[*_CacheItem[K, V]](),
		ghost:        *NewQueue[*_CacheItem[K, V]](),
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

func (c *Cache[K, V]) set(ctx context.Context, key K, value V, size int64) *_CacheItem[K, V] {
	shard := &c.shards[c.keyShardFunc(key)%numShards]
	shard.Lock()
	defer shard.Unlock()

	oldItem, ok := shard.values[key]
	if ok {

		// ghost item
		if !oldItem.valueOK {
			// insert new item
			item := &_CacheItem[K, V]{
				key:     key,
				value:   value,
				valueOK: true,
				size:    size,
			}
			item.count.Store(oldItem.count.Load())
			// replacing the oldItem. oldItem will be evicted from ghost queue eventually.
			shard.values[key] = item
			if c.postSet != nil {
				c.postSet(ctx, key, value, size)
			}
			return item
		}

		// existed and value ok, skip set
		return nil
	}

	item := &_CacheItem[K, V]{
		key:     key,
		value:   value,
		valueOK: true,
		size:    size,
	}
	shard.values[key] = item
	if c.postSet != nil {
		c.postSet(ctx, key, value, size)
	}

	return item
}

func (c *Cache[K, V]) Set(ctx context.Context, key K, value V, size int64) {
	if item := c.set(ctx, key, value, size); item != nil {
		// item inserted, enqueue
		c.enqueue(item)
		c.Evict(ctx, nil, 0)
	}
}

func (c *Cache[K, V]) enqueue(item *_CacheItem[K, V]) {
	if !c.queueLock.TryLock() {
		// try put itemQueue
		select {
		case c.itemQueue <- item:
			// let the queueLock holder do the job
			return
		default:
			// block until get lock
			c.queueLock.Lock()
			defer c.queueLock.Unlock()
		}
	} else {
		defer c.queueLock.Unlock()
	}

	// enqueue
	c.queue1.enqueue(item)
	c.used1 += item.size

	// help enqueue
	for {
		select {
		case item := <-c.itemQueue:
			c.queue1.enqueue(item)
			c.used1 += item.size
		default:
			return
		}
	}
}

func (c *Cache[K, V]) Get(ctx context.Context, key K) (value V, ok bool) {
	var item *_CacheItem[K, V]
	defer func() {
		// item ok, increase count
		if item != nil {
			item.inc()
		}
	}()

	shard := &c.shards[c.keyShardFunc(key)%numShards]
	shard.Lock()
	defer shard.Unlock()

	item, ok = shard.values[key]
	if !ok {
		// not exist
		return
	}

	// ghost item
	if !item.valueOK {
		ok = false
		return
	}

	if c.postGet != nil {
		c.postGet(ctx, item.key, item.value, item.size)
	}
	return item.value, true
}

func (c *Cache[K, V]) Delete(ctx context.Context, key K) {
	shard := &c.shards[c.keyShardFunc(key)%numShards]
	shard.Lock()
	defer shard.Unlock()
	item, ok := shard.values[key]
	if !ok {
		return
	}
	delete(shard.values, key)
	c.purgeItemValue(ctx, item)
	// we do not update queues here, to reduce cost
	// deleted item in queue will be evicted eventually.
}

func (c *Cache[K, V]) Evict(ctx context.Context, done chan int64, capacityCut int64) {
	if done == nil {
		// can be async
		if c.queueLock.TryLock() {
			defer c.queueLock.Unlock()
		} else {
			if capacityCut > 0 {
				// let the holder do more evict
				c.capacityCut.Add(capacityCut)
			}
			return
		}

	} else {
		if cap(done) < 1 {
			panic("should be buffered chan")
		}
		c.queueLock.Lock()
		defer c.queueLock.Unlock()
	}

	var target int64
	for {
		globalCapacityCut := c.capacityCut.Swap(0)
		target = c.capacity() - capacityCut - globalCapacityCut
		if target < 0 {
			target = 0
		}
		if c.used1+c.used2 <= target {
			break
		}
		target1 := c.capacity1() - capacityCut - globalCapacityCut
		if target1 < 0 {
			target1 = 0
		}
		if c.used1 > target1 {
			c.evict1(ctx)
		} else {
			c.evict2(ctx)
		}
	}
	if done != nil {
		done <- target
	}
}

// ForceEvict evicts n bytes despite capacity
func (c *Cache[K, V]) ForceEvict(ctx context.Context, n int64) {
	capacityCut := c.capacity() - c.used() + n
	c.Evict(ctx, nil, capacityCut)
}

func (c *Cache[K, V]) used() int64 {
	c.queueLock.RLock()
	defer c.queueLock.RUnlock()
	return c.used1 + c.used2
}

func (c *Cache[K, V]) evict1(ctx context.Context) {
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
			// put ghost
			c.enqueueGhost(ctx, item)
			c.evictGhost()
			c.used1 -= item.size
			return
		}
	}
}

func (c *Cache[K, V]) evict2(ctx context.Context) {
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
			// put ghost
			c.enqueueGhost(ctx, item)
			c.evictGhost()
			c.used2 -= item.size
			return
		}
	}
}

func (c *Cache[K, V]) enqueueGhost(ctx context.Context, item *_CacheItem[K, V]) {
	c.ghost.enqueue(item)
	c.ghostSize += item.size

	shard := &c.shards[c.keyShardFunc(item.key)%numShards]
	shard.Lock()
	defer shard.Unlock()
	c.purgeItemValue(ctx, item)
}

func (c *Cache[K, V]) purgeItemValue(ctx context.Context, item *_CacheItem[K, V]) {
	if !item.valueOK {
		return
	}
	if c.postEvict != nil {
		c.postEvict(ctx, item.key, item.value, item.size)
	}
	item.valueOK = false
	var zero V
	item.value = zero
}

func (c *Cache[K, V]) evictGhost() {
	ghostCapacity := c.capacity() - c.capacity1() // same to queue2 capacity
	for c.ghostSize > ghostCapacity {
		item, ok := c.ghost.dequeue()
		if !ok {
			break
		}
		c.ghostSize -= item.size
		c.deleteItem(item)
	}
}

func (c *Cache[K, V]) deleteItem(item *_CacheItem[K, V]) {
	shard := &c.shards[c.keyShardFunc(item.key)%numShards]
	shard.Lock()
	defer shard.Unlock()
	// item may be replaced in set, check before delete
	if shard.values[item.key] == item {
		delete(shard.values, item.key)
	}
}
