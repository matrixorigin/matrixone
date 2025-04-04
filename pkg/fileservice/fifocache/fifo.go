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
	"container/list"
	"context"
	"sync"
	"sync/atomic"

	"golang.org/x/sys/cpu"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

const numShards = 256

// Cache implements an in-memory cache with LRU-based eviction
type Cache[K comparable, V any] struct {
	capacity     fscache.CapacityFunc
	keyShardFunc func(K) uint64

	postSet   func(ctx context.Context, key K, value V, size int64)
	postGet   func(ctx context.Context, key K, value V, size int64)
	postEvict func(ctx context.Context, key K, value V, size int64)

	shards [numShards]struct {
		sync.Mutex
		values map[K]*_CacheItem[K, V]
		_      cpu.CacheLinePad
	}

	queueLock sync.RWMutex
	used      int64
	queue     *list.List // Doubly linked list for LRU

	capacityCut atomic.Int64
}

type _CacheItem[K comparable, V any] struct {
	key   K
	value V
	size  int64
	elem  *list.Element // Element in the linked list
}

func New[K comparable, V any](
	capacity fscache.CapacityFunc,
	keyShardFunc func(K) uint64,
	postSet func(ctx context.Context, key K, value V, size int64),
	postGet func(ctx context.Context, key K, value V, size int64),
	postEvict func(ctx context.Context, key K, value V, size int64),
) *Cache[K, V] {
	ret := &Cache[K, V]{
		capacity:     capacity,
		keyShardFunc: keyShardFunc,
		queue:        list.New(), // Initialize the linked list
		postSet:      postSet,
		postGet:      postGet,
		postEvict:    postEvict,
	}
	for i := range ret.shards {
		ret.shards[i].values = make(map[K]*_CacheItem[K, V], 1024)
	}
	return ret
}

func (c *Cache[K, V]) set(ctx context.Context, key K, value V, size int64) (added bool) {
	shard := &c.shards[c.keyShardFunc(key)%numShards]
	shard.Lock()
	defer shard.Unlock()
	_, ok := shard.values[key]
	if ok {
		// existed
		return false
	}

	c.queueLock.Lock()
	defer c.queueLock.Unlock()

	c.Evict(ctx, nil, size)

	item := &_CacheItem[K, V]{
		key:   key,
		value: value,
		size:  size,
	}
	item.elem = c.queue.PushBack(item)
	shard.values[key] = item
	if c.postSet != nil {
		c.postSet(ctx, key, value, size)
	}

	c.used += item.size
	return true
}

func (c *Cache[K, V]) Set(ctx context.Context, key K, value V, size int64) {
	c.set(ctx, key, value, size)
}

func (c *Cache[K, V]) Get(ctx context.Context, key K) (value V, ok bool) {
	shard := &c.shards[c.keyShardFunc(key)%numShards]
	shard.Lock()
	item, ok := shard.values[key]
	if !ok {
		shard.Unlock()
		return
	}
	if c.postGet != nil {
		c.postGet(ctx, item.key, item.value, item.size)
	}
	c.queueLock.Lock()
	defer c.queueLock.Unlock()
	c.queue.MoveToBack(item.elem)
	shard.Unlock()
	return item.value, true
}

func (c *Cache[K, V]) Delete(ctx context.Context, key K) {
	shard := &c.shards[c.keyShardFunc(key)%numShards]
	shard.Lock()
	item, ok := shard.values[key]
	if !ok {
		shard.Unlock()
		return
	}
	delete(shard.values, key)
	shard.Unlock()
	if c.postEvict != nil {
		c.postEvict(ctx, item.key, item.value, item.size)
	}
	c.queueLock.Lock()
	defer c.queueLock.Unlock()
	c.queue.Remove(item.elem)
	c.used -= item.size
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
		if c.used <= target {
			break
		}
		c.evict(ctx)
	}
	if done != nil {
		done <- target
	}
}

// ForceEvict evicts n bytes despite capacity
func (c *Cache[K, V]) ForceEvict(ctx context.Context, n int64) {
	capacityCut := c.capacity() - c.used + n
	c.Evict(ctx, nil, capacityCut)
}

func (c *Cache[K, V]) Used() int64 {
	c.queueLock.RLock()
	defer c.queueLock.RUnlock()
	return c.used
}

func (c *Cache[K, V]) evict(ctx context.Context) {
	// queue
	for {
		if c.queue.Len() == 0 {
			return
		}
		elem := c.queue.Front()
		item := elem.Value.(*_CacheItem[K, V])
		shard := &c.shards[c.keyShardFunc(item.key)%numShards]
		shard.Lock()
		delete(shard.values, item.key)
		shard.Unlock()
		if c.postEvict != nil {
			c.postEvict(ctx, item.key, item.value, item.size)
		}
		c.queue.Remove(elem)
		c.used -= item.size
		return
	}
}
