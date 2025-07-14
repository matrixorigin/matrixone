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

// Cache implements an in-memory cache with S3-FIFO-based eviction
// All postfn is very critical.  They will increment and decrement the reference counter of the cache data and deallocate the memory when reference counter is 0.
// Make sure the postfn is protected by mutex from shardmap.
type Cache[K comparable, V any] struct {
	capacity fscache.CapacityFunc
	capSmall fscache.CapacityFunc

	postSet   func(ctx context.Context, key K, value V, size int64)
	postGet   func(ctx context.Context, key K, value V, size int64)
	postEvict func(ctx context.Context, key K, value V, size int64)

	htab *ShardMap[K, *_CacheItem[K, V]]

	usedSmall atomic.Int64
	small     Queue[*_CacheItem[K, V]]
	usedMain  atomic.Int64
	main      Queue[*_CacheItem[K, V]]
}

type _CacheItem[K comparable, V any] struct {
	key   K
	value V
	size  int64

	// mutex protect the deleted, freq and postFn
	mu      sync.Mutex
	freq    int8
	deleted bool // flag indicate item is already deleted by either hashtable or evict
}

// Thread-safe
func (c *_CacheItem[K, V]) Inc() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.freq < 3 {
		c.freq += 1
	}
}

// Thread-safe
func (c *_CacheItem[K, V]) Dec() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.freq > 0 {
		c.freq -= 1
	}
}

// Thread-safe
func (c *_CacheItem[K, V]) GetFreq() int8 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.freq
}

// Thread-safe
func (c *_CacheItem[K, V]) IsDeleted() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deleted
}

// Thread-safe
// first MarkAsDeleted will decrement the ref counter and call postfn and set deleted = true.
// After first call, MarkAsDeleted will do nothing.
func (c *_CacheItem[K, V]) MarkAsDeleted(ctx context.Context, fn func(ctx context.Context, key K, value V, size int64)) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.deleted {
		return false
	}

	c.deleted = true

	if fn != nil {
		fn(ctx, c.key, c.value, c.size)
	}

	c.releaseValue()
	return true
}

// Thread-safe
func (c *_CacheItem[K, V]) PostFn(ctx context.Context, fn func(ctx context.Context, key K, value V, size int64)) {
	if fn != nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		fn(ctx, c.key, c.value, c.size)
	}
}

// Thread-safe
func (c *_CacheItem[K, V]) Retain(ctx context.Context, fn func(ctx context.Context, key K, value V, size int64)) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.deleted {
		return false
	}

	ok := c.retainValue()
	if !ok {
		return false
	}

	if fn != nil {
		fn(ctx, c.key, c.value, c.size)
	}

	return true
}

// INTERNAL: non-thread safe.
// if deleted = true, item value is already released by this Cache and is NOT valid to use it inside the Cache.
// if deleted = false, increment the reference counter of the value and it is safe to use now.
func (c *_CacheItem[K, V]) retainValue() bool {
	cdata, ok := any(c.value).(fscache.Data)
	if ok {
		cdata.Retain()
	}
	return true
}

// INTERNAL: non-thread safe.
// decrement the reference counter
func (c *_CacheItem[K, V]) releaseValue() {
	cdata, ok := any(c.value).(fscache.Data)
	if ok {
		cdata.Release()
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
		capSmall: func() int64 {
			cs := capacity() / 10
			if cs == 0 {
				cs = 1
			}
			return cs
		},
		small:     *NewQueue[*_CacheItem[K, V]](),
		main:      *NewQueue[*_CacheItem[K, V]](),
		postSet:   postSet,
		postGet:   postGet,
		postEvict: postEvict,
		htab:      NewShardMap[K, *_CacheItem[K, V]](keyShardFunc),
	}
	return ret
}

func (c *Cache[K, V]) Set(ctx context.Context, key K, value V, size int64) {

	item := &_CacheItem[K, V]{
		key:   key,
		value: value,
		size:  size,
	}

	// FSCACHEDATA RETAIN
	// increment the ref counter first no matter what to make sure the memory is occupied before hashtable.Set
	item.Retain(ctx, nil)

	ok := c.htab.Set(key, item, nil)
	if !ok {
		// existed
		// FSCACHEDATA RELEASE
		// decrement the ref counter if not set to release the resource
		item.MarkAsDeleted(ctx, nil)
		return
	}

	// postSet
	item.PostFn(ctx, c.postSet)

	// evict
	c.evictAll(ctx, nil, 0)

	// enqueue
	c.small.enqueue(item)
	c.usedSmall.Add(item.size)

}

func (c *Cache[K, V]) Get(ctx context.Context, key K) (value V, ok bool) {
	var item *_CacheItem[K, V]

	item, ok = c.htab.Get(key, nil)
	if !ok {
		return
	}

	// FSCACHEDATA RETAIN
	ok = item.Retain(ctx, c.postGet)
	if !ok {
		return item.value, false
	}

	// increment
	item.Inc()

	return item.value, true
}

func (c *Cache[K, V]) Delete(ctx context.Context, key K) {
	item, ok := c.htab.GetAndDelete(key, nil)

	if ok {
		// call Bytes.Release() to decrement the ref counter and protected by shardmap mutex.
		// item.deleted makes sure postEvict only call once.
		item.MarkAsDeleted(ctx, c.postEvict)
	}

}

func (c *Cache[K, V]) Evict(ctx context.Context, done chan int64, capacityCut int64) {
	target := c.evictAll(ctx, done, capacityCut)
	if done != nil {
		done <- target
	}
}

// ForceEvict evicts n bytes despite capacity
func (c *Cache[K, V]) ForceEvict(ctx context.Context, n int64) {
	capacityCut := c.capacity() - c.Used() + n
	c.Evict(ctx, nil, capacityCut)
}

func (c *Cache[K, V]) Used() int64 {
	return c.usedSmall.Load() + c.usedMain.Load()
}

func (c *Cache[K, V]) evictAll(ctx context.Context, done chan int64, capacityCut int64) int64 {
	var target int64
	target = c.capacity() - capacityCut - 1
	if target <= 0 {
		target = 0
	}
	targetSmall := c.capSmall() - capacityCut - 1
	if targetSmall <= 0 {
		targetSmall = 0
	}

	usedsmall := c.usedSmall.Load()
	usedmain := c.usedMain.Load()

	for usedmain+usedsmall > target {
		if usedsmall > targetSmall {
			c.evictSmall(ctx)
		} else {
			c.evictMain(ctx)
		}
		usedsmall = c.usedSmall.Load()
		usedmain = c.usedMain.Load()
	}

	return target + 1
}

func (c *Cache[K, V]) evictSmall(ctx context.Context) {
	// small fifo
	for c.usedSmall.Load() > 0 {
		item, ok := c.small.dequeue()
		if !ok {
			// queue empty
			return
		}

		deleted := item.IsDeleted()
		if deleted {
			c.usedSmall.Add(-item.size)
			return
		}

		if item.GetFreq() > 1 {
			// put main
			c.main.enqueue(item)
			c.usedSmall.Add(-item.size)
			c.usedMain.Add(item.size)
		} else {
			// evict
			c.htab.Remove(item.key)
			item.MarkAsDeleted(ctx, c.postEvict)

			c.usedSmall.Add(-item.size)
			return
		}
	}
}

func (c *Cache[K, V]) evictMain(ctx context.Context) {
	// main fifo
	for c.usedMain.Load() > 0 {
		item, ok := c.main.dequeue()
		if !ok {
			// empty queue
			return
		}

		deleted := item.IsDeleted()
		if deleted {
			c.usedMain.Add(-item.size)
			return
		}

		if item.GetFreq() > 0 {
			// re-enqueue
			item.Dec()
			c.main.enqueue(item)
		} else {
			// evict
			c.htab.Remove(item.key)
			item.MarkAsDeleted(ctx, c.postEvict)

			c.usedMain.Add(-item.size)
			return
		}
	}
}
