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

	mutex sync.Mutex
	htab  CacheMap[K, *_CacheItem[K, V]]

	small Queue[*_CacheItem[K, V]]
	main  Queue[*_CacheItem[K, V]]
}

type _CacheItem[K comparable, V any] struct {
	key   K
	value V
	size  int64

	// mutex protect the deleted, freq and postFn
	mu         sync.Mutex   // only used when SingleMutexFlag = false
	freq       int32        // same as atomicFreq but only used when SingleMytexFlag = true
	atomicFreq atomic.Int32 // same as freq but only used when SingleMytexFlag = false
	deleted    bool         // flag indicate item is already deleted by either hashtable or evict
}

// Thread-safe
func (c *_CacheItem[K, V]) Inc() {
	max_freq := int32(3)
	if !SingleMutexFlag {
		// multi mutex
		for {
			freq := c.atomicFreq.Load()
			if freq >= max_freq {
				return
			}

			if c.atomicFreq.CompareAndSwap(freq, freq+1) {
				return
			}
		}

	} else {
		// single mutex
		if c.freq < max_freq {
			c.freq += 1
		}
	}
}

// Thread-safe
func (c *_CacheItem[K, V]) Dec() {
	min_freq := int32(0)
	if !SingleMutexFlag {
		// multi mutex
		for {
			freq := c.atomicFreq.Load()
			if freq <= min_freq {
				return
			}
			if c.atomicFreq.CompareAndSwap(freq, freq-1) {
				return
			}
		}
	} else {
		// single mutex
		if c.freq > min_freq {
			c.freq -= 1
		}
	}
}

// Thread-safe
func (c *_CacheItem[K, V]) GetFreq() int32 {
	if !SingleMutexFlag {
		return c.atomicFreq.Load()
	}
	return c.freq
}

// Thread-safe
func (c *_CacheItem[K, V]) IsDeleted() bool {
	if !SingleMutexFlag {
		c.mu.Lock()
		defer c.mu.Unlock()
	}
	return c.deleted
}

// Thread-safe
// first MarkAsDeleted will decrement the ref counter and call postfn and set deleted = true.
// After first call, MarkAsDeleted will do nothing.
func (c *_CacheItem[K, V]) MarkAsDeleted(ctx context.Context, fn func(ctx context.Context, key K, value V, size int64)) bool {
	if !SingleMutexFlag {
		c.mu.Lock()
		defer c.mu.Unlock()
	}

	// check item is already deleted
	if c.deleted {
		// exit and return false which means no need to deallocate the memory
		return false
	}

	// set deleted = true
	c.deleted = true

	// call postEvict before decrement the ref counter
	if fn != nil {
		fn(ctx, c.key, c.value, c.size)
	}

	// decrement the ref counter
	c.releaseValue()
	return true
}

// Thread-safe
func (c *_CacheItem[K, V]) PostFn(ctx context.Context, fn func(ctx context.Context, key K, value V, size int64)) {
	if fn != nil {
		if !SingleMutexFlag {
			c.mu.Lock()
			defer c.mu.Unlock()
		}
		fn(ctx, c.key, c.value, c.size)
	}
}

// Thread-safe
func (c *_CacheItem[K, V]) Retain(ctx context.Context, fn func(ctx context.Context, key K, value V, size int64)) bool {
	if !SingleMutexFlag {
		c.mu.Lock()
		defer c.mu.Unlock()
	}

	// first check item is already deleted
	if c.deleted {
		return false
	}

	// if not deleted, increment ref counter to occupy the memory
	c.retainValue()

	// value is safe to be accessed now and call postfn
	if fn != nil {
		fn(ctx, c.key, c.value, c.size)
	}

	return true
}

func (c *_CacheItem[K, V]) Size() int64 {
	if !SingleMutexFlag {
		c.mu.Lock()
		defer c.mu.Unlock()
	}
	return c.size
}

// INTERNAL: non-thread safe.
// if deleted = true, item value is already released by this Cache and is NOT valid to use it inside the Cache.
// if deleted = false, increment the reference counter of the value and it is safe to use now.
func (c *_CacheItem[K, V]) retainValue() {
	cdata, ok := any(c.value).(fscache.SelfMaintainMemObj)
	if ok {
		cdata.Retain()
	}
}

// INTERNAL: non-thread safe.
// decrement the reference counter
func (c *_CacheItem[K, V]) releaseValue() {
	cdata, ok := any(c.value).(fscache.SelfMaintainMemObj)
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
		htab:      NewCacheMap[K, *_CacheItem[K, V]](keyShardFunc),
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
	// we don't need mutex here since item is still local
	item.Retain(ctx, nil)

	ok := c.htab.Set(key, item, nil)
	if !ok {
		// existed
		// FSCACHEDATA RELEASE
		// decrement the ref counter if not set to release the resource
		// still okay without mutex since item is not in hashtable
		item.MarkAsDeleted(ctx, nil)
		return
	}

	if SingleMutexFlag {
		c.mutex.Lock()
		defer c.mutex.Unlock()
	}

	// postSet
	item.PostFn(ctx, c.postSet)

	// evict
	c.evictAll(ctx, nil, 0)

	// enqueue
	c.small.enqueue(item, item.Size())

}

func (c *Cache[K, V]) Get(ctx context.Context, key K) (value V, ok bool) {
	var item *_CacheItem[K, V]

	item, ok = c.htab.Get(key, nil)
	if !ok {
		return
	}

	if SingleMutexFlag {
		c.mutex.Lock()
		defer c.mutex.Unlock()
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
	if !ok {
		// key not found
		return
	}

	if SingleMutexFlag {
		c.mutex.Lock()
		defer c.mutex.Unlock()
	}
	// call Bytes.Release() to decrement the ref counter and protected by shardmap mutex.
	// item.deleted makes sure postEvict only call once.
	item.MarkAsDeleted(ctx, c.postEvict)

}

func (c *Cache[K, V]) Evict(ctx context.Context, done chan int64, capacityCut int64) {
	if SingleMutexFlag {
		c.mutex.Lock()
		defer c.mutex.Unlock()
	}
	target := c.evictAll(ctx, done, capacityCut)
	if done != nil {
		done <- target
	}
}

// ForceEvict evicts n bytes despite capacity
func (c *Cache[K, V]) ForceEvict(ctx context.Context, n int64) {
	if SingleMutexFlag {
		c.mutex.Lock()
		defer c.mutex.Unlock()
	}
	capacityCut := c.capacity() - c.small.Used() + c.main.Used() + n
	c.evictAll(ctx, nil, capacityCut)
}

func (c *Cache[K, V]) Used() int64 {
	if SingleMutexFlag {
		c.mutex.Lock()
		defer c.mutex.Unlock()
	}
	return c.small.Used() + c.main.Used()
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

	usedsmall := c.small.Used()
	usedmain := c.main.Used()

	for usedmain+usedsmall > target {
		if usedsmall > targetSmall {
			c.evictSmall(ctx)
		} else {
			c.evictMain(ctx)
		}
		usedsmall = c.small.Used()
		usedmain = c.main.Used()
	}

	return target + 1
}

func (c *Cache[K, V]) evictSmall(ctx context.Context) {
	// small fifo
	for c.small.Used() > 0 {
		item, ok := c.small.dequeue()
		if !ok {
			// queue empty
			return
		}

		deleted := item.IsDeleted()
		if deleted {
			return
		}

		if item.GetFreq() > 1 {
			// put main
			c.main.enqueue(item, item.Size())
		} else {
			// evict
			c.htab.Remove(item.key)
			// mark item as deleted and item should not be accessed again
			item.MarkAsDeleted(ctx, c.postEvict)
			return
		}
	}
}

func (c *Cache[K, V]) evictMain(ctx context.Context) {
	// main fifo
	for c.main.Used() > 0 {
		item, ok := c.main.dequeue()
		if !ok {
			// empty queue
			return
		}

		deleted := item.IsDeleted()
		if deleted {
			return
		}

		if item.GetFreq() > 0 {
			// re-enqueue
			item.Dec()
			c.main.enqueue(item, item.size)
		} else {
			// evict
			c.htab.Remove(item.key)
			// mark item as deleted and item should not be accessed again
			item.MarkAsDeleted(ctx, c.postEvict)
			return
		}
	}
}
