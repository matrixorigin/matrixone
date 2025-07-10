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

	usedSmall      atomic.Int64
	small          Queue[*_CacheItem[K, V]]
	usedMain       atomic.Int64
	main           Queue[*_CacheItem[K, V]]
	ghost          *ghost[K]
	disable_s3fifo bool
}

type _CacheItem[K comparable, V any] struct {
	key   K
	value V
	size  int64

	// mutex protect the freq and postFn
	mu   sync.Mutex
	freq int8

	// deleted is protected by shardmap
	deleted atomic.Bool
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

// protected by shardmap
func (c *_CacheItem[K, V]) isDeleted() bool {
	return c.deleted.Load()
}

// protected by shardmap
func (c *_CacheItem[K, V]) setDeleted() bool {
	return c.deleted.CompareAndSwap(false, true)
}

func (c *_CacheItem[K, V]) postFn(ctx context.Context, fn func(ctx context.Context, key K, value V, size int64)) {
	if fn != nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		fn(ctx, c.key, c.value, c.size)
	}
}

// increment the reference counter
func (c *_CacheItem[K, V]) retainValue() bool {
	cdata, ok := any(c.value).(fscache.Data)
	if !ok {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isDeleted() {
		return false
	}
	cdata.Retain()
	return true
}

// decrement the reference counter
func (c *_CacheItem[K, V]) releaseValue() {
	cdata, ok := any(c.value).(fscache.Data)
	if !ok {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cdata.Release()
}

// assume cache size is 128K
// if cache capacity is smaller than 4G, ghost size is 100%.  Otherwise, 50%
func estimateGhostSize(capacity int64) int {
	estimate := int(capacity / int64(32806))
	if capacity > 8000000000 { // 8G
		// only 50%
		estimate /= 2
	}
	if estimate < 10000 {
		estimate = 10000
	}
	return estimate
}

func New[K comparable, V any](
	capacity fscache.CapacityFunc,
	keyShardFunc func(K) uint64,
	postSet func(ctx context.Context, key K, value V, size int64),
	postGet func(ctx context.Context, key K, value V, size int64),
	postEvict func(ctx context.Context, key K, value V, size int64),
	disable_s3fifo bool,
) *Cache[K, V] {

	ghostsize := estimateGhostSize(capacity())
	ret := &Cache[K, V]{
		capacity: capacity,
		capSmall: func() int64 {
			cs := capacity() / 10
			if cs == 0 {
				cs = 1
			}
			return cs
		},
		small:          *NewQueue[*_CacheItem[K, V]](),
		main:           *NewQueue[*_CacheItem[K, V]](),
		ghost:          newGhost[K](ghostsize),
		postSet:        postSet,
		postGet:        postGet,
		postEvict:      postEvict,
		htab:           NewShardMap[K, *_CacheItem[K, V]](keyShardFunc),
		disable_s3fifo: disable_s3fifo,
	}
	return ret
}

func (c *Cache[K, V]) Set(ctx context.Context, key K, value V, size int64) {

	item := &_CacheItem[K, V]{
		key:   key,
		value: value,
		size:  size,
	}

	// TODO: FSCACHEDATA RETAIN
	// increment the ref counter first no matter what otherwise there is a risk that value is deleted
	item.retainValue()

	ok := c.htab.Set(key, item, nil)
	if !ok {
		// existed
		// TODO: FSCACHEDATA RELEASE
		// decrement the ref counter if not set to release the resource
		item.releaseValue()
		return
	}

	// postSet
	item.postFn(ctx, c.postSet)

	// evict
	c.evictAll(ctx, nil, 0)

	// enqueue
	if c.disable_s3fifo {
		c.small.enqueue(item)
		c.usedSmall.Add(item.size)
	} else {
		if c.ghost.contains(item.key) {
			c.ghost.remove(item.key)
			c.main.enqueue(item)
			c.usedMain.Add(item.size)
		} else {
			c.small.enqueue(item)
			c.usedSmall.Add(item.size)
		}
	}

}

func (c *Cache[K, V]) Get(ctx context.Context, key K) (value V, ok bool) {
	var item *_CacheItem[K, V]

	item, ok = c.htab.Get(key, func(v *_CacheItem[K, V]) {})
	if !ok {
		return
	}

	// TODO: FSCACHEDATA RETAIN
	ok = item.retainValue()
	if !ok {
		return item.value, false
	}

	// increment
	item.inc()

	// postGet
	item.postFn(ctx, c.postGet)

	return item.value, true
}

func (c *Cache[K, V]) Delete(ctx context.Context, key K) {
	needsDelete := false

	item, ok := c.htab.GetAndDelete(key, func(v *_CacheItem[K, V]) {
		needsDelete = v.setDeleted()

	})

	if ok && needsDelete {
		// call Bytes.Release() to decrement the ref counter and protected by shardmap mutex.
		// item.deleted makes sure postEvict only call once.

		// deleted from hashtable
		item.postFn(ctx, c.postEvict)

		// TODO: FSCACHEDATA RELEASE
		item.releaseValue()
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

		deleted := item.isDeleted()
		if deleted {
			c.usedSmall.Add(-item.size)
			return
		}

		if item.getFreq() > 1 {
			// put main
			c.main.enqueue(item)
			c.usedSmall.Add(-item.size)
			c.usedMain.Add(item.size)
		} else {
			// evict
			needsDelete := false
			c.htab.GetAndDelete(item.key, func(v *_CacheItem[K, V]) {
				// item.deleted makes sure postEvict only call once.
				// post evict
				needsDelete = v.setDeleted()
			})

			if needsDelete {
				// call Bytes.Release() to decrement the ref counter
				// postEvict
				item.postFn(ctx, c.postEvict)
				// TODO: FSCACHEDATA RELEASE
				item.releaseValue()
			}

			c.usedSmall.Add(-item.size)
			if !c.disable_s3fifo {
				c.ghost.add(item.key)
			}
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

		deleted := item.isDeleted()
		if deleted {
			c.usedMain.Add(-item.size)
			return
		}

		if item.getFreq() > 0 {
			// re-enqueue
			item.dec()
			c.main.enqueue(item)
		} else {
			// evict
			needsDelete := false
			c.htab.GetAndDelete(item.key, func(v *_CacheItem[K, V]) {
				// item.deleted makes sure postEvict only call once.
				// post evict
				needsDelete = v.setDeleted()
			})
			if needsDelete {
				// call Bytes.Release() to decrement the ref counter
				// postEvict
				item.postFn(ctx, c.postEvict)
				// TODO: FSCACHEDATA RELEASE
				item.releaseValue()
			}

			c.usedMain.Add(-item.size)
			return
		}
	}
}
