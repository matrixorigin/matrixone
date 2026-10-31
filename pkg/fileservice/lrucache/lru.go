// Copyright 2026 Matrix Origin
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

// Package lrucache is a size-bounded cache with least-recently-used eviction, with the
// methods and callbacks of fifocache.Cache.
package lrucache

import (
	"container/list"
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

// Cache evicts the least recently used items once the total size exceeds the capacity.
// Set and Get make an item the most recently used.
type Cache[K comparable, V any] struct {
	mu       sync.Mutex
	capacity fscache.CapacityFunc
	used     int64
	nextSeq  uint64
	order    *list.List // of *item[K, V], most recently used first
	items    map[K]*list.Element

	postSet      func(ctx context.Context, key K, value V, size int64, seq uint64)
	postGet      func(ctx context.Context, key K, value V, size int64)
	prepareEvict func(key K, value V, size int64, seq uint64) func()
	postEvict    func(ctx context.Context, key K, value V, size int64, seq uint64)
	evictable    func(key K) bool
}

type item[K comparable, V any] struct {
	key   K
	value V
	size  int64
	seq   uint64
}

type evicted[K comparable, V any] struct {
	item   item[K, V]
	finish func()
}

// NewWithPrepareEvict returns a cache with the callbacks of fifocache.NewWithPrepareEvict:
// prepareEvict runs for Delete and eviction while the item is removed, and the cleanup it
// returns runs once after postEvict, outside the cache's lock. Replace does not invoke
// prepareEvict. keyShardFunc is accepted for the same signature and not used.
func NewWithPrepareEvict[K comparable, V any](
	capacity fscache.CapacityFunc,
	keyShardFunc func(K) uint64,
	postSet func(ctx context.Context, key K, value V, size int64, seq uint64),
	postGet func(ctx context.Context, key K, value V, size int64),
	prepareEvict func(key K, value V, size int64, seq uint64) func(),
	postEvict func(ctx context.Context, key K, value V, size int64, seq uint64),
) *Cache[K, V] {
	return &Cache[K, V]{
		capacity:     capacity,
		order:        list.New(),
		items:        make(map[K]*list.Element),
		postSet:      postSet,
		postGet:      postGet,
		prepareEvict: prepareEvict,
		postEvict:    postEvict,
	}
}

// SetEvictable makes eviction pass over the items for which evictable returns false,
// which then stay even when the cache is over capacity. evictable runs under the cache's
// lock and must not call the cache. Call it before the cache is used.
func (c *Cache[K, V]) SetEvictable(evictable func(key K) bool) {
	c.evictable = evictable
}

// Set adds key as the most recently used item, then evicts down to the capacity. It
// does nothing and returns false when key is present.
func (c *Cache[K, V]) Set(ctx context.Context, key K, value V, size int64) (inserted bool, rejected bool) {
	c.mu.Lock()
	if c.items[key] != nil {
		c.mu.Unlock()
		return false, false
	}
	c.nextSeq++
	it := item[K, V]{key: key, value: value, size: size, seq: c.nextSeq}
	c.items[key] = c.order.PushFront(&it)
	c.used += size
	c.mu.Unlock()
	if c.postSet != nil {
		c.postSet(ctx, key, value, size, it.seq)
	}
	c.Evict(ctx, nil, 0)
	return true, false
}

// SetOldest adds key as the least recently used item, then evicts down to the capacity.
// It does nothing and returns false when key is present.
func (c *Cache[K, V]) SetOldest(ctx context.Context, key K, value V, size int64) bool {
	c.mu.Lock()
	if c.items[key] != nil {
		c.mu.Unlock()
		return false
	}
	c.nextSeq++
	it := item[K, V]{key: key, value: value, size: size, seq: c.nextSeq}
	c.items[key] = c.order.PushBack(&it)
	c.used += size
	c.mu.Unlock()
	if c.postSet != nil {
		c.postSet(ctx, key, value, size, it.seq)
	}
	c.Evict(ctx, nil, 0)
	return true
}

// Get returns key's value and makes it the most recently used item.
func (c *Cache[K, V]) Get(ctx context.Context, key K) (value V, ok bool) {
	c.mu.Lock()
	e := c.items[key]
	if e == nil {
		c.mu.Unlock()
		return value, false
	}
	c.order.MoveToFront(e)
	it := *e.Value.(*item[K, V])
	c.mu.Unlock()
	if c.postGet != nil {
		c.postGet(ctx, key, it.value, it.size)
	}
	return it.value, true
}

// Contains reports whether key is present, without counting a use.
func (c *Cache[K, V]) Contains(key K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.items[key] != nil
}

// Delete removes key.
func (c *Cache[K, V]) Delete(ctx context.Context, key K) {
	c.mu.Lock()
	e := c.items[key]
	if e == nil {
		c.mu.Unlock()
		return
	}
	ev := c.removeLocked(e)
	c.mu.Unlock()
	c.finishEvictions(ctx, []evicted[K, V]{ev})
}

// Replace sets the value and size of a present key, keeping its place, and returns
// false when key is absent. The old value goes to postEvict.
func (c *Cache[K, V]) Replace(ctx context.Context, key K, value V, size int64) bool {
	c.mu.Lock()
	e := c.items[key]
	if e == nil {
		c.mu.Unlock()
		return false
	}
	it := e.Value.(*item[K, V])
	old := *it
	c.nextSeq++
	it.value, it.size, it.seq = value, size, c.nextSeq
	c.used += size - old.size
	seq := it.seq
	c.mu.Unlock()
	if c.postEvict != nil {
		c.postEvict(ctx, old.key, old.value, old.size, old.seq)
	}
	if c.postSet != nil {
		c.postSet(ctx, key, value, size, seq)
	}
	c.Evict(ctx, nil, 0)
	return true
}

// Evict evicts down to the capacity minus capacityCut and, when done is not nil, sends
// that target to it.
func (c *Cache[K, V]) Evict(ctx context.Context, done chan int64, capacityCut int64) {
	target := max(c.capacity()-capacityCut, 0)
	c.evictTo(ctx, target)
	if done != nil {
		done <- target
	}
}

// ForceEvict evicts n bytes despite the capacity.
func (c *Cache[K, V]) ForceEvict(ctx context.Context, n int64) {
	c.ForceEvictWithWait(ctx, n)
}

// ForceEvictWithWait evicts n bytes despite the capacity and returns the bytes used
// after.
func (c *Cache[K, V]) ForceEvictWithWait(ctx context.Context, n int64) int64 {
	if n <= 0 {
		return c.Used()
	}
	return c.evictTo(ctx, max(c.Used()-n, 0))
}

// EvictToTargetWithWait evicts down to target bytes, at most the capacity, and returns
// the bytes used after.
func (c *Cache[K, V]) EvictToTargetWithWait(ctx context.Context, target int64) int64 {
	return c.evictTo(ctx, min(max(target, 0), c.capacity()))
}

func (c *Cache[K, V]) Used() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.used
}

func (c *Cache[K, V]) Capacity() int64 {
	return c.capacity()
}

// evictTo removes the least recently used evictable items until at most target bytes
// are used, and returns the bytes used after.
func (c *Cache[K, V]) evictTo(ctx context.Context, target int64) int64 {
	var evictions []evicted[K, V]
	c.mu.Lock()
	for e := c.order.Back(); e != nil && c.used > target; {
		prev := e.Prev()
		if c.evictable == nil || c.evictable(e.Value.(*item[K, V]).key) {
			evictions = append(evictions, c.removeLocked(e))
		}
		e = prev
	}
	used := c.used
	c.mu.Unlock()
	c.finishEvictions(ctx, evictions)
	return used
}

func (c *Cache[K, V]) removeLocked(e *list.Element) evicted[K, V] {
	it := *e.Value.(*item[K, V])
	c.order.Remove(e)
	delete(c.items, it.key)
	c.used -= it.size
	ev := evicted[K, V]{item: it}
	if c.prepareEvict != nil {
		ev.finish = c.prepareEvict(it.key, it.value, it.size, it.seq)
	}
	return ev
}

// finishEvictions runs postEvict and the prepareEvict cleanup of each eviction, every
// cleanup even if a callback panics.
func (c *Cache[K, V]) finishEvictions(ctx context.Context, evictions []evicted[K, V]) {
	next := 0
	defer func() {
		for ; next < len(evictions); next++ {
			if f := evictions[next].finish; f != nil {
				f()
			}
		}
	}()
	for next < len(evictions) {
		ev := evictions[next]
		next++
		func() {
			if ev.finish != nil {
				defer ev.finish()
			}
			if c.postEvict != nil {
				c.postEvict(ctx, ev.item.key, ev.item.value, ev.item.size, ev.item.seq)
			}
		}()
	}
}
