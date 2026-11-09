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
// methods and callbacks of a DiskCache index.
package lrucache

import (
	"container/list"
	"context"
	"hash/maphash"
	"math"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

const (
	numShards  = 16
	evictBatch = 64
)

// Cache evicts the least recently used items once the total size exceeds the capacity.
// Set and Get make an item the most recently used. Keys are spread over shards, each
// ordered by the stamp of its items' last use, and eviction takes the evictable item with
// the oldest stamp across the shards when it removes it. An item that becomes evictable, or
// is set oldest, while an eviction scans the shards can be passed over by that eviction.
type Cache[K comparable, V any] struct {
	capacity fscache.CapacityFunc
	seed     maphash.Seed
	shards   [numShards]shard[K, V]
	used     atomic.Int64
	pending  atomic.Int64 // bytes of the items whose postSet has not returned
	nextSeq  atomic.Uint64
	newest   atomic.Int64  // last stamp given to a use
	added    atomic.Uint64 // count of Sets and of Replaces that grew an item
	oldest   atomic.Int64  // last stamp given by SetOldest
	evictor  sync.Mutex

	postSet      func(ctx context.Context, key K, value V, size int64, seq uint64)
	postGet      func(ctx context.Context, key K, value V, size int64)
	prepareEvict func(key K, value V, size int64, seq uint64) func()
	postEvict    func(ctx context.Context, key K, value V, size int64, seq uint64)
	evictable    func(key K) bool
}

type shard[K comparable, V any] struct {
	mu    sync.Mutex
	order list.List // of *item[K, V], highest stamp first
	items map[K]*list.Element
	tail  atomic.Int64 // stamp of the last item, or math.MaxInt64 when empty
	_     [128]byte
}

// updateTail publishes the stamp of the last item. The caller holds s.mu.
func (s *shard[K, V]) updateTail() {
	if e := s.order.Back(); e != nil {
		s.tail.Store(e.Value.(*item[K, V]).stamp)
	} else {
		s.tail.Store(math.MaxInt64)
	}
}

type item[K comparable, V any] struct {
	key   K
	value V
	size  int64
	seq   uint64
	stamp int64
	// pending is set until postSet of the item returns; eviction passes over it. It is
	// guarded by the item's shard lock.
	pending bool
	// oversized is set for an item larger than the capacity when it was added; Get does
	// not make it the most recently used.
	oversized bool
}

// NewWithPrepareEvict returns a cache with the callbacks of a DiskCache index:
// prepareEvict runs for Delete and eviction after the item is removed, then postEvict, then
// the cleanup prepareEvict returns, all outside the cache's locks. Replace does not invoke
// prepareEvict.
func NewWithPrepareEvict[K comparable, V any](
	capacity fscache.CapacityFunc,
	postSet func(ctx context.Context, key K, value V, size int64, seq uint64),
	postGet func(ctx context.Context, key K, value V, size int64),
	prepareEvict func(key K, value V, size int64, seq uint64) func(),
	postEvict func(ctx context.Context, key K, value V, size int64, seq uint64),
) *Cache[K, V] {
	c := &Cache[K, V]{
		capacity:     capacity,
		seed:         maphash.MakeSeed(),
		postSet:      postSet,
		postGet:      postGet,
		prepareEvict: prepareEvict,
		postEvict:    postEvict,
	}
	for i := range c.shards {
		c.shards[i].items = make(map[K]*list.Element)
		c.shards[i].tail.Store(math.MaxInt64)
	}
	return c
}

// SetEvictable makes eviction pass over the items for which evictable returns false,
// which then stay even when the cache is over capacity. evictable runs under a shard's
// lock and must not call the cache. Call it before the cache is used.
func (c *Cache[K, V]) SetEvictable(evictable func(key K) bool) {
	c.evictable = evictable
}

func (c *Cache[K, V]) shardOf(key K) *shard[K, V] {
	return &c.shards[maphash.Comparable(c.seed, key)%numShards]
}

// Set adds key as the most recently used item, then evicts down to the capacity unless an
// eviction is running, which then does. It does nothing and returns false when key is
// present.
func (c *Cache[K, V]) Set(ctx context.Context, key K, value V, size int64) (inserted bool, rejected bool) {
	return c.set(ctx, key, value, size, false), false
}

// SetOldest adds key as the least recently used item, then evicts as Set does. It does
// nothing and returns false when key is present.
func (c *Cache[K, V]) SetOldest(ctx context.Context, key K, value V, size int64) bool {
	return c.set(ctx, key, value, size, true)
}

func (c *Cache[K, V]) set(ctx context.Context, key K, value V, size int64, oldest bool) bool {
	// An item larger than the capacity is evicted before any other.
	oversized := size > c.capacity()
	oldest = oldest || oversized
	s := c.shardOf(key)
	s.mu.Lock()
	if s.items[key] != nil {
		s.mu.Unlock()
		return false
	}
	it := &item[K, V]{key: key, value: value, size: size, seq: c.nextSeq.Add(1), pending: true, oversized: oversized}
	if oversized {
		it.stamp = c.oldest.Add(-1) + math.MinInt64/2
		s.items[key] = s.order.PushBack(it)
	} else if oldest {
		it.stamp = c.oldest.Add(-1)
		e := s.order.Back()
		for e != nil && e.Value.(*item[K, V]).oversized {
			e = e.Prev()
		}
		if e == nil {
			s.items[key] = s.order.PushFront(it)
		} else {
			s.items[key] = s.order.InsertAfter(it, e)
		}
	} else {
		it.stamp = c.newest.Add(1)
		s.items[key] = s.order.PushFront(it)
	}
	s.updateTail()
	c.pending.Add(size)
	c.used.Add(size)
	c.added.Add(1)
	seq := it.seq
	s.mu.Unlock()
	if c.postSet != nil {
		c.postSet(ctx, key, value, size, seq)
	}
	c.evictor.Lock()
	s.mu.Lock()
	it.pending = false
	s.mu.Unlock()
	c.pending.Add(-size)
	c.evictor.Unlock()
	c.added.Add(1)
	c.evictOver(ctx)
	return true
}

// Get returns key's value and makes it the most recently used item, unless it was larger
// than the capacity when it was added.
func (c *Cache[K, V]) Get(ctx context.Context, key K) (value V, ok bool) {
	s := c.shardOf(key)
	s.mu.Lock()
	e := s.items[key]
	if e == nil {
		s.mu.Unlock()
		return value, false
	}
	it := e.Value.(*item[K, V])
	if !it.oversized {
		it.stamp = c.newest.Add(1)
		wasTail := e == s.order.Back()
		s.order.MoveToFront(e)
		if wasTail {
			s.updateTail()
		}
	}
	value, size := it.value, it.size
	s.mu.Unlock()
	if c.postGet != nil {
		c.postGet(ctx, key, value, size)
	}
	return value, true
}

// Contains reports whether key is present, without counting a use.
func (c *Cache[K, V]) Contains(key K) bool {
	s := c.shardOf(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.items[key] != nil
}

// Delete removes key.
func (c *Cache[K, V]) Delete(ctx context.Context, key K) {
	s := c.shardOf(key)
	s.mu.Lock()
	e := s.items[key]
	if e == nil {
		s.mu.Unlock()
		return
	}
	it := c.removeLocked(s, e)
	s.mu.Unlock()
	c.finishEvictions(ctx, []item[K, V]{it})
}

// Replace sets the value and size of a present key, keeping its place, then evicts as Set
// does. It returns false when key is absent. The old value goes to postEvict.
func (c *Cache[K, V]) Replace(ctx context.Context, key K, value V, size int64) bool {
	s := c.shardOf(key)
	s.mu.Lock()
	e := s.items[key]
	if e == nil {
		s.mu.Unlock()
		return false
	}
	it := e.Value.(*item[K, V])
	old := *it
	it.value, it.size, it.seq = value, size, c.nextSeq.Add(1)
	c.used.Add(size - old.size)
	if size > old.size {
		c.added.Add(1)
	}
	seq := it.seq
	s.mu.Unlock()
	if c.postEvict != nil {
		c.postEvict(ctx, old.key, old.value, old.size, old.seq)
	}
	if c.postSet != nil {
		c.postSet(ctx, key, value, size, seq)
	}
	c.evictOver(ctx)
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
	return c.used.Load()
}

func (c *Cache[K, V]) Capacity() int64 {
	return c.capacity()
}

// settled returns the bytes used by the items eviction may remove: an item whose postSet
// has not returned is neither evicted nor evicted for.
func (c *Cache[K, V]) settled() int64 {
	return c.used.Load() - c.pending.Load()
}

// evictTo removes the least recently used evictable items until at most target bytes
// are used, and returns the bytes used after.
func (c *Cache[K, V]) evictTo(ctx context.Context, target int64) int64 {
	for c.settled() > target {
		c.evictor.Lock()
		evictions := c.removeTo(target)
		c.evictor.Unlock()
		c.finishEvictions(ctx, evictions)
		if len(evictions) < evictBatch {
			break
		}
	}
	used := c.used.Load()
	c.evictOver(ctx)
	return used
}

// evictOver evicts down to the capacity, unless another eviction is running: that one
// checks again once it is done. It stops when nothing is evictable and nothing was added
// meanwhile.
func (c *Cache[K, V]) evictOver(ctx context.Context) {
	for c.settled() > c.capacity() {
		if !c.evictor.TryLock() {
			return
		}
		added := c.added.Load()
		evictions := c.removeTo(c.capacity())
		c.evictor.Unlock()
		c.finishEvictions(ctx, evictions)
		if len(evictions) == 0 && c.added.Load() == added {
			return
		}
	}
}

// removeTo removes up to evictBatch of the least recently used evictable items, stopping
// once at most target bytes are used. The caller holds c.evictor.
func (c *Cache[K, V]) removeTo(target int64) []item[K, V] {
	var evictions []item[K, V]
	for len(evictions) < evictBatch && c.settled() > target {
		it, ok := c.removeOldest(target)
		if !ok {
			break
		}
		evictions = append(evictions, it)
	}
	return evictions
}

// removeOldest removes the evictable item with the oldest stamp, and returns false when no
// item is evictable or at most target bytes are used. A shard whose oldest item changed
// since its stamp was read is read again. The caller holds c.evictor.
func (c *Cache[K, V]) removeOldest(target int64) (item[K, V], bool) {
	for {
		var oldest *shard[K, V]
		stamp := int64(math.MaxInt64)
		for i := range c.shards {
			if t := c.shards[i].tail.Load(); t < stamp {
				oldest, stamp = &c.shards[i], t
			}
		}
		if oldest == nil {
			return item[K, V]{}, false
		}
		oldest.mu.Lock()
		e := oldest.order.Back()
		if e == nil || e.Value.(*item[K, V]).stamp != stamp {
			oldest.mu.Unlock()
			continue
		}
		if c.canEvict(e) {
			if c.settled() <= target {
				oldest.mu.Unlock()
				return item[K, V]{}, false
			}
			it := c.removeLocked(oldest, e)
			oldest.mu.Unlock()
			return it, true
		}
		oldest.mu.Unlock()
		return c.scanOldest(target)
	}
}

// scanOldest is removeOldest looking past the items that are not evictable. It scans again
// when the oldest evictable item of the shard it picked changed meanwhile.
func (c *Cache[K, V]) scanOldest(target int64) (item[K, V], bool) {
	for {
		var oldest *shard[K, V]
		var stamp int64
		for i := range c.shards {
			s := &c.shards[i]
			s.mu.Lock()
			if e := c.oldestLocked(s); e != nil {
				if st := e.Value.(*item[K, V]).stamp; oldest == nil || st < stamp {
					oldest, stamp = s, st
				}
			}
			s.mu.Unlock()
		}
		if oldest == nil {
			return item[K, V]{}, false
		}
		oldest.mu.Lock()
		if e := c.oldestLocked(oldest); e != nil && e.Value.(*item[K, V]).stamp == stamp {
			if c.settled() <= target {
				oldest.mu.Unlock()
				return item[K, V]{}, false
			}
			it := c.removeLocked(oldest, e)
			oldest.mu.Unlock()
			return it, true
		}
		oldest.mu.Unlock()
	}
}

// oldestLocked returns the evictable element of s with the oldest stamp, or nil. The
// caller holds s.mu.
func (c *Cache[K, V]) oldestLocked(s *shard[K, V]) *list.Element {
	for e := s.order.Back(); e != nil; e = e.Prev() {
		if c.canEvict(e) {
			return e
		}
	}
	return nil
}

// canEvict reports whether eviction may remove the item of e. The caller holds its
// shard's lock.
func (c *Cache[K, V]) canEvict(e *list.Element) bool {
	it := e.Value.(*item[K, V])
	return !it.pending && (c.evictable == nil || c.evictable(it.key))
}

func (c *Cache[K, V]) removeLocked(s *shard[K, V], e *list.Element) item[K, V] {
	it := *e.Value.(*item[K, V])
	c.used.Add(-it.size)
	s.order.Remove(e)
	delete(s.items, it.key)
	s.updateTail()
	return it
}

// finishEvictions runs the callbacks of each removed item, for every item even if a callback
// panics.
func (c *Cache[K, V]) finishEvictions(ctx context.Context, evictions []item[K, V]) {
	next := 0
	defer func() {
		if next < len(evictions) {
			c.finishEvictions(ctx, evictions[next:])
		}
	}()
	for next < len(evictions) {
		it := evictions[next]
		next++
		c.finishEviction(ctx, it)
	}
}

func (c *Cache[K, V]) finishEviction(ctx context.Context, it item[K, V]) {
	if c.prepareEvict != nil {
		if finish := c.prepareEvict(it.key, it.value, it.size, it.seq); finish != nil {
			defer finish()
		}
	}
	if c.postEvict != nil {
		c.postEvict(ctx, it.key, it.value, it.size, it.seq)
	}
}
