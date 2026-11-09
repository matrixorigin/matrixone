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

// Package s3fifo is an S3-FIFO cache that owns the references to the values it holds.
//
// Keys are found through IndexShards index shards. Their values are kept in the small and main
// FIFO queues of S3-FIFO (SOSP '23), each split over PolicyShards policy shards, with a ghost
// queue of recently evicted keys split the same way. Every push into a queue is ordered over
// all shards, and eviction takes from the shard holding the oldest member, so the cache evicts
// as one S3-FIFO. One goroutine evicts at a time.
//
// A Set or Replace inserts its value, makes it evictable once its insertion is notified, then
// evicts while Used is over the capacity. When another goroutine holds the evictor and Used is
// within the ceiling, a sixteenth over the capacity, it returns and leaves the eviction to that
// goroutine; over the ceiling it waits for the evictor and evicts too. A Set or Replace that
// holds the evictor checks Used against the capacity again each time it lets go of it. EvictTo
// evicts only down to its own target and stops when ctx is done, so bytes left to it can stay
// over the capacity until the next Set, Replace or eviction. Used also exceeds the capacity
// while Sets are running, by up to a sixteenth of it plus the values being inserted, and after
// the capacity shrinks.
//
// The cache retains a value when it inserts it and releases it once the value has left the
// cache, outside all of its locks; a Get retains the value for the caller. A Set that returns
// true published its value, findable and counted in Used, and made it evictable; a concurrent
// Delete or eviction may have removed it by then. A Set that returns false cached nothing.
//
// Locks are taken evictor, then policy shard, then index shard, then ChargeGuard. A Get takes
// only its index shard.
package s3fifo

import (
	"context"
	"math"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

// Value is a value the cache holds a reference to while it is cached.
type Value interface {
	Retain()
	Release()
}

// Reason tells why a value left the cache.
type Reason uint8

const (
	// Evicted is a value evicted to stay within capacity.
	Evicted Reason = iota
	// Deleted is a value removed by Delete or DeleteGroup.
	Deleted
	// Replaced is a value replaced by Replace; its key stays cached.
	Replaced
)

const (
	// IndexShards is the number of index shards.
	IndexShards = 256
	// PolicyShards is the number of policy shards.
	PolicyShards = 16
	// evictBatch bounds the values an eviction holds before it releases them.
	evictBatch = 64
	maxFreq    = 3
)

// Options configures a Cache. Only Capacity and Hash are required.
type Options[K comparable, V Value] struct {
	Capacity fscache.CapacityFunc
	// Hash spreads keys over shards and identifies them in the ghost queue.
	Hash func(K) uint64
	// Group, when set, groups keys so that DeleteGroup removes a group without a scan.
	Group func(K) string
	// Admission, when it applies, is the most bytes the cache may hold after admitting a new
	// value: a value that would exceed it is rejected unless its key is in the ghost queue,
	// in which case older values are evicted to make room.
	Admission func(capacity int64) (target int64, ok bool)
	// ChargeGuard, when set, is held while a new value's bytes are added to Used, together
	// with OnCharge, so that a caller reserving capacity outside the cache observes the
	// reservation and the charge atomically; Guarded, when set, reports the values that need
	// it. Guarded and OnCharge must not call into the cache.
	ChargeGuard sync.Locker
	Guarded     func(value V) bool
	OnCharge    func(value V)
	// OnInsert and OnRemove are optional notifications. They run after the change, outside
	// the cache's locks, and while the value is still referenced; they may call into the
	// cache. A value becomes evictable once OnInsert returns.
	OnInsert func(key K, value V, size int64, seq uint64)
	OnRemove func(key K, value V, size int64, seq uint64, reason Reason)
}

// Cache is an S3-FIFO cache. Its methods are safe for concurrent use.
type Cache[K comparable, V Value] struct {
	opts       Options[K, V]
	index      [IndexShards]indexShard[K, V]
	policy     [PolicyShards]policyShard[K, V]
	used       atomic.Int64
	smallBytes atomic.Int64
	mainBytes  atomic.Int64
	ghostBytes atomic.Int64
	seq        atomic.Uint64
	order      atomic.Uint64
	// evictor is held by the goroutine evicting.
	evictor sync.Mutex
}

type queueID uint8

const (
	inSmall queueID = iota + 1
	inMain
)

type entry[K comparable, V Value] struct {
	key   K
	hash  uint64
	value V
	size  int64
	seq   uint64
	freq  atomic.Int32
	// queue is the queue holding the entry: 0 while it moves between queues and once it has
	// left the cache; order is its place in that queue over all shards. Guarded by the policy
	// shard lock.
	queue queueID
	order uint64
	// removed is set once the entry has left the cache. Guarded by the policy shard lock.
	removed bool
	// prevInGroup and nextInGroup link the entries of a group in its index shard. Guarded by
	// the index shard lock.
	prevInGroup, nextInGroup *entry[K, V]
}

type indexShard[K comparable, V Value] struct {
	mu    sync.Mutex
	index map[K]*entry[K, V]
	// groups holds the first entry of each group.
	groups map[string]*entry[K, V]
}

type policyShard[K comparable, V Value] struct {
	mu    sync.Mutex
	small fifo[K, V]
	main  fifo[K, V]
	ghost ghost
}

// removal is a value that has left the cache, to be notified and released outside the locks.
type removal[K comparable, V Value] struct {
	key    K
	value  V
	size   int64
	seq    uint64
	reason Reason
}

// New returns a cache configured by opts.
func New[K comparable, V Value](opts Options[K, V]) *Cache[K, V] {
	if opts.Capacity == nil || opts.Hash == nil {
		panic("s3fifo: Capacity and Hash are required")
	}
	c := &Cache[K, V]{opts: opts}
	for i := range c.index {
		c.index[i].index = make(map[K]*entry[K, V])
		if opts.Group != nil {
			c.index[i].groups = make(map[string]*entry[K, V])
		}
	}
	for i := range c.policy {
		c.policy[i].small.init(inSmall, &c.smallBytes)
		c.policy[i].main.init(inMain, &c.mainBytes)
		c.policy[i].ghost.init(&c.ghostBytes)
	}
	return c
}

func (c *Cache[K, V]) indexOf(hash uint64) *indexShard[K, V] {
	return &c.index[hash%IndexShards]
}

func (c *Cache[K, V]) policyOf(hash uint64) *policyShard[K, V] {
	return &c.policy[(hash/IndexShards)%PolicyShards]
}

func (c *Cache[K, V]) capacity() int64 {
	return max(c.opts.Capacity(), 0)
}

// Capacity returns the capacity.
func (c *Cache[K, V]) Capacity() int64 {
	return c.opts.Capacity()
}

// Used returns the bytes of the cached values.
func (c *Cache[K, V]) Used() int64 {
	return c.used.Load()
}

// Get returns the value of key retained for the caller, who releases it. admit, when not nil,
// runs before the value is retained, with the value's size; an error from it is a miss for
// which release is not returned. Admission, Retain, and any admission release on failure run
// under the index-shard lock and must not call into the cache.
func (c *Cache[K, V]) Get(key K, admit func(size int64) (release func(), err error)) (value V, release func(), ok bool, err error) {
	return c.get(key, admit, false)
}

// TryGet has Get's value and admission ownership contract, but returns a miss
// if the index shard is locked. Admission and Retain run under that lock and
// must not call into the cache. Hash, admission and value callbacks must also
// be bounded when the caller requires a bounded lookup.
func (c *Cache[K, V]) TryGet(key K, admit func(size int64) (release func(), err error)) (value V, release func(), ok bool, err error) {
	return c.get(key, admit, true)
}

func (c *Cache[K, V]) get(key K, admit func(size int64) (release func(), err error), try bool) (value V, release func(), ok bool, err error) {
	ix := c.indexOf(c.opts.Hash(key))
	if try {
		if !ix.mu.TryLock() {
			return value, nil, false, nil
		}
	} else {
		ix.mu.Lock()
	}
	defer ix.mu.Unlock()
	e := ix.index[key]
	if e == nil {
		return value, nil, false, nil
	}
	if admit != nil {
		if release, err = admit(e.size); err != nil {
			if release != nil {
				release()
			}
			return value, nil, false, err
		}
	}
	retained := false
	defer func() {
		if !retained && release != nil {
			release()
		}
	}()
	e.value.Retain()
	retained = true
	for f := e.freq.Load(); f < maxFreq && !e.freq.CompareAndSwap(f, f+1); f = e.freq.Load() {
	}
	return e.value, release, true, nil
}

// Contains reports whether key is cached.
func (c *Cache[K, V]) Contains(key K) bool {
	_, ok := c.CurrentSeq(key)
	return ok
}

// CurrentSeq returns the sequence number of key's cached value, assigned when it was set.
func (c *Cache[K, V]) CurrentSeq(key K) (uint64, bool) {
	ix := c.indexOf(c.opts.Hash(key))
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if e := ix.index[key]; e != nil {
		return e.seq, true
	}
	return 0, false
}

// Set caches value under key with size, retaining it; the caller keeps its own reference.
// It returns whether the value was cached, and whether Admission rejected it. A key already
// cached keeps its value.
func (c *Cache[K, V]) Set(key K, value V, size int64) (inserted bool, rejected bool) {
	capacity := c.capacity()
	if size > capacity {
		return false, false
	}
	hash := c.opts.Hash(key)
	ps, ix := c.policyOf(hash), c.indexOf(hash)
	if ix.contains(key) {
		return false, false
	}
	limit, ghostHit := int64(math.MaxInt64), false
	if c.opts.Admission != nil {
		if target, ok := c.opts.Admission(capacity); ok {
			limit = target
			if c.used.Load()+size > target {
				var cached bool
				cached, ghostHit = c.lookup(ps, ix, key, hash)
				if cached {
					return false, false
				}
				if !ghostHit || size > target {
					return false, true
				}
			}
		}
	}

	// The cache takes its reference before the value is visible and outside the locks, and
	// gives it back if the value is not cached.
	value.Retain()
	published := false
	defer func() {
		if !published {
			value.Release()
		}
	}()
	e := &entry[K, V]{key: key, hash: hash, value: value, size: size}
	var seq uint64
	var result insertResult
	if ghostHit {
		seq, result = c.evictAndPublish(ps, ix, e, limit)
	} else {
		seq, result = c.publish(ix, e, limit)
	}
	if result != insertOK {
		return false, result == insertRejected
	}
	published = true
	c.queue(ps, e, value, size, seq, ghostHit)
	c.evictOver()
	return true, false
}

func (c *Cache[K, V]) lookup(ps *policyShard[K, V], ix *indexShard[K, V], key K, hash uint64) (cached, ghostHit bool) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ix.mu.Lock()
	defer ix.mu.Unlock()
	_, cached = ix.index[key]
	return cached, ps.ghost.contains(hash)
}

type insertResult uint8

const (
	insertOK insertResult = iota
	insertCached
	insertRejected
)

// evictAndPublish evicts until e fits within limit and publishes it, each batch under one hold
// of the evictor, so that the room a batch makes is e's; the evicted values are released after.
func (c *Cache[K, V]) evictAndPublish(ps *policyShard[K, V], ix *indexShard[K, V], e *entry[K, V], limit int64) (uint64, insertResult) {
	var buf [evictBatch]removal[K, V]
	for {
		c.evictor.Lock()
		seq, result := c.publish(ix, e, limit)
		removed, done := buf[:0], false
		if result == insertRejected {
			removed, _, done = c.evictBatch(limit-e.size, removed)
			seq, result = c.publish(ix, e, limit)
		}
		c.evictor.Unlock()
		c.finishAll(removed)
		clear(removed)
		if result != insertRejected || done && len(removed) == 0 {
			return seq, result
		}
	}
}

// publish charges e within limit and indexes it, unless its key is already cached, and returns
// its sequence number. e is not queued yet.
func (c *Cache[K, V]) publish(ix *indexShard[K, V], e *entry[K, V], limit int64) (uint64, insertResult) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if _, cached := ix.index[e.key]; cached {
		return 0, insertCached
	}
	if !c.charge(e.value, e.size, limit) {
		return 0, insertRejected
	}
	e.seq = c.seq.Add(1)
	ix.index[e.key] = e
	ix.addToGroup(c.opts.Group, e)
	return e.seq, insertOK
}

// queue notifies the insertion of e, then makes it evictable, in the main queue when toMain is
// set or the ghost queue remembers it, unless it has left the cache or been queued meanwhile.
func (c *Cache[K, V]) queue(ps *policyShard[K, V], e *entry[K, V], value V, size int64, seq uint64, toMain bool) {
	defer func() {
		ps.mu.Lock()
		defer ps.mu.Unlock()
		if e.removed || e.queue != 0 {
			return
		}
		if ps.ghost.take(e.hash) || toMain {
			ps.main.push(e, c.order.Add(1))
		} else {
			ps.small.push(e, c.order.Add(1))
		}
	}()
	if c.opts.OnInsert != nil {
		c.opts.OnInsert(e.key, value, size, seq)
	}
}

// charge adds size to Used unless that takes it over limit, together with OnCharge under
// ChargeGuard for a value that needs it.
func (c *Cache[K, V]) charge(value V, size, limit int64) bool {
	guarded := c.opts.ChargeGuard != nil && (c.opts.Guarded == nil || c.opts.Guarded(value))
	if guarded {
		c.opts.ChargeGuard.Lock()
		defer c.opts.ChargeGuard.Unlock()
	}
	for {
		used := c.used.Load()
		if used+size > limit {
			return false
		}
		if c.used.CompareAndSwap(used, used+size) {
			break
		}
	}
	if guarded && c.opts.OnCharge != nil {
		committed := false
		defer func() {
			if !committed {
				c.used.Add(-size)
			}
		}()
		c.opts.OnCharge(value)
		committed = true
	}
	return true
}

// Replace replaces the value of key, when it is cached, with value of size, retaining it, and
// reports whether it did; the key keeps its queue and its reads, and is notified as Set's is.
// A value larger than the capacity deletes key instead.
func (c *Cache[K, V]) Replace(key K, value V, size int64) bool {
	if size > c.capacity() {
		c.Delete(key)
		return false
	}
	hash := c.opts.Hash(key)
	ps, ix := c.policyOf(hash), c.indexOf(hash)
	value.Retain()
	published := false
	defer func() {
		if !published {
			value.Release()
		}
	}()
	r, e, seq, inMain, ok := c.replace(ps, ix, key, value, size)
	if !ok {
		return false
	}
	published = true
	c.finish(r)
	c.queue(ps, e, value, size, seq, inMain)
	c.evictOver()
	return true
}

// replace swaps the value of key's entry and takes it out of its queue until queue puts it
// back. It returns the old value's removal, the entry, its new sequence number, and whether it
// was in the main queue.
func (c *Cache[K, V]) replace(ps *policyShard[K, V], ix *indexShard[K, V], key K, value V, size int64) (r removal[K, V], e *entry[K, V], seq uint64, inMain, ok bool) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ix.mu.Lock()
	defer ix.mu.Unlock()
	e = ix.index[key]
	if e == nil {
		return r, nil, 0, false, false
	}
	delta := size - e.size
	c.charge(value, max(delta, 0), math.MaxInt64)
	if delta < 0 {
		c.used.Add(delta)
	}
	if q := ps.queueOf(e); q != nil {
		inMain = q == &ps.main
		q.forget(e)
	}
	r = removal[K, V]{key: e.key, value: e.value, size: e.size, seq: e.seq, reason: Replaced}
	e.value, e.size, e.seq = value, size, c.seq.Add(1)
	return r, e, e.seq, inMain, true
}

// Delete removes key and releases its value.
func (c *Cache[K, V]) Delete(key K) {
	hash := c.opts.Hash(key)
	if r, ok := c.remove(c.policyOf(hash), c.indexOf(hash), key, nil); ok {
		c.finish(r)
	}
}

// remove takes key out of the cache, only if its entry is e when e is not nil.
func (c *Cache[K, V]) remove(ps *policyShard[K, V], ix *indexShard[K, V], key K, e *entry[K, V]) (removal[K, V], bool) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ix.mu.Lock()
	defer ix.mu.Unlock()
	cur := ix.index[key]
	if cur == nil || (e != nil && cur != e) {
		return removal[K, V]{}, false
	}
	return c.removeLocked(ps, ix, cur, Deleted), true
}

// DeleteGroup removes every key of group and releases their values. It needs Group.
func (c *Cache[K, V]) DeleteGroup(group string) {
	if c.opts.Group == nil {
		panic("s3fifo: DeleteGroup needs Group")
	}
	for i := range c.index {
		ix := &c.index[i]
		for _, e := range ix.groupEntries(group) {
			if r, ok := c.remove(c.policyOf(e.hash), ix, e.key, e); ok {
				c.finish(r)
			}
		}
	}
}

// EvictTo evicts values until Used is at most target, and returns Used. It stops early when
// ctx is done or nothing evictable is left, and does not check Used against the capacity, so
// Used can be over target or the capacity when it returns. Values whose insertion is still
// being notified are counted in Used but not yet evictable.
func (c *Cache[K, V]) EvictTo(ctx context.Context, target int64) int64 {
	var buf [evictBatch]removal[K, V]
	return c.evictTo(ctx, target, buf[:0])
}

// evictTo is EvictTo evicting in batches of the capacity of buf.
func (c *Cache[K, V]) evictTo(ctx context.Context, target int64, buf []removal[K, V]) int64 {
	target = max(target, 0)
	for {
		// The evicted values are released outside the evictor, so that a slow release does
		// not hold up the other evictions.
		c.evictor.Lock()
		removed, used, done := c.evictBatch(target, buf)
		c.evictor.Unlock()
		c.finishAll(removed)
		clear(removed)
		if done || ctx.Err() != nil {
			return used
		}
	}
}

// evictOver evicts while Used is over the capacity, in small batches as a Set has few values
// to evict. When another goroutine holds the evictor, it leaves the eviction to that one
// unless Used is over the ceiling, a sixteenth over the capacity, and then waits to evict too.
// It checks Used again each time it lets go of the evictor, so that it evicts the bytes of the
// Sets that left the eviction to it. It stops when nothing is left to evict: the bytes of a Set
// not yet queued are evicted by that Set.
func (c *Cache[K, V]) evictOver() {
	var buf [8]removal[K, V]
	for {
		capacity := c.capacity()
		used := c.used.Load()
		if used <= capacity {
			return
		}
		if used > capacity+capacity/16 {
			c.evictor.Lock()
		} else if !c.evictor.TryLock() {
			return
		}
		removed, used, done := c.evictBatch(capacity, buf[:0])
		c.evictor.Unlock()
		c.finishAll(removed)
		clear(removed)
		if done && used > capacity {
			return
		}
	}
}

// evictBatch evicts values into removed, up to its capacity, while Used is over target. done
// reports that Used is within target or that nothing is left to evict. The caller holds the
// evictor.
func (c *Cache[K, V]) evictBatch(target int64, removed []removal[K, V]) (_ []removal[K, V], used int64, done bool) {
	capacity := c.capacity()
	for len(removed) < cap(removed) {
		if used = c.used.Load(); used <= target {
			return removed, used, true
		}
		r, ok := c.evictOne(capacity, target)
		if !ok {
			return removed, c.used.Load(), true
		}
		removed = append(removed, r)
		c.trimGhost(capacity - capacity/10)
	}
	return removed, c.used.Load(), false
}

// evictOne evicts one value toward target: from the small queue while it holds more than a
// tenth of the capacity, less what target cuts from the capacity, otherwise from the main
// queue, taking the oldest member over all shards. A small-queue value read more than once
// moves to the main queue; a main-queue value read since it was last passed over goes back,
// one read fewer. An evicted value with bytes is remembered in the ghost queue. Once a queue is
// chosen, it is popped until a value of it is evicted or it is empty.
func (c *Cache[K, V]) evictOne(capacity, target int64) (removal[K, V], bool) {
	smallLimit := max(capacity/10-max(capacity-target, 0), 0)
	for {
		small := c.smallBytes.Load() > smallLimit || c.mainBytes.Load() == 0
		if ps, _ := c.oldest(small); ps == nil {
			small = !small
		}
		for {
			ps, head := c.oldest(small)
			if ps == nil {
				if c.smallBytes.Load() == 0 && c.mainBytes.Load() == 0 {
					return removal[K, V]{}, false
				}
				break
			}
			if r, evicted := c.step(ps, small, head); evicted {
				return r, true
			}
		}
	}
}

// oldest returns the policy shard holding the oldest member of the small or the main queue,
// and that member's order.
func (c *Cache[K, V]) oldest(small bool) (*policyShard[K, V], uint64) {
	var found *policyShard[K, V]
	min := uint64(noOrder)
	for i := range c.policy {
		ps := &c.policy[i]
		q := &ps.main
		if small {
			q = &ps.small
		}
		if h := q.head.Load(); h < min {
			min, found = h, ps
		}
	}
	return found, min
}

// step takes the oldest member of one queue of ps and evicts it, or moves it on. It does
// neither when that member is no longer the one at order head.
func (c *Cache[K, V]) step(ps *policyShard[K, V], small bool, head uint64) (removal[K, V], bool) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	q := &ps.main
	if small {
		q = &ps.small
	}
	if q.head.Load() != head {
		return removal[K, V]{}, false
	}
	e, ok := q.pop()
	if !ok {
		return removal[K, V]{}, false
	}
	if small && e.freq.Load() > 1 {
		ps.main.push(e, c.order.Add(1))
		return removal[K, V]{}, false
	}
	if !small && e.freq.Load() > 0 {
		e.freq.Add(-1)
		ps.main.push(e, c.order.Add(1))
		return removal[K, V]{}, false
	}
	if e.size > 0 {
		ps.ghost.add(e.hash, e.size, c.order.Add(1))
	}
	return c.evictLocked(ps, e), true
}

func (c *Cache[K, V]) evictLocked(ps *policyShard[K, V], e *entry[K, V]) removal[K, V] {
	ix := c.indexOf(e.hash)
	ix.mu.Lock()
	defer ix.mu.Unlock()
	return c.removeLocked(ps, ix, e, Evicted)
}

// trimGhost forgets the oldest ghost entries over all shards while the ghost queue holds more
// than target bytes, keeping at least one.
func (c *Cache[K, V]) trimGhost(target int64) {
	for c.ghostBytes.Load() > target {
		var found *policyShard[K, V]
		min, n := uint64(noOrder), 0
		for i := range c.policy {
			if h := c.policy[i].ghost.head.Load(); h != noOrder {
				n++
				if h < min {
					min, found = h, &c.policy[i]
				}
			}
		}
		if found == nil || n == 1 && found.ghostLen() <= 1 {
			return
		}
		found.dropGhost()
	}
}

// removeLocked takes e out of the index, its group, its queue and Used.
func (c *Cache[K, V]) removeLocked(ps *policyShard[K, V], ix *indexShard[K, V], e *entry[K, V], reason Reason) removal[K, V] {
	delete(ix.index, e.key)
	ix.removeFromGroup(c.opts.Group, e)
	if q := ps.queueOf(e); q != nil {
		q.forget(e)
	}
	e.removed = true
	c.used.Add(-e.size)
	r := removal[K, V]{key: e.key, value: e.value, size: e.size, seq: e.seq, reason: reason}
	var zero V
	e.value = zero
	return r
}

// finishAll finishes rs; when a notification panics, the values not yet finished are still
// released before the panic goes on.
func (c *Cache[K, V]) finishAll(rs []removal[K, V]) {
	next := 0
	defer func() {
		for ; next < len(rs); next++ {
			rs[next].value.Release()
		}
	}()
	for next < len(rs) {
		r := rs[next]
		next++
		c.finish(r)
	}
}

// finish notifies a removal and releases its value, outside the locks.
func (c *Cache[K, V]) finish(r removal[K, V]) {
	defer r.value.Release()
	if c.opts.OnRemove != nil {
		c.opts.OnRemove(r.key, r.value, r.size, r.seq, r.reason)
	}
}

func (ps *policyShard[K, V]) ghostLen() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.ghost.len()
}

func (ps *policyShard[K, V]) dropGhost() {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.ghost.drop()
}

func (ps *policyShard[K, V]) queueOf(e *entry[K, V]) *fifo[K, V] {
	switch e.queue {
	case inSmall:
		return &ps.small
	case inMain:
		return &ps.main
	}
	return nil
}

func (ix *indexShard[K, V]) contains(key K) bool {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	_, ok := ix.index[key]
	return ok
}

func (ix *indexShard[K, V]) groupEntries(group string) []*entry[K, V] {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	var entries []*entry[K, V]
	for e := ix.groups[group]; e != nil; e = e.nextInGroup {
		entries = append(entries, e)
	}
	return entries
}

func (ix *indexShard[K, V]) addToGroup(group func(K) string, e *entry[K, V]) {
	if group == nil {
		return
	}
	g := group(e.key)
	if first := ix.groups[g]; first != nil {
		first.prevInGroup = e
		e.nextInGroup = first
	}
	ix.groups[g] = e
}

func (ix *indexShard[K, V]) removeFromGroup(group func(K) string, e *entry[K, V]) {
	if group == nil {
		return
	}
	if e.prevInGroup != nil {
		e.prevInGroup.nextInGroup = e.nextInGroup
	} else if e.nextInGroup != nil {
		ix.groups[group(e.key)] = e.nextInGroup
	} else {
		delete(ix.groups, group(e.key))
	}
	if e.nextInGroup != nil {
		e.nextInGroup.prevInGroup = e.prevInGroup
	}
	e.prevInGroup, e.nextInGroup = nil, nil
}
