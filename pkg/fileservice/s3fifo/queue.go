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

package s3fifo

import (
	"math"
	"sync/atomic"
)

// noOrder is the head order of an empty queue.
const noOrder = math.MaxUint64

// fifo is one shard's part of a cache queue. An entry that leaves the queue other than by pop
// keeps its slot until popped or compacted away; a slot is live while its entry is in the queue
// at the slot's order, so an entry pushed again does not bring its old slot back. bytes counts
// only the members, as does total, the queue's bytes over all shards. head is the order of the
// oldest member, readable without the shard lock.
type fifo[K comparable, V Value] struct {
	id    queueID
	buf   []slot[K, V]
	first int
	dead  int
	bytes int64
	total *atomic.Int64
	head  atomic.Uint64
}

type slot[K comparable, V Value] struct {
	e     *entry[K, V]
	order uint64
}

func (q *fifo[K, V]) init(id queueID, total *atomic.Int64) {
	q.id, q.total = id, total
	q.head.Store(noOrder)
}

func (q *fifo[K, V]) live(s slot[K, V]) bool {
	return s.e.queue == q.id && s.e.order == s.order
}

// push appends e as the newest member, at order.
func (q *fifo[K, V]) push(e *entry[K, V], order uint64) {
	e.queue, e.order = q.id, order
	q.buf = append(q.buf, slot[K, V]{e: e, order: order})
	q.resize(e.size)
	if len(q.buf)-q.first-q.dead == 1 {
		q.head.Store(order)
	}
}

// pop removes the oldest member. The returned entry belongs to no queue.
func (q *fifo[K, V]) pop() (*entry[K, V], bool) {
	q.skipDead()
	if q.first == len(q.buf) {
		return nil, false
	}
	e := q.buf[q.first].e
	q.buf[q.first] = slot[K, V]{}
	q.first++
	e.queue = 0
	q.resize(-e.size)
	q.compact()
	q.skipDead()
	return e, true
}

// forget accounts for member e leaving the queue other than by pop.
func (q *fifo[K, V]) forget(e *entry[K, V]) {
	e.queue = 0
	q.resize(-e.size)
	q.dead++
	q.compact()
	q.skipDead()
}

// resize changes the queue's bytes by delta.
func (q *fifo[K, V]) resize(delta int64) {
	q.bytes += delta
	q.total.Add(delta)
}

// skipDead drops the slots at the front that are not live, and publishes the new head.
func (q *fifo[K, V]) skipDead() {
	for q.first < len(q.buf) && !q.live(q.buf[q.first]) {
		q.buf[q.first] = slot[K, V]{}
		q.first++
		q.dead--
	}
	if q.first == len(q.buf) {
		q.buf, q.first, q.dead = q.buf[:0], 0, 0
		q.head.Store(noOrder)
		return
	}
	q.head.Store(q.buf[q.first].order)
}

// compact drops the popped prefix, and the slots that are not live once they outnumber the
// members.
func (q *fifo[K, V]) compact() {
	if q.first > 1024 && q.first*2 > len(q.buf) {
		n := copy(q.buf, q.buf[q.first:])
		clear(q.buf[n:])
		q.buf, q.first = q.buf[:n], 0
	}
	if q.dead > 1024 && q.dead*2 > len(q.buf)-q.first {
		kept := q.buf[:0]
		for _, s := range q.buf[q.first:] {
			if q.live(s) {
				kept = append(kept, s)
			}
		}
		clear(q.buf[len(kept):])
		q.buf, q.first, q.dead = kept, 0, 0
	}
}

// ghost is one shard's part of the ghost queue: the hashes of recently evicted keys with their
// bytes, oldest first. A hash is remembered by its latest entry until a hit takes it or that
// entry leaves the queue. Every entry counts its bytes until it leaves the queue, as in the
// cache this package replaces, in total, the ghost queue's bytes over all shards. head is the
// order of this part's oldest entry, readable without the shard lock.
type ghost struct {
	latest map[uint64]uint64
	ring   []ghostEntry
	first  int
	total  *atomic.Int64
	head   atomic.Uint64
}

type ghostEntry struct {
	hash  uint64
	size  int64
	order uint64
}

func (g *ghost) init(total *atomic.Int64) {
	g.latest = make(map[uint64]uint64)
	g.total = total
	g.head.Store(noOrder)
}

func (g *ghost) len() int {
	return len(g.ring) - g.first
}

func (g *ghost) contains(hash uint64) bool {
	_, ok := g.latest[hash]
	return ok
}

// take reports whether hash is remembered, and forgets it.
func (g *ghost) take(hash uint64) bool {
	if _, ok := g.latest[hash]; !ok {
		return false
	}
	delete(g.latest, hash)
	return true
}

func (g *ghost) add(hash uint64, size int64, order uint64) {
	g.ring = append(g.ring, ghostEntry{hash: hash, size: size, order: order})
	g.latest[hash] = order
	g.total.Add(size)
	if g.len() == 1 {
		g.head.Store(order)
	}
}

// drop removes the oldest entry, forgetting its hash only if it is the hash's latest entry.
func (g *ghost) drop() {
	if g.first == len(g.ring) {
		return
	}
	old := g.ring[g.first]
	g.first++
	g.total.Add(-old.size)
	if order, ok := g.latest[old.hash]; ok && order == old.order {
		delete(g.latest, old.hash)
	}
	if g.first > 1024 && g.first*2 > len(g.ring) {
		n := copy(g.ring, g.ring[g.first:])
		g.ring, g.first = g.ring[:n], 0
	}
	if g.first == len(g.ring) {
		g.ring, g.first = g.ring[:0], 0
		g.head.Store(noOrder)
		return
	}
	g.head.Store(g.ring[g.first].order)
}
