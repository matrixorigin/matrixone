// Copyright 2022 Matrix Origin
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

package hashmap

import (
	"math/bits"
	"sync/atomic"
)

func New[K comparable, V any](cap int) *Map[K, V] {
	if cap < 1 {
		cap = 1
	}
	m := &Map[K, V]{}
	m.rehash(uint32(cap))
	return m
}

func (m *Map[K, V]) Get(h uint64, k K) (*V, bool) {
	var dist uint32

	for i := uint32(h >> m.shift); ; i++ {
		b := &m.buckets[i]
		if b.h == h && b.key == k {
			return b.val, b.val != nil
		}
		if b.dist < dist { // not found
			return nil, false
		}
		dist++
	}
}

// Set sets the value for the given key.
// return true if the key already exists.
func (m *Map[K, V]) Set(h uint64, k K, v *V) bool {
	return m.set(h, k, v)
}

func (m *Map[K, V]) Delete(h uint64, k K) {
	var dist uint32

	for i := uint32(h >> m.shift); ; i++ {
		b := &m.buckets[i]
		// found, shift the following buckets backwards
		// util the next bucket is empty or has zero distance.
		// note the empty values ara guarded by the zero distance.
		if b.h == h && b.key == k {
			for j := i + 1; ; j++ {
				t := &m.buckets[j]
				if t.dist == 0 {
					atomic.AddInt32(&m.count, -1)
					*b = bucket[K, V]{}
					return
				}
				b.h = t.h
				b.key = t.key
				b.val = t.val
				b.dist = t.dist - 1
				b = t
			}
		}
		if dist > b.dist { // not found
			return
		}
		dist++
	}
}

func (m *Map[K, V]) Len() int {
	return int(atomic.LoadInt32(&m.count))
}

func (m *Map[K, V]) set(h uint64, k K, v *V) bool {
	maybeExists := true
	n := bucket[K, V]{h: h, key: k, val: v, dist: 0}
	for i := uint32(h >> m.shift); ; i++ {
		b := &m.buckets[i]
		if maybeExists && b.h == h && b.key == k { // exists, update
			b.h = n.h
			b.val = n.val
			return true
		}
		if b.val == nil { // empty bucket, insert here
			atomic.AddInt32(&m.count, 1)
			*b = n
			return false
		}
		if b.dist < n.dist {
			n, *b = *b, n
			maybeExists = false
		}
		// far away, swap and keep searching
		n.dist++
		// rehash if the distance is too big
		if n.dist == m.maxDist {
			m.rehash(2 * m.size)
			i = uint32(n.h>>m.shift) - 1
			n.dist = 0
			maybeExists = false
		}
	}
}

func (m *Map[K, V]) rehash(size uint32) {
	oldBuckets := m.buckets
	m.count = 0
	m.size = size
	m.shift = uint32(64 - bits.Len32(m.size-1))
	m.maxDist = maxDistForSize(size)
	m.buckets = make([]bucket[K, V], size+m.maxDist)
	for i := range oldBuckets {
		b := &oldBuckets[i]
		if b.val != nil {
			m.set(b.h, b.key, b.val)
		}
	}
}

func maxDistForSize(size uint32) uint32 {
	desired := uint32(bits.Len32(size))
	if desired < 4 {
		desired = 4
	}
	return desired
}
