// Copyright 2021 - 2022 Matrix Origin
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

package lazylist

//
// A sorted list, see the LazyList described in
// The Art of Multiprocessor Programming, Chapter 9
//

import (
	"sync"
	"sync/atomic"
)

// node of the lazylist
type node[K any, V any] struct {
	key    K
	val    V
	next   atomic.Pointer[node[K, V]]
	marked atomic.Bool
	lock   sync.Mutex
}

func (n *node[K, V]) isMarked() bool {
	return n.marked.Load()
}

func (n *node[K, V]) isNotMarked() bool {
	return !n.marked.Load()
}

type Iter[K any, V any] struct {
	ka, kz K
	curr   *node[K, V]
}

func (iter Iter[K, V]) GetKey() K {
	return iter.curr.key
}

func (iter Iter[K, V]) GetVal() V {
	return iter.curr.val
}

type LazyList[K any, V any] struct {
	// sentinels
	head, tail *node[K, V]
	less       func(a, b K) bool
	eq         func(a, b K) bool
}

func New[K any, V any](less, eq func(a, b K) bool) *LazyList[K, V] {
	var l LazyList[K, V]
	l.head = &node[K, V]{}
	l.tail = &node[K, V]{}
	l.head.next.Store(l.tail)
	l.less = less
	l.eq = eq
	return &l
}

// Check prev and curr and still in the list and adjacent.
func (list *LazyList[K, V]) validate(pred, curr *node[K, V]) bool {
	return pred.isNotMarked() && curr.isNotMarked() && pred.next.Load() == curr
}

// return if pred, curr are still valid, and if valid the result of add.
func (list *LazyList[K, V]) lockAdd(pred, curr *node[K, V], key K, val V) (bool, bool) {
	pred.lock.Lock()
	defer pred.lock.Unlock()
	curr.lock.Lock()
	defer curr.lock.Unlock()
	// still valid?
	if list.validate(pred, curr) {
		if list.eq(curr.key, key) {
			// already exist
			return true, false
		} else {
			var node node[K, V]
			node.key = key
			node.val = val
			node.next.Store(curr)
			pred.next.Store(&node)
			return true, true
		}
	}
	return false, false
}

// Adding K, V to the list if not already exists.   Return false if already exists.
func (list *LazyList[K, V]) Add(key K, val V) bool {
	for {
		pred := list.head
		curr := pred.next.Load()
		// walk down the list
		for curr != list.tail && list.less(curr.key, key) {
			pred = curr
			curr = pred.next.Load()
		}

		valid, ret := list.lockAdd(pred, curr, key, val)
		if valid {
			return ret
		}
		// for loop
	}
}

func (list *LazyList[K, V]) lockRemove(pred, curr *node[K, V], key K) (bool, bool) {
	pred.lock.Lock()
	defer pred.lock.Unlock()
	curr.lock.Lock()
	defer curr.lock.Unlock()
	// still valid?
	if list.validate(pred, curr) {
		if !list.eq(curr.key, key) {
			return true, false
		} else {
			// logical remove
			curr.marked.Store(true)
			// physical remove
			pred.next.Store(curr.next.Load())
			return true, true
		}
	}
	return false, false
}

func (list *LazyList[K, V]) Remove(key K) bool {
	for {
		pred := list.head
		curr := pred.next.Load()
		for curr != list.tail && list.less(curr.key, key) {
			pred = curr
			curr = pred.next.Load()
		}

		valid, ret := list.lockRemove(pred, curr, key)
		if valid {
			return ret
		}
		// loop
	}
}

func (list *LazyList[K, V]) Lookup(key K) (V, bool) {
	curr := list.head.next.Load()
	for curr != list.tail && list.less(curr.key, key) {
		curr = curr.next.Load()
	}
	if list.eq(curr.key, key) && curr.isNotMarked() {
		return curr.val, true
	}
	return list.head.val, false
}

// Iter though list, [ka, kz)
func (list *LazyList[K, V]) Iterator(ka, kz K) *Iter[K, V] {
	curr := list.head.next.Load()
	for curr != list.tail && (curr.isNotMarked() || list.less(curr.key, ka)) {
		curr = curr.next.Load()
	}

	// Not found
	if curr == list.tail || !list.less(curr.key, kz) {
		return nil
	}

	return &Iter[K, V]{ka: ka, kz: kz, curr: curr}
}

func (list *LazyList[K, V]) Next(it *Iter[K, V]) *Iter[K, V] {
	// move to the next
	it.curr = it.curr.next.Load()
	for it.curr != list.tail && it.curr.isMarked() {
		it.curr = it.curr.next.Load()
	}

	if it.curr == list.tail || !list.less(it.curr.key, it.kz) {
		return nil
	}
	return it
}
