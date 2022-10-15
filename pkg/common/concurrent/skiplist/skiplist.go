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

package skiplist

import (
	"math/rand"
	"sync"
	"sync/atomic"
)

//
// skiplist, see the LazySkiplist described in
// The Art of Multiprocessor Programming, Chapter 14
//

// maxLevel at 7, bottom level is level 0, so we have at most 8 levels.
const (
	maxLevel = 7
	numLevel = 8
)

type node[K any, V any] struct {
	key         K
	val         V
	topLv       int
	marked      atomic.Bool
	fullyLinked atomic.Bool
	lock        sync.Mutex
	next        [numLevel]atomic.Pointer[node[K, V]]
}

func (n *node[K, V]) isNotMarked() bool {
	return !n.marked.Load()
}

func (n *node[K, V]) nextIs(lv int, curr *node[K, V]) bool {
	return n.next[lv].Load() == curr
}

// new node is created at a random level in [0, maxLevel)
// It is probalistic -- probablity 3/4(1/4)^N at lv N.
func randomLv() int {
	n := 1 << (numLevel * 2)
	r := rand.Intn(n)
	for i := 0; i < maxLevel; i++ {
		if r >= 1<<((maxLevel-i)*2) {
			return i
		}
	}
	return maxLevel - 1
}

// create a new node at a level.
func newNode[K any, V any](k K, v V, lv int) *node[K, V] {
	return &node[K, V]{
		key: k, val: v, topLv: lv,
	}
}

type Skiplist[K any, V any] struct {
	head, tail *node[K, V]
	less       func(a, b K) bool
	eq         func(a, b K) bool
}

func New[K any, V any](less, eq func(a, b K) bool) *Skiplist[K, V] {
	var lsl Skiplist[K, V]
	lsl.head = &node[K, V]{topLv: maxLevel}
	lsl.tail = &node[K, V]{topLv: maxLevel}
	for i := range lsl.head.next {
		lsl.head.next[i].Store(lsl.tail)
	}
	lsl.less = less
	lsl.eq = eq
	return &lsl
}

func (lsl *Skiplist[K, V]) find(k K, preds, succs *[numLevel]*node[K, V]) int {
	lvFound := -1
	pred := lsl.head
	// Search level top down to bottom.
	for lv := maxLevel; lv >= 0; lv-- {
		curr := pred.next[lv].Load()
		for curr != lsl.tail && lsl.less(curr.key, k) {
			pred = curr
			curr = pred.next[lv].Load()
		}

		if curr != lsl.tail && lsl.eq(curr.key, k) {
			lvFound = lv
		}
		preds[lv] = pred
		succs[lv] = curr
	}
	return lvFound
}

func (lsl *Skiplist[K, V]) lockAdd(topLv int, k K, v V, preds, succs *[numLevel]*node[K, V]) bool {
	var pred, succ *node[K, V]
	// This loop, we acquire lock from bottom lv and up.   This is important
	// for deadlock avoidance.
	for lv := 0; lv <= topLv; lv++ {
		pred = preds[lv]
		succ = succs[lv]
		// our lock is not reentrant, poor man's solution
		if lv == 0 || preds[lv] != preds[lv-1] {
			pred.lock.Lock()
			defer pred.lock.Unlock()
		}
		// check valid, that is, pred and succ not marked and pred->next == succ
		valid := pred.isNotMarked() && succ.isNotMarked() && pred.nextIs(lv, succ)
		if !valid {
			return false
		}
	}

	nn := newNode(k, v, topLv)
	for lv := 0; lv <= topLv; lv++ {
		nn.next[lv].Store(succs[lv])
	}
	for lv := 0; lv <= topLv; lv++ {
		preds[lv].next[lv].Store(nn)
	}
	nn.fullyLinked.Store(true)
	return true
}

func (lsl *Skiplist[K, V]) Add(k K, v V) bool {
	topLv := randomLv()
	var preds [numLevel]*node[K, V]
	var succs [numLevel]*node[K, V]
	for {
		lvFound := lsl.find(k, &preds, &succs)
		if lvFound >= 0 {
			nodeFound := succs[lvFound]
			if nodeFound.isNotMarked() {
				for !nodeFound.fullyLinked.Load() {
					// spin
				}
				// found already exists, return not added.
				return false
			}
			// found a marked node, retry ...
			continue
		}
		valid := lsl.lockAdd(topLv, k, v, &preds, &succs)
		if !valid {
			// not valid, retry
			continue
		}
		return true
	}
}

func (lsl *Skiplist[K, V]) lockRemove(victim *node[K, V], preds, succs *[numLevel]*node[K, V]) (bool, bool) {
	// lock victim
	victim.lock.Lock()
	defer victim.lock.Unlock()
	if victim.marked.Load() {
		// Already marked for deletion
		return true, false
	}

	// lock preds, in asceding order, deadlock avoidance.
	topLv := victim.topLv
	for lv := 0; lv <= topLv; lv++ {
		pred := preds[lv]
		// our lock is not reentrant, poor man's solution
		if lv == 0 || preds[lv] != preds[lv-1] {
			pred.lock.Lock()
			defer pred.lock.Unlock()
		}
		valid := pred.isNotMarked() && pred.nextIs(lv, victim)
		if !valid {
			return false, false
		}
	}

	// mark
	victim.marked.Store(true)
	// physical remove, top down so that we maintain the skiplist property.
	for lv := topLv; lv >= 0; lv-- {
		preds[lv].next[lv].Store(victim.next[lv].Load())
	}
	return true, true
}

func (lsl *Skiplist[K, V]) Remove(k K) bool {
	var preds [numLevel]*node[K, V]
	var succs [numLevel]*node[K, V]
	for {
		lvFound := lsl.find(k, &preds, &succs)
		if lvFound < 0 {
			return false
		}
		victim := succs[lvFound]
		// check if victim is ready to remove -- fully linked, not marked,
		// and topLv is lvFound.  node found below its topLv is either not
		// fully linked or marked and already partially unlinked by a
		// concurrent remove
		ready := victim.fullyLinked.Load() && (!victim.marked.Load()) && victim.topLv == lvFound
		if !ready {
			return false
		}
		// lock and remove.
		valid, ret := lsl.lockRemove(victim, &preds, &succs)
		if !valid {
			continue
		}
		return ret
	}
}

func (lsl *Skiplist[K, V]) Lookup(k K) (V, bool) {
	pred := lsl.head
	for lv := maxLevel; lv >= 0; lv-- {
		curr := pred.next[lv].Load()
		for curr != lsl.tail && lsl.less(curr.key, k) {
			pred = curr
			curr = pred.next[lv].Load()
		}

		if curr != lsl.tail && lsl.eq(curr.key, k) {
			return curr.val, !curr.marked.Load()
		}
	}
	return lsl.head.val, false
}
