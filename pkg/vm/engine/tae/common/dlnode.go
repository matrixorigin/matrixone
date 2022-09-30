// Copyright 2021 Matrix Origin
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

package common

import (
	"sync"
)

// Usage Example
/*
type Row struct {
	id int
}

func compare(a, b *Row) int {
	if a.id > b.id {
		return 1
	} else if a.id < b.id {
		return -1
	}
	return 0
}

dlist := NewGenericSortedDList[*Row](compare)
n1 := dlist.Insert(&Row{id: 10}) // [10]
n2 := dlist.Insert(&Row{id: 5})  // [10]<->[5]
n3 := dlist.Insert(&Row{id: 13}) // [13]<->[10]<->[5]
n3.id = 8
dlist.Update(n3)                 // [10]<->[8]<->[5]
dlist.Delete(n1)                 // [8]<->[5]

it := NewGenericSortedDListIt(nil, dlist,true)
for it.Valid() {
	n := it.GetPayload()
	// n.xxx
	it.Next()
}
*/

// Sorted doubly linked-list
type GenericSortedDList[T any] struct {
	head    *GenericDLNode[T]
	tail    *GenericDLNode[T]
	compare func(T, T) int
	depth   int
}

func NewGenericSortedDList[T any](compare func(T, T) int) *GenericSortedDList[T] {
	return &GenericSortedDList[T]{
		compare: compare,
	}
}

// Get the head node
func (l *GenericSortedDList[T]) GetHead() *GenericDLNode[T] {
	return l.head
}

// Get the tail node
func (l *GenericSortedDList[T]) GetTail() *GenericDLNode[T] {
	return l.tail
}

// Update the node to keep the list be sorted
//
// [List] [1,x1] <-> [3,x3] <-> [10,x10] <-> [20,x20]
//
//	|
//
// [Node]                       [10,x10]
//
// --------- UPDATE [10,x10] TO [2, x10]--------------
//
// [List] [1,x1] <-> [2,x10] <-> [3,x3] <-> [20,x20]
func (l *GenericSortedDList[T]) Update(n *GenericDLNode[T]) {
	nhead, ntail := n.KeepSorted(l.compare)
	if nhead != nil {
		l.head = nhead
	}
	if ntail != nil {
		l.tail = ntail
	}
}

// Get the length of the list
func (l *GenericSortedDList[T]) Depth() int {
	return l.depth
}

// Insert a object and wrap it as GenericDLNode into the list
// The inserted object must be instance of interface NodePayload
// [List]: [1,x1] <-> [5,x5] <-> [10,x10]
// Insert a node [7,x7]
// [List]: [1,x1] <-> [5,x5] <-> [7,x7] <-> [10,x10]
func (l *GenericSortedDList[T]) Insert(payload T) *GenericDLNode[T] {
	var (
		n    *GenericDLNode[T]
		tail *GenericDLNode[T]
	)
	n, l.head, tail = InsertGenericDLNode(payload, l.head, l.compare)
	if tail != nil {
		l.tail = tail
	}
	l.depth += 1
	return n
}

// Given a node and remove it from the list
//
//	Delete [node]
//
// [prev node] <-> [node] <-> [next node] =============> [prev node] <-> [next node]
func (l *GenericSortedDList[T]) Delete(n *GenericDLNode[T]) {
	prev := n.prev
	next := n.next
	if prev != nil && next != nil {
		prev.next = next
		next.prev = prev
	} else if prev == nil && next != nil {
		l.head = next
		next.prev = nil
	} else if prev != nil && next == nil {
		l.tail = prev
		prev.next = nil
	} else {
		l.head = nil
		l.tail = nil
	}
	l.depth -= 1
}

// Loop the list and apply fn on each node
func (l *GenericSortedDList[T]) Loop(fn func(n *GenericDLNode[T]) bool, reverse bool) {
	if reverse {
		LoopGenericSortedDList(l.tail, fn, reverse)
	} else {
		LoopGenericSortedDList(l.head, fn, reverse)
	}
}

// Doubly sorted linked-list node
type GenericDLNode[T any] struct {
	prev, next *GenericDLNode[T]
	payload    T
}

func (l *GenericDLNode[T]) GetPayload() T              { return l.payload }
func (l *GenericDLNode[T]) GetPrev() *GenericDLNode[T] { return l.prev }
func (l *GenericDLNode[T]) GetNext() *GenericDLNode[T] { return l.next }

// Keep node be sorted in the list
func (l *GenericDLNode[T]) KeepSorted(compare func(T, T) int) (head, tail *GenericDLNode[T]) {
	curr := l
	head = curr
	prev := l.prev
	next := l.next
	for (curr != nil && next != nil) && (compare(curr.payload, next.payload) < 0) {
		if head == curr {
			head = next
		}

		if prev != nil {
			prev.next = next
		}
		next.prev = prev

		prev = next
		next = next.next

		prev.next = curr
		curr.prev = prev
		curr.next = next
		if next != nil {
			next.prev = curr
		}
	}
	if next == nil {
		tail = curr
	}
	if head.prev != nil {
		head = nil
	}
	return head, tail
}

// Insert a wrapped object into a list specified by a head node
// node is the inserted dlnode
// nhead is the new head node
// ntail is the new tail node.
// If ntail is not nil, tail is updated. Else tail is not updated
func InsertGenericDLNode[T any](payload T,
	head *GenericDLNode[T],
	compare func(T, T) int) (node, nhead, ntail *GenericDLNode[T]) {
	node = &GenericDLNode[T]{
		payload: payload,
	}
	if head == nil {
		nhead = node
		ntail = node
		return
	}

	node.next = head
	head.prev = node
	nhead, ntail = node.KeepSorted(compare)
	return
}

// Loop the list and apply fn on each node
// head is the head node of a list
// fn is operation applied to each node during iterating.
// if fn(node) returns false, stop iterating.
// reverse is true to loop in reversed way.
func LoopGenericSortedDList[T any](head *GenericDLNode[T],
	fn func(node *GenericDLNode[T]) bool,
	reverse bool) {
	curr := head
	for curr != nil {
		goNext := fn(curr)
		if !goNext {
			break
		}
		if reverse {
			curr = curr.prev
		} else {
			curr = curr.next
		}
	}
}

// Sorted doubly linked-list iterator
type GenericSortedDListIt[T any] struct {
	linkLocker *sync.RWMutex
	curr       *GenericDLNode[T]
	nextFunc   func(*GenericDLNode[T]) *GenericDLNode[T]
}

// linkLocker is the outer locker to guard dlist access
func NewGenericSortedDListIt[T any](linkLocker *sync.RWMutex,
	dlist *GenericSortedDList[T],
	reverse bool) *GenericSortedDListIt[T] {
	it := &GenericSortedDListIt[T]{
		linkLocker: linkLocker,
	}
	if reverse {
		it.nextFunc = func(n *GenericDLNode[T]) *GenericDLNode[T] {
			return n.prev
		}
		it.curr = dlist.tail
	} else {
		it.nextFunc = func(n *GenericDLNode[T]) *GenericDLNode[T] {
			return n.next
		}
		it.curr = dlist.head
	}
	return it
}

func (it *GenericSortedDListIt[T]) Valid() bool {
	return it.curr != nil
}

func (it *GenericSortedDListIt[T]) Next() {
	if it.linkLocker == nil {
		it.curr = it.nextFunc(it.curr)
		return
	}
	it.linkLocker.RLock()
	it.curr = it.nextFunc(it.curr)
	it.linkLocker.RUnlock()
}

func (it *GenericSortedDListIt[T]) Get() *GenericDLNode[T] {
	return it.curr
}
