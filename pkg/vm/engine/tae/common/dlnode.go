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

func (r *Row) Compare(o *Row) int {
	or := o.(*Row)
	if r.id > ir.id {
		return 1
	} else if r.id < ir.id {
		return -1
	}
	return 0
}

dlist := new(SortedDList)
n1 := dlist.Insert(&Row{id: 10}) // [10]
n2 := dlist.Insert(&Row{id: 5})  // [10]<->[5]
n3 := dlist.Insert(&Row{id: 13}) // [13]<->[10]<->[5]
n3.id = 8
dlist.Update(n3)                 // [10]<->[8]<->[5]
dlist.Delete(n1)                 // [8]<->[5]

it := NewSortedDListIt(nil, dlist,true)
for it.Valid() {
	n := it.GetPayload()
	// n.xxx
	it.Next()
}
*/

// Sorted doubly linked-list
type SortedDList struct {
	head *DLNode
	tail *DLNode
}

// Get the head node
func (l *SortedDList) GetHead() *DLNode {
	return l.head
}

// Get the tail node
func (l *SortedDList) GetTail() *DLNode {
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
func (l *SortedDList) Update(n *DLNode) {
	nhead, ntail := n.KeepSorted()
	if nhead != nil {
		l.head = nhead
	}
	if ntail != nil {
		l.tail = ntail
	}
}

// Get the length of the list
func (l *SortedDList) Depth() int {
	depth := 0
	l.Loop(func(_ *DLNode) bool {
		depth++
		return true
	}, false)
	return depth
}

// Insert a object and wrap it as DLNode into the list
// The inserted object must be instance of interface NodePayload
// [List]: [1,x1] <-> [5,x5] <-> [10,x10]
// Insert a node [7,x7]
// [List]: [1,x1] <-> [5,x5] <-> [7,x7] <-> [10,x10]
func (l *SortedDList) Insert(payload NodePayload) *DLNode {
	var (
		n    *DLNode
		tail *DLNode
	)
	n, l.head, tail = InsertDLNode(payload, l.head)
	if tail != nil {
		l.tail = tail
	}
	return n
}

// Given a node and remove it from the list
//
//	Delete [node]
//
// [prev node] <-> [node] <-> [next node] =============> [prev node] <-> [next node]
func (l *SortedDList) Delete(n *DLNode) {
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
}

// Loop the list and apply fn on each node
func (l *SortedDList) Loop(fn func(n *DLNode) bool, reverse bool) {
	if reverse {
		LoopSortedDList(l.tail, fn, reverse)
	} else {
		LoopSortedDList(l.head, fn, reverse)
	}
}

// wrapped object type by a DLNode
type NodePayload interface {
	Compare(NodePayload) int
}

// Doubly sorted linked-list node
type DLNode struct {
	prev, next *DLNode
	payload    NodePayload
}

func (l *DLNode) Compare(o *DLNode) int {
	return l.payload.Compare(o.payload)
}

func (l *DLNode) GetPayload() NodePayload { return l.payload }
func (l *DLNode) GetPrev() *DLNode        { return l.prev }
func (l *DLNode) GetNext() *DLNode        { return l.next }

// Keep node be sorted in the list
func (l *DLNode) KeepSorted() (head *DLNode, tail *DLNode) {
	curr := l
	head = curr
	prev := l.prev
	next := l.next
	for (curr != nil && next != nil) && (curr.Compare(next) < 0) {
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
func InsertDLNode(payload NodePayload, head *DLNode) (node, nhead, ntail *DLNode) {
	node = &DLNode{
		payload: payload,
	}
	if head == nil {
		nhead = node
		ntail = node
		return
	}

	node.next = head
	head.prev = node
	nhead, ntail = node.KeepSorted()
	return
}

// Given a node of a dlist list, find the head node
func FindHead(n *DLNode) *DLNode {
	head := n
	for head.prev != nil {
		head = head.prev
	}
	return head
}

// Loop the list and apply fn on each node
// head is the head node of a list
// fn is operation applied to each node during iterating.
// if fn(node) returns false, stop iterating.
// reverse is true to loop in reversed way.
func LoopSortedDList(head *DLNode, fn func(node *DLNode) bool, reverse bool) {
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
type SortedDListIt struct {
	linkLocker *sync.RWMutex
	curr       *DLNode
	nextFunc   func(*DLNode) *DLNode
}

// linkLocker is the outer locker to guard dlist access
func NewSortedDListIt(linkLocker *sync.RWMutex, dlist *SortedDList, reverse bool) *SortedDListIt {
	it := &SortedDListIt{
		linkLocker: linkLocker,
	}
	if reverse {
		it.nextFunc = func(n *DLNode) *DLNode {
			return n.prev
		}
		it.curr = dlist.tail
	} else {
		it.nextFunc = func(n *DLNode) *DLNode {
			return n.next
		}
		it.curr = dlist.head
	}
	return it
}

func (it *SortedDListIt) Valid() bool {
	return it.curr != nil
}

func (it *SortedDListIt) Next() {
	if it.linkLocker == nil {
		it.curr = it.nextFunc(it.curr)
		return
	}
	it.linkLocker.RLock()
	it.curr = it.nextFunc(it.curr)
	it.linkLocker.RUnlock()
}

func (it *SortedDListIt) Get() *DLNode {
	return it.curr
}
