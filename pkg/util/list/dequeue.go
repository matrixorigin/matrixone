// Copyright 2023 Matrix Origin
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

package list

// Deque deque
type Deque[E any] interface {
	// Len returns the number of elements of Deque.
	// The complexity is O(1).
	Len() int
	// Clear clears Deque
	Clear()
	// Iter call fn on all elements which after the offset, stopped if false returned
	Iter(offset int, fn func(E) bool)
	// Front returns the first element of Deque, false if the list is empty.
	Front() (*Element[E], bool)
	// PopFront removes and returns the first element of Deque
	PopFront() *Element[E]
	// Front returns the first element of Deque, panic if the list is empty.
	MustFront() *Element[E]
	// Back returns the last element of Deque, false if the list is empty.
	Back() (*Element[E], bool)
	// PopBack removes and returns the last element of Deque
	PopBack() *Element[E]
	// MustBack returns the last element of Deque, panic if the list is empty.
	MustBack() *Element[E]
	// PushFront inserts a new element e with value v at the front of deque.
	PushFront(v E) *Element[E]
	// PushBack inserts a new element e with value v at the back of deque.
	PushBack(v E) *Element[E]
	// InsertBefore inserts a new element e with value v immediately before mark and returns e.
	// If mark is not an element of l, the list is not modified.
	// The mark must not be nil.
	InsertBefore(v E, mark *Element[E]) *Element[E]
	// InsertAfter inserts a new element e with value v immediately after mark and returns e.
	// If mark is not an element of l, the list is not modified.
	// The mark must not be nil.
	InsertAfter(v E, mark *Element[E]) *Element[E]
	// MoveToFront moves element e to the front of list l.
	// If e is not an element of l, the list is not modified.
	// The element must not be nil.
	MoveToFront(e *Element[E])
	// MoveToBack moves element e to the back of list l.
	// If e is not an element of l, the list is not modified.
	// The element must not be nil.
	MoveToBack(e *Element[E])
	// MoveBefore moves element e to its new position before mark.
	// If e or mark is not an element of l, or e == mark, the list is not modified.
	// The element and mark must not be nil.
	MoveBefore(e, mark *Element[E])
	// MoveAfter moves element e to its new position after mark.
	// If e or mark is not an element of l, or e == mark, the list is not modified.
	// The element and mark must not be nil.
	MoveAfter(e, mark *Element[E])
	// Remove removes e from l if e is an element of list l.
	// It returns the element value e.Value.
	// The element must not be nil.
	Remove(e *Element[E]) E
	// Truncate trancate deque, keeping the first size elements
	Truncate(keeping int)
	// Drain removes the specified range in the deque, returns drained
	Drain(from, to int) Deque[E]
}

// New returns an initialized Deque.
func New[E any]() Deque[E] {
	q := newDefaultDequeue[E]()
	q.Clear()
	return q
}

// Element is an Element of a linked Deque.
type Element[E any] struct {
	// Next and previous pointers in the doubly-linked Deque of elements.
	// To simplify the implementation, internally a Deque l is implemented
	// as a ring, such that &l.root is both the next element of the last
	// Deque element (l.Back()) and the previous element of the first Deque
	// element (l.Front()).
	next, prev *Element[E]

	// The list to which this element belongs.
	list *defaultDeque[E]

	// The value stored with this element.
	Value E
}

// Next returns the next Deque element or nil.
func (e *Element[E]) Next() *Element[E] {
	if p := e.next; e.list != nil && p != &e.list.root {
		return p
	}
	return nil
}

// Prev returns the previous Deque element or nil.
func (e *Element[E]) Prev() *Element[E] {
	if p := e.prev; e.list != nil && p != &e.list.root {
		return p
	}
	return nil
}

type defaultDeque[E any] struct {
	root Element[E] // sentinel list element, only &root, root.prev, and root.next are used
	len  int        // current list length excluding (this) sentinel element
}

func newDefaultDequeue[E any]() *defaultDeque[E] {
	return &defaultDeque[E]{}
}

func (q *defaultDeque[E]) Clear() {
	q.root.next = &q.root
	q.root.prev = &q.root
	q.len = 0
}

func (q *defaultDeque[E]) Truncate(keeping int) {
	if keeping >= q.len {
		return
	}

	q.doRangeRemove(keeping, q.len, false)
}

func (q *defaultDeque[E]) Len() int { return q.len }

func (q *defaultDeque[E]) Iter(offset int, fn func(E) bool) {
	if q.len == 0 {
		return
	}

	skipped := 0
	v, _ := q.Front()
	for e := v; e != nil; e = e.Next() {
		if skipped < offset {
			skipped++
			continue
		}

		if !fn(e.Value) {
			return
		}
	}
}

func (q *defaultDeque[E]) Front() (*Element[E], bool) {
	if q.len == 0 {
		return nil, false
	}
	return q.root.next, true
}

func (q *defaultDeque[E]) PopFront() *Element[E] {
	if q.len == 0 {
		return nil
	}

	return q.remove(q.root.next)
}

func (q *defaultDeque[E]) MustFront() *Element[E] {
	if q.len == 0 {
		panic("MustFront on a empty deque")
	}

	return q.root.next
}

func (q *defaultDeque[E]) Back() (*Element[E], bool) {
	if q.len == 0 {
		return nil, false
	}
	return q.root.prev, true
}

func (q *defaultDeque[E]) PopBack() *Element[E] {
	if q.len == 0 {
		return nil
	}

	return q.remove(q.root.prev)
}

func (q *defaultDeque[E]) MustBack() *Element[E] {
	if q.len == 0 {
		panic("MustBack on a empty deque")
	}

	return q.root.prev
}

func (q *defaultDeque[E]) PushFront(v E) *Element[E] {
	q.lazyInit()
	return q.insertValue(v, &q.root)
}

func (q *defaultDeque[E]) PushBack(v E) *Element[E] {
	q.lazyInit()
	return q.insertValue(v, q.root.prev)
}

func (q *defaultDeque[E]) Drain(from, to int) Deque[E] {
	return q.doRangeRemove(from, to, true)
}

// lazyInit lazily initializes a zero List value.
func (q *defaultDeque[E]) lazyInit() {
	if q.root.next == nil {
		q.Clear()
	}
}

// insert inserts e after at, increments l.len, and returns e.
func (q *defaultDeque[E]) insert(e, at *Element[E]) *Element[E] {
	e.prev = at
	e.next = at.next
	e.prev.next = e
	e.next.prev = e
	e.list = q
	q.len++
	return e
}

// insertValue is a convenience wrapper for insert(&Element{Value: v}, at).
func (q *defaultDeque[E]) insertValue(v E, at *Element[E]) *Element[E] {
	return q.insert(&Element[E]{Value: v}, at)
}

// remove removes e from its list, decrements l.len, and returns e.
func (q *defaultDeque[E]) remove(e *Element[E]) *Element[E] {
	e.prev.next = e.next
	e.next.prev = e.prev
	e.next = nil // avoid memory leaks
	e.prev = nil // avoid memory leaks
	e.list = nil
	q.len--
	return e
}

// move moves e to next to at and returns e.
func (q *defaultDeque[E]) move(e, at *Element[E]) *Element[E] {
	if e == at {
		return e
	}
	e.prev.next = e.next
	e.next.prev = e.prev

	e.prev = at
	e.next = at.next
	e.prev.next = e
	e.next.prev = e

	return e
}

func (q *defaultDeque[E]) Remove(e *Element[E]) E {
	if e.list == q {
		// if e.list == l, l must have been initialized when e was inserted
		// in l or l == nil (e is a zero Element) and l.remove will crash
		q.remove(e)
	}
	return e.Value
}

func (q *defaultDeque[E]) InsertBefore(v E, mark *Element[E]) *Element[E] {
	if mark.list != q {
		return nil
	}
	// see comment in List.Remove about initialization of l
	return q.insertValue(v, mark.prev)
}

func (q *defaultDeque[E]) InsertAfter(v E, mark *Element[E]) *Element[E] {
	if mark.list != q {
		return nil
	}
	// see comment in List.Remove about initialization of l
	return q.insertValue(v, mark)
}

func (q *defaultDeque[E]) MoveToFront(e *Element[E]) {
	if e.list != q || q.root.next == e {
		return
	}
	// see comment in List.Remove about initialization of l
	q.move(e, &q.root)
}

func (q *defaultDeque[E]) MoveToBack(e *Element[E]) {
	if e.list != q || q.root.prev == e {
		return
	}
	// see comment in List.Remove about initialization of l
	q.move(e, q.root.prev)
}

func (q *defaultDeque[E]) MoveBefore(e, mark *Element[E]) {
	if e.list != q || e == mark || mark.list != q {
		return
	}
	q.move(e, mark.prev)
}

func (q *defaultDeque[E]) MoveAfter(e, mark *Element[E]) {
	if e.list != q || e == mark || mark.list != q {
		return
	}
	q.move(e, mark)
}

func (q *defaultDeque[E]) doRangeRemove(from, to int, withRemoved bool) Deque[E] {
	if from >= to {
		return nil
	}

	q.lazyInit()
	if q.len == 0 {
		return nil
	}

	if to > q.len {
		to = q.len
	}

	i := 0
	var left *Element[E]
	var drainedRight *Element[E]
	right := &q.root
	for e := q.root.next; e != &q.root && e.list != nil; e = e.next {
		if i >= from && i < to {
			if left == nil {
				left = e
			}
			drainedRight = e
		} else if i >= to {
			right = e
			break
		}

		i++
	}

	q.len -= i - from
	left.prev.next = right
	right.prev = left.prev
	if right == &q.root {
		q.root.prev = left.prev
	}

	if !withRemoved {
		return nil
	}

	drained := newDefaultDequeue[E]()
	drained.Clear()
	left.prev = &drained.root
	drained.root.next = left
	drained.root.prev = drainedRight
	drainedRight.next = &drained.root
	drained.len = i - from
	for e := left; e != &q.root && e.list != nil; e = e.next {
		e.list = drained
		if e == drainedRight {
			break
		}
	}
	return drained
}
