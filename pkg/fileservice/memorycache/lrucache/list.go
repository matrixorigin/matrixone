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

package lrucache

import "sync"

// List represents a doubly linked list.
// The zero value for List is an empty list ready to use.
type list[K comparable, V BytesLike] struct {
	sync.RWMutex
	root lruItem[K, V] // sentinel list element, only &root, root.prev, and root.next are used
	len  int           // current list length excluding (this) sentinel element
}

// Init initializes or clears list l.
func (l *list[K, V]) init() *list[K, V] {
	l.root.next = &l.root
	l.root.prev = &l.root
	l.len = 0
	return l
}

// lazyInit lazily initializes a zero List value.
func (l *list[K, V]) lazyInit() {
	if l.root.next == nil {
		l.init()
	}
}

// New returns an initialized list.
func newList[K comparable, V BytesLike]() *list[K, V] { return new(list[K, V]).init() }

// Back returns the last element of list l or nil if the list is empty.
func (l *list[K, V]) Back() *lruItem[K, V] {
	l.RLock()
	defer l.RUnlock()
	if l.len == 0 {
		return nil
	}
	return l.root.prev
}

// remove removes e from its list, decrements l.len
func (l *list[K, V]) remove(e *lruItem[K, V]) {
	e.prev.next = e.next
	e.next.prev = e.prev
	e.next = nil // avoid memory leaks
	e.prev = nil // avoid memory leaks
	e.list = nil
	l.len--
}

// insert inserts e after at, increments l.len, and returns e.
func (l *list[K, V]) insert(e, at *lruItem[K, V]) *lruItem[K, V] {
	e.prev = at
	e.next = at.next
	e.prev.next = e
	e.next.prev = e
	e.list = l
	l.len++
	return e
}

// insertValue is a convenience wrapper for insert(&Element{Value: v}, at).
func (l *list[K, V]) insertValue(v *lruItem[K, V], at *lruItem[K, V]) *lruItem[K, V] {
	return l.insert(v, at)
}

// Remove removes e from l if e is an element of list l.
// It returns the element value e.Value.
// The element must not be nil.
func (l *list[K, V]) Remove(e *lruItem[K, V]) {
	l.Lock()
	defer l.Unlock()
	if e.list == l {
		// if e.list == l, l must have been initialized when e was inserted
		// in l or l == nil (e is a zero Element) and l.remove will crash
		l.remove(e)
	}
}

// PushFront inserts a new element e with value v at the front of list l and returns e.
func (l *list[K, V]) PushFront(v *lruItem[K, V]) *lruItem[K, V] {
	l.Lock()
	defer l.Unlock()
	l.lazyInit()
	return l.insertValue(v, &l.root)
}
