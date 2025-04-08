// Copyright 2024 Matrix Origin
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

package fifocache

import "container/list"

// ghost represents the `ghost` structure in the S3FIFO algorithm.
// At the time of writing, this implementation is not carefully designed.
// There can be a better way to implement `ghost`.
type ghost[K comparable] struct {
	size  int
	ll    *list.List
	items map[K]*list.Element
}

func newGhost[K comparable](size int) *ghost[K] {
	return &ghost[K]{
		size:  size,
		ll:    list.New(),
		items: make(map[K]*list.Element),
	}
}

func (b *ghost[K]) add(key K) {
	if _, ok := b.items[key]; ok {
		return
	}

	for b.ll.Len() >= b.size {
		e := b.ll.Back()
		delete(b.items, e.Value.(K))
		b.ll.Remove(e)
	}

	e := b.ll.PushFront(key)
	b.items[key] = e
}

func (b *ghost[K]) remove(key K) {
	if e, ok := b.items[key]; ok {
		b.ll.Remove(e)
		delete(b.items, key)
	}
}

func (b *ghost[K]) contains(key K) bool {
	_, ok := b.items[key]
	return ok
}

func (b *ghost[K]) clear() {
	b.ll.Init()
	for k := range b.items {
		delete(b.items, k)
	}
}
