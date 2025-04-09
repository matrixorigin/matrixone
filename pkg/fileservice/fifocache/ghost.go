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

import (
	"container/list"
	"sync"
)

// ghost implements a thread-safe ghost queue for S3-FIFO
type ghost[K comparable] struct {
	mu       sync.RWMutex // Use RWMutex for better read performance
	capacity int
	keys     map[K]*list.Element
	queue    *list.List
}

func newGhost[K comparable](capacity int) *ghost[K] {
	return &ghost[K]{
		capacity: capacity,
		keys:     make(map[K]*list.Element),
		queue:    list.New(),
	}
}

func (g *ghost[K]) add(key K) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, ok := g.keys[key]; ok {
		// Key already exists, maybe move it to back if needed by specific ghost logic
		// For simple ghost queue, we might just ignore or update frequency if tracked
		return
	}

	// Evict if capacity is reached
	if g.queue.Len() >= g.capacity {
		elem := g.queue.Front()
		if elem != nil {
			evictedKey := g.queue.Remove(elem).(K)
			delete(g.keys, evictedKey)
		}
	}

	// Add new key
	elem := g.queue.PushBack(key)
	g.keys[key] = elem
}

func (g *ghost[K]) remove(key K) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if elem, ok := g.keys[key]; ok {
		g.queue.Remove(elem)
		delete(g.keys, key)
	}
}

func (g *ghost[K]) contains(key K) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()

	_, ok := g.keys[key]
	return ok
}

/*
func (g *ghost[K]) clear() {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.queue.Init()
	for k := range g.keys {
		delete(g.keys, k)
	}
}
*/
