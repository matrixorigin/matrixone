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

import "sync"

type Queue[T any] struct {
	mu       sync.Mutex // Mutex to protect queue operations
	head     *queuePart[T]
	tail     *queuePart[T]
	partPool sync.Pool
	size     int
}

type queuePart[T any] struct {
	values []T
	next   *queuePart[T]
}

const maxQueuePartCapacity = 256

func NewQueue[T any]() *Queue[T] {
	queue := &Queue[T]{
		partPool: sync.Pool{
			New: func() any {
				return &queuePart[T]{
					values: make([]T, 0, maxQueuePartCapacity),
				}
			},
		},
	}
	part := queue.partPool.Get().(*queuePart[T])
	queue.head = part
	queue.tail = part
	return queue
}

// empty is an internal helper, assumes lock is held
func (p *Queue[T]) empty() bool {
	return p.head == p.tail && len(p.head.values) == 0
}

func (p *queuePart[T]) reset() {
	p.values = p.values[:0]
	p.next = nil
}

func (p *Queue[T]) enqueue(v T) {
	p.mu.Lock()         // Acquire lock
	defer p.mu.Unlock() // Ensure lock is released

	if len(p.head.values) >= maxQueuePartCapacity {
		// extend
		newPart := p.partPool.Get().(*queuePart[T])
		newPart.reset()
		p.head.next = newPart
		p.head = newPart
	}
	p.head.values = append(p.head.values, v)
	p.size++
}

func (p *Queue[T]) dequeue() (ret T, ok bool) {
	p.mu.Lock()         // Acquire lock
	defer p.mu.Unlock() // Ensure lock is released

	if p.empty() {
		return
	}

	if len(p.tail.values) == 0 {
		// shrink
		if p.tail.next == nil {
			// This should ideally not happen if empty() check passes,
			// but adding a safeguard.
			// Consider logging an error here if it does.
			return
		}
		part := p.tail
		p.tail = p.tail.next
		p.partPool.Put(part) // Return the old part to the pool
	}

	// Check again if the new tail part is also empty (unlikely but possible)
	if len(p.tail.values) == 0 {
		return
	}

	ret, p.tail.values = p.tail.values[0], p.tail.values[1:]
	p.size--
	ok = true
	return
}

func (p *Queue[T]) Len() int {
	p.mu.Lock()         // Acquire lock
	defer p.mu.Unlock() // Ensure lock is released
	return p.size
}

// remove is added for completeness, making it thread-safe as well.
// Note: This is O(N) where N is the number of elements. Use with caution in performance-critical paths.
func (p *Queue[T]) remove(item T) bool {
	p.mu.Lock()         // Acquire lock
	defer p.mu.Unlock() // Ensure lock is released

	if p.empty() {
		return false
	}

	currentPart := p.tail
	prevPart := (*queuePart[T])(nil) // Keep track of the previous part for potential removal

	for currentPart != nil {
		found := false
		newValues := currentPart.values[:0] // Create a new slice to hold non-removed items
		for _, val := range currentPart.values {
			// Use type assertion and comparison (adjust if T is not comparable)
			if any(val) == any(item) {
				found = true
				p.size-- // Decrement size only once if found
			} else {
				newValues = append(newValues, val)
			}
		}

		currentPart.values = newValues // Update the values slice

		// If the part becomes empty after removal and it's not the only part
		if len(currentPart.values) == 0 && !(currentPart == p.head && currentPart == p.tail) {
			if currentPart == p.tail {
				// Removing the tail part
				p.tail = currentPart.next
				if p.tail == nil { // Should not happen if head exists
					p.head = nil // Queue becomes empty
				}
				p.partPool.Put(currentPart)
			} else if prevPart != nil {
				// Removing a middle part
				prevPart.next = currentPart.next
				if currentPart == p.head { // Check if removing the head
					p.head = prevPart
				}
				p.partPool.Put(currentPart)
			}
			// Adjust currentPart for the next iteration if it was removed
			if prevPart != nil {
				currentPart = prevPart // Continue checking from the previous part's next
			} else {
				currentPart = p.tail // Restart from the new tail if tail was removed
				continue             // Skip incrementing prevPart
			}

		}

		if found {
			return true // Item found and removed
		}

		// Move to the next part
		prevPart = currentPart
		currentPart = currentPart.next
	}

	return false // Item not found
}
